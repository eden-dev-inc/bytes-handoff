#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CRATE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results"

IMPLEMENTATION="handoff"
SCENARIO="coalesced"
COMPLETION="fire_and_forget"
WORKER_THREADS="16"
CONNECTIONS="128"
ROUTE_FRAMES="64"
FRAME_LEN="63"
TUNNEL_BYTES="1048576"
READ_RESERVE="16384"
DURATION_SECONDS="5"
SERVICE_CORES="0-7,16-23"
DRIVER_CORES="8-15,24-31"
IDLE_TIMEOUT_MILLIS="2000"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --implementation) IMPLEMENTATION="$2"; shift 2 ;;
    --scenario) SCENARIO="$2"; shift 2 ;;
    --completion) COMPLETION="$2"; shift 2 ;;
    --worker-threads) WORKER_THREADS="$2"; shift 2 ;;
    --connections) CONNECTIONS="$2"; shift 2 ;;
    --route-frames) ROUTE_FRAMES="$2"; shift 2 ;;
    --frame-len) FRAME_LEN="$2"; shift 2 ;;
    --tunnel-bytes) TUNNEL_BYTES="$2"; shift 2 ;;
    --read-reserve) READ_RESERVE="$2"; shift 2 ;;
    --duration-seconds) DURATION_SECONDS="$2"; shift 2 ;;
    --service-cores) SERVICE_CORES="$2"; shift 2 ;;
    --driver-cores) DRIVER_CORES="$2"; shift 2 ;;
    --idle-timeout-millis) IDLE_TIMEOUT_MILLIS="$2"; shift 2 ;;
    --help)
      echo "Usage: $0 [--implementation handoff|manual_vec|raw_copy] [--scenario fragmented|coalesced]"
      echo "          [--service-cores CPUSET] [--driver-cores CPUSET] [--duration-seconds N]"
      exit 0
      ;;
    *)
      echo "Unknown arg: $1"
      exit 1
      ;;
  esac
done

if ! command -v taskset >/dev/null 2>&1; then
  echo "ERROR: taskset is required"
  exit 1
fi
if ! command -v perf >/dev/null 2>&1; then
  echo "ERROR: perf is required"
  exit 1
fi

mkdir -p "$RESULTS_DIR"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="$RESULTS_DIR/perf_split_${IMPLEMENTATION}_${SCENARIO}_${TIMESTAMP}"
mkdir -p "$RUN_DIR"

cargo build --release --bin bench_stream_harness --features bench-tools --manifest-path "$CRATE_DIR/Cargo.toml"

BIN="$CRATE_DIR/target/release/bench_stream_harness"
BASE_PORT=$((20000 + (($$ % 1000) * 2)))
SERVICE_ADDR="127.0.0.1:$BASE_PORT"
SINK_ADDR="127.0.0.1:$((BASE_PORT + 1))"
READY_FILE="$RUN_DIR/service.ready"
SERVICE_LOG="$RUN_DIR/service.out"
DRIVER_LOG="$RUN_DIR/driver.out"
PERF_DATA="$RUN_DIR/service.perf.data"

COMMON_ARGS=(
  --transport tcp
  --implementation "$IMPLEMENTATION"
  --scenario "$SCENARIO"
  --completion "$COMPLETION"
  --worker-threads "$WORKER_THREADS"
  --connections "$CONNECTIONS"
  --route-frames "$ROUTE_FRAMES"
  --frame-len "$FRAME_LEN"
  --tunnel-bytes "$TUNNEL_BYTES"
  --read-reserve "$READ_RESERVE"
  --duration-seconds "$DURATION_SECONDS"
)

echo "results=$RUN_DIR"
echo "implementation=$IMPLEMENTATION scenario=$SCENARIO service_cores=$SERVICE_CORES driver_cores=$DRIVER_CORES"

rm -f "$READY_FILE"
taskset -c "$SERVICE_CORES" perf record -F 997 -g --call-graph fp -o "$PERF_DATA" -- \
  "$BIN" "${COMMON_ARGS[@]}" \
  --role tcp-service \
  --service-addr "$SERVICE_ADDR" \
  --sink-addr "$SINK_ADDR" \
  --ready-file "$READY_FILE" \
  --idle-timeout-millis "$IDLE_TIMEOUT_MILLIS" \
  >"$SERVICE_LOG" 2>&1 &
SERVICE_PID=$!

for _ in $(seq 1 200); do
  if [[ -f "$READY_FILE" ]]; then
    break
  fi
  if ! kill -0 "$SERVICE_PID" 2>/dev/null; then
    echo "ERROR: service exited before readiness"
    cat "$SERVICE_LOG"
    exit 1
  fi
  sleep 0.05
done
if [[ ! -f "$READY_FILE" ]]; then
  echo "ERROR: timed out waiting for service readiness"
  kill "$SERVICE_PID" 2>/dev/null || true
  wait "$SERVICE_PID" 2>/dev/null || true
  cat "$SERVICE_LOG"
  exit 1
fi

taskset -c "$DRIVER_CORES" "$BIN" "${COMMON_ARGS[@]}" \
  --role tcp-driver \
  --service-addr "$SERVICE_ADDR" \
  --sink-addr "$SINK_ADDR" \
  --idle-timeout-millis "$IDLE_TIMEOUT_MILLIS" \
  >"$DRIVER_LOG"

wait "$SERVICE_PID"

perf report --stdio --no-children --percent-limit 1 -i "$PERF_DATA" >"$RUN_DIR/report_no_children.txt"
perf report --stdio --children --percent-limit 1 -i "$PERF_DATA" >"$RUN_DIR/report_children.txt"

sed -n '1,80p' "$SERVICE_LOG"
sed -n '1,80p' "$DRIVER_LOG"
sed -n '1,140p' "$RUN_DIR/report_no_children.txt"
echo "report_children=$RUN_DIR/report_children.txt"
echo "report_no_children=$RUN_DIR/report_no_children.txt"
