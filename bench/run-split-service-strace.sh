#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CRATE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results"

IMPLEMENTATION="handoff"
SCENARIO="coalesced"
COMPLETION="fire_and_forget"
WORKER_THREADS="16"
CONNECTIONS="64"
ROUTE_FRAMES="64"
FRAME_LEN="63"
TUNNEL_BYTES="262144"
READ_RESERVE="16384"
DURATION_SECONDS="2"
SERVICE_CORES="0-7,16-23"
DRIVER_CORES="8-15,24-31"
IDLE_TIMEOUT_MILLIS="2000"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --implementation) IMPLEMENTATION="$2"; shift 2 ;;
    --scenario) SCENARIO="$2"; shift 2 ;;
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
      exit 0
      ;;
    *)
      echo "Unknown arg: $1"
      exit 1
      ;;
  esac
done

mkdir -p "$RESULTS_DIR"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="$RESULTS_DIR/strace_split_${IMPLEMENTATION}_${SCENARIO}_${TIMESTAMP}"
mkdir -p "$RUN_DIR"

cargo build --release --bin bench_stream_harness --features bench-tools --manifest-path "$CRATE_DIR/Cargo.toml"

BIN="$CRATE_DIR/target/release/bench_stream_harness"
BASE_PORT=$((22000 + (($$ % 1000) * 2)))
SERVICE_ADDR="127.0.0.1:$BASE_PORT"
SINK_ADDR="127.0.0.1:$((BASE_PORT + 1))"
READY_FILE="$RUN_DIR/service.ready"
SERVICE_LOG="$RUN_DIR/service.out"
DRIVER_LOG="$RUN_DIR/driver.out"
STRACE_SUMMARY="$RUN_DIR/service.strace.summary"

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
rm -f "$READY_FILE"
taskset -c "$SERVICE_CORES" strace -c -f -o "$STRACE_SUMMARY" \
  "$BIN" "${COMMON_ARGS[@]}" \
  --role tcp-service \
  --service-addr "$SERVICE_ADDR" \
  --sink-addr "$SINK_ADDR" \
  --ready-file "$READY_FILE" \
  --idle-timeout-millis "$IDLE_TIMEOUT_MILLIS" \
  >"$SERVICE_LOG" 2>&1 &
SERVICE_PID=$!

for _ in $(seq 1 200); do
  [[ -f "$READY_FILE" ]] && break
  if ! kill -0 "$SERVICE_PID" 2>/dev/null; then
    echo "ERROR: service exited before readiness"
    cat "$SERVICE_LOG"
    exit 1
  fi
  sleep 0.05
done

taskset -c "$DRIVER_CORES" "$BIN" "${COMMON_ARGS[@]}" \
  --role tcp-driver \
  --service-addr "$SERVICE_ADDR" \
  --sink-addr "$SINK_ADDR" \
  --idle-timeout-millis "$IDLE_TIMEOUT_MILLIS" \
  >"$DRIVER_LOG"

wait "$SERVICE_PID"
sed -n '1,80p' "$SERVICE_LOG"
sed -n '1,80p' "$DRIVER_LOG"
cat "$STRACE_SUMMARY"
