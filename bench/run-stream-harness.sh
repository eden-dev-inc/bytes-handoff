#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CRATE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results"

detect_cpus() {
  nproc 2>/dev/null || getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.logicalcpu 2>/dev/null || echo 4
}

TRANSPORT="duplex"
IMPLEMENTATION="handoff"
SCENARIO="fragmented"
COMPLETION="ticket"
WORKER_THREADS="$(detect_cpus)"
CONNECTIONS="64"
RUNS="3"
ROUTE_FRAMES="64"
FRAME_LEN="63"
TUNNEL_BYTES="65536"
INPUT_FRAGMENT=""
READ_RESERVE="16384"
WRITE_PENDING_BYTES=""
DUPLEX_CAPACITY="262144"
ITERATIONS=""
DURATION_SECONDS=""
SERVICE_CORES=""
DRIVER_CORES=""
IDLE_TIMEOUT_MILLIS="2000"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --scenario) SCENARIO="$2"; shift 2 ;;
    --transport) TRANSPORT="$2"; shift 2 ;;
    --implementation) IMPLEMENTATION="$2"; shift 2 ;;
    --completion) COMPLETION="$2"; shift 2 ;;
    --worker-threads) WORKER_THREADS="$2"; shift 2 ;;
    --connections) CONNECTIONS="$2"; shift 2 ;;
    --runs) RUNS="$2"; shift 2 ;;
    --route-frames) ROUTE_FRAMES="$2"; shift 2 ;;
    --frame-len) FRAME_LEN="$2"; shift 2 ;;
    --tunnel-bytes) TUNNEL_BYTES="$2"; shift 2 ;;
    --input-fragment) INPUT_FRAGMENT="$2"; shift 2 ;;
    --read-reserve) READ_RESERVE="$2"; shift 2 ;;
    --write-pending-bytes) WRITE_PENDING_BYTES="$2"; shift 2 ;;
    --duplex-capacity) DUPLEX_CAPACITY="$2"; shift 2 ;;
    --iterations) ITERATIONS="$2"; shift 2 ;;
    --duration-seconds) DURATION_SECONDS="$2"; shift 2 ;;
    --service-cores) SERVICE_CORES="$2"; shift 2 ;;
    --driver-cores) DRIVER_CORES="$2"; shift 2 ;;
    --idle-timeout-millis) IDLE_TIMEOUT_MILLIS="$2"; shift 2 ;;
    --help)
      echo "Usage: $0 [--transport duplex|tcp] [--implementation handoff|manual_vec|raw_copy] [--scenario fragmented|coalesced|all] [--completion ticket|fire_and_forget] [--worker-threads N] [--connections N] [--runs N]"
      echo "          [--route-frames N] [--frame-len N] [--tunnel-bytes N] [--input-fragment N]"
      echo "          [--read-reserve N] [--write-pending-bytes N] [--duplex-capacity N] [--iterations N] [--duration-seconds N]"
      echo "          [--service-cores CPUSET --driver-cores CPUSET] [--idle-timeout-millis N]"
      exit 0
      ;;
    *)
      echo "Unknown arg: $1"
      exit 1
      ;;
  esac
done

case "$TRANSPORT" in
  duplex|tcp) ;;
  *)
    echo "ERROR: unsupported --transport '$TRANSPORT'"
    exit 1
    ;;
esac
case "$IMPLEMENTATION" in
  handoff|manual_vec|raw_copy) ;;
  *)
    echo "ERROR: unsupported --implementation '$IMPLEMENTATION'"
    exit 1
    ;;
esac
case "$SCENARIO" in
  fragmented|coalesced|all) ;;
  *)
    echo "ERROR: unsupported --scenario '$SCENARIO'"
    exit 1
    ;;
esac
case "$COMPLETION" in
  ticket|fire_and_forget) ;;
  *)
    echo "ERROR: unsupported --completion '$COMPLETION'"
    exit 1
    ;;
esac

mkdir -p "$RESULTS_DIR"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="$RESULTS_DIR/stream_${TRANSPORT}_${IMPLEMENTATION}_${SCENARIO}_${COMPLETION}_${TIMESTAMP}"
mkdir -p "$RUN_DIR"

echo "=== bytes-handoff stream harness ==="
echo "transport=$TRANSPORT implementation=$IMPLEMENTATION scenario=$SCENARIO completion=$COMPLETION worker_threads=$WORKER_THREADS connections=$CONNECTIONS runs=$RUNS"
echo "route_frames=$ROUTE_FRAMES frame_len=$FRAME_LEN tunnel_bytes=$TUNNEL_BYTES read_reserve=$READ_RESERVE duplex_capacity=$DUPLEX_CAPACITY"
if [[ -n "$WRITE_PENDING_BYTES" ]]; then
  echo "write_pending_bytes=$WRITE_PENDING_BYTES"
fi
if [[ -n "$SERVICE_CORES" || -n "$DRIVER_CORES" ]]; then
  echo "split_tcp_service_cores=${SERVICE_CORES:-none} split_tcp_driver_cores=${DRIVER_CORES:-none} idle_timeout_millis=$IDLE_TIMEOUT_MILLIS"
fi
if [[ -n "$ITERATIONS" ]]; then
  echo "iterations=$ITERATIONS"
fi
if [[ -n "$DURATION_SECONDS" ]]; then
  echo "duration_seconds=$DURATION_SECONDS"
fi
if [[ -n "$INPUT_FRAGMENT" ]]; then
  echo "input_fragment=$INPUT_FRAGMENT"
else
  echo "input_fragment=default_for_scenario"
fi
echo "results=$RUN_DIR"

cargo build --release --bin bench_stream_harness --features bench-tools --manifest-path "$CRATE_DIR/Cargo.toml"

BIN="$CRATE_DIR/target/release/bench_stream_harness"
if [[ ! -x "$BIN" ]]; then
  echo "ERROR: benchmark binary not found at $BIN"
  exit 1
fi

run_one_scenario() {
  local scenario_name="$1"
  local out_dir="$2"
  mkdir -p "$out_dir"

  local cmd=(
    "$BIN"
    --transport "$TRANSPORT"
    --implementation "$IMPLEMENTATION"
    --scenario "$scenario_name"
    --completion "$COMPLETION"
    --worker-threads "$WORKER_THREADS"
    --connections "$CONNECTIONS"
    --route-frames "$ROUTE_FRAMES"
    --frame-len "$FRAME_LEN"
    --tunnel-bytes "$TUNNEL_BYTES"
    --read-reserve "$READ_RESERVE"
    --duplex-capacity "$DUPLEX_CAPACITY"
  )
  if [[ -n "${WRITE_PENDING_BYTES:-}" ]]; then
    cmd+=(--write-pending-bytes "$WRITE_PENDING_BYTES")
  fi
  if [[ -n "${ITERATIONS:-}" ]]; then
    cmd+=(--iterations "$ITERATIONS")
  fi
  if [[ -n "${DURATION_SECONDS:-}" ]]; then
    cmd+=(--duration-seconds "$DURATION_SECONDS")
  fi
  if [[ -n "$INPUT_FRAGMENT" ]]; then
    cmd+=(--input-fragment "$INPUT_FRAGMENT")
  fi

  local split_tcp="false"
  if [[ "$TRANSPORT" == "tcp" && -n "$SERVICE_CORES" && -n "$DRIVER_CORES" ]]; then
    split_tcp="true"
    if ! command -v taskset >/dev/null 2>&1; then
      echo "ERROR: --service-cores/--driver-cores require taskset"
      exit 1
    fi
  elif [[ -n "$SERVICE_CORES" || -n "$DRIVER_CORES" ]]; then
    echo "ERROR: pass both --service-cores and --driver-cores with --transport tcp"
    exit 1
  fi

  echo ""
  echo "=== scenario=$scenario_name ==="
  for run in $(seq 1 "$RUNS"); do
    echo "[run $run/$RUNS] scenario=$scenario_name"
    if [[ "$split_tcp" == "true" ]]; then
      local scenario_offset=0
      if [[ "$scenario_name" == "coalesced" ]]; then
        scenario_offset=2000
      fi
      local base_port=$((20000 + scenario_offset + (($$ + run) % 1000) * 2))
      local service_addr="127.0.0.1:$base_port"
      local sink_addr="127.0.0.1:$((base_port + 1))"
      local ready_file="$out_dir/service-${run}.ready"
      local service_log="$out_dir/service-run-${run}.txt"
      rm -f "$ready_file"

      taskset -c "$SERVICE_CORES" "${cmd[@]}" \
        --role tcp-service \
        --service-addr "$service_addr" \
        --sink-addr "$sink_addr" \
        --ready-file "$ready_file" \
        --idle-timeout-millis "$IDLE_TIMEOUT_MILLIS" \
        >"$service_log" 2>&1 &
      local service_pid=$!

      for _ in $(seq 1 200); do
        if [[ -f "$ready_file" ]]; then
          break
        fi
        if ! kill -0 "$service_pid" 2>/dev/null; then
          echo "ERROR: tcp service exited before becoming ready"
          cat "$service_log"
          exit 1
        fi
        sleep 0.05
      done
      if [[ ! -f "$ready_file" ]]; then
        echo "ERROR: timed out waiting for tcp service readiness"
        kill "$service_pid" 2>/dev/null || true
        wait "$service_pid" 2>/dev/null || true
        cat "$service_log"
        exit 1
      fi

      taskset -c "$DRIVER_CORES" "${cmd[@]}" \
        --role tcp-driver \
        --service-addr "$service_addr" \
        --sink-addr "$sink_addr" \
        --idle-timeout-millis "$IDLE_TIMEOUT_MILLIS" \
        | tee "$out_dir/handoff-run-${run}.txt"

      wait "$service_pid"
    else
      "${cmd[@]}" | tee "$out_dir/handoff-run-${run}.txt"
    fi
  done
  python3 "$SCRIPT_DIR/summarize_stream_harness.py" "$out_dir"
}

if [[ "$SCENARIO" == "all" ]]; then
  run_one_scenario "fragmented" "$RUN_DIR/fragmented"
  run_one_scenario "coalesced" "$RUN_DIR/coalesced"
else
  run_one_scenario "$SCENARIO" "$RUN_DIR"
fi

echo ""
echo "Done. Results in: $RUN_DIR"
