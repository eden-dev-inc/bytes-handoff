#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CRATE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results"

ulimit -n 1048576 2>/dev/null || true

detect_cpus() {
  nproc 2>/dev/null || getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.logicalcpu 2>/dev/null || echo 4
}

TRANSPORT="cached"
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
INPUT_MODEL="fixed"
TCP_MSS_BYTES="${TCP_MSS_BYTES:-1460}"
TCP_SHARD_MODE="${TCP_SHARD_MODE:-shared}"
READ_RESERVE="16384"
HANDOFF_FLUSH_BYTES=""
READ_TELEMETRY="0"
COALESCER_STATS="0"
WRITE_PENDING_BYTES=""
DUPLEX_CAPACITY="262144"
ITERATIONS=""
DURATION_SECONDS=""
SERVICE_CORES=""
DRIVER_CORES=""
TASKSET_CORES="${TASKSET_CORES:-}"
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
    --input-model) INPUT_MODEL="$2"; shift 2 ;;
    --tcp-mss-bytes) TCP_MSS_BYTES="$2"; shift 2 ;;
    --tcp-shard-mode) TCP_SHARD_MODE="$2"; shift 2 ;;
    --read-reserve) READ_RESERVE="$2"; shift 2 ;;
    --handoff-flush-bytes) HANDOFF_FLUSH_BYTES="$2"; shift 2 ;;
    --read-telemetry) READ_TELEMETRY="1"; shift ;;
    --coalescer-stats) COALESCER_STATS="1"; shift ;;
    --write-pending-bytes) WRITE_PENDING_BYTES="$2"; shift 2 ;;
    --duplex-capacity) DUPLEX_CAPACITY="$2"; shift 2 ;;
    --iterations) ITERATIONS="$2"; shift 2 ;;
    --duration-seconds) DURATION_SECONDS="$2"; shift 2 ;;
    --service-cores) SERVICE_CORES="$2"; shift 2 ;;
    --driver-cores) DRIVER_CORES="$2"; shift 2 ;;
    --idle-timeout-millis) IDLE_TIMEOUT_MILLIS="$2"; shift 2 ;;
    --help)
      echo "Usage: $0 [--transport duplex|cached|tcp] [--implementation handoff|monoio_handoff|bytesmut_handoff|manual_vec|raw_copy] [--scenario fragmented|coalesced|all] [--completion ticket|fire_and_forget] [--worker-threads N] [--connections N] [--runs N]"
      echo "          [--route-frames N] [--frame-len N] [--tunnel-bytes N] [--input-fragment N] [--input-model fixed|tcp] [--tcp-mss-bytes N] [--tcp-shard-mode shared|direct]"
      echo "          [--read-reserve N] [--handoff-flush-bytes N] [--read-telemetry] [--coalescer-stats] [--write-pending-bytes N] [--duplex-capacity N] [--iterations N] [--duration-seconds N]"
      echo "          [--service-cores CPUSET --driver-cores CPUSET] [--idle-timeout-millis N]"
      echo ""
      echo "Environment:"
      echo "  TASKSET_CORES=CPUSET pins integrated duplex/cached runs with taskset"
      echo "  monoio_handoff + tcp requires --service-cores and --driver-cores; the service runs one Monoio listener/runtime per worker thread"
      exit 0
      ;;
    *)
      echo "Unknown arg: $1"
      exit 1
      ;;
  esac
done

case "$TRANSPORT" in
  duplex|cached|tcp) ;;
  *)
    echo "ERROR: unsupported --transport '$TRANSPORT'"
    exit 1
    ;;
esac
case "$IMPLEMENTATION" in
  handoff|monoio_handoff|bytesmut_handoff|manual_vec|raw_copy) ;;
  *)
    echo "ERROR: unsupported --implementation '$IMPLEMENTATION'"
    exit 1
    ;;
esac
if [[ "$IMPLEMENTATION" == "monoio_handoff" && "$TRANSPORT" != "duplex" && "$TRANSPORT" != "cached" && "$TRANSPORT" != "tcp" ]]; then
  echo "ERROR: monoio_handoff currently supports --transport duplex|cached|tcp"
  exit 1
fi
if [[ "$IMPLEMENTATION" == "monoio_handoff" && "$TRANSPORT" == "tcp" && ( -z "$SERVICE_CORES" || -z "$DRIVER_CORES" ) ]]; then
  echo "ERROR: monoio_handoff tcp runs require split mode with --service-cores and --driver-cores"
  exit 1
fi
if [[ "$TCP_SHARD_MODE" == "direct" && ( "$TRANSPORT" != "tcp" || "$IMPLEMENTATION" != "monoio_handoff" ) ]]; then
  echo "ERROR: --tcp-shard-mode direct currently requires --transport tcp --implementation monoio_handoff"
  exit 1
fi
case "$SCENARIO" in
  fragmented|coalesced|all) ;;
  *)
    echo "ERROR: unsupported --scenario '$SCENARIO'"
    exit 1
    ;;
esac
case "$INPUT_MODEL" in
  fixed|tcp) ;;
  *)
    echo "ERROR: unsupported --input-model '$INPUT_MODEL'"
    exit 1
    ;;
esac
case "$TCP_SHARD_MODE" in
  shared|direct) ;;
  *)
    echo "ERROR: unsupported --tcp-shard-mode '$TCP_SHARD_MODE'"
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
case "$READ_TELEMETRY" in
  0|1) ;;
  *)
    echo "ERROR: unsupported read telemetry flag '$READ_TELEMETRY'"
    exit 1
    ;;
esac

CARGO_FEATURES="${CARGO_FEATURES:-bench-tools}"
if [[ "$READ_TELEMETRY" == "1" ]]; then
  if [[ "$IMPLEMENTATION" == "monoio_handoff" ]]; then
    CARGO_FEATURES="$CARGO_FEATURES,telemetry-monoio"
  else
    CARGO_FEATURES="$CARGO_FEATURES,telemetry"
  fi
fi

mkdir -p "$RESULTS_DIR"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="$RESULTS_DIR/stream_${TRANSPORT}_${IMPLEMENTATION}_${SCENARIO}_${COMPLETION}_${TIMESTAMP}_$$"
mkdir -p "$RUN_DIR"

echo "=== bytes-handoff stream harness ==="
echo "transport=$TRANSPORT implementation=$IMPLEMENTATION scenario=$SCENARIO completion=$COMPLETION worker_threads=$WORKER_THREADS connections=$CONNECTIONS runs=$RUNS"
echo "route_frames=$ROUTE_FRAMES frame_len=$FRAME_LEN tunnel_bytes=$TUNNEL_BYTES input_model=$INPUT_MODEL tcp_mss_bytes=$TCP_MSS_BYTES tcp_shard_mode=$TCP_SHARD_MODE read_reserve=$READ_RESERVE duplex_capacity=$DUPLEX_CAPACITY"
if [[ -n "$WRITE_PENDING_BYTES" ]]; then
  echo "write_pending_bytes=$WRITE_PENDING_BYTES"
else
  echo "write_pending_bytes=default_8x_read_reserve"
fi
if [[ -n "$HANDOFF_FLUSH_BYTES" ]]; then
  echo "handoff_flush_bytes=$HANDOFF_FLUSH_BYTES"
else
  echo "handoff_flush_bytes=default_16384"
fi
echo "coalescer_stats_enabled=$COALESCER_STATS"
echo "read_telemetry_enabled=$READ_TELEMETRY"
echo "cargo_features=$CARGO_FEATURES"
if [[ -n "$SERVICE_CORES" || -n "$DRIVER_CORES" ]]; then
  echo "split_tcp_service_cores=${SERVICE_CORES:-none} split_tcp_driver_cores=${DRIVER_CORES:-none} idle_timeout_millis=$IDLE_TIMEOUT_MILLIS"
fi
if [[ -n "$TASKSET_CORES" ]]; then
  echo "taskset_cores=$TASKSET_CORES"
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

cargo build --release --bin bench_stream_harness --features "$CARGO_FEATURES" --manifest-path "$CRATE_DIR/Cargo.toml"

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
    --input-model "$INPUT_MODEL"
    --tcp-mss-bytes "$TCP_MSS_BYTES"
    --tcp-shard-mode "$TCP_SHARD_MODE"
    --read-reserve "$READ_RESERVE"
    --duplex-capacity "$DUPLEX_CAPACITY"
  )
  if [[ -n "${WRITE_PENDING_BYTES:-}" ]]; then
    cmd+=(--write-pending-bytes "$WRITE_PENDING_BYTES")
  fi
  if [[ -n "${HANDOFF_FLUSH_BYTES:-}" ]]; then
    cmd+=(--handoff-flush-bytes "$HANDOFF_FLUSH_BYTES")
  fi
  if [[ "$COALESCER_STATS" == "1" ]]; then
    cmd+=(--coalescer-stats)
  fi
  if [[ "$READ_TELEMETRY" == "1" ]]; then
    cmd+=(--read-telemetry)
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
  elif [[ -n "$TASKSET_CORES" ]]; then
    if [[ "$TRANSPORT" == "tcp" ]]; then
      echo "ERROR: TASKSET_CORES only supports integrated duplex/cached runs; use --service-cores/--driver-cores for --transport tcp"
      exit 1
    fi
    if ! command -v taskset >/dev/null 2>&1; then
      echo "ERROR: TASKSET_CORES requires taskset"
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
      local sink_port=$((base_port + 1))
      if [[ "$TCP_SHARD_MODE" == "direct" ]]; then
        sink_port=$((base_port + WORKER_THREADS + 1))
      fi
      local sink_addr="127.0.0.1:$sink_port"
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
      if [[ -n "$TASKSET_CORES" ]]; then
        taskset -c "$TASKSET_CORES" "${cmd[@]}" | tee "$out_dir/handoff-run-${run}.txt"
      else
        "${cmd[@]}" | tee "$out_dir/handoff-run-${run}.txt"
      fi
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
