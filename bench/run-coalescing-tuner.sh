#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

TRANSPORT="${TRANSPORT:-cached}"
IMPLEMENTATION="${IMPLEMENTATION:-handoff}"
SCENARIO="${SCENARIO:-fragmented}"
COMPLETION="${COMPLETION:-fire_and_forget}"
WORKER_THREADS="${WORKER_THREADS:-}"
CONNECTIONS="${CONNECTIONS:-128}"
RUNS="${RUNS:-3}"
ROUTE_FRAMES="${ROUTE_FRAMES:-64}"
FRAME_LEN="${FRAME_LEN:-63}"
TUNNEL_BYTES="${TUNNEL_BYTES:-1048576}"
INPUT_FRAGMENT="${INPUT_FRAGMENT:-64}"
INPUT_MODEL="${INPUT_MODEL:-fixed}"
TCP_MSS_BYTES="${TCP_MSS_BYTES:-1460}"
READ_RESERVE="${READ_RESERVE:-16384}"
WRITE_PENDING_BYTES="${WRITE_PENDING_BYTES:-}"
DURATION_SECONDS="${DURATION_SECONDS:-5}"
COALESCER_STATS="${COALESCER_STATS:-1}"
TASKSET_CORES="${TASKSET_CORES:-}"

THROUGHPUT_WITHIN="${THROUGHPUT_WITHIN:-0.05}"
MAX_READS_PER_FLUSH="${MAX_READS_PER_FLUSH:-none}"
MIN_THRESHOLD_BYTES="${MIN_THRESHOLD_BYTES:-1}"
MAX_THRESHOLD_BYTES="${MAX_THRESHOLD_BYTES:-16384}"
MIN_THRESHOLD_POINTS="${MIN_THRESHOLD_POINTS:-5}"
MAX_THRESHOLD_POINTS="${MAX_THRESHOLD_POINTS:-8}"
BATCH_SIZE="${BATCH_SIZE:-1}"

usage() {
  echo "Usage: $0"
  echo ""
  echo "Runs the adaptive coalescing tuner by repeatedly asking tune_coalescing"
  echo "for the next --handoff-flush-bytes value, then running the stream harness."
  echo ""
  echo "Configure with environment variables:"
  echo "  TRANSPORT=cached IMPLEMENTATION=handoff SCENARIO=fragmented COMPLETION=fire_and_forget"
  echo "  WORKER_THREADS='' CONNECTIONS=128 RUNS=3 DURATION_SECONDS=5 COALESCER_STATS=1"
  echo "  ROUTE_FRAMES=64 FRAME_LEN=63 TUNNEL_BYTES=1048576 INPUT_FRAGMENT=64 READ_RESERVE=16384"
  echo "  INPUT_MODEL=fixed TCP_MSS_BYTES=1460 # set INPUT_MODEL=tcp for MSS-sized TCP source chunks"
  echo "  WRITE_PENDING_BYTES='' # defaults to the stream harness budget"
  echo "  TASKSET_CORES='' # pins cached/duplex harness runs with taskset on Linux"
  echo "  THROUGHPUT_WITHIN=0.05 MIN_THRESHOLD_BYTES=1 MAX_THRESHOLD_BYTES=16384"
  echo "  MAX_READS_PER_FLUSH=none # set a number for a hard source-chunk cap"
  echo "  MIN_THRESHOLD_POINTS=5 MAX_THRESHOLD_POINTS=8 BATCH_SIZE=1"
}

if [[ "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ "$SCENARIO" == "all" ]]; then
  echo "ERROR: adaptive coalescing tuning expects one scenario at a time"
  exit 1
fi

tune_args=(
  --implementation "$IMPLEMENTATION"
  --throughput-within "$THROUGHPUT_WITHIN"
  --min-threshold-bytes "$MIN_THRESHOLD_BYTES"
  --max-threshold-bytes "$MAX_THRESHOLD_BYTES"
  --min-threshold-points "$MIN_THRESHOLD_POINTS"
  --max-threshold-points "$MAX_THRESHOLD_POINTS"
  --batch-size "$BATCH_SIZE"
)
if [[ "$MAX_READS_PER_FLUSH" == "none" || "$MAX_READS_PER_FLUSH" == "NONE" ]]; then
  tune_args+=(--no-max-reads-per-flush)
else
  tune_args+=(--max-reads-per-flush "$MAX_READS_PER_FLUSH")
fi

result_csvs=()

echo "=== bytes-handoff adaptive coalescing tuner ==="
echo "transport=$TRANSPORT implementation=$IMPLEMENTATION scenario=$SCENARIO completion=$COMPLETION"
echo "connections=$CONNECTIONS runs=$RUNS duration_seconds=$DURATION_SECONDS input_fragment=$INPUT_FRAGMENT input_model=$INPUT_MODEL tcp_mss_bytes=$TCP_MSS_BYTES"
echo "threshold_range=$MIN_THRESHOLD_BYTES..$MAX_THRESHOLD_BYTES max_reads_per_flush=$MAX_READS_PER_FLUSH min_points=$MIN_THRESHOLD_POINTS max_points=$MAX_THRESHOLD_POINTS batch_size=$BATCH_SIZE throughput_within=$THROUGHPUT_WITHIN"
if [[ -n "$TASKSET_CORES" ]]; then
  echo "taskset_cores=$TASKSET_CORES"
  export TASKSET_CORES
fi

cd "$REPO_ROOT"

for round in $(seq 1 "$MAX_THRESHOLD_POINTS"); do
  echo ""
  echo "=== tuning round $round ==="
  tune_cmd=(cargo run --release --bin tune_coalescing -- --next "${tune_args[@]}")
  if (( ${#result_csvs[@]} > 0 )); then
    tune_cmd+=("${result_csvs[@]}")
  fi
  tune_output="$("${tune_cmd[@]}")"
  printf '%s\n' "$tune_output"

  if grep -q '^recommended_handoff_flush_bytes=' <<<"$tune_output"; then
    exit 0
  fi

  next_line="$(grep '^next_handoff_flush_bytes=' <<<"$tune_output" | tail -n 1 || true)"
  if [[ -z "$next_line" ]]; then
    echo "ERROR: tuner did not emit next_handoff_flush_bytes"
    exit 1
  fi

  thresholds_csv="${next_line#next_handoff_flush_bytes=}"
  IFS=',' read -r -a thresholds <<<"$thresholds_csv"
  for threshold in "${thresholds[@]}"; do
    echo ""
    echo "=== measuring handoff_flush_bytes=$threshold ==="
    harness_cmd=(
      "$SCRIPT_DIR/run-stream-harness.sh"
      --transport "$TRANSPORT"
      --implementation "$IMPLEMENTATION"
      --scenario "$SCENARIO"
      --completion "$COMPLETION"
      --connections "$CONNECTIONS"
      --runs "$RUNS"
      --duration-seconds "$DURATION_SECONDS"
      --route-frames "$ROUTE_FRAMES"
      --frame-len "$FRAME_LEN"
      --tunnel-bytes "$TUNNEL_BYTES"
      --input-fragment "$INPUT_FRAGMENT"
      --input-model "$INPUT_MODEL"
      --tcp-mss-bytes "$TCP_MSS_BYTES"
      --read-reserve "$READ_RESERVE"
      --handoff-flush-bytes "$threshold"
    )
    if [[ -n "$WORKER_THREADS" ]]; then
      harness_cmd+=(--worker-threads "$WORKER_THREADS")
    fi
    if [[ -n "$WRITE_PENDING_BYTES" ]]; then
      harness_cmd+=(--write-pending-bytes "$WRITE_PENDING_BYTES")
    fi
    if [[ "$COALESCER_STATS" == "1" ]]; then
      harness_cmd+=(--coalescer-stats)
    fi

    harness_output="$("${harness_cmd[@]}")"
    printf '%s\n' "$harness_output"
    result_dir="$(awk -F': ' '/Done. Results in:/{print $2}' <<<"$harness_output" | tail -n 1)"
    if [[ -z "$result_dir" ]]; then
      echo "ERROR: harness did not report a results directory"
      exit 1
    fi
    result_csvs+=("$result_dir/runs.csv")
  done
done

echo ""
echo "=== final recommendation ==="
cargo run --release --bin tune_coalescing -- "${result_csvs[@]}" "${tune_args[@]}"
