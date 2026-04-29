#!/usr/bin/env bash
set -euo pipefail

SAMPLE_SIZE="${SAMPLE_SIZE:-100}"
MEASUREMENT_TIME="${MEASUREMENT_TIME:-5}"
WARM_UP_TIME="${WARM_UP_TIME:-3}"
BENCH="${BENCH:-all}"
TASKSET_CORES="${TASKSET_CORES:-}"
PERF_STAT="${PERF_STAT:-0}"

cmd=(cargo bench)
if [[ "$BENCH" != "all" ]]; then
  cmd+=(--bench "$BENCH")
fi
cmd+=(-- --sample-size "$SAMPLE_SIZE" --warm-up-time "$WARM_UP_TIME" --measurement-time "$MEASUREMENT_TIME")

if [[ -n "$TASKSET_CORES" ]]; then
  if command -v taskset >/dev/null 2>&1; then
    cmd=(taskset -c "$TASKSET_CORES" "${cmd[@]}")
  else
    echo "WARN: TASKSET_CORES was set, but taskset is not available"
  fi
fi

if [[ "$PERF_STAT" == "1" ]]; then
  if command -v perf >/dev/null 2>&1; then
    cmd=(
      perf stat
      -e cycles,instructions,cache-references,cache-misses,context-switches,cpu-migrations
      "${cmd[@]}"
    )
  else
    echo "WARN: PERF_STAT=1, but perf is not available"
  fi
fi

echo "=== bytes-handoff criterion baseline ==="
echo "sample_size=$SAMPLE_SIZE measurement_time=$MEASUREMENT_TIME warm_up_time=$WARM_UP_TIME bench=$BENCH taskset_cores=${TASKSET_CORES:-none} perf_stat=$PERF_STAT"
"${cmd[@]}"
