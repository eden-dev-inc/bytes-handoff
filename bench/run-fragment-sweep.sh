#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

detect_cpus() {
  nproc 2>/dev/null || getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.logicalcpu 2>/dev/null || echo 4
}

TRANSPORT="${TRANSPORT:-cached}"
if [[ -z "${IMPLEMENTATIONS:-}" ]]; then
  if [[ "$TRANSPORT" == "duplex" || "$TRANSPORT" == "cached" ]]; then
    IMPLEMENTATIONS="handoff bytesmut_handoff monoio_handoff manual_vec raw_copy"
  else
    IMPLEMENTATIONS="handoff manual_vec raw_copy"
  fi
fi
INPUT_FRAGMENTS="${INPUT_FRAGMENTS:-64 128 256 512 1024 2048 4096 8192 16384}"
INPUT_MODEL="${INPUT_MODEL:-fixed}"
TCP_MSS_BYTES="${TCP_MSS_BYTES:-1460}"
HANDOFF_FLUSH_BYTES_LIST="${HANDOFF_FLUSH_BYTES_LIST:-16384}"
COMPLETION="${COMPLETION:-fire_and_forget}"
WORKER_THREADS="${WORKER_THREADS:-$(detect_cpus)}"
CONNECTIONS="${CONNECTIONS:-128}"
RUNS="${RUNS:-2}"
ROUTE_FRAMES="${ROUTE_FRAMES:-64}"
FRAME_LEN="${FRAME_LEN:-63}"
TUNNEL_BYTES="${TUNNEL_BYTES:-1048576}"
READ_RESERVE="${READ_RESERVE:-16384}"
DURATION_SECONDS="${DURATION_SECONDS:-5}"
COALESCER_STATS="${COALESCER_STATS:-0}"

usage() {
  echo "Usage: $0"
  echo ""
  echo "Configure with environment variables:"
  echo "  TRANSPORT='cached|duplex|tcp'"
  echo "  IMPLEMENTATIONS='handoff bytesmut_handoff monoio_handoff manual_vec raw_copy'"
  echo "  INPUT_FRAGMENTS='64 128 256 512 1024 2048 4096 8192 16384'"
  echo "  INPUT_MODEL='fixed|tcp'"
  echo "  TCP_MSS_BYTES=1460 # used when INPUT_MODEL=tcp"
  echo "  HANDOFF_FLUSH_BYTES_LIST='1 128 256 512 1024 2048 4096 8192 16384'"
  echo "  COMPLETION=fire_and_forget WORKER_THREADS=$(detect_cpus) CONNECTIONS=128 RUNS=2 COALESCER_STATS=0"
  echo "  ROUTE_FRAMES=64 FRAME_LEN=63 TUNNEL_BYTES=1048576 READ_RESERVE=16384 DURATION_SECONDS=5"
}

if [[ "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

echo "=== bytes-handoff input-fragment sweep ==="
echo "transport=$TRANSPORT implementations=[$IMPLEMENTATIONS]"
echo "input_model=$INPUT_MODEL tcp_mss_bytes=$TCP_MSS_BYTES input_fragments=[$INPUT_FRAGMENTS] handoff_flush_bytes_list=[$HANDOFF_FLUSH_BYTES_LIST]"
echo "connections=$CONNECTIONS runs=$RUNS worker_threads=$WORKER_THREADS duration_seconds=$DURATION_SECONDS"
echo "route_frames=$ROUTE_FRAMES frame_len=$FRAME_LEN tunnel_bytes=$TUNNEL_BYTES read_reserve=$READ_RESERVE"
echo "coalescer_stats=$COALESCER_STATS"

for handoff_flush_bytes in $HANDOFF_FLUSH_BYTES_LIST; do
  for implementation in $IMPLEMENTATIONS; do
    for input_fragment in $INPUT_FRAGMENTS; do
      cmd=(
        "$SCRIPT_DIR/run-stream-harness.sh"
        --transport "$TRANSPORT"
        --implementation "$implementation"
        --scenario fragmented
        --completion "$COMPLETION"
        --worker-threads "$WORKER_THREADS"
        --connections "$CONNECTIONS"
        --runs "$RUNS"
        --duration-seconds "$DURATION_SECONDS"
        --route-frames "$ROUTE_FRAMES"
        --frame-len "$FRAME_LEN"
        --tunnel-bytes "$TUNNEL_BYTES"
        --input-fragment "$input_fragment"
        --input-model "$INPUT_MODEL"
        --tcp-mss-bytes "$TCP_MSS_BYTES"
        --read-reserve "$READ_RESERVE"
        --handoff-flush-bytes "$handoff_flush_bytes"
      )
      if [[ "$COALESCER_STATS" == "1" ]]; then
        cmd+=(--coalescer-stats)
      fi

      echo ""
      echo "=== implementation=$implementation input_model=$INPUT_MODEL input_fragment=$input_fragment handoff_flush_bytes=$handoff_flush_bytes ==="
      "${cmd[@]}"
    done
  done
done
