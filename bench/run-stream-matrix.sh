#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

TRANSPORT="${TRANSPORT:-tcp}"
IMPLEMENTATIONS="${IMPLEMENTATIONS:-handoff manual_vec raw_copy}"
SCENARIOS="${SCENARIOS:-fragmented coalesced}"
CONNECTIONS="${CONNECTIONS:-1 4 16 64 256}"
FRAME_SIZES="${FRAME_SIZES:-64 256 1024 4096 16384 65536 1048576}"
COMPLETION="${COMPLETION:-fire_and_forget}"
WORKER_THREADS="${WORKER_THREADS:-8}"
RUNS="${RUNS:-3}"
ROUTE_FRAMES="${ROUTE_FRAMES:-64}"
TUNNEL_BYTES="${TUNNEL_BYTES:-1048576}"
READ_RESERVE="${READ_RESERVE:-16384}"
DURATION_SECONDS="${DURATION_SECONDS:-0}"

usage() {
  echo "Usage: $0"
  echo ""
  echo "Configure with environment variables:"
  echo "  TRANSPORT='tcp|duplex'"
  echo "  IMPLEMENTATIONS='handoff manual_vec raw_copy'"
  echo "  SCENARIOS='fragmented coalesced'"
  echo "  CONNECTIONS='1 4 16 64 256'"
  echo "  FRAME_SIZES='64 256 1024 4096 16384 65536 1048576'"
  echo "  COMPLETION='ticket|fire_and_forget'"
  echo "  WORKER_THREADS=8 RUNS=3 ROUTE_FRAMES=64 TUNNEL_BYTES=1048576 READ_RESERVE=16384"
  echo "  DURATION_SECONDS=60"
}

if [[ "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

echo "=== bytes-handoff decision matrix ==="
echo "transport=$TRANSPORT implementations=[$IMPLEMENTATIONS] scenarios=[$SCENARIOS]"
echo "connections=[$CONNECTIONS] frame_sizes=[$FRAME_SIZES] runs=$RUNS worker_threads=$WORKER_THREADS"

for implementation in $IMPLEMENTATIONS; do
  for scenario in $SCENARIOS; do
    for connections in $CONNECTIONS; do
      for frame_size in $FRAME_SIZES; do
        frame_len=$((frame_size - 1))
        if [[ "$frame_len" -lt 16 ]]; then
          frame_len=16
        fi

        cmd=(
          "$SCRIPT_DIR/run-stream-harness.sh"
          --transport "$TRANSPORT"
          --implementation "$implementation"
          --scenario "$scenario"
          --completion "$COMPLETION"
          --worker-threads "$WORKER_THREADS"
          --connections "$connections"
          --runs "$RUNS"
          --route-frames "$ROUTE_FRAMES"
          --frame-len "$frame_len"
          --tunnel-bytes "$TUNNEL_BYTES"
          --read-reserve "$READ_RESERVE"
        )
        if [[ "$DURATION_SECONDS" != "0" ]]; then
          cmd+=(--duration-seconds "$DURATION_SECONDS")
        fi

        echo ""
        echo "=== implementation=$implementation scenario=$scenario connections=$connections frame_size=$frame_size ==="
        "${cmd[@]}"
      done
    done
  done
done
