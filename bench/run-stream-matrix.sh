#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

TRANSPORT="${TRANSPORT:-cached}"
if [[ -z "${IMPLEMENTATIONS:-}" ]]; then
  if [[ "${TRANSPORT:-cached}" == "duplex" || "${TRANSPORT:-cached}" == "cached" ]]; then
    IMPLEMENTATIONS="handoff monoio_handoff bytesmut_handoff manual_vec raw_copy"
  else
    IMPLEMENTATIONS="handoff manual_vec raw_copy"
  fi
fi
SCENARIOS="${SCENARIOS:-fragmented coalesced}"
CONNECTIONS="${CONNECTIONS:-1 4 16 64 256}"
FRAME_SIZES="${FRAME_SIZES:-64 256 1024 4096 16384 65536 1048576}"
INPUT_FRAGMENTS="${INPUT_FRAGMENTS:-}"
COMPLETION="${COMPLETION:-fire_and_forget}"
WORKER_THREADS="${WORKER_THREADS:-8}"
RUNS="${RUNS:-3}"
ROUTE_FRAMES="${ROUTE_FRAMES:-64}"
TUNNEL_BYTES="${TUNNEL_BYTES:-1048576}"
READ_RESERVE="${READ_RESERVE:-16384}"
HANDOFF_FLUSH_BYTES="${HANDOFF_FLUSH_BYTES:-}"
WRITE_PENDING_BYTES="${WRITE_PENDING_BYTES:-}"
DURATION_SECONDS="${DURATION_SECONDS:-0}"

usage() {
  echo "Usage: $0"
  echo ""
  echo "Configure with environment variables:"
  echo "  TRANSPORT='tcp|duplex|cached'"
  echo "  IMPLEMENTATIONS='handoff monoio_handoff bytesmut_handoff manual_vec raw_copy' (monoio_handoff requires TRANSPORT=duplex|cached)"
  echo "  SCENARIOS='fragmented coalesced'"
  echo "  CONNECTIONS='1 4 16 64 256'"
  echo "  FRAME_SIZES='64 256 1024 4096 16384 65536 1048576'"
  echo "  INPUT_FRAGMENTS='64 128 256 512 1024 2048 4096 8192 16384' (optional; defaults to scenario behavior)"
  echo "  COMPLETION='ticket|fire_and_forget'"
  echo "  WORKER_THREADS=8 RUNS=3 ROUTE_FRAMES=64 TUNNEL_BYTES=1048576 READ_RESERVE=16384 HANDOFF_FLUSH_BYTES=16384 WRITE_PENDING_BYTES=131072"
  echo "  DURATION_SECONDS=60"
}

if [[ "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

echo "=== bytes-handoff decision matrix ==="
echo "transport=$TRANSPORT implementations=[$IMPLEMENTATIONS] scenarios=[$SCENARIOS]"
echo "connections=[$CONNECTIONS] frame_sizes=[$FRAME_SIZES] runs=$RUNS worker_threads=$WORKER_THREADS"
if [[ -n "$INPUT_FRAGMENTS" ]]; then
  echo "input_fragments=[$INPUT_FRAGMENTS]"
fi
if [[ -n "$HANDOFF_FLUSH_BYTES" ]]; then
  echo "handoff_flush_bytes=$HANDOFF_FLUSH_BYTES"
fi
if [[ -n "$WRITE_PENDING_BYTES" ]]; then
  echo "write_pending_bytes=$WRITE_PENDING_BYTES"
fi

if [[ -n "$INPUT_FRAGMENTS" ]]; then
  input_fragment_values="$INPUT_FRAGMENTS"
else
  input_fragment_values="default"
fi

for implementation in $IMPLEMENTATIONS; do
  for scenario in $SCENARIOS; do
    for connections in $CONNECTIONS; do
      for frame_size in $FRAME_SIZES; do
        for input_fragment in $input_fragment_values; do
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
          if [[ "$input_fragment" != "default" ]]; then
            cmd+=(--input-fragment "$input_fragment")
          fi
          if [[ -n "$HANDOFF_FLUSH_BYTES" ]]; then
            cmd+=(--handoff-flush-bytes "$HANDOFF_FLUSH_BYTES")
          fi
          if [[ -n "$WRITE_PENDING_BYTES" ]]; then
            cmd+=(--write-pending-bytes "$WRITE_PENDING_BYTES")
          fi
          if [[ "$DURATION_SECONDS" != "0" ]]; then
            cmd+=(--duration-seconds "$DURATION_SECONDS")
          fi

          echo ""
          echo "=== implementation=$implementation scenario=$scenario connections=$connections frame_size=$frame_size input_fragment=$input_fragment ==="
          "${cmd[@]}"
        done
      done
    done
  done
done
