# Bench Harness

Script-driven end-to-end workloads for benchmarking `bytes-handoff` under a
content-routed stream shape.

## Why Not `benches/`?

The Criterion benches in `../benches/` isolate individual operations: read
buffering, splitting, write fan-in, and backpressure. This harness is different:

- It drives complete streams through client, proxy, and sink tasks.
- It combines fragmented ingress, routed prefixes, tunnel mode switching,
  preserved tail bytes, and `WriteHandoff` output.
- It emits machine-readable run artifacts for repeated comparison.
- It measures sustained throughput and CPU cost for a workload shape, not one
  operation in isolation.

## Quick Start

```bash
./bench/run-stream-harness.sh --scenario fragmented --runs 5
./bench/run-stream-harness.sh --scenario coalesced --runs 5
./bench/run-stream-harness.sh --transport tcp --scenario fragmented --runs 5
./bench/run-stream-harness.sh --transport tcp --implementation manual_vec --scenario fragmented --runs 5
./bench/run-stream-harness.sh --scenario fragmented --completion fire_and_forget --runs 5
./bench/run-stream-harness.sh --scenario all --worker-threads 8 --connections 128 --runs 5
```

Each run writes `handoff-run-*.txt` files and a `summary.csv` under
`bench/results/stream_<transport>_<implementation>_<scenario>_<completion>_<timestamp>/`.

For a decision matrix across implementation, arrival pattern, frame size, and
concurrency:

```bash
./bench/run-stream-matrix.sh
```

The matrix is configured with environment variables so the run shape is explicit:

```bash
IMPLEMENTATIONS="handoff manual_vec raw_copy" \
SCENARIOS="fragmented coalesced" \
CONNECTIONS="1 4 16 64 256" \
FRAME_SIZES="64 256 1024 4096 16384 65536 1048576" \
RUNS=3 \
./bench/run-stream-matrix.sh
```

For CPU-cost comparisons on Linux, prefer split-process TCP runs with the proxy
service pinned to different cores than the driver and sink. Example:

```bash
./bench/run-stream-harness.sh \
  --transport tcp \
  --implementation handoff \
  --scenario all \
  --completion fire_and_forget \
  --worker-threads 16 \
  --connections 128 \
  --runs 2 \
  --route-frames 64 \
  --frame-len 63 \
  --tunnel-bytes 1048576 \
  --read-reserve 16384 \
  --duration-seconds 10 \
  --service-cores 0-7,16-23 \
  --driver-cores 8-15,24-31
```

Run the same command with `--implementation manual_vec` and
`--implementation raw_copy` for the comparison table. The service-side logs are
`service-run-*.txt`; the driver-observed logs are `handoff-run-*.txt`.

## Workload

Each simulated connection:

1. Writes many fixed-size route frames in configurable fragments.
2. Sends a `TUNNEL\n` marker.
3. Streams an opaque tunnel payload.
4. Proxies routed prefixes and tunnel bytes through `HandoffBuffer` and
   `WriteHandoff`.
5. Drains a sink and asserts the exact byte count.

Scenarios:

- `duplex`: in-memory transport for low-overhead regression checks.
- `tcp`: localhost TCP transport with client, proxy, and sink sockets.
- `handoff`: `HandoffBuffer` plus `WriteHandoff`; this is the crate path.
- `manual_vec`: `Vec<u8>` parser with direct writes; this is the realistic
  baseline you might write by hand.
- `raw_copy`: unparsed async copy; this is a lower bound, not a semantic peer.
- `fragmented`: small client writes, default 64-byte fragments.
- `coalesced`: larger client writes, default fragment size equal to
  `read_reserve`.

Useful fields in each run output:

- `mib_per_sec`
- `streams_per_sec`
- `cpu_avg_cores`
- `cpu_ns_per_byte`
- `latency_p50_micros`
- `latency_p95_micros`
- `latency_p99_micros`
- `latency_p999_micros`
- `latency_max_micros`
- `voluntary_context_switches`
- `involuntary_context_switches`
- `max_rss_bytes`

Use `--duration-seconds 60` for sustained runs that repeat the connection wave
inside one process. That is the shape to use when watching RSS and allocator
behavior over time.

When reading results:

- `mib_per_sec` in the driver log is end-to-end throughput through the complete
  client/proxy/sink path.
- `mib_per_sec` in the service log is the proxy service's measured throughput.
- `cpu_avg_cores` in the service log is the proxy CPU cost; this is the number
  to compare when asking whether `bytes-handoff` costs more CPU than a manual
  implementation.
- `raw_copy` is a lower bound. It intentionally skips the route parsing and
  owned-prefix handoff work that `handoff` performs.

## Criterion Baseline Discipline

Use the baseline wrapper instead of ad hoc one-second Criterion runs:

```bash
./bench/run-criterion-baseline.sh
```

Defaults are `--sample-size 100 --measurement-time 5 --warm-up-time 3`. On
Linux, set `TASKSET_CORES=2-5` to pin the process and `PERF_STAT=1` to collect
cycles, instructions, cache misses, context switches, and CPU migrations.
Frequency governor and turbo policy still need to be controlled on the host.

## Direct Binary

```bash
cargo run --release --features bench-tools --bin bench_stream_harness -- \
  --transport tcp \
  --implementation handoff \
  --scenario fragmented \
  --worker-threads 8 \
  --connections 64
```
