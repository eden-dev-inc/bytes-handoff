# Bench Harness

Script-driven end-to-end workloads for benchmarking `bytes-handoff` under a
content-routed stream shape.

## Why Not `benches/`?

The Criterion benches in `../benches/` isolate individual operations: read
buffering, splitting, write fan-in, and backpressure. This harness is different:

- It drives complete streams through client, proxy, and sink tasks.
- It combines fragmented ingress, routed prefixes, tunnel mode switching,
  preserved tail bytes, and `WriteHandoff` output.
- It can run from cached payloads into a counting sink when you want proxy CPU
  without parallel load-generator CPU.
- It emits machine-readable run artifacts for repeated comparison.
- It measures sustained throughput and CPU cost for a workload shape, not one
  operation in isolation.

## Quick Start

```bash
./bench/run-stream-harness.sh --scenario fragmented --runs 5
./bench/run-stream-harness.sh --scenario coalesced --runs 5
./bench/run-stream-harness.sh --transport cached --scenario fragmented --runs 5
./bench/run-stream-harness.sh --transport tcp --scenario fragmented --runs 5
./bench/run-stream-harness.sh --transport tcp --implementation manual_vec --scenario fragmented --runs 5
./bench/run-stream-harness.sh --transport cached --implementation monoio_handoff --scenario fragmented --runs 5
./bench/run-stream-harness.sh --scenario fragmented --completion fire_and_forget --runs 5
./bench/run-stream-harness.sh --scenario all --worker-threads 8 --connections 128 --runs 5
./bench/run-fragment-sweep.sh
./bench/run-coalescing-tuner.sh
./bench/run-tcp-model-tuner.sh
cargo run --release --bin tune_coalescing -- bench/results --implementation handoff
```

The default transport is `cached`, which reuses prebuilt payload bytes and avoids
client/sink tasks running alongside the proxy. Pass `--transport duplex` for the
older in-memory client/proxy/sink shape, or `--transport tcp` for localhost
socket runs.

Each integrated run writes `handoff-run-*.txt` files and a `summary.csv` under
`bench/results/stream_<transport>_<implementation>_<scenario>_<completion>_<timestamp>_<pid>/`.
Split TCP runs also write `service-run-*.txt` files plus
`service-summary.csv`; use those for proxy throughput and CPU, and use
`handoff-run-*.txt` for driver-observed latency.

For a decision matrix across implementation, arrival pattern, frame size, and
concurrency:

```bash
./bench/run-stream-matrix.sh
```

The matrix is configured with environment variables so the run shape is explicit:

```bash
TRANSPORT=cached \
IMPLEMENTATIONS="handoff monoio_handoff bytesmut_handoff manual_vec raw_copy" \
SCENARIOS="fragmented coalesced" \
CONNECTIONS="1 4 16 64 256" \
FRAME_SIZES="64 256 1024 4096 16384 65536 1048576" \
RUNS=3 \
./bench/run-stream-matrix.sh
```

For proxy/runtime CPU without client and sink tasks running in parallel, use the
cached transport. This is the primary harness shape for comparing
implementations:

```bash
taskset -c 0-15 ./bench/run-stream-harness.sh \
  --transport cached \
  --implementation handoff \
  --scenario all \
  --completion fire_and_forget \
  --worker-threads 16 \
  --connections 128 \
  --runs 2 \
  --duration-seconds 5 \
  --route-frames 64 \
  --frame-len 63 \
  --tunnel-bytes 1048576 \
  --read-reserve 16384 \
  --handoff-flush-bytes 16384
```

`--input-fragment` controls how many bytes the cached reader yields per read
when `--input-model fixed` is selected. `--input-model tcp` instead uses
MSS-sized source chunks, controlled by `--tcp-mss-bytes`; the default is 1460,
which matches the usual TCP payload carried by a 1500-byte Ethernet MTU with
IPv4/TCP headers. That standard Ethernet MSS is the packet-size shape used for
testing, and it is intentionally separate from `read_reserve` because userspace
`read()` calls can receive multiple coalesced TCP segments.
`--handoff-flush-bytes` controls how many buffered tunnel bytes the handoff path
collects before submitting one owned `Bytes` chunk to `WriteHandoff`. The
default handoff flush threshold is 16 KiB, matching
`DEFAULT_WRITE_COALESCE_THRESHOLD`; pass `--handoff-flush-bytes 1` to model
flush-every-read behavior.

To find the crossover between tiny fragmented input and larger coalesced input,
run:

```bash
INPUT_FRAGMENTS="64 128 256 512 1024 2048 4096 8192 16384" \
HANDOFF_FLUSH_BYTES_LIST="1 128 256 512 1024 2048 4096 8192 16384" \
IMPLEMENTATIONS="handoff bytesmut_handoff monoio_handoff manual_vec raw_copy" \
WORKER_THREADS=16 \
CONNECTIONS=128 \
RUNS=2 \
DURATION_SECONDS=5 \
./bench/run-fragment-sweep.sh
```

This holds the protocol frame shape constant and varies only input arrival size
and tunnel handoff flush threshold.

For adaptive discovery, let the tuner choose each next threshold and run the
harness sequentially:

```bash
MAX_THRESHOLD_POINTS=8 \
MIN_THRESHOLD_POINTS=5 \
BATCH_SIZE=1 \
THROUGHPUT_WITHIN=0.05 \
./bench/run-coalescing-tuner.sh
```

For a TCP-like cached benchmark, run the wrapper:

```bash
./bench/run-tcp-model-tuner.sh
```

It sets `INPUT_MODEL=tcp`, keeps the cached transport, enables coalescer stats,
uses `TCP_MSS_BYTES=1460`, and searches thresholds up to 256 KiB. Override
`TASKSET_CORES`, `WORKER_THREADS`, `CONNECTIONS`, `TCP_MSS_BYTES`, or any
regular tuner variable to match the runtime shape you care about.

Run the tuner with the same worker count and CPU affinity you expect in
deployment. For cached or duplex runs on Linux, `TASKSET_CORES` pins the
integrated harness process while leaving Cargo/build work outside the measured
process:

```bash
TASKSET_CORES=0 \
WORKER_THREADS=1 \
MAX_THRESHOLD_BYTES=262144 \
MIN_THRESHOLD_POINTS=11 \
MAX_THRESHOLD_POINTS=15 \
BATCH_SIZE=3 \
./bench/run-coalescing-tuner.sh

TASKSET_CORES=0-15 \
WORKER_THREADS=16 \
MAX_THRESHOLD_BYTES=262144 \
MIN_THRESHOLD_POINTS=11 \
MAX_THRESHOLD_POINTS=15 \
BATCH_SIZE=3 \
./bench/run-coalescing-tuner.sh
```

That script enables `COALESCER_STATS=1` by default, calls
`tune_coalescing --next`, runs the requested `--handoff-flush-bytes` value with
the stream harness, then feeds the resulting `runs.csv` back into the tuner. The
search starts with the range boundaries and a log-space midpoint, fills large
unknown gaps until it has enough curve shape, and then probes neighbors around
the current throughput peak and recommendation.

To score an existing sweep without running more benchmarks, tune from the emitted
`runs.csv` files:

```bash
cargo run --release --bin tune_coalescing -- bench/results \
  --implementation handoff \
  --throughput-within 0.05
```

The CLI uses the crate's `WriteCoalescingTuner` and `WriteCoalescingSearch`
library APIs. It groups compatible runs and chooses the knee of the measured
flush-delay/throughput curve: measured oldest-byte flush wait on one axis,
throughput on the other. If coalescer stats are missing, it falls back to
`input_chunks_per_flush` as the visibility-delay proxy. Use
`--max-reads-per-flush`, `--max-avg-flush-wait-us`, `--max-max-flush-wait-us`, or
`--max-connection-p99-us` for hard budgets before the knee is chosen.

For diagnostic runs that also need observed coalescer-local wait time, pass
`COALESCER_STATS=1` to the sweep or `--coalescer-stats` to
`run-stream-harness.sh`, then tune with `--max-avg-flush-wait-us` or
`--max-max-flush-wait-us`. Keep stats disabled for headline throughput numbers;
the timer calls needed for wait measurement are deliberately opt-in.

The top-level README carries the latest release-validation comparison tables.
Keep this harness document focused on reproducibility: for a fresh threshold
decision, run `run-tcp-model-tuner.sh` on the target hardware and use the emitted
`runs.csv` with `tune_coalescing`. Thresholds below the configured TCP/MSS size
effectively flush one source chunk at a time, while larger thresholds trade
oldest-byte flush delay for fewer handoff submissions. Headline throughput
tables should be gathered with coalescer stats disabled; stats runs are for
understanding the curve and choosing the knee.

Use split-process TCP runs only when you want socket and kernel behavior. The
proxy service can be pinned to a different logical CPU set than the driver and
sink:

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
  --idle-timeout-millis 50 \
  --service-cores 0-15 \
  --driver-cores 16-31
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
- `cached`: prebuilt `PayloadSet` reader plus counting sink. This removes
  client/sink tasks and is the preferred transport when profiling proxy CPU.
- `tcp`: localhost TCP transport with client, proxy, and sink sockets.
- `handoff`: `HandoffBuffer` plus `WriteHandoff`; this is the crate path.
- `monoio_handoff`: Monoio `AsyncReadRent`/`AsyncWriteRent` path through
  `HandoffBuffer`, with direct thread-local writes from the shard task. This
  is the Monoio path that avoids cross-thread and cross-task write handoff
  coordination.
- `bytesmut_handoff`: direct `BytesMut::read_buf` plus
  `split_to(...).freeze()` and the same `WriteHandoff` output path as
  `handoff`. Use this to compare against the read-mutable buffer path that
  `HandoffBuffer` wraps; it is the closest behavior-preserving `BytesMut` peer in
  this harness.
- `manual_vec`: `Vec<u8>` parser with direct writes; this is a direct-parser
  control that omits owned cross-task handoff and bounded queued writes.
- `raw_copy`: unparsed async copy; this is a lower bound, not a semantic peer.
- `fragmented`: small client writes, default 64-byte fragments.
- `coalesced`: larger client writes, default fragment size equal to
  `read_reserve`.
- `input_model=fixed`: every cached read or client write uses
  `input_fragment`.
- `input_model=tcp`: cached reads or client writes are capped at
  `tcp_mss_bytes`, default 1460 bytes. This models standard Ethernet MSS-sized
  TCP payloads for testing rather than a userspace `read()` buffer size.
- `handoff_flush_bytes`: tunnel-mode flush threshold for the handoff
  implementations. The default is 16 KiB; use `1` for immediate flushes.

Useful fields in each run output:

- `mib_per_sec`
- `streams_per_sec`
- `cpu_avg_cores`
- `cpu_avg_cores_per_worker`
- `cpu_utilization_pct_per_worker`
- `cpu_ns_per_byte`
- `input_model`
- `tcp_mss_bytes`
- `handoff_flush_bytes`
- `coalescer_stats_enabled`
- `coalescer_input_chunks`
- `coalescer_flushes`
- `coalescer_avg_buffered_chunks_per_flush`
- `coalescer_max_chunks_per_flush`
- `coalescer_avg_flush_wait_nanos`
- `coalescer_max_flush_wait_nanos`
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

- `cached` transport results are proxy/runtime results, not socket throughput.
  The input comes from prebuilt payload bytes and output is counted in memory.
- `mib_per_sec` in the driver log is end-to-end throughput through the complete
  client/proxy/sink path.
- `mib_per_sec` in the service log is the proxy service's measured throughput.
- `cpu_avg_cores` in the service log is the proxy CPU cost; this is the number
  to compare when asking whether `bytes-handoff` costs more CPU than the
  baseline implementations.
- `cpu_avg_cores_per_worker` divides total process CPU by `--worker-threads`;
  for `monoio_handoff`, each worker thread owns one Monoio runtime and a
  partition of the cached, duplex, or split TCP connections.
- `raw_copy` is a lower bound. It intentionally skips the route parsing and
  owned-prefix handoff work that `handoff` performs.
- `manual_vec` is a direct-parser baseline, not a semantic peer for owned
  handoff. It is useful because it shows how much cost remains after you remove
  `Bytes` ownership, queueing, completion, and byte-budget backpressure.
- `bytesmut_handoff` is the closest behavior-preserving read-side peer because
  it keeps the same `WriteHandoff` output path while replacing `HandoffBuffer`
  with direct `BytesMut::read_buf`. When fragmented input creates many tiny
  prefixes, compare both paths on the workload: `HandoffBuffer` uses compact
  owned prefixes to avoid retaining larger read-buffer allocations, while the
  direct `BytesMut` path is the lower-level baseline for read-mutable state.

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
