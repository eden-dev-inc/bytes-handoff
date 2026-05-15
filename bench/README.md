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

`--input-fragment` controls how many bytes the cached reader yields per read.
`--handoff-flush-bytes` controls how many buffered tunnel bytes the handoff path
collects before submitting one owned `Bytes` chunk to `WriteHandoff`. The
default handoff flush threshold is `read_reserve`; pass
`--handoff-flush-bytes 1` to model the old flush-every-read behavior.

To find the crossover between tiny fragmented input and larger coalesced input,
run:

```bash
INPUT_FRAGMENTS="64 128 256 512 1024 2048 4096 8192 16384" \
HANDOFF_FLUSH_BYTES_LIST="1 256 1024 4096 16384" \
IMPLEMENTATIONS="handoff bytesmut_handoff monoio_handoff manual_vec raw_copy" \
WORKER_THREADS=16 \
CONNECTIONS=128 \
RUNS=2 \
DURATION_SECONDS=5 \
./bench/run-fragment-sweep.sh
```

This holds the protocol frame shape constant and varies only input arrival size
and tunnel handoff flush threshold.

On a 16 physical core Ubuntu 24.04 Linux server, a focused one-run sweep of
`handoff` with 128 cached connections, 64 route frames, 1 MiB tunnel payloads,
16 workers, and 3 second target runs showed why the default handoff flush
threshold is 16 KiB:

| input chunk | flush every read | flush at 16 KiB |
|---:|---:|---:|
| 64 B | 2045 MiB/s | 33289 MiB/s |
| 128 B | 2210 MiB/s | 35600 MiB/s |
| 256 B | 4588 MiB/s | 34836 MiB/s |
| 512 B | 4531 MiB/s | 35101 MiB/s |
| 1 KiB | 27181 MiB/s | 35408 MiB/s |
| 2 KiB | 34290 MiB/s | 37233 MiB/s |
| 4 KiB | 37499 MiB/s | 36199 MiB/s |
| 8 KiB | 38040 MiB/s | 37642 MiB/s |
| 16 KiB | 38624 MiB/s | 38291 MiB/s |

The immediate flush path crosses into the same range around 2-4 KiB input
chunks. Below that, per-chunk queueing and notification dominate, so collecting
tunnel bytes up to `read_reserve` is the better default for this workload.

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
  `HandoffBuffer` plus the single-threaded `MonoioWriteHandoff`; currently
  available for `duplex` and `cached` transport only.
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
- `handoff_flush_bytes`: tunnel-mode flush threshold for the handoff
  implementations. The default is `read_reserve`; use `1` for immediate flushes.

Useful fields in each run output:

- `mib_per_sec`
- `streams_per_sec`
- `cpu_avg_cores`
- `cpu_avg_cores_per_worker`
- `cpu_utilization_pct_per_worker`
- `cpu_ns_per_byte`
- `handoff_flush_bytes`
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
  partition of the cached or duplex connections.
- `raw_copy` is a lower bound. It intentionally skips the route parsing and
  owned-prefix handoff work that `handoff` performs.
- `manual_vec` is a direct-parser baseline, not a semantic peer for owned
  handoff. It is useful because it shows how much cost remains after you remove
  `Bytes` ownership, queueing, completion, and byte-budget backpressure.
- `bytesmut_handoff` is the closest behavior-preserving read-side peer because
  it keeps the same `WriteHandoff` output path while replacing `HandoffBuffer`
  with direct `BytesMut::read_buf`. When fragmented input creates many tiny
  prefixes, `HandoffBuffer` can be faster because it copies small prefixes into
  compact owned `Bytes` instead of freezing views that retain larger `BytesMut`
  allocations.

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
