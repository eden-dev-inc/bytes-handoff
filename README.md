# bytes-handoff

`bytes-handoff` is a small Rust crate for moving owned byte buffers across
async I/O boundaries.

It does not replace `AsyncRead` or `AsyncWrite`. It layers on top of them so
protocol code can:

- read bytes as soon as they arrive
- keep nonblocking reads in safe, owned mutable buffer state
- peek at incomplete input without committing
- preserve unconsumed tails across parser or mode boundaries
- split complete prefixes into `Bytes` for cheap cross-task handoff
- submit owned `Bytes` writes to an async writer without borrowing memory until
  the socket finishes
- bound queued writes by item count and byte count

The zero-copy claim here is intentionally scoped: `BytesMut`/`Bytes` avoid
extra application-level copies after socket ingress. They do not make TCP
kernel-to-userspace reads zero-copy.

The main use case is content-routed streaming I/O: many independent connections,
each with bytes arriving in arbitrary fragments, where routing decisions depend
on the stream content. In that setting the read buffer is protocol state, not
temporary scratch memory.

`bytes-handoff` is for protocols where the read buffer itself is part of the
protocol state: partial frames must survive, complete prefixes need owned
handoff, or a parser may switch into a raw tunnel without losing already-read
bytes.

The performance goal is not to beat raw copying. The goal is to make byte-zero
introspection, tail preservation, owned prefix handoff, and bounded queued
writes cheap enough for hot content-routed streams. The meaningful comparison
is therefore against code that preserves the same behavior. The benchmark suite
also includes `manual_vec` and `raw_copy` controls, but those are lower bounds:
they deliberately skip owned handoff, bounded queued writes, or parsing work
that this crate keeps.

## Feature Flags

The default crate API targets Tokio `AsyncRead` and `AsyncWrite`.

Enable `monoio` when the application runs thread-local Monoio shards and wants
to read from `monoio::io::AsyncReadRent` sources without changing the
`HandoffBuffer` parsing model:

```toml
bytes-handoff = { version = "1.3", features = ["monoio"] }
```

Enable `telemetry` to attach `fast-telemetry` counters, histograms, and gauges to
`HandoffBuffer` read paths. The base telemetry feature can snapshot metrics and
serialize Prometheus or DogStatsD text:

```toml
bytes-handoff = { version = "1.3", features = ["telemetry"] }
```

Enable `telemetry-otlp` or `telemetry-clickhouse` when the parent application
wants to serialize the read metrics into those `fast-telemetry` formats. Enable
`telemetry-export`, `telemetry-export-dogstatsd`, `telemetry-export-otlp`, or
`telemetry-export-clickhouse` when the parent also wants this crate to re-export
`fast_telemetry_export` exporter loops for convenient wiring.

Enable `telemetry-monoio` when the parent application wants the crate's Monoio
read API plus `fast-telemetry-export`'s Monoio-native exporter and local
flushing helpers:

```toml
bytes-handoff = { version = "1.3", features = ["telemetry-monoio"] }
```

The telemetry feature is disabled by default. When it is off, the optional
`fast-telemetry` dependency, the buffer telemetry field, and the instrumentation
calls are not compiled into the crate. The plain `telemetry` feature keeps
metric recording runtime-neutral. Export loops are caller-owned and only become
available through the explicit `telemetry-export*` features. The
`telemetry-monoio` feature also enables the crate's `monoio` feature.

The `bench-tools` feature is for this repository's harness binaries and should
not be needed by library users.

## What This Optimizes

The crate keeps the following behavior in the hot path:

- incomplete frame tails must survive future reads
- complete prefixes need to become owned `Bytes`
- already-read tails must survive parser mode changes, such as routed prefix to
  raw tunnel
- many small prefixes should not retain large read-buffer allocations
- queued writes need item and byte limits
- producers may need optional completion tickets from the writer task

In the cached harness, `handoff` stays close to the direct `BytesMut`
behavior-preserving baseline while keeping the crate API guarantees. The
16 KiB default coalescer recovers much of the fragmented-path throughput by
batching MSS-sized tunnel arrivals; direct-parser controls remain faster
because they skip owned cross-task handoff, bounded queued writes, or both. The
flush threshold is a caller policy; the crate's write coalescer uses a
TCP/MSS-tuned 16 KiB default and can model immediate flushing with
`WriteCoalescerConfig::immediate()`. Use the harness tuner on your workload to
pick a throughput-efficient point within your downstream visibility-latency
budget.

## Examples

The repository includes small runnable examples:

- `line_protocol`: incremental line parsing with a partial tail.
- `length_prefix`: length-prefixed frames where the header arrives before the
  full payload.
- `content_routing`: inspect buffered bytes, route complete safe prefixes, then
  switch to a raw tunnel while preserving already-read tail bytes.
- `read_and_drain`: inspect buffered bytes with a cursor and commit only the
  bytes accepted by the parser.
- `read_policy`: configure prefix-copy and Monoio sparse-read handoff policy.
- `write_handoff`: submit owned bytes to an async writer and await completion.
- `write_coalescer`: batch tiny fire-and-forget writes and flush at a message
  boundary.
- `coalescing_tuner`: choose a write coalescing threshold from measured
  throughput and flush-delay points.
- `monoio_line_protocol`: read newline-delimited frames from a Monoio
  `AsyncReadRent` source. Requires the `monoio` feature.
- `read_telemetry`: attach `fast-telemetry` read metrics. Requires the
  `telemetry` feature.

Run one with:

```bash
cargo run --example content_routing
cargo run --example read_and_drain
cargo run --example read_policy
cargo run --example write_coalescer
cargo run --example coalescing_tuner
cargo run --features monoio --example monoio_line_protocol
cargo run --features telemetry --example read_telemetry
```

## Read Handoff

```rust
use bytes::Bytes;
use bytes_handoff::HandoffBuffer;

/// Reads available bytes, splits one complete line, and hands it off.
///
/// `if let` drains at most one frame per read for illustration; real protocol
/// loops typically use `while let` to drain every complete frame before the
/// next `read_available` call. See `examples/line_protocol.rs`.
async fn read_one_line<R>(reader: &mut R) -> Result<(), Box<dyn std::error::Error>>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut buffer = HandoffBuffer::new(64 * 1024);

    buffer.read_available(reader).await?;

    if let Some(newline) = buffer.peek().iter().position(|b| *b == b'\n') {
        let line = buffer.split_prefix(newline + 1)?;
        send_to_worker(line);
    }

    Ok(())
}

/// Sends an owned byte slice to sync or async protocol work.
fn send_to_worker(_: Bytes) {}
```

## Read Drain And Policy

`HandoffBuffer::drain` gives parsers a cursor over buffered bytes and commits
the consumed prefix only if the parser closure succeeds. `read_and_drain`
combines one read with that cursor pattern:

```rust
use bytes_handoff::{BufferError, HandoffBuffer};

async fn drain_lines<R>(reader: &mut R) -> Result<Vec<String>, Box<dyn std::error::Error>>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut buffer = HandoffBuffer::new(64 * 1024);
    let (_, frames) = buffer
        .read_and_drain(reader, |cursor| {
            let mut frames = Vec::new();
            while let Some(newline) = cursor.remaining().iter().position(|b| *b == b'\n') {
                let frame = &cursor.remaining()[..newline + 1];
                frames.push(String::from_utf8_lossy(frame).trim_end().to_owned());
                cursor.consume(newline + 1)?;
            }
            Ok::<_, BufferError>(frames)
        })
        .await?;
    Ok(frames)
}
```

`HandoffBufferPolicy` controls small immutable prefix copies and the Monoio
sparse-read buffer swap heuristic. The default keeps tiny prefixes from holding
large read allocations while still allowing larger prefixes to use
`BytesMut::split_to(...).freeze()`.

## Write Handoff

```rust
use bytes::Bytes;
use bytes_handoff::{WriteCoalescer, WriteHandoff, WriteHandoffConfig};

/// Submits owned bytes to an async writer without blocking the producer.
async fn submit_owned_write<W>(writer: W) -> Result<(), Box<dyn std::error::Error>>
where
    W: tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let handoff = WriteHandoff::spawn(writer, WriteHandoffConfig::default());
    let mut coalescer = WriteCoalescer::new(handoff.clone());

    coalescer
        .write_fire_and_forget(Bytes::from_static(b"owned bytes"))
        .await?;
    coalescer.flush().await?;
    handoff.flush().await?;

    // Use `try_write` or `write` instead when the producer needs a completion
    // ticket for this chunk.
    Ok(())
}
```

`WriteHandoff::flush()` waits for all previously accepted writes to reach the
underlying writer and then flushes that writer. Use it when downstream flush
semantics matter.
Use `WriteHandoff::drain()` instead when you only need the accepted-write
barrier and do not need to call `AsyncWrite::flush` on the underlying writer.

`WriteHandoffConfig::default()` uses the measured default write budgets:
`DEFAULT_WRITE_MAX_ITEMS` is 1024 queued items, `DEFAULT_WRITE_COALESCE_THRESHOLD`
is 16 KiB, `DEFAULT_WRITE_PENDING_CHUNKS` is 8, and
`DEFAULT_WRITE_MAX_PENDING_BYTES` is therefore 128 KiB. Tune
`max_pending_bytes` upward when producers can legitimately accumulate more than
eight coalesced chunks without downstream progress; tune it downward to put a
tighter cap on queued owned bytes. Tune `max_items` when many small producers can
fill the item queue before they fill the byte budget. Use
`WriteHandoffConfig::default().with_max_pending_bytes(...)` or
`with_max_items(...)` when you want to change one budget and keep the rest of the
measured defaults.

Completion tickets can expose the write completion record when producers need
handoff-local timing:

```rust
let ticket = handoff
    .write_with_completion_stats(Bytes::from_static(b"SET key value\r\n"))
    .await?;
let completion = ticket.wait_completion().await?;
let stats = completion.stats();

// `stats` includes accepted-to-write queue time, writer-call time, and
// accepted-to-completion handoff time.
record_write_metrics(stats.queue_nanos, stats.write_nanos, stats.e2e_nanos);
completion.into_result()?;
```

For request metadata such as Eden command count or an upstream request start
time, use a completion callback and capture that metadata in the closure:

```rust
let command_count = request.command_count();
let request_started_at = std::time::Instant::now();
let bytes = request.into_response_bytes();

handoff
    .write_with_completion_callback(bytes, move |completion| {
        let app_e2e_nanos = request_started_at.elapsed().as_nanos();
        let stats = completion.stats();
        record_response_metrics(command_count, app_e2e_nanos, stats);
        if let Err(err) = completion.into_result() {
            record_write_error(command_count, err);
        }
    })
    .await?;
```

The callback is registered only after the write is accepted into the handoff. If
submission returns an immediate error, no completion callback runs and the caller
still owns that synchronous failure path.

`WriteCoalescer` is an optional fire-and-forget helper around `WriteHandoff`.
It collects small adjacent writes up to a byte threshold before submitting one
owned `Bytes` chunk to the writer task. The default threshold is 16 KiB
(`DEFAULT_WRITE_COALESCE_THRESHOLD`). Configure it with
`WriteCoalescer::with_threshold`, or use `WriteCoalescerConfig::immediate()` for
flush-every-write behavior. Always call `flush()` at message boundaries, before
switching modes when downstream latency matters, and before closing. Keep the
`WriteHandoffConfig` pending-byte budget large enough to cover the coalescing
threshold plus any message-boundary prefix or tail writes that may be accepted
before the writer task catches up.

The tradeoff is direct: a larger threshold reduces queueing, notification, and
writer-task overhead under tiny fragmented input; a smaller threshold makes
bytes visible to the downstream writer sooner. The benchmark harness includes a
tuner that maps measured oldest-byte flush delay against throughput and chooses
the knee of that curve.
Completion-ticket writes are not coalesced by this helper, because their
completion boundary is part of the observable behavior.

## Coalescing Tuning

The tuner is intentionally not "pick the fastest number." It uses flush delay as
one axis and throughput as the other. With coalescer stats enabled, flush delay
is the measured oldest-byte wait from the first buffered byte until the batch is
flushed. Without stats, the tuner falls back to estimated
`input_chunks_per_flush` as a portable visibility-delay proxy.

The scorer builds the Pareto frontier of the measured curve, normalizes both
axes, and chooses the point with the largest distance above the straight line
from the lowest-delay point to the highest-throughput point. That is a discrete
knee/inflection estimate: the point where the throughput gained by waiting
longer starts to flatten. Optional budgets such as `max_reads_per_flush`,
`max_avg_flush_wait_micros`, `max_max_flush_wait_micros`, or
`max_connection_p99_micros` are hard constraints applied before the knee is
chosen.

For discovery runs, use `WriteCoalescingSearch` or
`cargo run --release --bin tune_coalescing -- --next`. The adaptive search starts
with the minimum threshold, maximum threshold, and log-space midpoint; fills the
largest unknown gaps until it has enough curve shape; then probes the immediate
neighbors around the current recommendation and the current throughput peak. It
stops when those local neighbors are measured, or when `max_threshold_points` is
reached. This gives a binary-search-style hardware tuning loop without requiring
a full sweep. For cached or duplex Linux harness runs, set `TASKSET_CORES`
alongside `WORKER_THREADS` so the tuner measures the CPU shape you plan to run.
Use `./bench/run-tcp-model-tuner.sh` or pass `--input-model tcp` to model a
TCP stream as MSS-sized source chunks. The default `--tcp-mss-bytes 1460`
matches the usual TCP payload carried by a 1500-byte Ethernet MTU with IPv4/TCP
headers. That standard Ethernet MSS is the packet-size shape used for testing;
it is not the same thing as a userspace `read()` size, which can be larger when
the kernel has already coalesced multiple segments.

When coalescer stats are enabled, the CLI reports the measured
`input_chunks_per_flush` from the harness. Without stats, it falls back to
`ceil(threshold_bytes / input_fragment_bytes)`. For example, with 64 byte source
chunks and a 16 KiB threshold, bytes can wait for up to 256 source chunks before
the tunnel chunk is flushed. With `--input-model tcp`, that fallback uses
`tcp_mss_bytes` instead of userspace `read()` size.

The release validation numbers below include both immediate TCP/MSS flushing and
the 16 KiB default. On that cached shape, the default raises `handoff`
throughput from 27002.68 MiB/s to 33523.05 MiB/s while keeping p99 connection
latency in the same low-millisecond range. Read-side parsing still sees byte 0
as soon as it is read; the threshold only controls when already accepted tunnel
bytes are batched into the downstream writer. With standard 1460-byte TCP/MSS
source chunks, 16 KiB is roughly twelve source chunks, or less at end-of-stream.
That makes it a useful default, not a universal optimum. Run the tuner on the
hardware and workload you plan to ship when visibility latency is part of the
contract.

```rust
use bytes_handoff::{
    WriteCoalescingMeasurement, WriteCoalescingTuner, WriteCoalescingTunerConfig,
};

let tuner = WriteCoalescingTuner::new(WriteCoalescingTunerConfig {
    ..WriteCoalescingTunerConfig::default()
})?;

let recommendation = tuner.recommend([
    WriteCoalescingMeasurement {
        threshold_bytes: 64,
        input_fragment_bytes: 64,
        observed_input_chunks_per_flush: None,
        throughput_mib_per_sec: 100.0,
        cpu_ns_per_byte: 0.0,
        connection_p99_micros: None,
        avg_flush_wait_micros: Some(1.0),
        max_flush_wait_micros: None,
    },
    WriteCoalescingMeasurement {
        threshold_bytes: 1024,
        input_fragment_bytes: 64,
        observed_input_chunks_per_flush: None,
        throughput_mib_per_sec: 900.0,
        cpu_ns_per_byte: 0.0,
        connection_p99_micros: None,
        avg_flush_wait_micros: Some(10.0),
        max_flush_wait_micros: None,
    },
    WriteCoalescingMeasurement {
        threshold_bytes: 8192,
        input_fragment_bytes: 64,
        observed_input_chunks_per_flush: None,
        throughput_mib_per_sec: 980.0,
        cpu_ns_per_byte: 0.0,
        connection_p99_micros: None,
        avg_flush_wait_micros: Some(50.0),
        max_flush_wait_micros: None,
    },
    WriteCoalescingMeasurement {
        threshold_bytes: 16384,
        input_fragment_bytes: 64,
        observed_input_chunks_per_flush: None,
        throughput_mib_per_sec: 1000.0,
        cpu_ns_per_byte: 0.0,
        connection_p99_micros: None,
        avg_flush_wait_micros: Some(100.0),
        max_flush_wait_micros: None,
    },
])?;

assert_eq!(recommendation.threshold_bytes(), 1024);
assert_eq!(recommendation.reads_per_flush(), 16);
```

The same ranking is available as an adaptive planner:

```rust
use bytes_handoff::{WriteCoalescingSearch, WriteCoalescingSearchConfig};

let search = WriteCoalescingSearch::new(WriteCoalescingSearchConfig::default())?;
let first_step = search.step(std::iter::empty())?;
assert_eq!(first_step.thresholds(), &[1]);
```

## Telemetry Feature

The `telemetry` feature exposes `HandoffReadTelemetry` and
`HandoffReadTelemetryHandle`, backed by `fast-telemetry 0.7.1`. Attach a handle to
a buffer when you want read-path counters, size histograms, peak gauges,
snapshots, or text serialization:

```rust
use bytes_handoff::{HandoffBuffer, HandoffReadTelemetry, HandoffReadTelemetryHandle};

let telemetry = HandoffReadTelemetry::with_available_parallelism();
let handle = HandoffReadTelemetryHandle::from_arc(&telemetry);
let mut buffer = HandoffBuffer::new(64 * 1024).with_telemetry(handle);

// Read, split, advance, freeze, and tail handoff operations update metrics.
let snapshot = telemetry.snapshot();
let prometheus = telemetry.export_prometheus();
let mut dogstatsd = String::new();
telemetry.export_dogstatsd(&mut dogstatsd, &[("component", "bytes_handoff")]);
println!("{snapshot:?}");
println!("{prometheus}");
```

If the parent application already owns a `fast-telemetry` runtime, pass that
runtime to `bytes-handoff` as `Some(parent)`. The read metrics are registered
under the `bytes_handoff_read` scope and the returned direct handle records into
the parent runtime. Passing `None` starts a local `bytes-handoff` runtime with
available parallelism:

```rust
use bytes_handoff::{HandoffBuffer, HandoffReadTelemetryRuntime};

let parent_read_telemetry: Option<HandoffReadTelemetryRuntime> =
    Some(fast_telemetry::Runtime::new(fast_telemetry::RuntimeConfig::default()));
let mut buffer = HandoffBuffer::new(64 * 1024)
    .with_optional_telemetry(parent_read_telemetry.clone());

// The parent app can snapshot or export the same metrics from its normal
// telemetry task by visiting the shared runtime.
let runtime = parent_read_telemetry.expect("parent telemetry configured");
assert_eq!(runtime.registered_metrics_len(), 1);

let mut local_buffer = HandoffBuffer::new(64 * 1024)
    .with_optional_telemetry(None);
```

`HandoffReadTelemetry::from_optional_runtime` and
`HandoffReadTelemetryHandle::from_optional_runtime` provide the same
`Some(parent)` / `None(local)` behavior when the parent app wants to keep the
wrapper or handle directly. The older `from_optional_shared_metrics` helpers
still accept a raw `Arc<HandoffReadMetrics>` for tests or embedded collectors
that do not use the `fast-telemetry` runtime registry.
`HandoffReadTelemetry::visit_metrics` and the
`HandoffReadMetrics::visit_metrics` method expose the structured
`fast_telemetry::MetricVisitor` path for custom in-process collectors.

The read metric set covers successful reads, read errors, buffer-limit read
errors, read-size and buffered-size distributions, peak buffered bytes, buffer
growth, prefix split strategy, tail handoff, advance/freeze activity, and
Monoio read-buffer copy versus swap decisions.

By default, attached `HandoffReadTelemetryHandle`s use a grouped
`fast_telemetry::CounterSet` buffer. Related counter deltas are accumulated
locally and flushed to the shared `CounterSet` every
`DEFAULT_READ_COUNTER_BUFFER_FLUSH_EVERY` operations (currently `1_024`), on
drop, or when `flush_counter_buffer` / `HandoffBuffer::flush_telemetry` is
called. This keeps the hot path on the efficient grouped-counter update path
while avoiding a shared counter write for every read or prefix split.

```rust
use bytes_handoff::{
    DEFAULT_READ_COUNTER_BUFFER_FLUSH_EVERY, HandoffBuffer, HandoffReadTelemetry,
    HandoffReadTelemetryHandle,
};

let telemetry = HandoffReadTelemetry::with_available_parallelism();
let handle = HandoffReadTelemetryHandle::from_arc(&telemetry)
    .with_counter_flush_every(DEFAULT_READ_COUNTER_BUFFER_FLUSH_EVERY);
let mut buffer = HandoffBuffer::new(64 * 1024).with_telemetry(handle);

buffer.flush_telemetry();
let exact = telemetry.snapshot();
```

Call `flush_counter_buffer` or `HandoffBuffer::flush_telemetry` before an exact
snapshot or export if a buffer is still live. Dropping the handle also flushes
pending counter deltas. Embedded collectors that need every counter immediately
visible can opt out with `HandoffReadTelemetryHandle::with_direct_counters()`,
trading off the grouped default path for direct counter updates.

For exporter loops, the parent application owns the task and destination. The
crate exposes the metric schema and export methods:

```rust
use bytes_handoff::{HandoffReadMetricsDogStatsDState, HandoffReadTelemetry};

let telemetry = HandoffReadTelemetry::with_available_parallelism();
let mut state = HandoffReadMetricsDogStatsDState::new();
let tags = [("service", "gateway")];

tokio::spawn(bytes_handoff::telemetry_export::dogstatsd::run(
    config,
    cancel,
    move |out| telemetry.export_dogstatsd_delta(out, &tags, &mut state),
));
```

With `telemetry-otlp`, call `telemetry.export_otlp(out, timestamp)` from an
OTLP exporter closure. With `telemetry-clickhouse`, call
`telemetry.export_clickhouse(batch, timestamp)` from a ClickHouse exporter
closure. Monoio applications can enable `telemetry-monoio` and use
`bytes_handoff::telemetry_export::dogstatsd::run_monoio` or
`bytes_handoff::telemetry_export::otlp::run_monoio`; ClickHouse export should
usually stay on a private Tokio exporter because the ClickHouse transport is
Tokio-based.

The feature is intentionally opt-in. Without `features = ["telemetry"]`, the
dependency and all read-buffer instrumentation are absent from the compiled
crate. Use the narrow `telemetry-export*` features when the owning application
wants exporter-loop re-exports, and use `telemetry-monoio` when those exporters
should run inside Monoio workers.

## Monoio Feature

Enable `monoio` to use Monoio's ownership-based I/O traits directly:

```toml
bytes-handoff = { version = "1.3", features = ["monoio"] }
```

With the feature enabled, `HandoffBuffer::read_available_monoio` accepts
`monoio::io::AsyncReadRent`. The intended Monoio shape is a true thread-local
shard: one runtime per shard, local parser state, and direct
`AsyncWriteRent` writes from the same shard task. The Tokio API remains
available unchanged for cross-task owned write handoff. See
[`examples/monoio_line_protocol.rs`](examples/monoio_line_protocol.rs) for a
minimal feature-gated read example.

## Benchmarks

The repository includes Criterion benchmarks for the main operations this crate
adds around async I/O:

- incremental reads into persistent state
- complete-frame splitting into owned `Bytes`
- preserving an already-read tail when parser mode changes
- bounded owned write submission and completion tracking
- byte-budget backpressure

Run them with:

```bash
cargo bench
```

For a defensible local baseline:

```bash
./bench/run-criterion-baseline.sh
```

The baseline script uses `--sample-size 100 --measurement-time 5` by default.
On Linux, set `TASKSET_CORES=2-5` to pin the run with `taskset` and
`PERF_STAT=1` to collect cycles, instructions, cache misses, context switches,
and CPU migrations alongside Criterion. For release-grade numbers, run on the
same kernel and CPU family as deployment, with the CPU governor and turbo policy
fixed outside the script.

For an end-to-end stream harness rather than a Criterion microbenchmark:

```bash
./bench/run-stream-harness.sh --scenario fragmented --runs 5
./bench/run-stream-harness.sh --scenario coalesced --runs 5
./bench/run-stream-harness.sh --transport cached --scenario fragmented --runs 5
./bench/run-stream-harness.sh --transport tcp --scenario fragmented --runs 5
./bench/run-stream-harness.sh --transport cached --implementation monoio_handoff --scenario fragmented --runs 5
./bench/run-stream-matrix.sh
./bench/run-stream-harness.sh --scenario fragmented --completion fire_and_forget --runs 5
```

The harness drives complete client/proxy/sink streams through fragmented input,
content-routed prefixes, tunnel handoff, and `WriteHandoff` output. It writes
machine-readable run artifacts under `bench/results/`, including throughput,
latency percentiles, context switches, CPU cost, and peak RSS. Use
`--transport cached`, now the default, to feed proxies from prebuilt payloads
and write into a counting sink, which removes parallel client/sink CPU from the
measurement. Use `--transport tcp` for a localhost TCP service/sink harness. Use
`--implementation monoio_handoff` with `--transport cached` to compare the
thread-local Monoio shard path against the Tokio cached path. On Linux,
`monoio_handoff` also supports split TCP service runs with one Monoio
runtime/listener per worker thread:

```bash
./bench/run-stream-harness.sh \
  --transport tcp \
  --implementation monoio_handoff \
  --scenario fragmented \
  --completion fire_and_forget \
  --worker-threads 16 \
  --connections 128 \
  --input-model tcp \
  --service-cores 0-15 \
  --driver-cores 16-31
```

See [`bench/README.md`](bench/README.md).

### Cached Implementation Comparison

These are cached end-to-end harness results from a release validation run on a
16 physical core Linux host, not Criterion microbenchmarks. The cached
transport feeds each proxy from prebuilt payload bytes and writes into a
counting sink, so client, sink, live TCP, and kernel scheduling costs are not
part of the headline number. Treat these rows as runtime and handoff-path
comparisons for an explicit cached workload shape, not as socket throughput.

The fragmented TCP rows use the harness TCP/MSS input model: payload is
delivered to the reader in 1460-byte source chunks, then protocol route frames
inside that stream are 64 bytes each. This models packet-sized arrivals without
measuring the operating system's socket path.

Run shape:

- transport: cached payload reader plus counting sink
- worker threads: 16, one per physical core
- CPU affinity: benchmark process pinned to 16 cores with `taskset`
- concurrent connections: 128
- route frames per connection: 64
- route frame payload: 63 bytes
- tunnel payload per connection: 1 MiB
- read reserve: 16 KiB
- tunnel handoff flush threshold: immediate or the 16 KiB default, as shown
- fragmented TCP/MSS rows: 512 KiB handoff write pending budget, so the
  threshold comparison is not primarily a backpressure measurement
- coalesced table: 128 KiB default handoff write pending budget
  (`DEFAULT_WRITE_PENDING_CHUNKS * read_reserve`)
- completion mode: fire-and-forget
- runs: 2 per point
- target duration: 5 seconds per run

The implementations are:

- `handoff`: `HandoffBuffer` plus `WriteHandoff`; this is the crate path.
- `monoio_handoff`: Monoio `AsyncReadRent`/`AsyncWriteRent` through
  `HandoffBuffer`, with direct thread-local writes from the shard task. This is
  the Monoio shape that avoids cross-thread and cross-task write handoff
  coordination; the Linux TCP service path runs one thread-local Monoio
  runtime/listener per worker.
- `bytesmut_handoff`: direct `BytesMut::read_buf` plus
  `split_to(...).freeze()`, with the same `WriteHandoff` output path as
  `handoff`; this is the closest harness-level `read_mut`/`BytesMut`
  behavior-preserving `BytesMut` baseline.
- `manual_vec`: a persistent `Vec<u8>` parser with direct writes; this is a
  direct-parser control that omits owned cross-task handoff and bounded queued
  writes.
- `raw_copy`: unparsed async copy; this is a lower bound and does less protocol
  work than `handoff`.

Immediate fragmented input with the TCP/MSS source model, where `handoff`
submits every tunnel read as soon as it is observed:

| implementation | throughput | avg CPU used | cost | p99 latency |
|---|---:|---:|---:|---:|
| `handoff` | 27002.68 MiB/s | 13.11 cores | 0.46 ns/B | 3.616 ms |
| `bytesmut_handoff` | 26983.04 MiB/s | 13.16 cores | 0.46 ns/B | 3.631 ms |
| `manual_vec` | 41722.74 MiB/s | 13.99 cores | 0.32 ns/B | 0.399 ms |
| `raw_copy` | 42399.21 MiB/s | 14.23 cores | 0.32 ns/B | 0.395 ms |
| `monoio_handoff` | 44693.40 MiB/s | 16.06 cores | 0.34 ns/B | 0.385 ms |

16 KiB default fragmented input with the TCP/MSS source model, where the default
threshold batches tunnel bytes until 16 KiB or end-of-stream:

| implementation | throughput | avg CPU used | cost | p99 latency |
|---|---:|---:|---:|---:|
| `handoff` | 33523.05 MiB/s | 13.84 cores | 0.39 ns/B | 3.239 ms |
| `bytesmut_handoff` | 35117.47 MiB/s | 13.70 cores | 0.37 ns/B | 3.039 ms |
| `manual_vec` | 41800.76 MiB/s | 14.05 cores | 0.32 ns/B | 0.399 ms |
| `raw_copy` | 42404.23 MiB/s | 14.18 cores | 0.32 ns/B | 0.395 ms |
| `monoio_handoff` | 44695.10 MiB/s | 16.06 cores | 0.34 ns/B | 0.374 ms |

Coalesced input, where the cached reader yields 16 KiB chunks and the default
16 KiB threshold writes each tunnel chunk directly:

| implementation | throughput | avg CPU used | cost | p99 latency |
|---|---:|---:|---:|---:|
| `handoff` | 35748.92 MiB/s | 14.03 cores | 0.38 ns/B | 3.250 ms |
| `bytesmut_handoff` | 37484.27 MiB/s | 13.91 cores | 0.35 ns/B | 3.095 ms |
| `manual_vec` | 42487.96 MiB/s | 13.99 cores | 0.32 ns/B | 0.407 ms |
| `raw_copy` | 42463.62 MiB/s | 14.07 cores | 0.32 ns/B | 0.393 ms |
| `monoio_handoff` | 45232.18 MiB/s | 16.06 cores | 0.34 ns/B | 0.396 ms |

Interpretation:

- Immediate fragmented input is the latency-floor policy: tunnel bytes become
  visible downstream as soon as each read completes. It minimizes batching delay
  but submits many more handoff writes than the default threshold.
- The 16 KiB default threshold batches roughly MSS-sized arrivals before
  submitting the tunnel write. In this TCP/MSS model, that recovers most of the
  coalesced-path throughput for `handoff` and `bytesmut_handoff` while still
  bounding downstream visibility latency by the configured threshold.
- Coalesced cached input shows the steady-state cost when reads already arrive
  at the default threshold size. `handoff` remains close to the direct
  `BytesMut`/`read_buf` handoff baseline while preserving the crate behavior:
  byte-zero inspection, owned prefix handoff, bounded queued writes, and
  mode-switch tail preservation.
- The direct-parser controls, `manual_vec` and `raw_copy`, are useful upper and
  lower bounds, but they are not semantic replacements: they omit the owned
  cross-task handoff and bounded write-queue behavior this crate provides.
- `monoio_handoff` is the fully thread-local Monoio path. Because it writes
  directly from the shard task, the old local writer-queue overhead is gone;
  immediate, 16 KiB fragmented, and coalesced cached input all land in the same
  throughput class in this run.

### Linux Split TCP Service Comparison

These rows use the live localhost TCP split harness rather than the cached
reader. The service process accepts client TCP streams, proxies them to a TCP
sink, and the driver process supplies both clients and sink reads. The split
timing excludes the configured idle-drain timeout, so CPU utilization reflects
the active load window. CPU is shown separately for the driver/sink process and
the service process because they are pinned independently.

Run shape:

- transport: live localhost TCP service plus TCP sink
- input model: TCP/MSS source chunks, `--tcp-mss-bytes 1460`
- worker threads: 16 service workers and 16 driver/sink workers
- concurrent connections: 128
- route frames per connection: 64
- route frame payload: 63 bytes
- tunnel payload per connection: 1 MiB
- read reserve: 16 KiB
- tunnel handoff flush threshold: 16 KiB default
- handoff write pending budget: 512 KiB
- completion mode: fire-and-forget
- runs: 2 per point
- target duration: 5 seconds per run

Fragmented TCP/MSS input, 128 concurrent connections:

| implementation | driver throughput | driver CPU | service CPU | total CPU | driver cost | service cost | p99 latency |
|---|---:|---:|---:|---:|---:|---:|---:|
| `handoff` | 2559 MiB/s | 14.93 cores | 9.85 cores | 24.78 cores | 5.56 ns/B | 3.63 ns/B | 55.0 ms |
| `monoio_handoff` | 2557 MiB/s | 14.97 cores | 7.29 cores | 22.26 cores | 5.58 ns/B | 3.77 ns/B | 48.8 ms |

The Monoio service path used all 16 thread-local listener shards. In one run,
the accepted stream distribution was:

```text
762,798,826,837,782,848,792,777,812,845,782,774,775,748,812,830
```

So the split TCP result is not suffering from a single hot listener shard. A
direct-shard run, where each worker binds a dedicated port and clients connect
to `connection_id % worker_threads`, produced 2465 MiB/s with 51.1 ms p99
latency. That confirms listener distribution is not the limiting factor. The
driver/sink process is effectively saturated at about 15 cores; the service
process uses fewer cores because the single-host TCP driver/kernel path does
not feed it enough work to saturate all service workers.

A 512-connection Monoio saturation probe raised throughput and service CPU, but
with much higher queueing latency:

| implementation | connections | driver throughput | driver CPU | service CPU | p99 latency |
|---|---:|---:|---:|---:|---:|
| `monoio_handoff` | 512 | 3202 MiB/s | 15.39 cores | 9.98 cores | 1878 ms |

That probe is useful for understanding load generation, but the latency profile
is not a good default operating point.

The two runtime shapes now have different jobs: Tokio `handoff` preserves
cross-task owned write handoff, bounded queued writes, and completion semantics;
Monoio `monoio_handoff` is the thread-local shard path for users who can keep
parsing and writing on the same runtime thread.

### What The Read Benchmarks Measure

The read benchmarks are split by workload so the output does not imply one
single headline throughput number.

| benchmark family | what it measures | how to read it |
|---|---|---|
| `read_raw_discard_lower_bound` | Raw `read(&mut [u8])` into temporary scratch, then count and discard bytes. No parsing, persistent state, tail preservation, or owned handoff. | Lower-bound control for bytes that do not outlive the read call; not a peer comparison. |
| `read_owned_lines/manual_vec_copy` | Direct `read(&mut [u8])` loop that appends to persistent state, preserves partial lines, finds complete frames, and copies each frame into a new `Vec<u8>`. | Owned-frame control without `BytesMut`/`Bytes` splitting. |
| `read_owned_lines/bytesmut_split` | Direct `BytesMut` implementation: read into persistent mutable state and split complete frames into owned `Bytes`. | The closest baseline for wrapper overhead. |
| `read_owned_lines/handoff_buffer` | `HandoffBuffer`: same owned-frame workload, with max-length enforcement and the crate API around the buffer lifecycle. | Content-routed streams where buffering rules should live behind a small API. |
| `read_telemetry_cost` | `HandoffBuffer` line parsing with telemetry compiled out, compiled in but unattached, and attached to a real `HandoffReadTelemetry` handle. | Estimating read-path telemetry recording overhead before enabling it in a hot deployment. |
| `read_handoff_fragmentation_sweep` | The same `HandoffBuffer` line workload at different read reserve sizes. | Understanding sensitivity to tiny, fragmented reads versus coalesced reads. |
| `split_freeze_prefixes` | Cost of repeatedly splitting owned `Bytes` prefixes out of buffered state. | Many complete frames already buffered. |
| `split_prefix_mut` | Cost of splitting `BytesMut` prefixes without freezing them into `Bytes`. | Hot paths that keep mutable owned frames. |
| `take_tail_mode_switch` | Cost of preserving already-read bytes when switching parser modes, such as parsed routing to raw tunnel. | Protocols that inspect first, then tunnel or hand off the remaining stream. |

The raw discard lower bound should be much faster than the framed workloads.
That does not mean `HandoffBuffer` is slow at the job it is meant to do; it
means the raw benchmark does less work. For wrapper overhead, compare
`read_owned_lines/bytesmut_split` with `read_owned_lines/handoff_buffer`.

### What The Write Benchmarks Measure

| benchmark family | what it measures |
|---|---|
| `write_large_chunks/direct_write_all` | Direct `AsyncWriteExt::write_all` of large chunks into an in-memory duplex stream. |
| `write_large_chunks/handoff_ticket_single_task` | Submit the same chunks as owned `Bytes` through `WriteHandoff`, then await completion tickets. |
| `write_large_chunks/handoff_fire_and_forget_single_task` | Submit owned `Bytes` without allocating per-write completion tickets. |
| `write_many_tasks/ticket` | Many Tokio tasks submit owned `Bytes` to one handoff and await completion tickets. This is task fan-in, not a cross-thread producer benchmark. |
| `write_many_tasks/fire_and_forget` | The same task fan-in workload without per-write completion tickets. |
| `write_byte_budget_backpressure` | Fast rejection when a write exceeds the configured pending-byte budget. |

The write benchmarks measure owned `Bytes` submission into one async writer,
batched drain behavior, optional completion notification, and backpressure. They
are not raw socket throughput benchmarks.

Treat all benchmark numbers as directional, not universal. They use in-memory
readers/writers; real sockets add scheduler, kernel, and network effects. The
benchmark exists to make the tradeoff explicit: raw slice reads are best for
immediate consumption, while `bytes-handoff` targets safe mutable buffering,
prefix ownership, tail preservation, and bounded async write handoff.
