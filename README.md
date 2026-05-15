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

## What This Optimizes

The crate keeps the following behavior in the hot path:

- incomplete frame tails must survive future reads
- complete prefixes need to become owned `Bytes`
- already-read tails must survive parser mode changes, such as routed prefix to
  raw tunnel
- many small prefixes should not retain large read-buffer allocations
- queued writes need item and byte limits
- producers may need optional completion tickets from the writer task

In the cached harness, `handoff` is effectively tied with the direct `BytesMut`
behavior-preserving baseline on coalesced input, and is faster on fragmented
input because small prefixes are copied into compact `Bytes` and tiny tunnel
reads are coalesced before entering the write handoff. The flush threshold is a
caller policy; the crate's write coalescer defaults to 1 KiB and can model
immediate flushing with `WriteCoalescerConfig::immediate()`.

## Examples

The repository includes small runnable examples:

- `line_protocol`: incremental line parsing with a partial tail.
- `length_prefix`: length-prefixed frames where the header arrives before the
  full payload.
- `content_routing`: inspect buffered bytes, route complete safe prefixes, then
  switch to a raw tunnel while preserving already-read tail bytes.
- `write_handoff`: submit owned bytes to an async writer and await completion.

Run one with:

```bash
cargo run --example content_routing
```

## Read Handoff

```rust
use bytes::Bytes;
use bytes_handoff::HandoffBuffer;

/// Reads available bytes, splits one complete line, and hands it off.
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

## Write Handoff

```rust
use bytes::Bytes;
use bytes_handoff::{WriteCoalescer, WriteHandoff, WriteHandoffConfig};

/// Submits owned bytes to an async writer without blocking the producer.
async fn submit_owned_write<W>(writer: W) -> Result<(), Box<dyn std::error::Error>>
where
    W: tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let handoff = WriteHandoff::spawn(writer, WriteHandoffConfig::new(1024, 8 * 1024 * 1024));
    let mut coalescer = WriteCoalescer::new(handoff.clone());

    coalescer
        .write_fire_and_forget(Bytes::from_static(b"owned bytes"))
        .await?;
    coalescer.flush().await?;

    // Use `try_write` or `write` instead when the producer needs a completion
    // ticket for this chunk.
    Ok(())
}
```

`WriteCoalescer` is an optional fire-and-forget helper around `WriteHandoff`.
It collects small adjacent writes up to a byte threshold before submitting one
owned `Bytes` chunk to the writer task. The default threshold is 1 KiB
(`DEFAULT_WRITE_COALESCE_THRESHOLD`). Configure it with
`WriteCoalescer::with_threshold`, or use `WriteCoalescerConfig::immediate()` for
flush-every-write behavior. Always call `flush()` at message boundaries, before
switching modes when downstream latency matters, and before closing.

The tradeoff is direct: a larger threshold reduces queueing, notification, and
writer-task overhead under tiny fragmented input; a smaller threshold makes
bytes visible to the downstream writer sooner. Completion-ticket writes are not
coalesced by this helper, because their completion boundary is part of the
observable behavior.

## Monoio Feature

Enable `monoio` to use Monoio's ownership-based I/O traits directly:

```toml
bytes-handoff = { version = "1", features = ["monoio"] }
```

With the feature enabled, `HandoffBuffer::read_available_monoio` accepts
`monoio::io::AsyncReadRent`, and `WriteHandoff::spawn_monoio` returns a
single-threaded `MonoioWriteHandoff` that runs the background writer task with
`monoio::spawn` over `monoio::io::AsyncWriteRent`. The Tokio API remains
available unchanged.

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
Monoio read/write handoff path against the Tokio cached path; see
[`bench/README.md`](bench/README.md).

### Latest Linux Harness Results

These are cached end-to-end harness results from a 16 physical core Ubuntu
24.04 Linux server, not Criterion microbenchmarks. The cached transport feeds
each proxy from prebuilt payload bytes and writes into a counting sink, so
client, sink, TCP, and kernel scheduling costs are not part of the headline
number. Treat these rows as runtime and handoff-path comparisons, not as TCP
socket throughput.

Run shape:

- transport: cached payload reader plus counting sink
- worker threads: 16, one per physical core
- concurrent connections: 128
- route frames per connection: 64
- route frame payload: 63 bytes
- tunnel payload per connection: 1 MiB
- read reserve: 16 KiB
- tunnel handoff flush threshold: 16 KiB (`read_reserve`)
- handoff write pending budget: 32 KiB default (`2 * read_reserve`)
- completion mode: fire-and-forget
- runs: 2 per point
- target duration: 5 seconds per run

The implementations are:

- `handoff`: `HandoffBuffer` plus `WriteHandoff`; this is the crate path.
- `monoio_handoff`: Monoio `AsyncReadRent`/`AsyncWriteRent` through
  `HandoffBuffer` plus `MonoioWriteHandoff`; currently cached/duplex-only in the
  harness.
- `bytesmut_handoff`: direct `BytesMut::read_buf` plus
  `split_to(...).freeze()`, with the same `WriteHandoff` output path as
  `handoff`; this is the closest harness-level `read_mut`/`BytesMut`
  behavior-preserving `BytesMut` baseline.
- `manual_vec`: a persistent `Vec<u8>` parser with direct writes; this is a
  direct-parser control that omits owned cross-task handoff and bounded queued
  writes.
- `raw_copy`: unparsed async copy; this is a lower bound and does less protocol
  work than `handoff`.

Coalesced input, where the cached reader yields 16 KiB chunks:

| implementation | throughput | avg CPU used | cost | p99 latency |
|---|---:|---:|---:|---:|
| `handoff` | 38220 MiB/s | 15.38 cores | 0.38 ns/B | 3.15 ms |
| `bytesmut_handoff` | 38328 MiB/s | 15.38 cores | 0.38 ns/B | 3.15 ms |
| `manual_vec` | 43259 MiB/s | 14.84 cores | 0.33 ns/B | 0.574 ms |
| `raw_copy` | 43305 MiB/s | 14.83 cores | 0.33 ns/B | 0.509 ms |
| `monoio_handoff` | 41447 MiB/s | 16.06 cores | 0.37 ns/B | 3.26 ms |

Fragmented input, where the cached reader yields 64 byte chunks:

| implementation | throughput | avg CPU used | cost | p99 latency |
|---|---:|---:|---:|---:|
| `handoff` | 33364 MiB/s | 15.36 cores | 0.44 ns/B | 3.63 ms |
| `bytesmut_handoff` | 29748 MiB/s | 15.48 cores | 0.50 ns/B | 4.11 ms |
| `manual_vec` | 42654 MiB/s | 14.82 cores | 0.33 ns/B | 0.482 ms |
| `raw_copy` | 42666 MiB/s | 14.80 cores | 0.33 ns/B | 0.502 ms |
| `monoio_handoff` | 26218 MiB/s | 16.06 cores | 0.58 ns/B | 4.99 ms |

Interpretation:

- Coalesced cached input shows the steady-state cost of the introspectable
  handoff path without socket noise. `handoff` matches the direct
  `BytesMut`/`read_buf` handoff baseline and is about 12% behind the direct
  parser baseline while preserving the safer buffer lifecycle, owned prefix
  handoff, bounded write queue, and mode-switch tail preservation.
- Fragmented cached input is now protected by a 16 KiB tunnel flush threshold.
  That removes the old per-64-byte write-handoff cliff while still preserving
  byte-zero routing and exact output bytes.
- The direct `BytesMut` handoff baseline is the closest behavior-preserving
  comparison for the read-mutable path. `handoff` is effectively tied with it on
  coalesced input and is about 12% faster on fragmented input because tiny
  prefixes are copied into compact `Bytes` instead of freezing `split_to` views
  that keep larger buffer allocations alive until the write handoff drains them.
- `monoio_handoff` uses the available workers fully in both cached workloads.
  After tunnel coalescing, the Tokio handoff path is faster in fragmented cached
  input, while Monoio remains close on coalesced input.
- `raw_copy` is useful as a lower bound, but it is not a semantic replacement:
  it does not parse route frames, preserve parser state, or hand off owned
  prefixes.

The current optimization target is fragmented Tokio handoff: preserve byte-zero
inspection, tail preservation, owned prefix handoff, bounded queued writes, and
completion semantics while further reducing route-prefix queueing, notification,
and copy overhead.

### What The Read Benchmarks Measure

The read benchmarks are split by workload so the output does not imply one
single headline throughput number.

| benchmark family | what it measures | how to read it |
|---|---|---|
| `read_raw_discard_lower_bound` | Raw `read(&mut [u8])` into temporary scratch, then count and discard bytes. No parsing, persistent state, tail preservation, or owned handoff. | Lower-bound control for bytes that do not outlive the read call; not a peer comparison. |
| `read_owned_lines/manual_vec_copy` | Direct `read(&mut [u8])` loop that appends to persistent state, preserves partial lines, finds complete frames, and copies each frame into a new `Vec<u8>`. | Owned-frame control without `BytesMut`/`Bytes` splitting. |
| `read_owned_lines/bytesmut_split` | Direct `BytesMut` implementation: read into persistent mutable state and split complete frames into owned `Bytes`. | The closest baseline for wrapper overhead. |
| `read_owned_lines/handoff_buffer` | `HandoffBuffer`: same owned-frame workload, with max-length enforcement and the crate API around the buffer lifecycle. | Content-routed streams where buffering rules should live behind a small API. |
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
