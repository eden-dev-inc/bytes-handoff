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

Use plain `read(&mut [u8])` when bytes are consumed immediately in the same
function and then discarded. Use `bytes-handoff` when the read buffer itself is
part of the protocol state: partial frames must survive, complete prefixes need
owned handoff, or a parser may switch into a raw tunnel without losing already
read bytes.

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
use bytes_handoff::{WriteHandoff, WriteHandoffConfig};

/// Submits owned bytes to an async writer without blocking the producer.
fn submit_owned_write<W>(writer: W) -> Result<(), Box<dyn std::error::Error>>
where
    W: tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let handoff = WriteHandoff::spawn(writer, WriteHandoffConfig::new(1024, 8 * 1024 * 1024));

    handoff.try_write_fire_and_forget(Bytes::from_static(b"owned bytes"))?;

    // Use `try_write` or `write` instead when the producer needs a completion
    // ticket for this chunk.
    Ok(())
}
```

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
./bench/run-stream-harness.sh --transport tcp --scenario fragmented --runs 5
./bench/run-stream-matrix.sh
./bench/run-stream-harness.sh --scenario fragmented --completion fire_and_forget --runs 5
```

The harness drives complete client/proxy/sink streams through fragmented input,
content-routed prefixes, tunnel handoff, and `WriteHandoff` output. It writes
machine-readable run artifacts under `bench/results/`, including throughput,
latency percentiles, context switches, CPU cost, and peak RSS. Use
`--transport tcp` for a localhost TCP service/sink harness; see
[`bench/README.md`](bench/README.md).

### Latest TCP Harness Results

These are end-to-end localhost TCP harness results from an Ubuntu 24.04 server
(`adam`), not Criterion microbenchmarks. The driver/client and sink processes
were pinned to a different CPU set than the proxy service so the service-side
CPU cost can be read separately from load generation.

Run shape:

- transport: localhost TCP
- worker threads: 16
- concurrent connections: 128
- route frames per connection: 64
- route frame payload: 63 bytes
- tunnel payload per connection: 1 MiB
- read reserve: 16 KiB
- handoff write pending budget: 32 KiB default (`2 * read_reserve`)
- completion mode: fire-and-forget
- runs: 2 per point, 10 second target duration

The implementations are:

- `handoff`: `HandoffBuffer` plus `WriteHandoff`; this is the crate path.
- `manual_vec`: a hand-written persistent `Vec<u8>` parser with direct writes;
  this is the practical `read(&mut [u8])` comparison.
- `raw_copy`: unparsed async copy; this is a lower bound and does less protocol
  work than `handoff`.

Coalesced input, where client writes arrive in 16 KiB chunks:

| implementation | service throughput | service CPU | service cost | driver p99 latency |
|---|---:|---:|---:|---:|
| `handoff` | 2860 MiB/s | 6.27 cores | 2.09 ns/B | 59.8 ms |
| `manual_vec` | 3098 MiB/s | 5.09 cores | 1.57 ns/B | 48.9 ms |
| `raw_copy` | 3072 MiB/s | 4.57 cores | 1.42 ns/B | 56.2 ms |

Fragmented input, where client writes arrive in 64 byte chunks:

| implementation | service throughput | service CPU | service cost | driver p99 latency |
|---|---:|---:|---:|---:|
| `handoff` | 103.1 MiB/s | 7.58 cores | 70.14 ns/B | 1195 ms |
| `manual_vec` | 103.7 MiB/s | 6.69 cores | 61.64 ns/B | 1138 ms |
| `raw_copy` | 104.5 MiB/s | 6.64 cores | 60.72 ns/B | 1138 ms |

Interpretation:

- In the coalesced TCP workload, `handoff` is about 8% behind the practical
  manual `Vec<u8>` baseline on throughput and uses about 23% more proxy-service
  CPU. That is the current cost of the safer buffer lifecycle, owned prefix
  handoff, bounded write queue, and mode-switch tail preservation.
- In the fragmented 64 byte workload, throughput is dominated by tiny socket
  operations. All implementations cluster around 104 MiB/s; the difference is
  mostly service CPU cost.
- `raw_copy` is useful as a lower bound, but it is not a semantic replacement:
  it does not parse route frames, preserve parser state, or hand off owned
  prefixes.

### What The Read Benchmarks Measure

The read benchmarks are split by workload so the output does not imply one
single headline throughput number.

| benchmark family | what it measures | when it should win |
|---|---|---|
| `read_raw_discard_lower_bound` | Raw `read(&mut [u8])` into temporary scratch, then count and discard bytes. No parsing, persistent state, tail preservation, or owned handoff. | Immediate local consumption where bytes do not outlive the read call. Treat this as a lower bound, not a peer comparison. |
| `read_owned_lines/manual_vec_copy` | Manual `read(&mut [u8])` loop that appends to persistent state, preserves partial lines, finds complete frames, and copies each frame into a new `Vec<u8>`. | Code that needs owned frames but does not use `BytesMut`/`Bytes` splitting. |
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
