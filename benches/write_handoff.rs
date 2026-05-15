use bytes::Bytes;
use bytes_handoff::{WriteHandoff, WriteHandoffConfig};
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use std::io::{self, IoSlice};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::runtime::{Builder, Runtime};

#[cfg(feature = "monoio")]
type MonoioRuntime = monoio::Runtime<monoio::LegacyDriver>;

fn runtime() -> Runtime {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build benchmark runtime")
}

#[cfg(feature = "monoio")]
fn monoio_runtime() -> MonoioRuntime {
    monoio::RuntimeBuilder::<monoio::LegacyDriver>::new()
        .build()
        .expect("build monoio benchmark runtime")
}

#[derive(Clone, Default)]
struct DiscardCounter {
    written: Arc<AtomicUsize>,
}

impl DiscardCounter {
    fn bytes_written(&self) -> usize {
        self.written.load(Ordering::Acquire)
    }
}

struct TokioDiscardWriter {
    counter: DiscardCounter,
}

impl TokioDiscardWriter {
    fn new(counter: DiscardCounter) -> Self {
        Self { counter }
    }
}

impl tokio::io::AsyncWrite for TokioDiscardWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.counter.written.fetch_add(buf.len(), Ordering::AcqRel);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let written = bufs.iter().map(|buf| buf.len()).sum();
        self.counter.written.fetch_add(written, Ordering::AcqRel);
        Poll::Ready(Ok(written))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }
}

#[cfg(feature = "monoio")]
struct MonoioDiscardWriter {
    counter: DiscardCounter,
}

#[cfg(feature = "monoio")]
impl MonoioDiscardWriter {
    fn new(counter: DiscardCounter) -> Self {
        Self { counter }
    }
}

#[cfg(feature = "monoio")]
impl monoio::io::AsyncWriteRent for MonoioDiscardWriter {
    async fn write<T: monoio::buf::IoBuf>(&mut self, buf: T) -> monoio::BufResult<usize, T> {
        let len = monoio::buf::IoBuf::bytes_init(&buf);
        self.counter.written.fetch_add(len, Ordering::AcqRel);
        (Ok(len), buf)
    }

    async fn writev<T: monoio::buf::IoVecBuf>(
        &mut self,
        buf_vec: T,
    ) -> monoio::BufResult<usize, T> {
        #[cfg(unix)]
        {
            let iovecs = unsafe {
                std::slice::from_raw_parts(
                    monoio::buf::IoVecBuf::read_iovec_ptr(&buf_vec),
                    monoio::buf::IoVecBuf::read_iovec_len(&buf_vec),
                )
            };
            let written = iovecs.iter().map(|iovec| iovec.iov_len).sum();
            self.counter.written.fetch_add(written, Ordering::AcqRel);
            (Ok(written), buf_vec)
        }
        #[cfg(not(unix))]
        {
            (
                Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "vectored writes are not used by the monoio benchmark path",
                )),
                buf_vec,
            )
        }
    }

    async fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    async fn shutdown(&mut self) -> io::Result<()> {
        Ok(())
    }
}

async fn drain_expected<R>(mut reader: R, expected: usize) -> usize
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut total = 0;
    let mut buf = vec![0_u8; 64 * 1024];
    while total < expected {
        let read = reader.read(&mut buf).await.expect("read benchmark sink");
        if read == 0 {
            break;
        }
        total += read;
    }
    total
}

fn write_direct_chunks(rt: &Runtime, chunk_size: usize, chunks: usize) -> usize {
    rt.block_on(async {
        let total_bytes = chunk_size * chunks;
        let (mut writer, reader) = tokio::io::duplex(4 * 1024 * 1024);
        let drain = tokio::spawn(drain_expected(reader, total_bytes));
        let chunk = vec![7_u8; chunk_size];

        for _ in 0..chunks {
            writer
                .write_all(&chunk)
                .await
                .expect("write benchmark chunk");
        }

        let drained = drain.await.expect("drain task joins");
        black_box(drained)
    })
}

fn write_direct_discard_tokio(rt: &Runtime, chunk_size: usize, chunks: usize) -> usize {
    rt.block_on(async {
        let counter = DiscardCounter::default();
        let mut writer = TokioDiscardWriter::new(counter.clone());
        let chunk = vec![7_u8; chunk_size];

        for _ in 0..chunks {
            writer
                .write_all(&chunk)
                .await
                .expect("write benchmark chunk");
        }

        black_box(counter.bytes_written())
    })
}

#[cfg(feature = "monoio")]
fn write_direct_discard_monoio(rt: &mut MonoioRuntime, chunk_size: usize, chunks: usize) -> usize {
    use monoio::io::AsyncWriteRentExt as _;

    rt.block_on(async {
        let counter = DiscardCounter::default();
        let mut writer = MonoioDiscardWriter::new(counter.clone());
        let chunk = Bytes::from(vec![7_u8; chunk_size]);

        for _ in 0..chunks {
            let (result, returned) = writer.write_all(chunk.clone()).await;
            result.expect("write benchmark chunk");
            black_box(returned);
        }

        black_box(counter.bytes_written())
    })
}

#[derive(Clone, Copy)]
enum CompletionMode {
    Ticket,
    FireAndForget,
}

fn write_handoff_chunks(
    rt: &Runtime,
    chunk_size: usize,
    chunks: usize,
    tasks: usize,
    completion: CompletionMode,
) -> usize {
    rt.block_on(async {
        let total_bytes = chunk_size * chunks;
        let (writer, reader) = tokio::io::duplex(4 * 1024 * 1024);
        let handoff = WriteHandoff::spawn(
            writer,
            WriteHandoffConfig::new(chunks + tasks, total_bytes + chunk_size),
        );
        let drain = tokio::spawn(drain_expected(reader, total_bytes));
        let chunk = Bytes::from(vec![7_u8; chunk_size]);

        let mut handles = Vec::with_capacity(tasks);
        for task_id in 0..tasks {
            let handoff = handoff.clone();
            let chunk = chunk.clone();
            let per_task = chunks / tasks;
            let extra = usize::from(task_id < chunks % tasks);
            handles.push(tokio::spawn(async move {
                match completion {
                    CompletionMode::Ticket => {
                        let mut tickets = Vec::with_capacity(per_task + extra);
                        for _ in 0..(per_task + extra) {
                            tickets.push(handoff.write(chunk.clone()).await.expect("submit write"));
                        }
                        for ticket in tickets {
                            ticket.wait().await.expect("write completes");
                        }
                    }
                    CompletionMode::FireAndForget => {
                        for _ in 0..(per_task + extra) {
                            handoff
                                .try_write_fire_and_forget(chunk.clone())
                                .expect("submit write");
                        }
                    }
                }
            }));
        }

        for handle in handles {
            handle.await.expect("producer task joins");
        }

        let drained = drain.await.expect("drain task joins");
        black_box(drained)
    })
}

fn write_handoff_discard_tokio(
    rt: &Runtime,
    chunk_size: usize,
    chunks: usize,
    tasks: usize,
    completion: CompletionMode,
) -> usize {
    rt.block_on(async {
        let total_bytes = chunk_size * chunks;
        let counter = DiscardCounter::default();
        let writer = TokioDiscardWriter::new(counter.clone());
        let handoff = WriteHandoff::spawn(
            writer,
            WriteHandoffConfig::new(chunks + tasks + 1, total_bytes + chunk_size),
        );
        let chunk = Bytes::from(vec![7_u8; chunk_size]);

        let mut handles = Vec::with_capacity(tasks);
        for task_id in 0..tasks {
            let handoff = handoff.clone();
            let chunk = chunk.clone();
            let per_task = chunks / tasks;
            let extra = usize::from(task_id < chunks % tasks);
            handles.push(tokio::spawn(async move {
                match completion {
                    CompletionMode::Ticket => {
                        let mut tickets = Vec::with_capacity(per_task + extra);
                        for _ in 0..(per_task + extra) {
                            tickets.push(handoff.write(chunk.clone()).await.expect("submit write"));
                        }
                        for ticket in tickets {
                            ticket.wait().await.expect("write completes");
                        }
                    }
                    CompletionMode::FireAndForget => {
                        for _ in 0..(per_task + extra) {
                            handoff
                                .try_write_fire_and_forget(chunk.clone())
                                .expect("submit write");
                        }
                    }
                }
            }));
        }

        for handle in handles {
            handle.await.expect("producer task joins");
        }

        if matches!(completion, CompletionMode::FireAndForget) {
            let barrier = handoff
                .write(Bytes::new())
                .await
                .expect("submit fire-and-forget barrier");
            barrier.wait().await.expect("barrier write completes");
        }
        handoff.close();

        black_box(counter.bytes_written())
    })
}

#[cfg(feature = "monoio")]
fn write_handoff_discard_monoio(
    rt: &mut MonoioRuntime,
    chunk_size: usize,
    chunks: usize,
    tasks: usize,
    completion: CompletionMode,
) -> usize {
    rt.block_on(async {
        let total_bytes = chunk_size * chunks;
        let counter = DiscardCounter::default();
        let writer = MonoioDiscardWriter::new(counter.clone());
        let handoff = WriteHandoff::spawn_monoio(
            writer,
            WriteHandoffConfig::new(chunks + tasks + 1, total_bytes + chunk_size),
        );
        let chunk = Bytes::from(vec![7_u8; chunk_size]);

        let mut handles = Vec::with_capacity(tasks);
        for task_id in 0..tasks {
            let handoff = handoff.clone();
            let chunk = chunk.clone();
            let per_task = chunks / tasks;
            let extra = usize::from(task_id < chunks % tasks);
            handles.push(monoio::spawn(async move {
                match completion {
                    CompletionMode::Ticket => {
                        let mut tickets = Vec::with_capacity(per_task + extra);
                        for _ in 0..(per_task + extra) {
                            tickets.push(handoff.write(chunk.clone()).await.expect("submit write"));
                        }
                        for ticket in tickets {
                            ticket.wait().await.expect("write completes");
                        }
                    }
                    CompletionMode::FireAndForget => {
                        for _ in 0..(per_task + extra) {
                            handoff
                                .try_write_fire_and_forget(chunk.clone())
                                .expect("submit write");
                        }
                    }
                }
            }));
        }

        for handle in handles {
            handle.await;
        }

        if matches!(completion, CompletionMode::FireAndForget) {
            let barrier = handoff
                .write(Bytes::new())
                .await
                .expect("submit fire-and-forget barrier");
            barrier.wait().await.expect("barrier write completes");
        }
        handoff.close();

        black_box(counter.bytes_written())
    })
}

fn write_handoff_benches(c: &mut Criterion) {
    let rt = runtime();

    let mut large = c.benchmark_group("write_large_chunks");
    for chunk_size in [64 * 1024, 1024 * 1024] {
        let chunks = 32;
        large.throughput(Throughput::Bytes((chunk_size * chunks) as u64));
        large.bench_with_input(
            BenchmarkId::new("direct_write_all", chunk_size),
            &chunk_size,
            |b, chunk_size| {
                b.iter(|| write_direct_chunks(&rt, *chunk_size, chunks));
            },
        );
        large.bench_with_input(
            BenchmarkId::new("handoff_ticket_single_task", chunk_size),
            &chunk_size,
            |b, chunk_size| {
                b.iter(|| {
                    write_handoff_chunks(&rt, *chunk_size, chunks, 1, CompletionMode::Ticket)
                });
            },
        );
        large.bench_with_input(
            BenchmarkId::new("handoff_fire_and_forget_single_task", chunk_size),
            &chunk_size,
            |b, chunk_size| {
                b.iter(|| {
                    write_handoff_chunks(&rt, *chunk_size, chunks, 1, CompletionMode::FireAndForget)
                });
            },
        );
    }
    large.finish();

    let mut producers = c.benchmark_group("write_many_tasks");
    let chunk_size = 32 * 1024;
    let chunks = 256;
    producers.throughput(Throughput::Bytes((chunk_size * chunks) as u64));
    for task_count in [1, 4, 16, 64] {
        producers.bench_with_input(
            BenchmarkId::new("ticket", task_count),
            &task_count,
            |b, task_count| {
                b.iter(|| {
                    write_handoff_chunks(
                        &rt,
                        chunk_size,
                        chunks,
                        *task_count,
                        CompletionMode::Ticket,
                    )
                });
            },
        );
        producers.bench_with_input(
            BenchmarkId::new("fire_and_forget", task_count),
            &task_count,
            |b, task_count| {
                b.iter(|| {
                    write_handoff_chunks(
                        &rt,
                        chunk_size,
                        chunks,
                        *task_count,
                        CompletionMode::FireAndForget,
                    )
                });
            },
        );
    }
    producers.finish();
}

fn write_runtime_compare_benches(c: &mut Criterion) {
    let rt = runtime();
    #[cfg(feature = "monoio")]
    let mut monoio_rt = monoio_runtime();

    let mut large = c.benchmark_group("write_runtime_compare_large_chunks");
    for chunk_size in [64 * 1024, 1024 * 1024] {
        let chunks = 32;
        large.throughput(Throughput::Bytes((chunk_size * chunks) as u64));
        large.bench_with_input(
            BenchmarkId::new("tokio_direct_discard", chunk_size),
            &chunk_size,
            |b, chunk_size| {
                b.iter(|| write_direct_discard_tokio(&rt, *chunk_size, chunks));
            },
        );
        large.bench_with_input(
            BenchmarkId::new("tokio_handoff_ticket", chunk_size),
            &chunk_size,
            |b, chunk_size| {
                b.iter(|| {
                    write_handoff_discard_tokio(&rt, *chunk_size, chunks, 1, CompletionMode::Ticket)
                });
            },
        );
        large.bench_with_input(
            BenchmarkId::new("tokio_handoff_fire_and_forget", chunk_size),
            &chunk_size,
            |b, chunk_size| {
                b.iter(|| {
                    write_handoff_discard_tokio(
                        &rt,
                        *chunk_size,
                        chunks,
                        1,
                        CompletionMode::FireAndForget,
                    )
                });
            },
        );
        #[cfg(feature = "monoio")]
        large.bench_with_input(
            BenchmarkId::new("monoio_direct_discard", chunk_size),
            &chunk_size,
            |b, chunk_size| {
                b.iter(|| write_direct_discard_monoio(&mut monoio_rt, *chunk_size, chunks));
            },
        );
        #[cfg(feature = "monoio")]
        large.bench_with_input(
            BenchmarkId::new("monoio_handoff_ticket", chunk_size),
            &chunk_size,
            |b, chunk_size| {
                b.iter(|| {
                    write_handoff_discard_monoio(
                        &mut monoio_rt,
                        *chunk_size,
                        chunks,
                        1,
                        CompletionMode::Ticket,
                    )
                });
            },
        );
        #[cfg(feature = "monoio")]
        large.bench_with_input(
            BenchmarkId::new("monoio_handoff_fire_and_forget", chunk_size),
            &chunk_size,
            |b, chunk_size| {
                b.iter(|| {
                    write_handoff_discard_monoio(
                        &mut monoio_rt,
                        *chunk_size,
                        chunks,
                        1,
                        CompletionMode::FireAndForget,
                    )
                });
            },
        );
    }
    large.finish();

    let mut producers = c.benchmark_group("write_runtime_compare_many_tasks");
    let chunk_size = 32 * 1024;
    let chunks = 256;
    producers.throughput(Throughput::Bytes((chunk_size * chunks) as u64));
    for task_count in [1, 4, 16, 64] {
        producers.bench_with_input(
            BenchmarkId::new("tokio_ticket", task_count),
            &task_count,
            |b, task_count| {
                b.iter(|| {
                    write_handoff_discard_tokio(
                        &rt,
                        chunk_size,
                        chunks,
                        *task_count,
                        CompletionMode::Ticket,
                    )
                });
            },
        );
        producers.bench_with_input(
            BenchmarkId::new("tokio_fire_and_forget", task_count),
            &task_count,
            |b, task_count| {
                b.iter(|| {
                    write_handoff_discard_tokio(
                        &rt,
                        chunk_size,
                        chunks,
                        *task_count,
                        CompletionMode::FireAndForget,
                    )
                });
            },
        );
        #[cfg(feature = "monoio")]
        producers.bench_with_input(
            BenchmarkId::new("monoio_ticket", task_count),
            &task_count,
            |b, task_count| {
                b.iter(|| {
                    write_handoff_discard_monoio(
                        &mut monoio_rt,
                        chunk_size,
                        chunks,
                        *task_count,
                        CompletionMode::Ticket,
                    )
                });
            },
        );
        #[cfg(feature = "monoio")]
        producers.bench_with_input(
            BenchmarkId::new("monoio_fire_and_forget", task_count),
            &task_count,
            |b, task_count| {
                b.iter(|| {
                    write_handoff_discard_monoio(
                        &mut monoio_rt,
                        chunk_size,
                        chunks,
                        *task_count,
                        CompletionMode::FireAndForget,
                    )
                });
            },
        );
    }
    producers.finish();
}

fn backpressure_bench(c: &mut Criterion) {
    let rt = runtime();
    c.bench_function("write_byte_budget_backpressure", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (writer, _reader) = tokio::io::duplex(64);
                let handoff = WriteHandoff::spawn(writer, WriteHandoffConfig::new(1, 63));
                let rejected = handoff
                    .try_write(Bytes::from(vec![1_u8; 64]))
                    .expect_err("chunk exceeds byte budget");
                black_box(rejected.into_bytes().len())
            })
        });
    });
}

criterion_group!(
    benches,
    write_handoff_benches,
    write_runtime_compare_benches,
    backpressure_bench
);
criterion_main!(benches);
