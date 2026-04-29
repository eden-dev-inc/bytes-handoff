use bytes::Bytes;
use bytes_handoff::{WriteHandoff, WriteHandoffConfig};
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::runtime::{Builder, Runtime};

fn runtime() -> Runtime {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build benchmark runtime")
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

criterion_group!(benches, write_handoff_benches, backpressure_bench);
criterion_main!(benches);
