use bytes::BytesMut;
use bytes_handoff::{HandoffBuffer, HandoffBufferConfig};
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use memchr::memchr;
use tokio::io::AsyncReadExt;
use tokio::runtime::{Builder, Runtime};

fn runtime() -> Runtime {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build benchmark runtime")
}

fn line_payload(line_count: usize, line_len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(line_count * (line_len + 1));
    for i in 0..line_count {
        let label = format!("line-{i:08}");
        out.extend_from_slice(label.as_bytes());
        out.resize(out.len() + line_len.saturating_sub(label.len()), b'x');
        out.push(b'\n');
    }
    out
}

fn binary_payload(frame_count: usize, frame_len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(frame_count * frame_len);
    for i in 0..frame_count {
        out.extend((0..frame_len).map(|n| ((i + n) % 251) as u8));
    }
    out
}

fn drain_lines(buffer: &mut HandoffBuffer) -> usize {
    let mut frames = 0;
    while let Some(newline) = memchr(b'\n', buffer.peek()) {
        let line = buffer
            .split_prefix(newline + 1)
            .expect("newline prefix is in bounds");
        black_box(line);
        frames += 1;
    }
    frames
}

fn read_handoff_lines(rt: &Runtime, input: &[u8], read_reserve: usize) -> usize {
    rt.block_on(async {
        let mut reader = input;
        let mut buffer = HandoffBuffer::with_config(
            HandoffBufferConfig::new(input.len() + read_reserve).with_read_reserve(read_reserve),
        );
        let mut frames = 0;

        while !reader.is_empty() {
            let read = buffer
                .read_available(&mut reader)
                .await
                .expect("read from slice");
            if read == 0 {
                break;
            }
            frames += drain_lines(&mut buffer);
        }

        frames += drain_lines(&mut buffer);
        black_box(frames)
    })
}

fn read_raw_discard(rt: &Runtime, input: &[u8], read_size: usize) -> usize {
    rt.block_on(async {
        let mut reader = input;
        let mut scratch = vec![0_u8; read_size];
        let mut total = 0;

        loop {
            let read = reader.read(&mut scratch).await.expect("read from slice");
            if read == 0 {
                break;
            }
            total += read;
        }

        black_box(total)
    })
}

fn drain_manual_lines(pending: &mut Vec<u8>, start: &mut usize) -> usize {
    let mut frames = 0;
    while let Some(offset) = memchr(b'\n', &pending[*start..]) {
        let end = *start + offset + 1;
        let frame = pending[*start..end].to_vec();
        black_box(frame);
        *start = end;
        frames += 1;
    }

    if *start > 0 && (*start > pending.len() / 2 || *start == pending.len()) {
        pending.drain(..*start);
        *start = 0;
    }

    frames
}

fn read_mut_slice_copy_lines(rt: &Runtime, input: &[u8], read_size: usize) -> usize {
    rt.block_on(async {
        let mut reader = input;
        let mut scratch = vec![0_u8; read_size];
        let mut pending = Vec::with_capacity(read_size * 2);
        let mut start = 0;
        let mut frames = 0;

        loop {
            let read = reader.read(&mut scratch).await.expect("read from slice");
            if read == 0 {
                break;
            }
            pending.extend_from_slice(&scratch[..read]);
            frames += drain_manual_lines(&mut pending, &mut start);
        }

        frames += drain_manual_lines(&mut pending, &mut start);
        black_box(frames)
    })
}

fn drain_bytesmut_lines(pending: &mut BytesMut) -> usize {
    let mut frames = 0;
    while let Some(newline) = memchr(b'\n', pending) {
        let frame = pending.split_to(newline + 1).freeze();
        black_box(frame);
        frames += 1;
    }
    frames
}

fn read_bytesmut_split_lines(rt: &Runtime, input: &[u8], read_size: usize) -> usize {
    rt.block_on(async {
        let mut reader = input;
        let mut pending = BytesMut::new();
        let mut frames = 0;

        while !reader.is_empty() {
            pending.reserve(read_size);
            let mut capped_reader = (&mut reader).take(read_size as u64);
            let read = capped_reader
                .read_buf(&mut pending)
                .await
                .expect("read from slice");
            if read == 0 {
                break;
            }
            frames += drain_bytesmut_lines(&mut pending);
        }

        frames += drain_bytesmut_lines(&mut pending);
        black_box(frames)
    })
}

fn read_raw_lower_bound(c: &mut Criterion) {
    let rt = runtime();
    let lines = line_payload(16 * 1024, 32);

    let mut discard = c.benchmark_group("read_raw_discard_lower_bound");
    discard.throughput(Throughput::Bytes(lines.len() as u64));
    for read_size in [64, 16 * 1024] {
        discard.bench_with_input(
            BenchmarkId::from_parameter(read_size),
            &read_size,
            |b, read_size| {
                b.iter(|| read_raw_discard(&rt, black_box(&lines), *read_size));
            },
        );
    }
    discard.finish();
}

fn read_owned_line_benches(c: &mut Criterion) {
    let rt = runtime();
    let lines = line_payload(16 * 1024, 32);

    let mut group = c.benchmark_group("read_owned_lines");
    group.throughput(Throughput::Bytes(lines.len() as u64));
    for read_size in [64, 16 * 1024] {
        group.bench_with_input(
            BenchmarkId::new("manual_vec_copy", read_size),
            &read_size,
            |b, read_size| {
                b.iter(|| read_mut_slice_copy_lines(&rt, black_box(&lines), *read_size));
            },
        );
        group.bench_with_input(
            BenchmarkId::new("bytesmut_split", read_size),
            &read_size,
            |b, read_size| {
                b.iter(|| read_bytesmut_split_lines(&rt, black_box(&lines), *read_size));
            },
        );
        group.bench_with_input(
            BenchmarkId::new("handoff_buffer", read_size),
            &read_size,
            |b, read_size| {
                b.iter(|| read_handoff_lines(&rt, black_box(&lines), *read_size));
            },
        );
    }
    group.finish();
}

fn read_handoff_fragmentation_sweep(c: &mut Criterion) {
    let rt = runtime();
    let lines = line_payload(16 * 1024, 32);

    let mut group = c.benchmark_group("read_handoff_fragmentation_sweep");
    group.throughput(Throughput::Bytes(lines.len() as u64));
    for reserve in [1, 8, 64, 16 * 1024] {
        group.bench_with_input(
            BenchmarkId::from_parameter(reserve),
            &reserve,
            |b, reserve| {
                b.iter(|| read_handoff_lines(&rt, black_box(&lines), *reserve));
            },
        );
    }
    group.finish();
}

fn split_and_tail_benches(c: &mut Criterion) {
    let payload = binary_payload(128 * 1024, 16);

    let mut split = c.benchmark_group("split_freeze_prefixes");
    split.throughput(Throughput::Bytes(payload.len() as u64));
    for prefix_len in [16, 64, 512] {
        split.bench_with_input(
            BenchmarkId::from_parameter(prefix_len),
            &prefix_len,
            |b, prefix_len| {
                b.iter(|| {
                    let tail = BytesMut::from(black_box(payload.as_slice()));
                    let mut buffer =
                        HandoffBuffer::from_tail(tail, HandoffBufferConfig::new(payload.len()))
                            .expect("payload fits");
                    let mut prefixes = 0;
                    while buffer.len() >= *prefix_len {
                        let prefix = buffer
                            .split_prefix(*prefix_len)
                            .expect("prefix is in bounds");
                        black_box(prefix);
                        prefixes += 1;
                    }
                    black_box(prefixes)
                });
            },
        );
    }
    split.finish();

    let mut split_mut = c.benchmark_group("split_prefix_mut");
    split_mut.throughput(Throughput::Bytes(payload.len() as u64));
    for prefix_len in [16, 64, 512] {
        split_mut.bench_with_input(
            BenchmarkId::from_parameter(prefix_len),
            &prefix_len,
            |b, prefix_len| {
                b.iter(|| {
                    let tail = BytesMut::from(black_box(payload.as_slice()));
                    let mut buffer =
                        HandoffBuffer::from_tail(tail, HandoffBufferConfig::new(payload.len()))
                            .expect("payload fits");
                    let mut prefixes = 0;
                    while buffer.len() >= *prefix_len {
                        let prefix = buffer
                            .split_prefix_mut(*prefix_len)
                            .expect("prefix is in bounds");
                        black_box(prefix);
                        prefixes += 1;
                    }
                    black_box(prefixes)
                });
            },
        );
    }
    split_mut.finish();

    c.bench_function("take_tail_mode_switch", |b| {
        b.iter(|| {
            let tail = BytesMut::from(black_box(payload.as_slice()));
            let mut buffer =
                HandoffBuffer::from_tail(tail, HandoffBufferConfig::new(payload.len()))
                    .expect("payload fits");
            let prefix = buffer.split_prefix(512).expect("prefix is in bounds");
            let inherited_tail = buffer.take_tail();
            black_box((prefix.len(), inherited_tail.len()))
        });
    });
}

criterion_group!(
    benches,
    read_raw_lower_bound,
    read_owned_line_benches,
    read_handoff_fragmentation_sweep,
    split_and_tail_benches
);
criterion_main!(benches);
