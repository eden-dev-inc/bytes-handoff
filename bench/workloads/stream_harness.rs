use bytes::{Bytes, BytesMut};
use bytes_handoff::{
    DEFAULT_WRITE_COALESCE_THRESHOLD, HandoffBuffer, HandoffBufferConfig, MonoioWriteCoalescer,
    MonoioWriteHandoff, WriteCoalescer, WriteHandoff, WriteHandoffConfig,
};
use hdrhistogram::Histogram;
use monoio::buf::IoBufMut as _;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::runtime::Builder;
use tokio::time::timeout;

mod process_usage {
    include!("process_usage.rs");
}
use process_usage::ProcessCpuSnapshot;

#[derive(Clone, Copy)]
enum Scenario {
    Fragmented,
    Coalesced,
}

impl Scenario {
    fn parse(value: &str) -> Self {
        match value {
            "fragmented" => Self::Fragmented,
            "coalesced" => Self::Coalesced,
            _ => panic!("invalid --scenario: {value} (expected fragmented|coalesced)"),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Fragmented => "fragmented",
            Self::Coalesced => "coalesced",
        }
    }
}

#[derive(Clone, Copy)]
enum Transport {
    Duplex,
    Cached,
    Tcp,
}

impl Transport {
    fn parse(value: &str) -> Self {
        match value {
            "duplex" => Self::Duplex,
            "cached" => Self::Cached,
            "tcp" => Self::Tcp,
            _ => panic!("invalid --transport: {value} (expected duplex|cached|tcp)"),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Duplex => "duplex",
            Self::Cached => "cached",
            Self::Tcp => "tcp",
        }
    }
}

#[derive(Clone, Copy)]
enum CompletionMode {
    Ticket,
    FireAndForget,
}

impl CompletionMode {
    fn parse(value: &str) -> Self {
        match value {
            "ticket" => Self::Ticket,
            "fire_and_forget" => Self::FireAndForget,
            _ => panic!("invalid --completion: {value} (expected ticket|fire_and_forget)"),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Ticket => "ticket",
            Self::FireAndForget => "fire_and_forget",
        }
    }
}

#[derive(Clone, Copy)]
enum Implementation {
    Handoff,
    MonoioHandoff,
    BytesMutHandoff,
    ManualVec,
    RawCopy,
}

impl Implementation {
    fn parse(value: &str) -> Self {
        match value {
            "handoff" => Self::Handoff,
            "monoio_handoff" => Self::MonoioHandoff,
            "bytesmut_handoff" => Self::BytesMutHandoff,
            "manual_vec" => Self::ManualVec,
            "raw_copy" => Self::RawCopy,
            _ => panic!(
                "invalid --implementation: {value} (expected handoff|monoio_handoff|bytesmut_handoff|manual_vec|raw_copy)"
            ),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Handoff => "handoff",
            Self::MonoioHandoff => "monoio_handoff",
            Self::BytesMutHandoff => "bytesmut_handoff",
            Self::ManualVec => "manual_vec",
            Self::RawCopy => "raw_copy",
        }
    }
}

#[derive(Clone, Copy)]
enum Role {
    Integrated,
    TcpService,
    TcpDriver,
}

impl Role {
    fn parse(value: &str) -> Self {
        match value {
            "integrated" => Self::Integrated,
            "tcp-service" => Self::TcpService,
            "tcp-driver" => Self::TcpDriver,
            _ => panic!("invalid --role: {value} (expected integrated|tcp-service|tcp-driver)"),
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            Self::Integrated => "integrated",
            Self::TcpService => "tcp-service",
            Self::TcpDriver => "tcp-driver",
        }
    }
}

#[derive(Clone, Copy)]
struct Config {
    transport: Transport,
    implementation: Implementation,
    scenario: Scenario,
    completion: CompletionMode,
    worker_threads: usize,
    connections: usize,
    route_frames: usize,
    frame_len: usize,
    tunnel_bytes: usize,
    input_fragment: usize,
    read_reserve: usize,
    handoff_flush_bytes: usize,
    write_pending_bytes: usize,
    duplex_capacity: usize,
    iterations: usize,
    duration_seconds: f64,
}

#[derive(Clone)]
struct PayloadSet {
    payloads: Arc<Vec<Bytes>>,
}

impl PayloadSet {
    fn new(config: Config) -> Self {
        let payloads = (0..config.connections)
            .map(|connection_id| payload(connection_id, config))
            .collect();
        Self {
            payloads: Arc::new(payloads),
        }
    }

    fn get(&self, connection_id: usize) -> Bytes {
        self.payloads[connection_id % self.payloads.len()].clone()
    }
}

struct Cli {
    config: Config,
    role: Role,
    service_addr: SocketAddr,
    sink_addr: SocketAddr,
    ready_file: Option<PathBuf>,
    idle_timeout: Duration,
}

impl Config {
    fn bytes_per_connection(self) -> usize {
        self.route_frames * (self.frame_len + 1) + b"TUNNEL\n".len() + self.tunnel_bytes
    }

    fn max_output_items(self) -> usize {
        let tunnel_chunks = self
            .tunnel_bytes
            .saturating_add(self.handoff_flush_bytes.saturating_sub(1))
            / self.handoff_flush_bytes;
        self.route_frames + tunnel_chunks.saturating_add(16)
    }

    fn write_pending_bytes(self) -> usize {
        if self.write_pending_bytes == 0 {
            self.read_reserve.saturating_mul(2)
        } else {
            self.write_pending_bytes
        }
    }
}

struct RunResult {
    total_bytes: usize,
    total_streams: usize,
    iterations: usize,
    total_seconds: f64,
    latency: LatencySummary,
}

struct WorkerRunResult {
    total_bytes: usize,
    total_streams: usize,
    iterations: usize,
    latency: Histogram<u64>,
}

impl WorkerRunResult {
    fn new() -> Self {
        Self {
            total_bytes: 0,
            total_streams: 0,
            iterations: 0,
            latency: latency_histogram(),
        }
    }

    fn record(&mut self, result: ConnectionResult) {
        self.total_bytes += result.bytes;
        self.total_streams += 1;
        self.latency
            .record(result.latency_micros.max(1))
            .expect("record stream latency");
    }
}

struct ConnectionResult {
    bytes: usize,
    latency_micros: u64,
}

#[derive(Clone, Copy)]
struct LatencySummary {
    p50_micros: u64,
    p95_micros: u64,
    p99_micros: u64,
    p999_micros: u64,
    max_micros: u64,
}

impl LatencySummary {
    fn empty() -> Self {
        Self {
            p50_micros: 0,
            p95_micros: 0,
            p99_micros: 0,
            p999_micros: 0,
            max_micros: 0,
        }
    }
}

fn parse_args() -> Cli {
    let mut transport = Transport::Cached;
    let mut implementation = Implementation::Handoff;
    let mut scenario = Scenario::Fragmented;
    let mut completion = CompletionMode::Ticket;
    let mut role = Role::Integrated;
    let mut worker_threads = std::thread::available_parallelism().map_or(4, |n| n.get());
    let mut connections = 64usize;
    let mut route_frames = 64usize;
    let mut frame_len = 63usize;
    let mut tunnel_bytes = 64 * 1024usize;
    let mut input_fragment_set = false;
    let mut input_fragment = 64usize;
    let mut read_reserve = 16 * 1024usize;
    let mut handoff_flush_bytes = 0usize;
    let mut write_pending_bytes = 0usize;
    let mut duplex_capacity = 256 * 1024usize;
    let mut iterations = 1usize;
    let mut duration_seconds = 0.0f64;
    let mut service_addr = "127.0.0.1:39000"
        .parse::<SocketAddr>()
        .expect("parse default service address");
    let mut sink_addr = "127.0.0.1:39001"
        .parse::<SocketAddr>()
        .expect("parse default sink address");
    let mut ready_file = None;
    let mut idle_timeout_millis = 2_000u64;

    let args: Vec<String> = std::env::args().collect();
    let mut i = 1usize;
    while i < args.len() {
        match args[i].as_str() {
            "--transport" if i + 1 < args.len() => {
                transport = Transport::parse(&args[i + 1]);
                i += 2;
            }
            "--implementation" if i + 1 < args.len() => {
                implementation = Implementation::parse(&args[i + 1]);
                i += 2;
            }
            "--scenario" if i + 1 < args.len() => {
                scenario = Scenario::parse(&args[i + 1]);
                i += 2;
            }
            "--completion" if i + 1 < args.len() => {
                completion = CompletionMode::parse(&args[i + 1]);
                i += 2;
            }
            "--role" if i + 1 < args.len() => {
                role = Role::parse(&args[i + 1]);
                i += 2;
            }
            "--worker-threads" if i + 1 < args.len() => {
                worker_threads = args[i + 1]
                    .parse()
                    .expect("--worker-threads must be an integer");
                i += 2;
            }
            "--connections" if i + 1 < args.len() => {
                connections = args[i + 1].parse().expect("--connections must be an integer");
                i += 2;
            }
            "--route-frames" if i + 1 < args.len() => {
                route_frames = args[i + 1].parse().expect("--route-frames must be an integer");
                i += 2;
            }
            "--frame-len" if i + 1 < args.len() => {
                frame_len = args[i + 1].parse().expect("--frame-len must be an integer");
                i += 2;
            }
            "--tunnel-bytes" if i + 1 < args.len() => {
                tunnel_bytes = args[i + 1].parse().expect("--tunnel-bytes must be an integer");
                i += 2;
            }
            "--input-fragment" if i + 1 < args.len() => {
                input_fragment = args[i + 1]
                    .parse()
                    .expect("--input-fragment must be an integer");
                input_fragment_set = true;
                i += 2;
            }
            "--read-reserve" if i + 1 < args.len() => {
                read_reserve = args[i + 1].parse().expect("--read-reserve must be an integer");
                i += 2;
            }
            "--handoff-flush-bytes" if i + 1 < args.len() => {
                handoff_flush_bytes = args[i + 1]
                    .parse()
                    .expect("--handoff-flush-bytes must be an integer");
                i += 2;
            }
            "--write-pending-bytes" if i + 1 < args.len() => {
                write_pending_bytes = args[i + 1]
                    .parse()
                    .expect("--write-pending-bytes must be an integer");
                i += 2;
            }
            "--duplex-capacity" if i + 1 < args.len() => {
                duplex_capacity = args[i + 1]
                    .parse()
                    .expect("--duplex-capacity must be an integer");
                i += 2;
            }
            "--iterations" if i + 1 < args.len() => {
                iterations = args[i + 1].parse().expect("--iterations must be an integer");
                i += 2;
            }
            "--duration-seconds" if i + 1 < args.len() => {
                duration_seconds = args[i + 1]
                    .parse()
                    .expect("--duration-seconds must be a number");
                i += 2;
            }
            "--service-addr" if i + 1 < args.len() => {
                service_addr = args[i + 1]
                    .parse()
                    .expect("--service-addr must be host:port");
                i += 2;
            }
            "--sink-addr" if i + 1 < args.len() => {
                sink_addr = args[i + 1].parse().expect("--sink-addr must be host:port");
                i += 2;
            }
            "--ready-file" if i + 1 < args.len() => {
                ready_file = Some(PathBuf::from(&args[i + 1]));
                i += 2;
            }
            "--idle-timeout-millis" if i + 1 < args.len() => {
                idle_timeout_millis = args[i + 1]
                    .parse()
                    .expect("--idle-timeout-millis must be an integer");
                i += 2;
            }
            "--help" => {
                println!(
                    "Usage: bench_stream_harness [--transport duplex|cached|tcp] [--role integrated|tcp-service|tcp-driver] [--implementation handoff|monoio_handoff|bytesmut_handoff|manual_vec|raw_copy] [--scenario fragmented|coalesced] [--completion ticket|fire_and_forget] [--worker-threads N] [--connections N] [--route-frames N] [--frame-len N] [--tunnel-bytes N] [--input-fragment N] [--read-reserve N] [--handoff-flush-bytes N] [--write-pending-bytes N] [--duplex-capacity N] [--iterations N] [--duration-seconds N] [--service-addr HOST:PORT] [--sink-addr HOST:PORT] [--ready-file PATH] [--idle-timeout-millis N]"
                );
                println!("  duplex: in-memory client/proxy/sink transport");
                println!("  cached: prebuilt payload reader plus counting sink, without driver/sink tasks");
                println!("  tcp: localhost TCP client/proxy/sink transport");
                println!("  tcp-service: service-only process for split-core TCP runs");
                println!("  tcp-driver: client plus sink process for split-core TCP runs");
                println!("  handoff: HandoffBuffer plus WriteHandoff");
                println!(
                    "  monoio_handoff: Monoio HandoffBuffer plus WriteHandoff, duplex|cached transport only"
                );
                println!("  bytesmut_handoff: direct BytesMut read_buf plus WriteHandoff");
                println!("  manual_vec: Vec-backed parser with direct writes");
                println!("  raw_copy: unparsed async copy lower bound");
                println!("  fragmented: many small client writes, default input fragment 64 bytes");
                println!("  coalesced: larger client writes, default input fragment = read reserve");
                std::process::exit(0);
            }
            arg => panic!("unknown arg: {arg}"),
        }
    }

    assert!(worker_threads >= 1, "--worker-threads must be >= 1");
    assert!(connections >= 1, "--connections must be >= 1");
    assert!(route_frames >= 1, "--route-frames must be >= 1");
    assert!(frame_len >= 16, "--frame-len must be >= 16");
    assert!(tunnel_bytes >= 1, "--tunnel-bytes must be >= 1");
    assert!(read_reserve >= 1, "--read-reserve must be >= 1");
    assert!(
        write_pending_bytes == 0 || write_pending_bytes >= read_reserve,
        "--write-pending-bytes must be zero/default or >= --read-reserve"
    );
    assert!(duplex_capacity >= 1, "--duplex-capacity must be >= 1");
    assert!(iterations >= 1, "--iterations must be >= 1");
    assert!(
        idle_timeout_millis >= 1,
        "--idle-timeout-millis must be >= 1"
    );
    assert!(
        duration_seconds >= 0.0,
        "--duration-seconds must be zero or positive"
    );

    if !input_fragment_set && matches!(scenario, Scenario::Coalesced) {
        input_fragment = read_reserve;
    }
    if handoff_flush_bytes == 0 {
        handoff_flush_bytes = DEFAULT_WRITE_COALESCE_THRESHOLD;
    }
    assert!(input_fragment >= 1, "--input-fragment must be >= 1");
    assert!(
        handoff_flush_bytes >= 1,
        "--handoff-flush-bytes must be >= 1"
    );
    let effective_write_pending_bytes = if write_pending_bytes == 0 {
        read_reserve.saturating_mul(2)
    } else {
        write_pending_bytes
    };
    assert!(
        !matches!(
            implementation,
            Implementation::Handoff | Implementation::MonoioHandoff | Implementation::BytesMutHandoff
        ) || handoff_flush_bytes <= effective_write_pending_bytes,
        "--handoff-flush-bytes must be <= effective write pending byte budget"
    );
    assert!(
        !matches!(implementation, Implementation::MonoioHandoff)
            || matches!(transport, Transport::Duplex | Transport::Cached),
        "--implementation monoio_handoff currently supports --transport duplex|cached only"
    );
    assert!(
        !matches!(implementation, Implementation::MonoioHandoff) || matches!(role, Role::Integrated),
        "--implementation monoio_handoff currently supports --role integrated only"
    );

    let config = Config {
        transport,
        implementation,
        scenario,
        completion,
        worker_threads,
        connections,
        route_frames,
        frame_len,
        tunnel_bytes,
        input_fragment,
        read_reserve,
        handoff_flush_bytes,
        write_pending_bytes,
        duplex_capacity,
        iterations,
        duration_seconds,
    };

    Cli {
        config,
        role,
        service_addr,
        sink_addr,
        ready_file,
        idle_timeout: Duration::from_millis(idle_timeout_millis),
    }
}

fn payload(connection_id: usize, config: Config) -> Bytes {
    let mut out = Vec::with_capacity(config.bytes_per_connection());
    for frame_id in 0..config.route_frames {
        let label = format!("GET /tenant/{connection_id:04}/object/{frame_id:08}");
        out.extend_from_slice(label.as_bytes());
        out.resize(out.len() + config.frame_len.saturating_sub(label.len()), b'x');
        out.push(b'\n');
    }
    out.extend_from_slice(b"TUNNEL\n");
    for byte in 0..config.tunnel_bytes {
        out.push(((connection_id + byte) % 251) as u8);
    }
    Bytes::from(out)
}

fn run(config: Config) -> RunResult {
    if matches!(config.implementation, Implementation::MonoioHandoff) {
        return run_monoio(config);
    }
    let payloads = PayloadSet::new(config);

    let runtime = Builder::new_multi_thread()
        .worker_threads(config.worker_threads)
        .enable_all()
        .build()
        .expect("build benchmark runtime");

    let start = Instant::now();
    let (total_bytes, total_streams, iterations, latency) = runtime.block_on(async {
        let mut total_bytes = 0usize;
        let mut total_streams = 0usize;
        let mut iterations = 0usize;
        let mut latency = latency_histogram();
        let deadline = if config.duration_seconds > 0.0 {
            Some(Instant::now() + Duration::from_secs_f64(config.duration_seconds))
        } else {
            None
        };

        loop {
            let results = match config.transport {
                Transport::Duplex => run_duplex(config, payloads.clone()).await,
                Transport::Cached => run_cached(config, payloads.clone()).await,
                Transport::Tcp => run_tcp(config, payloads.clone()).await,
            };
            for result in results {
                total_bytes += result.bytes;
                total_streams += 1;
                latency
                    .record(result.latency_micros.max(1))
                    .expect("record stream latency");
            }
            iterations += 1;

            if iterations >= config.iterations && deadline.is_none_or(|end| Instant::now() >= end)
            {
                break;
            }
        }

        (total_bytes, total_streams, iterations, summarize_latency(&latency))
    });

    RunResult {
        total_bytes,
        total_streams,
        iterations,
        total_seconds: start.elapsed().as_secs_f64(),
        latency,
    }
}

fn run_monoio(config: Config) -> RunResult {
    assert!(
        matches!(config.transport, Transport::Duplex | Transport::Cached),
        "monoio_handoff currently supports duplex|cached transport only"
    );
    let payloads = PayloadSet::new(config);
    let start = Instant::now();
    let deadline = if config.duration_seconds > 0.0 {
        Some(start + Duration::from_secs_f64(config.duration_seconds))
    } else {
        None
    };
    let worker_threads = config.worker_threads.min(config.connections);
    let mut handles = Vec::with_capacity(worker_threads);
    for worker_id in 0..worker_threads {
        let payloads = payloads.clone();
        handles.push(
            std::thread::Builder::new()
                .name(format!("monoio-harness-{worker_id}"))
                .spawn(move || {
                    run_monoio_worker(config, payloads, worker_id, worker_threads, deadline)
                })
                .expect("spawn monoio harness worker"),
        );
    }

    let mut total_bytes = 0usize;
    let mut total_streams = 0usize;
    let mut latency = latency_histogram();
    for handle in handles {
        let worker = handle.join().expect("monoio harness worker joins");
        total_bytes += worker.total_bytes;
        total_streams += worker.total_streams;
        latency
            .add(&worker.latency)
            .expect("merge monoio worker latency histogram");
    }

    let iterations = total_streams / config.connections;

    RunResult {
        total_bytes,
        total_streams,
        iterations,
        total_seconds: start.elapsed().as_secs_f64(),
        latency: summarize_latency(&latency),
    }
}

fn run_monoio_worker(
    config: Config,
    payloads: PayloadSet,
    worker_id: usize,
    worker_threads: usize,
    deadline: Option<Instant>,
) -> WorkerRunResult {
    let mut runtime = monoio::RuntimeBuilder::<monoio::LegacyDriver>::new()
        .build()
        .expect("build monoio benchmark runtime");

    runtime.block_on(async move {
        let mut result = WorkerRunResult::new();
        loop {
            let results = run_monoio_duplex_partition(
                config,
                payloads.clone(),
                worker_id,
                worker_threads,
                result.iterations,
            )
            .await;
            for connection in results {
                result.record(connection);
            }
            result.iterations += 1;

            if result.iterations >= config.iterations
                && deadline.is_none_or(|end| Instant::now() >= end)
            {
                break;
            }
        }

        result
    })
}

fn latency_histogram() -> Histogram<u64> {
    Histogram::<u64>::new_with_max(60_000_000, 3).expect("create latency histogram")
}

fn summarize_latency(histogram: &Histogram<u64>) -> LatencySummary {
    LatencySummary {
        p50_micros: histogram.value_at_quantile(0.50),
        p95_micros: histogram.value_at_quantile(0.95),
        p99_micros: histogram.value_at_quantile(0.99),
        p999_micros: histogram.value_at_quantile(0.999),
        max_micros: histogram.max(),
    }
}

async fn run_duplex(config: Config, payloads: PayloadSet) -> Vec<ConnectionResult> {
    let mut handles = Vec::with_capacity(config.connections);
    for connection_id in 0..config.connections {
        handles.push(tokio::spawn(run_duplex_connection(
            config,
            payloads.get(connection_id),
        )));
    }

    let mut results = Vec::with_capacity(config.connections);
    for handle in handles {
        results.push(handle.await.expect("connection task joins"));
    }
    results
}

async fn run_duplex_connection(config: Config, payload: Bytes) -> ConnectionResult {
    let expected_len = payload.len();
    let (client, inbound) = tokio::io::duplex(config.duplex_capacity);
    let (outbound, sink) = tokio::io::duplex(config.duplex_capacity);

    let start = Instant::now();
    let client = tokio::spawn(write_fragments(client, payload, config.input_fragment));
    let proxy = tokio::spawn(proxy_stream(inbound, outbound, config, expected_len));
    let sink = tokio::spawn(drain_expected(sink, expected_len));

    client.await.expect("client task joins");
    proxy.await.expect("proxy task joins");
    let drained = sink.await.expect("sink task joins");
    assert_eq!(drained, expected_len);
    ConnectionResult {
        bytes: drained,
        latency_micros: start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
    }
}

async fn run_cached(config: Config, payloads: PayloadSet) -> Vec<ConnectionResult> {
    let mut handles = Vec::with_capacity(config.connections);
    for connection_id in 0..config.connections {
        handles.push(tokio::spawn(run_cached_connection(
            config,
            payloads.get(connection_id),
        )));
    }

    let mut results = Vec::with_capacity(config.connections);
    for handle in handles {
        results.push(handle.await.expect("cached connection task joins"));
    }
    results
}

async fn run_cached_connection(config: Config, payload: Bytes) -> ConnectionResult {
    let expected_len = payload.len();
    let reader = CachedFragmentedReader::new(payload, config.input_fragment);
    let sink = CountWriter::default();
    let output_bytes = sink.bytes.clone();

    let start = Instant::now();
    proxy_stream(reader, sink, config, expected_len).await;
    let drained = output_bytes.load(Ordering::Acquire);
    assert_eq!(drained, expected_len);

    ConnectionResult {
        bytes: drained,
        latency_micros: start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
    }
}

struct CachedFragmentedReader {
    payload: Bytes,
    offset: usize,
    fragment: usize,
}

impl CachedFragmentedReader {
    fn new(payload: Bytes, fragment: usize) -> Self {
        Self {
            payload,
            offset: 0,
            fragment,
        }
    }
}

impl AsyncRead for CachedFragmentedReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let remaining = self.payload.len().saturating_sub(self.offset);
        let read = remaining.min(self.fragment).min(buf.remaining());
        if read > 0 {
            let start = self.offset;
            let end = start + read;
            buf.put_slice(&self.payload[start..end]);
            self.offset = end;
        }
        Poll::Ready(Ok(()))
    }
}

#[derive(Default)]
struct CountWriter {
    bytes: Arc<AtomicUsize>,
}

impl AsyncWrite for CountWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.bytes.fetch_add(buf.len(), Ordering::AcqRel);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let written = bufs.iter().map(|buf| buf.len()).sum();
        self.bytes.fetch_add(written, Ordering::AcqRel);
        Poll::Ready(Ok(written))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

async fn run_monoio_duplex_partition(
    config: Config,
    payloads: PayloadSet,
    worker_id: usize,
    worker_threads: usize,
    iteration: usize,
) -> Vec<ConnectionResult> {
    let worker_connections = config
        .connections
        .saturating_add(worker_threads - 1 - worker_id)
        / worker_threads;
    let mut handles = Vec::with_capacity(worker_connections);
    for connection_offset in (worker_id..config.connections).step_by(worker_threads) {
        let connection_id = iteration * config.connections + connection_offset;
        handles.push(monoio::spawn(run_monoio_duplex_connection(
            config,
            payloads.get(connection_id),
        )));
    }

    let mut results = Vec::with_capacity(worker_connections);
    for handle in handles {
        results.push(handle.await);
    }
    results
}

async fn run_monoio_duplex_connection(config: Config, payload: Bytes) -> ConnectionResult {
    let expected_len = payload.len();
    let reader = MonoioFragmentedReader::new(payload, config.input_fragment);
    let sink = MonoioCountWriter::default();
    let output_bytes = sink.bytes.clone();

    let start = Instant::now();
    proxy_monoio_handoff(reader, sink, config, expected_len).await;
    let drained = output_bytes.load(Ordering::Acquire);
    assert_eq!(drained, expected_len);

    ConnectionResult {
        bytes: drained,
        latency_micros: start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
    }
}

struct MonoioFragmentedReader {
    payload: Bytes,
    offset: usize,
    fragment: usize,
}

impl MonoioFragmentedReader {
    fn new(payload: Bytes, fragment: usize) -> Self {
        Self {
            payload,
            offset: 0,
            fragment,
        }
    }
}

impl monoio::io::AsyncReadRent for MonoioFragmentedReader {
    async fn read<T: monoio::buf::IoBufMut>(&mut self, mut buf: T) -> monoio::BufResult<usize, T> {
        let remaining = self.payload.len().saturating_sub(self.offset);
        let read = remaining.min(self.fragment).min(buf.bytes_total());
        if read > 0 {
            // SAFETY: `read` is capped by both source remaining bytes and
            // destination capacity, and the regions do not overlap.
            unsafe {
                std::ptr::copy_nonoverlapping(
                    self.payload.as_ptr().add(self.offset),
                    buf.write_ptr(),
                    read,
                );
                buf.set_init(read);
            }
            self.offset += read;
        } else {
            // SAFETY: setting the initialized prefix to zero is always valid.
            unsafe {
                buf.set_init(0);
            }
        }

        (Ok(read), buf)
    }

    async fn readv<T: monoio::buf::IoVecBufMut>(
        &mut self,
        mut buf_vec: T,
    ) -> monoio::BufResult<usize, T> {
        let Some(mut raw) = (unsafe { monoio::buf::RawBuf::new_from_iovec_mut(&mut buf_vec) })
        else {
            return (Ok(0), buf_vec);
        };
        let remaining = self.payload.len().saturating_sub(self.offset);
        let read = remaining.min(self.fragment).min(raw.bytes_total());
        if read > 0 {
            // SAFETY: `raw` points at the first valid output iovec and `read`
            // is capped to its capacity.
            unsafe {
                std::ptr::copy_nonoverlapping(
                    self.payload.as_ptr().add(self.offset),
                    raw.write_ptr(),
                    read,
                );
                raw.set_init(read);
                buf_vec.set_init(read);
            }
            self.offset += read;
        } else {
            // SAFETY: setting the initialized prefix to zero is always valid.
            unsafe {
                buf_vec.set_init(0);
            }
        }

        (Ok(read), buf_vec)
    }
}

#[derive(Default)]
struct MonoioCountWriter {
    bytes: Arc<AtomicUsize>,
}

impl monoio::io::AsyncWriteRent for MonoioCountWriter {
    async fn write<T: monoio::buf::IoBuf>(&mut self, buf: T) -> monoio::BufResult<usize, T> {
        let len = buf.bytes_init();
        self.bytes.fetch_add(len, Ordering::AcqRel);
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
            self.bytes.fetch_add(written, Ordering::AcqRel);
            (Ok(written), buf_vec)
        }
        #[cfg(not(unix))]
        {
            (
                Err(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "monoio_handoff harness path does not use vectored writes",
                )),
                buf_vec,
            )
        }
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }

    async fn shutdown(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

async fn run_tcp(config: Config, payloads: PayloadSet) -> Vec<ConnectionResult> {
    let sink_listener = bind_tcp_listener(
        "127.0.0.1:0"
            .parse()
            .expect("parse ephemeral sink address"),
    )
    .expect("bind tcp sink listener");
    let sink_addr = sink_listener.local_addr().expect("read tcp sink address");
    let service_listener = bind_tcp_listener(
        "127.0.0.1:0"
            .parse()
            .expect("parse ephemeral service address"),
    )
    .expect("bind tcp service listener");
    let service_addr = service_listener
        .local_addr()
        .expect("read tcp service address");

    let sink = tokio::spawn(accept_tcp_sinks(sink_listener, config));
    let service = tokio::spawn(accept_tcp_service(service_listener, sink_addr, config));

    let mut clients = Vec::with_capacity(config.connections);
    for connection_id in 0..config.connections {
        clients.push(tokio::spawn(run_tcp_client(
            service_addr,
            config,
            payloads.get(connection_id),
        )));
    }

    for client in clients {
        client.await.expect("tcp client task joins");
    }
    service.await.expect("tcp service task joins");
    sink.await.expect("tcp sink task joins")
}

fn run_tcp_service_process(
    config: Config,
    service_addr: SocketAddr,
    sink_addr: SocketAddr,
    ready_file: Option<PathBuf>,
    idle_timeout: Duration,
) -> RunResult {
    let runtime = Builder::new_multi_thread()
        .worker_threads(config.worker_threads)
        .enable_all()
        .build()
        .expect("build tcp service runtime");

    let (total_streams, active_seconds) = runtime.block_on(async {
        let listener = bind_tcp_listener(service_addr).expect("bind tcp service listener");
        if let Some(path) = ready_file {
            std::fs::write(path, listener.local_addr().expect("read service addr").to_string())
                .expect("write service ready file");
        }
        accept_tcp_service_until_idle(listener, sink_addr, config, idle_timeout).await
    });

    RunResult {
        total_bytes: total_streams * config.bytes_per_connection(),
        total_streams,
        iterations: 1,
        total_seconds: active_seconds,
        latency: LatencySummary::empty(),
    }
}

fn run_tcp_driver_process(
    config: Config,
    service_addr: SocketAddr,
    sink_addr: SocketAddr,
    idle_timeout: Duration,
) -> RunResult {
    let payloads = PayloadSet::new(config);
    let runtime = Builder::new_multi_thread()
        .worker_threads(config.worker_threads)
        .enable_all()
        .build()
        .expect("build tcp driver runtime");

    let start = Instant::now();
    let (total_bytes, total_streams, iterations, latency) = runtime.block_on(async {
        let sink_listener = bind_tcp_listener(sink_addr).expect("bind tcp sink listener");
        let sinks = tokio::spawn(accept_tcp_sinks_until_idle(
            sink_listener,
            config,
            idle_timeout,
        ));

        let iterations = run_tcp_clients_until_done(service_addr, config, payloads).await;
        let results = sinks.await.expect("tcp split sink task joins");

        let mut total_bytes = 0usize;
        let mut latency =
            Histogram::<u64>::new_with_max(60_000_000, 3).expect("create latency histogram");
        for result in &results {
            total_bytes += result.bytes;
            latency
                .record(result.latency_micros.max(1))
                .expect("record stream latency");
        }
        (
            total_bytes,
            results.len(),
            iterations,
            summarize_latency(&latency),
        )
    });

    RunResult {
        total_bytes,
        total_streams,
        iterations,
        total_seconds: start.elapsed().as_secs_f64(),
        latency,
    }
}

fn bind_tcp_listener(addr: SocketAddr) -> std::io::Result<TcpListener> {
    let socket = if addr.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };
    socket.set_reuseaddr(true)?;
    socket.bind(addr)?;
    socket.listen(1024)
}

async fn run_tcp_clients_until_done(
    service_addr: SocketAddr,
    config: Config,
    payloads: PayloadSet,
) -> usize {
    let mut iterations = 0usize;
    let deadline = if config.duration_seconds > 0.0 {
        Some(Instant::now() + Duration::from_secs_f64(config.duration_seconds))
    } else {
        None
    };

    loop {
        let mut clients = Vec::with_capacity(config.connections);
        for connection_id in 0..config.connections {
            let connection_id = iterations * config.connections + connection_id;
            clients.push(tokio::spawn(run_tcp_client(
                service_addr,
                config,
                payloads.get(connection_id),
            )));
        }

        for client in clients {
            client.await.expect("tcp client task joins");
        }
        iterations += 1;

        if iterations >= config.iterations && deadline.is_none_or(|end| Instant::now() >= end) {
            break;
        }
    }

    iterations
}

async fn accept_tcp_service_until_idle(
    listener: TcpListener,
    sink_addr: SocketAddr,
    config: Config,
    idle_timeout: Duration,
) -> (usize, f64) {
    let mut proxies = Vec::new();
    let mut first_accept = None;
    loop {
        match timeout(idle_timeout, listener.accept()).await {
            Ok(Ok((inbound, _))) => {
                first_accept.get_or_insert_with(Instant::now);
                inbound
                    .set_nodelay(true)
                    .expect("set nodelay on service inbound stream");
                proxies.push(tokio::spawn(run_tcp_proxy(inbound, sink_addr, config)));
            }
            Ok(Err(error)) => panic!("accept tcp service client: {error}"),
            Err(_) if proxies.is_empty() => continue,
            Err(_) => break,
        }
    }

    let total_streams = proxies.len();
    for proxy in proxies {
        proxy.await.expect("tcp proxy task joins");
    }
    let active_seconds = first_accept.map_or(0.0, |start| start.elapsed().as_secs_f64());
    (total_streams, active_seconds)
}

async fn accept_tcp_sinks_until_idle(
    listener: TcpListener,
    config: Config,
    idle_timeout: Duration,
) -> Vec<ConnectionResult> {
    let mut sinks = Vec::new();
    loop {
        match timeout(idle_timeout, listener.accept()).await {
            Ok(Ok((stream, _))) => {
                stream
                    .set_nodelay(true)
                    .expect("set nodelay on sink stream");
                sinks.push(tokio::spawn(drain_connection(
                    stream,
                    config.bytes_per_connection(),
                )));
            }
            Ok(Err(error)) => panic!("accept tcp sink stream: {error}"),
            Err(_) if sinks.is_empty() => continue,
            Err(_) => break,
        }
    }

    let mut results = Vec::with_capacity(sinks.len());
    for sink in sinks {
        let result = sink.await.expect("tcp sink task joins");
        assert_eq!(result.bytes, config.bytes_per_connection());
        results.push(result);
    }
    results
}

async fn accept_tcp_service(listener: TcpListener, sink_addr: SocketAddr, config: Config) {
    let mut proxies = Vec::with_capacity(config.connections);
    for _ in 0..config.connections {
        let (inbound, _) = listener.accept().await.expect("accept tcp service client");
        inbound
            .set_nodelay(true)
            .expect("set nodelay on service inbound stream");
        proxies.push(tokio::spawn(run_tcp_proxy(inbound, sink_addr, config)));
    }

    for proxy in proxies {
        proxy.await.expect("tcp proxy task joins");
    }
}

async fn run_tcp_proxy(inbound: TcpStream, sink_addr: SocketAddr, config: Config) {
    let outbound = TcpStream::connect(sink_addr)
        .await
        .expect("connect tcp proxy to sink");
    outbound
        .set_nodelay(true)
        .expect("set nodelay on proxy outbound stream");
    proxy_stream(
        inbound,
        outbound,
        config,
        config.bytes_per_connection(),
    )
    .await;
}

async fn accept_tcp_sinks(listener: TcpListener, config: Config) -> Vec<ConnectionResult> {
    let mut sinks = Vec::with_capacity(config.connections);
    for _ in 0..config.connections {
        let (stream, _) = listener.accept().await.expect("accept tcp sink stream");
        stream
            .set_nodelay(true)
            .expect("set nodelay on sink stream");
        sinks.push(tokio::spawn(drain_connection(
            stream,
            config.bytes_per_connection(),
        )));
    }

    let mut results = Vec::with_capacity(config.connections);
    for sink in sinks {
        let result = sink.await.expect("tcp sink task joins");
        assert_eq!(result.bytes, config.bytes_per_connection());
        results.push(result);
    }
    results
}

async fn run_tcp_client(service_addr: SocketAddr, config: Config, payload: Bytes) {
    let stream = TcpStream::connect(service_addr)
        .await
        .expect("connect tcp client to service");
    stream
        .set_nodelay(true)
        .expect("set nodelay on tcp client stream");
    write_fragments(stream, payload, config.input_fragment).await;
}

async fn drain_connection<R>(reader: R, expected: usize) -> ConnectionResult
where
    R: AsyncRead + Unpin,
{
    let start = Instant::now();
    let bytes = drain_expected(reader, expected).await;
    ConnectionResult {
        bytes,
        latency_micros: start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
    }
}

async fn write_fragments<W>(mut writer: W, payload: Bytes, fragment: usize)
where
    W: AsyncWrite + Unpin,
{
    for chunk in payload.chunks(fragment) {
        writer.write_all(chunk).await.expect("write input fragment");
    }
    writer.shutdown().await.expect("shutdown input writer");
}

async fn drain_expected<R>(mut reader: R, expected: usize) -> usize
where
    R: AsyncRead + Unpin,
{
    let mut total = 0;
    let mut buf = vec![0_u8; 64 * 1024];
    while total < expected {
        let read = reader.read(&mut buf).await.expect("read harness sink");
        if read == 0 {
            break;
        }
        total += read;
    }
    total
}

async fn proxy_stream<R, W>(reader: R, writer: W, config: Config, expected_len: usize)
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin + Send + 'static,
{
    match config.implementation {
        Implementation::Handoff => proxy_handoff(reader, writer, config, expected_len).await,
        Implementation::MonoioHandoff => {
            panic!("monoio_handoff uses a dedicated Monoio cached/duplex harness path")
        }
        Implementation::BytesMutHandoff => {
            proxy_bytesmut_handoff(reader, writer, config, expected_len).await
        }
        Implementation::ManualVec => proxy_manual_vec(reader, writer, config).await,
        Implementation::RawCopy => proxy_raw_copy(reader, writer).await,
    }
}

async fn proxy_handoff<R, W>(mut reader: R, writer: W, config: Config, expected_len: usize)
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let handoff = WriteHandoff::spawn(
        writer,
        WriteHandoffConfig::new(config.max_output_items(), config.write_pending_bytes()),
    );
    let mut tunnel_coalescer =
        WriteCoalescer::with_threshold(handoff.clone(), config.handoff_flush_bytes);
    let mut buffer = HandoffBuffer::with_config(
        HandoffBufferConfig::new(expected_len + config.read_reserve)
            .with_read_reserve(config.read_reserve),
    );
    let mut tunnel = false;

    loop {
        let read = buffer
            .read_available(&mut reader)
            .await
            .expect("read harness input");
        if read == 0 {
            break;
        }

        if !tunnel {
            tunnel = route_complete_prefixes(&mut buffer, &handoff, config.completion).await;
        }

        if tunnel && !buffer.is_empty() {
            submit_tunnel(
                &handoff,
                &mut tunnel_coalescer,
                config.completion,
                buffer.freeze_all(),
            )
            .await;
        }
    }

    if !buffer.is_empty() {
        submit_tunnel(
            &handoff,
            &mut tunnel_coalescer,
            config.completion,
            buffer.freeze_all(),
        )
        .await;
    }
    tunnel_coalescer.flush().await.expect("flush tunnel coalescer");

    let barrier = handoff
        .write(Bytes::new())
        .await
        .expect("submit completion barrier");
    barrier.wait().await.expect("completion barrier");
    handoff.close();
}

async fn proxy_monoio_handoff<R, W>(
    mut reader: R,
    writer: W,
    config: Config,
    expected_len: usize,
) where
    R: monoio::io::AsyncReadRent,
    W: monoio::io::AsyncWriteRent + Unpin + 'static,
{
    let handoff = WriteHandoff::spawn_monoio(
        writer,
        WriteHandoffConfig::new(config.max_output_items() + 1, config.write_pending_bytes()),
    );
    let mut tunnel_coalescer =
        MonoioWriteCoalescer::with_threshold(handoff.clone(), config.handoff_flush_bytes);
    let mut buffer = HandoffBuffer::with_config(
        HandoffBufferConfig::new(expected_len + config.read_reserve)
            .with_read_reserve(config.read_reserve),
    );
    let mut tunnel = false;

    loop {
        let read = buffer
            .read_available_monoio(&mut reader)
            .await
            .expect("read monoio harness input");
        if read == 0 {
            break;
        }

        if !tunnel {
            tunnel = route_complete_prefixes_monoio(&mut buffer, &handoff, config.completion).await;
        }

        if tunnel && !buffer.is_empty() {
            submit_monoio_tunnel(
                &handoff,
                &mut tunnel_coalescer,
                config.completion,
                buffer.freeze_all(),
            )
            .await;
        }
    }

    if !buffer.is_empty() {
        submit_monoio_tunnel(
            &handoff,
            &mut tunnel_coalescer,
            config.completion,
            buffer.freeze_all(),
        )
        .await;
    }
    tunnel_coalescer
        .flush()
        .await
        .expect("flush monoio tunnel coalescer");

    let barrier = handoff
        .write(Bytes::new())
        .await
        .expect("submit monoio completion barrier");
    barrier.wait().await.expect("monoio completion barrier");
    handoff.close();
}

async fn proxy_bytesmut_handoff<R, W>(
    mut reader: R,
    writer: W,
    config: Config,
    expected_len: usize,
) where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let handoff = WriteHandoff::spawn(
        writer,
        WriteHandoffConfig::new(config.max_output_items(), config.write_pending_bytes()),
    );
    let mut tunnel_coalescer =
        WriteCoalescer::with_threshold(handoff.clone(), config.handoff_flush_bytes);
    let max_len = expected_len + config.read_reserve;
    let mut buffer = BytesMut::with_capacity(config.read_reserve);
    let mut tunnel = false;

    loop {
        buffer.reserve(config.read_reserve);
        let mut capped_reader = (&mut reader).take(config.read_reserve as u64);
        let read = capped_reader
            .read_buf(&mut buffer)
            .await
            .expect("read bytesmut baseline input");
        if read == 0 {
            break;
        }
        assert!(
            buffer.len() <= max_len,
            "bytesmut baseline exceeded configured buffer limit"
        );

        if !tunnel {
            tunnel =
                route_complete_prefixes_bytesmut(&mut buffer, &handoff, config.completion).await;
        }

        if tunnel && !buffer.is_empty() {
            submit_tunnel(
                &handoff,
                &mut tunnel_coalescer,
                config.completion,
                buffer.split().freeze(),
            )
            .await;
        }
    }

    if !buffer.is_empty() {
        submit_tunnel(
            &handoff,
            &mut tunnel_coalescer,
            config.completion,
            buffer.split().freeze(),
        )
        .await;
    }
    tunnel_coalescer.flush().await.expect("flush tunnel coalescer");

    let barrier = handoff
        .write(Bytes::new())
        .await
        .expect("submit bytesmut completion barrier");
    barrier.wait().await.expect("bytesmut completion barrier");
    handoff.close();
}

async fn proxy_manual_vec<R, W>(mut reader: R, mut writer: W, config: Config)
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut buf = Vec::with_capacity(config.read_reserve * 2);
    let mut scratch = vec![0_u8; config.read_reserve];
    let mut consumed = 0usize;
    let mut tunnel = false;

    loop {
        let read = reader
            .read(&mut scratch)
            .await
            .expect("read manual baseline input");
        if read == 0 {
            break;
        }
        buf.extend_from_slice(&scratch[..read]);

        if !tunnel {
            tunnel = write_manual_prefixes(&buf, &mut consumed, &mut writer).await;
            compact_manual_buffer(&mut buf, &mut consumed, config.read_reserve);
        }

        if tunnel && consumed < buf.len() {
            writer
                .write_all(&buf[consumed..])
                .await
                .expect("write manual baseline tunnel bytes");
            buf.clear();
            consumed = 0;
        }
    }

    if consumed < buf.len() {
        writer
            .write_all(&buf[consumed..])
            .await
            .expect("write manual baseline tail");
    }
    writer.shutdown().await.expect("shutdown manual baseline writer");
}

async fn write_manual_prefixes<W>(buf: &[u8], consumed: &mut usize, writer: &mut W) -> bool
where
    W: AsyncWrite + Unpin,
{
    loop {
        let available = &buf[*consumed..];
        if available.starts_with(b"TUNNEL\n") {
            return true;
        }
        if available.starts_with(b"TUN") {
            return false;
        }
        let Some(newline) = available.iter().position(|b| *b == b'\n') else {
            return false;
        };
        let end = *consumed + newline + 1;
        writer
            .write_all(&buf[*consumed..end])
            .await
            .expect("write manual baseline frame");
        *consumed = end;
    }
}

fn compact_manual_buffer(buf: &mut Vec<u8>, consumed: &mut usize, read_reserve: usize) {
    if *consumed == 0 {
        return;
    }
    if *consumed == buf.len() {
        buf.clear();
        *consumed = 0;
        return;
    }
    if *consumed >= read_reserve && *consumed * 2 >= buf.len() {
        buf.drain(..*consumed);
        *consumed = 0;
    }
}

async fn proxy_raw_copy<R, W>(mut reader: R, mut writer: W)
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    tokio::io::copy(&mut reader, &mut writer)
        .await
        .expect("raw copy proxy stream");
    writer.shutdown().await.expect("shutdown raw copy writer");
}

async fn route_complete_prefixes(
    buffer: &mut HandoffBuffer,
    handoff: &WriteHandoff,
    completion: CompletionMode,
) -> bool {
    loop {
        let bytes = buffer.peek();
        if bytes.starts_with(b"TUNNEL\n") {
            return true;
        }
        if bytes.starts_with(b"TUN") {
            return false;
        }
        let Some(newline) = bytes.iter().position(|b| *b == b'\n') else {
            return false;
        };
        let frame = buffer
            .split_prefix(newline + 1)
            .expect("newline prefix is in bounds");
        submit(handoff, completion, frame).await;
    }
}

async fn route_complete_prefixes_monoio(
    buffer: &mut HandoffBuffer,
    handoff: &MonoioWriteHandoff,
    completion: CompletionMode,
) -> bool {
    loop {
        let bytes = buffer.peek();
        if bytes.starts_with(b"TUNNEL\n") {
            return true;
        }
        if bytes.starts_with(b"TUN") {
            return false;
        }
        let Some(newline) = bytes.iter().position(|b| *b == b'\n') else {
            return false;
        };
        let frame = buffer
            .split_prefix(newline + 1)
            .expect("newline prefix is in bounds");
        submit_monoio(handoff, completion, frame).await;
    }
}

async fn route_complete_prefixes_bytesmut(
    buffer: &mut BytesMut,
    handoff: &WriteHandoff,
    completion: CompletionMode,
) -> bool {
    loop {
        if buffer.starts_with(b"TUNNEL\n") {
            return true;
        }
        if buffer.starts_with(b"TUN") {
            return false;
        }
        let Some(newline) = buffer.iter().position(|b| *b == b'\n') else {
            return false;
        };
        let frame = buffer.split_to(newline + 1).freeze();
        submit(handoff, completion, frame).await;
    }
}

async fn submit(handoff: &WriteHandoff, completion: CompletionMode, bytes: Bytes) {
    if bytes.is_empty() {
        return;
    }
    match completion {
        CompletionMode::Ticket => {
            let _ = handoff.write(bytes).await.expect("harness write handoff accepts bytes");
        }
        CompletionMode::FireAndForget => {
            handoff
                .write_fire_and_forget(bytes)
                .await
                .expect("harness write handoff accepts bytes");
        }
    }
}

async fn submit_tunnel(
    handoff: &WriteHandoff,
    coalescer: &mut WriteCoalescer,
    completion: CompletionMode,
    bytes: Bytes,
) {
    match completion {
        CompletionMode::Ticket => submit(handoff, completion, bytes).await,
        CompletionMode::FireAndForget => coalescer
            .write_fire_and_forget(bytes)
            .await
            .expect("harness coalesced write handoff accepts bytes"),
    }
}

async fn submit_monoio(handoff: &MonoioWriteHandoff, completion: CompletionMode, bytes: Bytes) {
    if bytes.is_empty() {
        return;
    }
    match completion {
        CompletionMode::Ticket => {
            if let Err(err) = handoff.try_write(bytes) {
                let _ = handoff
                    .write(err.into_bytes())
                    .await
                    .expect("monoio harness write handoff accepts bytes");
            }
        }
        CompletionMode::FireAndForget => {
            if let Err(err) = handoff.try_write_fire_and_forget(bytes) {
                handoff
                    .write_fire_and_forget(err.into_bytes())
                    .await
                    .expect("monoio harness write handoff accepts bytes");
            }
        }
    }
}

async fn submit_monoio_tunnel(
    handoff: &MonoioWriteHandoff,
    coalescer: &mut MonoioWriteCoalescer,
    completion: CompletionMode,
    bytes: Bytes,
) {
    match completion {
        CompletionMode::Ticket => submit_monoio(handoff, completion, bytes).await,
        CompletionMode::FireAndForget => coalescer
            .write_fire_and_forget(bytes)
            .await
            .expect("monoio harness coalesced write handoff accepts bytes"),
    }
}

fn main() {
    let cli = parse_args();
    let config = cli.config;
    let cpu_start = ProcessCpuSnapshot::capture().ok();
    let result = match cli.role {
        Role::Integrated => run(config),
        Role::TcpService => run_tcp_service_process(
            config,
            cli.service_addr,
            cli.sink_addr,
            cli.ready_file,
            cli.idle_timeout,
        ),
        Role::TcpDriver => {
            run_tcp_driver_process(config, cli.service_addr, cli.sink_addr, cli.idle_timeout)
        }
    };
    let cpu_usage = cpu_start.and_then(|start| {
        ProcessCpuSnapshot::capture()
            .ok()
            .map(|end| end.elapsed_since(start))
    });

    let bytes_per_sec = (result.total_bytes as f64) / result.total_seconds;
    let mib_per_sec = bytes_per_sec / (1024.0 * 1024.0);
    let gib_per_sec = bytes_per_sec / (1024.0 * 1024.0 * 1024.0);
    let streams_per_sec = (result.total_streams as f64) / result.total_seconds;
    let cpu_user_seconds = cpu_usage.map_or(0.0, |usage| usage.user_seconds);
    let cpu_system_seconds = cpu_usage.map_or(0.0, |usage| usage.system_seconds);
    let cpu_total_seconds = cpu_usage.map_or(0.0, |usage| usage.total_seconds);
    let cpu_avg_cores = if result.total_seconds > 0.0 {
        cpu_total_seconds / result.total_seconds
    } else {
        0.0
    };
    let cpu_utilization_pct = cpu_avg_cores * 100.0;
    let cpu_avg_cores_per_worker = cpu_avg_cores / (config.worker_threads as f64);
    let cpu_utilization_pct_per_worker = cpu_utilization_pct / (config.worker_threads as f64);
    let cpu_ns_per_byte = if result.total_bytes > 0 {
        (cpu_total_seconds * 1_000_000_000.0) / (result.total_bytes as f64)
    } else {
        0.0
    };

    println!("mode=stream_harness");
    println!("role={}", cli.role.as_str());
    println!("transport={}", config.transport.as_str());
    println!("implementation={}", config.implementation.as_str());
    println!("scenario={}", config.scenario.as_str());
    println!("completion={}", config.completion.as_str());
    println!("worker_threads={}", config.worker_threads);
    println!("connections={}", config.connections);
    println!("route_frames={}", config.route_frames);
    println!("frame_len={}", config.frame_len);
    println!("tunnel_bytes={}", config.tunnel_bytes);
    println!("input_fragment={}", config.input_fragment);
    println!("read_reserve={}", config.read_reserve);
    println!("handoff_flush_bytes={}", config.handoff_flush_bytes);
    println!("write_pending_bytes={}", config.write_pending_bytes());
    println!("duplex_capacity={}", config.duplex_capacity);
    println!("configured_iterations={}", config.iterations);
    println!("duration_seconds_target={:.6}", config.duration_seconds);
    println!("actual_iterations={}", result.iterations);
    println!("bytes_per_connection={}", config.bytes_per_connection());
    println!("total_streams={}", result.total_streams);
    println!("total_bytes={}", result.total_bytes);
    println!("total_seconds={:.6}", result.total_seconds);
    println!("bytes_per_sec={bytes_per_sec:.2}");
    println!("mib_per_sec={mib_per_sec:.2}");
    println!("gib_per_sec={gib_per_sec:.6}");
    println!("streams_per_sec={streams_per_sec:.2}");
    println!("cpu_usage_measured={}", cpu_usage.is_some());
    println!("cpu_user_seconds={cpu_user_seconds:.6}");
    println!("cpu_system_seconds={cpu_system_seconds:.6}");
    println!("cpu_total_seconds={cpu_total_seconds:.6}");
    println!("cpu_avg_cores={cpu_avg_cores:.6}");
    println!("cpu_utilization_pct={cpu_utilization_pct:.2}");
    println!("cpu_avg_cores_per_worker={cpu_avg_cores_per_worker:.6}");
    println!("cpu_utilization_pct_per_worker={cpu_utilization_pct_per_worker:.2}");
    println!("cpu_ns_per_byte={cpu_ns_per_byte:.2}");
    println!(
        "voluntary_context_switches={}",
        cpu_usage.map_or(0, |usage| usage.voluntary_context_switches)
    );
    println!(
        "involuntary_context_switches={}",
        cpu_usage.map_or(0, |usage| usage.involuntary_context_switches)
    );
    println!(
        "max_rss_bytes={}",
        cpu_usage.map_or(0, |usage| usage.max_rss_bytes)
    );
    println!("latency_p50_micros={}", result.latency.p50_micros);
    println!("latency_p95_micros={}", result.latency.p95_micros);
    println!("latency_p99_micros={}", result.latency.p99_micros);
    println!("latency_p999_micros={}", result.latency.p999_micros);
    println!("latency_max_micros={}", result.latency.max_micros);
}
