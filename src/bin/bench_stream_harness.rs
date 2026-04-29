#[cfg(feature = "bench-tools")]
include!("../../bench/workloads/stream_harness.rs");

#[cfg(not(feature = "bench-tools"))]
fn main() {
    eprintln!("bench_stream_harness requires --features bench-tools");
    std::process::exit(1);
}
