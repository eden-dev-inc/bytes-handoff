//! Demonstrates batching tiny fire-and-forget writes.
//!
//! `WriteCoalescer` keeps bytes visible to the parser immediately, but batches
//! already-accepted output bytes before submitting them to `WriteHandoff`.
//! Always flush at message boundaries when downstream visibility matters.

use bytes::Bytes;
use bytes_handoff::{WriteCoalescer, WriteHandoff, WriteHandoffConfig};
use tokio::io::AsyncReadExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (client, mut server) = tokio::io::duplex(128);
    let handoff = WriteHandoff::spawn(client, WriteHandoffConfig::new(16, 1024));
    let mut coalescer = WriteCoalescer::with_threshold_and_stats(handoff, 8);

    coalescer
        .write_fire_and_forget(Bytes::from_static(b"PING"))
        .await?;
    coalescer
        .write_fire_and_forget(Bytes::from_static(b" "))
        .await?;
    coalescer
        .write_fire_and_forget(Bytes::from_static(b"payload"))
        .await?;

    coalescer
        .write_fire_and_forget(Bytes::from_static(b"\n"))
        .await?;
    coalescer.flush().await?;

    let mut out = vec![0_u8; "PING payload\n".len()];
    server.read_exact(&mut out).await?;
    assert_eq!(out, b"PING payload\n");

    let stats = coalescer.stats();
    println!(
        "flushes={} avg_bytes_per_flush={:.1}",
        stats.flushes,
        stats.avg_bytes_per_flush()
    );

    Ok(())
}
