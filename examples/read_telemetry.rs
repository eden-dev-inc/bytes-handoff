//! Demonstrates opt-in read telemetry.
//!
//! Run with:
//! cargo run --features telemetry --example read_telemetry

use bytes_handoff::{HandoffBuffer, HandoffReadTelemetry, HandoffReadTelemetryHandle};
use tokio::io::AsyncWriteExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let telemetry = HandoffReadTelemetry::with_available_parallelism();
    let handle = HandoffReadTelemetryHandle::from_arc(&telemetry);
    let (mut client, mut server) = tokio::io::duplex(128);
    let mut buffer = HandoffBuffer::new(1024).with_telemetry(handle);

    client
        .write_all(b"route\npayload")
        .await
        .expect("write input");
    buffer.read_available(&mut server).await?;
    let route = buffer.split_prefix(6)?;
    buffer.advance(2)?;

    println!("route: {:?}", String::from_utf8_lossy(&route));
    println!("tail: {:?}", String::from_utf8_lossy(buffer.peek()));
    println!("snapshot: {:?}", telemetry.snapshot());
    println!("{}", telemetry.export_prometheus());

    Ok(())
}
