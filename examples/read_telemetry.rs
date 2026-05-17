//! Demonstrates opt-in read telemetry.
//!
//! Run with:
//! cargo run --features telemetry --example read_telemetry
//! cargo run --features telemetry-export --example read_telemetry

use bytes_handoff::{
    HandoffBuffer, HandoffReadMetricsDogStatsDState, HandoffReadTelemetry,
    HandoffReadTelemetryHandle,
};
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

    let mut dogstatsd = String::new();
    telemetry.export_dogstatsd(&mut dogstatsd, &[("component", "bytes_handoff")]);
    println!("{dogstatsd}");

    let mut dogstatsd_delta = String::new();
    let mut dogstatsd_state = HandoffReadMetricsDogStatsDState::new();
    telemetry.export_dogstatsd_delta(&mut dogstatsd_delta, &[], &mut dogstatsd_state);
    println!("{dogstatsd_delta}");

    #[cfg(feature = "telemetry-otlp")]
    {
        let mut metrics = Vec::new();
        telemetry.export_otlp(&mut metrics, fast_telemetry::otlp::now_nanos());
        println!("otlp_metrics={}", metrics.len());
    }

    #[cfg(feature = "telemetry-clickhouse")]
    {
        let mut batch = fast_telemetry::clickhouse::ClickHouseMetricBatch::new("bytes-handoff");
        telemetry.export_clickhouse(&mut batch, now_nanos());
        println!("clickhouse_rows={}", batch.total_rows());
    }

    Ok(())
}

#[cfg(feature = "telemetry-clickhouse")]
fn now_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}
