//! Demonstrates choosing a write coalescing threshold from measurements.
//!
//! Real applications should collect these measurements with the harness on the
//! target hardware. The tuner scores throughput against oldest-byte flush delay
//! and chooses the knee of the measured curve.

use bytes_handoff::{
    WriteCoalescingMeasurement, WriteCoalescingSearch, WriteCoalescingSearchConfig,
    WriteCoalescingSearchStep, WriteCoalescingTuner, WriteCoalescingTunerConfig,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tuner = WriteCoalescingTuner::new(WriteCoalescingTunerConfig {
        throughput_tolerance: 0.05,
        ..WriteCoalescingTunerConfig::default()
    })?;

    let recommendation = tuner.recommend([
        measurement(1, 1.0, 27002.68, 0.0),
        measurement(2 * 1024, 2.0, 15587.0, 15.8),
        measurement(8 * 1024, 6.0, 26621.0, 19.4),
        measurement(16 * 1024, 12.0, 33523.05, 17.0),
        measurement(32 * 1024, 22.5, 34000.0, 32.2),
    ])?;

    println!(
        "recommended={} bytes reason={}",
        recommendation.threshold_bytes(),
        recommendation.reason.as_str()
    );
    assert_eq!(recommendation.threshold_bytes(), 16 * 1024);

    let search = WriteCoalescingSearch::new(WriteCoalescingSearchConfig {
        max_threshold_bytes: 32 * 1024,
        batch_size: 3,
        ..WriteCoalescingSearchConfig::default()
    })?;
    match search.step(std::iter::empty::<WriteCoalescingMeasurement>())? {
        WriteCoalescingSearchStep::Measure { thresholds } => {
            println!("first thresholds to measure: {thresholds:?}");
        }
        WriteCoalescingSearchStep::Complete(recommendation) => {
            println!("already complete at {}", recommendation.threshold_bytes());
        }
    }

    Ok(())
}

fn measurement(
    threshold_bytes: usize,
    observed_input_chunks_per_flush: f64,
    throughput_mib_per_sec: f64,
    avg_flush_wait_micros: f64,
) -> WriteCoalescingMeasurement {
    WriteCoalescingMeasurement {
        threshold_bytes,
        input_fragment_bytes: 1460,
        observed_input_chunks_per_flush: Some(observed_input_chunks_per_flush),
        throughput_mib_per_sec,
        cpu_ns_per_byte: 0.0,
        connection_p99_micros: None,
        avg_flush_wait_micros: Some(avg_flush_wait_micros),
        max_flush_wait_micros: None,
    }
}
