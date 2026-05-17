//! Incremental byte ingestion and owned write handoff for async I/O boundaries.
//!
//! This crate is intentionally small: it does not replace `AsyncRead` or
//! `AsyncWrite`. It owns the byte lifecycle around those traits so callers can
//! peek at incomplete input, split complete prefixes into `Bytes`, preserve
//! tails across mode changes, coalesce tiny fire-and-forget writes, and submit
//! large owned writes without borrowing memory across an async socket operation.
//!
//! Enable the `monoio` feature to read into `HandoffBuffer` from
//! `AsyncReadRent` sources in thread-local `monoio` runtimes.
//!
//! Enable the `telemetry` feature to attach `fast-telemetry` read counters and
//! histograms to `HandoffBuffer`; the dependency and instrumentation are
//! compiled out when the feature is disabled.
//!
//! Enable `telemetry-otlp`, `telemetry-clickhouse`, or `telemetry-export*`
//! features when the parent application wants to serialize or ship those
//! metrics through `fast-telemetry` exporter loops.

mod error;
mod read;
#[cfg(feature = "telemetry")]
mod read_telemetry;
mod tune;
mod write;

pub use error::{BackpressureReason, BufferError, WriteBackpressure, WriteError};
#[cfg(any(
    feature = "telemetry-export",
    feature = "telemetry-export-dogstatsd",
    feature = "telemetry-export-otlp",
    feature = "telemetry-export-clickhouse",
    feature = "telemetry-monoio",
))]
pub use fast_telemetry_export as telemetry_export;
pub use read::{
    DEFAULT_MONOIO_SPARSE_READ_COPY_DENOMINATOR, DEFAULT_SMALL_PREFIX_COPY_MAX, HandoffBuffer,
    HandoffBufferConfig, HandoffBufferPolicy, HandoffDrainCursor,
};
#[cfg(feature = "telemetry")]
pub use read_telemetry::{
    HandoffReadHistogramSummary, HandoffReadMetrics, HandoffReadMetricsDogStatsDState,
    HandoffReadMetricsSnapshot, HandoffReadTelemetry, HandoffReadTelemetryHandle,
};
pub use tune::{
    DEFAULT_TUNING_BATCH_SIZE, DEFAULT_TUNING_MAX_READS_PER_FLUSH,
    DEFAULT_TUNING_MAX_THRESHOLD_BYTES, DEFAULT_TUNING_MAX_THRESHOLD_POINTS,
    DEFAULT_TUNING_MIN_THRESHOLD_BYTES, DEFAULT_TUNING_MIN_THRESHOLD_POINTS,
    DEFAULT_TUNING_THROUGHPUT_TOLERANCE, WriteCoalescingMeasurement, WriteCoalescingRecommendation,
    WriteCoalescingRecommendationReason, WriteCoalescingSearch, WriteCoalescingSearchConfig,
    WriteCoalescingSearchStep, WriteCoalescingTuner, WriteCoalescingTunerConfig,
    WriteCoalescingTuningError,
};
pub use write::{
    DEFAULT_WRITE_COALESCE_THRESHOLD, WriteCoalescer, WriteCoalescerConfig, WriteCoalescerStats,
    WriteCompletion, WriteHandoff, WriteHandoffConfig, WriteTicket,
};
