//! Incremental byte ingestion and owned write handoff for async I/O boundaries.
//!
//! This crate is intentionally small: it does not replace `AsyncRead` or
//! `AsyncWrite`. It owns the byte lifecycle around those traits so callers can
//! peek at incomplete input, split complete prefixes into `Bytes`, preserve
//! tails across mode changes, coalesce tiny fire-and-forget writes, and submit
//! large owned writes without borrowing memory across an async socket operation.
//!
//! Enable the `monoio` feature to use the equivalent
//! `AsyncReadRent`/`AsyncWriteRent` paths (`HandoffBuffer::read_available_monoio`
//! and `MonoioWriteHandoff`) for single-threaded `monoio` runtimes.

mod error;
mod read;
mod tune;
mod write;

pub use error::{BackpressureReason, BufferError, WriteBackpressure, WriteError};
pub use read::{HandoffBuffer, HandoffBufferConfig};
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
#[cfg(feature = "monoio")]
pub use write::{MonoioWriteCoalescer, MonoioWriteHandoff};
