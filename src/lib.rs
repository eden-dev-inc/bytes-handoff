//! Incremental byte ingestion and owned write handoff for async I/O boundaries.
//!
//! This crate is intentionally small: it does not replace `AsyncRead` or
//! `AsyncWrite`. It owns the byte lifecycle around those traits so callers can
//! peek at incomplete input, split complete prefixes into `Bytes`, preserve
//! tails across mode changes, and submit large owned writes without borrowing
//! memory across an async socket operation.

mod error;
mod read;
mod write;

pub use error::{BackpressureReason, BufferError, WriteBackpressure, WriteError};
pub use read::{HandoffBuffer, HandoffBufferConfig};
pub use write::{WriteCompletion, WriteHandoff, WriteHandoffConfig, WriteTicket};
