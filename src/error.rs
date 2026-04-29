use bytes::Bytes;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum BufferError {
    #[error("buffer limit exceeded: attempted {attempted} bytes, limit {limit} bytes")]
    LimitExceeded { attempted: usize, limit: usize },
    #[error("split prefix out of bounds: requested {requested} bytes, available {available} bytes")]
    SplitOutOfBounds { requested: usize, available: usize },
    #[error("read error: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Error)]
pub enum WriteError {
    #[error("write handoff is closed")]
    Closed,
    #[error("write exceeds byte budget: attempted {attempted} bytes, limit {limit} bytes")]
    ByteBudgetExceeded { attempted: usize, limit: usize },
    #[error("write error: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Error)]
#[error("write handoff backpressure: {reason}")]
pub struct WriteBackpressure {
    reason: BackpressureReason,
    bytes: Bytes,
}

impl WriteBackpressure {
    pub(crate) fn closed(bytes: Bytes) -> Self {
        Self {
            reason: BackpressureReason::Closed,
            bytes,
        }
    }

    pub(crate) fn queue_full(bytes: Bytes) -> Self {
        Self {
            reason: BackpressureReason::QueueFull,
            bytes,
        }
    }

    pub(crate) fn byte_budget_exceeded(bytes: Bytes, attempted: usize, limit: usize) -> Self {
        Self {
            reason: BackpressureReason::ByteBudgetExceeded { attempted, limit },
            bytes,
        }
    }

    pub fn into_bytes(self) -> Bytes {
        self.bytes
    }

    pub fn reason(&self) -> BackpressureReason {
        self.reason
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackpressureReason {
    Closed,
    QueueFull,
    ByteBudgetExceeded { attempted: usize, limit: usize },
}

impl std::fmt::Display for BackpressureReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Closed => f.write_str("closed"),
            Self::QueueFull => f.write_str("queue full"),
            Self::ByteBudgetExceeded { attempted, limit } => {
                write!(
                    f,
                    "byte budget exceeded: attempted {attempted} bytes, limit {limit} bytes"
                )
            }
        }
    }
}
