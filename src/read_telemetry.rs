use std::sync::Arc;

use fast_telemetry::{Counter, ExportMetrics, Histogram};

const SIZE_BUCKETS: &[u64] = &[
    0, 64, 128, 256, 512, 1_024, 2_048, 4_096, 8_192, 16_384, 32_768, 65_536, 131_072, 262_144,
    524_288, 1_048_576, 4_194_304, 16_777_216,
];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HandoffReadHistogramSummary {
    pub count: u64,
    pub sum: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HandoffReadMetricsSnapshot {
    pub read_calls: u64,
    pub read_bytes: u64,
    pub zero_reads: u64,
    pub buffer_growths: u64,
    pub buffer_growth_bytes: u64,
    pub split_prefixes: u64,
    pub split_prefix_bytes: u64,
    pub copied_prefixes: u64,
    pub copied_prefix_bytes: u64,
    pub frozen_prefixes: u64,
    pub frozen_prefix_bytes: u64,
    pub mutable_prefixes: u64,
    pub mutable_prefix_bytes: u64,
    pub freeze_all_calls: u64,
    pub freeze_all_bytes: u64,
    pub advances: u64,
    pub advanced_bytes: u64,
    pub tails_taken: u64,
    pub tail_bytes: u64,
    pub read_size_bytes: HandoffReadHistogramSummary,
    pub buffered_bytes: HandoffReadHistogramSummary,
}

#[derive(ExportMetrics)]
#[metric_prefix = "bytes_handoff_read"]
pub struct HandoffReadMetrics {
    #[help = "Total read attempts completed by HandoffBuffer"]
    pub read_calls: Counter,

    #[help = "Total bytes read into HandoffBuffer"]
    pub read_bytes: Counter,

    #[help = "Total reads that returned no bytes"]
    pub zero_reads: Counter,

    #[help = "Total buffer growth events before reads"]
    pub buffer_growths: Counter,

    #[help = "Total spare capacity bytes reserved during buffer growth"]
    pub buffer_growth_bytes: Counter,

    #[help = "Total immutable prefix splits"]
    pub split_prefixes: Counter,

    #[help = "Total bytes emitted by immutable prefix splits"]
    pub split_prefix_bytes: Counter,

    #[help = "Total immutable prefix splits served by copying"]
    pub copied_prefixes: Counter,

    #[help = "Total bytes emitted by copied immutable prefix splits"]
    pub copied_prefix_bytes: Counter,

    #[help = "Total immutable prefix splits served by freezing"]
    pub frozen_prefixes: Counter,

    #[help = "Total bytes emitted by frozen immutable prefix splits"]
    pub frozen_prefix_bytes: Counter,

    #[help = "Total mutable prefix splits"]
    pub mutable_prefixes: Counter,

    #[help = "Total bytes emitted by mutable prefix splits"]
    pub mutable_prefix_bytes: Counter,

    #[help = "Total calls that freeze all buffered bytes"]
    pub freeze_all_calls: Counter,

    #[help = "Total bytes emitted by freeze_all"]
    pub freeze_all_bytes: Counter,

    #[help = "Total buffer advance calls"]
    pub advances: Counter,

    #[help = "Total bytes consumed by advance calls"]
    pub advanced_bytes: Counter,

    #[help = "Total tail handoffs"]
    pub tails_taken: Counter,

    #[help = "Total bytes emitted by tail handoffs"]
    pub tail_bytes: Counter,

    #[help = "Read sizes returned by HandoffBuffer"]
    pub read_size_bytes: Histogram,

    #[help = "Buffered byte counts observed after read and consume operations"]
    pub buffered_bytes: Histogram,
}

impl HandoffReadMetrics {
    pub fn new(metric_shards: usize) -> Self {
        let metric_shards = metric_shards.max(1);
        Self {
            read_calls: Counter::new(metric_shards),
            read_bytes: Counter::new(metric_shards),
            zero_reads: Counter::new(metric_shards),
            buffer_growths: Counter::new(metric_shards),
            buffer_growth_bytes: Counter::new(metric_shards),
            split_prefixes: Counter::new(metric_shards),
            split_prefix_bytes: Counter::new(metric_shards),
            copied_prefixes: Counter::new(metric_shards),
            copied_prefix_bytes: Counter::new(metric_shards),
            frozen_prefixes: Counter::new(metric_shards),
            frozen_prefix_bytes: Counter::new(metric_shards),
            mutable_prefixes: Counter::new(metric_shards),
            mutable_prefix_bytes: Counter::new(metric_shards),
            freeze_all_calls: Counter::new(metric_shards),
            freeze_all_bytes: Counter::new(metric_shards),
            advances: Counter::new(metric_shards),
            advanced_bytes: Counter::new(metric_shards),
            tails_taken: Counter::new(metric_shards),
            tail_bytes: Counter::new(metric_shards),
            read_size_bytes: Histogram::new(SIZE_BUCKETS, metric_shards),
            buffered_bytes: Histogram::new(SIZE_BUCKETS, metric_shards),
        }
    }
}

pub struct HandoffReadTelemetry {
    metrics: HandoffReadMetrics,
}

impl std::fmt::Debug for HandoffReadTelemetry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HandoffReadTelemetry")
            .field("snapshot", &self.snapshot())
            .finish()
    }
}

impl HandoffReadTelemetry {
    pub fn new(metric_shards: usize) -> Arc<Self> {
        Arc::new(Self {
            metrics: HandoffReadMetrics::new(metric_shards),
        })
    }

    pub fn with_available_parallelism() -> Arc<Self> {
        let metric_shards = std::thread::available_parallelism()
            .map(|value| value.get())
            .unwrap_or(1);
        Self::new(metric_shards)
    }

    #[inline(always)]
    pub fn metrics(&self) -> &HandoffReadMetrics {
        &self.metrics
    }

    pub fn export_prometheus(&self) -> String {
        let mut output = String::new();
        self.metrics.export_prometheus(&mut output);
        output
    }

    pub fn snapshot(&self) -> HandoffReadMetricsSnapshot {
        HandoffReadMetricsSnapshot {
            read_calls: counter_sum(&self.metrics.read_calls),
            read_bytes: counter_sum(&self.metrics.read_bytes),
            zero_reads: counter_sum(&self.metrics.zero_reads),
            buffer_growths: counter_sum(&self.metrics.buffer_growths),
            buffer_growth_bytes: counter_sum(&self.metrics.buffer_growth_bytes),
            split_prefixes: counter_sum(&self.metrics.split_prefixes),
            split_prefix_bytes: counter_sum(&self.metrics.split_prefix_bytes),
            copied_prefixes: counter_sum(&self.metrics.copied_prefixes),
            copied_prefix_bytes: counter_sum(&self.metrics.copied_prefix_bytes),
            frozen_prefixes: counter_sum(&self.metrics.frozen_prefixes),
            frozen_prefix_bytes: counter_sum(&self.metrics.frozen_prefix_bytes),
            mutable_prefixes: counter_sum(&self.metrics.mutable_prefixes),
            mutable_prefix_bytes: counter_sum(&self.metrics.mutable_prefix_bytes),
            freeze_all_calls: counter_sum(&self.metrics.freeze_all_calls),
            freeze_all_bytes: counter_sum(&self.metrics.freeze_all_bytes),
            advances: counter_sum(&self.metrics.advances),
            advanced_bytes: counter_sum(&self.metrics.advanced_bytes),
            tails_taken: counter_sum(&self.metrics.tails_taken),
            tail_bytes: counter_sum(&self.metrics.tail_bytes),
            read_size_bytes: histogram_summary(&self.metrics.read_size_bytes),
            buffered_bytes: histogram_summary(&self.metrics.buffered_bytes),
        }
    }

    #[inline(always)]
    fn record_read(&self, read: usize, buffered: usize) {
        self.metrics.read_calls.inc();
        self.metrics.read_bytes.add(saturating_isize(read));
        if read == 0 {
            self.metrics.zero_reads.inc();
        }
        self.metrics.read_size_bytes.record(read as u64);
        self.metrics.buffered_bytes.record(buffered as u64);
    }

    #[inline(always)]
    fn record_buffer_growth(&self, bytes: usize) {
        self.metrics.buffer_growths.inc();
        self.metrics
            .buffer_growth_bytes
            .add(saturating_isize(bytes));
    }

    #[inline(always)]
    fn record_split_prefix(&self, bytes: usize, copied: bool, buffered: usize) {
        self.metrics.split_prefixes.inc();
        self.metrics.split_prefix_bytes.add(saturating_isize(bytes));
        match copied {
            true => {
                self.metrics.copied_prefixes.inc();
                self.metrics
                    .copied_prefix_bytes
                    .add(saturating_isize(bytes));
            }
            false => {
                self.metrics.frozen_prefixes.inc();
                self.metrics
                    .frozen_prefix_bytes
                    .add(saturating_isize(bytes));
            }
        }
        self.metrics.buffered_bytes.record(buffered as u64);
    }

    #[inline(always)]
    fn record_mutable_prefix(&self, bytes: usize, buffered: usize) {
        self.metrics.mutable_prefixes.inc();
        self.metrics
            .mutable_prefix_bytes
            .add(saturating_isize(bytes));
        self.metrics.buffered_bytes.record(buffered as u64);
    }

    #[inline(always)]
    fn record_freeze_all(&self, bytes: usize) {
        self.metrics.freeze_all_calls.inc();
        self.metrics.freeze_all_bytes.add(saturating_isize(bytes));
        self.metrics.buffered_bytes.record(0);
    }

    #[inline(always)]
    fn record_advance(&self, bytes: usize, buffered: usize) {
        self.metrics.advances.inc();
        self.metrics.advanced_bytes.add(saturating_isize(bytes));
        self.metrics.buffered_bytes.record(buffered as u64);
    }

    #[inline(always)]
    fn record_tail(&self, bytes: usize) {
        self.metrics.tails_taken.inc();
        self.metrics.tail_bytes.add(saturating_isize(bytes));
        self.metrics.buffered_bytes.record(0);
    }

    #[inline(always)]
    fn record_buffered(&self, buffered: usize) {
        self.metrics.buffered_bytes.record(buffered as u64);
    }
}

#[derive(Debug, Clone)]
pub struct HandoffReadTelemetryHandle {
    inner: Arc<HandoffReadTelemetry>,
}

impl HandoffReadTelemetryHandle {
    #[inline(always)]
    pub fn from_arc(telemetry: &Arc<HandoffReadTelemetry>) -> Self {
        Self {
            inner: Arc::clone(telemetry),
        }
    }

    #[inline(always)]
    pub fn telemetry(&self) -> &HandoffReadTelemetry {
        self.inner.as_ref()
    }

    #[inline(always)]
    pub(crate) fn record_read(&self, read: usize, buffered: usize) {
        self.inner.record_read(read, buffered);
    }

    #[inline(always)]
    pub(crate) fn record_buffer_growth(&self, bytes: usize) {
        self.inner.record_buffer_growth(bytes);
    }

    #[inline(always)]
    pub(crate) fn record_split_prefix(&self, bytes: usize, copied: bool, buffered: usize) {
        self.inner.record_split_prefix(bytes, copied, buffered);
    }

    #[inline(always)]
    pub(crate) fn record_mutable_prefix(&self, bytes: usize, buffered: usize) {
        self.inner.record_mutable_prefix(bytes, buffered);
    }

    #[inline(always)]
    pub(crate) fn record_freeze_all(&self, bytes: usize) {
        self.inner.record_freeze_all(bytes);
    }

    #[inline(always)]
    pub(crate) fn record_advance(&self, bytes: usize, buffered: usize) {
        self.inner.record_advance(bytes, buffered);
    }

    #[inline(always)]
    pub(crate) fn record_tail(&self, bytes: usize) {
        self.inner.record_tail(bytes);
    }

    #[inline(always)]
    pub(crate) fn record_buffered(&self, buffered: usize) {
        self.inner.record_buffered(buffered);
    }
}

#[inline(always)]
fn counter_sum(counter: &Counter) -> u64 {
    counter.sum().max(0) as u64
}

#[inline(always)]
fn histogram_summary(histogram: &Histogram) -> HandoffReadHistogramSummary {
    HandoffReadHistogramSummary {
        count: histogram.count(),
        sum: histogram.sum(),
    }
}

#[inline(always)]
fn saturating_isize(value: usize) -> isize {
    value.min(isize::MAX as usize) as isize
}
