use std::sync::Arc;

#[cfg(feature = "telemetry-clickhouse")]
use fast_telemetry::ClickHouseExport;
#[cfg(feature = "telemetry-otlp")]
use fast_telemetry::OtlpExport;
use fast_telemetry::{
    Counter, CounterSet, DogStatsDExport, ExportMetrics, Histogram, MaxGauge, MetricKind,
    MetricLabels, MetricMeta, MetricScope, MetricVisitor, PrometheusExport, Runtime, RuntimeConfig,
};

pub const HANDOFF_READ_METRIC_SCOPE: &str = "bytes_handoff_read";

const SIZE_BUCKETS: &[u64] = &[
    0, 64, 128, 256, 512, 1_024, 2_048, 4_096, 8_192, 16_384, 32_768, 65_536, 131_072, 262_144,
    524_288, 1_048_576, 4_194_304, 16_777_216,
];

const READ_CALLS: usize = 0;
const READ_BYTES: usize = 1;
const READ_ERRORS: usize = 2;
const READ_ERROR_LIMIT_EXCEEDED: usize = 3;
const ZERO_READS: usize = 4;
const BUFFER_GROWTHS: usize = 5;
const BUFFER_GROWTH_BYTES: usize = 6;
const SPLIT_PREFIXES: usize = 7;
const SPLIT_PREFIX_BYTES: usize = 8;
const COPIED_PREFIXES: usize = 9;
const COPIED_PREFIX_BYTES: usize = 10;
const FROZEN_PREFIXES: usize = 11;
const FROZEN_PREFIX_BYTES: usize = 12;
const MUTABLE_PREFIXES: usize = 13;
const MUTABLE_PREFIX_BYTES: usize = 14;
const FREEZE_ALL_CALLS: usize = 15;
const FREEZE_ALL_BYTES: usize = 16;
const ADVANCES: usize = 17;
const ADVANCED_BYTES: usize = 18;
const TAILS_TAKEN: usize = 19;
const TAIL_BYTES: usize = 20;
const MONOIO_READ_BUFFER_SWAPS: usize = 21;
const MONOIO_READ_BUFFER_COPIES: usize = 22;
const READ_COUNTER_COUNT: usize = 23;
pub const DEFAULT_READ_COUNTER_BUFFER_FLUSH_EVERY: usize = 64;

const MAX_BUFFERED_BYTES_NAME: &str = "bytes_handoff_read_max_buffered_bytes";
const MAX_BUFFERED_BYTES_DOGSTATSD_NAME: &str = "bytes_handoff_read.max_buffered_bytes";
const MAX_BUFFERED_BYTES_HELP: &str = "Maximum buffered bytes observed by HandoffBuffer";
const READ_SIZE_BYTES_NAME: &str = "bytes_handoff_read_read_size_bytes";
const READ_SIZE_BYTES_DOGSTATSD_NAME: &str = "bytes_handoff_read.read_size_bytes";
const READ_SIZE_BYTES_HELP: &str = "Read sizes returned by HandoffBuffer";
const BUFFERED_BYTES_NAME: &str = "bytes_handoff_read_buffered_bytes";
const BUFFERED_BYTES_DOGSTATSD_NAME: &str = "bytes_handoff_read.buffered_bytes";
const BUFFERED_BYTES_HELP: &str = "Buffered byte counts observed after read and consume operations";

#[derive(Clone, Copy, Debug)]
struct CounterMetric {
    counter_idx: usize,
    name: &'static str,
    dogstatsd_name: &'static str,
    help: &'static str,
}

const COUNTER_METRICS: [CounterMetric; 23] = [
    CounterMetric {
        counter_idx: READ_CALLS,
        name: "bytes_handoff_read_read_calls",
        dogstatsd_name: "bytes_handoff_read.read_calls",
        help: "Total read attempts completed by HandoffBuffer",
    },
    CounterMetric {
        counter_idx: READ_BYTES,
        name: "bytes_handoff_read_read_bytes",
        dogstatsd_name: "bytes_handoff_read.read_bytes",
        help: "Total bytes read into HandoffBuffer",
    },
    CounterMetric {
        counter_idx: READ_ERRORS,
        name: "bytes_handoff_read_read_errors",
        dogstatsd_name: "bytes_handoff_read.read_errors",
        help: "Total read attempts that returned an error",
    },
    CounterMetric {
        counter_idx: READ_ERROR_LIMIT_EXCEEDED,
        name: "bytes_handoff_read_read_error_limit_exceeded",
        dogstatsd_name: "bytes_handoff_read.read_error_limit_exceeded",
        help: "Total read attempts rejected because the configured buffer limit was exceeded",
    },
    CounterMetric {
        counter_idx: ZERO_READS,
        name: "bytes_handoff_read_zero_reads",
        dogstatsd_name: "bytes_handoff_read.zero_reads",
        help: "Total reads that returned no bytes",
    },
    CounterMetric {
        counter_idx: BUFFER_GROWTHS,
        name: "bytes_handoff_read_buffer_growths",
        dogstatsd_name: "bytes_handoff_read.buffer_growths",
        help: "Total buffer growth events before reads",
    },
    CounterMetric {
        counter_idx: BUFFER_GROWTH_BYTES,
        name: "bytes_handoff_read_buffer_growth_bytes",
        dogstatsd_name: "bytes_handoff_read.buffer_growth_bytes",
        help: "Total spare capacity bytes reserved during buffer growth",
    },
    CounterMetric {
        counter_idx: SPLIT_PREFIXES,
        name: "bytes_handoff_read_split_prefixes",
        dogstatsd_name: "bytes_handoff_read.split_prefixes",
        help: "Total immutable prefix splits",
    },
    CounterMetric {
        counter_idx: SPLIT_PREFIX_BYTES,
        name: "bytes_handoff_read_split_prefix_bytes",
        dogstatsd_name: "bytes_handoff_read.split_prefix_bytes",
        help: "Total bytes emitted by immutable prefix splits",
    },
    CounterMetric {
        counter_idx: COPIED_PREFIXES,
        name: "bytes_handoff_read_copied_prefixes",
        dogstatsd_name: "bytes_handoff_read.copied_prefixes",
        help: "Total immutable prefix splits served by copying",
    },
    CounterMetric {
        counter_idx: COPIED_PREFIX_BYTES,
        name: "bytes_handoff_read_copied_prefix_bytes",
        dogstatsd_name: "bytes_handoff_read.copied_prefix_bytes",
        help: "Total bytes emitted by copied immutable prefix splits",
    },
    CounterMetric {
        counter_idx: FROZEN_PREFIXES,
        name: "bytes_handoff_read_frozen_prefixes",
        dogstatsd_name: "bytes_handoff_read.frozen_prefixes",
        help: "Total immutable prefix splits served by freezing",
    },
    CounterMetric {
        counter_idx: FROZEN_PREFIX_BYTES,
        name: "bytes_handoff_read_frozen_prefix_bytes",
        dogstatsd_name: "bytes_handoff_read.frozen_prefix_bytes",
        help: "Total bytes emitted by frozen immutable prefix splits",
    },
    CounterMetric {
        counter_idx: MUTABLE_PREFIXES,
        name: "bytes_handoff_read_mutable_prefixes",
        dogstatsd_name: "bytes_handoff_read.mutable_prefixes",
        help: "Total mutable prefix splits",
    },
    CounterMetric {
        counter_idx: MUTABLE_PREFIX_BYTES,
        name: "bytes_handoff_read_mutable_prefix_bytes",
        dogstatsd_name: "bytes_handoff_read.mutable_prefix_bytes",
        help: "Total bytes emitted by mutable prefix splits",
    },
    CounterMetric {
        counter_idx: FREEZE_ALL_CALLS,
        name: "bytes_handoff_read_freeze_all_calls",
        dogstatsd_name: "bytes_handoff_read.freeze_all_calls",
        help: "Total calls that freeze all buffered bytes",
    },
    CounterMetric {
        counter_idx: FREEZE_ALL_BYTES,
        name: "bytes_handoff_read_freeze_all_bytes",
        dogstatsd_name: "bytes_handoff_read.freeze_all_bytes",
        help: "Total bytes emitted by freeze_all",
    },
    CounterMetric {
        counter_idx: ADVANCES,
        name: "bytes_handoff_read_advances",
        dogstatsd_name: "bytes_handoff_read.advances",
        help: "Total buffer advance calls",
    },
    CounterMetric {
        counter_idx: ADVANCED_BYTES,
        name: "bytes_handoff_read_advanced_bytes",
        dogstatsd_name: "bytes_handoff_read.advanced_bytes",
        help: "Total bytes consumed by advance calls",
    },
    CounterMetric {
        counter_idx: TAILS_TAKEN,
        name: "bytes_handoff_read_tails_taken",
        dogstatsd_name: "bytes_handoff_read.tails_taken",
        help: "Total tail handoffs",
    },
    CounterMetric {
        counter_idx: TAIL_BYTES,
        name: "bytes_handoff_read_tail_bytes",
        dogstatsd_name: "bytes_handoff_read.tail_bytes",
        help: "Total bytes emitted by tail handoffs",
    },
    CounterMetric {
        counter_idx: MONOIO_READ_BUFFER_SWAPS,
        name: "bytes_handoff_read_monoio_read_buffer_swaps",
        dogstatsd_name: "bytes_handoff_read.monoio_read_buffer_swaps",
        help: "Total monoio reads stored by swapping the read buffer into place",
    },
    CounterMetric {
        counter_idx: MONOIO_READ_BUFFER_COPIES,
        name: "bytes_handoff_read_monoio_read_buffer_copies",
        dogstatsd_name: "bytes_handoff_read.monoio_read_buffer_copies",
        help: "Total monoio reads stored by copying from the read buffer",
    },
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
    pub read_errors: u64,
    pub read_error_limit_exceeded: u64,
    pub zero_reads: u64,
    pub buffer_growths: u64,
    pub buffer_growth_bytes: u64,
    pub max_buffered_bytes: u64,
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
    pub monoio_read_buffer_swaps: u64,
    pub monoio_read_buffer_copies: u64,
    pub read_size_bytes: HandoffReadHistogramSummary,
    pub buffered_bytes: HandoffReadHistogramSummary,
}

pub struct HandoffReadMetrics {
    direct_counters: [Counter; READ_COUNTER_COUNT],
    counters: CounterSet,
    pub max_buffered_bytes: MaxGauge,
    pub read_size_bytes: Histogram,
    pub buffered_bytes: Histogram,
}

/// Shared fast-telemetry runtime for bytes-handoff read metrics.
pub type HandoffReadTelemetryRuntime = Arc<Runtime>;

impl HandoffReadMetrics {
    pub fn new(metric_shards: usize) -> Self {
        let metric_shards = metric_shards.max(1);
        Self {
            direct_counters: std::array::from_fn(|_| Counter::new(metric_shards)),
            counters: CounterSet::new(metric_shards, READ_COUNTER_COUNT),
            max_buffered_bytes: MaxGauge::new(metric_shards),
            read_size_bytes: Histogram::new(SIZE_BUCKETS, metric_shards),
            buffered_bytes: Histogram::new(SIZE_BUCKETS, metric_shards),
        }
    }

    pub fn snapshot(&self) -> HandoffReadMetricsSnapshot {
        HandoffReadMetricsSnapshot {
            read_calls: self.counter_value(READ_CALLS),
            read_bytes: self.counter_value(READ_BYTES),
            read_errors: self.counter_value(READ_ERRORS),
            read_error_limit_exceeded: self.counter_value(READ_ERROR_LIMIT_EXCEEDED),
            zero_reads: self.counter_value(ZERO_READS),
            buffer_growths: self.counter_value(BUFFER_GROWTHS),
            buffer_growth_bytes: self.counter_value(BUFFER_GROWTH_BYTES),
            max_buffered_bytes: max_gauge_value(&self.max_buffered_bytes),
            split_prefixes: self.counter_value(SPLIT_PREFIXES),
            split_prefix_bytes: self.counter_value(SPLIT_PREFIX_BYTES),
            copied_prefixes: self.counter_value(COPIED_PREFIXES),
            copied_prefix_bytes: self.counter_value(COPIED_PREFIX_BYTES),
            frozen_prefixes: self.counter_value(FROZEN_PREFIXES),
            frozen_prefix_bytes: self.counter_value(FROZEN_PREFIX_BYTES),
            mutable_prefixes: self.counter_value(MUTABLE_PREFIXES),
            mutable_prefix_bytes: self.counter_value(MUTABLE_PREFIX_BYTES),
            freeze_all_calls: self.counter_value(FREEZE_ALL_CALLS),
            freeze_all_bytes: self.counter_value(FREEZE_ALL_BYTES),
            advances: self.counter_value(ADVANCES),
            advanced_bytes: self.counter_value(ADVANCED_BYTES),
            tails_taken: self.counter_value(TAILS_TAKEN),
            tail_bytes: self.counter_value(TAIL_BYTES),
            monoio_read_buffer_swaps: self.counter_value(MONOIO_READ_BUFFER_SWAPS),
            monoio_read_buffer_copies: self.counter_value(MONOIO_READ_BUFFER_COPIES),
            read_size_bytes: histogram_summary(&self.read_size_bytes),
            buffered_bytes: histogram_summary(&self.buffered_bytes),
        }
    }

    pub fn visit_metrics<V: MetricVisitor + ?Sized>(&self, visitor: &mut V) {
        <Self as ExportMetrics>::visit_metrics(self, visitor);
    }

    pub fn export_prometheus(&self, output: &mut String) {
        for metric in &COUNTER_METRICS {
            write_counter_prometheus(
                output,
                metric.name,
                metric.help,
                self.counter_value(metric.counter_idx),
            );
        }
        self.max_buffered_bytes.export_prometheus(
            output,
            MAX_BUFFERED_BYTES_NAME,
            MAX_BUFFERED_BYTES_HELP,
        );
        self.read_size_bytes
            .export_prometheus(output, READ_SIZE_BYTES_NAME, READ_SIZE_BYTES_HELP);
        self.buffered_bytes
            .export_prometheus(output, BUFFERED_BYTES_NAME, BUFFERED_BYTES_HELP);
    }

    pub fn export_dogstatsd(&self, output: &mut String, tags: &[(&str, &str)]) {
        for metric in &COUNTER_METRICS {
            write_counter_dogstatsd(
                output,
                metric.dogstatsd_name,
                self.counter_value(metric.counter_idx),
                tags,
            );
        }
        self.max_buffered_bytes
            .export_dogstatsd(output, MAX_BUFFERED_BYTES_DOGSTATSD_NAME, tags);
        write_histogram_dogstatsd(
            output,
            READ_SIZE_BYTES_DOGSTATSD_NAME,
            self.read_size_bytes.count(),
            self.read_size_bytes.sum(),
            tags,
        );
        write_histogram_dogstatsd(
            output,
            BUFFERED_BYTES_DOGSTATSD_NAME,
            self.buffered_bytes.count(),
            self.buffered_bytes.sum(),
            tags,
        );
    }

    pub fn export_dogstatsd_delta(
        &self,
        output: &mut String,
        tags: &[(&str, &str)],
        state: &mut HandoffReadMetricsDogStatsDState,
    ) {
        for metric in &COUNTER_METRICS {
            let current = self.counter_value(metric.counter_idx);
            let delta = current.saturating_sub(state.counters[metric.counter_idx]);
            state.counters[metric.counter_idx] = current;
            write_counter_dogstatsd(output, metric.dogstatsd_name, delta, tags);
        }

        self.max_buffered_bytes
            .export_dogstatsd(output, MAX_BUFFERED_BYTES_DOGSTATSD_NAME, tags);

        let read_size_count = self.read_size_bytes.count();
        let read_size_sum = self.read_size_bytes.sum();
        write_histogram_dogstatsd(
            output,
            READ_SIZE_BYTES_DOGSTATSD_NAME,
            read_size_count.saturating_sub(state.read_size_count),
            read_size_sum.saturating_sub(state.read_size_sum),
            tags,
        );
        state.read_size_count = read_size_count;
        state.read_size_sum = read_size_sum;

        let buffered_count = self.buffered_bytes.count();
        let buffered_sum = self.buffered_bytes.sum();
        write_histogram_dogstatsd(
            output,
            BUFFERED_BYTES_DOGSTATSD_NAME,
            buffered_count.saturating_sub(state.buffered_count),
            buffered_sum.saturating_sub(state.buffered_sum),
            tags,
        );
        state.buffered_count = buffered_count;
        state.buffered_sum = buffered_sum;
    }

    pub fn export_dogstatsd_with_temporality(
        &self,
        output: &mut String,
        tags: &[(&str, &str)],
        temporality: fast_telemetry::Temporality,
        state: &mut HandoffReadMetricsDogStatsDState,
    ) {
        match temporality {
            fast_telemetry::Temporality::Cumulative => self.export_dogstatsd(output, tags),
            fast_telemetry::Temporality::Delta => self.export_dogstatsd_delta(output, tags, state),
        }
    }

    #[cfg(feature = "telemetry-otlp")]
    pub fn export_otlp(
        &self,
        metrics: &mut Vec<fast_telemetry::otlp::pb::Metric>,
        time_unix_nano: u64,
    ) {
        for metric in &COUNTER_METRICS {
            counter_from_value(self.counter_value(metric.counter_idx)).export_otlp(
                metrics,
                metric.name,
                metric.help,
                time_unix_nano,
            );
        }
        self.max_buffered_bytes.export_otlp(
            metrics,
            MAX_BUFFERED_BYTES_NAME,
            MAX_BUFFERED_BYTES_HELP,
            time_unix_nano,
        );
        self.read_size_bytes.export_otlp(
            metrics,
            READ_SIZE_BYTES_NAME,
            READ_SIZE_BYTES_HELP,
            time_unix_nano,
        );
        self.buffered_bytes.export_otlp(
            metrics,
            BUFFERED_BYTES_NAME,
            BUFFERED_BYTES_HELP,
            time_unix_nano,
        );
    }

    #[cfg(feature = "telemetry-clickhouse")]
    pub fn export_clickhouse(
        &self,
        batch: &mut fast_telemetry::clickhouse::ClickHouseMetricBatch,
        time_unix_nano: u64,
    ) {
        for metric in &COUNTER_METRICS {
            counter_from_value(self.counter_value(metric.counter_idx)).export_clickhouse(
                batch,
                metric.name,
                metric.help,
                time_unix_nano,
            );
        }
        self.max_buffered_bytes.export_clickhouse(
            batch,
            MAX_BUFFERED_BYTES_NAME,
            MAX_BUFFERED_BYTES_HELP,
            time_unix_nano,
        );
        self.read_size_bytes.export_clickhouse(
            batch,
            READ_SIZE_BYTES_NAME,
            READ_SIZE_BYTES_HELP,
            time_unix_nano,
        );
        self.buffered_bytes.export_clickhouse(
            batch,
            BUFFERED_BYTES_NAME,
            BUFFERED_BYTES_HELP,
            time_unix_nano,
        );
    }

    #[inline(always)]
    fn counter_value(&self, counter_idx: usize) -> u64 {
        let total = self.direct_counters[counter_idx].sum() + self.counters.sum(counter_idx);
        total.max(0) as u64
    }
}

impl ExportMetrics for HandoffReadMetrics {
    fn visit_metrics<V: MetricVisitor + ?Sized>(&self, visitor: &mut V) {
        for metric in &COUNTER_METRICS {
            visitor.counter(
                MetricMeta {
                    name: metric.name,
                    help: metric.help,
                    kind: MetricKind::Counter,
                    unit: None,
                },
                MetricLabels::none(),
                saturating_i64_from_u64(self.counter_value(metric.counter_idx)),
            );
        }
        visitor.gauge_i64(
            MetricMeta {
                name: MAX_BUFFERED_BYTES_NAME,
                help: MAX_BUFFERED_BYTES_HELP,
                kind: MetricKind::Gauge,
                unit: None,
            },
            MetricLabels::none(),
            self.max_buffered_bytes.get(),
        );
        visitor.histogram(
            MetricMeta {
                name: READ_SIZE_BYTES_NAME,
                help: READ_SIZE_BYTES_HELP,
                kind: MetricKind::Histogram,
                unit: None,
            },
            MetricLabels::none(),
            &self.read_size_bytes,
        );
        visitor.histogram(
            MetricMeta {
                name: BUFFERED_BYTES_NAME,
                help: BUFFERED_BYTES_HELP,
                kind: MetricKind::Histogram,
                unit: None,
            },
            MetricLabels::none(),
            &self.buffered_bytes,
        );
    }
}

#[derive(Clone, Debug, Default)]
pub struct HandoffReadMetricsDogStatsDState {
    counters: [u64; READ_COUNTER_COUNT],
    read_size_count: u64,
    read_size_sum: u64,
    buffered_count: u64,
    buffered_sum: u64,
}

impl HandoffReadMetricsDogStatsDState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn tracked_label_sets(&self) -> usize {
        0
    }
}

#[derive(Debug)]
struct HandoffReadCounterBuffer {
    deltas: [isize; READ_COUNTER_COUNT],
    ops_since_flush: usize,
    flush_every: usize,
    dirty: bool,
}

impl Default for HandoffReadCounterBuffer {
    fn default() -> Self {
        Self::new(DEFAULT_READ_COUNTER_BUFFER_FLUSH_EVERY)
    }
}

impl HandoffReadCounterBuffer {
    fn new(flush_every: usize) -> Self {
        assert!(flush_every >= 1, "flush_every must be >= 1");
        Self {
            deltas: [0; READ_COUNTER_COUNT],
            ops_since_flush: 0,
            flush_every,
            dirty: false,
        }
    }

    fn empty_like(&self) -> Self {
        Self::new(self.flush_every)
    }

    #[inline(always)]
    fn inc(&mut self, counter_idx: usize) {
        self.add(counter_idx, 1);
    }

    #[inline(always)]
    fn add(&mut self, counter_idx: usize, value: isize) {
        debug_assert!(counter_idx < self.deltas.len());
        let delta = &mut self.deltas[counter_idx];
        *delta = delta.saturating_add(value);
        self.dirty = true;
    }

    #[inline(always)]
    fn add_pair(&mut self, first_idx: usize, first: isize, second_idx: usize, second: isize) {
        self.add(first_idx, first);
        self.add(second_idx, second);
    }

    #[inline(always)]
    fn add_quad(&mut self, updates: [(usize, isize); 4]) {
        for (counter_idx, value) in updates {
            self.add(counter_idx, value);
        }
    }

    #[inline(always)]
    fn finish_op(&mut self) -> bool {
        if !self.dirty {
            return false;
        }

        self.dirty = false;
        self.ops_since_flush += 1;
        self.ops_since_flush >= self.flush_every
    }

    #[inline]
    fn flush_into(&mut self, metrics: &HandoffReadMetrics) {
        if self.ops_since_flush == 0 && !self.dirty {
            return;
        }

        let mut updates = [(0_usize, 0_isize); READ_COUNTER_COUNT];
        let mut update_count = 0;
        for (counter_idx, delta) in self.deltas.iter_mut().enumerate() {
            if *delta == 0 {
                continue;
            }
            updates[update_count] = (counter_idx, *delta);
            update_count += 1;
            *delta = 0;
        }
        if update_count > 0 {
            metrics.counters.add_index_values(&updates[..update_count]);
        }
        self.ops_since_flush = 0;
        self.dirty = false;
    }
}

pub struct HandoffReadTelemetry {
    runtime: Option<HandoffReadTelemetryRuntime>,
    metrics: Arc<HandoffReadMetrics>,
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
        let runtime = Runtime::new(RuntimeConfig::default());
        Self::from_runtime_with_shards(runtime, metric_shards)
    }

    pub fn with_available_parallelism() -> Arc<Self> {
        let runtime = Runtime::new(RuntimeConfig::default());
        Self::from_runtime(runtime)
    }

    pub fn metric_scope() -> MetricScope {
        MetricScope::from(HANDOFF_READ_METRIC_SCOPE)
    }

    pub fn from_metrics(metrics: HandoffReadMetrics) -> Arc<Self> {
        let runtime = Runtime::new(RuntimeConfig::default());
        Self::from_runtime_and_metrics(runtime, metrics)
    }

    pub fn from_shared_metrics(metrics: Arc<HandoffReadMetrics>) -> Arc<Self> {
        Arc::new(Self {
            runtime: None,
            metrics,
        })
    }

    /// Register a bytes-handoff read metric group on `runtime`.
    ///
    /// Recording uses the returned direct metric handles. Parent applications
    /// can export the registered group by visiting the shared runtime.
    pub fn from_runtime(runtime: HandoffReadTelemetryRuntime) -> Arc<Self> {
        let metric_shards = std::thread::available_parallelism()
            .map(|value| value.get())
            .unwrap_or(1);
        Self::from_runtime_with_shards(runtime, metric_shards)
    }

    /// Register a bytes-handoff read metric group on `runtime` with an explicit
    /// metric shard count.
    pub fn from_runtime_with_shards(
        runtime: HandoffReadTelemetryRuntime,
        metric_shards: usize,
    ) -> Arc<Self> {
        Self::from_runtime_and_metrics(runtime, HandoffReadMetrics::new(metric_shards))
    }

    /// Register an already constructed bytes-handoff read metric group on
    /// `runtime`.
    pub fn from_runtime_and_metrics(
        runtime: HandoffReadTelemetryRuntime,
        metrics: HandoffReadMetrics,
    ) -> Arc<Self> {
        let metrics = runtime
            .register_metrics(Self::metric_scope(), metrics)
            .into_metrics();
        Arc::new(Self {
            runtime: Some(runtime),
            metrics,
        })
    }

    /// Use the parent fast-telemetry runtime when provided, or create a local
    /// runtime with available parallelism when `runtime` is `None`.
    pub fn from_optional_runtime(runtime: Option<HandoffReadTelemetryRuntime>) -> Arc<Self> {
        match runtime {
            Some(runtime) => Self::from_runtime(runtime),
            None => Self::with_available_parallelism(),
        }
    }

    pub fn from_optional_shared_metrics(metrics: Option<Arc<HandoffReadMetrics>>) -> Arc<Self> {
        match metrics {
            Some(metrics) => Self::from_shared_metrics(metrics),
            None => Self::with_available_parallelism(),
        }
    }

    #[inline(always)]
    pub fn metrics(&self) -> &HandoffReadMetrics {
        self.metrics.as_ref()
    }

    #[inline(always)]
    pub fn runtime(&self) -> Option<&HandoffReadTelemetryRuntime> {
        self.runtime.as_ref()
    }

    #[inline(always)]
    pub fn shared_runtime(&self) -> Option<HandoffReadTelemetryRuntime> {
        self.runtime.clone()
    }

    #[inline(always)]
    pub fn shared_metrics(&self) -> Arc<HandoffReadMetrics> {
        Arc::clone(&self.metrics)
    }

    pub fn visit_metrics(&self, visitor: &mut dyn MetricVisitor) {
        self.metrics.visit_metrics(visitor);
    }

    pub fn export_prometheus(&self) -> String {
        let mut output = String::new();
        self.metrics.export_prometheus(&mut output);
        output
    }

    pub fn export_prometheus_into(&self, output: &mut String) {
        self.metrics.export_prometheus(output);
    }

    pub fn export_dogstatsd(&self, output: &mut String, tags: &[(&str, &str)]) {
        self.metrics.export_dogstatsd(output, tags);
    }

    pub fn export_dogstatsd_delta(
        &self,
        output: &mut String,
        tags: &[(&str, &str)],
        state: &mut HandoffReadMetricsDogStatsDState,
    ) {
        self.metrics.export_dogstatsd_delta(output, tags, state);
    }

    pub fn export_dogstatsd_with_temporality(
        &self,
        output: &mut String,
        tags: &[(&str, &str)],
        temporality: fast_telemetry::Temporality,
        state: &mut HandoffReadMetricsDogStatsDState,
    ) {
        self.metrics
            .export_dogstatsd_with_temporality(output, tags, temporality, state);
    }

    #[cfg(feature = "telemetry-otlp")]
    pub fn export_otlp(
        &self,
        metrics: &mut Vec<fast_telemetry::otlp::pb::Metric>,
        time_unix_nano: u64,
    ) {
        self.metrics.export_otlp(metrics, time_unix_nano);
    }

    #[cfg(feature = "telemetry-clickhouse")]
    pub fn export_clickhouse(
        &self,
        batch: &mut fast_telemetry::clickhouse::ClickHouseMetricBatch,
        time_unix_nano: u64,
    ) {
        self.metrics.export_clickhouse(batch, time_unix_nano);
    }

    pub fn snapshot(&self) -> HandoffReadMetricsSnapshot {
        self.metrics.snapshot()
    }

    #[cfg(test)]
    #[allow(dead_code)]
    #[inline(always)]
    fn record_read(&self, read: usize, buffered: usize) {
        if read == 0 {
            let updates = [(READ_CALLS, 1), (ZERO_READS, 1)];
            self.metrics.counters.add_index_values(&updates);
        } else {
            let updates = [(READ_CALLS, 1), (READ_BYTES, saturating_isize(read))];
            self.metrics.counters.add_index_values(&updates);
        }
        self.metrics.read_size_bytes.record(read as u64);
        self.record_buffered(buffered);
    }

    #[cfg(test)]
    #[allow(dead_code)]
    #[inline(always)]
    fn record_read_error(&self, limit_exceeded: bool) {
        if limit_exceeded {
            let updates = [READ_ERRORS, READ_ERROR_LIMIT_EXCEEDED];
            self.metrics.counters.add_indices(&updates, 1);
        } else {
            self.metrics.counters.inc(READ_ERRORS);
        }
    }

    #[cfg(test)]
    #[allow(dead_code)]
    #[inline(always)]
    fn record_buffer_growth(&self, bytes: usize) {
        let updates = [
            (BUFFER_GROWTHS, 1),
            (BUFFER_GROWTH_BYTES, saturating_isize(bytes)),
        ];
        self.metrics.counters.add_index_values(&updates);
    }

    #[cfg(test)]
    #[allow(dead_code)]
    #[inline(always)]
    fn record_split_prefix(&self, bytes: usize, copied: bool, buffered: usize) {
        let bytes = saturating_isize(bytes);
        match copied {
            true => {
                let updates = [
                    (SPLIT_PREFIXES, 1),
                    (SPLIT_PREFIX_BYTES, bytes),
                    (COPIED_PREFIXES, 1),
                    (COPIED_PREFIX_BYTES, bytes),
                ];
                self.metrics.counters.add_index_values(&updates);
            }
            false => {
                let updates = [
                    (SPLIT_PREFIXES, 1),
                    (SPLIT_PREFIX_BYTES, bytes),
                    (FROZEN_PREFIXES, 1),
                    (FROZEN_PREFIX_BYTES, bytes),
                ];
                self.metrics.counters.add_index_values(&updates);
            }
        }
        self.record_buffered(buffered);
    }

    #[cfg(test)]
    #[allow(dead_code)]
    #[inline(always)]
    fn record_mutable_prefix(&self, bytes: usize, buffered: usize) {
        let updates = [
            (MUTABLE_PREFIXES, 1),
            (MUTABLE_PREFIX_BYTES, saturating_isize(bytes)),
        ];
        self.metrics.counters.add_index_values(&updates);
        self.record_buffered(buffered);
    }

    #[cfg(test)]
    #[allow(dead_code)]
    #[inline(always)]
    fn record_freeze_all(&self, bytes: usize) {
        let updates = [
            (FREEZE_ALL_CALLS, 1),
            (FREEZE_ALL_BYTES, saturating_isize(bytes)),
        ];
        self.metrics.counters.add_index_values(&updates);
        self.record_buffered(0);
    }

    #[cfg(test)]
    #[allow(dead_code)]
    #[inline(always)]
    fn record_advance(&self, bytes: usize, buffered: usize) {
        let updates = [(ADVANCES, 1), (ADVANCED_BYTES, saturating_isize(bytes))];
        self.metrics.counters.add_index_values(&updates);
        self.record_buffered(buffered);
    }

    #[cfg(test)]
    #[allow(dead_code)]
    #[inline(always)]
    fn record_tail(&self, bytes: usize) {
        let updates = [(TAILS_TAKEN, 1), (TAIL_BYTES, saturating_isize(bytes))];
        self.metrics.counters.add_index_values(&updates);
        self.record_buffered(0);
    }

    #[cfg(test)]
    #[allow(dead_code)]
    #[inline(always)]
    #[cfg(feature = "monoio")]
    fn record_monoio_read_buffer_swap(&self) {
        self.metrics.counters.inc(MONOIO_READ_BUFFER_SWAPS);
    }

    #[cfg(test)]
    #[allow(dead_code)]
    #[inline(always)]
    #[cfg(feature = "monoio")]
    fn record_monoio_read_buffer_copy(&self) {
        self.metrics.counters.inc(MONOIO_READ_BUFFER_COPIES);
    }

    #[inline(always)]
    fn record_buffered(&self, buffered: usize) {
        self.metrics.buffered_bytes.record(buffered as u64);
        self.metrics
            .max_buffered_bytes
            .observe(saturating_i64(buffered));
    }
}

#[derive(Debug)]
pub struct HandoffReadTelemetryHandle {
    inner: Arc<HandoffReadTelemetry>,
    counter_buffer: Option<HandoffReadCounterBuffer>,
}

impl Clone for HandoffReadTelemetryHandle {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            counter_buffer: self
                .counter_buffer
                .as_ref()
                .map(HandoffReadCounterBuffer::empty_like),
        }
    }
}

impl Drop for HandoffReadTelemetryHandle {
    fn drop(&mut self) {
        self.flush_counter_buffer();
    }
}

impl HandoffReadTelemetryHandle {
    #[inline(always)]
    pub fn from_arc(telemetry: &Arc<HandoffReadTelemetry>) -> Self {
        Self {
            inner: Arc::clone(telemetry),
            counter_buffer: Some(HandoffReadCounterBuffer::default()),
        }
    }

    pub fn from_metrics(metrics: HandoffReadMetrics) -> Self {
        Self::from_arc(&HandoffReadTelemetry::from_metrics(metrics))
    }

    pub fn from_shared_metrics(metrics: Arc<HandoffReadMetrics>) -> Self {
        Self::from_arc(&HandoffReadTelemetry::from_shared_metrics(metrics))
    }

    pub fn from_runtime(runtime: HandoffReadTelemetryRuntime) -> Self {
        Self::from_arc(&HandoffReadTelemetry::from_runtime(runtime))
    }

    pub fn from_optional_runtime(runtime: Option<HandoffReadTelemetryRuntime>) -> Self {
        Self::from_arc(&HandoffReadTelemetry::from_optional_runtime(runtime))
    }

    pub fn from_optional_shared_metrics(metrics: Option<Arc<HandoffReadMetrics>>) -> Self {
        Self::from_arc(&HandoffReadTelemetry::from_optional_shared_metrics(metrics))
    }

    #[inline(always)]
    pub fn telemetry(&self) -> &HandoffReadTelemetry {
        self.inner.as_ref()
    }

    pub fn flush_counter_buffer(&mut self) {
        if let Some(counter_buffer) = &mut self.counter_buffer {
            counter_buffer.flush_into(self.inner.metrics());
        }
    }

    pub fn with_counter_flush_every(mut self, flush_every: usize) -> Self {
        self.flush_counter_buffer();
        self.counter_buffer = Some(HandoffReadCounterBuffer::new(flush_every));
        self
    }

    pub fn with_direct_counters(mut self) -> Self {
        self.flush_counter_buffer();
        self.counter_buffer = None;
        self
    }

    #[inline(always)]
    fn record_counter_inc(&mut self, counter_idx: usize) {
        let metrics = self.inner.metrics();
        match &mut self.counter_buffer {
            Some(counter_buffer) => {
                counter_buffer.inc(counter_idx);
                if counter_buffer.finish_op() {
                    counter_buffer.flush_into(metrics);
                }
            }
            None => metrics.direct_counters[counter_idx].inc(),
        }
    }

    #[inline(always)]
    fn record_counter_pair(
        &mut self,
        first_idx: usize,
        first: isize,
        second_idx: usize,
        second: isize,
    ) {
        let metrics = self.inner.metrics();
        match &mut self.counter_buffer {
            Some(counter_buffer) => {
                counter_buffer.add_pair(first_idx, first, second_idx, second);
                if counter_buffer.finish_op() {
                    counter_buffer.flush_into(metrics);
                }
            }
            None => {
                metrics.direct_counters[first_idx].add(first);
                metrics.direct_counters[second_idx].add(second);
            }
        }
    }

    #[inline(always)]
    fn record_counter_quad(&mut self, updates: [(usize, isize); 4]) {
        let metrics = self.inner.metrics();
        match &mut self.counter_buffer {
            Some(counter_buffer) => {
                counter_buffer.add_quad(updates);
                if counter_buffer.finish_op() {
                    counter_buffer.flush_into(metrics);
                }
            }
            None => {
                for (counter_idx, value) in updates {
                    metrics.direct_counters[counter_idx].add(value);
                }
            }
        }
    }

    #[inline(always)]
    pub(crate) fn record_read(&mut self, read: usize, buffered: usize) {
        if read == 0 {
            self.record_counter_pair(READ_CALLS, 1, ZERO_READS, 1);
        } else {
            self.record_counter_pair(READ_CALLS, 1, READ_BYTES, saturating_isize(read));
        }
        self.inner.metrics.read_size_bytes.record(read as u64);
        self.inner.record_buffered(buffered);
    }

    #[inline(always)]
    pub(crate) fn record_read_error(&mut self, limit_exceeded: bool) {
        if limit_exceeded {
            self.record_counter_pair(READ_ERRORS, 1, READ_ERROR_LIMIT_EXCEEDED, 1);
        } else {
            self.record_counter_inc(READ_ERRORS);
        }
    }

    #[inline(always)]
    pub(crate) fn record_buffer_growth(&mut self, bytes: usize) {
        self.record_counter_pair(
            BUFFER_GROWTHS,
            1,
            BUFFER_GROWTH_BYTES,
            saturating_isize(bytes),
        );
    }

    #[inline(always)]
    pub(crate) fn record_split_prefix(&mut self, bytes: usize, copied: bool, buffered: usize) {
        let bytes = saturating_isize(bytes);
        match copied {
            true => {
                self.record_counter_quad([
                    (SPLIT_PREFIXES, 1),
                    (SPLIT_PREFIX_BYTES, bytes),
                    (COPIED_PREFIXES, 1),
                    (COPIED_PREFIX_BYTES, bytes),
                ]);
            }
            false => {
                self.record_counter_quad([
                    (SPLIT_PREFIXES, 1),
                    (SPLIT_PREFIX_BYTES, bytes),
                    (FROZEN_PREFIXES, 1),
                    (FROZEN_PREFIX_BYTES, bytes),
                ]);
            }
        }
        self.inner.record_buffered(buffered);
    }

    #[inline(always)]
    pub(crate) fn record_mutable_prefix(&mut self, bytes: usize, buffered: usize) {
        self.record_counter_pair(
            MUTABLE_PREFIXES,
            1,
            MUTABLE_PREFIX_BYTES,
            saturating_isize(bytes),
        );
        self.inner.record_buffered(buffered);
    }

    #[inline(always)]
    pub(crate) fn record_freeze_all(&mut self, bytes: usize) {
        self.record_counter_pair(
            FREEZE_ALL_CALLS,
            1,
            FREEZE_ALL_BYTES,
            saturating_isize(bytes),
        );
        self.inner.record_buffered(0);
    }

    #[inline(always)]
    pub(crate) fn record_advance(&mut self, bytes: usize, buffered: usize) {
        self.record_counter_pair(ADVANCES, 1, ADVANCED_BYTES, saturating_isize(bytes));
        self.inner.record_buffered(buffered);
    }

    #[inline(always)]
    pub(crate) fn record_tail(&mut self, bytes: usize) {
        self.record_counter_pair(TAILS_TAKEN, 1, TAIL_BYTES, saturating_isize(bytes));
        self.inner.record_buffered(0);
    }

    #[inline(always)]
    #[cfg(feature = "monoio")]
    pub(crate) fn record_monoio_read_buffer_swap(&mut self) {
        self.record_counter_inc(MONOIO_READ_BUFFER_SWAPS);
    }

    #[inline(always)]
    #[cfg(feature = "monoio")]
    pub(crate) fn record_monoio_read_buffer_copy(&mut self) {
        self.record_counter_inc(MONOIO_READ_BUFFER_COPIES);
    }

    #[inline(always)]
    pub(crate) fn record_buffered(&mut self, buffered: usize) {
        self.inner.record_buffered(buffered);
    }
}

#[inline(always)]
fn max_gauge_value(gauge: &MaxGauge) -> u64 {
    gauge.get().max(0) as u64
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

#[inline(always)]
fn saturating_i64(value: usize) -> i64 {
    value.min(i64::MAX as usize) as i64
}

#[inline(always)]
fn saturating_i64_from_u64(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}

#[inline]
fn write_counter_prometheus(output: &mut String, name: &str, help: &str, value: u64) {
    output.push_str("# HELP ");
    output.push_str(name);
    output.push(' ');
    output.push_str(help);
    output.push_str("\n# TYPE ");
    output.push_str(name);
    output.push_str(" counter\n");
    output.push_str(name);
    output.push(' ');
    output.push_str(&value.to_string());
    output.push('\n');
}

#[inline]
fn write_counter_dogstatsd(output: &mut String, name: &str, value: u64, tags: &[(&str, &str)]) {
    output.push_str(name);
    output.push(':');
    output.push_str(&value.to_string());
    output.push_str("|c");
    append_dogstatsd_tags(output, tags);
    output.push('\n');
}

#[inline]
fn write_histogram_dogstatsd(
    output: &mut String,
    name: &str,
    count: u64,
    sum: u64,
    tags: &[(&str, &str)],
) {
    output.push_str(name);
    output.push_str(".count:");
    output.push_str(&count.to_string());
    output.push_str("|c");
    append_dogstatsd_tags(output, tags);
    output.push('\n');

    output.push_str(name);
    output.push_str(".sum:");
    output.push_str(&sum.to_string());
    output.push_str("|c");
    append_dogstatsd_tags(output, tags);
    output.push('\n');
}

#[inline]
fn append_dogstatsd_tags(output: &mut String, tags: &[(&str, &str)]) {
    if tags.is_empty() {
        return;
    }

    output.push_str("|#");
    for (idx, (name, value)) in tags.iter().enumerate() {
        if idx > 0 {
            output.push(',');
        }
        output.push_str(name);
        output.push(':');
        output.push_str(value);
    }
}

#[cfg(any(feature = "telemetry-otlp", feature = "telemetry-clickhouse"))]
fn counter_from_value(value: u64) -> Counter {
    let counter = Counter::new(1);
    counter.add(value.min(isize::MAX as u64) as isize);
    counter
}

#[cfg(test)]
mod tests {
    use super::*;

    fn telemetry_with_read_activity() -> Arc<HandoffReadTelemetry> {
        let telemetry = HandoffReadTelemetry::new(1);
        telemetry.record_read(64, 64);
        telemetry.record_split_prefix(16, true, 48);
        telemetry.record_freeze_all(48);
        telemetry
    }

    #[test]
    fn records_into_caller_owned_metrics() {
        let metrics = Arc::new(HandoffReadMetrics::new(1));
        let telemetry = HandoffReadTelemetry::from_shared_metrics(Arc::clone(&metrics));

        telemetry.record_read(64, 64);
        telemetry.record_split_prefix(16, false, 48);
        telemetry.record_advance(8, 40);

        assert!(Arc::ptr_eq(&metrics, &telemetry.shared_metrics()));

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.read_calls, 1);
        assert_eq!(snapshot.read_bytes, 64);
        assert_eq!(snapshot.split_prefixes, 1);
        assert_eq!(snapshot.frozen_prefixes, 1);
        assert_eq!(snapshot.advances, 1);
        assert_eq!(snapshot.advanced_bytes, 8);

        let mut prometheus = String::new();
        metrics.export_prometheus(&mut prometheus);
        assert!(prometheus.contains("bytes_handoff_read_read_calls 1"));
    }

    #[test]
    fn grouped_default_and_direct_handles_share_snapshot_counters() {
        let telemetry = HandoffReadTelemetry::new(1);
        let mut grouped = HandoffReadTelemetryHandle::from_arc(&telemetry);
        let mut direct = HandoffReadTelemetryHandle::from_arc(&telemetry).with_direct_counters();

        grouped.record_read(20, 20);
        direct.record_read(10, 10);

        assert_eq!(telemetry.snapshot().read_calls, 1);
        assert_eq!(telemetry.snapshot().read_size_bytes.count, 2);
        grouped.flush_counter_buffer();

        let snapshot = telemetry.snapshot();
        assert_eq!(snapshot.read_calls, 2);
        assert_eq!(snapshot.read_bytes, 30);
        assert_eq!(snapshot.read_size_bytes.count, 2);
    }

    #[test]
    fn optional_runtime_uses_parent_or_creates_local_metrics() {
        let parent_runtime = Runtime::new(RuntimeConfig::default());
        let parent_telemetry =
            HandoffReadTelemetry::from_optional_runtime(Some(Arc::clone(&parent_runtime)));

        assert!(
            parent_telemetry
                .runtime()
                .is_some_and(|runtime| Arc::ptr_eq(runtime, &parent_runtime))
        );
        assert_eq!(parent_runtime.registered_metrics_len(), 1);
        assert_eq!(
            parent_runtime.scopes(),
            vec![HandoffReadTelemetry::metric_scope()]
        );
        parent_telemetry.record_read(32, 32);
        assert_eq!(parent_telemetry.snapshot().read_bytes, 32);

        let local_telemetry = HandoffReadTelemetry::from_optional_runtime(None);
        assert!(local_telemetry.runtime().is_some());
        assert!(!Arc::ptr_eq(
            &parent_telemetry.shared_metrics(),
            &local_telemetry.shared_metrics()
        ));
        local_telemetry.record_read(16, 16);
        assert_eq!(local_telemetry.snapshot().read_bytes, 16);
    }

    #[test]
    fn exports_dogstatsd_cumulative_and_delta() {
        let telemetry = telemetry_with_read_activity();
        let mut output = String::new();

        telemetry.export_dogstatsd(&mut output, &[("component", "bytes_handoff")]);

        assert!(output.contains("bytes_handoff_read.read_calls"));
        assert!(output.contains("bytes_handoff_read.read_bytes"));
        assert!(output.contains("component:bytes_handoff"));

        output.clear();
        let mut state = HandoffReadMetricsDogStatsDState::new();
        telemetry.export_dogstatsd_delta(&mut output, &[], &mut state);

        assert!(output.contains("bytes_handoff_read.read_calls"));
        assert_eq!(state.tracked_label_sets(), 0);
    }

    #[cfg(feature = "telemetry-otlp")]
    #[test]
    fn exports_otlp_metrics() {
        let telemetry = telemetry_with_read_activity();
        let mut metrics = Vec::new();

        telemetry.export_otlp(&mut metrics, 123);

        assert!(!metrics.is_empty());
    }

    #[cfg(feature = "telemetry-clickhouse")]
    #[test]
    fn exports_clickhouse_rows() {
        let telemetry = telemetry_with_read_activity();
        let mut batch = fast_telemetry::clickhouse::ClickHouseMetricBatch::new("bytes-handoff");

        telemetry.export_clickhouse(&mut batch, 123);

        assert!(batch.total_rows() > 0);
    }
}
