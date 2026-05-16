use std::collections::BTreeSet;

use thiserror::Error;

pub const DEFAULT_TUNING_MIN_THRESHOLD_BYTES: usize = 1;
pub const DEFAULT_TUNING_MAX_THRESHOLD_BYTES: usize = 16 * 1024;
pub const DEFAULT_TUNING_MIN_THRESHOLD_POINTS: usize = 5;
pub const DEFAULT_TUNING_MAX_THRESHOLD_POINTS: usize = 8;
pub const DEFAULT_TUNING_BATCH_SIZE: usize = 1;
pub const DEFAULT_TUNING_THROUGHPUT_TOLERANCE: f64 = 0.05;
pub const DEFAULT_TUNING_MAX_READS_PER_FLUSH: usize = 16;
const KNEE_SCORE_EPSILON: f64 = 1.0e-9;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct WriteCoalescingMeasurement {
    pub threshold_bytes: usize,
    pub input_fragment_bytes: usize,
    pub observed_input_chunks_per_flush: Option<f64>,
    pub throughput_mib_per_sec: f64,
    pub cpu_ns_per_byte: f64,
    pub connection_p99_micros: Option<f64>,
    pub avg_flush_wait_micros: Option<f64>,
    pub max_flush_wait_micros: Option<f64>,
}

impl WriteCoalescingMeasurement {
    pub fn new(
        threshold_bytes: usize,
        input_fragment_bytes: usize,
        throughput_mib_per_sec: f64,
    ) -> Self {
        Self {
            threshold_bytes,
            input_fragment_bytes,
            observed_input_chunks_per_flush: None,
            throughput_mib_per_sec,
            cpu_ns_per_byte: 0.0,
            connection_p99_micros: None,
            avg_flush_wait_micros: None,
            max_flush_wait_micros: None,
        }
    }

    pub fn reads_per_flush(self) -> usize {
        self.input_chunks_per_flush().ceil().max(1.0) as usize
    }

    pub fn input_chunks_per_flush(self) -> f64 {
        self.observed_input_chunks_per_flush
            .filter(|chunks| chunks.is_finite() && *chunks > 0.0)
            .unwrap_or_else(|| self.estimated_input_chunks_per_flush() as f64)
    }

    fn estimated_input_chunks_per_flush(self) -> usize {
        self.threshold_bytes
            .saturating_add(self.input_fragment_bytes.saturating_sub(1))
            / self.input_fragment_bytes
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct WriteCoalescingTunerConfig {
    pub throughput_tolerance: f64,
    pub max_reads_per_flush: Option<usize>,
    pub max_connection_p99_micros: Option<f64>,
    pub max_avg_flush_wait_micros: Option<f64>,
    pub max_max_flush_wait_micros: Option<f64>,
}

impl Default for WriteCoalescingTunerConfig {
    fn default() -> Self {
        Self {
            throughput_tolerance: DEFAULT_TUNING_THROUGHPUT_TOLERANCE,
            max_reads_per_flush: None,
            max_connection_p99_micros: None,
            max_avg_flush_wait_micros: None,
            max_max_flush_wait_micros: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriteCoalescingRecommendationReason {
    FlushDelayThroughputKnee,
    LowestLatencyOnThroughputPlateau,
    LowestLatencyWithinBudget,
    NoBudgetMatch,
}

impl WriteCoalescingRecommendationReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FlushDelayThroughputKnee => "throughput/flush-delay knee",
            Self::LowestLatencyOnThroughputPlateau => {
                "fallback lowest visibility-latency threshold on the throughput plateau"
            }
            Self::LowestLatencyWithinBudget => {
                "fallback lowest visibility-latency threshold within the configured budgets"
            }
            Self::NoBudgetMatch => {
                "throughput/flush-delay knee; no point satisfied the configured budgets"
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct WriteCoalescingRecommendation {
    pub measurement: WriteCoalescingMeasurement,
    pub best_throughput_mib_per_sec: f64,
    pub throughput_floor_mib_per_sec: f64,
    pub reason: WriteCoalescingRecommendationReason,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WriteCoalescingSearchConfig {
    pub tuner_config: WriteCoalescingTunerConfig,
    pub min_threshold_bytes: usize,
    pub max_threshold_bytes: usize,
    pub min_threshold_points: usize,
    pub max_threshold_points: usize,
    pub batch_size: usize,
}

impl Default for WriteCoalescingSearchConfig {
    fn default() -> Self {
        Self {
            tuner_config: WriteCoalescingTunerConfig::default(),
            min_threshold_bytes: DEFAULT_TUNING_MIN_THRESHOLD_BYTES,
            max_threshold_bytes: DEFAULT_TUNING_MAX_THRESHOLD_BYTES,
            min_threshold_points: DEFAULT_TUNING_MIN_THRESHOLD_POINTS,
            max_threshold_points: DEFAULT_TUNING_MAX_THRESHOLD_POINTS,
            batch_size: DEFAULT_TUNING_BATCH_SIZE,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum WriteCoalescingSearchStep {
    Measure { thresholds: Vec<usize> },
    Complete(WriteCoalescingRecommendation),
}

impl WriteCoalescingSearchStep {
    pub fn is_complete(&self) -> bool {
        matches!(self, Self::Complete(_))
    }

    pub fn thresholds(&self) -> &[usize] {
        match self {
            Self::Measure { thresholds } => thresholds,
            Self::Complete(_) => &[],
        }
    }
}

#[derive(Clone, Debug)]
pub struct WriteCoalescingSearch {
    config: WriteCoalescingSearchConfig,
    candidates: Vec<usize>,
    tuner: WriteCoalescingTuner,
}

impl WriteCoalescingSearch {
    pub fn new(config: WriteCoalescingSearchConfig) -> Result<Self, WriteCoalescingTuningError> {
        validate_search_config(&config)?;
        let candidates = threshold_candidates(&config);
        let tuner = WriteCoalescingTuner::new(config.tuner_config)?;
        Ok(Self {
            config,
            candidates,
            tuner,
        })
    }

    pub fn config(&self) -> &WriteCoalescingSearchConfig {
        &self.config
    }

    pub fn candidates(&self) -> &[usize] {
        &self.candidates
    }

    pub fn step<I>(
        &self,
        measurements: I,
    ) -> Result<WriteCoalescingSearchStep, WriteCoalescingTuningError>
    where
        I: IntoIterator<Item = WriteCoalescingMeasurement>,
    {
        let measurements: Vec<_> = measurements.into_iter().collect();
        for (index, measurement) in measurements.iter().enumerate() {
            validate_measurement(index, *measurement)?;
            if measurement.threshold_bytes < self.config.min_threshold_bytes
                || measurement.threshold_bytes > self.config.max_threshold_bytes
            {
                return Err(WriteCoalescingTuningError::MeasurementOutsideSearchRange {
                    index,
                    threshold_bytes: measurement.threshold_bytes,
                });
            }
        }

        let measured_thresholds = measured_thresholds(&measurements);
        let remaining = self
            .config
            .max_threshold_points
            .saturating_sub(measured_thresholds.len());
        if measurements.is_empty() {
            return Ok(WriteCoalescingSearchStep::Measure {
                thresholds: initial_thresholds(&self.candidates, remaining, self.config.batch_size),
            });
        }

        if remaining == 0 {
            return self.complete(measurements);
        }

        let mut proposed = Vec::new();
        if measured_thresholds.len() < 3 {
            push_initial_thresholds(
                &self.candidates,
                &measured_thresholds,
                &mut proposed,
                remaining.min(self.config.batch_size),
            );
        }

        if proposed.is_empty() && measured_thresholds.len() < self.config.min_threshold_points {
            push_space_filling_thresholds(
                &self.candidates,
                &measured_thresholds,
                &mut proposed,
                remaining.min(self.config.batch_size),
            );
        }

        if proposed.is_empty() {
            self.push_neighbor_thresholds(
                &measurements,
                &measured_thresholds,
                &mut proposed,
                remaining.min(self.config.batch_size),
            )?;
        }

        if proposed.is_empty() {
            self.complete(measurements)
        } else {
            Ok(WriteCoalescingSearchStep::Measure {
                thresholds: proposed,
            })
        }
    }

    fn complete(
        &self,
        measurements: Vec<WriteCoalescingMeasurement>,
    ) -> Result<WriteCoalescingSearchStep, WriteCoalescingTuningError> {
        self.tuner
            .recommend(measurements)
            .map(WriteCoalescingSearchStep::Complete)
    }

    fn push_neighbor_thresholds(
        &self,
        measurements: &[WriteCoalescingMeasurement],
        measured_thresholds: &BTreeSet<usize>,
        proposed: &mut Vec<usize>,
        limit: usize,
    ) -> Result<(), WriteCoalescingTuningError> {
        let recommendation = self.tuner.recommend(measurements.iter().copied())?;
        let mut interesting = Vec::new();
        interesting.push(recommendation.measurement.threshold_bytes);
        if let Some(best) = choose_highest_throughput(measurements.iter()) {
            interesting.push(best.threshold_bytes);
        }
        for measurement in measurements.iter().filter(|measurement| {
            measurement.throughput_mib_per_sec >= recommendation.throughput_floor_mib_per_sec
        }) {
            interesting.push(measurement.threshold_bytes);
        }
        interesting.sort_unstable();
        interesting.dedup();

        for threshold in interesting {
            let Some(index) = self.candidate_index(threshold) else {
                continue;
            };
            if index > 0 {
                push_threshold(
                    self.candidates[index - 1],
                    measured_thresholds,
                    proposed,
                    limit,
                );
            }
            if index + 1 < self.candidates.len() {
                push_threshold(
                    self.candidates[index + 1],
                    measured_thresholds,
                    proposed,
                    limit,
                );
            }
            if proposed.len() >= limit {
                break;
            }
        }
        Ok(())
    }

    fn candidate_index(&self, threshold: usize) -> Option<usize> {
        self.candidates.binary_search(&threshold).ok().or_else(|| {
            let index = self
                .candidates
                .partition_point(|candidate| *candidate < threshold);
            if index == self.candidates.len() {
                index.checked_sub(1)
            } else {
                Some(index)
            }
        })
    }
}

impl WriteCoalescingRecommendation {
    pub fn threshold_bytes(self) -> usize {
        self.measurement.threshold_bytes
    }

    pub fn reads_per_flush(self) -> usize {
        self.measurement.reads_per_flush()
    }

    pub fn input_chunks_per_flush(self) -> f64 {
        self.measurement.input_chunks_per_flush()
    }
}

#[derive(Clone, Debug, Default)]
pub struct WriteCoalescingTuner {
    config: WriteCoalescingTunerConfig,
}

impl WriteCoalescingTuner {
    pub fn new(config: WriteCoalescingTunerConfig) -> Result<Self, WriteCoalescingTuningError> {
        validate_config(config)?;
        Ok(Self { config })
    }

    pub fn config(&self) -> WriteCoalescingTunerConfig {
        self.config
    }

    pub fn recommend<I>(
        &self,
        measurements: I,
    ) -> Result<WriteCoalescingRecommendation, WriteCoalescingTuningError>
    where
        I: IntoIterator<Item = WriteCoalescingMeasurement>,
    {
        let measurements: Vec<_> = measurements.into_iter().collect();
        if measurements.is_empty() {
            return Err(WriteCoalescingTuningError::EmptyMeasurements);
        }
        for (index, measurement) in measurements.iter().enumerate() {
            validate_measurement(index, *measurement)?;
        }

        let budgeted: Vec<_> = measurements
            .iter()
            .filter(|measurement| self.under_latency_budgets(measurement))
            .collect();
        if !budgeted.is_empty() {
            return Ok(self.recommend_from_candidates(
                budgeted.into_iter(),
                WriteCoalescingRecommendationReason::FlushDelayThroughputKnee,
            ));
        }

        Ok(self.recommend_from_candidates(
            measurements.iter(),
            WriteCoalescingRecommendationReason::NoBudgetMatch,
        ))
    }

    fn recommend_from_candidates<'a>(
        &self,
        candidates: impl Iterator<Item = &'a WriteCoalescingMeasurement>,
        reason: WriteCoalescingRecommendationReason,
    ) -> WriteCoalescingRecommendation {
        let candidates: Vec<_> = candidates.collect();
        let best_throughput_mib_per_sec = candidates
            .iter()
            .map(|measurement| measurement.throughput_mib_per_sec)
            .max_by(|left, right| left.total_cmp(right))
            .expect("non-empty candidates");
        let throughput_floor_mib_per_sec =
            best_throughput_mib_per_sec * (1.0 - self.config.throughput_tolerance);
        let measurement = choose_flush_delay_throughput_knee(candidates.iter().copied())
            .or_else(|| {
                choose_lowest_latency(candidates.iter().copied().filter(|measurement| {
                    measurement.throughput_mib_per_sec >= throughput_floor_mib_per_sec
                }))
            })
            .expect("best point is on plateau");

        self.recommendation(
            *measurement,
            best_throughput_mib_per_sec,
            throughput_floor_mib_per_sec,
            reason,
        )
    }

    fn recommendation(
        &self,
        measurement: WriteCoalescingMeasurement,
        best_throughput_mib_per_sec: f64,
        throughput_floor_mib_per_sec: f64,
        reason: WriteCoalescingRecommendationReason,
    ) -> WriteCoalescingRecommendation {
        WriteCoalescingRecommendation {
            measurement,
            best_throughput_mib_per_sec,
            throughput_floor_mib_per_sec,
            reason,
        }
    }

    fn under_latency_budgets(&self, measurement: &WriteCoalescingMeasurement) -> bool {
        read_point_under_budget(
            measurement.reads_per_flush(),
            self.config.max_reads_per_flush,
        ) && under_budget(
            measurement.connection_p99_micros,
            self.config.max_connection_p99_micros,
        ) && under_budget(
            measurement.avg_flush_wait_micros,
            self.config.max_avg_flush_wait_micros,
        ) && under_budget(
            measurement.max_flush_wait_micros,
            self.config.max_max_flush_wait_micros,
        )
    }
}

#[derive(Clone, Debug, Error, PartialEq)]
pub enum WriteCoalescingTuningError {
    #[error("at least one measurement is required")]
    EmptyMeasurements,
    #[error("throughput_tolerance must be finite and in [0.0, 1.0)")]
    InvalidThroughputTolerance,
    #[error("max_reads_per_flush must be non-zero")]
    InvalidReadPointBudget,
    #[error("latency budgets must be finite and non-negative")]
    InvalidLatencyBudget,
    #[error("search threshold range must be non-empty")]
    InvalidSearchRange,
    #[error("search point and batch limits must be non-zero and internally consistent")]
    InvalidSearchBudget,
    #[error("measurement {index} threshold {threshold_bytes} is outside the search range")]
    MeasurementOutsideSearchRange {
        index: usize,
        threshold_bytes: usize,
    },
    #[error("measurement {index} has invalid {field}")]
    InvalidMeasurement { index: usize, field: &'static str },
}

fn threshold_candidates(config: &WriteCoalescingSearchConfig) -> Vec<usize> {
    let mut candidates = Vec::new();
    candidates.push(config.min_threshold_bytes);

    let mut threshold = config
        .min_threshold_bytes
        .checked_next_power_of_two()
        .unwrap_or(config.max_threshold_bytes);
    if threshold <= config.min_threshold_bytes {
        threshold = threshold
            .checked_mul(2)
            .unwrap_or(config.max_threshold_bytes);
    }
    while threshold < config.max_threshold_bytes {
        candidates.push(threshold);
        let Some(next) = threshold.checked_mul(2) else {
            break;
        };
        threshold = next;
    }

    candidates.push(config.max_threshold_bytes);
    candidates.sort_unstable();
    candidates.dedup();
    candidates
}

fn measured_thresholds(measurements: &[WriteCoalescingMeasurement]) -> BTreeSet<usize> {
    measurements
        .iter()
        .map(|measurement| measurement.threshold_bytes)
        .collect()
}

fn initial_thresholds(candidates: &[usize], remaining: usize, batch_size: usize) -> Vec<usize> {
    let measured = BTreeSet::new();
    let mut proposed = Vec::new();
    push_initial_thresholds(
        candidates,
        &measured,
        &mut proposed,
        remaining.min(batch_size),
    );
    proposed
}

fn push_initial_thresholds(
    candidates: &[usize],
    measured_thresholds: &BTreeSet<usize>,
    proposed: &mut Vec<usize>,
    limit: usize,
) {
    if limit == 0 || candidates.is_empty() {
        return;
    }

    let midpoint = candidates[candidates.len() / 2];
    for threshold in [
        candidates[0],
        *candidates.last().expect("non-empty candidates"),
        midpoint,
    ] {
        push_threshold(threshold, measured_thresholds, proposed, limit);
    }
}

fn push_space_filling_thresholds(
    candidates: &[usize],
    measured_thresholds: &BTreeSet<usize>,
    proposed: &mut Vec<usize>,
    limit: usize,
) {
    while proposed.len() < limit {
        let Some(threshold) =
            space_filling_threshold(candidates, measured_thresholds, proposed.as_slice())
        else {
            break;
        };
        proposed.push(threshold);
    }
}

fn space_filling_threshold(
    candidates: &[usize],
    measured_thresholds: &BTreeSet<usize>,
    proposed: &[usize],
) -> Option<usize> {
    candidates
        .iter()
        .enumerate()
        .filter(|(_, threshold)| {
            !measured_thresholds.contains(threshold) && !proposed.contains(threshold)
        })
        .max_by_key(|(index, threshold)| {
            let distance = measured_thresholds
                .iter()
                .filter_map(|measured| candidates.binary_search(measured).ok())
                .map(|measured_index| index.abs_diff(measured_index))
                .min()
                .unwrap_or(usize::MAX);
            (distance, usize::MAX - **threshold)
        })
        .map(|(_, threshold)| *threshold)
}

fn push_threshold(
    threshold: usize,
    measured_thresholds: &BTreeSet<usize>,
    proposed: &mut Vec<usize>,
    limit: usize,
) {
    if proposed.len() < limit
        && !measured_thresholds.contains(&threshold)
        && !proposed.contains(&threshold)
    {
        proposed.push(threshold);
    }
}

fn choose_highest_throughput<'a>(
    measurements: impl Iterator<Item = &'a WriteCoalescingMeasurement>,
) -> Option<&'a WriteCoalescingMeasurement> {
    measurements.max_by(|left, right| {
        left.throughput_mib_per_sec
            .total_cmp(&right.throughput_mib_per_sec)
            .then_with(|| visibility_latency_rank(right).cmp(&visibility_latency_rank(left)))
            .then_with(|| connection_latency_rank(right).cmp(&connection_latency_rank(left)))
            .then_with(|| right.threshold_bytes.cmp(&left.threshold_bytes))
    })
}

#[derive(Clone, Copy, Debug)]
struct CurvePoint<'a> {
    measurement: &'a WriteCoalescingMeasurement,
    flush_delay: f64,
    throughput: f64,
}

fn choose_flush_delay_throughput_knee<'a>(
    measurements: impl Iterator<Item = &'a WriteCoalescingMeasurement>,
) -> Option<&'a WriteCoalescingMeasurement> {
    let measurements: Vec<_> = measurements.collect();
    if measurements.len() < 3 {
        return None;
    }

    let use_measured_delay = measurements
        .iter()
        .all(|measurement| flush_delay_micros(**measurement).is_some());
    let mut points: Vec<_> = measurements
        .iter()
        .map(|measurement| CurvePoint {
            measurement,
            flush_delay: if use_measured_delay {
                flush_delay_micros(**measurement).expect("checked measured delay")
            } else {
                measurement.input_chunks_per_flush()
            },
            throughput: measurement.throughput_mib_per_sec,
        })
        .collect();

    points.sort_by(|left, right| {
        left.flush_delay
            .total_cmp(&right.flush_delay)
            .then_with(|| right.throughput.total_cmp(&left.throughput))
            .then_with(|| {
                left.measurement
                    .threshold_bytes
                    .cmp(&right.measurement.threshold_bytes)
            })
    });

    let mut deduped = Vec::new();
    for point in points {
        if deduped
            .last()
            .is_some_and(|last: &CurvePoint<'_>| last.flush_delay == point.flush_delay)
        {
            continue;
        }
        deduped.push(point);
    }

    let mut frontier = Vec::new();
    let mut best_throughput = f64::NEG_INFINITY;
    for point in deduped {
        if point.throughput > best_throughput {
            best_throughput = point.throughput;
            frontier.push(point);
        }
    }
    if frontier.len() < 3 {
        return None;
    }

    let first = frontier.first().expect("frontier has points");
    let last = frontier.last().expect("frontier has points");
    let flush_delay_span = last.flush_delay - first.flush_delay;
    let throughput_span = last.throughput - first.throughput;
    if flush_delay_span <= 0.0 || throughput_span <= 0.0 {
        return None;
    }

    frontier[1..frontier.len() - 1]
        .iter()
        .map(|point| {
            let normalized_delay = (point.flush_delay - first.flush_delay) / flush_delay_span;
            let normalized_throughput = (point.throughput - first.throughput) / throughput_span;
            let score = normalized_throughput - normalized_delay;
            (point, score)
        })
        .filter(|(_, score)| *score > KNEE_SCORE_EPSILON)
        .max_by(|(left, left_score), (right, right_score)| {
            left_score
                .total_cmp(right_score)
                .then_with(|| left.throughput.total_cmp(&right.throughput))
                .then_with(|| right.flush_delay.total_cmp(&left.flush_delay))
                .then_with(|| {
                    right
                        .measurement
                        .threshold_bytes
                        .cmp(&left.measurement.threshold_bytes)
                })
        })
        .map(|(point, _)| point.measurement)
}

fn flush_delay_micros(measurement: WriteCoalescingMeasurement) -> Option<f64> {
    measurement
        .avg_flush_wait_micros
        .or(measurement.max_flush_wait_micros)
}

fn choose_lowest_latency<'a>(
    measurements: impl Iterator<Item = &'a WriteCoalescingMeasurement>,
) -> Option<&'a WriteCoalescingMeasurement> {
    measurements.min_by(|left, right| {
        visibility_latency_rank(left)
            .cmp(&visibility_latency_rank(right))
            .then_with(|| connection_latency_rank(left).cmp(&connection_latency_rank(right)))
            .then_with(|| left.threshold_bytes.cmp(&right.threshold_bytes))
            .then_with(|| {
                right
                    .throughput_mib_per_sec
                    .total_cmp(&left.throughput_mib_per_sec)
            })
    })
}

fn visibility_latency_rank(measurement: &WriteCoalescingMeasurement) -> (u8, u64) {
    if let Some(latency) = measurement.max_flush_wait_micros {
        return (0, latency_micros_rank(latency));
    }
    if let Some(latency) = measurement.avg_flush_wait_micros {
        return (1, latency_micros_rank(latency));
    }
    (2, measurement.reads_per_flush() as u64)
}

fn connection_latency_rank(measurement: &WriteCoalescingMeasurement) -> (u8, u64) {
    measurement
        .connection_p99_micros
        .map(|latency| (0, latency_micros_rank(latency)))
        .unwrap_or((1, u64::MAX))
}

fn latency_micros_rank(latency: f64) -> u64 {
    (latency * 1000.0).round().clamp(0.0, u64::MAX as f64) as u64
}

fn under_budget(measured: Option<f64>, budget: Option<f64>) -> bool {
    match budget {
        Some(budget) => measured.is_some_and(|measured| measured <= budget),
        None => true,
    }
}

fn read_point_under_budget(measured: usize, budget: Option<usize>) -> bool {
    budget.is_none_or(|budget| measured <= budget)
}

fn validate_config(config: WriteCoalescingTunerConfig) -> Result<(), WriteCoalescingTuningError> {
    if !config.throughput_tolerance.is_finite()
        || config.throughput_tolerance < 0.0
        || config.throughput_tolerance >= 1.0
    {
        return Err(WriteCoalescingTuningError::InvalidThroughputTolerance);
    }
    if config.max_reads_per_flush == Some(0) {
        return Err(WriteCoalescingTuningError::InvalidReadPointBudget);
    }
    for budget in [
        config.max_connection_p99_micros,
        config.max_avg_flush_wait_micros,
        config.max_max_flush_wait_micros,
    ] {
        if budget.is_some_and(|budget| !budget.is_finite() || budget < 0.0) {
            return Err(WriteCoalescingTuningError::InvalidLatencyBudget);
        }
    }
    Ok(())
}

fn validate_search_config(
    config: &WriteCoalescingSearchConfig,
) -> Result<(), WriteCoalescingTuningError> {
    validate_config(config.tuner_config)?;
    if config.min_threshold_bytes == 0
        || config.max_threshold_bytes == 0
        || config.min_threshold_bytes > config.max_threshold_bytes
    {
        return Err(WriteCoalescingTuningError::InvalidSearchRange);
    }
    if config.min_threshold_points == 0
        || config.max_threshold_points == 0
        || config.batch_size == 0
        || config.min_threshold_points > config.max_threshold_points
    {
        return Err(WriteCoalescingTuningError::InvalidSearchBudget);
    }
    Ok(())
}

fn validate_measurement(
    index: usize,
    measurement: WriteCoalescingMeasurement,
) -> Result<(), WriteCoalescingTuningError> {
    if measurement.threshold_bytes == 0 {
        return Err(WriteCoalescingTuningError::InvalidMeasurement {
            index,
            field: "threshold_bytes",
        });
    }
    if measurement.input_fragment_bytes == 0 {
        return Err(WriteCoalescingTuningError::InvalidMeasurement {
            index,
            field: "input_fragment_bytes",
        });
    }
    if measurement
        .observed_input_chunks_per_flush
        .is_some_and(|value| !value.is_finite() || value <= 0.0)
    {
        return Err(WriteCoalescingTuningError::InvalidMeasurement {
            index,
            field: "observed_input_chunks_per_flush",
        });
    }
    if !measurement.throughput_mib_per_sec.is_finite() || measurement.throughput_mib_per_sec < 0.0 {
        return Err(WriteCoalescingTuningError::InvalidMeasurement {
            index,
            field: "throughput_mib_per_sec",
        });
    }
    if !measurement.cpu_ns_per_byte.is_finite() || measurement.cpu_ns_per_byte < 0.0 {
        return Err(WriteCoalescingTuningError::InvalidMeasurement {
            index,
            field: "cpu_ns_per_byte",
        });
    }
    for (field, value) in [
        ("connection_p99_micros", measurement.connection_p99_micros),
        ("avg_flush_wait_micros", measurement.avg_flush_wait_micros),
        ("max_flush_wait_micros", measurement.max_flush_wait_micros),
    ] {
        if value.is_some_and(|value| !value.is_finite() || value < 0.0) {
            return Err(WriteCoalescingTuningError::InvalidMeasurement { index, field });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn measurement(
        threshold_bytes: usize,
        throughput_mib_per_sec: f64,
    ) -> WriteCoalescingMeasurement {
        WriteCoalescingMeasurement {
            threshold_bytes,
            input_fragment_bytes: 64,
            observed_input_chunks_per_flush: None,
            throughput_mib_per_sec,
            cpu_ns_per_byte: 1.0,
            connection_p99_micros: Some(100.0),
            avg_flush_wait_micros: None,
            max_flush_wait_micros: None,
        }
    }

    #[test]
    fn chooses_lowest_latency_on_throughput_plateau() {
        let tuner = WriteCoalescingTuner::new(WriteCoalescingTunerConfig {
            throughput_tolerance: 0.10,
            max_reads_per_flush: None,
            ..WriteCoalescingTunerConfig::default()
        })
        .expect("valid tuner");
        let mut smallest = measurement(1024, 90.0);
        smallest.connection_p99_micros = Some(30.0);
        smallest.max_flush_wait_micros = Some(30.0);
        let mut lower_latency = measurement(2048, 96.0);
        lower_latency.connection_p99_micros = Some(20.0);
        lower_latency.max_flush_wait_micros = Some(20.0);
        let mut best_throughput = measurement(4096, 100.0);
        best_throughput.connection_p99_micros = Some(25.0);
        best_throughput.max_flush_wait_micros = Some(25.0);

        let recommendation = tuner
            .recommend([smallest, lower_latency, best_throughput])
            .expect("recommend threshold");

        assert_eq!(recommendation.threshold_bytes(), 2048);
        assert_eq!(recommendation.reads_per_flush(), 32);
        assert_eq!(
            recommendation.reason,
            WriteCoalescingRecommendationReason::FlushDelayThroughputKnee
        );
    }

    #[test]
    fn chooses_flush_delay_throughput_knee() {
        let tuner = WriteCoalescingTuner::new(WriteCoalescingTunerConfig {
            max_reads_per_flush: None,
            ..WriteCoalescingTunerConfig::default()
        })
        .expect("valid tuner");
        let mut immediate = measurement(64, 100.0);
        immediate.avg_flush_wait_micros = Some(1.0);
        let mut knee = measurement(1024, 900.0);
        knee.avg_flush_wait_micros = Some(10.0);
        let mut late = measurement(8192, 980.0);
        late.avg_flush_wait_micros = Some(50.0);
        let mut max_throughput = measurement(16 * 1024, 1000.0);
        max_throughput.avg_flush_wait_micros = Some(100.0);

        let recommendation = tuner
            .recommend([immediate, knee, late, max_throughput])
            .expect("recommend threshold");

        assert_eq!(recommendation.threshold_bytes(), 1024);
        assert_eq!(
            recommendation.reason,
            WriteCoalescingRecommendationReason::FlushDelayThroughputKnee
        );
    }

    #[test]
    fn chooses_lowest_latency_budgeted_when_plateau_exceeds_latency_budget() {
        let tuner = WriteCoalescingTuner::new(WriteCoalescingTunerConfig {
            throughput_tolerance: 0.05,
            max_connection_p99_micros: Some(10.0),
            ..WriteCoalescingTunerConfig::default()
        })
        .expect("valid tuner");
        let mut low_latency = measurement(1024, 90.0);
        low_latency.connection_p99_micros = Some(8.0);
        let mut high_latency = measurement(2048, 100.0);
        high_latency.connection_p99_micros = Some(20.0);

        let recommendation = tuner
            .recommend([low_latency, high_latency])
            .expect("recommend threshold");

        assert_eq!(recommendation.threshold_bytes(), 1024);
        assert_eq!(
            recommendation.reason,
            WriteCoalescingRecommendationReason::FlushDelayThroughputKnee
        );
    }

    #[test]
    fn falls_back_to_plateau_when_no_latency_budget_matches() {
        let tuner = WriteCoalescingTuner::new(WriteCoalescingTunerConfig {
            throughput_tolerance: 0.10,
            max_connection_p99_micros: Some(1.0),
            ..WriteCoalescingTunerConfig::default()
        })
        .expect("valid tuner");

        let recommendation = tuner
            .recommend([measurement(1024, 90.0), measurement(2048, 100.0)])
            .expect("recommend threshold");

        assert_eq!(recommendation.threshold_bytes(), 1024);
        assert_eq!(
            recommendation.reason,
            WriteCoalescingRecommendationReason::NoBudgetMatch
        );
    }

    #[test]
    fn uses_smallest_threshold_as_latency_tie_break() {
        let tuner = WriteCoalescingTuner::new(WriteCoalescingTunerConfig {
            throughput_tolerance: 0.10,
            max_reads_per_flush: None,
            ..WriteCoalescingTunerConfig::default()
        })
        .expect("valid tuner");

        let recommendation = tuner
            .recommend([measurement(1024, 90.0), measurement(2048, 100.0)])
            .expect("recommend threshold");

        assert_eq!(recommendation.threshold_bytes(), 1024);
    }

    #[test]
    fn uses_flush_wait_latency_when_connection_latency_is_missing() {
        let tuner = WriteCoalescingTuner::new(WriteCoalescingTunerConfig {
            throughput_tolerance: 0.10,
            max_reads_per_flush: None,
            ..WriteCoalescingTunerConfig::default()
        })
        .expect("valid tuner");
        let mut higher_wait = measurement(1024, 90.0);
        higher_wait.connection_p99_micros = None;
        higher_wait.max_flush_wait_micros = Some(30.0);
        let mut lower_wait = measurement(2048, 100.0);
        lower_wait.connection_p99_micros = None;
        lower_wait.max_flush_wait_micros = Some(20.0);

        let recommendation = tuner
            .recommend([higher_wait, lower_wait])
            .expect("recommend threshold");

        assert_eq!(recommendation.threshold_bytes(), 2048);
    }

    #[test]
    fn read_point_budget_limits_large_coalescing_thresholds() {
        let tuner = WriteCoalescingTuner::new(WriteCoalescingTunerConfig {
            throughput_tolerance: 0.05,
            max_reads_per_flush: Some(16),
            ..WriteCoalescingTunerConfig::default()
        })
        .expect("valid tuner");
        let mut small_visible_window = measurement(1024, 90.0);
        small_visible_window.connection_p99_micros = Some(30.0);
        let mut larger_batch = measurement(32 * 1024, 100.0);
        larger_batch.connection_p99_micros = Some(5.0);

        let recommendation = tuner
            .recommend([small_visible_window, larger_batch])
            .expect("recommend threshold");

        assert_eq!(recommendation.threshold_bytes(), 1024);
        assert_eq!(recommendation.reads_per_flush(), 16);
        assert_eq!(
            recommendation.reason,
            WriteCoalescingRecommendationReason::FlushDelayThroughputKnee
        );
    }

    #[test]
    fn read_point_budget_uses_observed_input_chunks_when_available() {
        let tuner = WriteCoalescingTuner::new(WriteCoalescingTunerConfig {
            throughput_tolerance: 0.05,
            max_reads_per_flush: Some(4),
            ..WriteCoalescingTunerConfig::default()
        })
        .expect("valid tuner");
        let mut smaller = measurement(1024, 90.0);
        smaller.observed_input_chunks_per_flush = Some(1.0);
        let mut larger = measurement(32 * 1024, 100.0);
        larger.input_fragment_bytes = 1460;
        larger.observed_input_chunks_per_flush = Some(2.4);

        let recommendation = tuner
            .recommend([smaller, larger])
            .expect("recommend threshold");

        assert_eq!(recommendation.threshold_bytes(), 32 * 1024);
        assert_eq!(recommendation.reads_per_flush(), 3);
        assert_eq!(recommendation.input_chunks_per_flush(), 2.4);
    }

    #[test]
    fn adaptive_search_seeds_min_max_and_midpoint() {
        let search = WriteCoalescingSearch::new(WriteCoalescingSearchConfig {
            batch_size: 3,
            ..WriteCoalescingSearchConfig::default()
        })
        .expect("valid search");

        let step = search.step([]).expect("next step");

        assert_eq!(
            step,
            WriteCoalescingSearchStep::Measure {
                thresholds: vec![1, 16 * 1024, 128]
            }
        );
    }

    #[test]
    fn adaptive_search_refines_neighbor_around_the_current_plateau() {
        let search = WriteCoalescingSearch::new(WriteCoalescingSearchConfig {
            tuner_config: WriteCoalescingTunerConfig {
                throughput_tolerance: 0.05,
                max_reads_per_flush: None,
                ..WriteCoalescingTunerConfig::default()
            },
            min_threshold_points: 3,
            max_threshold_points: 6,
            ..WriteCoalescingSearchConfig::default()
        })
        .expect("valid search");
        let mut lower_latency = measurement(8192, 98.0);
        lower_latency.connection_p99_micros = Some(20.0);
        let mut higher_throughput = measurement(16 * 1024, 100.0);
        higher_throughput.connection_p99_micros = Some(30.0);

        let step = search
            .step([measurement(1, 10.0), lower_latency, higher_throughput])
            .expect("next step");

        assert_eq!(
            step,
            WriteCoalescingSearchStep::Measure {
                thresholds: vec![4096]
            }
        );
    }

    #[test]
    fn adaptive_search_completes_when_plateau_neighbors_are_measured() {
        let search = WriteCoalescingSearch::new(WriteCoalescingSearchConfig {
            tuner_config: WriteCoalescingTunerConfig {
                throughput_tolerance: 0.05,
                max_reads_per_flush: None,
                ..WriteCoalescingTunerConfig::default()
            },
            min_threshold_points: 3,
            max_threshold_points: 6,
            ..WriteCoalescingSearchConfig::default()
        })
        .expect("valid search");
        let mut lower_latency = measurement(8192, 98.0);
        lower_latency.connection_p99_micros = Some(20.0);
        let mut higher_throughput = measurement(16 * 1024, 100.0);
        higher_throughput.connection_p99_micros = Some(30.0);

        let step = search
            .step([
                measurement(1, 10.0),
                measurement(4096, 50.0),
                lower_latency,
                higher_throughput,
            ])
            .expect("next step");

        let WriteCoalescingSearchStep::Complete(recommendation) = step else {
            panic!("expected completed search");
        };
        assert_eq!(recommendation.threshold_bytes(), 8192);
    }

    #[test]
    fn rejects_invalid_measurements() {
        let tuner = WriteCoalescingTuner::default();
        let err = tuner
            .recommend([WriteCoalescingMeasurement::new(0, 64, 1.0)])
            .expect_err("invalid threshold");

        assert_eq!(
            err,
            WriteCoalescingTuningError::InvalidMeasurement {
                index: 0,
                field: "threshold_bytes"
            }
        );
    }
}
