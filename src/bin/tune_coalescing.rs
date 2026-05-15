use bytes_handoff::{
    WriteCoalescingMeasurement, WriteCoalescingRecommendation, WriteCoalescingSearch,
    WriteCoalescingSearchConfig, WriteCoalescingSearchStep, WriteCoalescingTuner,
};
use std::collections::BTreeMap;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};

const GROUP_COLUMNS: &[&str] = &[
    "transport",
    "implementation",
    "scenario",
    "completion",
    "worker_threads",
    "connections",
    "route_frames",
    "frame_len",
    "tunnel_bytes",
    "input_fragment",
    "input_model",
    "tcp_mss_bytes",
    "read_reserve",
    "write_pending_bytes",
    "duplex_capacity",
    "duration_seconds_target",
];

#[derive(Debug)]
struct Args {
    paths: Vec<PathBuf>,
    implementation: String,
    next: bool,
    search_config: WriteCoalescingSearchConfig,
}

#[derive(Clone, Debug)]
struct Row {
    fields: BTreeMap<String, String>,
}

#[derive(Clone, Debug)]
struct Point {
    measurement: WriteCoalescingMeasurement,
    runs: usize,
    coalescer_stats_enabled: bool,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = parse_args()?;
    if args.next && args.paths.is_empty() {
        let search = WriteCoalescingSearch::new(args.search_config)?;
        let step = search.step(std::iter::empty())?;
        print_search_step(&[], &[], step);
        return Ok(());
    }

    let tuner = WriteCoalescingTuner::new(args.search_config.tuner_config)?;
    let search = WriteCoalescingSearch::new(args.search_config)?;
    let rows = read_matching_rows(&args.paths, &args.implementation)?;
    let groups = group_rows(rows);
    let mut printed = false;

    for (key, rows) in groups {
        let mut threshold_groups: BTreeMap<usize, Vec<Row>> = BTreeMap::new();
        for row in rows {
            threshold_groups
                .entry(row.usize("handoff_flush_bytes", 1))
                .or_default()
                .push(row);
        }
        if !args.next && threshold_groups.len() < 2 {
            continue;
        }

        let points: Vec<_> = threshold_groups
            .values()
            .map(|rows| aggregate_point(rows))
            .collect::<Result<_, _>>()?;
        if args.next {
            let step = search.step(points.iter().map(|point| point.measurement))?;
            print_search_step(&key, &points, step);
        } else {
            let recommendation = tuner.recommend(points.iter().map(|point| point.measurement))?;
            print_group(&key, &points, recommendation);
        }
        printed = true;
    }

    if !printed {
        return Err("no compatible groups with at least two threshold points found".into());
    }
    Ok(())
}

fn parse_args() -> Result<Args, Box<dyn Error>> {
    let mut paths = Vec::new();
    let mut implementation = String::from("handoff");
    let mut search_config = WriteCoalescingSearchConfig::default();
    let mut next = false;
    let mut args = std::env::args().skip(1);

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--next" => {
                next = true;
            }
            "--implementation" => {
                implementation = args.next().ok_or("--implementation requires a value")?;
            }
            "--throughput-within" => {
                search_config.tuner_config.throughput_tolerance = args
                    .next()
                    .ok_or("--throughput-within requires a value")?
                    .parse()?;
            }
            "--max-reads-per-flush" => {
                search_config.tuner_config.max_reads_per_flush = Some(
                    args.next()
                        .ok_or("--max-reads-per-flush requires a value")?
                        .parse()?,
                );
            }
            "--no-max-reads-per-flush" => {
                search_config.tuner_config.max_reads_per_flush = None;
            }
            "--max-connection-p99-us" => {
                search_config.tuner_config.max_connection_p99_micros = Some(
                    args.next()
                        .ok_or("--max-connection-p99-us requires a value")?
                        .parse()?,
                );
            }
            "--max-avg-flush-wait-us" => {
                search_config.tuner_config.max_avg_flush_wait_micros = Some(
                    args.next()
                        .ok_or("--max-avg-flush-wait-us requires a value")?
                        .parse()?,
                );
            }
            "--max-max-flush-wait-us" => {
                search_config.tuner_config.max_max_flush_wait_micros = Some(
                    args.next()
                        .ok_or("--max-max-flush-wait-us requires a value")?
                        .parse()?,
                );
            }
            "--min-threshold-bytes" => {
                search_config.min_threshold_bytes = args
                    .next()
                    .ok_or("--min-threshold-bytes requires a value")?
                    .parse()?;
            }
            "--max-threshold-bytes" => {
                search_config.max_threshold_bytes = args
                    .next()
                    .ok_or("--max-threshold-bytes requires a value")?
                    .parse()?;
            }
            "--min-threshold-points" => {
                search_config.min_threshold_points = args
                    .next()
                    .ok_or("--min-threshold-points requires a value")?
                    .parse()?;
            }
            "--max-threshold-points" => {
                search_config.max_threshold_points = args
                    .next()
                    .ok_or("--max-threshold-points requires a value")?
                    .parse()?;
            }
            "--batch-size" => {
                search_config.batch_size = args
                    .next()
                    .ok_or("--batch-size requires a value")?
                    .parse()?;
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            value if value.starts_with('-') => {
                return Err(format!("unknown option: {value}").into());
            }
            value => paths.push(PathBuf::from(value)),
        }
    }

    if paths.is_empty() && !next {
        return Err("usage: tune_coalescing <runs.csv|results-dir>...".into());
    }
    Ok(Args {
        paths,
        implementation,
        next,
        search_config,
    })
}

fn print_usage() {
    println!("Usage: tune_coalescing <runs.csv|results-dir>...");
    println!("  --next                         print the next adaptive threshold to measure");
    println!("  --implementation NAME          default: handoff");
    println!("  --throughput-within FRACTION   default: 0.05");
    println!("  --max-reads-per-flush N        optional hard source-chunk budget");
    println!("  --no-max-reads-per-flush       use the unconstrained default");
    println!("  --max-connection-p99-us N");
    println!("  --max-avg-flush-wait-us N      requires --coalescer-stats runs");
    println!("  --max-max-flush-wait-us N      requires --coalescer-stats runs");
    println!("  --min-threshold-bytes N        default: 1");
    println!("  --max-threshold-bytes N        default: 16384");
    println!("  --min-threshold-points N       default: 5");
    println!("  --max-threshold-points N       default: 8");
    println!("  --batch-size N                 default: 1");
}

fn read_matching_rows(paths: &[PathBuf], implementation: &str) -> Result<Vec<Row>, Box<dyn Error>> {
    let mut out = Vec::new();
    for path in csv_paths(paths)? {
        for row in read_csv(&path)? {
            if row.get("implementation") == implementation
                && !row.get("handoff_flush_bytes").is_empty()
            {
                out.push(row);
            }
        }
    }
    if out.is_empty() {
        return Err("no matching stream-harness rows found".into());
    }
    Ok(out)
}

fn csv_paths(paths: &[PathBuf]) -> Result<Vec<PathBuf>, Box<dyn Error>> {
    let mut out = Vec::new();
    for path in paths {
        if path.is_dir() {
            collect_runs_csv(path, &mut out)?;
        } else {
            out.push(path.clone());
        }
    }
    out.sort();
    out.dedup();
    Ok(out)
}

fn collect_runs_csv(dir: &Path, out: &mut Vec<PathBuf>) -> Result<(), Box<dyn Error>> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_runs_csv(&path, out)?;
        } else if path.file_name().is_some_and(|name| name == "runs.csv") {
            out.push(path);
        }
    }
    Ok(())
}

fn read_csv(path: &Path) -> Result<Vec<Row>, Box<dyn Error>> {
    let text = fs::read_to_string(path)?;
    let mut lines = text.lines();
    let Some(header) = lines.next() else {
        return Ok(Vec::new());
    };
    let headers: Vec<_> = header.split(',').map(str::to_owned).collect();
    let mut rows = Vec::new();
    for line in lines.filter(|line| !line.trim().is_empty()) {
        let values: Vec<_> = line.split(',').collect();
        let mut fields = BTreeMap::new();
        for (index, header) in headers.iter().enumerate() {
            fields.insert(
                header.clone(),
                values.get(index).copied().unwrap_or_default().to_owned(),
            );
        }
        rows.push(Row { fields });
    }
    Ok(rows)
}

fn group_rows(rows: Vec<Row>) -> BTreeMap<Vec<String>, Vec<Row>> {
    let mut groups: BTreeMap<Vec<String>, Vec<Row>> = BTreeMap::new();
    for row in rows {
        let key = GROUP_COLUMNS
            .iter()
            .map(|column| row.get(column).to_owned())
            .collect();
        groups.entry(key).or_default().push(row);
    }
    groups
}

fn aggregate_point(rows: &[Row]) -> Result<Point, Box<dyn Error>> {
    let input_fragment_bytes = if rows[0].get("input_model") == "tcp" {
        rows[0].usize("tcp_mss_bytes", rows[0].usize("input_fragment", 1))
    } else {
        rows[0].usize("input_fragment", 1)
    };
    let threshold_bytes = rows[0].usize("handoff_flush_bytes", 1);
    let coalescer_stats_enabled = rows.iter().any(|row| row.bool("coalescer_stats_enabled"));
    let measurement = WriteCoalescingMeasurement {
        threshold_bytes,
        input_fragment_bytes,
        observed_input_chunks_per_flush: coalescer_stats_enabled
            .then(|| observed_input_chunks_per_flush(rows))
            .flatten(),
        throughput_mib_per_sec: mean_field(rows, "mib_per_sec"),
        cpu_ns_per_byte: mean_field(rows, "cpu_ns_per_byte"),
        connection_p99_micros: mean_optional_field(rows, "latency_p99_micros"),
        avg_flush_wait_micros: coalescer_stats_enabled
            .then(|| mean_optional_field(rows, "coalescer_avg_flush_wait_nanos"))
            .flatten()
            .map(|nanos| nanos / 1000.0),
        max_flush_wait_micros: coalescer_stats_enabled
            .then(|| mean_optional_field(rows, "coalescer_max_flush_wait_nanos"))
            .flatten()
            .map(|nanos| nanos / 1000.0),
    };
    Ok(Point {
        measurement,
        runs: rows.len(),
        coalescer_stats_enabled,
    })
}

fn observed_input_chunks_per_flush(rows: &[Row]) -> Option<f64> {
    mean_optional_field(rows, "coalescer_avg_buffered_chunks_per_flush")
        .filter(|chunks| *chunks > 0.0)
        .or_else(|| mean_optional_field(rows, "coalescer_avg_chunks_per_flush"))
        .filter(|chunks| *chunks > 0.0)
}

fn mean_field(rows: &[Row], field: &str) -> f64 {
    mean_optional_field(rows, field).unwrap_or(0.0)
}

fn mean_optional_field(rows: &[Row], field: &str) -> Option<f64> {
    let values: Vec<_> = rows.iter().filter_map(|row| row.f64(field)).collect();
    (!values.is_empty()).then(|| values.iter().sum::<f64>() / (values.len() as f64))
}

fn print_group(key: &[String], points: &[Point], recommendation: WriteCoalescingRecommendation) {
    print_points(key, points);
    print_recommendation(recommendation);
}

fn print_search_step(key: &[String], points: &[Point], step: WriteCoalescingSearchStep) {
    print_points(key, points);
    match step {
        WriteCoalescingSearchStep::Measure { thresholds } => {
            let thresholds = thresholds
                .iter()
                .map(|threshold| threshold.to_string())
                .collect::<Vec<_>>()
                .join(",");
            println!("next_handoff_flush_bytes={thresholds}");
        }
        WriteCoalescingSearchStep::Complete(recommendation) => {
            print_recommendation(recommendation);
        }
    }
}

fn print_points(key: &[String], points: &[Point]) {
    let label = GROUP_COLUMNS
        .iter()
        .zip(key)
        .map(|(column, value)| format!("{column}={value}"))
        .collect::<Vec<_>>()
        .join(" ");
    if !label.is_empty() {
        println!("\n# {label}");
    }
    if !points.is_empty() {
        println!(
            "threshold,input_chunks_per_flush,runs,mib_per_sec,cpu_ns_per_byte,connection_p99_us,coalescer_stats,avg_flush_wait_us,max_flush_wait_us"
        );
    }
    for point in points {
        println!(
            "{},{:.2},{},{:.2},{:.2},{:.2},{},{:.3},{:.3}",
            point.measurement.threshold_bytes,
            point.measurement.input_chunks_per_flush(),
            point.runs,
            point.measurement.throughput_mib_per_sec,
            point.measurement.cpu_ns_per_byte,
            point.measurement.connection_p99_micros.unwrap_or(0.0),
            usize::from(point.coalescer_stats_enabled),
            point.measurement.avg_flush_wait_micros.unwrap_or(0.0),
            point.measurement.max_flush_wait_micros.unwrap_or(0.0),
        );
    }
}

fn print_recommendation(recommendation: WriteCoalescingRecommendation) {
    println!(
        "recommended_handoff_flush_bytes={} recommended_input_chunks_per_flush={:.2} recommended_reads_per_flush={} recommended_mib_per_sec={:.2} recommended_connection_p99_us={:.2} recommended_avg_flush_wait_us={:.3} recommended_max_flush_wait_us={:.3} reason=\"{}\"",
        recommendation.threshold_bytes(),
        recommendation.input_chunks_per_flush(),
        recommendation.reads_per_flush(),
        recommendation.measurement.throughput_mib_per_sec,
        recommendation
            .measurement
            .connection_p99_micros
            .unwrap_or(0.0),
        recommendation
            .measurement
            .avg_flush_wait_micros
            .unwrap_or(0.0),
        recommendation
            .measurement
            .max_flush_wait_micros
            .unwrap_or(0.0),
        recommendation.reason.as_str(),
    );
}

impl Row {
    fn get(&self, field: &str) -> &str {
        self.fields.get(field).map_or("", String::as_str)
    }

    fn usize(&self, field: &str, default: usize) -> usize {
        self.get(field)
            .parse::<usize>()
            .or_else(|_| self.get(field).parse::<f64>().map(|value| value as usize))
            .unwrap_or(default)
    }

    fn f64(&self, field: &str) -> Option<f64> {
        let value = self.get(field);
        if value.is_empty() {
            None
        } else {
            value.parse().ok()
        }
    }

    fn bool(&self, field: &str) -> bool {
        matches!(self.get(field), "1" | "true" | "yes")
    }
}
