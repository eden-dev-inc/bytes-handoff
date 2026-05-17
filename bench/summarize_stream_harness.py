#!/usr/bin/env python3
import csv
import pathlib
import statistics
import sys


FIELDS = [
    "actual_iterations",
    "total_streams",
    "total_seconds",
    "bytes_per_sec",
    "mib_per_sec",
    "gib_per_sec",
    "streams_per_sec",
    "cpu_total_seconds",
    "cpu_avg_cores",
    "cpu_utilization_pct",
    "cpu_avg_cores_per_worker",
    "cpu_utilization_pct_per_worker",
    "cpu_ns_per_byte",
    "voluntary_context_switches",
    "involuntary_context_switches",
    "max_rss_bytes",
    "latency_p50_micros",
    "latency_p95_micros",
    "latency_p99_micros",
    "latency_p999_micros",
    "latency_max_micros",
    "coalescer_input_chunks",
    "coalescer_input_bytes",
    "coalescer_flushes",
    "coalescer_flush_bytes",
    "coalescer_buffered_flushes",
    "coalescer_direct_flushes",
    "coalescer_buffered_input_chunks",
    "coalescer_avg_bytes_per_flush",
    "coalescer_avg_chunks_per_flush",
    "coalescer_avg_buffered_chunks_per_flush",
    "coalescer_max_chunks_per_flush",
    "coalescer_max_bytes_per_flush",
    "coalescer_max_pending_bytes",
    "coalescer_avg_flush_wait_nanos",
    "coalescer_max_flush_wait_nanos",
    "read_telemetry_read_calls",
    "read_telemetry_read_bytes",
    "read_telemetry_read_errors",
    "read_telemetry_read_error_limit_exceeded",
    "read_telemetry_zero_reads",
    "read_telemetry_buffer_growths",
    "read_telemetry_buffer_growth_bytes",
    "read_telemetry_max_buffered_bytes",
    "read_telemetry_split_prefixes",
    "read_telemetry_split_prefix_bytes",
    "read_telemetry_copied_prefixes",
    "read_telemetry_copied_prefix_bytes",
    "read_telemetry_frozen_prefixes",
    "read_telemetry_frozen_prefix_bytes",
    "read_telemetry_mutable_prefixes",
    "read_telemetry_mutable_prefix_bytes",
    "read_telemetry_freeze_all_calls",
    "read_telemetry_freeze_all_bytes",
    "read_telemetry_advances",
    "read_telemetry_advanced_bytes",
    "read_telemetry_tails_taken",
    "read_telemetry_tail_bytes",
    "read_telemetry_monoio_read_buffer_swaps",
    "read_telemetry_monoio_read_buffer_copies",
    "read_telemetry_read_size_count",
    "read_telemetry_read_size_sum",
    "read_telemetry_buffered_bytes_count",
    "read_telemetry_buffered_bytes_sum",
]

RUN_COLUMNS = [
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
    "tcp_shard_mode",
    "read_reserve",
    "handoff_flush_bytes",
    "read_telemetry_enabled",
    "coalescer_stats_enabled",
    "write_pending_bytes",
    "duplex_capacity",
    "configured_iterations",
    "duration_seconds_target",
    *FIELDS,
]


def parse_run(path: pathlib.Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for line in path.read_text().splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        out[key.strip()] = value.strip()
    return out


def mean(values: list[float]) -> float:
    return statistics.fmean(values) if values else 0.0


def stdev(values: list[float]) -> float:
    return statistics.stdev(values) if len(values) >= 2 else 0.0


def mean_field(runs: list[dict[str, str]], field: str) -> float:
    return mean([float(run[field]) for run in runs if field in run])


def write_summary(
    run_dir: pathlib.Path,
    pattern: str,
    summary_name: str,
    runs_name: str,
    label: str,
) -> bool:
    runs = [parse_run(path) for path in sorted(run_dir.glob(pattern))]
    if not runs:
        return False

    summary_path = run_dir / summary_name
    with summary_path.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["metric", "mean", "stdev"])
        for field in FIELDS:
            values = [float(run[field]) for run in runs if field in run]
            writer.writerow([field, f"{mean(values):.6}", f"{stdev(values):.6}"])

    runs_path = run_dir / runs_name
    with runs_path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=RUN_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for run in runs:
            writer.writerow(run)

    transport = runs[0].get("transport", "unknown")
    implementation = runs[0].get("implementation", "unknown")
    scenario = runs[0].get("scenario", "unknown")
    throughput = mean_field(runs, "mib_per_sec")
    seconds = mean_field(runs, "total_seconds")
    cpu_avg_cores = mean_field(runs, "cpu_avg_cores")
    cpu_per_worker = mean_field(runs, "cpu_avg_cores_per_worker")
    cpu_ns_per_byte = mean_field(runs, "cpu_ns_per_byte")
    fields = [
        f"{label}_summary transport={transport}",
        f"implementation={implementation}",
        f"scenario={scenario}",
        f"runs={len(runs)}",
        f"mean_mib_per_sec={throughput:.2f}",
        f"mean_seconds={seconds:.6f}",
        f"mean_cpu_avg_cores={cpu_avg_cores:.2f}",
        f"mean_cpu_avg_cores_per_worker={cpu_per_worker:.3f}",
        f"mean_cpu_ns_per_byte={cpu_ns_per_byte:.2f}",
    ]
    if "latency_p99_micros" in runs[0]:
        latency_p99 = mean_field(runs, "latency_p99_micros")
        fields.append(f"mean_latency_p99_micros={latency_p99:.2f}")
    if runs[0].get("coalescer_stats_enabled", "").lower() in {"1", "true", "yes"}:
        wait_nanos = mean_field(runs, "coalescer_avg_flush_wait_nanos")
        chunks = mean_field(runs, "coalescer_avg_buffered_chunks_per_flush")
        fields.append(f"mean_coalescer_avg_flush_wait_nanos={wait_nanos:.2f}")
        fields.append(f"mean_coalescer_avg_buffered_chunks_per_flush={chunks:.2f}")
    print(" ".join(fields))
    print(f"summary_csv={summary_path}")
    print(f"runs_csv={runs_path}")
    return True


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: summarize_stream_harness.py <run-dir>")

    run_dir = pathlib.Path(sys.argv[1])
    if not write_summary(run_dir, "handoff-run-*.txt", "summary.csv", "runs.csv", "driver"):
        raise SystemExit(f"no handoff-run-*.txt files found in {run_dir}")
    write_summary(
        run_dir,
        "service-run-*.txt",
        "service-summary.csv",
        "service-runs.csv",
        "service",
    )


if __name__ == "__main__":
    main()
