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
    "cpu_ns_per_byte",
    "voluntary_context_switches",
    "involuntary_context_switches",
    "max_rss_bytes",
    "latency_p50_micros",
    "latency_p95_micros",
    "latency_p99_micros",
    "latency_p999_micros",
    "latency_max_micros",
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
    "read_reserve",
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


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: summarize_stream_harness.py <run-dir>")

    run_dir = pathlib.Path(sys.argv[1])
    runs = [parse_run(path) for path in sorted(run_dir.glob("handoff-run-*.txt"))]
    if not runs:
        raise SystemExit(f"no handoff-run-*.txt files found in {run_dir}")

    summary_path = run_dir / "summary.csv"
    with summary_path.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["metric", "mean", "stdev"])
        for field in FIELDS:
            values = [float(run[field]) for run in runs if field in run]
            writer.writerow([field, f"{mean(values):.6}", f"{stdev(values):.6}"])

    runs_path = run_dir / "runs.csv"
    with runs_path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=RUN_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for run in runs:
            writer.writerow(run)

    transport = runs[0].get("transport", "unknown")
    implementation = runs[0].get("implementation", "unknown")
    scenario = runs[0].get("scenario", "unknown")
    throughput = mean([float(run["mib_per_sec"]) for run in runs])
    seconds = mean([float(run["total_seconds"]) for run in runs])
    cpu_ns_per_byte = mean([float(run["cpu_ns_per_byte"]) for run in runs])
    latency_p99 = mean([float(run["latency_p99_micros"]) for run in runs])
    print(
        f"summary transport={transport} implementation={implementation} scenario={scenario} runs={len(runs)} "
        f"mean_mib_per_sec={throughput:.2f} mean_seconds={seconds:.6f} "
        f"mean_cpu_ns_per_byte={cpu_ns_per_byte:.2f} mean_latency_p99_micros={latency_p99:.2f}"
    )
    print(f"summary_csv={summary_path}")
    print(f"runs_csv={runs_path}")


if __name__ == "__main__":
    main()
