# fast-telemetry 0.7 Difference Report

Date: 2026-06-30

Branch: `dt/fast-telemetry-0.7`

Old commit: `b494c45`

New commit: `072eb54`

Update: a later commit on this branch changes the default
`HandoffReadTelemetryHandle` mode from direct counters to the grouped
`CounterSet` buffer. The measurements below isolate the earlier `072eb54`
state, where grouped counters were available but opt-in.

After the default switch, a short smoke run of the grouped default path produced
these telemetry-on results:

| workers | runs | duration | mean MiB/s | mean CPU cores | mean ns/B | mean p99 us | result directory |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 3 | 2 s | 274.84 | 0.85 | 3.03 | 5246.33 | `bench/results/stream_cached_handoff_fragmented_ticket_20260630_174621_97527` |
| 8 | 3 | 2 s | 1100.80 | 7.27 | 6.30 | 3937.00 | `bench/results/stream_cached_handoff_fragmented_ticket_20260630_174635_98147` |

The single-worker smoke run was noisy and should not be treated as a final
comparison. Re-run the full matrix against the latest branch head when making a
release decision on the default grouped-counter path.

## Goal

Isolate the benchmark difference between the old `fast-telemetry 0.6` read
telemetry integration and the new `fast-telemetry 0.7.1` integration. The useful
comparison is not just old telemetry-on vs new telemetry-on, because that folds
in baseline drift. This report compares telemetry-on against telemetry-off
inside each commit, then compares those overheads.

## Workload

All runs used the stream harness cached workload:

```bash
./bench/run-stream-harness.sh \
  --transport cached \
  --implementation handoff \
  --scenario fragmented \
  --completion ticket \
  --duration-seconds 5 \
  --runs 5 \
  --worker-threads N \
  --connections C
```

Telemetry rows add `--read-telemetry`.

The single-worker shape used `--worker-threads 1 --connections 16`. The
contention shape used `--worker-threads 8 --connections 64`.

## Result Directories

Old commit results were produced from
`/private/tmp/bytes-handoff-existing-telemetry-b494c45`.

| label | result directory |
| --- | --- |
| old 1 worker, telemetry off | `/private/tmp/bytes-handoff-existing-telemetry-b494c45/bench/results/stream_cached_handoff_fragmented_ticket_20260630_173246_26524` |
| old 1 worker, telemetry on | `/private/tmp/bytes-handoff-existing-telemetry-b494c45/bench/results/stream_cached_handoff_fragmented_ticket_20260630_173322_34072` |
| new 1 worker, telemetry off | `bench/results/stream_cached_handoff_fragmented_ticket_20260630_173355_36492` |
| new 1 worker, telemetry on | `bench/results/stream_cached_handoff_fragmented_ticket_20260630_173429_39465` |
| old 8 workers, telemetry off | `/private/tmp/bytes-handoff-existing-telemetry-b494c45/bench/results/stream_cached_handoff_fragmented_ticket_20260630_173503_40156` |
| old 8 workers, telemetry on | `/private/tmp/bytes-handoff-existing-telemetry-b494c45/bench/results/stream_cached_handoff_fragmented_ticket_20260630_173533_42093` |
| new 8 workers, telemetry off | `bench/results/stream_cached_handoff_fragmented_ticket_20260630_173604_45050` |
| new 8 workers, telemetry on | `bench/results/stream_cached_handoff_fragmented_ticket_20260630_173636_47301` |

## Summary

| commit | workers | telemetry | mean MiB/s | median MiB/s | stdev MiB/s | mean CPU cores | mean ns/B | mean p99 us |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| old | 1 | off | 184.06 | 201.48 | 43.28 | 0.62 | 3.22 | 22467.80 |
| old | 1 | on | 245.59 | 193.84 | 76.19 | 0.79 | 3.16 | 6514.60 |
| new | 1 | off | 339.61 | 358.20 | 46.75 | 0.91 | 2.57 | 5689.40 |
| new | 1 | on | 360.28 | 363.27 | 4.57 | 0.96 | 2.53 | 3351.80 |
| old | 8 | off | 1594.52 | 1585.97 | 52.65 | 7.34 | 4.39 | 2839.40 |
| old | 8 | on | 1515.16 | 1529.62 | 34.75 | 7.34 | 4.62 | 2977.40 |
| new | 8 | off | 1240.75 | 1241.28 | 21.41 | 7.13 | 5.48 | 3570.20 |
| new | 8 | on | 1172.41 | 1178.04 | 28.43 | 7.22 | 5.87 | 3814.60 |

## Telemetry Overhead

Throughput overhead is computed as:

```text
(telemetry_on_mib_per_sec - telemetry_off_mib_per_sec) / telemetry_off_mib_per_sec
```

Negative values are throughput drops.

| workers | old mean overhead | new mean overhead | mean delta-of-deltas | old median overhead | new median overhead | median delta-of-deltas |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | +33.43% | +6.09% | -27.34 pp | -3.79% | +1.42% | +5.21 pp |
| 8 | -4.98% | -5.51% | -0.53 pp | -3.55% | -5.09% | -1.54 pp |

## Interpretation

The fresh 8-worker paired run shows the new telemetry path is essentially in
the same overhead band as the old path. Mean throughput overhead changed by
`-0.53` percentage points, and median overhead changed by `-1.54` percentage
points. The earlier result with old telemetry at roughly `30 ms` p99 did not
reproduce here; the fresh old telemetry p99 was `2.98 ms`, while the fresh new
telemetry p99 was `3.81 ms`.

The single-worker result is less stable on this machine. The old single-worker
rows have large run-to-run movement, especially the old telemetry-on row, so the
mean comparison is not reliable. The median comparison is more useful: new
telemetry-on was `363.27 MiB/s` vs new telemetry-off at `358.20 MiB/s`, which is
within noise and does not show a single-worker regression.

There is a separate baseline shift in the 8-worker no-telemetry control:
`1594.52 MiB/s` old vs `1240.75 MiB/s` new by mean throughput. Because telemetry
is disabled in both rows, that shift should not be attributed to the
`fast-telemetry` recording path. The source diff outside `src/read_telemetry.rs`
is small, but `src/read.rs` does change a few methods from shared to mutable
receivers so telemetry buffers can flush and record through `&mut self`. If the
8-worker no-telemetry shift persists under a more controlled run, that should be
isolated separately with either a revert experiment or Criterion
`read_telemetry_cost` comparisons.

## Raw Throughput Samples

| label | MiB/s samples |
| --- | --- |
| old 1 worker, telemetry off | `110.49`, `203.87`, `201.48`, `214.00`, `190.45` |
| old 1 worker, telemetry on | `184.51`, `193.84`, `189.65`, `327.32`, `332.63` |
| new 1 worker, telemetry off | `375.63`, `358.20`, `283.39`, `297.15`, `383.66` |
| new 1 worker, telemetry on | `353.59`, `356.72`, `364.19`, `363.27`, `363.61` |
| old 8 workers, telemetry off | `1659.50`, `1518.96`, `1585.97`, `1622.86`, `1585.33` |
| old 8 workers, telemetry on | `1545.62`, `1507.84`, `1458.87`, `1529.62`, `1533.84` |
| new 8 workers, telemetry off | `1206.69`, `1253.10`, `1262.14`, `1240.56`, `1241.28` |
| new 8 workers, telemetry on | `1182.39`, `1203.17`, `1178.04`, `1171.98`, `1126.47` |
