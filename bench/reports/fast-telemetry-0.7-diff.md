# fast-telemetry 0.7 Difference Report

Date: 2026-06-30

Branch: `dt/fast-telemetry-0.7`

Old commit: `b494c45`

New commit: `072eb54`

Update: a later commit on this branch changes the default
`HandoffReadTelemetryHandle` mode from direct counters to the grouped
`CounterSet` buffer. A follow-up investigation found that the apparent large
8-worker regression was caused by the local, untracked `Cargo.lock` resolving
older benchmark dependencies in the new worktree (`bytes 1.11.1` and
`tokio 1.52.1`) than in the old worktree (`bytes 1.12.0` and `tokio 1.52.3`).
After `cargo update`, the no-telemetry baseline recovered.

The grouped-counter default was also retuned from flushing every `64` counter
operations to flushing every `1_024` counter operations. The stream harness now
has benchmark-only switches for direct counters and explicit grouped flush
intervals so this comparison can be repeated directly.

Corrected 8-worker head-to-head rerun after dependency lock update and the
`1_024` default flush interval:

| commit | workers | telemetry | mean MiB/s | median MiB/s | stdev MiB/s | mean CPU cores | mean ns/B | mean p99 us |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| old `b494c45` | 8 | off | 1643.84 | 1665.71 | 47.50 | 7.35 | 4.27 | 2778.60 |
| old `b494c45` | 8 | on | 1586.23 | 1592.66 | 18.38 | 7.45 | 4.48 | 2799.00 |
| new working tree | 8 | off | 1760.39 | 1771.44 | 53.75 | 7.45 | 4.04 | 2561.40 |
| new working tree | 8 | on, grouped default 1024 | 1678.84 | 1680.15 | 11.79 | 7.46 | 4.24 | 2676.60 |

Corrected 8-worker overheads:

| workers | old mean overhead | new mean overhead | mean delta-of-deltas | new on vs old on | new off vs old off |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | -3.50% | -4.63% | -1.13 pp | +5.84% | +7.09% |

Counter-mode tuning sweep on the new branch:

| telemetry counter mode | runs | mean MiB/s | median MiB/s | stdev MiB/s | mean ns/B | mean p99 us |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| direct counters | 3 | 1611.10 | 1614.23 | 67.11 | 4.43 | 2758.33 |
| grouped flush 1024 | 3 | 1637.10 | 1634.94 | 5.13 | 4.35 | 2749.00 |
| grouped flush 4096 | 3 | 1636.03 | 1630.96 | 60.19 | 4.35 | 2683.67 |
| grouped flush 8192 | 3 | 1613.68 | 1609.15 | 9.75 | 4.40 | 2791.67 |
| grouped default 1024 | 5 | 1678.84 | 1680.15 | 11.79 | 4.24 | 2676.60 |

The corrected result no longer shows a new baseline regression. The new
telemetry-on row is slightly higher than the old telemetry-on row, while the
remaining overhead is about one percentage point larger than the old path on
this workload. That remaining cost is expected to be dominated by the
read-size/buffered-size histograms and max gauge, which still record directly on
every read/consume event; grouped counters remove only the counter-update part
of the hot path.

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
| corrected old 8 workers, telemetry off | `/private/tmp/bytes-handoff-existing-telemetry-b494c45/bench/results/stream_cached_handoff_fragmented_ticket_20260630_180014_64382` |
| corrected old 8 workers, telemetry on | `/private/tmp/bytes-handoff-existing-telemetry-b494c45/bench/results/stream_cached_handoff_fragmented_ticket_20260630_180243_69604` |
| corrected new 8 workers, telemetry off | `bench/results/stream_cached_handoff_fragmented_ticket_20260630_180823_91590` |
| corrected new 8 workers, telemetry on, grouped default 1024 | `bench/results/stream_cached_handoff_fragmented_ticket_20260630_180746_90981` |
| new 8 workers, telemetry on, direct counters | `bench/results/stream_cached_handoff_fragmented_ticket_20260630_180557_88196` |
| new 8 workers, telemetry on, grouped flush 1024 | `bench/results/stream_cached_handoff_fragmented_ticket_20260630_180620_88468` |
| new 8 workers, telemetry on, grouped flush 4096 | `bench/results/stream_cached_handoff_fragmented_ticket_20260630_180640_89499` |
| new 8 workers, telemetry on, grouped flush 8192 | `bench/results/stream_cached_handoff_fragmented_ticket_20260630_180704_90020` |

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

The apparent baseline shift in that first 8-worker no-telemetry control was not
caused by the source changes. The new worktree had an untracked stale
`Cargo.lock` that resolved older benchmark dependencies than the old worktree.
After `cargo update`, the corrected new no-telemetry row rose to
`1760.39 MiB/s` and the corrected new default telemetry row rose to
`1678.84 MiB/s`.

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
| corrected old 8 workers, telemetry off | `1670.94`, `1665.71`, `1686.66`, `1627.66`, `1568.23` |
| corrected old 8 workers, telemetry on | `1597.05`, `1558.73`, `1592.66`, `1605.22`, `1577.47` |
| corrected new 8 workers, telemetry off | `1812.43`, `1809.07`, `1711.77`, `1771.44`, `1697.22` |
| corrected new 8 workers, telemetry on, grouped default 1024 | `1689.73`, `1677.80`, `1686.92`, `1680.15`, `1659.62` |
