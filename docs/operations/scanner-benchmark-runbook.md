# Scanner Benchmark Runbook

This runbook describes how to collect reproducible evidence for scanner
pressure, scanner progress, and scanner runtime tuning. It is intended for
maintainers and operators validating scanner changes in an isolated test
deployment.

Use this runbook together with
[Scanner Runtime Controls](scanner-runtime-controls.md). The runtime controls
document explains each field and configuration key; this document explains how
to run a comparable before/after observation and decide whether the scanner is
healthier after tuning.

## Scope

This runbook verifies that scanner pacing and cycle controls reduce background
pressure while preserving maintenance progress. It is useful for:

- mostly idle single-node, single-disk deployments with many small objects;
- single-node multi-disk or erasure-set deployments where scanner work is
  spread across disks and sets;
- distributed clusters where scanner pressure is mixed with lifecycle,
  replication, heal, or bitrot queues;
- lifecycle expiry or transition backlog investigations;
- bucket replication repair backlog investigations;
- scanner-originated heal or bitrot admission investigations;
- pull request evidence when scanner behavior, scanner status, or scanner
  controls change.

This runbook does not prove full MinIO parity, site-replication correctness,
or replaced-disk heal correctness. Those flows need dedicated distributed
tests because their failure modes are not limited to scanner pacing.

## Safety

Run the workload only in a disposable test environment. The commands below can
create many buckets and objects and can overwrite runtime scanner settings.

Record the current scanner and heal configuration before changing anything:

```bash
mkdir -p artifacts
mc admin config get ALIAS scanner > artifacts/scanner-config.before.txt
mc admin config get ALIAS heal > artifacts/heal-config.before.txt
```

Replace `ALIAS`, endpoint, and credentials with values for the test
deployment. Do not paste production credentials into saved artifacts.

## Required Tools

- `mc` or a compatible admin client for config changes.
- `awscurl` or another SigV4-capable HTTP client for `/v3/scanner/status`.
- `jq` for status extraction.
- `pidstat`, `mpstat`, `iostat`, `top`, or equivalent host telemetry.
- `warp`, `s3bench`, or the repository object benchmark scripts for workload
  generation.

## Test Matrix

At minimum, collect two runs on the same RustFS commit and the same workload:

| Run | Purpose | Example scanner settings |
|---|---|---|
| Baseline | Observe current behavior without additional pacing changes. | Existing config. |
| Pacing override | Measure whether cooperative scanner sleeps reduce pressure. | `scanner.delay="30"` and `scanner.max_wait="15"`. |

Add bounded-cycle runs when a single scan cycle is too long:

| Run | Purpose | Example scanner settings |
|---|---|---|
| Duration budget | Bound wall-clock time per cycle. | `scanner.cycle_max_duration="1800"`. |
| Object budget | Bound objects processed per cycle. | `scanner.cycle_max_objects="1000000"`. |
| Directory budget | Bound directories entered per cycle. | `scanner.cycle_max_directories="100000"`. |

When comparing runs, keep hardware, RustFS commit, object count, object size,
bucket count, scanner-enabled state, and foreground workload constant.

## Deployment Matrix

Use the smallest deployment that reproduces the symptom, but do not treat
single-node validation as the whole scanner test surface. Scanner changes that
touch queues, admission, or cross-node maintenance should include a distributed
run when practical.

| Deployment | What it validates | Minimum evidence |
|---|---|---|
| Single-node, single-disk | Small-object scanner pressure, pacing, cycle interval, and basic progress. | Scanner status time series plus host CPU and disk telemetry. |
| Single-node, multi-disk or erasure set | Set and disk scan concurrency, cycle budgets, checkpoint movement, usage cache persistence, and active path age. | Scanner status time series, per-disk host telemetry, and before/after data usage freshness. |
| Distributed cluster | Lifecycle transition queues, bucket replication repair admission, scanner-originated heal and bitrot admission, and queue/backlog pressure under cross-node work. | Scanner status time series from the cluster, host telemetry from each node, and subsystem-specific queued/skipped/missed counters. |

The single-node, single-disk run is the baseline because it is cheap and
repeatable. It is not sufficient for PRs that claim to improve distributed
queue behavior, replication repair, or heal/bitrot admission.

## Workload Shape

For the baseline small-object scanner-pressure run, use a workload that
creates many small objects and then leaves the service mostly idle while the
scanner walks the namespace. A useful minimum shape is:

- one RustFS node;
- one data disk;
- several buckets;
- at least 100,000 small objects total;
- scanner enabled;
- no sustained foreground workload during the observation window.

For a distributed backlog run, use the same structure but add the relevant
subsystem condition, such as lifecycle rules, a bucket replication target, or
a configured heal/bitrot scenario. Keep the status and host telemetry cadence
the same so the result remains comparable with the baseline run.

The repository object benchmark script can generate object traffic if `warp`
or `s3bench` is installed:

```bash
scripts/run_object_batch_bench.sh \
  --tool warp \
  --endpoint http://127.0.0.1:9000 \
  --access-key "$RUSTFS_ACCESS_KEY" \
  --secret-key "$RUSTFS_SECRET_KEY" \
  --bucket scanner-bench \
  --auto-new-bucket \
  --concurrency 64 \
  --duration 10m \
  --sizes 1KiB,4KiB,16KiB \
  --warp-mode put \
  --out-dir artifacts/object-load
```

If the object generator cannot create enough objects in one run, repeat the
same command with new buckets or prefixes and record the final object count.

## Status Collection

Capture scanner status before the workload, after the workload finishes, and
throughout the idle observation window.

The repository includes a scanner validation harness for repeatable collection:

```bash
export RUSTFS_ACCESS_KEY="<admin-access-key>"
export RUSTFS_SECRET_KEY="<admin-secret-key>"

scripts/run_scanner_validation_harness.sh \
  --alias ALIAS \
  --endpoint http://127.0.0.1:9000 \
  --deployment single-disk \
  --workload-label small-object-idle \
  --samples 30 \
  --interval-secs 60 \
  --out-dir artifacts/scanner-validation
```

The harness writes scanner/heal config snapshots, scanner status samples, host
telemetry when available, run metadata, and `scanner-summary.csv`.

For distributed runs, capture scanner admin metrics from every node with
`by-host=true`. The metrics endpoint reports the node that handles the request;
`by-host=true` preserves that node's host view but does not collect peer nodes.
These per-node artifacts include active path age, checkpoint state, pacing
pressure, source work, and queued/skipped/missed downstream admission counters.

```bash
for endpoint in http://node-a:9000 http://node-b:9000 http://node-c:9000; do
  node="${endpoint#http://}"
  node="${node%%:*}"
  awscurl \
    --service s3 \
    --region us-east-1 \
    --access_key "$RUSTFS_ACCESS_KEY" \
    --secret_key "$RUSTFS_SECRET_KEY" \
    --request GET \
    "${endpoint}/rustfs/admin/v3/metrics?types=1&by-host=true&n=1" \
    > "artifacts/scanner-metrics.${node}.$(date -u +%Y%m%dT%H%M%SZ).ndjson"
done
```

Example status request:

```bash
awscurl \
  --service s3 \
  --region us-east-1 \
  --access_key "$RUSTFS_ACCESS_KEY" \
  --secret_key "$RUSTFS_SECRET_KEY" \
  --request GET \
  'http://127.0.0.1:9000/rustfs/admin/v3/scanner/status' \
  | jq . > "artifacts/scanner-status.$(date -u +%Y%m%dT%H%M%SZ).json"
```

For a time series, sample once per minute:

```bash
mkdir -p artifacts/status
while sleep 60; do
  ts="$(date -u +%Y%m%dT%H%M%SZ)"
  awscurl \
    --service s3 \
    --region us-east-1 \
    --access_key "$RUSTFS_ACCESS_KEY" \
    --secret_key "$RUSTFS_SECRET_KEY" \
    --request GET \
    'http://127.0.0.1:9000/rustfs/admin/v3/scanner/status' \
    | jq . > "artifacts/status/scanner-status.${ts}.json"
done
```

Stop the loop after the planned observation window.

## Host Telemetry

Collect host metrics over the same window as scanner status.

```bash
pidstat -p "$(pidof rustfs)" 60 > artifacts/pidstat.txt
iostat -xz 60 > artifacts/iostat.txt
mpstat 60 > artifacts/mpstat.txt
```

If `pidstat` is not available, use `top`, `ps`, or the platform monitoring
system, but keep the sampling interval and observation window in the report.

## Runtime Tuning Examples

Persistent scanner config values use seconds for time fields. Use numeric
strings instead of duration suffixes:

```bash
mc admin config set ALIAS scanner delay="30" max_wait="15"
mc admin config set ALIAS scanner cycle="3600"
mc admin config set ALIAS scanner cycle_max_duration="1800"
mc admin config set ALIAS scanner cycle_max_objects="1000000"
mc admin config set ALIAS scanner cycle_max_directories="100000"
```

The canonical persistent bitrot cadence belongs to the `heal` subsystem:

```bash
mc admin config set ALIAS heal bitrot_cycle="2592000"
```

Environment variables take precedence over persisted config and should be
recorded separately:

```bash
RUSTFS_SCANNER_DELAY=30
RUSTFS_SCANNER_MAX_WAIT_SECS=15
RUSTFS_SCANNER_CYCLE=3600
RUSTFS_SCANNER_CYCLE_MAX_DURATION_SECS=1800
RUSTFS_SCANNER_CYCLE_MAX_OBJECTS=1000000
RUSTFS_SCANNER_CYCLE_MAX_DIRECTORIES=100000
RUSTFS_SCANNER_BITROT_CYCLE_SECS=2592000
```

After each config change, read scanner status and confirm the effective value
and `source` under `runtime_config`.

## Observation Window

Use the same window for each run:

1. Generate or verify the object namespace.
2. Wait until foreground workload is idle.
3. Save scanner and heal config.
4. Save one scanner status snapshot.
5. Collect scanner status and host telemetry for at least 30 minutes, or for
   one complete scanner cycle when that is practical.
6. Save one final scanner status snapshot.

Longer windows are better for cycle interval comparisons. Short windows are
acceptable for quick pressure checks only if the conclusion avoids changing
defaults.

## Fields To Compare

Compare these fields between baseline and tuned runs:

| Field | Why it matters |
|---|---|
| `runtime_config.*.value` and `runtime_config.*.source` | Confirms the tested settings actually took effect. |
| `metrics.pacing_pressure.primary_pressure` | Shows whether pressure is from queues, budgets, pause activity, active scans, or no scanner pressure. |
| `metrics.pacing_pressure.last_cycle_total_pause_ratio` | Shows how much of the last cycle was cooperative scanner pause time. |
| `metrics.current_cycle_objects_scanned` | Confirms object scan progress during the current cycle. |
| `metrics.current_cycle_directories_scanned` | Confirms directory walk progress during the current cycle. |
| `metrics.last_cycle_result` | Confirms whether the previous cycle completed, stopped partially, or failed. |
| `metrics.last_cycle_partial_reason` | Shows which budget stopped a partial cycle. |
| `metrics.last_cycle_partial_source` | Shows which scanner work source consumed the stopping budget. |
| `metrics.source_work` | Shows cumulative work found, queued, skipped, missed, executed, and failed by source. |
| `metrics.current_cycle_source_work` | Shows which source is consuming the current scan cycle. |
| `metrics.last_cycle_source_work` | Shows which source consumed the previous scan cycle. |
| `metrics.lifecycle_transition.scanner_missed` | Shows scanner-discovered transition work that could not be queued. |
| `metrics.lifecycle_transition.queue_full` | Shows transition queue pressure outside the scanner walk itself. |
| `metrics.scan_checkpoint` | Confirms partial cycles preserve resume context. |
| `metrics.oldest_active_path_age_seconds` | Helps identify scanner paths that may be stuck. |

Do not use a single CPU spike as the conclusion. Compare average and p95 CPU
over the same observation window.

## Interpreting Results

A useful tuning result has all of these properties:

- average or p95 scanner-related CPU and disk pressure decreases;
- `current_cycle_objects_scanned` or `current_cycle_directories_scanned`
  continues to advance;
- `source_work.missed` does not grow unexpectedly for lifecycle, replication,
  heal, or bitrot;
- `last_cycle_result` is either `success` or a partial result with a clear
  budget reason and checkpoint;
- data usage freshness remains acceptable for the tested deployment.

Treat these as failure signals:

- CPU drops only because the scanner stops making progress;
- `primary_pressure` stays at `queued_scans` while queues grow;
- `last_cycle_partial_reason` repeats forever with no checkpoint movement;
- lifecycle transition `scanner_missed` or `queue_full` grows during a run that
  was expected to reduce backlog;
- heal or bitrot work moves from `queued` to `missed` after a scanner pacing
  change.

## PR Evidence Checklist

For scanner behavior PRs, include this evidence when available:

- RustFS commit SHA and branch.
- Deployment shape: node count, disk count, disk type, CPU count, memory, and
  object count.
- Workload command or script and benchmark artifact path.
- Scanner and heal config before and after tuning.
- Observation window and sample interval.
- Scanner status snapshots or time series.
- Host CPU and disk telemetry.
- Short conclusion that separates pressure reduction from scanner progress.

For documentation-only PRs, it is enough to verify links and formatting.
