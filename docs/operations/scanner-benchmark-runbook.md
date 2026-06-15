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

The harness writes scanner/heal config snapshots, scanner status samples,
background heal status samples, host telemetry when available, run metadata,
`scanner-summary.csv`, and `scanner-validation-report.md`.

Use `--metrics-endpoints` when the validation needs per-node distributed
evidence. The value is a comma-separated list of RustFS endpoints:

```bash
scripts/run_scanner_validation_harness.sh \
  --alias ALIAS \
  --endpoint http://node-a:9000 \
  --deployment distributed \
  --workload-label lifecycle-replication-heal-backlog \
  --metrics-endpoints http://node-a:9000,http://node-b:9000,http://node-c:9000,http://node-d:9000 \
  --samples 30 \
  --interval-secs 60 \
  --out-dir artifacts/scanner-validation-distributed
```

Each sample stores `/v3/scanner/status`,
`/v3/background-heal/status`, and one by-host admin metrics response per
endpoint listed in `--metrics-endpoints`.

For distributed runs, capture scanner admin metrics from every node with
`by-host=true`. The metrics endpoint reports the node that handles the request;
`by-host=true` preserves that node's host view but does not collect peer nodes.
These per-node artifacts include active path age, checkpoint state, pacing
pressure, source work, and queued/skipped/missed downstream admission counters.
The validation harness can collect these artifacts automatically with
`--metrics-endpoints`; the manual loop below is useful when adding extra nodes
or collecting ad hoc snapshots outside the harness window.

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
| `metrics.maintenance_control.primary_control` | Shows whether source-level maintenance is blocked, deferred, active, only pacing-limited, or idle. |
| `metrics.maintenance_control.sources` | Shows the source, state, reason, backlog, current or last-cycle missed work, and partial-cycle count for each scanner maintenance source. |
| `metrics.current_cycle_objects_scanned` | Confirms object scan progress during the current cycle. |
| `metrics.current_cycle_directories_scanned` | Confirms directory walk progress during the current cycle. |
| `metrics.last_cycle_result` | Confirms whether the previous cycle completed, stopped partially, or failed. |
| `metrics.last_cycle_partial_reason` | Shows which budget stopped a partial cycle. |
| `metrics.last_cycle_partial_source` | Shows which scanner work source consumed the stopping budget. |
| `metrics.source_work` | Shows cumulative work found, queued, skipped, missed, executed, and failed by source. |
| `metrics.current_cycle_source_work` | Shows which source is consuming the current scan cycle. |
| `metrics.last_cycle_source_work` | Shows which source consumed the previous scan cycle. |
| `metrics.replication_repair` | Splits scanner-discovered replication repair by source and kind, including bucket object, delete-marker, version-purge, existing-object repair, and site replication boundary states. |
| `metrics.current_cycle_replication_repair` | Shows which replication repair kind is being discovered or admitted in the current cycle. |
| `metrics.last_cycle_replication_repair` | Shows which replication repair kind consumed the previous cycle. |
| `metrics.lifecycle_expiry.current_queued` | Shows scanner-driven expiry/delete work waiting in the expiry worker queue. |
| `metrics.lifecycle_expiry.current_active` | Shows scanner-driven expiry/delete work currently running in expiry workers. |
| `metrics.lifecycle_expiry.queue_missed` | Shows expiry/delete queue admission failures outside the scanner walk itself. |
| `metrics.lifecycle_expiry.scanner_missed` | Shows scanner-discovered expiry/delete work that could not be queued. |
| `metrics.lifecycle_transition.scanner_missed` | Shows scanner-discovered transition work that could not be queued. |
| `metrics.lifecycle_transition.queue_full` | Shows transition queue pressure outside the scanner walk itself. |
| `metrics.lifecycle_transition.compensation_pending` | Shows transition compensation still pending or running after queue pressure. |
| `metrics.lifecycle_transition.failed` | Shows transition worker failures, which should also surface as lifecycle source failure. |
| `metrics.scan_checkpoint` | Confirms partial cycles preserve resume context. |
| `metrics.oldest_active_path_age_seconds` | Helps identify scanner paths that may be stuck. |

Do not use a single CPU spike as the conclusion. Compare average and p95 CPU
over the same observation window.

For heal or bitrot pressure investigations, also capture
`/v3/background-heal/status` and compare `healOperations.queueLength`,
`healOperations.activeTasks`, `healOperations.queuedBySource`,
`healOperations.activeBySource`, `healOperations.queuedByPriority`, and
`healOperations.activeByPriority`. These fields distinguish scanner-submitted
low-priority work from manual admin heal and auto-heal work.

`scanner-summary.csv` includes the heal operation totals needed for quick
before/after comparison:

| Field | Why it matters |
|---|---|
| `heal_queue_length` | Total queued heal requests at the same timestamp as the scanner status sample. |
| `heal_active_tasks` | Total running heal tasks. |
| `heal_scanner_queued` | Scanner-submitted heal or bitrot work waiting in the queue. |
| `heal_admin_queued` | Manual/admin heal work waiting in the queue. |
| `heal_auto_heal_queued` | Auto-heal work waiting in the queue, typically from disk/set recovery paths. |

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
- lifecycle expiry `queue_missed`, `scanner_missed`, `current_queued`, or
  `current_active` grows during a run that was expected to reduce expiry
  backlog;
- lifecycle transition `scanner_missed`, `queue_full`,
  `compensation_pending`, or `failed` grows during a run that was expected to
  reduce backlog;
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
- `scanner-validation-report.md` from the harness when using the scripted
  collection path.

## Final Parity Validation Closure

Use the final validation run to prove the scanner control plane is coherent,
not to introduce new runtime behavior. A complete closure package should have
at least these runs:

| Run | Required evidence |
|---|---|
| Single-node, single-disk small-object idle | Scanner status series, host telemetry, `scanner-summary.csv`, and a conclusion that CPU or disk pressure is lower without scan progress stopping. |
| Single-node erasure or multi-disk | Checkpoint movement, active path age, set/disk scan pressure, data usage freshness, and before/after scanner config. |
| Distributed lifecycle backlog | `maintenance_control`, lifecycle expiry/transition queue fields, source work missed/failed counts, and by-host admin metrics. |
| Distributed replication backlog | Bucket replication repair kind counters, site replication passive/active boundary counters, source work queued/skipped/missed counts, and by-host admin metrics. |
| Heal or bitrot pressure | Background heal `healOperations` queued/active source and priority counts, scanner source work for heal/bitrot, and by-host admin metrics. |

The expected conclusion is MinIO-style scanner behavior at the operational
contract level: scanner remains enabled, pacing is observable and adjustable,
partial progress is explainable, maintenance work is attributed by source, and
downstream lifecycle, replication, heal, and bitrot backlog can be diagnosed
without guessing from CPU usage alone.

For documentation-only PRs, it is enough to verify links and formatting.
