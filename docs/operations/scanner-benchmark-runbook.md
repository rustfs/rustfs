# Scanner Benchmark Runbook

**Use this when:** you need reproducible before/after evidence that a scanner pacing or cycle change reduces background pressure without stalling lifecycle, replication, heal, or bitrot progress, or you are assembling evidence for a scanner-behavior PR.

**Source of truth:** `scripts/run_scanner_validation_harness.sh` (collection, `scanner-summary.csv` columns), `scripts/run_object_batch_bench.sh` (workload), and [Scanner Runtime Controls](scanner-runtime-controls.md) for the meaning of every status field and configuration key.

## Scope

This runbook verifies that scanner pacing and cycle controls reduce background pressure while preserving maintenance progress. It covers mostly idle single-node deployments with many small objects, multi-disk or erasure-set nodes, distributed clusters where scanner pressure mixes with lifecycle, replication, heal, or bitrot queues, and backlog investigations for any of those subsystems.

It does not prove full MinIO parity, site-replication correctness, or replaced-disk heal correctness. Those flows need dedicated distributed tests because their failure modes are not limited to scanner pacing.

## Safety

Run the workload only in a disposable test environment. The commands below can create many buckets and objects and overwrite runtime scanner settings. Record the current scanner and heal configuration before changing anything:

```bash
mkdir -p artifacts
mc admin config get ALIAS scanner > artifacts/scanner-config.before.txt
mc admin config get ALIAS heal > artifacts/heal-config.before.txt
```

The `scanner` and `heal` subsystems are served by `GetConfigKVHandler` (`rustfs/src/admin/handlers/config_admin.rs`, route `/v3/get-config-kv`); this was confirmed by code inspection, not by running `mc` against a live deployment. Replace `ALIAS`, endpoint, and credentials with values for the test deployment. Do not paste production credentials into saved artifacts.

## Required Tools

| Tool | Purpose |
|---|---|
| `mc` or a compatible admin client | Config snapshots and changes. |
| `awscurl` or another SigV4-capable HTTP client | `/v3/scanner/status` and admin metrics. |
| `jq` | Status extraction. |
| `pidstat`, `mpstat`, `iostat`, `top`, or equivalent | Host telemetry. |
| `warp`, `s3bench`, or `scripts/run_object_batch_bench.sh` | Workload generation. |

## Test Matrix

Collect at least two runs on the same RustFS commit and the same workload. Keep hardware, commit, object count, object size, bucket count, scanner-enabled state, and foreground workload constant between runs.

| Run | Purpose | Example scanner settings |
|---|---|---|
| Baseline | Observe current behavior without additional pacing changes. | Existing config. |
| Pacing override | Measure whether cooperative scanner sleeps reduce pressure. | `scanner.delay="30"` and `scanner.max_wait="15"`. |
| Duration budget (when one cycle is too long) | Bound wall-clock time per cycle. | `scanner.cycle_max_duration="1800"`. |
| Object budget | Bound objects processed per cycle. | `scanner.cycle_max_objects="1000000"`. |
| Directory budget | Bound directories entered per cycle. | `scanner.cycle_max_directories="100000"`. |

## Deployment Matrix

Use the smallest deployment that reproduces the symptom. The single-node, single-disk run is the cheap, repeatable baseline; it is not sufficient for PRs that claim to improve distributed queue behavior, replication repair, or heal/bitrot admission.

| Deployment | What it validates | Minimum evidence | Workload shape |
|---|---|---|---|
| Single-node, single-disk | Small-object scanner pressure, pacing, cycle interval, basic progress. | Scanner status time series plus host CPU and disk telemetry. | One node, one data disk, several buckets, at least 100,000 small objects, scanner enabled, no sustained foreground workload during observation. |
| Single-node, multi-disk or erasure set | Set and disk scan concurrency, cycle budgets, checkpoint movement, usage cache persistence, active path age. | Scanner status time series, per-disk host telemetry, before/after data usage freshness. | Same as above across all disks. |
| Distributed cluster | Lifecycle transition queues, bucket replication repair admission, scanner-originated heal and bitrot admission, queue/backlog pressure under cross-node work. | Scanner status time series from the cluster, host telemetry from each node, subsystem-specific queued/skipped/missed counters. | Same structure plus the relevant subsystem condition (lifecycle rules, a replication target, a heal/bitrot scenario); keep status and telemetry cadence identical to the baseline. |

Generate object traffic with the repository script if `warp` or `s3bench` is installed; repeat with new buckets or prefixes if one run cannot create enough objects, and record the final object count:

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

## Status Collection

Capture scanner status before the workload, after the workload finishes, and throughout the idle observation window. The validation harness does this repeatably and writes scanner/heal config snapshots, scanner status samples, background heal status samples, host telemetry when available, run metadata, `scanner-summary.csv`, and `scanner-validation-report.md`:

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

For per-node distributed evidence pass `--metrics-endpoints` (comma-separated). Each sample then stores `/v3/scanner/status`, one `/v3/background-heal/status` response per listed endpoint, and one by-host admin metrics response per listed endpoint; without it, background-heal status is captured only from `--endpoint`:

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

For ad hoc per-node snapshots outside the harness window, use the by-host `awscurl` loop in [Reading Distributed Metrics](scanner-runtime-controls.md#reading-distributed-metrics); the metrics endpoint reports only the node that handles the request.

### Bucket metrics freshness validation

Use the harness around a post-start bucket creation workload to cover the timing where scanner startup sees no buckets, a bucket is created afterwards, and the first metrics collection must not confuse a cold usage cache with real zero usage:

1. Start RustFS from an empty data path.
2. Start the harness before creating buckets.
3. Create a bucket, upload objects, and keep the harness running until at least one usage save is observed.
4. Compare `scanner-summary.csv` with `/rustfs/admin/v3/metrics?types=1&n=1` bucket metrics.

Expected evidence: dirty usage is marked, `life_time_scan_cycle` or `life_time_scan_bucket_drive` advances, `life_time_scan_object` advances for object workloads, and `life_time_save_usage` plus `usage_last_save_result=success` appear before non-zero bucket usage metrics are accepted as fresh.

### Manual status sampling

Single snapshot:

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

Time series (stop after the planned observation window):

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

## Host Telemetry

Collect host metrics over the same window as scanner status. If `pidstat` is unavailable, use `top`, `ps`, or the platform monitoring system, but record the sampling interval and window in the report.

```bash
pidstat -p "$(pidof rustfs)" 60 > artifacts/pidstat.txt
iostat -xz 60 > artifacts/iostat.txt
mpstat 60 > artifacts/mpstat.txt
```

## Runtime Tuning Examples

Persistent scanner config values use seconds for time fields; use numeric strings, not duration suffixes. The canonical persistent bitrot cadence belongs to the `heal` subsystem.

```bash
mc admin config set ALIAS scanner delay="30" max_wait="15"
mc admin config set ALIAS scanner cycle="3600"
mc admin config set ALIAS scanner cycle_max_duration="1800"
mc admin config set ALIAS scanner cycle_max_objects="1000000"
mc admin config set ALIAS scanner cycle_max_directories="100000"
mc admin config set ALIAS heal bitrot_cycle="2592000"
```

Environment variables take precedence over persisted config and should be recorded separately:

```bash
RUSTFS_SCANNER_DELAY=30
RUSTFS_SCANNER_MAX_WAIT_SECS=15
RUSTFS_SCANNER_CYCLE=3600
RUSTFS_SCANNER_CYCLE_MAX_DURATION_SECS=1800
RUSTFS_SCANNER_CYCLE_MAX_OBJECTS=1000000
RUSTFS_SCANNER_CYCLE_MAX_DIRECTORIES=100000
RUSTFS_SCANNER_BITROT_CYCLE_SECS=2592000
```

After each config change, read scanner status and confirm the effective value and `source` under `runtime_config`.

## Observation Window

Use the same window for each run:

1. Generate or verify the object namespace.
2. Wait until foreground workload is idle.
3. Save scanner and heal config.
4. Save one scanner status snapshot.
5. Collect scanner status and host telemetry for at least 30 minutes, or for one complete scanner cycle when practical.
6. Save one final scanner status snapshot.

Longer windows are better for cycle interval comparisons. Short windows are acceptable for quick pressure checks only if the conclusion avoids changing defaults.

## Fields To Compare

Field semantics are defined in [Scanner Runtime Controls](scanner-runtime-controls.md); the decision fields for a before/after comparison are:

| Field | Decision it supports |
|---|---|
| `runtime_config.*.value` and `runtime_config.*.source` | The tested settings actually took effect. |
| `metrics.pacing_pressure.primary_pressure`, `last_cycle_total_pause_ratio` | Where pressure comes from and how much of the cycle was cooperative pause. |
| `metrics.maintenance_control.primary_control`, `metrics.maintenance_control.sources` | Whether a maintenance source is blocked, deferred, active, or only pacing-limited. |
| `metrics.current_cycle_objects_scanned`, `metrics.current_cycle_directories_scanned` | Scan progress continues. |
| `metrics.last_cycle_result`, `last_cycle_partial_reason`, `last_cycle_partial_source` | Whether the previous cycle completed, which budget stopped it, and which source consumed it. |
| `metrics.source_work`, `metrics.current_cycle_source_work`, `metrics.last_cycle_source_work` | `missed` growth per source is a downstream admission problem, not pacing. |
| `metrics.replication_repair` (and current/last-cycle variants) | Repair kind, `scanner_role`, and `execution_owner` for replication backlog runs. |
| `metrics.lifecycle_expiry.{current_queued,current_active,queue_missed,scanner_missed}` | Expiry backlog and admission failures. |
| `metrics.lifecycle_transition.{scanner_missed,queue_full,compensation_pending,failed}` | Transition backlog, queue pressure, and worker failures. |
| `metrics.usage_freshness.*`, `metrics.current_cycle_usage_saves`, `metrics.last_cycle_usage_saves` | Bucket metrics freshness; `last_usage_save_result` must be `success`. |
| `metrics.life_time_ops.{scan_cycle,scan_bucket_drive,scan_object,save_usage}` | Cycles, bucket-drive scans, object scans, and `DataUsageInfo` saves actually happened after the workload. |
| `metrics.scan_checkpoint`, `metrics.oldest_active_path_age_seconds` | Partial cycles preserve resume context; stuck paths. |

Do not use a single CPU spike as the conclusion; compare average and p95 CPU over the same observation window.

For heal or bitrot pressure investigations, also capture `/v3/background-heal/status` from every distributed endpoint and compare `healOperations.queueLength`, `activeTasks`, `queuedBySource`, `activeBySource`, `queuedByPriority`, and `activeByPriority` (see [Reading Heal Operations](scanner-runtime-controls.md#reading-heal-operations)).

### `scanner-summary.csv` columns

In distributed runs the heal columns are aggregated from the background-heal snapshots captured across `--metrics-endpoints`.

| Column | Meaning |
|---|---|
| `heal_queue_length` | Total queued heal requests at the same timestamp as the scanner status sample. |
| `heal_active_tasks` | Total running heal tasks. |
| `heal_scanner_queued` | Scanner-submitted heal or bitrot work waiting in the queue. |
| `heal_admin_queued` | Manual/admin heal work waiting in the queue. |
| `heal_auto_heal_queued` | Auto-heal work waiting in the queue, typically from disk/set recovery paths. |
| `current_cycle_usage_saves` | Usage saves during the current cycle. |
| `last_cycle_usage_saves` | Usage saves from the last finished or partial cycle. |
| `usage_dirty_pending_buckets` | Dirty buckets still waiting for scanner refresh. |
| `usage_last_cycle_dirty_buckets` | Dirty buckets selected by the last cycle. |
| `usage_last_cycle_cleared_dirty_buckets` | Dirty bucket marks cleared by the last cycle. |
| `usage_last_save_result` | Last `DataUsageInfo` save result. |
| `usage_last_save_unix_secs` | Last `DataUsageInfo` save timestamp. |
| `life_time_scan_cycle` | Total scanner cycles observed by the node. |
| `life_time_scan_bucket_drive` | Total bucket-drive scans completed by the node. |
| `life_time_scan_object` | Total object scan operations observed by the node. |
| `life_time_save_usage` | Total usage save operations observed by the node. |

## Interpreting Results

A useful tuning result has all of these properties:

- average or p95 scanner-related CPU and disk pressure decreases;
- `current_cycle_objects_scanned` or `current_cycle_directories_scanned` continues to advance;
- `source_work.missed` does not grow unexpectedly for lifecycle, replication, heal, or bitrot;
- `last_cycle_result` is either `success` or a partial result with a clear budget reason and checkpoint;
- data usage freshness remains acceptable for the tested deployment.

Treat these as failure signals:

| Signal | Reading |
|---|---|
| CPU drops only because the scanner stops making progress | Not a tuning win. |
| `primary_pressure` stays at `queued_scans` while queues grow | Concurrency, not pacing, is the constraint. |
| `last_cycle_partial_reason` repeats forever with no checkpoint movement | Budget too small or checkpoint not advancing. |
| Lifecycle expiry `queue_missed`, `scanner_missed`, `current_queued`, or `current_active` grows during a run meant to reduce expiry backlog | Downstream expiry pressure. |
| Lifecycle transition `scanner_missed`, `queue_full`, `compensation_pending`, or `failed` grows during a run meant to reduce backlog | Downstream transition pressure. |
| Bucket metrics show zero usage after post-start uploads while dirty usage remains pending and `life_time_save_usage` does not advance | Usage freshness regression. |
| `bucket_replication` missed work with `scanner_role=repair_admission` grows while replication worker queues or target failures also grow | Downstream replication pressure, not only scanner pacing. |
| `site_replication` `active_resync` grows and is read as scanner-owned repair execution | Misreading: `scanner_role=boundary_signal` and `execution_owner=site_replication_runtime` mean active resync remains owned by the site replication runtime. |
| Heal or bitrot work moves from `queued` to `missed` after a scanner pacing change | Heal admission regression. |

## PR Evidence Checklist

For scanner behavior PRs, include when available:

- RustFS commit SHA and branch.
- Deployment shape: node count, disk count, disk type, CPU count, memory, object count.
- Workload command or script and benchmark artifact path.
- Scanner and heal config before and after tuning.
- Observation window and sample interval.
- Scanner status snapshots or time series.
- Host CPU and disk telemetry.
- Usage freshness fields from `scanner-summary.csv` when validating bucket metrics timing.
- A short conclusion that separates pressure reduction from scanner progress.
- `scanner-validation-report.md` from the harness when using the scripted collection path.
