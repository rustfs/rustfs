# Scanner Runtime Controls

**Use this when:** tuning scanner pacing or heal runtime knobs, reading `/v3/scanner/status`, deciding whether slow lifecycle/replication/heal progress is a scanner problem or a downstream queue problem, or migrating scanner settings from MinIO.

**Source of truth:** `crates/config/src/constants/scanner.rs` and `crates/config/src/constants/heal.rs` (keys, env names, `DEFAULT_*` constants), `crates/scanner/src/runtime_config.rs` (resolution order, parsing, hot update), `crates/scanner/src/scanner_folder.rs` (env-only scanner knobs, alerts), `crates/scanner/src/sleeper.rs` (throttling), `crates/heal/src/heal/manager.rs` (`HealConfig::default`), `crates/utils/src/envs.rs` (`EXTERNAL_COMPATIBLE_SUFFIXES`, MinIO env aliases).

For reproducible scanner-pressure validation and before/after evidence, see [Scanner Benchmark Runbook](scanner-benchmark-runbook.md). Alert thresholds and S3 event names are detailed in [Scanner Excess Alerts](scanner-excess-alerts.md).

## What the scanner does

The scanner is the background maintenance loop that walks stored objects and feeds usage accounting, lifecycle expiry and transition admission, bucket replication repair admission, scanner-originated heal and bitrot checks, and the namespace excess alerts. Slowing it reduces idle CPU and disk pressure but delays all of that work. Read the status fields below before changing cycle or pacing values.

## Configuration Sources

Scanner runtime config is resolved in this order:

1. Environment variables.
2. Persisted admin config for the `scanner` subsystem (`SCANNER_SUB_SYS`).
3. Built-in defaults or speed preset-derived values.

Bitrot cycle resolution differs because the canonical persistent key belongs to the `heal` subsystem:

1. `RUSTFS_SCANNER_BITROT_CYCLE_SECS`.
2. `heal.bitrot_cycle`.
3. Legacy compatibility key `scanner.bitrot_cycle`.
4. Built-in default.

`/v3/scanner/status` reports each effective runtime value with a `source` of `env`, `config`, `scanner_compat_config`, or `default`. Every key in the table below accepts admin config updates that take effect without a restart (`apply_scanner_runtime_config`).

## Runtime Controls

| Persistent key | Environment variable | Unit | Default (constant) | Effect |
|---|---|---:|---|---|
| `scanner.speed` | `RUSTFS_SCANNER_SPEED` | preset | `default` (`DEFAULT_SCANNER_SPEED`) | Selects the base pacing preset: `fastest`, `fast`, `default`, `slow`, or `slowest`. |
| `scanner.delay` | `RUSTFS_SCANNER_DELAY` | factor | preset-derived | Overrides the sleep multiplier. Valid range is `0` through `10000` (`MAX_SCANNER_DELAY_FACTOR`). |
| `scanner.max_wait` | `RUSTFS_SCANNER_MAX_WAIT_SECS` | seconds | preset-derived | Caps one scanner sleep. |
| `scanner.cycle` | `RUSTFS_SCANNER_CYCLE` | seconds | preset-derived | Sets the interval between scanner cycles. |
| `scanner.start_delay` | `RUSTFS_SCANNER_START_DELAY_SECS` (deprecated alias `RUSTFS_DATA_SCANNER_START_DELAY_SECS`) | seconds | unset | Sets startup delay and, for compatibility, the cycle interval when `scanner.cycle` is unset. |
| `scanner.cycle_max_duration` | `RUSTFS_SCANNER_CYCLE_MAX_DURATION_SECS` | seconds | `1800` (`DEFAULT_SCANNER_CYCLE_MAX_DURATION_SECS`) | Caps one cycle's runtime. An explicit `0` disables this budget. |
| `scanner.cycle_max_objects` | `RUSTFS_SCANNER_CYCLE_MAX_OBJECTS` | objects | `0` (`DEFAULT_SCANNER_CYCLE_MAX_OBJECTS`) | Caps objects processed by one cycle. `0` disables this budget. |
| `scanner.cycle_max_directories` | `RUSTFS_SCANNER_CYCLE_MAX_DIRECTORIES` | directories | `0` (`DEFAULT_SCANNER_CYCLE_MAX_DIRECTORIES`) | Caps directories entered by one cycle. `0` disables this budget. |
| `heal.bitrot_cycle` | `RUSTFS_SCANNER_BITROT_CYCLE_SECS` | seconds | `2592000` (`DEFAULT_HEAL_BITROT_CYCLE_SECS`, 30 days) | Controls periodic deep bitrot scans. `false`, `off`, `no`, or `disabled` disables periodic deep scans; `0`, `true`, `on`, or `yes` runs deep mode every scanner cycle. |
| `scanner.idle_mode` | `RUSTFS_SCANNER_IDLE_MODE` | boolean | `true` (`DEFAULT_SCANNER_IDLE_MODE`) | Master switch for scanner throttling: preset sleeps plus the foreground-read backoff floor. `false` disables both and the scanner runs at full speed. |
| `scanner.cache_save_timeout` | `RUSTFS_SCANNER_CACHE_SAVE_TIMEOUT_SECS` | seconds | `14` (`DEFAULT_SCANNER_CACHE_SAVE_TIMEOUT_SECS`) | Timeout for saving scanner cache; runtime enforces a minimum of `1` and keeps the default persistence budget within the distributed publication lease. |
| `scanner.max_concurrent_set_scans` | `RUSTFS_SCANNER_MAX_CONCURRENT_SET_SCANS` | count | `4` (`DEFAULT_SCANNER_MAX_CONCURRENT_SET_SCANS`) | Caps concurrent set-level scanner tasks. `0` keeps topology-derived concurrency. |
| `scanner.max_concurrent_disk_scans` | `RUSTFS_SCANNER_MAX_CONCURRENT_DISK_SCANS` | count | `4` (`DEFAULT_SCANNER_MAX_CONCURRENT_DISK_SCANS`) | Caps concurrent disk bucket walks per set. `0` keeps disk-count-derived concurrency. |
| `scanner.yield_every_n_objects` | `RUSTFS_SCANNER_YIELD_EVERY_N_OBJECTS` | objects | `128` (`DEFAULT_SCANNER_YIELD_EVERY_N_OBJECTS`) | Controls how often object loops yield to the async runtime. `0` disables this extra yield. |
| `scanner.alert_excess_versions` | `RUSTFS_SCANNER_ALERT_EXCESS_VERSIONS` | versions | `100` (`DEFAULT_SCANNER_ALERT_EXCESS_VERSIONS`) | Version count threshold for scanner alerts. |
| `scanner.alert_excess_version_size` | `RUSTFS_SCANNER_ALERT_EXCESS_VERSION_SIZE` | bytes | `1099511627776` (`DEFAULT_SCANNER_ALERT_EXCESS_VERSION_SIZE`) | Retained version byte threshold for scanner alerts. |
| `scanner.alert_excess_folders` | `RUSTFS_SCANNER_ALERT_EXCESS_FOLDERS` | folders | `65538` (`DEFAULT_SCANNER_ALERT_EXCESS_FOLDERS`) | Direct subfolder threshold for scanner alerts. |

Speed presets (`crates/config/src/constants/scanner.rs`) set the base sleep multiplier, maximum wait, and cycle interval:

| Preset | Sleep factor | Max sleep | Cycle interval |
|---|---:|---:|---:|
| `fastest` | 0 | 0 | 1s |
| `fast` | 1x | 100ms | 60s |
| `default` | 2x | 1s | 60s |
| `slow` | 10x | 15s | 60s |
| `slowest` | 100x | 15s | 30m |

Use `scanner.delay`, `scanner.max_wait`, and `scanner.cycle` when the preset is close but one axis needs a precise override. With `idle_mode=true`, directory-level sleep is `1ms x factor` and object-level sleep is `time spent on the object x factor`, both capped at `max_wait`; a foreground-read floor of `FOREGROUND_READ_BACKOFF_PER_REQUEST_MS` (10ms) per concurrent GetObject/streaming read, capped at `FOREGROUND_READ_BACKOFF_MAX_MS` (250ms), is applied on top and can exceed the preset's `max_wait` (`crates/scanner/src/sleeper.rs`).

### Environment-only scanner knobs

These have no persistent key and are read from the environment only.

| Environment variable | Default (constant) | Effect |
|---|---|---|
| `RUSTFS_SCANNER_ENABLED` (deprecated alias `RUSTFS_ENABLE_SCANNER`) | `true` (`scanner_enabled_from_env`, `rustfs/src/module_switches.rs`) | Starts the data scanner at all. The heal manager is initialized whenever heal or scanner is enabled, because scanner-produced heal candidates need a consumer. |
| `RUSTFS_SCANNER_ALERT_COOLDOWN_SECS` | `86400` (`DEFAULT_SCANNER_ALERT_COOLDOWN_SECS`, `scanner_folder.rs`) | Per-(kind, bucket, object) cooldown between S3 excess-alert events; `0` emits every cycle. See [Scanner Excess Alerts](scanner-excess-alerts.md). |
| `RUSTFS_SCANNER_DEEP_VERIFY_COOLDOWN_SECS` | `60` (`DEFAULT_SCANNER_DEEP_VERIFY_COOLDOWN_SECS`, `scanner_folder.rs`) | Objects modified within this window are skipped by deep (bitrot) verification in the current cycle. |
| `RUSTFS_HEAL_OBJECT_SELECT_PROB` | `1024` (`DEFAULT_HEAL_OBJECT_SELECT_PROB`, `scanner_folder.rs`) | Sampling divisor for scanner-originated heal checks: roughly one object in N per cycle is selected for a low-priority heal check. |
| `RUSTFS_DATA_USAGE_UPDATE_DIR_CYCLES` | `16` (`DATA_USAGE_UPDATE_DIR_CYCLES`, `scanner_folder.rs`) | Every N cycles a compacted directory is re-descended instead of reusing its cached usage. `1` forces re-descent every cycle (used by lifecycle e2e lanes). |
| `RUSTFS_DATA_USAGE_FAILED_OBJECT_TTL_SECS` | `86400` (`DEFAULT_FAILED_OBJECT_TTL_SECS`, `scanner_folder.rs`) | Retention of per-bucket failed-object entries in the usage cache. |
| `RUSTFS_DATA_USAGE_FAILED_OBJECTS_MAX` | `10000` (`DEFAULT_FAILED_OBJECTS_MAX`, `scanner_folder.rs`) | Cap on retained failed-object entries per bucket. |

### Cycle budgets and cadence

When the cycle duration control is unset, RustFS uses the finite 1800-second default. An explicit `0` preserves the compatibility behavior of an unbounded cycle; object and directory budgets likewise remain unbounded when explicitly set to `0`. Invalid or overflowing duration environment values are configuration errors rather than silent fallback values.

When a finite deadline expires, RustFS cancels cooperative scanner work and waits only for the existing bounded shutdown window. A non-yielding I/O future is dropped after that window. RustFS then attempts a higher leadership epoch so late cycle, usage, cache, and remote writes from the old generation fail closed. If the worker cannot stop cooperatively, the cycle state was not confirmed durable, or that epoch fence cannot be durably persisted, the scanner reports `recovery-required`; it does not claim an uncooperative cursor was saved.

An explicit `scanner.cycle` or `RUSTFS_SCANNER_CYCLE` is a minimum inter-cycle cadence: dirty-usage notifications do not bypass that configured interval. The default adaptive policy continues to use dirty-usage notifications to wake the scanner between timer-driven cycles.

## Single-disk clean-idle scheduling

An erasure single-disk deployment using the built-in cycle and bitrot defaults automatically backs off repeated clean idle scans instead of walking the same unchanged namespace every minute. Each successful timer-driven cycle that finds no dirty usage or unresolved maintenance work doubles the next interval. The status endpoint reports the effective interval and multiplier.

The backoff is reset to the base interval by object or bucket mutations, lifecycle or replication configuration changes, partial or failed cycles, usage persistence failures, and unresolved scanner-originated heal or bitrot work. Active lifecycle or replication rules keep the base cadence. An explicit cycle, a non-default persisted speed, any environment speed or start-delay override, an environment bitrot override, or a non-default persisted active bitrot cycle also keeps the configured cadence rather than applying the automatic policy. Persisting `scanner.speed=default` or the default bitrot cycle is normalized to the built-in default and therefore keeps automatic scheduling enabled.

Lifecycle and replication configuration inspection is bounded so a slow metadata read cannot stall scanner startup or scheduling. A failed or timed-out inspection keeps the base cadence and is retried after 5 minutes, doubling up to a maximum of 60 minutes while failures continue. A lifecycle or replication configuration change wakes the scanner and retries inspection immediately.

With the default 30-day bitrot cycle, the clean-idle interval is capped at the bitrot cycle divided by the object selection window (about 42 minutes with the default `RUSTFS_HEAL_OBJECT_SELECT_PROB`), which preserves the intended wall-clock bitrot coverage. If periodic bitrot is disabled, the clean-idle cap is 24 hours. The effective interval is jittered by up to 10 percent to avoid synchronized scanner starts.

## Status Endpoint

```text
GET /v3/scanner/status
```

The request must be authenticated with an admin identity that has `ServerInfoAdminAction`. The JSON response has these scanner-specific top-level objects:

| Object | Content |
|---|---|
| `runtime_config` | Effective runtime controls and their value sources. |
| `cycle_schedule` | Current effective cycle interval and clean-idle backoff state. |
| `metrics` | Scanner work, pressure, checkpoint, lifecycle, replication, heal, bitrot, and alert counters. |
| `data_movement_pause` | Global-pause policy, current movement reason, operation epoch, start time, duration, and estimated movement work items. |
| `pause_backlog` | Replicated durable pause ledger, post-pause catch-up phase, rate window, retry state, thresholds, and active alert reasons. |
| `catch_up_estimate` | Movement work plus current dirty-usage and already discovered lifecycle queues. |

Example fields to inspect:

```text
runtime_config.speed.value
runtime_config.delay.value
runtime_config.max_wait_seconds.value
runtime_config.cycle_interval_seconds.value
runtime_config.bitrot_cycle_seconds.value
cycle_schedule.effective_interval_seconds
cycle_schedule.clean_idle_backoff_enabled
cycle_schedule.clean_idle_backoff_multiplier
metrics.pacing_pressure.primary_pressure
metrics.pacing_pressure.last_cycle_budget_limited
metrics.lifecycle_transition.current_queued
metrics.lifecycle_transition.scanner_missed
metrics.maintenance_control.primary_control
metrics.source_work
metrics.replication_repair
metrics.scan_checkpoint
metrics.cycle_timeout_total
metrics.cycle_last_progress_age
metrics.leader_lease_without_progress
metrics.cycle_recovery_required_total
data_movement_pause.paused
data_movement_pause.reasons
data_movement_pause.duration_seconds
data_movement_pause.operation_epoch
data_movement_pause.movement_generation
data_movement_pause.movement_backlog_work_items
pause_backlog.persistence_state
pause_backlog.phase
pause_backlog.pause_duration_seconds
pause_backlog.pending_full_scan
pause_backlog.pending_work_items
pause_backlog.next_attempt_at_unix_secs
pause_backlog.alert_reasons
catch_up_estimate.dirty_usage_buckets
catch_up_estimate.discovered_expiry_items
catch_up_estimate.discovered_transition_items
```

## Usage State Reset

The supported break-glass route for rebuilding scanner usage state is `POST /v3/scanner/usage-state/reset` with body `{"mode":"full-rebuild"}`, authenticated as an admin identity holding `ConfigUpdateAdminAction` (route registered in `rustfs/src/admin/route_registration_test.rs`). Use it only after the scanner status shows a usage-floor load failure, a conflicting persisted usage floor, or an operator decision to discard the durable usage baseline and rebuild it from a full scanner pass.

The reset does not delete metadata files by hand and does not publish an authoritative zero-usage snapshot. It holds the scanner leader lock, fences the operation with the storage-owned publication epoch, CAS-publishes a v2 `bootstrap-pending` marker in the primary usage slot, then clears stale backup, legacy, and observed usage slots by object revision. The next scanner leadership claim binds that marker to a fresh epoch, and the next complete scanner cycle replaces it with authoritative usage.

```json
{
  "status": "reset",
  "mode": "full-rebuild",
  "usage_state": "bootstrap-pending",
  "leader_epoch": 9,
  "next_cycle": 42,
  "reset_paths": [
    "buckets/.usage.v2.json",
    "buckets/.usage.v2.json.bkp",
    "buckets/.usage.json",
    "buckets/.usage.json.bkp",
    "buckets/.usage.observed.json"
  ]
}
```

| Error mentions | Do |
|---|---|
| data movement | wait for decommission or rebalance to leave the scanner metadata path, then retry |
| invalid scanner cycle state | run `POST /v3/scanner/cycle-state/reset` with `{"mode":"full-rescan"}` first |

## Data Movement Pauses

RustFS uses a `global_pause` policy while pool decommission or rebalance can hide scanner metadata: usage publication, lifecycle discovery, tier cleanup discovery, scanner-originated heal and bitrot checks, and replication discovery are deferred together. A failed or canceled decommission remains a publication barrier until an operator retries or clears it. The same pause and estimate objects are included in `GET /v3/ilm/expiry/status`.

| Field | Meaning |
|---|---|
| `data_movement_pause.reasons` | In-process decommission worker state combined with durable pool and rebalance operation metadata. Exhausted operation epochs or movement generations fail closed and appear as explicit reasons. |
| `data_movement_pause.duration_seconds`, start time, `movement_backlog_work_items` | From durable metadata; a worker-only or exhausted-counter snapshot can report `paused=true` with zero start time and backlog. `movement_backlog_work_items` counts remaining movement bucket work units, not expired objects. |
| `catch_up_estimate` | Movement estimate plus dirty-usage buckets and lifecycle items discovered before or during the pause. `undiscovered_ilm_items_known=false` because a global pause cannot count newly expired objects without scanning; use `usage_baseline_unix_secs` to judge the estimate's age. |
| `pause_backlog.persistence_state` | `persistence_unavailable` when the `.scanner-pause-backlog.json` ledger cannot be read or updated; scanner cycles stay gated and persistence is retried every five minutes. |
| `pause_backlog.phase` | `idle`, `paused`, `catching_up`, or `retry_exhausted`. The ledger returns to `idle` only after one successful full namespace scan and zero known dirty-usage, expiry, and transition queues. |
| `membership_repair_pending` | A rejoining decommission source is being re-seeded from the last committed surviving-set ledger before a new full-membership commit is allowed. |
| `pause_backlog.thresholds` | Exact pause-duration, deferred-cycle, backlog-size, rate, and failure limits used by the running binary. |
| `pause_backlog.alert_reasons` | Exceeded thresholds, exhausted counters or retries, replica degradation, and persistence failures. |

Built-in limits reported under `pause_backlog.thresholds`:

| Limit | Value |
|---|---|
| Pause-duration alert | 24 hours |
| Movement-deferral alert | 3 deferrals in one unconverged pause episode |
| Backlog alert | 10,000 known pending work items |
| Catch-up rate window | At most 4 attempts per hour, no more than 1 per 5 minutes |
| Retry exhaustion | 5 consecutive failed or interrupted attempts, then a sparse hourly probe |

Catch-up attempts remain subject to the normal cycle duration, object, directory, sleeper, and foreground-read budgets.

Unlabeled Prometheus gauges:

| Gauge | Meaning |
|---|---|
| `rustfs_scanner_data_movement_paused` | Local pause snapshot. |
| `rustfs_scanner_data_movement_pause_duration_seconds` | Local pause duration. |
| `rustfs_scanner_data_movement_backlog_work_items` | Local movement backlog. |
| `rustfs_scanner_pause_backlog_phase` | `0` idle, `1` paused, `2` catching up, `3` retry exhausted. |
| `rustfs_scanner_pause_backlog_pause_duration_seconds` | Durable pause duration. |
| `rustfs_scanner_pause_backlog_pending_work_items` | Known pending work items. |
| `rustfs_scanner_pause_backlog_consecutive_failures` | Consecutive failed catch-up attempts. |
| `rustfs_scanner_pause_backlog_rate_limited` | Catch-up currently rate limited. |
| `rustfs_scanner_pause_backlog_retry_exhausted` | Ledger in `retry_exhausted`. |
| `rustfs_scanner_pause_backlog_alerting` | Any alert reason active. |
| `rustfs_scanner_pause_backlog_replica_degraded` | Ledger replica set degraded. |

## Reading Pacing Pressure

`metrics.pacing_pressure.primary_pressure` summarizes the highest-priority scanner pressure signal:

| Value | Meaning | Usual response |
|---|---|---|
| `queued_scans` | Set or disk scan queues are backing up. | Lower scanner concurrency or increase pacing delay if user traffic is affected. |
| `cycle_budget` | The last cycle stopped because a runtime/object/directory budget was reached. | Check `last_cycle_partial_reason` and `last_cycle_partial_source`; increase the specific budget if scans need to finish sooner. |
| `throttle_pause` | Scanner sleeps or cooperative yields were observed. | Expected when `idle_mode` is enabled; inspect pause ratios before tuning. |
| `active_scans` | Scanner work is active but not currently queued or budget-limited. | Usually healthy; correlate with CPU/disk metrics. |
| `none` | No current scanner pressure was observed. | No scanner pacing action needed. |

The ratio fields `last_cycle_throttle_sleep_ratio`, `last_cycle_yield_ratio`, and `last_cycle_total_pause_ratio` are fractions of the last cycle duration. If CPU is high but pause ratios are already high, increasing `scanner.delay` or `scanner.max_wait` may have limited value; check active paths, source work, and disk activity before changing the cycle interval.

## Reading Source Work

`metrics.source_work`, `metrics.current_cycle_source_work`, and `metrics.last_cycle_source_work` group scanner work by source: `usage`, `lifecycle`, `bucket_replication`, `site_replication`, `heal`, `bitrot`, `alerts`.

Each source has `checked`, `queued`, `executed`, `failed`, `skipped`, and `missed` counters. `missed` means the scanner found work but could not admit it to the downstream queue. `skipped` means the work was intentionally merged or deduplicated. Use these counters to decide whether scan progress is limited by scanner pacing or by a downstream subsystem such as lifecycle transition, replication repair, or heal admission.

## Reading Heal Operations

```text
POST /v3/background-heal/status
```

Reports scanner-driven bitrot state together with heal queue execution state. `healQueueLength` and `healActiveTasks` keep the legacy totals; `healOperations` adds the same totals split by request source and priority:

| Field | Meaning |
|---|---|
| `queueLength` | Total queued heal requests. |
| `activeTasks` | Total running heal tasks. |
| `queuedBySource` | Queued requests split into `scanner`, `admin`, `autoHeal`, and `internal`. |
| `activeBySource` | Running tasks split into `scanner`, `admin`, `autoHeal`, and `internal`. |
| `queuedByPriority` | Queued requests split into `low`, `normal`, `high`, and `urgent`. |
| `activeByPriority` | Running tasks split into `low`, `normal`, `high`, and `urgent`. |

Use this route when `metrics.source_work` shows `heal` or `bitrot` queued or missed work. Scanner-originated object checks should appear under `scanner/low`, manual admin heal under `admin/high`. If scanner work grows but admin work remains blocked, treat that as heal queue pressure rather than scanner pacing pressure.

## Heal runtime controls

Heal knobs are environment-only and read by `HealConfig::default` (`crates/heal/src/heal/manager.rs`), the MRF queue (`crates/heal/src/heal/mrf_queue.rs`), or the erasure-set healer (`crates/heal/src/heal/erasure_healer.rs`). The admin `heal` config subsystem accepts only `bitrot_cycle` (`HEAL_KEYS`), which is documented in the scanner table above. Constants live in `crates/config/src/constants/heal.rs` unless another file is named.

| Environment variable | Default (constant) | Effect |
|---|---|---|
| `RUSTFS_HEAL_ENABLED` (deprecated alias `RUSTFS_ENABLE_HEAL`) | `true` (`heal_enabled_from_env`, `rustfs/src/module_switches.rs`) | Master switch for the background heal manager. |
| `RUSTFS_HEAL_AUTO_HEAL_ENABLE` | `true` (`DEFAULT_HEAL_AUTO_HEAL_ENABLE`) | Enables automatic healing of detected issues; `false` leaves healing to manual admin requests. |
| `RUSTFS_HEAL_QUEUE_SIZE` | `10000` (`DEFAULT_HEAL_QUEUE_SIZE`) | Heal request queue capacity. |
| `RUSTFS_HEAL_INTERVAL_SECS` | `10` (`DEFAULT_HEAL_INTERVAL_SECS`) | Heal manager polling interval. |
| `RUSTFS_HEAL_TASK_TIMEOUT_SECS` | `300` (`DEFAULT_HEAL_TASK_TIMEOUT_SECS`) | Per-task timeout. |
| `RUSTFS_HEAL_MAX_CONCURRENT_HEALS` | `4` (`DEFAULT_HEAL_MAX_CONCURRENT_HEALS`) | Global concurrent heal task limit. |
| `RUSTFS_HEAL_MAX_CONCURRENT_PER_SET` | `1` (`DEFAULT_HEAL_MAX_CONCURRENT_PER_SET`) | Per-erasure-set limit; effective value is `min(global, per_set)`, each floored at `1`. |
| `RUSTFS_HEAL_LOW_PRIORITY_MERGE_ENABLE` | `true` (`DEFAULT_HEAL_LOW_PRIORITY_MERGE_ENABLE`) | Merge duplicate low-priority requests with the same dedup key. |
| `RUSTFS_HEAL_LOW_PRIORITY_DROP_WHEN_FULL` | `true` (`DEFAULT_HEAL_LOW_PRIORITY_DROP_WHEN_FULL`) | Drop, rather than block on, low-priority requests when the queue is full. |
| `RUSTFS_HEAL_EVENT_DRIVEN_SCHEDULER_ENABLE` | `true` (`DEFAULT_HEAL_EVENT_DRIVEN_SCHEDULER_ENABLE`) | Notify-driven scheduler wakeups. |
| `RUSTFS_HEAL_SET_BULKHEAD_ENABLE` | `true` (`DEFAULT_HEAL_SET_BULKHEAD_ENABLE`) | Per-set bulkhead scheduling. |
| `RUSTFS_HEAL_PAGE_PARALLEL_ENABLE` | `true` (`DEFAULT_HEAL_PAGE_PARALLEL_ENABLE`) | Page-level parallel object healing during erasure-set repair. |
| `RUSTFS_HEAL_PAGE_OBJECT_CONCURRENCY` | `8` (`DEFAULT_HEAL_PAGE_OBJECT_CONCURRENCY`) | Concurrent object heals within one erasure-set page. Forced to `1` when page parallelism is off, for `Deep` scan mode, and for `AutoHeal`-sourced requests (`ErasureSetHealer::effective_heal_page_object_concurrency_for_source`). |
| `RUSTFS_HEAL_MAINLINE_THROTTLE_ENABLE` | `true` (`DEFAULT_HEAL_MAINLINE_THROTTLE_ENABLE`) | Defer best-effort starts and cooperatively pace running admin heal at safe work boundaries. |
| `RUSTFS_HEAL_MAINLINE_READ_UTILIZATION_HIGH_PERCENT` | `80` (`DEFAULT_HEAL_MAINLINE_READ_UTILIZATION_HIGH_PERCENT`, capped at 100) | Read-utilization high watermark for start admission and running admin pacing; zero disables this class. |
| `RUSTFS_HEAL_MAINLINE_WRITE_UTILIZATION_HIGH_PERCENT` | `80` (`DEFAULT_HEAL_MAINLINE_WRITE_UTILIZATION_HIGH_PERCENT`, capped at 100) | Write-utilization high watermark for start admission and running admin pacing; zero disables this class. |
| `RUSTFS_HEAL_MAINLINE_MAX_SLEEP_MS` | `250` (`DEFAULT_HEAL_MAINLINE_MAX_SLEEP_MS`) | Start recheck interval; running admin waits cap each pacing-gate holder at 1000 ms. Zero disables running pacing. |
| `RUSTFS_HEAL_OVERLAP_POLICY` | `merge` (`DEFAULT_HEAL_OVERLAP_POLICY`) | `merge` dedups an admin heal start that overlaps a running or queued heal; `minio_error` returns a typed already-running / overlapping-paths rejection like madmin. |
| `RUSTFS_HEAL_MRF_ENABLE` | `true` (`DEFAULT_HEAL_MRF_ENABLE`) | MRF intent pipeline: error paths deliver repair intents to the heal runtime and unconsumed intents replay from the durable journal after restart. |
| `RUSTFS_HEAL_MRF_QUEUE_SIZE` | `100000` (`DEFAULT_HEAL_MRF_QUEUE_SIZE`) | MRF in-memory queue capacity. |
| `RUSTFS_HEAL_MRF_JOURNAL_MAX_BYTES` | `8388608` (`DEFAULT_HEAL_MRF_JOURNAL_MAX_BYTES`, 8 MiB) | MRF journal size at which compaction runs. |
| `RUSTFS_HEAL_MRF_REPLAY_BATCH` | `256` (`DEFAULT_HEAL_MRF_REPLAY_BATCH`) | Intents per replay push round. |
| `RUSTFS_HEAL_DANGLING_DELETE_GRACE_SECS` | `3600` (`DEFAULT_HEAL_DANGLING_DELETE_GRACE_SECS`, `crates/ecstore/src/set_disk/core/io_primitives.rs`) | A recently modified object is never deleted as dangling inside this window; `0` disables the grace window. |

### Running admin heal pacing

The manager passes its existing workload provider and a configuration snapshot into each admin execution. Bucket/prefix listing and object boundaries resample foreground pressure; erasure-set page workers also resample after earlier work releases page capacity. `High`, `Urgent`, and `force_start` do not exempt ordinary admin execution from this runtime pacing. The existing start-time bypass and overlap-control meanings are unchanged.

Each execution has its own pacing latch, with no new global manager or cross-set pacing lock. The low watermark for each enabled class is `max(1, floor(high * 3 / 4))`: the default high watermark 80 therefore recovers below 60. High pressure latches pacing, and intermediate pressure resets the recovery window. Unpaced starts resume after sampled pressure remains below the low watermarks for four pause intervals, normally one second. While pressure persists, a pacing-gate holder waits only one interval, at most one second, then permits maintenance to continue. Concurrent page waiters serialize through this task-local gate; queue waiting still counts against the existing task execution timeout.

The pacing gate holds neither namespace locks nor I/O/page permits while sleeping. At final page admission, each real permit acquisition gets a fresh, nonblocking pressure decision. Low-pressure work keeps that permit; only a unit that needs a pause releases capacity to wait. A unit that has completed one bounded pause may proceed despite persistent pressure, which supplies minimum maintenance progress without an endless acquire/pause loop. Existing object operations and commit tails are not interrupted because pressure rose. Cancellation and deadlines remain interruptible, and disabling pacing cannot bypass the global, per-set or page-concurrency hard caps. The existing `RUSTFS_HEAL_MAINLINE_THROTTLE_ENABLE=false` setting is the operational opt-out for newly created executions; no additional request override is introduced.

A missing provider, zero pause, or both class thresholds set to zero preserves unpaced execution. Missing counts follow the existing shared pressure interpreter; they are observations, not health, quorum or resource-ownership proof. The current provider exposes node-level workload classes, so this does not claim independent per-set foreground measurements or a hard global resource budget. Runtime waits increment `rustfs_heal_mainline_throttle_total` with `source=admin`, `result=delayed`, and a foreground-pressure or `recovery_window` reason. Real p99/throughput protection requires the separate W20 fixed-load ABBA measurements.

## Pending Heal Hints

The scanner's persisted pending-heal cache is a best-effort retry backstop, bounded to 10,000 hints per bucket and a 24-hour age limit. These limits do not authorize garbage collection of committed durable repair obligations. A durable owner must retain its independent replay record until verified object completion or an equivalent durable successor permits removal.

Accepted, merged, or policy-dropped admission does not clear an existing hint. Task completion and legacy repair notices also lack the incarnation, set scope, generation, and storage verification needed to prove repair responsibility was discharged. The legacy success path therefore retains hints conservatively; this is not a complete durable MRF handoff protocol.

Pending retries use their persisted attempt count and last-attempt timestamp, starting at 15 minutes and doubling up to six hours. Each bucket submits at most 128 due hints per cycle. Already admitted or policy-dropped hints retry at Low priority; queue-full hints keep High priority but obey the same due-time bound. A changed retry batch synchronizes its pending cache once, including when cancelled, rather than copying the entire table after every admission.

Rediscovery and admission observations update the recorded result but do not postpone an already armed retry. The actual retry loop advances the attempt count and timestamp before awaiting admission, so cancellation or repeated queue-full results cannot reset the retry budget.

| Producer | Current identity and compensation | Durable handoff boundary |
|---|---|---|
| Scanner corrupt metadata | Metadata kind with no invented version/set; an existing pending-cache hint remains available for bounded retries. | Cache publication is separate from MRF ingress. Its age/count limits mean it is not an irrevocable repair-obligation ledger. |
| Read decode failure | Decode kind, available version and erasure-set scope; a later failing read can rediscover the repair. | Nonblocking ingress and in-memory read-repair admission do not acknowledge durable acceptance. |
| Partial write | Partial-write kind, available version and erasure-set scope; an in-memory heal request is the fast path. | The caller's documented restart-survival requirement is not fulfilled by ignoring the ingress result or by removing the unaccepted journal record at manager admission. Verified durable ownership remains pending. |

Legacy notices carry only bucket/object/version, not a verified storage disposition, incarnation, scope, or durable responsibility generation. They are drained without clearing hints. Terminal callbacks release only their exact node-local ingress lease so rediscovery remains possible; lease generations are not durable successor receipts. Pending migration staging is not activated, and this change does not enable durable tombstones or garbage collection. Positive cleanup requires a storage-owner receipt with the complete responsibility identity and validated commit/fence evidence; neither task status nor the bounded diagnostic outcome window supplies it.

## Deliberate non-parity with MinIO

These differences from MinIO are design decisions, recorded so they are not re-filed as gaps.

| Area | RustFS behavior | Why it is not a gap |
|---|---|---|
| Bloom filter | `.bloomcycle.bin` (`DATA_USAGE_BLOOM_NAME`, `crates/scanner/src/data_usage_define.rs`) is reused only as the cycle/epoch fence. | MinIO master removed the bloom filter too. |
| Scanner leadership | Single cluster-wide scanner leader plus an epoch fence. | Same model as MinIO; the fence is additive. |
| Heal notifications | Heal emits no S3 bucket notification; results are exposed through admin status. | Same as MinIO. |
| Incomplete multipart cleanup | Runs as an independent background routine, not inside the scanner or ILM. | Same as MinIO. |
| Inline heal | The scanner only enqueues heal candidates; nothing heals inline on the scan path. | MinIO's inline `applyHealing` path is intentionally not a parity target. |
| Heal-sequence keep-alive | Admin heal status is a snapshot query with incremental `sinceSeq`/`nextSeq` semantics (`crates/heal-contracts/src/heal_channel.rs`). | MinIO's 10-second blank keep-alive write-back belongs to its streaming model and is not copied. |
| `.trash` / `tmp-old` paths | Layout constants are RustFS's own (`crates/ecstore/src/disk/local.rs`). | No literal alignment with MinIO path names is intended. |

## Migrating from MinIO scanner settings

Only two MinIO scanner variables are recognized. `apply_external_env_compat` (`crates/utils/src/envs.rs`, called from `rustfs/src/startup_preflight.rs`) copies `MINIO_<suffix>` into `RUSTFS_<suffix>` at startup for suffixes on `EXTERNAL_COMPATIBLE_SUFFIXES`, and only when the `RUSTFS_` key is absent; when both are set with different values the `RUSTFS_` value wins and a `Detected external-prefix compatibility conflicts` warning is logged. The scanner suffixes on that list are `SCANNER_SPEED` and `SCANNER_CYCLE` (tests `scanner_aliases_are_mapped_when_rustfs_missing` in `crates/utils/src/envs.rs`, `test_cycle_interval_supports_minio_speed_alias` and `test_cycle_interval_supports_minio_cycle_alias` in `crates/scanner/src/scanner/tests.rs`). Every other `MINIO_SCANNER_*` or `MINIO_HEAL_*` variable is silently ignored.

| MinIO setting | RustFS setting | Migration |
|---|---|---|
| `MINIO_SCANNER_SPEED` / `scanner speed` | `RUSTFS_SCANNER_SPEED` / `scanner.speed` | Env alias mapped at startup; preset names and the preset table are identical. |
| `MINIO_SCANNER_CYCLE` / `scanner cycle` | `RUSTFS_SCANNER_CYCLE` / `scanner.cycle` | Env alias mapped at startup. |
| `MINIO_SCANNER_IDLE_SPEED` / `scanner idle_speed` (`on` default, `off`) | `RUSTFS_SCANNER_IDLE_MODE` / `scanner.idle_mode` (`true` default, `false`) | Not mapped; must be rewritten. Direction matches (`on` and `true` both mean throttled). Both RustFS channels also accept `on`/`off` as booleans (`parse_config_bool`, `parse_bool_str`). RustFS `false` additionally disables the foreground-read backoff floor that MinIO does not have, so the scanner competes with foreground reads at full speed; use it only for benchmarks or exclusive-I/O windows. |
| `MINIO_HEAL_BITROTSCAN` / `heal bitrotscan` (default `off`) | `RUSTFS_SCANNER_BITROT_CYCLE_SECS` / `heal.bitrot_cycle` (default 30 days) | Not mapped. RustFS deep-scans periodically by default; set `off` or `disabled` to reproduce MinIO's default. |
| `MINIO_API_STALE_UPLOADS_EXPIRY` (24h) | `RUSTFS_API_STALE_UPLOADS_EXPIRY` (`DEFAULT_STALE_UPLOADS_EXPIRY`, 24h, `crates/ecstore/src/bucket/lifecycle/bucket_lifecycle_ops.rs`) | Not mapped; same default. |
| `MINIO_API_STALE_UPLOADS_CLEANUP_INTERVAL` (6h) | `RUSTFS_API_STALE_UPLOADS_CLEANUP_INTERVAL` (`DEFAULT_STALE_UPLOADS_CLEANUP_INTERVAL`, 6h) | Not mapped; same default. |
| `MINIO_API_DELETE_CLEANUP_INTERVAL` (5m) | None; `DELETED_OBJECTS_CLEANUP_INTERVAL` is a 5-minute constant in `crates/ecstore/src/disk/local.rs`. | No knob. Trash draining is not per-entry throttled. |
| `scanner alert_excess_folders` (50000) | `scanner.alert_excess_folders` (65538) | Not mapped; see [Scanner Excess Alerts](scanner-excess-alerts.md). |
| Any other `MINIO_SCANNER_*` | Corresponding `RUSTFS_SCANNER_*` from the tables above | Not mapped; rename explicitly. |

Stale-upload cleanup differs in one crash-recovery detail. RustFS's stale multipart cleanup (`cleanup_stale_multipart_uploads_in_set`) takes a namespace write lock, re-checks the upload under write quorum (`check_multipart_upload_path_exists`), and fans the delete out to every disk, where the local recursive delete renames the directory into `.rustfs.sys/tmp/.trash/<uuid>` (`move_to_trash`). If the process dies mid fan-out after more disks than the parity count have already moved the upload directory, the next cleanup pass's quorum re-check fails (`FileNotFound` is not in `OBJECT_OP_IGNORED_ERRS`) and the candidate is skipped, so the remaining per-disk residue is not reclaimed by that job. The residue is invisible to the S3 API and only consumes disk space; the window is milliseconds wide. MinIO processes each disk independently and converges in the same scenario.

## Replacement Recovery Completion

`POST /v3/background-heal/status` is an execution-queue view. `state=idle`, zero queue and active counts, an online disk, a readable object, or acceptance of an admin deep-heal request do not independently prove that a replacement disk contains every erasure shard.

Treat replacement recovery as verified only after the repair task has completed for the exact replacement instance and an operator has confirmed the target disk contains the expected `xl.meta` and data parts for every relevant object version. A replacement that is not mounted, is unsafe to format, loses its marker, or returns a partial target outcome must be treated as deferred or incomplete. Do not automate destructive replacement actions from an `idle` observation alone. A new node must not infer replacement completion from an old or unavailable peer; regard that information as unknown or degraded until every required peer can report the same replacement instance and verified completion.

`GET /rustfs/admin/v4/heal/replacement-recovery` reports durable automatic replacement records from survivor disks. `local.records[]` entries distinguish `waiting_for_replacement`, `running`, `incomplete`, `unrecoverable`, `cleanup_pending`, `completed`, and `unknown`; `local.definitive=false` or any `unknown` record means the node could not prove a local replacement state. The `cluster` section queries the replacement-recovery peer RPC and sets `cluster.definitive=true` only when the expected peer topology is complete, every peer supports the RPC, every peer snapshot is locally definitive, and all peers report the same replacement records. Old peers, unavailable peers, malformed peer payloads, topology gaps, and generation disagreements are reported as degraded or unknown rather than complete.

Replacement resume and checkpoint files use an independent on-disk schema. A newer reader rejects a future schema rather than continuing with data it cannot interpret, while an older binary cannot safely enforce the new generation fence. Do not roll a cluster back after a replacement generation has started; complete that recovery with the current-or-newer release, and if it cannot complete, keep that version for diagnosis rather than deleting its durable records or continuing with an older binary.

## Reading Replication Repair

`metrics.replication_repair`, `metrics.current_cycle_replication_repair`, and `metrics.last_cycle_replication_repair` split scanner-discovered replication repair work by source and repair kind. Each entry has the same `checked`, `queued`, `executed`, `failed`, `skipped`, and `missed` counters used by `source_work`, plus:

| Field | Meaning |
|---|---|
| `source` | `bucket_replication` for bucket replication repair, or `site_replication` for site replication boundary signals. |
| `kind` | Bucket repair kinds are `object`, `delete_marker`, `version_purge`, and `existing_object`. Site replication boundary kinds are `passive_requeue` and `active_resync`. |
| `scanner_role` | `repair_admission` means scanner found work and attempted to admit it to a worker queue. `boundary_signal` means scanner is reporting state owned by another runtime. |
| `execution_owner` | `bucket_replication_queue` for bucket replication repair execution, or `site_replication_runtime` for site replication resync execution. |

For bucket replication, `queued` means scanner-discovered repair was admitted to the replication queue, `missed` means the queue or worker path could not accept it, and `skipped` means the object did not require a new repair task. The site replication kinds keep passive scanner discovery separate from active resync; the scanner is never the active site replication resync controller.

| Scenario | Scanner source | Repair kind | Scanner role | Execution owner | Operational meaning |
|---|---|---|---|---|---|
| Bucket object, delete-marker, version-purge, or existing-object repair found during a scan | `bucket_replication` | `object`, `delete_marker`, `version_purge`, `existing_object` | `repair_admission` | `bucket_replication_queue` | Scanner found bucket replication repair work and attempted to admit it to the replication queue. |
| Peer-originated or passive site replication work is observed while scanning | `site_replication` | `passive_requeue` | `boundary_signal` | `site_replication_runtime` | Scanner is reporting a passive site-replication boundary signal; it is not taking ownership of active site resync. |
| Admin-triggered or runtime-owned site resync activity is visible in scanner metrics | `site_replication` | `active_resync` | `boundary_signal` | `site_replication_runtime` | A boundary/status signal owned by the site replication runtime, not scanner-controlled repair execution. |

If `site_replication` counters grow while bucket replication counters stay flat, investigate site replication status and resync state before tuning scanner pacing. If `bucket_replication` `missed` grows, investigate the bucket replication worker queue or target health before changing scanner cycle settings.

## Reading Maintenance Control

`metrics.maintenance_control` derives a source-level control snapshot from scanner pacing, partial-cycle state, source work, and lifecycle transition queue state. It does not change scanner scheduling; it explains why a source is moving, deferred, or blocked. When no scan cycle is active, source-work controls use the last completed cycle so recently missed work stays visible between passes.

`metrics.maintenance_control.primary_control`:

| Value | Meaning |
|---|---|
| `blocked_source` | At least one maintenance source found work that could not be admitted or is blocked by a downstream queue. |
| `deferred_source` | At least one source was deferred by a partial scanner cycle or budget-limited pass. |
| `active_source` | At least one source has current-cycle work or queued downstream work. |
| `pacing_pressure` | No source-specific state dominated, but scanner pacing pressure is still visible. |
| `none` | No source-level maintenance control pressure was observed. |

Each `metrics.maintenance_control.sources[]` entry:

| Field | Meaning |
|---|---|
| `source` | `usage`, `lifecycle`, `bucket_replication`, `site_replication`, `heal`, `bitrot`, or `alerts`. |
| `state` | `idle`, `active`, `deferred`, or `blocked`. |
| `reason` | `active_work`, `queued_work`, `partial_cycle`, `missed_work`, `expiry_queue_backlog`, `transition_failed`, `transition_compensation_backlog`, `transition_queue_backlog`, or `transition_queue_full`. |
| `backlog` | Current source-level backlog estimate from queued or missed work. |
| `current_checked` / `current_queued` / `current_missed` | Current-cycle counters for this source, or the last completed cycle when no scan cycle is active. |
| `lifetime_missed` | Lifetime missed work counter. |
| `partial_cycles` | Partial cycles attributed to this source. |

Read this snapshot before changing scanner controls: `blocked_source` with `lifecycle/missed_work` points at downstream lifecycle admission, `deferred_source` with `usage/partial_cycle` points at scanner cycle budgets, `lifecycle/expiry_queue_backlog` means expiry or delete work is still queued or active in the expiry worker pool, `lifecycle/transition_failed` means transition worker execution failed during the current or last completed cycle, and `lifecycle/transition_compensation_backlog` means transition compensation is still pending or running after queue backpressure.

`metrics.lifecycle_expiry` exposes the expiry/delete worker queue:

| Field | Meaning |
|---|---|
| `current_queue_capacity` | Effective expiry worker queue capacity for this node. |
| `current_queued` | Expiry/delete tasks waiting in the worker queue. |
| `current_active` | Expiry/delete tasks currently running. |
| `current_workers` | Configured expiry worker count. |
| `queue_missed` | Tasks that could not be queued because no worker channel was available or the queue was closed. |
| `scanner_queued` | Scanner-discovered expiry/delete object versions admitted to the expiry queue. |
| `scanner_missed` | Scanner-discovered expiry/delete object versions that could not be admitted. |

## Reading Distributed Metrics

`/rustfs/admin/v3/scanner/status` and `/rustfs/admin/v3/metrics` report the node that handles the HTTP request; the metrics endpoint does not fan out to peers. In distributed deployments, query every node explicitly and keep `by-host=true` so each response includes that node's host view:

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

The `aggregated.scanner` payload preserves the same scanner progress, checkpoint, pacing, source work, maintenance control, lifecycle expiry, and lifecycle transition fields used by the local scanner status, but only for the responding node; `by_host.*.scanner` keeps that node's host view. Compare the per-node artifacts externally to find old active paths, partial checkpoints, pacing pressure, source-level control pressure, or downstream queue admission problems across the deployment.

## Reading Lifecycle Transition Status

`metrics.lifecycle_transition`:

| Field | Meaning |
|---|---|
| `current_queue_capacity` | Current transition queue capacity. |
| `current_queued` | Transition tasks currently queued. |
| `current_active` | Transition tasks currently being processed. |
| `current_workers` | Transition worker count. |
| `queue_full` | Queue-full observations in the transition state. |
| `queue_send_timeout` | Send timeouts for transition queue admission. |
| `compensation_scheduled` | Buckets scheduled for transition compensation. |
| `compensation_pending` | Buckets with transition compensation still pending or running. |
| `compensation_running` | Transition compensation tasks currently running. |
| `scanner_queued` | Scanner transition tasks admitted to the queue. |
| `scanner_missed` | Scanner transition tasks that could not be admitted. |
| `completed` | Transition worker completions. |
| `failed` | Transition worker failures. |

When `scanner_missed` or `queue_full` rises, scanner lifecycle work is finding transition candidates faster than the transition queue accepts them: a downstream transition pressure signal, not just a scanner walk pressure signal.

## Tuning Workflow

For a mostly idle single-node, single-disk deployment with sustained CPU usage while the scanner is enabled:

1. Read `/v3/scanner/status`.
2. Check `metrics.pacing_pressure.primary_pressure`.
3. Check `metrics.maintenance_control.primary_control` and source entries before changing runtime controls.
4. Check `runtime_config.delay`, `runtime_config.max_wait_seconds`, and `runtime_config.cycle_interval_seconds` to confirm the active values and their sources.
5. Check `metrics.current_cycle_objects_scanned`, `metrics.current_cycle_directories_scanned`, and active paths to confirm the scanner is the active work.
6. If `primary_pressure` is `throttle_pause` and pause ratios are low, raise `scanner.delay` first.
7. If individual sleeps are too short, raise `scanner.max_wait`.
8. If each scan cycle finishes but starts too often, raise `scanner.cycle`.
9. If scans must be broken into bounded chunks, set one of `scanner.cycle_max_duration`, `scanner.cycle_max_objects`, or `scanner.cycle_max_directories`.
10. Recheck `pacing_pressure`, `maintenance_control`, source work, and lifecycle transition status after one or more scanner cycles.

Do not rely only on a longer cycle interval if lifecycle, replication, heal, or bitrot work must keep moving; use source work and transition status to confirm that background maintenance still progresses.

## Helm

The Helm chart exposes the scanner environment variables under `config.rustfs.scanner` (`helm/rustfs/values.yaml`):

```yaml
config:
  rustfs:
    scanner:
      speed: "slow"
      delay: "30"
      max_wait_secs: "15"
      cycle_secs: "3600"
      cycle_max_duration_secs: "1800"
      cycle_max_objects: "1000000"
      cycle_max_directories: "100000"
      idle_mode: "true"
      yield_every_n_objects: "128"
      bitrot_cycle_secs: "2592000"
```

Use `extraEnv` for environment variables that are not represented by chart values, including every heal knob above.
