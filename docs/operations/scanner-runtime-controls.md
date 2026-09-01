# Scanner Runtime Controls

This document describes the runtime controls and status fields for the RustFS
data scanner. It is written for operators who need to reduce scanner pressure,
diagnose slow scan progress, or confirm that background lifecycle, replication,
heal, bitrot, and usage work is still moving.

For reproducible scanner-pressure validation and before/after evidence, see
[Scanner Benchmark Runbook](scanner-benchmark-runbook.md).

## What the scanner does

The scanner is the background maintenance loop that walks stored objects and
feeds several subsystems:

- usage accounting and data usage cache updates;
- lifecycle expiry and transition admission;
- bucket replication repair admission;
- scanner-originated heal and bitrot checks;
- namespace alerts for excessive versions, retained version size, and folder
  fan-out.

Slowing the scanner can reduce idle CPU and disk pressure, but it also delays
the maintenance work above. Prefer using the status fields below before changing
cycle or pacing values.

## Configuration Sources

Scanner runtime config is resolved in this order:

1. Environment variables.
2. Persisted admin config for the `scanner` subsystem.
3. Built-in defaults or speed preset-derived values.

Bitrot cycle resolution is slightly different because the canonical persistent
key belongs to the `heal` subsystem:

1. `RUSTFS_SCANNER_BITROT_CYCLE_SECS`.
2. `heal.bitrot_cycle`.
3. Legacy compatibility key `scanner.bitrot_cycle`.
4. Built-in default.

The `/v3/scanner/status` response reports each effective runtime value with a
`source` of `env`, `config`, `scanner_compat_config`, or `default`.

## Runtime Controls

| Persistent key | Environment variable | Unit | Default | Effect |
|---|---|---:|---:|---|
| `scanner.speed` | `RUSTFS_SCANNER_SPEED` | preset | `default` | Selects the base pacing preset: `fastest`, `fast`, `default`, `slow`, or `slowest`. |
| `scanner.delay` | `RUSTFS_SCANNER_DELAY` | factor | preset-derived | Overrides the sleep multiplier. Valid range is `0` through `10000`. |
| `scanner.max_wait` | `RUSTFS_SCANNER_MAX_WAIT_SECS` | seconds | preset-derived | Caps one scanner sleep. |
| `scanner.cycle` | `RUSTFS_SCANNER_CYCLE` | seconds | preset-derived | Sets the interval between scanner cycles. |
| `scanner.start_delay` | `RUSTFS_SCANNER_START_DELAY_SECS` | seconds | unset | Sets startup delay and, for compatibility, the cycle interval when `scanner.cycle` is unset. |
| `scanner.cycle_max_duration` | `RUSTFS_SCANNER_CYCLE_MAX_DURATION_SECS` | seconds | `1800` | Caps one cycle's runtime. An explicit `0` disables this budget. |
| `scanner.cycle_max_objects` | `RUSTFS_SCANNER_CYCLE_MAX_OBJECTS` | objects | `0` | Caps objects processed by one cycle. `0` disables this budget. |
| `scanner.cycle_max_directories` | `RUSTFS_SCANNER_CYCLE_MAX_DIRECTORIES` | directories | `0` | Caps directories entered by one cycle. `0` disables this budget. |
| `heal.bitrot_cycle` | `RUSTFS_SCANNER_BITROT_CYCLE_SECS` | seconds | `2592000` | Controls periodic deep bitrot scans. `false`, `off`, `no`, or `disabled` disables periodic deep scans; `0`, `true`, `on`, or `yes` runs deep mode every scanner cycle. |
| `scanner.idle_mode` | `RUSTFS_SCANNER_IDLE_MODE` | boolean | `true` | Enables scanner sleeps and cooperative throttling. |
| `scanner.cache_save_timeout` | `RUSTFS_SCANNER_CACHE_SAVE_TIMEOUT_SECS` | seconds | `14` | Timeout for saving scanner cache; runtime enforces a minimum of `1` and keeps the default persistence budget within the distributed publication lease. |
| `scanner.max_concurrent_set_scans` | `RUSTFS_SCANNER_MAX_CONCURRENT_SET_SCANS` | count | `4` | Caps concurrent set-level scanner tasks. `0` keeps topology-derived concurrency. |
| `scanner.max_concurrent_disk_scans` | `RUSTFS_SCANNER_MAX_CONCURRENT_DISK_SCANS` | count | `4` | Caps concurrent disk bucket walks per set. `0` keeps disk-count-derived concurrency. |
| `scanner.yield_every_n_objects` | `RUSTFS_SCANNER_YIELD_EVERY_N_OBJECTS` | objects | `128` | Controls how often object loops yield to the async runtime. `0` disables this extra yield. |
| `scanner.alert_excess_versions` | `RUSTFS_SCANNER_ALERT_EXCESS_VERSIONS` | versions | `100` | Version count threshold for scanner alerts. |
| `scanner.alert_excess_version_size` | `RUSTFS_SCANNER_ALERT_EXCESS_VERSION_SIZE` | bytes | `1099511627776` | Retained version byte threshold for scanner alerts. |
| `scanner.alert_excess_folders` | `RUSTFS_SCANNER_ALERT_EXCESS_FOLDERS` | folders | `65538` | Direct subfolder threshold for scanner alerts. |

The `fastest`, `fast`, `default`, `slow`, and `slowest` presets set the base
sleep multiplier, maximum wait, and cycle interval. Use `scanner.delay`,
`scanner.max_wait`, and `scanner.cycle` when the preset is close but one axis
needs a precise override.

When the cycle duration control is unset, RustFS uses a finite 1800-second
(30-minute) default, matching the scanner benchmark guidance. An explicit `0`
preserves the compatibility behavior of an unbounded cycle; object and
directory budgets likewise remain unbounded when explicitly set to `0`. Invalid
or overflowing duration environment values are configuration errors rather than
silent fallback values.

When a finite deadline expires, RustFS cancels cooperative scanner work and
waits only for the existing bounded shutdown window. A non-yielding I/O future
is dropped after that window. RustFS then attempts a higher leadership epoch so
late cycle, usage, cache, and remote writes from the old generation fail closed.
If the worker cannot stop cooperatively, the cycle state was not confirmed
durable, or that epoch fence cannot be durably persisted, the scanner reports
`recovery-required`; it does not claim an uncooperative cursor was saved.

An explicit `scanner.cycle` or `RUSTFS_SCANNER_CYCLE` is a minimum inter-cycle
cadence: dirty-usage notifications do not bypass that configured interval.
The default adaptive policy continues to use dirty-usage notifications to wake
the scanner between timer-driven cycles.

## Single-disk clean-idle scheduling

An erasure single-disk deployment using the built-in cycle and bitrot defaults
automatically backs off repeated clean idle scans instead of walking the same
unchanged namespace every minute. Each successful timer-driven cycle that
finds no dirty usage or unresolved maintenance work doubles the next interval.
The status endpoint reports the effective interval and multiplier.

The backoff is reset to the base interval by object or bucket mutations,
lifecycle or replication configuration changes, partial or failed cycles,
usage persistence failures, and unresolved scanner-originated heal or bitrot
work. Active lifecycle or replication rules keep the base cadence. An explicit
cycle, a non-default persisted speed, any environment speed or start-delay
override, an environment bitrot override, or a non-default persisted active
bitrot cycle also keeps the configured cadence rather than applying the
automatic policy. Persisting `scanner.speed=default` or the default bitrot cycle
is normalized to the built-in default and therefore keeps automatic scheduling
enabled.

Lifecycle and replication configuration inspection is bounded so a slow
metadata read cannot stall scanner startup or scheduling. A failed or timed-out
inspection keeps the base cadence and is retried after 5 minutes, doubling up
to a maximum of 60 minutes while failures continue. A lifecycle or replication
configuration change wakes the scanner and retries inspection immediately.

With the default 30-day bitrot cycle, the clean-idle interval is capped at the
bitrot cycle divided by the object selection window. With the default selection
window this is about 42 minutes, which preserves the intended wall-clock bitrot
coverage. If periodic bitrot is disabled, the clean-idle policy cap is 24 hours.
The effective interval is jittered by up to 10 percent to avoid synchronized
scanner starts.

## Status Endpoint

The scanner status route is:

```text
GET /v3/scanner/status
```

The request must be authenticated with an admin identity that has
`ServerInfoAdminAction`. The JSON response has three scanner-specific top-level
objects:

- `runtime_config`: the effective runtime controls and their value sources.
- `cycle_schedule`: the current effective cycle interval and clean-idle
  backoff state.
- `metrics`: scanner work, pressure, checkpoint, lifecycle, replication, heal,
  bitrot, and alert counters.
- `data_movement_pause`: the global-pause policy, current movement reason,
  operation epoch, start time, duration, and estimated movement work items.
- `pause_backlog`: the replicated durable pause ledger, post-pause catch-up
  phase, rate window, retry state, thresholds, and active alert reasons.
- `catch_up_estimate`: movement work plus current dirty-usage and already
  discovered lifecycle queues.

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

The supported break-glass route for rebuilding scanner usage state is:

```text
POST /v3/scanner/usage-state/reset
{"mode":"full-rebuild"}
```

The request must be authenticated with an admin identity that has
`ConfigUpdateAdminAction`. Use it only after confirming the scanner status shows
a usage-floor load failure, a conflicting persisted usage floor, or an operator
decision to discard the durable usage baseline and rebuild it from a full
scanner pass.

The reset does not delete metadata files from disk by hand and does not publish
an authoritative zero-usage snapshot. It holds the scanner leader lock, fences
the operation with the storage-owned publication epoch, CAS-publishes a v2
`bootstrap-pending` marker in the primary usage slot, then clears stale backup,
legacy, and observed usage slots by object revision. The next scanner
leadership claim binds that marker to a fresh epoch and the next complete
scanner cycle replaces it with authoritative usage.

The JSON response is machine-readable:

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

If the response is an error mentioning data movement, wait for decommission or
rebalance to leave the scanner metadata path and retry. If it reports that the
scanner cycle state is invalid, run the cycle-state recovery reset first:

```text
POST /v3/scanner/cycle-state/reset
{"mode":"full-rescan"}
```

## Data Movement Pauses

RustFS currently uses a `global_pause` policy while pool decommission or
rebalance can hide scanner metadata. Usage publication, lifecycle discovery,
tier cleanup discovery, scanner-originated heal and bitrot checks, and
replication discovery are deferred together. A failed or canceled
decommission remains a publication barrier until an operator retries or clears
it.

`data_movement_pause.reasons` combines the in-process decommission worker state
with the durable pool and rebalance operation metadata. Exhausted operation
epochs or movement generations also fail closed and appear as explicit pause
reasons. Its start time, duration, and movement backlog come from the durable
metadata; a worker-only or exhausted-counter snapshot can therefore report
`paused=true` with zero start time and backlog.
`movement_backlog_work_items` counts remaining movement bucket work units, not
expired objects. `catch_up_estimate` combines that estimate with dirty-usage
buckets and lifecycle items that were already discovered before or during the
pause. The API sets `undiscovered_ilm_items_known=false` because a global pause
cannot count newly expired objects without scanning the namespace. Use
`usage_baseline_unix_secs` to judge the age of that estimate.

The same pause and estimate objects are included in
`GET /v3/ilm/expiry/status`. The gauges
`rustfs_scanner_data_movement_paused`,
`rustfs_scanner_data_movement_pause_duration_seconds`, and
`rustfs_scanner_data_movement_backlog_work_items` expose the local snapshot
without bucket-name labels.

The scanner persists `.scanner-pause-backlog.json` independently on erasure
sets in every surviving pool. A generation becomes authoritative only after
the identical commit record reaches every set named by its membership marker.
When a failed, canceled, or cleared decommission source rejoins, the last
committed surviving-set ledger seeds it before a new full-membership commit is
allowed; a smaller stale source membership cannot override the largest valid
surviving-set proof, and a membership claim is valid only when every declared
member stores the same proof. This repair appears as
`membership_repair_pending`. A partial commit is
rolled back to the previous stable generation after a crash or leader switch.
The ledger never rewrites pool or rebalance movement state. A new scanner
leader recovers the committed writer epoch and generation, counts an
interrupted attempt as a failure, and requires one successful full namespace
scan after movement clears. Known dirty-usage, expiry, and transition queues
must also reach zero before the ledger returns to `idle`. If the ledger cannot
be read or updated, scanner cycles remain gated and persistence is retried
every five minutes; the management status reports `persistence_unavailable`
until recovery.

Catch-up attempts remain subject to the normal cycle duration, object,
directory, sleeper, and foreground-read budgets. The additional durable rate
window admits at most four attempts per hour and no more than one attempt per
five minutes. Five consecutive failed or interrupted attempts move the ledger
to `retry_exhausted`; accelerated retries stop and a sparse hourly probe is
used instead. A successful probe can return to bounded catch-up.

`pause_backlog.thresholds` reports the exact pause-duration, deferred-cycle,
backlog-size, rate, and failure limits used by the running binary.
`pause_backlog.alert_reasons` identifies exceeded thresholds, exhausted
counters or retries, replica degradation, and persistence failures. The
threshold alerts fire after a 24-hour pause, three movement deferrals in one
unconverged pause episode, or 10,000 known pending work items. The
corresponding unlabeled gauges are:

- `rustfs_scanner_pause_backlog_phase` (`0` idle, `1` paused, `2` catching up,
  `3` retry exhausted);
- `rustfs_scanner_pause_backlog_pause_duration_seconds`;
- `rustfs_scanner_pause_backlog_pending_work_items`;
- `rustfs_scanner_pause_backlog_consecutive_failures`;
- `rustfs_scanner_pause_backlog_rate_limited`;
- `rustfs_scanner_pause_backlog_retry_exhausted`;
- `rustfs_scanner_pause_backlog_alerting`;
- `rustfs_scanner_pause_backlog_replica_degraded`.

## Reading Pacing Pressure

`metrics.pacing_pressure.primary_pressure` summarizes the highest-priority
scanner pressure signal:

| Value | Meaning | Usual response |
|---|---|---|
| `queued_scans` | Set or disk scan queues are backing up. | Lower scanner concurrency or increase pacing delay if user traffic is affected. |
| `cycle_budget` | The last cycle stopped because a runtime/object/directory budget was reached. | Check `last_cycle_partial_reason` and `last_cycle_partial_source`; increase the specific budget if scans need to finish sooner. |
| `throttle_pause` | Scanner sleeps or cooperative yields were observed. | Expected when `idle_mode` is enabled; inspect pause ratios before tuning. |
| `active_scans` | Scanner work is active but not currently queued or budget-limited. | Usually healthy; correlate with CPU/disk metrics. |
| `none` | No current scanner pressure was observed. | No scanner pacing action needed. |

The ratio fields are fractions of the last cycle duration:

- `last_cycle_throttle_sleep_ratio`
- `last_cycle_yield_ratio`
- `last_cycle_total_pause_ratio`

If CPU is high but pause ratios are already high, increasing `scanner.delay` or
`scanner.max_wait` may have limited value. Check active paths, source work, and
disk activity before changing the cycle interval.

## Reading Source Work

`metrics.source_work`, `metrics.current_cycle_source_work`, and
`metrics.last_cycle_source_work` group scanner work by source:

- `usage`
- `lifecycle`
- `bucket_replication`
- `site_replication`
- `heal`
- `bitrot`
- `alerts`

Each source has `checked`, `queued`, `executed`, `failed`, `skipped`, and
`missed` counters. `missed` means the scanner found work but could not admit it
to the downstream queue. `skipped` means the work was intentionally merged or
deduplicated.

Use these counters to decide whether scan progress is limited by scanner pacing
or by a downstream subsystem such as lifecycle transition, replication repair,
or heal admission.

## Reading Heal Operations

The background heal status route is:

```text
POST /v3/background-heal/status
```

It reports scanner-driven bitrot state together with heal queue execution
state. `healQueueLength` and `healActiveTasks` keep the legacy totals.
`healOperations` adds the same totals split by request source and priority:

| Field | Meaning |
|---|---|
| `queueLength` | Total queued heal requests. |
| `activeTasks` | Total running heal tasks. |
| `queuedBySource` | Queued requests split into `scanner`, `admin`, `autoHeal`, and `internal`. |
| `activeBySource` | Running tasks split into `scanner`, `admin`, `autoHeal`, and `internal`. |
| `queuedByPriority` | Queued requests split into `low`, `normal`, `high`, and `urgent`. |
| `activeByPriority` | Running tasks split into `low`, `normal`, `high`, and `urgent`. |

Use this route when `metrics.source_work` shows `heal` or `bitrot` queued or
missed work. Scanner-originated object checks should appear under
`scanner/low` for opportunistic work, while manual admin heal should appear
under `admin/high`. If scanner work grows but admin work remains blocked, treat
that as heal queue pressure rather than scanner pacing pressure.

## Replacement Recovery Completion

`POST /v3/background-heal/status` is an execution-queue view. `state=idle`, zero queue and active counts, an online disk, a readable object, or acceptance of an Admin deep-heal request do not independently prove that a replacement disk contains every erasure shard.

Treat replacement recovery as verified only after the repair task has completed for the exact replacement instance and an operator has confirmed the target disk contains the expected `xl.meta` and data parts for every relevant object version. A replacement that is not mounted, is unsafe to format, loses its marker, or returns a partial target outcome must be treated as deferred or incomplete rather than complete.

The v3 route and its peer status protocol preserve their existing fields for mixed-version clusters. A new node must not infer replacement completion from an old or unavailable peer; regard that information as unknown or degraded until every required peer can report the same replacement instance and verified completion. Do not automate destructive replacement actions from an `idle` observation alone.

`GET /rustfs/admin/v4/heal/replacement-recovery` reports durable automatic replacement records from survivor disks. Its `local.records[]` entries distinguish `waiting_for_replacement`, `running`, `incomplete`, `unrecoverable`, `cleanup_pending`, `completed`, and `unknown`; `local.definitive=false` or any `unknown` record means the node could not prove a local replacement state. Its `cluster` section queries the replacement-recovery peer RPC and sets `cluster.definitive=true` only when the expected peer topology is complete, every peer supports the RPC, every peer snapshot is locally definitive, and all peers report the same replacement records. Old peers, unavailable peers, malformed peer payloads, topology gaps, and generation disagreements are reported as degraded or unknown rather than complete.

Replacement resume and checkpoint files use an independent on-disk schema. A newer reader rejects a future schema rather than continuing with data it cannot interpret, while an older binary cannot safely enforce the new generation fence because it may ignore fields it does not know. Do not roll a cluster back after a replacement generation has started. Complete that recovery with the current-or-newer release; if it cannot complete, keep that version for diagnosis rather than deleting its durable records or continuing with an older binary.

## Reading Replication Repair

`metrics.replication_repair`, `metrics.current_cycle_replication_repair`, and
`metrics.last_cycle_replication_repair` split scanner-discovered replication
repair work by source and repair kind.

Each entry has the same `checked`, `queued`, `executed`, `failed`, `skipped`,
and `missed` counters used by `source_work`, plus:

| Field | Meaning |
|---|---|
| `source` | `bucket_replication` for bucket replication repair, or `site_replication` for site replication boundary signals. |
| `kind` | Bucket repair kinds are `object`, `delete_marker`, `version_purge`, and `existing_object`. Site replication boundary kinds are `passive_requeue` and `active_resync`. |
| `scanner_role` | `repair_admission` means scanner found work and attempted to admit it to a worker queue. `boundary_signal` means scanner is reporting state owned by another runtime. |
| `execution_owner` | `bucket_replication_queue` for bucket replication repair execution, or `site_replication_runtime` for site replication resync execution. |

For bucket replication, `queued` means scanner-discovered repair was admitted
to the replication queue, `missed` means the queue or worker path could not
accept it, and `skipped` means the object did not require a new repair task.

The site replication kinds keep passive scanner discovery separate from active
resync. Scanner status may report site replication boundary counters, but the
scanner should not be treated as the active site replication resync controller.

Use this boundary when interpreting replication pressure:

| Scenario | Scanner source | Repair kind | Scanner role | Execution owner | Operational meaning |
|---|---|---|---|---|---|
| Bucket object, delete-marker, version-purge, or existing-object repair found during a scan | `bucket_replication` | `object`, `delete_marker`, `version_purge`, `existing_object` | `repair_admission` | `bucket_replication_queue` | Scanner found bucket replication repair work and attempted to admit it to the replication queue. |
| Peer-originated or passive site replication work is observed while scanning | `site_replication` | `passive_requeue` | `boundary_signal` | `site_replication_runtime` | Scanner is reporting a passive site-replication boundary signal; it is not taking ownership of active site resync. |
| Admin-triggered or runtime-owned site resync activity is visible in scanner metrics | `site_replication` | `active_resync` | `boundary_signal` | `site_replication_runtime` | Treat this as a boundary/status signal owned by the site replication runtime, not as scanner-controlled repair execution. |

If `site_replication` counters grow while bucket replication counters stay
flat, investigate site replication status and resync state before tuning
scanner pacing. If `bucket_replication` `missed` grows, investigate the bucket
replication worker queue or target health before changing scanner cycle
settings.

## Reading Maintenance Control

`metrics.maintenance_control` derives a source-level control snapshot from
scanner pacing, partial-cycle state, source work, and lifecycle transition
queue state. It does not change scanner scheduling by itself; it explains why a
source is moving, deferred, or blocked. When no scan cycle is currently active,
source-work controls use the last completed cycle so recently missed work stays
visible between scanner passes.

`metrics.maintenance_control.primary_control` summarizes the highest-priority
source state:

| Value | Meaning |
|---|---|
| `blocked_source` | At least one maintenance source found work that could not be admitted or is blocked by a downstream queue. |
| `deferred_source` | At least one source was deferred by a partial scanner cycle or budget-limited pass. |
| `active_source` | At least one source has current-cycle work or queued downstream work. |
| `pacing_pressure` | No source-specific state dominated, but scanner pacing pressure is still visible. |
| `none` | No source-level maintenance control pressure was observed. |

Each `metrics.maintenance_control.sources[]` entry has:

| Field | Meaning |
|---|---|
| `source` | Scanner source such as `usage`, `lifecycle`, `bucket_replication`, `site_replication`, `heal`, `bitrot`, or `alerts`. |
| `state` | `idle`, `active`, `deferred`, or `blocked`. |
| `reason` | Derived reason such as `active_work`, `queued_work`, `partial_cycle`, `missed_work`, `expiry_queue_backlog`, `transition_failed`, `transition_compensation_backlog`, `transition_queue_backlog`, or `transition_queue_full`. |
| `backlog` | Current source-level backlog estimate from queued or missed work. |
| `current_checked` | Current-cycle checked work for this source, or the last completed cycle when no scan cycle is active. |
| `current_queued` | Current-cycle queued work for this source, or the last completed cycle when no scan cycle is active. |
| `current_missed` | Current-cycle work that could not be admitted, or the last completed cycle when no scan cycle is active. |
| `lifetime_missed` | Lifetime missed work counter for context. |
| `partial_cycles` | Partial cycles attributed to this source. |

Use this snapshot before changing scanner controls. For example,
`blocked_source` with `lifecycle/missed_work` points at downstream lifecycle
admission, while `deferred_source` with `usage/partial_cycle` points at scanner
cycle budgets. `lifecycle/expiry_queue_backlog` means scanner-driven expiry or
delete work is still queued or active in the expiry worker pool.
`lifecycle/transition_failed` means transition worker execution failed during
the current or last completed scan cycle, while
`lifecycle/transition_compensation_backlog` means transition compensation is
still pending or running after queue backpressure.

`metrics.lifecycle_expiry` exposes the expiry/delete worker queue observed by
scanner-driven lifecycle work:

| Field | Meaning |
|---|---|
| `current_queue_capacity` | Effective expiry worker queue capacity for this node. |
| `current_queued` | Expiry/delete tasks currently waiting in the worker queue. |
| `current_active` | Expiry/delete tasks currently running in a worker. |
| `current_workers` | Configured expiry worker count. |
| `queue_missed` | Expiry/delete tasks that could not be queued because no worker channel was available or the queue was closed. |
| `scanner_queued` | Scanner-discovered expiry/delete object versions admitted to the expiry queue. |
| `scanner_missed` | Scanner-discovered expiry/delete object versions that could not be admitted. |

## Reading Distributed Metrics

`/rustfs/admin/v3/scanner/status` and `/rustfs/admin/v3/metrics` report the
node that handles the HTTP request. The metrics endpoint does not fan out to
peer nodes. In distributed deployments, query every node explicitly and keep
`by-host=true` enabled so each response includes that node's host view:

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

The `aggregated.scanner` payload preserves the same scanner progress,
checkpoint, pacing, source work, maintenance control, lifecycle expiry, and
lifecycle transition fields used by the local scanner status, but only for the
node that returned the response. The `by_host.*.scanner` payload keeps that
node's host view.
Compare the per-node artifacts externally to find old active paths, partial
checkpoints, pacing pressure, source-level control pressure, or downstream
queue admission problems across the deployment.

## Reading Lifecycle Transition Status

`metrics.lifecycle_transition` focuses on scanner-driven lifecycle transition
work:

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

When `scanner_missed` or `queue_full` rises, scanner lifecycle work is finding
transition candidates faster than the transition queue can accept them. That is
a downstream transition pressure signal, not just a scanner walk pressure signal.

## Tuning Workflow

For symptoms where a mostly idle single-node, single-disk deployment has
sustained CPU usage while the scanner is enabled:

1. Read `/v3/scanner/status`.
2. Check `metrics.pacing_pressure.primary_pressure`.
3. Check `metrics.maintenance_control.primary_control` and source entries
   before changing runtime controls.
4. Check `runtime_config.delay`, `runtime_config.max_wait_seconds`, and
   `runtime_config.cycle_interval_seconds` to confirm the active values and
   their sources.
5. Check `metrics.current_cycle_objects_scanned`,
   `metrics.current_cycle_directories_scanned`, and active paths to confirm the
   scanner is the active work.
6. If `primary_pressure` is `throttle_pause` and pause ratios are low, raise
   `scanner.delay` first.
7. If individual sleeps are too short, raise `scanner.max_wait`.
8. If each scan cycle finishes but starts too often, raise `scanner.cycle`.
9. If scans must be broken into bounded chunks, set one of the cycle budgets:
   `scanner.cycle_max_duration`, `scanner.cycle_max_objects`, or
   `scanner.cycle_max_directories`.
10. Recheck `pacing_pressure`, `maintenance_control`, source work, and
    lifecycle transition status after one or more scanner cycles.

Do not rely only on a longer cycle interval if lifecycle, replication, heal, or
bitrot work must keep moving. Use source work and transition status to confirm
that background maintenance is still making progress.

## Helm

The Helm chart exposes the scanner environment variables under
`config.rustfs.scanner`. Example:

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

Use `extraEnv` for experimental or unrelated environment variables that are not
represented by chart values.
