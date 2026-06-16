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
| `scanner.cycle_max_duration` | `RUSTFS_SCANNER_CYCLE_MAX_DURATION_SECS` | seconds | `0` | Caps one cycle's runtime. `0` disables this budget. |
| `scanner.cycle_max_objects` | `RUSTFS_SCANNER_CYCLE_MAX_OBJECTS` | objects | `0` | Caps objects processed by one cycle. `0` disables this budget. |
| `scanner.cycle_max_directories` | `RUSTFS_SCANNER_CYCLE_MAX_DIRECTORIES` | directories | `0` | Caps directories entered by one cycle. `0` disables this budget. |
| `heal.bitrot_cycle` | `RUSTFS_SCANNER_BITROT_CYCLE_SECS` | seconds | `2592000` | Controls periodic deep bitrot scans. `false`, `off`, `no`, or `disabled` disables periodic deep scans; `0`, `true`, `on`, or `yes` runs deep mode every scanner cycle. |
| `scanner.idle_mode` | `RUSTFS_SCANNER_IDLE_MODE` | boolean | `true` | Enables scanner sleeps and cooperative throttling. |
| `scanner.cache_save_timeout` | `RUSTFS_SCANNER_CACHE_SAVE_TIMEOUT_SECS` | seconds | `30` | Timeout for saving scanner cache; runtime enforces a minimum of `1`. |
| `scanner.max_concurrent_set_scans` | `RUSTFS_SCANNER_MAX_CONCURRENT_SET_SCANS` | count | `0` | Caps concurrent set-level scanner tasks. `0` keeps topology-derived concurrency. |
| `scanner.max_concurrent_disk_scans` | `RUSTFS_SCANNER_MAX_CONCURRENT_DISK_SCANS` | count | `0` | Caps concurrent disk bucket walks per set. `0` keeps disk-count-derived concurrency. |
| `scanner.yield_every_n_objects` | `RUSTFS_SCANNER_YIELD_EVERY_N_OBJECTS` | objects | `128` | Controls how often object loops yield to the async runtime. `0` disables this extra yield. |
| `scanner.alert_excess_versions` | `RUSTFS_SCANNER_ALERT_EXCESS_VERSIONS` | versions | `100` | Version count threshold for scanner alerts. |
| `scanner.alert_excess_version_size` | `RUSTFS_SCANNER_ALERT_EXCESS_VERSION_SIZE` | bytes | `1099511627776` | Retained version byte threshold for scanner alerts. |
| `scanner.alert_excess_folders` | `RUSTFS_SCANNER_ALERT_EXCESS_FOLDERS` | folders | `65538` | Direct subfolder threshold for scanner alerts. |

The `fastest`, `fast`, `default`, `slow`, and `slowest` presets set the base
sleep multiplier, maximum wait, and cycle interval. Use `scanner.delay`,
`scanner.max_wait`, and `scanner.cycle` when the preset is close but one axis
needs a precise override.

## Status Endpoint

The scanner status route is:

```text
GET /v3/scanner/status
```

The request must be authenticated with an admin identity that has
`ServerInfoAdminAction`. The JSON response has two top-level objects:

- `runtime_config`: the effective runtime controls and their value sources.
- `metrics`: scanner work, pressure, checkpoint, lifecycle, replication, heal,
  bitrot, and alert counters.

Example fields to inspect:

```text
runtime_config.speed.value
runtime_config.delay.value
runtime_config.max_wait_seconds.value
runtime_config.cycle_interval_seconds.value
runtime_config.bitrot_cycle_seconds.value
metrics.pacing_pressure.primary_pressure
metrics.pacing_pressure.last_cycle_budget_limited
metrics.lifecycle_transition.current_queued
metrics.lifecycle_transition.scanner_missed
metrics.maintenance_control.primary_control
metrics.source_work
metrics.replication_repair
metrics.scan_checkpoint
```

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

For bucket replication, `queued` means scanner-discovered repair was admitted
to the replication queue, `missed` means the queue or worker path could not
accept it, and `skipped` means the object did not require a new repair task.

The site replication kinds keep passive scanner discovery separate from active
resync. Scanner status may report site replication boundary counters, but the
scanner should not be treated as the active site replication resync controller.

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
