# Scanner Runtime Controls

**Use this when:** tuning scanner pacing or heal runtime knobs, reading `/v3/scanner/status`, deciding whether slow lifecycle/replication/heal progress is a scanner problem or a downstream queue problem, or migrating scanner settings from MinIO.

**Source of truth:** `crates/config/src/constants/scanner.rs` and `crates/config/src/constants/heal.rs` (keys, env names, `DEFAULT_*` constants), `crates/scanner/src/runtime_config.rs` (resolution order, parsing, hot update), `crates/scanner/src/scanner_folder.rs` (env-only scanner knobs, alerts), `crates/scanner/src/sleeper.rs` and `crates/scanner/src/sleeper/io_budget.rs` (throttling and per-disk I/O budget), `crates/heal/src/heal/manager.rs` (`HealConfig::default`), `crates/utils/src/envs.rs` (`EXTERNAL_COMPATIBLE_SUFFIXES`, MinIO env aliases).

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
| `scanner.start_delay` | `RUSTFS_SCANNER_START_DELAY_SECS` (deprecated alias `RUSTFS_DATA_SCANNER_START_DELAY_SECS`) | seconds | unset | Sets an explicit startup delay and, for compatibility, the cycle interval when `scanner.cycle` is unset. When absent, startup uses the smaller of the cycle interval and 60 seconds, with jitter; see [Startup and cycle budgets](#startup-and-cycle-budgets). |
| `scanner.cycle_max_duration` | `RUSTFS_SCANNER_CYCLE_MAX_DURATION_SECS` | seconds | `1800` (`DEFAULT_SCANNER_CYCLE_MAX_DURATION_SECS`) | Caps one cycle's runtime. An explicit `0` disables this budget. |
| `scanner.cycle_max_objects` | `RUSTFS_SCANNER_CYCLE_MAX_OBJECTS` | objects | `0` (`DEFAULT_SCANNER_CYCLE_MAX_OBJECTS`) | Caps objects processed by one cycle. `0` disables this budget. |
| `scanner.cycle_max_directories` | `RUSTFS_SCANNER_CYCLE_MAX_DIRECTORIES` | directories | `0` (`DEFAULT_SCANNER_CYCLE_MAX_DIRECTORIES`) | Caps directories entered by one cycle. `0` disables this budget. |
| `heal.bitrot_cycle` | `RUSTFS_SCANNER_BITROT_CYCLE_SECS` | seconds | `2592000` (`DEFAULT_HEAL_BITROT_CYCLE_SECS`, 30 days) | Controls periodic deep bitrot scans. `false`, `off`, `no`, or `disabled` disables periodic deep scans; `0`, `true`, `on`, or `yes` runs deep mode every scanner cycle. |
| `scanner.idle_mode` | `RUSTFS_SCANNER_IDLE_MODE` | boolean | `true` (`DEFAULT_SCANNER_IDLE_MODE`) | Enables preset scanner sleeps and foreground-read backoff. `false` disables those sleeps; per-disk foreground-priority admission remains active. |
| `scanner.cache_save_timeout` | `RUSTFS_SCANNER_CACHE_SAVE_TIMEOUT_SECS` | seconds | `14` (`DEFAULT_SCANNER_CACHE_SAVE_TIMEOUT_SECS`) | Timeout for saving scanner cache; runtime enforces a minimum of `1` and keeps the default persistence budget within the distributed publication lease. |
| `scanner.recovery_backoff_cap` | `RUSTFS_SCANNER_RECOVERY_BACKOFF_CAP_SECS` | seconds | `600` (`DEFAULT_SCANNER_RECOVERY_BACKOFF_CAP_SECS`) | Caps the wait between automatic startup recovery retries. Must be a positive integer; `0` and malformed values are rejected, so retries never stop. Applies to the next computed retry, not to a wait already in progress. The retry count resets after 24 hours, which bounds a single wait at about one day even with a larger cap. |
| `scanner.max_concurrent_set_scans` | `RUSTFS_SCANNER_MAX_CONCURRENT_SET_SCANS` | count | `4` (`DEFAULT_SCANNER_MAX_CONCURRENT_SET_SCANS`) | Caps concurrent set-level scanner tasks. `0` keeps topology-derived concurrency. |
| `scanner.max_concurrent_disk_scans` | `RUSTFS_SCANNER_MAX_CONCURRENT_DISK_SCANS` | count | `4` (`DEFAULT_SCANNER_MAX_CONCURRENT_DISK_SCANS`) | Caps concurrent disk bucket walks per set. `0` keeps walker-count-derived concurrency. |
| `scanner.walkers_per_disk` | `RUSTFS_SCANNER_WALKERS_PER_DISK` | count | `1` (`DEFAULT_SCANNER_WALKERS_PER_DISK`) | Bucket walkers spawned per local disk of a set. The walkers of one disk share its scanner admission, so a peer still sees one scan session per physical disk; remote disks keep one walker. The resulting walker count is what `scanner.max_concurrent_disk_scans` caps, and the disk count stays the floor. Valid range is `1` through `16` (`MAX_SCANNER_WALKERS_PER_DISK`); `1` keeps today's I/O profile and is the right value for rotational media. The `disk_concurrency_budget` log line reports `walkers_per_disk`, `walkers`, the effective `concurrency_limit`, and its `reason` (`available_walkers`, `configured_cap`, or `count_budget`). |
| `scanner.disk_io_budget_iops` | `RUSTFS_SCANNER_DISK_IO_BUDGET_IOPS` | operations/s | `auto` (`DEFAULT_SCANNER_DISK_IO_BUDGET`) | Per-local-disk pacing for scanner directory opens, enumerated entries, symlink target stats, and object metadata reads. `auto` picks the media default (`100` on rotational or unknown media, `4000` on flash); `off` removes the cap; a number is an explicit rate. See [Per-disk background I/O budget](#per-disk-background-io-budget). |
| `scanner.disk_io_budget_bytes_per_sec` | `RUSTFS_SCANNER_DISK_IO_BUDGET_BYTES_PER_SEC` | bytes/s | `auto` (`DEFAULT_SCANNER_DISK_IO_BUDGET`) | Per-local-disk pacing for the metadata bytes the scanner reads. `auto` picks the media default (`16777216` on rotational or unknown media, `268435456` on flash); `off` removes the cap; a number is an explicit rate. |
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

Use `scanner.delay`, `scanner.max_wait`, and `scanner.cycle` when the preset is close but one axis needs a precise override. With `idle_mode=true`, directory-level sleep is `1ms x factor` and object-level sleep is `time spent on the object x factor`, both capped at `max_wait`; a foreground-read floor of `FOREGROUND_READ_BACKOFF_PER_REQUEST_MS` (10ms) per concurrent GetObject/streaming read, capped at `FOREGROUND_READ_BACKOFF_MAX_MS` (250ms), is applied on top and can exceed the preset's `max_wait` (`crates/scanner/src/sleeper.rs`). The per-disk foreground-priority gate is independent of `idle_mode` and remains active when sleeps are disabled.

### Startup and cycle budgets

Startup and the recurring cycle interval are separate. With a warm, complete usage cache and no active replication, the implicit startup delay uses `min(scanner.cycle, 60s)`, with the existing ±10% jitter. An explicit `scanner.start_delay` retains its configured delay and jitter. A cold usage cache, a valid newer observation requiring refresh, or active replication with existing buckets can skip the startup delay. Starting an attempt promptly does not bypass leader election, recovery, data-movement pauses, or disk admission.

Before this startup cap, a warm cache inherited the whole cycle interval. For example, `scanner.cycle=21600` could wait 5 hours 24 minutes to 6 hours 36 minutes after every process restart. For an explicit prompt start while retaining a six-hour recurring interval, set both `RUSTFS_SCANNER_CYCLE=21600` and `RUSTFS_SCANNER_START_DELAY_SECS=1`. Removing the cycle setting would allow the startup setting to supply the recurring interval through the compatibility fallback.

The startup usage preflight is read-only and has a 30-second deadline. If it cannot finish, the scanner proceeds to its normal fenced attempt; the timeout does not certify usage or bypass recovery checks. Cancellation interrupts both preflight and startup delay. Runtime timing changes use elapsed startup time rather than starting a fresh wait. Updates with the same effective startup delay retain the original jitter sample and deadline, including cycle changes masked by an explicit startup delay or the implicit 60-second cap. Maintenance changes wake the delay so the existing scanner path can inspect the new rules. Pre-leadership retry waits also wake on runtime and maintenance changes.

Cold-cache startup and startup without active replication avoid an extra bucket listing. Maintenance discovery keeps looking for replication after finding lifecycle rules in an earlier bucket. Once replication is found, both scheduling decisions are settled and remaining buckets need no startup inspection. Reapplying an unchanged resolved configuration does not reconfigure per-disk budgets or emit another configuration wakeup.

The `cycle_max_duration`, `cycle_max_objects`, and `cycle_max_directories` controls bound a cycle's work; they do not guarantee that every budget can complete a sweep in bounded chunks. A partial scan preserves useful checkpoints and coverage, but reopening ancestors and revalidating directory entries costs work again. A budget below the minimum replay or traversal cost can repeatedly stop at the same directory. Cross-bucket fairness also needs to be distinguished from progress within one bucket. Use pacing controls to limit background I/O, and choose cycle budgets large enough for the namespace to advance.

Budget cancellation alone does not publish complete usage or acknowledge dirty buckets. A partial cycle must retain that uncertainty; a growing directories-scanned counter is not proof of new coverage or published totals. Inspect checkpoint/frontier changes and publication status across attempts before changing budgets. A budget cancellation does not persist a disabled scheduler flag and does not justify deleting scanner state.

When runtime configuration is applied, `metrics.cycle_interval_seconds`, `metrics.bitrot_cycle_enabled`, and the cycle-budget fields describe that configuration even before the first scan. They do not establish that a cycle is running; use `metrics.current_cycle_active`, `cycle_schedule`, `cycle_recovery`, leadership, and `data_movement_pause` to interpret inactivity. A running cycle retains its existing budget; a configuration update applies to later budget creation. Configuration refreshes retain timeout/no-progress diagnostics until a new cycle starts. If the first refresh fails validation, the retained effective configuration still initializes its metrics while the validation error remains reported; a failed refresh cannot replace a newer accepted configuration. Older releases initialized these configuration metrics only on the first cycle, so zero values during startup did not prove that scheduling was disabled.

### Observing startup and a first publication

The native status response exposes one node-local `metrics.startup` snapshot. Prometheus reads the same snapshot; it is not reconstructed from cycle counters or a persisted usage timestamp. These fields describe the latest local runtime and are not merged across nodes by the realtime API.

| `metrics.startup.phase` | Prometheus `rustfs_scanner_startup_phase` | Meaning |
|---|---:|---|
| `unknown` | 0 | No runtime has been observed, or the report came from an older binary. |
| `initializing` | 1 | Startup recovery-intent discovery/replay or maintenance/default configuration initialization is in progress. |
| `usage_inspection` | 2 | The bounded read-only startup usage probe is in progress. No eventual attempt deadline has been chosen yet. |
| `waiting` | 3 | The runtime is waiting for its actual sampled startup deadline. |
| `attempted` | 4 | The runtime has entered the normal scanner leadership/recovery loop at least once. This does not prove leadership, an active bucket scan, or publication. |
| `stopped` | 5 | The owner ended or was cancelled. Earlier start/attempt timestamps remain available; the planned wait is cleared. |

`runtime_active` means the observation owner still exists, not that it is healthy or ready. `started_at_unix_secs` records that owner's creation. `phase_started_at_unix_secs` and `phase_age_seconds` describe its last transition or changed waiting deadline. `planned_attempt_at_unix_secs` is available only during an actual startup wait; it is a seconds-resolution estimate anchored to runtime start. The scheduler still uses its original monotonic deadline. `wait_remaining_seconds` and `wait_overdue_seconds` are derived from that deadline when read, so they do not require a polling task and remain useful if the wall clock changes. A deadline is eligibility to attempt the scanner's fenced path, not a promised scan-completion time.

`first_attempt_at_unix_secs` is recorded immediately before the first normal scanner leadership acquisition attempt, after startup recovery-intent discovery/replay. Startup intent replay remains `initializing`, including any leadership locks it needs. Lock contention or recovery failure is still an attempt. Repeated attempts retain the first timestamp; restarting creates a fresh observation. Late updates from an older runtime cannot overwrite the new owner's state.

Prometheus exports `rustfs_scanner_runtime_active`, `rustfs_scanner_startup_started_unix_seconds`, `rustfs_scanner_startup_phase_age_seconds`, `rustfs_scanner_startup_planned_attempt_unix_seconds`, `rustfs_scanner_startup_wait_remaining_seconds`, `rustfs_scanner_startup_wait_overdue_seconds`, and `rustfs_scanner_startup_first_attempt_unix_seconds` alongside the phase code. Optional times are zero when unavailable in Prometheus and null in native JSON. No bucket, path, or runtime-generation label is added.

Do not gate every usage alert with `usage_last_durable_success > 0`: that timestamp is local to the process and starts at zero, so the guard hides a runtime that never publishes. It records a durable publication event, including observational publication; it is not proof that all buckets have authoritative converged usage. Keep separate checks for a missing runtime, an overdue startup attempt, no first publication within the expected workload window, and stale publications after a previous success.

For an **expected-enabled SNSD job with no intentional recovery or movement pause**, these example expressions distinguish a missing runtime, stalled initialization, an overdue wait, and a missing first publication. Replace the job selector and windows with the deployment's expected startup and first-sweep times. The first-publication rule is not suitable for every distributed follower; check leadership, `cycle_recovery`, and `data_movement_pause` before applying it across a cluster.

```yaml
groups:
  - name: rustfs-scanner-startup
    rules:
      - alert: RustFSScannerRuntimeMissing
        expr: rustfs_scanner_runtime_active{job="rustfs-snsd"} == 0
        for: 5m
      - alert: RustFSScannerStartupInspectionStalled
        expr: ((rustfs_scanner_startup_phase{job="rustfs-snsd"} == 1) or (rustfs_scanner_startup_phase{job="rustfs-snsd"} == 2)) and (rustfs_scanner_startup_phase_age_seconds{job="rustfs-snsd"} > 120)
        for: 1m
      - alert: RustFSScannerStartupAttemptOverdue
        expr: (rustfs_scanner_startup_phase{job="rustfs-snsd"} == 3) and (rustfs_scanner_startup_wait_overdue_seconds{job="rustfs-snsd"} > 30)
        for: 1m
      - alert: RustFSScannerNoFirstUsagePublication
        expr: (rustfs_scanner_startup_phase{job="rustfs-snsd"} == 4) and (rustfs_scanner_startup_phase_age_seconds{job="rustfs-snsd"} > 600) and (rustfs_scanner_usage_last_durable_success_unix_seconds{job="rustfs-snsd"} == 0)
        for: 2m
```

An alert means the expected attempt or publication has not been observed; inspect progress, checkpoints, budgets, leadership, and pause reasons rather than treating every case as a dead scanner. None of these observations fills a missing success timestamp or acknowledges incomplete coverage.

### Walkers per disk

The scanner walks the buckets of an erasure set with one walker per local disk by default, so a single-disk deployment walks one bucket at a time no matter how `scanner.max_concurrent_disk_scans` is set. `scanner.walkers_per_disk` (`RUSTFS_SCANNER_WALKERS_PER_DISK`) multiplies each local disk into that many walkers.

- Keep the default `1` on rotational media: several walkers on one spindle compete for seeks and slow every walk down.
- On flash, `2` to `4` is the useful range. Raise it when the scanner status shows cycles ending on the cycle budget (`metrics.pacing_pressure.last_cycle_budget_limited`) or when many buckets are never reached within a cycle, and the disk has idle I/O capacity.
- The walkers of one disk share that disk's scanner admission, so a peer still sees one scan session per physical disk; remote disks keep one walker. `scanner.max_concurrent_disk_scans` still caps the resulting walker count, and the disk count stays the floor.
- Confirm the setting took effect from `runtime_config.walkers_per_disk.value` in the admin scanner status and from the `disk_concurrency_budget` log line, which reports `walkers_per_disk`, `walkers`, the effective `concurrency_limit`, and the `reason` it was chosen.
- Helm: `config.rustfs.scanner.walkers_per_disk`; Docker or systemd: the environment variable. The key is additive, so an older binary ignores it.

### Environment-only scanner knobs

These have no persistent key and are read from the environment only.

| Environment variable | Default (constant) | Effect |
|---|---|---|
| `RUSTFS_SCANNER_ENABLED` (deprecated alias `RUSTFS_ENABLE_SCANNER`) | `true` (`scanner_enabled_from_env`, `rustfs/src/module_switches.rs`) | Starts the data scanner. Scanner-produced heal candidates require the heal manager, so the manager and MRF consumer also start when scanner is on, even if `RUSTFS_HEAL_ENABLED=false`. |
| `RUSTFS_SCANNER_ALERT_COOLDOWN_SECS` | `86400` (`DEFAULT_SCANNER_ALERT_COOLDOWN_SECS`, `scanner_folder.rs`) | Per-(kind, bucket, object) cooldown between S3 excess-alert events; `0` emits every cycle. See [Scanner Excess Alerts](scanner-excess-alerts.md). |
| `RUSTFS_SCANNER_DEEP_VERIFY_COOLDOWN_SECS` | `60` (`DEFAULT_SCANNER_DEEP_VERIFY_COOLDOWN_SECS`, `scanner_folder.rs`) | Objects modified within this window are skipped by deep (bitrot) verification in the current cycle. |
| `RUSTFS_HEAL_OBJECT_SELECT_PROB` | `1024` (`DEFAULT_HEAL_OBJECT_SELECT_PROB`, `scanner_folder.rs`) | Sampling divisor for scanner-originated heal checks: roughly one object in N per cycle is selected for a low-priority heal check. |
| `RUSTFS_DATA_USAGE_UPDATE_DIR_CYCLES` | `16` (`DATA_USAGE_UPDATE_DIR_CYCLES`, `scanner_folder.rs`) | Every N cycles a compacted directory is re-descended instead of reusing its cached usage. `1` forces re-descent every cycle (used by lifecycle e2e lanes). |
| `RUSTFS_DATA_USAGE_FAILED_OBJECT_TTL_SECS` | `86400` (`DEFAULT_FAILED_OBJECT_TTL_SECS`, `scanner_folder.rs`) | Retention of per-bucket failed-object entries in the usage cache. |
| `RUSTFS_DATA_USAGE_FAILED_OBJECTS_MAX` | `10000` (`DEFAULT_FAILED_OBJECTS_MAX`, `scanner_folder.rs`) | Cap on retained failed-object entries per bucket. |

### Single-node, single-drive startup

RustFS does not turn off background services based on drive count. This follows MinIO's service-level startup model: MinIO starts auto-heal/MRF from its server-pool initialization and starts the data scanner separately when its scanner switch is on; its single-drive branch changes background-heal state handling rather than silently disabling both service initializers ([server startup](https://github.com/minio/minio/blob/master/cmd/server-main.go#L1050-L1055), [heal startup](https://github.com/minio/minio/blob/master/cmd/background-newdisks-heal-ops.go#L373-L387), [scanner single-drive path](https://github.com/minio/minio/blob/master/cmd/data-scanner.go#L75-L87)). RustFS starts the scanner by default and starts the heal manager/MRF consumer whenever either `RUSTFS_SCANNER_ENABLED` or `RUSTFS_HEAL_ENABLED` is true. Startup logs report the configured switches and effective scanner, heal-manager, and MRF-consumer states.

| `RUSTFS_SCANNER_ENABLED` | `RUSTFS_HEAL_ENABLED` | Effective behavior |
|---|---|---|
| `true` | `true` | Scanner usage, lifecycle/transition and replication discovery run; heal manager and MRF consumer run. |
| `true` | `false` | Scanner remains on; heal manager and MRF consumer still start because scanner work can submit heal requests. |
| `false` | `true` | Scanner-owned usage/lifecycle/replication discovery pauses; heal manager and MRF consumer remain available. |
| `false` | `false` | Scanner and manager-backed heal/MRF consumer do not start. Usage freshness and scanner-driven lifecycle/replication discovery pause; manager-backed admin heal is unavailable, and durable MRF work waits for the consumer to return. |

For a single slow drive, keep the default services enabled and use the per-disk foreground-priority gate and scanner pacing controls first. To opt into a maintenance-off mode explicitly, set both `RUSTFS_SCANNER_ENABLED=false` and `RUSTFS_HEAL_ENABLED=false`; changing disk count alone never selects that mode. `RUSTFS_SCANNER_BITROT_CYCLE_SECS=off` only disables periodic deep bitrot work and does not stop regular scanner cycles or checkpoints. `RUSTFS_HEAL_AUTO_HEAL_ENABLE=false` disables automatic heal policy but does not prevent the heal manager from starting when the scanner needs its request consumer.

### Cycle budgets and cadence

When the cycle duration control is unset, RustFS uses the finite 1800-second default. An explicit `0` preserves the compatibility behavior of an unbounded cycle; object and directory budgets likewise remain unbounded when explicitly set to `0`. Invalid or overflowing duration environment values are configuration errors rather than silent fallback values.

When a finite deadline expires, RustFS cancels cooperative scanner work and waits only for the existing bounded shutdown window. A non-yielding I/O future is dropped after that window. RustFS then attempts a higher leadership epoch so late cycle, usage, cache, and remote writes from the old generation fail closed. If the worker cannot stop cooperatively, the cycle state was not confirmed durable, or that epoch fence cannot be durably persisted, the scanner reports `recovery-required`; it does not claim an uncooperative cursor was saved.

An explicit `scanner.cycle` or `RUSTFS_SCANNER_CYCLE` is a minimum inter-cycle cadence: dirty-usage notifications do not bypass that configured interval. The default adaptive policy continues to use dirty-usage notifications to wake the scanner between timer-driven cycles.

Under continuous writes, a structurally complete walk can finish as `superseded`. Its durable observation remains separate from quota authority and leaves dirty work pending. Superseded retries may reuse that complete inventory to refresh dirty buckets, even while authority bootstrap is pending; missing or invalid coverage still forces a full walk. Ordinary maintenance timers, Deep scans, active lifecycle/replication and changed maintenance configuration retain full scans. Server info and datausageinfo can display complete observations, while Last Completed Scan advances only after an authoritative completed cycle. A write-free window is therefore not required to display observed counts, but arbitrary continuous writes do not guarantee authoritative completion. Historically dirty buckets remain eligible for rewalk, so narrower retries do not guarantee shorter cycles.

Superseded convergence retries retain exponential backoff for usage-only work. Active lifecycle or replication rules, and uncertain maintenance inspection, cap that retry interval at the ordinary configured cadence so complete observations cannot defer regular maintenance. An explicit cycle remains the duty-cycle floor. Convergence mode still suppresses dirty/activity shortcuts and does not grant quota authority; deferred backend/proof retries and pause catch-up limits retain their existing scheduling. Cycle runtime, pacing, and disk admission can still delay completed maintenance. Sustained supersedes with active maintenance may therefore run more full scans than usage-only convergence; use the existing pacing controls to manage that background I/O.

The telemetry recorder exports `rustfs_scanner_cycle_wakeups_total{reason}` for the reason actually selected when the outer scheduler wait returns. It does not count every notification or the internal activity-probe timer slices. Reasons are `timer`, `dirty_usage`, `movement_generation`, `cluster_activity`, `cluster_maintenance`, `cluster_activity_unavailable`, `runtime_config`, `maintenance_config`, `recovery`, `leader_lock_lost`, and `cancelled`.

`rustfs_scanner_cycle_attempts_total{reason}` counts arrivals at the existing budget-fenced cycle-runner call boundary, after the scheduler's pause, catch-up rate-limit, and leader checks. The startup attempt uses `reason="initial"` and has no matching wakeup. Configuration changes, cancellation, leader loss, or a blocked pause/catch-up gate can produce a wakeup without an attempt. An attempt may still be preempted inside the runner by cancellation, a fence, or a deadline; it does not prove an actual walk, completed cycle, healthy service, or idle convergence. Both families are process-local counters emitted through the existing telemetry recorder; without an installed recorder they are no-ops. No new setting or background probe is introduced.

## Single-disk clean-idle scheduling

An erasure single-disk deployment using the built-in cycle and bitrot defaults automatically backs off repeated clean idle scans instead of walking the same unchanged namespace every minute. Each successful timer-driven cycle that finds no dirty usage or unresolved maintenance work doubles the next interval. The status endpoint reports the effective interval and multiplier.

The backoff is reset to the base interval by object or bucket mutations, lifecycle or replication configuration changes, partial or failed cycles, usage persistence failures, and unresolved scanner-originated heal or bitrot work. Active lifecycle or replication rules keep the base cadence. An explicit cycle, a non-default persisted speed, any environment speed or start-delay override, an environment bitrot override, or a non-default persisted active bitrot cycle also keeps the configured cadence rather than applying the automatic policy. Persisting `scanner.speed=default` or the default bitrot cycle is normalized to the built-in default and therefore keeps automatic scheduling enabled.

Lifecycle and replication configuration inspection is bounded so a slow metadata read cannot stall scanner startup or scheduling. A failed or timed-out inspection keeps the base cadence and is retried after 5 minutes, doubling up to a maximum of 60 minutes while failures continue. A lifecycle or replication configuration change wakes the scanner and retries inspection immediately.

With the default 30-day bitrot cycle, the clean-idle interval is capped at the bitrot cycle divided by the object selection window (about 42 minutes with the default `RUSTFS_HEAL_OBJECT_SELECT_PROB`), which preserves the intended wall-clock bitrot coverage. If periodic bitrot is disabled, the clean-idle cap is 24 hours. The effective interval is jittered by up to 10 percent to avoid synchronized scanner starts.

## Per-disk background I/O admission

Local disk wrapper operations share an admission gate across foreground requests and scanner work. Foreground I/O never waits for a scanner permit. A queued scanner operation may start when its wrapper is idle or after eight foreground operation completions. If it is at the head of the background queue and no background operation is active, it starts after at most 100 ms; time queued behind an active background operation is additional. Only one background operation per wrapper runs at a time. Scanner directory reads use bounded batches, and scanner metadata reads release their permit before sending data to the listing consumer. GET streaming reads hold permits only while a disk read is pending, not for the lifetime of a client response. Cancellation removes queued waiters; a permit is released after its last operation or blocking worker holder exits. Tokio filesystem work already dispatched before cancellation may continue beyond the caller's future lifetime.

The `rustfs_disk_io_admission_active` foreground gauge is sampled while background waiters are queued; `rustfs_disk_io_admission_active{class="background"}` reports the active background permit, while `rustfs_disk_io_admission_waiters`, `rustfs_disk_io_admission_total`, and `rustfs_disk_io_admission_wait_seconds` report queued work, background admission counts, and wait time by disk endpoint. `rustfs_scanner_cache_serialized_bytes_total` counts logical encoded cache bytes; `rustfs_scanner_cache_serialize_seconds` records encoding time; `rustfs_scanner_cache_persist_attempt_seconds` records save-attempt duration and retry result, before ambiguous-write reconciliation.

This is logical operation admission, not an OS disk scheduler: foreground and background operations may overlap after a background permit is granted, and the gate does not promise a bandwidth or IOPS limit. It is scoped to each `LocalDiskWrapper`; separate paths to one physical device and remote disk RPCs do not share this gate. It also does not observe or schedule unrelated host processes that share the physical device.

## Per-disk background I/O budget

The admission gate orders scanner I/O behind foreground requests but does not bound how much background I/O an idle disk sees. `scanner.disk_io_budget_iops` and `scanner.disk_io_budget_bytes_per_sec` pace scanner work in addition to that gate. Each local disk owns one token bucket per axis (`crates/scanner/src/sleeper/io_budget.rs`); charges cover directory opens, each enumerated entry, symlink target stats, optional `xl.meta` size stats, and metadata reads. A charge that exceeds the remaining budget delays that walker. Tokio may prefetch a bounded directory chunk, and waits are capped to preserve progress, so these settings pace scanner work rather than enforcing a strict OS-level IOPS or bandwidth ceiling. The budget applies whether or not `idle_mode` is enabled and does not control unrelated host processes.

Only `xl.meta` files enter object metadata accounting. Other payload files consume entry charges without extra size stats or metadata-read charges. An `xl.meta` symlink reuses its resolved target size for the byte charge. Cancellation interrupts folder pacing, budget waits, and directory/stat waits. Filesystem work already dispatched to a blocking worker can still finish.

UUID candidates under a directory without `xl.meta` are classified during their normal directory traversal. Each directory open and entry is charged once; traversal yields every 64 entries and continues under the budget until a regular `part.N` is found or EOF is reached. A full batch is not evidence of absence. Candidate metadata accounting is deferred until classification finishes. A data directory retains the existing missing-parent-metadata and non-destructive heal path; a namespace continues from the same listing, without a second probe/open/entry pass. Data directories do not consume namespace-directory quota or a preset folder delay; a candidate proven to be a namespace consumes that quota and delay once.

Candidate entries use the normal raw-enumeration progress and checkpoint path. Cancellation or an iteration error leaves classification incomplete, never an EOF or successful coverage result. A child already absent or replaced by a non-directory at open is handled as vanished, without stale namespace totals or a missing-metadata heal request. A replacement retains the existing coverage gap; mid-iteration errors never become EOF. A clean namespace covered by a restored frontier reuses its prior totals only after current classification reaches EOF; metadata is not read or charged to object quota again. A later data candidate prevents completed namespace siblings from certifying the unresolved parent. Existing raw-page indexes are membership receipts, not portable filesystem seek offsets: a later cycle or process restart reopens and revalidates the directory, with actual reads charged again. This does not guarantee that a directory larger than the configured cycle budget can finish within that budget.

- Media is detected once per disk path from the backing block device's `rotational` flag on Linux, through partitions, `dm-*`, and software RAID; any spindle in the stack counts as rotational. Undetectable media, including non-Linux hosts and overlay filesystems without a bind-mounted volume, uses the rotational defaults because a spindle is the case that needs protection.
- `auto` resolves to `100` operations/s and `16 MiB/s` on rotational or unknown media and to `4000` operations/s and `256 MiB/s` on flash. The rotational values leave roughly half of a 7200 rpm spindle's random reads to foreground traffic; the flash values are a few percent of a SATA SSD. The `fastest` speed preset resolves `auto` to no cap; explicit numbers always apply.
- A budget of `0` or any tiny value never starves a walk: one charge is admitted per second (`SCANNER_DISK_IO_BUDGET_MAX_WAIT`), and an oversized charge owes at most one extra second of debt.
- The walkers of one disk (`scanner.walkers_per_disk`) share that disk's budget. Remote disks are paced by their owning node.
- Changes take effect at the next charge without a restart.

The status endpoint lists each disk under `disk_io_budget` with its detected `media`, the effective `iops` and `bytes_per_sec` (`null` when uncapped), charged totals, and accumulated `wait_events` / `wait_seconds`. Budget waits are also reported per cycle as `metrics.current_cycle_disk_io_budget_wait_events` / `_duration_seconds` and `metrics.last_cycle_disk_io_budget_wait_events` / `_duration_seconds`, as `metrics.pacing_pressure.last_cycle_disk_io_budget_wait_ratio` with the `disk_io_budget` primary pressure, and in `metrics.usage_freshness.disk_io_budget_wait_events`, `disk_io_budget_wait_millis`, and `last_disk_io_budget_wait_unix_secs`. Prometheus exports `rustfs_scanner_disk_io_budget_waits_total` and `rustfs_scanner_disk_io_budget_wait_seconds` by disk. The `scanner_disk_io_budget` log line reports the resolved media and rates once per disk.

Like admission, this is in-process pacing: it does not schedule other host processes. Deep bitrot verification reads are issued by the heal subsystem and are paced by heal controls, not by this budget.

## Status Endpoint

```text
GET /v3/scanner/status
```

The request must be authenticated with an admin identity that has `ServerInfoAdminAction`. The JSON response has these scanner-specific top-level objects:

| Object | Content |
|---|---|
| `runtime_config` | Effective runtime controls and their value sources. |
| `cycle_schedule` | Current effective cycle interval and clean-idle backoff state. |
| `cycle_recovery` | This node's cycle-state recovery observation, retry budget, and reason. |
| `bitrot` | How the current cycle's scan mode was decided: whether Deep is supported on this topology, where the bitrot cadence start came from, and the recorded start. See [Deep scan cadence state](#deep-scan-cadence-state). |
| `disk_io_budget` | One entry per local disk with detected media, effective per-disk I/O budget, charged totals, and accumulated budget waits. |
| `metrics` | Scanner work, pressure, checkpoint, lifecycle, replication, heal, bitrot, and alert counters. |
| `data_movement_pause` | Global-pause policy, current movement reason, operation epoch, start time, duration, and estimated movement work items. |
| `pause_backlog` | Replicated durable pause ledger, post-pause catch-up phase, rate window, retry state, thresholds, and active alert reasons. |
| `catch_up_estimate` | Movement work plus current dirty-usage and already discovered lifecycle queues. |
| `heal_observation` | This process's bounded canonical heal observations, including retained pending tasks, typed terminals, and unknown integrity. |

`metrics.usage_publication` describes one local publication attempt with its own
`scanner_epoch` and `cycle`, even after `metrics.current_cycle` returns to zero.
Its `class` distinguishes `progress_only`, `observational`, `complete_pending`,
`complete_converged`, and `unknown`; `outcome` retains the persistence result.
`pending_bucket_count` comes from the confirmed authoritative snapshot. It is
null for progress-only, observational, failed, or unverified publication, and
zero only for a confirmed converged snapshot. The live dirty-usage count can
change independently after publication.

An existing same-cycle snapshot may differ from the current retry. Its pending
count comes from the existing target bytes, never the retry's count. A cached
baseline or companion alone cannot confirm the current target; without an
actual target read, confirmed write, or existing root readback proof, the class
remains unknown. `snapshot_updated_at_unix_secs` is the snapshot's timestamp;
`observed_at_unix_secs` is when this process recorded the observation.

Starting another attempt replaces this record with running/unknown. Loss of
leadership, stopping, reset, or restart clears it, and a late old attempt cannot
restore it. These are local observations, not a restart-surviving or cluster-wide
history. Publication convergence does not certify cycle completion, heal task
completion, or verified object content.

Example fields to inspect:

```text
runtime_config.speed.value
runtime_config.delay.value
runtime_config.max_wait_seconds.value
runtime_config.cycle_interval_seconds.value
runtime_config.bitrot_cycle_seconds.value
runtime_config.walkers_per_disk.value
cycle_schedule.effective_interval_seconds
cycle_schedule.clean_idle_backoff_enabled
cycle_schedule.clean_idle_backoff_multiplier
metrics.pacing_pressure.primary_pressure
metrics.pacing_pressure.last_cycle_budget_limited
metrics.pacing_pressure.last_cycle_disk_io_budget_wait_ratio
metrics.usage_freshness.disk_io_budget_wait_events
bitrot.deep_scan_supported
bitrot.bitrot_start_kind
bitrot.bitrot_start_source
disk_io_budget[].media
disk_io_budget[].iops
disk_io_budget[].wait_seconds
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

### Deep scan cadence state

`bitrot` reports how the scanner decided the scan mode of the running or most recent cycle:

| Field | Meaning |
|---|---|
| `deep_scan_supported` | `false` on an erasure single-disk deployment, where scanner-originated object heal sampling is unsupported and Deep mode is never selected (rustfs/rustfs#8126). |
| `bitrot_start_kind` | `deep_window` when a recorded Deep window start is in effect, `anchor` when no Deep window is recorded anywhere and the cadence counts from this process's first cycle without one, `none` before the first cycle. |
| `bitrot_start_source` | Where the reference came from: `persisted` (read back from the background-heal object), `memory` (the in-memory anchor, or a window start this process recorded that storage has not returned, for example because the save was skipped without movement admission), or `none`. |
| `bitrot_start_cycle`, `bitrot_start_unix_secs` | The recorded window start, or the anchor time with cycle `0`; both `0` when none. |
| `current_scan_mode` | `normal` or `deep`. |

A cycle that finds no recorded Deep window runs Normal and anchors the cadence in process memory; nothing is persisted until the configured bitrot cycle has elapsed since that anchor (or on every cycle when the cycle is `0`), at which point the Deep cycle records the window start exactly as before. An anchor never opens the Deep selection window: a missing record used to be read as a window that started at cycle `0`, which made every cycle below the selection window run Deep whenever the start could not be read back (rustfs/rustfs#8015). The window start also stays in process memory across a skipped or failed save, so the next cycle continues the window instead of reopening one; when a persisted and an in-memory start both exist, the newer one wins and ties go to the persisted record. `RUSTFS_SCANNER_BITROT_CYCLE_SECS=off` still never selects Deep, deployments whose background-heal state persists normally keep their existing cadence, and a restart resets the anchor.

### Periodic checkpoint persistence

`metrics.scan_checkpoint` is the in-memory resume hint; it does not confirm a durable save. `metrics.scan_checkpoints_persisted` lists the latest confirmed periodic save for each `(pool_index, set_index)` handled by this process, with its bucket, `saved_unix_secs`, and optional checkpoint cursor. It retains one observation per set, is reset on process restart, and does not include the final bucket save. Query the peer executing the walk for remote-worker observations. `metrics.scan_checkpoint_save_failures` counts failed periodic saves, including failures to refresh CAS revisions after a write; failed attempts do not advance the confirmed save observation.

An ordinary failed save drops that snapshot and leaves the bounded checkpoint receiver active, so the next producer interval tries again. A CAS conflict or an unreadable revision after a successful write cancels the bucket attempt and reloads persisted state through the existing retry path. Leader, cycle, and publication fencing remain mandatory. This preserves future periodic save attempts for a continuing bucket walk.

Periodic snapshots include the suspended ancestor accounting needed to connect a nested active branch to the bucket root. Completed coverage is sealed independently of raw enumeration bookkeeping. A valid coverage frontier can therefore skip completed subtrees after a fenced leader handoff without counting unfinished work as completed. Emission remains bounded by the existing interval/object thresholds and capacity-one channel; invalid snapshot attempts also consume the producer interval so they cannot repeatedly clone the cache on every object.

A resumed walk retains its previous durable coverage while initial enumeration has not recopied the covered subtrees. Unfinished out-of-order heal revisits cannot be certified as completed prefix work, including in cancellation snapshots.

Shared compacted accumulators defer periodic emission until the owning subtree completes; replay inside that compacted subtree remains possible after an interruption. When bucket checkpoint preparation returns `Reset` for existing resume state, it logs `checkpoint_reset` with the validation reason. The `scan_checkpoint_used` counter includes validated coverage adoption by the forward sweep.

A confirmed periodic save does not prove restart resumability. Adoption still validates bucket/set identity and, for non-empty caches, connected cache scope. Skipping completed subtrees additionally requires a validated coverage frontier; raw enumeration state alone does not prove completed subtree coverage.

### Cycle recovery on followers

Cycle recovery status is local to each node. A transient cycle-state read failure can exhaust the short retry budget and enter `backoff`; the observation stays retryable, the enabled scanner keeps retrying leadership with an exponential delay capped by `scanner.recovery_backoff_cap` (default ten minutes) or sooner when recovery is explicitly woken, and `next_attempt_at_unix_secs` together with `reason` name the next attempt. If the next lock attempt is contended or times out waiting for `leader.lock`, the node records a follower iteration and clears only its unclassified `transient` or `backoff` observation. Normal follower retry cadence resumes without restarting the process.

This clears local retry bookkeeping, not persisted cycle state or recovery markers. Classified recovery states, cleanup-pending resets, and usage-floor failures remain unchanged. Any later leadership claim revalidates durable cycle state before scanning. A healthy follower recovery status does not prove that leader-dispatched scans are progressing; inspect the work counters separately.

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
| scanner leader lock is busy | retry with an async recovery intent or wait for the current leader to restart/release the lock |

When a running scanner leader already holds `leader.lock`, submit a durable async recovery intent instead of repeatedly calling the synchronous reset:

```json
{
  "mode": "full-rebuild",
  "async": true,
  "idempotency_key": "<stable-operator-request-id>"
}
```

The async form returns HTTP 202 when a new or replayable intent is accepted. Reusing the same `idempotency_key` is safe for retries of the same operator action; a different payload for the same key is rejected as a conflict. Poll `GET /v3/scanner/usage-state/recovery-intents/{intent_id}` until the intent reaches a terminal state.

A successful reset leaves usage state in `bootstrap-pending` only as a fenced rebuild marker. With the scanner enabled, the leader must treat that marker as pending full-rebuild work: clean-idle backoff must not extend the wait, an empty pause-backlog catch-up ledger must not rate-limit the rebuild, and the next admitted scanner cycle must rebuild full inventory. After a complete observation is durably saved, superseded retries may refresh dirty buckets using its validated coverage; bootstrap remains pending until authoritative publication replaces the marker.

When both the cycle body and usage JSON are corrupt, an enabled scanner can recover under its existing leader lock if the current cycle revision still has a recognized fencing header and a matching strict `blocked`/`corrupt` quarantine marker. It retains the original cycle and usage bytes under `.bloomcycle.bin.dual-recovery/`, records the exact expected replacement counter against the observed revisions, CAS-rebuilds that counter above the surviving epoch and cycle lower bounds, and CAS-publishes the existing `bootstrap-pending` marker. No recovery marker or usage slot is deleted. A restart after the counter commit resumes only when the durable expected counter and all source bindings match exactly; a different valid counter does not authorize completion. A committed bootstrap whose completion response was lost already permits the ordinary full rebuild.

This automatic path does not replace a usable primary or backup floor, interpret an unknown structured usage format as corruption, or recover a future-schema counter or an operator's `cleanup-pending` reset. Backend errors, movement, ownership loss, changed revisions, exhausted counters, and conflicting evidence defer recovery. Completely unreadable fencing headers remain fail-closed: the leader lock and movement lease cannot prove a replacement epoch exceeds a lost historical epoch. Keep the retained bytes for diagnosis and use the current binary's supported recovery surfaces; do not delete the evidence to force startup. Until a complete measured baseline publishes, the pending marker supplies no bucket sizes, so legacy quota admission without a persisted floor remains unavailable.

## Cleanup With The Scanner Disabled

With `RUSTFS_SCANNER_ENABLED=false`, startup makes one controlled attempt to finish a previously persisted cycle reset whose validated recovery marker is already `cleanup-pending`. This is metadata cleanup only: it does not start the ordinary scanner loop, scan namespaces, accept a new reset request, or automatically perform a usage-state `full-rebuild`. Missing, merely `blocked`, unknown-version, unknown-phase, or corrupt markers do not authorize an automatic reset.

The attempt uses the existing leader lock and revalidates the observed marker revision and phase after acquiring it. A busy leader or data-movement pause leaves the marker intact and is reported through `cycle_recovery.state` and `cycle_recovery.reason` in the existing scanner status response. There is no automatic retry loop while disabled. After resolving the blocker, explicitly retry `POST /v3/scanner/cycle-state/reset` with `{"mode":"full-rescan"}`, or restart to make another controlled attempt. Cycle-state reset remains a synchronous HTTP 200 operation; usage-state reset also supports the async recovery-intent form described above.

The startup probe is cancellation-aware and uses the existing cache persistence I/O timeout. Shutdown waits only for the existing server shutdown timeout. If the cleanup task cannot join in that window, the `scanner_cleanup_not_joined` warning means completion is unconfirmed, not drained. The task is not force-aborted or force-unlocked while its runtime remains alive; it retains its existing namespace/admission guards, and durable marker/fence state remains authoritative. Inspect status before retrying. This does not establish a hard deadline for an unresponsive storage operation or prove that I/O has drained when the process or runtime subsequently exits. Task-ownership timeout tests are not storage fsync, commit-tail, or process-crash durability evidence.

## Data Movement Pauses

RustFS uses a `global_pause` policy while pool decommission or rebalance can hide scanner metadata: usage publication, lifecycle discovery, tier cleanup discovery, scanner-originated heal and bitrot checks, and replication discovery are deferred together. A failed or canceled decommission remains a publication barrier until an operator retries or clears it. The same pause and estimate objects are included in `GET /v3/ilm/expiry/status`.

| Field | Meaning |
|---|---|
| `data_movement_pause.reasons` | In-process decommission worker state combined with durable pool and rebalance operation metadata. Exhausted operation epochs or movement generations fail closed and appear as explicit reasons. |
| `data_movement_pause.duration_seconds`, start time, `movement_backlog_work_items` | From durable metadata; a worker-only or exhausted-counter snapshot can report `paused=true` with zero start time and backlog. `movement_backlog_work_items` counts remaining movement bucket work units, not expired objects. |
| `catch_up_estimate` | Movement estimate plus dirty-usage buckets and lifecycle items discovered before or during the pause. `undiscovered_ilm_items_known=false` because a global pause cannot count newly expired objects without scanning; use `usage_baseline_unix_secs` to judge the estimate's age. |
| `pause_backlog.persistence_state` | `degraded` when the `.scanner-pause-backlog.json` ledger cannot be claimed or updated; `error` carries the reason. Scanner cycles keep running untracked behind the data-movement fence, and the ledger claim is retried on each scheduler wake at most every five minutes. `unavailable` means no scanner runtime on this node has attempted a claim. |
| `pause_backlog.phase` | `idle`, `paused`, `catching_up`, or `retry_exhausted`. The ledger returns to `idle` only after one successful full namespace scan and zero known dirty-usage, expiry, and transition queues. |
| `membership_repair_pending` | A rejoining decommission source is being re-seeded from the last committed surviving-set ledger before a new full-membership commit is allowed. |
| `rolled_forward_partial_commit` | Interleaved writers left no surviving commit and different stable fields; the next writer claim, or decommission retirement of the ledger, stabilizes the unique newest stable ledger. |
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
| `disk_io_budget` | The last cycle waited on the per-disk background I/O budget. | Expected on rotational media; raise `scanner.disk_io_budget_iops` or `scanner.disk_io_budget_bytes_per_sec` only when foreground latency has headroom. |
| `throttle_pause` | Scanner sleeps or cooperative yields were observed. | Expected when `idle_mode` is enabled; inspect pause ratios before tuning. |
| `active_scans` | Scanner work is active but not currently queued or budget-limited. | Usually healthy; correlate with CPU/disk metrics. |
| `none` | No current scanner pressure was observed. | No scanner pacing action needed. |

The ratio fields `last_cycle_throttle_sleep_ratio`, `last_cycle_disk_io_budget_wait_ratio`, `last_cycle_yield_ratio`, and `last_cycle_total_pause_ratio` are fractions of the last cycle duration. If CPU is high but pause ratios are already high, increasing `scanner.delay` or `scanner.max_wait` may have limited value; check active paths, source work, and disk activity before changing the cycle interval.

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

Durable partial-write responsibilities also have node-local MRF metrics:

| Metric | Meaning |
|---|---|
| `rustfs_heal_mrf_queue_depth` | In-memory MRF work plus retained durable responsibilities, including held and operator-accepted unverified entries. |
| `rustfs_heal_mrf_unverified_legacy` | Durable `PartialWrite` entries whose completed deep check found healthy legacy data without an independent payload identity proof. The journal entry remains intact; the current process pauses automatic re-dispatch for that entry. |
| `rustfs_heal_mrf_unverified_legacy_oldest_age_seconds` | Age of the oldest such retained responsibility on this node. |
| `rustfs_heal_mrf_legacy_generation_unknown` | Durable entries replayed without a source bucket-incarnation binding; they are not dispatched until an operator chooses an explicit disposition. |
| `rustfs_heal_mrf_bucket_incarnation_changed` | Durable entries whose source bucket incarnation differs from the current bucket; the old intent is parked rather than sent to the recreated bucket. |
| `rustfs_heal_mrf_operator_accepted_unverified` | Responsibilities for which an administrator explicitly accepted the unresolved data risk. They remain durable and are never reported as repaired or verified. |
| `rustfs_heal_mrf_operator_accepted_unverified_oldest_age_seconds` | Age of the oldest durable operator-accepted, still-unverified responsibility on this node. |

These gauges are emitted per node through the configured OTLP metrics exporter (`RUSTFS_OBS_ENDPOINT` or `RUSTFS_OBS_METRIC_ENDPOINT`). The background-heal status endpoint reports execution tasks; it does not include durable MRF responsibilities. Use the authenticated node-local `GET /rustfs/admin/v4/heal/mrf/responsibilities?limit=100&cursor=<nextCursor>` endpoint to list held, generation-unknown, incarnation-mismatched, or operator-accepted entries, including the stable responsibility ID, exact object/version/scope, source/current bucket incarnation, checkpoint owner/sequence, and any risk-acceptance record. `limit` is bounded to 1–256 and `nextCursor` is absent on the last page. If the listed bucket incarnation may have changed since the responsibility was parked, call the action endpoint with `action: "refresh"`, the listed `responsibilityId`, and its current `expectedBucketIncarnationId`. Refresh reads the live bucket identity, persists a changed-generation classification, and never dispatches a heal. Then review the fresh listing before taking another action. Use `action: "recheck"` to resume one exact source-bound held generation after restoring a trusted source or preparing an exact-version deletion. A recheck is persisted before the task is dispatched.

The listing's `contractVersion: 2` also includes `legacy_generation_unobserved` entries. Their `sourceBucketIncarnationId` and `bucketIncarnationId` are null: neither the original generation nor a current observation is available. These entries remain held across replay without automatic bucket-identity retry loops. Startup replay retains its existing one-time anchor lookup; a missing identity cannot resume the steady-state retry loop. The hold is derived from the existing source-unbound active checkpoint encoding; no new disk state tag is written. Use `action: "refresh"` with `expectedBucketIncarnationId: null` to request an observation. If no current identity can be read, refresh fails and retains the responsibility. If one is available, refresh records it as `legacy_generation_unknown`; it never manufactures a source identity. A stale null expectation is rejected once an observation has been recorded. Recheck and risk acceptance still require a known, non-nil current incarnation. Older server binaries can resume the old identity retry behavior after rollback; they must not be used to discharge this unresolved work.

An operator may instead choose `action: "acceptUnverifiedRisk"` only with `acknowledgeUnverifiedDataRisk: true`, explicit booleans for both source-incarnation acknowledgments, a non-empty reason, an audit reference, and a non-nil request ID. When `sourceBucketIncarnationId` is absent, set `acknowledgeUnknownSourceIncarnation: true`; when it differs from the current `bucketIncarnationId`, set `acknowledgeBucketIncarnationMismatch: true`. A generation with a known source/current mismatch is parked before dispatch, so its old intent cannot be applied to a recreated bucket. Legacy journal records without a source incarnation are listed as `legacy_generation_unobserved` until refresh records a current identity, then as `legacy_generation_unknown` and are not automatically dispatched; refresh can record the live generation but does not bind the old responsibility to it. This does not create a payload proof, delete the object, or remove the durable intent. It persists an `operator_accepted_unverified` disposition that stops automatic retries while leaving the responsibility visible. The actor, reason, reference, time, request ID, target identity, source/current bucket incarnations, and acknowledgment flags are kept in a checksummed lifecycle record in the same disk replica and alternating slot as its committed MRF checkpoint. If that lifecycle record is missing, corrupt, or belongs to another checkpoint, unbound records return to `legacy_generation_unobserved`; they are not treated as proof or dispatched against an unbound bucket. Pre-lifecycle binaries ignore this state and can heal a compatibility-journal object into a recreated bucket generation; downgrading while any durable MRF responsibility remains is unsupported. See `backlog-2682` in the compatibility cleanup register.

Example risk-acceptance body (substitute values from the node-local listing):

```json
{
  "action": "acceptUnverifiedRisk",
  "responsibilityId": "<responsibility-id>",
  "expectedBucketIncarnationId": "<bucket-incarnation-id>",
  "acknowledgeUnverifiedDataRisk": true,
  "acknowledgeUnknownSourceIncarnation": false,
  "acknowledgeBucketIncarnationMismatch": false,
  "reason": "The source was reviewed and the remaining identity risk is accepted",
  "reference": "INC-1234",
  "requestId": "<new-request-uuid>"
}
```

A process restart restores lifecycle state only when the checksummed sidecar matches the committed checkpoint. Otherwise the durable intent remains visible and generation-unknown; no record is deleted or certified, and other durable intents on that node replay normally. For source-bound unversioned objects, restore expected content from a trusted canonical source with protected shard integrity enabled, then request a targeted recheck so the exact intent can obtain a receipt. For a legacy record without a source generation, the operator must first establish which bucket generation the responsibility belongs to; if that cannot be established, keep it generation-unknown or explicitly accept the risk. For a versioned object, a protected rewrite creates a new version and does not resolve a responsibility for the old version; preserve the source and only retire that exact version when the intended data is backed up and an authoritative absence proof is appropriate. A successful receipt may discharge only the matching responsibility; an unverified result remains retained and becomes held again. If no trusted copy or expected checksum is available, keep the responsibility held or explicitly accept the risk with the node-local admin operation. The protected-copy migration in the [shard-integrity audit workflow](shard-integrity-audit.md) writes a new key and preserves its source; completing that migration alone does not prove or discharge an intent for the original key. Never remove MRF journal files manually.

### Admission lock timing

The OTLP exporter records these node-local histograms for completed manager admissions that return a receipt, including merged and rejected work. Cancellation and error exits without a receipt do not emit these observations.

| Metric | Meaning |
|---|---|
| `rustfs_heal_admission_lock_phase_seconds` | Existing total from immediately before waiting for `active_heals` until immediately after releasing it, with the existing source/result/context labels. |
| `rustfs_heal_admission_active_lock_wait_seconds` | Sum of waits to acquire the outermost admission `active_heals` guard, including forceStart reacquisition. |
| `rustfs_heal_admission_active_lock_hold_seconds` | Sum of the outer admission guard's actual held intervals, including waits for the nested queue and retrying mutexes. |

The two new histograms aggregate all observed admission sources and results without per-request labels. An ordinary uninterrupted admission partitions the phase into wait and hold. Admin forceStart releases these guards while cancelling overlaps, then reacquires them: the cancellation gap is included only in the original total phase, and the second acquisition contributes to wait. Cancellation subroutine work is outside these outer admission guard intervals. Recording happens after all three guards are released. They measure manager admission ownership, not namespace/object-lock waiting during repair. The status field `maxLockPhaseMicros` remains a manager-lifetime maximum, not a p95 sample. Keep same-window histogram counts and bucket resolution when deriving percentiles; missing observations do not mean zero latency. These metrics do not establish performance acceptance by themselves.

## Cache copy allocation profiling

With `hotpath-alloc`, an installed Hotpath counting allocator, an active profiler guard, and the Hotpath Prometheus exporter, `hotpath_function_alloc_bytes_total{function="cache_model.owned_copy"}` counts requested allocation bytes while copying existing cache-model values. Use `HOTPATH_ALLOC_CUMULATIVE=0` for exclusive allocation accounting. The shared data-usage model and Scanner use the same fixed label; it includes their projections outside background scanning. Fresh listing/decode ownership, collection growth around individual copies, serialization, and new aggregate outputs are outside this scope. Moving or sharing an owner does not count as a payload copy.

Scanner startup records an empty scope to establish a genuine zero observation. Export is asynchronous: a missing series is not zero, and separate scrapes do not establish a shared source window across metrics or nodes. This counter measures cumulative allocation requests, not retained bytes, peak memory, RSS, or copied logical string lengths. Compare builds with identical profiling features and record profiling overhead separately from default-build service performance.

## Experimental shared background admission

`RUSTFS_HEAL_ADAPTIVE_ADMISSION=true` enables Linux cgroup-v2 feedback shared by the heal manager’s four object-heal entry points and local Scanner namespace reads and object accounting. Scanner directory and local metadata reads acquire the physical leaves backing their local disk; object Heal admits the selected erasure sets across its requested pools. It is off by default while full SSD performance acceptance is pending. Unsupported platforms reject an enabled setting. Physical admission does not yet cover cache publication, lifecycle actions, the abandoned-child streaming list coordinator or independent bucket/format repair. Existing worker, disk and byte budgets still apply.

The node samples its effective CPU quota/cpuset, ancestor memory headroom, CPU usage/throttling and CPU/memory/I/O pressure once per second. Sampling uses one blocking worker at a time. Resource reads retain their real start/end times: reads spanning over 20 ms or older than one second after all inputs return cannot renew admission. Counter rates use both read brackets conservatively. The concurrency cap is at most the rounded-up effective CPU count and 256 scheduling units; existing operator limits still apply independently. At most 1% memory headroom blocks new work. This scheduling safeguard alone does not reserve memory for repair buffers.

The same sampler also controls the reusable staging storage of admitted buffered file readers and writers, whole-file results, and positioned-read results. Its process-wide cap is the smaller of 10% of the effective memory limit and the remaining headroom above 20% of that limit. Missing or expired samples block new allocations. A buffer reserves its payload size before allocation; growth counts both the old and replacement allocations until the old storage is freed. Shrinking the cap does not revoke an existing allocation, and reuse needs no new reservation. A denied allocation returns `WouldBlock`; reusable file streams accept no write bytes and preserve their read position. Cancellation and runtime shutdown keep storage charged until the actual native job releases it. Returned `Bytes` retain the allocation's charge through the last clone or slice. Standard-backend direct reads charge both the aligned bounce allocation and logical output while they coexist. Native io_uring reads retain their full allocation, including alignment padding, through the returned result; chunked reads also charge the assembled output while a native chunk is pending. Memory-budget denial does not switch either read path to a different backend.

Direct writers reserve their full aligned staging allocation when opened and retain it through native flushes, cancellation and runtime shutdown until the buffer is freed. An existing writer can reuse its allocation after the cap shrinks; a denied open does not disable direct I/O or fall back to buffered writes.

Admitted bitrot verification and reader scratch, Heal read and reconstruction shard buffers, legacy codec workspaces, and modern parity-verification scratch also reserve their payload storage before allocation. Cached legacy workspaces retain their charge while cached and can only reuse it for the same memory owner. Heal shard output retains its charge through the last `Bytes` clone or slice. HTTP read-file, walk-directory and namespace-scan response streams charge staging storage separately from the source reader. Short frames share the unused tail of that allocation; exhausted storage is reclaimed without allocating only after its last output reference drops. A retained output chunk keeps the full backing allocation charged and can prevent a replacement allocation after a cap reduction. EOF, read failure and dropping the stream release its own view, while output clones or slices keep their shared reservation.

Inline Heal readers share existing metadata bytes. Inline writers charge replacement storage while the old allocation remains live, and their returned `Bytes` retain the charge. Admitted remote Heal HTTP writers reserve each full 1 MiB staging allocation before copying input. Short flushes share that allocation with its unused tail; fully consumed storage is reused only after its last output reference drops. Queued chunks and the pending tail retain one shared reservation through the HTTP body and its final clone or slice, independently of the sender's disk capacity. A denied allocation accepts no new input bytes; existing storage remains usable after a cap reduction.

Admitted walk-directory and namespace-scan pipes reserve a fixed 1 MiB and 64 KiB ring, respectively, before their first nonempty write. Read and write ends retain the same allocation until both release it; consuming bytes does not refund storage still owned by either end. The ring never grows when writes wrap around. This also means a one-byte response reserves its full configured pipe capacity. Pipe admission failure terminates the response body, including legacy directory streams.

An authenticated namespace-scan POST also uses the receiver's enabled local admission policy when its sender omitted the background marker. Local admission preserves the authenticated wire class for nested RPC signatures. An admission-disabled receiver keeps its existing unmarked behavior; explicitly marked requests still require an enabled receiver.

Namespace frame payload and envelope encoding use the memory owner captured by the pipe before preflight scheduling capacity is dropped. Each replacement reserves old and new storage together. Both encoded allocations remain charged while a frame write is pending, and cancellation, timeout or write failure releases them. Encoding denial happens before writing the frame; it cannot fall back to unreserved encoding or become successful empty EOF. The existing MessagePack envelope, MAC and 2 MiB wire limit are preserved. Without a captured memory owner, encoding follows the existing path.

This cap does not yet cover other encode/decode paths, other RPC serializers and transport copies, allocator overhead or total process RSS. Already resident format-cache bytes are shared without a new payload allocation. The `rustfs_background_buffer_memory_limit_bytes` and `rustfs_background_buffer_memory_retained_bytes` gauges expose the sampled cap and retained payload. Like the node gauges, they can remain stale when the sampler stalls; the in-process admission deadline still expires after three seconds.

Each complete sample leases admission for three seconds. Missing resource caps block new work; missing or low-confidence feedback retains at most one maintenance unit. CPU use above 90%, throttling or pressure above 1% of elapsed time, memory headroom at most 10%, foreground errors/cancellations, a foreground queue or a comparable foreground p99 regression reduce concurrency immediately. Recovery requires CPU use at most 70%, throttling/stalls at most 0.1% of elapsed time, at least 20% memory headroom, fresh foreground evidence and actual successful work. The existing controller probes one additional unit only after eight consecutive healthy seconds and cooldown. Limits affect new admission and do not revoke active work.

The Scanner captures the same node owner once per bucket walk. Directory open/refill, entry type and size lookup, and object accounting try admission before their first storage poll; each scheduling unit ends when its operation future finishes. Physical leaves take FIFO turns in a stable order before node admission, and partial grants are returned before any resource wait. On contention, local and remote coordinators suspend the same traversal, release the bucket cache lease, and keep their physical/node FIFO position while topology remains current. The existing cycle budget and cancellation bound this wait. Topology validity is rechecked every 250 ms; expired or replaced plans yield a typed deferral. The coordinators reacquire the cache lease and verify both cache revisions before resuming with a permit or a typed deferral that preserves partial progress. A changed cache rejects the suspended result. Cancellation also drops the queued request.

The same sampling worker resolves local attachments through Linux sysfs, partitions and layered-device leaves. Device numbers plus `diskseq` distinguish replacements. Aliases share capacity; an attachment has its own three-second topology lease, so a healthy alias cannot renew a stale attachment. Missing identity or disk statistics prevents new physical admission. Rotational or unknown media stay at one unit; non-rotational media are bounded by a positive `nr_requests` and 256. Busy time above 90% or weighted queue time above one window cuts capacity; recovery also requires busy/queue time at most 50%, no outstanding I/O, valid foreground evidence and successful Scanner progress. Counter resets and gaps cannot justify growth. The node and physical controllers take turns probing, allowing at most one upward probe per sampling window.

Native directory work and the local metadata read retain only that disk's physical leaves after async cancellation. They do not retain the node scheduling unit. Later lifecycle/configuration work in object accounting does not inherit those physical units. Physical owners and carrier slots are preallocated during attachment refresh; object work does not discover devices or allocate a new resource group.

Resource-controlled peer requests carry `x-rustfs-rpc-background-admission: v1`, bound to their HTTP or gRPC signature. Stream retries preserve that classification without retaining the sender's disk quota. The receiver uses its existing shared node owner and admits the selected local physical device before storage locks or I/O. Repeated phases reuse the request's reservation, including when capacity is one. Missing capacity, an unknown UUID mapping, or an unavailable physical plan defers the request instead of probing other disks or reporting object absence. Cancellation returns node capacity; native work retains only its selected physical resources until it finishes.

An HTTP file response releases node admission after opening its reader, but records progress only after producing the declared byte count or reaching an unbounded EOF. Body errors, early EOF and cancellation do not credit successful preparation. The deferred progress record retains counter identities without keeping node slots or physical units.

Enable this experimental setting only after all peers support the admission marker. Older peers reject marked signatures, and a new peer with admission disabled rejects marked work; neither path silently runs it outside resource control. Requests without the marker retain their existing signature and foreground behavior. The receiver reports deferred admission as HTTP 503 or gRPC `RESOURCE_EXHAUSTED`.

Calls without a coordinating cache lease retain immediate resource deferral and useful partial-cache handling. Existing remote single-session ownership and local-disk admission shares remain in force. Disabled admission creates no request channel. Enabled walks use one bounded channel, with a response allocation and cache revision reads only on contention; the uncontended per-entry path adds no global lookup or task. The streaming abandoned-child coordinator requires a separate ownership boundary: retaining a node unit while it waits for a bounded-channel consumer that needs the same unit would deadlock at capacity one.

Foreground comparison uses the existing complete response-body windows and shared admission snapshots. Each active class needs at least 100 completions and a finite p99 interval; this permits the percentile rank but is not a statistical confidence guarantee. Comparisons require the same active classes and rates within 80–125% of a retained reference. A changed workload resets recovery. The reference is an observed workload window, not an independent background-off baseline. A p99 interval can justify stability only when its upper bound is within 5% of the reference lower bound; a lower bound above 105% of the reference upper bound is pressure. Overlapping/uncertain comparisons cannot justify growth. Missing queues, stale or repeated cuts, uncertain cut boundaries and low counts are not idle evidence.

Heal admission waits at most 250 ms before returning the existing typed transient deferral, before polling storage or taking namespace locks. No-lock callers never wait. Queued read repair uses the same node gate; direct foreground ECStore calls remain outside this heal-manager adapter. Cancellation releases the non-cloneable node scheduling unit. Native work retains separate physical-I/O units, and unsuccessful/cancelled/dry-run work cannot increment the node's completed-work counter. This scheduling counter is not an authoritative heal receipt and cannot discharge repair responsibility.

The `rustfs_background_node_admission_limit` and `rustfs_background_node_admission_active` gauges describe the last published sample, not a live reservation proof. A blocked sampler can leave these exports stale even though the admission lease has expired. Full-service validation of multi-disk Heal and remote RPC ownership, coverage of the remaining escaping buffers, and the matched-release SSD matrix/hot-shrink/72-hour soak remain required before enabling this by default or claiming optimal performance.

## Heal runtime controls

Heal knobs are environment-only and read by `HealConfig::default` (`crates/heal/src/heal/manager.rs`), the MRF queue (`crates/heal/src/heal/mrf_queue.rs`), or the erasure-set healer (`crates/heal/src/heal/erasure_healer.rs`). The admin `heal` config subsystem accepts only `bitrot_cycle` (`HEAL_KEYS`), which is documented in the scanner table above. Constants live in `crates/config/src/constants/heal.rs` unless another file is named.

| Environment variable | Default (constant) | Effect |
|---|---|---|
| `RUSTFS_HEAL_ENABLED` (deprecated alias `RUSTFS_ENABLE_HEAL`) | `true` (`heal_enabled_from_env`, `rustfs/src/module_switches.rs`) | Starts the background heal manager when enabled. The manager also starts whenever the scanner is enabled; disable both switches to keep the manager and MRF consumer from starting. |
| `RUSTFS_HEAL_AUTO_HEAL_ENABLE` | `true` (`DEFAULT_HEAL_AUTO_HEAL_ENABLE`) | Enables automatic healing of detected issues; `false` leaves healing to manual admin requests. |
| `RUSTFS_HEAL_QUEUE_SIZE` | `10000` (`DEFAULT_HEAL_QUEUE_SIZE`) | Heal request queue capacity. The manager reserves at least one slot per Scanner/ReadRepair/AutoHeal source, or 10% for larger queues, so sustained normal-priority MRF or internal background work cannot consume every slot. |
| `RUSTFS_HEAL_INTERVAL_SECS` | `10` (`DEFAULT_HEAL_INTERVAL_SECS`) | Heal manager polling interval. |
| `RUSTFS_HEAL_TASK_TIMEOUT_SECS` | `300` (`DEFAULT_HEAL_TASK_TIMEOUT_SECS`) | Per-task timeout. |
| `RUSTFS_HEAL_MAX_CONCURRENT_HEALS` | `4` (`DEFAULT_HEAL_MAX_CONCURRENT_HEALS`) | Global concurrent heal task limit. |
| `RUSTFS_HEAL_MAX_CONCURRENT_PER_SET` | `1` (`DEFAULT_HEAL_MAX_CONCURRENT_PER_SET`) | Per-erasure-set limit; effective value is `min(global, per_set)`, each floored at `1`. |
| `RUSTFS_HEAL_LOW_PRIORITY_MERGE_ENABLE` | `true` (`DEFAULT_HEAL_LOW_PRIORITY_MERGE_ENABLE`) | Merge duplicate low-priority requests with the same dedup key. |
| `RUSTFS_HEAL_LOW_PRIORITY_DROP_WHEN_FULL` | `true` (`DEFAULT_HEAL_LOW_PRIORITY_DROP_WHEN_FULL`) | Drop, rather than block on, low-priority requests only after the reserved best-effort capacity is occupied and the queue is actually full. |
| `RUSTFS_HEAL_EVENT_DRIVEN_SCHEDULER_ENABLE` | `true` (`DEFAULT_HEAL_EVENT_DRIVEN_SCHEDULER_ENABLE`) | Notify-driven scheduler wakeups. |
| `RUSTFS_HEAL_SET_BULKHEAD_ENABLE` | `true` (`DEFAULT_HEAL_SET_BULKHEAD_ENABLE`) | Per-set bulkhead scheduling. |
| `RUSTFS_HEAL_PAGE_PARALLEL_ENABLE` | `true` (`DEFAULT_HEAL_PAGE_PARALLEL_ENABLE`) | Page-level parallel object healing during erasure-set repair. |
| `RUSTFS_HEAL_PAGE_OBJECT_CONCURRENCY` | `8` for ordinary heals; `2` for automatic replacement | Concurrent object heals within one erasure-set page. An explicit value applies to both; automatic replacement clamps it to the semaphore's supported range. Forced to `1` when page parallelism is off, for `Deep` scans, and for automatic heals without a replacement generation or object locks. |
| `RUSTFS_HEAL_MAINLINE_THROTTLE_ENABLE` | `true` (`DEFAULT_HEAL_MAINLINE_THROTTLE_ENABLE`) | Defer best-effort starts and cooperatively pace running admin and automatic replacement heals at safe work boundaries. |
| `RUSTFS_HEAL_MAINLINE_READ_UTILIZATION_HIGH_PERCENT` | `80` (`DEFAULT_HEAL_MAINLINE_READ_UTILIZATION_HIGH_PERCENT`, capped at 100) | Read-utilization high watermark for start admission and running heal pacing; zero disables this class. |
| `RUSTFS_HEAL_MAINLINE_WRITE_UTILIZATION_HIGH_PERCENT` | `80` (`DEFAULT_HEAL_MAINLINE_WRITE_UTILIZATION_HIGH_PERCENT`, capped at 100) | Write-utilization high watermark for start admission and running heal pacing; zero disables this class. |
| `RUSTFS_HEAL_MAINLINE_MAX_SLEEP_MS` | `250` (`DEFAULT_HEAL_MAINLINE_MAX_SLEEP_MS`) | Start recheck interval; running waits cap each pacing-gate holder at 1000 ms. Zero disables running pacing. |
| `RUSTFS_HEAL_OVERLAP_POLICY` | `merge` (`DEFAULT_HEAL_OVERLAP_POLICY`) | `merge` reuses the token of an equivalent active, queued, or retrying admin heal; incompatible same-target starts and intersecting admin scopes return `already_running` / `overlapping_paths`. Durable-only owners reject until recovered or replaced with `forceStart`. `minio_error` also rejects equivalent starts and preserves overlap rejection against background tasks. |
| `RUSTFS_HEAL_MRF_ENABLE` | `true` (`DEFAULT_HEAL_MRF_ENABLE`) | MRF intent pipeline: error paths deliver repair intents to the heal runtime and unconsumed intents replay from the durable journal after restart. |
| `RUSTFS_HEAL_MRF_QUEUE_SIZE` | `100000` (`DEFAULT_HEAL_MRF_QUEUE_SIZE`) | MRF in-memory queue capacity. |
| `RUSTFS_HEAL_MRF_JOURNAL_MAX_BYTES` | `8388608` (`DEFAULT_HEAL_MRF_JOURNAL_MAX_BYTES`, 8 MiB) | MRF journal size at which compaction runs. |
| `RUSTFS_HEAL_MRF_REPLAY_BATCH` | `256` (`DEFAULT_HEAL_MRF_REPLAY_BATCH`) | Intents per replay push round. |
| `RUSTFS_HEAL_DANGLING_DELETE_GRACE_SECS` | `3600` (`DEFAULT_HEAL_DANGLING_DELETE_GRACE_SECS`, `crates/ecstore/src/set_disk/core/io_primitives.rs`) | A recently modified object is never deleted as dangling inside this window; `0` disables the grace window. |

The heal scheduler preserves priority ordering for urgent, high, and ordinary
work, but applies a bounded fairness quantum to a continuously refilled MRF
normal backlog: after four MRF/normal dispatches, a runnable Scanner,
ReadRepair, or AutoHeal request gets a service opportunity. This protects
integrity progress without weakening durable MRF admission or receipt
requirements. Queue capacities from 4 through 9 reserve three slots, one per
best-effort source, while still leaving one slot for MRF; capacities below 4
cannot isolate all work classes and
should only be used for diagnostic tests. Normal-priority non-best-effort submissions are deferred at the
reserve boundary; high and urgent requests, including admin or MRF recovery,
retain their priority and may use that space.

### Admin heal start, retries, and budgets

Admin heal has three separate budgets. Increasing one does not extend the others:

| Budget | Existing behavior |
|---|---|
| Control request | A start/query/cancel envelope has a bounded lifetime derived from the internode RPC timeout, with room for the transport response. It bounds control execution, not the admitted repair task. The HTTP caller can also stop waiting independently. |
| Task execution | `RUSTFS_HEAL_TASK_TIMEOUT_SECS` supplies the default of 300 seconds when the execution has no explicit timeout. Elapsed execution time is deducted before a recoverable scheduler retry, which keeps the request identity and remaining budget. Object/listing backoff and pressure pacing inside an execution consume that budget; time queued or in the scheduler's between-attempt backoff is not a new absolute wall-clock deadline. Zero is not an unlimited execution budget. |
| Object/listing retry | A recursive bucket/prefix traversal retries recoverable failures at most three times after the initial attempt, with 2/4/8-second delays plus the existing task-derived jitter. Object retries also obey the bounded delayed window and its 30-second age, checked at safe boundaries; listing retries keep their cursor. These limits do not extend the task execution budget or force an in-flight storage operation to finish within the retry age. |

Automatic replacement retries a transient object repair twice within its current page, waiting 250 ms and then 500 ms without holding page capacity. An exhausted retry remains unverified and still triggers the existing generation retry; target readback and checkpoint requirements are unchanged.

For a large admin heal, select a finite configured execution budget appropriate to the expected work and contention, and inspect progress while it runs. Investigate stalled work and object failures before increasing this budget; a larger timeout must not hide lock or quorum problems. A longer HTTP or internode timeout does not keep a repair task alive past its execution budget. A successful start response returns the canonical token for accepted or merged work; it does not prove that repair has completed. Query that token without `forceStart`; terminal timeout/cancellation reports retain completed progress while the task report remains available.

Capability preflight runs before constructing and admitting a start request. However, the public `cluster heal coordination unavailable` error can also follow a transport failure or an invalid coordinator response after a request was sent. An unknown or lost HTTP response is therefore not proof that no task was admitted.

The coordinator can replay the result of the exact original RPC envelope within its existing replay lifetime and coordinator epoch. Reusing an ID with changed parameters or nonce is rejected. This bounded, process-local receipt cache is not a general HTTP idempotency key or a restart-surviving admission receipt. Each new HTTP start constructs a new request/envelope, and a fresh `forceStart` intentionally requests a distinct start: do not automatically resend it after an ambiguous response. A fresh non-forced request follows the configured overlap policy, rather than recovering the original receipt. The v3 API rejects `forceStart` combined with `clientToken` or `forceStop`.

Implementation references: `rustfs/src/admin/handlers/heal.rs` (`submit_cluster_heal_start`, `new_heal_control_metadata`), `rustfs/src/storage/rpc/node_service.rs` (`execute_heal_control_envelope_with_manager`), `crates/protos/src/lib.rs` (`heal_control_execution_timeout`), and `crates/heal/src/heal/task.rs` (`retry_request_with_remaining_timeout`, `remaining_timeout`, `bucket_object_retry_delay`). These contracts do not replace the separate real response-loss, long-task, or start-latency validation lanes.

### Running admin heal pacing

The manager passes its existing workload provider and a configuration snapshot into each admin execution and automatic replacement generation. Bucket/prefix listing and object boundaries resample foreground pressure; erasure-set page workers also resample after earlier work releases page capacity. `High`, `Urgent`, and `force_start` do not exempt ordinary admin execution from this runtime pacing. The existing start-time bypass and overlap-control meanings are unchanged.

Each execution has its own pacing latch, with no new global manager or cross-set pacing lock. The low watermark for each enabled class is `max(1, floor(high * 3 / 4))`: the default high watermark 80 therefore recovers below 60. High pressure latches pacing, and intermediate pressure resets the recovery window. Unpaced starts resume after sampled pressure remains below the low watermarks for four pause intervals, normally one second. While pressure persists, a pacing-gate holder waits only one interval, at most one second, then permits maintenance to continue. Concurrent page waiters serialize through this task-local gate; queue waiting still counts against the existing task execution timeout.

The pacing gate holds neither namespace locks nor I/O/page permits while sleeping. At final page admission, each real permit acquisition gets a fresh, nonblocking pressure decision. Low-pressure work keeps that permit; only a unit that needs a pause releases capacity to wait. A unit that has completed one bounded pause may proceed despite persistent pressure, which supplies minimum maintenance progress without an endless acquire/pause loop. Existing object operations and commit tails are not interrupted because pressure rose. Cancellation interrupts the pacing wait; replacement operations retain their execution lease until storage work settles. Disabling pacing cannot bypass the global, per-set or page-concurrency hard caps. The existing `RUSTFS_HEAL_MAINLINE_THROTTLE_ENABLE=false` setting is the operational opt-out for newly created executions; no additional request override is introduced.

A missing provider, zero pause, or both class thresholds set to zero preserves unpaced execution. Missing counts follow the existing shared pressure interpreter; they are observations, not health, quorum or resource-ownership proof. The current provider exposes node-level workload classes, so this does not claim independent per-set foreground measurements or a hard global resource budget. Runtime waits increment `rustfs_heal_mainline_throttle_total` with `source=admin` or `source=auto_heal`, `result=delayed`, and a foreground-pressure or `recovery_window` reason. Real p99/throughput protection requires the separate W20 fixed-load ABBA measurements.

## Live Scanner Heal Completions

Scanner admissions now retain the canonical heal task ID, including merged
requests. Node-local metrics associate these live candidates with the executor's
typed terminal result:

| Metric | Meaning |
|---|---|
| `rustfs_scanner_heal_pending_tasks` | Canonical tasks awaiting a terminal observation. |
| `rustfs_scanner_heal_pending_oldest_seconds` | Age of the oldest retained pending candidate; refreshed every five seconds. |
| `rustfs_scanner_heal_candidate_terminal_total` | Associated candidates, labeled by object `disposition`, expected `scan_mode`, and unresolved `scope`. |
| `rustfs_scanner_heal_candidate_selected_data_dir_mismatch_observations_total` | Associated candidates whose discovered data-directory ID differs from the ID selected from quorum metadata by storage. Both IDs must be non-nil and refer to the same kind, bucket, and object name. This is an observation of two samples, not an identity or content verdict, and can overlap `scope` values such as `insufficient_scan`. |
| `rustfs_scanner_heal_task_terminal_observations_total` | Associated task observations, deduplicated within the bounded recent history. This is not an execution count. |
| `rustfs_scanner_heal_task_deep_check_observations_total` | The first associated observation of a completed, non-dry-run canonical task for which storage reported completing its Deep check path. A `Normal` request can enter that path internally. Restart or recent-history eviction can allow recounts. This does not certify all bytes or candidate coverage. |
| `rustfs_scanner_heal_candidate_unknown_total` | Candidates whose integrity remains unknown, labeled by reason. |
| `rustfs_scanner_heal_completion_lost_total` | Terminal notifications skipped by a lagging observer. |

These metrics use the configured metrics exporter. A task's `Completed` state
can include a deferred object; the `disposition` label preserves that distinction.
`MetadataHealthy`, `Repaired`, and authoritative absence never imply verified
payload bytes. Even `VerifiedHealthy` cannot certify scanner coverage: the current
contract does not bind the candidate's content identity and leader epoch. No
verified-byte or full-bucket coverage counter is advanced here.

The data-directory and Deep check observations do not prove continuous lock
ownership or the object's identity at task completion. They can also be
reported when namespace locking is disabled or explicitly skipped. A mismatch
does not change the candidate's identity scope, and a Deep check observation
does not make a `Normal` terminal satisfy a Deep candidate.

The scanner status response also exposes `heal_observation` without requiring
a metrics exporter. Reading status does not start an observer. `state` is
`not_started`, `active`, or `stopped`; unavailable pending counts are null.
`retained_pending_tasks` counts canonical IDs in the existing bounded map,
while `retained_pending_candidates` counts their associated discoveries.
`queued_admissions` samples the local admission channel and is not an atomic
cluster backlog. `terminal_task_observations_total` keeps the same bounded
deduplication semantics as the exporter counter.

`terminal_candidates` preserves execution, object disposition, scan mode, and
unresolved scope. `unknown_candidates` includes admission rejection, overflow,
loss, expiry, and missing terminal proof. These categories overlap: a completed
task with a repaired or verified-healthy object can still have unknown scanner
integrity. `integrity_state` therefore remains `unknown`, including when retained
pending is zero. `lost_completion_notifications` counts skipped messages, not
objects. Counts belong to the named local observer `generation`; runtime
reinstallation starts a new generation and process restart clears it. Usage
reset and scanner cycle changes do not clear pending heal observations, because
canonical tasks can outlive a scanner cycle.

The admission buffer, pending map, terminal channel, and recent history each hold
at most 1,024 entries. Retained identities are capped at 4 KiB; terminal history
expires after ten minutes and pending observations after one hour. Overflow,
notification loss, missing identity, and expiry remain unknown. Queue cancellation
or displacement paths without a terminal snapshot also age out as unknown.
Process restart clears this diagnostic state; a restarted runtime reinstalls its
observer. Counts and pending age are not a durable integrity ledger or a cold-range
completion deadline. Existing periodic traversal and pending retry hints provide
compensation independently.

## Pending Heal Hints

The scanner's persisted pending-heal cache is a best-effort retry backstop, bounded to 10,000 hints per bucket and a 24-hour age limit. These limits do not authorize garbage collection of committed durable repair obligations. A durable owner must retain its independent replay record until verified object completion or an equivalent durable successor permits removal.

Accepted, merged, or policy-dropped admission does not clear an existing hint. Task completion and legacy repair notices also lack the incarnation, set scope, generation, and storage verification needed to prove repair responsibility was discharged. The legacy success path therefore retains hints conservatively; this is not a complete durable MRF handoff protocol.

Pending retries use their persisted attempt count and last-attempt timestamp, starting at 15 minutes and doubling up to six hours. Each bucket submits at most 128 due hints per cycle. Already admitted or policy-dropped hints retry at Low priority; queue-full hints keep High priority but obey the same due-time bound. A changed retry batch synchronizes its pending cache once, including when cancelled, rather than copying the entire table after every admission.

Rediscovery and admission observations update the recorded result but do not postpone an already armed retry. The actual retry loop advances the attempt count and timestamp before awaiting admission, so cancellation or repeated queue-full results cannot reset the retry budget.

| Producer | Current identity and compensation | Durable handoff boundary |
|---|---|---|
| Scanner corrupt metadata | Metadata kind with no invented version/set; an existing pending-cache hint remains available for bounded retries. | Cache publication is separate from MRF ingress. Its age/count limits mean it is not an irrevocable repair-obligation ledger. |
| Read decode failure | Decode kind, available version and erasure-set scope; a later failing read can rediscover the repair. | Nonblocking ingress and in-memory read-repair admission do not acknowledge durable acceptance. |
| Partial write | Partial-write kind, version and erasure-set scope; each committed write receives a fresh responsibility lease. PUT rename convergence, partial delete-marker creation, and explicit partial-write producers use the durable MRF channel. | A successful durable-admission receipt follows committed checkpoint publication. The record remains after manager admission and retries until an exact verified storage receipt discharges it. Restart re-arms retained records even when the target is still offline. |

With MRF enabled, a partial commit waits up to ten seconds for its checkpoint receipt. Fully converged writes do not enter this path or add MRF journal writes. The consumer batches available submissions and uses the existing count/byte budgets; an offline member or exhausted hint retry count does not evict an admitted partial-write obligation. New responsibility for the same unversioned object invalidates the previous lease, and a task that already started cannot accept that new responsibility as a merged proof target.

Durable partial-write replay uses the manager's full proof anchor (including bucket incarnation and ingress lease) to observe queued, running, and retrying ownership. A due record already owned by the manager is not submitted again. Ownership is observed at the existing admission-backoff interval, which defaults to five seconds. Task-ID changes caused by retry merging do not change the responsibility identity. A notice is treated as in-flight only while its task has a live queued, running, or retrying owner. If those state locks are busy, the consumer falls back to ordinary manager admission rather than assuming that a task completed or indefinitely suppressing a retry.

After an admitted attempt ends without verified proof, the responsibility remains in the journal and its next attempt uses bounded exponential backoff with per-responsibility jitter. With the default five-second base, successive delay ranges are 5 seconds, 5–10, 10–20, 20–40, then 30–60 seconds. A larger explicitly supplied admission interval is not shortened. Full or already-running admission uses the existing admission interval and does not increment the unverified-completion streak. Bucket identity lookup or submission errors use the same bounded failure pacing.

Verified-proof and unverified-legacy events are consumed on the existing checkpoint tick even while a responsibility is backing off. A matching proof can therefore discharge responsibility without waiting for the next retry deadline. This is a retry delay bound, not a repair-completion deadline. New leases, replay after restart, and explicit operator rechecks start fresh runtime pacing; same-lease duplicate admission retains it. Rechecks still require their successor checkpoint before dispatch. Pacing state is not added to the journal format, does not consume the best-effort hint attempt limit, and never authorizes dropping a durable obligation.

Disabled/unavailable delivery, full queues, invalid identities, persistence failures and receipt timeouts are not durable admission. An already committed object is not rolled back: when its source bucket incarnation is known, the producer falls back to the in-memory Heal channel with that same generation fence. If the source incarnation is unavailable, it does not dispatch an unfenced heal that could mutate a recreated bucket. When MRF is enabled, a failed admission attempt also records `mrf_durable_admission_failed`. An admission timeout does not cancel a record already retained by the consumer. These failure cases therefore do not promise that every successful S3 response has a durable repair obligation. Early-ACK PUTs can also return before their rename tail settles; the tail submits responsibility when its final outcome requires repair. The MRF switch is independent of automatic disk scanning, so returning members can be repaired with scanner and auto-heal disabled.

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
| `MINIO_SCANNER_IDLE_SPEED` / `scanner idle_speed` (`on` default, `off`) | `RUSTFS_SCANNER_IDLE_MODE` / `scanner.idle_mode` (`true` default, `false`) | Not mapped; must be rewritten. Direction matches (`on` and `true` both mean throttled). Both RustFS channels also accept `on`/`off` as booleans (`parse_config_bool`, `parse_bool_str`). RustFS `false` disables sleeper delays and foreground-read backoff, but per-disk foreground-priority admission remains active. |
| `MINIO_HEAL_BITROTSCAN` / `heal bitrotscan` (default `off`) | `RUSTFS_SCANNER_BITROT_CYCLE_SECS` / `heal.bitrot_cycle` (default 30 days) | Not mapped. RustFS deep-scans periodically by default; set `off` or `disabled` to reproduce MinIO's default. |
| `MINIO_API_STALE_UPLOADS_EXPIRY` (24h) | `RUSTFS_API_STALE_UPLOADS_EXPIRY` (`DEFAULT_STALE_UPLOADS_EXPIRY`, 24h, `crates/ecstore/src/bucket/lifecycle/bucket_lifecycle_ops.rs`) | Not mapped; same default. |
| `MINIO_API_STALE_UPLOADS_CLEANUP_INTERVAL` (6h) | `RUSTFS_API_STALE_UPLOADS_CLEANUP_INTERVAL` (`DEFAULT_STALE_UPLOADS_CLEANUP_INTERVAL`, 6h) | Not mapped; same default. |
| `MINIO_API_DELETE_CLEANUP_INTERVAL` (5m) | None; `DELETED_OBJECTS_CLEANUP_INTERVAL` is a 5-minute constant in `crates/ecstore/src/disk/local.rs`. | No knob. Trash draining is not per-entry throttled. |
| `scanner alert_excess_folders` (50000) | `scanner.alert_excess_folders` (65538) | Not mapped; see [Scanner Excess Alerts](scanner-excess-alerts.md). |
| Any other `MINIO_SCANNER_*` | Corresponding `RUSTFS_SCANNER_*` from the tables above | Not mapped; rename explicitly. |

Stale-upload cleanup takes the upload namespace write lock and rechecks cleanup authority before deletion. An incomplete legacy upload without sufficient identity or deletion quorum remains on disk; age and absence from an S3 listing do not establish that its recovery or publication authority has ended. A native Abort with a complete, validated closure certificate can recover a smaller residual after partial deletion, subject to the original-participant checks. Missing, unavailable or corrupt proof keeps the operation conservative. For an otherwise valid native Abort certificate, an unavailable participant identity is a retryable write-quorum failure; a known conflicting identity remains a corruption error, including when another participant is unavailable. See [the multipart authority contract](../architecture/erasure-coding.md#7-write-path-and-write-quorum).

Size-filtered incomplete-upload rules also require every participant to answer the part listing and matching part metadata at write quorum. Unknown size skips only size-dependent rules. During a mixed-version upgrade, an older node can still evaluate those rules using the older empty-list behavior; fleet-wide protection requires every cleanup worker to run the updated version. Plan the rollout so older workers cannot perform size-filtered aborts while mixed versions are active.

The `rustfs_stale_multipart_cleanup_blocked_total` counter has a bounded `stage` label: `recheck` for lock or authority recheck failures, `lock_lost` when a held cleanup lock is lost, and `delete` for a failed authorized deletion. It counts attempts, not unique uploads or bytes, and includes transient failures; it carries no bucket, key or upload labels. The `multipart_cleanup_lock_or_recheck_failed` debug event identifies a candidate path and the failure; `multipart_cleanup_lock_lost` identifies the path when a previously acquired cleanup lock is lost. Restore unavailable disks and retry through supported S3 or background recovery paths when their prerequisites hold. Preserve the disk state and seek an evidence-based recovery procedure when authority remains uncertain; do not recursively remove a residual solely because it is old or a destination object exists. The default expiry is measured from upload initiation, including internal data-movement uploads, and can affect long-running uploads even without a bucket lifecycle rule. This differs from [Amazon S3](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html), which retains parts until the upload is completed or stopped and offers an explicit lifecycle rule to abort incomplete uploads. The interval is a schedule, not a guaranteed reclamation deadline.

## Replacement Recovery Completion

One reconnect sweep groups the newly attached blank disks in a set. If another reconnect is still pending, the ready targets are submitted after a one-second window rather than waiting indefinitely; later targets are submitted separately. Compatible automatic requests can add targets while the owner is still queued and has never executed or recovered a durable generation. Once execution or recovery fixes the target scope, later disks cannot join that generation or receive a merged receipt from it. Blank disks outside the frozen scope remain discoverable for the next automatic scan after the owner finishes.

`POST /v3/background-heal/status` is an execution-queue view. `state=idle`, zero queue and active counts, an online disk, a readable object, or acceptance of an admin deep-heal request do not independently prove that a replacement disk contains every erasure shard.

Treat replacement recovery as verified only after the repair task has completed for the exact replacement instance and an operator has confirmed the target disk contains the expected `xl.meta` and data parts for every relevant object version. A replacement that is not mounted, is unsafe to format, loses its marker, or returns a partial target outcome must be treated as deferred or incomplete. Do not automate destructive replacement actions from an `idle` observation alone. A new node must not infer replacement completion from an old or unavailable peer; regard that information as unknown or degraded until every required peer can report the same replacement instance and verified completion.

`GET /rustfs/admin/v4/heal/replacement-recovery` reports durable automatic replacement records from survivor disks. `local.records[]` entries distinguish `waiting_for_replacement`, `running`, `incomplete`, `unrecoverable`, `cleanup_pending`, `completed`, and `unknown`; `local.definitive=false` or any `unknown` record means the node could not prove a local replacement state. The `cluster` section queries the replacement-recovery peer RPC and sets `cluster.definitive=true` only when the expected peer topology is complete, every peer supports the RPC, every peer snapshot is locally definitive, and copies of each task agree. Different anchors can report different tasks, and a definitive node may report an empty list or a subset of another node's history. Disjoint target generations merge directly. Every pair of generations sharing a target slot must appear together in at least one definitive local snapshot; this preserves retained completion proofs and committed handoff histories while keeping overlapping records found only on separate peers non-definitive. Shared history is not transitive: observing A with B and B with C does not establish that A and C are compatible. A definitive history can include retired or unfinished generations, so consumers must inspect each record's state rather than treating `cluster.definitive` as proof that every generation completed. Old peers, unavailable peers, malformed peer payloads, topology gaps, and task disagreements are reported as degraded or unknown rather than complete.

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

`/rustfs/admin/v3/scanner/status` and `/rustfs/admin/v3/realtime` report the node that handles the HTTP request; the metrics endpoint does not fan out to peers. In distributed deployments, query every node explicitly and keep `by-host=true` so each response includes that node's host view:

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
    "${endpoint}/rustfs/admin/v3/realtime?types=1&by-host=true&n=1" \
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
9. To bound one cycle, set `scanner.cycle_max_duration`, `scanner.cycle_max_objects`, or `scanner.cycle_max_directories`; confirm that checkpoints advance under that budget. These controls do not guarantee resumable bounded chunks for every directory shape; see [Startup and cycle budgets](#startup-and-cycle-budgets).
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
      disk_io_budget_iops: "100"
      disk_io_budget_bytes_per_sec: "16777216"
```

Use `extraEnv` for environment variables that are not represented by chart values, including every heal knob above.

## Diagnostic same-bucket prefix attribution

Build with `diagnostic-prefix-attribution` to enable bounded diagnostic counters. The feature is disabled in ordinary builds. Set `RUSTFS_SCANNER_PREFIX_ATTRIBUTION` before starting RustFS to a JSON object with one bucket and two disjoint prefixes, for example `{"bucket":"bench","hot_prefix":"hot/","cold_prefix":"cold/"}`. Prefixes must end in `/` and have no empty, `.` or `..` path component. A missing, malformed or overlapping scope disables attribution; it does not emit zero. Restart to change the scope.

`rustfs_scanner_object_metadata_prefix_visits_total` classifies stored Scanner paths by disk into the fixed classes `hot`, `cold`, `other` and `ambiguous`. Its scope digest uses the `scanner-prefix-attribution-v2` domain; raw bucket and prefix text are not labels, but the digest is not a secrecy guarantee. A path containing a component ending in the encoded directory suffix is `ambiguous`, because a directory marker and a literal key can share that representation. Never count it as hot or cold. Other path normalization and filesystem aliases can also prevent recovery of the original S3 key, so this is not general raw-key attribution.

An attempt is recorded immediately before calling `get_size_with_tier_names` after node admission. A concurrent cancellation can still prevent its internal metadata read. Attempts include failures and repeated visits, and are not unique objects, native I/O or durable progress. A separate `rustfs_scanner_object_metadata_prefix_successes_total` increments only when that call returns success; it does not certify publication. An operation crossing a measurement boundary can produce a success in one window and an attempt in another. The class handles are registered for a valid matching bucket so an observed zero can be distinguished from a missing series.

Use a controlled canonical dataset and independently verify its inventory and per-disk scope before interpreting these counts. Require complete per-disk series for all four classes, unchanged scope, no resets and genuinely matching source windows; missing data or nonzero ambiguous counts cannot establish cold attribution. A zero ambiguous count does not prove a complete scan or rule out other path aliases. Build both comparison legs with the same feature and configuration, and separately measure its overhead. This diagnostic alone does not establish clone-byte attribution, physical cold cache, foreground latency or SSD acceptance.
