# Background Services Inventory

This document records the current background service surface before
BackgroundController work. It is a behavior-preservation inventory only; it does
not define a new scheduler, controller framework, or shutdown contract.

## Scope

- Related migration task: `BGC-001`.
- PR type: `docs-only`.
- Baseline: `upstream/main` at
  `03eb10b07f5f968c531151ae667dfe218050493d`.
- Out of scope: changing startup order, shutdown order, readiness, storage
  writes, heal admission, scanner scheduling, replication queues, config reload
  behavior, metrics intervals, or worker counts.

## Startup And Shutdown Owners

| Area | Startup owner | Shutdown owner | Current cancellation source |
|---|---|---|---|
| Main runtime token | `rustfs/src/main.rs::run` creates `ctx` after HTTP listeners start and before ECStore creation. | `rustfs/src/main.rs::handle_shutdown` calls `ctx.cancel()` before service-specific shutdown. | Shared `tokio_util::sync::CancellationToken`. |
| Scanner | `rustfs/src/main.rs::run` calls `init_data_scanner(ctx.clone(), store.clone())` after successful startup log and global init time. | Main shutdown calls `ctx.cancel()`; if scanner was enabled it also calls `shutdown_background_services()`. | Scanner loop receives the main runtime token. |
| Heal/AHM | Main creates `create_ahm_services_cancel_token()` before scanner/heal feature checks and calls `init_heal_manager(...)` when heal or scanner is enabled. | Main shutdown calls `shutdown_ahm_services()` when heal or scanner was enabled. | Global AHM token plus channel/worker-local state. |
| Replication pool | Main calls `init_background_replication(store.clone())` after global config init, then `pool.init_resync(ctx.clone(), buckets.clone())` after bucket listing. | No direct main shutdown call for the replication pool; resync receives the main runtime token. | Resync routine uses the main runtime token; per-bucket resync uses registered cancel tokens. |
| Lifecycle expiry/transition | `ECStore::init` calls `init_background_expiry(self.clone())` and `init_background_stale_multipart_upload_cleanup(self.clone())`. | Expiry workers read `get_background_services_cancel_token()` and fall back to a private token if none exists. Stale multipart cleanup exits when the weak ECStore reference cannot upgrade. | Inventory search found no current startup caller for `create_background_services_cancel_token()`. |
| Notification runtime | Main calls `init_event_notifier()` after buffer profile init. | Main shutdown calls `shutdown_event_notifier().await`. | Notification runtime owns target/replay shutdown internally. |
| Audit runtime | Main calls `start_audit_system().await`. | Main shutdown calls `stop_audit_system().await`. | Audit runtime owns target/replay shutdown internally. |
| Metrics and memory loops | Main calls `init_metrics_runtime(ctx.clone())`, `init_memory_observability(ctx.clone())`, and `init_auto_tuner(ctx.clone())` when observability metrics are enabled. | Main shutdown only cancels the shared runtime token. | Shared runtime token. |
| Allocator reclaim | Main calls `init_allocator_reclaim(ctx.clone())` unconditionally. | Main shutdown cancels the shared runtime token. | Shared runtime token. |
| Capacity manager | Main calls `init_capacity_management().await` before HTTP listener startup and ECStore creation. | No direct main shutdown call. | Current scheduled capacity and metrics loops do not receive a shutdown token. |
| Optional protocol servers | Main calls feature-gated FTP, FTPS, WebDAV, and SFTP init functions. | Main shutdown calls each stored `ShutdownHandle` and waits for all protocol shutdown futures. | Per-protocol broadcast shutdown handles. |
| Deferred IAM recovery | `bootstrap_or_defer_iam_init(...)` may spawn a deferred recovery loop. | Main shutdown cancels the shared runtime token. | Shared runtime token. |

## Service Inventory

| Service | Trigger and workers | Side effects | Status and metrics | Migration notes |
|---|---|---|---|---|
| Capacity background refresh | `rustfs/src/capacity/capacity_integration.rs::init_capacity_management` delegates to `init_capacity_management_for_local_disks`, then `crates/object-capacity/src/capacity_manager.rs::start_background_task` spawns a scheduled refresh loop and a runtime summary loop. | Refreshes global capacity cache from local disks and logs runtime summaries. | Uses the object-capacity manager state and log summaries; no explicit shutdown status surface is exposed here. | Add read-only status before any controller migration. A future controller must not change scheduled interval defaults or singleflight refresh behavior. |
| ECStore endpoint monitor | `crates/ecstore/src/sets.rs::new` spawns `monitor_and_connect_endpoints`. | Monitors endpoint connectivity and reconnect behavior for erasure sets. | Logs monitor start, cancellation, and exit. | This is storage-adjacent and must stay outside broad controller movement until storage shutdown semantics are explicitly covered. |
| Local disk health monitor | `crates/ecstore/src/store/init.rs::init` enables disk health checks after store initialization; `crates/ecstore/src/disk/disk_store.rs::enable_health_check` spawns writable and recovery monitors. | Periodically probes disk writability, can create test objects named `health-check-*`, and updates disk runtime health state. | Disk info includes runtime health metrics and waiting counts. | Do not merge this with scanner/heal controller work; probes affect disk health semantics. |
| Data scanner | `crates/scanner/src/scanner.rs::init_data_scanner` configures scanner defaults, applies runtime config, waits the initial scanner delay, then loops `run_data_scanner`. | Updates data usage cache, scans buckets/sets, evaluates lifecycle rules, queues replication heal, queues scanner heal, and emits scanner alerts. | Scanner runtime config/status is exposed through admin scanner status; scanner metrics record ILM, replication admission, heal admission, checkpoints, yields, and alerts. | Scanner implies heal because scanner can enqueue heal requests. Future controller status must separate scheduler state from scanner work-source accounting. |
| Heal/AHM | `crates/heal/src/lib.rs::init_heal_manager` starts `HealManager`, initializes the shared heal channel, and spawns `HealChannelProcessor`. | Consumes heal requests from the global heal channel and drives heal work through the configured heal storage API. | Global active-task and queue-length atomics track current heal pressure. | Keep heal admission and channel semantics intact. Controller work should first expose queue/active status and shutdown state. |
| Bucket replication pool | `crates/ecstore/src/bucket/replication/replication_pool.rs::init_background_replication` creates global replication stats and the global pool; pool resizing spawns regular, large-object, and failed-object workers. | Replicates object and delete operations, updates queue stats, and maintains replication worker pools. | Replication stats expose active worker counts and queue accounting. | Worker resize behavior currently closes channels to stop workers. Do not replace this with a generic controller until queue close semantics are captured by tests. |
| Bucket replication resync | Main calls `get_global_replication_pool().init_resync(ctx.clone(), buckets.clone())`; the pool spawns `start_resync_routine`. Admin site-replication handlers can start or cancel per-bucket resync with dedicated tokens. | Loads persisted resync state, starts bucket resync, persists status, and can cancel per-target resync. | Admin site-replication status surfaces resync state. | Preserve the split between startup resync and admin-triggered resync operations. |
| Lifecycle expiry and transition | `ECStore::init` calls `init_background_expiry(self.clone())`. Scanner evaluates lifecycle events and queues expiry/transition work through `apply_expiry_rule` and `apply_transition_rule`. | Deletes expired objects, queues transitions, updates lifecycle stats, and accounts scanner ILM actions only when work is queued. | Lifecycle state tracks worker counts, active tasks, queue send timeouts, compensation tasks, and transition stats. | This is not a separate periodic controller today; scanner is the main event source for object lifecycle evaluation. |
| Stale multipart cleanup | `ECStore::init` calls `init_background_stale_multipart_upload_cleanup(self.clone())`. | Periodically deletes stale multipart upload data. | Logs cleanup passes when objects are deleted. | Current loop has no explicit cancellation token and exits when ECStore is dropped. Future controller work needs an explicit lifecycle decision before changing it. |
| Notification runtime | `rustfs/src/server/event.rs::init_event_notifier` initializes live event stream support even when notification targets are disabled; when enabled, it loads server config and activates targets. Config reload uses `NotificationConfigManager::reload_config`. | Installs ECStore event dispatch hook, activates notification targets, manages replay/runtime target state, and supports live event streams. | Notification module state is refreshed from persisted module switches; target health is available through runtime target status. | Keep live event stream support separate from target delivery enablement. Reload must remain admin-triggered and peer-signaled. |
| Audit runtime | `rustfs/src/server/audit.rs::start_audit_system` starts audit only when module switches and configured targets allow it. `AuditSystem::reload_config` replaces runtime targets. | Dispatches audit events to configured targets and manages replay workers. | Audit observability records config reloads and target delivery metrics. | Do not couple audit lifecycle to notification lifecycle even though the runtime patterns are similar. |
| Dynamic config reload | Admin config handlers call `apply_dynamic_config_for_subsystem`, then `signal_dynamic_config_reload` or `signal_config_snapshot_reload` through the global notification system. | Applies scanner/heal runtime config, audit reloads, notification reloads, and peer reload signals. | Logs local and peer reload failures. Audit reload increments audit config reload metrics. | This is admin-triggered fanout, not a background scheduler. Controller work should preserve per-subsystem validation and error boundaries. |
| Metrics runtime | `crates/obs/src/metrics/scheduler.rs::init_metrics_runtime` spawns multiple interval loops for cluster, bucket, node, resource, audit, notification, and replication bandwidth metrics. | Periodically collects and reports metrics. | Reports through the metrics runtime and logs cancellation warnings. | Keep intervals and collector grouping stable while adding status snapshots. |
| Memory observability | `rustfs/src/memory_observability.rs::init_memory_observability` spawns a token-cancelled sampler. | Periodically records memory snapshots. | Emits memory observability metrics and exposes a read-only status snapshot for metrics enablement, interval, cancellation source, and shutdown handle shape. | This is the first low-risk pilot for read-only controller status because it already has a simple token loop. |
| Allocator reclaim | `rustfs/src/allocator_reclaim.rs::init_allocator_reclaim` spawns a token-cancelled reclaim loop when enabled. | Observes reclaimable work and may run allocator reclaim after idle intervals. | Emits reclaim enabled/backend counters, active-request gauges, scanner/heal activity gauges, and reclaim result counters. | A controller must preserve idle-streak logic and backend-specific force behavior. |
| Auto-tuner | `rustfs/src/init.rs::init_auto_tuner` optionally spawns a 60-second loop when `RUSTFS_AUTOTUNER_ENABLED` is true. | Tunes concurrency manager settings from performance metrics. | Logs iteration success/failure. | Treat as behavior-sensitive; a future controller needs explicit rollback because it can change runtime concurrency. |
| Update check | `rustfs/src/init.rs::init_update_check` spawns one async task with a 30-second timeout when update checks are enabled. | Performs version check network I/O and logs available updates. | Logs result only. | This is a one-shot task, not a controller candidate for the first BGC PRs. |
| Deferred IAM recovery | `rustfs/src/startup_iam.rs::spawn_iam_recovery_task` retries IAM init with backoff and finalizes readiness when successful. | Can initialize IAM later, initialize AppContext if needed, mark `IamReady`, and publish `FullReady`. | Readiness state reflects deferred recovery progress. | Keep this lifecycle-critical path separate from generic background controllers. |
| Optional protocol servers | `rustfs/src/init.rs` starts FTP, FTPS, WebDAV, and SFTP with per-protocol `ShutdownHandle`s when features and config enable them. | Serve protocol traffic in background tasks. | Shutdown logs per protocol. | Protocol servers already have explicit handles; do not fold them into BGC until the service registry owns shutdown ordering. |

## Current Gaps To Preserve Before Controller Work

- ECStore background-service cancellation has a public global token API, but this
  inventory found no current startup call to create that token. Lifecycle expiry
  workers therefore use their fallback token when no global token exists.
- Capacity manager loops do not receive the main runtime cancellation token.
- Replication worker pools stop some workers by closing channels, while resync
  uses cancellation tokens. These are different shutdown contracts.
- Scanner, lifecycle, replication, and heal are coupled by work queues and
  metrics. Moving one without status snapshots for the others risks hiding work
  admission failures.
- Dynamic config reload is admin-triggered and peer-signaled, not a periodic
  background loop.

## BGC-002 Contract Inputs

These inputs are formalized in
[`background-controller-contract.md`](background-controller-contract.md).

Future controller contract work should start with a read-only shape:

- `desired`: enabled/disabled plus static config source.
- `current`: started, stopped, running, degraded, or disabled.
- `status`: worker counts, queue lengths, last cycle/reload time, and last error.
- `shutdown`: cancellation source and whether the service has an explicit stop
  handle.
- `side_effects`: storage writes, target activation, external I/O, metrics, and
  readiness changes.

The first pilot should use a service with an existing simple cancellation loop
and no storage writes, such as memory observability. Scanner, heal, replication,
lifecycle, and disk health must wait for focused preservation tests.
