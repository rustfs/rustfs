# Background Controller Contract

**Use this when:** you add a status snapshot or reconcile surface for a background service (scanner, heal, lifecycle, replication, config reload, capacity, metrics, memory observability, allocator reclaim, auto-tuner), or you are tempted to fold several of them into a generic controller.
**Source of truth:** the shipped reference surfaces — `MemoryObservabilityReconcilePlan` and `reconcile()` in `rustfs/src/memory_observability.rs`, `AllocatorReclaimControllerSnapshot` and `AllocatorReclaimReconcilePlan` in `rustfs/src/allocator_reclaim.rs`, `MetricsRuntimeReconcilePlan` in `crates/obs/src/metrics/scheduler.rs`. Startup and shutdown ordering is owned by [runtime-lifecycle.md](runtime-lifecycle.md); the plane-level overview is in [storage-control-data-plane.md](storage-control-data-plane.md).

There is no `BackgroundController` trait, scheduler, or service registry. Each service exposes its own typed snapshot and reconcile plan; this page fixes the vocabulary and the rules those surfaces follow.

## Vocabulary

| Term | Meaning | Boundary |
|---|---|---|
| Desired | Static intent from env, persisted config, module switches, feature flags, bucket config, or admin configuration. | Read only; collecting desired state never normalizes or mutates config. |
| Current | Observed local runtime state: configured, disabled, running, degraded, stopping, or unknown. | Read only; never inferred by probes that create storage or network side effects. |
| Status | Machine-checkable snapshot of counters, worker counts, queue pressure, last cycle, last error, cancellation source, and shutdown-handle shape. | Side-effect-free; a missing surface is reported as `unknown`, never guessed. |
| Reconcile | Comparison of desired, current, and status that yields a plan. | Shipped plans only report; the only worker mutation they may request is `none`. |
| Side effects | Writes, deletes, queue admission, target activation, external I/O, metrics emission, readiness publication, peer signals, config reload fanout. | Declared per service before any controller touches it. |

## State Model

Snapshots use the narrowest state the code can prove:

| State | Meaning | Notes |
|---|---|---|
| NotConfigured | No valid desired source exists. | Config, module switches, or features make the service absent. |
| Disabled | A desired source exists and explicitly disables the service. | Not for missing config. |
| Starting | Start requested, steady state not reached. | Only where a start boundary exists. |
| Running | Active according to existing runtime state. | Not merely because config is enabled. |
| Degraded | Active with known error, partial, or stalled status. | No new failure classification is invented for a snapshot. |
| Stopping | Shutdown requested, not fully exited. | Only where shutdown is observable. |
| Stopped | Started earlier, now fully stopped. | Distinct from `Disabled` and `NotConfigured`. |
| Unknown | No safe status surface exists. | Preferred over speculation. |

## Read-Only Snapshot Requirements

- Status collection never starts, stops, resizes, or wakes a worker.
- Status collection never writes storage data, object metadata, target state, queue entries, persisted config, or resync metadata.
- Status collection never publishes readiness or peer reload signals.
- Missing fields are `unknown` or omitted with a documented reason.
- Cancellation source and shutdown-handle shape are reported separately from desired enabled/disabled state.
- Repeated `reconcile` calls over the same snapshot return the same plan.
- Scanner, heal, lifecycle, and replication status must not hide their queue and admission coupling.

## Coupling Notes

The services below share state or shutdown contracts and must not be folded into a generic controller without service-specific preservation tests:

- Scanner implies heal: the loop started by `init_data_scanner` (`rustfs/src/startup_lifecycle.rs`) enqueues heal work, so scanner status must separate scheduler state from work-source accounting.
- Heal/AHM owns its own token: `create_ahm_services_cancel_token` and `init_heal_manager` run in `rustfs/src/startup_background.rs`; `shutdown_ahm_services` runs in `rustfs/src/startup_shutdown.rs`. Heal admission and channel-close semantics stay intact.
- Replication has two shutdown contracts: the pool started by `init_background_replication` (`rustfs/src/startup_storage.rs`) stops workers by closing channels, while resync started by `init_resync` (`rustfs/src/startup_bucket_metadata.rs`) uses cancellation tokens, and admin-triggered resync uses per-bucket tokens.
- Lifecycle expiry, transition, and stale-multipart cleanup are started by `ECStore::init` (`init_background_expiry`, `init_background_stale_multipart_upload_cleanup` in `crates/ecstore/src/store/init.rs`), which binds the runtime token through `bind_background_cancel_token`; the scanner is their event source, so they are not a separate periodic controller.
- Notification and audit share a runtime pattern but not a lifecycle: `init_event_notifier` and `start_audit_system` (`rustfs/src/startup_audit.rs`), `shutdown_event_notifier` and `stop_audit_system` (`rustfs/src/startup_shutdown.rs`). Live event streams stay separate from target-delivery enablement.
- Dynamic config reload is admin-triggered fanout (`apply_dynamic_config_for_subsystem`, `signal_dynamic_config_reload`, `signal_config_snapshot_reload` in `rustfs/src/admin/service/config.rs`), not a loop; per-subsystem validation and error boundaries are preserved.
- Capacity refresh tasks are owned through `CapacityBackgroundTasks` returned by `init_capacity_management_managed` (`rustfs/src/capacity/capacity_integration.rs`, called from `rustfs/src/startup_entrypoint.rs`); scheduled interval defaults and singleflight refresh stay unchanged.
- Storage-adjacent monitors (`monitor_and_connect_endpoints` in `crates/ecstore/src/core/sets.rs`, `enable_health_check` in `crates/ecstore/src/disk/disk_store.rs`) change disk state and stay outside controller work.
- Deferred IAM recovery (`spawn_iam_recovery_task`, `rustfs/src/startup_iam.rs`) publishes readiness; optional protocol servers already own `ShutdownHandle`s; the auto-tuner (`init_auto_tuner` in `rustfs/src/init.rs`) changes runtime concurrency. All three stay outside generic controllers.
