# Runtime And Lifecycle Contracts

**Use this when:** moving or reordering anything in `rustfs/src/startup_*.rs`, changing readiness publication, or touching shutdown ordering.
**Source of truth:** the `rustfs/src/startup_*.rs` modules listed below; readiness semantics in [readiness-matrix.md](readiness-matrix.md); global-state targets in [global-state-inventory.md](global-state-inventory.md).

Runtime and lifecycle work must preserve startup ordering, readiness behavior, and shutdown semantics.

## Startup And Readiness

- HTTP can listen early, but normal requests stay behind the readiness gate.
- The `FullReady` formula, its dependencies, and the `RUSTFS_HEALTH_PEER_READY_CHECK_ENABLE` gate are defined once in [readiness-matrix.md](readiness-matrix.md); do not restate them elsewhere.
- Boot phases keep the existing fatal and non-fatal boundaries.
- AppContext migration keeps context-first lookup with global fallback until the global path is proven unused.
- Notify and audit lifecycle behavior must not drift during lifecycle movement.
- IAM and KMS startup, deferred recovery, and fatal-boundary behavior must not be changed by pure movement PRs.

## Startup Module Ownership

Each module owns one concern; orchestration order is owned by `startup_services`, `startup_lifecycle`, and `startup_shutdown`. Movement PRs may pass handles between modules but must not reorder the steps a module owns.

| Module (`rustfs/src/`) | Owns |
|---|---|
| `startup_entrypoint.rs` | CLI command dispatch into preflight and the runtime lifecycle. |
| `startup_preflight.rs` | License init, external env compatibility, runtime foundation bootstrap. |
| `startup_runtime.rs` | Runtime foundation orchestration; outbound TLS fatal boundary when configured material fails to load. |
| `startup_runtime_hooks.rs` | Startup diagnostics, profiling hook dispatch, default crypto provider installation. |
| `startup_tls_material.rs` | Outbound TLS material loading, global publication, generation recording, TLS metrics init. |
| `startup_runtime_sources.rs` | Process-local runtime source publication (port, buffer profile, KMS manager, TLS generation). |
| `startup_fs_guard.rs` | Unsupported-filesystem policy enforcement for endpoint paths. |
| `startup_deadlock.rs` | Deadlock detector state logging. |
| `startup_server.rs` | HTTP listener start and `ServiceStateManager` publication. |
| `startup_storage.rs` | Endpoints, local disks, ECStore, lock clients, global config, background replication init; `StorageReady`. |
| `startup_bucket_metadata.rs` | Bucket metadata system init, legacy meta-bucket import (`try_migrate_bucket_metadata`, `try_migrate_iam_config`), resync intents. |
| `startup_iam.rs` | IAM init, deferred recovery, `IamReady` publication. |
| `startup_auth.rs` | OIDC and federated identity setup. |
| `startup_notification.rs` | Notification system and bucket notification configuration. |
| `startup_audit.rs` | Event notifier and audit system start. |
| `startup_observability.rs` | Auto-tuner, update check, server info, compression totals. |
| `startup_background.rs` | Scanner, heal, bitrot self-test, workload-admission provider publication. |
| `startup_protocols.rs` | FTP/FTPS/SFTP/WebDAV sidecar start and shutdown senders. |
| `startup_optional_runtime_sidecars.rs` | Handles, shutdown planning, and shutdown execution for optional sidecars that are not readiness boundaries (currently protocol servers only). New sidecars enter here with explicit shutdown handles and status snapshots, not ad hoc work in `startup_services`. |
| `startup_services.rs` | Orchestration order of runtime service startup: KMS, optional runtimes, audit, metadata, IAM, auth, notification, background services, observability. |
| `startup_lifecycle.rs` | Ready publication, global init-time publication, scanner startup, shutdown-signal wait, shutdown delegation, final stopped-state log. |
| `startup_shutdown.rs` | The shutdown sequence (see below). |
| `startup_embedded.rs`, `startup_embedded_optional.rs` | Embedded-mode reuse of the phase owners above (see below). |

## Shutdown Lifecycle Boundary

`startup_shutdown` owns the main shutdown sequence after the process receives a shutdown signal. Startup modules may pass handles into this boundary, but they must not reorder runtime-token cancellation, background service shutdown, optional runtime shutdown planning, notifier/audit/profiling shutdown, HTTP shutdown, optional runtime waits, or final service-state publication.

## Embedded Startup Reuse

Embedded startup reuses the same phase owners as the binary: server and storage phases for listen context, endpoint/local disk setup, storage runtime setup, readiness publication, and replication startup; service helpers for optional service init, bucket metadata/IAM setup, notification setup, and shutdown cleanup; lifecycle helpers for IAM readiness publication, global init-time publication, and ready-state logging. Embedded-specific behavior that stays in `startup_embedded*.rs`: stable-port requirement, one-shot global initialization guard placement, S3-only HTTP listener, warning-only KMS/audit/notification failures, no binary-only background sidecars, no state manager, server handle construction, endpoint address normalization, and process-local one-shot shutdown cleanup.

## AppContext Foundation

AppContext is a context-first facade, not a full replacement for every process global. Resolver files are split and covered by compatibility tests before boot extraction or consumer migration, so the old global fallback path keeps working during transition. New migration work keeps fallback reads inside owner-local runtime-source boundaries and follows the target inventory in [global-state-inventory.md](global-state-inventory.md).
