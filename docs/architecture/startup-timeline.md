# Startup Timeline Baseline

This document records the current binary startup order before runtime/lifecycle
migration work. It is a behavior-preservation baseline only; it does not define
new startup semantics.

## Scope

- Baseline commit: `ae9d25879d72bc8977f08e61062c022e2142483b`
- Entry points covered: `rustfs/src/main.rs::main`,
  `rustfs/src/startup_entrypoint.rs::{run_process, async_main, run}`, and
  startup lifecycle helpers
- Related migration task: `G-007`
- Out of scope for this baseline: embedded startup, admin route-action matrix,
  and any runtime/lifecycle code movement

## Startup Stages

| Step | Source | Current action | Side effects | Fatal boundary | Ready stage |
|---|---|---|---|---|---|
| `BOOT-001` | `rustfs/src/startup_entrypoint.rs` | Apply external-prefix environment compatibility during async startup before command parsing. | Copies supported external env aliases into canonical `RUSTFS_*` process env keys and prints warnings or info to stderr. | Non-fatal; failure is logged to stderr and startup continues. | None |
| `BOOT-002` | `rustfs/src/main.rs` and `rustfs/src/startup_entrypoint.rs` | Call the startup entrypoint and build the Tokio runtime. | Installs runtime configuration and any runtime telemetry guard created by the runtime builder. | Fatal through `expect`; process exits if the runtime cannot be built. | None |
| `BOOT-003` | `rustfs/src/startup_entrypoint.rs` | Parse CLI command and dispatch non-server commands. | `info` and `tls` commands execute and return without server startup. | Command parse exits process with code 1; TLS command errors propagate. | None |
| `BOOT-004` | `rustfs/src/startup_preflight.rs` | Initialize config snapshot and license state. | Publishes config snapshot for later readers and initializes runtime license state. | License init is non-fallible in this path. | None |
| `BOOT-005` | `rustfs/src/startup_preflight.rs` | Initialize observability and store the global guard. | Initializes tracing/observability, stores the guard globally, and logs license/runtime telemetry status. | Fatal if observability init or guard publication fails. | None |
| `BOOT-006` | `rustfs/src/startup_runtime.rs` and `rustfs/src/startup_profiling.rs` | Log startup logo, initialize profiling, trusted proxies, rustls provider, and outbound TLS material. | Starts optional profiling tasks, trusted proxy config, default rustls provider, outbound TLS global state, TLS generation metric, and TLS metrics when enabled. | Profiling/proxy/provider setup is non-fatal; configured TLS material load is fatal on error. | None |
| `RUN-001` | `rustfs/src/startup_server.rs` | Enter startup run orchestration and create `GlobalReadiness`. | Allocates the readiness tracker shared with HTTP readiness gates. | Non-fatal. | Initial readiness state is not ready |
| `RUN-002` | `rustfs/src/startup_server.rs` | Parse and publish the configured region. | Updates ECStore global region when configured. | Fatal if the configured region is invalid. | None |
| `RUN-003` | `rustfs/src/startup_server.rs` | Resolve server address and warn on default credentials. | Computes server port/address and emits production credential warning when defaults are used. | Address parse is fatal; default credentials warning is non-fatal. | None |
| `RUN-004` | `rustfs/src/startup_server.rs` | Initialize global action credentials. | Publishes root/action credentials used by auth paths. | Fatal if global credentials cannot be initialized. | None |
| `RUN-005` | `rustfs/src/startup_server.rs` | Publish server port and address. | Updates global RustFS port and global address. | Non-fatal in this path. | None |
| `RUN-006` | `rustfs/src/startup_storage.rs` | Build endpoint pools and enforce unsupported filesystem policy. | Derives pool/set/disk layout from configured volumes and validates unsupported filesystem policy. | Fatal on endpoint build or unsupported filesystem policy error. | None |
| `RUN-007` | `rustfs/src/startup_storage.rs` | Publish endpoints and erasure type. | Updates global endpoints and erasure type. | Non-fatal in this path. | None |
| `RUN-008` | `rustfs/src/startup_storage.rs` | Initialize local disks, prewarm local disk id map, and initialize lock clients. | Opens local disk state, primes disk id lookup, and creates global lock clients. | Local disk init is fatal; prewarm and lock-client setup are non-fatal in this path. | None |
| `RUN-009` | `rustfs/src/startup_server.rs` | Initialize capacity management and service state manager. | Starts capacity management and moves service state to `Starting`. | Non-fatal in this path. | None |
| `RUN-010` | `rustfs/src/startup_server.rs` | Start S3 HTTP listener and optional console listener before storage is ready. | Starts HTTP servers with readiness gates; console listener starts only when enabled and configured. | Fatal if a configured listener cannot start. | Requests remain gated until full readiness except probe/admin/console/rpc/tonic/table-catalog exempt paths |
| `RUN-011` | `rustfs/src/startup_storage.rs` | Create cancellation token and initialize `ECStore`. | Creates the runtime cancellation token and storage engine. | Fatal if `ECStore::new` fails. | None |
| `RUN-012` | `rustfs/src/startup_storage.rs` | Initialize ECStore config and global config system. | Initializes ECStore config, attempts server-config migration, then retries global config init up to 15 times. | Migration attempt is non-fatal in this path; global config init becomes fatal after retries. | Marks the `GlobalReadiness` `StorageReady` stage after global config init succeeds; later runtime readiness still rechecks storage, IAM, and lock quorum before `FullReady` |
| `RUN-013` | `rustfs/src/startup_storage.rs` and `rustfs/src/startup_services.rs` | Start replication and KMS systems. | Starts background replication pool, then initializes KMS from startup services. | Replication init is non-fatal in this path; KMS init is fatal on error. | `StorageReady` stage is already marked; dynamic runtime storage readiness is still checked before `FullReady` |
| `RUN-014` | `rustfs/src/startup_optional_runtimes.rs` and `rustfs/src/startup_protocols.rs` | Initialize optional protocol servers. | Starts FTP/FTPS/WebDAV/SFTP when feature-enabled and configured, collecting shutdown handles. | Feature-enabled protocol init is fatal on error; disabled protocols are non-fatal. | None |
| `RUN-015` | `rustfs/src/startup_services.rs` and `rustfs/src/startup_service_components.rs` | Initialize buffer profiling, event notifier, audit, and deadlock detector. | Starts buffer profile system, event notifier, audit system, and optional deadlock detector. | Audit startup failure is logged and non-fatal; the others are non-fatal in this path. | None |
| `RUN-016` | `rustfs/src/startup_service_components.rs` | List buckets and run bucket/replication/IAM metadata migrations. | Reads bucket names, migrates bucket metadata, initializes replication resync, migrates IAM config, and initializes bucket metadata system. | Bucket list and replication resync are fatal on error; metadata migration calls are non-fatal in this path. | Storage remains ready; IAM not yet ready |
| `RUN-017` | `rustfs/src/startup_service_components.rs` and `rustfs/src/startup_iam.rs` | Bootstrap IAM inline or defer recovery. | Initializes IAM when possible; otherwise starts the deferred IAM recovery path through `startup_iam`. | Fatal only when `bootstrap_or_defer_iam_init` returns an unrecoverable error. | Inline success marks `IamReady`; deferred mode publishes `IamReady` later from the recovery task |
| `RUN-018` | `rustfs/src/startup_service_components.rs` | Initialize Keystone and OIDC auth integrations. | Loads Keystone env config and initializes OIDC providers. | Keystone config parse is fatal; Keystone runtime init failure is non-fatal; OIDC init failure is non-fatal. | None |
| `RUN-019` | `rustfs/src/startup_service_components.rs` | Add bucket notification config and initialize notification system. | Adds bucket notification configuration and publishes the global notification system. | Notification config add is non-fatal in this path; global notification init is fatal on error. | None |
| `RUN-020` | `rustfs/src/startup_service_components.rs` | Create AHM cancellation token and initialize heal manager when scanner or heal is enabled. | Creates AHM cancellation token and starts heal manager for heal/scanner workflows. | Heal manager init is fatal when enabled. | None |
| `RUN-021` | `rustfs/src/startup_service_components.rs` | Print server info, init update check, allocator reclaim, metrics, memory observability, and auto-tuner. | Starts informational/update/memory/metrics background tasks when enabled. | Non-fatal in this path. | None |
| `RUN-022` | `rustfs/src/startup_lifecycle.rs` and `rustfs/src/startup_iam.rs` | Log successful startup and publish full readiness for inline IAM. | Logs version/address, checks runtime readiness, marks `FullReady`, and sets service state to `Ready` when IAM was ready inline. | Fatal if runtime readiness is not reached within the startup wait. | Marks `FullReady` only for inline IAM here |
| `RUN-023` | `rustfs/src/startup_lifecycle.rs` | Publish global init time and start data scanner when enabled. | Sets global init time and starts scanner after the successful-startup log. | Scanner start is non-fatal in this path. | Full readiness may already be published or may await deferred IAM recovery |
| `RUN-024` | `rustfs/src/startup_lifecycle.rs` | Wait for shutdown signal. | Blocks the main task until a shutdown signal is received. | Non-fatal. | Runtime remains in its current readiness state |

## Deferred IAM Readiness

| Step | Source | Current action | Side effects | Fatal boundary | Ready stage |
|---|---|---|---|---|---|
| `IAM-001` | `rustfs/src/startup_iam.rs:256` | Attempt `init_iam_sys` during bootstrap. | Initializes IAM against the ECStore object layer when possible. | Recoverable failures can enter deferred mode; unrecoverable errors propagate from bootstrap. | None |
| `IAM-002` | `rustfs/src/startup_iam.rs:73` | Spawn IAM recovery loop when bootstrap is deferred. | Retries IAM initialization with exponential backoff until shutdown or success. | Retry failures are logged; the service remains degraded. | None |
| `IAM-003` | `rustfs/src/startup_iam.rs:52` | Finalize IAM recovery after init succeeds. | Initializes `AppContext` if needed, marks `IamReady`, and calls runtime readiness publication. | Finalize failures are retried by the recovery loop. | Marks `IamReady`, then `FullReady` when runtime readiness succeeds |

## Readiness Gate

| Step | Source | Current action | Side effects | Fatal boundary | Ready stage |
|---|---|---|---|---|---|
| `READY-001` | `rustfs/src/server/readiness.rs:130` | Treat exact probe paths and admin/console/rpc/tonic/table-catalog prefixes as readiness-gate bypass paths. | Bypass paths continue to the inner service while the global readiness gate is not ready. | Non-fatal. | Does not change readiness stages |
| `READY-002` | `rustfs/src/server/readiness.rs:171` | Reject non-probe requests while `GlobalReadiness` is not ready. | Returns `503 Service Unavailable`, `Retry-After: 5`, `Content-Type: text/plain; charset=utf-8`, and `Cache-Control: no-store`. | Non-fatal. | Does not change readiness stages |
| `READY-003` | `rustfs/src/server/readiness.rs:202` | Wait for runtime storage, IAM, and lock quorum readiness before publishing ready state. | Marks `FullReady` and updates `ServiceState` to `Ready` only when a state manager is provided. | Returns an error on timeout; inline startup treats that as fatal, while deferred IAM recovery retries finalization. | Marks `FullReady` |

## Shutdown Order

| Step | Source | Current action | Side effects | Fatal boundary | Ready stage |
|---|---|---|---|---|---|
| `STOP-001` | `rustfs/src/startup_shutdown.rs` | Cancel runtime token and move service state to `Stopping`. | Notifies cancellation-aware background tasks. | Non-fatal. | Service state moves to `Stopping`; readiness stages are not cleared here |
| `STOP-002` | `rustfs/src/startup_shutdown.rs` | Stop scanner/background services and AHM services according to enable flags. | Calls ECStore background shutdown and heal/scanner shutdown helpers. | Non-fatal in this path. | No readiness-stage change |
| `STOP-003` | `rustfs/src/startup_optional_runtimes.rs` | Plan optional runtime shutdown and log stopping state for FTP/FTPS/WebDAV/SFTP protocol servers. | Collects protocol shutdown handles. | Non-fatal in this path. | No readiness-stage change |
| `STOP-004` | `rustfs/src/startup_shutdown.rs` and `rustfs/src/startup_profiling.rs` | Stop event notifier, audit system, and profiling tasks. | Stops notifier and profiling tasks; audit stop failures are logged. | Non-fatal in this path. | No readiness-stage change |
| `STOP-005` | `rustfs/src/startup_shutdown.rs` and `rustfs/src/startup_optional_runtimes.rs` | Stop S3 and console HTTP servers, signal and wait for optional protocol shutdowns, then mark service state `Stopped`. | HTTP shutdown happens after notifier/audit/profiling shutdown in current order. | Join failures are logged by shutdown handles; this path does not return errors. | Service state moves to `Stopped`; readiness stages are not cleared here |

## Migration Rules

- Runtime/lifecycle PRs must map each moved startup line back to one of the
  `BOOT-*`, `RUN-*`, `IAM-*`, `READY-*`, or `STOP-*` rows.
- A `pure-move` PR must keep the fatal boundary and ready-stage column unchanged.
- Any intentional change to this table is a separate `behavior-change` PR with
  focused negative tests.
- Do not use this document to justify changing readiness, IAM recovery, HTTP
  listener timing, lock quorum, or shutdown order in a docs-only PR.
