# Runtime And Lifecycle Contracts

Runtime and lifecycle work must preserve startup ordering, readiness behavior, and
shutdown semantics.

## Startup And Readiness

- HTTP can listen early, but normal requests must remain behind readiness gates.
- `FullReady = storage_ready && iam_ready && lock_quorum_ready`.
- Boot phases must keep the old fatal and non-fatal boundaries.
- AppContext migration keeps context-first lookup with global fallback until the
  global path is proven unused.
- Notify and audit lifecycle behavior must not drift during lifecycle movement.
- IAM and KMS startup, deferred recovery, and fatal boundary behavior must not be
  changed by pure movement PRs.

## Service Registry Scope

`ServiceRegistry` is only for lifecycle and shutdown ordering. It must not become a
general dependency injection container.

Allowed responsibilities:

- Register start and stop order.
- Expose read-only status snapshots.
- Coordinate graceful shutdown.

Disallowed responsibilities:

- Construct arbitrary dependencies for business logic.
- Hide globals behind a service-locator API.
- Change startup side effects while moving code.

## Shutdown Lifecycle Boundary

`startup_shutdown` owns the main shutdown sequence after the process receives a
shutdown signal. Startup modules may pass handles into this boundary, but they
must not reorder runtime-token cancellation, background service shutdown,
optional runtime shutdown planning, notifier/audit/profiling shutdown, HTTP
shutdown, optional runtime waits, or final service-state publication.

## Startup Lifecycle Boundary

`startup_lifecycle` owns the ready-to-shutdown orchestration after runtime
services initialize. Service modules may return initialized handles into this
boundary, but they must not reorder ready publication, global init-time
publication, scanner startup, shutdown-signal wait, shutdown delegation, or the
final stopped-state log.

## Startup Service Component Boundary

`startup_service_components` owns individual runtime service startup component
helpers while `startup_services` preserves their orchestration order. Migration
PRs must not change KMS, optional runtime, audit, metadata, IAM, auth,
notification, background service, or observability startup ordering while moving
these helpers.

## Optional Runtime Boundary

`startup_optional_runtime_sidecars` owns startup handles, shutdown planning, and
shutdown execution for optional runtime services that are not readiness
boundaries. `startup_optional_runtimes` remains a compatibility handoff for the
old module path. The current owner set is protocol servers only. Future optional
sidecars must enter this boundary with explicit shutdown handles and status
snapshots instead of adding ad hoc startup or shutdown work to
`startup_services`.

## Startup Runtime Hook Boundary

`startup_runtime_hooks` owns runtime hook side effects that wrap the startup
foundation but are not TLS material loading: startup diagnostics, profiling
hook dispatch, and default crypto provider installation. `startup_runtime`
preserves BOOT-006 orchestration and outbound TLS fatal behavior, while
`startup_profiling` remains a compatibility handoff for the old profiling hook
path.

## Startup TLS Material Boundary

`startup_tls_material` owns configured outbound TLS material loading, global
outbound TLS publication, generation recording, and TLS metrics initialization.
`startup_runtime` still owns BOOT-006 ordering and must preserve the fatal
boundary when configured TLS material fails to load.

## AppContext Foundation

Early AppContext work should split resolver files and add compatibility tests before
boot extraction or consumer migration. This keeps the migration context-first while
preserving the old global fallback path during transition.
