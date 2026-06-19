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

## Optional Runtime Boundary

`startup_optional_runtimes` owns shutdown for optional runtime services that are
not readiness or fatal startup boundaries. The current owner set is protocol
servers only. Future optional sidecars must enter this boundary with explicit
shutdown handles and status snapshots instead of adding ad hoc shutdown work to
`startup_services`.

## AppContext Foundation

Early AppContext work should split resolver files and add compatibility tests before
boot extraction or consumer migration. This keeps the migration context-first while
preserving the old global fallback path during transition.
