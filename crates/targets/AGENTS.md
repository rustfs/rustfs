# Targets Crate Instructions

Applies to `crates/targets/`.

`rustfs-targets` is the shared target-plugin foundation for `audit` and
`notify`. It owns plugin metadata, builtin target descriptors, runtime
orchestration primitives, and plugin control-plane state modeling.

## Current Module Boundaries

- `manifest.rs`: declarative plugin metadata and marketplace-facing shape.
  Keep this layer declarative only; do not add runtime execution logic here.
- `catalog/`: centralized builtin descriptor registration and example external
  plugin assembly. Keep admin-facing plugin source data here instead of
  spreading it into handlers.
- `plugin.rs`: `TargetPluginDescriptor`, `TargetPluginRegistry`,
  `BuiltinTargetDescriptor`, and admin descriptor metadata.
- `runtime/`: shared runtime lifecycle and replay orchestration:
  `TargetRuntimeManager`, `ReplayWorkerManager`, `PluginRuntimeAdapter`,
  `BuiltinPluginRuntimeAdapter`, and sidecar protocol/runtime MVP types.
- `control_plane.rs`: install/enable/runtime state models and install policy
  validation helpers. Keep install/governance state logic centralized here.
- `config/`, `target/`, `store/`, `check/`: target config normalization,
  target implementations, queue/store, and endpoint connectivity checks.

## Change Style and Ownership Rules

- Preserve the layering above. Do not move install/runtime/governance logic
  into admin handlers or manifest structs.
- Prefer extending shared abstractions (`TargetPluginRegistry`,
  `PluginRuntimeAdapter`, `TargetRuntimeManager`) over duplicating per-domain
  orchestration logic.
- Keep external sidecar behavior scoped to current MVP boundaries unless the
  task explicitly includes real installer/transport integration.
- Reuse existing constants/keys from `rustfs_config`; avoid introducing
  duplicate literals for target field names and subsystem keys.

## Library Design

- Treat crate code as reusable library code by default.
- Return structured `TargetError`/`StoreError` results; avoid panic-driven
  control flow outside tests.
- Keep serialization contracts stable for types re-exported by `lib.rs`.

## Testing

- Keep unit tests close to the module they test.
- Keep integration tests under `crates/targets/tests/`.
- Add regression tests for behavior changes in:
  - plugin manifest/catalog/control-plane contracts
  - runtime adapter lifecycle behavior
  - target config normalization and validation
  - sidecar handshake/policy validation paths

## Async and Performance

- Keep async paths non-blocking.
- Avoid hot-path allocations and repeated config normalization when a cached
  snapshot can be reused.
- Use bounded concurrency and timeout guards for runtime and health checks.

## Integration Tests

Integration tests under `tests/` are `#[ignore]` by default so CI never runs
them. See module-level doc comments in each test file for prerequisites and
run commands.

- `tests/mysql_integration.rs` — MySQL 8.0+ / TiDB 8.5+
- `tests/postgres_integration.rs` — PostgreSQL

## Suggested Validation

- `cargo test -p rustfs-targets`
- If runtime/plugin contracts changed, run focused tests under:
  - `cargo test -p rustfs-targets plugin`
  - `cargo test -p rustfs-targets runtime`
  - `cargo test -p rustfs-targets control_plane`
- Full gate before commit: `make pre-commit`
