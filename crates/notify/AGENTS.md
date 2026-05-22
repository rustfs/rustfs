# Notify Crate Instructions

Applies to `crates/notify/`.

`rustfs-notify` is the domain layer for bucket notification semantics. It
builds rules, event dispatch flow, and config/runtime orchestration on top of
shared plugin/runtime primitives from `rustfs-targets`.

## Domain Boundaries

- Keep notify-specific business logic here:
  - bucket/rule evaluation
  - event bridge and pipeline dispatch
  - notify config reload orchestration
- Keep shared runtime/plugin mechanics in `rustfs-targets`:
  - do not duplicate replay worker lifecycle logic
  - do not reimplement plugin descriptor/registry/catalog semantics
  - do not move install/control-plane state into this crate

## Runtime Layering Rules

- `runtime_facade.rs` is the mutation/orchestration boundary:
  activation, replace, stop workers, shutdown.
- `runtime_view.rs` is read-only runtime observation:
  active targets, metrics/health snapshots, runtime status snapshots.
- `config_manager.rs` should map config to runtime updates through facade/view
  and `runtime_target_id_for_subsystem`; avoid bypassing these boundaries.
- `stream.rs` is a compatibility shim; new replay/runtime work should prefer
  shared helpers in `rustfs-targets::runtime`.

## Change Style

- Preserve best-effort dispatch semantics and observability signals unless the
  task explicitly requests behavior changes.
- Reuse existing notify constants and subsystem mappings from `rustfs_config`.
- Keep changes local and avoid cross-crate refactors from this crate unless
  required by the task.

## Testing

- Keep unit tests close to changed modules.
- Add regression tests for:
  - rules to runtime target resolution
  - runtime facade replace/shutdown behavior
  - runtime view health/status/metrics snapshots
- Suggested validation:
  - `cargo test -p rustfs-notify`
  - Focused: `cargo test -p rustfs-notify runtime_facade`
  - Focused: `cargo test -p rustfs-notify runtime_view`
  - Focused: `cargo test -p rustfs-notify config_manager`
- Full gate before commit: `make pre-commit`
