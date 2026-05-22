# Audit Crate Instructions

Applies to `crates/audit/`.

`rustfs-audit` is the domain layer for audit event fan-out and observability.
It composes shared plugin/runtime abstractions from `rustfs-targets` and keeps
audit-specific dispatch semantics, state transitions, and metrics in this
crate.

## Domain Boundaries

- Keep audit-specific behavior here:
  - audit event shaping and fan-out pipeline
  - audit system lifecycle/state transitions
  - audit metrics and reporting
- Keep shared plugin/runtime mechanics in `rustfs-targets`:
  - no duplicated replay worker orchestration
  - no duplicated runtime manager primitives
  - no plugin install/control-plane modeling in this crate

## Runtime Layering Rules

- `pipeline.rs` hosts:
  - `AuditPipeline` (dispatch and snapshot access)
  - `AuditRuntimeFacade` (runtime mutation path)
  - `AuditRuntimeView` (runtime read path)
- `registry.rs` should remain the single owner of runtime target container and
  plugin registry composition for audit.
- `system.rs` should coordinate lifecycle by calling facade/view/registry
  boundaries rather than embedding low-level runtime logic.

## Change Style

- Preserve audit delivery semantics and error handling behavior unless the task
  explicitly changes them.
- Prefer extending shared abstractions in `rustfs-targets` over patching
  one-off audit-only runtime flows.
- Keep logging and observability machine-meaningful; avoid noisy churn in hot
  dispatch paths.

## Testing

- Keep unit tests close to changed modules.
- Keep pipeline-layer regressions in `tests/pipeline_layer_test.rs`.
- Add regression tests for:
  - runtime facade activation/replace/stop/shutdown behavior
  - runtime view target/snapshot access
  - system reload and runtime commit/clear boundaries
- Suggested validation:
  - `cargo test -p rustfs-audit`
  - Focused: `cargo test -p rustfs-audit --test pipeline_layer_test`
  - Focused: `cargo test -p rustfs-audit pipeline`
- Full gate before commit: `make pre-commit`
