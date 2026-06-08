# rustfs-object-capacity Agent Guide

This document is a lightweight maintainer-oriented guide for collaborators working in `crates/object-capacity`.

## Purpose

`rustfs-object-capacity` is responsible for estimating and refreshing RustFS object-data usage, not general filesystem capacity. Changes in this crate can affect:

- admin `used_capacity` responses
- refresh latency and background work
- dirty-disk propagation after writes, heal, and data movement
- capacity-related metrics and operational observability

Keep changes narrow and source-driven.

## Source of Truth

Before changing behavior, read these files first:

- `src/scan.rs`
- `src/capacity_manager.rs`
- `src/capacity_scope.rs`
- `src/types.rs`
- `../../rustfs/src/capacity/service.rs`
- `../config/src/constants/capacity.rs`

Do not treat shell scripts or old docs as the authoritative behavior definition when the Rust code says otherwise.

## Change Priorities

Prefer the following order:

1. correctness of returned capacity values
2. safety of degraded behavior under timeout, stall, or partial failure
3. consistency of dirty-subset refresh and per-disk cache state
4. bounded scan cost and runtime overhead
5. clarity of metrics and logs

## Important Behavioral Constraints

- This crate intentionally prefers a usable degraded result over hard failure when partial progress is available.
- Dirty-subset refresh is safe only after a complete per-disk cache has been established.
- Subset refresh failures with partial errors must not silently corrupt aggregate cache state.
- Environment-derived configuration is cached behind `OnceLock` in non-test builds, so runtime env changes usually require a restart.
- Symlink following is disabled by default for safety and determinism.
- Multi-disk scans are concurrent, so avoid changes that introduce hidden shared-state coupling.

## Editing Guidance

- Keep scan-path changes local and easy to audit.
- Avoid refactoring the control flow unless it is necessary for correctness.
- Preserve the current exact-vs-estimated semantics unless the task explicitly changes them.
- Reuse existing configuration constants from `rustfs-config` instead of introducing duplicate literals.
- Treat metric names and labels as compatibility-sensitive unless the task explicitly allows a metric contract change.

## Validation Guidance

For code changes, prefer focused verification close to the changed behavior:

- scan correctness tests in `src/scan.rs`
- manager/refresh-state tests in `src/capacity_manager.rs`
- scope propagation tests in `src/capacity_scope.rs`
- integration behavior in `../../rustfs/src/capacity/service.rs` or related app tests when needed

Useful commands:

```bash
cargo test -p rustfs-object-capacity
cargo test -p rustfs-object-capacity --doc
cargo bench -p rustfs-object-capacity --bench capacity_scan
```

If the change is documentation-only, lightweight verification is enough.

## Documentation Expectations

- Keep `README.md` in English.
- Keep `README_ZH.md` aligned when behavior or public usage changes.
- Prefer describing real behavior from code instead of aspirational architecture.

## Common Pitfalls

- Assuming `used_capacity` means total disk usage instead of object-data usage.
- Forgetting that the initial full refresh is required before safe dirty-subset refresh.
- Forgetting that partial errors can still produce a returned result.
- Overlooking the fallback path that stores externally supplied disk-used capacity as `DataSource::Fallback`.
- Changing timeout or sampling behavior without reviewing the observable effects on degraded results and metrics.
