# Heal And Scanner Logging Governance

This document defines the logging convergence plan for `crates/heal` and
`crates/scanner`. The target style follows `rustfs/src/main.rs`: structured,
event-oriented, easy to scan, and conservative about noise.

## Goals

- Make logs readable in both plain text and structured sinks.
- Standardize around `event`, `component`, `subsystem`, `state` or `result`,
  and a small set of context fields.
- Preserve operationally important failures and lifecycle transitions.
- Reduce repetitive progress chatter, step banners, and sentence-style prefixes.

## Target Shape

Preferred log shape:

```rust
info!(
    target: "...",
    event = "...",
    component = "...",
    subsystem = "...",
    state = "...",
    key = value,
    "Short stable message"
);
```

Guidelines:

- `target`: module-oriented and stable, such as `rustfs::heal::task`.
- `event`: machine-friendly snake_case identifier.
- `component`: top-level area such as `heal` or `scanner`.
- `subsystem`: narrower runtime area such as `task`, `storage`, `io`,
  `folder`, or `disk_scanner`.
- `state` or `result`: preferred over sentence-only outcomes.
- Message text: short label, not a narrative sentence with embedded data.

## Severity Policy

- `error!`: request failure, persistence failure, runtime failure, or data loss
  relevant events.
- `warn!`: degraded path, skipped work, fallback, policy rejection, or noisy but
  actionable operator signal.
- `info!`: lifecycle milestones, admission decisions, successful completion of
  meaningful units of work.
- `debug!`: stage transitions, low-level flow, concurrency budgeting, and
  expected internal detail.

## Noise Reduction Rules

- Convert `Step 1`, `Step 2`, `Starting ...`, `Done ...` banners into structured
  stage transitions and keep them at `debug!` unless operators need them.
- Avoid repeated path-prefixed free text like `scan_folder:` or
  `start_auto_disk_scanner:`.
- Do not emit success logs for tiny inner-loop operations unless they represent
  a boundary that operators actually inspect.
- Keep duplicate admission, queue pressure, and temporary scanner folder churn
  visible, but structured and bounded.

## Rollout Phases

1. Define local event taxonomies and convert the highest-noise hotspots first.
2. Normalize lifecycle and admission logs in `heal::manager`.
3. Normalize task/storage logs in `heal`.
4. Normalize scanner runtime, IO, and folder traversal/lifecycle logs.
5. Extend guardrails so legacy sentence-style logs do not re-enter the touched
   surfaces.

## Batch Status

- Batch 1 completed:
  `heal::task`, `heal::storage` first object IO slice, `heal::manager` auto disk
  scanner, `scanner`, `scanner_io` disk bucket flow, `scanner_folder` lifecycle
  and traversal entry slice.
- Batch 2 completed:
  `heal::manager` lifecycle and queue admission, `scanner_io` persistence and
  publish flow, `scanner_folder` heal admission and alert logs.
- Batch 3 completed:
  `heal::storage` remaining repair/admin flow, `heal::task` bucket, metadata,
  MRF, EC decode, erasure set branches, `heal::erasure_healer`, `heal::resume`,
  `heal::channel`, plus scanner runtime-config, cache, and folder/heal tail
  cleanup.

## Verification

- `cargo fmt --all`
- `cargo check -p rustfs-heal -p rustfs-scanner`
- `./scripts/check_logging_guardrails.sh`
- before PR: `make pre-commit`

## Non-Goals

- No behavior changes to heal/scanner control flow.
- No broad refactors purely for logging convenience.
- No demotion of signals that operators rely on for data integrity, queue
  saturation, or persistence failures.
