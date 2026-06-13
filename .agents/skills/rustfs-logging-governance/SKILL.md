---
name: rustfs-logging-governance
description: Standardize and review RustFS logging with structured `tracing` events, lower noise on hot paths, preserve security-sensitive diagnostics, and extend guardrails to prevent legacy logging patterns from returning. Use when editing or reviewing RustFS logs, startup/config diagnostics, cloud metadata logs, request validation logs, or `scripts/check_logging_guardrails.sh`.
---

# RustFS Logging Governance

Use this skill when RustFS logging needs to be added, cleaned up, reviewed, or protected against regressions.

## Quick Start

1. Identify the files whose logs are changing.
2. Scan current `tracing` or `log` macros before editing.
3. Convert sentence-style logs to short event-style logs.
4. Demote hot-path success logs unless operators truly need them at `info`.
5. Preserve failure, fallback, and security-relevant diagnostics.
6. Update `scripts/check_logging_guardrails.sh` when a broad cleanup removes a legacy pattern class.
7. Validate with formatting, targeted checks/tests, and the logging guardrail script.

## Core Workflow

### 1. Scope the logging surface

- Read the changed module in full before touching log lines.
- Classify the log site:
  - lifecycle/startup
  - request or validation path
  - background loop or hot path
  - fallback/degraded behavior
  - cloud metadata or external fetch path
  - metrics/config summary
- Do not rewrite business logic to make logging easier.

### 2. Use the RustFS event shape

- Prefer fields first, message second.
- Use short labels, not prose paragraphs.
- Default field shape:
  - `event`
  - `component`
  - `subsystem`
  - `state` or `result`
  - key context fields
- Reuse stable field names and avoid inventing near-duplicates.

See `references/logging-governance.md` for the event model, level policy, and anti-pattern list.

### 3. Choose the right level

- `error`: operation failure that affects behavior or security guarantees.
- `warn`: degraded path, fallback, suspicious input, or operator-actionable misconfiguration.
- `info`: low-frequency lifecycle or mode selection.
- `debug`: targeted diagnostics and low-volume detail.
- `trace`: hot-path and repetitive success-path events.

When in doubt, lower the verbosity of normal success paths and keep structured detail in fields.

### 4. Preserve security and privacy boundaries

- Do not log secrets, tokens, auth headers, raw credential payloads, or merged config dumps.
- Avoid logging raw forwarded headers or full trusted network inventories above `debug`.
- Keep warning/error logs useful without echoing attacker-controlled payloads unnecessarily.

### 5. Keep summaries aggregated

- Replace multi-line startup banners or checklist logs with one structured event.
- If metrics already express a concept, avoid duplicating it with many `info!` lines.
- Prefer counts, modes, and sources over inventories unless debug detail is truly needed.

### 6. Update guardrails when needed

- Broad logging cleanup should usually extend `scripts/check_logging_guardrails.sh`.
- Add forbidden patterns only for styles the repo has intentionally retired:
  - sentence-style lifecycle logs
  - noisy hot-path `info!`
  - checklist-style summary logs
  - legacy fallback wording that has been replaced by structured fields
- Keep guardrails concrete and grep-friendly.

### 7. Validate manually

Use the smallest relevant set:

```bash
cargo fmt --all --check
./scripts/check_logging_guardrails.sh
cargo check -p <affected-crate>
cargo test -p <affected-crate>
```

For broader Rust changes, add:

```bash
./scripts/check_unsafe_code_allowances.sh
./scripts/check_architecture_migration_rules.sh
cargo clippy -p <affected-crates> --all-targets -- -D warnings
```

## RustFS-Specific Notes

- The durable RustFS logging direction is `event + component + subsystem + state/result + key context fields`.
- `crates/concurrency` and `crates/trusted-proxies` are examples of this style for lifecycle, fallback, and cloud metadata logs.
- `scripts/check_logging_guardrails.sh` is the enforcement point for preventing removed log styles from returning.

## References

- Read `references/logging-governance.md` when you need the detailed field set, anti-pattern examples, or guardrail update checklist.
