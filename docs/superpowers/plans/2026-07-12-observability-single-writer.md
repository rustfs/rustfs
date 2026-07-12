# Logging Safety Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prevent RustFS log inode aliasing, bound ECStore hot-path tracing, and verify the combined fix across supported deployments.

**Architecture:** Resolve stdout mirroring once from a tri-state configuration, validate Unix sink identity before registering logging workers, and make the packaged systemd unit route process output to journald. Local and OTLP initialization share the same helpers. ECStore data-path spans are TRACE-only and never capture heavy arguments. Static and runtime tests pin the combined invariants.

**Tech Stack:** Rust, tracing/tracing-appender, Unix file metadata, systemd, shell guardrails.

**Status updated:** 2026-07-12 after pulling `origin/houseme/pr-4765-ci-fix`.

**Status legend:** `[x]` completed, `[ ]` pending final validation or external evidence.

---

### Task 1: Preserve stdout configuration intent

**Files:**
- Modify: `crates/obs/src/config.rs`
- Modify: `crates/obs/src/telemetry/local.rs`
- Modify: `crates/obs/src/telemetry/otel.rs`

- [x] Add table tests proving unset, explicit true, and explicit false behavior in production and development.
- [x] Run the focused tests and confirm non-production plus explicit false fails on the current implementation.
- [x] Preserve `None` when the environment variable is absent and add one shared mirror resolver.
- [x] Use the resolver in both local and OTLP file initialization.
- [x] Run focused tests and confirm all table cases pass.

### Task 2: Reject same-inode stdout and rolling file sinks

**Files:**
- Modify: `crates/obs/src/telemetry/local.rs`
- Modify: `crates/obs/src/telemetry/otel.rs`
- Modify: `crates/obs/src/telemetry/rolling.rs` only if a read-only active-path accessor is required
- Test: `crates/obs/src/telemetry/local.rs` test module or a focused Unix integration test

- [x] Add Unix tests for same inode, hardlink alias, and distinct regular files; isolate stdout descriptor mutation in a child process if descriptor replacement is required.
- [x] Run the focused tests and confirm the same-inode case is not rejected by current code.
- [x] Add a shared pre-registration validator comparing regular-file device and inode metadata.
- [x] Invoke validation from local and OTLP file setup before subscriber registration and cleanup startup.
- [x] Run the focused tests and existing rolling/cleaner tests.

### Task 3: Establish the packaged systemd single-writer contract

**Files:**
- Modify: `deploy/build/rustfs.service`
- Modify: `scripts/check_logging_guardrails.sh`

- [x] Add a guardrail assertion that fails while the unit appends stdout or stderr to a RustFS-managed log file.
- [x] Run the guardrail and confirm it fails against the current unit.
- [x] Change stdout and stderr to journald.
- [x] Run the guardrail and confirm it passes.

### Task 4: Bound ECStore hot-path tracing

**Files:**
- Modify: `crates/ecstore/src/disk/mod.rs`
- Modify: `crates/ecstore/src/disk/local.rs`
- Modify: `crates/ecstore/src/cluster/rpc/remote_disk.rs`

- [x] Change per-object/per-disk/per-RPC success spans to TRACE with `skip_all` across all three layers.
- [x] Keep useful scalar context only in existing explicit structured RemoteDisk trace events; do not automatically capture method arguments.
- [x] Add a static guard that rejects scoped instrumentation without exact TRACE + `skip_all`, and rejects DEBUG RemoteDisk success events.
- [ ] Run the guard and focused crate tests.

### Task 5: Add combined regression guardrails

**Files:**
- Modify: `scripts/check_logging_guardrails.sh`
- Modify or create focused deployment/rotation tests as required

- [x] Guard the packaged systemd unit against append-to-active-log regression.
- [x] Guard ECStore data-path instrumentation against implicit INFO and heavy argument capture.
- [x] Verify Docker and Helm file logging remain distinct from container stdout without changing their persistence defaults.
- [ ] Add the repeatable four-node Warp acceptance procedure and measurements to the implementation handoff.

### Task 6: Verify and review

**Files:**
- Review all files changed above

- [x] Run `cargo fmt --all --check`.
- [x] Run `cargo test -p rustfs-obs`.
- [x] Run `./scripts/check_logging_guardrails.sh`.
- [ ] Run `make pre-commit` and resolve any in-scope failures.
- [x] Run the applicable correctness, concurrency, compatibility, performance, and test-coverage adversarial passes over the final diff.
- [ ] Run `make pre-pr` after all findings are resolved.
- [x] Clean generated build artifacts according to repository policy.
