---
name: rust-code-quality
description: Run a focused Rust quality review when the user requests one, when reviewing a Rust PR/commit, or when another selected review workflow delegates Rust-specific checks. Do not auto-load for every implementation edit.
---

# Rust Code Quality Gate

Use this skill for a dedicated Rust review to cover rules that `cargo clippy`
does not catch.

## Quick Start

1. Identify changed `.rs` files.
2. Run automated checks on changed files.
3. Run manual review checklist on the diff.
4. Resolve or rebut every finding with evidence; P0/P1 findings cannot be deferred.

## Automated Checks

Use these searches to find candidates in changed `.rs` files. Inspect syntax,
`#[cfg(test)]` scope, and the changed hunk before reporting a finding; text
filters do not reliably distinguish production code from tests.

```bash
# 1. unwrap/expect candidates
rg -n '\.unwrap\(\)|\.expect\(' <changed-files>

# 2. Silent type truncation via `as` cast
rg -n ' as (u8|u16|u32|u64|usize|i8|i16|i32|i64|isize)\b' <changed-files>

# 3. String as error type
rg -n 'Result<.*String>' <changed-files>

# 4. Box<dyn Error> in public APIs
rg -n 'Box<dyn.*Error' <changed-files>

# 5. println/eprintln in production
rg -n 'println!\|eprintln!' <changed-files>

# 6. Ordering::Relaxed usage (verify each is intentional)
rg -n 'Ordering::Relaxed' <changed-files>

# 7. Default substituted for a possibly-required value (judge each: is the value optional by domain?)
rg -n 'unwrap_or_default\(\)|unwrap_or\(' <changed-files>
```

## Manual Review Checklist

For the Rust diff under review, verify:

### Error Handling
- [ ] Every production `unwrap()` or `expect()` is infallible by type or a checked invariant; explain only non-obvious invariants, using an existing type, a useful `expect` message, or a concise comment
- [ ] No `Result<_, String>` in public API signatures
- [ ] Public library APIs use domain errors unless deliberate error erasure at a boundary is part of the contract
- [ ] `Error::source()` is overridden when inner error is stored
- [ ] Error messages are actionable without exposing secret input

### Type Safety
- [ ] No silent `as` truncation (negative→unsigned, large→small)
- [ ] Fallible numeric conversions use `TryFrom`/`try_into()` and return a typed error; clamp or saturate only when the domain explicitly requires it
- [ ] Floating-point to integer conversion validates finiteness, sign, and range before conversion

### Concurrency
- [ ] Lock acquisition order is documented when multiple locks are used, and matches every other call site taking any overlapping subset (ABBA check)
- [ ] No `tokio::sync` lock guard (read or write) held across `.await` without bounded hold time — long-lived read guards wedge writers (#4195)
- [ ] Atomic read-modify-write uses the direct `fetch_*` operation when possible; use `compare_exchange` only for conditional updates
- [ ] `std::sync::Mutex` in async context is held only briefly, never across `.await`

### Memory and Performance
- [ ] On an identified hot path, report cloning or allocation only with a concrete per-request/per-object cost or benchmark signal
- [ ] Prefer borrowing, moving, `Bytes`/`Arc`, or capacity reservation only when it reduces that cost without obscuring ownership or APIs

### Recursion Safety
- [ ] Recursion over untrusted, persisted, or otherwise unbounded input has a depth limit or uses iterative traversal
- [ ] Tree/cache traversals handle corrupted/cyclic input safely

### Testing
- [ ] Tests have an observable failure criterion; delegated assertions, `#[should_panic]`, snapshot/property checks, and meaningful `Result` failures do not need a redundant `assert!`
- [ ] Use `expect` only when its message improves failure diagnosis; do not add boilerplate to self-evident test setup
- [ ] Test volume and line count are never treated as production-code growth

### Serde
- [ ] Structs from untrusted input have `#[serde(deny_unknown_fields)]`
- [ ] `#[serde(default)]` not used on security-critical fields without validation

### Code Hygiene
- [ ] No `#![allow(dead_code)]` at crate root
- [ ] No camelCase statics or Hungarian notation
- [ ] New string literals don't duplicate existing constants

### Reuse and Necessity
- [ ] No new helper duplicates `crates/utils`, `crates/common`, the touched crate, the likely domain-owning crate, a relevant direct dependency, or plain std/tokio behavior; reused helpers match the call site's semantics
- [ ] No branch without a nameable concrete trigger; no re-validation of what a validated upstream layer on the same path already guarantees (Cross-Cutting Domain Invariant patterns and pre-destructive-action re-checks are load-bearing — keep them)
- [ ] Error context attached once where actionable, not re-wrapped at every hop; no typed→generic error conversion below aggregation/quorum layers
- [ ] Comments avoid narration and change history while completely stating non-obvious lock, `SAFETY`, durability, compatibility, and unwrap invariants
- [ ] No near-duplicate test pinning the same code path and poison-value class as an existing test (boundary companions — n==max vs max+1, absent/empty/nil UUID — are never near-duplicates)

## Severity Classification

- **P0 (Block merge)**: demonstrated data loss, security breach, remote crash, or deadlock
- **P1 (Must fix)**: concrete correctness, compatibility, or material hot-path regression
- **P2 (Should fix)**: avoidable duplication or maintainability issue with a concrete simpler replacement
- **P3 (Nice to fix)**: local style or clarity issue with no behavioral risk

## Output Template

```
## Rust Code Quality Report

### Automated Scan
- unwrap/expect candidates inspected: N
- numeric-cast candidates inspected: N
- error-type candidates inspected: N
- output-macro candidates inspected: N

### Findings
- [P1] `path:line` — description
  - Fix: ...
  - Validation: ...

### Verdict
PASS / BLOCKED (list blocking findings)
```
