---
name: rust-code-quality
description: Enforce Rust-specific code quality rules on every code change. Use before merge to catch unwrap abuse, silent truncation, unnecessary cloning, lock ordering violations, recursion risks, and error type anti-patterns.
---

# Rust Code Quality Gate

Use this skill on every Rust code change to enforce quality rules that `cargo clippy` does not catch.

## Quick Start

1. Identify changed `.rs` files.
2. Run automated checks on changed files.
3. Run manual review checklist on the diff.
4. Report findings; block merge if P0/P1 issues exist.

## Automated Checks

Run these on every changed `.rs` file (excluding test modules):

```bash
# 1. unwrap/expect in production code
rg -n '\.unwrap\(\)|\.expect\(' <changed-files> | grep -v '#\[cfg(test)\]' | grep -v 'test' | grep -v 'bench'

# 2. Silent type truncation via `as` cast
rg -n ' as (u8|u16|u32|u64|usize|i8|i16|i32|i64|isize)\b' <changed-files>

# 3. String as error type
rg -n 'Result<.*String>' <changed-files> | grep -v test

# 4. Box<dyn Error> in public APIs
rg -n 'Box<dyn.*Error' <changed-files> | grep -v test

# 5. println/eprintln in production
rg -n 'println!\|eprintln!' <changed-files> | grep -v test

# 6. Ordering::Relaxed usage (verify each is intentional)
rg -n 'Ordering::Relaxed' <changed-files>

# 7. Default substituted for a possibly-required value (judge each: is the value optional by domain?)
rg -n 'unwrap_or_default\(\)|unwrap_or\(' <changed-files>
```

## Manual Review Checklist

For every Rust code change, verify:

### Error Handling
- [ ] No `unwrap()` or `expect()` in production code without justification comment
- [ ] No `Result<_, String>` in public API signatures
- [ ] No `Box<dyn Error>` in public trait/struct methods
- [ ] `Error::source()` is overridden when inner error is stored
- [ ] Error messages are actionable (what failed, with what input)

### Type Safety
- [ ] No silent `as` truncation (negative→unsigned, large→small)
- [ ] `try_into()` or explicit clamping used for numeric conversions
- [ ] No `f64 as usize` without prior clamping

### Concurrency
- [ ] Lock acquisition order is documented when multiple locks are used, and matches every other call site taking any overlapping subset (ABBA check)
- [ ] No `tokio::sync` lock guard (read or write) held across `.await` without bounded hold time — long-lived read guards wedge writers (#4195)
- [ ] Concurrent counters use `compare_exchange` loops, not load-then-store
- [ ] `std::sync::Mutex` in async context is held only briefly, never across `.await`

### Memory and Performance
- [ ] No `.clone()` on structs with >5 heap-allocated fields in hot paths
- [ ] `HashMap::with_capacity()` / `Vec::with_capacity()` used when size is known
- [ ] Large buffers wrapped in `Arc` rather than cloned
- [ ] Temporary string computations use `&str` or `Cow<str>` instead of `String`

### Recursion Safety
- [ ] Recursive functions have a depth limit or use iterative traversal
- [ ] Tree/cache traversals handle corrupted/cyclic input safely

### Testing
- [ ] Every test function has at least one `assert!`
- [ ] Tests use `.expect("context")` not bare `.unwrap()`
- [ ] No `println!`/`eprintln!` in production code (use `tracing`)

### Serde
- [ ] Structs from untrusted input have `#[serde(deny_unknown_fields)]`
- [ ] `#[serde(default)]` not used on security-critical fields without validation

### Code Hygiene
- [ ] No `#![allow(dead_code)]` at crate root
- [ ] No camelCase statics or Hungarian notation
- [ ] New string literals don't duplicate existing constants

### Reuse and Necessity
- [ ] No new helper duplicating an existing workspace utility (`crates/utils`, `crates/common`, the touched crate) or plain std/tokio behavior no wrapper refines; reused helpers match the call site's semantics (normalization, error type, backoff, durability gating)
- [ ] No branch without a nameable concrete trigger; no re-validation of what a validated upstream layer on the same path already guarantees (Cross-Cutting Domain Invariant patterns and pre-destructive-action re-checks are load-bearing — keep them)
- [ ] Error context attached once where actionable, not re-wrapped at every hop; no typed→generic error conversion below aggregation/quorum layers
- [ ] No comments narrating the next line, restating a signature, or describing the change itself (invariant comments — lock ordering, `SAFETY`, unwrap justification — are not narration)
- [ ] No near-duplicate test pinning the same code path and poison-value class as an existing test (boundary companions — n==max vs max+1, absent/empty/nil UUID — are never near-duplicates)

## Severity Classification

- **P0 (Block merge)**: `unwrap()` in request hot path, silent truncation on user input, lock ordering violation, recursion without depth limit
- **P1 (Must fix)**: `Result<_, String>` in public API, unnecessary clone in hot path, `Box<dyn Error>` in trait method, `unwrap_or_default()` on a domain-required value (metadata, quorum, version id)
- **P2 (Should fix)**: Missing `assert!` in test, `println!` in production, missing `with_capacity`, new helper duplicating an existing workspace utility, defensive branch with no nameable trigger (corrupt or stale persisted/peer data is always a nameable trigger for boundary-crossing values), near-duplicate test, redundant error re-wrapping
- **P3 (Nice to fix)**: Naming convention violation, missing doc comment, `as_ptr()` vs `Arc::ptr_eq`, narrating comment

## Output Template

```
## Rust Code Quality Report

### Automated Scan
- unwrap/expect in production: N found
- as casts: N found
- String errors: N found
- println/eprintln: N found

### Findings
- [P1] `path:line` — description
  - Fix: ...
  - Validation: ...

### Verdict
PASS / BLOCKED (list blocking findings)
```
