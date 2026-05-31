# Crates Instructions

Applies to all paths under `crates/`.

## Library Design

- Treat crate code as reusable library code by default.
- Prefer `thiserror` for library-facing error types.
- Do not use `unwrap()`, `expect()`, or panic-driven control flow outside tests.

## Testing

- Keep unit tests close to the module they test.
- Keep integration tests under each crate's `tests/` directory.
- Add regression tests for bug fixes and behavior changes.

## Error Type Design

- Public API functions must return a typed error enum (preferably `thiserror`-derived), never `Result<_, String>`.
- Do not use `Box<dyn Error>` or `Box<dyn Error + Send + Sync>` in public trait methods or struct methods. Define a concrete error type with specific variants.
- When implementing `std::error::Error`, always override `fn source()` if you store an inner error. Breaking the error chain makes debugging impossible.
- Internal helpers that return `Result<_, String>` and are immediately wrapped via `.map_err(Error::other)` should return the actual error type directly.

## Concurrency

- Document lock acquisition order when a module uses multiple locks. Never acquire the same set of locks in different orders across code paths.
- Never hold a `tokio::sync::RwLock`/`Mutex` write guard across `.await` points unless the critical section is unavoidably async and the hold time is bounded.
- Prefer `compare_exchange` loops over load-then-store for concurrent counters (peak values, adaptive heuristics).
- When resetting multi-field atomic statistics, use a version/sequence counter or accept that concurrent readers may see partial snapshots; document the tradeoff.
- `std::sync::Mutex` is acceptable in async context only when held for a brief, non-`await`-containing critical section. If in doubt, use `tokio::sync::Mutex`.

## Recursion Safety

- Recursive tree/graph traversals must have a depth limit (e.g., `max_depth` counter) or use an iterative approach with an explicit `Vec` stack.
- This applies to cache trees, directory walks, and any user-influenced hierarchy.
- A corrupted or malicious input must not be able to overflow the thread stack.

## Type Casting

- Never use `as` for numeric conversions that may truncate or overflow. Use `try_into()` with explicit error handling, or clamp with `value.max(0) as usize` when the domain is bounded.
- `f64 as usize` saturates but is fragile; clamp to `[0, usize::MAX as f64]` first.
- Treat every `as` cast in a PR review as a potential bug; require justification.

## Testing

- Keep unit tests close to the module they test.
- Keep integration tests under each crate's `tests/` directory.
- Add regression tests for bug fixes and behavior changes.
- Every test function must contain at least one `assert!`/`assert_eq!`/`assert_matches!`. A test that only calls code without asserting is not a test.
- In tests, prefer `.expect("context: what was being tested")` over bare `.unwrap()`. A test failure should tell you which operation failed and with what input.

## Async and Performance

- Keep async paths non-blocking.
- Move CPU-heavy operations out of async hot paths with `tokio::task::spawn_blocking` when appropriate.
