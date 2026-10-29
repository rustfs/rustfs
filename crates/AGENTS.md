# Crates Instructions

Applies to all paths under `crates/`.

## Library Design

- Treat crate code as reusable library code by default.
- Prefer `thiserror` for library-facing error types.
- Do not use `unwrap()`, `expect()`, or panic-driven control flow outside tests.

## Error Type Design

- Fallible public APIs use typed errors that fit the boundary, preferably an existing domain error or standard error type; do not return `Result<_, String>`.
- Preserve error variants callers need for recovery, aggregation, or quorum decisions. Use type-erased errors only when erasure is intentional in the boundary contract, not as a shortcut around error design.
- When implementing `std::error::Error`, expose a stored inner error through `source()` so the error chain remains available.
- Internal helpers that return `Result<_, String>` and are immediately wrapped via `.map_err(Error::other)` should return the actual error type directly.

## Concurrency

- Document lock acquisition order when a module uses multiple locks. Never acquire the same set of locks in different orders across code paths.
- Never hold a `tokio::sync::RwLock`/`Mutex` write guard across `.await` points unless the critical section is unavoidably async and the hold time is bounded.
- Prefer direct atomic `fetch_*` operations for unconditional updates and `compare_exchange` loops only for conditional updates such as peaks or adaptive state.
- When resetting multi-field atomic statistics, use a version/sequence counter or accept that concurrent readers may see partial snapshots; document the tradeoff.
- `std::sync::Mutex` is acceptable in async context only when held for a brief, non-`await`-containing critical section. If in doubt, use `tokio::sync::Mutex`.

## Recursion Safety

- Recursive tree/graph traversals must have a depth limit (e.g., `max_depth` counter) or use an iterative approach with an explicit `Vec` stack.
- This applies to cache trees, directory walks, and any user-influenced hierarchy.
- A corrupted or malicious input must not be able to overflow the thread stack.

## Type Casting

- Never use `as` for numeric conversions that may truncate or overflow. Use `try_into()` with typed error handling; clamp or saturate only when the domain explicitly requires it.
- Before converting floating-point input to an integer, validate finiteness, sign, and the destination range. A lower-bound clamp alone is insufficient.
- Review narrowing, signedness-changing, and float-to-integer casts against the value range and domain contract; do not require unrelated rewrites of proven lossless conversions.

## Testing

- Keep unit tests close to the module they test.
- Keep integration tests under each crate's `tests/` directory.
- Add or update regression tests when existing tests do not directly cover the changed behavior; a bug regression should fail without the fix.
- Every test needs an observable failure criterion. Direct assertions, delegated assertions, snapshots/properties, `#[should_panic]`, and meaningful `Result` failures are all valid; a call that can silently succeed is not.
- In tests, prefer `.expect("context: what was being tested")` over bare `.unwrap()`. A test failure should tell you which operation failed and with what input.

## Async and Performance

- Keep async paths non-blocking.
- Move CPU-heavy operations out of async hot paths with `tokio::task::spawn_blocking` when appropriate.
