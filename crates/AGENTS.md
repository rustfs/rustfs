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

## Async and Performance

- Keep async paths non-blocking.
- Move CPU-heavy operations out of async hot paths with `tokio::task::spawn_blocking` when appropriate.
