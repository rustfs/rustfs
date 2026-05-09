# Targets Crate Instructions

Applies to `crates/targets/`.

`rustfs-targets` provides the notification target abstraction layer:
the `Target<E>` trait, built-in implementations (Webhook, Kafka, MQTT,
NATS, Pulsar, MySQL, Postgres, Redis), persistent queue store,
DSN/configuration builders, and the `ChannelTargetType` registry that
maps target types to their runtime factories.

## Library Design

- Treat crate code as reusable library code by default.
- Prefer `thiserror` for library-facing error types.
- Do not use `unwrap()`, `expect()`, or panic-driven control flow outside tests.

## Testing

- Keep unit tests close to the module they test.
- Keep integration tests under `crates/targets/tests/` directory.
- Add regression tests for bug fixes and behavior changes.

## Async and Performance

- Keep async paths non-blocking.
- Move CPU-heavy operations out of async hot paths with `tokio::task::spawn_blocking` when appropriate.

## Integration Tests

Integration tests under `tests/` are `#[ignore]` by default so CI never runs
them. See the module-level doc comment in each test file for prerequisites
and run commands:

- `tests/mysql_integration.rs` — MySQL 8.0+ / TiDB 8.5+
- `tests/postgres_integration.rs` — PostgreSQL

## Suggested Validation

- `cargo test -p rustfs-targets`
- Full gate before commit: `make pre-commit`
