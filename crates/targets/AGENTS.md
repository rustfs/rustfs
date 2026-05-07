# Targets Crate Instructions

Applies to `crates/targets/`.

`rustfs-targets` provides the notification target abstraction layer:
the `Target<E>` trait, built-in implementations (Webhook, Kafka, MQTT,
NATS, Pulsar, MySQL), persistent queue store, DSN/configuration
builders, and the `ChannelTargetType` registry that maps target types
to their runtime factories.

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

### MySQL Integration Tests

Integration tests in `tests/mysql_integration.rs` require a running MySQL 8.0+
or TiDB 8.5+ instance. They are `#[ignore]` by default so CI never runs them.

Start a test MySQL instance with Podman:

```bash
podman run -d --name rustfs-mysql-test \
  -e MYSQL_ROOT_PASSWORD=testpass \
  -e MYSQL_DATABASE=testdb \
  -p 3306:3306 \
  docker.io/library/mysql:8.0.36
```

Wait for MySQL to be ready (look for `ready for connections` in logs),
then run the integration tests:

```bash
RUSTFS_MYSQL_TEST_DSN="root:testpass@tcp(127.0.0.1:3306)/testdb" \
  cargo test -p rustfs-targets -- --ignored
```

Clean up:

```bash
podman rm -f rustfs-mysql-test
```

## Suggested Validation

- `cargo test -p rustfs-targets`
- Full gate before commit: `make pre-commit`
