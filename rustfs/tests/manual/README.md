# Manual test runners

Files in this directory are for manual execution only.
They are not auto-discovered as integration tests by `cargo test`.

## Dial9 runner

Build:

```bash
cargo build -p rustfs --features manual-test-runners --bin manual-test-dial9
```

Run:

```bash
cargo run -p rustfs --features manual-test-runners --bin manual-test-dial9
```

