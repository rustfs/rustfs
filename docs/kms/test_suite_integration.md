# KMS Test Suite Integration

RustFS ships with an extensive set of automated tests that exercise the KMS stack. This guide explains how to run them locally and in CI.

## Crate Overview

- `crates/kms` – unit tests for configuration, caching, and backend adapters.
- `crates/e2e_test/src/kms` – end-to-end suites for Local and Vault backends, multipart uploads, edge cases, and fault recovery.
- `crates/e2e_test/src/kms/common.rs` – reusable test environments (spins up RustFS, configures Vault, manages buckets).

## Prerequisites

| Requirement | Purpose |
|-------------|---------|
| `vault` binary (>=1.15) | Required for Vault end-to-end tests. Install from Vault releases. |
| `awscurl` (optional) | Debugging helper to hit admin endpoints. |
| `openssl`, `md5` | Used by SSE-C helpers during tests. |
| Local ports | Tests bind ephemeral ports (ensure `127.0.0.1:<random>` is free). |

## Running Unit Tests

```bash
cargo test --workspace --exclude e2e_test
```

This covers the core KMS crate plus supporting libraries.

## Running End-to-End Suites

### All KMS Tests

```bash
NO_PROXY=127.0.0.1,localhost \
HTTP_PROXY= HTTPS_PROXY= \
cargo test -p e2e_test kms:: -- --nocapture --test-threads=1
```

- `--nocapture` streams logs to stdout for troubleshooting.
- `--test-threads=1` ensures serial execution; most tests spawn standalone RustFS and Vault processes.

### Local Backend Only

```bash
cargo test -p e2e_test kms::kms_local_test:: -- --nocapture --test-threads=1
```

### Vault Backend Only

```bash
vault server -dev -dev-root-token-id=dev-root-token &
VAULT_PID=$!

cargo test -p e2e_test kms::kms_vault_test:: -- --nocapture --test-threads=1
kill $VAULT_PID
```

The tests can also start Vault automatically if the binary is found on `PATH`. When running in CI, whitelist the `vault` executable in the sandbox or mark the job as privileged.

## Updating Fixtures

- Adjustment to SSE behaviour or multipart limits often requires touching `crates/e2e_test/src/kms/common.rs`. Keep helpers generic so multiple tests can reuse them.
- When fixing bugs, add targeted coverage in the relevant suite (e.g. `kms_fault_recovery_test.rs`).
- Vault-specific fixtures live in `crates/e2e_test/src/kms/common.rs::VaultTestEnvironment`.

## Debugging Tips

- Use the `CLAUDE DEBUG` log lines (left intentionally verbose) to inspect the RustFS server flow during tests.
- If a test fails with `Operation not permitted`, rerun with sandbox overrides (`cargo test ...` with elevated permissions) as shown above.
- Attach `RUST_LOG=rustfs::kms=debug` to surface detailed backend interactions.

## CI Recommendations

- Split KMS tests into a dedicated job so slower suites (Vault) do not gate unrelated changes.
- Cache the Vault binary and reuse it across runs to minimise setup time.
- Surface logs and `target/debug/e2e_test-*` binaries as artifacts when failures occur.

For API usage examples and configuration reference, consult [http-api.md](http-api.md) and [dynamic-configuration-guide.md](dynamic-configuration-guide.md).
