# e2e_test

End-to-end test suite for RustFS. Tests spawn a real `rustfs` binary (see
`src/common.rs`) and drive it with the AWS SDK.

> This README currently only documents the CI smoke subset (backlog#1149
> ci-4). The full contributor guide — how to add a case, fixture helpers,
> `#[ignore]` semantics, harness topologies — is tracked as backlog#1153
> infra-10 and will extend this file.

## CI smoke subset (`--profile e2e-smoke`)

A subset of this crate runs on every PR via the `e2e-tests` job:

```bash
cargo nextest run --profile e2e-smoke -p e2e_test
```

The selection lives in `.config/nextest.toml` under `[profile.e2e-smoke]`
(`default-filter`). That filter is the **single wiring mechanism** for e2e
tests in CI — extend it (or add a sibling profile) instead of adding new e2e
jobs to `ci.yml`.

### Admission criteria for the smoke subset

A test module may join the smoke filter only if every test in it is:

1. **Fast** — single-digit seconds per test; the whole subset must keep the
   `e2e-tests` job ≤ 20 minutes.
2. **Single-node** — spawns its own server via
   `RustFSTestEnvironment`/`start_rustfs_server` on a random port with an
   isolated temp dir. No `RustFSTestClusterEnvironment`, no fixed ports.
3. **Dependency-free** — no pre-started server at `localhost:9000`, no Vault,
   no fixed protocol ports. Tools that may be absent on the runner (e.g.
   `awscurl`) are acceptable only when the test skips gracefully with a
   visible log line (see `bucket_policy_check_test.rs`).
4. **Not `#[ignore]`** — ignored tests are activation work (backlog#1149
   ci-13 / backlog#1148 ilm-3), not smoke candidates.

Note on `#[serial]`: nextest runs each test in its own process, so
`serial_test`'s in-process mutex does **not** serialize across tests there
(see the header of `.config/nextest.toml`). Smoke tests must therefore be
parallel-safe by construction (random port + isolated temp dir), which the
current subset is.

### Authoritative test inventory

`docs/testing/e2e-suite-inventory.md` records the per-module test counts as
listed by `cargo nextest list -p e2e_test`. Regenerate it when adding or
moving e2e tests so acceptance numbers in the test-strategy issues
(backlog#1147–#1155) stay auditable.
