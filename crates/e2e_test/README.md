# e2e_test

End-to-end test suite for RustFS. Each test spawns a **real `rustfs` binary**
(built on demand from the workspace) and drives it over the network with the
AWS SDK (`aws-sdk-s3`), raw HTTP (`reqwest` / `awscurl`), or a protocol client
(FTPS / WebDAV / SFTP). This is the black-box integration layer: exhaustive
end-to-end behavior lives here, unit behavior stays in the source crates
(see [`AGENTS.md`](AGENTS.md)).

The harness lives in [`src/common.rs`](src/common.rs) (single-node +
cluster environments, S3 client construction, `awscurl` helpers) and
[`src/chaos.rs`](src/chaos.rs) (in-process disk fault injection). Crate-wide
test conventions and environment-safety rules are in
[`AGENTS.md`](AGENTS.md); this file is the contributor guide.

## Module map (~50 modules)

Registered in [`src/lib.rs`](src/lib.rs). Grouped by concern:

| Group | Location | What it covers |
| --- | --- | --- |
| **functional** | top-level `*_test.rs` | S3 data plane: `list_objects_*`, `copy_object_*`, `delete_objects_versioning`, `head_object_*`, `checksum_upload`, `compression`, `content_encoding`, `special_chars`, `leading_slash_key`, `create_bucket_region`, `quota`, `data_usage`, `snowball_auto_extract`, `mc_mirror_small_bucket`, `archive_download_integrity`, `version_id_regression`, `delete_marker_migration_semantics` |
| **object_lock** | [`src/object_lock/`](src/object_lock) | Retention / legal-hold / WORM semantics |
| **kms** | [`src/kms/`](src/kms) | SSE-S3 / SSE-KMS / SSE-C, local + Vault backends, multipart encryption. Own guide: [`src/kms/README.md`](src/kms/README.md) |
| **policy** | [`src/policy/`](src/policy), `existing_object_tag_policy_test`, `bucket_policy_check_test`, `anonymous_access_test`, `security_boundary_test`, `multipart_auth_test` | IAM / bucket-policy / STS session policy, policy variables, anonymous access, DoS/SSRF boundaries. Own guide: [`src/policy/README.md`](src/policy/README.md) |
| **protocols** | [`src/protocols/`](src/protocols) | FTPS, WebDAV, SFTP compliance. Fixed ports, own guide: [`src/protocols/README.md`](src/protocols/README.md) |
| **reliant** | [`src/reliant/`](src/reliant) | Tests that reuse an **externally started** server (SQL/select, conditional writes, lifecycle, deleted-object reads, node-interact). Run via [`scripts/run_e2e_tests.sh`](../../scripts/run_e2e_tests.sh); see [`src/reliant/README.md`](src/reliant/README.md) |
| **cluster** | `cluster_concurrency_test`, `stale_multipart_cleanup_cluster_test`, `namespace_lock_quorum_test`, `admin_timeout_regression_test`, `object_lambda_test`, `replication_extension_test` | Multi-node scenarios via `RustFSTestClusterEnvironment` |
| **chaos / reliability** | [`src/chaos.rs`](src/chaos.rs), `reliability_disk_fault_test`, `heal_erasure_disk_rebuild_test`, `server_startup_failfast_test` | Disk offline/replace/corrupt, EC rebuild, heal, fail-fast startup |

## How to run

All commands assume repo root. `cargo test` triggers an on-demand build of the
`rustfs` binary from [`src/common.rs`](src/common.rs) (`rustfs_binary_path`) on
first use — the first invocation is slow, later ones reuse the binary.

```bash
# Whole crate (default = ignored tests skipped)
cargo nextest run -p e2e_test

# One module
cargo nextest run -p e2e_test -E 'test(list_objects_v2_pagination_test)'

# PR smoke subset (see "CI smoke subset" below)
cargo nextest run --profile e2e-smoke -p e2e_test

# ILM serial lane — ignored lifecycle tests, single-threaded (mirrors CI)
cargo nextest run -j1 --run-ignored ignored-only -p rustfs-scanner -p rustfs \
  -E 'binary(lifecycle_integration_test) or (package(rustfs) and test(lifecycle_transition_api_test))'

# Protocols suite — fixed ports, MUST be single-threaded, gated by build features
RUSTFS_BUILD_FEATURES=ftps,webdav,sftp \
  cargo test -p e2e_test test_protocol_core_suite -- --test-threads=1 --nocapture
```

The protocols suite has its own contract (fixed bind ports 9022–9301,
`--test-threads=1`, feature-gated scheduling) documented in
[`src/protocols/README.md`](src/protocols/README.md). `RUSTFS_BUILD_FEATURES`
selects which features the spawned binary is built with; leave it unset to run
every protocol entry.

### `#[ignore]` semantics

Ignored tests are excluded from the default `cargo nextest run` pass because
they need something the default runner does not provide. **Do not maintain a
static count here** — it rots (the set shrinks as ci-13 / ilm-3 activate
suites). Read the live sources instead:

```bash
rg -n '#\[ignore' crates/e2e_test/src   # every ignore + its reason string
```

The reason string on each attribute is the classifier. Current classes:

- **Needs a pre-started server** — `"requires running RustFS server at
  localhost:9000"` / `"Connects to existing rustfs server"`. These are the
  `reliant/*` and `policy/test_runner` tests; start a server first (e.g.
  [`scripts/run_e2e_tests.sh`](../../scripts/run_e2e_tests.sh)) or use
  `--run-ignored`.
- **Heavy / external tool** — `"Starts a rustfs server; enable when running
  full E2E"`, `"requires awscurl and spawns a real RustFS server"`. Spawn their
  own server and/or need `awscurl` on `PATH`.
- **Serial / global-state (ILM lane)** — lifecycle tests bind fixed ports and
  share process-global singletons; run via the ILM serial lane above.

## How to add a test

### Single-node (the common case)

Use `RustFSTestEnvironment` from [`src/common.rs`](src/common.rs). It picks a
**random free port** and a **unique temp dir** per instance, so tests are
parallel-safe by construction and clean up on `Drop`:

```rust
use crate::common::{RustFSTestEnvironment, TEST_BUCKET};

#[tokio::test]
async fn my_case() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;       // waits for readiness
    let client = env.create_s3_client();          // aws-sdk-s3 Client
    env.create_test_bucket(TEST_BUCKET).await?;
    // ... drive `client` ...
    Ok(())
}
```

Register the module in [`src/lib.rs`](src/lib.rs) under `#[cfg(test)]`.

### Cluster

Use `RustFSTestClusterEnvironment::new(node_count)` then `.start()`; it spawns
`node_count` servers over a shared erasure set and hands out per-node S3 clients
via `create_s3_client(idx)` / `create_all_clients()`. See
`cluster_concurrency_test.rs` and `namespace_lock_quorum_test.rs` for patterns.

### Fixture / helper inventory (`src/common.rs`)

| Helper | Purpose |
| --- | --- |
| `RustFSTestEnvironment::new` / `with_address` | Single-node env; random or fixed address |
| `start_rustfs_server` / `_with_env` / `_without_cleanup` | Spawn the server (optional extra args / env vars / no pre-cleanup) |
| `wait_for_server_ready` | Poll readiness before issuing requests |
| `create_s3_client` / `create_test_bucket` / `delete_test_bucket` | aws-sdk-s3 client + bucket lifecycle |
| `find_available_port` | Random free port (isolation primitive) |
| `rustfs_binary_path` / `_with_features` | Locate/build the binary; honors `RUSTFS_BUILD_FEATURES` |
| `requested_rustfs_build_features` / `rustfs_build_feature_enabled` | Feature-gate a test to what the binary was built with |
| `awscurl_available` + `execute_awscurl` / `awscurl_post` / `_get` / `_put` / `_delete` / `awscurl_post_sts_form_urlencoded` | Admin/STS API calls via `awscurl` (skip gracefully when absent) |
| `replication_fast_env` | Env vars that shrink replication timers (from repl-4); pass to `start_rustfs_server_with_env` |
| `local_http_client` / `init_logging` | Loopback HTTP client; idempotent tracing init |
| `RustFSTestClusterEnvironment` (`new`/`start`/`start_node`/`stop_node`/`create_all_clients`) | Multi-node harness |
| Constants: `DEFAULT_ACCESS_KEY`, `DEFAULT_SECRET_KEY`, `TEST_BUCKET`, `ENV_RUSTFS_BUILD_FEATURES` | Shared credentials / bucket name / env-var name |

Fault injectors live in [`src/chaos.rs`](src/chaos.rs): `DiskFaultHarness`
(`take_disk_offline`, `bring_disk_online`, `replace_disk_with_empty`,
`corrupt_object_shard`, `object_metadata_exists_on_disk`, `kill_server` /
`restart_server`) plus `signed_admin_post`.

### Isolation rules

- **Port**: never hard-code a port for single-node tests — `new()` allocates a
  random one. Fixed ports (protocols, ILM lane) force `--test-threads=1` / a
  serial CI lane.
- **Temp dir**: each env owns a temp dir cleaned on `Drop`; do not write under
  a shared path.
- **Orphans**: `RustFSTestEnvironment` kills its child on `Drop`, but a panicked
  or `kill -9`'d run can leak a `rustfs` process holding a port — see
  Troubleshooting.

### `#[serial]` vs nextest reality

`serial_test`'s `#[serial]` uses an **in-process** mutex. Under nextest each
test runs in its **own process**, so `#[serial]` does **not** serialize across
tests there — see the header of [`.config/nextest.toml`](../../.config/nextest.toml).
Real cross-test serialization comes from a nextest test-group (`max-threads =
1`) or a `-j1` CI lane. Single-node e2e tests should instead be parallel-safe by
construction (random port + isolated temp dir) and need no serialization.

## CI map

`e2e_test` is **excluded** from the main `cargo nextest run --profile ci --all`
pass ([`.github/workflows/ci.yml`](../../.github/workflows/ci.yml) line 158,
`--exclude e2e_test`) — the whole crate is too slow to gate every PR. Subsets
join CI through the nextest profile system only (never as ad-hoc jobs):

| Suite | Runs where | Status |
| --- | --- | --- |
| Smoke subset (`e2e-smoke` profile) | `e2e-tests` job, every PR | **Active** (backlog#1149 ci-4) |
| `s3s-e2e` black-box | `e2e-tests` + `e2e-tests-rio-v2` jobs | **Active** (external conformance tool) |
| ILM / lifecycle (ignored) | `test-ilm-integration-serial` lane, `-j1` | **Active** (backlog#1148 ilm-1) |
| KMS suite | — | Not in CI yet (backlog#1149 ci-5) |
| Protocols (FTPS/WebDAV/SFTP) | — | Not in CI yet (backlog#1149 ci-7) |
| Replication (fast subset) | `e2e-smoke` profile, `e2e-tests` job, every PR | **Active** (backlog#1147 repl-1) |
| Replication (slow + dual-node) | `e2e-repl-nightly` profile, scheduled workflow | **Active** (backlog#1147 repl-1) |
| `reliant/*` (pre-started server) | — | Manual only |

Links: [`ci.yml`](../../.github/workflows/ci.yml) `e2e-tests` (line 347),
`test-ilm-integration-serial` (line 196). The `e2e-smoke` `default-filter` in
[`.config/nextest.toml`](../../.config/nextest.toml) is the **single wiring
mechanism** — extend that filter (or add a sibling profile) to admit more
tests; do not add e2e jobs to `ci.yml`. repl-1 / ilm-3 are landing in parallel
and may add lanes; keep the table above easy to extend.

## Troubleshooting

**Reproduce a CI failure locally** — run the exact profile/lane:

```bash
# Smoke (e2e-tests job) — includes the 20 fast replication tests
cargo nextest run --profile e2e-smoke -p e2e_test
# Replication nightly lane (16 slow + dual-node tests; install awscurl for the
# STS dual-node test, else it skips gracefully)
cargo nextest run --profile e2e-repl-nightly -p e2e_test
# ILM serial lane
cargo nextest run -j1 --run-ignored ignored-only -p rustfs-scanner -p rustfs \
  -E 'binary(lifecycle_integration_test) or (package(rustfs) and test(lifecycle_transition_api_test))'
# s3s-e2e black box
./scripts/e2e-run.sh ./target/debug/rustfs /tmp/rustfs-e2e-data
```

**Stale binary.** Tests build the `rustfs` binary once and reuse it. To avoid
rebuilding while iterating on tests, `common.rs` reuses an existing binary when
running *inside* the e2e test process even if sources changed
(`can_reuse_inside_e2e`, [`src/common.rs`](src/common.rs) line 98). Downside: if
you changed **server** code, force a rebuild with
`cargo build -p rustfs` (or `touch` a source file outside the reuse window)
before re-running, or CI's freshly built artifact will diverge from your local
one.

**Port already in use / orphan processes.** A hard-killed run can leak a
`rustfs` child holding its port. Find and kill it:

```bash
pkill -f 'target/debug/rustfs' ; pkill -f 'target/release/rustfs'
```

The `s3s-e2e` CI job selects a random `RUSTFS_TEST_PORT` (see the `e2e-tests`
job) to dodge this; local single-node tests already use random ports, so a
lingering orphan is usually the cause of a spurious bind failure.

**`awscurl` not found.** `awscurl`-dependent tests skip gracefully with a
visible log line (`awscurl_available()`); install `awscurl` to actually run
them.

## Related

- Crate rules & environment safety: [`AGENTS.md`](AGENTS.md)
- Sub-suite guides: [`src/kms/README.md`](src/kms/README.md),
  [`src/policy/README.md`](src/policy/README.md),
  [`src/protocols/README.md`](src/protocols/README.md),
  [`src/reliant/README.md`](src/reliant/README.md)
- Authoritative per-module counts:
  [`docs/testing/e2e-suite-inventory.md`](../../docs/testing/e2e-suite-inventory.md)
- Test pyramid & flake policy: [`docs/testing/README.md`](../../docs/testing/README.md)

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
