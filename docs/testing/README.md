| Outbound target matrix | Replication of every object shape (empty, plain, retention, legal hold, multipart) against every remote-target failure mode the fake target models; an explicit expectation table pins known-red cells to an open issue | `cargo nextest run -p e2e_test -E 'test(/^replication_target_matrix_test::/)'` (build `target/debug/rustfs` first) | With `e2e-repl-nightly`; required locally for any change to outbound client defaults (SOP: [`docs/postmortems/2026-09-03-replication-checksum-default-regression.md`](../postmortems/2026-09-03-replication-checksum-default-regression.md)) |
# RustFS Testing

**Use this when:** you need to pick a test layer for a change, name a test so a gate keeps selecting it, understand why `#[serial]` does nothing under nextest, or handle a flaky test.
**Source of truth:** `.config/nextest.toml` (profiles, test-groups, quarantine), `.config/make/tests.mak` (`make test`), `.github/workflows/*.yml` (what runs when; matrix in [ci-gates.md](ci-gates.md)).

## Test taxonomy

Pick the lowest layer that can prove the change; add a higher-layer test only when the behaviour is not observable below it.

| Layer | What it covers | Entry command | When it runs (details: [ci-gates.md](ci-gates.md)) |
|---|---|---|---|
| Unit & crate integration | Per-crate logic and in-process integration tests | `cargo nextest run --all --exclude e2e_test` (or `-p <crate>`); `make test` wraps it | Every PR, required (`Test and Lint`, `ci` profile) |
| ecstore black-box | Erasure-coded read/write/recovery validation; profiles `quick` / `full` / `destructive` / `fuzz` | `scripts/run_ecstore_validation_suite.sh --profile quick` | Local and release validation only; not wired into any workflow. Contract: [ecstore-validation-suite-design.md](ecstore-validation-suite-design.md) |
| e2e (`e2e_test` crate) | A real `rustfs` binary per test, driven over the S3, admin, and protocol APIs | `cargo nextest run --profile e2e-smoke -p e2e_test` | PR: `e2e-smoke` (report-only); merge queue / main push: `e2e-full`; nightly: `e2e-repl-nightly`, `e2e-nightly`, `e2e-protocols`. Guide: [`crates/e2e_test/README.md`](../../crates/e2e_test/README.md) |
| s3s-e2e conformance | External S3 conformance tool against a live server | `./scripts/e2e-run.sh ./target/debug/rustfs <data-dir>` | PR, report-only (second half of the `End-to-End Tests` job) |
| S3 compatibility | `ceph/s3-tests` (boto3; allow-list `scripts/s3-tests/implemented_tests.txt`) and MinIO `mint` | `scripts/s3-tests/run.sh`; mint via `.github/workflows/mint.yml` | s3-tests: PR report-only plus a weekly full sweep; mint: weekly, report-only |
| Chaos / fault-injection | Single-node disk fault injection (`crates/e2e_test/src/chaos.rs`, `crates/e2e_test/src/fault_proxy.rs`) used by the reliability and heal e2e modules | Part of the e2e crate (`e2e-reliability` test-group) | With the `e2e-full` and nightly e2e lanes. A multi-node power-loss harness is not in tree |
| Fuzz | `cargo-fuzz` targets over untrusted parsing surfaces; isolated sub-workspace under `fuzz/` | `./scripts/fuzz/run.sh` (see [`fuzz/README.md`](../../fuzz/README.md)) | PR smoke on the paths listed in `.github/workflows/fuzz.yml`, plus nightly corpus |
| Benchmarks | Criterion benches under each crate's `benches/` | `cargo bench -p <crate>` | On demand; never a gate |

Every script named above is indexed with status and wiring in [`scripts/README.md`](../../scripts/README.md). Fixed GHSA advisories map to named regression tests in [security-regressions.md](security-regressions.md).

## Naming conventions

### Reserved test-name substrings (migration gate)

`scripts/check_migration_gate_count.sh` (runs in `Test and Lint`) selects migration-critical tests by name substring and fails when the count drops below `.config/migration-gate-floor.txt`. A rename that drops a substring silently thins the gate, so these substrings are reserved:

| Substring | Guards |
|---|---|
| `data_movement` | Cross-pool / cross-set object data-movement proofs |
| `rebalance` | Pool rebalance correctness |
| `decommission` | Pool decommission correctness |
| `source_cleanup` | Post-migration source cleanup |
| `delete_marker` | Delete-marker handling across migration |

A deliberate reduction lowers the floor in the same PR. The list above mirrors the script; change both together.

### General naming

- Name a regression test after what it pins (issue or advisory number, or the invariant) so `rg` finds the guard for a past bug.
- e2e lane membership is selected by test-name patterns in `.config/nextest.toml` (for example `_real_dual_node` / `_real_single_node` route replication tests to the nightly lane). Follow the existing marker of the suite you extend.
- Symbol naming follows the Rust API Guidelines (see `AGENTS.md`).

## nextest and `#[serial]`

`cargo-nextest` is the runner: `make test` requires it and CI installs it. nextest runs every test in its own process, so `serial_test`'s in-process `#[serial]` mutex does **not** serialize tests against each other; it only affects the plain `cargo test` fallback. Cross-test serialization under nextest comes from a `[test-groups]` entry with `max-threads = 1` in `.config/nextest.toml` (for example `ecstore-serial-flaky`, `e2e-reliability`) or from a `-j 1` lane. Prefer making tests self-isolating (per-test instance context, random port, own temp dir) over adding serialization. `RUSTFS_ALLOW_CARGO_TEST_FALLBACK=1 make test` runs plain `cargo test`; its results are not authoritative because `[test-groups]` do not apply.

Time-driven tests use paused tokio time (`start_paused` plus `tokio::time::advance`) or explicit synchronization instead of fixed `sleep` windows.

### Profiles

All profiles are defined in `.config/nextest.toml`; its block comments hold the filters and rationale.

| Profile | Role |
|---|---|
| `default` | Local runs; never retries |
| `ci` | PR gate for everything except `e2e_test`; global `retries = 0` plus the quarantine list |
| `e2e-smoke` | PR subset of `e2e_test` |
| `e2e-full` | Merge-queue / main-push single-node e2e lane |
| `e2e-repl-nightly` | Nightly slow / cross-process replication lane |
| `e2e-nightly` | Nightly serial multi-process cluster fault lane |
| `e2e-protocols` | Nightly fixed-port FTPS/SFTP/WebDAV lane, run with `-j 1` |

Membership of each e2e profile is pinned by a digest in `.config/e2e-<profile>-selection.txt` and checked by `scripts/check_test_wiring.py --check-profile <profile>` before the lane runs. To list what a profile selects on your platform (the result is platform-dependent because some modules are linux-only):

```bash
cargo nextest list -p e2e_test --profile e2e-smoke --message-format json \
  | jq -r '.["rust-suites"][].testcases | to_entries[] | select(.value["filter-match"].status == "matches") | .key | split("::")[0]' \
  | sort | uniq -c
```

## Flake policy

A flaky test fails non-deterministically without a corresponding code change. Retry semantics live in `.config/nextest.toml`: `default` never retries, `ci` has global `retries = 0`, and only quarantined tests get `retries = 2` under `ci`. A quarantined test that passes on retry is marked `flaky` in `target/nextest/ci/junit.xml` (uploaded as a CI artifact); that marker, not a green check, is how a live flake stays visible.

1. **Discover** — a non-deterministic failure (CI or local) or a `flaky` JUnit marker.
2. **Open an issue within 24h** describing symptom, suspected cause, and affected suite. No silent re-runs.
3. **Quarantine** — add a `[[profile.ci.overrides]]` entry with `retries = 2` and a comment linking exactly one OPEN issue. The current quarantine list is the `[[profile.ci.overrides]]` block in `.config/nextest.toml`.
4. **Fix or delete within 30 days** — make the test robust and remove the entry, or delete the test. An entry without a live OPEN issue link is a policy violation.

## Coverage

- `.github/workflows/coverage.yml` measures workspace line coverage on its schedule and on manual dispatch: `cargo llvm-cov nextest --workspace --exclude e2e_test` under the `ci` profile, the same scope as the PR gate. The per-crate table lands in the job summary; lcov and JSON exports are uploaded as an artifact (retention set in the workflow).
- PRs touching the paths listed in `coverage.yml` also run a report-only comparison against `.config/coverage-baselines.toml` via `scripts/check_security_coverage.py`: a regression is recorded in the summary without failing the job; missing or malformed coverage evidence fails closed.
- `make coverage` (`.config/make/coverage.mak`) is the local equivalent; it writes `target/llvm-cov/lcov.info` and `coverage.json` and prints the same table via `scripts/coverage_per_crate.py`.
- Not measured: doctests (`ci.yml` runs them uninstrumented) and the `e2e_test` crate.
- A baseline change needs a linked coverage run and a reviewed explanation in the PR.
