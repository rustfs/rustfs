# RustFS Testing

> **Owner: backlog#1153 (infra-11).** This file is the authoritative home for
> the test-layer taxonomy, naming conventions, and serial/nextest rules. The
> event × budget × required-status matrix is owned separately by
> `docs/testing/ci-gates.md` (backlog#1149 ci-15); this file links to it rather
> than duplicating counts, timeouts, or required-check names.

## Test taxonomy

RustFS layers its tests from cheap-and-narrow to expensive-and-broad. Higher
layers catch what lower layers cannot but cost more wall-clock and setup, so
each layer has a clear entry point and a clear "when". Pick the lowest layer
that can prove your change; add a higher-layer test only when the behaviour is
not observable below it.

| Layer | What it covers | Entry command | When it runs |
|---|---|---|---|
| Unit & crate integration | Per-crate logic and in-process integration tests, run under nextest | `cargo nextest run --all --exclude e2e_test` (or `-p <crate>`) | Every PR (required) |
| ecstore black-box | Erasure-coded read/write/recovery validation of the ecstore stack | `scripts/run_ecstore_validation_suite.sh --profile quick` | Local / release validation (not in CI workflows) |
| e2e (`e2e_test` crate) | Full server spun up per test, driven over the S3 API | `cargo nextest run --profile e2e-smoke -p e2e_test` | PR smoke lane + scheduled full/nightly lanes |
| s3s-e2e conformance | External S3 conformance tool run against a live rustfs server | `./scripts/e2e-run.sh ./target/debug/rustfs /tmp/rustfs-e2e-data` | Per-PR e2e gate (`e2e-tests` job) |
| S3 compatibility | Third-party suites: `ceph/s3-tests` (boto3) and MinIO `mint` (many SDKs) | `scripts/s3-tests/run.sh` (mint: `.github/workflows/mint.yml`) | s3-tests: per-PR gate; mint: scheduled, report-only |
| Chaos / fault-injection | Multi-node, power-loss, and disk-fault harness | — (harness planned) | Planned — tracked in backlog#1100 |
| Fuzz | `cargo-fuzz` targets over untrusted parsing/validation surfaces | `./scripts/fuzz/run.sh` (or `cd fuzz && cargo +nightly fuzz run <target>`) | PR smoke + nightly corpus (`.github/workflows/fuzz.yml`) |
| Benchmarks | Criterion micro/throughput benchmarks | `cargo bench -p <crate>` | On-demand / local |

> The **When it runs** column is a qualitative pointer only. The authoritative
> event × timeout × required-status matrix lives in `docs/testing/ci-gates.md`
> (ci-15) — do not duplicate its numbers here.

Layer notes:

- **Unit & crate integration** — the primary gate. `make test` wraps
  `cargo nextest run --all --exclude e2e_test`; CI runs the same set under the
  strict `ci` profile (`.config/nextest.toml`). Requires `cargo-nextest` (see
  [Serial execution & nextest profiles](#serial-execution--nextest-profiles)).
- **ecstore black-box** — `scripts/run_ecstore_validation_suite.sh` has four
  profiles (`quick` / `full` / `destructive` / `fuzz`); `quick` is the
  PR-smoke-sized core read/write/recovery pass. It is a local and
  release-validation tool, not wired into a CI workflow.
- **e2e** — each test spawns its own single-node rustfs server on a random port
  with an isolated temp dir, so the suite is parallel-safe. The `e2e-smoke`
  profile is the single PR wiring mechanism; slow/cross-process suites run in
  scheduled lanes (`e2e-repl-nightly`, and the reliability group). Full
  contributor guide: [`crates/e2e_test/README.md`](../../crates/e2e_test/README.md);
  per-module counts: [`e2e-suite-inventory.md`](e2e-suite-inventory.md).
- **s3s-e2e** — an external black-box conformance tool installed in CI; locally
  run it against a freshly built binary with `scripts/e2e-run.sh <binary>
  <data-dir>`.
- **S3 compatibility** — `ceph/s3-tests` exercises S3 semantics through boto3
  and is gated on a committed allow-list
  (`scripts/s3-tests/implemented_tests.txt`); MinIO `mint` runs many real
  client SDKs and is report-only until suites pass reliably (see the header of
  `.github/workflows/mint.yml`).
- **Chaos / fault-injection** — multi-node, power-loss, and disk-fault
  scenarios are out of scope for this repo's in-tree suites; the harness is
  tracked in backlog#1100. (Single-node disk-fault e2e tests already live in the
  e2e crate's `e2e-reliability` group.)
- **Fuzz** — the `cargo-fuzz` harness is an isolated sub-workspace under
  `fuzz/` (kept out of the root workspace on purpose); see
  [`fuzz/README.md`](../../fuzz/README.md) for targets and corpus rules.
- **Benchmarks** — Criterion benches live under each crate's `benches/`. They
  are not a gate; run them locally to compare before/after on a specific crate.

### Security advisory regression tests

Fixed GHSA advisories map to named, discoverable regression tests. The
advisory -> test map lives in
[`docs/testing/security-regressions.md`](security-regressions.md). sec-14
(backlog#1151) formalizes the written admission policy in `AGENTS.md`.

## Naming conventions

### Reserved test-name substrings (migration gate)

The migration-critical CI gate selects tests **by name substring** rather than
by module path, so a rename that drops the substring silently thins the gate.
These substrings are therefore reserved: keep them in the test name when a test
proves migration-critical behaviour.

| Substring | Guards |
|---|---|
| `data_movement` | Cross-pool / cross-set object data-movement proofs |
| `rebalance` | Pool rebalance correctness |
| `decommission` | Pool decommission correctness |
| `source_cleanup` | Post-migration source cleanup |
| `delete_marker` | Delete-marker handling across migration |

The gate is enforced by
[`scripts/check_migration_gate_count.sh`](../../scripts/check_migration_gate_count.sh),
which counts the selected tests and fails if the count drops below the committed
floor in
[`.config/migration-gate-floor.txt`](../../.config/migration-gate-floor.txt).
A deliberate reduction must lower the floor in the same PR, so the change is
reviewable in the diff (backlog#1153 infra-12). The substring list above is the
same one the gate uses; keep the two in sync when adding a reserved word.

### General naming

- Name a regression test after what it pins: the issue or advisory number
  (`..._regression_test`, `..._issue_NNNN_...`) or the invariant it protects, so
  a reviewer can find the guard for a past bug by grepping.
- e2e lane membership is driven by test-name patterns in `.config/nextest.toml`
  (for example the `_real_dual_node` / `_real_single_node` markers route
  replication tests into the nightly lane). Follow the existing marker when
  adding a test to an established suite; see
  [`crates/e2e_test/README.md`](../../crates/e2e_test/README.md).
- Follow the Rust API Guidelines for symbol naming (see `AGENTS.md`).

## Serial execution & nextest profiles

**`cargo-nextest` is the runner.** `make test` requires it and CI installs it;
plain `cargo test` is not a faithful substitute because the two runners execute
tests differently.

- **Install:** `cargo install cargo-nextest --locked`, or a prebuilt binary
  (faster) from <https://nexte.st/docs/installation/>.
- **Escape hatch:** `RUSTFS_ALLOW_CARGO_TEST_FALLBACK=1 make test` runs the
  plain `cargo test` fallback, but the results are **not authoritative** —
  serialization semantics differ from CI and `[test-groups]` do not apply.

**Why `#[serial]` is mostly a no-op under nextest.** nextest runs every test in
its own process, so `serial_test`'s in-process `#[serial]` mutex does **not**
serialize tests against each other. The mechanism that actually serializes
across nextest's process boundary is a nextest `[test-groups]` entry with
`max-threads = 1` (for example `ecstore-serial-flaky` and `e2e-reliability` in
`.config/nextest.toml`). Consequences:

- Do not add `#[serial]` expecting cross-test isolation under nextest. If two
  tests genuinely share process/global state or a fixed external resource,
  serialize them with a `[test-groups]` entry, or make each test self-isolating
  (per-test instance context — see backlog#1153 infra-7 / infra-8).
- A large share of the repo's existing `#[serial]` markers only affect the
  `cargo test` fallback. The serial-debt census and removal plan are tracked in
  backlog#1153 infra-7.

**Profiles** (all defined in `.config/nextest.toml`):

- `default` — local runs. **Never retries**: a red test locally is a real
  failure to investigate, not noise to retry away.
- `ci` — the strict CI gate: global `retries = 0` plus a narrowly-scoped
  quarantine list (`retries = 2`) for tests with a tracked OPEN flake issue.
- `e2e-smoke` — the PR smoke subset of the `e2e_test` crate (the single wiring
  mechanism for e2e in PR CI).
- `e2e-repl-nightly` — the scheduled slow/cross-process replication lane.

### Time control (paused vs real clock)

Time-driven tests should prefer paused time (`tokio::time` with `start_paused`
and `advance`) or explicit event synchronization over fixed `sleep` race
windows. The written convention and the `docs/testing/time-control.md` guide are
added by backlog#1153 infra-4.

## Coverage

Line coverage is measured **weekly, not per-PR**, and is non-blocking: it
exists for visibility and trend, never as a required check. Per-crate ratchets
for the security-critical crates (iam / kms / policy / crypto) build on this
baseline later (backlog#1153 infra-6, report-only first).

- **CI**: `.github/workflows/coverage.yml` runs every Sunday and on manual
  dispatch: `cargo llvm-cov nextest --workspace --exclude e2e_test` under the
  `ci` nextest profile — the same scope and profile as the PR test gate. The
  per-crate line-coverage table lands in the run's job summary; the lcov +
  JSON exports are uploaded as a `coverage-lcov-<run>` artifact kept for
  90 days. Scheduled failures open/append the `[scheduled-failure] coverage`
  issue via the shared alert action (ci-8).
- **Local**: `make coverage` is the equivalent (slow — instrumented rebuild
  plus the full suite). It prints the same per-crate table via
  `scripts/coverage_per_crate.py` and writes `target/llvm-cov/lcov.info` and
  `coverage.json`.
- **Trend comparison**: each run's job summary is the weekly per-crate
  snapshot — open two runs from the Actions history (workflow "coverage") and
  compare their tables. For line-level diffs, download the two runs'
  `coverage-lcov-*` artifacts and compare the `lcov.info` files with your lcov
  tooling of choice.
- **Not measured**: doctests (ci.yml runs them uninstrumented; covering them
  would require a nightly toolchain) and the `e2e_test` crate (excluded from
  the unit gate; its lanes are described in the taxonomy above).

## Flake policy

A flaky test is one that fails non-deterministically without a corresponding
code change. Flakes erode trust in the gate and block tightening required
checks, so they are handled on a strict, time-boxed loop.

**Retry semantics (source of truth: `.config/nextest.toml`):**

- The **local `default` profile never retries.** A red test on your machine is
  a real failure to investigate, not noise to paper over.
- The **CI `ci` profile runs with global `retries = 0`.** A new race must fail
  on its first occurrence so the first crime scene is never masked.
- Only tests on the **quarantine list** get `retries = 2`, and only under the
  `ci` profile. Each quarantine entry MUST link exactly one OPEN issue.
- **JUnit flaky markers are the observable.** A quarantined test that passes
  only after a retry is marked `flaky` in `target/nextest/ci/junit.xml`
  (uploaded as a CI artifact). That marker — not a green check — is how we see
  a flake is still live.

**Lifecycle of a flake:**

1. **Discover** — a test fails non-deterministically (CI or local), or shows a
   `flaky` marker in the JUnit report.
2. **Open an issue within 24h** — file/track an issue describing the flake
   (symptom, suspected cause, affected suite). No silent re-runs.
3. **Quarantine** — add the test to the quarantine override block in
   `.config/nextest.toml` with a comment linking that OPEN issue. This grants
   `retries = 2` under CI so the flake stops reddening unrelated PRs, while the
   `flaky` marker keeps it visible.
4. **Fix or delete within 30 days** — make the test robust (then remove the
   quarantine entry) or delete the test. A quarantine entry may not outlive its
   fix window; an entry without a live OPEN issue link is a policy violation.

First quarantine members: the two backlog#937 ecstore groups
(`concurrent_resend_same_part_commits_one_generation` and
`store::bucket::tests::bucket_delete_*`).
