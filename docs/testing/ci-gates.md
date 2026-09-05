# CI gate matrix

**Use this when:** a check is red and you need to know whether it blocks the merge, which workflow and job produced it, and how to reproduce it locally.
**Source of truth:** the live `main` ruleset (command below) for required status; `.github/workflows/<file>.yml` for triggers, `paths`, `timeout-minutes`, and cron; `.config/nextest.toml` for e2e profile filters; `.github/scheduled-validations.json` for the freshness-watchdog list.

A job blocks a merge only when its exact check name is in the live `main` ruleset. A workflow name, a `merge_group` trigger, or a red PR check does not make a job required by itself.

## Required merge checks

The `main` ruleset (`6436880`) requires exactly these contexts, with `strict_required_status_checks_policy=false`:

| Required context | Producer | Validation |
|---|---|---|
| `CLA Check` | `cla.yml` | Contributor agreement |
| `Quick Checks` | `ci.yml` job `quick-checks` | Formatting and repository guard scripts |
| `Test and Lint` | `ci.yml` job `test-and-lint` | Clippy, workspace nextest (`ci` profile, excluding `e2e_test`), doctests, migration-gate count (`scripts/check_migration_gate_count.sh`) |

For PRs limited to the `paths-ignore` list in `ci.yml`, `ci-docs-only.yml` reports `Quick Checks` and `Test and Lint` under the same names; it runs the quick checks and `scripts/check_no_planning_docs.sh`, not a Rust build or tests. `scripts/check_ci_paths_sync.sh` keeps the two path lists aligned.

Verify the live rule before changing merge policy:

```bash
gh api repos/rustfs/rustfs/rulesets/6436880 \
  --jq '.rules[] | select(.type == "required_status_checks") | .parameters'
```

Promotion rule: never promote a report-only lane to required from one green run. Require at least 14 days and 30 representative PRs with at least 99% complete execution, then update the ruleset and this file together.

## Pull request and merge matrix

"Report-only" means visible and actionable but not in the required list. Budgets are each job's `timeout-minutes` in the named workflow and are not copied here.

| Event | Check name | Workflow / job | Merge status | Reproduce |
|---|---|---|---|---|
| PR, non-doc change | `Quick Checks` | `ci.yml` `quick-checks` | Required | `make pre-commit` |
| PR, non-doc change | `Test and Lint` | `ci.yml` `test-and-lint` | Required | `cargo clippy --all-targets -- -D warnings`; `cargo nextest run --profile ci --all --exclude e2e_test`; `cargo test --all --doc`; `scripts/check_migration_gate_count.sh` |
| PR, non-doc change | `Typos` | `ci.yml` `typos` | Report-only | `typos` |
| PR, non-doc change | `ILM Integration (serial)` | `ci.yml` `test-ilm-integration-serial` | Report-only | exact command in the job |
| PR, non-doc change | `Test and Lint (rio-v2)`, `Test and Lint (swift)`, `Test and Lint (sftp)` | `ci.yml` `test-and-lint-rio-v2`, `test-and-lint-protocols` | Report-only | `cargo nextest run` with the job's `--features` |
| PR, non-doc change | `Connect Short Credential Boundary` | `ci.yml` `connect-short-credential-boundary` | Report-only | `cargo test -p rustfs --test connect_registration --features connect-e2e-short-credentials`; `cargo check -p rustfs --release --features connect-e2e-short-credentials` must fail |
| PR, non-doc change | `Build RustFS Debug Binary` | `ci.yml` `build-rustfs-debug-binary` | Report-only; prerequisite for the black-box jobs | `cargo build -p rustfs --bins` |
| PR, non-doc change | `io_uring Integration (real)` | `ci.yml` `uring-integration` | Report-only | `cargo test -p rustfs-ecstore --lib uring_ -- --test-threads=1 --nocapture` |
| PR, non-doc change | `End-to-End Tests` | `ci.yml` `e2e-tests` | Report-only | `cargo nextest run --profile e2e-smoke -p e2e_test`, then `./scripts/e2e-run.sh ./target/debug/rustfs <data-dir>`; membership guards `scripts/check_test_wiring.py --check-profile e2e-smoke <listing.json>` and `scripts/check_security_smoke_count.sh check <listing.json>` |
| PR, non-doc change | `S3 Implemented Tests` | `ci.yml` `s3-implemented-tests` | Report-only | build `rustfs`, then `scripts/s3-tests/run.sh` with the job's `DEPLOY_MODE` / `TEST_MODE` / `MAXFAIL` env |
| PR, non-doc change | `S3 Lifecycle Behavior Tests` | `ci.yml` `s3-lifecycle-behavior-tests` | Report-only | `scripts/s3-tests/run.sh` with the job's accelerated-scanner env |
| PR touching `paths` in `audit.yml` | `Cargo Deny`, `Workflow Pin Report`, `Dependency Review` | `audit.yml` `cargo-deny`, `workflow-pin-report`, `dependency-review` | Report-only | `cargo deny check`; `scripts/security/check_workflow_pins.sh` |
| PR touching `paths` in `architecture-migration-rules.yml` | `Architecture Migration Rules` | `architecture-migration-rules.yml` `architecture-migration-rules` | Report-only | `scripts/check_architecture_migration_rules.sh` |
| PR touching `paths` in `nix.yml` | `Nix Build & Check` | `nix.yml` `nix-validation` | Report-only | `nix flake check` |
| PR touching `paths` in `fuzz.yml` | `Build Fuzz Harness`, `Smoke / <target>` | `fuzz.yml` `fuzz-build`, `pr-fuzz-smoke` | Report-only | `MAX_TOTAL_TIME=60 ./scripts/fuzz/run.sh` |
| PR touching `paths` in `windows-filesystem.yml` | `Rename Safety` | `windows-filesystem.yml` `rename-safety` | Report-only | the `cargo test -p rustfs-ecstore --lib <filter>` commands in the job, on Windows |
| PR touching `paths` in `coverage.yml` | `Workspace line coverage` | `coverage.yml` `coverage` | Report-only | `make coverage`; `python3 scripts/check_security_coverage.py target/llvm-cov/coverage.json` |
| PR touching `paths` in `e2e-upgrade.yml` | `Direct upgrade from rc.2` | `e2e-upgrade.yml` `direct-upgrade` | Report-only | the `cargo test --locked -p e2e_test` command in the job with `RUSTFS_UPGRADE_SOURCE_BINARY` pointing at the pinned previous release |
| PR touching `paths` in `oidc-keycloak.yml` | `OIDC Keycloak live gate` | `oidc-keycloak.yml` `oidc-keycloak-live` | Report-only | `cargo build --locked -p rustfs --bin rustfs`, then `bash scripts/test/oidc_keycloak_live.sh ./target/debug/rustfs` |
| PR touching `paths` in `targets-integration.yml` | `PostgreSQL, MySQL, AMQP, and NATS` | `targets-integration.yml` `targets-live` | Report-only | start the containers as in the job, export the `RUSTFS_TEST_*` DSNs, then the job's `cargo test --locked -p rustfs-targets --test <name> -- --ignored --test-threads=1` commands |
| PR limited to main-CI-excluded paths | `Quick Checks`, `Test and Lint` | `ci-docs-only.yml` `quick-checks`, `test-and-lint` | Required | `git diff --check`; `make doc-paths-check`; `scripts/check_no_planning_docs.sh` |
| `merge_group`; push to `main` | `End-to-End Tests (full merge gate)` | `ci.yml` `e2e-full` | Report-only | `cargo nextest run --profile e2e-full -p e2e_test` |

e2e filters live in `.config/nextest.toml`; extend a profile instead of adding a second selector. Before a profile runs, `scripts/check_test_wiring.py` compares its listing to the committed digest in `.config/e2e-<profile>-selection.txt`, so a silent test drop fails closed.

Scanner usage and heal rebuild coverage are intentionally split by risk and
cost. `data_usage_test` runs in the PR `e2e-smoke` lane so changes that affect
authoritative scanner usage publication, quota-visible usage, or admin usage
snapshots get an end-to-end signal before merge review. `heal_erasure_disk_rebuild_test`
runs in `e2e-full` so core erasure heal rebuild regressions are caught no later
than the merge queue or `main` push lane; it also remains in `e2e-nightly` with
the serialized cluster fault-domain suites for scheduled soak signal.

## Scheduled validation

Scheduled lanes never block a PR. Their workflow-local gate fails the run, scheduled failures route to the shared failure-issue action, and `scheduled-validation-freshness.yml` fails when a workflow listed in `.github/scheduled-validations.json` has not run within its `max_age_hours` (a `never_ran_grace_until` entry covers the window before a newly enabled cron's first slot). Cadence is qualitative here; the cron lives in each workflow's `on.schedule`.

| Workflow (cadence) | Jobs | Verdict and artifacts | In freshness list | Reproduce |
|---|---|---|---|---|
| `ci.yml` (weekly) | full matrix, including the schedule/dispatch-only rio-v2 jobs `build-rustfs-debug-binary-rio-v2` and `e2e-tests-rio-v2` | per-job | yes | dispatch `ci.yml` |
| `build.yml` (weekly) | `build-rustfs` over the six-target platform matrix in `prepare-platform-matrix` (four Linux, macOS aarch64, Windows x86_64) | build/package integrity | yes | dispatch `build.yml` with an exact platform set |
| `e2e-replication-nightly.yml` (nightly) | `repl-nightly`, `cluster-nightly`, `protocols-nightly` | three independent gates; JUnit, membership listing, server logs | yes | `cargo nextest run --profile e2e-repl-nightly -p e2e_test`; `--profile e2e-nightly`; `-j 1 --profile e2e-protocols` |
| `e2e-s3tests.yml` (weekly) | `s3tests` (single and distributed, four shards each), `upstream-head-canary` | compatibility gate; report, JUnit, node IDs, server logs | yes | `scripts/s3-tests/run.sh` against an existing single or distributed target |
| `fuzz.yml` (nightly) | `nightly-fuzz-corpus` per target | gate; corpus and crash artifacts | yes | `MAX_TOTAL_TIME=<seconds> ./scripts/fuzz/run.sh` |
| `minio-interop.yml` (nightly) | `minio-interop` | EC + SSE read-parity gate | yes, with `never_ran_grace_until` | pinned Docker fixture steps in the workflow |
| `on-demand-migration-interop.yml` (nightly) | `minio-source`, `cloud-source` (`aws`, `r2`, `gcs`) | report-only provider interop; one JSON report per provider naming cases, timings and source request counts, plus JUnit and MinIO logs. A cloud provider whose `ODM_INTEROP_*` secrets are absent is skipped with a summary note, not failed | no | start the pinned MinIO container as in the job, export the `RUSTFS_ODM_INTEROP_*` variables, then `cargo nextest run --profile e2e-odm-interop -p e2e_test` |
| `performance-ab.yml` (nightly) | `warp-ab` | regression-budget gate; A/B summaries and server logs | yes | `bash scripts/run_hotpath_warp_abba.sh --help` |
| `nightly-gnu.yml` (nightly) | `build`, `kms-vault-lane`, `kms-vault-ha-failover` | build, live Vault, and HA failover gates | yes | commands and pinned Vault images in the workflow |
| `audit.yml` (nightly) | `cargo-deny`, `workflow-pin-report` | dependency and workflow-pin gates | yes | `cargo deny check`; `scripts/security/check_workflow_pins.sh` |
| `mint.yml` (weekly) | `mint` | report-only by design; per-suite PASS/FAIL/NA and raw `log.json` | yes | pinned Docker sequence in the workflow |
| `coverage.yml` (weekly) | `coverage` | report-only trend; lcov and JSON artifact | yes | `make coverage` |
| `runner-hygiene.yml` (monthly) | `check-ephemerality` | runner ephemerality | yes | dispatch |
| `e2e-upgrade.yml` (weekly) | `direct-upgrade` | upgrade gate; server logs | no | see the PR row |
| `oidc-keycloak.yml` (weekly) | `oidc-keycloak-live` | live OIDC gate | no | see the PR row |
| `targets-integration.yml` (nightly) | `targets-live` | live target gate; container logs | no | see the PR row |
| `scheduled-validation-freshness.yml` (nightly) | `check-freshness` | fails on a never-created or stale schedule | n/a | dispatch |

Manual `workflow_dispatch` runs are debugging evidence and do not open scheduled-failure issues. A manual performance run may explicitly allow a known regression; that override is not a passing baseline.

## Release validation

Post-merge and tag-driven; not a substitute for a PR gate.

| Trigger | Workflow / job | Result |
|---|---|---|
| Push to `main`, weekly schedule, dispatch | `build.yml` `build-rustfs` (a development build on a main push restricts the matrix to the Linux targets) | build artifacts; no release publication |
| Valid release or preview tag | `build.yml` `build-rustfs`, `create-release`, `upload-release-assets`, `publish-release` | draft release, checksummed assets, publish |
| Successful non-preview release-tag build (`workflow_run`) | `docker.yml` `build-docker`, `scan-docker-image` | multi-architecture images and vulnerability report |
| Successful release-tag build (`workflow_run`) | `package.yml` `package` | DEB/RPM packages and checksums uploaded to the release |
| Successful non-preview release-tag build (`workflow_run`) | `helm-package.yml` `build-helm-package`, `publish-helm-package` | versioned chart and repository index |
| Final tag's release published | `build.yml` `cleanup-preview-releases` | deletes every `<target>-preview.<N>` Release for that target; the tags are kept |

Use an exact preview tag for an end-to-end release rehearsal. Manual dispatches are backfill/debug paths and do not prove the automatic `workflow_run` chain.

## Change checklist

Update this file in the same PR when a job or check name changes, a workflow gains or loses a `pull_request` or `schedule` trigger, required contexts or strict/merge-queue policy change, report-only vs gating semantics change, or `.github/scheduled-validations.json` membership changes. Do not copy timeouts, crons, or test counts here.

## ECStore invariant selection

The existing `ci.yml` test-and-lint job runs the ordinary ECStore and filemeta tests. After that run, `scripts/check_test_wiring.py --check-core` checks the same nextest profile and package selection against `.config/ecstore-required-tests.json`. Every named test must exist, match the filter, and be non-ignored; the job also requires a nonempty JUnit report. This checks membership without running the tests twice. `core-test-listing.json`, JUnit, and the run log are retained in the existing test-and-lint artifact.

The manifest records a minimum set of invariants: write quorum, metadata rollback, stale-writer lock loss, plaintext Range content, multipart cancellation, hiding uncommitted LIST versions, real MinIO metadata, and corrupt part arrays. Renaming or moving a required test must update the manifest in the same change after checking the compiled listing. Extend this list as new deterministic regressions land; it is not a claim that all storage invariants are covered.

The checked-in MinIO corpus is pinned by file SHA256 and its documented source release. The static wiring guard and the CI selection check both reject missing or changed fixtures. These are metadata fixtures, not a legacy shard-body corpus or proof of crash durability. Optional `legacy_bitrot_read_test` runs may still skip when their external corpus is absent; they do not satisfy a required compatibility lane. Real encrypted fixture reads remain in `minio-interop.yml`, and multi-node fault schedules remain in the existing nightly cluster lane. In-process reopen tests do not establish power-loss durability.

Run `python3 scripts/check_test_wiring.py --self-test` to exercise the negative cases: removed/ignored/filtered tests, malformed listing, absent fixtures, and wrong fixture hashes. Do not update hashes merely to silence the guard; a fixture change needs source/provenance and compatibility review.
## Scanner/Heal Evidence Receipts

The existing `scripts/check_test_wiring.py` also validates Scanner/Heal case
evidence registered in `.config/scanner-heal-required-tests.json`. It records
already-built binaries and checks existing nextest output; it does not build,
run tests, deploy servers, inject faults, or start another CI lane.

The initial case is `background-target-restart`, emitted by
`heal_erasure_disk_rebuild_test::tests::test_cluster_root_heal_recovers_remote_shards_after_background_target_restart`.
That test already runs in `e2e-nightly`. When `RUSTFS_SCANNER_HEAL_RUN_DIR` is set,
it checks the actual server and test-executable hashes against `run.json`, pins
the same server binary for all node starts, and writes its oracle only after
the real assertions pass. The artifact contains the actual pre/post target
PIDs, per-node S3 listings, expected and downloaded complete-body hashes/lengths,
and target-disk `VersionShardCensus` fingerprints. Existing baseline objects
must match their pre-fault physical manifests; the object created during the
outage has no pre-fault target shard and is checked for complete physical parts
and exact S3 content.

This case is a **four-node, one-drive-per-node process-restart test**. It is not
power-loss validation, a 3x4 EC8+4 experiment, an all-version inventory, or proof
of scanner enumeration, exact MRF disposition, legacy migration, or rollback.
The registry keeps all G01-G14/P1-P4 and R-E/R-D/R-L release requirements pending
until their actual feature-specific oracles and required topologies exist.
Missing cases cannot be supplied by synthetic W20 results. W20's bounded JSON
and file-hash helpers are reused; its ABBA performance contracts remain in
`docs/operations/scanner-benchmark-runbook.md`.

### Recording One Case

Use a committed source tree, independently built current binaries, sufficient
free disk space, and a task-owned artifact directory that does not yet exist.
Set `SERVER_BINARY` and `TEST_BINARY` to those exact executable paths. The begin
command requires the server's embedded `--version` commit to match the clean
checkout and its embedded Git status to be clean. The producer also checks
compile-time hashes of its two oracle source files, so a stale test executable
cannot acquire current oracle semantics just by copying a receipt. The E2E
uses its existing temporary cluster directories and cleanup. With a dedicated
`CARGO_TARGET_DIR`, execute the existing selected case as follows:

```bash
CASE=background-target-restart
FILTER='test(test_cluster_root_heal_recovers_remote_shards_after_background_target_restart)'
RUN_DIR="$PWD/artifacts/scanner-heal-run"
scripts/python_bin.sh scripts/check_test_wiring.py \
  --begin-scanner-heal "$RUN_DIR" "$SERVER_BINARY" "$TEST_BINARY"
export RUSTFS_SCANNER_HEAL_RUN_DIR="$RUN_DIR"
export CARGO_BIN_EXE_rustfs="$SERVER_BINARY"
cargo nextest list --profile e2e-nightly -p e2e_test -E "$FILTER" \
  --message-format json > "$RUN_DIR/listing.json"
rm -f "$CARGO_TARGET_DIR/nextest/e2e-nightly/junit.xml"
set +e
cargo nextest run --profile e2e-nightly -p e2e_test -E "$FILTER"
test_exit=$?
set -e
cp "$CARGO_TARGET_DIR/nextest/e2e-nightly/junit.xml" "$RUN_DIR/junit.xml"
scripts/python_bin.sh scripts/check_test_wiring.py --finish-scanner-heal "$RUN_DIR" "$test_exit"
scripts/python_bin.sh scripts/check_test_wiring.py --check-scanner-heal "$RUN_DIR" "$CASE"
```

Do not replace a nonzero command exit with zero. Missing JUnit or an oracle
emission failure also fails acceptance. Each retry needs a new run directory;
the producer refuses to overwrite an existing oracle. Keep failed-run logs and
artifacts. The receipt pins source revision, actual binary hashes, run identity,
start/finish times, and the artifact hashes. `listing.json`, `junit.xml`, and
each oracle are limited to 1 MiB; object evidence has the fixture's 9..65 object
bound. Credentials are not included in the receipt.

The checker rejects unselected/ignored tests, zero/duplicate JUnit cases,
failures, skipped tests, retry/flaky records, stale or changed artifacts,
different builds or run IDs, unchanged process IDs, wrong topology, missing
shard parts, and mismatched S3 content/listings. The raw oracle JSON is emitted
by the real E2E producer, not accepted from an adapter copying expectations.

`--check-scanner-heal "$RUN_DIR" release` checks available case evidence and
returns nonzero for every pending release requirement. A focused case pass
does not approve release. In particular, R-E requires fixed-budget real
restarts without an unbudgeted final sweep, R-D requires the full
manager/event/ledger disposition chain, and R-L requires source-conflict and
crash/retirement evidence. Reader-only or unit fixtures cannot substitute for
these. The external `rustfs/auto-testing` nightly workflow and its deliberate
continue-on-error policy are not interpreted as a passed release gate.

Run parser/receipt regressions with
`scripts/python_bin.sh scripts/check_test_wiring.py --self-test`. Those fixtures
validate the checker only and produce no runtime or performance evidence.
