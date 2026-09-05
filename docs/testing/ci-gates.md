# CI gate matrix

**Use this when:** a check is red and you need to know whether it blocks the merge, which workflow and job produced it, and how to reproduce it locally.
**Source of truth:** the live `main` ruleset (command below) for required status; `.github/workflows/<file>.yml` for triggers, `paths`, `timeout-minutes`, and cron; `.config/nextest.toml` for e2e profile filters; `.github/scheduled-validations.json` for the freshness-watchdog list.

A job blocks a merge when its exact check name is required by the live `main` ruleset, or when its result is required by the `Test and Lint` aggregate. A workflow name, a `merge_group` trigger, or an unrelated red PR check does not make a job required by itself.

## Required merge checks

The `main` ruleset (`6436880`) requires exactly these contexts, with `strict_required_status_checks_policy=false`:

| Required context | Producer | Validation |
|---|---|---|
| `CLA Check` | `cla.yml` | Contributor agreement |
| `Quick Checks` | `ci.yml` job `quick-checks` | Formatting and repository guard scripts |
| `Test and Lint` | `ci.yml` job `required-checks` | Exact expected results for every CI validation job, including workspace checks, critical E2E, feature lanes, and event-specific full suites |

Every PR enters `ci.yml`. The `classify-changes` job uses the base revision of `scripts/ci_gate.py` to select a conservative documentation-only path: root Markdown/licenses, `AGENTS.md`, Markdown under `docs/` or `.agents/skills/`, and documentation images. Unknown paths, unavailable Git history, an empty diff, or a missing base policy select the full matrix. Renames include their deleted source path. Documentation-only PRs still run Quick Checks and Typos; the aggregate requires the expensive jobs to be skipped exactly as selected.

`required-checks` runs even after failed or skipped dependencies. `scripts/ci_gate.py verify` rejects missing jobs, unexpected jobs, failure, cancellation, and unexpected skips; optional lanes are required only on their declared events. `Workspace Test and Lint` is the ordinary Rust job, while `Test and Lint` uniquely names the aggregate. New validation jobs must update both its direct dependencies and the script contract. Test this wiring and its failure cases with `python3 scripts/ci_gate.py --self-test`.

Verify the live rule before changing merge policy:

```bash
gh api repos/rustfs/rustfs/rulesets/6436880 \
  --jq '.rules[] | select(.type == "required_status_checks") | .parameters'
```

The aggregate requires the validation lanes already selected by `ci.yml`; this closes the gap where a failing critical lane left the required workspace check green. Independent workflows remain report-only unless separately required. Before adding a new expensive lane or moving existing PR coverage to a schedule, collect representative execution and regression evidence, establish ownership and a working scheduled replacement, and update this reference with the resulting policy.

## Pull request and merge matrix

"Via aggregate" means a wrong result fails the required `Test and Lint` check. "Report-only" means visible and actionable but outside both the required list and aggregate. Budgets are each job's `timeout-minutes` in the named workflow and are not copied here.

| Event | Check name | Workflow / job | Merge status | Reproduce |
|---|---|---|---|---|
| PR, non-doc change | `Quick Checks` | `ci.yml` `quick-checks` | Required | `make pre-commit` |
| PR, non-doc change | `Workspace Test and Lint` | `ci.yml` `test-and-lint` | Via aggregate | `cargo clippy --all-targets -- -D warnings`; `cargo nextest run --profile ci --all --exclude e2e_test`; `cargo test --all --doc`; `scripts/check_migration_gate_count.sh` |
| PR, non-doc change | `Typos` | `ci.yml` `typos` | Via aggregate | `typos` |
| PR, non-doc change | `ILM Integration (serial)` | `ci.yml` `test-ilm-integration-serial` | Via aggregate | exact command in the job |
| PR, non-doc change | `Test and Lint (rio-v2)`, `Test and Lint (swift)`, `Test and Lint (sftp)` | `ci.yml` `test-and-lint-rio-v2`, `test-and-lint-protocols` | Via aggregate | `cargo nextest run` with the job's `--features` |
| PR, non-doc change | `Connect Short Credential Boundary` | `ci.yml` `connect-short-credential-boundary` | Via aggregate | `cargo test -p rustfs --test connect_registration --features connect-e2e-short-credentials`; `cargo check -p rustfs --release --features connect-e2e-short-credentials` must fail |
| PR, non-doc change | `Build RustFS Debug Binary` | `ci.yml` `build-rustfs-debug-binary` | Via aggregate; prerequisite for black-box jobs | `cargo build -p rustfs --bins --features e2e-test-hooks` |
| PR, non-doc change | `io_uring Integration (real)` | `ci.yml` `uring-integration` | Via aggregate | `cargo test -p rustfs-ecstore --lib uring_ -- --test-threads=1 --nocapture` |
| PR, non-doc change | `End-to-End Tests` | `ci.yml` `e2e-tests` | Via aggregate | `cargo nextest run --profile e2e-smoke -p e2e_test`, then `./scripts/e2e-run.sh ./target/debug/rustfs <data-dir>`; membership guards `scripts/check_test_wiring.py --check-profile e2e-smoke <listing.json>` and `scripts/check_security_smoke_count.sh check <listing.json>` |
| PR, non-doc change | `S3 Implemented Tests` | `ci.yml` `s3-implemented-tests` | Via aggregate | build `rustfs`, then `scripts/s3-tests/run.sh` with the job's `DEPLOY_MODE` / `TEST_MODE` / `MAXFAIL` env |
| PR, non-doc change | `S3 Lifecycle Behavior Tests` | `ci.yml` `s3-lifecycle-behavior-tests` | Via aggregate | `scripts/s3-tests/run.sh` with the job's accelerated-scanner env |
| PR touching `paths` in `audit.yml` | `Cargo Deny`, `Workflow Pin Report`, `Dependency Review` | `audit.yml` `cargo-deny`, `workflow-pin-report`, `dependency-review` | Report-only | `cargo deny check`; `scripts/security/check_workflow_pins.sh` |
| PR touching `paths` in `architecture-migration-rules.yml` | `Architecture Migration Rules` | `architecture-migration-rules.yml` `architecture-migration-rules` | Report-only | `scripts/check_architecture_migration_rules.sh` |
| PR touching `paths` in `nix.yml` | `Nix Build & Check` | `nix.yml` `nix-validation` | Report-only | `nix flake check` |
| PR touching `paths` in `fuzz.yml` | `Build Fuzz Harness`, `Smoke / <target>` | `fuzz.yml` `fuzz-build`, `pr-fuzz-smoke` | Report-only | `MAX_TOTAL_TIME=60 ./scripts/fuzz/run.sh` |
| PR touching `paths` in `windows-filesystem.yml` | `Rename Safety` | `windows-filesystem.yml` `rename-safety` | Report-only | the `cargo test -p rustfs-ecstore --lib <filter>` commands in the job, on Windows |
| PR touching `paths` in `coverage.yml` | `Workspace line coverage` | `coverage.yml` `coverage` | Report-only | `make coverage`; `python3 scripts/check_security_coverage.py target/llvm-cov/coverage.json` |
| PR touching `paths` in `e2e-upgrade.yml` | `Direct upgrade from rc.2` | `e2e-upgrade.yml` `direct-upgrade` | Report-only | the `cargo test --locked -p e2e_test` command in the job with `RUSTFS_UPGRADE_SOURCE_BINARY` pointing at the pinned previous release |
| PR touching `paths` in `oidc-keycloak.yml` | `OIDC Keycloak live gate` | `oidc-keycloak.yml` `oidc-keycloak-live` | Report-only | `cargo build --locked -p rustfs --bin rustfs`, then `bash scripts/test/oidc_keycloak_live.sh ./target/debug/rustfs` |
| PR touching `paths` in `targets-integration.yml` | `PostgreSQL, MySQL, AMQP, and NATS` | `targets-integration.yml` `targets-live` | Report-only | start the containers as in the job, export the `RUSTFS_TEST_*` DSNs, then the job's `cargo test --locked -p rustfs-targets --test <name> -- --ignored --test-threads=1` commands |
| PR, documentation-only selection | `Quick Checks`, `Typos`, `Test and Lint` | `ci.yml` `quick-checks`, `typos`, `required-checks` | Required directly or via aggregate | Quick Checks commands; `python3 scripts/ci_gate.py --self-test` |
| `merge_group`; push to `main` | `End-to-End Tests (full merge gate)` | `ci.yml` `e2e-full` | Via aggregate on these events | `cargo nextest run --profile e2e-full -p e2e_test` |

e2e filters live in `.config/nextest.toml`; extend a profile instead of adding a second selector. Before a profile runs, `scripts/check_test_wiring.py` compares its listing to the committed digest in `.config/e2e-<profile>-selection.txt`, so a silent test drop fails closed.

Scanner usage and heal rebuild coverage are intentionally split by risk and
cost. `data_usage_test` runs in the PR `e2e-smoke` lane so changes that affect
authoritative scanner usage publication, quota-visible usage, or admin usage
snapshots get an end-to-end signal before merge review. `heal_erasure_disk_rebuild_test`
runs in `e2e-full` so core erasure heal rebuild regressions are caught no later
than the merge queue or `main` push lane; it also remains in `e2e-nightly` with
the serialized cluster fault-domain suites for scheduled soak signal.

## Scheduled validation

Scheduled lanes never block a PR. Their workflow-local gate fails the run, scheduled failures route to the shared failure-issue action, and `scheduled-validation-freshness.yml` fails when a workflow listed in `.github/scheduled-validations.json` has no recent attempt or completed successful scheduled run within its `max_age_hours` (a `never_ran_grace_until` entry covers the window before a newly enabled cron's first slot). Cadence is qualitative here; the cron lives in each workflow's `on.schedule`.

| Workflow (cadence) | Jobs | Verdict and artifacts | In freshness list | Reproduce |
|---|---|---|---|---|
| `ci.yml` (weekly) | full matrix, including the schedule/dispatch-only rio-v2 jobs `build-rustfs-debug-binary-rio-v2` and `e2e-tests-rio-v2` | strict aggregate; the full E2E lane runs on dispatch, merge groups, and main pushes | yes | dispatch `ci.yml` |
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
| `scheduled-validation-freshness.yml` (nightly) | `check-freshness` | fails on missing or stale attempts or completed successes | n/a | dispatch |

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
