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
| PR touching `paths` in `e2e-upgrade.yml` | `Direct upgrade from the previous release`, `Mixed-version rolling upgrade from the previous release`, `Bucket configuration survives the upgrade`, `Rollback reads current bucket metadata` | `e2e-upgrade.yml` `upgrade` matrix | Report-only | the `cargo test --locked -p e2e_test` command in the job with `RUSTFS_UPGRADE_SOURCE_BINARY` pointing at the pinned previous release (`UPGRADE_SOURCE_VERSION`) |
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
| `e2e-distributed.yml` (storage-sensitive PRs + nightly) | `distributed` | fail-closed 4-node 4-disk S3, durability, replication, movement, fault, and direct/rolling upgrade gate; JUnit, membership listing, per-node server logs | yes, with `never_ran_grace_until` | download the pinned previous release as in the workflow, export `RUSTFS_UPGRADE_SOURCE_BINARY`, then `cargo nextest run --profile e2e-distributed -p e2e_test` |
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
| `e2e-upgrade.yml` (weekly) | `upgrade` (4-case matrix) | upgrade and rollback gate; server logs | no | see the PR row |
| `oidc-keycloak.yml` (weekly) | `oidc-keycloak-live` | live OIDC gate | no | see the PR row |
| `targets-integration.yml` (nightly) | `targets-live` | live target gate; container logs | no | see the PR row |
| `scheduled-validation-freshness.yml` (nightly) | `check-freshness` | fails on missing or stale attempts or completed successes | n/a | dispatch |

Manual `workflow_dispatch` runs are debugging evidence and do not open scheduled-failure issues. A manual performance run may explicitly allow a known regression; that override is not a passing baseline.

## Packaged functional acceptance

`rustfs-functional-chain.yml` dispatches the packaged-build suites in `rustfs-*-test.yml` on the shared lab runners. A failing suite step or job must fail its workflow. Report collection, cleanup, and dispatch of the next suite can still run with `always()`; continuing diagnostics does not make the failed suite successful.

Workflow status preserves errors that the test scripts report. It does not establish complete execution or a common package identity across the chain: inspect the current run's case results, package identity, and test-script revision as well. A script that returns zero after a failed tool invocation needs its own result check.

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

The manifest records a minimum set of invariants: write quorum, metadata rollback, stale-writer lock loss, plaintext Range content, multipart cancellation, hiding uncommitted LIST versions, real MinIO metadata, corrupt part arrays, and the shared on-demand-migration source-backend contract for each provider dialect (S3, Azure, native GCS). The three contract entries live in the `rustfs` suite and reach the lane through that package's default features, so dropping `gcs` from `rustfs`'s defaults fails this check instead of silently deselecting the GCS contract (rustfs/backlog#2323). Renaming or moving a required test must update the manifest in the same change after checking the compiled listing. Extend this list as new deterministic regressions land; it is not a claim that all storage invariants are covered.

The checked-in MinIO corpus is pinned by file SHA256 and its documented source release. The static wiring guard and the CI selection check both reject missing or changed fixtures. These are metadata fixtures, not a legacy shard-body corpus or proof of crash durability. Optional `legacy_bitrot_read_test` runs may still skip when their external corpus is absent; they do not satisfy a required compatibility lane. Real encrypted fixture reads remain in `minio-interop.yml`, and multi-node fault schedules remain in the existing nightly cluster lane. In-process reopen tests do not establish power-loss durability.

Run `python3 scripts/check_test_wiring.py --self-test` to exercise the negative cases: removed/ignored/filtered tests, malformed listing, absent fixtures, and wrong fixture hashes. Do not update hashes merely to silence the guard; a fixture change needs source/provenance and compatibility review.
## Scanner/Heal Evidence Receipts

The existing `scripts/check_test_wiring.py` also validates Scanner/Heal case
evidence registered in `.config/scanner-heal-required-tests.json`. It records
already-built binaries and checks existing nextest output; it does not build,
run tests, deploy servers, inject faults, or start another CI lane.

The registered cases are emitted by existing E2E tests. The original
`background-target-restart` / `background-target-crash` cases run in
`e2e-nightly` on a four-node, one-drive-per-node topology. The
`ec84-target-drive-restart` case runs in `e2e-distributed` on a three-node,
four-drive EC8+4 topology. When `RUSTFS_SCANNER_HEAL_RUN_DIR` is set, the
producer checks the actual server and test-executable hashes against `run.json`,
pins the same server binary for all node starts, and writes its oracle only
after the real assertions pass. The artifact contains the actual pre/post target
PIDs, per-node S3 listings, expected and downloaded complete-body hashes/lengths,
and target-disk `VersionShardCensus` fingerprints. Existing baseline objects
must match their pre-fault physical manifests; the object created during the
outage has no pre-fault target shard and is checked for complete physical parts
and exact S3 content.

These cases are still restart-focused evidence slices. They are not power-loss
validation, an all-version inventory, or proof of scanner enumeration, exact MRF
disposition, legacy migration, multi-pool/multi-set release coverage, or
rollback.
The schema 2 registry separates the implemented single-set restart lane from
structured release lanes for authority coverage, checkpoint/crash, status and
outcome, MRF responsibility, mixed-version rollback, scheduler pressure,
maintenance producers, and EC8+4 multi-set coverage. All G01-G14/P1-P4 and
R-E/R-D/R-L release requirements stay `pending` until their actual
feature-specific oracles, measurements and required topologies exist. Missing
cases cannot be supplied by synthetic W20 results. W20's bounded JSON and
file-hash helpers are reused; its ABBA performance contracts remain in
`docs/operations/scanner-benchmark-runbook.md`.
Measured ABBA manifests must also carry the runbook's `release_evidence`
contract. The runner rejects reports that cannot bind the exact 3x4 EC8+4
topology, multi-pool/multi-set shape, distributed same-window metrics endpoints,
restart/crash modes, mixed-version reader/writer/rollback participation, and
allocation/flamegraph/RSS/save-frequency profile artifact plan. Synthetic runs
and manifests missing that contract remain harness-only evidence.

### Recording One Case

Use a committed source tree, independently built current binaries, sufficient
free disk space, and a task-owned artifact directory that does not yet exist.
Set `SERVER_BINARY` and `TEST_BINARY` to those exact executable paths. The begin
command requires the server's embedded `--version` commit to match the clean
checkout and its embedded Git status to be clean. The E2E crate's build script
embeds its build-time Git revision/dirty state, lockfile Git blob, enabled crate
features, target, profile and encoded Rust flags. It tracks the crate/dependency
trees, Cargo inputs and Git HEAD/ref/index, including `common.rs` restart logic.
The producer checks this compiled identity against the receipt; it does not
copy a current source revision into an older test binary's identity. The E2E
uses its existing temporary cluster directories and cleanup. `CARGO_TARGET_DIR`
controls compilation output; nextest's default report store remains the
workspace's `target/nextest`. Prefer the registry-aware runner for concrete
cases:

```bash
scripts/run_scanner_heal_evidence_case.sh --case background-target-restart
scripts/run_scanner_heal_evidence_case.sh --case ec84-target-drive-restart
```

Set `RUSTFS_E2E_EXPECTED_FEATURES` to the actual intended e2e crate feature set,
including `default` for a default-feature build, comma-separated for extra
features, or empty for `--no-default-features`. It is mandatory when beginning
a run. Crate features are distinct from the spawned server's build features.

Do not replace a nonzero command exit with zero. Missing JUnit or an oracle
emission failure also fails acceptance. Each retry needs a new run directory;
the producer refuses to overwrite an existing oracle. Keep failed-run logs and
artifacts. The receipt pins source revision, actual binary hashes, run identity,
start/finish times, and the artifact hashes. `listing.json`, `junit.xml`, and
each oracle are limited to 1 MiB; object evidence has the fixture's 9..65 object
bound. Credentials are not included in the receipt.

The checker binds nextest's flattened suite `binary-id`/`binary-path` to the
actual test executable and requires the JUnit testcase's embedded execution
timestamp to fall inside the receipt window (with millisecond precision).
Copying an old JUnit file and refreshing its mtime does not make it new evidence.
Schema versions, topology counts, PIDs, EC geometry and shard indices require
actual integers: booleans and fractional values are rejected, and an index must
fit the physical data-plus-parity geometry.

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
these. The external `rustfs/auto-testing` functional workflows propagate suite
failures. Their workflow status does not establish this registry's required
case coverage, build provenance, or object-level oracles.

For automation, `--check-scanner-heal-release "$RUN_DIR"` emits one compact
JSON decision and exits nonzero while blocked. `verified_cases` contains only
cases that pass the complete receipt, build provenance, nextest/JUnit and real
oracle checks; `rejected_cases` names registered cases that do not, and
`pending_gates` names the unimplemented release requirements and
`pending_lanes` names the structured release lanes that still need real
evidence. Schema 1 is deliberately marked `release_schema_capable: false`
because it models only the single-version, unversioned-object restart/crash
cases. Schema 2 can describe the wider release matrix, but approval still
requires every registered case to verify and every required gate to leave
`pending` only after a future checker can bind it to real feature-specific
evidence. The current checker hard-rejects missing structured requirements and
pending gates mapped to an implemented lane, so clearing pending text cannot
become approval. A focused run, synthetic harness, compile-only result,
skipped/retried test, ordinary CI success, or unregistered mixed-version,
rollback, EC8+4 or performance claim therefore cannot become a release approval.
For high-risk rollback gates, `evidence_fields` records the specific proof
fields that a future real-evidence checker must bind before a pending gate can
move out of the blocked set. G03 keeps scoped ACK tied to durable root
publication, ACK request identity, participating peer capability snapshots, and
mixed-peer fallback oracles; G09 keeps mixed-version reader, writer, and rollback
payload evidence explicit. These fields are part of the release contract, not
evidence by themselves.

The upgrade compatibility E2E can emit raw G09 JSON artifacts when
`RUSTFS_SCANNER_HEAL_G09_EVIDENCE_DIR` points at a fresh, task-owned directory.
The rolling mixed-version test writes `G09-mixed_version_reader_evidence.json`
and `G09-mixed_version_writer_evidence.json` after the old/new reader and writer
assertions pass. The bucket-metadata rollback test writes
`G09-rollback_payload_evidence.json` after the current -> previous -> current
round trip has read back the known bucket configuration and objects. These
artifacts are measured inputs for a later release bundle; the bundle must still
record their relative paths, hashes, command provenance, timestamps, roles,
participating revisions, and case lists before
`--check-scanner-heal-release-bundle` can validate them.

For a release-candidate or PR-head Linux x86_64 host, run the full raw G09
artifact pass with:

```bash
scripts/run_scanner_heal_g09_upgrade_evidence.sh
```

The script mirrors the pinned previous-release asset used by the upgrade
workflow, builds the current checkout, runs the mixed-version and rollback E2E
lanes, and fails unless all three raw G09 artifacts are measured, revision-bound,
and role-bound. Use `--source-binary` for a custom previous-release binary on
another platform, or `--test mixed-version|rollback` while narrowing a failure.
It performs a free-space preflight before building so a saturated validation
host fails before producing partial evidence.

The W16 recovery-intent and quota-authority lanes can emit raw G04/G12 JSON
artifacts with:

```bash
scripts/run_scanner_heal_w16_recovery_evidence.sh
```

The runner builds the current checkout, runs the scanner recovery-intent and
disabled-startup crash-boundary tests, runs the scanner quota reset-preservation
tests, and runs the distributed hard-quota admission E2E. A full run writes
`release-bundle-w16.json` and validates the G04 and G12 gates with
`--check-scanner-heal-release-bundle-gate`. Use `--test g04|g12` while narrowing
a failure; a single gate descriptor still does not approve the complete release
bundle.

The W13 durable MRF replay lanes can emit raw G07/G08/P4 JSON artifacts with:

```bash
scripts/run_scanner_heal_w13_mrf_evidence.sh
```

The runner builds the current checkout, runs the ignored MRF evidence test, and
writes `release-bundle-w13.json` for `--check-scanner-heal-release-bundle-gate`.
Use `--test g07|g08|p4` while narrowing a failure. G08 disk-full evidence must
run against a real fillable filesystem: on Linux as root the runner mounts a
small tmpfs automatically, otherwise pass `--enospc-root` pointing at a
pre-mounted small filesystem. P4 is release evidence only when it completes the
default two-hour soak; `--allow-short-soak` is diagnostic and skips P4 bundle
gate validation.

When the real release lanes have produced their dedicated artifacts, validate
the complete hard-gate bundle with:

```bash
scripts/python_bin.sh scripts/check_test_wiring.py \
  --check-scanner-heal-release-bundle /path/to/release-evidence.json
```

The bundle checker is intentionally stricter than the case checker. It requires
schema 2 registry metadata, `evidence: measured`, the current checkout revision,
all G01-G14/P1-P4/R-E/R-D/R-L gates, per-gate `status: pass`, lane identity,
relative artifact paths, matching SHA256 hashes, and non-empty summaries. It
also binds each evidence field to its own run provenance: `source_revision`,
`run_id`, `measurement_window_id`, timezone-qualified `started_at` and
`finished_at`, command arguments, and artifact format. The field
`source_revision` must match the bundle revision, and measured performance
duration cannot exceed the recorded run window.
When an evidence or profile artifact declares a JSON format, the checker also
opens that artifact and requires its payload to repeat the same measured
`source_revision`, `run_id`, `measurement_window_id`, gate and field identity;
profile sub-artifacts must additionally name their artifact kind. Updating only
the outer bundle hash cannot turn a stale JSON summary into current release
evidence.

The hard evidence shape remains claim-specific: mixed-version gates must name at
least two participating versions, crash/durable replay gates must include
crash-boundary evidence, G14 must record EC8+4 with at least three nodes and four
drives per node plus multi-set and multi-pool evidence, performance gates need
measured durations, P3's pressure run needs at least two hours, and P1 needs a
symbolized profile summary with resolved samples. Every G14 field and every
performance gate's fields must also share one `measurement_window_id`, so EC8+4,
multi-set/multi-pool, ABBA, throughput, and profiling artifacts cannot be
stitched together from unrelated runs. P1 `profile_evidence` must bind every
required profile artifact kind (`allocation-profile`, `flamegraph`,
`rss-samples`, and `save-frequency`) with a relative path, artifact format,
non-empty file, matching SHA256, and descriptor-level `source_revision`,
`run_id`, and `measurement_window_id` values that match the parent profile
evidence. Missing, synthetic, stale, tampered, undersized, cross-run, or
topology-mismatched evidence returns a compact blocked or invalid JSON result and
a nonzero exit.

The scheduler-pressure lane must also carry the numbers needed to close W09,
W10, and W11: bounded deferred item/byte/age limits, zero duplicate tasks,
pressure pacing engagement, recovery and lock-hold timings, fixed offered load,
foreground p95/p99 latency, throughput, error count, attempt-cost samples, and
completed heal object counts.

This command validates the evidence package; it does not create evidence. A
handwritten JSON file, a synthetic harness pass, a single focused case, or a
local unit fixture still cannot satisfy the distributed, mixed-version,
crash-restart, durable MRF replay, EC8+4, ABBA, or profiling gates.

Run parser/receipt regressions with
`scripts/python_bin.sh scripts/check_test_wiring.py --self-test`. Those fixtures
validate the checker only and produce no runtime or performance evidence.

For local bundle-shape dry runs, generate a task-owned fixture directory with:

```bash
scripts/python_bin.sh scripts/check_test_wiring.py \
  --write-scanner-heal-release-bundle-fixture /path/to/fixture-dir
```

The generated file is marked `fixture_only` and is intentionally rejected by the
release bundle checker. Use it to rehearse field names, artifact paths, hashes,
profile artifact membership, mixed-version roles, and same-window provenance
before copying the shape into a real measured bundle. It is not ABBA, profile,
mixed-version, crash-restart, or release approval evidence.
