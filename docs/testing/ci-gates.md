# CI gate matrix

This file is the source of truth for which validation runs on each event, its
configured wall-clock budget, and whether it can block a merge. Test taxonomy,
naming, and nextest serialization rules remain in [README.md](README.md); e2e
membership and counts remain in
[e2e-suite-inventory.md](e2e-suite-inventory.md).

The distinction between **required** and **report-only** is load-bearing:
a failing job blocks a merge only when its exact check name is present in the
live `main` ruleset. A workflow name, a `merge_group` trigger, or a red PR check
does not make a job required by itself.

## Required merge checks

The live `main` ruleset (`6436880`) currently requires exactly these contexts:

| Required context | Producer | Validation |
|---|---|---|
| `CLA Check` | `.github/workflows/cla.yml` | Contributor agreement |
| `Quick Checks` | `.github/workflows/ci.yml` | Formatting and repository guard scripts |
| `Test and Lint` | `.github/workflows/ci.yml` | Clippy, workspace nextest excluding `e2e_test`, doctests, and migration proofs |

For pull requests limited to the paths excluded by the main CI workflow,
`.github/workflows/ci-docs-only.yml` reports `Quick Checks` and
`Test and Lint` under the same names. It runs the real quick checks and the
planning-document guard; it does not claim that Rust compilation or runtime
tests ran. Despite the workflow name, these paths also include selected deploy,
workflow, and lock files.

Verify the live rule rather than trusting this snapshot before changing merge
policy:

```bash
gh api repos/rustfs/rustfs/rulesets/6436880 \
  --jq '.rules[] | select(.type == "required_status_checks") | .parameters'
```

The ruleset currently has `strict_required_status_checks_policy=false`.
`Continuous Integration` accepts `merge_group` events and runs `e2e-full` for
them, but `End-to-End Tests (full merge gate)` is not currently a required
context. Therefore the repository is prepared to test a merge-queue SHA, but
the workflow alone does not prove that every merge passed that lane.

## Pull request and merge matrix

Budgets below are job `timeout-minutes`, not typical runtimes. “Report-only”
means the result is visible and actionable but is not in the live required
context list.

| Event | Validation | Budget | Merge status | Reproduction |
|---|---|---:|---|---|
| PR, non-doc change | `Quick Checks` | 10 min | Required | `make pre-commit` (broader local umbrella) |
| PR, non-doc change | `Test and Lint` | 90 min | Required | `cargo nextest run --profile ci --all --exclude e2e_test` |
| PR, non-doc change | `Typos` | 10 min | Report-only | `typos` |
| PR, non-doc change | `ILM Integration (serial)` | 90 min | Report-only | Use the exact command in `.github/workflows/ci.yml` |
| PR, non-doc change | rio-v2 / swift / sftp test-and-lint variants | 90 min each | Report-only | `cargo nextest run` with the workflow's feature set |
| PR, non-doc change | `Build RustFS Debug Binary` | 30 min | Report-only; prerequisite for black-box lanes | `cargo build -p rustfs --bins` |
| PR, non-doc change | `io_uring Integration (real)` | 30 min | Report-only | `cargo test -p rustfs-ecstore --lib uring_ -- --test-threads=1 --nocapture` |
| PR, non-doc change | `End-to-End Tests` (`e2e-smoke` plus `s3s-e2e`) | 30 min | Report-only | `cargo nextest run --profile e2e-smoke -p e2e_test`; then `./scripts/e2e-run.sh ./target/debug/rustfs <data-dir>` |
| PR, non-doc change | `S3 Implemented Tests` | 60 min | Report-only | Build `rustfs`, then run `scripts/s3-tests/run.sh` with `DEPLOY_MODE=binary`, `TEST_MODE=single`, and `MAXFAIL=0` |
| PR, non-doc change | `S3 Lifecycle Behavior Tests` | 30 min | Report-only | Use the accelerated scanner environment in `.github/workflows/ci.yml` with `scripts/s3-tests/run.sh` |
| PR touching dependency or workflow inputs | Cargo Deny / Workflow Pin Report / Dependency Review | 20 / 5 / 30 min | Report-only | `cargo deny check`; `scripts/security/check_workflow_pins.sh` |
| PR touching architecture rules or architecture docs | `Architecture Migration Rules` | 10 min | Report-only | `scripts/check_architecture_migration_rules.sh` |
| PR touching Nix or workspace manifests | `Nix Build & Check` | 60 min | Report-only | `nix flake check` |
| PR limited to main-CI-excluded paths | companion `Quick Checks` and `Test and Lint` | 10 min each | Required | `git diff --check`; `make doc-paths-check` when documentation paths changed |
| `merge_group` | Standard CI plus `e2e-full` | 55 min for `e2e-full` | Standard required contexts only; `e2e-full` report-only | `cargo nextest run --profile e2e-full -p e2e_test` |
| Push to `main` | Standard CI plus `e2e-full` | 55 min for `e2e-full` | Post-merge detection | Same as `merge_group` |
| PR touching fuzz inputs or harness paths | Build plus five 60-second fuzz smoke targets | 60 min build; 30 min per target | Report-only | `MAX_TOTAL_TIME=60 ./scripts/fuzz/run.sh` |
| PR touching selected ecstore disk/format paths | `Rename Safety` on Windows | 60 min | Report-only | Run the four `cargo test -p rustfs-ecstore --lib <filter>` commands in `windows-filesystem.yml` on Windows |

The authoritative e2e filters live in `.config/nextest.toml`; extend a profile
instead of adding a second ad-hoc selector. Before a profile runs,
`scripts/check_test_wiring.py` compares its exact membership to the committed
digest so a silent test drop fails closed.

## Scheduled and manual validation

Scheduled lanes are independent fault domains. They do not block a pull
request, but their workflow-local gate can fail the run and scheduled failures
are routed to the shared failure-issue action. The scheduled-validation
watchdog and freshness workflow separately detect incomplete runs and missing
schedules.

| Cadence (UTC unless noted) | Workflow / validation | Budget | Verdict and artifacts | Reproduction |
|---|---|---:|---|---|
| Daily 02:17 | Fuzz: five nightly corpus targets | 60 min build; 60 min per target | Gate; corpus/crash artifacts, scheduled failure alert | `MAX_TOTAL_TIME=<seconds> ./scripts/fuzz/run.sh` |
| Daily 03:17 | MinIO interop (EC + SSE read parity) | 40 min | Gate; scheduled failure alert | Dispatch `minio-interop.yml` or follow its pinned Docker fixture steps |
| Daily 04:29 | Replication / cluster-fault / protocol e2e | 45 / 90 / 90 min | Three independent gates; JUnit, membership, and server logs | `cargo nextest run --profile e2e-repl-nightly -p e2e_test`; `--profile e2e-nightly`; `-j 1 --profile e2e-protocols` |
| Daily 06:31 | Warp performance A/B | 180 min | Regression budget gate; A/B summaries and server logs | `bash scripts/run_hotpath_warp_abba.sh --help` |
| Daily 00:07 Asia/Shanghai (16:07 UTC previous day) | Nightly GNU build and Vault lanes | 150 / 90 / 60 min | Build, live Vault, and HA failover gates | Use the commands and pinned Vault images in `nightly-gnu.yml` |
| Daily 03:23 | Security Audit | 20 / 5 min, plus 30 min on PR dependency review | Cargo Deny and workflow-pin gates; scheduled failure alert | `cargo deny check`; `scripts/security/check_workflow_pins.sh` |
| Daily 23:47 | Scheduled Validation Freshness | 10 min | Fails when a critical schedule was never created or is stale | Dispatch `scheduled-validation-freshness.yml` |
| Sunday 00:11 | Full `Continuous Integration` matrix | Per-job budgets above | Weekly variant coverage, including dormant rio-v2 binary/e2e lanes | Dispatch `ci.yml` |
| Sunday 01:13 | Seven-platform build matrix | 150 min per platform | Build/package integrity; scheduled failure alert | Dispatch `build.yml` with an exact platform set |
| Sunday 02:19 | Ceph s3-tests full sweep: single and real four-node, four shards each | 180 min per shard | Compatibility gate; report, JUnit, exact node IDs, and server logs | `scripts/s3-tests/run.sh` against an existing single or distributed target |
| Sunday 06:41 | Mint | 120 min | **Report-only by design**; per-suite PASS/FAIL/NA and raw `log.json` | Reproduce the pinned Docker sequence in `mint.yml` or dispatch it |
| Sunday 07:43 | Workspace line coverage | 120 min | Report-only trend; lcov and JSON retained 90 days | `make coverage` |
| Monthly, day 1 06:37 | Runner Hygiene | 15 min | Validates runner ephemerality; scheduled failure alert | Dispatch `runner-hygiene.yml` |

Manual `workflow_dispatch` exists for the scheduled workflows above. Manual
runs are debugging evidence and intentionally do not open scheduled-failure
issues. A manual performance run may explicitly allow a known regression; that
override must not be treated as an ordinary passing baseline.

## Release validation

Release validation is post-merge and tag-driven; it does not substitute for a
pull-request gate.

| Event | Validation | Budget | Result |
|---|---|---:|---|
| Push to `main` or weekly schedule | `Build and Release` platform matrix | 150 min per platform | Build artifacts for all selected targets; no release publication on a main push |
| Valid release or preview tag | `Build and Release` plus asset checks | 150 min per platform | Draft release, checksummed assets, and publish step |
| Successful non-preview release-tag build | Docker image build and image scan | 60 min build; 30 min scan | Multi-architecture images plus vulnerability report |
| Successful release-tag build | DEB/RPM packaging | 30 min per architecture | Packages and checksum files uploaded to the release |
| Successful non-preview release-tag build | Helm template test and package | 30 min build; 30 min publish | Versioned chart and repository index |

Use an exact preview tag for end-to-end release rehearsal. Manual dispatches
are backfill/debug paths and do not prove the automatic `workflow_run` chain.

## Evidence requirements

A green check is useful only when it proves the intended behavior ran:

- Record the exact commit SHA and run URL.
- Separate product failure from runner prerequisites, service readiness, and
  cancellation. Repair the precondition, then rerun the exact workload.
- Preserve membership manifests, JUnit, raw compatibility logs, seeds, and
  server logs where the workflow provides them.
- For a bug fix or a new fault checker, provide sensitivity evidence: the old
  behavior or an intentional mutation must fail the new oracle, and the fixed
  behavior must pass it.
- Never promote a report-only lane to required from one green run. Require at
  least 14 days and 30 representative pull requests with at least 99% complete
  execution, then update the ruleset and this table together.

## Change checklist

Update this file in the same pull request when any of these change:

- workflow triggers, job names, timeouts, or nextest profile ownership;
- required status contexts or strict/merge-queue policy;
- scheduled cadence, alert routing, artifact contract, or local reproduction;
- report-only versus gating semantics.

Do not copy per-module test counts here. Update
[e2e-suite-inventory.md](e2e-suite-inventory.md) and its enforced membership
digest instead.
