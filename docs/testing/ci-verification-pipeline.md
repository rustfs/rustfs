# Verification plans and reusable CI artifacts

Use this guide when changing validation selection, test execution, or promoting a measured CI optimization. The workflow is `.github/workflows/ci.yml`; the selection policy is `.config/ci-plan.json`; the periodic responsibility registry is `.github/scheduled-validations.json`.

## Selection and rollout

The classifier reads the pull request base revision's planner and policy. It computes the changed packages and their reverse dependency closure from both revisions, including renamed dependencies, build dependencies, target dependencies and proc macros. Core package responsibilities are unioned with the committed test inventory. Unknown paths, changed build inputs, unavailable history or unresolved inventory expand validation. The proposed plan explains each selection and records candidate commit/tree, policy revision, event, configurations and expected job outcomes.

The initial policy is `shadow`: proposed selection is retained for comparison, while effective execution retains the full existing workspace and Connect responsibility for ordinary code changes. Existing documentation and isolated E2E scopes remain conservative. A base policy predating structured plans selects the full matrix. Promotion to `enforced` is a separate reviewed policy change after representative shadow comparisons, changed-test leak checks and failed-selection fixtures pass. Changing a PR's own policy cannot authorize skipping its base revision's requirements.

`Test and Lint` remains the required aggregate name. Every verification producer and consumer is a direct dependency. The verifier accepts only the effective plan's exact expected success/skipped outcomes; failure, cancellation, missing results, extra jobs or a plan for a different candidate fail. Quick Checks and CLA retain their separate required names.

## Build and execute

The workspace producer performs Clippy including the E2E harness with warnings denied, builds the selected nextest archive, executes doctests and checks log-rule anchors. The workspace consumer verifies and executes the archive, validates actual JUnit membership against its nextest listing, retains the core inventory and migration count floor, then executes the existing ignored ILM selection serially without a second compilation. The producer retains the measured Cargo build budget; execution retains nextest test groups and retry policy.

The E2E producer builds the hooks server and its existing provenance sidecars, builds the harness archive once, and runs harness doctests. E2E smoke, full E2E and the short core contract consume that archive. S3 implemented and lifecycle tests depend directly on the server producer and can run alongside E2E. The core profile uses three existing tests for object write/read, anonymous authorization denial and bytes surviving restart; its execution report must contain exactly those tests.

`scripts/ci_test_artifact.py` binds each archive to source content, commit/tree, lockfile, Rust and nextest versions, configuration, build flags, repository and run/attempt, then verifies its digest and size before use. A consumer with an older glibc, different platform, different selection or different identity fails. Re-running only a failed consumer does not borrow a previous attempt's archive: re-run the producer and consumers together. Archives expire after one day and are never used as release certification or uploaded into a privileged publishing context.

Consumer setup does not restore a Cargo target cache. It installs the runtime, then downloads and validates the current artifact. Each consumer extracts once when listing tests; subsequent runs use the extracted Cargo and binary metadata, including the serial ignored ILM run. Missing metadata fails instead of triggering a rebuild. Several tests embed absolute manifest paths and binary paths at compile time. Until those fixtures are made relocatable and tested, consumers require the same absolute checkout path as the producer, use the same runner family, and extract into that checkout's `target` directory. `--workspace-remap` alone does not rewrite strings compiled into a test executable. Source fixtures remain in the exact checkout, while non-test binaries and build outputs travel in the archive. The external s3s tool retains its independent pinned installation.

Workspace execution initially has one shard. There is no measured evidence yet that distributing this workspace archive improves total occupancy after compression, transfer and extraction. This also preserves every cross-test resource group. A future multi-shard promotion must use observed durations, keep every shared-resource component in one shard, cap concurrency, and prove the union of successful shard executions equals the selected listing. Merely applying nextest hash partitions would split process-local resource groups and is not an acceptable promotion.

## Measurements and operational switches

Use [resource accounting](ci-resource-accounting.md) to compare the same candidate and include all workflow attempts, cancellation, queue delay, shared merge-group allocation, and unknown costs. Use the independent [runner pilot](ci-runner-pilot.md) to establish storage, runtime and cleanup capabilities before modifying placement. Neither a generated archive nor a successful static check proves the target latency or cost reduction.

Periodic and manual suites use the responsibility registry and retain the existing immutable candidate/evidence contract. A selective manual run carries an explicit suite plan and cannot substitute for a full periodic chain. Shared environments remain locked through preparation, execution and cleanup. Health runs separately on hosted runners and checks the expected candidate, attempt and completed evidence; see [periodic validation orchestration](validation-orchestration.md).

## Merge-queue preparation and rollback

The merge-group event plans and validates its actual combined SHA/tree with the full Linux and feature configuration matrix. Main pushes keep complete validation; no reuse is inferred from tree equality because commit identity and build inputs can matter. CLA remains an independent queue-entry requirement.

`scripts/ci_merge_queue.py` prepares a bounded queue proposal from a live ruleset snapshot without calling a mutation API. It preserves all existing checks, reviews, bypass actors and branch conditions, adds an `ALLGREEN` squash queue with at most two concurrent builds/two merged entries, and produces the original rollback payload. Its timeout accommodates the existing full gate while measurements are collected. API fields follow [GitHub's ruleset contract](https://docs.github.com/en/rest/repos/rules).

```bash
mkdir -p artifacts/queue-switch
gh api repos/rustfs/rustfs/rulesets/6436880 > artifacts/queue-switch/before.json
python3 scripts/ci_merge_queue.py artifacts/queue-switch/before.json > artifacts/queue-switch/prepared.json
python3 scripts/ci_merge_queue.py artifacts/queue-switch/before.json \
  --prepared artifacts/queue-switch/prepared.json --phase before
```

Enabling the queue is an explicit maintainer operation after the proposal is accepted and real merge-group validation succeeds. Before that operation, refresh the live snapshot and compare `--phase before`; drift requires a new proposal. Keep the unchanged required check names throughout the switch. After application, read the same ruleset again and compare `--phase after`; record the operation, resulting ruleset and actual merge-group run. Rollback restores the saved `before` payload and verifies it with `--phase before`, retaining PR checks and full main validation. The helper does not apply either payload.

The deployment rehearsal must include two individually passing changes whose combination fails, queue regrouping after a base update, cancelled/missing results and a mismatched artifact. The combined candidate must remain blocked until its own full verification succeeds. Static fixture tests exercise the selection, identity and result contracts; they are not evidence that the live queue is enabled. The current rollout retains the pre-existing repository protection until the separate switch is performed.

## Reproduction and regression gates

```bash
python3 -m pip install PyYAML==6.0.3
python3 scripts/ci_gate.py --self-test
python3 scripts/ci_gate.py --check-workflow
python3 scripts/test_ci_test_artifact.py
python3 scripts/check_connect_release_boundary.py --self-test
python3 scripts/test_ci_merge_queue.py
python3 scripts/check_test_wiring.py --self-test
python3 scripts/check_test_wiring.py
actionlint
```

The full `make script-tests` includes these contracts plus the periodic registry, environment cleanup and accounting tests. The semantic workflow checks accept equivalent YAML block scalars while rejecting conditions or shell constructs that suppress required execution or its failure.
