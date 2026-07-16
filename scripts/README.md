# scripts/ index

Authoritative inventory of everything under `scripts/` (backlog#1153
infra-13). One row per top-level entry; subdirectories get one row each and
keep their own READMEs. The test-layer map that ties the major runners
together lives in [`docs/testing/README.md`](../docs/testing/README.md).

**Statuses**

- **ci-gate** — wired into CI, release, or image-build pipelines. Do not move,
  rename, or change flags without updating the wiring listed in the last
  column.
- **dev-tool** — run by humans: local dev loops, runbooks, validation
  harnesses. Kept working, not wired into CI.
- **archived** — one-shot scripts whose investigation/issue is finished, moved
  to [`scripts/archive/`](archive/). Unmaintained reference material: never
  wire into CI, and expect bit-rot. To resurrect one, move it back and give it
  an index row here.

**Adding a script?** Add an index row in the same PR. Issue-scoped scripts
(`run_issueNNN_*`, `validate_issue_NNN_*`) are expected to be archived when
their issue closes.

## Repository & CI gates

| Entry | Status | Purpose | Wiring / docs |
|---|---|---|---|
| `check_architecture_migration_rules.sh` | ci-gate | Architecture-boundary anti-regression guard | ci.yml Quick Checks; `make pre-commit` |
| `check_body_cache_whitelist.sh` | ci-gate | Keeps the app-layer body-cache eligibility gate fail-closed | ci.yml Quick Checks |
| `check_doc_paths.sh` | ci-gate | Fails when instruction/architecture docs reference repo paths that no longer exist | `make pre-commit` / `pre-pr` |
| `check_extension_schema_boundaries.sh` | ci-gate | Extension-schema crate boundary guard | ci.yml Quick Checks; `make pre-commit` |
| `check_layer_dependencies.sh` | ci-gate | Crate-layering DAG guard (reads `layer-dependency-baseline.txt`) | ci.yml Quick Checks |
| `check_logging_guardrails.sh` | ci-gate | Blocks legacy logging patterns from returning | `make pre-commit` / `pre-pr` |
| `check_migration_gate_count.sh` | ci-gate | Migration-critical test gate with committed count floor (`.config/migration-gate-floor.txt`) | ci.yml Test and Lint; `docs/testing/README.md` |
| `check_no_planning_docs.sh` | ci-gate | Blocks committed planning-type documents | ci.yml Quick Checks; `make pre-commit` |
| `check_no_tokio_io_uring.sh` | ci-gate | Keeps tokio's io-uring backend disabled | ci.yml Quick Checks |
| `check_unsafe_code_allowances.sh` | ci-gate | Unsafe-code allowance ledger guard | ci.yml Quick Checks |
| `layer-dependency-baseline.txt` | ci-gate (data) | Committed baseline consumed by `check_layer_dependencies.sh` | arch-checks skill |
| `static.sh` | ci-gate | Static-build helper executed inside image builds | `Dockerfile.source`, `Dockerfile.decommission-local` |
| `helm_chart_version.sh` | ci-gate | Keeps the Helm chart version in sync with the release | helm-package.yml |
| `test_helm_templates.sh` | ci-gate | Helm template rendering test | helm-package.yml |

## Test & e2e runners

| Entry | Status | Purpose | Wiring / docs |
|---|---|---|---|
| `e2e-run.sh` | ci-gate | Boots a rustfs server and runs the `s3s-e2e` black-box conformance tool against it | ci.yml `e2e-tests` jobs; `docs/testing/README.md` |
| `run_ecstore_validation_suite.sh` | dev-tool | ecstore black-box validation suite (`quick`/`full`/`destructive`/`fuzz` profiles) | `docs/testing/README.md`, `docs/testing/ecstore-validation-suite-design.md` |
| `run_e2e_tests.sh` | dev-tool | Local `e2e_test` crate runner (starts a server, applies filters, cleans up) | `crates/e2e_test/README.md` |
| `run.sh` | dev-tool | Local rustfs startup wrapper | `make e2e-server`; Justfile |
| `run.ps1` | dev-tool | Windows counterpart of `run.sh` | — |
| `probe.sh` | dev-tool | Probe-style e2e run | `make probe-e2e` |
| `run_scanner_validation_harness.sh` | dev-tool | Scanner validation harness | `docs/operations/scanner-benchmark-runbook.md` |
| `test_scanner_validation_harness.sh` | dev-tool | Self-test for the scanner validation harness | — |
| `test_build_rustfs_options.sh` | dev-tool | Shell test for rustfs build-option wiring | `make test` (script-tests) |
| `test_entrypoint_credentials.sh` | dev-tool | Container entrypoint credential-handling test | `make test` (script-tests) |
| `test_helm_chart_version.sh` | dev-tool | Test for `helm_chart_version.sh` | — |
| `windows-sftp-listener-smoke.sh` | dev-tool | Confirms `rustfs.exe --features sftp` binds an SFTP listener on Windows | — |

## Benchmark & performance harnesses

| Entry | Status | Purpose | Wiring / docs |
|---|---|---|---|
| `run_hotpath_warp_ab.sh` | ci-gate | Linux warp A/B rig for the hotpath series | performance-ab.yml (scheduled); `docs/operations/hotpath-warp-ab-runbook.md` |
| `hotpath_warp_ab_gate.sh` | dev-tool | Relative-budget gate evaluated over the warp A/B results | used by `run_hotpath_warp_ab.sh`; hotpath runbook |
| `run_internode_grpc_ab_bench.sh` | dev-tool | One-click A/B driver for the internode gRPC optimization stages | `docs/operations/internode-grpc-benchmark-runbook.md` |
| `run_internode_transport_baseline.sh` | dev-tool | Internode transport baseline runner | internode runbook; `crates/io-metrics/README.md` |
| `run_four_node_cluster_failover_bench.sh` | dev-tool | Four-node cluster failover benchmark | `docker/compose/README.md`; internode runbook |
| `run_object_batch_bench.sh` | dev-tool | Batch object benchmark runner (warp/s3bench) | internode + scanner runbooks |
| `run_object_batch_bench_enhanced.sh` | dev-tool | Enhanced batch benchmark runner; hub used by the smoke rigs | hotpath runbook |
| `run_get_codec_streaming_smoke.sh` | dev-tool | Local GET benchmark harness for the codec streaming read path | `docs/testing/ecstore-validation-suite-design.md` |
| `run_gt1g_get_http_matrix.sh` | dev-tool | >1 GiB GET HTTP matrix | `docs/testing/ecstore-validation-suite-design.md` |
| `run_gt1g_multipart_put_matrix.sh` | dev-tool | >1 GiB multipart PUT matrix | `docs/testing/ecstore-validation-suite-design.md` |
| `run_scanner_benchmarks.sh` | dev-tool (disposition pending) | Scanner performance benchmark runner. Contains a hardcoded stale path; **disposition owned by backlog perf-10 — do not fix, move, or delete it here** | — |

## Local development & operations

| Entry | Status | Purpose | Wiring / docs |
|---|---|---|---|
| `dev_clear.sh` | dev-tool | Local dev cleanup. `scripts/dev_*.sh` is a CI paths-filter glob — keep the naming | ci.yml/build.yml paths filters |
| `dev_deploy.sh` | dev-tool | Copy a built binary to dev servers | `make deploy` (`.config/make/deploy.mak`); Justfile |
| `dev_rustfs.sh` | dev-tool | Local dev run loop | — |
| `dev_rustfs.env` | dev-tool (data) | Env presets for the dev scripts | — |
| `restart_local_single_node_multidisk_rustfs.sh` | dev-tool | Restart a local single-node multi-disk instance | — |
| `inspect_dashboard.sh` | dev-tool | Sanity-checks the Grafana dashboard JSON | `.docker/observability` |
| `notify.sh` | dev-tool | Starts a local webhook receiver for notify-target development | — |
| `install-flatc.sh` | dev-tool | Local flatc installer (macOS) | — |
| `install-protoc.sh` | dev-tool | Local protoc installer (macOS/Linux) | — |
| `makefile-header.sh` | dev-tool | Generates the `## —— section ——` header lines used in `.config/make/*.mak` | — |
| `tls_gen.md` | dev-tool (doc) | Notes on generating local TLS certificates | — |

## Subdirectories

| Entry | Status | Purpose | Wiring / docs |
|---|---|---|---|
| `fuzz/` | ci-gate | Unified cargo-fuzz runner and helpers for the `fuzz/` sub-workspace | fuzz.yml; `fuzz/README.md` |
| `s3-tests/` | ci-gate | ceph/s3-tests compatibility harness (allow-lists, patches, report tooling) | ci.yml; e2e-s3tests.yml; `scripts/s3-tests/README.md` |
| `security/` | ci-gate | Workflow-pin enforcement and release supply-chain asset generation | audit.yml; build.yml |
| `table-catalog/` | dev-tool | S3-Tables / pyiceberg validation suite | `docs/architecture/s3-tables-support-matrix.md` |
| `test/` | dev-tool | Manual operational validation runbooks (decommission, tier lifecycle), paired `.sh` + `.md` | — |
| `archive/` | archived | Retired one-shot scripts (see below) | — |

## Archived (scripts/archive/)

Moved 2026-07 (backlog#1153 infra-13) after a whole-tree reference census:
each entry had **zero references** from CI, Makefiles, docs, or code — or was
referenced only by other scripts in this same archived set. Reasons:

| Entry | Was |
|---|---|
| `validate_issue_785_list_objects.sh` | One-shot issue validation (list-objects series) |
| `validate_issue_786_list_objects.sh` | One-shot issue validation (list-objects series) |
| `validate_issue_787_list_quorum.sh` | One-shot issue validation (list-quorum) |
| `validate_issue_841_list_objects_observability.sh` | One-shot issue validation (list observability) |
| `validate_issue_1365_docker.sh` | One-shot issue validation (docker repro) |
| `validate_issue_2723_site_replication.sh` | One-shot issue validation (site replication) |
| `validate_issue_3031_docker.sh` | One-shot issue validation (docker repro) |
| `run_issue712_deeper_zero_copy_put_with_capture.sh` | One-shot perf capture for backlog#712 |
| `run_issue797_local_4node_16disk_ab.sh` | One-shot 4-node/16-disk A/B for backlog#797 |
| `run_issue_2573_acceptance.sh` | One-shot acceptance run for issue #2573 |
| `run_issue_2941_perf_capture.sh` | One-shot perf capture for issue #2941 |
| `run_put_large_stage_breakdown.sh` | backlog#706 large-PUT stage breakdown (family) |
| `run_put_large_stage_breakdown_with_capture.sh` | backlog#706 one-shot wrapper (family) |
| `run_put_large_tuning_matrix.sh` | backlog#706 tuning matrix (family) |
| `collect_put_large_stage_breakdown_artifacts.sh` | backlog#706 artifact collector (family) |
| `analyze_put_service_metrics_deltas.py` | backlog#706 metrics-delta analyzer (family) |
| `README-stress-test.md` | GET-optimization one-shot suite doc |
| `stress-test-get-optimization.sh` | GET-optimization one-shot stress test |
| `quick-validate-get-optimization.sh` | GET-optimization one-shot validation |
| `benchmark-sf-optimization.sh` | GET-optimization one-shot benchmark |
| `prepare_gt1g_get_test_objects.sh` | >1 GiB GET investigation one-shot fixture prep |
| `run_gt1g_multipart_put_server_path_focus.sh` | >1 GiB PUT investigation one-shot focus run |
| `run_get_metrics_gate_smoke.sh` | One-shot GET metrics-gate smoke |
| `run_listobjects_verified_bench.sh` | One-shot verified list-objects bench |
| `run_object_batch_bench_abc.sh` | One-shot capacity/object profile A/B/C controller |
| `run_object_data_cache_bench.sh` | One-shot GET bench for the object-data-cache rollout gate |
| `setup-test-binaries.sh` | One-shot Docker-build test binary fixture |
| `test.sh` | Ancient manual `mc` bucket smoke scratchpad |
| `test_policy.json` | Orphaned IAM policy fixture (hardcoded test bucket) |
