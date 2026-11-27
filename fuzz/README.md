# RustFS Fuzz Harness

This directory contains the standalone `cargo-fuzz` harness for RustFS.

It is intentionally isolated from the main workspace so that:

- root `cargo fmt --all`
- root `cargo nextest`
- root `make pre-commit`

continue to behave exactly as they do today.

## Prerequisites

Install `cargo-fuzz` and a nightly toolchain locally:

```bash
cargo install cargo-fuzz
rustup toolchain install nightly
```

## Layout

```text
fuzz/
  Cargo.toml
  fuzz_targets/
  corpus/
  artifacts/
```

Crash reproducers are written under `fuzz/artifacts/<target>/`.

## Targets

- `bucket_validation`
  - Exercises RustFS bucket-name and bucket/object argument validation.
- `archive_extract`
  - Exercises archive entry path normalization, prefix application, and bucket-namespace containment checks.
- `path_containment`
  - Exercises object-path acceptance and containment invariants using public path helpers.
- `local_metadata`
  - Exercises `rustfs-filemeta` metadata decoding and `rustfs-utils` block decompression.
- `policy_ingress`
  - Exercises RustFS-owned bucket policy and policy-doc JSON ingress behavior, including strict-vs-tolerant unknown-field expectations.
- `sigv4_request`
  - Exercises public SigV4 request canonicalization and signing APIs (`try_sign_v4`, `try_pre_sign_v4`) using bounded `Arbitrary` requests and raw corpus fixtures with fixed test credentials and timestamp. No-panic client signing is paired with separate server-side presigned header rejection tests and rejection-branch mutation verification.
- `notify_xml`
  - Exercises `NotificationConfiguration::from_reader` and `NotificationConfiguration::validate` on raw XML bytes and bounded `Arbitrary` XML variants, asserting deterministic rejection of duplicate queues, invalid filters, and unsupported topic/lambda configurations.

## Usage

Run a single target:

```bash
cd fuzz
cargo +nightly fuzz run path_containment
```

Use the unified runner script from the repository root:

```bash
# Build + run all smoke targets (60s each)
./scripts/fuzz/run.sh

# Build only (no fuzz run)
BUILD_ONLY=1 ./scripts/fuzz/run.sh

# Build + run a single target
FUZZ_TARGET=path_containment ./scripts/fuzz/run.sh

# Nightly-style: 300s per target
MAX_TOTAL_TIME=300 ./scripts/fuzz/run.sh

# Replay a recorded libFuzzer seed
FUZZ_TARGET=path_containment FUZZ_SEED=123456789 ./scripts/fuzz/run.sh

# Skip build (use pre-built harness)
SKIP_BUILD=1 FUZZ_TARGET=local_metadata ./scripts/fuzz/run.sh
```

Each run writes `fuzz/artifacts/<target>/run-manifest.txt` and `fuzz/artifacts/<target>/run-summary.json` with the target, toolchain, libFuzzer seed, corpus digest, time budget, elapsed runtime (excluding build time), exit code, crash reproducer digest (if any), Git revision and dirty state, and runner mode. CI uploads those artifacts with the corpus and any crash input so the exact run can be replayed.

## Critical Rejection-Branch Mutation Verification

Fixed critical rejection-branch mutations are registered in `.config/security-rejection-mutants.toml` and verified in isolated temporary workspaces via:

```bash
python3 scripts/security/check_rejection_mutants.py --self-test
python3 scripts/security/check_rejection_mutants.py --cases .config/security-rejection-mutants.toml
```

Each mutation runs baseline green first and requires an explicit JUnit assertion failure on the mutated code; compile errors, zero matching tests, timeouts, and harness errors are rejected as invalid verification.

## CI Workflow

The GitHub Actions workflow (`.github/workflows/fuzz.yml`) exposes `workflow_call` (invoked by `ci.yml` job `security-input-gate`) alongside `schedule` and `workflow_dispatch`, using a **build/run separation** pattern:

1. **`select-targets`** uses `scripts/fuzz/select_targets.py` to select checks from changed paths. Documentation-only and unrelated PRs record `N/A`. Unknown diffs and merge groups select every target. A PR that changes the selector or target manifest runs the full matrix and rejection checks without executing the changed selector.
2. **`fuzz-build`** — compiles the selected fuzz targets once under a cache key isolated by toolchain, target, lockfile, and harness digest, then uploads prebuilt binaries.
3. **`pr-fuzz-smoke`** — matrix job running each selected target in parallel (60s execution budget per target) plus **`rejection-mutants`** verifying the fixed rejection-branch mutations.
4. **`nightly-fuzz-corpus`** — matrix job running all 7 targets in parallel (300s each) on the daily schedule with failure alerting.
5. **`security-input-gate-verdict`** — fail-closed summary gate over all selected `workflow_call` sub-jobs.

Note: PR smoke and fixed rejection-branch mutations do not substitute for full-application sanitizers, long-running stress tests, or RSS/fd exhaustion gates.

## Seed Corpus

Initial seed directories live under `fuzz/corpus/`.

- Keep small, representative, and target-specific inputs here.
- Prefer checked-in fixtures for stable formats like `xl.meta`.
- Use `cargo fuzz cmin` / `cargo fuzz tmin` to shrink corpora and crashes before committing them.

The helper scripts also invoke `cargo +nightly fuzz ...` explicitly so local execution does not depend on the default toolchain.
