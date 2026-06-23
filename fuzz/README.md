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

# Skip build (use pre-built harness)
SKIP_BUILD=1 FUZZ_TARGET=local_metadata ./scripts/fuzz/run.sh
```

## CI Workflow

The GitHub Actions workflow (`.github/workflows/fuzz.yml`) uses a **build/run separation** pattern:

1. **`fuzz-build`** — compiles all fuzz targets once, uploads `fuzz/target/` as an artifact.
2. **`pr-fuzz-smoke`** — matrix job that runs each target in parallel (60s each). Restores the build artifact so there is no recompilation.
3. **`nightly-fuzz-corpus`** — matrix job that runs each target in parallel (300s each) on a daily schedule.

This design avoids redundant compilation across targets and keeps wall-clock time low.

## Seed Corpus

Initial seed directories live under `fuzz/corpus/`.

- Keep small, representative, and target-specific inputs here.
- Prefer checked-in fixtures for stable formats like `xl.meta`.
- Use `cargo fuzz cmin` / `cargo fuzz tmin` to shrink corpora and crashes before committing them.

The helper scripts also invoke `cargo +nightly fuzz ...` explicitly so local execution does not depend on the default toolchain.
