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

## Targets

- `bucket_validation`
  - Exercises RustFS bucket-name and bucket/object argument validation without pulling in the full `rustfs` binary crate graph.
- `path_containment`
  - Exercises object-path acceptance and containment invariants using public path helpers.
- `local_metadata`
  - Exercises `rustfs-filemeta` metadata decoding and `rustfs-utils` block decompression.

## Usage

Run a single target:

```bash
cd fuzz
cargo +nightly fuzz run path_containment
```

Run bounded smoke targets from the repository root:

```bash
./scripts/fuzz/run_ci_targets.sh
```

Run a longer local/nightly-style pass:

```bash
./scripts/fuzz/run_nightly_targets.sh
```

## Seed Corpus

Initial seed directories live under `fuzz/corpus/`.

- Keep small, representative, and target-specific inputs here.
- Prefer checked-in fixtures for stable formats like `xl.meta`.
- Use `cargo fuzz cmin` / `cargo fuzz tmin` to shrink corpora and crashes before committing them.

The helper scripts also invoke `cargo +nightly fuzz ...` explicitly so local execution does not depend on the default toolchain.
