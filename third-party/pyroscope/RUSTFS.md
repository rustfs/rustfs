# Temporary Pyroscope dependency patch

This is the published [pyroscope 2.1.1](https://crates.io/crates/pyroscope/2.1.1) source, from upstream commit `bc958d9e9d934ab97bc3c6887ed62c5880467d6b`, with its original license retained. Repository automation is omitted and documentation trailing whitespace is normalized. Copied comments and documentation also have spelling corrections required by the repository typo check.

The only runtime dependency change is the `object` dependency requirement, from `0.39` to `0.40`, in both manifests. Runtime behavior, features, build script, examples, and original tests are unchanged; source edits beyond the dependency requirement only correct comments. `tests/rustfs_zstd.rs` adds bounded valid and oversized-block regression coverage through `object`'s decompression API, registered in both manifests. `object 0.39` requires vulnerable `ruzstd 0.8`; `object 0.40` permits the patched `ruzstd 0.9.1` required by [RUSTSEC-2026-0344](https://rustsec.org/advisories/RUSTSEC-2026-0344). Keep the patched version in the workspace lockfile and retain the existing Cargo Deny gate.

Remove this directory, the workspace exclusion, and the `[patch.crates-io]` entry once an upstream Pyroscope release accepts `object 0.40` or later. Track validation and removal in [rustfs/backlog#2825](https://github.com/rustfs/backlog/issues/2825).

Run the retained upstream unit tests and the RustFS regressions with `cargo test --manifest-path third-party/pyroscope/Cargo.toml --features backend-pprof-rs --lib --test rustfs_zstd`.
