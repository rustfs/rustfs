# Repository Guidelines

## Project Structure & Module Organization
The workspace root defines shared dependencies in `Cargo.toml`. The service binary lives in `rustfs/` with entrypoints under `src/main.rs`. Crypto, IAM, and KMS components sit in `crates/` (notably `crates/crypto`, `crates/iam`, `crates/kms`). End-to-end fixtures are in `crates/e2e_test/` and `test_standalone/`. Operational tooling resides in `scripts/`, while deployment manifests are under `deploy/` and Docker assets at the root. Before editing, skim a crate’s README or module-level docs to confirm its responsibility.

## Build, Test, and Development Commands
Use `cargo build --release` for optimized binaries, or run `./build-rustfs.sh --dev` when iterating locally; `make build` mirrors the release pipeline. Cross-platform builds rely on `./build-rustfs.sh --platform <target>` or `make build-cross-all`. Validate code early with `cargo check --all-targets`. Run unit coverage using `cargo test --workspace --exclude e2e_test` and prefer `cargo nextest run --all --exclude e2e_test` if installed. Execute `make pre-commit` (fmt, clippy, check, test) before every push. After generating code, always run `make clippy` and ensure it completes successfully before proceeding.

## Coding Style & Naming Conventions
Formatting follows `rustfmt.toml` (130-column width, async-friendly wrapping). Adopt `snake_case` for items, `PascalCase` for types, and `SCREAMING_SNAKE_CASE` for constants. Avoid `unwrap()` and `expect()` outside tests; propagate errors with `Result` and crate-specific `thiserror` types. Keep async code non-blocking—use `tokio::task::spawn_blocking` if CPU-heavy work is unavoidable. Document public APIs with focused `///` comments that cover parameters, errors, and examples.

## Testing Guidelines
Co-locate unit tests with modules and use behavior-led names such as `handles_expired_token`. Place integration suites in `tests/` folders and exhaustive flows in `crates/e2e_test/`. For KMS validation, clear proxies and run `NO_PROXY=127.0.0.1,localhost HTTP_PROXY= HTTPS_PROXY= cargo test --package e2e_test kms:: -- --nocapture --test-threads=1`. Always finish by running `cargo test --all` to ensure coverage across crates.

## Commit & Pull Request Guidelines
Create feature branches (`feat/...`, `fix/...`, `refactor/...`) after syncing `main`; never commit directly to the protected branch. Commits must align with Conventional Commits (e.g., `feat: add kms key rotation`) and remain under 72 characters. Each commit should compile, format cleanly, pass clippy with `-D warnings`, and include relevant tests. Open pull requests with `gh pr create`, provide a concise summary, list verification commands, and wait for reviewer approval before merging.

## Communication Rules
- Respond to the user in Chinese; use English in all other contexts.
