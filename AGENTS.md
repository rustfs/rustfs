# Repository Guidelines

## Communication Rules
- Respond to the user in Chinese; use English in all other contexts.
- Code and documentation must be written in English only. Chinese text is allowed solely as test data/fixtures when a case explicitly requires Chinese-language content for validation.

## Project Structure & Module Organization
The workspace root hosts shared dependencies in `Cargo.toml`. The service binary lives under `rustfs/src/main.rs`, while reusable crates sit in `crates/` (`crypto`, `iam`, `kms`, and `e2e_test`). Local fixtures for standalone flows reside in `test_standalone/`, deployment manifests are under `deploy/`, Docker assets sit at the root, and automation lives in `scripts/`. Skim each crate’s README or module docs before contributing changes.

## Build, Test, and Development Commands
Run `cargo check --all-targets` for fast validation. Build release binaries via `cargo build --release` or the pipeline-aligned `make build`. Use `./build-rustfs.sh --dev` for iterative development and `./build-rustfs.sh --platform <target>` for cross-compiles. Prefer `make pre-commit` before pushing to cover formatting, clippy, checks, and tests.
Always ensure `cargo fmt --all --check`, `cargo test --workspace --exclude e2e_test`, and `cargo clippy --all-targets --all-features -- -D warnings` complete successfully after each code change to keep the tree healthy and warning-free.

## Coding Style & Naming Conventions
Formatting follows the repo `rustfmt.toml` (130-column width). Use `snake_case` for items, `PascalCase` for types, and `SCREAMING_SNAKE_CASE` for constants. Avoid `unwrap()` or `expect()` outside tests; bubble errors with `Result` and crate-specific `thiserror` types. Keep async code non-blocking and offload CPU-heavy work with `tokio::task::spawn_blocking` when necessary.

## Testing Guidelines
Co-locate unit tests with their modules and give behavior-led names such as `handles_expired_token`. Integration suites belong in each crate’s `tests/` directory, while exhaustive end-to-end scenarios live in `crates/e2e_test/`. Run `cargo test --workspace --exclude e2e_test` during iteration, `cargo nextest run --all --exclude e2e_test` when available, and finish with `cargo test --all` before requesting review. Use `NO_PROXY=127.0.0.1,localhost HTTP_PROXY= HTTPS_PROXY=` for KMS e2e tests.
When fixing bugs or adding features, include regression tests that capture the new behavior so future changes cannot silently break it.

## Commit & Pull Request Guidelines
Work on feature branches (e.g., `feat/...`) after syncing `main`. Follow Conventional Commits under 72 characters (e.g., `feat: add kms key rotation`). Each commit must compile, format cleanly, and pass `make pre-commit`. Open PRs with a concise summary, note verification commands, link relevant issues, and wait for reviewer approval.

## Security & Configuration Tips
Do not commit secrets or cloud credentials; prefer environment variables or vault tooling. Review IAM- and KMS-related changes with a second maintainer. Confirm proxy settings before running sensitive tests to avoid leaking traffic outside localhost.
