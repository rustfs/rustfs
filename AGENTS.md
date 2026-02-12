# Repository Guidelines

This file provides guidance for AI agents and developers working with code in this repository.

## Project Overview

RustFS is a high-performance distributed object storage software built with Rust, providing S3-compatible APIs and advanced features like data lakes, AI, and big data support. It's designed as an alternative to MinIO with better performance and a more business-friendly Apache 2.0 license.

## ⚠️ Pre-Commit Checklist (MANDATORY)

**Before EVERY commit, you MUST run and pass ALL of the following:**
```bash
cargo fmt --all --check    # Code formatting
cargo clippy --all-targets --all-features -- -D warnings  # Lints
cargo test --workspace --exclude e2e_test  # Unit tests
```
Or simply run `make pre-commit` which covers all checks. **DO NOT commit if any check fails.**

## Communication Rules

- Respond to the user in the same language used by the user.
- Code and documentation must be written in English only.
- **Pull Request titles and descriptions must be written in English** to ensure consistency and accessibility for all contributors.

## Project Structure

The workspace root hosts shared dependencies in `Cargo.toml`. The service binary lives under `rustfs/src/main.rs`, while reusable crates sit in `crates/` (including `ecstore`, `iam`, `kms`, `madmin`, `s3select-api`, `s3select-query`, `config`, `crypto`, `lock`, `filemeta`, `rio`, `common`, `protos`, `audit-logger`, `notify`, `obs`, `workers`, `appauth`, `ahm`, `mcp`, `signer`, `checksums`, `utils`, `zip`, `targets`, and `e2e_test`). Deployment manifests are under `deploy/`, Docker assets sit at the root, and automation lives in `scripts/`.

### Core Architecture

- **Main Binary (`rustfs/`):** Entry point at `rustfs/src/main.rs`, includes admin, auth, config, server, storage, license management, and profiling modules.
- **Key Crates:** Cargo workspace with 25+ crates supporting S3-compatible APIs, erasure coding storage, IAM, KMS, S3 Select, and observability.
- **Build System:** Custom `build-rustfs.sh` script, multi-architecture Docker builds, Make/Just task runners, cross-compilation support.

## Build, Test, and Development Commands

### Quick Commands
- `cargo check --all-targets` - Fast validation
- `cargo build --release` or `make build` - Release build
- `./build-rustfs.sh --dev` - Development build with debug symbols
- `make pre-commit` - Run all quality checks (fmt, clippy, check, test)

### Testing
- `cargo test --workspace --exclude e2e_test` - Unit tests
- `cargo test --package e2e_test` - E2E tests
- For KMS tests: `NO_PROXY=127.0.0.1,localhost HTTP_PROXY= HTTPS_PROXY= http_proxy= https_proxy= cargo test --package e2e_test test_local_kms_end_to_end -- --nocapture --test-threads=1`

### Cross-Platform Builds
- `./build-rustfs.sh --platform x86_64-unknown-linux-musl` - Build for musl
- `./build-rustfs.sh --platform aarch64-unknown-linux-gnu` - Build for ARM64
- `make build-cross-all` - Build all supported architectures

## Coding Style & Safety Requirements

- **Formatting:** Follow `rustfmt.toml` (130-column width). Use `snake_case` for items, `PascalCase` for types, `SCREAMING_SNAKE_CASE` for constants.
- **Safety:** `unsafe_code = "deny"` enforced at workspace level. Never use `unwrap()`, `expect()`, or panic-inducing code except in tests.
- **Error Handling:** Prefer `anyhow` for applications, `thiserror` for libraries. Use proper error handling with `Result<T, E>` and `Option<T>`.
- **Async:** Keep async code non-blocking. Offload CPU-heavy work with `tokio::task::spawn_blocking` when necessary.
- **Language:** Code comments, function names, variable names, and all text in source files must be in English only.

## Testing Guidelines

Co-locate unit tests with their modules and give behavior-led names. Integration suites belong in each crate's `tests/` directory, while exhaustive end-to-end scenarios live in `crates/e2e_test/`. When fixing bugs or adding features, include regression tests that capture the new behavior.

## KMS (Key Management Service)

- **Implementation:** Complete with Local and Vault backends, auto-configures on startup with `--kms-enable` flag.
- **Encryption:** Full S3-compatible server-side encryption (SSE-S3, SSE-KMS, SSE-C).
- **Testing:** Comprehensive E2E tests in `crates/e2e_test/src/kms/`. Requires proxy bypass for local testing.
- **Key Files:** `crates/kms/`, `rustfs/src/storage/ecfs.rs`, `rustfs/src/admin/handlers/kms*.rs`

## Environment Variables

- `RUSTFS_ENABLE_SCANNER` - Enable/disable background data scanner (default: true)
- `RUSTFS_ENABLE_HEAL` - Enable/disable auto-heal functionality (default: true)
- For KMS tests: `NO_PROXY=127.0.0.1,localhost` and clear proxy environment variables

## Commit & Pull Request Guidelines

Work on feature branches (e.g., `feat/...`) after syncing `main`. Follow Conventional Commits under 72 characters. Each commit must compile, format cleanly, and pass `make pre-commit`.

**Pull Request Requirements:**
- PR titles and descriptions **MUST be written in English**
- Open PRs with a concise summary, note verification commands, link relevant issues
- Follow the PR template format and fill in all required sections
- Wait for reviewer approval before merging

## Security & Configuration Tips

Do not commit secrets or cloud credentials; prefer environment variables or vault tooling. Review IAM- and KMS-related changes with a second maintainer. Confirm proxy settings before running sensitive tests to avoid leaking traffic outside localhost.

## Important Reminders

- Always compile after code changes: Use `cargo build` to catch errors early
- Don't bypass tests: All functionality must be properly tested, not worked around
- Use proper error handling: Never use `unwrap()` or `expect()` in production code (except tests)
- Do what has been asked; nothing more, nothing less
- NEVER create files unless they're absolutely necessary for achieving your goal
- ALWAYS prefer editing an existing file to creating a new one
- NEVER proactively create documentation files (*.md) or README files unless explicitly requested
- NEVER commit PR description files (e.g., PR_DESCRIPTION.md): These are temporary reference files for creating pull requests and should remain local only
