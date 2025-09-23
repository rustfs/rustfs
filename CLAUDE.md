# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

RustFS is a high-performance distributed object storage software built with Rust, providing S3-compatible APIs and advanced features like data lakes, AI, and big data support. It's designed as an alternative to MinIO with better performance and a more business-friendly Apache 2.0 license.

## Build Commands

### Primary Build Commands
- `cargo build --release` - Build the main RustFS binary
- `./build-rustfs.sh` - Recommended build script that handles console resources and cross-platform compilation
- `./build-rustfs.sh --dev` - Development build with debug symbols
- `make build` or `just build` - Use Make/Just for standardized builds

### Platform-Specific Builds
- `./build-rustfs.sh --platform x86_64-unknown-linux-musl` - Build for musl target
- `./build-rustfs.sh --platform aarch64-unknown-linux-gnu` - Build for ARM64
- `make build-musl` or `just build-musl` - Build musl variant
- `make build-cross-all` - Build all supported architectures

### Testing Commands
- `cargo test --workspace --exclude e2e_test` - Run unit tests (excluding e2e tests)
- `cargo nextest run --all --exclude e2e_test` - Use nextest if available (faster)
- `cargo test --all --doc` - Run documentation tests
- `make test` or `just test` - Run full test suite
- `make pre-commit` - Run all quality checks (fmt, clippy, check, test)

### End-to-End Testing
- `cargo test --package e2e_test` - Run all e2e tests
- `./scripts/run_e2e_tests.sh` - Run e2e tests via script
- `./scripts/run_scanner_benchmarks.sh` - Run scanner performance benchmarks

### KMS-Specific Testing (with proxy bypass)
- `NO_PROXY=127.0.0.1,localhost HTTP_PROXY= HTTPS_PROXY= http_proxy= https_proxy= cargo test --package e2e_test test_local_kms_end_to_end -- --nocapture --test-threads=1` - Run complete KMS end-to-end test
- `NO_PROXY=127.0.0.1,localhost HTTP_PROXY= HTTPS_PROXY= http_proxy= https_proxy= cargo test --package e2e_test kms:: -- --nocapture --test-threads=1` - Run all KMS tests
- `cargo test --package e2e_test test_local_kms_key_isolation -- --nocapture --test-threads=1` - Test KMS key isolation
- `cargo test --package e2e_test test_local_kms_large_file -- --nocapture --test-threads=1` - Test KMS with large files

### Code Quality
- `cargo fmt --all` - Format code
- `cargo clippy --all-targets --all-features -- -D warnings` - Lint code
- `make pre-commit` or `just pre-commit` - Run all quality checks (fmt, clippy, check, test)

### Quick Development Commands
- `make help` or `just help` - Show all available commands with descriptions
- `make help-build` - Show detailed build options and cross-compilation help
- `make help-docker` - Show comprehensive Docker build and deployment options
- `./scripts/dev_deploy.sh <IP>` - Deploy development build to remote server
- `./scripts/run.sh` - Start local development server
- `./scripts/probe.sh` - Health check and connectivity testing

### Docker Build Commands
- `make docker-buildx` - Build multi-architecture production images
- `make docker-dev-local` - Build development image for local use
- `./docker-buildx.sh --push` - Build and push production images

## Architecture Overview

### Core Components

**Main Binary (`rustfs/`):**
- Entry point at `rustfs/src/main.rs`
- Core modules: admin, auth, config, server, storage, license management, profiling
- HTTP server with S3-compatible APIs
- Service state management and graceful shutdown
- Parallel service initialization with DNS resolver, bucket metadata, and IAM

**Key Crates (`crates/`):**
- `ecstore` - Erasure coding storage implementation (core storage layer)
- `iam` - Identity and Access Management
- `kms` - Key Management Service for encryption and key handling
- `madmin` - Management dashboard and admin API interface  
- `s3select-api` & `s3select-query` - S3 Select API and query engine
- `config` - Configuration management with notify features
- `crypto` - Cryptography and security features
- `lock` - Distributed locking implementation
- `filemeta` - File metadata management
- `rio` - Rust I/O utilities and abstractions
- `common` - Shared utilities and data structures
- `protos` - Protocol buffer definitions
- `audit-logger` - Audit logging for file operations
- `notify` - Event notification system
- `obs` - Observability utilities
- `workers` - Worker thread pools and task scheduling
- `appauth` - Application authentication and authorization
- `ahm` - Asynchronous Hash Map for concurrent data structures
- `mcp` - MCP server for S3 operations
- `signer` - Client request signing utilities
- `checksums` - Client checksum calculation utilities
- `utils` - General utility functions and helpers
- `zip` - ZIP file handling and compression
- `targets` - Target-specific configurations and utilities

### Build System
- Cargo workspace with 25+ crates (including new KMS functionality)
- Custom `build-rustfs.sh` script for advanced build options
- Multi-architecture Docker builds via `docker-buildx.sh`
- Both Make and Just task runners supported with comprehensive help
- Cross-compilation support for multiple Linux targets
- Automated CI/CD with GitHub Actions for testing, building, and Docker publishing
- Performance benchmarking and audit workflows

### Key Dependencies
- `axum` - HTTP framework for S3 API server
- `tokio` - Async runtime
- `s3s` - S3 protocol implementation library
- `datafusion` - For S3 Select query processing  
- `hyper`/`hyper-util` - HTTP client/server utilities
- `rustls` - TLS implementation
- `serde`/`serde_json` - Serialization
- `tracing` - Structured logging and observability
- `pprof` - Performance profiling with flamegraph support
- `tikv-jemallocator` - Memory allocator for Linux GNU builds

### Development Workflow
- Console resources are embedded during build via `rust-embed`
- Protocol buffers generated via custom `gproto` binary
- E2E tests in separate crate (`e2e_test`) with comprehensive KMS testing
- Shadow build for version/metadata embedding
- Support for both GNU and musl libc targets
- Development scripts in `scripts/` directory for common tasks
- Git hooks setup available via `make setup-hooks` or `just setup-hooks`

### Performance & Observability
- Performance profiling available with `pprof` integration (disabled on Windows)
- Profiling enabled via environment variables in production
- Built-in observability with OpenTelemetry integration
- Background services (scanner, heal) can be controlled via environment variables:
  - `RUSTFS_ENABLE_SCANNER` (default: true)
  - `RUSTFS_ENABLE_HEAL` (default: true)

### Service Architecture
- Service state management with graceful shutdown handling
- Parallel initialization of core systems (DNS, bucket metadata, IAM)
- Event notification system with MQTT and webhook support
- Auto-heal and data scanner for storage integrity
- Jemalloc allocator for Linux GNU targets for better performance

## Environment Variables
- `RUSTFS_ENABLE_SCANNER` - Enable/disable background data scanner (default: true)
- `RUSTFS_ENABLE_HEAL` - Enable/disable auto-heal functionality (default: true)
- Various profiling and observability controls
- Build-time variables for Docker builds (RELEASE, REGISTRY, etc.)
- Test environment configurations in `scripts/dev_rustfs.env`

### KMS Environment Variables
- `NO_PROXY=127.0.0.1,localhost` - Required for KMS E2E tests to bypass proxy
- `HTTP_PROXY=` `HTTPS_PROXY=` `http_proxy=` `https_proxy=` - Clear proxy settings for local KMS testing

## KMS (Key Management Service) Architecture

### KMS Implementation Status
- **Full KMS Integration:** Complete implementation with Local and Vault backends
- **Automatic Configuration:** KMS auto-configures on startup with `--kms-enable` flag
- **Encryption Support:** Full S3-compatible server-side encryption (SSE-S3, SSE-KMS, SSE-C)
- **Admin API:** Complete KMS management via HTTP admin endpoints
- **Production Ready:** Comprehensive testing including large files and key isolation

### KMS Configuration
- **Local Backend:** `--kms-backend local --kms-key-dir <path> --kms-default-key-id <id>`
- **Vault Backend:** `--kms-backend vault --kms-vault-endpoint <url> --kms-vault-key-name <name>`
- **Auto-startup:** KMS automatically initializes when `--kms-enable` is provided
- **Manual Configuration:** Also supports dynamic configuration via admin API

### S3 Encryption Support
- **SSE-S3:** Server-side encryption with S3-managed keys (`ServerSideEncryption: AES256`)
- **SSE-KMS:** Server-side encryption with KMS-managed keys (`ServerSideEncryption: aws:kms`)
- **SSE-C:** Server-side encryption with customer-provided keys
- **Response Headers:** All encryption types return correct `server_side_encryption` headers in PUT/GET responses

### KMS Testing Architecture
- **Comprehensive E2E Tests:** Located in `crates/e2e_test/src/kms/`
- **Test Environments:** Automated test environment setup with temporary directories
- **Encryption Coverage:** Tests all three encryption types (SSE-S3, SSE-KMS, SSE-C)
- **API Coverage:** Tests all KMS admin APIs (CreateKey, DescribeKey, ListKeys, etc.)
- **Edge Cases:** Key isolation, large file handling, error scenarios

### Key Files for KMS
- `crates/kms/` - Core KMS implementation with Local/Vault backends
- `rustfs/src/main.rs` - KMS auto-initialization in `init_kms_system()`
- `rustfs/src/storage/ecfs.rs` - SSE encryption/decryption in PUT/GET operations
- `rustfs/src/admin/handlers/kms*.rs` - KMS admin endpoints
- `crates/e2e_test/src/kms/` - Comprehensive KMS test suite
- `crates/rio/src/encrypt_reader.rs` - Streaming encryption for large files

## Code Style and Safety Requirements
- **Language Requirements:**
  - Communicate with me in Chinese, but **only English can be used in code files**
  - Code comments, function names, variable names, and all text in source files must be in English only
  - No Chinese characters, emojis, or non-ASCII characters are allowed in any source code files
  - This includes comments, strings, documentation, and any other text within code files
- **Safety-Critical Rules:**
  - `unsafe_code = "deny"` enforced at workspace level
  - Never use `unwrap()`, `expect()`, or panic-inducing code except in tests
  - Avoid blocking I/O operations in async contexts
  - Use proper error handling with `Result<T, E>` and `Option<T>`
  - Follow Rust's ownership and borrowing rules strictly
- **Performance Guidelines:**
  - Use `cargo clippy --all-targets --all-features -- -D warnings` to catch issues
  - Prefer `anyhow` for error handling in applications, `thiserror` for libraries
  - Use appropriate async runtimes and avoid blocking calls
- **Testing Standards:**
  - All new features must include comprehensive tests
  - Use `#[cfg(test)]` for test-only code that may use panic macros
  - E2E tests should cover KMS integration scenarios

## Common Development Tasks

### Running KMS Tests Locally
1. **Clear proxy settings:** KMS tests require direct localhost connections
2. **Use serial execution:** `--test-threads=1` prevents port conflicts
3. **Enable output:** `--nocapture` shows detailed test logs
4. **Full command:** `NO_PROXY=127.0.0.1,localhost HTTP_PROXY= HTTPS_PROXY= http_proxy= https_proxy= cargo test --package e2e_test test_local_kms_end_to_end -- --nocapture --test-threads=1`

### KMS Development Workflow
1. **Code changes:** Modify KMS-related code in `crates/kms/` or `rustfs/src/`
2. **Compile:** Always run `cargo build` after changes
3. **Test specific functionality:** Use targeted test commands for faster iteration
4. **Full validation:** Run complete end-to-end tests before commits

### Debugging KMS Issues
- **Server startup:** Check that KMS auto-initializes with debug logs
- **Encryption failures:** Verify SSE headers are correctly set in both PUT and GET responses
- **Test failures:** Use `--nocapture` to see detailed error messages
- **Key management:** Test admin API endpoints with proper authentication

## Important Reminders
- **Always compile after code changes:** Use `cargo build` to catch errors early
- **Don't bypass tests:** All functionality must be properly tested, not worked around
- **Use proper error handling:** Never use `unwrap()` or `expect()` in production code (except tests)
- **Follow S3 compatibility:** Ensure all encryption types return correct HTTP response headers

# important-instruction-reminders
Do what has been asked; nothing more, nothing less.
NEVER create files unless they're absolutely necessary for achieving your goal.
ALWAYS prefer editing an existing file to creating a new one.
NEVER proactively create documentation files (*.md) or README files. Only create documentation files if explicitly requested by the User.
