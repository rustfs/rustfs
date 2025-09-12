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

### Code Quality
- `cargo fmt --all` - Format code
- `cargo clippy --all-targets --all-features -- -D warnings` - Lint code
- `make pre-commit` or `just pre-commit` - Run all quality checks (fmt, clippy, check, test)

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

### Build System
- Cargo workspace with 25+ crates
- Custom `build-rustfs.sh` script for advanced build options
- Multi-architecture Docker builds via `docker-buildx.sh`
- Both Make and Just task runners supported
- Cross-compilation support for multiple Linux targets

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
- E2E tests in separate crate (`e2e_test`)
- Shadow build for version/metadata embedding
- Support for both GNU and musl libc targets

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
- `RUSTFS_ENABLE_SCANNER` - Enable/disable background data scanner
- `RUSTFS_ENABLE_HEAL` - Enable/disable auto-heal functionality
- Various profiling and observability controls

## Code Style
- Communicate with me in Chinese, but only English can be used in code files.
- Code that may cause program crashes (such as unwrap/expect) must not be used, except for testing purposes.
- Code that may cause performance issues (such as blocking IO) must not be used, except for testing purposes.
- Code that may cause memory leaks must not be used, except for testing purposes.
- Code that may cause deadlocks must not be used, except for testing purposes.
- Code that may cause undefined behavior must not be used, except for testing purposes.
- Code that may cause panics must not be used, except for testing purposes.
- Code that may cause data races must not be used, except for testing purposes.
