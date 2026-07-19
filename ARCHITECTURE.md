# ARCHITECTURE.md

> Last updated: 2026-07-02 · Revision: 2
>
> This document describes the high-level architecture of RustFS.
> If you want to familiarize yourself with the code base, you are in the right place!
>
> See also [CONTRIBUTING.md](CONTRIBUTING.md) for development workflow.
> See also [docs/architecture](docs/architecture/overview.md) for active
> architecture migration guardrails.

## Bird's Eye View

RustFS is a high-performance, S3-compatible distributed object storage system written
in Rust. It uses erasure coding for data durability, supports multi-tenancy through
IAM/STS, and provides a web-based admin console.

A running RustFS node exposes:

- **S3 API** (port 9000) — the primary data path for object CRUD
- **Admin API** (port 9000, `/minio/` prefix) — cluster management, IAM, metrics
- **Console** (port 9001) — web UI backed by the Admin API
- **Inter-node RPC** (gRPC/tonic) — cluster communication for distributed mode

The core data flow for a PUT request looks like:

```
HTTP request
  → server (TLS, auth, routing, compression)
    → app/object_usecase (validation, policy, lifecycle)
      → storage/ecfs (erasure coding, encryption, checksums)
        → ecstore (disk pool selection, data distribution)
          → rio (reader pipeline: encrypt → compress → hash → write)
            → io-core (zero-copy I/O, buffer pool, direct I/O)
              → local disk / remote disk via RPC
```

## Code Map

The repository is a Cargo workspace with a flat `crates/` layout:

```
rustfs/                      # Workspace root (virtual manifest)
├── rustfs/                  # Main binary + library crate
│   └── src/
│       ├── main.rs          # Entry point, startup sequence
│       ├── lib.rs           # Module tree root
│       ├── server/          # HTTP server, TLS, routing, middleware
│       ├── admin/           # Admin API handlers and console
│       ├── app/             # Use-case layer (object, bucket, multipart)
│       ├── storage/         # Storage engine interface and implementation
│       ├── auth.rs          # S3 request authentication
│       ├── config/          # CLI args, config parsing, workload profiles
│       └── ...
├── crates/                  # library crates (authoritative list: Cargo.toml [workspace].members)
│   ├── ecstore/             # Erasure-coded storage engine
│   ├── rio/                 # Reader I/O pipeline (encrypt, compress, hash)
│   ├── io-core/             # Zero-copy I/O, scheduling, buffer pool
│   ├── io-metrics/          # I/O metrics collection
│   ├── common/              # Shared runtime state, globals, data usage types
│   ├── config/              # Configuration types and parsing
│   ├── utils/               # Pure utility functions
│   ├── ...                  # (see "Crate Reference" below)
│   └── e2e_test/            # End-to-end integration tests
└── docs/                    # Design documents and analysis
```

### Main Crate Layers (`rustfs/src/`)

The main crate is organized in layers, top to bottom:

| Layer | Directory | Responsibility |
|-------|-----------|----------------|
| **Server** | `server/` | HTTP listener, TLS, CORS, compression, middleware, graceful shutdown |
| **Admin** | `admin/` | Admin API routing, 30+ handler modules, web console |
| **App** | `app/` | Use-case orchestration: object_usecase, bucket_usecase, multipart_usecase |
| **Storage** | `storage/` | S3 API translation, erasure-coded FS, SSE encryption, RPC, concurrency |
| **Auth** | `auth.rs` | S3 signature verification, credential validation |
| **Config** | `config/` | CLI parsing, config struct, workload profiles |

A request flows **downward** through the layers. No layer should reach upward
(e.g., storage must not import from admin).

### Crate Reference

`Cargo.toml` is the authoritative workspace membership and `cargo tree` is the
authoritative dependency graph. This overview deliberately avoids line-count
and dependency-depth snapshots because both quickly become stale during
refactors.

#### By Domain

| Domain | Current workspace crates | Responsibility |
|--------|--------------------------|----------------|
| Foundation | `checksums`, `common`, `config`, `data-usage`, `utils` | Shared configuration, data-usage models, utilities, and checksums. |
| I/O and storage | `concurrency`, `ecstore`, `filemeta`, `heal`, `io-core`, `io-metrics`, `lifecycle`, `lock`, `object-capacity`, `object-data-cache`, `replication`, `rio`, `rio-v2`, `scanner`, `storage-api` | Erasure-coded object storage, metadata, recovery, lifecycle, replication, locking, cache, and I/O pipelines. |
| Security and identity | `credentials`, `crypto`, `iam`, `keystone`, `kms`, `policy`, `security-governance`, `signer`, `tls-runtime`, `trusted-proxies` | Credentials, authentication, authorization, encryption, key management, TLS, and security contracts. |
| Protocols and contracts | `extension-schema`, `madmin`, `protos`, `protocols`, `s3-ops`, `s3-types`, `s3select-api`, `s3select-query` | Admin, inter-node, S3, S3 Select, and optional protocol contracts. |
| Operations and integration | `audit`, `notify`, `obs`, `targets`, `zip` | Auditing, observability, event delivery, notification targets, and archive support. |
| Test support | `e2e_test`, `test-utils` | End-to-end validation and shared test bootstrap utilities. |

The `rustfs` binary crate composes these libraries into the running server.
`ecstore` remains the storage engine at the architectural center; its internal
module split is tracked under `docs/architecture/`.

## Architecture Invariants

> These are rules that the codebase should follow. Some are currently violated
> (marked with ⚠️). Documenting them here makes the violations explicit and
> trackable.

1. **Layers flow downward.** Server → Admin/App → Storage → ecstore → rio/io-core.
   No upward imports.

2. **Leaf crates have zero internal dependencies.** `config`, `credentials`, `crypto`,
   `io-metrics`, and `madmin` should depend only on external crates.
   - ✅ RESOLVED: the historical `utils → config` and `common → filemeta`/`madmin`
     edges were removed; do not reintroduce them (see Known Structural Issues).

3. **Each type has exactly one definition.** Types shared across crates must be defined
   in one crate and re-exported or imported by others.
   - ⚠️ VIOLATED: `ReplicationStats` (4 copies), `LastMinuteLatency` (3 copies),
     `BackpressureConfig` (3 copies), `DataUsageInfo` (2 copies).

4. **ecstore does not know about HTTP or S3 protocol details.** It operates on
   storage-level abstractions (objects, buckets, disks, pools).

5. **The `rustfs` binary crate is the only place that wires everything together.**
   Individual crates should be testable in isolation.

6. **Error types use `thiserror` with descriptive names** (e.g., `StorageError`,
   not bare `Error`).
   - ⚠️ VIOLATED: 6 crates use `pub enum Error`; 2 crates use `snafu`;
      `heal` use `anyhow` in library code.

## Known Structural Issues

> This section documents known problems in the current architecture.
> It exists so the team can track and address them deliberately.

### Critical

- **common/scanner code duplication (~3K lines).** `scanner` depends on `common`
  but maintains its own copies of `DataUsageInfo`, `LastMinuteLatency`, and related
  types instead of importing them.

- **ecstore is a monolith (87K lines, 163 files).** It contains disk management,
  bucket management, erasure coding, replication, lifecycle, RPC, and configuration
  — all in one crate. It should be decomposed along its existing subdirectories.

### High

- **Dependency inversions.** Historical `utils → config` and
  `common → filemeta/madmin` edges must stay removed so leaf/helper crates do
  not regain upward dependencies.

- **Three-layer BackpressureConfig/DeadlockConfig duplication** across io-core,
  concurrency, and `rustfs/src/storage`. Storage policies now expose and consume
  explicit projections into the concurrency/io-core policy shapes, and workload
  admission snapshots are composed through provider registries; later work
  should use those bridges before deleting compatibility wrappers.

### Medium

- **Inconsistent error handling.** Three strategies (thiserror/snafu/anyhow) and
  mixed naming (bare `Error` vs descriptive names).

- **Ambiguous common vs utils boundary.** Both described as "utilities and data
  structures." Need clear ownership rules.

## Cross-Cutting Concerns

### Error Handling

The project convention is `thiserror` for typed errors with descriptive names.
See `AGENTS.md`: "Prefer thiserror for library-facing error types."

```rust
// GOOD
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("disk not found: {0}")]
    DiskNotFound(String),
}

// AVOID
pub enum Error { ... }        // too generic
anyhow::Result<T>             // in library code (OK in tests/CLI)
```

### Logging & Tracing

- Use `tracing` crate (`info!`, `warn!`, `error!`, `debug!`, `trace!`)
- Structured fields: `tracing::info!(bucket = %name, "created bucket")`
- Spans for request-scoped context

### Metrics

- Prometheus-style metrics via `rustfs-obs` runtime and schema
- I/O-specific counters via `rustfs-io-metrics`
- Registration happens at crate level, collection/reporting in `rustfs-obs`

### Testing

- Unit tests: `#[cfg(test)] mod tests` in the same file
- Integration tests: inside respective crates (not top-level `tests/`)
- E2E tests: `crates/e2e_test/` — tests against a running server
- Run all: `make test` or `cargo nextest run`

## Startup Sequence

The binary (`main.rs`) boots in this order:

1. Environment variable compatibility (`MINIO_*` → `RUSTFS_*`)
2. Tokio runtime construction
3. CLI argument parsing
4. License, observability, TLS, trusted proxies initialization
5. Config parsing, server address resolution
6. Credentials, endpoints, local disks, lock client initialization
7. Capacity management initialization
8. HTTP server start (S3 API + optional console)
9. ECStore initialization (erasure coding storage engine)
10. Global config, background replication, KMS
11. Optional: FTP/FTPS/WebDAV servers
12. Event notifier, audit system, deadlock detector
13. Bucket metadata, IAM, Keystone, OIDC
14. Scanner and heal manager
15. Metrics system, mark `FullReady`
16. Wait for shutdown signal → graceful shutdown

## Dependency Diagram (Simplified)

```
                            ┌─────────┐
                            │  rustfs │  (binary + lib, 75K lines)
                            │  main   │
                            └────┬────┘
                                 │
                 ┌───────────────┼───────────────┐
                 │               │               │
            ┌────▼────┐     ┌────▼────┐   ┌──────▼─────┐
            │ server  │     │  admin  │   │    app     │
            │ (HTTP)  │     │(console)│   │(use-cases) │
            └────┬────┘     └────┬────┘   └──────┬─────┘
                 │               │               │
                 └───────────────┼───────────────┘
                                 │
                          ┌──────▼──────┐
                          │   storage   │
                          │ (ecfs, SSE, │
                          │  RPC, ACL)  │
                          └──────┬──────┘
                                 │
              ┌──────────────────┼──────────────────┐
              │                  │                  │
        ┌─────▼──────┐    ┌──────▼──────┐    ┌──────▼──────┐
        │  ecstore   │    │     rio     │    │   io-core   │
        │ (87K,core) │    │  (readers)  │    │ (zero-copy) │
        └─────┬──────┘    └─────────────┘    └─────────────┘
              │
     ┌─────┬──┼──┬─────┬──────┐
     │     │  │  │     │      │
 common utils config policy filemeta ...
```

## How to Navigate

- **"Where does S3 PutObject go?"**
  `server/` routes → `app/object_usecase` validates → `storage/ecfs` encodes →
  `ecstore` distributes → `rio` encrypts/compresses → `io-core` writes

- **"Where are bucket policies enforced?"**
  `app/bucket_usecase` calls into `crates/policy/`

- **"Where is replication configured?"**
  `admin/handlers/replication.rs` and `admin/handlers/site_replication.rs` for API,
  `ecstore/src/bucket/replication/` for engine

- **"Where do I add a new admin endpoint?"**
  Add handler in `admin/handlers/`, register in `admin/router.rs`

- **"Where do I add a new metric?"**
  Define descriptor/collector in `crates/obs/src/metrics/`, expose via `/minio/v2/metrics`

---

*Inspired by [matklad's ARCHITECTURE.md](https://matklad.github.io/2021/02/06/ARCHITECTURE.md.html)
and [rust-analyzer's architecture.md](https://github.com/rust-analyzer/rust-analyzer/blob/master/docs/book/src/contributing/architecture.md).*
