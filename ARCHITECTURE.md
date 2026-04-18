# ARCHITECTURE.md

> Last updated: 2026-04-13 · Revision: 1 (draft)
>
> This document describes the high-level architecture of RustFS.
> If you want to familiarize yourself with the code base, you are in the right place!
>
> See also [CONTRIBUTING.md](CONTRIBUTING.md) for development workflow.

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
├── rustfs/                  # Main binary + library crate (75K lines)
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
├── crates/                  # 39 library crates
│   ├── ecstore/             # Erasure-coded storage engine (⚠️ 87K lines)
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

Crates are organized in a dependency DAG with 9 depth levels (0 = leaf, 8 = top):

```
Depth 0 — LEAF (no internal deps):
  appauth, checksums, config, credentials, crypto, io-metrics,
  madmin, s3-common, workers, zip

Depth 1:
  io-core (→ io-metrics)
  policy (→ config, credentials, crypto)
  utils (→ config)                        ⚠️ inverted: utils should be leaf

Depth 2:
  concurrency, filemeta, keystone, kms, lock, obs,
  signer, targets, trusted-proxies

Depth 3:
  common (→ filemeta, madmin)             ⚠️ inverted: common should be leaf

Depth 4:
  object-capacity, protos, rio

Depth 5 — CORE:
  ecstore (16 internal deps, 11 dependents — the architectural heart)

Depth 6:
  audit, heal, iam, metrics, notify, s3select-api, scanner

Depth 7:
  object-io, protocols, s3select-query

Depth 8 — TOP:
  rustfs (35 internal deps — the binary, depends on almost everything)
```

#### By Domain

**Core Infrastructure:**

| Crate | Lines | Purpose |
|-------|-------|---------|
| `config` | 3.3K | Configuration types and environment parsing |
| `utils` | 8.7K | Pure utilities (paths, compression, network, retry) |
| `common` | 4.4K | Shared runtime state, globals, data usage types, metrics |
| `madmin` | 5.5K | Admin API request/response types |

**I/O Pipeline:**

| Crate | Lines | Purpose |
|-------|-------|---------|
| `io-core` | 6.5K | Zero-copy I/O, buffer pool, direct I/O, scheduling, backpressure |
| `io-metrics` | 4.5K | I/O operation metrics and counters |
| `rio` | 6.9K | Composable reader chain (encrypt → compress → hash → limit) |
| `object-io` | 2.4K | High-level object read/write using rio + ecstore |
| `concurrency` | 1.8K | Concurrency control wrappers over io-core |

**Storage Engine:**

| Crate | Lines | Purpose |
|-------|-------|---------|
| `ecstore` | 87K | ⚠️ Erasure-coded storage: disks, pools, buckets, replication, lifecycle |
| `filemeta` | 10K | File/object metadata types and versioning |
| `checksums` | 732 | Checksum computation |
| `lock` | 7.1K | Distributed lock manager |
| `heal` | 5.9K | Data healing / bitrot repair |
| `scanner` | 5.4K | Background data usage scanner |
| `object-capacity` | 2.5K | Capacity tracking and management |

**Security & Auth:**

| Crate | Lines | Purpose |
|-------|-------|---------|
| `crypto` | 1.6K | Encryption primitives |
| `credentials` | 713 | Credential types (access key / secret key) |
| `signer` | 1.4K | S3 v4 request signing |
| `iam` | 9.0K | Identity and access management |
| `policy` | 8.8K | Policy engine (S3 bucket/IAM policies) |
| `kms` | 8.1K | Key management service integration |
| `keystone` | 1.9K | OpenStack Keystone auth |
| `appauth` | 143 | Application-level auth tokens |

**Protocol & API:**

| Crate | Lines | Purpose |
|-------|-------|---------|
| `protos` | 5.7K | Protobuf/gRPC definitions for inter-node RPC |
| `protocols` | 18K | FTP/FTPS, WebDAV, Swift API support |
| `s3-common` | 738 | Shared S3 types |
| `s3select-api` | 1.9K | S3 Select interface |
| `s3select-query` | 3.6K | S3 Select query engine |

**Observability:**

| Crate | Lines | Purpose |
|-------|-------|---------|
| `metrics` | 8.4K | Prometheus metric collectors |
| `io-metrics` | 4.5K | I/O-specific metrics |
| `obs` | 5.6K | OpenTelemetry tracing and telemetry |
| `audit` | 2.4K | Audit logging |

**Events:**

| Crate | Lines | Purpose |
|-------|-------|---------|
| `notify` | 5.5K | Event notification system |
| `targets` | 3.2K | Notification targets (Kafka, AMQP, webhook, etc.) |

**Other:**

| Crate | Lines | Purpose |
|-------|-------|---------|
| `trusted-proxies` | 4.0K | Trusted proxy / IP forwarding |
| `zip` | 986 | ZIP archive support for bulk downloads |
| `workers` | 136 | Simple worker abstraction |

## Architecture Invariants

> These are rules that the codebase should follow. Some are currently violated
> (marked with ⚠️). Documenting them here makes the violations explicit and
> trackable.

1. **Layers flow downward.** Server → Admin/App → Storage → ecstore → rio/io-core.
   No upward imports.

2. **Leaf crates have zero internal dependencies.** `config`, `credentials`, `crypto`,
   `io-metrics`, `madmin`, `s3-common` should depend only on external crates.
   - ⚠️ VIOLATED: `utils` depends on `config`, `common` depends on `filemeta` and `madmin`.

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

- **Dependency inversions.** `utils → config` and `common → filemeta/madmin` break
  the layering model. These need to be untangled.

- **Three-layer BackpressureConfig/DeadlockConfig duplication** across io-core,
  concurrency, and rustfs/storage. Should be defined once with builder/composition.

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
            ┌────▼────┐    ┌────▼────┐    ┌─────▼─────┐
            │ server  │    │  admin  │    │    app     │
            │ (HTTP)  │    │(console)│    │(use-cases) │
            └────┬────┘    └────┬────┘    └─────┬─────┘
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
        ┌─────▼─────┐    ┌──────▼──────┐    ┌──────▼──────┐
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
