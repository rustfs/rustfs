# RustFS Key Management Service

The RustFS Key Management Service (KMS) provides end-to-end key orchestration, envelope encryption, and S3-compatible semantics for encrypted object storage. It sits between the RustFS API surface and the underlying encryption primitives, ensuring that data at rest and in flight remains protected while keeping operational workflows simple.

## Highlights

- **Multiple backends** – plug in Vault for production or use the Local filesystem backend for development and CI.
- **Envelope encryption** – master keys protect data-encryption keys (DEKs); DEKs protect object payloads with AES-256-GCM streaming.
- **S3 compatibility** – works transparently with `SSE-S3`, `SSE-KMS`, and `SSE-C` headers so existing tools continue to function.
- **Dynamic lifecycle** – configure, rotate, or swap backends at runtime by calling the admin REST API; no server restart is required.
- **Caching & resilience** – built-in caching minimises latency, while health probes, retries, and metrics help operators keep track of the service.

## Architecture

```
       ┌──────────────────────────────────────────────────────────┐
       │                       RustFS Frontend                     │
       │  (S3 compatible API, IAM, policy engine, bucket logic)    │
       └──────────────┬───────────────────────────────────────────┘
                      │
                      ▼
            ┌──────────────────────────────┐
            │  Encryption Service Manager  │
            │  • Applies admin config      │
            │  • Controls backend runtime  │
            │  • Exposes metrics / health  │
            └──────────────┬──────────────┘
                           │
                 ┌─────────┴─────────┐
                 │                   │
                 ▼                   ▼
        ┌────────────────┐   ┌────────────────────┐
        │ Local Backend  │   │ Vault Backend       │
        │ • File-based   │   │ • Transit engine    │
        │ • Dev / CI     │   │ • Production ready  │
        └────────────────┘   └────────────────────┘
```

### Components at a Glance

| Component                     | Responsibility                                                         |
|------------------------------|-------------------------------------------------------------------------|
| `rustfs::kms::manager`       | Owns backend lifecycle, caching, and key orchestration.                |
| `rustfs::kms::encryption`    | Encrypts/decrypts payloads, issues data keys, validates headers.       |
| Admin REST handlers          | Accept configuration requests (`configure`, `start`, `status`, etc.). |
| Backends                     | `local` (filesystem) and `vault` (Transit) implementations.            |

## Supported Backends

| Backend | When to use | Key storage | Authentication | Notes |
|---------|-------------|-------------|----------------|-------|
| Local   | Development, CI, integration tests | JSON-encoded key blobs on disk | none | Simple, fast to bootstrap, not secure for production. |
| Vault   | Production or pre-production | Vault Transit & KV engines | token or AppRole | Supports rotation, audit logging, sealed-state recovery, TLS. |

Refer to [configuration.md](configuration.md) for static configuration details and [dynamic-configuration-guide.md](dynamic-configuration-guide.md) for the runtime workflow.

## Encryption Workflows

RustFS KMS supports the same S3 semantics users expect:

- **SSE-S3** – RustFS manages the data key lifecycle and returns the `x-amz-server-side-encryption` header.
- **SSE-KMS** – RustFS issues per-object data keys bound to the configured KMS backend, exposing the `x-amz-server-side-encryption` header with value `aws:kms`.
- **SSE-C** – Clients provide a 256-bit key and MD5 checksum per request; RustFS uses KMS to encrypt metadata, while encrypted payloads are streamed with the customer key.

Internally, every object follows the envelope-encryption flow below:

1. Determine the logical key-id (default, explicit header, or SSE-C customer key).
2. Ask the configured backend for a DEK or encryption context.
3. Stream-encrypt the payload with AES-256-GCM (1 MiB chunking, authenticated headers).
4. Persist metadata (IV, checksum, key-id) alongside object state.
5. During GET/HEAD, the same process runs in reverse with integrity checks.

## Quick Start

1. **Build RustFS** – `cargo build --release` or run the project-specific build helper.
2. **Prepare credentials** – ensure you have admin access keys; for Vault, export `VAULT_ADDR` and a root or scoped token.
3. **Launch RustFS** – `./target/release/rustfs server` (KMS starts in `NotConfigured`).
4. **Configure the backend**:

   ```bash
   # Local backend (ephemeral testing)
   awscurl --service s3 --region us-east-1 \
     --access_key admin --secret_key admin \
     -X POST -d '{
       "backend_type": "local",
       "key_dir": "/var/lib/rustfs/kms-keys",
       "default_key_id": "rustfs-master"
     }' \
     http://localhost:9000/rustfs/admin/v3/kms/configure
   
   awscurl --service s3 --region us-east-1 \
     --access_key admin --secret_key admin \
     -X POST http://localhost:9000/rustfs/admin/v3/kms/start
   ```

   ```bash
   # Vault backend (production)
   awscurl --service s3 --region us-east-1 \
     --access_key admin --secret_key admin \
     -X POST -d '{
       "backend_type": "vault",
       "address": "https://vault.example.com:8200",
       "auth_method": {
         "token": "s.XYZ..."
       },
       "mount_path": "transit",
       "kv_mount": "secret",
       "key_path_prefix": "rustfs/kms/keys",
       "default_key_id": "rustfs-master"
     }' \
     https://rustfs.example.com/rustfs/admin/v3/kms/configure
   
   awscurl --service s3 --region us-east-1 \
     --access_key admin --secret_key admin \
     -X POST https://rustfs.example.com/rustfs/admin/v3/kms/start
   ```

5. **Verify**:

   ```bash
   awscurl --service s3 --region us-east-1 \
     --access_key admin --secret_key admin \
     http://localhost:9000/rustfs/admin/v3/kms/status
   ```

   The response should include `"status": "Running"` and the configured backend summary.

## Documentation Map

| Topic | Description |
|-------|-------------|
| [http-api.md](http-api.md) | Formal REST endpoint reference with request/response samples. |
| [dynamic-configuration-guide.md](dynamic-configuration-guide.md) | Gradual rollout, rotation, and failover playbooks. |
| [configuration.md](configuration.md) | Static configuration files, environment variables, Helm/Ansible hints. |
| [api.md](api.md) | Rust crate interfaces (`ConfigureKmsRequest`, `KmsManager`, encryption helpers). |
| [sse-integration.md](sse-integration.md) | Mapping between S3 headers and RustFS behaviour, client examples. |
| [security.md](security.md) | Threat model, access control, TLS, auditing, secrets hygiene. |
| [test_suite_integration.md](test_suite_integration.md) | Running e2e, Vault, and regression test suites. |
| [troubleshooting.md](troubleshooting.md) | Common errors and recovery steps. |

## Terminology

| Term | Definition |
|------|------------|
| **KMS backend** | Implementation that holds master keys (Local filesystem or Vault Transit/ KV). |
| **Master Key** | Root key stored in the backend; encrypts data keys. |
| **Data Encryption Key (DEK)** | Per-object key that encrypts payload chunks. |
| **Envelope Encryption** | Wrapping DEKs with a higher-level key before persisting. |
| **SSE-S3 / SSE-KMS / SSE-C** | Amazon S3-compatible encryption modes supported by RustFS. |

For deeper dives continue with the documents referenced above. EOF
