# RustFS KMS Developer API

This document targets developers extending RustFS or embedding the KMS primitives directly. The `rustfs-kms` crate exposes building blocks for configuration, backend orchestration, and data-key lifecycle management.

## Crate Overview

Add the crate to your workspace (already included in RustFS):

```toml
[dependencies]
rustfs-kms = { path = "crates/kms" }
```

Key namespaces:

| Module | Purpose |
|--------|---------|
| `rustfs_kms::config` | Typed configuration objects for local/vault backends. |
| `rustfs_kms::manager::KmsManager` | High-level coordinator that proxies operations to a backend. |
| `rustfs_kms::encryption::service::EncryptionService` | Frontend consumed by RustFS S3 handlers. |
| `rustfs_kms::backends` | Backend trait definitions and concrete implementations. |
| `rustfs_kms::types` | Request/response DTOs used by the REST handlers and manager. |
| `rustfs_kms::service_manager` | Async runtime that powers `/kms/configure`, `/kms/start`, etc. |

## Constructing a Configuration

```rust
use rustfs_kms::config::{BackendConfig, KmsBackend, KmsConfig, LocalConfig, VaultConfig, VaultAuthMethod};

let config = KmsConfig {
    backend: KmsBackend::Vault,
    backend_config: BackendConfig::Vault(VaultConfig {
        address: "https://vault.example.com:8200".parse().unwrap(),
        auth_method: VaultAuthMethod::Token { token: "s.XYZ".into() },
        namespace: None,
        mount_path: "transit".into(),
        kv_mount: "secret".into(),
        key_path_prefix: "rustfs/kms/keys".into(),
        tls: None,
    }),
    default_key_id: Some("rustfs-master".into()),
    timeout: std::time::Duration::from_secs(30),
    retry_attempts: 3,
    enable_cache: true,
    cache_config: Default::default(),
};
```

To build configurations from the admin REST payloads, use `api_types::ConfigureKmsRequest`:

```rust
use rustfs_kms::api_types::{ConfigureKmsRequest, ConfigureVaultKmsRequest};

let request = ConfigureKmsRequest::Vault(ConfigureVaultKmsRequest {
    address: "https://vault.example.com:8200".into(),
    auth_method: VaultAuthMethod::Token { token: "s.XYZ".into() },
    namespace: None,
    mount_path: Some("transit".into()),
    kv_mount: Some("secret".into()),
    key_path_prefix: Some("rustfs/kms/keys".into()),
    default_key_id: Some("rustfs-master".into()),
    skip_tls_verify: Some(false),
    timeout_seconds: Some(30),
    retry_attempts: Some(5),
    enable_cache: Some(true),
    max_cached_keys: Some(2048),
    cache_ttl_seconds: Some(600),
});

let kms_config: KmsConfig = (&request).into();
```

## Service Manager Lifecycle

The admin layer interacts with a `ServiceManager` singleton that wraps `KmsManager`:

```rust
use rustfs_kms::{init_global_kms_service_manager, get_global_kms_service_manager};

let manager = init_global_kms_service_manager();
manager.configure(config).await?;
manager.start().await?;

let status = manager.get_status().await; // -> KmsServiceStatus::Running
```

`get_global_encryption_service()` returns the `EncryptionService` façade that the S3 request handlers call. The service exposes async methods mirroring AWS KMS semantics:

```rust
use rustfs_kms::types::{CreateKeyRequest, KeyUsage, GenerateDataKeyRequest, KeySpec};
use rustfs_kms::get_global_encryption_service;

let service = get_global_encryption_service().await.expect("service not initialised");

let create = CreateKeyRequest {
    key_name: None,
    key_usage: KeyUsage::EncryptDecrypt,
    description: Some("project-alpha".into()),
    tags: Default::default(),
    origin: None,
    policy: None,
};
let created = service.create_key(create).await?;

let data_key = service
    .generate_data_key(GenerateDataKeyRequest {
        key_id: created.key_id.clone(),
        key_spec: KeySpec::Aes256,
        encryption_context: Default::default(),
    })
    .await?;
```

## Backend Integration Points

To add a custom backend:

1. Implement the `KmsBackend` trait (see `crates/kms/src/backends/mod.rs`).
2. Provide conversions from `ConfigureKmsRequest` into your backend’s config struct.
3. Register the backend in `BackendFactory` and extend the admin handlers to accept the new `backend_type` string.

The trait contract requires implementing methods such as `create_key`, `encrypt`, `decrypt`, `generate_data_key`, `list_keys`, and `health_check`.

```rust
#[async_trait::async_trait]
pub trait KmsBackend: Send + Sync {
    async fn create_key(&self, request: CreateKeyRequest) -> Result<CreateKeyResponse>;
    async fn encrypt(&self, request: EncryptRequest) -> Result<EncryptResponse>;
    async fn decrypt(&self, request: DecryptRequest) -> Result<DecryptResponse>;
    async fn generate_data_key(&self, request: GenerateDataKeyRequest) -> Result<GenerateDataKeyResponse>;
    async fn describe_key(&self, request: DescribeKeyRequest) -> Result<DescribeKeyResponse>;
    async fn list_keys(&self, request: ListKeysRequest) -> Result<ListKeysResponse>;
    async fn delete_key(&self, request: DeleteKeyRequest) -> Result<DeleteKeyResponse>;
    async fn cancel_key_deletion(&self, request: CancelKeyDeletionRequest) -> Result<CancelKeyDeletionResponse>;
    async fn health_check(&self) -> Result<bool>;
}
```

## Encryption Pipeline Helpers

`EncryptionService` contains two methods used by the S3 PUT/GET pipeline:

- `encrypt_stream` (invoked by `PutObject` and multipart uploads) obtains DEKs, encrypts payload chunks with AES-256-GCM, and returns headers.
- `decrypt_stream` resolves metadata, fetches the required DEK or customer key, and streams plaintext back to the client.

Both rely on `ObjectCipher` implementations defined in `crates/kms/src/encryption/ciphers.rs`. When adjusting chunk sizes or cipher suites, update these implementations and the SSE documentation.

## Testing Utilities

- `rustfs_kms::mock` contains in-memory backends used by unit tests.
- The e2e crate (`crates/e2e_test`) exposes helpers such as `LocalKMSTestEnvironment` and `VaultTestEnvironment` for integration testing.
- Run the full suite: `cargo test --workspace --exclude e2e_test` for unit coverage, `cargo test -p e2e_test kms:: -- --nocapture` for end-to-end validation.

## Error Handling Conventions

All public async methods return `rustfs_kms::error::Result<T>`. Errors are categorised as:

| Variant | Meaning |
|---------|---------|
| `KmsError::Configuration` | Invalid or missing backend configuration. |
| `KmsError::Backend`       | Underlying backend failure (Vault error, disk I/O, etc.). |
| `KmsError::Crypto`        | Integrity or cryptographic failure. |
| `KmsError::Cache`         | Cache lookup or eviction failure. |

Map these errors to HTTP responses using the helper macros in `rustfs/src/admin/handlers`.

---

For operational workflows continue with [http-api.md](http-api.md) and [dynamic-configuration-guide.md](dynamic-configuration-guide.md). For encryption semantics, see [sse-integration.md](sse-integration.md).
