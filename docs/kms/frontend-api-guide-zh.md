# RustFS KMS Frontend Integration Guide

This document targets frontend engineers who need to integrate with the RustFS Key Management Service (KMS). It provides a complete API reference, usage notes, and example implementations.

## 📋 Contents

1. [Quick Start](#quick-start)
2. [Authentication & Permissions](#authentication--permissions)
3. [API Catalog](#api-catalog)
4. [Service Management APIs](#service-management-apis)
5. [Key Management APIs](#key-management-apis)
6. [Data Encryption APIs](#data-encryption-apis)
7. [Bucket Encryption Configuration APIs](#bucket-encryption-configuration-apis)
8. [Monitoring & Cache APIs](#monitoring--cache-apis)
9. [Common Error Codes](#common-error-codes)
10. [Data Types](#data-types)
11. [Implementation Examples](#implementation-examples)

## Quick Start

### Base configuration

| Setting | Value |
|---------|-------|
| **Base URL** | `http://localhost:9000/rustfs/admin/v3` (local development) |
| **Production URL** | `https://your-rustfs-domain.com/rustfs/admin/v3` |
| **Request format** | `application/json` |
| **Response format** | `application/json` |
| **Authentication** | AWS Signature Version 4 |
| **Encoding** | UTF-8 |

### Common request headers

| Header | Required | Value |
|--------|----------|-------|
| `Content-Type` | ✅ | `application/json` |
| `Authorization` | ✅ | `AWS4-HMAC-SHA256 Credential=...` |
| `X-Amz-Date` | ✅ | ISO 8601 timestamp |

## Authentication & Permissions

### Required IAM permissions

Clients must have `ServerInfoAdminAction` to invoke KMS APIs.

### AWS SigV4 signing

All requests must be signed with SigV4.

- **Access Key ID** – account access key
- **Secret Access Key** – corresponding secret key
- **Region** – `us-east-1`
- **Service** – `execute-api`

## API Catalog

### Service management

| Method | Path | Description | Status |
|--------|------|-------------|--------|
| `POST` | `/kms/configure` | Configure the KMS service | ✅ Available |
| `POST` | `/kms/start` | Start the service | ✅ Available |
| `POST` | `/kms/stop` | Stop the service | ✅ Available |
| `GET` | `/kms/service-status` | Retrieve service status | ✅ Available |
| `POST` | `/kms/reconfigure` | Reconfigure and restart | ✅ Available |

### Key management

| Method | Path | Description | Status |
|--------|------|-------------|--------|
| `POST` | `/kms/keys` | Create a master key | ✅ Available |
| `GET` | `/kms/keys` | List keys | ✅ Available |
| `GET` | `/kms/keys/{key_id}` | Get key metadata | ✅ Available |
| `DELETE` | `/kms/keys/delete` | Schedule key deletion | ✅ Available |
| `POST` | `/kms/keys/cancel-deletion` | Cancel key deletion | ✅ Available |

### Data encryption

| Method | Path | Description | Status |
|--------|------|-------------|--------|
| `POST` | `/kms/generate-data-key` | Generate a data key | ✅ Available |
| `POST` | `/kms/decrypt` | Decrypt a data key | ⚠️ Not implemented |

### Bucket encryption configuration

| Method | Path | Description | Status |
|--------|------|-------------|--------|
| `GET` | `/api/v1/buckets` | List buckets | ✅ Available |
| `GET` | `/api/v1/bucket-encryption/{bucket}` | Get default encryption | ✅ Available |
| `PUT` | `/api/v1/bucket-encryption/{bucket}` | Set default encryption | ✅ Available |
| `DELETE` | `/api/v1/bucket-encryption/{bucket}` | Remove default encryption | ✅ Available |

### Monitoring & cache

| Method | Path | Description | Status |
|--------|------|-------------|--------|
| `GET` | `/kms/config` | Retrieve KMS configuration | ✅ Available |
| `POST` | `/kms/clear-cache` | Clear the KMS cache | ✅ Available |

### Legacy compatibility endpoints

| Method | Path | Description | Status |
|--------|------|-------------|--------|
| `POST` | `/kms/create-key` | Create key (legacy) | ✅ Available |
| `GET` | `/kms/describe-key` | Describe key (legacy) | ✅ Available |
| `GET` | `/kms/list-keys` | List keys (legacy) | ✅ Available |
| `GET` | `/kms/status` | KMS status (legacy) | ✅ Available |

> ✅ **Available** – implemented and usable.  
> ⚠️ **Not implemented** – API shape defined but backend missing.  
> Prefer the new endpoints; legacy routes exist for backwards compatibility.

## Service Management APIs

### 1. Configure KMS

`POST /kms/configure`

Parameters:

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `backend_type` | string | ✅ | `"local"` or `"vault"` |
| `key_directory` | string | Cond. | Local backend key directory |
| `default_key_id` | string | ✅ | Default master key ID |
| `enable_cache` | boolean | ❌ | Toggle cache (default `true`) |
| `cache_ttl_seconds` | integer | ❌ | Cache TTL (default `600`) |
| `timeout_seconds` | integer | ❌ | Operation timeout (default `30`) |
| `retry_attempts` | integer | ❌ | Retry attempts (default `3`) |
| `address` | string | Cond. | Vault server address |
| `auth_method` | object | Cond. | Vault auth config |
| `mount_path` | string | Cond. | Vault transit mount path |
| `kv_mount` | string | Cond. | Vault KV mount |
| `key_path_prefix` | string | Cond. | Vault key prefix |

Vault `auth_method` fields:

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `token` | string | ✅ | Vault token |

Response
```json
{
  "success": boolean,
  "message": string,
  "config_id": string?
}
```

### 2. Start KMS

`POST /kms/start`

Response fields: `success`, `message`, `status` (`Running`, `Stopped`, `Error`).

### 3. Stop KMS

`POST /kms/stop`

Same response structure as `/kms/start`.

### 4. Service status

`GET /kms/service-status`

Response
```json
{
  "status": "Running" | "Stopped" | "NotConfigured" | "Error",
  "backend_type": "local" | "vault",
  "healthy": boolean,
  "config_summary": {
    "backend_type": string,
    "default_key_id": string,
    "timeout_seconds": integer,
    "retry_attempts": integer,
    "enable_cache": boolean
  }
}
```

### 5. Reconfigure

`POST /kms/reconfigure`

Accepts the same payload as `/kms/configure` and restarts the service.

## Key Management APIs

### 1. Create key

`POST /kms/keys`

Parameters:

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `KeyUsage` | string | ✅ | `"ENCRYPT_DECRYPT"` |
| `Description` | string | ❌ | Description (≤256 chars) |
| `Tags` | object | ❌ | Key/value tag map |

Response includes `key_id` and `key_metadata` (enabled, usage, creation date, etc.).

### 2. Key metadata

`GET /kms/keys/{key_id}` returns the `key_metadata` object.

### 3. List keys

`GET /kms/keys?limit=&marker=` with pagination support.

### 4. Schedule deletion

`DELETE /kms/keys/delete`

Parameters: `key_id`, optional `pending_window_in_days` (7–30, default 7).

### 5. Cancel deletion

`POST /kms/keys/cancel-deletion`

Provide `key_id`; response returns updated metadata with `deletion_date = null`.

## Data Encryption APIs

### 1. Generate data key

`POST /kms/generate-data-key`

Parameters: `key_id`, optional `key_spec` (`AES_256` or `AES_128`), optional `encryption_context` map.

Response contains `plaintext_key` (Base64) and `ciphertext_blob` (Base64).

### 2. Decrypt data key

`POST /kms/decrypt`

> ⚠️ Not yet implemented. Expect parameters `ciphertext_blob` and optional `encryption_context`. A future response will expose `key_id` and `plaintext`.

## Bucket Encryption Configuration APIs

RustFS exposes S3-compatible endpoints via the AWS SDK.

### 1. List buckets

Use `ListBuckets` from the AWS SDK.

### 2. Get default encryption

`GetBucketEncryption` returns SSE rules (`SSEAlgorithm`, optional `KMSMasterKeyID`). A 404 indicates no configuration.

### 3. Set default encryption

`PutBucketEncryption` supports SSE-S3 (`AES256`) or SSE-KMS (`aws:kms` + key ID).

### 4. Delete default encryption

`DeleteBucketEncryption` removes the configuration.

Example composable and helper utilities are provided in the original Chinese document; port them as needed.

## Monitoring & Cache APIs

### 1. Get KMS config

`GET /kms/config` returns backend, cache settings, and default key ID.

### 2. Clear cache

`POST /kms/clear-cache` invalidates cached key metadata.

### 3. Legacy status

`GET /kms/status` (legacy) provides cache hit/miss stats.

## Common Error Codes

### HTTP status codes

| Code | Error | Description |
|------|-------|-------------|
| 200 | – | Success |
| 400 | `InvalidRequest` | Bad request or parameters |
| 401 | `AccessDenied` | Authentication failure |
| 403 | `AccessDenied` | Authorization failure |
| 404 | `NotFound` | Resource not found |
| 409 | `Conflict` | Resource conflict |
| 500 | `InternalError` | Server error |

### Error payload

```json
{
  "error": {
    "code": string,
    "message": string,
    "request_id": string?
  }
}
```

### Specific codes

- `InvalidRequest` – check payload
- `AccessDenied` – verify credentials/permissions
- `KeyNotFound` – key ID incorrect
- `InvalidKeyState` – key disabled or invalid
- `ServiceNotConfigured` – configure KMS first
- `ServiceNotRunning` – start the service
- `BackendError` – backend failure
- `EncryptionFailed` / `DecryptionFailed` – inspect ciphertext/context

## Data Types

### `KeyMetadata`

| Field | Type | Description |
|-------|------|-------------|
| `key_id` | string | UUID |
| `description` | string | Key description |
| `enabled` | boolean | Whether the key is enabled |
| `key_usage` | string | Always `ENCRYPT_DECRYPT` |
| `creation_date` | string | ISO 8601 timestamp |
| `rotation_enabled` | boolean | Rotation status |
| `deletion_date` | string? | Scheduled deletion timestamp |

### `ConfigSummary`

| Field | Type | Description |
|-------|------|-------------|
| `backend_type` | string | `local` or `vault` |
| `default_key_id` | string | Default master key |
| `timeout_seconds` | integer | Operation timeout |
| `retry_attempts` | integer | Retry attempts |
| `enable_cache` | boolean | Cache toggle |

### Enumerations

- `ServiceStatus` – `Running`, `Stopped`, `NotConfigured`, `Error`
- `BackendType` – `local`, `vault`
- `KeyUsage` – `ENCRYPT_DECRYPT`
- `KeySpec` – `AES_256`, `AES_128`

## Implementation Examples

The original guide included extensive code samples covering bucket encryption flows, Vue/React composables, and full application scaffolding. The key patterns are:

1. **Signed requests** – Use AWS SigV4 (via AWS SDK or manual signing) to call `/rustfs/admin/v3` endpoints.
2. **Multipart encryption flow** – Request a data key, encrypt data locally, upload ciphertext, and store the encrypted key blob.
3. **Bucket encryption lifecycle** – Use the S3 SDK to configure default SSE policies, optionally provisioning dedicated KMS keys per bucket.
4. **Health monitoring** – Periodically poll `/kms/status` or `/kms/config` to ensure the service is healthy and cache hit ratios remain acceptable.

## Troubleshooting & Support

If issues arise:

1. Verify the KMS service is healthy via `/kms/service-status`.
2. Confirm Vault or local backend configuration.
3. Inspect server logs for detailed error messages.
4. Run `cargo test -p e2e_test kms:: -- --nocapture` to validate the setup.
5. Ensure your AWS SDK version supports the required S3/KMS calls.

Common questions:

- **Bucket encryption fails with insufficient permissions** – Ensure the IAM policy grants `s3:GetBucketEncryption`, `s3:PutBucketEncryption`, `s3:DeleteBucketEncryption`, and (for SSE-KMS) `kms:DescribeKey`.
- **Unable to select a KMS key** – Confirm the KMS service is running, the key is enabled, and `KeyUsage` is `ENCRYPT_DECRYPT`.
- **Frontend shows incorrect encryption state** – A 404 during `GetBucketEncryption` is normal (no configuration). Allow for network latency before refreshing the status.

---

_Last updated: 2024-09-22_
