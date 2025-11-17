# KMS Admin HTTP API Reference

The RustFS KMS admin API is exposed under the admin prefix (`/rustfs/admin/v3`). Requests must be signed with SigV4 credentials that have the `ServerInfoAdminAction` permission. All request and response bodies use JSON, and all endpoints return standard HTTP status codes.

- Base URL examples: `http://localhost:9000/rustfs/admin/v3`, `https://rustfs.example.com/rustfs/admin/v3`.
- Headers: set `Content-Type: application/json` for requests with bodies.
- Authentication: sign with SigV4 (`awscurl`, `aws-signature-v4`, or the official SDKs).

## Service Lifecycle

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/kms/configure`    | POST | Apply the initial backend configuration. Does not start the service. |
| `/kms/reconfigure`  | POST | Merge a new configuration on top of the existing one. |
| `/kms/start`        | POST | Start the configured backend. |
| `/kms/stop`         | POST | Stop the backend; configuration is kept. |
| `/kms/status`       | GET  | Lightweight status summary (`Running`, `Configured`, etc.). |
| `/kms/service-status` | GET | Backward-compatible alias for `/kms/status`. |
| `/kms/config`       | GET  | Returns the cached configuration summary. |
| `/kms/clear-cache`  | POST | Clears in-memory DEK and metadata caches. |

### Configure / Reconfigure

**Request**
```json
{
  "backend_type": "vault",
  "address": "https://vault.example.com:8200",
  "auth_method": { "token": "s.XYZ" },
  "mount_path": "transit",
  "kv_mount": "secret",
  "key_path_prefix": "rustfs/kms/keys",
  "default_key_id": "rustfs-master",
  "enable_cache": true,
  "cache_ttl_seconds": 600,
  "timeout_seconds": 30,
  "retry_attempts": 3
}
```

**Response**
```json
{
  "success": true,
  "message": "KMS configured successfully",
  "status": "Configured"
}
```

> **Partial updates:** `/kms/reconfigure` updates only the fields present in the payload. Use this to rotate tokens or adjust cache parameters without resubmitting the full configuration.

### Start / Stop

**Start response**
```json
{
  "success": true,
  "message": "KMS service started successfully",
  "status": "Running"
}
```

**Stop response**
```json
{
  "success": true,
  "message": "KMS service stopped successfully",
  "status": "Configured"
}
```

### Status & Config

`GET /kms/status`
```json
{
  "status": "Running",
  "backend_type": "vault",
  "healthy": true,
  "config_summary": {
    "backend_type": "vault",
    "default_key_id": "rustfs-master",
    "timeout_seconds": 30,
    "retry_attempts": 3,
    "enable_cache": true,
    "cache_summary": {
      "max_keys": 1024,
      "ttl_seconds": 600,
      "enable_metrics": true
    },
    "backend_summary": {
      "backend_type": "vault",
      "address": "https://vault.example.com:8200",
      "auth_method_type": "token",
      "namespace": null,
      "mount_path": "transit",
      "kv_mount": "secret",
      "key_path_prefix": "rustfs/kms/keys"
    }
  }
}
```

`GET /kms/config`
```json
{
  "backend": "vault",
  "cache_enabled": true,
  "cache_max_keys": 1024,
  "cache_ttl_seconds": 600,
  "default_key_id": "rustfs-master"
}
```

`POST /kms/clear-cache` returns HTTP `204` with an empty body when successful.

## Key Management

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/kms/keys`                | POST | Create a new master key in the backend. |
| `/kms/keys`                | GET  | List master keys (paginated). |
| `/kms/keys/{key_id}`       | GET  | Retrieve metadata for a specific key. |
| `/kms/keys/delete`         | DELETE | Schedule key deletion. |
| `/kms/keys/cancel-deletion` | POST | Cancel a pending deletion request. |

### Create Key

**Request**
```json
{
  "KeyUsage": "ENCRYPT_DECRYPT",
  "Description": "project-alpha",
  "Tags": {
    "owner": "security",
    "env": "prod"
  }
}
```

**Response**
```json
{
  "key_id": "fa5bac0e-2a2c-4f9a-a09d-2f5b8a59ed85",
  "key_metadata": {
    "key_id": "fa5bac0e-2a2c-4f9a-a09d-2f5b8a59ed85",
    "description": "project-alpha",
    "enabled": true,
    "key_usage": "ENCRYPT_DECRYPT",
    "creation_date": "2024-09-18T07:10:42.012345Z",
    "rotation_enabled": false
  }
}
```

### List Keys

`GET /kms/keys?limit=50&marker=<token>`
```json
{
  "keys": [
    { "key_id": "fa5bac0e-2a2c-4f9a-a09d-2f5b8a59ed85", "description": "project-alpha" }
  ],
  "truncated": false,
  "next_marker": null
}
```

### Describe Key

`GET /kms/keys/fa5bac0e-2a2c-4f9a-a09d-2f5b8a59ed85`
```json
{
  "key_metadata": {
    "key_id": "fa5bac0e-2a2c-4f9a-a09d-2f5b8a59ed85",
    "description": "project-alpha",
    "enabled": true,
    "key_usage": "ENCRYPT_DECRYPT",
    "creation_date": "2024-09-18T07:10:42.012345Z",
    "deletion_date": null
  }
}
```

### Delete & Cancel Deletion

**Delete request**
```json
{
  "key_id": "fa5bac0e-2a2c-4f9a-a09d-2f5b8a59ed85",
  "pending_window_in_days": 7
}
```

**Cancel deletion**
```json
{
  "key_id": "fa5bac0e-2a2c-4f9a-a09d-2f5b8a59ed85"
}
```

Both endpoints respond with the updated `key_metadata`.

## Data Key Operations

`POST /kms/generate-data-key`

**Request**
```json
{
  "key_id": "fa5bac0e-2a2c-4f9a-a09d-2f5b8a59ed85",
  "key_spec": "AES_256",
  "encryption_context": {
    "bucket": "analytics-data",
    "object": "2024/09/18/report.parquet"
  }
}
```

**Response**
```json
{
  "key_id": "fa5bac0e-2a2c-4f9a-a09d-2f5b8a59ed85",
  "plaintext_key": "sQW6qt0yS7CqD6c8hY7GZg==",
  "ciphertext_blob": "gAAAAABlLK..."
}
```

- `plaintext_key` is Base64-encoded and must be zeroised after use.
- `ciphertext_blob` can be stored alongside object metadata for future re-wraps.

## Error Handling

| Code | Meaning | Example payload |
|------|---------|-----------------|
| `400 Bad Request` | Malformed JSON or missing required fields. | `{ "code": "InvalidRequest", "message": "invalid JSON" }` |
| `401 Unauthorized` | Request was not signed or credentials are invalid. | `{ "code": "AccessDenied", "message": "authentication required" }` |
| `403 Forbidden` | Caller lacks admin permissions. | `{ "code": "AccessDenied", "message": "unauthorised" }` |
| `409 Conflict` | Backend already configured in an incompatible way. | `{ "code": "Conflict", "message": "KMS already running" }` |
| `500 Internal Server Error` | Backend failure or transient issue. Logs include details. | `{ "code": "InternalError", "message": "failed to create key: ..." }` |

## Useful Utilities

- [`awscurl`](https://github.com/okigan/awscurl) for quick SigV4 requests.
- The `scripts/` directory contains example shell scripts to configure local and Vault backends automatically.
- The e2e test harness (`cargo test -p e2e_test kms:: -- --nocapture`) demonstrates end-to-end API usage against both backends.

For dynamic workflows and automation strategies, continue with [dynamic-configuration-guide.md](dynamic-configuration-guide.md).
