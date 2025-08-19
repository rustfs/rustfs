# RustFS KMS and Server-Side Encryption (SSE)

This document explains how to configure the Key Management Service (KMS) for RustFS, manage keys, and use S3-compatible Server-Side Encryption (SSE) for objects. It covers both Vault Transit and Local backends, encryption context (AAD), and practical curl examples.

## Overview

- Backends: Vault Transit (recommended for production), Local (development/testing).
- Defaults:
  - Vault transit mount path: transit
  - AES-256-GCM is used to encrypt object data; KMS manages the data key (DEK).
- Encryption context (AAD): RustFS canonically builds a JSON map containing at least bucket and key to bind ciphertext to object identity. You can also provide extra AAD via request header.

Security and authentication
- Admin APIs under /rustfs/admin/v3/... require AWS SigV4.
- Use AK/SK that match RustFS server config (service usually "s3"). Missing or invalid signatures return 403 AccessDenied.

## Configure KMS

Endpoint:
- POST /rustfs/admin/v3/kms/configure

Request body fields
- kms_type: string, required. One of:
  - "vault": HashiCorp Vault Transit engine
  - "local": built-in local KMS (dev/test)
- vault_address: string, required when kms_type=vault. Vault HTTP(S) URL.
- vault_token: string, optional. Use Token auth when provided.
- vault_app_role_id: string, optional. Used with vault_app_role_secret_id for AppRole auth.
- vault_app_role_secret_id: string, optional. Used with vault_app_role_id for AppRole auth.
- vault_namespace: string, optional. Vault Enterprise namespace; omit for root.
- vault_mount_path: string, optional, default "transit". Transit engine mount name (not a KV path).
- vault_timeout_seconds: integer, optional, default 30. Timeout for Vault requests.
- default_key_id: string, optional. Default master key for SSE-KMS when no key is specified. If omitted, RustFS falls back to "rustfs-default-key" and will lazily create it on first use when permitted.

Environment variable mapping
- RUSTFS_KMS_DEFAULT_KEY_ID → default_key_id

Auth selection (automatic)
- When both vault_app_role_id and vault_app_role_secret_id are present → AppRole auth.
- Else if vault_token is present → Token auth.
- Otherwise → Invalid configuration.

Body (Vault with Token):
```json
{
  "kms_type": "vault",
  "vault_address": "https://vault.example.com",
  "vault_token": "s.xxxxx",
  "vault_namespace": "optional-namespace",
  "vault_mount_path": "transit",
  "vault_timeout_seconds": 30
}
```

Body (Vault with AppRole):
```json
{
  "kms_type": "vault",
  "vault_address": "https://vault.example.com",
  "vault_app_role_id": "role-id",
  "vault_app_role_secret_id": "secret-id",
  "vault_mount_path": "transit"
}
```

Body (Local):
```json
{
  "kms_type": "local"
}
```

Status and health:
- GET /rustfs/admin/v3/kms/status → { status: OK|Degraded|Failed, backend, healthy }
  - OK: KMS reachable and can generate data keys
  - Degraded: KMS reachable but encryption path not verified
  - Failed: not reachable
  - Note: Fresh setups with no keys yet still report usable; Transit not mounted or Vault sealed reports failure.
 - GET /rustfs/admin/v3/kms/config → returns current KMS configuration (sanitized, without secrets). Example:
   {
     "kms_type": "Vault",
     "default_key_id": null,
     "timeout_secs": 30,
     "retry_attempts": 3,
     "enable_audit": true,
     "audit_log_path": null,
     "backend": {
       "type": "vault",
       "address": "http://localhost:8200",
       "namespace": null,
       "mount_path": "transit",
       "auth_method": "token"
     }
   }

## Key Management APIs

- Create key: POST /rustfs/admin/v3/kms/key/create
  - Recommended: pass parameters in JSON body
    {
      "keyName": "<id>",
      "algorithm": "AES-256"
    }
  - Backward compatible: query params `?keyName=<id>&algorithm=AES-256` still work
- Key status: GET /rustfs/admin/v3/kms/key/status?keyName=<id>
- List keys: GET /rustfs/admin/v3/kms/key/list
- Enable key: PUT /rustfs/admin/v3/kms/key/enable?keyName=<id>
- Disable key: PUT /rustfs/admin/v3/kms/key/disable?keyName=<id>
  - Vault limitation: Transit does not support disabling keys; RustFS returns 501 with guidance.
- Rotate key: POST /rustfs/admin/v3/kms/key/rotate?keyName=<id>
- Rewrap ciphertext: POST /rustfs/admin/v3/kms/rewrap (body: {"ciphertext_b64":"...","context":{...}})
 - Delete key: DELETE /rustfs/admin/v3/kms/key/delete?keyName=<id>[&pendingWindowDays=7]
   - Schedules deletion when the backend supports it; Vault transit performs immediate deletion and does not support cancellation.

Parameters and options
- keyName: string, required. Master key ID (Transit key name). Use human-readable IDs like "app-default".
- algorithm: string, optional, default "AES-256". Supported values:
  - "AES-256", "AES-128", "RSA-2048", "RSA-4096"
  - Hint: with Vault Transit, actual key_type is defined by the engine (commonly aes256-gcm96). RustFS aligns metadata and sanity checks, but Vault’s definition is authoritative.

API details highlights
- key/list returns an empty list when Vault has no keys yet (404 is treated as empty, not an error).
- key/enable ensures the key exists (may lazily create). key/disable is not supported on Transit (501 NotImplemented with guidance).

Error format (admin APIs)
Errors are returned as JSON:
```json
{"code":"InvalidConfiguration","message":"Failed to create KMS manager","description":"Error: ..."}
```
Common codes: AccessDenied, InvalidConfiguration, NotFound, NotImplemented.

Notes
- RustFS uses the Vault Transit engine (encrypt/decrypt/rewrap and datakey/plaintext). Ensure the Transit engine is enabled and mounted (default: transit).
- KV engine paths (e.g., secret/data/...) are not supported; there is no vault_key_path parameter. Such fields will be ignored if provided.
- You don't need an explicit vault_auth_method field; RustFS infers the auth method: token when vault_token is present; AppRole when both vault_app_role_id and vault_app_role_secret_id are present. Extra fields are ignored.
- Wrapped DEKs include a small key_id header to help auto-select the correct key during decryption.

## Using SSE with Objects

RustFS supports SSE-S3 (AES256) and SSE-KMS (aws:kms) headers on PUT. Object data is encrypted with DEK (AES-256-GCM) and metadata stores wrapped DEK and parameters.

Headers for PUT (either option):
- SSE-S3: x-amz-server-side-encryption: AES256
- SSE-KMS: x-amz-server-side-encryption: aws:kms
- Optional KMS key: x-amz-server-side-encryption-aws-kms-key-id: <key-id>
- Optional encryption context (JSON): x-amz-server-side-encryption-context: {"project":"demo","tenant":"t1"}

SSE-C (customer-provided keys) headers (supported for single PUT/GET/COPY):
- x-amz-server-side-encryption-customer-algorithm: AES256
- x-amz-server-side-encryption-customer-key: <Base64-encoded 256-bit key>
- x-amz-server-side-encryption-customer-key-MD5: <Base64-encoded MD5(key)>

Notes
- Use HTTPS for SSE-C to avoid exposing plaintext keys. RustFS never persists the provided key, only algorithm and random IV; you must provide the same key again on GET.
- For COPY: use x-amz-copy-source-server-side-encryption-customer-* to decrypt source; choose SSE-S3/SSE-KMS or SSE-C for destination.
- Multipart uploads: SSE-C is not supported; use single PUT or COPY.

Constraints and values
- x-amz-server-side-encryption: AES256 (SSE-S3) or aws:kms (SSE-KMS).
- x-amz-server-side-encryption-aws-kms-key-id should be an existing or lazily creatable key name.
- x-amz-server-side-encryption-context must be JSON (UTF-8). Large contexts increase metadata overhead.

DSSE compatibility
- Accepts aws:kms:dsse as the header value and normalizes to aws:kms in responses/HEAD.

Key selection
- If x-amz-server-side-encryption-aws-kms-key-id is present, it is used.
- Otherwise RustFS uses KMS default_key_id if configured; if none, it falls back to "rustfs-default-key" and will attempt to create it automatically (best-effort).

Encryption context (AAD)
- If you pass a JSON string in x-amz-server-side-encryption-context, it will be merged with the defaults. RustFS persists the effective context internally and no longer exposes per-field x-amz-server-side-encryption-context-*.
- RustFS always includes bucket and key in the context to bind the ciphertext to the object identity.
- On GET, RustFS decrypts using the internally stored sealed context; clients don’t need to send any special headers.

Persisted metadata
- Public: x-amz-server-side-encryption (AES256|aws:kms) and x-amz-server-side-encryption-aws-kms-key-id (when applicable)
- Internal (hidden): x-rustfs-internal-sse-key, x-rustfs-internal-sse-iv, x-rustfs-internal-sse-tag, x-rustfs-internal-sse-context
  - These fields contain the sealed DEK, IV, AEAD tag, and JSON context. They are filtered out from responses and object HEAD.

Bucket defaults and multipart behavior
- When a bucket has a default SSE (SSE-S3 or SSE-KMS), RustFS uses it when requests omit SSE headers.
- For multipart uploads: CreateMultipartUpload records the encryption intent; CompleteMultipartUpload writes internal sealed metadata and returns proper SSE headers (and KMS KeyId if applicable), aligned with MinIO/S3 behavior.
- Multipart + SSE-C is currently not supported.

## Curl Examples

Configure Vault KMS (token):
```bash
curl -sS -X POST \
  http://127.0.0.1:9000/rustfs/admin/v3/kms/configure \
  -H 'Content-Type: application/json' \
  -d '{
    "kms_type":"vault",
    "vault_address":"https://vault.example.com",
    "vault_token":"s.xxxxx",
    "vault_mount_path":"transit"
  }'
```

Parameter sanity
- Supported fields: kms_type, vault_address, vault_token, vault_namespace, vault_mount_path, vault_timeout_seconds, vault_app_role_id, vault_app_role_secret_id.
- Not supported: vault_key_path, vault_auth_method (ignored if present). Set vault_mount_path to your actual Transit mount name when different from transit.
- If your Vault only has the KV engine (e.g., secret/...), enable the Transit engine first, then configure RustFS.

Create a key (JSON body recommended):
```bash
curl -sS -X POST \
  'http://127.0.0.1:9000/rustfs/admin/v3/kms/key/create' \
  -H 'Content-Type: application/json' \
  -d '{"keyName":"app-default","algorithm":"AES-256"}'
```

Rotate a key:
```bash
curl -sS -X POST 'http://127.0.0.1:9000/rustfs/admin/v3/kms/key/rotate?keyName=app-default'
```

PUT with SSE-S3 (AES256):
```bash
curl -sS -X PUT 'http://127.0.0.1:9000/bucket1/hello.txt' \
  -H 'x-amz-server-side-encryption: AES256' \
  --data-binary @./hello.txt
```

PUT with SSE-KMS and context:
```bash
curl -sS -X PUT 'http://127.0.0.1:9000/bucket1/secret.txt' \
  -H 'x-amz-server-side-encryption: aws:kms' \
  -H 'x-amz-server-side-encryption-aws-kms-key-id: app-default' \
  -H 'x-amz-server-side-encryption-context: {"project":"demo","env":"staging"}' \
  --data-binary @./secret.txt
```

SSE-C upload (single PUT):
```bash
curl -sS -X PUT 'http://127.0.0.1:9000/bucket1/private.txt' \
  -H 'x-amz-server-side-encryption-customer-algorithm: AES256' \
  -H "x-amz-server-side-encryption-customer-key: <Base64Key>" \
  -H "x-amz-server-side-encryption-customer-key-MD5: <Base64MD5>" \
  --data-binary @./private.txt
```

SSE-C GET:
```bash
curl -sS 'http://127.0.0.1:9000/bucket1/private.txt' \
  -H 'x-amz-server-side-encryption-customer-algorithm: AES256' \
  -H "x-amz-server-side-encryption-customer-key: <Base64Key>" \
  -H "x-amz-server-side-encryption-customer-key-MD5: <Base64MD5>" \
  -o ./private.out
```

COPY with SSE-C source and SSE-KMS destination:
```bash
curl -sS -X PUT 'http://127.0.0.1:9000/bucket1/copied.txt' \
  -H 'x-amz-copy-source: /bucket1/private.txt' \
  -H 'x-amz-copy-source-server-side-encryption-customer-algorithm: AES256' \
  -H "x-amz-copy-source-server-side-encryption-customer-key: <Base64Key>" \
  -H "x-amz-copy-source-server-side-encryption-customer-key-MD5: <Base64MD5>" \
  -H 'x-amz-server-side-encryption: aws:kms' \
  -H 'x-amz-server-side-encryption-aws-kms-key-id: app-default'
```

GET (transparent decryption):
```bash
curl -sS 'http://127.0.0.1:9000/bucket1/secret.txt' -o ./secret.out
```

Rewrap a wrapped DEK (admin):
```bash
curl -sS -X POST 'http://127.0.0.1:9000/rustfs/admin/v3/kms/rewrap' \
  -H 'Content-Type: application/json' \
  -d '{"ciphertext_b64":"<base64-of-wrapped-dek>","context":{"bucket":"bucket1","key":"secret.txt"}}'
```

## Batch rewrap encrypted objects

Use this admin API to rewrap all wrapped DEKs under a bucket/prefix to the latest KMS key version. Supports dry run, pagination, and non-recursive listing.

- Endpoint: POST /rustfs/admin/v3/kms/rewrap-bucket
- Body fields:
  - bucket: string (required)
  - prefix: string (optional)
  - recursive: bool (default: true)
  - page_size: integer 1..=1000 (default: 1000)
  - max_objects: integer (optional upper bound of processed objects)
  - dry_run: bool (default: false)

Returns and constraints
- When dry_run=true: no writes; returns stats like { matched, would_rewrap, errors }.
- When running: returns { rewrapped, failed, errors } where errors is an array of { key, error }.
- Prefer chunking by prefix and limiting page_size/max_objects for large datasets.

Notes
- Rewrap preserves ciphertext format with the embedded key_id header.
- For Vault, the original encryption context (AAD) stored in object metadata is used for validation.
- When dry_run=true, no metadata is updated; the response reports how many objects would be rewrapped.

Example (dry-run, recursive):
```bash
curl -sS -X POST 'http://127.0.0.1:9000/rustfs/admin/v3/kms/rewrap-bucket' \
  -H 'Content-Type: application/json' \
  -d '{
    "bucket":"bucket1",
    "prefix":"tenant-a/",
    "recursive":true,
    "page_size":1000,
    "dry_run":true
  }'
```

Example (non-recursive, limit to 200 objects):
```bash
curl -sS -X POST 'http://127.0.0.1:9000/rustfs/admin/v3/kms/rewrap-bucket' \
  -H 'Content-Type: application/json' \
  -d '{
    "bucket":"bucket1",
    "prefix":"tenant-a/",
    "recursive":false,
    "page_size":500,
    "max_objects":200,
    "dry_run":false
  }'
```

## Runbook: key rotation + batch rewrap

1) Rotate the master key version (admin)
```bash
curl -sS -X POST 'http://127.0.0.1:9000/rustfs/admin/v3/kms/key/rotate?keyName=app-default'
```
2) Dry-run to assess impact
```bash
curl -sS -X POST 'http://127.0.0.1:9000/rustfs/admin/v3/kms/rewrap-bucket' \
  -H 'Content-Type: application/json' \
  -d '{"bucket":"bucket1","prefix":"tenant-a/","dry_run":true}'
```
3) Execute in batches (by prefix/limits)
```bash
curl -sS -X POST 'http://127.0.0.1:9000/rustfs/admin/v3/kms/rewrap-bucket' \
  -H 'Content-Type: application/json' \
  -d '{"bucket":"bucket1","prefix":"tenant-a/","page_size":1000,"max_objects":500}'
```
4) Sample verification: random GETs; verify content and metadata (SSE fields, wrapped DEK updated).

Notes
- Always dry-run first; then run in segments to control risk/load.
- Monitor KMS status (/v3/kms/status) and error items before/after runs.

## Permissions (Vault Transit)

Minimum capabilities for an in-use key (e.g., app-default):
- transit/datakey/plaintext (generate plaintext DEK and wrapped key)
- transit/encrypt, transit/decrypt (fall-back and tooling paths)
- transit/rewrap (update ciphertext to latest key version)

Example policy snippet (replace mount path and key names accordingly):
```hcl
path "transit/datakey/plaintext/app-default" { capabilities = ["update"] }
path "transit/encrypt/app-default"        { capabilities = ["update"] }
path "transit/decrypt/app-default"        { capabilities = ["update"] }
path "transit/rewrap/app-default"         { capabilities = ["update"] }
```

## Troubleshooting

- KMS status shows Failed: verify address/token/approle and that the Transit engine is enabled and mounted (default: transit). Sealed Vaults will report failure.
- Access denied on datakey/plaintext: adjust Vault policies to allow transit generate for that key.
- Disable not supported on Vault: remove/rotate keys or adjust Vault policies instead.
- rewrap-bucket returns errors: reduce scope (prefix), lower page_size, and inspect { key, error } entries.
- GET fails (decryption error): ensure the internally sealed context (including bucket/key) is valid; the server decrypts using the sealed AAD and clients do not need extra headers. Also verify Vault policies allow AAD-bound operations.

## Roadmap

- Bounded retries/backoff and metrics around KMS calls.
- Richer admin UX and examples.

For developers
- See "KMS/SSE Internal Design Overview": docs/en/kms-internal.md
