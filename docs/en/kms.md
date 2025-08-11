# RustFS KMS and Server-Side Encryption (SSE)

This document explains how to configure the Key Management Service (KMS) for RustFS, manage keys, and use S3-compatible Server-Side Encryption (SSE) for objects. It covers both Vault Transit and Local backends, encryption context (AAD), and practical curl examples.

## Overview

- Backends: Vault Transit (recommended for production), Local (development/testing).
- Defaults:
  - Vault transit mount path: transit
  - AES-256-GCM is used to encrypt object data; KMS manages the data key (DEK).
- Encryption context (AAD): RustFS canonically builds a JSON map containing at least bucket and key to bind ciphertext to object identity. You can also provide extra AAD via request header.

## Configure KMS

Endpoint:
- POST /rustfs/admin/v3/kms/configure

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

## Key Management APIs

- Create key: POST /rustfs/admin/v3/kms/key/create?keyName=<id>[&algorithm=AES-256]
- Key status: GET /rustfs/admin/v3/kms/key/status?keyName=<id>
- List keys: GET /rustfs/admin/v3/kms/key/list
- Enable key: PUT /rustfs/admin/v3/kms/key/enable?keyName=<id>
- Disable key: PUT /rustfs/admin/v3/kms/key/disable?keyName=<id>
  - Vault limitation: Transit does not support disabling keys; RustFS returns 501 with guidance.
- Rotate key: POST /rustfs/admin/v3/kms/key/rotate?keyName=<id>
- Rewrap ciphertext: POST /rustfs/admin/v3/kms/rewrap (body: {"ciphertext_b64":"...","context":{...}})

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

Key selection
- If x-amz-server-side-encryption-aws-kms-key-id is present, it is used.
- Otherwise RustFS uses KMS default_key_id if configured; if none, it falls back to "rustfs-default-key" and will attempt to create it automatically (best-effort).

Encryption context (AAD)
- If you pass a JSON string in x-amz-server-side-encryption-context, it will be merged with the defaults and stored per-field as x-amz-server-side-encryption-context-<k>.
- RustFS always includes bucket and key in the context to bind the ciphertext to the object identity.
- On GET, RustFS reconstructs the context from metadata and decrypts transparently. Clients don’t need to send any special headers to read encrypted objects.

Persisted metadata (managed by RustFS)
- x-amz-server-side-encryption-key: base64 wrapped DEK
- x-amz-server-side-encryption-iv: base64 IV
- x-amz-server-side-encryption-tag: base64 AEAD tag (GCM)
- x-amz-server-side-encryption-context-*: per-field AAD (e.g., …-context-bucket, …-context-key, …-context-project)

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

Create a key:
```bash
curl -sS -X POST 'http://127.0.0.1:9000/rustfs/admin/v3/kms/key/create?keyName=app-default&algorithm=AES-256'
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

## Troubleshooting

- KMS status shows Failed: verify address/token/approle and that the transit engine is enabled and mounted at the expected path (default: transit).
- Access denied on datakey/plaintext: adjust Vault policies to allow transit generate for the key.
- Disable not supported on Vault: remove/rotate keys or adjust Vault policies instead.

## Roadmap

- Bounded retries/backoff and metrics around KMS calls.
- Richer admin UX and examples.
