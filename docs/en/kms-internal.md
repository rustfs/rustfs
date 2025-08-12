# RustFS KMS/SSE Internal Design Overview

This document targets developers. It explains the internal design, data model, core flows, and error semantics of RustFS object encryption (SSE) and KMS integration. It complements the user-facing manual (kms.md).

## Goals

- Align with MinIO/S3 behavior: SSE-S3, SSE-KMS (accept aws:kms:dsse), SSE-C; multipart Complete returns correct SSE headers.
- Security convergence: persist only sealed metadata internally; avoid exposing sensitive components; HEAD/GET never reveal internal fields.
- Simplicity: clients use standard SSE headers only; GET requires no extra headers; bucket default encryption is applied automatically.
- Loose coupling: ObjectEncryptionService decouples crypto from storage; support both Vault and Local KMS backends.

## Components and Responsibilities

- ObjectEncryptionService (crates/kms)
  - Generates DEKs, wraps/unwraps them, and performs streaming AES-256-GCM.
  - Unifies KMS interactions via a KmsManager trait (Vault/Local backends).
  - Normalizes DSSE: aws:kms:dsse is treated as aws:kms.
- Storage layer (rustfs/src/storage/ecfs.rs)
  - Invokes encryption on PUT/COPY/multipart Complete; decryption on GET/COPY source.
  - Manages metadata read/write and filtering; exposes standard SSE headers, hides internal sealed metadata.
- Admin layer (rustfs/src/admin/handlers/kms.rs)
  - KMS configuration, key management, and batch rewrap; rewrap reads/writes internal sealed fields directly.

## Data Model

- Public (persisted and visible)
  - x-amz-server-side-encryption: AES256 | aws:kms (input may accept aws:kms:dsse; stored/responded as aws:kms)
  - x-amz-server-side-encryption-aws-kms-key-id: <key-id> (for SSE-KMS)
- Internal sealed metadata (persisted and hidden; prefix x-rustfs-internal-)
  - x-rustfs-internal-sse-key: base64(wrapped DEK with embedded key_id header)
  - x-rustfs-internal-sse-iv: base64(IV)
  - x-rustfs-internal-sse-tag: base64(GCM TAG)
  - x-rustfs-internal-sse-context: JSON of the effective AAD (at least bucket and key; merges user-provided context if any)

Notes
- For SSE-C, user-provided keys are never persisted; only the IV is stored internally; the public algorithm header is AES256.
- HEAD/List filter out all internal fields; only public SSE headers remain visible.

## Core Flows

### PUT (single object)
1) Parse SSE headers: SSE-S3 or SSE-KMS (optional key-id and context JSON); SSE-C has distinct validation (key/MD5).
2) Build AAD: include bucket and key; merge x-amz-server-side-encryption-context if provided.
3) Generate DEK and encrypt the stream via AES-256-GCM.
4) Write object metadata:
   - Public: algorithm header and optional KMS KeyId.
   - Internal: sealed sse-key/iv/tag/context.

### GET
1) Detect SSE-C: internal sse-iv present without sse-key (or legacy public IV) → require client SSE-C headers.
2) Otherwise, read sealed metadata and decrypt using KMS and sealed AAD.

### COPY
- Source: same as GET (SSE-C requires x-amz-copy-source-server-side-encryption-customer-* headers).
- Destination: same as PUT; you may choose a new SSE algorithm and KMS KeyId.

### Multipart
- CreateMultipartUpload: record algorithm and optional KMS KeyId; do not persist public context; mark x-amz-multipart-encryption-pending.
- UploadPart: write parts normally (we seal the entire object at completion in this implementation).
- CompleteMultipartUpload: merge object → encrypt as PUT → write internal sealed metadata → respond with SSE headers (and KeyId if applicable).
- SSE-C: multipart is currently unsupported (matching user docs).

## KMS Interactions

- Vault Transit
  - Use datakey/plaintext to obtain a plaintext DEK and a wrapped key; decrypt/rewrap are used for fallbacks and tools.
  - Lazily create default_key_id (defaults to rustfs-default-key) when allowed by policy; failures do not block non-KMS paths.
- Local KMS
  - Dev/test only; interface aligned with Vault.

## Errors and Edges

- Missing internal sealed fields: decryption fails (GET/COPY source).
- Missing SSE-C key/MD5: invalid request.
- KMS unreachable: encryption/decryption/rewrap operations fail; status endpoint shows Failed.
- AAD mismatch: unwrap fails; verify bucket/key and any custom AAD are consistent.

## Compatibility

- Legacy public fallbacks removed: we no longer decrypt from x-amz-server-side-encryption-{key,iv,tag,context-*}.
- Batch rewrap: read internal sse-key and sse-context; write back only the new sse-key (preserving ciphertext format and key_id header).

## Testing

- KMS unit tests: internal sealed roundtrip; failure when only legacy public fields are provided.
- Storage layer:
  - PUT/GET/COPY across SSE-C and SSE-KMS/S3 paths;
  - Multipart Complete returns SSE headers;
  - HEAD filters internal fields.

## Roadmap

- Backoff/metrics for KMS calls.
- Consider SSE-C multipart support while preserving sealed metadata semantics.
