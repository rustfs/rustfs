# SSE Crate Instructions

Applies to `crates/sse/`.

## Boundary

- This crate sits between the S3 layer and the storage engine and depends on neither. Its `rustfs-*` dependencies are pinned by `scripts/check_architecture_migration_rules.sh` to `rustfs-config`, `rustfs-kms`, `rustfs-policy`, `rustfs-s3-types`, and `rustfs-utils`; see [crate boundaries](../../docs/architecture/crate-boundaries.md).
- Anything the crate needs from the running server arrives through `SseHost` (`src/host.rs`), passed as the first argument of every entry point that consults the server. Do not add a global that stands in for the host: a caller must not be able to reach key selection without naming the server it speaks for.
- Translation to and from storage-engine types (`ObjectEncryptionResolver`, `WriteEncryption`, `MultipartCompletionAuthorization`) and request types belongs in `rustfs/src/storage/sse.rs`, not here.
- Report failures as `SseError`, classified with the S3 error code the caller answers with. A failure that did not originate from the request goes through `SseError::internal`, which keeps its detail out of the response.

## Security

- Never log plaintext customer keys, decrypted key material, data-key envelopes, nonces, or SSE request payloads, including through error strings and `Debug` output.
- KMS audit tags may carry only the fields documented on `KmsRequestAuditState`.
- The `test-util` feature exposes authorization overrides and DEK-provider injection. It is a dev-dependency feature only; never enable it from a normal dependency.

## Change Coordination

When changing key resolution, metadata, or error classification, verify compatibility with:

- `rustfs/src/storage/sse.rs` (the host implementation and the engine-boundary adapters)
- `crates/kms/`
- `crates/utils/src/http/object_encryption_keys.rs` (the stored-metadata key vocabulary)
- `rustfs/src/kms_rekey.rs` and `rustfs/src/admin/handlers/kms*.rs`

## Testing

Run both builds; the `rio-v2` feature swaps the object-key scheme and compiles different code paths:

```bash
cargo nextest run -p rustfs-sse --no-tests fail
cargo nextest run -p rustfs-sse --features rio-v2 --no-tests fail
```

The adapter tests live with the adapter:

```bash
cargo nextest run -p rustfs --no-tests fail -E 'test(/^storage::sse::/)'
```
