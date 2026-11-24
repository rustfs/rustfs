# SSE Crate Instructions

Applies to `crates/sse/`.

## Boundary

- This crate sits between the S3 layer and the storage engine and depends on neither. Its `rustfs-*` dependencies are pinned by `scripts/check_architecture_migration_rules.sh` to `rustfs-config`, `rustfs-kms`, `rustfs-policy`, `rustfs-s3-types`, and `rustfs-utils`; see [crate boundaries](../../docs/architecture/crate-boundaries.md).
- Anything the crate needs from the running server arrives through `SseHost` (`src/host.rs`), passed as the first argument of every entry point that consults the server. Do not add a global that stands in for the host: a caller must not be able to reach key selection without naming the server it speaks for.
- Translation to and from storage-engine types (`ObjectEncryptionResolver`, `WriteEncryption`, `MultipartCompletionAuthorization`) and request types belongs in `rustfs/src/storage/sse.rs`, not here.
- Report failures as `SseError`, classified with the S3 error code the caller answers with. A failure that did not originate from the request goes through `SseError::internal`, which keeps its detail out of the response.

## Layout

- `src/startup.rs` settles the state that is fixed for the lifetime of the process and reports it. Nothing that follows the KMS service state belongs there: the service follows configuration and can be started, restarted, or stopped from the admin API, so every choice that depends on it is made per request.
- `src/core/` holds the entry points (`write.rs`, `read.rs`, `rewrap.rs`) and the request principal (`request.rs`). An entry point only orders the steps; the decisions live in the two layers below it.
- `src/core/s3/` is what S3 defines: the SSE headers of a request and a response, the scheme a write or a stored object resolves to, the SSE-C checks, and the authorization of a KMS key.
- `src/core/keying/` is how RustFS forms, seals, and records an object's key. `direct.rs` and `object.rs` are the two modes, and each owns both directions of its stored record: the code that writes a key's metadata sits next to the code that reads it back. `mod.rs` is the only place that says which mode a build writes; `stored.rs` holds the order in which a read recognizes the modes and picks the provider that unwraps the data key; `metadata.rs` records the scheme and lists every stored key name.
- `src/kms/` holds the data-key providers: they wrap and unwrap a data key and decide nothing about schemes. The write and rewrap entry points, `core/keying/stored.rs`, and the startup check of the local master key call them; `core/s3/` never does.

Rules that keep the layers apart:

- `core/keying/` never imports from `core/s3/`. `core/s3/` reads stored metadata only through `core/keying/metadata.rs`. Anything else that needs both layers is joined in an entry point.
- `#[cfg(feature = "rio-v2")]` appears only under `core/keying/` and `kms/`. An entry point or `core/s3/` that needs a build-dependent answer asks `core/keying/` for it.
- A new stored format, including a MinIO-compatible one, is added under `core/keying/` (and `kms/` when it brings its own wrapping) without changes under `core/s3/`: a mode file with its record and read functions, one entry in the recognition order of `stored.rs`, and its key names in the lists of `metadata.rs`.
- A read decides from what the object recorded. The scheme a request advertises and whether a KMS is running now are write policy and runtime state; neither says how an existing object was keyed.

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
