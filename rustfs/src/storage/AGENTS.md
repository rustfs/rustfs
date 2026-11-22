# Storage Module Instructions

Applies to `rustfs/src/storage/`.

## Correctness First

- Preserve object data integrity checks, versioning behavior, and S3-compatible response semantics.
- Do not weaken validation paths for range reads, multipart behavior, or metadata consistency.
- If behavior changes, add regression tests near the affected storage module.

## Security and Encryption

- Never log plaintext customer keys, decrypted key material, or sensitive SSE request payloads.
- Keep SSE behavior aligned with KMS and admin handler integrations.
- SSE key resolution lives in `crates/sse/`; `sse.rs` here only binds it to the running server (`RustfsSseHost`) and translates across the storage-engine boundary. Put new SSE logic in the crate unless it needs ecstore, IAM, or request types.
- Coordinate related changes with:
  - `crates/sse/`
  - `crates/kms/`
  - `rustfs/src/admin/handlers/kms*.rs`
  - `crates/ecstore/`

## Performance and Async

- Keep I/O paths non-blocking where possible.
- Avoid adding extra allocations or blocking work in hot object read/write paths without clear justification.

## Suggested Validation

- Targeted module tests in `rustfs/src/storage/*_test.rs`
