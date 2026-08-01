# KMS Crate Instructions

Applies to `crates/kms/`.

## Change Coordination

When changing key-management behavior, verify compatibility with:

- `rustfs/src/storage/ecfs.rs`
- `rustfs/src/admin/handlers/kms.rs`
- `rustfs/src/admin/handlers/kms_dynamic.rs`
- `rustfs/src/admin/handlers/kms_keys.rs`
- `rustfs/src/admin/handlers/kms_management.rs`

## Security

- Never log plaintext keys, key material, or sensitive request payloads.
- Prefer explicit error propagation over panic paths.

## Testing

For local KMS end-to-end tests, keep proxy bypass settings:

```bash
NO_PROXY=127.0.0.1,localhost HTTP_PROXY= HTTPS_PROXY= http_proxy= https_proxy= \
cargo test --package e2e_test test_local_kms_end_to_end -- --nocapture --test-threads=1
```

## Local Key Export for SSE-S3 Migration Tests

Use the read-only `local_kms_key_decrypt` example to export an AES-256 Local
KMS key as the base64 value expected by `RUSTFS_SSE_S3_MASTER_KEY`:

```bash
export RUSTFS_KMS_LOCAL_MASTER_KEY='<local-kms-at-rest-master-key>'
export RUSTFS_SSE_S3_MASTER_KEY="$(
  cargo run -q -p rustfs-kms --example local_kms_key_decrypt -- \
    /absolute/path/to/<key-id>.key
)"
```

For a `plaintext-dev-only` Local KMS key file,
`RUSTFS_KMS_LOCAL_MASTER_KEY` is not required.

The example writes only the base64-encoded 32-byte key to stdout. Diagnostics
go to stderr. Never paste its output into logs, shell history, issue comments,
or committed configuration. The export path must remain read-only and must
reuse `LocalKmsClient` decoding so current Argon2id and legacy key-file
compatibility stay aligned with the backend.
