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
