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

### Black-box behavior suite and the Vault lane

`crates/kms/tests/behavior_*.rs` drive the crate through its public entry
points only. By default they run against the Local and Static backends.

Setting `RUSTFS_KMS_VAULT_TOKEN` adds the Vault KV2 and Vault Transit backends
to every `for_each_backend` spec, against a live server
(`RUSTFS_KMS_VAULT_ADDR`, default `http://127.0.0.1:8200`):

```bash
NO_PROXY=127.0.0.1,localhost HTTP_PROXY= HTTPS_PROXY= http_proxy= https_proxy= \
RUSTFS_KMS_VAULT_TOKEN=<dev-token> cargo test -p rustfs-kms
```

The server needs a KV v2 engine at `secret/` and a Transit engine at
`transit/`, matching the crate's config defaults.

**Run the Vault lane whenever you touch rotation or versioning.** `rotate` and
`versioning` are advertised only by the Vault backends, so without it every
capability-gated branch for them takes the `UnsupportedCapability` side and
`behavior_rotation.rs` never asserts the working half — a rotation that dropped
prior key versions would go green.

The lane creates real keys under unique names (`behavior-kv2-*`,
`behavior-transit-*`) and does not remove them, so a dev Vault accumulates them
across runs. Clear them out periodically — against a dev server only:

```bash
vault list -format=json transit/keys | jq -r '.[] | select(startswith("behavior-transit-"))' | while read -r k; do vault write "transit/keys/$k/config" deletion_allowed=true >/dev/null && vault delete "transit/keys/$k"; done
```

```bash
vault list -format=json secret/metadata/rustfs/kms/keys | jq -r '.[] | select(startswith("behavior-kv2-"))' | xargs -I{} vault kv metadata delete secret/rustfs/kms/keys/{}
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
