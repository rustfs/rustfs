# KMS Development Defaults Inventory

This inventory tracks `KMSD-001` for
[`rustfs/backlog#660`](https://github.com/rustfs/backlog/issues/660). It records
current KMS development defaults before any production default hardening.

## Scope

- Source files reviewed: `crates/kms/src/config.rs` and
  `crates/kms/src/api_types.rs`.
- This is a `docs-only` task.
- No runtime behavior, config serialization, admin authorization, startup order,
  global state, storage path, or crate boundary changes are included.
- Follow-up hardening must be done in separate `security-change` PRs with focused
  tests.

## Current Defaults

| Source | Default | Current behavior | Classification |
|---|---|---|---|
| `KmsConfig::default()` | Local backend | Uses `LocalConfig::default()` and validates successfully. | dev-only |
| `LocalConfig::default().key_dir` | OS temp dir plus `rustfs_kms_keys` | Keys are stored under the process temp directory. | dev-only |
| `LocalConfig::default().master_key` | `None` | Local key files are stored in plaintext when no master key is configured. | invalid for production |
| `LocalConfig::default().file_permissions` | `0o600` | Owner read/write only for key files. | production-safe as a permission default, but not sufficient without encrypted key storage |
| `KmsConfig::local(key_dir)` | caller-provided key dir, default local fields | Keeps `master_key = None` unless the caller supplies one later. | dev-only unless explicit encryption material is configured |
| `KmsConfig::from_env()` local key dir | `./kms_keys` | The env loader builds a relative path, then existing validation rejects it because local key dirs must be absolute. | invalid as a standalone default |
| `KmsConfig::from_env()` local master key | absent | Leaves `master_key = None`. | invalid for production |
| `VaultConfig::default().address` | `http://localhost:8200` | HTTP is accepted by validation. | dev-only |
| `VaultTransitConfig::default().address` | `http://localhost:8200` | HTTP is accepted by validation. | dev-only |
| `VaultConfig::default().auth_method` | token dev-token | The default token is accepted if used as-is. | invalid for production |
| `VaultTransitConfig::default().auth_method` | token dev-token | The default token is accepted if used as-is. | invalid for production |
| `VaultConfig::default().tls` | `None` | No custom TLS settings. HTTPS without custom TLS relies on system CA when no skip flag is set. | production-safe only when HTTPS and system trust are intended |
| `VaultTransitConfig::default().tls` | `None` | Same TLS behavior as Vault KV2. | production-safe only when HTTPS and system trust are intended |
| `ConfigureVaultKmsRequest.skip_tls_verify` | omitted means false | `to_kms_config()` leaves TLS config as `None` unless the request explicitly sets true. | production-safe when omitted |
| `ConfigureVaultTransitKmsRequest.skip_tls_verify` | omitted means false | Same behavior as Vault KV2 configure requests. | production-safe when omitted |
| `skip_tls_verify = true` in configure requests | explicit insecure opt-in | Creates a TLS config with `skip_verify = true`. | invalid for production |

## Existing Validation Boundary

- Local key directories must be absolute.
- Timeout and retry attempts must be greater than zero.
- Vault addresses must use HTTP or HTTPS.
- Vault mount paths must be non-empty.
- HTTPS with custom TLS config and verification enabled warns when relying on
  system CA instead of custom CA/client certificates.
- Existing validation does not fail closed for HTTP Vault addresses, dev-token,
  missing local master key, temp key dirs, or explicit `skip_tls_verify = true`.

## Hardening Follow-Ups

`KMSD-002` should make Local KMS unsafe defaults explicit development opt-ins or
production failures:

- no local master key;
- local key directory under the process temp directory;
- local env defaults that cannot pass validation without an absolute path.

`KMSD-003` should make Vault unsafe defaults explicit development opt-ins or
production failures:

- HTTP Vault addresses;
- default dev-token credentials;
- explicit `skip_tls_verify = true`.

Both follow-ups must preserve existing development workflows through documented
compatibility behavior or explicit development mode. They must not modify KMS
runtime logic only to satisfy tests.

## Test Expectations For Follow-Ups

- Add focused negative tests before changing production default behavior.
- Keep persisted config serialization compatibility tests separate from runtime
  fail-closed tests.
- Cover both env-loaded configuration and admin configure request conversion.
- Prove development opt-in paths remain explicit and searchable.
- Do not alter KMS key operation behavior, authorization actions, cache behavior,
  or storage hot paths while hardening defaults.
