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

## Hardening Behavior

`KMSD-002` makes Local KMS unsafe defaults explicit development opt-ins or
production failures:

- no local master key fails validation unless
  `allow_insecure_dev_defaults = true`;
- local key directories under the process temp directory fail validation unless
  `allow_insecure_dev_defaults = true`;
- `RUSTFS_KMS_LOCAL_MASTER_KEY` is the production-safe local CLI/env path for
  encrypted local key files;
- `RUSTFS_KMS_ALLOW_INSECURE_DEV_DEFAULTS=true` is the development-only escape
  hatch for local plaintext or temp-dir setups.

`KMSD-003` makes Vault unsafe defaults explicit development opt-ins or
production failures:

- HTTP Vault addresses fail validation unless explicit development opt-in is set;
- default `dev-token` credentials fail validation unless explicit development
  opt-in is set;
- explicit `skip_tls_verify = true` fails validation unless explicit development
  opt-in is set;
- `RUSTFS_KMS_ALLOW_INSECURE_DEV_DEFAULTS=true` applies to `KmsConfig::from_env`;
- admin configure requests can set `allow_insecure_dev_defaults = true` for the
  same development-only behavior.

These checks run in `KmsConfig::validate()` so CLI startup, persisted dynamic
configuration, service-manager start/reconfigure, and direct backend
construction use the same fail-closed behavior.

## Compatibility Notes

- Production Local KMS deployments should configure an absolute key directory
  outside the process temp directory and set `RUSTFS_KMS_LOCAL_MASTER_KEY`.
- Local development setups that intentionally store plaintext key files or use
  temp directories must set `RUSTFS_KMS_ALLOW_INSECURE_DEV_DEFAULTS=true` or the
  admin request field `allow_insecure_dev_defaults = true`.
- Production Vault deployments should use HTTPS, non-default credentials, and
  TLS verification.
- Local Vault development setups that intentionally use HTTP, `dev-token`, or
  skip TLS verification must set the same explicit development opt-in.
- Legacy persisted KMS config JSON remains deserializable; old unsafe persisted
  values default to production mode and fail validation until secured or
  explicitly marked development-only.

## Test Coverage

- Local production fail-closed and development opt-in validation.
- Vault HTTP, default token, and skip-TLS fail-closed validation plus explicit
  development opt-in.
- `KmsConfig::from_env()` development default rejection and opt-in behavior.
- Admin configure request conversion to the same validation behavior.
- KMS service manager rejects unsafe configs before moving to `Configured`.
