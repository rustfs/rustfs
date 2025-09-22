# KMS Configuration Guide

This guide describes the configuration surfaces for the RustFS Key Management Service. RustFS can be configured statically at process start or dynamically via the admin REST API. Most operators start with a static bootstrap (CLI flags, configuration file, or environment variables) and then rely on dynamic configuration to rotate keys or swap backends.

## Configuration Sources

| Mechanism           | When to use                                             | Notes |
|---------------------|----------------------------------------------------------|-------|
| CLI flags           | Local development, ad-hoc testing                        | `rustfs server --kms-enable --kms-backend vault ...` |
| Environment vars    | Container/Helm/Ansible deployments                       | Prefix variables with `RUSTFS_` (see table below). |
| Static config file  | Use your orchestration tooling to render TOML/YAML, then pass the corresponding flags during startup. |
| Dynamic REST API    | Post-start updates without restarting (see [dynamic-configuration-guide.md](dynamic-configuration-guide.md)). |

## CLI Flags & Environment Variables

| CLI flag                    | Env variable                   | Description |
|-----------------------------|--------------------------------|-------------|
| `--kms-enable`              | `RUSTFS_KMS_ENABLE`            | Enables KMS at startup. Defaults to `false`. |
| `--kms-backend <local|vault>` | `RUSTFS_KMS_BACKEND`         | Selects the backend implementation. Defaults to `local`. |
| `--kms-key-dir <path>`      | `RUSTFS_KMS_KEY_DIR`           | Required when `kms-backend=local`; directory that stores wrapped master keys. |
| `--kms-vault-address <url>` | `RUSTFS_KMS_VAULT_ADDRESS`     | Vault base URL (e.g. `https://vault.example.com:8200`). |
| `--kms-vault-token <token>` | `RUSTFS_KMS_VAULT_TOKEN`       | Token used for Vault authentication. Prefer AppRole or short-lived tokens. |
| `--kms-default-key-id <id>` | `RUSTFS_KMS_DEFAULT_KEY_ID`    | Default key used when clients omit `x-amz-server-side-encryption-aws-kms-key-id`. |

> **Tip:** Even when you plan to reconfigure the backend dynamically, setting `--kms-enable` is useful because it instantiates the global manager eagerly and surfaces better error messages when configuration fails.

## Static TOML Example (Local Backend)

```toml
# rustfs.toml
[kms]
enabled = true
backend = "local"
key_dir = "/var/lib/rustfs/kms-keys"
default_key_id = "rustfs-master"
```

Render this file using your favourite template tool and translate it to CLI flags when launching RustFS:

```bash
rustfs server \
  --kms-enable \
  --kms-backend local \
  --kms-key-dir /var/lib/rustfs/kms-keys \
  --kms-default-key-id rustfs-master
```

## Static TOML Example (Vault Backend)

```toml
[kms]
enabled = true
backend = "vault"
vault_address = "https://vault.example.com:8200"
# Supply either a token or render AppRole credentials dynamically
vault_token = "s.XYZ..."
default_key_id = "rustfs-master"
```

Ensure that the Vault binary is reachable and the Transit engine is initialised before starting RustFS:

```bash
vault secrets enable transit
vault secrets enable -path=secret kv-v2
vault write transit/keys/rustfs-master type=aes256-gcm96
```

If you prefer AppRole authentication, omit `vault_token` and set the token dynamically via the REST API once RustFS is online (see [dynamic-configuration-guide.md](dynamic-configuration-guide.md)).

## Backend-Specific Options

### Local Backend

| Field            | Description |
|------------------|-------------|
| `key_dir`        | Directory where wrapped master keys are stored (`*.key` JSON files). Ensure it is backed up securely in persistent deployments. |
| `default_key_id` | Optional; if not provided, SSE-S3 uploads require an explicit header. |
| `file_permissions` (REST only) | Octal permissions applied to generated key files (`0o600` by default). |
| `master_key` (REST only) | Base64-encoded wrapping key used to protect DEKs on disk. Leave unset to generate one automatically. |

During development you can generate a default key manually:

```bash
mkdir -p /tmp/rustfs-keys
openssl rand -hex 32 > /tmp/rustfs-keys/rustfs-master.material
```

The KMS e2e tests also demonstrate programmatic key creation using the `/kms/keys` API.

### Vault Backend

| Field               | Description |
|---------------------|-------------|
| `address`           | Base URL including scheme. TLS is strongly recommended. |
| `auth_method`       | `Token { token: "..." }` or `AppRole { role_id, secret_id }`. Tokens should be renewable or short-lived. |
| `mount_path`        | Transit engine mount (default `transit`). |
| `kv_mount`          | KV v2 engine used to stash wrapped keys or metadata. |
| `key_path_prefix`   | Prefix under the KV mount (e.g. `rustfs/kms/keys`). |
| `namespace`         | Vault enterprise namespace (optional). |
| `skip_tls_verify`   | Development convenience; avoid using this in production. |
| `default_key_id`    | Transit key to use when clients omit `x-amz-server-side-encryption-aws-kms-key-id`. |

## Advanced Runtime Knobs (REST API)

The dynamic API exposes additional fields not available on the CLI:

| Field | Purpose |
|-------|---------|
| `timeout_seconds` | Backend operation timeout (defaults to 30s). |
| `retry_attempts`  | Number of retries for transient backend failures (defaults to 3). |
| `enable_cache`    | Enables in-memory cache of DEKs and metadata. |
| `max_cached_keys` / `cache_ttl_seconds` | Cache size and TTL limits. |

These options are mostly relevant for large deployments; configure them via the `/kms/configure` REST call once the service is online.

## Bootstrapping Workflow

1. Pick a backend (`local` or `vault`).
2. Ensure the required infrastructure is ready (filesystem permissions or Vault engines).
3. Start RustFS with `--kms-enable` and the minimal bootstrap flags.
4. Call the REST API to refine configuration (timeouts, cache, AppRole, etc.).
5. Verify with `/kms/status` and issue a test `PutObject` using SSE headers.
6. Record the configuration in your infra-as-code tooling for repeatability.

For runtime reconfiguration (rotating keys, swapping from local to Vault) follow the step-by-step guide in [dynamic-configuration-guide.md](dynamic-configuration-guide.md).
