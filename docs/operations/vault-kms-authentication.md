# Vault KMS authentication runbook

This runbook covers how the RustFS Vault KMS backends (KV2 and Transit) authenticate to Vault, how to deploy AppRole and Vault Agent token-file authentication, and how the fail-closed credential window behaves in production. For what each backend stores in Vault and the KV2/Transit policy scopes, see [KMS backend security properties](kms-backend-security.md).

## Choosing an authentication method

| Method | Config tag | Credential lifetime | Background renewal | Recommended for |
| --- | --- | --- | --- | --- |
| Static token | `Token` | Whatever the operator provisioned; RustFS never renews it | None | Development; short-lived experiments |
| AppRole | `AppRole` | Lease-bound token obtained by login; renewed by RustFS | Renew at half TTL, re-login on failure | Production without a Vault Agent sidecar |
| Kubernetes | `Kubernetes` | Lease-bound token obtained by login; renewed by RustFS | Renew at half TTL, re-login on failure | Production on Kubernetes, with no credential to distribute |
| Agent token file | `TokenFile` | Owned by Vault Agent; RustFS only re-reads the sink file | File re-read once per poll interval | Production with a Vault Agent (or equivalent) managing auth |

Exactly one method must be configured. Setting `RUSTFS_KMS_VAULT_TOKEN_FILE` together with any other method, or `RUSTFS_KMS_VAULT_KUBERNETES_ROLE` together with `RUSTFS_KMS_VAULT_APPROLE_ROLE_ID`, is rejected at startup with a configuration error, because the effective identity would be ambiguous. A leftover `RUSTFS_KMS_VAULT_TOKEN` alongside a configured login method is tolerated and ignored, so a stale variable cannot silently downgrade the identity.

All of these are read the same way whether the service is started with `RUSTFS_KMS_ENABLE=true` or configured later through `POST /rustfs/admin/v3/kms/configure`.

The default `dev-token` fallback for `RUSTFS_KMS_VAULT_TOKEN` is rejected outside explicit development mode (`RUSTFS_KMS_ALLOW_INSECURE_DEV_DEFAULTS=true`), as are plain-HTTP Vault addresses and disabled TLS verification.

## AppRole authentication

### Vault-side setup

Create a policy scoped to exactly what the backend needs (see the KV2 and Transit policy examples in [KMS backend security properties](kms-backend-security.md)), then an AppRole that issues tokens carrying it:

```shell
vault policy write rustfs-kms rustfs-kms-policy.hcl

vault auth enable approle

vault write auth/approle/role/rustfs-kms \
    token_policies="rustfs-kms" \
    token_ttl=1h \
    token_max_ttl=24h \
    secret_id_ttl=90d \
    secret_id_num_uses=0
```

Keep `token_ttl` comfortably above the RustFS per-attempt timeout (default 30s): the fail-closed window defaults to one attempt timeout, so a token TTL close to it leaves almost no usable lifetime.

### RustFS configuration

```shell
RUSTFS_KMS_BACKEND=vault-transit            # or "vault" for the KV2 backend
RUSTFS_KMS_VAULT_ADDRESS=https://vault.example.com:8200
RUSTFS_KMS_VAULT_APPROLE_ROLE_ID=<role-id>
RUSTFS_KMS_VAULT_APPROLE_SECRET_ID_FILE=/etc/rustfs/approle-secret-id
# Alternatively, inline (the file takes precedence when both are set):
# RUSTFS_KMS_VAULT_APPROLE_SECRET_ID=<secret-id>
# Optional, defaults to "approle":
# RUSTFS_KMS_VAULT_APPROLE_MOUNT=approle
```

RustFS logs in at startup, then renews the token at half its TTL in the background. If renewal fails (network, Vault sealed, token revoked), it falls back to a full re-login; if that also fails, it keeps retrying every few seconds until Vault recovers.

### SecretID delivery and rotation

Deliver the SecretID out of band — a secrets-manager-mounted file, an init-container writing `RUSTFS_KMS_VAULT_APPROLE_SECRET_ID_FILE`, or Vault response wrapping unwrapped by your deployment tooling. Treat it like a password: owner-readable file permissions, never in logs or shell history.

The secret_id file is re-read on every login attempt, so rotating the SecretID is a two-step operation with no restart: generate a new SecretID (`vault write -f auth/approle/role/rustfs-kms/secret-id`), atomically replace the file, then revoke the old SecretID accessor. The already-issued token keeps renewing; the new SecretID is only needed at the next full re-login.

An empty or missing secret_id file fails the login attempt immediately (no Vault round trip). At startup the error is fatal — provider construction fails and the process exits — so a file missing at boot is recovered by restarting the process, not by an in-process retry. Once RustFS is running, the same failure is retried on the normal refresh cadence, so repairing the file mid-run heals the backend without a restart.

## Kubernetes authentication

On Kubernetes this is the method to prefer: the pod's own ServiceAccount is the identity, so there is no credential to distribute, rotate, or leak into a Secret.

### Vault-side setup

```shell
vault auth enable kubernetes

vault write auth/kubernetes/config \
    kubernetes_host="https://$KUBERNETES_SERVICE_HOST:$KUBERNETES_SERVICE_PORT"

vault write auth/kubernetes/role/rustfs \
    bound_service_account_names=rustfs \
    bound_service_account_namespaces=rustfs \
    token_policies=rustfs-kms \
    token_ttl=1h
```

As with AppRole, keep `token_ttl` comfortably above the RustFS per-attempt timeout (default 30s).

### RustFS configuration

```shell
RUSTFS_KMS_BACKEND=vault-transit            # or "vault" for the KV2 backend
RUSTFS_KMS_VAULT_ADDRESS=https://vault.vault.svc.cluster.local:8200
RUSTFS_KMS_VAULT_KUBERNETES_ROLE=rustfs
# Optional, defaults to "kubernetes":
# RUSTFS_KMS_VAULT_KUBERNETES_MOUNT=kubernetes
# Optional, defaults to the kubelet's projected token path:
# RUSTFS_KMS_VAULT_KUBERNETES_JWT_PATH=/var/run/secrets/kubernetes.io/serviceaccount/token
```

RustFS logs in at startup and renews the token at half its TTL, falling back to a fresh login exactly as AppRole does. The ServiceAccount token is re-read from disk on every login rather than cached, so a projected token the kubelet rotates is picked up without a restart.

A missing or empty token file fails the login attempt immediately (no Vault round trip). At startup the error is fatal — provider construction fails and the process exits — so a token projected late during a slow pod start is recovered by the pod restart loop, not by an in-process retry. Once RustFS is running, a token file that goes missing or turns empty is retried on the normal refresh cadence and heals the backend on its own.

## Vault Agent token file

In this mode a Vault Agent (or any equivalent process) owns authentication and token renewal, and RustFS only reads the token sink file.

### Vault Agent example

```hcl
auto_auth {
  method "approle" {
    config = {
      role_id_file_path   = "/etc/vault-agent/role-id"
      secret_id_file_path = "/etc/vault-agent/secret-id"
    }
  }

  sink "file" {
    config = {
      path = "/run/vault-agent/token"
      mode = 0600
    }
  }
}
```

### RustFS configuration

```shell
RUSTFS_KMS_BACKEND=vault-transit
RUSTFS_KMS_VAULT_ADDRESS=https://vault.example.com:8200
RUSTFS_KMS_VAULT_TOKEN_FILE=/run/vault-agent/token
```

The poll interval (`poll_interval_secs` in the `TokenFile` auth configuration, default 30 seconds) controls how often the file is re-read. Each successful read grants the token an observed validity of twice the poll interval and installs a fresh client generation, so an agent-rotated token is picked up within one poll interval of the atomic replace.

Requirements enforced at every read, each failing the refresh without contacting Vault:

- The file must exist and be non-empty after trimming whitespace.
- On Unix, the file must not be readable or writable by group or other (mode `0600` or stricter). Wider permissions are a hard error naming the offending mode, mirroring the SFTP host-key rule. RustFS must run as the file's owner.

If the agent stops refreshing the file that is fine — RustFS re-reads the same token and keeps going as long as the token itself is valid on the Vault side. If the file disappears or turns empty, RustFS keeps serving requests on the last-read token until the fail-closed window trips, and heals automatically once the file is restored.

## Fail-closed window

For lease-bound credentials (AppRole and Kubernetes tokens, token files), `current()` refuses to hand out a token that is within the safety window of its expiry and has not been refreshed. Requests then fail with `KMS credentials unavailable: ...` instead of being sent with a token that could lapse mid-flight and fail unpredictably on the Vault side.

- Default window: one per-attempt timeout (`RUSTFS_KMS_TIMEOUT_SECS`, default 30s) — a request issued now can legitimately stay in flight that long, so the token must outlive it.
- Override: `refresh_safety_window_secs` on the `AppRole`, `Kubernetes` or `TokenFile` auth configuration.
- Static tokens never trip the window: they carry no lease and are assumed valid until Vault says otherwise.

The window is a symptom threshold, not the fault itself: by the time it trips, refresh has been failing for roughly half the token TTL (AppRole, Kubernetes) or two poll intervals (token file).

### Troubleshooting

| Symptom | Log line to look for | Likely cause and fix |
| --- | --- | --- |
| Requests fail with `KMS credentials unavailable` | `Vault credential refresh failed; retrying until the credentials recover` (warn, repeated) | Vault unreachable/sealed, or the credential source is broken; the provider recovers on its own once refresh succeeds — fix the cause, no restart needed |
| Renewal succeeded but re-login later fails | `Vault token renewal failed; falling back to a fresh login` followed by login errors | SecretID expired/revoked or AppRole role changed; rotate the secret_id file |
| Token file mode error at startup or during polls | `has insecure permissions` in the error | Fix the sink `mode` (0600) and the file owner; the next poll heals the provider |
| Token file missing/empty errors | `Failed to read Vault token file` / `token file ... is empty` | Vault Agent down or sink misconfigured; restart the agent, the next poll heals the provider |
| Kubernetes login fails with a permission error | `Vault Kubernetes login failed` | The pod's ServiceAccount is not in the role's `bound_service_account_names`/`_namespaces`, or `auth/kubernetes/config` names the wrong API server |
| Kubernetes ServiceAccount token errors | `Failed to read Kubernetes ServiceAccount token` / `ServiceAccount token ... is empty` | The token is not projected into the pod (check `automountServiceAccountToken` and the volume mount); the next refresh cycle heals the provider |
| Startup fails immediately with a configuration error naming two env vars | — | Two auth methods configured at once; keep exactly one of token, AppRole, Kubernetes, token file |

When diagnosing, confirm three clocks/lifetimes in order: the Vault token TTL (`vault token lookup` with the token's accessor), the RustFS refresh cadence (half TTL or the poll interval), and the fail-closed window. The renewal task logs every failed cycle, so a silent gap in warnings combined with `CredentialsUnavailable` errors points at the process clock or a paused runtime rather than Vault.
