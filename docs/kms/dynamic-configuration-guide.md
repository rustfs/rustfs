# Dynamic KMS Configuration Playbook

RustFS exposes a first-class admin REST API that allows you to configure, start, stop, and reconfigure the KMS subsystem without restarting the server. This document walks through common operational scenarios.

## Prerequisites

- RustFS is running and reachable on the admin endpoint (typically `http(s)://<host>/rustfs/admin/v3`).
- You have admin access and credentials (access key/secret or session token) with the `ServerInfoAdminAction` permission.
- Optional: `awscurl` or another SigV4-aware HTTP client to sign admin requests.

Before starting, confirm the KMS service manager is initialised:

```bash
awscurl --service s3 --region us-east-1 \
  --access_key admin --secret_key admin \
  http://localhost:9000/rustfs/admin/v3/kms/status
```

The initial response shows `"status": "NotConfigured"`.

## Initial Configuration Flow

1. **Submit the configuration**
   ```bash
   awscurl --service s3 --region us-east-1 \
     --access_key admin --secret_key admin \
     -X POST -d '{
       "backend_type": "local",
       "key_dir": "/var/lib/rustfs/kms-keys",
       "default_key_id": "rustfs-master",
       "enable_cache": true,
       "cache_ttl_seconds": 900
     }' \
     http://localhost:9000/rustfs/admin/v3/kms/configure
   ```

2. **Start the service**
   ```bash
   awscurl --service s3 --region us-east-1 \
     --access_key admin --secret_key admin \
     -X POST http://localhost:9000/rustfs/admin/v3/kms/start
   ```

3. **Verify**
   ```bash
   awscurl --service s3 --region us-east-1 \
     --access_key admin --secret_key admin \
     http://localhost:9000/rustfs/admin/v3/kms/status
   ```

   Look for `"status": "Running"` and a backend summary.

## Switching to Vault

To migrate from the local backend to Vault:

1. Prepare Vault:
   ```bash
   vault secrets enable transit
   vault secrets enable -path=secret kv-v2
   vault write transit/keys/rustfs-master type=aes256-gcm96
   ```

2. Configure the new backend without stopping service:
   ```bash
   awscurl --service s3 --region us-east-1 \
     --access_key admin --secret_key admin \
     -X POST -d '{
       "backend_type": "vault",
       "address": "https://vault.example.com:8200",
       "auth_method": { "approle": { "role_id": "...", "secret_id": "..." } },
       "mount_path": "transit",
       "kv_mount": "secret",
       "key_path_prefix": "rustfs/kms/keys",
       "default_key_id": "rustfs-master",
       "retry_attempts": 5,
       "timeout_seconds": 60
     }' \
     http://localhost:9000/rustfs/admin/v3/kms/reconfigure
   ```

3. Confirm the new backend is active via `/kms/status`.

4. Run test uploads with `SSE-KMS` headers to ensure the new backend is serving requests.

## Rotating the Default Key

1. **Create a new key** using the key management API:
   ```bash
   awscurl --service s3 --region us-east-1 \
     --access_key admin --secret_key admin \
     -X POST -d '{ "KeyUsage": "ENCRYPT_DECRYPT", "Description": "rotation-2024-09" }' \
     http://localhost:9000/rustfs/admin/v3/kms/keys
   ```

2. **Set it as default** via `reconfigure`:
   ```bash
   awscurl --service s3 --region us-east-1 \
     --access_key admin --secret_key admin \
     -X POST -d '{
       "backend_type": "vault",
       "default_key_id": "rotation-2024-09"
     }' \
     http://localhost:9000/rustfs/admin/v3/kms/reconfigure
   ```

   Only the fields supplied in the payload are updated; omitted fields keep their previous values.

3. **Validate** by uploading a new object and checking that `x-amz-server-side-encryption-aws-kms-key-id` reports the new key.

## Rolling Cache or Timeout Changes

Caching knobs help tune latency. To adjust them at runtime:

```bash
awscurl --service s3 --region us-east-1 \
  --access_key admin --secret_key admin \
  -X POST -d '{
    "enable_cache": true,
    "max_cached_keys": 2048,
    "cache_ttl_seconds": 600,
    "timeout_seconds": 20
  }' \
  http://localhost:9000/rustfs/admin/v3/kms/reconfigure
```

## Pausing the KMS Service

Stopping the service keeps configuration in place but disables new KMS operations. Existing SSE objects remain accessible only if their metadata allows offline decryption.

```bash
awscurl --service s3 --region us-east-1 \
  --access_key admin --secret_key admin \
  -X POST http://localhost:9000/rustfs/admin/v3/kms/stop
```

Restart later with `/kms/start`.

## Automation Tips

- Wrap REST calls in an idempotent script (see `scripts/` for examples) so you can re-run configuration safely.
- Use `--test-threads=1` when running KMS e2e suites in CI; they spin up real servers and Vault instances.
- In Kubernetes, run the configuration script as an init job that waits for both RustFS and Vault readiness before calling `/kms/configure`.
- Emit events to your observability platform: successful reconfigurations generate structured logs with the backend summary.

## Rollback Strategy

If a new configuration introduces errors:

1. Call `/kms/reconfigure` with the previous payload (keep a snapshot in version control).
2. If the backend is unreachable, call `/kms/stop` to protect data from partial writes.
3. Investigate logs under `rustfs::kms::*` and Vault audit logs.
4. Once the issue is resolved, reapply the desired configuration and restart.

Dynamic configuration makes backend maintenance safe and repeatable—ensure every change is scripted and traceable.
