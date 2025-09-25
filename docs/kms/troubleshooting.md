# KMS Troubleshooting

Use this checklist to diagnose and resolve common KMS-related issues.

## Quick Diagnostics

1. **Check status**
   ```bash
   awscurl --service s3 --region us-east-1 \
     --access_key admin --secret_key admin \
     http://localhost:9000/rustfs/admin/v3/kms/status
   ```
2. **Inspect logs** – enable `RUST_LOG=rustfs::kms=debug`.
3. **Verify backend reachability** – for Vault, run `vault status` and test network connectivity.
4. **Run a smoke test** – upload a small object with `--server-side-encryption AES256`.

## Common Issues

| Symptom | Likely Cause | Resolution |
|---------|--------------|------------|
| `status: NotConfigured` | KMS was never configured or configuration failed. | POST `/kms/configure`, then `/kms/start`. Check logs for JSON parsing errors. |
| `healthy: false` | Backend health probe failed; Vault sealed or filesystem inaccessible. | Unseal Vault, confirm permissions on the key directory, re-run `/kms/start`. |
| `InternalError: failed to create key` | Backend rejected the request (e.g. Vault policy). | Review Vault audit logs and ensure the RustFS policy has `transit/keys/*` access. |
| `AccessDenied` when downloading SSE-C objects | Missing/incorrect SSE-C headers. | Provide the same `x-amz-server-side-encryption-customer-*` headers used during upload. |
| Multipart SSE-C download truncated | Parts smaller than 5 MiB stored inline; older builds mishandled them. | Re-upload with ≥5 MiB parts or upgrade to the latest RustFS build. |
| `Operation not permitted` during tests | OS sandbox blocked launching `vault` or `rustfs`. | Re-run tests with elevated permissions (`cargo test ...` with sandbox overrides). |
| `KMS key directory is required for local backend` | Started RustFS with `--kms-backend local` but no `--kms-key-dir`. | Supply the flag or use the dynamic API to set the directory before calling `/kms/start`. |

## Clearing the Cache

If data keys become stale (e.g. after manual rotation in Vault), clear the cache:

```bash
awscurl --service s3 --region us-east-1 \
  --access_key admin --secret_key admin \
  -X POST http://localhost:9000/rustfs/admin/v3/kms/clear-cache
```

## Resetting the Service

1. `POST /kms/stop`
2. `POST /kms/configure` with the known-good payload
3. `POST /kms/start`
4. Verify with `/kms/status`

## Support Data Collection

When opening an issue, capture:

- Output of `/kms/status` and `/kms/config`
- Relevant RustFS logs (`rustfs::kms=*`)
- Vault audit log snippets (if using Vault)
- The SSE headers used by the failing client request

Providing these artifacts drastically speeds up triage.
