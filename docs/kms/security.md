# KMS Security Guidelines

This document summarises the security posture of the RustFS KMS subsystem and offers guidance for safe production deployment.

## Threat Model

- Attackers might obtain network access to RustFS or Vault.
- Leaked admin credentials could manipulate KMS configuration.
- Misconfigured SSE-C clients could expose plaintext keys.
- Insider threats may attempt to extract master keys from disk-based storage.

RustFS mitigates these risks via access control, auditability, and best practices outlined below.

## Authentication & Authorisation

- The admin API requires SigV4 credentials with `ServerInfoAdminAction`. Restrict these credentials to trusted automation.
- Do **not** share admin credentials with regular S3 clients. Provision separate IAM users for data-plane traffic.
- When running behind a reverse proxy, ensure the proxy passes through headers required for SigV4 signature validation.

## Network Security

- Enforce TLS for both RustFS and Vault deployments. Set `skip_tls_verify=false` in production.
- Use mTLS or private network peering between RustFS and Vault where possible.
- Restrict Vault transit endpoints using network ACLs or service meshes so only RustFS can reach them.

## Secret Management

- Never store Vault tokens directly in configuration files. Prefer AppRole or short-lived tokens injected at runtime.
- If you must render a token (e.g. in CI), use environment variables with limited scope and rotate them frequently.
- For the local backend, keep the key directory on encrypted disks with tight POSIX permissions (default `0o600`).

## Vault Hardening Checklist

- Enable audit logging (`vault audit enable file file_path=/var/log/vault_audit.log`).
- Create a dedicated policy granting access only to the `transit` and `secret` paths used by RustFS.
- Configure automatic token renewal or rely on `vault agent` to manage token lifetimes.
- Monitor the health endpoint (`/v1/sys/health`) and integrate it into your on-call alerts.

## Caching & Memory Hygiene

- When `enable_cache=true`, DEKs are stored in memory for the configured TTL. Tune `max_cached_keys` and TTL to balance latency versus exposure.
- The encryption service zeroises plaintext keys after use. Avoid logging plaintext keys or contexts in custom code.
- For workloads that require strict FIPS compliance, disable caching and rely on Vault for each request.

## SSE-C Considerations

- Clients are responsible for providing 256-bit keys and MD5 hashes. Reject uploads where the digest does not match.
- Educate clients that SSE-C keys are never stored server side; losing the key means losing access to the object.
- Use HTTPS for all client connections to prevent key disclosure.

## Audit & Monitoring

- Capture structured logs emitted under the `rustfs::kms` target. Each admin call logs request principals and outcomes.
- Export metrics such as cache hit ratio, backend latency, and failure counts to your observability stack.
- Periodically run the e2e Vault suite in a staging environment to verify backup/restore procedures.

## Incident Response

1. Stop the KMS service (`POST /kms/stop`) to freeze new operations.
2. Rotate admin credentials and Vault tokens.
3. Examine audit logs to determine the blast radius.
4. Restore keys from backups or Vault versions if tampering occurred.
5. Reconfigure the backend using trusted credentials and restart the service.

By adhering to these practices, you can deploy RustFS KMS with confidence across regulated or high-security environments.
