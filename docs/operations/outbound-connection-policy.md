# Outbound Connection Policy

**Use this when:** a webhook, audit target, OIDC provider, or object-lambda endpoint on a private or container network (Compose service names, `host.docker.internal`, RFC 1918 addresses) is not being reached, or you need to know which server-initiated connections RustFS restricts and how to allowlist one.
**Source of truth:** `crates/utils/src/egress.rs` (`OutboundPolicy`, `OutboundDnsResolver`, `validate_outbound_url`, `ENV_OUTBOUND_ALLOW_ORIGINS`).

RustFS validates every operator-configured outbound destination to close a server-side request forgery (SSRF) class. Two layers exist:

| Layer | What it checks | Escape hatch |
| --- | --- | --- |
| Literal URL check (`validate_outbound_url`) | Scheme is `http`/`https`; the host is not `localhost` or a loopback, private, shared, reserved, link-local, unspecified, or metadata address (IPv4-mapped and embedded IPv6 forms are classified by the embedded IPv4) | None |
| Full policy (`OutboundPolicy` + `OutboundDnsResolver`) | The literal check, plus re-validation of every address DNS returns on each new connection, so a hostname cannot be rebound to a restricted address after it was accepted | `RUSTFS_OUTBOUND_ALLOW_ORIGINS` for the loopback, private, shared, and reserved classes |

## Which subsystem uses which layer

| Subsystem | Layer | Notes |
| --- | --- | --- |
| Event-notification webhooks (`RUSTFS_NOTIFY_WEBHOOK_*`) and audit webhooks (`RUSTFS_AUDIT_WEBHOOK_*`) | Full policy | Proxies disabled and redirects not followed, so the endpoint must be reachable directly (`crates/targets/src/target/webhook.rs`) |
| Target configuration validation (startup and admin API) | Full policy | `crates/targets/src/config/common.rs` `validate_outbound_http_url`; `rustfs/src/admin/handlers/target_descriptor.rs` |
| OIDC discovery, JWKS, and token requests | Full policy | A blocked provider logs `OIDC provider discovery blocked by outbound policy` naming the origin to allowlist (`crates/iam/src/oidc.rs`) |
| Object Lambda targets | Full policy | `rustfs/src/admin/router.rs` `outbound_policy` |
| Bucket replication targets | Literal check, relaxed | Private addresses are always allowed; loopback only with `RUSTFS_REPLICATION_ALLOW_LOOPBACK_TARGET=true` (`crates/ecstore/src/bucket/remote_s3_client.rs` `validate_remote_endpoint`, shared with on-demand migration sources) |
| Site replication peers | Literal check | `rustfs/src/site_replication/mod.rs` |
| Tiering warm backends (S3, MinIO, RustFS, Azure, GCS, Aliyun, Tencent, Huawei, R2) | Literal check | `crates/ecstore/src/services/tier/warm_backend.rs` `validate_endpoint`; the RustFS provider adds a debug-only, env-gated loopback exception for e2e tests |
| Keystone `auth_url` | Literal check | `crates/keystone/src/config.rs` |

The allowlist affects only the "Full policy" rows. A literal-check subsystem rejects a hostname that is itself a restricted IP literal, does not re-check what a hostname resolves to, and cannot be widened by `RUSTFS_OUTBOUND_ALLOW_ORIGINS`.

## Symptoms

- Bucket event rules and webhook configuration look correct and uploads succeed, but no POST reaches the receiver.
- Target validation reports `<field> is not allowed: ...` with a reason such as `private address` or `loopback host`; when an exact-origin allowlist entry would fix it, the message says so.
- An OIDC login button is missing and startup logs `OIDC provider discovery blocked by outbound policy`.

## `RUSTFS_OUTBOUND_ALLOW_ORIGINS`

A comma-separated list of exact HTTP(S) origins permitted to resolve to otherwise-restricted addresses. It is a process-level setting read once at startup; individual target configuration cannot extend it.

```bash
# exact scheme://host:port — comma-separate multiple origins
RUSTFS_OUTBOUND_ALLOW_ORIGINS=http://logstash:8080,http://host.docker.internal:3020
```

| Rule | Detail |
| --- | --- |
| Exact origin | `scheme://host:port`. `http://logstash:8080` does not authorize `http://logstash:9090` or `https://logstash:8080` |
| Scheme | `http` or `https` only |
| Default port | If omitted, the scheme default (`80` / `443`) applies and the destination must use that port |
| Origin only | A trailing `/` is accepted; any path, query, or fragment (`http://logstash:8080/events`) is rejected |
| No userinfo | `http://user:pass@host` is rejected |
| No empty entries | A trailing or doubled comma is rejected |
| Fail closed | An invalid list yields `invalid outbound policy` / `invalid origin at position N` and the affected subsystem does not start with a partially applied allowlist |

### What stays blocked even when allowlisted

- Cloud metadata endpoints (`169.254.169.254` and the other well-known IMDS addresses).
- Link-local addresses (`169.254.0.0/16`, `fe80::/10`) and the unspecified address (`0.0.0.0`, `::`).
- IPv4-mapped, IPv4-compatible, and NAT64/6to4-embedded forms of the above; the embedded IPv4 address is what gets classified, so `::ffff:127.0.0.1` cannot bypass the policy.

The allowlist authorizes only the exact host named. A DNS answer for a different hostname that points at a private address is still rejected, and each new connection re-validates the resolved addresses.

## Docker Compose example

```yaml
services:
  rustfs:
    image: rustfs/rustfs:latest
    environment:
      RUSTFS_NOTIFY_ENABLE: "true"
      RUSTFS_NOTIFY_WEBHOOK_ENABLE_PRIMARY: "on"
      RUSTFS_NOTIFY_WEBHOOK_ENDPOINT_PRIMARY: "http://logstash:8080/events"
      RUSTFS_NOTIFY_WEBHOOK_QUEUE_DIR_PRIMARY: "/tmp/rustfs-events"
      # Allow the webhook host to resolve to the Compose private network.
      # The allowlist takes the origin only, without the /events path.
      RUSTFS_OUTBOUND_ALLOW_ORIGINS: "http://logstash:8080"
  logstash:
    image: docker.elastic.co/logstash/logstash:8.15.0
    # ...
```

The endpoint keeps its full path (`/events`); the allowlist entry is the origin only. Restart RustFS after changing the variable — the policy is read at startup — and check the logs for `is not allowed` messages if a target still fails to activate.
