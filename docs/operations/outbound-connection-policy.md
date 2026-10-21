# Outbound Connection Policy

**Use this when:** a webhook, audit target, OIDC provider, or object-lambda endpoint on a private or container network (Compose service names, `host.docker.internal`, RFC 1918 addresses) is not being reached, or you need to know which server-initiated connections RustFS restricts and how to allowlist one.
**Source of truth:** `crates/utils/src/egress.rs` (`OutboundPolicy`, `OutboundDnsResolver`, `validate_outbound_url`, `validate_outbound_url_allow_private`, `ENV_OUTBOUND_ALLOW_ORIGINS`).

RustFS validates every operator-configured outbound destination to close a server-side request forgery (SSRF) class. Two layers exist:

| Layer | What it checks | Escape hatch |
| --- | --- | --- |
| Literal URL check (`validate_outbound_url`) | Host is not `localhost`, loopback, private, link-local, unspecified, or `169.254.169.254`; IPv4-mapped and IPv4-compatible IPv6 literals are classified by the embedded IPv4 | None |
| Full policy (`OutboundPolicy` + `OutboundDnsResolver`) | HTTP(S) URL without embedded credentials; additional shared, reserved, and known metadata address checks, plus re-validation of DNS answers on each new connection | `RUSTFS_OUTBOUND_ALLOW_ORIGINS` for the loopback, private, shared, and reserved classes |

## Which subsystem uses which layer

| Subsystem | Layer | Notes |
| --- | --- | --- |
| Event-notification webhooks (`RUSTFS_NOTIFY_WEBHOOK_*`) and audit webhooks (`RUSTFS_AUDIT_WEBHOOK_*`) | Full policy | Proxies disabled and redirects not followed, so the endpoint must be reachable directly (`crates/targets/src/target/webhook.rs`) |
| Target configuration validation (startup and admin API) | Full policy | `crates/targets/src/config/common.rs` `validate_outbound_http_url`; `rustfs/src/admin/handlers/target_descriptor.rs` |
| OIDC discovery, JWKS, and token requests | Full policy | A blocked provider logs `OIDC provider discovery blocked by outbound policy` naming the origin to allowlist (`crates/iam/src/oidc/provider.rs`, `crates/iam/src/oidc/transport.rs`) |
| Object Lambda targets | Full policy | `rustfs/src/admin/router.rs` `outbound_policy` |
| Bucket replication targets | Literal check, relaxed | Private addresses are always allowed; loopback only with `RUSTFS_REPLICATION_ALLOW_LOOPBACK_TARGET=true` (`crates/ecstore/src/bucket/remote_s3_client.rs` `validate_remote_endpoint`, shared with on-demand migration sources) |
| On-demand migration sources | Literal check, relaxed | Same guard and same escape hatch as replication targets (see below) |
| Site replication peers | Literal check | `rustfs/src/site_replication/mod.rs` |
| Self-hosted tiering warm backends (S3, MinIO, RustFS) | Literal check, relaxed | `validate_outbound_url_allow_private` allows private storage addresses except known IPv6 metadata addresses; RustFS retains its own debug-only loopback exception |
| Cloud tiering warm backends (Azure, GCS, Aliyun, Tencent, Huawei, R2) | Literal check | `crates/ecstore/src/services/tier/warm_backend.rs` `validate_endpoint`; Wasabi additionally requires its canonical provider endpoint before using the S3 constructor |
| Keystone `auth_url` | Literal check | `crates/keystone/src/config.rs` |

The allowlist affects only the "Full policy" rows. A literal-check subsystem rejects a hostname that is itself a restricted IP literal, does not re-check what a hostname resolves to, and cannot be widened by `RUSTFS_OUTBOUND_ALLOW_ORIGINS`.

S3, MinIO, and RustFS tiers accept RFC 1918 and ordinary IPv6 ULA addresses without an environment switch. The private-address exception retains the literal guard's other rejections and does not allow the known IPv6 metadata endpoints `fd00:ec2::254`, `fd00:ec2::23`, or `fd20:ce::254`. It does not add DNS filtering or change the other subsystems' policies. `RUSTFS_REPLICATION_ALLOW_LOOPBACK_TARGET` has no effect on tiers. RustFS's `RUSTFS_TIER_RUSTFS_ALLOW_LOOPBACK_ENDPOINT` remains a debug-build-only exception; release builds reject literal loopback endpoints.

The same tier endpoint check runs when constructing a driver from an existing configuration. Keep the original tier name and endpoint when recovering an upgrade that blocked a private IP; changing them can break existing destination bindings. Restoring the connection does not repair missing legacy cleanup metadata. See [tier free-version recovery](tier-ilm-debugging.md#free-version-recovery-controls).

On-demand migration sources share the replication egress guard exactly: the source endpoint an operator saves through `PUT /rustfs/admin/v3/on-demand-migration/{bucket}` goes through the same `validate_remote_endpoint` in `crates/ecstore/src/bucket/remote_s3_client.rs`, so private addresses are accepted (a source on the same private network is the normal migration case), loopback is refused unless `RUSTFS_REPLICATION_ALLOW_LOOPBACK_TARGET=true`, and metadata, link-local and unspecified addresses stay blocked. The variable is named for replication because it is one switch over one shared guard; setting it for a single-host migration also relaxes loopback for replication targets, so prefer a real hostname or private address for the source and keep the switch for lab and e2e hosts. `RUSTFS_OUTBOUND_ALLOW_ORIGINS` does not apply here. Two migration-specific rules sit on top of the endpoint guard and are enforced by the config validator, not by the egress layer: a source that names this bucket on this deployment is rejected as a self-reference, and a source matching one of the bucket's own replication targets is rejected as a replication loop. See [on-demand-migration.md](on-demand-migration.md).

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
