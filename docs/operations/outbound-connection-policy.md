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
| Bucket replication targets and on-demand migration sources | Storage endpoint policy | Filters the configured URL and DNS answers. Private storage addresses are supported. Loopback requires `RUSTFS_REPLICATION_ALLOW_LOOPBACK_TARGET=true`. |
| Site replication peers | Literal check | `rustfs/src/site_replication/mod.rs` |
| Self-hosted S3, MinIO and RustFS tiers | Storage endpoint policy | Checks DNS answers on each connection. RustFS also has a debug-only loopback setting. |
| Azure Blob, Aliyun, legacy Azure, Tencent, Huawei and R2 tiers | Full policy | Filters DNS on direct connections. Private virtual-hosted tiers need exact origins for both the endpoint and the bucket hostname, including any non-default port. Wasabi requires its canonical provider endpoint. |
| Native GCS tier | Fixed service endpoint | Both data and control clients use `https://storage.googleapis.com`. Omit the tier endpoint or specify that HTTPS origin. The Google SDK manages DNS for this origin. |
| Keystone identity service | Full policy | Connects directly to the configured identity endpoint and disables redirects. |

S3, MinIO and RustFS tiers accept RFC 1918 and ordinary IPv6 ULA addresses. Known metadata addresses remain forbidden. The RustFS tier supports `RUSTFS_TIER_RUSTFS_ALLOW_LOOPBACK_ENDPOINT` in debug builds. Replication, migration and self-hosted storage tiers keep their own loopback rules independently of `RUSTFS_OUTBOUND_ALLOW_ORIGINS`.

Before upgrading a cluster with native GCS tiers, check each tier’s endpoint. A custom hostname, IP address or emulator endpoint will prevent the tier from opening, including reads of objects already transitioned there. Verify that the same objects and generations are available through Google’s service endpoint before switching. If the backing store requires its custom endpoint, migrate the data while the previous release can still read it. `RUSTFS_OUTBOUND_ALLOW_ORIGINS` cannot enable a custom native GCS tier endpoint.

OIDC, Keystone, native Azure Blob tiering and native Azure/GCS migration connect directly so RustFS can filter the destination at connection time. Nodes that previously used an HTTP forward proxy for these services need direct egress to the configured endpoints. For a Keystone installation at `http://keystone:5000` on a private network, include that exact origin in `RUSTFS_OUTBOUND_ALLOW_ORIGINS` before upgrading.

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
