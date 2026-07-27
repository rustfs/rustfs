# Outbound Connection Policy

This document describes the outbound connection policy that RustFS applies to
server-initiated HTTP(S) requests, and the `RUSTFS_OUTBOUND_ALLOW_ORIGINS`
allowlist operators can use to reach endpoints on private or container networks.

It is written for operators who upgraded to `1.0.0-beta.11` (or later) and found
that event-notification webhooks, audit webhooks, or other outbound integrations
stopped reaching endpoints that worked before — typically Docker Compose service
names, `host.docker.internal`, or RFC 1918 addresses.

## Background: what the policy protects

Several RustFS subsystems open connections to operator-configured URLs. To close
a server-side request forgery (SSRF) class of problem, RustFS validates every such
destination and re-checks the addresses returned by DNS on each new connection,
so a hostname cannot be rebound to a restricted address after it is first
accepted.

The policy governs the outbound clients used by:

- event-notification webhooks (`RUSTFS_NOTIFY_WEBHOOK_*`);
- audit webhooks (`RUSTFS_AUDIT_WEBHOOK_*`);
- OIDC identity-provider requests;
- S3 tiering (warm-backend) endpoints;
- Keystone auth URLs.

The webhook and audit outbound clients also **disable proxies and do not follow
redirects**, so the destination must be reachable directly at the configured URL.

## What changed in beta.11

| | beta.10 | beta.11+ |
|---|---|---|
| Literal `localhost` / private / loopback IPs | Rejected | Rejected |
| Hostnames that resolve to private/loopback addresses (`logstash`, `host.docker.internal`, Compose service DNS, …) | Allowed | **Blocked at DNS/connect time** unless allowlisted |
| Escape hatch for private destinations | None | `RUSTFS_OUTBOUND_ALLOW_ORIGINS` |
| Proxies / redirects for outbound clients | Followed | Disabled |

Before beta.11 a webhook endpoint whose hostname happened to resolve to a
private address was accepted. Beta.11 fails that resolution check unless the
exact origin is on the allowlist. This is why a Compose setup that delivered
events on beta.10 can go silent after the upgrade even though the configuration
is unchanged.

## Symptoms

- Bucket event rules and webhook configuration look correct.
- Uploads and audited API calls succeed.
- No HTTP POST reaches the internal webhook receiver.
- The target may appear offline or fail activation when its endpoint resolves to
a loopback, private, shared, or reserved address.
- Startup or target validation reports `webhook endpoint is not allowed: ...`
  with a reason such as `private address` or `loopback host`.

## `RUSTFS_OUTBOUND_ALLOW_ORIGINS`

`RUSTFS_OUTBOUND_ALLOW_ORIGINS` is a comma-separated list of exact HTTP(S)
origins that are permitted to resolve to otherwise-restricted addresses. It is an
operator-owned process setting read once at startup; individual target
configuration cannot extend it.

```bash
# exact scheme://host:port — comma-separate multiple origins
RUSTFS_OUTBOUND_ALLOW_ORIGINS=http://logstash:8080,http://host.docker.internal:3020
```

### Origin format rules

Each entry is matched as an **exact origin** (`scheme://host:port`):

- The scheme must be `http` or `https`.
- The host and port must match the destination exactly. An allowlisted
  `http://logstash:8080` does **not** authorize `http://logstash:9090` or
  `https://logstash:8080`.
- If the port is omitted, the scheme's default is used (`80` for `http`, `443`
  for `https`); the destination must then use that same default port.
- Entries must be origins only. A trailing `/` is accepted, but a path, query,
  or fragment (for example `http://logstash:8080/events`) is **rejected** as an
  invalid origin — the process fails closed rather than silently ignoring the
  path.
- Userinfo (`http://user:pass@host`) is not allowed.
- An empty entry (for example a trailing or doubled comma) is rejected.

An invalid list fails closed: the affected subsystem reports an
`invalid outbound policy` / `invalid origin at position N` error instead of
starting with a partially applied allowlist.

### What stays blocked even when allowlisted

Allowlisting an origin only relaxes the loopback, private, shared, and reserved
address classes for that exact origin. The following remain forbidden for every
origin, allowlisted or not:

- cloud metadata endpoints (for example `169.254.169.254` and the other
  well-known IMDS addresses);
- link-local addresses (`169.254.0.0/16`, `fe80::/10`);
- the unspecified address (`0.0.0.0`, `::`);
- IPv4-mapped, IPv4-compatible, and NAT64/6to4-embedded forms of any of the
  above (RustFS classifies the embedded IPv4 destination, so `::ffff:127.0.0.1`
  and similar cannot be used to bypass the policy).

The allowlist authorizes only the exact host you name. A DNS answer for a
different hostname that points at a private address is still rejected, and each
new connection re-validates the resolved addresses so a rebinding answer fails
closed.

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
      # Note: the allowlist takes the origin only, without the /events path.
      RUSTFS_OUTBOUND_ALLOW_ORIGINS: "http://logstash:8080"
  logstash:
    image: docker.elastic.co/logstash/logstash:8.15.0
    # ...
```

The endpoint keeps its full path (`/events`); the allowlist entry is the origin
(`http://logstash:8080`) only.

## Upgrade checklist (beta.10 → beta.11+)

1. List every outbound endpoint whose hostname resolves to a loopback, private,
   shared, or reserved address: notification webhooks, audit webhooks, OIDC
   providers, tiering endpoints, and Keystone auth URLs.
2. Add each one to `RUSTFS_OUTBOUND_ALLOW_ORIGINS` as an exact
   `scheme://host:port` origin (no path).
3. Ensure the endpoint is reachable directly — for webhook and audit targets,
   proxies are disabled and redirects are not followed.
4. Restart RustFS; the policy is read at startup.
5. Confirm delivery, and check the logs for `... is not allowed` messages if a
   target still fails to activate.
