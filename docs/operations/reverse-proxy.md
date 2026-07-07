# Running RustFS behind a reverse proxy

RustFS speaks plain S3 over HTTP/1.1 and HTTP/2 and works behind reverse
proxies (Caddy, Nginx, HAProxy) and CDNs (Cloudflare). Most proxy problems are
**not** RustFS storage bugs — the same request sent directly to `:9000`
succeeds, while the proxied request fails. This page documents the request
semantics RustFS expects from the proxy layer and gives known-good
configurations.

> Rule of thumb: if a request works against `http://<host>:9000` directly but
> fails through the proxy, the fault is in the proxy/CDN request forwarding, not
> in RustFS object handling. Use the checklist below to find which forwarding
> behavior broke.

## What RustFS requires from the proxy

S3 clients sign requests with AWS SigV4. RustFS (via `s3s`) re-derives the
signature from the forwarded request, and streams the request body to storage.
For this to succeed the proxy must forward the request **byte-for-byte** with
respect to the signed material and the body:

1. **Do not alter the body.** No transparent compression, no re-encoding, no
   truncation. If the client sent `Content-Length: N`, exactly `N` body bytes
   must reach RustFS. If fewer bytes arrive, RustFS waits for the rest per the
   HTTP spec and the request appears to hang until the client aborts.
2. **Do not rewrite signed headers.** `Host` and any `x-amz-*` / signed headers
   must reach RustFS unchanged. Rewriting `Host` is fine only if the client
   signed with that same host.
3. **Preserve `Content-Length`; avoid re-chunking large bodies.** Some CDNs
   drop `Content-Length` and switch to `Transfer-Encoding: chunked`, or buffer
   the whole request body before forwarding — both change the timing and
   framing RustFS sees.
4. **Keep upstream idle keep-alive shorter than RustFS's, or vice-versa** (see
   next section) so the proxy never reuses a connection RustFS has already
   closed.
5. **Do not strip `ETag`** from responses (breaks multipart completion).

## Idle keep-alive: the #1 cause of `socket hang up` on writes

RustFS closes **idle** upstream HTTP/1.1 keep-alive connections after
`RUSTFS_HTTP1_HEADER_READ_TIMEOUT` seconds (default **75s**; see
`crates/config/src/constants/tls.rs`). Reverse proxies keep a pool of upstream
connections and reuse them. If the proxy's upstream idle-keepalive window is
**longer** than RustFS's timeout, the proxy can pick a connection that RustFS
has already FIN'd, write a request onto the dead socket, and the client sees:

```
TimeoutError: socket hang up          # ECONNRESET
AbortError: Request aborted
```

This is most visible on large `PutObject` uploads because:

- `PUT` is non-idempotent, so proxies will **not** transparently retry it; and
- a larger body keeps the connection in use longer, widening the race window,
  so small uploads on the same path often succeed.

### Fix — make the two windows agree

Pick **either** side; doing both is safest:

- **RustFS side:** keep `RUSTFS_HTTP1_HEADER_READ_TIMEOUT` (default 75s) *above*
  the proxy's upstream idle-keepalive. To harden slowloris protection on a
  directly-exposed node instead, lower it — but then also lower the proxy
  keepalive below it.
- **Proxy side:** lower the proxy's upstream idle-keepalive below RustFS's
  timeout, or disable upstream keep-alive entirely.

## Known-good Caddy configuration

```caddy
your-domain.example.com {
	reverse_proxy http://127.0.0.1:9000 {
		transport http {
			# Talk HTTP/1.1 to RustFS.
			versions 1.1

			# Keep the proxy's upstream idle-keepalive BELOW RustFS's
			# RUSTFS_HTTP1_HEADER_READ_TIMEOUT (default 75s) so Caddy never
			# reuses a connection RustFS already closed. Set to 0 to disable
			# upstream keep-alive entirely (simplest, slightly less efficient).
			keepalive 30s
			keepalive_idle_conns_per_host 0

			# Never let the proxy compress/transform the request body.
			compression off

			# Generous timeouts for multi-MB single-request PUTs.
			dial_timeout 30s
			read_timeout 300s
			write_timeout 300s
		}

		# Forward the body untouched; do not negotiate compression upstream.
		header_up Accept-Encoding identity

		# Preserve the host the client signed with.
		header_up Host {upstream_hostport}

		# Stream immediately instead of buffering.
		flush_interval -1
	}
}
```

## Nginx equivalent (essentials)

```nginx
location / {
    proxy_pass http://127.0.0.1:9000;
    proxy_http_version 1.1;

    # Nginx default upstream keepalive is 60s; keep it under RustFS's 75s.
    # (set `keepalive` in the matching `upstream {}` block)
    proxy_set_header Connection "";

    proxy_set_header Host $host;
    proxy_set_header Accept-Encoding "identity";

    # Do not buffer/limit large uploads.
    proxy_request_buffering off;
    client_max_body_size 0;
    proxy_read_timeout 300s;
    proxy_send_timeout 300s;
}
```

## Cloudflare (orange-cloud) caveats

Cloudflare's proxy (orange cloud) may **buffer the entire request body** before
forwarding, and can rewrite requests to `Transfer-Encoding: chunked`, dropping
the client's `Content-Length`. Symptoms match this pattern exactly: tiny uploads
succeed, larger uploads fail with `socket hang up`.

- For large object writes, prefer **DNS-only (grey cloud)** for the S3 endpoint,
  or a Cloudflare plan/tunnel configuration that does not buffer/re-chunk the
  request body.
- Force `Accept-Encoding: identity` so nothing in the path negotiates
  compression (see issues #609, #1492).
- Ensure `Content-Length` reaches RustFS; disable chunked re-encoding in tunnel
  settings (see issue #934).

## Diagnosis checklist

Run each step and note where behavior diverges:

1. **Bypass the proxy.** Send the failing request to `http://<host>:9000`
   directly. Success here confirms the fault is in the proxy/CDN path.
2. **Bypass the CDN, keep the proxy.** Point the proxy straight at the origin
   (Cloudflare grey cloud / direct DNS). If it now works, the CDN was
   buffering/re-chunking the body.
3. **Check idle reuse.** If failures are intermittent and correlate with upload
   size, it is almost always the keep-alive mismatch above. Lower the proxy
   keepalive (or disable it) and retry.
4. **Compare bytes.** Confirm the proxy forwards exactly `Content-Length` body
   bytes with no compression/transformation.
5. **Confirm signed headers survive.** `Host` and `x-amz-*` headers must reach
   RustFS unchanged; a `SignatureDoesNotMatch` (rather than a hang) points here.

## Related issues

- #3076 — Large single-request PutObject fails behind Caddy (this document)
- #609 — Bucket inaccessible via Cloudflare proxied DNS (`Accept-Encoding`)
- #1492 — SigV4 `SignatureDoesNotMatch` on Cloudflare tunnel (`Accept-Encoding`)
- #934 — Console fails behind Cloudflare tunnels (chunked / `Content-Length`)
- #1766 — Large multipart upload fails through Nginx (`ETag` stripping)
