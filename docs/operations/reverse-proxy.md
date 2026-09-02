# Running RustFS behind a reverse proxy

**Use this when:** a request succeeds against `http://<host>:9000` directly but fails, hangs, or resets through Caddy, Nginx, HAProxy, or Cloudflare.

**Source of truth:** `crates/config/src/constants/tls.rs` (`DEFAULT_HTTP1_HEADER_READ_TIMEOUT`, `DEFAULT_HTTP_REQUEST_BODY_READ_TIMEOUT`); the `put_object_body_read_stalled` log event.

RustFS speaks plain S3 over HTTP/1.1 and HTTP/2. Most proxy problems are not RustFS storage bugs: if the same request works directly against `:9000`, the fault is in proxy/CDN request forwarding. Use the checklist below to find which forwarding behavior broke.

## What RustFS requires from the proxy

S3 clients sign requests with AWS SigV4. RustFS (via `s3s`) re-derives the signature from the forwarded request and streams the request body to storage, so the proxy must forward the signed material and the body byte-for-byte:

| Requirement | Why |
| --- | --- |
| Do not alter the body (no compression, re-encoding, truncation). | If the client sent `Content-Length: N`, exactly `N` body bytes must arrive; with fewer, RustFS waits for the rest and the request appears to hang until the client aborts. |
| Do not rewrite signed headers (`Host`, `x-amz-*`). | Rewriting `Host` is fine only if the client signed with that same host; otherwise `SignatureDoesNotMatch`. |
| Preserve `Content-Length`; do not re-chunk or buffer large bodies. | Switching to `Transfer-Encoding: chunked` or buffering the whole body changes the framing and timing RustFS sees. |
| Keep the proxy's upstream idle keep-alive shorter than RustFS's timeout (next section). | Otherwise the proxy reuses a connection RustFS has already closed. |
| Do not strip `ETag` from responses. | Breaks multipart completion. |

## Idle keep-alive: the main cause of `socket hang up` on writes

RustFS closes idle upstream HTTP/1.1 keep-alive connections after `RUSTFS_HTTP1_HEADER_READ_TIMEOUT` seconds (`DEFAULT_HTTP1_HEADER_READ_TIMEOUT`, 75). Reverse proxies pool and reuse upstream connections. If the proxy's upstream idle-keepalive window is longer than RustFS's timeout, the proxy can pick a connection RustFS has already FIN'd, write a request onto the dead socket, and the client sees:

```text
TimeoutError: socket hang up          # ECONNRESET
AbortError: Request aborted
```

This is most visible on large `PutObject` uploads: `PUT` is non-idempotent, so proxies will not transparently retry it, and a larger body keeps the connection in use longer, widening the race window, so small uploads on the same path often succeed.

Fix by making the two windows agree (doing both is safest):

1. RustFS side: keep `RUSTFS_HTTP1_HEADER_READ_TIMEOUT` above the proxy's upstream idle-keepalive. To harden slowloris protection on a directly exposed node instead, lower it, and then also lower the proxy keepalive below it.
2. Proxy side: lower the proxy's upstream idle-keepalive below RustFS's timeout, or disable upstream keep-alive entirely.

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

Cloudflare's proxy may buffer the entire request body before forwarding and can rewrite requests to `Transfer-Encoding: chunked`, dropping the client's `Content-Length`. The symptom is exactly the pattern above: tiny uploads succeed, larger uploads fail with `socket hang up`. For large object writes prefer DNS-only (grey cloud) for the S3 endpoint, or a plan/tunnel configuration that does not buffer or re-chunk the body. The `Accept-Encoding` and `Content-Length` rows in the issue table below are the Cloudflare-specific failures seen so far.

## Diagnosis checklist

1. Bypass the proxy. Send the failing request to `http://<host>:9000` directly. Success confirms the fault is in the proxy/CDN path.
2. Bypass the CDN, keep the proxy. Point the proxy straight at the origin (Cloudflare grey cloud / direct DNS). If it now works, the CDN was buffering or re-chunking the body.
3. Check idle reuse. Intermittent failures that correlate with upload size are almost always the keep-alive mismatch. Lower the proxy keepalive (or disable it) and retry.
4. Check for a truncated body. If the upload hangs indefinitely rather than resetting, the proxy is forwarding a partial body and then going silent without closing the connection. RustFS bounds this wait with `RUSTFS_HTTP_REQUEST_BODY_READ_TIMEOUT` (`DEFAULT_HTTP_REQUEST_BODY_READ_TIMEOUT`, 300; `0` disables) and on timeout logs `put_object_body_read_stalled` with the received/expected byte counts.
5. Compare bytes. Confirm the proxy forwards exactly `Content-Length` body bytes with no compression or transformation.
6. Confirm signed headers survive. `Host` and `x-amz-*` must reach RustFS unchanged; a `SignatureDoesNotMatch` (rather than a hang) points here.

## Known failure signatures

| Symptom | Forwarding fault | Issue |
| --- | --- | --- |
| Large single-request PutObject fails behind Caddy | Upstream idle keep-alive longer than RustFS's timeout | #3076 |
| Bucket inaccessible via Cloudflare proxied DNS | `Accept-Encoding` negotiation / body transformation | #609 |
| SigV4 `SignatureDoesNotMatch` on Cloudflare tunnel | `Accept-Encoding` header rewritten | #1492 |
| Console fails behind Cloudflare tunnels | Chunked re-encoding drops `Content-Length` | #934 |
| Large multipart upload fails through Nginx | `ETag` stripped from responses | #1766 |
