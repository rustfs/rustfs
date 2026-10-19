# Serving RustFS under a URL path prefix

**Use this when:** several RustFS instances (operator Tenants or Helm releases in different namespaces) share one host and are told apart by path, for example `https://tools.example.com/tools/rustfs/...`, or when a client gets `SignatureDoesNotMatch` through a proxy that rewrites the path.

**Source of truth:** `rustfs/src/server/prefix.rs` (validation and combination), `rustfs/src/server/layer.rs` (prefix handling in the request stack), `crates/config/src/constants/app.rs` (`ENV_RUSTFS_API_PREFIX`, `ENV_RUSTFS_CONSOLE_PREFIX`), `crates/config/src/constants/proxy.rs` (`RUSTFS_TRUSTED_PROXY_*`).

For keep-alive, body streaming, and Cloudflare behavior that applies to every proxy setup, read [reverse-proxy.md](reverse-proxy.md) first. This page covers only what changes when the public URL has a path prefix.

## What it enables

Without a prefix, one hostname serves one RustFS instance, because the S3 API owns `/`. With a prefix, the proxy routes by path and each instance gets its own URL space:

| Instance | S3 / admin API | Console |
| --- | --- | --- |
| namespace `team-a` | `https://tools.example.com/team-a/api` | `https://tools.example.com/team-a/console/` |
| namespace `team-b` | `https://tools.example.com/team-b/api` | `https://tools.example.com/team-b/console/` |

Stock SigV4 clients (aws-cli, AWS SDKs) work with path-style addressing and an endpoint that includes the prefix. Header-signed and presigned requests are both supported.

## Requirements

The feature needs an s3s version that provides `SignedUriPath` (the hook that lets RustFS hand s3s the path the client signed). A RustFS build against an s3s without it does not support `RUSTFS_API_PREFIX`.

## Environment variables

Both variables are read once at startup. An invalid value stops the server with an `InvalidInput` error that names the variable.

| Variable | Default | Rule |
| --- | --- | --- |
| `RUSTFS_API_PREFIX` | unset: no prefix, behavior unchanged | Absolute path, not `/`, at most 256 bytes, one trailing `/` is trimmed. Segments are non-empty and use only `A-Za-z0-9._~-`; `.` and `..` are rejected. Must not overlap a reserved RustFS route (see below), must not equal the console prefix, and must not be nested under it. An empty string counts as unset. |
| `RUSTFS_CONSOLE_PREFIX` | `/rustfs/console`, or `<RUSTFS_API_PREFIX>/rustfs/console` when `RUSTFS_API_PREFIX` is set | Same syntax and reserved-route check. May be nested under the API prefix. An empty string is rejected (it does not mean "default"); unset the variable instead. |

Reserved routes (a prefix is rejected if it equals, is nested under, or is a parent of any of these): `/rustfs/admin`, `/minio/admin`, `/iceberg/v1`, `/_iceberg/v1`, `/rustfs/rpc`, `/rustfs/peer`, `/node_service.NodeService`, `/health`, `/minio/health`, `/profile`, `/index.html`, `/favicon.ico`, and the two `apple-touch-icon` files. For example `/rustfs` and `/minio` are rejected; `/tools/rustfs` is fine.

| Mode | Settings | API URL | Console URL |
| --- | --- | --- | --- |
| none | both unset | `/` | `/rustfs/console` |
| (a) one variable | `RUSTFS_API_PREFIX=/tools/rustfs` | `/tools/rustfs` | `/tools/rustfs/rustfs/console` |
| (b) two variables | `RUSTFS_API_PREFIX=/tools/rustfs/api`, `RUSTFS_CONSOLE_PREFIX=/tools/rustfs/console` | `/tools/rustfs/api` | `/tools/rustfs/console` |

In mode (a), once the console moves under the API prefix the unprefixed `/rustfs/console` is no longer the console; a `GET /` on the console port redirects to the new prefix. Port-forward users should open the prefixed URL.

Behavior to know:

- Prefix support is additive. Unprefixed paths keep working, so pod probes (`/health`), internode traffic, the operator, and in-cluster clients that reach the pod directly need no change.
- With `RUSTFS_API_PREFIX` set, RustFS injects a small script as the first child of `<head>` in the console's HTML pages. It points the console's API base at `location.origin` plus the prefix. Without it the console keeps using the page origin and cannot work under a path. Only `.html` assets are changed.
- With `RUSTFS_API_PREFIX` set, the OIDC callback URL sent to the identity provider (`<prefix>/rustfs/admin/v3/oidc/callback/<provider>`) and the object ZIP download URLs (`<prefix>/rustfs/admin/v3/object-zip-downloads/...`) include the prefix. Register the prefixed callback URL with your identity provider.
- Bucket shadowing: with prefix `/tools/rustfs/api`, keys under `rustfs/api/` in a bucket named `tools` cannot be reached path-style without the prefix, by any encoding, because that path belongs to the prefix. They remain reachable through the prefix itself (`/tools/rustfs/api/tools/rustfs/api/...`). This is the same class as the existing `/rustfs/console` shadow. Choose a first segment that is not, and will not become, a bucket name.
- Send the prefix literally. A request whose path matches the prefix only after percent-decoding (for example `/tools/rustf%73/...`) is rejected with an empty HTTP 400 response, not passed through. Every SigV4 client sends the prefix literally, so this affects only hand-built requests.
- Internode paths (`/rustfs/rpc`, `/node_service.NodeService`, `/rustfs/peer`) are never served through the API prefix. Expose only the prefix at the proxy and keep internode traffic on the cluster network.

## Supported proxy mode: pass the path through unchanged

The proxy must forward the request path exactly as the client sent it, including the prefix. RustFS removes the prefix itself, after it records the original path.

Why this is required. A SigV4 signature covers the canonical request, and the canonical request contains the URI path. A client talking to `https://tools.example.com/tools/rustfs/api/bucket/key` signs `/tools/rustfs/api/bucket/key`. RustFS verifies the signature against that exact path and routes on the remainder (`/bucket/key`). If the proxy strips the prefix, RustFS receives `/bucket/key`, computes a different canonical request, and answers `SignatureDoesNotMatch`. Presigned URLs fail the same way.

This is why these common patterns do not work:

- Envoy `prefix_rewrite` / `regex_rewrite` that removes the prefix.
- ingress-nginx `nginx.ingress.kubernetes.io/rewrite-target`.
- Traefik `StripPrefix` / `StripPrefixRegex` middleware.
- nginx `proxy_pass http://upstream/;` (a URI part replaces the matched location).

This is the setup behind issue #2433.

`X-Forwarded-Prefix` is deliberately ignored. A header is client-influenced, and the prefix is part of what the signature check trusts, so it comes only from the static `RUSTFS_API_PREFIX` setting.

## What the proxy must preserve

| Requirement | Why |
| --- | --- |
| Forward the path and query byte for byte. No prefix rewrite, no decoding (nginx `$uri`), no slash merging or dot-segment normalization, no `%2F` unescaping, no re-encoding. | The path is signed. A changed byte is a different canonical request. |
| `Host` as the client sent it, including a non-default port (nginx `$http_host`, not `$host`). | SigV4 signs `host`. `$host` drops the port, so `https://h:8443/...` fails. |
| `Authorization` and every `x-amz-*` header unchanged: `x-amz-date`, `x-amz-content-sha256`, `x-amz-security-token`, `x-amz-decoded-content-length`. | They are signed or carry signed state. |
| `Content-MD5`, `Content-Type`, `Content-Length`, `Transfer-Encoding`, and `Content-Encoding: aws-chunked` unchanged. | Streaming uploads (aws-chunked) sign each chunk; re-chunking or compressing breaks them. |
| No request buffering, compression, or body size limit on the API path. | Large uploads; see [reverse-proxy.md](reverse-proxy.md). |
| `X-Forwarded-Proto` and `X-Forwarded-For` set by the proxy, overwriting any client-sent values. | The OIDC code reads `X-Forwarded-Proto` from any request, whether or not it comes from a trusted proxy (`RUSTFS_TRUSTED_PROXY_*` does not gate it), and takes the host from `Host`. A client-sent value would shape the callback URL. They are not signature inputs. |
| `Upgrade` / `Connection` passthrough on the console path if your setup relies on it. | See the Cloudflare notes in [reverse-proxy.md](reverse-proxy.md). |

To pin the OIDC callback origin regardless of request headers, set `RUSTFS_BROWSER_REDIRECT_URL` (scheme and authority only, no path) or an explicit OIDC redirect URI on the provider. The callback URL is then `RUSTFS_BROWSER_REDIRECT_URL` followed by `<api_prefix>/rustfs/admin/v3/oidc/callback/<provider>`.

Route both the API prefix and the console prefix to port 9001 (the `console` port). The 9001 listener serves the console and the full S3/admin API, and it strips the API prefix itself. In mode (a) one rule for the API prefix is enough, because the console lives under it.

Why not 9000 for the API prefix: the embedded console requests `<API prefix><console prefix>/version`, for example `/tools/rustfs/api/tools/rustfs/console/version`. Port 9000 (`endpoint`) has no console routes, so it treats that request as an S3 request and answers 403 or 501 NotImplemented, and the console fails to load its version info. Routing the API prefix to 9000 still works for S3 clients, but anything the console requests under `<api><console>/...` fails. Use 9000 only for an S3-only setup without the console.

## Security: shared origin

Instances that share one hostname share one browser origin. An HTML object served from one tenant's bucket (or opened through a presigned link) can run script in the same origin as another tenant's console. Paths are not a browser security boundary.

- The embedded console keeps its session in browser storage under fixed keys (for example `auth.credentials`), and that storage is per origin. Consoles of several instances on one hostname therefore overwrite each other's session: logging in to one logs the browser out of the other, and two open console tabs keep expiring each other's session (`InvalidAccessKeyId` in the server log). One browser profile can be signed in to one console per hostname at a time.
- For multi-tenant or untrusted-user setups, use a separate hostname per console. The S3 APIs may share a hostname.
- If you share the origin anyway, have the proxy add `X-Content-Type-Options: nosniff` and `Content-Security-Policy: sandbox` (or `Content-Disposition: attachment`) on API-prefix responses. See the nginx example. This can break browser-rendered objects such as static sites.

## Examples

All examples send both `/tools/rustfs/api` and `/tools/rustfs/console` to `:9001` (mode b). For mode (a), a single rule sending `/tools/rustfs` to `:9001` covers the API and the console.

### Nginx

```nginx
# Port 9001 serves the console and the full API.
upstream rustfs_console { server 127.0.0.1:9001; keepalive 16; }

server {
    listen 443 ssl;
    server_name tools.example.com;

    # No URI part in proxy_pass: nginx then forwards the request URI as the
    # client sent it.
    location = /tools/rustfs/api  { proxy_pass http://rustfs_console; include /etc/nginx/rustfs-proxy.conf; }
    location /tools/rustfs/api/   { proxy_pass http://rustfs_console; include /etc/nginx/rustfs-proxy.conf; }

    location = /tools/rustfs/console { proxy_pass http://rustfs_console; include /etc/nginx/rustfs-proxy.conf; }
    location /tools/rustfs/console/  { proxy_pass http://rustfs_console; include /etc/nginx/rustfs-proxy.conf; }
}
```

`/etc/nginx/rustfs-proxy.conf`:

```nginx
proxy_http_version 1.1;
proxy_set_header Connection "";          # or Upgrade handling for the console
proxy_set_header Host $http_host;        # keeps the client's port
# Overwrite client-sent forwarding headers.
proxy_set_header X-Forwarded-Proto $scheme;
proxy_set_header X-Forwarded-Host $http_host;
proxy_set_header Forwarded "";
proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
proxy_set_header Accept-Encoding "identity";
proxy_request_buffering off;
proxy_buffering off;
client_max_body_size 0;
proxy_read_timeout 300s;
proxy_send_timeout 300s;
```

Optional hardening for the API location only, for shared-origin setups (see [Security: shared origin](#security-shared-origin)). It can break objects meant to render in the browser, such as static sites:

```nginx
location /tools/rustfs/api/ {
    proxy_pass http://rustfs_console;
    include /etc/nginx/rustfs-proxy.conf;
    add_header X-Content-Type-Options "nosniff" always;
    add_header Content-Security-Policy "sandbox" always;   # or: add_header Content-Disposition "attachment" always;
}
```

Because `add_header` in a `location` replaces inherited ones, repeat any other headers you need there.

### Envoy

Working: match on the prefix and do not rewrite the path.

```yaml
# HttpConnectionManager
normalize_path: false
merge_slashes: false
path_with_escaped_slashes_action: KEEP_UNCHANGED
route_config:
  virtual_hosts:
  - name: tools
    domains: ["tools.example.com"]
    routes:
    # Both clusters point at port 9001.
    - match: { path_separated_prefix: /tools/rustfs/api }
      route: { cluster: rustfs_console, timeout: 0s }
    - match: { path_separated_prefix: /tools/rustfs/console }
      route: { cluster: rustfs_console, timeout: 0s }
```

Not working: a typical config that strips the prefix, which is the shape of the #2433 setup (written here as a representative example, not copied from the issue).

```yaml
    - match: { prefix: /tools/rustfs/api }
      route:
        cluster: rustfs_console
        prefix_rewrite: /          # <- breaks SigV4: SignatureDoesNotMatch
```

With `path_separated_prefix`, `/tools/rustfs/api` and `/tools/rustfs/api/...` match, but `/tools/rustfs/apix` does not.

### Traefik

`IngressRoute` with `PathPrefix` and no `StripPrefix` middleware:

```yaml
apiVersion: traefik.io/v1alpha1
kind: IngressRoute
metadata:
  name: rustfs
  namespace: team-a
spec:
  entryPoints: [websecure]
  routes:
  - match: Host(`tools.example.com`) && PathPrefix(`/tools/rustfs/api`)
    kind: Rule
    services:
    - { name: rustfs-svc, port: console }
  - match: Host(`tools.example.com`) && PathPrefix(`/tools/rustfs/console`)
    kind: Rule
    services:
    - { name: rustfs-svc, port: console }
  tls: {}
```

Do not attach a `StripPrefix` or `StripPrefixRegex` middleware. Traefik does not buffer request bodies unless a `buffering` middleware is attached.

### Kubernetes Ingress (ingress-nginx)

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: rustfs
  namespace: team-a
  annotations:
    nginx.ingress.kubernetes.io/proxy-body-size: "0"
    nginx.ingress.kubernetes.io/proxy-request-buffering: "off"
    # Do NOT set nginx.ingress.kubernetes.io/rewrite-target.
spec:
  ingressClassName: nginx
  tls:
  - hosts: [tools.example.com]
    secretName: tools-tls
  rules:
  - host: tools.example.com
    http:
      paths:
      - path: /tools/rustfs/api
        pathType: Prefix
        backend: { service: { name: rustfs-svc, port: { name: console } } }
      - path: /tools/rustfs/console
        pathType: Prefix
        backend: { service: { name: rustfs-svc, port: { name: console } } }
```

Several namespaces can share one host with one Ingress object each, using different paths. Ingresses with the same host must reference a certificate that covers it (a TLS secret per namespace holding the same certificate, or an ingress-nginx default certificate).

## Helm

Set the prefixes in values (the ConfigMap renders `RUSTFS_API_PREFIX` / `RUSTFS_CONSOLE_PREFIX` only when non-empty) and add prefixed `ingress.hosts[].paths[]`:

```yaml
config:
  rustfs:
    api_prefix: /tools/rustfs/api
    console_prefix: /tools/rustfs/console
ingress:
  enabled: true
  className: nginx
  hosts:
  - host: tools.example.com
    paths:
    - { path: /tools/rustfs/api, pathType: Prefix, servicePort: console }
    - { path: /tools/rustfs/console, pathType: Prefix, servicePort: console }
```

Both paths use `servicePort: console` (9001); in mode (a) a single path for the API prefix is enough. Do not set `nginx.ingress.kubernetes.io/rewrite-target`. The `gatewayApi` and `contour` options of the chart route `/` only and do not support prefixes. See [helm/README.md](../../helm/README.md) for the full value reference.

## Operator

The Tenant's `spec.env` list sets additional RustFS container environment variables. Availability depends on the operator version; this was checked against the `main` branch of rustfs/operator, not against every release.

```yaml
apiVersion: rustfs.com/v1alpha1
kind: Tenant
metadata:
  name: rustfs
  namespace: team-a
spec:
  env:
  - name: RUSTFS_API_PREFIX
    value: /tools/rustfs/api
  - name: RUSTFS_CONSOLE_PREFIX
    value: /tools/rustfs/console
```

Check the `apiVersion` against your installed CRD. The operator's own Ingress routes `/` only, so disable it and create an Ingress like the one above whose backends (for the API prefix and the console prefix) are the `<tenant>-console` Service on port 9001. When the `rustfs-tenant` chart sets `env`, it replaces the whole list, so include every variable you need.

## Clients

aws-cli (`~/.aws/config`):

```ini
[profile tools]
region = us-east-1
s3 =
    addressing_style = path
    signature_version = s3v4
```

```bash
aws --profile tools --endpoint-url https://tools.example.com/tools/rustfs/api/ s3 ls
```

aws-cli 2.37 was verified with an endpoint without a trailing slash (create bucket, single and multipart uploads, listing, download, presigned GET); botocore inserts the separator itself. The trailing slash is only required for the Rust SDK.

AWS SDKs: enable path-style (`force_path_style(true)` in Rust, `forcePathStyle: true` in JS, `addressing_style: path` in boto3) and use an endpoint that includes the prefix **with a trailing slash**, for example `http://host/tools/rustfs/api/`. Verified with aws-sdk-s3 (Rust) 1.152: without the trailing slash the SDK joins the endpoint path and the bucket with no separator (`.../apibkt/...`) and the request misses the prefix. Other SDKs were not checked; use the trailing slash there too. Virtual-hosted style is not supported together with a prefix. Presigned URLs include the prefix because the SDK builds them from the endpoint.

MinIO `mc` is not supported: minio-go rejects endpoint URLs that contain a path. Unverified here (high confidence, from minio-go behavior); test before relying on it.

## Known limitations

- Some console features build absolute paths from the page origin (for example `/rustfs/admin/v4/runtime/capabilities`). Those requests leave the prefix and return 404 or hit another instance. Fixing this needs a change in the console frontend, not in the server.
- Server logs and traces show the path with the prefix removed. Add the prefix yourself when correlating with proxy logs.
- Bucket shadowing, described above.
- The `Location` field in the CompleteMultipartUpload response does not include the prefix. SDKs ignore it; aws-cli prints it.
- The chart's `gatewayApi` and `contour` routes cover `/` only.

## Manual verification

### Console (nginx, mode b)

1. Start RustFS with `RUSTFS_API_PREFIX=/tools/rustfs/api` and `RUSTFS_CONSOLE_PREFIX=/tools/rustfs/console`, and put the nginx config above in front of it.
2. Open `https://tools.example.com/tools/rustfs/console/` with browser devtools (Network tab, preserve log) open.
3. Log in with an access key and secret key, list buckets, create a bucket, upload an object, download it.
4. Every request must start with `/tools/rustfs/`. Anything else is a known limitation (above) or a problem; see Troubleshooting.

### aws-cli

```bash
export AWS_PROFILE=tools
EP=https://tools.example.com/tools/rustfs/api    # aws-cli does not need a trailing slash
aws --endpoint-url $EP s3 mb s3://prefix-test
echo hello > hello.txt
aws --endpoint-url $EP s3 cp hello.txt s3://prefix-test/hello.txt
aws --endpoint-url $EP s3 ls s3://prefix-test
URL=$(aws --endpoint-url $EP s3 presign s3://prefix-test/hello.txt --expires-in 300)
echo "$URL"          # the path starts with /tools/rustfs/api/prefix-test/
curl -fsS "$URL"     # prints: hello
```

Expected: every command succeeds. Removing `/tools/rustfs/api` from the presigned URL must return 403 `SignatureDoesNotMatch`.

## Troubleshooting

`SignatureDoesNotMatch`:

1. The proxy strips or rewrites the prefix (`prefix_rewrite`, `rewrite-target`, `StripPrefix`, `proxy_pass` with a URI part). Compare the path RustFS receives with the path the client signed.
2. `Host` lost its port or was replaced (`$host` instead of `$http_host`).
3. The path was normalized, decoded, or re-encoded (merged slashes, `%2F`, `$uri`).
4. The client endpoint prefix differs from `RUSTFS_API_PREFIX`. Matching is case-sensitive.
   An SDK endpoint without a trailing slash (`.../api` instead of `.../api/`) produces URLs like `.../apibkt/key`, which do not match the prefix. Add the trailing slash. aws-cli does not need it.
5. The client is `mc` (unsupported with a path endpoint).
6. Clock skew between client and server.
7. The proxy modified `x-amz-*`, `Content-MD5`, or the body framing.

Console requests go to `/` or `/rustfs/admin/...` without the prefix:

1. `RUSTFS_API_PREFIX` is not set on the instance, so the console is not told about the prefix. Check the container environment.
2. The browser cached the HTML. Hard refresh.
3. A known console limitation (above).

Console shows "Failed to get release info", or `GET <api><console>/version` (for example `/team-b/rustfs/api/team-b/rustfs/console/version`) returns 501 NotImplemented or 403: the API prefix is routed to port 9000, which has no console routes and treats the request as S3. Route the API prefix to port 9001 (Helm `servicePort: console`); RustFS strips the API prefix and the console answers 200 with the version JSON.

Console assets return 404: the console prefix does not reach port 9001, or the proxy strips the console prefix. Confirm `curl -I https://tools.example.com/tools/rustfs/console/index.html` returns 200.
