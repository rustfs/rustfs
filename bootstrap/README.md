# RustFS image bootstrap

The official image bundles this directory at `/etc/rustfs/bootstrap`. The default `buckets.json` is an empty JSON array, so the image starts RustFS directly and does not require a bootstrap endpoint or bootstrap credentials.

To create buckets, replace `buckets.json` in a derived image with a JSON array of bucket names. The entrypoint creates each bucket if it does not exist and applies `public-access-block.json`, whose default blocks all four public access paths. Edit that file in the derived image only when public access is an explicit requirement.

Optional per-bucket files use the bucket name as the filename without an extension:

| Directory | File format | Applied operation |
| --- | --- | --- |
| `policies/` | S3 bucket policy JSON | `rc bucket anonymous set-json` |
| `versioning/` | JSON boolean (`true` or `false`) | Enable or suspend bucket versioning |
| `lifecycle/` | `rc bucket lifecycle rule import` JSON | Replace lifecycle rules |
| `cors/` | JSON or XML accepted by `rc bucket cors set` | Replace CORS rules |
| `notifications/` | JSON array of `{ "arn": "...", "events": ["..."] }` | Add or replace event rules by ARN (prefix/suffix filters are not currently accepted) |

Every per-bucket file must correspond to a bucket listed in `buckets.json`. Applying these files is repeatable: bucket creation ignores existing buckets, while each configuration operation replaces or reconciles that bucket's requested state.

When `buckets.json` is non-empty, set `RUSTFS_BOOTSTRAP_ENDPOINT` to the RustFS S3 endpoint reachable from the container, for example `http://rustfs:9000` for a Compose service or the internal service DNS name in Kubernetes. The entrypoint waits on that endpoint's `/health/ready`; it never assumes `localhost`. `RUSTFS_BOOTSTRAP_TIMEOUT` controls the readiness wait in seconds (default `120`, maximum `3600`). `RUSTFS_BOOTSTRAP_DIR` can point to a different in-image configuration directory; it defaults to `/etc/rustfs/bootstrap`. Set `RUSTFS_ACCESS_KEY` and `RUSTFS_SECRET_KEY`, or their `_FILE` variants, for bootstrap authentication. The endpoint and credentials are read at runtime and are not baked into the image.

RustFS built-in IAM policies remain unchanged; the default bootstrap installs no custom bucket policies, lifecycle rules, versioning, CORS rules, or notification rules. The bundled RustFS CLI is resolved from a published release and its release checksum is verified. Set the Docker build argument `RC_RELEASE` to a version to pin it. Build the RustFS image after a CLI release containing the `bucket public-access-block` commands is available. For Kubernetes, the configured service must route traffic to the starting pod before its readiness probe succeeds (for example, with `publishNotReadyAddresses: true`); otherwise the service endpoint cannot reach the pod while the entrypoint waits for readiness.
