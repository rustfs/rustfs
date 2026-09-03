# On-Demand Migration

**Use this when:** you are moving an existing S3-compatible bucket into RustFS without a stop-the-world copy, or you are debugging a bucket that serves reads from an external source (424 `SourceUnavailable`, an open circuit breaker, missing pulled objects, a source 403).
**Source of truth:** `crates/ecstore/src/bucket/on_demand_migration/` (`config.rs` for the wire model and its bounds, `sys.rs` for the per-node runtime, `source_client.rs` for the outbound client, `pull.rs` for the write-back pipeline, `breaker.rs` and `negative_cache.rs` for the protections), `rustfs/src/app/object/get.rs` and `head.rs` for the read paths, `rustfs/src/app/object/on_demand_migration_put.rs` for the local write, `rustfs/src/admin/handlers/on_demand_migration.rs` for the admin API, and `crates/obs/src/metrics/schema/on_demand_migration.rs` for the metric contract.

On-Demand Migration (ODM) attaches an external S3-compatible **source bucket** to a local RustFS bucket. When a client GETs a key that does not exist locally, RustFS fetches it from the source, streams it to the client, and stores it locally in the same pass; every later read is served locally. It is a pull-style, lazy migration path — the RustFS equivalent of Cloudflare R2 Sippy, Tigris shadow buckets, and Alibaba Cloud OSS / Tencent COS mirror-back-to-origin.

The module is on by default (rustfs/backlog#2163); set `RUSTFS_ON_DEMAND_MIGRATION_ENABLED=false` on every node to turn it off (`rustfs/src/module_switches.rs`). With the switch off, the runtime never intervenes on a read and the admin `PUT` route refuses with `OnDemandMigrationDisabled`. Reads of the configuration and of the status endpoint keep working while the switch is off, so a disabled deployment can still be inspected. The switch only decides whether the module may act at all: a bucket with no `on-demand-migration.json` is never resolved by the runtime and makes no source call, so turning the module on changes nothing for buckets you have not configured.

## Positioning

| Capability | Direction | What it moves | Where the authoritative copy is | When to use it instead |
|---|---|---|---|---|
| **On-demand migration** (this page) | Inbound pull from a foreign S3 bucket | The objects clients actually read, plus an optional background backfill for the rest | The source until an object is pulled; local afterwards | You are migrating away from another provider and want zero-downtime cutover |
| Bucket replication (`docs/operations/replication-check.md`) | Outbound push to a target | Every new write, continuously | Local; the target is a copy | You want an ongoing copy of local writes elsewhere |
| Site replication | Bidirectional, cluster-wide | Buckets, IAM, policies, objects | Every peer is authoritative | You want two RustFS deployments to converge |
| Tiering / ILM (`docs/operations/tier-ilm-debugging.md`) | Outbound push to a warm backend, transparent read-back | Cold local objects, by lifecycle rule | Local metadata, remote data | You want to reduce local capacity for cold data you already own |
| Replication read-proxy | Inbound proxy to a replication target | Nothing — it only proxies the response | The target | An active-active pair has a replication lag window; this runs *before* ODM on a miss and never stores anything |

The order on a GET miss is fixed: local read → replication read-proxy → on-demand migration. ODM is consulted last because it is the expensive, authoritative-external answer.

## Enabling a source

Admin requests are SigV4-signed like any S3 request; the examples use [`awscurl`](https://github.com/okigan/awscurl) with the deployment's admin credentials. The routes and their authorization are pinned in `docs/architecture/admin-route-action-snapshot.md`.

| Route | Action | Notes |
|---|---|---|
| `PUT /rustfs/admin/v3/on-demand-migration/{bucket}` | `admin:SetBucketOnDemandMigration` | Requires the module switch and the server license; validates, probes the source, then saves |
| `PUT /rustfs/admin/v3/on-demand-migration/{bucket}?dry-run=true` | `admin:SetBucketOnDemandMigration` | Same validation and probe, saves nothing (`updated_at` is `null`) |
| `GET /rustfs/admin/v3/on-demand-migration/{bucket}` | `admin:GetBucketOnDemandMigration` | Returns the redacted config; `NoSuchConfiguration` (404) when unset |
| `GET /rustfs/admin/v3/on-demand-migration/{bucket}/status` | `admin:GetBucketOnDemandMigration` | Switch state plus this node's runtime snapshot |
| `DELETE /rustfs/admin/v3/on-demand-migration/{bucket}` | `admin:SetBucketOnDemandMigration` | Idempotent, answers 204; already-pulled objects stay in place |

Validate first, with `dry-run`, so a bad endpoint or a wrong key never reaches bucket metadata:

```bash
cat > /tmp/odm.json <<'JSON'
{
  "source": {
    "provider": "minio",
    "endpoint": "https://source.example.com:9000",
    "region": "us-east-1",
    "bucket": "legacy-photos",
    "credentials": { "access_key": "AKIA…", "secret_key": "REDACTED" }
  },
  "filter": { "source_prefix": "photos/" },
  "policy": { "inline_max_bytes": 16777216, "max_concurrent_pulls": 8 }
}
JSON

awscurl --service s3 --region us-east-1 \
  --access_key "$RUSTFS_ACCESS_KEY" --secret_key "$RUSTFS_SECRET_KEY" \
  --request PUT --header 'Content-Type: application/json' \
  --data "$(cat /tmp/odm.json)" \
  "http://<host>:9000/rustfs/admin/v3/on-demand-migration/photos?dry-run=true"
```

The response echoes the **redacted** config (`secret_key` and `session_token` become `REDACTED`) and a probe summary: `reachable` (the source answered `HeadBucket`), `listable` (a one-key `ListObjectsV2` succeeded) and `sample_key` (the first key that listing returned, absent for an empty source). Repeat the call without `?dry-run=true` to save it; the handler then fans a metadata reload out to the peers so the whole cluster picks the source up without waiting for the refresh loop.

```bash
# Read the saved configuration back
awscurl --service s3 --region us-east-1 \
  --access_key "$RUSTFS_ACCESS_KEY" --secret_key "$RUSTFS_SECRET_KEY" \
  "http://<host>:9000/rustfs/admin/v3/on-demand-migration/photos"

# Per-node runtime status (breaker, counters, queue, last source error)
awscurl --service s3 --region us-east-1 \
  --access_key "$RUSTFS_ACCESS_KEY" --secret_key "$RUSTFS_SECRET_KEY" \
  "http://<host>:9000/rustfs/admin/v3/on-demand-migration/photos/status"

# Stop consulting the source; pulled objects stay
awscurl --service s3 --region us-east-1 \
  --access_key "$RUSTFS_ACCESS_KEY" --secret_key "$RUSTFS_SECRET_KEY" \
  --request DELETE \
  "http://<host>:9000/rustfs/admin/v3/on-demand-migration/photos"
```

Setting `"enabled": false` in the config has the same read-path effect as deleting it (the runtime tears the bucket state down) while keeping the source, credentials and policy on record for a later re-enable.

The status endpoint reports **the node that answered the request**. Counters, queue depth and breaker state are per-node runtime state, so in a distributed deployment query every node; the saved configuration and `updated_at` are cluster-wide.

### Backfill (ships with ODM-12)

Read-through only migrates what clients touch. The background backfill job walks the source listing and pulls the remainder, with a persisted checkpoint (`.rustfs.sys/buckets/<bucket>/on-demand-migration-backfill.json`), a single-owner lease, resume after restart, and `POST .../{bucket}/backfill?op=start|cancel` plus `GET .../{bucket}/backfill` admin routes. That slice (rustfs/backlog#2159) is not part of the build this page was written against: the shape above is the agreed design, and the exact request/response bodies must be re-checked against `docs/architecture/admin-route-action-snapshot.md` once it lands.

## Configuration reference

The persisted blob is `on-demand-migration.json` in the bucket's metadata. Unknown fields are rejected rather than dropped, so a config written by a newer build fails loudly on an older one. Every default and bound below comes from `crates/ecstore/src/bucket/on_demand_migration/config.rs`.

| Field | Type | Default | Bounds / rules |
|---|---|---|---|
| `version` | integer | `1` | Must be `1` |
| `enabled` | bool | `true` | `false` keeps the config but stops all source traffic |
| `source.provider` | `s3` \| `aws` \| `minio` \| `rustfs` \| `r2` \| `gcs` | — (required) | Drives endpoint and addressing defaults |
| `source.endpoint` | string \| null | — | `http(s)://host[:port]`, no path, query, fragment or userinfo. Required for every provider except `aws`, where it is derived from `region` |
| `source.region` | string | — (required) | Non-empty. `auto` is accepted only for `r2`, `minio`, `rustfs` and is signed as `us-east-1` |
| `source.bucket` | string | — (required) | Non-empty, no `/` and no whitespace |
| `source.path_style` | `auto` \| `path` \| `virtual` | `auto` | `auto` resolves to path-style for IP-literal or `localhost` endpoints and for `s3`/`minio`/`rustfs`; virtual-host for `aws`/`gcs`/`r2` |
| `source.credentials` | object \| null | `null` | `null` means anonymous, which the client builder does not support yet: the admin `PUT` refuses it with `InvalidArgument`, and a config that reached the metadata another way resolves as unavailable. `access_key` and `secret_key` must be non-empty; `session_token` is optional but must be non-empty when present |
| `source.tls.skip_verify` | bool | `false` | Disables certificate verification for the source connection |
| `source.tls.ca_cert_pem` | string \| null | `null` | Must contain `-----BEGIN CERTIFICATE-----` |
| `filter.prefix` | string \| null | `null` | Null or non-empty. Only local keys with this prefix consult the source |
| `filter.source_prefix` | string \| null | `null` | Null or non-empty. Prepended to the local key to form the source key |
| `policy.head` | `proxy` \| `local_only` | `proxy` | `local_only` answers a HEAD miss with 404 and no source traffic |
| `policy.range_get` | `serve_and_backfill` \| `serve_only` | `serve_and_backfill` | Whether a Range GET also queues a background pull of the whole object |
| `policy.source_error` | `propagate` \| `not_found` | `propagate` | `propagate` answers 424 `SourceUnavailable`; `not_found` degrades to 404 |
| `policy.respect_local_delete_marker` | bool | `true` | A local delete marker is the final answer; only a versioned bucket can produce one |
| `policy.preserve_etag` | bool | `true` | Keeps the source ETag on the stored object unless the bucket encrypts by default |
| `policy.copy_tags` | bool | `false` | Copies source object tags; needs `s3:GetObjectTagging` and costs one extra source call per inline pull |
| `policy.emit_events` | bool | `true` | Whether a write-back emits `ObjectCreated` notifications |
| `policy.negative_cache_ttl_secs` | integer | `30` | `0..=3600`; `0` disables the negative cache |
| `policy.inline_max_bytes` | integer | `16777216` (16 MiB) | `0..=268435456` (256 MiB). At or below this size a GET miss is teed inline; above it the response streams through and a background pull stores the object |
| `policy.multipart_part_size_bytes` | integer | `67108864` (64 MiB) | `5242880..=5368709120` (5 MiB…5 GiB). Objects needing more than 10 000 parts require a larger part size |
| `policy.max_concurrent_pulls` | integer | `8` | `1..=256`; shared by the inline and background paths |
| `policy.pull_queue_capacity` | integer | `1024` | `1..=65536`; a full queue drops the *background* job, never the client response |
| `policy.source_timeout.connect_ms` | integer | `5000` | `100..=600000` |
| `policy.source_timeout.first_byte_ms` | integer | `15000` | `100..=600000`; also the window a follower waits for the singleflight leader before streaming through |
| `policy.source_timeout.idle_ms` | integer | `30000` | `100..=600000`; enforced per body chunk on both the background pump and the inline tee |
| `policy.bandwidth_limit_bytes_per_sec` | integer \| null | `null` | When set, at least `65536` |

Values that are **not** configurable: the breaker opens after 5 consecutive counted failures inside a 30 s window, stays open for 30 s and then admits one probe (`breaker.rs`); the negative cache holds at most 100 000 keys per bucket with LRU eviction (`negative_cache.rs`); a background pull retries a retryable source failure at most 3 times with 1 s / 4 s / 16 s base delays plus up to 25 % jitter (`pull.rs`). The SDK's own retry policy is disabled on the source client, so one logical source call is exactly one wire request and the retry budget above is the only one.

Validation also rejects two shapes outright: a source whose endpoint and bucket name **this** bucket on this deployment (`SelfReference`), and a source that matches one of the bucket's own replication targets (`ReplicationLoop`) — that pairing would amplify a write-back into a loop.

## Provider presets and source permissions

| Provider | Endpoint | Addressing | `region` | Notes |
|---|---|---|---|---|
| `aws` | Optional; derived as `https://s3.<region>.amazonaws.com` | Virtual-host | Real region required (`auto` rejected) | The derived form only accepts `[A-Za-z0-9-]` in `region` |
| `s3` | Required | Path-style | Real region | Generic S3-compatible endpoint (Wasabi, Backblaze B2 S3 API, Ceph RGW, …) |
| `minio` | Required | Path-style | `auto` allowed | |
| `rustfs` | Required | Path-style | `auto` allowed | A RustFS source answers the migration request locally thanks to the anti-loop marker |
| `r2` | `https://<account-id>.r2.cloudflarestorage.com` | Virtual-host | `auto` allowed (signed as `us-east-1`) | |
| `gcs` | `https://storage.googleapis.com` | Virtual-host | Real region required | Uses the GCS XML interoperability API with an HMAC key pair, not a service-account JSON key |

Azure Blob has no preset; a native provider is deferred (rustfs/backlog#2166).

The credentials only ever need read access to the source bucket:

- `s3:ListBucket` on the bucket — used by the admin probe and by the backfill listing.
- `s3:GetObject` on `<bucket>/*` — used by every HEAD and GET against the source.
- `s3:GetObjectTagging` on `<bucket>/*` — only when `policy.copy_tags` is `true`.

No write, delete, ACL or versioning permission is required or used. Scope the policy to the prefix in `filter.source_prefix` when the source bucket holds unrelated data.

## Request semantics

Behaviour a client can observe. The "Test" column names the case that pins it: `*_test.rs` files live under `crates/e2e_test/src/on_demand_migration/`, and the unit tests live next to the code in `rustfs/src/app/object/get.rs`, `head.rs` and `shared.rs`.

| Situation | Behaviour | Test |
|---|---|---|
| GET miss, object at or below `inline_max_bytes` | One source GET, teed: the client streams while the same bytes are written locally. Later reads are local and carry no source marker | `get_basic_test.rs::get_miss_pulls_inline_and_serves_locally_afterwards`, `get.rs::odm_get_inline_streams_to_client_and_commits_the_same_bytes` |
| GET miss, object above `inline_max_bytes` | Streamed straight through (nothing stored inline) and a background pull of the whole object is queued | `get_basic_test.rs::get_large_object_streams_through_and_backfills_in_background`, `get.rs::odm_get_large_object_streams_through_and_queues_a_background_pull` |
| Range GET miss | The requested range is passed through as 206; with `range_get = serve_and_backfill` a whole-object background pull is queued, with `serve_only` nothing is queued | `get_basic_test.rs::get_range_streams_206_and_backfills_the_whole_object`, `get.rs::odm_get_range_streams_206_and_queues_per_policy` |
| Client disconnects mid-stream on an inline pull | The write-back keeps draining the source and still stores the whole object | `get.rs::odm_get_inline_client_disconnect_still_stores_the_whole_object` |
| Concurrent GET misses of one key | Singleflight: one leader tees, followers wait up to `first_byte_ms` and then re-read locally; on leader failure or timeout they stream through without queueing | `concurrency_test.rs::test_odm_concurrent_misses_on_one_key_coalesce`, `get.rs::odm_get_follower_rereads_local_after_the_leader_commits` |
| HEAD miss | Proxied to the source, nothing is written locally; `local_only` answers 404 without source traffic | `head.rs::odm_head_source_hit_returns_output_and_writes_nothing_back`, `head.rs::odm_head_local_only_policy_is_404_without_source_traffic`, `real_source_test.rs::test_odm_rustfs_source_serves_pull_head_range_and_prefixes_real_single_node` |
| LIST | Local only. A key that exists on the source but was never pulled is not listed, even while a GET of it succeeds | `interaction_test.rs::test_odm_write_back_respects_the_bucket_quota` |
| PUT / DELETE | Never touch the source. A local PUT shadows the source key for good | `interaction_test.rs::test_odm_delete_marker_shadows_the_source_but_a_plain_delete_does_not` |
| Versioned bucket, local delete marker | With `respect_local_delete_marker` (default) the marker is the final answer and the source is not consulted | `interaction_test.rs::test_odm_delete_marker_shadows_the_source_but_a_plain_delete_does_not`, `head.rs::odm_head_verdict_respects_local_delete_marker_by_policy` |
| Unversioned bucket, object deleted locally | The key becomes an ordinary miss and is pulled from the source again | `interaction_test.rs::test_odm_delete_marker_shadows_the_source_but_a_plain_delete_does_not` |
| GET with `versionId`, or a `partNumber` read | Never consults the source; the local 404 stands | `get_basic_test.rs::get_with_version_id_does_not_consult_the_source`, `get.rs::odm_get_gate_rejects_part_reads_and_version_reads` |
| Conditional headers (`If-Match`, `If-None-Match`, `If-Modified-Since`, `If-Unmodified-Since`) | Evaluated locally against the source HEAD with the usual S3 semantics; never forwarded to the source | `get.rs::odm_get_conditional_headers_are_answered_from_the_source_head`, `shared.rs::odm_source_preconditions_follow_local_semantics` |
| Source object is SSE-C encrypted | 424 `SourceUnavailable` with class `unsupported`, whatever `source_error` says | `fault_test.rs::test_odm_ssec_source_object_is_unsupported`, `head.rs::odm_head_unsupported_source_object_is_424_regardless_of_policy` |
| Source answers 404 | 404 to the client and the key is negative-cached for `negative_cache_ttl_secs` | `get_basic_test.rs::get_source_not_found_is_404_and_negative_cached`, `fault_test.rs::test_odm_source_not_found_is_negative_cached_for_the_ttl` |
| Source answers 403 | 424 (or 404 under `not_found`); the breaker is **not** opened — a credential problem is not a transient one | `fault_test.rs::test_odm_source_access_denied_propagates_without_opening_the_breaker` |
| Repeated source 5xx / timeouts | The breaker opens, GETs answer per `source_error` and HEADs answer 404; a probe closes it again after the open window | `fault_test.rs::test_odm_repeated_source_errors_open_the_breaker_and_recover`, `head.rs::odm_head_source_errors_follow_policy_and_open_the_breaker` |
| Bucket default SSE | The pulled object is stored encrypted and reads back in plaintext; the ETag override is dropped, and the source ETag is kept in metadata | `interaction_test.rs::test_odm_pulled_object_uses_bucket_default_encryption`, `on_demand_migration_put.rs::write_back_under_bucket_default_sse_stores_ciphertext_and_records_source_etag` |
| Object Lock bucket | The pulled object inherits the bucket's default retention | `interaction_test.rs::test_odm_pulled_object_inherits_object_lock_retention` |
| Bucket quota exceeded | The client is still served from the source; the write-back is rejected, nothing is stored, and the failure counts under `quota` | `interaction_test.rs::test_odm_write_back_respects_the_bucket_quota`, `on_demand_migration_put.rs::write_back_reports_a_full_bucket_quota` |
| Notifications | A write-back emits `ObjectCreated` with principal `rustfs-on-demand-migration`, unless `emit_events` is `false` | `interaction_test.rs::test_odm_pull_emits_object_created_events_unless_disabled` |
| Replication configured on the local bucket | A pulled object replicates like any other write; configuring one of the bucket's replication targets as the source is rejected | `interaction_test.rs::test_odm_pulled_object_replicates_and_target_as_source_is_rejected`, `on_demand_migration_put.rs::write_back_schedules_replication_and_names_the_migration_principal` |
| Source is itself a RustFS/MinIO deployment with its own source | The anti-loop marker stops the chain at the first hop; mutual configurations still terminate | `real_source_test.rs::test_odm_chained_sources_stop_at_the_loop_guard_real_single_node` |
| Config disabled or deleted | Source traffic stops immediately; already-pulled objects stay readable | `get_basic_test.rs::get_after_disable_does_not_consult_the_source`, `interaction_test.rs::test_odm_disable_keeps_pulled_objects_and_stops_source_traffic` |

Every response that was answered from the source — GET or HEAD — carries `x-rustfs-on-demand-migration: source`. A local hit carries no such header, which makes it the cheapest way to tell a migrated read from a served one.

Source-backed responses report only what the source can vouch for: its ETag, its `Last-Modified`, `Content-*`, `Cache-Control`, `Expires`, `Accept-Ranges` and `x-amz-meta-*`. There is no version id, no SSE header, no storage class and no checksum on a source-backed response, because the object is not local yet.

## Integrity and ETag

- The write-back is given the size the source advertised, so a truncated or over-long source body fails the local write instead of committing a partial object; the background path additionally enforces the advertised length in its pump, ahead of the write.
- When the source ETag is a bare 32-hex-digit value (a single-part, unencrypted object), it is used as the expected plaintext MD5 of the single-part write-back. A mismatch is a `BadDigest`, classified as `etag_mismatch`, and nothing is stored.
- A multipart source ETag (`<md5>-<n>`) cannot be verified this way and is not used as a digest; the object is still stored, and the multipart write-back re-chunks it by `multipart_part_size_bytes`, so the *local* ETag will differ from the source's unless `preserve_etag` keeps it for display.
- `preserve_etag` (default on) stores the source ETag as the object's ETag. It is dropped when the bucket encrypts by default — the SSE write path owns the ETag there, exactly like replication receive.
- The source ETag is always recorded in internal metadata regardless of the policy, so an audit or a later comparison can still see it.
- `Last-Modified` of a pulled object is the local write time, not the source's. The source timestamp is preserved in metadata.

## Metadata mapping

Copied to the local object:

| Source | Stored as |
|---|---|
| `Content-Type`, `Content-Encoding`, `Content-Disposition`, `Content-Language`, `Cache-Control`, `Expires` | The same standard headers (empty values are skipped) |
| `x-amz-meta-*` | The same user metadata |
| Object tags | Local tags, only when `policy.copy_tags` is set |

Deliberately **not** copied: the storage class (the local bucket's class applies), the source `Last-Modified` (the local write time applies), ACLs, and any source SSE header (the local bucket's encryption applies).

Five provenance keys are written on every pulled object under both internal prefixes (`x-rustfs-internal-` and `x-minio-internal-`), so they never appear in client-visible metadata: `odm-source` (`<provider>:<source bucket>`), `odm-source-etag`, `odm-source-last-modified`, `odm-source-version-id` (empty for an unversioned source) and `odm-pulled-at`. They are the audit trail that tells a migrated object from a client write.

## Protection mechanisms

| Mechanism | What it protects | Behaviour |
|---|---|---|
| Circuit breaker | A struggling source | 5 consecutive transport failures (throttle, timeout, connect, 5xx) inside 30 s open it for 30 s, then one probe decides. A source 404 is a healthy answer and resets the streak; 403 and unsupported-object errors are neutral |
| Negative cache | Repeated misses for keys the source does not have | A source 404 is remembered per key for `negative_cache_ttl_secs` (100 000 keys per bucket, LRU) |
| Singleflight | A thundering herd on one cold key | One leader pull per key; followers wait `first_byte_ms` for it, then re-read locally or stream through |
| Concurrency limit | Local write amplification | `max_concurrent_pulls` permits shared by inline and background pulls |
| Bounded queue | Unbounded memory on a burst | `pull_queue_capacity` waiting jobs; overflow is counted as `queue_full` and never fails a client response |
| Bandwidth limit | Source and network saturation | `bandwidth_limit_bytes_per_sec` (minimum 64 KiB/s) on the source client |
| Retry budget | Transient source blips | Background pulls retry a retryable failure up to 3 times (1 s / 4 s / 16 s plus jitter). Inline pulls never retry: the bytes are already on their way to the client |
| Anti-loop marker | Migration chains between RustFS/MinIO deployments | Every source request carries `x-rustfs-source-proxy-request` and `x-minio-source-proxy-request`; a request carrying it is always answered locally |
| Outbound endpoint policy | SSRF | See [outbound-connection-policy.md](outbound-connection-policy.md) |

## Background services

The background workers that belong to this feature are inventoried, with their desired sources, status inputs and declared side effects, in `docs/architecture/background-services-inventory.md`.

- **Write-back commit task** — spawned per inline pull, outlives the request so a client disconnect cannot truncate the stored object.
- **Pull queue dispatcher** — one per configured bucket, started lazily on the first background pull; drains the bounded queue under the concurrency limit and is cancelled when the bucket's config changes or disappears.
- **Backfill job and its recovery loop** — ships with rustfs/backlog#2159 (see [Backfill](#backfill-ships-with-odm-12)).

## Error codes

| Code | HTTP | When |
|---|---|---|
| `SourceUnavailable` | 424 | A source failure under `source_error = propagate`. The message carries only the failure class: `timeout`, `connect`, `server_error`, `throttled`, `access_denied`, `other`, `breaker_open` (an open breaker on a GET), `client_build` or `unsupported` (a bucket whose source client could not be built, including a credential-less source) |
| `SourceUnavailable` | 424 | An SSE-C source object, always — this one does not follow `source_error` |
| `NoSuchKey` | 404 | The source answered 404, the key is negative-cached, a HEAD hit an open breaker, or `source_error = not_found` hides a source failure |
| `OnDemandMigrationDisabled` | 400 | Admin `PUT` while `RUSTFS_ON_DEMAND_MIGRATION_ENABLED` is off |
| `OnDemandMigrationSourceUnreachable` | 400 | The admin probe's `HeadBucket` or one-key listing failed; the message names the class only |
| `NoSuchConfiguration` | 404 | Admin `GET` on a bucket with no source configured |
| `InvalidArgument` | 400 | Config validation failed (bad endpoint, empty region, out-of-range policy value, self-reference, replication loop, unknown JSON field) |
| `AccessDenied` | 403 | The admin action is not authorized, or the license check denies the entitlement |
| `NoSuchBucket` | 404 | The local bucket does not exist |

Source error messages never cross the boundary verbatim: an SDK message can embed the signed request, the endpoint host and the key, so only the stable class label is returned and logged.

## Observability

All series are bucket-scoped under `rustfs_on_demand_migration_*` and appear only for buckets with a configured source; when a bucket's config disappears its series are retired on the next collection cycle.

| Metric | Type | Labels |
|---|---|---|
| `requests_total` | counter | `bucket`, `op` (`get`, `head`), `outcome` (`source_hit`, `source_miss`, `source_error`, `breaker_open`, `negative_cached`, `filtered`, `unsupported`) |
| `pulled_bytes_total` | counter | `bucket` |
| `pulled_objects_total` | counter | `bucket`, `path` (`inline`, `background`, `backfill`) |
| `pull_failures_total` | counter | `bucket`, `reason` (`source_not_found`, `source_access_denied`, `source_throttled`, `source_timeout`, `source_connect`, `source_server_error`, `source_unsupported`, `source_other`, `etag_mismatch`, `local_write`, `quota`, `canceled`, `queue_full`) |
| `inflight_pulls` | gauge | `bucket` |
| `queue_depth` | gauge | `bucket` |
| `source_latency_seconds_distribution` / `_sum` / `_count` | counter | `bucket`, plus `le` on the distribution |
| `breaker_state` | gauge | `bucket` — `0` closed, `1` half-open, `2` open |

```promql
# Share of GETs that entered ODM and were answered by the source
sum by (bucket) (rate(rustfs_on_demand_migration_requests_total{op="get",outcome="source_hit"}[5m]))
  / sum by (bucket) (rate(rustfs_on_demand_migration_requests_total{op="get"}[5m]))

# Migration throughput
sum by (bucket) (rate(rustfs_on_demand_migration_pulled_bytes_total[5m]))

# Alert: a source breaker is open
max by (bucket) (rustfs_on_demand_migration_breaker_state) >= 2

# Source latency p99
histogram_quantile(0.99, sum by (bucket, le) (rate(rustfs_on_demand_migration_source_latency_seconds_distribution[5m])))

# Pull failures by reason
sum by (bucket, reason) (rate(rustfs_on_demand_migration_pull_failures_total[5m]))
```

`GET .../{bucket}/status` returns the same counters plus the configuration view: `configured`, `enabled` (the config's own switch), `module_enabled` (the process switch), `provider`, `endpoint_host`, `breaker.state`, `counters` (`requests_total`, `pulled_bytes_total`, `pulled_objects_total`, `pull_failures_total`, `source_latency`), `last_source_error` (`class` and `at`), `inflight_pulls`, `queue_depth`, `served_by_source_ratio` and `updated_at`. Two fields are always `null` in this build and are not a bug: `breaker.opened_at` (the runtime holds a monotonic instant, not a wall clock) and `served_by_source_ratio` (there is no per-bucket GET total to divide by, and a fabricated `0` would be worse than an honest `null`). When a bucket has no live state on the node — the module is off, or nothing has been read yet — the runtime fields are `null` while `provider` and `endpoint_host` still reflect the saved config.

## Troubleshooting

**Admin `PUT` returns `OnDemandMigrationDisabled`.** `RUSTFS_ON_DEMAND_MIGRATION_ENABLED` is not `true` on the node that handled the request. Set it on every node and restart; the switch is read at startup.

**Admin `PUT` returns `OnDemandMigrationSourceUnreachable`.** The probe (`HeadBucket` plus a one-key list) failed. The message names the class only, so match it against the source's own access log: `access_denied` is credentials or a missing `s3:ListBucket`; `not_found` is a wrong bucket name or the wrong addressing style; `connect` is DNS, TLS or routing; `timeout` means the source did not answer inside `connect_ms`/`first_byte_ms`.

**Reads return 424 `SourceUnavailable`.** Read the class in the message and `last_source_error` in the status. `access_denied` means the key exists but `s3:GetObject` is missing (or the credentials were rotated). `server_error`/`timeout`/`connect` are transient and will open the breaker after five in a row. `unsupported` means the source object is SSE-C. If you would rather have clients see a plain 404 while you fix the source, set `policy.source_error = not_found` — but note that a sync client will then read it as "deleted".

**Everything 404s and `breaker_state` is `2`.** The breaker is open; it re-probes after 30 s. Fix the source first — while it is open, no source traffic leaves the node, which is the point.

**A key that exists on the source 404s immediately, without source traffic.** Either the negative cache still holds a previous 404 (wait out `negative_cache_ttl_secs`, default 30 s), or `filter.prefix` does not admit the key, or a local delete marker shadows it on a versioned bucket, or the request carried a `versionId`.

**The endpoint is rejected as an SSRF risk.** Loopback source endpoints are refused unless `RUSTFS_REPLICATION_ALLOW_LOOPBACK_TARGET=true`; private addresses are always allowed. See [outbound-connection-policy.md](outbound-connection-policy.md).

**Self-signed TLS on the source.** Put the issuing certificate in `source.tls.ca_cert_pem` (PEM text, in the JSON body). `source.tls.skip_verify` exists for a lab and disables verification entirely — do not use it against a source reached over an untrusted network.

**`NoSuchBucket` from the source, or a 301/307 redirect.** The addressing style is wrong: a virtual-host request against a path-style-only server looks like a missing bucket, and a path-style request against AWS gets redirected. Set `source.path_style` explicitly instead of relying on `auto`.

**Large objects are served but never stored.** Check `pull_failures_total`: `queue_full` means bursts exceed `pull_queue_capacity` (raise it, or raise `max_concurrent_pulls`); `quota` means the bucket quota rejected the write-back and `local_write` a genuine local write failure; `etag_mismatch` means the source body did not match the ETag the source advertised. The client response is served in all three cases — only the local copy is missing.

**The first read of a key is slow, later ones are fast.** Expected: the first read pays a source HEAD plus a source GET. Watch `source_latency_seconds_*` for the source's contribution and lower `inline_max_bytes` if teeing large objects is hurting first-byte latency.

**Nothing at all happens on a miss.** Confirm in order: the module switch (`module_enabled` in the status), `enabled` in the config, `filter.prefix`, and that the request is a plain GET/HEAD (no `versionId`, no `partNumber`, no `x-rustfs-source-proxy-request` marker from a peer).

## Known limitations

- **Source updates do not propagate.** Once an object is pulled, the local copy is authoritative; a later change on the source is never noticed. Plan the cutover so the source stops taking writes.
- **Unversioned buckets re-pull deleted keys.** An unversioned bucket keeps nothing after a delete, so the key looks like an ordinary miss and is migrated again. Only a versioned bucket can shadow the source with a delete marker (`respect_local_delete_marker`).
- **SSE-C source objects are not supported.** They are rejected with 424 `unsupported`; migrate them by another route.
- **Anonymous (credential-less) sources are not supported yet.** `source.credentials: null` parses and passes structural validation, but the client builder has no anonymous mode, so the admin `PUT` refuses it and the runtime would treat such a bucket as unavailable. A public source still needs a key pair.
- **Azure Blob is not a supported source** (rustfs/backlog#2166). GCS is supported only through its XML interoperability API with HMAC keys.
- **LIST does not merge the source** (rustfs/backlog#2164). Only local objects are listed, so a client that lists before reading will not see un-migrated keys.
- **Write-through is undecided** (rustfs/backlog#2165). PUT and DELETE never reach the source in this version.
- **`pull_failures_total` counts abandoned pulls, not attempts.** A pull that failed twice and then succeeded contributes nothing; attempt-level failure needs a new counter.
- **The breaker's 30 s open window is a compile-time constant** with no environment override, which is why breaker-related tests have to wait it out.
- **`breaker.opened_at` and `served_by_source_ratio` are always `null`** in the status response (see Observability).

## Security notes

Source credentials are stored **unencrypted** in the bucket's metadata, in the same trust boundary as `bucket-targets.json` and the tier configuration; anyone who can read the drives can read them. Encryption at rest for bucket-level remote credentials is tracked in rustfs/backlog#2168. Until then, treat the source key as a long-lived secret: scope it to read-only on one bucket (or one prefix), and rotate it on the source after the migration completes, then clear the config.

Credentials never leave the server in readable form: every admin response returns the redacted config (`secret_key` and `session_token` become `REDACTED`), no log line carries the configuration, and probe or source failures report only a stable class label rather than the SDK message, which can embed the signed request. Configuration endpoints are also refused entirely when the endpoint string tries to embed userinfo.

Outbound safety rests on three guards: the shared endpoint policy that refuses loopback and metadata-service addresses, the validator that refuses a source naming this bucket on this deployment or one of its own replication targets, and the anti-loop request markers that stop a migration chain between RustFS or MinIO deployments at the first hop. Every source request additionally carries a `RustFS-OnDemandMigration/<version>` `User-Agent` suffix so the source operator can attribute the traffic.
