# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Replication

- Object Lock replication PUTs now carry a required integrity header, fixing target rejection introduced by the plain-payload default ([#7097](https://github.com/rustfs/rustfs/pull/7097)). This changes the default outbound request for locked objects but adds no persisted format.
- Multipart source objects stay on the multipart transport even when their checksum record is a whole-object checksum, so objects above the single-PUT limit remain replicable ([#7047](https://github.com/rustfs/rustfs/pull/7047)).
- Targets that mint their own version IDs now use a per-target version ledger for tag, retention, legal-hold, and permanent-delete mutations; ambiguous pre-ledger matches fail with backoff instead of guessing ([#7368](https://github.com/rustfs/rustfs/pull/7368)). This adds dual-prefixed internal metadata keys that older readers ignore.
- Single-part source checksums are forwarded as `x-amz-checksum-*` headers instead of user metadata, so the replica preserves checksum responses ([#7313](https://github.com/rustfs/rustfs/pull/7313)). This changes the default outbound headers for checksummed objects.
- Site-replication outage recovery now uses a bounded 30-second retry drain plus the 600-second full reconciliation pass, persists destructive liabilities before local deletion, and fences replay settlement and peer edits ([#7148](https://github.com/rustfs/rustfs/pull/7148)). Persisted additions are optional and ignored by older readers.
- IAM snapshot/deletion replay, target-assigned delete-marker purges, timestamp ordering, and best-effort peer broadcast now close the control-plane gaps found by the R6 review ([#7195](https://github.com/rustfs/rustfs/pull/7195)).
- Upgrade and rollback: upgrade every node in one site consecutively and verify reconciliation before moving to the next site; do not intentionally run a site mixed-version. Target-version ledger keys are harmless on rollback, although old code cannot use their routing. Before rolling back past [#7307](https://github.com/rustfs/rustfs/pull/7307), drain or repair every pending version purge: older code can free a retained version's data directory before its remote purge is acknowledged. See `docs/operations/site-replication-operations.md`.

### Security
- **Presigned URLs honour only signed headers** (GHSA-g8w9-qw9q-fghr): a SigV4 presigned request that carries an `x-amz-*` request header not listed in `X-Amz-SignedHeaders` is now rejected with `403 AccessDenied` ("There were headers present in the request which were not signed"), matching AWS S3. Previously the holder of a presigned `PutObject` URL could add unsigned `x-amz-tagging`, `x-amz-storage-class`, `x-amz-website-redirect-location`, ACL, metadata, Object Lock or SSE headers and have them applied. Presigners that intend a property must set it before signing so the SDK lists the header in `SignedHeaders`; `x-amz-cf-id` (CloudFront) remains tolerated unsigned. Header-signed SigV4 and SigV2 requests are unchanged.

### Fixed
- **Fresh multi-pool bootstrap with distinct format creators**: a new deployment whose pools have their first endpoint on different nodes (for example two single-node pools) could never publish its initial `pool.bin`: each node held fresh-bootstrap proof only for the pool it formatted, the deployment-wide proof collapsed to none, and every node died with `pool metadata recovery required: no durable bootstrap identity or pool.bin replica is available` after the startup retry budget. The first pool's creator now mints the pending cluster identity on its own pool, every other creator copies that nonce-bound identity onto the pool it formatted first-hand, and the elected writer publishes `pool.bin` once every pool replica carries the same pending identity. Corrupt or disagreeing replicas, pools that merely have a format, expansion pools joining an initialized deployment, and restarts without first-hand proof still fail closed. Non-elected nodes that start before `pool.bin` exists, and the elected writer while it waits for the other creators, no longer latch their pool-metadata write gate for the life of the process. Refs rustfs/backlog#2338, rustfs/backlog#2375.
- **Lock RPC timeout storms** (#7363): the remote lock client no longer evicts and re-dials the shared internode HTTP/2 channel on every request deadline. A timeout evicts only when the peer has not completed any lock RPC for two deadlines, evictions and transport-failure re-dials are rate limited per peer (`RUSTFS_OBJECT_LOCK_RPC_EVICTION_COOLDOWN_MS`, default 5 s), and a timed-out request is left running instead of being reset (bounded per peer by `RUSTFS_OBJECT_LOCK_RPC_DETACHED_LIMIT`, default 256), so a slow lock endpoint can no longer drive the `RST_STREAM`/`GOAWAY too_many_resets`/reconnect loop. A lock granted after its caller timed out is released immediately, and unlocks that fail the quick retries continue on a deferred 1/2/4/8/16 s schedule before the server lease reclaims them. New `rustfs_remote_lock_*` metrics cover timeouts, evictions, suppressed evictions, detached streams, late completions and late releases per peer. Operator guide at `docs/operations/lock-rpc-storm-protection.md`.
- **KMS failures on the S3 data path carry an actionable status**: only "key not found" and a backend outage were classified; every other KMS failure — a disabled or pending-deletion key, a denied KMS grant, an encryption-context mismatch, an unsupported algorithm, a credential or timeout failure, a capability the backend does not have — collapsed onto `500 InternalError`. SDKs therefore applied exponential backoff to configuration errors that no retry can fix, and monitoring filed every one of them as a server fault. Unusable-key and request-side failures now return `400`, a denied grant `403`, transient backend failures `503` — including a key store the backend could not read, so an outage stays distinguishable from a missing key all the way to the client — and a missing backend capability `501`. Damaged or unreadable key material still returns `500`, which is what it is.
- **Bare SSE-KMS writes on a node without a running KMS**: `x-amz-server-side-encryption: aws:kms` without a key id (and no bucket default key) returned `500 InternalError` while KMS was stopped or never configured, because the "no key available" branch exited before the availability classification that the keyed form already received. `PutObject` and `CreateMultipartUpload` now return `503 ServiceUnavailable` while a configured KMS is stopped, `400 InvalidRequest` when KMS was never configured, and `400 InvalidRequest` naming the missing key id when a running KMS has no default key.
- **Reading an object whose KMS key is gone returned `500`**: `GetObject`, `CopyObject` and `UploadPartCopy` on an SSE-KMS object whose key had been deleted reported `500 InternalError` ("KMS key not found"), while the same condition on `PutObject` already returned `400 KMS.NotFoundException`. The read path carried only four classifications across the storage boundary and folded a missing key, a denied KMS grant and a missing backend capability onto "decryption failed". Those reads now return `400 KMS.NotFoundException`, `403 AccessDenied` and `501 NotImplemented` respectively; `HeadObject` is unaffected because it never unwraps the data key. An envelope the configured backend cannot unwrap (the key was re-created under the same name, or the backend was switched) stays `500` but now says so instead of the generic internal-error text.
- **KMS key-management routes answered `500` for client-side failures**: `POST /rustfs/admin/v3/kms/keys` and the legacy `create-key` alias reported every backend refusal as `500`, including a blank key name (each backend failed differently, the Local backend by writing a key file with an empty stem) and a name that already exists; `POST /rustfs/admin/v3/kms/generate-data-key` did the same for an unknown or disabled key, although `describe` and `delete` already classified those. A blank or whitespace key name is now refused before it reaches any backend (`400`), a taken name is `409`, an unknown key is `404` (`KMS.NotFoundException`), a disabled key `400`, and a capability the backend lacks `501`; damaged key material stays `500`. The read-only Static backend now reports create, delete and cancel-deletion as missing capabilities (`501`), matching its rotate and enable/disable answers, instead of `400`/`500`.
- **SSE-S3 responses named the internal wrapping key**: `PutObject`, `CopyObject`, `CreateMultipartUpload` and `GetObject` for an `AES256` object returned `x-amz-server-side-encryption-aws-kms-key-id` carrying the KMS key that wraps the SSE-S3 data key (the service default key, or the literal `default` on a node without KMS), although the header is defined for `aws:kms` objects only and `CompleteMultipartUpload` and `HeadObject` already omitted it. Those responses now advertise a key id only for `aws:kms` objects.
- **PutBucketEncryption accepted algorithms the server cannot honour**: a default-encryption rule naming an unknown `SSEAlgorithm` (for example `AES128`), a rule without `ApplyServerSideEncryptionByDefault`, an empty rule list, or a `KMSMasterKeyID` on an `AES256` rule was stored as written. `GetBucketEncryption` then reported that configuration while every header-less write was encrypted under the `AES256` fallback, so the bucket's advertised and actual schemes disagreed. Those configurations are now refused with `400` (`MalformedXML` for a malformed rule, `InvalidArgument` for a key id on a non-KMS rule) and nothing is stored.
- **SSE-C on buckets with default encryption**: a `PutObject` carrying a valid SSE-C header triple on a bucket that has default encryption configured no longer fails with `400 InvalidArgument` ("The SSE-C and managed server-side encryption headers cannot be used together"). PUT and the POST-object/extract path resolved the bucket default with a hard-coded "no explicit SSE-C" flag, so the default was layered onto the request and then tripped the request's own mutual-exclusion check; an SSE-C request now suppresses the bucket default on all three write paths, matching COPY and AWS S3. Every bucket with default encryption previously refused SSE-C single PUTs outright, while `CreateMultipartUpload` on the same bucket succeeded.
- **Explicit SSE-S3 on SSE-KMS-default buckets**: `x-amz-server-side-encryption: AES256` against a bucket whose default is `aws:kms` no longer fails with `400 InvalidArgument`. The bucket default's KMS key id was inherited independently of the effective algorithm, producing a self-contradictory `AES256` + key-id pair; the key id is now inherited only when the effective algorithm is `aws:kms`. `PutBucketEncryption` fills in a default key id automatically, so this affected nearly every SSE-KMS-default bucket.
- **Restore of encrypted or compressed multipart objects (silent data corruption)**: restoring a multipart object from a remote tier addressed the tier in *plaintext* coordinates while the copy-back reads the *stored* representation. Every part received a misaligned slice of the remote object whose length still satisfied the range, the hash reader and the completion size check, so the restore reported success and replaced the object's bytes. Restore now accumulates stored part sizes, passes the stored length to the hash reader alongside the plaintext length, and validates against the stored size. Objects restored by an affected release must be re-restored from the tier or recovered from a backup — this release does not detect or repair them retroactively.
- **Restore no longer drifts the object ETag**: the copy-back digests stored (encrypted or compressed) bytes, so the recomputed MD5 is not the object's public ETag. Single-part and multipart restores now preserve the original object ETag, and each restored part keeps its own recorded part ETag.
- **ILM archive no longer forwards encryption metadata to the tier**: transition requests carried the object's SSE headers and the RustFS-wrapped data key as request headers. Any S3 target rejected an SSE-C archive outright (`400`, no key supplied), an SSE-KMS archive asked the target to encrypt a second time under a key id it does not own, and the wrapped DEK left the cluster. The archive request now strips every SSE header and encryption marker using the same predicate the replication path uses; the local `xl.meta` keeps all of it, so read-through and restore are unaffected.
- **KMS reload is no longer a no-op on a node whose KMS failed to start**: `POST /rustfs/admin/v3/kms/reload` short-circuited whenever the persisted configuration matched the in-memory one byte for byte. A node whose KMS failed to start (for example Vault briefly unreachable during a rolling restart) keeps that configuration and sits in `Error`, so the documented recovery call returned "reloaded successfully" while leaving the node down — and did the same on every peer through the reload broadcast. Reload now short-circuits only for a service that is actually running, and otherwise reconfigures, which starts the service.
- **AWS KMS capability reporting**: the AWS backend no longer advertises `versioning` support through `GET /rustfs/admin/v3/kms/status`. AWS KMS key versions are not enumerable through this backend, as the backend documentation already stated.
- **Multipart admission queue**: an `UploadPart` waiting for a foreground write permit now waits at most 10 s by default (`RUSTFS_PUT_MULTIPART_FOREGROUND_ADMISSION_WAIT_TIMEOUT_MS`, previously 30 s), so a queued part returns S3 `SlowDown` before the client's socket write timeout drops the connection. Separately, the API listener no longer forces a 4 MiB `SO_RCVBUF` on every accepted socket (kernel autotuning applies; `RUSTFS_HTTP_SOCKET_RECV_BUFFER_BYTES` restores a fixed size), so a queued part no longer lets up to 8 MiB of unread body accumulate in kernel memory per connection, which is what throttled whole nodes under SDK-default multipart concurrency. Fixes #7385.
- **Helm Ingress**: `customAnnotations` are now merged with class-specific annotations (nginx/traefik) instead of being ignored when `ingress.className` is set.
- **Per-pool erasure parity**: Erasure parity (STANDARD and reduced-redundancy) is now resolved independently for every pool instead of reusing the first pool's value. A heterogeneous topology — for example a 4-drive pool plus a 2-drive pool created during expansion — previously inherited the first pool's parity and could resolve to zero data shards in the smaller pool, panicking Reed-Solomon construction on write. Automatic parity now resolves per pool (for example `2+2` in the 4-drive pool and `1+1` in the 2-drive pool). Fixes #4801.

### Added
- **On-Demand Migration**: Lazy, pull-style migration of an existing S3-compatible bucket into RustFS. A local bucket is attached to an external source bucket; a GET for a key that does not exist locally fetches it from the source, streams it to the client, and stores it locally in the same pass, so every later read is served locally. The module is on by default; set `RUSTFS_ON_DEMAND_MIGRATION_ENABLED=false` on every node to turn it off. A bucket with no source configured behaves exactly as before — the runtime never intervenes on its reads and makes no outbound call. Operator guide at `docs/operations/on-demand-migration.md`.
  - Per-bucket configuration persisted as `on-demand-migration.json` in the bucket metadata: source provider (`s3`, `aws`, `minio`, `rustfs`, `r2`, `gcs`), endpoint, region, addressing style, credentials and TLS material, an optional key-prefix filter and source-prefix rewrite, and a policy block covering the inline size threshold, multipart part size, concurrency, queue capacity, timeouts, bandwidth limit and negative-cache TTL
  - Admin routes under `/rustfs/admin/v3/on-demand-migration/{bucket}`: `PUT` (with `?dry-run=true` to validate and probe the source without saving), `GET`, `DELETE`, `GET .../status`, plus `POST .../backfill?op=start|cancel` and `GET .../backfill` for the background full-backfill job with its resumable checkpoint. Authorized by the new `admin:GetBucketOnDemandMigration` and `admin:SetBucketOnDemandMigration` actions; every response redacts `secret_key` and `session_token`
  - Read paths: an object at or below `policy.inline_max_bytes` (16 MiB by default) is teed to the client and to the local store in a single source read; a larger object or a Range read streams through and a background pull stores the whole object. A HEAD miss is proxied to the source and stores nothing (`policy.head = local_only` disables it). Every source-backed response carries `x-rustfs-on-demand-migration: source`
  - Protections: a per-source circuit breaker, a per-key negative cache, singleflight per key, a concurrency limit and a bounded pull queue shared by the inline and background paths, an optional bandwidth limit, an anti-loop request marker, and the shared outbound-endpoint (SSRF) policy
  - Metrics under `rustfs_on_demand_migration_*` (`requests_total`, `pulled_bytes_total`, `pulled_objects_total`, `pull_failures_total`, `inflight_pulls`, `queue_depth`, `source_latency_seconds_*`, `breaker_state`), mirrored per node by the admin status route
  - Listings: `ListObjects` v1 remains local with ordinary key markers. `ListObjectsV2` can merge source objects when `policy.list_through = true`; this is off by default
  - Upgrade and rollback: finish upgrading every node before enabling ODM. An rc.5 node that writes bucket configuration drops the ODM fields from metadata; neither a later restart nor moving the service out of ECStore recovers them. Before rollback, disable ODM and securely retain the original full configuration and credentials. After every node returns to a compatible version, restore and validate that configuration. Redacted exports cannot replace the credential backup; source-only objects are unavailable through RustFS while ODM is disabled. See the upgrade and rollback section of `docs/operations/on-demand-migration.md`
  - Optional Google dependencies: default and `full` server builds retain native GCS support. `cargo build -p rustfs --no-default-features --features ftps,webdav` excludes Google SDKs while preserving configuration decoding and redaction; native GCS ODM and tier operations require the `gcs` feature. Do not use that build with existing GCS-tiered data
  - Limitations: PUT and DELETE never reach the source; a source object updated after it was pulled is not re-fetched; SSE-C source objects are unsupported and answer 424; `Last-Modified` on a pulled object is the local write time, with the source timestamp kept in metadata
- **NATS JetStream Publish Path**: Opt-in at-least-once delivery for the NATS notify and audit targets. A NATS Core publish flushes to the connection without awaiting a broker acknowledgement, so an event can be lost across a broker restart or a reconnect after the send queue has already cleared it. A queued event now clears only after the JetStream `PublishAck`, so bucket notifications survive those interruptions. Off by default and byte-identical to the NATS Core path when disabled.
  - Three configuration keys per target: `JETSTREAM_ENABLE`, `JETSTREAM_STREAM_NAME`, and `JETSTREAM_ACK_TIMEOUT_SECS`, under the `RUSTFS_NOTIFY_NATS_` and `RUSTFS_AUDIT_NATS_` prefixes
  - Durable store-and-forward with a stable dedup id sent as the `Nats-Msg-Id` header, so a replay after a crash is collapsed by the server duplicate window
  - Pre-flight stream validation, and a bounded failed-events store (count and TTL). Only a non-retryable rejection is recorded in the failed-events store. A retryable condition keeps the entry on the live queue until it is delivered
  - Operator guide at `docs/operations/nats-jetstream.md`
- **OpenStack Keystone Authentication Integration**: Full support for OpenStack Keystone authentication via X-Auth-Token headers
  - Tower-based middleware (`KeystoneAuthLayer`) self-contained within `rustfs-keystone` crate
  - Task-local storage for async-safe credential passing between middleware and auth handlers
  - Automatic detection of Keystone credentials (access keys prefixed with `keystone:`)
  - Role-based permission mapping (admin/reseller_admin roles grant owner permissions)
  - Token caching for high-performance validation with configurable cache size and TTL
  - Dual authentication support: Keystone and standard AWS Signature v4 work simultaneously
  - Immediate 401 response for invalid tokens (no fallback to local auth)
  - XML-formatted error responses compatible with S3 API
  - Comprehensive integration documentation with manual testing guide
  - **32 unit and integration tests** covering middleware, auth handlers, task-local storage, and role detection
- **SFTPv3 Protocol Support**: SSH-hosted SFTPv3 subsystem that translates each file operation into S3 calls against the local object store. Authentication uses IAM credentials (SSH username = access key, SSH password = secret key).
  - Full SFTPv3 packet coverage: open, read, write, stat, lstat, fstat, mkdir, rmdir, rename, remove, opendir, readdir, realpath, close, plus the rest of the 21-packet specification
  - Streaming multipart write up to the part size times 10000 parts (156.25 GiB at the default part size)
  - Per-handle read-ahead cache with configurable window size and process-wide memory ceiling
  - Per-session liveness watchdog: Linux probes `/proc/net/tcp` and cancels wedged sessions on the order of 45 seconds; non-Linux falls back to an inactivity ceiling on the order of 30 minutes
  - 30-second SSH handshake deadline, per-call backend operation timeout, bounded multipart-abort fan-out, graceful-shutdown cascade
  - 34 SFTPv3 compliance test cases under `crates/e2e_test/src/protocols/sftp_compliance.rs` spread across three entry points: `test_sftp_compliance_suite` (shared session), `test_sftp_compliance_readonly` (read-only mode), and `test_sftp_compliance_standalone` (one rustfs spawn per case)
  - Four-layer regression-prevention tests guard against silent feature deletion: compile-time module assertion, module-presence unit test, cross-module `Protocol` enum assertion, end-to-end SSH banner test against the running binary

### Changed
- **Encryption and KMS work merged since `1.0.0-rc.5`** (entries were missing from this section):
  - **Persisted KMS configuration secrets** are sealed field-by-field with `RUSTFS_KMS_CONFIG_SECRET`. **When the variable is unset the secrets are persisted in cleartext and the server only warns** (`persisted KMS configuration carries cleartext secrets`); it never refuses the write. Set it, identically, on every node, and re-save the configuration to seal an existing one.
  - **New v2 ciphertext frame format** with per-frame index binding and final-frame authentication. Its write switch `RUSTFS_ENCRYPTION_FRAME_V2` is **off by default**: v2 frames are unreadable by nodes without v2 read support, and encrypted ciphertext travels verbatim through transition, decommission and SSE-C replication passthrough, so turn it on only after every node — and every RustFS warm/replication target that receives raw ciphertext — runs a release with v2 read support. Reading v2 objects needs no switch.
  - **Per-key SSE-KMS authorization** (`RUSTFS_KMS_ENFORCE_SSE_KEY_POLICY`, default `false`). With it on, anonymous callers hold no KMS grants, so **a public bucket serving SSE-KMS objects is an incompatible combination** and those reads return `AccessDenied`.
  - Envelope context binding as KMS AAD (`ENV_KMS_ENVELOPE_AAD`, off by default; a node that predates the field cannot open bound envelopes).
  - Vault custom CA and mutual TLS; object-level DEK rewrap plus a batch rekey admin API; a backend-locality runtime signal on `kms/status`.
  - Single-pass decryption for encrypted GET, and encrypted single-part closed-range seek — the latter is now **on by default** (`RUSTFS_ENCRYPTED_RANGE_SEEK`, default `true`; the switch remains as a kill switch).
- **Vault static tokens are now tracked and renewed**: with `Token` authentication RustFS hard-coded "this token has no lease", so the renewal task never started and no remaining-TTL gauge was published. `vault token create` grants a 768-hour TTL by default, which turned a healthy-looking cluster into one where every KMS call returned 403 about a month later, with no self-healing short of a restart or reconfigure. RustFS now calls `auth/token/lookup-self` at login and adopts what Vault reports: a non-expiring token behaves exactly as before, an expiring renewable one is renewed at half TTL like the other auth methods, and an expiring non-renewable one logs `vault_static_token_not_renewable` and publishes its remaining TTL. The probe never fails the login: a token whose policy omits `lookup-self` (Vault's `default` policy grants it), or a Vault that is unreachable at that moment, logs `vault_static_token_lookup_failed` and falls back to the previous no-lease behaviour, so no deployment that works today stops working.
- **SSE-C over a plaintext transport is reported**: AWS S3 and MinIO refuse an SSE-C request that did not arrive over TLS, because the customer key travels in a request header. RustFS accepted them on any transport and still does by default — flipping to a rejection inside a release window would break plaintext staging and test deployments. Each such request now increments `rustfs_ssec_plaintext_requests_total` and logs one `ssec_request_without_tls` warning per process, and `RUSTFS_SSE_C_REQUIRE_TLS=true` opts into the AWS `400` now. The default is expected to flip in a later release; confirm the counter reads zero first. The verdict is per connection: a TLS listener satisfies it, and so does an `https` protocol forwarded by a proxy the trusted-proxy configuration accepts.
- **Local KMS backend on a distributed deployment says what actually breaks**: the backend keeps key material and its Argon2id salt on each node's own disk, so two nodes derive different keys from the same `master_key` and an object encrypted on one node cannot be decrypted on another — intermittent 500s behind a load balancer. Configuring it while the deployment is distributed now logs `kms_node_local_backend_in_distributed_deployment` and appends that consequence to the `kms/configure` response, instead of only the generic "development only" positioning warning. It remains a warning, not a gate.
- **SSE-KMS is refused when no KMS is running (breaking)**: a write requesting `x-amz-server-side-encryption: aws:kms` on a node with no KMS service no longer succeeds. Earlier releases wrapped the data key with the node-local `RUSTFS_SSE_S3_MASTER_KEY` while still writing `aws:kms` and the requested key id into the object metadata — metadata that claimed a KMS protection the object never had, under a key that was never consulted. Such a request now returns `400 InvalidRequest` when KMS was never configured and `503` when a configured service is not running; the refusal is evaluated after the per-key authorization gate, so an unauthorized caller still receives `403 AccessDenied`. **Upgrade note:** a deployment that relied on this write succeeding will start receiving 4xx/503. Either configure a KMS, or request `AES256` and keep the documented SSE-S3 local-master-key fallback, which is unchanged. Objects already written this way remain readable.
- **Legacy ciphertext nonce layouts are now locked per segment**: while decrypting a v1 segment, the reader locks onto whichever of the three historical nonce layouts decoded the segment's first non-zero-index frame and rejects any later frame that needs a different one. Because a frame encrypted at block index zero authenticates under the pre-`1.0.0-alpha.91` reused-part-nonce layout at any position, an attacker able to rewrite the underlying shards could previously replay it and have the forged plaintext returned with `200`. New `RUSTFS_ENCRYPTION_LEGACY_NONCE_FALLBACK` (default `true`) drops that third layout entirely when set to `false`, which closes the residual case of a stream built purely from repeats of frame zero. Turn it off only after migrating pre-alpha.91 encrypted objects (rewrite in place with CopyObject); see [KMS backend security properties](docs/operations/kms-backend-security.md) for what the v1 frame layout does and does not authenticate.
- **HTTP Server Stack**: Integrated `KeystoneAuthLayer` middleware from `rustfs-keystone` crate into service stack (positioned after ReadinessGateLayer)
- **Storage-class validation on startup (upgrade note)**: A persisted explicit storage class (`RUSTFS_STORAGE_CLASS_STANDARD` / `RUSTFS_STORAGE_CLASS_RRS`, for example `EC:2`) is now validated against the actual per-pool drive counts at startup and rejected when a pool cannot satisfy it. This is fail-closed and correct, but a cluster that persisted a storage class larger than a small or heterogeneous pool can hold (for example `EC:2` alongside a 2-drive pool), which earlier releases accepted and silently resolved to an invalid layout, will now refuse to start after upgrade. To recover, unset `RUSTFS_STORAGE_CLASS_STANDARD` so the server derives a valid per-pool default automatically, or set it to a value every pool can satisfy.
- **IAMAuth**: Enhanced `get_secret_key()` to return empty secret for Keystone credentials (bypasses signature validation)
- **Auth Module**: Modified `check_key_valid()` to retrieve Keystone credentials from task-local storage and determine admin status
- **`StorageBackend` trait**: extended with multipart upload methods (`create_multipart_upload`, `upload_part`, `complete_multipart_upload`, `abort_multipart_upload`) plus `upload_part_copy`. Streaming-upload code path is now available to FTPS, WebDAV, and Swift drivers as well.
- **`Protocol` enum**: new `Protocol::Sftp` variant with corresponding `S3Action` mappings. Every match arm on `Protocol` updated to handle the new variant exhaustively.

### Technical Details
- Middleware is self-contained in `rustfs-keystone` crate following the trusted-proxies pattern for integration-specific middleware
- Uses `BoxBody` pattern for Hyper 1.x compatibility
- Task-local storage provides request-scoped credential passing without modifying HTTP request/response types
- Integration preserves existing S3 authentication flow while adding Keystone support
- Zero breaking changes to existing functionality
- No new top-level directories in main binary crate (middleware lives in integration crate)
- SSH/SFTP wire handling via the `russh` and `russh-sftp` crates. SFTPv3 framing is implemented by `russh-sftp`; the rustfs-side `SftpDriver` implements `russh_sftp::server::Handler` and dispatches to the storage backend
- Drop-time abort for in-flight multipart uploads honours IAM Deny on `AbortMultipartUpload`. `start_multipart_upload` caches the authorisation decision so the synchronous `Drop` path can honour Allow / Deny policies without re-querying IAM
- Per-handle read cache uses an `Arc<AtomicU64>` shared across every `SftpDriver` instance to enforce a process-wide memory ceiling. On ceiling breach the populate is skipped and the read serves correctly via a single-call backend fetch
- Per-session liveness watchdog runs as a tokio task per accepted connection. Reads `/proc/net/tcp` and `/proc/net/tcp6` to look up the (local, peer) tuple's TCP state and cancels via `tokio_util::sync::CancellationToken` when wedge conditions are confirmed across two consecutive ticks
- Path canonicalisation rejects paths containing `\0`, `\r`, or `\n` and resolves traversal via `path::clean()` before any backend dispatch
- Cipher / KEX / MAC / host-key algorithm allowlists are hardcoded with no environment override. Strict-KEX (CVE-2023-48795 / Terrapin) marker presence asserted by unit test
- Per-session handle cap (default 64, configurable 8 to 1024) with UUID-generated handle ids
- Crate-level `#![deny(unsafe_code)]` is in force across `crates/protocols`. Socket fd duplication for the watchdog uses the safe `AsFd::try_clone_to_owned` path (Linux). Non-Linux targets use the inactivity-ceiling watchdog
- Platform-specific imports are cfg-gated. Unix enforces owner-only host-key mode bits (no group or other permission bits). Windows loads host keys without a mode check and trusts operator-managed NTFS ACLs. Targets that are neither Unix nor Windows fail SFTP at config-load with SftpInitError::UnsupportedPlatform

### Documentation
- Updated `crates/keystone/README.md` with complete integration architecture and workflow
- Added detailed manual testing guide with 10 test scenarios
- Updated main `README.md` to list Keystone authentication as available feature
- Added troubleshooting section for common integration issues
- Module-level rustdoc on `crates/protocols/src/sftp/mod.rs` describing the public API surface, configuration contract, and the architecture of the read cache and the wedge watchdog

### Configuration
New environment variables:
- `RUSTFS_KEYSTONE_ENABLE` - Enable/disable Keystone authentication (default: false)
- `RUSTFS_KEYSTONE_AUTH_URL` - Keystone API endpoint URL
- `RUSTFS_KEYSTONE_VERSION` - Keystone API version (v3)
- `RUSTFS_KEYSTONE_ADMIN_USER` - Admin username for privileged operations
- `RUSTFS_KEYSTONE_ADMIN_PASSWORD` - Admin password
- `RUSTFS_KEYSTONE_ADMIN_PROJECT` - Admin project name
- `RUSTFS_KEYSTONE_ADMIN_DOMAIN` - Admin domain name (default: Default)
- `RUSTFS_KEYSTONE_CACHE_SIZE` - Token cache size (default: 10000)
- `RUSTFS_KEYSTONE_CACHE_TTL` - Token cache TTL in seconds (default: 300)
- `RUSTFS_KEYSTONE_VERIFY_SSL` - Verify SSL certificates (default: true)
- `RUSTFS_SFTP_ENABLE` - Enable/disable SFTP (default: false)
- `RUSTFS_SFTP_ADDRESS` - Listen address (default: 0.0.0.0:2222)
- `RUSTFS_SFTP_HOST_KEY_DIR` - Directory containing host key files (must exist). On Unix each file must grant no group or other permission bits (owner access only). On Windows the files load without a mode check and rustfs trusts the directory NTFS ACL
- `RUSTFS_SFTP_HOST_KEY_RELOAD_ENABLE` - Rescan the host-key directory without a restart (default: false)
- `RUSTFS_SFTP_HOST_KEY_RELOAD_INTERVAL` - Host-key rescan interval in seconds, minimum 5 (default: 30)
- `RUSTFS_SFTP_IDLE_TIMEOUT` - Session idle timeout in seconds (default: 600)
- `RUSTFS_SFTP_PART_SIZE` - Multipart part size in bytes (default: 16 MiB)
- `RUSTFS_SFTP_READ_ONLY` - Reject write packets at the protocol layer (default: false)
- `RUSTFS_SFTP_BANNER` - SSH protocol identification string, must begin with `SSH-2.0-` (default: `SSH-2.0-RustFS`)
- `RUSTFS_SFTP_HANDLES_PER_SESSION` - Per-session open-handle cap, 8 to 1024 (default: 64)
- `RUSTFS_SFTP_BACKEND_OP_TIMEOUT_SECS` - Per-call backend deadline in seconds, 5 to 600 (default: 60)
- `RUSTFS_SFTP_READ_CACHE_WINDOW_BYTES` - Per-handle read-cache window in bytes, 256 KiB to 64 MiB or 0 to disable (default: 4 MiB)
- `RUSTFS_SFTP_READ_CACHE_TOTAL_MEM_BYTES` - Process-wide read-cache memory ceiling in bytes, 16 MiB minimum (default: 256 MiB)

### Files Added
- `crates/protocols/src/sftp/mod.rs` - SFTP module entry point, public API surface, crate-level rustdoc, regression-prevention test
- `crates/protocols/src/sftp/config.rs` - `SftpConfig` and `SftpInitError` types, env-var resolvers, host-key directory loader with permission enforcement
- `crates/protocols/src/sftp/constants.rs` - Named constants grouped by purpose: S3 error codes, HTTP error codes, POSIX mode bits, protocol identifiers, operational limits
- `crates/protocols/src/sftp/server.rs` - `SftpServer` SSH server, russh handler, password authentication against IAM, accept loop, per-session task spawn
- `crates/protocols/src/sftp/driver.rs` - `SftpDriver` per-session SFTPv3 handler dispatching each operation onto the `StorageBackend`
- `crates/protocols/src/sftp/state.rs` - `HandleState` variants for read, write-buffering, write-streaming, write-failed handles
- `crates/protocols/src/sftp/lifecycle.rs` - Per-session activity stamp, weak-ref registry, `/proc/net/tcp` probe for the wedge watchdog
- `crates/protocols/src/sftp/wedge_watchdog.rs` - Per-session liveness watchdog cancelling sessions silent at the SFTP layer while the kernel reports CLOSE_WAIT
- `crates/protocols/src/sftp/fallback_watchdog.rs` - Per-session silence-only liveness backstop for non-Linux targets, cancelling sessions only at the fallback idle ceiling
- `crates/protocols/src/sftp/read_cache.rs` - Per-handle in-memory read-ahead cache with shared atomic accumulator for the process-wide memory ceiling
- `crates/protocols/src/sftp/attrs.rs` - SFTPv3 `FileAttributes` mapping for objects and directories, longname formatting, mtime clamping
- `crates/protocols/src/sftp/dir.rs` - OPENDIR / READDIR pagination, root-bucket listing, sub-directory listing under a prefix
- `crates/protocols/src/sftp/errors.rs` - `SftpError` thiserror enum and S3-error classification into SFTPv3 status codes
- `crates/protocols/src/sftp/paths.rs` - Path canonicalisation, traversal rejection, `\0` / `\r` / `\n` rejection, bucket+key decomposition
- `crates/protocols/src/sftp/read.rs` - READ packet handler, EOF semantics, `MAX_READ_LEN` bound, integration with the read cache
- `crates/protocols/src/sftp/write.rs` - WRITE packet handler, in-memory buffering up to part size, transition to streaming multipart, CLOSE finalisation
- `crates/protocols/src/sftp/test_support.rs` - Test fixtures and helper builders for SFTP unit tests
- `crates/protocols/src/common/dummy_storage.rs` - In-memory `StorageBackend` test backend covering every method, used by SFTP unit tests and the FTPS / Swift / WebDAV test suites
- `crates/e2e_test/src/protocols/sftp_core.rs` - End-to-end regressions for the handshake deadline, idle-timeout disconnect, and the wedge watchdog
- `crates/e2e_test/src/protocols/sftp_compliance.rs` - SFTPv3 compliance suite entry points (`test_sftp_compliance_suite`, `test_sftp_compliance_readonly`, `test_sftp_compliance_standalone`)
- `crates/e2e_test/src/protocols/sftp_compliance_tests.rs` - Per-case test bodies (CMPTST-01..34), shared fixture helpers, lifecycle counters
- `crates/e2e_test/src/protocols/sftp_helpers.rs` - SFTP-specific test helpers and fixture seeders

### Files Modified
- `crates/keystone/src/middleware.rs` - Created Keystone authentication middleware (self-contained in keystone crate)
- `crates/keystone/src/lib.rs` - Exported middleware module and KEYSTONE_CREDENTIALS
- `crates/keystone/Cargo.toml` - Added Tower/HTTP dependencies for middleware functionality
- `rustfs/src/server/http.rs` - Integrated KeystoneAuthLayer from rustfs-keystone crate
- `rustfs/src/auth.rs` - Enhanced IAMAuth and check_key_valid for Keystone support, imported KEYSTONE_CREDENTIALS from rustfs-keystone
- `crates/keystone/README.md` - Comprehensive integration documentation
- `README.md` - Added Keystone as available feature
- `Cargo.toml` - Added the `sftp` feature alongside the existing protocol features
- `Cargo.lock` - Updated to include the new `russh`, `russh-sftp`, `socket2`, `tokio-util`, `subtle`, `uuid` dependencies and their transitive crates
- `crates/protocols/Cargo.toml` - Declared `russh`, `russh-sftp`, `socket2`, `tokio-util`, `subtle`, `uuid` under the `sftp` feature flag
- `crates/protocols/src/lib.rs` - Added `pub mod sftp` behind `#[cfg(feature = "sftp")]` plus the crate-level `#![deny(unsafe_code)]` lint
- `crates/protocols/src/common/client/s3.rs` - Extended the `StorageBackend` trait with `create_multipart_upload`, `upload_part`, `complete_multipart_upload`, `abort_multipart_upload`, and `upload_part_copy`
- `crates/protocols/src/common/session.rs` - Added the `Protocol::Sftp` variant and its `S3Action` mappings
- `crates/protocols/src/common/gateway.rs` - Handles the new `Protocol::Sftp` variant exhaustively
- `crates/protocols/src/common/mod.rs` - Exposed the new `dummy_storage` module
- `crates/protocols/src/constants.rs` - Added shared POSIX mode-bit constants used by SFTP and other protocols
- `crates/config/src/constants/protocols.rs` - `RUSTFS_SFTP_*` environment variable names and defaults
- `crates/utils/src/retry.rs` - Added the generic exponential-backoff retry helper used by the SFTP write path
- `crates/e2e_test/Cargo.toml` - Added the e2e test dependencies for SFTP (paramiko fixture, SSH keypair generation)
- `crates/e2e_test/src/protocols/mod.rs` - Registered the new `sftp_core`, `sftp_compliance`, `sftp_compliance_tests`, and `sftp_helpers` modules
- `crates/e2e_test/src/protocols/README.md` - Documented the SFTP test entry points and case index
- `crates/e2e_test/src/protocols/test_env.rs` - Added SFTP host-key directory provisioning to the shared protocol test environment
- `crates/e2e_test/src/protocols/test_runner.rs` - Wired the SFTP entry points into the runner
- `rustfs/Cargo.toml` - Added the `sftp` feature flag
- `rustfs/src/lib.rs` - One-line addition exporting the SFTP wiring
- `rustfs/src/init.rs` - Build and start the `SftpServer` when `RUSTFS_SFTP_ENABLE` is true
- `rustfs/src/main.rs` - Routed shutdown signals to the SFTP server alongside the other protocols
- `rustfs/src/protocols/client.rs` - Client-builder support for the new `Protocol::Sftp` variant

### Testing
- 16 unit tests in rustfs-keystone crate (config, auth, middleware, identity)
- 10 integration tests in rustfs-keystone crate (task-local storage, middleware layer, scope isolation)
- 6 auth unit tests in rustfs crate (role detection, task-local storage, Keystone credential handling)
- **Total: 32 tests** passing with zero compilation errors
- Manual testing guide provided for end-to-end validation
- All Keystone tests passing with `cargo test --all --exclude e2e_test`
- 34 SFTPv3 compliance test cases (CMPTST-01..34) split across three entry points: `test_sftp_compliance_suite` (shared session, cases 01-14), `test_sftp_compliance_readonly` (read-only mode, cases 15-23), `test_sftp_compliance_standalone` (one rustfs spawn per case, cases 24-34)
- Regression-prevention tests at four layers: compile-time module assertion in `crates/protocols/src/lib.rs`, module-presence unit test in `crates/protocols/src/sftp/mod.rs`, cross-module `Protocol` enum assertion, and end-to-end SSH banner test against the running binary
- Standalone end-to-end regressions for the SSH handshake deadline, the idle-timeout disconnect path, and the wedge watchdog (Linux fast-kill and the cross-platform fallback path)
- Inline unit tests in every SFTP source file covering pure helpers (path canonicalisation, attribute mapping, S3-error classification, env-var bound resolvers)
- Strict-KEX (CVE-2023-48795) marker presence assertion as a unit test in `crates/protocols/src/sftp/server.rs`
- All tests passing with `cargo test --all --features sftp` against a 64-bit Linux target

---

## Previous Releases

See [GitHub Releases](https://github.com/rustfs/rustfs/releases) for previous version history.
