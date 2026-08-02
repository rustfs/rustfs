# KMS backend security properties

RustFS ships several KMS backends. They differ not only in deployment effort but in **where master key material lives and who can read it**. Pick a backend based on the confidentiality boundary you need, not on the name alone.

For how the Vault backends authenticate (static token, AppRole, Vault Agent token file) and how credential refresh and the fail-closed window behave, see the [Vault KMS authentication runbook](vault-kms-authentication.md). For what may be claimed about the cryptographic implementations themselves, see [Cryptographic compliance positioning](kms-cryptographic-compliance.md). For which RustFS identities may manage or use a given key, see [Per-key KMS authorization](kms-per-key-authorization.md). If you are migrating from MinIO, read [Migrating from MinIO: encrypted objects do not carry over](#migrating-from-minio-encrypted-objects-do-not-carry-over) first.

## Backend comparison

| Backend | Config tag | Master key material location | At-rest protection of key material | Durability | Rotation | Intended use |
| --- | --- | --- | --- | --- | --- | --- |
| Local | `Local` | Files under `key_dir`, encrypted with the configured local master key | Local master key (AES-GCM) + file permissions | Crash-durable commits on local filesystems only; see [Local backend durability and deployment support matrix](#local-backend-durability-and-deployment-support-matrix) | Rejected by design (single material, development backend) | Development; single-node setups that accept host-level trust |
| Static | `Static` | Provided out-of-band via environment/file; never persisted by RustFS | Operator-managed secret distribution | No state persisted by RustFS | Rejected (read-only backend) | Simple deployments with an external secret manager |
| Vault KV2 | `VaultKV2` (legacy alias `Vault`) | Stored **directly** in Vault KV v2 (Base64-encoded plaintext) | Vault ACLs + KV v2 at-rest encryption + TLS only | Delegated to Vault storage | Versioned retention (immutable per-version records + current pointer) | Deployments that accept Vault KV ACLs as the sole confidentiality boundary |
| Vault Transit | `VaultTransit` | Key-encryption keys never leave Vault; only Transit ciphertext is visible outside | Vault Transit engine (cryptographic isolation) | Delegated to Vault storage | Via Vault Transit key versioning | Deployments that need key material to be unreadable through storage APIs |
| AWS KMS | `AWS` (alias `AwsKms`) | Key material never leaves AWS KMS; RustFS mirrors no key state | AWS KMS (cryptographic isolation) + IAM | Delegated to AWS | On-demand `RotateKeyOnDemand`; prior backing keys stay usable for decryption | Deployments already rooted in AWS IAM that want AWS as the cryptographic root — read [AWS KMS: deviations from the shared backend contract](#aws-kms-deviations-from-the-shared-backend-contract) first |

## Migrating from MinIO: encrypted objects do not carry over

> **Warning: RustFS does not currently support reading objects that MinIO encrypted.**
> This applies to SSE-S3, SSE-KMS, and SSE-C, in every released binary and container image, and it holds regardless of which KMS backend you configure. Configuring the `Static` backend with the same key material MinIO used does **not** make those objects readable — MinIO wraps data keys in a different envelope format that no RustFS backend produces or accepts (`crates/kms/src/config.rs:304-308`). Plan for this **before** moving data. Tracked in rustfs/backlog#1638.

Two properties of the failure make it easy to discover too late:

- **It does not fail closed.** The read path treats a MinIO-encrypted object as unencrypted and returns the stored ciphertext instead of raising an error. A GET that returns 200 is not evidence that an object migrated correctly.
- **Surrounding metadata migrates fine.** The object's `xl.meta` parses, so encrypted objects list and HEAD normally and report plausible sizes. Only the payload is wrong.

Verify by content, not by status code: checksum a sample of encrypted objects against the source before decommissioning the MinIO deployment.

Current options for a migration whose source contains encrypted objects:

- Decrypt on the MinIO side first, migrate plaintext, then let RustFS re-encrypt with its own KMS.
- Copy through the S3 API rather than moving drives — MinIO decrypts on read, and RustFS encrypts on write. This re-encrypts rather than preserving ciphertext and costs a full data transfer.
- Leave encrypted objects on MinIO and migrate only unencrypted data.

Inventory the source before choosing: bucket default-encryption settings mean objects can be encrypted without any client having sent SSE headers.

The same limitation applies in reverse — objects RustFS encrypts are not readable by MinIO. For the code-level breakdown of which seams block each SSE mode, see [MinIO file-format interoperability, Part C](../architecture/minio-file-format-compat.md#part-c--server-side-encryption-sse).

## Vault KV2: what the backend does and does not do

The Vault KV2 backend uses Vault purely as a **secure storage** service:

- Master key material is generated by RustFS and written to KV v2 as a Base64-encoded value (`encrypted_key_material` is an encoding, not a ciphertext).
- The backend never calls the Vault Transit engine. The `mount_path` configuration field and the `RUSTFS_KMS_VAULT_MOUNT_PATH` environment variable are deprecated leftovers: they are accepted for compatibility and ignored.
- Data-encryption keys (DEKs) handed to the object-encryption path are still wrapped with AES-256-GCM under the master key; the statement above concerns the master key's storage in Vault, not the DEK envelope.
- The backend reports this boundary in its `backend_info` metadata as `at_rest_protection: vault-kv2-acl`.
- Key rotation retains every historical master key version as an immutable record under `{prefix}/{key_id}/versions/{N}` and only then moves the current-version pointer; see [Master key rotation](#master-key-rotation-retention-destruction-and-upgrade-ordering) for the retention preconditions and the cluster-upgrade ordering constraint.

> **Warning: KV read access is equivalent to holding the master keys.**
> Any Vault identity (token, AppRole, or policy) that can `read` the RustFS key path in KV v2 can recover the plaintext master key material and decrypt every object protected by those keys. Treat KV read grants on that path with the same care as handing out the keys themselves. If this is not acceptable, use the Vault Transit backend instead.

## Minimal Vault policy for the KV2 backend

Scope the RustFS token/AppRole to exactly the KV v2 mount and key prefix it is configured with (defaults shown: mount `secret`, prefix `rustfs/kms/keys`), and grant no other identity read access to that subtree:

```hcl
# RustFS KMS (Vault KV2 backend) — key storage only, no Transit access needed.
path "secret/data/rustfs/kms/keys/*" {
  capabilities = ["create", "read", "update"]
}

path "secret/metadata/rustfs/kms/keys/*" {
  capabilities = ["list", "read", "delete"]
}
```

Notes:

- The trailing wildcards also cover the per-version material records that rotation creates under `.../keys/{key_id}/versions/{N}`; no extra policy paths are needed.
- `delete` on the metadata path is required for permanent key deletion (`force_immediate`); drop it if you never hard-delete keys. RustFS refuses `force_immediate` unless the server sets `RUSTFS_KMS_ALLOW_IMMEDIATE_DELETION=true`, so leaving that gate off keeps the capability unreachable no matter what the Vault policy allows.
- Do not attach `sudo`, wildcard mounts, or Transit paths to this policy; the KV2 backend does not use them.
- Auditing KV reads on the key prefix is strongly recommended: every read event is a potential master-key disclosure.

## Master key rotation: retention, destruction, and upgrade ordering

Rotation support differs per backend. Local and Static reject rotation outright (`InvalidOperation`); their single key material is never overwritten. Vault Transit delegates rotation to the Transit engine's own key versioning (ciphertext is version-prefixed, e.g. `vault:v1:...`). Vault KV2 rotates by retaining every historical version, as described below. Rotation is reachable through the admin API as `POST /rustfs/admin/v3/kms/keys/rotate`, which the route policy classifies as high risk and gates behind `kms:RotateKey`; it is not exposed through the S3 surface. The upgrade ordering constraint below therefore applies to an operator action, not only to a call from inside the process.

### Vault KV2 versioned retention model

Each rotation writes the new version's material to `{prefix}/{key_id}/versions/{N}` as an immutable, create-only record, and only after that material is durably persisted does a check-and-set write move the top-level record (the current-version pointer, which also mirrors the current material as a fast path). The first rotation additionally freezes the pre-rotation material as a version record and pins it as the key's `baseline_version`; DEK envelopes written before versioning existed (no `master_key_version` field) always resolve to that baseline, never to whatever version is current.

Decryption loads exactly the version recorded in the envelope and fails closed with a typed `KeyVersionNotFound` error when that version's record is missing. There is deliberately no fallback to the current material: falling back would silently feed the wrong key to AEAD and mask tampered envelopes.

### Retention and destruction preconditions

- Every version record that any stored DEK envelope references must remain readable. Until an object rewrap/migration capability exists, assume **every** version of a rotated key is referenced: destroying a version record permanently orphans all objects whose DEKs it wrapped.
- Version records are ordinary KV v2 secrets under the key subtree. Never run `kv metadata delete` or `kv destroy` against `{prefix}/{key_id}/versions/*`, and do not apply `delete-version-after` or retention tooling to that subtree. RustFS-managed retention does not rely on KV2's own secret versioning (each version record has a single KV revision), so KV `max-versions` settings do not protect or endanger history — but metadata deletion always removes a record entirely.
- Permanent key deletion through RustFS (`force_immediate` after `PendingDeletion`) purges the key's version records together with the key record; that is the only supported way to remove them. It is refused by default: the server must set `RUSTFS_KMS_ALLOW_IMMEDIATE_DELETION=true`, and the request must be a `DELETE` with a JSON body that sets `force_immediate` and echoes the key id back as `confirm_key_id` — the query-parameter form (`?force_immediate=true`) is refused outright, whatever the gate is set to. Leave the gate off unless you are actively destroying keys, and turn it off again afterwards — the pending-deletion window plus `CancelKeyDeletion` is the only recovery path for objects encrypted under the key.
- For Vault Transit, retention is governed by the Transit key's `min_decryption_version`: never raise it above the oldest version that may still protect live ciphertext.
- `force_immediate` is additionally refused, with a `409 Conflict`, while any bucket's default encryption configuration still names the key, or while the key is the KMS service default key. A scheduled deletion is not refused for that reason: it destroys nothing and stays cancellable, and the background sweep re-checks the same references before it destroys the material.

### Reading the `impact` section

`DeleteKey` responses always carry an `impact` section listing the configuration that currently points at the key — the buckets whose default encryption names it, and whether it is the service default key — so the references that will refuse the destruction are visible when the deletion is scheduled rather than only in a server-side log once the window has run out.

`DescribeKey` (`GET /rustfs/admin/v3/kms/keys/{key_id}`) can return the same section, but only when the request asks for it with `impact=true`. It is opt-in there because collecting it lists every bucket and `DescribeKey` is polled; without the parameter the endpoint does exactly the work it did before and returns no `impact` field at all. A value other than `true` or `false` is rejected with `400` rather than treated as `false`, so a typo can never answer a request for the section with a response that merely lacks one. **An absent section means "not collected", never "nothing references this key".**

Read it for what it says and nothing more. `coverage.scanned` names the sources that were read; `coverage.not_scanned` names the ones that were not, which currently includes every object encrypted under the key. `completeness` is `exact` only over the scanned sources, and `unavailable` when a source could not be read at all — an unavailable report is not an empty one, and both an unreadable source and an outstanding reference will stop the sweep from destroying the material.

**An empty `references` list does not mean the key is unused.** No object metadata is consulted, so a key with no configuration references can still protect an arbitrary amount of live data, which stays readable only until the material is gone. There is no field in the response that asserts otherwise, and none should be inferred from one.

### Upgrade before first rotation (hard constraint)

Do not rotate any key until **every** RustFS node in the cluster runs a build that understands the `master_key_version` envelope field. Older binaries ignore the field and always decrypt with the current material: harmless while nothing has been rotated, but after a rotation they will fail to decrypt every object wrapped by an earlier key version. Complete the rolling upgrade of the entire cluster first, then rotate.

This is the sharpest instance of a broader class of constraints; the rest are collected in [Mixed-version clusters during a rolling upgrade](#mixed-version-clusters-during-a-rolling-upgrade).

## Mixed-version clusters during a rolling upgrade

During a rolling upgrade the cluster runs two RustFS builds at once. That window matters more for KMS than for most subsystems, because KMS state is shared three ways: **Vault** holds the key records and Transit metadata, **cluster storage** holds the persisted KMS configuration, and **each node's process memory** holds caches and the live backend instance. Nodes on different builds agree on the first, may disagree on the third, and — for configuration — can disagree for as long as the operator leaves them running, because the reload broadcast that converges configuration is one of the things an older build rejects.

This section states only what is true of the current implementation. It is written for the KV2 and Transit backends; the Local backend is unsupported for multi-node deployments regardless of version (see the [deployment support matrix](#deployment-support-matrix)).

### Persisted formats are backward compatible in both directions

Nothing in this list requires a coordinated format cutover. The compatibility is deliberate and is covered by decode tests.

- **DEK envelopes.** `DataKeyEnvelope::master_key_version` is optional and omitted when absent, so envelopes written by non-rotating backends stay byte-identical to the historical seven-field JSON shape. An upgraded node reading a pre-versioning envelope resolves `None` to the key's recorded baseline version, or — for a key that was never rotated, and so has no baseline — to the current version, which is exactly the pre-versioning behavior.
- **KV2 key records.** `baseline_version` is read with a serde default, so records written by older builds deserialize unchanged, and `None` correctly means "never rotated".
- **Transit metadata records.** Metadata persisted in KV v2 by either build decodes on the other.

The one-way hazard is the rotation constraint above: an older binary reading a *new* envelope silently ignores the version field and decrypts with the current material.

### Guarantees that hold only once every node is upgraded

These are properties of the upgraded code, so a single node left behind removes them for the whole cluster.

- **Check-and-set lifecycle writes.** Upgraded builds write every KV2 lifecycle mutation — create, enable, disable, tag metadata, schedule deletion, cancel deletion — as a versioned read followed by a check-and-set write, retrying on conflict by re-reading and re-validating the state gate (rustfs/rustfs#5518). Transit metadata writes got the same treatment (rustfs/rustfs#5520). Builds older than those write blind. A blind write from an old node can overwrite a check-and-set commit from an upgraded node without any conflict being reported, which is precisely the lost update the change was made to eliminate.
- **`baseline_version` survives a write-back.** The KV2 key record does not deny unknown fields, so an old build reads a new record without error — and drops `baseline_version` when it writes that record back for any reason. A key that loses its baseline resolves pre-versioning envelopes to the current version again, which after a rotation means the wrong master key material. Any lifecycle operation issued to an old node is enough to trigger this.
- **Version-record awareness.** Rotation stores each historical version under `{prefix}/{key_id}/versions/{N}` as a create-only record (check-and-set of 0), so two nodes racing the same version number produce exactly one creator; the loser adopts the persisted, never-current material or fails without touching the current pointer. Old builds have no concept of that sub-path: they never read or write it, and their key listing reports the KV2 directory entry (`my-key/`) as though it were a key, because the directory filter only exists in upgraded builds.

### Windows in which nodes can legitimately disagree

Even with every node on the same build, some state is process-local. These windows are bounded by design, except the last one.

| What can diverge | Bound | Mechanism |
| --- | --- | --- |
| Transit key lifecycle state used by the `encrypt` and `generate_data_key` gates | ≤ 300 s (`METADATA_CACHE_TTL`) | Each node caches Transit metadata in process, TTL- and capacity-bounded, with targeted invalidation when a data-path call reports the key is gone server-side. A disable or schedule-deletion performed on one node is enforced on the others within one TTL at the latest, sooner if they hit that signal. |
| `describe_key` output | One metadata cache TTL: 300 s by default, otherwise whatever `cache_ttl_seconds` was configured with, clamped to 24 h | The manager-level key metadata cache, built from the configured cache settings. This is a reporting cache; the KV2 state gates do not read it. |
| KV2 key lifecycle state | None | The KV2 backend re-reads the key record from Vault for every lifecycle and data-key operation, so a committed disable is effective on every upgraded node immediately. |
| Active KMS configuration | One best-effort reload broadcast; unbounded for any peer that did not apply it | See below. |

Builds older than rustfs/rustfs#5520 held the Transit metadata cache with no TTL and no capacity bound. On such a node the divergence window is not 300 seconds but "until the process restarts": it can keep encrypting under a key that another node disabled, indefinitely.

The `describe_key` bound is the only one on that list an operator sets, so compute it rather than assuming the default: the window is the `cache_ttl_seconds` the KMS configure request was given, 300 s when it was omitted, clamped down to 24 h at use if it is larger (clamped rather than rejected, so an oversized setting still starts). Zero is refused outright while caching is enabled. `kms service-status` and the KMS configuration endpoint report the effective, post-clamp value, so the number the admin API shows is the number the cache honours. Note that this is the Transit row's neighbour and not its equal: `METADATA_CACHE_TTL` above is a separate, deliberately non-tunable 300 s, because that cache does gate cryptographic operations.

One upgrade caveat: builds older than rustfs/rustfs#5569 ignored `cache_ttl_seconds` and ran a hardcoded 300 s, while their configure converters persisted 3600 s as the default value. A cluster configured through the admin API before that fix therefore widens its `describe_key` staleness window from an effective 300 s to the 3600 s already stored in `config/kms_config.json`, with no configuration change of its own. Read the reported value back after upgrading instead of assuming it stayed at 300 s. No cryptographic or authorization path widens with it — encrypt, decrypt and data-key generation go straight to the backend and never read this cache.

### Configuration changes converge through a best-effort peer reload

`POST /rustfs/admin/v3/kms/configure` and `POST /rustfs/admin/v3/kms/reconfigure` persist the new configuration to cluster storage at `config/kms_config.json`, switch the KMS service **on the node that handled the request**, and then broadcast a reload signal to every peer. A peer that accepts the signal re-reads the persisted configuration and reconfigures itself, so a runtime change normally reaches the whole cluster without any restart. A peer already running that exact configuration treats the signal as a no-op.

Convergence is best effort by contract, and the request never fails on account of a peer: the local node has already switched, and KMS configuration has no quorum or authoritative holder to roll back to. What that leaves:

- The broadcast is sent **once**, with no background retry. A peer that is unreachable, that rejects the signal because its build predates the KMS subsystem, or whose reload itself fails keeps serving its previous configuration until a later `reconfigure` reaches it, or until it restarts and loads the persisted configuration during startup. For those peers the split window is still unbounded.
- The admin response reports success either way, but its message names every peer that did not converge, and the server logs one `kms_peer_config_reload_failed` warning per peer. Read the message: an operation that reports success can still have left the cluster split.
- For as long as a split lasts, both configurations are live. If the change switched backends, or changed the Vault mount or key prefix, different nodes write new key material to different places, and a key created through one node is invisible to the others.

`GET /rustfs/admin/v3/kms/service-status` makes the split observable from a single request: it returns a `cluster_config` object holding one redacted configuration fingerprint per node plus a `consistent` flag. `consistent` is true only when every node answered with the same fingerprint — an unreachable peer, a peer whose build reports no fingerprint, and a node with no configuration at all each read as divergent rather than as agreement. Secrets are substituted out before a configuration is fingerprinted, so two nodes on the same backend holding different credentials still fingerprint alike; the field detects a configuration split, not a credential split.

Treat a `configure` or `reconfigure` whose response names unconverged peers as an unfinished cluster-wide operation: re-issue it once those peers are reachable, or restart them.

### Recommended rolling upgrade order

Follow the node-at-a-time procedure in the [multi-node restart runbook](rolling-restart.md); this adds the KMS-specific sequencing around it.

1. **Freeze KMS administrative traffic** for the duration: no key creation, enable, disable, tagging, schedule-deletion, cancel-deletion, rotation, or reconfiguration. Object read and write traffic continues normally.
2. **Upgrade one node at a time**, waiting for each to report ready before starting the next.
3. **Verify no node is left behind** before unfreezing. A single old node is enough to reintroduce blind writes and to strip `baseline_version` on its next lifecycle write.
4. **Resume administrative traffic.**
5. **Only then perform the first rotation of any key.** Once the whole cluster understands `master_key_version`, rotation is safe; before that it is not.
6. **If the KMS configuration was changed at any point**, confirm `cluster_config.consistent` is true in the `service-status` response, and re-issue the change — or restart the node — for every peer still reporting a different fingerprint. A peer whose build predates the reload signal never converges on its own.

### Do not do these during a mixed-version window

- **Rotate any key.** This is the hard constraint stated above; a rotation is unrecoverable for objects an old node must read.
- **Issue any KV2 lifecycle write to an old node.** Its blind write can clobber a concurrent check-and-set commit and will drop `baseline_version` from the record.
- **Create the same key ID from two nodes.** The create path is create-only on upgraded builds, but an old node's blind write does not honor that: the later writer's material wins and every DEK already wrapped with the earlier material becomes permanently unwrappable.
- **Assume a disable or schedule-deletion took effect cluster-wide.** Old Transit nodes cache lifecycle state without expiry; confirm per node, or restart the old nodes, before treating a key as no longer in use.
- **Reconfigure the KMS backend and consider it done.** The reload broadcast is exactly what an old build rejects, so during a mixed-version window the change reaches only the node that served it and the already-upgraded peers. Check the response message and `cluster_config.consistent` before assuming otherwise.
- **Delete or prune version records** under `{prefix}/{key_id}/versions/*` for any reason. This is never safe, mixed-version or not; see [Retention and destruction preconditions](#retention-and-destruction-preconditions).

## Choosing between Vault KV2 and Vault Transit

Use **Vault Transit** (`VaultTransit`) when key material must be cryptographically isolated from anyone holding storage-level read access: Transit keeps key-encryption keys inside Vault and only ever returns ciphertext, and supports server-side key versioning/rotation.

Use **Vault KV2** only when you accept that the Vault ACL on the key path *is* the confidentiality boundary and you want the operational simplicity of a single KV mount.

## AWS KMS: deviations from the shared backend contract

Select it with `RUSTFS_KMS_BACKEND=aws`. Credentials and region resolution are delegated entirely to the standard `aws-config` provider chain (environment, shared profile, container/IMDS role), so RustFS never stores, persists, or redacts AWS credential material of its own. Only two non-credential settings are read: `RUSTFS_KMS_AWS_REGION` and `RUSTFS_KMS_AWS_ENDPOINT_URL`. A plaintext (`http://`) endpoint override would expose every KMS request including plaintext data keys, so it is refused unless the development opt-in is set.

AWS owns key state, backing-key rotation, and the deletion window, and this backend mirrors none of it locally. That makes four behaviours differ from every RustFS-managed backend. Verify each against your operational assumptions before switching:

| Behaviour | RustFS-managed backends | AWS KMS backend |
| --- | --- | --- |
| Decryption with a `Disabled` or `PendingDeletion` key | Kept working, so disabling a key never breaks reads of objects already encrypted under it | **Refused by AWS.** Objects encrypted under a key that is later disabled become unreadable until it is re-enabled |
| Key deletion | Physical deletion available | **No physical delete.** `ScheduleKeyDeletion` is the only removal path; AWS destroys the material when the 7-30 day window elapses. RustFS never destroys AWS-held material, and `force_immediate` is refused |
| Cancelling a scheduled deletion | Key returns to `Enabled` | Key is left **`Disabled`**; enable it explicitly to make it usable again |
| Creating a key under a caller-chosen name | The requested name becomes the key id | **Refused.** AWS assigns identifiers and this backend does not manage aliases, so a named create would produce a key unreachable by that name |

Two consequences follow from that last row: **SSE-S3 key auto-creation and the synthetic KMS probe are unavailable on this backend**, because both address a key by a name they choose. Pre-create keys in AWS and reference them by AWS key id or ARN.

Key versions are opaque. AWS addresses backing keys internally and picks the right one to decrypt with, so RustFS reports `key_version` as 1 and cannot enumerate versions. Rotation uses `RotateKeyOnDemand`, which retains prior backing keys for decryption; AWS's separate automatic yearly rotation is neither enabled nor reported on by RustFS.

The KMS admin API accepts the AWS backend as `"backend_type": "AWS"` (aliases `aws`, `aws-kms`, `aws_kms`, `AwsKms`) on `/v3/kms/configure` and `/v3/kms/reconfigure`. The body carries `region` (**required**), and optionally `endpoint_url`, `default_key_id`, and the shared timeout/retry/cache settings. It accepts no credential fields at all — unknown fields are rejected — because every node resolves credentials through its own provider chain.

`region` is mandatory on this path even though `RUSTFS_KMS_AWS_REGION` is optional at startup: the admin configuration is persisted once and replayed on every node, so a request that left the region to each node's ambient chain would let nodes address different regions, and therefore different keys, while reporting an identical configuration. `default_key_id` must be an AWS key id or ARN that already exists — this backend never creates keys by name.

## Local backend durability and deployment support matrix

The Local backend stores one JSON record per key (`<key_id>.key`) plus an Argon2id salt file (`.master-key.salt`) inside the configured `key_dir`. This section documents which deployments that layout supports and how the backend recovers from a crash or power loss. For where the key material lives and who can read it, see the [backend comparison](#backend-comparison) above.

### Positioning

The facts today:

- `Local` is the current default backend (`kms_backend` defaults to `local`).
- The RustFS Kubernetes operator places the key directory on a PersistentVolumeClaim, so the keys survive pod rescheduling.
- The in-code documentation labels the backend "for development and testing only", and configuration validation enforces stricter rules outside explicit development mode: a master key is required and `key_dir` must not live under the process temp directory.
- Production multi-node deployments should use the Vault Transit backend.

The backend's final support level is positioning under review (internal tracking); this section describes what the implementation guarantees, not a commitment to a support tier.

### Deployment support matrix

| Deployment | Supported | Notes |
| --- | --- | --- |
| Local filesystem (ext4, XFS, APFS, ...) | Yes | The commit protocol relies on POSIX `rename`/`hard_link` atomicity and `fsync` durability, which local filesystems provide |
| Kubernetes PVC | Yes | Only when the PersistentVolume is backed by a local or block filesystem; this is how the RustFS operator provisions the key directory |
| NFS or other shared/network filesystems | No | Network filesystems do not reliably provide the atomicity and fsync semantics the commit protocol depends on; an NFS-backed PersistentVolume is this case, not the PVC case above |
| Multiple RustFS processes sharing one `key_dir` | No | Concurrent key **creation** is linearized (`hard_link` refuses to clobber an existing key), but every other write — status updates, deletion, cancellation — is a read-modify-write with no cross-process lock, so concurrent writers can silently lose updates |

Within a single process, per-key write locks serialize read-modify-write updates, so concurrent API calls against one RustFS instance are safe.

### Crash recovery behavior

Every mutation of the key directory uses a durable commit protocol:

1. A temp file (`<name>.tmp-<uuid>`) is created exclusively in `key_dir`.
2. The content is written and fsynced (`sync_all`).
3. The file is published atomically: `rename` to replace an existing file, `hard_link` to create a new one without clobbering.
4. The parent directory is fsynced so the new directory entry is durable.

Deletion mirrors the tail of the protocol (`remove_file` followed by a parent directory fsync), so a deleted key cannot resurface after power loss. A crash at any step leaves either the complete old state or the complete new state, plus at most an unpublished temp file.

On startup the backend then:

- **Removes orphaned commit temp files.** The matcher is strict (`<prefix>.tmp-<uuid>`, never anything ending in `.key`), so published key files — including a key the user named to look like a temp file — are never touched. Publishing is atomic, so a matching leftover can only be an unpublished remnant of an interrupted commit.
- **Validates every published `.key` file.** A record that fails to decode fails startup rather than being silently skipped.
- **Guards the salt file.** If `.master-key.salt` is missing but the directory contains keys marked `encrypted-master-key`, initialization fails closed with a configuration error naming the salt path. A regenerated salt derives a different master key and can never decrypt those keys, so the correct recovery is to **restore the salt file (or the whole directory) from backup**, never to let a fresh salt be generated. An empty directory, or a legacy directory predating the salt file, still initializes normally.

### Backing up the key directory

Back up `key_dir` as a whole, including the hidden `.master-key.salt` file. A key file on its own is not restorable: decrypting it requires the master key derived from the configured `master_key` **and** the persisted salt. Restoring a partial directory — key files without the salt, or the salt without the key files — leaves the backend unable to decrypt, and the salt guard above will (correctly) refuse to start with encrypted keys and no salt. Losing the salt file with no backup means every key encrypted under it is unrecoverable.
