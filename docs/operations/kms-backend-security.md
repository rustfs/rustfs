# KMS backend security properties

RustFS ships several KMS backends. They differ not only in deployment effort but in **where master key material lives and who can read it**. Pick a backend based on the confidentiality boundary you need, not on the name alone.

For how the Vault backends authenticate (static token, AppRole, Kubernetes, Vault Agent token file) and how credential refresh and the fail-closed window behave, see the [Vault KMS authentication runbook](vault-kms-authentication.md). For what may be claimed about the cryptographic implementations themselves, see [Cryptographic compliance positioning](kms-cryptographic-compliance.md). For which RustFS identities may manage or use a given key, see [Per-key KMS authorization](kms-per-key-authorization.md). If you are migrating from MinIO, read [Migrating from MinIO: encrypted objects do not carry over](#migrating-from-minio-encrypted-objects-do-not-carry-over) first.

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

The read does fail closed — ciphertext is never served as plaintext. MinIO's internal encryption headers mark the object as encrypted (`crates/utils/src/http/header_compat.rs:50-67`), so the read path demands encryption material and refuses when none resolves (`crates/ecstore/src/object_api/readers.rs:559-568`). Two properties still make the problem easy to discover late:

- **The error does not say what happened.** It surfaces as a 500 `InternalError`, which reads as a RustFS fault rather than "another implementation encrypted this object".
- **Surrounding metadata migrates fine.** The object's `xl.meta` parses, so encrypted objects list and HEAD normally and report plausible sizes. The failure appears only when something reads the payload.

Read a sample of encrypted objects, not just their listings, before decommissioning the MinIO deployment.

Current options for a migration whose source contains encrypted objects:

- Decrypt on the MinIO side first, migrate plaintext, then let RustFS re-encrypt with its own KMS.
- Copy through the S3 API rather than moving drives — MinIO decrypts on read, and RustFS encrypts on write. This re-encrypts rather than preserving ciphertext and costs a full data transfer.
- Leave encrypted objects on MinIO and migrate only unencrypted data.

Inventory the source before choosing: bucket default-encryption settings mean objects can be encrypted without any client having sent SSE headers.

The same limitation applies in reverse — objects RustFS encrypts are not readable by MinIO. For the code-level breakdown of which seams block each SSE mode, see [MinIO file-format interoperability, Part C](../architecture/minio-file-format-compat.md#part-c--server-side-encryption-sse).

The migration warning is not a wire-protocol promise: the **AWS KMS wire protocol** and **MinIO KES wire protocol** are explicit non-targets for this document. The AWS backend uses the AWS SDK client path (`crates/kms/src/backends/aws.rs:830`), and KES remains outside the MinIO on-disk interop scope. Track those ecosystem evaluations and the MinIO/RustFS SSE compatibility matrix in the [#1562 Production Ready exit gate](https://github.com/rustfs/backlog/issues/1562); #1638 alone does not satisfy that gate.

## Vault KV2: what the backend does and does not do

The Vault KV2 backend uses Vault purely as a **secure storage** service:

- Master key material is generated by RustFS and written to KV v2 as a Base64-encoded value (`encrypted_key_material` is an encoding, not a ciphertext).
- The backend never calls the Vault Transit engine. The `mount_path` configuration field and the `RUSTFS_KMS_VAULT_MOUNT_PATH` environment variable are deprecated leftovers: they are accepted for compatibility and ignored.
- Data-encryption keys (DEKs) handed to the object-encryption path are still wrapped with AES-256-GCM under the master key; the statement above concerns the master key's storage in Vault, not the DEK envelope.
- No runtime interface reports this boundary. `GET /rustfs/admin/v3/kms/status` names the active backend (`backend_type: vault-kv2`) and returns a `capabilities` matrix, but that matrix enumerates only the operations the backend supports — nothing in it describes where master key material lives or who can read it. Determining which confidentiality boundary is in force means reading `backend_type` and applying the comparison table above; this document is the only statement of the boundary an operator can consult.
- The `at_rest_protection: storage-only` field carried by a KMS backup manifest is a different thing: it declares the protection state of key material inside a backup bundle, not a property the running backend reports about itself.
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

Rotation support differs per backend. Local and Static advertise no `rotate` capability — `capabilities.rotate` is false in the `kms/status` response — and reject rotation with `UnsupportedCapability`; their single key material is never overwritten. Vault Transit delegates rotation to the Transit engine's own key versioning (ciphertext is version-prefixed, e.g. `vault:v1:...`). Vault KV2 rotates by retaining every historical version, as described below. Rotation is reachable through the admin API as `POST /rustfs/admin/v3/kms/keys/rotate`, which the route policy classifies as high risk and gates behind `kms:RotateKey`; it is not exposed through the S3 surface. The upgrade ordering constraint below therefore applies to an operator action, not only to a call from inside the process.

### Rotation drivers and scheduling, per backend

The rotate endpoint is one API over three very different mechanisms, and which component actually performs the rotation decides how periodic rotation must be scheduled — on two backends it cannot be scheduled at all.

| Backend | Can rotate | Who performs the rotation | How to schedule periodic rotation |
| --- | --- | --- | --- |
| Local | No | Nobody — the backend advertises no `rotate` capability and the rotate endpoint is refused with `UnsupportedCapability` | Cannot be scheduled. Migrating to a rotating backend is the only path to rotation |
| Static | No | Nobody — same refusal as Local; the material is supplied out-of-band and read-only | Cannot be scheduled. Migrate to a rotating backend |
| Vault KV2 | Yes | **RustFS** owns the whole rotation protocol: freeze the outgoing material as an immutable version record, persist the new version's material, then move the current pointer with a check-and-set write | An **external scheduler** (cron, Kubernetes CronJob, your automation platform) calling `POST /rustfs/admin/v3/kms/keys/rotate`. RustFS deliberately ships no built-in rotation timer — see below |
| Vault Transit | Yes | **Vault's Transit engine** — RustFS only forwards the call to Transit's rotate endpoint and records the version bump in its own metadata | Vault's native `auto_rotate_period` on the Transit key. Do **not** additionally point an external scheduler at the RustFS rotate endpoint — see below |
| AWS KMS | Yes | **AWS** — the RustFS rotate endpoint maps to `RotateKeyOnDemand` | AWS's native automatic rotation, configured on the AWS side. Do **not** drive periodic rotation through the RustFS endpoint — see below |

**Local and Static: the wrap ceiling is unmitigable.** These backends wrap every DEK with AES-256-GCM under their single master key using a random 96-bit nonce, and NIST SP 800-38D caps AES-GCM at 2^32 invocations per key when nonces are chosen at random. Each encrypted object write wraps a DEK, so the invocation count tracks the number of encrypted-object writes over the deployment's lifetime. On a rotating backend that count restarts whenever new master key material takes over; on Local and Static it can never restart, because there is no rotation to restart it. The only mitigation is migrating to a backend that rotates. The same 2^32 bound applies to the KV2 backend's wrapping — RustFS wraps DEKs locally there too — but there each rotation mints fresh master key material and resets the count, which is one more reason to actually schedule KV2 rotation rather than merely support it.

**Vault KV2: bring your own scheduler, deliberately.** RustFS performs the rotation but does not decide when: there is no built-in rotation worker, by design rather than omission. A timer inside the server cannot verify the [cluster-upgrade precondition](#upgrade-before-first-rotation-hard-constraint) before firing, and rotation is not idempotent — without leader election, N nodes running the same schedule would perform N rotations per period, advancing the key version N times. Run exactly one external scheduler, point it at the admin rotate endpoint with credentials scoped to `kms:RotateKey`, and use the [rotation readiness fields](#rotation-readiness-reported-never-acted-on) plus the `KmsKeyRotationOverdue` alert in the [KMS observability runbook](kms-observability-runbook.md#kmskeyrotationoverdue) to verify the schedule is actually keeping up.

**Vault Transit: exactly one owner of the version cadence.** Configure `auto_rotate_period` on the Transit key and let Vault own the schedule. Layering an external scheduler that calls the RustFS rotate endpoint on top of `auto_rotate_period` creates two competing owners of the key's version cadence, and the effective rotation period stops being the one either owner was configured with. The data path is indifferent to who rotates — Transit ciphertext self-describes the version that wrapped it, so envelopes never pin a version RustFS tracked — but the key version RustFS reports only advances when rotation goes through RustFS, so on an auto-rotating key treat the reported version as a floor, not the truth.

**AWS KMS: native automatic rotation for cadence, `RotateKeyOnDemand` for incidents.** The RustFS rotate endpoint maps to AWS `RotateKeyOnDemand`, and AWS enforces a lifetime limit on the number of on-demand rotations a key may receive (see the AWS KMS documentation) — a periodic scheduler driving the RustFS endpoint will exhaust that quota and then fail forever. Configure AWS's automatic rotation for periodic cadence and keep the RustFS endpoint for what on-demand rotation is for: incident response and one-off rotations. Note that RustFS neither enables nor observes AWS automatic rotation, and it records no rotation timestamp for AWS keys, so the readiness fields and the rotation-age gauge measure key age on this backend — verify the actual cadence in AWS, not through RustFS.

**Pre-rotation checklist** (before the first rotation of any key, and before enabling any schedule):

1. Every node in the cluster runs a build that understands the `master_key_version` envelope field — the [hard upgrade-ordering constraint](#upgrade-before-first-rotation-hard-constraint) below. A timer cannot check this; you must.
2. No rolling upgrade is in progress — see [Do not do these during a mixed-version window](#do-not-do-these-during-a-mixed-version-window).
3. The [retention and destruction preconditions](#retention-and-destruction-preconditions) are understood: every version record a stored DEK envelope references must remain readable forever, and no retention tooling prunes the version subtree.
4. For KV2, exactly one scheduler exists, so no two callers race the same rotation period.
5. `RUSTFS_KMS_ROTATION_MAX_AGE_SECS` is set to the rotation period your policy requires, so the per-key `rotation_due` verdict and the rotation-age alert verify the schedule instead of assuming it.

### Rotation readiness: reported, never acted on

RustFS does not rotate keys on a schedule. There is no built-in rotation worker, deliberately: rotation is a policy decision with a per-backend cost and a hard upgrade-ordering constraint (see below), and a server that rotated on its own would make that decision on an operator's behalf at a moment it did not choose. What the server does instead is tell you which keys have outlived a period you configure.

Set `RUSTFS_KMS_ROTATION_MAX_AGE_SECS` to that period in whole seconds. Unset — the default — leaves the verdict unreported rather than assuming a policy: how often keys must be rotated is a compliance decision, and a built-in default would report keys as overdue against a rule nobody wrote. An unparsable value is treated the same way, with a warning, instead of silently falling back to a number the operator did not choose. Values below one hour are raised to one hour, because a threshold of seconds reports every key as overdue moments after it was rotated and teaches operators to ignore the signal.

A second, independent threshold covers the cryptographic bound rather than the policy one. `RUSTFS_KMS_ROTATION_MAX_WRAPS` is the number of data keys one key's material may wrap before the verdict reports `rotation_due` with reason `wraps`. It follows the same discipline — unset or unparsable leaves the verdict unreported, and values below one million are raised to one million because wraps are accounted in reserved blocks of that size, so a smaller threshold would trip on the first reservation. Only backends where RustFS wraps locally and can rotate report a count (Vault KV2 today); Transit and AWS wrap externally and report none, so the wrap half stays silent there rather than guessing. When both thresholds are crossed the reported reason is `wraps`: the AES-GCM random-nonce ceiling is not negotiable, while the age period is a policy an operator chose.

`GET /rustfs/admin/v3/kms/keys` then carries two additional fields per key:

- `rotation_due` — whether the key has outlived the configured period.
- `rotation_due_reason` — `age` when the key was rotated but longer ago than the period, `never_rotated` when it has never been rotated and has been in use longer than the period, and `unsupported` when the backend cannot rotate at all. Absent when there is no verdict.

The verdict is advisory in the strongest sense: nothing consults it before encrypting or decrypting, a key reported as due keeps serving traffic unchanged, and it has no effect on readiness or liveness. It is computed in one place, from the backend's declared rotation capability plus the key's own timestamps, so no two backends can disagree about what "overdue" means — and a backend that cannot rotate is reported as `unsupported` rather than being told to do something it cannot.

`GET /rustfs/admin/v3/kms/keys/{key_id}` does **not** carry these fields. Its response type records a creation date but no rotation timestamp, so a verdict computed there could not tell a key rotated last week from one never rotated at all, and reporting `never_rotated` for a key that was in fact rotated would be worse than reporting nothing. Read the verdict from the listing.

Driving the rotation itself remains external: call `POST /rustfs/admin/v3/kms/keys/rotate` from your own scheduler, having first satisfied the upgrade-ordering constraint below — and only on the backend where that is the right scheduling model; see [Rotation drivers and scheduling, per backend](#rotation-drivers-and-scheduling-per-backend).

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

- **DEK envelopes.** `DataKeyEnvelope::master_key_version` is optional and omitted when absent, so envelopes written by non-rotating backends stay byte-identical to the historical seven-field JSON shape. An upgraded node reading a pre-versioning envelope resolves `None` to the key's recorded baseline version, or — for a key that was never rotated, and so has no baseline — to the current version, which is exactly the pre-versioning behavior. Unknown values are skipped while parsing; a bounded field-name sample is emitted at a progressively rate-limited `warn` level, and `rustfs_kms_persisted_unknown_fields_total{record_kind="data-key-envelope"}` counts every observed field.
- **Local key records.** Each `<key_id>.key` record carries `format_version: 1`; records written before that field existed default to version 1 when read, and the pre-version reader ignores the added v1 marker. A reader accepts a record whose version is at most the version it understands, and rejects a newer version with `UnsupportedFormatVersion` before it attempts to decrypt key material. Unknown fields remain accepted for rollback compatibility. Their values are ignored while parsing; a bounded field-name sample is emitted at a progressively rate-limited `warn` level, and `rustfs_kms_persisted_unknown_fields_total{record_kind="local-key-record"}` counts every observed field. Once a future version greater than 1 has written a key record, do not roll back to a build that predates this marker: such a build cannot reject that future version before interpreting the rest of the record.
- **KV2 key records.** `baseline_version` is read with a serde default, so records written by older builds deserialize unchanged, and `None` correctly means "never rotated".
- **Transit metadata records.** Metadata persisted in KV v2 by either build decodes on the other.

The one-way hazard is the rotation constraint above: an older binary reading a *new* envelope silently ignores the version field and decrypts with the current material.

### Guarantees that hold only once every node is upgraded

These are properties of the upgraded code, so a single node left behind removes them for the whole cluster.

- **Check-and-set lifecycle writes.** Upgraded builds write every KV2 lifecycle mutation — create, enable, disable, tag metadata, schedule deletion, cancel deletion — as a versioned read followed by a check-and-set write, retrying on conflict by re-reading and re-validating the state gate (rustfs/rustfs#5518). Transit metadata writes got the same treatment (rustfs/rustfs#5520). Builds older than those write blind. A blind write from an old node can overwrite a check-and-set commit from an upgraded node without any conflict being reported, which is precisely the lost update the change was made to eliminate.
- **`baseline_version` survives a write-back.** The KV2 key record does not deny unknown fields, so an old build reads a new record without error — and drops `baseline_version` when it writes that record back for any reason. A key that loses its baseline resolves pre-versioning envelopes to the current version again, which after a rotation means the wrong master key material. Any lifecycle operation issued to an old node is enough to trigger this.
- **`wrap_budget_reserved` keeps overestimating.** The KV2 key record's approximate wrap counter (`wrap_budget_reserved`, behind the `rustfs_kms_max_key_wrap_operations` gauge) is dropped the same way when an old build rewrites the record, regressing the count toward zero — the one way this deliberately overestimate-only counter can understate the wraps actually performed. Nothing breaks: the counter is advisory, and the next block reservation from an upgraded node re-establishes a floor. Just do not trust a *low* gauge reading taken during or shortly after a mixed-version window.
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

The AWS backend is intentionally exempt from `backends::contract_tests::assert_state_machine_contract`. That shared driver assumes that disabled and pending-deletion keys still decrypt, that cancelling deletion returns a key to `Enabled`, and that creation accepts a caller-assigned key name. AWS rejects decryption for the first case, leaves a cancelled key `Disabled`, and assigns key identifiers itself, so running the driver would encode the wrong behavior. The exemption is pinned by the offline `aws_backend_shared_contract_exemption_is_pinned` test in `crates/kms/src/backends/aws.rs`; if AWS changes any of these semantics, integrate the backend into the shared driver and remove this exemption rather than weakening the shared assertions.

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
- **Guards the salt file.** If `.master-key.salt` is missing but the directory contains keys marked `encrypted-master-key`, initialization fails closed with a configuration error naming the salt path. A regenerated salt derives a different master key and can never decrypt those keys, so the correct recovery is to **restore the salt file (or the whole directory) from backup**, never to let a fresh salt be generated. The guard is equally strict about a record it cannot read or cannot interpret — for example one written by a newer RustFS that names an at-rest protection this build does not implement: such a directory's protection state is unknown, so no replacement salt is generated for it either. Recovery is to restore the salt file, run a build that understands the record, or move the unrecognized file out of `key_dir` after confirming it is not needed. An empty directory, or a legacy directory predating the salt file, still initializes normally.

### Filesystem permissions and the boundaries the protocol assumes

The key directory is held at `0o700` and every file published into it — key records, the salt, and the files a restore stages and cuts over — is written owner-only. The requested mode is applied and re-read on the open file *before* the content becomes durable, so the process umask cannot widen it, and an unspecified `file_permissions` resolves to owner-only inside the commit protocol rather than at each call site, so no write path can leave it to the umask.

A directory wider than `0o700` is **narrowed on every start**, and the result is re-read to confirm it took effect. It is not refused: the mode is far more often the platform's than the operator's — kubelet creates an `emptyDir` `0o777`, several PVC provisioners `mkdir -m 0777`, a `--tmpfs` mount lands at `1777` — and refusing would turn each of those into a server that will not start while leaving the exposure in place on the way out. Narrowing removes it. Only a directory this process cannot secure is fatal, because at that point the mode is both dangerous and outside our control. Narrowing is logged with the previous mode whenever it was reachable beyond the owner.

Publishing never writes through a symlink. `hard_link` refuses any destination that already exists — including a dangling symlink — so a create cannot adopt an inode it did not write, and `rename` replaces the link itself rather than the file it points at. Startup removes anything wearing a commit-temp name that is not a directory, symlinks included; the protocol only ever creates temps with `create_new`, so such an entry is either its own leftover or something planted.

Two boundaries in this area are **not** verified, and deployments should not assume them:

- **Cross-device operations.** The temp file is always created in the destination's own directory, so `rename` and `hard_link` never cross a filesystem and `EXDEV` is unreachable by construction. That invariant is tested; a real cross-device attempt is not, because it needs a second filesystem. The restore staging directory is always `.restore-staging` inside `key_dir`, so this holds by construction unless that subdirectory is separately bind-mounted onto another filesystem — do not do that.
- **The key directory being replaced mid-commit.** Every path is re-resolved from the directory name rather than held as a directory file descriptor. If `key_dir` is swapped between the `rename` and the parent `fsync`, the fsync lands on the replacement and the new directory entry is never made durable, while the call still reports success. Reaching this requires write access to the key directory's **parent**, which nothing here checks — the mode enforcement above covers `key_dir` itself and says nothing about what encloses it. Keep the parent owner-writable too. Closing this properly means moving the protocol to `renameat`/`linkat` against a held directory descriptor; it is a real gap, recorded as one.

### Backing up the key directory

Back up `key_dir` as a whole, including the hidden `.master-key.salt` file. A key file on its own is not restorable: decrypting it requires the master key derived from the configured `master_key` **and** the persisted salt. Restoring a partial directory — key files without the salt, or the salt without the key files — leaves the backend unable to decrypt, and the salt guard above will (correctly) refuse to start with encrypted keys and no salt. Losing the salt file with no backup means every key encrypted under it is unrecoverable.
