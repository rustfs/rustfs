# Object integrity inventory, audit, and migration

**Use this when:** assessing legacy object protection, checking a bounded manifest against an existing SHA256, or creating protected copies without overwriting the source.

## Protection and activation

Legacy shard-local checksums can detect accidental corruption, but do not independently bind a complete valid donor shard to its original object. A successful legacy read or Heal is not proof of original content. Inventory reports the stored protection declaration; it does not verify payload bytes. `independent_commitment` requires consistent object and part declarations. `unknown` means metadata could not be observed reliably.

Both `RUSTFS_SHARD_INTEGRITY_WRITE` and `RUSTFS_SHARD_INTEGRITY_FLEET_CONFIRMED` remain false by default. Enabling both permits protected new writes on that process. The second flag is an operator attestation, not a peer capability probe or a storage fencing mechanism. Before enabling, qualify all readers, writers, repair processes, rollback binaries, and replacement nodes against the protected format. Prevent old processes from accessing those drives. Measure full GET, Range GET, PUT, degraded read, and repair latency and throughput on the deployment's erasure layout. The readiness endpoint reports these requirements as unverified; it does not make the deployment safe automatically.

Disabling the write flags stops protection for new writes without an inherited mode; it does not remove existing commitments or make an old binary safe. Keep compatible readers for already migrated objects. Normal S3 requests and existing legacy reads do not depend on the integrity job worker being available.

## Administrative API

Use an authenticated, signed admin client. Paths use `/rustfs/admin/v3/integrity` and normal RustFS admin request authentication and body conventions.

| Method and suffix | Permission | Behavior |
|---|---|---|
| `GET /readiness` | `admin:ServerInfo` | Local support, local flags, operator attestation, and unverified fleet requirements |
| `GET /{bucket}/inventory` | `admin:InspectData` | One page of version observations, no payload scan |
| `POST /{bucket}/jobs` | `admin:StartBatchJob` | Persist a paused manifest and return its ID |
| `GET /{bucket}/jobs/{job_id}` | `admin:DescribeBatchJob` | Durable status and per-item outcomes |
| `POST /{bucket}/jobs/{job_id}/control` | `admin:StartBatchJob` | Explicit `resume`, `pause`, or `cancel` |

Inventory accepts `prefix`, `key-marker`, `version-marker`, and `limit` (1–100). Continue with both returned markers only after storing the entire page. Each inventory item returns a reusable string version selector, including `"null"` for the null version. Pages are observations, not a bucket snapshot: concurrent creates and deletes can affect enumeration. A recreated bucket has a different incarnation; do not combine observations across incarnations. Inventory and audit suppress their own read-repair submissions and bypass the body cache for content verification; they do not stop unrelated normal reads or background maintenance.

Example job body:

```json
{
  "mode": "audit",
  "items": [
    {
      "key": "archive/object.bin",
      "version_id": null,
      "expected_sha256": "ungWv48Bz+pBQUDeXa4iI7ADYaOWF3qctBD/YfIAFa0="
    }
  ],
  "bytes_per_second": 10485760,
  "max_object_bytes": 1073741824
}
```

The example digest is illustrative; supply the expected digest of your own object from an independent trusted record. `version_id` omitted or JSON null selects the current version; the string `"null"` selects the null version. A non-null version must be a UUID. The job records and rechecks the observed source identity. For `mode: "migrate"`, each item also requires a distinct `target_key` in the same bucket. No target may equal any source key in the manifest.

`expected_sha256` is canonical Base64 of 32 bytes, not hex or an ETag. When omitted, only a canonical stored single-part SHA256 is eligible. A stored checksum verifies consistency with stored metadata; it does not establish an external historical provenance. The service never derives a new expected digest from an unchecked read and calls that historical evidence.

Start the persisted job with:

```json
{"operation":"resume"}
```

Use the same control endpoint with `pause` or `cancel`. Poll status by ID and retain the ID externally; there is no job-list endpoint.

## Supported scope and resource limits

Audit accepts local, untransformed single-part objects with an expected SHA256. Multipart, encrypted, compressed, tiered objects, delete markers, and invalid protection declarations are reported as unsupported. Migration additionally requires a plain unversioned bucket without configured encryption, Object Lock, replication, notification, quota, lifecycle, table namespace, ACL, access logging, or on-demand migration. Disk commits continue to use the bucket’s existing durability policy. These checks hold the bucket configuration fence through publication so the direct storage writer cannot bypass those policies during a race.

Each manifest contains 1–64 items. `max_object_bytes` is 1 byte–5 GiB and limits the anonymous local temporary file used for one object. Ensure that amount of space is available in the process temporary directory. One cluster-wide worker runs at a time. `bytes_per_second` is 64 KiB/s–1 GiB/s and bounds each sequential source read, destination PUT stream, and verification read. It is not a physical disk or network aggregate ceiling: erasure redundancy and internal buffering still apply. Metadata requests are not byte throttled. Lower rates extend the time a source read or destination publication holds its normal object and bucket configuration locks; choose the rate and per-object budget with concurrent traffic in mind.

Migration uploads the same staged bytes that passed SHA256 verification, rechecks that checksum during PUT, and publishes with a create-only precondition. It preserves user metadata, content type/encoding/language, disposition, cache control, expiration, storage class, and tags. The new key has a new modification time and storage identity; consumers switch keys separately. The source is neither overwritten nor deleted. No original-object metadata is backfilled in place.

## Recovery and interpretation

`complete` means every item reached a final outcome, not that every item passed. Inspect `verified`, `migrated`, `mismatch`, `unsupported`, `stale`, and `conflict` individually. A source that is temporarily unavailable leaves the job `failed`; `resume` retries unavailable or prepared items while preserving completed results. A mismatch never publishes a target.

Pause and cancel first persist intent. An in-flight conditional target publication may finish; terminal acknowledgement follows after the worker stops. Cancellation does not delete a successfully published target. After a coordinator restart, `running` is the last durable state and does not prove a worker is alive. Explicitly resume to reacquire the distributed worker lock and recover. A persisted cancel intent is completed rather than restarted. A busy response means another worker still owns the lock; retry later.

A prepared checkpoint precedes destination publication. If the PUT acknowledgement or completion checkpoint is lost, resume first checks the destination's internal operation identity, protection declarations, size, and uncached full-content SHA256. Only a matching receipt is accepted as this job's completed copy. An unrelated destination is a conflict and is never overwritten. Source changes before verification finishes produce `stale`; create a new manifest after investigating the change. Bucket deletion/recreation invalidates the old job incarnation.

This workflow does not automatically repair damaged historical content, certify all historical shards, enable protected writes fleet-wide, clean MRF queues, or authorize rolling back to an incompatible binary.
