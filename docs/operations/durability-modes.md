# Durability modes (drive sync tiers)

RustFS lets operators choose how much fsync work runs on the object write
path. The default (`strict`) preserves the fully synced behavior RustFS has
always shipped; the relaxed tiers are **opt-in** trades of power-loss
durability for latency/IOPS.

## Configuration

```bash
# New tiered switch (wins when set to a valid value)
RUSTFS_DURABILITY_MODE=strict|relaxed|none   # default: strict

# Legacy binary switch (kept for compatibility, superseded by the above)
RUSTFS_DRIVE_SYNC_ENABLE=true|false          # default: true
```

Resolution rules:

| `RUSTFS_DURABILITY_MODE` | `RUSTFS_DRIVE_SYNC_ENABLE` | Effective mode |
| --- | --- | --- |
| unset | unset | `strict` (default) |
| unset | `true` | `strict` |
| unset | `false` | `legacy-off` (the historical "everything off" semantics) |
| `strict` / `relaxed` / `none` | anything | the named mode |
| invalid value | any | logged warning, falls back to the legacy switch, then the default |

Values are case-insensitive and whitespace-tolerant. The mode is resolved
**once per process** and cached (it also removes the per-call `getenv` the
old switch performed a dozen times per PUT); changing the environment
requires a restart. The resolved mode is logged at startup under the
`disk_local_durability_mode` event.

`legacy-off` is not a value of `RUSTFS_DURABILITY_MODE`; it is only reachable
through `RUSTFS_DRIVE_SYNC_ENABLE=false` so that existing deployments keep
their exact current behavior. It is deprecated and will be retained for at
least one major version.

## What each mode fsyncs

Write points on the object path and how each mode treats them:

| Write point | `strict` | `relaxed` | `none` | `legacy-off` |
| --- | --- | --- | --- | --- |
| Erasure shard files (fdatasync before the commit rename) | yes | **yes** | no | no |
| Multipart part payload (fdatasync before `rename_part` commit) | yes | **yes** | no | no |
| xl.meta contents (tmp write before the commit rename) | yes | no | no | no |
| Inline objects (data embedded in xl.meta) | yes | no | no | no |
| Old-metadata rollback backups | yes | no | no | no |
| Directory entries of commit renames (fsync of the parent dir) | yes | no | no | no |
| System-critical writes (see pinning below) | yes | yes (pinned) | yes (pinned) | **no** |

## Power-loss guarantees, honestly stated

**`strict` (default).** Every acknowledged write (PUT, UploadPart,
CompleteMultipartUpload, delete markers, metadata updates) is durable on the
individual drive before the 200 OK: payload bytes, xl.meta, rollback backups,
and the directory entries of the commit renames are all fsynced. A whole-node
(or whole-cluster) power failure does not lose acknowledged data. This is the
current mainline behavior, unchanged.

**`relaxed`.** Payload bytes of non-inline objects and multipart parts are
fdatasynced to the device before the acknowledgement, but the metadata
commits — xl.meta contents, rollback backups, and the directory entries of
the commit renames — are left to the page cache. Consequences on a power
failure:

- On the affected drive, a **recently acknowledged version can be lost
  entirely** — not merely "the directory entry rolls back". When neither the
  xl.meta bytes nor the rename's directory entry are synced, the commit
  itself can vanish; surviving shard bytes become unreferenced orphans on
  that drive.
- **Inline (small) objects receive no per-object fsync at all** in this mode:
  their data lives inside xl.meta, and xl.meta is not synced. This matches
  MinIO's default posture (no per-object fsync) but means small objects have
  the widest loss window.
- Durability of acknowledged writes therefore rests on **erasure-coded
  redundancy across other nodes** plus the unclean-shutdown heal introduced
  in PR #4221 converging the affected drive afterwards.

Deployment rule for `relaxed`: only multi-node clusters whose nodes sit in
**independent power domains** (separate feeds/UPS). If all nodes can lose
power simultaneously — the exact incident class that motivated PR #4221 —
`relaxed` can lose recently acknowledged objects cluster-wide. Single-node
deployments must stay on `strict`.

**`none`.** No fsync on the object data path at all; acknowledged objects can
vanish wholesale on power loss, payload included. System-critical writes are
still pinned (below). This is the tier equivalent of the old escape hatch,
useful for throwaway/benchmark data only.

**`legacy-off`.** The historical semantics of
`RUSTFS_DRIVE_SYNC_ENABLE=false`, preserved bit for bit for existing
deployments: nothing is fsynced anywhere, **including system-critical
metadata** such as `format.json`. Prefer `RUSTFS_DURABILITY_MODE=none`, which
keeps the system-critical writes safe.

## System-critical pinning

Writes that commit into system namespaces are pinned to `strict` regardless
of the configured tier (except under `legacy-off`, see above):

- `.rustfs.sys` — `format.json`, IAM and cluster configuration, bucket
  metadata, and everything else outside the scratch namespaces;
- `.minio.sys` — the same namespace during MinIO migration.

The scratch namespaces `.rustfs.sys/tmp` and `.rustfs.sys/multipart` stage
in-flight **user object data** and follow the configured tier — they are
exactly the writes the relaxed tiers exist for. Their durability is decided
by the destination volume at commit time, so an IAM or bucket-metadata object
staged in tmp still commits with full `strict` durability.

The durability mode is server-side configuration only; it cannot be raised or
lowered by any request header.

## Per-bucket durability (phase 2)

A bucket can override the process-wide mode with its own tier. The override
is stored in the bucket's metadata (a `durability.json` entry in
`.rustfs.sys/buckets/<bucket>/.metadata.bin`, written as a RustFS extension
field that MinIO's decoder skips) and resolved per write at the commit
points, so no restart is needed.

### Configuration

```bash
# Set an override (admin credentials, ConfigUpdateAdminAction)
curl -X PUT "http://<host>/rustfs/admin/v3/bucket-durability/<bucket>" \
     -d '{"mode":"relaxed"}'   # strict | relaxed | none

# Read it back ("mode": null means the bucket inherits the global mode)
curl "http://<host>/rustfs/admin/v3/bucket-durability/<bucket>"

# Clear it (the bucket inherits the global mode again)
curl -X DELETE "http://<host>/rustfs/admin/v3/bucket-durability/<bucket>"
```

`mc` integration is a follow-up; for now the admin API above is the
configuration plane.

### Resolution order

For a write committing into volume `V`:

1. system-critical namespaces (`.rustfs.sys`, `.minio.sys` outside the
   scratch dirs) are always `strict` — a bucket override can never be
   attached to them, and any attempt is rejected and logged;
2. otherwise, if the destination bucket has an override, the override wins —
   in **both** directions (a bucket can be `relaxed` under a `strict` global
   default, or pinned `strict` under a `relaxed` global default);
3. otherwise the process-wide mode applies.

Under `legacy-off` (`RUSTFS_DRIVE_SYNC_ENABLE=false`) per-bucket overrides
do not apply at all: the legacy switch keeps its historical semantics bit
for bit. `legacy-off` is also not a valid per-bucket tier.

Because scratch-staged data commits under the destination volume, an object
staged in `.rustfs.sys/tmp` follows the override of the bucket it commits
into, exactly like the global tier.

### Propagation and effect latency

The override rides the existing bucket-metadata cache, with no invalidation
channel of its own:

- on the node that applies the config change, the new tier is effective for
  writes that resolve their durability after the update completes;
- other nodes are told to reload the bucket's metadata right away (the same
  peer notification used for every bucket config change); if a peer misses
  the notification, the periodic bucket-metadata refresh loop (15 minutes)
  converges it;
- an in-flight operation keeps the tier it resolved at its start — a single
  commit is never half-old-tier, half-new-tier;
- deleting the bucket drops the override with the rest of its metadata.

Until a peer has reloaded the metadata, its writes use the previous tier.
This is the same eventual-consistency window every other bucket config
(policy, quota, versioning) already has.

### Power-loss guarantees

Identical to the corresponding global tier, scoped to the bucket. In
particular the `relaxed` deployment rule (multi-node clusters in independent
power domains only) applies per bucket: overriding a bucket to `relaxed` on
a single-node deployment can lose recently acknowledged objects in that
bucket on power failure.

## Performance expectations

The often-quoted 26x PUT throughput delta was measured on macOS with the old
binary switch fully **off** (equivalent to `none`/`legacy-off`), where
`F_FULLFSYNC` heavily amplifies sync cost. `relaxed` keeps the per-shard
fdatasync, so its gain is necessarily smaller and must be measured on the
target platform (Linux ext4/xfs) before being relied on. Do not use `none`
numbers to size `relaxed`.

## Scope

Phase 1 (rustfs/backlog#926) shipped the global, per-process tier configured
by environment variable. Phase 2 (rustfs/backlog#938) adds the per-bucket
override described above, configured through the admin API and stored in
bucket metadata. `mc admin` integration for the per-bucket tier is a
follow-up.
