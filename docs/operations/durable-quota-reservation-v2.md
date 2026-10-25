# Durable quota reservation protocol v2

**Use this when:** operating durable hard quotas, upgrading quota-enabled clusters, or diagnosing quota writes during a node outage.

Protocol v2 is an opt-in hard-quota protocol for distributed deployments. It
keeps the quota limit authoritative in one allocator ledger while moving
request reservations into sixteen independently locked shard ledgers.

## State and invariants

The allocator ledger contains the bucket incarnation, quota revision, limit,
monotonic generation, and durable credit grants. A grant belongs to exactly
one shard and is never larger than the remaining quota at the allocator write
that issued it. A shard ledger contains the same incarnation/revision, the
allocator generation it adopted, its grants, accounted usage, and pending
reservations.

The safety invariant is:

```text
sum(shard grant amounts) <= quota_limit
shard.accounted_usage <= sum(shard grant amounts)
```

`accounted_usage` includes committed bytes and pending positive reservation
growth. An abort removes the pending growth; a commit removes only the pending
record because its growth is already accounted. A reservation whose commit
marker was persisted is removed only as a precharged reconcile candidate; the
shard is then rebuilt from objects belonging to that shard.

## Locking and fencing

The v2 lock order is:

```text
caller object/upload -> metadata transaction -> operation lock -> allocator
read lock -> shard write lock
```

Credit refill releases the operation and shard locks before taking the
allocator write lock. The allocator write is fenced before persistence. Shard
reservation and settlement fence the shard before persistence. The allocator
generation and bucket incarnation/revision are checked on every load, so a
late writer from an older epoch fails closed. Allocator and shard paths include
the bucket incarnation and quota revision, so a quota change starts a new epoch
after an exact bucket bootstrap instead of reusing old grants.

## Crash and replay behavior

Allocator grants are durable before a shard can adopt them. If a process exits
after the allocator write but before the shard write, the next shard writer
adopts the grant by ID. If a process exits after a shard reservation, the
operation lock probe can reclaim reservations and marks `commit_started`
entries for exact shard reconciliation, preventing a crash window from
releasing credit for an object that may already be durable.
Repeated commit/abort operations are keyed by the operation ID and expected
reservation fields; mismatches fail closed.

## Mixed-version rollout

`reservation_protocol: 2` is serialized with `reservation_quota`. A node that
does not understand v2 must reject the configuration rather than treating it
as the legacy snapshot protocol. Operators must upgrade the whole fleet before
calling `BucketQuota::new_sharded`. Downgrades require clearing or reconciling
all v2 allocator/shard ledgers first; v1 and v2 ledgers are never mixed.

The default `BucketQuota::new` remains protocol v1. This keeps existing
deployments compatible until an operator has completed the rollout and chosen
the sharded protocol explicitly.

## Availability and fence protocol upgrades

Both reservation protocols use the same write-availability rules. Setting a
durable quota still requires the existing whole-fleet capability proof. Once
configured, a quota write does not require that proof to remain live. It needs
the namespace locks, authoritative bucket configuration, quota ledger, and
object mutation fences to satisfy their respective quorums. A failed node
therefore does not stop quota writes when those quorums remain available.

Object fences use the target erasure set's write quorum. Ledger, allocator,
and shard fencing retains the existing per-pool set checks. An unavailable
set that owns required quota state can still block a write even if its object
data set is healthy; a majority of nodes across the whole cluster is not a
substitute for these quorums.

Participating disks must also agree with the coordinator's configured
pool/set/disk topology. The dedicated quota RPCs authenticate that topology
identity and reject a different layout, including a different pool order.
An offline node does not change the configured topology, so this check does
not require every node to respond.

Quota mutations use the dedicated `AcquireQuotaMutationFence`,
`ReleaseQuotaMutationFence`, and `RenameDataWithQuotaFence` RPCs. A fenced
rename requires a valid token for that disk process and object. A restart
discards the token registry, and revocation waits for existing claims to
drain before returning success. Unsupported peers fail closed; clients never
fall back to ordinary rename or lease RPCs for a quota mutation.

Upgrade every node before relying on degraded quota writes. During a rolling
upgrade, new coordinators cannot count old peers toward quota fence quorums,
so their quota writes can temporarily fail until enough upgraded disks are
available in every required set. Older coordinators retain their whole-fleet
proof requirement. The same limitation applies when rolling back: avoid
routing quota writes to a newer coordinator that lacks enough compatible
peers. The quota and ledger formats are unchanged, including the zero-quota
encoding that makes pre-reservation coordinators reject positive growth.

## Performance boundary

Normal v2 requests take a shared allocator read lock and one shard write lock;
the bucket-wide allocator write lock is used only to issue a new credit chunk.
The per-grant chunk is bounded by roughly one sixteenth of the quota (or the
requested growth when larger), so one active shard cannot consume the entire
remaining quota through a single speculative grant. It is a conservative upper
bound, not a best-effort token bucket.
Unused credit is intentionally retained until the quota epoch changes or a
future reconciliation operation returns it, trading some capacity utilization
for a simple crash-safe upper bound.

## Quotas saved before the reservation protocols

A quota configuration without `reservation_protocol` (saved by a release
before protocol v1) is still admitted from scanner usage snapshots: its writes
need an authoritative or degraded usage value and fail closed with a
retryable `503` when neither exists. Decision D3
([scanner-usage-authority-decision.md](../architecture/scanner-usage-authority-decision.md))
moves such buckets onto the sharded durable protocol by re-saving the same
limit and type with `reservation_protocol: 2`; `created_at` is preserved.

Two triggers exist and both go through the same guarded save:

- Lazily, on the first quota read of such a bucket through the instance's
  registered bucket metadata system (every PUT, COPY, multipart completion
  and admin quota read). The read answers with the configuration it found and
  schedules the re-save in the background; it is never blocked or failed by
  the attempt. An attempt that does not settle the bucket is retried no
  sooner than five minutes later.
- In bulk, with `POST /rustfs/admin/v3/quota-migration`
  (`admin:SetBucketQuota`). The sweep visits every bucket and answers with a
  per-bucket report:

```json
{
  "target_reservation_protocol": 2,
  "migrated": 1, "already_durable": 3, "no_quota": 10, "skipped": 0, "failed": 0,
  "buckets": [
    {"bucket": "photos", "outcome": "migrated", "reservation_protocol": 2, "updated_at": "2026-10-19T02:14:07Z"},
    {"bucket": "logs", "outcome": "already_durable", "reservation_protocol": 1},
    {"bucket": "tmp", "outcome": "no_quota"}
  ]
}
```

`outcome` is one of `migrated`, `already_durable`, `no_quota`, `skipped`
(`reason`: `unsupported_reservation_protocol`, `quota_revision_changed`,
`bucket_incarnation_changed`) or `failed` (`reason` carries the error). A
failed bucket does not stop the sweep; run the command again after fixing the
cause.

The save keeps the admin save's preconditions: the whole-fleet capability
proof must be live (a mixed-version fleet fails the attempt rather than
exposing protocol v2 to a node that would fail closed on it), the bucket
incarnation must still match, and the quota revision must be the one that was
read. A quota edited in between is reported as `quota_revision_changed` and
left for the next attempt. A protocol this node does not understand is never
rewritten. Durable quotas (protocol v1 or v2) are not touched; the admin
default for a new quota remains protocol v1.

Each attempt logs one `quota_reservation_protocol_migration` event
(`trigger` is `lazy` or `admin`, `result` is `migrated`, `noop`, `skipped` or
`failed`) and increments
`rustfs_quota_reservation_protocol_migrations_total` with the same labels.

## Usage reporting for durable quotas

For a bucket on protocol v1 or v2 the request-path quota check and the admin
quota endpoints read scanner usage for reporting only. When no usage is
available, `GET /rustfs/admin/v3/quota-stats/{bucket}` and
`POST /rustfs/admin/v3/quota-check/{bucket}` answer `200` with
`current_usage: null` and `remaining_quota: null` instead of `503`;
quota statistics also return `usage_percentage: null`. Object writes are
admitted by the ledger alone. Each such
lookup logs a `quota_usage_report` event at debug level and increments
`rustfs_quota_usage_report_unavailable_total`. A bucket without
`reservation_protocol` keeps the previous admission behaviour until it is migrated.

Read-only `GET /rustfs/admin/v3/quota/{bucket}` and
`GET /rustfs/admin/v3/quota-stats/{bucket}` use optional usage reporting for
all buckets, including buckets without a quota and snapshot-protocol quotas.
They return the configured limit while a full scan is still pending:
`size: null` on the quota response, or `current_usage: null`,
`remaining_quota: null` and `usage_percentage: null` on statistics when no
usage is available. Known usage, including zero, remains numeric. Missing
buckets and configuration errors remain errors. The MinIO-compatible
`GET /rustfs/admin/v3/get-bucket-quota?bucket=...` continues returning the
configured quota in its existing format.

These reads do not force an early scanner reload or consult a degraded floor;
the shared usage lookup retains its normal cache/backend refresh behavior.
They do not publish intermediate bucket-scan results, so finishing a bucket
walk does not guarantee immediately updated usage. Legacy write admission
and quota-check previews retain their existing usage requirements.

Distributed single-bucket usage reads project the requested size directly from
the cached snapshot. They share the full-snapshot loader's TTL, negative cache,
serialized refresh and invalidation generation, but do not clone other buckets
or their histograms. Refresh-only consumers also avoid copying the snapshot.
The full-snapshot API still returns an owned copy. Cache hits therefore perform
one bucket lookup; a cold or invalidated cache still requires the existing
backend load. This does not change scanner publication latency or the local
mutation cache's refresh policy.
