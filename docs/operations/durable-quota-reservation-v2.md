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
