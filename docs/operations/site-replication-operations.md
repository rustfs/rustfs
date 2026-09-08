# Site Replication Operations

**Use this when:** operating a site-replication deployment, diagnosing a peer
outage or incomplete topology change, pairing sites that already contain data,
or planning an upgrade.

**Source of truth:** `rustfs/src/admin/handlers/site_replication.rs`,
`rustfs/src/site_replication/`, and the bucket-replication worker under
`crates/ecstore/src/bucket/replication/`.

Site replication combines two different convergence paths:

- the control plane replicates buckets, bucket metadata, IAM, and topology;
- ordinary bucket replication moves object versions and delete operations.

An `enabled: true` response only says that a site has more than one configured
peer. It does not prove that every peer is reachable or caught up. Always read
`pendingOperation`, `retryStats`, `PeerErrors`, and `Metrics` as well.

## Routine checks

Run these commands from an admin workstation with one alias per site:

```console
mc admin replicate info site-a
mc admin replicate status site-a
```

Check more than one site. A partition can leave each side with a different but
locally valid view.

`replicate info` is the compact control-plane view:

| Field | Interpretation |
|---|---|
| `enabled` | More than one site is configured; this is not a health verdict. |
| `sites` | The locally persisted topology. Compare deployment IDs and endpoints on every site. |
| `retryStats.pending` | Collapsed peer deliveries waiting to be retried. |
| `retryStats.failed` | Deliveries that crossed the escalation threshold and require attention. |
| `retryStats.lastError` | A redacted summary of the most recent delivery failure. |
| `pendingOperation` | A durable multi-step topology operation described below. Absence is the healthy steady state. |

`replicate status` adds detailed convergence state:

| Field | Interpretation |
|---|---|
| `Sites` / `PeerStates` | Configured peers and derived reachability/configuration state. |
| `PeerErrors` | A peer could not be queried. Its detailed counters may be absent; do not read zeros as success. |
| `BucketStats` | Per-bucket presence and versioning, replication, lifecycle, Object Lock, and metadata mismatches. |
| `PolicyStats`, `UserStats`, `GroupStats` | IAM inventory mismatches. |
| `RetryStats` | Durable control-plane retry backlog and escalation count. |
| `Metrics.replMetrics` | Per-destination online state, downtime, replicated counts/bytes, and `failed` totals/windows. |
| `Metrics.queued` / `Metrics.inProgress` | Object work waiting or active on the responding node. |
| `Metrics.errors` | Node-level object-replication failures. When only queue statistics are available, RustFS synthesizes a node entry and preserves this counter rather than reporting zero. |
| `Metrics.retries` | Redeliveries. Always zero today: a failed object is not retried by an event, it waits for the scanner pass described below. Read `errors` instead. |

Healthy means: the same topology is visible on all sites, no pending operation,
no peer error, no failed retry escalation, required bucket/IAM state is in sync,
and queue/error counters are stable or falling. Counters are cumulative; alert on
their rate and on a backlog that does not drain, not merely on a non-zero total.

## Pending operations and recovery

`pendingOperation` contains `operation`, an opaque `id`, `pendingPeers`, and
`ackedPeers`. Do not edit the site-replication state object by hand. The marker
is the crash-recovery journal and removing it can make a partially applied
operation look complete.

The heavyweight reconciler runs once at startup and every 600 seconds. The
lightweight retry drain runs every 30 seconds. A restart is therefore a valid
way to cause an immediate heavyweight pass after the underlying fault has been
fixed, but it is not a substitute for fixing connectivity, credentials, TLS,
or the remote endpoint.

### `remove`

The original topology and each peer acknowledgement are persisted before the
operation finalizes. While peers remain in `pendingPeers`, restore access to
them and wait for reconciliation. If a peer is permanently gone, a new remove
request may remove all currently active unacknowledged peers; RustFS permits
that request and then finalizes against the remaining topology. Removing the
local site or all sites is also an explicit completion path.

Do not re-add a site merely to hide this marker. First compare the topology on
all reachable peers. If the same operation ID makes no progress for more than
one heavyweight interval, collect `PeerErrors`, `RetryStats`, and the
site-replication logs before retrying the remove.

### `rotate-svc-acct`

Service-account rotation keeps the candidate secrets and peer acknowledgements
until every current remote peer accepts the rotation. Restore the failing peer
and allow the reconciler to resume it. Do not manually delete either candidate
credential during this window: doing so can remove the only credential that a
not-yet-acknowledged peer accepts.

After the marker clears, verify `replicate status` from every site, then retire
any separately retained old credential material according to local policy.

### `endpoint-refresh`

An endpoint, CA, or TLS-verification edit first refreshes the replication
target on every active peer and records acknowledgements. On startup and every
heavyweight pass, RustFS probes peer capability, uses the endpoint-refresh API
when supported (or the legacy peer-edit fallback), refreshes local bucket
targets, and commits the edit only after every still-active peer acknowledges.

If this marker is stuck:

1. Confirm that the proposed endpoint and CA are correct and reachable from
   every site, not only from the admin workstation.
2. Restore the site-replication service account and TLS trust path.
3. Wait for one 600-second pass or restart one healthy node to trigger the
   startup pass.
4. Re-run the identical edit only if the operation remains visible; a different
   endpoint edit is rejected while the existing refresh is pending. The journal
   pins the edit's payload, so a re-run without `--replicate-ilm-expiry` keeps
   the value the first attempt recorded, and a re-run asking for a different
   value is rejected. Finish or remove the pending refresh before changing it.

A peer removed from the topology no longer blocks completion. A remove request
is accepted when it removes every active unacknowledged peer.

## Outage recovery and convergence time

Control-plane retry begins on the 30-second drain, while heavyweight snapshots,
pending topology operations, and bucket wiring are revisited on the 600-second
pass. Object MRF entries are persisted every 10 seconds by default and target
health is probed every 5 seconds. These are scheduling bounds, not delivery
SLAs: network timeouts and the amount of queued work add to them.

Objects that must be rediscovered by the scanner have this conservative upper
bound before discovery:

```text
RUSTFS_DATA_USAGE_UPDATE_DIR_CYCLES
    × max(RUSTFS_SCANNER_CYCLE, actual duration of one scanner cycle)
```

The defaults re-descend a compacted directory every 16 cycles. A practical
production starting point for a tighter recovery objective is
`RUSTFS_DATA_USAGE_UPDATE_DIR_CYCLES=4`; `1` forces re-descent every cycle.
Measure the additional disk and metadata load before lowering it further or
tuning the scanner cadence. For an immediate operator-driven recovery, start a
site resync with `mc admin replicate resync start` and monitor its status.
Transfer time after discovery remains proportional to backlog size, bandwidth,
worker capacity, and target latency. Use queue depth and the rate of
`Metrics.errors` rather than the formula alone to decide whether convergence is
progressing.

## Pairing sites that already contain data

When more than one requested site is non-empty, preflight considers each bucket
name held by more than one site:

- versioning must be `Enabled` on every site holding the shared bucket;
- Object Lock enablement must be identical on every holder.

A bucket present on only one site is safe: post-add backfill creates it on the
other peers. A shared unversioned bucket is rejected because merging can
overwrite the only copy of an object. An Object Lock mismatch is rejected
because lock enablement cannot be changed after bucket creation and convergence
could otherwise strip a WORM guarantee.

If preflight rejects the pair, keep the authoritative copy, delete the
conflicting bucket (or its contents) from all other sites, run `replicate add`
again, and then start `replicate resync` from the surviving site. Back up and
validate the authoritative data before deleting anything.

## IAM convergence and repair boundary

Ordinary IAM changes are delivered to each peer. A successful bulk IAM import
also schedules one collapsed full-IAM snapshot per remote peer. A failed IAM
deletion is replayed before that snapshot so the snapshot cannot re-create a
principal or grant that was already revoked.

The safety state has two bounds:

- deletion high-water marks are retained for 30 days;
- deletion replay bodies are capped at 256 distinct entities per peer.

Repeated deletion of the same entity replaces its saved body. When the per-peer
cap is exceeded or the body cannot be serialized, the retry entry remains
escalated rather than pretending the deletion is replayable. An item from an
older sender without a source timestamp cannot install the 30-day high-water
mark, so verify it explicitly after a prolonged split. A successful drain
clears replay bodies; removing the peer prunes its bodies. For an escalated IAM
retry, use the site-replication repair workflow for the affected peer and IAM
family, then verify users, service accounts, groups, policies, and mappings on
both sides. Repair is the operator's explicit accountability transfer and
clears the saved deletion bodies only after the IAM repair succeeds.

Treat IAM divergence as a security incident: a user deleted on one site can
remain usable on an unreachable peer until replay or repair completes.

## Encrypted objects

| Source form | Replication behavior | Fail-closed condition |
|---|---|---|
| SSE-S3 | The source decrypts the object; the request sends only `AES256` intent; the destination encrypts with its own KMS. Source envelope material never leaves the site. | The destination cannot satisfy the encryption request, or the source metadata is incomplete/unsupported. The replica is `FAILED`; plaintext is not silently stored. |
| SSE-KMS | The source decrypts the object; the request sends `aws:kms` intent without the source-local key ID; the destination selects its own configured KMS key. | Either side cannot decrypt/encrypt, or the metadata mixes incompatible encryption evidence. |
| SSE-C | Stored ciphertext and the required SSE-C replication transport metadata pass through. RustFS verifies target evidence before accepting the replica. | The target does not echo the customer-algorithm evidence, required material/layout is absent, or the metadata is ambiguous. |

Unknown MinIO/RustFS encryption markers are never forwarded as ordinary user
metadata. They fail replication so an operator must migrate or repair the
object with a supported format.

## Rolling upgrades and rollback

Keep every node in one site on the same version whenever possible. Upgrade all
nodes of one site consecutively, verify its startup reconciliation and status,
then move to the next site. Do not intentionally leave a site mixed-version:
admin requests can land on different nodes, and an older node may not resume a
new pending-operation shape or expose its health fields.

Current state additions are optional and defaulted, so older readers ignore
them. The target-version ledger is stored as dual-prefixed internal object
metadata and is also ignored by older readers; rollback does not corrupt the
object format, but older code loses the assigned-version routing improvement.

Before rolling back across the fix that retains the data directory of a version
awaiting purge replication (rustfs/rustfs#7307), ensure no version purge is
pending. Older code can free that retained version's data directory before the
remote purge is acknowledged, leaving unreadable metadata and blocking bucket
deletion. Drain or repair replication and take a metadata/data backup first.

## Runtime knobs

These values are read when the owning background task starts. Restart the
server after changing them. The millisecond intervals have a 10 ms floor;
invalid values fall back to the default with a warning.

| Variable | Default | Effect |
|---|---:|---|
| `RUSTFS_REPL_HEALTH_CHECK_INTERVAL_MS` | `5000` | Remote-target health probe interval. Lowering it increases outbound probes. |
| `RUSTFS_REPL_MRF_FLUSH_INTERVAL_MS` | `10000` | Maximum periodic interval between MRF persistence flushes; 1,000 new entries also trigger a flush. |
| `RUSTFS_REPL_RESYNC_POLL_MAX_MS` | `60000` | Upper bound for randomized resync retry-poll sleep. |
| `RUSTFS_REPL_RESYNC_MAX_JOBS` | `2` | Concurrent resync jobs; values are bounded to `1..=32`. |

Transport-specific controls and target behavior are documented in
[Replication outbound transport](replication-outbound-transport.md). Validate a
new destination with [Replication target check](replication-check.md), and read
[Replication object size limits](replication-object-size-limits.md) before
moving large objects.
