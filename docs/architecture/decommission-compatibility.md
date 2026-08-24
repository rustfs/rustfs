# Decommission Compatibility Scope

This note records the current RustFS decommission contract for admin/API
compatibility reviews.

## Current Contract

RustFS supports queued multi-pool decommission start requests on multi-pool
deployments.

The admin handler accepts the request shape used by the MinIO-compatible admin
API, including comma-separated pool targets. An empty target list is rejected.
Single-pool deployments reject decommission because there is no destination pool.
On multi-pool deployments, one or more valid target pools are accepted as a
single queued operation.

### Request Semantics

`POST /v3/pools/decommission` with comma-separated pool targets is treated as a
queue submission:

- validate all requested pool identifiers before mutating metadata;
- reject duplicate target pools in the same request;
- reject active or queued target pools;
- reject completed decommission targets because completion means the pool can be
  removed from the deployment configuration;
- allow failed or canceled targets to be retried;
- persist queued metadata before starting workers;
- start only the local-leader prefix of the queue on the receiving node.

The local-leader-prefix rule keeps the active worker on the leader for the pool
being moved while still allowing a request to contain later targets whose leaders
are different nodes. Later queued targets are recovered or promoted by the
leader that owns that target.

Admin start, cancel, and clear requests may arrive on any cluster node. When the
target pool first endpoint is remote, RustFS forwards the operation over the
authenticated internode RPC channel to that first endpoint. The receiving node
still enforces the local-leader rule before mutating decommission state.

### Persisted Metadata Shape

The queue is persisted in pool metadata and decoded with the rest of
`PoolMeta`. Each pool entry can distinguish:

- `active`: at most one pool currently moving data;
- `queued`: validated pools waiting for the active entry to finish;
- `completed`: pools finished successfully;
- `failed`: pools whose worker reached terminal failure;
- `canceled`: pools canceled before or during execution.

Legacy metadata without queue fields decodes as a non-queued decommission entry,
preserving restart behavior for already deployed clusters.

### Serial Scheduling And Recovery

Only one queued entry may own a decommission worker at a time. Startup recovery:

- loads pool metadata before rebalance recovery;
- resumes the first local non-terminal active/queued entry;
- skips a durably completed prefix and promotes the next queued entry only after
  successful completion;
- treats failed or canceled terminal entries as an automatic-promotion barrier,
  leaving later queued pools visible but stopped until an operator retries,
  clears, or otherwise resolves the terminal entry;
- keeps queued pools out of active worker scheduling until promotion, while still
  making their future state visible in admin status.

Promotion is persisted before worker execution. If cancellation is already
requested immediately after promotion, RustFS persists a canceled terminal state
instead of leaving the promoted pool active without a worker.

### Cancel Semantics

Cancel separates active and queued behavior:

- canceling the active entry requests worker cancellation and persists terminal
  metadata;
- canceling a queued entry marks that entry canceled before it becomes active;
- failed or canceled terminal entries can be cleared explicitly when the operator
  chooses to abandon the decommission attempt;
- peer reload failures during cancel must be surfaced in status and logs.

Cancel requests can be accepted on non-leader nodes as remote cancel intent; the
leader observes the pending cancel and applies it to the active worker.

### Status Response Shape

`GET /v3/pools/list` and `GET /v3/pools/status?pool=...` expose per-pool
machine-readable decommission state. The `status` field can report `active`,
`running`, `queued`, `complete`, `failed`, or `canceled`.

When decommission metadata is present, `decommissionInfo` includes:

- queue and terminal flags: `queued`, `complete`, `failed`, `canceled`;
- progress counters: `objectsDecommissioned`,
  `objectsDecommissionedFailed`, `bytesDecommissioned`, and
  `bytesDecommissionedFailed`;
- current location: `bucket`, `prefix`, and `object`;
- queue/history lists: `queuedBuckets` and `decommissionedBuckets`;
- `waitingReason`, currently `queued` for queued entries and
  `waiting_for_worker` when metadata exists but no worker has started.

This makes queued pools and stalled metadata visible without requiring operators
to inspect pool metadata files directly.

## MinIO Divergence Decisions

This section records the current product decisions for behavior that is close to
MinIO but not always byte-for-byte identical.

### Empty Delete Markers

MinIO decommission documentation states that empty delete markers, meaning delete
markers with no successor object versions, are not transitioned to another pool.

RustFS follows that behavior for decommission when the bucket has no replication
configuration: a lone remaining delete marker is treated as cleanup-only metadata
and is skipped. When replication is configured, RustFS intentionally keeps the
delete marker eligible for movement so delete-marker replication and purge state
are not lost.

RustFS rebalance uses the same predicate as decommission: skip only a lone delete
marker without replication. This is intentional even though MinIO's public
documentation calls out the decommission case more explicitly than the rebalance
case.

Regression guards:

- `should_skip_decommission_delete_marker_characterizes_empty_marker_without_replication`
- `should_skip_decommission_delete_marker_characterizes_replication_configured`
- `test_should_skip_rebalance_delete_marker_characterizes_empty_marker_without_replication`
- `test_should_skip_rebalance_delete_marker_characterizes_replication_configured`

### Lifecycle-Expired Versions During Cleanup

MinIO decommission ignores versions that are already expired by lifecycle rules.
RustFS follows that decommission behavior by allowing safely expired versions to
count toward source cleanup completion.

RustFS rebalance is intentionally stricter. Expired versions do not prove that a
target pool received an equivalent version, so rebalance cleanup requires actual
rebalance completion for the source entry instead of treating lifecycle-expired
versions as moved.

Regression guards:

- `test_should_cleanup_decommission_source_entry_accepts_migrated_and_safely_expired_versions`
- `test_should_cleanup_decommission_source_entry_accepts_versions_only_safely_expired_by_lifecycle`
- `test_should_cleanup_rebalance_source_entry_rejects_versions_only_expired_by_lifecycle`

No migration step is required for these decisions because this note documents the
current RustFS behavior. Changing either decision later requires an operator
compatibility note and updated characterization tests.

## Tier Free Versions During Decommission

A tier free version is an internal xl.meta record (`rustfs_filemeta::FREE_VERSION`,
flagged `XL_FLAG_FREE_VERSION`) shaped like a delete marker. It is created by
`MetaObject::init_free_version` when a version whose remote transition completed is
deleted locally: the visible version is removed and the record keeps the remote-tier
identity (tier, object name, version id, state, destination id) needed for an
idempotent remote delete. Free versions are not user-visible versions; `num_versions`
and all listing/GET paths exclude them.

### Lifecycle And Consumers

Creation: any local delete that removes a version whose transition status is
`complete` appends the record via `MetaObject::delete_version` →
`init_free_version` (skipped only when `skip_tier_free_version` is set, as on
data-movement copies). The same deletes also persist a durable tier-journal
entry on every user-facing path: S3 single deletes (`execute_delete_object` →
`delete_object_with_tier_delete_journal`), S3 batch deletes, lifecycle expiry,
and lifecycle delete-all all prepare and commit a journal entry around the
delete. A journal entry is omitted when the removed version's transition state
decodes as `TransitionVersionState::Unknown`, or on internal journal-less
delete paths that never touch transitioned user objects.

Consumption while the record exists: the background recovery loop started by
`init_background_expiry` (spawned by `spawn_tier_free_version_recovery_once`,
enabled by default) scans disks for pending records and re-enqueues them; the
usage scanner does the same; the lifecycle worker then deletes the remote tier
object idempotently and only afterwards removes the local record. Heal walks
include free-version records in metadata healing. Transition planning,
replication, restore, GET, listings, and usage aggregation never depend on
them.

### Decommission Handling

The exact decommission inventory loader (`load_file_info_versions_exact` via
`get_all_file_info_versions`) keeps free-version records inline in `versions`.
The migration loop handles them before lifecycle expiry and delete-marker
shortcuts. It selects a target pool using the free-version-aware lookup, then
writes the original free record to every target disk with the normal metadata
write quorum. The free-version marker, local version id, transition identity,
transition state, and destination id are preserved at the FileInfo/metadata
boundary.

The source record is physically removed only after the target write quorum has
committed and the source cleanup preflight still matches the exact inventory.
If the lifecycle worker has already completed the remote delete and removed the
source record before decommission acquires the source lock, decommission records
that identity as already consumed and treats the missing source record as safe.
If target capacity, metadata validation, lock fencing, or quorum fails, the
source record remains and the entry records `state = "free_version_retained"`
with reason `tier_free_version_migration_failed`; the worker retries the
operation on a later pass. A target record with the same version id is accepted
only when its free-version identity matches; a conflicting ordinary version or
different free record is an overwrite error. This makes retries idempotent and
prevents a free record from replacing a user-visible version.

### Reference-Audit Result

After migration, user-facing GET/list/transition/replication/restore paths still
exclude the record. Recovery, usage scanning, lifecycle tier cleanup, and heal
continue to see it when they request free versions, so an unresolved remote
delete remains actionable on the target pool. The committed tier journal remains
an independent retry source where one exists; it is not used as a reason to drop
the xl.meta record. In particular, `Unknown` transition state records are
migrated unchanged rather than discarded: the lifecycle worker retains them if
remote identity validation cannot make a delete request.

Each migrated record emits `state = "free_version_migrated"` with reason
`tier_free_version_migrated`. A record consumed before migration emits
`state = "free_version_consumed"` with reason
`tier_free_version_already_consumed`. Each failed record emits the retained state
and failure reason above. The entry also emits a disposition summary with
migrated, consumed, retained, and total counts. The final decommission sweep uses
the exact loader, counts free records still present, and emits one retained
record/reason for each unresolved free version before failing the sweep. This
makes successful migration, completed cleanup, and retained cleanup obligations
visible instead of silently omitting free records.

No new S3-visible version or admin response field is needed: free versions remain
internal and are never counted as user-visible versions. The structured
`decommission_entry` events are the operational status surface for the
free-version disposition; the existing decommission item/failed counters still
report the enclosing object migration result.

Regression guard:

- `decommission_tier_free_version_preserves_remote_identity`
- `decommission_tier_free_version_resume_requires_write_quorum`
- `decommission_tier_free_version_commit_rejects_lost_fence`
- `test_decommission_cleanup_preflight_accepts_migrated_free_version_consumed_from_source`
- `decommission_entry_skips_cleanup_only_marker_when_free_version_is_present`
- `decommission_entry_rejects_subquorum_free_version_conflict_and_retains_source`

## Regression Guard

The queued multi-pool contract is guarded by:

- `test_contextualized_decommission_start_request_allows_multiple_target_pools`
- `test_decommission_start_local_leader_allows_remote_queued_pool`
- `test_local_decommission_queue_prefix_stops_at_remote_leader`
- `test_decommission_peer_target_returns_none_for_local_first_endpoint`
- `test_pool_meta_queued_decommission_is_not_suspended_until_promoted`
- `test_pool_meta_promoted_queued_decommission_can_be_canceled`
- `test_first_resumable_decommission_queue_indices_stops_at_failed_or_canceled_state`
- `test_first_resumable_decommission_queue_indices_allows_after_completed_prefix`
- `admin_pool_list_item_exposes_queued_decommission_state`

These tests live in `crates/ecstore/src/core/pools.rs` and
`rustfs/src/app/admin_usecase.rs`.
