# Decommission Compatibility Scope

This note records the current RustFS decommission contract for admin/API
compatibility reviews.

## Current Contract

RustFS supports one target pool per decommission start request.

The admin handler accepts the request shape used by the MinIO-compatible admin
API, including comma-separated pool targets. The storage layer intentionally
rejects requests that resolve to more than one target pool with:

```text
failed to start decommission: decommission supports one target pool at a time
```

This is the supported RustFS behavior for the current remediation scope. It is
more conservative than MinIO's queued multi-pool decommission model, where
multiple pools can be submitted and processed serially.

## Out Of Scope

Queued multi-pool decommission is not part of the current implementation. Adding
it requires a separate product-approved design covering:

- persisted queue format and validation;
- restart recovery for active and queued pools;
- status semantics for active, queued, canceled, failed, and completed pools;
- one-active-pool-at-a-time worker scheduling;
- admin compatibility for partial queue failures and duplicate pool targets.

Until that design lands, clients should submit at most one decommission target
pool per request.

## Queued Multi-pool Design Draft

This draft records the minimum product decision needed before RustFS can align
with MinIO's queued multi-pool decommission behavior. It is not an implementation
approval.

### Request Semantics

If approved, `POST /v3/pools/decommission` with comma-separated pool targets
would be treated as a single queue submission:

- validate all requested pool identifiers before mutating metadata;
- reject duplicate target pools in the same request;
- reject targets that are already active, queued, completed, or canceled unless
  product explicitly chooses idempotent resubmission semantics;
- keep one operation ID for the queue submission and a stable per-pool queue
  entry ID for audit/status correlation;
- return success only after the queued metadata is durably written and required
  peers have reloaded it, following the existing single-pool start barrier.

The current single-pool request remains a queue with exactly one entry.

### Persisted Metadata Shape

The queue must be persisted in `pool.bin` or a versioned companion structure
loaded atomically with pool metadata. The metadata must distinguish:

- `active`: at most one pool currently moving data;
- `queued`: validated pools waiting for the active entry to finish;
- `completed`: pools finished successfully;
- `failed`: pools whose worker reached terminal failure;
- `canceled`: pools canceled before or during execution.

Legacy metadata without a queue must decode as a queue containing the existing
active decommission pool, preserving restart behavior for already deployed
clusters. Unknown queue states must fail closed during decode rather than
silently restoring a pool to ordinary placement.

### Serial Scheduling And Recovery

Only one queued entry may own a decommission worker at a time. Startup recovery
must:

- load pool metadata before rebalance recovery;
- resume the active entry if it exists and is not terminal;
- promote the next queued entry only after the previous entry is durably
  completed, failed, or canceled;
- avoid promoting another entry while peer reload propagation is unresolved;
- keep queued pools out of active worker scheduling until promotion, while still
  making their future state visible in admin status.

Promotion must be persisted before worker spawn. If worker spawn fails after
promotion, the promoted entry must remain visible as failed or start-degraded
rather than being silently skipped.

### Cancel Semantics

Cancel must define separate behavior for active and queued entries:

- canceling the active entry requests worker cancellation and persists terminal
  metadata using the same "cancel is not ordinary pool restoration" semantics as
  the current single-pool flow;
- canceling queued entries removes or marks those entries before they ever become
  active, without modifying ordinary placement for those pools;
- canceling the whole queue should be an explicit API mode, not an accidental
  side effect of canceling one target;
- peer reload failures during cancel must be surfaced in status and logs.

The API should reject ambiguous cancel requests that do not identify whether the
operator intends to cancel one entry, the active entry, or the whole queue.

### Status Response Shape

Admin status needs enough structure for operators and `mc admin decommission`
compatibility:

- queue operation ID;
- active entry with pool index, state, progress, last error, and propagation
  status;
- queued entries with stable order and submission time;
- completed/failed/canceled history with terminal time and reason;
- last metadata propagation attempt and peer failures for start, promotion, and
  cancel.

The status response must make it obvious when no worker is currently running
because the queue is waiting for propagation, retry, or operator action.

### Approval Gate

Before implementation, product/compatibility review must choose:

- whether multi-pool submission is required for RustFS admin compatibility;
- whether duplicate or already-terminal targets are rejected or treated
  idempotently;
- whether cancel addresses one target by default or the entire queue;
- how long completed history is retained in pool metadata.

Until those choices are made, RustFS intentionally preserves the current
single-pool contract.

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

## Regression Guard

The single-pool contract is guarded by
`test_contextualized_decommission_start_request_rejects_multiple_target_pools`
in `crates/ecstore/src/pools.rs`.
