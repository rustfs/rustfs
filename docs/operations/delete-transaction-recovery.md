# Delete transaction recovery

An interrupted object deletion can leave metadata backups, reserved shards, or
an unpublished target. Normal S3 listings cannot discover all of these files.
Recovery therefore inspects raw transaction records on every configured drive
in the owning erasure set under the bucket incarnation fence and exact-key
write lock. It does not modify GET or PUT behavior.

## Decisions and replay

New deletions assemble the backup, versioned prepared journal, and exact target
in the private system temporary area, then atomically publish the complete
transaction directory before modifying metadata. Interrupted preparation cannot
leave an undiscoverable target-only record or temporary file in the object key. After the metadata write quorum, the coordinator publishes durable
commit receipts to a write quorum before returning success. A failed or lost
publication response is indeterminate: recovery must inspect the persisted
records instead of immediately restoring the backup.

| Evidence | Action |
| --- | --- |
| New prepared transaction, no decision, intact original shards and matching generation | Restore and sync the original metadata before removing preparation evidence. |
| Structured commit receipt | Publish the certified target to matching returning participants and finish deletion. |
| Missing, unavailable, or unsupported participant | Preserve records and retry when every configured participant can be inspected. |
| Newer generation or partially overlapping version history | Preserve records and report blocked recovery. |
| Historical backup or disk-local marker without a structured set decision | Preserve records during automatic recovery. |

Missing local metadata does not authorize user-bucket residue cleanup, even
when the current object exists on a metadata quorum. Legacy local cleanup is
restricted to internal system-bucket maintenance. User-bucket legacy residue
uses only the guarded set-wide removal described below. Dry-run and execution
share the same source-generation check; an already-absent explicit non-null
version can converge without touching unrelated live versions.

Rollback preserves native multipart metadata and integrity sidecars. Protected
parts require their declared proof generation; recovery verifies both the proof
records and actual shard payloads before restoring metadata or removing
recovery evidence. Missing or corrupt required proofs leave the transaction
blocked with its backup intact.

A disk-local `delete-data.<T>` marker does not prove an erasure-set decision.
Returning drives cannot restore stale data after a certified deletion. Commit
receipts remain until every participant and related transaction for the key
has converged. For consecutive certified version deletions, an older decision
can finish its own data cleanup after later deletion removes current metadata.
A prepared copy needs a matching same-transaction decision before promotion;
absence alone cannot certify it. Cross-transaction null-version supersession also requires causal
predecessor evidence; timestamps alone cannot authorize it. Cleanup respects active snapshots and removes only validated
transaction files, with directory removal performed nonrecursively.

A live object upgraded from 1.0.1 can still carry an old
`reserve-delete-data.<T_old>` inside its data directory. A later certified
deletion may reclaim that regular-file reservation together with the payload
only in the directory owned by the current transaction. The old reservation
never authorizes deletion by itself. Foreign commit markers, malformed marker
names, symlinks, unknown files, live versions, and UUID-prefix child objects
remain protected. Snapshot release uses the same checked cleanup path.

Ordinary heal applies the same boundary to orphan data directories. A UUID
directory name alone is not evidence that a directory contains disposable
shards: it can be a child object's key. Reclamation requires a complete drive
inventory, readable parent metadata that does not reference the directory, no
pending transaction records, and a leaf containing only recognized payload
files. The final mutation rechecks these conditions under the metadata lease,
including after snapshot deferral. It unlinks only the classified files and
removes the empty directory nonrecursively; strict mode synchronizes both the
leaf and its parent. Missing metadata or an unsupported inspection RPC leaves
the directory intact for later recovery.
The evidence check traverses empty pages until completion and owns its iterator
state for the request, so large multipart directories do not block cleanup and
completed or canceled probes do not consume shared discovery cursors.

## Entry points

Every startup, including a clean restart, starts incremental raw discovery of
local transaction records. During normal operation, deletion registers a
bounded, coalesced exact-key retry hint before preparing metadata. Synchronous
completion clears only its own hint generation; an uncertain publication or
unfinished cleanup remains eligible for automatic replay without another
DELETE or administrator request. The on-disk journal and receipt remain the
only durable decision authority; the in-memory hints contain no commit decision.
Returning drives and queue overflow request discovery again, and a low-frequency
bounded reconciliation scan covers records written through remote participants.
Discovery advances by pages and retries failed sources separately, rather than
rescanning every healthy bucket on each maintenance tick.
Corrupt record contents remain discoverable by their physical key; exact-key
inspection rejects them without hiding later records. Unsafe paths, symlinks,
and structural filesystem errors still stop that discovery scope.

Unclean-shutdown erasure-set healing also performs transaction recovery before
ordinary object listing. Administrator bucket, set, prefix, and object heal use the
same recovery core. Existing heal selection, lifecycle admission, namespace
locking, and movement guards still apply. Prefix recovery settles only matching
keys, including transactions hidden from listings. Dry-run inspects and reports without
settling transactions.

An unconditional DELETE of a missing exact key, or an administrator heal with
`remove` enabled, can authorize historical residue removal only when the
bucket is authoritatively never-versioned and has no Object Lock. Every
configured drive must be inspected, the current object must be absent, and all
residue must match the guarded legacy layout. Versioned/suspended buckets,
retained versions, conditions, unknown entries, symlinks, and prefix children
are not covered by that authorization. The same fresh intent can certify the
absence of an unprotected new prepared null-version transaction when every
configured drive has no current metadata. This is a new authorized decision;
it does not infer an old commit from missing causal evidence. Mixed or
unrecognized historical layouts remain blocked.

A certified absent-target transaction can coexist with an unrelated old backup
in a different data directory. Automatic recovery finishes the certified
transaction's own cleanup, preserves the ambiguous backup, and retains receipts
until the whole key settles. A fresh unconditional DELETE can discard eligible
legacy residue alongside those receipts under the same checks above. A foreign
transaction marker alone never authorizes removing the backup.

An isolated `xl.meta.bkp` is not enough to distinguish a committed delete from
an interrupted rollback. Automatic recovery does not guess which outcome was
intended. Preserve a copy of the original volume and use an authoritative
application backup or snapshot for recovery when no transaction decision can be
established. `remove` means explicitly discarding eligible residue; it cannot
restore ambiguous data or bypass versioning and Object Lock protections.

A missing specific-version DELETE first recovers hidden transactions and then
reads the current version again. It must not acknowledge an unresolved hidden
version as an idempotent no-op.

## Upgrade and failure handling

Upgrade all participating nodes before relying on the new protocol. Recovery
uses one authenticated additive RPC; unsupported peers cannot silently accept
new flags through the legacy delete endpoint. An online older participant
causes the modern operation to fail before decision publication. Modern nodes
also reject legacy forward transactional user-bucket deletes. A cluster still
controlled by an older majority retains the older behavior. Do not downgrade
while recovery records remain; old binaries cannot interpret the new protocol.
An upgraded initiator reports an unsupported participant as retryable S3
`ServiceUnavailable` (503), including when the delete aggregates pool results.

Keep unavailable drives and transaction directories intact. Bring the full
set online, run a scoped heal in dry-run mode, inspect its recovery result,
and then run the same scoped heal without dry-run. Use `remove` for ambiguous
historical residue only after verifying that the exact objects are intended
to be deleted and the bucket meets the restrictions above. Do not recursively
remove transaction directories by hand to make a bucket appear empty.

Recovery reports discovered, committed, rolled-back, reclaimed historical,
blocked, pending, and failed outcomes. Pending and failed counts can represent
an incomplete set or a failed discovery/inspection scope rather than a known
number of transactions. Bucket and prefix results
include a structured `deleteRecovery` status: `pending` means unavailable
participants or unfinished work, `blocked` means insufficient safe evidence,
and `failed` means corrupt transaction data. A local corruption or pending
transaction does not prevent other buckets or ordinary object healing from
running. The pass still reports incomplete recovery and does not publish a
completed recovery checkpoint. Cancellation, lifecycle/incarnation changes,
movement fences, invalid paths, and other set-level errors retain their
termination semantics. Exact-object recovery still returns a retryable quorum
error for unavailable participants; blocked evidence requires operator
investigation. A successful
S3 delete response does not imply immediate physical cleanup while snapshots
or returning participants still retain necessary proof.

If ordinary set healing overlaps unresolved transactions, its resume state
durably records that coverage needs refreshing. Pending retries preserve the
completed object-scan progress. Once recovery resolves, the set is scanned
again before reporting completion, including when a background rollback already
restored and finalized formerly hidden objects. This additional scan is required
on that recovery transition; it is not a periodic full-set heal.

The upgrade CI retains its pinned rc.5 source and independently tests stable
1.0.1. The legacy-reservation case uses metadata and shards written by the
official 1.0.1 binary and injects an empty reservation while it is stopped;
this is a deterministic interruption fixture, not a hardware power-loss test.
Mixed-version stages verify that modern DELETE fails safely while older
participants are online. They do not promise continuous DELETE availability or
arbitrary downgrade while new recovery records exist.

## Settlement deadline and durability

`RUSTFS_DELETE_RECOVERY_TIMEOUT_SECS` controls the settlement RPC deadline,
including full shard verification during rollback. Key and bucket lifecycle
ownership stay attached to late local filesystem operations after cancellation
or timeout. The default is 900 seconds;
valid values are 30 through 86400 seconds. Invalid values fail explicitly.
Increase it when intact large objects need more verification time. It does not
change normal object read or delete-apply deadlines. A timeout can conceal a
completed mutation, so rerun inspection and recovery rather than guessing the
outcome from the RPC error.

The records and restores follow the configured durability policy. Strict mode
syncs the required metadata and directory changes. Process SIGKILL testing
checks restart behavior; it does not establish power-loss behavior on every
filesystem or storage device. Mode `none` intentionally offers weaker sync
guarantees. Do not treat a throughput comparison as a durability substitute.

## Reproduce the Docker regression suite

Build the candidate with the test-only barriers enabled:

```sh
cargo build --locked --release -p rustfs --bin rustfs --no-default-features --features e2e-test-hooks
python3 scripts/test/delete_recovery_docker.py --help
```

Pass a native Linux binary with `--binary`, a Linux image containing Python and
curl with `--image`, and a unique Docker namespace. The optional
`--baseline-binary` enables the mixed-version case. The harness uses synthetic
S3 objects, localhost ports, labeled isolated volumes, and a non-root server.
It stops only its own containers and keeps JSON results and logs in the chosen
artifact directory. Production builds must not enable `e2e-test-hooks`.

Run the preactivation/prepare/apply/decision process-stop cases, interrupted restore,
returning and untouched peers, both generation orderings, version/retention
and prefix protection, administrator dry-run/removal, response-loss,
permission/corruption, and mixed-version cases. Preserve the source identity
of both binaries with the results. A passing process-stop matrix does not
cover hardware power cuts, Windows runtime behavior, or DELETE throughput.
