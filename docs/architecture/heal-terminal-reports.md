# Retained Admin Heal Reports

Admin heal tokens retain their terminal status for ten minutes from the original completion time. The persistence owner is `RootHealRecovery` in `crates/heal/src/heal/manager/root_recovery.rs`; the bounded report codec is in `crates/heal/src/heal/manager/root_recovery/report.rs`.

## Publication and Recovery

The original schema-1 `terminal-root-heal-<token>.json` remains the commit marker. Its separate `heal-terminal-report-<token>.json` report has its own schema version and embeds the exact terminal identity. Both files belong to the same coordinator disk as the pending intent.

Publication writes the report through the storage owner's conditional file update, writes the terminal marker, and then conditionally removes the pending intent. Recovery never treats a report without its marker as committed. A committed terminal continues to suppress stale pending work even when its report is unreadable. An initial publication failure leaves the existing responsibility available for recovery.

The report preserves the canonical execution, traversal coverage, cumulative counters, diagnostic object window, progress, legacy result window, truncation flags, and incremental cursors. Restoring it neither recounts objects nor derives counters from progress. Its embedded terminal must match the retained marker before the report can be returned.

## Cancellation

An active cancellation first publishes an `aborted/cancelled` partial snapshot before retiring active ownership. The worker can subsequently finish recording its last object. Its final report replaces the earlier report, while the terminal marker and original completion timestamp remain unchanged. Cancelling a retry preserves any retained preceding attempt's outcome; unavailable historical counters remain unavailable.

If the final report update fails, the scheduler rereads the disk because an error can occur after publication. It returns that persisted report when readable. If the result cannot be resolved, the cached response exposes no canonical outcome or result window and marks the detail as truncated. It does not assume that the previous report won an uncertain write.

## Bounds and Retention

Report encoding and reading each enforce an 8 MiB JSON limit. The canonical diagnostic window retains at most 128 objects and 64 KiB of object storage accounting, with at most 1 KiB per detail. The legacy result window retains the existing 1 MiB memory budget. Decoding validates the report version, terminal identity, terminal execution, counters, object bounds, and cursor ordering, and reconstructs memory accounting.

GC uses the original completion timestamp, including after repeated restarts or cancellation refinement. For an expired terminal, it removes stale pending work before the report and marker. Report deletions share the existing 64-deletion budget. Expired orphan reports left before marker publication or by rollback GC are removed without deleting pending responsibility. Corrupt records are retained and reported as errors.

## Upgrade and Rollback

Schema-1 terminals without reports remain queryable. A terminal response without a canonical outcome adds `outcomeStatus: "unavailable"`; it does not fabricate counters or complete coverage. Missing historical result windows are marked truncated. Responses with an outcome and running responses do not add this field.

Older binaries retain their original terminal decoder and ignore the distinct report namespace. They continue to query status and prevent replay, but do not expose the new report's outcome. A later upgrade can read a retained report again; reports whose markers were collected by the old binary are handled as uncommitted orphans. Rollback therefore preserves the old status/replay contract, not the new outcome capability.


## Administrator Erasure-Set Traversal

A pool/set root request uses an explicit `all_buckets: true` marker beside the empty `buckets` list in its recovery scope. Recovery also accepts the historical omitted-marker spelling with an empty list, because that spelling was emitted by admission. A marker that conflicts with the list is rejected, and new admission validates the same scope before writing it. Named-bucket scopes keep their existing representation.

The traversal uses the administrator token as its checkpoint identity and the pending intent's owner disk as its resume anchor. Its first durable snapshot freezes the resolved bucket names and their incarnation IDs. A replay uses this snapshot and validates the original incarnation before each page and storage mutation. A bucket created after this snapshot is outside the resumed responsibility; a same-name replacement cannot acquire the old bucket's authority.

Administrator checkpoints use schema 8 and keep canonical counters and their bounded diagnostic window in the same conditional write as the per-version dedup identity and progress counters. An acknowledged object is published to the task outcome only after that write. Page cursors advance after their checkpoint ledger is durable. Ordinary and automatic replacement checkpoints continue to use schema 7.

Each object exhausts the existing bounded object retry budget before its final disposition is recorded. A traversal retry resumes the committed cursor instead of resetting successful objects for a full rescan. Missing or mismatched receipts, cutoff skips, and queued lifecycle expiry remain explicitly unresolved. Metadata maintenance without a canonical object receipt also remains unresolved. A completed traversal with exhausted object errors retains a failed task status and its canonical dispositions.

The administrator status codec preserves the exact version selector in each traversal receipt. An unversioned object's null slot is the nil UUID string, while a direct current-object request can carry JSON `null`. Consumers must not erase that distinction: an unspecified latest selector cannot replace the checkpoint's exact version identity. Pool/set consumers also validate the receipt's pool and set against the requested selectors.

Erasure traversal progress retains its legacy successful-processing counters. For twelve successfully processed objects with one repair and eleven verified healthy objects, `progress.objectsScanned` and `progress.objectsHealed` are both 12, while the canonical outcome has `processed=12`, `healed=1`, and `unchanged=11`. A consumer checks the outcome's partition and dispositions instead of requiring its repair count to equal the legacy progress count. Legacy shards without independent commitments can also complete physical repair while all twelve outcomes remain `unknown`; physical execution alone does not certify their identity. Tests requiring positive receipts must seed protected objects under the supported rollout gates.

The real-storage administrator contract tests cover both twelve-object cases through the status codec and retained same-token queries. A separate subprocess test kills an active executor with SIGKILL after acknowledged partial work, then reopens its disks, adds a bucket outside the original snapshot, and verifies the original token, scope, receipt identities, aggregate counters, and repaired bytes. This process-crash test does not establish VM power-loss, filesystem durability, or multi-set isolation guarantees.

A completed administrator checkpoint remains available until its terminal report and marker commit and the pending owner is retired. Normal publication then removes the completed resume artifacts. Cancellation, unreadable artifacts, or an interrupted cleanup can leave evidence for the existing conservative resume GC; none of those artifacts may authorize another token or an automatic replacement generation.

Older binaries do not understand administrator checkpoint schema 8, and their all-buckets recovery decoder rejects the historical empty scope. Keep the corrected binary for outstanding pool/set tasks and retained pool/set token queries. Rollback does not affect the schema-7 checkpoint representation used by ordinary and automatic replacement heals.
