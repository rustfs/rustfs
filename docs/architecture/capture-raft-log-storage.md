# Capture Raft log storage

**Use this when:** adapting the native capture writer to OpenRaft log persistence or changing checkpoint-authorized log prefix cleanup.

`crates/capture-authority/src/raft_log_storage.rs` implements `NativeRaftLogStore::new(NativeLogWriter)` and the actual OpenRaft 0.9.25 `RaftLogStorage<CaptureTypeConfig>` and `RaftLogReader` traits. Clones share the original Store and write ordering; no adapter opens another redb database. The crate enables the existing dependency's `storage-v2` feature without changing its version.

## Durable operations

Vote, append, truncate, committed-marker writes and purge run through the existing native blocking writer. Append supplies its real `LogFlushed` callback to that worker and reports success only after the original durable commit returns. Failure reports an error rather than success, including the existing indeterminate-commit write fence. Cancellation of an awaiting append does not release the worker's ordering guard or callback before storage work finishes. Lock order remains writer ordering followed by Store; no Store mutex is held across an await.

`get_log_state` reads the durable purged marker and actual retained log tail, checks the stored tail agrees, and reports the purged ID as the tail when no log remains. Applied progress is not used as a substitute. Committed-marker writes reuse the existing exact-identity and monotonicity checks. `save_committed(None)` cannot clear an existing marker.

`get_log_reader` and direct reader calls delegate to the existing `NativeLogReader`. The adapter translates native failures to typed OpenRaft storage errors; it does not reinterpret application responses or manufacture consensus results.

## Authorized prefix purge

A purge target requires the currently selected durable local checkpoint and validated baseline/suffix recovery evidence. It must lie at or before the checkpoint cut and match the complete retained LogId, including term and leader. Missing checkpoints, uncovered targets, mismatched identity and write-disabled handles reject before deletion.

The original writer ordering and Store ownership cover validation through commit. The transaction rechecks the selected checkpoint and exact target, deletes only LOGS and APPLY_RESULTS through that target, and advances the existing purged marker atomically. Retained logs above the target and all EVENTS, OPERATIONS and object/version state remain intact. Repeating the exact current purged ID succeeds without moving the marker; older or conflicting requests cannot rewind it or claim an identity no longer retained.

The existing `NativeLogWriter::purge_checkpoint` receipt API still checks its exact generation and checkpoint cut, then invokes the same operation at that cut. The on-disk purged marker and checkpoint encodings are unchanged: existing recovery already validates the remaining prefix from purged+1 through the baseline cut. Partial purge therefore needs no migration.

## Verification and owner boundary

The `raft_log_storage` tests invoke actual storage traits against real redb. `RaftLogStorageExt::blocking_append` is the public OpenRaft helper that constructs its own private `LogFlushed`, invokes the adapter and waits for the callback. Tests do not forge callbacks or substitute direct writer append for trait acceptance. Reopen and the existing disk-fault backend exercise flush persistence, cancellation, failed commits, partial purge, identity rejection and retained suffix continuation. Existing native checkpoint and source reader tests verify compatibility and event retention.

This adapter is the production authority owner's log-storage prerequisite. It does not implement `RaftStateMachine`, snapshot receive/install, the actual Raft owner, network transport or S3 publication. Trusted test preparation uses existing native apply/checkpoint calls without claiming real consensus. S06 remains blocked and R12 remains frozen; this slice does not complete U05/U06.

Rollback can remove adapter consumers while retaining the existing native writer, checkpoint and disk formats. Prefixes already durably purged remain recoverable from their selected checkpoint and retained suffix; rollback must not discard that baseline.
