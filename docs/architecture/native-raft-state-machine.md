# Native Raft state machine and snapshots

**Use this when:** adapting native capture application state to OpenRaft, receiving snapshots, or investigating snapshot recovery and shared log ownership.

`crates/capture-authority/src/raft_state_machine.rs` provides `NativeRaftStateMachine::new(NativeLogWriter, CheckpointLimits)`. It implements the actual OpenRaft 0.9.25 state-machine and snapshot-builder traits. Pair it with `NativeRaftLogStore` using clones of the same writer. Apply uses the existing durable native application, membership and typed responses; no second live database is opened.

## Selected baseline and transport

Nonempty builds publish through `NativeLogWriter::checkpoint_local`. Empty builds publish a verified empty baseline under the same write ordering. Both selections include complete snapshot metadata in the same durable transaction. Export reads the selected baseline header, four baseline tables and metadata from one redb read transaction. It never substitutes live tables from a later applied cut. `get_current_snapshot` reconstructs that selection after restart.

The received ID is persisted verbatim. Locally generated IDs include cluster, local node and durable generation, so different nodes do not reuse a generation-only ID. Old nonempty baselines without full metadata derive this local ID; newly published metadata has a required marker, so missing metadata cannot silently fall back. Recovery verifies generation, complete applied LogId and membership against the selected header. A subsequent local checkpoint replaces the incoming metadata atomically.

`crates/capture-authority/src/native_snapshot_data.rs` implements bounded synchronous and asynchronous read/write/seek. Receive capacity is the configured frame limit plus the 56-byte capability manifest. Writes and seeks check arithmetic and capacity before changing state or allocating; allocation is fallible. The transport contains the existing capability manifest followed by the existing four-section frame. No private publisher types or new snapshot codec are involved.

## Validate and install

`crates/capture-authority/src/native_snapshot_install.rs` decodes into an isolated bounded staging database. It checks capability integrity, frame resource limits, membership, provenance and four-table relationships. Incoming metadata must match the full decoded applied ID and membership. The durable destination cluster and capture binding must match; local node identity and vote are preserved.

Publication acquires writer ordering then Store ownership. One Immediate durable transaction selects the baseline and metadata, replaces live tables and the disposable event index, and updates applied, membership, committed, tail and purged markers. A compatible complete cut retains its continuous suffix, including committed entries not yet applied. Conflicting uncommitted suffixes are discarded; applied history and conflicting committed history cannot be rolled back. A cut beyond the local tail becomes the durable purged boundary. Empty baselines recover by replaying from index zero through the same bounded disk-backed scratch path used for nonempty baselines, including operator recovery-limit overrides.

Cancellation before final publication rejects; a blocking commit retains ownership and the received handle until it finishes even if its async caller is dropped. Indeterminate commits retain the existing write fence. Reopen observes the complete old or complete new state. Snapshot EVENTS remain authoritative and are neither consumed nor deleted by publisher acknowledgements.

## Receive and purge ordering

OpenRaft can enqueue state-machine install and then run log purge before the separate state-machine worker begins installation. `begin_receiving_snapshot` therefore attaches a shared receive guard to the bounded handle. Install requires a handle belonging to that writer; callers of a future full-snapshot transport must use this trait entry point, rather than constructing an unrelated buffer.

When a purge lacks baseline coverage and a receive is pending, the log adapter waits for receive completion without holding writer ordering. Successful install, rejection and receive cancellation release the guard and wake waiters. Multiple handles retain independent ownership. Waiting has an absolute 30-second bound; a malformed or stalled receive cannot indefinitely block an uncovered purge. An uncovered purge without a pending receive still rejects immediately. Install itself atomically establishes the covered purged boundary, making the subsequent same-cut purge idempotent.

## Compatibility and verification

Existing nonempty schema-7 baselines remain readable. Empty baselines use the existing encoding with an absent applied cut, which older binaries reject. Before rolling back to such a binary, publish a compatible nonempty checkpoint or retain the new version. Do not discard the selected baseline after logs have been purged. The complete metadata keys are additive; incoming snapshot IDs must not be regenerated during recovery.

`raft_state_machine_tests.rs` and `native_snapshot_install_tests.rs` invoke actual traits on two real redb stores, check restart and continuation, bound receives, reject corrupt identity, preserve committed suffixes, and inject cancellation/power cuts around final commits. Deterministic pending-receive tests exercise the trait install/purge race and cancellation wakeup. These are local storage tests: they do not establish network consensus, a live Raft owner, or production S3 automatic capture. S06 remains blocked and R12 remains frozen.
