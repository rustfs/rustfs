# ECStore core regression gates

The required native tests and pinned fixture hashes live in
`.config/ecstore-required-tests.json`. `Test and Lint` checks that each test
was selected by the existing `ci` run, is not ignored, and has a fresh nonempty
JUnit report. The check does not start another workspace test run.

## Native invariant coverage

The names below identify tests in `rustfs-ecstore` unless a different crate is
shown. The manifest contains their complete module paths.

| Boundary | Required test | Oracle |
|---|---|---|
| Write quorum | `inline_put_direct_commit_accepts_exact_quorum_and_rejects_quorum_minus_one` | W commits; W-1 fails without exposing a fresh object. |
| Metadata rollback | `write_unique_file_info_reverts_metadata_when_write_quorum_fails` | A failed write preserves the previous metadata. |
| Rollback inspection | `rename_rollback_incomplete_preserves_overwrite_data_dirs_and_staging` | Real failed undo records rejected inspection admission and retains incomplete receipts, old shards, backups, and staging across reopen. |
| Closed inspection channel | `rename_rollback_closed_heal_receiver_preserves_recovery_material` | A fresh child closes the real heal receiver; all five rollback faults under both completion policies retain recovery material and record the actual failed send. |
| Cancelled undo | `rename_rollback_incomplete_cancelled_task_is_not_success` | A cancelled undo remains incomplete. |
| Fault schedule | `commit_fault_schedule_keeps_multiple_disk_phases_independent` | Two armed phases remain independently paused and are released by their own handles. |
| Fault checkpoint owner | `commit_fault_schedule_rejects_duplicate_checkpoint_without_disarming_owner` | A duplicate checkpoint cannot replace the current handle. |
| Completion equivalence | `rename_rollback_incomplete_matches_early_ack_and_full_wait_after_reopen` | Both completion policies retain the same recovery residue. |
| Physical undo ownership | `rename_rollback_children_keep_namespace_ownership_after_coordinator_panic` | A coordinator panic cannot retire a running physical undo. |
| Late tail failure | `rename_data_early_ack_post_mutation_tail_error_never_rolls_back_commit` | A late error cannot reverse an acknowledged quorum commit. |
| Control-plane completion | `tail_drained_put_waits_for_tail_and_allows_immediate_cas` | TailDrained waits for the tail before a same-key CAS. |
| Seeded completion pressure | `seeded_put_completion_pressure_drains_every_real_tail` | Four rounds with seed `0x2248_0009` retain target ownership under two real pressure PUTs, check complete bodies and same-key CAS for normal callers, and drain all tails and staging after normal or cancelled callers. |
| Borrowed lock | `no_lock_put_waits_for_rename_tail_under_outer_guard` | A borrowed write does not reacquire the same key and waits for its tail. |
| Borrowed capability | `borrowed_write_context_rejects_wrong_namespace_and_cache_flag` | A different namespace or cache flag cannot manufacture a write owner. |
| Quota read ownership | `quota_snapshot_reuses_same_name_owner_and_rereads_disk_authority` | A same-named write reuses its actual owner without recursive acquisition; stale cache and corrupt disk metadata cannot replace disk authority. |
| Quota snapshot scope | `quota_snapshot_rejects_wrong_scope_incarnation_and_transaction_guard` | A snapshot for another store, bucket, incarnation or guard cannot authorize namespace reuse. |
| Quota lifecycle | `quota_snapshot_rejects_lifecycle_loss_during_read_and_changed_disk_incarnation` | Losing the lifecycle fence during the read or changing the disk incarnation rejects reuse. |
| Marker purge receipt | `quorum_marker_purge_receipt_survives_reopen_with_stale_member` | A purge receipt survives fresh-store reopen with a stale member, remains version-scoped and can be consumed. |
| Borrowed cancellation | `borrowed_tail_drained_put_keeps_owner_after_caller_cancel` and `borrowed_complete_keeps_namespace_owner_after_caller_cancel` | The actual namespace owner survives caller cancellation until physical publication drains. |
| Copy completion | `borrowed_copy_tail_drained_waits_for_rename_tail` | Copy preserves an explicit TailDrained policy after a metadata quorum commits. |
| Pool metadata owner | `pool_meta_cas_context_retains_actual_owner_for_exact_target` and `pool_meta_cas_rejects_released_owner` | CAS retains its real owner and rejects a released guard. |
| Catalog publication | `rustfs`: `commit_backend_borrowed_put_preserves_owner_authorization_and_precondition` | An authorized write under the actual outer owner avoids recursive acquisition, preserves IfAbsent, and leaves the original bytes after conflict. |
| Stale writer | `put_object_no_lock_aborts_after_outer_namespace_lock_loss` | A lost outer fence rejects publication. |
| Cancelled writer | `tail_drained_put_owned_commit_survives_waiter_cancellation` | Cancelling the waiter does not cancel the commit owner. |
| GET reconstruction | `blackbox_get_restores_body_after_one_shard_file_is_removed` | The complete returned body equals the original bytes. |
| Range reconstruction | `blackbox_range_read_restores_exact_slice_with_one_offline_disk` | The requested byte slice is reconstructed exactly. |
| Transitioned Range | `transitioned_compressed_object_range_get_returns_plaintext_slice` | A transitioned compressed object returns plaintext Range bytes. |
| MPU cancellation | `cancelled_complete_keeps_upload_lock_through_tail_cleanup` | Upload lock ownership survives caller cancellation and tail cleanup. |
| MPU abort/complete | `abort_and_complete_linearize_for_plain_sse_and_legacy_layouts` | Abort and complete have one outcome across the supported layouts. |
| MPU delete quorum | `abort_enforces_delete_write_quorum_boundary` | Abort requires the existing delete write quorum. |
| MPU physical publication | `complete_multipart_timeout_keeps_namespace_owner_until_physical_publication` | Timeout cannot retire an active physical publication. |
| Restore failures | `multipart_restore_aborts_every_post_create_failure` | Every injected post-create failure aborts the remote upload. |
| Partial LIST metadata | `rustfs-filemeta`: `resolve_with_write_quorum_slack_keeps_partial_latest_hidden_during_merge` | A partial newer version remains hidden. |
| Metadata observations | `metadata_observation_all_four_slot_states_and_arrival_orders_preserve_reduction` and its companion tests | Pending, absent, corrupt, offline and successful disk slots retain the existing quorum decisions across all four-slot arrival orders. |
| LIST backend parity | `list_objects_shared_corpus_*` | One independent namespace oracle checks actual disk, set, pool, store and index paths, including version visibility, page boundaries, faults and cancellation. |
| LIST logical budget | `list_path_gather_results_counts_common_prefixes_before_page_limit` | Repeated common prefixes consume one page slot while max+1 retains a continuation. |
| Directory-marker metadata | `rustfs-filemeta`: `resolve_directory_marker_and_prefix_preserve_quorum_in_both_orders` | Quorum object metadata survives same-named prefix candidates; a fallback directory still requires its own quorum. |
| MinIO metadata | `rustfs-filemeta`: `parses_real_minio_object_xlmeta` | Independently captured MinIO metadata is readable. |
| Legacy metadata | `rustfs-filemeta`: `test_issue_2288_legacy_xlmeta_compatibility` | The fixed legacy fixture decodes with its expected object fields. |
| Legacy null version | `rustfs-filemeta`: `test_into_fileinfo_reads_legacy_nil_uuid_inline_key` | Legacy nil UUID inline data retains null-version semantics. |
| Corrupt part arrays | `rustfs-filemeta`: `crc_valid_but_part_arrays_corrupt_into_fileinfo_errors_not_panics` | CRC-valid inconsistent arrays return an error rather than panic. |
| External erasure shards | `pinned_erasure_fixtures_test`: `pinned_minio_shards_decode_and_reconstruct_plaintext` and `pinned_legacy_shards_decode_and_reconstruct_plaintext` | Public GET dispatch returns the complete independent body and exact Range bytes, including after a real data shard is removed before the first GET. |
| External shard bitrot | `pinned_erasure_fixtures_test`: `pinned_erasure_shards_reject_bitrot_corruption` | Corrupt captured shards fail verification before changing the caller's buffer. |
| External shard quorum and codec | `pinned_erasure_fixtures_test`: `pinned_erasure_shards_reject_insufficient_quorum_and_wrong_codec` | Five shards cannot satisfy the six-shard read quorum; the opposite codec cannot reproduce the oracle. |

The same manifest retains the S3, Azure and GCS source-contract tests for ODM.
The static LIST keys in `crates/ecstore/tests/fixtures/list_namespace_keys.json`
are also consumed by ODM's `list_through_static_namespace_boundary_matrix`.
The ECStore query replay uses seed `0xec5707`; the ODM matrix retains seed
`0xec5706`.
Fixture provenance is recorded in
[`crates/ecstore/tests/fixtures/minio/README.md`](../../crates/ecstore/tests/fixtures/minio/README.md).
These metadata fixtures do not establish compatibility for an external legacy
erasure-shard corpus.

The separate [external shard corpus](../../crates/ecstore/tests/fixtures/erasure-shards/README.md)
contains non-inline object files from released MinIO GF8 and historical RustFS
GF16 processes. The manifest and plaintext hashes are required by the same
gate; each integration test validates all captured metadata and shard hashes.
Missing or changed files fail rather than skip. Historical RustFS is upgrade
compatibility evidence, not an independently implemented GF16 codec. These
reads do not establish live MinIO drive-set migration or crash durability.

## Historical LIST regressions

Both cases below run in `Test and Lint` through the existing `ci` profile.
`reference_page` consumes the input namespace graph without reading FileMeta
or calling production pagination. Every store, pool and set page is compared
for object keys, common prefixes, continuation and truncation; the complete
walk must contain each visible identity exactly once.

| Report | Physical input | Required test |
|---|---|---|
| [#7049](https://github.com/rustfs/rustfs/pull/7049) | `a/first`, `a/nested/second`, `a0`, `b/final`; raw and paged limits 1 through 4 exercise a child reaching its limit before a later sibling. | `list_objects_shared_corpus_cross_subdirectory_limit` |
| [#7010](https://github.com/rustfs/rustfs/pull/7010) | Four disks first retain three committed metadata copies, then a different disk goes offline. Two readable copies remain among three online disks; plain and slash-delimited pages use the same visible-namespace oracle. | `list_objects_shared_corpus_offline_corrupt_and_minority_stale` |

Native negative controls use the unchanged corpus at main `68ad1a1d7bb`.
Removing only the two old #7049 limit guards still passes because the recursive
child's stop result provides another protection. Removing those guards and
ignoring that result in the same stack-draining loop makes raw limit 1 return
`a/first` and `a0` instead of only `a/first`. This tests the historical invariant,
not a literal revert of #7049. Setting the current offline write-quorum
allowance to zero makes the #7010 corpus return `InsufficientReadQuorum` where
the independent oracle requires visible objects. Restoring both production
files passes both tests. These local controls do not establish a concurrent
cross-page snapshot or process-crash safety.

## Write owner and completion routes

These routes explain the ownership exercised by the required tests. A raw
`no_lock` flag does not establish a borrowed owner.

| Caller | Commit owner | Completion |
|---|---|---|
| Ordinary PUT | The writer acquires and retains its namespace guard. | Quorum by default; explicit TailDrained is preserved. |
| CompleteMultipartUpload | Owned by default, or a validated target-key WriteCommitGuard; the upload guard remains retained. | Full tail completion. |
| S3 CopyObject destination | The use case supplies its actual destination WriteCommitGuard to the inner PUT. | Explicit TailDrained. |
| Direct store Copy | Its actual destination owner is propagated to the inner writer. | Preserves the destination options. |
| Restore copy-back | The inner PUT or MPU acquires its own object writer; remote-tuple and bucket fences are separate. | PUT preserves its policy; MPU waits for the full tail. |
| Scanner publication | An exact-target publication capability and scanner scope. | Scanner scope disables early ACK. |
| Decommission target | Its target publication capability and capacity reservation. | Capacity admission disables PUT early ACK; MPU waits for the full tail. |
| Startup metadata migration | Sealed OfflineRecovery mode. | Explicit TailDrained. |

Publication and OfflineRecovery are separate capabilities. These local owner
rules do not establish durable distributed generation authority.

## Selection and evidence

After the normal `ci` test run, inspect its actual selection:

```bash
cargo nextest list --profile ci --all --exclude e2e_test --message-format json > core-test-listing.json
python3 scripts/check_test_wiring.py --check-core core-test-listing.json
```

`python3 scripts/check_test_wiring.py --self-test` exercises missing, ignored,
filtered and malformed listing cases, and missing or altered required fixtures.
Those cases must fail; an optional legacy fixture test that returns early is
not evidence for a required fixture.

The `test-and-lint` artifact contains the native log, listing, JUnit and
execution receipt. A native test that reopens a disk in the same process does
not establish process-crash or power-loss durability.

The closed-receiver child uses the existing rollback matrix and is reaped on
normal exit. Its harness has no independent total deadline; a hang is not
covered by the passing matrix.

## Process and network faults

Use the existing `e2e-full`, `e2e-nightly` and `e2e-distributed` lanes for real
server-process restart, fresh-drive and network-fault scenarios. Their
membership remains pinned by `.config/e2e-*-selection.txt`; the lane checks
selection before execution. See [distributed-e2e.md](distributed-e2e.md) for
the named scenarios, platform requirements and failure artifacts.

The protocol membership includes WebDAV quota reporting from #8197: 19 tests
on Linux and 17 on macOS, where two SFTP compliance cases are Linux-only.
The updated pins retain every previously selected test.

Record the source commit, selected test names, actual fault reached, process
logs, metadata observations and JUnit for each run. A capability early return,
setup failure, missing report or abstract protocol model cannot establish
that the fault ran. Native ownership tests do not prove the durable ordered
authority described in
[unified-object-generation.md](../architecture/unified-object-generation.md).
