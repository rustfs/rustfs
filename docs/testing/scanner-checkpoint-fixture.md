# Scanner Checkpoint Fixture

## Raw Enumeration Restart Diagnostic

`enumeration_restart_worker` exercises the real `scan_data_folder` with a local disk and valid `xl.meta` objects. Without configuration it is a positive CI control: four one-byte objects must complete and survive a cache codec round trip. It is not an ignored test or an assertion that a known defect must persist.

```sh
RUST_MIN_STACK=4194304 cargo test -p rustfs-scanner --lib enumeration_restart_worker -- --nocapture
cargo test -p rustfs-scanner --lib --no-run --message-format=json
python3 -m unittest discover -s scripts -p 'test_diagnose_scanner_enumeration_restart.py'
```

Use the `executable` from the scanner library test `compiler-artifact` JSON record as `--test-binary` below. The driver verifies that it contains the exact worker test before doing any work; a zero-test filter cannot pass.

```sh
python3 scripts/diagnose_scanner_enumeration_restart.py \
  --test-binary /path/to/compiled/scanner-libtest \
  --output /tmp/scanner-enumeration-new-run \
  --objects 128 --raw-entry-budget 8 --rounds 8
```

The output directory must not exist. Each round starts a new OS test-worker process, opens the same synthetic disk, decodes the preceding cache, invokes the real scanner, encodes the returned cache, and decodes it again. When cancellation returns no useful partial cache, it preserves the previous cache. Reports identify the actual child PID, round, raw entries and name bytes observed, processed objects, retained object/version/byte counts, and completeness. No observed-name set, `readdir` offset, or assumed stable ordering is used as durable progress. Namespace creation happens only during fixture setup, before scan accounting.

The `cfg(test)` hook observes actual entries delivered by `read_dir` and cancels the existing cycle token at the fixed entry limit. This is a deterministic injected **raw-entry work budget**, not a wall-clock performance measurement or a claim that kernel prefetch, probes, allocations, name bytes, or cache I/O are independently budgeted. The watchdog timeout only bounds worker lifetime. The hook does not replace enumeration, classification, or recursion, and does not exist in production builds. In particular, `xl.meta` object-boundary classification is unchanged.

Exit 0 requires exact complete object/version/byte coverage within the same fixed budget on every executed round. Exit 1 means the strict convergence oracle remains unmet, including the current flat-directory enumeration starvation case. Exit 2 means invalid input, worker failure, or invalid evidence; it is not a successful reproduction. There is no final unbudgeted sweep. Small fixtures can pass; that does not establish the general R-E gate from [the scanner review comment](https://github.com/rustfs/backlog/issues/2240#issuecomment-5549222480). Raw entries observed are not a retained enumeration watermark. This is scanner-worker process restart plus codec evidence, **not** whole-daemon restart, EC quorum persistence, crash/fsync durability, remote RPC, or a throughput benchmark. The caller owns the bounded evidence directory and may remove it after inspection.

### Missing Storage Capability

The current `scanner_folder::FolderScanner::scan_folder` collects child folders before recursing. `LocalDisk::scan_dir` also reads the whole parent before sorting and applying `forward_to`. The persistent key-only listing index's `collect_persistent_key_only_index_objects` / `rebuild_persistent_key_only_index` collects all objects in memory before publication and excludes deleted entries. It cannot supply a restartable first-build cursor over per-disk raw entries, orphan directories, and metadata boundaries. Repeated listing from the beginning is real work, not free pagination.

A future storage-owner capability must expose an explicit unsupported/building/ready state and a durable snapshot/index identity bound to disk mount, bucket incarnation, and directory identity. It must budget the first build and every page, including entry count, name bytes, metadata probes, I/O and time; survive a process restart during first build; seal page data before advancing the manifest; and distinguish enumerated, classified, and fully processed frontiers. An uncommitted page may be replayed only within a bounded cost. `xl.meta` classification must finish before descendants become traversable namespace. Missing capability or invalid identities must not become fabricated progress or completeness. No such capability is implemented by this diagnostic, and ordinary local storage remains without this R-E guarantee.

## Completed Subtree Checkpoint Fixture

The `checkpoint_fixture` tests exercise a bounded namespace of 24 static objects and one repeatedly updated hot object. Each of three rounds runs the production local disk scanner with an object budget, saves the returned partial cache through the production persistence codec and revision checks to a two-file test backend, and reloads it before preparing the next round. The fixture prints static-subtree coverage at each boundary and cumulative visited entries. This is a diagnostic of retained coverage, not a throughput benchmark.

Run the fixture and confirm the test filter selects a nonzero number of tests:

```sh
cargo test -p rustfs-scanner --lib checkpoint_fixture -- --list
RUST_MIN_STACK=4194304 cargo test -p rustfs-scanner --lib checkpoint_fixture -- --nocapture
```

Both the unchanged-plan and hot-plan cases require durable static coverage to increase each round. `LostAtPrepare` identifies invalidation before traversal; `LostAtReload` identifies loss between the returned cache and persisted data; `WalkWithoutRetention` identifies visited growth without durable coverage growth. Missing, corrupt, empty-root, and oversized checkpoint inputs are rejected by the strict fixture reader. Save failure and publication-epoch rejection must preserve the preceding file bytes. Parent cancellation is checked separately from object-budget exhaustion. Superseded classification is tested separately from either incomplete outcome.

After three interrupted rounds, the fixture overwrites a previously visited object with two versions, deletes another visited object, and creates one more hot object. It then keeps the same four-object budget until the stable namespace is certified. Finishing a sweep that spans different mutation plans must first return partial; a subsequent verification sweep must produce exactly 25 objects, 2 versioned entries, and 34 logical bytes. There is no final unbudgeted sweep.

The new bucket checkpoint binds the persisted bucket incarnation, set layout, publication epoch, tier generation and scan mode, with the existing source/leader/key-format checks. Its forward sweep records the starting and requested mutation plans separately. Partial sweeps omit the legacy `scan_plan_digest`, so older readers rebuild instead of treating mixed observations as a current complete snapshot. Completed sweeps restore that digest only after covering one mutation plan. Unsupported or missing identities retain the legacy rebuild path. The stable-plan fast path requires a complete snapshot without unfinished checkpoint state; its interrupted result must enter forward validation on reload.

A coverage receipt binds the completed traversal frontier to its scope, starting plan and canonical covered-prefix digest. It excludes ancestor aggregates and the unvisited suffix, so unrelated suffix changes cannot invalidate completed work. Cancellation seals only the completed frontier; failed child traversal and known failed-metadata skips block further frontier advancement. A saved cursor pointing at an existing but unvisited old subtree is rejected unless it agrees with that receipt. The receipt is a consistency check for storage owned by the scanner, not authentication against a party able to forge the entire cache and recompute its digest. New metadata remains map-encoded with optional top-level fields.

The Normal-to-Deep regression holds the mutation plan fixed, changes metadata in a previously visited prefix, and verifies that the real Deep disk-scan entry point reads that prefix again. It also checks that a complete Normal cache cannot satisfy the Deep `Current` path. The fixture disables heal side effects; it proves traversal re-entry, not actual bitrot detection or repair. Additional tests cover map round-trips, stale identities, existing-but-uncovered cursors, coverage gaps, and per-instance metadata ownership.

This fixture bounds object processing after directory enumeration. It does not prove fixed-budget enumeration of arbitrarily wide directories or real process-restart convergence. Those gates require a storage-owned resumable enumeration capability, including its initial construction cost; a readdir offset, an in-memory iterator or a last-name filter is not that capability.

For every saved partial cache, the fixture also passes its progress through the production authenticated remote terminal-frame writer and stream consumer. A remote partial result must remain partial even when its progress reports visited objects. This covers the return-frame contract; it does not execute the remote RPC server, distributed locks, EC quorum persistence, mixed-version peers, process crashes, or fsync durability. The file backend models revision preconditions and persistence errors, not a concurrent object store.

The synthetic namespace contains no customer data. Temporary files are removed with their owning fixture. Rolling back to a reader without the optional checkpoint metadata rebuilds partial coverage; it must not clear quota floors or complete authoritative snapshots. A passing fixture alone does not establish that the field report in [issue #7108](https://github.com/rustfs/rustfs/issues/7108) has been independently reproduced or fixed. A field diagnosis must separately identify the source capture, cycle and leader identity, and decoded bucket/set caches.

## Segment Observation Diagnostics

The nested `segment_observation` fixture compares diagnostic on/off runs of the real folder walker over six objects in `hot/`, `cold/`, and `other/`. Each run first rewrites `hot/one` with a different, equal-length ETag in real fixture metadata and reads it back to verify changed bytes at unchanged length. The successful fixture write supplies its known key to a diagnostic executed inside the real walker's path callback. Both runs save and reload the actual cache through the existing codec and revision-aware file backend. Assertions compare traversal order and the entire decoded cache, not encoded map order or aggregate size alone. Proposed top-level segments never reach a scanner selector or publication decision, and non-proposed segments must still be walked. The diagnostic retains at most four segments and 128 segment-name bytes; actual-walk samples are limited to 32 entries and 1,024 bytes. Exceeding sample limits fails the fixture rather than silently truncating its oracle. Saving a cache here is not an authoritative root publication.

Entry/byte overflow and malformed keys reject the fixture proposal. Missing producers, process restarts, event gaps, and compacted child coverage remain **unverified production capabilities**, not simulated success cases in this fixture. Mainline bucket dirty generations and hashed metadata-cache invalidation stripes are not an exact, replayable object-key stream. The open [prefix reuse proposal #7208](https://github.com/rustfs/rustfs/pull/7208) is a separate candidate implementation; these tests neither import its hint map nor activate its skip path.

The ECStore `segment_observation_equal_size_mutations_retire_metadata_generation` test uses the existing exact-key, test-only invalidation probe and actual owner operations. A same-length PUT must change the returned body and ETag while retiring the old generation; metadata-only PUT must change returned metadata and retire the old generation while size and ETag remain equal. Setup uses the existing full-fanout cache-priming helper; the observed mutations use normal owner locking. This is focused producer evidence, not an end-to-end connection between the owner probe and scanner range selection. The existing semantic mutation matrix covers additional owner entry points separately.

```sh
cargo test -p rustfs-scanner --lib segment_observation -- --list
RUST_MIN_STACK=4194304 cargo test -p rustfs-scanner --lib segment_observation -- --nocapture
RUST_MIN_STACK=4194304 cargo test -p rustfs-ecstore --lib segment_observation_equal_size_mutations_retire_metadata_generation -- --nocapture
```

[W19](https://github.com/rustfs/backlog/issues/2272) remains open for trustworthy producer coverage, source/incarnation binding, and production shadow observations. No production stream, durable journal, runtime feature switch, scan skipping, or performance claim is introduced here. No restart/gap detection or restart-safe production coverage is established, and the revision-aware file backend does not prove EC publication durability.
