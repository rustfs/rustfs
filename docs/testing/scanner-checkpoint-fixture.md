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

The unchanged-plan case requires durable static coverage to increase each round. The hot-plan diagnostic changes the bucket plan digest between rounds and reports where coverage is lost without asserting that a particular defect must remain present. To require progress in this diagnostic as well:

```sh
RUST_MIN_STACK=4194304 RUSTFS_CHECKPOINT_REQUIRE_PROGRESS=1 cargo test -p rustfs-scanner --lib checkpoint_fixture_hot_digest_diagnostic -- --nocapture
```

A nonzero exit from the strict command means that walked work did not become additional retained static coverage. `LostAtPrepare` identifies invalidation before traversal; `LostAtReload` identifies loss between the returned cache and persisted data; `WalkWithoutRetention` identifies visited growth without durable coverage growth. Missing, corrupt, empty-root, and oversized checkpoint inputs are rejected by the strict fixture reader. Save failure and publication-epoch rejection must preserve the preceding file bytes. Parent cancellation is checked separately from object-budget exhaustion. Superseded classification is tested separately from either incomplete outcome.

For every saved partial cache, the fixture also passes its progress through the production authenticated remote terminal-frame writer and stream consumer. A remote partial result must remain partial even when its progress reports visited objects. This covers the return-frame contract; it does not execute the remote RPC server, distributed locks, EC quorum persistence, mixed-version peers, process crashes, or fsync durability. The file backend models revision preconditions and persistence errors, not a concurrent object store.

The synthetic namespace contains no customer data. Temporary files are removed with their owning fixture. Production scan semantics and persistent formats are unchanged, so rollback consists of removing these tests and this guide. A passing fixture alone does not establish that the field report in [issue #7108](https://github.com/rustfs/rustfs/issues/7108) has been independently reproduced or fixed. A field diagnosis must separately identify the source capture, cycle and leader identity, and decoded bucket/set caches.
