# Scanner Checkpoint Fixture

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

## Segment Observation Diagnostics

The nested `segment_observation` fixture compares diagnostic on/off runs of the real folder walker over six objects in `hot/`, `cold/`, and `other/`. Both runs save and reload the actual cache through the existing codec and revision-aware file backend. Assertions compare traversal order and the entire decoded cache, not encoded map order or aggregate size alone. Proposed top-level segments are a post-walk diagnostic and never reach a scanner selector or publication decision. The diagnostic retains at most four segments and 128 segment-name bytes; actual-walk samples are limited to 32 entries and 1,024 bytes. Exceeding sample limits fails the fixture rather than silently truncating its oracle.

Coverage loss for a missing producer, process restart, event gap, or compacted child coverage is an explicit fixture input and rejects the proposal. Entry/byte overflow and malformed keys also reject it. These tests verify the diagnostic response to those inputs, **not** production detection of lost events or complete mutation coverage. Mainline bucket dirty generations and hashed metadata-cache invalidation stripes are not an exact, replayable object-key stream. The open [prefix reuse proposal #7208](https://github.com/rustfs/rustfs/pull/7208) is a separate candidate implementation; these tests neither import its hint map nor activate its skip path.

The ECStore `segment_observation_equal_size_mutations_retire_metadata_generation` test uses the existing exact-key, test-only invalidation probe and actual owner operations. A same-length PUT must change the returned body and ETag while retiring the old generation; metadata-only PUT must change returned metadata and retire the old generation while size and ETag remain equal. Setup uses the existing full-fanout cache-priming helper; the observed mutations use normal owner locking. This is focused producer evidence, not an end-to-end connection between the owner probe and scanner range selection. The existing semantic mutation matrix covers additional owner entry points separately.

```sh
cargo test -p rustfs-scanner --lib segment_observation -- --list
RUST_MIN_STACK=4194304 cargo test -p rustfs-scanner --lib segment_observation -- --nocapture
RUST_MIN_STACK=4194304 cargo test -p rustfs-ecstore --lib segment_observation_equal_size_mutations_retire_metadata_generation -- --nocapture
```

[W19](https://github.com/rustfs/backlog/issues/2272) remains open for trustworthy producer coverage, source/incarnation binding, and production shadow observations. No production stream, durable journal, runtime feature switch, scan skipping, or performance claim is introduced here. Restart/gap fixtures do not establish restart-safe production coverage, and the revision-aware file backend does not prove EC publication durability.
