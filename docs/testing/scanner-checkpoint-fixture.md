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
