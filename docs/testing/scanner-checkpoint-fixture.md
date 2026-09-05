# Scanner Checkpoint Fixture

The `checkpoint_fixture` tests exercise a bounded namespace of 24 static objects and one repeatedly updated hot object. Each of three rounds runs the production local disk scanner with an object budget, saves the returned partial cache through the production persistence codec and revision checks to a two-file test backend, and reloads it before preparing the next round. The fixture prints static-subtree coverage at each boundary and cumulative visited entries. This is a diagnostic of retained coverage, not a throughput benchmark.

Run the fixture and confirm the test filter selects a nonzero number of tests:

```sh
cargo test -p rustfs-scanner --lib checkpoint_fixture -- --list
RUST_MIN_STACK=4194304 cargo test -p rustfs-scanner --lib checkpoint_fixture -- --nocapture
```

Both the unchanged-plan and hot-plan cases require durable static coverage to increase each round. `LostAtPrepare` identifies invalidation before traversal; `LostAtReload` identifies loss between the returned cache and persisted data; `WalkWithoutRetention` identifies visited growth without durable coverage growth. Missing, corrupt, empty-root, and oversized checkpoint inputs are rejected by the strict fixture reader. Save failure and publication-epoch rejection must preserve the preceding file bytes. Parent cancellation is checked separately from object-budget exhaustion. Superseded classification is tested separately from either incomplete outcome.

After three interrupted rounds, the fixture overwrites a previously visited object with two versions, deletes another visited object, and creates one more hot object. It then keeps the same four-object budget until the stable namespace is certified. Finishing a sweep that spans different mutation plans must first return partial; a subsequent verification sweep must produce exactly 25 objects, 2 versioned entries, and 34 logical bytes. There is no final unbudgeted sweep.

The new bucket checkpoint binds the persisted bucket incarnation, set layout, publication epoch and tier generation, with the existing source/leader/key-format checks. Its forward sweep records the starting and requested mutation plans separately. Partial sweeps omit the legacy `scan_plan_digest`, so older readers rebuild instead of treating mixed observations as a current complete snapshot. Completed sweeps restore that digest only after covering one mutation plan. Unsupported or missing identities retain the legacy rebuild path. Round-trip, stale identity, invalid cursor and instance-owner tests cover these boundaries. The new metadata remains map-encoded with optional top-level fields.

This fixture bounds object processing after directory enumeration. It does not prove fixed-budget enumeration of arbitrarily wide directories or real process-restart convergence. Those gates require a storage-owned resumable enumeration capability, including its initial construction cost; a readdir offset, an in-memory iterator or a last-name filter is not that capability.

For every saved partial cache, the fixture also passes its progress through the production authenticated remote terminal-frame writer and stream consumer. A remote partial result must remain partial even when its progress reports visited objects. This covers the return-frame contract; it does not execute the remote RPC server, distributed locks, EC quorum persistence, mixed-version peers, process crashes, or fsync durability. The file backend models revision preconditions and persistence errors, not a concurrent object store.

The synthetic namespace contains no customer data. Temporary files are removed with their owning fixture. Rolling back to a reader without the optional checkpoint metadata rebuilds partial coverage; it must not clear quota floors or complete authoritative snapshots. A passing fixture alone does not establish that the field report in [issue #7108](https://github.com/rustfs/rustfs/issues/7108) has been independently reproduced or fixed. A field diagnosis must separately identify the source capture, cycle and leader identity, and decoded bucket/set caches.
