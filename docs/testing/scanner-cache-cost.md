# Scanner Cache Cost Profile

The `cache_cost_profile_preserves_checkpoint_and_counts` test isolates the real cache operations used by the scanner: full clone, `copy_with_children`, checked flattening, MessagePack encoding, and `save_with_revisions_for_epoch`. It does not run a namespace walker or the scanner scheduler. An unchanged-cache save is deliberately requested to measure its cost, not to claim that production always saves cold buckets.

```sh
cargo test -p rustfs-scanner --lib cache_cost_profile -- --list
RUST_MIN_STACK=4194304 cargo test -p rustfs-scanner --lib cache_cost_profile -- --nocapture
env -u RUSTFLAGS -u CARGO_ENCODED_RUSTFLAGS \
  CARGO_PROFILE_TEST_OPT_LEVEL=0 CARGO_PROFILE_DEV_OPT_LEVEL=0 \
  RUSTFS_CACHE_COST_SOURCE="$(git rev-parse HEAD)" \
  RUSTFS_CACHE_COST_TREE="$(git rev-parse HEAD^{tree})" \
  RUST_MIN_STACK=4194304 RUSTFS_CACHE_COST_PROFILE=1 \
  cargo test -p rustfs-scanner --lib cache_cost_profile -- --nocapture
```

The default positive control has 64 object entries and one sample for each of unchanged, small-dirty, and all-dirty caches. Explicit profiling uses 1,024, 4,096, and 16,384 object entries, each with five samples in all three scenarios. Small-dirty updates one percent of leaves, with a minimum of one; all-dirty updates every leaf. Each synthetic object initially accounts for two versions and 4,096 logical bytes. These are cache metadata fixtures, not uploaded S3 bodies. Small-dirty snapshots also carry a partial flag and resume marker. No test is ignored, and wall-time thresholds do not determine correctness.

Every measured result is checked outside its timing interval: clone and subtree copy retain every field and entry; flattening yields exact object/version/byte counts; encoding reloads the same structure; saves write both main and backup and reload the same checkpoint. A stale revision with conflicting content must fail without replacing the preceding main cache. This checks the fixture's revision contract, not distributed CAS or publication-authority behavior.

`CACHE_COST` JSON rows contain:

The explicit profile command is an unoptimized Cargo test/debug run (`opt-level=0`), not a release build. Run from a clean worktree and retain the command, source SHA/tree, compiler version and relevant Cargo configuration with the raw rows. Each row records compile-time assertion mode, visible optimization/flag overrides and supplied source identifiers. Null build fields mean unrecorded, not inferred defaults; these fields alone do not discover every Cargo configuration source. Debug phase ratios are not production hotspot evidence and cannot justify a runtime optimization or close the performance task. No release rebuild is required for this bounded diagnostic.

| Field | Meaning |
|---|---|
| `clone`, `copy_with_children`, `checked_flatten`, `encode` | Phase wall-clock p50 and maximum nanoseconds; setup, validation, and disposal are excluded. |
| `save_inclusive` | Actual save entry-point wall time, including its own encoding, buffer copies, admission checks, and both backend calls. This overlaps the independently measured encode operation. |
| `memory_backend_ingest` | Sum of time inside the two counted in-memory backend puts, including stream consumption and revision checking. It is part of `save_inclusive`, not an additional cost. |
| `cache_wire_bytes` | Full snapshot's actual MessagePack size; not heap allocation, cloned bytes, or retained S3 payload bytes. |
| `changed_entry_wire_bytes` | Sum of serialized changed leaf entries, excluding keys, ancestors and metadata; a diagnostic denominator, not a durable-progress proof. Zero in the unchanged scenario. |
| `save_body_bytes_per_sample` | Bytes consumed by both successful main/backup put streams. It does not include network framing, erasure shards or retries. |
| `retained_cache_entries` | Structurally verified cache entries including the root, not newly proven namespace coverage. |

The fixture has two memory slots capped at 32 MiB each, at most 16,384 leaves, at most five samples per case, and nine profile rows. Oversized wire data and unknown configuration fail. No sample history grows with runtime and no permanent service starts. Profile runs must be exclusive of builds and other benchmarks; otherwise label the measurements exploratory/noisy. The default debug build is a diagnostic, not release throughput evidence. Repeated identical phases can benefit from warm allocator and CPU caches; the test does not establish absence of quadratic growth or bounded production RSS.

Use the existing [scanner ABBA harness](../../scripts/scanner_abba.py) and [benchmark runbook](../operations/scanner-benchmark-runbook.md) for deployment comparisons. This microprofile does not supply deployment ABBA, a flamegraph, allocation attribution, syscall/fsync latency, remote RPC, erasure persistence, process-crash recovery, or a performance improvement. Only measured evidence can justify a separately reviewed runtime optimization; serialization, partial/complete proof, and persistence boundaries remain unchanged here.
