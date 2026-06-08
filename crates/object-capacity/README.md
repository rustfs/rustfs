# rustfs-object-capacity

`rustfs-object-capacity` is the core object-capacity statistics component in RustFS. It scans local data directories, maintains a capacity cache, triggers incremental refreshes after writes, and provides the admin layer with a used-capacity result that is as inexpensive and resilient as possible.

This crate is not meant to measure total filesystem capacity. Its job is to answer: "How many bytes are currently occupied by RustFS object data?" It makes practical tradeoffs between accuracy, freshness, and scan cost.

## Core Responsibilities

- Scan one or more local data-disk roots and aggregate used bytes and file counts.
- Reduce scan cost on large directories with an "exact prefix + sampled overflow" strategy.
- Return usable degraded results when scans time out, traversal stalls, or some directories fail, instead of failing the entire request immediately.
- Maintain a global `HybridCapacityManager` cache with scheduled refresh, write-triggered refresh, foreground blocking refresh, and background refresh.
- Track which disks were affected by writes so the system can refresh only the dirty subset after a complete per-disk cache is available.
- Emit capacity-related metrics for observability and benchmarks.

## Module Layout

- `src/lib.rs`
  Re-exports `scan_used_capacity_disks`, `CapacityDiskRef`, and `CapacityScanSummary`.
- `src/types.rs`
  Defines scan input/output types, including `CapacityDiskRef`, the internal `CapacityScanResult`, and the public `CapacityScanSummary`.
- `src/scan.rs`
  Implements directory traversal, sampled estimation, timeout/stall detection, multi-disk concurrent scans, and conversion into `CapacityUpdate`.
- `src/capacity_manager.rs`
  Owns caching, write-frequency tracking, singleflight refresh coordination, background tasks, dirty-subset merge logic, and the global singleton manager.
- `src/capacity_scope.rs`
  Tracks "which disks were touched by a write", including token-bound local scopes and the global dirty-scope registry.
- `benches/capacity_scan.rs`
  Exercises the public scan API with benchmark scenarios for exact, sampled, and multi-disk scans.

## Data Model

### `CapacityDiskRef`

```rust
pub struct CapacityDiskRef {
    pub endpoint: String,
    pub drive_path: String,
}
```

This is the minimal unit required for a scan:

- `endpoint` is used to distinguish metrics and logs.
- `drive_path` is the local disk root path.

### `CapacityScanSummary`

```rust
pub struct CapacityScanSummary {
    pub used_bytes: u64,
    pub file_count: usize,
    pub sampled_count: usize,
    pub is_estimated: bool,
    pub had_partial_errors: bool,
    pub scan_duration: Duration,
}
```

Field meanings:

- `used_bytes`: the computed or estimated used capacity.
- `file_count`: the number of regular files traversed.
- `sampled_count`: the number of overflow files sampled after crossing the threshold.
- `is_estimated`: whether the result is estimated instead of exact.
- `had_partial_errors`: whether traversal encountered local errors while still producing a result.
- `scan_duration`: total scan duration.

## Scan Algorithm

The directory scan lives in `scan.rs::get_dir_size_async` and works as follows:

1. Wrap blocking directory traversal in `tokio::task::spawn_blocking` so the async runtime is not blocked.
2. Walk the directory tree with `WalkDir` and count only regular files.
3. If the file count stays below `DEFAULT_MAX_FILES_THRESHOLD` (default `200_000`), add every file size exactly.
4. After crossing the threshold:
   - Keep the first `max_files_threshold` files as an exact prefix.
   - Sample every `sample_rate` file after that and estimate the overflow portion from sampled bytes.
5. Periodically perform progress checks:
   - If total elapsed time exceeds the timeout, attempt to fall back to a sampled estimate.
   - If no file progress is observed within `stall_timeout`, treat the traversal as stalled.
6. If some directory entries or metadata reads fail:
   - As long as at least one disk scan succeeds, return a partial-success result.
   - Mark the result with `had_partial_errors = true`.

### Scan Concurrency

- Multi-disk scans run concurrently through `buffer_unordered`.
- The current hard-coded maximum concurrency is `4` disks.
- A failure on one disk does not immediately stop scans for the others.

### Timeout and Estimation Fallback

This crate is intentionally not "timeout means hard failure":

- If enough sampled data has already been collected, a timeout or stall produces an estimated result.
- Only when no usable estimate is available does the scan return an error.
- This keeps capacity queries useful for large directories, slow disks, and temporary I/O stalls.

### Symlink Handling

- Symlinks are not followed by default: `RUSTFS_CAPACITY_FOLLOW_SYMLINKS=false`.
- If enabled, the scan applies circular-reference detection and a maximum follow depth.
- The default maximum depth is `3`.

## Capacity Cache and Refresh Strategy

`HybridCapacityManager` is the state center of this crate.

### Cached State

- Latest total capacity value `total_used`
- Last refresh time `last_update`
- File count `file_count`
- Estimated/exact flag `is_estimated`
- Data source `DataSource`
- Per-disk cache `disk_cache`
- Dirty-disk set
- Recent 60-second write buckets

### `DataSource`

- `RealTime`
  Foreground real-time refresh when no cache exists yet.
- `Scheduled`
  Background refresh triggered by the scheduled task.
- `WriteTriggered`
  Refresh triggered when write frequency is high and the cache is old enough.
- `Fallback`
  Fallback to externally supplied disk-used capacity when all scans fail.

### Refresh Entry Points

- `refresh_or_join`
  A singleflight foreground refresh. If another refresh is already running, callers join and wait for the shared result.
- `spawn_refresh_if_needed`
  A background refresh. If another refresh is already running, it is skipped.
- `start_background_task`
  Starts two background tasks:
  - the scheduled capacity refresh task
  - the runtime summary logging task

### Singleflight Semantics

`refresh_or_join` and `spawn_refresh_if_needed` use a `watch` channel to coordinate refresh cycles:

- Only one leader performs the actual refresh at a time.
- Joiners share the same published result after the leader completes.
- Panics inside the refresh function are caught and converted into errors so callers do not crash with the leader.

## Dirty Scope and Subset Refresh

One of the main optimizations in this crate is "refresh only the disks dirtied by writes".

### Scope Propagation

`capacity_scope.rs` provides two ways to propagate dirty disks:

- token scope
  - The caller first binds a write operation to a disk set with `record_capacity_scope(token, scope)`.
  - Later, `record_write_operation_with_scope_token(Some(token))` consumes that scope and marks the disks dirty.
- global dirty scope
  - `record_global_dirty_scope(scope)` records dirty disks directly in the global registry.
  - The manager drains and merges them during `get_dirty_disks()`.

### When Dirty-Subset Refresh Is Allowed

Refreshing only dirty disks is safe only when:

- `disk_cache_complete == true`
- which means the system has already completed at least one full refresh without partial errors
- and the per-disk cache is fully populated

If the per-disk cache is incomplete, or there are no dirty disks, the system falls back to a full refresh.

### Merge Rules After a Subset Refresh

- On a successful full refresh, `per_disk` replaces the entire `disk_cache`.
- On a successful dirty-subset refresh, only the affected per-disk entries are updated.
- The total capacity is recomputed from the updated `disk_cache` instead of trusting the subset sum directly.
- If a dirty-subset refresh reports partial errors, that cycle fails and the caller falls back to a full refresh to recover consistency.

## Relationship to the RustFS Main Flow

This crate provides capacity primitives only. The actual RustFS integration lives in `rustfs/src/capacity/service.rs`.

The high-level flow is:

1. Startup calls `init_capacity_management_for_local_disks()`.
2. It collects all local disks and calls `capacity_manager::start_background_task(...)`.
3. Admin used-capacity queries first try the `HybridCapacityManager` cache.
4. If the cache is fresh enough, the cached value is returned directly.
5. If the cache is stale but still acceptable, the stale value is served and a background refresh is triggered.
6. If the cache is very stale and the write rate is high, the request blocks on a foreground refresh.
7. If the initial real-time scan fails, the service falls back to externally supplied disk-used capacity and stores it as `Fallback`.

`crates/ecstore/src/set_disk.rs` is responsible for recording capacity scopes during object writes, heal operations, data movement, and related flows, so this crate can learn which disks were affected.

## Public API

### 1. Direct Scan

This is useful for benchmarks, operational tooling, or isolated validation.

```rust
use rustfs_object_capacity::{CapacityDiskRef, scan_used_capacity_disks};

let disks = vec![
    CapacityDiskRef {
        endpoint: "node-a".to_string(),
        drive_path: "/data/disk1".to_string(),
    },
];

let summary = scan_used_capacity_disks(&disks).await?;
println!(
    "used={} files={} estimated={}",
    summary.used_bytes, summary.file_count, summary.is_estimated
);
# Ok::<(), Box<dyn std::error::Error>>(())
```

### 2. Use the Global Manager

This is useful for in-service caching and refresh orchestration.

```rust
use rustfs_object_capacity::capacity_manager::{DataSource, get_capacity_manager};

let manager = get_capacity_manager();

if let Some(cached) = manager.get_capacity().await {
    println!("cached bytes={}", cached.total_used);
}

manager.record_write_operation().await;

let _ = manager
    .refresh_or_join(DataSource::Scheduled, || async {
        rustfs_object_capacity::scan::refresh_capacity_with_scope(
            vec![rustfs_object_capacity::CapacityDiskRef {
                endpoint: "node-a".to_string(),
                drive_path: "/data/disk1".to_string(),
            }],
            false,
        )
        .await
    })
    .await;
```

### 3. Propagate a Dirty Scope

```rust
use rustfs_object_capacity::capacity_scope::{
    CapacityScope, CapacityScopeDisk, record_capacity_scope,
};
use rustfs_object_capacity::capacity_manager::get_capacity_manager;
use uuid::Uuid;

let token = Uuid::new_v4();
record_capacity_scope(
    token,
    CapacityScope {
        disks: vec![CapacityScopeDisk {
            endpoint: "node-a".to_string(),
            drive_path: "/data/disk1".to_string(),
        }],
    },
);

get_capacity_manager()
    .record_write_operation_with_scope_token(Some(token))
    .await;
```

## Environment Variables and Defaults

The configuration constants are defined in `crates/config/src/constants/capacity.rs`.

| Environment Variable | Default | Description |
| --- | --- | --- |
| `RUSTFS_CAPACITY_SCHEDULED_INTERVAL` | `120s` | Scheduled refresh interval |
| `RUSTFS_CAPACITY_WRITE_TRIGGER_DELAY` | `5s` | Debounce delay after writes |
| `RUSTFS_CAPACITY_WRITE_FREQUENCY_THRESHOLD` | `5` | Recent 60-second write-frequency threshold |
| `RUSTFS_CAPACITY_FAST_UPDATE_THRESHOLD` | `30s` | Cache age required before fast refresh is considered |
| `RUSTFS_CAPACITY_MAX_FILES_THRESHOLD` | `200000` | Exact-count file threshold |
| `RUSTFS_CAPACITY_STAT_TIMEOUT` | `3s` | Base scan timeout |
| `RUSTFS_CAPACITY_SAMPLE_RATE` | `200` | Overflow-file sampling interval |
| `RUSTFS_CAPACITY_METRICS_INTERVAL` | `600s` | Runtime summary emission interval |
| `RUSTFS_CAPACITY_FOLLOW_SYMLINKS` | `false` | Whether to follow symlinks |
| `RUSTFS_CAPACITY_MAX_SYMLINK_DEPTH` | `3` | Maximum symlink follow depth |
| `RUSTFS_CAPACITY_ENABLE_DYNAMIC_TIMEOUT` | `true` | Whether to enable dynamic timeout scaling |
| `RUSTFS_CAPACITY_MIN_TIMEOUT` | `2s` | Dynamic-timeout lower bound |
| `RUSTFS_CAPACITY_MAX_TIMEOUT` | `15s` | Dynamic-timeout upper bound |
| `RUSTFS_CAPACITY_STALL_TIMEOUT` | `20s` | Stall-detection threshold |

### Configuration-Caching Note

In non-test builds, configuration is cached behind `OnceLock`:

- Environment variables are effectively read once on first access.
- Updating `RUSTFS_CAPACITY_*` during runtime usually does not take effect immediately.
- A process restart is normally required to apply configuration changes reliably.

## Metrics

This crate reports multiple metric families to `rustfs-io-metrics::capacity_metrics`, including:

- cache hit / miss / served state
- refresh inflight, joiners, and success / error outcomes
- current capacity bytes
- write frequency
- dirty-disk count
- per-disk scan duration, sampling mode, timeout fallback, stall detection, and symlink statistics

So this crate is both a capacity-calculation component and an important producer of runtime observability data.

## Benchmarks

Run the benchmark suite with:

```bash
cargo bench -p rustfs-object-capacity --bench capacity_scan
```

Current benchmark scenarios:

- `capacity_scan_exact`
  Single-disk exact scan over 10k files.
- `capacity_scan_sampled`
  Single-disk scan over 202,048 files that triggers sampled estimation.
- `capacity_scan_multi_disk`
  Four-disk exact scan with mixed directory sizes.

## Known Boundaries and Tradeoffs

- It sums file sizes under RustFS object-data directories; it is not a full replacement for filesystem-level `du`.
- Estimated mode prioritizes bounded cost and usable results over perfect per-run precision.
- Dirty-subset refresh is safe only after a complete per-disk cache has been established.
- Partial errors intentionally try to return a degraded result, which improves availability but means callers should pay attention to `had_partial_errors`.
- Symlink following is disabled by default for safety and determinism.

## Relevant Source Entry Points

- [src/lib.rs](./src/lib.rs)
- [src/scan.rs](./src/scan.rs)
- [src/capacity_manager.rs](./src/capacity_manager.rs)
- [src/capacity_scope.rs](./src/capacity_scope.rs)
- [src/types.rs](./src/types.rs)
- [benches/capacity_scan.rs](./benches/capacity_scan.rs)
- [../../rustfs/src/capacity/service.rs](../../rustfs/src/capacity/service.rs)
- [../config/src/constants/capacity.rs](../config/src/constants/capacity.rs)
