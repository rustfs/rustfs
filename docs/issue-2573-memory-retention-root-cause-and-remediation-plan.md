# Issue #2573 Memory Retention Root Cause and Remediation Plan

- Issue: https://github.com/rustfs/rustfs/issues/2573
- Updated analysis date: 2026-04-26
- Scope: current mainline code, current runtime behavior, allocator/platform-specific retention

## 1. Problem Statement

The current memory problem is no longer related to `RUSTFS_OBJECT_CACHE_ENABLE`; that feature has already been removed.

The real issue is **memory retention after benchmark load**, not just transient peak usage:

- baseline after restart: about `3.5GiB`
- `mixed` with `4KiB` objects: about `5.11GiB`
- `mixed` with `11MiB` objects: about `70.11GiB`
- benchmark ends, but memory does not naturally fall back to the expected steady-state baseline

This means the current behavior combines:

1. heap amplification during request execution
2. allocator retention after logical frees
3. file/page cache retention after large object read/write workloads

## 2. Platform and Allocator Constraints

Current runtime constraint:

- macOS on Apple Silicon (`M5 Max`) in the user-reported environment
- current RustFS allocator path is **not jemalloc** on this target
- only `linux + x86_64 + gnu` uses `tikv-jemallocator`
- all other non-Windows targets use `TracingAllocator<mimalloc::MiMalloc>`

Code references:

- allocator selection: `rustfs/src/main.rs`
- generic profiling path: `rustfs/src/profiling.rs`

Implications:

1. The repository currently has allocator-aware active reclaim only on the jemalloc path.
2. The mimalloc path supports profiling/sampling, but not allocator reclaim.
3. Therefore, even when objects are logically released, RSS may remain high due to retained allocator segments.

## 3. Root Cause Model

### 3.1 Not a single leak, but a three-layer retention stack

#### Layer A: Request-path heap amplification

Write path:

- `SetDisks::put_object()`
- `Erasure::encode()`
- `mpsc<Vec<Bytes>>` in-flight encoded shard queue

Key facts:

- erasure block size is fixed at `1MiB`
- single-node 4-disk default layout is `2 data + 2 parity`
- each `1MiB` logical block becomes about `2MiB` expanded shard payload
- fixed queue depth `8` means about `16MiB` encoded in-flight data per active request path

This explains peak anonymous heap pressure, but **cannot explain tens of GiB alone**.

#### Layer B: Large-object file/page cache retention

Current object write/read paths use regular buffered file I/O:

- no effective object-path `O_DIRECT`
- no object-range `posix_fadvise(..., DONTNEED)` after large reads/writes

Consequences:

- `11MiB` objects generate significant page cache residency
- `mixed` workload repeatedly writes, reads, and deletes large files
- file cache remains charged to the container/cgroup after benchmark completion

This is the primary explanation for the current `11MiB mixed -> 70.11GiB` result.

#### Layer C: Allocator retention after free

On the current target, RustFS runs on mimalloc instead of jemalloc.

Consequences:

- freed request buffers do not guarantee immediate RSS drop
- allocator segments can remain reserved
- current code has no explicit mimalloc collection/purge path

This is the main reason why memory does not naturally fall back to the `3.5GiB` steady-state baseline after load.

### 3.2 Why the old "43GiB" estimate is not accurate

The old estimate assumes:

- whole object size as the amplification unit
- direct multiplication by queue depth and disks

But the real hot-path unit is **EC block size (`1MiB`)**, not whole object size.

Therefore:

- fixed depth `8` is an important amplifier
- but it is only one part of the story
- it does not fully explain `21.5GiB`
- it definitely does not explain the current `70.11GiB`

## 4. Confirmed Current-Code Findings

### 4.1 GET small-object whole-buffer behavior still exists

Objects below the seek-support threshold are fully buffered in memory.

Current threshold:

- `DEFAULT_OBJECT_SEEK_SUPPORT_THRESHOLD = 10 * 1024 * 1024`

This contributes to `4KiB mixed -> 5.11GiB`.

### 4.2 Current "zero-copy" path is still mmap + copy

The Unix read path uses `mmap`, then copies requested bytes into `Bytes`.

This means:

- file cache is populated
- user-space heap copy is also created
- large read workloads incur both cache and heap pressure

### 4.3 Current inflight-byte cap exists, but default budget is too weak

`RUSTFS_ERASURE_ENCODE_MAX_INFLIGHT_BYTES` was added, but the default `32MiB` budget often still resolves to the historical effective depth cap of `8`.

So the mechanism exists, but the default behavior still allows excessive heap amplification on common topologies.

## 5. Remediation Objectives

The objective is not "make memory zero".

The objective is:

1. control heap amplification during load
2. reclaim allocator-retained memory after load
3. reclaim file/page cache left by large object benchmarks
4. return close to the stable steady-state baseline (`~3.5GiB`) without restart

## 6. Execution Strategy

Implementation must proceed in the following order.

### Phase 1: Memory Observability Split

Add visibility for:

- process resident memory
- process virtual memory
- allocator-observable memory state
- cgroup anonymous memory
- cgroup file memory
- cgroup active/inactive file cache
- EC encode in-flight bytes
- GET whole-buffered bytes

Reason:

- we must stop treating `docker stats` as a single-source truth
- we need to separate heap from file cache before optimizing reclaim

### Phase 2: Mimalloc Active Reclaim

Introduce allocator control for non-jemalloc targets:

- add a mimalloc-specific collect/reclaim path
- expose a runtime trigger for manual benchmark cooldown
- optionally add idle-time background reclaim

Reason:

- current mimalloc path can sample memory but cannot proactively release retained segments

### Phase 3: Large-Object Page Cache Reclaim

Introduce range/file-level reclaim for large object workloads:

- write-complete reclaim
- sequential-read-complete reclaim
- feature-gated and benchmark-friendly behavior

Reason:

- this is the most likely primary cause of `11MiB mixed -> 70.11GiB`

### Phase 4: Heap Amplification Control

Tighten request-path memory amplification:

- reduce effective EC inflight defaults
- make seek-support threshold configurable
- fix misleading zero-copy saved-bytes accounting
- export current buffered/inflight gauges

Reason:

- necessary to reduce anonymous heap pressure
- but should follow observability and reclaim foundations

### Phase 5: Verification and Acceptance

Acceptance must verify both:

1. peak behavior during load
2. decay behavior after load ends

## 7. Verification Matrix

### Profile A: Small-object mixed

- object sizes: `4KiB`
- method: `mixed`
- baseline focus: anonymous heap and allocator retention

Expected outcome:

- post-benchmark memory should fall near `3.5GiB`
- file-cache metrics should not dominate

### Profile B: Large-object mixed

- object sizes: `11MiB`
- method: `mixed`
- baseline focus: file/page cache retention + allocator retention

Expected outcome:

- file-memory metrics should clearly spike during load
- post-benchmark memory should fall near `3.5GiB`
- page-cache reclaim should account for the majority of the observed drop

## 8. Acceptance Criteria

The solution is accepted when all of the following are true:

1. `4KiB mixed` no longer remains materially above steady-state after cooldown.
2. `11MiB mixed` no longer remains in the tens-of-GiB range after cooldown.
3. allocator and file-cache metrics clearly explain where the memory went and how it came back down.
4. cooldown does not require process restart.

## 9. Deliverables

This plan is paired with the following execution task files:

- `docs/tasks/issue-2573/01-memory-observability-split.md`
- `docs/tasks/issue-2573/02-mimalloc-active-reclaim.md`
- `docs/tasks/issue-2573/03-large-object-page-cache-reclaim.md`
- `docs/tasks/issue-2573/04-heap-amplification-control.md`
- `docs/tasks/issue-2573/05-benchmark-and-acceptance.md`

## 10. Summary

The deep root cause is:

- **not object cache**
- **not only fixed EC queue depth**
- **not only allocator retention**

It is the combined effect of:

1. heap amplification from EC and small-object buffering
2. mimalloc retention without active reclaim
3. large-object page cache retention without object-level drop policy

The implementation order must follow:

1. observability split
2. allocator reclaim
3. page-cache reclaim
4. heap amplification tuning
5. benchmark validation
