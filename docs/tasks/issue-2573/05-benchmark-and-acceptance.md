# Task 05: Benchmark and Acceptance

## Goal

Validate both peak memory behavior and post-benchmark cooldown behavior without restart.

## Profiles

- `4KiB mixed`
- `11MiB mixed`

## Required Evidence

- process memory gauges
- cgroup anon/file split gauges
- allocator cooldown behavior
- benchmark memory decay after load

## Dashboard Queries To Watch

- `rustfs_system_process_resident_memory_bytes` or `rustfs_memory_process_resident_bytes`
- `rustfs_memory_process_virtual_bytes`
- `rustfs_memory_cgroup_anon_bytes`
- `rustfs_memory_cgroup_file_bytes`
- `rustfs_delete_tail_activity_total_inflight_current`
- `rustfs_memory_allocator_reclaim_scanner_activity_current`
- `rustfs_memory_allocator_reclaim_heal_activity_current`
- `rustfs_memory_allocator_reclaim_reclaimable_work_current`
- `rustfs_memory_allocator_reclaim_idle_streak`

## Execution Checklist

1. Run `4KiB mixed` and record:
   - process RSS peak
   - process virtual peak
   - reclaim idle streak after load
   - whether delete-tail / scanner / heal activity drains to zero
2. Run `11MiB mixed` and record:
   - process RSS peak
   - anon/file split evolution
   - reclaim idle streak after load
   - whether file memory drops after cooldown
3. Run `delete-only` and record:
   - delete tail inflight
   - scanner activity
   - heal activity
   - reclaim skipped reasons
4. Confirm cooldown happens without restart.

## Acceptance

- post-benchmark memory returns close to `3.5GiB`
- `11MiB mixed` no longer remains in the tens-of-GiB range after cooldown
- `delete-only` no longer shows unbounded post-benchmark tail growth without corresponding background-activity evidence
