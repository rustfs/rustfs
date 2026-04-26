# Task 03: Large-Object Page Cache Reclaim

## Goal

Prevent large-object mixed workloads from leaving tens of GiB of file/page cache charged after benchmark completion.

## Scope

- add write-complete reclaim for large object files
- add sequential-read-complete reclaim for large object files
- keep behavior feature-gated and benchmark-friendly

## Verification

- large-object `mixed` workloads show file-memory drop after cooldown
- no correctness regression for object reads/writes
