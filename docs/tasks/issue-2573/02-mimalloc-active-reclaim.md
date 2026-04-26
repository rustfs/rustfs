# Task 02: Mimalloc Active Reclaim

## Goal

Provide an active reclaim path for non-jemalloc targets so freed allocations can be returned closer to steady-state after benchmark load.

## Scope

- add allocator control abstraction
- add mimalloc-specific collect/reclaim call path
- add runtime trigger for manual cooldown
- optionally add idle-time reclaim loop

## Verification

- post-benchmark allocator-retained memory drops without restart
- no-op behavior remains safe on unsupported targets
