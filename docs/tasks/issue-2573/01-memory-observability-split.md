# Task 01: Memory Observability Split

## Goal

Separate anonymous heap, allocator-retained memory, and file/page cache in runtime metrics so benchmark results stop relying on a single opaque `docker stats` number.

## Scope

- add process memory gauges
- add cgroup memory split gauges where available
- add EC in-flight bytes gauge
- add GET buffered-bytes gauge

## Outputs

- runtime metrics for:
  - process resident bytes
  - process virtual bytes
  - cgroup anon bytes
  - cgroup file bytes
  - cgroup active file bytes
  - cgroup inactive file bytes
  - EC inflight bytes
  - GET buffered bytes

## Verification

- metrics appear during runtime
- Linux container builds populate cgroup split fields
- non-Linux targets degrade gracefully
