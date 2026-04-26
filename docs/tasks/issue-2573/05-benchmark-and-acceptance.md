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

## Acceptance

- post-benchmark memory returns close to `3.5GiB`
- `11MiB mixed` no longer remains in the tens-of-GiB range after cooldown
