# Task 04: Heap Amplification Control

## Goal

Reduce anonymous heap pressure during load by tightening current request-path amplification points.

## Scope

- lower effective EC inflight defaults
- add configurable seek-support threshold
- export current buffered/inflight gauges
- fix misleading zero-copy saved-bytes accounting

## Verification

- reduced anonymous-memory peaks in `4KiB` and `11MiB` mixed profiles
- no unacceptable throughput regression
