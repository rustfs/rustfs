# Workload Admission Contracts

This document records the `rustfs/backlog#660` PR-05 and PR-07 scheduler
preservation and runtime workload-class contract slice.

## Preservation Coverage

The `rustfs-concurrency` tests pin the current reusable scheduler and
admission-facing behavior before later snapshot extraction:

- Worker slot over-release remains clamped by the configured worker limit.
- Scheduler default buffer and priority thresholds remain unchanged.
- Scheduler priority boundaries remain high below the high threshold, normal at
  both thresholds, and low above the low threshold.
- Backpressure pipe metadata reads preserve buffer capacity and state without
  mutating the manager state.
- `GetObjectQueueSnapshot` preserves saturated, over-available, and zero-total
  permit semantics.

## Workload Class Contract

`WorkloadClass` defines the required future admission categories:

- Foreground read.
- Foreground write.
- Metadata.
- Scanner.
- Repair.
- Replication.

`AdmissionState`, `WorkloadAdmissionSnapshot`, and
`WorkloadAdmissionRegistrySnapshot` define read-only status shapes for later
runtime owners. They do not replace the current scheduler, request guard,
scanner, heal, replication, or ECStore placement behavior.

## Boundary Rules

- `rustfs-concurrency` owns this reusable contract surface.
- The contract does not depend on `rustfs-ecstore` or RustFS binary runtime
  state.
- No scheduler decision logic, queue capacity, Tokio runtime default, scanner
  admission, heal admission, replication admission, placement, membership, or
  NUMA behavior changes are part of this slice.

## Set-Local Snapshot Extraction

The RustFS storage `ConcurrencyManager` now implements
`WorkloadAdmissionSnapshotProvider` for local foreground-read admission:

- `ForegroundRead` reports local disk-read permit usage through
  `GetObjectQueueSnapshot`.
- `active` is the number of disk-read permits currently in use.
- `limit` is the configured maximum concurrent disk reads.
- `queued` remains `None` because the current semaphore does not expose waiter
  counts.
- Scanner, repair, replication, foreground write, and metadata entries remain
  `Unknown` until their owning runtime components expose read-only status.

This is an observation surface only. Permit acquisition, priority assignment,
buffer sizing, storage media detection, request guards, and queue behavior are
unchanged.

## Heal Repair Snapshot Extraction

The RustFS integration layer now exposes a read-only repair admission snapshot
from the heal runtime counters:

- `Repair` reports the current heal active task count.
- `queued` reports the current heal queue length.
- `limit` remains `None` because the configured heal queue and concurrency
  limits live behind the async heal manager state.
- Other workload classes remain `Unknown` in this provider until their owning
  runtime components expose read-only status.

This is an observation surface only. Heal request admission, queue capacity,
priority merge/drop policy, task scheduling, retry handling, and repair
behavior are unchanged.

## Replication Snapshot Extraction

The RustFS integration layer now exposes a read-only replication admission
snapshot from the existing replication pool and queue statistics:

- `Replication` reports active regular, large-object, and MRF worker counts.
- `queued` reports the current site replication queue count when queue stats
  are immediately observable.
- `limit` remains `None` because replication worker limits remain owned by the
  async replication pool and resize policy.
- If the replication runtime has not initialized, or queue stats are currently
  locked, the snapshot reports `Unknown` instead of blocking or guessing.

This is an observation surface only. Replication admission, queue channel
capacity, worker resize behavior, MRF handling, target dispatch, and resync
behavior are unchanged.

## RustFS Runtime Owner Snapshot Extraction

The RustFS integration layer now extends the workload admission registry with
additional read-only owner mappings:

- `ForegroundRead` reuses the storage `ConcurrencyManager` disk-read permit
  snapshot so the RustFS-level provider exposes the same active and limit
  counts as the storage-local provider.
- `Scanner` reports the existing scanner active work-unit counter. When the
  counter is zero, the snapshot remains `Unknown` because the current counter
  cannot distinguish an idle scanner from a scanner that has not initialized.
- `Metadata` reports `Open` once the bucket metadata runtime handle is
  available, and `Unknown` before initialization.
- `ForegroundWrite` remains `Unknown` until a write-specific admission owner
  exposes a read-only surface.

This is an observation surface only. Disk-read permit acquisition, scanner
cycle scheduling, bucket metadata loading, metadata locks, object write paths,
and queue behavior are unchanged.
