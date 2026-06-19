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
