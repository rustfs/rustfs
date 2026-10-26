# Workload Admission Contracts

**Use this when:** adding a workload class or snapshot provider, consuming admission state from a background job, or deciding whether a job can "join" admission (it cannot; see Observation Surface Only).
**Source of truth:** `WorkloadClass`, `AdmissionState`, `WorkloadAdmissionSnapshot`, `WorkloadAdmissionRegistrySnapshot`, `WorkloadAdmissionSnapshotProvider`, and `foreground_pressure` in `crates/concurrency/src/workload.rs`; the provider and consumer files named below.

## Contract Shapes

`rustfs-concurrency` owns the read-only shapes. `WorkloadClass` enumerates the admission categories (the variants are the source of truth); `AdmissionState`, `WorkloadAdmissionSnapshot`, and `WorkloadAdmissionRegistrySnapshot` are status shapes for runtime owners to fill. They do not replace the scheduler, request guards, scanner, heal, replication, or ECStore placement behavior. `GetObjectQueueSnapshot` permit semantics (saturated, over-available, zero-total) and worker-slot over-release clamping are pinned by `rustfs-concurrency` tests; scheduler buffer/priority behavior is pinned by `rustfs-io-core` and `rustfs/src/storage/concurrency/` tests.

## Class To Provider Table

| Class | Provider (`impl WorkloadAdmissionSnapshotProvider`) | `active` / `queued` / `limit` source | Reports `Unknown` when |
|---|---|---|---|
| `ForegroundRead` | `ConcurrencyManager` in `rustfs/src/storage/concurrency/manager.rs` (source of truth); re-exposed unchanged by the RustFS runtime provider | disk-read permits in use / observed pending primary-permit acquisitions (`None` after observation-counter overflow) / configured max concurrent disk reads | the storage registry has no entry |
| `ForegroundWrite` | `ConcurrencyManager` in `rustfs/src/storage/concurrency/manager.rs` (source of truth); re-exposed unchanged by the RustFS runtime provider | allocated/reserved permit units under automatic sizing, request-count permits for explicit/strict limits, or the legacy active-write counter / observed pending PUT/multipart permit acquisitions (zero for policies with no gate, `None` after observation-counter overflow) / matching unit budget or request-count limit | the storage registry has no entry |
| `Metadata` | `RustFsWorkloadAdmissionSnapshotProvider` in `rustfs/src/workload_admission.rs` | `Open` once the bucket metadata runtime handle exists; no counts | bucket metadata runtime not initialized |
| `Scanner` | same | scanner active work-unit counter / none / configured set-scan limit when nonzero | scanner runtime not initialized |
| `Repair` | same | heal active tasks / heal queue length / `None` (limits live behind the async heal manager state) | heal manager not initialized |
| `Replication` | same | active regular + large-object + MRF workers / site replication queue count / `None` (limits owned by the async pool and resize policy) | replication runtime not initialized, or queue stats currently locked |

Pending counts belong to the acquisition futures, not semaphore introspection: an immediately ready acquisition never enters the counter. Cancellation, timeout, close and successful acquisition release the observation. Read counts include the borrowed, owned, prioritized and primary GET paths; the degraded lane remains a nonblocking try-acquire. Write counts cover both ordinary and bounded multipart waits without changing the multipart queue limit. Counts and permit use are sampled independently, not a transactional queue snapshot; counter overflow is unknown until process restart.

## Observation Surface Only

This registry is an observation surface only. Providers do not acquire permits or change priority, buffer sizing, storage media detection, request guards, queue capacity, heal priority merge/drop policy, replication worker resize and MRF handling, scanner cycles, bucket metadata locks or object writes. A consumer must use its own admission owner; a snapshot is not a reservation (see [kms-bulk-rekey-contract.md](kms-bulk-rekey-contract.md)). The opt-in heal node controller consumes this registry as described in [scanner runtime controls](../operations/scanner-runtime-controls.md#experimental-heal-node-admission).

Consumers that read the snapshot to self-throttle exist, and they do not change the owners' decisions:

| Consumer | File | Behavior |
|---|---|---|
| Data-movement backpressure (decommission, rebalance) | `crates/ecstore/src/data_movement/backpressure.rs` (`wait_for_data_movement_admission`, `foreground_pressure`) | Delays the next data-movement step while `ForegroundRead` or `ForegroundWrite` usage exceeds the configured high-water percent. ECStore receives the provider through `set_workload_admission_snapshot_provider` (`crates/ecstore/src/lib.rs`), published from `rustfs/src/startup_background.rs`; with no provider the step is admitted immediately. |
| Heal manager mainline throttle | `crates/heal/src/heal/manager.rs` (`new_with_workload_provider`) | When `mainline_throttle_enable` is set, defers heal work while `ForegroundRead` or `ForegroundWrite` utilization exceeds the configured high-water percents; with no provider or the throttle disabled, heal pacing is unchanged. |
| Scanner sleeper and scan fan-out | `crates/scanner/src/workload_admission.rs`, `crates/scanner/src/sleeper.rs`, and `crates/scanner/src/scanner_io/guards.rs` | Reads the same provider published from `rustfs/src/startup_background.rs` and combines it with scanner-local foreground read guards. Foreground activity increases scanner sleeps and reduces set/disk scan fan-out to one; cycle budgets still own object, directory, and duration limits. With no provider, scanner keeps the legacy local foreground-read behavior. |

## Boundary Rules

- `rustfs-concurrency` owns the contract surface and does not depend on `rustfs-ecstore` or RustFS binary runtime state.
- Adding a class or provider changes no scheduler decision logic, queue capacity, Tokio runtime default, scanner/heal/replication admission, placement, membership, or NUMA behavior.
- Providers report `Unknown` rather than blocking or guessing when their owner is uninitialized or its stats are not immediately observable.

## Provider Composition

`WorkloadAdmissionRegistrySnapshot::overlay` composes provider-owned registries without mutating runtime owners: the storage concurrency provider is the source of truth for `ForegroundRead`; the RustFS runtime owner provider overlays metadata, scanner, repair, replication, and foreground-write status on top; matching classes are replaced by the later snapshot and new classes are appended without reordering. `workload_admission_registry_snapshot` in `rustfs/src/workload_admission.rs` is the single composed registry consumers read.
