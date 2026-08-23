# Scanner/Heal admission Phase 0 baseline

This document records the current entry points and safety boundaries for backlog #1939. It is an inventory and test contract, not a lease design. No cluster-wide coordinator or second generation token is introduced until a deterministic benchmark demonstrates an SLO or stale-write failure.

## Entry-point inventory

| Work | Entry point | I/O and current guard | Fallback/namespace semantics |
| --- | --- | --- | --- |
| Scanner read/list | `crates/scanner/src/scanner_io/io_disk.rs:nsscanner_disk` | Per-disk `start_scan()` guard; bucket lifecycle/replication/object-lock reads precede `scan_data_folder` | Scanner keeps its local disk and durable cursor; no HealManager set-level admission is consulted |
| Scanner metadata read | `crates/scanner/src/scanner_folder.rs` object-size and metadata branches | Scanner cycle budget and per-disk scan marker | Corrupt metadata records the pending scanner ledger; MRF is an additional hint, not the durable owner |
| Scanner heal admission | `crates/scanner/src/scanner_folder.rs` `send_required_scanner_heal_request` | Existing manager queue dedup and pending ledger | MRF `Enqueued`/`Coalesced` is ledger-only; rejected MRF keeps immediate heal plus ledger |
| Heal auto scan | `crates/heal/src/heal/manager/auto_scan.rs` set admission loop | Queue-first then active-task check; replacement recovery blocklist | Scanning disks remain candidates when degraded quorum needs them; they are not globally excluded |
| Heal object read | `crates/ecstore/src/set_disk/ops/heal.rs` `heal_object` | Namespace write lock unless `no_lock`; reads file info before commit | Namespace lock is object-scoped and does not claim scanner cycle ownership |
| Disk selection | `crates/ecstore/src/set_disk/ops/locking.rs` candidate selection | Healing disks are ordered after new disks; scanning disks may remain candidates | Degraded/quorum fallback is preserved |
| Data movement | Existing storage-owned movement/publication generation (#1905/#1942) | This issue does not add a second coordinator | Future admission must validate the storage generation at the final commit |

## Baseline contract

The deterministic baseline in `scanner_heal_admission_baseline.rs` encodes the investigation matrix only: ScannerRead+HealRead may overlap, HealWrite conflicts with scanner reads, DataMovementWrite conflicts with all work, and independent set identities remain concurrent. It does not claim that production currently enforces the matrix.

The production facts that must be measured before Phase 1 are scanner p99, heal p99, cursor/checkpoint delay, queue and pending-ledger depth, and starvation by set. The benchmark matrix must include restart recovery, degraded quorum/scanning-disk fallback, urgent replacement heal, and at least two independent sets.

The executable fixture uses a fixed eight-sample restart/degraded sequence so the baseline is reproducible without wall-clock noise: two sets each receive ScannerRead, HealRead, HealWrite and a follow-up ScannerRead. Its expected synthetic p99 is 420 microseconds, maximum modeled backlog is 2, two HealWrite samples are deferred, and the independent second set still services three reads. These are fixture values, not production SLO claims; production benchmark output must replace them with measured p99, backlog and per-set wait distributions.

The inventory test reads the current source files and asserts the named guards/fallback branches are still present (`start_scan`, pending-ledger admission, Heal queue/active checks, namespace `get_write_lock`, and scanning-disk re-append). A source rename or guard removal therefore fails the baseline instead of silently leaving stale documentation.

Commit-time generation-fencing, lease-expiry, and lock-order tests are intentionally deferred until a Phase-0 fixture demonstrates a stale write or an SLO violation; arithmetic-only placeholders would stay green if production paths regressed.

If a future fixture demonstrates stale destructive writes, the fix must extend the storage-owned generation/admission primitive and validate the token at the final metadata/format/delete commit. Cancellation or a local lease alone is not a fence.
