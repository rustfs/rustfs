# Placement And Repair Invariants

**Use this when:** changing anything that resolves an object to a pool, set, or disk, or that admits scanner or heal work; these are the behaviors later scheduler or topology work must preserve.
**Source of truth:** `Sets::get_disks_by_key` / `get_hashed_set_index` in `crates/ecstore/src/core/sets.rs`; `DistributionAlgoVersion` in `crates/ecstore/src/layout/format.rs`; `crc_hash` / `sip_hash` in `crates/utils/src/hash.rs`; `ScannerCycleBudget` in `crates/scanner/src/scanner_budget.rs`; `crates/scanner/src/scanner_heal_admission_baseline.rs`.

## Object To Set Hash Rule

Objects reach a set through `Sets::get_disks_by_key`, which calls `get_hashed_set_index` on the object key:

- `DistributionAlgoVersion::V1` uses `crc_hash(input, set_count)`.
- `DistributionAlgoVersion::V2` and `V3` use `sip_hash(input, set_count, format_id_bytes)`.
- The format ID is part of the V2/V3 distribution seed, so changing the seed, object key, set count, or algorithm changes placement.

Preservation rule: every object read, write, list, heal, repair, and decommission path that resolves a set for an existing object must preserve the same object key and format distribution algorithm.

## Pool, Set, And Disk Assignment Boundary

Pool selection is separate from set hashing:

- Existing objects are discovered across pools and resolved to the best current pool candidate before reads or updates continue.
- New object writes select an available pool from current per-pool free-space inputs after suspended or rebalancing pools are skipped.
- Set selection inside a pool uses the object-to-set hash rule above.
- Disk index assignment comes from endpoint and format metadata, not from a scheduler decision.

Boundary rule: schedulers may influence admission, worker concurrency, or buffer sizing, but they must not rewrite pool, set, or disk indexes.

## Readiness And Lock Quorum Boundary

Runtime readiness checks storage and lock health independently:

- Storage readiness requires every observed set to meet write quorum based on the set drive count and storage class data/parity shape.
- Lock readiness aggregates per-set lock-client host quorum and fails fast if any set loses quorum.
- Object and bucket mutations acquire namespace locks through the existing storage lock wrappers before changing object or bucket state.

Boundary rule: readiness and lock quorum must stay set-aware. A global healthy disk count or global connected-host count is not sufficient when any individual set is below quorum.

## Scanner Budget Preservation

Scanner cycles are bounded by `ScannerCycleBudget`:

- Runtime budget cancels the child token after the configured duration.
- Object budget cancels after the configured object count.
- Directory budget rejects additional directories and cancels with the directories reason.
- Partial-cycle metrics and checkpoints use the budget reason.

Preservation rule: later scheduler work can change how scan cycles are admitted only if it preserves the budget reason, checkpoint reason, and child-token cancellation behavior.

## Heal Admission Preservation

Scanner and background repair work enter the heal manager through explicit admission:

- Scanner object heal requests are low priority and may be accepted, merged, rejected as full, or dropped.
- Required/high-priority heal candidates escalate on non-admission instead of silently disappearing.
- Heal queue admission deduplicates queued and active work unless the request explicitly forces admission.
- Full queues can drop low-priority work or displace lower-priority work for a higher-priority request according to current manager rules.

Preservation rule: repair scheduling changes must keep admission outcomes observable and must not convert rejected or dropped repair work into silent success.

### Scanner/heal admission entry points

No cluster-wide coordinator or second generation token exists for scanner/heal admission; each entry point keeps its own guard. `crates/scanner/src/scanner_heal_admission_baseline.rs` `include_str!`s the scanner sources and asserts the named guards are still present, so a rename or guard removal fails that test instead of silently leaving this table stale. It also encodes the investigation matrix (scanner read and heal read may overlap; heal write conflicts with scanner reads; data-movement write conflicts with all work; independent sets stay concurrent) without claiming production enforces it.

| Work | Entry point | Current guard | Fallback / namespace semantics |
|---|---|---|---|
| Scanner read/list | `nsscanner_disk` in `crates/scanner/src/scanner_io/io_disk.rs` | Per-disk `start_scan()` guard; bucket lifecycle/replication/object-lock reads precede `scan_data_folder` | Scanner keeps its local disk and durable cursor; no HealManager set-level admission is consulted |
| Scanner metadata read | Object-size and metadata branches in `crates/scanner/src/scanner_folder.rs` | Scanner cycle budget and per-disk scan marker | Corrupt metadata records the pending scanner ledger; MRF is a hint, not the durable owner |
| Scanner heal admission | `send_required_scanner_heal_request` in `crates/scanner/src/scanner_folder.rs` | Manager queue dedup and pending ledger (`update_pending_scanner_heal_after_admission`) | MRF `Enqueued` / `Coalesced` is ledger-only; rejected MRF keeps immediate heal plus ledger |
| Heal auto scan | `start_auto_disk_scanner` in `crates/heal/src/heal/manager/auto_scan.rs` | Queue-first then active-task check; replacement recovery blocklist | Scanning disks remain candidates when degraded quorum needs them; they are not globally excluded |
| Heal object read | `heal_object` in `crates/ecstore/src/set_disk/ops/heal.rs` | Namespace write lock (`get_write_lock`) unless `no_lock`; reads file info before commit | The namespace lock is object-scoped and does not claim scanner cycle ownership |
| Disk selection | `get_online_disks_with_healing_and_info` in `crates/ecstore/src/set_disk/ops/locking.rs` | Healing disks are ordered after new disks; scanning disks may remain candidates | Degraded/quorum fallback is preserved |
| Data movement | `wait_for_data_movement_admission` in `crates/ecstore/src/data_movement/backpressure.rs` | Storage-owned backpressure on foreground pressure; no second coordinator | Any future admission token must be validated at the final metadata/format/delete commit (see [unified-object-generation.md](unified-object-generation.md)) |

Rules: cancellation or a local lease alone is not a fence; if a fixture ever demonstrates a stale destructive write, the fix extends the storage-owned generation/admission primitive and validates the token at the final commit rather than adding a coordinator.

## Behavior Change Gates

Any later placement or repair PR must use the following gates:

- Placement gate: prove object-to-set hashing is unchanged for existing object keys and format algorithms.
- Pool gate: prove pool selection does not choose suspended or rebalancing pools unless the existing path already allows it.
- Quorum gate: prove storage readiness and lock readiness remain per-set.
- Scanner gate: prove scan budget reason and checkpoint mapping remain stable.
- Heal gate: prove low-priority scanner heal, forced heal, duplicate merge, and queue-full outcomes remain distinct.
- Rollback gate: if a new scheduler sidecar is disabled, placement and repair must fall back to the current direct ECStore/scanner/heal behavior.
