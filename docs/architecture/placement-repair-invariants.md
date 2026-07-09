# Placement And Repair Invariants

This inventory covers `G-012` for `rustfs/backlog#666`. It records the current
object placement, readiness, lock quorum, scanner, and repair boundaries that
later scheduler or topology work must preserve.

## Object To Set Hash Rule

Objects reach a set through `Sets::get_disks_by_key`, which calls
`get_hashed_set_index` on the object key:

- `DistributionAlgoVersion::V1` uses `crc_hash(input, set_count)`.
- `DistributionAlgoVersion::V2` and `V3` use
  `sip_hash(input, set_count, format_id_bytes)`.
- The format ID is part of the V2/V3 distribution seed, so changing the seed,
  object key, set count, or algorithm changes placement.

Preservation rule: every object read, write, list, heal, repair, and
decommission path that resolves a set for an existing object must preserve the
same object key and format distribution algorithm.

## Pool, Set, And Disk Assignment Boundary

Pool selection is separate from set hashing:

- Existing objects are discovered across pools and resolved to the best current
  pool candidate before reads or updates continue.
- New object writes select an available pool from current per-pool free-space
  inputs after suspended or rebalancing pools are skipped.
- Set selection inside a pool still uses the object-to-set hash rule above.
- Disk index assignment comes from endpoint and format metadata, not from a
  scheduler decision.

Boundary rule: schedulers may influence admission, worker concurrency, or
buffer sizing, but they must not rewrite pool, set, or disk indexes.

## Readiness And Lock Quorum Boundary

Runtime readiness currently checks storage and lock health independently:

- Storage readiness requires every observed set to meet write quorum based on
  the set drive count and storage class data/parity shape.
- Lock readiness aggregates per-set lock-client host quorum and fails fast if
  any set loses quorum.
- Object and bucket mutations acquire namespace locks through the existing
  storage lock wrappers before changing object or bucket state.

Boundary rule: readiness and lock quorum must stay set-aware. A global healthy
disk count or global connected-host count is not sufficient when any individual
set is below quorum.

## Scanner Budget Preservation

Scanner cycles are bounded by `ScannerCycleBudget`:

- Runtime budget cancels the child token after the configured duration.
- Object budget cancels after the configured object count.
- Directory budget rejects additional directories and cancels with the
  directories reason.
- Partial-cycle metrics and checkpoints use the budget reason.

Preservation rule: later scheduler work can change how scan cycles are admitted
only if it preserves the budget reason, checkpoint reason, and child-token
cancellation behavior.

## Heal Admission Preservation

Scanner and background repair work enter the heal manager through explicit
admission:

- Scanner object heal requests are low priority and may be accepted, merged,
  rejected as full, or dropped.
- Required/high-priority heal candidates escalate on non-admission instead of
  silently disappearing.
- Heal queue admission deduplicates queued and active work unless the request
  explicitly forces admission.
- Full queues can drop low-priority work or displace lower-priority work for a
  higher-priority request according to current manager rules.

Preservation rule: repair scheduling changes must keep admission outcomes
observable and must not convert rejected or dropped repair work into silent
success.

## Behavior Change Gates

Any later placement or repair PR must use the following gates:

- Placement gate: prove object-to-set hashing is unchanged for existing object
  keys and format algorithms.
- Pool gate: prove pool selection does not choose suspended or rebalancing
  pools unless the existing path already allows it.
- Quorum gate: prove storage readiness and lock readiness remain per-set.
- Scanner gate: prove scan budget reason and checkpoint mapping remain stable.
- Heal gate: prove low-priority scanner heal, forced heal, duplicate merge, and
  queue-full outcomes remain distinct.
- Rollback gate: if a new scheduler sidecar is disabled, placement and repair
  must fall back to the current direct ECStore/scanner/heal behavior.
