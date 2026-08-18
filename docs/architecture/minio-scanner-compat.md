# MinIO ↔ RustFS Data Scanner Comparison

Assesses how closely the RustFS background data scanner matches MinIO's
`cmd/data-scanner.go` implementation: cycle leadership, namespace walk,
usage accounting, ILM admission, replication repair, heal/bitrot selection,
alerts, and operator surfaces. This is a **durable gap analysis**. It changes
no scanner code. Every claim cites the code that backs it.

MinIO sources below are the public `minio/minio` `master` tree as of
2026-08-18 (`cmd/data-scanner.go`, `cmd/erasure.go`, `cmd/xl-storage.go`,
`cmd/data-usage-cache.go`, `internal/config/scanner/scanner.go`). They are
not files in this repository.

Operator runtime knobs already documented here stay in
[scanner-runtime-controls.md](../operations/scanner-runtime-controls.md).
This page does not duplicate that runbook.

## Executive Summary

The two scanners share the same skeleton. Both run one cluster-wide leader
loop, persist a cycle counter in `.bloomcycle.bin`, walk folders with a
1-in-16 compacted-leaf schedule, select objects for heal with a 1-in-1024
hash, compact usage trees at the same child thresholds, evaluate ILM through
a lifecycle evaluator, enqueue replication heals, emit excess-version and
excess-folder alerts, and throttle with a proportional sleeper.

The remaining gaps are not "the scanner is missing". They are **heal
fidelity**, **cross-set scheduling**, **on-disk cache interop**, and
**operator/notification wire names**. The highest-severity difference is
that MinIO heals a selected object inline and then cleans abandoned parts,
while RustFS admits a low-priority heal request that can be dropped and
never calls `check_abandoned_parts` on that path.

RustFS also has several load-bearing additions MinIO does not: dirty-usage
fast wake, cycle budgets, leader-epoch fencing, remote NS-scanner protocol
v6, checkpoint resume, and clean-idle backoff. Those should be preserved.

| Area | Verdict | Why it matters |
|---|---|---|
| Cycle / leadership / bitrot mode | Close | Same `.bloomcycle.bin` counter, same deep-scan window of `healObjectSelectProb` cycles. |
| Folder walk, compact, 1/16 + 1/1024 selection | Close | Constants and `mod` / `modAlt` schedule match. |
| ILM eval + expiry/transition enqueue | Close | Same action set; RustFS additionally gates metrics on queue admission. |
| Bucket replication repair | Close | Both call `queueReplicationHeal` / `queue_heal`. |
| Scanner-selected object heal | **Gap** | MinIO `HealObject` is synchronous; RustFS async admission can drop the check. |
| Abandoned-part cleanup on selected objects | **Gap** | MinIO calls `CheckAbandonedParts` after heal; RustFS scanner path does not. |
| Bucket order across erasure sets | **Gap** | MinIO shuffles per set; RustFS is deterministic dirty→new→existing. |
| `.usage-cache.bin` bytes | **Incompatible** | MinIO is zstd+msgp v8; RustFS is raw MessagePack. Reconstructable, not reusable. |
| Excess-folder default | Differs | MinIO `50000`; RustFS `65538`. |
| Alert event names | Differs | MinIO `s3:ObjectManyVersions`; RustFS `s3:Scanner:ManyVersions`. |

---

## Architecture Overlay

Both stacks are `init → leader lock → cycle → NSScanner → per-set disk walk →
scanDataFolder → applyActions`.

```text
initDataScanner / init_data_scanner
        │
        ▼
runDataScanner / run_data_scanner          (cluster leader lock)
        │
        ├─ load .bloomcycle.bin cycle state
        ├─ getCycleScanMode (Normal vs Deep bitrot)
        └─ NSScanner(wantCycle, scanMode)
                │
                ▼
        per erasure set  (MinIO: er.nsScanner; RustFS: scanner_io)
                │
                ├─ load set .usage-cache.bin
                ├─ bucket order (new first, then existing)
                └─ per disk: NSScanner / scan_data_folder
                        │
                        ├─ lifecycle + replication config
                        ├─ folder walk, compact, 1/16 skip
                        ├─ getSize → applyActions
                        │       ├─ ILM eval
                        │       ├─ heal selected versions
                        │       └─ healReplication
                        └─ abandoned-children heal walk
```

| Stage | MinIO | RustFS |
|---|---|---|
| Startup | `initDataScanner` goroutine, random sleep ≥ 1s between `runDataScanner` calls | `init_data_scanner` in `crates/scanner/src/scanner.rs`; optional cold-cache / replication skip of start delay |
| Leader | `globalLeaderLock.GetLock` (blocks) | `leader.lock` write lock with timeout; contended cycle returns and retries |
| Cycle persist | LE `uint64` + msgp `currentScannerCycle` at `.bloomcycle.bin` | LE `uint64` + optional `RSCYC001` epoch header + msgpack `CurrentCycle` |
| Set walk | `erasureObjects.nsScanner` in MinIO `cmd/erasure.go` | `crates/scanner/src/scanner_io.rs` |
| Folder walk | `folderScanner.scanFolder` | `crates/scanner/src/scanner_folder.rs` |
| Object actions | `scannerItem.applyActions` | `ScannerItem::apply_actions` |
| Usage publish | `storeDataUsageInBackend` ← `.usage.json` | `store_data_usage_in_backend` ← `.usage.v2.json` (legacy `.usage.json` read-only; `scanner-usage-v2` in [compat-cleanup-register.md](compat-cleanup-register.md)) |

---

## What Already Matches

These are not gaps. Treat regressions here as MinIO-parity bugs.

### Cycle constants and heal selection

| Constant | MinIO | RustFS | Evidence |
|---|---|---|---|
| Folder sleep quantum | 1ms | sleeper `MIN_SLEEP` 1ms | MinIO `dataScannerSleepPerFolder`; `crates/scanner/src/sleeper.rs` |
| Compacted-leaf visit period | 16 | 16 (`RUSTFS_DATA_USAGE_UPDATE_DIR_CYCLES`) | MinIO `dataUsageUpdateDirCycles`; `crates/scanner/src/scanner_folder.rs` `DATA_USAGE_UPDATE_DIR_CYCLES` |
| Heal object probability | 1024 | 1024 (`RUSTFS_HEAL_OBJECT_SELECT_PROB`) | MinIO `healObjectSelectProb`; `DEFAULT_HEAL_OBJECT_SELECT_PROB` |
| Compact least objects | 500 | 500 | both `dataScannerCompactLeastObject` / `DATA_SCANNER_COMPACT_LEAST_OBJECT` |
| Compact at children | 10000 | 10000 | both |
| Compact at folders | 2500 | 2500 | `children/4` |
| Force compact folders | 250000 | 250000 | both |
| Start delay default | 1 minute | speed-preset derived (default 1 minute) | MinIO `dataScannerStartDelay`; RustFS speed preset |
| Excess versions | 100 | 100 | MinIO `scannerExcessObjectVersions`; `DEFAULT_SCANNER_ALERT_EXCESS_VERSIONS` |
| Excess version size | 1 TiB | 1 TiB | MinIO `scannerExcessObjectVersionsTotalSize`; `DEFAULT_SCANNER_ALERT_EXCESS_VERSION_SIZE` |

Folder skip uses `hash.mod(nextCycle, 16)`. Object heal uses
`hash.modAlt(nextCycle/div, healObjectSelect/div)`. Compacted folders raise
`objectHealProbDiv` to 16 so the 1/1024 overall probability still holds.
RustFS copies this in `scan_folder` (`mod_` + `object_heal_prob_div`).

Path identity is the cleaned path string, not a digest. MinIO `hashPath` is
`path.Clean`; RustFS `hash_path` in `crates/data-usage/src/data_usage.rs`
cleans the same way. `xxhash` is only used in MinIO `mod` / `modAlt`.

### Bitrot cycle window

Both enter Deep scan when:

- bitrot cycle is `0` (always deep), or
- `current - bitrotStartCycle < healObjectSelectProb`, or
- wall time since `bitrotStartTime` exceeds the configured bitrot cycle.

MinIO: `getCycleScanMode` in `cmd/data-scanner.go`. RustFS:
`get_cycle_scan_mode` in `crates/scanner/src/scanner.rs`. Both persist
`backgroundHealInfo` / `BackgroundHealInfo` and skip it on single-disk
(`globalIsErasureSD` / `scanner_is_erasure_sd`).

### ILM action coverage

`apply_actions` covers the same action enum MinIO does: delete, delete
version, delete restored, delete-all, del-marker-delete-all, transition,
and none (heal + replication). Evaluator is constructed with lock
retention and replication config. Free versions are swept through
`enqueue_free_version` / `enqueue_runtime_free_version`. Noncurrent
versions batch through `enqueueNoncurrentVersions` /
`enqueue_runtime_newer_noncurrent`.

### Speed presets

`fastest` / `fast` / `default` / `slow` / `slowest` map to the same delay,
max-wait, and cycle defaults MinIO `LookupConfig` uses (`0/0/1s`,
`1/100ms/1m`, `2/1s/1m`, `10/15s/1m`, `100/15s/30m`).

### Abandoned-children folder heal

When a previously cached child is missing from the current directory listing,
both scanners quorum-list the prefix and enqueue bucket/object heals. RustFS
keeps this walk in `scan_folder` after the new/existing folder scans.

### Read-path heal still exists

MinIO also heals from GET/HEAD and MRF; the scanner is not the only heal
source (MinIO PR 18050). RustFS GET decode errors enqueue
`HealRequestSource::ReadRepair` in `crates/ecstore/src/set_disk/read.rs`.
Scanner-heal gaps therefore delay *background* repair, not all repair.

---

## Gaps

Severity is the operator-visible failure if the gap is left as-is.

### G1 — Scanner object heal is async and droppable (high)

MinIO `applyHealing` calls `ObjectLayer.HealObject` and waits. The folder
walker then treats `getSize` as having already healed the object
(`cmd/data-scanner.go`, comment on `abandonedChildren` deletion). After a
successful heal it always runs `CheckAbandonedParts` with
`Remove: healDeleteDangling`.

RustFS `heal_actions` always returns the original `actual_size` and, when
heal is selected, calls `enqueue_heal` → `send_heal_request_with_admission`
at `HealChannelPriority::Low`. `Full` and `Dropped` admissions are logged
and skipped. `RUSTFS_SCANNER_INLINE_HEAL_ENABLE` only warns
`inline_heal_rollback_unsupported` (`warn_inline_heal_compat_requested` in
`crates/scanner/src/scanner_folder.rs`).

**Failure:** a 1/1024-selected object with a missing shard can remain
unhealed for many more cycles if the heal channel is full. Bitrot Deep
selection has the same drop window. Usage accounting is unchanged by heal
outcome, so a reconstructed size never replaces the pre-heal size in that
cycle.

**Do not "fix" this by making every scanner heal inline on the walk
goroutine.** MinIO can afford that because `HealObject` is the storage
layer. RustFS already has a heal worker pool and admission. The missing
contract is: scanner-selected heals must be durable (pending_heals retry)
and must not be silent-dropped without a later guaranteed retry.

Pending heals already exist for some metadata/abandoned-child failures
(`PendingScannerHeal` in the usage cache). Object-selection heals that hit
`HealAdmissionResult::Full` do not currently join that retry list.

### G2 — No `CheckAbandonedParts` on the scanner object-heal path (high)

`check_abandoned_parts` is implemented on the store
(`crates/ecstore/src/store/heal.rs`, `crates/ecstore/src/set_disk/ops/heal.rs`)
and is in the object API. The scanner never calls it. The heal task
processor (`crates/heal/src/heal/task.rs`) also does not call it after a
scanner-originated `heal_object`.

MinIO records this as `scannerMetricCleanAbandoned`. RustFS defines
`Metric::CleanAbandoned` in `crates/common/src/metrics.rs` but the scanner
crate never records it.

**Failure:** leftover `part.N` files after a successful object heal stay
until some other heal path notices them. Disk usage and bitrot surface
area remain inflated.

### G3 — Erasure-set bucket order is not shuffled (medium)

MinIO `nsScanner` builds a permutation of buckets, emits *new* buckets
(absent from the old cache) first in that random order, then existing
buckets in that random order. Comment: otherwise the same buckets are
scanned across every erasure set at the same time.

RustFS `bucket_usage_scan_order` in `crates/scanner/src/scanner_io.rs` is
deterministic: dirty buckets, then cache-miss (new) buckets, then
cache-hit buckets, preserving `ListBuckets` order.

Dirty-first is a RustFS improvement (MinIO has no dirty-usage wake). The
gap is the *existing* bucket tail: under many buckets and several sets,
RustFS lock-steps ILM/heal/replication load onto the same prefixes.

### G4 — Alert event names and audit channel (medium)

MinIO emits `event.ObjectManyVersions`, `event.ObjectLargeVersions`,
`event.PrefixManyFolders`, plus `auditLogInternal` events
`scanner:manyversions` / `scanner:largeversions` / `scanner:manyprefixes`.

RustFS emits `s3:Scanner:ManyVersions`, `s3:Scanner:LargeVersions`,
`s3:Scanner:BigPrefix` (`EVENT_SCANNER_*` in
`crates/scanner/src/scanner_folder.rs`; wire names in
`crates/s3-types/src/event_name.rs`). Notifications are edge-held 24h
(MinIO re-emits every cycle). There is no scanner audit-log counterpart.

**Failure:** notification destinations configured for MinIO event names
miss RustFS scanner alerts. Audit pipelines that key on
`scanner:manyversions` see nothing.

### G5 — Excess-folder default differs (low)

MinIO `scannerExcessFolders` default is `50000`
(`internal/config/scanner/scanner.go`). RustFS
`DEFAULT_SCANNER_ALERT_EXCESS_FOLDERS` is `65538`
(`crates/config/src/constants/scanner.rs`).

**Failure:** the same prefix is silent on MinIO and noisy on RustFS (or
the reverse if an operator copied MinIO runbooks).

### G6 — `.usage-cache.bin` is not MinIO-readable (medium for migration, low otherwise)

MinIO writes one version byte (`dataUsageCacheVerCurrent = 8`) plus zstd
plus msgp (`cmd/data-usage-cache.go` `serializeTo`). RustFS
`DataUsageCache::save_inner` writes uncompressed `rmp_serde` with no
version byte (`crates/scanner/src/data_usage_define.rs`).

Both use the same object name `.usage-cache.bin` and a `.bkp` sibling.
A MinIO disk set attached to RustFS rebuilds the tree on first scan; the
bytes are not reused. The inverse is also true.

`.bloomcycle.bin` is closer: both start with a little-endian `u64` next
cycle. RustFS additionally writes `RSCYC001` + leader epoch when fencing
is active, and still reads a bare 8-byte or 8-byte+msgpack MinIO blob
(`decode_scanner_cycle_state`). MinIO cannot consume the fenced form.

Cluster usage snapshots diverge on purpose: MinIO `.usage.json`, RustFS
authoritative `.usage.v2.json`. That is already a compat register item,
not a scanner-logic bug.

### G7 — Heal-selected usage size ignores heal result (low)

MinIO `healActions` replaces `actualSz` with `HealObject`'s
`res.ObjectSize` when positive. RustFS `heal_actions` always returns
`actual_size`. Wrong sizes persist until the *next* cycle that both
selects the object *and* observes healed metadata.

This is secondary to G1: without a completed heal there is no new size.

### G8 — Operator metric names and `mc admin scanner info` (low)

MinIO `scannerMetric.String()` is PascalCase (`ReadMetadata`, `ScanObject`,
`ILM`). RustFS `Metric::as_str` is snake_case (`read_metadata`,
`scan_object`, `ilm`). `mc admin scanner info` against a RustFS
`ScannerMetrics.life_time_ops` map will not match MinIO dashboard keys.

RustFS exposes a richer `/v3/scanner/status` (freshness, runtime config
sources, cycle schedule, admission). That is the supported operator
surface; MinIO `mc` scanner info is not a compatibility target unless
explicitly added.

### G9 — Unversioned replication heal gate (low)

MinIO `healReplication` returns immediately when `oi.VersionID == ""`.
RustFS allows the call when the object is a delete marker or has a
version-purge status even if `version_id` is none/nil
(`ScannerItem::heal_replication`). This is likely *more* correct for
purge/delete-marker repair on unversioned-looking entries, but it is a
behavioral difference worth pinning with a test rather than copying
MinIO's empty-VersionID skip blindly.

---

## RustFS-Only Behavior To Keep

These are not MinIO gaps. Removing them to "match MinIO" would be a
regression.

| Addition | Where | Why keep it |
|---|---|---|
| Dirty-usage fast wake + superseded retry (5s base) | `crates/scanner/src/scanner.rs`, `scanner_io.rs` `record_dirty_usage_bucket` | Quota/usage lag after write bursts; MinIO waits a full cycle. |
| Cycle object/directory/runtime budgets | `crates/scanner/src/scanner_budget.rs` | Bounds scanner blast radius; MinIO only sleeps. |
| Leader epoch + CAS persist | `encode_scanner_cycle_state` | Split-brain cycle counters after lock loss. |
| Remote NS-scanner protocol v6 | `crates/scanner/src/remote_scanner.rs`; compat `ns-scanner-rpc-v3` | Distributed disk walks with fencing. |
| Scan checkpoints / resume hints | `DataUsageScanCheckpoint` | Partial cycles after budget cancel. |
| Clean-idle backoff (single-disk / erasure) | `ScannerCleanIdleBackoff` | Stops minute-cadence full walks on idle namespaces. |
| Heal/replication admission metrics | `HealAdmissionResult`, `ScannerReplicationQueueAdmission` | Makes G1 observable; MinIO has no equivalent queue. |
| Alert emission cooldown | 24h edge-hold | Avoids notification storms MinIO still has. |

---

## Improvement Workstreams

These are contracts, not a checklist. Each workstream is independently
shippable. Do not couple them into one "make scanner like MinIO" rewrite.

### W1 — Durable scanner-selected heal (closes G1, G7)

**Invariant:** if an object is selected by `modAlt` in a cycle that
`should_heal()`, that object/version is either healed, recorded in
`pending_heals` for a later cycle, or the cycle is marked incomplete for
heal work. Silent `Full`/`Dropped` is not a success.

**Shape:** keep the heal channel. On `Full`/`Dropped`, persist
`PendingScannerHeal` (object, version, scan mode) the same way abandoned
metadata heals already persist. Retry at high or at least non-droppable
priority next cycle. When a heal *completes*, optionally replace the
accounted size with the healed size (G7).

**Do not:** call `HealObject` inline from `scan_folder` as a default. The
unsupported `RUSTFS_SCANNER_INLINE_HEAL_ENABLE` warning exists because
that rollback fights the worker pool. An opt-in inline path is only
justified if a measured admission-drop rate stays high after durable
retry.

**Tests:** (a) selected object missing one shard, heal channel full →
pending_heals non-empty, next cycle heals it; (b) Deep mode + recent
mtime stays Normal (existing cooldown); (c) usage size updates only after
heal success; (d) revert of pending_heals-on-drop fails the test.

### W2 — Abandoned-part cleanup after scanner object heal (closes G2)

**Invariant:** a scanner-selected object heal that succeeds (or that the
heal worker reports as already consistent) runs `check_abandoned_parts`
with dangling removal, matching MinIO `healDeleteDangling = true`.

**Shape:** call it from the heal worker when `source == Scanner`, not
from the folder walk. That keeps IO off the scanner hot path. Record
`Metric::CleanAbandoned` so last-minute scanner metrics are not a dead
enum.

**Tests:** object with an extra `part.N` after a valid heal → part
removed; dry-run heal does not delete (existing set_disk tests stay
authoritative); `CleanAbandoned` lifetime counter increments.

### W3 — Per-set shuffle of existing buckets (closes G3)

**Invariant:** dirty and new buckets still go first (RustFS dirty-usage
contract). The existing-bucket tail is shuffled per erasure set per
cycle so sets do not scan the same prefix concurrently.

**Shape:** smallest change is `bucket_usage_scan_order` taking a
per-set RNG seed (cycle + pool + set). Do not shuffle dirty buckets;
that would delay quota/usage repair.

**Tests:** two sets, three existing buckets, same cycle → different
existing tails; dirty bucket always index 0.

### W4 — Notification and audit aliases (closes G4, optionally G5)

**Invariant:** a destination subscribed to MinIO names
`s3:ObjectManyVersions` / `s3:ObjectLargeVersions` /
`s3:PrefixManyFolders` receives RustFS scanner alerts. Keep the current
`s3:Scanner:*` names as aliases, not replacements, until clients migrate.

**Shape:** dual-name parse in `crates/s3-types/src/event_name.rs` (already
comments "corresponding to Go") plus dual emit, or a compatibility
mapping at notify dispatch. Audit events are optional and should reuse
the existing audit pipeline rather than a scanner-specific logger.

Align `DEFAULT_SCANNER_ALERT_EXCESS_FOLDERS` to `50000` only with a
release note; 65538 is not a bug, it is a silent default drift.

### W5 — Cache-format interop (closes G6 only if migration requires it)

**Invariant for RustFS-only clusters:** none. Rebuilding `.usage-cache.bin`
on first scan is acceptable.

**Invariant if MinIO disk import is a product goal:** either detect MinIO
v8 zstd+msgp and ignore/rebuild, or implement a one-shot importer.
Writing MinIO-shaped cache from RustFS is not required for serving
objects.

Document in operations that `.usage-cache.bin` is not a migration
artifact. `.bloomcycle.bin` 8-byte prefix already round-trips.

### W6 — Operator surface (closes G8)

Keep `/v3/scanner/status` as the source of truth. If `mc admin scanner
info` support is required, add a madmin-shaped projection with PascalCase
`life_time_ops` keys *in addition to* snake_case, behind a documented
compat flag. Do not rename RustFS metrics; Prometheus and status JSON
already use snake_case.

---

## Suggested Verification (when a workstream ships)

Scanner changes are high-risk under AGENTS.md (lifecycle/tiering,
heal, S3-visible usage). A workstream PR should run:

- `cargo fmt --all --check`
- `cargo test -p rustfs-scanner` (and heal tests for W2)
- the crate's lifecycle integration tests when ILM admission changes
- `make doc-paths-check` if this file's citations move

Do not run `make pre-pr` for documentation-only edits of this page.

---

## Sources

RustFS:

- `crates/scanner/src/scanner.rs` — leader loop, cycle fencing, bitrot mode
- `crates/scanner/src/scanner_folder.rs` — folder walk, ILM, heal, alerts
- `crates/scanner/src/scanner_io.rs` — NSScanner, bucket order, dirty usage
- `crates/scanner/src/scanner_budget.rs` — cycle budgets
- `crates/scanner/src/sleeper.rs` — proportional throttle
- `crates/scanner/src/data_usage_define.rs` — cache persist
- `crates/scanner/src/runtime_config.rs` — env/config resolution
- `crates/config/src/constants/scanner.rs` — defaults
- `crates/common/src/metrics.rs` — metric enum (MinIO-shaped)
- `rustfs/src/admin/handlers/scanner.rs` — `/v3/scanner/status`
- [compat-cleanup-register.md](compat-cleanup-register.md) — `scanner-usage-v2`, `ns-scanner-rpc-v3`

MinIO (`minio/minio` master, 2026-08-18):

- `cmd/data-scanner.go` — init/run, applyActions, healReplication, sleeper
- `cmd/data-scanner-metric.go` — metric enum and `mc` report
- `cmd/erasure.go` — `nsScanner` shuffle and per-disk walk
- `cmd/xl-storage.go` — disk `NSScanner` / getSize
- `cmd/data-usage-cache.go` — hash mod, zstd+msgp cache
- `cmd/data-usage.go` — `.usage.json` / `.bloomcycle.bin` names
- `internal/config/scanner/scanner.go` — speed presets and alert defaults
