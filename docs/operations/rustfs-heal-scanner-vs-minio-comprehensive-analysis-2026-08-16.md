# RustFS heal & scanner vs MinIO — comprehensive parity analysis (v2, 2026-08-16)

> English | [中文版](rustfs-heal-scanner-vs-minio-comprehensive-analysis-2026-08-16_zh.md)

- Date: 2026-08-16 (based on that day's `main` code; audit HEAD ≈ `a118d7e4f`)
- Scope: `crates/heal` (src 19,560 lines + tests 2,274 lines), `crates/scanner` (src ~26,000 lines + tests), `crates/data-usage`, the heal/heal_walk/bitrot_self_verify and config parts of `crates/ecstore`, `crates/common/src/heal_channel.rs`, `crates/madmin` (heal/scanner wire types), `rustfs/src` (startup wiring, admin handlers, cluster RPC)
- Parity baseline: minio/minio master (HEAD `7aac2a2c5b`; the repo has entered maintenance mode with master frozen, i.e. its final state)
- Method: four parallel audit tracks (heal crate / scanner crate / ecstore integration layer / MinIO source study), with key conclusions verified by hand one by one (points marked "verified first-hand" below were checked against the source directly)
- This document supersedes `docs/rustfs-heal-scanner-vs-minio-parity-assessment.md` (2026-06-15, v1). Since v1 there have been more than 80 heal/scanner commits (the full automatic drive-replacement healing chain, the resume state machine, making usage convergence authoritative, cluster-level heal coordination, ILM restore semantics, etc.), so v1's feature inventory and gap judgments are comprehensively outdated; v1 conclusions such as "bloom filter missing" were verified this round to be **misjudgments** (see §5.4).

---

## 0. Conclusion summary

1. **Overall verdict: the core functional chains of heal and scanner are complete.** Object-level heal (quorum arbitration + ETag fallback + bitrot Deep verification + dangling handling), erasure set deep scans (per-set disk-walk union enumeration), per-version resumable scans (schema'd persistence layer + CAS atomic publish + crash-window backfill), automatic drive-replacement healing (readiness validation + identity fencing + durable intent + completion proof), the scanner cycle loop (leader lock + persisted leader-epoch fence), data usage statistics (bucket-level/cluster-level, primary + backup + observed snapshots, epoch/cycle anti-rollback), the full ILM action set (expiry/transition/noncurrent/free-version/delete-marker cleanup), and the admin Start/Query/Cancel protocol (clientToken semantics aligned with madmin) — all of these are implemented and carry regression tests. There are **no empty implementations / early-return stubs** inside the two crates; every exceptional path has logs + metrics + error semantics.
2. **The main gaps concentrate on "entry points and the observability surface", not on the repair algorithms themselves**: the MRF/ECDecode/Metadata task executors are implemented but have no production trigger entry (`HealEvent` is entirely unwired); `CheckAbandonedParts` is `NotImplemented` at all three ecstore layers; the heal/scanner trace channels are missing; scanner excess S3 events are missing; madmin client methods are missing (only wire types exist); heal byte-level progress/ETA is not implemented.
3. **Important corrections to the v1 understanding**: the bloom filter has been **removed** from current MinIO master (`.bloomcycle.bin` stores only a cycle count), so RustFS's current state matches MinIO; the MinIO scanner is likewise a **cluster-level leader singleton**, and RustFS's leader.lock model is the same shape as MinIO's; RustFS's ETag majority-fallback arbitration is already implemented (`crates/ecstore/src/set_disk/ops/heal.rs:525-567,679`, verified first-hand) — the arbitration gap v1 worried about does not exist.
4. **RustFS exceeds MinIO in several places**: the remote_scanner RPC protocol (remote peers scan locally instead of the leader reading remote drives across the network), the persisted leader-epoch CAS fence, cycle budgets and per-set/per-disk concurrency gates, the pending-heal ledger, the durable replacement intent + completion proof state machine, foreground pressure gating (mainline throttle), and the cluster heal control coordinator + envelope replay protection.
5. Gap severity tally: 8 P1 items (behavioral/operational alignment gaps), 9 P2 items (completeness), 3 P3 items (cleanup/low risk), and 7 items of "not pursuing parity by design". Full list in §6.

---

## 1. Architecture overview

### 1.1 RustFS's three-layer architecture

RustFS splits the heal/scanner functionality that MinIO keeps inside the `cmd/` monolith into three layers plus two standalone crates:

| Layer | Location | Responsibilities |
|---|---|---|
| Primitives layer | `crates/ecstore/src/set_disk/ops/heal.rs` (~3,240 lines), `ops/heal_walk.rs`, `ops/bitrot_self_verify.rs`; upper wrappers `store/heal.rs`, `store/heal_walk.rs`, `core/sets.rs` | Object/bucket/format/replacement-drive format repair, disk-walk union enumeration, write-path bitrot self-verification; the `rustfs_storage_api::HealOperations` contract is implemented by `SetDisks`/`Sets`/`ECStore` (`crates/storage-api/src/object.rs:503-519`) |
| heal runtime | `crates/heal` | Process-level HealManager (priority queue/scheduler/auto disk scanner/resumable resume), HealChannelProcessor (consumes the global heal channel), drive-replacement recovery state machine |
| scanner runtime | `crates/scanner` | Data usage scanning, ILM evaluation and enqueueing, heal candidate production, replication usage statistics, remote scanner RPC |
| Shared protocol | `crates/common/src/heal_channel.rs` (~776 lines) | Start/Query/Cancel command channel, `HealOpts`/`HealScanMode`/`HealRequestSource`/`HealAdmission*` shared types, `HealResultItem` (madmin) |
| Shared data | `crates/data-usage` | `DataUsageEntry/Info`, histograms, `hash_path`; produced by the scanner, consumed by ecstore/admin |

Startup chain (wiring verified first-hand):

1. `rustfs/src/startup_services.rs:93` → `init_background_service_runtime(store)`.
2. `rustfs/src/startup_background.rs:41-81`: create the global heal service cancel token; read `RUSTFS_SCANNER_ENABLED` (alias `RUSTFS_ENABLE_SCANNER`, default true) and `RUSTFS_HEAL_ENABLED` (alias `RUSTFS_ENABLE_HEAL`, default true); **the heal manager is initialized whenever either heal or scanner is enabled** (heal candidates produced by the scanner need a consumer; with both off, the heal channel is not initialized and `send_heal_request` reports "Heal channel not initialized").
3. `crates/heal/src/lib.rs:142-216`: atomic initialization inside an owned task (a caller cancel cannot leave a half-initialized manager behind, `lib.rs:123-131`; `GLOBAL_HEAL_RUNTIME_INIT` mutex single-flight) → `HealManager::start()` → `rustfs_common::heal_channel::init_heal_channels()` → spawn `HealChannelProcessor::start_with_receipts`.
4. `crates/heal/src/heal/manager.rs:1301-1356` `HealManager::start`: `start_scheduler()` (`manager.rs:2394-2461`, interval default 10s + `Notify` event-driven wakeup) → `process_unclean_shutdown()` (`manager.rs:1362-1695`) → when `enable_auto_heal` (default true), `start_auto_disk_scanner()` (`manager.rs:2464-2999`).
5. After the server is ready, `rustfs/src/startup_lifecycle.rs:150-152`: when `enable_scanner`, `init_data_scanner(token, store)` (`crates/scanner/src/scanner.rs:1293-1372`).
6. Graceful shutdown: `rustfs/src/startup_shutdown.rs:308` `shutdown_ahm_services()` (cancel token); `:414` `clear_unclean_shutdown_markers()`.

### 1.2 MinIO's corresponding structure (final master state)

| MinIO file | Responsibilities |
|---|---|
| `cmd/admin-heal-ops.go` | Manual admin heal sequence (healSequence, clientToken/forceStart/forceStop) |
| `cmd/global-heal.go` | Resident background heal queue (newBgHealSequence, token fixed `0000-…`, never ends) + `healErasureSet` (full-object heal per set) |
| `cmd/background-heal-ops.go` | healRoutine worker pool (`_MINIO_HEAL_WORKERS`, default GOMAXPROCS/2) consuming healTask |
| `cmd/mrf.go` | MRF (Most Recent Fail) queue (capacity 100,000), persisted at process exit to `.minio.sys/buckets/.heal/mrf/list.bin` with startup replay |
| `cmd/background-newdisks-heal-ops.go` | Automatic resync for new/replaced drives (monitorLocalDisksAndHeal 10s polling + healFreshDisk + healingTracker) |
| `cmd/erasure-healing.go` / `erasure-healing-common.go` | Object-level heal core (~800 lines), listAndHeal |
| `cmd/data-scanner.go` | Scanner loop (globalLeaderLock cluster singleton) + folderScanner + applyActions |
| `cmd/erasure.go` (nsScanner) / `erasure-server-pool.go` | NSScanner three-layer structure |
| `cmd/bucket-lifecycle.go` | ILM executor (expiry/transition worker pools) |
| `cmd/xl-storage.go` | DiskInfo.Healing, CheckParts/VerifyFile, CleanAbandonedData, RenameData healing branch |
| `cmd/prepare-storage.go` | waitForFormatErasure new-drive startup handshake |

### 1.3 Architecture-level differences (design trade-offs, not defects)

1. **heal queue model**: MinIO funnels every heal (scanner sampling/MRF/admin/new-disk resync) into a single channel + a fixed worker pool (new-disk resync additionally has a per-drive worker pool); RustFS is a multi-policy scheduler built from a priority heap + dedup-merge + capacity-tiered dropping + per-set bulkhead + foreground pressure gating (`manager.rs:3003-3420`). RustFS is more expressive, at the cost of an observability question around "duplicate requests being merged" (already pointed out in v1; the current `HealAdmissionReceipt` canonical task_id + alias mechanism answers it, `manager.rs:1759-1846`).
2. **scanner remote-drive access**: the MinIO leader transparently reads and writes remote-node drives through the disk abstraction layer; the RustFS leader pushes scan execution down to the remote peer to run locally via the remote_scanner RPC (`crates/scanner/src/remote_scanner.rs`), with only results and progress heartbeats sent back. Both are cluster single-leader. RustFS's approach saves the leader↔remote metadata read amplification, at the cost of maintaining a separate RPC protocol (HMAC per-frame authentication, session replay cache, fence re-validation, `remote_scanner.rs:52-61,405-496,1024-1065`).
3. **heal state persistence**: MinIO uses a single file `.healing.bin` (msgp healingTracker, reset whenever the diskID mismatches); RustFS uses a schema'd multi-file layout (resume/checkpoint/intent/seal/proof, each CAS-published, `resume.rs:38-61`), with the crash window explicitly backfilled (`erasure_healer.rs:389-402`, `resume.rs:1027-1057`).
4. **write-path self-protection**: MinIO relies on background heal to converge after writes; RustFS, after the commit rename in PutObject/CompleteMultipartUpload, actively checks `convergence.needs_heal()` and immediately enqueues an object heal (`set_disk/ops/object.rs:2291-2306`, `ops/multipart.rs:2574-2589`), and additionally has read repair (`io_primitives.rs:1040-1160`).

---

## 2. Heal implemented-feature panorama

### 2.1 Task types (`HealType`, `crates/heal/src/heal/task.rs:85-111`)

| Type | Semantics | Executor | Production trigger |
|---|---|---|---|
| `Cluster` | all buckets healed in turn (structure + optional recursive objects), in-batch retry ≤3 | `heal_cluster` task.rs:1420-1490 | channel: empty bucket means Cluster (channel.rs:576-577) |
| `Object{bucket,object,version_id}` | single object/version; when absent, rebuild per `recreate_missing` or error out | `heal_object` task.rs:855-1146 | admin, scanner, read-repair, write-path convergence, add_partial |
| `Bucket{bucket}` | bucket metadata/structure; `recursive` additionally walks all object versions | `heal_bucket` task.rs:1284-1418 + `heal_bucket_objects` task.rs:1508-1698 | admin (POST /v3/heal/{bucket}), scanner `build_bucket_heal_request` |
| `Prefix{bucket,prefix}` | recursive by prefix | `heal_prefix` task.rs:1492-1506 | channel: `recursive && prefix` non-empty (channel.rs:578-585) |
| `ErasureSet{buckets,set_disk_id}` | format repair + healing marker + per-bucket preprocessing + resumable per-version deep scan | `heal_erasure_set` task.rs:2158-2642 | admin (pool/set params), auto disk scanner, unclean shutdown, renew_disk, durable replacement recovery |
| `Metadata{bucket,object}` | metadata only (Deep, does not rebuild data) | `heal_metadata` task.rs:1700-1859 | **no production trigger** (§6 HS-01) |
| `MRF{meta_path}` | failure-path-driven Deep repair (recursive+update_parity) | `heal_mrf` task.rs:1861-1992 | **no production trigger** (only `HealEvent` can generate it, unwired) |
| `ECDecode{bucket,object,version_id}` | EC decode rebuild (Deep+recreate+update_parity), Urgent priority | `heal_ec_decode` task.rs:1994-2156 | **no production trigger** (only `HealEvent` can generate it, unwired) |

Priorities `Low/Normal/High/Urgent` (task.rs:168-179); state machine `Pending/Running/Retrying/Completed/Failed/Cancelled/Timeout` (task.rs:225-241).

### 2.2 Trigger-path panorama (beyond admin)

| Channel | source | Priority | Evidence |
|---|---|---|---|
| Scanner periodic sampling (1/1024, `RUSTFS_HEAL_OBJECT_SELECT_PROB`) | Scanner | Low | `scanner_folder.rs:2117-2136`, `:1150`; `remove_corrupted=HEAL_DELETE_DANGLING(true)`, `recreate_missing=false` (`common/heal_channel.rs:24`, `scanner_folder.rs:510-511`) |
| Scanner metadata corruption (get_size failure classified HealMetadata) | Scanner | High | `scanner_folder.rs:2147-2208`, `:1244-1260` |
| Scanner abandoned children (present in cache, absent on disk, list_path_raw quorum verification) | Scanner | High (bucket-level + object-level) | `scanner_folder.rs:2528-2792` |
| Scanner pending-heal ledger retry (persisted after rejection by a full heal channel, ≤128 per bucket per round, 10k cap) | Scanner | original priority | `scanner_folder.rs:1721-1763`, `:99-100` |
| auto disk scanner (unformatted drive confirmed via replacement_readiness / `runtime_state=="returning"` drive / durable-intent re-entry) | AutoHeal | Low | `manager.rs:2464-2999` |
| unclean shutdown recovery (startup reads the `unclean-shutdown` marker → ErasureSet heal for all local sets) | AutoHeal | Low | `manager.rs:1362-1695` |
| write-path convergence (after PutObject/CompleteMultipartUpload, `convergence.needs_heal()`) | Internal | Normal | `set_disk/ops/object.rs:2291-2306`, `ops/multipart.rs:2574-2589` |
| partial-object heal (add_partial) | Internal | Normal | `set_disk/ops/object.rs:5808-5825` |
| stale data-directory cleanup leftover enqueue | Internal | Normal | `set_disk/core/io_primitives.rs:3880-3907` |
| read repair (metadata_read_error / missing_shards / decode_error, TTL dedup cache) | ReadRepair | Low | `set_disk/read.rs:407,995,1079` → `submit_read_repair_heal` (`io_primitives.rs:1105-1160`), `recreate_missing=true` |
| drive reconnect hits UnformattedDisk → send_heal_disk | AutoHeal | Normal | `set_disk/ops/locking.rs:339-347` |
| Admin API (incl. cluster coordinator routing) | Admin | High | `rustfs/src/admin/handlers/heal.rs:174-212`, `:771-930` |
| cluster RPC heal (peer invocation) | — | — | `rustfs/src/storage/rpc/node_service/heal.rs`, `ecstore/src/cluster/rpc/peer_s3_client.rs:296,1209` |

Note: MinIO's MRF channel (read-path immediate delivery on missing/corrupt parts + queue persistence + shutdown replay, `cmd/mrf.go`, `erasure-object.go:395-410,800-812`) is **partially replaced** in RustFS by read-repair + write-path convergence; the three executors `HealType::MRF`/`ECDecode`/`Metadata` have no production entry (see §6 HS-01 for details).

### 2.3 Object-level heal semantics (ecstore `set_disk/ops/heal.rs`)

Flow (`heal_object_with_explicit_version_regen` from :426):

1. Take the object write lock (unless `no_lock`); an `object` ending with `/` goes through object-directory heal (`heal_object_dir_locked` :1587-1717: dangling determination + `remove` deletion + missing-volume rebuild).
2. `read_all_fileinfo` reads xl.meta from all disks; all-not-found is treated as already deleted and returns.
3. **quorum arbitration + ETag fallback** (verified first-hand): `list_online_disks` treats the mod-time quorum as authoritative; when quorum fails it falls back to ETag majority arbitration (`:525-567` `filter_by_etag`/`quorum_etag`); `pick_valid_fileinfo` picks the canonical metadata; the cannotHeal determination for "number of bad-meta disks > parity" is waived when the ETag agrees across all disks (`:679`). Matches MinIO's dual arbitration in `filterDisksByETag`.
4. `disks_with_all_parts` (:562-572) validates parts per `scan_mode`: **Normal only stats (CheckParts semantics), Deep does full bitrot verification (VerifyFile semantics)**; when a Normal scan detects `FileCorrupt` it automatically escalates to Deep and retries once (`:2022-2031`, same shape as MinIO erasure-healing.go:1101-1106); a no-parity object (EC:0) with a bitrot failure is judged unrecoverable (`:700-726`).
5. `should_heal_object_on_disk` (:606-650) classifies each disk as missing/corrupt/offline/outdated → rebuild: per-part bitrot reader/writer (using per-part checksum + algorithm), write into a temporary volume then rename to commit (`HEAL_RENAME_INCOMPLETE` retry semantics :24); dangling-deletion safety check `dangling_delete_safety` (:1488); **orphan data-directory reclamation `reclaim_orphan_data_dirs_best_effort` (:1428)** — this part covers the main scenarios of MinIO's `CleanAbandonedData` (but there is no standalone `CheckAbandonedParts` API, see §6 HS-02).
6. Versioned objects: enumerate "every version" (`storage.rs:1494-1530`); the delete-marker path is decided by `latest_meta.deleted` (`storage.rs:262-277` comment); regression tests `tests/heal_b5_versioned_regression_test.rs:282,334`.
7. Explicit-version rebuild `try_regenerate_explicit_version_meta` (:1318); cleanup of local leftovers of transitioned objects.
8. The write path additionally has shard-level bitrot self-verification `verify_written_bitrot_shards` (`ops/bitrot_self_verify.rs:45-129`, HighwayHash256S, verifying freshly written shards right before the final rename, serving the EC:0 no-parity case) — **note this is not background bitrot patrol**; background patrol is carried by scanner bitrot_cycle-driven Deep heal.

heal-crate-side wrapper (`task.rs:855-1146`): existence check (transient errors become `TransientSkip` to avoid false failures :551-569); scanner synthetic-directory normalization (:1148-1180); `recreate_missing` rebuild (:1183-1282); data-usage-cache object-lock timeout exemption (:571-653); not-found → treated_as_deleted success (:1012-1029); results `HealResultItem` keep at most 1024 entries + truncated flag (:50,845-852).

Recursive walk (`heal_bucket_objects` task.rs:1508-1698): paginated enumeration of all versions including delete markers, transient-error exponential-backoff retry ≤3 (2^n + jitter :620-627), failure-sample log truncation ≤5 entries, aggregated `BatchHealFailure`.

### 2.4 erasure set heal and resumable scans

`heal_erasure_set` (task.rs:2158-2642) runs in four phases (4-step progress tracking):

1. **Replacement intent and recovery-drive selection** (AutoHeal only + non-empty heal_endpoints): reuse the drive holding the durable intent / exclude the target endpoints and pick surviving drives; already-completed generations get an idempotent CleanupPending wrap-up.
2. **Format repair**: `heal_replacement_format(dry_run, pool, set, targets)` (`storage.rs:1372-1384`, trait default fail-closed); per-target-drive results must all be ok (`erasure_healer.rs:97-102`) + identity-fence re-check (task.rs:2410-2420).
3. **healing marker**: write an owner CAS marker `{set_disk_id}:{task_id}` to the target drive (`mod.rs:80-229`, CAS + rollback + unique concurrent owner), which makes `DiskInfo.healing` true (assignment chain verified first-hand `set_disk/mod.rs:4988`).
4. **Per-bucket preprocessing + resumable deep scan**: `ErasureSetHealer::heal_erasure_set` (`erasure_healer.rs:242-278`).

`ErasureSetHealer` scan details (benchmarked against MinIO `healErasureSet`; the `heal_walk.rs:15-23` module comment explicitly cites MinIO `global-heal.go`'s listPathRaw + objQuorum=1 + mergeXLV2Versions):

- **Enumerator choice (backlog#920)**: Deep or AutoHeal → per-set **disk-walk union enumeration** `list_versions_for_heal_page_disk_walk` ("exists on any drive" means sub-quorum reconstructible; `storage.rs:1559-1644`, page bounds 1,000 objects/10,000 versions, `dw1:` cursor); ordinary requests go through read-quorum `list_object_versions`.
- **Resume cursor**: the authoritative cursor is an opaque continuation token (`v1:` = marker JSON, `dw1:` = disk-walk key; the two namespaces are mutually exclusive against misreads, `storage.rs:81-260`); after each completed page, persist the cursor first, then clear the dedup set (`erasure_healer.rs:922-927`).
- **In-page concurrency**: FuturesUnordered + Semaphore, default `RUSTFS_HEAL_PAGE_OBJECT_CONCURRENCY=8`, Deep/AutoHeal forces 1 (`erasure_healer.rs:105-142`).
- **per-version dedup**: `compose_key` length-prefix injection encoding (`resume.rs:281-288`).
- **Error classification**: truly absent (FileNotFound etc.) → Absent (counted as success); infrastructure-transient (quorum/DiskNotFound/SlowDown etc.) → Transient (counted as skipped); everything else Failed (`erasure_healer.rs:148-182`; the comment cites backlog#856/#799 B7: offline drives must not be recorded healed/absent).
- **Loop protection**: abort when an empty page is truncated or the page-tail version identity does not advance (:933-949).
- **Completion determination**: if any of failed/skipped/failed_buckets is >0, do not mark complete; `schedule_retry()` resets both the resume and checkpoint layers (:561-626; backlog#855/B6/#1033: a skip round must not be marked complete).
- **Replacement-drive commit proof**: physical read-back on the target endpoints `replacement_targets_have_version` (`ops/heal.rs:340-412`); unconfirmed → transient skip.

### 2.5 Automatic drive-replacement healing (replacement recovery)

- **Identification** (`replacement_readiness.rs:25-73`): `replacement_mount_lease_root()` exists, canonicalize succeeds, is a mount point, the physical device id is non-empty, disjoint from the root device, and shares no physical device with sibling drives (Linux uses /proc/self/mountinfo mount-id+dev+ino). The non-root mount check has a regression test (`manager.rs:3549`).
- **State machine** (`resume.rs:63-73`): `Intent → Rebuilding → (write proof) Verified → CleanupPending → cleanup`; `Abandoned` is a terminal state; state transitions write the persistence layer first, then mutate (`save_state_strict`).
- **Persistence** (`resume.rs:38-61`, schema ResumeState=5/Checkpoint=5/proof=1): `{task_id}_ahm_resume_state.json`, `_ahm_checkpoint.json`, and intent/seal/completion_proof under the `buckets/ahm-replacement/` namespace; torn write + no seal is recognizable and rebuilt atomically (:1316-1338); CAS publish, refuses to overwrite a concurrently valid proof (:1512-1585).
- **Recovery**: both unclean shutdown and the periodic scan recover unfinished/pending-cleanup replacement generations from surviving drives (`manager.rs:1435-1640,2663-2815`); multi-generation conflict / validation failure → freeze that set (`replacement_recovery_blocked_sets`, `manager.rs:69-87,2782-2815`).
- **External snapshot**: `current_replacement_recovery_snapshot` (`lib.rs:262-333`) merges local surviving-drive records; conflict → Unknown / non-definitive; admin `GET /v4/heal/replacement-recovery`.

### 2.6 Scheduler (manager.rs)

- Priority heap + FIFO within the same priority (:148-191,330-347); dedup key per type (:469-506); enqueue three-state dedup active→queued→retrying (:1759-1785); duplicates default to Merged and return the canonical task_id (`HealAdmissionReceipt`, :1821-1846) + client token alias (:1219-1246).
- Capacity: when the queue is full, best-effort sources (Scanner/AutoHeal/ReadRepair) or low-priority items get Dropped(QueueFull); Admin/Internal may evict queued lower-priority items (`push_displacing_lower_priority` :353-396); 80%/95% tiered pressure handling (:885-909).
- Concurrency: global `max_concurrent_heals` (default 4) + per-set bulkhead `max_concurrent_per_set` (default 1) (:3040-3073,3434-3447).
- Foreground pressure gating, mainline throttle: delay best-effort tasks when foreground read/write permit utilization is ≥80% (:919-1009,2999-3020).
- Timeout: task-level aggregate timeout (default 300s), remaining budget preserved across retries (task.rs:444-451, PR #6101).
- Recoverable retry: `is_recoverable_heal()` (error.rs:83-136) ≤3 attempts, 2^n backoff capped at 30s; retries hold ownership inside a standalone backoff task (:3235-3382).
- Completion states are retained for 10 minutes for querying (:42).

### 2.7 Admin API and cluster coordination

- Routes (`rustfs/src/admin/handlers/heal.rs:174-212`): `POST /rustfs/admin/v3/heal/`, `/heal/{bucket}`, `/heal/{bucket}/{prefix}` (the same POST distinguishes start/query/cancel by the query `clientToken/forceStart/forceStop`, aligned with mc admin heal semantics); `POST /v3/background-heal/status`; `GET /v4/heal/replacement-recovery`. Permission `HealAdminAction` (route_policy.rs:334-341).
- Cluster coordination (heal.rs:771-930 + `node_service.rs:514-606`): `heal_topology_fingerprint` + deterministic-by-topology coordinator-node selection + coordinator epoch; envelope validation + SHA256 digest replay protection; when the coordinator is not local, go through peer gRPC `heal_control`; `probe_heal_control` capability probe (rolling-upgrade scenario).
- Request: the body is `HealOpts` (`recursive/dryRun/remove/recreate/scanMode(0/1/2)/updateParity/nolock/pool/set`, serde camelCase, fields aligned with madmin.HealOpts); a root heal start requires `recursive=true` or a `pool+set` pair; body cap 1MB.
- Response: `HealStartSuccess{clientToken, clientAddress, startTime}`; `HealTaskStatus{summary, detail, startTime, settings, items, truncated, progress}` (summary ∈ running/finished/stopped/notFound); `BackgroundHealStatus` (bitrot start time/cycle/current mode + `disabled/uninitialized/idle/active/degraded` states — an unreachable peer is explicitly degraded rather than impersonating idle, issue #5850) + `healOperations` as a priority×source matrix + cluster progress.
- `HealResultItem`/`HealDriveInfo`/`HealItemType`/DriveState enums are JSON-compatible with madmin (`crates/madmin/src/heal_commands.rs:19-65`).
- A status payload over 8MiB is truncated by halving (channel.rs:37,73-104); path-token validation (wrong token rejected; an empty path matches Cluster only).

### 2.8 heal metrics and logs

Metrics: `rustfs_heal_admission_total{source,result,reason,context}`, `rustfs_heal_task_start_total`, `rustfs_heal_task_running{type,set}`, `rustfs_heal_queue_delay_seconds`, `rustfs_heal_scheduler_skip_total`, `rustfs_heal_mainline_throttle_total`, `rustfs_heal_page_concurrency_current{set}`, `rustfs_heal_candidate_enqueue/merge/drop/priority_reject_total`, `rustfs_heal_read_repair_dedup_total{reason}`, etc. All logs are structured event style (PR #5720); per-object logs are demoted to prevent storms (`demote_to_debug_when!`, #5716/#5719/#5727).

---

## 3. Scanner implemented-feature panorama

### 3.1 Loop, leader, immediate triggering

- **Cluster single leader**: distributed ns write lock `leader.lock` (`scanner.rs:3156-3207`, timeout default 5s) + **persisted leader-epoch CAS fence**: the leader writes (cycle, leader_epoch) encoded as `RSCYC001` into `.bloomcycle.bin` using an ETag precondition (`scanner.rs:118,1850-1861,2177-2334`); usage snapshots additionally carry an epoch fence (:2087-2153). Lock lost → cancel the current cycle, converging within 30s (:108-111,2623-2642).
- One round executes immediately after the lock is acquired; cycle = `RUSTFS_SCANNER_CYCLE` > config cycle > start_delay > deployment default > speed tier (±10% jitter, floor 1s).
- **clean-idle exponential backoff**: consecutive fully-clean idle intervals double (capped at 24h; bitrot-cycle compression cap; disabled when a bucket has active lifecycle/replication rules, :383-456,1382-1512).
- **superseded/deferred backoff**: exponential backoff from 5s capped at 30min (:105-106,3432-3438); maintenance probing failures get an independent backoff (:459-505).
- **Immediate wakeup**: ① dirty-usage fast path — write-path put/delete/multipart/bucket operations call `record_dirty_usage_bucket` (`scanner_io.rs:222-235`; call sites include `rustfs/src/app/object_usecase.rs:6221`), bump the generation and Notify-wake the leader; dirty buckets are queued first (`scanner_io.rs:462-488`); ② maintenance-config changes (lifecycle/replication settings call `record_scanner_maintenance_change`); ③ runtime-config hot updates generation+Notify; ④ cluster activity snapshot changes.
- **Cluster coordination**: `probe_scanner_activity` gathers this node's and peers' `ScannerNodeActivity` (instance_id/namespace_generation/maintenance_generation/protocol_version/topology_digest/data_movement_active/dirty usage); the topology digest covers pools/sets/drives URLs; a mismatched protocol version refuses to share the cache lock (`scanner.rs:970-1068`); **cycles are deferred during data movement (rebalance/decommission)** (`scanner_io.rs:2226-2374`); at cycle end, per-peer RPC confirms the dirty-usage ack (`scanner.rs:2925-2952`).

### 3.2 Traversal model

- The main traversal is a **full directory walk** (tokio::fs::read_dir recursion, `scanner_folder.rs:1915-2234`), not via metacache; metacache/`list_path_raw` is used only for the abandoned-children cross-drive verification (:2528-2792).
- Three-level concurrency: leader → per-set (semaphore default 4) → per-disk bucket scans (default 4) → single-drive recursion; a cache lock per bucket per set `.scanner-cycle.lock.pool-N.set-M` (losing the lock cancels that bucket's scan; lock contention re-queues); single-scan admission per drive (local drives also go through the semaphore, `scanner_io.rs:3246-3274`).
- Bucket ordering: after shuffle, re-ordered as dirty → uncached → cached (`scanner_io.rs:2947-2949,462-488`); entries within a directory sorted by name + resume-hint rotation (`scanner_folder.rs:333-359`).
- **Resumable scanning**: `DataUsageScanCheckpoint{version,resume_after,reason}` persisted in the cache info (`data_usage_define.rs:68,293-307`); written on budget exhaustion/cancel; resumption has Used/Stale/NoHint metrics; the resume unit is a directory (no cross-cycle object-level pagination).
- Erasure semantics: finding `xl.meta` marks an object boundary with no descent; at most 64 UUID data-dir candidate entries probed; data without metadata → record failed + high-priority heal; symlink directories ignored / cycles skipped.
- Cooperative yielding: `yield_now` every N objects (default 128).

### 3.3 Large-bucket skip strategy (benchmarked against MinIO compaction)

1. Cache-currency reuse: if the bucket and scan plan are unchanged (name/source/snapshot_complete/plan digest/next_cycle/leader_epoch/cache_key_format all match), the whole bucket is skipped (`scanner_io.rs:1062-1109`).
2. compacted-directory 16-cycle rotation window: rescan only when `hash mod (next_cycle, 16)` hits, otherwise copy from the old cache (`scanner_folder.rs:74,2429-2442`).
3. compaction thresholds: children <500 or pure-object leaves compress into a single entry; subfolders ≥2500 (root 10000) pre-compressed; children ≥10000 reduced (:75-78,2314-2340,2846-2887).
4. failed-object TTL skip: 86400s / at most 10,000 entries (:88-91,1354-1381).

Compared with MinIO master: MinIO's skip strategy is likewise hash-mod-16 cycles + a compaction threshold tree (500/10000/2500), and the **bloom filter has been removed from master**. RustFS's constants and structure share the same origin as MinIO's current state (MinIO does not adopt cross-drive dirty-generation prioritization; RustFS additionally has two more skip layers — plan digest and cache-currency validation).

### 3.4 data usage statistics

- Dimensions: per-directory entry (size/objects/versions/delete_markers/size histogram/version histogram/replication stats/failed_objects/per-tier stats/children/compacted, `data-usage/src/data_usage.rs:661-679`); per-object SizeSummary (incl. per-ARN replication-target stats and tier stats; tier classification: fully transitioned counts toward its tier, otherwise by storage class; free versions not counted); bucket-level `BucketUsageInfo`; cluster-level `DataUsageInfo` (incl. scanner_cycle/scanner_epoch fence + usage_snapshot_complete).
- Storage: per bucket per set `{bucket}/.usage-cache.bin` (primary + `.bkp` backup + CAS retry); the authoritative cluster snapshot `buckets/data-usage/data-usage.json` (`.bkp` synced every 10 cycles, legacy path compatible); stale snapshots rejected on write (triple epoch/cycle/last_update determination); observation snapshots superseded by a race are stored separately as `data-usage-observed.json`.
- Consumption: `replace_bucket_usage_memory_from_info` refreshes bucket-usage memory + two-level cache invalidation (`scanner.rs:4142-4152`) → bucket stats/quota/admin account_info/system; the write path overlays memory in real time; at startup, reading the snapshot detects a cold cache and skips startup delay.
- Incomplete multipart uploads are not counted (consistent with MinIO, which also does not scan the multipart bucket).

### 3.5 ILM integration

- Per object `ScannerItem::apply_actions` (`scanner_folder.rs:747-1032`): `Evaluator::new(lifecycle).with_lock_retention(...).with_replication_config(...).eval()` batch evaluation.
- Implemented actions (the full IlmAction set, `common/src/metrics.rs:34-45`): expiry deletes (Delete/DeleteRestored/DeleteRestoredVersion), all-versions deletes (DeleteAllVersions/DelMarkerDeleteAllVersions, stop further versions after handling), transition (Transition/TransitionVersion, tier list read at runtime), noncurrent batches (DeleteVersionAction → `enqueue_by_newer_noncurrent`), free-version cleanup (`enqueue_free_version`), object-lock retention constraints. **A one-to-one mapping onto MinIO's 9 ILM actions.**
- Execution model: the scanner is the "discover and enqueue" role (the expiry/transition queues live in ecstore `bucket_lifecycle_ops.rs`); actions are consumed by worker pools — the same shape as MinIO's globalExpiryState/globalTransitionState.
- AbortIncompleteMultipartUpload is not executed inside scanner/ILM (MinIO likewise: `internal/bucket/lifecycle/rule.go` has a FIXME, and it is actually carried by the `erasureSets.cleanupStaleUploads` global routine); in RustFS it is an independent ecstore background task `init_background_stale_multipart_upload_cleanup` (`bucket_lifecycle_ops.rs:3289-3320`) + on-demand at bucket deletion.
- Integration-test coverage: transition+restore, free-version, noncurrent, delete-marker, 0-day, background-scan expiry (`scanner/tests/lifecycle_integration_test.rs:1071-2095`).

### 3.6 heal candidate production (scanner side)

- Sampling: `hash mod_alt(next_cycle/prob_div, 1024/prob_div)`; when rescanning via the compacted branch, prob_div=16 gives an equivalent ×16 probability (the same compensation as MinIO, `scanner_folder.rs:125-127,2117-2122`).
- deep/normal: cycle-level `get_cycle_scan_mode` (bitrot_cycle default 30d, `scanner.rs:1626-1657`) → object-level with `HealScanMode::Deep`; fresh objects (modified within 60s) are demoted to Normal (:146-155); state persisted in `.background-heal.json` (`BackgroundHealInfo{bitrot_start_time,bitrot_start_cycle,current_scan_mode}`, same path and structure as MinIO).
- The scanner only enqueues, never executes inline (inline heal was removed; the compat flag only warns, `scanner_folder.rs:411-427`); `HealScanMode::Deep` is just a marker — the bitrot-verification read happens at the heal consumer (the ecstore Deep path).
- Metadata corruption → high-priority heal (`classify_get_size_failure` → HealMetadata); abandoned children → list_path_raw quorum verification + bucket-level/object-level high-priority heal; healing drives get sticky skipping (`should_heal` :1628-1648).
- pending-heal ledger: candidates rejected by a full heal channel are persisted into the cache info and retried next round.
- Replication heal: `queue_replication_heal` → the replication queue (going through the replication channel, not the heal channel); per-ARN replication usage statistics.

### 3.7 remote_scanner RPC protocol (RustFS-specific)

Requests ≤16KB msgpack (version/request_id/server_epoch/session_id/session_sequence/bucket/next_cycle/leader_epoch/scan_plan_digest/skip_healing/scan_mode/budget); frames ≤2MB, HMAC-SHA256 per-frame authentication (domain `rustfs-ns-scanner-frame-v3`); progress heartbeats 1s (250ms in budget mode); phase announcements Scanning→Persisting; RPC lifetime cap 24h, disconnect grace 2min; anti-replay session+sequence cache (capacity 65536); the server validates leader-fence and persisted-cycle consistency + fence re-validation every 5s; results Complete/Partial/NamespaceNotFound/CycleAhead; remote drives without v4-protocol support fall back to the leader scanning locally (`remote_scanner.rs` whole file; `scanner_io.rs:2750-2812`).

### 3.8 Rate limiting / budgets / hot updates / observability

- DynamicSleeper proportional backoff (speed tiers fastest/fast/default/slow/slowest, same five-tier parameters as MinIO); idle_mode master switch; an extra backoff capped at 250ms per request (10ms base) driven by foreground S3 read traffic.
- Cycle budget ScannerCycleBudget: max_duration/max_objects/max_directories (default 0 = unlimited); partial cycles still advance the cycle count.
- runtime_config with three-layer sources (env > config > default) and per-field source markers (Env/Config/ScannerCompatConfig/Default); admin `PUT /v3/config` hot update → generation+Notify takes effect immediately; `GET /v3/scanner/status` returns enabled/freshness(fresh/stale/unknown)/metrics/cycle_schedule/runtime_config; `GET /v3/ilm/expiry/status` returns expiry queue/workers/missed/blocked.
- Metrics: leader lock; cycle complete/partial/deferred/superseded; versions scanned; per-source (Usage/Lifecycle/BucketReplication/SiteReplication/Heal/Bitrot/Alerts) checked/executed/queued/missed; checkpoint set/used/stale; current path (per-disk+bucket in real time); cache save series; concurrency series; alerts (excess versions/version size/folders).

---

## 4. Item-by-item parity versus MinIO

### 4.1 heal trigger-channel comparison

| MinIO channel | RustFS counterpart | Status |
|---|---|---|
| A. Manual admin heal (healSequence, clientToken/forceStart/forceStop) | heal channel Start/Query/Cancel + cluster coordinator + envelope replay protection | ✅ equivalent and enhanced (cluster routing); sequence-semantics differences in §6 HS-06 |
| B. Resident background heal queue (newBgHealSequence + healRoutine worker pool) | HealManager resident scheduler + priority queue + bulkhead | ✅ equivalent and enhanced |
| C. Automatic new/replaced-drive resync (monitorLocalDisksAndHeal 10s + healFreshDisk + healingTracker + waitForFormatErasure handshake) | auto disk scanner (10s) + replacement_readiness + durable intent/proof state machine + heal_replacement_format | ✅ equivalent and enhanced (identity fence + completion proof; MinIO's tracker is stronger on external visibility, see §6 HS-07) |
| D. MRF (100k queue + persisted list.bin + shutdown replay + read-path corrupt delivery) | read-repair (Low + TTL dedup) + write-path convergence heal carry it partially; the `HealType::MRF` executor has no production entry | ⚠️ partially equivalent (§6 HS-01) |
| E. Scanner sampled heal (1/1024 + compacted ×16 compensation) + abandoned children | the same sampling + ×16 compensation + abandoned children + pending-heal ledger | ✅ equivalent and enhanced (the ledger) |
| F. Read-path inline trigger → MRF (GetObject part missing/corrupt, metadata rebuild missingBlocks>0) | read repair (three entries: missing_shards/decode_error/metadata_read_error) | ✅ equivalent (enqueued into the heal queue rather than the MRF queue) |

### 4.2 Object-level heal semantics comparison

| Feature | MinIO | RustFS | Status |
|---|---|---|---|
| mod-time quorum arbitration | listOnlineDisks | same | ✅ |
| ETag majority fallback (clock drift) | filterDisksByETag | `filter_by_etag`/`quorum_etag` (heal.rs:525-567) | ✅ verified first-hand |
| cannotHeal ETag waiver | waived on all-consistent ETag retry | heal.rs:679 | ✅ |
| Normal=CheckParts (stat) / Deep=VerifyFile (bitrot) | yes | `disks_with_all_parts` by scan_mode (ops/heal.rs:562-572,978-1024) | ✅ |
| Normal detecting corrupt auto-escalates to one Deep retry | erasure-healing.go:1101-1106 | ops/heal.rs:2022-2031 | ✅ |
| dangling determination (not-found > parity) + deletion auditing | isObjectDangling/deleteIfDangling | `dangling_delete_safety` (:1488) + scanner HEAL_DELETE_DANGLING | ✅ (audit-tags details differ) |
| Orphan data-dir/inline cleanup (CleanAbandonedData) | CheckAbandonedParts (invoked explicitly on scanner sampling + admin Remove) | in-heal-path `reclaim_orphan_data_dirs_best_effort` (:1428); standalone API NotImplemented at all three layers | ⚠️ partially equivalent (§6 HS-02) |
| Versioned/delete-marker heal | HealObject versionID; nullVersionID special case | per-version enumeration + delete-marker latest heal (B5 regression) | ✅ |
| Object-level healing metadata marker (x-minio-healing, RenameData skips version cleanup) | yes | no object-level marker; relies on drive-level healing.bin + NSLock + rename semantics | ⚠️ evaluation item (§6 HS-12) |
| Distribution/Index consistency, three lines of defense | yes (manual modification rejected) | target-drive format results all-ok check + identity fence | ✅ (different granularity) |
| no-parity (EC:0) objects | bitrot treated as unrecoverable | judged unrecoverable (:700-726) + write self-verification | ✅ enhanced (write-path self-verification) |
| three-layer distribution inconsistency refuses heal | yes | heal_walk normalization + page-bound defense | ✅ (different implementation approach) |
| multipart orphan reconciliation | carried by CheckAbandonedParts | explicitly NotImplemented (carried by lifecycle cleanup) | ⚠️ §6 HS-02 |
| suspended/decommissioned pool handling | skipped via IsSuspended | deferral semantics (store/heal.rs:192-207, PR #5876) | ✅ |
| heal mutually exclusive with concurrent deletes | NSLock + healing marker | NSLock + write lock | ✅ |

### 4.3 new-drive resync comparison

| MinIO | RustFS | Status |
|---|---|---|
| waitForFormatErasure handshake waiting indefinitely on four classes of recoverable errors | startup drive resolution + renew_disk reconnect path | ✅ (different model: RustFS does not block at startup waiting for format) |
| HealFormat NSLock + errNoHealRequired + refFormat-mismatch rejection | `heal_format`/`heal_replacement_format` fail-closed + target-slot restriction (PR #1787 semantics) | ✅ enhanced |
| per (pool,set) distributed lock preventing concurrent resync | set-level queue dedup + bulkhead (manager.rs:2854-2889) | ✅ |
| brand-new-cluster detection (drives-to-heal == total drives does not trigger) | replacement_readiness (independent mount point / physical-device validation, non-root) | ✅ enhanced |
| healingTracker (.healing.bin: Bytes/Items counters, QueuedBuckets/HealedBuckets, Resume snapshot, RetryAttempts ≤4, HealID linkage, diskID-change reset) | resume/checkpoint schema'd persistence + durable intent/proof (per-task files, CAS) | ✅ equivalent and enhanced (crash-window backfill); but **external snapshot visibility** is weaker than MinIO's (§6 HS-07) |
| skip versions written after heal start (ModTime > Started) | no such filter | ⚠️ §6 HS-13 |
| skip ILM-expired versions (filterLifecycle) | no such filter | ⚠️ §6 HS-13 |
| worker count max(GOMAXPROCS,NR)/4 floor 4, heal:drive_workers override | in-page concurrency 8 (Deep/AutoHeal forced to 1) + per-set bulkhead | ✅ (different parameter model) |
| waitForLowHTTPReq yield per entry | mainline throttle (foreground-utilization gating) | ✅ enhanced |
| heal scope includes the two pseudo-buckets `.minio.sys/config` and `.minio.sys/buckets`; newest bucket first | ErasureSet task pre-processes per bucket (meta-bucket semantics carried by heal_bucket) | ✅ (no "newest first" ordering) |
| whole-failure retry ≤4 (resetHealing + errRetryHealing) | schedule_retry resets both layers + recoverable retry ≤3 | ✅ |

### 4.4 scanner comparison

| MinIO | RustFS | Status |
|---|---|---|
| cluster single leader (globalLeaderLock) | leader.lock + persisted leader-epoch CAS fence | ✅ enhanced (epoch fence against split-brain; MinIO has no persisted epoch) |
| `.bloomcycle.bin` stores only the cycle (bloom removed) | same path stores cycle+leader_epoch (RSCYC001) | ✅ aligned (v1 misjudgment corrected) |
| folderScanner hash-mod-16 + compaction (500/10000/2500) | same constants + plan digest + cache-currency validation + dirty-first | ✅ enhanced |
| ≤GOMAXPROCS parallel scans per drive; healing drives excluded | per-set/per-disk semaphores + sticky skip of healing drives | ✅ |
| scannerSleeper (factor 2/max 1s, speed tiers hot-swapped) | DynamicSleeper same + idle_mode + foreground-read backoff | ✅ enhanced |
| idle semantics: `scanner:idle_speed=on` (throttle only in idle windows, full speed when busy) | `RUSTFS_SCANNER_IDLE_MODE=true` (master switch for rate limiting) | ⚠️ opposite semantic direction, §6 HS-14 |
| applyActions order (heal→ILM→replication→alerts) | apply_actions same order (heal candidates→ILM→replication heal→alerts) | ✅ |
| ILM 9 actions + batch evaluation + DeletePrefixObject optimization | same 9 actions + batch evaluation + expiry queue | ✅ (whether DeleteAllVersions has the single-call optimization was not checked line by line) |
| abandoned children (listPathRaw minDisks=N/2 detects under-written drives) | list_path_raw + quorum verification + high-priority heal | ✅ |
| incomplete multipart independent routine (6h interval/24h expiry, rename into .trash) | ecstore independent background task (configurable interval/expiry) | ✅ (trash two-stage cleanup detail differences, §6 HS-18) |
| usage dimensions (size/objects/versions/DM/histograms/replication/tier/bucket level) | full coverage + cluster snapshot with triple anti-rollback | ✅ enhanced |
| prefix-level usage (loadPrefixUsageFromBackend, consumed by console) | the cache holds the directory tree but flattens only to bucket level | ❌ §6 HS-08 |
| excess events s3:ObjectManyVersions/LargeVersions/PrefixManyFolders + auditing | metrics alert_excess_* only (defaults 100/1TiB/65538 vs MinIO 100/1TB/50000) | ⚠️ §6 HS-04/HS-17 |
| scanner metrics v3 (bucket_scans/directories/objects/versions/last_activity) | full rustfs_scanner_* suite + freshness | ✅ (different naming scheme) |
| TraceScanner / realtime metrics (mc admin scanner status/trace) | no trace channel; /v3/scanner/status has its own structure | ⚠️ §6 HS-03 |

### 4.5 admin/CLI/API surface comparison

| MinIO | RustFS | Status |
|---|---|---|
| `POST /minio/admin/v3/heal/...` start/status/cancel | `POST /rustfs/admin/v3/heal/...` same three states | ✅ (different path prefix is expected) |
| `HealStartSuccess`/`HealTaskStatus`/`HealResultItem`/DriveState | same-named fields JSON-compatible | ✅ |
| `POST /v3/background-heal/status` (BgHealState aggregate) | same path + degraded semantics + operations matrix | ✅ enhanced (no MRF per-endpoint sub-state, because there is no MRF) |
| `GET /v3/healthinfo` per-drive `HealInfo *HealingDisk` | no equivalent healthinfo heal field (replacement-recovery v4 covers part of it) | ⚠️ §6 HS-07 |
| madmin client HealStart/HealStatus/BackgroundHealStatus/ScannerStatus methods | wire types only, no client methods | ❌ §6 HS-05 |
| mc admin heal --pool/--set, --scan-mode, --force-start/stop | HealOpts full field support (pool/set/scanMode/forceStart/forceStop) | ✅ (server-side ready; missing the mc-side entry, HS-05) |
| ErrHealAlreadyRunning / ErrHealOverlappingPaths typed errors | dedup-merge + eviction semantics; no typed overlap rejection | ⚠️ §6 HS-06 |
| result backpressure (maxUnconsumedItems=1000, 10s keep-alive streaming, 24h unconsumed abort) | snapshot-style query (1024 entries + 8MiB truncation + 10min retention) | ⚠️ §6 HS-06 |
| `mc support inspect`/healing-bin offline dump | none (inspect.rs exists but the healing dump is unconfirmed) | ⚠️ P3 |

### 4.6 observability surface comparison

| Dimension | MinIO | RustFS | Status |
|---|---|---|---|
| heal metrics | minio_heal_objects_total/heal_total/errors_total/time_last_activity + v3 drive_health 2=healing | full rustfs_heal_* suite (admission/queue delay/running/throttle/page concurrency) | ✅ (RustFS lacks an equivalent of the single drive_health=healing gauge; DiskInfo.healing is already assigned) |
| scanner metrics | v3 6 + realtime 18 items | full rustfs_scanner_* suite + per-source dimensions | ✅ |
| ILM metrics | v3 5 (expiry/transition pending/active/missed + versions_scanned) | ilm expiry status API + scanner per-source | ✅ (different metrics and API shape) |
| trace | TraceHealing/TraceScanner channels | none | ❌ §6 HS-03 |
| auditing | HealObject events, dangling-deletion audit, scanner:manyversions etc. | structured logs (event style) + metrics; no audit-log events | ⚠️ §6 HS-04 |
| progress | healingTracker Bytes/Items/QueuedBuckets/current object + usage-cache total baseline | HealProgress{scanned/healed/failed/bytes/current_object/percentage}; bytes_processed annotated as 0, estimated_completion_time always None | ⚠️ §6 HS-07 |

### 4.7 configuration surface comparison (defaults)

| MinIO | RustFS | Notes |
|---|---|---|
| `heal:bitrotscan` (default off; on=every cycle; Nm=N×30×24h) | `heal.bitrot_cycle` / `RUSTFS_SCANNER_BITROT_CYCLE_SECS` (default 30d=2592000s; 0/on=Deep every cycle, off=disabled) | ✅ same semantics (RustFS default 30d, MinIO default off — **different defaults**, RustFS more aggressive) |
| `heal:max_io=100`/`max_sleep=250ms` (waitForLowIO) | mainline throttle thresholds 80%/80%, max_sleep 250ms | ✅ same shape (different threshold model) |
| `heal:drive_workers` (default -1 auto) | in-page concurrency 8 + per-set 1 | ✅ same shape |
| `_MINIO_HEAL_WORKERS` (GOMAXPROCS/2) | `RUSTFS_HEAL_MAX_CONCURRENT_HEALS=4` + `_MAX_CONCURRENT_PER_SET=1` | ✅ |
| `_MINIO_AUTO_DRIVE_HEALING` (on) | `RUSTFS_HEAL_AUTO_HEAL_ENABLE=true` | ✅ |
| `_MINIO_SCANNER` (on) | `RUSTFS_SCANNER_ENABLED=true` | ✅ |
| `scanner:speed` five tiers (default=2x/1s/1m) | same five tiers, same names, same parameters | ✅ |
| `scanner:idle_speed` (on) | `RUSTFS_SCANNER_IDLE_MODE` (true) | ⚠️ semantic direction (HS-14) |
| `scanner:alert_excess_versions=100` | 100 | ✅ |
| `scanner:alert_excess_folders=50000` | 65538 (compatible with the PBS layout) | ⚠️ HS-17 |
| `ilm:expiration_workers=100`/`transition_workers=100` | ecstore expiry/transition worker pools (keys under the ilm subsystem) | ✅ (defaults not checked item by item) |
| `api:stale_upload_cleanup_interval=6h`/`expiry=24h` | ecstore background task, configurable via env | ✅ (defaults not checked item by item) |
| — (none) | `RUSTFS_HEAL_QUEUE_SIZE=10000`, `_TASK_TIMEOUT_SECS=300`, `_INTERVAL_SECS=10`, `_LOW_PRIORITY_MERGE/DROP`, `_PAGE_*`, `_SET_BULKHEAD`, `_MAINLINE_*`, `RUSTFS_SCANNER_CYCLE_MAX_*` budgets, `_MAX_CONCURRENT_SET/DISK_SCANS=4`, `_YIELD_EVERY_N_OBJECTS=128`, etc. | RustFS-specific (finer-grained) |

### 4.8 Where RustFS exceeds MinIO

1. remote_scanner RPC (scan execution pushed down to the remote peer locally, with HMAC authentication/replay cache/fence re-validation/disconnect grace).
2. Persisted leader-epoch CAS fence + usage-snapshot epoch/cycle anti-rollback (MinIO has only the lock, no persisted epoch).
3. Cycle budgets (max_duration/objects/directories) + partial-cycle advancement semantics.
4. per-set/per-disk scan concurrency gates + a cache lock per bucket per set.
5. pending-heal ledger (heal candidates are not lost when the heal channel is full).
6. Drive-replacement durable intent + completion proof state machine + identity fence (MinIO's healingTracker has no proof).
7. mainline throttle foreground pressure gating (driven by permit utilization).
8. Cluster heal control coordinator + envelope replay protection + explicit degraded fallback.
9. Write-path shard bitrot self-verification (the EC:0 case).
10. dirty-usage fast-path wakeup (immediate write-path notification + dirty buckets first).
11. heal runtime observability matrix (priority×source operations snapshot).
12. workload admission integration (the heal scheduler reads the foreground pressure snapshot).

---

## 5. Gap and improvement list

Severity definitions: P1 = behavioral/operational alignment gap (affects production operations or toolchain compatibility); P2 = completeness (the feature exists but is missing a corner); P3 = cleanup/low risk. Each item includes current-state evidence, MinIO behavior, impact, recommendation, and acceptance.

### P1 (8 items)

**HS-01 The MRF/ECDecode/Metadata heal task types have no production trigger; HealEvent unwired**
- Current state: the `HealType::MRF/ECDecode/Metadata` executors are complete (task.rs:1700-2156) but have no production trigger anywhere in the repo; `HealEvent`/`HealEventHandler` (event.rs:50-367) has zero references outside the crate (verified first-hand by grep); channel conversion produces only Cluster/Object/Bucket/Prefix/ErasureSet (channel.rs:566-601).
- MinIO: mrf.go has a standalone MRF queue (capacity 100k, drop-and-count when full), msgp persistence to `.heal/mrf/list.bin` at process exit + startup replay, 1s delay for enqueues <1s (waiting for network recovery), healSleeper rate limiting; on the read path, GetObject part missing/corrupt, metadata rebuild missingBlocks>0, partial Put success, DeleteObject, multipart, and the peer client add up to 7+ delivery points.
- Impact: RustFS's read-repair + write-path convergence covers the main scenarios, but lacks: ① an event-driven Urgent ECDecode rebuild entry (on ecstore decode failure there is currently only Low read-repair); ② a metadata-only heal entry (the scanner's HealMetadata classification exists but goes through ordinary object heal); ③ MRF queue persistence (unconsumed repair intents are lost on restart — partially mitigated by the scanner's pending-heal ledger).
- Recommendation: a pick-one-of-three decision — (a) wire HealEvent (emit events at ecstore decode-failure/metadata-corruption points) + implement a persistent retry ledger; (b) delete the MRF/ECDecode/Metadata dead code and keep only a documentation note; (c) keep the executors and demote HealEvent to an internal API. (a) is recommended, but first quantify whether read-repair already meets the response-time requirements for decode-failure scenarios.
- Acceptance: an e2e decode-failure → Urgent heal-request chain; replay of pending repair intents after restart; HealEvent ring-buffer metrics.

**HS-02 CheckAbandonedParts NotImplemented at all three layers (missing standalone abandoned-data reconciliation entry)**
- Current state: `set_disk/ops/heal.rs:2052-2056`, `core/sets.rs:1144-1148`, `store/heal.rs:258-266` explicitly return `Err(NotImplemented)` at all three layers (verified first-hand); the comment reads "intentionally retained above the set layer until there is a concrete caller".
- MinIO: `CheckAbandonedParts` → per-drive `CleanAbandonedData`: read xl.meta → list UUID data-dirs + inline entries → diff against getDataDirs → delete surplus data-dirs/inline entries and rewrite xl.meta; invoked explicitly on scanner-sampled heals and admin heal Remove.
- Impact: RustFS's in-heal-path `reclaim_orphan_data_dirs_best_effort` (:1428) covers "reclaim orphan directories while healing", but ① there is no standalone trigger point (MinIO can also clean abandoned data before an object reaches the heal threshold); ② orphan inline-data entry cleanup is unconfirmed; ③ multipart orphan reconciliation is explicitly out of scope (a design decision, carried by lifecycle).
- Recommendation: evaluate promoting `reclaim_orphan_data_dirs_best_effort` to a fixed step of heal_object (if it is not already) + implement a real HealOperations::check_abandoned_parts (calling the same reclamation logic), or explicitly document "carried by lifecycle" and close the API surface.
- Acceptance: construct data-dir/inline orphans → cleaned after scanner sampling/admin heal; the three-layer API returns success or an explicitly documented NotSupported.

**HS-03 heal/scanner trace channels missing**
- Current state: zero hits for TraceHealing/TraceScanner (verified first-hand by grepping the whole repo).
- MinIO: `madmin.TraceHealing` (mc admin trace --healing, FuncName=heal.Bucket/heal.Object/heal.CheckAbandonedParts, with dry/remove/mode/version-id/disks/bytes), `TraceScanner` (mc admin scanner trace, supports --filter-size/--response-duration).
- Impact: no way to observe in real time the latency and parameters of individual heal/scanner actions; troubleshooting can rely only on aggregated metrics and logs.
- Recommendation: instrument heal-channel execution and scanner folder/item handling, and hook them into the existing admin trace subscription surface (reuse the rustfs trace infrastructure if it exists; otherwise extend it per madmin TraceType).
- Acceptance: an mc-equivalent tool can subscribe to the heal/scanner trace stream.

**HS-04 Scanner excess S3 events and auditing missing**
- Current state: only `rustfs_scanner_excess_*_total` metrics (versions 100 / version size 1TiB / folders 65538).
- MinIO: emits `s3:ObjectManyVersions` (>100 versions), `s3:ObjectLargeVersions` (cumulative >1TB), `s3:PrefixManyFolders` (>50000 subdirectories) events (UserAgent: Scanner) + scanner:manyversions/largeversions/manyprefixes auditing.
- Impact: users relying on event subscriptions for capacity governance (console/external auditing) receive no alerts.
- Recommendation: hook the scanner_folder alert points into notify event publishing (reusing the lifecycle event-channel semantics).
- Acceptance: after configuring bucket notifications, an over-threshold object triggers an event.

**HS-05 madmin client methods missing**
- Current state: `crates/madmin/src/heal_commands.rs` has only wire types (HealDriveInfo/Infos/HealResultItem); no HealStart/HealStatus/BackgroundHealStatus/ScannerStatus client methods.
- MinIO: madmin-go provides the full client; mc admin heal/scanner/status/trace are all built on it.
- Impact: admin tools like mc cannot directly drive the RustFS heal/scanner admin surface; automated operations must hand-write HTTP.
- Recommendation: add the client following the madmin-go interface shape (the server side is ready; this is pure client work).
- Acceptance: complete the start→query→cancel flow with the madmin client.

**HS-06 admin heal sequence semantics differ from MinIO**
- Current state: duplicate/overlapping requests are dedup-merged (returning the canonical task_id) or evicted; no ErrHealAlreadyRunning/ErrHealOverlappingPaths typed errors (verified first-hand: manager.rs:1309's already_running is an idempotent-startup guard, not an admin semantic); results are snapshot-style queries (1024 entries/8MiB truncation/10min retention), not MinIO's streaming increments (clientToken pulls increments + maxUnconsumedItems=1000 backpressure + 10s keep-alive + 24h unconsumed abort).
- Impact: mc admin heal's interaction model (long connection pulling increments) behaves against RustFS as multiple snapshot polls; automation scripts cannot easily distinguish "merged" from "newly started".
- Recommendation: ① incremental semantics: channel query supports item increments since the last clientToken (or a cursor); ② overlapping requests return a typed error code (or an explicit merged_into field in the receipt — the existing alias mechanism already provides the base); ③ verify forceStart's stop-old-then-start-new semantics.
- Acceptance: an madmin-compatible client polling in the MinIO style can retrieve the full item set.

**HS-07 healing progress and drive-level healing state insufficiently visible externally**
- Current state: byte-recovery progress `progress.bytes_processed = 0 // set to 0 for now` (erasure_healer.rs:967); `HealProgress::estimated_completion_time` is always None and `HealStatistics::add_healed_objects` is never written (progress.rs:38,135-139 zero calls); healthinfo has no per-drive HealInfo equivalent (MinIO HealingDisk: BytesDone/Failed/Skipped, ObjectsTotal baseline, QueuedBuckets/HealedBuckets, Resume snapshot, current object); v3 metrics lack an equivalent of the single drive_health=2 (healing) gauge.
- Impact: during a drive rebuild (potentially hours to days) operations cannot answer "where are we / how much is left / when will it finish".
- Recommendation: ① accumulate bytes in erasure set heal (heal_object already yields the object size); ② read the object-total baseline from usage-cache (the same approach as MinIO); ③ expose a per-drive healing snapshot in admin healthinfo/background status (DiskInfo.healing already exists; add the aggregated exposure); ④ derive the ETA from baseline + rate.
- Acceptance: during a drive rebuild, admin shows byte progress and ETA; an mc info-equivalent output shows the Healing flag.

**HS-08 prefix-level usage not exposed**
- Current state: the DataUsageCache holds the directory-tree entries (organized by hash_path), but `dui()` flattens only to the bucket name (data_usage_define.rs:858-915).
- MinIO: `loadPrefixUsageFromBackend` (30s cache) aggregates prefix usage from each set's `.usage-cache.bin`, consumed by console bucket-prefix statistics.
- Impact: console/front ends cannot show prefix-level usage; there is no API to locate "which prefix is using the space" in a large bucket.
- Recommendation: implement a prefix-flattening query API (the data is already in the cache; this is pure aggregation and exposure work).
- Acceptance: a ListBuckets/PrefixUsage API returns statistics matching the prefix filter.

### P2 (9 items)

**HS-09 get_disk_status always returns Ok (the only TODO)**: `crates/heal/src/heal/storage.rs:930-943` (verified first-hand). Currently no production caller (low risk). Recommendation: delete the method or wire it to the real ecstore disk status (the DiskStatus enum is already defined).

**HS-10 About 1/3 of HealStorageAPI methods are dead code**: get_object_meta/get_object_data/put_object_data/delete_object/verify_object_integrity/ec_decode_rebuild/get_disk_status/format_disk/heal_bucket_metadata/get_object_size/get_object_checksum/list_objects_for_heal (the non-paginated version, with its own memory_heavy warning) all have 0 callers. Recommendation: clean up or wire them together with the HS-01 decision (dead interfaces mislead future maintainers into thinking a call path exists).

**HS-11 bitrot self-test missing**: MinIO at startup runs bitrotSelfTest over known vectors for the four algorithms and exits Fatal on failure (guarding against silent data corruption). RustFS has no equivalent (verified first-hand by grep). Recommendation: at startup, run known-vector self-tests for HighwayHash256S and the other algorithms in use (low cost, high value).

**HS-12 object-level healing metadata marker evaluation**: during heal, MinIO tags objects with `x-minio-healing:true`, and RenameData uses it to skip version cleanup/legacy purge (missing it lets heal and concurrent deletes destroy each other). RustFS has no object-level marker (verified first-hand by grep; object.rs has no healing branch) and relies on NSLock + rename semantics. Recommendation: audit whether the RustFS rename-commit path has a "heal commit racing concurrent delete/version cleanup" window; if not, document the difference, and if so, add a marker-equivalent mechanism.

**HS-13 erasure set heal lacks "skip newly written / ILM-expired versions" filters**: MinIO resync skips versions with ModTime>tracker.Started (so heal does not chase the tail of new writes) and ILM-expired versions (so work is not wasted). RustFS's erasure_healer does not implement such filters (per-version dedup exists; time/ILM filters do not). Impact: a long tail on rebuild completion (the completion decision for a continuously written bucket is pushed out by new versions) and wasted heal work. Recommendation: add a started_at time filter at the disk-walk enumeration point + an evaluator pre-check.

**HS-14 scanner idle semantics point the opposite way from MinIO**: MinIO `scanner:idle_speed=on` (default) means "throttle only when the cluster is idle, full speed when busy"; RustFS `RUSTFS_SCANNER_IDLE_MODE=true` (default) is a master switch for rate limiting (false = never sleep at all). The default behaviors may end up similar (both throttle), but the parameter semantics are not interchangeable; migration docs must state this explicitly; if mc config compatibility is the goal, a rename/re-semantization is needed. Recommendation: document the difference first, then evaluate aligning the semantics.

**HS-15 alert_excess_folders default differs**: RustFS 65538 (compatible with the PBS/Proxmox layout, scanner_folder.rs:79) vs MinIO 50000. The behavioral difference is that the trigger threshold differs out of the box. Recommendation: document it (keeping 65538 has local rationale).

**HS-16 single-node default-cycle hook not enabled**: `single_disk_default_cycle_secs(_features) -> None` is always empty (scanner.rs:1428-1430); single-node deployments get no dedicated default-cycle override. Recommendation: after deciding the single-node default-cycle policy, enable or delete the hook.

**HS-17 DeleteAllVersions batch-optimization check**: MinIO uses the single DeletePrefix+DeletePrefixObject call instead of per-version fan-out. Whether RustFS's expiry-queue path has the same optimization was not verified line by line (integration tests cover behavioral correctness). Recommendation: check the `apply_expiry_rule` all-versions delete path; if there is no prefix single-call optimization, evaluate adding it.

### P3 (3 items)

**HS-18 trash/temp-directory two-stage cleanup detail check**: MinIO cleans `.minio.sys/tmp/.trash` (delete_cleanup_interval default 5m + deleteCleanupSleeper) and stale uploads are renamed into trash in two stages. RustFS has delete_tail_activity.rs and the stale multipart task; whether the two-stage semantics are fully aligned was not verified line by line. Recommendation: align or document.

**HS-19 root-heal direct path is dead code**: `should_handle_root_heal_directly` is always false (admin/handlers/heal.rs:1200-1202, locked by a test); the store.heal_format direct branch is unreachable. Recommendation: delete the dead branch or restore the direct path as a fallback for cluster-coordination failure.

**HS-20 compat flags and dead metrics cleanup**: `RUSTFS_SCANNER_INLINE_HEAL_ENABLE` (enabling only warns) + the dead `rustfs_scanner_inline_heal_total` metric + the scanner-domain code in `rustfs_common::metrics` awaiting layering migration (backlog #1843 already filed). Recommendation: clean up along with the layering migration.

### Not pursuing parity by design (7 items, recorded to prevent later misreading as gaps)

1. **bloom filter**: removed from MinIO master; RustFS reuses `.bloomcycle.bin` as the cycle/epoch fence, consistent with MinIO's current state.
2. **scanner cluster single leader**: both sides agree; RustFS additionally has the epoch fence.
3. **heal emits no S3 bucket notification**: both sides agree (heal results go through admin status).
4. **incomplete multipart not executed inside scanner/ILM**: both sides agree (independent background routine).
5. **inline heal removal**: a deliberate RustFS choice (the scanner only enqueues); MinIO's applyHealing inline path is not a parity target.
6. **heal-sequence resident keep-alive (10s blank write-back)**: RustFS's snapshot-query model differs; handling incremental semantics per HS-06 is enough — do not copy the streaming keep-alive.
7. **`.trash`/`tmp-old` path-name compatibility**: RustFS's layout constants are independent; no literal alignment with MinIO paths.

---

## 6. Configuration defaults master table (RustFS)

heal (env prefix `RUSTFS_HEAL_`, `crates/config/src/constants/heal.rs`, consumed at `manager.rs:724-800`):

| Setting | Default | Hot update |
|---|---|---|
| AUTO_HEAL_ENABLE | true | no |
| QUEUE_SIZE | 10000 | no |
| INTERVAL_SECS | 10 | no (fixed at startup) |
| TASK_TIMEOUT_SECS | 300 | no |
| MAX_CONCURRENT_HEALS | 4 | no |
| MAX_CONCURRENT_PER_SET | 1 (≤min(global, value)) | no |
| LOW_PRIORITY_MERGE_ENABLE | true | no |
| LOW_PRIORITY_DROP_WHEN_FULL | true | no |
| PAGE_OBJECT_CONCURRENCY | 8 (Deep/AutoHeal forced to 1) | no |
| EVENT_DRIVEN_SCHEDULER_ENABLE | true | no |
| SET_BULKHEAD_ENABLE | true | no |
| PAGE_PARALLEL_ENABLE | true | no |
| MAINLINE_THROTTLE_ENABLE | true | no |
| MAINLINE_READ/WRITE_UTILIZATION_HIGH_PERCENT | 80/80 | no |
| MAINLINE_MAX_SLEEP_MS | 250 | no |
| (master switch) RUSTFS_HEAL_ENABLED | true | no |
| admin subsystem heal.bitrot_cycle | 30d | yes (via scanner runtime config) |

scanner (admin subsystem `scanner`, `crates/config/src/constants/scanner.rs` + `ecstore/src/config/scanner.rs` + `runtime_config.rs:527-673`):

| Key | env | Default |
|---|---|---|
| speed | RUSTFS_SCANNER_SPEED | default (2x/1s/60s) |
| delay / max_wait / cycle / start_delay | RUSTFS_SCANNER_* | derived/empty |
| cycle_max_duration/objects/directories | …_MAX_* | 0 (unlimited) |
| bitrot_cycle | …_BITROT_CYCLE_SECS | 2592000 (30d; 0/on=every cycle, off=disabled) |
| idle_mode | …_IDLE_MODE | true |
| cache_save_timeout | …_CACHE_SAVE_TIMEOUT_SECS | 30s |
| max_concurrent_set_scans / disk_scans | …_MAX_CONCURRENT_* | 4/4 |
| yield_every_n_objects | …_YIELD_EVERY_N_OBJECTS | 128 |
| alert_excess_versions / version_size / folders | …_ALERT_* | 100 / 1TiB / 65538 |

scanner-internal env: `RUSTFS_DATA_USAGE_UPDATE_DIR_CYCLES=16`, `RUSTFS_HEAL_OBJECT_SELECT_PROB=1024`, `RUSTFS_SCANNER_DEEP_VERIFY_COOLDOWN_SECS=60`, `RUSTFS_DATA_USAGE_FAILED_OBJECT_TTL_SECS=86400`/`_MAX=10000`, `RUSTFS_LOCK_ACQUIRE_TIMEOUT=5s`, `RUSTFS_SCANNER_ENABLED=true`, `RUSTFS_SCANNER_INLINE_HEAL_ENABLE=false` (compat warning).

All 17 scanner keys support the env > config dual channel + admin PUT hot update (generation+Notify takes effect immediately); heal runtime parameters are currently env-only (no admin hot-update entry; the `Arc<RwLock<HealConfig>>` structure is already reserved).

---

## 7. Related backlog / history index

- Automatic drive-replacement healing series (closed loop): backlog #1786 (redundant false-green algorithm), #1787 (target-slot restriction), #1789 (binding resume and the healing marker to the replacement instance), #1791 (black-box/white-box acceptance matrix).
- #801 DiskInfo.healing never assigned (fixed and closed; the assignment chain now lives at `set_disk/mod.rs:4988`).
- #1651 Scanner metrics node/source/bucket-drive dimensions (OPEN; related to §3.8/§4.6 of this analysis).
- #1843 crates/common 83% scanner/heal domain code layering migration (OPEN; includes HS-20).
- Historical defects cited in code comments (now guarded with regression tests): #856/#799 B7 (offline drive falsely recorded healed), #855/B6/#1033 (a skip round must not be marked complete), #920 (sub-quorum union enumeration), #856 B5 (per-version resume), #5173 (bitrot trailing bytes), #5029 (stale-version merge at regression nodes).
- v1 parity document: `docs/rustfs-heal-scanner-vs-minio-parity-assessment.md` (superseded by this document); the landing playbook `docs/rustfs-heal-scanner-vs-minio-improvement-playbook.md` (some entries have since been overtaken by implementation).
- Drive-replacement deep analyses: `docs/new-disk-replacement-and-healing-deep-analysis-zh.md`, `docs/node-disk-identity-and-healing-analysis-zh.md`.

## 8. Audit method and limitations

- Four parallel audit tracks (heal crate file by file, scanner crate file by file, ecstore integration-layer wiring, MinIO master source study) + the main session verifying each key "missing" conclusion first-hand (the get_disk_status TODO, HealEvent's zero external references, .bloomcycle.bin having no bloom implementation, check_abandoned_parts NotImplemented at all three layers, the ETag fallback being implemented, zero trace-channel hits, the already_running semantics).
- Points not verified line by line (marked "unconfirmed / not checked line by line" in the text): the DeleteAllVersions prefix single-call optimization (HS-17), trash two-stage cleanup details (HS-18), ilm worker default comparisons, stale multipart default comparisons, mc CLI flag spellings (MinIO side). Of these, HS-17 and HS-18 completed line-by-line verification on 2026-08-19; conclusions in §9.2/§9.3.
- MinIO-side references follow its master `7aac2a2c5b`; RustFS-side line numbers follow the 2026-08-16 workspace — for later evolution, search by symbol name instead.

## 9. Landing results (updated 2026-08-19)

All 14 sub-issues derived from this audit (backlog #1865~#1878) are closed. This section is the final disposition record for the gap list HS-01~HS-20, and also the incremental baseline for the next parity re-audit.

### 9.1 Landed (all PRs merged to main)

- HS-01 MRF wiring + persistent repair ledger (#1865, PR #6189): decision (a) chosen. common MRF channel (bounded 8192, try_send never blocks) + heal mrf_queue (100k entries / 8MiB dual-capacity ring) + `buckets/.heal/mrf/journal.bin` CRC-persisted replay (torn tail truncated, deleted after replay) + three delivery points (read decode_error→Urgent ECDecode, scanner metadata corruption→High Metadata, add_partial→Normal) + `RUSTFS_HEAL_MRF_ENABLE` one-switch rollback.
- HS-02 abandoned parts/data-dir reconciliation (#1866, PR #6179): wired up the abandoned-check entry, retaining dry-run / reclaim counters.
- HS-03 heal/scanner trace channels (#1867, PR #6179): in-process trace bus + `/v3/trace` admin streaming subscription + heal task / abandoned-parts / scanner folder / ILM / heal-candidate trace producers.
- HS-04 scanner excess S3 events (#1868, PR #6176): the three events `s3:Scanner:ManyVersions/LargeVersions/BigPrefix` + 24h edge cooldown; the HS-15 threshold delta documented (`docs/operations/scanner-excess-alerts.md`).
- HS-05 madmin client phase 1 (#1869, PR #6166): SigV4 admin client heal/scanner methods; incremental-consumption methods await a follow-up (the protocol was already folded in by HS-06).
- HS-06 admin heal incremental semantics and typed overlap (#1870, PR #6206): `sinceSeq/nextSeq/minSeq` incremental cursor (wire additive; absent = full snapshot) + `RUSTFS_HEAL_OVERLAP_POLICY` (default merge unchanged; under minio_error, typed AlreadyRunning/OverlappingPaths rejections) + forceStart stops the old sequence before starting the new one.
- HS-07 healing progress visibility (#1871, PR #6179): data-usage total baseline + baseline/current/healed counters.
- HS-08 prefix usage (#1872, PR #6171): `GET /v3/usage/{bucket}`.
- HS-11 bitrot startup self-test (#1873, PR #6165).
- HS-13 heal skip filters (#1875, PR #6179): filter-hit versions are no longer counted as failures.
- HS-16 single-node cycle hook (#1878, PR #6250): removed the always-None hook; the decision record is in `docs/operations/heal-scanner-parity-notes-zh.md`.
- HS-09/10/19/20 dead-code cleanup batch (#1877, PR #6256): net −911 lines, zero behavior change; the `get_disk_status` TODO (the repo's only product TODO) cleared to zero; `ec_decode_rebuild`/`get_object_meta`, kept due to the HS-01 linkage, are retained with Reserved annotations (MRF currently executes via `heal_object`).

### 9.2 Confirmed "already implemented / not a gap" after verification (audit-period misjudgment corrections, four in total)

- bloom filter (corrected in §0): removed from MinIO master; both sides now agree.
- ETag fallback arbitration (corrected in §0): RustFS already has the implementation (`set_disk/ops/heal.rs`).
- HS-17 (#1876, closed after line-by-line verification on 2026-08-19): the DeleteAllVersions prefix single-call optimization is fully implemented in RustFS — `apply_expiry_on_non_transitioned_objects` sets `delete_prefix + delete_prefix_object` for the two `delete_all()` actions and then performs a single `delete_object` call (`bucket_lifecycle_ops.rs:5047-5056`); the SetDisks branch takes one write lock + one all-version quorum read + inline per-version object-lock checks (`set_disk/ops/object.rs:5566-5612`), aligned line by line with MinIO `expire.go`'s `applyExpiryOnNonTransitionedObjects`. The item §8 listed as "not verified line by line" now has a conclusion: the current state is already the optimized path; nothing to implement.
- HS-14 (#1878, checked alongside PR #6250): MinIO's "idle = throttle only when idle" was the behavior before 2024-01 minio/minio#18734 (`scannerIdleMode` is now a static config; `idle_speed=on` by default means always throttling per the speed tier — the "idle" naming is a historical leftover); RustFS's `RUSTFS_SCANNER_IDLE_MODE` points the same way as MinIO's current semantics, and additionally has a foreground-read backoff floor that MinIO lacks. The real migration traps (the variable must carry the `RUSTFS_` prefix, the `on/off` vs `true/false` vocabulary, `false` also turning off foreground protection) are documented in `docs/operations/heal-scanner-parity-notes-zh.md`.

### 9.3 Audit-style conclusions (no code change needed)

- HS-12 (#1874, PR #6183): the class of race MinIO defends against with `x-minio-healing` does not exist — every commit surface for the same (bucket, object) is mutually exclusive under the same object-level ns write lock, and the heal lock guard covers the whole rename commit; delivered 2 concurrency-invariant regression tests + the intersection matrix in `docs/operations/heal-concurrency-safety-notes-zh.md`.
- HS-18 (#1878, line-by-line verification on 2026-08-19): trash/tmp three-stage cleanup fully aligned — stale multipart isolation-cleanup is equivalent and safer (`delete_all_with_quorum` recursively deletes per drive, i.e. the `move_to_trash` rename into `.rustfs.sys/tmp/.trash`, plus lock + fence); trash draining is essentially equivalent (no per-entry sleeper throttling; the 5m cycle naturally rate-limits); tmp non-trash 24h reclamation is equivalent (RustFS's 5m is more timely than MinIO's 6h); the three cycle defaults 24h/6h/5m all align. The item §8 listed as "not verified line by line" now has a conclusion.

### 9.4 Handed over to follow-ups (summarized in the backlog#1862 comment thread)

HS-01 bitrot GET→MRF full-chain e2e, kill -9 journal replay e2e, queue-full RSS stress test (≤ budget+10%); HS-05/06 madmin incremental-consumption methods + single-source wire + embedded e2e + multi-round polling soak; HS-08 multi-drive scanner cycle e2e; HS-04 excess audit entries; HS-18 the stale-multipart crash-residue window below quorum (crashing mid-fan-out with already-cleaned drives > parity means FileNotFound is not in the ignore set, so convergence is unnatural; the fix needs a dedicated quorum variant).

Recommendation for the next re-audit: trigger it after the next big heal/scanner feature lands, using this section as the incremental baseline.
