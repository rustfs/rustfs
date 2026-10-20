# Scanner Usage Authority Decision

**Use this when:** deciding whether quota admission depends on scanner data usage, removing scanner publication layers, or designing a scanner storage boundary.
**Source of truth:** `crates/ecstore/src/bucket/quota/reservation.rs` (`QuotaLedger`, `QuotaContext::reserve`, `exact_bucket_usage`), `crates/ecstore/src/bucket/quota/checker.rs` (`QuotaChecker::get_real_time_usage`), `rustfs/src/app/object/shared.rs` (`map_quota_check_outcome`, `apply_quota_admission`), [scanner-usage-publication.md](scanner-usage-publication.md), and the scanner publication state under `crates/scanner/src/scanner/`.

## Decision

Date: 2026-10-19. Supersedes the 2026-09-03 decision recorded below.

Hard quota admission is owned by the durable reservation ledger, not by scanner
data usage. `QuotaLedger::accounted_usage` is initialised and reconciled by
`exact_bucket_usage`, a `list_object_versions` walk of the bucket, and
`QuotaContext::reserve` admits a write against `admitted_usage + growth <=
quota_limit`; neither reads a scanner snapshot. Every quota saved through the
admin API carries `reservation_protocol = V2`.

Scanner data usage is best-effort reporting input: admin info, Console,
metrics, and the degraded fallback of `QuotaChecker::get_real_time_usage` for
buckets whose quota predates the reservation protocol. It must not be able to
fail a write on a bucket the ledger admits. The known violation is
`map_quota_check_outcome`, which turns `QuotaError::UsageUnavailable` into
`ServiceUnavailable` even when `QuotaCheckResult::uses_durable_reservations`
is true and `apply_quota_admission` already admits unconditionally.

## Rationale

The ledger already carries the write-path accounting that the 2026-09-03
decision assumed only the scanner could provide: an exact baseline, per-write
reservations settled at commit, and a reconcile step after a denial. With the
admission decision made there, keeping scanner usage authoritative buys no
enforcement and costs every deployment the proof machinery that makes a
distributed background scan safe to feed a quota: leases that reject pending
PUT commits, observed-versus-authoritative routing, and a usage floor whose
unreadability stops the scanner. Buckets without a configured quota never read
usage at all (`check_quota_with_usage_reporting` returns on the zero-I/O path),
so the authority protected a narrow case while blocking usage statistics,
lifecycle discovery, and heal discovery for everyone.

## Consequences

- [scanner-usage-publication.md](scanner-usage-publication.md) keeps the
  fences that protect consistency across leadership and data movement:
  leadership claim, storage publication epoch, CAS revision, per-set
  freshness, tier registry generation. The layers that existed only to make
  scanner usage a quota authority are scheduled for staged removal under a
  compatibility window: the usage floor as hard-quota input, observed-versus-
  authoritative routing as an admission distinction, and strict
  publication-lease rejection of pending ordinary namespace commits.
- Buckets with `reservation_protocol = None` are migrated by re-saving their
  quota configuration through the existing save path; until migrated they keep
  the degraded floor path and fail closed only when neither authoritative usage
  nor a floor exists. The re-save is triggered lazily by the first quota read
  of such a bucket (`QuotaChecker::get_quota_config`) and in bulk by
  `POST /rustfs/admin/v3/quota-migration`; the limit and type are unchanged,
  the write keeps the incarnation, quota-revision and fleet-proof checks of
  the admin save, and a bucket whose quota changed in between is skipped,
  not overwritten. See
  [durable-quota-reservation-v2.md](../operations/durable-quota-reservation-v2.md).
  Note: the admin default for a new quota is still protocol v1
  (`BucketQuota::new`); v2 is chosen per request or by this migration. Both
  are durable, so neither is a migration candidate.
- [scanner-per-bucket-convergence.md](scanner-per-bucket-convergence.md) no
  longer cites quota authority as a reason to keep uncertified buckets
  fail-closed; an uncertified bucket keeps its last certified value as its
  reported value.
- The scanner storage boundary (#2219) may be designed against the reporting
  contract; it must still expose the CAS key-value, cycle lock, and
  recovery-marker capabilities, but the usage-floor and observed-snapshot
  capabilities become reporting surfaces rather than proof obligations.

### Remaining "authority" consumers after D3 (inventory, 2026-10-19)

The quota path itself no longer reads scanner usage as authority for a
durable bucket: `QuotaChecker::check_quota_with_usage_reporting` reports an
unavailable lookup as `current_usage = None` and `map_quota_check_outcome`
never sees `UsageUnavailable` for it. The surfaces below still carry the
authority shape. Each is listed with its production readers, what it means
now, the condition under which it can be retired, and the compatibility
window. Retirement itself is separate work; none of it is required for
correctness after this decision.

| Surface | Readers today | Meaning after D3 | Retirement condition | Compatibility window |
|---|---|---|---|---|
| `usage_snapshot_complete` (`DataUsageInfo::is_complete_bucket_usage_snapshot`) | quota-facing loader `discard_incomplete_bucket_usage`, `normalize_loaded_data_usage`, `replace_bucket_usage_memory_from_info_if_generation`, `get_bucket_usage_memory` in `crates/ecstore/src/data_usage/mod.rs`; server/pool info in `crates/ecstore/src/diagnostics/admin_server_info.rs`; `narrow_data_usage_snapshot_to_measured_buckets` in `rustfs/src/app/admin_usecase.rs`; decommission capacity in `crates/ecstore/src/core/pools.rs`; obs metrics, heal progress baseline; scanner `data_usage_info_is_cold`, `complete_scanner_cache_snapshot_plan_digest`, `reject_incomplete_snapshot` | A completeness mark for reporting (admin, Console, metrics, heal progress). For quota it only decides whether a memory value exists, which can no longer fail a durable write. | No bucket with `reservation_protocol = None` remains (lazy trigger plus admin sweep), so the loader's discard is no longer an admission gate; the mark itself stays as the reporting contract. | Until every quota is migrated, plus one release in which the snapshot-protocol admission path is still compiled. |
| `lookup_degraded_bucket_usage_baseline` / `degraded_baseline` (`crates/ecstore/src/data_usage/mod.rs`) | `QuotaChecker::get_real_time_usage` (`crates/ecstore/src/bucket/quota/checker.rs`); `session_capacity_view` in `rustfs/src/protocols/client.rs`; fed by `load_data_usage_from_backend_with_baseline`, `backfill_degraded_baseline_from_observed`, `cache_data_usage_snapshot_result` | Admission input only for snapshot-protocol buckets; for durable buckets it only shapes the reported `current_usage`. | Same as above; afterwards it is a reporting fallback that can be dropped together with the capacity view's choice of source. | Same as above. |
| Usage floor identity (the `scanner_epoch`/`scanner_cycle` identity of `buckets/.usage.v2.json` and `buckets/.usage.json` under `.rustfs.sys`, each with a `.bkp`; there is no separate floor object) | `persisted_usage_floor_for_startup`, `persisted_usage_floor`, `apply_persisted_usage_floor`, `reset_scanner_cycle_recovery_for_intent`, the rc.1–rc.3 `legacy_*_usage_fence` readers in `crates/scanner/src/scanner/cycle_state.rs`; `data_usage_info_has_persisted_baseline_identity` in `crates/scanner/src/scanner.rs`; `usage_snapshot_for_epoch_fence` in `crates/scanner/src/scanner/leadership.rs`; `read_data_usage_persist_baseline` in `crates/scanner/src/scanner/usage_store.rs`; `UsageSnapshotSource` resolution in `crates/ecstore/src/data_usage/mod.rs` | The scanner's own monotonic cycle fence for leadership and cycle recovery. It is no longer a quota proof: an unreadable floor stopping the leader iteration (`usage_floor_load_failed`) is a scanner-availability choice, not a quota requirement. | The quota fallback no longer reads the legacy `.usage.json` baseline; then "floor unreadable stops the scanner" can become "report and rebuild", and the rc.1–rc.3 fences (`RUSTFS_COMPAT_TODO` backlog-2102 / backlog-2122) retire on their own schedule. Cycle monotonicity stays. | One release after the compat markers' own deadline. |
| Observed-versus-authoritative routing (`.usage.observed.json` versus `.usage.v2.json`) | decision in `classify_nsscanner_cycle`, `apply_scanner_cycle_publication_marks`, `publish_observational_snapshot`, `scanner_cycle_activity_status` (`crates/scanner/src/scanner_io.rs`); write routing in `crates/scanner/src/scanner/usage_store.rs`; readers `load_observed_data_usage_snapshot`, `select_admin_data_usage_snapshot`, `merge_partial_observation_for_admin` and the degraded-baseline backfill (`crates/ecstore/src/data_usage/mod.rs`); scanner cold-start and cache reuse; `ScannerCyclePublicationPlan::for_result` | A completeness-marked report. "Never a hard-quota authority" is moot for durable buckets; the distinction remains a reporting mark (`usage_snapshot_observational`, `converged`). | The quota fallback stops distinguishing the two objects; then the pair can collapse into one snapshot carrying the marks, after a reader-first migration of the observed-object consumers (admin, Console, obs). | Readers accept both layouts for one release before the writer changes. |
| Publication-lease strict mode (`purpose == Authoritative && namespace_commits_pending()`) | `acquire_scanner_publication_lease_with_purpose`, `validate_scanner_publication_lease_with_purpose`, `acquire_scanner_publication_lease_guard_with_target`, `scanner_data_usage_publication_blocked` in `crates/ecstore/src/store/mod.rs`; `namespace_commits_pending` in `crates/ecstore/src/runtime/instance.rs`; `scanner_local_publication_defer_reason` in `crates/scanner/src/scanner.rs`; lease RPCs and the lease token on `RenameData`/`Delete` in `crates/protos/src/node.proto` | A pending ordinary PUT commit is a write-barrier event (the cycle is superseded or the bucket pending), not a reason to deny an authoritative lease. Data-movement and generation checks stay. | Drop the pending-commit rejection and its two revalidation twins, and the pending-commit term of the local barrier; keep the movement fence and the lease token on writes. The local barrier's term is handled by the scanner cycle-termination work; the remote lease follows once that lands. | Mixed versions are safe: an old peer still rejects and the coordinator treats that as retryable (`scanner_publication_lease_error_is_retryable`), so the removal rolls out node by node. |

## Superseded decision (2026-09-03)

RustFS keeps scanner data usage as authoritative for quota admission.

This selects option A from the backlog decision record: the scanner publication protocol remains necessary while quota admission consumes scanner usage. The cycle epoch, publication CAS, data-movement fence, tier-registry fence, observed snapshot layer, and persisted usage floor are retained and documented as protocol invariants rather than treated as removable compatibility clutter.

### Rationale at the time

Quota is a write-path admission decision, so serving quota from best-effort scanner data would turn temporary scanner lag into under-enforcement. The current design therefore needs an availability story for authoritative usage instead of deleting the proof layers that make it authoritative.

The scanner usage floor provides that availability story. It is a lower bound used when a complete authoritative snapshot is not available, including cold startup, upgrade recovery, and incomplete-cycle repair. Observed snapshots remain useful for admin and observability, but they do not become quota authority.

### Consequences at the time

#2214 is the hard design input for future usage-publication changes. A future proposal may still choose soft quota and MinIO-style best-effort usage, but that would be a product change with its own staged compatibility plan for persisted artifacts.

#2219 may design the scanner storage boundary against the current authoritative protocol. The interface must include the CAS key-value, cycle lock, usage-floor, observed-snapshot, and recovery-marker capabilities needed by [scanner-usage-publication.md](scanner-usage-publication.md); it must not hide those proof obligations behind a generic object-store trait.
