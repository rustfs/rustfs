# Scanner Usage Authority Decision

**Use this when:** deciding whether quota admission depends on scanner data usage, removing scanner publication layers, or designing a scanner storage boundary.
**Source of truth:** `crates/ecstore/src/bucket/quota/reservation.rs` (`QuotaLedger`, `QuotaContext::reserve`, `exact_bucket_usage`), `crates/ecstore/src/bucket/quota/checker.rs` (`QuotaChecker::get_real_time_usage`), `rustfs/src/app/object/shared.rs` (`map_quota_check_outcome`, `apply_quota_admission`), [scanner-usage-publication.md](scanner-usage-publication.md), and the scanner publication state under `crates/scanner/src/scanner/`.

## Decision

Date: 2026-10-19. Supersedes the 2026-09-03 decision recorded below.

Hard quota admission is owned by the durable reservation ledger, not by scanner
data usage. `QuotaLedger::accounted_usage` is initialised and reconciled by
`exact_bucket_usage`, a `list_object_versions` walk of the bucket, and
`QuotaContext::reserve` admits a write against `admitted_usage + growth <=
quota_limit`; neither reads a scanner snapshot. The admin API defaults to
reservation protocol v1 through `BucketQuota::new`. Protocol v2 is selected
explicitly or by quota migration; both protocols use durable reservations.

Scanner data usage is best-effort reporting input for durable quota buckets:
`QuotaChecker::report_only_usage` returns the existing usage lookup's value or
`None`, without explicitly consulting the persisted degraded floor. Both quota checks and quota statistics
use this optional report. `apply_quota_admission` leaves admission to the ledger
when `QuotaCheckResult::uses_durable_reservations` is true. Buckets whose quota
predates reservation protocols retain `QuotaChecker::get_real_time_usage` and
its degraded fallback until their quota is migrated.

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
| `usage_snapshot_complete` (`DataUsageInfo::is_complete_bucket_usage_snapshot`) | quota-facing loader `discard_incomplete_bucket_usage`, `normalize_loaded_data_usage`, `replace_bucket_usage_memory_from_info_if_generation`, `get_bucket_usage_memory` in `crates/ecstore/src/data_usage/mod.rs`; server/pool info in `crates/ecstore/src/diagnostics/admin_server_info.rs`; `narrow_data_usage_snapshot_to_measured_buckets` in `rustfs/src/app/admin_usecase.rs`; decommission capacity in `crates/ecstore/src/core/pools.rs`; obs metrics, heal progress baseline; scanner `data_usage_info_is_cold`, `complete_scanner_cache_snapshot_plan_digest`, `reject_incomplete_snapshot` | A completeness mark for reporting (admin, Console, metrics, heal progress). For quota it only decides whether a memory value exists, which can no longer fail a durable write. | No configured finite quota with `reservation_protocol = None` remains (lazy trigger plus admin sweep), so the loader's discard is no longer an admission gate; the mark itself stays as the reporting contract. | Until every quota is migrated, plus one release in which the snapshot-protocol admission path is still compiled. |
| `lookup_degraded_bucket_usage_baseline` / `degraded_baseline` (`crates/ecstore/src/data_usage/mod.rs`) | `QuotaChecker::get_real_time_usage` for snapshot-protocol admission and explicit non-durable reporting; `session_capacity_view` in `rustfs/src/protocols/client.rs`; fed by `load_data_usage_from_backend_with_baseline`, `backfill_degraded_baseline_from_observed`, `cache_data_usage_snapshot_result` | Retired from durable-v1/v2 quota checks and statistics: an unknown memory value is reported as `None`. Still an admission input for `reservation_protocol = None` and a capacity-reporting fallback. | Retire the remaining quota consumer only after every configured snapshot-protocol quota and every quota-setting node has migrated; capacity reporting remains a separate source-selection contract. | Durable reporting can roll node by node among D3-capable readers. The snapshot-protocol fallback stays for the migration window plus one release with its admission path compiled. |
| Usage floor identity (the `scanner_epoch`/`scanner_cycle` identity of `buckets/.usage.v2.json` and `buckets/.usage.json` under `.rustfs.sys`, each with a `.bkp`; there is no separate floor object) | `persisted_usage_floor_for_startup`, `persisted_usage_floor`, `apply_persisted_usage_floor`, `reset_scanner_cycle_recovery_for_intent`, the rc.1–rc.3 `legacy_*_usage_fence` readers in `crates/scanner/src/scanner/cycle_state.rs`; `data_usage_info_has_persisted_baseline_identity` in `crates/scanner/src/scanner.rs`; `usage_snapshot_for_epoch_fence` in `crates/scanner/src/scanner/leadership.rs`; `read_data_usage_persist_baseline` in `crates/scanner/src/scanner/usage_store.rs`; `UsageSnapshotSource` resolution in `crates/ecstore/src/data_usage/mod.rs` | The scanner's own monotonic cycle fence for leadership and cycle recovery. It is no longer a quota proof: an unreadable floor stopping the leader iteration (`usage_floor_load_failed`) is a scanner-availability choice, not a quota requirement. | The quota fallback no longer reads the legacy `.usage.json` baseline; then "floor unreadable stops the scanner" can become "report and rebuild", and the rc.1–rc.3 fences (`RUSTFS_COMPAT_TODO` backlog-2102 / backlog-2122) retire on their own schedule. Cycle monotonicity stays. | One release after the compat markers' own deadline. |
| Observed-versus-authoritative routing (`.usage.observed.json` versus `.usage.v2.json`) | decision in `classify_nsscanner_cycle`, `apply_scanner_cycle_publication_marks`, `publish_observational_snapshot`, `scanner_cycle_activity_status` (`crates/scanner/src/scanner_io.rs`); write routing in `crates/scanner/src/scanner/usage_store.rs`; readers `load_observed_data_usage_snapshot`, `select_admin_data_usage_snapshot`, `merge_partial_observation_for_admin` and the degraded-baseline backfill (`crates/ecstore/src/data_usage/mod.rs`); scanner cold-start and cache reuse; `ScannerCyclePublicationPlan::for_result` | A completeness-marked report. "Never a hard-quota authority" is moot for durable buckets; the distinction remains a reporting mark (`usage_snapshot_observational`, `converged`). | The quota fallback stops distinguishing the two objects; then the pair can collapse into one snapshot carrying the marks, after a reader-first migration of the observed-object consumers (admin, Console, obs). | Readers accept both layouts for one release before the writer changes. |
| Publication-lease strict mode (`purpose == Authoritative && namespace_commits_pending()`) | Former acquire/validate/guard and local-barrier rejection sites; current leases still protect movement generations, purpose, expiry and publication writes | The ordinary-PUT veto is already removed: lease handling fences data movement, and cycle termination no longer treats ordinary PUT as data movement (`rustfs/rustfs#8696`). Namespace counters retained for diagnostics and ownership are not a quota veto. | No remaining pending-ordinary-commit rejection to remove at this baseline. Retain movement/epoch exhaustion checks, terminal movement debt, token purpose and lease-token validation on writes. | An older rejecting peer remains retryable through `scanner_publication_lease_error_is_retryable`. Keep lease RPC fields and purpose readers during mixed-version rollout; this retirement does not authorize deleting them. |

### Durable reporting fallback retirement

`check_quota_with_usage_reporting` and `get_quota_stats` no longer call
`usage_or_degraded_baseline` through `report_only_usage`. A missing,
incomplete, legacy-only or observed-only snapshot can therefore change a
durable bucket's `current_usage` and `remaining` from a cached floor to `None`.
Unknown is never reported as zero. The admin quota-check preview uses the
existing unknown-usage branch, so its advisory `allowed` can change from false
to true when only a degraded floor had exceeded the limit; it does not reserve
capacity or override the ledger decision on an actual write. Each unavailable
report increments
`rustfs_quota_usage_report_unavailable_total`; admission still uses the same
reservation ledger and exact baseline. Quota-free admission keeps its existing
zero-usage-I/O path.

The existing `get_bucket_usage_memory` lookup retains its cache-refresh
semantics: a distributed persisted-usage lookup or an expired local cache can
still load the backend. Retiring the explicit degraded-floor consumer does not
make durable reporting a zero-I/O operation. The no-early-reload regression
covers the separate `reload_unknown_bucket_usage` trigger only.

This consumer removal needs no format change, writer activation or quota
migration. Among D3-capable nodes, an older node can report the old floor while
a newer node reports unknown; both keep ledger admission and return a successful
best-effort report. Snapshot-protocol buckets (`reservation_protocol = None`)
still take `get_real_time_usage`, including early refresh, the legacy and
observed degraded baseline, and fail-closed admission when no grounded value
exists. Earlier nodes that still make scanner availability a durable-write
veto must first reach the D3 baseline; this change does not prove that earlier
fleet safe.

Rollback to a D3 reader before this consumer removal restores floor-based
optional reporting. No scanner snapshot, backup, quota record or reservation
record is removed or rewritten to enable either direction. Limits, quota types,
incarnations and reservation protocols remain unchanged.

The other retirement items keep independent compatibility boundaries and must
be delivered separately:

- **Completeness:** keep `usage_snapshot_complete` for reporting, heal progress
  and movement consumers. Its quota-only discard cannot disappear until no
  configured snapshot-protocol quota remains and all quota-setting nodes use
  durable protocols, followed by one release retaining the old admission path.
  Reader changes must first preserve unknown/incomplete reporting explicitly;
  accepting an incomplete report cannot certify a complete snapshot.
- **Floor identity:** retain monotonic epoch/cycle and leadership recovery
  checks. The quota fallback must first stop reading legacy usage, and the
  `backlog-2102` / `backlog-2122` readers stay while rc.1-rc.3 remain supported
  direct-upgrade sources, plus their documented retirement window. A reader
  must first accept and classify the retained legacy/current floor forms
  without resetting cycle identity before a later writer or startup policy
  changes. Removing a quota consumer alone does not satisfy this boundary.
- **Observed routing:** keep both snapshot layouts and their reporting marks.
  Readers for admin, Console and observability must accept both layouts for
  one release before any writer collapses them; snapshot-protocol admission
  must already have migrated. Rollback during that window keeps the dual
  readers and both persisted objects. No object is deleted to force selection.
- **Publication leases:** the ordinary-PUT veto is already retired. Retained
  purpose, movement/epoch fences and write tokens protect publication and
  movement. Future cleanup of the wire shape requires its own reader-first
  compatibility change and cannot follow merely from D3.

## Superseded decision (2026-09-03)

RustFS keeps scanner data usage as authoritative for quota admission.

This selects option A from the backlog decision record: the scanner publication protocol remains necessary while quota admission consumes scanner usage. The cycle epoch, publication CAS, data-movement fence, tier-registry fence, observed snapshot layer, and persisted usage floor are retained and documented as protocol invariants rather than treated as removable compatibility clutter.

### Rationale at the time

Quota is a write-path admission decision, so serving quota from best-effort scanner data would turn temporary scanner lag into under-enforcement. The current design therefore needs an availability story for authoritative usage instead of deleting the proof layers that make it authoritative.

The scanner usage floor provides that availability story. It is a lower bound used when a complete authoritative snapshot is not available, including cold startup, upgrade recovery, and incomplete-cycle repair. Observed snapshots remain useful for admin and observability, but they do not become quota authority.

### Consequences at the time

#2214 is the hard design input for future usage-publication changes. A future proposal may still choose soft quota and MinIO-style best-effort usage, but that would be a product change with its own staged compatibility plan for persisted artifacts.

#2219 may design the scanner storage boundary against the current authoritative protocol. The interface must include the CAS key-value, cycle lock, usage-floor, observed-snapshot, and recovery-marker capabilities needed by [scanner-usage-publication.md](scanner-usage-publication.md); it must not hide those proof obligations behind a generic object-store trait.
