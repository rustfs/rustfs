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
  nor a floor exists.
- [scanner-per-bucket-convergence.md](scanner-per-bucket-convergence.md) no
  longer cites quota authority as a reason to keep uncertified buckets
  fail-closed; an uncertified bucket keeps its last certified value as its
  reported value.
- The scanner storage boundary (#2219) may be designed against the reporting
  contract; it must still expose the CAS key-value, cycle lock, and
  recovery-marker capabilities, but the usage-floor and observed-snapshot
  capabilities become reporting surfaces rather than proof obligations.

## Superseded decision (2026-09-03)

RustFS keeps scanner data usage as authoritative for quota admission.

This selects option A from the backlog decision record: the scanner publication protocol remains necessary while quota admission consumes scanner usage. The cycle epoch, publication CAS, data-movement fence, tier-registry fence, observed snapshot layer, and persisted usage floor are retained and documented as protocol invariants rather than treated as removable compatibility clutter.

### Rationale at the time

Quota is a write-path admission decision, so serving quota from best-effort scanner data would turn temporary scanner lag into under-enforcement. The current design therefore needs an availability story for authoritative usage instead of deleting the proof layers that make it authoritative.

The scanner usage floor provides that availability story. It is a lower bound used when a complete authoritative snapshot is not available, including cold startup, upgrade recovery, and incomplete-cycle repair. Observed snapshots remain useful for admin and observability, but they do not become quota authority.

### Consequences at the time

#2214 is the hard design input for future usage-publication changes. A future proposal may still choose soft quota and MinIO-style best-effort usage, but that would be a product change with its own staged compatibility plan for persisted artifacts.

#2219 may design the scanner storage boundary against the current authoritative protocol. The interface must include the CAS key-value, cycle lock, usage-floor, observed-snapshot, and recovery-marker capabilities needed by [scanner-usage-publication.md](scanner-usage-publication.md); it must not hide those proof obligations behind a generic object-store trait.
