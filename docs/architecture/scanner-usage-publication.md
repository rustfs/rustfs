# Scanner Usage Publication Contract

**Use this when:** changing scanner data-usage persistence, quota-visible usage snapshots, scanner cycle state, dirty-usage catch-up, or the conditions under which an observed scanner snapshot may be served.
**Source of truth:** `crates/scanner/src/scanner/usage_store.rs`, `crates/scanner/src/scanner/cycle_state.rs`, `crates/scanner/src/scanner/backlog.rs`, `crates/scanner/src/scanner/leadership.rs`, `crates/scanner/src/data_usage_define.rs`, and quota fallback behavior in `crates/ecstore/src/bucket/quota/checker.rs`.

## Ownership Model

One scanner cycle owns an authoritative publication only after it holds the
cluster scanner leadership claim and proves that the storage publication epoch
has not moved. The leadership claim is persisted in the scanner cycle state,
while data-usage publication admission is owned by ECStore because it knows
whether rebalance, decommission, or another data-movement operation has changed
the generation that scanner results are allowed to describe.

The scanner may compute usage without publication ownership, but it must not
turn that result into authoritative quota-visible state. A complete publication
therefore has three identities:

- the scanner leader epoch that owns the cycle;
- the storage publication epoch that fences data movement;
- the per-object CAS revision on the usage object being replaced.

If any identity changes before commit, the result is a candidate for retry or
observation, not an authoritative baseline.

Ordinary PUT rename fanouts also track instance-scoped in-flight work. A quorum
ACK does not release it: the actual disk tasks retain ownership until their
rename work ends, including when the request caller is cancelled. Scan admission
remains movement-only so sustained PUTs do not stop namespace walks and
scanner-driven lifecycle discovery. The post-walk local publication check and
remote publication leases reject pending fanouts. Begin/end namespace generations
invalidate scans and cached plans across the fanout; after acquiring remote
leases, the coordinator rechecks the full activity digest before publishing an
authoritative aggregate. This catches a tail that finishes between the scan's
last probe and lease acquisition.

This adds no namespace or movement lock. An already-verified older snapshot may
still precede a newly started write. Sustained or stalled PUT tails can delay
authoritative usage publication, which resumes through the existing retry
schedule rather than a new immediate-wakeup protocol. Intermediate per-set and
prefix cache readers retain their existing approximate-cache semantics. A
prolonged pending tail with no generation changes can also delay cycle advancement
and fresh rescans of already-current caches; this is not a guarantee of lifecycle
progress under indefinitely stalled storage I/O.

This PUT-tail protection requires every writer node to be upgraded. It does not
prove that a failed tail replica has healed, and it does not extend the same
in-flight tracking to multipart or other namespace mutation paths.

## Fences

The protocol uses separate fences because they exclude different stale inputs.
They must not be collapsed unless the replacement proves the same exclusions.

| Fence | Owner | Excludes |
|---|---|---|
| Scanner leadership claim | scanner | competing scanner leaders and stale cycle writers |
| Storage publication epoch | ECStore | usage computed across rebalance, decommission, or other data-movement generations |
| Publication lease | scanner peers through ECStore-facing activity probes | remote dirty-usage or maintenance state that has not acknowledged the candidate |
| CAS revision | backing config object store | lost updates to `.usage.v2.json`, `.usage.json`, or cycle-state objects |
| Per-set freshness | scanner aggregation | a merged usage snapshot that combines stale and current set results |
| Tier registry generation | scanner tier accounting | bytes classified against a different warm-tier registry |
| Usage floor identity | scanner publication and ECStore quota fallback | empty or legacy values becoming plausible authoritative quota input |

A reader that cannot prove the required fence for its surface must fail closed
or use the documented observed path below. It must not synthesize an empty usage
snapshot for a missing or corrupt authoritative object.

## Persisted Objects

The persisted objects are part of the compatibility contract. Removing one
requires a compatibility window and a dedicated cleanup entry.

| Object | Owner | Lifecycle |
|---|---|---|
| `.usage-cache.bin` under each bucket and set | scanner disk walk | Rebuilt by scanner from object metadata. Missing data causes a rescan for that bucket/set; corrupt data is not a complete baseline. |
| `.bloomcycle.bin` | scanner cycle state | CAS-updated by the leader. Missing state starts from an uninitialized cycle; corrupt or future state is quarantined before automatic retry. |
| `.usage.v2.json` and `.usage.json` | scanner authoritative publication | `.usage.v2.json` is the primary complete usage snapshot. `.usage.json` is read only as a legacy or companion baseline when it carries a valid persisted identity. Neither bypasses the v2 epoch fence, and readers may treat a snapshot as authoritative only when its baseline identity and completion fields validate. |
| `.usage.observed.json` | scanner observation path | Written when an authoritative publication cannot be proven but a diagnostic snapshot is still useful. It is never a hard-quota authority. |
| `bucket-metadata/.usage.json` | scanner usage floor | Carries the persisted per-bucket floor used by quota during a degraded authoritative-usage window. It is static until the next complete scanner publication. |
| `.bloomcycle.bin.recovery-required.json` | scanner cycle recovery | Quarantines invalid cycle state with retry evidence. Only scanner recovery code updates or clears it. |
| `.scanner-cycle.lock` | scanner runtime lock | Serializes cycle-level work. A missing lock object is not itself usage evidence. |
| `.scanner-pause-backlog.json` | scanner pause and catch-up ledger | Tracks dirty usage, discovered lifecycle work, and full-scan catch-up while authoritative publication is fenced by data movement. It never grants publication admission. |

## Observed Snapshots

Observed snapshots are a diagnostic and availability layer. They may be served
only when the snapshot explicitly reports that it is partial or observational,
and only to consumers that do not make hard quota, durability, or deletion
decisions from it. Admin usage views may expose this state with completeness
flags so operators can see progress while the authoritative publication is
blocked. Quota enforcement must not use an observed snapshot as the current
usage authority.

When an authoritative snapshot is unavailable, quota admission may use the
persisted usage floor. That is an availability fallback, not a fresh count: live
writes do not advance the floor, and overrun is bounded only by writes accepted
before the next complete scanner publication. If no valid persisted floor is
available, quota remains unavailable and fails closed.

## Availability Decision

Decision date: 2026-09-03.

RustFS keeps scanner usage as the authority for hard quota admission. The
publication protocol therefore remains necessary: leadership, storage epoch,
lease, CAS, observed snapshot, and usage-floor layers are the proof machinery
that lets a distributed background scan feed a quota decision without accepting
stale or cross-generation usage as current truth.

The availability contract is:

- the authoritative fast path reads complete in-memory or persisted scanner
  usage;
- during upgrade or publication outage, quota may admit against the persisted
  per-bucket usage floor;
- the floor is advisory for the outage window and must converge back to a
  complete scanner publication;
- a bucket with neither authoritative usage nor a valid floor fails closed.

Changing this decision to a soft-quota model would be a product change, not a
scanner refactor. It would need a staged removal of the authority-specific
layers and compatibility handling for the persisted objects above.

## Deletion And Recovery Rules

Only the owner of an object may delete or quarantine it:

- scanner may rebuild per-set `.usage-cache.bin` after a scan proves the
  replacement contents;
- scanner cycle recovery may quarantine invalid `.bloomcycle.bin` and clear the
  marker only after a valid cycle state is persisted;
- scanner publication may replace `.usage.v2.json` or legacy companions only
  through the publication fences above;
- quota consumers may read the usage floor but must not delete or repair it;
- operators may reset scanner usage state only through the supported scanner
  reset surface, which records the reset paths and forces a full rebuild.

Missing, undecodable, or identity-less data is not converted to zero. It is
reported as uninitialized, recovery-required, observed-only, or unavailable
according to the reader's surface.

## Existing Fixes As Invariants

Several prior scanner fixes are consequences of this contract rather than
standalone patches:

- incomplete scanner usage must not become a complete admin or quota baseline,
  because completeness and floor identity are part of publication ownership;
- dirty usage and maintenance acknowledgements must fence publication, because
  a remote node with unacknowledged work can invalidate the candidate;
- a legacy or backup usage object may help recover availability only when it
  carries a valid baseline identity and does not cross the primary epoch fence.
