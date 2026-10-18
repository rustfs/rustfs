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
invalidate cached plans across the fanout; after acquiring remote leases, the
coordinator rechecks the guardrail fields of the activity snapshot (the
structural digest and the movement fields, plus the peers' dirty-usage state
while per-bucket certification is local only) before publishing an
authoritative aggregate. This catches a tail that finishes between the scan's
last probe and lease acquisition. Local namespace and dirty-usage generation
movement in that window is not a veto: its buckets stay in the dirty map and
are the next cycle's work, as described in
[scanner-per-bucket-convergence.md](scanner-per-bucket-convergence.md).

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

## Dirty producer replay ownership

The dirty producer journal is partitioned by the configured node RPC authority.
A domain-separated SHA-256 owner identifier selects
`scanner/durable-dirty-producer-replay/<owner>.json`; the record also binds that
owner, which must match before replay. The same endpoint reuses its record after
a process restart. Changing the endpoint selects a new record and retains the
existing full-scan and peer-instance fences.

The legacy shared `scanner/durable-dirty-producer-replay.json` is retained without
being imported or deleted: it cannot prove which node owns all pending entries.
Mixed-version nodes therefore cannot overwrite or clear upgraded nodes' records.
Rollback leaves the owned records available for a subsequent upgrade.

Committed mutation callbacks coalesce into bounded node-local state. A single
worker snapshots that state at most once per second and retries failed storage
operations after five seconds. It holds no state lock during storage I/O and
confirms only the snapshot revision it saved. Dropping the last journal handle
releases its worker. This asynchronous hint is not a per-S3-commit durability
receipt; initial full scans and peer-instance changes still fence restart gaps.

The complete owned JSON record must fit the replay reader's 64 KiB limit.
Oversized prefix scopes become whole-bucket hints; if the resulting complete
record still cannot fit, an explicit unverified marker prevents truncated
coverage from becoming producer authority. Invalid or unknown mutations remain
unverified until a verified dirty acknowledgement clears all pending work.

Publication logs preserve the existing retry and ACK semantics while exposing
`stage`, `blocker`, `partial_cause`, and `snapshot_kind`. A successful `observed`
save does not imply authoritative publication or dirty acknowledgement. Cold
bucket traversal still requires complete distributed invalidation evidence;
node-local journal ownership does not relax the cluster activity proof.

## Fences

The protocol uses separate fences because they exclude different stale inputs.
They must not be collapsed unless the replacement proves the same exclusions.

| Fence | Owner | Excludes |
|---|---|---|
| Scanner leadership claim | scanner | competing scanner leaders and stale cycle writers |
| Storage publication epoch | ECStore | usage computed across rebalance, decommission, or other data-movement generations |
| Publication lease | scanner peers through ECStore-facing activity probes | remote dirty-usage or maintenance state that has not acknowledged the candidate |
| CAS revision | backing config object store | lost updates to usage snapshots, scanner caches, or cycle-state objects |
| Per-set freshness | scanner aggregation | a merged usage snapshot that combines stale and current set results |
| Tier registry generation | scanner tier accounting | bytes classified against a different warm-tier registry |
| Usage floor identity | scanner publication and ECStore quota fallback | empty or legacy values becoming plausible authoritative quota input |

A reader that cannot prove the required fence for its surface must fail closed
or use the documented observed path below. It must not synthesize an empty usage
snapshot for a missing or corrupt authoritative object.

## Cache Execution Identity

The structural scan-plan digest can remain stable across ordinary bucket writes
so a scoped scan can retain unaffected baseline buckets. It is not sufficient
proof for reusing a completed result within the same cycle. Bucket work uses an
execution digest combining the structural plan and the full activity snapshot,
with the bucket's dirty generation included in its cache identity. Completed set
caches carry the same execution digest separately from their structural plan.
The persisted set-root fast path requires equal execution identities as well as
the existing source, cycle, leader, tier, and cache-structure checks.

A set scan also captures its starting cache revisions. When the persisted
execution differs, replacement requires those revisions to remain unchanged;
otherwise a slow scan could overwrite a newer completed result. The existing
cache lock, conditional save, and movement admission still fence the commit.

The optional `scan_execution_digest` field is appended to the map-encoded cache
metadata. Legacy caches remain readable but cannot satisfy same-cycle set-root
reuse without this identity. Older readers can ignore the added map key, but
older writers do not enforce its fence; readability is not a mixed-version
publication-safety guarantee.

Bucket checkpoints distinguish completed traversal from object health. A failed
object does not stop later completed subtrees from advancing the traversal
frontier. On resume, a sorted index of cached entries with `failed_objects > 0`
excludes their containing subtrees from reuse; healthy covered siblings keep
their exact cached accounting. Failure counters survive retry-cache TTL and
capacity eviction, and any remaining entry failure keeps the bucket partial.
Directory traversal errors and unresolved size reconciliation still stop the
frontier because they do not have the same per-entry coverage proof.

Coverage follows path-component traversal order, including punctuation in sibling
names. Coverage receipts use digest domain 3 and bind every entry in each covered
subtree using the same predicate as resume, while excluding the unvisited suffix.
No unsigned nested failure can authorize subtree reuse. Current
readers accept and migrate domain 2 and legacy epoch-bound receipts. Older
binaries cannot validate a domain 3 receipt and safely reread the bucket rather
than applying their unconditional covered-subtree skip. The serialized receipt
shape, cache CAS, leader epoch and publication fences are unchanged.

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
decisions from it. The scanner routes a snapshot to the observed object by the
explicit `usage_snapshot_observational` mark that a vetoed or unverifiable cycle
sets, not by `usage_snapshot_converged`: a complete cycle that certified every
bucket but the ones written during its walk publishes an unconverged
authoritative snapshot that names those buckets as pending. Admin usage views may expose this state with completeness
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

## Diagnosing Incomplete Census Progress

Treat bucket coverage, set-worker completion, cycle certification, and usage
publication as separate stages. A successful save can be observational, and a
worker finishing does not certify its namespace coverage.

| Signal | Meaning and next evidence |
| --- | --- |
| `bucket_checkpoint_prepared` | Checkpoint admission identifies the bucket, pool/set, cycle/leader epoch, and previous cycle/epoch. `resume_position` distinguishes retained traversal position from a fresh sweep; `mixed_coverage` reports whether that retained prefix began under another plan. |
| `bucket_sweep_incomplete` | A bucket reached the end of a sweep but needs another verification pass. `unresolved_objects` reports the complete coverage/health gate, including failed child-cache entries that can be absent from the root counter and diagnostic failure map. Inspect it with `coverage_gap`, `root_failed_objects`, `failed_objects`, `size_reconciliation_entries`, and `mixed_coverage`, using bucket, pool/set, and cycle/leader epoch. `resume_position_on_entry` identifies a sweep entered with traversal position; `resume_position` reports the returned cache position after terminal cleanup. Multiple blockers can coexist. |
| `partial_saved_not_published` | The local or remote worker successfully saved a terminal Partial cache. Correlate its bucket, pool/set, cycle/leader epoch and `resume_position` with admission and sweep events. This confirms persistence, not complete coverage or authoritative publication. |
| `remote_scan_partial` | The coordinator received a Partial terminal response for the named bucket and disk. Correlate the worker's bucket-sweep or checkpoint events; Partial is not a complete bucket root. |
| `bucket_result_rejected` | A bucket result did not match the cycle's frozen tier-registry generation and was not applied. |
| `set_cache_publish_skipped` | `completed_buckets` counts distinct bucket roots successfully applied to this set cache. Use `first_incomplete_bucket` with pool/set/cycle to locate one actual gap in this set. Inspect `unprocessed_buckets` and budget/cancellation state, then correlate that bucket's worker events. |
| `set_scan_completed` | Set workers have finished. `coverage_complete` reports the structural coverage gate, not durable publication. This event alone does not prove a complete set snapshot. |
| `incomplete_coverage` | The cycle did not obtain complete structural coverage. This does not identify a hot bucket or prove that ordinary writes caused the gap. |
| `publication_proof_pending` | Publication is waiting for a retryable proof. Use cycle/stage, `pending_duration`, the preceding `publication_proof_started` kind, and the peer/purpose in the error. This event does not assert that the walk was Complete. |

For a report such as 52 completed buckets out of 53, first identify whether
the missing bucket was Partial, failed, never admitted, or rejected at merge.
Only then inspect its worker evidence. Mixed coverage is a resumable sweep
under a newer plan; failed objects and size reconciliation require different
follow-up. A global dirty-generation change alone does not establish which
bucket failed to produce a complete root.

A mixed tail clears traversal positions before returning Partial. If that
terminal cache is saved, the next fresh sweep binds both coverage plans to its
new request, even if ordinary writes continue between cycles. A periodic
checkpoint saved before terminal cleanup still contains traversal position;
resuming it under a changed plan can legitimately be mixed again. Correlate
admission, sweep completion, and terminal persistence for the same bucket and
pool/set across consecutive cycles. Samples from different sets cannot establish
a repeated mixed loop. Check save failures, remote interruption, and leadership
changes if only periodic progress survives.

An unknown bucket cannot be quarantined as zero in an authoritative total.
Existing pending-bucket semantics still require complete structural coverage.
A separately labelled subtotal would need proof for each included bucket across
every source, with missing buckets explicitly unavailable or stale; it must not
advance full-census authority or acknowledge their dirty work.

Use a bounded DEBUG window on the capacity-scanner leader and the workers
serving the affected bucket:

```text
RUST_LOG=warn,rustfs::scanner=debug,rustfs::scanner::io=debug,rustfs::scanner::folder=debug
```

Capture one representative cycle with its effective budget configuration,
source commit on every node, per-set completion summaries, partial-bucket
events, remote errors, publication kind and result, and cycle-state persistence.
The summaries report counts and bounded bucket identifiers, not object-path
inventories. Keep logs access-controlled and redact deployment identifiers
before sharing them.

An unlimited walk budget can also leave a retryable authoritative publication
waiting for leadership loss or a successful proof. Observational publication
still retains the movement fence, while authoritative publication requires the
strict namespace proof. Do not remove those fences to clear a status. Verify
that a deployment contains the relevant fixes by commit ancestry; a newer
release name alone is not evidence that it includes current main.

Keep usage objects, checkpoint data, and recovery material intact during
diagnosis. Acceptance requires usable census values, appropriately advancing
dirty acknowledgements after publication, and progress across later cycles or
leadership changes. Worker completion timestamps alone are insufficient.

## Existing Fixes As Invariants

Several prior scanner fixes are consequences of this contract rather than
standalone patches:

- incomplete scanner usage must not become a complete admin or quota baseline,
  because completeness and floor identity are part of publication ownership;
- dirty usage and maintenance acknowledgements must fence publication, because
  a remote node with unacknowledged work can invalidate the candidate;
- a legacy or backup usage object may help recover availability only when it
  carries a valid baseline identity and does not cross the primary epoch fence.
