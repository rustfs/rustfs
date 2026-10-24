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

Ordinary PUT rename fanouts track instance-scoped in-flight work
(`NamespaceCommitGuard` in `crates/ecstore/src/runtime/instance.rs`): a quorum
ACK does not release it, the actual disk tasks retain ownership until their
rename work ends. A pending ordinary PUT commit is a write-barrier event for
its bucket, not a usage-publication veto (decision of 2026-10-19). The S3
handler shades the bucket through `record_dirty_usage_object_from_producer`
after the ACK, the finalize certification compares the bucket's generation,
and a peer whose dirty state moved supersedes the cycle until scoped
acknowledgements exist; together they already cover a tail that lands after
the walk. The guard drops before the handler records the shade, so a
pending-commit veto could never close the window ahead of the shade either.
`scanner_data_usage_publication_blocked` therefore blocks usage publication
only for data movement (`scanner_data_usage_publication_snapshot_blocked`),
and the same rule applies on the post-walk probe, the local defer decision,
the post-lease recheck, the usage-store route probe, and the ECStore
publication admission and remote lease paths. Scan admission remains
movement-only so sustained PUTs do not stop namespace walks and
scanner-driven lifecycle discovery. After acquiring remote leases, the
coordinator still rechecks the guardrail fields of the activity snapshot (the
reuse identity and the movement fields, plus the peers' dirty-usage state while
per-bucket certification is local only) before publishing an authoritative
aggregate. Local namespace and dirty-usage generation movement in that window
is not a veto: its buckets stay in the dirty map and are the next cycle's
work, as described in
[scanner-per-bucket-convergence.md](scanner-per-bucket-convergence.md).

This adds no namespace or movement lock. An already-verified older snapshot may
still precede a newly started write; that write's bucket is the next cycle's
work. Intermediate per-set and prefix cache readers retain their existing
approximate-cache semantics. The in-flight tracking does not extend to
multipart or other namespace mutation paths, and it does not prove that a
failed tail replica has healed.

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
| Usage floor identity | scanner publication and the ECStore quota fallback for buckets without a reservation protocol | empty or legacy values becoming a plausible degraded quota floor |

A reader that cannot prove the required fence for its surface must fail closed
or use the documented observed path below. It must not synthesize an empty usage
snapshot for a missing or corrupt authoritative object.

## Cache Execution Identity

The structural scan-plan digest can remain stable across ordinary bucket writes
so a scoped scan can retain unaffected baseline buckets. It is not sufficient
proof for reusing a completed result within the same cycle. Bucket work uses an
execution digest combining the structural plan and the full activity snapshot,
with the bucket's dirty generation included in its cache identity. A bucket
sweep's checkpoint is keyed separately by the coverage identity
(`scanner_activity_coverage_digest`, see
[scanner-per-bucket-convergence.md](scanner-per-bucket-convergence.md)), so
the reuse identity can change without restarting a sweep in progress. Completed
set caches carry the same execution digest separately from their structural
plan; a set cache that stands in for buckets whose walk stopped early binds
the carried bucket names into it (`scanner_carried_execution_digest`), so it
is never adopted in place of a full walk of the same cycle, nor the reverse.
The persisted set-root fast path requires equal execution identities as well as
the existing source, cycle, leader, tier, and cache-structure checks.

Remote workers receive the reuse plan as `scan_plan_digest` and the independent
coverage plan as optional `checkpoint_plan_digest`. The v3 capability proof
authenticates `ns_scanner_checkpoint_coverage` before a coordinator sends the
new field, and the signed request URL binds the entire MessagePack body by its
SHA-256 digest. Older coordinators omit the field and retain their reuse-plan
checkpoint behavior. A peer that lacks the authenticated capability uses the
existing coordinator-side scan path, because its strict request reader rejects
unknown fields. Movement, leader, source, incarnation, and tier checks still
apply to checkpoint admission and persistence.

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
| `.bloomcycle.bin.dual-recovery/<sha(marker)>/` (`cycle.bin`, `<sha(identity)>/usage-N.bin`, `expected-cycle.bin`) | scanner dual-corruption recovery | Written once per recovery attempt with an `If-None-Match` precondition and confirmed by read-back: the original cycle bytes, the original usage-slot bytes, and the exact replacement counter expected against the observed revisions. Immutable. A restart after the counter commit resumes only when `expected-cycle.bin` and every source binding match. Never deleted automatically; one set accumulates per incident. It is diagnostic evidence and never usage or quota authority. |
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

Decision date: 2026-10-19, superseding 2026-09-03; the record is
[scanner-usage-authority-decision.md](scanner-usage-authority-decision.md).

Hard quota admission is owned by the durable reservation ledger. Scanner usage
is best-effort reporting input and the degraded fallback for quotas that
predate the reservation protocol. The publication protocol keeps the fences
that protect consistency across leadership and data movement: leadership
claim, storage publication epoch, CAS revision, per-set freshness, and tier
registry generation. The layers that existed only to make scanner usage a
quota authority are scheduled for staged removal under a compatibility
window:

- the usage floor as hard-quota input (it remains the degraded fallback for
  `reservation_protocol = None` buckets until they are migrated);
- observed-versus-authoritative routing as an admission distinction (it
  remains a completeness mark for readers);
- strict publication-lease rejection of pending ordinary namespace commits.

The availability contract is:

- a bucket admitted by the reservation ledger never fails a write because
  scanner usage is unavailable;
- a bucket without a reservation protocol keeps the degraded floor path and
  fails closed only when neither authoritative usage nor a floor exists;
- scanner publication incompleteness is reported, never converted to zero.

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

Every fail-closed scanner gate has an automatic exit and a bounded dwell time.
None of the exits deletes evidence or relaxes the leadership, CAS, movement, or
usage-floor fences:

- A retryable recovery observation (a transient cycle-state read failure, a
  usage-floor load failure, a pending legacy floor recovery, or a quarantine
  marker that could not be written) never becomes terminal. Once the short
  retry budget is spent the state reads `backoff`, `retryable` stays true, the
  supervisor keeps retrying with an exponential delay capped at ten minutes or
  sooner when recovery is explicitly woken, and `next_attempt_at_unix_secs`
  plus the `reason` name the next attempt. A successful iteration resets the
  count, and an observation older than 24 hours restarts its budget. Followers
  still clear only an unclassified `transient` or `backoff` observation on lock
  contention.
- A `.bloomcycle.bin` that this reader classifies as `corrupt` is quarantined
  by writing the marker, and the next leader iteration rebuilds the counter: it
  starts from an uninitialized cycle raised to the usage-floor cycle and lets
  the ordinary leadership claim CAS-replace the quarantined revision under a
  higher epoch. The marker is retained as evidence. Because its
  `primary_revision` no longer matches the primary, older and newer readers
  start normally, and the supported cycle-state reset remains the way to remove
  it. A `future_schema` counter and a `cleanup-pending` marker stay blocked: the
  first belongs to a newer binary and the second to an operator reset in
  progress. Only a marker that cannot be written or read keeps the node in
  `blocked`, probed every five minutes.
- A pause ledger that cannot be claimed (no replicas, or conflicting or invalid
  replica records) runs the leader in `degraded` mode instead of skipping every
  cycle. Cycles run untracked against an empty in-memory ledger, publication is
  still admitted only through the data-movement fence, `persistence_state`
  reports `degraded` with the claim error, and the claim is retried on every
  scheduler wake and every movement probe, at most once per five minutes. When
  the claim succeeds the durable ledger resumes with its persisted phase and
  catch-up semantics.
- The usage-state full-rebuild reset accepts a cluster whose usage slots are
  all corrupt and whose cycle state is missing. It starts from the empty
  baseline and epoch 1 exactly as a missing floor does, and quota-bearing
  buckets keep failing closed until the rebuilt baseline publishes. A corrupt
  cycle state must still be rebuilt first.
- When both the cycle body and every usage slot are corrupt, the leader
  rebuilds without an operator reset only if the cycle revision still carries a
  recognized fencing header and a matching strict quarantine marker. It first
  retains the original bytes and the expected replacement counter under
  `.bloomcycle.bin.dual-recovery/`, then CAS-rebuilds the counter above the
  surviving epoch and cycle bounds and CAS-publishes the `bootstrap-pending`
  marker, so quota-bearing buckets keep failing closed until a complete
  baseline publishes. Scanner code never deletes or rewrites the evidence
  objects, and operators must not delete them to force startup; they are kept
  for diagnosis. A usable usage
  slot, an unknown structured usage format, a `future_schema` counter, a
  `cleanup-pending` reset, or an unreadable fencing header keeps the existing
  fail-closed paths.
- A cycle-state save that fails without revealing a newer leadership epoch is
  retried under the same epoch: inside the save by reconciling against durable
  state (a committed write is confirmed from the read-back, a failed write is
  retried with the current revision), and after an exhausted save by reloading
  durable state in place. Only a durable state owned by another epoch, or one
  that cannot be read back, ends the leader iteration and triggers a new claim,
  so peers see a changed `(cycle, epoch)` fence only when ownership changed.

## Diagnosing Incomplete Census Progress

Treat bucket coverage, set-worker completion, cycle certification, and usage
publication as separate stages. A successful save can be observational, and a
worker finishing does not certify its namespace coverage.

| Signal | Meaning and next evidence |
| --- | --- |
| `bucket_checkpoint_prepared` | Checkpoint admission identifies the bucket, pool/set, cycle/leader epoch, and previous cycle/epoch. `resume_position` distinguishes retained traversal position from a fresh sweep; `mixed_coverage` reports whether that retained prefix began under another plan. |
| `bucket_sweep_incomplete` | A bucket reached the end of a sweep but needs another verification pass. `unresolved_objects` reports the complete coverage/health gate, including failed child-cache entries that can be absent from the root counter and diagnostic failure map. Inspect it with `coverage_gap`, `root_failed_objects`, `failed_objects`, `size_reconciliation_entries`, and `mixed_coverage`, using bucket, pool/set, and cycle/leader epoch. `resume_position_on_entry` identifies a sweep entered with traversal position; `resume_position` reports the returned cache position after terminal cleanup. Multiple blockers can coexist. |
| `partial_saved_not_published` | The local or remote worker successfully saved a terminal Partial cache. Correlate its bucket, pool/set, cycle/leader epoch and `resume_position` with admission and sweep events. This confirms persistence, not complete coverage or authoritative publication. |
| `checkpoint_identity_unavailable` | A local worker could not read the persisted bucket identity. `stage=admission` preserves the existing checkpoint without preparing a legacy rebuild; `stage=post_scan` rejects the unverified candidate before persistence. Correlate bucket, pool/set, cycle/epoch and the underlying read error. Remote terminal errors distinguish the same two stages. |
| `checkpoint_identity_changed` | A successful post-scan read no longer matches the admitted identity, including a now-absent sidecar. Rejecting the result protects against bucket replacement; this differs from an unavailable read. |
| `remote_scan_partial` | The coordinator received a Partial terminal response for the named bucket and disk. Correlate the worker's bucket-sweep or checkpoint events; Partial is not a complete bucket root, but the set may still cover the bucket with its last certified value (`partial_buckets_carried`). |
| `partial_buckets_carried` | The set published the last certified value for `carried_buckets` buckets whose walk stopped early; `first_carried_bucket` names one. The cycle can complete with them pending and the next cycle walks them. A partial bucket without a certified value on the set is counted by `set_cache_publish_skipped` instead. |
| `bucket_result_rejected` | A bucket result did not match the cycle's frozen tier-registry generation and was not applied. |
| `set_cache_publish_skipped` | `completed_buckets` counts distinct bucket roots successfully applied to this set cache and `carried_buckets` the partial walks covered by a certified value. Use `first_incomplete_bucket` with pool/set/cycle to locate one actual gap in this set. Inspect `unprocessed_buckets` and budget/cancellation state, then correlate that bucket's worker events. |
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

Identity lookup has three outcomes: a verified UUID, a confirmed absent sidecar,
or a read failure. Only confirmed absence may use the existing full-rebuild path
without checkpoint identity. A lock, transport, or corrupt-metadata error must
not become absence and reset a valid frontier. A successful read that proves a
different identity still invalidates the old result.

The `.bucket-incarnation@latest` resource belongs to a read of the persisted
identity sidecar. Scanner identity checks occur before a walk and again before
saving its result; set inventory preflight also reads identities. A single
remote lock-client warning can coexist with a successful quorum, so it does not
prove that checkpoint persistence failed. Use the scanner's identity stage and
the terminal save result to establish that connection.

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
waiting for leadership loss or a successful proof. Observational and
authoritative publication both retain the movement fence, on this node and
through the remote publication leases. Do not remove those fences to clear a
status. A lease-scoped target operation (validation, rename, or delete on the
leasing node) shares the read admission the lease already holds rather than
queueing a new read on the movement gate. The gate is write-preferring, so a
new read would wait behind a movement writer that is itself waiting for the
lease, and neither would progress until the lease expired (rustfs/backlog#2873). Verify
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
