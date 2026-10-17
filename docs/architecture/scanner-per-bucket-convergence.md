# Scanner Per-Bucket Convergence Design

**Use this when:** changing what makes a scanner cycle `Complete`, `Superseded`, or converged; adding or re-classifying a dirty-usage producer; deciding whether a background writeback may invalidate a scanner cycle; changing scanner worker concurrency or throttling; or activating scoped dirty acknowledgements.
**Source of truth:** `classify_nsscanner_cycle`, `scanner_cycle_activity_status`, and `scoped_scan_scope_from_dirty_buckets` in `crates/scanner/src/scanner_io.rs`; `dirty_usage_snapshot_status` and `acknowledge_scoped_dirty_usage` in `crates/scanner/src/scanner_io/dirty_usage.rs`; `scanner_concurrency_limit` in `crates/scanner/src/scanner_io/guards.rs`; the per-disk worker loop in `crates/scanner/src/scanner_io/io_cache.rs`; `DynamicSleeper` in `crates/scanner/src/sleeper.rs`; `ScannerNodeActivity` in `crates/scanner/src/scanner/activity.rs`; `persist_replication_state_if_current` in `crates/ecstore/src/bucket/replication/replication_resyncer.rs`; the publication fences in [scanner-usage-publication.md](scanner-usage-publication.md).

## Decision

Date: 2026-10-13. Status: accepted design; phases ship in the order given
under [Delivery](#delivery), each as its own change.

The scanner is a concurrent collector walking a namespace that mutators keep
changing. It is specified here with the vocabulary of concurrent garbage
collection, because the problems and the known-good answers are the same.

1. **Tri-color buckets with snapshot-at-the-beginning termination.** A cycle
   is `Complete` when every bucket in its start snapshot has been walked and
   the publication is durable. Buckets that mutators shaded grey during the
   walk stay grey for the next cycle and are reported as pending; they do not
   turn the cycle into `Superseded`.
2. **The cluster activity probe stays, as a guardrail only.** It vetoes a
   publication when the heap it describes is not the heap that was walked:
   topology, peer identity, protocol version, data movement, or a pending PUT
   tail changed. Namespace and dirty-usage generation movement alone never
   vetoes; it only shades buckets.
3. **Replication status writebacks are status-class barrier hits.** They
   shade a bucket for a scoped rescan but are not namespace activity. They
   never uncertify a walk and never block cycle completion. The scanner's own
   queued replication heals fall under the same class.
4. **Worker concurrency is paced, not derived from disk count.** The number of
   bucket walkers per erasure set and the per-object throttle are set from a
   pacing target with CPU and I/O ceilings, with disk type as the default
   input. Disk count remains a floor, not a cap.
5. **Guardrail vetoes are bounded too.** A probe that fails for one peer
   localises to the buckets that peer dirtied; it does not defer the whole
   cycle. A veto is always attributed to a named field, retried on a bounded
   backoff, and reported with its reason. The guardrail protects publication
   correctness; it is not allowed to become a third way of never converging.

Every liveness rule above is paired with a boundedness rule in
[Boundedness constraints](#boundedness-constraints). A change that improves
one side must restate its effect on the other.

Backward compatibility is a hard requirement of this design, not a phase.
Every phase must satisfy [Backward compatibility](#backward-compatibility)
on its own: a cluster can be upgraded node by node, and any node can be
rolled back to the previous release, without a scanner reset and without
losing the authoritative usage baseline.

## Two failure modes

Scanner defects fall into two opposite classes and a fix for one can create
the other:

- **Long tail**: part of the namespace is never reached. A cycle keeps being
  invalidated, a budget keeps cutting the same buckets, or a queue keeps
  reordering new names ahead of old ones. The symptom is usage that never
  converges and lifecycle or heal work that never runs for some buckets.
- **Burst amplification**: a sudden wide failure (a disk loss, a mass delete,
  a replication resync) makes the scanner discover a large amount of work at
  once and the scanner, heal, and replication paths saturate CPU, disk, and
  network together.

A third class sits beside these two and is easy to mistake for the first:

- **Guardrail false positives**: the cycle is invalidated not by writes but
  by the proof machinery itself. An in-flight PUT classified as data
  movement, a journal lock timeout reported as an unavailable activity
  baseline, or one unreachable peer in a large cluster defers the whole
  cycle, and the dirty map is never cleared. Per-bucket certification does
  nothing for this class because the walk never reaches certification. The
  guardrail therefore carries its own boundedness rules, attributed per
  field and localised per peer, so a misclassification can be fixed without
  touching the termination rule.

The current cycle classifier is in the first class. The pre- and post-walk
proofs require the global dirty-usage generation and the full activity
snapshot to be identical, so one write to one bucket during a walk makes the
whole cycle `Superseded`. Scoped rescans then need a converged baseline from
`complete_scanner_cache_baseline_plan_digest`, which only a quiet full cycle
can produce, so every retry is a full walk and cycle time never shrinks.
Since replication writebacks started marking buckets dirty, a resync is a
standing mutator and the scanner's own queued heals write back during its
walk, so the scanner invalidates itself. This is a stop-the-world
termination rule applied to a concurrent collector; it fails on liveness by
construction, not under load.

The per-bucket data model that a concurrent collector needs already exists:
per-bucket generations are the card table, `notify_scanner_dirty_usage_mutation`
is the write barrier, `DirtyUsageBucketScope::TopLevelEntries` are finer
cards, and `acknowledge_scoped_dirty_usage` is exact per-card clearing. The
cycle-level decision never consumed it.

## Collector model

### Colors

| Color | Meaning | Where it lives |
|---|---|---|
| white | not walked in this cycle | bucket in the start snapshot without a result |
| grey | walked or not, but a mutator touched it since the cycle's start snapshot | bucket present in the dirty map with a generation newer than its start generation |
| black | walked and certified: its result is current with respect to the start snapshot | bucket acknowledged and cleared from the dirty map |

The write barrier is the existing committed-mutation observer. It shades a
bucket grey by advancing its generation; it never blocks the mutator and it
runs no scanner code inline. Barrier hits carry a class:

| Class | Producers | Effect on usage | Effect on color |
|---|---|---|---|
| `Namespace` | PUT, multipart complete, copy, delete, delete-objects, bucket create, lifecycle expiry, tier expiration cleanup, heal rewrites that change size or version count | size, versions, delete markers, histograms | shades grey; the bucket is uncertified if hit during its walk |
| `Status` | replication status writeback (`persist_replication_state_if_current` in `Update` mode), scanner-queued replication heal, any writeback that only changes replication or restore state on an existing version | only `replication_stats` folded in by `apply_scanner_size_summary` | none: never shades, never uncertifies a walk, never counts as pending; refreshed by the full walk that every cycle with an active replication rule performs |

The class is attached in the mutation observer installed by
`init_background_replication` in `rustfs/src/storage/storage_api.rs`. A
`Namespace` hit advances the bucket's generation; a `Status` hit records only
producer coverage. While the scanner is the replication heal driver,
`ScannerMaintenanceFeatures::needs_regular_cycle` forces a full walk on every
cycle of a deployment with an active replication rule, so a `Status` hit needs
no rescan queue: the walk that would consume it always happens. If a later
phase lifts that forcing, `Status` hits must then join the scoped rescan set,
and that set must travel to the coordinator through the peer dirty-usage
snapshot, because hits land on whichever node commits the writeback.

A replication maintenance walk refreshes compacted prefixes rather than
sampling their old totals. A completed bucket cache with active replication
starts a new coverage sweep even when the cycle is retried; unfinished
checkpoints retain their existing frontier. Set persistence binds the
observed replication statistics to its execution digest, so same-cycle
reuse cannot restore statistics from an earlier walk. None of these cache
requirements changes the namespace generation or publication fences.

### Snapshot at the beginning

A cycle is responsible for the namespace as it was at its start snapshot.
Each bucket walk records its start generation for the local node and for
every peer scoped entry resolved by `resolve_remote_dirty_usage_scope`.
Mutations after that point are the next cycle's work. This is what makes
termination possible under continuous writes: the termination condition is
"no grey bucket in the start snapshot", not "no mutation anywhere".

Usage is a sum, not a reachability fixed point. A certified bucket's value
plus an uncertified bucket's last certified value is a valid snapshot, so the
collector does not need a globally consistent heap to publish. This is why a
`Complete` cycle with pending buckets is sound as an authoritative baseline.

### Certification

After the authoritative publication is durable and the root publication
proof holds, the finalize path acknowledges every walked bucket with the
compare-and-remove semantics of `acknowledge_scoped_dirty_usage`:

- generation unchanged: black, cleared from the dirty map. `Status` hits
  never move it;
- generation moved by a `Namespace` hit: stays grey with the newer
  generation and counts as pending;
- a peer reported the bucket dirty without a scoped entry the coordinator can
  verify: stays grey. This is the mixed-version and partial-peer fallback,
  and it is per bucket, not per cycle.

The whole-cycle generation acknowledgement remains the fallback when no
scoped entries can be built at all. It is never combined with per-bucket
certification in the same cycle.

### Bounded remark

Before finalize, the cycle may re-walk grey buckets at most once, within the
remaining cycle budget and with the same per-bucket limits as the main walk.
A bucket still grey after that is handed to the next cycle. There is no loop
until quiet.

### Cycle classification

`classify_nsscanner_cycle` keeps its incomplete branches: budget,
cancellation, failed walks, and unverifiable dirty or activity state still
return `Incomplete` or `Deferred`. The final branch changes from a global
equality test to:

- guardrail vetoed: `Superseded`;
- otherwise: `Complete`, carrying `pending_dirty_buckets` as the count of
  buckets left grey by `Namespace` hits.

`usage_snapshot_converged` becomes `pending_dirty_buckets == 0`. A `Complete`
cycle with pending buckets publishes an authoritative snapshot: certified
buckets are current, pending buckets carry their last certified values in
`buckets_usage`. `DataUsageInfo` in `crates/data-usage/src/data_usage.rs`
gains an additive, map-encoded list of pending bucket names beside
`usage_snapshot_set_states`, so readers can tell a certified value from a
carried one without a schema break. This replaces the behaviour where a
superseded result is published only as an observed snapshot and never
becomes a baseline.

### Guardrail

The probe compares the `ScannerNodeActivity` snapshot before and after the
walk. Field roles:

| Field | Role |
|---|---|
| `instance_id`, `protocol_version`, `topology_digest` | veto: a peer restarted, changed version, or the topology changed |
| `data_movement_active`, `movement_generation`, `publication_blocked` | veto: rebalance, decommission, or a pending PUT tail can make the walk describe a different generation |
| `maintenance_generation` | veto only when the cycle published maintenance results; otherwise a shade signal |
| `namespace_generation`, `dirty_usage_generation`, `dirty_usage_pending` | shade signal: consumed per bucket; never a veto on their own |

`scanner_activity_structural_digest` already separates the structural part
of the snapshot from the generations. The guardrail is the structural digest
plus the movement fields.

The guardrail is bounded in three ways:

- **Attributed.** A veto names the field that moved. A whole-snapshot
  inequality is never a veto on its own, so a misclassification such as an
  in-flight PUT reported as data movement is fixed in that field's producer
  and nowhere else.
- **Localised.** When the probe cannot be read from one peer, or that peer
  answers with a protocol it cannot be verified under, the coordinator keeps
  every bucket that peer dirtied grey (through the peer set resolved by
  `resolve_remote_dirty_usage_scope`) and publishes the rest. Only a
  movement or topology veto, which describes the whole heap, defers the
  cycle. `ActivityBaselineUnavailable` therefore becomes a per-peer
  condition, not a cycle outcome, unless no peer answered at all.
- **Retried on a bound.** A deferred cycle retries on the existing
  superseded backoff schedule with a cap, and `usage_freshness` reports the
  field and peer that caused the last deferral so the operator can tell a
  rebalance from a broken probe.

### Generations of buckets

Most buckets are cold and a few are hot. The scanner keeps two populations
with separate cadences, in the sense of a generational collector:

- **young**: buckets shaded grey within the last `K` cycles. They are walked
  every cycle, scoped to their dirty cards where a scope exists, under a
  small per-cycle quantum.
- **old**: buckets black for `K` consecutive cycles. They are walked by a
  checkpointed full sweep that is guaranteed to visit every old bucket at
  least once every `M` cycles, independent of young-generation load.

Promotion is `K` consecutive certifications; demotion is any `Namespace`
hit. The split exists so that the long tail has its own cadence instead of
competing with hot buckets for one budget. `K` and `M` are scanner runtime
configuration with defaults; the full sweep keeps today's checkpoint and
resume semantics.

### Baseline bootstrap

`complete_scanner_cache_baseline_plan_digest` accepts a `Complete`
publication as the structural baseline whether or not it is converged.
Bucket-cache reuse is governed by color: a black bucket reuses its cache
under the structural plan digest, a grey bucket is rescanned. This removes
the dependency where scoped rescans need a converged baseline and a
converged baseline needs a quiet cycle. The cache execution identity in
[scanner-usage-publication.md](scanner-usage-publication.md) is unchanged
for within-cycle reuse.

## Pacing and worker concurrency

Before phase 3 the number of bucket walkers per erasure set equalled the
number of disks in the set, capped by `max_concurrent_disk_scans`, and
`scanner_concurrency_limit` took the minimum of the configured value and the
disk count. Buckets queue on one channel per set and each walker takes one
bucket at a time; inside a bucket the folder walk is sequential and
`DynamicSleeper` sleeps after every object for the work time multiplied by
the speed factor. On a single-disk deployment this is one walker, one bucket,
one object at a time, at roughly one third duty cycle under the default
speed. Walker count is therefore a property of topology, not of the work.

A collector's pacer sets collector work from the mutator's rate under a
resource ceiling. The scanner adopts the same shape:

- **Target**: the young generation must be walked within one cycle interval
  and the old generation within `M` intervals. The pacer derives a required
  object rate from the last cycle's object count and wall time.
- **Walkers**: `available` for `scanner_concurrency_limit` becomes the disk
  count multiplied by a per-disk walker factor. The factor defaults from disk
  type: 1 for rotational media, higher for flash, where xl.meta decode is CPU
  bound and there is no seek penalty. Disk count stays the floor so
  distributed deployments keep at least today's parallelism. Shipped as
  `scanner.walkers_per_disk` (`RUSTFS_SCANNER_WALKERS_PER_DISK`, `1` through
  `16`): each local coordinator disk gets that many walkers, which share one
  per-disk scanner admission so a peer still sees one scan session per
  physical disk; remote disks keep one walker. The default is `1` everywhere
  until a local disk can report its media type
  (`rustfs_utils::os::Info::rotational` is never populated today); the flash
  default above `1` remains the intent.
- **Throttle**: the `DynamicSleeper` factor is the pacer's output, not a
  fixed preset. `ScannerSpeed` presets remain as operator overrides and as
  the initial value. Foreground workload backoff in `sleeper.rs` remains the
  hard ceiling: the pacer never reduces the sleep below what foreground
  admission demands.
- **Ceilings**: a CPU share and a per-disk I/O share bound the pacer's
  output. When the target cannot be met inside the ceilings the pacer reports
  it in `usage_freshness` rather than exceeding them.

## Boundedness constraints

Each rule below bounds a liveness rule above. They are part of the design,
not tuning advice.

| Liveness rule | Bound |
|---|---|
| Grey buckets are rescanned next cycle | A scoped cycle walks at most a configured number of buckets and objects. Grey buckets beyond that are ordered by waiting age, oldest first, so every bucket is walked within a bounded number of cycles. New names never reorder ahead of an older waiter. |
| `Status` hits are not namespace activity | A `Status` hit costs one atomic OR on the writeback path and no scanner work. It never triggers a dirty-usage wake. A replication resync therefore adds nothing to scanner load beyond the full walk the cycle already performs. |
| A `Complete` cycle with pending buckets is a baseline | When the pending set is the whole namespace (a mass write, a disk loss), the next cycle is declared a full sweep explicitly and uses the existing superseded retry backoff schedule. A full sweep is never run under the scoped cycle's wake cadence. |
| Bounded remark | At most one remark pass per cycle, inside the remaining budget. |
| Old-generation sweep every `M` cycles | The sweep is checkpointed and resumes; it does not restart on a `Namespace` hit. A hit only demotes the bucket to young for the following cycle. |
| Pacer raises walkers and lowers sleep | CPU and I/O ceilings are absolute. Foreground admission backoff cannot be undercut. Walker count never exceeds the ceiling-derived maximum even when the target is missed. |
| Faster convergence discovers heal and replication work sooner | Heal admission keeps its queue bounds and `queue_full` and `dropped` accounting in `crates/scanner/src/scanner_folder/ledger.rs`; replication MRF keeps its pending cap. Convergence speed does not change retry intervals. Discovered work is swept lazily under those bounds, in the sense of a lazy sweeper: discovery and execution are decoupled. |
| Guardrail vetoes publication | A veto is attributed to a field and localised to a peer where the field allows it; only movement and topology defer the whole cycle. Deferred retries use the capped superseded backoff. A probe failure on one peer never costs more than that peer's dirtied buckets. |
| One bucket walk may be large | Inside a bucket, the walk takes a per-prefix quantum: a prefix that exhausts it checkpoints and yields to the next prefix or bucket, and resumes under the existing cursor and page-index fences. No single prefix may consume the whole cycle budget. The walker's own bookkeeping must stay linear in entries per prefix. `RawEnumerationProgress::record_entry` in `crates/scanner/src/scanner_folder.rs` once re-normalized the accumulated entry list on every entry (rustfs#8113: 8,000 entries cost about 40 s); since #8114 the page writer hashes each entry once when its page commits, and a regression test pins 20,000 entries in one prefix to that cost so pacing cannot be built on a quadratic walker. |

## What stays fail-closed

- Quota authority is unchanged: an uncertified bucket keeps its last certified
  value as the floor, as the usage floor does today. Live writes do not
  advance it.
- The storage publication epoch, leadership claim, CAS revision, publication
  lease, and tier registry fences are untouched. This design narrows what a
  generation movement means; it removes no fence.
- A peer that cannot produce scoped entries keeps every bucket it dirtied
  grey. Rolling upgrades degrade to today's behaviour for those buckets and
  nothing else.
- Scoped acknowledgements keep the receiver bounds: entry count, byte size,
  concurrency, and deadline. A cycle that would exceed them falls back to
  whole-cycle acknowledgement for that cycle.

## Backward compatibility

Compatibility means four things here, and each phase must prove all four:
an older release reads every artifact the new release writes; the new
release reads every artifact the older release left behind; a mixed-version
cluster keeps publishing wherever it publishes today; and every behaviour an
operator relies on today is reachable by configuration. Rollback must never
require a scanner reset.

### Persisted artifacts

| Artifact | Encoding and tolerance today | Rule in this design | Rollback behaviour |
|---|---|---|---|
| `.usage.v2.json` (`DataUsageInfo`) | JSON; every optional field is `serde(default)` and skipped when empty; unknown fields are ignored | The pending-bucket list is an additive optional field. `usage_snapshot_converged` keeps its meaning. No existing field changes type or semantics. | An older reader ignores the pending list and sees a complete snapshot with `usage_snapshot_converged = false`, which it already handles as an authoritative but unconverged publication. |
| `.usage-cache.bin` (`DataUsageCache`) | MessagePack; `DataUsageScanIdentity`, `DataUsageScanProgress`, and `DataUsageRawEnumerationCursor` deny unknown fields; the map-encoded cache metadata tolerates unknown keys | No field is added to a struct that denies unknown fields. Bucket color and start generation are process state in the dirty map, not cache fields. If a per-bucket value must persist, it goes into the map-encoded metadata as `scan_execution_digest` did. The old-generation sweep reuses the existing checkpoint, resume, and cursor fields unchanged. | An older reader ignores any added map key and treats the cache under its existing identity rules. |
| `.bloomcycle.bin` and recovery markers | Versioned by `schema_version`; a mismatch is quarantined | No schema bump. Pending-bucket state is derived from the dirty map and the published snapshot, not stored in cycle state. | Unchanged. |
| `scanner/durable-dirty-producer-replay/<owner>.json` | Strict schema; entries deny unknown fields | The barrier-hit class is not persisted. Replayed entries are treated as `Namespace` class, which can only cause an extra scoped rescan, never a missed one. | Unchanged. |
| `.usage.observed.json` | JSON, same tolerance as `.usage.v2.json` | Still written for guardrail-vetoed cycles. A `Complete` cycle with pending buckets writes the authoritative object, not the observed one. | An older release keeps reading observed objects by its existing rules. |

### Internode protocol

- `SCANNER_ACTIVITY_PROTOCOL_VERSION` is matched exactly by
  `scanner_cycle_activity_status`; a peer on any other version fails the
  probe and the cycle is deferred. Phases 1 to 4 therefore change no
  `ScannerNodeActivity` wire field and do not bump the version. The
  guardrail reinterprets fields that already exist.
- Phase 5 sends scoped acknowledgements over the existing
  `SCOPED_DIRTY_USAGE_PROTOCOL_VERSION` receiver, gated per peer by the
  existing `probe_only` capability request. A peer without the capability
  keeps its dirtied buckets grey and is acknowledged by the whole-cycle path.
  No activity protocol bump is required for scoped acknowledgements.
- If a later change does need an activity protocol bump, it inherits the
  existing fail-closed mixed-version window and must say so in its
  compatibility note; this design does not widen that window.

### Admin API and console

- `ScannerUsageFreshnessSnapshot` and the scanner status contract gain only
  additive fields with `serde(default)`. No existing field is renamed,
  removed, or changes type.
- `last_cycle_result` keeps its existing string values. A `Complete` cycle
  with pending buckets reports `complete`; consumers that do not read
  `pending_dirty_buckets` see a completed cycle, which is the truthful
  reading for them.
- `usage_snapshot_converged` keeps its meaning on every surface that
  exposes it, including account info and cluster usage.
- No contract version carried to the console changes. The console gates on
  contract versions elsewhere; this design must not be the change that
  trips that gate.

### Configuration

- New keys (walker factor, pacer ceilings, `K`, `M`, scoped-cycle quantum)
  live in the `scanner` subsystem beside the existing keys. The server
  config already preserves unknown keys, so an older release leaves them in
  place and ignores them.
- `ScannerSpeed` presets and `max_concurrent_disk_scans` keep their names
  and accepted values. Today's throttle is reachable by setting the preset
  explicitly and the walker factor to 1; the documented operator procedure
  for restoring today's I/O profile is that single pair of settings.
- Every new key has a default that keeps rotational-media behaviour equal to
  today's. On flash the defaults deliberately change behaviour: a walker
  factor above 1 and a pacer-driven throttle are part of the fix, not a side
  effect, so they are on by default and called out in the release note of
  the phase that ships them. Compatibility is a property of artifacts,
  protocols, APIs, and configuration reachability; it is not a promise that
  default scanner throughput stays the same.

### Metrics

Existing metric names, labels, and meanings are unchanged. New values are
new series. The `superseded_cycles` counter and the superseded code of the
last-cycle-result gauge in `crates/obs/src/metrics/collectors/scanner.rs`
will fall because superseded becomes rare; that is a semantic consequence,
not a rename.

### Mixed-version behaviour summary

| Cluster state | Result |
|---|---|
| All nodes on the new release | Full design in effect. |
| Coordinator new, some peers old (same activity protocol) | Per-bucket certification applies to local and capability-bearing peers; buckets dirtied on old peers stay grey and clear through whole-cycle acknowledgement. Publication continues. |
| Coordinator old, some peers new | Old classifier runs; new peers answer the existing activity and acknowledgement protocols unchanged. A `Status` hit on a new peer no longer moves that peer's dirty generation, so it no longer supersedes the old coordinator's cycle; everything else equals today's. |
| Node rolled back after the new release published | The rolled-back node reads the new `.usage.v2.json` as an unconverged authoritative snapshot and continues with today's rules; no reset. |

No temporary compatibility path is introduced by this design, so no
`RUSTFS_COMPAT_TODO` entry is required. If an implementation phase finds it
needs one, it registers it in
[compat-cleanup-register.md](compat-cleanup-register.md) with a removal
condition before merging.

## Surfaces

- `usage_freshness` in the admin scanner status reports
  `pending_dirty_buckets` (`Namespace` class only), plus the pacer target,
  achieved rate, and whether a ceiling is binding. The console marks usage
  stale only for that count or for a guardrail veto.
- `last_cycle_result` reports `complete` for a cycle with pending buckets;
  `superseded` is reserved for guardrail vetoes and should be rare.
- The superseded retry schedule applies only to guardrail vetoes and to
  explicit full sweeps. A `Complete` cycle with pending buckets uses the
  ordinary dirty-usage wake and the next cycle is scoped to the pending set.
- `rustfs_cluster_usage_objects_snapshot_converged` keeps its meaning.

## Delivery

Phases are independent and ship in order; each has an oracle on both failure
modes.

1. **Status-class barrier hits.** Route replication writebacks, and with
   them the scanner-queued heals, off the namespace path so they never
   advance the dirty-usage generation, wake the scanner or reach the journal.
   Oracle: a single-node site-replication resync reaches a `Complete` cycle
   while the resync is running, and `replication_stats` for the bucket
   refresh on the following cycle.
2. **Tri-color certification, bounded remark, baseline bootstrap, aging
   order, per-prefix quantum, bounded guardrail.** Record start generations,
   certify at finalize, publish `Complete` with pending buckets, accept a
   non-converged `Complete` publication as the structural baseline, order
   grey buckets by waiting age under a per-cycle quantum, give each prefix
   inside a bucket its own quantum, and attribute and localise guardrail
   vetoes. Oracle: a single-node deployment with one bucket written every
   few seconds reaches `Complete` on the first full walk, reports exactly
   that bucket pending, and the next cycle walks only that bucket; a
   deployment with more buckets than the quantum walks every bucket within
   the bounded number of cycles; a bucket holding one flat prefix of roughly
   a hundred thousand entries no longer starves the buckets after it; a
   multi-node deployment with one peer's probe failing publishes every bucket
   that peer did not dirty.
3. **Pacer and walker concurrency.** Pin the entry bookkeeping in
   `RawEnumerationProgress::record_entry` to linear cost first (the
   clone-per-entry itself was removed by #8114; the regression test and the
   in-place page writer are this phase's groundwork), then decouple walker
   count from disk count (`walkers_per_disk`, shipped) and drive the throttle
   from the pacing target under CPU and I/O ceilings (open). Oracle: on a single flash disk the same namespace
   converges in a fraction of today's cycle time with scanner CPU under the
   ceiling; on rotational media the default factor keeps today's I/O
   profile; a foreground write load at the admission threshold still backs
   the scanner off, and latency-sensitive foreground reads on a single NVMe
   disk stay within their pre-scanner envelope while a cycle runs.
4. **Generations and the old sweep.** Promote and demote buckets, run the
   checkpointed old sweep on its own cadence. Oracle: a cold bucket is walked
   at least once every `M` cycles while a hot bucket is written continuously.
5. **Scoped acknowledgement sender.** Activate production of scoped entries
   for peers behind the existing receiver so distributed deployments certify
   per bucket. Oracle: a multi-node deployment with writes on one node
   certifies all buckets written elsewhere and leaves only the hot bucket
   grey; a peer on the previous protocol version leaves its dirtied buckets
   grey and nothing else.

Phase 1 lets a replicated single-writer deployment converge; phase 2 makes
continuous business writes converge; phase 3 shortens the cycle itself;
phase 4 protects the long tail; phase 5 extends all of it to distributed
clusters.

### Validation topologies

Each phase is validated against the deployment shapes that reported the
defect, not only against synthetic fixtures. The shapes are listed by their
characteristics; the originating reports are the issues referenced from the
tracking discussion.

| Shape | Exercises |
|---|---|
| single node, single disk, site replication resync in progress | phase 1: `Status`-class hits, self-invalidation by queued heals |
| single node, single disk, parity 0, on the order of 1,500 buckets and 1.4 million objects, continuous writes, no lifecycle or replication | phases 2 and 3: termination rule, baseline bootstrap, aging order, walker count |
| single node, one bucket whose metadata prefix holds on the order of 100,000 keys with noncurrent versions and delete markers under Object Lock | phase 2 per-prefix quantum, phase 3 bookkeeping prerequisite |
| 40 nodes, one pool, 400 drives, continuous log-chunk writes | phase 2 bounded guardrail, phase 5 scoped acknowledgements, journal and lease behaviour under load |
| 4 nodes, 48 drives, on the order of 600 million objects, continuous file-system-backed writes | phase 5 at scale, guardrail attribution under a permanent PUT tail |
| single node, NVMe, latency-sensitive small-object foreground reads | phase 3 ceilings and foreground backoff |

A phase is not complete until its oracle holds on every shape it names. A
pass on synthetic fixtures alone is evidence for a pull request, not for
release.

## Non-goals

- Soft quota or best-effort usage. The authority decision in
  [scanner-usage-authority-decision.md](scanner-usage-authority-decision.md)
  stands.
- Removing or merging any publication fence.
- Making replication statistics authoritative. They remain observational and
  may lag by one cycle after a `Status` hit.
- Parallelising the folder walk inside one bucket. Walker count is raised at
  bucket granularity and the walk inside a bucket is bounded by the
  per-prefix quantum; intra-bucket parallelism is a separate change with its
  own ordering and checkpoint implications.
