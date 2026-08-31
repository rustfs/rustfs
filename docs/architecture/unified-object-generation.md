# Unified Per-Object Generation Authority

Establishes a **single per-object generation authority** that spans object
commit, GET snapshots, garbage collection, and quota accounting, and pins the
transport, encoding, proto-evolution, and mixed-version contracts that every
consumer must obey.

This is a **design and contract document**. It changes no storage code. It is
the shared prerequisite for five implementation sub-issues under the
[#1307](https://github.com/rustfs/backlog/issues/1307) adversarial-review
program:
[#1312](https://github.com/rustfs/backlog/issues/1312) (commit fencing),
[#1313](https://github.com/rustfs/backlog/issues/1313) (read lease),
[#1314](https://github.com/rustfs/backlog/issues/1314) (prepared pool read),
[#1318](https://github.com/rustfs/backlog/issues/1318) (quota reservation), and
[#1323](https://github.com/rustfs/backlog/issues/1323) (old-dir GC).

Tracks [rustfs/backlog#1326](https://github.com/rustfs/backlog/issues/1326).

## Why one authority

The #1307 adversarial-review verdict (issuecomment-4992565957) found that the
five sub-issues each reach for their own generation / fencing / lease token to
solve the same underlying problem — **commit mutual-exclusion plus snapshot
lifetime**. Left independent, they diverge and punch through one another:

- #1323 old-dir GC can reclaim a directory still referenced by a #1313 lease if
  the two disagree on what "current generation" means.
- #1312 fence epoch and #1318 quota reservation token, if derived from two
  different monotonic sources, cannot be compared — a late commit fenced on one
  plane can still settle quota on the other.

The fix is a single authority with one selected comparison rule, one persistence
semantics, and one transport binding, that every consumer references rather than
re-derives.

## Target authority and the current bounded token

The target contract still requires **one per-object commit identity** consumed
by commit fencing, read leases, cleanup, prepared reads, and quota settlement.
No consumer may mint a second value and call it the same generation.

The concrete ordering semantics are not settled, however. The original #1326
proposal requires a total-ordered, monotonic lock-grant epoch. Current main does
not implement that proposal. PR #6077 instead implements an opaque transaction
identity:

- `assign_object_transaction_epoch` mints a random non-nil UUID for PUT and
  CompleteMultipartUpload when the object-transaction gate is active.
- The UUID is written through `FileInfo::set_object_transaction_epoch` into the
  dual internal metadata map.
- The coordinator reads the current UUID (or `Absent`) and revalidates exact
  equality immediately before `rename_data`.
- Old-data cleanup receipts carry the committed UUID and reconciliation deletes
  only when the receipt UUID still equals the current object UUID.

This is a useful **equality-CAS fence and cleanup identity**. It is not a
monotonic epoch, is not minted by the distributed lock grant, and is not
compared atomically at each disk's `xl.meta` commit point. Until the decision
below is made, documents and issue checklists must call it the *object
transaction UUID* rather than use it as proof that the target generation
authority exists.

### Ordering decision required

Before #1313, #1314, or a unified quota binding can consume the authority, one
of these contracts must be selected and tested:

1. **Total-ordered fencing epoch.** A lock grant returns a durable per-object
   `(term, counter)` (or another specified total-order type). Every disk rejects
   a lower epoch at the atomic metadata commit point. The value never regresses
   across lock-plane restart, failover, or minority recovery.
2. **Opaque commit-generation identity.** Consumers compare only exact identity;
   no `<` / `>` semantics are permitted. The authoritative commit must perform
   an atomic expected-generation CAS, and all lease, cleanup, prepared-read, and
   quota contracts must be rewritten in terms of “references this exact
   generation,” not “lower/newer generation.”

The current UUID implementation proves neither a durable total order nor a
per-disk atomic expected-generation CAS, so it does not by itself decide between
these options.

### Persistence semantics if total order is selected

A total-ordered epoch must be **monotonic across lock-plane restart and
failover**. The distributed lock entry remains in-memory; deriving a counter
from that entry alone would reset it after restart. The chosen source therefore
must be either quorum-persisted before grant or derived from a durable term whose
full `(term, counter)` comparison cannot regress. This requirement does not
apply to an opaque UUID as an ordering rule; the opaque alternative instead
requires atomic expected-identity comparison and durable crash recovery.

## Consumer binding contracts

### Current implementation snapshot (2026-08-31, main@9ee7b1221)

This table separates code that exists on current main from the target contract.
Closing an implementation issue does not imply that its token is already the
unified authority.

| Surface | Current main | Gap against this contract |
|---|---|---|
| PUT / CompleteMultipartUpload (#1312, PR #6077) | Owned commit tasks retain the relevant guards; an opt-in gate persists a random object transaction UUID and performs a quorum metadata equality recheck before rename | no lock-grant monotonic source; no per-disk atomic epoch/CAS comparison; the live proof is the reused remote-version-state fleet proof, not a dedicated generation capability |
| Old-data cleanup (#1323, PR #6077) | JSON receipt carries transaction UUID, old dir, and committed dir; reconciliation is gated and requires UUID equality | no generation-bound read lease is consulted, so this is crash cleanup fencing rather than the full #1313/#1323 lease lifetime contract |
| Read lease (#1313) | short-term streaming/multipart path holds the namespace read lock through EOF/drop; deterministic part-boundary coverage is tracked by PR #6887 | no cross-node generation-bound lease registry, TTL reclamation, or crash recovery |
| Prepared pool read (#1314) | PR #6889 tracks a pool-local prepared identity and fails closed/refetches when pool state changes | not merged on this snapshot; pool-local identity is not a cross-pool generation authority; black-box mixed-version/rebalance coverage remains open |
| Quota reservation (#1318) | durable per-bucket ledger plus independent snapshot-lease mutation-fence tokens; issue closed after PR #6058 | reservation and settle are not bound to the object transaction UUID; the independent fence must be reconciled with the selected authority or explicitly proven to be a separate, non-generation arbitration domain |
| Internode integrity (#1327, #1541, #1542) | v2/v3 HMAC binds audience, exact method, timestamp, nonce, canonical body digest, and receiver boot epoch; body-bound RPC policy has exact-set coverage | signature/body/replay strict switches remain default-off rollout gates; generation enforcement cannot treat an unrelated fleet-version proof as proof that these strict contracts converged |

| Consumer | How it binds generation | Key invariant |
|---|---|---|
| #1312 commit fence | selected generation is checked at `rename`, rollback restore/delete, and cleanup mutation points using the chosen ordered or exact-CAS rule | a stale writer is rejected on **all** disks; an already-ACK'd write is never rolled back |
| #1313 read lease | lease binds the exact generation observed at read time; GC runs only after every lease referencing that generation is released | lease is visible across nodes; a crashed reader's lease is reclaimed by TTL |
| #1323 old-dir GC | cleanup job carries the committed generation; before deleting `old_dir` it confirms that no lease for the generation owning that directory remains | `old_dir != committed_dir`; a still-referenced directory is never deleted |
| #1314 prepared pool read | the `PreparedPoolRead` bundle carries the generation resolved during pool lookup; the chosen pool's reader setup reuses it only after a match | generation mismatch forces a fallback to full metadata fanout |
| #1318 quota reservation | reservation / settle record binds the exact object generation (and an ordered epoch too, if that option is selected) | a late commit cannot settle quota for a different committed generation |

### Fence coverage is three disk-write points, not one (#1312 B2)

Checking the generation only before the `rename` fanout is insufficient. The
authoritative commit sequence is `tmp sync → data-dir rename → xl.meta commit →
directory sync` in `crates/ecstore/src/disk/local.rs`, and there are two further
detachable disk-write points in
`crates/ecstore/src/set_disk/core/io_primitives.rs`:

- **Rollback restore/delete** — on quorum failure each disk can restore backup
  metadata or delete the failed version. A stale writer's rollback must compare
  the expected generation, otherwise it can overwrite or delete the winner's
  already-committed metadata.
- **`commit_rename_data_dir`** — a cancel-then-detach disk-write point; the
  coordinator's "reap all child tasks" must explicitly include it so a cancelled
  writer cannot bypass fence/lease and keep deleting directories.

If generation is validated only after data-dir rename, a fenced writer
may already have renamed its data-dir into the object path, leaving a staged
orphan. Either move the fence ahead of the data-dir rename, or declare that
orphan an acceptable residue accounted for by GC metrics — the white-box
acceptance "no background disk write after release" must be rewritten
accordingly.

Current PR #6077 performs a quorum metadata equality recheck before rename and
reaps owned commit work. That closes important cancellation windows, but it is
not evidence that every disk mutation above performs the selected generation
comparison atomically. The writer inventory and per-point CAS/ordering proof
remain acceptance work for #1326 even though #1312 is closed.

### Post-commit convergence is orthogonal to the fence (#1321)

The same `SetDisks::rename_data` path already returns a post-commit
convergence classification (`RenameConvergence`, rustfs/backlog#1321) that
tells the caller whether the *committed* replicas need heal to converge —
`AllSuccessIdentical` (no heal), `PartialCommit` (a replica failed/offline),
`SignatureDivergent` (committed replicas' version signatures differ), or
`Unknown` (no signature was produced, e.g. >10 versions — scanner-backstopped).
This replaced an earlier `Option<Vec<u8>>` heuristic under which any
version signature looked like "needs heal", so every healthy multipart
completion self-enqueued.

Convergence is a *post-commit* signal (the write landed; do the replicas need
reconciliation), whereas the #1312 fence is a *commit* gate (a stale epoch is
rejected before the write lands, surfaced through the existing `Result::Err`
channel). They compose on the one `rename_data` path rather than competing:
the fence decides whether a convergence is produced at all, and
`RenameConvergence` classifies it once produced. A future fence-aware
convergence variant, if ever needed, is an additive change to that enum and
does not disturb the epoch comparison at the disk-write points above.

## Transport and security contract

Generation and all derived tokens (lease, reservation) cross node boundaries in
internode RPC bodies. Every such flow must be signature-bound.

### RPC signature binding (#1312 B3, #1313, #1318)

**Requirement.** The canonical body carrying a generation or derived token must
be folded into the internode HMAC. The authenticated scope binds the target
audience, exact service/method, timestamp, nonce, canonical body digest, and
receiver replay epoch. The receiver must consume the nonce in a bounded replay
cache; transmitting a nonce without receiver-side consumption is not replay
protection.

**Current substrate (verified on main).** The original legacy-only description
is obsolete:

- RPC v2 binds target audience, exact method, POST, timestamp, nonce, and body
  digest.
- Body-bound policy covers mutating disk RPCs including `RenameData`; its
  versioned canonical body includes every `RenameDataRequest` field, so the
  `FileInfo` metadata map carrying the transaction UUID is authenticated.
- PR #5425 extended canonical-body enforcement to implemented non-disk mutating
  unary RPCs and added an exact policy/handler coverage partition.
- PR #5455 added the receiver boot epoch and rotating replay scope so signatures
  captured before a receiver restart are rejected after capability convergence.

The rollout switches
`RUSTFS_INTERNODE_RPC_SIGNATURE_STRICT`,
`RUSTFS_INTERNODE_RPC_BODY_DIGEST_STRICT`, and
`RUSTFS_INTERNODE_RPC_REPLAY_SCOPE_STRICT` remain default-off for rolling
compatibility. The compatibility register and fallback/overflow metrics govern
their fleet convergence. Therefore a generation capability may claim strong
transport binding only when the relevant strict modes have converged; the
object-transaction gate's current remote-version-state fleet proof is not, by
itself, proof of RPC signature/body/replay strictness.

Acceptance for each generation consumer includes method substitution, canonical
body tamper, nonce replay, receiver restart, and stripped-strict-metadata
negative tests. Generation rollout must also record which strict-mode evidence
authorized enforcement.

### Encoding contract (#1312 B1)

The on-disk persistence of generation must not perturb the file format:

- **Do not bump `XL_META_VERSION` / `XL_HEADER_VERSION`.**
  `crates/filemeta/src/filemeta/codec.rs` rejects `meta_ver > 3` and
  `header_ver > 3` outright (`decode_xl_headers`), and both constants are `3`
  (`crates/filemeta/src/filemeta.rs:53-54`). Bumping either makes every new
  `xl.meta` unreadable by rolling-upgrade old RustFS nodes and by MinIO — a
  total read failure, not a graceful downgrade.
- **Do not add generation as a `FileInfo` struct field.** The internode RPC layer serializes `FileInfo` with two different msgpack encoders depending on the call site: `encode_msgpack` uses rmp_serde's default **array** (positional) encoding for the `read_version` family, where a new positional field breaks decode across mixed-version nodes; `encode_msgpack_named` uses `.with_struct_map()` (named-map) encoding for `rename_data` (`crates/ecstore/src/cluster/rpc/remote_disk.rs`), which is more tolerant but still requires `#[serde(default)]` and MinIO-side agreement. Because a `FileInfo` field would have to be correct under *both* encoders and under the JSON compatibility twin (see "Wire-encoding migration" below), do not add one — use the metadata map, which rides through every encoder unchanged.
- **Where it lives today.** The object transaction UUID uses the version's
  internal metadata map under the dual-key contract
  (`x-rustfs-internal-*` / `x-minio-internal-*`) via
  `set_object_transaction_epoch`. Missing, malformed, nil, or conflicting dual
  values fail closed when fencing is active.
- **Sidecars are not an equivalent alternative.** A future sidecar is admissible
  only if it commits atomically with `xl.meta` and has a specified crash-recovery
  protocol. No such protocol is implemented, so a sidecar cannot be selected by
  an implementation issue merely because this document mentions one.
- **Regression guard.** Preserve the #4377 real-MinIO `xl.meta` interop
  regression (the fixture family around `crates/filemeta/src/filemeta.rs`):
  objects written by a new node must still be readable by old RustFS nodes and
  by MinIO, in both upgrade and downgrade directions.

### Wire-encoding migration (JSON → msgpack) interaction

The internode RPC layer retains a JSON/msgpack rolling-compatibility window, and
generation-bearing fields must respect it.

- **Dual-field transport.** Each dual-encoded RPC field exists twice in `crates/protos/src/node.proto`: a JSON `string` field and a msgpack `bytes _bin` field (e.g. `file_info` #4 alongside `file_info_bin` #7 on `RenameDataRequest`). Senders emit both; receivers `decode_msgpack_or_json` prefer the `_bin` form and fall back to the JSON string only when `_bin` is empty (`crates/ecstore/src/cluster/rpc/remote_disk.rs`).
- **Capability flags, default off.** `rustfs_protos::internode_rpc_msgpack_only()` only drops the redundant JSON copy when both `RUSTFS_INTERNODE_RPC_MSGPACK_ONLY=true` and `RUSTFS_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED=true` are deliberately enabled after the JSON-fallback metric reads zero fleet-wide and the convergence runbook is followed. Generation follows the same default-off, fleet-confirmed, metric-reads-zero rollout discipline, but a msgpack proof is not itself a generation capability proof.
- **Generation must ride both encodings during the window.** If epoch lives in the version's internal metadata map, that map is carried inside `FileInfo`, so it is present in both the msgpack `_bin` and JSON copies automatically — good. But any new *top-level* generation datum must be added to **both** the msgpack and JSON representations (and, for msgpack, be safe under both the array and named-map encoders). A field added to only one encoding is silently lost the moment a peer falls back to the other — exactly the failure the JSON-fallback metric exists to catch.
- **Signature binds a canonical form.** `RenameDataRequest` now has a versioned,
  injective canonical-body encoder that covers both compatibility fields and is
  authenticated independently of whichever JSON/msgpack decoder branch a peer
  consumes. A generation-capable strict request must reject missing or
  mismatched canonical-body metadata; it must not silently downgrade to an
  unauthenticated JSON twin.

### Proto evolution

No top-level proto field is required by the current metadata-map UUID. If a
future ordered epoch or explicit expected-generation is added to proto, it uses
**proto3 `optional`** (explicit presence). A non-optional scalar is forbidden:
an old coordinator talking to a new disk decodes absence as a plausible zero.

### Mixed-version gate — one direction

When generation enforcement is not explicitly requested, or fleet confirmation
is absent, behavior falls back to current semantics. Fail-closed is reserved for
an explicit administrator-confirmed strict rollout.

Current object transaction fencing follows that direction:

- `RUSTFS_OBJECT_TRANSACTION_FENCING_WRITE` and
  `RUSTFS_OBJECT_TRANSACTION_FENCING_FLEET_CONFIRMED` both default false.
- With either flag absent, PUT/MPU does not persist or consume the transaction
  UUID.
- With both flags enabled, failure to obtain or retain the live fleet proof
  rejects the commit before rename.

This is an opt-in strict gate, not a negotiated generation capability. The
proof is currently borrowed from the remote-version-state writer rollout. It
proves current membership/process-epoch convergence for that feature, but does
not prove an epoch type, per-disk generation CAS support, or RPC strict-mode
convergence. Treating it as the final handshake is forbidden without an
explicit proof mapping for those properties.

## Capability negotiation

Generation enforcement requires one **live fleet proof**, not independent
boolean guesses in each consumer. The proof contract contains at least:

1. the selected authority version and comparison mode (ordered or exact-CAS),
2. the current membership/topology fingerprint and process epochs,
3. support for every required disk mutation point,
4. RPC signature/body/replay strict convergence, and
5. the on-disk encoding version (the current metadata-map UUID is version 1).

The authoritative writer enables enforcement only while every target disk in
the set is covered by a current proof. Membership change or an old node rejoin
revokes that proof. Revocation before commit fails an explicitly strict request;
when strict generation was never requested, the request remains on the legacy
path. Revocation never rewrites or lowers an already-persisted generation.

The existing fleet-proof machinery in `notification_sys` may be reused if its
authenticated statements are extended to cover the properties above. The
runtime capability contract may instead expose the proof. This document does
not choose the storage mechanism; it requires one token whose acquisition and
revalidation semantics are shared by all consumers.

## Implementation order

Some original prerequisites have landed, but not in the originally proposed
form. Remaining work follows this order:

1. **Resolve the authority mode in #1326.** Select total order or opaque
   exact-CAS, specify its atomic commit point, and audit PR #6077 against it.
   Do not retrofit ordering semantics onto the existing random UUID.
2. **Define the generation fleet proof.** Map generation enablement to the RPC
   signature/body/replay strict proofs delivered by #1327/#1541/#1542 and to
   the selected per-disk comparison capability. Keep all strict defaults off
   until fallback metrics converge.
3. **Implement #1313 generation-bound read leases.** The lease registry,
   cross-node visibility, TTL, and crash recovery must exist before old-dir GC
   can claim the full snapshot-lifetime guarantee. #1325 supplies the required
   multi-node failure tests.
4. **Bind #1314 prepared reads.** A bundle binds the exact selected generation
   within its source pool. Cross-pool ordering is forbidden until a common
   authority is demonstrated. Validate rebalance and mixed-version fallback in
   the #1325 multi-pool harness.
5. **Reconcile #1318 quota fencing.** Either bind reserve/settle/reconcile to
   the selected object generation or document and prove that its independent
   snapshot-lease fence is a separate arbitration domain that cannot settle a
   different generation.
6. **Re-audit #1323 cleanup.** The existing UUID receipt remains valid crash
   cleanup, but full closure against active readers requires the #1313 lease
   check and the selected generation semantics.

## Open design decisions (pin before contract closure)

The following decisions remain blockers for calling the contract implemented:

- **Authority mode.** Choose total order or opaque exact-CAS. If total order is
  selected, define the type, per-object scope, persistence, overflow, and
  never-regress restart/minority-recovery tests. If opaque identity is selected,
  define the atomic expected-generation CAS and remove all ordered wording.
- **Complete xl.meta-writer coverage.** Enumerate commit rename, rollback
  restore/delete, cleanup, heal, transition, restore, replication, and data
  movement. Each path must compare/carry the selected generation or be proved
  incapable of replacing the authoritative object identity.
- **Rollback is an expected-generation CAS (#1312 B2).** The quorum-failure
  rollback in `rename_data` can restore backup metadata, not just remove a
  writer-private temporary file. It must execute only when the stored generation
  still matches the failed writer's expected generation. Panic, cancel, and
  timeout outcomes must be reaped into coordinator convergence rather than skip
  rollback through an early return.
- **Sidecar is excluded unless proven atomic.** An epoch sidecar outside `xl.meta` is only admissible if it commits at the same atomic/CAS point as `xl.meta` with a defined recovery; otherwise it opens a crash gap and must be rejected in favor of the version-internal metadata map. The earlier "metadata map or sidecar" phrasing does not treat the two as equally safe.
- **Generation capability proof.** Decide whether to extend the current
  authenticated fleet proof or the runtime capability contract. It must prove
  authority version, mutation coverage, topology/process epoch, and RPC strict
  convergence in one revalidatable token.
- **Read-lease and GC crash recovery.** Select the cross-node registry, TTL
  reclamation, lease-holder crash behavior, and GC-executor recovery. The
  current cleanup receipt equality check does not answer these questions.
- **Quota reserve → commit → settle binding.** The durable ledger's idempotency
  exists, but its independent mutation tokens must be related to the selected
  object generation with a concrete late-settle rejection test.
- **PreparedPoolRead is pool-local only.** A #1314 bundle's generation validates freshness only within the pool that produced it. It cannot order commits across different pools unless a cross-pool common authority exists; absent that, the multi-pool wait cannot be short-circuited.
- **Hot-path cost is a blocking metric.** Measure any additional consensus
  write, fsync, fleet-proof lookup, lease operation, or centralized serialization
  under 4 KiB and high-concurrency hot-key/hot-bucket A/B.
- **Test infrastructure.** #1325 still lacks the complete 4-node × 4-drive,
  2-pool, directed network-fault, and large-object budget needed for restart,
  mixed-version, and cross-node lease acceptance.

## Acceptance for this contract

- [x] Architecture document exists and is linked from the architecture index.
- [x] Transport signature, encoding, proto presence, mixed-version direction,
  and capability-proof requirements are defined once.
- [x] Current implementations are separated from target guarantees; a closed
  child issue is not treated as proof of unified generation binding.
- [ ] Authority mode and atomic comparison semantics are selected and tested.
- [ ] #1312 / #1313 / #1314 / #1318 / #1323 bodies reference this document and
  use the selected authority terminology.
- [ ] #1313 and #1314 bind the selected generation and pass #1325 multi-node /
  multi-pool failure tests.
- [ ] #1318 either binds reserve/settle to the selected generation or provides
  an accepted proof that its separate fence cannot cross-settle generations.
- [ ] #1323 reconciliation checks both committed generation and active
  generation-bound leases.
- [ ] Generation strict enablement is backed by one live proof that includes RPC
  signature/body/replay strict convergence and per-disk comparison support.
