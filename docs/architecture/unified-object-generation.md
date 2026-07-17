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

The fix is a single authority with one monotonic source, one persistence
semantics, and one transport binding, that every consumer references rather than
re-derives.

## The authority (single source)

**The per-object fencing epoch defined by #1312 is the sole generation
authority.** No other monotonic counter, timestamp, or random token may stand in
for generation.

- The distributed lock grant returns a monotonic `epoch` for the object key.
  Acquiring the object write-lock is the only way to mint a new generation.
- The epoch travels down the authoritative commit path (with
  `RenameDataRequest` / the local `DiskAPI` call) and is compared at each disk's
  atomic `xl.meta` commit point, rejecting stale epochs. It adds no extra
  network round trip (#1312 implementation clause 2).
- Every consumer in the table below **binds** this epoch. None defines its own.

### Monotonicity persistence semantics

The epoch must be **monotonic across lock-plane restart and failover**
(#1312 B4). Today the distributed lock entry is in-memory only
(`crates/lock/src/distributed_lock.rs` has no persistence path), so a lock-service
restart resets the counter to zero: a new writer draws epoch 1 while disks have
already observed epoch 100, producing either a permanent write rejection or a
fence *inversion*. To prevent this, the epoch must be one of:

1. **Quorum-persisted** before it is handed to a writer, or
2. **Derived from a durable monotonic source** — a `(term, counter)` pair where
   `term` advances on every lock-service leadership change and is itself durable,
   so the composite never regresses even when `counter` resets.

The comparison at the disk commit point is on the full composite; a lower
`(term, counter)` is always rejected.

## Consumer binding contracts

| Consumer | How it binds generation | Key invariant |
|---|---|---|
| #1312 commit fence | epoch compared at three disk-write points — `rename`, rollback `delete`, and `commit_rename_data_dir` cleanup | stale epoch rejected on **all** disks; an already-ACK'd write is never rolled back |
| #1313 read lease | lease binds the generation observed at read time; GC runs only after every lease referencing that generation is released | lease is visible across nodes; a crashed reader's lease is reclaimed by TTL |
| #1323 old-dir GC | cleanup job carries the committed generation; before deleting `old_dir` it confirms no lease referencing a lower generation still points at it | `old_dir != committed_dir`; a still-referenced directory is never deleted |
| #1314 prepared pool read | the `PreparedPoolRead` bundle carries the generation resolved during pool lookup; the chosen pool's reader setup reuses it only after a match | generation mismatch forces a fallback to full metadata fanout |
| #1318 quota reservation | reservation / settle token binds the object generation | a late commit holding an old-generation token cannot settle a newer generation |

### Fence coverage is three disk-write points, not one (#1312 B2)

Comparing the epoch at the `rename` commit point alone is insufficient. The
authoritative commit sequence is `tmp sync → data-dir rename → xl.meta commit →
directory sync` in `crates/ecstore/src/disk/local.rs`, and there are two further
detachable disk-write points in
`crates/ecstore/src/set_disk/core/io_primitives.rs`:

- **Rollback delete** — on quorum failure each disk runs
  `delete_version(undo_write=true)`. A fenced old writer's rollback must also
  compare epoch, otherwise it deletes the winner's already-committed version.
- **`commit_rename_data_dir`** — a cancel-then-detach disk-write point; the
  coordinator's "reap all child tasks" must explicitly include it so a cancelled
  writer cannot bypass fence/lease and keep deleting directories.

If the epoch is validated only at the `xl.meta` commit point, a fenced writer
may already have renamed its data-dir into the object path, leaving a staged
orphan. Either move the fence ahead of the data-dir rename, or declare that
orphan an acceptable residue accounted for by GC metrics — the white-box
acceptance "no background disk write after release" must be rewritten
accordingly.

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

**Requirement.** The RPC body digest carrying a generation/epoch/token must be
folded into the RPC HMAC, binding `method + object key + generation`, and the
request must carry a nonce / one-shot identifier inside the 300s replay window.
The nonce is only meaningful if the **receiver enforces it**: each disk keeps a
bounded seen-nonce cache covering the 300s freshness window and rejects any
request whose nonce was already observed. A nonce that is merely transmitted but
not checked provides no replay protection.

This generalizes the existing `walk_dir` pattern: `walk_dir` computes a
`Sha256` of the request body and places it in the signed URL query as
`walk_dir_body_sha256`
(`crates/ecstore/src/cluster/rpc/internode_data_transport.rs:187`), so the body
digest is transitively covered by the URL signature. New generation-bearing RPCs
adopt the same `*_body_sha256` mechanism.

**Current gap (verified).** The internode HMAC covers only
`{path_and_query}|{method}|{timestamp}`
(`signature_payload`, `crates/ecstore/src/cluster/rpc/http_auth.rs:75-83`). It
binds neither the request body nor a nonce, and the 300s freshness window has no
one-shot guard. Without the binding above:

- An on-path or replaying attacker can inject a high epoch (e.g. `u32::MAX`) and
  **permanently fence out** a key's legitimate writes — monotonicity only
  rejects *low/old* epochs, never a forged-high one.
- A captured lease/reservation token can be replayed within 300s to block
  old-dir GC (storage-exhaustion DoS) or to double-reserve / prematurely settle
  quota.

Acceptance for each consumer must include: "a replayed old signature to a
different method, and a forged-high-epoch request, are both rejected."

### Encoding contract (#1312 B1)

The on-disk persistence of generation must not perturb the file format:

- **Do not bump `XL_META_VERSION` / `XL_HEADER_VERSION`.**
  `crates/filemeta/src/filemeta/codec.rs` rejects `meta_ver > 3` and
  `header_ver > 3` outright (`decode_xl_headers`), and both constants are `3`
  (`crates/filemeta/src/filemeta.rs:53-54`). Bumping either makes every new
  `xl.meta` unreadable by rolling-upgrade old RustFS nodes and by MinIO — a
  total read failure, not a graceful downgrade.
- **Do not add generation as a `FileInfo` struct field.** The internode RPC layer serializes `FileInfo` with two different msgpack encoders depending on the call site: `encode_msgpack` uses rmp_serde's default **array** (positional) encoding for the `read_version` family, where a new positional field breaks decode across mixed-version nodes; `encode_msgpack_named` uses `.with_struct_map()` (named-map) encoding for `rename_data` (`crates/ecstore/src/cluster/rpc/remote_disk.rs`), which is more tolerant but still requires `#[serde(default)]` and MinIO-side agreement. Because a `FileInfo` field would have to be correct under *both* encoders and under the JSON compatibility twin (see "Wire-encoding migration" below), do not add one — use the metadata map, which rides through every encoder unchanged.
- **Where it may live.** Only inside a version's internal metadata **map**
  (MinIO skips unknown internal keys and the map encoding is extensible) or in a
  per-disk sidecar outside `xl.meta`. If it goes in the metadata map, it must
  obey the dual-key contract (`x-rustfs-internal-*` / `x-minio-internal-*`, see
  AGENTS.md "Cross-Cutting Domain Invariants").
- **Regression guard.** Preserve the #4377 real-MinIO `xl.meta` interop
  regression (the fixture family around `crates/filemeta/src/filemeta.rs`):
  objects written by a new node must still be readable by old RustFS nodes and
  by MinIO, in both upgrade and downgrade directions.

### Wire-encoding migration (JSON → msgpack) interaction

The internode RPC layer is mid-migration from JSON to msgpack binary, and generation-bearing fields must respect that migration window — this is not optional context, it changes how epoch is transported.

- **Dual-field transport.** Each dual-encoded RPC field exists twice in `crates/protos/src/node.proto`: a JSON `string` field and a msgpack `bytes _bin` field (e.g. `file_info` #4 alongside `file_info_bin` #7 on `RenameDataRequest`). Senders emit both; receivers `decode_msgpack_or_json` prefer the `_bin` form and fall back to the JSON string only when `_bin` is empty (`crates/ecstore/src/cluster/rpc/remote_disk.rs`).
- **Capability flag, default off.** `rustfs_protos::internode_rpc_msgpack_only()` (env `RUSTFS_INTERNODE_RPC_MSGPACK_ONLY`, default **false**) gates dropping the redundant JSON copy. It may only be flipped after the `record_msgpack_json_fallback` metric reads zero fleet-wide and the convergence runbook is followed (`crates/protos/src/lib.rs:146`). **Reuse this exact capability + metric-reads-zero model as the mixed-version gate for generation** rather than inventing a parallel handshake; the section above ("Capability negotiation") is layered on top of it, not instead of it.
- **Generation must ride both encodings during the window.** If epoch lives in the version's internal metadata map, that map is carried inside `FileInfo`, so it is present in both the msgpack `_bin` and JSON copies automatically — good. But any new *top-level* generation datum must be added to **both** the msgpack and JSON representations (and, for msgpack, be safe under both the array and named-map encoders). A field added to only one encoding is silently lost the moment a peer falls back to the other — exactly the failure the JSON-fallback metric exists to catch.
- **Signature must bind a canonical form.** Because a field is transmitted as both JSON and msgpack and a peer may consume either, the body-digest binding in "RPC signature binding" above must be computed over a single canonical representation (the msgpack `_bin` bytes) — not over whichever copy happened to be decoded. Once the `generation` capability is negotiated for a request, a fenced / generation-bearing request must **reject the JSON fallback path** so a downgrade to the unsigned/loosely-bound JSON copy cannot bypass the epoch check.

### Proto evolution

New generation/epoch proto fields use **proto3 `optional`** (explicit presence).
A non-optional field is forbidden: an old coordinator talking to a new disk
decodes an absent field as `0`, which is indistinguishable from a real
`epoch == 0` and silently breaks the "stale epoch rejected" invariant during
upgrade.

### Mixed-version gate — one direction

When the cluster-level generation capability is **not** negotiated on every
target disk, the behavior **falls back to current semantics** (existing lock +
`is_lock_lost()` check for #1312; degraded-allow read-check for #1318 at
`rustfs/src/app/object_usecase.rs`; full fanout for #1314). Fail-closed is
**only** an explicit administrator strict mode. Defaulting to fail-closed is
forbidden — it makes writes unavailable for the whole rolling-upgrade window.

## Capability negotiation

Generation enforcement is a **cluster-level handshake**, not a per-request
probe:

- A node advertises a `generation` capability once it can (a) mint quorum-durable
  epochs, (b) compare epochs at all three disk-write points, and (c) verify the
  body-digest-bound RPC signature.
- The authoritative writer enables hard enforcement for an object only when
  **all** target disks in the set advertise the capability. Any missing
  advertisement pins that commit to the mixed-version fallback above.
- The capability is surfaced through the existing runtime capability contract
  surface (see [runtime-capability-contracts.md](runtime-capability-contracts.md)),
  so consumers read one negotiated flag rather than each re-deriving support.
- Enforcement tracks the current membership rather than latching: it turns on
  for a set only while every disk in that set advertises `generation`, and a
  single old node rejoining drops the affected sets back to the mixed-version
  fallback rather than failing closed. It never regresses the on-disk epoch —
  falling back stops *comparing* new epochs, it does not lower any epoch already
  persisted.

## Implementation order

1. **#1312 first.** It defines the epoch, its persistence, the three fence
   points, the RPC signature binding, and the encoding location. Everything
   downstream depends on its epoch existing.
2. **#1313** (read lease) reuses the #1312 epoch as the lease generation and
   must land before or alongside #1323.
3. **#1323** (old-dir GC) depends on #1313 leases being present and
   cross-node-visible; its "no lease references old_dir" check has nothing to
   query otherwise.
4. **#1318** (quota reservation) and **#1314** (prepared pool read) bind the
   epoch independently; both gate on the same capability handshake.

## Open design decisions (pin before implementation)

This document fixes the transport, encoding, proto, and gate constraints, but it is not yet a complete implementable algorithm. The following must be decided and written down before any of the five consumers is coded (per the #1307 maintainer re-review, issuecomment-4992956256):

- **Epoch type and total order.** The concrete token type and its total-order rule — a term+counter tuple, its persistence, and overflow behavior. Whether monotonicity is global or strictly per-object.
- **Never-regress on lock-service restart / minority recovery.** The epoch source must survive a lock-service restart or minority-quorum recovery without ever handing out an epoch lower than one already persisted on disk (an in-memory counter reset to zero is a fencing inversion). This is the same requirement as "Monotonicity persistence semantics" above, elevated to a hard, tested acceptance.
- **Complete xl.meta-writer coverage.** Every code path that writes xl.meta (commit rename, rollback delete/metadata restore, old-dir cleanup, heal, transition) must be enumerated and shown to compare or carry the epoch. A single unfenced writer voids the guarantee.
- **Rollback is an expected-generation CAS (#1312 B2).** The quorum-failure rollback at `io_primitives.rs:2646-2691` restores a metadata backup, not just a per-writer tmp delete, so a late rollback by writer A can overwrite writer B's committed xl.meta. Rollback must execute only when `stored_epoch == failed_writer_epoch`; a higher stored epoch must abort the rollback. Task panic / cancel / timeout at `io_primitives.rs:2602-2605` must be reaped into the coordinator's state machine, never bubble out via `?` and skip convergence.
- **Sidecar is excluded unless proven atomic.** An epoch sidecar outside `xl.meta` is only admissible if it commits at the same atomic/CAS point as `xl.meta` with a defined recovery; otherwise it opens a crash gap and must be rejected in favor of the version-internal metadata map. The earlier "metadata map or sidecar" phrasing does not treat the two as equally safe.
- **Read-lease and GC crash recovery.** Lease registry location (local vs cross-node), TTL reclamation, and crash recovery for both the lease holder and the GC executor.
- **Quota reserve → commit → settle idempotency.** The cross-stage reconcile / idempotency story for #1318, including owner-crash reconciliation, so a reservation is neither lost nor double-counted.
- **PreparedPoolRead is pool-local only.** A #1314 bundle's generation validates freshness only within the pool that produced it. It cannot order commits across different pools unless a cross-pool common authority exists; absent that, the multi-pool wait cannot be short-circuited.
- **Hot-path cost is a blocking metric.** If per-PUT fencing grant, quota reserve, or cleanup journal adds a consensus write / fsync / centralized serialization point, it must be measured under 4KiB and high-concurrency hot-key / hot-bucket A/B as a blocking gate, not accepted by default.

## Acceptance for this contract

- #1312 / #1313 / #1314 / #1318 / #1323 bodies reference this unified
  generation and no longer define their own token.
- The five constraints — transport signature, encoding, proto presence,
  mixed-version gate direction, and capability negotiation — are pinned here
  once; each implementation sub-issue follows them rather than re-deciding.
