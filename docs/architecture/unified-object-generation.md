# Object Transaction UUID And Generation-Fencing Contract

**Use this when:** adding or changing anything that fences a commit, scopes a read lease, gates old-directory cleanup, binds prepared pool reads, or settles quota against "the current version of an object", or when adding a field that rides internode RPC or `xl.meta`.
**Source of truth:** `assign_object_transaction_epoch` in `crates/ecstore/src/set_disk/ops/object.rs` and `crates/ecstore/src/set_disk/ops/multipart.rs`; `FileInfo::set_object_transaction_epoch` in `crates/filemeta/src/fileinfo.rs`; `commit_rename_data_dir` and `RenameConvergence` in `crates/ecstore/src/set_disk/core/io_primitives.rs`; `PreparedPoolReadFallbackBarrier` in `crates/ecstore/src/store/rebalance.rs`; `crates/protos/src/node.proto`; env constants in `crates/config/src/constants/object.rs` and `crates/config/src/constants/internode.rs`.

Design tracking lives in `rustfs/backlog#1326`. This document holds only the invariants.

## Authority

The target contract requires **one per-object commit identity** consumed by commit fencing, read leases, cleanup, prepared reads, and quota settlement. No consumer may mint a second value and call it the same generation.

What exists today is an **object transaction UUID**, not the target authority:

| Property | Current implementation |
|---|---|
| Minting | `assign_object_transaction_epoch` mints a random non-nil UUID for PUT and CompleteMultipartUpload when the object-transaction gate is active. |
| Persistence | Written through `FileInfo::set_object_transaction_epoch` into the version's internal metadata map under the dual-key contract (`x-rustfs-internal-*` / `x-minio-internal-*`). |
| Fence check | The coordinator reads the current UUID (or `Absent`) and revalidates exact equality immediately before `rename_data`. |
| Cleanup | Old-data cleanup receipts carry the committed UUID; reconciliation deletes only when the receipt UUID still equals the current object UUID. |

This is an equality-CAS fence and cleanup identity. It is not a monotonic epoch, is not minted by the distributed lock grant, and is not compared atomically at each disk's `xl.meta` commit point. Documents and issues must call it the *object transaction UUID*, not proof that the generation authority exists.

### Authority modes (one must be selected)

| Mode | Contract | Persistence requirement |
|---|---|---|
| Total-ordered fencing epoch | A lock grant returns a durable per-object `(term, counter)`; every disk rejects a lower epoch at the atomic metadata commit point; the value never regresses across lock-plane restart, failover, or minority recovery. | Quorum-persisted before grant, or derived from a durable term whose full comparison cannot regress. The in-memory distributed lock entry alone is insufficient. |
| Opaque commit-generation identity | Consumers compare exact identity only; no `<` / `>` semantics. The authoritative commit performs an atomic expected-generation CAS; lease, cleanup, prepared-read, and quota contracts are phrased as "references this exact generation". | Atomic expected-identity comparison plus durable crash recovery. |

The current UUID proves neither a durable total order nor a per-disk atomic CAS, so it does not decide between the modes.

## Consumer Binding

| Consumer | Binds generation how | Key invariant | Current state |
|---|---|---|---|
| Commit fence (PUT / CompleteMultipartUpload) | Checked at `rename`, rollback restore/delete, and cleanup mutation points using the selected rule | A stale writer is rejected on **all** disks; an already-ACK'd write is never rolled back | Opt-in UUID equality recheck before rename; no per-disk atomic comparison |
| Read lease | Lease binds the exact generation observed at read time; GC runs only after every lease on that generation is released | Lease visible across nodes; crashed reader's lease reclaimed by TTL | Streaming/multipart GET holds the namespace read lock through EOF/drop (part-boundary coverage: `#6887`); no cross-node generation-bound registry |
| Old-dir GC | Cleanup job carries the committed generation and confirms no lease owns `old_dir` before deleting | `old_dir != committed_dir`; a still-referenced directory is never deleted | UUID receipt equality (`#6077`); no lease consultation |
| Prepared pool read | The prepared bundle carries the generation resolved during pool lookup; the chosen pool reuses it only after a match | Mismatch forces fallback to full metadata fanout | `PreparedPoolReadFallbackBarrier` (`#6889`) is a pool-local identity that fails closed / refetches on pool state change; it is not a cross-pool authority |
| Quota reservation | Reserve / settle record binds the exact object generation (and the ordered epoch too, if selected) | A late commit cannot settle quota for a different committed generation | Durable per-bucket ledger with independent snapshot-lease fence tokens (`#6058`); not bound to the transaction UUID |

## Fence Coverage: Three Disk-Write Points

Checking generation only before the `rename` fanout is insufficient. The commit sequence is `tmp sync → data-dir rename → xl.meta commit → directory sync` in `crates/ecstore/src/disk/local.rs`, and `crates/ecstore/src/set_disk/core/io_primitives.rs` has two further detachable disk-write points:

1. **Rollback restore/delete.** On quorum failure each disk can restore backup metadata or delete the failed version. A stale writer's rollback must compare the expected generation, or it can overwrite or delete the winner's committed metadata. Panic, cancel, and timeout outcomes must be reaped into coordinator convergence rather than skip rollback through an early return.
2. **`commit_rename_data_dir`.** A cancel-then-detach disk-write point; the coordinator's "reap all child tasks" must include it so a cancelled writer cannot bypass fence or lease and keep deleting directories.

If generation is validated only after the data-dir rename, a fenced writer may already have renamed its data-dir into the object path, leaving a staged orphan. Either move the fence ahead of the data-dir rename, or declare that orphan an accepted residue accounted for by GC metrics.

`RenameConvergence` (`AllSuccessIdentical` / `PartialCommit` / `SignatureDivergent` / `Unknown`) is a *post-commit* heal signal on the same `rename_data` path; the fence is a *commit* gate. They compose: the fence decides whether a convergence is produced, `RenameConvergence` classifies it. A fence-aware convergence variant would be an additive enum change.

## Transport And Security

Generation and derived tokens (lease, reservation) cross node boundaries in internode RPC bodies; every such flow must be signature-bound.

| Rule | Detail |
|---|---|
| HMAC scope | Target audience, exact service/method, timestamp, nonce, canonical body digest, receiver replay (boot) epoch. The receiver consumes the nonce in a bounded replay cache; a transmitted-but-unconsumed nonce is not replay protection. |
| Current substrate | RPC v2/v3 in `crates/ecstore/src/cluster/rpc/http_auth.rs` binds all of the above. Body-bound policy covers mutating disk RPCs including `RenameData`, whose versioned canonical body includes every `RenameDataRequest` field, so the `FileInfo` metadata map carrying the UUID is authenticated. |
| Strict switches | `RUSTFS_INTERNODE_RPC_SIGNATURE_STRICT`, `RUSTFS_INTERNODE_RPC_BODY_DIGEST_STRICT`, `RUSTFS_INTERNODE_RPC_REPLAY_SCOPE_STRICT` (`crates/config/src/constants/internode.rs`) are default-off rollout gates governed by [compat-cleanup-register.md](compat-cleanup-register.md). A generation capability may claim strong transport binding only after the relevant strict modes have converged fleet-wide. |
| Acceptance tests per consumer | Method substitution, canonical body tamper, nonce replay, receiver restart, stripped-strict-metadata negatives. |

## Encoding Rules

| Rule | Reason |
|---|---|
| **Do not bump `XL_META_VERSION` or `XL_HEADER_VERSION`** (`crates/filemeta/src/filemeta.rs`). | `decode_xl_headers` in `crates/filemeta/src/filemeta/codec.rs` rejects newer values outright; a bump makes every new `xl.meta` unreadable by rolling-upgrade old nodes and by MinIO. See [minio-file-format-compat.md](minio-file-format-compat.md). |
| **Do not add generation as a `FileInfo` struct field.** | Internode RPC serializes `FileInfo` with two msgpack encoders: positional-array encoding for the `read_version` family (a new positional field breaks mixed-version decode) and `encode_msgpack_named` (named-map) for `rename_data` in `rustfs/src/storage/rpc/node_service/disk.rs`. A field would have to be correct under both plus the JSON compatibility twin. Use the metadata map, which rides every encoder unchanged. |
| **Metadata-map dual key.** | The UUID lives under `x-rustfs-internal-*` / `x-minio-internal-*`; missing, malformed, nil, or conflicting dual values fail closed when fencing is active. |
| **No sidecar unless atomic.** | An epoch sidecar outside `xl.meta` is admissible only if it commits at the same atomic/CAS point as `xl.meta` with a specified crash-recovery protocol. None is implemented. |
| **Regression guard.** | The real-MinIO `xl.meta` interop fixtures in `crates/filemeta/src/filemeta.rs` must keep passing: objects written by a new node stay readable by old RustFS nodes and by MinIO in both upgrade directions. |

### Wire-encoding window (JSON and msgpack)

- Dual-encoded RPC fields exist twice in `crates/protos/src/node.proto`: a JSON `string` field and a msgpack `bytes *_bin` field (e.g. `file_info` and `file_info_bin` on `RenameDataRequest`). Senders emit both; receivers (`decode_msgpack_or_json` in `crates/ecstore/src/cluster/rpc/remote_disk.rs`) prefer `_bin` and fall back to JSON only when `_bin` is empty.
- `rustfs_protos::internode_rpc_msgpack_only()` drops the JSON copy only when both `RUSTFS_INTERNODE_RPC_MSGPACK_ONLY` and `RUSTFS_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED` are set after the JSON-fallback metric reads zero fleet-wide.
- Generation inside the `FileInfo` metadata map is carried in both copies automatically. Any new *top-level* generation datum must be added to both encodings and be safe under both msgpack encoders; a field in only one encoding is silently lost when a peer falls back.
- `RenameDataRequest` has a versioned, injective canonical-body encoder covering both compatibility fields; a strict generation-capable request must reject missing or mismatched canonical-body metadata rather than downgrade to the unauthenticated JSON twin.

### Proto evolution

No top-level proto field is required by the metadata-map UUID. If an ordered epoch or explicit expected-generation is ever added to proto, it uses **proto3 `optional`** (explicit presence). A non-optional scalar is forbidden: an old coordinator talking to a new disk decodes absence as a plausible zero.

## Mixed-Version Gate: One Direction

When generation enforcement is not explicitly requested, or fleet confirmation is absent, behavior falls back to current semantics. Fail-closed is reserved for an explicit administrator-confirmed strict rollout.

| Flag (`crates/config/src/constants/object.rs`) | Default | Effect |
|---|---|---|
| `RUSTFS_OBJECT_TRANSACTION_FENCING_WRITE` | false | With either flag absent, PUT/MPU neither persists nor consumes the transaction UUID. |
| `RUSTFS_OBJECT_TRANSACTION_FENCING_FLEET_CONFIRMED` | false | With both enabled, failure to obtain or retain the live fleet proof rejects the commit before rename. |

The fleet proof is currently borrowed from the remote-version-state writer rollout. It proves membership/process-epoch convergence for that feature only; it does not prove an epoch type, per-disk CAS support, or RPC strict-mode convergence, and must not be treated as the final generation handshake.

### Capability negotiation (target)

Generation enforcement requires one **live fleet proof** containing at least: the selected authority version and comparison mode; the current membership/topology fingerprint and process epochs; support for every required disk mutation point; RPC signature/body/replay strict convergence; and the on-disk encoding version (the metadata-map UUID is version 1). Membership change or an old-node rejoin revokes the proof; revocation before commit fails an explicitly strict request and never rewrites or lowers a persisted generation. The proof may extend the authenticated fleet-proof machinery in `notification_sys` or the runtime capability contract; this document requires one shared token, not a mechanism.

## Open Decisions

Blockers for calling the contract implemented:

1. **Authority mode.** Total order or opaque exact-CAS. Do not retrofit ordering semantics onto the existing random UUID.
2. **Complete `xl.meta`-writer coverage.** Enumerate commit rename, rollback restore/delete, cleanup, heal, transition, restore, replication, and data movement; each path compares/carries the selected generation or is proved incapable of replacing the authoritative identity.
3. **Rollback as expected-generation CAS.** The quorum-failure rollback in `rename_data` restores backup metadata, not just a private temp file; it must run only when the stored generation still matches the failed writer's expectation.
4. **Generation capability proof.** Extend the fleet proof or the runtime capability contract; one revalidatable token.
5. **Read-lease and GC crash recovery.** Cross-node registry, TTL reclamation, lease-holder crash behavior, GC-executor recovery.
6. **Quota reserve → commit → settle binding.** Relate the ledger's independent mutation tokens to the selected generation, with a concrete late-settle rejection test, or prove the fence is a separate arbitration domain that cannot cross-settle.
7. **Prepared reads stay pool-local.** `PreparedPoolReadFallbackBarrier` validates freshness only within the pool that produced it; cross-pool ordering requires a common authority, and the multi-pool wait cannot be short-circuited without one.
8. **Hot-path cost is a blocking metric.** Measure any added consensus write, fsync, fleet-proof lookup, lease operation, or centralized serialization under 4 KiB and hot-key/hot-bucket A/B.
9. **Test infrastructure.** Multi-node, multi-pool, directed network-fault, and large-object budget for restart, mixed-version, and cross-node lease acceptance.
