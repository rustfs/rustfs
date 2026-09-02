# Storage, Control Plane, And Background Controllers

**Use this when:** adding a storage API surface, a cluster read model, or a background-service status/reconcile surface, and you need to know which layer owns it and what must not drift.
**Source of truth:** `crates/storage-api` (trait contracts), `crates/ecstore/src/api/mod.rs` (facade groups, `api::cluster`), `crates/ecstore/src/cluster/` (control plane), [background-controller-contract.md](background-controller-contract.md) (controller vocabulary).

## Storage API Contracts

Storage API contracts must not absorb implementation details from ECStore or the
reader pipeline.

Out of scope for the contract layer:

- KMS/SSE implementation.
- Range and compression behavior.
- Erasure coding and bitrot logic.
- Remote disk transport and recovery.

No-drift behavior:

- Object-to-set hash remains unchanged.
- Write quorum remains unchanged.
- Reader decryption, etag/checksum, version, and delete-marker behavior remain
  unchanged.
- Public compatibility paths remain available through temporary re-exports or
  wrappers during pure moves.

## Cluster Control Plane

ClusterControlPlane starts as a read-only facade inside `crates/ecstore/src/cluster`.
Do not create a standalone cluster crate until internal dependencies are stable.

Initial scope:

- Topology snapshot.
- Membership snapshot.
- Lock registry snapshot.
- Peer health snapshot.
- Pool state snapshot.

The first read-only implementation lives behind `rustfs_ecstore::api::cluster`.
It maps existing endpoint pools into the shared storage-api topology contract and
an ECStore-owned static membership snapshot. It must not expose local disk paths,
start health checks, mutate endpoint ownership, or change placement/readiness.
The same facade also owns static pool-state, local-node storage, and peer-health
status projections. `peer_health_snapshot` in
`crates/ecstore/src/cluster/control_plane.rs` projects the internode health
tracker's per-node reachability (`PEER_HEALTH_REACHABLE` /
`PEER_HEALTH_UNREACHABLE`, or not-reported); the facade itself starts no probes
and issues no RPC-based health checks.
Readiness impact for storage, lock quorum, peer health, probes, admin routes,
RPC, and the S3 data plane is recorded in
[`readiness-matrix.md`](readiness-matrix.md).

Risk controls:

- Distributed lock quorum remains per set.
- RemoteDisk suspect/offline/recovery, timeout, and connection eviction semantics
  must not be simplified.
- Health impact behavior must be feature-gated if it changes production behavior.

## Background Controllers

Scanner, heal, lifecycle, replication, config reload, metrics, and auto-tuning
controllers should move behind explicit controller boundaries after lifecycle
contracts are stable.

The first controller work should be read-only status and shutdown ordering, not
behavior changes.
