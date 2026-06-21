# Storage, Control Plane, And Background Controllers

This document defines migration boundaries for the storage hot path and adjacent
control-plane responsibilities.

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
status projections. Peer health remains explicitly unknown until a later slice
wires real health signals; this document does not authorize background probes or
RPC-based health checks.

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
