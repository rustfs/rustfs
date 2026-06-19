# Runtime Capability Contracts

This document records the `rustfs/backlog#660` PR-08 and PR-09 contract slice.
It adds read-only observability and topology snapshot shapes to
`rustfs-storage-api` without coupling the contract crate to runtime, ECStore,
admin routes, profiling, or observability implementation crates.

## Observability Snapshot Contract

`ObservabilitySnapshot` records:

- Runtime telemetry capability state.
- Userspace CPU and memory profiling capability state.
- Process, system, and cgroup memory sampling state.
- Platform support for target triple, OS, architecture, allocator, eBPF, and
  NUMA capability.

Unsupported, disabled, and unknown states are represented by `CapabilityState`
instead of failing snapshot construction. The contract is intentionally read
only and does not replace existing profiling routes, telemetry APIs, exporter
pipelines, or startup behavior.

## Topology Snapshot Contract

`TopologySnapshot` records:

- Pool, set, and disk identity indexes plus optional stable IDs.
- Optional zone, rack, node, media, NUMA, and additional labels.
- Topology-wide profiling, NUMA, failure-domain label, and media-label
  capability states.
- Per-disk media, failure-domain, NUMA, and profiling capability states.

Missing labels are represented as absent `Option` values. Extra topology labels
belong in the `additional` label map, so future inventory labels do not require
ECStore type leakage.

## Boundary Rules

- No `rustfs-ecstore`, `rustfs-obs`, Axum, KMS, admin route, OTEL, eBPF, or
  profiling implementation dependency is added to `rustfs-storage-api`.
- No placement, membership, NUMA pinning, profiling, startup, admin route, or
  exporter behavior changes are part of this contract slice.
- Providers must map implementation failures into `CapabilitySnapshotError`
  before crossing the contract boundary.
