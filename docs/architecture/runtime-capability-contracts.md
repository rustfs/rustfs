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

## RustFS Provider Slice

`rustfs/src/runtime_capabilities.rs` wires the contracts to RustFS runtime
owners through read-only providers:

- `RustFsObservabilitySnapshotProvider` maps current dial9, profiling,
  memory-sampling, platform, allocator, eBPF, and NUMA capability state without
  starting telemetry, profiling, allocator reclaim, or memory-observability
  workers.
- `EndpointTopologySnapshotProvider` maps `EndpointServerPools` into pool, set,
  and disk topology snapshots without changing endpoint construction,
  placement, readiness, locks, or ECStore metadata. Local file endpoint paths are
  intentionally not used as disk IDs or labels.

Unsupported or unavailable runtime capabilities are reported as `unsupported`
or `unknown` contract states instead of activating fallback behavior.

## Storage-Class Write Contract

Authenticated clients discover the storage-class write contract from
`GET /rustfs/admin/v4/runtime/capabilities`. The additive
`storage_classes` object is versioned independently from the route:

```json
{
  "storage_classes": {
    "contract_version": 1,
    "supported_write_classes": ["STANDARD", "REDUCED_REDUNDANCY"],
    "unsupported_write_error": "InvalidStorageClass",
    "legacy_label_behavior": "normalized_to_effective_class"
  }
}
```

`supported_write_classes` is the complete client-selectable write allowlist.
Any other value fails before object or multipart mutation with the stable S3
error named by `unsupported_write_error`. `legacy_label_behavior` means
non-transitioned historical label-only metadata is reported as its effective
local class; actual lifecycle transition tier names remain unchanged.

The values are sourced from
[`crates/ecstore/src/config/storageclass.rs`](../../crates/ecstore/src/config/storageclass.rs),
which also owns write validation and response normalization. Consumers must
branch on `contract_version` before assigning meaning to future fields. The
admin route continues to require `ServerInfoAdminAction`; capability discovery
does not weaken authentication or authorization.
