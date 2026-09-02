# Runtime Capability Contracts

**Use this when:** changing the read-only observability or topology snapshot contracts in `rustfs-storage-api`, their RustFS providers, or the `storage_classes` payload of `GET /rustfs/admin/v4/runtime/capabilities`.
**Source of truth:** `ObservabilitySnapshot` in `crates/storage-api/src/observability.rs`; `TopologySnapshot` in `crates/storage-api/src/topology.rs`; `CapabilityState` and `CapabilitySnapshotError` in `crates/storage-api/src/capability.rs`; providers in `rustfs/src/runtime_capabilities.rs`; storage-class constants in `crates/ecstore/src/config/storageclass.rs`.

## Snapshot Contracts

Field lists live on the defining types and are not repeated here.

| Contract | Defining type | RustFS provider | Rule |
|---|---|---|---|
| Observability | `ObservabilitySnapshot` | `RustFsObservabilitySnapshotProvider` | Reports runtime telemetry, profiling, memory-sampling, platform, allocator, eBPF, and NUMA capability as `CapabilityState` values without starting telemetry, profiling, allocator reclaim, or memory-observability workers. |
| Topology | `TopologySnapshot` | `EndpointTopologySnapshotProvider` | Maps `EndpointServerPools` into pool/set/disk indexes, optional stable IDs, and optional zone/rack/node/media/NUMA labels without changing endpoint construction, placement, readiness, locks, or ECStore metadata. Local file endpoint paths are never used as disk IDs or labels; extra labels go in the `additional` map so future inventory labels need no ECStore type leakage. |

Unsupported, disabled, and unknown states are values of `CapabilityState`, not construction failures. Missing labels are `None`. Providers map implementation failures into `CapabilitySnapshotError` before crossing the contract boundary. Neither contract replaces existing profiling routes, telemetry APIs, exporter pipelines, or startup behavior.

## Boundary Rules

- `rustfs-storage-api` gains no dependency on `rustfs-ecstore`, `rustfs-obs`, Axum, KMS, admin routes, OTEL, eBPF, or profiling implementation crates.
- Providers are read-only. Adding or changing a provider changes no placement, membership, NUMA pinning, profiling, startup, admin-route, or exporter behavior.
- Unsupported or unavailable runtime capabilities are reported as `unsupported` or `unknown`; they never activate fallback behavior.

## Storage-Class Write Contract

Authenticated clients discover the storage-class write contract from `GET /rustfs/admin/v4/runtime/capabilities`. The additive `storage_classes` object is versioned independently from the route:

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

| Field | Meaning |
|---|---|
| `supported_write_classes` | The complete client-selectable write allowlist. Any other value fails before object or multipart mutation with the S3 error named by `unsupported_write_error`. |
| `unsupported_write_error` | Stable S3 error code (`UNSUPPORTED_WRITE_ERROR` in `crates/ecstore/src/config/storageclass.rs`). |
| `legacy_label_behavior` | Non-transitioned historical label-only metadata is reported as its effective local class; lifecycle transition tier names are unchanged. |
| `contract_version` | Consumers must branch on it before assigning meaning to future fields. |

Values, write validation, and response normalization are owned by `crates/ecstore/src/config/storageclass.rs`. The route continues to require `ServerInfoAdminAction`; capability discovery does not weaken authentication or authorization.
