# Internode Transport Capabilities

Status: design note for backend-neutral capability reporting. This document
does not add a backend and does not require specialized hardware or transport
crates.

## Open-source Scope

The OSS scope is:

- define honest capability reporting for the `InternodeDataTransport` adapter;
- keep `tcp-http` as the default backend;
- keep existing TCP/HTTP behavior unchanged;
- document the capability fields needed for maintainable transport code;
- avoid hardware-specific dependencies or backend implementations.

The OSS scope is not:

- RDMA support;
- DPU support;
- DOCA support;
- BlueField support;
- RoCE/InfiniBand support;
- hardware benchmark planning;
- hardware-specific backend implementation.

## Purpose

`InternodeDataTransportCapabilities` describes what a backend can honestly do
for RustFS internode data-plane transfers. The fields are intentionally neutral:
they can describe the current TCP/HTTP backend and a future non-default backend
without naming a specific transport implementation.

The capability report is descriptive. It does not select a backend, negotiate
with peers, or weaken object correctness semantics.

## Capability Fields

| Field | Meaning |
| --- | --- |
| `streaming_read` | The backend can open a remote disk reader for `read_file_stream`. |
| `streaming_write` | The backend can open a remote disk writer for `create_file` or `append_file`. |
| `streaming_walk_dir` | The backend can stream `walk_dir` responses. |
| `zero_copy_candidate` | The backend has an API shape that could avoid an extra user-space payload copy. This is not a promise that every transfer is zero-copy. |
| `registered_memory_required` | The backend requires pinned, registered, staged, or otherwise backend-managed buffers before payload transfer. |
| `ordered_delivery` | Bytes for each opened transfer are delivered in order. |
| `max_transfer_size` | Optional RustFS-level cap for a single transfer. `None` means no additional cap beyond the backend/protocol/runtime limits. |
| `fallback_supported` | The backend can participate in the behavior-preserving TCP fallback path. |

## TCP/HTTP Backend

The default TCP/HTTP backend reports only capabilities it actually provides:

| Field | TCP/HTTP value | Reason |
| --- | --- | --- |
| `streaming_read` | `true` | `HttpReader` streams `/rustfs/rpc/read_file_stream` responses. |
| `streaming_write` | `true` | `HttpWriter` streams `/rustfs/rpc/put_file_stream` request bodies. |
| `streaming_walk_dir` | `true` | `HttpReader` streams `/rustfs/rpc/walk_dir` responses. |
| `zero_copy_candidate` | `false` | The current path exposes `AsyncRead`/`AsyncWrite` and HTTP body chunks; it copies through normal user-space buffers. |
| `registered_memory_required` | `false` | TCP/HTTP does not require RustFS-managed pinned, registered, or backend-owned buffers. |
| `ordered_delivery` | `true` | Each HTTP request body or response body is consumed as an ordered byte stream. |
| `max_transfer_size` | `None` | RustFS does not impose an extra per-transfer cap at the capability layer. |
| `fallback_supported` | `true` | TCP/HTTP is the behavior-preserving default and fallback path. |

## Non-default Backend Fit

A future non-default backend can be described without changing the meaning of
the existing TCP report:

| Capability shape | Interpretation |
| --- | --- |
| `zero_copy_candidate=true`, `registered_memory_required=true` | The backend can only use its lower-copy path with buffers that satisfy backend-specific ownership or registration rules. |
| `zero_copy_candidate=true`, `registered_memory_required=false` | The backend may expose owned chunks or another lower-copy path without requiring caller-managed registration. |
| `max_transfer_size=Some(n)` | The backend has a RustFS-visible transfer size ceiling and callers must split larger transfers or use fallback behavior. |
| `ordered_delivery=false` | The backend cannot be used behind the current stream API without an ordering or reassembly layer. |

Unsupported or mismatched capabilities must not silently change quorum,
integrity verification, retry, or timeout semantics.
