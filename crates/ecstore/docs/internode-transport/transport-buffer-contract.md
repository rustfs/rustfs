# Internode Transport Buffer Contract

Status: design note only. This document defines a backend-neutral buffer
ownership and lifecycle contract for the `InternodeDataTransport` adapter. It
does not implement a new backend and does not change production behavior.

## Open-source Scope

The open-source RustFS path keeps `tcp-http` as the default internode data
transport. This document defines adapter contracts only:

- no production RDMA, DPU, DOCA, BlueField, DPDK, SPDK, or hardware
  acceleration backend is introduced;
- no hardware SDK, `libibverbs`, `rdma-core`, or vendor dependency is added;
- no new accepted production backend value is added;
- future external or separately maintained backends may implement the same
  adapter boundary without changing RustFS core data-plane logic.

Examples of possible future external backends include DOCA/BlueField,
RDMA/RoCE, or other DPU/NIC implementations. These are examples only and are
not implemented or scheduled by this design.

## Current Adapter Surface

The current data-plane surface is byte-stream based:

| Current path | Current API shape | Current ownership |
| --- | --- | --- |
| Remote read stream | `InternodeDataTransport::open_read(...) -> FileReader` | Backend returns boxed `AsyncRead`; callers provide temporary `ReadBuf` storage per poll. |
| Remote write stream | `InternodeDataTransport::open_write(...) -> FileWriter` | Callers pass borrowed `&[u8]` slices into boxed `AsyncWrite`; the backend owns any async body staging. |
| Walk-dir stream | `InternodeDataTransport::open_walk_dir(...) -> FileReader` | Same boxed stream model as read, with a small serialized request body. |

This API is correct for the current TCP/HTTP backend. A future non-default
backend may have stricter memory ownership, buffer lifetime, or completion
requirements, so the adapter contract needs to describe those boundaries
without assuming a specific implementation.

## Buffer Ownership Model

| Buffer role | Allocator | Lifetime owner | Backend-specific state | TCP/HTTP behavior |
| --- | --- | --- | --- | --- |
| Send buffer | Caller or RustFS-owned pool | Caller until the backend accepts the buffer; backend until completion if an owned-buffer API is used | Optional backend-managed buffer, staging buffer, or registration handle | Copy into the existing `AsyncWrite` path when the backend cannot use the buffer directly. |
| Receive buffer | Caller-provided storage or backend-owned receive pool | Backend while filling; caller after completion if ownership is returned | Optional backend-owned receive buffer or backend-specific registration | Copy from `AsyncRead` into caller storage as today. |
| Control metadata | RustFS caller | Caller/request object | Not buffer-managed by the data-plane backend | Serialize into HTTP/gRPC/control-plane messages. |
| Fallback staging | TCP/HTTP backend | TCP/HTTP backend | No backend-specific registration | Existing `HttpReader`/`HttpWriter` buffering semantics. |

Backends with stricter memory requirements must not let callers mutate or reuse
a buffer while an async transfer is still in flight. A backend that cannot use a
caller buffer directly must either reject the transfer before payload movement
or copy through a clearly documented backend-owned staging buffer.

Zero-copy is not guaranteed by this contract. Backends must document whether
their path is zero-copy, copy-reduced, or staging-buffer based, and must
document where copies occur.

## Compatibility Contract

The current stream API remains the OSS compatibility contract:

```rust
#[async_trait::async_trait]
pub trait InternodeDataTransport {
    async fn open_read(&self, request: ReadStreamRequest) -> Result<FileReader>;
    async fn open_write(&self, request: WriteStreamRequest) -> Result<FileWriter>;
    async fn open_walk_dir(&self, request: WalkDirStreamRequest) -> Result<FileReader>;
}
```

Future extensions for backend-managed buffers should be additive and
capability-gated. A possible shape is:

```rust
pub struct TransferBuffer {
    pub bytes: bytes::Bytes,
    pub backend_state: Option<TransportBufferState>,
}

pub struct CompletedTransfer {
    pub bytes: bytes::Bytes,
    pub transfer_len: usize,
}

#[async_trait::async_trait]
pub trait BackendBufferInternodeTransport: InternodeDataTransport {
    async fn write_backend_buffer(&self, request: WriteStreamRequest, buffer: TransferBuffer) -> Result<CompletedTransfer>;
    async fn read_backend_buffer(&self, request: ReadStreamRequest, buffer: TransferBuffer) -> Result<CompletedTransfer>;
}
```

The concrete type of `TransportBufferState` should stay backend-private. The
generic contract only needs to state whether the buffer is usable by the
backend, who owns it during transfer, and when ownership returns.

This PR does not add this extension. It documents the boundary that a future
external backend may need.

## Required Contract for Stricter Backends

| Area | Required contract |
| --- | --- |
| Ownership | Define when caller-owned bytes become backend-owned and when ownership returns. |
| Completion | Signal completion before RustFS can reuse or mutate backend-managed memory. |
| Staging | Declare whether the backend copies through backend-owned staging buffers when direct use is unavailable. |
| Size limits | Expose any RustFS-visible `max_transfer_size`. |
| Ordering | Either provide ordered delivery or include a reassembly layer before exposing stream semantics. |
| Copy accounting | Document every known copy boundary and avoid claiming zero-copy unless the path proves it. |

## Optional Optimizations

| Area | Optional behavior |
| --- | --- |
| Buffer pooling | RustFS may keep reusable pools for send and receive buffers. |
| Backend state cache | A backend may cache backend-specific handles for long-lived buffers. |
| Scatter/gather | A backend may accept multiple shard slices without repacking when its completion model can report per-slice ownership safely. |
| Backend-owned receive | A backend may return owned receive chunks instead of filling caller-provided buffers. |

These are optional optimizations, not requirements for the OSS TCP/HTTP path.

## Current API Limitations

| Current API | Limitation for stricter backends |
| --- | --- |
| `FileReader = Box<dyn AsyncRead + Send + Sync + Unpin>` | `AsyncRead` exposes temporary caller `ReadBuf` storage, not a stable backend-managed receive buffer or explicit completion token. |
| `FileWriter = Box<dyn AsyncWrite + Send + Sync + Unpin>` | `AsyncWrite::poll_write` receives borrowed `&[u8]` that cannot outlive the poll, so async direct transfer requires copying or a different ownership API. |
| `HttpWriter` | The async HTTP body must own `Bytes`, so borrowed write buffers are copied into `BytesMut` or `Bytes`. |
| `write_body_chunks_to_writer` | Server-side HTTP body chunks are copied into `BytesMut` before local disk write. |
| Erasure encode output | Encoded shards are represented as `Vec<Bytes>` and written through `AsyncWrite`, not a completion-aware backend-buffer API. |
| Erasure decode input | Shard reads allocate `Vec<u8>` buffers before decode; no backend-owned receive pool is visible at the transport boundary. |

These limitations do not block the current `tcp-http` backend. They describe
where a future external backend would need staging or an additive API.

## External Backend Crate Compatibility

`InternodeDataTransport` should remain implementable by future backends without
modifying RustFS core data-plane logic. In the short term, the trait and
`tcp-http` backend may remain inside `ecstore`.

A future external or separately maintained backend could live in a separate
crate if the trait, request/response types, capability report, and error model
are public and stable enough. This PR does not perform a crate split, add
runtime loading, or introduce a plugin system.
