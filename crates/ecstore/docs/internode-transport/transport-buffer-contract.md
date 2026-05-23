# Internode Transport Buffer Contract

Status: design note only. This document defines a backend-neutral buffer
ownership and lifecycle contract for the `InternodeDataTransport` adapter. It
does not implement a new backend and does not change production behavior.

## Open-source Scope

The open-source RustFS path keeps `tcp-http` as the default internode data
transport. This document defines adapter contracts only:

- no additional production backend is introduced;
- no dependency is added;
- no new accepted production backend value is added;
- RustFS core data-plane logic remains independent of the concrete transport
  implementation.

## Current Adapter Surface

The current data-plane surface is byte-stream based:

| Current path | Current API shape | Current ownership |
| --- | --- | --- |
| Remote read stream | `InternodeDataTransport::open_read(...) -> FileReader` | Backend returns boxed `AsyncRead`; callers provide temporary `ReadBuf` storage per poll. |
| Remote write stream | `InternodeDataTransport::open_write(...) -> FileWriter` | Callers pass borrowed `&[u8]` slices into boxed `AsyncWrite`; the backend owns any async body staging. |
| Walk-dir stream | `InternodeDataTransport::open_walk_dir(...) -> FileReader` | Same boxed stream model as read, with a small serialized request body. |

This API is correct for the current TCP/HTTP backend. The adapter contract
describes current ownership boundaries without assuming implementation details
outside `TcpHttpInternodeDataTransport`.

## Buffer Ownership Model

| Buffer role | Allocator | Lifetime owner | Transport state | TCP/HTTP behavior |
| --- | --- | --- | --- | --- |
| Send buffer | Caller or RustFS-owned pool | Caller until the writer copies or accepts bytes for the HTTP body | Existing HTTP body staging | Copy into the existing `AsyncWrite` path when the writer cannot use the borrowed slice directly. |
| Receive buffer | Caller-provided storage | Reader while filling; caller after `poll_read` returns | Existing HTTP response body chunks | Copy from `AsyncRead` into caller storage as today. |
| Control metadata | RustFS caller | Caller/request object | Not buffer-managed by the data-plane backend | Serialize into HTTP/gRPC/control-plane messages. |
| Fallback staging | TCP/HTTP backend | TCP/HTTP backend | Existing `HttpReader`/`HttpWriter` buffers | Existing buffering semantics. |

The current writer must not retain borrowed caller slices beyond the write call.
When bytes must outlive the call, they are copied into owned HTTP body chunks.

This contract does not claim zero-copy behavior. The current TCP/HTTP path
documents where copies occur.

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

## Current Adapter Contract

| Area | Required contract |
| --- | --- |
| Ownership | Define when caller-owned bytes are copied or accepted by the transport. |
| Completion | Return from stream operations only when bytes are accepted or an error is reported. |
| Staging | Keep staging behavior inside the TCP/HTTP implementation. |
| Size limits | Report any RustFS-visible `max_transfer_size`; TCP/HTTP currently reports none. |
| Ordering | Preserve ordered byte-stream semantics. |
| Copy accounting | Document known copy boundaries and avoid unmeasured zero-copy claims. |

## Current API Limitations

| Current API | Current limitation |
| --- | --- |
| `FileReader = Box<dyn AsyncRead + Send + Sync + Unpin>` | `AsyncRead` exposes temporary caller `ReadBuf` storage. |
| `FileWriter = Box<dyn AsyncWrite + Send + Sync + Unpin>` | `AsyncWrite::poll_write` receives borrowed `&[u8]` that cannot outlive the poll. |
| `HttpWriter` | The async HTTP body must own `Bytes`, so borrowed write buffers are copied into `BytesMut` or `Bytes`. |
| `write_body_chunks_to_writer` | Server-side HTTP body chunks are copied into `BytesMut` before local disk write. |
| Erasure encode output | Encoded shards are represented as `Vec<Bytes>` and written through `AsyncWrite`. |
| Erasure decode input | Shard reads allocate `Vec<u8>` buffers before decode. |

These limitations do not block the current `tcp-http` backend.

## Adapter Stability

`InternodeDataTransport` should keep RustFS core data-plane logic separate from
the concrete transport implementation. The trait and `tcp-http` backend remain
inside `ecstore`.

This PR does not perform a crate split, add runtime loading, introduce a plugin
system, add a backend value, or implement a new transport backend.
