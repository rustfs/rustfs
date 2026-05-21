# Internode Buffer Contract and Backend Negotiation

Status: design note only. This document defines future buffer ownership,
lifetime, fallback, and backend selection rules for internode data-plane
backends. It does not implement RDMA and does not change production behavior.

## Scope

The current data-plane surface is byte-stream based:

| Current path | Current API shape | Current ownership |
| --- | --- | --- |
| Remote read stream | `InternodeDataTransport::open_read(...) -> FileReader` | Backend returns boxed `AsyncRead`; callers provide temporary `ReadBuf` storage per poll. |
| Remote write stream | `InternodeDataTransport::open_write(...) -> FileWriter` | Callers pass borrowed `&[u8]` slices into boxed `AsyncWrite`; the backend owns any async body staging. |
| Walk-dir stream | `InternodeDataTransport::open_walk_dir(...) -> FileReader` | Same boxed stream model as read, with a small serialized request body. |

That API is correct for TCP/HTTP. A registered-memory backend needs a stricter
contract because buffer ownership and transfer completion become part of the
correctness boundary.

## Buffer Ownership Model

| Buffer role | Allocator | Lifetime owner | Registration owner | TCP behavior |
| --- | --- | --- | --- | --- |
| Send buffer | Caller or RustFS-owned pool | Caller until backend accepts the buffer; backend until completion if using an owned-buffer API | Backend or a transport-owned registration cache | Copy into the existing `AsyncWrite` path when the backend cannot use the buffer directly. |
| Receive buffer | Caller-provided pool or backend-owned receive pool | Backend while filling; caller after completion | Backend or receive-pool owner | Copy from `AsyncRead` into caller storage as today. |
| Control metadata | RustFS caller | Caller/request object | Not registered | Serialize into HTTP/gRPC/control-plane messages. |
| Fallback staging | TCP backend | TCP backend | Not registered | Existing `HttpReader`/`HttpWriter` buffering semantics. |

Registered buffers must not be mutated by the caller while an async transfer is
in flight. A backend that needs pinned memory must either reject non-registered
buffers before transfer starts or copy through fallback staging.

## Possible Trait Shape

The current stream API remains the compatibility contract:

```rust
#[async_trait::async_trait]
pub trait InternodeDataTransport {
    async fn open_read(&self, request: ReadStreamRequest) -> Result<FileReader>;
    async fn open_write(&self, request: WriteStreamRequest) -> Result<FileWriter>;
    async fn open_walk_dir(&self, request: WalkDirStreamRequest) -> Result<FileReader>;
}
```

A future registered-buffer extension should be additive and capability-gated:

```rust
pub struct TransferBuffer {
    pub bytes: bytes::Bytes,
    pub registration: Option<TransportRegistration>,
}

pub struct CompletedTransfer {
    pub bytes: bytes::Bytes,
    pub transfer_len: usize,
}

#[async_trait::async_trait]
pub trait RegisteredBufferInternodeTransport: InternodeDataTransport {
    async fn write_registered(&self, request: WriteStreamRequest, buffer: TransferBuffer) -> Result<CompletedTransfer>;
    async fn read_registered(&self, request: ReadStreamRequest, buffer: TransferBuffer) -> Result<CompletedTransfer>;
}
```

The concrete type of `TransportRegistration` should stay backend-private. The
generic contract only needs to state whether the buffer is registered and who
owns it until completion.

## API Impact Classification

Required for registered-memory backends:

| Area | Required contract |
| --- | --- |
| Ownership | A transfer API must define when caller-owned bytes become backend-owned and when ownership returns. |
| Completion | The backend must signal completion before RustFS can reuse or mutate registered memory. |
| Fallback | A backend must declare whether it can copy through TCP-compatible staging when direct registration is unavailable. |
| Size limits | The backend must expose any RustFS-visible `max_transfer_size`. |
| Ordering | The backend must either provide ordered delivery or include a reassembly layer before exposing stream semantics. |

Optional optimizations:

| Area | Optional behavior |
| --- | --- |
| Buffer pooling | RustFS may keep reusable pools for registered send and receive buffers. |
| Registration cache | A backend may cache registration handles for long-lived buffers. |
| Scatter/gather | A backend may accept multiple shard slices without repacking when its completion model can report per-slice ownership safely. |
| Backend-owned receive | A backend may return owned receive chunks instead of filling caller-provided buffers. |

## Current API Blockers

| Current API | Blocker for registered memory |
| --- | --- |
| `FileReader = Box<dyn AsyncRead + Send + Sync + Unpin>` | `AsyncRead` exposes temporary caller `ReadBuf` storage, not registered receive memory or backend-owned completion. |
| `FileWriter = Box<dyn AsyncWrite + Send + Sync + Unpin>` | `AsyncWrite::poll_write` receives borrowed `&[u8]` that cannot outlive the poll, so async zero-copy transfer requires copying or a different ownership API. |
| `HttpWriter` | The async HTTP body must own `Bytes`, so borrowed write buffers are copied into `BytesMut` or `Bytes`. |
| `write_body_chunks_to_writer` | Server-side HTTP body chunks are copied into `BytesMut` before local disk write. |
| Erasure encode output | Encoded shards are represented as `Vec<Bytes>` and written through `AsyncWrite`, not a completion-aware registered-buffer API. |
| Erasure decode input | Shard reads allocate `Vec<u8>` buffers before decode; no registered receive pool is visible at the transport boundary. |

## Static Backend Selection

Static config is the first selection model. Existing accepted values remain:

| Config value | Meaning |
| --- | --- |
| unset | Use default TCP/HTTP backend. |
| `tcp-http` | Use default TCP/HTTP backend. |
| `tcp` | Alias for `tcp-http`. |
| any unsupported value | Fail closed with a diagnostic naming `RUSTFS_INTERNODE_DATA_TRANSPORT` and the invalid value. |

A future fallback policy must be explicit. Invalid backend names must continue
to fail closed unless a separate, intentionally configured fallback policy says
otherwise.

## Dynamic Negotiation Boundary

Dynamic negotiation, if added, belongs on the existing gRPC control plane. Data
transfer must start only after both peers agree on:

| Negotiated item | Required property |
| --- | --- |
| Backend name | Both peers know the backend and have it enabled. |
| Capability set | Required capabilities match the operation. |
| Max transfer size | The selected operation fits or is split before transfer starts. |
| Registration rules | Both peers agree whether registered memory is required. |
| Security mode | Authentication and encryption requirements are satisfied before out-of-band transfer. |
| Fallback policy | Both peers agree whether TCP fallback is allowed for this operation. |

Negotiation must not silently downgrade security or bypass existing disk
health, quorum, timeout, and integrity semantics.

## Failure Matrix

| Condition | Default behavior | Explicit fallback behavior | Observability |
| --- | --- | --- | --- |
| Unsupported configured backend | Fail closed during transport construction. | Fall back only when the fallback policy explicitly allows unsupported-backend fallback. | Error includes config key and invalid value. |
| Peer does not support selected backend | Fail before payload transfer. | Use TCP/HTTP only when both local policy and peer policy allow it. | Count peer mismatch and selected fallback backend. |
| Capability mismatch | Fail before payload transfer. | Use TCP/HTTP only if TCP satisfies the operation and policy allows fallback. | Record missing capability names. |
| Connection setup failure | Fail the operation. | Retry on TCP/HTTP when fallback is allowed and the operation has not transferred payload bytes. | Count setup failure, retry, and fallback result. |
| Partial transfer failure | Fail the operation and let existing object/quorum logic decide retry behavior. | Do not silently resume on another backend unless the transfer protocol can prove byte range, checksum, and idempotency boundaries. | Count partial failure with bytes completed. |
| Max transfer size exceeded | Fail before payload transfer or split at a higher layer. | Use TCP/HTTP if policy allows and TCP has no RustFS-level cap. | Record rejected size and selected backend. |
| Auth or encryption mismatch | Fail closed. | No fallback unless the fallback path satisfies the same or stronger security requirements. | Security failure metric and audit log entry. |

## Security and Metrics Requirements

Security requirements:

- Backend selection must preserve peer authentication.
- Fallback must not weaken encryption or authorization.
- Out-of-band data-plane transfers must still bind to the intended disk, volume,
  path, and request authority.
- Partial transfers must not bypass bitrot verification or erasure quorum
  handling.

Metrics requirements:

- selected backend
- requested backend
- fallback backend, when used
- operation name
- success/failure
- transferred bytes
- setup failure count
- partial transfer failure count
- capability mismatch count
- fallback decision count
