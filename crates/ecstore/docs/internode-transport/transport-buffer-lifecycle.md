# Internode Buffer Lifecycle and Copy Count

Status: P1-D analysis only. This document records the current TCP/HTTP
internode data path and the ownership boundaries that matter for the
backend-neutral `InternodeDataTransport` adapter. It does not implement a new
backend or change production behavior.

## Open-source Scope

The OSS scope is:

- define buffer ownership and copy-count behavior for the current
  `InternodeDataTransport` adapter;
- keep `tcp-http` as the default backend;
- keep existing TCP/HTTP behavior unchanged;
- document copy hotspots and ownership gaps for maintainable transport code;
- avoid hardware-specific dependencies or backend implementations.

The OSS scope is not:

- RDMA support;
- DPU support;
- DOCA support;
- BlueField support;
- RoCE/InfiniBand support;
- hardware benchmark planning;
- hardware-specific backend implementation.

## Scope

The covered paths are the large internode data-plane calls currently routed
through `InternodeDataTransport`:

| Path | Entry | Transport owner | Server owner |
| --- | --- | --- | --- |
| Read stream | `RemoteDisk::read_file_stream` in `crates/ecstore/src/rpc/remote_disk.rs` | `TcpHttpInternodeDataTransport::open_read` in `crates/ecstore/src/rpc/internode_data_transport.rs`, `HttpReader` in `crates/rio/src/http_reader.rs` | `handle_read_file` in `rustfs/src/storage/rpc/http_service.rs` |
| Write stream | `RemoteDisk::create_file`, `RemoteDisk::append_file` in `crates/ecstore/src/rpc/remote_disk.rs` | `TcpHttpInternodeDataTransport::open_write` in `crates/ecstore/src/rpc/internode_data_transport.rs`, `HttpWriter` in `crates/rio/src/http_reader.rs` | `handle_put_file` in `rustfs/src/storage/rpc/http_service.rs` |
| Walk dir stream | `RemoteDisk::walk_dir` in `crates/ecstore/src/rpc/remote_disk.rs` | `TcpHttpInternodeDataTransport::open_walk_dir`, `HttpReader` | `handle_walk_dir` in `rustfs/src/storage/rpc/http_service.rs` |

Object read/write/heal callers enter these streams through
`create_bitrot_reader` and `create_bitrot_writer` in
`crates/ecstore/src/bitrot.rs`. Erasure decode and encode then move data
through `ParallelReader` in `crates/ecstore/src/erasure_coding/decode.rs` and
`MultiWriter` in `crates/ecstore/src/erasure_coding/encode.rs`.

## Read Stream

| Step | Owner | Buffer type | Copy? | Reason |
| --- | --- | --- | --- | --- |
| Build request | `RemoteDisk::read_file_stream` | `String` fields in `ReadStreamRequest` | Yes | Volume, path, endpoint, and disk references are copied into an owned request before async transport dispatch. This is metadata, not payload. |
| Select transport | `TcpHttpInternodeDataTransport::open_read` | URL `String`, `HeaderMap` | Yes | URL and auth headers are HTTP control data. No object bytes are copied here. |
| Open local file on server | `handle_read_file`, `LocalDisk::read_file_stream` | `FileCacheReclaimReader` boxed as `FileReader` | No payload copy | The server owns an async file reader positioned at the requested offset. |
| File to HTTP body | `read_file_body_stream` | `ReaderStream<AsyncRead>` yielding `Bytes` | Yes | `ReaderStream::with_capacity` reads from the file into chunk buffers. This is the file-to-network buffer materialization point. |
| Length limiting | `rustfs_utils::net::bytes_stream` | `Bytes` | Usually no | `Bytes::truncate` adjusts the chunk view when the last chunk exceeds the requested length. It does not copy the retained prefix. |
| HTTP receive | `HttpReader::with_capacity_and_stall_timeout` | `reqwest::Response::bytes_stream()` yielding `Bytes` | Network stack dependent | The user-level object is `Bytes`; any kernel/TLS/hyper copy is below the current RustFS abstraction. |
| Stream to caller buffer | `HttpReader::poll_read` | `StreamReader<Stream<Item = Bytes>>`, caller `ReadBuf` | Yes | `StreamReader` exposes `AsyncRead`, so it copies bytes from each `Bytes` chunk into the caller-provided `ReadBuf`. |
| Bitrot verification | `BitrotReader::read` | caller `&mut [u8]`, `hash_buf: Vec<u8>` | No additional payload copy | The bitrot reader reads hash bytes into `hash_buf` and payload bytes directly into the supplied output slice. Hash calculation reads the slice. |
| Erasure shard read | `ParallelReader::read` | `Vec<u8>` per shard | Yes | Each shard read allocates `vec![0u8; shard_size]`; data is filled there before decode/reconstruction. |
| Object response write | `write_data_blocks` | slices of shard `Vec<u8>` | No extra staging copy | Decoded data block slices are written to the target writer with `write_all`; the target may copy internally. |
| Remote zero-copy helper | `RemoteDisk::read_file_zero_copy` | `Vec<u8>` then `Bytes` | Yes | The remote implementation reads the full stream into a `Vec` and converts it into `Bytes`. It is a convenience fallback, not network zero-copy. |

## Write Stream

| Step | Owner | Buffer type | Copy? | Reason |
| --- | --- | --- | --- | --- |
| Build writer request | `RemoteDisk::create_file`, `RemoteDisk::append_file` | `String` fields in `WriteStreamRequest` | Yes | Volume, path, endpoint, and disk references are copied into an owned request. This is metadata. |
| Select transport | `TcpHttpInternodeDataTransport::open_write` | URL `String`, `HeaderMap` | Yes | URL and auth headers are HTTP control data. No object bytes are copied here. |
| Erasure encode input | `Erasure::encode` in `encode.rs` | reusable `Vec<u8>` sized to `block_size` | Yes | `rustfs_utils::read_full` fills a block buffer from the source reader before encoding. |
| Erasure encode output | `Erasure::encode_data` caller in `encode.rs` | `Vec<Bytes>` per encoded block | Yes | Encoding creates shard `Bytes` for data and parity blocks before queueing them to writers. |
| Multi-writer fanout | `MultiWriter::write` | borrowed `Bytes` shards | No additional fanout copy | The writer fanout passes borrowed `Bytes` references to each `BitrotWriterWrapper`. |
| Bitrot write | `BitrotWriter::write` | shard `&[u8]`, checksum bytes | Yes for checksum bytes | The payload slice is passed to the inner writer, while checksum bytes are generated and written before payload when enabled. |
| Client HTTP writer buffer | `HttpWriter::poll_write` and `poll_write_vectored` | `BytesMut` pending chunk or `Bytes::copy_from_slice` | Yes | Small writes are coalesced with `BytesMut::extend_from_slice`; large single writes still copy into an owned `Bytes` because the async body must outlive the caller's borrowed buffer. |
| Client channel to reqwest | `HttpWriter::poll_send_pending_chunk`, `ReceiverStream` | `Bytes` | No | `BytesMut::split().freeze()` transfers owned chunk storage to `Bytes`; the mpsc channel and stream move the `Bytes` handle. |
| HTTP receive body on server | `handle_put_file` | `Incoming::into_data_stream()` yielding `Bytes` | Network stack dependent | The server receives owned `Bytes` chunks from hyper. |
| Server body coalescing | `write_body_chunks_to_writer` | `BytesMut` sized to `DEFAULT_READ_BUFFER_SIZE` | Yes | Each incoming `Bytes` chunk is copied into `pending` before writing to the local file writer. This normalizes chunk size but adds a full payload copy. |
| Local file write | `LocalDisk::create_file`, `LocalDisk::append_file`, `FileCacheReclaimWriter` | `&[u8]` into `tokio::fs::File` | Kernel dependent | RustFS passes slices to Tokio file writes. Kernel page-cache copies are below the RustFS abstraction. |

## Request and Serialization Boundaries

| Boundary | Owner | Buffer type | Copy? | Notes |
| --- | --- | --- | --- | --- |
| Read/write query parameters | `build_read_file_stream_url`, `build_put_file_stream_url` | URL-encoded `String` | Yes | Metadata only. It includes disk, volume, path, offset, length, append, and size. |
| Auth headers | `build_auth_headers` callers | `HeaderMap` | Yes | Metadata only. This is currently tied to HTTP request construction. |
| Walk dir request | `RemoteDisk::walk_dir`, `open_walk_dir`, `handle_walk_dir` | JSON `Vec<u8>` body, collected `Bytes` on server | Yes | Walk dir is a streamed response but its request body is serialized JSON control data. |
| gRPC read/write-all | `RemoteDisk::read_all`, `RemoteDisk::write_all`, `NodeService::{handle_read_all,handle_write_all}` | Prost `Bytes`/message bodies | Yes | These paths are still gRPC byte paths, not `InternodeDataTransport`; they matter for metrics and inventory but are outside this P1-D stream-copy count. |

## Hotspots

| Rank | Hotspot | Impact | Reason |
| --- | --- | --- | --- |
| 1 | `HttpWriter::poll_write` and `poll_write_vectored` | High on write path | Every borrowed caller buffer is copied into owned `BytesMut` or `Bytes` before it can be sent by an async HTTP body. |
| 2 | `write_body_chunks_to_writer` | High on write path | The server copies every received `Bytes` chunk into a coalescing `BytesMut` before local disk write. |
| 3 | `ParallelReader::read` shard buffers | High on read path | Each shard read allocates and fills a `Vec<u8>` before decode can proceed. This is also where degraded reads wait on quorum. |
| 4 | `ReaderStream::with_capacity` plus `StreamReader` | Medium on read path | Server file reads create `Bytes` chunks, then client `AsyncRead` copies those chunks into the caller's `ReadBuf`. |
| 5 | `Erasure::encode` block and shard materialization | Medium on write path | Source data is first read into a block `Vec<u8>`, then encoded into per-shard `Bytes`. This is necessary for the current erasure API. |
| 6 | `RemoteDisk::read_file_zero_copy` | Medium when used | Remote zero-copy reads buffer the whole stream into memory. The name does not mean zero-copy over the network. |
| 7 | URL/query/header/JSON serialization | Low | Metadata copies are small and not on the large payload hot path. |

## Adapter Ownership Gaps

1. `FileReader` and `FileWriter` are boxed `AsyncRead`/`AsyncWrite` trait
   objects. They expose borrowed buffers per poll, not stable backend-owned
   regions, transfer handles, or explicit completion ownership.
2. `InternodeDataTransport` currently returns stream traits only. Its
   capabilities advertise that TCP/HTTP does not require backend-specific
   buffer registration and is not a zero-copy candidate, but there is no
   backend API to pass stable backend-managed buffers.
3. `HttpWriter` must own outgoing chunks because the async request body outlives
   the caller's borrowed `&[u8]`. A lower-copy backend would need a different
   lifetime contract or an owned buffer pool.
4. Server write handling normalizes all incoming body chunks into a new
   `BytesMut`. Avoiding that copy would require passing incoming `Bytes` or
   backend-owned receive buffers directly into the disk/bitrot writer contract.
5. Erasure decode owns shard `Vec<u8>` buffers and write-back happens through
   `AsyncWrite`. A lower-copy backend would need explicit ownership of shard
   buffers across decode, reconstruction, and network completion.
6. Erasure encode materializes `Vec<Bytes>` blocks before fanout. A
   backend that can send multiple stable slices would need an encode output
   representation that can be transferred without repacking.
7. The HTTP auth and URL construction boundary is part of the current TCP/HTTP
   backend. A non-HTTP backend would need equivalent peer authentication and
   disk addressing without assuming URL query parameters.
8. Local disk zero-copy exists only for local reads via `read_file_zero_copy`.
   Remote disks deliberately fall back to network streaming and full-buffer
   collection for the zero-copy helper.
