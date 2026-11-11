# S3 over RDMA for LLM serving

**Use this when:** you change the RDMA or NIXL data path, the large-PUT path, or a per-bucket ETag setting, or you build a client that loads model weights or offloads KV cache to RustFS over RDMA.

This document describes the design and the invariants of RustFS's S3-over-RDMA data path. It does not repeat settings or the wire format. Those live in the operations guides:

- [nixl-s3-over-rdma.md](../operations/nixl-s3-over-rdma.md): NIXL build, settings, wire contract, device check, internode NIXL, large-PUT tuning, per-bucket ETag API.
- [cuobject-s3-over-rdma.md](../operations/cuobject-s3-over-rdma.md): the cuObject transport.

## Goals

- Move object bytes between RustFS and GPU or host memory with one-sided RDMA, while the request stays an ordinary signed S3 call. Authentication, authorization, range resolution, quota and erasure coding stay unchanged.
- Serve the two LLM-serving workloads at line rate:
  - **Model weight loading:** about 1.5 TB per replica, read many times.
  - **KV-cache offload:** many 10-60 MB objects written once per prefill and read back by any node.
- Run the whole data plane, including internode shard traffic, on GPU-side InfiniBand rails that have no IP addresses.

## Server components

| Layer | Code | Role |
|---|---|---|
| Client last hop, cuObject | `crates/cuobject/src/lib.rs`, `rustfs/src/app/object/cuobject.rs` | First transport: `x-amz-rdma-token`, NVIDIA cuObject server library through the `cuobject` bindings from crates.io. Kept as a compile-time alternative. |
| Client last hop, NIXL | `crates/nixl/src/lib.rs` (`RdmaToken::from_headers`, `ensure_remote`), `crates/nixl/src/mem.rs` | NIXL/UCX transport selected by the `nixl` feature. The client names a registered `dram` or `vram` buffer; the server WRITEs (GET) or READs (PUT) it. |
| Shared pipeline | `rustfs/src/app/object/cuobject.rs` | Segment fan-out of large GETs, staging chunks, shard arena, completion handling. The same pipeline serves both transports; only the transfer layer differs. |
| Device health | `crates/nixl/src/devices.rs` | Startup check that leaves out RDMA devices that are down, slow or flapping. |
| Internode, NIXL | `crates/ecstore/src/cluster/rpc/nixl_internode.rs`, `rustfs/src/storage/rpc/nixl_read.rs` | `RUSTFS_INTERNODE_DATA_TRANSPORT=nixl`: signed control request over HTTP, data over NIXL, for shard reads and writes. |
| Transport selection | `crates/ecstore/src/cluster/rpc/internode_data_transport.rs` | `tcp-http`, `tcp` or `nixl`. Internode RDMA runs over NIXL. |
| Erasure read path | `crates/ecstore/src/erasure/codec/shard_buf.rs`, `crates/ecstore/src/erasure/codec/shard_emit.rs`, `crates/ecstore/src/erasure/coding/bitrot.rs` | Two buffer generations per shard slot, zero-copy emit from registered shard buffers, bitrot verification off the decode task. |
| PUT path | `rustfs/src/app/object/put.rs`, `rustfs/src/app/multipart_usecase.rs`, `crates/rio/src/etag_reader.rs` (`with_md5_offload`), `crates/rio/src/hash_reader.rs` (`EtagAlgorithm`) | Zero-copy RDMA PutObject body, read-ahead, zero-copy UploadPart chunks, ETag MD5 on a blocking-pool thread, per-bucket XXH3 ETags. |
| Per-bucket ETag | `crates/ecstore/src/bucket/etag.rs` (`set_bucket_xxh3_etag`, `bucket_uses_xxh3_etag`), `rustfs/src/admin/handlers/etag.rs` | `etag.json` bucket-metadata entry; admin `GET/PUT/DELETE /rustfs/admin/v3/bucket-etag/{bucket}`. |
| Per-bucket durability | `crates/ecstore/src/bucket/durability.rs` | Existing `strict`, `relaxed` or `none` override. A KV-cache bucket is the intended user of a relaxed tier. |

## Design decisions

- **Signed S3 request, RDMA payload.** The client sends a normal signed S3 request whose body is empty. Three headers name its NIXL agent, its agent metadata and one registered buffer. The reply comes back only after the last transfer completed, so the client may use its buffer as soon as the response arrives. Clients sign with `UNSIGNED-PAYLOAD`, because the payload never crosses HTTP.
- **NIXL/UCX replaced cuObject as the default.** UCX addresses devices by GID, LID and queue-pair numbers carried in the agent metadata. That works on the GPU-side InfiniBand rails without IPoIB or a subnet manager, and on RoCE without `rdma_cm`. cuObject stays as a compile-time alternative.
- **GPU or host memory.** A `vram:<gpu>:<addr>:<len>` buffer works when the client's NIC can reach GPU memory (GPUDirect over dma-buf with the open NVIDIA driver; `nvidia_peermem` is not needed). The server does not need to know which kind it is writing into.
- **Ranges are first class.** A ranged GET lands at offset 0 of the named buffer. Clients split large objects into ranged requests spread over servers to raise parallelism.
- **Internode traffic on the same rails.** With `nixl` internode transport, every shard read and write between servers also moves over NIXL. The control request stays on the internode HTTP connection and binds the transfer with a signed metadata digest (and a one-time nonce for writes), so a replayed request cannot redirect a transfer.
- **Large PUTs are CPU-bound, not link-bound.** The fixes went to the CPU path: offloaded MD5, zero-copy bodies and allocator settings. The admission limit stays at 32 large PUTs per server; raising it to 64 or 128 gained nothing.
- **Per-bucket settings instead of process-wide switches.** XXH3 ETags and relaxed durability suit a KV-cache bucket and break assumptions of ordinary S3 clients, so both are bucket properties. A request that carries Content-MD5 always gets an MD5 ETag.

## Invariants

- `Agent::post_xfer_req` in `nixl-sys` returns `true` while a transfer is in progress. Treating `true` as done refills staging chunks under the NIC and corrupts data. Verify any transport change with one round into a zeroed buffer, because repeated rounds mask the bug.
- Every agent runs NIXL's progress thread, including agents that only receive RDMA writes; peers otherwise declare it dead.
- A client exports its agent metadata after registering every buffer it will name. The server reloads metadata when its hash changes, so a request that still carries an older copy can make the server swap back to it. Register all buffers before the first transfer.
- A registered buffer that an abandoned request named stays out of the pool longer than the peer's transfer timeout.
- Direct-I/O shard writers must be shut down at the end of every NIXL write segment (`finish_segment_file` in `rustfs/src/storage/rpc/nixl_read.rs`); otherwise the trailing sub-alignment bytes are lost.
- A multipart upload keeps the ETag algorithm its bucket had when the upload was created.

## Clients

Clients live outside this repository. Two LLM-serving clients were built against this path and shaped its requirements:

- **Model weight loading into GPU memory.** The loader plans a checkpoint into whole-tensor units, fetches each unit with several ranged NIXL GETs spread over the servers straight into GPU memory, and shares it with the other ranks over NCCL. No checkpoint byte touches disk or host memory, so ranged GETs into `vram` buffers and many concurrent requests per client are the hot path.
- **KV-cache offload.** A hierarchical-cache backend writes groups of KV pages (tens of MB each) once per prefill and reads them back from any node. It needs cheap existence checks, PUTs that return only after the bytes are durable enough for the bucket's durability setting, and ETags that do not cost an MD5 per object, hence per-bucket XXH3 ETags and relaxed durability.

## Open items

- **Stable per-model namespace in KV object names.** Clients should key KV objects by model ID, revision and KV dtype rather than a local path, and use a bucket per model.
- **Write admission for bursts.** Large-PUT admission stays at 32 per server; clients that offload KV in bursts should cap their own concurrent PUTs below it.
