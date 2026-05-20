# RFC: Pluggable Internode Data Transport

> Status: draft
> Last updated: 2026-05-19
> Scope: internode data-path analysis, benchmark baseline, and transport boundary

## Summary

RustFS does not currently include RDMA, RoCE, InfiniBand, DPU, BlueField/DOCA,
DPDK, SPDK, or SmartNIC offload support. The current distributed internode
paths use TCP-based HTTP/gRPC transports:

- `tonic` gRPC `NodeService` for most control, metadata, lock, health, and
  peer operations.
- HTTP streaming routes under `/rustfs/rpc/` for remote disk file streams.

RDMA/RoCE is still a plausible future optimization for large internode disk
data transfers, but it should not replace the whole internode RPC surface.
The correct first step is to isolate the data plane, establish a TCP baseline,
and introduce a pluggable transport boundary only around high-volume streams.

## Goals

- Document the current internode control plane and data plane.
- Identify the existing transfer paths that could benefit from a future
  high-throughput backend.
- Define the minimum benchmark baseline required before transport changes.
- Sketch a pluggable transport boundary that preserves the current TCP/HTTP
  behavior as the default backend.
- Reserve explicit boundaries for future RDMA/RoCE/InfiniBand work without
  committing RustFS to a specific vendor stack.

## Non-Goals

- Implement RDMA, RoCE, InfiniBand, DPU, DOCA, DPDK, SPDK, or SmartNIC support.
- Replace `tonic` gRPC for control-plane RPCs.
- Redesign erasure coding, quorum handling, disk health tracking, or object
  correctness semantics.
- Require RDMA-capable hardware for default development, CI, or ordinary
  RustFS deployments.

## Current Internode Architecture

### Server-side entry points

The main HTTP server builds a hybrid service per connection:

- `rustfs/src/server/http.rs` wires a `NodeServiceServer` for gRPC.
- `rustfs/src/storage/rpc/InternodeRpcService` intercepts HTTP paths under
  `/rustfs/rpc/`.
- Other HTTP/S3 traffic continues through the normal S3 service.

Compression logic already treats `/rustfs/rpc/` and `/rustfs/peer/` as internode
RPC paths and skips normal response compression for them.

### gRPC channel management

`crates/protos/src/lib.rs` creates internode gRPC channels with `tonic`
`Endpoint`:

- connect timeout
- TCP keepalive
- HTTP/2 keepalive interval and timeout
- request timeout
- optional TLS configuration
- global channel caching and failed-connection eviction

This confirms the current gRPC transport is TCP/HTTP2-based.

### NodeService layout

`crates/protos/src/node.proto` defines one `NodeService` that mixes several
classes of RPCs:

- meta service: bucket and metadata operations
- disk service: local/remote disk operations
- lock service: distributed lock operations
- peer rest service: node health, metrics, IAM/policy reload, rebalance,
  profiling, events, and admin-style operations

The service layout is practical today, but it is too broad to become an RDMA
surface. A future high-throughput transport should target only disk data
streams and keep this gRPC service as the control plane.

## Control Plane vs Data Plane

### Control plane

These paths carry coordination, metadata, health, and administrative state.
They should remain on gRPC/TCP:

| Area | Client/server code | Examples | Notes |
| --- | --- | --- | --- |
| Bucket peer ops | `crates/ecstore/src/rpc/peer_s3_client.rs`, `rustfs/src/storage/rpc/bucket.rs` | `MakeBucket`, `ListBucket`, `DeleteBucket`, `GetBucketInfo`, `HealBucket` | Small metadata/control payloads. |
| Locking | `crates/ecstore/src/rpc/remote_locker.rs`, `rustfs/src/storage/rpc/lock.rs` | `Lock`, `UnLock`, `Refresh`, batch lock/unlock | Latency-sensitive but not bulk data; correctness and timeout semantics matter more than transport bandwidth. |
| Peer/admin state | `crates/ecstore/src/rpc/peer_rest_client.rs`, `rustfs/src/storage/rpc/health.rs`, `metrics.rs`, `event.rs` | `LocalStorageInfo`, `ServerInfo`, `GetMetrics`, `GetLiveEvents`, reload APIs, rebalance APIs | Operational control plane. |
| Disk metadata/control | `crates/ecstore/src/rpc/remote_disk.rs`, `rustfs/src/storage/rpc/disk.rs` | `DiskInfo`, `ReadXL`, `ReadVersion`, `ReadMetadata`, `WriteMetadata`, `RenameFile`, `RenamePart`, `Delete*`, `VerifyFile`, `CheckParts` | Usually metadata, integrity checks, or namespace mutations. |
| Connection health | `RemoteDisk`, `RemotePeerS3Client`, `PeerRestClient` | TCP connectivity probes and fault/recovery state | Must remain available even if an optional data backend is unavailable. |

### Data plane candidates

These paths move object shard bytes or stream potentially large disk data and
are the only reasonable first candidates for a pluggable transport.

| Priority | Path | Current client | Current server | Current transport | Why it matters |
| --- | --- | --- | --- | --- | --- |
| P0 | `read_file_stream` | `RemoteDisk::read_file_stream` | `handle_read_file` in `http_service.rs` | HTTP `GET /rustfs/rpc/read_file_stream` with a streaming response body | Main remote disk read stream used by bitrot readers and erasure reads. |
| P0 | `put_file_stream` | `RemoteDisk::create_file` and `RemoteDisk::append_file` | `handle_put_file` in `http_service.rs` | HTTP `PUT /rustfs/rpc/put_file_stream` with a streaming request body | Main remote disk write stream used by bitrot writers and erasure writes. |
| P1 | `walk_dir` | `RemoteDisk::walk_dir` | `handle_walk_dir` in `http_service.rs` | HTTP `GET /rustfs/rpc/walk_dir` with a streamed metadata listing | Can be high-volume during scans/healing, but it is metadata-oriented rather than object byte data. |
| P1 | `ReadAll` / `WriteAll` | `RemoteDisk::read_all` / `write_all` | gRPC unary disk handlers | gRPC unary `bytes` payload | Moves bytes today, but should be measured before treating it as a high-throughput data path. |
| P2 | proto `WriteStream` / `ReadAt` | currently not used | currently returns unimplemented | gRPC streaming definitions exist but are not implemented | Possible future API shape, not a current production path. |

## Current Object Write Path

For object PUTs in distributed erasure mode, the relevant flow is:

1. Upper storage layers prepare object data and erasure metadata.
2. `SetDisks` selects local and remote disks.
3. `create_bitrot_writer` calls `disk.create_file(...)` for each shard writer.
4. For a remote disk, `RemoteDisk::create_file` returns an `HttpWriter`.
5. `HttpWriter` sends an HTTP `PUT` to `/rustfs/rpc/put_file_stream`.
6. The remote node's `handle_put_file` opens the local file writer and copies
   incoming body chunks into it.
7. `Erasure::encode` writes shards through `MultiWriter` to all selected
   writers while enforcing write quorum.

This is the primary write data-plane candidate.

## Current Object Read Path

For object GETs and repair reads in distributed erasure mode, the relevant flow is:

1. `SetDisks` prepares shard readers for the selected disks.
2. `create_bitrot_reader` uses local zero-copy only when `disk.is_local()`.
3. For a remote disk, it calls `disk.read_file_stream(...)`.
4. `RemoteDisk::read_file_stream` returns an `HttpReader`.
5. `HttpReader` sends an HTTP `GET` to `/rustfs/rpc/read_file_stream`.
6. The remote node's `handle_read_file` opens the local disk stream and returns
   it as an HTTP streaming body.
7. The erasure decoder reads from the shard streams and reconstructs the object.

This is the primary read data-plane candidate.

## Existing Metrics and Benchmark Surface

RustFS already has coarse internode metrics in `crates/common/src/internode_metrics.rs`:

- sent bytes
- received bytes
- outgoing requests
- incoming requests
- errors
- dial errors
- average dial time

These metrics are useful as a starting point, but they are not enough for a
transport RFC. A transport benchmark needs route-level and operation-level
measurements for at least:

- `read_file_stream`
- `put_file_stream`
- `walk_dir`
- gRPC `ReadAll` / `WriteAll`
- gRPC control-plane request volume

Existing benchmark assets:

- `scripts/run_object_batch_bench.sh`
- `scripts/run_object_batch_bench_enhanced.sh`
- `scripts/run_object_batch_bench_abc.sh`
- `scripts/run_four_node_cluster_failover_bench.sh`
- `scripts/run_internode_transport_baseline.sh` (scenario matrix wrapper for local vs distributed TCP baseline artifacts)
- Criterion benches under `crates/ecstore/benches/`

These mostly cover S3/object workload or erasure coding performance. They do
not yet isolate internode transport cost.

## Required TCP Baseline

Before adding a transport abstraction or any RDMA backend, collect a baseline
for the current TCP/HTTP/gRPC implementation.

### Topology

Minimum:

- 1-node local erasure deployment, to measure local disk and erasure overhead.
- 4-node distributed erasure deployment, to measure internode overhead.

Preferred:

- Same host count and disk layout for every run.
- Dedicated network interface or isolated VLAN.
- Fixed CPU governor and no unrelated background load.
- Recorded kernel version, NIC model, MTU, RustFS commit, Rust toolchain, and
  benchmark tool versions.

### Workloads

| Workload | Sizes | Concurrency | Main signal |
| --- | --- | --- | --- |
| S3 PUT | 4 KiB, 1 MiB, 16 MiB, 128 MiB, 1 GiB | 1, 16, 64, 128 | End-to-end write throughput and tail latency. |
| S3 GET | 4 KiB, 1 MiB, 16 MiB, 128 MiB, 1 GiB | 1, 16, 64, 128 | End-to-end read throughput and tail latency. |
| Remote disk stream read | shard-sized ranges from `read_file_stream` | 1, 16, 64 | Isolated internode read path. |
| Remote disk stream write | shard-sized writes through `put_file_stream` | 1, 16, 64 | Isolated internode write path. |
| Healing / repair | missing disk or missing shard scenario | controlled | Rebuild throughput and read/write amplification. |
| Scanner walk | large bucket/object namespace | controlled | Metadata streaming pressure, not primary RDMA target. |

### Measurements

Collect:

- throughput in bytes/s and objects/s
- p50, p95, p99, and max latency
- CPU utilization per process and per core
- memory RSS and allocation pressure where available
- `rustfs_system_network_internode_*` metrics
- TCP retransmits, socket errors, and NIC throughput
- disk throughput and utilization
- failure/retry/fallback counts

The baseline should produce a machine-readable artifact, for example
`target/bench/internode-transport/<timestamp>/summary.csv`, plus the exact
commands and configuration used.

### Baseline runner entry point

Use `scripts/run_internode_transport_baseline.sh` to execute a reproducible
S3 PUT/GET matrix against `local` and `distributed` scenarios and export:

- `summary.csv` (throughput/latency summary per workload and object size)
- `internode_metric_deltas.csv` (operation-level internode metric deltas when
  `--metrics-url` is provided)

## Transport Abstraction Proposal

### Design principle

Keep `NodeService` as the control plane. Introduce a separate data transport
only below `RemoteDisk`, where remote disk byte streams are opened today.

The first implementation should be a no-behavior-change TCP/HTTP backend that
wraps the current `HttpReader`, `HttpWriter`, and `/rustfs/rpc/*` handlers.
Only after that wrapper is benchmarked should an experimental RDMA/RoCE backend
be considered.

### Candidate boundary

The narrowest useful boundary is remote disk stream transfer:

```rust
#[async_trait::async_trait]
pub trait InternodeDataTransport: Send + Sync + std::fmt::Debug {
    async fn open_read(&self, request: ReadStreamRequest) -> Result<FileReader>;
    async fn open_write(&self, request: WriteStreamRequest) -> Result<FileWriter>;
    async fn walk_dir(&self, request: WalkDirStreamRequest, writer: &mut dyn AsyncWrite) -> Result<()>;
    fn capabilities(&self) -> InternodeTransportCapabilities;
}
```

Initial request fields should mirror the current HTTP query parameters:

- peer endpoint
- disk reference
- volume
- path
- offset
- length
- append/create mode
- expected size
- auth or transfer token material

The initial TCP backend can keep the current signed HTTP URLs internally.

### Integration point

`RemoteDisk` should delegate only these methods to the data transport:

- `read_file_stream`
- `read_file_zero_copy` as a wrapper over `read_file_stream` unless the backend
  supports a stronger zero-copy API
- `append_file`
- `create_file`
- optionally `walk_dir`

All other `RemoteDisk` methods should continue using the current gRPC client
until measurements prove otherwise.

### Capability model

Avoid hard-coding RDMA assumptions into the generic interface. Use capabilities:

- stream read
- stream write
- bounded range read
- bidirectional streaming
- registered memory support
- scatter/gather support
- zero-copy receive into caller-owned buffers
- authenticated out-of-band transfer
- transport fallback support

The first TCP backend should report only capabilities that it actually provides.

## TCP Fallback Requirements

TCP/HTTP/gRPC must remain the default and required backend.

Fallback rules:

- If no explicit data transport is configured, use the current TCP/HTTP
  implementation.
- If an experimental backend fails initialization, either fail fast with a clear
  error or fall back to TCP only when the configured policy allows fallback.
- Runtime fallback must preserve object correctness and quorum semantics.
- Fallback events must be logged and counted in metrics.
- CI and local development must not require RDMA-capable hardware.

Suggested future configuration shape:

```text
RUSTFS_INTERNODE_DATA_TRANSPORT=tcp
RUSTFS_INTERNODE_DATA_TRANSPORT_FALLBACK=tcp
```

Do not add these settings until there is an implementation PR that uses them.

## Future RDMA/RoCE/InfiniBand Boundary

A future RDMA backend should be experimental and feature-gated. It should be
designed as an optional data-plane backend, not as a replacement for the gRPC
control plane.

Required design areas:

- peer capability discovery over the existing gRPC control plane
- connection management and health mapping into existing disk fault handling
- memory registration lifecycle and registration cache
- buffer ownership, pinning, alignment, and lifetime rules
- scatter/gather behavior for erasure shards
- authentication and authorization for out-of-band data transfers
- encryption/TLS-equivalent story or a documented deployment boundary
- timeout, cancellation, retry, and fallback behavior
- metrics for registration cost, transfer latency, bytes, queue depth, retries,
  fallback, and errors
- hardware and kernel compatibility matrix

The first RDMA prototype should target `read_file_stream` and `put_file_stream`
only. `walk_dir`, metadata RPCs, locks, admin RPCs, and bucket coordination
should remain on gRPC unless a later benchmark identifies a specific bottleneck.

## DPU, DOCA, DPDK, SPDK, and SmartNIC Notes

These technologies should not drive the first abstraction:

- DPU/BlueField/DOCA may become relevant for TLS, checksum, compression, or
  storage/network offload, but they are vendor- and deployment-specific.
- DPDK is a poor first fit because RustFS is currently an HTTP/S3 object store
  and does not have a custom packet data plane.
- SPDK may be relevant only if RustFS adds a raw block or NVMe-oriented local
  storage backend. The current disk model is filesystem-based.
- SmartNIC offload should be discussed only after the data-plane boundary and
  baseline metrics show where CPU is spent.

## Suggested PR Sequence

1. Add this RFC and the current-path classification.
2. Add route-level internode metrics for `/rustfs/rpc/read_file_stream`,
   `/rustfs/rpc/put_file_stream`, `/rustfs/rpc/walk_dir`, and gRPC disk byte
   calls.
3. Add an internode transport benchmark harness that can run against a local
   multi-node cluster and produce repeatable artifacts.
4. Introduce an `InternodeDataTransport` wrapper with a TCP/HTTP backend that
   preserves current behavior.
5. Move `RemoteDisk` stream methods to the transport wrapper without changing
   default behavior.
6. Add an experimental feature-gated RDMA/RoCE backend only after the baseline
   proves that internode byte transfer is a limiting factor.

## Open Questions

- Which production workload is the primary target: large-object throughput,
  small-object tail latency, healing throughput, or rebalance throughput?
- Should `ReadAll` and `WriteAll` stay as gRPC unary calls, or should large
  payloads be redirected to the data transport?
- Is `walk_dir` a metadata control stream or a secondary data-plane stream for
  scanner/healing workloads?
- What is the acceptable fallback policy for an explicitly configured
  experimental backend?
- How should an RDMA backend preserve authentication and encryption guarantees
  currently provided by signed HTTP requests and TLS-capable gRPC/HTTP clients?
- What hardware matrix is required before accepting a non-default RDMA backend?

## Immediate Next Steps

- Create a focused issue from this RFC.
- Add route-level internode metrics before changing transport code.
- Extend existing benchmark scripts or add a new script to isolate remote disk
  stream read/write throughput.
- Keep the first code PR behavior-preserving and TCP-only.
