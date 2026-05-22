# RFC: Internode Transport Adapter Boundary

> Status: draft
> Last updated: 2026-05-22
> Scope: OSS internode data-plane adapter analysis, benchmark baseline, and
> transport boundary

## Summary

The current distributed internode paths use TCP-based HTTP/gRPC transports:

- `tonic` gRPC `NodeService` for most control, metadata, lock, health, and
  peer operations.
- HTTP streaming routes under `/rustfs/rpc/` for remote disk file streams.

This document frames the existing work as an OSS `InternodeDataTransport`
adapter boundary. The adapter keeps RustFS data-plane logic separate from the
concrete transport backend while preserving the current TCP/HTTP behavior as
the default implementation.

Current implementation status:

- `InternodeDataTransport` exists in
  `crates/ecstore/src/rpc/internode_data_transport.rs`.
- The default and only production backend is `tcp-http`; `tcp` is accepted as
  an alias.
- `RUSTFS_INTERNODE_DATA_TRANSPORT` selects the backend. Blank or unset values
  use `tcp-http`; invalid values fail closed.
- `RemoteDisk::read_file_stream`, `RemoteDisk::create_file`,
  `RemoteDisk::append_file`, and `RemoteDisk::walk_dir` delegate to the
  transport.
- `NodeService` gRPC remains the internode control plane and continues to carry
  metadata/control operations.

Related design notes in this directory:

- `transport-capabilities.md`
- `transport-buffer-lifecycle.md`
- `transport-buffer-contract.md`
- `transport-fallback-and-selection.md`

## Open-source Scope

The OSS scope is:

- define a clear `InternodeDataTransport` adapter boundary;
- keep `tcp-http` as the default backend;
- keep existing TCP/HTTP behavior unchanged;
- keep internode data-plane behavior observable through metrics and baseline
  tooling;
- document buffer ownership, fallback, and capability expectations for
  maintainable transport code;
- avoid hardware-specific dependencies or backend implementations.

The OSS scope is not:

- RDMA support;
- DPU support;
- DOCA support;
- BlueField support;
- RoCE/InfiniBand support;
- hardware benchmark planning;
- hardware-specific backend implementation.

Hardware-specific transport experiments are outside the scope of this OSS
document.

## Goals

- Document the current internode control plane and data plane.
- Identify the existing transfer paths covered by the
  `InternodeDataTransport` adapter and the paths that remain on gRPC.
- Define the minimum benchmark baseline required before transport changes.
- Sketch a pluggable transport boundary that preserves the current TCP/HTTP
  behavior as the default backend.
- Document backend-neutral capability, fallback, buffer ownership, and
  observability expectations.

## Non-Goals

- Implement RDMA, RoCE, InfiniBand, DPU, DOCA, DPDK, SPDK, or SmartNIC support.
- Replace `tonic` gRPC for control-plane RPCs.
- Redesign erasure coding, quorum handling, disk health tracking, or object
  correctness semantics.
- Require specialized hardware for default development, CI, or ordinary RustFS
  deployments.

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

The service layout is practical today, but it is too broad to become the
transport adapter surface. A pluggable data transport should target only disk
data streams and keep this gRPC service as the control plane.

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

## P1 Data Path Inventory

Classification:

- Covered by `InternodeDataTransport`: `RemoteDisk` opens the transfer through
  the transport abstraction.
- Still direct TCP/HTTP/gRPC: bytes move over a fixed internode protocol
  outside the transport abstraction.
- Metadata/control-plane only: payloads are expected to be small metadata,
  namespace, lock, health, or admin messages.
- Not relevant: declared or test-only paths that are not current production
  data paths.

### Covered by `InternodeDataTransport`

| Path | Owner references | Server references | Classification | Notes |
| --- | --- | --- | --- | --- |
| Remote shard read stream | `crates/ecstore/src/rpc/remote_disk.rs::RemoteDisk::read_file_stream`; `crates/ecstore/src/rpc/internode_data_transport.rs::InternodeDataTransport::open_read`; `crates/ecstore/src/bitrot.rs::create_bitrot_reader` | `rustfs/src/storage/rpc/http_service.rs::handle_read_file` | Covered by `InternodeDataTransport` | Object GET, repair reads, and erasure decode use this path for remote shard bytes. |
| Remote shard write stream | `RemoteDisk::create_file`; `RemoteDisk::append_file`; `InternodeDataTransport::open_write`; `crates/ecstore/src/bitrot.rs::create_bitrot_writer` | `rustfs/src/storage/rpc/http_service.rs::handle_put_file` | Covered by `InternodeDataTransport` | Object PUT and multipart part upload use this path for remote shard bytes. |
| Remote namespace walk stream | `RemoteDisk::walk_dir`; `InternodeDataTransport::open_walk_dir`; `crates/ecstore/src/cache_value/metacache_set.rs` walk producers | `rustfs/src/storage/rpc/http_service.rs::handle_walk_dir` | Covered by `InternodeDataTransport` | High-volume listing/scanner/heal metadata stream. It is not object byte data, but it is a large internode stream. |
| Remote zero-copy read fallback | `RemoteDisk::read_file_zero_copy` | same as remote shard read stream | Covered by `InternodeDataTransport` through `read_file_stream` | The remote path buffers the stream into `Bytes`; true zero-copy is not guaranteed for remote disks. |

### Still Direct TCP/HTTP/gRPC

| Path | Owner references | Server references | Classification | Notes |
| --- | --- | --- | --- | --- |
| `ReadAll` | `RemoteDisk::read_all`; `crates/ecstore/src/store_init.rs`; heal resume metadata readers | `rustfs/src/storage/rpc/disk.rs::handle_read_all` | Still direct gRPC | Unary `bytes` response. Currently used mostly for metadata/config files; measure before moving. |
| `WriteAll` | `RemoteDisk::write_all`; `crates/ecstore/src/store_init.rs`; heal resume metadata writers | `rustfs/src/storage/rpc/disk.rs::handle_write_all` | Still direct gRPC | Unary `bytes` request. Currently used mostly for metadata/config/checkpoint writes. |
| `ReadMultiple` | `RemoteDisk::read_multiple`; `crates/ecstore/src/set_disk/read.rs::read_multiple_files` | `rustfs/src/storage/rpc/disk.rs::handle_read_multiple` | Still direct gRPC | Returns multiple small file payloads, usually metadata/listing support. Could become large with many entries. |
| `ReadParts` | `RemoteDisk::read_parts`; `crates/ecstore/src/set_disk/read.rs::read_parts`; multipart list/complete paths | `rustfs/src/storage/rpc/disk.rs::handle_read_parts` | Still direct gRPC | Encoded `ObjectPartInfo` metadata, not object data. |
| `RenamePart` | `RemoteDisk::rename_part`; `crates/ecstore/src/set_disk/write.rs::rename_part` | `rustfs/src/storage/rpc/disk.rs::handle_rename_part` | Still direct gRPC | Carries part metadata while committing multipart data already written through stream writers. |
| `ListDir` | `RemoteDisk::list_dir`; multipart/lifecycle metadata listing callers | `rustfs/src/storage/rpc/disk.rs::handle_list_dir` | Still direct gRPC | Directory name listing, metadata/control-plane unless measured otherwise. |
| Legacy gRPC `WalkDir` | `rustfs/src/storage/rpc/node_service.rs::NodeService::walk_dir` | same file | Still direct gRPC | Server implementation remains, but current `RemoteDisk::walk_dir` uses HTTP through the transport. Keep until callers are audited or compatibility policy is set. |

### Metadata/control-plane only

| Area | Owner references | Classification | Notes |
| --- | --- | --- | --- |
| Disk metadata and namespace mutations | `RemoteDisk::{read_metadata,write_metadata,update_metadata,read_version,read_xl,rename_data,rename_file,delete*,verify_file,check_parts,disk_info}` | Metadata/control-plane only | These remain on gRPC by design. |
| Peer/bucket/admin operations | `crates/ecstore/src/rpc/{peer_s3_client.rs,peer_rest_client.rs,remote_locker.rs}` and matching `rustfs/src/storage/rpc/*` handlers | Metadata/control-plane only | Not candidates for a data-plane backend without separate measurements. |
| Store init and format operations | `crates/ecstore/src/store_init.rs` | Metadata/control-plane only | Uses `ReadAll`/`WriteAll` for small format/config objects. |
| Heal orchestration | `crates/heal/src/heal/storage.rs` and `crates/ecstore/src/set_disk.rs::heal_object` | Metadata/control-plane plus covered data reads | Heal object data reads go through `get_object_reader` and then covered shard streams; resume/checkpoint metadata uses direct gRPC disk metadata calls. |

### Not Relevant Current Paths

| Path | Owner references | Classification | Notes |
| --- | --- | --- | --- |
| Proto `Write` | `crates/protos/src/node.proto`; `rustfs/src/storage/rpc/disk.rs::handle_write` | Not relevant | Handler is unimplemented. |
| Proto `WriteStream` | `crates/protos/src/node.proto`; `rustfs/src/storage/rpc/node_service.rs::write_stream` | Not relevant | Returns `unimplemented`. |
| Proto `ReadAt` | `crates/protos/src/node.proto`; `rustfs/src/storage/rpc/node_service.rs::read_at` | Not relevant | Returns `unimplemented`. |
| E2E reliant gRPC helpers | `crates/e2e_test/src/reliant/*` | Not relevant | Test harnesses, not production internode data-path callers. |

### Current Limitations

| Risk | Limitation |
| --- | --- |
| Medium | `ReadAll` and `WriteAll` still carry unary `bytes` over gRPC. They appear metadata-oriented today, but there is no size threshold or routing policy. |
| Medium | `ReadMultiple` can aggregate many metadata files into one gRPC response. |
| Low | Legacy gRPC `WalkDir` remains implemented while `RemoteDisk::walk_dir` uses HTTP through the transport. |
| Medium | Remote `read_file_zero_copy` is a buffered read over the transport, not a remote zero-copy contract. |
| Medium | Server-side TCP HTTP route handling is outside the client-side trait. |

## Current Object Write Path

For object PUTs in distributed erasure mode, the relevant flow is:

1. Upper storage layers prepare object data and erasure metadata.
2. `SetDisks` selects local and remote disks.
3. `create_bitrot_writer` calls `disk.create_file(...)` for each shard writer.
4. For a remote disk, `RemoteDisk::create_file` delegates to
   `InternodeDataTransport::open_write`.
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
4. `RemoteDisk::read_file_stream` delegates to
   `InternodeDataTransport::open_read`.
5. `HttpReader` sends an HTTP `GET` to `/rustfs/rpc/read_file_stream`.
6. The remote node's `handle_read_file` opens the local disk stream and returns
   it as an HTTP streaming body.
7. The erasure decoder reads from the shard streams and reconstructs the object.

This is the primary read data-plane candidate.

## Existing Metrics and Benchmark Surface

RustFS already has coarse internode metrics in `crates/io-metrics/src/internode_metrics.rs`:

- sent bytes
- received bytes
- outgoing requests
- incoming requests
- errors
- dial errors
- average dial time

These metrics are useful as a starting point. For backend comparisons, the
relevant route-level and operation-level dimensions are:

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

Before changing internode data transport behavior or comparing a non-default
backend, collect a baseline for the current TCP/HTTP/gRPC implementation.

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
| Scanner walk | large bucket/object namespace | controlled | Metadata streaming pressure, not the primary object-byte transport path. |

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
Non-default backend work should not proceed until the default wrapper is
measured and adapter gaps are documented.

### Candidate boundary

The current boundary is remote disk stream transfer:

```rust
#[async_trait::async_trait]
pub trait InternodeDataTransport: Send + Sync + std::fmt::Debug {
    async fn open_read(&self, request: ReadStreamRequest) -> Result<FileReader>;
    async fn open_write(&self, request: WriteStreamRequest) -> Result<FileWriter>;
    async fn open_walk_dir(&self, request: WalkDirStreamRequest) -> Result<FileReader>;
    fn name(&self) -> &'static str;
    fn capabilities(&self) -> InternodeDataTransportCapabilities;
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
- optional stall timeout for long-running listing streams

The initial TCP backend can keep the current signed HTTP URLs internally.

### Integration point

`RemoteDisk` delegates only these methods to the data transport:

- `read_file_stream`
- `read_file_zero_copy` as a wrapper over `read_file_stream` unless the backend
  supports a stronger zero-copy API
- `append_file`
- `create_file`
- `walk_dir`

All other `RemoteDisk` methods continue using the current gRPC client
until measurements prove otherwise.

### Capability model

Avoid hard-coding transport-specific assumptions into the generic interface.
Use capabilities:

- stream read
- stream write
- bounded range read
- bidirectional streaming
- backend-specific buffer registration or staging requirements
- stable buffer ownership support
- copy-reduced receive into caller-owned or backend-owned buffers
- authenticated out-of-band transfer
- transport fallback support

The first TCP backend should report only capabilities that it actually provides.

## TCP Fallback Requirements

TCP/HTTP/gRPC must remain the default and required backend.

Fallback rules:

- If no explicit data transport is configured, use the current TCP/HTTP
  implementation.
- The current accepted values for `RUSTFS_INTERNODE_DATA_TRANSPORT` are
  `tcp-http` and the `tcp` alias. Empty and unset values use `tcp-http`.
- Invalid configured values fail closed with an error that includes the env var
  name and invalid value.
- If a future non-default backend fails initialization, either fail fast with a
  clear error or fall back to TCP only when the configured policy allows
  fallback.
- Runtime fallback must preserve object correctness and quorum semantics.
- Fallback events must be logged and counted in metrics.
- CI and local development must not require specialized transport hardware.

Suggested future configuration shape:

```text
RUSTFS_INTERNODE_DATA_TRANSPORT=tcp-http
RUSTFS_INTERNODE_DATA_TRANSPORT_FALLBACK=tcp
```

Do not add fallback settings until there is an implementation PR that uses them.

## Baseline Validation Commands

Dry-run command:

```bash
scripts/run_internode_transport_baseline.sh \
  --access-key minioadmin \
  --secret-key minioadmin \
  --scenarios local=http://127.0.0.1:9000,distributed=http://127.0.0.1:9001 \
  --sizes 4KiB,1MiB \
  --concurrencies 1 \
  --duration 10s \
  --dry-run
```

Real TCP baseline command with metrics:

```bash
RUSTFS_INTERNODE_DATA_TRANSPORT=tcp-http \
scripts/run_internode_transport_baseline.sh \
  --access-key "$RUSTFS_ACCESS_KEY" \
  --secret-key "$RUSTFS_SECRET_KEY" \
  --scenarios local=http://127.0.0.1:9000,distributed=http://127.0.0.1:9001 \
  --metrics-url http://127.0.0.1:9000/metrics \
  --out-dir target/bench/internode-transport/manual-run
```

Expected artifacts:

- `run_manifest.txt`
- `summary.csv`
- `internode_metric_deltas.csv` when `--metrics-url` is provided

The baseline validates the default TCP/HTTP path only. It must not be used to
claim support or performance for any non-default transport backend.

## Non-default Backend Boundary

A future non-default backend must be explicitly enabled and must not replace
`tcp-http` silently. It should be designed as an optional data-plane backend,
not as a replacement for the gRPC control plane.

A future non-default backend would need an explicit design for:

- peer capability discovery over the existing gRPC control plane;
- connection management and health mapping into existing disk fault handling;
- backend-specific buffer lifecycle and any staging or registration cache;
- buffer ownership, alignment, and lifetime rules;
- stable buffer behavior for erasure shards;
- authentication and authorization for out-of-band data transfers;
- encryption or an equivalent documented security boundary;
- timeout, cancellation, retry, and fallback behavior;
- metrics for transfer latency, bytes, queue depth, retries, fallback, and
  errors.

`walk_dir`, metadata RPCs, locks, admin RPCs, and bucket coordination remain
outside the current data-plane boundary.

## Out of Scope

Hardware-specific transport experiments are outside the scope of this OSS
document. This RFC does not add a plugin system, split the adapter into a
separate crate, add accepted backend values, or implement a new transport
backend.
