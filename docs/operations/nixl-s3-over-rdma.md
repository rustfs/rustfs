# S3-over-RDMA on NIXL/UCX

RustFS can serve S3 GETs and PUTs over RDMA with NIXL (NVIDIA Inference Xfer
Library) and its UCX backend instead of the cuObject library described in
[cuobject-s3-over-rdma.md](cuobject-s3-over-rdma.md). The request stays an
ordinary signed S3 call; the client adds its NIXL agent name, agent metadata
and registered buffer in headers, the server moves the bytes with NIXL WRITE
(GET) or READ (PUT) transfers, and the reply carries the same
`x-amz-rdma-reply` and `x-amz-rdma-bytes-transferred` headers as the cuObject
transport. UCX addresses every device by the GID, LID and queue-pair numbers
in the agent metadata, so the GPU-side InfiniBand rails work without IPoIB or
a subnet administrator and the CPU-side RoCE ports work without rdma_cm.

The server-side pipeline (segment fan-out, staging chunks, shard arena,
per-write stages) is the one the cuObject transport uses; only the transfer
layer differs. The two transports are compile-time alternatives: a build with
the `nixl` feature uses NIXL for every RDMA request, even when `cuobject` is
also enabled.

## Build

- Cargo feature `nixl` on the `rustfs` crate (`--features rdma,nixl`); the
  `rustfs-nixl` crate depends on the `nixl-sys` binding from the NIXL
  repository at tag `v1.5.2`.
- `NIXL_PREFIX` points at a prefix with `include/` (the NIXL C++ headers of
  that tag) and `lib/` holding `libnixl.so`, `libnixl_build.so`,
  `libnixl_common.so`, `libserdes.so`, `libstream.so` and
  `lib/plugins/libplugin_UCX.so`. The `nixl-cu13` wheel ships the libraries
  (under `site-packages/.nixl_cu13.mesonpy.libs/`); the headers come from the
  repository. `bindgen` needs `libclang` at build time.
- At run time the server needs `LD_LIBRARY_PATH` on that `lib/` and
  `NIXL_PLUGIN_DIR` on `lib/plugins`.

## Settings

| Variable | Default | Meaning |
|---|---|---|
| `RUSTFS_NIXL_ENABLE` | `false` | Serve requests that carry the NIXL headers over NIXL. |
| `RUSTFS_NIXL_AGENT_NAME` | `rustfs-<first local endpoint host>` | This node's agent name; must be unique in the cluster. |
| `RUSTFS_NIXL_UCX_DEVICES` | all devices | Comma-separated UCX device names (`mlx5_9,mlx5_10`; NIXL adds the port); pin the agent to the rails that reach the clients. With `UCX_MAX_RMA_RAILS=2` in the server's environment, UCX stripes every write across two of these ports and the client must have registered its buffer on both of them, or the transfer fails and the GET is declined. |
| `RUSTFS_NIXL_NUM_WORKERS` | `1` | UCX progress workers of the agent. |
| `RUSTFS_NIXL_CHUNK_BYTES` | `16 MiB` | Staging chunk, one transfer descriptor at most. |
| `RUSTFS_NIXL_POOL_BYTES` | `1 GiB` | Registered staging memory. |
| `RUSTFS_NIXL_MAX_INFLIGHT` | `256` | Transfers in flight per node. |
| `RUSTFS_NIXL_TRANSFER_TIMEOUT_SECS` | `60` | A transfer that has not completed by then fails the request instead of hanging it. |
| `RUSTFS_NIXL_DEVICE_CHECK` | `true` | Check the devices at startup and leave out unhealthy ones (see below). |
| `RUSTFS_NIXL_DEVICE_MAX_LINK_DROPS` | `16` | Leave out a device whose link dropped this many times more since boot than the quietest device of its link layer; `0` skips this test. |
| `RUSTFS_NIXL_DEVICE_FLAP_WINDOW_MS` | `3000` | How long the startup check watches the link counters; `0` skips the watch. |
| `RUSTFS_INTERNODE_DATA_TRANSPORT` | `tcp-http` | `nixl` moves internode shard reads over NIXL too (see below). |
| `RUSTFS_NIXL_INTERNODE_DEPTH` | `4` | Internode chunk requests in flight per remote shard stream. |
| `RUSTFS_CUOBJECT_PUT_READ_AHEAD` | `4` | Staging chunks read from the client's buffer ahead of the PUT body's consumer; each holds one chunk of the pool. |

The pipeline settings `RUSTFS_CUOBJECT_GET_FANOUT`,
`RUSTFS_CUOBJECT_GET_FANOUT_MIN_BYTES`, `RUSTFS_CUOBJECT_GET_FANOUT_BUDGET`,
`RUSTFS_CUOBJECT_INFLIGHT_WRITES` and `RUSTFS_CUOBJECT_SHARD_ARENA_BYTES`
apply unchanged.

### Large-object PUT tuning

Several settings outside the NIXL variables decide PUT throughput on large hosts:

- `RUSTFS_RUNTIME_WORKER_THREADS`: the runtime defaults to at most 16 worker threads, sized for small-object PUTs. A large PUT runs its part's MD5 and erasure encoding inline on those workers in bursts of 20-70 ms, so with 16 workers a task woken by a finished NIXL transfer waits behind them while most cores stay idle. On the 256-core staging servers 128 workers doubled 16-stream PUT throughput (12.5 to 26 GB/s).
- `MIMALLOC_ALLOW_THP=0`: with transparent huge pages in `madvise` mode, the allocator's huge-page requests for the per-stripe buffers send the kernel into direct compaction, which took about a third of the CPU during PUTs.
- `MIMALLOC_PURGE_DELAY=-1`: the allocator keeps freed memory for reuse instead of returning it to the kernel. Every PUT allocates and frees tens of megabytes of stripe and staging buffers, and returning them made the kernel fault in and zero fresh pages, shoot down TLBs and compact memory under its zone lock. Keeping them raised a 4-client PUT of 57 MB objects from about 14 to about 18 GB/s on two servers. The process's resident memory then stays at its peak.
- `RUSTFS_OBJECT_DATA_CACHE_FILL_ON_PUT=false` for write-heavy buckets such as an LLM KV cache: fill-on-put copies every PUT body that fits the cache entry limit into the object data cache, which cost about 30-40 ms per 57 MB object. Without it, an RDMA PutObject body also stays in the staging chunks it was read into (no copy).
- `RUSTFS_OBJECT_ETAG_MD5_OFFLOAD_MIN_BYTES` (default 8 MiB, `0` turns it off): bodies at least this large compute their ETag MD5 on a blocking-pool thread while the body streams into the erasure encoder, instead of inline before it. The ETag and the Content-MD5 check are unchanged; the PUT fails before commit on a mismatch.
- `RUSTFS_OBJECT_DIRECT_IO_WRITE_ENABLE=true`: shard files are written with O_DIRECT instead of through the page cache, which otherwise costs a copy into freshly allocated, zeroed cache pages, zone-lock contention and compaction on every write. With four PUT clients on two servers (57 MB objects) it more than doubled the aggregate (7 to 16-21 GB/s) while reads of the just-written objects, now served by the drives, stayed within about 10%. The NIXL internode shard writes finish every file with a writer shutdown, which the direct writer needs to write its trailing sub-alignment bytes.
- A per-bucket XXH3 ETag for buckets whose clients never treat the ETag as an MD5, such as an LLM KV cache: objects written to the bucket without Content-MD5 get an XXH3-128 ETag in the same 32-hex-digit form, an order of magnitude cheaper to compute. It halved the single-stream PUT of 57 MB objects (105 to 55 ms) but did not raise the saturated aggregate. Other buckets keep MD5 ETags, so ordinary S3 clients (rclone checksums, SDKs that verify an upload against the returned ETag) are unaffected. See "Per-bucket ETag algorithm" below.

`RUSTFS_OBS_PUT_STAGE_METRICS_ENABLED=true` together with `RUSTFS_OBS_GET_STAGE_LOCAL_SUMMARY_ENABLED=true` writes the PUT and erasure stage timings (body wait, encode, internode segment writes, part commit phases) into the local stage summary file without a metrics exporter.

### Per-bucket ETag algorithm

A bucket's ETag algorithm is stored as a RustFS extension entry (`etag.json`) in its bucket metadata and set through the admin API with `ConfigUpdateAdminAction`:

```bash
# Use XXH3-128 ETags for objects written to the bucket without Content-MD5
curl -X PUT "http://<host>/rustfs/admin/v3/bucket-etag/<bucket>" -d '{"algorithm":"xxh3"}'   # md5 | xxh3

# Read it back ("algorithm": null means no setting: MD5 ETags)
curl "http://<host>/rustfs/admin/v3/bucket-etag/<bucket>"

# Clear it (MD5 ETags again)
curl -X DELETE "http://<host>/rustfs/admin/v3/bucket-etag/<bucket>"
```

- It applies to PutObject and UploadPart; CopyObject, tier restore and internal writes keep MD5.
- A request that carries Content-MD5 always gets an MD5 ETag and the exact check. Some SDKs add Content-MD5 to every PutObject and UploadPart on their own (boto3 before 1.36, for example), so their objects keep MD5 ETags, and the MD5 cost, even in an XXH3 bucket; the writer that should benefit must not send it.
- A multipart upload keeps the algorithm the bucket had when the upload was created, so all its parts hash alike; the multipart ETag is formed from the part ETags as usual.
- The setting propagates like the per-bucket durability override: immediately on the node that applied it, to peers through the bucket-metadata reload notification, and at the latest with the 15-minute metadata refresh. Objects already written keep their ETags.

## Wire contract

Request headers (GET, PUT, UploadPart):

| Header | Value |
|---|---|
| `x-rustfs-nixl-agent` | The client's agent name (printable ASCII, up to 128 bytes). |
| `x-rustfs-nixl-metadata` | The client's agent metadata (`get_local_md`), base64. May be omitted once this server has loaded that agent; a server that does not know the agent declines the request. A restarted client sends new metadata, which replaces the old. |
| `x-rustfs-nixl-buffer` | `<dram|vram>:<device>:<address>:<length>`: the registered buffer the object (or the requested range) lands in from offset 0, or is read from for a PUT. Address and length are decimal or `0x` hex. |

Reply headers: `x-amz-rdma-reply` (`200`, `206` for a range, `501` when the
transfer could not run), `x-amz-rdma-bytes-transferred`. The HTTP body is
empty; the reply is sent after the last transfer completed, so the client may
read its buffer as soon as the response arrives. The server never needs the
client to load the server's metadata.

## Device health check

UCX keeps using a device whose link is down or flapping, and a transfer
striped over that lane fails; NIXL then drops the whole peer. Before the
agent is created the server reads `/sys/class/infiniband/<dev>/ports/1` for
every device in `RUSTFS_NIXL_UCX_DEVICES` (every RDMA device on the node
when the list is empty) and leaves out a device that

1. is missing, or whose port is not `ACTIVE` with physical state `LinkUp`;
2. runs below the best rate of the candidates of its link layer;
3. lost its link more than `RUSTFS_NIXL_DEVICE_MAX_LINK_DROPS` times more
   often since boot than the quietest device of its link layer (a rail
   that flaps every few minutes rarely drops inside a short watch, and
   comparing with the quietest peer ignores events that hit every rail,
   such as a switch reboot);
4. loses its link during the watch window.

One log line (`event=cuobject_service_state state=device_check`) lists the
kept devices and every excluded one with its reason, as a warning when any
is excluded. If no device passes, startup fails. The check runs once; a
device that fails later stays in use until the next restart. Clients choose
their own devices.

## Internode reads and writes over NIXL

With `RUSTFS_INTERNODE_DATA_TRANSPORT=nixl` the shard reads and writes between
servers also move over NIXL, so a cluster can run its whole data plane on the
GPU-side InfiniBand rails, which have no IP addresses. The control request
stays on the internode HTTP connection; only the data moves over the rails.

- For each chunk of a remote shard stream the reading node sends a signed
  `GET /rustfs/rpc/read_file_nixl` with the usual disk, volume, path, offset
  and length, plus its agent name (`nixl_agent`), one buffer registered with
  its agent (`nixl_buffer`, same format as `x-rustfs-nixl-buffer`) and the
  SHA-256 of its agent metadata (`nixl_md`). The metadata itself travels in
  `x-rustfs-nixl-metadata` and must hash to the signed digest, so a replayed
  request cannot point the transfer at another endpoint.
- The serving node reads the range with `pread` into one of its registered
  staging chunks, writes it into the named buffer with a NIXL WRITE and
  answers `200` with `x-rustfs-nixl-bytes` (fewer than asked means end of
  file). The buffer reaches the decoder as a zero-copy `Bytes`.
- `RUSTFS_NIXL_INTERNODE_DEPTH` (default `4`) chunk requests run per stream;
  a chunk is one staging chunk (`RUSTFS_NIXL_CHUNK_BYTES`).
- Shard writes: the writing node stages the file's bytes in registered
  buffers and, once the file is complete (or every four buffers for a large
  file), sends a signed `PUT /rustfs/rpc/write_file_nixl` naming them
  (`nixl_buffers`, in file order), its agent, the metadata digest and a
  one-time `nixl_nonce`. The disk's node records the nonce (a replay is
  refused), pulls every buffer with a NIXL READ and writes it through the
  same disk writer as the TCP path (create, or append for later segments),
  checks the declared size on the last segment and flushes. The buffers
  return to the writer's pool only after the node answered.
- Directory walks, unbounded reads and any read stream whose first chunk
  fails use TCP/HTTP, which also keeps its typed errors (file not found and
  so on). A build without the `nixl` feature answers the route with `404`,
  which the reader treats the same way.
- A receive buffer whose request was abandoned (a dropped reader, chunks past
  the end of the file) stays out of the pool for two minutes, longer than
  the serving node's transfer timeout, because the peer may still write it.
- Every node's agent runs NIXL's progress thread. A node that is the target
  of RDMA writes must keep answering UCX connection handshakes and
  keepalives even with no transfer of its own in flight; without it peers
  declare the node dead and NIXL drops the agent ("remote agent ... was
  disconnected after transfer request creation"). After any failed
  transfer the server forgets the peer and reloads its metadata on the next
  request.

Rails: name every rail of the node in `RUSTFS_NIXL_UCX_DEVICES`, minus any
that is down, and set `UCX_MAX_RMA_RAILS=8` so host-memory transfers stripe
across them; GPU clients register only their GPU's rail.

## Implementation notes

- `Agent::post_xfer_req` in the `nixl-sys` binding returns `true` while the
  transfer is still in progress and `false` when it completed before
  returning. The server polls `get_xfer_status` only in the first case. The
  opposite reading treats every transfer as complete at post time, refills
  the staging chunk before the NIC has read it, and the client receives the
  next chunk's bytes at the earlier offset; the reply also arrives before the
  data. Repeated rounds of the same object hide that, so a verification run
  uses one round into a zeroed buffer.
- A client that never polls its agent needs NIXL's progress thread (the
  binding's `Agent::new` enables it; `new_configured` only with
  `enable_prog_thread`), because the server's endpoint finishes its UCX
  connection handshake only when the client's worker progresses. Without it
  every transfer to that client stays in progress until the timeout.
- The binding serializes every agent call with a lock of its own; the agent
  is nevertheless created with reader-writer synchronization so correctness
  does not rest on that detail.
- Transfers are posted and polled by one completion thread (`nixl-xfer`);
  request tasks wait on a oneshot. The binding takes a process-wide lock on
  every agent call, and hundreds of tasks each spinning on their own
  transfer's status made that lock a convoy.
- Keep the page cache for served shards (`RUSTFS_OBJECT_FILE_CACHE_RECLAIM_READ_ENABLE=false`)
  and size the staging pool for the load (`RUSTFS_NIXL_POOL_BYTES`, 8 GiB
  for 16 concurrent streams).
- The chunk-reuse contract can be checked without RustFS: a writer fills a
  remote buffer from a small pool of chunks, refilling each as soon as NIXL
  reports its transfer complete, and the target verifies every word against
  its offset.

## Validation

Check a transport change with a client that fetches an object into a
zero-filled GPU or host buffer through this contract and compares it with a
plain HTTP GET of the same object. Verify with a single round: later rounds
rewrite the same bytes and mask a transfer that completed early.
