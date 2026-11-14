# cuObject S3-over-RDMA GET (last hop into client memory)

RustFS can serve `GetObject` over NVIDIA's cuObject protocol: the client
(for example the NIXL `OBJ` plugin used by the SGLang `--load-format rdma`
loader) registers its GPU or host buffer with the cuObject client library,
puts the resulting RDMA token in the `x-amz-rdma-token` request header, and
RustFS writes the object bytes straight into that buffer with one-sided RDMA
writes. The HTTP response carries no body.

This is the Phase 1 "last hop" of the S3-over-RDMA plan. It is independent of
the internode shard transport selected by `RUSTFS_INTERNODE_DATA_TRANSPORT`;
either can be enabled on its own.

## Build

The feature links NVIDIA's cuObject *server* library (`libcuobjserver`, part
of the CUDA apt repository as `libcuobjserver` / `libcuobjserver-dev`) through
the [`cuobject`](https://crates.io/crates/cuobject) bindings; their
`cuobject-sys` build script generates the FFI with bindgen (needs libclang)
and compiles a C++ wrapper over the library. It is off by default and
Linux-only:

```bash
# CUOBJSERVER_ROOT points at a prefix holding include/cuobjserver.h and lib/libcuobjserver.so
CUOBJSERVER_ROOT=/usr cargo build --release -p rustfs --features cuobject
```

The binary dynamically links `libcuobjserver.so.2`; keep it on the library
path at run time. Without the feature every setting below is ignored and
tokens are left untouched, so the client falls back to a normal HTTP GET.

## Settings

| Variable | Default | Meaning |
| --- | --- | --- |
| `RUSTFS_CUOBJECT_ENABLE` | `false` | Start the cuObject servers and honour `x-amz-rdma-token`. |
| `RUSTFS_CUOBJECT_ADDRS` | local endpoint hosts | Comma-separated RDMA (RoCE/IB) IPs to serve from; one cuObject server per address. |
| `RUSTFS_CUOBJECT_PORT` | `18515` | cuObject control port; the client never dials it directly with the DC transport. |
| `RUSTFS_CUOBJECT_CHUNK_BYTES` | `16 MiB` | Staging chunk: the object is streamed into registered host memory in chunks of this size and each chunk is written to the client with one RDMA operation. Max 1 GiB. |
| `RUSTFS_CUOBJECT_INFLIGHT_WRITES` | `3` | Staging chunks per GET: while one chunk is being RDMA-written the decoder keeps filling the next ones, so the stripe loop never waits for a write. Each costs one chunk of the pool; a busy pool degrades to fewer chunks rather than failing the GET. |
| `RUSTFS_CUOBJECT_POOL_BYTES` | `1 GiB` | Registered staging pool per server; bounds concurrent RDMA GETs to about `pool / (chunk x inflight)`. |
| `RUSTFS_CUOBJECT_NUM_DCIS` | `128` | DC initiators the server library opens. |
| `RUSTFS_CUOBJECT_ASYNC_TRANSFERS` | `true` | Complete transfers through the library's asynchronous mode: the write is submitted with a handle and the task waits on the channel's completion descriptor, so no blocking thread is held per transfer. `false` keeps one blocking library call per transfer. |

Invalid settings or a server library failure stop startup. A node whose
endpoint host is not an RDMA-capable address logs `cuobject_service_state
state=skipped` and serves plain HTTP.

## Wire contract

Request: a normal SigV4-signed `GET` with `x-amz-content-sha256:
UNSIGNED-PAYLOAD`, a `Range` header covering the client buffer, and
`x-amz-rdma-token: <addr>:<size>:<rkey>:<lid>:<dctn>:<gidpresent>:<gid>`.

Response: the usual `GetObject` headers (ETag included) with an empty body
and `Content-Length: 0`, plus:

- `x-amz-rdma-reply`: `200` for a full object, `206` for a range (matches the
  HTTP status, which is `206` whenever a range was requested).
- `x-amz-rdma-bytes-transferred`: bytes written into the client buffer; absent
  when zero.

Writes are pipelined: chunk N is written by its own task while the decoder
fills chunk N+1; completions are collected in issue order. Each chunk is
owned by its blocking write until the DMA returns, so on a failure the GET
waits for the outstanding writes before their chunks return to the pool;
a staging slot is never reused while the NIC may still read it.

A malformed token is ignored (the GET is served over HTTP); the client treats
the missing reply header as "declined". A token buffer smaller than the
response is rejected with `InvalidRange`; RDMA write failures surface as
`InternalError` and nothing is sent over HTTP. Stage metrics record the
transfer under `s3_handler/rdma_transfer`.

## PUT and UploadPart over RDMA

A `PutObject` or `UploadPart` with an empty HTTP body and
`x-amz-rdma-token` (SigV4-signed, `x-amz-content-sha256: UNSIGNED-PAYLOAD`,
`Content-Length: 0`) takes its bytes from the client's registered buffer: the
object (or part) length is the token's buffer length, RustFS RDMA-reads it
into staging chunks one chunk ahead of the writer, and the bytes then go
through the ordinary PUT pipeline (checksums, erasure encode, fill-on-PUT
when enabled). The response is the usual one plus `x-amz-rdma-reply: 200`
and `x-amz-rdma-bytes-transferred`. A malformed token is ignored and the
empty body is stored as an empty object, exactly as without the feature;
the NIXL client treats a reply without `x-amz-rdma-reply` as declined.

## Validation

1. `RUSTFS_CUOBJECT_ENABLE=true RUSTFS_CUOBJECT_ADDRS=<roce ip>` on each node
   and confirm `cuobject_service_state state=started` in the log.
2. On the client, point `CUFILE_ENV_PATH_JSON` at a `cufile.json` whose
   `rdma_dev_addr_list` names the client's RDMA IP.
3. Run the SGLang loader with `--load-format rdma` and
   `"accelerated": true` in `--model-loader-extra-config`; the NIXL `OBJ`
   backend then advertises `VRAM_SEG` and every tensor arrives in GPU memory
   without an HTTP body.

The generic `x-amz-rdma-token` client lives in NIXL 1.6.0 (git `2ff2509f`,
2026-11-02) or newer. The published `nixl-cu13` 1.5.2 wheel ships only the
Dell vendor engine, so with it `accelerated=true` fails at GPU staging
registration; build NIXL from source with `aws-cpp-sdk-s3`, `aws-cpp-sdk-s3-crt`
and a `cuobjclient-<cuda>.pc` on `PKG_CONFIG_PATH`, then run with
`PYTHONPATH=<prefix>/lib/python3/dist-packages` and
`NIXL_PLUGIN_DIR=<prefix>/lib/<triplet>/plugins`.

### Results (four servers on RoCE, 2026-11-03)

Direct cuObject client (one GET of the 988 MB model file, RDMA write into
GPU memory, byte-exact against the HTTP body):

| Path | Throughput |
| --- | --- |
| RDMA GET into GPU memory | 2.1-2.2 GB/s |
| RDMA GET into pinned host memory | 2.2 GB/s |
| HTTP GET into host memory (same client) | 1.9-2.3 GB/s |

Both paths are bound by the single client link and the erasure-coded read
from four trays; the RDMA path removes the HTTP body copy and the host-to-GPU
copy. Inside the SGLang loader (NIXL, 256 MiB VRAM staging) the model loaded
at 3.0 GB/s; the error paths (undersized token buffer -> `InvalidRange`,
malformed token -> plain HTTP body) behave as described above.
