# Internode RDMA Simulator Transport

`rdma-sim` is an experimental internode data transport backend used to validate
whether `InternodeDataTransport` can host RDMA-like semantics. It is not a
production RDMA backend, does not use RDMA hardware, and does not link
`libibverbs`, `rdma-core`, DOCA, DPDK, SPDK, EFA, RoCE, or InfiniBand
libraries.

The default backend remains `tcp-http`. The simulator makes no performance
claim and should not be used as evidence of RDMA throughput or latency.

## Selection

The backend is compiled only with the `experimental-rdma-sim` Cargo feature.
The feature is not enabled by default.

| `RUSTFS_INTERNODE_DATA_TRANSPORT` | Feature disabled | Feature enabled |
| --- | --- | --- |
| unset or blank | `tcp-http` | `tcp-http` |
| `tcp-http` | `tcp-http` | `tcp-http` |
| `tcp` | `tcp-http` alias | `tcp-http` alias |
| `rdma-sim` | fails closed with an error naming `experimental-rdma-sim` | selects `rdma-sim` |
| `rdma` or `real-rdma` | fails closed | fails closed |
| unknown value | fails closed | fails closed |

The simulator does not silently fall back to `tcp-http`. The P2 fallback design
requires fallback to be explicit and observable before it becomes runtime
behavior.

## Supported Operation

The MVP operation is `read_file_stream`, implemented through
`InternodeDataTransport::open_read`.

The simulator uses the existing HTTP read route as the byte source so the spike
can validate the client-side transport boundary without changing the
server-side control plane, S3 API, gRPC surface, or disk handlers. Metrics are
recorded with `operation=read_file_stream` and `backend=rdma-sim`.

`put_file_stream` and `walk_dir` return clear unsupported-operation errors.
They are not delegated to `tcp-http`.

## Capabilities

`rdma-sim` reports capabilities that describe simulator semantics only:

| Capability | Value |
| --- | --- |
| backend name | `rdma-sim` |
| streaming read | `true` |
| streaming write | `false` |
| streaming walk-dir | `false` |
| zero-copy candidate | `true` |
| registered memory required | `true` |
| ordered delivery | `true` |
| max transfer size | none at the RustFS abstraction layer |
| fallback supported | `false` |

The `zero-copy candidate` flag means the abstraction can describe a future
registered-buffer data path. It does not mean this simulator avoids copies or
improves performance.

## Memory Registration Simulation

`read_file_stream` wraps receive-side reads with an internal registration
lifecycle:

| Type | Purpose |
| --- | --- |
| `SimulatedMemoryRegistry` | Tracks registered receive regions by simulated memory key. |
| `SimulatedMemoryRegion` | Records the region length accepted by the simulator. |
| `SimulatedMemoryKey` | Represents an opaque key that must match a registered region. |
| `RegisteredBufferHandle` | Carries the key and requested transfer length for validation. |

The simulator enforces that a receive region is registered before a simulated
operation completes. Validation fails if the key is invalid or the region has
already been unregistered. The registry does not pin memory, does not register
real DMA memory, and does not use unsafe RDMA APIs.

The current `read_file_stream` API does not accept caller-provided buffers, so
the simulator registers internal receive-side read buffers around `AsyncRead`
polls. A hardware-backed backend would need a stronger buffer ownership
contract before it can use caller-owned registered memory directly.

## Completion Simulation

The reader tracks a small internal completion state:

| State | Meaning |
| --- | --- |
| `Idle` | No simulated operation is active. |
| `Submitted` | A receive buffer has been registered and the inner stream is being polled. |
| `Completed` | The read completed successfully and the region was unregistered. |
| `Failed` | Registration, key validation, timeout, partial transfer, or completion failed. |

Fault injection is available only through test constructors. Covered modes
include registration failure, invalid memory key, completion error, timeout,
and partial transfer. Errors propagate through the existing `AsyncRead` /
`InternodeDataTransport` boundary rather than exposing low-level RDMA verbs.

## Observability

The simulator records internode operation metrics with low-cardinality labels:

| Metric behavior | Labels |
| --- | --- |
| outgoing read request | `operation=read_file_stream`, `backend=rdma-sim` |
| received bytes | `operation=read_file_stream`, `backend=rdma-sim` |
| simulator errors | operation-specific, `backend=rdma-sim` |

The implementation does not add object names, full URLs, full paths, or peer
identity strings as metric labels.

## Known Abstraction Gaps

| Area | Current state |
| --- | --- |
| Caller-owned buffers | `read_file_stream` returns `AsyncRead`; it does not let the caller provide registered receive buffers. |
| Server-side data plane | The simulator validates the client-side transport boundary and still uses the existing HTTP read handler as its byte source. |
| Write path | `put_file_stream` remains unsupported in `rdma-sim`, so send-buffer ownership is not validated here. |
| Walk-dir path | `walk_dir` remains unsupported because it is metadata-oriented and not the primary RDMA-like data path. |
| Runtime fallback | No automatic fallback is implemented for `rdma-sim`; selection failures and operation failures are explicit errors. |
| Hardware semantics | Pinning, memory registration lifetime across async tasks, queue pairs, completion queues, ordering guarantees from hardware, and peer negotiation are not modeled. |

These gaps mean a hardware-backed P3-B spike can be considered only as a
separate experiment behind its own feature gate and dependency boundary. It
would still need to keep `tcp-http` as the default, preserve correctness
semantics, and avoid claiming performance improvement without measured data.

## Validation

Default TCP/HTTP behavior:

```bash
cargo test -p rustfs-ecstore rpc::internode_data_transport
```

Simulator behavior:

```bash
cargo test -p rustfs-ecstore --features experimental-rdma-sim rpc::internode_data_transport
cargo test -p rustfs-ecstore --features experimental-rdma-sim rdma_sim
```

Formatting:

```bash
cargo fmt --check
```
