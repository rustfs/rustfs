# Native Raft owner

**Use this when:** starting the capture authority's actual Raft lifecycle, submitting native decisions, or wiring a shared source reader.

`crates/capture-authority/src/raft_owner.rs` provides `NativeRaftOwner::start(writer, node_id, config, limits, network)`. It validates OpenRaft configuration, snapshot limits, writable native application state, capture binding and the runtime ID against durable META identity before calling real OpenRaft 0.9.25 `Raft::new`. The log and state-machine adapters receive clones of the same `NativeLogWriter`; neither opens another live database. Start never initializes membership, clears storage, or generates a replacement identity.

## Formation, writes and progress

`bootstrap` is an explicit first-cluster operation. Its member map must contain the durable local node, and every `CaptureNode` address must pass the existing validator. It then invokes actual `Raft::initialize`. The caller is responsible for consistent initial membership across nodes; start on a recovered database must not bootstrap it again. OpenRaft's concrete initialization failure is preserved.

`client_write` submits the existing typed `CaptureCommand` to actual Raft. Its `ClientWriteResponse<CaptureTypeConfig>` contains the real consensus entry LogId and native application response. The first decision identity inside an idempotent Created response can precede the outer Raft retry entry; successful retries do not create another authoritative event.

`NativeRaftOwnerError` retains concrete OpenRaft write, initialization and Fatal errors, including ForwardToLeader. `metrics` returns the existing watch receiver with leader, applied and fatal state. Lack of a leader, loss of majority, caller timeout or transport failure is not a committed-success result. An interrupted request can have an unknown outcome: preserve the same operation identity when retrying. The owner does not manufacture a success response from local staging or direct apply.

## Reader and shutdown ownership

On Linux and macOS, `bind_reader` binds the accepted `SourceReaderServer` with this same writer. It returns the server and shutdown signal; the caller owns its serve task. Stop that reader and await its drain before releasing storage. The owner does not claim to drain a server task it does not own.

`shutdown` stops admission to new owner bootstrap/write/reader-binding calls, then invokes and awaits real `Raft::shutdown`. OpenRaft 0.9.25 joins its core and tick, but does not join its state-machine worker or detached snapshot builders. A shared lifetime lease in the actual native state machine, its builder clones and the owner-created log storage/replication readers therefore extends shutdown until their final real drop, after their writer/Store fields have been released. A cancelled asynchronous replication read also carries the lease into its real blocking worker until that worker releases Store; external writer clones are not attached to this owner lease. The completion state survives cancelled shutdown waiters; notification is registered before checking it. Calls racing the admission boundary can already be in Raft; only their actual returned result proves success. Ordinary async cancellation preserves the previously accepted storage adapters' blocking commit ownership. Shutdown also waits for owner-created replication readers. An injected network that never finishes its RPC can therefore delay shutdown; this owner supplies no default transport deadline. A production factory must honor the OpenRaft RPC timeout contract. Dropping the owner is not an async shutdown barrier; a cancelled shutdown waiter should call shutdown again and await completion.

`raft` exposes the existing handle for explicit transport registration. Raw handle clones and the original writer are trusted internal caller-owned references; this owner type does not seal every bypass. Do not manually append or apply through writer clones while Raft is running. Before reopening the same redb file, shut down Raft and the reader and release external writer, Raft and factory references. Shutdown does not revoke independent read handles or delete EVENTS.

## Transport boundary and verification

A `RaftNetworkFactory<CaptureTypeConfig>` is mandatory. This module has no default or production network implementation and does not automatically connect to existing RustFS RPC. Authentication, cluster/member/target binding, signed bodies, replay protection, snapshot transfer framing and transport timeouts remain subsequent production work. Factory implementers must use the accepted snapshot receive path and its bounded handles.

`crates/capture-authority/src/raft_owner_tests.rs` uses real Raft instances and real redb log/state-machine adapters. Its direct RPC factory is only a test transport: it forwards append, vote and snapshot calls to actual peer Raft handles. Tests cover explicit bootstrap, restart without initialize, retry identity, runtime identity rejection, live shared reader, shutdown, three-node majority, leader failover and snapshot catch-up through actual Raft RPC. Readiness comes from metrics and authoritative events, rather than a fixed sleep.

These tests establish the internal owner and consensus path under that test transport. They do not prove production network trust or wire native capture into S3 PUT, Copy or multipart publication. S06 remains blocked, R12 remains frozen, and production S3 automatic capture remains a later integration task. Removing this lifecycle adapter does not change the accepted native storage/snapshot formats; retain the selected durable baseline after prefix purge and follow its existing empty-baseline rollback restrictions.
