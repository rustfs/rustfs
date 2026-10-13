# Native Raft transport

**Use this when:** mounting authenticated capture Raft RPCs, configuring its network factory, or investigating message, replay and lifecycle limits.

`crates/ecstore/src/cluster/rpc/capture_raft.rs` implements the production `CaptureRaftNetworkFactory` and `CaptureRaftRpc`. Capture authority remains independent of ECStore. `NativeRaftOwner::identity` returns the identity read from durable META before that owner started; the server derives its cluster/node identity from this accessor. Factory/server constructors validate explicit fixed members and addresses. A network client accepts the target node metadata only when it exactly matches that member map. Vote/append/snapshot leader identity must match the authenticated body's source node; the destination and cluster must match the actual owner.

## Mount and authentication

`CaptureRaftRpc::into_service` returns a bounded generated tonic service for mounting beside existing `NodeService/Ping`. Authentication occurs inside each Raft handler exactly once. Future RustFS mounting must not also apply the old HTTP `check_auth` interceptor, which would consume the same nonce twice. This slice supplies no standalone production daemon or startup integration.

`crates/ecstore/src/cluster/rpc/client.rs` provides the narrow capture client helper. It reuses existing channel/TLS/cache configuration, signature interceptor and `AuthenticatedChannel`. Every exchange performs an actual authenticated Ping challenge/proof handshake before dispatch. The existing peer epoch/capability cache is learned from that response, never prefilled. Missing or revoked authenticated capability rejects the call.

All three Raft methods require the existing v3 boot-scoped header before using `verify_tonic_rpc_signature_with_bootstrap(false)` and strict canonical body-digest verification. This does not depend on compatibility switches enabling strictness, and has no v1/unsigned fallback. The existing nonce cache rejects replay and the server's process epoch rejects old-boot signatures. The server returns the existing boot epoch/capability proof headers with successful and actual Raft error replies, preserving the authenticated channel's peer state. Rejections count through the existing internode authentication metric; no new credential protocol or registry is introduced. This uses RustFS's shared internal RPC credential trust, not individual peer certificates or plugin-runner service mTLS.

## Wire contract and errors

`crates/protos/src/node.proto` adds only `CaptureRaftService` and opaque canonical request/response bodies plus a response proof. The version-1 JSON envelope strictly binds cluster, source, target, method, fresh request nonce and the complete typed payload. Input byte bounds precede parsing. Typed re-encoding must exactly match the received bytes, rejecting unknown fields, duplicate/noncanonical fields and trailing data. Nested payloads use `RawValue` and the same typed serialization on both ends; they never pass through a string-sorted generic JSON map. Existing command and explicit capture membership validation still applies.

The response proof reuses the existing internode HMAC helper over a domain-separated, length-delimited complete original request and complete result. It therefore includes the original nonce, payload/digest semantics, cluster/target and method. Changed results, old replies and cross-request reuse fail verification before returning a Raft result.

Success values retain actual OpenRaft responses. Remote stopped, panicked, storage-I/O, defensive-storage and snapshot-mismatch classes remain typed `RPCError::RemoteError`; transport rejection and timeout cannot become success. Original storage error strings and backtraces are never sent. A bounded local serde projection preserves safe I/O subject/verb when available; if the projection exceeds its 16-KiB budget, the wire marks that operation unavailable and the local reconstruction uses a redacted generic I/O context. Defensive errors retain their typed subject/violation without backtrace.

## Limits and lifecycle

All members must use consistent message limits; heterogeneous receiver limits are not negotiated. Defaults are 16 MiB request body, 1 MiB response body, 300 append entries, 3 MiB snapshot chunk and 16 concurrent admitted calls per factory/server. Connect timeout is three seconds and configured request timeout five seconds. Constructors reject invalid/overflowing limits and Raft batch/chunk configurations that exceed them. Snapshot capacity accounts for worst-case four JSON bytes per raw byte plus header allowance. The append count matches OpenRaft's default 300; no entries are silently truncated. If a multi-entry encoded request exceeds the byte limit, `PayloadTooLarge` gives OpenRaft a smaller count hint so it can retry a complete prefix. A single entry that cannot fit is explicitly rejected.

Bounded encoding uses checked lengths and fallible growth. Each exchange's timeout includes admission, encoding, handshake and RPC, capped by `RPCOption::soft_ttl()` before its hard deadline. The server holds its admission permit in an owned task through the actual Raft future and signed reply; cancelling an HTTP waiter does not release that permit or abort the queued Raft operation. Such cancellation has an unknown remote outcome, not a success guarantee.

Keep a `CaptureRaftRpc` clone to manage mounting. Its `shutdown` stops new admission and waits for all actual accepted tasks by acquiring the full permit set; cancellation can be followed by another shutdown wait. Stop/await the tonic listener and then shut down the actual Raft owner and release external references before reopening the same redb. This does not replace the owner's accepted SM/builder/replication drain or source-reader shutdown contract.

## Verification and remaining work

`crates/ecstore/src/cluster/rpc/capture_raft_tests.rs` uses three real TCP listeners, the production factory and service, three actual Raft instances and real redb. Its minimal Ping route calls production signing/boot-proof functions. A frame-level test fault wrapper alters only delivery/proofs; it does not replace the Raft handler or manually apply decisions. Tests cover majority, failover, real snapshot RPC catch-up, identity/authentication/replay, remote errors, size/deadline/shutdown, byte-limit batch splitting and cancelled HTTP admission. The worker pause test proves ownership of accepted service work before actual Raft dispatch, not a simulated redb commit.

Production RustFS startup mounting and S3 automatic capture are later slices. No existing service/authentication behavior or capture storage format is replaced. The generated protobuf remains additive; older peers without this service cannot join this capture Raft transport through a downgrade fallback.
