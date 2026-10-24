# Internode HTTP streaming transport

**Use this when:** large distributed reads stall, or you need to select the node-to-node HTTP protocol while retaining TLS and connection reuse.

The internode HTTP data plane defaults to pooled HTTP/1.1. Each active response has its own connection, so a paused shard body cannot exhaust HTTP/2 receive credit shared by other shard bodies. Bounded shard responses advertise their exact positive Content-Length so consuming the requested bytes completes HTTP framing without waiting for a chunked terminator. Requests with length zero retain the existing stream-to-EOF semantics. Completed responses can return their connections to the idle pool. TLS certificate verification, client identities, proxy policy, and certificate rotation use the existing client configuration.

This setting covers internode shard reads, writes, and directory walks. It does not change the public S3 listener, gRPC control channels, or outbound replication and tier S3 clients.

| Setting | Default | Effect |
|---|---|---|
| `RUSTFS_INTERNODE_HTTP_PROTOCOL` | `http1` | `http1` restricts the client to HTTP/1.1, including over TLS. `auto` restores the previous protocol negotiation: TLS ALPN can select HTTP/2; plaintext uses HTTP/1.1. Values are case insensitive and trimmed; missing or unknown values select `http1`. |
| `RUSTFS_INTERNODE_HTTP_POOL_MAX_IDLE_PER_HOST` | Existing tuning profile | Limits idle connections. `0` disables pooling entirely, including shared HTTP/2 connections. |
| `RUSTFS_INTERNODE_HTTP_POOL_IDLE_TIMEOUT_SECS` | Existing tuning profile | Evicts idle connections. It does not isolate concurrent HTTP/2 streams on an active connection. |

Set the protocol on every node and restart the processes. The node initiating a read, scan, or heal chooses the outbound protocol. With `http1`, retain the normal connection-pool settings; there is no need to set the maximum idle count to zero to isolate active bodies.

During a rolling upgrade, keep `RUSTFS_INTERNODE_HTTP_POOL_MAX_IDLE_PER_HOST=0` on older nodes that lack the protocol selector. Their coordinators, scanner and heal workers still choose the older outbound policy. After a node runs a release with the selector, use `http1` and restore its normal idle-pool setting. New response framing alone does not isolate streams initiated by an older HTTP/2 client.

HTTP/2 window and adaptive-window settings apply only with `auto` and a TLS connection that negotiates HTTP/2. The `legacy`, `balanced`, and `throughput` tuning profiles continue to control their existing pool and window settings; none overrides the protocol policy.

HTTP/1.1 uses more concurrent connections than multiplexed HTTP/2. Size file-descriptor and connection budgets for the workload. For bounded reads, consuming the full advertised byte count completes HTTP framing and permits reuse without an additional EOF poll. Unknown-length reads still require EOF. Cancelled or partially consumed responses can close their connections. A bounded response that ends early is a transport error; the server retains its existing read cap and never pads or shrinks a short response into success. Disabling pooling also adds TCP/TLS setup and connection churn to subsequent requests. No universal throughput improvement is implied by the protocol default.

For validation, compare complete 100 MiB and 1 GiB GETs through every coordinator, verify lengths and hashes, and include concurrent GET, scanner, and heal activity. Record latency, stall errors, CPU, connection counts, and file descriptors. Include small objects and uploads when assessing a deployment's performance. Local transport tests exercise TLS ALPN, shared HTTP/2 backpressure, full-body delivery, and HTTP/1.1 idle reuse; they do not replace a distributed cluster run.

The distributed E2E case below starts four processes with four drives each and a generated CA. Both S3 and volume endpoints use verified TLS. It streams 32 KiB, 100 MiB and 1 GiB uploads from files, checks complete and unaligned Range responses through every coordinator, and repeats full reads with all four coordinators active concurrently. Each GET checks Content-Length, Content-Range where applicable, the received byte count and SHA-256. SDK GET retries are disabled, and captured node logs must contain no internode body stalls.

```bash
export RUSTUP_TOOLCHAIN=stable
export NO_PROXY=127.0.0.1,localhost HTTP_PROXY= HTTPS_PROXY=
python3 scripts/e2e_binary.py build --features e2e-test-hooks
mkdir -p target/internode-acceptance
RUSTFS_E2E_INTERNODE_HTTP_MODE=http1 \
RUSTFS_INTERNODE_ACCEPTANCE_REPORT="$PWD/target/internode-acceptance/http1.json" \
python3 scripts/e2e_binary.py run --features e2e-test-hooks -- \
  cargo nextest run -p e2e_test --profile e2e-distributed \
  -E 'test(four_node_tls_large_and_range_gets_preserve_bytes)' --no-capture
```

Use an unused report filename for each invocation. The report has per-request timings and integrity results; adjacent files retain node logs and process IDs for external resource sampling. `RUSTFS_E2E_INTERNODE_HTTP_MODE=auto` selects pooled protocol negotiation, and `unpooled` selects the same negotiation with the idle pool disabled. Scanner and heal are enabled, but this case does not inject damage or certify their progress. A passing result does not complete mixed-version, mTLS or certificate-rotation acceptance.

These correctness timings are not a capacity benchmark: the client hashes every byte and shares a host with the four servers. For performance attribution, use the same release binary, topology and workload in isolated ABBA runs, gather CPU, FD, connections and TIME_WAIT externally, and reject candidate conclusions when the repeated baseline drifts. A few correctness requests cannot establish p99 or universal protocol superiority.

The mechanism follows [HTTP/2 flow control](https://www.rfc-editor.org/rfc/rfc9113.html#name-flow-control): stream and connection DATA credit both apply. In the client dependency, credit is released when the application polls body frames. Multiple paused bodies can therefore consume shared connection credit required by a stream the caller is still waiting for. Increasing a finite window or retrying the GET does not remove this coupling. Control frames and response headers are not subject to DATA credit.

S3 [GetObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html) defines the returned object body, content length, and byte-range semantics; it does not require HTTP/2 for internode transport. This policy changes the transport below the existing integrity, quorum, timeout, and identity-fenced resume behavior.
