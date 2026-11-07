# Internode gRPC A/B benchmark runbook

**Use this when:** collecting before/after evidence for an env-gated internode gRPC transport stage (P0 transport tuning, P1 channel isolation, P2 msgpack-only codec, P3 prewarm/offline bypass) on a real multi-node cluster.
**Source of truth:** `scripts/run_internode_grpc_ab_bench.sh` (stage/phase driver), `crates/config/src/constants/internode.rs` (`ENV_INTERNODE_*` / `DEFAULT_INTERNODE_*`), `crates/io-metrics/src/internode_metrics.rs` (metric names), [internode-msgpack-json-convergence-runbook.md](internode-msgpack-json-convergence-runbook.md) (the P2 gate).

Every stage is env-gated, so "before" and "after" run the *same binary* with different server env; no rebuild between runs. Live runs need a multi-node cluster (Docker compose or two or more endpoints), a load tool (`warp` or `s3bench`), and a metrics sink; they are not runnable in a single-process sandbox.

## Prerequisites

| Requirement | Detail |
| --- | --- |
| RPC secret | Internode RPC fails closed: remote endpoints with default credentials and no `RUSTFS_RPC_SECRET` abort startup with `store init aborted: endpoints include remote nodes but ...` (`crates/ecstore/src/store/init.rs`). Set a non-default `RUSTFS_RPC_SECRET`, identical on every node. |
| systemd start timeout | `deploy/build/rustfs.service` is `Type=notify` and ships `TimeoutStartSec=120s`; READY fires only after quorum. If freshly purged disks need longer, raise it in a drop-in rather than lowering it. |
| Metrics export | RustFS has no Prometheus pull endpoint (`/admin/v3/realtime` is NDJSON, see `rustfs/src/admin/handlers/realtime.rs`); it pushes OTLP. Run an otel-collector (OTLP receiver → Prometheus exporter) and set `RUSTFS_OBS_ENDPOINT`, `RUSTFS_OBS_METRICS_EXPORT_ENABLED=true`, `RUSTFS_OBS_METER_INTERVAL=5`. For lock p99 also set `RUSTFS_OBJECT_LOCK_DIAG_ENABLE=true` (default off). |
| Server env | `RUSTFS_INTERNODE_*` are **server** env. For p0/p1/p2, source the emitted `server-env.sh` on every node and restart before the run; the driver cannot mutate a running server. |

## Driver

```bash
scripts/run_internode_grpc_ab_bench.sh --stage <p0|p1|p2|p3> --phase <before|after|request-only|canary|rollback> [--dry-run] [-- <bench args>]
```

The driver writes the stage/phase server env to `<out-dir>/server-env.sh` and runs the underlying bench into `target/bench/internode-transport/<stage>-<phase>/`. `p0/p1/p3` accept `before|after`; `p2` also accepts `request-only|canary|rollback`. Each phase first unsets its managed settings, then exports its overrides, so successive phases restore defaults correctly. Other environment variables retain their values. `--dry-run` writes the phase file and prints the command while skipping the workload. Apply the phase file through the deployment's service environment and read back the effective settings on every restarted node.

For P3 single-variable experiments, select `--lifecycle-feature prewarm` or `--lifecycle-feature offline`. The default `both` preserves the combined experiment. Measure prewarm with cold first calls and delayed peer startup, and measure offline bypass with sustained peer loss and survivor-only client traffic. Record the selected feature with each result.

Set `GRPC_BENCH_METRICS_URL` to the Collector Prometheus exporter endpoint. Generate each phase with `--dry-run`, apply its environment through the node service configuration, restart and verify all nodes, then execute the matching workload command.

```bash
# P1 A/B: restart the cluster with each phase's server-env.sh between the two runs
scripts/run_internode_grpc_ab_bench.sh --stage p1 --phase before -- --access-key AK --secret-key SK --metrics-url "${GRPC_BENCH_METRICS_URL:?set the Collector Prometheus exporter URL}"
scripts/run_internode_grpc_ab_bench.sh --stage p1 --phase after  -- --access-key AK --secret-key SK --metrics-url "${GRPC_BENCH_METRICS_URL:?set the Collector Prometheus exporter URL}"
# P3 failover A/B (docker four-node)
scripts/run_internode_grpc_ab_bench.sh --stage p3 --phase after
# P2 env previews
scripts/run_internode_grpc_ab_bench.sh --stage p2 --phase canary --dry-run
```

Underlying benches: `scripts/run_internode_transport_baseline.sh` (throughput/latency via `scripts/run_object_batch_bench.sh`; `--metrics-url <prometheus>` also captures internode metric deltas) and `scripts/run_four_node_cluster_failover_bench.sh` (4-node compose cluster, kills `FAILOVER_NODE`). `target/` is gitignored; attach the paired directories to the PR or issue.

## Per-stage env matrix

Run **before** at the baseline column and **after** at the enabled column, everything else at defaults, with a restart between. Defaults are the `DEFAULT_INTERNODE_*` constants in `crates/config/src/constants/internode.rs`.

| Stage | Env | before | after |
| --- | --- | --- | --- |
| P0 nodelay | `RUSTFS_INTERNODE_RPC_TCP_NODELAY` | `false` | unset (`DEFAULT_INTERNODE_RPC_TCP_NODELAY`) |
| P0 stream window | `RUSTFS_INTERNODE_RPC_HTTP2_STREAM_WINDOW_SIZE` | `0` | unset (`DEFAULT_INTERNODE_RPC_HTTP2_STREAM_WINDOW_SIZE`) |
| P0 conn window | `RUSTFS_INTERNODE_RPC_HTTP2_CONN_WINDOW_SIZE` | `0` | unset (`DEFAULT_INTERNODE_RPC_HTTP2_CONN_WINDOW_SIZE`) |
| P0 msg limit | `RUSTFS_INTERNODE_RPC_MAX_MESSAGE_SIZE` | `4194304` (tonic default) | unset (RustFS default, see `rustfs/src/server/http.rs`) |
| P1 isolation | `RUSTFS_INTERNODE_CHANNEL_ISOLATION` | `false` (default) | `true` |
| P1 bulk pool | `RUSTFS_INTERNODE_BULK_CHANNELS` | unset | `2` |
| P2 msgpack-only | `RUSTFS_INTERNODE_RPC_MSGPACK_ONLY` + `RUSTFS_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED` | both `false` (default) | per the convergence runbook |
| P3 prewarm | `RUSTFS_INTERNODE_PREWARM` | `false` (default) | `true` |
| P3 offline bypass | `RUSTFS_INTERNODE_OFFLINE_BYPASS` | `false` (default) | `true` |
| P3 reprobe / threshold | `RUSTFS_INTERNODE_OFFLINE_REPROBE_SECS` / `RUSTFS_INTERNODE_OFFLINE_FAILURE_THRESHOLD` | defaults | defaults (`DEFAULT_INTERNODE_OFFLINE_REPROBE_SECS`, `DEFAULT_INTERNODE_OFFLINE_FAILURE_THRESHOLD`) |

## Metrics to capture

The lifetime guards cover ReadAll, WriteAll, ReadMultiple and BatchReadVersion. Their active/completion metrics describe these measured operations and follow the general metrics runtime gate. Startup duration recording keeps the existing instrument, and disabled detailed metrics avoid the additional gauge/counter registrations. Bulk client unary duration covers each attempt from polling the tonic call through response-body decoding, including transport errors and cancellation by an outer deadline. Retries contribute separate samples. Channel acquisition and application serialization before the call require their own timing. The server handler duration covers handler entry through return or drop, including application payload decoding, disk work and response construction. Protobuf transport encoding and response transmission follow handler completion. Keep these two instruments separate when calculating quantiles.

All names are defined in `crates/io-metrics/src/internode_metrics.rs`.

| Metric | Stage signal |
| --- | --- |
| `rustfs_system_network_internode_operation_duration_ms{operation,backend}` | control-plane RTT (P0), lock/bulk latency (P1), first-op latency (P3) |
| `rustfs_system_network_internode_operation_server_duration_ms{operation,backend}` | server handler time for ReadAll, WriteAll, ReadMultiple and BatchReadVersion |
| `rustfs_system_network_internode_rpc_active{operation,backend,side}` | live client attempts or server handlers, including pending unary response bodies on the client |
| `rustfs_system_network_internode_rpc_completions_total{operation,backend,outcome}` | client attempt outcomes `success`, `error` and `cancelled`. An outer timeout cancels an attempt, while a returned transport timeout is an error |
| `rustfs_system_network_internode_operation_payload_bytes` | payload size distribution (P0/P1 sizing) |
| `rustfs_system_network_internode_operation_large_payloads_total` | large unary RPCs sharing a channel (P1 target) |
| `rustfs_system_network_internode_dial_avg_time_nanos`, `rustfs_system_network_internode_dial_errors_total` | connect cost and failures (P3) |
| `rustfs_system_network_internode_msgpack_json_decode_total{direction,message,codec}`, `..._msgpack_json_fallback_total`, `..._msgpack_json_decode_error_total` | P2 gate inputs |
| `rustfs_cluster_servers_offline_total` | offline detection correctness (P3 bypass) |
| lock p99 (lock metrics, needs `RUSTFS_OBJECT_LOCK_DIAG_ENABLE=true`) | P1 head-of-line-blocking win |

Verify `RUSTFS_OBS_METRICS_EXPORT_ENABLED=true` and that the Collector exports the intended node/process identities. The client `success` outcome denotes an `Ok` tonic exchange. Interpret payload-level success using the returned operation result and operation error counters. RPC duration buckets use milliseconds and resolve sub-millisecond samples; object-lock duration buckets use seconds. The active-client metric counts the complete measured unary future, while tonic's transport concurrency limit ends when its HTTP response future completes. The two counts have different lifetimes. `BatchReadVersion` traffic requires the batch gate, such as `RUSTFS_GET_METADATA_READ_VERSION_COALESCE=on` on compatible peers; verify actual batch request counts and keep that setting identical across A/B legs.

## Isolated bulk tuning

These settings apply when `RUSTFS_INTERNODE_CHANNEL_ISOLATION=true` selects an isolated bulk cache key. Control and dedicated recovery channels use their existing transport settings. Defaults preserve the existing transport behavior.

P1 phase files clear bulk-specific overrides so an isolation experiment inherits the same common RPC window policy on both legs. For a separate bulk-tuning experiment, apply the chosen override after the phase file and include it in the effective configuration record.

| Environment variable | Default | Scope |
| --- | --- | --- |
| `RUSTFS_INTERNODE_BULK_HTTP2_STREAM_WINDOW_SIZE` | Inherit `RUSTFS_INTERNODE_RPC_HTTP2_STREAM_WINDOW_SIZE` | Bulk response receive credit per stream, bytes |
| `RUSTFS_INTERNODE_BULK_HTTP2_CONN_WINDOW_SIZE` | Inherit `RUSTFS_INTERNODE_RPC_HTTP2_CONN_WINDOW_SIZE` | Bulk response receive credit per connection, bytes |
| `RUSTFS_INTERNODE_BULK_CONCURRENCY_LIMIT` | `64` | Active tonic transport response futures per bulk channel; minimum 1 |
| `RUSTFS_INTERNODE_BULK_BUFFER_SIZE` | `1024` | Pending tonic service buffer capacity per bulk channel; minimum 1 |

Zero receive-window values select the linked library default. In the locked Hyper 1.12 client this is 2 MiB per stream and 5 MiB per connection, while RustFS explicitly defaults to 1 MiB and 2 MiB. Record which policy the experiment actually selects. Values above the HTTP/2 31-bit limit fail channel acquisition with an invalid-input error. Change connection receive credit first, then stream credit only when measured bandwidth-delay product and resource budgets justify it. For example, compare the default 2 MiB connection window with `RUSTFS_INTERNODE_BULK_HTTP2_CONN_WINDOW_SIZE=8388608`, holding other settings fixed. Request-upload flow control is configured by the receiving server. Keep server-wide window changes in a separate experiment.

Bulk selection advances independently for each peer. Cursor bookkeeping caches up to 512 peer addresses; replacing an entry restarts its selection sequence while cached connection keys remain usable. HTTPS cache hits share the TLS-generation read lock, with generation validation retained before handoff. Connection misses still coalesce under the existing per-key dial lock.

## Local transport experiment

The [channel benchmark](../../crates/protos/examples/internode_channel_bench.rs) exercises the production channel acquisition path against two local tonic echo peers. It validates every returned byte and reports per-class request percentiles, bulk payload throughput and accepted connections per peer. An optional fourth argument selects control workers that continuously send 7-byte RPCs until the bulk workers complete. This is a closed-loop concurrency workload. Record achieved request rates and use fixed offered-load tests for the final control-plane SLO assessment. It bounds the requested workload and binds only to loopback. Its scope is the channel/HTTP2/TCP path. Distributed storage acceptance additionally requires the real four-node workload and fault matrix.

```bash
cargo build --release -p rustfs-protos --example internode_channel_bench
RUSTFS_INTERNODE_CHANNEL_ISOLATION=false target/release/examples/internode_channel_bench 16 256 262144 2
RUSTFS_INTERNODE_CHANNEL_ISOLATION=true RUSTFS_INTERNODE_BULK_CHANNELS=2 target/release/examples/internode_channel_bench 16 256 262144 2
```

Freeze the binary and environment, qualify repeated A baselines, then run A1/B1/B2/A2. Interpret payload throughput as one payload per validated round trip; the echo transfers that payload in both directions. Keep source revision, binary identity, workload arguments, connection counts, all errors and raw samples with the results. Stop attribution when baseline drift exceeds the declared gate. Use the effective four-node deployment configuration for final acceptance, with metadata, locks, multipart, TLS rotation and peer failure covered.

## Acceptance gates

Record the verdict (pass/fail plus measured delta) in the paired directory's summary.

| Stage | Bench | Gate | Primary metrics |
| --- | --- | --- | --- |
| P0 | `run_internode_transport_baseline.sh --sizes 4KiB,1MiB,16MiB,128MiB --concurrencies 1,16,64` | small-RPC `duration_ms` (DiskInfo/Ping) down; large-metadata (ReadMultiple/BatchReadVersion) throughput up; a `>4 MiB` multi-version `xl.meta` no longer fails `out_of_range` | `..._operation_duration_ms{operation}`, `..._operation_payload_bytes`, object-bench throughput |
| P1 | `run_internode_transport_baseline.sh` with a mixed workload (large `ReadAll` plus high-frequency `Refresh`) | lock p99 down by at least 20% with `RUSTFS_INTERNODE_CHANNEL_ISOLATION=true` | lock p99, `..._operation_large_payloads_total` |
| P2 | none (not a throughput gate) | operational gate defined once in [internode-msgpack-json-convergence-runbook.md](internode-msgpack-json-convergence-runbook.md): expected `codec="msgpack"` decode series non-zero, fallback and decode-error series zero across a full observation window before both flags are enabled. Optionally a `dhat`/`heaptrack` micro-run for codec allocation. | the three `msgpack_json_*` counters |
| P3 cold-start | `run_internode_transport_baseline.sh` on a fresh cluster, first cross-node op | first cross-node op latency drops by about one connect RTT with prewarm | `..._dial_avg_time_nanos`, first-op `..._operation_duration_ms` |
| P3 offline | sustained-offline plus survivor cross-node access (below) | with bypass on, survivor ops to the downed peer fast-fail instead of hanging for the dial timeout; `rustfs_cluster_servers_offline_total` is `1` while down and `0` after recovery | `rustfs_cluster_servers_offline_total`, survivor `..._operation_duration_ms`, `..._dial_errors_total` |

P3 offline method: the standard four-node failover bench is not sensitive to the bypass (quorum holds, `recovery_seconds=0`). Instead: all nodes up → stop one node and keep it down → warm up until offline detection trips → drive warp against the survivors only (`--host` excludes the dead node) → compare survivor op p99 and the offline gauge with `RUSTFS_INTERNODE_OFFLINE_BYPASS` off and on.

Artifact layout:

```text
target/bench/internode-transport/
  p0-before/  p0-after/                                            # server-env.sh + object-bench summaries + metric deltas
  p1-before/  p1-after/                                            # + lock p99 delta
  p2-before/  p2-request-only/  p2-canary/  p2-after/  p2-rollback/ # env previews + counter observations
  p3-before/  p3-after/                                            # cold-start + sustained-offline + offline gauge trace
```

## Rollback

Every stage rolls back by restoring the baseline column and restarting. P2 rollback and its wire-format guarantees are in the [convergence runbook's rollback matrix](internode-msgpack-json-convergence-runbook.md#rollback-matrix); do not remove or reuse JSON proto fields as part of any benchmark stage.
