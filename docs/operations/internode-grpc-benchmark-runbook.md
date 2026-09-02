# Internode gRPC A/B benchmark runbook

**Use this when:** collecting before/after evidence for an env-gated internode gRPC transport stage (P0 transport tuning, P1 channel isolation, P2 msgpack-only codec, P3 prewarm/offline bypass) on a real multi-node cluster.
**Source of truth:** `scripts/run_internode_grpc_ab_bench.sh` (stage/phase driver), `crates/config/src/constants/internode.rs` (`ENV_INTERNODE_*` / `DEFAULT_INTERNODE_*`), `crates/io-metrics/src/internode_metrics.rs` (metric names), [internode-msgpack-json-convergence-runbook.md](internode-msgpack-json-convergence-runbook.md) (the P2 gate).

Every stage is env-gated, so "before" and "after" run the *same binary* with different server env; no rebuild between runs. Live runs need a multi-node cluster (Docker compose or two or more endpoints), a load tool (`warp` or `s3bench`), and a metrics sink; they are not runnable in a single-process sandbox.

## Prerequisites

| Requirement | Detail |
| --- | --- |
| RPC secret | Internode RPC fails closed: remote endpoints with default credentials and no `RUSTFS_RPC_SECRET` abort startup with `store init aborted: endpoints include remote nodes but ...` (`crates/ecstore/src/store/init.rs`). Set a non-default `RUSTFS_RPC_SECRET`, identical on every node. |
| systemd start timeout | `deploy/build/rustfs.service` is `Type=notify` and ships `TimeoutStartSec=120s`; READY fires only after quorum. If freshly purged disks need longer, raise it in a drop-in rather than lowering it. |
| Metrics export | RustFS has no Prometheus pull endpoint (`/admin/v3/metrics` is NDJSON, see `rustfs/src/admin/handlers/metrics.rs`); it pushes OTLP. Run an otel-collector (OTLP receiver → Prometheus exporter) and set `RUSTFS_OBS_ENDPOINT`, `RUSTFS_OBS_METRICS_EXPORT_ENABLED=true`, `RUSTFS_OBS_METER_INTERVAL=5`. For lock p99 also set `RUSTFS_OBJECT_LOCK_DIAG_ENABLE=true` (default off). |
| Server env | `RUSTFS_INTERNODE_*` are **server** env. For p0/p1/p2, source the emitted `server-env.sh` on every node and restart before the run; the driver cannot mutate a running server. |

## Driver

```bash
scripts/run_internode_grpc_ab_bench.sh --stage <p0|p1|p2|p3> --phase <before|after|request-only|canary|rollback> [--dry-run] [-- <bench args>]
```

The driver writes the stage/phase server env to `<out-dir>/server-env.sh` and runs the underlying bench into `target/bench/internode-transport/<stage>-<phase>/`. `p0/p1/p3` accept `before|after`; `p2` also accepts `request-only|canary|rollback`. `--dry-run` prints the env and command only.

```bash
# P1 A/B: restart the cluster with each phase's server-env.sh between the two runs
scripts/run_internode_grpc_ab_bench.sh --stage p1 --phase before -- --access-key AK --secret-key SK --metrics-url http://node1:9000/metrics
scripts/run_internode_grpc_ab_bench.sh --stage p1 --phase after  -- --access-key AK --secret-key SK --metrics-url http://node1:9000/metrics
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
| P1 bulk pool | `RUSTFS_INTERNODE_BULK_CHANNELS` | `1` | unset (`DEFAULT_INTERNODE_BULK_CHANNELS`) or higher |
| P2 msgpack-only | `RUSTFS_INTERNODE_RPC_MSGPACK_ONLY` + `RUSTFS_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED` | both `false` (default) | per the convergence runbook |
| P3 prewarm | `RUSTFS_INTERNODE_PREWARM` | `false` (default) | `true` |
| P3 offline bypass | `RUSTFS_INTERNODE_OFFLINE_BYPASS` | `false` (default) | `true` |
| P3 reprobe / threshold | `RUSTFS_INTERNODE_OFFLINE_REPROBE_SECS` / `RUSTFS_INTERNODE_OFFLINE_FAILURE_THRESHOLD` | defaults | defaults (`DEFAULT_INTERNODE_OFFLINE_REPROBE_SECS`, `DEFAULT_INTERNODE_OFFLINE_FAILURE_THRESHOLD`) |

## Metrics to capture

All names are defined in `crates/io-metrics/src/internode_metrics.rs`.

| Metric | Stage signal |
| --- | --- |
| `rustfs_system_network_internode_operation_duration_ms{operation,backend}` | control-plane RTT (P0), lock/bulk latency (P1), first-op latency (P3) |
| `rustfs_system_network_internode_operation_payload_bytes` | payload size distribution (P0/P1 sizing) |
| `rustfs_system_network_internode_operation_large_payloads_total` | large unary RPCs sharing a channel (P1 target) |
| `rustfs_system_network_internode_dial_avg_time_nanos`, `rustfs_system_network_internode_dial_errors_total` | connect cost and failures (P3) |
| `rustfs_system_network_internode_msgpack_json_decode_total{direction,message,codec}`, `..._msgpack_json_fallback_total`, `..._msgpack_json_decode_error_total` | P2 gate inputs |
| `rustfs_cluster_servers_offline_total` | offline detection correctness (P3 bypass) |
| lock p99 (lock metrics, needs `RUSTFS_OBJECT_LOCK_DIAG_ENABLE=true`) | P1 head-of-line-blocking win |

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
