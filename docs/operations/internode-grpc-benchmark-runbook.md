# Internode gRPC Optimization — A/B Benchmark Runbook

Reproducible procedure to collect **before/after** artifacts for each internode gRPC
optimization stage (grpc-optimization P0–P3). Every stage is env-gated, so "before" and
"after" are the *same binary* with different env — no rebuild between runs.

> Live runs need a multi-node cluster (Docker or ≥2 rustfs endpoints), a load tool
> (`warp` or `s3bench`), and a Prometheus scrape of `/metrics`. They are not runnable in a
> single-process sandbox. Capture artifacts on a real cluster.

## One-click driver

`scripts/run_internode_grpc_ab_bench.sh --stage <p0|p1|p2|p3> --phase <before|after> [-- <bench args>]`
wraps the env matrix below: it writes the stage/phase **server** env to
`<out-dir>/server-env.sh`, then runs the right underlying bench into
`target/bench/internode-transport/<stage>-<phase>/`.

```bash
# P1 A/B (restart the cluster with each phase's server-env.sh between the two runs):
scripts/run_internode_grpc_ab_bench.sh --stage p1 --phase before -- --access-key AK --secret-key SK --metrics-url http://node1:9000/metrics
scripts/run_internode_grpc_ab_bench.sh --stage p1 --phase after  -- --access-key AK --secret-key SK --metrics-url http://node1:9000/metrics
# P3 failover A/B (docker four-node):
scripts/run_internode_grpc_ab_bench.sh --stage p3 --phase after
```

`RUSTFS_INTERNODE_*` are **server** env: for the load-driven stages (p0/p1/p2) source the emitted
`server-env.sh` on every node and restart rustfs *before* the run — the driver cannot mutate an
already-running server. Use `--dry-run` to preview the env and command.

## Harness

- Throughput / latency: `scripts/run_internode_transport_baseline.sh` (drives
  `run_object_batch_bench.sh`; writes `target/bench/internode-transport-<ts>/`). Pass
  `--metrics-url <prometheus>` to also capture internode metric deltas.
- Failover / offline: `scripts/run_four_node_cluster_failover_bench.sh` (spins up a 4-node
  compose cluster, kills `FAILOVER_NODE`, benchmarks; writes
  `target/bench/four-node-failover-<ts>/`).

Store the paired artifacts as `target/bench/internode-transport/{baseline,after-P0,after-P1,after-P3}/`
(the paths the design docs reference). `target/` is gitignored — attach the artifacts to the PR.

## Metrics to capture (Prometheus)

| Metric | Stage signal |
|---|---|
| `rustfs_system_network_internode_operation_duration_ms{operation,backend}` | control-plane RTT (P0), lock/bulk latency |
| `rustfs_system_network_internode_operation_payload_bytes` | payload size distribution (P0/P1 sizing) |
| `rustfs_system_network_internode_operation_large_payloads_total` | large unary RPCs sharing the channel (P1 target) |
| `rustfs_system_network_internode_dial_avg_time_nanos`, `..._dial_errors_total` | connect cost (P3 prewarm) |
| `rustfs_system_network_internode_msgpack_json_fallback_total{direction,message}` | must be **0** before enabling msgpack-only (P2) |
| `rustfs_cluster_servers_offline_total` | offline detection correctness (P3 bypass) |
| lock p99 (lock metrics) | P1 head-of-line-blocking win |

## Per-stage env matrix

Run **before** with the stage's env at its baseline column, **after** with the enabled
column, everything else at defaults. Roll a restart between runs.

| Stage | Env | before (baseline) | after (enabled) |
|---|---|---|---|
| P0 nodelay | `RUSTFS_INTERNODE_RPC_TCP_NODELAY` | `false` | `true` (default) |
| P0 stream window | `RUSTFS_INTERNODE_RPC_HTTP2_STREAM_WINDOW_SIZE` | `0` | unset (1 MiB) |
| P0 conn window | `RUSTFS_INTERNODE_RPC_HTTP2_CONN_WINDOW_SIZE` | `0` | unset (2 MiB) |
| P0 msg limit | `RUSTFS_INTERNODE_RPC_MAX_MESSAGE_SIZE` | `4194304` | unset (100 MiB) |
| P1 isolation | `RUSTFS_INTERNODE_CHANNEL_ISOLATION` | `false` (default) | `true` |
| P1 bulk pool | `RUSTFS_INTERNODE_BULK_CHANNELS` | `1` | `2`–`4` |
| P2 msgpack-only | `RUSTFS_INTERNODE_RPC_MSGPACK_ONLY` | `false` (default) | `true` (only after fallback counter = 0 across a window) |
| P3 prewarm | `RUSTFS_INTERNODE_PREWARM` | `false` (default) | `true` |
| P3 offline bypass | `RUSTFS_INTERNODE_OFFLINE_BYPASS` | `false` (default) | `true` |
| P3 reprobe / threshold | `RUSTFS_INTERNODE_OFFLINE_REPROBE_SECS` / `RUSTFS_INTERNODE_OFFLINE_FAILURE_THRESHOLD` | defaults | `5` / `3` |

## Procedure per stage

1. **Baseline**: start the cluster with the stage's env at the *before* column. Run the
   relevant bench; save to `.../baseline/` (or `.../after-P{n-1}/` when chaining stages).
2. **After**: restart with the *after* column; re-run the identical bench; save to
   `.../after-P{n}/`.
3. Diff the object-bench summaries and the metric deltas.

- **P0** — `run_internode_transport_baseline.sh` with `--sizes 4KiB,1MiB,16MiB,128MiB` and
  `--concurrencies 1,16,64`. Expect: small-RPC `duration_ms` (DiskInfo/Ping) down (nodelay),
  large-metadata (ReadMultiple/BatchReadVersion) throughput up (windows). Functional: a
  `>4 MiB` multi-version `xl.meta` no longer fails `out_of_range`.
- **P1** — mixed workload (large `ReadAll` + high-frequency `Refresh`). Acceptance gate from
  the design doc: **lock p99 down ≥ 20%** with `RUSTFS_INTERNODE_CHANNEL_ISOLATION=true`.
- **P2** — observe `msgpack_json_fallback_total` across a release window; it must stay **0**
  before flipping `RUSTFS_INTERNODE_RPC_MSGPACK_ONLY=true` (see the msgpack convergence
  runbook). Codec allocation via a `dhat`/`heaptrack` micro-run.
- **P3** — cold-start: first cross-node op latency should drop ~one connect RTT with prewarm.
  Failover: `run_four_node_cluster_failover_bench.sh`, kill a node with
  `RUSTFS_INTERNODE_OFFLINE_BYPASS=true`; expect faster failover and a correct
  `rustfs_cluster_servers_offline_total` (1 while the node is down, back to 0 after recovery).

## Rollback

Every stage is a single-env rollback (set the env back to its baseline column and restart).
No wire-format is broken except P2 stage 2 (proto field removal), which is a separate release.
