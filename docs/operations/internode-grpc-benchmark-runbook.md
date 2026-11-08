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

For P0 single-parameter experiments, select `--transport-feature nodelay`, `stream-window`, `connection-window` or `max-message`. The default `all` retains the original combined preset and cannot attribute a result to one parameter. Selected P0 features write to `<out-root>/p0-<feature>-<phase>/`, keeping separate parameter artifacts from overwriting each other. Both legs clear the four P0-managed settings; the baseline overrides only the selected setting, while the candidate restores defaults. Apply any intended non-default production settings equally after the phase file, and read back effective settings, including bulk overrides that may take precedence. Zero receive windows select the linked library default, rather than a zero-byte or historical 64 KiB window; the phase names do not imply that the candidate is faster.

```bash
scripts/run_internode_grpc_ab_bench.sh --stage p0 --phase before --transport-feature connection-window --dry-run
scripts/run_internode_grpc_ab_bench.sh --stage p0 --phase after --transport-feature connection-window --dry-run
```

Dry-run command display redacts access keys, secret keys and session tokens. Actual dispatch preserves the supplied arguments. Keep credentials out of published evidence and shell history; only use task-owned buckets because load tools may clear their selected bucket.

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

For fleet-wide transport changes such as switching between plaintext and TLS, stop all test nodes and verify their processes have exited before starting any node with the new settings. A per-node stop/start loop can temporarily mix protocols and exhaust peer startup retries. Preserve real shutdown failures such as `http12_deadline_missed`; a successful `systemctl stop` command alone does not establish a clean application exit. Use independent collector endpoints and configure its own telemetry listener too, so a second collector does not collide with an existing instance's default internal metrics port.

Object workload latency and RPC latency describe different boundaries. Verify that the intended bulk RPCs actually occurred before attributing isolation, pool or window effects; large S3 objects alone do not prove large unary gRPC payload coverage. Record the histogram boundaries when comparing code versions: a finer bucket layout changes quantile estimates without establishing a latency improvement. Keep the instrument name, units and client/server boundaries with the raw evidence.

## Measurement identity and maintenance observations

Freeze a fresh main revision and candidate before rebuilding both binaries in separate source and Cargo target directories. Record the actual deployed executable identity, configuration, workload and resource ownership. Earlier reports remain attached to their original revisions. Run fault recovery on separate test volumes from the steady-state campaign, or finish recovery and demonstrate stable maintenance observations before measuring. Keep healing enabled; bypassing recovery changes the correctness contract.

`ReadVersion` uses the control channel and already exports `read_version_request_encode`, `read_version_rpc_roundtrip` and `read_version_response_decode` under `rustfs_system_network_internode_operation_stage_duration_ms`; its server stages include `read_version_disk_read`. Metrics export enables these GET stages without the PUT-specific stage flag. The roundtrip timer starts after Channel acquisition, so it does not measure TLS cache or connection acquisition. Ordinary small-object GET can predominantly exercise this control method. Confirm the actual bulk methods, payloads and achieved control request rate before changing bulk settings.

Normal GET aggregation requires `RUSTFS_GET_METADATA_READ_VERSION_COALESCE=auto|on`, which defaults off. Setting only `RUSTFS_METADATA_BATCH_READ` does not assemble GET batches. The remote batch gate resolves `RUSTFS_GET_METADATA_READ_VERSION_COALESCE` before `RUSTFS_METADATA_BATCH_READ`, then the legacy `RUSTFS_BATCH_METADATA_RPC`; `auto` can fall back on UNIMPLEMENTED, while `on` cannot. Restart after changing these cached gates and verify `rustfs_get_metadata_read_version_coalescer_total{event="attempted_batch"}` and `rustfs_remote_disk_batch_read_version_gate_total{mode,decision}`. Treat enabling aggregation as its own experiment because it changes the method distribution and adds the coalescing delay.

For each cell, record prepare, warmup, measure and cleanup separately. Request-level timestamps from the load driver define the measurement window; a Prometheus snapshot bracketing the entire Warp command includes preparation and cleanup. Do not divide such counter deltas by the timed workload duration or call object STAT/GET P99 a control-RPC P99. Record cumulative histogram bucket bounds and per-series timestamps; a bucket upper bound is not an exact quantile. Use external RPC samples for comparisons across changed bucket layouts.

The offline [maintenance-window checker](../../scripts/check_internode_grpc_window.py) consumes recorded OTLP JSON, retaining resource identity and producer point timestamps that a collector's cached Prometheus exposition may conceal. Build one snapshot per observation from the most recent export of each current RustFS process. Do not concatenate multiple exports of the same series into a snapshot. Independently record each node's PID, `/proc/<pid>/stat` start-time ticks and `/proc/sys/kernel/random/boot_id` at every observation; process names can contain spaces and parentheses, so parse the fields after the final `)` before reading the start-time field. A PID alone does not detect reuse, and start-time ticks alone do not detect a reboot. Synchronize producer and capture clocks, and predeclare export-age and sample-gap budgets suitable for the configured export interval. Repeated cached points, stale/future points, resets, missing required gauges, process changes and phase mismatches invalidate evidence.

The manifest is a bounded JSON object with this shape; replace the illustrative timestamps and identities with recorded values. Capture times at the first and last observations must match the driver-marked measurement boundaries. Every sample carries an independent process observation, even when the identity is unchanged.

```json
{
  "scope": "measurement",
  "start_ns": 100000000000,
  "end_ns": 220000000000,
  "max_sample_gap_ns": 2000000000,
  "max_export_age_ns": 2000000000,
  "processes": [{"node": "node1", "pid": 101, "start_ticks": 500, "boot_id": "11111111-1111-4111-8111-111111111111"}],
  "samples": [
    {"phase": "measurement", "captured_ns": 100000000000, "path": "measure-start.json", "processes": [{"node": "node1", "pid": 101, "start_ticks": 500, "boot_id": "11111111-1111-4111-8111-111111111111"}]},
    {"phase": "measurement", "captured_ns": 102000000000, "path": "measure-002.json", "processes": [{"node": "node1", "pid": 101, "start_ticks": 500, "boot_id": "11111111-1111-4111-8111-111111111111"}]}
  ]
}
```

Include all intermediate observations and a final sample at `end_ns`; the abbreviated example is intentionally incomplete and does not pass. The `node` value matches OTLP `service.instance.id`, and `pid` matches `process.pid`. Supply every test node in each process inventory; `--expected-nodes` defaults to four and rejects a partial inventory. The checker ignores exports from other processes, requires current-process MRF, heal-activity and scanner-activity gauges on every node, checks optional running-task gauges when present, and detects heal-start counter increments even if the gauge is zero at both endpoints. Lazy counters may be absent while idle; a newly appearing nonzero counter is activity, not an assumed zero baseline.

```bash
python3 scripts/check_internode_grpc_window.py --manifest /path/to/cell/window.json --out /path/to/cell/maintenance-report.json
```

Exit 0 means the sampled maintenance observations were stable, 1 means activity was observed, and 2 means the evidence is invalid. The report binds the manifest and each snapshot's SHA-256. Use a fresh output path per invocation and retain the exit status; exit 2 cannot qualify even if an earlier report exists. This does not authorize performance attribution or establish that no activity occurred between samples: method coverage, external RPC timing, sufficient duration/sample count, errors, resource isolation and baseline/ABBA drift gates remain separate requirements. Do not relabel old whole-command snapshots as measurement-only evidence or fill missing points with zero. Collect native OTLP for new experiments; the checker deliberately does not turn old Prometheus snapshots into fresh producer observations.

Use this zero-maintenance check for isolated experiments that predeclare maintenance-free observations. A busy verdict is not a production health failure: legitimate healing or scanning under mixed load can be part of the workload. Such experiments need a separately declared maintenance-rate/queue envelope and comparable system observations on both sides; do not disable required recovery or reuse the zero-maintenance verdict as their general acceptance criterion.

The shared server currently uses fixed HTTP/2 receive windows. With locked Hyper 1.12.0, the fixed stream and connection window setters following `adaptive_window(true)` disable adaptation. Moving that call last would change flow-control behavior; it is a separate experiment affecting the shared HTTP/2 listener, not a no-op cleanup. Keep fixed-window and adaptive-policy changes separate from client receive-window experiments.

## Shutdown diagnostics

Enable the existing `RUSTFS_OBS_LOG_STDOUT_ENABLED=true` on the task-owned diagnostic deployment and verify the sink before testing; `RUST_LOG` only sets filtering. Select the server module at debug level for the low-frequency `http_connection_drain` lifecycle events, while keeping other filtering identical between compared cells. Record `graceful_retired`, `children_retired`, `tls_reloader_retired`, and timeout `phase` (`graceful`, `child_join`, `completion_observed`, or `tls_reloader`), elapsed time, budget available at phase entry and tracked future count. `completion_observed` means the result was observed after the original deadline, not that tasks remain running. A tracked-future count is a snapshot, not proof that the original JoinHandles have been observed. No task identity, request body or credential is emitted by these diagnostics.

Reproduce empty/active RPC drains on baseline and candidate, plaintext and TLS, single-node and full-stop barriers, with independent test data. Preserve the first deadline failure even if every process eventually exits. The original shared deadline remains authoritative, and timeout still waits for the real child/reloader retirement. Never abort accepted writes, discard join evidence, clear unclean-shutdown markers or extend the deadline merely to make a benchmark green. Compare confirmed writes and recovery state before declaring a shutdown fix.

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
