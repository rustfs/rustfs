# Internode Transport Metrics and Baseline

Status: current OSS behavior. This document describes the metrics and baseline
runner used to observe the existing `tcp-http` internode transport adapter. It
does not add a backend, change runtime behavior, or make performance claims.

## Scope

The current OSS adapter scope is:

- `tcp-http` is the default and only supported internode data transport backend;
- `tcp` is a legacy alias for `tcp-http`;
- unsupported backend values fail closed during transport construction;
- metrics and baseline artifacts are used to understand the current data-plane
  behavior.

## Operation Metrics

Operation-level internode metrics are defined in
`crates/io-metrics/src/internode_metrics.rs`.

| Metric | Labels | Meaning |
| --- | --- | --- |
| `rustfs_system_network_internode_operation_sent_bytes_total` | `operation`, `backend` | Bytes sent for a known internode operation. |
| `rustfs_system_network_internode_operation_recv_bytes_total` | `operation`, `backend` | Bytes received for a known internode operation. |
| `rustfs_system_network_internode_operation_requests_outgoing_total` | `operation`, `backend` | Outgoing requests opened for a known internode operation. |
| `rustfs_system_network_internode_operation_requests_incoming_total` | `operation`, `backend` | Incoming requests handled for a known internode operation. |
| `rustfs_system_network_internode_operation_errors_total` | `operation`, `backend` | Errors observed for a known internode operation. |

Current operation label values are:

| Operation | Transport path | Notes |
| --- | --- | --- |
| `read_file_stream` | HTTP `/rustfs/rpc/read_file_stream` | Remote disk read stream opened by `InternodeDataTransport::open_read`. |
| `put_file_stream` | HTTP `/rustfs/rpc/put_file_stream` | Remote disk write stream opened by `InternodeDataTransport::open_write`. |
| `walk_dir` | HTTP `/rustfs/rpc/walk_dir` | Remote walk-dir stream opened by `InternodeDataTransport::open_walk_dir`. |
| `grpc_read_all` | gRPC `ReadAll` | Unary bytes response outside the adapter. |
| `grpc_write_all` | gRPC `WriteAll` | Unary bytes request outside the adapter. |

Current backend label values are:

| Backend | Meaning |
| --- | --- |
| `tcp-http` | Current HTTP stream backend for adapter-routed operations. |
| `grpc` | Current gRPC path for `ReadAll` and `WriteAll`. |
| `unknown` | Aggregate internode metric path when an operation-specific backend is not available. |

The `backend` label reflects the current instrumentation source, for example
the `tcp-http` stream path, gRPC path, or aggregate unknown path. It does not
imply additional supported transport backends.

Metric labels must stay low cardinality. Do not add object names, full paths,
full URLs, peer-specific dynamic strings, request IDs, or other per-request
values as labels.

## Current Instrumentation Points

| Area | File | Current signal |
| --- | --- | --- |
| Outgoing HTTP stream requests | `crates/rio/src/http_reader.rs` | Outgoing request count, sent bytes for writer bodies, received bytes for reader bodies, and errors for known `/rustfs/rpc/` operations. |
| Incoming HTTP stream requests | `rustfs/src/storage/rpc/http_service.rs` | Incoming request count, sent bytes for read and walk streams, received bytes for put streams, and route errors. |
| gRPC `ReadAll` / `WriteAll` | `rustfs/src/storage/rpc/disk.rs` | Incoming request count, sent/received bytes, and errors with the `grpc` backend label. |

Known gaps:

- Operation-level metrics do not currently expose latency histograms.
- The baseline runner reads Prometheus text output only when `--metrics-url` is
  provided.
- Metrics are useful for attribution, but they do not replace end-to-end
  correctness tests or object benchmark output.

## Baseline Runner

Use `scripts/run_internode_transport_baseline.sh` to run the current S3 PUT/GET
matrix against one or more configured endpoints.

Required inputs:

- `--access-key`
- `--secret-key`

Common optional inputs:

- `--tool warp|s3bench`
- `--scenarios name=url,...`
- `--sizes 4KiB,1MiB,...`
- `--concurrencies 1,16,...`
- `--duration 90s`
- `--metrics-url http://host:port/metrics`
- `--out-dir target/bench/internode-transport/manual-run`
- `--dry-run`

Dry-run example:

```bash
scripts/run_internode_transport_baseline.sh \
  --access-key minioadmin \
  --secret-key minioadmin \
  --scenarios local=http://127.0.0.1:9000,distributed=http://127.0.0.1:9001 \
  --sizes 4KiB,1MiB \
  --concurrencies 1 \
  --duration 10s \
  --dry-run
```

Real baseline example with metrics:

```bash
RUSTFS_INTERNODE_DATA_TRANSPORT=tcp-http \
scripts/run_internode_transport_baseline.sh \
  --access-key "$RUSTFS_ACCESS_KEY" \
  --secret-key "$RUSTFS_SECRET_KEY" \
  --scenarios local=http://127.0.0.1:9000,distributed=http://127.0.0.1:9001 \
  --metrics-url http://127.0.0.1:9000/metrics \
  --out-dir target/bench/internode-transport/manual-run
```

The real baseline requires a running RustFS deployment and working benchmark
tool. Do not invent or copy synthetic benchmark numbers into PR descriptions.

## Output Artifacts

The baseline runner writes:

| File | Created when | Contents |
| --- | --- | --- |
| `run_manifest.txt` | Always | Timestamp, git commit, dirty flag, Rust compiler version, kernel string, scenario matrix, tool, workload settings, metrics URL, and redacted credentials. |
| `summary.csv` | Always | `scenario`, `endpoint`, `workload`, `concurrency`, `size`, `status`, `throughput`, `requests_per_sec`, `avg_latency`, `error_count`, `log_file`, and `run_dir`. |
| `internode_metric_deltas.csv` | When `--metrics-url` is set | `scenario`, `workload`, `concurrency`, `size`, metric name, operation, backend, before value, after value, and delta. |

In dry-run mode, the runner still creates the manifest and CSV headers, and the
underlying object benchmark command is printed with credentials redacted.

## Interpretation

Use baseline artifacts to record and inspect the current `tcp-http` adapter
behavior across commits, environments, and scenario matrices. A baseline run is
valid only for the exact environment and command recorded in
`run_manifest.txt`.

Baseline artifacts should state whether metrics were collected. When
`internode_metric_deltas.csv` is absent, benchmark output cannot attribute
internode operation deltas from Prometheus metrics.
