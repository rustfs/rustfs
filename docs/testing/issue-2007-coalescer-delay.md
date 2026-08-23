# Backlog #2007 Coalescer Delay Validation

`scripts/issue_2007_coalescer_prometheus_report.py` is a read-only Prometheus
report helper for validating whether the GET metadata `ReadVersion` coalescer
default can move from `200us` to `50us`.

The benchmark itself is intentionally external to this helper: use the same
main build, bucket/object set, workload, and
`RUSTFS_BATCH_READ_VERSION_SERVER_PARALLELISM=4` for both cells. Only switch:

```bash
RUSTFS_GET_METADATA_READ_VERSION_COALESCE=auto
RUSTFS_GET_METADATA_READ_VERSION_COALESCE_DELAY_MICROS=200
RUSTFS_GET_METADATA_READ_VERSION_COALESCE_DELAY_MICROS=50
```

After each measured workload window, collect a report from Prometheus:

```bash
scripts/issue_2007_coalescer_prometheus_report.py \
  --query-url http://prometheus.example:9090 \
  --profile delay-200us \
  --window 180s \
  --rustfs-selector 'server=~"node[5-8]"' \
  --node-selector 'instance=~"node[5-8].*"'

scripts/issue_2007_coalescer_prometheus_report.py \
  --query-url http://prometheus.example:9090 \
  --profile delay-50us \
  --window 180s \
  --rustfs-selector 'server=~"node[5-8]"' \
  --node-selector 'instance=~"node[5-8].*"'
```

The output is Markdown and is suitable for attaching to the issue alongside the
warp throughput, average latency, p95, p99, and TTFB p99 from the fixed
workload run.

Required RustFS signals:

- `grpc_read_version` and `grpc_batch_read_version` outgoing request increases.
- Coalescer batch distribution from
  `rustfs_get_metadata_read_version_coalescer_total{event="attempted_batch"}`.
- `batch_read_version_coalescer_wait`, `batch_read_version_rpc_roundtrip`,
  `batch_read_version_disk_read`, and `batch_read_version_response_map` p99.

Required host-cost signals:

- CPU busy from `node_cpu_seconds_total`.
- Network RX/TX from `node_network_receive_bytes_total` and
  `node_network_transmit_bytes_total`.
- Disk read await, average queue depth, and utilization from node-exporter disk
  counters.

If a section reports `UNAVAILABLE`, treat that evidence as missing rather than
zero. Do not use a default-change PR until the `50us` cell has stable
throughput/latency benefit and CPU, network, and disk cost are available and
acceptable.
