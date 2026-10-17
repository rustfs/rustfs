# Bucket usage metrics

Use these metrics to collect every bucket's logical usage through the existing
OpenTelemetry pipeline. RustFS reads the cached scanner usage snapshot once per
collection; exporting metrics does not enumerate objects or run an inline scan.

## Export and query

Configure the RustFS nodes with an OTLP/HTTP destination and a stable cluster ID:

```bash
RUSTFS_OBS_ENDPOINT=http://otel-collector:4318
RUSTFS_OBS_METRICS_EXPORT_ENABLED=true
OTEL_RESOURCE_ATTRIBUTES=rustfs.cluster.id=production-a
```

Append the cluster ID to any existing resource attributes. The shipped
[Collector configuration](../../.docker/observability/otel-collector-config.yaml)
receives OTLP and exposes Prometheus samples on port 8889; its
`resource_to_telemetry_conversion` promotes the cluster ID to
`rustfs_cluster_id`. Prometheus scrapes the Collector. See
[storage metrics](storage-metrics.md#configure-the-pipeline) for pipeline setup
and resource identity.

The following existing gauges carry a `bucket` label:

| Metric | Meaning |
|---|---|
| `rustfs_cluster_usage_buckets_total_bytes` | Logical scanner bytes across retained object versions, including noncurrent versions; excludes delete-marker bytes. |
| `rustfs_cluster_usage_buckets_objects_count` | Scanner object-entry/key count, separate from version and delete-marker counts. |

Logical usage differs from physical drive usage, which includes erasure-coding
overhead and can differ because of compression or remote tiering. Object-entry
count can include keys with only noncurrent versions or delete markers, so it
need not equal the number of currently visible objects from `ListObjectsV2`.

For monitoring, query all buckets in one cluster:

```promql
max by (rustfs_cluster_id, job, bucket) (
  rustfs_cluster_usage_buckets_total_bytes{rustfs_cluster_id="production-a"}
)
```

```promql
max by (rustfs_cluster_id, job, bucket) (
  rustfs_cluster_usage_buckets_objects_count{rustfs_cluster_id="production-a"}
)
```

Each reporting node exports the whole cluster's logical usage. Summing those
copies would multiply the result by the node count. Keep cluster identity when
grouping: independent clusters can share both a job and a bucket name. The
bundled Bucket Size and Bucket Objects panels use the selected **Storage
cluster** and deduplicate that cluster's reporting nodes.

Nodes can briefly observe different snapshots. `max` is useful for dashboards
but can retain an older, larger value after deletes. For a consistent accounting
observation, select one reporting node for both bytes and object count:

```promql
rustfs_cluster_usage_buckets_total_bytes{
  rustfs_cluster_id="production-a", service_instance_id="node1:9000"
}
```

Use the same selector with `rustfs_cluster_usage_buckets_objects_count`, and
check the promoted `service_instance_id` in your Collector output. These
floating-point, periodic gauges are usage observations, not an exact billing
ledger or a transactional view of writes and deletes.

## Freshness and missing values

Usage collection runs on `RUSTFS_METRICS_CLUSTER_INTERVAL_SEC` (60 seconds by
default), independently of the OTLP export interval
`RUSTFS_OBS_METER_INTERVAL` (30 seconds by default). The separate bucket
collector defaults to 300 seconds. Neither interval forces a scanner cycle.

Check these gauges on the same reporting node as the bucket values:

- `rustfs_cluster_usage_objects_since_last_update_seconds`: age of the
  persisted usage snapshot when it was collected. A bucket cleanup can advance
  the timestamp; it is not necessarily the age of a full object rescan.
- `rustfs_cluster_usage_objects_snapshot_converged`: `1` for a converged
  snapshot, `0` when convergence is not confirmed, including observational or
  unknown state. Complete observations can still supply usage during sustained writes.

Before a complete snapshot covers the current bucket namespace, the usage
collector omits its values. A missing series is unknown; do not replace it with
zero for accounting. A bucket confirmed empty in a complete snapshot exports
zero bytes and zero objects.

A temporary collection failure preserves the last valid export snapshot.
RustFS stops exporting that snapshot once collection stalls beyond three times
the larger collection/export interval. Deleted bucket series receive bounded
zero tombstones and then stop being exported after a valid updated snapshot.
The Collector can retain the last sample after export stops; the shipped
configuration uses a five-minute `metric_expiration`. Prometheus history also
retains past samples.

If metrics are absent, first check OTLP connectivity and metrics export enablement,
then the scanner leader's `GET /rustfs/admin/v3/scanner/status` and the admin
usage snapshot. A shorter scrape interval cannot repair an incomplete scanner
snapshot. The scanner publication and cache path is shared with the Console's
usage display.

## Regression checks

The SDK test uses the production bucket collector and an actual OpenTelemetry
reader to verify two populated buckets, a confirmed-empty bucket, decreasing
usage, and removal from subsequent exports:

```bash
RUSTUP_TOOLCHAIN=stable cargo test -p rustfs-obs actual_sdk_exports_bucket_size_and_object_count_as_current_gauges --lib
```

The PromQL fixture checks same-name buckets in separate clusters and multiple
reporting nodes. Run it from `.docker/observability/tests`:

```bash
promtool test rules bucket-usage.test.yml
```
