# RustFS Obs Metrics Coverage Gap Report

Date: 2026-04-23  
Scope: Compare `crates/obs/src/metrics/schema` metric definitions against `.docker/observability/grafana/dashboards/rustfs.json` PromQL usage.

## Comparison Method

- Metric source of truth: schema descriptors under `crates/obs/src/metrics/schema/*.rs`.
- Name construction rule:
  - Base metric name from `MetricDescriptor::get_full_metric_name()`.
  - For `counter` metrics, comparison also accepts `_total` suffixed runtime names.
- Dashboard side: all metric tokens matching `rustfs_*` extracted from `rustfs.json`.

## Summary

- Metrics defined from schema: `158`
- Metrics referenced in dashboard: `42`
- Metrics missing in dashboard: `116`

## Missing Counts by Subsystem

- `system_drive`: 17
- `system_process`: 16
- `cluster_erasure_set`: 14
- `cluster_iam`: 10
- `bucket_replication`: 9
- `bucket_api`: 8
- `cluster_usage_objects`: 8
- `cluster_usage_buckets`: 7
- `notification`: 6
- `ilm`: 5
- `system_network_internode`: 5
- `scanner`: 4
- `cluster_config`: 2
- `audit`: 1
- `api_requests`: 1
- `system_cpu`: 1
- `system_gpu`: 1
- `system_memory`: 1

## Replication-Specific Missing Metrics

- `rustfs_bucket_replication_last_hour_failed_bytes`
- `rustfs_bucket_replication_last_minute_failed_bytes`
- `rustfs_bucket_replication_last_minute_failed_count`
- `rustfs_bucket_replication_latency_ms`
- `rustfs_bucket_replication_proxied_delete_tagging_requests_failures`
- `rustfs_bucket_replication_proxied_delete_tagging_requests_total`
- `rustfs_bucket_replication_proxied_get_tagging_requests_failures`
- `rustfs_bucket_replication_proxied_get_tagging_requests_total`
- `rustfs_bucket_replication_total_failed_bytes`

## Next Patch Plan (Applied in This Iteration)

1. Replication-first:
   - Extend Bucket Replication panels with missing bytes and tagging proxy metrics.
   - Use compatible query expressions where old/new metric names may coexist.
2. System/Cluster next:
   - Add subsystem-grouped coverage panels for `system_*` and `cluster_*` missing families.
