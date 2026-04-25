# RustFS Metrics Naming Contract

## Purpose

This document defines the canonical naming contract for RustFS metrics.

## Canonical Rules

1. All RustFS metrics must use the `rustfs_` prefix.
2. Canonical metric names use underscore notation only.
3. New metrics must not use dot notation.
4. Metric names must encode one stable semantic and one stable unit.
5. Units must be explicit.

Preferred suffixes:

- `_total`
- `_seconds`
- `_bytes`
- `_ratio`
- `_count`

Avoid unless there is a strong compatibility reason:

- `.`
- `%`-style unit names in the metric name
- mixed time units for the same concept

## Label Rules

1. Labels must be low cardinality by default.
2. Route path, request id, object key, ARN-like unbounded IDs, or raw error text must not be introduced casually.
3. Prefer stable labels such as:
   - `method`
   - `operation`
   - `result`
   - `status`
   - `status_class`
   - `bucket`
   - `target_arn`
   - `drive`
   - `server`
   - `tier`

## Canonical Theme Mapping

### HTTP / API

Canonical prefix:

- `rustfs_http_server_*`

Allowed compatibility bridge during migration:

- existing `rustfs_api_requests_*`
- existing `rustfs_request_*`

Long-term direction:

- unify request volume, failures, active requests, latency, request body size, response body size

### Scanner

Canonical prefix:

- `rustfs_scanner_*`

This family should carry:

- cycles
- cycle duration
- bucket scans
- directory scans
- object scans
- version scans
- last activity

### Internode Network

Canonical prefix:

- `rustfs_system_network_internode_*`

Migration source:

- `rustfs.internode.*`

### Host / Process

Canonical prefixes:

- `rustfs_system_cpu_*`
- `rustfs_system_memory_*`
- `rustfs_system_process_*`
- `rustfs_system_network_host_*`
- `rustfs_system_drive_*`

### Cluster / Capacity / Usage

Canonical prefixes:

- `rustfs_cluster_*`
- `rustfs_cluster_health_*`
- `rustfs_cluster_usage_objects_*`
- `rustfs_cluster_usage_buckets_*`
- `rustfs_cluster_erasure_set_*`
- `rustfs_cluster_config_*`
- `rustfs_cluster_iam_*`

### Replication

Canonical prefixes:

- `rustfs_replication_*`
- `rustfs_bucket_replication_*`

### Notification / Audit / Security

Canonical prefixes:

- `rustfs_notification_*`
- `rustfs_audit_*`
- `rustfs_tls_*`

### Storage / IO / Zero Copy

Canonical prefixes:

- `rustfs_io_*`
- `rustfs_zero_copy_*`
- `rustfs_bytes_pool_*`
- `rustfs_s3_*`
- `rustfs_capacity_*`
- `rustfs_cache_*`
- `rustfs_disk_*`
- `rustfs_memory_*`
- `rustfs_cpu_*`
- `rustfs_lock_*`
- `rustfs_deadlock_*`
- `rustfs_backpressure_*`

## Canonical Migration Map

Representative required migrations:

- `rustfs.internode.sent.bytes.total` -> `rustfs_system_network_internode_sent_bytes_total`
- `rustfs.internode.recv.bytes.total` -> `rustfs_system_network_internode_recv_bytes_total`
- `rustfs.internode.errors.total` -> `rustfs_system_network_internode_errors_total`
- `rustfs.internode.dial.errors.total` -> `rustfs_system_network_internode_dial_errors_total`
- `rustfs.internode.dial.avg_time.nanos` -> `rustfs_system_network_internode_dial_avg_time_nanos`

- `rustfs.s3.get_object.total` -> `rustfs_s3_get_object_total`
- `rustfs.s3.get_object.duration.ms` -> `rustfs_s3_get_object_duration_ms`
- `rustfs.s3.get_object.size.bytes` -> `rustfs_s3_get_object_size_bytes`
- `rustfs.s3.put_object.total` -> `rustfs_s3_put_object_total`
- `rustfs.s3.list_objects.total` -> `rustfs_s3_list_objects_total`
- `rustfs.s3.delete_object.total` -> `rustfs_s3_delete_object_total`

- `rustfs.zero_copy.memory.saved.bytes` -> `rustfs_zero_copy_memory_saved_bytes_total`
- `rustfs.zero_copy.bytes.saved.total` -> `rustfs_zero_copy_bytes_saved_total`
- `rustfs.zero_copy.reads.total` -> `rustfs_zero_copy_reads_total`
- `rustfs.zero_copy.write.total` -> `rustfs_zero_copy_write_total`

- `rustfs.bytes.pool.acquisitions.total` -> `rustfs_bytes_pool_acquisitions_total`
- `rustfs.bytes.pool.hits.total` -> `rustfs_bytes_pool_hits_total`
- `rustfs.bytes.pool.misses.total` -> `rustfs_bytes_pool_misses_total`
- `rustfs.bytes.pool.allocated.bytes` -> `rustfs_bytes_pool_allocated_bytes`

- `rustfs.io.buffer.size.bytes` -> `rustfs_io_buffer_size_bytes`
- `rustfs.io.bandwidth.bps` -> `rustfs_io_bandwidth_bps`
- `rustfs.io.transfer.bytes` -> `rustfs_io_transfer_bytes_total`

- `rustfs.capacity.current` -> `rustfs_capacity_current_bytes`
- `rustfs.memory.used.bytes` -> `rustfs_memory_used_bytes`
- `rustfs.memory.total.bytes` -> `rustfs_memory_total_bytes`
- `rustfs.cpu.usage.percent` -> `rustfs_cpu_usage_ratio`

- `rustfs.log.chain.fallback_request_id.total` -> `rustfs_log_chain_fallback_request_id_total`
- `rustfs.object_tag_conditions.fetched` -> `rustfs_object_tag_conditions_fetched_total`
- `rustfs.object_tagging.operation.duration.seconds` -> `rustfs_object_tagging_operation_duration_seconds`

## Compatibility Policy

During migration:

1. Source code must move to underscore names.
2. Dashboard queries may temporarily use:

```promql
new_metric or old_metric
```

3. Compatibility queries must be removed once the source migration and dashboard rollout are complete.

## Prohibited New Additions

The following are prohibited for all new metrics:

- dot notation names
- embedding raw high-cardinality identifiers in labels
- creating a second metric family for an existing canonical theme without strong justification

## Task 1 Completion Criteria

Task 1 is considered complete once:

- this naming contract exists
- the canonical families and migration map are documented
- follow-up tasks can directly implement code changes against this contract
