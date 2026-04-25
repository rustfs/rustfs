# RustFS Metrics Inventory

## Purpose

This document is the Task 1 inventory for RustFS metrics remediation.

It answers:

- where metrics are emitted today
- which metric families already use underscore naming
- which families still use dot naming
- which families are already represented by `crates/obs`
- which families are still direct business-side emissions

## Source Modules

### `crates/obs`

Primary role:

- schema definition
- collector assembly
- runtime scheduling
- exporter integration

Current runtime-wired themes:

- cluster
- cluster health
- bucket
- node
- system drive
- bucket replication
- site replication
- audit
- notification
- resource
- system cpu
- system memory
- system process
- system network host

Defined but not fully runtime-wired themes:

- cluster config
- cluster erasure set
- cluster iam
- cluster usage
- request
- scanner
- ilm
- system network internode

### `rustfs/src/server/http.rs`

Primary role:

- HTTP request ingress metrics

Current direct metrics:

- `rustfs_api_requests_total`
- `rustfs_api_requests_failure_total`
- `rustfs_request_body_bytes_total`
- `rustfs_request_latency_ms`
- `rustfs_request_body_len`

Observation:

- this is a direct producer outside `crates/obs` request collector
- request method labeling already exists here

### `crates/common`

Primary role:

- scanner metrics
- internode metrics

Current direct metrics:

- scanner:
  - `rustfs_scanner_objects_scanned_total`
  - `rustfs_scanner_directories_scanned_total`
  - `rustfs_scanner_buckets_scanned_total`
  - `rustfs_scanner_cycles_total`
  - `rustfs_scanner_cycle_duration_seconds`
  - `rustfs_scanner_bucket_drive_duration_seconds`
- internode:
  - dot notation `rustfs.internode.*`

Observation:

- scanner already has an underscore family
- internode still uses dot notation and must be normalized

### `crates/io-metrics`

Primary role:

- hot-path storage, IO, zero-copy, bandwidth, timeout, pool, cache, capacity metrics

Current situation:

- underscore families already exist for some IO metrics, for example:
  - `rustfs_io_get_object_*`
  - `rustfs_io_disk_permit_wait_duration_seconds`
  - `rustfs_io_buffer_multiplier`
- a large set still uses dot notation, especially:
  - `rustfs.s3.*`
  - `rustfs.zero_copy.*`
  - `rustfs.bytes.pool.*`
  - `rustfs.io.*`
  - `rustfs.capacity.*`
  - `rustfs.cache.*`
  - `rustfs.disk.*`
  - `rustfs.memory.*`
  - `rustfs.cpu.*`
  - `rustfs.errors.*`
  - `rustfs.timeouts.*`
  - `rustfs.retries.*`
  - `rustfs.bandwidth.*`
  - `rustfs.lock.*`
  - `rustfs.deadlock.*`
  - `rustfs.backpressure.*`

### `rustfs/src/storage/*`

Primary role:

- object tagging
- request context fallback chains
- concurrency / lock / backpressure auxiliaries

Current direct metrics:

- `rustfs.log.chain.fallback_request_id.total`
- `rustfs.object_tag_conditions.*`
- `rustfs.object_tagging.operation.duration.seconds`
- `rustfs.delete_object_tagging.success`
- `rustfs.get_object_tagging.success`
- `rustfs.put_object_tagging.success`
- `rustfs.put_object_tagging.failure`
- `rustfs.lock.hold.duration.seconds`
- `rustfs.concurrent.get.requests`
- `rustfs.backpressure.events.total`
- `rustfs.zero_copy.write.attempts.total`
- `rustfs.zero_copy.write.size.bytes`

### `crates/audit`

Primary role:

- audit subsystem observability

Current situation:

- mostly underscore naming already
- needs later alignment with `crates/obs` audit-facing families and dashboard usage

## Naming Inventory

### Families already aligned to underscore canonical naming

Representative families:

- `rustfs_api_requests_*`
- `rustfs_request_*`
- `rustfs_system_*`
- `rustfs_cluster_*`
- `rustfs_bucket_*`
- `rustfs_replication_*`
- `rustfs_notification_*`
- `rustfs_audit_*`
- `rustfs_io_get_object_*`
- `rustfs_io_disk_permit_wait_duration_seconds`
- `rustfs_io_queue_*`
- `rustfs_io_buffer_multiplier`
- `rustfs_scanner_*`
- `rustfs_log_cleaner_*`
- `rustfs_tls_handshake_failures`

### Families still using dot naming and scheduled for migration

High-priority families:

- `rustfs.internode.*`
- `rustfs.s3.*`
- `rustfs.zero_copy.*`
- `rustfs.bytes.pool.*`
- `rustfs.io.*`
- `rustfs.capacity.*`
- `rustfs.cache.*`
- `rustfs.disk.*`
- `rustfs.memory.*`
- `rustfs.cpu.*`
- `rustfs.errors.*`
- `rustfs.timeouts.*`
- `rustfs.retries.*`
- `rustfs.bandwidth.*`
- `rustfs.lock.*`
- `rustfs.deadlock.*`
- `rustfs.backpressure.*`
- `rustfs.object_tag_conditions.*`
- `rustfs.object_tagging.*`
- `rustfs.log.chain.*`

## Ownership by Theme

### API / HTTP

- owner modules:
  - `rustfs/src/server/http.rs`
  - `crates/obs/src/metrics/schema/request.rs`
  - `crates/obs/src/metrics/collectors/request.rs`

### Scanner / Background

- owner modules:
  - `crates/common/src/metrics.rs`
  - `crates/scanner/src/*`
  - `crates/obs/src/metrics/schema/scanner.rs`
  - `crates/obs/src/metrics/collectors/scanner.rs`
  - `crates/obs/src/metrics/collectors/ilm.rs`

### Storage / IO / Zero Copy / Buffering

- owner modules:
  - `crates/io-metrics/src/*`
  - `rustfs/src/storage/*`
  - `rustfs/src/app/object_usecase.rs`

### Cluster / Capacity / Erasure Set

- owner modules:
  - `crates/obs/src/metrics/schema/cluster*.rs`
  - `crates/obs/src/metrics/collectors/cluster*.rs`
  - `crates/obs/src/metrics/stats_collector.rs`

### Replication

- owner modules:
  - `crates/obs/src/metrics/schema/bucket_replication.rs`
  - `crates/obs/src/metrics/collectors/bucket_replication.rs`
  - `crates/obs/src/metrics/collectors/replication.rs`
  - `crates/obs/src/metrics/stats_collector.rs`
  - `rustfs_ecstore::bucket::replication`

### Audit / Notification / IAM / Security

- owner modules:
  - `crates/audit/src/observability.rs`
  - `crates/obs/src/metrics/collectors/audit.rs`
  - `crates/obs/src/metrics/collectors/notification*.rs`
  - `crates/obs/src/metrics/collectors/cluster_iam.rs`
  - `rustfs/src/server/http.rs`

## Current Gaps

1. `crates/obs` has a broader schema surface than its runtime wiring.
2. HTTP and scanner both have split producers with partially overlapping semantics.
3. Dot notation remains widespread in `crates/io-metrics`, `crates/common`, and `rustfs/src/storage`.
4. Dashboard consumption currently mixes canonical underscore families with exporter-normalized dot families.

## Task 1 Completion Criteria

Task 1 is considered complete once:

- this inventory exists
- the naming contract exists
- the main producer modules and migration targets are identified
- Task 2 can proceed with an explicit canonical map
