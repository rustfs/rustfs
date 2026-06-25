# Issue #712 Local Queryable Metrics Backend Guide

## 1. Purpose

This guide is intended to unblock the third validation batch for `#712` by making multipart PUT stage metrics queryable from a local backend.

The current problem is not that multipart stage metrics are missing from the code path. The actual problem is that the local environment does not expose a queryable metrics backend:

1. `rustfs/admin/v3/metrics` is available, but it returns an admin-side JSON snapshot rather than the `metrics` crate histogram series
2. there is no local Prometheus or equivalent queryable metrics endpoint listening by default
3. therefore the following stage labels cannot be queried directly yet:
   - `multipart_ingress_prepare`
   - `multipart_set_disk_writer_setup`
   - `multipart_set_disk_encode`
   - `multipart_complete_tail`

The goal of this guide is to close that gap.

## 2. Recommended approach

Reuse the repository's existing observability stack:

1. `.docker/observability/docker-compose.yml`

Why this is the preferred path:

1. it is already maintained in-repo
2. it includes OTEL Collector, Prometheus, and Grafana
3. it can receive telemetry from RustFS through `RUSTFS_OBS_ENDPOINT`

## 3. Expected data flow

After startup, the intended flow is:

1. RustFS
   - `RUSTFS_OBS_ENDPOINT=http://host.docker.internal:4318`
2. OTEL Collector
   - receives OTLP/HTTP telemetry
3. Prometheus
   - scrapes collector-exported metrics
4. Query surface
   - `http://127.0.0.1:9090`

## 4. Startup steps

### 4.1 Start the observability stack

From the repository root:

```bash
cd .docker/observability
docker compose up -d
```

### 4.2 Wait for core services

Recommended checks:

```bash
curl -fsS http://127.0.0.1:9090/-/ready
curl -fsS http://127.0.0.1:3000/api/health
```

If you need container status:

```bash
docker compose ps
```

### 4.3 Point RustFS to the OTEL Collector

For local single-node multi-disk validation:

```bash
export RUSTFS_OBS_ENDPOINT=http://host.docker.internal:4318
```

If you use the repository-local restart helper:

```bash
bash scripts/restart_local_single_node_multidisk_rustfs.sh
```

Make sure the final runtime environment really contains:

```bash
RUSTFS_OBS_ENDPOINT=http://host.docker.internal:4318
```

## 5. Minimal query validation

### 5.1 Confirm Prometheus can see RustFS metrics

```bash
curl -fsS 'http://127.0.0.1:9090/api/v1/query?query=rustfs_s3_put_object_total'
```

### 5.2 Confirm stage labels exist

```bash
curl -fsS 'http://127.0.0.1:9090/api/v1/label/stage/values'
```

If the pipeline is working, the result should include:

1. `multipart_ingress_prepare`
2. `multipart_set_disk_writer_setup`
3. `multipart_set_disk_encode`
4. `multipart_complete_tail`

### 5.3 Direct P95 query

```bash
curl -fsS 'http://127.0.0.1:9090/api/v1/query?query=histogram_quantile(0.95,sum by(stage,le)(rate(rustfs_s3_put_object_stage_duration_ms_bucket{stage=~"multipart_.*"}[5m])))'
```

## 6. Recommended silent validation order

Once the backend is queryable, use this order for the third multipart validation batch:

1. start the observability stack
2. restart RustFS and confirm `RUSTFS_OBS_ENDPOINT` is active
3. run one multipart baseline:
   - `1g-64m-pc4`
   - `2g-128m-pc4`
4. ignore streaming benchmark logs and only keep:
   - `summary.csv`
   - Prometheus query outputs
5. summarize:
   - throughput / reqps / average latency
   - P95 / P99 for the four multipart stages

## 7. Recommended query set

### 7.1 Multipart stage P95

```promql
histogram_quantile(
  0.95,
  sum by (stage, le) (
    rate(rustfs_s3_put_object_stage_duration_ms_bucket{stage=~"multipart_.*"}[5m])
  )
)
```

### 7.2 Multipart stage P99

```promql
histogram_quantile(
  0.99,
  sum by (stage, le) (
    rate(rustfs_s3_put_object_stage_duration_ms_bucket{stage=~"multipart_.*"}[5m])
  )
)
```

### 7.3 Complete tail focus

```promql
histogram_quantile(
  0.95,
  sum by (instance, le) (
    rate(rustfs_s3_put_object_stage_duration_ms_bucket{stage="multipart_complete_tail"}[5m])
  )
)
```

### 7.4 Encode focus

```promql
histogram_quantile(
  0.95,
  sum by (instance, le) (
    rate(rustfs_s3_put_object_stage_duration_ms_bucket{stage="multipart_set_disk_encode"}[5m])
  )
)
```

## 8. Suggested result layout

Recommended layout:

```text
target/bench/
  issue712-multipart-server-path-focus/
    summary.csv
    metrics-query.txt
    promql/
      multipart-stage-p95.txt
      multipart-stage-p99.txt
      multipart-complete-tail-p95.txt
      multipart-encode-p95.txt
```

## 9. Troubleshooting

### 9.1 Prometheus does not start

Check:

```bash
cd .docker/observability
docker compose logs prometheus
```

### 9.2 RustFS does not export telemetry

Check:

1. `RUSTFS_OBS_ENDPOINT` really points to `http://host.docker.internal:4318`
2. the OTEL collector container is running
3. RustFS was restarted after the environment variable changed

### 9.3 Stage labels are missing

Check:

1. a multipart PUT workload really ran
2. `put_stage_metrics_enabled()` was enabled at runtime
3. the query window is not too short

## 10. Recommendation

When `#712` third-batch validation resumes, do not run the benchmark first and hunt for metrics later.

Use this order instead:

1. bring up a queryable backend
2. run the multipart baseline
3. read only:
   - `summary.csv`
   - Prometheus stage query outputs
