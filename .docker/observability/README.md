# RustFS Observability Stack

This directory contains the comprehensive observability stack for RustFS, designed to provide deep insights into application performance, logs, and traces.

## Components

The stack is composed of the following best-in-class open-source components:

- **Prometheus** (v2.53.1): The industry standard for metric collection and alerting.
- **Grafana** (v11.1.0): The leading platform for observability visualization.
- **Loki** (v3.1.0): A horizontally-scalable, highly-available, multi-tenant log aggregation system.
- **Tempo** (v2.5.0): A high-volume, minimal dependency distributed tracing backend.
- **Jaeger** (v1.59.0): Distributed tracing system (configured as a secondary UI/storage).
- **OpenTelemetry Collector** (v0.104.0): A vendor-agnostic implementation for receiving, processing, and exporting telemetry data.

By default, this stack uses Tempo in single-binary mode and does not require Kafka/Redpanda.
If you want the Kafka-backed HA Tempo path, use `docker-compose-example-for-rustfs.yml` together with `docker-compose-tempo-ha-override.yml`.

## Architecture

1. **Telemetry Collection**: Applications send OTLP (OpenTelemetry Protocol) data (Metrics, Logs, Traces) to the **OpenTelemetry Collector**.
2. **Processing & Exporting**: The Collector processes the data (batching, memory limiting) and exports it to the respective backends:
    - **Traces** -> **Tempo** (Primary) & **Jaeger** (Secondary/Optional)
    - **Metrics** -> **Prometheus** (via scraping the Collector's exporter)
    - **Logs** -> **Loki**
3. **Visualization**: **Grafana** connects to all backends (Prometheus, Tempo, Loki, Jaeger) to provide a unified dashboard experience.

## Features

- **Full Persistence**: All data (Metrics, Logs, Traces) is persisted to Docker volumes, ensuring no data loss on restart.
- **Correlation**: Seamless navigation between Metrics, Logs, and Traces in Grafana.
  - Jump from a Metric spike to relevant Traces.
  - Jump from a Trace to relevant Logs.
- **High Performance**: Optimized configurations for batching, compression, and memory management.
- **Standardized Protocols**: Built entirely on OpenTelemetry standards.

## GET Performance Optimization Dashboards

Three pre-built Grafana dashboards are included for monitoring RustFS GET performance optimization rollout:

### Available Dashboards

| Dashboard | File | Description |
|-----------|------|-------------|
| **GET Rollout Health** | `grafana-get-rollout-health.json` | Monitors optimization rollout: latency by reader path, early-stop hit rate, codec streaming usage, pipeline failures |
| **GET Data Integrity** | `grafana-get-data-integrity.json` | Monitors data safety: bitrot verify failures, decode errors, short reads, shard read outcomes |
| **GET Resource Impact** | `grafana-get-resource-impact.json` | Monitors resource usage: concurrent requests, IO queue utilization, disk permit wait, RSS trend |

### Prometheus Alert Rules

The file `prometheus-rules/rustfs-get-optimization-alerts.yaml` contains pre-configured alerting rules:

| Alert | Severity | Condition |
|-------|----------|-----------|
| `GetP99Regression` | Critical | GET p99 latency > 2x baseline for 10m |
| `PipelineFailureSpike` | Critical | Pipeline failure rate > 5x baseline for 5m |
| `BitrotMismatchSpike` | Critical | Bitrot mismatch rate > 3x baseline for 5m |
| `EarlyStopInsufficientQuorum` | Warning | Early-stop insufficient quorum rate > 0.1/s for 5m |
| `CodecStreamingFallbackSpike` | Warning | Codec streaming fallback > 10x baseline for 10m |
| `IoQueueSaturation` | Warning | IO queue utilization > 90% for 5m |

### Enabling Alert Rules

Add the alert rules file to your Prometheus configuration:

```yaml
# prometheus.yml
rule_files:
  - "/etc/prometheus/rules/*.yml"

# Or mount the file in docker-compose.yml:
# volumes:
#   - ./prometheus-rules:/etc/prometheus/rules
```

### Dashboard Usage

The dashboards are automatically provisioned when Grafana starts. They use the `${DS_PROMETHEUS}` datasource variable, so you need a Prometheus datasource configured in Grafana.

Key panels to monitor during optimization rollout:

1. **GET Latency by Reader Path** - Compare `codec_streaming` vs `legacy_duplex` latency
2. **Early-Stop Hit Rate** - Verify early-stop is triggering effectively
3. **Pipeline Failure Rate** - Detect any new failure modes introduced by optimizations
4. **Bitrot Verify Failures** - Ensure data integrity is maintained

## Quick Start

### Prerequisites

- Docker
- Docker Compose

### Deploy

Run the following command to start the entire stack:

```bash
docker compose up -d
```

### High Availability Tempo

The default `docker-compose.yml` is the single-node stack.
If you need the Kafka-backed HA Tempo configuration, start it with:

```bash
docker compose -f docker-compose-example-for-rustfs.yml -f docker-compose-tempo-ha-override.yml up -d
```

### Access Dashboards

| Service        | URL                                              | Credentials       | Description                    |
| :------------- | :----------------------------------------------- | :---------------- | :----------------------------- |
| **Grafana**    | [http://localhost:3000](http://localhost:3000)   | `admin` / `admin` | Main visualization hub.        |
| **Prometheus** | [http://localhost:9090](http://localhost:9090)   | -                 | Metric queries and status.     |
| **Jaeger UI**  | [http://localhost:16686](http://localhost:16686) | -                 | Secondary trace visualization. |
| **Tempo**      | [http://localhost:3200](http://localhost:3200)   | -                 | Tempo status/metrics.          |

## Configuration

### Data Persistence

Data is stored in the following Docker volumes:

- `prometheus-data`: Prometheus metrics
- `tempo-data`: Tempo traces (WAL and Blocks)
- `loki-data`: Loki logs (Chunks and Rules)
- `jaeger-data`: Jaeger traces (Badger DB)

To clear all data:

```bash
docker compose down -v
```

### Customization

- **Prometheus**: Edit `prometheus.yml` to add scrape targets or alerting rules.
- **Grafana**: Dashboards and datasources are provisioned from the `grafana/` directory.
- **Collector**: Edit `otel-collector-config.yaml` to modify pipelines, processors, or exporters.

### Verifying RustFS Traces

When RustFS points `RUSTFS_OBS_ENDPOINT` at this stack, treat the value as the
OTLP/HTTP base URL, for example:

```bash
export RUSTFS_OBS_ENDPOINT=http://host.docker.internal:4318
```

RustFS automatically expands that base URL to:

- `/v1/traces`
- `/v1/metrics`
- `/v1/logs`

Important behavior notes:

- Logs and metrics usually appear during startup, so seeing those two signals
  first is expected.
- Visible trace data usually requires real HTTP/S3/gRPC request traffic after
  startup, because request-path spans are created on demand.
- `RUSTFS_OBS_LOGGER_LEVEL=info` keeps the top-level request span but filters
  many nested `debug` spans. If Tempo or Jaeger looks sparse, retry with
  `RUSTFS_OBS_LOGGER_LEVEL=debug` before suspecting collector or Tempo issues.

Minimal validation flow:

```bash
# 1. Start this observability stack.
docker compose up -d

# 2. Start RustFS with OTLP/HTTP export and richer span visibility.
export RUSTFS_OBS_ENDPOINT=http://host.docker.internal:4318
export RUSTFS_OBS_LOGGER_LEVEL=debug

# 3. Generate real request traffic.
curl -I http://127.0.0.1:9000/health
curl -I http://127.0.0.1:9000/health/ready

# 4. Inspect Grafana or Jaeger.
# Grafana: http://localhost:3000
# Jaeger:  http://localhost:16686
```

If logs and metrics are present but traces are sparse, the most common cause is
"no real request traffic yet" or "`info` level filtered nested spans", not an
OTLP routing failure.

## Troubleshooting

- **Service Health**: Check the health of services using `docker compose ps`.
- **Logs**: View logs for a specific service using `docker compose logs -f <service_name>`.
- **Otel Collector**: Check `http://localhost:13133` for health status and `http://localhost:1888/debug/pprof/` for profiling.
