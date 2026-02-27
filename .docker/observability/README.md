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

## Architecture

1.  **Telemetry Collection**: Applications send OTLP (OpenTelemetry Protocol) data (Metrics, Logs, Traces) to the **OpenTelemetry Collector**.
2.  **Processing & Exporting**: The Collector processes the data (batching, memory limiting) and exports it to the respective backends:
    -   **Traces** -> **Tempo** (Primary) & **Jaeger** (Secondary/Optional)
    -   **Metrics** -> **Prometheus** (via scraping the Collector's exporter)
    -   **Logs** -> **Loki**
3.  **Visualization**: **Grafana** connects to all backends (Prometheus, Tempo, Loki, Jaeger) to provide a unified dashboard experience.

## Features

-   **Full Persistence**: All data (Metrics, Logs, Traces) is persisted to Docker volumes, ensuring no data loss on restart.
-   **Correlation**: Seamless navigation between Metrics, Logs, and Traces in Grafana.
    -   Jump from a Metric spike to relevant Traces.
    -   Jump from a Trace to relevant Logs.
-   **High Performance**: Optimized configurations for batching, compression, and memory management.
-   **Standardized Protocols**: Built entirely on OpenTelemetry standards.

## Quick Start

### Prerequisites

-   Docker
-   Docker Compose

### Deploy

Run the following command to start the entire stack:

```bash
docker compose up -d
```

### Access Dashboards

| Service | URL | Credentials | Description |
| :--- | :--- | :--- | :--- |
| **Grafana** | [http://localhost:3000](http://localhost:3000) | `admin` / `admin` | Main visualization hub. |
| **Prometheus** | [http://localhost:9090](http://localhost:9090) | - | Metric queries and status. |
| **Jaeger UI** | [http://localhost:16686](http://localhost:16686) | - | Secondary trace visualization. |
| **Tempo** | [http://localhost:3200](http://localhost:3200) | - | Tempo status/metrics. |

## Configuration

### Data Persistence

Data is stored in the following Docker volumes:

-   `prometheus-data`: Prometheus metrics
-   `tempo-data`: Tempo traces (WAL and Blocks)
-   `loki-data`: Loki logs (Chunks and Rules)
-   `jaeger-data`: Jaeger traces (Badger DB)

To clear all data:

```bash
docker compose down -v
```

### Customization

-   **Prometheus**: Edit `prometheus.yml` to add scrape targets or alerting rules.
-   **Grafana**: Dashboards and datasources are provisioned from the `grafana/` directory.
-   **Collector**: Edit `otel-collector-config.yaml` to modify pipelines, processors, or exporters.

## Troubleshooting

-   **Service Health**: Check the health of services using `docker compose ps`.
-   **Logs**: View logs for a specific service using `docker compose logs -f <service_name>`.
-   **Otel Collector**: Check `http://localhost:13133` for health status and `http://localhost:1888/debug/pprof/` for profiling.
