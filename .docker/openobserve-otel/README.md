# OpenObserve + OpenTelemetry Collector

[![OpenObserve](https://img.shields.io/badge/OpenObserve-OpenSource-blue.svg)](https://openobserve.org)
[![OpenTelemetry](https://img.shields.io/badge/OpenTelemetry-Collector-green.svg)](https://opentelemetry.io/)

English | [中文](README_ZH.md)

This directory contains the configuration files for setting up an observability stack with OpenObserve and OpenTelemetry
Collector.

### Overview

This setup provides a complete observability solution for your applications:

- **OpenObserve**: A modern, open-source observability platform for logs, metrics, and traces.
- **OpenTelemetry Collector**: Collects and processes telemetry data before sending it to OpenObserve.

### Setup Instructions

1. **Prerequisites**:
    - Docker and Docker Compose installed
    - Sufficient memory resources (minimum 2GB recommended)

2. **Starting the Services**:
   ```bash
   cd .docker/openobserve-otel
   docker compose -f docker-compose.yml  up -d
   ```

3. **Accessing the Dashboard**:
    - OpenObserve UI: http://localhost:5080
    - Default credentials:
        - Username: root@rustfs.com
        - Password: rustfs123

### Configuration

#### OpenObserve Configuration

The OpenObserve service is configured with:

- Root user credentials
- Data persistence through a volume mount
- Memory cache enabled
- Health checks
- Exposed ports:
    - 5080: HTTP API and UI
    - 5081: OTLP gRPC

#### OpenTelemetry Collector Configuration

The collector is configured to:

- Receive telemetry data via OTLP (HTTP and gRPC)
- Collect logs from files
- Process data in batches
- Export data to OpenObserve
- Manage memory usage

### Integration with Your Application

To send telemetry data from your application, configure your OpenTelemetry SDK to send data to:

- OTLP gRPC: `localhost:4317`
- OTLP HTTP: `localhost:4318`

For example, in a Rust application using the `rustfs-obs` library:

```bash
export RUSTFS_OBS_ENDPOINT=http://localhost:4318
export RUSTFS_OBS_SERVICE_NAME=yourservice
export RUSTFS_OBS_SERVICE_VERSION=1.0.0
export RUSTFS_OBS_ENVIRONMENT=development
```

