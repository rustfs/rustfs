# RustFS Docker Infrastructure

This directory contains the complete Docker infrastructure for building, deploying, and monitoring RustFS. It provides ready-to-use configurations for development, testing, and production-grade observability.

## 📂 Directory Structure

| Directory | Description | Status |
| :--- | :--- | :--- |
| **[`observability/`](observability/README.md)** | **[RECOMMENDED]** Full-stack observability (Prometheus, Grafana, Tempo, Loki). | ✅ Production-Ready |
| **[`compose/`](compose/README.md)** | Specialized setups (e.g., 4-node distributed cluster testing). | ⚠️ Testing Only |
| **[`mqtt/`](mqtt/README.md)** | EMQX Broker configuration for MQTT integration testing. | 🧪 Development |
| **[`openobserve-otel/`](openobserve-otel/README.md)** | Alternative lightweight observability stack using OpenObserve. | 🔄 Alternative |

---

## 📄 Root Directory Files

The following files in the project root are essential for Docker operations:

### Build Scripts & Dockerfiles

| File | Description | Usage |
| :--- | :--- | :--- |
| **`docker-buildx.sh`** | **Multi-Arch Build Script**<br>Automates building and pushing Docker images for `amd64` and `arm64`. Supports release and dev channels. | `./docker-buildx.sh --push` |
| **`Dockerfile`** | **Production Image (Alpine)**<br>Lightweight image using musl libc. Downloads pre-built binaries from GitHub Releases. | `docker build -t rustfs:latest .` |
| **`Dockerfile.glibc`** | **Production Image (Ubuntu)**<br>Standard image using glibc. Useful if you need specific dynamic libraries. | `docker build -f Dockerfile.glibc .` |
| **`Dockerfile.source`** | **Development Image**<br>Builds RustFS from source code. Includes build tools. Ideal for local development and CI. | `docker build -f Dockerfile.source .` |

### Docker Compose Configurations

| File | Description | Usage |
| :--- | :--- | :--- |
| **`docker-compose.yml`** | **Main Development Setup**<br>Comprehensive setup with profiles for development, observability, and proxying. | `docker compose up -d`<br>`docker compose --profile observability up -d` |
| **`docker-compose-simple.yml`** | **Quick Start Setup**<br>Minimal configuration running a single RustFS instance with 4 volumes. Perfect for first-time users. | `docker compose -f docker-compose-simple.yml up -d` |

---

## 🌟 Observability Stack (Recommended)

Located in: [`.docker/observability/`](observability/README.md)

We provide a comprehensive, industry-standard observability stack designed for deep insights into RustFS performance. This is the recommended setup for both development and production monitoring.

### Components
- **Metrics**: Prometheus (Collection) + Grafana (Visualization)
- **Traces**: Tempo (Storage) + Jaeger (UI)
- **Logs**: Loki
- **Ingestion**: OpenTelemetry Collector

### Key Features
- **Full Persistence**: All metrics, logs, and traces are saved to Docker volumes, ensuring no data loss on restarts.
- **Correlation**: Seamlessly jump between Logs, Traces, and Metrics in Grafana.
- **High Performance**: Optimized configurations for batching, compression, and memory management.

### Quick Start
```bash
cd .docker/observability
docker compose up -d
```

---

## 🧪 Specialized Environments

Located in: [`.docker/compose/`](compose/README.md)

These configurations are tailored for specific testing scenarios that require complex topologies.

### Distributed Cluster (4-Nodes)
Simulates a real-world distributed environment with 4 RustFS nodes running locally.
```bash
docker compose -f .docker/compose/docker-compose.cluster.yaml up -d
```

### Integrated Observability Test
A self-contained environment running 4 RustFS nodes alongside the full observability stack. Useful for end-to-end testing of telemetry.
```bash
docker compose -f .docker/compose/docker-compose.observability.yaml up -d
```

---

## 📡 MQTT Integration

Located in: [`.docker/mqtt/`](mqtt/README.md)

Provides an EMQX broker for testing RustFS MQTT features.

### Quick Start
```bash
cd .docker/mqtt
docker compose up -d
```
- **Dashboard**: [http://localhost:18083](http://localhost:18083) (Default: `admin` / `public`)
- **MQTT Port**: `1883`

---

## 👁️ Alternative: OpenObserve

Located in: [`.docker/openobserve-otel/`](openobserve-otel/README.md)

For users preferring a lightweight, all-in-one solution, we support OpenObserve. It combines logs, metrics, and traces into a single binary and UI.

### Quick Start
```bash
cd .docker/openobserve-otel
docker compose up -d
```

---

## 🔧 Common Operations

### Cleaning Up
To stop all containers and remove volumes (**WARNING**: deletes all persisted data):
```bash
docker compose down -v
```

### Viewing Logs
To follow logs for a specific service:
```bash
docker compose logs -f [service_name]
```

### Checking Status
To see the status of all running containers:
```bash
docker compose ps
```
