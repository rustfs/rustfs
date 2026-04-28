# Specialized Docker Compose Configurations

This directory contains specialized Docker Compose configurations for specific testing scenarios.

## ⚠️ Important Note

**For Observability:**
We **strongly recommend** using the new, fully integrated observability stack located in `../observability/`. It provides a production-ready setup with Prometheus, Grafana, Tempo, Loki, and OpenTelemetry Collector, all with persistent storage and optimized configurations.

The `docker-compose.observability.yaml` in this directory is kept for legacy reference or specific minimal testing needs but is **not** the primary recommended setup.

## 📁 Configuration Files

### Cluster Testing

- **`docker-compose.cluster.yaml`**
  - **Purpose**: Simulates a 4-node RustFS distributed cluster.
  - **Use Case**: Testing distributed storage logic, consensus, and failover.
  - **Nodes**: 4 RustFS instances.
  - **Storage**: Uses local HTTP endpoints.

### Legacy / Minimal Observability

- **`docker-compose.observability.yaml`**
  - **Purpose**: A minimal observability setup.
  - **Status**: **Deprecated**. Please use `../observability/docker-compose.yml` instead.

## 🚀 Usage Examples

### Cluster Testing

To start a 4-node cluster for distributed testing:

```bash
# From project root
docker compose -f .docker/compose/docker-compose.cluster.yaml up -d
```

### Script-Based 4-Node Validation (Recommended)

Use the local validation script when you need local-source image build, failover checks,
and benchmark workflow in one command:

```bash
# Default mode: WAIT_PROBE_MODE=service
# This avoids false negatives where /health/ready remains 503 locally
# while the service path is already available.
./scripts/run_four_node_cluster_failover_bench.sh
```

Strict mode is available when you explicitly want `/health/ready == 200` as the gate:

```bash
WAIT_PROBE_MODE=ready ./scripts/run_four_node_cluster_failover_bench.sh
```

### (Deprecated) Minimal Observability

```bash
# From project root
docker compose -f .docker/compose/docker-compose.observability.yaml up -d
```
