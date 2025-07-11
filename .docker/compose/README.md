# Docker Compose Configurations

This directory contains specialized Docker Compose configurations for different use cases.

## 📁 Configuration Files

### Main Configuration (Root Directory)

- **`../../docker-compose.yml`** - **Default Production Setup**
  - Complete production-ready configuration
  - Includes RustFS server + full observability stack
  - Supports multiple profiles: `dev`, `observability`, `cache`, `proxy`
  - Recommended for most users

### Specialized Configurations

- **`docker-compose.cluster.yaml`** - **Distributed Testing**
  - 4-node cluster setup for testing distributed storage
  - Uses local compiled binaries
  - Simulates multi-node environment
  - Ideal for development and cluster testing

- **`docker-compose.observability.yaml`** - **Observability Focus**
  - Specialized setup for testing observability features
  - Includes OpenTelemetry, Jaeger, Prometheus, Loki, Grafana
  - Uses `Dockerfile.obs` for builds
  - Perfect for observability development

## 🚀 Usage Examples

### Production Setup

```bash
# Start main service
docker-compose up -d

# Start with development profile
docker-compose --profile dev up -d

# Start with full observability
docker-compose --profile observability up -d
```

### Cluster Testing

```bash
# Build and start 4-node cluster
cd .docker/compose
docker-compose -f docker-compose.cluster.yaml up -d
```

### Observability Testing

```bash
# Start observability-focused environment
cd .docker/compose
docker-compose -f docker-compose.observability.yaml up -d
```

## 🔧 Configuration Overview

| Configuration | Nodes | Storage | Observability | Use Case |
|---------------|-------|---------|---------------|----------|
| **Main** | 1 | Volume mounts | Full stack | Production |
| **Cluster** | 4 | HTTP endpoints | Basic | Testing |
| **Observability** | 4 | Local data | Advanced | Development |

## 📝 Notes

- Always ensure you have built the required binaries before starting cluster tests
- The main configuration is sufficient for most use cases
- Specialized configurations are for specific testing scenarios
