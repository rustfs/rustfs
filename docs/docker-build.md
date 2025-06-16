# RustFS Docker Build and Deployment Guide

This document describes how to build and deploy RustFS using Docker, including the automated GitHub Actions workflow for building and pushing images to Docker Hub and GitHub Container Registry.

## 🚀 Quick Start

### Using Pre-built Images

```bash
# Pull and run the latest RustFS image
docker run -d \
  --name rustfs \
  -p 9000:9000 \
  -p 9001:9001 \
  -v rustfs_data:/data \
  -e RUSTFS_VOLUMES=/data/rustfs0,/data/rustfs1,/data/rustfs2,/data/rustfs3 \
  -e RUSTFS_ACCESS_KEY=rustfsadmin \
  -e RUSTFS_SECRET_KEY=rustfsadmin \
  -e RUSTFS_CONSOLE_ENABLE=true \
  rustfs/rustfs:latest
```

### Using Docker Compose

```bash
# Basic deployment
docker-compose up -d

# Development environment
docker-compose --profile dev up -d

# With observability stack
docker-compose --profile observability up -d

# Full stack with all services
docker-compose --profile dev --profile observability --profile testing up -d
```

## 📦 Available Images

Our GitHub Actions workflow builds multiple image variants:

### Image Registries

- **Docker Hub**: `rustfs/rustfs`
- **GitHub Container Registry**: `ghcr.io/rustfs/s3-rustfs`

### Image Variants

| Variant | Tag Suffix | Description | Use Case |
|---------|------------|-------------|----------|
| Production | *(none)* | Minimal Ubuntu-based runtime | Production deployment |
| Ubuntu | `-ubuntu22.04` | Ubuntu 22.04 based build environment | Development/Testing |
| Rocky Linux | `-rockylinux9.3` | Rocky Linux 9.3 based build environment | Enterprise environments |
| Development | `-devenv` | Full development environment | Development/Debugging |

### Supported Architectures

All images support multi-architecture:
- `linux/amd64` (x86_64-unknown-linux-musl)
- `linux/arm64` (aarch64-unknown-linux-gnu)

### Tag Examples

```bash
# Latest production image
rustfs/rustfs:latest
rustfs/rustfs:main

# Specific version
rustfs/rustfs:v1.0.0
rustfs/rustfs:v1.0.0-ubuntu22.04

# Development environment
rustfs/rustfs:latest-devenv
rustfs/rustfs:main-devenv
```

## 🔧 GitHub Actions Workflow

The Docker build workflow (`.github/workflows/docker.yml`) automatically:

1. **Builds cross-platform binaries** for `amd64` and `arm64`
2. **Creates Docker images** for all variants
3. **Pushes to registries** (Docker Hub and GitHub Container Registry)
4. **Creates multi-arch manifests** for seamless platform selection
5. **Performs security scanning** using Trivy

### Cross-Compilation Strategy

To handle complex native dependencies, we use different compilation strategies:

- **x86_64**: Native compilation with `x86_64-unknown-linux-musl` for static linking
- **aarch64**: Cross-compilation with `aarch64-unknown-linux-gnu` using the `cross` tool

This approach ensures compatibility with various C libraries while maintaining performance.

### Workflow Triggers

- **Push to main branch**: Builds and pushes `main` and `latest` tags
- **Tag push** (`v*`): Builds and pushes version tags
- **Pull requests**: Builds images without pushing
- **Manual trigger**: Workflow dispatch with options

### Required Secrets

Configure these secrets in your GitHub repository:

```bash
# Docker Hub credentials
DOCKERHUB_USERNAME=your-dockerhub-username
DOCKERHUB_TOKEN=your-dockerhub-access-token

# GitHub token is automatically available
GITHUB_TOKEN=automatically-provided
```

## 🏗️ Building Locally

### Prerequisites

- Docker with BuildKit enabled
- Rust toolchain (1.85+)
- Protocol Buffers compiler (protoc 31.1+)
- FlatBuffers compiler (flatc 25.2.10+)
- `cross` tool for ARM64 compilation

### Installation Commands

```bash
# Install Rust targets
rustup target add x86_64-unknown-linux-musl
rustup target add aarch64-unknown-linux-gnu

# Install cross for ARM64 compilation
cargo install cross --git https://github.com/cross-rs/cross

# Install protoc (macOS)
brew install protobuf

# Install protoc (Ubuntu)
sudo apt-get install protobuf-compiler

# Install flatc
# Download from: https://github.com/google/flatbuffers/releases
```

### Build Commands

```bash
# Test cross-compilation setup
./scripts/test-cross-build.sh

# Build production image for local platform
docker build -t rustfs:local .

# Build multi-stage production image
docker build -f Dockerfile.multi-stage -t rustfs:multi-stage .

# Build specific variant
docker build -f .docker/Dockerfile.ubuntu22.04 -t rustfs:ubuntu .

# Build for specific platform
docker build --platform linux/amd64 -t rustfs:amd64 .
docker build --platform linux/arm64 -t rustfs:arm64 .

# Build multi-platform image
docker buildx build --platform linux/amd64,linux/arm64 -t rustfs:multi .
```

### Cross-Compilation

```bash
# Generate protobuf code first
cargo run --bin gproto

# Native x86_64 build
cargo build --release --target x86_64-unknown-linux-musl --bin rustfs

# Cross-compile for ARM64
cross build --release --target aarch64-unknown-linux-gnu --bin rustfs
```

### Build with Docker Compose

```bash
# Build all services
docker-compose build

# Build specific service
docker-compose build rustfs

# Build development environment
docker-compose build rustfs-dev
```

## 🚀 Deployment Options

### 1. Single Container

```bash
docker run -d \
  --name rustfs \
  --restart unless-stopped \
  -p 9000:9000 \
  -p 9001:9001 \
  -v /data/rustfs:/data \
  -e RUSTFS_VOLUMES=/data/rustfs0,/data/rustfs1,/data/rustfs2,/data/rustfs3 \
  -e RUSTFS_ADDRESS=0.0.0.0:9000 \
  -e RUSTFS_CONSOLE_ENABLE=true \
  -e RUSTFS_CONSOLE_ADDRESS=0.0.0.0:9001 \
  -e RUSTFS_ACCESS_KEY=rustfsadmin \
  -e RUSTFS_SECRET_KEY=rustfsadmin \
  rustfs/rustfs:latest
```

### 2. Docker Compose Profiles

```bash
# Production deployment
docker-compose up -d

# Development with debugging
docker-compose --profile dev up -d

# With monitoring stack
docker-compose --profile observability up -d

# Complete testing environment
docker-compose --profile dev --profile observability --profile testing up -d
```

### 3. Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: rustfs
spec:
  replicas: 3
  selector:
    matchLabels:
      app: rustfs
  template:
    metadata:
      labels:
        app: rustfs
    spec:
      containers:
      - name: rustfs
        image: rustfs/rustfs:latest
        ports:
        - containerPort: 9000
        - containerPort: 9001
        env:
        - name: RUSTFS_VOLUMES
          value: "/data/rustfs0,/data/rustfs1,/data/rustfs2,/data/rustfs3"
        - name: RUSTFS_ADDRESS
          value: "0.0.0.0:9000"
        - name: RUSTFS_CONSOLE_ENABLE
          value: "true"
        - name: RUSTFS_CONSOLE_ADDRESS
          value: "0.0.0.0:9001"
        volumeMounts:
        - name: data
          mountPath: /data
      volumes:
      - name: data
        persistentVolumeClaim:
          claimName: rustfs-data
```

## ⚙️ Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `RUSTFS_VOLUMES` | Comma-separated list of data volumes | Required |
| `RUSTFS_ADDRESS` | Server bind address | `0.0.0.0:9000` |
| `RUSTFS_CONSOLE_ENABLE` | Enable web console | `false` |
| `RUSTFS_CONSOLE_ADDRESS` | Console bind address | `0.0.0.0:9001` |
| `RUSTFS_ACCESS_KEY` | S3 access key | `rustfsadmin` |
| `RUSTFS_SECRET_KEY` | S3 secret key | `rustfsadmin` |
| `RUSTFS_LOG_LEVEL` | Log level | `info` |
| `RUSTFS_OBS_ENDPOINT` | Observability endpoint | `""` |
| `RUSTFS_TLS_PATH` | TLS certificates path | `""` |

### Volume Mounts

- **Data volumes**: `/data/rustfs{0,1,2,3}` - RustFS data storage
- **Logs**: `/app/logs` - Application logs
- **Config**: `/etc/rustfs/` - Configuration files
- **TLS**: `/etc/ssl/rustfs/` - TLS certificates

### Ports

- **9000**: S3 API endpoint
- **9001**: Web console (if enabled)
- **9002**: Admin API (if enabled)
- **50051**: gRPC API (if enabled)

## 🔍 Monitoring and Observability

### Health Checks

The Docker images include built-in health checks:

```bash
# Check container health
docker ps --filter "name=rustfs" --format "table {{.Names}}\t{{.Status}}"

# View health check logs
docker inspect rustfs --format='{{json .State.Health}}'
```

### Metrics and Tracing

When using the observability profile:

- **Prometheus**: http://localhost:9090
- **Grafana**: http://localhost:3000 (admin/admin)
- **Jaeger**: http://localhost:16686
- **OpenTelemetry Collector**: http://localhost:8888/metrics

### Log Collection

```bash
# View container logs
docker logs rustfs -f

# Export logs
docker logs rustfs > rustfs.log 2>&1
```

## 🛠️ Development

### Development Environment

```bash
# Start development container
docker-compose --profile dev up -d rustfs-dev

# Access development container
docker exec -it rustfs-dev bash

# Mount source code for live development
docker run -it --rm \
  -v $(pwd):/root/s3-rustfs \
  -p 9000:9000 \
  rustfs/rustfs:devenv \
  bash
```

### Building from Source in Container

```bash
# Use development image for building
docker run --rm \
  -v $(pwd):/root/s3-rustfs \
  -w /root/s3-rustfs \
  rustfs/rustfs:ubuntu22.04 \
  cargo build --release --bin rustfs
```

### Testing Cross-Compilation

```bash
# Run the test script to verify cross-compilation setup
./scripts/test-cross-build.sh

# This will test:
# - x86_64-unknown-linux-musl compilation
# - aarch64-unknown-linux-gnu cross-compilation
# - Docker builds for both architectures
```

## 🔐 Security

### Security Scanning

The workflow includes Trivy security scanning:

```bash
# Run security scan locally
docker run --rm -v /var/run/docker.sock:/var/run/docker.sock \
  -v $HOME/Library/Caches:/root/.cache/ \
  aquasec/trivy:latest image rustfs/rustfs:latest
```

### Security Best Practices

1. **Use non-root user**: Images run as `rustfs` user (UID 1000)
2. **Minimal base images**: Ubuntu minimal for production
3. **Security updates**: Regular base image updates
4. **Secret management**: Use Docker secrets or environment files
5. **Network security**: Use Docker networks and proper firewall rules

## 📝 Troubleshooting

### Common Issues

#### 1. Cross-Compilation Failures

**Problem**: ARM64 build fails with linking errors
```bash
error: linking with `aarch64-linux-gnu-gcc` failed
```

**Solution**: Use the `cross` tool instead of native cross-compilation:
```bash
# Install cross tool
cargo install cross --git https://github.com/cross-rs/cross

# Use cross for ARM64 builds
cross build --release --target aarch64-unknown-linux-gnu --bin rustfs
```

#### 2. Protobuf Generation Issues

**Problem**: Missing protobuf definitions
```bash
error: failed to run custom build command for `protos`
```

**Solution**: Generate protobuf code first:
```bash
cargo run --bin gproto
```

#### 3. Docker Build Failures

**Problem**: Binary not found in Docker build
```bash
COPY failed: file not found in build context
```

**Solution**: Ensure binaries are built before Docker build:
```bash
# Build binaries first
cargo build --release --target x86_64-unknown-linux-musl --bin rustfs
cross build --release --target aarch64-unknown-linux-gnu --bin rustfs

# Then build Docker image
docker build .
```

### Debug Commands

```bash
# Check container status
docker ps -a

# View container logs
docker logs rustfs --tail 100

# Access container shell
docker exec -it rustfs bash

# Check resource usage
docker stats rustfs

# Inspect container configuration
docker inspect rustfs

# Test cross-compilation setup
./scripts/test-cross-build.sh
```

## 🔄 CI/CD Integration

### GitHub Actions

The provided workflow can be customized:

```yaml
# Override image names
env:
  REGISTRY_IMAGE_DOCKERHUB: myorg/rustfs
  REGISTRY_IMAGE_GHCR: ghcr.io/myorg/rustfs
```

### GitLab CI

```yaml
build:
  stage: build
  image: docker:latest
  services:
    - docker:dind
  script:
    - docker build -t $CI_REGISTRY_IMAGE:$CI_COMMIT_SHA .
    - docker push $CI_REGISTRY_IMAGE:$CI_COMMIT_SHA
```

### Jenkins Pipeline

```groovy
pipeline {
    agent any
    stages {
        stage('Build') {
            steps {
                script {
                    docker.build("rustfs:${env.BUILD_ID}")
                }
            }
        }
        stage('Push') {
            steps {
                script {
                    docker.withRegistry('https://registry.hub.docker.com', 'dockerhub-credentials') {
                        docker.image("rustfs:${env.BUILD_ID}").push()
                    }
                }
            }
        }
    }
}
```

## 📚 Additional Resources

- [Docker Official Documentation](https://docs.docker.com/)
- [Docker Compose Reference](https://docs.docker.com/compose/)
- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [Cross-compilation with Rust](https://rust-lang.github.io/rustup/cross-compilation.html)
- [Cross tool documentation](https://github.com/cross-rs/cross)
- [RustFS Configuration Guide](../README.md)
