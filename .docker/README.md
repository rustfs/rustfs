# RustFS Docker Images

This directory contains Docker configuration files and supporting infrastructure for building and running RustFS container images.

## 📁 Directory Structure

```
rustfs/
├── Dockerfile           # Production image (Alpine + pre-built binaries)
├── Dockerfile.source    # Development image (Debian + source build)
├── docker-buildx.sh     # Multi-architecture build script
├── Makefile             # Build automation with simplified commands
└── .docker/             # Supporting infrastructure
    ├── observability/   # Monitoring and observability configs
    ├── compose/         # Docker Compose configurations
    ├── mqtt/            # MQTT broker configs
    └── openobserve-otel/ # OpenObserve + OpenTelemetry configs
```

## 🎯 Image Variants

### Core Images

| Image | Base OS | Build Method | Size | Use Case |
|-------|---------|--------------|------|----------|
| `production` (default) | Alpine 3.18 | GitHub Releases | Smallest | Production deployment |
| `source` | Debian Bookworm | Source build | Medium | Custom builds with cross-compilation |
| `dev` | Debian Bookworm | Development tools | Large | Interactive development |

## 🚀 Usage Examples

### Quick Start (Production)

```bash
# Default production image (Alpine + GitHub Releases)
docker run -p 9000:9000 rustfs/rustfs:latest

# Specific version
docker run -p 9000:9000 rustfs/rustfs:1.2.3
```

### Complete Tag Strategy Examples

```bash
# Stable Releases
docker run rustfs/rustfs:1.2.3           # Main version (production)
docker run rustfs/rustfs:1.2.3-production # Explicit production variant
docker run rustfs/rustfs:1.2.3-source   # Source build variant
docker run rustfs/rustfs:latest          # Latest stable

# Prerelease Versions
docker run rustfs/rustfs:1.3.0-alpha.2  # Specific alpha version
docker run rustfs/rustfs:alpha           # Latest alpha
docker run rustfs/rustfs:beta            # Latest beta
docker run rustfs/rustfs:rc              # Latest release candidate

# Development Versions
docker run rustfs/rustfs:dev             # Latest main branch development
docker run rustfs/rustfs:dev-13e4a0b     # Specific commit
docker run rustfs/rustfs:dev-latest      # Latest development
docker run rustfs/rustfs:main-latest     # Main branch latest
```

### Development Environment

```bash
# Quick setup using Makefile (recommended)
make docker-dev-local     # Build development image locally
make dev-env-start        # Start development container

# Manual Docker commands
docker run -it -v $(pwd):/workspace -p 9000:9000 rustfs/rustfs:latest-dev

# Build from source locally
docker build -f Dockerfile.source -t rustfs:custom .

# Development with hot reload
docker-compose up rustfs-dev
```

## 🏗️ Build Arguments and Scripts

### Using Makefile Commands (Recommended)

The easiest way to build images using simplified commands:

```bash
# Development images (build from source)
make docker-dev-local             # Build for local use (single arch)
make docker-dev                   # Build multi-arch (for CI/CD)
make docker-dev-push REGISTRY=xxx # Build and push to registry

# Production images (using pre-built binaries)
make docker-buildx                # Build multi-arch production images
make docker-buildx-push           # Build and push production images
make docker-buildx-version VERSION=v1.0.0  # Build specific version

# Development environment
make dev-env-start                # Start development container
make dev-env-stop                 # Stop development container
make dev-env-restart              # Restart development container

# Help
make help-docker                  # Show all Docker-related commands
```

### Using docker-buildx.sh (Advanced)

For direct script usage and advanced scenarios:

```bash
# Build latest version for all architectures
./docker-buildx.sh

# Build and push to registry
./docker-buildx.sh --push

# Build specific version
./docker-buildx.sh --release v1.2.3

# Build and push specific version
./docker-buildx.sh --release v1.2.3 --push
```

### Manual Docker Builds

All images support dynamic version selection:

```bash
# Build production image with latest release
docker build --build-arg RELEASE="latest" -t rustfs:latest .

# Build from source with specific target
docker build -f Dockerfile.source \
  --build-arg TARGETPLATFORM="linux/amd64" \
  -t rustfs:source .

# Development build
docker build -f Dockerfile.source -t rustfs:dev .
```

## 🔧 Binary Download Sources

### Unified GitHub Releases

The production image downloads from GitHub Releases for reliability and transparency:

- ✅ **production** → GitHub Releases API with automatic latest detection
- ✅ **Checksum verification** → SHA256SUMS validation when available
- ✅ **Multi-architecture** → Supports amd64 and arm64

### Source Build

The source variant compiles from source code with advanced features:

- 🔧 **Cross-compilation** → Supports multiple target platforms via `TARGETPLATFORM`
- ⚡ **Build caching** → sccache for faster compilation
- 🎯 **Optimized builds** → Release optimizations with LTO and symbol stripping

## 📋 Architecture Support

All variants support multi-architecture builds:

- **linux/amd64** (x86_64)
- **linux/arm64** (aarch64)

Architecture is automatically detected during build using Docker's `TARGETARCH` build argument.

## 🔐 Security Features

- **Checksum Verification**: Production image verifies SHA256SUMS when available
- **Non-root User**: All images run as user `rustfs` (UID 1000)
- **Minimal Runtime**: Production image only includes necessary dependencies
- **Secure Defaults**: No hardcoded credentials or keys

## 🛠️ Development Workflow

### Quick Start with Makefile (Recommended)

```bash
# 1. Start development environment
make dev-env-start

# 2. Your development container is now running with:
#    - Port 9000 exposed for RustFS
#    - Port 9010 exposed for admin console
#    - Current directory mounted as /workspace

# 3. Stop when done
make dev-env-stop
```

### Manual Development Setup

```bash
# Build development image from source
make docker-dev-local

# Or use traditional Docker commands
docker build -f Dockerfile.source -t rustfs:dev .

# Run with development tools
docker run -it -v $(pwd):/workspace -p 9000:9000 rustfs:dev bash

# Or use docker-compose for complex setups
docker-compose up rustfs-dev
```

### Common Development Tasks

```bash
# Build and test locally
make build                        # Build binary natively
make docker-dev-local            # Build development Docker image
make test                        # Run tests
make fmt                         # Format code
make clippy                      # Run linter

# Get help
make help                        # General help
make help-docker                 # Docker-specific help
make help-build                  # Build-specific help
```

## 🚀 CI/CD Integration

The project uses GitHub Actions for automated multi-architecture Docker builds:

### Automated Builds

- **Tags**: Automatic builds triggered on version tags (e.g., `v1.2.3`)
- **Main Branch**: Development builds with `dev-latest` and `main-latest` tags
- **Pull Requests**: Test builds without registry push

### Build Variants

Each build creates three image variants:

- `rustfs/rustfs:v1.2.3` (production - Alpine-based)
- `rustfs/rustfs:v1.2.3-source` (source build - Debian-based)
- `rustfs/rustfs:v1.2.3-dev` (development - Debian-based with tools)

### Manual Builds

Trigger custom builds via GitHub Actions:

```bash
# Use workflow_dispatch to build specific versions
# Available options: latest, main-latest, dev-latest, v1.2.3, dev-abc123
```

## 📦 Supporting Infrastructure

The `.docker/` directory contains supporting configuration files:

- **observability/** - Prometheus, Grafana, OpenTelemetry configs
- **compose/** - Multi-service Docker Compose setups
- **mqtt/** - MQTT broker configurations
- **openobserve-otel/** - Log aggregation and tracing setup

See individual README files in each subdirectory for specific usage instructions.
