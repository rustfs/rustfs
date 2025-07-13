# RustFS Docker Images

This directory contains Docker configuration files and supporting infrastructure for building and running RustFS container images.

## 📁 Directory Structure

```
rustfs/
├── Dockerfile           # Production image (Alpine + GitHub Releases)
├── Dockerfile.source    # Source build (Debian + cross-compilation)
├── cargo.config.toml    # Rust cargo configuration
├── docker-buildx.sh     # Multi-architecture build script
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
# Start development container
docker run -it -v $(pwd):/workspace -p 9000:9000 rustfs/rustfs:latest-dev

# Build from source locally
docker build -f Dockerfile.source -t rustfs:custom .

# Development with hot reload
docker-compose up rustfs-dev
```

## 🏗️ Build Arguments and Scripts

### Using docker-buildx.sh (Recommended)

For multi-architecture builds, use the provided script:

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

For local development and testing:

```bash
# Quick development setup
docker-compose up rustfs-dev

# Custom source build
docker build -f Dockerfile.source -t rustfs:custom .

# Run with development tools
docker run -it -v $(pwd):/workspace rustfs:custom bash
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
