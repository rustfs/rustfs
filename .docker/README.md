# RustFS Docker Images

This directory contains organized Dockerfile configurations for building RustFS container images across multiple platforms and system versions.

## 📁 Directory Structure

```
.docker/
├── alpine/                    # Alpine Linux variants
│   ├── Dockerfile.prebuild    # Alpine + GitHub Releases binaries
│   └── Dockerfile.source      # Alpine + source compilation
├── ubuntu/                    # Ubuntu variants
│   ├── Dockerfile.prebuild    # Ubuntu + GitHub Releases binaries
│   ├── Dockerfile.source      # Ubuntu + source compilation
│   └── Dockerfile.dev         # Ubuntu + development environment
└── cargo.config.toml         # Rust cargo configuration
```

## 🎯 Image Variants

### Production Images

| Variant | Base OS | Build Method | Size | Use Case |
|---------|---------|--------------|------|----------|
| `production` (default) | Alpine 3.18 | GitHub Releases | Smallest | Production deployment |
| `alpine` | Alpine 3.18 | GitHub Releases | Small | Explicit Alpine choice |
| `alpine-source` | Alpine 3.18 | Source build | Small | Custom Alpine builds |
| `ubuntu` | Ubuntu 22.04 | GitHub Releases | Medium | Ubuntu environments |
| `ubuntu-source` | Ubuntu 22.04 | Source build | Medium | Full Ubuntu compatibility |

### Development Images

| Variant | Base OS | Features | Use Case |
|---------|---------|----------|----------|
| `ubuntu-dev` | Ubuntu 22.04 | Full toolchain + dev tools | Interactive development |

## 🚀 Usage Examples

### Quick Start (Production)

```bash
# Default production image (Alpine + GitHub Releases)
docker run -p 9000:9000 rustfs/rustfs:latest

# Specific version with production variant
docker run -p 9000:9000 rustfs/rustfs:1.2.3-production

# Explicit Alpine variant
docker run -p 9000:9000 rustfs/rustfs:latest-alpine

# Ubuntu-based production
docker run -p 9000:9000 rustfs/rustfs:latest-ubuntu
```

### Complete Tag Strategy Examples

```bash
# Stable Releases
docker run rustfs/rustfs:1.2.3                # Main version (production)
docker run rustfs/rustfs:1.2.3-production     # Explicit production variant
docker run rustfs/rustfs:1.2.3-alpine         # Explicit Alpine variant
docker run rustfs/rustfs:1.2.3-alpine-source  # Alpine source build
docker run rustfs/rustfs:latest               # Latest stable

# Prerelease Versions
docker run rustfs/rustfs:1.3.0-alpha.2        # Specific alpha version
docker run rustfs/rustfs:1.3.0-alpha.2-alpine # Alpha with Alpine
docker run rustfs/rustfs:alpha                # Latest alpha
docker run rustfs/rustfs:beta                 # Latest beta
docker run rustfs/rustfs:rc                   # Latest release candidate

# Development Versions
docker run rustfs/rustfs:dev                  # Latest development
docker run rustfs/rustfs:dev-13e4a0b          # Specific commit
docker run rustfs/rustfs:dev-alpine           # Development Alpine
```

### Development Environment

```bash
# Start development container
docker run -it -v $(pwd):/app -p 9000:9000 rustfs/rustfs:latest-ubuntu-dev

# Inside container:
cd /app
cargo build --release
cargo run
```

## 🏗️ Build Arguments

All images support dynamic version selection from GitHub Releases:

```bash
# Build with latest release
docker build \
  --build-arg VERSION="latest" \
  --build-arg BUILD_TYPE="release" \
  -f .docker/alpine/Dockerfile.prebuild \
  -t rustfs:latest-alpine .

# Build with specific version
docker build \
  --build-arg VERSION="v1.0.0" \
  --build-arg BUILD_TYPE="release" \
  -f .docker/ubuntu/Dockerfile.prebuild \
  -t rustfs:1.0.0-ubuntu .
```

## 🔧 Binary Download Sources

### Unified GitHub Releases

All prebuild variants now download from GitHub Releases for consistency:

- ✅ **production** (root Dockerfile) → GitHub Releases
- ✅ **alpine** → GitHub Releases
- ✅ **ubuntu** → GitHub Releases

### Source Build Variants

Source variants compile from source code:

- **alpine-source** → Compile from source on Alpine
- **ubuntu-source** → Compile from source on Ubuntu

## 📋 Architecture Support

All variants support multi-architecture builds:

- **linux/amd64** (x86_64)
- **linux/arm64** (aarch64)

Architecture is automatically detected during build using `TARGETARCH` build argument.

## 🔐 Security Features

- **Checksum Verification**: All prebuild variants verify SHA256SUMS when available
- **Non-root User**: All images run as user `rustfs` (UID 1000)
- **Minimal Runtime**: Runtime images only include necessary dependencies

## 🛠️ Development Workflow

For local development and testing:

```bash
# Build from source (development)
docker build -f .docker/alpine/Dockerfile.source -t rustfs:dev-alpine .

# Run with development tools
docker run -it -v $(pwd):/workspace rustfs:dev-alpine bash
```
