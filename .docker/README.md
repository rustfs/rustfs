# RustFS Docker Images

This directory contains organized Dockerfile configurations for building RustFS container images across multiple platforms and system versions.

## 📁 Directory Structure

```
.docker/
├── alpine/                    # Alpine Linux variants
│   ├── Dockerfile.prebuild    # Alpine + pre-built binaries
│   └── Dockerfile.source      # Alpine + source compilation
├── ubuntu/                    # Ubuntu variants
│   ├── Dockerfile.prebuild    # Ubuntu + pre-built binaries
│   ├── Dockerfile.source      # Ubuntu + source compilation
│   └── Dockerfile.dev         # Ubuntu + development environment
└── cargo.config.toml         # Rust cargo configuration
```

## 🎯 Image Variants

### Production Images

| Variant | Base OS | Build Method | Size | Use Case |
|---------|---------|--------------|------|----------|
| `production` (default) | Alpine 3.18 | Pre-built | Smallest | Production deployment |
| `alpine` | Alpine 3.18 | Pre-built | Small | Explicit Alpine choice |
| `alpine-source` | Alpine 3.18 | Source build | Small | Custom Alpine builds |
| `ubuntu` | Ubuntu 22.04 | Pre-built | Medium | Ubuntu environments |
| `ubuntu-source` | Ubuntu 22.04 | Source build | Medium | Full Ubuntu compatibility |

### Development Images

| Variant | Base OS | Features | Use Case |
|---------|---------|----------|----------|
| `ubuntu-dev` | Ubuntu 22.04 | Full toolchain + dev tools | Interactive development |

## 🚀 Usage Examples

### Quick Start (Production)

```bash
# Default production image (Alpine + pre-built)
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

All images support dynamic version selection:

```bash
# Build with specific version
docker build \
  --build-arg VERSION="1.0.0" \
  --build-arg BUILD_TYPE="release" \
  -f .docker/alpine/Dockerfile.prebuild \
  -t rustfs:1.0.0-alpine .
```

## 🌐 Multi-Platform Support

All images support multiple architectures:

- `linux/amd64` (Intel/AMD 64-bit)
- `linux/arm64` (ARM 64-bit, Apple Silicon, etc.)

## ⚡ Build Speed Optimizations

### Docker Build Optimizations

- **Multi-layer caching**: GitHub Actions cache + Registry cache
- **Parallel matrix builds**: All 5 variants build simultaneously
- **Multi-platform builds**: amd64/arm64 built in parallel
- **BuildKit features**: Advanced caching and inline cache

### Rust Compilation Optimizations

- **sccache**: Distributed compilation cache for Rust builds
- **Parallel compilation**: Uses all available CPU cores (`-j $(nproc)`)
- **Optimized cargo config**: Sparse registry protocol, fast linker (lld)
- **Dependency caching**: Separate Docker layers for dependencies vs. source code
- **Release optimizations**: LTO, strip symbols, optimized codegen

### Cache Strategy

```yaml
# GitHub Actions cache
cache-from: type=gha,scope=docker-{variant}
cache-to: type=gha,mode=max,scope=docker-{variant}

# Registry cache (persistent across runs)
cache-from: type=registry,ref=ghcr.io/rustfs/rustfs:buildcache-{variant}
cache-to: type=registry,ref=ghcr.io/rustfs/rustfs:buildcache-{variant}
```

### Build Performance Comparison

| Build Type | Time (Est.) | Cache Hit | Cache Miss |
|------------|-------------|-----------|-----------|
| Production (Alpine pre-built) | ~2-3 min | ~1 min | ~2 min |
| Alpine pre-built | ~2-3 min | ~1 min | ~2 min |
| Alpine source | ~8-12 min | ~3-5 min | ~10 min |
| Ubuntu pre-built | ~3-4 min | ~1-2 min | ~3 min |
| Ubuntu source | ~10-15 min | ~4-6 min | ~12 min |

## 📋 Build Matrix

| Trigger | Version Format | Download Path | Image Tags |
|---------|---------------|---------------|------------|
| `push main` | `dev-{sha}` | `artifacts/rustfs/dev/` | `dev-{sha}-{variant}`, `dev-{variant}`, `dev` |
| `push 1.2.3` | `1.2.3` | `artifacts/rustfs/release/` | `1.2.3-{variant}`, `1.2.3`, `latest-{variant}`, `latest` |
| `push 1.3.0-alpha.2` | `1.3.0-alpha.2` | `artifacts/rustfs/release/` | `1.3.0-alpha.2-{variant}`, `alpha-{variant}`, `alpha` |
| `push 1.3.0-beta.1` | `1.3.0-beta.1` | `artifacts/rustfs/release/` | `1.3.0-beta.1-{variant}`, `beta-{variant}`, `beta` |
| `push 1.3.0-rc.1` | `1.3.0-rc.1` | `artifacts/rustfs/release/` | `1.3.0-rc.1-{variant}`, `rc-{variant}`, `rc` |
