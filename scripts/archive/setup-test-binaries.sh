#!/usr/bin/env bash

# Setup test binaries for Docker build testing
# This script creates temporary binary files for testing Docker build process

set -e

echo "Setting up test binaries for Docker build..."

# Create temporary rustfs binary
./build-rustfs.sh -p x86_64-unknown-linux-gnu

# Create test directory structure
mkdir -p test-releases/server/rustfs/release/linux-amd64/archive
mkdir -p test-releases/server/rustfs/release/linux-arm64/archive

# Get version
VERSION=$(git describe --abbrev=0 --tags 2>/dev/null || git rev-parse --short HEAD)

# Copy binaries
cp target/x86_64-unknown-linux-gnu/release/rustfs test-releases/server/rustfs/release/linux-amd64/archive/rustfs.${VERSION}
cp target/x86_64-unknown-linux-gnu/release/rustfs.sha256sum test-releases/server/rustfs/release/linux-amd64/archive/rustfs.${VERSION}.sha256sum

# Create dummy signatures
echo "dummy signature" > test-releases/server/rustfs/release/linux-amd64/archive/rustfs.${VERSION}.minisig
echo "dummy signature" > test-releases/server/rustfs/release/linux-arm64/archive/rustfs.${VERSION}.minisig

# Also copy for arm64 (using same binary for testing)
cp target/aarch64-unknown-linux-gnu/release/rustfs test-releases/server/rustfs/release/linux-arm64/archive/rustfs.${VERSION}
cp target/aarch64-unknown-linux-gnu/release/rustfs.sha256sum test-releases/server/rustfs/release/linux-arm64/archive/rustfs.${VERSION}.sha256sum

echo "Test binaries created for version: ${VERSION}"
echo "You can now test Docker builds with these local binaries"
echo ""
echo "To start a local HTTP server for testing:"
echo "  cd test-releases && python3 -m http.server 8000"
echo ""
echo "Then modify Dockerfile to use http://host.docker.internal:8000 instead of https://dl.rustfs.com/artifacts/rustfs"
