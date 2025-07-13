#!/bin/bash

# Download static curl binary for Docker runtime
# This script downloads a statically compiled curl binary for use in minimal containers

set -e

# Determine architecture
ARCH=$(uname -m)
case $ARCH in
    x86_64)
        CURL_ARCH="x64"
        ;;
    aarch64)
        CURL_ARCH="aarch64"
        ;;
    armv7l)
        CURL_ARCH="armv7"
        ;;
    *)
        echo "Unsupported architecture: $ARCH"
        exit 1
        ;;
esac

# Download static curl
CURL_VERSION="8.11.0"
CURL_URL="https://github.com/moparisthebest/static-curl/releases/download/v${CURL_VERSION}/curl-${CURL_ARCH}"

echo "Downloading static curl for ${ARCH}..."
wget -q -O /usr/bin/curl "${CURL_URL}"
chmod +x /usr/bin/curl

# Verify curl works
if /usr/bin/curl --version > /dev/null 2>&1; then
    echo "Static curl installed successfully"
else
    echo "Failed to install static curl"
    exit 1
fi
