# Copyright 2024 RustFS Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM alpine:3.18 AS builder

# Build arguments for dynamic artifact download
ARG VERSION=""
ARG BUILD_TYPE="release"
ARG TARGETARCH

RUN apk add -U --no-cache \
    ca-certificates \
    curl \
    bash \
    unzip

# Generate correct filename and path based on build type and version
RUN set -ex; \
    # Map TARGETARCH to our naming convention
    case "${TARGETARCH}" in \
        amd64) ARCH="x86_64" ;; \
        arm64) ARCH="aarch64" ;; \
        *) echo "Unsupported architecture: ${TARGETARCH}" && exit 1 ;; \
    esac; \
    \
    # Determine download path and filename based on build type
    if [ "${BUILD_TYPE}" = "development" ]; then \
        # Development build: artifacts/rustfs/dev/rustfs-linux-{arch}-dev-{sha}.zip
        if [ -z "${VERSION}" ]; then \
            echo "VERSION must be provided for development builds" && exit 1; \
        fi; \
        DOWNLOAD_PATH="artifacts/rustfs/dev"; \
        FILENAME="rustfs-linux-${ARCH}-dev-${VERSION}.zip"; \
    else \
        # Release/Prerelease build: artifacts/rustfs/release/rustfs-linux-{arch}-v{version}.zip
        if [ -z "${VERSION}" ]; then \
            echo "VERSION must be provided for release builds" && exit 1; \
        fi; \
        DOWNLOAD_PATH="artifacts/rustfs/release"; \
        FILENAME="rustfs-linux-${ARCH}-v${VERSION}.zip"; \
    fi; \
    \
    # Download the appropriate binary
    DOWNLOAD_URL="https://dl.rustfs.com/${DOWNLOAD_PATH}/${FILENAME}"; \
    echo "Downloading RustFS binary from: ${DOWNLOAD_URL}"; \
    curl -Lo /tmp/rustfs.zip "${DOWNLOAD_URL}" || { \
        echo "Failed to download ${DOWNLOAD_URL}"; \
        echo "Available files might be:"; \
        echo "  Release: https://dl.rustfs.com/artifacts/rustfs/release/rustfs-linux-${ARCH}-v{version}.zip"; \
        echo "  Dev: https://dl.rustfs.com/artifacts/rustfs/dev/rustfs-linux-${ARCH}-dev-{sha}.zip"; \
        exit 1; \
    }; \
    unzip -o /tmp/rustfs.zip -d /tmp; \
    mv /tmp/rustfs /rustfs; \
    chmod +x /rustfs; \
    rm -rf /tmp/*

FROM alpine:3.18

RUN apk add -U --no-cache \
    ca-certificates \
    bash

COPY --from=builder /rustfs /usr/local/bin/rustfs

ENV RUSTFS_ACCESS_KEY=rustfsadmin \
    RUSTFS_SECRET_KEY=rustfsadmin \
    RUSTFS_ADDRESS=":9000" \
    RUSTFS_CONSOLE_ENABLE=true \
    RUST_LOG=warn

EXPOSE 9000

RUN mkdir -p /data
VOLUME /data

CMD ["rustfs", "/data"]
