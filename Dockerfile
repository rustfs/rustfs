# Multi-stage build for RustFS production image

# Build stage: Download and extract RustFS binary
FROM alpine:3.22 AS build

# Build arguments for platform and release
ARG TARGETARCH
ARG RELEASE=latest

# Install minimal dependencies for downloading and extracting
RUN apk add --no-cache ca-certificates curl unzip

# Create build directory
WORKDIR /build

# Set architecture-specific variables
RUN if [ "$TARGETARCH" = "amd64" ]; then \
        echo "x86_64-musl" > /tmp/arch; \
    elif [ "$TARGETARCH" = "arm64" ]; then \
        echo "aarch64-musl" > /tmp/arch; \
    else \
        echo "unsupported" > /tmp/arch; \
    fi
RUN ARCH=$(cat /tmp/arch) && \
    if [ "$ARCH" = "unsupported" ]; then \
        echo "Unsupported architecture: $TARGETARCH" && exit 1; \
    fi && \
    if [ "${RELEASE}" = "latest" ]; then \
        # For latest, download from GitHub releases using the -latest suffix
        PACKAGE_NAME="rustfs-linux-${ARCH}-latest.zip"; \
        # Use GitHub API to get the latest release URL
        LATEST_RELEASE_URL=$(curl -s https://api.github.com/repos/rustfs/rustfs/releases/latest | grep -o '"browser_download_url": "[^"]*'"${PACKAGE_NAME}"'"' | cut -d'"' -f4 | head -1); \
        if [ -z "$LATEST_RELEASE_URL" ]; then \
            echo "Failed to find latest release for ${PACKAGE_NAME}" >&2; \
            exit 1; \
        fi; \
        DOWNLOAD_URL="$LATEST_RELEASE_URL"; \
    else \
        # For specific versions, construct the GitHub release URL directly
        VERSION="v${RELEASE#v}"; \
        PACKAGE_NAME="rustfs-linux-${ARCH}-${VERSION}.zip"; \
        DOWNLOAD_URL="https://github.com/rustfs/rustfs/releases/download/${VERSION}/${PACKAGE_NAME}"; \
    fi && \
    echo "Downloading ${PACKAGE_NAME} from ${DOWNLOAD_URL}" >&2 && \
    curl -f -L "${DOWNLOAD_URL}" -o rustfs.zip && \
    unzip rustfs.zip -d /build && \
    chmod +x /build/rustfs && \
    rm rustfs.zip || { echo "Failed to download or extract ${PACKAGE_NAME}" >&2; exit 1; }

# Runtime stage: Configure runtime environment
FROM alpine:3.22.1

# Build arguments and labels
ARG RELEASE=latest
ARG BUILD_DATE
ARG VCS_REF

LABEL name="RustFS" \
      vendor="RustFS Team" \
      maintainer="RustFS Team <dev@rustfs.com>" \
      version="${RELEASE}" \
      release="${RELEASE}" \
      build-date="${BUILD_DATE}" \
      vcs-ref="${VCS_REF}" \
      summary="High-performance distributed object storage system compatible with S3 API" \
      description="RustFS is a distributed object storage system written in Rust, supporting erasure coding, multi-tenant management, and observability." \
      url="https://rustfs.com" \
      license="Apache-2.0"

# Install runtime dependencies
RUN echo "https://dl-cdn.alpinelinux.org/alpine/v3.20/community" >> /etc/apk/repositories && \
    apk update && \
    apk add --no-cache ca-certificates bash gosu coreutils shadow && \
    addgroup -g 1000 rustfs && \
    adduser -u 1000 -G rustfs -s /bin/bash -D rustfs

# Copy CA certificates and RustFS binary from build stage
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /build/rustfs /usr/bin/rustfs

# Copy entry point script
COPY entrypoint.sh /entrypoint.sh

# Set permissions
RUN chmod +x /usr/bin/rustfs /entrypoint.sh && \
    mkdir -p /data /logs && \
    chown rustfs:rustfs /data /logs && \
    chmod 700 /data /logs

# Environment variables (credentials should be set via environment or secrets)
ENV RUSTFS_ADDRESS=:9000 \
    RUSTFS_ACCESS_KEY=rustfsadmin \
    RUSTFS_SECRET_KEY=rustfsadmin \
    RUSTFS_CONSOLE_ENABLE=true \
    RUSTFS_VOLUMES=/data \
    RUST_LOG=warn \
    RUSTFS_OBS_LOG_DIRECTORY=/logs \
    RUSTFS_SINKS_FILE_PATH=/logs

# Expose port
EXPOSE 9000

# Volumes for data and logs
VOLUME ["/data", "/logs"]

# Set entry point
ENTRYPOINT ["/entrypoint.sh"]
CMD ["/usr/bin/rustfs"]

