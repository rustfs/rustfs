# Multi-stage build for RustFS production image
FROM alpine:3.18 AS build

# Build arguments
ARG TARGETARCH
ARG RELEASE=latest

# Install dependencies for downloading and verifying binaries
RUN apk add --no-cache \
    ca-certificates \
    curl \
    bash \
    wget \
    unzip \
    jq

# Create build directory
WORKDIR /build

# Map TARGETARCH to architecture format used in GitHub releases
RUN case "${TARGETARCH}" in \
        "amd64") ARCH="x86_64" ;; \
        "arm64") ARCH="aarch64" ;; \
        *) echo "Unsupported architecture: ${TARGETARCH}" && exit 1 ;; \
    esac && \
    echo "ARCH=${ARCH}" > /build/arch.env

# Download rustfs binary from GitHub Releases
RUN . /build/arch.env && \
    GITHUB_REPO="rustfs/rustfs" && \
    if [ "${RELEASE}" = "latest" ]; then \
        # Get latest release tag \
        LATEST_TAG=$(curl -s "https://api.github.com/repos/${GITHUB_REPO}/releases/latest" | jq -r '.tag_name'); \
        RELEASE_TAG="${LATEST_TAG}"; \
        echo "Latest release tag: ${RELEASE_TAG}"; \
    else \
        RELEASE_TAG="${RELEASE}"; \
        echo "Using specified release tag: ${RELEASE_TAG}"; \
    fi && \
    PACKAGE_NAME="rustfs-linux-${ARCH}-${RELEASE_TAG}.zip" && \
    DOWNLOAD_URL="https://github.com/${GITHUB_REPO}/releases/download/${RELEASE_TAG}/${PACKAGE_NAME}" && \
    echo "Downloading ${PACKAGE_NAME} from ${DOWNLOAD_URL}..." && \
    curl -s -L "${DOWNLOAD_URL}" -o /build/rustfs.zip && \
    if [ ! -f /build/rustfs.zip ] || [ ! -s /build/rustfs.zip ]; then \
        echo "❌ Failed to download binary package"; \
        echo "💡 Make sure the release ${RELEASE_TAG} exists and contains ${PACKAGE_NAME}"; \
        echo "🔗 Check: https://github.com/${GITHUB_REPO}/releases/tag/${RELEASE_TAG}"; \
        exit 1; \
    fi && \
    unzip /build/rustfs.zip -d /build && \
    chmod +x /build/rustfs && \
    rm /build/rustfs.zip

# Optional: Download and verify checksums if available
RUN . /build/arch.env && \
    GITHUB_REPO="rustfs/rustfs" && \
    if [ "${RELEASE}" = "latest" ]; then \
        LATEST_TAG=$(curl -s "https://api.github.com/repos/${GITHUB_REPO}/releases/latest" | jq -r '.tag_name'); \
        RELEASE_TAG="${LATEST_TAG}"; \
    else \
        RELEASE_TAG="${RELEASE}"; \
    fi && \
    CHECKSUM_FILE="SHA256SUMS" && \
    CHECKSUM_URL="https://github.com/${GITHUB_REPO}/releases/download/${RELEASE_TAG}/${CHECKSUM_FILE}" && \
    echo "Attempting to download checksums from ${CHECKSUM_URL}..." && \
    if curl -s -L "${CHECKSUM_URL}" -o /build/SHA256SUMS; then \
        echo "✅ Checksums downloaded, verifying binary..."; \
        cd /build && \
        PACKAGE_NAME="rustfs-linux-${ARCH}-${RELEASE_TAG}.zip" && \
        if grep -q "${PACKAGE_NAME}" SHA256SUMS; then \
            echo "${PACKAGE_NAME}" > temp_sums && \
            grep "${PACKAGE_NAME}" SHA256SUMS >> temp_sums && \
            sha256sum -c temp_sums || (echo "❌ Checksum verification failed" && exit 1); \
            echo "✅ Checksum verification passed"; \
        else \
            echo "⚠️  Checksum for ${PACKAGE_NAME} not found in SHA256SUMS, skipping verification"; \
        fi; \
    else \
        echo "⚠️  Checksums not available, skipping verification"; \
    fi

# Runtime stage
FROM alpine:3.18

# Set build arguments and labels
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
    summary="RustFS is a high-performance distributed object storage system written in Rust, compatible with S3 API." \
    description="RustFS is a high-performance distributed object storage software built using Rust. It supports erasure coding storage, multi-tenant management, observability, and other enterprise-level features." \
    url="https://rustfs.com" \
    license="Apache-2.0"

# Install runtime dependencies
RUN apk add --no-cache \
    ca-certificates \
    curl \
    tzdata \
    bash \
    && addgroup -g 1000 rustfs \
    && adduser -u 1000 -G rustfs -s /bin/sh -D rustfs

# Environment variables
ENV RUSTFS_ACCESS_KEY_FILE=access_key \
    RUSTFS_SECRET_KEY_FILE=secret_key \
    RUSTFS_ROOT_USER_FILE=access_key \
    RUSTFS_ROOT_PASSWORD_FILE=secret_key \
    RUSTFS_CONFIG_ENV_FILE=config.env \
    RUSTFS_ADDRESS=":9000" \
    RUSTFS_CONSOLE_ENABLE=true \
    RUSTFS_VOLUMES=/data

# Set permissions for /usr/bin (similar to MinIO's approach)
RUN chmod -R 777 /usr/bin

# Copy CA certificates and binaries from build stage
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /build/rustfs /usr/bin/

# Copy entrypoint script
COPY scripts/scripts/entrypoint.sh /usr/bin/entrypoint.sh
RUN chmod +x /usr/bin/entrypoint.sh /usr/bin/rustfs

# Create data directory
RUN mkdir -p /data /config && chown -R rustfs:rustfs /data /config

# Switch to non-root user
USER rustfs

# Set working directory
WORKDIR /data

# Expose port
EXPOSE 9000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --retries=3 --start-period=40s \
    CMD curl -f http://localhost:9000/health || exit 1

# Volume for data
VOLUME ["/data"]

# Set entrypoint
ENTRYPOINT ["/usr/bin/entrypoint.sh"]
CMD ["rustfs"]
