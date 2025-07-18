# Multi-stage build for RustFS production image
FROM alpine:latest AS build

# Build arguments - use TARGETPLATFORM for consistency with Dockerfile.source
ARG TARGETPLATFORM
ARG BUILDPLATFORM
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

# Map TARGETPLATFORM to architecture format used in builds
RUN case "${TARGETPLATFORM}" in \
        "linux/amd64") ARCH="x86_64" ;; \
        "linux/arm64") ARCH="aarch64" ;; \
        *) echo "Unsupported platform: ${TARGETPLATFORM}" && exit 1 ;; \
    esac && \
    echo "ARCH=${ARCH}" > /build/arch.env

# Download rustfs binary from dl.rustfs.com (release channel only)
RUN . /build/arch.env && \
    BASE_URL="https://dl.rustfs.com/artifacts/rustfs/release" && \
    PLATFORM="linux" && \
    if [ "${RELEASE}" = "latest" ]; then \
        # Download latest release version \
        PACKAGE_NAME="rustfs-${PLATFORM}-${ARCH}-latest.zip"; \
        DOWNLOAD_URL="${BASE_URL}/${PACKAGE_NAME}"; \
        echo "📥 Downloading latest release build: ${PACKAGE_NAME}"; \
    else \
        # Download specific release version \
        PACKAGE_NAME="rustfs-${PLATFORM}-${ARCH}-v${RELEASE}.zip"; \
        DOWNLOAD_URL="${BASE_URL}/${PACKAGE_NAME}"; \
        echo "📥 Downloading specific release version: ${PACKAGE_NAME}"; \
    fi && \
    echo "🔗 Download URL: ${DOWNLOAD_URL}" && \
    curl -f -L "${DOWNLOAD_URL}" -o /build/rustfs.zip && \
    if [ ! -f /build/rustfs.zip ] || [ ! -s /build/rustfs.zip ]; then \
        echo "❌ Failed to download binary package"; \
        echo "💡 Make sure the package ${PACKAGE_NAME} exists"; \
        echo "🔗 Check: ${DOWNLOAD_URL}"; \
        exit 1; \
    fi && \
    unzip /build/rustfs.zip -d /build && \
    chmod +x /build/rustfs && \
    rm /build/rustfs.zip && \
    echo "✅ Successfully downloaded and extracted rustfs binary"

# Runtime stage
FROM alpine:latest

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
    bash \
    && addgroup -g 1000 rustfs \
    && adduser -u 1000 -G rustfs -s /bin/sh -D rustfs

# Environment variables
ENV RUSTFS_ACCESS_KEY=rustfsadmin \
    RUSTFS_SECRET_KEY=rustfsadmin \
    RUSTFS_ADDRESS=":9000" \
    RUSTFS_CONSOLE_ENABLE=true \
    RUSTFS_VOLUMES=/data \
    RUST_LOG=warn \
    RUSTFS_OBS_LOG_DIRECTORY=/logs

# Set permissions for /usr/bin (similar to MinIO's approach)
RUN chmod -R 755 /usr/bin

# Copy CA certificates and binaries from build stage
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /build/rustfs /usr/bin/

# Set executable permissions
RUN chmod +x /usr/bin/rustfs

# Create data directory
RUN mkdir -p /data /logs && chown -R rustfs:rustfs /data /logs

# Switch to non-root user
USER rustfs

# Set working directory
WORKDIR /data

# Expose port
EXPOSE 9000


# Volume for data
VOLUME ["/data"]

# Set entrypoint
ENTRYPOINT ["/usr/bin/rustfs"]
