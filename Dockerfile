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

# Detect architecture and download corresponding binary
RUN case "${TARGETARCH}" in \
        amd64) ARCH="x86_64" ;; \
        arm64) ARCH="aarch64" ;; \
        *) echo "Unsupported architecture: ${TARGETARCH}" >&2 && exit 1 ;; \
    esac && \
    BASE_URL="https://dl.rustfs.com/artifacts/rustfs/release" && \
    PACKAGE_NAME="rustfs-linux-${ARCH}-${RELEASE#v}.zip" && \
    DOWNLOAD_URL="${BASE_URL}/${PACKAGE_NAME}" && \
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

USER rustfs
