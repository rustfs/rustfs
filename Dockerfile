# -------------------
# Build stage
# -------------------
FROM alpine:3.22 AS build

ARG TARGETARCH
ARG RELEASE=latest

RUN apk add --no-cache ca-certificates curl unzip
WORKDIR /build

# Download and extract release package matching current TARGETARCH
# - If RELEASE=latest: take first tag_name from /releases (may include pre-releases)
# - Otherwise use specified tag (e.g. v0.1.2)
RUN set -eux; \
    case "$TARGETARCH" in \
      amd64)  ARCH_SUBSTR="x86_64-musl"  ;; \
      arm64)  ARCH_SUBSTR="aarch64-musl" ;; \
      *) echo "Unsupported TARGETARCH=$TARGETARCH" >&2; exit 1 ;; \
    esac; \
    if [ "$RELEASE" = "latest" ]; then \
      TAG="$(curl -fsSL https://api.github.com/repos/rustfs/rustfs/releases \
              | grep -o '"tag_name": "[^"]*"' | cut -d'"' -f4 | head -n 1)"; \
    else \
      TAG="$RELEASE"; \
    fi; \
    echo "Using tag: $TAG (arch pattern: $ARCH_SUBSTR)"; \
    # Find download URL in assets list for this tag that contains arch substring and ends with .zip
    URL="$(curl -fsSL "https://api.github.com/repos/rustfs/rustfs/releases/tags/$TAG" \
           | grep -o "\"browser_download_url\": \"[^\"]*${ARCH_SUBSTR}[^\"]*\\.zip\"" \
           | cut -d'"' -f4 | head -n 1)"; \
    if [ -z "$URL" ]; then echo "Failed to locate release asset for $ARCH_SUBSTR at tag $TAG" >&2; exit 1; fi; \
    echo "Downloading: $URL"; \
    curl -fL "$URL" -o rustfs.zip; \
    unzip -q rustfs.zip -d /build; \
    # If binary is not in root directory, try to locate and move from zip to /build/rustfs
    if [ ! -x /build/rustfs ]; then \
      BIN_PATH="$(unzip -Z -1 rustfs.zip | grep -E '(^|/)rustfs$' | head -n 1 || true)"; \
      if [ -n "$BIN_PATH" ]; then \
        mkdir -p /build/.tmp && unzip -q rustfs.zip "$BIN_PATH" -d /build/.tmp && \
        mv "/build/.tmp/$BIN_PATH" /build/rustfs; \
      fi; \
    fi; \
    [ -x /build/rustfs ] || { echo "rustfs binary not found in asset" >&2; exit 1; }; \
    chmod +x /build/rustfs; \
    rm -rf rustfs.zip /build/.tmp || true


# -------------------
# Runtime stage
# -------------------
FROM alpine:3.22

ARG RELEASE=latest
ARG BUILD_DATE
ARG VCS_REF

LABEL name="RustFS" \
      maintainer="RustFS Team" \
      version="${RELEASE}" \
      build-date="${BUILD_DATE}" \
      vcs-ref="${VCS_REF}" \
      description="RustFS - Distributed object storage (S3 compatible)"

# Install only runtime requirements: certificates and coreutils (provides chroot --userspec)
RUN apk add --no-cache ca-certificates coreutils && \
    addgroup -g 1000 rustfs && \
    adduser  -u 1000 -G rustfs -s /sbin/nologin -D rustfs

# Copy binary and entry script (ensure fixed entrypoint.sh exists in repository)
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /build/rustfs /usr/bin/rustfs
COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /usr/bin/rustfs /entrypoint.sh && \
    mkdir -p /data /logs && \
    chown rustfs:rustfs /data /logs && \
    chmod 0750 /data /logs

# Default environment (can be overridden in docker run/compose)
ENV RUSTFS_ADDRESS=":9000" \
    RUSTFS_ACCESS_KEY="rustfsadmin" \
    RUSTFS_SECRET_KEY="rustfsadmin" \
    RUSTFS_CONSOLE_ENABLE="true" \
    RUSTFS_VOLUMES="/data" \
    RUST_LOG="warn" \
    RUSTFS_OBS_LOG_DIRECTORY="/logs" \
    RUSTFS_SINKS_FILE_PATH="/logs" \
    RUSTFS_USERNAME="rustfs" \
    RUSTFS_GROUPNAME="rustfs" \
    RUSTFS_UID="1000" \
    RUSTFS_GID="1000"

EXPOSE 9000
VOLUME ["/data", "/logs"]

ENTRYPOINT ["/entrypoint.sh"]
CMD ["/usr/bin/rustfs"]
