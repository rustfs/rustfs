FROM alpine:3.22 AS build

ARG TARGETARCH
ARG RELEASE=latest

RUN apk add --no-cache ca-certificates curl unzip
WORKDIR /build

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


FROM alpine:3.22

ARG RELEASE=latest
ARG BUILD_DATE
ARG VCS_REF

LABEL name="RustFS" \
      vendor="RustFS Team" \
      maintainer="RustFS Team <dev@rustfs.com>" \
      version="v${RELEASE#v}" \
      release="${RELEASE}" \
      build-date="${BUILD_DATE}" \
      vcs-ref="${VCS_REF}" \
      summary="High-performance distributed object storage system compatible with S3 API" \
      description="RustFS is a distributed object storage system written in Rust, supporting erasure coding, multi-tenant management, and observability." \
      url="https://rustfs.com" \
      license="Apache-2.0"

RUN apk add --no-cache ca-certificates coreutils

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /build/rustfs /usr/bin/rustfs
COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /usr/bin/rustfs /entrypoint.sh && \
    mkdir -p /data /logs && \
    chmod 0750 /data /logs

ENV RUSTFS_ADDRESS=":9000" \
    RUSTFS_ACCESS_KEY="rustfsadmin" \
    RUSTFS_SECRET_KEY="rustfsadmin" \
    RUSTFS_CONSOLE_ENABLE="true" \
    RUSTFS_VOLUMES="/data" \
    RUST_LOG="warn" \
    RUSTFS_OBS_LOG_DIRECTORY="/logs" \
    RUSTFS_SINKS_FILE_PATH="/logs"

EXPOSE 9000
VOLUME ["/data", "/logs"]

ENTRYPOINT ["/entrypoint.sh"]

CMD ["rustfs"]
