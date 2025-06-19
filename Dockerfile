FROM alpine:latest

# Install runtime dependencies
RUN apk add --no-cache \
    ca-certificates \
    tzdata \
    && rm -rf /var/cache/apk/*

# Create rustfs user and group
RUN addgroup -g 1000 rustfs && \
    adduser -D -s /bin/sh -u 1000 -G rustfs rustfs

WORKDIR /app

# Create data directories
RUN mkdir -p /data/rustfs{0,1,2,3} && \
    chown -R rustfs:rustfs /data /app

# Copy binary based on target architecture
COPY --chown=rustfs:rustfs \
  target/*/release/rustfs \
  /app/rustfs

RUN chmod +x /app/rustfs

# Switch to non-root user
USER rustfs

# Expose ports
EXPOSE 9000 9001

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:9000/health || exit 1

# Set default command
CMD ["/app/rustfs"]
