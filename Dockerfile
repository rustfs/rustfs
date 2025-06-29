FROM alpine:latest

# Install runtime dependencies
RUN apk add --no-cache \
    ca-certificates \
    tzdata \
    && rm -rf /var/cache/apk/*

# Create rustfs user and group
RUN addgroup -g 1000 rustfs && \
    adduser -D -s /bin/sh -u 1000 -G rustfs rustfs

# Create data directories
RUN mkdir -p /data/rustfs && \
    chown -R rustfs:rustfs /data 

# Copy binary based on target architecture
COPY --chown=rustfs:rustfs \
  target/*/release/rustfs \
  /usr/local/bin/rustfs

RUN chmod +x /usr/local/bin/rustfs

# Switch to non-root user
USER rustfs

# Expose ports
EXPOSE 9000 9001

VOLUME /data

# Set default command
CMD ["rustfs", "/data"]
