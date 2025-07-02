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
