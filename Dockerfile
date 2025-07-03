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


# Create data directories
RUN mkdir -p /data/rustfs 


# Copy binary based on target architecture
COPY target/*/release/rustfs \
  /usr/local/bin/rustfs

RUN chmod +x /usr/local/bin/rustfs

ENV RUSTFS_ROOT_USER=rustfsadmin \
    RUSTFS_ROOT_PASSWORD=rustfsadmin \
    RUSTFS_ADDRESS=":9000" \
    RUSTFS_CONSOLE_ADDRESS=":9001" \
    RUSTFS_CONSOLE_ENABLE=true \
    RUST_LOG=warn


# Expose ports
EXPOSE 9000 9001

VOLUME /data

# Set default command
CMD ["rustfs", "/data"]
