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

FROM alpine:3.18 AS builder

RUN apk add -U --no-cache \
    ca-certificates \
    curl \
    bash \
    unzip

RUN curl -Lo /tmp/rustfs.zip https://dl.rustfs.com/artifacts/rustfs/rustfs-release-x86_64-unknown-linux-musl.latest.zip && \
    unzip /tmp/rustfs.zip -d /tmp && \
    mv /tmp/rustfs-release-x86_64-unknown-linux-musl/bin/rustfs /rustfs && \
    chmod +x /rustfs && \
    rm -rf /tmp/*

FROM alpine:3.18

RUN apk add -U --no-cache \
    ca-certificates \
    bash

COPY --from=builder /rustfs /usr/local/bin/rustfs

ENV RUSTFS_ACCESS_KEY=rustfsadmin \
    RUSTFS_SECRET_KEY=rustfsadmin \
    RUSTFS_ADDRESS=":9000" \
    RUSTFS_CONSOLE_ADDRESS=":9001" \
    RUSTFS_CONSOLE_ENABLE=true \
    RUST_LOG=warn

EXPOSE 9000 9001

RUN mkdir -p /data
VOLUME /data

CMD ["rustfs", "/data"]
