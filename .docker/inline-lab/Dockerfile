# syntax=docker/dockerfile:1.6
# Copyright 2024 RustFS Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Server-only Linux validation image. The build context supplies a Git bundle.
FROM rust:1.100.0-trixie AS build-tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates clang lld libssl-dev pkg-config protobuf-compiler python3 python3-boto3 \
    && rm -rf /var/lib/apt/lists/*
ENV RUSTUP_TOOLCHAIN=1.100.0 \
    CARGO_BUILD_JOBS=4 \
    CARGO_NET_GIT_FETCH_WITH_CLI=true \
    CARGO_TARGET_DIR=/work/rustfs/target \
    RUSTFLAGS="-C link-arg=-fuse-ld=lld"

FROM build-tools AS build
ARG SOURCE_REVISION
COPY repository.bundle /tmp/repository.bundle
COPY inspect_geometry.rs /tmp/inspect_geometry.rs
RUN git clone /tmp/repository.bundle /work/rustfs \
    && git -C /work/rustfs checkout --detach "$SOURCE_REVISION" \
    && test "$(git -C /work/rustfs rev-parse HEAD)" = "$SOURCE_REVISION"
WORKDIR /work/rustfs
RUN --mount=type=cache,id=rustfs-inline-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=rustfs-inline-git,target=/usr/local/cargo/git \
    --mount=type=cache,id=rustfs-inline-linux-target,target=/work/rustfs/target \
    cargo build --locked --release --bin rustfs \
    && install -Dm755 target/release/rustfs /out/rustfs

# Build the diagnostic tool after preserving the clean-source server binary.
RUN cp /tmp/inspect_geometry.rs crates/filemeta/examples/inline_lab_inspect.rs
RUN --mount=type=cache,id=rustfs-inline-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=rustfs-inline-git,target=/usr/local/cargo/git \
    --mount=type=cache,id=rustfs-inline-linux-target,target=/work/rustfs/target \
    cargo build --locked --release -p rustfs-filemeta --example inline_lab_inspect \
    && install -Dm755 target/release/examples/inline_lab_inspect /out/inline_lab_inspect

FROM debian:trixie-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl libssl3t64 \
    && rm -rf /var/lib/apt/lists/*
ARG SOURCE_REVISION
LABEL org.opencontainers.image.revision="$SOURCE_REVISION" \
    rustfs.validation="inline"
COPY --from=build /out/rustfs /usr/local/bin/rustfs
COPY --from=build /out/inline_lab_inspect /usr/local/bin/inline_lab_inspect
ENTRYPOINT ["/usr/local/bin/rustfs"]
