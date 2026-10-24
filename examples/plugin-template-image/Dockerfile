FROM rust:1.99-trixie AS build
WORKDIR /src
RUN apt-get update && apt-get install -y --no-install-recommends pkg-config libvips-dev ca-certificates && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY src ./src
ENV CARGO_BUILD_JOBS=2
RUN cargo test --locked --release && cargo build --locked --release

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libvips42t64 && rm -rf /var/lib/apt/lists/* && mkdir -p /output && chown 65534:65534 /output
COPY --from=build /src/target/release/rustfs-image-transform /usr/local/bin/rustfs-image-transform
ENV VIPS_CONCURRENCY=1
USER 65534:65534
WORKDIR /output
CMD ["/usr/local/bin/rustfs-image-transform"]
