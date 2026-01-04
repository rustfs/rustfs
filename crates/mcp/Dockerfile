FROM rust:1.88 AS builder

WORKDIR /build

COPY . .

RUN cargo build --release -p rustfs-mcp

FROM debian:bookworm-slim

WORKDIR /app

COPY --from=builder /build/target/release/rustfs-mcp /app/

RUN apt-get update && apt-get install -y ca-certificates && update-ca-certificates

ENTRYPOINT ["/app/rustfs-mcp"]
