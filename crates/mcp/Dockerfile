FROM rust:1.88 AS builder

WORKDIR /build

COPY . .

RUN cargo build --release -p rustfs-mcp

FROM debian:bookworm-slim

WORKDIR /app

COPY --from=builder /build/target/release/rustfs-mcp /app/

ENTRYPOINT ["/app/rustfs-mcp"]