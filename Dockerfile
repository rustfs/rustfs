FROM alpine:latest

# RUN apk add --no-cache <package-name>

WORKDIR /app

RUN mkdir -p /data/rustfs0  /data/rustfs1  /data/rustfs2  /data/rustfs3 

COPY ./target/x86_64-unknown-linux-musl/release/rustfs /app/rustfs

RUN chmod +x /app/rustfs

EXPOSE 9000
EXPOSE 9001


CMD ["/app/rustfs"]