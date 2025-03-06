# How to compile RustFS

| Must package | Version |
|--------------|---------|
| Rust         | 1.8.5   |
| protoc       | 27.0    |
| flatc        | 24.0+   |

Download Links:

https://github.com/google/flatbuffers/releases/download/v24.3.25/Linux.flatc.binary.g++-13.zip

https://github.com/protocolbuffers/protobuf/releases/download/v27.0/protoc-27.0-linux-x86_64.zip

Or use Docker:

- uses: arduino/setup-protoc@v3
  with:
  version: "27.0"

- uses: Nugine/setup-flatc@v1
  with:
  version: "24.3.25"

# How to add Console web

1. wget [http://dl.rustfs.com/console/console.latest.tar.gz](https://dl.rustfs.com/console/rustfs-console-latest.zip)

2. mkdir in this repos folder `./rustfs/static`

3. Compile RustFS

# Star RustFS

Add Env infomation:

```
export RUST_LOG="rustfs=debug,ecstore=debug,s3s=debug,iam=debug"
export RUSTFS_VOLUMES="./target/volume/test"
export RUSTFS_ADDRESS="0.0.0.0:9000"
export RUSTFS_CONSOLE_ENABLE=true
export RUSTFS_CONSOLE_ADDRESS="0.0.0.0:9001"
export RUSTFS_SERVER_ENDPOINT="http://127.0.0.1:9000"
```

You need replace your real data folder:

```
./rustfs /data/rustfs
```
