#!/bin/bash -e

if [ -z "$SKIP_BUILD" ]; then
    cargo build -p rustfs --bins
fi

current_dir=$(pwd)

# mkdir -p ./target/volume/test
mkdir -p ./target/volume/test{0..4}


if [ -z "$RUST_LOG" ]; then
    export RUST_BACKTRACE=1
    export RUST_LOG="rustfs=debug,ecstore=debug,s3s=debug,iam=debug"
fi

# export RUSTFS_ERASURE_SET_DRIVE_COUNT=5

# export RUSTFS_STORAGE_CLASS_INLINE_BLOCK="512 KB"

export RUSTFS_VOLUMES="./target/volume/test{0...4}"
# export RUSTFS_VOLUMES="./target/volume/test"
export RUSTFS_ADDRESS="0.0.0.0:9000"
export RUSTFS_CONSOLE_ENABLE=true
export RUSTFS_CONSOLE_ADDRESS="0.0.0.0:9002"
# export RUSTFS_SERVER_DOMAINS="localhost:9000"

# 具体路径修改为配置文件真实路径，obs.example.toml 仅供参考
export RUSTFS_OBS_CONFIG="./config/obs.example.toml"

if [ -n "$1" ]; then
	export RUSTFS_VOLUMES="$1"
fi



# check ./rustfs/static/index.html not exists
if [ ! -f ./rustfs/static/index.html ]; then
    echo "Downloading rustfs-console-latest.zip"
    # download rustfs-console-latest.zip do not show log
    curl -s -L "https://dl.rustfs.com/artifacts/console/rustfs-console-latest.zip" -o tempfile.zip && unzip -q -o tempfile.zip -d ./rustfs/static && rm tempfile.zip
fi

cargo run --bin rustfs