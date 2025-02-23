#!/bin/bash -e

if [ -z "$SKIP_BUILD" ]; then
    cargo build -p rustfs --bins
fi

current_dir=$(pwd)

mkdir -p ./target/volume/test
# mkdir -p ./target/volume/test{0..4}


# if [ -z "$RUST_LOG" ]; then
#     export RUST_BACKTRACE=1
#     export RUST_LOG="rustfs=debug,ecstore=debug,s3s=debug,iam=debug"
# fi

# export RUSTFS_ERASURE_SET_DRIVE_COUNT=5

# export RUSTFS_STORAGE_CLASS_INLINE_BLOCK="512 KB"

# RUSTFS_VOLUMES="./target/volume/test{0...4}"
export RUSTFS_VOLUMES="./target/volume/test"
export RUSTFS_ADDRESS="0.0.0.0:9000"
export RUSTFS_CONSOLE_ENABLE=true
export RUSTFS_CONSOLE_ADDRESS="0.0.0.0:9002"
export RUSTFS_SERVER_ENDPOINT="http://localhost:9000"

if [ -n "$1" ]; then
	export RUSTFS_VOLUMES="$1"
fi


cargo run --bin rustfs