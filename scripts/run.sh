#!/bin/bash -e

if [ -z "$SKIP_BUILD" ]; then
    cargo build -p rustfs --bins
fi

current_dir=$(pwd)

mkdir -p ./target/volume/test
mkdir -p ./target/volume/test{0..4}


if [ -z "$RUST_LOG" ]; then
    export RUST_LOG="rustfs=debug,ecstore=debug,s3s=debug,reader=debug,router=debug"
fi

# export RUSTFS_ERASURE_SET_DRIVE_COUNT=5

export RUSTFS_STORAGE_CLASS_INLINE_BLOCK="512 KB"

# DATA_DIR_ARG="./target/volume/test{0...4}"
DATA_DIR_ARG="./target/volume/test"

if [ -n "$1" ]; then
	DATA_DIR_ARG="$1"
fi


# cargo run  "$DATA_DIR_ARG"
    # -- --access-key AKEXAMPLERUSTFS    \
    # --secret-key SKEXAMPLERUSTFS    \
    # --address       0.0.0.0:9010       \
    # --domain-name   127.0.0.1:9010  \
    # "$DATA_DIR_ARG"

./target/debug/rustfs "$DATA_DIR_ARG"
# cargo run ./target/volume/test