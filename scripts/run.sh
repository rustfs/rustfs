#!/bin/bash

mkdir -p ./target/volume/test{0..4}

DATA_DIR="./target/volume/test{0...4}"

if [ -n "$1" ]; then
	DATA_DIR="$1"
fi

if [ -z "$RUST_LOG" ]; then
    export RUST_LOG="rustfs=debug,ecstore=debug,s3s=debug"
fi

cargo run \
    -- --access-key AKEXAMPLERUSTFS    \
    --secret-key SKEXAMPLERUSTFS    \
    --address       0.0.0.0:9010       \
    --domain-name   127.0.0.1:9010  \
    "$DATA_DIR"
