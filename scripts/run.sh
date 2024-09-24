#!/bin/bash

current_dir=$(pwd)

mkdir -p ./target/volume/test
mkdir -p ./target/volume/test{0..4}


if [ -z "$RUST_LOG" ]; then
    export RUST_LOG="rustfs=debug,ecstore=info,s3s=debug"
fi

DATA_DIR_ARG="./target/volume/test{0...4}"

if [ -n "$1" ]; then
	DATA_DIR_ARG="$1"
fi


# cargo run  "$DATA_DIR_ARG"
    # -- --access-key AKEXAMPLERUSTFS    \
    # --secret-key SKEXAMPLERUSTFS    \
    # --address       0.0.0.0:9010       \
    # --domain-name   127.0.0.1:9010  \
    # "$DATA_DIR_ARG"

cargo run "$DATA_DIR_ARG"