#!/bin/bash
DATA_DIR="/tmp"

if [ -n "$1" ]; then
	DATA_DIR="$1"
fi

if [ -z "$RUST_LOG" ]; then
    export RUST_LOG="rustfs=debug"
fi

cargo run \
    -- --access-key AKEXAMPLERUSTFS    \
    --secret-key SKEXAMPLERUSTFS    \
    --address       0.0.0.0:9010       \
    --domain-name   localhost:9010  \
    "$DATA_DIR"
