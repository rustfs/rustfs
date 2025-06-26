#!/bin/bash -e

current_dir=$(pwd)
echo "Current directory: $current_dir"

if [ -z "$RUST_LOG" ]; then
    export RUST_BACKTRACE=1
    export RUST_LOG="rustfs=info,ecstore=info,s3s=debug,iam=info"
fi

# deploy/logs/notify directory
echo "Creating log directory if it does not exist..."
mkdir -p "$current_dir/deploy/logs/notify"

# 启动 webhook 服务器
echo "Starting webhook server..."
cargo run --example webhook -p rustfs-notify &