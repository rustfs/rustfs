#!/bin/bash -e

# check ./rustfs/static/index.html not exists
if [ ! -f ./rustfs/static/index.html ]; then
    echo "Downloading rustfs-console-latest.zip"
    # download rustfs-console-latest.zip do not show log
    curl -s -L "https://dl.rustfs.com/artifacts/console/rustfs-console-latest.zip" -o tempfile.zip && unzip -q -o tempfile.zip -d ./rustfs/static && rm tempfile.zip
fi

if [ -z "$SKIP_BUILD" ]; then
    cargo build -p rustfs --bins
fi

current_dir=$(pwd)

# mkdir -p ./target/volume/test
mkdir -p ./target/volume/test{0..4}


if [ -z "$RUST_LOG" ]; then
    export RUST_BACKTRACE=1
#    export RUST_LOG="rustfs=debug,ecstore=debug,s3s=debug,iam=debug"
    export RUST_LOG="rustfs=info,ecstore=info,s3s=info,iam=info,rustfs-obs=info"
fi

# export RUSTFS_ERASURE_SET_DRIVE_COUNT=5

# export RUSTFS_STORAGE_CLASS_INLINE_BLOCK="512 KB"

export RUSTFS_VOLUMES="./target/volume/test{0...4}"
# export RUSTFS_VOLUMES="./target/volume/test"
export RUSTFS_ADDRESS=":9000"
export RUSTFS_CONSOLE_ENABLE=true
export RUSTFS_CONSOLE_ADDRESS=":9001"
# export RUSTFS_SERVER_DOMAINS="localhost:9000"
# HTTPS 证书目录
# export RUSTFS_TLS_PATH="./deploy/certs"

# 具体路径修改为配置文件真实路径，obs.example.toml 仅供参考 其中 `RUSTFS_OBS_CONFIG` 和下面变量二选一
# export RUSTFS_OBS_ENDPOINT=http://localhost:4317

# 如下变量需要必须参数都有值才可以，以及会覆盖配置文件中的值
#export RUSTFS_OBSERVABILITY_ENDPOINT=http://localhost:4317
#export RUSTFS_OBSERVABILITY_ENDPOINT=http://localhost:4317
#export RUSTFS_OBSERVABILITY_USE_STDOUT=false
#export RUSTFS_OBSERVABILITY_SAMPLE_RATIO=2.0
#export RUSTFS_OBSERVABILITY_METER_INTERVAL=31
#export RUSTFS_OBSERVABILITY_SERVICE_NAME=rustfs
#export RUSTFS_OBSERVABILITY_SERVICE_VERSION=0.1.0
#export RUSTFS_OBSERVABILITY_ENVIRONMENT=develop
#export RUSTFS_OBSERVABILITY_LOGGER_LEVEL=debug
export RUSTFS_OBSERVABILITY_LOCAL_LOGGING_ENABLED=true
#export RUSTFS_OBSERVABILITY_LOCAL_LOGGING_ENABLED=false # Whether to enable local logging
export RUSTFS_OBSERVABILITY_LOG_DIRECTORY="./deploy/logs" # Log directory
export RUSTFS_OBSERVABILITY_LOG_ROTATION_TIME="minute" # Log rotation time unit, can be "second", "minute", "hour", "day"
export RUSTFS_OBSERVABILITY_LOG_ROTATION_SIZE_MB=1 # Log rotation size in MB

#
#export RUSTFS_SINKS_FILE_PATH=./deploy/logs/rustfs.log
#export RUSTFS_SINKS_FILE_BUFFER_SIZE=12
#export RUSTFS_SINKS_FILE_FLUSH_INTERVAL_MS=1000
#export RUSTFS_SINKS_FILE_FLUSH_THRESHOLD=100
#
#export RUSTFS_SINKS_KAKFA_BROKERS=localhost:9092
#export RUSTFS_SINKS_KAKFA_TOPIC=logs
#export RUSTFS_SINKS_KAKFA_BATCH_SIZE=100
#export RUSTFS_SINKS_KAKFA_BATCH_TIMEOUT_MS=1000
#
#export RUSTFS_SINKS_WEBHOOK_ENDPOINT=http://localhost:8080/webhook
#export RUSTFS_SINKS_WEBHOOK_AUTH_TOKEN=you-auth-token
#export RUSTFS_SINKS_WEBHOOK_BATCH_SIZE=100
#export RUSTFS_SINKS_WEBHOOK_BATCH_TIMEOUT_MS=1000
#
#export RUSTFS_LOGGER_QUEUE_CAPACITY=10

export OTEL_INSTRUMENTATION_NAME="rustfs"
export OTEL_INSTRUMENTATION_VERSION="0.1.1"
export OTEL_INSTRUMENTATION_SCHEMA_URL="https://opentelemetry.io/schemas/1.31.0"
export OTEL_INSTRUMENTATION_ATTRIBUTES="env=production"

# 事件消息配置
#export RUSTFS_EVENT_CONFIG="./deploy/config/event.example.toml"

if [ -n "$1" ]; then
	export RUSTFS_VOLUMES="$1"
fi

# 启动 webhook 服务器
#cargo run --example webhook -p rustfs-event-notifier &
# 启动主服务
cargo run --bin rustfs