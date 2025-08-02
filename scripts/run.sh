#!/bin/bash -e
# Copyright 2024 RustFS Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


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
echo "Current directory: $current_dir"

# mkdir -p ./target/volume/test
mkdir -p ./target/volume/test{1..4}


if [ -z "$RUST_LOG" ]; then
    export RUST_BACKTRACE=1
    export RUST_LOG="rustfs=debug,ecstore=info,s3s=debug,iam=info"
fi

# export RUSTFS_ERASURE_SET_DRIVE_COUNT=5

# export RUSTFS_STORAGE_CLASS_INLINE_BLOCK="512 KB"

export RUSTFS_VOLUMES="./target/volume/test{1...4}"
# export RUSTFS_VOLUMES="./target/volume/test"
export RUSTFS_ADDRESS=":9000"
export RUSTFS_CONSOLE_ENABLE=true
export RUSTFS_CONSOLE_ADDRESS=":9001"
# export RUSTFS_SERVER_DOMAINS="localhost:9000"
# HTTPS 证书目录
# export RUSTFS_TLS_PATH="./deploy/certs"

# 可观测性 相关配置信息
#export RUSTFS_OBS_ENDPOINT=http://localhost:4317 # OpenTelemetry Collector 的地址
#export RUSTFS_OBS_USE_STDOUT=false # 是否使用标准输出
#export RUSTFS_OBS_SAMPLE_RATIO=2.0 # 采样率，0.0-1.0之间，0.0表示不采样，1.0表示全部采样
#export RUSTFS_OBS_METER_INTERVAL=1 # 采样间隔，单位为秒
#export RUSTFS_OBS_SERVICE_NAME=rustfs # 服务名称
#export RUSTFS_OBS_SERVICE_VERSION=0.1.0 # 服务版本
export RUSTFS_OBS_ENVIRONMENT=develop # 环境名称
export RUSTFS_OBS_LOGGER_LEVEL=info # 日志级别，支持 trace, debug, info, warn, error
export RUSTFS_OBS_LOCAL_LOGGING_ENABLED=true # 是否启用本地日志记录
export RUSTFS_OBS_LOG_DIRECTORY="$current_dir/deploy/logs" # Log directory
export RUSTFS_OBS_LOG_ROTATION_TIME="hour" # Log rotation time unit, can be "second", "minute", "hour", "day"
export RUSTFS_OBS_LOG_ROTATION_SIZE_MB=100 # Log rotation size in MB

export RUSTFS_SINKS_FILE_PATH="$current_dir/deploy/logs"
export RUSTFS_SINKS_FILE_BUFFER_SIZE=12
export RUSTFS_SINKS_FILE_FLUSH_INTERVAL_MS=1000
export RUSTFS_SINKS_FILE_FLUSH_THRESHOLD=100
#
# Kafka sink 配置
#export RUSTFS_SINKS_KAFKA_BROKERS=localhost:9092
#export RUSTFS_SINKS_KAFKA_TOPIC=logs
#export RUSTFS_SINKS_KAFKA_BATCH_SIZE=100
#export RUSTFS_SINKS_KAFKA_BATCH_TIMEOUT_MS=1000
#
# Webhook sink 配置
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

# notify
export RUSTFS_NOTIFY_WEBHOOK_ENABLE="on" # 是否启用 webhook 通知
export RUSTFS_NOTIFY_WEBHOOK_ENDPOINT="http://[::]:3020/webhook" # webhook 通知地址
export RUSTFS_NOTIFY_WEBHOOK_QUEUE_DIR="$current_dir/deploy/logs/notify"

export RUSTFS_NOTIFY_WEBHOOK_ENABLE_PRIMARY="on" # 是否启用 webhook 通知
export RUSTFS_NOTIFY_WEBHOOK_ENDPOINT_PRIMARY="http://[::]:3020/webhook" # webhook 通知地址
export RUSTFS_NOTIFY_WEBHOOK_QUEUE_DIR_PRIMARY="$current_dir/deploy/logs/notify"

export RUSTFS_NOTIFY_WEBHOOK_ENABLE_MASTER="on" # 是否启用 webhook 通知
export RUSTFS_NOTIFY_WEBHOOK_ENDPOINT_MASTER="http://[::]:3020/webhook" # webhook 通知地址
export RUSTFS_NOTIFY_WEBHOOK_QUEUE_DIR_MASTER="$current_dir/deploy/logs/notify"


export RUSTFS_NS_SCANNER_INTERVAL=60  # 对象扫描间隔时间，单位为秒
# exportRUSTFS_SKIP_BACKGROUND_TASK=true

export RUSTFS_COMPRESSION_ENABLED=true # 是否启用压缩

#export RUSTFS_REGION="us-east-1"

# 事件消息配置
#export RUSTFS_EVENT_CONFIG="./deploy/config/event.example.toml"

if [ -n "$1" ]; then
	export RUSTFS_VOLUMES="$1"
fi

# 启动 webhook 服务器
#cargo run --example webhook -p rustfs-notify &
# 启动主服务
cargo run --bin rustfs
