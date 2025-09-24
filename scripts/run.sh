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
export RUSTFS_EXTERNAL_ADDRESS=":9000"
# export RUSTFS_SERVER_DOMAINS="localhost:9000"
# HTTPS certificate directory
# export RUSTFS_TLS_PATH="./deploy/certs"

# Observability related configuration
#export RUSTFS_OBS_ENDPOINT=http://localhost:4317 # OpenTelemetry Collector address
#export RUSTFS_OBS_USE_STDOUT=false # Whether to use standard output
#export RUSTFS_OBS_SAMPLE_RATIO=2.0 # Sample ratio, between 0.0-1.0, 0.0 means no sampling, 1.0 means full sampling
#export RUSTFS_OBS_METER_INTERVAL=1 # Sampling interval in seconds
#export RUSTFS_OBS_SERVICE_NAME=rustfs # Service name
#export RUSTFS_OBS_SERVICE_VERSION=0.1.0 # Service version
export RUSTFS_OBS_ENVIRONMENT=production # Environment name
export RUSTFS_OBS_LOGGER_LEVEL=info # Log level, supports trace, debug, info, warn, error
export RUSTFS_OBS_LOCAL_LOGGING_ENABLED=true # Whether to enable local logging
export RUSTFS_OBS_LOG_DIRECTORY="$current_dir/deploy/logs" # Log directory
export RUSTFS_OBS_LOG_ROTATION_TIME="hour" # Log rotation time unit, can be "second", "minute", "hour", "day"
export RUSTFS_OBS_LOG_ROTATION_SIZE_MB=100 # Log rotation size in MB
export RUSTFS_OBS_LOG_POOL_CAPA=10240
export RUSTFS_OBS_LOG_MESSAGE_CAPA=32768
export RUSTFS_OBS_LOG_FLUSH_MS=300

export RUSTFS_SINKS_FILE_PATH="$current_dir/deploy/logs"
export RUSTFS_SINKS_FILE_BUFFER_SIZE=12
export RUSTFS_SINKS_FILE_FLUSH_INTERVAL_MS=1000
export RUSTFS_SINKS_FILE_FLUSH_THRESHOLD=100

#tokio runtime
export RUSTFS_RUNTIME_WORKER_THREADS=16
export RUSTFS_RUNTIME_MAX_BLOCKING_THREADS=1024
export RUSTFS_RUNTIME_THREAD_PRINT_ENABLED=true
# shellcheck disable=SC2125
export RUSTFS_RUNTIME_THREAD_STACK_SIZE=1024*1024
export RUSTFS_RUNTIME_THREAD_KEEP_ALIVE=60
export RUSTFS_RUNTIME_GLOBAL_QUEUE_INTERVAL=31

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
export RUSTFS_NOTIFY_WEBHOOK_ENABLE="on" # Whether to enable webhook notification
export RUSTFS_NOTIFY_WEBHOOK_ENDPOINT="http://[::]:3020/webhook" # Webhook notification address
export RUSTFS_NOTIFY_WEBHOOK_QUEUE_DIR="$current_dir/deploy/logs/notify"

export RUSTFS_NOTIFY_WEBHOOK_ENABLE_PRIMARY="on" # Whether to enable webhook notification
export RUSTFS_NOTIFY_WEBHOOK_ENDPOINT_PRIMARY="http://[::]:3020/webhook" # Webhook notification address
export RUSTFS_NOTIFY_WEBHOOK_QUEUE_DIR_PRIMARY="$current_dir/deploy/logs/notify"

export RUSTFS_NOTIFY_WEBHOOK_ENABLE_MASTER="on" # Whether to enable webhook notification
export RUSTFS_NOTIFY_WEBHOOK_ENDPOINT_MASTER="http://[::]:3020/webhook" # Webhook notification address
export RUSTFS_NOTIFY_WEBHOOK_QUEUE_DIR_MASTER="$current_dir/deploy/logs/notify"


export RUSTFS_NS_SCANNER_INTERVAL=60  # Object scanning interval in seconds
# exportRUSTFS_SKIP_BACKGROUND_TASK=true

export RUSTFS_COMPRESSION_ENABLED=true # Whether to enable compression

#export RUSTFS_REGION="us-east-1"

# Event message configuration
#export RUSTFS_EVENT_CONFIG="./deploy/config/event.example.toml"

if [ -n "$1" ]; then
	export RUSTFS_VOLUMES="$1"
fi

# Start webhook server
#cargo run --example webhook -p rustfs-notify &
# Start main service
cargo run --bin rustfs
