#!/usr/bin/env bash
set -e

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
    export RUST_LOG="rustfs=debug,ecstore=info,s3s=debug,iam=info,notify=info"
fi

# export RUSTFS_ERASURE_SET_DRIVE_COUNT=5

# export RUSTFS_STORAGE_CLASS_INLINE_BLOCK="512 KB"

export RUSTFS_VOLUMES="./target/volume/test{1...4}"
# export RUSTFS_VOLUMES="./target/volume/test"
export RUSTFS_ADDRESS=":9000"
export RUSTFS_CONSOLE_ENABLE=true
export RUSTFS_CONSOLE_ADDRESS=":9001"
# export RUSTFS_SERVER_DOMAINS="localhost:9000"
# HTTPS certificate directory
# export RUSTFS_TLS_PATH="./deploy/certs"

# Observability related configuration
#export RUSTFS_OBS_ENDPOINT=http://localhost:4318 # OpenTelemetry Collector address
# RustFS OR OTEL exporter configuration
#export RUSTFS_OBS_TRACE_ENDPOINT=http://localhost:4318/v1/traces # OpenTelemetry Collector trace address http://localhost:4318/v1/traces
#export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://localhost:14318/v1/traces
#export RUSTFS_OBS_METRIC_ENDPOINT=http://localhost:9090/api/v1/otlp/v1/metrics # OpenTelemetry Collector metric address
#export OTEL_EXPORTER_OTLP_METRICS_ENDPOINT=http://localhost:9090/api/v1/otlp/v1/metrics
#export RUSTFS_OBS_LOG_ENDPOINT=http://loki:3100/otlp/v1/logs # OpenTelemetry Collector logs address http://loki:3100/otlp/v1/logs
#export OTEL_EXPORTER_OTLP_LOGS_ENDPOINT=http://loki:3100/otlp/v1/logs
#export RUSTFS_OBS_USE_STDOUT=true # Whether to use standard output
#export RUSTFS_OBS_SAMPLE_RATIO=2.0 # Sample ratio, between 0.0-1.0, 0.0 means no sampling, 1.0 means full sampling
#export RUSTFS_OBS_METER_INTERVAL=1 # Sampling interval in seconds
#export RUSTFS_OBS_SERVICE_NAME=rustfs # Service name
#export RUSTFS_OBS_SERVICE_VERSION=0.1.0 # Service version
export RUSTFS_OBS_ENVIRONMENT=develop # Environment name
export RUSTFS_OBS_LOGGER_LEVEL=info # Log level, supports trace, debug, info, warn, error
export RUSTFS_OBS_LOG_STDOUT_ENABLED=true # Whether to enable local stdout logging
export RUSTFS_OBS_LOG_DIRECTORY="$current_dir/deploy/logs" # Log directory
export RUSTFS_OBS_LOG_ROTATION_TIME="minutely" # Log rotation time unit, can be "minutely", "hourly", "daily"
export RUSTFS_OBS_LOG_KEEP_FILES=30 # Number of log files to keep
export RUSTFS_OBS_LOG_MESSAGE_CAPA=32768 # Log message capacity
export RUSTFS_OBS_LOG_FLUSH_MS=300 # Log flush interval in milliseconds

#tokio runtime
export RUSTFS_RUNTIME_WORKER_THREADS=16
export RUSTFS_RUNTIME_MAX_BLOCKING_THREADS=1024
export RUSTFS_RUNTIME_THREAD_PRINT_ENABLED=false
export RUSTFS_OBS_LOG_CLEANUP_INTERVAL_SECONDS=300
# shellcheck disable=SC2125
export RUSTFS_RUNTIME_THREAD_STACK_SIZE=1024*1024
export RUSTFS_RUNTIME_THREAD_KEEP_ALIVE=60
export RUSTFS_RUNTIME_GLOBAL_QUEUE_INTERVAL=31

export OTEL_INSTRUMENTATION_NAME="rustfs"
export OTEL_INSTRUMENTATION_VERSION="0.1.1"
export OTEL_INSTRUMENTATION_SCHEMA_URL="https://opentelemetry.io/schemas/1.31.0"
export OTEL_INSTRUMENTATION_ATTRIBUTES="env=production"

# # notify
# export RUSTFS_NOTIFY_WEBHOOK_ENABLE="on" # Whether to enable webhook notification
# export RUSTFS_NOTIFY_WEBHOOK_ENDPOINT="http://[::]:3020/webhook" # Webhook notification address
# export RUSTFS_NOTIFY_WEBHOOK_QUEUE_DIR="$current_dir/deploy/logs/notify"

# export RUSTFS_NOTIFY_WEBHOOK_ENABLE_PRIMARY="on" # Whether to enable webhook notification
# export RUSTFS_NOTIFY_WEBHOOK_ENDPOINT_PRIMARY="http://[::]:3020/webhook" # Webhook notification address
# export RUSTFS_NOTIFY_WEBHOOK_QUEUE_DIR_PRIMARY="$current_dir/deploy/logs/notify"

# export RUSTFS_NOTIFY_WEBHOOK_ENABLE_MASTER="on" # Whether to enable webhook notification
# export RUSTFS_NOTIFY_WEBHOOK_ENDPOINT_MASTER="http://[::]:3020/webhook" # Webhook notification address
# export RUSTFS_NOTIFY_WEBHOOK_QUEUE_DIR_MASTER="$current_dir/deploy/logs/notify"

# export RUSTFS_AUDIT_WEBHOOK_ENABLE="on" # Whether to enable webhook audit
# export RUSTFS_AUDIT_WEBHOOK_ENDPOINT="http://[::]:3020/webhook" # Webhook audit address
# export RUSTFS_AUDIT_WEBHOOK_QUEUE_DIR="$current_dir/deploy/logs/audit"

# export RUSTFS_AUDIT_WEBHOOK_ENABLE_PRIMARY="on" # Whether to enable webhook audit
# export RUSTFS_AUDIT_WEBHOOK_ENDPOINT_PRIMARY="http://[::]:3020/webhook" # Webhook audit address
# export RUSTFS_AUDIT_WEBHOOK_QUEUE_DIR_PRIMARY="$current_dir/deploy/logs/audit"

# export RUSTFS_AUDIT_WEBHOOK_ENABLE_MASTER="on" # Whether to enable webhook audit
# export RUSTFS_AUDIT_WEBHOOK_ENDPOINT_MASTER="http://[::]:3020/webhook" # Webhook audit address
# export RUSTFS_AUDIT_WEBHOOK_QUEUE_DIR_MASTER="$current_dir/deploy/logs/audit"

# export RUSTFS_POLICY_PLUGIN_URL="http://localhost:8181/v1/data/rustfs/authz/allow"  # The URL of the OPA system
# export RUSTFS_POLICY_PLUGIN_AUTH_TOKEN="your-opa-token"  # The authentication token for the OPA system is optional


export RUSTFS_NS_SCANNER_INTERVAL=60  # Object scanning interval in seconds
# export RUSTFS_SKIP_BACKGROUND_TASK=true

# Storage level compression (compression at object storage level)
# export RUSTFS_COMPRESSION_ENABLED=true # Whether to enable storage-level compression for objects

# HTTP Response Compression (whitelist-based, aligned with MinIO)
# By default, HTTP response compression is DISABLED (aligned with MinIO behavior)
# When enabled, only explicitly configured file types will be compressed
# This preserves Content-Length headers for better browser download experience

# Enable HTTP response compression
# export RUSTFS_COMPRESS_ENABLE=on

# Example 1: Compress text files and logs
# Suitable for log files, text documents, CSV files
# export RUSTFS_COMPRESS_ENABLE=on
# export RUSTFS_COMPRESS_EXTENSIONS=.txt,.log,.csv
# export RUSTFS_COMPRESS_MIME_TYPES=text/*
# export RUSTFS_COMPRESS_MIN_SIZE=1000

# Example 2: Compress JSON and XML API responses
# Suitable for API services that return JSON/XML data
# export RUSTFS_COMPRESS_ENABLE=on
# export RUSTFS_COMPRESS_EXTENSIONS=.json,.xml
# export RUSTFS_COMPRESS_MIME_TYPES=application/json,application/xml
# export RUSTFS_COMPRESS_MIN_SIZE=1000

# Example 3: Comprehensive web content compression
# Suitable for web applications (HTML, CSS, JavaScript, JSON)
# export RUSTFS_COMPRESS_ENABLE=on
# export RUSTFS_COMPRESS_EXTENSIONS=.html,.css,.js,.json,.xml,.txt,.svg
# export RUSTFS_COMPRESS_MIME_TYPES=text/*,application/json,application/xml,application/javascript,image/svg+xml
# export RUSTFS_COMPRESS_MIN_SIZE=1000

# Example 4: Compress only large text files (minimum 10KB)
# Useful when you want to avoid compression overhead for small files
# export RUSTFS_COMPRESS_ENABLE=on
# export RUSTFS_COMPRESS_EXTENSIONS=.txt,.log
# export RUSTFS_COMPRESS_MIME_TYPES=text/*
# export RUSTFS_COMPRESS_MIN_SIZE=10240

# Notes:
# - Only files matching EITHER extensions OR MIME types will be compressed (whitelist approach)
# - Error responses (4xx, 5xx) are never compressed to avoid Content-Length issues
# - Already encoded content (gzip, br, deflate, zstd) is automatically skipped
# - Minimum size threshold prevents compression of small files where overhead > benefit
# - Wildcard patterns supported in MIME types (e.g., text/* matches text/plain, text/html, etc.)

# Trusted Proxy Configuration
# export RUSTFS_TRUSTED_PROXY_ENABLED=true
# export RUSTFS_TRUSTED_PROXY_NETWORKS=127.0.0.1,::1,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16,fd00::/8
# export RUSTFS_TRUSTED_PROXY_VALIDATION_MODE=hop_by_hop
# export RUSTFS_TRUSTED_PROXY_ENABLE_RFC7239=true
# export RUSTFS_TRUSTED_PROXY_MAX_HOPS=10
# export RUSTFS_TRUSTED_PROXY_METRICS_ENABLED=true

#export RUSTFS_REGION="us-east-1"

export RUSTFS_ENABLE_SCANNER=true

export RUSTFS_ENABLE_HEAL=true

# Object cache configuration
export RUSTFS_OBJECT_CACHE_ENABLE=true

# Profiling configuration
export RUSTFS_ENABLE_PROFILING=false
# Memory profiling periodic dump
export RUSTFS_PROF_MEM_PERIODIC=false

# Heal configuration queue size
export RUSTFS_HEAL_QUEUE_SIZE=10000

# rustfs trust system CA certificates
export RUSTFS_TRUST_SYSTEM_CA=true

# FTP/FTPS Configuration
#
# Certificate directory structure:
# deploy/certs/ftps/
# ├── rustfs_cert.pem  (server certificate)
# ├── rustfs_key.pem   (server private key)
# └── example1.com/    (optional: domain-specific certificates)
#     ├── rustfs_cert.pem
#     └── rustfs_key.pem

# export RUSTFS_FTP_ENABLE=true
# export RUSTFS_FTP_ADDRESS="0.0.0.0:8021"

# export RUSTFS_FTPS_ENABLE=true
# export RUSTFS_FTPS_ADDRESS="0.0.0.0:8022"
# export RUSTFS_FTPS_CERTS_DIR="${current_dir}/deploy/certs/ftps"

# Use default timeout (60 seconds)
# No environment variable needed

# Increase timeout for high-latency network storage
#export RUSTFS_LOCK_ACQUIRE_TIMEOUT=120

# Reduce timeout for low-latency local storage
export RUSTFS_LOCK_ACQUIRE_TIMEOUT=30

if [ -n "$1" ]; then
	export RUSTFS_VOLUMES="$1"
fi

# Enable jemalloc for memory profiling
# MALLOC_CONF parameters:
#   prof:true                - Enable heap profiling
#   prof_active:true         - Start profiling immediately
#   lg_prof_sample:16        - Average number of bytes between samples (2^16 = 65536 bytes)
#   log:true                 - Enable logging
#   narenas:2                - Number of arenas (controls concurrency and memory fragmentation)
#   lg_chunk:21              - Chunk size (2^21 = 2MB)
#   background_thread:true   - Enable background threads for purging
#   dirty_decay_ms:1000      - Time (ms) before dirty pages are purged
#   muzzy_decay_ms:1000      - Time (ms) before muzzy pages are purged
# You can override these defaults by setting the MALLOC_CONF environment variable before running this script.
if [ -z "$MALLOC_CONF" ]; then
    export MALLOC_CONF="prof:true,prof_active:true,lg_prof_sample:16,log:true,narenas:2,lg_chunk:21,background_thread:true,dirty_decay_ms:1000,muzzy_decay_ms:1000"
fi

# Start webhook server
#cargo run --example webhook -p rustfs-notify &
# Start main service
# To run with profiling enabled, uncomment the following line and comment the next line
#cargo run --profile profiling --bin rustfs
# To run with FTP/FTPS support, use:
# cargo run --bin rustfs --features ftps
# To run in release mode, use the following line
#cargo run --profile release --bin rustfs
# To run in debug mode, use the following line
cargo run --bin rustfs