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

# RustFS Startup Script
# This script sets up environment variables and starts the RustFS service

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
    export RUST_LOG="info,rustfs=debug,rustfs_ecstore=info,s3s=debug,rustfs_iam=info,rustfs_notify=info"
fi

# export RUSTFS_ERASURE_SET_DRIVE_COUNT=5

# export RUSTFS_STORAGE_CLASS_INLINE_BLOCK="512 KB"

# This script provisions multiple local export directories on the same disk.
# Default the bypass only for this local layout, while still allowing callers
# to override it explicitly through the environment.
if [ -z "${RUSTFS_UNSAFE_BYPASS_DISK_CHECK+x}" ] && [ -z "${MINIO_CI+x}" ]; then
    export RUSTFS_UNSAFE_BYPASS_DISK_CHECK=true
fi

export RUSTFS_VOLUMES="./target/volume/test{1...4}"
# export RUSTFS_VOLUMES="./target/volume/test"
export RUSTFS_ADDRESS=":9000"
export RUSTFS_CONSOLE_ENABLE=true
export RUSTFS_CONSOLE_ADDRESS=":9001"
# export RUSTFS_SERVER_DOMAINS="localhost:9000"
# HTTPS certificate directory
# export RUSTFS_TLS_PATH="./deploy/certs"

# Observability related configuration
export RUSTFS_OBS_ENDPOINT=http://localhost:4318 # OpenTelemetry Collector address
# RustFS OR OTEL exporter configuration
#export RUSTFS_OBS_TRACE_ENDPOINT=http://localhost:4318/v1/traces # OpenTelemetry Collector trace address http://localhost:4318/v1/traces
#export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://localhost:14318/v1/traces
#export RUSTFS_OBS_METRIC_ENDPOINT=http://localhost:9090/api/v1/otlp/v1/metrics # OpenTelemetry Collector metric address
#export OTEL_EXPORTER_OTLP_METRICS_ENDPOINT=http://localhost:9090/api/v1/otlp/v1/metrics
#export RUSTFS_OBS_LOG_ENDPOINT=http://loki:3100/otlp/v1/logs # OpenTelemetry Collector logs address http://loki:3100/otlp/v1/logs
#export OTEL_EXPORTER_OTLP_LOGS_ENDPOINT=http://loki:3100/otlp/v1/logs
export RUSTFS_OBS_PROFILING_ENDPOINT=http://localhost:4040 # OpenTelemetry Collector profiling address
export RUSTFS_OBS_USE_STDOUT=true # Whether to use standard output
export RUSTFS_OBS_SAMPLE_RATIO=2.0 # Sample ratio, between 0.0-1.0, 0.0 means no sampling, 1.0 means full sampling
export RUSTFS_OBS_METER_INTERVAL=1 # Sampling interval in seconds
export RUSTFS_OBS_SERVICE_NAME=rustfs # Service name
export RUSTFS_OBS_SERVICE_VERSION=0.1.0 # Service version
export RUSTFS_OBS_ENVIRONMENT=production # Environment name development, staging, production
export RUSTFS_OBS_LOGGER_LEVEL=info # Log level, supports trace, debug, info, warn, error
export RUSTFS_OBS_LOG_STDOUT_ENABLED=true # Whether to enable local stdout logging
export RUSTFS_OBS_LOG_DIRECTORY="$current_dir/deploy/logs" # Log directory
export RUSTFS_OBS_LOG_ROTATION_TIME="minutely" # Log rotation time unit, can be "minutely", "hourly", "daily"
export RUSTFS_OBS_LOG_KEEP_FILES=10 # Number of log files to keep
export RUSTFS_OBS_LOG_CLEANUP_INTERVAL_SECONDS=30
export RUSTFS_OBS_LOG_MIN_FILE_AGE_SECONDS=60
export RUSTFS_OBS_LOG_COMPRESSED_FILE_RETENTION_DAYS=7


#tokio runtime
export RUSTFS_RUNTIME_WORKER_THREADS=16
export RUSTFS_RUNTIME_MAX_BLOCKING_THREADS=1024
export RUSTFS_RUNTIME_THREAD_PRINT_ENABLED=false
# shellcheck disable=SC2125
export RUSTFS_RUNTIME_THREAD_STACK_SIZE=1024*1024
export RUSTFS_RUNTIME_THREAD_KEEP_ALIVE=60
export RUSTFS_RUNTIME_GLOBAL_QUEUE_INTERVAL=31

# ============================================================================
# dial9 Tokio Runtime Telemetry Configuration
# ============================================================================
# dial9 provides low-overhead Tokio runtime-level telemetry for performance diagnostics.
# It captures events like PollStart/End, WorkerPark/Unpark, QueueSample, TaskSpawn.
#
# Features:
# - CPU overhead < 5% (with sampling rate 1.0)
# - Automatic file rotation (configurable size and count)
# - Graceful degradation if initialization fails
#
# Note: Disabled by default. Enable only when needed for runtime diagnostics.
# Note: Requires build flag --cfg tokio_unstable (set in .cargo/config.toml).

# Enable dial9 telemetry (default: false)
#export RUSTFS_RUNTIME_DIAL9_ENABLED=true

# Output directory for trace files (default: /var/log/rustfs/telemetry)
#export RUSTFS_RUNTIME_DIAL9_OUTPUT_DIR="$current_dir/deploy/telemetry"

# Trace file prefix (default: rustfs-tokio)
#export RUSTFS_RUNTIME_DIAL9_FILE_PREFIX=rustfs-tokio

# Maximum trace file size in bytes (default: 104857600 = 100MB)
#export RUSTFS_RUNTIME_DIAL9_MAX_FILE_SIZE=104857600

# Number of rotated files to keep (default: 10)
#export RUSTFS_RUNTIME_DIAL9_ROTATION_COUNT=10

# Sampling rate: 0.0 to 1.0 (default: 1.0 = 100% sampling)
# Lower values reduce CPU overhead. Recommended: 0.1-0.5 for production.
#export RUSTFS_RUNTIME_DIAL9_SAMPLING_RATE=1.0

# S3 upload settings (not yet implemented; reserved for future use):
#export RUSTFS_RUNTIME_DIAL9_S3_BUCKET=my-trace-bucket
#export RUSTFS_RUNTIME_DIAL9_S3_PREFIX=telemetry/

# --- Scenario 1: Development / Debugging ---
# Full tracing with local storage, high sampling rate
#export RUSTFS_RUNTIME_DIAL9_ENABLED=true
#export RUSTFS_RUNTIME_DIAL9_OUTPUT_DIR="$current_dir/deploy/telemetry"
#export RUSTFS_RUNTIME_DIAL9_SAMPLING_RATE=1.0

# --- Scenario 2: Production Diagnostics ---
# Reduced sampling rate to minimize overhead
#export RUSTFS_RUNTIME_DIAL9_ENABLED=true
#export RUSTFS_RUNTIME_DIAL9_SAMPLING_RATE=0.1

# --- Scenario 3: Performance Investigation ---
# Short-term tracing with high detail, manual cleanup
#export RUSTFS_RUNTIME_DIAL9_ENABLED=true
#export RUSTFS_RUNTIME_DIAL9_OUTPUT_DIR=/tmp/rustfs-telemetry-investigation
#export RUSTFS_RUNTIME_DIAL9_SAMPLING_RATE=1.0
#export RUSTFS_RUNTIME_DIAL9_ROTATION_COUNT=3

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

export RUSTFS_SCANNER_ENABLED=true

export RUSTFS_HEAL_ENABLED=true

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


# ============================================================================
# Capacity Statistics Configuration
# ============================================================================

# --- Capacity Management System ---
# The capacity management system provides accurate capacity statistics with
# high performance through hybrid caching strategy.
#
# Features:
# - Hybrid caching: scheduled updates + write triggers + smart detection
# - Performance protection: sampling, timeout, fallback
# - Comprehensive metrics: 17 metrics for monitoring
# - Low overhead: < 0.1% CPU, < 1MB memory
#
# For more details, see: .codeartsdoer/specs/fix-capacity-calculation/

# --- Basic Configuration ---
# Scheduled update interval (seconds)
# How often to perform full capacity recalculation
# Default: 300 (5 minutes)
# Recommended: 300-600 for production, 60-120 for testing
export RUSTFS_CAPACITY_SCHEDULED_INTERVAL=300

# Write trigger delay (seconds)
# Delay after write operation before triggering capacity update
# Default: 10
# Recommended: 5-15
export RUSTFS_CAPACITY_WRITE_TRIGGER_DELAY=10

# Write frequency threshold (writes per minute)
# Threshold for triggering fast updates during high write frequency
# Default: 10
# Recommended: 5-20
export RUSTFS_CAPACITY_WRITE_FREQUENCY_THRESHOLD=10

# Fast update threshold (seconds)
# Cache age threshold for considering data as fresh
# Default: 60
# Recommended: 30-120
export RUSTFS_CAPACITY_FAST_UPDATE_THRESHOLD=60

# --- Performance Protection ---
# Maximum files threshold
# When file count exceeds this, sampling is used for performance
# Default: 1000000 (1 million)
# Recommended: 500000-2000000
export RUSTFS_CAPACITY_MAX_FILES_THRESHOLD=1000000

# Statistics timeout (seconds)
# Maximum time to wait for capacity calculation
# Default: 5
# Recommended: 3-10
export RUSTFS_CAPACITY_STAT_TIMEOUT=5

# Sample rate
# When sampling is enabled, check every N files
# Default: 100
# Recommended: 50-200
export RUSTFS_CAPACITY_SAMPLE_RATE=100

# --- Monitoring Configuration ---
# Metrics logging interval (seconds)
# How often to log capacity metrics summary
# Default: 600 (10 minutes)
# Recommended: 300-900
export RUSTFS_CAPACITY_METRICS_INTERVAL=600

# --- Scenario 1: High Performance Production ---
# For high-throughput production environments with millions of files
# export RUSTFS_CAPACITY_SCHEDULED_INTERVAL=600
# export RUSTFS_CAPACITY_WRITE_TRIGGER_DELAY=15
# export RUSTFS_CAPACITY_WRITE_FREQUENCY_THRESHOLD=20
# export RUSTFS_CAPACITY_FAST_UPDATE_THRESHOLD=120
# export RUSTFS_CAPACITY_MAX_FILES_THRESHOLD=2000000
# export RUSTFS_CAPACITY_STAT_TIMEOUT=10
# export RUSTFS_CAPACITY_SAMPLE_RATE=200
# export RUSTFS_CAPACITY_METRICS_INTERVAL=900

# --- Scenario 2: Low Latency Testing ---
# For testing environments requiring frequent updates
# export RUSTFS_CAPACITY_SCHEDULED_INTERVAL=60
# export RUSTFS_CAPACITY_WRITE_TRIGGER_DELAY=5
# export RUSTFS_CAPACITY_WRITE_FREQUENCY_THRESHOLD=5
# export RUSTFS_CAPACITY_FAST_UPDATE_THRESHOLD=30
# export RUSTFS_CAPACITY_MAX_FILES_THRESHOLD=500000
# export RUSTFS_CAPACITY_STAT_TIMEOUT=3
# export RUSTFS_CAPACITY_SAMPLE_RATE=50
# export RUSTFS_CAPACITY_METRICS_INTERVAL=300

# --- Scenario 3: Small Scale Deployment ---
# For small deployments with < 100K files
# export RUSTFS_CAPACITY_SCHEDULED_INTERVAL=300
# export RUSTFS_CAPACITY_WRITE_TRIGGER_DELAY=10
# export RUSTFS_CAPACITY_WRITE_FREQUENCY_THRESHOLD=10
# export RUSTFS_CAPACITY_FAST_UPDATE_THRESHOLD=60
# export RUSTFS_CAPACITY_MAX_FILES_THRESHOLD=100000
# export RUSTFS_CAPACITY_STAT_TIMEOUT=5
# export RUSTFS_CAPACITY_SAMPLE_RATE=100
# export RUSTFS_CAPACITY_METRICS_INTERVAL=600

# --- Scenario 4: Debugging / Troubleshooting ---
# Enable more frequent updates and shorter timeouts for debugging
# export RUSTFS_CAPACITY_SCHEDULED_INTERVAL=30
# export RUSTFS_CAPACITY_WRITE_TRIGGER_DELAY=2
# export RUSTFS_CAPACITY_WRITE_FREQUENCY_THRESHOLD=3
# export RUSTFS_CAPACITY_FAST_UPDATE_THRESHOLD=10
# export RUSTFS_CAPACITY_MAX_FILES_THRESHOLD=10000
# export RUSTFS_CAPACITY_STAT_TIMEOUT=2
# export RUSTFS_CAPACITY_SAMPLE_RATE=10
# export RUSTFS_CAPACITY_METRICS_INTERVAL=60

# ============================================
# Concurrent Request Optimization Configuration
# ============================================
# These settings optimize GetObject performance under high concurrency.
# Most features are enabled by default with sensible values.
# Uncomment and adjust based on your scenario.

# --- Default Configuration (Recommended for most cases) ---
# Request timeout: 30 seconds (prevents indefinite hangs)
export RUSTFS_OBJECT_GET_TIMEOUT=30
# Disk read timeout: 10 seconds
export RUSTFS_OBJECT_DISK_READ_TIMEOUT=10
# Lock acquire timeout: 5 seconds
export RUSTFS_OBJECT_LOCK_ACQUIRE_TIMEOUT=5
# Duplex buffer size: 4MB (4x larger than original 1MB)
export RUSTFS_OBJECT_DUPLEX_BUFFER_SIZE=4194304
# I/O buffer size: 128KB
export RUSTFS_OBJECT_IO_BUFFER_SIZE=131072
# Max concurrent disk reads: 64
export RUSTFS_OBJECT_MAX_CONCURRENT_DISK_READS=64
# Lock optimization: release read lock after metadata read
export RUSTFS_OBJECT_LOCK_OPTIMIZATION_ENABLE=true
# Priority scheduling: small requests get higher priority
export RUSTFS_OBJECT_PRIORITY_SCHEDULING_ENABLE=true
# Deadlock detection: disabled by default (has performance overhead)
# export RUSTFS_OBJECT_DEADLOCK_DETECTION_ENABLE=false

# --- Scenario 1: Home NAS / Small Storage Server ---
# Hardware: 4-8 cores, 8-16GB RAM, 1-4 HDDs, 1Gbps network
# Typical concurrency: 5-20 requests
# export RUSTFS_OBJECT_GET_TIMEOUT=30
# export RUSTFS_OBJECT_DUPLEX_BUFFER_SIZE=4194304
# export RUSTFS_OBJECT_MAX_CONCURRENT_DISK_READS=64
# export RUSTFS_OBJECT_LOCK_OPTIMIZATION_ENABLE=true
# export RUSTFS_OBJECT_PRIORITY_SCHEDULING_ENABLE=true

# --- Scenario 2: Medium Enterprise Storage ---
# Hardware: 8-16 cores, 32-64GB RAM, 4-12 HDDs/SSDs, 10Gbps network
# Typical concurrency: 20-100 requests
# export RUSTFS_OBJECT_GET_TIMEOUT=60
# export RUSTFS_OBJECT_DUPLEX_BUFFER_SIZE=8388608
# export RUSTFS_OBJECT_IO_BUFFER_SIZE=262144
# export RUSTFS_OBJECT_MAX_CONCURRENT_DISK_READS=128
# export RUSTFS_OBJECT_HIGH_CONCURRENCY_THRESHOLD=16
# export RUSTFS_OBJECT_LOCK_OPTIMIZATION_ENABLE=true
# export RUSTFS_OBJECT_PRIORITY_SCHEDULING_ENABLE=true

# --- Scenario 3: Large Enterprise / Cloud Storage ---
# Hardware: 32+ cores, 128+GB RAM, NVMe SSD array, 25-100Gbps network
# Typical concurrency: 100-1000 requests
# export RUSTFS_OBJECT_GET_TIMEOUT=120
# export RUSTFS_OBJECT_DUPLEX_BUFFER_SIZE=16777216
# export RUSTFS_OBJECT_IO_BUFFER_SIZE=524288
# export RUSTFS_OBJECT_MAX_CONCURRENT_DISK_READS=256
# export RUSTFS_OBJECT_HIGH_CONCURRENCY_THRESHOLD=32
# export RUSTFS_OBJECT_LOCK_OPTIMIZATION_ENABLE=true
# export RUSTFS_OBJECT_PRIORITY_SCHEDULING_ENABLE=true

# --- Scenario 4: Kopia Backup Optimized ---
# Problem: Kopia sends 20-30 concurrent range requests for 20-26MB objects
# Solution: Larger buffer, lock optimization, priority scheduling
# export RUSTFS_OBJECT_GET_TIMEOUT=45
# export RUSTFS_OBJECT_DUPLEX_BUFFER_SIZE=8388608
# export RUSTFS_OBJECT_MAX_CONCURRENT_DISK_READS=64
# export RUSTFS_OBJECT_LOCK_OPTIMIZATION_ENABLE=true
# export RUSTFS_OBJECT_PRIORITY_SCHEDULING_ENABLE=true
# export RUSTFS_OBJECT_DEADLOCK_DETECTION_ENABLE=true  # Enable for debugging

# --- Scenario 5: Low Power / Embedded Storage ---
# Hardware: 2-4 cores (ARM/x86), 2-4GB RAM, SD card/eMMC, 100Mbps-1Gbps
# Typical concurrency: 1-5 requests
# export RUSTFS_OBJECT_GET_TIMEOUT=60
# export RUSTFS_OBJECT_DISK_READ_TIMEOUT=20
# export RUSTFS_OBJECT_DUPLEX_BUFFER_SIZE=2097152
# export RUSTFS_OBJECT_IO_BUFFER_SIZE=65536
# export RUSTFS_OBJECT_MAX_CONCURRENT_DISK_READS=16
# export RUSTFS_OBJECT_HIGH_CONCURRENCY_THRESHOLD=4
# export RUSTFS_OBJECT_LOCK_OPTIMIZATION_ENABLE=true
# export RUSTFS_OBJECT_PRIORITY_SCHEDULING_ENABLE=true

# --- Scenario 6: Debugging / Troubleshooting ---
# Enable all diagnostic features
# export RUSTFS_OBJECT_GET_TIMEOUT=15
# export RUSTFS_OBJECT_DEADLOCK_DETECTION_ENABLE=true
# export RUSTFS_OBJECT_DEADLOCK_CHECK_INTERVAL=3
# export RUSTFS_OBJECT_DEADLOCK_HANG_THRESHOLD=5


# ============================================================================
# Backpressure Configuration
# ============================================================================

# High watermark: trigger backpressure when buffer usage exceeds this percentage
export RUSTFS_BACKPRESSURE_HIGH_WATERMARK=80
# Low watermark: release backpressure when buffer usage drops below this percentage
export RUSTFS_BACKPRESSURE_LOW_WATERMARK=50

if [ -n "$1" ]; then
	export RUSTFS_VOLUMES="$1"
fi

# ============================================================================
# Memory Profiling Configuration
# ============================================================================

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

# ============================================================================
# Service Startup
# ============================================================================

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
