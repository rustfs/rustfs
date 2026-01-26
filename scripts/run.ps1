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

$ErrorActionPreference = "Stop"

# Check if ./rustfs/static/index.html exists
if (-not (Test-Path "./rustfs/static/index.html")) {
    Write-Host "Downloading rustfs-console-latest.zip"
    # Download rustfs-console-latest.zip
    Invoke-WebRequest -Uri "https://dl.rustfs.com/artifacts/console/rustfs-console-latest.zip" -OutFile "tempfile.zip"

    # Unzip
    Expand-Archive -Path "tempfile.zip" -DestinationPath "./rustfs/static" -Force

    # Remove temp file
    Remove-Item "tempfile.zip"
}

if (-not $env:SKIP_BUILD) {
    cargo build -p rustfs --bins
}

$current_dir = Get-Location
Write-Host "Current directory: $current_dir"

# Create directories
1..4 | ForEach-Object {
    New-Item -ItemType Directory -Force -Path "./target/volume/test$_" | Out-Null
}

if (-not $env:RUST_LOG) {
    $env:RUST_BACKTRACE = "1"
    $env:RUST_LOG = "rustfs=debug,ecstore=info,s3s=debug,iam=info,notify=info"
}

# $env:RUSTFS_ERASURE_SET_DRIVE_COUNT = "5"
# $env:RUSTFS_STORAGE_CLASS_INLINE_BLOCK = "512 KB"

$env:RUSTFS_VOLUMES = "./target/volume/test{1...4}"
# $env:RUSTFS_VOLUMES = "./target/volume/test"
$env:RUSTFS_ADDRESS = ":9000"
$env:RUSTFS_CONSOLE_ENABLE = "true"
$env:RUSTFS_CONSOLE_ADDRESS = ":9001"
# $env:RUSTFS_SERVER_DOMAINS = "localhost:9000"
# HTTPS certificate directory
# $env:RUSTFS_TLS_PATH = "./deploy/certs"

# Observability related configuration
# $env:RUSTFS_OBS_ENDPOINT = "http://localhost:4318" # OpenTelemetry Collector address
# RustFS OR OTEL exporter configuration
# $env:RUSTFS_OBS_TRACE_ENDPOINT = "http://localhost:4318/v1/traces" # OpenTelemetry Collector trace address
# $env:OTEL_EXPORTER_OTLP_TRACES_ENDPOINT = "http://localhost:14318/v1/traces"
# $env:RUSTFS_OBS_METRIC_ENDPOINT = "http://localhost:9090/api/v1/otlp/v1/metrics" # OpenTelemetry Collector metric address
# $env:OTEL_EXPORTER_OTLP_METRICS_ENDPOINT = "http://localhost:9090/api/v1/otlp/v1/metrics"
# $env:RUSTFS_OBS_LOG_ENDPOINT = "http://loki:3100/otlp/v1/logs" # OpenTelemetry Collector logs address
# $env:OTEL_EXPORTER_OTLP_LOGS_ENDPOINT = "http://loki:3100/otlp/v1/logs"
# $env:RUSTFS_OBS_USE_STDOUT = "true" # Whether to use standard output
# $env:RUSTFS_OBS_SAMPLE_RATIO = "2.0" # Sample ratio, between 0.0-1.0
# $env:RUSTFS_OBS_METER_INTERVAL = "1" # Sampling interval in seconds
# $env:RUSTFS_OBS_SERVICE_NAME = "rustfs" # Service name
# $env:RUSTFS_OBS_SERVICE_VERSION = "0.1.0" # Service version
$env:RUSTFS_OBS_ENVIRONMENT = "develop" # Environment name
$env:RUSTFS_OBS_LOGGER_LEVEL = "info" # Log level
$env:RUSTFS_OBS_LOG_STDOUT_ENABLED = "false" # Whether to enable local stdout logging
$env:RUSTFS_OBS_LOG_DIRECTORY = "$current_dir/deploy/logs" # Log directory
$env:RUSTFS_OBS_LOG_ROTATION_TIME = "hour" # Log rotation time unit
$env:RUSTFS_OBS_LOG_ROTATION_SIZE_MB = "100" # Log rotation size in MB
$env:RUSTFS_OBS_LOG_POOL_CAPA = "10240" # Log pool capacity
$env:RUSTFS_OBS_LOG_MESSAGE_CAPA = "32768" # Log message capacity
$env:RUSTFS_OBS_LOG_FLUSH_MS = "300" # Log flush interval in milliseconds

# tokio runtime
$env:RUSTFS_RUNTIME_WORKER_THREADS = "16"
$env:RUSTFS_RUNTIME_MAX_BLOCKING_THREADS = "1024"
$env:RUSTFS_RUNTIME_THREAD_PRINT_ENABLED = "false"
$env:RUSTFS_RUNTIME_THREAD_STACK_SIZE = 1024 * 1024
$env:RUSTFS_RUNTIME_THREAD_KEEP_ALIVE = "60"
$env:RUSTFS_RUNTIME_GLOBAL_QUEUE_INTERVAL = "31"

$env:OTEL_INSTRUMENTATION_NAME = "rustfs"
$env:OTEL_INSTRUMENTATION_VERSION = "0.1.1"
$env:OTEL_INSTRUMENTATION_SCHEMA_URL = "https://opentelemetry.io/schemas/1.31.0"
$env:OTEL_INSTRUMENTATION_ATTRIBUTES = "env=production"

# notify
# $env:RUSTFS_NOTIFY_WEBHOOK_ENABLE = "on"
# $env:RUSTFS_NOTIFY_WEBHOOK_ENDPOINT = "http://[::]:3020/webhook"
# $env:RUSTFS_NOTIFY_WEBHOOK_QUEUE_DIR = "$current_dir/deploy/logs/notify"

# $env:RUSTFS_NOTIFY_WEBHOOK_ENABLE_PRIMARY = "on"
# $env:RUSTFS_NOTIFY_WEBHOOK_ENDPOINT_PRIMARY = "http://[::]:3020/webhook"
# $env:RUSTFS_NOTIFY_WEBHOOK_QUEUE_DIR_PRIMARY = "$current_dir/deploy/logs/notify"

# $env:RUSTFS_NOTIFY_WEBHOOK_ENABLE_MASTER = "on"
# $env:RUSTFS_NOTIFY_WEBHOOK_ENDPOINT_MASTER = "http://[::]:3020/webhook"
# $env:RUSTFS_NOTIFY_WEBHOOK_QUEUE_DIR_MASTER = "$current_dir/deploy/logs/notify"

# $env:RUSTFS_AUDIT_WEBHOOK_ENABLE = "on"
# $env:RUSTFS_AUDIT_WEBHOOK_ENDPOINT = "http://[::]:3020/webhook"
# $env:RUSTFS_AUDIT_WEBHOOK_QUEUE_DIR = "$current_dir/deploy/logs/audit"

# $env:RUSTFS_AUDIT_WEBHOOK_ENABLE_PRIMARY = "on"
# $env:RUSTFS_AUDIT_WEBHOOK_ENDPOINT_PRIMARY = "http://[::]:3020/webhook"
# $env:RUSTFS_AUDIT_WEBHOOK_QUEUE_DIR_PRIMARY = "$current_dir/deploy/logs/audit"

# $env:RUSTFS_AUDIT_WEBHOOK_ENABLE_MASTER = "on"
# $env:RUSTFS_AUDIT_WEBHOOK_ENDPOINT_MASTER = "http://[::]:3020/webhook"
# $env:RUSTFS_AUDIT_WEBHOOK_QUEUE_DIR_MASTER = "$current_dir/deploy/logs/audit"

# $env:RUSTFS_POLICY_PLUGIN_URL = "http://localhost:8181/v1/data/rustfs/authz/allow"
# $env:RUSTFS_POLICY_PLUGIN_AUTH_TOKEN = "your-opa-token"

$env:RUSTFS_NS_SCANNER_INTERVAL = "60"
# $env:RUSTFS_SKIP_BACKGROUND_TASK = "true"

# Storage level compression
# $env:RUSTFS_COMPRESSION_ENABLED = "true"

# HTTP Response Compression
# $env:RUSTFS_COMPRESS_ENABLE = "on"

# Example 1: Compress text files and logs
# $env:RUSTFS_COMPRESS_ENABLE = "on"
# $env:RUSTFS_COMPRESS_EXTENSIONS = ".txt,.log,.csv"
# $env:RUSTFS_COMPRESS_MIME_TYPES = "text/*"
# $env:RUSTFS_COMPRESS_MIN_SIZE = "1000"

# Example 2: Compress JSON and XML API responses
# $env:RUSTFS_COMPRESS_ENABLE = "on"
# $env:RUSTFS_COMPRESS_EXTENSIONS = ".json,.xml"
# $env:RUSTFS_COMPRESS_MIME_TYPES = "application/json,application/xml"
# $env:RUSTFS_COMPRESS_MIN_SIZE = "1000"

# Example 3: Comprehensive web content compression
# $env:RUSTFS_COMPRESS_ENABLE = "on"
# $env:RUSTFS_COMPRESS_EXTENSIONS = ".html,.css,.js,.json,.xml,.txt,.svg"
# $env:RUSTFS_COMPRESS_MIME_TYPES = "text/*,application/json,application/xml,application/javascript,image/svg+xml"
# $env:RUSTFS_COMPRESS_MIN_SIZE = "1000"

# Example 4: Compress only large text files
# $env:RUSTFS_COMPRESS_ENABLE = "on"
# $env:RUSTFS_COMPRESS_EXTENSIONS = ".txt,.log"
# $env:RUSTFS_COMPRESS_MIME_TYPES = "text/*"
# $env:RUSTFS_COMPRESS_MIN_SIZE = "10240"

# $env:RUSTFS_REGION = "us-east-1"

$env:RUSTFS_ENABLE_SCANNER = "false"
$env:RUSTFS_ENABLE_HEAL = "false"

# Object cache configuration
$env:RUSTFS_OBJECT_CACHE_ENABLE = "true"

# Profiling configuration
$env:RUSTFS_ENABLE_PROFILING = "false"

# Heal configuration queue size
$env:RUSTFS_HEAL_QUEUE_SIZE = "10000"

# rustfs trust system CA certificates
$env:RUSTFS_TRUST_SYSTEM_CA = "true"

# Enable FTP server
$env:RUSTFS_FTPS_ENABLE = "false"

# Increase timeout for high-latency network storage
# $env:RUSTFS_LOCK_ACQUIRE_TIMEOUT = "120"

# Reduce timeout for low-latency local storage
$env:RUSTFS_LOCK_ACQUIRE_TIMEOUT = "30"

if ($args.Count -gt 0) {
    $env:RUSTFS_VOLUMES = $args[0]
}

# Enable jemalloc for memory profiling
if (-not $env:MALLOC_CONF) {
    $env:MALLOC_CONF = "prof:true,prof_active:true,lg_prof_sample:16,log:true,narenas:2,lg_chunk:21,background_thread:true,dirty_decay_ms:1000,muzzy_decay_ms:1000"
}

# Start webhook server
# Start-Process cargo -ArgumentList "run --example webhook -p rustfs-notify" -NoNewWindow

# Start main service
# To run with profiling enabled:
# cargo run --profile profiling --bin rustfs
# To run in release mode:
# cargo run --profile release --bin rustfs
# To run in debug mode:
cargo run --bin rustfs