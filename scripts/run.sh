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
    export RUST_LOG="rustfs=debug,ecstore=debug,s3s=debug,iam=debug"
fi

# export RUSTFS_ERASURE_SET_DRIVE_COUNT=5

# export RUSTFS_STORAGE_CLASS_INLINE_BLOCK="512 KB"

export RUSTFS_VOLUMES="./target/volume/test{0...4}"
# export RUSTFS_VOLUMES="./target/volume/test"
export RUSTFS_ADDRESS="[::]:9000"
export RUSTFS_CONSOLE_ENABLE=true
export RUSTFS_CONSOLE_ADDRESS="[::]:9002"
# export RUSTFS_SERVER_DOMAINS="localhost:9000"
# HTTPS 证书目录
# export RUSTFS_TLS_PATH="./deploy/certs"

# 具体路径修改为配置文件真实路径，obs.example.toml 仅供参考 其中`RUSTFS_OBS_CONFIG` 和下面变量二选一
export RUSTFS_OBS_CONFIG="./deploy/config/obs.example.toml"

# 如下变量需要必须参数都有值才可以，以及会覆盖配置文件中的值
export RUSTFS__OBSERVABILITY__ENDPOINT=http://localhost:4317
export RUSTFS__OBSERVABILITY__USE_STDOUT=false
export RUSTFS__OBSERVABILITY__SAMPLE_RATIO=2.0
export RUSTFS__OBSERVABILITY__METER_INTERVAL=30
export RUSTFS__OBSERVABILITY__SERVICE_NAME=rustfs
export RUSTFS__OBSERVABILITY__SERVICE_VERSION=0.1.0
export RUSTFS__OBSERVABILITY__ENVIRONMENT=develop
export RUSTFS__OBSERVABILITY__LOGGER_LEVEL=debug
export RUSTFS__OBSERVABILITY__LOCAL_LOGGING_ENABLED=true
export RUSTFS__SINKS__FILE__ENABLED=true
export RUSTFS__SINKS__FILE__PATH="./deploy/logs/rustfs.log"
export RUSTFS__SINKS__WEBHOOK__ENABLED=false
export RUSTFS__SINKS__WEBHOOK__ENDPOINT=""
export RUSTFS__SINKS__WEBHOOK__AUTH_TOKEN=""
export RUSTFS__SINKS__KAFKA__ENABLED=false
export RUSTFS__SINKS__KAFKA__BOOTSTRAP_SERVERS=""
export RUSTFS__SINKS__KAFKA__TOPIC=""
export RUSTFS__LOGGER__QUEUE_CAPACITY=10

# 事件消息配置
export RUSTFS_EVENT_CONFIG="./deploy/config/event.example.toml"

if [ -n "$1" ]; then
	export RUSTFS_VOLUMES="$1"
fi

# 启动 webhook 服务器
cargo run --example webhook -p rustfs-event-notifier &
# 启动主服务
cargo run --bin rustfs