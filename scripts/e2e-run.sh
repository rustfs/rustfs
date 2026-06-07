#!/usr/bin/env bash
set -ex
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

BIN=$1
VOLUME=$2
RUSTFS_PID=""
RUSTFS_TEST_PORT="${RUSTFS_TEST_PORT:-9000}"
RUSTFS_TEST_LOG="${RUSTFS_TEST_LOG:-${VOLUME}.log}"

cleanup() {
    if [ -n "${RUSTFS_PID}" ] && kill -0 "${RUSTFS_PID}" 2>/dev/null; then
        kill "${RUSTFS_PID}" 2>/dev/null || true
        wait "${RUSTFS_PID}" 2>/dev/null || true
    fi
}

trap cleanup EXIT INT TERM

chmod +x "$BIN"
mkdir -p "$VOLUME"
mkdir -p "$(dirname "${RUSTFS_TEST_LOG}")"

export RUSTFS_ACCESS_KEY="${RUSTFS_ACCESS_KEY:-rustfsadmin}"
export RUSTFS_SECRET_KEY="${RUSTFS_SECRET_KEY:-rustfsadmin}"
export RUST_LOG="rustfs=debug,ecstore=debug,s3s=debug,iam=debug"
export RUST_BACKTRACE=full
"$BIN" --address "127.0.0.1:${RUSTFS_TEST_PORT}" "$VOLUME" > "${RUSTFS_TEST_LOG}" 2>&1 &
RUSTFS_PID=$!

sleep 10

export AWS_ACCESS_KEY_ID="${RUSTFS_ACCESS_KEY:-rustfsadmin}"
export AWS_SECRET_ACCESS_KEY="${RUSTFS_SECRET_KEY:-rustfsadmin}"
export AWS_REGION=us-east-1
export AWS_ENDPOINT_URL="http://localhost:${RUSTFS_TEST_PORT}"
export RUST_LOG="s3s_e2e=debug,s3s_test=info,s3s=debug"
export RUST_BACKTRACE=full
s3s-e2e
