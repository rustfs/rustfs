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


current_dir=$(pwd)
echo "Current directory: $current_dir"

if [ -z "$RUST_LOG" ]; then
    export RUST_BACKTRACE=1
    export RUST_LOG="rustfs=info,ecstore=info,s3s=debug,iam=info"
fi

# deploy/logs/notify directory
echo "Creating log directory if it does not exist..."
mkdir -p "$current_dir/deploy/logs/notify"

# Start webhook server
echo "Starting webhook server..."
cargo run --example webhook -p rustfs-notify &