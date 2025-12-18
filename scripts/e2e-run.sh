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

chmod +x $BIN
mkdir -p $VOLUME

export RUST_LOG="rustfs=debug,ecstore=debug,s3s=debug,iam=debug"
export RUST_BACKTRACE=full
$BIN $VOLUME > /tmp/rustfs.log 2>&1 &

sleep 10

export AWS_ACCESS_KEY_ID=rustfsadmin
export AWS_SECRET_ACCESS_KEY=rustfsadmin
export AWS_REGION=us-east-1
export AWS_ENDPOINT_URL=http://localhost:9000
export RUST_LOG="s3s_e2e=debug,s3s_test=info,s3s=debug"
export RUST_BACKTRACE=full
s3s-e2e

killall $BIN
