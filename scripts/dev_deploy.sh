#!/usr/bin/env bash
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


# Script name: scp_to_servers.sh

rm ./target/x86_64-unknown-linux-gnu/release/rustfs.zip
# Compress ./target/x86_64-unknown-linux-gnu/release/rustfs
zip -j ./target/x86_64-unknown-linux-gnu/release/rustfs.zip ./target/x86_64-unknown-linux-gnu/release/rustfs

# Upload to server
LOCAL_FILE="./target/x86_64-unknown-linux-gnu/release/rustfs.zip"
REMOTE_PATH="~"

# IP parameter must be provided, otherwise exit with error
if [ -z "$1" ]; then
    echo "Usage: $0 <server_ip>"
    echo "Please provide target server IP address"
    exit 1
fi

SERVER_LIST=("root@$1")

# Iterate through server list
for SERVER in "${SERVER_LIST[@]}"; do
    echo "Copying file to server: $SERVER target path: $REMOTE_PATH"
    scp "$LOCAL_FILE" "${SERVER}:${REMOTE_PATH}"
    if [ $? -eq 0 ]; then
        echo "Successfully copied to $SERVER"
    else
        echo "Failed to copy to $SERVER"
    fi
done