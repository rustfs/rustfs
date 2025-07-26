#!/bin/bash
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


# 脚本名称：scp_to_servers.sh

rm ./target/x86_64-unknown-linux-gnu/release/rustfs.zip
# 压缩./target/x86_64-unknown-linux-gnu/release/rustfs
zip -j ./target/x86_64-unknown-linux-gnu/release/rustfs.zip ./target/x86_64-unknown-linux-gnu/release/rustfs

# 上传到服务器
LOCAL_FILE="./target/x86_64-unknown-linux-gnu/release/rustfs.zip"
REMOTE_PATH="~"

# 必须传入IP参数，否则报错退出
if [ -z "$1" ]; then
    echo "用法: $0 <server_ip>"
    echo "请传入目标服务器IP地址"
    exit 1
fi

SERVER_LIST=("root@$1")

# 遍历服务器列表
for SERVER in "${SERVER_LIST[@]}"; do
    echo "正在将文件复制到服务器：$SERVER 目标路径：$REMOTE_PATH"
    scp "$LOCAL_FILE" "${SERVER}:${REMOTE_PATH}"
    if [ $? -eq 0 ]; then
        echo "成功复制到 $SERVER"
    else
        echo "复制到 $SERVER 失败"
    fi
done