#!/bin/bash

# 脚本名称：scp_to_servers.sh

rm ./target/x86_64-unknown-linux-musl/release/rustfs.zip
# 压缩./target/x86_64-unknown-linux-musl/release/rustfs
zip  ./target/x86_64-unknown-linux-musl/release/rustfs.zip ./target/x86_64-unknown-linux-musl/release/rustfs


# 本地文件路径
LOCAL_FILE="./target/x86_64-unknown-linux-musl/release/rustfs.zip"
REMOTE_PATH="~"

# 定义服务器列表数组
# 格式：服务器 IP 用户名 目标路径
SERVER_LIST=(
    "root@121.89.80.13"
)

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


# ps -ef | grep rustfs | awk '{print $2}'| xargs kill -9