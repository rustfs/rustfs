#!/bin/bash
clear

# 获取当前平台架构
ARCH=$(uname -m)

# 根据架构设置 target 目录
if [ "$ARCH" == "x86_64" ]; then
    TARGET_DIR="target/x86_64"
elif [ "$ARCH" == "aarch64" ]; then
    TARGET_DIR="target/arm64"
else
    TARGET_DIR="target/unknown"
fi

# 设置 CARGO_TARGET_DIR 并构建项目
CARGO_TARGET_DIR=$TARGET_DIR RUSTFLAGS="-C link-arg=-fuse-ld=mold" cargo build --package rustfs

echo -e "\a"
echo -e "\a"
echo -e "\a"
