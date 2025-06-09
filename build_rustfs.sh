#!/bin/bash
clear

# Get the current platform architecture
ARCH=$(uname -m)

# Set the target directory according to the schema
if [ "$ARCH" == "x86_64" ]; then
    TARGET_DIR="target/x86_64"
elif [ "$ARCH" == "aarch64" ]; then
    TARGET_DIR="target/arm64"
else
    TARGET_DIR="target/unknown"
fi

# Set CARGO_TARGET_DIR and build the project
CARGO_TARGET_DIR=$TARGET_DIR RUSTFLAGS="-C link-arg=-fuse-ld=mold" cargo build --package rustfs

echo -e "\a"
echo -e "\a"
echo -e "\a"
