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
CARGO_TARGET_DIR=$TARGET_DIR RUSTFLAGS="-C link-arg=-fuse-ld=mold" cargo build --release --package rustfs

echo -e "\a"
echo -e "\a"
echo -e "\a"
