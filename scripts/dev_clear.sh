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

for i in {0..3}; do
    DIR="/data/rustfs$i"
    echo "处理 $DIR"
    if [ -d "$DIR" ]; then
        echo "清空 $DIR"
        sudo rm -rf "$DIR"/* "$DIR"/.[!.]* "$DIR"/..?* 2>/dev/null || true
        echo "已清空 $DIR"
    else
        echo "$DIR 不存在，跳过"
    fi
done