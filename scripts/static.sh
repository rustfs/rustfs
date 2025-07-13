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

# Updated to use build-rustfs.sh for console assets download
echo "🎨 Downloading console static assets..."
echo "💡 This script now uses build-rustfs.sh for unified asset management."

# Use the unified build script to download console assets
./build-rustfs.sh --download-console --force-console-update
