// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub const DEFAULT_DIR: &str = "/opt/rustfs/events"; // Default directory for event store
pub const DEFAULT_LIMIT: u64 = 100000; // Default store limit
pub const DEFAULT_EXT: &str = ".unknown"; // Default file extension
pub const COMPRESS_EXT: &str = ".snappy"; // Extension for compressed files

/// STORE_EXTENSION - file extension of an event file in store
pub const STORE_EXTENSION: &str = ".event";
