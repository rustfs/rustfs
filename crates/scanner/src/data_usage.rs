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

use rustfs_ecstore::disk::{BUCKET_META_PREFIX, RUSTFS_META_BUCKET};
use rustfs_utils::path::SLASH_SEPARATOR;
use std::sync::LazyLock;

// Data usage constants
pub const DATA_USAGE_ROOT: &str = SLASH_SEPARATOR;

const DATA_USAGE_OBJ_NAME: &str = ".usage.json";

const DATA_USAGE_BLOOM_NAME: &str = ".bloomcycle.bin";

pub const DATA_USAGE_CACHE_NAME: &str = ".usage-cache.bin";

// Data usage paths (computed at runtime)
pub static DATA_USAGE_BUCKET: LazyLock<String> =
    LazyLock::new(|| format!("{RUSTFS_META_BUCKET}{SLASH_SEPARATOR}{BUCKET_META_PREFIX}"));

pub static DATA_USAGE_OBJ_NAME_PATH: LazyLock<String> =
    LazyLock::new(|| format!("{BUCKET_META_PREFIX}{SLASH_SEPARATOR}{DATA_USAGE_OBJ_NAME}"));

pub static DATA_USAGE_BLOOM_NAME_PATH: LazyLock<String> =
    LazyLock::new(|| format!("{BUCKET_META_PREFIX}{SLASH_SEPARATOR}{DATA_USAGE_BLOOM_NAME}"));

pub static BACKGROUND_HEAL_INFO_PATH: LazyLock<String> =
    LazyLock::new(|| format!("{BUCKET_META_PREFIX}{SLASH_SEPARATOR}.background-heal.json"));
