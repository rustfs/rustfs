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

//! Process-wide toggle for Wasabi-compatible version ids vs legacy UUID behavior.

use std::sync::LazyLock;

static WASABI_VERSION_IDS_ENABLED: LazyLock<bool> =
    LazyLock::new(|| rustfs_utils::get_env_bool(rustfs_config::ENV_WASABI_VERSION_IDS, true));

/// Returns whether Wasabi-compatible version id mode is enabled for this process.
///
/// The value is read once from [`rustfs_config::ENV_WASABI_VERSION_IDS`] (default `true`).
#[inline]
pub fn wasabi_version_ids_enabled() -> bool {
    *WASABI_VERSION_IDS_ENABLED
}
