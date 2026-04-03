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

use crate::error::{Result, StorageError};
use http::HeaderMap;
use std::sync::LazyLock;

static WASABI_VERSION_IDS_ENABLED: LazyLock<bool> =
    LazyLock::new(|| rustfs_utils::get_env_bool(rustfs_config::ENV_WASABI_VERSION_IDS, true));

/// HTTP header used by replication/sync to pin the S3 `VersionId` on PUT (Wasabi compatibility).
///
/// HTTP header names are case-insensitive; this is the canonical lowercase form for lookups.
pub const WASABI_SET_VERSION_ID_HEADER: &str = "x-wasabi-set-version-id";

/// Returns whether Wasabi-compatible version id mode is enabled for this process.
///
/// The value is read once from [`rustfs_config::ENV_WASABI_VERSION_IDS`] (default `true`).
#[inline]
pub fn wasabi_version_ids_enabled() -> bool {
    *WASABI_VERSION_IDS_ENABLED
}

/// When Wasabi version-id mode is **off**, rejects any non-empty `X-Wasabi-Set-Version-Id` header.
///
/// When mode is **on**, this is a no-op (strict validation of the value is implemented elsewhere).
pub fn ensure_wasabi_set_version_id_header_allowed(headers: &HeaderMap, bucket: &str, object: &str) -> Result<()> {
    if wasabi_version_ids_enabled() {
        return Ok(());
    }
    let Some(raw) = headers.get(WASABI_SET_VERSION_ID_HEADER) else {
        return Ok(());
    };
    let s = raw.to_str().map_err(|_| {
        StorageError::InvalidArgument(
            bucket.to_owned(),
            object.to_owned(),
            "X-Wasabi-Set-Version-Id: invalid header value encoding".to_owned(),
        )
    })?;
    if s.trim().is_empty() {
        return Ok(());
    }
    Err(StorageError::InvalidArgument(
        bucket.to_owned(),
        object.to_owned(),
        "X-Wasabi-Set-Version-Id is not supported when RUSTFS_WASABI_VERSION_IDS is false".to_owned(),
    ))
}
