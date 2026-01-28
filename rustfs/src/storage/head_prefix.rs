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

use rustfs_ecstore::store::ECStore;
use std::sync::Arc;

/// Determines if the key "looks like a prefix" (ends with `/`).
/// Note: No special handling for empty strings here; the caller must ensure the key has passed `validate_object_key`.
#[inline]
pub(crate) fn is_prefix_key(key: &str) -> bool {
    key.ends_with('/')
}

/// Constructs a more explicit error message when `HEAD` is performed on a `prefix`-style key but the directory marker object is missing.
///
/// `has_children`:
/// - true: Indicates that there are objects under the prefix, but the "directory marker object" does not exist (common in semantics relying solely on prefix listing).
/// - false: Indicates that there are no objects under the prefix either.
pub(crate) fn head_prefix_not_found_message(bucket: &str, key: &str, has_children: bool) -> String {
    if has_children {
        format!(
            "NoSuchKey: key `{}` looks like a prefix (ends with `/`), prefix has children objects, \
but directory marker object does not exist in bucket `{}`",
            key, bucket
        )
    } else {
        format!(
            "NoSuchKey: key `{}` looks like a prefix (ends with `/`), but no directory marker object \
and no objects exist under this prefix in bucket `{}`",
            key, bucket
        )
    }
}

/// Lightweight probe: Checks if any objects exist under the prefix (max_keys=1).
///
/// Purpose: Only used to optimize the error message, without changing error codes or external semantics.
pub(crate) async fn probe_prefix_has_children(store: Arc<ECStore>, bucket: &str, prefix: &str) -> Result<bool, String> {
    // Even if the underlying implementation has overhead for list, this is only one extra request, and it only happens when NotFound occurs and the key ends with `/`.
    // No delimiter is introduced here to avoid "virtual directory" hierarchy affecting the judgment: as long as there are any objects under the prefix.
    let res = store
        .list_objects(
            bucket, prefix, None,  // marker
            None,  // delimiter
            1,     // max_keys
            false, // fetch_owner
            false, // include_versions
            None,  // start_after
        )
        .await
        .map_err(|e| format!("{e}"))?;

    let has_objects = !res.objects.is_empty();
    let has_common_prefixes = !res.common_prefixes.is_empty();

    Ok(has_objects || has_common_prefixes)
}
