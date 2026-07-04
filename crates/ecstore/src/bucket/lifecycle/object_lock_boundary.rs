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

use std::collections::HashMap;

use crate::bucket::object_lock::objectlock_sys::{self, ObjectLockBlockReason};
use crate::object_api::ObjectInfo;

pub(crate) fn is_object_locked_by_metadata(user_defined: &HashMap<String, String>, is_delete_marker: bool) -> bool {
    rustfs_lifecycle::object_lock::is_object_locked_by_metadata(user_defined, is_delete_marker)
}

pub(crate) async fn check_object_lock_for_deletion(
    bucket: &str,
    obj_info: &ObjectInfo,
    bypass_governance: bool,
) -> Option<ObjectLockBlockReason> {
    objectlock_sys::check_object_lock_for_deletion(bucket, obj_info, bypass_governance).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_object_locked_by_metadata_preserves_object_lock_parser_behavior() {
        let mut user_defined = HashMap::new();
        user_defined.insert("x-amz-object-lock-legal-hold".to_string(), "ON".to_string());

        assert!(is_object_locked_by_metadata(&user_defined, false));
        assert!(!is_object_locked_by_metadata(&user_defined, true));
    }
}
