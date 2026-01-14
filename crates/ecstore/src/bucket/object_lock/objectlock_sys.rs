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

use crate::bucket::metadata_sys::get_object_lock_config;
use crate::bucket::object_lock::objectlock;
use crate::store_api::ObjectInfo;
use s3s::dto::{DefaultRetention, ObjectLockLegalHoldStatus, ObjectLockRetentionMode};
use std::sync::Arc;
use time::OffsetDateTime;

pub struct BucketObjectLockSys {}

impl BucketObjectLockSys {
    #[allow(clippy::new_ret_no_self)]
    pub async fn new() -> Arc<Self> {
        Arc::new(Self {})
    }

    pub async fn get(bucket: &str) -> Option<DefaultRetention> {
        if let Ok(object_lock_config) = get_object_lock_config(bucket).await
            && let Some(object_lock_rule) = object_lock_config.0.rule
        {
            return object_lock_rule.default_retention;
        }
        None
    }
}

pub fn enforce_retention_for_deletion(obj_info: &ObjectInfo) -> bool {
    if obj_info.delete_marker {
        return false;
    }

    let lhold = objectlock::get_object_legalhold_meta(obj_info.user_defined.clone());
    match lhold.status {
        Some(st) if st.as_str() == ObjectLockLegalHoldStatus::ON => {
            return true;
        }
        _ => (),
    }

    let ret = objectlock::get_object_retention_meta(obj_info.user_defined.clone());
    match ret.mode {
        Some(r) if (r.as_str() == ObjectLockRetentionMode::COMPLIANCE || r.as_str() == ObjectLockRetentionMode::GOVERNANCE) => {
            let t = objectlock::utc_now_ntp();
            if OffsetDateTime::from(ret.retain_until_date.expect("err!")).unix_timestamp() > t.unix_timestamp() {
                return true;
            }
        }
        _ => (),
    }
    false
}
