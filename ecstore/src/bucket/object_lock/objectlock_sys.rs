use std::any::{Any, TypeId};
use std::sync::Arc;
use time::OffsetDateTime;
use tracing::{error, warn};

use s3s::dto::{DefaultRetention, ObjectLockLegalHoldStatus, ObjectLockRetentionMode};

use crate::bucket::metadata_sys::get_object_lock_config;
use crate::store_api::ObjectInfo;

use super::objectlock;

pub struct BucketObjectLockSys {}

impl BucketObjectLockSys {
    #[allow(clippy::new_ret_no_self)]
    pub async fn new() -> Arc<Self> {
        Arc::new(Self {})
    }

    pub async fn get(bucket: &str) -> Option<DefaultRetention> {
        if let Some(object_lock_rule) = get_object_lock_config(bucket)
            .await
            .expect("get_object_lock_config err!")
            .0
            .rule
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
