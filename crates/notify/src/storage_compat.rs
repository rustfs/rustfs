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

use chrono::{DateTime, Utc};
use rustfs_config::server_config::Config;
use rustfs_ecstore::{config, global};

use crate::event::NotifyObjectInfo;

type EcstoreObjectInfo = rustfs_ecstore::store_api::ObjectInfo;

#[derive(Debug)]
pub(crate) enum NotifyConfigStoreError {
    StorageNotAvailable,
    Read(String),
    Save(String),
}

pub(crate) async fn update_server_config<F>(mut modifier: F) -> Result<Option<Config>, NotifyConfigStoreError>
where
    F: FnMut(&mut Config) -> bool,
{
    let Some(store) = global::resolve_object_store_handle() else {
        return Err(NotifyConfigStoreError::StorageNotAvailable);
    };

    let mut new_config = config::com::read_config_without_migrate(store.clone())
        .await
        .map_err(|err| NotifyConfigStoreError::Read(err.to_string()))?;

    if !modifier(&mut new_config) {
        return Ok(None);
    }

    config::com::save_server_config(store, &new_config)
        .await
        .map_err(|err| NotifyConfigStoreError::Save(err.to_string()))?;

    Ok(Some(new_config))
}

impl From<EcstoreObjectInfo> for NotifyObjectInfo {
    fn from(object: EcstoreObjectInfo) -> Self {
        Self {
            bucket: object.bucket,
            name: object.name,
            size: object.size,
            etag: object.etag,
            content_type: object.content_type,
            user_defined: object
                .user_defined
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
            version_id: object.version_id.map(|version_id| version_id.to_string()),
            mod_time: object
                .mod_time
                .and_then(|value| DateTime::<Utc>::from_timestamp(value.unix_timestamp(), value.nanosecond())),
            restore_expires: object
                .restore_expires
                .and_then(|value| DateTime::<Utc>::from_timestamp(value.unix_timestamp(), value.nanosecond())),
            storage_class: object.storage_class,
            transitioned_tier: (!object.transitioned_object.tier.is_empty()).then_some(object.transitioned_object.tier),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_ecstore::bucket::lifecycle::bucket_lifecycle_ops::TransitionedObject;
    use std::{collections::HashMap, sync::Arc};
    use time::{Duration, OffsetDateTime};

    #[test]
    fn ecstore_object_info_conversion_preserves_notify_event_fields() {
        let mod_time = OffsetDateTime::UNIX_EPOCH + Duration::seconds(42);
        let restore_expires = OffsetDateTime::UNIX_EPOCH + Duration::seconds(1_700_000_000);
        let mut metadata = HashMap::new();
        metadata.insert("x-amz-meta-key".to_string(), "value".to_string());

        let converted = NotifyObjectInfo::from(EcstoreObjectInfo {
            bucket: "bucket".to_string(),
            name: "object".to_string(),
            size: 123,
            etag: Some("etag".to_string()),
            content_type: Some("text/plain".to_string()),
            user_defined: Arc::new(metadata),
            mod_time: Some(mod_time),
            restore_expires: Some(restore_expires),
            storage_class: Some("GLACIER".to_string()),
            transitioned_object: TransitionedObject {
                tier: "DEEP_ARCHIVE".to_string(),
                ..Default::default()
            },
            ..Default::default()
        });

        assert_eq!(converted.bucket, "bucket");
        assert_eq!(converted.name, "object");
        assert_eq!(converted.size, 123);
        assert_eq!(converted.etag.as_deref(), Some("etag"));
        assert_eq!(converted.content_type.as_deref(), Some("text/plain"));
        assert_eq!(converted.user_defined.get("x-amz-meta-key").map(String::as_str), Some("value"));
        assert_eq!(converted.mod_time, DateTime::<Utc>::from_timestamp(42, 0));
        assert_eq!(converted.restore_expires, DateTime::<Utc>::from_timestamp(1_700_000_000, 0));
        assert_eq!(converted.storage_class.as_deref(), Some("GLACIER"));
        assert_eq!(converted.transitioned_tier.as_deref(), Some("DEEP_ARCHIVE"));
    }
}
