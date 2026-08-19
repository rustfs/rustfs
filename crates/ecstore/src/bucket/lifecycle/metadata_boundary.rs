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

use std::sync::Arc;

use s3s::dto::{BucketLifecycleConfiguration, ObjectLockConfiguration};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::bucket::metadata::BucketMetadata;
use crate::bucket::metadata_sys::{self, ObjectLockConfigState};
use crate::error::{Error, Result};

#[derive(Debug)]
pub(crate) struct LifecycleExpiryConfigs {
    pub(crate) lifecycle: Option<Arc<BucketLifecycleConfiguration>>,
    pub(crate) object_lock: Option<Arc<ObjectLockConfiguration>>,
    pub(crate) bucket_incarnation_id: Uuid,
    pub(crate) table_bucket_enabled: bool,
}

async fn get_authoritative_metadata(
    api: &crate::store::ECStore,
    bucket: &str,
    bucket_incarnation_id: Uuid,
) -> Result<Arc<BucketMetadata>> {
    let sys = metadata_sys::bucket_metadata_sys_of(&api.ctx)?;
    let sys = sys.read().await.clone();
    let metadata = sys.get_authoritative_metadata(bucket).await?;
    if !metadata.bucket_incarnation_sidecar || metadata.bucket_incarnation_id != bucket_incarnation_id {
        return Err(Error::other(format!("bucket lifecycle metadata is not authoritative: {bucket}")));
    }
    Ok(metadata)
}

pub(crate) async fn lifecycle_expiry_allowed(
    api: &crate::store::ECStore,
    bucket: &str,
    bucket_incarnation_id: Uuid,
) -> Result<bool> {
    Ok(!get_authoritative_metadata(api, bucket, bucket_incarnation_id)
        .await?
        .table_bucket_enabled())
}

pub(crate) async fn get_expiry_configs(api: &crate::store::ECStore, bucket: &str) -> Result<LifecycleExpiryConfigs> {
    let bucket_incarnation_id = api.bucket_incarnation_id_from_disk(bucket).await?;
    let metadata = get_authoritative_metadata(api, bucket, bucket_incarnation_id).await?;
    let table_bucket_enabled = metadata.table_bucket_enabled();

    let lifecycle = if metadata.lifecycle_config.is_none() && !metadata.lifecycle_config_xml.is_empty() {
        return Err(Error::other("persisted bucket lifecycle configuration is invalid"));
    } else {
        metadata
            .lifecycle_config
            .clone()
            .filter(|config| !config.rules.is_empty())
            .map(Arc::new)
    };
    if lifecycle.is_none() {
        return Ok(LifecycleExpiryConfigs {
            lifecycle: None,
            object_lock: None,
            bucket_incarnation_id,
            table_bucket_enabled,
        });
    }
    let object_lock = match metadata_sys::object_lock_config_state_from_authoritative_metadata(&metadata)? {
        ObjectLockConfigState::Configured { config, .. } => Some(Arc::new(config)),
        ObjectLockConfigState::ConfirmedAbsent => None,
        ObjectLockConfigState::Fabricated => {
            return Err(Error::other(format!("bucket Object Lock metadata is not authoritative: {bucket}")));
        }
    };

    Ok(LifecycleExpiryConfigs {
        lifecycle,
        object_lock,
        bucket_incarnation_id,
        table_bucket_enabled,
    })
}

pub(crate) async fn get_lifecycle_config(bucket: &str) -> Result<(BucketLifecycleConfiguration, OffsetDateTime)> {
    metadata_sys::get_lifecycle_config(bucket).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bucket::metadata::BucketMetadata;
    use crate::bucket::metadata_sys::{self, test_support::isolated_store_over_temp_disks};
    use crate::storage_api_contracts::bucket::MakeBucketOptions;
    use s3s::dto::{ExpirationStatus, LifecycleExpiration, LifecycleRule};
    use serial_test::serial;

    fn lifecycle_config() -> BucketLifecycleConfiguration {
        BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: Some(LifecycleExpiration {
                    days: Some(1),
                    ..Default::default()
                }),
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: Some("expire".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
            }],
        }
    }

    #[tokio::test]
    #[serial]
    async fn expiry_configs_are_resolved_from_the_owning_store() {
        let (_dirs_a, store_a) = isolated_store_over_temp_disks().await;
        let (_dirs_b, store_b) = isolated_store_over_temp_disks().await;
        let bucket = "same-name-expiry-config";
        store_a
            .peer_sys
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .unwrap();
        store_b
            .peer_sys
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .unwrap();
        metadata_sys::init_bucket_metadata_sys(store_a.clone(), vec![bucket.to_string()]).await;
        metadata_sys::init_bucket_metadata_sys(store_b.clone(), vec![bucket.to_string()]).await;

        let mut metadata = BucketMetadata::new(bucket);
        let lifecycle = lifecycle_config();
        metadata.lifecycle_config_xml = crate::bucket::utils::serialize(&lifecycle).unwrap();
        metadata.lifecycle_config = Some(lifecycle);
        metadata.table_bucket_config_json = br#"{"enabled":true}"#.to_vec();
        metadata_sys::set_new_bucket_metadata_in(&store_a.ctx, metadata)
            .await
            .unwrap();
        metadata_sys::set_new_bucket_metadata_in(&store_b.ctx, BucketMetadata::new(bucket))
            .await
            .unwrap();

        let configs = get_expiry_configs(&store_a, bucket).await.unwrap();
        assert!(configs.lifecycle.is_some());
        assert!(configs.table_bucket_enabled);
        assert!(
            !lifecycle_expiry_allowed(&store_a, bucket, configs.bucket_incarnation_id)
                .await
                .unwrap()
        );
        assert!(get_expiry_configs(&store_b, bucket).await.unwrap().lifecycle.is_none());
    }
}
