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

use super::*;
use crate::bucket::{
    metadata::{BUCKET_TABLE_RESERVED_PREFIX, table_bucket_catalog_metadata_prefix},
    utils::is_meta_bucketname,
};
use crate::runtime::sources as runtime_sources;
use crate::set_disk::get_lock_acquire_timeout;
use crate::storage_api_contracts::bucket::SRBucketDeleteOp;
use crate::storage_api_contracts::namespace::NamespaceLocking as _;

const DELETED_BUCKETS_PREFIX: &str = ".deleted";

fn should_override_created_from_metadata(created: OffsetDateTime) -> bool {
    created != OffsetDateTime::UNIX_EPOCH
}

fn validate_table_bucket_delete_allowed(
    bucket: &str,
    table_bucket_enabled: bool,
    table_catalog_metadata_exists: bool,
) -> Result<()> {
    if table_bucket_enabled && table_catalog_metadata_exists {
        return Err(StorageError::BucketNotEmpty(bucket.to_string()));
    }

    Ok(())
}

async fn table_catalog_metadata_exists(bucket: &str) -> bool {
    let local_disks = all_local_disk().await;
    for disk in local_disks.iter() {
        let catalog_path = disk.path().join(bucket).join(BUCKET_TABLE_RESERVED_PREFIX);
        if has_xlmeta_files(&catalog_path).await {
            return true;
        }
    }

    false
}

async fn validate_table_bucket_delete_guard(bucket: &str) -> Result<()> {
    let table_bucket_enabled = metadata_sys::get(bucket)
        .await
        .is_ok_and(|metadata| metadata.table_bucket_enabled());
    if table_bucket_enabled {
        validate_table_bucket_delete_allowed(bucket, true, table_catalog_metadata_exists(bucket).await)?;
    }

    Ok(())
}

fn bucket_delete_metadata_cleanup_prefixes(bucket: &str) -> [String; 2] {
    [
        table_bucket_catalog_metadata_prefix(bucket),
        format!("{BUCKET_META_PREFIX}/{bucket}"),
    ]
}

fn bucket_deleted_marker_prefix(bucket: &str) -> String {
    format!("{BUCKET_META_PREFIX}/{DELETED_BUCKETS_PREFIX}/{bucket}")
}

fn bucket_deleted_marker_volume(bucket: &str) -> String {
    format!("{RUSTFS_META_BUCKET}/{}", bucket_deleted_marker_prefix(bucket))
}

impl ECStore {
    async fn mark_bucket_deleted(&self, bucket: &str) -> Result<()> {
        let marker_volume = bucket_deleted_marker_volume(bucket);

        self.peer_sys
            .make_bucket(
                marker_volume.as_str(),
                &MakeBucketOptions {
                    force_create: true,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| to_object_err(e.into(), vec![bucket]))?;

        Ok(())
    }

    async fn cleanup_deleted_bucket_metadata(&self, bucket: &str, include_deleted_marker: bool) -> Result<()> {
        for prefix in bucket_delete_metadata_cleanup_prefixes(bucket) {
            self.delete_all(RUSTFS_META_BUCKET, prefix.as_str()).await?;
        }

        if include_deleted_marker {
            let marker_prefix = bucket_deleted_marker_prefix(bucket);
            self.delete_all(RUSTFS_META_BUCKET, marker_prefix.as_str()).await?;
        }

        metadata_sys::remove_bucket_metadata(bucket).await?;
        runtime_sources::delete_bucket_monitor_entry(bucket);
        Ok(())
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_make_bucket(&self, bucket: &str, opts: &MakeBucketOptions) -> Result<()> {
        if !is_meta_bucketname(bucket)
            && let Err(err) = check_valid_bucket_name_strict(bucket)
        {
            return Err(StorageError::BucketNameInvalid(err.to_string()));
        }

        let _ns_guard = if !opts.no_lock {
            let ns_lock = self.new_ns_lock(bucket, bucket).await?;
            Some(
                ns_lock
                    .get_write_lock(get_lock_acquire_timeout())
                    .await
                    .map_err(|e| match e {
                        rustfs_lock::error::LockError::QuorumNotReached { required, achieved } => {
                            StorageError::NamespaceLockQuorumUnavailable {
                                mode: "write",
                                bucket: bucket.to_string(),
                                object: bucket.to_string(),
                                required,
                                achieved,
                            }
                        }
                        other => StorageError::Lock(other),
                    })?,
            )
        } else {
            None
        };

        if let Err(err) = self.peer_sys.make_bucket(bucket, opts).await {
            let err = to_object_err(err.into(), vec![bucket]);
            if is_err_bucket_exists(&err)
                && let Err(heal_err) = self
                    .handle_heal_bucket(
                        bucket,
                        &HealOpts {
                            recreate: true,
                            ..Default::default()
                        },
                    )
                    .await
            {
                warn!("best-effort bucket heal after BucketExists failed: {heal_err}");
            }
            if !is_err_bucket_exists(&err) {
                error!("make bucket failed: {err}");
                let _ = self
                    .delete_bucket(
                        bucket,
                        &DeleteBucketOptions {
                            no_lock: true,
                            no_recreate: true,
                            ..Default::default()
                        },
                    )
                    .await;
            }
            return Err(err);
        };

        let mut meta = BucketMetadata::new(bucket);

        meta.set_created(opts.created_at);

        if opts.lock_enabled {
            meta.object_lock_config_xml = crate::bucket::utils::serialize::<ObjectLockConfiguration>(&enableObjcetLockConfig)?;
            meta.versioning_config_xml = crate::bucket::utils::serialize::<VersioningConfiguration>(&enableVersioningConfig)?;
        }

        if opts.versioning_enabled {
            meta.versioning_config_xml = crate::bucket::utils::serialize::<VersioningConfiguration>(&enableVersioningConfig)?;
        }

        meta.save().await?;

        set_bucket_metadata(bucket.to_string(), meta).await?;

        Ok(())
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_get_bucket_info(&self, bucket: &str, opts: &BucketOptions) -> Result<BucketInfo> {
        let mut info = self.peer_sys.get_bucket_info(bucket, opts).await?;

        if let Ok(sys) = metadata_sys::get(bucket).await {
            if should_override_created_from_metadata(sys.created) {
                info.created = Some(sys.created);
            }
            info.versioning = sys.versioning();
            info.object_locking = sys.object_locking();
        }

        Ok(info)
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_list_bucket(&self, opts: &BucketOptions) -> Result<Vec<BucketInfo>> {
        // TODO: opts.cached

        let mut buckets = self.peer_sys.list_bucket(opts).await?;

        if !opts.no_metadata {
            for bucket in buckets.iter_mut() {
                if let Ok(created) = metadata_sys::created_at(&bucket.name).await
                    && should_override_created_from_metadata(created)
                {
                    bucket.created = Some(created);
                }
            }
        }
        Ok(buckets)
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_delete_bucket(&self, bucket: &str, opts: &DeleteBucketOptions) -> Result<()> {
        if is_meta_bucketname(bucket) {
            return Err(StorageError::BucketNameInvalid(bucket.to_string()));
        }

        if let Err(err) = check_valid_bucket_name(bucket) {
            return Err(StorageError::BucketNameInvalid(err.to_string()));
        }

        let _ns_guard = if !opts.no_lock {
            let ns_lock = self.new_ns_lock(bucket, bucket).await?;
            Some(
                ns_lock
                    .get_write_lock(get_lock_acquire_timeout())
                    .await
                    .map_err(|e| match e {
                        rustfs_lock::error::LockError::QuorumNotReached { required, achieved } => {
                            StorageError::NamespaceLockQuorumUnavailable {
                                mode: "write",
                                bucket: bucket.to_string(),
                                object: bucket.to_string(),
                                required,
                                achieved,
                            }
                        }
                        other => StorageError::Lock(other),
                    })?,
            )
        } else {
            None
        };

        // Check bucket exists before deletion (per S3 API spec)
        // If bucket doesn't exist, return NoSuchBucket error
        if let Err(err) = self.peer_sys.get_bucket_info(bucket, &BucketOptions::default()).await {
            // Convert DiskError to StorageError for comparison
            let storage_err: StorageError = err.into();
            if is_err_bucket_not_found(&storage_err) {
                return Err(StorageError::BucketNotFound(bucket.to_string()));
            }
            return Err(to_object_err(storage_err, vec![bucket]));
        }

        validate_table_bucket_delete_guard(bucket).await?;

        let sr_mark_delete = opts.srdelete_op == SRBucketDeleteOp::MarkDelete;
        let sr_purge = opts.srdelete_op == SRBucketDeleteOp::Purge;

        // Check bucket is empty before deletion (per S3 API spec)
        // If bucket is not empty (contains actual objects with xl.meta files) and force
        // is not set, return BucketNotEmpty error.
        // Note: Empty directories (left after object deletion) should NOT count as objects.
        if !opts.force && !sr_mark_delete {
            let local_disks = all_local_disk().await;
            for disk in local_disks.iter() {
                // Check if bucket directory contains any xl.meta files (actual objects)
                // We recursively scan for xl.meta files to determine if bucket has objects
                // Use the disk's root path to construct bucket path
                let bucket_path = disk.path().join(bucket);
                if has_xlmeta_files(&bucket_path).await {
                    return Err(StorageError::BucketNotEmpty(bucket.to_string()));
                }
            }
        }

        if sr_mark_delete {
            self.mark_bucket_deleted(bucket).await?;
            self.cleanup_deleted_bucket_metadata(bucket, false).await?;
            return Ok(());
        }

        self.peer_sys
            .delete_bucket(bucket, opts)
            .await
            .map_err(|e| to_object_err(e.into(), vec![bucket]))?;

        self.cleanup_deleted_bucket_metadata(bucket, sr_purge).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        bucket_delete_metadata_cleanup_prefixes, bucket_deleted_marker_prefix, bucket_deleted_marker_volume,
        should_override_created_from_metadata, validate_table_bucket_delete_allowed,
    };
    use crate::bucket::metadata::table_bucket_catalog_metadata_prefix;
    use crate::bucket::metadata_sys;
    use crate::disk::{BUCKET_META_PREFIX, RUSTFS_META_BUCKET};
    use crate::error::StorageError;
    use crate::object_api::{ObjectOptions, PutObjReader};
    use crate::storage_api_contracts::{
        bucket::{BucketOperations as _, DeleteBucketOptions, MakeBucketOptions, SRBucketDeleteOp},
        object::{ObjectIO as _, ObjectOperations as _},
    };
    use crate::store::{ECStore, init_local_disks};
    use crate::{
        disk::endpoint::Endpoint,
        layout::endpoints::{EndpointServerPools, Endpoints, PoolEndpoints},
    };
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use time::OffsetDateTime;
    use tokio::sync::OnceCell;
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    static BUCKET_DELETE_TEST_ENV: OnceCell<(Vec<PathBuf>, Arc<ECStore>)> = OnceCell::const_new();

    async fn setup_bucket_delete_test_env() -> (Vec<PathBuf>, Arc<ECStore>) {
        BUCKET_DELETE_TEST_ENV
            .get_or_init(|| async {
                let temp_dir = std::env::temp_dir().join(format!("rustfs_bucket_delete_test_{}", Uuid::new_v4()));
                tokio::fs::create_dir_all(&temp_dir)
                    .await
                    .expect("test base directory should be created");

                let disk_paths = (0..4)
                    .map(|disk_idx| temp_dir.join(format!("disk{disk_idx}")))
                    .collect::<Vec<_>>();

                for disk_path in &disk_paths {
                    tokio::fs::create_dir_all(disk_path)
                        .await
                        .expect("disk directory should be created");
                }

                let mut endpoints = Vec::with_capacity(disk_paths.len());
                for (disk_idx, disk_path) in disk_paths.iter().enumerate() {
                    let mut endpoint =
                        Endpoint::try_from(disk_path.to_str().expect("disk path should be utf8")).expect("endpoint should parse");
                    endpoint.set_pool_index(0);
                    endpoint.set_set_index(0);
                    endpoint.set_disk_index(disk_idx);
                    endpoints.push(endpoint);
                }

                let endpoint_pools = EndpointServerPools(vec![PoolEndpoints {
                    legacy: false,
                    set_count: 1,
                    drives_per_set: 4,
                    endpoints: Endpoints::from(endpoints),
                    cmd_line: "bucket-delete-test".to_string(),
                    platform: format!("OS: {} | Arch: {}", std::env::consts::OS, std::env::consts::ARCH),
                }]);

                init_local_disks(endpoint_pools.clone())
                    .await
                    .expect("local disks should initialize");
                let ecstore =
                    ECStore::new("127.0.0.1:0".parse().expect("test address"), endpoint_pools, CancellationToken::new())
                        .await
                        .expect("ECStore should initialize");

                if metadata_sys::get_global_bucket_metadata_sys().is_none() {
                    metadata_sys::init_bucket_metadata_sys(ecstore.clone(), Vec::new()).await;
                }

                (disk_paths, ecstore)
            })
            .await
            .clone()
    }

    async fn create_bucket_with_object(ecstore: &Arc<ECStore>, bucket: &str, object: &str) {
        ecstore
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");

        let mut reader = PutObjReader::from_vec(b"delete bucket semantics".to_vec());
        ecstore
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("object should be written");
        ecstore
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("object should be readable before bucket delete");
    }

    async fn any_disk_path_exists(disk_paths: &[PathBuf], relative_path: impl AsRef<Path>) -> bool {
        for disk_path in disk_paths {
            if tokio::fs::try_exists(disk_path.join(relative_path.as_ref()))
                .await
                .expect("test disk path should be stat-able")
            {
                return true;
            }
        }
        false
    }

    async fn any_disk_has_object_metadata(disk_paths: &[PathBuf], bucket: &str) -> bool {
        for disk_path in disk_paths {
            if super::has_xlmeta_files(&disk_path.join(bucket)).await {
                return true;
            }
        }
        false
    }

    #[test]
    fn should_not_override_when_metadata_created_is_unix_epoch() {
        assert!(!should_override_created_from_metadata(OffsetDateTime::UNIX_EPOCH));
    }

    #[test]
    fn should_override_when_metadata_created_is_valid_time() {
        let created = OffsetDateTime::from_unix_timestamp(1704067200).expect("valid timestamp");
        assert!(should_override_created_from_metadata(created));
    }

    #[test]
    fn table_bucket_delete_guard_rejects_remaining_catalog_metadata() {
        let err = validate_table_bucket_delete_allowed("table-bucket", true, true).unwrap_err();

        assert!(matches!(err, StorageError::BucketNotEmpty(bucket) if bucket == "table-bucket"));
        assert!(validate_table_bucket_delete_allowed("table-bucket", true, false).is_ok());
        assert!(validate_table_bucket_delete_allowed("regular-bucket", false, true).is_ok());
    }

    #[test]
    fn bucket_delete_metadata_cleanup_removes_internal_table_catalog_prefix() {
        let prefixes = bucket_delete_metadata_cleanup_prefixes("analytics");

        assert!(prefixes.contains(&table_bucket_catalog_metadata_prefix("analytics")));
        assert!(prefixes.contains(&"buckets/analytics".to_string()));
    }

    #[test]
    fn bucket_delete_marker_path_uses_internal_deleted_bucket_metadata_prefix() {
        assert_eq!(bucket_deleted_marker_prefix("analytics"), "buckets/.deleted/analytics");
        assert_eq!(
            bucket_deleted_marker_volume("analytics"),
            format!("{RUSTFS_META_BUCKET}/{BUCKET_META_PREFIX}/.deleted/analytics")
        );
    }

    #[tokio::test]
    async fn bucket_delete_mark_delete_marks_metadata_deleted_without_physical_object_delete() {
        let (disk_paths, ecstore) = setup_bucket_delete_test_env().await;
        let bucket = format!("bucket-mark-delete-{}", Uuid::new_v4().simple());
        let object = "object.txt";

        create_bucket_with_object(&ecstore, &bucket, object).await;
        assert!(metadata_sys::get(&bucket).await.is_ok());

        ecstore
            .delete_bucket(
                &bucket,
                &DeleteBucketOptions {
                    srdelete_op: SRBucketDeleteOp::MarkDelete,
                    ..Default::default()
                },
            )
            .await
            .expect("MarkDelete should not reject non-empty bucket data");

        assert!(
            any_disk_has_object_metadata(&disk_paths, &bucket).await,
            "MarkDelete must not physically remove object xl.meta data"
        );
        assert!(
            any_disk_path_exists(&disk_paths, bucket_deleted_marker_volume(&bucket)).await,
            "MarkDelete should persist the deleted-bucket marker"
        );
        assert!(
            metadata_sys::get(&bucket).await.is_err(),
            "deleted bucket metadata must be removed from the local cache"
        );
    }

    #[tokio::test]
    async fn bucket_delete_purge_removes_bucket_data_and_internal_metadata() {
        let (disk_paths, ecstore) = setup_bucket_delete_test_env().await;
        let bucket = format!("bucket-purge-{}", Uuid::new_v4().simple());
        let object = "object.txt";
        let metadata_prefix = format!("{RUSTFS_META_BUCKET}/{BUCKET_META_PREFIX}/{bucket}");

        create_bucket_with_object(&ecstore, &bucket, object).await;
        assert!(any_disk_path_exists(&disk_paths, &metadata_prefix).await);

        ecstore
            .delete_bucket(
                &bucket,
                &DeleteBucketOptions {
                    force: true,
                    srdelete_op: SRBucketDeleteOp::Purge,
                    ..Default::default()
                },
            )
            .await
            .expect("Purge should force-delete bucket data");

        assert!(!any_disk_path_exists(&disk_paths, &bucket).await, "Purge should remove the bucket volume");
        assert!(
            !any_disk_path_exists(&disk_paths, &metadata_prefix).await,
            "Purge should remove bucket metadata prefix"
        );
        assert!(
            metadata_sys::get(&bucket).await.is_err(),
            "purged bucket metadata must be removed from the local cache"
        );
    }

    #[tokio::test]
    async fn bucket_delete_default_s3_delete_still_rejects_non_empty_bucket() {
        let (disk_paths, ecstore) = setup_bucket_delete_test_env().await;
        let bucket = format!("bucket-s3-delete-{}", Uuid::new_v4().simple());
        let object = "object.txt";

        create_bucket_with_object(&ecstore, &bucket, object).await;

        let err = ecstore
            .delete_bucket(&bucket, &DeleteBucketOptions::default())
            .await
            .expect_err("default S3 DeleteBucket should reject non-empty buckets");

        assert!(matches!(err, StorageError::BucketNotEmpty(name) if name == bucket));
        assert!(
            any_disk_has_object_metadata(&disk_paths, &bucket).await,
            "failed default S3 DeleteBucket must keep object data"
        );
        assert!(
            metadata_sys::get(&bucket).await.is_ok(),
            "failed default S3 DeleteBucket must keep metadata cache"
        );
    }
}
