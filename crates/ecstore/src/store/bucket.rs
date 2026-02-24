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

impl ECStore {
    #[instrument(skip(self))]
    pub(super) async fn handle_make_bucket(&self, bucket: &str, opts: &MakeBucketOptions) -> Result<()> {
        if !is_meta_bucketname(bucket)
            && let Err(err) = check_valid_bucket_name_strict(bucket)
        {
            return Err(StorageError::BucketNameInvalid(err.to_string()));
        }

        // TODO: nslock

        if let Err(err) = self.peer_sys.make_bucket(bucket, opts).await {
            let err = to_object_err(err.into(), vec![bucket]);
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
            info.created = Some(sys.created);
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
                if let Ok(created) = metadata_sys::created_at(&bucket.name).await {
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

        // TODO: nslock

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

        // Check bucket is empty before deletion (per S3 API spec)
        // If bucket is not empty (contains actual objects with xl.meta files) and force
        // is not set, return BucketNotEmpty error.
        // Note: Empty directories (left after object deletion) should NOT count as objects.
        if !opts.force {
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

        self.peer_sys
            .delete_bucket(bucket, opts)
            .await
            .map_err(|e| to_object_err(e.into(), vec![bucket]))?;

        // TODO: replication opts.srdelete_op

        // Delete the metadata
        self.delete_all(RUSTFS_META_BUCKET, format!("{BUCKET_META_PREFIX}/{bucket}").as_str())
            .await?;
        if let Some(monitor) = get_global_bucket_monitor() {
            monitor.delete_bucket(bucket);
        }
        Ok(())
    }
}
