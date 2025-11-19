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

use crate::{Error, Result};
use async_trait::async_trait;
use rustfs_common::heal_channel::{HealOpts, HealScanMode};
use rustfs_ecstore::{
    disk::{DiskStore, endpoint::Endpoint},
    store::ECStore,
    store_api::{BucketInfo, ObjectIO, StorageAPI},
};
use rustfs_madmin::heal_commands::HealResultItem;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

/// Disk status for heal operations
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiskStatus {
    /// Ok
    Ok,
    /// Offline
    Offline,
    /// Corrupt
    Corrupt,
    /// Missing
    Missing,
    /// Permission denied
    PermissionDenied,
    /// Faulty
    Faulty,
    /// Root mount
    RootMount,
    /// Unknown
    Unknown,
    /// Unformatted
    Unformatted,
}

/// Heal storage layer interface
#[async_trait]
pub trait HealStorageAPI: Send + Sync {
    /// Get object meta
    async fn get_object_meta(&self, bucket: &str, object: &str) -> Result<Option<rustfs_ecstore::store_api::ObjectInfo>>;

    /// Get object data
    async fn get_object_data(&self, bucket: &str, object: &str) -> Result<Option<Vec<u8>>>;

    /// Put object data
    async fn put_object_data(&self, bucket: &str, object: &str, data: &[u8]) -> Result<()>;

    /// Delete object
    async fn delete_object(&self, bucket: &str, object: &str) -> Result<()>;

    /// Check object integrity
    async fn verify_object_integrity(&self, bucket: &str, object: &str) -> Result<bool>;

    /// EC decode rebuild
    async fn ec_decode_rebuild(&self, bucket: &str, object: &str) -> Result<Vec<u8>>;

    /// Get disk status
    async fn get_disk_status(&self, endpoint: &Endpoint) -> Result<DiskStatus>;

    /// Format disk
    async fn format_disk(&self, endpoint: &Endpoint) -> Result<()>;

    /// Get bucket info
    async fn get_bucket_info(&self, bucket: &str) -> Result<Option<BucketInfo>>;

    /// Fix bucket metadata
    async fn heal_bucket_metadata(&self, bucket: &str) -> Result<()>;

    /// Get all buckets
    async fn list_buckets(&self) -> Result<Vec<BucketInfo>>;

    /// Check object exists
    async fn object_exists(&self, bucket: &str, object: &str) -> Result<bool>;

    /// Get object size
    async fn get_object_size(&self, bucket: &str, object: &str) -> Result<Option<u64>>;

    /// Get object checksum
    async fn get_object_checksum(&self, bucket: &str, object: &str) -> Result<Option<String>>;

    /// Heal object using ecstore
    async fn heal_object(
        &self,
        bucket: &str,
        object: &str,
        version_id: Option<&str>,
        opts: &HealOpts,
    ) -> Result<(HealResultItem, Option<Error>)>;

    /// Heal bucket using ecstore
    async fn heal_bucket(&self, bucket: &str, opts: &HealOpts) -> Result<HealResultItem>;

    /// Heal format using ecstore
    async fn heal_format(&self, dry_run: bool) -> Result<(HealResultItem, Option<Error>)>;

    /// List objects for healing
    async fn list_objects_for_heal(&self, bucket: &str, prefix: &str) -> Result<Vec<String>>;

    /// Get disk for resume functionality
    async fn get_disk_for_resume(&self, set_disk_id: &str) -> Result<DiskStore>;
}

/// ECStore Heal storage layer implementation
pub struct ECStoreHealStorage {
    ecstore: Arc<ECStore>,
}

impl ECStoreHealStorage {
    pub fn new(ecstore: Arc<ECStore>) -> Self {
        Self { ecstore }
    }
}

#[async_trait]
impl HealStorageAPI for ECStoreHealStorage {
    async fn get_object_meta(&self, bucket: &str, object: &str) -> Result<Option<rustfs_ecstore::store_api::ObjectInfo>> {
        debug!("Getting object meta: {}/{}", bucket, object);

        match self.ecstore.get_object_info(bucket, object, &Default::default()).await {
            Ok(info) => Ok(Some(info)),
            Err(e) => {
                // Map ObjectNotFound to None to align with Option return type
                if matches!(e, rustfs_ecstore::error::StorageError::ObjectNotFound(_, _)) {
                    debug!("Object meta not found: {}/{}", bucket, object);
                    Ok(None)
                } else {
                    error!("Failed to get object meta: {}/{} - {}", bucket, object, e);
                    Err(Error::other(e))
                }
            }
        }
    }

    async fn get_object_data(&self, bucket: &str, object: &str) -> Result<Option<Vec<u8>>> {
        debug!("Getting object data: {}/{}", bucket, object);

        let reader = match (*self.ecstore)
            .get_object_reader(bucket, object, None, Default::default(), &Default::default())
            .await
        {
            Ok(reader) => reader,
            Err(e) => {
                error!("Failed to get object: {}/{} - {}", bucket, object, e);
                return Err(Error::other(e));
            }
        };

        // WARNING: Returning Vec<u8> for large objects is dangerous. To avoid OOM, cap the read size.
        // If needed, refactor callers to stream instead of buffering entire object.
        const MAX_READ_BYTES: usize = 16 * 1024 * 1024; // 16 MiB cap
        let mut buf = Vec::with_capacity(1024 * 1024);
        use tokio::io::AsyncReadExt as _;
        let mut n_read: usize = 0;
        let mut stream = reader.stream;
        loop {
            // Read in chunks
            let mut chunk = vec![0u8; 1024 * 1024];
            match stream.read(&mut chunk).await {
                Ok(0) => break,
                Ok(n) => {
                    buf.extend_from_slice(&chunk[..n]);
                    n_read += n;
                    if n_read > MAX_READ_BYTES {
                        warn!(
                            "Object data exceeds cap ({} bytes), aborting full read to prevent OOM: {}/{}",
                            MAX_READ_BYTES, bucket, object
                        );
                        return Err(Error::other(format!(
                            "Object too large: {n_read} bytes (max: {MAX_READ_BYTES} bytes) for {bucket}/{object}"
                        )));
                    }
                }
                Err(e) => {
                    error!("Failed to read object data: {}/{} - {}", bucket, object, e);
                    return Err(Error::other(e));
                }
            }
        }
        Ok(Some(buf))
    }

    async fn put_object_data(&self, bucket: &str, object: &str, data: &[u8]) -> Result<()> {
        debug!("Putting object data: {}/{} ({} bytes)", bucket, object, data.len());

        let mut reader = rustfs_ecstore::store_api::PutObjReader::from_vec(data.to_vec());
        match (*self.ecstore)
            .put_object(bucket, object, &mut reader, &Default::default())
            .await
        {
            Ok(_) => {
                info!("Successfully put object: {}/{}", bucket, object);
                Ok(())
            }
            Err(e) => {
                error!("Failed to put object: {}/{} - {}", bucket, object, e);
                Err(Error::other(e))
            }
        }
    }

    async fn delete_object(&self, bucket: &str, object: &str) -> Result<()> {
        debug!("Deleting object: {}/{}", bucket, object);

        match self.ecstore.delete_object(bucket, object, Default::default()).await {
            Ok(_) => {
                info!("Successfully deleted object: {}/{}", bucket, object);
                Ok(())
            }
            Err(e) => {
                error!("Failed to delete object: {}/{} - {}", bucket, object, e);
                Err(Error::other(e))
            }
        }
    }

    async fn verify_object_integrity(&self, bucket: &str, object: &str) -> Result<bool> {
        debug!("Verifying object integrity: {}/{}", bucket, object);

        // Check object metadata first
        match self.get_object_meta(bucket, object).await? {
            Some(obj_info) => {
                if obj_info.size < 0 {
                    warn!("Object has invalid size: {}/{}", bucket, object);
                    return Ok(false);
                }

                // Stream-read the object to a sink to avoid loading into memory
                match (*self.ecstore)
                    .get_object_reader(bucket, object, None, Default::default(), &Default::default())
                    .await
                {
                    Ok(reader) => {
                        let mut stream = reader.stream;
                        match tokio::io::copy(&mut stream, &mut tokio::io::sink()).await {
                            Ok(_) => {
                                info!("Object integrity check passed: {}/{}", bucket, object);
                                Ok(true)
                            }
                            Err(e) => {
                                warn!("Object stream read failed: {}/{} - {}", bucket, object, e);
                                Ok(false)
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Failed to get object reader: {}/{} - {}", bucket, object, e);
                        Ok(false)
                    }
                }
            }
            None => {
                warn!("Object metadata not found: {}/{}", bucket, object);
                Ok(false)
            }
        }
    }

    async fn ec_decode_rebuild(&self, bucket: &str, object: &str) -> Result<Vec<u8>> {
        debug!("EC decode rebuild: {}/{}", bucket, object);

        // Use ecstore's heal_object to rebuild the object
        let heal_opts = HealOpts {
            recursive: false,
            dry_run: false,
            remove: false,
            recreate: true,
            scan_mode: HealScanMode::Deep,
            update_parity: true,
            no_lock: false,
            pool: None,
            set: None,
        };

        match self.heal_object(bucket, object, None, &heal_opts).await {
            Ok((_result, error)) => {
                if error.is_some() {
                    return Err(Error::TaskExecutionFailed {
                        message: format!("Heal failed: {error:?}"),
                    });
                }

                // After healing, try to read the object data
                match self.get_object_data(bucket, object).await? {
                    Some(data) => {
                        info!("EC decode rebuild successful: {}/{} ({} bytes)", bucket, object, data.len());
                        Ok(data)
                    }
                    None => {
                        error!("Object not found after heal: {}/{}", bucket, object);
                        Err(Error::TaskExecutionFailed {
                            message: format!("Object not found after heal: {bucket}/{object}"),
                        })
                    }
                }
            }
            Err(e) => {
                error!("Heal operation failed: {}/{} - {}", bucket, object, e);
                Err(e)
            }
        }
    }

    async fn get_disk_status(&self, endpoint: &Endpoint) -> Result<DiskStatus> {
        debug!("Getting disk status: {:?}", endpoint);

        // TODO: implement disk status check using ecstore
        // For now, return Ok status
        info!("Disk status check: {:?} - OK", endpoint);
        Ok(DiskStatus::Ok)
    }

    async fn format_disk(&self, endpoint: &Endpoint) -> Result<()> {
        debug!("Formatting disk: {:?}", endpoint);

        // Use ecstore's heal_format
        match self.heal_format(false).await {
            Ok((_, error)) => {
                if error.is_some() {
                    return Err(Error::other(format!("Format failed: {error:?}")));
                }
                info!("Successfully formatted disk: {:?}", endpoint);
                Ok(())
            }
            Err(e) => {
                error!("Failed to format disk: {:?} - {}", endpoint, e);
                Err(e)
            }
        }
    }

    async fn get_bucket_info(&self, bucket: &str) -> Result<Option<BucketInfo>> {
        debug!("Getting bucket info: {}", bucket);

        match self.ecstore.get_bucket_info(bucket, &Default::default()).await {
            Ok(info) => Ok(Some(info)),
            Err(e) => {
                error!("Failed to get bucket info: {} - {}", bucket, e);
                Err(Error::other(e))
            }
        }
    }

    async fn heal_bucket_metadata(&self, bucket: &str) -> Result<()> {
        debug!("Healing bucket metadata: {}", bucket);

        let heal_opts = HealOpts {
            recursive: true,
            dry_run: false,
            remove: false,
            recreate: false,
            scan_mode: HealScanMode::Normal,
            update_parity: false,
            no_lock: false,
            pool: None,
            set: None,
        };

        match self.heal_bucket(bucket, &heal_opts).await {
            Ok(_) => {
                info!("Successfully healed bucket metadata: {}", bucket);
                Ok(())
            }
            Err(e) => {
                error!("Failed to heal bucket metadata: {} - {}", bucket, e);
                Err(e)
            }
        }
    }

    async fn list_buckets(&self) -> Result<Vec<BucketInfo>> {
        debug!("Listing buckets");

        match self.ecstore.list_bucket(&Default::default()).await {
            Ok(buckets) => Ok(buckets),
            Err(e) => {
                error!("Failed to list buckets: {}", e);
                Err(Error::other(e))
            }
        }
    }

    async fn object_exists(&self, bucket: &str, object: &str) -> Result<bool> {
        debug!("Checking object exists: {}/{}", bucket, object);

        // Use get_object_info for efficient existence check without heavy heal operations
        match self.ecstore.get_object_info(bucket, object, &Default::default()).await {
            Ok(_) => Ok(true), // Object exists
            Err(e) => {
                // Map ObjectNotFound to false, other errors to false as well for safety
                if matches!(e, rustfs_ecstore::error::StorageError::ObjectNotFound(_, _)) {
                    debug!("Object not found: {}/{}", bucket, object);
                    Ok(false)
                } else {
                    debug!("Error checking object existence {}/{}: {}", bucket, object, e);
                    Ok(false) // Treat errors as non-existence to be safe
                }
            }
        }
    }

    async fn get_object_size(&self, bucket: &str, object: &str) -> Result<Option<u64>> {
        debug!("Getting object size: {}/{}", bucket, object);

        match self.get_object_meta(bucket, object).await {
            Ok(Some(obj_info)) => Ok(Some(obj_info.size as u64)),
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn get_object_checksum(&self, bucket: &str, object: &str) -> Result<Option<String>> {
        debug!("Getting object checksum: {}/{}", bucket, object);

        match self.get_object_meta(bucket, object).await {
            Ok(Some(obj_info)) => {
                // Convert checksum bytes to hex string
                let checksum = obj_info.checksum.iter().map(|b| format!("{b:02x}")).collect::<String>();
                Ok(Some(checksum))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn heal_object(
        &self,
        bucket: &str,
        object: &str,
        version_id: Option<&str>,
        opts: &HealOpts,
    ) -> Result<(HealResultItem, Option<Error>)> {
        debug!("Healing object: {}/{}", bucket, object);

        let version_id_str = version_id.unwrap_or("");

        match self.ecstore.heal_object(bucket, object, version_id_str, opts).await {
            Ok((result, ecstore_error)) => {
                let error = ecstore_error.map(Error::other);
                info!("Heal object completed: {}/{} - result: {:?}, error: {:?}", bucket, object, result, error);
                Ok((result, error))
            }
            Err(e) => {
                error!("Heal object failed: {}/{} - {}", bucket, object, e);
                Err(Error::other(e))
            }
        }
    }

    async fn heal_bucket(&self, bucket: &str, opts: &HealOpts) -> Result<HealResultItem> {
        debug!("Healing bucket: {}", bucket);

        match self.ecstore.heal_bucket(bucket, opts).await {
            Ok(result) => {
                info!("Heal bucket completed: {} - result: {:?}", bucket, result);
                Ok(result)
            }
            Err(e) => {
                error!("Heal bucket failed: {} - {}", bucket, e);
                Err(Error::other(e))
            }
        }
    }

    async fn heal_format(&self, dry_run: bool) -> Result<(HealResultItem, Option<Error>)> {
        debug!("Healing format (dry_run: {})", dry_run);

        match self.ecstore.heal_format(dry_run).await {
            Ok((result, ecstore_error)) => {
                let error = ecstore_error.map(Error::other);
                info!("Heal format completed - result: {:?}, error: {:?}", result, error);
                Ok((result, error))
            }
            Err(e) => {
                error!("Heal format failed: {}", e);
                Err(Error::other(e))
            }
        }
    }

    async fn list_objects_for_heal(&self, bucket: &str, prefix: &str) -> Result<Vec<String>> {
        debug!("Listing objects for heal: {}/{}", bucket, prefix);

        // Use list_objects_v2 to get objects
        match self
            .ecstore
            .clone()
            .list_objects_v2(bucket, prefix, None, None, 1000, false, None, false)
            .await
        {
            Ok(list_info) => {
                let objects: Vec<String> = list_info.objects.into_iter().map(|obj| obj.name).collect();
                info!("Found {} objects for heal in {}/{}", objects.len(), bucket, prefix);
                Ok(objects)
            }
            Err(e) => {
                error!("Failed to list objects for heal: {}/{} - {}", bucket, prefix, e);
                Err(Error::other(e))
            }
        }
    }

    async fn get_disk_for_resume(&self, set_disk_id: &str) -> Result<DiskStore> {
        debug!("Getting disk for resume: {}", set_disk_id);

        // Parse set_disk_id to extract pool and set indices
        let (pool_idx, set_idx) = crate::heal::utils::parse_set_disk_id(set_disk_id)?;

        // Get the first available disk from the set
        let disks = self
            .ecstore
            .get_disks(pool_idx, set_idx)
            .await
            .map_err(|e| Error::TaskExecutionFailed {
                message: format!("Failed to get disks for pool {pool_idx} set {set_idx}: {e}"),
            })?;

        // Find the first available disk
        if let Some(disk_store) = disks.into_iter().flatten().next() {
            info!("Found disk for resume: {:?}", disk_store);
            return Ok(disk_store);
        }

        Err(Error::TaskExecutionFailed {
            message: format!("No available disk found for set_disk_id: {set_disk_id}"),
        })
    }
}
