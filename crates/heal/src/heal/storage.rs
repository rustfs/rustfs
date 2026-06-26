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
use rustfs_madmin::heal_commands::HealResultItem;
use std::sync::Arc;
use tracing::{debug, error, warn};

use super::storage_api::storage::{
    BucketInfo, BucketOperations, DiskSetSelector, HealOperations as _, ListOperations as _, ObjectIO as _,
    ObjectOperations as _, StorageAdminApi,
};
use super::{DiskStore, ECStore, Endpoint, StorageError};
pub use super::{HealObjectInfo, HealObjectOptions, HealPutObjReader};

const LOG_COMPONENT_HEAL: &str = "heal";
const LOG_SUBSYSTEM_STORAGE: &str = "storage";
const EVENT_HEAL_STORAGE_OBJECT_IO: &str = "heal_storage_object_io";
const EVENT_HEAL_STORAGE_OBJECT_READ_LIMIT: &str = "heal_storage_object_read_limit";
const EVENT_HEAL_STORAGE_OBJECT_VERIFY: &str = "heal_storage_object_verify";
const EVENT_HEAL_STORAGE_ADMIN_OP: &str = "heal_storage_admin_op";
const EVENT_HEAL_STORAGE_REPAIR_OP: &str = "heal_storage_repair_op";

pub(crate) fn next_heal_listing_token(
    bucket: &str,
    prefix: &str,
    next_token: Option<String>,
    is_truncated: bool,
) -> Result<Option<String>> {
    if !is_truncated {
        return Ok(None);
    }

    next_token.map(Some).ok_or_else(|| Error::TaskExecutionFailed {
        message: format!("Object listing for {bucket}/{prefix} was truncated without continuation token"),
    })
}

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
    async fn get_object_meta(&self, bucket: &str, object: &str) -> Result<Option<HealObjectInfo>>;

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

    /// List objects for healing (returns all objects, may use significant memory for large buckets)
    ///
    /// WARNING: This method loads all objects into memory at once. For buckets with many objects,
    /// consider using `list_objects_for_heal_page` instead to process objects in pages.
    async fn list_objects_for_heal(&self, bucket: &str, prefix: &str) -> Result<Vec<String>>;

    /// List objects for healing with pagination (returns one page and continuation token)
    /// Returns (objects, next_continuation_token, is_truncated)
    async fn list_objects_for_heal_page(
        &self,
        bucket: &str,
        prefix: &str,
        continuation_token: Option<&str>,
    ) -> Result<(Vec<String>, Option<String>, bool)>;

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

fn is_transient_object_exists_message(message: &str) -> bool {
    let message = message.to_ascii_lowercase();

    [
        "failed to acquire read lock",
        "lock acquisition failed",
        "lock acquisition timeout",
        "quorum not reached",
        "deadline has elapsed",
        "timed out",
        "network error",
        "transport error",
        "connection refused",
    ]
    .iter()
    .any(|pattern| message.contains(pattern))
}

fn is_transient_object_exists_error(err: &StorageError) -> bool {
    if err.is_quorum_error() {
        return true;
    }

    match err {
        StorageError::Lock(lock_err) => lock_err.is_retryable() || is_transient_object_exists_message(&lock_err.to_string()),
        StorageError::Io(io_err) => is_transient_object_exists_message(&io_err.to_string()),
        StorageError::SlowDown | StorageError::OperationCanceled => true,
        _ => false,
    }
}

#[async_trait]
impl HealStorageAPI for ECStoreHealStorage {
    async fn get_object_meta(&self, bucket: &str, object: &str) -> Result<Option<HealObjectInfo>> {
        debug!(
            target: "rustfs::heal::storage",
            event = EVENT_HEAL_STORAGE_OBJECT_IO,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_STORAGE,
            operation = "get_object_meta",
            bucket,
            object,
            "Heal storage request started"
        );

        match self.ecstore.get_object_info(bucket, object, &Default::default()).await {
            Ok(info) => Ok(Some(info)),
            Err(e) => {
                // Map ObjectNotFound to None to align with Option return type
                if matches!(e, StorageError::ObjectNotFound(_, _)) {
                    debug!(
                        target: "rustfs::heal::storage",
                        event = EVENT_HEAL_STORAGE_OBJECT_IO,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_STORAGE,
                        operation = "get_object_meta",
                        bucket,
                        object,
                        result = "not_found",
                        "Heal storage object metadata missing"
                    );
                    Ok(None)
                } else {
                    error!(
                        target: "rustfs::heal::storage",
                        event = EVENT_HEAL_STORAGE_OBJECT_IO,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_STORAGE,
                        operation = "get_object_meta",
                        bucket,
                        object,
                        result = "failed",
                        error = %e,
                        "Heal storage request failed"
                    );
                    Err(Error::other(e))
                }
            }
        }
    }

    async fn get_object_data(&self, bucket: &str, object: &str) -> Result<Option<Vec<u8>>> {
        debug!(
            target: "rustfs::heal::storage",
            event = EVENT_HEAL_STORAGE_OBJECT_IO,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_STORAGE,
            operation = "get_object_data",
            bucket,
            object,
            "Heal storage request started"
        );

        let reader = match (*self.ecstore)
            .get_object_reader(bucket, object, None, Default::default(), &Default::default())
            .await
        {
            Ok(reader) => reader,
            Err(e) => {
                error!(
                    target: "rustfs::heal::storage",
                    event = EVENT_HEAL_STORAGE_OBJECT_IO,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_STORAGE,
                    operation = "get_object_data",
                    bucket,
                    object,
                    result = "failed",
                    error = %e,
                    "Heal storage request failed"
                );
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
                            target: "rustfs::heal::storage",
                            event = EVENT_HEAL_STORAGE_OBJECT_READ_LIMIT,
                            component = LOG_COMPONENT_HEAL,
                            subsystem = LOG_SUBSYSTEM_STORAGE,
                            bucket,
                            object,
                            max_read_bytes = MAX_READ_BYTES,
                            bytes_read = n_read,
                            "Heal storage aborted object read after reaching safety cap"
                        );
                        return Err(Error::other(format!(
                            "Object too large: {n_read} bytes (max: {MAX_READ_BYTES} bytes) for {bucket}/{object}"
                        )));
                    }
                }
                Err(e) => {
                    error!(
                        target: "rustfs::heal::storage",
                        event = EVENT_HEAL_STORAGE_OBJECT_IO,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_STORAGE,
                        operation = "read_object_data",
                        bucket,
                        object,
                        result = "failed",
                        error = %e,
                        "Heal storage request failed"
                    );
                    return Err(Error::other(e));
                }
            }
        }
        Ok(Some(buf))
    }

    async fn put_object_data(&self, bucket: &str, object: &str, data: &[u8]) -> Result<()> {
        debug!(
            target: "rustfs::heal::storage",
            event = EVENT_HEAL_STORAGE_OBJECT_IO,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_STORAGE,
            operation = "put_object_data",
            bucket,
            object,
            bytes = data.len(),
            "Heal storage request started"
        );

        let mut reader = HealPutObjReader::from_vec(data.to_vec());
        match (*self.ecstore)
            .put_object(bucket, object, &mut reader, &Default::default())
            .await
        {
            Ok(_) => {
                debug!(
                    target: "rustfs::heal::storage",
                    event = EVENT_HEAL_STORAGE_OBJECT_IO,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_STORAGE,
                    operation = "put_object_data",
                    bucket,
                    object,
                    result = "ok",
                    "Heal storage object write completed"
                );
                Ok(())
            }
            Err(e) => {
                error!(
                    target: "rustfs::heal::storage",
                    event = EVENT_HEAL_STORAGE_OBJECT_IO,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_STORAGE,
                    operation = "put_object_data",
                    bucket,
                    object,
                    result = "failed",
                    error = %e,
                    "Heal storage request failed"
                );
                Err(Error::other(e))
            }
        }
    }

    async fn delete_object(&self, bucket: &str, object: &str) -> Result<()> {
        debug!(
            target: "rustfs::heal::storage",
            event = EVENT_HEAL_STORAGE_OBJECT_IO,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_STORAGE,
            operation = "delete_object",
            bucket,
            object,
            "Heal storage request started"
        );

        match self.ecstore.delete_object(bucket, object, Default::default()).await {
            Ok(_) => {
                debug!(
                    target: "rustfs::heal::storage",
                    event = EVENT_HEAL_STORAGE_OBJECT_IO,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_STORAGE,
                    operation = "delete_object",
                    bucket,
                    object,
                    result = "ok",
                    "Heal storage object delete completed"
                );
                Ok(())
            }
            Err(e) => {
                error!(
                    target: "rustfs::heal::storage",
                    event = EVENT_HEAL_STORAGE_OBJECT_IO,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_STORAGE,
                    operation = "delete_object",
                    bucket,
                    object,
                    result = "failed",
                    error = %e,
                    "Heal storage request failed"
                );
                Err(Error::other(e))
            }
        }
    }

    async fn verify_object_integrity(&self, bucket: &str, object: &str) -> Result<bool> {
        debug!(
            target: "rustfs::heal::storage",
            event = EVENT_HEAL_STORAGE_OBJECT_VERIFY,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_STORAGE,
            bucket,
            object,
            state = "started",
            "Heal storage object verification started"
        );

        // Check object metadata first
        match self.get_object_meta(bucket, object).await? {
            Some(obj_info) => {
                if obj_info.size < 0 {
                    warn!(
                        target: "rustfs::heal::storage",
                        event = EVENT_HEAL_STORAGE_OBJECT_VERIFY,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_STORAGE,
                        bucket,
                        object,
                        state = "invalid_size",
                        "Heal storage object verification failed"
                    );
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
                                debug!(
                                    target: "rustfs::heal::storage",
                                    event = EVENT_HEAL_STORAGE_OBJECT_VERIFY,
                                    component = LOG_COMPONENT_HEAL,
                                    subsystem = LOG_SUBSYSTEM_STORAGE,
                                    bucket,
                                    object,
                                    state = "ok",
                                    "Heal storage object verified"
                                );
                                Ok(true)
                            }
                            Err(e) => {
                                warn!(
                                    target: "rustfs::heal::storage",
                                    event = EVENT_HEAL_STORAGE_OBJECT_VERIFY,
                                    component = LOG_COMPONENT_HEAL,
                                    subsystem = LOG_SUBSYSTEM_STORAGE,
                                    bucket,
                                    object,
                                    state = "stream_read_failed",
                                    error = %e,
                                    "Heal storage object verification failed"
                                );
                                Ok(false)
                            }
                        }
                    }
                    Err(e) => {
                        warn!(
                            target: "rustfs::heal::storage",
                            event = EVENT_HEAL_STORAGE_OBJECT_VERIFY,
                            component = LOG_COMPONENT_HEAL,
                            subsystem = LOG_SUBSYSTEM_STORAGE,
                            bucket,
                            object,
                            state = "reader_open_failed",
                            error = %e,
                            "Heal storage object verification failed"
                        );
                        Ok(false)
                    }
                }
            }
            None => {
                warn!(
                    target: "rustfs::heal::storage",
                    event = EVENT_HEAL_STORAGE_OBJECT_VERIFY,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_STORAGE,
                    bucket,
                    object,
                    state = "metadata_missing",
                    "Heal storage object verification failed"
                );
                Ok(false)
            }
        }
    }

    async fn ec_decode_rebuild(&self, bucket: &str, object: &str) -> Result<Vec<u8>> {
        debug!(
            target: "rustfs::heal::storage",
            event = EVENT_HEAL_STORAGE_REPAIR_OP,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_STORAGE,
            operation = "ec_decode_rebuild",
            bucket,
            object,
            state = "started",
            "Heal storage repair started"
        );

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
                        debug!(
                            target: "rustfs::heal::storage",
                            event = EVENT_HEAL_STORAGE_REPAIR_OP,
                            component = LOG_COMPONENT_HEAL,
                            subsystem = LOG_SUBSYSTEM_STORAGE,
                            operation = "ec_decode_rebuild",
                            bucket,
                            object,
                            bytes = data.len(),
                            state = "ok",
                            "Heal storage EC decode rebuild completed"
                        );
                        Ok(data)
                    }
                    None => {
                        error!(
                            target: "rustfs::heal::storage",
                            event = EVENT_HEAL_STORAGE_REPAIR_OP,
                            component = LOG_COMPONENT_HEAL,
                            subsystem = LOG_SUBSYSTEM_STORAGE,
                            operation = "ec_decode_rebuild",
                            bucket,
                            object,
                            state = "missing_after_heal",
                            "Heal storage repair failed"
                        );
                        Err(Error::TaskExecutionFailed {
                            message: format!("Object not found after heal: {bucket}/{object}"),
                        })
                    }
                }
            }
            Err(e) => {
                error!(
                    target: "rustfs::heal::storage",
                    event = EVENT_HEAL_STORAGE_REPAIR_OP,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_STORAGE,
                    operation = "ec_decode_rebuild",
                    bucket,
                    object,
                    state = "failed",
                    error = %e,
                    "Heal storage repair failed"
                );
                Err(e)
            }
        }
    }

    async fn get_disk_status(&self, endpoint: &Endpoint) -> Result<DiskStatus> {
        debug!(
            target: "rustfs::heal::storage",
            event = EVENT_HEAL_STORAGE_ADMIN_OP,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_STORAGE,
            operation = "get_disk_status",
            endpoint = ?endpoint,
            state = "started",
            "Heal storage admin operation started"
        );

        // TODO: implement disk status check using ecstore
        // For now, return Ok status
        debug!(
            target: "rustfs::heal::storage",
            event = EVENT_HEAL_STORAGE_ADMIN_OP,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_STORAGE,
            operation = "get_disk_status",
            endpoint = ?endpoint,
            result = "ok",
            disk_status = "ok",
            "Heal storage disk status resolved"
        );
        Ok(DiskStatus::Ok)
    }

    async fn format_disk(&self, endpoint: &Endpoint) -> Result<()> {
        debug!(
            target: "rustfs::heal::storage",
            event = EVENT_HEAL_STORAGE_ADMIN_OP,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_STORAGE,
            operation = "format_disk",
            endpoint = ?endpoint,
            state = "started",
            "Heal storage admin operation started"
        );

        // Use ecstore's heal_format
        match self.heal_format(false).await {
            Ok((_, error)) => {
                if error.is_some() {
                    return Err(Error::other(format!("Format failed: {error:?}")));
                }
                debug!(
                    target: "rustfs::heal::storage",
                    event = EVENT_HEAL_STORAGE_ADMIN_OP,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_STORAGE,
                    operation = "format_disk",
                    endpoint = ?endpoint,
                    result = "ok",
                    "Heal storage disk format completed"
                );
                Ok(())
            }
            Err(e) => {
                error!(
                    target: "rustfs::heal::storage",
                    event = EVENT_HEAL_STORAGE_ADMIN_OP,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_STORAGE,
                    operation = "format_disk",
                    endpoint = ?endpoint,
                    result = "failed",
                    error = %e,
                    "Heal storage admin operation failed"
                );
                Err(e)
            }
        }
    }

    async fn get_bucket_info(&self, bucket: &str) -> Result<Option<BucketInfo>> {
        debug!(
            target: "rustfs::heal::storage",
            event = EVENT_HEAL_STORAGE_ADMIN_OP,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_STORAGE,
            operation = "get_bucket_info",
            bucket,
            state = "started",
            "Heal storage admin operation started"
        );

        match self.ecstore.get_bucket_info(bucket, &Default::default()).await {
            Ok(info) => Ok(Some(info)),
            Err(e) => {
                error!(
                    target: "rustfs::heal::storage",
                    event = EVENT_HEAL_STORAGE_ADMIN_OP,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_STORAGE,
                    operation = "get_bucket_info",
                    bucket,
                    result = "failed",
                    error = %e,
                    "Heal storage admin operation failed"
                );
                Err(Error::other(e))
            }
        }
    }

    async fn heal_bucket_metadata(&self, bucket: &str) -> Result<()> {
        debug!(
            target: "rustfs::heal::storage",
            event = EVENT_HEAL_STORAGE_REPAIR_OP,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_STORAGE,
            operation = "heal_bucket_metadata",
            bucket,
            state = "started",
            "Heal storage repair started"
        );

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
                debug!(
                    target: "rustfs::heal::storage",
                    event = EVENT_HEAL_STORAGE_REPAIR_OP,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_STORAGE,
                    operation = "heal_bucket_metadata",
                    bucket,
                    result = "ok",
                    "Heal storage bucket metadata repaired"
                );
                Ok(())
            }
            Err(e) => {
                error!(
                    target: "rustfs::heal::storage",
                    event = EVENT_HEAL_STORAGE_REPAIR_OP,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_STORAGE,
                    operation = "heal_bucket_metadata",
                    bucket,
                    result = "failed",
                    error = %e,
                    "Heal storage repair failed"
                );
                Err(e)
            }
        }
    }

    async fn list_buckets(&self) -> Result<Vec<BucketInfo>> {
        debug!(
            target: "rustfs::heal::storage",
            event = EVENT_HEAL_STORAGE_ADMIN_OP,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_STORAGE,
            operation = "list_buckets",
            state = "started",
            "Heal storage admin operation started"
        );

        match self.ecstore.list_bucket(&Default::default()).await {
            Ok(buckets) => Ok(buckets),
            Err(e) => {
                error!(
                    target: "rustfs::heal::storage",
                    event = EVENT_HEAL_STORAGE_ADMIN_OP,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_STORAGE,
                    operation = "list_buckets",
                    result = "failed",
                    error = %e,
                    "Heal storage admin operation failed"
                );
                Err(Error::other(e))
            }
        }
    }

    async fn object_exists(&self, bucket: &str, object: &str) -> Result<bool> {
        debug!(
            target: "rustfs::heal::storage",
            event = EVENT_HEAL_STORAGE_OBJECT_IO,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_STORAGE,
            operation = "object_exists",
            bucket,
            object,
            "Heal storage request started"
        );

        // Existence checks are best-effort for background heal scheduling, so avoid
        // acquiring an extra namespace read lock here.
        let opts = HealObjectOptions {
            no_lock: true,
            ..Default::default()
        };

        match self.ecstore.get_object_info(bucket, object, &opts).await {
            Ok(_) => Ok(true), // Object exists
            Err(e) => {
                if matches!(e, StorageError::ObjectNotFound(_, _)) {
                    debug!(
                        target: "rustfs::heal::storage",
                        event = EVENT_HEAL_STORAGE_OBJECT_IO,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_STORAGE,
                        operation = "object_exists",
                        bucket,
                        object,
                        result = "not_found",
                        "Heal storage object absence confirmed"
                    );
                    Ok(false)
                } else if is_transient_object_exists_error(&e) {
                    warn!(
                        target: "rustfs::heal::storage",
                        event = EVENT_HEAL_STORAGE_OBJECT_IO,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_STORAGE,
                        operation = "object_exists",
                        bucket,
                        object,
                        result = "transient_skip",
                        error = %e,
                        "Heal storage request skipped due to transient error"
                    );
                    Err(Error::transient_skip(format!(
                        "Skipped object existence check for {bucket}/{object}: {e}"
                    )))
                } else {
                    error!(
                        target: "rustfs::heal::storage",
                        event = EVENT_HEAL_STORAGE_OBJECT_IO,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_STORAGE,
                        operation = "object_exists",
                        bucket,
                        object,
                        result = "failed",
                        error = %e,
                        "Heal storage request failed"
                    );
                    Err(Error::other(e))
                }
            }
        }
    }

    async fn get_object_size(&self, bucket: &str, object: &str) -> Result<Option<u64>> {
        debug!(
            target: "rustfs::heal::storage",
            event = EVENT_HEAL_STORAGE_OBJECT_IO,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_STORAGE,
            operation = "get_object_size",
            bucket,
            object,
            "Heal storage request started"
        );

        match self.get_object_meta(bucket, object).await {
            Ok(Some(obj_info)) => Ok(Some(obj_info.size as u64)),
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn get_object_checksum(&self, bucket: &str, object: &str) -> Result<Option<String>> {
        debug!(
            target: "rustfs::heal::storage",
            event = EVENT_HEAL_STORAGE_OBJECT_IO,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_STORAGE,
            operation = "get_object_checksum",
            bucket,
            object,
            "Heal storage request started"
        );

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
        debug!(
            target: "rustfs::heal::storage",
            event = EVENT_HEAL_STORAGE_REPAIR_OP,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_STORAGE,
            operation = "heal_object",
            bucket,
            object,
            version_id = ?version_id,
            scan_mode = %opts.scan_mode.as_str(),
            dry_run = opts.dry_run,
            state = "started",
            "Heal storage repair started"
        );

        let version_id_str = version_id.unwrap_or("");

        match self.ecstore.heal_object(bucket, object, version_id_str, opts).await {
            Ok((result, ecstore_error)) => {
                let error = ecstore_error.map(Error::Storage);
                debug!(
                    target: "rustfs::heal::storage",
                    event = EVENT_HEAL_STORAGE_REPAIR_OP,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_STORAGE,
                    operation = "heal_object",
                    bucket,
                    object,
                    version_id = ?version_id,
                    drives_after = result.after.drives.len(),
                    has_error = error.is_some(),
                    result = "ok",
                    "Heal storage object repair completed"
                );
                Ok((result, error))
            }
            Err(e) => {
                error!(
                    target: "rustfs::heal::storage",
                    event = EVENT_HEAL_STORAGE_REPAIR_OP,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_STORAGE,
                    operation = "heal_object",
                    bucket,
                    object,
                    version_id = ?version_id,
                    result = "failed",
                    error = %e,
                    "Heal storage repair failed"
                );
                Err(Error::Storage(e))
            }
        }
    }

    async fn heal_bucket(&self, bucket: &str, opts: &HealOpts) -> Result<HealResultItem> {
        debug!(
            target: "rustfs::heal::storage",
            event = EVENT_HEAL_STORAGE_REPAIR_OP,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_STORAGE,
            operation = "heal_bucket",
            bucket,
            dry_run = opts.dry_run,
            recursive = opts.recursive,
            state = "started",
            "Heal storage repair started"
        );

        match self.ecstore.heal_bucket(bucket, opts).await {
            Ok(result) => {
                debug!(
                    target: "rustfs::heal::storage",
                    event = EVENT_HEAL_STORAGE_REPAIR_OP,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_STORAGE,
                    operation = "heal_bucket",
                    bucket,
                    drives_after = result.after.drives.len(),
                    result = "ok",
                    "Heal storage bucket repair completed"
                );
                Ok(result)
            }
            Err(e) => {
                error!(
                    target: "rustfs::heal::storage",
                    event = EVENT_HEAL_STORAGE_REPAIR_OP,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_STORAGE,
                    operation = "heal_bucket",
                    bucket,
                    result = "failed",
                    error = %e,
                    "Heal storage repair failed"
                );
                Err(Error::Storage(e))
            }
        }
    }

    async fn heal_format(&self, dry_run: bool) -> Result<(HealResultItem, Option<Error>)> {
        debug!(
            target: "rustfs::heal::storage",
            event = EVENT_HEAL_STORAGE_REPAIR_OP,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_STORAGE,
            operation = "heal_format",
            dry_run,
            state = "started",
            "Heal storage repair started"
        );

        match self.ecstore.heal_format(dry_run).await {
            Ok((result, ecstore_error)) => {
                let error = ecstore_error.map(Error::Storage);
                debug!(
                    target: "rustfs::heal::storage",
                    event = EVENT_HEAL_STORAGE_REPAIR_OP,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_STORAGE,
                    operation = "heal_format",
                    drives_after = result.after.drives.len(),
                    has_error = error.is_some(),
                    result = "ok",
                    "Heal storage format repair completed"
                );
                Ok((result, error))
            }
            Err(e) => {
                error!(
                    target: "rustfs::heal::storage",
                    event = EVENT_HEAL_STORAGE_REPAIR_OP,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_STORAGE,
                    operation = "heal_format",
                    result = "failed",
                    error = %e,
                    "Heal storage repair failed"
                );
                Err(Error::Storage(e))
            }
        }
    }

    async fn list_objects_for_heal(&self, bucket: &str, prefix: &str) -> Result<Vec<String>> {
        debug!(
            target: "rustfs::heal::storage",
            event = EVENT_HEAL_STORAGE_ADMIN_OP,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_STORAGE,
            operation = "list_objects_for_heal",
            bucket,
            prefix,
            state = "started",
            "Heal storage admin operation started"
        );
        warn!(
            target: "rustfs::heal::storage",
            event = EVENT_HEAL_STORAGE_ADMIN_OP,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_STORAGE,
            operation = "list_objects_for_heal",
            bucket,
            prefix,
            state = "memory_heavy",
            "Heal storage object listing loads all objects into memory"
        );

        let mut all_objects = Vec::new();
        let mut continuation_token: Option<String> = None;

        loop {
            let (page_objects, next_token, is_truncated) = self
                .list_objects_for_heal_page(bucket, prefix, continuation_token.as_deref())
                .await?;

            all_objects.extend(page_objects);

            if !is_truncated {
                break;
            }

            continuation_token = next_heal_listing_token(bucket, prefix, next_token, is_truncated)?;
        }

        debug!(
            target: "rustfs::heal::storage",
            event = EVENT_HEAL_STORAGE_ADMIN_OP,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_STORAGE,
            operation = "list_objects_for_heal",
            bucket,
            prefix,
            object_count = all_objects.len(),
            result = "ok",
            "Heal storage object listing completed"
        );
        Ok(all_objects)
    }

    async fn list_objects_for_heal_page(
        &self,
        bucket: &str,
        prefix: &str,
        continuation_token: Option<&str>,
    ) -> Result<(Vec<String>, Option<String>, bool)> {
        debug!(
            target: "rustfs::heal::storage",
            event = EVENT_HEAL_STORAGE_ADMIN_OP,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_STORAGE,
            operation = "list_objects_for_heal_page",
            bucket,
            prefix,
            continuation_token = ?continuation_token,
            state = "started",
            "Heal storage admin operation started"
        );

        const MAX_KEYS: i32 = 1000;
        let continuation_token_opt = continuation_token.map(|s| s.to_string());

        // Use list_objects_v2 to get objects with pagination
        let list_info = match self
            .ecstore
            .clone()
            .list_objects_v2(bucket, prefix, continuation_token_opt, None, MAX_KEYS, false, None, false)
            .await
        {
            Ok(info) => info,
            Err(e) => {
                error!(
                    target: "rustfs::heal::storage",
                    event = EVENT_HEAL_STORAGE_ADMIN_OP,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_STORAGE,
                    operation = "list_objects_for_heal_page",
                    bucket,
                    prefix,
                    result = "failed",
                    error = %e,
                    "Heal storage admin operation failed"
                );
                return Err(Error::other(e));
            }
        };

        // Collect objects from this page
        let page_objects: Vec<String> = list_info.objects.into_iter().map(|obj| obj.name).collect();
        let page_count = page_objects.len();

        debug!(
            target: "rustfs::heal::storage",
            event = EVENT_HEAL_STORAGE_ADMIN_OP,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_STORAGE,
            operation = "list_objects_for_heal_page",
                        bucket,
                        prefix,
                        object_count = page_count,
                        is_truncated = list_info.is_truncated,
                        state = "page_loaded",
                        "Heal storage object listing page loaded"
        );

        Ok((page_objects, list_info.next_continuation_token, list_info.is_truncated))
    }

    async fn get_disk_for_resume(&self, set_disk_id: &str) -> Result<DiskStore> {
        debug!(
            target: "rustfs::heal::storage",
            event = EVENT_HEAL_STORAGE_ADMIN_OP,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_STORAGE,
            operation = "get_disk_for_resume",
            set_disk_id,
            state = "started",
            "Heal storage admin operation started"
        );

        // Parse set_disk_id to extract pool and set indices
        let (pool_idx, set_idx) = crate::heal::utils::parse_set_disk_id(set_disk_id)?;

        // Get the first available disk from the set
        let disks = StorageAdminApi::disk_set_inventory(self.ecstore.as_ref(), DiskSetSelector::new(pool_idx, set_idx))
            .await
            .map_err(|e| Error::TaskExecutionFailed {
                message: format!("Failed to get disks for pool {pool_idx} set {set_idx}: {e}"),
            })?;

        // Find the first available disk
        if let Some(disk_store) = disks.into_iter().flatten().next() {
            debug!(
                target: "rustfs::heal::storage",
                event = EVENT_HEAL_STORAGE_ADMIN_OP,
                component = LOG_COMPONENT_HEAL,
                subsystem = LOG_SUBSYSTEM_STORAGE,
                operation = "get_disk_for_resume",
                set_disk_id,
                result = "ok",
                disk = ?disk_store,
                "Heal storage resume disk resolved"
            );
            return Ok(disk_store);
        }

        Err(Error::TaskExecutionFailed {
            message: format!("No available disk found for set_disk_id: {set_disk_id}"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::StorageError;
    use super::{is_transient_object_exists_error, is_transient_object_exists_message, next_heal_listing_token};

    #[test]
    fn next_heal_listing_token_returns_none_for_complete_page() {
        assert_eq!(
            next_heal_listing_token("bucket", "prefix", None, false).expect("complete page should not fail"),
            None
        );
    }

    #[test]
    fn next_heal_listing_token_returns_token_for_truncated_page() {
        assert_eq!(
            next_heal_listing_token("bucket", "prefix", Some("token-1".to_string()), true)
                .expect("truncated page with token should continue"),
            Some("token-1".to_string())
        );
    }

    #[test]
    fn next_heal_listing_token_fails_for_truncated_page_without_token() {
        let err = next_heal_listing_token("bucket", "prefix", None, true).expect_err("truncated page without token must fail");

        assert!(matches!(err, super::Error::TaskExecutionFailed { .. }));
        assert!(err.to_string().contains("truncated without continuation token"));
    }

    #[test]
    fn transient_object_exists_message_matches_lock_quorum_failures() {
        assert!(is_transient_object_exists_message(
            "Failed to acquire read lock: ns_loc: read lock acquisition failed on bucket/object: Quorum not reached: required 2, achieved 0"
        ));
        assert!(is_transient_object_exists_message("deadline has elapsed"));
    }

    #[test]
    fn transient_object_exists_error_matches_quorum_variants() {
        assert!(is_transient_object_exists_error(&StorageError::ErasureReadQuorum));
        assert!(is_transient_object_exists_error(&StorageError::InsufficientReadQuorum(
            "bucket".to_string(),
            "object".to_string(),
        )));
    }

    #[test]
    fn transient_object_exists_error_does_not_treat_not_found_as_transient() {
        assert!(!is_transient_object_exists_error(&StorageError::ObjectNotFound(
            "bucket".to_string(),
            "object".to_string(),
        )));
    }
}
