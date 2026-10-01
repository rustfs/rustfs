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

use crate::common::client::s3::StorageBackend as S3StorageBackend;
use crate::common::gateway::S3Action;
use crate::common::gateway::authorize_operation;
use async_trait::async_trait;
use rustfs_utils::MaskedAccessKey;
use rustfs_utils::path;
use s3s::dto::*;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::AsyncRead;
use tracing::{debug, error};
use unftp_core::storage::{Error, ErrorKind, Fileinfo, Metadata, Result, StorageBackend};

const LOG_COMPONENT_PROTOCOLS: &str = "protocols";
const LOG_SUBSYSTEM_FTPS_DRIVER: &str = "ftps_driver";
const EVENT_FTPS_BUCKET_DELETE_FAILED: &str = "ftps_bucket_delete_failed";
const EVENT_FTPS_METADATA_FAILED: &str = "ftps_metadata_failed";
const EVENT_FTPS_LIST_FAILED: &str = "ftps_list_failed";
const EVENT_FTPS_STREAM_READ_FAILED: &str = "ftps_stream_read_failed";
const EVENT_FTPS_OBJECT_GET_FAILED: &str = "ftps_object_get_failed";
const EVENT_FTPS_OBJECT_PUT_FAILED: &str = "ftps_object_put_failed";
const EVENT_FTPS_OBJECT_DELETE_STATE: &str = "ftps_object_delete_state";
const EVENT_FTPS_DIRECTORY_STATE: &str = "ftps_directory_state";
const EVENT_FTPS_CWD_FAILED: &str = "ftps_cwd_failed";
const EVENT_FTPS_RENAME_STATE: &str = "ftps_rename_state";

fn parse_s3_path(path_input: &str) -> std::result::Result<(String, Option<String>), String> {
    if path_input.chars().any(char::is_control) {
        return Err("control characters are not allowed in FTPS paths".to_string());
    }

    let cleaned_path = path::clean(path_input);
    let (bucket, object) = path::path_to_bucket_object(&cleaned_path);

    if object.contains(path::GLOBAL_DIR_SUFFIX) {
        return Err("internal directory marker is not allowed in FTPS paths".to_string());
    }

    let key = if object.is_empty() { None } else { Some(object) };
    Ok((bucket, key))
}

/// FTPS metadata implementation
#[derive(Debug, Clone)]
pub struct FtpsMetadata {
    /// File size in bytes
    pub size: u64,
    /// Modification time
    pub modified: Option<std::time::SystemTime>,
    /// Whether this is a directory
    pub is_dir: bool,
}

impl Metadata for FtpsMetadata {
    fn len(&self) -> u64 {
        self.size
    }
    fn is_dir(&self) -> bool {
        self.is_dir
    }
    fn is_file(&self) -> bool {
        !self.is_dir
    }
    fn is_symlink(&self) -> bool {
        false
    }
    fn modified(&self) -> Result<std::time::SystemTime> {
        self.modified
            .ok_or_else(|| Error::new(ErrorKind::PermanentFileNotAvailable, "No modification time available"))
    }
    fn gid(&self) -> u32 {
        0
    }
    fn uid(&self) -> u32 {
        0
    }
}

/// FTPS storage driver implementation
pub struct FtpsDriver<S> {
    /// Storage backend for S3 operations
    storage: Arc<S>,
}

impl<S> Debug for FtpsDriver<S>
where
    S: S3StorageBackend + Debug + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FtpsDriver").field("storage", &"StorageBackend").finish()
    }
}

impl<S> FtpsDriver<S>
where
    S: S3StorageBackend + Debug + 'static,
{
    /// Create a new FTPS driver with the given storage backend
    pub fn new(storage: S) -> Self {
        Self {
            storage: Arc::new(storage),
        }
    }

    /// List all buckets (for root path)
    async fn list_buckets(
        &self,
        session_context: &crate::common::session::SessionContext,
    ) -> Result<Vec<Fileinfo<PathBuf, <FtpsDriver<S> as unftp_core::storage::StorageBackend<super::server::FtpsUser>>::Metadata>>>
    {
        match authorize_operation(session_context, &S3Action::ListBuckets, "", None).await {
            Ok(_) => {}
            Err(_e) => {
                return Err(Error::new(ErrorKind::PermanentFileNotAvailable, "Access denied"));
            }
        }

        let mut list_result = Vec::new();
        match self.storage.list_buckets(session_context.credentials()).await {
            Ok(output) => {
                if let Some(buckets) = output.buckets {
                    for bucket in buckets {
                        if let Some(ref bucket_name) = bucket.name {
                            let metadata = FtpsMetadata {
                                size: 0,
                                modified: bucket.creation_date.map(|dt| {
                                    let offset_dt: time::OffsetDateTime = dt.into();
                                    std::time::SystemTime::from(offset_dt)
                                }),
                                is_dir: true,
                            };

                            list_result.push(Fileinfo {
                                path: PathBuf::from(bucket_name),
                                metadata,
                            });
                        }
                    }
                }

                Ok(list_result)
            }
            Err(_) => Err(Error::new(ErrorKind::PermanentFileNotAvailable, "List failed")),
        }
    }

    /// Recursively delete all objects in a bucket, then delete the bucket itself.
    async fn delete_bucket_recursively(
        &self,
        bucket: &str,
        session_context: &crate::common::session::SessionContext,
    ) -> Result<()> {
        // SECURITY: s3:DeleteBucket does not imply the right to destroy the
        // bucket contents. Enumerating and deleting each object are separate
        // authorization boundaries and must be cleared on their own.
        authorize_operation(session_context, &S3Action::ListBucket, bucket, None)
            .await
            .map_err(|_| Error::new(ErrorKind::PermanentFileNotAvailable, "Access denied"))?;

        // First, delete all objects in the bucket (with pagination)
        let mut continuation_token = None;
        loop {
            let mut list_input = ListObjectsV2Input::builder().bucket(bucket.to_string());

            if let Some(token) = continuation_token {
                list_input = list_input.continuation_token(token);
            }

            let list_input = list_input.build().map_err(|e| {
                Error::new(ErrorKind::PermanentFileNotAvailable, format!("Failed to build ListObjectsV2Input: {}", e))
            })?;

            if let Ok(output) = self.storage.list_objects_v2(list_input, session_context.credentials()).await {
                // Delete all objects in this page
                if let Some(objects) = output.contents {
                    for obj in objects {
                        if let Some(obj_key) = obj.key {
                            authorize_operation(session_context, &S3Action::DeleteObject, bucket, Some(&obj_key))
                                .await
                                .map_err(|_| Error::new(ErrorKind::PermanentFileNotAvailable, "Access denied"))?;

                            let _ = self
                                .storage
                                .delete_object(bucket, &obj_key, session_context.credentials())
                                .await;
                        }
                    }
                }

                // Check if there are more objects
                if !output.is_truncated.unwrap_or(false) {
                    break;
                }
                continuation_token = Some(output.next_continuation_token);
            } else {
                break;
            }
        }

        // Then delete the bucket
        match self.storage.delete_bucket(bucket, session_context.credentials()).await {
            Ok(_) => Ok(()),
            Err(e) if e.to_string().contains("NoSuchBucket") => Ok(()),
            Err(e) => {
                error!(
                    event = EVENT_FTPS_BUCKET_DELETE_FAILED,
                    component = LOG_COMPONENT_PROTOCOLS,
                    subsystem = LOG_SUBSYSTEM_FTPS_DRIVER,
                    bucket = %bucket,
                    error = %e,
                    "ftps bucket delete failed"
                );
                Err(Error::new(ErrorKind::PermanentFileNotAvailable, format!("Delete bucket failed: {}", e)))
            }
        }
    }
}

#[async_trait]
impl<S> StorageBackend<super::server::FtpsUser> for FtpsDriver<S>
where
    S: S3StorageBackend + Debug + 'static,
{
    type Metadata = FtpsMetadata;

    async fn metadata<P: AsRef<Path> + Send>(&self, user: &super::server::FtpsUser, path: P) -> Result<Self::Metadata> {
        let path_str = path.as_ref().to_string_lossy();
        let session_context = &user.session_context;

        let (bucket, key) = parse_s3_path(&path_str)
            .map_err(|e| Error::new(ErrorKind::PermanentFileNotAvailable, format!("{}: {}", "Invalid path", e)))?;

        if let Some(key) = key {
            // Authorize HeadObject
            authorize_operation(session_context, &S3Action::HeadObject, &bucket, Some(&key))
                .await
                .map_err(|_| Error::new(ErrorKind::PermanentFileNotAvailable, "Access denied"))?;

            match self.storage.head_object(&bucket, &key, session_context.credentials()).await {
                Ok(output) => {
                    let size = output.content_length.unwrap_or(0) as u64;
                    let modified = output.last_modified.map(|dt| {
                        // Convert s3s Timestamp to SystemTime
                        let offset_dt: time::OffsetDateTime = dt.into();
                        std::time::SystemTime::from(offset_dt)
                    });

                    Ok(FtpsMetadata {
                        size,
                        modified,
                        is_dir: false,
                    })
                }
                Err(e) => {
                    error!(
                        event = EVENT_FTPS_METADATA_FAILED,
                        component = LOG_COMPONENT_PROTOCOLS,
                        subsystem = LOG_SUBSYSTEM_FTPS_DRIVER,
                        path = %path_str,
                        bucket = %bucket,
                        object = %key,
                        error = %e,
                        "ftps metadata failed"
                    );
                    Err(Error::new(ErrorKind::PermanentFileNotAvailable, format!("{}: {}", "Metadata failed", e)))
                }
            }
        } else {
            // Directory metadata - use HeadBucket
            // Authorize HeadBucket
            authorize_operation(session_context, &S3Action::HeadBucket, &bucket, None)
                .await
                .map_err(|_| Error::new(ErrorKind::PermanentFileNotAvailable, "Access denied"))?;

            let bucket_clone = bucket.clone();
            match self.storage.head_bucket(&bucket, session_context.credentials()).await {
                Ok(_) => Ok(FtpsMetadata {
                    size: 0,
                    modified: Some(std::time::SystemTime::now()),
                    is_dir: true,
                }),
                Err(e) => {
                    error!(
                        event = EVENT_FTPS_METADATA_FAILED,
                        component = LOG_COMPONENT_PROTOCOLS,
                        subsystem = LOG_SUBSYSTEM_FTPS_DRIVER,
                        path = %path_str,
                        bucket = %bucket_clone,
                        error = %e,
                        "ftps metadata failed"
                    );
                    Err(Error::new(
                        ErrorKind::PermanentFileNotAvailable,
                        format!("{}: {}", "Bucket metadata failed", e),
                    ))
                }
            }
        }
    }

    async fn list<P: AsRef<Path> + Send>(
        &self,
        user: &super::server::FtpsUser,
        path: P,
    ) -> Result<Vec<Fileinfo<PathBuf, Self::Metadata>>> {
        let path_str = path.as_ref().to_string_lossy();

        // Get session context from user
        let session_context = &user.session_context;

        // Check if this is root path listing
        if path_str == "/" || path_str == "/." {
            return self.list_buckets(session_context).await;
        }

        let (bucket, prefix) = parse_s3_path(&path_str)
            .map_err(|e| Error::new(ErrorKind::PermanentFileNotAvailable, format!("{}: {}", "Invalid path", e)))?;

        // Authorize the operation
        authorize_operation(session_context, &S3Action::ListBucket, &bucket, prefix.as_deref())
            .await
            .map_err(|_| Error::new(ErrorKind::PermanentFileNotAvailable, "Access denied"))?;

        let prefix_with_slash = prefix.clone().map(|p| if p.ends_with('/') { p } else { format!("{}/", p) });

        let list_input = ListObjectsV2Input::builder()
            .bucket(bucket)
            .prefix(prefix_with_slash.clone())
            .delimiter(Some("/".to_string()))
            .build()
            .map_err(|e| {
                Error::new(ErrorKind::PermanentFileNotAvailable, format!("Failed to build ListObjectsV2Input: {}", e))
            })?;

        match self.storage.list_objects_v2(list_input, session_context.credentials()).await {
            Ok(output) => {
                let mut fileinfos = Vec::new();

                // Add files (objects)
                if let Some(objects) = output.contents {
                    for obj in objects {
                        if let Some(key) = obj.key {
                            // Filter: only show files directly in current directory
                            // Skip files in subdirectories (they should be accessed via cd)
                            let should_show = if prefix.is_none() {
                                // Root directory: only show files without "/"
                                !key.contains('/')
                            } else {
                                // Subdirectory: show files starting with prefix
                                key.starts_with(&prefix_with_slash.clone().unwrap_or_default())
                            };

                            if !should_show {
                                continue;
                            }

                            let filename = PathBuf::from(key.as_str())
                                .file_name()
                                .ok_or_else(|| {
                                    Error::new(ErrorKind::PermanentFileNotAvailable, format!("Invalid filename: {}", key))
                                })
                                .map(PathBuf::from)?;

                            let size = obj.size.unwrap_or(0) as u64;
                            let modified = obj.last_modified.map(|dt: s3s::dto::Timestamp| {
                                // Convert s3s Timestamp to SystemTime
                                let offset_dt: time::OffsetDateTime = dt.into();
                                std::time::SystemTime::from(offset_dt)
                            });

                            let metadata = FtpsMetadata {
                                size,
                                modified,
                                is_dir: false,
                            };

                            fileinfos.push(Fileinfo {
                                path: filename,
                                metadata,
                            });
                        }
                    }
                }

                // Add directories (common prefixes)
                if let Some(common_prefixes) = output.common_prefixes {
                    for prefix in common_prefixes {
                        if let Some(prefix_str) = prefix.prefix {
                            let dir_name = PathBuf::from(prefix_str.as_str().trim_end_matches('/'))
                                .file_name()
                                .ok_or_else(|| {
                                    Error::new(ErrorKind::PermanentFileNotAvailable, format!("Invalid directory: {}", prefix_str))
                                })
                                .map(PathBuf::from)?;

                            let metadata = FtpsMetadata {
                                size: 0,
                                modified: Some(std::time::SystemTime::now()),
                                is_dir: true,
                            };

                            fileinfos.push(Fileinfo {
                                path: dir_name,
                                metadata,
                            });
                        }
                    }
                }

                Ok(fileinfos)
            }
            Err(e) => {
                error!(
                    event = EVENT_FTPS_LIST_FAILED,
                    component = LOG_COMPONENT_PROTOCOLS,
                    subsystem = LOG_SUBSYSTEM_FTPS_DRIVER,
                    path = %path_str,
                    bucket = %prefix_with_slash.unwrap_or_default(),
                    error = %e,
                    "ftps list failed"
                );
                Err(Error::new(ErrorKind::PermanentFileNotAvailable, format!("{}: {}", "List failed", e)))
            }
        }
    }

    async fn get<P: AsRef<Path> + Send>(
        &self,
        user: &super::server::FtpsUser,
        path: P,
        start_pos: u64,
    ) -> Result<Box<dyn AsyncRead + Send + Sync + Unpin>> {
        let path_str = path.as_ref().to_string_lossy();
        let session_context = &user.session_context;
        let masked_username = MaskedAccessKey(&user.username);

        let (bucket, key) = parse_s3_path(&path_str)
            .map_err(|e| Error::new(ErrorKind::PermanentFileNotAvailable, format!("{}: {}", "Invalid path", e)))?;

        let key = key.ok_or_else(|| Error::new(ErrorKind::PermanentFileNotAvailable, "Cannot get directory"))?;

        // Authorize GetObject
        authorize_operation(session_context, &S3Action::GetObject, &bucket, Some(&key))
            .await
            .map_err(|_| Error::new(ErrorKind::PermanentFileNotAvailable, "Access denied"))?;

        match self
            .storage
            .get_object(
                &bucket,
                &key,
                session_context.credentials(),
                Some(start_pos), // Pass start_pos for range request
            )
            .await
        {
            Ok(output) => {
                let body = output
                    .body
                    .ok_or_else(|| Error::new(ErrorKind::PermanentFileNotAvailable, "No body in response"))?;

                use futures_util::StreamExt;
                let mut data = Vec::new();
                let mut stream = body;
                while let Some(chunk_result) = stream.next().await {
                    match chunk_result {
                        Ok(bytes) => data.extend_from_slice(&bytes),
                        Err(e) => {
                            error!(
                                event = EVENT_FTPS_STREAM_READ_FAILED,
                                component = LOG_COMPONENT_PROTOCOLS,
                                subsystem = LOG_SUBSYSTEM_FTPS_DRIVER,
                                username = %masked_username,
                                path = %path_str,
                                bucket = %bucket,
                                object = %key,
                                error = %e,
                                "ftps stream read failed"
                            );
                            return Err(Error::new(ErrorKind::PermanentFileNotAvailable, format!("Stream error: {}", e)));
                        }
                    }
                }

                Ok(Box::new(std::io::Cursor::new(data)))
            }
            Err(e) => {
                error!(
                    event = EVENT_FTPS_OBJECT_GET_FAILED,
                    component = LOG_COMPONENT_PROTOCOLS,
                    subsystem = LOG_SUBSYSTEM_FTPS_DRIVER,
                    username = %masked_username,
                    path = %path_str,
                    bucket = %bucket,
                    object = %key,
                    start_pos,
                    error = %e,
                    "ftps object get failed"
                );
                Err(Error::new(ErrorKind::PermanentFileNotAvailable, format!("{}: {}", "Get failed", e)))
            }
        }
    }

    async fn put<P: AsRef<Path> + Send + Debug, R: tokio::io::AsyncRead + Send + Sync + Unpin + 'static>(
        &self,
        user: &super::server::FtpsUser,
        bytes: R,
        path: P,
        start_pos: u64,
    ) -> Result<u64> {
        let path_str = path.as_ref().to_string_lossy();
        let session_context = &user.session_context;
        let masked_username = MaskedAccessKey(&user.username);

        let (bucket, key) = parse_s3_path(&path_str)
            .map_err(|e| Error::new(ErrorKind::PermanentFileNotAvailable, format!("{}: {}", "Invalid path", e)))?;

        let key = key.ok_or_else(|| Error::new(ErrorKind::PermanentFileNotAvailable, "Cannot put to directory"))?;

        // Check if this is an append operation (start_pos > 0)
        if start_pos > 0 {
            return Err(Error::new(
                ErrorKind::CommandNotImplemented,
                "Append operations (start_pos > 0) are not supported with S3 backend",
            ));
        }

        // Authorize the operation
        authorize_operation(session_context, &S3Action::PutObject, &bucket, Some(&key))
            .await
            .map_err(|_| Error::new(ErrorKind::PermanentFileNotAvailable, "Access denied"))?;

        upload::upload(Arc::clone(&self.storage), session_context, bytes, &bucket, &key)
            .await
            .inspect_err(|e| {
                error!(
                    event = EVENT_FTPS_OBJECT_PUT_FAILED,
                    component = LOG_COMPONENT_PROTOCOLS,
                    subsystem = LOG_SUBSYSTEM_FTPS_DRIVER,
                    username = %masked_username,
                    error = %e,
                    "ftps object put failed"
                );
            })
    }

    async fn del<P: AsRef<Path> + Send>(&self, user: &super::server::FtpsUser, path: P) -> Result<()> {
        let path_str = path.as_ref().to_string_lossy();
        let session_context = &user.session_context;
        let masked_username = MaskedAccessKey(&user.username);
        debug!(
            event = EVENT_FTPS_OBJECT_DELETE_STATE,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_FTPS_DRIVER,
            state = "requested",
            username = %masked_username,
            path = %path_str,
            "FTPS delete requested"
        );

        let (bucket, key) = parse_s3_path(&path_str)
            .map_err(|e| Error::new(ErrorKind::PermanentFileNotAvailable, format!("{}: {}", "Invalid path", e)))?;

        if let Some(key) = key {
            // Authorize delete object
            authorize_operation(session_context, &S3Action::DeleteObject, &bucket, Some(&key))
                .await
                .map_err(|_| Error::new(ErrorKind::PermanentFileNotAvailable, "Access denied"))?;

            // Delete file
            match self.storage.delete_object(&bucket, &key, session_context.credentials()).await {
                Ok(_) => Ok(()),
                Err(e) => {
                    error!(
                        event = EVENT_FTPS_OBJECT_DELETE_STATE,
                        component = LOG_COMPONENT_PROTOCOLS,
                        subsystem = LOG_SUBSYSTEM_FTPS_DRIVER,
                        state = "delete_failed",
                        username = %masked_username,
                        path = %path_str,
                        bucket = %bucket,
                        object = %key,
                        error = %e,
                        "ftps object delete state changed"
                    );
                    Err(Error::new(ErrorKind::PermanentFileNotAvailable, format!("Delete failed: {}", e)))
                }
            }
        } else {
            // Delete directory (bucket)
            // If path ends with '/', treat it as bucket deletion request
            if path_str.ends_with('/') {
                // Authorize delete bucket
                authorize_operation(session_context, &S3Action::DeleteBucket, &bucket, None)
                    .await
                    .map_err(|_| Error::new(ErrorKind::PermanentFileNotAvailable, "Access denied"))?;

                self.delete_bucket_recursively(&bucket, session_context).await
            } else {
                Err(Error::new(ErrorKind::PermanentFileNotAvailable, "Directory deletion not supported"))
            }
        }
    }

    async fn mkd<P: AsRef<Path> + Send>(&self, user: &super::server::FtpsUser, path: P) -> Result<()> {
        let path_str = path.as_ref().to_string_lossy();
        let session_context = &user.session_context;
        let masked_username = MaskedAccessKey(&user.username);
        debug!(
            event = EVENT_FTPS_DIRECTORY_STATE,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_FTPS_DRIVER,
            state = "create_requested",
            username = %masked_username,
            path = %path_str,
            "ftps directory state changed"
        );

        let (bucket, _key) = parse_s3_path(&path_str)
            .map_err(|e| Error::new(ErrorKind::PermanentFileNotAvailable, format!("{}: {}", "Invalid path", e)))?;

        // MKD creates a bucket, so it has to clear the same authorization boundary as
        // an S3 CreateBucket call.
        authorize_operation(session_context, &S3Action::CreateBucket, &bucket, None)
            .await
            .map_err(|_| Error::new(ErrorKind::PermanentFileNotAvailable, "Access denied"))?;

        // Create bucket for directory
        match self.storage.create_bucket(&bucket, session_context.credentials()).await {
            Ok(_) => {
                debug!(
                    event = EVENT_FTPS_DIRECTORY_STATE,
                    component = LOG_COMPONENT_PROTOCOLS,
                    subsystem = LOG_SUBSYSTEM_FTPS_DRIVER,
                    state = "created",
                    username = %masked_username,
                    path = %path_str,
                    bucket = %bucket,
                    "FTPS directory created"
                );
                Ok(())
            }
            Err(e) => {
                error!(
                    event = EVENT_FTPS_DIRECTORY_STATE,
                    component = LOG_COMPONENT_PROTOCOLS,
                    subsystem = LOG_SUBSYSTEM_FTPS_DRIVER,
                    state = "create_failed",
                    username = %masked_username,
                    path = %path_str,
                    bucket = %bucket,
                    error = %e,
                    "ftps directory state changed"
                );
                Err(Error::new(ErrorKind::PermanentFileNotAvailable, format!("Mkdir failed: {}", e)))
            }
        }
    }

    async fn rmd<P: AsRef<Path> + Send>(&self, user: &super::server::FtpsUser, path: P) -> Result<()> {
        let path_str = path.as_ref().to_string_lossy();
        let session_context = &user.session_context;

        let (bucket, _key) = parse_s3_path(&path_str)
            .map_err(|e| Error::new(ErrorKind::PermanentFileNotAvailable, format!("{}: {}", "Invalid path", e)))?;

        // Authorize delete bucket
        authorize_operation(session_context, &S3Action::DeleteBucket, &bucket, None)
            .await
            .map_err(|_| Error::new(ErrorKind::PermanentFileNotAvailable, "Access denied"))?;

        // Try to delete bucket recursively
        match self.delete_bucket_recursively(&bucket, session_context).await {
            Ok(_) => {
                debug!(
                    event = EVENT_FTPS_DIRECTORY_STATE,
                    component = LOG_COMPONENT_PROTOCOLS,
                    subsystem = LOG_SUBSYSTEM_FTPS_DRIVER,
                    state = "removed",
                    path = %path_str,
                    bucket = %bucket,
                    "FTPS directory removed"
                );
                Ok(())
            }
            Err(e) => {
                // Check if error is NoSuchBucket - treat as success (idempotent)
                let error_msg = e.to_string();
                if error_msg.contains("NoSuchBucket") || error_msg.contains("does not exist") {
                    debug!(
                        event = EVENT_FTPS_DIRECTORY_STATE,
                        component = LOG_COMPONENT_PROTOCOLS,
                        subsystem = LOG_SUBSYSTEM_FTPS_DRIVER,
                        state = "already_removed",
                        bucket = %bucket,
                        "FTPS directory already removed"
                    );
                    Ok(())
                } else {
                    error!(
                        event = EVENT_FTPS_DIRECTORY_STATE,
                        component = LOG_COMPONENT_PROTOCOLS,
                        subsystem = LOG_SUBSYSTEM_FTPS_DRIVER,
                        state = "remove_failed",
                        path = %path_str,
                        bucket = %bucket,
                        error = %e,
                        "ftps directory state changed"
                    );
                    Err(e)
                }
            }
        }
    }

    async fn cwd<P: AsRef<Path> + Send>(&self, user: &super::server::FtpsUser, path: P) -> Result<()> {
        let path_str = path.as_ref().to_string_lossy();
        let session_context = &user.session_context;

        let (bucket, _key) = parse_s3_path(&path_str)
            .map_err(|e| Error::new(ErrorKind::PermanentFileNotAvailable, format!("{}: {}", "Invalid path", e)))?;

        // Authorize HeadBucket (CWD probes bucket existence)
        authorize_operation(session_context, &S3Action::HeadBucket, &bucket, None)
            .await
            .map_err(|_| Error::new(ErrorKind::PermanentFileNotAvailable, "Access denied"))?;

        // Check if bucket exists
        match self.storage.head_bucket(&bucket, session_context.credentials()).await {
            Ok(_) => Ok(()),
            Err(e) => {
                error!(
                    event = EVENT_FTPS_CWD_FAILED,
                    component = LOG_COMPONENT_PROTOCOLS,
                    subsystem = LOG_SUBSYSTEM_FTPS_DRIVER,
                    path = %path_str,
                    bucket = %bucket,
                    error = %e,
                    "ftps cwd failed"
                );
                Err(Error::new(ErrorKind::PermanentFileNotAvailable, format!("CWD failed: {}", e)))
            }
        }
    }

    async fn rename<P: AsRef<Path> + Send>(&self, user: &super::server::FtpsUser, from: P, to: P) -> Result<()> {
        let from_str = from.as_ref().to_string_lossy();
        let to_str = to.as_ref().to_string_lossy();
        debug!(
            event = EVENT_FTPS_RENAME_STATE,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_FTPS_DRIVER,
            state = "unsupported",
            username = %MaskedAccessKey(&user.username),
            from = %from_str,
            to = %to_str,
            "FTPS rename unsupported"
        );

        Err(Error::new(
            ErrorKind::CommandNotImplemented,
            "Rename operation not supported in S3 backend",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::parse_s3_path;
    use rustfs_utils::path;

    /// GHSA-g3vq-vv42-f647: MKD creates a bucket, so it must clear the
    /// `s3:CreateBucket` authorization boundary before touching the backend.
    /// The queued success is what makes this a real guard — an unqueued
    /// create_bucket would fail on its own and the test would pass even if the
    /// authorization check were removed again.
    #[tokio::test]
    async fn ghsa_g3vq_mkd_denied_before_reaching_backend() {
        use super::FtpsDriver;
        use crate::common::dummy_storage::DummyBackend;
        use crate::common::gateway::with_test_auth_override;
        use crate::common::session::{Protocol, test_session};
        use unftp_core::storage::StorageBackend as _;

        let backend = DummyBackend::new();
        backend.queue_create_bucket_ok();

        let driver = FtpsDriver::new(backend);
        let user = super::super::server::FtpsUser {
            username: "denied-user".to_string(),
            name: None,
            session_context: test_session(Protocol::Ftps),
        };

        let result = with_test_auth_override(|_, _, _| false, driver.mkd(&user, "/denied-bucket")).await;

        assert!(
            result.is_err(),
            "MKD must fail closed when authorization denies s3:CreateBucket, even though the backend was primed to succeed"
        );
    }

    /// RMD deletes every object in the bucket, so `s3:DeleteBucket` alone must
    /// not be enough: each object needs its own `s3:DeleteObject` boundary. The
    /// backend is primed so that the whole recursive delete would succeed if the
    /// per-object check were removed again.
    #[tokio::test]
    async fn ftps_rmd_denied_per_object_does_not_delete_bucket_contents() {
        use super::FtpsDriver;
        use crate::common::dummy_storage::DummyBackend;
        use crate::common::gateway::{S3Action, with_test_auth_override};
        use crate::common::session::{Protocol, test_session};
        use unftp_core::storage::StorageBackend as _;

        let backend = DummyBackend::new();
        backend.queue_list_objects_v2_ok_with_keys(&["secret.txt"]);
        backend.queue_delete_object_ok();
        backend.queue_delete_bucket_ok();

        let driver = FtpsDriver::new(backend.clone());
        let user = super::super::server::FtpsUser {
            username: "bucket-only-user".to_string(),
            name: None,
            session_context: test_session(Protocol::Ftps),
        };

        let result = with_test_auth_override(
            |action, _bucket, _object| !matches!(action, S3Action::DeleteObject),
            driver.rmd(&user, "/victim-bucket"),
        )
        .await;

        assert!(result.is_err(), "RMD must fail closed when s3:DeleteObject is denied for a bucket member");
        assert!(
            backend.delete_object_calls().is_empty(),
            "no object may be deleted once s3:DeleteObject is denied, got {:?}",
            backend.delete_object_calls()
        );
        assert!(
            backend.delete_bucket_calls().is_empty(),
            "the bucket must survive when its contents could not be authorized for deletion"
        );
    }

    proptest::proptest! {
        #[test]
        fn parse_s3_path_never_leaks_control_bytes_or_traversal_in_ok_output(
            input in proptest::prelude::any::<String>(),
        ) {
            match parse_s3_path(&input) {
                Err(_) => {}
                Ok((bucket, key)) => {
                    proptest::prop_assert!(!bucket.contains('/'));
                    proptest::prop_assert!(!bucket.chars().any(char::is_control));

                    if let Some(k) = key.as_deref() {
                        proptest::prop_assert!(!k.chars().any(char::is_control));
                        proptest::prop_assert!(!k.starts_with('/'));
                        proptest::prop_assert!(!k.split('/').any(|segment| segment == ".."));
                        proptest::prop_assert!(!k.contains(path::GLOBAL_DIR_SUFFIX));
                    }
                }
            }
        }
    }

    #[test]
    fn parse_s3_path_rejects_control_bytes() {
        assert!(parse_s3_path("/bucket/line\rfeed").is_err());
        assert!(parse_s3_path("/bucket/line\nfeed").is_err());
        assert!(parse_s3_path("/bucket/tab\tname").is_err());
    }

    #[test]
    fn parse_s3_path_rejects_internal_directory_marker() {
        assert!(parse_s3_path("/bucket/__XLDIR__").is_err());
    }
}

mod upload {
    use crate::common::{
        client::s3::StorageBackend,
        gateway::{S3Action, authorize_operation},
        session::SessionContext,
    };
    use bytes::Bytes;
    use s3s::dto::*;
    use std::{
        sync::{Arc, LazyLock},
        time::Duration,
    };
    use tokio::{
        io::{AsyncRead, AsyncReadExt},
        sync::Semaphore,
    };
    use unftp_core::storage::{Error, ErrorKind, Result};

    // One in-flight part per upload; never buffer the next part while the backend consumes this one.
    const PART_SIZE: usize = 16 * 1024 * 1024;
    const MAX_PARTS: i32 = 10_000;
    const ABORT_TIMEOUT: Duration = Duration::from_secs(30);
    static ABORT_PERMITS: LazyLock<Arc<Semaphore>> = LazyLock::new(|| Arc::new(Semaphore::new(32)));
    const EVENT_FTPS_UPLOAD_CLEANUP: &str = "ftps_upload_cleanup";
    const LOG_COMPONENT_PROTOCOLS: &str = "protocols";
    const LOG_SUBSYSTEM_FTPS_UPLOAD: &str = "ftps_upload";

    fn upload_error(error: impl std::fmt::Display) -> Error {
        Error::new(ErrorKind::TransientFileNotAvailable, error.to_string())
    }

    async fn authorize(session: &SessionContext, action: S3Action, bucket: &str, key: &str) -> Result<()> {
        authorize_operation(session, &action, bucket, Some(key))
            .await
            .map_err(|_| Error::new(ErrorKind::PermanentFileNotAvailable, "Access denied"))
    }

    fn next_part_number(completed: usize) -> Result<i32> {
        let completed = i32::try_from(completed).map_err(upload_error)?;
        if completed >= MAX_PARTS {
            return Err(Error::new(
                ErrorKind::ExceededStorageAllocationError,
                "FTPS multipart upload exceeds 10000 parts",
            ));
        }
        Ok(completed + 1)
    }

    async fn read_part(reader: &mut (impl AsyncRead + Unpin)) -> Result<Vec<u8>> {
        let mut part = Vec::with_capacity(PART_SIZE);
        reader
            .take(u64::try_from(PART_SIZE).map_err(upload_error)?)
            .read_to_end(&mut part)
            .await
            .map_err(upload_error)?;
        Ok(part)
    }

    fn body(part: Vec<u8>) -> StreamingBlob {
        StreamingBlob::from_bytes(Bytes::from(part))
    }

    // Keep the upload ID alive across all cancellable awaits after CreateMultipartUpload.
    // A process/runtime crash still requires an AbortIncompleteMultipartUpload lifecycle rule.
    struct PendingUpload<S: StorageBackend + 'static> {
        storage: Arc<S>,
        session: SessionContext,
        input: Option<AbortMultipartUploadInput>,
    }

    impl<S: StorageBackend + 'static> PendingUpload<S> {
        async fn abort(&mut self) {
            if let Some(input) = self.input.as_ref() {
                abort_upload(&*self.storage, &self.session, input.clone()).await;
                self.input = None;
            }
        }
    }

    async fn abort_upload<S: StorageBackend>(storage: &S, session: &SessionContext, input: AbortMultipartUploadInput) {
        let cleanup = async {
            authorize(session, S3Action::AbortMultipartUpload, &input.bucket, &input.key).await?;
            storage
                .abort_multipart_upload(input, session.credentials())
                .await
                .map_err(upload_error)?;
            Ok::<_, Error>(())
        };
        if !matches!(tokio::time::timeout(ABORT_TIMEOUT, cleanup).await, Ok(Ok(()))) {
            tracing::warn!(
                event = EVENT_FTPS_UPLOAD_CLEANUP,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_FTPS_UPLOAD,
                result = "abort_failed",
                "FTPS incomplete upload requires lifecycle cleanup"
            );
        }
    }

    impl<S: StorageBackend + 'static> Drop for PendingUpload<S> {
        fn drop(&mut self) {
            let Some(input) = self.input.take() else {
                return;
            };
            let (Ok(runtime), Ok(permit)) =
                (tokio::runtime::Handle::try_current(), Arc::clone(&ABORT_PERMITS).try_acquire_owned())
            else {
                tracing::warn!(
                    event = EVENT_FTPS_UPLOAD_CLEANUP,
                    component = LOG_COMPONENT_PROTOCOLS,
                    subsystem = LOG_SUBSYSTEM_FTPS_UPLOAD,
                    result = "abort_unavailable",
                    "FTPS incomplete upload requires lifecycle cleanup"
                );
                return;
            };
            let storage = Arc::clone(&self.storage);
            let session = self.session.clone();
            runtime.spawn(async move {
                let _permit = permit;
                abort_upload(&*storage, &session, input).await;
            });
        }
    }

    pub(super) async fn upload<S: StorageBackend + 'static>(
        storage: Arc<S>,
        session: &SessionContext,
        mut reader: impl AsyncRead + Unpin,
        bucket: &str,
        key: &str,
    ) -> Result<u64> {
        let mut part = read_part(&mut reader).await?;
        if part.len() < PART_SIZE {
            let size = u64::try_from(part.len()).map_err(upload_error)?;
            authorize(session, S3Action::PutObject, bucket, key).await?;
            let input = PutObjectInput::builder()
                .bucket(bucket.to_owned())
                .key(key.to_owned())
                .content_length(Some(i64::try_from(size).map_err(upload_error)?))
                .body(Some(body(part)))
                .build()
                .map_err(upload_error)?;
            storage.put_object(input, session.credentials()).await.map_err(upload_error)?;
            return Ok(size);
        }

        authorize(session, S3Action::CreateMultipartUpload, bucket, key).await?;
        let input = CreateMultipartUploadInput::builder()
            .bucket(bucket.to_owned())
            .key(key.to_owned())
            .build()
            .map_err(upload_error)?;
        let output = storage
            .create_multipart_upload(input, session.credentials())
            .await
            .map_err(upload_error)?;
        let upload_id = output
            .upload_id
            .filter(|id| !id.is_empty())
            .ok_or_else(|| upload_error("CreateMultipartUpload returned no upload ID"))?;
        let abort = AbortMultipartUploadInput::builder()
            .bucket(bucket.to_owned())
            .key(key.to_owned())
            .upload_id(upload_id.clone())
            .build()
            .map_err(upload_error)?;
        let mut pending = PendingUpload {
            storage: Arc::clone(&storage),
            session: session.clone(),
            input: Some(abort),
        };

        let result = async {
            let mut parts = Vec::new();
            let mut total = 0u64;
            while !part.is_empty() {
                let number = next_part_number(parts.len())?;
                let length = i64::try_from(part.len()).map_err(upload_error)?;
                total = total
                    .checked_add(u64::try_from(part.len()).map_err(upload_error)?)
                    .ok_or_else(|| upload_error("FTPS upload size overflow"))?;
                authorize(session, S3Action::UploadPart, bucket, key).await?;
                let input = UploadPartInput::builder()
                    .bucket(bucket.to_owned())
                    .key(key.to_owned())
                    .upload_id(upload_id.clone())
                    .part_number(number)
                    .content_length(Some(length))
                    .body(Some(body(part)))
                    .build()
                    .map_err(upload_error)?;
                let output = storage
                    .upload_part(input, session.credentials())
                    .await
                    .map_err(upload_error)?;
                let etag = output.e_tag.ok_or_else(|| upload_error("UploadPart returned no ETag"))?;
                parts.push(CompletedPart {
                    e_tag: Some(etag),
                    part_number: Some(number),
                    ..Default::default()
                });
                part = read_part(&mut reader).await?;
            }
            authorize(session, S3Action::CompleteMultipartUpload, bucket, key).await?;
            let input = CompleteMultipartUploadInput::builder()
                .bucket(bucket.to_owned())
                .key(key.to_owned())
                .upload_id(upload_id)
                .multipart_upload(Some(CompletedMultipartUpload { parts: Some(parts) }))
                .build()
                .map_err(upload_error)?;
            storage
                .complete_multipart_upload(input, session.credentials())
                .await
                .map_err(upload_error)?;
            // No await between a successful commit and disarming cancellation cleanup.
            pending.input = None;
            Ok(total)
        }
        .await;
        if result.is_err() {
            pending.abort().await;
        }
        result
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::{
            common::{
                dummy_storage::{DummyBackend, DummyError},
                gateway::{is_operation_supported, with_test_auth_override},
                session::{Protocol, test_session},
            },
            ftps::{driver::FtpsDriver, server::FtpsUser},
        };
        use std::{
            pin::Pin,
            task::{Context, Poll},
        };
        use tokio::{io::ReadBuf, sync::Notify};
        use unftp_core::storage::StorageBackend as _;

        fn user() -> FtpsUser {
            FtpsUser {
                username: "upload-test".into(),
                name: None,
                session_context: test_session(Protocol::Ftps),
            }
        }

        fn multipart_backend() -> DummyBackend {
            let backend = DummyBackend::new();
            backend.queue_create_multipart_upload_ok("upload-1");
            backend.queue_complete_multipart_upload_ok();
            backend
        }

        // Refuse to produce part N+1 before part N has reached the backend. The old
        // read-to-EOF implementation fails this without allocating a huge fixture.
        struct PacedReader {
            backend: DummyBackend,
            position: usize,
            length: usize,
            fail_at_end: bool,
        }
        impl AsyncRead for PacedReader {
            fn poll_read(mut self: Pin<&mut Self>, _: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
                if self.position / PART_SIZE > self.backend.upload_part_calls().len() {
                    return Poll::Ready(Err(std::io::Error::other("read ahead of uploaded part")));
                }
                if self.position == self.length && self.fail_at_end {
                    return Poll::Ready(Err(std::io::Error::other("interrupted source")));
                }
                let count = buf.remaining().min(self.length - self.position).min(8191);
                let value = u8::try_from(self.position / PART_SIZE).expect("small test part number");
                // Do not cross a part boundary in a single read, so each part has a distinct byte.
                let count = count.min(PART_SIZE - self.position % PART_SIZE);
                buf.put_slice(&vec![value; count]);
                self.position += count;
                Poll::Ready(Ok(()))
            }
        }

        #[test]
        fn ftps_part_numbers_stop_at_s3_limit() {
            assert_eq!(next_part_number(0).expect("first part"), 1);
            assert_eq!(next_part_number(9_999).expect("last part"), 10_000);
            assert!(next_part_number(10_000).is_err());
            assert!(next_part_number(usize::MAX).is_err());
        }

        #[tokio::test]
        async fn ftps_part_buffer_does_not_grow_to_detect_eof() {
            let mut reader = tokio::io::repeat(0).take(u64::try_from(PART_SIZE).expect("size"));
            let part = read_part(&mut reader).await.expect("read full part");
            assert_eq!(part.len(), PART_SIZE);
            assert_eq!(part.capacity(), PART_SIZE);
            assert!(read_part(&mut reader).await.expect("EOF").is_empty());
        }

        #[tokio::test]
        async fn ftps_small_and_empty_uploads_preserve_bytes() {
            for payload in [Vec::new(), b"small upload\x00\xff".to_vec()] {
                let backend = DummyBackend::new();
                backend.capture_upload_bodies();
                let driver = FtpsDriver::new(backend.clone());
                let result = with_test_auth_override(
                    |_, _, _| true,
                    driver.put(&user(), std::io::Cursor::new(payload.clone()), "/bucket/key", 0),
                )
                .await
                .expect("STOR");
                assert_eq!(result, u64::try_from(payload.len()).expect("size"));
                assert_eq!(backend.upload_bodies(), vec![payload]);
                assert!(backend.create_multipart_calls().is_empty());
            }
        }

        #[tokio::test]
        async fn ftps_upload_streams_before_eof_and_preserves_part_bytes() {
            for length in [PART_SIZE - 1, PART_SIZE, PART_SIZE + 1, 2 * PART_SIZE, 2 * PART_SIZE + 7] {
                let backend = multipart_backend();
                backend.capture_upload_bodies();
                for n in 1..=3 {
                    backend.queue_upload_part_ok(format!("etag-{n}"));
                }
                let driver = FtpsDriver::new(backend.clone());
                let reader = PacedReader {
                    backend: backend.clone(),
                    position: 0,
                    length,
                    fail_at_end: false,
                };
                let size = with_test_auth_override(|_, _, _| true, driver.put(&user(), reader, "/bucket/key", 0))
                    .await
                    .expect("bounded STOR");
                assert_eq!(size, u64::try_from(length).expect("size"));
                let bodies = backend.upload_bodies();
                assert_eq!(bodies.iter().map(Vec::len).sum::<usize>(), length);
                for (index, part) in bodies.iter().enumerate() {
                    assert!(part.len() <= PART_SIZE);
                    assert!(part.iter().all(|b| *b == u8::try_from(index).expect("index")));
                }
                if length >= PART_SIZE {
                    assert_eq!(backend.complete_multipart_calls()[0].part_count, length.div_ceil(PART_SIZE));
                    for (index, call) in backend.upload_part_calls().iter().enumerate() {
                        assert_eq!(call.part_number, i32::try_from(index + 1).expect("part"));
                        assert_eq!(call.content_length, Some(i64::try_from(bodies[index].len()).expect("length")));
                    }
                }
                assert!(backend.abort_multipart_calls().is_empty());
            }
        }

        #[tokio::test]
        async fn ftps_upload_aborts_on_source_part_or_complete_failure() {
            for failure in ["source", "part", "etag", "complete"] {
                let backend = DummyBackend::new();
                backend.queue_create_multipart_upload_ok("upload-1");
                match failure {
                    "part" => backend.queue_upload_part_err(DummyError::Injected("part failed".into())),
                    "etag" => backend.queue_upload_part_ok_without_etag(),
                    _ => backend.queue_upload_part_ok("etag-1"),
                }
                backend.queue_complete_multipart_upload_err(DummyError::Injected("complete failed".into()));
                let driver = FtpsDriver::new(backend.clone());
                let reader = PacedReader {
                    backend: backend.clone(),
                    position: 0,
                    length: PART_SIZE,
                    fail_at_end: failure == "source",
                };
                let result = with_test_auth_override(|_, _, _| true, driver.put(&user(), reader, "/bucket/key", 0)).await;
                assert!(result.is_err(), "{failure} must not report a committed object");
                assert_eq!(backend.abort_multipart_calls().len(), 1, "{failure}");
                assert_eq!(backend.abort_multipart_calls()[0].upload_id, "upload-1");
                assert_eq!(backend.complete_multipart_calls().len(), usize::from(failure == "complete"));
            }
        }

        #[tokio::test]
        async fn ftps_upload_enforces_authorization_at_each_mutation() {
            for denied in [
                S3Action::PutObject,
                S3Action::CreateMultipartUpload,
                S3Action::UploadPart,
                S3Action::CompleteMultipartUpload,
            ] {
                assert!(is_operation_supported(Protocol::Ftps, &denied));
                let backend = multipart_backend();
                backend.queue_upload_part_ok("etag-1");
                let driver = FtpsDriver::new(backend.clone());
                let reader = tokio::io::repeat(0).take(u64::try_from(PART_SIZE).expect("size"));
                let denied_action = denied.clone();
                let result = with_test_auth_override(
                    move |action, _, _| action != &denied_action,
                    driver.put(&user(), reader, "/bucket/key", 0),
                )
                .await;
                assert!(result.is_err());
                match denied {
                    S3Action::PutObject | S3Action::CreateMultipartUpload => assert!(backend.create_multipart_calls().is_empty()),
                    S3Action::UploadPart => assert!(backend.upload_part_calls().is_empty()),
                    _ => assert!(backend.complete_multipart_calls().is_empty()),
                }
            }
        }

        #[tokio::test]
        async fn ftps_upload_cleanup_does_not_bypass_abort_permission() {
            let backend = multipart_backend();
            backend.queue_upload_part_err(DummyError::Injected("part failed".into()));
            let driver = FtpsDriver::new(backend.clone());
            let reader = tokio::io::repeat(0).take(u64::try_from(PART_SIZE).expect("size"));
            let result = with_test_auth_override(
                |action, _, _| action != &S3Action::AbortMultipartUpload,
                driver.put(&user(), reader, "/bucket/key", 0),
            )
            .await;
            assert!(result.is_err());
            assert!(backend.abort_multipart_calls().is_empty());
        }

        #[tokio::test]
        async fn ftps_cancelled_upload_aborts_pending_parts() {
            let backend = multipart_backend();
            let entered = Arc::new(Notify::new());
            backend.stall_upload_part(Arc::clone(&entered));
            let driver = FtpsDriver::new(backend.clone());
            let user = user();
            with_test_auth_override(|_, _, _| true, async {
                let reader = tokio::io::repeat(0).take(u64::try_from(PART_SIZE * 2).expect("size"));
                let mut put = Box::pin(driver.put(&user, reader, "/bucket/key", 0));
                tokio::select! {
                    _ = entered.notified() => {},
                    result = &mut put => panic!("upload should stall: {result:?}"),
                }
                drop(put);
                tokio::time::timeout(Duration::from_secs(5), async {
                    while backend.abort_multipart_calls().is_empty() {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("cancellation cleanup");
            })
            .await;
            assert_eq!(backend.abort_multipart_calls().len(), 1);
            assert!(backend.complete_multipart_calls().is_empty());
        }
        // Yield on each read so the memory probe keeps all upload buffers live at once.
        struct YieldingReader {
            remaining: u64,
            yielded: bool,
        }

        impl AsyncRead for YieldingReader {
            fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
                if self.remaining == 0 {
                    return Poll::Ready(Ok(()));
                }
                if !self.yielded {
                    self.yielded = true;
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                self.yielded = false;
                let count = buf
                    .remaining()
                    .min(1024 * 1024)
                    .min(usize::try_from(self.remaining).unwrap_or(usize::MAX));
                buf.initialize_unfilled_to(count).fill(0x5a);
                buf.advance(count);
                self.remaining -= u64::try_from(count).expect("read size");
                Poll::Ready(Ok(()))
            }
        }

        #[tokio::test]
        #[ignore = "manual synthetic concurrent-upload memory measurement"]
        async fn ftps_large_upload_memory_probe() {
            let bytes: u64 = std::env::var("RUSTFS_FTPS_TEST_BYTES")
                .unwrap_or_else(|_| "67108864".into())
                .parse()
                .expect("byte count");
            let concurrency: usize = std::env::var("RUSTFS_FTPS_TEST_CONCURRENCY")
                .unwrap_or_else(|_| "10".into())
                .parse()
                .expect("concurrency");
            assert!(bytes > 0 && concurrency > 0);
            let operations = (0..concurrency).map(|_| async {
                let backend = multipart_backend();
                for n in 0..bytes.div_ceil(u64::try_from(PART_SIZE).expect("part size")) {
                    backend.queue_upload_part_ok(format!("part-{n}"));
                }
                let driver = FtpsDriver::new(backend);
                let reader = YieldingReader {
                    remaining: bytes,
                    yielded: false,
                };
                driver.put(&user(), reader, "/bucket/key", 0).await
            });
            let results = with_test_auth_override(|_, _, _| true, futures_util::future::join_all(operations)).await;
            for result in results {
                assert_eq!(result.expect("upload"), bytes);
            }
            println!("synthetic_upload_bytes={bytes} concurrent_uploads={concurrency}");
        }
    }
}
