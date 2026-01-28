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
use futures_util::stream;
use libunftp::storage::{Error, ErrorKind, Fileinfo, Metadata, Result, StorageBackend};
use rustfs_utils::path;
use s3s::dto::*;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use tokio::io::AsyncRead;
use tracing::{debug, error};

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
    storage: S,
}

impl<S> Debug for FtpsDriver<S>
where
    S: S3StorageBackend + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FtpsDriver").field("storage", &"StorageBackend").finish()
    }
}

impl<S> FtpsDriver<S>
where
    S: S3StorageBackend + Debug,
{
    /// Create a new FTPS driver with the given storage backend
    pub fn new(storage: S) -> Self {
        Self { storage }
    }

    /// List all buckets (for root path)
    async fn list_buckets(
        &self,
        session_context: &crate::common::session::SessionContext,
    ) -> Result<Vec<Fileinfo<PathBuf, <FtpsDriver<S> as libunftp::storage::StorageBackend<super::server::FtpsUser>>::Metadata>>>
    {
        match authorize_operation(session_context, &S3Action::ListBuckets, "", None).await {
            Ok(_) => {}
            Err(_e) => {
                return Err(Error::new(ErrorKind::PermanentFileNotAvailable, "Access denied"));
            }
        }

        let mut list_result = Vec::new();
        match self
            .storage
            .list_buckets(
                &session_context.principal.user_identity.credentials.access_key,
                &session_context.principal.user_identity.credentials.secret_key,
            )
            .await
        {
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

    fn parse_s3_path(&self, path: &str) -> std::result::Result<(String, Option<String>), String> {
        let cleaned_path = path::clean(path);
        let (bucket, object) = path::path_to_bucket_object(&cleaned_path);
        let key = if object.is_empty() { None } else { Some(object) };

        Ok((bucket, key))
    }
}

#[async_trait]
impl<S> StorageBackend<super::server::FtpsUser> for FtpsDriver<S>
where
    S: S3StorageBackend + Debug,
{
    type Metadata = FtpsMetadata;

    async fn metadata<P: AsRef<Path> + Send>(&self, user: &super::server::FtpsUser, path: P) -> Result<Self::Metadata> {
        let path_str = path.as_ref().to_string_lossy();
        let session_context = &user.session_context;

        let (bucket, key) = self
            .parse_s3_path(&path_str)
            .map_err(|e| Error::new(ErrorKind::PermanentFileNotAvailable, format!("{}: {}", "Invalid path", e)))?;

        if let Some(key) = key {
            match self
                .storage
                .head_object(
                    &bucket,
                    &key,
                    &session_context.principal.user_identity.credentials.access_key,
                    &session_context.principal.user_identity.credentials.secret_key,
                )
                .await
            {
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
                    error!("Failed to get metadata for '{}': {}", path_str, e);
                    Err(Error::new(ErrorKind::PermanentFileNotAvailable, format!("{}: {}", "Metadata failed", e)))
                }
            }
        } else {
            // Directory metadata - use HeadBucket
            let bucket_clone = bucket.clone();
            match self
                .storage
                .head_bucket(
                    &bucket,
                    &session_context.principal.user_identity.credentials.access_key,
                    &session_context.principal.user_identity.credentials.secret_key,
                )
                .await
            {
                Ok(_) => Ok(FtpsMetadata {
                    size: 0,
                    modified: Some(std::time::SystemTime::now()),
                    is_dir: true,
                }),
                Err(e) => {
                    error!("Failed to get bucket metadata for '{}': {}", bucket_clone, e);
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

        let (bucket, prefix) = self
            .parse_s3_path(&path_str)
            .map_err(|e| Error::new(ErrorKind::PermanentFileNotAvailable, format!("{}: {}", "Invalid path", e)))?;

        // Authorize the operation
        authorize_operation(session_context, &S3Action::ListBucket, &bucket, prefix.as_deref())
            .await
            .map_err(|_| Error::new(ErrorKind::PermanentFileNotAvailable, "Access denied"))?;

        let list_input = ListObjectsV2Input::builder()
            .bucket(bucket)
            .prefix(prefix.map(|p| p.to_string()))
            .delimiter(Some("/".to_string()))
            .build()
            .map_err(|e| {
                Error::new(ErrorKind::PermanentFileNotAvailable, format!("Failed to build ListObjectsV2Input: {}", e))
            })?;

        match self
            .storage
            .list_objects_v2(
                list_input,
                &session_context.principal.user_identity.credentials.access_key,
                &session_context.principal.user_identity.credentials.secret_key,
            )
            .await
        {
            Ok(output) => {
                let mut fileinfos = Vec::new();

                // Add files (objects)
                if let Some(objects) = output.contents {
                    for obj in objects {
                        if let Some(key) = obj.key {
                            let filename = PathBuf::from(key.as_str());
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
                            let dir_name = PathBuf::from(prefix_str.as_str().trim_end_matches('/'));
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
                error!("Failed to list '{}': {}", path_str, e);
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

        let (bucket, key) = self
            .parse_s3_path(&path_str)
            .map_err(|e| Error::new(ErrorKind::PermanentFileNotAvailable, format!("{}: {}", "Invalid path", e)))?;

        let key = key.ok_or_else(|| Error::new(ErrorKind::PermanentFileNotAvailable, "Cannot get directory"))?;

        match self
            .storage
            .get_object(
                &bucket,
                &key,
                &session_context.principal.user_identity.credentials.access_key,
                &session_context.principal.user_identity.credentials.secret_key,
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
                            error!("Error reading stream: {}", e);
                            return Err(Error::new(ErrorKind::PermanentFileNotAvailable, format!("Stream error: {}", e)));
                        }
                    }
                }

                Ok(Box::new(std::io::Cursor::new(data)))
            }
            Err(e) => {
                error!("Failed to get '{}': {}", path_str, e);
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

        let (bucket, key) = self
            .parse_s3_path(&path_str)
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

        // Convert AsyncRead to bytes
        let bytes_vec = {
            let mut buffer = Vec::new();
            let mut reader = bytes;
            tokio::io::copy(&mut reader, &mut buffer)
                .await
                .map_err(|e| Error::new(ErrorKind::TransientFileNotAvailable, e.to_string()))?;
            buffer
        };

        let file_size = bytes_vec.len();

        let mut put_builder = PutObjectInput::builder();
        put_builder.set_bucket(bucket.clone());
        put_builder.set_key(key.clone());
        put_builder.set_content_length(Some(file_size as i64));

        // Create StreamingBlob with known size
        let data_bytes = bytes::Bytes::from(bytes_vec);
        let stream = stream::once(async move { Ok::<bytes::Bytes, std::io::Error>(data_bytes) });
        let streaming_blob = s3s::dto::StreamingBlob::wrap(stream);
        put_builder.set_body(Some(streaming_blob));
        let put_input = put_builder
            .build()
            .map_err(|_| Error::new(ErrorKind::PermanentFileNotAvailable, "Failed to build PutObjectInput"))?;

        match self
            .storage
            .put_object(
                put_input,
                &session_context.principal.user_identity.credentials.access_key,
                &session_context.principal.user_identity.credentials.secret_key,
            )
            .await
        {
            Ok(_output) => {
                Ok(file_size as u64) // Return the size of the uploaded object
            }
            Err(e) => {
                error!("FTPS put - S3 error details: {:?}", e);
                Err(Error::new(
                    ErrorKind::PermanentFileNotAvailable,
                    format!("Failed to upload object: {:?}", e),
                ))
            }
        }
    }

    async fn del<P: AsRef<Path> + Send>(&self, user: &super::server::FtpsUser, path: P) -> Result<()> {
        let path_str = path.as_ref().to_string_lossy();
        let session_context = &user.session_context;
        debug!("FTPS delete request for user '{}' path '{}'", user.username, path_str);

        let (bucket, key) = self
            .parse_s3_path(&path_str)
            .map_err(|e| Error::new(ErrorKind::PermanentFileNotAvailable, format!("{}: {}", "Invalid path", e)))?;

        if let Some(key) = key {
            // Delete file
            match self
                .storage
                .delete_object(
                    &bucket,
                    &key,
                    &session_context.principal.user_identity.credentials.access_key,
                    &session_context.principal.user_identity.credentials.secret_key,
                )
                .await
            {
                Ok(_) => Ok(()),
                Err(e) => {
                    error!("Failed to delete file '{}': {}", path_str, e);
                    Err(Error::new(ErrorKind::PermanentFileNotAvailable, format!("Delete failed: {}", e)))
                }
            }
        } else {
            // Delete directory (bucket) - not supported in typical FTP
            Err(Error::new(ErrorKind::PermanentFileNotAvailable, "Directory deletion not supported"))
        }
    }

    async fn mkd<P: AsRef<Path> + Send>(&self, user: &super::server::FtpsUser, path: P) -> Result<()> {
        let path_str = path.as_ref().to_string_lossy();
        let session_context = &user.session_context;
        debug!("FTPS mkdir request for user '{}' path '{}'", user.username, path_str);

        let (bucket, _key) = self
            .parse_s3_path(&path_str)
            .map_err(|e| Error::new(ErrorKind::PermanentFileNotAvailable, format!("{}: {}", "Invalid path", e)))?;

        // Create bucket for directory
        match self
            .storage
            .create_bucket(
                &bucket,
                &session_context.principal.user_identity.credentials.access_key,
                &session_context.principal.user_identity.credentials.secret_key,
            )
            .await
        {
            Ok(_) => {
                debug!("Successfully created directory/bucket '{}'", path_str);
                Ok(())
            }
            Err(e) => {
                error!("Failed to create directory/bucket '{}': {}", path_str, e);
                Err(Error::new(ErrorKind::PermanentFileNotAvailable, format!("Mkdir failed: {}", e)))
            }
        }
    }

    async fn rmd<P: AsRef<Path> + Send>(&self, user: &super::server::FtpsUser, path: P) -> Result<()> {
        let path_str = path.as_ref().to_string_lossy();
        let session_context = &user.session_context;

        let (bucket, _key) = self
            .parse_s3_path(&path_str)
            .map_err(|e| Error::new(ErrorKind::PermanentFileNotAvailable, format!("{}: {}", "Invalid path", e)))?;

        // Delete bucket for directory
        match self
            .storage
            .delete_bucket(
                &bucket,
                &session_context.principal.user_identity.credentials.access_key,
                &session_context.principal.user_identity.credentials.secret_key,
            )
            .await
        {
            Ok(_) => {
                debug!("Successfully removed directory/bucket '{}'", path_str);
                Ok(())
            }
            Err(e) => {
                error!("Failed to remove directory/bucket '{}': {}", path_str, e);
                Err(Error::new(ErrorKind::PermanentFileNotAvailable, format!("Rmdir failed: {}", e)))
            }
        }
    }

    async fn cwd<P: AsRef<Path> + Send>(&self, user: &super::server::FtpsUser, path: P) -> Result<()> {
        let path_str = path.as_ref().to_string_lossy();
        let session_context = &user.session_context;

        let (bucket, _key) = self
            .parse_s3_path(&path_str)
            .map_err(|e| Error::new(ErrorKind::PermanentFileNotAvailable, format!("{}: {}", "Invalid path", e)))?;

        // Check if bucket exists
        match self
            .storage
            .head_bucket(
                &bucket,
                &session_context.principal.user_identity.credentials.access_key,
                &session_context.principal.user_identity.credentials.secret_key,
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                error!("CWD to '{}' failed: {}", path_str, e);
                Err(Error::new(ErrorKind::PermanentFileNotAvailable, format!("CWD failed: {}", e)))
            }
        }
    }

    async fn rename<P: AsRef<Path> + Send>(&self, user: &super::server::FtpsUser, from: P, to: P) -> Result<()> {
        let from_str = from.as_ref().to_string_lossy();
        let to_str = to.as_ref().to_string_lossy();
        debug!("FTPS rename request for user '{}' from '{}' to '{}'", user.username, from_str, to_str);

        Err(Error::new(ErrorKind::PermanentFileNotAvailable, "Atomic rename not supported in S3"))
    }
}
