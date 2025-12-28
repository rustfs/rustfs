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

//! FTPS driver implementation
//!
//! This module provides the FTPS driver that integrates with libunftp
//! and translates FTP operations to S3 actions through the gateway.

use crate::protocols::client::s3::ProtocolS3Client;
use crate::protocols::gateway::action::S3Action;
use crate::protocols::gateway::adapter::is_operation_supported;
use crate::protocols::gateway::authorize::authorize_operation;
use crate::protocols::gateway::error::map_s3_error_to_ftps;
use crate::protocols::gateway::restrictions::{get_s3_equivalent_operation, is_ftp_feature_supported};
use crate::protocols::session::context::SessionContext;
use async_trait::async_trait;
use futures::stream;
use futures_util::TryStreamExt;
use libunftp::storage::{Error, ErrorKind, Fileinfo, Metadata, Result, StorageBackend};
use s3s::dto::StreamingBlob;
use s3s::dto::{GetObjectInput, PutObjectInput};
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use tokio::io::AsyncRead;
use tokio_stream::Stream;
use tracing::{debug, error, trace};

/// FTPS storage driver implementation
#[derive(Debug)]
pub struct FtpsDriver {
}

impl FtpsDriver {
    /// Create a new FTPS driver
    pub fn new() -> Self {
        Self { }
    }

    /// Validate FTP feature support
    fn validate_feature_support(&self, feature: &str) -> Result<()> {
        if !is_ftp_feature_supported(feature) {
            let error_msg = if let Some(s3_equivalent) = get_s3_equivalent_operation(feature) {
                format!("Unsupported FTP feature: {}. S3 equivalent: {}", feature, s3_equivalent)
            } else {
                format!("Unsupported FTP feature: {}", feature)
            };
            error!("{}", error_msg);
            return Err(Error::new(ErrorKind::PermanentFileNotAvailable, error_msg));
        }
        Ok(())
    }

    /// Get SessionContext from User
    fn get_session_context_from_user(&self, user: &super::server::FtpsUser) -> Result<SessionContext> {
        Ok(user.session_context.clone())
    }

    /// Create ProtocolS3Client for the given user
    fn create_s3_client_for_user(&self, user: &super::server::FtpsUser) -> Result<ProtocolS3Client> {
        let session_context = &user.session_context;
        let fs = crate::storage::ecfs::FS {};

        let s3_client = ProtocolS3Client::new(fs, session_context.access_key().to_string());
        Ok(s3_client)
    }

    
    /// List all buckets (for root path)
    async fn list_buckets(&self, user: &super::server::FtpsUser, session_context: &SessionContext) -> Result<Vec<Fileinfo<PathBuf, FtpsMetadata>>> {
        debug!("FTPS LIST - listing all buckets for root path");

        let s3_client = self.create_s3_client_for_user(user)?;

        let action = S3Action::ListBuckets;
        if !is_operation_supported(crate::protocols::session::context::Protocol::Ftps, &action) {
            error!("FTPS LIST - ListBuckets operation not supported for FTPS protocol");
            return Err(Error::new(ErrorKind::PermanentFileNotAvailable, "Operation not supported"));
        }

        // Authorize the operation
        debug!("FTPS LIST - authorizing ListBuckets operation with empty bucket");
        match authorize_operation(session_context, &action, "", None).await {
            Ok(_) => debug!("FTPS LIST - ListBuckets authorization successful"),
            Err(e) => {
                error!("FTPS LIST - ListBuckets authorization failed: {}", e);
                return Err(Error::new(ErrorKind::PermanentFileNotAvailable, "Access denied"));
            }
        }

        let mut list_result = Vec::new();

        // List all buckets
        let input = s3s::dto::ListBucketsInput::builder()
            .build()
            .map_err(|_| Error::new(ErrorKind::PermanentFileNotAvailable, "Failed to build ListBucketsInput"))?;

        // Get the real secret key from the authenticated user
        let secret_key = &session_context.principal.user_identity.credentials.secret_key;
        debug!("FTPS LIST - calling S3 list_buckets with access_key: {}", session_context.principal.access_key());

        match s3_client.list_buckets(input, secret_key).await {
            Ok(output) => {
                debug!("FTPS LIST - S3 list_buckets succeeded, buckets count: {:?}",
                       output.buckets.as_ref().map(|b| b.len()).unwrap_or(0));
                if let Some(buckets) = output.buckets {
                    for bucket in buckets {
                        if let Some(ref bucket_name) = bucket.name {
                            debug!("FTPS LIST - found bucket: '{}'", bucket_name);

                            let metadata = FtpsMetadata {
                                size: 0,
                                is_directory: true,
                                modification_time: bucket
                                    .creation_date
                                    .map(|t| {
                                        let offset_datetime: time::OffsetDateTime = t.into();
                                        offset_datetime.unix_timestamp() as u64
                                    })
                                    .unwrap_or(0),
                            };

                            list_result.push(Fileinfo {
                                path: PathBuf::from(bucket_name),
                                metadata,
                            });
                        }
                    }
                }

                debug!("FTPS LIST - successfully listed {} buckets", list_result.len());
                Ok(list_result)
            }
            Err(e) => {
                error!("FTPS LIST - Failed to list buckets: {}", e);
                debug!("FTPS LIST - S3 list_buckets failed, error: {:?}", e);
                let protocol_error = map_s3_error_to_ftps(&e);
                Err(Error::new(ErrorKind::PermanentFileNotAvailable, protocol_error))
            }
        }
    }

    /// Create bucket
    async fn create_bucket(&self, user: &super::server::FtpsUser, session_context: &SessionContext, bucket: &str) -> Result<()> {
        debug!("FTPS CREATE_BUCKET - bucket: '{}'", bucket);

        let s3_client = self.create_s3_client_for_user(user)?;

        let action = S3Action::CreateBucket;
        if !is_operation_supported(crate::protocols::session::context::Protocol::Ftps, &action) {
            error!("FTPS CREATE_BUCKET - operation not supported for FTPS protocol");
            return Err(Error::new(ErrorKind::PermanentFileNotAvailable, "Operation not supported"));
        }

        // Authorize the operation
        debug!("FTPS CREATE_BUCKET - authorizing bucket creation");
        match authorize_operation(session_context, &action, bucket, None).await {
            Ok(_) => debug!("FTPS CREATE_BUCKET - authorization successful"),
            Err(e) => {
                error!("FTPS CREATE_BUCKET - authorization failed: {}", e);
                return Err(Error::new(ErrorKind::PermanentFileNotAvailable, "Access denied"));
            }
        }

        // Create bucket
        let mut input_builder = s3s::dto::CreateBucketInput::builder();
        input_builder.set_bucket(bucket.to_string());
        let input = input_builder
            .build()
            .map_err(|_| Error::new(ErrorKind::PermanentFileNotAvailable, "Failed to build CreateBucketInput"))?;

        match s3_client.create_bucket(input).await {
            Ok(_) => {
                debug!("FTPS CREATE_BUCKET - successfully created bucket: '{}'", bucket);
                Ok(())
            }
            Err(e) => {
                error!("FTPS CREATE_BUCKET - failed to create bucket: '{}', error: {}", bucket, e);
                let protocol_error = map_s3_error_to_ftps(&e);
                Err(Error::new(ErrorKind::PermanentFileNotAvailable, protocol_error))
            }
        }
    }

    /// Get bucket and key from path
    fn parse_path(&self, path: &str) -> Result<(String, Option<String>)> {
        debug!("FTPS parse_path - input: '{}'", path);
        let path = path.trim_start_matches('/');
        if path.is_empty() {
            error!("FTPS parse_path - empty path after trimming");
            return Err(Error::new(ErrorKind::PermanentFileNotAvailable, "Invalid path"));
        }

        let parts: Vec<&str> = path.split('/').collect();
        let bucket = parts[0].to_string();
        debug!("FTPS parse_path - bucket: '{}', parts count: {}", bucket, parts.len());

        if parts.len() == 1 {
            // Only bucket specified
            debug!("FTPS parse_path - only bucket specified: '{}'", bucket);
            Ok((bucket, None))
        } else {
            // Bucket and object key
            let key = parts[1..].join("/");
            debug!("FTPS parse_path - bucket: '{}', key: '{}'", bucket, key);
            Ok((bucket, Some(key)))
        }
    }
}

#[async_trait]
impl StorageBackend<super::server::FtpsUser> for FtpsDriver {
    type Metadata = FtpsMetadata;

    /// Get file metadata
    async fn metadata<P: AsRef<Path> + Send + Debug>(&self, user: &super::server::FtpsUser, path: P) -> Result<Self::Metadata> {
        trace!("FTPS metadata request for path: {:?}", path);

        let s3_client = self.create_s3_client_for_user(user)?;

        let path_str = path.as_ref().to_string_lossy();
        let (bucket, key) = self.parse_path(&path_str)?;

        if let Some(object_key) = key {
            // Object metadata request
            let action = S3Action::HeadObject;
            if !is_operation_supported(crate::protocols::session::context::Protocol::Ftps, &action) {
                return Err(Error::new(ErrorKind::PermanentFileNotAvailable, "Operation not supported"));
            }

            // Authorize the operation
            let session_context = self.get_session_context_from_user(user)?;
            // Log the operation for audit purposes
            debug!(
                "FTPS operation authorized: user={}, action={}, bucket={}, object={}, source_ip={}",
                session_context.access_key(),
                action.as_str(),
                bucket,
                object_key,
                session_context.source_ip
            );

            authorize_operation(&session_context, &action, &bucket, Some(&object_key))
                .await
                .map_err(|_| Error::new(ErrorKind::PermanentFileNotAvailable, "Access denied"))?;

            let mut builder = s3s::dto::HeadObjectInput::builder();
            builder.set_bucket(bucket.clone());
            builder.set_key(object_key.clone());
            let input = builder
                .build()
                .map_err(|_| Error::new(ErrorKind::PermanentFileNotAvailable, "Failed to build HeadObjectInput"))?;

            match s3_client.head_object(input).await {
                Ok(output) => {
                    let metadata = FtpsMetadata {
                        size: output.content_length.unwrap_or(0) as u64,
                        is_directory: false,
                        modification_time: output
                            .last_modified
                            .map(|t| {
                                let offset_datetime: time::OffsetDateTime = t.into();
                                offset_datetime.unix_timestamp() as u64
                            })
                            .unwrap_or(0),
                    };
                    Ok(metadata)
                }
                Err(e) => {
                    error!("Failed to get object metadata: {}", e);
                    Err(map_s3_error_to_ftps(&e))
                }
            }
        } else {
            // Bucket metadata request
            let action = S3Action::HeadBucket;
            if !is_operation_supported(crate::protocols::session::context::Protocol::Ftps, &action) {
                return Err(Error::new(ErrorKind::PermanentFileNotAvailable, "Operation not supported"));
            }

            // Authorize the operation
            let session_context = self.get_session_context_from_user(user)?;
            authorize_operation(&session_context, &action, &bucket, None)
                .await
                .map_err(|_| Error::new(ErrorKind::PermanentFileNotAvailable, "Access denied"))?;

            let mut builder = s3s::dto::HeadBucketInput::builder();
            builder.set_bucket(bucket.clone());
            let input = builder
                .build()
                .map_err(|_| Error::new(ErrorKind::PermanentFileNotAvailable, "Failed to build HeadBucketInput"))?;

            match s3_client.head_bucket(input).await {
                Ok(_) => {
                    let metadata = FtpsMetadata {
                        size: 0,
                        is_directory: true,
                        modification_time: 0,
                    };
                    Ok(metadata)
                }
                Err(e) => {
                    error!("Failed to get bucket metadata: {}", e);
                    Err(map_s3_error_to_ftps(&e))
                }
            }
        }
    }

    /// Get directory listing
    async fn list<P: AsRef<Path> + Send + Debug>(
        &self,
        user: &super::server::FtpsUser,
        path: P,
    ) -> Result<Vec<Fileinfo<PathBuf, Self::Metadata>>> {
        let path_debug = format!("{:?}", path);
        debug!("FTPS LIST request - user: {}, path: {}", user.username, path_debug);

        let s3_client = self.create_s3_client_for_user(user)?;
        let session_context = self.get_session_context_from_user(user)?;

        let path_str = path.as_ref().to_string_lossy();
        debug!("FTPS LIST - parsing path: '{}'", path_str);

        // Check if this is root path listing
        if path_str == "/" {
            debug!("FTPS LIST - root path listing, using ListBuckets");
            return self.list_buckets(user, &session_context).await;
        }

        let (bucket, prefix) = self.parse_path(&path_str)?;
        debug!("FTPS LIST - parsed bucket: '{}', prefix: {:?}", bucket, prefix);

        // Validate feature support
        self.validate_feature_support("LIST command")?;

        let action = S3Action::ListBucket;
        if !is_operation_supported(crate::protocols::session::context::Protocol::Ftps, &action) {
            return Err(Error::new(ErrorKind::PermanentFileNotAvailable, "Operation not supported"));
        }

        // Authorize the operation
        debug!("FTPS LIST - authorizing operation for bucket: '{}', prefix: {:?}", bucket, prefix);
        match authorize_operation(&session_context, &action, &bucket, prefix.as_deref()).await {
            Ok(_) => debug!("FTPS LIST - authorization successful"),
            Err(e) => {
                error!("FTPS LIST - authorization failed: {}", e);
                return Err(Error::new(ErrorKind::PermanentFileNotAvailable, "Access denied"));
            }
        }

        let mut list_result = Vec::new();

        // List objects with prefix
        let mut builder = s3s::dto::ListObjectsV2Input::builder();
        builder.set_bucket(bucket.clone());
        builder.set_prefix(prefix.clone());
        builder.set_delimiter(Option::from("/".to_string()));
        let input = builder
            .build()
            .map_err(|_| Error::new(ErrorKind::PermanentFileNotAvailable, "Failed to build ListObjectsV2Input"))?;

        match s3_client.list_objects_v2(input).await {
            Ok(output) => {
                // Add directories (common prefixes)
                if let Some(common_prefixes) = output.common_prefixes {
                    for prefix_info in common_prefixes {
                        if let Some(key) = prefix_info.prefix {
                            let dir_name = key.trim_end_matches('/').to_string();

                            let metadata = FtpsMetadata {
                                size: 0,
                                is_directory: true,
                                modification_time: 0,
                            };

                            list_result.push(Fileinfo {
                                path: PathBuf::from(dir_name),
                                metadata,
                            });
                        }
                    }
                }

                // Add files (objects)
                if let Some(contents) = output.contents {
                    for object in contents {
                        if let Some(key) = object.key {
                            let file_name = key;

                            let metadata = FtpsMetadata {
                                size: object.size.unwrap_or(0) as u64,
                                is_directory: false,
                                modification_time: object
                                    .last_modified
                                    .map(|t| {
                                        let offset_datetime: time::OffsetDateTime = t.into();
                                        offset_datetime.unix_timestamp() as u64
                                    })
                                    .unwrap_or(0),
                            };

                            list_result.push(Fileinfo {
                                path: PathBuf::from(file_name),
                                metadata,
                            });
                        }
                    }
                }

                Ok(list_result)
            }
            Err(e) => {
                error!("Failed to list objects: {}", e);
                let protocol_error = map_s3_error_to_ftps(&e);
                Err(Error::new(ErrorKind::PermanentFileNotAvailable, protocol_error))
            }
        }
    }

    /// Get file
    async fn get<P: AsRef<Path> + Send + Debug>(
        &self,
        user: &super::server::FtpsUser,
        path: P,
        start_pos: u64,
    ) -> Result<Box<dyn AsyncRead + Send + Sync + Unpin>> {
        trace!("FTPS get request for path: {:?} at position: {}", path, start_pos);

        let s3_client = self.create_s3_client_for_user(user)?;
        let session_context = self.get_session_context_from_user(user)?;

        let path_str = path.as_ref().to_string_lossy();
        let (bucket, key) = self.parse_path(&path_str)?;

        if key.is_none() {
            return Err(Error::new(ErrorKind::PermanentFileNotAvailable, "Cannot read bucket as file"));
        }

        let object_key = key.unwrap();

        let action = S3Action::GetObject;
        if !is_operation_supported(crate::protocols::session::context::Protocol::Ftps, &action) {
            return Err(Error::new(ErrorKind::PermanentFileNotAvailable, "Operation not supported"));
        }

        // Authorize the operation
        authorize_operation(&session_context, &action, &bucket, Some(&object_key))
            .await
            .map_err(|_| Error::new(ErrorKind::PermanentFileNotAvailable, "Access denied"))?;

        let mut builder = GetObjectInput::builder();
        builder.set_bucket(bucket);
        builder.set_key(object_key);
        let mut input = builder
            .build()
            .map_err(|_| Error::new(ErrorKind::PermanentFileNotAvailable, "Failed to build GetObjectInput"))?;

        if start_pos > 0 {
            input.range = Some(
                s3s::dto::Range::parse(&format!("bytes={}-", start_pos))
                    .map_err(|_| Error::new(ErrorKind::PermanentFileNotAvailable, "Invalid range format"))?,
            );
        }

        match s3_client.get_object(input).await {
            Ok(output) => {
                if let Some(body) = output.body {
                    // Map the s3s/Box<dyn StdError> error to std::io::Error
                    let stream = body.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));
                    // Wrap the stream in StreamReader to make it a tokio::io::AsyncRead
                    let reader = tokio_util::io::StreamReader::new(stream);
                    Ok(Box::new(reader))
                } else {
                    Err(Error::new(ErrorKind::PermanentFileNotAvailable, "Empty object body"))
                }
            }
            Err(e) => {
                error!("Failed to get object: {}", e);
                let protocol_error = map_s3_error_to_ftps(&e);
                Err(Error::new(ErrorKind::PermanentFileNotAvailable, protocol_error))
            }
        }
    }

    /// Put file
    async fn put<P: AsRef<Path> + Send + Debug, R: AsyncRead + Send + Sync + Unpin + 'static>(
        &self,
        user: &super::server::FtpsUser,
        input: R,
        path: P,
        start_pos: u64,
    ) -> Result<u64> {
        trace!("FTPS put request for path: {:?} at position: {}", path, start_pos);

        let s3_client = self.create_s3_client_for_user(user)?;
        let session_context = self.get_session_context_from_user(user)?;

        let path_str = path.as_ref().to_string_lossy();
        let (bucket, key) = self.parse_path(&path_str)?;

        if key.is_none() {
            return Err(Error::new(ErrorKind::PermanentFileNotAvailable, "Cannot write to bucket directly"));
        }

        let object_key = key.unwrap();

        // Check for append operation (not supported)
        if start_pos > 0 {
            self.validate_feature_support("APPE command (file append)")?;
        }

        let action = S3Action::PutObject;
        if !is_operation_supported(crate::protocols::session::context::Protocol::Ftps, &action) {
            return Err(Error::new(ErrorKind::PermanentFileNotAvailable, "Operation not supported"));
        }

        // Authorize the operation
        authorize_operation(&session_context, &action, &bucket, Some(&object_key))
            .await
            .map_err(|_| Error::new(ErrorKind::PermanentFileNotAvailable, "Access denied"))?;

        // Convert AsyncRead to bytes
        let bytes_vec = {
            let mut buffer = Vec::new();
            let mut reader = input;
            tokio::io::copy(&mut reader, &mut buffer)
                .await
                .map_err(|e| Error::new(ErrorKind::TransientFileNotAvailable, e.to_string()))?;
            buffer
        };

        let file_size = bytes_vec.len();
        debug!("FTPS put - read {} bytes from input", file_size);

        let mut put_builder = PutObjectInput::builder();
        put_builder.set_bucket(bucket.clone());
        put_builder.set_key(object_key.clone());
        put_builder.set_content_length(Some(file_size as i64));  // 设置content_length！
        // Create StreamingBlob with known size
        let data_bytes = bytes::Bytes::from(bytes_vec);
        let stream = stream::once(async move { Ok::<bytes::Bytes, std::io::Error>(data_bytes) });
        let streaming_blob = StreamingBlob::wrap(stream);
        debug!("FTPS put - StreamingBlob created, size hint: {:?}", streaming_blob.size_hint());
        debug!("FTPS put - actual data size: {} bytes", file_size);
        put_builder.set_body(Some(streaming_blob));
        let put_input = put_builder
            .build()
            .map_err(|_| Error::new(ErrorKind::PermanentFileNotAvailable, "Failed to build PutObjectInput"))?;

        debug!("FTPS put - calling S3 put_object with bucket: '{}', key: '{}', size: {} bytes", bucket, object_key, file_size);
        debug!("FTPS put - PutObjectInput body size hint: {:?}", put_input.body.as_ref().map(|b| b.size_hint()));
        match s3_client.put_object(put_input).await {
            Ok(output) => {
                debug!("Successfully put object: {:?}", output);
                // Return the size of the uploaded object
                Ok(file_size as u64)
            }
            Err(e) => {
                error!("Failed to put object: {}", e);
                error!("FTPS put - S3 error details: {:?}", e);
                let protocol_error = map_s3_error_to_ftps(&e);
                Err(Error::new(ErrorKind::PermanentFileNotAvailable, protocol_error))
            }
        }
    }

    /// Delete file
    async fn del<P: AsRef<Path> + Send + Debug>(&self, user: &super::server::FtpsUser, path: P) -> Result<()> {
        trace!("FTPS delete request for path: {:?}", path);

        let s3_client = self.create_s3_client_for_user(user)?;
        let session_context = self.get_session_context_from_user(user)?;

        let path_str = path.as_ref().to_string_lossy();
        let (bucket, key) = self.parse_path(&path_str)?;

        if key.is_none() {
            return Err(Error::new(ErrorKind::PermanentFileNotAvailable, "Cannot delete bucket"));
        }

        let object_key = key.unwrap();

        let action = S3Action::DeleteObject;
        if !is_operation_supported(crate::protocols::session::context::Protocol::Ftps, &action) {
            return Err(Error::new(ErrorKind::PermanentFileNotAvailable, "Operation not supported"));
        }

        // Authorize the operation
        authorize_operation(&session_context, &action, &bucket, Some(&object_key))
            .await
            .map_err(|_| Error::new(ErrorKind::PermanentFileNotAvailable, "Access denied"))?;

        let mut builder = s3s::dto::DeleteObjectInput::builder();
        builder.set_bucket(bucket);
        builder.set_key(object_key);
        let input = builder
            .build()
            .map_err(|_| Error::new(ErrorKind::PermanentFileNotAvailable, "Failed to build DeleteObjectInput"))?;

        match s3_client.delete_object(input).await {
            Ok(_) => {
                debug!("Successfully deleted object");
                Ok(())
            }
            Err(e) => {
                error!("Failed to delete object: {}", e);
                let protocol_error = map_s3_error_to_ftps(&e);
                Err(Error::new(ErrorKind::PermanentFileNotAvailable, protocol_error))
            }
        }
    }

    /// Create directory
    async fn mkd<P: AsRef<Path> + Send + Debug>(&self, user: &super::server::FtpsUser, path: P) -> Result<()> {
        let path_debug = format!("{:?}", path);
        debug!("FTPS MKDIR request - user: {}, path: {}", user.username, path_debug);

        let s3_client = self.create_s3_client_for_user(user)?;
        let session_context = self.get_session_context_from_user(user)?;

        let path_str = path.as_ref().to_string_lossy();
        debug!("FTPS MKDIR - parsing path: '{}'", path_str);
        let (bucket, key) = self.parse_path(&path_str)?;
        debug!("FTPS MKDIR - parsed bucket: '{}', key: {:?}", bucket, key);

        let dir_key = if let Some(k) = key {
            // Creating directory inside bucket
            if k.ends_with('/') { k } else { format!("{}/", k) }
        } else {
            // Creating bucket - use CreateBucket action instead of PutObject
            debug!("FTPS MKDIR - Creating bucket: '{}'", bucket);
            return self.create_bucket(user, &session_context, &bucket).await;
        };

        let action = S3Action::PutObject;
        if !is_operation_supported(crate::protocols::session::context::Protocol::Ftps, &action) {
            return Err(Error::new(ErrorKind::PermanentFileNotAvailable, "Operation not supported"));
        }

        // Authorize the operation
        authorize_operation(&session_context, &action, &bucket, Some(&dir_key))
            .await
            .map_err(|_| Error::new(ErrorKind::PermanentFileNotAvailable, "Access denied"))?;

        // Create directory marker object
        let mut input_builder = PutObjectInput::builder();
        input_builder.set_bucket(bucket);
        input_builder.set_key(dir_key);
        input_builder.set_body(Some(StreamingBlob::from(s3s::Body::from(Vec::new()))));
        let input = input_builder
            .build()
            .map_err(|_| Error::new(ErrorKind::PermanentFileNotAvailable, "Failed to build PutObjectInput"))?;

        match s3_client.put_object(input).await {
            Ok(_) => {
                debug!("Successfully created directory marker");
                Ok(())
            }
            Err(e) => {
                error!("Failed to create directory marker: {}", e);
                let protocol_error = map_s3_error_to_ftps(&e);
                Err(Error::new(ErrorKind::PermanentFileNotAvailable, protocol_error))
            }
        }
    }

    /// Rename file or directory
    async fn rename<P: AsRef<Path> + Send + Debug>(&self, user: &super::server::FtpsUser, from: P, to: P) -> Result<()> {
        trace!("FTPS rename request from: {:?} to: {:?}", from, to);

        let s3_client = self.create_s3_client_for_user(user)?;
        let session_context = self.get_session_context_from_user(user)?;

        // Validate feature support
        self.validate_feature_support("Atomic RNFR/RNTO rename")?;

        let from_path = from.as_ref().to_string_lossy();
        let to_path = to.as_ref().to_string_lossy();

        let (from_bucket, from_key) = self.parse_path(&from_path)?;
        let (to_bucket, to_key) = self.parse_path(&to_path)?;

        if from_key.is_none() || to_key.is_none() {
            return Err(Error::new(ErrorKind::PermanentFileNotAvailable, "Cannot rename bucket"));
        }

        let from_object_key = from_key.unwrap();
        let to_object_key = to_key.unwrap();

        // Check if this is a cross-bucket operation (not supported)
        if from_bucket != to_bucket {
            return Err(Error::new(ErrorKind::PermanentFileNotAvailable, "Cross-bucket rename not supported"));
        }

        // CopyObject operation
        let copy_action = S3Action::CopyObject;
        if !is_operation_supported(crate::protocols::session::context::Protocol::Ftps, &copy_action) {
            return Err(Error::new(ErrorKind::PermanentFileNotAvailable, "Copy operation not supported"));
        }

        // Authorize the copy operation
        authorize_operation(&session_context, &copy_action, &to_bucket, Some(&to_object_key))
            .await
            .map_err(|_| Error::new(ErrorKind::PermanentFileNotAvailable, "Access denied"))?;

        let mut copy_builder = s3s::dto::CopyObjectInput::builder();
        copy_builder.set_bucket(to_bucket.clone());
        copy_builder.set_copy_source(s3s::dto::CopySource::Bucket {
            bucket: Box::from(from_bucket.clone()),
            key: Box::from(from_object_key.clone()),
            version_id: None,
        });
        copy_builder.set_key(to_object_key.clone());
        let copy_input = copy_builder
            .build()
            .map_err(|_| Error::new(ErrorKind::PermanentFileNotAvailable, "Failed to build CopyObjectInput"))?;

        match s3_client.copy_object(copy_input).await {
            Ok(_) => {
                debug!("Successfully copied object for rename");
                // Delete original object
                let delete_action = S3Action::DeleteObject;
                if !is_operation_supported(crate::protocols::session::context::Protocol::Ftps, &delete_action) {
                    return Err(Error::new(ErrorKind::PermanentFileNotAvailable, "Delete operation not supported"));
                }

                // Authorize the delete operation
                authorize_operation(&session_context, &delete_action, &from_bucket, Some(&from_object_key))
                    .await
                    .map_err(|_| Error::new(ErrorKind::PermanentFileNotAvailable, "Access denied"))?;

                let mut builder = s3s::dto::DeleteObjectInput::builder();
                builder.set_bucket(from_bucket.to_string());
                builder.set_key(from_object_key);
                let delete_input = builder
                    .build()
                    .map_err(|_| Error::new(ErrorKind::PermanentFileNotAvailable, "Failed to build DeleteObjectInput"))?;

                match s3_client.delete_object(delete_input).await {
                    Ok(_) => {
                        debug!("Successfully deleted original object after rename");
                        Ok(())
                    }
                    Err(e) => {
                        error!("Failed to delete original object after copy: {}", e);
                        let protocol_error = map_s3_error_to_ftps(&e);
                        Err(Error::new(ErrorKind::PermanentFileNotAvailable, protocol_error))
                    }
                }
            }
            Err(e) => {
                error!("Failed to copy object for rename: {}", e);
                let protocol_error = map_s3_error_to_ftps(&e);
                Err(Error::new(ErrorKind::PermanentFileNotAvailable, protocol_error))
            }
        }
    }

    /// Remove directory
    async fn rmd<P: AsRef<Path> + Send + Debug>(&self, user: &super::server::FtpsUser, path: P) -> Result<()> {
        trace!("FTPS rmdir request for path: {:?}", path);

        let s3_client = self.create_s3_client_for_user(user)?;
        let session_context = self.get_session_context_from_user(user)?;

        let path_str = path.as_ref().to_string_lossy();
        let (bucket, key) = self.parse_path(&path_str)?;

        let dir_key = if let Some(k) = key {
            if k.ends_with('/') { k } else { format!("{}/", k) }
        } else {
            return Err(Error::new(ErrorKind::PermanentFileNotAvailable, "Cannot remove bucket"));
        };

        let action = S3Action::DeleteObject;
        if !is_operation_supported(crate::protocols::session::context::Protocol::Ftps, &action) {
            return Err(Error::new(ErrorKind::PermanentFileNotAvailable, "Operation not supported"));
        }

        // Authorize the operation
        authorize_operation(&session_context, &action, &bucket, Some(&dir_key))
            .await
            .map_err(|_| Error::new(ErrorKind::PermanentFileNotAvailable, "Access denied"))?;

        let mut builder = s3s::dto::DeleteObjectInput::builder();
        builder.set_bucket(bucket);
        builder.set_key(dir_key);
        let input = builder
            .build()
            .map_err(|_| Error::new(ErrorKind::PermanentFileNotAvailable, "Failed to build DeleteObjectInput"))?;

        match s3_client.delete_object(input).await {
            Ok(_) => {
                debug!("Successfully removed directory marker");
                Ok(())
            }
            Err(e) => {
                error!("Failed to remove directory marker: {}", e);
                let protocol_error = map_s3_error_to_ftps(&e);
                Err(Error::new(ErrorKind::PermanentFileNotAvailable, protocol_error))
            }
        }
    }

    /// Change working directory
    async fn cwd<P: AsRef<Path> + Send + Debug>(&self, user: &super::server::FtpsUser, path: P) -> Result<()> {
        debug!("FTPS cwd request for path: {:?}", path);

        let session_context = self.get_session_context_from_user(user)?;
        let path_str = path.as_ref().to_string_lossy();

        // Handle special cases
        if path_str == "/" {
            // cd to root directory - always allowed
            debug!("FTPS cwd - changing to root directory");
            return Ok(());
        }

        if path_str == ".." {
            // cd .. from root directory should fail
            error!("FTPS cwd - cannot go above root directory");
            return Err(Error::new(ErrorKind::PermanentFileNotAvailable, "Cannot go above root directory"));
        }

        // Parse the path
        let (bucket, _key) = self.parse_path(&path_str)?;
        debug!("FTPS cwd - parsed bucket: '{}', key: {:?}", bucket, _key);

        // Validate feature support
        self.validate_feature_support("CWD command")?;

        // Verify that the bucket exists by trying to list it
        let s3_client = self.create_s3_client_for_user(user)?;
        let action = S3Action::HeadBucket;
        if !is_operation_supported(crate::protocols::session::context::Protocol::Ftps, &action) {
            return Err(Error::new(ErrorKind::PermanentFileNotAvailable, "Operation not supported"));
        }

        // Authorize the operation first
        authorize_operation(&session_context, &action, &bucket, None)
            .await
            .map_err(|_| Error::new(ErrorKind::PermanentFileNotAvailable, "Access denied"))?;

        // Check if bucket actually exists
        let mut builder = s3s::dto::HeadBucketInput::builder();
        builder.set_bucket(bucket.clone());
        let input = builder
            .build()
            .map_err(|_| Error::new(ErrorKind::PermanentFileNotAvailable, "Failed to build HeadBucketInput"))?;

        match s3_client.head_bucket(input).await {
            Ok(_) => {
                debug!("FTPS cwd - bucket '{}' exists and is accessible", bucket);
                Ok(())
            }
            Err(e) => {
                error!("FTPS cwd - bucket '{}' does not exist or access denied: {}", bucket, e);
                Err(Error::new(ErrorKind::PermanentFileNotAvailable, "Directory not found"))
            }
        }
    }
}

/// FTPS metadata implementation
#[derive(Debug, Clone)]
pub struct FtpsMetadata {
    /// File size in bytes
    size: u64,
    /// Whether this is a directory
    is_directory: bool,
    /// Last modification time (Unix timestamp)
    modification_time: u64,
}

impl Metadata for FtpsMetadata {
    /// Get file size
    fn len(&self) -> u64 {
        self.size
    }

    /// Check if file is empty
    fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// Check if this is a directory
    fn is_dir(&self) -> bool {
        self.is_directory
    }

    /// Check if this is a file
    fn is_file(&self) -> bool {
        !self.is_directory
    }

    /// Check if file is a symbolic link (stub implementation)
    ///
    /// S3 doesn't support symbolic links
    fn is_symlink(&self) -> bool {
        false
    }

    /// Get last modification time
    fn modified(&self) -> Result<std::time::SystemTime> {
        Ok(std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(self.modification_time))
    }

    /// Get file permissions (stub implementation)
    fn gid(&self) -> u32 {
        0
    }

    /// Get file permissions (stub implementation)
    fn uid(&self) -> u32 {
        0
    }

    /// Get file permissions (stub implementation)
    fn links(&self) -> u64 {
        1
    }
}
