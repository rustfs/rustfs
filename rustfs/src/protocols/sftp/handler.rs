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

use crate::protocols::client::s3::ProtocolS3Client;
use crate::protocols::gateway::action::S3Action;
use crate::protocols::gateway::adapter::is_operation_supported;
use crate::protocols::gateway::authorize::authorize_operation;
use crate::protocols::gateway::error::{map_s3_error_to_protocol, S3ErrorCode};
use crate::protocols::gateway::restrictions::{get_s3_equivalent_operation, is_sftp_feature_supported};
use crate::protocols::session::context::SessionContext;
use async_trait::async_trait;
use russh_sftp::protocol::{
    Attrs, Data, File, FileAttributes, Handle, Name, OpenFlags, Packet, Status, StatusCode, Version,
};
use russh_sftp::server::Handler;
use s3s::dto::{GetObjectInput, PutObjectInput};
use s3s::dto::StreamingBlob;
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, RwLock};
use futures_util::TryStreamExt;
// Used for shared state across async boundaries
use tracing::{debug, error, trace, warn};

/// SFTP session handle
#[derive(Debug, Clone)]
struct SftpHandle {
    /// Handle ID
    id: u32,
    /// File path
    path: String,
    /// File attributes
    attrs: FileAttributes,
    /// Current position for read/write operations
    position: u64,
}

/// SFTP session context
#[derive(Debug, Clone)]
pub struct SftpSessionContext {
    /// Session context for this SFTP session
    pub session_context: SessionContext,
}

/// Internal state for SftpHandler to allow shared access via Arc/RwLock
#[derive(Debug)]
struct SftpHandlerInner {
    /// Session context
    session_context: Option<SftpSessionContext>,
    /// Active file handles
    handles: HashMap<String, SftpHandle>,
    /// Next handle ID
    next_handle_id: u32,
}

/// SFTP handler implementation
#[derive(Debug, Clone)]
pub struct SftpHandler {
    inner: Arc<RwLock<SftpHandlerInner>>,
}

impl SftpHandler {
    /// Create a new SFTP handler
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(SftpHandlerInner {
                session_context: None,
                handles: HashMap::new(),
                next_handle_id: 1,
            })),
        }
    }

    /// Set session context
    pub fn set_session_context(&mut self, context: SftpSessionContext) {
        if let Ok(mut inner) = self.inner.write() {
            inner.session_context = Some(context);
        } else {
            error!("Failed to acquire write lock for setting session context");
        }
    }

    /// Get session context
    fn get_session_context(&self) -> Result<SftpSessionContext, StatusCode> {
        let inner = self.inner.read().map_err(|_| StatusCode::Failure)?;
        inner.session_context.clone().ok_or(StatusCode::PermissionDenied)
    }

    /// Create ProtocolS3Client for current session
    ///
    /// MINIO CONSTRAINT: Must use the same capabilities as external S3 clients
    fn create_s3_client(&self) -> Result<ProtocolS3Client, StatusCode> {
        let context = self.get_session_context()?;
        let fs = crate::storage::ecfs::FS {};
        let s3_client = ProtocolS3Client::new(fs, context.session_context.access_key().to_string());
        Ok(s3_client)
    }

    /// Validate SFTP feature support
    ///
    /// MINIO CONSTRAINT: Must reject unsupported SFTP features
    fn validate_feature_support(&self, feature: &str) -> Result<(), StatusCode> {
        if !is_sftp_feature_supported(feature) {
            let error_msg = if let Some(s3_equivalent) = get_s3_equivalent_operation(feature) {
                format!("Unsupported SFTP feature: {}. S3 equivalent: {}", feature, s3_equivalent)
            } else {
                format!("Unsupported SFTP feature: {}", feature)
            };
            error!("{}", error_msg);
            return Err(StatusCode::Failure);
        }
        Ok(())
    }

    /// Get bucket and key from path
    fn parse_path(&self, path: &str) -> Result<(String, Option<String>), StatusCode> {
        let path = path.trim_start_matches('/');
        if path.is_empty() {
            return Err(StatusCode::NoSuchFile);
        }

        let parts: Vec<&str> = path.split('/').collect();
        let bucket = parts[0].to_string();

        if parts.len() == 1 {
            // Only bucket specified
            Ok((bucket, None))
        } else {
            // Bucket and object key
            let key = parts[1..].join("/");
            Ok((bucket, Some(key)))
        }
    }

    /// Create a new handle for a file
    fn create_handle(&self, path: String, attrs: FileAttributes) -> Result<String, StatusCode> {
        let mut inner = self.inner.write().map_err(|_| StatusCode::Failure)?;

        let handle_id = inner.next_handle_id;
        inner.next_handle_id += 1;

        let handle_name = format!("handle_{}", handle_id);

        let sftp_handle = SftpHandle {
            id: handle_id,
            path,
            attrs,
            position: 0,
        };

        inner.handles.insert(handle_name.clone(), sftp_handle);
        Ok(handle_name)
    }

    /// Get handle path by name (cloned to release lock)
    fn get_handle_info(&self, handle: &str) -> Option<(String, u64)> {
        let inner = self.inner.read().ok()?;
        inner.handles.get(handle).map(|h| (h.path.clone(), h.position))
    }

    /// Remove handle by name
    fn remove_handle(&self, handle: &str) {
        if let Ok(mut inner) = self.inner.write() {
            inner.handles.remove(handle);
        }
    }
}

#[async_trait]
impl Handler for SftpHandler {
    type Error = StatusCode;

    fn unimplemented(&self) -> Self::Error {
        StatusCode::OpUnsupported
    }

    /// Handle version negotiation
    fn init(
        &mut self,
        version: u32,
        _extensions: HashMap<String, String>,
    ) -> impl Future<Output = Result<Version, Self::Error>> + Send {
        trace!("SFTP init request with version: {}", version);
        async move {
            Ok(Version::new())
        }
    }

    /// Handle file open request
    fn open(
        &mut self,
        id: u32,
        filename: String,
        flags: OpenFlags,
        attrs: FileAttributes,
    ) -> impl Future<Output = Result<Handle, Self::Error>> + Send {
        // Clone self to move into async block
        let this = self.clone();

        Box::pin(async move {
            trace!("SFTP open request for file: {} with flags: {:?}, attrs: {:?}", filename, flags, attrs);

            // We store the handle in our map
            let handle_name = this.create_handle(filename.clone(), attrs)?;

            Ok(Handle { id, handle: handle_name })
        })
    }

    /// Handle file close request
    fn close(&mut self, id: u32, handle: String) -> impl Future<Output = Result<Status, Self::Error>> + Send {
        let this = self.clone();
        Box::pin(async move {
            trace!("SFTP close request for handle: {}", handle);

            this.remove_handle(&handle);
            debug!("Successfully closed handle: {}", handle);

            Ok(Status {
                id,
                status_code: StatusCode::Ok,
                error_message: "Success".to_string(),
                language_tag: "en".to_string(),
            })
        })
    }

    /// Handle file read request
    fn read(
        &mut self,
        id: u32,
        handle: String,
        offset: u64,
        len: u32,
    ) -> impl Future<Output = Result<Data, Self::Error>> + Send {
        let this = self.clone();
        Box::pin(async move {
            trace!("SFTP read request for handle: {} at offset: {} len: {}", handle, offset, len);

            let (path, _) = this.get_handle_info(&handle).ok_or(StatusCode::NoSuchFile)?;

            let (bucket, key) = this.parse_path(&path)?;
            let object_key = key.ok_or(StatusCode::NoSuchFile)?;

            let action = S3Action::GetObject;
            if !is_operation_supported(crate::protocols::session::context::Protocol::Sftp, &action) {
                return Err(StatusCode::OpUnsupported);
            }

            // Authorize
            let context = this.get_session_context()?;
            authorize_operation(
                &context.session_context,
                &action,
                &bucket,
                Some(&object_key),
            ).await.map_err(|_| StatusCode::PermissionDenied)?;

            // Construct S3 request with Range header
            let mut builder = GetObjectInput::builder();
            builder.set_bucket(bucket);
            builder.set_key(object_key);

            // S3 Range is inclusive. e.g. bytes=0-9 gets 10 bytes.
            let range_end = offset + (len as u64) - 1;
            let range_str = format!("bytes={}-{}", offset, range_end);

            if let Ok(range) = s3s::dto::Range::parse(&range_str) {
                builder.set_range(Option::from(range));
            }

            let input = builder.build().map_err(|_| StatusCode::Failure)?;
            let s3_client = this.create_s3_client()?;

            match s3_client.get_object(input).await {
                Ok(output) => {
                    let mut data_vec = Vec::new();
                    if let Some(body) = output.body {
                        use tokio::io::AsyncReadExt;
                        // Convert the S3 stream to an async reader
                        let stream = body.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));
                        let mut reader = tokio_util::io::StreamReader::new(stream);
                        // Read into vector
                        reader.read_to_end(&mut data_vec).await.map_err(|_| StatusCode::Failure)?;
                    }

                    Ok(Data {
                        id,
                        data: data_vec,
                    })
                }
                Err(e) => {
                    error!("Failed to read object: {}", e);
                    Err(StatusCode::Failure)
                }
            }
        })
    }

    /// Handle file write request
    ///
    /// MINIO CONSTRAINT: Must map to S3 PutObject operation
    fn write(
        &mut self,
        id: u32,
        handle: String,
        offset: u64,
        data: Vec<u8>,
    ) -> impl Future<Output = Result<Status, Self::Error>> + Send {
        let this = self.clone();

        Box::pin(async move {
            trace!("SFTP write request for handle: {} at offset: {} with {} bytes", handle, offset, data.len());

            let (path, current_pos) = this.get_handle_info(&handle).ok_or(StatusCode::NoSuchFile)?;

            let (bucket, key) = this.parse_path(&path)?;

            if key.is_none() {
                return Err(StatusCode::NoSuchFile);
            }

            let object_key = key.unwrap();

            // Check for append operation (not supported fully without multipart, rejecting complex appends)
            if offset != current_pos && offset != 0 {
                this.validate_feature_support("SSH_FXP_OPEN random write / append")?;
            }

            let action = S3Action::PutObject;
            if !is_operation_supported(crate::protocols::session::context::Protocol::Sftp, &action) {
                return Err(StatusCode::OpUnsupported);
            }

            // Authorize the operation
            let context = this.get_session_context()?;

            debug!(
                "SFTP operation authorized: user={}, action={}, bucket={}, object={}, source_ip={}",
                context.session_context.access_key(),
                action.as_str(),
                bucket,
                object_key,
                context.session_context.source_ip
            );

            authorize_operation(
                &context.session_context,
                &action,
                &bucket,
                Some(&object_key),
            ).await.map_err(|_| StatusCode::PermissionDenied)?;

            let data_len = data.len();
            // Convert Vec<u8> to StreamingBlob
            let blob = StreamingBlob::from(s3s::Body::from(data));

            let input = PutObjectInput {
                bucket,
                key: object_key,
                body: Some(blob),
                ..Default::default()
            };

            let s3_client = this.create_s3_client()?;
            match s3_client.put_object(input).await {
                Ok(_) => {
                    debug!("Successfully wrote {} bytes to file", data_len);
                    // Update position in handle
                    if let Ok(mut inner) = this.inner.write() {
                        if let Some(h) = inner.handles.get_mut(&handle) {
                            h.position += data_len as u64;
                        }
                    }

                    Ok(Status {
                        id,
                        status_code: StatusCode::Ok,
                        error_message: "Success".to_string(),
                        language_tag: "en".to_string(),
                    })
                }
                Err(e) => {
                    let err_msg = e.to_string();
                    error!("Failed to put object: {}", err_msg);
                    let s3_error = S3ErrorCode::from_protocol_error("sftp", &err_msg);
                    let protocol_error = map_s3_error_to_protocol("sftp", &s3_error);
                    warn!("Protocol mapped error: {}", protocol_error);
                    Err(StatusCode::Failure)
                }
            }
        })
    }

    /// Handle lstat request (stat without following symlinks)
    ///
    /// MINIO CONSTRAINT: Must map to S3 HeadObject operation
    fn lstat(
        &mut self,
        id: u32,
        path: String,
    ) -> impl Future<Output = Result<Attrs, Self::Error>> + Send {
        trace!("SFTP lstat request for path: {}", path);
        // Delegate to stat logic via self clone
        let this = self.clone();
        Box::pin(async move {
            this.stat_logic(id, path).await
        })
    }

    /// Handle fstat request (stat by handle)
    ///
    /// MINIO CONSTRAINT: Must map to S3 HeadObject operation
    fn fstat(
        &mut self,
        id: u32,
        handle: String,
    ) -> impl Future<Output = Result<Attrs, Self::Error>> + Send {
        trace!("SFTP fstat request for handle: {}", handle);
        let this = self.clone();
        Box::pin(async move {
            let (path, _) = this.get_handle_info(&handle).ok_or(StatusCode::NoSuchFile)?;
            this.stat_logic(id, path).await
        })
    }

    /// Handle setstat request (set file attributes)
    ///
    /// MINIO CONSTRAINT: Must map to S3 operations for supported attributes
    fn setstat(
        &mut self,
        id: u32,
        path: String,
        _attrs: FileAttributes,
    ) -> impl Future<Output = Result<Status, Self::Error>> + Send {
        trace!("SFTP setstat request for path: {}", path);
        let this = self.clone();

        Box::pin(async move {
            // Validate feature support
            this.validate_feature_support("SSH_FXP_SETSTAT operation")?;

            // S3 has limited support for file attributes
            // We'll return success for now
            Ok(Status {
                id,
                status_code: StatusCode::Ok,
                error_message: "Success".to_string(),
                language_tag: "en".to_string(),
            })
        })
    }

    /// Handle fsetstat request (set file attributes by handle)
    ///
    /// MINIO CONSTRAINT: Must map to S3 operations for supported attributes
    fn fsetstat(
        &mut self,
        id: u32,
        handle: String,
        attrs: FileAttributes,
    ) -> impl Future<Output = Result<Status, Self::Error>> + Send {
        trace!("SFTP fsetstat request for handle: {}", handle);
        let this = self.clone();
        Box::pin(async move {
            let (path, _) = this.get_handle_info(&handle).ok_or(StatusCode::NoSuchFile)?;
            // Reuse setstat logic via recursion (since we are in async block, we can't call self.setstat easily without creating new future)
            // Simulating the call:
            this.validate_feature_support("SSH_FXP_SETSTAT operation")?;
            debug!("Ignored setstat attributes for {}: {:?}", path, attrs);

            Ok(Status {
                id,
                status_code: StatusCode::Ok,
                error_message: "Success".to_string(),
                language_tag: "en".to_string(),
            })
        })
    }

    /// Handle directory open request
    ///
    /// MINIO CONSTRAINT: Must map to S3 ListObjects operation
    fn opendir(
        &mut self,
        id: u32,
        path: String,
    ) -> impl Future<Output = Result<Handle, Self::Error>> + Send {
        trace!("SFTP opendir request for path: {}", path);
        let this = self.clone();

        Box::pin(async move {
            // We use the path as the handle for directory listing simplicity here,
            // but wrapped in our handle tracking
            let mut attrs = FileAttributes::default();
            attrs.set_dir(true);
            let handle_name = this.create_handle(path, attrs)?;

            Ok(Handle {
                id,
                handle: handle_name,
            })
        })
    }

    /// Handle directory list request
    ///
    /// MINIO CONSTRAINT: Must map to S3 ListObjects operation
    fn readdir(
        &mut self,
        id: u32,
        handle: String,
    ) -> impl Future<Output = Result<Name, Self::Error>> + Send {
        trace!("SFTP readdir request for handle: {}", handle);
        let this = self.clone();

        Box::pin(async move {
            let (path, _) = this.get_handle_info(&handle).ok_or(StatusCode::NoSuchFile)?;

            let (bucket, prefix) = match this.parse_path(&path) {
                Ok(result) => result,
                Err(_) => return Err(StatusCode::NoSuchFile),
            };

            // Validate feature support
            this.validate_feature_support("SSH_FXP_READDIR operation")?;

            let action = S3Action::ListBucket;
            if !is_operation_supported(crate::protocols::session::context::Protocol::Sftp, &action) {
                return Err(StatusCode::OpUnsupported);
            }

            // Authorize the operation
            let context = this.get_session_context()?;

            debug!(
                "SFTP operation authorized: user={}, action={}, bucket={}, object={:?}, source_ip={}",
                context.session_context.access_key(),
                action.as_str(),
                bucket,
                prefix.as_deref(),
                context.session_context.source_ip
            );

            authorize_operation(
                &context.session_context,
                &action,
                &bucket,
                prefix.as_deref(),
            ).await.map_err(|_| StatusCode::PermissionDenied)?;

            // List objects with prefix
            let mut builder = s3s::dto::ListObjectsV2Input::builder();
            builder.set_bucket(bucket.clone());
            if let Some(p) = prefix.clone() {
                builder.set_prefix(Option::from(p));
            }
            builder.set_delimiter(Some("/".to_string()));

            let input = builder.build().map_err(|_| StatusCode::Failure)?;

            let s3_client = this.create_s3_client()?;
            match s3_client.list_objects_v2(input).await {
                Ok(output) => {
                    let mut names = Vec::new();

                    // Add directories (common prefixes)
                    if let Some(common_prefixes) = output.common_prefixes {
                        for prefix_entry in common_prefixes {
                            if let Some(key) = prefix_entry.prefix {
                                let dir_name = if let Some(p) = &prefix {
                                    key.trim_start_matches(p).trim_end_matches('/').to_string()
                                } else {
                                    key.trim_end_matches('/').to_string()
                                };

                                // Skip empty names
                                if dir_name.is_empty() { continue; }

                                names.push(File {
                                    filename: dir_name,
                                    longname: String::new(),
                                    attrs: {
                                        let mut attrs = FileAttributes::default();
                                        attrs.set_dir(true);
                                        attrs
                                    },
                                });
                            }
                        }
                    }

                    // Add files (objects)
                    if let Some(contents) = output.contents {
                        for object in contents {
                            if let Some(key) = object.key {
                                let file_name = if let Some(p) = &prefix {
                                    key.trim_start_matches(p).to_string()
                                } else {
                                    key.clone()
                                };

                                // Skip empty names or exact matches to prefix (if prefix is a dir)
                                if file_name.is_empty() || file_name == "/" { continue; }

                                names.push(File {
                                    filename: file_name,
                                    longname: String::new(),
                                    attrs: FileAttributes {
                                        size: object.size.map(|s| s as u64),
                                        ..Default::default()
                                    },
                                });
                            }
                        }
                    }

                    Ok(Name {
                        id,
                        files: names,
                    })
                }
                Err(e) => {
                    error!("Failed to list objects: {}", e);
                    Err(StatusCode::Failure)
                }
            }
        })
    }

    /// Handle file remove request
    fn remove(
        &mut self,
        id: u32,
        path: String,
    ) -> impl Future<Output = Result<Status, Self::Error>> + Send {
        trace!("SFTP remove request for path: {}", path);
        let this = self.clone();

        Box::pin(async move {
            let (bucket, key) = this.parse_path(&path)?;

            if key.is_none() {
                return Err(StatusCode::Failure);
            }

            let object_key = key.unwrap();

            let action = S3Action::DeleteObject;
            if !is_operation_supported(crate::protocols::session::context::Protocol::Sftp, &action) {
                return Err(StatusCode::OpUnsupported);
            }

            // Authorize the operation
            let context = this.get_session_context()?;

            authorize_operation(
                &context.session_context,
                &action,
                &bucket,
                Some(&object_key),
            ).await.map_err(|_| StatusCode::PermissionDenied)?;

            let mut builder = s3s::dto::DeleteObjectInput::builder();
            builder.set_bucket(bucket);
            builder.set_key(object_key);
            let input = builder.build().map_err(|_| StatusCode::Failure)?;

            let s3_client = this.create_s3_client()?;
            match s3_client.delete_object(input).await {
                Ok(_) => {
                    debug!("Successfully removed file: {}", path);
                    Ok(Status {
                        id,
                        status_code: StatusCode::Ok,
                        error_message: "Success".to_string(),
                        language_tag: "en".to_string(),
                    })
                }
                Err(e) => {
                    let err_msg = e.to_string();
                    error!("Failed to delete object: {}", err_msg);
                    let s3_error = S3ErrorCode::from_protocol_error("sftp", &err_msg);
                    let protocol_error = map_s3_error_to_protocol("sftp", &s3_error);
                    warn!("Protocol mapped error: {}", protocol_error);
                    Err(StatusCode::Failure)
                }
            }
        })
    }

    /// Handle directory creation request
    fn mkdir(&mut self, id: u32, path: String, attrs: FileAttributes) -> impl Future<Output = Result<Status, Self::Error>> + Send {
        let this = self.clone();

        Box::pin(async move {
            trace!("SFTP mkdir request for path: {} with attrs: {:?}", path, attrs);

            let (bucket, key) = this.parse_path(&path)?;

            let dir_key = if let Some(k) = key {
                if k.ends_with('/') {
                    k
                } else {
                    format!("{}/", k)
                }
            } else {
                return Err(StatusCode::Failure);
            };

            let action = S3Action::PutObject;
            if !is_operation_supported(crate::protocols::session::context::Protocol::Sftp, &action) {
                return Err(StatusCode::OpUnsupported);
            }

            // Authorize the operation
            let context = this.get_session_context()?;
            authorize_operation(
                &context.session_context,
                &action,
                &bucket,
                Some(&dir_key),
            ).await.map_err(|_| StatusCode::PermissionDenied)?;

            // Create directory marker object
            let blob = StreamingBlob::from(s3s::Body::from(Vec::new()));

            let input = PutObjectInput {
                bucket,
                key: dir_key,
                body: Some(blob),
                ..Default::default()
            };

            let s3_client = this.create_s3_client()?;
            match s3_client.put_object(input).await {
                Ok(_) => {
                    debug!("Successfully created directory: {}", path);
                    Ok(Status {
                        id,
                        status_code: StatusCode::Ok,
                        error_message: "Success".to_string(),
                        language_tag: "en".to_string(),
                    })
                }
                Err(e) => {
                    let err_msg = e.to_string();
                    error!("Failed to create directory marker: {}", err_msg);
                    let s3_error = S3ErrorCode::from_protocol_error("sftp", &err_msg);
                    let protocol_error = map_s3_error_to_protocol("sftp", &s3_error);
                    warn!("Protocol mapped error: {}", protocol_error);
                    Err(StatusCode::Failure)
                }
            }
        })
    }

    /// Handle directory removal request
    fn rmdir(&mut self, id: u32, path: String) -> impl Future<Output = Result<Status, Self::Error>> + Send {
        let this = self.clone();

        Box::pin(async move {
            trace!("SFTP rmdir request for path: {}", path);

            let (bucket, key) = this.parse_path(&path)?;

            let dir_key = if let Some(k) = key {
                if k.ends_with('/') {
                    k
                } else {
                    format!("{}/", k)
                }
            } else {
                return Err(StatusCode::Failure);
            };

            let action = S3Action::DeleteObject;
            if !is_operation_supported(crate::protocols::session::context::Protocol::Sftp, &action) {
                return Err(StatusCode::OpUnsupported);
            }

            // Authorize the operation
            let context = this.get_session_context()?;

            authorize_operation(
                &context.session_context,
                &action,
                &bucket,
                Some(&dir_key),
            ).await.map_err(|_| StatusCode::PermissionDenied)?;

            let mut builder = s3s::dto::DeleteObjectInput::builder();
            builder.set_bucket(bucket);
            builder.set_key(dir_key);
            let input = builder.build().map_err(|_| StatusCode::Failure)?;

            let s3_client = this.create_s3_client()?;
            match s3_client.delete_object(input).await {
                Ok(_) => {
                    debug!("Successfully removed directory: {}", path);
                    Ok(Status {
                        id,
                        status_code: StatusCode::Ok,
                        error_message: "Success".to_string(),
                        language_tag: "en".to_string(),
                    })
                }
                Err(e) => {
                    let err_msg = e.to_string();
                    error!("Failed to remove directory marker: {}", err_msg);
                    let s3_error = S3ErrorCode::from_protocol_error("sftp", &err_msg);
                    let protocol_error = map_s3_error_to_protocol("sftp", &s3_error);
                    warn!("Protocol mapped error: {}", protocol_error);
                    Err(StatusCode::Failure)
                }
            }
        })
    }

    /// Handle realpath request
    fn realpath(
        &mut self,
        id: u32,
        path: String,
    ) -> impl Future<Output = Result<Name, Self::Error>> + Send {
        trace!("SFTP realpath request for path: {}", path);
        async move {
            // For simplicity, we'll just return the path as-is
            Ok(Name {
                id,
                files: vec![File {
                    filename: path,
                    longname: String::new(),
                    attrs: FileAttributes::default(),
                }],
            })
        }
    }

    /// Handle file stat request
    fn stat(
        &mut self,
        id: u32,
        path: String,
    ) -> impl Future<Output = Result<Attrs, Self::Error>> + Send {
        trace!("SFTP stat request for path: {}", path);
        let this = self.clone();
        Box::pin(async move {
            this.stat_logic(id, path).await
        })
    }

    /// Handle file rename request
    ///
    /// map to S3 CopyObject + DeleteObject operations
    fn rename(&mut self, id: u32, oldpath: String, newpath: String) -> impl Future<Output = Result<Status, Self::Error>> + Send {
        trace!("SFTP rename request from: {} to: {}", oldpath, newpath);
        let this = self.clone();

        Box::pin(async move {
            // Validate feature support
            this.validate_feature_support("SSH_FXP_RENAME atomic rename")?;

            let (from_bucket, from_key) = this.parse_path(&oldpath)?;
            let (to_bucket, to_key) = this.parse_path(&newpath)?;

            if from_key.is_none() || to_key.is_none() {
                return Err(StatusCode::Failure);
            }

            let from_object_key = from_key.unwrap();
            let to_object_key = to_key.unwrap();

            // Check if this is a cross-bucket operation (not supported)
            if from_bucket != to_bucket {
                return Err(StatusCode::OpUnsupported);
            }

            // CopyObject operation
            let copy_action = S3Action::CopyObject;
            if !is_operation_supported(crate::protocols::session::context::Protocol::Sftp, &copy_action) {
                return Err(StatusCode::OpUnsupported);
            }

            // Authorize the copy operation
            let context = this.get_session_context()?;
            authorize_operation(
                &context.session_context,
                &copy_action,
                &to_bucket,
                Some(&to_object_key),
            ).await.map_err(|_| StatusCode::PermissionDenied)?;

            let mut copy_builder = s3s::dto::CopyObjectInput::builder();
            copy_builder.set_bucket(to_bucket.clone());
            copy_builder.set_copy_source(s3s::dto::CopySource::Bucket {
                bucket: Box::from(from_bucket.clone()),
                key: Box::from(from_object_key.clone()),
                version_id: None,
            });
            copy_builder.set_key(to_object_key.clone());

            let copy_input = copy_builder.build().map_err(|_| StatusCode::Failure)?;

            let s3_client = this.create_s3_client()?;
            match s3_client.copy_object(copy_input).await {
                Ok(_) => {
                    debug!("Successfully copied object for rename");

                    // Delete original object
                    let delete_action = S3Action::DeleteObject;
                    if !is_operation_supported(crate::protocols::session::context::Protocol::Sftp, &delete_action) {
                        // Note: We are in a partial state here (Copy succeeded, Delete failed/unsupported)
                        return Err(StatusCode::OpUnsupported);
                    }

                    // Authorize the delete operation
                    // We need to fetch context again or reuse
                    authorize_operation(
                        &context.session_context,
                        &delete_action,
                        &from_bucket,
                        Some(&from_object_key),
                    ).await.map_err(|_| StatusCode::PermissionDenied)?;

                    let mut del_builder = s3s::dto::DeleteObjectInput::builder();
                    del_builder.set_bucket(from_bucket);
                    del_builder.set_key(from_object_key);
                    let delete_input = del_builder.build().map_err(|_| StatusCode::Failure)?;

                    match s3_client.delete_object(delete_input).await {
                        Ok(_) => {
                            debug!("Successfully deleted original object after rename");
                            Ok(Status {
                                id,
                                status_code: StatusCode::Ok,
                                error_message: "Success".to_string(),
                                language_tag: "en".to_string(),
                            })
                        }
                        Err(e) => {
                            let err_msg = e.to_string();
                            error!("Failed to delete original object after copy: {}", err_msg);
                            let s3_error = S3ErrorCode::from_protocol_error("sftp", &err_msg);
                            let protocol_error = map_s3_error_to_protocol("sftp", &s3_error);
                            warn!("Protocol error: {}", protocol_error);
                            Err(StatusCode::Failure)
                        }
                    }
                }
                Err(e) => {
                    let err_msg = e.to_string();
                    error!("Failed to copy object for rename: {}", err_msg);
                    let s3_error = S3ErrorCode::from_protocol_error("sftp", &err_msg);
                    let protocol_error = map_s3_error_to_protocol("sftp", &s3_error);
                    warn!("Protocol error: {}", protocol_error);
                    Err(StatusCode::Failure)
                }
            }
        })
    }

    /// Handle read symbolic link request
    ///
    /// Symbolic links are not supported in S3
    fn readlink(
        &mut self,
        _id: u32,
        path: String,
    ) -> impl Future<Output = Result<Name, Self::Error>> + Send {
        trace!("SFTP readlink request for path: {}", path);
        let this = self.clone();

        Box::pin(async move {
            // Validate feature support
            this.validate_feature_support("SSH_FXP_READLINK operation")?;
            // S3 doesn't support symbolic links
            Err(StatusCode::OpUnsupported)
        })
    }

    /// Handle symbolic link request
    fn symlink(
        &mut self,
        _id: u32,
        linkpath: String,
        targetpath: String,
    ) -> impl Future<Output = Result<Status, Self::Error>> + Send {
        trace!("SFTP symlink request from: {} to: {}", linkpath, targetpath);
        let this = self.clone();

        Box::pin(async move {
            // Validate feature support
            this.validate_feature_support("SSH_FXP_SYMLINK operation")?;
            // S3 doesn't support symbolic links
            Err(StatusCode::OpUnsupported)
        })
    }

    /// Handle extended request
    ///
    /// MINIO CONSTRAINT: Must handle SFTP extensions appropriately
    fn extended(
        &mut self,
        _id: u32,
        request: String,
        _data: Vec<u8>,
    ) -> impl Future<Output = Result<Packet, Self::Error>> + Send {
        trace!("SFTP extended request: {}", request);
        async move {
            // S3 doesn't support SFTP extensions, return unsupported operation
            Err(StatusCode::OpUnsupported)
        }
    }
}

// Private helper methods for SftpHandler to reuse logic
impl SftpHandler {
    /// Internal logic for stat operations
    async fn stat_logic(&self, id: u32, path: String) -> Result<Attrs, StatusCode> {
        let (bucket, key) = self.parse_path(&path)?;

        if let Some(object_key) = key {
            // Object stat request
            let action = S3Action::HeadObject;
            if !is_operation_supported(crate::protocols::session::context::Protocol::Sftp, &action) {
                return Err(StatusCode::OpUnsupported);
            }

            // Authorize the operation
            let context = self.get_session_context()?;
            authorize_operation(
                &context.session_context,
                &action,
                &bucket,
                Some(&object_key),
            ).await.map_err(|_| StatusCode::PermissionDenied)?;

            let input = s3s::dto::HeadObjectInput {
                bucket: bucket.clone(),
                key: object_key.clone(),
                ..Default::default()
            };

            let s3_client = self.create_s3_client()?;
            match s3_client.head_object(input).await {
                Ok(output) => {
                    let attrs = FileAttributes {
                        size: Some(output.content_length.unwrap_or(0) as u64),
                        ..Default::default()
                    };
                    Ok(Attrs { id, attrs })
                }
                Err(e) => {
                    error!("Failed to get object metadata: {}", e);
                    Err(StatusCode::NoSuchFile)
                }
            }
        } else {
            // Directory stat request (bucket)
            let action = S3Action::HeadBucket;
            if !is_operation_supported(crate::protocols::session::context::Protocol::Sftp, &action) {
                return Err(StatusCode::OpUnsupported);
            }

            // Authorize the operation
            let context = self.get_session_context()?;
            authorize_operation(
                &context.session_context,
                &action,
                &bucket,
                None,
            ).await.map_err(|_| StatusCode::PermissionDenied)?;

            let input = s3s::dto::HeadBucketInput {
                bucket: bucket.clone(),
                expected_bucket_owner: None,
            };

            let s3_client = self.create_s3_client()?;
            match s3_client.head_bucket(input).await {
                Ok(_) => {
                    let mut attrs = FileAttributes::default();
                    attrs.set_dir(true);
                    Ok(Attrs { id, attrs })
                }
                Err(e) => {
                    error!("Failed to get bucket metadata: {}", e);
                    Err(StatusCode::NoSuchFile)
                }
            }
        }
    }
}