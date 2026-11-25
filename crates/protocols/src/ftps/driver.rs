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
use async_trait::async_trait;
use bytes::{Buf as _, Bytes};
use rustfs_gateway_stream::{ByteStream, PayloadRead, PayloadStream, StreamError, StreamErrorKind};
use rustfs_gateway_types::dto::*;
use rustfs_gateway_types::{BucketName, ObjectKey, OpaqueString};
use rustfs_utils::MaskedAccessKey;
use rustfs_utils::path;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};
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

#[derive(Clone, Copy, PartialEq, Eq)]
enum StreamReadState {
    Open,
    Eof,
    Failed,
}

/// Adapts a push-model [`ByteStream`] to `tokio::io::AsyncRead` for FTPS `RETR`.
///
/// Holds at most one in-flight chunk from the storage backend and releases it
/// as soon as its last byte has been copied into the caller's buffer. Wrapping
/// the `Send`-only stream in a `Mutex` lends `Sync` (required by
/// `unftp_core::storage::StorageBackend::get`) without `unsafe`; `poll_read`
/// accesses the stream through `Mutex::get_mut()` under `&mut self`.
struct FtpsBodyReader {
    stream: Mutex<ByteStream>,
    current: Bytes,
    state: StreamReadState,
    username: String,
    path: String,
    bucket: String,
    key: String,
}

impl FtpsBodyReader {
    fn new(stream: ByteStream, username: String, path: String, bucket: String, key: String) -> Self {
        Self {
            stream: Mutex::new(stream),
            current: Bytes::new(),
            state: StreamReadState::Open,
            username,
            path,
            bucket,
            key,
        }
    }
}

fn stream_error_to_io(err: StreamError) -> std::io::Error {
    let kind = match err.kind() {
        StreamErrorKind::Io(inner) => inner.kind(),
        StreamErrorKind::IncompleteBody => std::io::ErrorKind::UnexpectedEof,
        StreamErrorKind::LengthMismatch { .. } => std::io::ErrorKind::InvalidData,
        StreamErrorKind::PolledAfterEof | StreamErrorKind::Upstream(_) => std::io::ErrorKind::Other,
    };
    let bytes_before_error = err.bytes_before_error();
    match err.into_kind() {
        StreamErrorKind::Io(inner) => inner,
        other => std::io::Error::new(kind, StreamError::new(other).with_bytes_before_error(bytes_before_error)),
    }
}

impl AsyncRead for FtpsBodyReader {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }

        loop {
            if !this.current.is_empty() {
                let n = buf.remaining().min(this.current.len());
                buf.put_slice(&this.current[..n]);
                this.current.advance(n);
                if this.current.is_empty() {
                    // Release the producer's chunk allocation as soon as its last byte is copied.
                    this.current = Bytes::new();
                }
                return Poll::Ready(Ok(()));
            }

            match this.state {
                StreamReadState::Eof => return Poll::Ready(Ok(())),
                StreamReadState::Failed => {
                    return Poll::Ready(Err(std::io::Error::other("ftps object stream already failed")));
                }
                StreamReadState::Open => {}
            }

            let stream = this.stream.get_mut().unwrap_or_else(PoisonError::into_inner);
            match Pin::new(stream).poll_read(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(PayloadRead::Chunk(chunk))) => {
                    this.current = chunk;
                }
                Poll::Ready(Ok(PayloadRead::Eof { .. })) => {
                    this.state = StreamReadState::Eof;
                    return Poll::Ready(Ok(()));
                }
                Poll::Ready(Err(e)) => {
                    this.state = StreamReadState::Failed;
                    error!(
                        event = EVENT_FTPS_STREAM_READ_FAILED,
                        component = LOG_COMPONENT_PROTOCOLS,
                        subsystem = LOG_SUBSYSTEM_FTPS_DRIVER,
                        username = %MaskedAccessKey(&this.username),
                        path = %this.path,
                        bucket = %this.bucket,
                        object = %this.key,
                        error = %e,
                        "ftps stream read failed"
                    );
                    return Poll::Ready(Err(stream_error_to_io(e)));
                }
            }
        }
    }
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
        match self
            .storage
            .authorize_operation(session_context, &S3Action::ListBuckets, "", None)
            .await
        {
            Ok(_) => {}
            Err(_e) => {
                return Err(Error::new(ErrorKind::PermanentFileNotAvailable, "Access denied"));
            }
        }

        let mut list_result = Vec::new();
        match self.storage.list_buckets(session_context.credentials()).await {
            Ok(output) => {
                for bucket in output.buckets {
                    let metadata = FtpsMetadata {
                        size: 0,
                        modified: time::OffsetDateTime::from_unix_timestamp(bucket.creation_date.secs())
                            .ok()
                            .and_then(|value| value.replace_nanosecond(bucket.creation_date.subsec_nanos()).ok())
                            .map(std::time::SystemTime::from),
                        is_dir: true,
                    };

                    list_result.push(Fileinfo {
                        path: PathBuf::from(bucket.name.as_str()),
                        metadata,
                    });
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
        // First, delete all objects in the bucket (with pagination)
        let mut continuation_token = None;
        loop {
            let mut list_input = ListObjectsV2Input::builder().bucket(BucketName::new(bucket.to_owned()).map_err(|e| {
                Error::new(ErrorKind::PermanentFileNotAvailable, format!("Failed to build ListObjectsV2Input: {e}"))
            })?);

            if let Some(token) = continuation_token {
                list_input = list_input.continuation_token(OpaqueString::from(token));
            }

            let list_input = list_input.build();

            self.storage
                .authorize_list_objects(session_context, &list_input)
                .await
                .map_err(|_| Error::new(ErrorKind::PermanentFileNotAvailable, "Access denied"))?;

            if let Ok(output) = self.storage.list_objects_v2(list_input, session_context.credentials()).await {
                // Delete all objects in this page
                for obj in output.contents {
                    let obj_key = obj.key;
                    self.storage
                        .authorize_operation(session_context, &S3Action::DeleteObject, bucket, Some(obj_key.as_str()))
                        .await
                        .map_err(|_| Error::new(ErrorKind::PermanentFileNotAvailable, "Access denied"))?;

                    let _ = self
                        .storage
                        .delete_object(bucket, obj_key.as_str(), session_context.credentials())
                        .await;
                }

                // Check if there are more objects
                if !output.is_truncated {
                    break;
                }
                continuation_token = output.next_continuation_token.map(|token| token.into_string());
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
            self.storage
                .authorize_operation(session_context, &S3Action::HeadObject, &bucket, Some(&key))
                .await
                .map_err(|_| Error::new(ErrorKind::PermanentFileNotAvailable, "Access denied"))?;

            match self.storage.head_object(&bucket, &key, session_context.credentials()).await {
                Ok(output) => {
                    let size = output.content_length.unwrap_or(0) as u64;
                    let modified = output.last_modified.and_then(|dt| {
                        time::OffsetDateTime::from_unix_timestamp(dt.secs())
                            .ok()
                            .and_then(|value| value.replace_nanosecond(dt.subsec_nanos()).ok())
                            .map(std::time::SystemTime::from)
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
            self.storage
                .authorize_operation(session_context, &S3Action::HeadBucket, &bucket, None)
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

        let prefix_with_slash = prefix.clone().map(|p| if p.ends_with('/') { p } else { format!("{}/", p) });

        let list_input = ListObjectsV2Input::builder()
            .bucket(BucketName::new(bucket.clone()).map_err(|e| {
                Error::new(ErrorKind::PermanentFileNotAvailable, format!("Failed to build ListObjectsV2Input: {e}"))
            })?)
            .prefix(prefix_with_slash.clone().unwrap_or_default())
            .delimiter("/".to_string())
            .build();

        self.storage
            .authorize_list_objects(session_context, &list_input)
            .await
            .map_err(|_| Error::new(ErrorKind::PermanentFileNotAvailable, "Access denied"))?;

        match self.storage.list_objects_v2(list_input, session_context.credentials()).await {
            Ok(output) => {
                let mut fileinfos = Vec::new();

                // Add files (objects)
                for obj in output.contents {
                    let key = obj.key;
                    // Filter: only show files directly in current directory
                    // Skip files in subdirectories (they should be accessed via cd)
                    let should_show = match prefix_with_slash.as_deref() {
                        None => {
                            // Root directory: only show files without "/"
                            !key.as_str().contains('/')
                        }
                        Some(prefix_with_slash) => {
                            // Subdirectory: show files starting with prefix, excluding the
                            // directory's own marker object (MKD creates one)
                            key.as_str().starts_with(prefix_with_slash) && key.as_str() != prefix_with_slash
                        }
                    };

                    if !should_show {
                        continue;
                    }

                    let filename = PathBuf::from(key.as_str())
                        .file_name()
                        .ok_or_else(|| Error::new(ErrorKind::PermanentFileNotAvailable, format!("Invalid filename: {key:?}")))
                        .map(PathBuf::from)?;

                    let size = obj.size.max(0) as u64;
                    let modified = time::OffsetDateTime::from_unix_timestamp(obj.last_modified.secs())
                        .ok()
                        .and_then(|value| value.replace_nanosecond(obj.last_modified.subsec_nanos()).ok())
                        .map(std::time::SystemTime::from);

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

                // Add directories (common prefixes)
                for prefix in output.common_prefixes {
                    let prefix_str = prefix.prefix;
                    let dir_name = PathBuf::from(prefix_str.trim_end_matches('/'))
                        .file_name()
                        .ok_or_else(|| {
                            Error::new(ErrorKind::PermanentFileNotAvailable, format!("Invalid directory: {prefix_str}"))
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
        self.storage
            .authorize_operation(session_context, &S3Action::GetObject, &bucket, Some(&key))
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

                // Stream chunks directly to libunftp's data channel rather than buffering
                // the full object in memory before the first byte is written.
                Ok(Box::new(FtpsBodyReader::new(
                    body,
                    user.username.clone(),
                    path_str.into_owned(),
                    bucket,
                    key,
                )))
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
        self.storage
            .authorize_operation(session_context, &S3Action::PutObject, &bucket, Some(&key))
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
            self.storage
                .authorize_operation(session_context, &S3Action::DeleteObject, &bucket, Some(&key))
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
            // DELE is strictly a file-deletion verb (RFC 959); bucket/directory paths
            // must never recursively wipe a bucket via DELE.
            Err(Error::new(
                ErrorKind::PermanentFileNotAvailable,
                "Directory deletion not supported via DELE; use RMD",
            ))
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

        let (bucket, key) = parse_s3_path(&path_str)
            .map_err(|e| Error::new(ErrorKind::PermanentFileNotAvailable, format!("{}: {}", "Invalid path", e)))?;

        if bucket.is_empty() {
            return Err(Error::new(ErrorKind::PermanentFileNotAvailable, "Invalid path: empty bucket name"));
        }

        if let Some(key) = key {
            // A path below the bucket names a subdirectory: represent it as a
            // zero-byte marker object with a trailing slash rather than a bucket.
            let dir_key = if key.ends_with('/') { key } else { format!("{key}/") };

            self.storage
                .authorize_operation(session_context, &S3Action::PutObject, &bucket, Some(&dir_key))
                .await
                .map_err(|_| Error::new(ErrorKind::PermanentFileNotAvailable, "Access denied"))?;

            let put_input = PutObjectInput::builder()
                .bucket(BucketName::new(bucket.clone()).map_err(|e| {
                    Error::new(ErrorKind::PermanentFileNotAvailable, format!("Failed to build PutObjectInput: {e}"))
                })?)
                .key(ObjectKey::new(dir_key.clone()).map_err(|e| {
                    Error::new(ErrorKind::PermanentFileNotAvailable, format!("Failed to build PutObjectInput: {e}"))
                })?)
                .content_length(0)
                .content_type("application/x-directory".to_string())
                .body(ByteStream::from_bytes(Bytes::new()))
                .build();

            return match self.storage.put_object(put_input, session_context.credentials()).await {
                Ok(_) => {
                    debug!(
                        event = EVENT_FTPS_DIRECTORY_STATE,
                        component = LOG_COMPONENT_PROTOCOLS,
                        subsystem = LOG_SUBSYSTEM_FTPS_DRIVER,
                        state = "created",
                        username = %masked_username,
                        path = %path_str,
                        bucket = %bucket,
                        object = %dir_key,
                        "FTPS directory marker created"
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
                        object = %dir_key,
                        error = %e,
                        "ftps directory state changed"
                    );
                    Err(Error::new(ErrorKind::PermanentFileNotAvailable, format!("Mkdir failed: {e}")))
                }
            };
        }

        // MKD at bucket level creates a bucket, so it must clear the same authorization
        // boundary as an S3 CreateBucket call.
        self.storage
            .authorize_operation(session_context, &S3Action::CreateBucket, &bucket, None)
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

        let (bucket, key) = parse_s3_path(&path_str)
            .map_err(|e| Error::new(ErrorKind::PermanentFileNotAvailable, format!("{}: {}", "Invalid path", e)))?;

        if bucket.is_empty() {
            return Err(Error::new(ErrorKind::PermanentFileNotAvailable, "Invalid path: empty bucket name"));
        }

        if let Some(key) = key {
            // A path below the bucket names a subdirectory prefix. Follow RFC 959 and
            // the sibling SFTP driver: refuse non-empty directories with 550 and
            // delete only the directory's own marker object, leaving the bucket and
            // every sibling prefix untouched.
            let prefix_with_slash = if key.ends_with('/') { key } else { format!("{key}/") };

            // max_keys=2 because the listing may return the directory's own marker
            // (decoded to "prefix/") as a content entry alongside a real child.
            let list_input = ListObjectsV2Input::builder()
                .bucket(BucketName::new(bucket.clone()).map_err(|e| {
                    Error::new(ErrorKind::PermanentFileNotAvailable, format!("Failed to build ListObjectsV2Input: {e}"))
                })?)
                .prefix(prefix_with_slash.clone())
                .delimiter("/".to_string())
                .max_keys(2)
                .build();

            self.storage
                .authorize_list_objects(session_context, &list_input)
                .await
                .map_err(|_| Error::new(ErrorKind::PermanentFileNotAvailable, "Access denied"))?;

            let output = self
                .storage
                .list_objects_v2(list_input, session_context.credentials())
                .await
                .map_err(|e| {
                    error!(
                        event = EVENT_FTPS_DIRECTORY_STATE,
                        component = LOG_COMPONENT_PROTOCOLS,
                        subsystem = LOG_SUBSYSTEM_FTPS_DRIVER,
                        state = "remove_failed",
                        path = %path_str,
                        bucket = %bucket,
                        object = %prefix_with_slash,
                        error = %e,
                        "ftps directory state changed"
                    );
                    Error::new(ErrorKind::PermanentFileNotAvailable, format!("List failed: {e}"))
                })?;

            let has_content = output
                .contents
                .iter()
                .any(|obj| obj.key.as_str() != prefix_with_slash.as_str());
            let has_subdirs = !output.common_prefixes.is_empty();
            if has_content || has_subdirs || output.is_truncated {
                return Err(Error::new(ErrorKind::PermanentDirectoryNotEmpty, "Directory not empty"));
            }

            self.storage
                .authorize_operation(session_context, &S3Action::DeleteObject, &bucket, Some(&prefix_with_slash))
                .await
                .map_err(|_| Error::new(ErrorKind::PermanentFileNotAvailable, "Access denied"))?;

            self.storage
                .delete_object(&bucket, &prefix_with_slash, session_context.credentials())
                .await
                .map_err(|e| {
                    error!(
                        event = EVENT_FTPS_DIRECTORY_STATE,
                        component = LOG_COMPONENT_PROTOCOLS,
                        subsystem = LOG_SUBSYSTEM_FTPS_DRIVER,
                        state = "remove_failed",
                        path = %path_str,
                        bucket = %bucket,
                        object = %prefix_with_slash,
                        error = %e,
                        "ftps directory state changed"
                    );
                    Error::new(ErrorKind::PermanentFileNotAvailable, format!("Delete failed: {e}"))
                })?;

            debug!(
                event = EVENT_FTPS_DIRECTORY_STATE,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_FTPS_DRIVER,
                state = "removed",
                path = %path_str,
                bucket = %bucket,
                object = %prefix_with_slash,
                "FTPS directory removed"
            );
            return Ok(());
        }

        // Authorize delete bucket
        self.storage
            .authorize_operation(session_context, &S3Action::DeleteBucket, &bucket, None)
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
        self.storage
            .authorize_operation(session_context, &S3Action::HeadBucket, &bucket, None)
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

    #[tokio::test]
    async fn listing_authorizes_normalized_prefix_and_delimiter() {
        use crate::common::dummy_storage::DummyBackend;
        use crate::common::gateway::with_test_auth_override;
        use crate::common::session::{Protocol, test_session};
        use unftp_core::storage::StorageBackend as _;
        let backend = DummyBackend::new();
        let driver = super::FtpsDriver::new(backend.clone());
        let user = super::super::server::FtpsUser {
            username: "protocol-user".into(),
            name: None,
            session_context: test_session(Protocol::Ftps),
        };
        with_test_auth_override(
            |_, bucket, prefix| bucket == "bucket" && prefix == Some("private/"),
            driver.list(&user, "/bucket/private"),
        )
        .await
        .expect("normalized prefix satisfies conditional allow");
        let authorized = backend.list_authorizations();
        let executed = backend.list_objects_calls();
        assert_eq!(authorized.len(), 1);
        assert_eq!(executed.len(), 1);
        assert_eq!(authorized[0].prefix.as_deref(), Some("private/"));
        assert_eq!(authorized[0].delimiter.as_deref(), Some("/"));
        assert_eq!(authorized[0].prefix, executed[0].prefix);
        assert_eq!(authorized[0].delimiter, executed[0].delimiter);
        assert_eq!(authorized[0].max_keys, executed[0].max_keys);
        let denied =
            with_test_auth_override(|_, _, prefix| prefix != Some("private/"), driver.list(&user, "/bucket/private")).await;
        assert!(denied.is_err(), "matching prefix deny blocks LIST");
        assert_eq!(backend.list_objects_calls().len(), 1, "denied LIST cannot reach storage");
    }

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

    #[tokio::test]
    async fn backend_policy_denial_blocks_ftp_read_and_bucket_creation() {
        use crate::common::dummy_storage::DummyBackend;
        use crate::common::gateway::with_test_auth_override;
        use crate::common::session::{Protocol, test_session};
        use unftp_core::storage::StorageBackend as _;
        let backend = DummyBackend::new().deny_authorization();
        backend.queue_create_bucket_ok();
        backend.queue_get_object_bytes(b"secret content".to_vec());
        let driver = super::FtpsDriver::new(backend);
        let user = super::super::server::FtpsUser {
            username: "protocol-user".into(),
            name: None,
            session_context: test_session(Protocol::Ftps),
        };
        let result = with_test_auth_override(|_, _, _| true, driver.mkd(&user, "/bucket")).await;
        assert!(result.is_err(), "backend policy deny must override identity allow");
        let result = with_test_auth_override(|_, _, _| true, driver.get(&user, "/bucket/secret.txt", 0)).await;
        assert!(result.is_err(), "backend policy denial must prevent returning the queued object body");
    }

    /// RETR must stream the object body without buffering the whole object in
    /// RAM first. The backend yields one chunk and then stalls forever, so a
    /// driver that collects before returning never completes `get()`, whereas a
    /// streaming driver returns immediately and yields the first chunk.
    #[tokio::test]
    async fn ftps_get_streams_body_without_buffering_whole_object() {
        use super::FtpsDriver;
        use crate::common::dummy_storage::DummyBackend;
        use crate::common::gateway::with_test_auth_override;
        use crate::common::session::{Protocol, test_session};
        use std::time::Duration;
        use tokio::io::AsyncReadExt;
        use unftp_core::storage::StorageBackend as _;

        let backend = DummyBackend::new();
        backend.queue_get_object_stalling_after_chunk(b"head".to_vec(), 1 << 30);
        let driver = FtpsDriver::new(backend);
        let user = super::super::server::FtpsUser {
            username: "protocol-user".into(),
            name: None,
            session_context: test_session(Protocol::Ftps),
        };

        let mut reader = tokio::time::timeout(
            Duration::from_secs(5),
            with_test_auth_override(|_, _, _| true, driver.get(&user, "/bucket/big.bin", 0)),
        )
        .await
        .expect("RETR must return before the entire body has been read")
        .expect("RETR");

        let mut head = [0u8; 4];
        tokio::time::timeout(Duration::from_secs(5), reader.read_exact(&mut head))
            .await
            .expect("first chunk must be readable while the backend stalls")
            .expect("read first chunk");
        assert_eq!(&head, b"head");
    }

    /// Multi-chunk streaming with small caller buffers must preserve all bytes
    /// across chunk splits and return clean EOF on repeated trailing reads.
    #[tokio::test]
    async fn ftps_get_streams_multi_chunk_and_partial_reads_byte_exact() {
        use super::FtpsDriver;
        use crate::common::dummy_storage::DummyBackend;
        use crate::common::gateway::with_test_auth_override;
        use crate::common::session::{Protocol, test_session};
        use tokio::io::AsyncReadExt;
        use unftp_core::storage::StorageBackend as _;

        let backend = DummyBackend::new();
        backend.queue_get_object_chunks(vec![Ok(b"abcdefg".to_vec()), Ok(b"".to_vec()), Ok(b"12345".to_vec())], 12);
        let driver = FtpsDriver::new(backend);
        let user = super::super::server::FtpsUser {
            username: "protocol-user".into(),
            name: None,
            session_context: test_session(Protocol::Ftps),
        };

        let mut reader = with_test_auth_override(|_, _, _| true, driver.get(&user, "/bucket/multi.bin", 0))
            .await
            .expect("RETR");

        let mut out = Vec::new();
        let mut buf = [0u8; 3];
        loop {
            let n = reader.read(&mut buf).await.expect("read chunk");
            if n == 0 {
                break;
            }
            out.extend_from_slice(&buf[..n]);
        }
        assert_eq!(out, b"abcdefg12345");
        assert_eq!(reader.read(&mut buf).await.expect("repeated EOF read"), 0);
    }

    /// Mid-stream backend I/O errors must surface on the returned AsyncRead
    /// after already-delivered bytes, preserving error kind and never turning
    /// subsequent polls into silent EOF.
    #[tokio::test]
    async fn ftps_get_propagates_mid_stream_backend_error() {
        use super::FtpsDriver;
        use crate::common::dummy_storage::DummyBackend;
        use crate::common::gateway::with_test_auth_override;
        use crate::common::session::{Protocol, test_session};
        use tokio::io::AsyncReadExt;
        use unftp_core::storage::StorageBackend as _;

        let backend = DummyBackend::new();
        backend.queue_get_object_chunks(
            vec![
                Ok(b"part".to_vec()),
                Err(std::io::Error::new(std::io::ErrorKind::ConnectionReset, "disk read failed")),
            ],
            8,
        );
        let driver = FtpsDriver::new(backend);
        let user = super::super::server::FtpsUser {
            username: "protocol-user".into(),
            name: None,
            session_context: test_session(Protocol::Ftps),
        };

        let mut reader = with_test_auth_override(|_, _, _| true, driver.get(&user, "/bucket/broken.bin", 0))
            .await
            .expect("RETR starts streaming");

        let mut head = [0u8; 4];
        reader.read_exact(&mut head).await.expect("initial chunk delivered");
        assert_eq!(&head, b"part");

        let mut tail = [0u8; 4];
        let err = reader
            .read(&mut tail)
            .await
            .expect_err("mid-stream backend error must propagate");
        assert_eq!(err.kind(), std::io::ErrorKind::ConnectionReset);
        assert!(
            reader.read(&mut tail).await.is_err(),
            "polling a failed stream again must keep failing instead of returning EOF"
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

    /// RMD on a path below a bucket must confine itself to that subdirectory:
    /// refuse non-empty directories (objects, child prefixes, or truncated pages)
    /// with 550, fail closed on list errors, and otherwise delete only the
    /// directory's own marker without touching the bucket or sibling prefixes.
    #[tokio::test]
    async fn ftps_rmd_subdirectory_only_removes_its_marker() {
        use super::FtpsDriver;
        use crate::common::dummy_storage::{DummyBackend, DummyError};
        use crate::common::gateway::{S3Action, with_test_auth_override};
        use crate::common::session::{Protocol, test_session};
        use rustfs_gateway_types::ObjectKey;
        use rustfs_gateway_types::dto::{CommonPrefix, ListObjectsV2Output, Object};
        use unftp_core::storage::{ErrorKind, StorageBackend as _};

        let backend = DummyBackend::new();
        backend.queue_list_objects_v2_ok_with_keys(&["scratch/", "scratch/tmp.txt"]);
        backend.queue_delete_object_ok();
        backend.queue_delete_object_ok();
        backend.queue_delete_bucket_ok();

        let driver = FtpsDriver::new(backend.clone());
        let user = super::super::server::FtpsUser {
            username: "member".to_string(),
            name: None,
            session_context: test_session(Protocol::Ftps),
        };

        // Non-empty directory with a file must be refused with 550.
        let err = with_test_auth_override(|_, _, _| true, driver.rmd(&user, "/team-bucket/scratch"))
            .await
            .expect_err("RMD of a directory containing a file must be refused");
        assert_eq!(err.kind(), ErrorKind::PermanentDirectoryNotEmpty);
        assert!(backend.delete_object_calls().is_empty());
        assert!(backend.delete_bucket_calls().is_empty());

        // Non-empty directory with a nested subdirectory must be refused with 550.
        backend.queue_list_objects_v2_ok(ListObjectsV2Output {
            contents: vec![Object {
                key: ObjectKey::new("scratch/".to_string()).expect("valid key"),
                ..Default::default()
            }],
            common_prefixes: vec![CommonPrefix {
                prefix: "scratch/nested/".to_string(),
            }],
            ..Default::default()
        });
        let err = with_test_auth_override(|_, _, _| true, driver.rmd(&user, "/team-bucket/scratch"))
            .await
            .expect_err("RMD of a directory with child prefixes must be refused");
        assert_eq!(err.kind(), ErrorKind::PermanentDirectoryNotEmpty);
        assert!(backend.delete_object_calls().is_empty());

        // A truncated listing must fail closed as non-empty.
        backend.queue_list_objects_v2_ok(ListObjectsV2Output {
            contents: vec![Object {
                key: ObjectKey::new("scratch/".to_string()).expect("valid key"),
                ..Default::default()
            }],
            is_truncated: true,
            ..Default::default()
        });
        let err = with_test_auth_override(|_, _, _| true, driver.rmd(&user, "/team-bucket/scratch"))
            .await
            .expect_err("RMD must fail closed on a truncated listing");
        assert_eq!(err.kind(), ErrorKind::PermanentDirectoryNotEmpty);
        assert!(backend.delete_object_calls().is_empty());

        // A backend list error must not fall through to any delete.
        backend.queue_list_objects_v2_err(DummyError::Injected("backend list failed".into()));
        let err = with_test_auth_override(|_, _, _| true, driver.rmd(&user, "/team-bucket/scratch"))
            .await
            .expect_err("RMD must fail closed when list_objects_v2 errors");
        assert_eq!(err.kind(), ErrorKind::PermanentFileNotAvailable);
        assert!(backend.delete_object_calls().is_empty());

        // Empty subdirectory removes only its marker under ListBucket + DeleteObject
        // without needing DeleteBucket.
        backend.queue_list_objects_v2_ok_with_keys(&["scratch/"]);
        with_test_auth_override(
            |action, bucket, object| {
                bucket == "team-bucket"
                    && matches!(action, S3Action::ListBucket | S3Action::DeleteObject)
                    && object == Some("scratch/")
            },
            driver.rmd(&user, "/team-bucket/scratch"),
        )
        .await
        .expect("RMD of an empty subdirectory succeeds");

        // Trailing-slash path removes the same normalized marker.
        backend.queue_list_objects_v2_ok_with_keys(&["scratch/"]);
        with_test_auth_override(|_, _, _| true, driver.rmd(&user, "/team-bucket/scratch/"))
            .await
            .expect("RMD with trailing slash succeeds");

        let listed = backend.list_objects_calls();
        assert_eq!(listed.len(), 6);
        for input in &listed {
            assert_eq!(
                input.prefix.as_deref(),
                Some("scratch/"),
                "listing must be confined to the directory prefix"
            );
            assert_eq!(input.delimiter.as_deref(), Some("/"));
            assert_eq!(input.max_keys, Some(2));
        }
        let deleted: Vec<String> = backend.delete_object_calls().into_iter().map(|c| c.key).collect();
        assert_eq!(deleted, vec!["scratch/".to_string(), "scratch/".to_string()]);
        assert!(
            backend.delete_bucket_calls().is_empty(),
            "RMD below the bucket root must never delete the bucket"
        );
    }

    /// MKD on a path below a bucket creates a zero-byte directory marker object
    /// under `s3:PutObject` instead of creating a bucket, and LIST hides that
    /// marker inside the directory.
    #[tokio::test]
    async fn ftps_mkd_subdirectory_creates_marker_not_bucket() {
        use super::FtpsDriver;
        use crate::common::dummy_storage::DummyBackend;
        use crate::common::gateway::{S3Action, with_test_auth_override};
        use crate::common::session::{Protocol, test_session};
        use unftp_core::storage::StorageBackend as _;

        let backend = DummyBackend::new();
        backend.capture_upload_bodies();
        backend.queue_put_object_ok();
        backend.queue_create_bucket_ok();

        let driver = FtpsDriver::new(backend.clone());
        let user = super::super::server::FtpsUser {
            username: "member".to_string(),
            name: None,
            session_context: test_session(Protocol::Ftps),
        };

        with_test_auth_override(
            |action, bucket, object| {
                matches!(action, S3Action::PutObject) && bucket == "team-bucket" && object == Some("newdir/")
            },
            driver.mkd(&user, "/team-bucket/newdir"),
        )
        .await
        .expect("MKD of a subdirectory succeeds under s3:PutObject");

        let puts = backend.put_object_calls();
        assert_eq!(puts.len(), 1);
        assert_eq!((puts[0].bucket.as_str(), puts[0].key.as_str()), ("team-bucket", "newdir/"));
        assert_eq!(backend.upload_bodies(), vec![Vec::<u8>::new()]);
        assert!(
            backend.create_bucket_calls().is_empty(),
            "MKD below a bucket must never call create_bucket"
        );
        assert!(
            with_test_auth_override(|_, _, _| false, driver.mkd(&user, "/team-bucket/denied"))
                .await
                .is_err(),
            "MKD must fail closed when s3:PutObject is denied"
        );

        // Listing the directory must not present its own marker as a file.
        backend.queue_list_objects_v2_ok_with_keys(&["newdir/", "newdir/a.txt"]);
        let entries = with_test_auth_override(|_, _, _| true, driver.list(&user, "/team-bucket/newdir"))
            .await
            .expect("LIST of the new directory succeeds");
        let names: Vec<_> = entries.iter().map(|e| e.path.to_string_lossy().into_owned()).collect();
        assert_eq!(names, vec!["a.txt".to_string()]);
    }

    /// DELE must refuse bucket paths with or without a trailing slash so it can
    /// never silently wipe a whole bucket.
    #[tokio::test]
    async fn ftps_del_refuses_bucket_paths() {
        use super::FtpsDriver;
        use crate::common::dummy_storage::DummyBackend;
        use crate::common::gateway::with_test_auth_override;
        use crate::common::session::{Protocol, test_session};
        use unftp_core::storage::StorageBackend as _;

        let backend = DummyBackend::new();
        backend.queue_list_objects_v2_ok_with_keys(&["reports/q3.pdf"]);
        backend.queue_delete_object_ok();
        backend.queue_delete_bucket_ok();

        let driver = FtpsDriver::new(backend.clone());
        let user = super::super::server::FtpsUser {
            username: "member".to_string(),
            name: None,
            session_context: test_session(Protocol::Ftps),
        };

        for path in ["/team-bucket/", "/team-bucket", "/"] {
            let result = with_test_auth_override(|_, _, _| true, driver.del(&user, path)).await;
            assert!(result.is_err(), "DELE on bucket/root path {path:?} must be refused");
        }
        assert!(backend.delete_object_calls().is_empty());
        assert!(backend.delete_bucket_calls().is_empty());
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
        client::s3::{MultipartRequest, StorageBackend},
        gateway::S3Action,
        session::SessionContext,
    };
    use bytes::Bytes;
    use rustfs_gateway_stream::ByteStream;
    use rustfs_gateway_types::dto::*;
    use rustfs_gateway_types::{BucketName, ObjectKey};
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

    async fn authorize<S: StorageBackend>(
        storage: &S,
        session: &SessionContext,
        action: S3Action,
        bucket: &str,
        key: &str,
    ) -> Result<()> {
        storage
            .authorize_operation(session, &action, bucket, Some(key))
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

    fn body(part: Vec<u8>) -> ByteStream {
        ByteStream::from_bytes(Bytes::from(part))
    }

    // Keep the upload ID alive across all cancellable awaits after CreateMultipartUpload.
    // A process/runtime crash still requires an AbortIncompleteMultipartUpload lifecycle rule.
    struct PendingUpload<S: StorageBackend + 'static> {
        storage: Arc<S>,
        session: SessionContext,
        input: Option<MultipartRequest<AbortMultipartUploadInput>>,
    }

    impl<S: StorageBackend + 'static> PendingUpload<S> {
        async fn abort(&mut self) {
            if let Some(input) = self.input.as_ref() {
                abort_upload(&*self.storage, &self.session, input.clone()).await;
                self.input = None;
            }
        }
    }

    async fn abort_upload<S: StorageBackend>(
        storage: &S,
        session: &SessionContext,
        input: MultipartRequest<AbortMultipartUploadInput>,
    ) {
        let cleanup = async {
            authorize(
                storage,
                session,
                S3Action::AbortMultipartUpload,
                input.input().bucket.as_str(),
                input.input().key.as_str(),
            )
            .await?;
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
            authorize(storage.as_ref(), session, S3Action::PutObject, bucket, key).await?;
            let input = PutObjectInput::builder()
                .bucket(BucketName::new(bucket.to_owned()).map_err(upload_error)?)
                .key(ObjectKey::new(key.to_owned()).map_err(upload_error)?)
                .content_length(i64::try_from(size).map_err(upload_error)?)
                .body(body(part))
                .build();
            storage.put_object(input, session.credentials()).await.map_err(upload_error)?;
            return Ok(size);
        }

        authorize(storage.as_ref(), session, S3Action::CreateMultipartUpload, bucket, key).await?;
        let input = CreateMultipartUploadInput::builder()
            .bucket(BucketName::new(bucket.to_owned()).map_err(upload_error)?)
            .key(ObjectKey::new(key.to_owned()).map_err(upload_error)?)
            .build();
        let output = storage
            .create_multipart_upload(input, session.credentials())
            .await
            .map_err(upload_error)?;
        let upload_id = if output.upload_id.is_empty() {
            return Err(upload_error("CreateMultipartUpload returned no upload ID"));
        } else {
            output.upload_id
        };
        let abort = AbortMultipartUploadInput::builder()
            .bucket(BucketName::new(bucket.to_owned()).map_err(upload_error)?)
            .key(ObjectKey::new(key.to_owned()).map_err(upload_error)?)
            .build();
        let mut pending = PendingUpload {
            storage: Arc::clone(&storage),
            session: session.clone(),
            input: Some(MultipartRequest::abort(abort, upload_id.clone())),
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
                authorize(storage.as_ref(), session, S3Action::UploadPart, bucket, key).await?;
                let input = UploadPartInput::builder()
                    .bucket(BucketName::new(bucket.to_owned()).map_err(upload_error)?)
                    .key(ObjectKey::new(key.to_owned()).map_err(upload_error)?)
                    .part_number(number)
                    .content_length(length)
                    .body(body(part))
                    .build();
                let output = storage
                    .upload_part(MultipartRequest::upload_part(input, upload_id.clone()), session.credentials())
                    .await
                    .map_err(upload_error)?;
                let etag = if output.e_tag == rustfs_gateway_types::ETag::default() {
                    return Err(upload_error("UploadPart returned no ETag"));
                } else {
                    output.e_tag
                };
                parts.push(CompletedPart {
                    e_tag: Some(etag),
                    part_number: number,
                    ..Default::default()
                });
                part = read_part(&mut reader).await?;
            }
            authorize(storage.as_ref(), session, S3Action::CompleteMultipartUpload, bucket, key).await?;
            let input = CompleteMultipartUploadInput::builder()
                .bucket(BucketName::new(bucket.to_owned()).map_err(upload_error)?)
                .key(ObjectKey::new(key.to_owned()).map_err(upload_error)?)
                .multipart_upload(CompletedMultipartUpload { parts })
                .build();
            storage
                .complete_multipart_upload(MultipartRequest::complete(input, upload_id), session.credentials())
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
                assert_eq!(backend.abort_multipart_calls()[0].upload_id, "<opaque>");
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
