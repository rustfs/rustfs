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
use crate::common::gateway::{S3Action, authorize_operation};
use crate::common::session::SessionContext;
use bytes::Bytes;
use dav_server::davpath::DavPath;
use dav_server::fs::{
    DavDirEntry, DavFile, DavFileSystem, DavMetaData, FsError, FsFuture, FsResult, FsStream, OpenOptions, ReadDirMeta,
};
use futures_util::{FutureExt, StreamExt, stream};
use percent_encoding::percent_decode_str;
use rustfs_utils::path;
use s3s::dto::*;
use std::fmt::Debug;
use std::io::SeekFrom;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::RwLock;
use tracing::{debug, error};

/// Convert s3s ETag enum to string
fn etag_to_string(etag: &ETag) -> String {
    match etag {
        ETag::Strong(s) => s.clone(),
        ETag::Weak(s) => s.clone(),
    }
}

/// WebDAV metadata implementation
#[derive(Debug, Clone)]
pub struct WebDavMetaData {
    /// File size in bytes
    pub size: u64,
    /// Modification time
    pub modified: SystemTime,
    /// Creation time
    pub created: SystemTime,
    /// Whether this is a directory
    pub is_dir: bool,
    /// ETag (optional)
    pub etag: Option<String>,
    /// Content type (optional)
    pub content_type: Option<String>,
}

impl DavMetaData for WebDavMetaData {
    fn len(&self) -> u64 {
        self.size
    }

    fn modified(&self) -> FsResult<SystemTime> {
        Ok(self.modified)
    }

    fn is_dir(&self) -> bool {
        self.is_dir
    }

    fn created(&self) -> FsResult<SystemTime> {
        Ok(self.created)
    }

    fn etag(&self) -> Option<String> {
        self.etag.clone()
    }
}

/// WebDAV directory entry implementation
#[derive(Debug, Clone)]
pub struct WebDavDirEntry {
    /// Entry name
    pub name: String,
    /// Entry metadata
    pub metadata: WebDavMetaData,
}

impl DavDirEntry for WebDavDirEntry {
    fn name(&self) -> Vec<u8> {
        self.name.as_bytes().to_vec()
    }

    fn metadata(&self) -> FsFuture<'_, Box<dyn DavMetaData>> {
        let meta = self.metadata.clone();
        async move { Ok(Box::new(meta) as Box<dyn DavMetaData>) }.boxed()
    }
}

/// WebDAV file implementation for reading/writing
pub struct WebDavFile<S>
where
    S: S3StorageBackend + Debug + Clone + Send + Sync + 'static,
{
    /// Storage backend
    storage: S,
    /// Session context for authorization
    session_context: Arc<SessionContext>,
    /// Bucket name
    bucket: String,
    /// Object key
    key: String,
    /// Current position in file (using RwLock for interior mutability in async)
    position: Arc<RwLock<u64>>,
    /// File size (known after metadata fetch)
    size: Option<u64>,
    /// Write buffer for accumulating data before upload
    write_buffer: Arc<RwLock<Vec<u8>>>,
    /// Whether we're in write mode
    is_write: bool,
    /// Maximum body size for chunked transfers
    max_body_size: u64,
}

impl<S> WebDavFile<S>
where
    S: S3StorageBackend + Debug + Clone + Send + Sync + 'static,
{
    /// Default maximum body size (5GB)
    pub const DEFAULT_MAX_BODY_SIZE: u64 = 5 * 1024 * 1024 * 1024;

    pub fn new(storage: S, session_context: Arc<SessionContext>, bucket: String, key: String, is_write: bool) -> Self {
        Self::with_max_body_size(storage, session_context, bucket, key, is_write, Self::DEFAULT_MAX_BODY_SIZE)
    }

    pub fn with_max_body_size(
        storage: S,
        session_context: Arc<SessionContext>,
        bucket: String,
        key: String,
        is_write: bool,
        max_body_size: u64,
    ) -> Self {
        Self {
            storage,
            session_context,
            bucket,
            key,
            position: Arc::new(RwLock::new(0)),
            size: None,
            write_buffer: Arc::new(RwLock::new(Vec::new())),
            is_write,
            max_body_size,
        }
    }
}

impl<S> Debug for WebDavFile<S>
where
    S: S3StorageBackend + Debug + Clone + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebDavFile")
            .field("bucket", &self.bucket)
            .field("key", &self.key)
            .field("position", &"<locked>")
            .finish()
    }
}

impl<S> DavFile for WebDavFile<S>
where
    S: S3StorageBackend + Debug + Clone + Send + Sync + 'static,
{
    fn metadata(&mut self) -> FsFuture<'_, Box<dyn DavMetaData>> {
        let storage = self.storage.clone();
        let session_context = self.session_context.clone();
        let bucket = self.bucket.clone();
        let key = self.key.clone();

        async move {
            match storage
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
                    let modified = output
                        .last_modified
                        .map(|dt| {
                            let offset_dt: time::OffsetDateTime = dt.into();
                            SystemTime::from(offset_dt)
                        })
                        .unwrap_or_else(SystemTime::now);

                    Ok(Box::new(WebDavMetaData {
                        size,
                        modified,
                        created: modified,
                        is_dir: false,
                        etag: output.e_tag.as_ref().map(etag_to_string),
                        content_type: output.content_type.map(|c| c.to_string()),
                    }) as Box<dyn DavMetaData>)
                }
                Err(e) => {
                    error!("Failed to get file metadata for {}/{}: {}", bucket, key, e);
                    Err(FsError::NotFound)
                }
            }
        }
        .boxed()
    }

    fn write_buf(&mut self, mut buf: Box<dyn bytes::Buf + Send>) -> FsFuture<'_, ()> {
        let write_buffer = self.write_buffer.clone();
        let max_body_size = self.max_body_size;
        async move {
            let mut buffer = write_buffer.write().await;
            // Consume all chunks from the buffer, not just the first one
            while buf.has_remaining() {
                let chunk = buf.chunk();
                // Check size limit before extending
                if buffer.len() as u64 + chunk.len() as u64 > max_body_size {
                    return Err(FsError::TooLarge);
                }
                buffer.extend_from_slice(chunk);
                buf.advance(chunk.len());
            }
            Ok(())
        }
        .boxed()
    }

    fn write_bytes(&mut self, buf: Bytes) -> FsFuture<'_, ()> {
        let write_buffer = self.write_buffer.clone();
        let max_body_size = self.max_body_size;
        async move {
            let mut buffer = write_buffer.write().await;
            // Check size limit before extending
            if buffer.len() as u64 + buf.len() as u64 > max_body_size {
                return Err(FsError::TooLarge);
            }
            buffer.extend_from_slice(&buf);
            Ok(())
        }
        .boxed()
    }

    fn read_bytes(&mut self, count: usize) -> FsFuture<'_, Bytes> {
        let storage = self.storage.clone();
        let session_context = self.session_context.clone();
        let bucket = self.bucket.clone();
        let key = self.key.clone();
        let position = self.position.clone();

        async move {
            let start_pos = *position.read().await;
            match storage
                .get_object_range(
                    &bucket,
                    &key,
                    &session_context.principal.user_identity.credentials.access_key,
                    &session_context.principal.user_identity.credentials.secret_key,
                    start_pos,
                    count as u64,
                )
                .await
            {
                Ok(output) => {
                    if let Some(body) = output.body {
                        let mut data = Vec::new();
                        let mut stream = body;
                        while let Some(chunk_result) = stream.next().await {
                            match chunk_result {
                                Ok(bytes) => data.extend_from_slice(&bytes),
                                Err(e) => {
                                    error!("Error reading stream: {}", e);
                                    return Err(FsError::GeneralFailure);
                                }
                            }
                        }
                        // Update position after successful read
                        let bytes_read = data.len() as u64;
                        *position.write().await = start_pos + bytes_read;
                        Ok(Bytes::from(data))
                    } else {
                        Ok(Bytes::new())
                    }
                }
                Err(e) => {
                    error!("Failed to read bytes from {}/{}: {}", bucket, key, e);
                    Err(FsError::GeneralFailure)
                }
            }
        }
        .boxed()
    }

    fn seek(&mut self, pos: SeekFrom) -> FsFuture<'_, u64> {
        let position = self.position.clone();
        let size = self.size;

        async move {
            let current_pos = *position.read().await;
            let new_pos = match pos {
                SeekFrom::Start(offset) => offset,
                SeekFrom::End(offset) => {
                    let file_size = size.unwrap_or(0);
                    if offset < 0 {
                        file_size.saturating_sub((-offset) as u64)
                    } else {
                        file_size + offset as u64
                    }
                }
                SeekFrom::Current(offset) => {
                    if offset < 0 {
                        current_pos.saturating_sub((-offset) as u64)
                    } else {
                        current_pos + offset as u64
                    }
                }
            };
            // Persist the new position
            *position.write().await = new_pos;
            Ok(new_pos)
        }
        .boxed()
    }

    fn flush(&mut self) -> FsFuture<'_, ()> {
        let storage = self.storage.clone();
        let session_context = self.session_context.clone();
        let bucket = self.bucket.clone();
        let key = self.key.clone();
        let write_buffer = self.write_buffer.clone();
        let is_write = self.is_write;

        async move {
            if !is_write {
                return Ok(());
            }

            // Use write lock and std::mem::take to avoid cloning the buffer
            let mut buffer = write_buffer.write().await;
            let file_size = buffer.len();
            let data_bytes = Bytes::from(std::mem::take(&mut *buffer));
            drop(buffer);

            let stream = stream::once(async move { Ok::<Bytes, std::io::Error>(data_bytes) });
            let streaming_blob = StreamingBlob::wrap(stream);

            let put_input = PutObjectInput::builder()
                .bucket(bucket.clone())
                .key(key.clone())
                .content_length(Some(file_size as i64))
                .body(Some(streaming_blob))
                .build()
                .map_err(|_| FsError::GeneralFailure)?;

            match storage
                .put_object(
                    put_input,
                    &session_context.principal.user_identity.credentials.access_key,
                    &session_context.principal.user_identity.credentials.secret_key,
                )
                .await
            {
                Ok(_) => {
                    debug!("Successfully flushed {} bytes to {}/{}", file_size, bucket, key);
                    // Buffer already cleared by std::mem::take above
                    Ok(())
                }
                Err(e) => {
                    error!("Failed to flush to {}/{}: {}", bucket, key, e);
                    Err(FsError::GeneralFailure)
                }
            }
        }
        .boxed()
    }
}

/// WebDAV filesystem driver implementation
pub struct WebDavDriver<S>
where
    S: S3StorageBackend + Debug + Clone + Send + Sync + 'static,
{
    /// Storage backend for S3 operations
    storage: S,
    /// Session context for authorization
    session_context: Arc<SessionContext>,
}

impl<S> Debug for WebDavDriver<S>
where
    S: S3StorageBackend + Debug + Clone + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebDavDriver").field("storage", &"StorageBackend").finish()
    }
}

impl<S> Clone for WebDavDriver<S>
where
    S: S3StorageBackend + Debug + Clone + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            session_context: self.session_context.clone(),
        }
    }
}

impl<S> WebDavDriver<S>
where
    S: S3StorageBackend + Debug + Clone + Send + Sync + 'static,
{
    /// Create a new WebDAV driver with the given storage backend and session context
    pub fn new(storage: S, session_context: Arc<SessionContext>) -> Self {
        Self {
            storage,
            session_context,
        }
    }

    /// Parse WebDAV path to bucket and object key
    fn parse_path(&self, path: &DavPath) -> Result<(String, Option<String>), FsError> {
        let path_str = path.as_url_string();
        let decoded_path = percent_decode_str(&path_str)
            .decode_utf8()
            .map_err(|_| FsError::GeneralFailure)?;
        let cleaned_path = path::clean(&decoded_path);
        let (bucket, object) = path::path_to_bucket_object(&cleaned_path);

        if bucket.is_empty() {
            return Ok((String::new(), None));
        }

        let key = if object.is_empty() { None } else { Some(object) };
        Ok((bucket, key))
    }

    /// Check if path is root
    fn is_root(&self, path: &DavPath) -> bool {
        let path_str = path.as_url_string();
        path_str == "/" || path_str.is_empty()
    }

    /// List all buckets (for root path)
    async fn list_buckets(&self) -> FsResult<Vec<WebDavDirEntry>> {
        match authorize_operation(&self.session_context, &S3Action::ListBuckets, "", None).await {
            Ok(_) => {}
            Err(_e) => {
                return Err(FsError::Forbidden);
            }
        }

        match self
            .storage
            .list_buckets(
                &self.session_context.principal.user_identity.credentials.access_key,
                &self.session_context.principal.user_identity.credentials.secret_key,
            )
            .await
        {
            Ok(output) => {
                let mut entries = Vec::new();
                if let Some(buckets) = output.buckets {
                    for bucket in buckets {
                        if let Some(ref bucket_name) = bucket.name {
                            let modified = bucket
                                .creation_date
                                .map(|dt| {
                                    let offset_dt: time::OffsetDateTime = dt.into();
                                    SystemTime::from(offset_dt)
                                })
                                .unwrap_or_else(SystemTime::now);

                            entries.push(WebDavDirEntry {
                                name: bucket_name.clone(),
                                metadata: WebDavMetaData {
                                    size: 0,
                                    modified,
                                    created: modified,
                                    is_dir: true,
                                    etag: None,
                                    content_type: None,
                                },
                            });
                        }
                    }
                }
                Ok(entries)
            }
            Err(e) => {
                error!("Failed to list buckets: {}", e);
                Err(FsError::GeneralFailure)
            }
        }
    }

    /// List objects in a bucket
    async fn list_objects(&self, bucket: &str, prefix: Option<&str>) -> FsResult<Vec<WebDavDirEntry>> {
        // Authorize the operation
        authorize_operation(&self.session_context, &S3Action::ListBucket, bucket, prefix)
            .await
            .map_err(|_| FsError::Forbidden)?;

        let prefix_with_slash = prefix.map(|p| if p.ends_with('/') { p.to_string() } else { format!("{}/", p) });

        let list_input = ListObjectsV2Input::builder()
            .bucket(bucket.to_string())
            .prefix(prefix_with_slash.clone())
            .delimiter(Some("/".to_string()))
            .build()
            .map_err(|_| FsError::GeneralFailure)?;

        match self
            .storage
            .list_objects_v2(
                list_input,
                &self.session_context.principal.user_identity.credentials.access_key,
                &self.session_context.principal.user_identity.credentials.secret_key,
            )
            .await
        {
            Ok(output) => {
                let mut entries = Vec::new();

                // Add files (objects)
                if let Some(objects) = output.contents {
                    for obj in objects {
                        if let Some(key) = obj.key {
                            // Filter: only show files directly in current directory
                            let should_show = if prefix.is_none() {
                                !key.contains('/')
                            } else {
                                key.starts_with(&prefix_with_slash.clone().unwrap_or_default())
                            };

                            if !should_show {
                                continue;
                            }

                            let filename = std::path::PathBuf::from(key.as_str())
                                .file_name()
                                .map(|n| n.to_string_lossy().to_string())
                                .unwrap_or_else(|| key.clone());

                            let size = obj.size.unwrap_or(0) as u64;
                            let modified = obj
                                .last_modified
                                .map(|dt| {
                                    let offset_dt: time::OffsetDateTime = dt.into();
                                    SystemTime::from(offset_dt)
                                })
                                .unwrap_or_else(SystemTime::now);

                            entries.push(WebDavDirEntry {
                                name: filename,
                                metadata: WebDavMetaData {
                                    size,
                                    modified,
                                    created: modified,
                                    is_dir: false,
                                    etag: obj.e_tag.as_ref().map(etag_to_string),
                                    content_type: None,
                                },
                            });
                        }
                    }
                }

                // Add directories (common prefixes)
                if let Some(common_prefixes) = output.common_prefixes {
                    for prefix in common_prefixes {
                        if let Some(prefix_str) = prefix.prefix {
                            let dir_name = std::path::PathBuf::from(prefix_str.as_str().trim_end_matches('/'))
                                .file_name()
                                .map(|n| n.to_string_lossy().to_string())
                                .unwrap_or_else(|| prefix_str.clone());

                            entries.push(WebDavDirEntry {
                                name: dir_name,
                                metadata: WebDavMetaData {
                                    size: 0,
                                    modified: SystemTime::now(),
                                    created: SystemTime::now(),
                                    is_dir: true,
                                    etag: None,
                                    content_type: None,
                                },
                            });
                        }
                    }
                }

                Ok(entries)
            }
            Err(e) => {
                error!("Failed to list objects in {}: {}", bucket, e);
                Err(FsError::GeneralFailure)
            }
        }
    }

    /// Recursively delete all objects in a bucket, then delete the bucket itself
    async fn delete_bucket_recursively(&self, bucket: &str) -> FsResult<()> {
        // First, delete all objects in the bucket (with pagination)
        let mut continuation_token = None;
        loop {
            let mut list_input = ListObjectsV2Input::builder().bucket(bucket.to_string());

            if let Some(token) = continuation_token {
                list_input = list_input.continuation_token(token);
            }

            let list_input = list_input.build().map_err(|_| FsError::GeneralFailure)?;

            if let Ok(output) = self
                .storage
                .list_objects_v2(
                    list_input,
                    &self.session_context.principal.user_identity.credentials.access_key,
                    &self.session_context.principal.user_identity.credentials.secret_key,
                )
                .await
            {
                // Delete all objects in this page
                if let Some(objects) = output.contents {
                    for obj in objects {
                        if let Some(obj_key) = obj.key {
                            let _ = self
                                .storage
                                .delete_object(
                                    bucket,
                                    &obj_key,
                                    &self.session_context.principal.user_identity.credentials.access_key,
                                    &self.session_context.principal.user_identity.credentials.secret_key,
                                )
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
        match self
            .storage
            .delete_bucket(
                bucket,
                &self.session_context.principal.user_identity.credentials.access_key,
                &self.session_context.principal.user_identity.credentials.secret_key,
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(e) if e.to_string().contains("NoSuchBucket") => Ok(()),
            Err(e) => {
                error!("Failed to delete bucket '{}': {}", bucket, e);
                Err(FsError::GeneralFailure)
            }
        }
    }
}

impl<S> DavFileSystem for WebDavDriver<S>
where
    S: S3StorageBackend + Debug + Clone + Send + Sync + 'static,
{
    fn open<'a>(&'a self, path: &'a DavPath, options: OpenOptions) -> FsFuture<'a, Box<dyn DavFile>> {
        let storage = self.storage.clone();
        let session_context = self.session_context.clone();

        async move {
            let (bucket, key) = self.parse_path(path)?;

            if bucket.is_empty() {
                return Err(FsError::Forbidden);
            }

            let key = key.ok_or(FsError::Forbidden)?; // Cannot open a bucket as a file

            // Check authorization based on operation type
            if options.write || options.create || options.create_new || options.append {
                authorize_operation(&session_context, &S3Action::PutObject, &bucket, Some(&key))
                    .await
                    .map_err(|_| FsError::Forbidden)?;
            } else {
                authorize_operation(&session_context, &S3Action::GetObject, &bucket, Some(&key))
                    .await
                    .map_err(|_| FsError::Forbidden)?;
            }

            let is_write = options.write || options.create || options.create_new || options.append;
            let file = WebDavFile::new(storage, session_context, bucket, key, is_write);

            Ok(Box::new(file) as Box<dyn DavFile>)
        }
        .boxed()
    }

    fn read_dir<'a>(&'a self, path: &'a DavPath, _meta: ReadDirMeta) -> FsFuture<'a, FsStream<Box<dyn DavDirEntry>>> {
        async move {
            let entries = if self.is_root(path) {
                self.list_buckets().await?
            } else {
                let (bucket, prefix) = self.parse_path(path)?;
                if bucket.is_empty() {
                    self.list_buckets().await?
                } else {
                    self.list_objects(&bucket, prefix.as_deref()).await?
                }
            };

            let stream = stream::iter(entries.into_iter().map(|e| Ok(Box::new(e) as Box<dyn DavDirEntry>)));
            Ok(Box::pin(stream) as FsStream<Box<dyn DavDirEntry>>)
        }
        .boxed()
    }

    fn metadata<'a>(&'a self, path: &'a DavPath) -> FsFuture<'a, Box<dyn DavMetaData>> {
        async move {
            if self.is_root(path) {
                return Ok(Box::new(WebDavMetaData {
                    size: 0,
                    modified: SystemTime::now(),
                    created: SystemTime::now(),
                    is_dir: true,
                    etag: None,
                    content_type: None,
                }) as Box<dyn DavMetaData>);
            }

            let (bucket, key) = self.parse_path(path)?;

            if bucket.is_empty() {
                return Ok(Box::new(WebDavMetaData {
                    size: 0,
                    modified: SystemTime::now(),
                    created: SystemTime::now(),
                    is_dir: true,
                    etag: None,
                    content_type: None,
                }) as Box<dyn DavMetaData>);
            }

            if let Some(key) = key {
                // Get object metadata
                match self
                    .storage
                    .head_object(
                        &bucket,
                        &key,
                        &self.session_context.principal.user_identity.credentials.access_key,
                        &self.session_context.principal.user_identity.credentials.secret_key,
                    )
                    .await
                {
                    Ok(output) => {
                        let size = output.content_length.unwrap_or(0) as u64;
                        let modified = output
                            .last_modified
                            .map(|dt| {
                                let offset_dt: time::OffsetDateTime = dt.into();
                                SystemTime::from(offset_dt)
                            })
                            .unwrap_or_else(SystemTime::now);

                        Ok(Box::new(WebDavMetaData {
                            size,
                            modified,
                            created: modified,
                            is_dir: false,
                            etag: output.e_tag.as_ref().map(etag_to_string),
                            content_type: output.content_type.map(|c| c.to_string()),
                        }) as Box<dyn DavMetaData>)
                    }
                    Err(e) => {
                        // Check if it might be a "directory" (prefix)
                        let prefix = format!("{}/", key);
                        let list_input = ListObjectsV2Input::builder()
                            .bucket(bucket.clone())
                            .prefix(Some(prefix))
                            .max_keys(Some(1))
                            .build()
                            .map_err(|_| FsError::GeneralFailure)?;

                        match self
                            .storage
                            .list_objects_v2(
                                list_input,
                                &self.session_context.principal.user_identity.credentials.access_key,
                                &self.session_context.principal.user_identity.credentials.secret_key,
                            )
                            .await
                        {
                            Ok(output) => {
                                if output.contents.map(|c| !c.is_empty()).unwrap_or(false)
                                    || output.common_prefixes.map(|c| !c.is_empty()).unwrap_or(false)
                                {
                                    // It's a directory
                                    Ok(Box::new(WebDavMetaData {
                                        size: 0,
                                        modified: SystemTime::now(),
                                        created: SystemTime::now(),
                                        is_dir: true,
                                        etag: None,
                                        content_type: None,
                                    }) as Box<dyn DavMetaData>)
                                } else {
                                    debug!("Object not found: {}/{}: {}", bucket, key, e);
                                    Err(FsError::NotFound)
                                }
                            }
                            Err(_) => {
                                debug!("Object not found: {}/{}: {}", bucket, key, e);
                                Err(FsError::NotFound)
                            }
                        }
                    }
                }
            } else {
                // Get bucket metadata
                match self
                    .storage
                    .head_bucket(
                        &bucket,
                        &self.session_context.principal.user_identity.credentials.access_key,
                        &self.session_context.principal.user_identity.credentials.secret_key,
                    )
                    .await
                {
                    Ok(_) => Ok(Box::new(WebDavMetaData {
                        size: 0,
                        modified: SystemTime::now(),
                        created: SystemTime::now(),
                        is_dir: true,
                        etag: None,
                        content_type: None,
                    }) as Box<dyn DavMetaData>),
                    Err(e) => {
                        debug!("Bucket not found: {}: {}", bucket, e);
                        Err(FsError::NotFound)
                    }
                }
            }
        }
        .boxed()
    }

    fn create_dir<'a>(&'a self, path: &'a DavPath) -> FsFuture<'a, ()> {
        async move {
            let (bucket, key) = self.parse_path(path)?;

            if bucket.is_empty() {
                return Err(FsError::Forbidden);
            }

            if let Some(key_str) = key {
                // Creating a "directory" in S3 by creating a zero-byte object with trailing slash
                let dir_key = if key_str.ends_with('/') {
                    key_str.to_string()
                } else {
                    format!("{}/", key_str)
                };

                authorize_operation(&self.session_context, &S3Action::PutObject, &bucket, Some(&dir_key))
                    .await
                    .map_err(|_| FsError::Forbidden)?;

                // Create empty streaming blob for directory marker
                let stream = futures_util::stream::once(async { Ok::<Bytes, std::io::Error>(Bytes::new()) });
                let streaming_blob = s3s::dto::StreamingBlob::wrap(stream);

                let put_input = s3s::dto::PutObjectInput::builder()
                    .bucket(bucket.clone())
                    .key(dir_key.clone())
                    .content_length(Some(0))
                    .content_type(Some("application/x-directory".to_string()))
                    .body(Some(streaming_blob))
                    .build()
                    .map_err(|_| FsError::GeneralFailure)?;

                match self
                    .storage
                    .put_object(
                        put_input,
                        &self.session_context.principal.user_identity.credentials.access_key,
                        &self.session_context.principal.user_identity.credentials.secret_key,
                    )
                    .await
                {
                    Ok(_) => {
                        debug!("Successfully created directory '{}' in bucket '{}'", dir_key, bucket);
                        return Ok(());
                    }
                    Err(e) => {
                        error!("Failed to create directory '{}' in bucket '{}': {}", dir_key, bucket, e);
                        return Err(FsError::GeneralFailure);
                    }
                }
            }

            // Create bucket
            authorize_operation(&self.session_context, &S3Action::CreateBucket, &bucket, None)
                .await
                .map_err(|_| FsError::Forbidden)?;

            match self
                .storage
                .create_bucket(
                    &bucket,
                    &self.session_context.principal.user_identity.credentials.access_key,
                    &self.session_context.principal.user_identity.credentials.secret_key,
                )
                .await
            {
                Ok(_) => {
                    debug!("Successfully created bucket '{}'", bucket);
                    Ok(())
                }
                Err(e) => {
                    error!("Failed to create bucket '{}': {}", bucket, e);
                    Err(FsError::GeneralFailure)
                }
            }
        }
        .boxed()
    }

    fn remove_dir<'a>(&'a self, path: &'a DavPath) -> FsFuture<'a, ()> {
        async move {
            let (bucket, key) = self.parse_path(path)?;

            if bucket.is_empty() {
                return Err(FsError::Forbidden);
            }

            if let Some(prefix) = key {
                // Delete all objects with this prefix (subdirectory)
                let prefix_with_slash = if prefix.ends_with('/') {
                    prefix.to_string()
                } else {
                    format!("{}/", prefix)
                };

                authorize_operation(&self.session_context, &S3Action::DeleteObject, &bucket, Some(&prefix_with_slash))
                    .await
                    .map_err(|_| FsError::Forbidden)?;

                // List and delete all objects with this prefix
                let mut continuation_token = None;
                loop {
                    let mut list_input = ListObjectsV2Input::builder()
                        .bucket(bucket.clone())
                        .prefix(Some(prefix_with_slash.clone()));

                    if let Some(token) = continuation_token {
                        list_input = list_input.continuation_token(token);
                    }

                    let list_input = list_input.build().map_err(|_| FsError::GeneralFailure)?;

                    if let Ok(output) = self
                        .storage
                        .list_objects_v2(
                            list_input,
                            &self.session_context.principal.user_identity.credentials.access_key,
                            &self.session_context.principal.user_identity.credentials.secret_key,
                        )
                        .await
                    {
                        if let Some(objects) = output.contents {
                            for obj in objects {
                                if let Some(obj_key) = obj.key {
                                    let _ = self
                                        .storage
                                        .delete_object(
                                            &bucket,
                                            &obj_key,
                                            &self.session_context.principal.user_identity.credentials.access_key,
                                            &self.session_context.principal.user_identity.credentials.secret_key,
                                        )
                                        .await;
                                }
                            }
                        }

                        if !output.is_truncated.unwrap_or(false) {
                            break;
                        }
                        continuation_token = Some(output.next_continuation_token);
                    } else {
                        break;
                    }
                }

                // Also delete the directory marker itself
                let _ = self
                    .storage
                    .delete_object(
                        &bucket,
                        &prefix_with_slash,
                        &self.session_context.principal.user_identity.credentials.access_key,
                        &self.session_context.principal.user_identity.credentials.secret_key,
                    )
                    .await;

                return Ok(());
            }

            // Delete bucket
            authorize_operation(&self.session_context, &S3Action::DeleteBucket, &bucket, None)
                .await
                .map_err(|_| FsError::Forbidden)?;

            self.delete_bucket_recursively(&bucket).await
        }
        .boxed()
    }

    fn remove_file<'a>(&'a self, path: &'a DavPath) -> FsFuture<'a, ()> {
        async move {
            let (bucket, key) = self.parse_path(path)?;

            if bucket.is_empty() {
                return Err(FsError::Forbidden);
            }

            let key = key.ok_or(FsError::Forbidden)?;

            // Authorize delete object
            authorize_operation(&self.session_context, &S3Action::DeleteObject, &bucket, Some(&key))
                .await
                .map_err(|_| FsError::Forbidden)?;

            match self
                .storage
                .delete_object(
                    &bucket,
                    &key,
                    &self.session_context.principal.user_identity.credentials.access_key,
                    &self.session_context.principal.user_identity.credentials.secret_key,
                )
                .await
            {
                Ok(_) => {
                    debug!("Successfully deleted object '{}/{}'", bucket, key);
                    Ok(())
                }
                Err(e) => {
                    error!("Failed to delete object '{}/{}': {}", bucket, key, e);
                    Err(FsError::GeneralFailure)
                }
            }
        }
        .boxed()
    }

    fn rename<'a>(&'a self, from: &'a DavPath, to: &'a DavPath) -> FsFuture<'a, ()> {
        let storage = self.storage.clone();
        let session_context = self.session_context.clone();

        async move {
            let (src_bucket, src_key) = self.parse_path(from)?;
            let (dst_bucket, dst_key) = self.parse_path(to)?;

            if src_bucket.is_empty() || dst_bucket.is_empty() {
                return Err(FsError::Forbidden);
            }

            let src_key = src_key.ok_or(FsError::Forbidden)?;
            let dst_key = dst_key.ok_or(FsError::Forbidden)?;

            let access_key = &session_context.principal.user_identity.credentials.access_key;
            let secret_key = &session_context.principal.user_identity.credentials.secret_key;

            // Authorize read on source and write on destination
            authorize_operation(&session_context, &S3Action::GetObject, &src_bucket, Some(&src_key))
                .await
                .map_err(|_| FsError::Forbidden)?;
            authorize_operation(&session_context, &S3Action::PutObject, &dst_bucket, Some(&dst_key))
                .await
                .map_err(|_| FsError::Forbidden)?;

            // Get source object content
            match storage.get_object(&src_bucket, &src_key, access_key, secret_key, None).await {
                Ok(get_output) => {
                    // Read all content from source
                    let body_bytes = if let Some(body) = get_output.body {
                        let chunks: Vec<Result<Bytes, Box<dyn std::error::Error + Send + Sync>>> = body.collect().await;
                        let mut all_data = Vec::new();
                        for chunk in chunks {
                            match chunk {
                                Ok(data) => all_data.extend_from_slice(&data),
                                Err(e) => {
                                    error!("Failed to read source object body: {}", e);
                                    return Err(FsError::GeneralFailure);
                                }
                            }
                        }
                        Bytes::from(all_data)
                    } else {
                        Bytes::new()
                    };

                    let content_length = body_bytes.len() as i64;
                    let content_type = get_output.content_type.clone();

                    // Put object to destination
                    let stream = stream::once(async move { Ok::<Bytes, std::io::Error>(body_bytes) });
                    let streaming_blob = StreamingBlob::wrap(stream);

                    let mut put_builder = PutObjectInput::builder()
                        .bucket(dst_bucket.clone())
                        .key(dst_key.clone())
                        .content_length(Some(content_length))
                        .body(Some(streaming_blob));

                    if let Some(ref ct) = content_type {
                        put_builder = put_builder.content_type(Some(ct.clone()));
                    }

                    let put_input = put_builder.build().map_err(|_| FsError::GeneralFailure)?;

                    match storage.put_object(put_input, access_key, secret_key).await {
                        Ok(_) => {
                            debug!("Successfully copied '{}/{}' to '{}/{}", src_bucket, src_key, dst_bucket, dst_key);
                        }
                        Err(e) => {
                            error!("Failed to copy object to destination: {}", e);
                            return Err(FsError::GeneralFailure);
                        }
                    }

                    // Delete source object
                    match storage.delete_object(&src_bucket, &src_key, access_key, secret_key).await {
                        Ok(_) => {
                            debug!("Successfully deleted source '{}/{}' after rename", src_bucket, src_key);
                            Ok(())
                        }
                        Err(e) => {
                            error!("Failed to delete source object after rename: {}", e);
                            Err(FsError::GeneralFailure)
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to get source object '{}' in '{}': {}", src_key, src_bucket, e);
                    Err(FsError::GeneralFailure)
                }
            }
        }
        .boxed()
    }

    fn copy<'a>(&'a self, _from: &'a DavPath, _to: &'a DavPath) -> FsFuture<'a, ()> {
        // Could implement using S3 CopyObject, but not required for basic WebDAV
        async move { Err(FsError::NotImplemented) }.boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::WebDavDriver;
    use crate::common::client::s3::StorageBackend as S3StorageBackend;
    use crate::common::session::{Protocol, ProtocolPrincipal, SessionContext};
    use async_trait::async_trait;
    use dav_server::davpath::DavPath;
    use dav_server::fs::FsError;
    use rustfs_credentials::Credentials;
    use rustfs_policy::auth::UserIdentity;
    use s3s::dto::*;
    use std::fmt::{Debug, Formatter};
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::Arc;

    #[derive(Clone)]
    struct DummyStorage;

    impl Debug for DummyStorage {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            f.write_str("DummyStorage")
        }
    }

    #[async_trait]
    impl S3StorageBackend for DummyStorage {
        type Error = std::io::Error;

        async fn get_object(
            &self,
            _bucket: &str,
            _key: &str,
            _access_key: &str,
            _secret_key: &str,
            _start_pos: Option<u64>,
        ) -> Result<GetObjectOutput, Self::Error> {
            unreachable!("parse_path tests should not hit storage")
        }

        async fn get_object_range(
            &self,
            _bucket: &str,
            _key: &str,
            _access_key: &str,
            _secret_key: &str,
            _start_pos: u64,
            _length: u64,
        ) -> Result<GetObjectOutput, Self::Error> {
            unreachable!("parse_path tests should not hit storage")
        }

        async fn put_object(
            &self,
            _input: PutObjectInput,
            _access_key: &str,
            _secret_key: &str,
        ) -> Result<PutObjectOutput, Self::Error> {
            unreachable!("parse_path tests should not hit storage")
        }

        async fn delete_object(
            &self,
            _bucket: &str,
            _key: &str,
            _access_key: &str,
            _secret_key: &str,
        ) -> Result<DeleteObjectOutput, Self::Error> {
            unreachable!("parse_path tests should not hit storage")
        }

        async fn head_object(
            &self,
            _bucket: &str,
            _key: &str,
            _access_key: &str,
            _secret_key: &str,
        ) -> Result<HeadObjectOutput, Self::Error> {
            unreachable!("parse_path tests should not hit storage")
        }

        async fn head_bucket(
            &self,
            _bucket: &str,
            _access_key: &str,
            _secret_key: &str,
        ) -> Result<HeadBucketOutput, Self::Error> {
            unreachable!("parse_path tests should not hit storage")
        }

        async fn list_objects_v2(
            &self,
            _input: ListObjectsV2Input,
            _access_key: &str,
            _secret_key: &str,
        ) -> Result<ListObjectsV2Output, Self::Error> {
            unreachable!("parse_path tests should not hit storage")
        }

        async fn list_buckets(&self, _access_key: &str, _secret_key: &str) -> Result<ListBucketsOutput, Self::Error> {
            unreachable!("parse_path tests should not hit storage")
        }

        async fn create_bucket(
            &self,
            _bucket: &str,
            _access_key: &str,
            _secret_key: &str,
        ) -> Result<CreateBucketOutput, Self::Error> {
            unreachable!("parse_path tests should not hit storage")
        }

        async fn delete_bucket(
            &self,
            _bucket: &str,
            _access_key: &str,
            _secret_key: &str,
        ) -> Result<DeleteBucketOutput, Self::Error> {
            unreachable!("parse_path tests should not hit storage")
        }
    }

    fn driver() -> WebDavDriver<DummyStorage> {
        let identity = UserIdentity::new(Credentials {
            access_key: "ak".to_string(),
            secret_key: "sk".to_string(),
            ..Default::default()
        });
        let session_context = SessionContext::new(
            ProtocolPrincipal::new(Arc::new(identity)),
            Protocol::WebDav,
            IpAddr::V4(Ipv4Addr::LOCALHOST),
        );

        WebDavDriver::new(DummyStorage, Arc::new(session_context))
    }

    #[test]
    fn parse_path_decodes_url_encoded_object_names() {
        let driver = driver();
        let path = DavPath::new("/bucket/%E6%96%87%E4%BB%B6%20name.txt").expect("path should parse");

        let (bucket, key) = driver.parse_path(&path).expect("path should decode");

        assert_eq!(bucket, "bucket");
        assert_eq!(key.as_deref(), Some("文件 name.txt"));
    }

    #[test]
    fn parse_path_rejects_invalid_utf8_percent_encoding() {
        let driver = driver();
        let path = DavPath::new("/bucket/%FFreport.txt").expect("path should parse");

        let err = driver.parse_path(&path).expect_err("invalid utf8 should be rejected");

        assert_eq!(err, FsError::GeneralFailure);
    }
}
