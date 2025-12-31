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
use crate::protocols::gateway::authorize::authorize_operation;
use crate::protocols::gateway::error::map_s3_error_to_sftp_status;
use crate::protocols::session::context::SessionContext;
use futures::TryStreamExt;
use russh_sftp::protocol::{Attrs, Data, File, FileAttributes, Handle, Name, OpenFlags, Status, StatusCode, Version};
use russh_sftp::server::Handler;
use rustfs_utils::path;
use s3s::dto::{DeleteBucketInput, DeleteObjectInput, GetObjectInput, ListObjectsV2Input, PutObjectInput, StreamingBlob};
use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::fs::{File as TokioFile, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::RwLock;
use tracing::{debug, error, trace};
use uuid::Uuid;

const INITIAL_HANDLE_ID: u32 = 1;
const ROOT_PATH: &str = "/";
const CURRENT_DIR: &str = ".";
const PARENT_DIR: &str = "..";
const HANDLE_ID_PREFIX: &str = "handle_";
const PATH_SEPARATOR: &str = "/";
const PERMISSION_DENIED_PATH: &str = "..";
const DIR_MODE: u32 = 0o040000;
const FILE_MODE: u32 = 0o100000;
const DIR_PERMISSIONS: u32 = 0o755;
const FILE_PERMISSIONS: u32 = 0o644;

/// State associated with an open file handle
#[derive(Debug)]
enum HandleState {
    Read {
        path: String,
        bucket: String,
        key: String,
    },
    Write {
        path: String,
        bucket: String,
        key: String,
        temp_file_path: PathBuf,
        file_handle: Option<TokioFile>,
    },
    Dir {
        path: String,
        files: Vec<File>,
        offset: usize,
    },
}

#[derive(Clone)]
pub struct SftpHandler {
    session_context: SessionContext,
    handles: Arc<RwLock<HashMap<String, HandleState>>>,
    next_handle_id: Arc<AtomicU32>,
    temp_dir: PathBuf,
    current_dir: Arc<RwLock<String>>,
}

impl SftpHandler {
    pub fn new(session_context: SessionContext) -> Self {
        Self {
            session_context,
            handles: Arc::new(RwLock::new(HashMap::new())),
            next_handle_id: Arc::new(AtomicU32::new(INITIAL_HANDLE_ID)),
            temp_dir: std::env::temp_dir(),
            current_dir: Arc::new(RwLock::new(ROOT_PATH.to_string())),
        }
    }

    fn create_s3_client(&self) -> Result<ProtocolS3Client, StatusCode> {
        // Create FS instance (empty struct that accesses global ECStore)
        let fs = crate::storage::ecfs::FS {};
        let client = ProtocolS3Client::new(fs, self.session_context.access_key().to_string());
        Ok(client)
    }

    fn parse_path(&self, path_str: &str) -> Result<(String, Option<String>), StatusCode> {
        if path_str.contains(PERMISSION_DENIED_PATH) {
            return Err(StatusCode::PermissionDenied);
        }

        // Clean the path to normalize
        let cleaned_path = path::clean(path_str);

        let (bucket, object) = path::path_to_bucket_object(&cleaned_path);

        let key = if object.is_empty() { None } else { Some(object) };

        debug!(
            "SFTP parse_path - input: '{}', cleaned: '{}', bucket: '{}', key: {:?}",
            path_str, cleaned_path, bucket, key
        );
        Ok((bucket, key))
    }

    fn generate_handle_id(&self) -> String {
        let id = self.next_handle_id.fetch_add(1, Ordering::Relaxed);
        format!("{}{}", HANDLE_ID_PREFIX, id)
    }

    /// Convert relative path to absolute path based on current directory
    async fn resolve_path(&self, path_str: &str) -> String {
        let current = self.current_dir.read().await;

        if path_str.starts_with(PATH_SEPARATOR) {
            // Absolute path
            return path::clean(path_str).to_string();
        }

        // Relative path
        if path_str == CURRENT_DIR {
            current.clone()
        } else if path_str == PARENT_DIR {
            if *current == ROOT_PATH {
                ROOT_PATH.to_string()
            } else {
                let parent = std::path::Path::new(&*current)
                    .parent()
                    .map(|p| p.to_str().unwrap())
                    .unwrap_or(ROOT_PATH);
                path::clean(parent).to_string()
            }
        } else {
            // Join current directory with path
            let joined = if *current == ROOT_PATH {
                format!("{}{}", PATH_SEPARATOR, path_str.trim_start_matches(PATH_SEPARATOR))
            } else {
                format!(
                    "{}{}{}",
                    current.trim_end_matches(PATH_SEPARATOR),
                    PATH_SEPARATOR,
                    path_str.trim_start_matches(PATH_SEPARATOR)
                )
            };
            path::clean(&joined).to_string()
        }
    }

    async fn cleanup_state(&self, state: HandleState) {
        if let HandleState::Write { temp_file_path, .. } = state {
            let _ = tokio::fs::remove_file(temp_file_path).await;
        }
    }

    async fn do_stat(&self, path: String) -> Result<FileAttributes, StatusCode> {
        debug!("SFTP do_stat - input path: '{}'", path);

        let (bucket, key_opt) = self.parse_path(&path)?;

        if bucket.is_empty() {
            let mut attrs = FileAttributes::default();
            attrs.set_dir(true);
            attrs.size = Some(0);
            let current_mode = attrs.permissions.unwrap_or(0);
            attrs.permissions = Some(current_mode | DIR_MODE | DIR_PERMISSIONS);
            return Ok(attrs);
        }

        let action = if key_opt.is_none() {
            S3Action::HeadBucket
        } else {
            S3Action::HeadObject
        };

        debug!("SFTP do_stat - parsed bucket: '{}', key: {:?}, action: {:?}", bucket, key_opt, action);

        authorize_operation(&self.session_context, &action, &bucket, key_opt.as_deref())
            .await
            .map_err(|_| StatusCode::PermissionDenied)?;

        let s3_client = self.create_s3_client()?;

        match action {
            S3Action::HeadBucket => {
                let input = s3s::dto::HeadBucketInput {
                    bucket,
                    ..Default::default()
                };

                match s3_client.head_bucket(input).await {
                    Ok(_) => {
                        let mut attrs = FileAttributes::default();
                        attrs.set_dir(true);
                        attrs.size = Some(0);
                        attrs.permissions = Some(DIR_PERMISSIONS | DIR_MODE);
                        attrs.mtime = Some(0);
                        Ok(attrs)
                    }
                    Err(_) => Err(StatusCode::NoSuchFile),
                }
            }

            S3Action::HeadObject => {
                let key = key_opt.expect("key_opt should be Some for HeadObject action");
                let input = s3s::dto::HeadObjectInput {
                    bucket,
                    key,
                    ..Default::default()
                };

                match s3_client.head_object(input).await {
                    Ok(out) => {
                        let mut attrs = FileAttributes::default();
                        attrs.set_dir(false);
                        attrs.size = Some(out.content_length.unwrap_or(0) as u64);

                        if let Some(lm) = out.last_modified {
                            let dt = time::OffsetDateTime::from(lm);
                            attrs.mtime = Some(dt.unix_timestamp() as u32);
                        }

                        attrs.permissions = Some(FILE_PERMISSIONS | FILE_MODE);
                        Ok(attrs)
                    }
                    Err(_) => Err(StatusCode::NoSuchFile),
                }
            }

            _ => {
                error!("SFTP do_stat - Unexpected action type");
                Err(StatusCode::Failure)
            }
        }
    }
}

impl Handler for SftpHandler {
    type Error = StatusCode;

    fn unimplemented(&self) -> Self::Error {
        StatusCode::OpUnsupported
    }

    async fn init(&mut self, version: u32, _extensions: HashMap<String, String>) -> Result<Version, Self::Error> {
        trace!("SFTP Init version: {}", version);
        Ok(Version::new())
    }

    fn open(
        &mut self,
        id: u32,
        filename: String,
        pflags: OpenFlags,
        _attrs: FileAttributes,
    ) -> impl Future<Output = Result<Handle, Self::Error>> + Send {
        let this = self.clone();

        async move {
            debug!("SFTP Open: {} (flags: {:?})", filename, pflags);

            // Resolve relative path to absolute path
            let resolved_filename = this.resolve_path(&filename).await;

            let (bucket, key_opt) = this.parse_path(&resolved_filename)?;

            if bucket.is_empty() {
                return Err(StatusCode::PermissionDenied); // Cannot open root directory as file
            }

            let key = key_opt.ok_or(StatusCode::PermissionDenied)?; // Cannot open bucket as file

            let handle_id = this.generate_handle_id();
            let state;

            if pflags.contains(OpenFlags::WRITE) || pflags.contains(OpenFlags::CREATE) || pflags.contains(OpenFlags::TRUNCATE) {
                let action = S3Action::PutObject;
                authorize_operation(&this.session_context, &action, &bucket, Some(&key))
                    .await
                    .map_err(|_| StatusCode::PermissionDenied)?;

                if pflags.contains(OpenFlags::APPEND) {
                    return Err(StatusCode::OpUnsupported);
                }

                let temp_filename = format!("rustfs-sftp-{}.tmp", Uuid::new_v4());
                let temp_path = this.temp_dir.join(temp_filename);

                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&temp_path)
                    .await
                    .map_err(|e| {
                        error!("Failed to create temp file: {}", e);
                        StatusCode::Failure
                    })?;

                state = HandleState::Write {
                    path: filename.clone(),
                    bucket,
                    key,
                    temp_file_path: temp_path,
                    file_handle: Some(file),
                };
            } else {
                let action = S3Action::GetObject;
                authorize_operation(&this.session_context, &action, &bucket, Some(&key))
                    .await
                    .map_err(|_| StatusCode::PermissionDenied)?;

                state = HandleState::Read {
                    path: filename.clone(),
                    bucket,
                    key,
                };
            }

            this.handles.write().await.insert(handle_id.clone(), state);
            Ok(Handle { id, handle: handle_id })
        }
    }

    fn close(&mut self, id: u32, handle: String) -> impl Future<Output = Result<Status, Self::Error>> + Send {
        let this = self.clone();
        async move {
            let state = this.handles.write().await.remove(&handle);

            match state {
                Some(HandleState::Write {
                    bucket,
                    key,
                    temp_file_path,
                    mut file_handle,
                    ..
                }) => {
                    let mut file = file_handle.take().ok_or(StatusCode::Failure)?;

                    if let Err(e) = file.flush().await {
                        error!("Flush to disk failed: {}", e);
                        let _ = tokio::fs::remove_file(&temp_file_path).await;
                        return Err(StatusCode::Failure);
                    }

                    let metadata = file.metadata().await.map_err(|e| {
                        error!("Failed to get metadata: {}", e);
                        StatusCode::Failure
                    })?;
                    let file_size = metadata.len();

                    if let Err(e) = file.seek(std::io::SeekFrom::Start(0)).await {
                        error!("Seek temp file failed: {}", e);
                        let _ = tokio::fs::remove_file(&temp_file_path).await;
                        return Err(StatusCode::Failure);
                    }

                    let s3_client = match this.create_s3_client() {
                        Ok(c) => c,
                        Err(e) => {
                            let _ = tokio::fs::remove_file(&temp_file_path).await;
                            return Err(e);
                        }
                    };

                    let stream = tokio_util::io::ReaderStream::new(file);
                    let body = StreamingBlob::wrap(stream);

                    let input = PutObjectInput::builder()
                        .bucket(bucket.clone())
                        .key(key.clone())
                        .body(Option::from(body))
                        .content_length(Option::from(file_size as i64)) // 告诉 S3 文件多大
                        .build()
                        .unwrap();

                    let result = match s3_client.put_object(input).await {
                        Ok(_) => Status {
                            id,
                            status_code: StatusCode::Ok,
                            error_message: "Success".into(),
                            language_tag: "en".into(),
                        },
                        Err(e) => {
                            error!("S3 PutObject failed: {}", e);
                            let status_code = map_s3_error_to_sftp_status(&e);
                            return Err(status_code);
                        }
                    };

                    let _ = tokio::fs::remove_file(&temp_file_path).await;
                    Ok(result)
                }
                Some(state) => {
                    this.cleanup_state(state).await;
                    Ok(Status {
                        id,
                        status_code: StatusCode::Ok,
                        error_message: "Success".into(),
                        language_tag: "en".into(),
                    })
                }
                None => Err(StatusCode::NoSuchFile),
            }
        }
    }

    fn read(&mut self, id: u32, handle: String, offset: u64, len: u32) -> impl Future<Output = Result<Data, Self::Error>> + Send {
        let this = self.clone();
        async move {
            let (bucket, key) = {
                let guard = this.handles.read().await;
                match guard.get(&handle) {
                    Some(HandleState::Read { bucket, key, .. }) => (bucket.clone(), key.clone()),
                    Some(_) => return Err(StatusCode::OpUnsupported),
                    None => return Err(StatusCode::NoSuchFile),
                }
            };

            let range_end = offset + (len as u64) - 1;
            let mut builder = GetObjectInput::builder();
            builder.set_bucket(bucket);
            builder.set_key(key);
            if let Ok(range) = s3s::dto::Range::parse(&format!("bytes={}-{}", offset, range_end)) {
                builder.set_range(Some(range));
            }

            let s3_client = this.create_s3_client()?;
            let input = builder.build().map_err(|_| StatusCode::Failure)?;

            match s3_client.get_object(input).await {
                Ok(output) => {
                    let mut data = Vec::with_capacity(len as usize);
                    if let Some(body) = output.body {
                        let stream = body.map_err(std::io::Error::other);
                        let mut reader = tokio_util::io::StreamReader::new(stream);
                        let _ = reader.read_to_end(&mut data).await;
                    }
                    Ok(Data { id, data })
                }
                Err(e) => {
                    debug!("S3 Read failed: {}", e);
                    Ok(Data { id, data: Vec::new() })
                }
            }
        }
    }

    fn write(
        &mut self,
        id: u32,
        handle: String,
        offset: u64,
        data: Vec<u8>,
    ) -> impl Future<Output = Result<Status, Self::Error>> + Send {
        let this = self.clone();
        async move {
            let mut guard = this.handles.write().await;

            if let Some(HandleState::Write { file_handle, .. }) = guard.get_mut(&handle) {
                if let Some(file) = file_handle {
                    if let Err(e) = file.seek(std::io::SeekFrom::Start(offset)).await {
                        error!("File seek failed: {}", e);
                        return Err(StatusCode::Failure);
                    }

                    if let Err(e) = file.write_all(&data).await {
                        error!("File write failed: {}", e);
                        return Err(StatusCode::Failure);
                    }

                    Ok(Status {
                        id,
                        status_code: StatusCode::Ok,
                        error_message: "Success".into(),
                        language_tag: "en".into(),
                    })
                } else {
                    Err(StatusCode::Failure)
                }
            } else {
                Err(StatusCode::NoSuchFile)
            }
        }
    }

    fn lstat(&mut self, id: u32, path: String) -> impl Future<Output = Result<Attrs, Self::Error>> + Send {
        let this = self.clone();
        async move {
            let resolved = this.resolve_path(&path).await;
            let attrs = this.do_stat(resolved).await?;
            Ok(Attrs { id, attrs })
        }
    }

    fn fstat(&mut self, id: u32, handle: String) -> impl Future<Output = Result<Attrs, Self::Error>> + Send {
        let this = self.clone();
        async move {
            let path = {
                let guard = this.handles.read().await;
                match guard.get(&handle) {
                    Some(HandleState::Read { path, .. }) => path.clone(),
                    Some(HandleState::Write { path, .. }) => path.clone(),
                    Some(HandleState::Dir { path, .. }) => path.clone(),
                    None => return Err(StatusCode::NoSuchFile),
                }
            };
            let attrs = this.do_stat(path).await?;
            Ok(Attrs { id, attrs })
        }
    }

    fn opendir(&mut self, id: u32, path: String) -> impl Future<Output = Result<Handle, Self::Error>> + Send {
        let this = self.clone();
        async move {
            debug!("SFTP Opendir START: path='{}'", path);

            // Resolve relative path to absolute path
            let resolved_path = this.resolve_path(&path).await;
            debug!("SFTP Opendir - resolved path: '{}'", resolved_path);

            // Handle root directory case - list all buckets
            if resolved_path == "/" || resolved_path == "/." {
                debug!("SFTP Opendir - listing root directory (all buckets)");
                let action = S3Action::ListBuckets;
                authorize_operation(&this.session_context, &action, "", None)
                    .await
                    .map_err(|_| StatusCode::PermissionDenied)?;

                // List all buckets
                let s3_client = this.create_s3_client().inspect_err(|&e| {
                    error!("SFTP Opendir - failed to create S3 client: {}", e);
                })?;

                let input = s3s::dto::ListBucketsInput::builder()
                    .build()
                    .map_err(|_| StatusCode::Failure)?;

                let secret_key = &this.session_context.principal.user_identity.credentials.secret_key;
                let output = s3_client.list_buckets(input, secret_key).await.map_err(|e| {
                    error!("SFTP Opendir - failed to list buckets: {}", e);
                    StatusCode::Failure
                })?;

                let mut files = Vec::new();
                if let Some(buckets) = output.buckets {
                    for bucket in buckets {
                        if let Some(bucket_name) = bucket.name {
                            let mut attrs = FileAttributes::default();
                            attrs.set_dir(true);
                            attrs.permissions = Some(0o755);
                            files.push(File {
                                filename: bucket_name.clone(),
                                longname: format!("drwxr-xr-x    2 0        0            0 Dec 28 18:54 {}", bucket_name),
                                attrs,
                            });
                        }
                    }
                }

                let handle_id = this.generate_handle_id();
                let mut guard = this.handles.write().await;
                guard.insert(
                    handle_id.clone(),
                    HandleState::Dir {
                        path: "/".to_string(),
                        files,
                        offset: 0,
                    },
                );
                return Ok(Handle { id, handle: handle_id });
            }

            // Handle bucket directory listing
            let (bucket, key_prefix) = this.parse_path(&resolved_path)?;
            debug!("SFTP Opendir - bucket: '{}', key_prefix: {:?}", bucket, key_prefix);

            let action = S3Action::ListBucket;
            authorize_operation(&this.session_context, &action, &bucket, key_prefix.as_deref())
                .await
                .map_err(|_| StatusCode::PermissionDenied)?;

            let mut builder = s3s::dto::ListObjectsV2Input::builder();
            builder.set_bucket(bucket.clone());

            let prefix = if let Some(ref p) = key_prefix {
                path::retain_slash(p)
            } else {
                String::new()
            };

            if !prefix.is_empty() {
                builder.set_prefix(Some(prefix));
            }
            builder.set_delimiter(Some("/".to_string()));

            let s3_client = this.create_s3_client()?;
            let input = builder.build().map_err(|_| StatusCode::Failure)?;

            let mut files = Vec::new();
            match s3_client.list_objects_v2(input).await {
                Ok(output) => {
                    if let Some(prefixes) = output.common_prefixes {
                        for p in prefixes {
                            if let Some(prefix_str) = p.prefix {
                                let name = prefix_str
                                    .trim_end_matches('/')
                                    .split('/')
                                    .next_back()
                                    .unwrap_or("")
                                    .to_string();
                                if !name.is_empty() {
                                    let mut attrs = FileAttributes::default();
                                    attrs.set_dir(true);
                                    attrs.permissions = Some(0o755);
                                    files.push(File {
                                        filename: name.clone(),
                                        longname: format!("drwxr-xr-x 1 rustfs rustfs 0 Jan 1 1970 {}", name),
                                        attrs,
                                    });
                                }
                            }
                        }
                    }
                    if let Some(contents) = output.contents {
                        for obj in contents {
                            if let Some(key) = obj.key {
                                if key.ends_with('/') {
                                    continue;
                                }
                                let name = key.split('/').next_back().unwrap_or("").to_string();
                                let size = obj.size.unwrap_or(0) as u64;
                                let mut attrs = FileAttributes {
                                    size: Some(size),
                                    permissions: Some(0o644),
                                    ..Default::default()
                                };
                                if let Some(lm) = obj.last_modified {
                                    let dt = time::OffsetDateTime::from(lm);
                                    attrs.mtime = Some(dt.unix_timestamp() as u32);
                                }
                                files.push(File {
                                    filename: name.clone(),
                                    longname: format!("-rw-r--r-- 1 rustfs rustfs {} Jan 1 1970 {}", size, name),
                                    attrs,
                                });
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("S3 List failed: {}", e);
                    return Err(StatusCode::Failure);
                }
            }

            let handle_id = this.generate_handle_id();
            this.handles
                .write()
                .await
                .insert(handle_id.clone(), HandleState::Dir { path, files, offset: 0 });

            Ok(Handle { id, handle: handle_id })
        }
    }

    fn readdir(&mut self, id: u32, handle: String) -> impl Future<Output = Result<Name, Self::Error>> + Send {
        let this = self.clone();
        async move {
            let mut guard = this.handles.write().await;

            if let Some(HandleState::Dir { files, offset, .. }) = guard.get_mut(&handle) {
                debug!("SFTP Readdir - handle: {}, offset: {}, total files: {}", handle, offset, files.len());
                for (i, f) in files.iter().enumerate() {
                    debug!("SFTP Readdir - file[{}]: filename='{}', longname='{}'", i, f.filename, f.longname);
                }

                if *offset >= files.len() {
                    debug!("SFTP Readdir - offset {} >= files length {}, returning empty", offset, files.len());
                    return Ok(Name { id, files: Vec::new() });
                }
                let chunk = files[*offset..].to_vec();
                debug!("SFTP Readdir - returning {} files (offset {})", chunk.len(), offset);
                *offset = files.len();
                Ok(Name { id, files: chunk })
            } else {
                debug!("SFTP Readdir - handle '{}' not found or not a directory handle", handle);
                Err(StatusCode::NoSuchFile)
            }
        }
    }

    fn remove(&mut self, id: u32, filename: String) -> impl Future<Output = Result<Status, Self::Error>> + Send {
        let this = self.clone();
        async move {
            // Resolve relative path to absolute path
            let resolved_filename = this.resolve_path(&filename).await;

            let (bucket, key_opt) = this.parse_path(&resolved_filename)?;

            if let Some(key) = key_opt {
                // Delete object
                let action = S3Action::DeleteObject;
                authorize_operation(&this.session_context, &action, &bucket, Some(&key))
                    .await
                    .map_err(|_| StatusCode::PermissionDenied)?;

                let input = DeleteObjectInput {
                    bucket,
                    key,
                    ..Default::default()
                };

                let s3_client = this.create_s3_client()?;
                s3_client.delete_object(input).await.map_err(|e| {
                    error!("SFTP REMOVE - failed to delete object: {}", e);
                    StatusCode::Failure
                })?;

                Ok(Status {
                    id,
                    status_code: StatusCode::Ok,
                    error_message: "Success".into(),
                    language_tag: "en".into(),
                })
            } else {
                // Delete bucket - check if bucket is empty first
                debug!("SFTP REMOVE - attempting to delete bucket: '{}'", bucket);

                let action = S3Action::DeleteBucket;
                authorize_operation(&this.session_context, &action, &bucket, None)
                    .await
                    .map_err(|_| StatusCode::PermissionDenied)?;

                let s3_client = this.create_s3_client()?;

                // Check if bucket is empty
                let list_input = ListObjectsV2Input {
                    bucket: bucket.clone(),
                    max_keys: Some(1),
                    ..Default::default()
                };

                match s3_client.list_objects_v2(list_input).await {
                    Ok(output) => {
                        if let Some(objects) = output.contents {
                            if !objects.is_empty() {
                                debug!("SFTP REMOVE - bucket '{}' is not empty, cannot delete", bucket);
                                return Ok(Status {
                                    id,
                                    status_code: StatusCode::Failure,
                                    error_message: format!("Bucket '{}' is not empty", bucket),
                                    language_tag: "en".into(),
                                });
                            }
                        }
                    }
                    Err(e) => {
                        debug!("SFTP REMOVE - failed to list objects: {}", e);
                    }
                }

                // Bucket is empty, delete it
                let delete_bucket_input = DeleteBucketInput {
                    bucket: bucket.clone(),
                    ..Default::default()
                };

                match s3_client.delete_bucket(delete_bucket_input).await {
                    Ok(_) => {
                        debug!("SFTP REMOVE - successfully deleted bucket: '{}'", bucket);
                        Ok(Status {
                            id,
                            status_code: StatusCode::Ok,
                            error_message: "Success".into(),
                            language_tag: "en".into(),
                        })
                    }
                    Err(e) => {
                        error!("SFTP REMOVE - failed to delete bucket '{}': {}", bucket, e);
                        Ok(Status {
                            id,
                            status_code: StatusCode::Failure,
                            error_message: format!("Failed to delete bucket: {}", e),
                            language_tag: "en".into(),
                        })
                    }
                }
            }
        }
    }
    fn mkdir(
        &mut self,
        id: u32,
        path: String,
        _attrs: FileAttributes,
    ) -> impl Future<Output = Result<Status, Self::Error>> + Send {
        let this = self.clone();
        async move {
            let (bucket, key_opt) = this.parse_path(&path)?;

            if let Some(key) = key_opt {
                // Create directory inside bucket
                let dir_key = path::retain_slash(&key);

                let action = S3Action::PutObject;
                authorize_operation(&this.session_context, &action, &bucket, Some(&dir_key))
                    .await
                    .map_err(|_| StatusCode::PermissionDenied)?;

                let s3_client = this.create_s3_client()?;
                let empty_stream = futures::stream::empty::<Result<bytes::Bytes, std::io::Error>>();
                let body = StreamingBlob::wrap(empty_stream);
                let input = PutObjectInput {
                    bucket,
                    key: dir_key,
                    body: Some(body),
                    ..Default::default()
                };

                match s3_client.put_object(input).await {
                    Ok(_) => Ok(Status {
                        id,
                        status_code: StatusCode::Ok,
                        error_message: "Directory created".into(),
                        language_tag: "en".into(),
                    }),
                    Err(e) => {
                        error!("SFTP Failed to create directory: {}", e);
                        Ok(Status {
                            id,
                            status_code: StatusCode::Failure,
                            error_message: format!("Failed to create directory: {}", e),
                            language_tag: "en".into(),
                        })
                    }
                }
            } else {
                // Create bucket
                debug!("SFTP mkdir - Creating bucket: '{}'", bucket);

                let action = S3Action::CreateBucket;
                authorize_operation(&this.session_context, &action, &bucket, None)
                    .await
                    .map_err(|_| StatusCode::PermissionDenied)?;

                let s3_client = this.create_s3_client()?;
                let input = s3s::dto::CreateBucketInput {
                    bucket,
                    ..Default::default()
                };

                match s3_client.create_bucket(input).await {
                    Ok(_) => Ok(Status {
                        id,
                        status_code: StatusCode::Ok,
                        error_message: "Bucket created".into(),
                        language_tag: "en".into(),
                    }),
                    Err(e) => {
                        error!("SFTP Failed to create bucket: {}", e);
                        Ok(Status {
                            id,
                            status_code: StatusCode::Failure,
                            error_message: format!("Failed to create bucket: {}", e),
                            language_tag: "en".into(),
                        })
                    }
                }
            }
        }
    }

    fn rmdir(&mut self, id: u32, path: String) -> impl Future<Output = Result<Status, Self::Error>> + Send {
        self.remove(id, path)
    }

    fn realpath(&mut self, id: u32, path: String) -> impl Future<Output = Result<Name, Self::Error>> + Send {
        let this = self.clone();
        async move {
            let resolved = this.resolve_path(&path).await;
            debug!("SFTP Realpath - input: '{}', resolved: '{}'", path, resolved);

            // Check if this path is a directory and get proper attributes
            let attrs = this.do_stat(resolved.clone()).await.unwrap_or_else(|_| {
                let mut default_attrs = FileAttributes::default();
                // Assume it's a directory if stat fails (for root path)
                default_attrs.set_dir(true);
                default_attrs
            });

            Ok(Name {
                id,
                files: vec![File {
                    filename: resolved.clone(),
                    longname: format!(
                        "{:?} {:>4} {:>6} {:>6} {:>8} {} {}",
                        if attrs.is_dir() { "drwxr-xr-x" } else { "-rw-r--r--" },
                        1,
                        "rustfs",
                        "rustfs",
                        attrs.size.unwrap_or(0),
                        "Jan 1 1970",
                        resolved.split('/').next_back().unwrap_or(&resolved)
                    ),
                    attrs,
                }],
            })
        }
    }

    fn stat(&mut self, id: u32, path: String) -> impl Future<Output = Result<Attrs, Self::Error>> + Send {
        let this = self.clone();
        async move {
            let attrs = this.do_stat(path).await?;
            Ok(Attrs { id, attrs })
        }
    }
}
