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
use crate::storage::ecfs::FS;
use futures::TryStreamExt;
use russh_sftp::protocol::{Attrs, Data, File, FileAttributes, Handle, Name, OpenFlags, Status, StatusCode, Version};
use russh_sftp::server::Handler;
use s3s::dto::{CopyObjectInput, DeleteObjectInput, GetObjectInput, PutObjectInput, StreamingBlob};
use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::fs::{File as TokioFile, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::RwLock;
use tracing::{debug, error, trace, warn};
use uuid::Uuid;

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
}

impl SftpHandler {
    pub fn new(session_context: SessionContext) -> Self {
        Self {
            session_context,
            handles: Arc::new(RwLock::new(HashMap::new())),
            next_handle_id: Arc::new(AtomicU32::new(1)),
            temp_dir: std::env::temp_dir(),
        }
    }

    fn create_s3_client(&self) -> Result<ProtocolS3Client, StatusCode> {
        let fs = FS::new();
        let client = ProtocolS3Client::new(fs, self.session_context.access_key().to_string());
        Ok(client)
    }

    fn parse_path(&self, path: &str) -> Result<(String, Option<String>), StatusCode> {
        if path.contains("..") {
            return Err(StatusCode::PermissionDenied);
        }
        let path = path.trim_start_matches('/');
        if path.is_empty() || path == "." {
            return Err(StatusCode::NoSuchFile);
        }
        let parts: Vec<&str> = path.splitn(2, '/').collect();
        let bucket = parts[0].to_string();
        if parts.len() == 1 || parts[1].is_empty() {
            Ok((bucket, None))
        } else {
            Ok((bucket, Some(parts[1].to_string())))
        }
    }

    fn generate_handle_id(&self) -> String {
        let id = self.next_handle_id.fetch_add(1, Ordering::Relaxed);
        format!("handle_{}", id)
    }

    async fn cleanup_state(&self, state: HandleState) {
        if let HandleState::Write { temp_file_path, .. } = state {
            let _ = tokio::fs::remove_file(temp_file_path).await;
        }
    }

    async fn do_stat(&self, path: String) -> Result<FileAttributes, StatusCode> {
        debug!("SFTP do_stat - input path: '{}'", path);
        let (bucket, key_opt) = self.parse_path(&path)?;
        debug!("SFTP do_stat - parsed bucket: '{}', key: {:?}", bucket, key_opt);

        // Choose correct action based on whether we're dealing with a bucket or object
        let action = if key_opt.is_none() {
            S3Action::HeadBucket
        } else {
            S3Action::HeadObject
        };

        authorize_operation(&self.session_context, &action, &bucket, key_opt.as_deref())
            .await
            .map_err(|_| StatusCode::PermissionDenied)?;

        let s3_client = self.create_s3_client()?;

        if let Some(key) = key_opt {
            let input = s3s::dto::HeadObjectInput {
                bucket,
                key,
                ..Default::default()
            };
            match s3_client.head_object(input).await {
                Ok(out) => {
                    let mut attrs = FileAttributes::default();
                    attrs.size = Some(out.content_length.unwrap_or(0) as u64);
                    if let Some(lm) = out.last_modified {
                        let dt = time::OffsetDateTime::from(lm);
                        attrs.mtime = Some(dt.unix_timestamp() as u32);
                    }
                    attrs.permissions = Some(0o644);
                    Ok(attrs)
                }
                Err(_) => Err(StatusCode::NoSuchFile),
            }
        } else {
            let input = s3s::dto::HeadBucketInput {
                bucket,
                ..Default::default()
            };
            match s3_client.head_bucket(input).await {
                Ok(_) => {
                    let mut attrs = FileAttributes::default();
                    attrs.set_dir(true);
                    attrs.permissions = Some(0o755);
                    Ok(attrs)
                }
                Err(_) => Err(StatusCode::NoSuchFile),
            }
        }
    }
}

impl Handler for SftpHandler {
    type Error = StatusCode;

    fn unimplemented(&self) -> Self::Error {
        StatusCode::OpUnsupported
    }

    fn init(
        &mut self,
        version: u32,
        _extensions: HashMap<String, String>,
    ) -> impl Future<Output = Result<Version, Self::Error>> + Send {
        async move {
            trace!("SFTP Init version: {}", version);
            Ok(Version::new())
        }
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

            let (bucket, key_opt) = this.parse_path(&filename)?;
            let key = key_opt.ok_or(StatusCode::PermissionDenied)?;

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

                    let input = PutObjectInput {
                        bucket: bucket.clone(),
                        key: key.clone(),
                        body: Some(body),
                        ..Default::default()
                    };

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
                        let stream = body.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));
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
            let attrs = this.do_stat(path).await?;
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
            debug!("SFTP Opendir: {}", path);

            // Handle root directory case - list all buckets
            if path == "/" || path == "/." || path == "." {
                debug!("SFTP Opendir - listing root directory (all buckets)");
                let action = S3Action::ListBuckets;
                authorize_operation(&this.session_context, &action, "", None)
                    .await
                    .map_err(|_| StatusCode::PermissionDenied)?;

                // List all buckets
                let s3_client = this.create_s3_client()
                    .map_err(|e| {
                        error!("SFTP Opendir - failed to create S3 client: {}", e);
                        e
                    })?;

                let input = s3s::dto::ListBucketsInput::builder()
                    .build()
                    .map_err(|_| StatusCode::Failure)?;

                let secret_key = &this.session_context.principal.user_identity.credentials.secret_key;
                let output = s3_client.list_buckets(input, secret_key)
                    .await
                    .map_err(|e| {
                        error!("SFTP Opendir - failed to list buckets: {}", e);
                        StatusCode::Failure
                    })?;

                let mut files = Vec::new();
                if let Some(buckets) = output.buckets {
                    for bucket in buckets {
                        if let Some(bucket_name) = bucket.name {
                            let attrs = FileAttributes {
                                size: Some(0),
                                ..Default::default()
                            };
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
                guard.insert(handle_id.clone(), HandleState::Dir {
                    path: "/".to_string(),
                    files,
                    offset: 0
                });
                return Ok(Handle { id, handle: handle_id });
            }

            // Handle bucket directory listing
            let (bucket, key_prefix) = this.parse_path(&path)?;

            let action = S3Action::ListBucket;
            authorize_operation(&this.session_context, &action, &bucket, key_prefix.as_deref())
                .await
                .map_err(|_| StatusCode::PermissionDenied)?;

            let mut builder = s3s::dto::ListObjectsV2Input::builder();
            builder.set_bucket(bucket);

            let prefix = if let Some(p) = key_prefix {
                if p.ends_with('/') { p } else { format!("{}/", p) }
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
                                let name = prefix_str.trim_end_matches('/').split('/').last().unwrap_or("").to_string();
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
                                let name = key.split('/').last().unwrap_or("").to_string();
                                let size = obj.size.unwrap_or(0) as u64;
                                let mut attrs = FileAttributes::default();
                                attrs.size = Some(size);
                                attrs.permissions = Some(0o644);
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
                if *offset >= files.len() {
                    return Ok(Name { id, files: Vec::new() });
                }
                let chunk = files[*offset..].to_vec();
                *offset = files.len();
                Ok(Name { id, files: chunk })
            } else {
                Err(StatusCode::NoSuchFile)
            }
        }
    }

    fn remove(&mut self, id: u32, filename: String) -> impl Future<Output = Result<Status, Self::Error>> + Send {
        let this = self.clone();
        async move {
            let (bucket, key_opt) = this.parse_path(&filename)?;
            let key = key_opt.ok_or(StatusCode::NoSuchFile)?;

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
                error!("Remove failed: {}", e);
                StatusCode::Failure
            })?;

            Ok(Status {
                id,
                status_code: StatusCode::Ok,
                error_message: "Success".into(),
                language_tag: "en".into(),
            })
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
            let key = key_opt.ok_or(StatusCode::Failure)?;
            let dir_key = if key.ends_with('/') { key } else { format!("{}/", key) };

            let action = S3Action::PutObject;
            authorize_operation(&this.session_context, &action, &bucket, Some(&dir_key))
                .await
                .map_err(|_| StatusCode::PermissionDenied)?;

            let empty_stream = futures::stream::empty::<Result<bytes::Bytes, std::io::Error>>();
            let body = StreamingBlob::wrap(empty_stream);
            let input = PutObjectInput {
                bucket,
                key: dir_key,
                body: Some(body),
                ..Default::default()
            };

            let s3_client = this.create_s3_client()?;
            s3_client.put_object(input).await.map_err(|e| {
                error!("Mkdir failed: {}", e);
                StatusCode::Failure
            })?;

            Ok(Status {
                id,
                status_code: StatusCode::Ok,
                error_message: "Success".into(),
                language_tag: "en".into(),
            })
        }
    }

    fn rmdir(&mut self, id: u32, path: String) -> impl Future<Output = Result<Status, Self::Error>> + Send {
        self.remove(id, path)
    }

    fn realpath(&mut self, id: u32, path: String) -> impl Future<Output = Result<Name, Self::Error>> + Send {
        async move {
            let normalized = if path.starts_with('/') { path } else { format!("/{}", path) };
            Ok(Name {
                id,
                files: vec![File {
                    filename: normalized.clone(),
                    longname: normalized,
                    attrs: FileAttributes::default(),
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

    fn rename(&mut self, id: u32, oldpath: String, newpath: String) -> impl Future<Output = Result<Status, Self::Error>> + Send {
        let this = self.clone();
        async move {
            debug!("SFTP Rename: {} -> {}", oldpath, newpath);

            let (src_bucket, src_key_opt) = this.parse_path(&oldpath)?;
            let (dst_bucket, dst_key_opt) = this.parse_path(&newpath)?;

            let src_key = src_key_opt.ok_or(StatusCode::Failure)?;
            let dst_key = dst_key_opt.ok_or(StatusCode::Failure)?;

            authorize_operation(&this.session_context, &S3Action::PutObject, &dst_bucket, Some(&dst_key))
                .await
                .map_err(|_| StatusCode::PermissionDenied)?;

            authorize_operation(&this.session_context, &S3Action::DeleteObject, &src_bucket, Some(&src_key))
                .await
                .map_err(|_| StatusCode::PermissionDenied)?;

            let s3_client = this.create_s3_client()?;

            let copy_source = s3s::dto::CopySource::Bucket {
                bucket: src_bucket.clone().into_boxed_str(),
                key: src_key.clone().into_boxed_str(),
                version_id: None,
            };

            let copy_input = CopyObjectInput::builder()
                .bucket(dst_bucket)
                .key(dst_key)
                .copy_source(copy_source)
                .build()
                .map_err(|_| StatusCode::Failure)?;

            if let Err(e) = s3_client.copy_object(copy_input).await {
                error!("Rename(Copy) failed: {}", e);
                return Err(StatusCode::Failure);
            }

            let delete_input = DeleteObjectInput {
                bucket: src_bucket,
                key: src_key,
                ..Default::default()
            };

            if let Err(e) = s3_client.delete_object(delete_input).await {
                error!("Rename(DeleteOld) failed: {}", e);
                warn!("Orphaned file after rename");
            }

            Ok(Status {
                id,
                status_code: StatusCode::Ok,
                error_message: "Success".into(),
                language_tag: "en".into(),
            })
        }
    }
}
