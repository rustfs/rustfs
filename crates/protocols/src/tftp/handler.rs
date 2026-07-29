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

//! TftpdHandler and util functions used in tftp server.

use super::config::{TftpAccessMode, TftpConfig};
use crate::common::client::s3::StorageBackend;
use crate::common::gateway::{AuthorizationError, S3Action, authorize_operation};
use crate::common::session::{Protocol, ProtocolPrincipal, SessionContext};
use crate::constants::network::DEFAULT_SOURCE_IP;
use async_tftp::packet;
use async_tftp::server::Handler;
use futures_lite::{AsyncWrite, StreamExt, io::Cursor};
use futures_util::stream;
use s3s::dto::{PutObjectInput, StreamingBlob};
use std::fmt::Debug;
use std::io;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::OnceCell;
use tracing::{debug, error, info};

const LOG_COMPONENT_PROTOCOLS: &str = "protocols";
const LOG_SUBSYSTEM_TFTP_SERVER: &str = "tftp_server";
const EVENT_TFTP_RRQ_STATE: &str = "tftp_rrq_state";
const EVENT_TFTP_WRQ_STATE: &str = "tftp_wrq_state";
const EVENT_TFTP_SESSION_STATE: &str = "tftp_session_state";

use rustfs_utils::path;

fn parse_s3_path(path_input: &str) -> std::result::Result<(String, Option<String>), String> {
    if path_input.chars().any(char::is_control) {
        return Err("control characters are not allowed in TFTP paths".to_string());
    }

    let cleaned_path = path::clean(path_input);
    let (bucket, object) = path::path_to_bucket_object(&cleaned_path);

    if object.contains(path::GLOBAL_DIR_SUFFIX) {
        return Err("internal directory marker is not allowed in TFTP paths".to_string());
    }

    let key = if object.is_empty() { None } else { Some(object) };
    Ok((bucket, key))
}

/// Resolve a TFTP request path into an S3 (bucket, key) pair.
///
/// When `default_bucket` is set, the entire path is the S3 key:
///   `/any/path`  → (default_bucket, "any/path")
///   `relative`   → (default_bucket, "relative")
///
/// When `default_bucket` is NOT set, the first path component is the bucket:
///   `/bucket/obj/key` → ("bucket", "obj/key")
///   `/just-bucket`    → error (no key after bucket)
pub fn resolve_tftp_path(default_bucket: Option<&str>, path: &Path) -> Result<(String, String), String> {
    let path_str = path.to_string_lossy();
    let trimmed = path_str.trim_start_matches('/');

    if let Some(bucket) = default_bucket {
        let trimmed = trimmed.to_string();
        if trimmed.is_empty() {
            return Err(format!("path '{}' is a empty path;", path.display()));
        }
        Ok((bucket.to_string(), trimmed))
    } else {
        let (bucket, key) = parse_s3_path(&path_str).map_err(|e| format!("{}: {}", "Invalid path", e))?;
        let key = key.ok_or_else(|| {
            format!(
                "path '{}' has no key after bucket prefix; use /<bucket>/<key> or set RUSTFS_TFTP_BUCKET",
                path.display()
            )
        })?;
        Ok((bucket, key))
    }
}

// ---------------------------------------------------------------------------
// VecWriter — in-memory AsyncWrite that uploads to S3 on drop
// ---------------------------------------------------------------------------

/// Accumulates TFTP write bytes into an in-memory buffer.
/// On drop the buffer is uploaded to S3 via a single PutObject call.
pub struct VecWriter<S: StorageBackend + Send + Sync + 'static> {
    buf: Vec<u8>,
    storage: Arc<S>,
    bucket: String,
    key: String,
    access_key: String,
}

impl<S: StorageBackend + Send + Sync + 'static> VecWriter<S> {
    fn new(storage: Arc<S>, bucket: String, key: String, access_key: String) -> Self {
        VecWriter {
            buf: Vec::new(),
            storage,
            bucket,
            key,
            access_key,
        }
    }
}

impl<S: StorageBackend + Send + Sync + 'static> AsyncWrite for VecWriter<S> {
    fn poll_write(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        self.buf.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl<S: StorageBackend + Send + Sync + 'static> Drop for VecWriter<S> {
    fn drop(&mut self) {
        let data = std::mem::take(&mut self.buf);
        let storage = Arc::clone(&self.storage);
        let bucket = self.bucket.clone();
        let key = self.key.clone();
        let access_key = self.access_key.clone();

        tokio::spawn(async move {
            let size = data.len();
            let mut put_builder = PutObjectInput::builder();
            put_builder.set_bucket(bucket.clone());
            put_builder.set_key(key.clone());
            put_builder.set_content_length(Some(size as i64));

            // Create StreamingBlob with known size
            let data_bytes = bytes::Bytes::from(data);
            let stream = stream::once(async move { Ok::<bytes::Bytes, std::io::Error>(data_bytes) });
            let streaming_blob = StreamingBlob::wrap(stream);
            put_builder.set_body(Some(streaming_blob));
            let input = match put_builder.build() {
                Ok(input) => input,
                Err(e) => {
                    error!(
                        event = EVENT_TFTP_WRQ_STATE,
                        component = LOG_COMPONENT_PROTOCOLS,
                        subsystem = LOG_SUBSYSTEM_TFTP_SERVER,
                        bucket = %bucket, key = %key, error = %e,
                        "Failed to build PutObjectInput for TFTP WRQ"
                    );
                    return;
                }
            };

            match storage.put_object(input, &access_key, "").await {
                Ok(_) => {
                    info!(
                        event = EVENT_TFTP_WRQ_STATE,
                        component = LOG_COMPONENT_PROTOCOLS,
                        subsystem = LOG_SUBSYSTEM_TFTP_SERVER,
                        bucket = %bucket, key = %key, size = size,
                        "TFTP upload to S3 completed"
                    );
                }
                Err(e) => {
                    error!(
                        event = EVENT_TFTP_WRQ_STATE,
                        component = LOG_COMPONENT_PROTOCOLS,
                        subsystem = LOG_SUBSYSTEM_TFTP_SERVER,
                        bucket = %bucket, key = %key, error = %e,
                        "Failed to upload TFTP data to S3"
                    );
                }
            }
        });
    }
}

// ---------------------------------------------------------------------------
// TftpdHandler — async-tftp Handler backed by StorageBackend
// ---------------------------------------------------------------------------

/// Implements async_tftp::server::Handler, translating RRQ/WRQ into
/// S3 GetObject / PutObject calls.
pub struct TftpdHandler<S: StorageBackend + Send + Sync + 'static> {
    storage: Arc<S>,
    default_bucket: Option<String>,
    mode: TftpAccessMode,
    access_key: String,
    /// Lazily-initialised session context built from the configured
    /// credentials via IAM. TFTP has no per-connection authentication,
    /// so the same context is reused for every request.
    session_context: OnceCell<SessionContext>,
}

impl<S: StorageBackend + Send + Sync + 'static> TftpdHandler<S> {
    /// Create a new handler from configuration and a storage backend.
    pub fn new(config: &TftpConfig, storage: Arc<S>) -> Self {
        TftpdHandler {
            storage,
            default_bucket: config.default_bucket.clone(),
            mode: config.mode,
            access_key: config.access_key.clone(),
            session_context: OnceCell::new(),
        }
    }

    /// Lazily initialise and return the per-server [`SessionContext`].
    ///
    /// On first call this looks up the configured access key via IAM
    /// and caches the result. Credential validation (secret-key check)
    /// already happened in [`TftpConfig::validate`], so this method
    /// only builds the struct.
    /// Subsequent calls return the cached context without IAM round-trips.
    async fn get_session_context(&self) -> Result<&SessionContext, AuthorizationError> {
        self.session_context
            .get_or_try_init(|| async {
                use rustfs_iam::get;

                let iam_sys = get().map_err(|e| {
                    error!(
                        event = EVENT_TFTP_SESSION_STATE,
                        component = LOG_COMPONENT_PROTOCOLS,
                        subsystem = LOG_SUBSYSTEM_TFTP_SERVER,
                        result = "iam_unavailable",
                        error = %e,
                        "TFTP session init: IAM unavailable"
                    );
                    AuthorizationError::IamUnavailable
                })?;

                let (user_identity, is_valid) = iam_sys.check_key(&self.access_key).await.map_err(|e| {
                    error!(
                        event = EVENT_TFTP_SESSION_STATE,
                        component = LOG_COMPONENT_PROTOCOLS,
                        subsystem = LOG_SUBSYSTEM_TFTP_SERVER,
                        result = "check_key_failed",
                        error = %e,
                        "TFTP session init: key check failed"
                    );
                    AuthorizationError::IamUnavailable
                })?;

                if !is_valid {
                    error!(
                        event = EVENT_TFTP_SESSION_STATE,
                        component = LOG_COMPONENT_PROTOCOLS,
                        subsystem = LOG_SUBSYSTEM_TFTP_SERVER,
                        result = "invalid_access_key",
                        "TFTP session init: access key rejected"
                    );
                    return Err(AuthorizationError::AccessDenied);
                }

                let identity = user_identity.ok_or_else(|| {
                    error!(
                        event = EVENT_TFTP_SESSION_STATE,
                        component = LOG_COMPONENT_PROTOCOLS,
                        subsystem = LOG_SUBSYSTEM_TFTP_SERVER,
                        result = "identity_missing",
                        "TFTP session init: identity missing"
                    );
                    AuthorizationError::AccessDenied
                })?;

                let source_ip: IpAddr = DEFAULT_SOURCE_IP.parse().unwrap();
                let principal = ProtocolPrincipal::new(Arc::new(identity));
                Ok(SessionContext::new(principal, Protocol::Tftp, source_ip))
            })
            .await
    }
}

impl<S: StorageBackend + Send + Sync + 'static + Debug> Handler for TftpdHandler<S> {
    type Reader = Cursor<Vec<u8>>;
    type Writer = VecWriter<S>;

    async fn read_req_open(&mut self, _client: &SocketAddr, path: &Path) -> Result<(Self::Reader, Option<u64>), packet::Error> {
        if self.mode == TftpAccessMode::WriteOnly {
            return Err(packet::Error::Msg("TFTP server is write-only".to_string()));
        }

        let (bucket, key) = resolve_tftp_path(self.default_bucket.as_deref(), path).map_err(|e| packet::Error::Msg(e))?;

        debug!(
            event = EVENT_TFTP_RRQ_STATE,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_TFTP_SERVER,
            bucket = %bucket, key = %key,
            "TFTP RRQ"
        );
        let session_ctx = self.get_session_context().await.map_err(|e| match e {
            AuthorizationError::IamUnavailable => packet::Error::Msg("Internal authentication service unavailable".to_string()),
            AuthorizationError::AccessDenied => packet::Error::PermissionDenied,
        })?;

        authorize_operation(session_ctx, &S3Action::GetObject, &bucket, Some(&key))
            .await
            .map_err(|_| packet::Error::PermissionDenied)?;

        let output = self
            .storage
            .get_object(&bucket, &key, &self.access_key, "", None)
            .await
            .map_err(|e| {
                error!(
                    event = EVENT_TFTP_RRQ_STATE,
                    component = LOG_COMPONENT_PROTOCOLS,
                    subsystem = LOG_SUBSYSTEM_TFTP_SERVER,
                    bucket = %bucket, key = %key, error = %e,
                    "S3 get_object failed for TFTP RRQ"
                );
                packet::Error::FileNotFound
            })?;

        let content_length = output.content_length.unwrap_or(0).max(0) as u64;

        // Drain the S3 body stream into an in-memory buffer.
        let mut buf = Vec::with_capacity(content_length as usize);
        if let Some(mut body) = output.body {
            while let Some(chunk_result) = body.next().await {
                let chunk = chunk_result.map_err(|_| packet::Error::Msg("Failed to read object body".into()))?;
                buf.extend_from_slice(&chunk);
            }
        }

        info!(
            event = EVENT_TFTP_RRQ_STATE,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_TFTP_SERVER,
            bucket = %bucket, key = %key, size = buf.len(),
            "TFTP RRQ: loaded from S3"
        );

        Ok((Cursor::new(buf), Some(content_length)))
    }

    async fn write_req_open(
        &mut self,
        _client: &SocketAddr,
        path: &Path,
        _size: Option<u64>,
    ) -> Result<Self::Writer, packet::Error> {
        if self.mode == TftpAccessMode::ReadOnly {
            return Err(packet::Error::Msg("TFTP server is read-only".to_string()));
        }

        let (bucket, key) = resolve_tftp_path(self.default_bucket.as_deref(), path).map_err(|e| packet::Error::Msg(e))?;

        debug!(
            event = EVENT_TFTP_WRQ_STATE,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_TFTP_SERVER,
            bucket = %bucket, key = %key,
            "TFTP WRQ"
        );

        let session_ctx = self.get_session_context().await.map_err(|e| match e {
            AuthorizationError::IamUnavailable => packet::Error::Msg("Internal authentication service unavailable".to_string()),
            AuthorizationError::AccessDenied => packet::Error::PermissionDenied,
        })?;

        authorize_operation(session_ctx, &S3Action::PutObject, &bucket, Some(&key))
            .await
            .map_err(|_| packet::Error::PermissionDenied)?;

        info!(
            event = EVENT_TFTP_WRQ_STATE,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_TFTP_SERVER,
            bucket = %bucket, key = %key,
            "TFTP WRQ: ready to receive"
        );

        Ok(VecWriter::new(Arc::clone(&self.storage), bucket, key, self.access_key.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_with_default_bucket() {
        let bucket = "mybucket";

        let (b, k) = resolve_tftp_path(Some(bucket), Path::new("/foo/bar.txt")).unwrap();
        assert_eq!(b, bucket);
        assert_eq!(k, "foo/bar.txt");

        let (b, k) = resolve_tftp_path(Some(bucket), Path::new("file.txt")).unwrap();
        assert_eq!(b, bucket);
        assert_eq!(k, "file.txt");

        let (b, k) = resolve_tftp_path(Some(bucket), Path::new("/foo/bar/")).unwrap();
        assert_eq!(b, bucket);
        assert_eq!(k, "foo/bar/");

        let (b, k) = resolve_tftp_path(Some(bucket), Path::new("/foo//bar")).unwrap();
        assert_eq!(b, bucket);
        assert_eq!(k, "foo//bar");

        let (b, k) = resolve_tftp_path(Some(bucket), Path::new("/路径/文件.txt")).unwrap();
        assert_eq!(b, bucket);
        assert_eq!(k, "路径/文件.txt");

        let err = resolve_tftp_path(Some(bucket), Path::new("")).unwrap_err();
        assert!(err.contains("is a empty path"));

        let err = resolve_tftp_path(Some(bucket), Path::new("/")).unwrap_err();
        assert!(err.contains("is a empty path"));
    }

    #[test]
    fn resolve_without_default_bucket() {
        let (b, k) = resolve_tftp_path(None, Path::new("/mybucket/foo/bar.txt")).unwrap();
        assert_eq!(b, "mybucket");
        assert_eq!(k, "foo/bar.txt");

        let (b, k) = resolve_tftp_path(None, Path::new("/bucket/a/b/c/d/e")).unwrap();
        assert_eq!(b, "bucket");
        assert_eq!(k, "a/b/c/d/e");

        let (b, k) = resolve_tftp_path(None, Path::new("/bucket/k")).unwrap();
        assert_eq!(b, "bucket");
        assert_eq!(k, "k");

        let (b, k) = resolve_tftp_path(None, Path::new("/bucket__XLDIR__/mykey")).unwrap();
        assert_eq!(b, "bucket__XLDIR__");
        assert_eq!(k, "mykey");

        let (b, k) = resolve_tftp_path(None, Path::new("/存储桶/对象.txt")).unwrap();
        assert_eq!(b, "存储桶");
        assert_eq!(k, "对象.txt");

        let (b, k) = resolve_tftp_path(None, Path::new("/bucket/../other/key")).unwrap();
        assert_eq!(b, "other");
        assert_eq!(k, "key");

        let (b, k) = resolve_tftp_path(None, Path::new("/bucket/./key")).unwrap();
        assert_eq!(b, "bucket");
        assert_eq!(k, "key");

        let err = resolve_tftp_path(None, Path::new("/just-bucket")).unwrap_err();
        assert!(err.contains("no key after bucket prefix"));

        let err = resolve_tftp_path(None, Path::new("/just-bucket/")).unwrap_err();
        assert!(err.contains("no key after bucket prefix"));

        let err = resolve_tftp_path(None, Path::new("nobucket")).unwrap_err();
        assert!(err.contains("no key after bucket prefix"));

        let err = resolve_tftp_path(None, Path::new("/bucket/..")).unwrap_err();
        assert!(err.contains("no key after bucket prefix"));

        let err = resolve_tftp_path(None, Path::new("/bucket/key\x00hidden")).unwrap_err();
        assert!(err.contains("Invalid path"));
        assert!(err.contains("control characters"));

        let err = resolve_tftp_path(None, Path::new("/bucket/key__XLDIR__")).unwrap_err();
        assert!(err.contains("Invalid path"));
        assert!(err.contains("internal directory marker"));
    }
}
