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

//! Swift object operations
//!
//! This module implements Swift object CRUD operations including upload, download,
//! metadata management, server-side copy, and HTTP range requests.
//!
//! ## Server-Side Copy
//!
//! Swift supports two methods for server-side object copying:
//!
//! 1. **COPY method with Destination header**:
//!    ```text
//!    COPY /v1/AUTH_account/container/source_object
//!    Destination: /container/dest_object
//!    X-Auth-Token: token
//!    ```
//!
//! 2. **PUT method with X-Copy-From header**:
//!    ```text
//!    PUT /v1/AUTH_account/container/dest_object
//!    X-Copy-From: /container/source_object
//!    X-Auth-Token: token
//!    ```
//!
//! The `copy_object` function implements the core copy logic. Handler integration
//! requires passing request headers to identify copy operations.
//!
//! ## Range Requests
//!
//! HTTP Range requests allow partial object downloads:
//!
//! - `bytes=0-1023` - First 1024 bytes
//! - `bytes=1000-` - From byte 1000 to end
//! - `bytes=-500` - Last 500 bytes
//!
//! The `parse_range_header` function parses Range headers, and `get_object` accepts
//! an optional range parameter. See RANGE_REQUESTS.md for details.

use super::account::validate_account_access;
use super::container::ContainerMapper;
use super::expiration_worker::{track_object_expiration, untrack_object_expiration};
use super::quota::QuotaReservationGuard;
use super::sse;
use super::storage_api::object::{BucketOperations, BucketOptions, HTTPRangeSpec, ObjectIO as _, ObjectOperations as _};
use super::{
    SwiftError, SwiftResult, SwiftWriteCommitGuard, SwiftWriteCompletion, SwiftWritePlan, acquire_swift_self_copy_lock,
    prepare_swift_get_object_reader, record_swift_bucket_object_delete_memory, record_swift_bucket_object_write_memory,
    resolve_swift_object_store_handle, validate_metadata,
};
use axum::http::HeaderMap;
use rustfs_credentials::Credentials;
use rustfs_rio::HashReader;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};
use tracing::debug;
use tracing::error;

pub use super::{SwiftGetObjectReader, SwiftObjectInfo, SwiftObjectOptions, SwiftPutObjReader};

const LOG_COMPONENT_PROTOCOLS: &str = "protocols";
const LOG_SUBSYSTEM_SWIFT_OBJECT: &str = "swift_object";
const EVENT_SWIFT_OBJECT_STORAGE_STATE: &str = "swift_object_storage_state";
const SWIFT_DELETE_AT_METADATA: &str = "x-delete-at";
const USER_METADATA_PREFIX: &str = "x-amz-meta-";

/// Maximum object size in bytes (5GB - Swift default)
pub const MAX_OBJECT_SIZE: i64 = 5 * 1024 * 1024 * 1024;
const MAX_OBJECT_SIZE_U64: u64 = MAX_OBJECT_SIZE as u64;

/// Streaming reader guard that aborts as soon as an upload exceeds `max_bytes`.
///
/// Prevents chunked or unknown-length uploads (`content_length == -1`) and
/// bodies that outrun a declared `Content-Length` from bypassing `MAX_OBJECT_SIZE`
/// or a reserved container quota.
pub(crate) struct MaxBytesReader<R> {
    inner: R,
    max_bytes: u64,
    bytes_read: u64,
    exceeded: Arc<AtomicBool>,
}

impl<R> MaxBytesReader<R> {
    pub(crate) fn new(inner: R, max_bytes: u64, exceeded: Arc<AtomicBool>) -> Self {
        Self {
            inner,
            max_bytes,
            bytes_read: 0,
            exceeded,
        }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for MaxBytesReader<R> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        if self.exceeded.load(Ordering::Acquire) {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("object size exceeds maximum of {} bytes", self.max_bytes),
            )));
        }

        let remaining = self.max_bytes.saturating_sub(self.bytes_read);
        if remaining == 0 {
            // Probe at most 1 byte into a local buffer so the caller's ReadBuf is
            // never modified when returning Poll::Ready(Err(_)).
            let mut probe = [0u8; 1];
            let mut probe_buf = ReadBuf::new(&mut probe);
            return match Pin::new(&mut self.inner).poll_read(cx, &mut probe_buf) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
                Poll::Ready(Ok(())) => {
                    if probe_buf.filled().is_empty() {
                        Poll::Ready(Ok(()))
                    } else {
                        self.exceeded.store(true, Ordering::Release);
                        Poll::Ready(Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("object size exceeds maximum of {} bytes", self.max_bytes),
                        )))
                    }
                }
            };
        }

        let cap = remaining.min(buf.remaining() as u64) as usize;
        if cap == 0 {
            return Poll::Ready(Ok(()));
        }

        let slice = buf.initialize_unfilled_to(cap);
        let mut sub = ReadBuf::new(slice);
        match Pin::new(&mut self.inner).poll_read(cx, &mut sub) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
            Poll::Ready(Ok(())) => {
                let read = sub.filled().len();
                buf.advance(read);
                self.bytes_read = self.bytes_read.saturating_add(read as u64);
                Poll::Ready(Ok(()))
            }
        }
    }
}

/// Strictly parse an optional `Content-Length` header.
///
/// Returns `Ok(None)` when absent, `Ok(Some(len))` for a valid non-negative integer,
/// and `Err(SwiftError::BadRequest)` for negative or malformed values.
pub(crate) fn parse_content_length(headers: &HeaderMap) -> SwiftResult<Option<u64>> {
    let Some(header_val) = headers.get("content-length") else {
        return Ok(None);
    };
    let raw = header_val
        .to_str()
        .map_err(|_| SwiftError::BadRequest("Invalid Content-Length header".to_string()))?
        .trim();
    let parsed = raw
        .parse::<u64>()
        .map_err(|_| SwiftError::BadRequest("Invalid Content-Length header".to_string()))?;
    Ok(Some(parsed))
}

fn logical_object_size_u64(info: &SwiftObjectInfo) -> u64 {
    let raw = info.get_actual_size().unwrap_or(info.size);
    u64::try_from(raw).unwrap_or(0)
}

fn is_reserved_swift_user_metadata_key(key: &str) -> bool {
    // Reserve the full vendor namespaces rather than only internal/encryption
    // sub-prefixes: outbound replication forwards bare stored `x-rustfs-*` /
    // `x-minio-*` keys verbatim as request headers, and storage paths inspect
    // bare vendor keys in `user_defined`.
    rustfs_utils::http::starts_with_ignore_ascii_case(key, "x-amz-")
        || rustfs_utils::http::starts_with_ignore_ascii_case(key, "x-rustfs-")
        || rustfs_utils::http::starts_with_ignore_ascii_case(key, "x-minio-")
}

fn stored_swift_user_metadata_key(key: &str) -> String {
    if is_reserved_swift_user_metadata_key(key) {
        format!("{USER_METADATA_PREFIX}{key}")
    } else {
        key.to_string()
    }
}

pub(super) fn swift_response_user_metadata_key(key: &str) -> Option<&str> {
    if rustfs_utils::http::is_internal_key(key)
        || rustfs_utils::http::starts_with_ignore_ascii_case(key, "x-rustfs-encryption-")
        || rustfs_utils::http::starts_with_ignore_ascii_case(key, "x-minio-encryption-")
    {
        return None;
    }
    if let Some(unescaped) = key.strip_prefix(USER_METADATA_PREFIX)
        && is_reserved_swift_user_metadata_key(unescaped)
    {
        return Some(unescaped);
    }
    Some(key)
}

fn swift_user_metadata(headers: &HeaderMap) -> Option<HashMap<String, String>> {
    let mut metadata = HashMap::new();
    let mut present = false;
    for (header_name, header_value) in headers.iter() {
        let header_name = header_name.as_str().to_lowercase();
        let Some(key) = header_name.strip_prefix("x-object-meta-") else {
            continue;
        };
        present = true;
        if let Ok(value) = header_value.to_str() {
            metadata.insert(stored_swift_user_metadata_key(key), value.to_string());
        }
    }
    present.then_some(metadata)
}

/// Object key translator for Swift object names
///
/// Handles URL encoding/decoding and path normalization for Swift object keys.
/// Swift object names can contain any UTF-8 characters except null bytes.
pub struct ObjectKeyMapper;

impl ObjectKeyMapper {
    /// Create a new object key mapper
    pub fn new() -> Self {
        Self
    }

    /// Validate Swift object name
    ///
    /// Object names must:
    /// - Not be empty
    /// - Not exceed 1024 bytes (UTF-8 encoded)
    /// - Not contain null bytes
    /// - Not contain '..' path segments (directory traversal)
    /// - Not start with '/' (leading slash handled by routing)
    pub fn validate_object_name(object: &str) -> SwiftResult<()> {
        if object.is_empty() {
            return Err(SwiftError::BadRequest("Object name cannot be empty".to_string()));
        }

        if object.len() > 1024 {
            return Err(SwiftError::BadRequest("Object name too long (max 1024 bytes)".to_string()));
        }

        if object.contains('\0') {
            return Err(SwiftError::BadRequest("Object name cannot contain null bytes".to_string()));
        }

        if object.chars().any(char::is_control) {
            return Err(SwiftError::BadRequest("Object name cannot contain control characters".to_string()));
        }

        if object.starts_with('/') {
            return Err(SwiftError::BadRequest("Object name cannot start with '/'".to_string()));
        }

        // Check for directory traversal attempts
        if object.contains("..") {
            // Allow ".." as part of a filename, but not as a path segment
            for segment in object.split('/') {
                if segment == ".." {
                    return Err(SwiftError::BadRequest("Object name cannot contain '..' path segments".to_string()));
                }
            }
        }

        Ok(())
    }

    /// Convert Swift object name to S3 object key
    ///
    /// Swift object names are URL-decoded when received in the URL path,
    /// then stored as-is in S3. Special characters are preserved.
    ///
    /// Example:
    /// - Swift: "photos/vacation/beach photo.jpg"
    /// - S3: "photos/vacation/beach photo.jpg"
    pub fn swift_to_s3_key(object: &str) -> SwiftResult<String> {
        Self::validate_object_name(object)?;
        Ok(object.to_string())
    }

    /// Convert S3 object key to Swift object name
    ///
    /// This is essentially an identity transformation since we store
    /// Swift object names as-is in S3.
    pub fn s3_to_swift_name(key: &str) -> String {
        key.to_string()
    }

    /// Build full S3 object key from container and object
    ///
    /// Combines container name (with tenant prefix) and object name.
    /// The container is already validated and includes tenant prefix.
    ///
    /// Example:
    /// - Container: "abc123:photos"
    /// - Object: "vacation/beach.jpg"
    /// - Bucket: "abc123:photos"
    /// - Key: "vacation/beach.jpg"
    pub fn build_s3_key(object: &str) -> SwiftResult<String> {
        Self::swift_to_s3_key(object)
    }

    /// Extract object name from URL path
    ///
    /// The object name comes from the URL path and may be percent-encoded.
    /// This function handles URL decoding while preserving special characters.
    ///
    /// Example URL: /v1/AUTH_abc/container/path%2Fto%2Ffile.txt
    /// Decoded: "path/to/file.txt"
    pub fn decode_object_from_url(encoded: &str) -> SwiftResult<String> {
        // Decode percent-encoding
        let decoded = urlencoding::decode(encoded).map_err(|e| SwiftError::BadRequest(format!("Invalid URL encoding: {}", e)))?;

        Self::validate_object_name(&decoded)?;
        Ok(decoded.to_string())
    }

    /// Encode object name for URL
    ///
    /// When constructing URLs (e.g., for redirect responses), we need to
    /// percent-encode object names.
    pub fn encode_object_for_url(object: &str) -> String {
        urlencoding::encode(object).to_string()
    }

    /// Check if object name represents a directory (pseudo-directory)
    ///
    /// In Swift, objects ending with '/' are treated as directory markers.
    pub fn is_directory_marker(object: &str) -> bool {
        object.ends_with('/')
    }

    /// Normalize object path
    ///
    /// Removes redundant slashes and normalizes the path while preserving
    /// trailing slashes for directory markers.
    pub fn normalize_path(object: &str) -> String {
        // Split by '/', filter out empty segments (except if it's the end)
        let has_trailing_slash = object.ends_with('/');

        let normalized_segments: Vec<&str> = object.split('/').filter(|s| !s.is_empty()).collect();

        let mut result = normalized_segments.join("/");

        // Preserve trailing slash for directory markers
        if has_trailing_slash && !result.is_empty() {
            result.push('/');
        }

        result
    }
}

impl Default for ObjectKeyMapper {
    fn default() -> Self {
        Self::new()
    }
}

fn metadata_delete_at(metadata: &HashMap<String, String>) -> Option<u64> {
    metadata
        .get(SWIFT_DELETE_AT_METADATA)
        .and_then(|value| value.parse::<u64>().ok())
}

async fn update_object_expiration_tracking(account: &str, container: &str, object: &str, delete_at: Option<u64>) {
    if let Some(delete_at) = delete_at {
        track_object_expiration(account, container, object, delete_at).await;
    } else {
        untrack_object_expiration(account, container, object).await;
    }
}

/// Sanitize storage layer errors for client responses
///
/// Logs detailed error server-side while returning generic message to client.
/// This prevents information disclosure vulnerabilities.
fn sanitize_storage_error<E: std::fmt::Display>(operation: &str, error: E) -> SwiftError {
    // Log detailed error server-side
    error!(
        event = EVENT_SWIFT_OBJECT_STORAGE_STATE,
        component = LOG_COMPONENT_PROTOCOLS,
        subsystem = LOG_SUBSYSTEM_SWIFT_OBJECT,
        operation = %operation,
        error = %error,
        result = "failed",
        "swift object storage state changed"
    );

    // Return generic error to client
    SwiftError::InternalServerError(format!("{} operation failed", operation))
}

/// Upload an object to Swift storage (PUT)
///
/// Maps Swift container/object to S3 bucket/key and stores the object.
/// Extracts metadata from X-Object-Meta-* headers and returns ETag.
///
/// # Arguments
/// * `account` - Swift account name (AUTH_{project_id})
/// * `container` - Container name
/// * `object` - Object name
/// * `credentials` - User credentials with project_id
/// * `reader` - Object content reader (implements AsyncRead)
/// * `headers` - HTTP headers including metadata
///
/// # Returns
/// * `Ok(etag)` - Object ETag on success
/// * `Err(SwiftError)` - Error if validation fails or upload fails
pub async fn put_object<R>(
    account: &str,
    container: &str,
    object: &str,
    credentials: &Credentials,
    reader: R,
    headers: &HeaderMap,
) -> SwiftResult<String>
where
    R: tokio::io::AsyncRead + Unpin + Send + Sync + 'static,
{
    put_object_internal(account, container, object, credentials, reader, headers, MAX_OBJECT_SIZE_U64, None).await
}

/// Upload an object using a caller-held [`QuotaReservationGuard`] (used when the
/// handler reserves quota before versioning archival to avoid double-reserving).
pub(super) async fn put_object_with_reservation<R>(
    account: &str,
    container: &str,
    object: &str,
    credentials: &Credentials,
    reader: R,
    headers: &HeaderMap,
    reservation: QuotaReservationGuard,
) -> SwiftResult<String>
where
    R: tokio::io::AsyncRead + Unpin + Send + Sync + 'static,
{
    put_object_internal(
        account,
        container,
        object,
        credentials,
        reader,
        headers,
        MAX_OBJECT_SIZE_U64,
        Some(reservation),
    )
    .await
}

/// Upload an object enforcing a custom maximum object size limit (also used by tests).
#[allow(clippy::too_many_arguments)]
pub async fn put_object_with_max_size<R>(
    account: &str,
    container: &str,
    object: &str,
    credentials: &Credentials,
    reader: R,
    headers: &HeaderMap,
    max_object_size: u64,
) -> SwiftResult<String>
where
    R: tokio::io::AsyncRead + Unpin + Send + Sync + 'static,
{
    put_object_internal(account, container, object, credentials, reader, headers, max_object_size, None).await
}

#[allow(clippy::too_many_arguments)]
async fn put_object_internal<R>(
    account: &str,
    container: &str,
    object: &str,
    credentials: &Credentials,
    reader: R,
    headers: &HeaderMap,
    max_object_size: u64,
    existing_reservation: Option<QuotaReservationGuard>,
) -> SwiftResult<String>
where
    R: tokio::io::AsyncRead + Unpin + Send + Sync + 'static,
{
    // 1. Validate account access and get project_id
    let project_id = validate_account_access(account, credentials)?;

    // 2. Validate object name
    ObjectKeyMapper::validate_object_name(object)?;

    // 3. Get S3 key from object name
    let s3_key = ObjectKeyMapper::swift_to_s3_key(object)?;

    // 4. Map container to bucket using tenant prefixing
    let mapper = ContainerMapper::default();
    let bucket = mapper.swift_to_s3_bucket(container, &project_id);

    // 5. Extract Swift metadata from X-Object-Meta-* headers
    let mut user_metadata = swift_user_metadata(headers).unwrap_or_default();

    // 6. Extract Content-Type if provided
    if let Some(content_type) = headers.get("content-type")
        && let Ok(ct_str) = content_type.to_str()
    {
        user_metadata.insert("content-type".to_string(), ct_str.to_string());
    }

    // 7. Extract and validate expiration headers (X-Delete-At / X-Delete-After)
    let delete_at = super::expiration::extract_expiration(headers)?;
    if let Some(delete_at) = delete_at {
        super::expiration::validate_expiration(delete_at)?;
        user_metadata.insert(SWIFT_DELETE_AT_METADATA.to_string(), delete_at.to_string());
    }

    // 8. Extract symlink target if creating a symlink
    if let Some(symlink_target) = super::symlink::extract_symlink_target(headers)? {
        // Store the fully qualified target (container/object)
        let target_value = symlink_target.to_header_value(container);
        user_metadata.insert("x-object-symlink-target".to_string(), target_value.clone());
        debug!(
            event = EVENT_SWIFT_OBJECT_STORAGE_STATE,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_SWIFT_OBJECT,
            state = "symlink_target_recorded",
            target = %target_value,
            "swift object storage state changed"
        );
    }

    // 9. Validate metadata limits
    validate_metadata(&user_metadata)?;

    // 10. Strictly parse Content-Length (None if omitted; invalid/negative rejected)
    let declared_size = parse_content_length(headers)?;
    let effective_max = max_object_size.min(MAX_OBJECT_SIZE_U64);

    // 11. Reject declared lengths exceeding the maximum object size upfront
    if let Some(size) = declared_size
        && size > effective_max
    {
        return Err(SwiftError::RequestEntityTooLarge(format!(
            "Object size {} bytes exceeds maximum of {} bytes",
            size, effective_max
        )));
    }

    let content_length = match declared_size {
        Some(size) => i64::try_from(size)
            .map_err(|_| SwiftError::RequestEntityTooLarge(format!("Object size exceeds maximum of {} bytes", effective_max)))?,
        None => -1i64,
    };

    // 12. Get storage layer
    let Some(store) = resolve_swift_object_store_handle() else {
        return Err(SwiftError::InternalServerError("Storage layer not initialized".to_string()));
    };

    // 13. Verify bucket/container exists
    store.get_bucket_info(&bucket, &BucketOptions::default()).await.map_err(|e| {
        if e.to_string().contains("does not exist") {
            SwiftError::NotFound(format!("Container '{}' not found", container))
        } else {
            sanitize_storage_error("Container verification", e)
        }
    })?;

    // 14. Atomically reserve container quota (held across the write + overlay update)
    let quota_guard = match existing_reservation {
        Some(guard) => guard,
        None => super::quota::reserve_upload_quota(account, container, declared_size, 1, credentials).await?,
    };

    // Capture previous object size for accurate in-memory usage overlay updates
    let previous_current_size = store
        .get_object_info(&bucket, &s3_key, &SwiftObjectOptions::default())
        .await
        .ok()
        .filter(|info| !info.delete_marker)
        .map(|info| logical_object_size_u64(&info));

    // 15. Resolve and apply bucket default server-side encryption (SSE-S3 / SSE-KMS)
    let mut write_plan = SwiftWritePlan::new();
    if let Some(prepared_sse) = sse::prepare_write_encryption(&bucket, &s3_key, declared_size, Some(credentials)).await? {
        write_plan = write_plan.with_encryption(prepared_sse.write_encryption);
        user_metadata.extend(prepared_sse.metadata);
    }

    // 16. Prepare object options with metadata
    let opts = SwiftObjectOptions {
        user_defined: user_metadata,
        ..Default::default()
    };

    // 17. Enforce a hard streaming ceiling on actual bytes read, covering both
    // unknown-length (-1) streams and streams that send more than Content-Length.
    let stream_limit = declared_size.unwrap_or(effective_max);
    let size_exceeded = Arc::new(AtomicBool::new(false));
    let bounded_reader = MaxBytesReader::new(reader, stream_limit, Arc::clone(&size_exceeded));
    let buf_reader = tokio::io::BufReader::new(bounded_reader);

    // 18. Create HashReader and apply write plan (encryption when bucket default SSE applies)
    let hash_reader = HashReader::from_stream(buf_reader, content_length, content_length, None, None, false)
        .map_err(|e| sanitize_storage_error("Hash reader creation", e))?;
    let hash_reader = write_plan
        .apply(hash_reader, content_length)
        .map_err(|e| sanitize_storage_error("Object encryption setup", e))?;

    // 19. Wrap in PutObjReader as expected by storage layer
    let mut put_reader = SwiftPutObjReader::new(hash_reader);

    // 20. Upload object to storage
    let obj_info = store
        .put_object(&bucket, &s3_key, &mut put_reader, &opts)
        .await
        .map_err(|e| {
            if size_exceeded.load(Ordering::Acquire) {
                SwiftError::RequestEntityTooLarge(format!("Object size exceeds maximum of {} bytes", stream_limit))
            } else {
                sanitize_storage_error("Object upload", e)
            }
        })?;

    // 21. Atomically commit the in-memory bucket usage overlay and release the
    // in-flight quota reservation before post-commit expiration tracking.
    let committed_size = logical_object_size_u64(&obj_info);
    quota_guard
        .finish_with_overlay(|| record_swift_bucket_object_write_memory(&bucket, previous_current_size, committed_size))
        .await;

    update_object_expiration_tracking(account, container, object, delete_at).await;

    // 22. Return ETag (MD5 hash in hex format)
    Ok(obj_info.etag.unwrap_or_default())
}

/// Upload an object with custom metadata (for SLO/DLO internal use)
///
/// Similar to put_object, but allows directly specifying metadata instead of extracting from headers.
/// This is used internally for storing SLO manifests and marker objects.
pub async fn put_object_with_metadata<R>(
    account: &str,
    container: &str,
    object: &str,
    credentials: &Option<Credentials>,
    reader: R,
    metadata: &HashMap<String, String>,
) -> SwiftResult<String>
where
    R: tokio::io::AsyncRead + Unpin + Send + Sync + 'static,
{
    put_object_with_metadata_and_size(account, container, object, credentials, reader, metadata, None, None).await
}

/// Upload an object with custom metadata, explicit byte length, and optional pre-acquired
/// quota reservation (used by SLO, DLO, and server-side copy paths).
#[allow(clippy::too_many_arguments)]
pub(super) async fn put_object_with_metadata_and_size<R>(
    account: &str,
    container: &str,
    object: &str,
    credentials: &Option<Credentials>,
    reader: R,
    metadata: &HashMap<String, String>,
    known_size: Option<u64>,
    existing_reservation: Option<QuotaReservationGuard>,
) -> SwiftResult<String>
where
    R: tokio::io::AsyncRead + Unpin + Send + Sync + 'static,
{
    put_object_with_metadata_and_options(
        account,
        container,
        object,
        credentials,
        reader,
        metadata,
        known_size,
        existing_reservation,
        None,
    )
    .await
}

/// Internal helper supporting pre-acquired self-copy namespace write guards so
/// same-object copies never deadlock on ECStore object locks.
#[allow(clippy::too_many_arguments)]
async fn put_object_with_metadata_and_options<R>(
    account: &str,
    container: &str,
    object: &str,
    credentials: &Option<Credentials>,
    reader: R,
    metadata: &HashMap<String, String>,
    known_size: Option<u64>,
    existing_reservation: Option<QuotaReservationGuard>,
    self_copy_guard: Option<&SwiftWriteCommitGuard>,
) -> SwiftResult<String>
where
    R: tokio::io::AsyncRead + Unpin + Send + Sync + 'static,
{
    // Require authenticated credentials so quota and KMS authorization can never fail open.
    let creds = credentials
        .as_ref()
        .ok_or_else(|| SwiftError::Unauthorized("Credentials required for object upload".to_string()))?;
    let project_id = validate_account_access(account, creds)?;

    // Validate object name
    ObjectKeyMapper::validate_object_name(object)?;

    // Get S3 key from object name
    let s3_key = ObjectKeyMapper::swift_to_s3_key(object)?;

    // Map container to bucket using tenant prefixing
    let mapper = ContainerMapper::default();
    let bucket = mapper.swift_to_s3_bucket(container, &project_id);

    // Validate metadata limits
    validate_metadata(metadata)?;
    let delete_at = metadata_delete_at(metadata);

    if let Some(size) = known_size
        && size > MAX_OBJECT_SIZE_U64
    {
        return Err(SwiftError::RequestEntityTooLarge(format!(
            "Object size {} bytes exceeds maximum of {} bytes",
            size, MAX_OBJECT_SIZE_U64
        )));
    }

    let content_length = match known_size {
        Some(size) => i64::try_from(size).map_err(|_| {
            SwiftError::RequestEntityTooLarge(format!("Object size exceeds maximum of {} bytes", MAX_OBJECT_SIZE_U64))
        })?,
        None => -1i64,
    };

    // Get storage layer
    let Some(store) = resolve_swift_object_store_handle() else {
        return Err(SwiftError::InternalServerError("Storage layer not initialized".to_string()));
    };

    // Verify bucket/container exists (skip re-querying bucket info when the caller
    // already verified it and holds a self-copy lock that could share a lock key with `bucket`).
    if self_copy_guard.is_none() {
        store.get_bucket_info(&bucket, &BucketOptions::default()).await.map_err(|e| {
            if e.to_string().contains("does not exist") {
                SwiftError::NotFound(format!("Container '{}' not found", container))
            } else {
                sanitize_storage_error("Container verification", e)
            }
        })?;
    }

    // Atomically reserve container quota
    let quota_guard = match existing_reservation {
        Some(guard) => guard,
        None => super::quota::reserve_upload_quota(account, container, known_size, 1, creds).await?,
    };

    let lookup_opts = SwiftObjectOptions {
        no_lock: self_copy_guard.is_some(),
        ..Default::default()
    };
    let previous_current_size = store
        .get_object_info(&bucket, &s3_key, &lookup_opts)
        .await
        .ok()
        .filter(|info| !info.delete_marker)
        .map(|info| logical_object_size_u64(&info));

    // Apply bucket default SSE (SSE-S3 / SSE-KMS)
    let mut user_defined = metadata.clone();
    let mut write_plan = SwiftWritePlan::new();
    if let Some(prepared_sse) = sse::prepare_write_encryption(&bucket, &s3_key, known_size, Some(creds)).await? {
        write_plan = write_plan.with_encryption(prepared_sse.write_encryption);
        user_defined.extend(prepared_sse.metadata);
    }

    // Prepare object options with metadata (and pre-acquired self-copy write lock if present)
    let mut opts = SwiftObjectOptions {
        user_defined,
        no_lock: self_copy_guard.is_some(),
        ..Default::default()
    };
    if let Some(guard) = self_copy_guard {
        opts.add_write_commit_guard(guard);
        opts.write_completion = SwiftWriteCompletion::TailDrained;
    }

    let stream_limit = known_size.unwrap_or(MAX_OBJECT_SIZE_U64);
    let size_exceeded = Arc::new(AtomicBool::new(false));
    let bounded_reader = MaxBytesReader::new(reader, stream_limit, Arc::clone(&size_exceeded));
    let buf_reader = tokio::io::BufReader::new(bounded_reader);

    // Create HashReader and apply write plan
    let hash_reader = HashReader::from_stream(buf_reader, content_length, content_length, None, None, false)
        .map_err(|e| sanitize_storage_error("Hash reader creation", e))?;
    let hash_reader = write_plan
        .apply(hash_reader, content_length)
        .map_err(|e| sanitize_storage_error("Object encryption setup", e))?;

    // Wrap in PutObjReader
    let mut put_reader = SwiftPutObjReader::new(hash_reader);

    // Upload object to storage
    let obj_info = store
        .put_object(&bucket, &s3_key, &mut put_reader, &opts)
        .await
        .map_err(|e| {
            if size_exceeded.load(Ordering::Acquire) {
                SwiftError::RequestEntityTooLarge(format!("Object size exceeds maximum of {} bytes", stream_limit))
            } else {
                sanitize_storage_error("Object upload", e)
            }
        })?;

    let committed_size = logical_object_size_u64(&obj_info);
    quota_guard
        .finish_with_overlay(|| record_swift_bucket_object_write_memory(&bucket, previous_current_size, committed_size))
        .await;

    update_object_expiration_tracking(account, container, object, delete_at).await;

    // Return ETag
    Ok(obj_info.etag.unwrap_or_default())
}

/// Download an object from Swift storage (GET)
///
/// Maps Swift container/object to S3 bucket/key and retrieves the object content.
/// Returns the object stream and metadata. Supports HTTP Range requests.
///
/// # Arguments
/// * `account` - Swift account name (AUTH_{project_id})
/// * `container` - Container name
/// * `object` - Object name
/// * `credentials` - User credentials with project_id
/// * `range` - Optional HTTP Range specification (bytes=start-end)
///
/// # Returns
/// * `Ok((stream, object_info))` - Object content stream and metadata
/// * `Err(SwiftError)` - Error if validation fails or object not found
///
/// # Range Requests
/// Swift supports standard HTTP Range requests:
/// - `bytes=0-999` - First 1000 bytes
/// - `bytes=1000-1999` - Bytes 1000-1999
/// - `bytes=1000-` - From byte 1000 to end
/// - `bytes=-500` - Last 500 bytes
pub async fn get_object(
    account: &str,
    container: &str,
    object: &str,
    credentials: &Credentials,
    range: Option<HTTPRangeSpec>,
) -> SwiftResult<SwiftGetObjectReader> {
    // 1. Validate account access and get project_id
    let project_id = validate_account_access(account, credentials)?;

    // 2. Validate object name
    ObjectKeyMapper::validate_object_name(object)?;

    // 3. Get S3 key from object name
    let s3_key = ObjectKeyMapper::swift_to_s3_key(object)?;

    // 4. Map container to bucket using tenant prefixing
    let mapper = ContainerMapper::default();
    let bucket = mapper.swift_to_s3_bucket(container, &project_id);

    // 5. Get storage layer
    let Some(store) = resolve_swift_object_store_handle() else {
        return Err(SwiftError::InternalServerError("Storage layer not initialized".to_string()));
    };

    // 6. Prepare object options
    let opts = SwiftObjectOptions::default();

    // 7. Two-stage prepared read: fetch metadata under the read lock, authorize
    // `kms:Decrypt` BEFORE ECStore unwraps the DEK, then open the body reader.
    let prepared = prepare_swift_get_object_reader(&store, &bucket, &s3_key, range, HeaderMap::new(), &opts)
        .await
        .map_err(|e| {
            let err_str = e.to_string();
            if err_str.contains("does not exist") || err_str.contains("not found") {
                SwiftError::NotFound(format!("Object '{}' not found in container '{}'", object, container))
            } else {
                sanitize_storage_error("Object read", e)
            }
        })?;

    sse::authorize_object_read(Some(credentials), &prepared.object_info().user_defined).await?;

    let reader: SwiftGetObjectReader = prepared.into_reader().await.map_err(|e| {
        let err_str = e.to_string();
        if err_str.contains("does not exist") || err_str.contains("not found") {
            SwiftError::NotFound(format!("Object '{}' not found in container '{}'", object, container))
        } else {
            sanitize_storage_error("Object read", e)
        }
    })?;

    Ok(reader)
}

/// Get object metadata without content (HEAD)
///
/// Maps Swift container/object to S3 bucket/key and retrieves only the metadata.
/// This is more efficient than GET when only metadata is needed.
///
/// # Arguments
/// * `account` - Swift account name (AUTH_{project_id})
/// * `container` - Container name
/// * `object` - Object name
/// * `credentials` - User credentials with project_id
///
/// # Returns
/// * `Ok(object_info)` - Object metadata (ObjectInfo)
/// * `Err(SwiftError)` - Error if validation fails or object not found
pub async fn head_object(
    account: &str,
    container: &str,
    object: &str,
    credentials: &Credentials,
) -> SwiftResult<SwiftObjectInfo> {
    // 1. Validate account access and get project_id
    let project_id = validate_account_access(account, credentials)?;

    // 2. Validate object name
    ObjectKeyMapper::validate_object_name(object)?;

    // 3. Get S3 key from object name
    let s3_key = ObjectKeyMapper::swift_to_s3_key(object)?;

    // 4. Map container to bucket using tenant prefixing
    let mapper = ContainerMapper::default();
    let bucket = mapper.swift_to_s3_bucket(container, &project_id);

    // 5. Get storage layer
    let Some(store) = resolve_swift_object_store_handle() else {
        return Err(SwiftError::InternalServerError("Storage layer not initialized".to_string()));
    };

    // 6. Prepare object options
    let opts = SwiftObjectOptions::default();

    // 7. Get object info (metadata only) from storage
    let info: SwiftObjectInfo = store.get_object_info(&bucket, &s3_key, &opts).await.map_err(|e| {
        let err_str = e.to_string();
        if err_str.contains("does not exist") || err_str.contains("not found") {
            SwiftError::NotFound(format!("Object '{}' not found in container '{}'", object, container))
        } else {
            sanitize_storage_error("Object metadata retrieval", e)
        }
    })?;

    // 8. Check if this is a delete marker
    if info.delete_marker {
        return Err(SwiftError::NotFound(format!(
            "Object '{}' not found in container '{}'",
            object, container
        )));
    }

    Ok(info)
}

/// Delete an object from Swift storage (DELETE)
///
/// Maps Swift container/object to S3 bucket/key and deletes the object.
/// Swift DELETE is idempotent - deleting a non-existent object returns success.
///
/// # Arguments
/// * `account` - Swift account name (AUTH_{project_id})
/// * `container` - Container name
/// * `object` - Object name
/// * `credentials` - User credentials with project_id
///
/// # Returns
/// * `Ok(())` - Object deleted successfully (or didn't exist)
/// * `Err(SwiftError)` - Error if validation fails or deletion fails
pub async fn delete_object(account: &str, container: &str, object: &str, credentials: &Credentials) -> SwiftResult<()> {
    // 1. Validate account access and get project_id
    let project_id = validate_account_access(account, credentials)?;

    // 2. Validate object name
    ObjectKeyMapper::validate_object_name(object)?;

    // 3. Get S3 key from object name
    let s3_key = ObjectKeyMapper::swift_to_s3_key(object)?;

    // 4. Map container to bucket using tenant prefixing
    let mapper = ContainerMapper::default();
    let bucket = mapper.swift_to_s3_bucket(container, &project_id);

    // 5. Get storage layer
    let Some(store) = resolve_swift_object_store_handle() else {
        return Err(SwiftError::InternalServerError("Storage layer not initialized".to_string()));
    };

    // 6. Prepare object options for deletion
    let opts = SwiftObjectOptions::default();

    // 7. Delete object from storage
    // Swift DELETE is idempotent - returns success even if object doesn't exist
    match store.delete_object(&bucket, &s3_key, opts).await {
        Ok(deleted_info) => {
            let deleted_size = logical_object_size_u64(&deleted_info);
            record_swift_bucket_object_delete_memory(&bucket, deleted_size, !deleted_info.delete_marker).await;
            untrack_object_expiration(account, container, object).await;
            Ok(())
        }
        Err(e) => {
            let err_str = e.to_string();
            // Only fail if the container (bucket) doesn't exist
            if err_str.contains("Bucket not found") {
                Err(SwiftError::NotFound(format!("Container '{}' not found", container)))
            } else if err_str.contains("Object not found") || err_str.contains("does not exist") {
                // Object already gone - this is success for idempotent DELETE
                untrack_object_expiration(account, container, object).await;
                Ok(())
            } else {
                Err(sanitize_storage_error("Object deletion", e))
            }
        }
    }
}

/// Update object metadata (POST)
///
/// Updates user-defined metadata (X-Object-Meta-*) for an existing object
/// without changing the object content. This is a Swift-specific operation.
///
/// # Arguments
/// * `account` - Swift account name (AUTH_{project_id})
/// * `container` - Container name
/// * `object` - Object name
/// * `credentials` - User credentials with project_id
/// * `headers` - HTTP headers containing X-Object-Meta-* headers to update
///
/// # Returns
/// * `Ok(())` - Metadata updated successfully
/// * `Err(SwiftError)` - Error if validation fails, object not found, or update fails
pub async fn update_object_metadata(
    account: &str,
    container: &str,
    object: &str,
    credentials: &Credentials,
    headers: &HeaderMap,
) -> SwiftResult<()> {
    // 1. Validate account access and get project_id
    let project_id = validate_account_access(account, credentials)?;

    // 2. Validate object name
    ObjectKeyMapper::validate_object_name(object)?;

    // 3. Get S3 key from object name
    let s3_key = ObjectKeyMapper::swift_to_s3_key(object)?;

    // 4. Map container to bucket using tenant prefixing
    let mapper = ContainerMapper::default();
    let bucket = mapper.swift_to_s3_bucket(container, &project_id);

    // 5. Get storage layer
    let Some(store) = resolve_swift_object_store_handle() else {
        return Err(SwiftError::InternalServerError("Storage layer not initialized".to_string()));
    };

    // 6. First, get the existing object info to verify it exists
    let opts = SwiftObjectOptions::default();
    let existing_info = store.get_object_info(&bucket, &s3_key, &opts).await.map_err(|e| {
        let err_str = e.to_string();
        if err_str.contains("does not exist") || err_str.contains("not found") {
            SwiftError::NotFound(format!("Object '{}' not found in container '{}'", object, container))
        } else {
            sanitize_storage_error("Object info retrieval", e)
        }
    })?;

    // 7. Check if this is a delete marker
    if existing_info.delete_marker {
        return Err(SwiftError::NotFound(format!(
            "Object '{}' not found in container '{}'",
            object, container
        )));
    }

    // 8. Extract new metadata from X-Object-Meta-* headers
    let mut new_metadata = swift_user_metadata(headers).unwrap_or_default();

    // 9. Also update Content-Type if provided
    if let Some(content_type) = headers.get("content-type")
        && let Ok(ct_str) = content_type.to_str()
    {
        new_metadata.insert("content-type".to_string(), ct_str.to_string());
    }

    let delete_at = super::expiration::extract_expiration(headers)?;
    if let Some(delete_at) = delete_at {
        super::expiration::validate_expiration(delete_at)?;
        new_metadata.insert(SWIFT_DELETE_AT_METADATA.to_string(), delete_at.to_string());
    }

    // 10. Validate metadata limits
    validate_metadata(&new_metadata)?;

    // 11. Prepare options for metadata update
    // Swift POST replaces all custom metadata, not merges
    let update_opts = SwiftObjectOptions {
        user_defined: new_metadata,
        mod_time: existing_info.mod_time,
        version_id: existing_info.version_id.map(|v| v.to_string()),
        ..Default::default()
    };

    // 12. Update object metadata
    let _updated_info = store
        .put_object_metadata(&bucket, &s3_key, &update_opts)
        .await
        .map_err(|e| sanitize_storage_error("Metadata update", e))?;

    update_object_expiration_tracking(account, container, object, delete_at).await;

    Ok(())
}

/// Server-side copy object (COPY)
///
/// Copies an object from source container/object to destination container/object
/// without transferring data through the client. This is a Swift-specific operation
/// that uses the underlying storage layer's copy capabilities.
///
/// # Arguments
/// * `src_account` - Source Swift account name (AUTH_{project_id})
/// * `src_container` - Source container name
/// * `src_object` - Source object name
/// * `dst_account` - Destination Swift account name (AUTH_{project_id})
/// * `dst_container` - Destination container name
/// * `dst_object` - Destination object name
/// * `credentials` - User credentials with project_id
/// * `headers` - HTTP headers (may contain metadata to set on destination)
///
/// # Returns
/// * `Ok(String)` - ETag of the copied object
/// * `Err(SwiftError)` - Error if validation fails, source not found, or copy fails
///
/// # Swift COPY Behavior
/// - Copies object data and system metadata (content-type, etag, etc.)
/// - Can optionally set new custom metadata via X-Object-Meta-* headers
/// - If no custom metadata provided, copies source custom metadata
/// - Destination container must exist before copy
/// - Atomic operation (either succeeds completely or fails)
///
/// # Handler Integration Note
/// The current handler architecture needs to be updated to pass headers through
/// to support COPY method and X-Copy-From header detection. See handler.rs for details.
#[allow(clippy::too_many_arguments)] // Necessary for full copy functionality
pub async fn copy_object(
    src_account: &str,
    src_container: &str,
    src_object: &str,
    dst_account: &str,
    dst_container: &str,
    dst_object: &str,
    credentials: &Credentials,
    headers: &HeaderMap,
) -> SwiftResult<String> {
    // 1. Validate source account access and get project_id
    let src_project_id = validate_account_access(src_account, credentials)?;

    // 2. Validate destination account access (may be different account)
    let dst_project_id = validate_account_access(dst_account, credentials)?;

    // 3. Validate object names
    ObjectKeyMapper::validate_object_name(src_object)?;
    ObjectKeyMapper::validate_object_name(dst_object)?;

    // 4. Get S3 keys from object names
    let src_s3_key = ObjectKeyMapper::swift_to_s3_key(src_object)?;
    let dst_s3_key = ObjectKeyMapper::swift_to_s3_key(dst_object)?;

    // 5. Map containers to buckets using tenant prefixing
    let mapper = ContainerMapper::default();
    let src_bucket = mapper.swift_to_s3_bucket(src_container, &src_project_id);
    let dst_bucket = mapper.swift_to_s3_bucket(dst_container, &dst_project_id);

    // 6. Get storage layer
    let Some(store) = resolve_swift_object_store_handle() else {
        return Err(SwiftError::InternalServerError("Storage layer not initialized".to_string()));
    };

    // 7. Verify destination container exists BEFORE taking any object lock so
    // same-object copies whose key matches the bucket name cannot collide on lock keys.
    let bucket_opts = BucketOptions::default();
    let _dst_bucket_info = store.get_bucket_info(&dst_bucket, &bucket_opts).await.map_err(|e| {
        let err_str = e.to_string();
        if err_str.contains("does not exist") || err_str.contains("not found") {
            SwiftError::NotFound(format!("Destination container '{}' not found", dst_container))
        } else {
            sanitize_storage_error("Destination container verification", e)
        }
    })?;

    // 8. For same-object self-copy, pre-acquire the exclusive namespace write lock
    // and open both the source reader and destination writer with `no_lock = true`
    // so the reader's lock guard never self-deadlocks against `put_object`.
    let is_self_copy = src_bucket == dst_bucket && src_s3_key == dst_s3_key;
    let self_copy_guard = if is_self_copy {
        Some(
            acquire_swift_self_copy_lock(&store, &dst_bucket, &dst_s3_key)
                .await
                .map_err(|e| sanitize_storage_error("Self-copy lock", e))?,
        )
    } else {
        None
    };

    // 9. Open a locked prepared source snapshot so authorization, size validation,
    // quota reservation, and metadata all read the exact object version being copied.
    let src_opts = SwiftObjectOptions {
        no_lock: self_copy_guard.is_some(),
        ..Default::default()
    };
    let prepared = prepare_swift_get_object_reader(&store, &src_bucket, &src_s3_key, None, HeaderMap::new(), &src_opts)
        .await
        .map_err(|e| {
            let err_str = e.to_string();
            if err_str.contains("does not exist") || err_str.contains("not found") {
                SwiftError::NotFound(format!("Source object '{}' not found in container '{}'", src_object, src_container))
            } else {
                sanitize_storage_error("Source object read", e)
            }
        })?;

    let locked_info = prepared.object_info();
    if locked_info.delete_marker {
        return Err(SwiftError::NotFound(format!(
            "Source object '{}' not found in container '{}'",
            src_object, src_container
        )));
    }

    // 10. Authorize `kms:Decrypt` strictly BEFORE opening/unwrapping the source reader.
    sse::authorize_object_read(Some(credentials), &locked_info.user_defined).await?;

    // 11. Enforce maximum object size from the locked source snapshot.
    let src_plaintext_size = logical_object_size_u64(locked_info);
    if src_plaintext_size > MAX_OBJECT_SIZE_U64 {
        return Err(SwiftError::RequestEntityTooLarge(format!(
            "Object size {} bytes exceeds maximum of {} bytes",
            src_plaintext_size, MAX_OBJECT_SIZE_U64
        )));
    }

    // 12. Prepare destination metadata from the locked source snapshot.
    let mut new_metadata = (*locked_info.user_defined).clone();
    if let Some(custom_metadata) = swift_user_metadata(headers) {
        new_metadata = custom_metadata;
    }
    let content_type = if let Some(ct) = headers.get("content-type") {
        ct.to_str().ok().map(|s| s.to_string())
    } else {
        locked_info.content_type.clone()
    };
    if let Some(ct) = content_type {
        new_metadata.insert("content-type".to_string(), ct);
    }
    sse::strip_source_encryption_metadata(&mut new_metadata);
    validate_metadata(&new_metadata)?;

    // 13. Atomically reserve destination container quota (self-copy overwrites in place
    // with zero net size/count delta while still failing if already over quota).
    let (reserve_bytes, reserve_objects) = if is_self_copy { (0, 0) } else { (src_plaintext_size, 1) };
    let quota_guard =
        super::quota::reserve_upload_quota(dst_account, dst_container, Some(reserve_bytes), reserve_objects, credentials).await?;

    // 14. Open the authorized source stream and write through the unified write path
    // so plaintext, SSE decrypt, destination default SSE re-encryption, size bounds,
    // quota reservation, and usage-overlay updates all apply consistently.
    let src_reader = prepared
        .into_reader()
        .await
        .map_err(|e| sanitize_storage_error("Source object read", e))?;

    let creds_opt = Some(credentials.clone());
    Box::pin(put_object_with_metadata_and_options(
        dst_account,
        dst_container,
        dst_object,
        &creds_opt,
        src_reader.stream,
        &new_metadata,
        Some(src_plaintext_size),
        Some(quota_guard),
        self_copy_guard.as_ref(),
    ))
    .await
}

/// Parse Swift Destination header
///
/// The Destination header format is: `/container/object`
/// The account segment is not included in the Destination header (it's implicit from the request URL).
/// Object names can contain slashes (e.g., `/container/path/to/file.txt`).
///
/// # Arguments
/// * `destination` - The Destination header value
///
/// # Returns
/// * `Ok((container, object))` - Parsed container and object names
/// * `Err(SwiftError)` - Error if format is invalid
///
/// # Examples
/// ```ignore
/// // Simple object name
/// let (container, object) = parse_destination_header("/my-container/my-object.txt")?;
/// assert_eq!(container, "my-container");
/// assert_eq!(object, "my-object.txt");
///
/// // Object with path (slashes preserved)
/// let (container, object) = parse_destination_header("/my-container/path/to/file.txt")?;
/// assert_eq!(container, "my-container");
/// assert_eq!(object, "path/to/file.txt");
/// ```
pub fn parse_destination_header(destination: &str) -> SwiftResult<(String, String)> {
    let destination = destination.trim_start_matches('/');
    let parts: Vec<&str> = destination.splitn(2, '/').collect();

    if parts.len() < 2 {
        return Err(SwiftError::BadRequest(
            "Invalid Destination header format. Expected: /container/object".to_string(),
        ));
    }

    let container = parts[0].to_string();
    let object = parts[1].to_string();

    if container.is_empty() || object.is_empty() {
        return Err(SwiftError::BadRequest(
            "Destination container and object names cannot be empty".to_string(),
        ));
    }

    Ok((container, object))
}

/// Parse Swift X-Copy-From header
///
/// The X-Copy-From header format is: `/container/object`
/// This function parses it into container and object components.
///
/// # Arguments
/// * `copy_from` - The X-Copy-From header value
///
/// # Returns
/// * `Ok((container, object))` - Parsed container and object names
/// * `Err(SwiftError)` - Error if format is invalid
pub fn parse_copy_from_header(copy_from: &str) -> SwiftResult<(String, String)> {
    // Same parsing logic as Destination header
    parse_destination_header(copy_from)
}

/// Parse HTTP Range header for Swift
///
/// Parses standard HTTP Range header (e.g., "bytes=0-1023") into HTTPRangeSpec.
/// Swift uses the same Range header format as HTTP/S3.
///
/// # Arguments
/// * `range_str` - The Range header value (e.g., "bytes=0-1023")
///
/// # Returns
/// * `Ok(HTTPRangeSpec)` - Parsed range specification
/// * `Err(SwiftError)` - Error if format is invalid
///
/// # Supported Formats
/// - `bytes=0-1023` - Bytes 0 through 1023 (inclusive)
/// - `bytes=1024-` - From byte 1024 to end of file
/// - `bytes=-500` - Last 500 bytes (suffix range)
///
/// # Examples
/// ```ignore
/// let range = parse_range_header("bytes=0-1023")?;
/// assert_eq!(range.start, 0);
/// assert_eq!(range.end, 1023);
/// ```
pub fn parse_range_header(range_str: &str) -> SwiftResult<HTTPRangeSpec> {
    if !range_str.starts_with("bytes=") {
        return Err(SwiftError::BadRequest("Range header must start with 'bytes='".to_string()));
    }

    let range_part = &range_str[6..]; // Remove "bytes=" prefix

    if let Some(dash_pos) = range_part.find('-') {
        let start_str = &range_part[..dash_pos];
        let end_str = &range_part[dash_pos + 1..];

        if start_str.is_empty() && end_str.is_empty() {
            return Err(SwiftError::BadRequest("Invalid range format: both start and end are empty".to_string()));
        }

        if start_str.is_empty() {
            // Suffix range: bytes=-500 (last 500 bytes)
            let length = end_str
                .parse::<i64>()
                .map_err(|_| SwiftError::BadRequest("Invalid range format: suffix length not a number".to_string()))?;

            if length <= 0 {
                return Err(SwiftError::BadRequest("Invalid range format: suffix length must be positive".to_string()));
            }

            Ok(HTTPRangeSpec {
                is_suffix_length: true,
                start: -length,
                end: -1,
            })
        } else {
            // Regular range or open-ended range
            let start = start_str
                .parse::<i64>()
                .map_err(|_| SwiftError::BadRequest("Invalid range format: start not a number".to_string()))?;

            let end = if end_str.is_empty() {
                -1 // Open-ended range: bytes=500-
            } else {
                end_str
                    .parse::<i64>()
                    .map_err(|_| SwiftError::BadRequest("Invalid range format: end not a number".to_string()))?
            };

            if start < 0 {
                return Err(SwiftError::BadRequest("Invalid range format: start must be non-negative".to_string()));
            }

            if end != -1 && end < start {
                return Err(SwiftError::BadRequest("Invalid range format: end must be >= start".to_string()));
            }

            Ok(HTTPRangeSpec {
                is_suffix_length: false,
                start,
                end,
            })
        }
    } else {
        Err(SwiftError::BadRequest("Invalid range format: missing '-'".to_string()))
    }
}

/// Format Content-Range header for Swift responses
///
/// Creates a Content-Range header value for partial content responses.
/// Format: "bytes start-end/total"
///
/// # Arguments
/// * `start` - Start byte position (inclusive)
/// * `end` - End byte position (inclusive)
/// * `total` - Total size of the object
///
/// # Returns
/// * Formatted Content-Range header value
///
/// # Example
/// ```ignore
/// let header = format_content_range(0, 1023, 5000);
/// assert_eq!(header, "bytes 0-1023/5000");
/// ```
pub fn format_content_range(start: i64, end: i64, total: i64) -> String {
    format!("bytes {}-{}/{}", start, end, total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn test_validate_object_name_valid() {
        assert!(ObjectKeyMapper::validate_object_name("myfile.txt").is_ok());
        assert!(ObjectKeyMapper::validate_object_name("path/to/file.jpg").is_ok());
        assert!(ObjectKeyMapper::validate_object_name("file with spaces.pdf").is_ok());
        assert!(ObjectKeyMapper::validate_object_name("special-chars_@#$.txt").is_ok());
        assert!(ObjectKeyMapper::validate_object_name("unicode-文件.txt").is_ok());
    }

    #[test]
    fn swift_user_metadata_cannot_materialize_internal_storage_keys() {
        let mut headers = HeaderMap::new();
        headers.insert("x-object-meta-x-rustfs-internal-actual-size", "1".parse().expect("valid metadata value"));
        headers.insert(
            "x-object-meta-x-rustfs-replication-ssec-crc",
            "forged-crc".parse().expect("valid metadata value"),
        );
        headers.insert(
            "x-object-meta-x-rustfs-replication-ssec-original-size",
            "777".parse().expect("valid metadata value"),
        );
        headers.insert(
            "x-object-meta-x-rustfs-replication-compression",
            "zstd".parse().expect("valid metadata value"),
        );
        headers.insert("x-object-meta-description", "safe".parse().expect("valid metadata value"));
        let metadata = swift_user_metadata(&headers).expect("custom metadata should be detected");

        assert_eq!(metadata.get("x-amz-meta-x-rustfs-internal-actual-size").map(String::as_str), Some("1"));
        assert_eq!(
            metadata.get("x-amz-meta-x-rustfs-replication-ssec-crc").map(String::as_str),
            Some("forged-crc")
        );
        assert_eq!(
            metadata
                .get("x-amz-meta-x-rustfs-replication-ssec-original-size")
                .map(String::as_str),
            Some("777")
        );
        assert_eq!(
            metadata
                .get("x-amz-meta-x-rustfs-replication-compression")
                .map(String::as_str),
            Some("zstd")
        );
        assert_eq!(metadata.get("description").map(String::as_str), Some("safe"));
        for bare in [
            "x-rustfs-internal-actual-size",
            "x-rustfs-replication-ssec-crc",
            "x-rustfs-replication-ssec-original-size",
            "x-rustfs-replication-compression",
        ] {
            assert!(!metadata.contains_key(bare), "{bare} must not be stored bare");
        }
        assert_eq!(
            stored_swift_user_metadata_key("x-minio-encryption-original-size"),
            "x-amz-meta-x-minio-encryption-original-size"
        );
        assert_eq!(
            stored_swift_user_metadata_key("X-Rustfs-Replication-Ssec-Crc"),
            "x-amz-meta-X-Rustfs-Replication-Ssec-Crc"
        );
        assert_eq!(stored_swift_user_metadata_key("x-minio-source-mtime"), "x-amz-meta-x-minio-source-mtime");
        assert_eq!(stored_swift_user_metadata_key("description"), "description");
    }

    #[test]
    fn swift_user_metadata_response_mapping_is_reversible_and_filters_internal_keys() {
        assert_eq!(
            swift_response_user_metadata_key("x-amz-meta-x-rustfs-internal-actual-size"),
            Some("x-rustfs-internal-actual-size")
        );
        assert_eq!(
            swift_response_user_metadata_key("x-amz-meta-x-rustfs-replication-ssec-crc"),
            Some("x-rustfs-replication-ssec-crc")
        );
        assert_eq!(
            swift_response_user_metadata_key("x-amz-meta-x-rustfs-replication-ssec-original-size"),
            Some("x-rustfs-replication-ssec-original-size")
        );
        assert_eq!(
            swift_response_user_metadata_key("x-amz-meta-x-rustfs-replication-compression"),
            Some("x-rustfs-replication-compression")
        );
        assert_eq!(
            swift_response_user_metadata_key("x-amz-meta-x-minio-source-mtime"),
            Some("x-minio-source-mtime")
        );
        assert_eq!(swift_response_user_metadata_key("x-amz-meta-x-amz-checksum"), Some("x-amz-checksum"));
        assert_eq!(swift_response_user_metadata_key("x-amz-meta-description"), Some("x-amz-meta-description"));
        assert_eq!(swift_response_user_metadata_key("description"), Some("description"));
        assert_eq!(swift_response_user_metadata_key("x-rustfs-internal-actual-size"), None);
        assert_eq!(swift_response_user_metadata_key("x-minio-internal-actual-size"), None);
    }

    #[test]
    fn test_validate_object_name_empty() {
        let result = ObjectKeyMapper::validate_object_name("");
        assert!(result.is_err());
        match result {
            Err(SwiftError::BadRequest(msg)) => {
                assert!(msg.contains("empty"));
            }
            _ => panic!("Expected BadRequest error"),
        }
    }

    #[test]
    fn test_validate_object_name_too_long() {
        let long_name = "a".repeat(1025);
        let result = ObjectKeyMapper::validate_object_name(&long_name);
        assert!(result.is_err());
        match result {
            Err(SwiftError::BadRequest(msg)) => {
                assert!(msg.contains("too long"));
            }
            _ => panic!("Expected BadRequest error"),
        }
    }

    #[test]
    fn test_validate_object_name_null_byte() {
        let result = ObjectKeyMapper::validate_object_name("file\0name.txt");
        assert!(result.is_err());
        match result {
            Err(SwiftError::BadRequest(msg)) => {
                assert!(msg.contains("null"));
            }
            _ => panic!("Expected BadRequest error"),
        }
    }

    #[test]
    fn test_validate_object_name_control_character() {
        let result = ObjectKeyMapper::validate_object_name("file\nname.txt");
        assert!(result.is_err());
        match result {
            Err(SwiftError::BadRequest(msg)) => {
                assert!(msg.contains("control"));
            }
            _ => panic!("Expected BadRequest error"),
        }
    }

    #[test]
    fn test_validate_object_name_leading_slash() {
        let result = ObjectKeyMapper::validate_object_name("/file.txt");
        assert!(result.is_err());
        match result {
            Err(SwiftError::BadRequest(msg)) => {
                assert!(msg.contains("start with '/'"));
            }
            _ => panic!("Expected BadRequest error"),
        }
    }

    #[test]
    fn test_validate_object_name_directory_traversal() {
        // ".." as a path segment should be rejected
        assert!(ObjectKeyMapper::validate_object_name("path/../file.txt").is_err());
        assert!(ObjectKeyMapper::validate_object_name("../file.txt").is_err());
        assert!(ObjectKeyMapper::validate_object_name("path/..").is_err());

        // But ".." in a filename should be allowed
        assert!(ObjectKeyMapper::validate_object_name("file..txt").is_ok());
        assert!(ObjectKeyMapper::validate_object_name("my..file.txt").is_ok());
    }

    #[test]
    fn test_metadata_delete_at_parses_valid_timestamp() {
        let mut metadata = HashMap::new();
        metadata.insert(SWIFT_DELETE_AT_METADATA.to_string(), "1740000000".to_string());

        assert_eq!(metadata_delete_at(&metadata), Some(1740000000));
    }

    #[test]
    fn test_metadata_delete_at_ignores_invalid_timestamp() {
        let mut metadata = HashMap::new();
        metadata.insert(SWIFT_DELETE_AT_METADATA.to_string(), "invalid".to_string());

        assert_eq!(metadata_delete_at(&metadata), None);
    }

    #[test]
    fn test_swift_to_s3_key() {
        assert_eq!(ObjectKeyMapper::swift_to_s3_key("file.txt").unwrap(), "file.txt");
        assert_eq!(ObjectKeyMapper::swift_to_s3_key("path/to/file.jpg").unwrap(), "path/to/file.jpg");
        assert_eq!(ObjectKeyMapper::swift_to_s3_key("file with spaces.pdf").unwrap(), "file with spaces.pdf");
    }

    #[test]
    fn test_s3_to_swift_name() {
        assert_eq!(ObjectKeyMapper::s3_to_swift_name("file.txt"), "file.txt");
        assert_eq!(ObjectKeyMapper::s3_to_swift_name("path/to/file.jpg"), "path/to/file.jpg");
    }

    #[test]
    fn test_decode_object_from_url() {
        // Basic decoding
        assert_eq!(ObjectKeyMapper::decode_object_from_url("file.txt").unwrap(), "file.txt");

        // Percent-encoded spaces
        assert_eq!(
            ObjectKeyMapper::decode_object_from_url("file%20with%20spaces.txt").unwrap(),
            "file with spaces.txt"
        );

        // Percent-encoded special characters
        assert_eq!(
            ObjectKeyMapper::decode_object_from_url("path%2Fto%2Ffile.txt").unwrap(),
            "path/to/file.txt"
        );

        // Unicode characters
        assert_eq!(ObjectKeyMapper::decode_object_from_url("%E6%96%87%E4%BB%B6.txt").unwrap(), "文件.txt");
    }

    #[test]
    fn test_encode_object_for_url() {
        assert_eq!(ObjectKeyMapper::encode_object_for_url("file.txt"), "file.txt");

        assert_eq!(ObjectKeyMapper::encode_object_for_url("file with spaces.txt"), "file%20with%20spaces.txt");

        assert_eq!(ObjectKeyMapper::encode_object_for_url("path/to/file.txt"), "path%2Fto%2Ffile.txt");
    }

    #[test]
    fn test_is_directory_marker() {
        assert!(ObjectKeyMapper::is_directory_marker("folder/"));
        assert!(ObjectKeyMapper::is_directory_marker("path/to/dir/"));
        assert!(!ObjectKeyMapper::is_directory_marker("file.txt"));
        assert!(!ObjectKeyMapper::is_directory_marker("folder"));
    }

    #[test]
    fn test_normalize_path() {
        // Remove redundant slashes
        assert_eq!(ObjectKeyMapper::normalize_path("path//to///file.txt"), "path/to/file.txt");

        // Preserve trailing slash for directories
        assert_eq!(ObjectKeyMapper::normalize_path("path/to/dir/"), "path/to/dir/");

        // Remove leading/trailing slashes except for directory marker
        assert_eq!(ObjectKeyMapper::normalize_path("/path/to/file.txt"), "path/to/file.txt");

        // Empty segments
        assert_eq!(ObjectKeyMapper::normalize_path("path///to/file.txt"), "path/to/file.txt");
    }

    proptest! {
        #[test]
        fn validate_object_name_ok_outputs_are_safe(input in any::<String>()) {
            if let Ok(()) = ObjectKeyMapper::validate_object_name(&input) {
                prop_assert!(!input.is_empty());
                prop_assert!(input.len() <= 1024);
                prop_assert!(!input.starts_with('/'));
                prop_assert!(!input.chars().any(char::is_control));
                prop_assert!(!input.split('/').any(|segment| segment == ".."));
            }
        }

        #[test]
        fn decode_object_from_url_round_trips_valid_input(input in any::<String>()) {
            let encoded = ObjectKeyMapper::encode_object_for_url(&input);

            match ObjectKeyMapper::decode_object_from_url(&encoded) {
                Ok(decoded) => {
                    prop_assert_eq!(decoded.as_str(), input.as_str());
                    prop_assert!(ObjectKeyMapper::validate_object_name(&decoded).is_ok());
                }
                Err(_) => {
                    prop_assert!(ObjectKeyMapper::validate_object_name(&input).is_err());
                }
            }
        }

        #[test]
        fn normalize_path_is_idempotent(input in any::<String>()) {
            let once = ObjectKeyMapper::normalize_path(&input);
            let twice = ObjectKeyMapper::normalize_path(&once);
            prop_assert_eq!(twice, once);
        }
    }

    #[test]
    fn test_parse_destination_header() {
        // Valid destination headers
        let (container, object) = parse_destination_header("/my-container/my-object.txt").unwrap();
        assert_eq!(container, "my-container");
        assert_eq!(object, "my-object.txt");

        let (container, object) = parse_destination_header("/container/path/to/object.txt").unwrap();
        assert_eq!(container, "container");
        assert_eq!(object, "path/to/object.txt");

        // Without leading slash
        let (container, object) = parse_destination_header("container/object.txt").unwrap();
        assert_eq!(container, "container");
        assert_eq!(object, "object.txt");

        // Invalid formats
        assert!(parse_destination_header("/container-only").is_err());
        assert!(parse_destination_header("/").is_err());
        assert!(parse_destination_header("").is_err());
        assert!(parse_destination_header("/container/").is_err());
    }

    #[test]
    fn test_parse_copy_from_header() {
        // Valid X-Copy-From headers
        let (container, object) = parse_copy_from_header("/source-container/source.txt").unwrap();
        assert_eq!(container, "source-container");
        assert_eq!(object, "source.txt");

        let (container, object) = parse_copy_from_header("/my-container/photos/vacation.jpg").unwrap();
        assert_eq!(container, "my-container");
        assert_eq!(object, "photos/vacation.jpg");

        // Invalid formats (same as destination header)
        assert!(parse_copy_from_header("/container-only").is_err());
        assert!(parse_copy_from_header("").is_err());
    }

    #[test]
    fn test_parse_range_header_regular() {
        // Regular range: bytes=0-1023
        let range = parse_range_header("bytes=0-1023").unwrap();
        assert!(!range.is_suffix_length);
        assert_eq!(range.start, 0);
        assert_eq!(range.end, 1023);

        // Another regular range
        let range = parse_range_header("bytes=1000-1999").unwrap();
        assert!(!range.is_suffix_length);
        assert_eq!(range.start, 1000);
        assert_eq!(range.end, 1999);
    }

    #[test]
    fn test_parse_range_header_open_ended() {
        // Open-ended range: bytes=1000-
        let range = parse_range_header("bytes=1000-").unwrap();
        assert!(!range.is_suffix_length);
        assert_eq!(range.start, 1000);
        assert_eq!(range.end, -1);

        // From start to end
        let range = parse_range_header("bytes=0-").unwrap();
        assert!(!range.is_suffix_length);
        assert_eq!(range.start, 0);
        assert_eq!(range.end, -1);
    }

    #[test]
    fn test_parse_range_header_suffix() {
        // Suffix range: bytes=-500 (last 500 bytes)
        let range = parse_range_header("bytes=-500").unwrap();
        assert!(range.is_suffix_length);
        assert_eq!(range.start, -500);
        assert_eq!(range.end, -1);

        // Last 1 byte
        let range = parse_range_header("bytes=-1").unwrap();
        assert!(range.is_suffix_length);
        assert_eq!(range.start, -1);
        assert_eq!(range.end, -1);
    }

    #[test]
    fn test_parse_range_header_invalid() {
        // Missing "bytes=" prefix
        assert!(parse_range_header("0-1023").is_err());
        assert!(parse_range_header("range=0-1023").is_err());

        // Missing dash
        assert!(parse_range_header("bytes=01023").is_err());

        // Both empty
        assert!(parse_range_header("bytes=-").is_err());

        // End before start
        assert!(parse_range_header("bytes=1000-999").is_err());

        // Negative start (invalid for regular range)
        assert!(parse_range_header("bytes=-1000-2000").is_err());

        // Invalid numbers
        assert!(parse_range_header("bytes=abc-def").is_err());
        assert!(parse_range_header("bytes=0-xyz").is_err());

        // Zero or negative suffix length
        assert!(parse_range_header("bytes=-0").is_err());
    }

    #[test]
    fn test_format_content_range() {
        assert_eq!(format_content_range(0, 1023, 5000), "bytes 0-1023/5000");
        assert_eq!(format_content_range(1000, 1999, 10000), "bytes 1000-1999/10000");
        assert_eq!(format_content_range(0, 0, 1), "bytes 0-0/1");
        assert_eq!(format_content_range(9999, 9999, 10000), "bytes 9999-9999/10000");
    }

    #[test]
    fn test_build_s3_key() {
        assert_eq!(ObjectKeyMapper::build_s3_key("file.txt").unwrap(), "file.txt");

        assert_eq!(ObjectKeyMapper::build_s3_key("path/to/file.jpg").unwrap(), "path/to/file.jpg");
    }

    #[test]
    fn test_parse_content_length_valid_missing_negative_and_malformed() {
        let empty = HeaderMap::new();
        assert_eq!(parse_content_length(&empty).unwrap(), None);

        let mut zero = HeaderMap::new();
        zero.insert("content-length", "0".parse().expect("valid header"));
        assert_eq!(parse_content_length(&zero).unwrap(), Some(0));

        let mut positive = HeaderMap::new();
        positive.insert("content-length", "12345".parse().expect("valid header"));
        assert_eq!(parse_content_length(&positive).unwrap(), Some(12345));

        let mut negative = HeaderMap::new();
        negative.insert("content-length", "-1".parse().expect("valid header"));
        assert!(matches!(parse_content_length(&negative), Err(SwiftError::BadRequest(_))));

        let mut malformed = HeaderMap::new();
        malformed.insert("content-length", "12abc".parse().expect("valid header"));
        assert!(matches!(parse_content_length(&malformed), Err(SwiftError::BadRequest(_))));
    }

    #[tokio::test]
    async fn test_max_bytes_reader_allows_exact_limit_and_aborts_on_overflow() {
        use tokio::io::AsyncReadExt;

        // Exact limit succeeds and leaves exceeded=false.
        let exceeded = Arc::new(AtomicBool::new(false));
        let mut reader = MaxBytesReader::new(std::io::Cursor::new(vec![b'a'; 1024]), 1024, Arc::clone(&exceeded));
        let mut out = Vec::new();
        reader.read_to_end(&mut out).await.expect("exact limit must succeed");
        assert_eq!(out.len(), 1024);
        assert!(!exceeded.load(Ordering::Acquire));

        // 1 byte over limit fails immediately and stops consuming an infinite stream.
        let exceeded = Arc::new(AtomicBool::new(false));
        let mut infinite = tokio::io::repeat(b'b').take(1024 * 1024);
        let mut guarded = MaxBytesReader::new(&mut infinite, 2048, Arc::clone(&exceeded));
        let mut buf = Vec::new();
        let err = guarded
            .read_to_end(&mut buf)
            .await
            .expect_err("stream exceeding max_bytes must fail");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(exceeded.load(Ordering::Acquire));
        let consumed = (1024 * 1024) - infinite.limit();
        assert!(
            consumed <= 2048 + 8192,
            "guarded reader must stop shortly after max_bytes, consumed {consumed}"
        );
    }
}
