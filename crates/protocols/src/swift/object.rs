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
use super::{SwiftError, SwiftResult};
use axum::http::HeaderMap;
use rustfs_credentials::Credentials;
use rustfs_ecstore::new_object_layer_fn;
use rustfs_ecstore::store_api::{BucketOperations, BucketOptions, ObjectIO, ObjectOperations, ObjectOptions, PutObjReader};
use rustfs_rio::HashReader;
use std::collections::HashMap;
use tracing::debug;
use tracing::error;

/// Maximum number of metadata headers allowed per object (Swift standard)
const MAX_METADATA_COUNT: usize = 90;

/// Maximum size in bytes for a single metadata value (Swift standard)
const MAX_METADATA_VALUE_SIZE: usize = 256;

/// Maximum object size in bytes (5GB - Swift default)
const MAX_OBJECT_SIZE: i64 = 5 * 1024 * 1024 * 1024;

/// Object key translator for Swift object names
///
/// Handles URL encoding/decoding and path normalization for Swift object keys.
/// Swift object names can contain any UTF-8 characters except null bytes.
#[allow(dead_code)] // Used in: object operations
pub struct ObjectKeyMapper;

impl ObjectKeyMapper {
    /// Create a new object key mapper
    #[allow(dead_code)] // Used in: object operations
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
    #[allow(dead_code)] // Used in: object operations
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
    #[allow(dead_code)] // Used in: object operations
    pub fn swift_to_s3_key(object: &str) -> SwiftResult<String> {
        Self::validate_object_name(object)?;
        Ok(object.to_string())
    }

    /// Convert S3 object key to Swift object name
    ///
    /// This is essentially an identity transformation since we store
    /// Swift object names as-is in S3.
    #[allow(dead_code)] // Used in: object operations
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
    #[allow(dead_code)] // Used in: object operations
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
    #[allow(dead_code)] // Used in: object operations
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
    #[allow(dead_code)] // Used in: object operations
    pub fn encode_object_for_url(object: &str) -> String {
        urlencoding::encode(object).to_string()
    }

    /// Check if object name represents a directory (pseudo-directory)
    ///
    /// In Swift, objects ending with '/' are treated as directory markers.
    #[allow(dead_code)] // Used in: object operations
    pub fn is_directory_marker(object: &str) -> bool {
        object.ends_with('/')
    }

    /// Normalize object path
    ///
    /// Removes redundant slashes and normalizes the path while preserving
    /// trailing slashes for directory markers.
    #[allow(dead_code)] // Used in: object operations
    pub fn normalize_path(object: &str) -> String {
        // Split by '/', filter out empty segments (except if it's the end)
        let segments: Vec<&str> = object.split('/').collect();
        let has_trailing_slash = object.ends_with('/');

        let normalized_segments: Vec<&str> = segments.into_iter().filter(|s| !s.is_empty()).collect();

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

/// Validate metadata against Swift limits
///
/// Checks that:
/// - Total number of metadata entries doesn't exceed MAX_METADATA_COUNT
/// - Individual metadata values don't exceed MAX_METADATA_VALUE_SIZE
///
/// Returns error if limits are exceeded.
fn validate_metadata(metadata: &HashMap<String, String>) -> SwiftResult<()> {
    // Check total metadata count
    if metadata.len() > MAX_METADATA_COUNT {
        return Err(SwiftError::BadRequest(format!(
            "Too many metadata headers: {} (max: {})",
            metadata.len(),
            MAX_METADATA_COUNT
        )));
    }

    // Check individual value sizes
    for (key, value) in metadata.iter() {
        if value.len() > MAX_METADATA_VALUE_SIZE {
            return Err(SwiftError::BadRequest(format!(
                "Metadata value for '{}' too large: {} bytes (max: {} bytes)",
                key,
                value.len(),
                MAX_METADATA_VALUE_SIZE
            )));
        }
    }

    Ok(())
}

/// Sanitize storage layer errors for client responses
///
/// Logs detailed error server-side while returning generic message to client.
/// This prevents information disclosure vulnerabilities.
fn sanitize_storage_error<E: std::fmt::Display>(operation: &str, error: E) -> SwiftError {
    // Log detailed error server-side
    error!("Storage operation '{}' failed: {}", operation, error);

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
#[allow(dead_code)] // Handler integration: PUT object
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
    let mut user_metadata = HashMap::new();
    for (header_name, header_value) in headers.iter() {
        let header_str = header_name.as_str().to_lowercase();
        if let Some(meta_key) = header_str.strip_prefix("x-object-meta-")
            && let Ok(value_str) = header_value.to_str()
        {
            user_metadata.insert(meta_key.to_string(), value_str.to_string());
        }
    }

    // 6. Extract Content-Type if provided
    if let Some(content_type) = headers.get("content-type")
        && let Ok(ct_str) = content_type.to_str()
    {
        user_metadata.insert("content-type".to_string(), ct_str.to_string());
    }

    // 7. Extract and validate expiration headers (X-Delete-At / X-Delete-After)
    if let Some(delete_at) = super::expiration::extract_expiration(headers)? {
        super::expiration::validate_expiration(delete_at)?;
        user_metadata.insert("x-delete-at".to_string(), delete_at.to_string());
    }

    // 8. Extract symlink target if creating a symlink
    if let Some(symlink_target) = super::symlink::extract_symlink_target(headers)? {
        // Store the fully qualified target (container/object)
        let target_value = symlink_target.to_header_value(container);
        user_metadata.insert("x-object-symlink-target".to_string(), target_value);
        debug!("Creating symlink to target: {}", user_metadata.get("x-object-symlink-target").unwrap());
    }

    // 9. Validate metadata limits
    validate_metadata(&user_metadata)?;

    // 10. Get content length from headers (-1 if not provided)
    let content_length = headers
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(-1);

    // 11. Validate content length doesn't exceed maximum
    if content_length > MAX_OBJECT_SIZE {
        return Err(SwiftError::BadRequest(format!(
            "Object size {} bytes exceeds maximum of {} bytes",
            content_length, MAX_OBJECT_SIZE
        )));
    }

    // 12. Get storage layer
    let Some(store) = new_object_layer_fn() else {
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

    // 12. Prepare object options with metadata
    let opts = ObjectOptions {
        user_defined: user_metadata,
        ..Default::default()
    };

    // 13. Wrap reader in buffered reader for streaming hash validation
    let buf_reader = tokio::io::BufReader::new(reader);

    // 14. Create HashReader (no MD5/SHA256 validation for Swift)
    let hash_reader = HashReader::from_stream(buf_reader, content_length, content_length, None, None, false)
        .map_err(|e| sanitize_storage_error("Hash reader creation", e))?;

    // 15. Wrap in PutObjReader as expected by storage layer
    let mut put_reader = PutObjReader::new(hash_reader);

    // 16. Upload object to storage
    let obj_info = store
        .put_object(&bucket, &s3_key, &mut put_reader, &opts)
        .await
        .map_err(|e| sanitize_storage_error("Object upload", e))?;

    // 17. Return ETag (MD5 hash in hex format)
    Ok(obj_info.etag.unwrap_or_default())
}

/// Upload an object with custom metadata (for SLO/DLO internal use)
///
/// Similar to put_object, but allows directly specifying metadata instead of extracting from headers.
/// This is used internally for storing SLO manifests and marker objects.
#[allow(dead_code)] // Used by SLO implementation
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
    // If credentials are provided, validate access
    let project_id = if let Some(creds) = credentials {
        validate_account_access(account, creds)?
    } else {
        // For testing/internal use, extract project_id from account
        account
            .strip_prefix("AUTH_")
            .ok_or_else(|| SwiftError::Unauthorized("Invalid account format".to_string()))?
            .to_string()
    };

    // Validate object name
    ObjectKeyMapper::validate_object_name(object)?;

    // Get S3 key from object name
    let s3_key = ObjectKeyMapper::swift_to_s3_key(object)?;

    // Map container to bucket using tenant prefixing
    let mapper = ContainerMapper::default();
    let bucket = mapper.swift_to_s3_bucket(container, &project_id);

    // Validate metadata limits
    validate_metadata(metadata)?;

    // Get storage layer
    let Some(store) = new_object_layer_fn() else {
        return Err(SwiftError::InternalServerError("Storage layer not initialized".to_string()));
    };

    // Verify bucket/container exists
    store.get_bucket_info(&bucket, &BucketOptions::default()).await.map_err(|e| {
        if e.to_string().contains("does not exist") {
            SwiftError::NotFound(format!("Container '{}' not found", container))
        } else {
            sanitize_storage_error("Container verification", e)
        }
    })?;

    // Prepare object options with metadata
    let opts = ObjectOptions {
        user_defined: metadata.clone(),
        ..Default::default()
    };

    // Content length (use -1 for unknown)
    let content_length = -1i64;

    // Wrap reader in buffered reader for streaming hash validation
    let buf_reader = tokio::io::BufReader::new(reader);

    // Create HashReader
    let hash_reader = HashReader::from_stream(buf_reader, content_length, content_length, None, None, false)
        .map_err(|e| sanitize_storage_error("Hash reader creation", e))?;

    // Wrap in PutObjReader
    let mut put_reader = PutObjReader::new(hash_reader);

    // Upload object to storage
    let obj_info = store
        .put_object(&bucket, &s3_key, &mut put_reader, &opts)
        .await
        .map_err(|e| sanitize_storage_error("Object upload", e))?;

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
#[allow(dead_code)] // Handler integration: GET object
pub async fn get_object(
    account: &str,
    container: &str,
    object: &str,
    credentials: &Credentials,
    range: Option<rustfs_ecstore::store_api::HTTPRangeSpec>,
) -> SwiftResult<rustfs_ecstore::store_api::GetObjectReader> {
    use rustfs_ecstore::store_api::GetObjectReader;

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
    let Some(store) = new_object_layer_fn() else {
        return Err(SwiftError::InternalServerError("Storage layer not initialized".to_string()));
    };

    // 6. Prepare object options
    let opts = ObjectOptions::default();

    // 7. Get object reader from storage with range support
    let reader: GetObjectReader = store
        .get_object_reader(&bucket, &s3_key, range, HeaderMap::new(), &opts)
        .await
        .map_err(|e| {
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
#[allow(dead_code)] // Handler integration: HEAD object
pub async fn head_object(
    account: &str,
    container: &str,
    object: &str,
    credentials: &Credentials,
) -> SwiftResult<rustfs_ecstore::store_api::ObjectInfo> {
    use rustfs_ecstore::store_api::ObjectInfo;

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
    let Some(store) = new_object_layer_fn() else {
        return Err(SwiftError::InternalServerError("Storage layer not initialized".to_string()));
    };

    // 6. Prepare object options
    let opts = ObjectOptions::default();

    // 7. Get object info (metadata only) from storage
    let info: ObjectInfo = store.get_object_info(&bucket, &s3_key, &opts).await.map_err(|e| {
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
#[allow(dead_code)] // Handler integration: DELETE object
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
    let Some(store) = new_object_layer_fn() else {
        return Err(SwiftError::InternalServerError("Storage layer not initialized".to_string()));
    };

    // 6. Prepare object options for deletion
    let opts = ObjectOptions::default();

    // 7. Delete object from storage
    // Swift DELETE is idempotent - returns success even if object doesn't exist
    match store.delete_object(&bucket, &s3_key, opts).await {
        Ok(_) => Ok(()),
        Err(e) => {
            let err_str = e.to_string();
            // Only fail if the container (bucket) doesn't exist
            if err_str.contains("Bucket not found") {
                Err(SwiftError::NotFound(format!("Container '{}' not found", container)))
            } else if err_str.contains("Object not found") || err_str.contains("does not exist") {
                // Object already gone - this is success for idempotent DELETE
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
#[allow(dead_code)] // Handler integration: POST object
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
    let Some(store) = new_object_layer_fn() else {
        return Err(SwiftError::InternalServerError("Storage layer not initialized".to_string()));
    };

    // 6. First, get the existing object info to verify it exists
    let opts = ObjectOptions::default();
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
    let mut new_metadata = HashMap::new();
    for (header_name, header_value) in headers.iter() {
        let header_str = header_name.as_str().to_lowercase();
        if let Some(meta_key) = header_str.strip_prefix("x-object-meta-")
            && let Ok(value_str) = header_value.to_str()
        {
            new_metadata.insert(meta_key.to_string(), value_str.to_string());
        }
    }

    // 9. Also update Content-Type if provided
    if let Some(content_type) = headers.get("content-type")
        && let Ok(ct_str) = content_type.to_str()
    {
        new_metadata.insert("content-type".to_string(), ct_str.to_string());
    }

    // 10. Validate metadata limits
    validate_metadata(&new_metadata)?;

    // 11. Prepare options for metadata update
    // Swift POST replaces all custom metadata, not merges
    let update_opts = ObjectOptions {
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
#[allow(dead_code)] // Handler integration: COPY object
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
    let Some(store) = new_object_layer_fn() else {
        return Err(SwiftError::InternalServerError("Storage layer not initialized".to_string()));
    };

    // 7. First, verify source object exists and get its info
    let src_opts = ObjectOptions::default();
    let mut src_info = store
        .get_object_info(&src_bucket, &src_s3_key, &src_opts)
        .await
        .map_err(|e| {
            let err_str = e.to_string();
            if err_str.contains("does not exist") || err_str.contains("not found") {
                SwiftError::NotFound(format!("Source object '{}' not found in container '{}'", src_object, src_container))
            } else {
                sanitize_storage_error("Source object info retrieval", e)
            }
        })?;

    // 8. Check if source is a delete marker
    if src_info.delete_marker {
        return Err(SwiftError::NotFound(format!(
            "Source object '{}' not found in container '{}'",
            src_object, src_container
        )));
    }

    // 9. Verify destination container exists by trying to get its info
    let bucket_opts = BucketOptions::default();
    let _dst_bucket_info = store.get_bucket_info(&dst_bucket, &bucket_opts).await.map_err(|e| {
        let err_str = e.to_string();
        if err_str.contains("does not exist") || err_str.contains("not found") {
            SwiftError::NotFound(format!("Destination container '{}' not found", dst_container))
        } else {
            sanitize_storage_error("Destination container verification", e)
        }
    })?;

    // 10. Prepare metadata for destination object
    // Start with source metadata
    let mut new_metadata = src_info.user_defined.clone();

    // 11. If custom metadata headers provided, use those instead (Swift behavior)
    let mut has_custom_meta = false;
    for (header_name, header_value) in headers.iter() {
        let header_str = header_name.as_str().to_lowercase();
        if let Some(meta_key) = header_str.strip_prefix("x-object-meta-") {
            if !has_custom_meta {
                // First custom meta header - clear source metadata
                new_metadata.clear();
                has_custom_meta = true;
            }
            if let Ok(value_str) = header_value.to_str() {
                new_metadata.insert(meta_key.to_string(), value_str.to_string());
            }
        }
    }

    // 12. Also check for Content-Type override
    let content_type = if let Some(ct) = headers.get("content-type") {
        ct.to_str().ok().map(|s| s.to_string())
    } else {
        src_info.content_type.clone()
    };

    if let Some(ct) = content_type {
        new_metadata.insert("content-type".to_string(), ct);
    }

    // 13. Validate metadata limits
    validate_metadata(&new_metadata)?;

    // 14. Prepare destination options
    let dst_opts = ObjectOptions {
        user_defined: new_metadata,
        ..Default::default()
    };

    // 15. Perform server-side copy
    let dst_info = store
        .copy_object(&src_bucket, &src_s3_key, &dst_bucket, &dst_s3_key, &mut src_info, &src_opts, &dst_opts)
        .await
        .map_err(|e| sanitize_storage_error("Object copy", e))?;

    // 16. Return the ETag of the destination object
    Ok(dst_info.etag.unwrap_or_default())
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
#[allow(dead_code)] // Handler integration: COPY method
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
#[allow(dead_code)] // Handler integration: X-Copy-From
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
#[allow(dead_code)] // Handler integration: Range header
pub fn parse_range_header(range_str: &str) -> SwiftResult<rustfs_ecstore::store_api::HTTPRangeSpec> {
    use rustfs_ecstore::store_api::HTTPRangeSpec;

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
#[allow(dead_code)] // Handler integration: Range header
pub fn format_content_range(start: i64, end: i64, total: i64) -> String {
    format!("bytes {}-{}/{}", start, end, total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_object_name_valid() {
        assert!(ObjectKeyMapper::validate_object_name("myfile.txt").is_ok());
        assert!(ObjectKeyMapper::validate_object_name("path/to/file.jpg").is_ok());
        assert!(ObjectKeyMapper::validate_object_name("file with spaces.pdf").is_ok());
        assert!(ObjectKeyMapper::validate_object_name("special-chars_@#$.txt").is_ok());
        assert!(ObjectKeyMapper::validate_object_name("unicode-文件.txt").is_ok());
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
}
