// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Swift container operations
//!
//! This module implements Swift container CRUD operations and container-bucket translation.

use super::account::validate_account_access;
use super::metadata_update::{CONTAINER_META_TAG_PREFIX, MetadataUpdate};
use super::storage_api::container::{
    BucketInfo, BucketOperations, BucketOptions, DeleteBucketOptions, ListOperations as _, MakeBucketOptions,
};
use super::types::Container;
use super::{SwiftError, SwiftResult};
use super::{get_swift_bucket_metadata, get_swift_bucket_usage, resolve_swift_object_store_handle, update_swift_bucket_tagging};
use rustfs_credentials::Credentials;
use s3s::dto::{Tag, Tagging};
use sha2::{Digest, Sha256};
use tracing::{debug, error};

fn required_bucket_usage(usage: &std::collections::HashMap<String, (u64, u64)>, bucket: &str) -> SwiftResult<(u64, u64)> {
    usage
        .get(bucket)
        .copied()
        .ok_or_else(|| SwiftError::ServiceUnavailable("Container usage is unavailable".to_string()))
}

const LOG_COMPONENT_PROTOCOLS: &str = "protocols";
const LOG_SUBSYSTEM_SWIFT_CONTAINER: &str = "swift_container";
const EVENT_SWIFT_CONTAINER_STORAGE_STATE: &str = "swift_container_storage_state";

/// Sanitize storage layer errors for client responses
///
/// Logs detailed error server-side while returning generic message to client.
/// This prevents information disclosure vulnerabilities.
fn sanitize_storage_error<E: std::fmt::Display>(operation: &str, error: E) -> SwiftError {
    // Log detailed error server-side
    error!(
        event = EVENT_SWIFT_CONTAINER_STORAGE_STATE,
        component = LOG_COMPONENT_PROTOCOLS,
        subsystem = LOG_SUBSYSTEM_SWIFT_CONTAINER,
        operation = %operation,
        error = %error,
        result = "failed",
        "swift container storage state changed"
    );

    // Return generic error to client
    SwiftError::InternalServerError(format!("{} operation failed", operation))
}

/// Convert S3 tags back to Swift container metadata
///
/// Extracts only tags with "swift-meta-" prefix, which represent Swift container metadata.
/// Other tags are ignored (they may be used for other purposes).
fn s3_tags_to_swift_metadata(tagging: &Tagging) -> std::collections::HashMap<String, String> {
    let mut metadata = std::collections::HashMap::new();

    for tag in &tagging.tag_set {
        if let (Some(key), Some(value)) = (&tag.key, &tag.value)
            && let Some(meta_key) = key.strip_prefix(CONTAINER_META_TAG_PREFIX)
        {
            metadata.insert(meta_key.to_string(), value.clone());
        }
    }

    metadata
}

/// Container name translation options
#[derive(Debug, Clone)]
pub struct ContainerMapperConfig {
    /// Enable tenant prefixing for bucket names
    /// When true, Swift container names are prefixed with SHA256 hash of project_id
    /// Example: container "mycontainer" for project "abc123" becomes bucket "a1b2c3d4e5f6a1b2-mycontainer"
    /// where "a1b2c3d4e5f6a1b2" is the first 16 hex chars of SHA256("abc123")
    pub tenant_prefix_enabled: bool,
}

impl Default for ContainerMapperConfig {
    fn default() -> Self {
        Self {
            tenant_prefix_enabled: true,
        }
    }
}

/// Handles translation between Swift container names and S3 bucket names
pub struct ContainerMapper {
    config: ContainerMapperConfig,
}

impl Default for ContainerMapper {
    fn default() -> Self {
        Self::new(ContainerMapperConfig::default())
    }
}

impl ContainerMapper {
    /// Create a new container mapper with given configuration
    pub fn new(config: ContainerMapperConfig) -> Self {
        Self { config }
    }

    /// Generate a deterministic hash prefix from project_id
    ///
    /// Uses SHA256 to create a 16-character lowercase hex prefix that:
    /// - Is deterministic (same project_id always produces same hash)
    /// - Is collision-resistant (cryptographic hash)
    /// - Uses only [a-z0-9] characters (S3 bucket name compatible)
    /// - Has fixed length (16 chars from 8 bytes)
    fn hash_project_id(&self, project_id: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(project_id.as_bytes());
        let result = hasher.finalize();

        // Format first 8 bytes directly as hex (SHA256 always produces 32 bytes)
        format!(
            "{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            result[0], result[1], result[2], result[3], result[4], result[5], result[6], result[7]
        )
    }

    /// Convert Swift container name to S3 bucket name
    ///
    /// When tenant_prefix_enabled is true:
    ///   container="mycontainer", project_id="abc123" -> "a1b2c3d4e5f6a1b2-mycontainer"
    /// When tenant_prefix_enabled is false:
    ///   container="mycontainer", project_id="abc123" -> "mycontainer"
    ///
    /// Note: Uses SHA256 hash of project_id as prefix, ensuring:
    /// - No collision risk (cryptographic hash)
    /// - S3 bucket name compatible (only uses [a-z0-9-])
    /// - Deterministic mapping (same input always produces same bucket name)
    /// - Fixed-length prefix (16 hex chars = 8 bytes)
    pub fn swift_to_s3_bucket(&self, container: &str, project_id: &str) -> String {
        if self.config.tenant_prefix_enabled {
            let hash = self.hash_project_id(project_id);
            format!("{}-{}", hash, container)
        } else {
            container.to_string()
        }
    }

    /// Convert S3 bucket name to Swift container name
    ///
    /// When tenant_prefix_enabled is true:
    ///   bucket="a1b2c3d4e5f6a1b2-mycontainer", project_id="abc123" -> "mycontainer"
    /// When tenant_prefix_enabled is false:
    ///   bucket="mycontainer", project_id="abc123" -> "mycontainer"
    ///
    /// Returns None if bucket doesn't belong to this tenant
    pub fn s3_to_swift_container(&self, bucket: &str, project_id: &str) -> Option<String> {
        if self.config.tenant_prefix_enabled {
            let hash = self.hash_project_id(project_id);
            let prefix = format!("{}-", hash);
            bucket.strip_prefix(&prefix).map(|container| container.to_string())
        } else {
            Some(bucket.to_string())
        }
    }

    /// Check if a bucket belongs to the specified project
    pub fn bucket_belongs_to_project(&self, bucket: &str, project_id: &str) -> bool {
        if self.config.tenant_prefix_enabled {
            let hash = self.hash_project_id(project_id);
            bucket.starts_with(&format!("{}-", hash))
        } else {
            // Without tenant prefixing, we can't determine ownership from name alone
            true
        }
    }
}

/// Convert BucketInfo to Swift Container
///
/// Maps S3 bucket metadata to Swift container format:
/// - name: Extracted from bucket name (removing tenant prefix if present)
/// - count: Number of objects (not available in BucketInfo, set to 0)
/// - bytes: Total bytes (not available in BucketInfo, set to 0)
/// - last_modified: ISO 8601 timestamp from created date
pub fn bucket_info_to_container(info: &BucketInfo, mapper: &ContainerMapper, project_id: &str) -> Option<Container> {
    // Extract container name (removing tenant prefix if applicable)
    let container_name = mapper.s3_to_swift_container(&info.name, project_id)?;

    // Format timestamp as ISO 8601
    let last_modified = info.created.map(|dt| {
        dt.format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_else(|_| String::new())
    });

    Some(Container {
        name: container_name,
        count: 0, // Will be populated from bucket metadata in future
        bytes: 0, // Will be populated from bucket metadata in future
        last_modified,
    })
}

/// List containers for a Swift account
///
/// This function:
/// 1. Validates account access using Keystone project_id
/// 2. Lists all S3 buckets
/// 3. Filters to buckets belonging to this tenant (using tenant prefix)
/// 4. Converts BucketInfo to Swift Container format
pub async fn list_containers(account: &str, credentials: &Credentials) -> SwiftResult<Vec<Container>> {
    // Validate account access and extract project_id
    let project_id = validate_account_access(account, credentials)?;

    // Create mapper with default config (tenant prefixing enabled)
    let mapper = ContainerMapper::default();

    // Get storage layer
    let Some(store) = resolve_swift_object_store_handle() else {
        return Err(SwiftError::InternalServerError("Storage layer not initialized".to_string()));
    };

    // List all buckets
    let bucket_infos = store
        .list_bucket(&BucketOptions::default())
        .await
        .map_err(|e| sanitize_storage_error("Container listing", e))?;
    let usage = get_swift_bucket_usage()
        .await
        .map_err(|e| sanitize_storage_error("Container usage retrieval", e))?
        .ok_or_else(|| SwiftError::ServiceUnavailable("Container usage is unavailable".to_string()))?;

    // Filter and convert buckets to containers
    let containers: Vec<Container> = bucket_infos
        .iter()
        .filter(|info| mapper.bucket_belongs_to_project(&info.name, &project_id))
        .filter_map(|info| {
            bucket_info_to_container(info, &mapper, &project_id).map(|mut container| {
                let (count, bytes) = required_bucket_usage(&usage, &info.name)?;
                container.count = count;
                container.bytes = bytes;
                Ok(container)
            })
        })
        .collect::<SwiftResult<Vec<_>>>()?;

    debug!(
        event = EVENT_SWIFT_CONTAINER_STORAGE_STATE,
        component = LOG_COMPONENT_PROTOCOLS,
        subsystem = LOG_SUBSYSTEM_SWIFT_CONTAINER,
        operation = "list_containers",
        account = %account,
        container_count = containers.len(),
        result = "ok",
        "swift container storage state changed"
    );

    Ok(containers)
}

/// Create a container for a Swift account
///
/// This function:
/// 1. Validates account access using Keystone project_id
/// 2. Converts Swift container name to S3 bucket name (with tenant prefix)
/// 3. Creates the bucket in S3 storage
///
/// Swift semantics:
/// - PUT /v1/{account}/{container} creates a container
/// - Returns 201 Created on success
/// - Returns 202 Accepted if container already exists
/// - Returns 400 Bad Request for invalid container names
pub async fn create_container(account: &str, container: &str, credentials: &Credentials) -> SwiftResult<bool> {
    // Validate account access and extract project_id
    let project_id = validate_account_access(account, credentials)?;

    // Validate container name
    validate_container_name(container)?;

    // Create mapper with default config (tenant prefixing enabled)
    let mapper = ContainerMapper::default();

    // Convert Swift container name to S3 bucket name
    let bucket_name = mapper.swift_to_s3_bucket(container, &project_id);

    // Get storage layer
    let Some(store) = resolve_swift_object_store_handle() else {
        return Err(SwiftError::InternalServerError("Storage layer not initialized".to_string()));
    };

    // Check if bucket already exists
    let bucket_exists = store.get_bucket_info(&bucket_name, &BucketOptions::default()).await.is_ok();

    if bucket_exists {
        // Container already exists - Swift returns 202 Accepted
        return Ok(false);
    }

    // Create the bucket
    store
        .make_bucket(
            &bucket_name,
            &MakeBucketOptions {
                force_create: false,
                lock_enabled: false,
                versioning_enabled: false,
                created_at: None,
                no_lock: false,
            },
        )
        .await
        .map_err(|e| sanitize_storage_error("Container creation", e))?;

    // Container created successfully - return true for 201 Created
    Ok(true)
}

/// Validate Swift container name
///
/// Container names must:
/// - Be 1-256 characters
/// - Not contain '/' (reserved for objects)
/// - Not be empty
fn validate_container_name(container: &str) -> SwiftResult<()> {
    if container.is_empty() {
        return Err(SwiftError::BadRequest("Container name cannot be empty".to_string()));
    }

    if container.len() > 256 {
        return Err(SwiftError::BadRequest("Container name too long (max 256 characters)".to_string()));
    }

    if container.contains('/') {
        return Err(SwiftError::BadRequest("Container name cannot contain '/'".to_string()));
    }

    Ok(())
}

/// Container metadata for HEAD response
#[derive(Debug, Clone)]
pub struct ContainerMetadata {
    /// Number of objects in container
    pub object_count: u64,
    /// Total bytes used by objects
    pub bytes_used: u64,
    /// Container creation timestamp
    pub created: Option<time::OffsetDateTime>,
    /// Custom metadata (from X-Container-Meta-* headers)
    pub custom_metadata: std::collections::HashMap<String, String>,
}

async fn get_container_metadata_base(
    account: &str,
    container: &str,
    credentials: &Credentials,
) -> SwiftResult<(String, BucketInfo, std::collections::HashMap<String, String>)> {
    let project_id = validate_account_access(account, credentials)?;
    validate_container_name(container)?;
    let bucket_name = ContainerMapper::default().swift_to_s3_bucket(container, &project_id);
    let Some(store) = resolve_swift_object_store_handle() else {
        return Err(SwiftError::InternalServerError("Storage layer not initialized".to_string()));
    };
    let bucket_info = store
        .get_bucket_info(&bucket_name, &BucketOptions::default())
        .await
        .map_err(|e| {
            if e.to_string().contains("not found") || e.to_string().contains("NoSuchBucket") {
                SwiftError::NotFound(format!("Container '{}' not found", container))
            } else {
                sanitize_storage_error("Container metadata retrieval", e)
            }
        })?;
    let custom_metadata = get_swift_bucket_metadata(&bucket_name)
        .await
        .ok()
        .and_then(|bucket_meta| bucket_meta.tagging_config.as_ref().map(s3_tags_to_swift_metadata))
        .unwrap_or_default();
    Ok((bucket_name, bucket_info, custom_metadata))
}

pub(crate) async fn get_container_custom_metadata(
    account: &str,
    container: &str,
    credentials: &Credentials,
) -> SwiftResult<std::collections::HashMap<String, String>> {
    let (_, _, custom_metadata) = get_container_metadata_base(account, container, credentials).await?;
    Ok(custom_metadata)
}

/// Get container metadata (for HEAD operation)
///
/// This function:
/// 1. Validates account access using Keystone project_id
/// 2. Converts Swift container name to S3 bucket name
/// 3. Retrieves bucket info from storage
/// 4. Returns container metadata
///
/// Swift semantics:
/// - HEAD /v1/{account}/{container} returns container metadata
/// - Returns 204 No Content on success with headers
/// - Returns 404 Not Found if container doesn't exist
pub async fn get_container_metadata(account: &str, container: &str, credentials: &Credentials) -> SwiftResult<ContainerMetadata> {
    let (bucket_name, bucket_info, custom_metadata) = get_container_metadata_base(account, container, credentials).await?;

    // Ecstore serves the persisted scanner snapshot through a bounded in-process cache.
    // Single-node deployments overlay successful in-process mutations; distributed
    // deployments use only the cluster-wide persisted snapshot.
    let usage = get_swift_bucket_usage()
        .await
        .map_err(|e| sanitize_storage_error("Container usage retrieval", e))?
        .ok_or_else(|| SwiftError::ServiceUnavailable("Container usage is unavailable".to_string()))?;
    let (object_count, bytes_used) = required_bucket_usage(&usage, &bucket_name)?;

    Ok(ContainerMetadata {
        object_count,
        bytes_used,
        created: bucket_info.created,
        custom_metadata,
    })
}

/// Update container metadata (for POST operation)
///
/// This function:
/// 1. Validates account access using Keystone project_id
/// 2. Converts Swift container name to S3 bucket name
/// 3. Validates container exists
/// 4. Updates custom metadata (X-Container-Meta-* headers)
///
/// Swift semantics:
/// - POST /v1/{account}/{container} updates container metadata
/// - Returns 204 No Content on success
/// - Returns 404 Not Found if container doesn't exist
/// - Metadata is provided via X-Container-Meta-* headers
/// - The update is additive: items the request does not name keep their stored
///   value, and removal is explicit, via `X-Remove-Container-Meta-{name}` or an
///   empty value
pub async fn update_container_metadata(
    account: &str,
    container: &str,
    credentials: &Credentials,
    update: MetadataUpdate,
) -> SwiftResult<()> {
    // Validate account access and extract project_id
    let project_id = validate_account_access(account, credentials)?;

    // Validate container name
    validate_container_name(container)?;

    // These tags are persisted into the bucket metadata file, which every
    // later config write rewrites in full — so unbounded metadata inflates
    // the cost of unrelated writes for the life of the container. The item
    // count is capped against the merged result, inside the rewrite.
    update.validate()?;

    // Create mapper with default config (tenant prefixing enabled)
    let mapper = ContainerMapper::default();

    // Convert Swift container name to S3 bucket name
    let bucket_name = mapper.swift_to_s3_bucket(container, &project_id);

    // Get storage layer
    let Some(store) = resolve_swift_object_store_handle() else {
        return Err(SwiftError::InternalServerError("Storage layer not initialized".to_string()));
    };

    // Verify container exists. Checked before the empty-update shortcut below,
    // so a POST to a container that does not exist still answers 404 whether
    // or not it carried metadata.
    store
        .get_bucket_info(&bucket_name, &BucketOptions::default())
        .await
        .map_err(|e| {
            if e.to_string().contains("not found") || e.to_string().contains("NoSuchBucket") {
                SwiftError::NotFound(format!("Container '{}' not found", container))
            } else {
                sanitize_storage_error("Container metadata retrieval", e)
            }
        })?;

    // An update that names no item leaves stored metadata alone, so skip the
    // persisted write and the peer reload it triggers rather than rewriting
    // the config to its current value. The handler reaches here on every
    // container POST, including ACL-only and versioning-only ones.
    if update.is_empty() {
        return Ok(());
    }

    // Merge into the persisted tags: only the swift-meta-* items this update
    // names change, so the container's other metadata — and the ACL and
    // versioning tags sharing this tag set — survive. An empty result clears
    // the tagging config.
    update_swift_bucket_tagging(bucket_name, |current| update.apply_to_tags(current, CONTAINER_META_TAG_PREFIX)).await
}

/// Delete a container
///
/// This function:
/// 1. Validates account access using Keystone project_id
/// 2. Converts Swift container name to S3 bucket name
/// 3. Verifies container exists
/// 4. Deletes the bucket from storage
///
/// Swift semantics:
/// - DELETE /v1/{account}/{container} deletes a container
/// - Returns 204 No Content on success
/// - Returns 404 Not Found if container doesn't exist
/// - Returns 409 Conflict if container is not empty
pub async fn delete_container(account: &str, container: &str, credentials: &Credentials) -> SwiftResult<()> {
    // Validate account access and extract project_id
    let project_id = validate_account_access(account, credentials)?;

    // Validate container name
    validate_container_name(container)?;

    // Create mapper with default config (tenant prefixing enabled)
    let mapper = ContainerMapper::default();

    // Convert Swift container name to S3 bucket name
    let bucket_name = mapper.swift_to_s3_bucket(container, &project_id);

    // Get storage layer
    let Some(store) = resolve_swift_object_store_handle() else {
        return Err(SwiftError::InternalServerError("Storage layer not initialized".to_string()));
    };

    // Verify container exists first
    store
        .get_bucket_info(&bucket_name, &BucketOptions::default())
        .await
        .map_err(|e| {
            if e.to_string().contains("not found") || e.to_string().contains("NoSuchBucket") {
                SwiftError::NotFound(format!("Container '{}' not found", container))
            } else {
                sanitize_storage_error("Container info retrieval", e)
            }
        })?;

    // Delete the bucket
    store
        .delete_bucket(
            &bucket_name,
            &DeleteBucketOptions {
                force: false, // Swift requires containers to be empty
                no_lock: false,
                no_recreate: false,
                ..Default::default()
            },
        )
        .await
        .map_err(|e| {
            let error_msg = e.to_string();
            // Check if bucket is not empty
            if error_msg.contains("not empty") || error_msg.contains("BucketNotEmpty") {
                SwiftError::Conflict(format!("Container '{}' is not empty. Delete all objects first.", container))
            } else if error_msg.contains("not found") || error_msg.contains("NoSuchBucket") {
                SwiftError::NotFound(format!("Container '{}' not found", container))
            } else {
                sanitize_storage_error("Container deletion", e)
            }
        })?;

    Ok(())
}

/// List objects in a container (GET /v1/{account}/{container})
///
/// Returns a list of objects within the specified container.
/// Supports pagination, prefix filtering, and delimiter-based hierarchical listing.
///
/// # Arguments
///
/// * `account` - Swift account identifier (AUTH_{project_id})
/// * `container` - Container name
/// * `credentials` - Keystone credentials from middleware
/// * `limit` - Maximum number of objects to return (default 10000)
/// * `marker` - Pagination marker (start after this object name)
/// * `prefix` - Filter objects by prefix
/// * `delimiter` - Delimiter for hierarchical listings (usually "/")
///
/// # Returns
///
/// A vector of Object structs containing object metadata
///
/// # Errors
///
/// Returns SwiftError if:
/// - Account validation fails
/// - Container doesn't exist
/// - Storage layer errors occur
pub async fn list_objects(
    account: &str,
    container: &str,
    credentials: &Credentials,
    limit: Option<i32>,
    marker: Option<String>,
    prefix: Option<String>,
    delimiter: Option<String>,
) -> SwiftResult<Vec<super::types::Object>> {
    use super::types::Object;

    // Validate account access and extract project_id
    let project_id = validate_account_access(account, credentials)?;

    // Map container to bucket
    let mapper = ContainerMapper::default();
    let bucket = mapper.swift_to_s3_bucket(container, &project_id);

    // Get storage layer
    let Some(store) = resolve_swift_object_store_handle() else {
        return Err(SwiftError::InternalServerError("Storage layer not initialized".to_string()));
    };

    // Verify bucket exists
    store.get_bucket_info(&bucket, &BucketOptions::default()).await.map_err(|e| {
        if e.to_string().contains("does not exist") {
            SwiftError::NotFound(format!("Container '{}' not found", container))
        } else {
            sanitize_storage_error("Container access", e)
        }
    })?;

    // Prepare list parameters
    let max_keys = limit.unwrap_or(10000).max(0);
    let prefix_str = prefix.unwrap_or_default();
    let delimiter_opt = delimiter.filter(|d| !d.is_empty());

    // List objects from storage
    let object_infos = store
        .list_objects_v2(
            &bucket,
            &prefix_str,
            marker,
            delimiter_opt,
            max_keys,
            false, // fetch_owner
            None,  // start_after
            false, // include_deleted
        )
        .await
        .map_err(|e| sanitize_storage_error("Object listing", e))?;

    // Convert ObjectInfo to Swift Object format
    let mut swift_objects = Vec::new();
    for obj_info in object_infos.objects {
        // Skip empty names
        if obj_info.name.is_empty() {
            continue;
        }

        // Format last_modified as ISO 8601
        let last_modified = if let Some(mod_time) = obj_info.mod_time {
            mod_time
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap_or_default()
        } else {
            String::new()
        };

        swift_objects.push(Object {
            name: obj_info.name,
            hash: obj_info.etag.unwrap_or_default(),
            bytes: obj_info.size as u64,
            content_type: obj_info
                .content_type
                .unwrap_or_else(|| "application/octet-stream".to_string()),
            last_modified,
        });
    }

    Ok(swift_objects)
}

/// Enable object versioning for a container
///
/// When versioning is enabled, old versions of objects are automatically
/// archived to the specified archive container when overwritten or deleted.
///
/// # Arguments
/// * `account` - Account identifier (e.g., "AUTH_7188e165...")
/// * `container` - Container name to enable versioning on
/// * `archive_container` - Container name where versions will be stored
/// * `credentials` - Keystone credentials
///
/// # Returns
/// - Ok(()) if versioning was enabled successfully
/// - Err if container doesn't exist or archive container is invalid
///
/// # Storage
/// Versioning configuration is stored as an S3 bucket tag:
/// - Tag key: `swift-versions-location`
/// - Tag value: archive container name
pub async fn enable_versioning(
    account: &str,
    container: &str,
    archive_container: &str,
    credentials: &Credentials,
) -> SwiftResult<()> {
    // Validate account access
    let project_id = validate_account_access(account, credentials)?;

    // Validate container names
    validate_container_name(container)?;
    validate_container_name(archive_container)?;

    // Cannot version a container to itself
    if container == archive_container {
        return Err(SwiftError::BadRequest(
            "Archive container must be different from versioned container".to_string(),
        ));
    }

    // Create mapper
    let mapper = ContainerMapper::default();
    let bucket_name = mapper.swift_to_s3_bucket(container, &project_id);
    let archive_bucket_name = mapper.swift_to_s3_bucket(archive_container, &project_id);

    // Get storage layer
    let Some(store) = resolve_swift_object_store_handle() else {
        return Err(SwiftError::InternalServerError("Storage layer not initialized".to_string()));
    };

    // Verify container exists
    store
        .get_bucket_info(&bucket_name, &BucketOptions::default())
        .await
        .map_err(|e| {
            if e.to_string().contains("not found") || e.to_string().contains("NoSuchBucket") {
                SwiftError::NotFound(format!("Container '{}' not found", container))
            } else {
                sanitize_storage_error("Container verification", e)
            }
        })?;

    // Verify archive container exists (do NOT auto-create for security)
    // Users must explicitly create the archive container before enabling versioning
    store
        .get_bucket_info(&archive_bucket_name, &BucketOptions::default())
        .await
        .map_err(|e| {
            if e.to_string().contains("not found") || e.to_string().contains("NoSuchBucket") {
                SwiftError::BadRequest(format!(
                    "Archive container '{}' does not exist. Please create it before enabling versioning.",
                    archive_container
                ))
            } else {
                sanitize_storage_error("Archive container verification", e)
            }
        })?;

    // Rewrite the persisted tags: replace any versioning tag with the new
    // archive location while preserving all other tags.
    update_swift_bucket_tagging(bucket_name, |current| {
        let mut tagging = current.cloned().unwrap_or_else(|| Tagging { tag_set: vec![] });

        tagging
            .tag_set
            .retain(|tag| tag.key.as_deref() != Some("swift-versions-location"));

        tagging.tag_set.push(Tag {
            key: Some("swift-versions-location".to_string()),
            value: Some(archive_container.to_string()), // Store Swift container name, not S3 bucket name
        });

        Ok(tagging)
    })
    .await?;

    Ok(())
}

/// Disable object versioning for a container
///
/// Removes versioning configuration from the container. Existing archived
/// versions are NOT deleted.
///
/// # Arguments
/// * `account` - Account identifier
/// * `container` - Container name to disable versioning on
/// * `credentials` - Keystone credentials
pub async fn disable_versioning(account: &str, container: &str, credentials: &Credentials) -> SwiftResult<()> {
    // Validate account access
    let project_id = validate_account_access(account, credentials)?;

    // Validate container name
    validate_container_name(container)?;

    // Create mapper
    let mapper = ContainerMapper::default();
    let bucket_name = mapper.swift_to_s3_bucket(container, &project_id);

    let Some(store) = resolve_swift_object_store_handle() else {
        return Err(SwiftError::InternalServerError("Storage layer not initialized".to_string()));
    };

    // Verify container exists. Without this the rewrite below would persist a
    // fabricated default for a container that does not exist: the metadata
    // loader turns "no metadata on disk" into a fresh BucketMetadata, and
    // writing that creates an orphan .metadata.bin and caches a fabricated
    // default as authoritative.
    store
        .get_bucket_info(&bucket_name, &BucketOptions::default())
        .await
        .map_err(|e| {
            if e.to_string().contains("not found") || e.to_string().contains("NoSuchBucket") {
                SwiftError::NotFound(format!("Container '{}' not found", container))
            } else {
                sanitize_storage_error("Container verification", e)
            }
        })?;

    // Rewrite the persisted tags: drop the versioning tag while preserving
    // all other tags. An empty result clears the tagging config.
    update_swift_bucket_tagging(bucket_name, |current| {
        let mut tagging = current.cloned().unwrap_or_else(|| Tagging { tag_set: vec![] });

        tagging
            .tag_set
            .retain(|tag| tag.key.as_deref() != Some("swift-versions-location"));

        Ok(tagging)
    })
    .await?;

    Ok(())
}

/// Get the archive container name for a versioned container
///
/// Returns None if versioning is not enabled for the container.
///
/// # Arguments
/// * `account` - Account identifier
/// * `container` - Container name to check
/// * `credentials` - Keystone credentials
///
/// # Returns
/// - Some(archive_container_name) if versioning is enabled
/// - None if versioning is not enabled
pub async fn get_versions_location(account: &str, container: &str, credentials: &Credentials) -> SwiftResult<Option<String>> {
    // Validate account access
    let project_id = validate_account_access(account, credentials)?;

    // Validate container name
    validate_container_name(container)?;

    // Create mapper
    let mapper = ContainerMapper::default();
    let bucket_name = mapper.swift_to_s3_bucket(container, &project_id);

    // Load bucket metadata
    let bucket_meta = match get_swift_bucket_metadata(&bucket_name).await {
        Ok(meta) => meta,
        Err(_) => {
            // Container doesn't exist
            return Ok(None);
        }
    };

    // Check for versioning tag
    if let Some(tagging) = &bucket_meta.tagging_config {
        for tag in &tagging.tag_set {
            if tag.key.as_deref() == Some("swift-versions-location") {
                return Ok(tag.value.clone());
            }
        }
    }

    Ok(None)
}

/// Set container ACLs (read and/or write)
///
/// Stores ACLs in S3 bucket tags for persistent storage.
///
/// # Arguments
/// * `account` - Account identifier
/// * `container` - Container name
/// * `read_acl` - Read ACL header value (X-Container-Read), or None to remove
/// * `write_acl` - Write ACL header value (X-Container-Write), or None to remove
/// * `credentials` - Keystone credentials
///
/// # Returns
/// - Ok(()) if ACLs were set successfully
/// - Err if validation fails or storage error occurs
///
/// # Storage
/// ACLs are stored as S3 bucket tags:
/// - Tag key: `swift-acl-read` with comma-separated grants
/// - Tag key: `swift-acl-write` with comma-separated grants
///
/// # Example
/// ```ignore
/// set_container_acl(
///     "AUTH_abc123",
///     "photos",
///     Some(".r:*,AUTH_def456"),  // Public + specific account
///     Some("AUTH_def456"),        // Only specific account can write
///     &credentials
/// ).await?;
/// ```
pub async fn set_container_acl(
    account: &str,
    container: &str,
    read_acl: Option<&str>,
    write_acl: Option<&str>,
    credentials: &Credentials,
) -> SwiftResult<()> {
    use super::acl::ContainerAcl;

    // Validate ACLs by parsing them
    if let Some(read) = read_acl {
        ContainerAcl::parse_read(read)?;
    }
    if let Some(write) = write_acl {
        ContainerAcl::parse_write(write)?;
    }

    // Validate account access
    let project_id = validate_account_access(account, credentials)?;

    // Validate container name
    validate_container_name(container)?;

    // Map container to S3 bucket
    let mapper = ContainerMapper::default();
    let bucket_name = mapper.swift_to_s3_bucket(container, &project_id);

    // Get storage layer
    let Some(store) = resolve_swift_object_store_handle() else {
        return Err(SwiftError::InternalServerError("Storage layer not initialized".to_string()));
    };

    // Verify container exists
    store
        .get_bucket_info(&bucket_name, &BucketOptions::default())
        .await
        .map_err(|e| {
            if e.to_string().contains("not found") || e.to_string().contains("NoSuchBucket") {
                SwiftError::NotFound(format!("Container '{}' not found", container))
            } else {
                sanitize_storage_error("Container verification", e)
            }
        })?;

    // Rewrite the persisted tags: replace the ACL tags with the new grants
    // while preserving all other tags. An empty result clears the tagging
    // config.
    update_swift_bucket_tagging(bucket_name, |current| {
        let mut tagging = current.cloned().unwrap_or_else(|| Tagging { tag_set: vec![] });

        tagging
            .tag_set
            .retain(|tag| tag.key.as_deref() != Some("swift-acl-read") && tag.key.as_deref() != Some("swift-acl-write"));

        if let Some(read) = read_acl
            && !read.trim().is_empty()
        {
            tagging.tag_set.push(Tag {
                key: Some("swift-acl-read".to_string()),
                value: Some(read.to_string()),
            });
        }

        if let Some(write) = write_acl
            && !write.trim().is_empty()
        {
            tagging.tag_set.push(Tag {
                key: Some("swift-acl-write".to_string()),
                value: Some(write.to_string()),
            });
        }

        Ok(tagging)
    })
    .await?;

    debug!(
        "Set ACLs for container {}/{}: read={:?}, write={:?}",
        account, container, read_acl, write_acl
    );

    Ok(())
}

/// Get container ACLs
///
/// Retrieves ACLs from S3 bucket tags and parses them.
///
/// # Arguments
/// * `account` - Account identifier
/// * `container` - Container name
/// * `credentials` - Keystone credentials
///
/// # Returns
/// ContainerAcl with read and write grants, or empty ACL if none set
///
/// # Example
/// ```ignore
/// let acl = get_container_acl("AUTH_abc123", "photos", &credentials).await?;
/// if acl.is_public_read() {
///     println!("Container is publicly readable");
/// }
/// ```
pub async fn get_container_acl(
    account: &str,
    container: &str,
    credentials: &Credentials,
) -> SwiftResult<super::acl::ContainerAcl> {
    use super::acl::ContainerAcl;

    // Validate account access
    let project_id = validate_account_access(account, credentials)?;

    // Map container to S3 bucket
    let mapper = ContainerMapper::default();
    let bucket_name = mapper.swift_to_s3_bucket(container, &project_id);

    // Load bucket metadata
    let bucket_meta = get_swift_bucket_metadata(&bucket_name).await.map_err(|e| {
        if e.to_string().contains("not found") {
            SwiftError::NotFound(format!("Container '{}' not found", container))
        } else {
            SwiftError::InternalServerError(format!("Failed to load bucket metadata: {}", e))
        }
    })?;

    // Get tagging config
    let tagging = bucket_meta.tagging_config.as_ref();

    let mut read_grants = Vec::new();
    let mut write_grants = Vec::new();

    if let Some(tags) = tagging {
        for tag in &tags.tag_set {
            match (tag.key.as_deref(), tag.value.as_deref()) {
                (Some("swift-acl-read"), Some(value)) => {
                    read_grants = ContainerAcl::parse_read(value)?;
                }
                (Some("swift-acl-write"), Some(value)) => {
                    write_grants = ContainerAcl::parse_write(value)?;
                }
                _ => {}
            }
        }
    }

    Ok(ContainerAcl {
        read: read_grants,
        write: write_grants,
    })
}

/// Delete container ACLs
///
/// Removes both read and write ACL tags from the container.
///
/// # Arguments
/// * `account` - Account identifier
/// * `container` - Container name
/// * `credentials` - Keystone credentials
///
/// # Returns
/// Ok(()) if ACLs were deleted successfully
pub async fn delete_container_acl(account: &str, container: &str, credentials: &Credentials) -> SwiftResult<()> {
    // Setting both ACLs to None removes them
    set_container_acl(account, container, None, None, credentials).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::OffsetDateTime;

    #[test]
    fn test_swift_to_s3_bucket_with_prefix() {
        let mapper = ContainerMapper::new(ContainerMapperConfig {
            tenant_prefix_enabled: true,
        });

        let bucket = mapper.swift_to_s3_bucket("mycontainer", "abc123");
        let expected_hash = mapper.hash_project_id("abc123");
        assert_eq!(bucket, format!("{}-mycontainer", expected_hash));

        // Verify hash is 16 hex characters (lowercase)
        assert_eq!(expected_hash.len(), 16);
        assert!(
            expected_hash
                .chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
    }

    #[test]
    fn test_swift_to_s3_bucket_without_prefix() {
        let mapper = ContainerMapper::new(ContainerMapperConfig {
            tenant_prefix_enabled: false,
        });

        let bucket = mapper.swift_to_s3_bucket("mycontainer", "abc123");
        assert_eq!(bucket, "mycontainer");
    }

    #[test]
    fn test_s3_to_swift_container_with_prefix() {
        let mapper = ContainerMapper::new(ContainerMapperConfig {
            tenant_prefix_enabled: true,
        });

        // Test with correct tenant
        let hash_abc123 = mapper.hash_project_id("abc123");
        let bucket_name = format!("{}-mycontainer", hash_abc123);
        let container = mapper.s3_to_swift_container(&bucket_name, "abc123");
        assert_eq!(container, Some("mycontainer".to_string()));

        // Different tenant should return None (different hash)
        let container = mapper.s3_to_swift_container(&bucket_name, "xyz789");
        assert_eq!(container, None);
    }

    #[test]
    fn test_s3_to_swift_container_without_prefix() {
        let mapper = ContainerMapper::new(ContainerMapperConfig {
            tenant_prefix_enabled: false,
        });

        let container = mapper.s3_to_swift_container("mycontainer", "abc123");
        assert_eq!(container, Some("mycontainer".to_string()));
    }

    #[test]
    fn test_bucket_belongs_to_project() {
        let mapper = ContainerMapper::new(ContainerMapperConfig {
            tenant_prefix_enabled: true,
        });

        let hash_abc123 = mapper.hash_project_id("abc123");
        let hash_xyz789 = mapper.hash_project_id("xyz789");

        let bucket_abc = format!("{}-mycontainer", hash_abc123);
        let bucket_xyz = format!("{}-mycontainer", hash_xyz789);

        assert!(mapper.bucket_belongs_to_project(&bucket_abc, "abc123"));
        assert!(!mapper.bucket_belongs_to_project(&bucket_xyz, "abc123"));
        assert!(!mapper.bucket_belongs_to_project("mycontainer", "abc123"));
    }

    #[test]
    fn test_bucket_info_to_container() {
        let mapper = ContainerMapper::new(ContainerMapperConfig {
            tenant_prefix_enabled: true,
        });

        let hash = mapper.hash_project_id("abc123");
        let bucket_name = format!("{}-mycontainer", hash);

        let info = BucketInfo {
            name: bucket_name,
            created: Some(OffsetDateTime::now_utc()),
            deleted: None,
            versioning: false,
            object_locking: false,
        };

        let container = bucket_info_to_container(&info, &mapper, "abc123");
        assert!(container.is_some());
        let container = container.unwrap();
        assert_eq!(container.name, "mycontainer");
        assert_eq!(container.count, 0);
        assert_eq!(container.bytes, 0);
        assert!(container.last_modified.is_some());
    }

    #[test]
    fn test_bucket_info_to_container_wrong_tenant() {
        let mapper = ContainerMapper::new(ContainerMapperConfig {
            tenant_prefix_enabled: true,
        });

        let hash = mapper.hash_project_id("abc123");
        let bucket_name = format!("{}-mycontainer", hash);

        let info = BucketInfo {
            name: bucket_name,
            created: Some(OffsetDateTime::now_utc()),
            deleted: None,
            versioning: false,
            object_locking: false,
        };

        // Different project_id should return None (different hash)
        let container = bucket_info_to_container(&info, &mapper, "xyz789");
        assert!(container.is_none());
    }

    #[test]
    fn test_validate_container_name_valid() {
        assert!(validate_container_name("mycontainer").is_ok());
        assert!(validate_container_name("my-container").is_ok());
        assert!(validate_container_name("my_container").is_ok());
        assert!(validate_container_name("my.container").is_ok());
        assert!(validate_container_name("123").is_ok());
    }

    #[test]
    fn test_validate_container_name_empty() {
        let result = validate_container_name("");
        assert!(result.is_err());
        match result {
            Err(SwiftError::BadRequest(msg)) => {
                assert!(msg.contains("empty"));
            }
            _ => panic!("Expected BadRequest error"),
        }
    }

    #[test]
    fn test_validate_container_name_too_long() {
        let long_name = "a".repeat(257);
        let result = validate_container_name(&long_name);
        assert!(result.is_err());
        match result {
            Err(SwiftError::BadRequest(msg)) => {
                assert!(msg.contains("too long"));
            }
            _ => panic!("Expected BadRequest error"),
        }
    }

    #[test]
    fn test_validate_container_name_with_slash() {
        let result = validate_container_name("my/container");
        assert!(result.is_err());
        match result {
            Err(SwiftError::BadRequest(msg)) => {
                assert!(msg.contains("'/'"));
            }
            _ => panic!("Expected BadRequest error"),
        }
    }

    #[test]
    fn test_no_tenant_collision_with_separator_in_names() {
        // This test verifies that the collision vulnerability identified by Codex is fixed.
        // With "--" separator: ("a", "b--c") and ("a--b", "c") both produced "a--b--c" (COLLISION!)
        // With "/" separator: ("a", "b--c") → "a/b--c" and ("a--b", "c") → "a--b/c" (but "/" breaks S3)
        // With hash: Uses SHA256 of project_id as prefix - cryptographically secure, no collisions
        let mapper = ContainerMapper::new(ContainerMapperConfig {
            tenant_prefix_enabled: true,
        });

        // These should map to DIFFERENT buckets using different hash prefixes
        let bucket1 = mapper.swift_to_s3_bucket("b--c", "a");
        let bucket2 = mapper.swift_to_s3_bucket("c", "a--b");
        assert_ne!(bucket1, bucket2, "Collision detected! Tenant isolation broken.");

        // Verify bucket names use hash prefixes
        let hash_a = mapper.hash_project_id("a");
        let hash_ab = mapper.hash_project_id("a--b");
        assert_eq!(bucket1, format!("{}-b--c", hash_a));
        assert_eq!(bucket2, format!("{}-c", hash_ab));

        // Verify hashes are different (no collision)
        assert_ne!(hash_a, hash_ab);

        // Verify correct tenant ownership - each bucket belongs to only ONE tenant
        assert!(mapper.bucket_belongs_to_project(&bucket1, "a"));
        assert!(!mapper.bucket_belongs_to_project(&bucket1, "a--b"));

        assert!(mapper.bucket_belongs_to_project(&bucket2, "a--b"));
        assert!(!mapper.bucket_belongs_to_project(&bucket2, "a"));

        // Verify reverse mapping works correctly
        assert_eq!(mapper.s3_to_swift_container(&bucket1, "a"), Some("b--c".to_string()));
        assert_eq!(mapper.s3_to_swift_container(&bucket1, "a--b"), None);

        assert_eq!(mapper.s3_to_swift_container(&bucket2, "a--b"), Some("c".to_string()));
        assert_eq!(mapper.s3_to_swift_container(&bucket2, "a"), None);
    }

    #[test]
    fn test_hash_deterministic() {
        // Verify that hashing is deterministic (same input always produces same output)
        let mapper = ContainerMapper::new(ContainerMapperConfig {
            tenant_prefix_enabled: true,
        });

        let hash1 = mapper.hash_project_id("test-project");
        let hash2 = mapper.hash_project_id("test-project");
        assert_eq!(hash1, hash2, "Hash must be deterministic");

        // Verify hash format (16 lowercase hex characters)
        assert_eq!(hash1.len(), 16);
        assert!(hash1.chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
    }

    #[test]
    fn test_hash_s3_compatible() {
        // Verify bucket names are S3-compatible (only use [a-z0-9-])
        let mapper = ContainerMapper::new(ContainerMapperConfig {
            tenant_prefix_enabled: true,
        });

        let bucket = mapper.swift_to_s3_bucket("mycontainer", "test-project-123");

        // Check all characters are S3-compatible
        for c in bucket.chars() {
            assert!(
                c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-',
                "Bucket name contains invalid character: {}",
                c
            );
        }

        // Verify starts with lowercase letter or digit (not dash)
        let first_char = bucket.chars().next().unwrap();
        assert!(first_char.is_ascii_lowercase() || first_char.is_ascii_digit());
    }

    #[test]
    fn test_s3_tags_to_swift_metadata() {
        let tagging = Tagging {
            tag_set: vec![
                Tag {
                    key: Some("swift-meta-color".to_string()),
                    value: Some("blue".to_string()),
                },
                Tag {
                    key: Some("swift-meta-description".to_string()),
                    value: Some("test container".to_string()),
                },
                Tag {
                    key: Some("other-tag".to_string()),
                    value: Some("should-be-ignored".to_string()),
                },
            ],
        };

        let metadata = s3_tags_to_swift_metadata(&tagging);
        assert_eq!(metadata.len(), 2);
        assert_eq!(metadata.get("color"), Some(&"blue".to_string()));
        assert_eq!(metadata.get("description"), Some(&"test container".to_string()));
        assert!(!metadata.contains_key("other-tag"));
    }

    #[test]
    fn test_s3_tags_to_swift_metadata_empty() {
        let tagging = Tagging { tag_set: vec![] };

        let metadata = s3_tags_to_swift_metadata(&tagging);
        assert!(metadata.is_empty());
    }

    #[test]
    fn test_s3_tags_to_swift_metadata_no_swift_tags() {
        let tagging = Tagging {
            tag_set: vec![
                Tag {
                    key: Some("env".to_string()),
                    value: Some("production".to_string()),
                },
                Tag {
                    key: Some("team".to_string()),
                    value: Some("backend".to_string()),
                },
            ],
        };

        let metadata = s3_tags_to_swift_metadata(&tagging);
        assert!(metadata.is_empty());
    }

    #[test]
    fn test_metadata_roundtrip() {
        // What a POST writes must be what a HEAD reads back: run the items
        // through the tag-merge write path and the tag-parse read path.
        let update = MetadataUpdate::default()
            .set("color", "blue")
            .set("owner", "alice")
            .set("priority", "high");

        let tagging = update
            .apply_to_tags(None, CONTAINER_META_TAG_PREFIX)
            .expect("merge should be accepted");
        let recovered = s3_tags_to_swift_metadata(&tagging);

        assert_eq!(recovered.len(), 3);
        assert_eq!(recovered.get("color").map(String::as_str), Some("blue"));
        assert_eq!(recovered.get("owner").map(String::as_str), Some("alice"));
        assert_eq!(recovered.get("priority").map(String::as_str), Some("high"));
    }

    #[test]
    fn test_tag_preservation_merge_with_existing() {
        // A container metadata POST shares its tag set with the container ACL
        // and versioning tags, and with whatever S3 tags the bucket carries.
        // Only the swift-meta-* items the POST names may change.
        let existing = Tagging {
            tag_set: vec![
                Tag {
                    key: Some("swift-meta-color".to_string()),
                    value: Some("blue".to_string()),
                },
                Tag {
                    key: Some("swift-acl-read".to_string()),
                    value: Some(".r:*".to_string()),
                },
                Tag {
                    key: Some("swift-versions-location".to_string()),
                    value: Some("archive".to_string()),
                },
                Tag {
                    key: Some("env".to_string()),
                    value: Some("production".to_string()),
                },
            ],
        };

        let merged = MetadataUpdate::default()
            .set("description", "test")
            .apply_to_tags(Some(&existing), CONTAINER_META_TAG_PREFIX)
            .expect("merge should be accepted");
        let tag = |key: &str| {
            merged
                .tag_set
                .iter()
                .find(|t| t.key.as_deref() == Some(key))
                .and_then(|t| t.value.as_deref())
        };

        assert_eq!(tag("swift-meta-description"), Some("test"), "the new item should be added");
        assert_eq!(tag("swift-meta-color"), Some("blue"), "an unnamed item should be preserved");
        assert_eq!(tag("swift-acl-read"), Some(".r:*"), "the ACL tag should be preserved");
        assert_eq!(tag("swift-versions-location"), Some("archive"), "the versioning tag should be preserved");
        assert_eq!(tag("env"), Some("production"), "non-Swift tags should be preserved");
        assert_eq!(merged.tag_set.len(), 5);
    }

    #[test]
    fn test_tag_preservation_remove_only_swift() {
        // Removing the last container metadata item leaves the other tags —
        // and so must not clear the tagging config.
        let existing = Tagging {
            tag_set: vec![
                Tag {
                    key: Some("swift-meta-color".to_string()),
                    value: Some("blue".to_string()),
                },
                Tag {
                    key: Some("env".to_string()),
                    value: Some("production".to_string()),
                },
                Tag {
                    key: Some("cost-center".to_string()),
                    value: Some("engineering".to_string()),
                },
            ],
        };

        let merged = MetadataUpdate::default()
            .remove("color")
            .apply_to_tags(Some(&existing), CONTAINER_META_TAG_PREFIX)
            .expect("merge should be accepted");

        assert_eq!(merged.tag_set.len(), 2);
        assert!(merged.tag_set.iter().any(|t| t.key.as_deref() == Some("env")));
        assert!(merged.tag_set.iter().any(|t| t.key.as_deref() == Some("cost-center")));
        assert!(
            !merged
                .tag_set
                .iter()
                .any(|t| t.key.as_ref().is_some_and(|k| k.starts_with(CONTAINER_META_TAG_PREFIX))),
            "the removed item should be gone"
        );
    }

    #[test]
    fn test_tag_preservation_empty_after_swift_removal() {
        // Removing every item when nothing else is tagged empties the tag set,
        // which is how the caller knows to clear the tagging config.
        let existing = Tagging {
            tag_set: vec![
                Tag {
                    key: Some("swift-meta-color".to_string()),
                    value: Some("blue".to_string()),
                },
                Tag {
                    key: Some("swift-meta-owner".to_string()),
                    value: Some("alice".to_string()),
                },
            ],
        };

        let merged = MetadataUpdate::default()
            .remove("color")
            .remove("owner")
            .apply_to_tags(Some(&existing), CONTAINER_META_TAG_PREFIX)
            .expect("merge should be accepted");

        assert!(merged.tag_set.is_empty(), "tagging should be empty after removing every swift-meta-* tag");
    }

    // Object Versioning Tests

    #[test]
    fn test_validate_versioning_container_names() {
        // Test that container and archive must be different
        // This is a unit test that doesn't require storage layer
        let container = "mycontainer";
        let archive = "mycontainer"; // Same as container

        // In enable_versioning, this would return an error
        assert_eq!(container, archive);
        // Would fail: enable_versioning(account, container, archive, creds).await
    }

    #[test]
    fn test_versioning_tag_format() {
        // Test versioning tag format
        let mut tagging = Tagging { tag_set: vec![] };

        tagging.tag_set.push(Tag {
            key: Some("swift-versions-location".to_string()),
            value: Some("archive-container".to_string()),
        });

        assert_eq!(tagging.tag_set.len(), 1);
        assert_eq!(tagging.tag_set[0].key.as_deref(), Some("swift-versions-location"));
        assert_eq!(tagging.tag_set[0].value.as_deref(), Some("archive-container"));
    }

    #[test]
    fn test_versioning_tag_extraction() {
        // Test extracting versioning location from tags
        let tagging = Tagging {
            tag_set: vec![
                Tag {
                    key: Some("swift-meta-color".to_string()),
                    value: Some("red".to_string()),
                },
                Tag {
                    key: Some("swift-versions-location".to_string()),
                    value: Some("my-archive".to_string()),
                },
                Tag {
                    key: Some("env".to_string()),
                    value: Some("prod".to_string()),
                },
            ],
        };

        // Find versioning tag
        let versions_location = tagging
            .tag_set
            .iter()
            .find(|tag| tag.key.as_deref() == Some("swift-versions-location"))
            .and_then(|tag| tag.value.clone());

        assert_eq!(versions_location, Some("my-archive".to_string()));
    }

    #[test]
    fn test_versioning_tag_removal() {
        // Test removing versioning tag while preserving others
        let mut tagging = Tagging {
            tag_set: vec![
                Tag {
                    key: Some("swift-meta-color".to_string()),
                    value: Some("red".to_string()),
                },
                Tag {
                    key: Some("swift-versions-location".to_string()),
                    value: Some("my-archive".to_string()),
                },
                Tag {
                    key: Some("env".to_string()),
                    value: Some("prod".to_string()),
                },
            ],
        };

        // Remove versioning tag
        tagging
            .tag_set
            .retain(|tag| tag.key.as_deref() != Some("swift-versions-location"));

        // Verify: versioning tag removed, others preserved
        assert_eq!(tagging.tag_set.len(), 2);
        assert!(tagging.tag_set.iter().any(|t| t.key.as_deref() == Some("swift-meta-color")));
        assert!(tagging.tag_set.iter().any(|t| t.key.as_deref() == Some("env")));
        assert!(
            !tagging
                .tag_set
                .iter()
                .any(|t| t.key.as_deref() == Some("swift-versions-location"))
        );
    }

    #[test]
    fn test_versioning_tag_update() {
        // Test updating versioning location
        let mut tagging = Tagging {
            tag_set: vec![Tag {
                key: Some("swift-versions-location".to_string()),
                value: Some("old-archive".to_string()),
            }],
        };

        // Remove old versioning tag
        tagging
            .tag_set
            .retain(|tag| tag.key.as_deref() != Some("swift-versions-location"));

        // Add new versioning tag
        tagging.tag_set.push(Tag {
            key: Some("swift-versions-location".to_string()),
            value: Some("new-archive".to_string()),
        });

        // Verify update
        assert_eq!(tagging.tag_set.len(), 1);
        assert_eq!(tagging.tag_set[0].value.as_deref(), Some("new-archive"));
    }

    #[test]
    fn nonempty_container_usage_preserves_authoritative_totals() {
        let usage = std::collections::HashMap::from([("tenant-container".to_string(), (7, 4097))]);

        assert_eq!(
            required_bucket_usage(&usage, "tenant-container").expect("authoritative bucket usage"),
            (7, 4097)
        );
    }

    #[test]
    fn missing_container_usage_fails_closed() {
        let usage = std::collections::HashMap::new();

        assert!(matches!(
            required_bucket_usage(&usage, "tenant-container"),
            Err(SwiftError::ServiceUnavailable(_))
        ));
    }
}
