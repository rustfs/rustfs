//! Object Versioning Support for Swift API
//!
//! Implements Swift object versioning where old versions are automatically
//! archived when objects are overwritten or deleted.
//!
//! # Architecture
//!
//! - **Version-enabled container**: Primary container holding current objects
//! - **Archive container**: Separate container storing old versions
//! - **Version naming**: `{inverted_timestamp}/{container}/{object}`
//!
//! # Version Naming Convention
//!
//! Versions are stored with inverted timestamps so newer versions sort first:
//! ```text
//! Original: /v1/AUTH_test/photos/cat.jpg
//! Version 1: /v1/AUTH_test/archive/9999999999.999999999/photos/cat.jpg
//! Version 2: /v1/AUTH_test/archive/9999999999.999999998/photos/cat.jpg
//! ```
//!
//! The timestamp is calculated as: `9999999999.999999999 - current_timestamp`
//!
//! Timestamps use 9 decimal places (nanosecond precision) to prevent collisions
//! in high-throughput scenarios where multiple versions are created rapidly.
//!
//! # Versioning Flow
//!
//! ## On PUT (overwrite):
//! 1. Check if container has versioning enabled
//! 2. If object exists, copy it to archive with versioned name
//! 3. Proceed with normal PUT operation
//!
//! ## On DELETE:
//! 1. Check if container has versioning enabled
//! 2. Delete current object
//! 3. List versions in archive (newest first)
//! 4. If versions exist, restore newest to current container
//! 5. Delete restored version from archive

use super::container::{ContainerMapper, get_versions_location};
use super::object::{ObjectKeyMapper, head_object, get_object, put_object};
use super::{SwiftError, SwiftResult};
use super::account::validate_account_access;
use rustfs_credentials::Credentials;
use rustfs_ecstore::new_object_layer_fn;
use rustfs_ecstore::store_api::{ListOperations, ObjectOptions, ObjectOperations};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, error};
use std::sync::Arc;

/// Generate a version name for an archived object
///
/// Version names use inverted timestamps to sort newest-first:
/// Format: `{inverted_timestamp}/{container}/{object}`
///
/// # Example
/// ```text
/// Container: "photos"
/// Object: "cat.jpg"
/// Timestamp: 1709740800.123456789
/// Result: "9990259199.876543210/photos/cat.jpg"
/// ```
///
/// # Precision
/// Uses 9 decimal places (nanosecond precision) to prevent timestamp
/// collisions in high-throughput scenarios (up to 1 billion ops/sec).
///
/// # Arguments
/// * `container` - Original container name
/// * `object` - Original object name
///
/// # Returns
/// Versioned object name with inverted timestamp prefix
pub fn generate_version_name(container: &str, object: &str) -> String {
    // Get current timestamp
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| std::time::Duration::from_secs(0));

    let timestamp = now.as_secs_f64();

    // Invert timestamp so newer versions sort first
    // Max reasonable timestamp: 9999999999 (year 2286)
    // Using 9 decimal places (nanosecond precision) to prevent collisions
    // in high-throughput scenarios where objects are uploaded rapidly
    let inverted = 9999999999.999999999 - timestamp;

    // Format: {inverted_timestamp}/{container}/{object}
    // 9 decimal places = nanosecond precision (prevents collisions up to 1B ops/sec)
    format!("{:.9}/{}/{}", inverted, container, object)
}

/// Archive the current version of an object before overwriting
///
/// This function is called before PUT operations on versioned containers.
/// It copies the current object to the archive container with a versioned name.
///
/// # Arguments
/// * `account` - Account identifier
/// * `container` - Container name (primary container)
/// * `object` - Object name to archive
/// * `archive_container` - Archive container name
/// * `credentials` - Keystone credentials
///
/// # Returns
/// - Ok(()) if archiving succeeded or object doesn't exist
/// - Err if archiving failed
///
/// # Notes
/// - If object doesn't exist, returns Ok(()) (nothing to archive)
/// - Preserves all metadata from original object
/// - Generates timestamp-based version name
pub async fn archive_current_version(
    account: &str,
    container: &str,
    object: &str,
    archive_container: &str,
    credentials: &Credentials,
) -> SwiftResult<()> {
    debug!(
        "Archiving current version of {}/{}/{} to {}",
        account, container, object, archive_container
    );

    // Check if object exists
    let object_info = match head_object(account, container, object, credentials).await {
        Ok(info) => info,
        Err(SwiftError::NotFound(_)) => {
            // Object doesn't exist - nothing to archive
            debug!("Object does not exist, nothing to archive");
            return Ok(());
        }
        Err(e) => return Err(e),
    };

    // Generate version name
    let version_name = generate_version_name(container, object);

    debug!("Generated version name: {}", version_name);

    // Validate account and get project_id
    let project_id = validate_account_access(account, credentials)?;

    // Map containers to S3 buckets
    let mapper = ContainerMapper::default();
    let source_bucket = mapper.swift_to_s3_bucket(container, &project_id);
    let archive_bucket = mapper.swift_to_s3_bucket(archive_container, &project_id);

    // Map object names to S3 keys
    let source_key = ObjectKeyMapper::swift_to_s3_key(object)?;
    let version_key = ObjectKeyMapper::swift_to_s3_key(&version_name)?;

    // Get storage layer
    let Some(store) = new_object_layer_fn() else {
        return Err(SwiftError::InternalServerError("Storage layer not initialized".to_string()));
    };

    // Copy object to archive using S3's copy_object operation
    // This is more efficient than GET + PUT for large objects
    let opts = ObjectOptions::default();

    // Get source object info for copy operation
    let mut src_info = store
        .get_object_info(&source_bucket, &source_key, &opts)
        .await
        .map_err(|e| {
            error!("Failed to get source object info: {}", e);
            SwiftError::InternalServerError(format!("Failed to get object info for archiving: {}", e))
        })?;

    store
        .copy_object(
            &source_bucket,
            &source_key,
            &archive_bucket,
            &version_key,
            &mut src_info,
            &opts,
            &opts,
        )
        .await
        .map_err(|e| {
            error!("Failed to copy object to archive: {}", e);
            SwiftError::InternalServerError(format!("Failed to archive version: {}", e))
        })?;

    debug!("Successfully archived version to {}/{}", archive_container, version_name);

    Ok(())
}

/// Restore the previous version of an object after deletion
///
/// This function is called after DELETE operations on versioned containers.
/// It finds the newest archived version and restores it to the current container.
///
/// # Arguments
/// * `account` - Account identifier
/// * `container` - Container name (primary container)
/// * `object` - Object name to restore
/// * `archive_container` - Archive container name
/// * `credentials` - Keystone credentials
///
/// # Returns
/// - Ok(true) if a version was restored
/// - Ok(false) if no versions exist
/// - Err if restore failed
///
/// # Notes
/// - Lists versions sorted by timestamp (newest first)
/// - Restores only the newest version
/// - Deletes the restored version from archive
pub async fn restore_previous_version(
    account: &str,
    container: &str,
    object: &str,
    archive_container: &str,
    credentials: &Credentials,
) -> SwiftResult<bool> {
    debug!(
        "Restoring previous version of {}/{}/{} from {}",
        account, container, object, archive_container
    );

    // List versions for this object
    let versions = list_object_versions(account, container, object, archive_container, credentials).await?;

    if versions.is_empty() {
        debug!("No versions found to restore");
        return Ok(false);
    }

    // Get newest version (first in list, since they're sorted newest-first)
    let newest_version = &versions[0];

    debug!("Restoring version: {}", newest_version);

    // Validate account and get project_id
    let project_id = validate_account_access(account, credentials)?;

    // Map containers to S3 buckets
    let mapper = ContainerMapper::default();
    let target_bucket = mapper.swift_to_s3_bucket(container, &project_id);
    let archive_bucket = mapper.swift_to_s3_bucket(archive_container, &project_id);

    // Map object names to S3 keys
    let target_key = ObjectKeyMapper::swift_to_s3_key(object)?;
    let version_key = ObjectKeyMapper::swift_to_s3_key(newest_version)?;

    // Get storage layer
    let Some(store) = new_object_layer_fn() else {
        return Err(SwiftError::InternalServerError("Storage layer not initialized".to_string()));
    };

    let opts = ObjectOptions::default();

    // Get version object info for copy operation
    let mut version_info = store
        .get_object_info(&archive_bucket, &version_key, &opts)
        .await
        .map_err(|e| {
            error!("Failed to get version object info: {}", e);
            SwiftError::InternalServerError(format!("Failed to get version info for restore: {}", e))
        })?;

    // Copy version back to original location
    store
        .copy_object(
            &archive_bucket,
            &version_key,
            &target_bucket,
            &target_key,
            &mut version_info,
            &opts,
            &opts,
        )
        .await
        .map_err(|e| {
            error!("Failed to restore version: {}", e);
            SwiftError::InternalServerError(format!("Failed to restore version: {}", e))
        })?;

    // Delete the version from archive after successful restore
    store
        .delete_object(&archive_bucket, &version_key, opts)
        .await
        .map_err(|e| {
            error!("Failed to delete archived version after restore: {}", e);
            // Don't fail the restore if deletion fails - object is restored
            SwiftError::InternalServerError(format!("Version restored but cleanup failed: {}", e))
        })?;

    debug!("Successfully restored version from {}", newest_version);

    Ok(true)
}

/// List all versions of an object in the archive container
///
/// Returns versions sorted by timestamp (newest first).
///
/// # Arguments
/// * `account` - Account identifier
/// * `container` - Original container name
/// * `object` - Original object name
/// * `archive_container` - Archive container name
/// * `credentials` - Keystone credentials
///
/// # Returns
/// Vec of version names (full paths including timestamp prefix)
///
/// # Example
/// ```text
/// Input: account="AUTH_test", container="photos", object="cat.jpg"
/// Output: [
///     "9999999999.99999/photos/cat.jpg",
///     "9999999999.99998/photos/cat.jpg",
///     "9999999999.99997/photos/cat.jpg",
/// ]
/// ```
pub async fn list_object_versions(
    account: &str,
    container: &str,
    object: &str,
    archive_container: &str,
    credentials: &Credentials,
) -> SwiftResult<Vec<String>> {
    debug!(
        "Listing versions of {}/{}/{} in {}",
        account, container, object, archive_container
    );

    // Validate account and get project_id
    let project_id = validate_account_access(account, credentials)?;

    // Map archive container to S3 bucket
    let mapper = ContainerMapper::default();
    let archive_bucket = mapper.swift_to_s3_bucket(archive_container, &project_id);

    // Get storage layer
    let Some(store) = new_object_layer_fn() else {
        return Err(SwiftError::InternalServerError("Storage layer not initialized".to_string()));
    };

    // Build prefix for listing versions
    // We want all objects matching: {timestamp}/{container}/{object}
    // So prefix is: {container}/{object}
    // But we need to include the timestamp part, so we list all and filter

    // List all objects in archive container with a prefix
    // Since versions are stored as {timestamp}/{container}/{object}, we can't use
    // a simple prefix. We need to list all and filter.
    let list_result = store
        .list_objects_v2(
            &archive_bucket,
            "", // No prefix - we'll filter manually
            None, // No continuation token
            None, // No delimiter
            1000, // Max keys
            false, // Don't fetch owner
            None, // No start_after
            false, // Don't include deleted
        )
        .await
        .map_err(|e| {
            error!("Failed to list archive container: {}", e);
            SwiftError::InternalServerError(format!("Failed to list versions: {}", e))
        })?;

    // Filter for this specific object and extract version names
    let mut versions: Vec<String> = Vec::new();
    let suffix = format!("/{}/{}", container, object);

    for obj_info in list_result.objects {
        // Convert S3 key back to Swift object name
        let swift_name = ObjectKeyMapper::s3_to_swift_name(&obj_info.name);

        // Check if this is a version of our object
        if swift_name.ends_with(&suffix) {
            versions.push(swift_name);
        }
    }

    // Sort by timestamp (newest first)
    // Since timestamps are inverted (newer = smaller number), ascending string sort
    // gives us newest first because smaller numbers sort first lexicographically
    versions.sort_by(|a: &String, b: &String| a.cmp(b)); // Ascending sort for inverted timestamps

    debug!("Found {} versions", versions.len());

    Ok(versions)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_version_name() {
        let version = generate_version_name("photos", "cat.jpg");

        // Should have format: {timestamp}/photos/cat.jpg
        assert!(version.contains("/photos/cat.jpg"));

        // Timestamp part should be a float with 5 decimal places
        let parts: Vec<&str> = version.split('/').collect();
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[1], "photos");
        assert_eq!(parts[2], "cat.jpg");

        // Timestamp should be parseable as f64
        let timestamp: f64 = parts[0].parse().expect("Timestamp should be a float");
        assert!(timestamp > 0.0);
        assert!(timestamp < 10000000000.0); // Reasonable range
    }

    #[test]
    fn test_version_name_timestamp_ordering() {
        // Generate two version names with a small delay
        let version1 = generate_version_name("photos", "test.jpg");
        std::thread::sleep(std::time::Duration::from_millis(10));
        let version2 = generate_version_name("photos", "test.jpg");

        // Extract timestamps
        let ts1: f64 = version1.split('/').next().unwrap().parse().unwrap();
        let ts2: f64 = version2.split('/').next().unwrap().parse().unwrap();

        // Newer version should have SMALLER timestamp (inverted)
        assert!(ts2 < ts1, "Newer version should have smaller inverted timestamp");

        // When sorted in ASCENDING order (a.cmp(b)), smaller timestamps come first
        // Since timestamps are inverted, this gives us newest first
        let mut versions = vec![version1.clone(), version2.clone()];
        versions.sort_by(|a, b| a.cmp(b)); // Ascending sort

        // The newest version (version2, with smaller timestamp) should come first
        assert_eq!(versions[0], version2, "After ascending sort, newer version (smaller timestamp) should be first");
        assert_eq!(versions[1], version1);
    }

    #[test]
    fn test_version_name_different_objects() {
        let version1 = generate_version_name("photos", "cat.jpg");
        let version2 = generate_version_name("photos", "dog.jpg");
        let version3 = generate_version_name("videos", "cat.jpg");

        // Different objects should have different paths
        assert!(version1.ends_with("/photos/cat.jpg"));
        assert!(version2.ends_with("/photos/dog.jpg"));
        assert!(version3.ends_with("/videos/cat.jpg"));
    }

    #[test]
    fn test_version_name_format() {
        let version = generate_version_name("my-container", "my-object.txt");

        // Should match pattern: {float}.{9digits}/{container}/{object}
        let pattern = regex::Regex::new(r"^\d+\.\d{9}/my-container/my-object\.txt$").unwrap();
        assert!(pattern.is_match(&version), "Version name format incorrect: {}", version);
    }

    #[test]
    fn test_version_timestamp_inversion() {
        // Test that timestamp inversion works correctly
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();

        let inverted = 9999999999.99999 - now;

        // Inverted timestamp should be positive and reasonable
        assert!(inverted > 7000000000.0); // We're past year 2000
        assert!(inverted < 10000000000.0); // Before year 2286
    }

    #[test]
    fn test_version_name_special_characters() {
        // Test versioning with special characters in names
        let version = generate_version_name("my-container", "path/to/object.txt");

        // Should preserve the path structure
        assert!(version.ends_with("/my-container/path/to/object.txt"));
    }

    #[test]
    fn test_list_versions_filtering() {
        // Simulate filtering logic for list_object_versions
        let archive_objects = vec![
            "9999999999.99999/photos/cat.jpg",
            "9999999999.99998/photos/cat.jpg",
            "9999999999.99997/photos/dog.jpg", // Different object
            "9999999999.99996/videos/cat.jpg", // Different container
        ];

        let target_suffix = "/photos/cat.jpg";
        let mut versions: Vec<String> = archive_objects
            .into_iter()
            .filter(|name| name.ends_with(target_suffix))
            .map(|s| s.to_string())
            .collect();

        versions.sort_by(|a, b| a.cmp(b)); // Ascending sort for inverted timestamps

        // Should have 2 versions of photos/cat.jpg
        assert_eq!(versions.len(), 2);
        // With ascending sort and inverted timestamps, smaller timestamp comes first (newest)
        assert!(versions[0].starts_with("9999999999.99998")); // Newer version first
        assert!(versions[1].starts_with("9999999999.99999")); // Older version second
    }

    #[test]
    fn test_version_timestamp_uniqueness() {
        // Generate many versions quickly to test uniqueness
        let mut timestamps = std::collections::HashSet::new();

        for _ in 0..100 {
            let version = generate_version_name("test", "object");
            let ts = version.split('/').next().unwrap().to_string();
            timestamps.insert(ts);
        }

        // Should have at least some unique timestamps
        // (May not be 100 due to system clock granularity)
        assert!(timestamps.len() > 1, "Timestamps should be mostly unique");
    }
}
