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

//! Swift account operations and validation

use super::{SwiftError, SwiftResult};
use rustfs_credentials::Credentials;
use rustfs_ecstore::new_object_layer_fn;
use rustfs_ecstore::store_api::{BucketOperations, MakeBucketOptions};
use s3s::dto::{Tag, Tagging};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use time;

/// Validate that the authenticated user has access to the requested account
///
/// This function ensures tenant isolation by verifying that the account
/// in the URL matches the project_id from the Keystone credentials.
///
/// # Arguments
///
/// * `account` - Account identifier from URL (e.g., "AUTH_7188e165...")
/// * `credentials` - Keystone credentials from middleware
///
/// # Returns
///
/// The project_id if validation succeeds, or an error if:
/// - Account format is invalid
/// - Credentials don't contain project_id
/// - Account project_id doesn't match credentials project_id
#[allow(dead_code)] // Used by Swift implementation
pub fn validate_account_access(account: &str, credentials: &Credentials) -> SwiftResult<String> {
    // Extract project_id from account (strip "AUTH_" prefix)
    let account_project_id = account
        .strip_prefix("AUTH_")
        .ok_or_else(|| SwiftError::BadRequest(format!("Invalid account format: {}. Expected AUTH_{{project_id}}", account)))?;

    // Get project_id from Keystone credentials
    let cred_project_id = credentials
        .claims
        .as_ref()
        .and_then(|claims| claims.get("keystone_project_id"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            SwiftError::Unauthorized("Missing project_id in credentials. Keystone authentication required.".to_string())
        })?;

    // Verify account matches authenticated project
    if account_project_id != cred_project_id {
        return Err(SwiftError::Forbidden(format!(
            "Access denied. Account {} does not match authenticated project {}",
            account_project_id, cred_project_id
        )));
    }

    Ok(cred_project_id.to_string())
}

/// Check if user has admin privileges
///
/// Admin users (with "admin" or "reseller_admin" roles) can perform
/// cross-tenant operations and administrative tasks.
#[allow(dead_code)] // Used by Swift implementation
pub fn is_admin_user(credentials: &Credentials) -> bool {
    credentials
        .claims
        .as_ref()
        .and_then(|claims| claims.get("keystone_roles"))
        .and_then(|roles| roles.as_array())
        .map(|roles| {
            roles
                .iter()
                .any(|r| r.as_str().map(|s| s == "admin" || s == "reseller_admin").unwrap_or(false))
        })
        .unwrap_or(false)
}

/// Get account metadata bucket name
///
/// Account metadata is stored in a special S3 bucket named after
/// the hashed account identifier. This allows storing TempURL keys
/// and other account-level metadata.
///
/// # Format
/// ```text
/// swift-account-{sha256(account)[0..16]}
/// ```
fn get_account_metadata_bucket_name(account: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(account.as_bytes());
    let hash_bytes = hasher.finalize();
    let hash = hex::encode(hash_bytes);
    format!("swift-account-{}", &hash[0..16])
}

/// Get account metadata from S3 bucket tags
///
/// Retrieves account-level metadata such as TempURL keys.
/// Metadata is stored as S3 bucket tags with the prefix `swift-account-meta-`.
///
/// # Arguments
/// * `account` - Account identifier (e.g., "AUTH_7188e165...")
/// * `credentials` - S3 credentials for accessing the metadata bucket
///
/// # Returns
/// HashMap of metadata key-value pairs (without the prefix)
pub async fn get_account_metadata(
    account: &str,
    _credentials: &Option<Credentials>,
) -> SwiftResult<HashMap<String, String>> {
    let bucket_name = get_account_metadata_bucket_name(account);

    // Try to load bucket metadata
    let bucket_meta = match rustfs_ecstore::bucket::metadata_sys::get(&bucket_name).await {
        Ok(meta) => meta,
        Err(_) => {
            // Bucket doesn't exist - return empty metadata
            return Ok(HashMap::new());
        }
    };

    // Extract metadata from bucket tags
    let mut metadata = HashMap::new();
    if let Some(tagging) = &bucket_meta.tagging_config {
        for tag in &tagging.tag_set {
            if let (Some(key), Some(value)) = (&tag.key, &tag.value) {
                if key.starts_with("swift-account-meta-") {
                    let meta_key = &key[19..]; // Strip "swift-account-meta-" prefix
                    metadata.insert(meta_key.to_string(), value.clone());
                }
            }
        }
    }

    Ok(metadata)
}

/// Update account metadata (stored in S3 bucket tags)
///
/// Updates account-level metadata such as TempURL keys.
/// Only updates swift-account-meta-* tags, preserving other tags.
///
/// # Arguments
/// * `account` - Account identifier
/// * `metadata` - Metadata key-value pairs to store (keys will be prefixed with `swift-account-meta-`)
/// * `credentials` - S3 credentials
pub async fn update_account_metadata(
    account: &str,
    metadata: &HashMap<String, String>,
    _credentials: &Option<Credentials>,
) -> SwiftResult<()> {
    let bucket_name = get_account_metadata_bucket_name(account);

    let Some(store) = new_object_layer_fn() else {
        return Err(SwiftError::InternalServerError("Storage layer not initialized".to_string()));
    };

    // Create bucket if it doesn't exist
    let bucket_exists = rustfs_ecstore::bucket::metadata_sys::get(&bucket_name).await.is_ok();
    if !bucket_exists {
        // Create bucket for account metadata
        store
            .make_bucket(&bucket_name, &MakeBucketOptions::default())
            .await
            .map_err(|e| SwiftError::InternalServerError(format!("Failed to create account metadata bucket: {}", e)))?;
    }

    // Load current bucket metadata
    let bucket_meta = rustfs_ecstore::bucket::metadata_sys::get(&bucket_name)
        .await
        .map_err(|e| SwiftError::InternalServerError(format!("Failed to load bucket metadata: {}", e)))?;

    let mut bucket_meta_clone = (*bucket_meta).clone();

    // Get existing tags, preserving non-Swift tags
    let mut existing_tagging = bucket_meta_clone.tagging_config.clone()
        .unwrap_or_else(|| Tagging { tag_set: vec![] });

    // Remove old swift-account-meta-* tags while preserving other tags
    existing_tagging.tag_set.retain(|tag| {
        if let Some(key) = &tag.key {
            !key.starts_with("swift-account-meta-")
        } else {
            true
        }
    });

    // Add new metadata tags
    for (key, value) in metadata {
        existing_tagging.tag_set.push(Tag {
            key: Some(format!("swift-account-meta-{}", key)),
            value: Some(value.clone()),
        });
    }

    let now = time::OffsetDateTime::now_utc();

    if existing_tagging.tag_set.is_empty() {
        // No tags remain; clear tagging config
        bucket_meta_clone.tagging_config_xml = Vec::new();
        bucket_meta_clone.tagging_config_updated_at = now;
        bucket_meta_clone.tagging_config = None;
    } else {
        // Serialize tags to XML
        let tagging_xml = quick_xml::se::to_string(&existing_tagging)
            .map_err(|e| SwiftError::InternalServerError(format!("Failed to serialize tags: {}", e)))?;

        bucket_meta_clone.tagging_config_xml = tagging_xml.into_bytes();
        bucket_meta_clone.tagging_config_updated_at = now;
        bucket_meta_clone.tagging_config = Some(existing_tagging);
    }

    // Save updated metadata
    rustfs_ecstore::bucket::metadata_sys::set_bucket_metadata(bucket_name.clone(), bucket_meta_clone)
        .await
        .map_err(|e| SwiftError::InternalServerError(format!("Failed to save metadata: {}", e)))?;

    Ok(())
}

/// Get TempURL key for account
///
/// Retrieves the TempURL key from account metadata.
/// Returns None if no TempURL key is set.
pub async fn get_tempurl_key(
    account: &str,
    credentials: &Option<Credentials>,
) -> SwiftResult<Option<String>> {
    let metadata = get_account_metadata(account, credentials).await?;
    Ok(metadata.get("temp-url-key").cloned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

    fn create_test_credentials(project_id: &str, roles: Vec<&str>) -> Credentials {
        let mut claims = HashMap::new();
        claims.insert("keystone_project_id".to_string(), json!(project_id));
        claims.insert("keystone_roles".to_string(), json!(roles));

        Credentials {
            access_key: "keystone:user123".to_string(),
            claims: Some(claims),
            ..Default::default()
        }
    }

    #[test]
    fn test_validate_account_access_success() {
        let creds = create_test_credentials("7188e165c0ae4424ac68ae2e89a05c50", vec!["member"]);
        let result = validate_account_access("AUTH_7188e165c0ae4424ac68ae2e89a05c50", &creds);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "7188e165c0ae4424ac68ae2e89a05c50");
    }

    #[test]
    fn test_validate_account_access_mismatch() {
        let creds = create_test_credentials("project123", vec!["member"]);
        let result = validate_account_access("AUTH_project456", &creds);

        assert!(result.is_err());
        match result.unwrap_err() {
            SwiftError::Forbidden(msg) => assert!(msg.contains("does not match")),
            _ => panic!("Expected Forbidden error"),
        }
    }

    #[test]
    fn test_validate_account_access_invalid_format() {
        let creds = create_test_credentials("project123", vec!["member"]);
        let result = validate_account_access("invalid_format", &creds);

        assert!(result.is_err());
        match result.unwrap_err() {
            SwiftError::BadRequest(msg) => assert!(msg.contains("Invalid account format")),
            _ => panic!("Expected BadRequest error"),
        }
    }

    #[test]
    fn test_validate_account_access_missing_project_id() {
        let mut creds = Credentials::default();
        let mut claims = HashMap::new();
        claims.insert("keystone_roles".to_string(), json!(["member"]));
        creds.claims = Some(claims);

        let result = validate_account_access("AUTH_project123", &creds);

        assert!(result.is_err());
        match result.unwrap_err() {
            SwiftError::Unauthorized(msg) => assert!(msg.contains("Missing project_id")),
            _ => panic!("Expected Unauthorized error"),
        }
    }

    #[test]
    fn test_is_admin_user_with_admin_role() {
        let creds = create_test_credentials("project123", vec!["admin", "member"]);
        assert!(is_admin_user(&creds));
    }

    #[test]
    fn test_is_admin_user_with_reseller_admin_role() {
        let creds = create_test_credentials("project123", vec!["reseller_admin"]);
        assert!(is_admin_user(&creds));
    }

    #[test]
    fn test_is_admin_user_without_admin_role() {
        let creds = create_test_credentials("project123", vec!["member", "reader"]);
        assert!(!is_admin_user(&creds));
    }

    #[test]
    fn test_is_admin_user_no_roles() {
        let mut creds = Credentials::default();
        let mut claims = HashMap::new();
        claims.insert("keystone_project_id".to_string(), json!("project123"));
        creds.claims = Some(claims);
        assert!(!is_admin_user(&creds));
    }

    #[test]
    fn test_get_account_metadata_bucket_name() {
        let bucket = get_account_metadata_bucket_name("AUTH_test123");
        assert!(bucket.starts_with("swift-account-"));
        assert_eq!(bucket.len(), "swift-account-".len() + 16); // prefix + 16 hex chars

        // Should be deterministic
        let bucket2 = get_account_metadata_bucket_name("AUTH_test123");
        assert_eq!(bucket, bucket2);

        // Different accounts should have different buckets
        let bucket3 = get_account_metadata_bucket_name("AUTH_test456");
        assert_ne!(bucket, bucket3);
    }
}
