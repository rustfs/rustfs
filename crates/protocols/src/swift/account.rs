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

use super::metadata_update::{ACCOUNT_META_TAG_PREFIX, MetadataUpdate};
use super::storage_api::account::{BucketOperations, MakeBucketOptions};
use super::{SwiftError, SwiftResult};
use super::{get_swift_bucket_metadata, resolve_swift_object_store_handle, update_swift_bucket_tagging};
use rustfs_credentials::Credentials;
use sha2::{Digest, Sha256};
use std::collections::HashMap;

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
pub async fn get_account_metadata(account: &str, _credentials: &Option<Credentials>) -> SwiftResult<HashMap<String, String>> {
    let bucket_name = get_account_metadata_bucket_name(account);

    // Try to load bucket metadata
    let bucket_meta = match get_swift_bucket_metadata(&bucket_name).await {
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
            if let (Some(key), Some(value)) = (&tag.key, &tag.value)
                && let Some(meta_key) = key.strip_prefix(ACCOUNT_META_TAG_PREFIX)
            {
                metadata.insert(meta_key.to_string(), value.clone());
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
/// Swift account POST is additive: `update` names the items to write and the
/// items to drop, and everything else keeps its stored value. Replacing the
/// whole set instead would make an unrelated POST — setting a quota, say —
/// delete the account's TempURL signing key, permanently invalidating every
/// outstanding TempURL and FormPost signature for the account.
///
/// The caller must own the account: this metadata holds the account's TempURL
/// signing key, so writing it for someone else's account would let the writer
/// mint valid pre-signed URLs against that account's objects. Reads
/// ([`get_account_metadata`]) stay unauthenticated on purpose — TempURL
/// signature validation has to resolve the key before any credentials exist.
///
/// # Arguments
/// * `account` - Account identifier
/// * `update` - Metadata items to set and to remove (names are prefixed with `swift-account-meta-`)
/// * `credentials` - Keystone credentials of the caller
pub async fn update_account_metadata(
    account: &str,
    update: &MetadataUpdate,
    credentials: &Option<Credentials>,
) -> SwiftResult<()> {
    let Some(credentials) = credentials.as_ref() else {
        return Err(SwiftError::Unauthorized(
            "Keystone authentication required to update account metadata".to_string(),
        ));
    };
    validate_account_access(account, credentials)?;

    // These tags are persisted into the bucket metadata file, which every
    // later config write rewrites in full — so unbounded metadata inflates
    // the cost of unrelated writes for the life of the account. The item
    // count is capped against the merged result, inside the rewrite.
    update.validate()?;

    // An update that names no item changes nothing, so there is no reason to
    // bring the account's metadata bucket into existence for it.
    if update.is_empty() {
        return Ok(());
    }

    let bucket_name = get_account_metadata_bucket_name(account);

    let Some(store) = resolve_swift_object_store_handle() else {
        return Err(SwiftError::InternalServerError("Storage layer not initialized".to_string()));
    };

    // Create bucket if it doesn't exist
    let bucket_exists = get_swift_bucket_metadata(&bucket_name).await.is_ok();
    if !bucket_exists {
        // Create bucket for account metadata
        store
            .make_bucket(&bucket_name, &MakeBucketOptions::default())
            .await
            .map_err(|e| SwiftError::InternalServerError(format!("Failed to create account metadata bucket: {}", e)))?;
    }

    // Merge into the persisted tags: only the swift-account-meta-* items this
    // update names change, and non-Swift tags are left alone. An empty result
    // clears the tagging config.
    update_swift_bucket_tagging(bucket_name, |current| update.apply_to_tags(current, ACCOUNT_META_TAG_PREFIX)).await
}

/// Get TempURL key for account
///
/// Retrieves the TempURL key from account metadata.
/// Returns None if no TempURL key is set.
pub async fn get_tempurl_key(account: &str, credentials: &Option<Credentials>) -> SwiftResult<Option<String>> {
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
