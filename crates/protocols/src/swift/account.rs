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
}
