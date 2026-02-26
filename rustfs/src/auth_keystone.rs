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

//! OpenStack Keystone authentication integration for RustFS

use http::HeaderMap;
use rustfs_credentials::Credentials;
use rustfs_keystone::{KeystoneAuthProvider, KeystoneClient, KeystoneConfig, KeystoneIdentityMapper};
use s3s::{S3Error, S3ErrorCode, S3Result, s3_error};
use std::sync::{Arc, OnceLock};
use tracing::{debug, error, info, warn};

static KEYSTONE_AUTH: OnceLock<Arc<KeystoneAuthProvider>> = OnceLock::new();
static KEYSTONE_MAPPER: OnceLock<Arc<KeystoneIdentityMapper>> = OnceLock::new();
static KEYSTONE_CONFIG: OnceLock<KeystoneConfig> = OnceLock::new();

/// Initialize Keystone authentication
pub async fn init_keystone_auth(config: KeystoneConfig) -> Result<(), Box<dyn std::error::Error>> {
    if !config.enable {
        info!("Keystone authentication disabled");
        return Ok(());
    }

    info!("Initializing Keystone authentication...");

    // Validate configuration
    config.validate()?;

    let version = config.get_version()?;
    let client = KeystoneClient::new(
        config.auth_url.clone(),
        version,
        config.admin_user.clone(),
        config.admin_password.clone(),
        config.admin_project.clone(),
        config.get_admin_domain(),
        config.verify_ssl,
    );

    let auth_provider = KeystoneAuthProvider::new(
        client.clone(),
        config.cache_size,
        config.get_cache_ttl(),
        config.enable_cache,
    );

    let mut mapper = KeystoneIdentityMapper::new(Arc::new(client), config.enable_tenant_prefix);

    // Add custom role mappings if configured
    if let Some(role_mappings) = &config.role_mappings {
        for mapping in role_mappings {
            mapper.add_role_mapping(mapping.keystone_role.clone(), mapping.rustfs_policy.clone());
        }
    }

    KEYSTONE_AUTH
        .set(Arc::new(auth_provider))
        .map_err(|_| "Keystone auth already initialized")?;

    KEYSTONE_MAPPER
        .set(Arc::new(mapper))
        .map_err(|_| "Keystone mapper already initialized")?;

    KEYSTONE_CONFIG
        .set(config.clone())
        .map_err(|_| "Keystone config already initialized")?;

    info!("Keystone authentication initialized successfully");
    info!("  Auth URL: {}", config.auth_url);
    info!("  Version: {}", config.version);
    info!("  Tenant prefix enabled: {}", config.enable_tenant_prefix);
    info!("  Token caching enabled: {}", config.enable_cache);

    Ok(())
}

/// Get Keystone auth provider
pub fn get_keystone_auth() -> Option<Arc<KeystoneAuthProvider>> {
    KEYSTONE_AUTH.get().cloned()
}

/// Get Keystone identity mapper
pub fn get_keystone_mapper() -> Option<Arc<KeystoneIdentityMapper>> {
    KEYSTONE_MAPPER.get().cloned()
}

/// Get Keystone configuration
pub fn get_keystone_config() -> Option<&'static KeystoneConfig> {
    KEYSTONE_CONFIG.get()
}

/// Check if Keystone is enabled
pub fn is_keystone_enabled() -> bool {
    KEYSTONE_CONFIG.get().map(|c| c.enable).unwrap_or(false)
}

/// Authenticate request with Keystone
///
/// Checks for:
/// 1. X-Auth-Token header (Keystone token)
/// 2. X-Storage-Token header (Swift compatibility)
///
/// Returns Some(Credentials) if authenticated via Keystone,
/// None if Keystone is disabled or no Keystone headers present
pub async fn authenticate_keystone(headers: &HeaderMap) -> S3Result<Option<Credentials>> {
    let auth_provider = match get_keystone_auth() {
        Some(provider) => provider,
        None => return Ok(None), // Keystone not enabled
    };

    // Check for X-Auth-Token header (Keystone v3)
    if let Some(token) = headers.get("X-Auth-Token").and_then(|v| v.to_str().ok()) {
        debug!("Found X-Auth-Token header, validating with Keystone");

        return match auth_provider.authenticate_with_token(token).await {
            Ok(cred) => {
                info!("Keystone token authentication successful: user={}", cred.parent_user);
                Ok(Some(cred))
            }
            Err(e) => {
                error!("Keystone token authentication failed: {}", e);
                Err(s3_error!(InvalidToken, "Invalid Keystone token: {}", e))
            }
        };
    }

    // Check for X-Storage-Token header (Swift compatibility)
    if let Some(token) = headers.get("X-Storage-Token").and_then(|v| v.to_str().ok()) {
        debug!("Found X-Storage-Token header, validating with Keystone");

        return match auth_provider.authenticate_with_token(token).await {
            Ok(cred) => {
                info!("Keystone Swift token authentication successful: user={}", cred.parent_user);
                Ok(Some(cred))
            }
            Err(e) => {
                error!("Keystone Swift token authentication failed: {}", e);
                Err(s3_error!(InvalidToken, "Invalid Keystone token: {}", e))
            }
        };
    }

    // No Keystone headers found
    Ok(None)
}

/// Apply tenant prefix to bucket name
pub fn apply_tenant_prefix(bucket: &str, cred: &Credentials) -> String {
    let mapper = match get_keystone_mapper() {
        Some(m) => m,
        None => return bucket.to_string(),
    };

    // Extract project_id from claims
    let project_id = cred
        .claims
        .as_ref()
        .and_then(|claims| claims.get("keystone_project_id"))
        .and_then(|v| v.as_str());

    mapper.apply_tenant_prefix(bucket, project_id)
}

/// Remove tenant prefix from bucket name
pub fn remove_tenant_prefix(prefixed_bucket: &str, cred: &Credentials) -> String {
    let mapper = match get_keystone_mapper() {
        Some(m) => m,
        None => return prefixed_bucket.to_string(),
    };

    let project_id = cred
        .claims
        .as_ref()
        .and_then(|claims| claims.get("keystone_project_id"))
        .and_then(|v| v.as_str());

    mapper.remove_tenant_prefix(prefixed_bucket, project_id)
}

/// Check if bucket belongs to user's project
pub fn is_user_bucket(bucket: &str, cred: &Credentials) -> bool {
    let mapper = match get_keystone_mapper() {
        Some(m) => m,
        None => return true,
    };

    let project_id = cred
        .claims
        .as_ref()
        .and_then(|claims| claims.get("keystone_project_id"))
        .and_then(|v| v.as_str());

    mapper.is_project_bucket(bucket, project_id)
}

/// Filter bucket list to only show user's project buckets
pub fn filter_bucket_list(buckets: Vec<String>, cred: &Credentials) -> Vec<String> {
    let mapper = match get_keystone_mapper() {
        Some(m) => m,
        None => return buckets,
    };

    if !mapper.is_tenant_prefix_enabled() {
        return buckets;
    }

    let project_id = cred
        .claims
        .as_ref()
        .and_then(|claims| claims.get("keystone_project_id"))
        .and_then(|v| v.as_str());

    if let Some(proj_id) = project_id {
        let prefix = format!("{}:", proj_id);
        buckets
            .into_iter()
            .filter(|b| b.starts_with(&prefix))
            .map(|b| b[prefix.len()..].to_string())
            .collect()
    } else {
        // No project ID, return unprefixed buckets only
        buckets.into_iter().filter(|b| !b.contains(':')).collect()
    }
}

/// Check if credential is from Keystone
pub fn is_keystone_credential(cred: &Credentials) -> bool {
    cred.claims
        .as_ref()
        .and_then(|claims| claims.get("auth_source"))
        .and_then(|v| v.as_str())
        .map(|s| s == "keystone")
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

    fn create_test_credentials(project_id: Option<&str>) -> Credentials {
        let mut claims = HashMap::new();
        claims.insert("auth_source".to_string(), json!("keystone"));
        if let Some(proj_id) = project_id {
            claims.insert("keystone_project_id".to_string(), json!(proj_id));
        }

        Credentials {
            access_key: "test-access".to_string(),
            secret_key: "test-secret".to_string(),
            claims: Some(claims),
            ..Default::default()
        }
    }

    #[test]
    fn test_is_keystone_credential() {
        let cred = create_test_credentials(Some("proj123"));
        assert!(is_keystone_credential(&cred));

        let non_keystone_cred = Credentials::default();
        assert!(!is_keystone_credential(&non_keystone_cred));
    }
}
