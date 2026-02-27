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

use crate::{EC2Credential, KeystoneClient, KeystoneError, KeystoneToken, Result, TokenCache};
use rustfs_credentials::Credentials;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info};

/// Keystone authentication provider
///
/// This provider validates credentials against OpenStack Keystone
/// and maps Keystone identities to RustFS credentials.
pub struct KeystoneAuthProvider {
    client: Arc<KeystoneClient>,
    token_cache: TokenCache,
    ec2_cache: TokenCache,
    enable_cache: bool,
}

impl KeystoneAuthProvider {
    /// Create new authentication provider
    pub fn new(client: KeystoneClient, cache_size: u64, cache_ttl: Duration, enable_cache: bool) -> Self {
        Self {
            client: Arc::new(client),
            token_cache: TokenCache::new(cache_size, cache_ttl),
            ec2_cache: TokenCache::new(cache_size, cache_ttl),
            enable_cache,
        }
    }

    /// Disable caching (for testing)
    pub fn without_cache(mut self) -> Self {
        self.enable_cache = false;
        self
    }

    /// Authenticate using Keystone token (X-Auth-Token header)
    pub async fn authenticate_with_token(&self, token: &str) -> Result<Credentials> {
        // Check cache first
        if self.enable_cache
            && let Some(cached_token) = self.token_cache.get(token).await
            && !cached_token.is_expired()
        {
            debug!("Token cache hit: user_id={}", cached_token.user_id);
            return Ok(self.keystone_token_to_credentials(&cached_token));
        }

        if self.enable_cache {
            debug!("Cached token expired or not found, re-validating");
        }

        // Validate token with Keystone
        let keystone_token = self.client.validate_token(token).await?;

        // Check expiration
        if keystone_token.is_expired() {
            return Err(KeystoneError::TokenExpired);
        }

        // Cache token
        if self.enable_cache {
            self.token_cache
                .insert(token.to_string(), Arc::new(keystone_token.clone()))
                .await;
        }

        info!(
            "Keystone authentication successful: user={}, project={:?}",
            keystone_token.username, keystone_token.project_name
        );

        Ok(self.keystone_token_to_credentials(&keystone_token))
    }

    /// Authenticate using EC2 credentials (S3 API with AWS SigV4)
    pub async fn authenticate_with_ec2(&self, access_key: &str, signature: &str, string_to_sign: &str) -> Result<Credentials> {
        // Check cache
        let cache_key = format!("{}:{}", access_key, signature);
        if self.enable_cache
            && let Some(cached) = self.ec2_cache.get(&cache_key).await
            && !cached.is_expired()
        {
            debug!("EC2 credential cache hit: access_key={}", access_key);
            return Ok(self.keystone_token_to_credentials(&cached));
        }

        // Validate EC2 credentials with Keystone
        let ec2_cred = self
            .client
            .validate_ec2_credentials(access_key, signature, string_to_sign)
            .await?;

        // Convert to Keystone token (need to get full token info)
        let keystone_token = self.ec2_to_keystone_token(&ec2_cred).await?;

        // Cache
        if self.enable_cache {
            self.ec2_cache.insert(cache_key, Arc::new(keystone_token.clone())).await;
        }

        info!(
            "EC2 credential authentication successful: user={}, access_key={}",
            ec2_cred.user_id, access_key
        );

        Ok(self.keystone_token_to_credentials(&keystone_token))
    }

    /// Convert EC2 credential to Keystone token
    async fn ec2_to_keystone_token(&self, ec2_cred: &EC2Credential) -> Result<KeystoneToken> {
        // In a real implementation, you'd need to:
        // 1. Use admin credentials to get user/project details
        // 2. Or maintain a mapping table
        // For simplicity, construct a minimal token

        Ok(KeystoneToken {
            token: String::new(),
            user_id: ec2_cred.user_id.clone(),
            username: ec2_cred.user_id.clone(), // Use user_id as username
            project_id: ec2_cred.project_id.clone(),
            project_name: ec2_cred.project_id.clone(),
            domain_id: None,
            domain_name: None,
            roles: vec!["Member".to_string()], // Default role
            expires_at: time::OffsetDateTime::now_utc() + time::Duration::hours(24),
            issued_at: time::OffsetDateTime::now_utc(),
        })
    }

    /// Convert Keystone token to RustFS credentials
    fn keystone_token_to_credentials(&self, token: &KeystoneToken) -> Credentials {
        use serde_json::json;

        // Map Keystone roles to RustFS groups
        let groups = Some(token.roles.clone());

        // Add Keystone-specific claims
        let mut claims = HashMap::new();
        claims.insert("keystone_user_id".to_string(), json!(token.user_id));
        claims.insert("keystone_username".to_string(), json!(token.username));
        if let Some(ref proj_id) = token.project_id {
            claims.insert("keystone_project_id".to_string(), json!(proj_id));
        }
        if let Some(ref proj_name) = token.project_name {
            claims.insert("keystone_project_name".to_string(), json!(proj_name));
        }
        if let Some(ref dom_id) = token.domain_id {
            claims.insert("keystone_domain_id".to_string(), json!(dom_id));
        }
        if let Some(ref dom_name) = token.domain_name {
            claims.insert("keystone_domain_name".to_string(), json!(dom_name));
        }
        claims.insert("keystone_roles".to_string(), json!(token.roles));
        claims.insert("auth_source".to_string(), json!("keystone"));

        Credentials {
            access_key: format!("keystone:{}", token.user_id),
            secret_key: String::new(), // Not used for token auth
            session_token: token.token.clone(),
            expiration: Some(token.expires_at),
            status: "active".to_string(),
            parent_user: token.username.clone(),
            groups,
            claims: Some(claims),
            name: Some(token.username.clone()),
            description: Some(format!("Keystone user: {}", token.username)),
        }
    }

    /// Invalidate cached token
    pub async fn invalidate_token(&self, token: &str) {
        self.token_cache.invalidate(token).await;
    }

    /// Clear all caches
    pub async fn clear_caches(&self) {
        self.token_cache.clear().await;
        self.ec2_cache.clear().await;
    }

    /// Check if user has admin privileges
    pub fn is_admin(&self, cred: &Credentials) -> bool {
        cred.groups
            .as_ref()
            .map(|groups| {
                groups
                    .iter()
                    .any(|g| g.eq_ignore_ascii_case("admin") || g.eq_ignore_ascii_case("reseller_admin"))
            })
            .unwrap_or(false)
    }

    /// Extract project ID from credentials
    pub fn get_project_id(&self, cred: &Credentials) -> Option<String> {
        cred.claims
            .as_ref()
            .and_then(|claims| claims.get("keystone_project_id"))
            .and_then(|v| v.as_str())
            .map(String::from)
    }

    /// Extract user ID from credentials
    pub fn get_user_id(&self, cred: &Credentials) -> Option<String> {
        cred.claims
            .as_ref()
            .and_then(|claims| claims.get("keystone_user_id"))
            .and_then(|v| v.as_str())
            .map(String::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keystone_token_to_credentials() {
        let client = KeystoneClient::new(
            "http://localhost:5000".to_string(),
            crate::KeystoneVersion::V3,
            None,
            None,
            None,
            "Default".to_string(),
            true,
        );

        let provider = KeystoneAuthProvider::new(client, 100, Duration::from_secs(60), true);

        let token = KeystoneToken {
            token: "test-token".to_string(),
            user_id: "user123".to_string(),
            username: "testuser".to_string(),
            project_id: Some("proj456".to_string()),
            project_name: Some("testproject".to_string()),
            domain_id: Some("default".to_string()),
            domain_name: Some("Default".to_string()),
            roles: vec!["Member".to_string(), "admin".to_string()],
            expires_at: time::OffsetDateTime::now_utc() + time::Duration::hours(1),
            issued_at: time::OffsetDateTime::now_utc(),
        };

        let cred = provider.keystone_token_to_credentials(&token);

        assert_eq!(cred.access_key, "keystone:user123");
        assert_eq!(cred.parent_user, "testuser");
        assert_eq!(cred.groups, Some(vec!["Member".to_string(), "admin".to_string()]));
        assert!(cred.claims.is_some());

        let claims = cred.claims.unwrap();
        assert_eq!(claims.get("keystone_user_id").unwrap().as_str().unwrap(), "user123");
        assert_eq!(claims.get("keystone_project_id").unwrap().as_str().unwrap(), "proj456");
    }

    #[test]
    fn test_is_admin() {
        let client = KeystoneClient::new(
            "http://localhost:5000".to_string(),
            crate::KeystoneVersion::V3,
            None,
            None,
            None,
            "Default".to_string(),
            true,
        );

        let provider = KeystoneAuthProvider::new(client, 100, Duration::from_secs(60), true);

        let mut cred = Credentials {
            groups: Some(vec!["Member".to_string()]),
            ..Default::default()
        };
        assert!(!provider.is_admin(&cred));

        cred.groups = Some(vec!["admin".to_string()]);
        assert!(provider.is_admin(&cred));

        cred.groups = Some(vec!["Admin".to_string()]);
        assert!(provider.is_admin(&cred));
    }
}
