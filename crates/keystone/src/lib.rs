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

//! OpenStack Keystone integration for RustFS
//!
//! This module provides authentication and identity management
//! integration with OpenStack Keystone, similar to Ceph RGW.
//!
//! # Features
//!
//! - Keystone v3 token authentication
//! - EC2 credential support for S3 API compatibility
//! - Multi-tenancy with project-based bucket prefixing
//! - Role-based access control mapping
//! - Token caching for performance
//!
//! # Example
//!
//! ```no_run
//! use rustfs_keystone::{KeystoneConfig, KeystoneClient, KeystoneAuthProvider};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = KeystoneConfig::from_env()?;
//! let client = KeystoneClient::new(
//!     config.auth_url.clone(),
//!     config.get_version()?,
//!     config.admin_user.clone(),
//!     config.admin_password.clone(),
//!     config.admin_project.clone(),
//!     config.verify_ssl,
//! );
//!
//! let auth_provider = KeystoneAuthProvider::new(
//!     client,
//!     config.cache_size,
//!     config.get_cache_ttl(),
//! );
//!
//! // Authenticate with Keystone token
//! let credentials = auth_provider.authenticate_with_token("token123").await?;
//! # Ok(())
//! # }
//! ```

use moka::future::Cache;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use time::OffsetDateTime;

pub mod auth;
pub mod client;
pub mod config;
pub mod error;
pub mod identity;

pub use auth::KeystoneAuthProvider;
pub use client::KeystoneClient;
pub use config::{KeystoneConfig, RoleMapping};
pub use error::{KeystoneError, Result};
pub use identity::KeystoneIdentityMapper;

/// Keystone API version
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeystoneVersion {
    /// Keystone API v2.0 (legacy)
    V2_0,
    /// Keystone API v3
    V3,
}

/// Keystone token information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeystoneToken {
    /// Token string (may be empty for cached tokens)
    pub token: String,
    /// User ID
    pub user_id: String,
    /// Username
    pub username: String,
    /// Project/Tenant ID
    pub project_id: Option<String>,
    /// Project/Tenant name
    pub project_name: Option<String>,
    /// Domain ID
    pub domain_id: Option<String>,
    /// Domain name
    pub domain_name: Option<String>,
    /// Assigned roles
    pub roles: Vec<String>,
    /// Token expiration time
    pub expires_at: OffsetDateTime,
    /// Token issue time
    pub issued_at: OffsetDateTime,
}

impl KeystoneToken {
    /// Check if token is expired
    pub fn is_expired(&self) -> bool {
        OffsetDateTime::now_utc() >= self.expires_at
    }

    /// Check if token has specific role
    pub fn has_role(&self, role: &str) -> bool {
        self.roles.iter().any(|r| r == role)
    }

    /// Check if token has admin role
    pub fn is_admin(&self) -> bool {
        self.has_role("admin") || self.has_role("Admin")
    }
}

/// EC2 credentials from Keystone
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EC2Credential {
    /// Access key (format: user_id:project_id or user_id)
    pub access: String,
    /// Secret key
    pub secret: String,
    /// User ID
    pub user_id: String,
    /// Project ID
    pub project_id: Option<String>,
    /// Trust ID (for delegated credentials)
    pub trust_id: Option<String>,
}

impl EC2Credential {
    /// Parse access key to extract user_id and project_id
    ///
    /// Format: "user_id:project_id" or "user_id"
    pub fn parse_access_key(access_key: &str) -> Option<(String, Option<String>)> {
        if access_key.contains(':') {
            let parts: Vec<&str> = access_key.split(':').collect();
            if parts.len() == 2 {
                return Some((parts[0].to_string(), Some(parts[1].to_string())));
            }
        }
        Some((access_key.to_string(), None))
    }
}

/// Token cache for performance optimization
#[derive(Clone)]
pub struct TokenCache {
    cache: Cache<String, Arc<KeystoneToken>>,
}

impl TokenCache {
    /// Create new token cache
    pub fn new(capacity: u64, ttl: Duration) -> Self {
        Self {
            cache: Cache::builder().max_capacity(capacity).time_to_live(ttl).build(),
        }
    }

    /// Get cached token
    pub async fn get(&self, token: &str) -> Option<Arc<KeystoneToken>> {
        self.cache.get(token).await
    }

    /// Insert token into cache
    pub async fn insert(&self, token: String, info: Arc<KeystoneToken>) {
        self.cache.insert(token, info).await;
    }

    /// Invalidate cached token
    pub async fn invalidate(&self, token: &str) {
        self.cache.invalidate(token).await;
    }

    /// Clear all cached tokens
    pub async fn clear(&self) {
        self.cache.invalidate_all();
    }
}
