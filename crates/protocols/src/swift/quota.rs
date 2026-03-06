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

//! Container Quota Support for Swift API
//!
//! This module implements container quotas that limit the size and/or number
//! of objects that can be stored in a container. Quotas are enforced during
//! PUT operations and reject uploads that would exceed configured limits.
//!
//! # Configuration
//!
//! Quotas are configured via container metadata:
//!
//! - `X-Container-Meta-Quota-Bytes`: Maximum total bytes allowed in container
//! - `X-Container-Meta-Quota-Count`: Maximum number of objects allowed in container
//!
//! # Usage
//!
//! ```bash
//! # Set byte quota (10 GB)
//! swift post my-container -H "X-Container-Meta-Quota-Bytes: 10737418240"
//!
//! # Set object count quota (1000 objects)
//! swift post my-container -H "X-Container-Meta-Quota-Count: 1000"
//!
//! # Set both quotas
//! swift post my-container \
//!   -H "X-Container-Meta-Quota-Bytes: 10737418240" \
//!   -H "X-Container-Meta-Quota-Count: 1000"
//!
//! # Remove quotas
//! swift post my-container \
//!   -H "X-Remove-Container-Meta-Quota-Bytes:" \
//!   -H "X-Remove-Container-Meta-Quota-Count:"
//! ```
//!
//! # Enforcement
//!
//! When a PUT request would cause the container to exceed its quota:
//! - Request is rejected with 413 Payload Too Large
//! - Response includes quota headers showing current usage
//! - Object is not uploaded
//!
//! # Example
//!
//! ```bash
//! # Container has quota of 1GB
//! swift post my-container -H "X-Container-Meta-Quota-Bytes: 1073741824"
//!
//! # Current usage: 900MB, uploading 200MB file
//! swift upload my-container large-file.bin
//!
//! # Response: 413 Payload Too Large
//! # X-Container-Meta-Quota-Bytes: 1073741824
//! # X-Container-Bytes-Used: 943718400
//! ```

use super::{SwiftError, SwiftResult, container};
use rustfs_credentials::Credentials;
use tracing::debug;

/// Quota configuration for a container
#[derive(Debug, Clone, Default)]
pub struct QuotaConfig {
    /// Maximum total bytes allowed in container
    pub quota_bytes: Option<u64>,

    /// Maximum number of objects allowed in container
    pub quota_count: Option<u64>,
}

impl QuotaConfig {
    /// Load quota configuration from container metadata
    pub async fn load(account: &str, container_name: &str, credentials: &Credentials) -> SwiftResult<Self> {
        // Get container metadata
        let container_info = container::get_container_metadata(account, container_name, credentials).await?;

        let mut config = QuotaConfig::default();

        // Parse Quota-Bytes
        if let Some(quota_bytes_str) = container_info.custom_metadata.get("x-container-meta-quota-bytes") {
            config.quota_bytes = quota_bytes_str.parse().ok();
        }

        // Parse Quota-Count
        if let Some(quota_count_str) = container_info.custom_metadata.get("x-container-meta-quota-count") {
            config.quota_count = quota_count_str.parse().ok();
        }

        Ok(config)
    }

    /// Check if any quotas are configured
    pub fn is_enabled(&self) -> bool {
        self.quota_bytes.is_some() || self.quota_count.is_some()
    }

    /// Check if adding an object would exceed quotas
    ///
    /// Returns Ok(()) if within quota, Err with 413 if exceeded
    pub fn check_quota(&self, current_bytes: u64, current_count: u64, additional_bytes: u64) -> SwiftResult<()> {
        // Check byte quota
        if let Some(max_bytes) = self.quota_bytes {
            let new_bytes = current_bytes.saturating_add(additional_bytes);
            if new_bytes > max_bytes {
                return Err(SwiftError::RequestEntityTooLarge(format!(
                    "Upload would exceed quota-bytes limit: {} + {} > {}",
                    current_bytes, additional_bytes, max_bytes
                )));
            }
        }

        // Check count quota
        if let Some(max_count) = self.quota_count {
            let new_count = current_count.saturating_add(1);
            if new_count > max_count {
                return Err(SwiftError::RequestEntityTooLarge(format!(
                    "Upload would exceed quota-count limit: {} + 1 > {}",
                    current_count, max_count
                )));
            }
        }

        Ok(())
    }
}

/// Check if upload would exceed container quotas
///
/// Returns Ok(()) if quota not exceeded or not configured
/// Returns Err(SwiftError::RequestEntityTooLarge) if quota would be exceeded
pub async fn check_upload_quota(
    account: &str,
    container_name: &str,
    object_size: u64,
    credentials: &Credentials,
) -> SwiftResult<()> {
    // Load quota config
    let quota = QuotaConfig::load(account, container_name, credentials).await?;

    // If no quotas configured, allow upload
    if !quota.is_enabled() {
        return Ok(());
    }

    // Get current container usage
    let metadata = container::get_container_metadata(account, container_name, credentials).await?;

    // Check if upload would exceed quota
    quota.check_quota(metadata.bytes_used, metadata.object_count, object_size)?;

    debug!(
        "Quota check passed: {}/{:?} bytes, {}/{:?} objects",
        metadata.bytes_used, quota.quota_bytes, metadata.object_count, quota.quota_count
    );

    Ok(())
}

/// Check if quotas are enabled for a container
pub async fn is_enabled(account: &str, container_name: &str, credentials: &Credentials) -> SwiftResult<bool> {
    match QuotaConfig::load(account, container_name, credentials).await {
        Ok(config) => Ok(config.is_enabled()),
        Err(_) => Ok(false), // Container doesn't exist or no quotas configured
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quota_config_default() {
        let config = QuotaConfig::default();
        assert!(!config.is_enabled());
        assert!(config.quota_bytes.is_none());
        assert!(config.quota_count.is_none());
    }

    #[test]
    fn test_quota_config_enabled_bytes() {
        let config = QuotaConfig {
            quota_bytes: Some(1000),
            quota_count: None,
        };
        assert!(config.is_enabled());
    }

    #[test]
    fn test_quota_config_enabled_count() {
        let config = QuotaConfig {
            quota_bytes: None,
            quota_count: Some(100),
        };
        assert!(config.is_enabled());
    }

    #[test]
    fn test_quota_config_enabled_both() {
        let config = QuotaConfig {
            quota_bytes: Some(1000),
            quota_count: Some(100),
        };
        assert!(config.is_enabled());
    }

    #[test]
    fn test_check_quota_within_bytes_limit() {
        let config = QuotaConfig {
            quota_bytes: Some(1000),
            quota_count: None,
        };

        // Current: 500 bytes, adding 400 bytes = 900 total (within 1000 limit)
        let result = config.check_quota(500, 0, 400);
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_quota_exceeds_bytes_limit() {
        let config = QuotaConfig {
            quota_bytes: Some(1000),
            quota_count: None,
        };

        // Current: 500 bytes, adding 600 bytes = 1100 total (exceeds 1000 limit)
        let result = config.check_quota(500, 0, 600);
        assert!(result.is_err());
        match result {
            Err(SwiftError::RequestEntityTooLarge(msg)) => {
                assert!(msg.contains("quota-bytes"));
            }
            _ => panic!("Expected RequestEntityTooLarge error"),
        }
    }

    #[test]
    fn test_check_quota_exact_bytes_limit() {
        let config = QuotaConfig {
            quota_bytes: Some(1000),
            quota_count: None,
        };

        // Current: 500 bytes, adding 500 bytes = 1000 total (exactly at limit)
        let result = config.check_quota(500, 0, 500);
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_quota_within_count_limit() {
        let config = QuotaConfig {
            quota_bytes: None,
            quota_count: Some(10),
        };

        // Current: 5 objects, adding 1 = 6 total (within 10 limit)
        let result = config.check_quota(0, 5, 100);
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_quota_exceeds_count_limit() {
        let config = QuotaConfig {
            quota_bytes: None,
            quota_count: Some(10),
        };

        // Current: 10 objects, adding 1 = 11 total (exceeds 10 limit)
        let result = config.check_quota(0, 10, 100);
        assert!(result.is_err());
        match result {
            Err(SwiftError::RequestEntityTooLarge(msg)) => {
                assert!(msg.contains("quota-count"));
            }
            _ => panic!("Expected RequestEntityTooLarge error"),
        }
    }

    #[test]
    fn test_check_quota_exact_count_limit() {
        let config = QuotaConfig {
            quota_bytes: None,
            quota_count: Some(10),
        };

        // Current: 9 objects, adding 1 = 10 total (exactly at limit)
        let result = config.check_quota(0, 9, 100);
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_quota_both_limits_within() {
        let config = QuotaConfig {
            quota_bytes: Some(1000),
            quota_count: Some(10),
        };

        // Both within limits
        let result = config.check_quota(500, 5, 400);
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_quota_bytes_exceeded_count_within() {
        let config = QuotaConfig {
            quota_bytes: Some(1000),
            quota_count: Some(10),
        };

        // Bytes exceeded, count within
        let result = config.check_quota(500, 5, 600);
        assert!(result.is_err());
    }

    #[test]
    fn test_check_quota_count_exceeded_bytes_within() {
        let config = QuotaConfig {
            quota_bytes: Some(1000),
            quota_count: Some(10),
        };

        // Count exceeded, bytes within
        let result = config.check_quota(500, 10, 100);
        assert!(result.is_err());
    }

    #[test]
    fn test_check_quota_no_limits() {
        let config = QuotaConfig {
            quota_bytes: None,
            quota_count: None,
        };

        // No limits, should always pass
        let result = config.check_quota(999999, 999999, 999999);
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_quota_zero_bytes_limit() {
        let config = QuotaConfig {
            quota_bytes: Some(0),
            quota_count: None,
        };

        // Zero limit means no uploads allowed
        let result = config.check_quota(0, 0, 1);
        assert!(result.is_err());
    }

    #[test]
    fn test_check_quota_zero_count_limit() {
        let config = QuotaConfig {
            quota_bytes: None,
            quota_count: Some(0),
        };

        // Zero limit means no objects allowed
        let result = config.check_quota(0, 0, 100);
        assert!(result.is_err());
    }

    #[test]
    fn test_quota_overflow_protection() {
        let config = QuotaConfig {
            quota_bytes: Some(u64::MAX),
            quota_count: Some(u64::MAX),
        };

        // Test saturating_add protection
        let result = config.check_quota(u64::MAX - 100, 0, 200);
        // Should saturate to u64::MAX and compare against quota
        assert!(result.is_ok());
    }
}
