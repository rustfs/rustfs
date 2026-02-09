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

use super::{BucketQuota, QuotaCheckResult, QuotaError, QuotaOperation};
use crate::bucket::metadata_sys::{BucketMetadataSys, update};
use crate::data_usage::get_bucket_usage_memory;
use rustfs_common::metrics::Metric;
use rustfs_config::QUOTA_CONFIG_FILE;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tracing::{debug, warn};

pub struct QuotaChecker {
    metadata_sys: Arc<RwLock<BucketMetadataSys>>,
}

impl QuotaChecker {
    pub fn new(metadata_sys: Arc<RwLock<BucketMetadataSys>>) -> Self {
        Self { metadata_sys }
    }

    pub async fn check_quota(
        &self,
        bucket: &str,
        operation: QuotaOperation,
        operation_size: u64,
    ) -> Result<QuotaCheckResult, QuotaError> {
        self.check_quota_with_usage_reporting(bucket, operation, operation_size, false)
            .await
    }

    /// Check quota with option to force usage calculation even when no quota is configured
    pub async fn check_quota_with_usage_reporting(
        &self,
        bucket: &str,
        operation: QuotaOperation,
        operation_size: u64,
        force_usage_calculation: bool,
    ) -> Result<QuotaCheckResult, QuotaError> {
        let start_time = Instant::now();
        let quota_config = self.get_quota_config(bucket).await?;

        // If no quota limit is set, allow operation
        let quota_limit = match quota_config.quota {
            None => {
                let current_usage = if force_usage_calculation {
                    Some(self.get_real_time_usage(bucket).await?)
                } else {
                    None // Skip expensive usage calculation when no quota and not forced for performance
                };
                return Ok(QuotaCheckResult {
                    allowed: true,
                    current_usage,
                    quota_limit: None,
                    operation_size,
                    remaining: None,
                });
            }
            Some(q) => q,
        };

        let current_usage = self.get_real_time_usage(bucket).await?;

        let expected_usage = match operation {
            QuotaOperation::PutObject | QuotaOperation::PostObject | QuotaOperation::CopyObject => current_usage + operation_size,
            QuotaOperation::DeleteObject => current_usage.saturating_sub(operation_size),
        };

        let allowed = match operation {
            QuotaOperation::PutObject | QuotaOperation::PostObject | QuotaOperation::CopyObject => {
                quota_config.check_operation_allowed(current_usage, operation_size)
            }
            QuotaOperation::DeleteObject => true,
        };

        let remaining = if quota_limit >= expected_usage {
            Some(quota_limit - expected_usage)
        } else {
            Some(0)
        };

        if !allowed {
            warn!(
                "Quota exceeded for bucket: {}, current: {}, limit: {}, attempted: {}",
                bucket, current_usage, quota_limit, operation_size
            );
        }

        let result = QuotaCheckResult {
            allowed,
            current_usage: Some(current_usage),
            quota_limit: Some(quota_limit),
            operation_size,
            remaining,
        };

        let duration = start_time.elapsed();
        rustfs_common::metrics::Metrics::inc_time(Metric::QuotaCheck, duration).await;
        if !allowed {
            rustfs_common::metrics::Metrics::inc_time(Metric::QuotaViolation, duration).await;
        }

        Ok(result)
    }

    pub async fn get_quota_config(&self, bucket: &str) -> Result<BucketQuota, QuotaError> {
        let meta = self
            .metadata_sys
            .read()
            .await
            .get(bucket)
            .await
            .map_err(QuotaError::StorageError)?;

        if meta.quota_config_json.is_empty() {
            debug!("No quota config found for bucket: {}, using default", bucket);
            return Ok(BucketQuota::new(None));
        }

        let quota: BucketQuota = serde_json::from_slice(&meta.quota_config_json).map_err(|e| QuotaError::InvalidConfig {
            reason: format!("Failed to parse quota config: {}", e),
        })?;

        Ok(quota)
    }

    pub async fn set_quota_config(&mut self, bucket: &str, quota: BucketQuota) -> Result<(), QuotaError> {
        let json_data = serde_json::to_vec(&quota).map_err(|e| QuotaError::InvalidConfig {
            reason: format!("Failed to serialize quota config: {}", e),
        })?;
        let start_time = Instant::now();

        update(bucket, QUOTA_CONFIG_FILE, json_data)
            .await
            .map_err(QuotaError::StorageError)?;

        rustfs_common::metrics::Metrics::inc_time(Metric::QuotaSync, start_time.elapsed()).await;
        Ok(())
    }

    pub async fn get_quota_stats(&self, bucket: &str) -> Result<(BucketQuota, Option<u64>), QuotaError> {
        // If bucket doesn't exist, return ConfigNotFound error
        if !self.bucket_exists(bucket).await {
            return Err(QuotaError::ConfigNotFound {
                bucket: bucket.to_string(),
            });
        }

        let quota = self.get_quota_config(bucket).await?;
        let current_usage = self.get_real_time_usage(bucket).await.unwrap_or(0);

        Ok((quota, Some(current_usage)))
    }

    pub async fn bucket_exists(&self, bucket: &str) -> bool {
        self.metadata_sys.read().await.get(bucket).await.is_ok()
    }

    pub async fn get_real_time_usage(&self, bucket: &str) -> Result<u64, QuotaError> {
        Ok(get_bucket_usage_memory(bucket).await.unwrap_or(0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_quota_check_no_limit() {
        let result = QuotaCheckResult {
            allowed: true,
            current_usage: None,
            quota_limit: None,
            operation_size: 1024,
            remaining: None,
        };

        assert!(result.allowed);
        assert_eq!(result.quota_limit, None);
    }

    #[tokio::test]
    async fn test_quota_check_within_limit() {
        let quota = BucketQuota::new(Some(2048)); // 2KB

        // Current usage 512, trying to add 1024
        let allowed = quota.check_operation_allowed(512, 1024);
        assert!(allowed);
    }

    #[tokio::test]
    async fn test_quota_check_exceeds_limit() {
        let quota = BucketQuota::new(Some(1024)); // 1KB

        // Current usage 512, trying to add 1024
        let allowed = quota.check_operation_allowed(512, 1024);
        assert!(!allowed);
    }
}
