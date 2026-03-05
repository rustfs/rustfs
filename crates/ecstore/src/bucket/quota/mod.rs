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

pub mod checker;

use crate::error::Result;
use rustfs_config::{
    QUOTA_API_PATH, QUOTA_EXCEEDED_ERROR_CODE, QUOTA_INTERNAL_ERROR_CODE, QUOTA_INVALID_CONFIG_ERROR_CODE,
    QUOTA_NOT_FOUND_ERROR_CODE,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use time::OffsetDateTime;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum QuotaType {
    /// Hard quota: reject immediately when exceeded
    #[default]
    #[serde(alias = "HARD", alias = "hard")]
    Hard,
}

/// Bucket quota configuration. quota_type defaults to Hard when omitted.
#[derive(Debug, Deserialize, Serialize, Default, Clone, PartialEq)]
pub struct BucketQuota {
    #[serde(default)]
    pub quota: Option<u64>,
    /// Defaults to Hard when missing.
    #[serde(default)]
    pub quota_type: QuotaType,
    /// Timestamp when this quota configuration was set (for audit purposes)
    #[serde(default, with = "time::serde::rfc3339::option")]
    pub created_at: Option<OffsetDateTime>,
    /// Accept updated_at for compatibility; not used.
    #[serde(default, with = "time::serde::rfc3339::option", skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<OffsetDateTime>,
}

impl BucketQuota {
    /// Serialize to JSON bytes. Same format as parse_all_configs.
    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(Into::into)
    }

    /// Deserialize from JSON bytes. Same format as parse_all_configs.
    pub fn unmarshal(buf: &[u8]) -> Result<Self> {
        serde_json::from_slice(buf).map_err(Into::into)
    }

    pub fn new(quota: Option<u64>) -> Self {
        let now = OffsetDateTime::now_utc();
        Self {
            quota,
            quota_type: QuotaType::Hard,
            created_at: Some(now),
            updated_at: None,
        }
    }

    pub fn get_quota_limit(&self) -> Option<u64> {
        self.quota
    }

    pub fn check_operation_allowed(&self, current_usage: u64, operation_size: u64) -> bool {
        if let Some(quota_limit) = self.quota {
            current_usage.saturating_add(operation_size) <= quota_limit
        } else {
            true // No quota limit
        }
    }

    pub fn get_remaining_quota(&self, current_usage: u64) -> Option<u64> {
        self.quota.map(|limit| limit.saturating_sub(current_usage))
    }
}

#[derive(Debug)]
pub struct QuotaCheckResult {
    pub allowed: bool,
    /// current_usage: None when skipped for performance (no quota configured)
    pub current_usage: Option<u64>,
    /// quota_limit: None means unlimited
    pub quota_limit: Option<u64>,
    pub operation_size: u64,
    pub remaining: Option<u64>,
}

#[derive(Debug)]
pub enum QuotaOperation {
    PutObject,
    PostObject,
    CopyObject,
    DeleteObject,
}

#[derive(Debug, Error)]
pub enum QuotaError {
    #[error("Bucket quota exceeded: current={current}, limit={limit}, operation={operation}")]
    QuotaExceeded { current: u64, limit: u64, operation: u64 },
    #[error("Quota configuration not found for bucket: {bucket}")]
    ConfigNotFound { bucket: String },
    #[error("Invalid quota configuration: {reason}")]
    InvalidConfig { reason: String },
    #[error("Storage error: {0}")]
    StorageError(#[from] crate::error::StorageError),
}

#[derive(Debug, Serialize)]
pub struct QuotaErrorResponse {
    #[serde(rename = "Code")]
    pub code: String,
    #[serde(rename = "Message")]
    pub message: String,
    #[serde(rename = "Resource")]
    pub resource: String,
    #[serde(rename = "RequestId")]
    pub request_id: String,
    #[serde(rename = "HostId")]
    pub host_id: String,
}

impl QuotaErrorResponse {
    pub fn new(quota_error: &QuotaError, request_id: &str, host_id: &str) -> Self {
        match quota_error {
            QuotaError::QuotaExceeded { .. } => Self {
                code: QUOTA_EXCEEDED_ERROR_CODE.to_string(),
                message: quota_error.to_string(),
                resource: QUOTA_API_PATH.to_string(),
                request_id: request_id.to_string(),
                host_id: host_id.to_string(),
            },
            QuotaError::ConfigNotFound { .. } => Self {
                code: QUOTA_NOT_FOUND_ERROR_CODE.to_string(),
                message: quota_error.to_string(),
                resource: QUOTA_API_PATH.to_string(),
                request_id: request_id.to_string(),
                host_id: host_id.to_string(),
            },
            QuotaError::InvalidConfig { .. } => Self {
                code: QUOTA_INVALID_CONFIG_ERROR_CODE.to_string(),
                message: quota_error.to_string(),
                resource: QUOTA_API_PATH.to_string(),
                request_id: request_id.to_string(),
                host_id: host_id.to_string(),
            },
            QuotaError::StorageError(_) => Self {
                code: QUOTA_INTERNAL_ERROR_CODE.to_string(),
                message: quota_error.to_string(),
                resource: QUOTA_API_PATH.to_string(),
                request_id: request_id.to_string(),
                host_id: host_id.to_string(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Legacy format: quota, created_at, updated_at (no quota_type)
    #[test]
    fn deserialize_format_without_quota_type() {
        let json = r#"{"quota":1073741824,"created_at":"2024-01-01T00:00:00Z","updated_at":"2024-01-01T00:00:00Z"}"#;
        let q: BucketQuota = serde_json::from_slice(json.as_bytes()).expect("should parse");
        assert_eq!(q.quota, Some(1073741824));
        assert_eq!(q.quota_type, QuotaType::Hard);
        assert!(q.created_at.is_some());
        assert!(q.updated_at.is_some());
    }

    /// RustFS format: quota, quota_type, created_at
    #[test]
    fn deserialize_rustfs_format() {
        let json = r#"{"quota":1073741824,"quota_type":"Hard","created_at":"2024-01-01T00:00:00Z"}"#;
        let q: BucketQuota = serde_json::from_slice(json.as_bytes()).expect("should parse");
        assert_eq!(q.quota, Some(1073741824));
        assert_eq!(q.quota_type, QuotaType::Hard);
        assert!(q.created_at.is_some());
        assert!(q.created_at.is_some_and(|t| t.unix_timestamp() == 1704067200));
    }

    /// E2E format uses "HARD" (uppercase)
    #[test]
    fn deserialize_quota_type_hard_uppercase() {
        let json = r#"{"quota":2048,"quota_type":"HARD"}"#;
        let q: BucketQuota = serde_json::from_slice(json.as_bytes()).expect("should parse");
        assert_eq!(q.quota_type, QuotaType::Hard);
    }

    /// marshal_msg/unmarshal use JSON, same as parse_all_configs
    #[test]
    fn marshal_unmarshal_roundtrip() {
        let q = BucketQuota::new(Some(1073741824));
        let buf = q.marshal_msg().expect("marshal");
        let restored = BucketQuota::unmarshal(&buf).expect("unmarshal");
        assert_eq!(q.quota, restored.quota);
        assert_eq!(q.quota_type, restored.quota_type);
    }

    /// unmarshal accepts format without quota_type
    #[test]
    fn unmarshal_format_without_quota_type() {
        let json = r#"{"quota":1073741824,"created_at":"2024-01-01T00:00:00Z","updated_at":"2024-01-01T00:00:00Z"}"#;
        let q = BucketQuota::unmarshal(json.as_bytes()).expect("should parse");
        assert_eq!(q.quota, Some(1073741824));
        assert_eq!(q.quota_type, QuotaType::Hard);
    }
}
