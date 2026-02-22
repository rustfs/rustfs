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
use rmp_serde::Serializer as rmpSerializer;
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
    Hard,
}

#[derive(Debug, Deserialize, Serialize, Default, Clone, PartialEq)]
pub struct BucketQuota {
    pub quota: Option<u64>,
    pub quota_type: QuotaType,
    /// Timestamp when this quota configuration was set (for audit purposes)
    pub created_at: Option<OffsetDateTime>,
}

impl BucketQuota {
    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        self.serialize(&mut rmpSerializer::new(&mut buf).with_struct_map())?;
        Ok(buf)
    }

    pub fn unmarshal(buf: &[u8]) -> Result<Self> {
        let t: BucketQuota = rmp_serde::from_slice(buf)?;
        Ok(t)
    }

    pub fn new(quota: Option<u64>) -> Self {
        let now = OffsetDateTime::now_utc();
        Self {
            quota,
            quota_type: QuotaType::Hard,
            created_at: Some(now),
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
