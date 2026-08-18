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
pub(crate) mod reservation;

use crate::error::Result;
use rustfs_config::{
    QUOTA_API_PATH, QUOTA_EXCEEDED_ERROR_CODE, QUOTA_INTERNAL_ERROR_CODE, QUOTA_INVALID_CONFIG_ERROR_CODE,
    QUOTA_NOT_FOUND_ERROR_CODE,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};
use thiserror::Error;
use time::OffsetDateTime;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum QuotaType {
    /// Hard quota accounting.
    #[default]
    #[serde(alias = "HARD", alias = "hard")]
    Hard,
}

pub(crate) const QUOTA_RESERVATION_PROTOCOL_V1: u32 = 1;

/// Bucket quota configuration. quota_type defaults to Hard when omitted.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct BucketQuota {
    pub quota: Option<u64>,
    /// Defaults to Hard when missing.
    pub quota_type: QuotaType,
    /// Optional durable reservation protocol. The wire format gives older
    /// nodes a zero hard quota so a mixed-version fleet fails closed.
    pub reservation_protocol: Option<u32>,
    /// Timestamp when this quota configuration was set (for audit purposes)
    pub created_at: Option<OffsetDateTime>,
    /// Accept updated_at for compatibility; not used.
    pub updated_at: Option<OffsetDateTime>,
}

#[derive(Deserialize, Serialize)]
struct BucketQuotaWire {
    #[serde(default)]
    quota: Option<u64>,
    #[serde(default)]
    quota_type: QuotaType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reservation_protocol: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reservation_quota: Option<u64>,
    #[serde(default, with = "time::serde::rfc3339::option")]
    created_at: Option<OffsetDateTime>,
    #[serde(default, with = "time::serde::rfc3339::option", skip_serializing_if = "Option::is_none")]
    updated_at: Option<OffsetDateTime>,
}

impl Serialize for BucketQuota {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let durable = self.uses_durable_reservations();
        BucketQuotaWire {
            quota: if durable { Some(0) } else { self.quota },
            quota_type: self.quota_type.clone(),
            reservation_protocol: self.reservation_protocol,
            reservation_quota: if durable { self.quota } else { None },
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for BucketQuota {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = BucketQuotaWire::deserialize(deserializer)?;
        let quota = if wire.reservation_protocol == Some(QUOTA_RESERVATION_PROTOCOL_V1) {
            Some(
                wire.reservation_quota
                    .ok_or_else(|| D::Error::custom("reservation_quota is required for reservation protocol v1"))?,
            )
        } else {
            wire.quota
        };
        Ok(Self {
            quota,
            quota_type: wire.quota_type,
            reservation_protocol: wire.reservation_protocol,
            created_at: wire.created_at,
            updated_at: wire.updated_at,
        })
    }
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
            reservation_protocol: quota.map(|_| QUOTA_RESERVATION_PROTOCOL_V1),
            created_at: Some(now),
            updated_at: None,
        }
    }

    pub fn get_quota_limit(&self) -> Option<u64> {
        self.quota
    }

    pub fn uses_durable_reservations(&self) -> bool {
        self.reservation_protocol == Some(QUOTA_RESERVATION_PROTOCOL_V1)
    }

    pub fn has_unsupported_reservation_protocol(&self) -> bool {
        self.reservation_protocol
            .is_some_and(|version| version != QUOTA_RESERVATION_PROTOCOL_V1)
    }

    pub fn check_operation_allowed(&self, current_usage: u64, operation_size: u64) -> bool {
        if operation_size == 0 {
            return true;
        }
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
    pub uses_durable_reservations: bool,
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
    #[error("Authoritative data usage is unavailable for bucket: {bucket}")]
    UsageUnavailable { bucket: String },
    #[error("Invalid quota configuration: {reason}")]
    InvalidConfig { reason: String },
    #[error("Storage error: {0}")]
    StorageError(#[from] crate::error::StorageError),
}

#[derive(Debug, Serialize)]
#[allow(dead_code, reason = "asserted by this file's tests (backlog#1823)")]
pub struct QuotaErrorResponse {
    #[serde(rename = "Code")]
    pub code: String,
    #[serde(rename = "Message")]
    pub message: String,
    #[serde(rename = "Resource")]
    pub resource: String,
    // External quota error contract follows the existing PascalCase error schema.
    #[serde(rename = "RequestId")]
    pub request_id: String,
    #[serde(rename = "HostId")]
    pub host_id: String,
}

impl QuotaErrorResponse {
    #[allow(dead_code, reason = "asserted by this file's tests (backlog#1823)")]
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
            QuotaError::UsageUnavailable { .. } | QuotaError::StorageError(_) => Self {
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
    use serde_json::Value;

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
        assert_eq!(restored.quota_type, QuotaType::Hard);
        assert_eq!(restored.reservation_protocol, Some(QUOTA_RESERVATION_PROTOCOL_V1));
    }

    #[test]
    fn clearing_quota_keeps_the_legacy_compatible_type() {
        let quota = BucketQuota::new(None);

        assert_eq!(quota.quota_type, QuotaType::Hard);
        assert_eq!(quota.reservation_protocol, None);
        assert!(!quota.uses_durable_reservations());
    }

    #[test]
    fn durable_quota_makes_legacy_nodes_fail_closed() {
        let json = serde_json::to_vec(&BucketQuota::new(Some(2048))).expect("durable quota should serialize");
        let quota: BucketQuota = serde_json::from_slice(&json).expect("current quota version should parse");
        assert!(quota.uses_durable_reservations());
        assert_eq!(quota.quota, Some(2048));

        #[derive(Deserialize)]
        enum LegacyQuotaType {
            Hard,
        }
        #[derive(Deserialize)]
        struct LegacyBucketQuota {
            quota: Option<u64>,
            quota_type: LegacyQuotaType,
        }
        let legacy = serde_json::from_slice::<LegacyBucketQuota>(&json)
            .expect("legacy readers should ignore the reservation protocol field");
        assert_eq!(legacy.quota, Some(0));
        assert!(matches!(legacy.quota_type, LegacyQuotaType::Hard));
    }

    #[test]
    fn unknown_reservation_protocol_does_not_activate_v1() {
        let quota: BucketQuota =
            serde_json::from_str(r#"{"quota":0,"quota_type":"Hard","reservation_protocol":2,"reservation_quota":2048}"#)
                .expect("future protocol should remain parseable");

        assert!(!quota.uses_durable_reservations());
        assert!(quota.has_unsupported_reservation_protocol());
    }

    #[test]
    fn reservation_protocol_v1_requires_reservation_quota() {
        let err = serde_json::from_str::<BucketQuota>(r#"{"quota":0,"quota_type":"Hard","reservation_protocol":1}"#)
            .expect_err("v1 without its authoritative quota must fail closed");

        assert!(err.to_string().contains("reservation_quota is required"));
    }

    /// unmarshal accepts format without quota_type
    #[test]
    fn unmarshal_format_without_quota_type() {
        let json = r#"{"quota":1073741824,"created_at":"2024-01-01T00:00:00Z","updated_at":"2024-01-01T00:00:00Z"}"#;
        let q = BucketQuota::unmarshal(json.as_bytes()).expect("should parse");
        assert_eq!(q.quota, Some(1073741824));
        assert_eq!(q.quota_type, QuotaType::Hard);
    }

    #[test]
    fn quota_error_response_serializes_request_id_as_pascal_case_contract() {
        let response = QuotaErrorResponse::new(
            &QuotaError::InvalidConfig {
                reason: "bad quota".to_string(),
            },
            "req-quota-123",
            "host-quota-1",
        );

        let value = serde_json::to_value(response).expect("quota error response should serialize");
        assert_eq!(value["RequestId"], Value::String("req-quota-123".to_string()));
        assert!(value.get("request_id").is_none(), "quota error contract must not expose request_id");
    }
}
