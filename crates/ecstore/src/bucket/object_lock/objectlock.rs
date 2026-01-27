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

use s3s::dto::{Date, ObjectLockLegalHold, ObjectLockLegalHoldStatus, ObjectLockRetention, ObjectLockRetentionMode};
use s3s::header::{X_AMZ_OBJECT_LOCK_LEGAL_HOLD, X_AMZ_OBJECT_LOCK_MODE, X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE};
use std::collections::HashMap;
use time::{OffsetDateTime, format_description};

const _ERR_MALFORMED_BUCKET_OBJECT_CONFIG: &str = "invalid bucket object lock config";
const _ERR_INVALID_RETENTION_DATE: &str = "date must be provided in ISO 8601 format";
const _ERR_PAST_OBJECTLOCK_RETAIN_DATE: &str = "the retain until date must be in the future";
const _ERR_UNKNOWN_WORMMODE_DIRECTIVE: &str = "unknown WORM mode directive";
const _ERR_OBJECTLOCK_MISSING_CONTENT_MD5: &str =
    "content-MD5 HTTP header is required for Put Object requests with Object Lock parameters";
const _ERR_OBJECTLOCK_INVALID_HEADERS: &str =
    "x-amz-object-lock-retain-until-date and x-amz-object-lock-mode must both be supplied";
const _ERR_MALFORMED_XML: &str = "the XML you provided was not well-formed or did not validate against our published schema";

pub fn utc_now_ntp() -> OffsetDateTime {
    OffsetDateTime::now_utc()
}

pub fn get_object_retention_meta(meta: &HashMap<String, String>) -> ObjectLockRetention {
    // Note: X_AMZ_OBJECT_LOCK_MODE.as_str() is already lowercase ("x-amz-object-lock-mode")
    let mode_str = meta.get(X_AMZ_OBJECT_LOCK_MODE.as_str());

    let Some(mode_str) = mode_str else {
        return ObjectLockRetention {
            mode: None,
            retain_until_date: None,
        };
    };

    // If mode is invalid, return empty retention (don't panic)
    let Some(mode) = parse_ret_mode(mode_str.as_str()) else {
        return ObjectLockRetention {
            mode: None,
            retain_until_date: None,
        };
    };

    let till_str = meta.get(X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.as_str());

    let retain_until_date = till_str
        .and_then(|s| OffsetDateTime::parse(s, &format_description::well_known::Iso8601::DEFAULT).ok())
        .map(Date::from);

    ObjectLockRetention {
        mode: Some(mode),
        retain_until_date,
    }
}

pub fn get_object_legalhold_meta(meta: &HashMap<String, String>) -> ObjectLockLegalHold {
    // Note: X_AMZ_OBJECT_LOCK_LEGAL_HOLD.as_str() is already lowercase
    let hold_str = meta.get(X_AMZ_OBJECT_LOCK_LEGAL_HOLD.as_str());

    match hold_str.and_then(|s| parse_legalhold_status(s)) {
        Some(status) => ObjectLockLegalHold { status: Some(status) },
        None => ObjectLockLegalHold { status: None },
    }
}

/// Parse retention mode string into ObjectLockRetentionMode.
/// Returns None for invalid/unknown mode strings instead of panicking.
pub fn parse_ret_mode(mode_str: &str) -> Option<ObjectLockRetentionMode> {
    match mode_str.to_uppercase().as_str() {
        "GOVERNANCE" => Some(ObjectLockRetentionMode::from_static(ObjectLockRetentionMode::GOVERNANCE)),
        "COMPLIANCE" => Some(ObjectLockRetentionMode::from_static(ObjectLockRetentionMode::COMPLIANCE)),
        _ => None,
    }
}

/// Parse legal hold status string into ObjectLockLegalHoldStatus.
/// Returns None for invalid/unknown status strings instead of panicking.
pub fn parse_legalhold_status(hold_str: &str) -> Option<ObjectLockLegalHoldStatus> {
    match hold_str.to_uppercase().as_str() {
        "ON" => Some(ObjectLockLegalHoldStatus::from_static(ObjectLockLegalHoldStatus::ON)),
        "OFF" => Some(ObjectLockLegalHoldStatus::from_static(ObjectLockLegalHoldStatus::OFF)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_ret_mode_valid() {
        // Test uppercase
        let mode = parse_ret_mode("GOVERNANCE");
        assert!(mode.is_some());
        assert_eq!(mode.unwrap().as_str(), ObjectLockRetentionMode::GOVERNANCE);

        let mode = parse_ret_mode("COMPLIANCE");
        assert!(mode.is_some());
        assert_eq!(mode.unwrap().as_str(), ObjectLockRetentionMode::COMPLIANCE);

        // Test lowercase
        let mode = parse_ret_mode("governance");
        assert!(mode.is_some());
        assert_eq!(mode.unwrap().as_str(), ObjectLockRetentionMode::GOVERNANCE);

        let mode = parse_ret_mode("compliance");
        assert!(mode.is_some());
        assert_eq!(mode.unwrap().as_str(), ObjectLockRetentionMode::COMPLIANCE);

        // Test mixed case
        let mode = parse_ret_mode("Governance");
        assert!(mode.is_some());
        assert_eq!(mode.unwrap().as_str(), ObjectLockRetentionMode::GOVERNANCE);
    }

    #[test]
    fn test_parse_ret_mode_invalid() {
        // Test invalid values return None instead of panicking
        assert!(parse_ret_mode("INVALID").is_none());
        assert!(parse_ret_mode("").is_none());
        assert!(parse_ret_mode("gov").is_none());
        assert!(parse_ret_mode("comp").is_none());
    }

    #[test]
    fn test_parse_legalhold_status_valid() {
        // Test uppercase
        let status = parse_legalhold_status("ON");
        assert!(status.is_some());
        assert_eq!(status.unwrap().as_str(), ObjectLockLegalHoldStatus::ON);

        let status = parse_legalhold_status("OFF");
        assert!(status.is_some());
        assert_eq!(status.unwrap().as_str(), ObjectLockLegalHoldStatus::OFF);

        // Test lowercase
        let status = parse_legalhold_status("on");
        assert!(status.is_some());
        assert_eq!(status.unwrap().as_str(), ObjectLockLegalHoldStatus::ON);

        let status = parse_legalhold_status("off");
        assert!(status.is_some());
        assert_eq!(status.unwrap().as_str(), ObjectLockLegalHoldStatus::OFF);
    }

    #[test]
    fn test_parse_legalhold_status_invalid() {
        // Test invalid values return None instead of panicking
        assert!(parse_legalhold_status("INVALID").is_none());
        assert!(parse_legalhold_status("").is_none());
        assert!(parse_legalhold_status("true").is_none());
        assert!(parse_legalhold_status("false").is_none());
    }

    #[test]
    fn test_get_object_retention_meta_empty() {
        let meta = HashMap::new();
        let retention = get_object_retention_meta(&meta);
        assert!(retention.mode.is_none());
        assert!(retention.retain_until_date.is_none());
    }

    #[test]
    fn test_get_object_retention_meta_with_mode() {
        let mut meta = HashMap::new();
        meta.insert("x-amz-object-lock-mode".to_string(), "GOVERNANCE".to_string());
        let retention = get_object_retention_meta(&meta);
        assert!(retention.mode.is_some());
        assert_eq!(retention.mode.unwrap().as_str(), ObjectLockRetentionMode::GOVERNANCE);
        assert!(retention.retain_until_date.is_none());
    }

    #[test]
    fn test_get_object_retention_meta_with_invalid_mode() {
        let mut meta = HashMap::new();
        meta.insert("x-amz-object-lock-mode".to_string(), "INVALID_MODE".to_string());
        let retention = get_object_retention_meta(&meta);
        // Invalid mode should return empty retention, not panic
        assert!(retention.mode.is_none());
        assert!(retention.retain_until_date.is_none());
    }

    #[test]
    fn test_get_object_retention_meta_with_date() {
        let mut meta = HashMap::new();
        meta.insert("x-amz-object-lock-mode".to_string(), "COMPLIANCE".to_string());
        meta.insert("x-amz-object-lock-retain-until-date".to_string(), "2030-01-01T00:00:00Z".to_string());
        let retention = get_object_retention_meta(&meta);
        assert!(retention.mode.is_some());
        assert_eq!(retention.mode.unwrap().as_str(), ObjectLockRetentionMode::COMPLIANCE);
        assert!(retention.retain_until_date.is_some());
    }

    #[test]
    fn test_get_object_legalhold_meta_empty() {
        let meta = HashMap::new();
        let legalhold = get_object_legalhold_meta(&meta);
        assert!(legalhold.status.is_none());
    }

    #[test]
    fn test_get_object_legalhold_meta_on() {
        let mut meta = HashMap::new();
        meta.insert("x-amz-object-lock-legal-hold".to_string(), "ON".to_string());
        let legalhold = get_object_legalhold_meta(&meta);
        assert!(legalhold.status.is_some());
        assert_eq!(legalhold.status.unwrap().as_str(), ObjectLockLegalHoldStatus::ON);
    }

    #[test]
    fn test_get_object_legalhold_meta_off() {
        let mut meta = HashMap::new();
        meta.insert("x-amz-object-lock-legal-hold".to_string(), "OFF".to_string());
        let legalhold = get_object_legalhold_meta(&meta);
        assert!(legalhold.status.is_some());
        assert_eq!(legalhold.status.unwrap().as_str(), ObjectLockLegalHoldStatus::OFF);
    }

    #[test]
    fn test_get_object_legalhold_meta_invalid() {
        let mut meta = HashMap::new();
        meta.insert("x-amz-object-lock-legal-hold".to_string(), "INVALID".to_string());
        let legalhold = get_object_legalhold_meta(&meta);
        // Invalid status should return None, not panic
        assert!(legalhold.status.is_none());
    }
}
