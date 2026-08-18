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

use crate::bucket::metadata_sys::{ObjectLockConfigState, get_object_lock_config, get_object_lock_config_state};
use crate::bucket::object_lock::objectlock;
use crate::error::{Error, Result, StorageError};
use crate::object_api::ObjectInfo;
use s3s::dto::{Date, DefaultRetention, ObjectLockConfiguration, ObjectLockLegalHoldStatus, ObjectLockRetentionMode};
use s3s::header::{X_AMZ_OBJECT_LOCK_LEGAL_HOLD, X_AMZ_OBJECT_LOCK_MODE, X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE};
use std::sync::Arc;
use time::OffsetDateTime;

pub struct BucketObjectLockSys {}

impl BucketObjectLockSys {
    #[allow(clippy::new_ret_no_self)]
    pub async fn new() -> Arc<Self> {
        Arc::new(Self {})
    }

    pub async fn get(bucket: &str) -> Option<DefaultRetention> {
        if let Ok(object_lock_config) = get_object_lock_config(bucket).await
            && let Some(object_lock_rule) = object_lock_config.0.rule
        {
            return object_lock_rule.default_retention;
        }
        None
    }
}

pub(crate) fn ensure_recursive_force_delete_allowed_for_state(bucket: &str, state: &ObjectLockConfigState) -> Result<()> {
    match state {
        ObjectLockConfigState::ConfirmedAbsent => Ok(()),
        ObjectLockConfigState::Configured { .. } => Err(StorageError::InvalidArgument(
            bucket.to_string(),
            String::new(),
            "force-delete is forbidden on Object Locking enabled buckets".to_string(),
        )),
        ObjectLockConfigState::Fabricated => {
            Err(Error::other(format!("bucket Object Lock metadata is not authoritative: {bucket}")))
        }
    }
}

/// Check if a retention period is still active based on mode and retain_until_date
pub fn is_retention_active(mode: &str, retain_until_date: Option<&s3s::dto::Date>) -> bool {
    if mode != ObjectLockRetentionMode::COMPLIANCE && mode != ObjectLockRetentionMode::GOVERNANCE {
        return false;
    }
    if let Some(retain_until) = retain_until_date {
        let now = objectlock::utc_now_ntp();
        return OffsetDateTime::from(retain_until.clone()).unix_timestamp() > now.unix_timestamp();
    }
    false
}

/// Check if retention modification is blocked for the given object.
pub fn check_retention_for_modification(
    user_defined: &std::collections::HashMap<String, String>,
    new_mode: Option<&str>,
    new_retain_until: Option<OffsetDateTime>,
    bypass_governance: bool,
) -> Option<ObjectLockBlockReason> {
    let retention = objectlock::get_object_retention_meta(user_defined);

    let Some(mode) = &retention.mode else {
        return None;
    };

    let mode_str = mode.as_str();
    if !is_retention_active(mode_str, retention.retain_until_date.as_ref()) {
        return None;
    }

    let existing_retain_until = retention.retain_until_date.as_ref().map(|d| OffsetDateTime::from(d.clone()));
    let mode_changed = new_mode != Some(mode_str);

    // Check if new retention period is shorter than existing
    let is_shortening = match (&existing_retain_until, &new_retain_until) {
        (Some(existing), Some(new)) => new < existing,
        (Some(_), None) => true, // Clearing retention is shortening
        _ => false,
    };

    // COMPLIANCE mode: cannot shorten retention at all (even with bypass)
    // Can only extend the retention period
    if mode_str == ObjectLockRetentionMode::COMPLIANCE {
        if mode_changed || is_shortening {
            return Some(ObjectLockBlockReason::Retention {
                mode: mode_str.to_string(),
                retain_until: existing_retain_until,
            });
        }
        // Extending retention in COMPLIANCE mode is allowed
        return None;
    }

    // GOVERNANCE mode: extending is always allowed, shortening requires bypass
    // This matches AWS S3 behavior where:
    // - Extending retention: allowed without bypass permission
    // - Shortening/removing retention: requires bypass permission
    if mode_str == ObjectLockRetentionMode::GOVERNANCE {
        if (mode_changed || is_shortening) && !bypass_governance {
            return Some(ObjectLockBlockReason::Retention {
                mode: mode_str.to_string(),
                retain_until: existing_retain_until,
            });
        }
        // Extending retention or shortening with bypass is allowed
        return None;
    }

    None
}

pub fn add_years(dt: OffsetDateTime, years: i32) -> OffsetDateTime {
    let target_year = dt.year() + years;
    dt.replace_year(target_year)
        .or_else(|_| {
            // Feb 29 -> non-leap year: use Feb 28
            dt.replace_day(28).and_then(|d| d.replace_year(target_year))
        })
        .unwrap_or(dt)
}

/// Check if an object has legal hold enabled.
/// Returns true if legal hold is ON.
#[allow(dead_code, reason = "asserted by this file's tests (backlog#1823)")]
fn has_legal_hold(user_defined: &std::collections::HashMap<String, String>) -> bool {
    let lhold = objectlock::get_object_legalhold_meta(user_defined);
    matches!(lhold.status, Some(ref st) if st.as_str() == ObjectLockLegalHoldStatus::ON)
}

/// Check if an object is locked based on its metadata.
/// This is a common function used by both lifecycle evaluation and deletion checks.
///
/// # Arguments
/// * `user_defined` - The object's user-defined metadata
/// * `is_delete_marker` - Whether the object is a delete marker
///
/// # Returns
/// * `true` if the object is locked (cannot be deleted/modified)
/// * `false` if the object is not locked
#[allow(dead_code, reason = "asserted by this file's tests (backlog#1823)")]
pub fn is_object_locked_by_metadata(user_defined: &std::collections::HashMap<String, String>, is_delete_marker: bool) -> bool {
    // Delete markers are never locked
    if is_delete_marker {
        return false;
    }

    // Check legal hold - always blocks if ON
    if has_legal_hold(user_defined) {
        return true;
    }

    // Check retention - reuse is_retention_active to avoid code duplication
    let ret = objectlock::get_object_retention_meta(user_defined);
    if let Some(mode) = &ret.mode
        && is_retention_active(mode.as_str(), ret.retain_until_date.as_ref())
    {
        return true;
    }

    false
}

/// Reason why object deletion is blocked by Object Lock
#[derive(Debug, Clone, PartialEq)]
pub enum ObjectLockBlockReason {
    /// Object has legal hold enabled (must be explicitly removed)
    LegalHold,
    /// Object is under retention until the specified date
    Retention {
        mode: String,
        retain_until: Option<OffsetDateTime>,
    },
}

impl ObjectLockBlockReason {
    /// Get a user-friendly error message for this block reason
    pub fn error_message(&self) -> String {
        match self {
            ObjectLockBlockReason::LegalHold => {
                "Object has a legal hold and cannot be deleted. Remove the legal hold first.".to_string()
            }
            ObjectLockBlockReason::Retention { mode, retain_until } => {
                if let Some(until) = retain_until {
                    format!("Object is under {} retention and cannot be deleted until {}", mode, until)
                } else {
                    format!("Object is under {} retention and cannot be deleted", mode)
                }
            }
        }
    }
}

/// Check if retention blocks deletion based on mode and bypass permission.
/// Returns Some(ObjectLockBlockReason) if blocked, None if allowed.
fn check_retention_blocks_deletion(
    mode_str: &str,
    retain_until: Option<OffsetDateTime>,
    bypass_governance: bool,
) -> Option<ObjectLockBlockReason> {
    // COMPLIANCE mode cannot be bypassed; GOVERNANCE can only be bypassed with permission
    let can_bypass = mode_str == ObjectLockRetentionMode::GOVERNANCE && bypass_governance;
    if !can_bypass {
        return Some(ObjectLockBlockReason::Retention {
            mode: mode_str.to_string(),
            retain_until,
        });
    }
    None
}

/// Check an object's lock metadata using an already resolved bucket Object
/// Lock configuration. `None` means the configuration is confirmed absent.
///
/// # S3 Standard Behavior
/// - COMPLIANCE mode: Cannot be deleted even with bypass header
/// - GOVERNANCE mode: Can be deleted if bypass_governance is true (caller must verify s3:BypassGovernanceRetention permission)
/// - Legal Hold: Cannot be bypassed regardless of mode
pub(crate) fn check_object_lock_for_deletion_with_config(
    config: Option<&ObjectLockConfiguration>,
    obj_info: &ObjectInfo,
    bypass_governance: bool,
) -> Result<Option<ObjectLockBlockReason>> {
    if obj_info.delete_marker {
        return Ok(None);
    }

    if let Some(status) = obj_info.user_defined.get(X_AMZ_OBJECT_LOCK_LEGAL_HOLD.as_str()) {
        if status.eq_ignore_ascii_case(ObjectLockLegalHoldStatus::ON) {
            return Ok(Some(ObjectLockBlockReason::LegalHold));
        }
        if !status.eq_ignore_ascii_case(ObjectLockLegalHoldStatus::OFF) {
            return Err(Error::other("persisted object legal-hold metadata is invalid"));
        }
    }

    let mode = obj_info.user_defined.get(X_AMZ_OBJECT_LOCK_MODE.as_str());
    let retain_until = obj_info.user_defined.get(X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.as_str());
    let explicit_ret = match (mode, retain_until) {
        (None, None) => None,
        (Some(mode), Some(retain_until)) => {
            let mode =
                objectlock::parse_ret_mode(mode).ok_or_else(|| Error::other("persisted object retention mode is invalid"))?;
            let retain_until = OffsetDateTime::parse(retain_until, &time::format_description::well_known::Iso8601::DEFAULT)
                .map(Date::from)
                .map_err(|_| Error::other("persisted object retention date is invalid"))?;
            Some((mode, retain_until))
        }
        _ => return Err(Error::other("persisted object retention metadata is incomplete")),
    };

    if let Some((mode, retain_until)) = &explicit_ret {
        let mode_str = mode.as_str();
        if is_retention_active(mode_str, Some(retain_until))
            && let Some(reason) =
                check_retention_blocks_deletion(mode_str, Some(OffsetDateTime::from(retain_until.clone())), bypass_governance)
        {
            return Ok(Some(reason));
        }
    }

    if explicit_ret.is_none()
        && let Some(default_retention) = config.and_then(|config| config.rule.as_ref()?.default_retention.as_ref())
        && let Some(mode) = &default_retention.mode
    {
        let mode_str = mode.as_str();
        if mode_str == ObjectLockRetentionMode::COMPLIANCE || mode_str == ObjectLockRetentionMode::GOVERNANCE {
            // Calculate retention expiration date from object modification time
            let mod_time = obj_info
                .mod_time
                .ok_or_else(|| Error::other("persisted object modification time is missing"))?;
            let now = objectlock::utc_now_ntp();
            let retain_until = if let Some(days) = default_retention.days {
                mod_time.saturating_add(time::Duration::days(i64::from(days)))
            } else {
                let years = default_retention
                    .years
                    .ok_or_else(|| Error::other("persisted bucket Object Lock retention period is invalid"))?;
                add_years(mod_time, years)
            };

            if retain_until.unix_timestamp() > now.unix_timestamp()
                && let Some(reason) = check_retention_blocks_deletion(mode_str, Some(retain_until), bypass_governance)
            {
                return Ok(Some(reason));
            }
        }
    }

    Ok(None)
}

pub(crate) fn check_object_lock_for_deletion_with_state(
    state: &ObjectLockConfigState,
    obj_info: &ObjectInfo,
    bypass_governance: bool,
) -> Result<Option<ObjectLockBlockReason>> {
    match state {
        ObjectLockConfigState::Configured { config, .. } => {
            check_object_lock_for_deletion_with_config(Some(config), obj_info, bypass_governance)
        }
        ObjectLockConfigState::ConfirmedAbsent => check_object_lock_for_deletion_with_config(None, obj_info, bypass_governance),
        ObjectLockConfigState::Fabricated => Err(Error::other("bucket Object Lock metadata is not authoritative")),
    }
}

/// Compatibility wrapper for callers that predate fallible metadata lookup.
/// An authority/read/parse failure is represented as a blocking reason rather
/// than the old fail-open `None` result.
pub async fn check_object_lock_for_deletion(
    bucket: &str,
    obj_info: &ObjectInfo,
    bypass_governance: bool,
) -> Option<ObjectLockBlockReason> {
    match get_object_lock_config_state(bucket)
        .await
        .and_then(|state| check_object_lock_for_deletion_with_state(&state, obj_info, bypass_governance))
    {
        Ok(reason) => reason,
        Err(_) => Some(ObjectLockBlockReason::LegalHold),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use s3s::dto::{ObjectLockEnabled, ObjectLockRule};
    use time::{Date, Month, PrimitiveDateTime, Time};

    fn make_datetime(year: i32, month: u8, day: u8) -> OffsetDateTime {
        let date = Date::from_calendar_date(year, Month::try_from(month).unwrap(), day).unwrap();
        let time = Time::from_hms(0, 0, 0).unwrap();
        PrimitiveDateTime::new(date, time).assume_utc()
    }

    fn default_retention_config(mode: &'static str) -> ObjectLockConfiguration {
        ObjectLockConfiguration {
            object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
            rule: Some(ObjectLockRule {
                default_retention: Some(DefaultRetention {
                    mode: Some(ObjectLockRetentionMode::from_static(mode)),
                    days: Some(30),
                    years: None,
                }),
            }),
        }
    }

    #[test]
    fn deletion_with_config_blocks_active_default_compliance_even_with_bypass() {
        let config = default_retention_config(ObjectLockRetentionMode::COMPLIANCE);
        let obj_info = ObjectInfo {
            mod_time: Some(OffsetDateTime::now_utc()),
            ..Default::default()
        };

        let result = check_object_lock_for_deletion_with_config(Some(&config), &obj_info, true);

        assert!(matches!(result, Ok(Some(ObjectLockBlockReason::Retention { .. }))));
    }

    #[test]
    fn deletion_with_config_allows_active_default_governance_with_bypass() {
        let config = default_retention_config(ObjectLockRetentionMode::GOVERNANCE);
        let obj_info = ObjectInfo {
            mod_time: Some(OffsetDateTime::now_utc()),
            ..Default::default()
        };

        assert!(matches!(
            check_object_lock_for_deletion_with_config(Some(&config), &obj_info, true),
            Ok(None)
        ));
    }

    #[test]
    fn deletion_with_default_retention_rejects_missing_object_mod_time() {
        let config = default_retention_config(ObjectLockRetentionMode::COMPLIANCE);

        let err = check_object_lock_for_deletion_with_config(Some(&config), &ObjectInfo::default(), false)
            .expect_err("default retention needs an authoritative object modification time");

        assert!(err.to_string().contains("modification time"));
    }

    #[test]
    fn deletion_with_confirmed_absence_still_blocks_explicit_compliance() {
        let retain_until = OffsetDateTime::now_utc() + time::Duration::days(30);
        let mut user_defined = std::collections::HashMap::new();
        user_defined.insert("x-amz-object-lock-mode".to_string(), ObjectLockRetentionMode::COMPLIANCE.to_string());
        user_defined.insert(
            "x-amz-object-lock-retain-until-date".to_string(),
            retain_until
                .format(&time::format_description::well_known::Rfc3339)
                .expect("retain-until date should format"),
        );
        let obj_info = ObjectInfo {
            user_defined: Arc::new(user_defined),
            ..Default::default()
        };

        let result = check_object_lock_for_deletion_with_config(None, &obj_info, true);

        assert!(matches!(result, Ok(Some(ObjectLockBlockReason::Retention { .. }))));
    }

    #[test]
    fn deletion_with_fabricated_bucket_metadata_fails_closed() {
        let err = check_object_lock_for_deletion_with_state(&ObjectLockConfigState::Fabricated, &ObjectInfo::default(), false)
            .expect_err("non-authoritative Object Lock metadata must block deletion");

        assert!(err.to_string().contains("not authoritative"));
    }

    #[test]
    fn recursive_force_delete_with_fabricated_bucket_metadata_fails_closed() {
        let err = ensure_recursive_force_delete_allowed_for_state("bucket", &ObjectLockConfigState::Fabricated)
            .expect_err("non-authoritative Object Lock metadata must block recursive deletion");

        assert!(err.to_string().contains("not authoritative"));
    }

    #[test]
    fn deletion_rejects_incomplete_persisted_retention_metadata() {
        let mut user_defined = std::collections::HashMap::new();
        user_defined.insert(
            X_AMZ_OBJECT_LOCK_MODE.as_str().to_string(),
            ObjectLockRetentionMode::COMPLIANCE.to_string(),
        );
        let obj_info = ObjectInfo {
            user_defined: Arc::new(user_defined),
            ..Default::default()
        };

        let err = check_object_lock_for_deletion_with_config(None, &obj_info, false)
            .expect_err("mode without retain-until date must fail closed");

        assert!(err.to_string().contains("incomplete"));
    }

    #[test]
    fn deletion_rejects_each_malformed_persisted_retention_shape() {
        let valid_date = (OffsetDateTime::now_utc() + time::Duration::days(30))
            .format(&time::format_description::well_known::Rfc3339)
            .expect("retain-until date should format");
        let cases = [
            ("invalid mode", Some("INVALID"), Some(valid_date.as_str()), "retention mode"),
            (
                "invalid date",
                Some(ObjectLockRetentionMode::COMPLIANCE),
                Some("not-a-date"),
                "retention date",
            ),
            ("date only", None, Some(valid_date.as_str()), "incomplete"),
        ];

        for (case, mode, retain_until, expected) in cases {
            let mut user_defined = std::collections::HashMap::new();
            if let Some(mode) = mode {
                user_defined.insert(X_AMZ_OBJECT_LOCK_MODE.as_str().to_string(), mode.to_string());
            }
            if let Some(retain_until) = retain_until {
                user_defined.insert(X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.as_str().to_string(), retain_until.to_string());
            }
            let obj_info = ObjectInfo {
                user_defined: Arc::new(user_defined),
                ..Default::default()
            };

            let err = check_object_lock_for_deletion_with_config(None, &obj_info, false).expect_err(case);
            assert!(err.to_string().contains(expected), "unexpected {case} error: {err}");
        }
    }

    #[test]
    fn deletion_rejects_invalid_persisted_legal_hold_metadata() {
        let mut user_defined = std::collections::HashMap::new();
        user_defined.insert(X_AMZ_OBJECT_LOCK_LEGAL_HOLD.as_str().to_string(), "INVALID".to_string());
        let obj_info = ObjectInfo {
            user_defined: Arc::new(user_defined),
            ..Default::default()
        };

        let err = check_object_lock_for_deletion_with_config(None, &obj_info, false)
            .expect_err("invalid legal-hold value must fail closed");

        assert!(err.to_string().contains("legal-hold"));
    }

    #[test]
    fn test_add_years_normal() {
        // Normal case: add 1 year to a regular date
        let dt = make_datetime(2024, 3, 15);
        let result = add_years(dt, 1);
        assert_eq!(result.year(), 2025);
        assert_eq!(result.month(), Month::March);
        assert_eq!(result.day(), 15);
    }

    #[test]
    fn test_add_years_multiple() {
        // Add multiple years
        let dt = make_datetime(2024, 6, 1);
        let result = add_years(dt, 5);
        assert_eq!(result.year(), 2029);
        assert_eq!(result.month(), Month::June);
        assert_eq!(result.day(), 1);
    }

    #[test]
    fn test_add_years_leap_year_to_leap_year() {
        // Feb 29 in leap year to another leap year (2024 -> 2028)
        let dt = make_datetime(2024, 2, 29);
        let result = add_years(dt, 4);
        assert_eq!(result.year(), 2028);
        assert_eq!(result.month(), Month::February);
        assert_eq!(result.day(), 29);
    }

    #[test]
    fn test_add_years_leap_year_to_non_leap_year() {
        // Feb 29 in leap year to non-leap year should become Feb 28
        let dt = make_datetime(2024, 2, 29);
        let result = add_years(dt, 1);
        assert_eq!(result.year(), 2025);
        assert_eq!(result.month(), Month::February);
        assert_eq!(result.day(), 28);
    }

    #[test]
    fn test_add_years_negative() {
        // Subtract years
        let dt = make_datetime(2024, 3, 15);
        let result = add_years(dt, -2);
        assert_eq!(result.year(), 2022);
        assert_eq!(result.month(), Month::March);
        assert_eq!(result.day(), 15);
    }

    #[test]
    fn test_add_years_zero() {
        // Add zero years (should return same date)
        let dt = make_datetime(2024, 7, 4);
        let result = add_years(dt, 0);
        assert_eq!(result.year(), 2024);
        assert_eq!(result.month(), Month::July);
        assert_eq!(result.day(), 4);
    }

    #[test]
    fn test_is_retention_active_invalid_mode() {
        // Invalid mode should return false
        assert!(!is_retention_active("INVALID", None));
        assert!(!is_retention_active("", None));
    }

    #[test]
    fn test_is_retention_active_no_date() {
        // Valid mode but no retain_until_date should return false
        assert!(!is_retention_active(ObjectLockRetentionMode::COMPLIANCE, None));
        assert!(!is_retention_active(ObjectLockRetentionMode::GOVERNANCE, None));
    }

    #[test]
    fn test_is_retention_active_future_date() {
        // Valid mode with future retain_until_date should return true
        let future_date = OffsetDateTime::now_utc() + time::Duration::days(30);
        let s3_date = s3s::dto::Date::from(future_date);

        assert!(is_retention_active(ObjectLockRetentionMode::COMPLIANCE, Some(&s3_date)));
        let future_date = OffsetDateTime::now_utc() + time::Duration::days(30);
        let s3_date = s3s::dto::Date::from(future_date);
        assert!(is_retention_active(ObjectLockRetentionMode::GOVERNANCE, Some(&s3_date)));
    }

    #[test]
    fn test_is_retention_active_past_date() {
        // Valid mode with past retain_until_date should return false
        let past_date = OffsetDateTime::now_utc() - time::Duration::days(30);
        let s3_date = s3s::dto::Date::from(past_date);

        assert!(!is_retention_active(ObjectLockRetentionMode::COMPLIANCE, Some(&s3_date)));
        let past_date = OffsetDateTime::now_utc() - time::Duration::days(30);
        let s3_date = s3s::dto::Date::from(past_date);
        assert!(!is_retention_active(ObjectLockRetentionMode::GOVERNANCE, Some(&s3_date)));
    }

    #[test]
    fn test_check_retention_for_modification_no_existing_retention() {
        // No existing retention - modification should be allowed
        let user_defined = std::collections::HashMap::new();
        let new_retain = Some(OffsetDateTime::now_utc() + time::Duration::days(30));
        assert!(check_retention_for_modification(&user_defined, None, new_retain, false).is_none());
    }

    #[test]
    fn test_check_retention_for_modification_compliance_extend() {
        // COMPLIANCE mode - extending retention should be allowed
        let mut user_defined = std::collections::HashMap::new();
        let existing_retain = OffsetDateTime::now_utc() + time::Duration::days(30);
        user_defined.insert("x-amz-object-lock-mode".to_string(), "COMPLIANCE".to_string());
        user_defined.insert(
            "x-amz-object-lock-retain-until-date".to_string(),
            existing_retain
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap(),
        );

        // Extending by another 30 days should be allowed
        let new_retain = Some(existing_retain + time::Duration::days(30));
        assert!(
            check_retention_for_modification(&user_defined, Some(ObjectLockRetentionMode::COMPLIANCE), new_retain, false)
                .is_none()
        );
    }

    #[test]
    fn test_check_retention_for_modification_compliance_shorten() {
        // COMPLIANCE mode - shortening retention should be blocked
        let mut user_defined = std::collections::HashMap::new();
        let existing_retain = OffsetDateTime::now_utc() + time::Duration::days(60);
        user_defined.insert("x-amz-object-lock-mode".to_string(), "COMPLIANCE".to_string());
        user_defined.insert(
            "x-amz-object-lock-retain-until-date".to_string(),
            existing_retain
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap(),
        );

        // Shortening to 30 days should be blocked
        let new_retain = Some(OffsetDateTime::now_utc() + time::Duration::days(30));
        let result =
            check_retention_for_modification(&user_defined, Some(ObjectLockRetentionMode::COMPLIANCE), new_retain, false);
        assert!(result.is_some());
        assert!(matches!(result, Some(ObjectLockBlockReason::Retention { .. })));
    }

    #[test]
    fn test_check_retention_for_modification_compliance_clear() {
        // COMPLIANCE mode - clearing retention should be blocked
        let mut user_defined = std::collections::HashMap::new();
        let existing_retain = OffsetDateTime::now_utc() + time::Duration::days(30);
        user_defined.insert("x-amz-object-lock-mode".to_string(), "COMPLIANCE".to_string());
        user_defined.insert(
            "x-amz-object-lock-retain-until-date".to_string(),
            existing_retain
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap(),
        );

        // Clearing (None) should be blocked
        let result = check_retention_for_modification(&user_defined, None, None, false);
        assert!(result.is_some());
    }

    #[test]
    fn test_check_retention_for_modification_governance_shorten_without_bypass() {
        // GOVERNANCE mode - shortening retention without bypass should be blocked
        let mut user_defined = std::collections::HashMap::new();
        let existing_retain = OffsetDateTime::now_utc() + time::Duration::days(30);
        user_defined.insert("x-amz-object-lock-mode".to_string(), "GOVERNANCE".to_string());
        user_defined.insert(
            "x-amz-object-lock-retain-until-date".to_string(),
            existing_retain
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap(),
        );

        // Shortening from 30 days to 15 days without bypass should be blocked
        let new_retain = Some(OffsetDateTime::now_utc() + time::Duration::days(15));
        let result =
            check_retention_for_modification(&user_defined, Some(ObjectLockRetentionMode::GOVERNANCE), new_retain, false);
        assert!(result.is_some());
    }

    #[test]
    fn test_check_retention_for_modification_governance_extend_without_bypass() {
        // GOVERNANCE mode - extending retention without bypass should be allowed
        // This matches AWS S3 behavior where extending is always allowed
        let mut user_defined = std::collections::HashMap::new();
        let existing_retain = OffsetDateTime::now_utc() + time::Duration::days(30);
        user_defined.insert("x-amz-object-lock-mode".to_string(), "GOVERNANCE".to_string());
        user_defined.insert(
            "x-amz-object-lock-retain-until-date".to_string(),
            existing_retain
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap(),
        );

        // Extending from 30 days to 60 days without bypass should be allowed
        let new_retain = Some(OffsetDateTime::now_utc() + time::Duration::days(60));
        assert!(
            check_retention_for_modification(&user_defined, Some(ObjectLockRetentionMode::GOVERNANCE), new_retain, false)
                .is_none()
        );
    }

    #[test]
    fn test_check_retention_for_modification_governance_shorten_with_bypass() {
        // GOVERNANCE mode - shortening retention with bypass should be allowed
        let mut user_defined = std::collections::HashMap::new();
        let existing_retain = OffsetDateTime::now_utc() + time::Duration::days(30);
        user_defined.insert("x-amz-object-lock-mode".to_string(), "GOVERNANCE".to_string());
        user_defined.insert(
            "x-amz-object-lock-retain-until-date".to_string(),
            existing_retain
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap(),
        );

        // Shortening from 30 days to 15 days with bypass should be allowed
        let new_retain = Some(OffsetDateTime::now_utc() + time::Duration::days(15));
        assert!(
            check_retention_for_modification(&user_defined, Some(ObjectLockRetentionMode::GOVERNANCE), new_retain, true)
                .is_none()
        );
    }

    #[test]
    fn test_check_retention_for_modification_governance_mode_change_without_bypass() {
        let mut user_defined = std::collections::HashMap::new();
        let existing_retain = OffsetDateTime::now_utc() + time::Duration::days(30);
        user_defined.insert("x-amz-object-lock-mode".to_string(), "GOVERNANCE".to_string());
        user_defined.insert(
            "x-amz-object-lock-retain-until-date".to_string(),
            existing_retain
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap(),
        );

        let result = check_retention_for_modification(
            &user_defined,
            Some(ObjectLockRetentionMode::COMPLIANCE),
            Some(existing_retain),
            false,
        );
        assert!(result.is_some());
    }

    #[test]
    fn test_check_retention_for_modification_governance_mode_change_with_bypass() {
        let mut user_defined = std::collections::HashMap::new();
        let existing_retain = OffsetDateTime::now_utc() + time::Duration::days(30);
        user_defined.insert("x-amz-object-lock-mode".to_string(), "GOVERNANCE".to_string());
        user_defined.insert(
            "x-amz-object-lock-retain-until-date".to_string(),
            existing_retain
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap(),
        );

        assert!(
            check_retention_for_modification(
                &user_defined,
                Some(ObjectLockRetentionMode::COMPLIANCE),
                Some(existing_retain),
                true,
            )
            .is_none()
        );
    }

    #[test]
    fn test_check_retention_for_modification_compliance_mode_change() {
        let mut user_defined = std::collections::HashMap::new();
        let existing_retain = OffsetDateTime::now_utc() + time::Duration::days(30);
        user_defined.insert("x-amz-object-lock-mode".to_string(), "COMPLIANCE".to_string());
        user_defined.insert(
            "x-amz-object-lock-retain-until-date".to_string(),
            existing_retain
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap(),
        );

        let result = check_retention_for_modification(
            &user_defined,
            Some(ObjectLockRetentionMode::GOVERNANCE),
            Some(existing_retain),
            true,
        );
        assert!(result.is_some());
    }

    #[test]
    fn test_is_object_locked_by_metadata_delete_marker() {
        // Delete markers are never locked
        let user_defined = std::collections::HashMap::new();
        assert!(!is_object_locked_by_metadata(&user_defined, true));
    }

    #[test]
    fn test_is_object_locked_by_metadata_legal_hold_on() {
        // Legal hold ON should be locked
        let mut user_defined = std::collections::HashMap::new();
        user_defined.insert("x-amz-object-lock-legal-hold".to_string(), "ON".to_string());
        assert!(is_object_locked_by_metadata(&user_defined, false));
    }

    #[test]
    fn test_is_object_locked_by_metadata_legal_hold_off() {
        // Legal hold OFF should not be locked
        let mut user_defined = std::collections::HashMap::new();
        user_defined.insert("x-amz-object-lock-legal-hold".to_string(), "OFF".to_string());
        assert!(!is_object_locked_by_metadata(&user_defined, false));
    }

    #[test]
    fn test_is_object_locked_by_metadata_retention_active() {
        // Active retention should be locked
        let mut user_defined = std::collections::HashMap::new();
        let future_date = OffsetDateTime::now_utc() + time::Duration::days(30);
        user_defined.insert("x-amz-object-lock-mode".to_string(), "COMPLIANCE".to_string());
        user_defined.insert(
            "x-amz-object-lock-retain-until-date".to_string(),
            future_date.format(&time::format_description::well_known::Rfc3339).unwrap(),
        );
        assert!(is_object_locked_by_metadata(&user_defined, false));
    }

    #[test]
    fn test_is_object_locked_by_metadata_retention_expired() {
        // Expired retention should not be locked
        let mut user_defined = std::collections::HashMap::new();
        let past_date = OffsetDateTime::now_utc() - time::Duration::days(30);
        user_defined.insert("x-amz-object-lock-mode".to_string(), "COMPLIANCE".to_string());
        user_defined.insert(
            "x-amz-object-lock-retain-until-date".to_string(),
            past_date.format(&time::format_description::well_known::Rfc3339).unwrap(),
        );
        assert!(!is_object_locked_by_metadata(&user_defined, false));
    }

    #[test]
    fn test_is_object_locked_by_metadata_no_lock() {
        // No lock settings should not be locked
        let user_defined = std::collections::HashMap::new();
        assert!(!is_object_locked_by_metadata(&user_defined, false));
    }
}
