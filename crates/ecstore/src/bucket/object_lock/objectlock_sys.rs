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

use crate::bucket::metadata_sys::get_object_lock_config;
use crate::bucket::object_lock::objectlock;
use crate::store_api::ObjectInfo;
use s3s::dto::{DefaultRetention, ObjectLockLegalHoldStatus, ObjectLockRetentionMode};
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

/// Check if a retention period is still active based on mode and retain_until_date
#[cfg_attr(test, allow(dead_code))]
pub(crate) fn is_retention_active(mode: &str, retain_until_date: Option<s3s::dto::Date>) -> bool {
    if mode != ObjectLockRetentionMode::COMPLIANCE && mode != ObjectLockRetentionMode::GOVERNANCE {
        return false;
    }
    if let Some(retain_until) = retain_until_date {
        let now = objectlock::utc_now_ntp();
        return OffsetDateTime::from(retain_until).unix_timestamp() > now.unix_timestamp();
    }
    false
}

/// Add years to an OffsetDateTime, correctly handling leap years.
/// If the source date is Feb 29 and the target year is not a leap year,
/// it will fall back to Feb 28 of the target year.
pub(crate) fn add_years(dt: OffsetDateTime, years: i32) -> OffsetDateTime {
    let target_year = dt.year() + years;
    // Try to replace the year directly
    if let Ok(new_dt) = dt.replace_year(target_year) {
        return new_dt;
    }
    // Handle Feb 29 -> non-leap year case: use Feb 28
    if dt.month() == time::Month::February
        && dt.day() == 29
        && let Ok(new_dt) = dt.replace_day(28).and_then(|d| d.replace_year(target_year))
    {
        return new_dt;
    }
    // Fallback: iteratively add years one at a time to handle edge cases correctly
    // This is more accurate than multiplying by 365 days
    let mut result = dt;
    let step = if years > 0 { 1 } else { -1 };
    for _ in 0..years.abs() {
        let next_year = result.year() + step;
        result = if let Ok(new_dt) = result.replace_year(next_year) {
            new_dt
        } else {
            // Feb 29 -> non-leap year: use Feb 28
            result
                .replace_day(28)
                .and_then(|d| d.replace_year(next_year))
                .unwrap_or(result)
        };
    }
    result
}

/// Check if object deletion should be blocked due to Object Lock.
/// This checks:
/// 1. Legal hold status (cannot be bypassed)
/// 2. Explicit retention on the object (from object metadata)
/// 3. Default retention from bucket configuration (calculated from object creation time)
///
/// # Arguments
/// * `bucket` - The bucket name
/// * `obj_info` - Object information
/// * `bypass_governance` - If true, GOVERNANCE mode retention can be bypassed (requires permission check by caller)
///
/// # S3 Standard Behavior
/// - COMPLIANCE mode: Cannot be deleted even with bypass header
/// - GOVERNANCE mode: Can be deleted if bypass_governance is true (caller must verify s3:BypassGovernanceRetention permission)
/// - Legal Hold: Cannot be bypassed regardless of mode
pub async fn check_object_lock_for_deletion(bucket: &str, obj_info: &ObjectInfo, bypass_governance: bool) -> bool {
    if obj_info.delete_marker {
        return false;
    }

    // 1. Check legal hold - cannot be bypassed
    let lhold = objectlock::get_object_legalhold_meta(&obj_info.user_defined);
    if matches!(lhold.status, Some(st) if st.as_str() == ObjectLockLegalHoldStatus::ON) {
        return true;
    }

    // 2. Check explicit retention
    let explicit_ret = objectlock::get_object_retention_meta(&obj_info.user_defined);
    if let Some(mode) = &explicit_ret.mode {
        let mode_str = mode.as_str();
        if is_retention_active(mode_str, explicit_ret.retain_until_date) {
            // COMPLIANCE mode cannot be bypassed; GOVERNANCE can only be bypassed with permission
            let can_bypass = mode_str == ObjectLockRetentionMode::GOVERNANCE && bypass_governance;
            if !can_bypass {
                return true;
            }
        }
    }

    // 3. Check default retention only if no explicit retention is set
    if explicit_ret.mode.is_none()
        && let Some(default_retention) = BucketObjectLockSys::get(bucket).await
        && let Some(mode) = &default_retention.mode
    {
        let mode_str = mode.as_str();
        if mode_str == ObjectLockRetentionMode::COMPLIANCE || mode_str == ObjectLockRetentionMode::GOVERNANCE {
            // Calculate retention expiration date from object modification time
            if let Some(mod_time) = obj_info.mod_time {
                let now = objectlock::utc_now_ntp();
                let retain_until = if let Some(days) = default_retention.days {
                    mod_time.saturating_add(time::Duration::days(days as i64))
                } else if let Some(years) = default_retention.years {
                    add_years(mod_time, years)
                } else {
                    return false; // No retention period specified
                };

                if retain_until.unix_timestamp() > now.unix_timestamp() {
                    // COMPLIANCE mode cannot be bypassed; GOVERNANCE can only be bypassed with permission
                    let can_bypass = mode_str == ObjectLockRetentionMode::GOVERNANCE && bypass_governance;
                    if !can_bypass {
                        return true; // Object is still under retention
                    }
                }
            }
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::{Date, Month, PrimitiveDateTime, Time};

    fn make_datetime(year: i32, month: u8, day: u8) -> OffsetDateTime {
        let date = Date::from_calendar_date(year, Month::try_from(month).unwrap(), day).unwrap();
        let time = Time::from_hms(0, 0, 0).unwrap();
        PrimitiveDateTime::new(date, time).assume_utc()
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

        assert!(is_retention_active(ObjectLockRetentionMode::COMPLIANCE, Some(s3_date)));
        let future_date = OffsetDateTime::now_utc() + time::Duration::days(30);
        let s3_date = s3s::dto::Date::from(future_date);
        assert!(is_retention_active(ObjectLockRetentionMode::GOVERNANCE, Some(s3_date)));
    }

    #[test]
    fn test_is_retention_active_past_date() {
        // Valid mode with past retain_until_date should return false
        let past_date = OffsetDateTime::now_utc() - time::Duration::days(30);
        let s3_date = s3s::dto::Date::from(past_date);

        assert!(!is_retention_active(ObjectLockRetentionMode::COMPLIANCE, Some(s3_date)));
        let past_date = OffsetDateTime::now_utc() - time::Duration::days(30);
        let s3_date = s3s::dto::Date::from(past_date);
        assert!(!is_retention_active(ObjectLockRetentionMode::GOVERNANCE, Some(s3_date)));
    }
}
