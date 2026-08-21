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

use std::collections::HashMap;

use s3s::dto::{ObjectLockConfiguration, ObjectLockRetentionMode};
use s3s::header::{X_AMZ_OBJECT_LOCK_LEGAL_HOLD, X_AMZ_OBJECT_LOCK_MODE, X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE};
use time::{OffsetDateTime, format_description};

pub fn is_object_locked_by_metadata(user_defined: &HashMap<String, String>, is_delete_marker: bool) -> bool {
    if is_delete_marker {
        return false;
    }

    if user_defined
        .get(X_AMZ_OBJECT_LOCK_LEGAL_HOLD.as_str())
        .is_some_and(|value| value.eq_ignore_ascii_case("ON"))
    {
        return true;
    }

    let Some(mode) = user_defined.get(X_AMZ_OBJECT_LOCK_MODE.as_str()) else {
        return false;
    };
    if !is_retention_mode(mode) {
        return false;
    }

    user_defined
        .get(X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.as_str())
        .and_then(|value| OffsetDateTime::parse(value, &format_description::well_known::Iso8601::DEFAULT).ok())
        .is_some_and(|retain_until| retain_until.unix_timestamp() > OffsetDateTime::now_utc().unix_timestamp())
}

/// Check persisted object-lock metadata and the bucket default retention.
///
/// A configured default retention with missing or malformed input is treated
/// as locked so a lifecycle worker cannot turn incomplete metadata into an
/// unsafe delete.
pub fn is_object_locked(
    user_defined: &HashMap<String, String>,
    is_delete_marker: bool,
    config: Option<&ObjectLockConfiguration>,
    mod_time: Option<OffsetDateTime>,
) -> bool {
    if is_delete_marker {
        return false;
    }
    if is_object_locked_by_metadata(user_defined, false) {
        return true;
    }
    if has_explicit_lock_metadata(user_defined) {
        return !explicit_lock_metadata_is_well_formed(user_defined);
    }

    let Some(default_retention) = config.and_then(|config| config.rule.as_ref()?.default_retention.as_ref()) else {
        return false;
    };
    let Some(mode) = default_retention.mode.as_ref() else {
        return true;
    };
    if !is_retention_mode(mode.as_str()) {
        return true;
    }

    let Some(mod_time) = mod_time else {
        return true;
    };
    let Some(retain_until) = default_retention_until(mod_time, default_retention) else {
        return true;
    };

    retain_until.unix_timestamp() > OffsetDateTime::now_utc().unix_timestamp()
}

fn has_explicit_lock_metadata(user_defined: &HashMap<String, String>) -> bool {
    user_defined.contains_key(X_AMZ_OBJECT_LOCK_LEGAL_HOLD.as_str())
        || user_defined.contains_key(X_AMZ_OBJECT_LOCK_MODE.as_str())
        || user_defined.contains_key(X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.as_str())
}

fn explicit_lock_metadata_is_well_formed(user_defined: &HashMap<String, String>) -> bool {
    if user_defined
        .get(X_AMZ_OBJECT_LOCK_LEGAL_HOLD.as_str())
        .is_some_and(|value| !value.eq_ignore_ascii_case("ON") && !value.eq_ignore_ascii_case("OFF"))
    {
        return false;
    }

    match (
        user_defined.get(X_AMZ_OBJECT_LOCK_MODE.as_str()),
        user_defined.get(X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.as_str()),
    ) {
        (None, None) => true,
        (Some(mode), Some(retain_until)) => {
            is_retention_mode(mode)
                && OffsetDateTime::parse(retain_until, &format_description::well_known::Iso8601::DEFAULT).is_ok()
        }
        _ => false,
    }
}

fn default_retention_until(mod_time: OffsetDateTime, retention: &s3s::dto::DefaultRetention) -> Option<OffsetDateTime> {
    match (retention.days, retention.years) {
        (Some(days), None) if days > 0 => Some(mod_time.saturating_add(time::Duration::days(i64::from(days)))),
        (None, Some(years)) if years > 0 => add_years(mod_time, years),
        _ => None,
    }
}

fn add_years(mod_time: OffsetDateTime, years: i32) -> Option<OffsetDateTime> {
    let target_year = mod_time.year().checked_add(years)?;
    mod_time
        .replace_year(target_year)
        .or_else(|_| mod_time.replace_day(28).and_then(|date| date.replace_year(target_year)))
        .ok()
}

fn is_retention_mode(mode: &str) -> bool {
    mode.eq_ignore_ascii_case(ObjectLockRetentionMode::COMPLIANCE)
        || mode.eq_ignore_ascii_case(ObjectLockRetentionMode::GOVERNANCE)
}

#[cfg(test)]
mod tests {
    use super::*;

    use s3s::dto::{DefaultRetention, ObjectLockEnabled, ObjectLockRule};
    use time::Duration;

    #[test]
    fn is_object_locked_by_metadata_preserves_object_lock_parser_behavior() {
        let mut user_defined = HashMap::new();
        user_defined.insert(X_AMZ_OBJECT_LOCK_LEGAL_HOLD.as_str().to_string(), "ON".to_string());

        assert!(is_object_locked_by_metadata(&user_defined, false));
        assert!(!is_object_locked_by_metadata(&user_defined, true));
    }

    fn default_retention_config(days: i32) -> ObjectLockConfiguration {
        ObjectLockConfiguration {
            object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
            rule: Some(ObjectLockRule {
                default_retention: Some(DefaultRetention {
                    days: Some(days),
                    mode: Some(ObjectLockRetentionMode::from_static(ObjectLockRetentionMode::GOVERNANCE)),
                    years: None,
                }),
            }),
        }
    }

    #[test]
    fn default_retention_blocks_lifecycle_delete_until_expired() {
        let config = default_retention_config(30);
        let created = OffsetDateTime::now_utc() - Duration::days(1);

        assert!(is_object_locked(&HashMap::new(), false, Some(&config), Some(created)));
    }

    #[test]
    fn expired_default_retention_allows_lifecycle_delete() {
        let config = default_retention_config(1);
        let created = OffsetDateTime::now_utc() - Duration::days(2);

        assert!(!is_object_locked(&HashMap::new(), false, Some(&config), Some(created)));
    }

    #[test]
    fn missing_mod_time_blocks_default_retention_delete() {
        let config = default_retention_config(30);

        assert!(is_object_locked(&HashMap::new(), false, Some(&config), None));
    }

    #[test]
    fn zero_default_retention_days_fail_closed() {
        let config = default_retention_config(0);
        let created = OffsetDateTime::now_utc() - Duration::days(2);

        assert!(is_object_locked(&HashMap::new(), false, Some(&config), Some(created)));
    }

    #[test]
    fn zero_default_retention_years_fail_closed() {
        let config = ObjectLockConfiguration {
            object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
            rule: Some(ObjectLockRule {
                default_retention: Some(DefaultRetention {
                    days: None,
                    mode: Some(ObjectLockRetentionMode::from_static(ObjectLockRetentionMode::GOVERNANCE)),
                    years: Some(0),
                }),
            }),
        };
        let created = OffsetDateTime::now_utc() - Duration::days(2);

        assert!(is_object_locked(&HashMap::new(), false, Some(&config), Some(created)));
    }

    #[test]
    fn default_retention_years_block_lifecycle_delete_until_expired() {
        let config = ObjectLockConfiguration {
            object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
            rule: Some(ObjectLockRule {
                default_retention: Some(DefaultRetention {
                    days: None,
                    mode: Some(ObjectLockRetentionMode::from_static(ObjectLockRetentionMode::COMPLIANCE)),
                    years: Some(1),
                }),
            }),
        };
        let created = OffsetDateTime::now_utc() - Duration::days(1);

        assert!(is_object_locked(&HashMap::new(), false, Some(&config), Some(created)));
    }

    #[test]
    fn expired_explicit_retention_does_not_reapply_default_retention() {
        let config = default_retention_config(30);
        let mut user_defined = HashMap::new();
        user_defined.insert(
            X_AMZ_OBJECT_LOCK_MODE.as_str().to_string(),
            ObjectLockRetentionMode::GOVERNANCE.to_string(),
        );
        user_defined.insert(
            X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.as_str().to_string(),
            (OffsetDateTime::now_utc() - Duration::days(1))
                .format(&format_description::well_known::Iso8601::DEFAULT)
                .expect("expired retention date should format"),
        );

        assert!(!is_object_locked(&user_defined, false, Some(&config), Some(OffsetDateTime::now_utc())));
    }

    #[test]
    fn malformed_explicit_retention_fails_closed() {
        let config = default_retention_config(1);
        let mut user_defined = HashMap::new();
        user_defined.insert(
            X_AMZ_OBJECT_LOCK_MODE.as_str().to_string(),
            ObjectLockRetentionMode::GOVERNANCE.to_string(),
        );
        let created = OffsetDateTime::now_utc() - Duration::days(2);

        assert!(is_object_locked(&user_defined, false, Some(&config), Some(created)));
    }

    #[test]
    fn delete_markers_are_not_locked_by_default_retention() {
        let config = default_retention_config(30);

        assert!(!is_object_locked(&HashMap::new(), true, Some(&config), None));
    }
}
