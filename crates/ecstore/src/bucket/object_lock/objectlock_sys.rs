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

use crate::bucket::metadata_sys::{
    ObjectLockConfigState, default_retention_from_object_lock_config, get_object_lock_config, get_object_lock_config_state,
};
use crate::bucket::object_lock::objectlock;
use crate::bucket::object_lock::types::{DefaultRetention, LegalHoldStatus, RetentionMode};
use crate::error::{Error, Result, StorageError};
use crate::object_api::{ObjectInfo, ObjectOptions};
use rustfs_utils::http::headers::{
    AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER, AMZ_OBJECT_LOCK_MODE_LOWER, AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER,
};
use std::sync::Arc;
use time::OffsetDateTime;

pub struct BucketObjectLockSys {}

impl BucketObjectLockSys {
    #[allow(clippy::new_ret_no_self)]
    pub async fn new() -> Arc<Self> {
        Arc::new(Self {})
    }

    /// The bucket's active default retention, if the bucket has an
    /// authoritative Object Lock configuration with a usable
    /// GOVERNANCE/COMPLIANCE default retention rule.
    pub async fn get(bucket: &str) -> Option<DefaultRetention> {
        if let Ok(object_lock_config) = get_object_lock_config(bucket).await {
            return default_retention_from_object_lock_config(&object_lock_config.0);
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
pub fn is_retention_active(_mode: RetentionMode, retain_until_date: Option<OffsetDateTime>) -> bool {
    if let Some(retain_until) = retain_until_date {
        let now = objectlock::utc_now_ntp();
        return retain_until.unix_timestamp() > now.unix_timestamp();
    }
    false
}

/// Check if retention modification is blocked for the given object.
pub fn check_retention_for_modification(
    user_defined: &std::collections::HashMap<String, String>,
    new_mode: Option<RetentionMode>,
    new_retain_until: Option<OffsetDateTime>,
    bypass_governance: bool,
) -> Option<ObjectLockBlockReason> {
    let retention = objectlock::get_object_retention_meta(user_defined);

    let mode = retention.mode?;

    if !is_retention_active(mode, retention.retain_until_date) {
        return None;
    }

    let existing_retain_until = retention.retain_until_date;
    let mode_changed = new_mode != Some(mode);

    // Check if new retention period is shorter than existing
    let is_shortening = match (&existing_retain_until, &new_retain_until) {
        (Some(existing), Some(new)) => new < existing,
        (Some(_), None) => true, // Clearing retention is shortening
        _ => false,
    };

    match mode {
        // COMPLIANCE mode: cannot shorten retention at all (even with bypass)
        // Can only extend the retention period
        RetentionMode::Compliance => {
            if mode_changed || is_shortening {
                return Some(ObjectLockBlockReason::Retention {
                    mode,
                    retain_until: existing_retain_until,
                });
            }
            // Extending retention in COMPLIANCE mode is allowed
            None
        }
        // GOVERNANCE mode: extending is always allowed, shortening requires bypass
        // This matches AWS S3 behavior where:
        // - Extending retention: allowed without bypass permission
        // - Shortening/removing retention: requires bypass permission
        RetentionMode::Governance => {
            if (mode_changed || is_shortening) && !bypass_governance {
                return Some(ObjectLockBlockReason::Retention {
                    mode,
                    retain_until: existing_retain_until,
                });
            }
            // Extending retention or shortening with bypass is allowed
            None
        }
    }
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
fn has_legal_hold(user_defined: &std::collections::HashMap<String, String>) -> bool {
    objectlock::get_object_legalhold_meta(user_defined).is_on()
}

/// Whether an authorized replication write (`ObjectOptions::replication_request`)
/// may overwrite a locked destination version.
///
/// The source's lock state governs a replica (MinIO `checkPutObjectLockAllowed`
/// skips the existing-version check for replicas), and a source-side hold
/// release or retention change reaches this site only through this write. The
/// overwrite is allowed only when the write carries the source timestamp of
/// every category that currently locks the version, so receiver-side LWW
/// (`merge_replication_metadata_lww`) judges each of them: a category locked
/// more recently here is kept, otherwise the source's newer state wins. A write
/// without that timestamp carries no source decision for the category — the
/// metadata replace would lift the lock unjudged — so it stays WORM-rejected.
///
/// The locking categories come from the same authoritative evaluation as the
/// commit-time WORM gate (`check_object_lock_for_deletion_with_state`): the
/// bucket default retention locks a version that carries no explicit
/// retention keys, so it is judged here too rather than read off the keys.
/// Malformed persisted lock metadata or a non-authoritative bucket
/// configuration is an error, never a pass.
pub fn replication_write_may_pass_worm_gate(
    state: &ObjectLockConfigState,
    obj_info: &ObjectInfo,
    opts: &ObjectOptions,
) -> Result<bool> {
    if !opts.replication_request {
        return Ok(false);
    }
    if obj_info.delete_marker {
        // Delete markers are never locked (same as the WORM gate).
        return Ok(true);
    }
    let default_retention = default_retention_from_state(state)?;
    if legal_hold_locks(obj_info)? && opts.replication_legalhold_timestamp.is_none() {
        return Ok(false);
    }
    let retention_locked = active_retention(default_retention.as_ref(), obj_info)?.is_some();
    Ok(!(retention_locked && opts.replication_retention_timestamp.is_none()))
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
    if let Some(mode) = ret.mode
        && is_retention_active(mode, ret.retain_until_date)
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
        mode: RetentionMode,
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
    mode: RetentionMode,
    retain_until: Option<OffsetDateTime>,
    bypass_governance: bool,
) -> Option<ObjectLockBlockReason> {
    // COMPLIANCE mode cannot be bypassed; GOVERNANCE can only be bypassed with permission
    let can_bypass = mode == RetentionMode::Governance && bypass_governance;
    if !can_bypass {
        return Some(ObjectLockBlockReason::Retention { mode, retain_until });
    }
    None
}

/// Check an object's lock metadata using an already resolved bucket default
/// retention. `None` means the bucket configuration is confirmed absent or
/// carries no usable default retention rule.
///
/// # S3 Standard Behavior
/// - COMPLIANCE mode: Cannot be deleted even with bypass header
/// - GOVERNANCE mode: Can be deleted if bypass_governance is true (caller must verify s3:BypassGovernanceRetention permission)
/// - Legal Hold: Cannot be bypassed regardless of mode
pub(crate) fn check_object_lock_for_deletion_with_default_retention(
    default_retention: Option<&DefaultRetention>,
    obj_info: &ObjectInfo,
    bypass_governance: bool,
) -> Result<Option<ObjectLockBlockReason>> {
    if obj_info.delete_marker {
        return Ok(None);
    }

    if legal_hold_locks(obj_info)? {
        return Ok(Some(ObjectLockBlockReason::LegalHold));
    }

    if let Some((mode, retain_until)) = active_retention(default_retention, obj_info)?
        && let Some(reason) = check_retention_blocks_deletion(mode, Some(retain_until), bypass_governance)
    {
        return Ok(Some(reason));
    }

    Ok(None)
}

/// A cleared retention / legal hold is persisted as empty strings (the MinIO
/// on-disk shape, `parse_object_lock_retention`); read it as "no lock" rather
/// than as corrupt metadata.
fn persisted_lock_value<'a>(obj_info: &'a ObjectInfo, key: &str) -> Option<&'a String> {
    obj_info.user_defined.get(key).filter(|value| !value.is_empty())
}

/// Whether the version's persisted legal hold is ON. Any other non-empty
/// value than ON/OFF is malformed metadata and fails closed.
fn legal_hold_locks(obj_info: &ObjectInfo) -> Result<bool> {
    let Some(status) = persisted_lock_value(obj_info, AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER) else {
        return Ok(false);
    };
    match LegalHoldStatus::parse(status) {
        Some(LegalHoldStatus::On) => Ok(true),
        Some(LegalHoldStatus::Off) => Ok(false),
        None => Err(Error::other("persisted object legal-hold metadata is invalid")),
    }
}

/// The retention that currently locks the version, if any: the explicit
/// persisted retention when the keys are present, otherwise the bucket
/// default retention computed from the version's modification time. Returns
/// `(mode, retain_until)` only while the retention is still active.
fn active_retention(
    default_retention: Option<&DefaultRetention>,
    obj_info: &ObjectInfo,
) -> Result<Option<(RetentionMode, OffsetDateTime)>> {
    let mode = persisted_lock_value(obj_info, AMZ_OBJECT_LOCK_MODE_LOWER);
    let retain_until = persisted_lock_value(obj_info, AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER);
    match (mode, retain_until) {
        (None, None) => {}
        (Some(mode), Some(retain_until)) => {
            let mode =
                objectlock::parse_ret_mode(mode).ok_or_else(|| Error::other("persisted object retention mode is invalid"))?;
            let retain_until = OffsetDateTime::parse(retain_until, &time::format_description::well_known::Iso8601::DEFAULT)
                .map_err(|_| Error::other("persisted object retention date is invalid"))?;
            return Ok(is_retention_active(mode, Some(retain_until)).then_some((mode, retain_until)));
        }
        _ => return Err(Error::other("persisted object retention metadata is incomplete")),
    }

    let Some(default_retention) = default_retention else {
        return Ok(None);
    };
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
    Ok((retain_until.unix_timestamp() > now.unix_timestamp()).then_some((default_retention.mode, retain_until)))
}

/// The bucket default retention carried by an authoritative Object Lock
/// state. `ConfirmedAbsent` and a configuration without a usable default
/// retention rule are both `None`; a fabricated state is an error, never a
/// pass.
fn default_retention_from_state(state: &ObjectLockConfigState) -> Result<Option<DefaultRetention>> {
    match state {
        ObjectLockConfigState::Configured { config, .. } => Ok(default_retention_from_object_lock_config(config)),
        ObjectLockConfigState::ConfirmedAbsent => Ok(None),
        ObjectLockConfigState::Fabricated => Err(Error::other("bucket Object Lock metadata is not authoritative")),
    }
}

pub(crate) fn check_object_lock_for_deletion_with_state(
    state: &ObjectLockConfigState,
    obj_info: &ObjectInfo,
    bypass_governance: bool,
) -> Result<Option<ObjectLockBlockReason>> {
    check_object_lock_for_deletion_with_default_retention(
        default_retention_from_state(state)?.as_ref(),
        obj_info,
        bypass_governance,
    )
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
    use crate::bucket::metadata_sys::configured_object_lock_state_for_tests;
    use rustfs_utils::http::headers::{
        AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER, AMZ_OBJECT_LOCK_MODE_LOWER, AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER,
    };
    use time::{Date, Month, PrimitiveDateTime, Time};

    fn make_datetime(year: i32, month: u8, day: u8) -> OffsetDateTime {
        let date = Date::from_calendar_date(year, Month::try_from(month).unwrap(), day).unwrap();
        let time = Time::from_hms(0, 0, 0).unwrap();
        PrimitiveDateTime::new(date, time).assume_utc()
    }

    fn default_retention(mode: RetentionMode) -> DefaultRetention {
        DefaultRetention {
            mode,
            days: Some(30),
            years: None,
        }
    }

    #[test]
    fn deletion_with_config_blocks_active_default_compliance_even_with_bypass() {
        let retention = default_retention(RetentionMode::Compliance);
        let obj_info = ObjectInfo {
            mod_time: Some(OffsetDateTime::now_utc()),
            ..Default::default()
        };

        let result = check_object_lock_for_deletion_with_default_retention(Some(&retention), &obj_info, true);

        assert!(matches!(result, Ok(Some(ObjectLockBlockReason::Retention { .. }))));
    }

    #[test]
    fn deletion_with_config_allows_active_default_governance_with_bypass() {
        let retention = default_retention(RetentionMode::Governance);
        let obj_info = ObjectInfo {
            mod_time: Some(OffsetDateTime::now_utc()),
            ..Default::default()
        };

        assert!(matches!(
            check_object_lock_for_deletion_with_default_retention(Some(&retention), &obj_info, true),
            Ok(None)
        ));
    }

    #[test]
    fn deletion_with_default_retention_rejects_missing_object_mod_time() {
        let retention = default_retention(RetentionMode::Compliance);

        let err = check_object_lock_for_deletion_with_default_retention(Some(&retention), &ObjectInfo::default(), false)
            .expect_err("default retention needs an authoritative object modification time");

        assert!(err.to_string().contains("modification time"));
    }

    #[test]
    fn deletion_with_confirmed_absence_still_blocks_explicit_compliance() {
        let retain_until = OffsetDateTime::now_utc() + time::Duration::days(30);
        let mut user_defined = std::collections::HashMap::new();
        user_defined.insert("x-amz-object-lock-mode".to_string(), RetentionMode::COMPLIANCE.to_string());
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

        let result = check_object_lock_for_deletion_with_default_retention(None, &obj_info, true);

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
        user_defined.insert(AMZ_OBJECT_LOCK_MODE_LOWER.to_string(), RetentionMode::COMPLIANCE.to_string());
        let obj_info = ObjectInfo {
            user_defined: Arc::new(user_defined),
            ..Default::default()
        };

        let err = check_object_lock_for_deletion_with_default_retention(None, &obj_info, false)
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
            ("invalid date", Some(RetentionMode::COMPLIANCE), Some("not-a-date"), "retention date"),
            ("date only", None, Some(valid_date.as_str()), "incomplete"),
        ];

        for (case, mode, retain_until, expected) in cases {
            let mut user_defined = std::collections::HashMap::new();
            if let Some(mode) = mode {
                user_defined.insert(AMZ_OBJECT_LOCK_MODE_LOWER.to_string(), mode.to_string());
            }
            if let Some(retain_until) = retain_until {
                user_defined.insert(AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER.to_string(), retain_until.to_string());
            }
            let obj_info = ObjectInfo {
                user_defined: Arc::new(user_defined),
                ..Default::default()
            };

            let err = check_object_lock_for_deletion_with_default_retention(None, &obj_info, false).expect_err(case);
            assert!(err.to_string().contains(expected), "unexpected {case} error: {err}");
        }
    }

    fn replication_opts(hold_ts: bool, retention_ts: bool) -> ObjectOptions {
        ObjectOptions {
            replication_request: true,
            replication_legalhold_timestamp: hold_ts.then_some(OffsetDateTime::UNIX_EPOCH),
            replication_retention_timestamp: retention_ts.then_some(OffsetDateTime::UNIX_EPOCH),
            ..Default::default()
        }
    }

    fn lock_metadata(entries: &[&[(&str, &str)]]) -> std::collections::HashMap<String, String> {
        entries
            .iter()
            .flat_map(|entries| entries.iter())
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    fn lock_object_info(user_defined: std::collections::HashMap<String, String>) -> ObjectInfo {
        ObjectInfo {
            user_defined: Arc::new(user_defined),
            mod_time: Some(OffsetDateTime::now_utc()),
            ..Default::default()
        }
    }

    /// A replication write passes the WORM gate only when it carries the
    /// source timestamp of every category that currently locks the version.
    #[test]
    fn replication_write_passes_worm_gate_only_with_every_locking_category_timestamp() {
        let hold = [(AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER, "ON")];
        let retention = [
            (AMZ_OBJECT_LOCK_MODE_LOWER, "GOVERNANCE"),
            (AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER, "2099-01-01T00:00:00Z"),
        ];
        let expired = [
            (AMZ_OBJECT_LOCK_MODE_LOWER, "COMPLIANCE"),
            (AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER, "2000-01-01T00:00:00Z"),
        ];
        let absent = ObjectLockConfigState::ConfirmedAbsent;
        let passes = |state: &ObjectLockConfigState, entries: &[&[(&str, &str)]], opts: &ObjectOptions| {
            replication_write_may_pass_worm_gate(state, &lock_object_info(lock_metadata(entries)), opts)
                .expect("well-formed lock metadata must be judged")
        };

        assert!(passes(&absent, &[&hold, &retention], &replication_opts(true, true)));
        assert!(!passes(&absent, &[&hold, &retention], &replication_opts(true, false)));
        assert!(!passes(&absent, &[&hold, &retention], &replication_opts(false, true)));

        assert!(passes(&absent, &[&hold], &replication_opts(true, false)));
        assert!(!passes(&absent, &[&hold], &replication_opts(false, true)));
        assert!(passes(&absent, &[&retention], &replication_opts(false, true)));
        assert!(!passes(&absent, &[&retention], &replication_opts(true, false)));

        // Expired retention and a released hold no longer lock anything.
        assert!(passes(
            &absent,
            &[&expired, &[(AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER, "OFF")]],
            &replication_opts(false, false)
        ));

        // Never for a non-replication write, whatever it carries.
        let local = ObjectOptions {
            replication_request: false,
            ..replication_opts(true, true)
        };
        assert!(!passes(&absent, &[&hold], &local));
    }

    /// The bucket default retention locks a version that carries no explicit
    /// retention keys (`check_object_lock_for_deletion_with_default_retention`
    /// judges it from the modification time), so the replication bypass must
    /// demand the retention source timestamp for it too — a tagging-only
    /// replication write must not overwrite the default-protected version
    /// unjudged.
    #[test]
    fn replication_write_under_bucket_default_retention_requires_retention_timestamp() {
        for mode in [RetentionMode::Compliance, RetentionMode::Governance] {
            let state = configured_object_lock_state_for_tests(mode, 30);
            let no_keys = lock_object_info(std::collections::HashMap::new());
            assert!(
                check_object_lock_for_deletion_with_state(&state, &no_keys, false)
                    .expect("default retention must be judged")
                    .is_some(),
                "{mode}: the gate must report the default retention lock"
            );

            let tagging_only = ObjectOptions {
                replication_request: true,
                replication_tagging_timestamp: Some(OffsetDateTime::UNIX_EPOCH),
                ..Default::default()
            };
            assert!(
                !replication_write_may_pass_worm_gate(&state, &no_keys, &tagging_only).expect("judged"),
                "{mode}: a tagging-only replication write must not pass the default retention lock"
            );
            assert!(
                replication_write_may_pass_worm_gate(&state, &no_keys, &replication_opts(false, true)).expect("judged"),
                "{mode}: the retention source timestamp lets LWW judge the default retention"
            );

            // Default retention plus a legal hold: both categories need a timestamp.
            let held = lock_object_info(lock_metadata(&[&[(AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER, "ON")]]));
            assert!(!replication_write_may_pass_worm_gate(&state, &held, &replication_opts(false, true)).expect("judged"));
            assert!(!replication_write_may_pass_worm_gate(&state, &held, &replication_opts(true, false)).expect("judged"));
            assert!(replication_write_may_pass_worm_gate(&state, &held, &replication_opts(true, true)).expect("judged"));

            // A version whose default retention has already expired (old
            // mod_time) is not locked by the default any more.
            let expired_default = ObjectInfo {
                mod_time: Some(make_datetime(2000, 1, 1)),
                ..lock_object_info(std::collections::HashMap::new())
            };
            assert!(replication_write_may_pass_worm_gate(&state, &expired_default, &tagging_only).expect("judged"));

            // A delete marker is never locked, so there is nothing to judge.
            let delete_marker = ObjectInfo {
                delete_marker: true,
                ..lock_object_info(std::collections::HashMap::new())
            };
            assert!(replication_write_may_pass_worm_gate(&state, &delete_marker, &tagging_only).expect("judged"));

            // Cleared (empty) explicit keys fall back to the bucket default.
            let cleared = lock_object_info(lock_metadata(&[&[(AMZ_OBJECT_LOCK_MODE_LOWER, "")]]));
            assert!(!replication_write_may_pass_worm_gate(&state, &cleared, &tagging_only).expect("judged"));
        }
    }

    /// The replication bypass never judges from a non-authoritative bucket
    /// state or malformed persisted lock metadata; both are errors, not a pass.
    #[test]
    fn replication_write_worm_gate_fails_closed_on_unverifiable_lock_state() {
        let opts = replication_opts(true, true);
        let err = replication_write_may_pass_worm_gate(
            &ObjectLockConfigState::Fabricated,
            &lock_object_info(std::collections::HashMap::new()),
            &opts,
        )
        .expect_err("fabricated bucket lock metadata must not be judged");
        assert!(err.to_string().contains("not authoritative"));

        let malformed = lock_object_info(lock_metadata(&[&[(AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER, "MAYBE")]]));
        let err = replication_write_may_pass_worm_gate(&ObjectLockConfigState::ConfirmedAbsent, &malformed, &opts)
            .expect_err("malformed legal hold must not be judged");
        assert!(err.to_string().contains("legal-hold"));

        let state = configured_object_lock_state_for_tests(RetentionMode::Compliance, 30);
        let no_mod_time = ObjectInfo::default();
        let err = replication_write_may_pass_worm_gate(&state, &no_mod_time, &opts)
            .expect_err("default retention without a modification time must not be judged");
        assert!(err.to_string().contains("modification time"));
    }

    /// A local PutObjectRetention / PutObjectLegalHold "clear" persists the
    /// lock keys as empty strings (the MinIO on-disk shape, see
    /// `parse_object_lock_retention`); that is "no lock", not corruption, and
    /// must not wedge later explicit-version PUTs or deletes
    /// (rustfs/backlog#1953).
    #[test]
    fn deletion_treats_cleared_empty_lock_metadata_as_unlocked() {
        let cases: [(&str, &[&str]); 3] = [
            (
                "cleared retention",
                &[AMZ_OBJECT_LOCK_MODE_LOWER, AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER],
            ),
            ("cleared legal hold", &[AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER]),
            (
                "all cleared",
                &[
                    AMZ_OBJECT_LOCK_MODE_LOWER,
                    AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER,
                    AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER,
                ],
            ),
        ];

        for (case, keys) in cases {
            let user_defined = keys.iter().map(|key| (key.to_string(), String::new())).collect();
            let obj_info = ObjectInfo {
                user_defined: Arc::new(user_defined),
                ..Default::default()
            };

            let result = check_object_lock_for_deletion_with_default_retention(None, &obj_info, false);
            assert!(matches!(result, Ok(None)), "{case}: empty lock keys must read as unlocked: {result:?}");
        }
    }

    #[test]
    fn deletion_rejects_invalid_persisted_legal_hold_metadata() {
        let mut user_defined = std::collections::HashMap::new();
        user_defined.insert(AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER.to_string(), "INVALID".to_string());
        let obj_info = ObjectInfo {
            user_defined: Arc::new(user_defined),
            ..Default::default()
        };

        let err = check_object_lock_for_deletion_with_default_retention(None, &obj_info, false)
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
    fn test_is_retention_active_no_date() {
        // Valid mode but no retain_until_date should return false
        assert!(!is_retention_active(RetentionMode::Compliance, None));
        assert!(!is_retention_active(RetentionMode::Governance, None));
    }

    #[test]
    fn test_is_retention_active_future_date() {
        // Valid mode with future retain_until_date should return true
        let future_date = OffsetDateTime::now_utc() + time::Duration::days(30);

        assert!(is_retention_active(RetentionMode::Compliance, Some(future_date)));
        assert!(is_retention_active(RetentionMode::Governance, Some(future_date)));
    }

    #[test]
    fn test_is_retention_active_past_date() {
        // Valid mode with past retain_until_date should return false
        let past_date = OffsetDateTime::now_utc() - time::Duration::days(30);

        assert!(!is_retention_active(RetentionMode::Compliance, Some(past_date)));
        assert!(!is_retention_active(RetentionMode::Governance, Some(past_date)));
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
        assert!(check_retention_for_modification(&user_defined, Some(RetentionMode::Compliance), new_retain, false).is_none());
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
        let result = check_retention_for_modification(&user_defined, Some(RetentionMode::Compliance), new_retain, false);
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
        let result = check_retention_for_modification(&user_defined, Some(RetentionMode::Governance), new_retain, false);
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
        assert!(check_retention_for_modification(&user_defined, Some(RetentionMode::Governance), new_retain, false).is_none());
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
        assert!(check_retention_for_modification(&user_defined, Some(RetentionMode::Governance), new_retain, true).is_none());
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

        let result =
            check_retention_for_modification(&user_defined, Some(RetentionMode::Compliance), Some(existing_retain), false);
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
            check_retention_for_modification(&user_defined, Some(RetentionMode::Compliance), Some(existing_retain), true)
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

        let result =
            check_retention_for_modification(&user_defined, Some(RetentionMode::Governance), Some(existing_retain), true);
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
