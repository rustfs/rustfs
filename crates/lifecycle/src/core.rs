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

use rustfs_config::{DEFAULT_ILM_DAY_SECS, ENV_ILM_DEBUG_DAY_SECS, ENV_ILM_PROCESS_TIME, ENV_ILM_PROCESS_TIME_DEPRECATED};
use rustfs_replication::{ReplicationStatusType, VersionPurgeStatusType};
use s3s::dto::{
    BucketLifecycleConfiguration, ExpirationStatus, LifecycleExpiration, LifecycleRule, LifecycleRuleFilter,
    NoncurrentVersionTransition, ObjectLockConfiguration, ObjectLockEnabled, RestoreRequest, Transition,
};
use std::collections::HashMap;
use std::sync::Arc;
use time::macros::offset;
use time::{self, Duration, OffsetDateTime};
use tracing::{debug, warn};
use uuid::Uuid;

use crate::rule::{Filter, NoncurrentVersionTransitionOps, TransitionOps};

pub const TRANSITION_COMPLETE: &str = "complete";
pub const TRANSITION_PENDING: &str = "pending";
const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_LIFECYCLE: &str = "lifecycle";
const EVENT_LIFECYCLE_EXPIRY_COMPUTED: &str = "lifecycle_expiry_computed";
const EVENT_LIFECYCLE_DEBUG_DAY_SECS: &str = "lifecycle_debug_day_secs";
const ERR_LIFECYCLE_NO_RULE: &str = "Lifecycle configuration should have at least one rule";
const ERR_LIFECYCLE_DUPLICATE_ID: &str = "Rule ID must be unique. Found same ID for more than one rule";
const _ERR_XML_NOT_WELL_FORMED: &str =
    "The XML you provided was not well-formed or did not validate against our published schema";
const ERR_LIFECYCLE_BUCKET_LOCKED: &str =
    "ExpiredObjectAllVersions element and DelMarkerExpiration action cannot be used on an object locked bucket";
const ERR_LIFECYCLE_TOO_MANY_RULES: &str = "Lifecycle configuration should have at most 1000 rules";
const ERR_LIFECYCLE_INVALID_EXPIRATION_DAYS: &str = "'Days' for Expiration action must be a positive integer";
const ERR_LIFECYCLE_INVALID_NONCURRENT_EXPIRATION_DAYS: &str =
    "'NoncurrentDays' for NoncurrentVersionExpiration action must be a positive integer";
const ERR_LIFECYCLE_INVALID_ABORT_INCOMPLETE_MPU_DAYS: &str =
    "'DaysAfterInitiation' for AbortIncompleteMultipartUpload action must be a positive integer";
const ERR_LIFECYCLE_INVALID_EXPIRATION_DATE_NOT_MIDNIGHT: &str = "Expiration.Date must be at midnight UTC";
const ERR_LIFECYCLE_INVALID_EXPIRED_OBJECT_DELETE_MARKER: &str =
    "ExpiredObjectDeleteMarker cannot be specified with Days or Date";
const ERR_LIFECYCLE_INVALID_RULE_ID_TOO_LONG: &str = "Rule ID must be at most 255 characters";
const ERR_LIFECYCLE_INVALID_RULE_STATUS: &str = "Rule status must be either Enabled or Disabled";
const ERR_LIFECYCLE_DEL_MARKER_WITH_TAGS: &str = "Rule with DelMarkerExpiration cannot have tags based filtering";
const ERR_LIFECYCLE_EXPIRED_OBJECT_DELETE_MARKER_WITH_TAGS: &str =
    "Rule with ExpiredObjectDeleteMarker cannot have tags based filtering";
const ERR_LIFECYCLE_RULE_MUST_HAVE_ACTION: &str = "Rule must have at least one of Expiration, Transition, NoncurrentVersionExpiration, NoncurrentVersionTransition, or DelMarkerExpiration";
const ERR_LIFECYCLE_PREFIX_FILTER_CONFLICT: &str = "Legacy Prefix and Filter cannot both be present in a lifecycle rule. Use Filter.Prefix instead of the top-level Prefix element.";

pub use rustfs_common::metrics::IlmAction;

#[async_trait::async_trait]
pub trait RuleValidate {
    fn validate(&self) -> Result<(), std::io::Error>;
}

#[async_trait::async_trait]
impl RuleValidate for LifecycleRule {
    /*fn validate_id(&self) -> Result<()> {
        if self.id.len() > 255 {
            return errInvalidRuleID;
        }
        Ok(())
    }

    fn validate_status(&self) -> Result<()> {
        if self.status.len() == 0 {
            return ErrEmptyRuleStatus;
        }

        if self.status != Enabled && self.status != Disabled {
            return ErrInvalidRuleStatus;
        }
        Ok(())
    }

    fn validate_expiration(&self) -> Result<()> {
        self.expiration.validate();
    }

    fn validate_noncurrent_expiration(&self) -> Result<()> {
        self.noncurrent_version_expiration.validate()
    }

    fn validate_prefix_and_filter(&self) -> Result<()> {
        if !self.prefix.set && self.Filter.isempty() || self.prefix.set && !self.filter.isempty() {
            return ErrXMLNotWellFormed;
        }
        if !self.prefix.set {
            return self.filter.validate();
        }
        Ok(())
    }

    fn validate_transition(&self) -> Result<()> {
        self.Transition.Validate()
    }

    fn validate_noncurrent_transition(&self) -> Result<()> {
        self.NoncurrentVersionTransition.Validate()
    }

    fn get_prefix(&self) -> String {
        if p := self.Prefix.String(); p != "" {
          return p
        }
        if p := self.Filter.Prefix.String(); p != "" {
          return p
        }
        if p := self.Filter.And.Prefix.String(); p != "" {
          return p
        }
        "".to_string()
    }*/

    fn validate(&self) -> Result<(), std::io::Error> {
        // S3 standard: Legacy Prefix and Filter cannot both be present.
        // An empty prefix is treated as "not set" and is allowed with a Filter.
        let has_legacy_prefix = self.prefix.as_deref().is_some_and(|p| !p.is_empty());
        let has_filter = self.filter.is_some();
        if has_legacy_prefix && has_filter {
            return Err(std::io::Error::other(ERR_LIFECYCLE_PREFIX_FILTER_CONFLICT));
        }

        // Rule with DelMarkerExpiration cannot have tags based filtering
        let has_tag_filter = self
            .filter
            .as_ref()
            .is_some_and(|f| f.tag.is_some() || f.and.as_ref().and_then(|a| a.tags.as_ref()).is_some());
        if has_tag_filter && self.del_marker_expiration.is_some() {
            return Err(std::io::Error::other(ERR_LIFECYCLE_DEL_MARKER_WITH_TAGS));
        }
        if has_tag_filter
            && self
                .expiration
                .as_ref()
                .is_some_and(|expiration| expiration.expired_object_delete_marker.is_some_and(|v| v))
        {
            return Err(std::io::Error::other(ERR_LIFECYCLE_EXPIRED_OBJECT_DELETE_MARKER_WITH_TAGS));
        }
        if let Some(expiration) = &self.expiration
            && expiration.expired_object_delete_marker.is_some_and(|v| v)
            && (expiration.days.is_some() || expiration.date.is_some())
        {
            return Err(std::io::Error::other(ERR_LIFECYCLE_INVALID_EXPIRED_OBJECT_DELETE_MARKER));
        }
        // Rule must have at least one action
        let has_expiration = self.expiration.is_some();
        let has_transition = self.transitions.as_ref().is_some_and(|t| !t.is_empty());
        let has_noncurrent_expiration = self
            .noncurrent_version_expiration
            .as_ref()
            .and_then(|e| e.noncurrent_days)
            .is_some();
        let has_noncurrent_transition = self
            .noncurrent_version_transitions
            .as_ref()
            .and_then(|t| t.first())
            .and_then(|t| t.storage_class.as_ref())
            .is_some();
        let has_abort_incomplete_multipart_upload = self.abort_incomplete_multipart_upload.is_some();
        let has_del_marker_expiration = self
            .del_marker_expiration
            .as_ref()
            .and_then(|d| d.days)
            .is_some_and(|d| d > 0);
        if !has_expiration
            && !has_transition
            && !has_noncurrent_expiration
            && !has_noncurrent_transition
            && !has_abort_incomplete_multipart_upload
            && !has_del_marker_expiration
        {
            return Err(std::io::Error::other(ERR_LIFECYCLE_RULE_MUST_HAVE_ACTION));
        }
        Ok(())
    }
}

fn lifecycle_rule_prefix(rule: &LifecycleRule) -> Option<&str> {
    // Prefer a non-empty legacy prefix; treat an empty legacy prefix as if it were not set
    if let Some(p) = rule.prefix.as_deref()
        && !p.is_empty()
    {
        return Some(p);
    }

    let filter = rule.filter.as_ref()?;

    if let Some(p) = filter.prefix.as_deref() {
        return Some(p);
    }

    filter.and.as_ref().and_then(|and| and.prefix.as_deref())
}

#[async_trait::async_trait]
pub trait Lifecycle {
    async fn has_transition(&self) -> bool;
    fn has_expiry(&self) -> bool;
    fn has_active_rules(&self, prefix: &str) -> bool;
    async fn validate(&self, lr: &ObjectLockConfiguration) -> Result<(), std::io::Error>;
    async fn filter_rules(&self, obj: &ObjectOpts) -> Option<Vec<LifecycleRule>>;
    async fn eval(&self, obj: &ObjectOpts) -> Event;
    async fn predict_expiration(&self, obj: &ObjectOpts) -> Event;
    async fn eval_inner(&self, obj: &ObjectOpts, now: OffsetDateTime, newer_noncurrent_versions: usize) -> Event;
    //fn set_prediction_headers(&self, w: http.ResponseWriter, obj: ObjectOpts);
    async fn noncurrent_versions_expiration_limit(self: Arc<Self>, obj: &ObjectOpts) -> Event;
}

#[async_trait::async_trait]
impl Lifecycle for BucketLifecycleConfiguration {
    async fn has_transition(&self) -> bool {
        for rule in self.rules.iter() {
            if rule.transitions.as_ref().is_some_and(|transitions| !transitions.is_empty()) {
                return true;
            }
        }
        false
    }

    fn has_expiry(&self) -> bool {
        for rule in self.rules.iter() {
            if rule.expiration.is_some() || rule.noncurrent_version_expiration.is_some() {
                return true;
            }
        }
        false
    }

    fn has_active_rules(&self, prefix: &str) -> bool {
        if self.rules.is_empty() {
            return false;
        }
        for rule in self.rules.iter() {
            if rule.status.as_str() == ExpirationStatus::DISABLED {
                continue;
            }

            let rule_prefix = lifecycle_rule_prefix(rule).unwrap_or("");
            if !prefix.is_empty()
                && !rule_prefix.is_empty()
                && !prefix.starts_with(rule_prefix)
                && !rule_prefix.starts_with(prefix)
            {
                continue;
            }

            if let Some(rule_noncurrent_version_expiration) = &rule.noncurrent_version_expiration {
                if let Some(noncurrent_days) = rule_noncurrent_version_expiration.noncurrent_days
                    && noncurrent_days >= 0
                {
                    return true;
                }
                if let Some(newer_noncurrent_versions) = rule_noncurrent_version_expiration.newer_noncurrent_versions
                    && newer_noncurrent_versions > 0
                {
                    return true;
                }
            }
            if rule
                .noncurrent_version_transitions
                .as_ref()
                .is_some_and(|transitions| !transitions.is_empty())
            {
                return true;
            }
            if let Some(rule_expiration) = &rule.expiration {
                if let Some(date1) = rule_expiration.date.clone()
                    && OffsetDateTime::from(date1).unix_timestamp() < OffsetDateTime::now_utc().unix_timestamp()
                {
                    return true;
                }
                if rule_expiration.date.is_some() {
                    return true;
                }
                if rule_expiration.days.is_some() {
                    return true;
                }
                if let Some(expired_object_delete_marker) = rule_expiration.expired_object_delete_marker
                    && expired_object_delete_marker
                {
                    return true;
                }
            }
            if let Some(rule_transition) = rule.transitions.as_ref().and_then(|transitions| transitions.first()) {
                if let Some(date1) = rule_transition.date.clone()
                    && OffsetDateTime::from(date1).unix_timestamp() < OffsetDateTime::now_utc().unix_timestamp()
                {
                    return true;
                }
                return true;
            }
        }
        false
    }

    async fn validate(&self, lr: &ObjectLockConfiguration) -> Result<(), std::io::Error> {
        if self.rules.len() > 1000 {
            return Err(std::io::Error::other(ERR_LIFECYCLE_TOO_MANY_RULES));
        }
        if self.rules.is_empty() {
            return Err(std::io::Error::other(ERR_LIFECYCLE_NO_RULE));
        }

        for r in &self.rules {
            if r.status != ExpirationStatus::from_static(ExpirationStatus::ENABLED)
                && r.status != ExpirationStatus::from_static(ExpirationStatus::DISABLED)
            {
                return Err(std::io::Error::other(ERR_LIFECYCLE_INVALID_RULE_STATUS));
            }

            if let Some(expiration) = &r.expiration {
                if let Some(expiration_date) = &expiration.date {
                    let date = OffsetDateTime::from(expiration_date.clone());
                    if date.hour() != 0 || date.minute() != 0 || date.second() != 0 || date.nanosecond() != 0 {
                        return Err(std::io::Error::other(ERR_LIFECYCLE_INVALID_EXPIRATION_DATE_NOT_MIDNIGHT));
                    }
                }
                // S3 requires Expiration.Days to be a positive integer (>= 1). AWS and the
                // ceph s3-tests `test_lifecycle_expiration_days0` case reject Days == 0 with
                // InvalidArgument; only Date-based rules may omit Days entirely.
                if let Some(days) = expiration.days
                    && days < 1
                {
                    return Err(std::io::Error::other(ERR_LIFECYCLE_INVALID_EXPIRATION_DAYS));
                }
            }
            // S3 requires NoncurrentVersionExpiration.NoncurrentDays to be a positive
            // integer (>= 1); AWS rejects 0 with InvalidArgument.
            if let Some(noncurrent_version_expiration) = &r.noncurrent_version_expiration
                && let Some(noncurrent_days) = noncurrent_version_expiration.noncurrent_days
                && noncurrent_days < 1
            {
                return Err(std::io::Error::other(ERR_LIFECYCLE_INVALID_NONCURRENT_EXPIRATION_DAYS));
            }
            // S3 requires AbortIncompleteMultipartUpload.DaysAfterInitiation to be present
            // and a positive integer (>= 1); AWS rejects 0 (and a missing value) with
            // InvalidArgument.
            if let Some(abort_incomplete_multipart_upload) = &r.abort_incomplete_multipart_upload {
                match abort_incomplete_multipart_upload.days_after_initiation {
                    Some(days) if days >= 1 => {}
                    _ => return Err(std::io::Error::other(ERR_LIFECYCLE_INVALID_ABORT_INCOMPLETE_MPU_DAYS)),
                }
            }
            if let Some(transitions) = &r.transitions {
                for transition in transitions {
                    TransitionOps::validate(transition)?;
                }
            }
            if let Some(noncurrent_transitions) = &r.noncurrent_version_transitions {
                for transition in noncurrent_transitions {
                    NoncurrentVersionTransitionOps::validate(transition)?;
                }
            }
            if let Some(id) = &r.id
                && id.len() > 255
            {
                return Err(std::io::Error::other(ERR_LIFECYCLE_INVALID_RULE_ID_TOO_LONG));
            }
            r.validate()?;
            if let Some(object_lock_enabled) = lr.object_lock_enabled.as_ref()
                && object_lock_enabled.as_str() == ObjectLockEnabled::ENABLED
            {
                if let Some(expiration) = r.expiration.as_ref() {
                    // Object Lock + ExpiredObjectAllVersions conflict (MinIO extension)
                    if expiration.expired_object_all_versions.is_some_and(|v| v) {
                        return Err(std::io::Error::other(ERR_LIFECYCLE_BUCKET_LOCKED));
                    }
                }
                // Object Lock + DelMarkerExpiration conflict
                if r.del_marker_expiration.is_some() {
                    return Err(std::io::Error::other(ERR_LIFECYCLE_BUCKET_LOCKED));
                }
            }
        }
        for (i, _) in self.rules.iter().enumerate() {
            if i == self.rules.len() - 1 {
                break;
            }
            let other_rules = &self.rules[i + 1..];
            for other_rule in other_rules {
                if let (Some(id1), Some(id2)) = (&self.rules[i].id, &other_rule.id)
                    && id1 == id2
                {
                    return Err(std::io::Error::other(ERR_LIFECYCLE_DUPLICATE_ID));
                }
            }
        }
        Ok(())
    }

    async fn filter_rules(&self, obj: &ObjectOpts) -> Option<Vec<LifecycleRule>> {
        if obj.name.is_empty() {
            return None;
        }
        let mut rules = Vec::<LifecycleRule>::new();
        for rule in self.rules.iter() {
            if rule.status.as_str() == ExpirationStatus::DISABLED {
                continue;
            }
            if let Some(rule_prefix) = lifecycle_rule_prefix(rule)
                && !obj.name.starts_with(rule_prefix)
            {
                continue;
            }
            if let Some(filter) = rule.filter.as_ref() {
                if !<LifecycleRuleFilter as Filter>::test_tags(filter, &obj.user_tags) {
                    continue;
                }
                if !obj.delete_marker
                    && !<LifecycleRuleFilter as Filter>::by_size(filter, i64::try_from(obj.size).unwrap_or(i64::MAX))
                {
                    continue;
                }
            }
            rules.push(rule.clone());
        }
        Some(rules)
    }

    async fn eval(&self, obj: &ObjectOpts) -> Event {
        self.eval_inner(obj, OffsetDateTime::now_utc(), 0).await
    }

    async fn predict_expiration(&self, obj: &ObjectOpts) -> Event {
        let mod_time = match obj.mod_time {
            Some(time) => {
                if time.unix_timestamp() == 0 {
                    return Event::default();
                }
                time
            }
            None => return Event::default(),
        };

        if obj.delete_marker || !(obj.is_latest || obj.version_id.is_none_or(|v| v.is_nil())) {
            return Event::default();
        }

        let Some(lc_rules) = self.filter_rules(obj).await else {
            return Event::default();
        };

        let mut event: Option<Event> = None;
        for rule in lc_rules {
            let Some(expiration) = rule.expiration else {
                continue;
            };
            if expiration.expired_object_delete_marker.is_some_and(|v| v) {
                continue;
            }
            let Some(due) = (match (&expiration.date, expiration.days) {
                (Some(date), _) => Some(OffsetDateTime::from(date.clone())),
                (None, Some(days)) => Some(expected_expiry_time(mod_time, days)),
                _ => None,
            }) else {
                continue;
            };

            let predicted = Event {
                action: IlmAction::DeleteAction,
                rule_id: rule.id.clone().unwrap_or_default(),
                due: Some(due),
                noncurrent_days: 0,
                newer_noncurrent_versions: 0,
                storage_class: "".into(),
            };
            let predicted_due = predicted.due.unwrap_or(OffsetDateTime::UNIX_EPOCH).unix_timestamp();
            let should_replace = event
                .as_ref()
                .is_none_or(|e| predicted_due < e.due.unwrap_or(OffsetDateTime::UNIX_EPOCH).unix_timestamp());
            if should_replace {
                event = Some(predicted);
            }
        }

        event.unwrap_or_default()
    }

    async fn eval_inner(&self, obj: &ObjectOpts, now: OffsetDateTime, newer_noncurrent_versions: usize) -> Event {
        let mut events = Vec::<Event>::new();
        debug!(
            "eval_inner: object={}, mod_time={:?}, successor_mod_time={:?}, now={:?}, is_latest={}, delete_marker={}",
            obj.name, obj.mod_time, obj.successor_mod_time, now, obj.is_latest, obj.delete_marker
        );

        // Gracefully handle missing mod_time instead of panicking
        let mod_time = match obj.mod_time {
            Some(t) => t,
            None => {
                debug!("eval_inner: mod_time is None for object={}, returning default event", obj.name);
                return Event::default();
            }
        };

        if mod_time.unix_timestamp() == 0 {
            debug!("eval_inner: mod_time is 0, returning default event");
            return Event::default();
        }

        if let Some(restore_expires) = obj.restore_expires
            && restore_expires.unix_timestamp() != 0
            && now.unix_timestamp() > restore_expires.unix_timestamp()
        {
            let mut action = IlmAction::DeleteRestoredAction;
            if !obj.is_latest {
                action = IlmAction::DeleteRestoredVersionAction;
            }

            events.push(Event {
                action,
                due: Some(now),
                rule_id: "".into(),
                noncurrent_days: 0,
                newer_noncurrent_versions: 0,
                storage_class: "".into(),
            });
        }

        if let Some(ref lc_rules) = self.filter_rules(obj).await {
            for rule in lc_rules.iter() {
                if obj.is_latest && obj.expired_object_deletemarker() {
                    if let Some(expiration) = rule.expiration.as_ref()
                        && expiration.expired_object_delete_marker.is_some_and(|v| v)
                    {
                        // Preserve explicit date/days scheduling when configured.
                        // If only ExpiredObjectDeleteMarker=true is set, delete immediately.
                        let due = expiration.next_due(obj).unwrap_or(now);
                        if now.unix_timestamp() >= due.unix_timestamp() {
                            events.push(Event {
                                action: IlmAction::DeleteVersionAction,
                                rule_id: rule.id.clone().unwrap_or_default(),
                                due: Some(due),
                                noncurrent_days: 0,
                                newer_noncurrent_versions: 0,
                                storage_class: "".into(),
                            });
                            // Stop after scheduling an expired delete-marker event.
                            break;
                        }
                    }
                    // DelMarkerExpiration: expire delete marker after N days from mod_time
                    if obj.delete_marker
                        && let Some(ref dme) = rule.del_marker_expiration
                        && let Some(days) = dme.days
                        && days > 0
                    {
                        let due = expected_expiry_time(mod_time, days);
                        if now.unix_timestamp() >= due.unix_timestamp() {
                            events.push(Event {
                                action: IlmAction::DelMarkerDeleteAllVersionsAction,
                                rule_id: rule.id.clone().unwrap_or_default(),
                                due: Some(due),
                                noncurrent_days: 0,
                                newer_noncurrent_versions: 0,
                                storage_class: "".into(),
                            });
                        }
                        continue;
                    }
                }

                if !obj.is_latest
                    && let Some(ref noncurrent_version_expiration) = rule.noncurrent_version_expiration
                    && let Some(retain_newer_noncurrent_versions) = noncurrent_version_expiration.newer_noncurrent_versions
                    && newer_noncurrent_versions < usize::try_from(retain_newer_noncurrent_versions).unwrap_or(usize::MAX)
                {
                    continue;
                }

                if !obj.is_latest
                    && let Some(ref noncurrent_version_expiration) = rule.noncurrent_version_expiration
                    && let Some(noncurrent_days) = noncurrent_version_expiration.noncurrent_days
                    && let Some(successor_mod_time) = obj.successor_mod_time
                {
                    let expected_expiry = expected_expiry_time(successor_mod_time, noncurrent_days);
                    if now.unix_timestamp() >= expected_expiry.unix_timestamp() {
                        events.push(Event {
                            action: IlmAction::DeleteVersionAction,
                            rule_id: rule.id.clone().unwrap_or_default(),
                            due: Some(expected_expiry),
                            noncurrent_days: 0,
                            newer_noncurrent_versions: 0,
                            storage_class: "".into(),
                        });
                    }
                }

                if !obj.is_latest
                    && let Some(noncurrent_version_transition) = rule
                        .noncurrent_version_transitions
                        .as_ref()
                        .and_then(|transitions| transitions.first())
                    && let Some(storage_class) = noncurrent_version_transition.storage_class.as_ref()
                    && !storage_class.as_str().is_empty()
                    && !obj.delete_marker
                    && obj.transition_status != TRANSITION_COMPLETE
                {
                    let due = noncurrent_version_transition.next_due(obj);
                    if let Some(due0) = due
                        && (now.unix_timestamp() == 0 || now.unix_timestamp() > due0.unix_timestamp())
                    {
                        events.push(Event {
                            action: IlmAction::TransitionVersionAction,
                            rule_id: rule.id.clone().unwrap_or_default(),
                            due,
                            storage_class: storage_class.as_str().to_string(),
                            ..Default::default()
                        });
                    }
                }

                debug!(
                    "eval_inner: checking expiration condition - is_latest={}, delete_marker={}, version_id={:?}, condition_met={}",
                    obj.is_latest,
                    obj.delete_marker,
                    obj.version_id,
                    (obj.is_latest || obj.version_id.is_none_or(|v| v.is_nil())) && !obj.delete_marker
                );
                // Allow expiration for latest objects OR non-versioned objects (empty version_id)
                if (obj.is_latest || obj.version_id.is_none_or(|v| v.is_nil())) && !obj.delete_marker {
                    debug!("eval_inner: entering expiration check");
                    if let Some(ref expiration) = rule.expiration {
                        if let Some(ref date) = expiration.date {
                            let date0 = OffsetDateTime::from(date.clone());
                            if date0.unix_timestamp() != 0 && (now.unix_timestamp() >= date0.unix_timestamp()) {
                                debug!("eval_inner: expiration by date - date0={:?}", date0);
                                events.push(Event {
                                    action: IlmAction::DeleteAction,
                                    rule_id: rule.id.clone().unwrap_or_default(),
                                    due: Some(date0),
                                    noncurrent_days: 0,
                                    newer_noncurrent_versions: 0,
                                    storage_class: "".into(),
                                });
                            }
                        } else if let Some(days) = expiration.days {
                            let expected_expiry: OffsetDateTime = expected_expiry_time(mod_time, days);
                            debug!(
                                "eval_inner: expiration check - days={}, obj_time={:?}, expiry_time={:?}, now={:?}, should_expire={}",
                                days,
                                mod_time,
                                expected_expiry,
                                now,
                                now.unix_timestamp() > expected_expiry.unix_timestamp()
                            );
                            if now.unix_timestamp() >= expected_expiry.unix_timestamp() {
                                debug!("eval_inner: object should expire, adding DeleteAction");
                                let mut event = Event {
                                    action: IlmAction::DeleteAction,
                                    rule_id: rule.id.clone().unwrap_or_default(),
                                    due: Some(expected_expiry),
                                    noncurrent_days: 0,
                                    newer_noncurrent_versions: 0,
                                    storage_class: "".into(),
                                };
                                // MinIO extension: ExpiredObjectAllVersions deletes all versions
                                if rule
                                    .expiration
                                    .as_ref()
                                    .and_then(|e| e.expired_object_all_versions)
                                    .unwrap_or(false)
                                {
                                    event.action = IlmAction::DeleteAllVersionsAction;
                                }
                                events.push(event);
                            }
                        } else {
                            debug!("eval_inner: expiration.days is None");
                        }
                    } else {
                        debug!("eval_inner: rule.expiration is None");
                    }

                    if obj.transition_status != TRANSITION_COMPLETE
                        && let Some(transition) = rule.transitions.as_ref().and_then(|transitions| transitions.first())
                        && let Some(storage_class) = transition.storage_class.as_ref()
                        && !storage_class.as_str().is_empty()
                    {
                        let due = transition.next_due(obj);
                        if let Some(due0) = due
                            && (now.unix_timestamp() == 0 || now.unix_timestamp() > due0.unix_timestamp())
                        {
                            events.push(Event {
                                action: IlmAction::TransitionAction,
                                rule_id: rule.id.clone().unwrap_or_default(),
                                due,
                                storage_class: storage_class.as_str().to_string(),
                                noncurrent_days: 0,
                                newer_noncurrent_versions: 0,
                            });
                        }
                    }
                }
            }
        }

        if !events.is_empty() {
            // Select the winning event using a strict total order (MinIO semantics):
            // the earliest `due` wins, and ties break toward delete-type actions. A
            // missing `due` is treated as UNIX_EPOCH. This replaces a hand-written
            // `sort_by` comparator that was not a strict weak ordering (it could return
            // `Ordering::Less` for both `(a, b)` and `(b, a)`), which panics on the
            // repository toolchain and did not deterministically pick the earliest event.
            let event = events
                .iter()
                .min_by_key(|event| {
                    (
                        event.due.unwrap_or(OffsetDateTime::UNIX_EPOCH).unix_timestamp(),
                        ilm_action_priority_rank(&event.action),
                    )
                })
                .cloned()
                .unwrap_or_default();
            return event;
        }

        Event::default()
    }

    async fn noncurrent_versions_expiration_limit(self: Arc<Self>, obj: &ObjectOpts) -> Event {
        if let Some(filter_rules) = self.filter_rules(obj).await {
            for rule in filter_rules.iter() {
                if let Some(ref noncurrent_version_expiration) = rule.noncurrent_version_expiration {
                    return if let Some(newer_noncurrent_versions) = noncurrent_version_expiration.newer_noncurrent_versions {
                        if newer_noncurrent_versions == 0 {
                            continue;
                        }
                        Event {
                            action: IlmAction::DeleteVersionAction,
                            rule_id: rule.id.clone().unwrap_or_default(),
                            noncurrent_days: u32::try_from(noncurrent_version_expiration.noncurrent_days.unwrap_or(0))
                                .unwrap_or(u32::MAX),
                            newer_noncurrent_versions: usize::try_from(newer_noncurrent_versions).unwrap_or(usize::MAX),
                            due: Some(OffsetDateTime::UNIX_EPOCH),
                            storage_class: "".into(),
                        }
                    } else {
                        Event {
                            action: IlmAction::DeleteVersionAction,
                            rule_id: rule.id.clone().unwrap_or_default(),
                            noncurrent_days: u32::try_from(noncurrent_version_expiration.noncurrent_days.unwrap_or(0))
                                .unwrap_or(u32::MAX),
                            newer_noncurrent_versions: 0,
                            due: Some(OffsetDateTime::UNIX_EPOCH),
                            storage_class: "".into(),
                        }
                    };
                }
            }
        }
        Event::default()
    }
}

#[async_trait::async_trait]
pub trait LifecycleCalculate {
    fn next_due(&self, obj: &ObjectOpts) -> Option<OffsetDateTime>;
}

#[async_trait::async_trait]
impl LifecycleCalculate for LifecycleExpiration {
    fn next_due(&self, obj: &ObjectOpts) -> Option<OffsetDateTime> {
        if !obj.is_latest || !obj.delete_marker {
            return None;
        }
        // Check date first (date-based expiration takes priority over days).
        // A zero unix timestamp means "not set" (default value) and is skipped.
        if let Some(ref date) = self.date {
            let expiry_date = OffsetDateTime::from(date.clone());
            if expiry_date.unix_timestamp() != 0 {
                return Some(expiry_date);
            }
        }
        match self.days {
            Some(days) => obj.mod_time.map(|mod_time| expected_expiry_time(mod_time, days)),
            None => None,
        }
    }
}

#[async_trait::async_trait]
impl LifecycleCalculate for NoncurrentVersionTransition {
    fn next_due(&self, obj: &ObjectOpts) -> Option<OffsetDateTime> {
        if obj.is_latest || self.storage_class.is_none() {
            return None;
        }
        match self.noncurrent_days {
            Some(noncurrent_days) => {
                if let Some(successor_mod_time) = obj.successor_mod_time {
                    Some(expected_expiry_time(successor_mod_time, noncurrent_days))
                } else {
                    Some(expected_expiry_time(OffsetDateTime::now_utc(), noncurrent_days))
                }
            }
            None => obj.successor_mod_time,
        }
    }
}

#[async_trait::async_trait]
impl LifecycleCalculate for Transition {
    fn next_due(&self, obj: &ObjectOpts) -> Option<OffsetDateTime> {
        if !obj.is_latest {
            return None;
        }

        if let Some(date) = self.date.clone() {
            return Some(date.into());
        }

        match (self.days, obj.mod_time) {
            (Some(days), Some(mod_time)) => Some(expected_expiry_time(mod_time, days)),
            (Some(_), None) => None,
            (None, mod_time) => mod_time,
        }
    }
}

pub fn expected_expiry_time(mod_time: OffsetDateTime, days: i32) -> OffsetDateTime {
    if days == 0 {
        debug!(
            event = EVENT_LIFECYCLE_EXPIRY_COMPUTED,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_LIFECYCLE,
            days,
            result = "unix_epoch",
            "Computed immediate lifecycle expiry time"
        );
        return OffsetDateTime::UNIX_EPOCH; // Return epoch time to ensure immediate expiry
    }
    // One "day" is normally 86400 seconds; RUSTFS_ILM_DEBUG_DAY_SECS (test/debug
    // only) can shrink it so Days-based rules become testable in seconds. Unset =>
    // ilm_day_secs() == 86400, byte-identical to `Duration::days(days)`.
    let offset_secs = i64::from(days).saturating_mul(i64::from(ilm_day_secs()));
    let t = mod_time
        .to_offset(offset!(-0:00:00))
        .saturating_add(Duration::seconds(offset_secs));

    // Round up to the next processing boundary per S3-compatible Days semantics.
    // Canonical key: RUSTFS_ILM_PROCESS_TIME; deprecated alias: _RUSTFS_ILM_PROCESS_TIME.
    // When RUSTFS_ILM_PROCESS_TIME is unset the boundary defaults to the (possibly
    // debug-accelerated) day length, so RUSTFS_ILM_DEBUG_DAY_SECS scales rounding too.
    // TODO(GA): Remove ENV_ILM_PROCESS_TIME_DEPRECATED compatibility during GA release.
    let process_interval_secs = ilm_process_interval_secs();

    let boundary_nanos = i128::from(process_interval_secs) * 1_000_000_000;
    let timestamp_nanos = t.unix_timestamp_nanos();
    let remainder = timestamp_nanos.rem_euclid(boundary_nanos);
    let rounded_nanos = if remainder == 0 {
        timestamp_nanos
    } else {
        timestamp_nanos + (boundary_nanos - remainder)
    };
    OffsetDateTime::from_unix_timestamp_nanos(rounded_nanos).unwrap_or(t)
}

fn ilm_process_interval_secs() -> u32 {
    // An explicit RUSTFS_ILM_PROCESS_TIME (or its deprecated alias) always wins.
    // Otherwise fall back to the lifecycle day length so the debug day-length
    // override rescales the rounding boundary consistently with the deadline.
    std::env::var(ENV_ILM_PROCESS_TIME)
        .ok()
        .or_else(|| std::env::var(ENV_ILM_PROCESS_TIME_DEPRECATED).ok())
        .and_then(|value| value.parse::<i32>().ok())
        .filter(|value| *value > 0)
        .and_then(|value| u32::try_from(value).ok())
        .unwrap_or_else(ilm_day_secs)
}

/// Number of seconds treated as one lifecycle "day".
///
/// **TEST/DEBUG ONLY override.** Returns [`DEFAULT_ILM_DAY_SECS`] (86400) unless
/// the `RUSTFS_ILM_DEBUG_DAY_SECS` environment variable is set to a positive
/// integer, modeled on Ceph RGW's `rgw_lc_debug_interval`. When set, one
/// Days-based lifecycle "day" is compressed to that many seconds, making
/// expiration / transition / noncurrent Days rules exercisable in seconds.
///
/// A missing / zero / non-numeric value falls back to 86400, so with the
/// variable unset the behavior is byte-for-byte identical to production.
/// Enabling it emits a `WARN` because it accelerates data deletion and must
/// never be set in a production deployment. Only relative `Days` deadlines are
/// rescaled; absolute `Date`-based rules are unaffected.
///
/// In production the value is parsed once (via `OnceLock`) so there is no
/// per-object environment read on the scanner evaluation path. Under `#[cfg(test)]`
/// the environment is re-read on every call so `temp_env`-style tests remain
/// deterministic and cannot poison each other across a shared process.
fn ilm_day_secs() -> u32 {
    #[cfg(test)]
    {
        resolve_ilm_day_secs()
    }
    #[cfg(not(test))]
    {
        use std::sync::OnceLock;
        static CACHE: OnceLock<u32> = OnceLock::new();
        *CACHE.get_or_init(resolve_ilm_day_secs)
    }
}

fn resolve_ilm_day_secs() -> u32 {
    match std::env::var(ENV_ILM_DEBUG_DAY_SECS) {
        Ok(raw) => parse_ilm_day_secs(&raw),
        Err(_) => DEFAULT_ILM_DAY_SECS,
    }
}

/// Parse a `RUSTFS_ILM_DEBUG_DAY_SECS` value. Positive integers activate the
/// override (with a `WARN`); anything else warns and falls back to 86400.
fn parse_ilm_day_secs(raw: &str) -> u32 {
    match raw.trim().parse::<u32>() {
        Ok(secs) if secs > 0 => {
            warn!(
                event = EVENT_LIFECYCLE_DEBUG_DAY_SECS,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                day_secs = secs,
                "RUSTFS_ILM_DEBUG_DAY_SECS is set: lifecycle Days are accelerated to {secs}s/day. \
                 TEST/DEBUG ONLY - this accelerates object deletion and must never be enabled in production."
            );
            secs
        }
        _ => {
            warn!(
                event = EVENT_LIFECYCLE_DEBUG_DAY_SECS,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                raw = raw,
                fallback_day_secs = DEFAULT_ILM_DAY_SECS,
                "Ignoring invalid RUSTFS_ILM_DEBUG_DAY_SECS value; falling back to 86400s/day."
            );
            DEFAULT_ILM_DAY_SECS
        }
    }
}

pub async fn abort_incomplete_multipart_upload_due(
    lc: &BucketLifecycleConfiguration,
    obj: &ObjectOpts,
) -> Option<(OffsetDateTime, String)> {
    let initiated = obj.mod_time?;
    let rules = lc.filter_rules(obj).await?;

    rules
        .into_iter()
        .filter_map(|rule| {
            let days = rule
                .abort_incomplete_multipart_upload
                .as_ref()?
                .days_after_initiation
                .filter(|days| *days >= 0)?;
            Some((expected_expiry_time(initiated, days), rule.id.unwrap_or_default()))
        })
        .min_by_key(|(due, _)| due.unix_timestamp_nanos())
}

#[derive(Debug, Default)]
pub struct ObjectOpts {
    pub name: String,
    pub user_tags: String,
    pub mod_time: Option<OffsetDateTime>,
    pub size: usize,
    pub version_id: Option<Uuid>,
    pub is_latest: bool,
    pub delete_marker: bool,
    pub num_versions: usize,
    pub successor_mod_time: Option<OffsetDateTime>,
    pub transition_status: String,
    pub restore_ongoing: bool,
    pub restore_expires: Option<OffsetDateTime>,
    pub versioned: bool,
    pub version_suspended: bool,
    pub user_defined: HashMap<String, String>,
    pub version_purge_status: VersionPurgeStatusType,
    pub replication_status: ReplicationStatusType,
}

impl ObjectOpts {
    pub fn expired_object_deletemarker(&self) -> bool {
        self.delete_marker && self.is_latest && self.num_versions == 1
    }
}

/// Total-order rank for lifecycle actions used to break `due` ties.
///
/// Delete-type actions rank before every other action so that, when two events
/// share the same `due`, a delete wins (MinIO semantics). The concrete numeric
/// values only matter relative to each other.
fn ilm_action_priority_rank(action: &IlmAction) -> u8 {
    match action {
        IlmAction::DeleteAllVersionsAction
        | IlmAction::DelMarkerDeleteAllVersionsAction
        | IlmAction::DeleteAction
        | IlmAction::DeleteVersionAction => 0,
        _ => 1,
    }
}

#[derive(Debug, Clone)]
pub struct Event {
    pub action: IlmAction,
    pub rule_id: String,
    pub due: Option<OffsetDateTime>,
    pub noncurrent_days: u32,
    pub newer_noncurrent_versions: usize,
    pub storage_class: String,
}

impl Default for Event {
    fn default() -> Self {
        Self {
            action: IlmAction::NoneAction,
            rule_id: "".into(),
            due: Some(OffsetDateTime::UNIX_EPOCH),
            noncurrent_days: 0,
            newer_noncurrent_versions: 0,
            storage_class: "".into(),
        }
    }
}

pub use rustfs_storage_api::ExpirationOptions;

#[derive(Debug, Clone)]
pub struct TransitionOptions {
    pub status: String,
    pub tier: String,
    pub etag: String,
    pub restore_request: RestoreRequest,
    pub restore_expiry: OffsetDateTime,
    pub expire_restored: bool,
}

impl Default for TransitionOptions {
    fn default() -> Self {
        Self {
            status: Default::default(),
            tier: Default::default(),
            etag: Default::default(),
            restore_request: Default::default(),
            restore_expiry: OffsetDateTime::now_utc(),
            expire_restored: Default::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use s3s::dto::{LifecycleRuleFilter, TransitionStorageClass};
    use serial_test::serial;
    use std::sync::Arc;
    use time::macros::datetime;

    fn with_default_ilm_process_time(test: impl FnOnce()) {
        // Also neutralize the debug day-length override so these boundary tests
        // stay hermetic against an ambient RUSTFS_ILM_DEBUG_DAY_SECS.
        temp_env::with_var_unset(ENV_ILM_DEBUG_DAY_SECS, || {
            temp_env::with_var_unset(ENV_ILM_PROCESS_TIME, || {
                temp_env::with_var_unset(ENV_ILM_PROCESS_TIME_DEPRECATED, test);
            });
        });
    }

    #[tokio::test]
    #[serial]
    async fn validate_rejects_zero_expiration_days() {
        // S3 compatibility: Expiration.Days must be a positive integer (>= 1). AWS and
        // the ceph s3-tests `test_lifecycle_expiration_days0` case reject Days == 0 with
        // InvalidArgument. The PutBucketLifecycleConfiguration path maps this io::Error
        // to s3_error!(InvalidArgument) (see execute_put_bucket_lifecycle_configuration).
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: Some(LifecycleExpiration {
                    days: Some(0),
                    ..Default::default()
                }),
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: None,
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
            }],
        };

        let err = lc
            .validate(&ObjectLockConfiguration::default())
            .await
            .expect_err("zero-day expiration should be rejected");

        assert_eq!(err.to_string(), ERR_LIFECYCLE_INVALID_EXPIRATION_DAYS);
    }

    #[tokio::test]
    #[serial]
    async fn validate_rejects_negative_expiration_days() {
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: Some(LifecycleExpiration {
                    days: Some(-1),
                    ..Default::default()
                }),
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: None,
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
            }],
        };

        let err = lc
            .validate(&ObjectLockConfiguration::default())
            .await
            .expect_err("expected validation error");

        assert_eq!(err.to_string(), ERR_LIFECYCLE_INVALID_EXPIRATION_DAYS);
    }

    #[tokio::test]
    #[serial]
    async fn validate_accepts_positive_expiration_days() {
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: Some(LifecycleExpiration {
                    days: Some(30),
                    ..Default::default()
                }),
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: None,
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
            }],
        };

        lc.validate(&ObjectLockConfiguration::default())
            .await
            .expect("expected validation to pass");
    }

    #[tokio::test]
    #[serial]
    async fn validate_accepts_one_day_boundary_values() {
        // Pin the exact >= 1 boundary: a value of 1 is the smallest legal positive
        // integer and must be accepted for every day-count field tightened for S3
        // compatibility. This catches an off-by-one that would reject Days == 1.
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: Some(LifecycleExpiration {
                    days: Some(1),
                    ..Default::default()
                }),
                abort_incomplete_multipart_upload: Some(s3s::dto::AbortIncompleteMultipartUpload {
                    days_after_initiation: Some(1),
                }),
                del_marker_expiration: None,
                filter: None,
                id: Some("one-day-boundary".to_string()),
                noncurrent_version_expiration: Some(s3s::dto::NoncurrentVersionExpiration {
                    noncurrent_days: Some(1),
                    newer_noncurrent_versions: None,
                }),
                noncurrent_version_transitions: None,
                prefix: Some("boundary/".to_string()),
                transitions: None,
            }],
        };

        lc.validate(&ObjectLockConfiguration::default())
            .await
            .expect("one-day boundary values should be accepted");
    }

    #[tokio::test]
    #[serial]
    async fn has_active_rules_accepts_zero_day_expiration() {
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: Some(LifecycleExpiration {
                    days: Some(0),
                    ..Default::default()
                }),
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: Some("zero-day-active".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: Some("test/".to_string()),
                transitions: None,
            }],
        };

        assert!(lc.has_active_rules("test/"));
    }

    #[tokio::test]
    #[serial]
    async fn validate_rejects_zero_noncurrent_expiration_days() {
        // S3 compatibility: NoncurrentVersionExpiration.NoncurrentDays must be a positive
        // integer (>= 1); AWS rejects 0 with InvalidArgument.
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: None,
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: None,
                noncurrent_version_expiration: Some(s3s::dto::NoncurrentVersionExpiration {
                    noncurrent_days: Some(0),
                    newer_noncurrent_versions: None,
                }),
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
            }],
        };

        let err = lc
            .validate(&ObjectLockConfiguration::default())
            .await
            .expect_err("zero-day noncurrent expiration should be rejected");

        assert_eq!(err.to_string(), ERR_LIFECYCLE_INVALID_NONCURRENT_EXPIRATION_DAYS);
    }

    #[tokio::test]
    #[serial]
    async fn validate_rejects_negative_noncurrent_expiration_days() {
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: None,
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: None,
                noncurrent_version_expiration: Some(s3s::dto::NoncurrentVersionExpiration {
                    noncurrent_days: Some(-1),
                    newer_noncurrent_versions: None,
                }),
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
            }],
        };

        let err = lc
            .validate(&ObjectLockConfiguration::default())
            .await
            .expect_err("expected validation error");

        assert_eq!(err.to_string(), ERR_LIFECYCLE_INVALID_NONCURRENT_EXPIRATION_DAYS);
    }

    #[tokio::test]
    #[serial]
    async fn validate_accepts_abort_incomplete_multipart_upload_only_rule() {
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: None,
                abort_incomplete_multipart_upload: Some(s3s::dto::AbortIncompleteMultipartUpload {
                    days_after_initiation: Some(2),
                }),
                del_marker_expiration: None,
                filter: None,
                id: Some("abort-only".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: Some("test/".to_string()),
                transitions: None,
            }],
        };

        lc.validate(&ObjectLockConfiguration::default())
            .await
            .expect("expected validation to pass");
    }

    #[tokio::test]
    #[serial]
    async fn validate_rejects_zero_abort_incomplete_multipart_upload_days() {
        // S3 compatibility: AbortIncompleteMultipartUpload.DaysAfterInitiation must be a
        // positive integer (>= 1); AWS rejects 0 with InvalidArgument.
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: None,
                abort_incomplete_multipart_upload: Some(s3s::dto::AbortIncompleteMultipartUpload {
                    days_after_initiation: Some(0),
                }),
                del_marker_expiration: None,
                filter: None,
                id: Some("abort-zero".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: Some("test/".to_string()),
                transitions: None,
            }],
        };

        let err = lc
            .validate(&ObjectLockConfiguration::default())
            .await
            .expect_err("zero-day abort incomplete multipart upload should be rejected");

        assert_eq!(err.to_string(), ERR_LIFECYCLE_INVALID_ABORT_INCOMPLETE_MPU_DAYS);
    }

    #[tokio::test]
    #[serial]
    async fn validate_rejects_missing_abort_incomplete_multipart_upload_days() {
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: None,
                abort_incomplete_multipart_upload: Some(s3s::dto::AbortIncompleteMultipartUpload {
                    days_after_initiation: None,
                }),
                del_marker_expiration: None,
                filter: None,
                id: Some("abort-missing".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: Some("test/".to_string()),
                transitions: None,
            }],
        };

        let err = lc.validate(&ObjectLockConfiguration::default()).await.unwrap_err();

        assert_eq!(err.to_string(), ERR_LIFECYCLE_INVALID_ABORT_INCOMPLETE_MPU_DAYS);
    }

    #[tokio::test]
    #[serial]
    async fn validate_rejects_negative_abort_incomplete_multipart_upload_days() {
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: None,
                abort_incomplete_multipart_upload: Some(s3s::dto::AbortIncompleteMultipartUpload {
                    days_after_initiation: Some(-1),
                }),
                del_marker_expiration: None,
                filter: None,
                id: Some("abort-negative".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: Some("test/".to_string()),
                transitions: None,
            }],
        };

        let err = lc.validate(&ObjectLockConfiguration::default()).await.unwrap_err();

        assert_eq!(err.to_string(), ERR_LIFECYCLE_INVALID_ABORT_INCOMPLETE_MPU_DAYS);
    }

    #[tokio::test]
    #[serial]
    async fn abort_incomplete_multipart_upload_due_accepts_zero_days() {
        let initiated = OffsetDateTime::from_unix_timestamp(1_000_000).unwrap();
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: None,
                abort_incomplete_multipart_upload: Some(s3s::dto::AbortIncompleteMultipartUpload {
                    days_after_initiation: Some(0),
                }),
                del_marker_expiration: None,
                filter: None,
                id: Some("abort-zero".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: Some("test/".to_string()),
                transitions: None,
            }],
        };
        let opts = ObjectOpts {
            name: "test/object".to_string(),
            mod_time: Some(initiated),
            ..Default::default()
        };

        let (due, rule_id) = abort_incomplete_multipart_upload_due(&lc, &opts)
            .await
            .expect("zero-day abort rule should be due");

        assert_eq!(rule_id, "abort-zero");
        assert_eq!(due, OffsetDateTime::UNIX_EPOCH);
    }

    #[tokio::test]
    #[serial]
    async fn validate_rejects_non_midnight_expiration_date() {
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: Some(LifecycleExpiration {
                    date: Some(OffsetDateTime::from_unix_timestamp(20_000_101).unwrap().into()),
                    ..Default::default()
                }),
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: None,
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
            }],
        };

        let err = lc.validate(&ObjectLockConfiguration::default()).await.unwrap_err();

        assert_eq!(err.to_string(), ERR_LIFECYCLE_INVALID_EXPIRATION_DATE_NOT_MIDNIGHT);
    }

    #[tokio::test]
    #[serial]
    async fn predict_expiration_selects_closest_expiry_for_put_object() {
        let base_time = OffsetDateTime::from_unix_timestamp(1_000_000).unwrap();
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![
                LifecycleRule {
                    status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                    expiration: Some(LifecycleExpiration {
                        days: Some(30),
                        ..Default::default()
                    }),
                    abort_incomplete_multipart_upload: None,
                    del_marker_expiration: None,
                    filter: None,
                    id: Some("rule-days".to_string()),
                    noncurrent_version_expiration: None,
                    noncurrent_version_transitions: None,
                    prefix: None,
                    transitions: None,
                },
                LifecycleRule {
                    status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                    expiration: Some(LifecycleExpiration {
                        date: Some((base_time + Duration::days(1)).into()),
                        ..Default::default()
                    }),
                    abort_incomplete_multipart_upload: None,
                    del_marker_expiration: None,
                    filter: None,
                    id: Some("rule-date".to_string()),
                    noncurrent_version_expiration: None,
                    noncurrent_version_transitions: None,
                    prefix: None,
                    transitions: None,
                },
            ],
        };
        let opts = ObjectOpts {
            name: "obj".to_string(),
            mod_time: Some(base_time),
            is_latest: true,
            version_id: None,
            ..Default::default()
        };

        let event = lc.predict_expiration(&opts).await;
        let expected = base_time + Duration::days(1);

        assert_eq!(event.action, IlmAction::DeleteAction);
        assert_eq!(event.rule_id, "rule-date");
        assert_eq!(event.due, Some(expected));
    }

    #[tokio::test]
    #[serial]
    async fn validate_accepts_multiple_rules_without_ids() {
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![
                LifecycleRule {
                    status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                    expiration: Some(LifecycleExpiration {
                        days: Some(30),
                        ..Default::default()
                    }),
                    abort_incomplete_multipart_upload: None,
                    del_marker_expiration: None,
                    filter: None,
                    id: None,
                    noncurrent_version_expiration: None,
                    noncurrent_version_transitions: None,
                    prefix: None,
                    transitions: None,
                },
                LifecycleRule {
                    status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                    expiration: Some(LifecycleExpiration {
                        days: Some(31),
                        ..Default::default()
                    }),
                    abort_incomplete_multipart_upload: None,
                    del_marker_expiration: None,
                    filter: None,
                    id: None,
                    noncurrent_version_expiration: None,
                    noncurrent_version_transitions: None,
                    prefix: None,
                    transitions: None,
                },
            ],
        };

        lc.validate(&ObjectLockConfiguration::default())
            .await
            .expect("expected validation to pass");
    }

    #[tokio::test]
    #[serial]
    async fn validate_rejects_rule_id_too_long() {
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: Some(LifecycleExpiration {
                    days: Some(30),
                    ..Default::default()
                }),
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: Some("a".repeat(256)),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
            }],
        };

        let err = lc.validate(&ObjectLockConfiguration::default()).await.unwrap_err();

        assert_eq!(err.to_string(), ERR_LIFECYCLE_INVALID_RULE_ID_TOO_LONG);
    }

    #[tokio::test]
    #[serial]
    async fn validate_rejects_duplicate_rule_ids() {
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![
                LifecycleRule {
                    status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                    expiration: Some(LifecycleExpiration {
                        days: Some(1),
                        ..Default::default()
                    }),
                    abort_incomplete_multipart_upload: None,
                    del_marker_expiration: None,
                    filter: None,
                    id: Some("dup-rule".to_string()),
                    noncurrent_version_expiration: None,
                    noncurrent_version_transitions: None,
                    prefix: None,
                    transitions: None,
                },
                LifecycleRule {
                    status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                    expiration: Some(LifecycleExpiration {
                        days: Some(2),
                        ..Default::default()
                    }),
                    abort_incomplete_multipart_upload: None,
                    filter: None,
                    id: Some("dup-rule".to_string()),
                    noncurrent_version_expiration: None,
                    noncurrent_version_transitions: None,
                    prefix: None,
                    transitions: None,
                    del_marker_expiration: None,
                },
            ],
        };

        let err = lc.validate(&ObjectLockConfiguration::default()).await.unwrap_err();
        assert_eq!(err.to_string(), ERR_LIFECYCLE_DUPLICATE_ID);
    }

    #[tokio::test]
    #[serial]
    async fn validate_rejects_transition_without_storage_class() {
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: None,
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: Some("transition-no-storage".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: Some(vec![Transition {
                    days: Some(1),
                    date: None,
                    storage_class: None,
                }]),
            }],
        };

        let err = lc.validate(&ObjectLockConfiguration::default()).await.unwrap_err();

        assert_eq!(err.to_string(), "ERR_XML_NOT_WELL_FORMED");
    }

    #[tokio::test]
    #[serial]
    async fn validate_rejects_transition_without_date_or_days() {
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: None,
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: Some("transition-no-schedule".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: Some(vec![Transition {
                    days: None,
                    date: None,
                    storage_class: Some(TransitionStorageClass::from_static("WARM")),
                }]),
            }],
        };

        let err = lc.validate(&ObjectLockConfiguration::default()).await.unwrap_err();

        assert!(err.to_string().contains("Exactly one of Days"));
    }

    #[tokio::test]
    #[serial]
    async fn validate_rejects_noncurrent_transition_without_days() {
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: None,
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: Some("noncurrent-transition-no-days".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: Some(vec![NoncurrentVersionTransition {
                    noncurrent_days: None,
                    newer_noncurrent_versions: None,
                    storage_class: Some(TransitionStorageClass::from_static("COLD")),
                }]),
                prefix: None,
                transitions: None,
            }],
        };

        let err = lc.validate(&ObjectLockConfiguration::default()).await.unwrap_err();

        assert!(err.to_string().contains("Exactly one of Days"));
    }

    #[tokio::test]
    #[serial]
    async fn empty_transition_vectors_are_not_active_or_due() {
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: None,
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: Some("empty-transition".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: Some(vec![]),
                prefix: None,
                transitions: Some(vec![]),
            }],
        };
        let opts = ObjectOpts {
            name: "obj".to_string(),
            mod_time: Some(OffsetDateTime::from_unix_timestamp(1_000_000).unwrap()),
            is_latest: true,
            ..Default::default()
        };

        assert!(!lc.has_transition().await);
        assert!(!lc.has_active_rules("obj"));
        assert_eq!(lc.eval_inner(&opts, OffsetDateTime::now_utc(), 0).await.action, IlmAction::NoneAction);
    }

    #[tokio::test]
    #[serial]
    async fn eval_inner_expires_latest_object_after_days_due() {
        let base_time = datetime!(2025-01-15 10:30:45 UTC);
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: Some(LifecycleExpiration {
                    days: Some(1),
                    ..Default::default()
                }),
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: Some("expire-days".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
            }],
        };

        let opts = ObjectOpts {
            name: "obj".to_string(),
            mod_time: Some(base_time),
            is_latest: true,
            ..Default::default()
        };
        let event = lc.eval_inner(&opts, datetime!(2025-01-17 00:00:00 UTC), 0).await;

        assert_eq!(event.action, IlmAction::DeleteAction);
        assert_eq!(event.rule_id, "expire-days");
        assert_eq!(event.due, Some(datetime!(2025-01-17 00:00:00 UTC)));
    }

    #[tokio::test]
    #[serial]
    async fn eval_inner_keeps_latest_object_before_days_due() {
        let base_time = OffsetDateTime::from_unix_timestamp(1_000_000).unwrap();
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: Some(LifecycleExpiration {
                    days: Some(2),
                    ..Default::default()
                }),
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: Some("expire-days".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
            }],
        };

        let opts = ObjectOpts {
            name: "obj".to_string(),
            mod_time: Some(base_time),
            is_latest: true,
            ..Default::default()
        };
        let event = lc.eval_inner(&opts, base_time + Duration::hours(12), 0).await;

        assert_eq!(event.action, IlmAction::NoneAction);
    }

    #[tokio::test]
    #[serial]
    async fn eval_inner_transitions_latest_object_after_days_due() {
        let base_time = OffsetDateTime::from_unix_timestamp(1_000_000).unwrap();
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: None,
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: Some("transition-days".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: Some(vec![Transition {
                    days: Some(1),
                    date: None,
                    storage_class: Some(TransitionStorageClass::from_static("COLDTIER44")),
                }]),
            }],
        };

        let opts = ObjectOpts {
            name: "obj".to_string(),
            mod_time: Some(base_time),
            is_latest: true,
            transition_status: "".to_string(),
            ..Default::default()
        };
        let event = lc.eval_inner(&opts, base_time + Duration::days(2), 0).await;

        assert_eq!(event.action, IlmAction::TransitionAction);
        assert_eq!(event.rule_id, "transition-days");
        assert_eq!(event.storage_class, "COLDTIER44");
    }

    #[tokio::test]
    #[serial]
    async fn eval_inner_transitions_latest_object_after_date_due() {
        let base_time = OffsetDateTime::from_unix_timestamp(1_000_000).unwrap();
        let transition_date = base_time - Duration::days(1);
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: None,
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: Some("transition-date".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: Some(vec![Transition {
                    days: None,
                    date: Some(transition_date.into()),
                    storage_class: Some(TransitionStorageClass::from_static("WARM")),
                }]),
            }],
        };

        let opts = ObjectOpts {
            name: "obj".to_string(),
            mod_time: Some(base_time),
            is_latest: true,
            transition_status: "".to_string(),
            ..Default::default()
        };
        let event = lc.eval_inner(&opts, base_time + Duration::days(1), 0).await;

        assert_eq!(event.action, IlmAction::TransitionAction);
        assert_eq!(event.rule_id, "transition-date");
        assert_eq!(event.storage_class, "WARM");
        assert_eq!(event.due, Some(transition_date));
    }

    #[tokio::test]
    #[serial]
    async fn eval_inner_selects_earliest_due_among_multiple_past_due_events() {
        let base_time = OffsetDateTime::from_unix_timestamp(1_000_000).unwrap();
        // Two enabled rules both yield a past-due DeleteAction and a third yields a
        // past-due TransitionAction. The rule with the shortest expiration (earliest
        // `due`) must win deterministically.
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![
                LifecycleRule {
                    status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                    expiration: Some(LifecycleExpiration {
                        days: Some(5),
                        ..Default::default()
                    }),
                    abort_incomplete_multipart_upload: None,
                    del_marker_expiration: None,
                    filter: None,
                    id: Some("delete-late".to_string()),
                    noncurrent_version_expiration: None,
                    noncurrent_version_transitions: None,
                    prefix: None,
                    transitions: None,
                },
                LifecycleRule {
                    status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                    expiration: Some(LifecycleExpiration {
                        days: Some(1),
                        ..Default::default()
                    }),
                    abort_incomplete_multipart_upload: None,
                    del_marker_expiration: None,
                    filter: None,
                    id: Some("delete-early".to_string()),
                    noncurrent_version_expiration: None,
                    noncurrent_version_transitions: None,
                    prefix: None,
                    transitions: None,
                },
                LifecycleRule {
                    status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                    expiration: None,
                    abort_incomplete_multipart_upload: None,
                    del_marker_expiration: None,
                    filter: None,
                    id: Some("transition-mid".to_string()),
                    noncurrent_version_expiration: None,
                    noncurrent_version_transitions: None,
                    prefix: None,
                    transitions: Some(vec![Transition {
                        days: Some(3),
                        date: None,
                        storage_class: Some(TransitionStorageClass::from_static("COLDTIER")),
                    }]),
                },
            ],
        };

        let opts = ObjectOpts {
            name: "obj".to_string(),
            mod_time: Some(base_time),
            is_latest: true,
            transition_status: "".to_string(),
            ..Default::default()
        };
        let event = lc.eval_inner(&opts, base_time + Duration::days(10), 0).await;

        assert_eq!(event.action, IlmAction::DeleteAction);
        assert_eq!(event.rule_id, "delete-early");
    }

    #[tokio::test]
    #[serial]
    async fn eval_inner_does_not_panic_on_many_equal_due_events() {
        let base_time = OffsetDateTime::from_unix_timestamp(1_000_000).unwrap();
        // Many enabled rules that all yield a DeleteAction with an identical `due`.
        // The previous hand-written comparator was not a strict weak ordering and
        // panicked on the repository toolchain; the total-order selection must not
        // panic and must return a deterministic winner (the first rule).
        let rules: Vec<LifecycleRule> = (0..25)
            .map(|i| LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: Some(LifecycleExpiration {
                    days: Some(1),
                    ..Default::default()
                }),
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: Some(format!("rule-{i:02}")),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
            })
            .collect();
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules,
        };

        let opts = ObjectOpts {
            name: "obj".to_string(),
            mod_time: Some(base_time),
            is_latest: true,
            ..Default::default()
        };
        let event = lc.eval_inner(&opts, base_time + Duration::days(10), 0).await;

        assert_eq!(event.action, IlmAction::DeleteAction);
        assert_eq!(event.rule_id, "rule-00");
    }

    #[tokio::test]
    #[serial]
    async fn eval_inner_expires_noncurrent_version_after_due() {
        let base_time = OffsetDateTime::from_unix_timestamp(1_000_000).unwrap();
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: None,
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: Some("noncurrent-expire".to_string()),
                noncurrent_version_expiration: Some(s3s::dto::NoncurrentVersionExpiration {
                    noncurrent_days: Some(1),
                    newer_noncurrent_versions: None,
                }),
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
            }],
        };

        let opts = ObjectOpts {
            name: "obj".to_string(),
            mod_time: Some(base_time),
            successor_mod_time: Some(base_time),
            is_latest: false,
            version_id: Some(Uuid::new_v4()),
            ..Default::default()
        };
        let event = lc.eval_inner(&opts, base_time + Duration::days(2), 0).await;

        assert_eq!(event.action, IlmAction::DeleteVersionAction);
        assert_eq!(event.rule_id, "noncurrent-expire");
        assert_eq!(event.due, Some(expected_expiry_time(base_time, 1)));
    }

    #[tokio::test]
    #[serial]
    async fn eval_inner_expires_noncurrent_version_immediately_when_zero_days() {
        let base_time = OffsetDateTime::from_unix_timestamp(1_000_000).unwrap();
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: None,
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: Some("noncurrent-expire-immediate".to_string()),
                noncurrent_version_expiration: Some(s3s::dto::NoncurrentVersionExpiration {
                    noncurrent_days: Some(0),
                    newer_noncurrent_versions: None,
                }),
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
            }],
        };

        let opts = ObjectOpts {
            name: "obj".to_string(),
            mod_time: Some(base_time),
            successor_mod_time: Some(base_time),
            is_latest: false,
            version_id: Some(Uuid::new_v4()),
            ..Default::default()
        };
        let event = lc.eval_inner(&opts, base_time, 0).await;

        assert_eq!(event.action, IlmAction::DeleteVersionAction);
        assert_eq!(event.rule_id, "noncurrent-expire-immediate");
        assert_eq!(event.due, Some(expected_expiry_time(base_time, 0)));
    }

    #[tokio::test]
    #[serial]
    async fn eval_inner_transitions_noncurrent_version_after_due() {
        let base_time = OffsetDateTime::from_unix_timestamp(1_000_000).unwrap();
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: None,
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: Some("noncurrent-transition".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: Some(vec![NoncurrentVersionTransition {
                    noncurrent_days: Some(1),
                    newer_noncurrent_versions: None,
                    storage_class: Some(TransitionStorageClass::from_static("COLDTIER44")),
                }]),
                prefix: None,
                transitions: None,
            }],
        };

        let opts = ObjectOpts {
            name: "obj".to_string(),
            mod_time: Some(base_time),
            successor_mod_time: Some(base_time),
            is_latest: false,
            transition_status: "".to_string(),
            version_id: Some(Uuid::new_v4()),
            ..Default::default()
        };
        let event = lc.eval_inner(&opts, base_time + Duration::days(2), 0).await;

        assert_eq!(event.action, IlmAction::TransitionVersionAction);
        assert_eq!(event.rule_id, "noncurrent-transition");
        assert_eq!(event.storage_class, "COLDTIER44");
    }

    #[tokio::test]
    #[serial]
    async fn noncurrent_versions_expiration_limit_returns_configured_limits() {
        let lc = Arc::new(BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: None,
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: Some("noncurrent-limit".to_string()),
                noncurrent_version_expiration: Some(s3s::dto::NoncurrentVersionExpiration {
                    noncurrent_days: Some(7),
                    newer_noncurrent_versions: Some(3),
                }),
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
            }],
        });

        let opts = ObjectOpts {
            name: "obj".to_string(),
            mod_time: Some(OffsetDateTime::from_unix_timestamp(1_000_000).unwrap()),
            is_latest: false,
            version_id: Some(Uuid::new_v4()),
            ..Default::default()
        };
        let event = lc.noncurrent_versions_expiration_limit(&opts).await;

        assert_eq!(event.action, IlmAction::DeleteVersionAction);
        assert_eq!(event.rule_id, "noncurrent-limit");
        assert_eq!(event.noncurrent_days, 7);
        assert_eq!(event.newer_noncurrent_versions, 3);
    }

    #[tokio::test]
    #[serial]
    async fn evaluator_honors_newer_noncurrent_versions_retention_count() {
        let base_time = OffsetDateTime::from_unix_timestamp(1_000_000).unwrap();
        let lc = Arc::new(BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: None,
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: Some("retain-two-noncurrent".to_string()),
                noncurrent_version_expiration: Some(s3s::dto::NoncurrentVersionExpiration {
                    noncurrent_days: Some(1),
                    newer_noncurrent_versions: Some(2),
                }),
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
            }],
        });
        let mut objs = vec![ObjectOpts {
            name: "obj".to_string(),
            mod_time: Some(base_time + Duration::days(4)),
            successor_mod_time: None,
            is_latest: true,
            version_id: Some(Uuid::new_v4()),
            num_versions: 5,
            ..Default::default()
        }];
        for days_ago in (0..4).rev() {
            objs.push(ObjectOpts {
                name: "obj".to_string(),
                mod_time: Some(base_time + Duration::days(days_ago)),
                successor_mod_time: Some(base_time + Duration::days(days_ago + 1)),
                is_latest: false,
                version_id: Some(Uuid::new_v4()),
                num_versions: 5,
                ..Default::default()
            });
        }

        let events = crate::evaluator::Evaluator::new(lc)
            .eval(&objs)
            .await
            .expect("version group should evaluate");

        assert_eq!(events[1].action, IlmAction::NoneAction);
        assert_eq!(events[2].action, IlmAction::NoneAction);
        assert_eq!(events[3].action, IlmAction::DeleteVersionAction);
        assert_eq!(events[4].action, IlmAction::DeleteVersionAction);
    }

    #[tokio::test]
    #[serial]
    async fn validate_rejects_invalid_status_case_sensitive() {
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static("enabled"),
                expiration: Some(LifecycleExpiration {
                    days: Some(30),
                    ..Default::default()
                }),
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: None,
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
            }],
        };

        let err = lc.validate(&ObjectLockConfiguration::default()).await.unwrap_err();

        assert_eq!(err.to_string(), ERR_LIFECYCLE_INVALID_RULE_STATUS);
    }

    #[tokio::test]
    #[serial]
    async fn filter_rules_respects_filter_prefix() {
        let filter = LifecycleRuleFilter {
            prefix: Some("prefix".to_string()),
            ..Default::default()
        };
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: Some(LifecycleExpiration {
                    days: Some(30),
                    ..Default::default()
                }),
                abort_incomplete_multipart_upload: None,
                filter: Some(filter),
                id: Some("rule".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
                del_marker_expiration: None,
            }],
        };

        let match_obj = ObjectOpts {
            name: "prefix/file".to_string(),
            mod_time: Some(OffsetDateTime::from_unix_timestamp(1_000_000).unwrap()),
            is_latest: true,
            ..Default::default()
        };
        let matched = lc.filter_rules(&match_obj).await.unwrap();
        assert_eq!(matched.len(), 1);

        let non_match_obj = ObjectOpts {
            name: "other/file".to_string(),
            mod_time: Some(OffsetDateTime::from_unix_timestamp(1_000_000).unwrap()),
            is_latest: true,
            ..Default::default()
        };
        let not_matched = lc.filter_rules(&non_match_obj).await.unwrap();
        assert_eq!(not_matched.len(), 0);
    }

    #[tokio::test]
    #[serial]
    async fn filter_rules_respects_filter_and_prefix() {
        let and = s3s::dto::LifecycleRuleAndOperator {
            prefix: Some("prefix".to_string()),
            ..Default::default()
        };
        let filter = LifecycleRuleFilter {
            and: Some(and),
            ..Default::default()
        };

        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: Some(LifecycleExpiration {
                    days: Some(30),
                    ..Default::default()
                }),
                abort_incomplete_multipart_upload: None,
                filter: Some(filter),
                id: Some("rule-and-prefix".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
                del_marker_expiration: None,
            }],
        };

        let match_obj = ObjectOpts {
            name: "prefix/file".to_string(),
            mod_time: Some(OffsetDateTime::from_unix_timestamp(1_000_000).unwrap()),
            is_latest: true,
            ..Default::default()
        };
        let matched = lc.filter_rules(&match_obj).await.unwrap();
        assert_eq!(matched.len(), 1);

        let non_match_obj = ObjectOpts {
            name: "other/file".to_string(),
            mod_time: Some(OffsetDateTime::from_unix_timestamp(1_000_000).unwrap()),
            is_latest: true,
            ..Default::default()
        };
        let not_matched = lc.filter_rules(&non_match_obj).await.unwrap();
        assert_eq!(not_matched.len(), 0);
    }

    #[tokio::test]
    #[serial]
    async fn filter_rules_respects_filter_tag() {
        let filter = LifecycleRuleFilter {
            tag: Some(s3s::dto::Tag {
                key: Some("env".to_string()),
                value: Some("prod".to_string()),
            }),
            ..Default::default()
        };
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: Some(LifecycleExpiration {
                    days: Some(30),
                    ..Default::default()
                }),
                abort_incomplete_multipart_upload: None,
                filter: Some(filter),
                id: Some("rule-tag".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
                del_marker_expiration: None,
            }],
        };

        let matched = lc
            .filter_rules(&ObjectOpts {
                name: "obj".to_string(),
                user_tags: "env=prod&team=storage".to_string(),
                mod_time: Some(OffsetDateTime::from_unix_timestamp(1_000_000).unwrap()),
                is_latest: true,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(matched.len(), 1);

        let not_matched = lc
            .filter_rules(&ObjectOpts {
                name: "obj".to_string(),
                user_tags: "env=dev&team=storage".to_string(),
                mod_time: Some(OffsetDateTime::from_unix_timestamp(1_000_000).unwrap()),
                is_latest: true,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(not_matched.len(), 0);
    }

    #[tokio::test]
    #[serial]
    async fn filter_rules_respects_filter_and_tags() {
        let filter = LifecycleRuleFilter {
            and: Some(s3s::dto::LifecycleRuleAndOperator {
                tags: Some(vec![
                    s3s::dto::Tag {
                        key: Some("env".to_string()),
                        value: Some("prod".to_string()),
                    },
                    s3s::dto::Tag {
                        key: Some("team".to_string()),
                        value: Some("storage".to_string()),
                    },
                ]),
                ..Default::default()
            }),
            ..Default::default()
        };

        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: Some(LifecycleExpiration {
                    days: Some(30),
                    ..Default::default()
                }),
                abort_incomplete_multipart_upload: None,
                filter: Some(filter),
                id: Some("rule-and-tags".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
                del_marker_expiration: None,
            }],
        };

        let matched = lc
            .filter_rules(&ObjectOpts {
                name: "obj".to_string(),
                user_tags: "env=prod&team=storage".to_string(),
                mod_time: Some(OffsetDateTime::from_unix_timestamp(1_000_000).unwrap()),
                is_latest: true,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(matched.len(), 1);

        let not_matched = lc
            .filter_rules(&ObjectOpts {
                name: "obj".to_string(),
                user_tags: "env=prod&team=platform".to_string(),
                mod_time: Some(OffsetDateTime::from_unix_timestamp(1_000_000).unwrap()),
                is_latest: true,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(not_matched.len(), 0);
    }

    #[tokio::test]
    #[serial]
    async fn expired_object_delete_marker_ignores_marker_with_noncurrent_versions_present() {
        let base_time = OffsetDateTime::from_unix_timestamp(1_000_000).unwrap();
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: Some(LifecycleExpiration {
                    expired_object_delete_marker: Some(true),
                    ..Default::default()
                }),
                abort_incomplete_multipart_upload: None,
                filter: None,
                id: Some("rule-expired-del-marker".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
                del_marker_expiration: None,
            }],
        };

        let opts = ObjectOpts {
            name: "obj".to_string(),
            mod_time: Some(base_time),
            is_latest: true,
            delete_marker: true,
            num_versions: 2,
            version_id: Some(Uuid::new_v4()),
            ..Default::default()
        };

        let now = base_time + Duration::days(2);
        let event = lc.eval_inner(&opts, now, 0).await;
        assert_eq!(event.action, IlmAction::NoneAction);
        assert_eq!(event.due, Some(OffsetDateTime::UNIX_EPOCH));
    }

    #[tokio::test]
    async fn expired_object_delete_marker_ignores_unknown_version_count() {
        let base_time = OffsetDateTime::from_unix_timestamp(1_000_000).unwrap();
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: Some(LifecycleExpiration {
                    expired_object_delete_marker: Some(true),
                    ..Default::default()
                }),
                abort_incomplete_multipart_upload: None,
                filter: None,
                id: Some("rule-expired-del-marker".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
                del_marker_expiration: None,
            }],
        };

        let opts = ObjectOpts {
            name: "obj".to_string(),
            mod_time: Some(base_time),
            is_latest: true,
            delete_marker: true,
            num_versions: 0,
            version_id: Some(Uuid::new_v4()),
            ..Default::default()
        };

        let now = base_time + Duration::days(2);
        let event = lc.eval_inner(&opts, now, 0).await;
        assert_eq!(event.action, IlmAction::NoneAction);
        assert_eq!(event.due, Some(OffsetDateTime::UNIX_EPOCH));
    }

    #[tokio::test]
    #[serial]
    async fn expired_object_delete_marker_deletes_only_delete_marker_immediately() {
        let base_time = OffsetDateTime::from_unix_timestamp(1_000_000).unwrap();
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: Some(LifecycleExpiration {
                    expired_object_delete_marker: Some(true),
                    ..Default::default()
                }),
                abort_incomplete_multipart_upload: None,
                filter: None,
                id: Some("rule-expired-del-marker".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
                del_marker_expiration: None,
            }],
        };

        let opts = ObjectOpts {
            name: "obj".to_string(),
            mod_time: Some(base_time),
            is_latest: true,
            delete_marker: true,
            num_versions: 1,
            version_id: Some(Uuid::new_v4()),
            ..Default::default()
        };

        let now = base_time + Duration::days(2);
        let event = lc.eval_inner(&opts, now, 0).await;

        assert_eq!(event.action, IlmAction::DeleteVersionAction);
        assert_eq!(event.due, Some(now));
    }

    #[tokio::test]
    async fn expired_object_delete_marker_without_date_or_days_deletes_immediately() {
        let base_time = OffsetDateTime::from_unix_timestamp(1_000_000).unwrap();
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: Some(LifecycleExpiration {
                    expired_object_delete_marker: Some(true),
                    ..Default::default()
                }),
                abort_incomplete_multipart_upload: None,
                filter: None,
                id: Some("rule-expired-del-marker-immediate".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
                del_marker_expiration: None,
            }],
        };

        let opts = ObjectOpts {
            name: "obj".to_string(),
            mod_time: Some(base_time),
            is_latest: true,
            delete_marker: true,
            num_versions: 1,
            version_id: Some(Uuid::new_v4()),
            ..Default::default()
        };

        let now = base_time + Duration::days(2);
        let event = lc.eval_inner(&opts, now, 0).await;
        assert_eq!(event.action, IlmAction::DeleteVersionAction);
        assert_eq!(event.due, Some(now));
    }

    #[tokio::test]
    async fn validate_rejects_expired_object_delete_marker_with_date() {
        let future_date = OffsetDateTime::from_unix_timestamp(86_400 * 10).expect("valid midnight UTC test timestamp");
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: Some(LifecycleExpiration {
                    date: Some(future_date.into()),
                    expired_object_delete_marker: Some(true),
                    ..Default::default()
                }),
                abort_incomplete_multipart_upload: None,
                filter: None,
                id: Some("rule-date-del-marker".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
                del_marker_expiration: None,
            }],
        };

        let err = lc.validate(&ObjectLockConfiguration::default()).await.unwrap_err();

        assert_eq!(err.to_string(), ERR_LIFECYCLE_INVALID_EXPIRED_OBJECT_DELETE_MARKER);
    }

    #[tokio::test]
    async fn validate_rejects_expired_object_delete_marker_with_days() {
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: Some(LifecycleExpiration {
                    days: Some(1),
                    expired_object_delete_marker: Some(true),
                    ..Default::default()
                }),
                abort_incomplete_multipart_upload: None,
                filter: None,
                id: Some("rule-days-del-marker".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
                del_marker_expiration: None,
            }],
        };

        let err = lc.validate(&ObjectLockConfiguration::default()).await.unwrap_err();

        assert_eq!(err.to_string(), ERR_LIFECYCLE_INVALID_EXPIRED_OBJECT_DELETE_MARKER);
    }

    #[tokio::test]
    async fn validate_rejects_expired_object_delete_marker_with_tag_filter() {
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: Some(LifecycleExpiration {
                    expired_object_delete_marker: Some(true),
                    ..Default::default()
                }),
                abort_incomplete_multipart_upload: None,
                filter: Some(LifecycleRuleFilter {
                    tag: Some(s3s::dto::Tag {
                        key: Some("env".to_string()),
                        value: Some("prod".to_string()),
                    }),
                    ..Default::default()
                }),
                id: Some("rule-tag-del-marker".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
                del_marker_expiration: None,
            }],
        };

        let err = lc.validate(&ObjectLockConfiguration::default()).await.unwrap_err();

        assert_eq!(err.to_string(), ERR_LIFECYCLE_EXPIRED_OBJECT_DELETE_MARKER_WITH_TAGS);
    }

    // --- TASK-002 tests: Object Lock + ExpiredObjectDeleteMarker compatibility ---

    #[tokio::test]
    #[serial]
    async fn validate_allows_expired_object_delete_marker_on_locked_bucket() {
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: Some(LifecycleExpiration {
                    expired_object_delete_marker: Some(true),
                    ..Default::default()
                }),
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: Some("test-rule".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
            }],
        };

        let locked_config = ObjectLockConfiguration {
            object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
            ..Default::default()
        };

        lc.validate(&locked_config)
            .await
            .expect("expected validation to pass for ExpiredObjectDeleteMarker on locked bucket");
    }

    #[tokio::test]
    #[serial]
    async fn validate_allows_expired_object_delete_marker_on_unlocked_bucket() {
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: Some(LifecycleExpiration {
                    expired_object_delete_marker: Some(true),
                    ..Default::default()
                }),
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: Some("test-rule".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
            }],
        };

        // Default ObjectLockConfiguration (no lock enabled) should pass
        lc.validate(&ObjectLockConfiguration::default())
            .await
            .expect("expected validation to pass on unlocked bucket");
    }

    #[tokio::test]
    #[serial]
    async fn validate_allows_non_delete_marker_expiration_on_locked_bucket() {
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: Some(LifecycleExpiration {
                    days: Some(30),
                    ..Default::default()
                }),
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: Some("test-rule".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
            }],
        };

        let locked_config = ObjectLockConfiguration {
            object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
            ..Default::default()
        };

        // Days-based expiration (not DeleteMarker) should be allowed on locked bucket
        lc.validate(&locked_config)
            .await
            .expect("expected days-based expiration to pass on locked bucket");
    }

    #[tokio::test]
    #[serial]
    async fn validate_rejects_del_marker_expiration_on_locked_bucket() {
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: Some(LifecycleExpiration {
                    days: Some(30),
                    ..Default::default()
                }),
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: Some(s3s::dto::DelMarkerExpiration { days: Some(1) }),
                filter: None,
                id: Some("test-rule".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
            }],
        };

        let locked_config = ObjectLockConfiguration {
            object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
            ..Default::default()
        };

        let err = lc.validate(&locked_config).await.unwrap_err();
        assert_eq!(err.to_string(), ERR_LIFECYCLE_BUCKET_LOCKED);
    }

    #[tokio::test]
    #[serial]
    async fn validate_rejects_zero_day_del_marker_expiration_on_locked_bucket() {
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: Some(LifecycleExpiration {
                    days: Some(30),
                    ..Default::default()
                }),
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: Some(s3s::dto::DelMarkerExpiration { days: Some(0) }),
                filter: None,
                id: Some("test-rule".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
            }],
        };

        let locked_config = ObjectLockConfiguration {
            object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
            ..Default::default()
        };

        let err = lc.validate(&locked_config).await.unwrap_err();
        assert_eq!(err.to_string(), ERR_LIFECYCLE_BUCKET_LOCKED);
    }

    // --- TASK-003 tests: Round up to next UTC processing boundary ---

    #[test]
    #[serial]
    fn expected_expiry_time_rounds_up_to_next_midnight_utc() {
        with_default_ilm_process_time(|| {
            // Object created at 2025-01-15T10:30:45Z, expire in 30 days
            let mod_time = datetime!(2025-01-15 10:30:45 UTC);
            let result = expected_expiry_time(mod_time, 30);

            // Should round up to the next midnight: 2025-02-15T00:00:00Z
            assert_eq!(result.hour(), 0);
            assert_eq!(result.minute(), 0);
            assert_eq!(result.second(), 0);
            assert_eq!(result, datetime!(2025-02-15 00:00:00 UTC));
        });
    }

    #[test]
    #[serial]
    fn expected_expiry_time_immediate_expiry_returns_epoch() {
        with_default_ilm_process_time(|| {
            let mod_time = datetime!(2025-06-01 12:00:00 UTC);
            let result = expected_expiry_time(mod_time, 0);
            assert_eq!(result, OffsetDateTime::UNIX_EPOCH);
        });
    }

    #[test]
    #[serial]
    fn expected_expiry_time_preserves_exact_midnight_boundary() {
        with_default_ilm_process_time(|| {
            let mod_time = datetime!(2025-03-01 00:00:00 UTC);
            let result = expected_expiry_time(mod_time, 1);
            assert_eq!(result, datetime!(2025-03-02 00:00:00 UTC));
        });
    }

    #[test]
    #[serial]
    fn expected_expiry_time_rounds_end_of_day_to_following_midnight() {
        with_default_ilm_process_time(|| {
            let mod_time = datetime!(2025-06-15 23:59:59 UTC);
            let result = expected_expiry_time(mod_time, 1);
            assert_eq!(result, datetime!(2025-06-17 00:00:00 UTC));
        });
    }

    #[test]
    #[serial]
    fn expected_expiry_time_uses_canonical_process_time_boundary() {
        let mod_time = datetime!(2025-01-15 10:30:45 UTC);

        temp_env::with_var(ENV_ILM_PROCESS_TIME, Some("3600"), || {
            temp_env::with_var_unset(ENV_ILM_PROCESS_TIME_DEPRECATED, || {
                let result = expected_expiry_time(mod_time, 1);
                assert_eq!(result, datetime!(2025-01-16 11:00:00 UTC));
            });
        });
    }

    #[test]
    #[serial]
    fn expected_expiry_time_uses_deprecated_process_time_alias() {
        let mod_time = datetime!(2025-01-15 10:30:45 UTC);

        temp_env::with_var_unset(ENV_ILM_PROCESS_TIME, || {
            temp_env::with_var(ENV_ILM_PROCESS_TIME_DEPRECATED, Some("3600"), || {
                let result = expected_expiry_time(mod_time, 1);
                assert_eq!(result, datetime!(2025-01-16 11:00:00 UTC));
            });
        });
    }

    #[test]
    #[serial]
    fn expected_expiry_time_uses_default_boundary_when_process_time_is_zero_or_invalid() {
        let mod_time = datetime!(2025-01-15 10:30:45 UTC);

        temp_env::with_var(ENV_ILM_PROCESS_TIME, Some("0"), || {
            temp_env::with_var_unset(ENV_ILM_PROCESS_TIME_DEPRECATED, || {
                let result = expected_expiry_time(mod_time, 30);
                assert_eq!(result, datetime!(2025-02-15 00:00:00 UTC));
            });
        });

        temp_env::with_var(ENV_ILM_PROCESS_TIME, Some("not-a-number"), || {
            temp_env::with_var_unset(ENV_ILM_PROCESS_TIME_DEPRECATED, || {
                let result = expected_expiry_time(mod_time, 30);
                assert_eq!(result, datetime!(2025-02-15 00:00:00 UTC));
            });
        });
    }

    // --- ilm-5 tests: RUSTFS_ILM_DEBUG_DAY_SECS time-acceleration switch ---

    // (a) Default path (env unset) is byte-identical: one day == 86400s.
    #[test]
    #[serial]
    fn ilm_day_secs_defaults_to_86400_when_unset() {
        temp_env::with_var_unset(ENV_ILM_DEBUG_DAY_SECS, || {
            assert_eq!(ilm_day_secs(), DEFAULT_ILM_DAY_SECS);
            assert_eq!(ilm_day_secs(), 86400);
        });
    }

    // (b) Pure parse: positive integers activate the override.
    #[test]
    fn parse_ilm_day_secs_accepts_positive_values() {
        assert_eq!(parse_ilm_day_secs("1"), 1);
        assert_eq!(parse_ilm_day_secs("2"), 2);
        assert_eq!(parse_ilm_day_secs("  10 "), 10);
        assert_eq!(parse_ilm_day_secs("3600"), 3600);
    }

    // (c) Pure parse: invalid / zero / negative fall back to 86400.
    #[test]
    fn parse_ilm_day_secs_rejects_invalid_and_zero() {
        assert_eq!(parse_ilm_day_secs("0"), DEFAULT_ILM_DAY_SECS);
        assert_eq!(parse_ilm_day_secs("-5"), DEFAULT_ILM_DAY_SECS);
        assert_eq!(parse_ilm_day_secs("not-a-number"), DEFAULT_ILM_DAY_SECS);
        assert_eq!(parse_ilm_day_secs(""), DEFAULT_ILM_DAY_SECS);
        assert_eq!(parse_ilm_day_secs("1.5"), DEFAULT_ILM_DAY_SECS);
    }

    // (b) End-to-end env read scales the day length.
    #[test]
    #[serial]
    fn ilm_day_secs_scales_when_env_set() {
        temp_env::with_var(ENV_ILM_DEBUG_DAY_SECS, Some("2"), || {
            assert_eq!(ilm_day_secs(), 2);
        });
    }

    // (c) Invalid env value falls back to 86400.
    #[test]
    #[serial]
    fn ilm_day_secs_falls_back_on_invalid_env() {
        temp_env::with_var(ENV_ILM_DEBUG_DAY_SECS, Some("bogus"), || {
            assert_eq!(ilm_day_secs(), DEFAULT_ILM_DAY_SECS);
        });
        temp_env::with_var(ENV_ILM_DEBUG_DAY_SECS, Some("0"), || {
            assert_eq!(ilm_day_secs(), DEFAULT_ILM_DAY_SECS);
        });
    }

    // Deadline math scales: with a 1s day and PROCESS_TIME unset, a Days=1 rule is
    // due 1s after mod_time (rounded up to the next 1s boundary => same instant).
    #[test]
    #[serial]
    fn expected_expiry_time_scales_with_debug_day_secs() {
        let mod_time = datetime!(2025-01-15 10:30:45 UTC);
        temp_env::with_var(ENV_ILM_DEBUG_DAY_SECS, Some("1"), || {
            temp_env::with_var_unset(ENV_ILM_PROCESS_TIME, || {
                temp_env::with_var_unset(ENV_ILM_PROCESS_TIME_DEPRECATED, || {
                    // 1 "day" == 1s; boundary also defaults to 1s so no extra rounding.
                    assert_eq!(expected_expiry_time(mod_time, 1), mod_time + Duration::seconds(1));
                    assert_eq!(expected_expiry_time(mod_time, 5), mod_time + Duration::seconds(5));
                });
            });
        });
    }

    // days == 0 still yields the immediate-expiry sentinel regardless of the switch.
    #[test]
    #[serial]
    fn expected_expiry_time_zero_days_ignores_debug_day_secs() {
        let mod_time = datetime!(2025-06-01 12:00:00 UTC);
        temp_env::with_var(ENV_ILM_DEBUG_DAY_SECS, Some("2"), || {
            assert_eq!(expected_expiry_time(mod_time, 0), OffsetDateTime::UNIX_EPOCH);
        });
    }

    // (③) Interaction with an explicit RUSTFS_ILM_PROCESS_TIME: the deadline offset
    // uses the accelerated day length, but the rounding boundary honors PROCESS_TIME.
    #[test]
    #[serial]
    fn expected_expiry_time_debug_day_secs_respects_explicit_process_time() {
        let mod_time = datetime!(2025-01-15 10:30:00 UTC);
        // day == 10s, but round up to the next 60s (PROCESS_TIME) boundary.
        temp_env::with_var(ENV_ILM_DEBUG_DAY_SECS, Some("10"), || {
            temp_env::with_var(ENV_ILM_PROCESS_TIME, Some("60"), || {
                temp_env::with_var_unset(ENV_ILM_PROCESS_TIME_DEPRECATED, || {
                    // mod_time + 30s = 10:30:30, rounded up to next minute => 10:31:00.
                    let result = expected_expiry_time(mod_time, 3);
                    assert_eq!(result, datetime!(2025-01-15 10:31:00 UTC));
                });
            });
        });
    }

    // (③) With the switch unset, an explicit PROCESS_TIME behaves exactly as before.
    #[test]
    #[serial]
    fn expected_expiry_time_unset_debug_day_secs_matches_legacy_process_time() {
        let mod_time = datetime!(2025-01-15 10:30:45 UTC);
        temp_env::with_var_unset(ENV_ILM_DEBUG_DAY_SECS, || {
            temp_env::with_var(ENV_ILM_PROCESS_TIME, Some("3600"), || {
                temp_env::with_var_unset(ENV_ILM_PROCESS_TIME_DEPRECATED, || {
                    assert_eq!(expected_expiry_time(mod_time, 1), datetime!(2025-01-16 11:00:00 UTC));
                });
            });
        });
    }

    /// Run an async body inside a synchronous `temp_env` scope so the overridden
    /// environment stays set across every `.await`. Uses a fresh current-thread
    /// runtime (no outer runtime => `block_on` is safe) and keeps these tests off
    /// the optional `async_closure` temp-env feature.
    fn block_on_with_env<F: std::future::Future>(vars: &[(&str, Option<&str>)], fut_fn: impl FnOnce() -> F) -> F::Output {
        temp_env::with_vars(vars, || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build runtime");
            rt.block_on(fut_fn())
        })
    }

    // The abort-incomplete-multipart deadline path also scales through the switch.
    #[test]
    #[serial]
    fn abort_incomplete_multipart_due_scales_with_debug_day_secs() {
        use s3s::dto::AbortIncompleteMultipartUpload;
        let initiated = datetime!(2025-01-15 10:30:45 UTC);
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: None,
                abort_incomplete_multipart_upload: Some(AbortIncompleteMultipartUpload {
                    days_after_initiation: Some(3),
                }),
                del_marker_expiration: None,
                filter: None,
                id: Some("abort-mpu".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
            }],
        };
        let opts = ObjectOpts {
            name: "obj".to_string(),
            mod_time: Some(initiated),
            is_latest: true,
            ..Default::default()
        };

        block_on_with_env(
            &[
                (ENV_ILM_DEBUG_DAY_SECS, Some("1")),
                (ENV_ILM_PROCESS_TIME, None),
                (ENV_ILM_PROCESS_TIME_DEPRECATED, None),
            ],
            || async {
                let (due, id) = abort_incomplete_multipart_upload_due(&lc, &opts).await.expect("due");
                assert_eq!(due, initiated + Duration::seconds(3));
                assert_eq!(id, "abort-mpu");
            },
        );
    }

    // (⑤ evaluator seam) A Days=1 rule fires under RUSTFS_ILM_DEBUG_DAY_SECS=1 once
    // `now` advances a few seconds past a mod_time only ~seconds in the past.
    #[test]
    #[serial]
    fn eval_inner_expires_days_one_rule_under_debug_day_secs() {
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: Some(LifecycleExpiration {
                    days: Some(1),
                    ..Default::default()
                }),
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: Some("expire-days".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
            }],
        };

        let mod_time = datetime!(2025-01-15 10:30:00 UTC);
        let opts = ObjectOpts {
            name: "obj".to_string(),
            mod_time: Some(mod_time),
            is_latest: true,
            ..Default::default()
        };

        block_on_with_env(
            &[
                (ENV_ILM_DEBUG_DAY_SECS, Some("1")),
                (ENV_ILM_PROCESS_TIME, None),
                (ENV_ILM_PROCESS_TIME_DEPRECATED, None),
            ],
            || async {
                // Only 2s after mod_time: with a real day this would NOT be due, but a
                // 1s day makes the Days=1 deadline (mod_time + 1s) already in the past.
                let now = mod_time + Duration::seconds(2);
                let event = lc.eval_inner(&opts, now, 0).await;
                assert_eq!(event.action, IlmAction::DeleteAction);
                assert_eq!(event.rule_id, "expire-days");
                assert_eq!(event.due, Some(mod_time + Duration::seconds(1)));
            },
        );
    }

    // Absolute Date-based rules must NOT scale with the switch (regression guard).
    #[test]
    #[serial]
    fn eval_inner_date_rule_ignores_debug_day_secs() {
        let expiry_date = datetime!(2025-06-01 00:00:00 UTC);
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: Some(LifecycleExpiration {
                    date: Some(expiry_date.into()),
                    ..Default::default()
                }),
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: Some("expire-date".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
            }],
        };
        let opts = ObjectOpts {
            name: "obj".to_string(),
            mod_time: Some(datetime!(2025-01-15 10:30:00 UTC)),
            is_latest: true,
            ..Default::default()
        };

        block_on_with_env(&[(ENV_ILM_DEBUG_DAY_SECS, Some("1"))], || async {
            // A few seconds after mod_time; the absolute June date is still far away.
            let now = datetime!(2025-01-15 10:30:05 UTC);
            let event = lc.eval_inner(&opts, now, 0).await;
            assert_eq!(event.action, IlmAction::NoneAction);
        });
    }

    // --- TASK-007 tests: Legacy Prefix/Filter conflict ---

    #[tokio::test]
    #[serial]
    async fn validate_rejects_prefix_and_filter_both_present() {
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: Some(LifecycleExpiration {
                    days: Some(30),
                    ..Default::default()
                }),
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: Some(LifecycleRuleFilter {
                    prefix: Some("logs/".to_string()),
                    ..Default::default()
                }),
                id: Some("test-rule".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: Some("archive/".to_string()),
                transitions: None,
            }],
        };

        let err = lc.validate(&ObjectLockConfiguration::default()).await.unwrap_err();
        assert_eq!(err.to_string(), ERR_LIFECYCLE_PREFIX_FILTER_CONFLICT);
    }

    #[tokio::test]
    #[serial]
    async fn validate_allows_prefix_without_filter() {
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: Some(LifecycleExpiration {
                    days: Some(30),
                    ..Default::default()
                }),
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: Some("test-rule".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: Some("logs/".to_string()),
                transitions: None,
            }],
        };

        lc.validate(&ObjectLockConfiguration::default())
            .await
            .expect("prefix without filter should be valid");
    }

    #[tokio::test]
    #[serial]
    async fn validate_allows_filter_without_prefix() {
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: Some(LifecycleExpiration {
                    days: Some(30),
                    ..Default::default()
                }),
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: Some(LifecycleRuleFilter {
                    prefix: Some("logs/".to_string()),
                    ..Default::default()
                }),
                id: Some("test-rule".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
            }],
        };

        lc.validate(&ObjectLockConfiguration::default())
            .await
            .expect("filter without prefix should be valid");
    }

    #[tokio::test]
    #[serial]
    async fn validate_allows_empty_prefix_with_filter() {
        // Empty prefix should be treated as "not set"
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: Some(LifecycleExpiration {
                    days: Some(30),
                    ..Default::default()
                }),
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: Some(LifecycleRuleFilter {
                    prefix: Some("logs/".to_string()),
                    ..Default::default()
                }),
                id: Some("test-rule".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: Some("".to_string()), // empty = not set
                transitions: None,
            }],
        };

        lc.validate(&ObjectLockConfiguration::default())
            .await
            .expect("empty prefix with filter should be valid");
    }

    // --- TASK-004 tests: ExpiredObjectAllVersions ---

    #[tokio::test]
    #[serial]
    async fn validate_rejects_expired_object_all_versions_on_locked_bucket() {
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: Some(LifecycleExpiration {
                    days: Some(30),
                    expired_object_all_versions: Some(true),
                    ..Default::default()
                }),
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: Some("all-versions-rule".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
            }],
        };

        let locked_config = ObjectLockConfiguration {
            object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
            ..Default::default()
        };

        let err = lc.validate(&locked_config).await.unwrap_err();
        assert_eq!(err.to_string(), ERR_LIFECYCLE_BUCKET_LOCKED);
    }

    #[tokio::test]
    #[serial]
    async fn validate_allows_expired_object_all_versions_on_unlocked_bucket() {
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: Some(LifecycleExpiration {
                    days: Some(30),
                    expired_object_all_versions: Some(true),
                    ..Default::default()
                }),
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: Some("all-versions-rule".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
            }],
        };

        lc.validate(&ObjectLockConfiguration::default())
            .await
            .expect("ExpiredObjectAllVersions should be allowed on unlocked bucket");
    }

    #[tokio::test]
    #[serial]
    async fn eval_inner_triggers_delete_all_versions_when_expired_object_all_versions_set() {
        let base_time = OffsetDateTime::from_unix_timestamp(1_000_000).unwrap();
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: Some(LifecycleExpiration {
                    days: Some(1),
                    expired_object_all_versions: Some(true),
                    ..Default::default()
                }),
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: Some("all-versions-rule".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
            }],
        };

        let opts = ObjectOpts {
            name: "obj".to_string(),
            mod_time: Some(base_time),
            is_latest: true,
            version_id: None,
            ..Default::default()
        };

        // now is after the expiry time
        let now = base_time + Duration::days(2);
        let event = lc.eval_inner(&opts, now, 0).await;
        assert_eq!(event.action, IlmAction::DeleteAllVersionsAction);
        assert_eq!(event.rule_id, "all-versions-rule");
    }

    #[tokio::test]
    #[serial]
    async fn eval_inner_uses_delete_action_when_all_versions_not_set() {
        let base_time = OffsetDateTime::from_unix_timestamp(1_000_000).unwrap();
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: Some(LifecycleExpiration {
                    days: Some(1),
                    expired_object_all_versions: None, // not set
                    ..Default::default()
                }),
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: Some("normal-rule".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
            }],
        };

        let opts = ObjectOpts {
            name: "obj".to_string(),
            mod_time: Some(base_time),
            is_latest: true,
            version_id: None,
            ..Default::default()
        };

        let now = base_time + Duration::days(2);
        let event = lc.eval_inner(&opts, now, 0).await;
        // Without ExpiredObjectAllVersions, should use normal DeleteAction
        assert_eq!(event.action, IlmAction::DeleteAction);
    }

    /// Property-based tests for the rule evaluator (backlog#1148 ilm-14,
    /// follow-up to backlog#1030 / rustfs#4455).
    ///
    /// backlog#1030 found that the old hand-written winner comparator was not a
    /// strict weak ordering and could panic — proof that enumerated cases do
    /// not cover the space of colliding events. These properties pin, over
    /// randomized rule sets and object states:
    ///
    /// * `eval_inner` never panics and is deterministic for a fixed input;
    /// * the winning event matches an independently recomputed candidate set:
    ///   earliest `due` wins, ties break toward delete-class actions (the
    ///   `min_by_key` selection that replaced the rustfs#4455 comparator);
    /// * `expected_expiry_time` is monotonically non-decreasing in `days` and
    ///   always lands on the processing boundary, both at production defaults
    ///   and under an explicit `RUSTFS_ILM_PROCESS_TIME`.
    ///
    /// Case counts are tuned so the whole module runs in seconds inside the
    /// default CI test job.
    mod proptests {
        use super::*;
        use proptest::prelude::*;
        use s3s::dto::{NoncurrentVersionExpiration, Tag};
        use serial_test::serial;

        const DAY_SECS: i64 = 86400;

        /// Fixed positive anchor so generated instants stay in a realistic
        /// range; strategies explore offsets around it.
        fn base() -> OffsetDateTime {
            datetime!(2025-01-01 00:00:00 UTC)
        }

        fn ts(offset_secs: i64) -> OffsetDateTime {
            base() + Duration::seconds(offset_secs)
        }

        /// Pin the day length and processing boundary to production defaults
        /// for the duration of `f`. `temp_env` also serializes environment
        /// access against the other env-sensitive tests in this file.
        fn with_production_time_env<T>(f: impl FnOnce() -> T) -> T {
            temp_env::with_vars(
                [
                    (ENV_ILM_DEBUG_DAY_SECS, None::<&str>),
                    (ENV_ILM_PROCESS_TIME, None),
                    (ENV_ILM_PROCESS_TIME_DEPRECATED, None),
                ],
                f,
            )
        }

        fn arb_opt_time(window_secs: i64) -> impl Strategy<Value = Option<OffsetDateTime>> {
            prop_oneof![
                2 => Just(None),
                1 => Just(Some(OffsetDateTime::UNIX_EPOCH)),
                5 => (-window_secs..window_secs).prop_map(|s| Some(ts(s))),
            ]
        }

        fn arb_object_name() -> impl Strategy<Value = String> {
            prop_oneof![
                1 => Just(String::new()),
                3 => Just("docs/report.txt".to_string()),
                2 => Just("logs/app.log".to_string()),
                1 => "[a-z]{1,12}",
            ]
        }

        fn arb_user_tags() -> impl Strategy<Value = String> {
            prop_oneof![
                Just(String::new()),
                Just("env=prod".to_string()),
                Just("env=prod&team=storage".to_string()),
                Just("team=storage".to_string()),
            ]
        }

        fn arb_transition_status() -> impl Strategy<Value = String> {
            prop_oneof![
                Just(String::new()),
                Just(TRANSITION_PENDING.to_string()),
                Just(TRANSITION_COMPLETE.to_string()),
            ]
        }

        prop_compose! {
            fn arb_object_identity()(
                name in arb_object_name(),
                user_tags in arb_user_tags(),
                mod_time in arb_opt_time(4 * DAY_SECS),
                size in prop_oneof![Just(0usize), (1usize..1_048_576), Just(usize::MAX >> 1)],
                version_id in prop_oneof![Just(None), Just(Some(Uuid::nil())), Just(Some(Uuid::from_u128(7)))],
                is_latest in any::<bool>(),
                delete_marker in any::<bool>(),
            ) -> (String, String, Option<OffsetDateTime>, usize, Option<Uuid>, bool, bool) {
                (name, user_tags, mod_time, size, version_id, is_latest, delete_marker)
            }
        }

        prop_compose! {
            /// Random object state across the version-chain dimensions: chain
            /// position (`is_latest` / `num_versions` / `successor_mod_time`),
            /// delete markers, suspension, and transition status.
            fn arb_object_opts()(
                (name, user_tags, mod_time, size, version_id, is_latest, delete_marker) in arb_object_identity(),
                num_versions in 0usize..4,
                successor_mod_time in arb_opt_time(4 * DAY_SECS),
                transition_status in arb_transition_status(),
                restore_ongoing in any::<bool>(),
                restore_expires in arb_opt_time(2 * DAY_SECS),
                versioned in any::<bool>(),
                version_suspended in any::<bool>(),
            ) -> ObjectOpts {
                ObjectOpts {
                    name,
                    user_tags,
                    mod_time,
                    size,
                    version_id,
                    is_latest,
                    delete_marker,
                    num_versions,
                    successor_mod_time,
                    transition_status,
                    restore_ongoing,
                    restore_expires,
                    versioned,
                    version_suspended,
                    ..Default::default()
                }
            }
        }

        fn arb_prefix() -> impl Strategy<Value = Option<String>> {
            prop_oneof![
                3 => Just(None),
                1 => Just(Some(String::new())),
                2 => Just(Some("docs/".to_string())),
                1 => Just(Some("logs/".to_string())),
            ]
        }

        fn arb_filter() -> impl Strategy<Value = Option<LifecycleRuleFilter>> {
            prop_oneof![
                4 => Just(None),
                1 => Just(Some(LifecycleRuleFilter {
                    prefix: Some("docs/".to_string()),
                    ..Default::default()
                })),
                1 => Just(Some(LifecycleRuleFilter {
                    tag: Some(Tag {
                        key: Some("env".to_string()),
                        value: Some("prod".to_string()),
                    }),
                    ..Default::default()
                })),
                1 => (0i64..2048).prop_map(|gt| Some(LifecycleRuleFilter {
                    object_size_greater_than: Some(gt),
                    ..Default::default()
                })),
            ]
        }

        prop_compose! {
            fn arb_expiration()(
                date_off in prop::option::of(-2 * DAY_SECS..2 * DAY_SECS),
                days in prop::option::of(0i32..4),
                delete_marker_flag in prop::option::of(any::<bool>()),
                all_versions in prop::option::of(any::<bool>()),
            ) -> LifecycleExpiration {
                LifecycleExpiration {
                    date: date_off.map(|s| ts(s).into()),
                    days,
                    expired_object_delete_marker: delete_marker_flag,
                    expired_object_all_versions: all_versions,
                }
            }
        }

        prop_compose! {
            fn arb_transition()(
                date_off in prop::option::of(-2 * DAY_SECS..2 * DAY_SECS),
                days in prop::option::of(0i32..4),
                storage_class in prop_oneof![
                    Just(None),
                    Just(Some(TransitionStorageClass::from_static(""))),
                    Just(Some(TransitionStorageClass::from_static("COLD"))),
                ],
            ) -> Transition {
                Transition {
                    date: date_off.map(|s| ts(s).into()),
                    days,
                    storage_class,
                }
            }
        }

        prop_compose! {
            /// Fully random rule across every dimension the evaluator reads:
            /// Days/Date expiration and transition, prefix, tag/size filters,
            /// noncurrent actions, delete-marker actions, AllVersions.
            fn arb_rule()(
                enabled in prop::bool::weighted(0.8),
                id in prop::option::of("[a-z]{1,6}"),
                prefix in arb_prefix(),
                filter in arb_filter(),
                expiration in prop::option::of(arb_expiration()),
                transitions in prop::option::of(prop::collection::vec(arb_transition(), 0..2)),
                nc_exp in prop::option::of((prop::option::of(0i32..4), prop::option::of(0i32..3))),
                nc_tr in prop::option::of((prop::option::of(0i32..4), any::<bool>())),
                dme_days in prop::option::of(0i32..3),
                abort_days in prop::option::of(0i32..3),
            ) -> LifecycleRule {
                LifecycleRule {
                    status: if enabled {
                        ExpirationStatus::from_static(ExpirationStatus::ENABLED)
                    } else {
                        ExpirationStatus::from_static(ExpirationStatus::DISABLED)
                    },
                    id,
                    prefix,
                    filter,
                    expiration,
                    transitions,
                    noncurrent_version_expiration: nc_exp.map(|(noncurrent_days, newer_noncurrent_versions)| {
                        NoncurrentVersionExpiration {
                            noncurrent_days,
                            newer_noncurrent_versions,
                        }
                    }),
                    noncurrent_version_transitions: nc_tr.map(|(noncurrent_days, has_sc)| {
                        vec![NoncurrentVersionTransition {
                            noncurrent_days,
                            newer_noncurrent_versions: None,
                            storage_class: has_sc.then(|| TransitionStorageClass::from_static("COLD")),
                        }]
                    }),
                    del_marker_expiration: dme_days.map(|days| s3s::dto::DelMarkerExpiration { days: Some(days) }),
                    abort_incomplete_multipart_upload: abort_days.map(|days| {
                        s3s::dto::AbortIncompleteMultipartUpload {
                            days_after_initiation: Some(days),
                        }
                    }),
                }
            }
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(96))]

            /// `eval_inner` must never panic on any rule/object/now
            /// combination, and must be deterministic: the same input
            /// evaluated twice yields an identical event.
            #[test]
            #[serial]
            fn eval_inner_never_panics_and_is_deterministic(
                rules in prop::collection::vec(arb_rule(), 0..4),
                obj in arb_object_opts(),
                now_off in (-4 * DAY_SECS)..(8 * DAY_SECS),
                newer_noncurrent in 0usize..4,
            ) {
                let lc = BucketLifecycleConfiguration {
                    expiry_updated_at: None,
                    rules,
                };
                let now = ts(now_off);
                let (first, second) = block_on_with_env(&[], || async {
                    let first = lc.eval_inner(&obj, now, newer_noncurrent).await;
                    let second = lc.eval_inner(&obj, now, newer_noncurrent).await;
                    (first, second)
                });
                prop_assert_eq!(first.action, second.action);
                prop_assert_eq!(&first.rule_id, &second.rule_id);
                prop_assert_eq!(first.due, second.due);
                prop_assert_eq!(&first.storage_class, &second.storage_class);
            }
        }

        /// One oracle candidate: `(due unix timestamp, action rank)` with rank
        /// 0 for delete-class actions and 1 otherwise.
        type Candidate = (i64, u8);

        /// Independent re-statement of the delete-class rank. Deliberately NOT
        /// `ilm_action_priority_rank`: the winner assertion classifies the
        /// actual event with this copy, so a regression in the production rank
        /// function cannot silently re-rank both sides of the comparison.
        fn oracle_rank(action: &IlmAction) -> u8 {
            match action {
                IlmAction::DeleteAllVersionsAction
                | IlmAction::DelMarkerDeleteAllVersionsAction
                | IlmAction::DeleteAction
                | IlmAction::DeleteVersionAction => 0,
                _ => 1,
            }
        }

        /// Independently recompute the eligible event set `eval_inner` should
        /// consider for a live current version under `selection`-shaped rules
        /// (expiration and first-transition only, no filters): expiration
        /// fires when `now >= due`, transition when `now > due` and the object
        /// has not already transitioned. Selection semantics under test:
        /// earliest due wins, ties prefer delete-class.
        fn oracle_candidates(lc: &BucketLifecycleConfiguration, obj: &ObjectOpts, now: OffsetDateTime) -> Vec<Candidate> {
            let mod_time = obj.mod_time.expect("selection strategy always sets mod_time");
            let mut candidates = Vec::new();
            for rule in &lc.rules {
                if let Some(expiration) = &rule.expiration {
                    // Mirrors the eval branches: a Date-based expiry needs a
                    // non-zero date and fires when `now >= date`; a Days-based
                    // expiry fires when `now >= expected_expiry_time`.
                    if let Some(date) = &expiration.date {
                        let due = OffsetDateTime::from(date.clone());
                        if due.unix_timestamp() != 0 && now.unix_timestamp() >= due.unix_timestamp() {
                            candidates.push((due.unix_timestamp(), 0));
                        }
                    } else if let Some(days) = expiration.days {
                        let due = expected_expiry_time(mod_time, days);
                        if now.unix_timestamp() >= due.unix_timestamp() {
                            candidates.push((due.unix_timestamp(), 0));
                        }
                    }
                }
                if obj.transition_status != TRANSITION_COMPLETE
                    && let Some(transition) = rule.transitions.as_ref().and_then(|transitions| transitions.first())
                    && transition.storage_class.as_ref().is_some_and(|sc| !sc.as_str().is_empty())
                {
                    let due = if let Some(date) = &transition.date {
                        Some(OffsetDateTime::from(date.clone()))
                    } else {
                        transition.days.map(|days| expected_expiry_time(mod_time, days))
                    };
                    if let Some(due) = due
                        && now.unix_timestamp() > due.unix_timestamp()
                    {
                        candidates.push((due.unix_timestamp(), 1));
                    }
                }
            }
            candidates
        }

        prop_compose! {
            /// Rules restricted to the shapes the selection oracle models:
            /// enabled, unfiltered, with exactly one of Date/Days per action so
            /// the Date-over-Days precedence inside one action never applies.
            fn arb_selection_rule()(
                kind in 0u8..3,
                exp_by_date in any::<bool>(),
                exp_date_off in (-2 * DAY_SECS)..(2 * DAY_SECS),
                exp_days in 0i32..4,
                all_versions in any::<bool>(),
                tr_by_date in any::<bool>(),
                tr_date_off in (-2 * DAY_SECS)..(2 * DAY_SECS),
                tr_days in 0i32..4,
                id in "[a-z]{1,6}",
            ) -> LifecycleRule {
                let expiration = (kind != 1).then(|| LifecycleExpiration {
                    date: exp_by_date.then(|| ts(exp_date_off).into()),
                    days: (!exp_by_date).then_some(exp_days),
                    expired_object_delete_marker: None,
                    expired_object_all_versions: all_versions.then_some(true),
                });
                let transitions = (kind != 0).then(|| {
                    vec![Transition {
                        date: tr_by_date.then(|| ts(tr_date_off).into()),
                        days: (!tr_by_date).then_some(tr_days),
                        storage_class: Some(TransitionStorageClass::from_static("COLD")),
                    }]
                });
                LifecycleRule {
                    status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                    id: Some(id),
                    prefix: None,
                    filter: None,
                    expiration,
                    transitions,
                    noncurrent_version_expiration: None,
                    noncurrent_version_transitions: None,
                    del_marker_expiration: None,
                    abort_incomplete_multipart_upload: None,
                }
            }
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(128))]

            /// Differential test of winner selection (the rustfs#4455 fix):
            /// for a live current version under randomized expiration and
            /// transition rules, `eval_inner`'s winner must carry the
            /// minimum `(due, rank)` of the independently recomputed
            /// candidate set — earliest due wins, ties prefer delete-class —
            /// and must be `NoneAction` exactly when that set is empty.
            #[test]
            #[serial]
            fn eval_inner_winner_matches_selection_oracle(
                rules in prop::collection::vec(arb_selection_rule(), 0..5),
                mod_off in 0i64..(2 * DAY_SECS),
                now_off in (-DAY_SECS)..(6 * DAY_SECS),
                already_transitioned in any::<bool>(),
            ) {
                let lc = BucketLifecycleConfiguration {
                    expiry_updated_at: None,
                    rules,
                };
                let obj = ObjectOpts {
                    name: "docs/report.txt".to_string(),
                    mod_time: Some(ts(mod_off)),
                    is_latest: true,
                    transition_status: if already_transitioned {
                        TRANSITION_COMPLETE.to_string()
                    } else {
                        String::new()
                    },
                    ..Default::default()
                };
                let now = ts(now_off);

                // Oracle and evaluator must observe the same (pinned) time env.
                let (event, expected) = with_production_time_env(|| {
                    let expected = oracle_candidates(&lc, &obj, now).into_iter().min();
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("build runtime");
                    let event = rt.block_on(lc.eval_inner(&obj, now, 0));
                    (event, expected)
                });

                match expected {
                    None => prop_assert_eq!(event.action, IlmAction::NoneAction),
                    Some((due_ts, rank)) => {
                        prop_assert_ne!(event.action, IlmAction::NoneAction);
                        let actual_due = event.due.expect("winning event carries a due").unix_timestamp();
                        prop_assert_eq!(actual_due, due_ts);
                        prop_assert_eq!(oracle_rank(&event.action), rank);
                    }
                }
            }
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(256))]

            /// At production defaults `expected_expiry_time` is monotonically
            /// non-decreasing in `days` (days == 0 maps to UNIX_EPOCH, below
            /// any post-1970 deadline).
            #[test]
            #[serial]
            fn expected_expiry_time_is_monotonic_in_days(
                mod_off in 0i64..(3650 * DAY_SECS),
                d1 in 0i32..2000,
                d2 in 0i32..2000,
            ) {
                let (lo, hi) = if d1 <= d2 { (d1, d2) } else { (d2, d1) };
                let mod_time = ts(mod_off);
                let (e_lo, e_hi) = with_production_time_env(|| {
                    (expected_expiry_time(mod_time, lo), expected_expiry_time(mod_time, hi))
                });
                prop_assert!(
                    e_lo <= e_hi,
                    "expected_expiry_time not monotonic: days={} -> {:?}, days={} -> {:?}",
                    lo, e_lo, hi, e_hi
                );
            }

            /// At production defaults every Days-based deadline is rounded up
            /// to the next whole-day boundary: the result is day-aligned, not
            /// before `mod_time + days`, and less than one boundary beyond it.
            #[test]
            #[serial]
            fn expected_expiry_time_lands_on_default_day_boundary(
                mod_off in 0i64..(3650 * DAY_SECS),
                days in 1i32..2000,
            ) {
                let mod_time = ts(mod_off);
                let result = with_production_time_env(|| expected_expiry_time(mod_time, days));
                let boundary_nanos = i128::from(DAY_SECS) * 1_000_000_000;
                prop_assert_eq!(result.unix_timestamp_nanos().rem_euclid(boundary_nanos), 0);
                let raw = mod_time + Duration::seconds(i64::from(days) * DAY_SECS);
                prop_assert!(result >= raw);
                prop_assert!(result - raw < Duration::seconds(DAY_SECS));
            }

            /// With an explicit `RUSTFS_ILM_PROCESS_TIME`, deadlines round up
            /// to that boundary instead: aligned to it, never early, and less
            /// than one boundary late.
            #[test]
            #[serial]
            fn expected_expiry_time_lands_on_explicit_process_boundary(
                mod_off in 0i64..(365 * DAY_SECS),
                days in 1i32..400,
                boundary in 1u32..7200,
            ) {
                let mod_time = ts(mod_off);
                let boundary_str = boundary.to_string();
                let result = temp_env::with_vars(
                    [
                        (ENV_ILM_DEBUG_DAY_SECS, None::<&str>),
                        (ENV_ILM_PROCESS_TIME, Some(boundary_str.as_str())),
                        (ENV_ILM_PROCESS_TIME_DEPRECATED, None),
                    ],
                    || expected_expiry_time(mod_time, days),
                );
                let boundary_nanos = i128::from(boundary) * 1_000_000_000;
                prop_assert_eq!(result.unix_timestamp_nanos().rem_euclid(boundary_nanos), 0);
                let raw = mod_time + Duration::seconds(i64::from(days) * DAY_SECS);
                prop_assert!(result >= raw);
                prop_assert!(result - raw < Duration::seconds(i64::from(boundary)));
            }
        }
    }
}
