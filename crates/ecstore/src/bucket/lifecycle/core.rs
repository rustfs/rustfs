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

use rustfs_filemeta::{ReplicationStatusType, VersionPurgeStatusType};
use s3s::dto::{
    BucketLifecycleConfiguration, ExpirationStatus, LifecycleExpiration, LifecycleRule, LifecycleRuleFilter,
    NoncurrentVersionTransition, ObjectLockConfiguration, ObjectLockEnabled, RestoreRequest, Transition, TransitionStorageClass,
};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use time::macros::offset;
use time::{self, Duration, OffsetDateTime};
use tracing::{debug, info};
use uuid::Uuid;

use crate::store_api::ObjectInfo;

pub const TRANSITION_COMPLETE: &str = "complete";
pub const TRANSITION_PENDING: &str = "pending";
const ERR_LIFECYCLE_NO_RULE: &str = "Lifecycle configuration should have at least one rule";
const ERR_LIFECYCLE_DUPLICATE_ID: &str = "Rule ID must be unique. Found same ID for more than one rule";
const _ERR_XML_NOT_WELL_FORMED: &str =
    "The XML you provided was not well-formed or did not validate against our published schema";
const ERR_LIFECYCLE_BUCKET_LOCKED: &str = "ExpiredObjectDeleteMarker is not allowed on a bucket with Object Lock enabled";
const ERR_LIFECYCLE_TOO_MANY_RULES: &str = "Lifecycle configuration should have at most 1000 rules";
const ERR_LIFECYCLE_INVALID_EXPIRATION_DAYS: &str = "Lifecycle expiration days must not be negative";
const ERR_LIFECYCLE_INVALID_NONCURRENT_EXPIRATION_DAYS: &str = "Lifecycle noncurrent expiration days must not be negative";
const ERR_LIFECYCLE_INVALID_EXPIRATION_DATE_NOT_MIDNIGHT: &str = "Expiration.Date must be at midnight UTC";
const ERR_LIFECYCLE_INVALID_RULE_ID_TOO_LONG: &str = "Rule ID must be at most 255 characters";
const ERR_LIFECYCLE_INVALID_RULE_STATUS: &str = "Rule status must be either Enabled or Disabled";
const ERR_LIFECYCLE_DEL_MARKER_WITH_TAGS: &str = "Rule with DelMarkerExpiration cannot have tags based filtering";
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
            if rule.transitions.is_some() {
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
            if rule.noncurrent_version_transitions.is_some() {
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
            if let Some(rule_transitions) = &rule.transitions {
                let rule_transitions_0 = rule_transitions[0].clone();
                if let Some(date1) = rule_transitions_0.date
                    && OffsetDateTime::from(date1).unix_timestamp() < OffsetDateTime::now_utc().unix_timestamp()
                {
                    return true;
                }
            }
            if rule.transitions.is_some() {
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
                if let Some(days) = expiration.days
                    && days < 0
                {
                    return Err(std::io::Error::other(ERR_LIFECYCLE_INVALID_EXPIRATION_DAYS));
                }
            }
            if let Some(noncurrent_version_expiration) = &r.noncurrent_version_expiration
                && let Some(noncurrent_days) = noncurrent_version_expiration.noncurrent_days
                && noncurrent_days < 0
            {
                return Err(std::io::Error::other(ERR_LIFECYCLE_INVALID_NONCURRENT_EXPIRATION_DAYS));
            }
            if let Some(id) = &r.id
                && id.len() > 255
            {
                return Err(std::io::Error::other(ERR_LIFECYCLE_INVALID_RULE_ID_TOO_LONG));
            }
            r.validate()?;
            if let Some(object_lock_enabled) = lr.object_lock_enabled.as_ref()
                && object_lock_enabled.as_str() == ObjectLockEnabled::ENABLED
                && let Some(expiration) = r.expiration.as_ref()
            {
                // Object Lock + ExpiredObjectDeleteMarker conflict
                if expiration.expired_object_delete_marker.is_some_and(|v| v) {
                    return Err(std::io::Error::other(ERR_LIFECYCLE_BUCKET_LOCKED));
                }
                // Object Lock + ExpiredObjectAllVersions conflict (MinIO extension)
                if expiration.expired_object_all_versions.is_some_and(|v| v) {
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
                if !<LifecycleRuleFilter as crate::bucket::lifecycle::rule::Filter>::test_tags(filter, &obj.user_tags) {
                    continue;
                }
                if !obj.delete_marker
                    && !<LifecycleRuleFilter as crate::bucket::lifecycle::rule::Filter>::by_size(filter, obj.size as i64)
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

    async fn eval_inner(&self, obj: &ObjectOpts, now: OffsetDateTime, _newer_noncurrent_versions: usize) -> Event {
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
                    && let Some(newer_noncurrent_versions) = noncurrent_version_expiration.newer_noncurrent_versions
                    && newer_noncurrent_versions > 0
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
                    && let Some(ref noncurrent_version_transitions) = rule.noncurrent_version_transitions
                    && let Some(ref storage_class) = noncurrent_version_transitions[0].storage_class
                    && storage_class.as_str() != ""
                    && !obj.delete_marker
                    && obj.transition_status != TRANSITION_COMPLETE
                {
                    let due = rule.noncurrent_version_transitions.as_ref().unwrap()[0].next_due(obj);
                    if let Some(due0) = due
                        && (now.unix_timestamp() == 0 || now.unix_timestamp() > due0.unix_timestamp())
                    {
                        events.push(Event {
                            action: IlmAction::TransitionVersionAction,
                            rule_id: rule.id.clone().unwrap_or_default(),
                            due,
                            storage_class: rule.noncurrent_version_transitions.as_ref().unwrap()[0]
                                .storage_class
                                .clone()
                                .unwrap()
                                .as_str()
                                .to_string(),
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
                        && let Some(ref transitions) = rule.transitions
                    {
                        let due = transitions[0].next_due(obj);
                        if let Some(due0) = due
                            && (now.unix_timestamp() == 0 || now.unix_timestamp() > due0.unix_timestamp())
                        {
                            events.push(Event {
                                action: IlmAction::TransitionAction,
                                rule_id: rule.id.clone().unwrap_or_default(),
                                due,
                                storage_class: transitions[0]
                                    .storage_class
                                    .clone()
                                    .unwrap_or_else(|| TransitionStorageClass::from_static(""))
                                    .as_str()
                                    .to_string(),
                                noncurrent_days: 0,
                                newer_noncurrent_versions: 0,
                            });
                        }
                    }
                }
            }
        }

        if !events.is_empty() {
            events.sort_by(|a, b| {
                if now.unix_timestamp() > a.due.unwrap_or(OffsetDateTime::UNIX_EPOCH).unix_timestamp()
                    && now.unix_timestamp() > b.due.unwrap_or(OffsetDateTime::UNIX_EPOCH).unix_timestamp()
                    || a.due.unwrap_or(OffsetDateTime::UNIX_EPOCH).unix_timestamp()
                        == b.due.unwrap_or(OffsetDateTime::UNIX_EPOCH).unix_timestamp()
                {
                    match a.action {
                        IlmAction::DeleteAllVersionsAction
                        | IlmAction::DelMarkerDeleteAllVersionsAction
                        | IlmAction::DeleteAction
                        | IlmAction::DeleteVersionAction => {
                            return Ordering::Less;
                        }
                        _ => (),
                    }
                    match b.action {
                        IlmAction::DeleteAllVersionsAction
                        | IlmAction::DelMarkerDeleteAllVersionsAction
                        | IlmAction::DeleteAction
                        | IlmAction::DeleteVersionAction => {
                            return Ordering::Greater;
                        }
                        _ => (),
                    }
                    return Ordering::Less;
                }

                if a.due.unwrap_or(OffsetDateTime::UNIX_EPOCH).unix_timestamp()
                    < b.due.unwrap_or(OffsetDateTime::UNIX_EPOCH).unix_timestamp()
                {
                    return Ordering::Less;
                }
                Ordering::Greater
            });
            return events[0].clone();
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
                            noncurrent_days: noncurrent_version_expiration.noncurrent_days.unwrap_or(0) as u32,
                            newer_noncurrent_versions: newer_noncurrent_versions as usize,
                            due: Some(OffsetDateTime::UNIX_EPOCH),
                            storage_class: "".into(),
                        }
                    } else {
                        Event {
                            action: IlmAction::DeleteVersionAction,
                            rule_id: rule.id.clone().unwrap_or_default(),
                            noncurrent_days: noncurrent_version_expiration.noncurrent_days.unwrap_or(0) as u32,
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

        match self.days {
            Some(days) => Some(expected_expiry_time(obj.mod_time.unwrap(), days)),
            None => obj.mod_time,
        }
    }
}

pub fn expected_expiry_time(mod_time: OffsetDateTime, days: i32) -> OffsetDateTime {
    if days == 0 {
        info!("expected_expiry_time: days=0, returning UNIX_EPOCH for immediate expiry");
        return OffsetDateTime::UNIX_EPOCH; // Return epoch time to ensure immediate expiry
    }
    let t = mod_time
        .to_offset(offset!(-0:00:00))
        .saturating_add(Duration::days(days as i64));

    // Truncate to midnight UTC per S3 standard, unless overridden by env var.
    // _RUSTFS_ILM_PROCESS_TIME controls the truncation granularity in seconds.
    let truncation_secs = env::var("_RUSTFS_ILM_PROCESS_TIME")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(86400); // default: truncate to midnight (24h)

    let unix_secs = t.unix_timestamp();
    let truncated_secs = (unix_secs / truncation_secs as i64) * truncation_secs as i64;
    OffsetDateTime::from_unix_timestamp(truncated_secs).unwrap_or(t)
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
                .filter(|days| *days > 0)?;
            Some((expected_expiry_time(initiated, days), rule.id.unwrap_or_default()))
        })
        .min_by_key(|(due, _)| due.unix_timestamp_nanos())
}

#[derive(Default)]
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
        self.delete_marker && self.is_latest
    }

    pub fn from_object_info(oi: &ObjectInfo) -> Self {
        Self {
            name: oi.name.clone(),
            user_tags: oi.user_tags.clone(),
            mod_time: oi.mod_time,
            size: oi.size as usize,
            version_id: oi.version_id,
            is_latest: oi.is_latest,
            delete_marker: oi.delete_marker,
            num_versions: oi.num_versions,
            successor_mod_time: oi.successor_mod_time,
            transition_status: oi.transitioned_object.status.clone(),
            restore_ongoing: oi.restore_ongoing,
            restore_expires: oi.restore_expires,
            versioned: false,
            version_suspended: false,
            user_defined: oi.user_defined.clone(),
            version_purge_status: oi.version_purge_status.clone(),
            replication_status: oi.replication_status.clone(),
        }
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

#[derive(Debug, Clone, Default)]
pub struct ExpirationOptions {
    pub expire: bool,
}

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
    use s3s::dto::LifecycleRuleFilter;
    use serial_test::serial;
    use std::sync::Arc;
    use time::macros::datetime;

    #[tokio::test]
    #[serial]
    async fn validate_accepts_zero_expiration_days() {
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

        lc.validate(&ObjectLockConfiguration::default())
            .await
            .expect("zero-day expiration should be accepted");
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
    async fn validate_accepts_zero_noncurrent_expiration_days() {
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

        lc.validate(&ObjectLockConfiguration::default())
            .await
            .expect("zero-day noncurrent expiration should be accepted");
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
    async fn eval_inner_expires_latest_object_after_days_due() {
        let base_time = OffsetDateTime::from_unix_timestamp(1_000_000).unwrap();
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
        let event = lc.eval_inner(&opts, base_time + Duration::days(2), 0).await;

        assert_eq!(event.action, IlmAction::DeleteAction);
        assert_eq!(event.rule_id, "expire-days");
        assert_eq!(event.due, Some(expected_expiry_time(base_time, 1)));
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
    async fn expired_object_delete_marker_applies_with_noncurrent_versions_present() {
        let base_time = OffsetDateTime::from_unix_timestamp(1_000_000).unwrap();
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
        assert_eq!(event.action, IlmAction::DeleteVersionAction);
        assert_eq!(event.due, Some(expected_expiry_time(base_time, 1)));
    }

    #[tokio::test]
    #[serial]
    async fn expired_object_delete_marker_deletes_only_delete_marker_after_due() {
        let base_time = OffsetDateTime::from_unix_timestamp(1_000_000).unwrap();
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
        assert_eq!(event.due, Some(expected_expiry_time(base_time, 1)));
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
    async fn expired_object_delete_marker_date_based_not_yet_due() {
        // A date-based rule that has not yet reached its expiry date must not
        // trigger immediate deletion (unwrap_or(now) must not override the date).
        let base_time = OffsetDateTime::from_unix_timestamp(1_000_000).unwrap();
        let future_date = base_time + Duration::days(10);
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

        let opts = ObjectOpts {
            name: "obj".to_string(),
            mod_time: Some(base_time),
            is_latest: true,
            delete_marker: true,
            num_versions: 1,
            version_id: Some(Uuid::new_v4()),
            ..Default::default()
        };

        // now is before the configured date — must not schedule deletion
        let now_before = base_time + Duration::days(5);
        let event_before = lc.eval_inner(&opts, now_before, 0).await;
        assert_eq!(event_before.action, IlmAction::NoneAction);

        // now is after the configured date — must schedule deletion
        let now_after = base_time + Duration::days(11);
        let event_after = lc.eval_inner(&opts, now_after, 0).await;
        assert_eq!(event_after.action, IlmAction::DeleteVersionAction);
        assert_eq!(event_after.due, Some(future_date));
    }

    // --- TASK-002 tests: Object Lock + ExpiredObjectDeleteMarker conflict ---

    #[tokio::test]
    #[serial]
    async fn validate_rejects_expired_object_delete_marker_on_locked_bucket() {
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

        let err = lc.validate(&locked_config).await.unwrap_err();
        assert_eq!(err.to_string(), ERR_LIFECYCLE_BUCKET_LOCKED);
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

    // --- TASK-003 tests: Midnight UTC truncation ---

    #[test]
    fn expected_expiry_time_truncates_to_midnight_utc() {
        // Object created at 2025-01-15T10:30:45Z, expire in 30 days
        let mod_time = datetime!(2025-01-15 10:30:45 UTC);
        let result = expected_expiry_time(mod_time, 30);

        // Should be truncated to midnight: 2025-02-14T00:00:00Z
        assert_eq!(result.hour(), 0);
        assert_eq!(result.minute(), 0);
        assert_eq!(result.second(), 0);
        assert_eq!(result, datetime!(2025-02-14 00:00:00 UTC));
    }

    #[test]
    fn expected_expiry_time_immediate_expiry_returns_epoch() {
        let mod_time = datetime!(2025-06-01 12:00:00 UTC);
        let result = expected_expiry_time(mod_time, 0);
        assert_eq!(result, OffsetDateTime::UNIX_EPOCH);
    }

    #[test]
    fn expected_expiry_time_truncates_already_midnight() {
        let mod_time = datetime!(2025-03-01 00:00:00 UTC);
        let result = expected_expiry_time(mod_time, 1);
        assert_eq!(result, datetime!(2025-03-02 00:00:00 UTC));
    }

    #[test]
    fn expected_expiry_time_truncates_end_of_day() {
        let mod_time = datetime!(2025-06-15 23:59:59 UTC);
        let result = expected_expiry_time(mod_time, 1);
        assert_eq!(result, datetime!(2025-06-16 00:00:00 UTC));
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
}
