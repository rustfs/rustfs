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
#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_assignments)]
#![allow(unused_must_use)]
#![allow(clippy::all)]

use crate::bucket::lifecycle::rule::TransitionOps;
use crate::store_api::ObjectInfo;
use rustfs_filemeta::{ReplicationStatusType, VersionPurgeStatusType};
use s3s::dto::{
    BucketLifecycleConfiguration, ExpirationStatus, LifecycleExpiration, LifecycleRule, NoncurrentVersionTransition,
    ObjectLockConfiguration, ObjectLockEnabled, RestoreRequest, Transition,
};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::env;
use std::fmt::Display;
use std::sync::Arc;
use time::macros::{datetime, offset};
use time::{self, Duration, OffsetDateTime};
use tracing::info;
use uuid::Uuid;

pub const TRANSITION_COMPLETE: &str = "complete";
pub const TRANSITION_PENDING: &str = "pending";
const ERR_LIFECYCLE_NO_RULE: &str = "Lifecycle configuration should have at least one rule";
const ERR_LIFECYCLE_DUPLICATE_ID: &str = "Rule ID must be unique. Found same ID for more than one rule";
const _ERR_XML_NOT_WELL_FORMED: &str =
    "The XML you provided was not well-formed or did not validate against our published schema";
const ERR_LIFECYCLE_BUCKET_LOCKED: &str =
    "ExpiredObjectAllVersions element and DelMarkerExpiration action cannot be used on an retention bucket";
const ERR_LIFECYCLE_TOO_MANY_RULES: &str = "Lifecycle configuration should have at most 1000 rules";

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
        /*self.validate_id()?;
        self.validate_status()?;
        self.validate_expiration()?;
        self.validate_noncurrent_expiration()?;
        self.validate_prefix_and_filter()?;
        self.validate_transition()?;
        self.validate_noncurrent_transition()?;
        if (!self.Filter.Tag.IsEmpty() || len(self.Filter.And.Tags) != 0) && !self.delmarker_expiration.Empty() {
          return errInvalidRuleDelMarkerExpiration
        }
        if !self.expiration.set && !self.transition.set && !self.noncurrent_version_expiration.set && !self.noncurrent_version_transitions.unwrap()[0].set && self.delmarker_expiration.Empty() {
          return errXMLNotWellFormed
        }*/
        Ok(())
    }
}

#[async_trait::async_trait]
pub trait Lifecycle {
    async fn has_transition(&self) -> bool;
    fn has_expiry(&self) -> bool;
    fn has_active_rules(&self, prefix: &str) -> bool;
    async fn validate(&self, lr: &ObjectLockConfiguration) -> Result<(), std::io::Error>;
    async fn filter_rules(&self, obj: &ObjectOpts) -> Option<Vec<LifecycleRule>>;
    async fn eval(&self, obj: &ObjectOpts) -> Event;
    async fn eval_inner(&self, obj: &ObjectOpts, now: OffsetDateTime, newer_noncurrent_versions: usize) -> Event;
    //fn set_prediction_headers(&self, w: http.ResponseWriter, obj: ObjectOpts);
    async fn noncurrent_versions_expiration_limit(self: Arc<Self>, obj: &ObjectOpts) -> Event;
}

#[async_trait::async_trait]
impl Lifecycle for BucketLifecycleConfiguration {
    async fn has_transition(&self) -> bool {
        for rule in self.rules.iter() {
            if !rule.transitions.is_none() {
                return true;
            }
        }
        false
    }

    fn has_expiry(&self) -> bool {
        for rule in self.rules.iter() {
            if !rule.expiration.is_none() || !rule.noncurrent_version_expiration.is_none() {
                return true;
            }
        }
        false
    }

    fn has_active_rules(&self, prefix: &str) -> bool {
        if self.rules.len() == 0 {
            return false;
        }
        for rule in self.rules.iter() {
            if rule.status.as_str() == ExpirationStatus::DISABLED {
                continue;
            }

            let rule_prefix = rule.prefix.as_ref().expect("err!");
            if prefix.len() > 0 && rule_prefix.len() > 0 && !prefix.starts_with(rule_prefix) && !rule_prefix.starts_with(&prefix)
            {
                continue;
            }

            let rule_noncurrent_version_expiration = rule.noncurrent_version_expiration.as_ref().expect("err!");
            if rule_noncurrent_version_expiration.noncurrent_days.expect("err!") > 0 {
                return true;
            }
            if rule_noncurrent_version_expiration.newer_noncurrent_versions.expect("err!") > 0 {
                return true;
            }
            if !rule.noncurrent_version_transitions.is_none() {
                return true;
            }
            let rule_expiration = rule.expiration.as_ref().expect("err!");
            if !rule_expiration.date.is_none()
                && OffsetDateTime::from(rule_expiration.date.clone().expect("err!")).unix_timestamp()
                    < OffsetDateTime::now_utc().unix_timestamp()
            {
                return true;
            }
            if !rule_expiration.date.is_none() {
                return true;
            }
            if rule_expiration.expired_object_delete_marker.expect("err!") {
                return true;
            }
            let rule_transitions: &[Transition] = &rule.transitions.as_ref().expect("err!");
            let rule_transitions_0 = rule_transitions[0].clone();
            if !rule_transitions_0.date.is_none()
                && OffsetDateTime::from(rule_transitions_0.date.expect("err!")).unix_timestamp()
                    < OffsetDateTime::now_utc().unix_timestamp()
            {
                return true;
            }
            if !rule.transitions.is_none() {
                return true;
            }
        }
        false
    }

    async fn validate(&self, lr: &ObjectLockConfiguration) -> Result<(), std::io::Error> {
        if self.rules.len() > 1000 {
            return Err(std::io::Error::other(ERR_LIFECYCLE_TOO_MANY_RULES));
        }
        if self.rules.len() == 0 {
            return Err(std::io::Error::other(ERR_LIFECYCLE_NO_RULE));
        }

        for r in &self.rules {
            r.validate()?;
            /*if let Some(object_lock_enabled) = lr.object_lock_enabled.as_ref() {
                if let Some(expiration) = r.expiration.as_ref() {
                    if let Some(expired_object_delete_marker) = expiration.expired_object_delete_marker {
                        if object_lock_enabled.as_str() == ObjectLockEnabled::ENABLED && (expired_object_delete_marker) {
                            return Err(std::io::Error::other(ERR_LIFECYCLE_BUCKET_LOCKED));
                        }
                    }
                        }
            }*/
        }
        for (i, _) in self.rules.iter().enumerate() {
            if i == self.rules.len() - 1 {
                break;
            }
            let other_rules = &self.rules[i + 1..];
            for other_rule in other_rules {
                if self.rules[i].id == other_rule.id {
                    return Err(std::io::Error::other(ERR_LIFECYCLE_DUPLICATE_ID));
                }
            }
        }
        Ok(())
    }

    async fn filter_rules(&self, obj: &ObjectOpts) -> Option<Vec<LifecycleRule>> {
        if obj.name == "" {
            return None;
        }
        let mut rules = Vec::<LifecycleRule>::new();
        for rule in self.rules.iter() {
            if rule.status.as_str() == ExpirationStatus::DISABLED {
                continue;
            }
            if let Some(prefix) = rule.prefix.clone() {
                if !obj.name.starts_with(prefix.as_str()) {
                    continue;
                }
            }
            /*if !rule.filter.test_tags(obj.user_tags) {
                continue;
            }*/
            //if !obj.delete_marker && !rule.filter.BySize(obj.size) {
            if !obj.delete_marker && false {
                continue;
            }
            rules.push(rule.clone());
        }
        Some(rules)
    }

    async fn eval(&self, obj: &ObjectOpts) -> Event {
        self.eval_inner(obj, OffsetDateTime::now_utc(), 0).await
    }

    async fn eval_inner(&self, obj: &ObjectOpts, now: OffsetDateTime, newer_noncurrent_versions: usize) -> Event {
        let mut events = Vec::<Event>::new();
        info!(
            "eval_inner: object={}, mod_time={:?}, now={:?}, is_latest={}, delete_marker={}",
            obj.name, obj.mod_time, now, obj.is_latest, obj.delete_marker
        );

        // Gracefully handle missing mod_time instead of panicking
        let mod_time = match obj.mod_time {
            Some(t) => t,
            None => {
                info!("eval_inner: mod_time is None for object={}, returning default event", obj.name);
                return Event::default();
            }
        };

        if mod_time.unix_timestamp() == 0 {
            info!("eval_inner: mod_time is 0, returning default event");
            return Event::default();
        }

        if let Some(restore_expires) = obj.restore_expires {
            if !restore_expires.unix_timestamp() == 0 && now.unix_timestamp() > restore_expires.unix_timestamp() {
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
        }

        if let Some(ref lc_rules) = self.filter_rules(obj).await {
            for rule in lc_rules.iter() {
                if obj.expired_object_deletemarker() {
                    if let Some(expiration) = rule.expiration.as_ref() {
                        if let Some(expired_object_delete_marker) = expiration.expired_object_delete_marker {
                            events.push(Event {
                                action: IlmAction::DeleteVersionAction,
                                rule_id: rule.id.clone().expect("err!"),
                                due: Some(now),
                                noncurrent_days: 0,
                                newer_noncurrent_versions: 0,
                                storage_class: "".into(),
                            });
                            break;
                        }

                        if let Some(days) = expiration.days {
                            let expected_expiry = expected_expiry_time(mod_time, days /*, date*/);
                            if now.unix_timestamp() >= expected_expiry.unix_timestamp() {
                                events.push(Event {
                                    action: IlmAction::DeleteVersionAction,
                                    rule_id: rule.id.clone().expect("err!"),
                                    due: Some(expected_expiry),
                                    noncurrent_days: 0,
                                    newer_noncurrent_versions: 0,
                                    storage_class: "".into(),
                                });
                                break;
                            }
                        }
                    }
                }

                if obj.is_latest {
                    if let Some(ref expiration) = rule.expiration {
                        if let Some(expired_object_delete_marker) = expiration.expired_object_delete_marker {
                            if obj.delete_marker && expired_object_delete_marker {
                                let due = expiration.next_due(obj);
                                if let Some(due) = due {
                                    if now.unix_timestamp() >= due.unix_timestamp() {
                                        events.push(Event {
                                            action: IlmAction::DelMarkerDeleteAllVersionsAction,
                                            rule_id: rule.id.clone().expect("err!"),
                                            due: Some(due),
                                            noncurrent_days: 0,
                                            newer_noncurrent_versions: 0,
                                            storage_class: "".into(),
                                        });
                                    }
                                }
                                continue;
                            }
                        }
                    }
                }

                if !obj.is_latest {
                    if let Some(ref noncurrent_version_expiration) = rule.noncurrent_version_expiration {
                        if let Some(newer_noncurrent_versions) = noncurrent_version_expiration.newer_noncurrent_versions {
                            if newer_noncurrent_versions > 0 {
                                continue;
                            }
                        }
                    }
                }

                if !obj.is_latest {
                    if let Some(ref noncurrent_version_expiration) = rule.noncurrent_version_expiration {
                        if let Some(noncurrent_days) = noncurrent_version_expiration.noncurrent_days {
                            if noncurrent_days != 0 {
                                if let Some(successor_mod_time) = obj.successor_mod_time {
                                    let expected_expiry = expected_expiry_time(successor_mod_time, noncurrent_days);
                                    if now.unix_timestamp() >= expected_expiry.unix_timestamp() {
                                        events.push(Event {
                                            action: IlmAction::DeleteVersionAction,
                                            rule_id: rule.id.clone().expect("err!"),
                                            due: Some(expected_expiry),
                                            noncurrent_days: 0,
                                            newer_noncurrent_versions: 0,
                                            storage_class: "".into(),
                                        });
                                    }
                                }
                            }
                        }
                    }
                }

                if !obj.is_latest {
                    if let Some(ref noncurrent_version_transitions) = rule.noncurrent_version_transitions {
                        if let Some(ref storage_class) = noncurrent_version_transitions[0].storage_class {
                            if storage_class.as_str() != "" && !obj.delete_marker && obj.transition_status != TRANSITION_COMPLETE
                            {
                                let due = rule.noncurrent_version_transitions.as_ref().unwrap()[0].next_due(obj);
                                if let Some(due0) = due {
                                    if now.unix_timestamp() == 0 || now.unix_timestamp() > due0.unix_timestamp() {
                                        events.push(Event {
                                            action: IlmAction::TransitionVersionAction,
                                            rule_id: rule.id.clone().expect("err!"),
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
                            }
                        }
                    }
                }

                info!(
                    "eval_inner: checking expiration condition - is_latest={}, delete_marker={}, version_id={:?}, condition_met={}",
                    obj.is_latest,
                    obj.delete_marker,
                    obj.version_id,
                    (obj.is_latest || obj.version_id.is_none_or(|v| v.is_nil())) && !obj.delete_marker
                );
                // Allow expiration for latest objects OR non-versioned objects (empty version_id)
                if (obj.is_latest || obj.version_id.is_none_or(|v| v.is_nil())) && !obj.delete_marker {
                    info!("eval_inner: entering expiration check");
                    if let Some(ref expiration) = rule.expiration {
                        if let Some(ref date) = expiration.date {
                            let date0 = OffsetDateTime::from(date.clone());
                            if date0.unix_timestamp() != 0 && (now.unix_timestamp() >= date0.unix_timestamp()) {
                                info!("eval_inner: expiration by date - date0={:?}", date0);
                                events.push(Event {
                                    action: IlmAction::DeleteAction,
                                    rule_id: rule.id.clone().expect("err!"),
                                    due: Some(date0),
                                    noncurrent_days: 0,
                                    newer_noncurrent_versions: 0,
                                    storage_class: "".into(),
                                });
                            }
                        } else if let Some(days) = expiration.days {
                            let expected_expiry: OffsetDateTime = expected_expiry_time(mod_time, days);
                            info!(
                                "eval_inner: expiration check - days={}, obj_time={:?}, expiry_time={:?}, now={:?}, should_expire={}",
                                days,
                                mod_time,
                                expected_expiry,
                                now,
                                now.unix_timestamp() > expected_expiry.unix_timestamp()
                            );
                            if now.unix_timestamp() >= expected_expiry.unix_timestamp() {
                                info!("eval_inner: object should expire, adding DeleteAction");
                                let mut event = Event {
                                    action: IlmAction::DeleteAction,
                                    rule_id: rule.id.clone().expect("err!"),
                                    due: Some(expected_expiry),
                                    noncurrent_days: 0,
                                    newer_noncurrent_versions: 0,
                                    storage_class: "".into(),
                                };
                                /*if rule.expiration.expect("err!").delete_all.val {
                                    event.action = IlmAction::DeleteAllVersionsAction
                                }*/
                                events.push(event);
                            }
                        } else {
                            info!("eval_inner: expiration.days is None");
                        }
                    } else {
                        info!("eval_inner: rule.expiration is None");
                    }

                    if obj.transition_status != TRANSITION_COMPLETE {
                        if let Some(ref transitions) = rule.transitions {
                            let due = transitions[0].next_due(obj);
                            if let Some(due0) = due {
                                if now.unix_timestamp() == 0 || now.unix_timestamp() > due0.unix_timestamp() {
                                    events.push(Event {
                                        action: IlmAction::TransitionAction,
                                        rule_id: rule.id.clone().expect("err!"),
                                        due,
                                        storage_class: transitions[0].storage_class.clone().expect("err!").as_str().to_string(),
                                        noncurrent_days: 0,
                                        newer_noncurrent_versions: 0,
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }

        if events.len() > 0 {
            events.sort_by(|a, b| {
                if now.unix_timestamp() > a.due.expect("err!").unix_timestamp()
                    && now.unix_timestamp() > b.due.expect("err").unix_timestamp()
                    || a.due.expect("err").unix_timestamp() == b.due.expect("err").unix_timestamp()
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

                if a.due.expect("err").unix_timestamp() < b.due.expect("err").unix_timestamp() {
                    return Ordering::Less;
                }
                return Ordering::Greater;
            });
            return events[0].clone();
        }

        Event::default()
    }

    async fn noncurrent_versions_expiration_limit(self: Arc<Self>, obj: &ObjectOpts) -> Event {
        if let Some(filter_rules) = self.filter_rules(obj).await {
            for rule in filter_rules.iter() {
                if let Some(ref noncurrent_version_expiration) = rule.noncurrent_version_expiration {
                    if let Some(newer_noncurrent_versions) = noncurrent_version_expiration.newer_noncurrent_versions {
                        if newer_noncurrent_versions == 0 {
                            continue;
                        }
                        return Event {
                            action: IlmAction::DeleteVersionAction,
                            rule_id: rule.id.clone().expect("err"),
                            noncurrent_days: noncurrent_version_expiration.noncurrent_days.expect("noncurrent_days err.") as u32,
                            newer_noncurrent_versions: newer_noncurrent_versions as usize,
                            due: Some(OffsetDateTime::UNIX_EPOCH),
                            storage_class: "".into(),
                        };
                    } else {
                        return Event {
                            action: IlmAction::DeleteVersionAction,
                            rule_id: rule.id.clone().expect("err"),
                            noncurrent_days: noncurrent_version_expiration.noncurrent_days.expect("noncurrent_days err.") as u32,
                            newer_noncurrent_versions: 0,
                            due: Some(OffsetDateTime::UNIX_EPOCH),
                            storage_class: "".into(),
                        };
                    }
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
        match self.days {
            Some(days) => Some(expected_expiry_time(obj.mod_time.unwrap(), days)),
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
        if !obj.is_latest || self.days.is_none() {
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
    let mut hour = 3600;
    if let Ok(env_ilm_hour) = env::var("_RUSTFS_ILM_PROCESS_TIME") {
        if let Ok(num_hour) = env_ilm_hour.parse::<usize>() {
            hour = num_hour;
        }
    }
    //t.Truncate(24 * hour)
    info!("expected_expiry_time: mod_time={:?}, days={}, result={:?}", mod_time, days, t);
    t
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
        self.delete_marker && self.num_versions == 1
    }

    pub fn from_object_info(oi: &ObjectInfo) -> Self {
        Self {
            name: oi.name.clone(),
            user_tags: oi.user_tags.clone(),
            mod_time: oi.mod_time,
            size: oi.size as usize,
            version_id: oi.version_id.clone(),
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
