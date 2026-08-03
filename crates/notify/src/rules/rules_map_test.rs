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

//! Regressions for rule matching against the KMS event namespace.
//!
//! KMS events (backlog#1583) are appended to `EventName` for the audit sink,
//! but they must stay invisible to bucket notification rules: a subscriber
//! asking for `s3:ObjectCreated:*` — or for everything — must never receive
//! key management activity.

use super::RulesMap;
use rustfs_s3_types::EventName;
use rustfs_targets::arn::TargetID;

const KMS_EVENTS: &[EventName] = &[
    EventName::KmsKeyCreated,
    EventName::KmsKeyRotated,
    EventName::KmsKeyEnabled,
    EventName::KmsKeyDisabled,
    EventName::KmsKeyDeletionScheduled,
    EventName::KmsKeyDeletionCancelled,
    EventName::KmsKeyDeleted,
    EventName::KmsKeyAccessed,
    EventName::KmsServiceConfigured,
    EventName::KmsServiceStarted,
    EventName::KmsServiceStopped,
];

fn test_target() -> TargetID {
    TargetID::new("primary".to_string(), "webhook".to_string())
}

fn rules_for(events: &[EventName]) -> RulesMap {
    let mut rules = RulesMap::new();
    rules.add_rule_config(events, String::new(), test_target());
    rules
}

#[test]
fn s3_wildcard_subscriptions_do_not_match_kms_events() {
    let rules = rules_for(&[
        EventName::ObjectCreatedAll,
        EventName::ObjectAccessedAll,
        EventName::ObjectRemovedAll,
        EventName::ObjectTaggingAll,
        EventName::ObjectReplicationAll,
        EventName::ObjectRestoreAll,
        EventName::ObjectTransitionAll,
        EventName::LifecycleExpirationAll,
        EventName::ObjectScannerAll,
    ]);

    for event in KMS_EVENTS {
        assert!(!rules.has_subscriber(event), "{event} must not have an S3 wildcard subscriber");
        assert!(
            rules.match_rules(*event, "any/object").is_empty(),
            "{event} must not match any S3 wildcard rule"
        );
    }
}

#[test]
fn everything_subscription_does_not_match_kms_events() {
    let rules = rules_for(&[EventName::Everything]);

    // Sanity: the catch-all really is subscribed to the S3 surface.
    assert!(rules.has_subscriber(&EventName::ObjectCreatedPut));

    for event in KMS_EVENTS {
        assert!(!rules.has_subscriber(event), "{event} must stay outside the s3 catch-all");
        assert!(
            rules.match_rules(*event, "any/object").is_empty(),
            "{event} must not match the s3 catch-all rule"
        );
    }
}

#[test]
fn kms_subscription_does_not_leak_into_s3_matching() {
    // The inverse direction: even an explicit KMS subscription must not widen
    // the mask so that S3 events start matching a KMS-only rule.
    let rules = rules_for(KMS_EVENTS);

    for event in [
        EventName::ObjectCreatedPut,
        EventName::ObjectRemovedDelete,
        EventName::ObjectAccessedGet,
        EventName::BucketCreated,
    ] {
        assert!(!rules.has_subscriber(&event), "{event} must not match a KMS-only rule");
        assert!(
            rules.match_rules(event, "any/object").is_empty(),
            "{event} must not match a KMS-only rule"
        );
    }
}
