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

use std::sync::Arc;

use s3s::dto::{BucketLifecycleConfiguration, ObjectLockConfiguration, ObjectLockEnabled};
use time::OffsetDateTime;
use tracing::info;

use super::object_lock_boundary;
use crate::bucket::lifecycle::lifecycle::{Event, Lifecycle, ObjectOpts};
use crate::bucket::lifecycle::replication_sink::{self, LifecycleReplicationConfig};
use rustfs_common::metrics::IlmAction;

const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_LIFECYCLE: &str = "lifecycle";
const EVENT_LIFECYCLE_VERSION_SCAN_SKIPPED: &str = "lifecycle_version_scan_skipped";

/// Evaluator - evaluates lifecycle policy on objects for the given lifecycle
/// configuration and lock retention configuration.
pub struct Evaluator {
    policy: Arc<BucketLifecycleConfiguration>,
    lock_retention: Option<Arc<ObjectLockConfiguration>>,
}

impl Evaluator {
    /// NewEvaluator - creates a new evaluator with the given lifecycle
    pub fn new(policy: Arc<BucketLifecycleConfiguration>) -> Self {
        Self {
            policy,
            lock_retention: None,
        }
    }

    /// WithLockRetention - sets the lock retention configuration for the evaluator
    pub fn with_lock_retention(mut self, lr: Option<Arc<ObjectLockConfiguration>>) -> Self {
        self.lock_retention = lr;
        self
    }

    /// WithReplicationConfig is retained for caller compatibility.
    /// Lifecycle replication guards are evaluated from per-object replication state.
    pub fn with_replication_config(self, _rcfg: Option<Arc<LifecycleReplicationConfig>>) -> Self {
        self
    }

    /// IsPendingReplication checks if the object is pending replication.
    pub fn is_pending_replication(&self, obj: &ObjectOpts) -> bool {
        replication_sink::has_pending_lifecycle_replication(obj)
    }

    fn any_version_has_pending_replication(&self, objs: &[ObjectOpts]) -> bool {
        objs.iter().any(|obj| self.is_pending_replication(obj))
    }

    /// IsObjectLocked checks if it is appropriate to remove an
    /// object according to locking configuration when this is lifecycle/bucket quota asking.
    /// Uses the common `is_object_locked_by_metadata` function for consistency.
    pub fn is_object_locked(&self, obj: &ObjectOpts) -> bool {
        // First check if object lock is enabled for this bucket
        if self.lock_retention.as_ref().is_none_or(|v| {
            v.object_lock_enabled
                .as_ref()
                .is_none_or(|v| v.as_str() != ObjectLockEnabled::ENABLED)
        }) {
            return false;
        }

        // Use the common function to check if the object is locked
        object_lock_boundary::is_object_locked_by_metadata(&obj.user_defined, obj.delete_marker)
    }

    /// eval will return a lifecycle event for each object in objs for a given time.
    async fn eval_inner(&self, objs: &[ObjectOpts], now: OffsetDateTime) -> Vec<Event> {
        let mut events = vec![Event::default(); objs.len()];
        let mut newer_noncurrent_versions = 0;

        'top_loop: {
            for (i, obj) in objs.iter().enumerate() {
                let mut event = self.policy.eval_inner(obj, now, newer_noncurrent_versions).await;
                if replication_sink::lifecycle_action_waits_for_replication(event.action) && self.is_pending_replication(obj) {
                    event = Event::default();
                }
                match event.action {
                    IlmAction::DeleteAllVersionsAction | IlmAction::DelMarkerDeleteAllVersionsAction => {
                        // Skip if bucket has object locking enabled; To prevent the
                        // possibility of violating an object retention on one of the
                        // noncurrent versions of this object.
                        if self.lock_retention.as_ref().is_some_and(|v| {
                            v.object_lock_enabled
                                .as_ref()
                                .is_some_and(|v| v.as_str() == ObjectLockEnabled::ENABLED)
                        }) || self.any_version_has_pending_replication(objs)
                        {
                            event = Event::default();
                        } else {
                            // No need to evaluate remaining versions' lifecycle
                            // events after DeleteAllVersionsAction*
                            events[i] = event;

                            info!(
                                event = EVENT_LIFECYCLE_VERSION_SCAN_SKIPPED,
                                component = LOG_COMPONENT_ECSTORE,
                                subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                                reason = "delete_all_versions_action",
                                action = ?events[i].action,
                                "Skipped remaining lifecycle version scan"
                            );

                            break 'top_loop;
                        }
                    }
                    IlmAction::DeleteVersionAction | IlmAction::DeleteRestoredVersionAction => {
                        // Defensive code, should never happen
                        if obj.version_id.is_none_or(|v| v.is_nil()) {
                            event.action = IlmAction::NoneAction;
                        }
                        if self.is_object_locked(obj) {
                            event = Event::default();
                        }
                    }
                    _ => {}
                }

                if !obj.is_latest {
                    match event.action {
                        IlmAction::DeleteVersionAction => {
                            // this noncurrent version will be expired, nothing to add
                        }
                        _ => {
                            // this noncurrent version will be spared
                            newer_noncurrent_versions += 1;
                        }
                    }
                }
                events[i] = event;
            }
        }
        events
    }

    /// Eval will return a lifecycle event for each object in objs
    pub async fn eval(&self, objs: &[ObjectOpts]) -> Result<Vec<Event>, std::io::Error> {
        if objs.is_empty() {
            return Ok(vec![]);
        }
        if objs.len() != objs[0].num_versions {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("number of versions mismatch, expected {}, got {}", objs[0].num_versions, objs.len()),
            ));
        }
        Ok(self.eval_inner(objs, OffsetDateTime::now_utc()).await)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rustfs_common::metrics::IlmAction;
    use s3s::dto::{
        BucketLifecycleConfiguration, ExpirationStatus, LifecycleExpiration, LifecycleRule, Transition, TransitionStorageClass,
    };
    use time::OffsetDateTime;
    use uuid::Uuid;

    use super::*;
    use crate::bucket::lifecycle::replication_sink::{ReplicationStatusType, VersionPurgeStatusType};
    fn expired_marker_lifecycle() -> Arc<BucketLifecycleConfiguration> {
        Arc::new(BucketLifecycleConfiguration {
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
                id: Some("expired-marker".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
            }],
        })
    }

    fn latest_expiration_lifecycle() -> Arc<BucketLifecycleConfiguration> {
        Arc::new(BucketLifecycleConfiguration {
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
                id: Some("expire-current".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
            }],
        })
    }

    fn latest_transition_lifecycle() -> Arc<BucketLifecycleConfiguration> {
        Arc::new(BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: None,
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: Some("transition-current".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: Some(vec![Transition {
                    days: Some(1),
                    date: None,
                    storage_class: Some(TransitionStorageClass::from_static("WARM")),
                }]),
            }],
        })
    }

    fn all_versions_expiration_lifecycle() -> Arc<BucketLifecycleConfiguration> {
        Arc::new(BucketLifecycleConfiguration {
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
                id: Some("delete-all".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
            }],
        })
    }

    fn object_opts(replication_status: ReplicationStatusType, version_purge_status: VersionPurgeStatusType) -> ObjectOpts {
        ObjectOpts {
            name: "logs/object".to_string(),
            mod_time: Some(OffsetDateTime::from_unix_timestamp(1_000_000).expect("valid fixed test timestamp")),
            version_id: Some(Uuid::new_v4()),
            is_latest: true,
            delete_marker: true,
            num_versions: 1,
            replication_status,
            version_purge_status,
            ..Default::default()
        }
    }

    fn current_object_opts(replication_status: ReplicationStatusType) -> ObjectOpts {
        ObjectOpts {
            name: "logs/object".to_string(),
            mod_time: Some(OffsetDateTime::from_unix_timestamp(1_000_000).expect("valid fixed test timestamp")),
            version_id: Some(Uuid::new_v4()),
            is_latest: true,
            num_versions: 1,
            replication_status,
            ..Default::default()
        }
    }

    fn versioned_object_opts(replication_status: ReplicationStatusType, is_latest: bool) -> ObjectOpts {
        ObjectOpts {
            num_versions: 2,
            is_latest,
            ..current_object_opts(replication_status)
        }
    }

    #[tokio::test]
    async fn evaluator_allows_expired_delete_marker_after_replication_completed() {
        let evaluator = Evaluator::new(expired_marker_lifecycle());

        let events = evaluator
            .eval(&[object_opts(
                ReplicationStatusType::Completed,
                VersionPurgeStatusType::Complete,
            )])
            .await
            .expect("completed replication should allow lifecycle evaluation");

        assert_eq!(events[0].action, IlmAction::DeleteVersionAction);
    }

    #[tokio::test]
    async fn evaluator_skips_expired_delete_marker_while_replication_pending() {
        let evaluator = Evaluator::new(expired_marker_lifecycle());

        let events = evaluator
            .eval(&[object_opts(ReplicationStatusType::Pending, VersionPurgeStatusType::default())])
            .await
            .expect("pending replication should still return a lifecycle decision");

        assert_eq!(events[0].action, IlmAction::NoneAction);
    }

    #[tokio::test]
    async fn evaluator_skips_expired_delete_marker_while_version_purge_pending() {
        let evaluator = Evaluator::new(expired_marker_lifecycle());

        let events = evaluator
            .eval(&[object_opts(ReplicationStatusType::Completed, VersionPurgeStatusType::Pending)])
            .await
            .expect("pending version purge should still return a lifecycle decision");

        assert_eq!(events[0].action, IlmAction::NoneAction);
    }

    #[tokio::test]
    async fn evaluator_skips_latest_expiration_while_replication_failed() {
        let evaluator = Evaluator::new(latest_expiration_lifecycle());

        let events = evaluator
            .eval(&[current_object_opts(ReplicationStatusType::Failed)])
            .await
            .expect("failed replication should still return a lifecycle decision");

        assert_eq!(events[0].action, IlmAction::NoneAction);
    }

    #[tokio::test]
    async fn evaluator_allows_latest_expiration_after_replication_completed() {
        let evaluator = Evaluator::new(latest_expiration_lifecycle());

        let events = evaluator
            .eval(&[current_object_opts(ReplicationStatusType::Completed)])
            .await
            .expect("completed replication should allow latest expiration");

        assert_eq!(events[0].action, IlmAction::DeleteAction);
    }

    #[tokio::test]
    async fn evaluator_skips_transition_while_replication_pending() {
        let evaluator = Evaluator::new(latest_transition_lifecycle());

        let events = evaluator
            .eval(&[current_object_opts(ReplicationStatusType::Pending)])
            .await
            .expect("pending replication should still return a lifecycle decision");

        assert_eq!(events[0].action, IlmAction::NoneAction);
    }

    #[tokio::test]
    async fn evaluator_allows_transition_after_replication_completed() {
        let evaluator = Evaluator::new(latest_transition_lifecycle());

        let events = evaluator
            .eval(&[current_object_opts(ReplicationStatusType::Completed)])
            .await
            .expect("completed replication should allow transition");

        assert_eq!(events[0].action, IlmAction::TransitionAction);
    }

    #[tokio::test]
    async fn evaluator_skips_delete_all_versions_when_any_version_replication_pending() {
        let evaluator = Evaluator::new(all_versions_expiration_lifecycle());
        let latest = versioned_object_opts(ReplicationStatusType::Completed, true);
        let noncurrent = versioned_object_opts(ReplicationStatusType::Pending, false);

        let events = evaluator
            .eval(&[latest, noncurrent])
            .await
            .expect("pending noncurrent replication should still return lifecycle decisions");

        assert_eq!(events[0].action, IlmAction::NoneAction);
        assert_eq!(events[1].action, IlmAction::NoneAction);
    }

    #[tokio::test]
    async fn evaluator_allows_delete_all_versions_when_all_versions_replication_completed() {
        let evaluator = Evaluator::new(all_versions_expiration_lifecycle());
        let latest = versioned_object_opts(ReplicationStatusType::Completed, true);
        let noncurrent = versioned_object_opts(ReplicationStatusType::Completed, false);

        let events = evaluator
            .eval(&[latest, noncurrent])
            .await
            .expect("completed replication should allow delete-all lifecycle decision");

        assert_eq!(events[0].action, IlmAction::DeleteAllVersionsAction);
        assert_eq!(events[1].action, IlmAction::NoneAction);
    }
}
