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

use crate::bucket::lifecycle::lifecycle::{Event, Lifecycle, ObjectOpts};
use crate::bucket::object_lock::objectlock_sys::is_object_locked_by_metadata;
use crate::bucket::replication::ReplicationConfig;
use rustfs_common::metrics::IlmAction;

/// Evaluator - evaluates lifecycle policy on objects for the given lifecycle
/// configuration, lock retention configuration and replication configuration.
pub struct Evaluator {
    policy: Arc<BucketLifecycleConfiguration>,
    lock_retention: Option<Arc<ObjectLockConfiguration>>,
    repl_cfg: Option<Arc<ReplicationConfig>>,
}

impl Evaluator {
    /// NewEvaluator - creates a new evaluator with the given lifecycle
    pub fn new(policy: Arc<BucketLifecycleConfiguration>) -> Self {
        Self {
            policy,
            lock_retention: None,
            repl_cfg: None,
        }
    }

    /// WithLockRetention - sets the lock retention configuration for the evaluator
    pub fn with_lock_retention(mut self, lr: Option<Arc<ObjectLockConfiguration>>) -> Self {
        self.lock_retention = lr;
        self
    }

    /// WithReplicationConfig - sets the replication configuration for the evaluator
    pub fn with_replication_config(mut self, rcfg: Option<Arc<ReplicationConfig>>) -> Self {
        self.repl_cfg = rcfg;
        self
    }

    /// IsPendingReplication checks if the object is pending replication.
    pub fn is_pending_replication(&self, obj: &ObjectOpts) -> bool {
        use crate::bucket::replication::ReplicationConfigurationExt;
        if self.repl_cfg.is_none() {
            return false;
        }
        if let Some(rcfg) = &self.repl_cfg
            && rcfg
                .config
                .as_ref()
                .is_some_and(|config| config.has_active_rules(obj.name.as_str(), true))
            && !obj.version_purge_status.is_empty()
        {
            return true;
        }
        false
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
        is_object_locked_by_metadata(&obj.user_defined, obj.delete_marker)
    }

    /// eval will return a lifecycle event for each object in objs for a given time.
    async fn eval_inner(&self, objs: &[ObjectOpts], now: OffsetDateTime) -> Vec<Event> {
        let mut events = vec![Event::default(); objs.len()];
        let mut newer_noncurrent_versions = 0;

        'top_loop: {
            for (i, obj) in objs.iter().enumerate() {
                let mut event = self.policy.eval_inner(obj, now, newer_noncurrent_versions).await;
                match event.action {
                    IlmAction::DeleteAllVersionsAction | IlmAction::DelMarkerDeleteAllVersionsAction => {
                        // Skip if bucket has object locking enabled; To prevent the
                        // possibility of violating an object retention on one of the
                        // noncurrent versions of this object.
                        if self.lock_retention.as_ref().is_some_and(|v| {
                            v.object_lock_enabled
                                .as_ref()
                                .is_some_and(|v| v.as_str() == ObjectLockEnabled::ENABLED)
                        }) {
                            event = Event::default();
                        } else {
                            // No need to evaluate remaining versions' lifecycle
                            // events after DeleteAllVersionsAction*
                            events[i] = event;

                            info!("eval_inner: skipping remaining versions' lifecycle events after DeleteAllVersionsAction*");

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

                        if self.is_pending_replication(obj) {
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
