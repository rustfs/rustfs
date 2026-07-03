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
use tracing::debug;

const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_LIFECYCLE: &str = "lifecycle";
const EVENT_LIFECYCLE_CLEANUP_SKIPPED: &str = "lifecycle_cleanup_skipped";
const EVENT_LIFECYCLE_CLEANUP_FAILED: &str = "lifecycle_cleanup_failed";

use crate::bucket::lifecycle::lifecycle;
use crate::bucket::replication::{ReplicationLifecycleBridge, ReplicationState};
use crate::bucket::versioning::VersioningApi;
use crate::bucket::versioning_sys::BucketVersioningSys;
use crate::object_api::ObjectOptions;
use crate::storage_api_contracts::object::{ObjectOperations as _, ObjectToDelete};
use crate::store::ECStore;
use rustfs_lock::MAX_DELETE_LIST;

pub async fn delete_object_versions(api: &Arc<ECStore>, bucket: &str, to_del: &[ObjectToDelete], _lc_event: lifecycle::Event) {
    let version_suspended = match BucketVersioningSys::get(bucket).await {
        Ok(vc) => vc.suspended(),
        Err(err) => {
            debug!(
                event = EVENT_LIFECYCLE_CLEANUP_SKIPPED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                bucket,
                error = ?err,
                reason = "versioning_config_unavailable",
                "Skipped lifecycle noncurrent version cleanup"
            );
            return;
        }
    };
    let mut remaining = to_del;
    loop {
        let mut to_del = remaining;
        if to_del.len() > MAX_DELETE_LIST {
            remaining = &to_del[MAX_DELETE_LIST..];
            to_del = &to_del[..MAX_DELETE_LIST];
        } else {
            remaining = &[];
        }

        let mut replication_candidates: Vec<Option<ReplicationState>> = Vec::with_capacity(to_del.len());
        for object in to_del.iter() {
            let version_id = object.version_id.map(|vid| vid.to_string());
            let opts = ObjectOptions {
                version_id: version_id.clone(),
                versioned: true,
                version_suspended,
                ..Default::default()
            };
            let candidate = match api.get_object_info(bucket, &object.object_name, &opts).await {
                Ok(info) => {
                    let dsc = ReplicationLifecycleBridge::check_delete_replication(bucket, object, &info, &opts).await;
                    dsc.replicate_any()
                        .then(|| ReplicationLifecycleBridge::version_delete_replication_state(&dsc))
                }
                Err(err) => {
                    debug!(
                        event = EVENT_LIFECYCLE_CLEANUP_SKIPPED,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                        bucket,
                        object = %object.object_name,
                        version_id = ?version_id,
                        error = ?err,
                        reason = "object_info_unavailable",
                        "Skipped lifecycle delete replication scheduling"
                    );
                    None
                }
            };
            replication_candidates.push(candidate);
        }

        let (mut deleted_objs, errors) = api
            .delete_objects(
                bucket,
                to_del.to_vec(),
                ObjectOptions {
                    version_suspended,
                    ..Default::default()
                },
            )
            .await;

        for (i, deleted_obj) in deleted_objs.iter_mut().enumerate() {
            if errors.get(i).and_then(|err| err.as_ref()).is_some() {
                continue;
            }
            let Some(replication_state) = replication_candidates.get(i).and_then(|c| c.clone()) else {
                continue;
            };
            deleted_obj.replication_state = Some(replication_state);
            ReplicationLifecycleBridge::schedule_delete(bucket.to_string(), deleted_obj.clone()).await;
        }

        for (i, err) in errors.iter().enumerate() {
            if let Some(e) = err {
                let obj_name = to_del.get(i).map(|o| o.object_name.as_str()).unwrap_or("<unknown>");
                let vid = to_del
                    .get(i)
                    .and_then(|o| o.version_id)
                    .map(|v| v.to_string())
                    .unwrap_or_default();
                debug!(
                    event = EVENT_LIFECYCLE_CLEANUP_FAILED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                    bucket,
                    object = obj_name,
                    version_id = %vid,
                    error = ?e,
                    "Failed lifecycle noncurrent version cleanup"
                );
            }
        }
        if remaining.is_empty() {
            break;
        }
    }
}
