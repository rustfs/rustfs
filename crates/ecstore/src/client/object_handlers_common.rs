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
use crate::bucket::replication::{ReplicationLifecycleBridge, ReplicationObjectBridge};
use crate::object_api::ObjectOptions;
use crate::storage_api_contracts::object::{ObjectOperations as _, ObjectToDelete};
use crate::store::ECStore;
use rustfs_lock::MAX_DELETE_LIST;

pub async fn delete_object_versions(api: &Arc<ECStore>, bucket: &str, to_del: &[ObjectToDelete], _lc_event: lifecycle::Event) {
    let delete_config_snapshot = match ReplicationObjectBridge::delete_request_config(api, bucket).await {
        Ok(snapshot) => Arc::new(snapshot),
        Err(err) => {
            debug!(
                event = EVENT_LIFECYCLE_CLEANUP_SKIPPED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                bucket,
                error = ?err,
                reason = "delete_config_snapshot_unavailable",
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

        let (mut deleted_objs, errors) = api
            .delete_objects(
                bucket,
                to_del.to_vec(),
                ObjectOptions {
                    delete_replication_config_snapshot: Some(Arc::clone(&delete_config_snapshot)),
                    ..Default::default()
                },
            )
            .await;

        for (i, deleted_obj) in deleted_objs.iter_mut().enumerate() {
            if errors.get(i).and_then(|err| err.as_ref()).is_some() {
                continue;
            }
            // Evict any cached body for the successfully deleted noncurrent
            // version so it does not sit resident until TTL (ODC-26).
            if let Some(target) = to_del.get(i) {
                crate::object_api::notify_object_mutation(bucket, &target.object_name).await;
            }
            if deleted_obj.replication_state.is_none() {
                continue;
            }
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
