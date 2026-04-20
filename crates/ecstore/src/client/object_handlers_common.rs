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
use tracing::warn;

use crate::bucket::lifecycle::lifecycle;
use crate::bucket::replication::{
    DeletedObjectReplicationInfo, check_replicate_delete, schedule_replication_delete,
};
use crate::bucket::versioning::VersioningApi;
use crate::bucket::versioning_sys::BucketVersioningSys;
use crate::store::ECStore;
use crate::store_api::{ObjectInfo, ObjectOperations, ObjectOptions, ObjectToDelete};
use rustfs_lock::MAX_DELETE_LIST;
use rustfs_filemeta::{
    REPLICATE_INCOMING_DELETE, ReplicationState, version_purge_statuses_map,
};

fn lifecycle_version_delete_replication_state(replicate_decision_str: String, pending_status: Option<String>) -> ReplicationState {
    ReplicationState {
        replicate_decision_str,
        version_purge_status_internal: pending_status.clone(),
        purge_targets: version_purge_statuses_map(pending_status.as_deref().unwrap_or_default()),
        ..Default::default()
    }
}

pub async fn delete_object_versions(api: &Arc<ECStore>, bucket: &str, to_del: &[ObjectToDelete], _lc_event: lifecycle::Event) {
    let version_suspended = match BucketVersioningSys::get(bucket).await {
        Ok(vc) => vc.suspended(),
        Err(err) => {
            warn!(bucket, error = ?err, "failed to get versioning config during lifecycle noncurrent version cleanup");
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

        let mut replication_candidates: Vec<Option<(ObjectInfo, ReplicationState)>> = Vec::with_capacity(to_del.len());
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
                    let dsc = check_replicate_delete(bucket, object, &info, &opts, None).await;
                    dsc.replicate_any().then(|| {
                        let state =
                            lifecycle_version_delete_replication_state(dsc.to_string(), dsc.pending_status());
                        (info, state)
                    })
                }
                Err(_) => None,
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
            let Some((_, replication_state)) = replication_candidates.get(i).and_then(|c| c.clone()) else {
                continue;
            };
            deleted_obj.replication_state = Some(replication_state);
            schedule_replication_delete(DeletedObjectReplicationInfo {
                delete_object: deleted_obj.clone(),
                bucket: bucket.to_string(),
                event_type: REPLICATE_INCOMING_DELETE.to_string(),
                ..Default::default()
            })
            .await;
        }

        for (i, err) in errors.iter().enumerate() {
            if let Some(e) = err {
                let obj_name = to_del.get(i).map(|o| o.object_name.as_str()).unwrap_or("<unknown>");
                let vid = to_del
                    .get(i)
                    .and_then(|o| o.version_id)
                    .map(|v| v.to_string())
                    .unwrap_or_default();
                warn!(bucket, object = obj_name, version_id = %vid, error = ?e, "failed to delete noncurrent version during lifecycle cleanup");
            }
        }
        if remaining.is_empty() {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::lifecycle_version_delete_replication_state;

    #[test]
    fn lifecycle_version_delete_replication_state_tracks_pending_purge_targets() {
        let state = lifecycle_version_delete_replication_state(
            "arn:aws:s3:::target=true;false;arn:aws:s3:::target;".to_string(),
            Some("arn:aws:s3:::target=PENDING;".to_string()),
        );

        assert_eq!(
            state.version_purge_status_internal.as_deref(),
            Some("arn:aws:s3:::target=PENDING;")
        );
        assert!(state.purge_targets.contains_key("arn:aws:s3:::target"));
        assert_eq!(
            state.replicate_decision_str,
            "arn:aws:s3:::target=true;false;arn:aws:s3:::target;"
        );
    }
}
