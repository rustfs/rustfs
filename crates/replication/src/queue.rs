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

use std::any::Any;

use rustfs_storage_api::DeletedObject;

use crate::{
    DeletedObjectReplicationInfo, MrfReplicateEntry, REPLICATE_EXISTING, REPLICATE_HEAL, REPLICATE_HEAL_DELETE,
    ReplicateObjectInfo, ReplicationStatusType, ReplicationType, ReplicationWorkerOperation, ResyncDecision,
    VersionPurgeStatusType,
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ReplicationQueueAdmission {
    #[default]
    Skipped,
    Queued,
    Missed,
}

impl ReplicationQueueAdmission {
    pub fn merge(&mut self, other: Self) {
        *self = match (*self, other) {
            (Self::Missed, _) | (_, Self::Missed) => Self::Missed,
            (Self::Queued, _) | (_, Self::Queued) => Self::Queued,
            (Self::Skipped, Self::Skipped) => Self::Skipped,
        };
    }
}

#[derive(Debug, Clone, Default)]
pub struct ReplicationHealQueueResult {
    pub object_info: ReplicateObjectInfo,
    pub admission: ReplicationQueueAdmission,
}

#[derive(Debug, Clone, Default)]
pub enum ReplicationHealQueueAction {
    #[default]
    Skip,
    QueueObject,
    QueueDelete(DeletedObjectReplicationInfo),
    QueueResyncDeletes(ReplicationHealResyncDeletes),
}

#[derive(Debug, Clone, Default)]
pub struct ReplicationHealResyncDeletes {
    pub delete_info: DeletedObjectReplicationInfo,
    pub existing_obj_resync: ResyncDecision,
}

impl ReplicationHealResyncDeletes {
    pub fn target_delete_infos(&self) -> impl Iterator<Item = DeletedObjectReplicationInfo> + '_ {
        self.existing_obj_resync
            .targets
            .iter()
            .filter(|(_, target)| target.replicate)
            .map(|(target_arn, target)| {
                let mut delete_info = self.delete_info.clone();
                delete_info.reset_id = target.reset_id.clone();
                delete_info.target_arn = target_arn.clone();
                delete_info
            })
    }
}

pub fn replication_heal_queue_action(roi: &mut ReplicateObjectInfo) -> ReplicationHealQueueAction {
    if !roi.dsc.replicate_any() {
        return ReplicationHealQueueAction::Skip;
    }

    if roi.replication_status == ReplicationStatusType::Completed
        && roi.version_purge_status.is_empty()
        && !roi.existing_obj_resync.must_resync()
    {
        return ReplicationHealQueueAction::Skip;
    }

    if roi.delete_marker || !roi.version_purge_status.is_empty() {
        let delete_info = heal_deleted_object_replication_info(roi);

        if is_pending_or_failed_object_heal(roi) || is_pending_or_failed_version_purge(roi) {
            return ReplicationHealQueueAction::QueueDelete(delete_info);
        }

        if roi.existing_obj_resync.must_resync()
            && (roi.replication_status == ReplicationStatusType::Completed || roi.replication_status.is_empty())
        {
            return ReplicationHealQueueAction::QueueResyncDeletes(ReplicationHealResyncDeletes {
                delete_info,
                existing_obj_resync: roi.existing_obj_resync.clone(),
            });
        }

        return ReplicationHealQueueAction::Skip;
    }

    if roi.existing_obj_resync.must_resync() {
        roi.op_type = ReplicationType::ExistingObject;
    }

    if is_pending_or_failed_object_heal(roi) {
        roi.event_type = REPLICATE_HEAL.to_string();
        return ReplicationHealQueueAction::QueueObject;
    }

    if roi.existing_obj_resync.must_resync() {
        roi.event_type = REPLICATE_EXISTING.to_string();
        return ReplicationHealQueueAction::QueueObject;
    }

    ReplicationHealQueueAction::Skip
}

fn heal_deleted_object_replication_info(roi: &ReplicateObjectInfo) -> DeletedObjectReplicationInfo {
    let (version_id, delete_marker_version_id) = if roi.version_purge_status.is_empty() {
        (None, roi.version_id)
    } else {
        (roi.version_id, None)
    };

    DeletedObjectReplicationInfo {
        delete_object: DeletedObject {
            object_name: roi.name.clone(),
            delete_marker_version_id,
            version_id,
            replication_state: roi.replication_state.clone(),
            delete_marker_mtime: roi.mod_time,
            delete_marker: roi.delete_marker,
            ..Default::default()
        },
        bucket: roi.bucket.clone(),
        op_type: ReplicationType::Heal,
        event_type: REPLICATE_HEAL_DELETE.to_string(),
        ..Default::default()
    }
}

fn is_pending_or_failed_object_heal(roi: &ReplicateObjectInfo) -> bool {
    matches!(roi.replication_status, ReplicationStatusType::Pending | ReplicationStatusType::Failed)
}

fn is_pending_or_failed_version_purge(roi: &ReplicateObjectInfo) -> bool {
    matches!(roi.version_purge_status, VersionPurgeStatusType::Pending | VersionPurgeStatusType::Failed)
}

#[derive(Debug, Clone, PartialEq)]
pub enum ReplicationPriority {
    Fast,
    Slow,
    Auto,
}

impl std::str::FromStr for ReplicationPriority {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "fast" => Ok(ReplicationPriority::Fast),
            "slow" => Ok(ReplicationPriority::Slow),
            "auto" => Ok(ReplicationPriority::Auto),
            _ => Ok(ReplicationPriority::Auto),
        }
    }
}

impl ReplicationPriority {
    pub fn as_str(&self) -> &'static str {
        match self {
            ReplicationPriority::Fast => "fast",
            ReplicationPriority::Slow => "slow",
            ReplicationPriority::Auto => "auto",
        }
    }
}

#[derive(Debug)]
pub enum ReplicationOperation {
    Object(Box<ReplicateObjectInfo>),
    Delete(Box<DeletedObjectReplicationInfo>),
}

impl ReplicationWorkerOperation for ReplicationOperation {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn to_mrf_entry(&self) -> MrfReplicateEntry {
        match self {
            ReplicationOperation::Object(obj) => obj.to_mrf_entry(),
            ReplicationOperation::Delete(del) => del.to_mrf_entry(),
        }
    }

    fn get_bucket(&self) -> &str {
        match self {
            ReplicationOperation::Object(obj) => obj.get_bucket(),
            ReplicationOperation::Delete(del) => del.get_bucket(),
        }
    }

    fn get_object(&self) -> &str {
        match self {
            ReplicationOperation::Object(obj) => obj.get_object(),
            ReplicationOperation::Delete(del) => del.get_object(),
        }
    }

    fn get_size(&self) -> i64 {
        match self {
            ReplicationOperation::Object(obj) => obj.get_size(),
            ReplicationOperation::Delete(del) => del.get_size(),
        }
    }

    fn is_delete_marker(&self) -> bool {
        match self {
            ReplicationOperation::Object(obj) => obj.is_delete_marker(),
            ReplicationOperation::Delete(del) => del.is_delete_marker(),
        }
    }

    fn get_op_type(&self) -> ReplicationType {
        match self {
            ReplicationOperation::Object(obj) => obj.get_op_type(),
            ReplicationOperation::Delete(del) => del.get_op_type(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use rustfs_storage_api::DeletedObject;
    use uuid::Uuid;

    use super::{ReplicationHealQueueAction, ReplicationOperation, ReplicationPriority, ReplicationQueueAdmission};
    use crate::{
        DeletedObjectReplicationInfo, REPLICATE_EXISTING, REPLICATE_HEAL, REPLICATE_HEAL_DELETE, ReplicateDecision,
        ReplicateObjectInfo, ReplicateTargetDecision, ReplicationStatusType, ReplicationType, ReplicationWorkerOperation,
        ResyncTargetDecision, VersionPurgeStatusType, replication_heal_queue_action,
    };

    #[test]
    fn replication_queue_admission_combines_target_results() {
        let mut admission = ReplicationQueueAdmission::Skipped;

        admission.merge(ReplicationQueueAdmission::Queued);
        assert_eq!(admission, ReplicationQueueAdmission::Queued);

        admission.merge(ReplicationQueueAdmission::Missed);
        assert_eq!(admission, ReplicationQueueAdmission::Missed);

        admission.merge(ReplicationQueueAdmission::Skipped);
        assert_eq!(admission, ReplicationQueueAdmission::Missed);
    }

    #[test]
    fn heal_queue_action_routes_failed_objects_to_heal_queue() {
        let mut roi = replicate_object_info(ReplicationStatusType::Failed);

        let action = replication_heal_queue_action(&mut roi);

        assert!(matches!(action, ReplicationHealQueueAction::QueueObject));
        assert_eq!(roi.event_type, REPLICATE_HEAL);
    }

    #[test]
    fn heal_queue_action_routes_existing_object_resync() {
        let mut roi = replicate_object_info(ReplicationStatusType::Completed);
        roi.existing_obj_resync.targets.insert(
            "arn:target".to_string(),
            ResyncTargetDecision {
                replicate: true,
                reset_id: "reset-1".to_string(),
                reset_before_date: None,
            },
        );

        let action = replication_heal_queue_action(&mut roi);

        assert!(matches!(action, ReplicationHealQueueAction::QueueObject));
        assert_eq!(roi.op_type, ReplicationType::ExistingObject);
        assert_eq!(roi.event_type, REPLICATE_EXISTING);
    }

    #[test]
    fn heal_queue_action_routes_pending_delete_marker() {
        let version_id = Uuid::new_v4();
        let mut roi = replicate_object_info(ReplicationStatusType::Pending);
        roi.delete_marker = true;
        roi.version_id = Some(version_id);

        let action = replication_heal_queue_action(&mut roi);

        let ReplicationHealQueueAction::QueueDelete(delete_info) = action else {
            panic!("expected delete queue action");
        };
        assert_eq!(delete_info.bucket, "bucket");
        assert_eq!(delete_info.event_type, REPLICATE_HEAL_DELETE);
        assert_eq!(delete_info.delete_object.object_name, "object");
        assert_eq!(delete_info.delete_object.delete_marker_version_id, Some(version_id));
        assert_eq!(delete_info.delete_object.version_id, None);
    }

    #[test]
    fn heal_queue_action_expands_resync_delete_targets() {
        let mut roi = replicate_object_info(ReplicationStatusType::Completed);
        roi.delete_marker = true;
        roi.existing_obj_resync.targets.insert(
            "arn:replicate".to_string(),
            ResyncTargetDecision {
                replicate: true,
                reset_id: "reset-1".to_string(),
                reset_before_date: None,
            },
        );
        roi.existing_obj_resync.targets.insert(
            "arn:skip".to_string(),
            ResyncTargetDecision {
                replicate: false,
                reset_id: "reset-2".to_string(),
                reset_before_date: None,
            },
        );

        let action = replication_heal_queue_action(&mut roi);

        let ReplicationHealQueueAction::QueueResyncDeletes(batch) = action else {
            panic!("expected resync delete queue action");
        };
        let target_deletes = batch.target_delete_infos().collect::<Vec<_>>();
        assert_eq!(target_deletes.len(), 1);
        assert_eq!(target_deletes[0].target_arn, "arn:replicate");
        assert_eq!(target_deletes[0].reset_id, "reset-1");
    }

    #[test]
    fn heal_queue_action_routes_pending_version_purge() {
        let version_id = Uuid::new_v4();
        let mut roi = replicate_object_info(ReplicationStatusType::Completed);
        roi.version_id = Some(version_id);
        roi.version_purge_status = VersionPurgeStatusType::Pending;

        let action = replication_heal_queue_action(&mut roi);

        let ReplicationHealQueueAction::QueueDelete(delete_info) = action else {
            panic!("expected version purge delete queue action");
        };
        assert_eq!(delete_info.delete_object.version_id, Some(version_id));
        assert_eq!(delete_info.delete_object.delete_marker_version_id, None);
    }

    #[test]
    fn replication_priority_parses_known_values_and_defaults_unknown() {
        assert_eq!(ReplicationPriority::from_str("fast"), Ok(ReplicationPriority::Fast));
        assert_eq!(ReplicationPriority::from_str("slow"), Ok(ReplicationPriority::Slow));
        assert_eq!(ReplicationPriority::from_str("auto"), Ok(ReplicationPriority::Auto));
        assert_eq!(ReplicationPriority::from_str("unexpected"), Ok(ReplicationPriority::Auto));
        assert_eq!(ReplicationPriority::Fast.as_str(), "fast");
        assert_eq!(ReplicationPriority::Slow.as_str(), "slow");
        assert_eq!(ReplicationPriority::Auto.as_str(), "auto");
    }

    #[test]
    fn replication_operation_delegates_delete_contract() {
        let info = DeletedObjectReplicationInfo {
            bucket: "bucket".to_string(),
            op_type: ReplicationType::Delete,
            delete_object: DeletedObject {
                object_name: "object".to_string(),
                delete_marker: true,
                ..Default::default()
            },
            ..Default::default()
        };
        let operation = ReplicationOperation::Delete(Box::new(info));

        assert_eq!(operation.get_bucket(), "bucket");
        assert_eq!(operation.get_object(), "object");
        assert_eq!(operation.get_op_type(), ReplicationType::Delete);
        assert!(operation.is_delete_marker());
    }

    #[test]
    fn replication_operation_delegates_object_contract() {
        let info = ReplicateObjectInfo {
            bucket: "bucket".to_string(),
            name: "object".to_string(),
            size: 128,
            op_type: ReplicationType::Object,
            ..Default::default()
        };
        let operation = ReplicationOperation::Object(Box::new(info));

        assert_eq!(operation.get_bucket(), "bucket");
        assert_eq!(operation.get_object(), "object");
        assert_eq!(operation.get_size(), 128);
        assert_eq!(operation.get_op_type(), ReplicationType::Object);
    }

    fn replicate_object_info(replication_status: ReplicationStatusType) -> ReplicateObjectInfo {
        let mut dsc = ReplicateDecision::new();
        dsc.set(ReplicateTargetDecision::new("arn:target".to_string(), true, false));

        ReplicateObjectInfo {
            bucket: "bucket".to_string(),
            name: "object".to_string(),
            replication_status,
            dsc,
            ..Default::default()
        }
    }
}
