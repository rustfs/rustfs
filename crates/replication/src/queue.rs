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

use crate::{DeletedObjectReplicationInfo, MrfReplicateEntry, ReplicateObjectInfo, ReplicationType, ReplicationWorkerOperation};

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

    use super::{ReplicationOperation, ReplicationPriority, ReplicationQueueAdmission};
    use crate::{DeletedObjectReplicationInfo, ReplicateObjectInfo, ReplicationType, ReplicationWorkerOperation};

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
}
