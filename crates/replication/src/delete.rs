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

use crate::{MrfOpKind, MrfReplicateEntry, ReplicationType, ReplicationWorkerOperation};

#[derive(Debug, Clone, Default)]
pub struct DeletedObjectReplicationInfo {
    pub delete_object: DeletedObject,
    pub bucket: String,
    pub event_type: String,
    pub op_type: ReplicationType,
    pub reset_id: String,
    pub target_arn: String,
}

impl ReplicationWorkerOperation for DeletedObjectReplicationInfo {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn to_mrf_entry(&self) -> MrfReplicateEntry {
        MrfReplicateEntry {
            bucket: self.bucket.clone(),
            object: self.delete_object.object_name.clone(),
            version_id: self.delete_object.version_id,
            retry_count: 0,
            size: 0,
            op: MrfOpKind::Delete,
            delete_marker_version_id: self.delete_object.delete_marker_version_id,
            delete_marker: self.delete_object.delete_marker,
        }
    }

    fn get_bucket(&self) -> &str {
        &self.bucket
    }

    fn get_object(&self) -> &str {
        &self.delete_object.object_name
    }

    fn get_size(&self) -> i64 {
        0
    }

    fn is_delete_marker(&self) -> bool {
        true
    }

    fn get_op_type(&self) -> ReplicationType {
        self.op_type
    }
}

#[cfg(test)]
mod tests {
    use super::DeletedObjectReplicationInfo;
    use crate::{MrfOpKind, ReplicationType, ReplicationWorkerOperation};
    use rustfs_storage_api::DeletedObject;
    use uuid::Uuid;

    #[test]
    fn deleted_object_replication_info_encodes_delete_mrf_entry() {
        let version_id = Uuid::new_v4();
        let delete_marker_version_id = Uuid::new_v4();
        let info = DeletedObjectReplicationInfo {
            bucket: "bucket".to_string(),
            op_type: ReplicationType::Delete,
            delete_object: DeletedObject {
                object_name: "object".to_string(),
                version_id: Some(version_id),
                delete_marker_version_id: Some(delete_marker_version_id),
                delete_marker: true,
                ..Default::default()
            },
            ..Default::default()
        };

        let entry = info.to_mrf_entry();

        assert_eq!(entry.bucket, "bucket");
        assert_eq!(entry.object, "object");
        assert_eq!(entry.version_id, Some(version_id));
        assert_eq!(entry.delete_marker_version_id, Some(delete_marker_version_id));
        assert_eq!(entry.op, MrfOpKind::Delete);
        assert!(entry.delete_marker);
        assert_eq!(info.get_object(), "object");
    }
}
