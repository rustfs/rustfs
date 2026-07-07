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

use crate::storage_api::DeletedObject;
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
            // Persist the original delete-marker mtime as Unix nanoseconds so replay after a
            // restart stamps the replica with the source timestamp rather than the replay time
            // (backlog#867). None when unknown; replay then falls back to the current time.
            delete_marker_mtime: self
                .delete_object
                .delete_marker_mtime
                .and_then(|t| i64::try_from(t.unix_timestamp_nanos()).ok()),
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

pub fn is_version_delete_replication(dobj: &DeletedObject) -> bool {
    dobj.version_id.is_some() || (dobj.delete_marker_version_id.is_some() && !dobj.delete_marker)
}

pub fn should_retry_delete_marker_purge(dobj: &DeletedObject) -> bool {
    dobj.delete_marker_version_id.is_some()
}

pub fn is_retryable_delete_replication_head_error(is_not_found: bool, code: Option<&str>) -> bool {
    !(is_not_found || matches!(code, Some("MethodNotAllowed" | "405")))
}

#[cfg(test)]
mod tests {
    use super::{
        DeletedObjectReplicationInfo, is_retryable_delete_replication_head_error, is_version_delete_replication,
        should_retry_delete_marker_purge,
    };
    use crate::storage_api::DeletedObject;
    use crate::{MrfOpKind, ReplicationType, ReplicationWorkerOperation};
    use uuid::Uuid;

    #[test]
    fn deleted_object_replication_info_encodes_delete_mrf_entry() {
        let version_id = Uuid::new_v4();
        let delete_marker_version_id = Uuid::new_v4();
        let mtime = time::OffsetDateTime::from_unix_timestamp_nanos(1_705_312_200_123_456_789).expect("valid mtime");
        let info = DeletedObjectReplicationInfo {
            bucket: "bucket".to_string(),
            op_type: ReplicationType::Delete,
            delete_object: DeletedObject {
                object_name: "object".to_string(),
                version_id: Some(version_id),
                delete_marker_version_id: Some(delete_marker_version_id),
                delete_marker: true,
                delete_marker_mtime: Some(mtime),
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
        // The original mtime must be persisted (as Unix nanos) so replay keeps the source
        // timestamp instead of stamping the replica with the replay time (backlog#867).
        assert_eq!(
            entry.delete_marker_mtime,
            Some(mtime.unix_timestamp_nanos() as i64),
            "delete-marker mtime must be persisted in the MRF entry"
        );
        assert_eq!(info.get_object(), "object");
    }

    #[test]
    fn deleted_object_replication_info_without_mtime_yields_none() {
        // Absent source mtime must persist as None so replay falls back to the current time,
        // preserving pre-#867 behaviour.
        let info = DeletedObjectReplicationInfo {
            bucket: "bucket".to_string(),
            delete_object: DeletedObject {
                object_name: "object".to_string(),
                delete_marker: true,
                delete_marker_mtime: None,
                ..Default::default()
            },
            ..Default::default()
        };

        assert_eq!(info.to_mrf_entry().delete_marker_mtime, None);
    }

    #[test]
    fn version_delete_replication_tracks_delete_marker_version_purge() {
        let dobj = DeletedObject {
            delete_marker: false,
            delete_marker_version_id: Some(Uuid::new_v4()),
            ..Default::default()
        };

        assert!(is_version_delete_replication(&dobj));
    }

    #[test]
    fn version_delete_replication_tracks_explicit_version_id() {
        let dobj = DeletedObject {
            version_id: Some(Uuid::new_v4()),
            ..Default::default()
        };

        assert!(is_version_delete_replication(&dobj));
    }

    #[test]
    fn version_delete_replication_keeps_delete_marker_creation_separate() {
        let dobj = DeletedObject {
            delete_marker: true,
            delete_marker_version_id: Some(Uuid::new_v4()),
            ..Default::default()
        };

        assert!(!is_version_delete_replication(&dobj));
    }

    #[test]
    fn delete_marker_purge_retry_covers_version_purge_and_marker_creation() {
        let version_purge = DeletedObject {
            delete_marker: false,
            delete_marker_version_id: Some(Uuid::new_v4()),
            ..Default::default()
        };
        let marker_creation = DeletedObject {
            delete_marker: true,
            delete_marker_version_id: Some(Uuid::new_v4()),
            ..Default::default()
        };

        assert!(should_retry_delete_marker_purge(&version_purge));
        assert!(should_retry_delete_marker_purge(&marker_creation));

        let marker_without_version = DeletedObject {
            delete_marker: true,
            ..Default::default()
        };
        assert!(!should_retry_delete_marker_purge(&marker_without_version));
    }

    #[test]
    fn retryable_delete_replication_head_error_allows_expected_delete_marker_responses() {
        assert!(!is_retryable_delete_replication_head_error(false, Some("405")));
        assert!(!is_retryable_delete_replication_head_error(false, Some("MethodNotAllowed")));
        assert!(!is_retryable_delete_replication_head_error(true, Some("NoSuchKey")));
        assert!(is_retryable_delete_replication_head_error(false, Some("AccessDenied")));
    }
}
