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

use crate::{ReplicationState, ReplicationStatusType, VersionPurgeStatusType};
use time::OffsetDateTime;
use uuid::Uuid;

#[derive(Debug, Default, Clone)]
pub struct ObjectToDelete {
    pub object_name: String,
    pub version_id: Option<Uuid>,
    pub delete_marker_replication_status: Option<String>,
    pub version_purge_status: Option<VersionPurgeStatusType>,
    pub version_purge_statuses: Option<String>,
    pub replicate_decision_str: Option<String>,
}

impl ObjectToDelete {
    pub fn replication_state(&self) -> ReplicationState {
        ReplicationState {
            replication_status_internal: self.delete_marker_replication_status.clone(),
            version_purge_status_internal: self.version_purge_statuses.clone(),
            replicate_decision_str: self.replicate_decision_str.clone().unwrap_or_default(),
            targets: crate::replication_statuses_map(self.delete_marker_replication_status.as_deref().unwrap_or_default()),
            purge_targets: crate::version_purge_statuses_map(self.version_purge_statuses.as_deref().unwrap_or_default()),
            ..Default::default()
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct DeletedObject {
    pub delete_marker: bool,
    pub delete_marker_version_id: Option<Uuid>,
    pub object_name: String,
    pub version_id: Option<Uuid>,
    pub delete_marker_mtime: Option<OffsetDateTime>,
    pub replication_state: Option<ReplicationState>,
    pub found: bool,
    pub force_delete: bool,
}

impl DeletedObject {
    pub fn version_purge_status(&self) -> VersionPurgeStatusType {
        self.replication_state
            .as_ref()
            .map(|v| v.composite_version_purge_status())
            .unwrap_or(VersionPurgeStatusType::Empty)
    }

    pub fn delete_marker_replication_status(&self) -> ReplicationStatusType {
        self.replication_state
            .as_ref()
            .map(|v| v.composite_replication_status())
            .unwrap_or(ReplicationStatusType::Empty)
    }
}
