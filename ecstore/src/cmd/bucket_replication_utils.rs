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

use std::collections::HashMap;
use chrono::{DateTime, Utc};

// Representation of the replication status
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatusType {
    Pending,
    Completed,
    CompletedLegacy,
    Failed,
    Replica,
}

// Representation of version purge status type (customize as needed)
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionPurgeStatusType {
    Pending,
    Completed,
    Failed,
}

// ReplicationState struct definition
#[derive(Debug, Clone)]
pub struct ReplicationState {
    // Timestamp when the last replica update was received
    pub replica_time_stamp: DateTime<Utc>,

    // Replica status
    pub replica_status: StatusType,

    // Represents DeleteMarker replication state
    pub delete_marker: bool,

    // Timestamp when the last replication activity happened
    pub replication_time_stamp: DateTime<Utc>,

    // Stringified representation of all replication activity
    pub replication_status_internal: String,

    // Stringified representation of all version purge statuses
    // Example format: "arn1=PENDING;arn2=COMPLETED;"
    pub version_purge_status_internal: String,

    // Stringified representation of replication decision for each target
    pub replicate_decision_str: String,

    // Map of ARN -> replication status for ongoing replication activity
    pub targets: HashMap<String, StatusType>,

    // Map of ARN -> VersionPurgeStatus for all the targets
    pub purge_targets: HashMap<String, VersionPurgeStatusType>,

    // Map of ARN -> stringified reset id and timestamp for all the targets
    pub reset_statuses_map: HashMap<String, String>,
}