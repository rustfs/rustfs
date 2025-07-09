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

use super::datatypes::{StatusType, VersionPurgeStatusType};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::time::Duration;
use time::OffsetDateTime;

/// Type - replication type enum
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ReplicationType {
    #[default]
    Unset,
    Object,
    Delete,
    Metadata,
    Heal,
    ExistingObject,
    Resync,
    All,
}

impl ReplicationType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ReplicationType::Unset => "",
            ReplicationType::Object => "OBJECT",
            ReplicationType::Delete => "DELETE",
            ReplicationType::Metadata => "METADATA",
            ReplicationType::Heal => "HEAL",
            ReplicationType::ExistingObject => "EXISTING_OBJECT",
            ReplicationType::Resync => "RESYNC",
            ReplicationType::All => "ALL",
        }
    }

    pub fn is_valid(&self) -> bool {
        matches!(
            self,
            ReplicationType::Object
                | ReplicationType::Delete
                | ReplicationType::Metadata
                | ReplicationType::Heal
                | ReplicationType::ExistingObject
                | ReplicationType::Resync
                | ReplicationType::All
        )
    }

    pub fn is_data_replication(&self) -> bool {
        matches!(self, ReplicationType::Object | ReplicationType::Delete | ReplicationType::Heal)
    }
}

impl fmt::Display for ReplicationType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<&str> for ReplicationType {
    fn from(s: &str) -> Self {
        match s {
            "UNSET" => ReplicationType::Unset,
            "OBJECT" => ReplicationType::Object,
            "DELETE" => ReplicationType::Delete,
            "METADATA" => ReplicationType::Metadata,
            "HEAL" => ReplicationType::Heal,
            "EXISTING_OBJECT" => ReplicationType::ExistingObject,
            "RESYNC" => ReplicationType::Resync,
            "ALL" => ReplicationType::All,
            _ => ReplicationType::Unset,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MRFReplicateEntry {
    #[serde(rename = "bucket")]
    pub bucket: String,

    #[serde(rename = "object")]
    pub object: String,

    #[serde(skip_serializing, skip_deserializing)]
    pub version_id: String,

    #[serde(rename = "retryCount")]
    pub retry_count: i32,

    #[serde(skip_serializing, skip_deserializing)]
    pub size: i64,
}

pub trait ReplicationWorkerOperation: Any + Send + Sync {
    fn to_mrf_entry(&self) -> MRFReplicateEntry;
    fn as_any(&self) -> &dyn Any;
}

/// ReplicationState represents internal replication state
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ReplicationState {
    pub replica_timestamp: Option<OffsetDateTime>,
    pub replica_status: StatusType,
    pub delete_marker: bool,
    pub replication_timestamp: Option<OffsetDateTime>,
    pub replication_status_internal: String,
    pub version_purge_status_internal: String,
    pub replicate_decision_str: String,
    pub targets: HashMap<String, StatusType>,
    pub purge_targets: HashMap<String, VersionPurgeStatusType>,
    pub reset_statuses_map: HashMap<String, String>,
}

impl ReplicationState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns true if replication state is identical for version purge statuses and replication statuses
    pub fn equal(&self, other: &ReplicationState) -> bool {
        self.replica_status == other.replica_status
            && self.replication_status_internal == other.replication_status_internal
            && self.version_purge_status_internal == other.version_purge_status_internal
    }

    /// Returns overall replication status for the object version being replicated
    pub fn composite_replication_status(&self) -> StatusType {
        if !self.replication_status_internal.is_empty() {
            match StatusType::from(self.replication_status_internal.as_str()) {
                StatusType::Pending | StatusType::Completed | StatusType::Failed | StatusType::Replica => {
                    return StatusType::from(self.replication_status_internal.as_str());
                }
                _ => {
                    let repl_status = get_composite_replication_status(&self.targets);

                    if self.replica_timestamp.is_none() {
                        return repl_status;
                    }

                    if repl_status == StatusType::Completed {
                        if let (Some(replica_timestamp), Some(replication_timestamp)) =
                            (self.replica_timestamp, self.replication_timestamp)
                        {
                            if replica_timestamp > replication_timestamp {
                                return self.replica_status.clone();
                            }
                        }
                    }

                    return repl_status;
                }
            }
        } else if self.replica_status != StatusType::default() {
            return self.replica_status.clone();
        }

        StatusType::default()
    }

    /// Returns overall replication purge status for the permanent delete being replicated
    pub fn composite_version_purge_status(&self) -> VersionPurgeStatusType {
        match VersionPurgeStatusType::from(self.version_purge_status_internal.as_str()) {
            VersionPurgeStatusType::Pending | VersionPurgeStatusType::Complete | VersionPurgeStatusType::Failed => {
                VersionPurgeStatusType::from(self.version_purge_status_internal.as_str())
            }
            _ => get_composite_version_purge_status(&self.purge_targets),
        }
    }

    /// Returns replicatedInfos struct initialized with the previous state of replication
    pub fn target_state(&self, arn: &str) -> ReplicatedTargetInfo {
        ReplicatedTargetInfo {
            arn: arn.to_string(),
            prev_replication_status: self.targets.get(arn).cloned().unwrap_or_default(),
            version_purge_status: self.purge_targets.get(arn).cloned().unwrap_or_default(),
            resync_timestamp: self.reset_statuses_map.get(arn).cloned().unwrap_or_default(),
            ..Default::default()
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ReplicationAction {
    /// Replicate all data
    All,
    /// Replicate only metadata
    Metadata,
    /// Do not replicate
    #[default]
    None,
}

impl ReplicationAction {
    /// Returns string representation of replication action
    pub fn as_str(&self) -> &'static str {
        match self {
            ReplicationAction::All => "all",
            ReplicationAction::Metadata => "metadata",
            ReplicationAction::None => "none",
        }
    }
}

impl fmt::Display for ReplicationAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<&str> for ReplicationAction {
    fn from(s: &str) -> Self {
        match s {
            "all" => ReplicationAction::All,
            "metadata" => ReplicationAction::Metadata,
            "none" => ReplicationAction::None,
            _ => ReplicationAction::None,
        }
    }
}

/// ReplicatedTargetInfo struct represents replication info on a target
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ReplicatedTargetInfo {
    pub arn: String,
    pub size: i64,
    pub duration: Duration,
    pub replication_action: ReplicationAction,
    pub op_type: ReplicationType,
    pub replication_status: StatusType,
    pub prev_replication_status: StatusType,
    pub version_purge_status: VersionPurgeStatusType,
    pub resync_timestamp: String,
    pub replication_resynced: bool,
    pub endpoint: String,
    pub secure: bool,
    pub error: Option<String>,
}

impl ReplicatedTargetInfo {
    /// Returns true for a target if arn is empty
    pub fn is_empty(&self) -> bool {
        self.arn.is_empty()
    }
}

pub fn get_composite_replication_status(targets: &HashMap<String, StatusType>) -> StatusType {
    if targets.is_empty() {
        return StatusType::Empty;
    }

    let mut completed = 0;
    for status in targets.values() {
        match status {
            StatusType::Failed => return StatusType::Failed,
            StatusType::Completed => completed += 1,
            _ => {}
        }
    }

    if completed == targets.len() {
        StatusType::Completed
    } else {
        StatusType::Pending
    }
}

pub fn get_composite_version_purge_status(targets: &HashMap<String, VersionPurgeStatusType>) -> VersionPurgeStatusType {
    if targets.is_empty() {
        return VersionPurgeStatusType::default();
    }

    let mut completed = 0;
    for status in targets.values() {
        match status {
            VersionPurgeStatusType::Failed => return VersionPurgeStatusType::Failed,
            VersionPurgeStatusType::Complete => completed += 1,
            _ => {}
        }
    }

    if completed == targets.len() {
        VersionPurgeStatusType::Complete
    } else {
        VersionPurgeStatusType::Pending
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ReplicateTargetDecision {
    pub replicate: bool,
    pub synchronous: bool,
    pub arn: String,
    pub id: String,
}

impl ReplicateTargetDecision {
    pub fn new(arn: String, replicate: bool, sync: bool) -> Self {
        Self {
            replicate,
            synchronous: sync,
            arn,
            id: String::new(),
        }
    }
}

impl fmt::Display for ReplicateTargetDecision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{};{};{};{}", self.replicate, self.synchronous, self.arn, self.id)
    }
}

/// ReplicateDecision represents replication decision for each target
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicateDecision {
    pub targets_map: HashMap<String, ReplicateTargetDecision>,
}

impl ReplicateDecision {
    pub fn new() -> Self {
        Self {
            targets_map: HashMap::new(),
        }
    }

    /// Returns true if at least one target qualifies for replication
    pub fn replicate_any(&self) -> bool {
        self.targets_map.values().any(|t| t.replicate)
    }

    /// Returns true if at least one target qualifies for synchronous replication
    pub fn is_synchronous(&self) -> bool {
        self.targets_map.values().any(|t| t.synchronous)
    }

    /// Updates ReplicateDecision with target's replication decision
    pub fn set(&mut self, target: ReplicateTargetDecision) {
        self.targets_map.insert(target.arn.clone(), target);
    }

    /// Returns a stringified representation of internal replication status with all targets marked as `PENDING`
    pub fn pending_status(&self) -> String {
        let mut result = String::new();
        for target in self.targets_map.values() {
            if target.replicate {
                result.push_str(&format!("{}={};", target.arn, StatusType::Pending.as_str()));
            }
        }
        result
    }
}

impl fmt::Display for ReplicateDecision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut result = String::new();
        for (key, value) in &self.targets_map {
            result.push_str(&format!("{key}={value},"));
        }
        write!(f, "{}", result.trim_end_matches(','))
    }
}

impl Default for ReplicateDecision {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ResyncTargetDecision {
    pub replicate: bool,
    pub reset_id: String,
    pub reset_before_date: Option<OffsetDateTime>,
}

/// ResyncDecision is a struct representing a map with target's individual resync decisions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResyncDecision {
    pub targets: HashMap<String, ResyncTargetDecision>,
}

impl ResyncDecision {
    pub fn new() -> Self {
        Self { targets: HashMap::new() }
    }

    /// Returns true if no targets with resync decision present
    pub fn is_empty(&self) -> bool {
        self.targets.is_empty()
    }

    pub fn must_resync(&self) -> bool {
        self.targets.values().any(|v| v.replicate)
    }

    pub fn must_resync_target(&self, tgt_arn: &str) -> bool {
        self.targets.get(tgt_arn).map(|v| v.replicate).unwrap_or(false)
    }
}

impl Default for ResyncDecision {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicateObjectInfo {
    pub name: String,
    pub size: i64,
    pub actual_size: i64,
    pub bucket: String,
    pub version_id: String,
    pub etag: String,
    pub mod_time: Option<OffsetDateTime>,
    pub replication_status: StatusType,
    pub replication_status_internal: String,
    pub delete_marker: bool,
    pub version_purge_status_internal: String,
    pub version_purge_status: VersionPurgeStatusType,
    pub replication_state: ReplicationState,
    pub op_type: ReplicationType,
    pub dsc: ReplicateDecision,
    pub existing_obj_resync: ResyncDecision,
    pub target_statuses: HashMap<String, StatusType>,
    pub target_purge_statuses: HashMap<String, VersionPurgeStatusType>,
    pub replication_timestamp: Option<OffsetDateTime>,
    pub ssec: bool,
    pub user_tags: HashMap<String, String>,
    pub checksum: Option<String>,
    pub retry_count: u32,
}

lazy_static::lazy_static! {
    static ref REPL_STATUS_REGEX: Regex = Regex::new(r"([^=].*?)=([^,].*?);").unwrap();
}

impl ReplicateObjectInfo {
    /// Returns replication status of a target
    pub fn target_replication_status(&self, arn: &str) -> StatusType {
        let captures = REPL_STATUS_REGEX.captures_iter(&self.replication_status_internal);
        for cap in captures {
            if cap.len() == 3 && &cap[1] == arn {
                return StatusType::from(&cap[2]);
            }
        }
        StatusType::default()
    }

    /// Returns the relevant info needed by MRF
    pub fn to_mrf_entry(&self) -> MRFReplicateEntry {
        MRFReplicateEntry {
            bucket: self.bucket.clone(),
            object: self.name.clone(),
            version_id: self.version_id.clone(),
            retry_count: self.retry_count as i32,
            size: self.size,
        }
    }
}
