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

use bytes::Bytes;
use core::fmt;
use regex::Regex;
use rustfs_utils::http::RESERVED_METADATA_PREFIX_LOWER;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::HashMap;
use std::sync::LazyLock;
use std::time::Duration;
use time::OffsetDateTime;
use uuid::Uuid;

pub const REPLICATION_RESET: &str = "replication-reset";
pub const REPLICATION_STATUS: &str = "replication-status";

// ReplicateQueued - replication being queued trail
pub const REPLICATE_QUEUED: &str = "replicate:queue";

// ReplicateExisting - audit trail for existing objects replication
pub const REPLICATE_EXISTING: &str = "replicate:existing";
// ReplicateExistingDelete - audit trail for delete replication triggered for existing delete markers
pub const REPLICATE_EXISTING_DELETE: &str = "replicate:existing:delete";

// ReplicateMRF - audit trail for replication from Most Recent Failures (MRF) queue
pub const REPLICATE_MRF: &str = "replicate:mrf";
// ReplicateIncoming - audit trail of inline replication
pub const REPLICATE_INCOMING: &str = "replicate:incoming";
// ReplicateIncomingDelete - audit trail of inline replication of deletes.
pub const REPLICATE_INCOMING_DELETE: &str = "replicate:incoming:delete";

// ReplicateHeal - audit trail for healing of failed/pending replications
pub const REPLICATE_HEAL: &str = "replicate:heal";
// ReplicateHealDelete - audit trail of healing of failed/pending delete replications.
pub const REPLICATE_HEAL_DELETE: &str = "replicate:heal:delete";

/// StatusType of Replication for x-amz-replication-status header
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default, Hash)]
pub enum ReplicationStatusType {
    /// Pending - replication is pending.
    Pending,
    /// Completed - replication completed ok.
    Completed,
    /// CompletedLegacy was called "COMPLETE" incorrectly.
    CompletedLegacy,
    /// Failed - replication failed.
    Failed,
    /// Replica - this is a replica.
    Replica,
    #[default]
    Empty,
}

impl ReplicationStatusType {
    /// Returns string representation of status
    pub fn as_str(&self) -> &'static str {
        match self {
            ReplicationStatusType::Pending => "PENDING",
            ReplicationStatusType::Completed => "COMPLETED",
            ReplicationStatusType::CompletedLegacy => "COMPLETE",
            ReplicationStatusType::Failed => "FAILED",
            ReplicationStatusType::Replica => "REPLICA",
            ReplicationStatusType::Empty => "",
        }
    }
    pub fn is_empty(&self) -> bool {
        matches!(self, ReplicationStatusType::Empty)
    }
}

impl fmt::Display for ReplicationStatusType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<&str> for ReplicationStatusType {
    fn from(s: &str) -> Self {
        match s {
            "PENDING" => ReplicationStatusType::Pending,
            "COMPLETED" => ReplicationStatusType::Completed,
            "COMPLETE" => ReplicationStatusType::CompletedLegacy,
            "FAILED" => ReplicationStatusType::Failed,
            "REPLICA" => ReplicationStatusType::Replica,
            _ => ReplicationStatusType::Empty,
        }
    }
}

impl From<VersionPurgeStatusType> for ReplicationStatusType {
    fn from(status: VersionPurgeStatusType) -> Self {
        match status {
            VersionPurgeStatusType::Pending => ReplicationStatusType::Pending,
            VersionPurgeStatusType::Complete => ReplicationStatusType::Completed,
            VersionPurgeStatusType::Failed => ReplicationStatusType::Failed,
            VersionPurgeStatusType::Empty => ReplicationStatusType::Empty,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum VersionPurgeStatusType {
    Pending,
    Complete,
    Failed,
    #[default]
    Empty,
}

impl VersionPurgeStatusType {
    /// Returns string representation of version purge status
    pub fn as_str(&self) -> &'static str {
        match self {
            VersionPurgeStatusType::Pending => "PENDING",
            VersionPurgeStatusType::Complete => "COMPLETE",
            VersionPurgeStatusType::Failed => "FAILED",
            VersionPurgeStatusType::Empty => "",
        }
    }

    /// Returns true if the version is pending purge.
    pub fn is_pending(&self) -> bool {
        matches!(self, VersionPurgeStatusType::Pending | VersionPurgeStatusType::Failed)
    }

    pub fn is_empty(&self) -> bool {
        matches!(self, VersionPurgeStatusType::Empty)
    }
}

impl fmt::Display for VersionPurgeStatusType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<&str> for VersionPurgeStatusType {
    fn from(s: &str) -> Self {
        match s {
            "PENDING" => VersionPurgeStatusType::Pending,
            "COMPLETE" => VersionPurgeStatusType::Complete,
            "FAILED" => VersionPurgeStatusType::Failed,
            _ => VersionPurgeStatusType::Empty,
        }
    }
}

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

/// ReplicationState represents internal replication state
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct ReplicationState {
    pub replica_timestamp: Option<OffsetDateTime>,
    pub replica_status: ReplicationStatusType,
    pub delete_marker: bool,
    pub replication_timestamp: Option<OffsetDateTime>,
    pub replication_status_internal: Option<String>,
    pub version_purge_status_internal: Option<String>,
    pub replicate_decision_str: String,
    pub targets: HashMap<String, ReplicationStatusType>,
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
    pub fn composite_replication_status(&self) -> ReplicationStatusType {
        if let Some(replication_status_internal) = &self.replication_status_internal {
            match ReplicationStatusType::from(replication_status_internal.as_str()) {
                ReplicationStatusType::Pending
                | ReplicationStatusType::Completed
                | ReplicationStatusType::Failed
                | ReplicationStatusType::Replica => {
                    return ReplicationStatusType::from(replication_status_internal.as_str());
                }
                _ => {
                    let repl_status = get_composite_replication_status(&self.targets);

                    if self.replica_timestamp.is_none() {
                        return repl_status;
                    }

                    if repl_status == ReplicationStatusType::Completed {
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
        } else if self.replica_status != ReplicationStatusType::default() {
            return self.replica_status.clone();
        }

        ReplicationStatusType::default()
    }

    /// Returns overall replication purge status for the permanent delete being replicated
    pub fn composite_version_purge_status(&self) -> VersionPurgeStatusType {
        match VersionPurgeStatusType::from(self.version_purge_status_internal.clone().unwrap_or_default().as_str()) {
            VersionPurgeStatusType::Pending | VersionPurgeStatusType::Complete | VersionPurgeStatusType::Failed => {
                VersionPurgeStatusType::from(self.version_purge_status_internal.clone().unwrap_or_default().as_str())
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

pub fn get_composite_replication_status(targets: &HashMap<String, ReplicationStatusType>) -> ReplicationStatusType {
    if targets.is_empty() {
        return ReplicationStatusType::Empty;
    }

    let mut completed = 0;
    for status in targets.values() {
        match status {
            ReplicationStatusType::Failed => return ReplicationStatusType::Failed,
            ReplicationStatusType::Completed => completed += 1,
            _ => {}
        }
    }

    if completed == targets.len() {
        ReplicationStatusType::Completed
    } else {
        ReplicationStatusType::Pending
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
    pub replication_status: ReplicationStatusType,
    pub prev_replication_status: ReplicationStatusType,
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

/// ReplicatedInfos struct contains replication information for multiple targets
#[derive(Debug, Clone)]
pub struct ReplicatedInfos {
    pub replication_timestamp: Option<OffsetDateTime>,
    pub targets: Vec<ReplicatedTargetInfo>,
}

impl ReplicatedInfos {
    /// Returns the total size of completed replications
    pub fn completed_size(&self) -> i64 {
        let mut sz = 0i64;
        for target in &self.targets {
            if target.is_empty() {
                continue;
            }
            if target.replication_status == ReplicationStatusType::Completed
                && target.prev_replication_status != ReplicationStatusType::Completed
            {
                sz += target.size;
            }
        }
        sz
    }

    /// Returns true if replication was attempted on any of the targets for the object version queued
    pub fn replication_resynced(&self) -> bool {
        for target in &self.targets {
            if target.is_empty() || !target.replication_resynced {
                continue;
            }
            return true;
        }
        false
    }

    /// Returns internal representation of replication status for all targets
    pub fn replication_status_internal(&self) -> Option<String> {
        let mut result = String::new();
        for target in &self.targets {
            if target.is_empty() {
                continue;
            }
            result.push_str(&format!("{}={};", target.arn, target.replication_status));
        }
        if result.is_empty() { None } else { Some(result) }
    }

    /// Returns overall replication status across all targets
    pub fn replication_status(&self) -> ReplicationStatusType {
        if self.targets.is_empty() {
            return ReplicationStatusType::Empty;
        }

        let mut completed = 0;
        for target in &self.targets {
            match target.replication_status {
                ReplicationStatusType::Failed => return ReplicationStatusType::Failed,
                ReplicationStatusType::Completed => completed += 1,
                _ => {}
            }
        }

        if completed == self.targets.len() {
            ReplicationStatusType::Completed
        } else {
            ReplicationStatusType::Pending
        }
    }

    /// Returns overall version purge status across all targets
    pub fn version_purge_status(&self) -> VersionPurgeStatusType {
        if self.targets.is_empty() {
            return VersionPurgeStatusType::Empty;
        }

        let mut completed = 0;
        for target in &self.targets {
            match target.version_purge_status {
                VersionPurgeStatusType::Failed => return VersionPurgeStatusType::Failed,
                VersionPurgeStatusType::Complete => completed += 1,
                _ => {}
            }
        }

        if completed == self.targets.len() {
            VersionPurgeStatusType::Complete
        } else {
            VersionPurgeStatusType::Pending
        }
    }

    /// Returns internal representation of version purge status for all targets
    pub fn version_purge_status_internal(&self) -> Option<String> {
        let mut result = String::new();
        for target in &self.targets {
            if target.is_empty() || target.version_purge_status.is_empty() {
                continue;
            }
            result.push_str(&format!("{}={};", target.arn, target.version_purge_status));
        }
        if result.is_empty() { None } else { Some(result) }
    }

    /// Returns replication action based on target that actually performed replication
    pub fn action(&self) -> ReplicationAction {
        for target in &self.targets {
            if target.is_empty() {
                continue;
            }
            // rely on replication action from target that actually performed replication now.
            if target.prev_replication_status != ReplicationStatusType::Completed {
                return target.replication_action;
            }
        }
        ReplicationAction::None
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MrfReplicateEntry {
    #[serde(rename = "bucket")]
    pub bucket: String,

    #[serde(rename = "object")]
    pub object: String,

    #[serde(skip_serializing, skip_deserializing)]
    pub version_id: Option<Uuid>,

    #[serde(rename = "retryCount")]
    pub retry_count: i32,

    #[serde(skip_serializing, skip_deserializing)]
    pub size: i64,
}

pub trait ReplicationWorkerOperation: Any + Send + Sync {
    fn to_mrf_entry(&self) -> MrfReplicateEntry;
    fn as_any(&self) -> &dyn Any;
    fn get_bucket(&self) -> &str;
    fn get_object(&self) -> &str;
    fn get_size(&self) -> i64;
    fn is_delete_marker(&self) -> bool;
    fn get_op_type(&self) -> ReplicationType;
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
    pub fn pending_status(&self) -> Option<String> {
        let mut result = String::new();
        for target in self.targets_map.values() {
            if target.replicate {
                result.push_str(&format!("{}={};", target.arn, ReplicationStatusType::Pending.as_str()));
            }
        }
        if result.is_empty() { None } else { Some(result) }
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

// parse k-v pairs of target ARN to stringified ReplicateTargetDecision delimited by ',' into a
// ReplicateDecision struct
pub fn parse_replicate_decision(_bucket: &str, s: &str) -> std::io::Result<ReplicateDecision> {
    let mut decision = ReplicateDecision::new();

    if s.is_empty() {
        return Ok(decision);
    }

    for p in s.split(',') {
        if p.is_empty() {
            continue;
        }

        let slc = p.split('=').collect::<Vec<&str>>();
        if slc.len() != 2 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("invalid replicate decision format: {s}"),
            ));
        }

        let tgt_str = slc[1].trim_matches('"');
        let tgt = tgt_str.split(';').collect::<Vec<&str>>();
        if tgt.len() != 4 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("invalid replicate decision format: {s}"),
            ));
        }

        let tgt = ReplicateTargetDecision {
            replicate: tgt[0] == "true",
            synchronous: tgt[1] == "true",
            arn: tgt[2].to_string(),
            id: tgt[3].to_string(),
        };
        decision.targets_map.insert(slc[0].to_string(), tgt);
    }

    Ok(decision)

    // r = ReplicateDecision{
    // 	targetsMap: make(map[string]replicateTargetDecision),
    // }
    // if len(s) == 0 {
    // 	return
    // }
    // for _, p := range strings.Split(s, ",") {
    // 	if p == "" {
    // 		continue
    // 	}
    // 	slc := strings.Split(p, "=")
    // 	if len(slc) != 2 {
    // 		return r, errInvalidReplicateDecisionFormat
    // 	}
    // 	tgtStr := strings.TrimSuffix(strings.TrimPrefix(slc[1], `"`), `"`)
    // 	tgt := strings.Split(tgtStr, ";")
    // 	if len(tgt) != 4 {
    // 		return r, errInvalidReplicateDecisionFormat
    // 	}
    // 	r.targetsMap[slc[0]] = replicateTargetDecision{Replicate: tgt[0] == "true", Synchronous: tgt[1] == "true", Arn: tgt[2], ID: tgt[3]}
    // }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicateObjectInfo {
    pub name: String,
    pub size: i64,
    pub actual_size: i64,
    pub bucket: String,
    pub version_id: Option<Uuid>,
    pub etag: Option<String>,
    pub mod_time: Option<OffsetDateTime>,
    pub replication_status: ReplicationStatusType,
    pub replication_status_internal: Option<String>,
    pub delete_marker: bool,
    pub version_purge_status_internal: Option<String>,
    pub version_purge_status: VersionPurgeStatusType,
    pub replication_state: Option<ReplicationState>,
    pub op_type: ReplicationType,
    pub event_type: String,
    pub dsc: ReplicateDecision,
    pub existing_obj_resync: ResyncDecision,
    pub target_statuses: HashMap<String, ReplicationStatusType>,
    pub target_purge_statuses: HashMap<String, VersionPurgeStatusType>,
    pub replication_timestamp: Option<OffsetDateTime>,
    pub ssec: bool,
    pub user_tags: String,
    pub checksum: Option<Bytes>,
    pub retry_count: u32,
}

impl ReplicationWorkerOperation for ReplicateObjectInfo {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn to_mrf_entry(&self) -> MrfReplicateEntry {
        MrfReplicateEntry {
            bucket: self.bucket.clone(),
            object: self.name.clone(),
            version_id: self.version_id,
            retry_count: self.retry_count as i32,
            size: self.size,
        }
    }

    fn get_bucket(&self) -> &str {
        &self.bucket
    }

    fn get_object(&self) -> &str {
        &self.name
    }

    fn get_size(&self) -> i64 {
        self.size
    }

    fn is_delete_marker(&self) -> bool {
        self.delete_marker
    }

    fn get_op_type(&self) -> ReplicationType {
        self.op_type
    }
}

static REPL_STATUS_REGEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"([^=].*?)=([^,].*?);").unwrap());

impl ReplicateObjectInfo {
    /// Returns replication status of a target
    pub fn target_replication_status(&self, arn: &str) -> ReplicationStatusType {
        let binding = self.replication_status_internal.clone().unwrap_or_default();
        let captures = REPL_STATUS_REGEX.captures_iter(&binding);
        for cap in captures {
            if cap.len() == 3 && &cap[1] == arn {
                return ReplicationStatusType::from(&cap[2]);
            }
        }
        ReplicationStatusType::default()
    }

    /// Returns the relevant info needed by MRF
    pub fn to_mrf_entry(&self) -> MrfReplicateEntry {
        MrfReplicateEntry {
            bucket: self.bucket.clone(),
            object: self.name.clone(),
            version_id: self.version_id,
            retry_count: self.retry_count as i32,
            size: self.size,
        }
    }
}

// constructs a replication status map from string representation
pub fn replication_statuses_map(s: &str) -> HashMap<String, ReplicationStatusType> {
    let mut targets = HashMap::new();
    let rep_stat_matches = REPL_STATUS_REGEX.captures_iter(s).map(|c| c.extract());
    for (_, [arn, status]) in rep_stat_matches {
        if arn.is_empty() {
            continue;
        }
        let status = ReplicationStatusType::from(status);
        targets.insert(arn.to_string(), status);
    }
    targets
}

// constructs a version purge status map from string representation
pub fn version_purge_statuses_map(s: &str) -> HashMap<String, VersionPurgeStatusType> {
    let mut targets = HashMap::new();
    let purge_status_matches = REPL_STATUS_REGEX.captures_iter(s).map(|c| c.extract());
    for (_, [arn, status]) in purge_status_matches {
        if arn.is_empty() {
            continue;
        }
        let status = VersionPurgeStatusType::from(status);
        targets.insert(arn.to_string(), status);
    }
    targets
}

pub fn get_replication_state(rinfos: &ReplicatedInfos, prev_state: &ReplicationState, _vid: Option<String>) -> ReplicationState {
    let reset_status_map: Vec<(String, String)> = rinfos
        .targets
        .iter()
        .filter(|v| !v.resync_timestamp.is_empty())
        .map(|t| (target_reset_header(t.arn.as_str()), t.resync_timestamp.clone()))
        .collect();

    let repl_statuses = rinfos.replication_status_internal();
    let vpurge_statuses = rinfos.version_purge_status_internal();

    let mut reset_statuses_map = prev_state.reset_statuses_map.clone();
    for (key, value) in reset_status_map {
        reset_statuses_map.insert(key, value);
    }

    ReplicationState {
        replicate_decision_str: prev_state.replicate_decision_str.clone(),
        reset_statuses_map,
        replica_timestamp: prev_state.replica_timestamp,
        replica_status: prev_state.replica_status.clone(),
        targets: replication_statuses_map(&repl_statuses.clone().unwrap_or_default()),
        replication_status_internal: repl_statuses,
        replication_timestamp: rinfos.replication_timestamp,
        purge_targets: version_purge_statuses_map(&vpurge_statuses.clone().unwrap_or_default()),
        version_purge_status_internal: vpurge_statuses,

        ..Default::default()
    }
}

pub fn target_reset_header(arn: &str) -> String {
    format!("{RESERVED_METADATA_PREFIX_LOWER}{REPLICATION_RESET}-{arn}")
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
