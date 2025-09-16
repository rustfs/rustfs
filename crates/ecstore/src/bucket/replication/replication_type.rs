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

use crate::error::{Error, Result};
use crate::store_api::ObjectInfo;

use regex::Regex;

use rustfs_filemeta::VersionPurgeStatusType;
use rustfs_filemeta::{ReplicatedInfos, ReplicationType};
use rustfs_filemeta::{ReplicationState, ReplicationStatusType};
use rustfs_utils::http::RESERVED_METADATA_PREFIX_LOWER;
use rustfs_utils::http::RUSTFS_REPLICATION_RESET_STATUS;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
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
pub fn parse_replicate_decision(_bucket: &str, s: &str) -> Result<ReplicateDecision> {
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
            return Err(Error::other(format!("invalid replicate decision format: {s}")));
        }

        let tgt_str = slc[1].trim_matches('"');
        let tgt = tgt_str.split(';').collect::<Vec<&str>>();
        if tgt.len() != 4 {
            return Err(Error::other(format!("invalid replicate decision format: {s}")));
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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ResyncTargetDecision {
    pub replicate: bool,
    pub reset_id: String,
    pub reset_before_date: Option<OffsetDateTime>,
}

pub fn target_reset_header(arn: &str) -> String {
    format!("{RESERVED_METADATA_PREFIX_LOWER}{REPLICATION_RESET}-{arn}")
}

impl ResyncTargetDecision {
    pub fn resync_target(
        oi: &ObjectInfo,
        arn: &str,
        reset_id: &str,
        reset_before_date: Option<OffsetDateTime>,
        status: ReplicationStatusType,
    ) -> Self {
        let rs = oi
            .user_defined
            .get(target_reset_header(arn).as_str())
            .or(oi.user_defined.get(RUSTFS_REPLICATION_RESET_STATUS))
            .map(|s| s.to_string());

        let mut dec = Self::default();

        let mod_time = oi.mod_time.unwrap_or(OffsetDateTime::UNIX_EPOCH);

        if rs.is_none() {
            let reset_before_date = reset_before_date.unwrap_or(OffsetDateTime::UNIX_EPOCH);
            if !reset_id.is_empty() && mod_time < reset_before_date {
                dec.replicate = true;
                return dec;
            }

            dec.replicate = status == ReplicationStatusType::Empty;

            return dec;
        }

        if reset_id.is_empty() || reset_before_date.is_none() {
            return dec;
        }

        let rs = rs.unwrap();
        let reset_before_date = reset_before_date.unwrap();

        let parts: Vec<&str> = rs.splitn(2, ';').collect();

        if parts.len() != 2 {
            return dec;
        }

        let new_reset = parts[0] == reset_id;

        if !new_reset && status == ReplicationStatusType::Completed {
            return dec;
        }

        dec.replicate = new_reset && mod_time < reset_before_date;

        dec
    }
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
    pub checksum: Vec<u8>,
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

lazy_static::lazy_static! {
    static ref REPL_STATUS_REGEX: Regex = Regex::new(r"([^=].*?)=([^,].*?);").unwrap();
}

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
