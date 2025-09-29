use core::fmt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use time::OffsetDateTime;

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
