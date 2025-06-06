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