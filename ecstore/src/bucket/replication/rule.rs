use super::filter::Filter;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Default)]
pub enum Status {
    #[default]
    Enabled,
    Disabled,
}

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct DeleteMarkerReplication {
    pub status: Status,
}

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct DeleteReplication {
    pub status: Status,
}

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct ExistingObjectReplication {
    pub status: Status,
}

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct Destination {
    pub bucket: String,
    pub storage_class: String,
    pub arn: String,
}

// 定义ReplicaModifications结构体
#[derive(Debug, Deserialize, Serialize, Default)]
pub struct ReplicaModifications {
    status: Status,
}

// 定义SourceSelectionCriteria结构体
#[derive(Debug, Deserialize, Serialize, Default)]
pub struct SourceSelectionCriteria {
    replica_modifications: ReplicaModifications,
}

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct Rule {
    pub id: String,
    pub status: Status,
    pub priority: usize,
    pub delete_marker_replication: DeleteMarkerReplication,
    pub delete_replication: DeleteReplication,
    pub destination: Destination,
    pub source_selection_criteria: SourceSelectionCriteria,
    pub filter: Filter,
    pub existing_object_replication: ExistingObjectReplication,
}
