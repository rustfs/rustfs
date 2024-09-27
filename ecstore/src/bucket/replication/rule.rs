use super::filter::Filter;

#[derive(Debug)]
pub enum Status {
    Enabled,
    Disabled,
}

#[derive(Debug)]
pub struct DeleteMarkerReplication {
    pub status: Status,
}

#[derive(Debug)]
pub struct DeleteReplication {
    pub status: Status,
}

#[derive(Debug)]
pub struct ExistingObjectReplication {
    pub status: Status,
}

#[derive(Debug)]
pub struct Destination {
    pub bucket: String,
    pub storage_class: String,
    pub arn: String,
}

// 定义ReplicaModifications结构体
#[derive(Debug)]
pub struct ReplicaModifications {
    status: Status,
}

// 定义SourceSelectionCriteria结构体
#[derive(Debug)]
pub struct SourceSelectionCriteria {
    replica_modifications: ReplicaModifications,
}

#[derive(Debug)]
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
