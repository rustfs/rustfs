// Replication status type for x-amz-replication-status header
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatusType {
    Pending,
    Completed,
    CompletedLegacy,
    Failed,
    Replica,
}

impl StatusType {
    // Converts the enum variant to its string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            StatusType::Pending => "PENDING",
            StatusType::Completed => "COMPLETED",
            StatusType::CompletedLegacy => "COMPLETE",
            StatusType::Failed => "FAILED",
            StatusType::Replica => "REPLICA",
        }
    }

    // Checks if the status is empty (not set)
    pub fn is_empty(&self) -> bool {
        matches!(self, StatusType::Pending) // Adjust this as needed
    }
}
