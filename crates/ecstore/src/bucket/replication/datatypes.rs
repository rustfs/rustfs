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
