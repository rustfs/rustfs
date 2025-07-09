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

use serde::{Deserialize, Serialize};
use std::fmt;

/// StatusType of Replication for x-amz-replication-status header
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default, Hash)]
pub enum StatusType {
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

impl StatusType {
    /// Returns string representation of status
    pub fn as_str(&self) -> &'static str {
        match self {
            StatusType::Pending => "PENDING",
            StatusType::Completed => "COMPLETED",
            StatusType::CompletedLegacy => "COMPLETE",
            StatusType::Failed => "FAILED",
            StatusType::Replica => "REPLICA",
            StatusType::Empty => "",
        }
    }
    pub fn is_empty(&self) -> bool {
        matches!(self, StatusType::Empty)
    }
}

impl fmt::Display for StatusType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<&str> for StatusType {
    fn from(s: &str) -> Self {
        match s {
            "PENDING" => StatusType::Pending,
            "COMPLETED" => StatusType::Completed,
            "COMPLETE" => StatusType::CompletedLegacy,
            "FAILED" => StatusType::Failed,
            "REPLICA" => StatusType::Replica,
            _ => StatusType::Empty,
        }
    }
}

impl From<VersionPurgeStatusType> for StatusType {
    fn from(status: VersionPurgeStatusType) -> Self {
        match status {
            VersionPurgeStatusType::Pending => StatusType::Pending,
            VersionPurgeStatusType::Complete => StatusType::Completed,
            VersionPurgeStatusType::Failed => StatusType::Failed,
            VersionPurgeStatusType::Empty => StatusType::Empty,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ResyncStatusType {
    #[default]
    NoResync,
    ResyncPending,
    ResyncCanceled,
    ResyncStarted,
    ResyncCompleted,
    ResyncFailed,
}

impl ResyncStatusType {
    pub fn is_valid(&self) -> bool {
        *self != ResyncStatusType::NoResync
    }
}

impl fmt::Display for ResyncStatusType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ResyncStatusType::ResyncStarted => "Ongoing",
            ResyncStatusType::ResyncCompleted => "Completed",
            ResyncStatusType::ResyncFailed => "Failed",
            ResyncStatusType::ResyncPending => "Pending",
            ResyncStatusType::ResyncCanceled => "Canceled",
            ResyncStatusType::NoResync => "",
        };
        write!(f, "{s}")
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
