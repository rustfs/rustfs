// Copyright 2026 RustFS Team
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

//! Typed object results shared by producers and the heal executor.

use serde::{Deserialize, Serialize};
use std::time::SystemTime;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealObjectKind {
    Object,
    Metadata,
    Decode,
    DeleteMarkerPurge,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HealObjectIdentity {
    pub kind: HealObjectKind,
    pub bucket: String,
    pub object: String,
    /// The requested version; None remains unresolved, never an absence proof.
    pub version_id: Option<String>,
    pub bucket_incarnation_id: Option<Uuid>,
    pub pool_index: Option<usize>,
    pub set_index: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealDeferredReason {
    DanglingDeleteGrace,
    RetiredMarkerProof,
    TransientUsageCache,
    TransientExistenceCheck,
    Deadline,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealFailureClass {
    Recoverable,
    RetryExhausted,
    Permanent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "state",
    content = "details",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum HealObjectDisposition {
    /// The legacy storage response does not prove the requested check or commit.
    Unknown,
    Repaired,
    VerifiedHealthy,
    /// Authoritative metadata/presence proof only. This does not certify
    /// payload integrity.
    MetadataHealthy,
    AuthoritativelyAbsent,
    Deferred {
        reason: HealDeferredReason,
        retry_not_before: Option<SystemTime>,
    },
    Failed(HealFailureClass),
    Cancelled,
    DryRunObserved,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealObjectReceipt {
    pub identity: HealObjectIdentity,
    pub disposition: HealObjectDisposition,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealAbortReason {
    Cancelled,
    Deadline,
    Untraversable,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", content = "reason", rename_all = "snake_case")]
pub enum HealExecutionOutcome {
    #[default]
    Pending,
    Running,
    Completed,
    CompletedWithErrors,
    Aborted(HealAbortReason),
}
