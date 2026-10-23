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

//! Normal returns from usage-candidate construction, not scan starts, durable
//! publication, quota authority or background convergence. The first existing
//! check that returns no candidate determines the outcome. Nested validation
//! and flattening do not create additional observations.

use serde::{Deserialize, Serialize};

/// The authoritative-candidate or non-authoritative view builder.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScannerUsageCandidateKind {
    Completed,
    Observational,
}

impl ScannerUsageCandidateKind {
    pub const ALL: [Self; 2] = [Self::Completed, Self::Observational];

    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Observational => "observational",
        }
    }

    pub(crate) const fn index(self) -> usize {
        self as usize
    }
}

/// A constructed candidate, or the first check that returned no candidate.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScannerUsageCandidateOutcome {
    Accepted,
    InventoryIncomplete,
    PublishGate,
    IncompleteSets,
    GenerationMismatch,
    InvalidInventory,
    InvalidRoot,
    Flatten,
    Merge,
    TierProof,
    UsageCounters,
    SetState,
    CountOverflow,
    MissingSource,
    SourceMembership,
    NoUsableSnapshots,
    TierGeneration,
    MissingLastUpdate,
    EmptyNamespaceGuard,
}

impl ScannerUsageCandidateOutcome {
    pub const ALL: [Self; 19] = [
        Self::Accepted,
        Self::InventoryIncomplete,
        Self::PublishGate,
        Self::IncompleteSets,
        Self::GenerationMismatch,
        Self::InvalidInventory,
        Self::InvalidRoot,
        Self::Flatten,
        Self::Merge,
        Self::TierProof,
        Self::UsageCounters,
        Self::SetState,
        Self::CountOverflow,
        Self::MissingSource,
        Self::SourceMembership,
        Self::NoUsableSnapshots,
        Self::TierGeneration,
        Self::MissingLastUpdate,
        Self::EmptyNamespaceGuard,
    ];

    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::InventoryIncomplete => "inventory_incomplete",
            Self::PublishGate => "publish_gate",
            Self::IncompleteSets => "incomplete_sets",
            Self::GenerationMismatch => "generation_mismatch",
            Self::InvalidInventory => "invalid_inventory",
            Self::InvalidRoot => "invalid_root",
            Self::Flatten => "flatten",
            Self::Merge => "merge",
            Self::TierProof => "tier_proof",
            Self::UsageCounters => "usage_counters",
            Self::SetState => "set_state",
            Self::CountOverflow => "count_overflow",
            Self::MissingSource => "missing_source",
            Self::SourceMembership => "source_membership",
            Self::NoUsableSnapshots => "no_usable_snapshots",
            Self::TierGeneration => "tier_generation",
            Self::MissingLastUpdate => "missing_last_update",
            Self::EmptyNamespaceGuard => "empty_namespace_guard",
        }
    }

    pub(crate) const fn index(self) -> usize {
        self as usize
    }
}

/// One process-lifetime terminal-result bucket. The fixed buckets are loaded
/// independently; a report is not an atomic snapshot across concurrent calls.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ScannerUsageCandidateCount {
    pub kind: ScannerUsageCandidateKind,
    pub outcome: ScannerUsageCandidateOutcome,
    pub count: u64,
}

pub(crate) const USAGE_CANDIDATE_COUNTER_SLOTS: usize =
    ScannerUsageCandidateKind::ALL.len() * ScannerUsageCandidateOutcome::ALL.len();
