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

//! Runtime workload admission contract types.

/// Stable workload classes used by future runtime admission snapshots.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WorkloadClass {
    /// Foreground read requests.
    ForegroundRead,
    /// Foreground write requests.
    ForegroundWrite,
    /// Metadata and namespace operations.
    Metadata,
    /// Background scanner work.
    Scanner,
    /// Repair and heal work.
    Repair,
    /// Replication work.
    Replication,
}

impl WorkloadClass {
    /// Required workload classes covered by the runtime admission contract.
    pub const REQUIRED: [Self; 6] = [
        Self::ForegroundRead,
        Self::ForegroundWrite,
        Self::Metadata,
        Self::Scanner,
        Self::Repair,
        Self::Replication,
    ];

    /// Return a stable label for logs, metrics, and snapshots.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ForegroundRead => "foreground_read",
            Self::ForegroundWrite => "foreground_write",
            Self::Metadata => "metadata",
            Self::Scanner => "scanner",
            Self::Repair => "repair",
            Self::Replication => "replication",
        }
    }
}

impl std::fmt::Display for WorkloadClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Read-only admission state for a workload class.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AdmissionState {
    /// Admission is open for this workload class.
    Open,
    /// Admission is currently throttled.
    Throttled,
    /// Admission is saturated or queue-limited.
    Saturated,
    /// Admission is disabled by configuration or capability.
    Disabled,
    /// Admission state is not currently known.
    #[default]
    Unknown,
}

impl AdmissionState {
    /// Return whether the state allows admission without throttling.
    pub const fn is_open(self) -> bool {
        matches!(self, Self::Open)
    }
}

/// Read-only admission snapshot for one workload class.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkloadAdmissionSnapshot {
    /// Workload class described by this snapshot.
    pub class: WorkloadClass,
    /// Current admission state.
    pub state: AdmissionState,
    /// Active work count when the owner exposes it.
    pub active: Option<usize>,
    /// Queued work count when the owner exposes it.
    pub queued: Option<usize>,
    /// Admission limit when the owner exposes it.
    pub limit: Option<usize>,
    /// Optional state reason for disabled, throttled, saturated, or unknown states.
    pub reason: Option<String>,
}

impl WorkloadAdmissionSnapshot {
    /// Create a snapshot for a workload class and admission state.
    pub const fn new(class: WorkloadClass, state: AdmissionState) -> Self {
        Self {
            class,
            state,
            active: None,
            queued: None,
            limit: None,
            reason: None,
        }
    }

    /// Add optional active, queued, and limit counters.
    pub const fn with_counts(mut self, active: Option<usize>, queued: Option<usize>, limit: Option<usize>) -> Self {
        self.active = active;
        self.queued = queued;
        self.limit = limit;
        self
    }

    /// Add a human-readable reason.
    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }
}

/// Read-only admission registry snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WorkloadAdmissionRegistrySnapshot {
    entries: Vec<WorkloadAdmissionSnapshot>,
}

impl WorkloadAdmissionRegistrySnapshot {
    /// Create a registry snapshot from per-class entries.
    pub fn new(entries: Vec<WorkloadAdmissionSnapshot>) -> Self {
        Self { entries }
    }

    /// Borrow all entries in snapshot order.
    pub fn entries(&self) -> &[WorkloadAdmissionSnapshot] {
        &self.entries
    }

    /// Find the admission snapshot for a workload class.
    pub fn get(&self, class: WorkloadClass) -> Option<&WorkloadAdmissionSnapshot> {
        self.entries.iter().find(|entry| entry.class == class)
    }
}

/// Provider boundary for future read-only workload admission snapshots.
pub trait WorkloadAdmissionSnapshotProvider {
    /// Return the current workload admission registry snapshot.
    fn workload_admission_snapshot(&self) -> WorkloadAdmissionRegistrySnapshot;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workload_contract_covers_required_classes() {
        let labels: Vec<_> = WorkloadClass::REQUIRED.iter().map(|class| class.as_str()).collect();

        assert_eq!(
            labels,
            vec![
                "foreground_read",
                "foreground_write",
                "metadata",
                "scanner",
                "repair",
                "replication"
            ]
        );
        assert_eq!(WorkloadClass::Repair.to_string(), "repair");
    }

    #[test]
    fn admission_snapshot_preserves_unknown_and_counted_states() {
        let unknown = WorkloadAdmissionSnapshot::new(WorkloadClass::Scanner, AdmissionState::Unknown);
        let open = WorkloadAdmissionSnapshot::new(WorkloadClass::ForegroundRead, AdmissionState::Open).with_counts(
            Some(3),
            Some(1),
            Some(8),
        );
        let registry = WorkloadAdmissionRegistrySnapshot::new(vec![unknown.clone(), open.clone()]);

        assert_eq!(registry.entries(), &[unknown, open]);
        assert_eq!(
            registry.get(WorkloadClass::Scanner).map(|snapshot| snapshot.state),
            Some(AdmissionState::Unknown)
        );
        assert!(matches!(
            registry.get(WorkloadClass::ForegroundRead).map(|snapshot| snapshot.state),
            Some(state) if state.is_open()
        ));
        assert_eq!(
            registry
                .get(WorkloadClass::ForegroundRead)
                .and_then(|snapshot| snapshot.limit),
            Some(8)
        );
    }
}
