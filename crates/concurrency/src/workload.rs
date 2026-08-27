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

use serde::{Deserialize, Serialize};

/// Stable workload classes used by future runtime admission snapshots.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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

    /// Return a registry where matching entries from `overlay` replace this
    /// registry's entries, and new classes are appended in overlay order.
    pub fn overlay(mut self, overlay: Self) -> Self {
        for entry in overlay.entries {
            if let Some(existing) = self.entries.iter_mut().find(|existing| existing.class == entry.class) {
                *existing = entry;
            } else {
                self.entries.push(entry);
            }
        }

        self
    }
}

/// Provider boundary for future read-only workload admission snapshots.
pub trait WorkloadAdmissionSnapshotProvider {
    /// Return the current workload admission registry snapshot.
    fn workload_admission_snapshot(&self) -> WorkloadAdmissionRegistrySnapshot;
}

/// Foreground workload pressure observed against a configured utilization threshold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ForegroundPressure {
    /// Foreground workload class whose utilization reached its threshold.
    pub class: WorkloadClass,
    /// Observed utilization percentage for the class.
    pub usage_pct: usize,
    /// Configured threshold percentage that the observed utilization reached.
    pub threshold_pct: usize,
}

impl ForegroundPressure {
    /// Return a stable reason label for logs and metrics.
    pub const fn reason(self) -> &'static str {
        match self.class {
            WorkloadClass::ForegroundRead => "foreground_read_pressure",
            WorkloadClass::ForegroundWrite => "foreground_write_pressure",
            _ => "foreground_pressure",
        }
    }
}

/// Return the strongest foreground pressure in `snapshot`, if any.
///
/// A zero threshold disables its class. `Saturated` counts as full utilization
/// regardless of the reported limit; otherwise a class contributes only when it
/// reports a non-zero limit, with a missing active count read as zero. When both
/// classes are above their threshold the higher utilization wins.
///
/// Callers own the enable switch: this function evaluates thresholds only.
pub fn foreground_pressure(
    snapshot: &WorkloadAdmissionRegistrySnapshot,
    read_threshold_pct: usize,
    write_threshold_pct: usize,
) -> Option<ForegroundPressure> {
    [
        (WorkloadClass::ForegroundRead, read_threshold_pct),
        (WorkloadClass::ForegroundWrite, write_threshold_pct),
    ]
    .into_iter()
    .filter_map(|(class, threshold_pct)| {
        if threshold_pct == 0 {
            return None;
        }

        let entry = snapshot.get(class)?;
        let usage_pct = if matches!(entry.state, AdmissionState::Saturated) {
            100
        } else {
            let limit = entry.limit?;
            if limit == 0 {
                return None;
            }
            entry
                .active
                .unwrap_or(0)
                .saturating_mul(100)
                .checked_div(limit)
                .unwrap_or(100)
        };

        (usage_pct >= threshold_pct).then_some(ForegroundPressure {
            class,
            usage_pct,
            threshold_pct,
        })
    })
    .max_by_key(|pressure| pressure.usage_pct)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn sorted_json_value(value: Value) -> Value {
        match value {
            Value::Array(values) => Value::Array(values.into_iter().map(sorted_json_value).collect()),
            Value::Object(object) => {
                let mut entries: Vec<_> = object.into_iter().collect();
                entries.sort_by(|(left, _), (right, _)| left.cmp(right));

                Value::Object(
                    entries
                        .into_iter()
                        .map(|(key, value)| (key, sorted_json_value(value)))
                        .collect(),
                )
            }
            value => value,
        }
    }

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

        insta::assert_json_snapshot!(
            "workload_admission_registry_snapshot",
            sorted_json_value(serde_json::to_value(&registry).expect("workload admission registry should serialize"))
        );
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

    #[test]
    fn admission_registry_overlay_replaces_existing_classes_and_appends_new_ones() {
        let base = WorkloadAdmissionRegistrySnapshot::new(vec![
            WorkloadAdmissionSnapshot::new(WorkloadClass::ForegroundRead, AdmissionState::Open),
            WorkloadAdmissionSnapshot::new(WorkloadClass::Scanner, AdmissionState::Unknown),
        ]);
        let overlay = WorkloadAdmissionRegistrySnapshot::new(vec![
            WorkloadAdmissionSnapshot::new(WorkloadClass::Scanner, AdmissionState::Open).with_counts(Some(2), None, None),
            WorkloadAdmissionSnapshot::new(WorkloadClass::Repair, AdmissionState::Open),
        ]);

        let registry = base.overlay(overlay);

        assert_eq!(registry.entries().len(), 3);
        assert_eq!(
            registry.get(WorkloadClass::ForegroundRead).map(|snapshot| snapshot.state),
            Some(AdmissionState::Open)
        );
        assert_eq!(
            registry.get(WorkloadClass::Scanner).map(|snapshot| snapshot.state),
            Some(AdmissionState::Open)
        );
        assert_eq!(registry.get(WorkloadClass::Scanner).and_then(|snapshot| snapshot.active), Some(2));
        assert_eq!(
            registry.get(WorkloadClass::Repair).map(|snapshot| snapshot.state),
            Some(AdmissionState::Open)
        );
    }

    #[test]
    fn workload_admission_snapshot_round_trips_with_snake_case_enums() {
        let snapshot = WorkloadAdmissionSnapshot::new(WorkloadClass::ForegroundWrite, AdmissionState::Disabled)
            .with_counts(Some(0), Some(0), Some(0))
            .with_reason("foreground write admission not exposed");
        let encoded = sorted_json_value(serde_json::to_value(&snapshot).expect("serialize workload admission snapshot"));

        insta::assert_json_snapshot!("workload_admission_snapshot", encoded);
        assert_eq!(encoded["class"], json!("foreground_write"));
        assert_eq!(encoded["state"], json!("disabled"));

        let decoded: WorkloadAdmissionSnapshot =
            serde_json::from_value(encoded).expect("deserialize workload admission snapshot");
        assert_eq!(decoded, snapshot);
    }

    #[test]
    fn workload_admission_registry_rejects_unknown_fields() {
        let raw = json!({
            "entries": [{
                "class": "foreground_read",
                "state": "open",
                "active": 1,
                "queued": 0,
                "limit": 8,
                "reason": null,
                "unexpected": true
            }]
        });

        let err = serde_json::from_value::<WorkloadAdmissionRegistrySnapshot>(raw)
            .expect_err("unknown workload admission fields must be rejected");

        assert!(err.to_string().contains("unexpected"));
    }

    fn counted(
        class: WorkloadClass,
        state: AdmissionState,
        active: Option<usize>,
        limit: Option<usize>,
    ) -> WorkloadAdmissionSnapshot {
        WorkloadAdmissionSnapshot::new(class, state).with_counts(active, None, limit)
    }

    fn registry(entries: Vec<WorkloadAdmissionSnapshot>) -> WorkloadAdmissionRegistrySnapshot {
        WorkloadAdmissionRegistrySnapshot::new(entries)
    }

    #[test]
    fn foreground_pressure_reason_labels_cover_non_foreground_classes() {
        let read = ForegroundPressure {
            class: WorkloadClass::ForegroundRead,
            usage_pct: 90,
            threshold_pct: 80,
        };
        let write = ForegroundPressure {
            class: WorkloadClass::ForegroundWrite,
            usage_pct: 90,
            threshold_pct: 80,
        };
        let repair = ForegroundPressure {
            class: WorkloadClass::Repair,
            usage_pct: 90,
            threshold_pct: 80,
        };

        assert_eq!(read.reason(), "foreground_read_pressure");
        assert_eq!(write.reason(), "foreground_write_pressure");
        assert_eq!(repair.reason(), "foreground_pressure");
    }

    #[test]
    fn foreground_pressure_is_disabled_when_both_thresholds_are_zero() {
        let snapshot = registry(vec![
            counted(WorkloadClass::ForegroundRead, AdmissionState::Saturated, Some(8), Some(8)),
            counted(WorkloadClass::ForegroundWrite, AdmissionState::Saturated, Some(8), Some(8)),
        ]);

        assert_eq!(foreground_pressure(&snapshot, 0, 0), None);
    }

    #[test]
    fn foreground_pressure_skips_only_the_class_whose_threshold_is_zero() {
        let snapshot = registry(vec![
            counted(WorkloadClass::ForegroundRead, AdmissionState::Open, Some(10), Some(10)),
            counted(WorkloadClass::ForegroundWrite, AdmissionState::Open, Some(9), Some(10)),
        ]);

        assert_eq!(
            foreground_pressure(&snapshot, 0, 80),
            Some(ForegroundPressure {
                class: WorkloadClass::ForegroundWrite,
                usage_pct: 90,
                threshold_pct: 80,
            })
        );
        assert_eq!(
            foreground_pressure(&snapshot, 80, 0),
            Some(ForegroundPressure {
                class: WorkloadClass::ForegroundRead,
                usage_pct: 100,
                threshold_pct: 80,
            })
        );
    }

    #[test]
    fn foreground_pressure_ignores_missing_entries() {
        let snapshot = registry(vec![counted(WorkloadClass::Scanner, AdmissionState::Saturated, Some(8), Some(8))]);

        assert_eq!(foreground_pressure(&snapshot, 1, 1), None);
    }

    #[test]
    fn foreground_pressure_ignores_missing_and_zero_limits() {
        let missing_limit = registry(vec![counted(
            WorkloadClass::ForegroundRead,
            AdmissionState::Throttled,
            Some(8),
            None,
        )]);
        let zero_limit = registry(vec![counted(
            WorkloadClass::ForegroundWrite,
            AdmissionState::Throttled,
            Some(8),
            Some(0),
        )]);

        assert_eq!(foreground_pressure(&missing_limit, 1, 1), None);
        assert_eq!(foreground_pressure(&zero_limit, 1, 1), None);
    }

    #[test]
    fn foreground_pressure_treats_saturated_as_full_without_reading_limit() {
        let snapshot = registry(vec![
            counted(WorkloadClass::ForegroundRead, AdmissionState::Saturated, None, None),
            counted(WorkloadClass::ForegroundWrite, AdmissionState::Saturated, Some(0), Some(0)),
        ]);

        assert_eq!(
            foreground_pressure(&snapshot, 100, 0),
            Some(ForegroundPressure {
                class: WorkloadClass::ForegroundRead,
                usage_pct: 100,
                threshold_pct: 100,
            })
        );
        assert_eq!(
            foreground_pressure(&snapshot, 0, 100),
            Some(ForegroundPressure {
                class: WorkloadClass::ForegroundWrite,
                usage_pct: 100,
                threshold_pct: 100,
            })
        );
    }

    #[test]
    fn foreground_pressure_reads_missing_active_as_zero() {
        let snapshot = registry(vec![counted(WorkloadClass::ForegroundRead, AdmissionState::Open, None, Some(8))]);

        assert_eq!(foreground_pressure(&snapshot, 1, 1), None);
    }

    #[test]
    fn foreground_pressure_returns_the_higher_utilization_when_both_classes_exceed() {
        let read_higher = registry(vec![
            counted(WorkloadClass::ForegroundRead, AdmissionState::Open, Some(19), Some(20)),
            counted(WorkloadClass::ForegroundWrite, AdmissionState::Open, Some(17), Some(20)),
        ]);
        let write_higher = registry(vec![
            counted(WorkloadClass::ForegroundRead, AdmissionState::Open, Some(17), Some(20)),
            counted(WorkloadClass::ForegroundWrite, AdmissionState::Open, Some(19), Some(20)),
        ]);

        assert_eq!(
            foreground_pressure(&read_higher, 80, 80),
            Some(ForegroundPressure {
                class: WorkloadClass::ForegroundRead,
                usage_pct: 95,
                threshold_pct: 80,
            })
        );
        assert_eq!(
            foreground_pressure(&write_higher, 80, 80),
            Some(ForegroundPressure {
                class: WorkloadClass::ForegroundWrite,
                usage_pct: 95,
                threshold_pct: 80,
            })
        );
    }

    #[test]
    fn foreground_pressure_breaks_utilization_ties_toward_the_write_class() {
        let snapshot = registry(vec![
            counted(WorkloadClass::ForegroundRead, AdmissionState::Open, Some(18), Some(20)),
            counted(WorkloadClass::ForegroundWrite, AdmissionState::Open, Some(18), Some(20)),
        ]);

        assert_eq!(
            foreground_pressure(&snapshot, 80, 80),
            Some(ForegroundPressure {
                class: WorkloadClass::ForegroundWrite,
                usage_pct: 90,
                threshold_pct: 80,
            })
        );
    }

    #[test]
    fn foreground_pressure_triggers_exactly_at_the_threshold_and_not_below() {
        let at_threshold = registry(vec![counted(
            WorkloadClass::ForegroundRead,
            AdmissionState::Open,
            Some(8),
            Some(10),
        )]);
        let below_threshold = registry(vec![counted(
            WorkloadClass::ForegroundRead,
            AdmissionState::Open,
            Some(7),
            Some(10),
        )]);

        assert_eq!(
            foreground_pressure(&at_threshold, 80, 80),
            Some(ForegroundPressure {
                class: WorkloadClass::ForegroundRead,
                usage_pct: 80,
                threshold_pct: 80,
            })
        );
        assert_eq!(foreground_pressure(&below_threshold, 80, 80), None);
    }
}
