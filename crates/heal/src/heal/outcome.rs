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

//! Execution results are separate from repair responsibility. A legacy
//! successful storage call supplies no authoritative repair receipt.

use serde::{Deserialize, Serialize};
use std::{collections::VecDeque, time::SystemTime};
use uuid::Uuid;

const MAX_OUTCOME_ITEMS: usize = 128;
const MAX_OUTCOME_BYTES: usize = 64 * 1024;
const MAX_OUTCOME_DETAIL_BYTES: usize = 1024;

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HealObjectOutcome {
    pub identity: HealObjectIdentity,
    pub disposition: HealObjectDisposition,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealObjectReceipt {
    pub identity: HealObjectIdentity,
    pub disposition: HealObjectDisposition,
}

impl HealObjectReceipt {
    pub(crate) fn verified_for(&self, expected: &HealObjectIdentity) -> bool {
        let disposition_verifies = match self.disposition {
            HealObjectDisposition::Repaired
            | HealObjectDisposition::VerifiedHealthy
            | HealObjectDisposition::AuthoritativelyAbsent => true,
            // A metadata/presence proof never certifies payload bytes, so it
            // cannot discharge a request that exists to decode the payload.
            HealObjectDisposition::MetadataHealthy => expected.kind != HealObjectKind::Decode,
            _ => false,
        };
        disposition_verifies
            && self.identity.kind == expected.kind
            && self.identity.bucket == expected.bucket
            && self.identity.object == expected.object
            && self.identity.version_id == expected.version_id
            && self.identity.pool_index == expected.pool_index
            && self.identity.set_index == expected.set_index
            && self.identity.bucket_incarnation_id == expected.bucket_incarnation_id
            && expected.bucket_incarnation_id.is_some()
    }
}

impl HealObjectOutcome {
    fn retained_bytes(&self) -> usize {
        size_of::<Self>()
            .saturating_add(self.identity.bucket.capacity())
            .saturating_add(self.identity.object.capacity())
            .saturating_add(self.identity.version_id.as_ref().map_or(0, String::capacity))
            .saturating_add(self.detail.as_ref().map_or(0, String::capacity))
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealTraversalCoverage {
    #[default]
    Unknown,
    Partial,
    Complete,
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

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HealOutcomeCounters {
    pub processed: u64,
    pub healed: u64,
    pub unchanged: u64,
    /// Deferred, cancelled, dry-run and unverified results remain unresolved.
    pub skipped: u64,
    pub failed: u64,
    pub unknown: u64,
    pub attempt_failures: u64,
    pub overflowed: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealTaskOutcome {
    pub execution: HealExecutionOutcome,
    pub coverage: HealTraversalCoverage,
    pub counters: HealOutcomeCounters,
    /// A bounded diagnostic window, not a complete responsibility ledger.
    pub objects: VecDeque<HealObjectOutcome>,
    pub objects_truncated: bool,
    #[serde(skip)]
    retained_object_bytes: usize,
    #[serde(skip)]
    untraversable: bool,
}

impl<'de> Deserialize<'de> for HealTaskOutcome {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        struct Snapshot {
            execution: HealExecutionOutcome,
            coverage: HealTraversalCoverage,
            counters: HealOutcomeCounters,
            objects: VecDeque<HealObjectOutcome>,
            objects_truncated: bool,
        }

        let mut snapshot = Snapshot::deserialize(deserializer)?;
        if snapshot.objects.len() > MAX_OUTCOME_ITEMS {
            return Err(serde::de::Error::custom("heal outcome object window exceeds its limit"));
        }
        let mut retained_object_bytes = 0usize;
        for object in &mut snapshot.objects {
            object.identity.bucket.shrink_to_fit();
            object.identity.object.shrink_to_fit();
            if let Some(version) = &mut object.identity.version_id {
                version.shrink_to_fit();
            }
            if let Some(detail) = &mut object.detail {
                if detail.len() > MAX_OUTCOME_DETAIL_BYTES {
                    return Err(serde::de::Error::custom("heal outcome detail exceeds its limit"));
                }
                detail.shrink_to_fit();
            }
            retained_object_bytes = retained_object_bytes.saturating_add(object.retained_bytes());
        }
        if retained_object_bytes > MAX_OUTCOME_BYTES {
            return Err(serde::de::Error::custom("heal outcome bytes exceed their limit"));
        }
        let counters = &snapshot.counters;
        let total = counters
            .healed
            .checked_add(counters.unchanged)
            .and_then(|total| total.checked_add(counters.skipped))
            .and_then(|total| total.checked_add(counters.failed));
        if !counters.overflowed && (total != Some(counters.processed) || counters.unknown > counters.skipped) {
            return Err(serde::de::Error::custom("heal outcome counters are inconsistent"));
        }
        Ok(Self {
            execution: snapshot.execution,
            coverage: snapshot.coverage,
            counters: snapshot.counters,
            objects: snapshot.objects,
            objects_truncated: snapshot.objects_truncated,
            retained_object_bytes,
            untraversable: snapshot.execution == HealExecutionOutcome::Aborted(HealAbortReason::Untraversable),
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HealOutcomeWireError {
    #[error("heal outcome is missing execution or counters")]
    MissingFields,
    #[error("heal outcome has invalid or unsupported execution fields")]
    InvalidFields(#[from] serde_json::Error),
    #[error("finished heal summary contradicts its canonical outcome")]
    ContradictoryCompletion,
}

/// Reconcile a peer's successful legacy summary using the canonical owner types.
/// A running retry may legitimately retain the preceding attempt's outcome.
pub fn legacy_wire_status<'a>(
    summary: &'a str,
    wire: &serde_json::Value,
    truncated: bool,
) -> Result<(&'a str, Option<String>), HealOutcomeWireError> {
    if summary != "finished" {
        return Ok((summary, None));
    }
    let execution = HealExecutionOutcome::deserialize(wire.get("execution").ok_or(HealOutcomeWireError::MissingFields)?)?;
    let counters = HealOutcomeCounters::deserialize(wire.get("counters").ok_or(HealOutcomeWireError::MissingFields)?)?;
    if matches!(execution, HealExecutionOutcome::Pending | HealExecutionOutcome::Running)
        || (execution == HealExecutionOutcome::Completed && counters.failed > 0)
    {
        return Err(HealOutcomeWireError::ContradictoryCompletion);
    }
    let (adapted, detail) = legacy_execution_status(summary, None, execution, &counters, first_failed_wire_object(wire));
    Ok((
        adapted,
        if adapted != summary {
            heal_status_detail(detail, truncated)
        } else {
            None
        },
    ))
}

pub(crate) fn heal_status_detail(detail: Option<String>, truncated: bool) -> Option<String> {
    if !truncated {
        return detail;
    }
    let truncation = "heal result items were truncated";
    Some(detail.map_or_else(|| truncation.to_string(), |detail| format!("{detail}; {truncation}")))
}

fn legacy_execution_status<'a>(
    summary: &'a str,
    detail: Option<String>,
    execution: HealExecutionOutcome,
    counters: &HealOutcomeCounters,
    first_failure: Option<String>,
) -> (&'a str, Option<String>) {
    if summary != "finished" {
        return (summary, detail);
    }
    match execution {
        HealExecutionOutcome::CompletedWithErrors => {
            let detail = format!("heal traversal completed with errors: {} failed objects", counters.failed);
            ("stopped", Some(append_first_failure(detail, first_failure)))
        }
        HealExecutionOutcome::Aborted(reason) => {
            let reason = match reason {
                HealAbortReason::Cancelled => "cancelled",
                HealAbortReason::Deadline => "timed out",
                HealAbortReason::Untraversable => "untraversable",
            };
            ("stopped", Some(format!("heal task {reason}")))
        }
        HealExecutionOutcome::Completed if counters.unknown > 0 => (
            summary,
            Some(format!(
                "heal traversal completed; authoritative storage proof is unavailable for {} objects",
                counters.unknown
            )),
        ),
        HealExecutionOutcome::Pending | HealExecutionOutcome::Running => {
            ("running", Some("heal execution has not reached a terminal outcome".to_string()))
        }
        HealExecutionOutcome::Completed => (summary, detail),
    }
}

impl HealTaskOutcome {
    pub(crate) fn legacy_status<'a>(&self, summary: &'a str, detail: Option<String>) -> (&'a str, Option<String>) {
        legacy_execution_status(summary, detail, self.execution, &self.counters, self.first_failed_object())
    }

    pub(crate) fn start(&mut self) {
        if self.execution != HealExecutionOutcome::Aborted(HealAbortReason::Cancelled) {
            self.execution = HealExecutionOutcome::Running;
        }
        self.coverage = HealTraversalCoverage::Partial;
    }

    pub(crate) fn attempt_failed(&mut self) {
        self.counters.overflowed |= !super::progress::increment_counter(&mut self.counters.attempt_failures);
    }

    pub(crate) fn mark_untraversable(&mut self) {
        self.untraversable = true;
        self.coverage = HealTraversalCoverage::Partial;
    }

    pub(crate) fn finish(&mut self, abort: Option<HealAbortReason>) {
        if self.execution == HealExecutionOutcome::Aborted(HealAbortReason::Cancelled) {
            return;
        }
        let abort = abort.or(self.untraversable.then_some(HealAbortReason::Untraversable));
        self.execution = match abort {
            Some(reason) => HealExecutionOutcome::Aborted(reason),
            None if self.counters.failed > 0 => HealExecutionOutcome::CompletedWithErrors,
            None => HealExecutionOutcome::Completed,
        };
        self.coverage = if abort.is_none() && !self.counters.overflowed {
            HealTraversalCoverage::Complete
        } else {
            HealTraversalCoverage::Partial
        };
    }

    pub(crate) fn record(&mut self, mut item: HealObjectOutcome) {
        use super::progress::increment_counter;
        let counters = &mut self.counters;
        counters.overflowed |= !increment_counter(&mut counters.processed);
        let counter = match item.disposition {
            HealObjectDisposition::Repaired => &mut counters.healed,
            HealObjectDisposition::VerifiedHealthy
            | HealObjectDisposition::MetadataHealthy
            | HealObjectDisposition::AuthoritativelyAbsent => &mut counters.unchanged,
            HealObjectDisposition::Failed(_) => &mut counters.failed,
            HealObjectDisposition::Unknown => {
                counters.overflowed |= !increment_counter(&mut counters.unknown);
                &mut counters.skipped
            }
            _ => &mut counters.skipped,
        };
        counters.overflowed |= !increment_counter(counter);
        if let Some(detail) = &mut item.detail {
            let mut end = detail.len().min(MAX_OUTCOME_DETAIL_BYTES);
            while !detail.is_char_boundary(end) {
                end -= 1;
            }
            self.objects_truncated |= end < detail.len();
            detail.truncate(end);
            detail.shrink_to_fit();
        }
        let bytes = item.retained_bytes();
        if bytes > MAX_OUTCOME_BYTES {
            self.objects_truncated = true;
            return;
        }
        while self.objects.len() >= MAX_OUTCOME_ITEMS || self.retained_object_bytes.saturating_add(bytes) > MAX_OUTCOME_BYTES {
            let Some(oldest) = self.objects.pop_front() else { break };
            self.retained_object_bytes = self.retained_object_bytes.saturating_sub(oldest.retained_bytes());
            self.objects_truncated = true;
        }
        self.retained_object_bytes = self.retained_object_bytes.saturating_add(bytes);
        self.objects.push_back(item);
    }

    pub(crate) fn retained_bytes(&self) -> usize {
        size_of::<Self>()
            .saturating_add(self.retained_object_bytes)
            .saturating_add(self.objects.capacity().saturating_mul(size_of::<HealObjectOutcome>()))
    }

    fn first_failed_object(&self) -> Option<String> {
        self.objects.iter().find_map(first_failed_outcome_object)
    }
}

fn append_first_failure(mut detail: String, first_failure: Option<String>) -> String {
    if let Some(first_failure) = first_failure {
        detail.push_str("; ");
        detail.push_str(&first_failure);
    }
    detail
}

fn first_failed_outcome_object(item: &HealObjectOutcome) -> Option<String> {
    let HealObjectDisposition::Failed(class) = item.disposition else {
        return None;
    };
    Some(format_first_failed_object(
        &item.identity.bucket,
        &item.identity.object,
        item.identity.version_id.as_deref(),
        failure_class_label(class),
        item.detail.as_deref(),
    ))
}

fn first_failed_wire_object(wire: &serde_json::Value) -> Option<String> {
    let objects = wire.get("objects")?.as_array()?;
    objects.iter().find_map(|item| {
        let disposition = item.get("disposition")?;
        if disposition.get("state")?.as_str()? != "failed" {
            return None;
        }
        let identity = item.get("identity")?;
        let bucket = identity.get("bucket")?.as_str()?;
        let object = identity.get("object")?.as_str()?;
        let version_id = identity.get("versionId").and_then(serde_json::Value::as_str);
        let class = disposition
            .get("details")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        let detail = item.get("detail").and_then(serde_json::Value::as_str);
        Some(format_first_failed_object(bucket, object, version_id, class, detail))
    })
}

fn failure_class_label(class: HealFailureClass) -> &'static str {
    match class {
        HealFailureClass::Recoverable => "recoverable",
        HealFailureClass::RetryExhausted => "retry_exhausted",
        HealFailureClass::Permanent => "permanent",
    }
}

fn format_first_failed_object(bucket: &str, object: &str, version_id: Option<&str>, class: &str, detail: Option<&str>) -> String {
    let mut message = format!("first failed object {bucket}/{object} ({class})");
    if let Some(version_id) = version_id.filter(|version_id| !version_id.is_empty()) {
        message.push_str(", version ");
        message.push_str(version_id);
    }
    if let Some(detail) = detail.filter(|detail| !detail.is_empty()) {
        message.push_str(": ");
        message.push_str(detail);
    }
    message
}

#[cfg(test)]
mod canonical_outcome_tests {
    use super::*;

    fn item(disposition: HealObjectDisposition) -> HealObjectOutcome {
        HealObjectOutcome {
            identity: HealObjectIdentity {
                kind: HealObjectKind::Object,
                bucket: "bucket".to_string(),
                object: "object".to_string(),
                version_id: None,
                bucket_incarnation_id: None,
                pool_index: None,
                set_index: None,
            },
            disposition,
            detail: None,
        }
    }

    #[test]
    fn persisted_outcome_rebuilds_accounting_and_enforces_window_boundaries() {
        let mut outcome = HealTaskOutcome::default();
        for _ in 0..MAX_OUTCOME_ITEMS {
            outcome.record(item(HealObjectDisposition::Repaired));
        }
        outcome.finish(None);
        let value = serde_json::to_value(&outcome).expect("bounded outcome");
        let mut restored: HealTaskOutcome = serde_json::from_value(value.clone()).expect("restore bounded window");
        assert_eq!(restored.retained_object_bytes, outcome.retained_object_bytes);
        restored.record(item(HealObjectDisposition::Repaired));
        assert_eq!(restored.objects.len(), MAX_OUTCOME_ITEMS);
        assert!(restored.objects_truncated);
        let mut oversized = value;
        let extra = oversized["objects"][0].clone();
        oversized["objects"].as_array_mut().expect("objects").push(extra);
        assert!(serde_json::from_value::<HealTaskOutcome>(oversized).is_err());

        let mut object = item(HealObjectDisposition::Repaired);
        let fixed_bytes = object.retained_bytes() - object.identity.object.capacity();
        object.identity.object = "x".repeat(MAX_OUTCOME_BYTES - fixed_bytes);
        let mut outcome = HealTaskOutcome::default();
        outcome.record(object);
        outcome.finish(None);
        assert_eq!(outcome.retained_object_bytes, MAX_OUTCOME_BYTES);
        let mut value = serde_json::to_value(outcome).expect("exact byte boundary");
        let restored: HealTaskOutcome = serde_json::from_value(value.clone()).expect("exact bound remains readable");
        assert_eq!(restored.retained_object_bytes, MAX_OUTCOME_BYTES);
        value["objects"][0]["identity"]["object"] = serde_json::json!("x".repeat(MAX_OUTCOME_BYTES - fixed_bytes + 1));
        assert!(serde_json::from_value::<HealTaskOutcome>(value).is_err());
    }

    #[test]
    fn metadata_health_receipt_never_discharges_a_payload_decode_request() {
        let incarnation = Uuid::new_v4();
        let mut expected = HealObjectIdentity {
            bucket_incarnation_id: Some(incarnation),
            ..item(HealObjectDisposition::Unknown).identity
        };
        let mut receipt = HealObjectReceipt {
            identity: expected.clone(),
            disposition: HealObjectDisposition::MetadataHealthy,
        };
        assert!(
            receipt.verified_for(&expected),
            "a presence proof still settles a metadata-level object request"
        );

        expected.kind = HealObjectKind::Decode;
        receipt.identity.kind = HealObjectKind::Decode;
        assert!(
            !receipt.verified_for(&expected),
            "a presence-only proof must not clear a payload decode responsibility"
        );
        receipt.disposition = HealObjectDisposition::VerifiedHealthy;
        assert!(receipt.verified_for(&expected));
    }

    #[test]
    fn canonical_outcome_categories_have_one_terminal_count() {
        let mut outcome = HealTaskOutcome::default();
        for disposition in [
            HealObjectDisposition::Unknown,
            HealObjectDisposition::Repaired,
            HealObjectDisposition::VerifiedHealthy,
            HealObjectDisposition::MetadataHealthy,
            HealObjectDisposition::AuthoritativelyAbsent,
            HealObjectDisposition::Deferred {
                reason: HealDeferredReason::DanglingDeleteGrace,
                retry_not_before: None,
            },
            HealObjectDisposition::Failed(HealFailureClass::Permanent),
            HealObjectDisposition::Cancelled,
            HealObjectDisposition::DryRunObserved,
        ] {
            outcome.record(item(disposition));
        }
        let c = &outcome.counters;
        assert_eq!((c.processed, c.healed, c.unchanged, c.skipped, c.failed, c.unknown), (9, 1, 3, 4, 1, 1));
        assert_eq!(c.processed, c.healed + c.unchanged + c.skipped + c.failed);
    }

    #[test]
    fn canonical_outcome_window_count_bytes_and_oversize_keep_total_counts() {
        let mut outcome = HealTaskOutcome::default();
        for _ in 0..MAX_OUTCOME_ITEMS {
            outcome.record(item(HealObjectDisposition::Unknown));
        }
        assert_eq!(outcome.objects.len(), MAX_OUTCOME_ITEMS);
        assert!(!outcome.objects_truncated);
        outcome.record(item(HealObjectDisposition::Unknown));
        assert_eq!(outcome.objects.len(), MAX_OUTCOME_ITEMS);
        assert!(outcome.objects_truncated);
        let mut oversized = item(HealObjectDisposition::Failed(HealFailureClass::Permanent));
        oversized.identity.object = "x".repeat(MAX_OUTCOME_BYTES);
        outcome.record(oversized);
        assert_eq!(outcome.counters.processed, u64::try_from(MAX_OUTCOME_ITEMS + 2).expect("bounded count"));
        assert_eq!(outcome.counters.failed, 1);
        assert!(outcome.retained_object_bytes <= MAX_OUTCOME_BYTES);
        for _ in 0..MAX_OUTCOME_ITEMS {
            let mut failed = item(HealObjectDisposition::Failed(HealFailureClass::Permanent));
            failed.detail = Some("\u{4fee}".repeat(MAX_OUTCOME_DETAIL_BYTES));
            outcome.record(failed);
        }
        assert!(outcome.retained_object_bytes <= MAX_OUTCOME_BYTES);
        assert!(outcome.objects.iter().all(|item| {
            item.detail
                .as_ref()
                .is_none_or(|detail| detail.len() <= MAX_OUTCOME_DETAIL_BYTES)
        }));
        assert!(outcome.objects.len() < MAX_OUTCOME_ITEMS);
    }

    #[test]
    fn outcome_v3_serialization_keeps_unverified_dispositions_and_window_bounds() {
        let mut outcome = HealTaskOutcome::default();
        for disposition in [
            HealObjectDisposition::Unknown,
            HealObjectDisposition::Deferred {
                reason: HealDeferredReason::DanglingDeleteGrace,
                retry_not_before: None,
            },
            HealObjectDisposition::DryRunObserved,
        ] {
            outcome.record(item(disposition));
        }
        outcome.finish(None);
        let wire = serde_json::to_value(&outcome).expect("canonical wire view");
        assert_eq!(wire["execution"]["state"], "completed");
        assert_eq!(wire["counters"]["healed"], 0);
        assert_eq!(wire["counters"]["skipped"], 3);
        assert_eq!(wire["objects"][1]["disposition"]["details"]["reason"], "dangling_delete_grace");
        assert!(wire["objects"][1]["identity"]["bucketIncarnationId"].is_null());
        for _ in 0..MAX_OUTCOME_ITEMS + 1 {
            let mut result = item(HealObjectDisposition::Unknown);
            result.detail = Some("\"".repeat(MAX_OUTCOME_DETAIL_BYTES));
            outcome.record(result);
        }
        let bytes = serde_json::to_vec(&outcome).expect("bounded canonical samples");
        assert!(
            bytes.len() < 8 * MAX_OUTCOME_BYTES,
            "JSON escaping remains bounded independently of object count"
        );
        assert!(outcome.objects_truncated);
    }

    #[test]
    fn outcome_v3_wire_consistency_rejects_unknown_success_without_rejecting_extensions() {
        let mut outcome = HealTaskOutcome::default();
        outcome.finish(None);
        let mut wire = serde_json::to_value(&outcome).expect("canonical snapshot");
        wire["execution"]["futureField"] = serde_json::json!({"new": true});
        wire["counters"]["futureCounter"] = serde_json::json!(42);
        assert_eq!(
            legacy_wire_status("finished", &wire, false).expect("unknown extension fields"),
            ("finished", None)
        );
        wire["execution"]["state"] = serde_json::json!("future_execution");
        assert!(legacy_wire_status("finished", &wire, false).is_err());
        assert_eq!(
            legacy_wire_status("running", &wire, false).expect("unknown nonterminal outcome"),
            ("running", None)
        );
        wire["execution"] = serde_json::json!({"state":"completed"});
        wire["counters"]["failed"] = serde_json::json!(1);
        assert!(matches!(
            legacy_wire_status("finished", &wire, false),
            Err(HealOutcomeWireError::ContradictoryCompletion)
        ));
        wire.as_object_mut().expect("object").remove("execution");
        assert!(matches!(
            legacy_wire_status("finished", &wire, false),
            Err(HealOutcomeWireError::MissingFields)
        ));
    }

    #[test]
    fn canonical_outcome_counter_overflow_cannot_claim_complete_coverage() {
        let mut outcome = HealTaskOutcome::default();
        outcome.counters.processed = u64::MAX;
        outcome.record(item(HealObjectDisposition::Unknown));
        outcome.finish(None);
        assert!(outcome.counters.overflowed);
        assert_eq!(outcome.counters.processed, u64::MAX);
        assert_eq!(outcome.coverage, HealTraversalCoverage::Partial);
    }

    #[test]
    fn positive_receipt_requires_exact_identity_and_bucket_incarnation() {
        let incarnation = Uuid::new_v4();
        let expected = HealObjectIdentity {
            bucket_incarnation_id: Some(incarnation),
            ..item(HealObjectDisposition::Unknown).identity
        };
        let mut receipt = HealObjectReceipt {
            identity: expected.clone(),
            disposition: HealObjectDisposition::Repaired,
        };

        receipt.identity.bucket_incarnation_id = None;
        assert!(
            !receipt.verified_for(&expected),
            "a positive storage receipt without bucket incarnation must remain untrusted"
        );

        receipt.identity.bucket_incarnation_id = Some(incarnation);
        assert!(receipt.verified_for(&expected));

        receipt.identity.bucket_incarnation_id = Some(Uuid::new_v4());
        assert!(
            !receipt.verified_for(&expected),
            "a storage receipt for a different bucket incarnation must not clear the requested responsibility"
        );

        receipt.identity.version_id = Some("older-version".to_string());
        assert!(
            !receipt.verified_for(&expected),
            "a storage receipt for a different object/version tuple must not clear the requested responsibility"
        );

        receipt.identity = HealObjectIdentity {
            bucket_incarnation_id: Some(incarnation),
            pool_index: Some(1),
            ..expected.clone()
        };
        assert!(
            !receipt.verified_for(&expected),
            "a storage receipt for a different erasure location must not clear the requested responsibility"
        );

        receipt.identity = HealObjectIdentity {
            bucket_incarnation_id: Some(incarnation),
            ..expected
        };
        receipt.disposition = HealObjectDisposition::Unknown;
        assert!(
            !receipt.verified_for(&receipt.identity),
            "legacy success without a positive disposition remains unknown"
        );
    }
}
