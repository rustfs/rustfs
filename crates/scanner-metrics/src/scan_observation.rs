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

//! Node-local, process-lifetime observations of scanner work. Counters are
//! independently loaded, so concurrent reports need not balance exactly.
//! These observations never grant cache reuse or publication authority.

use crate::metrics::Metrics;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

pub const PREFIX_REUSE_BLOCKERS: [&str; 8] = [
    "missing_producer_identity",
    "missing_durable_journal_replay",
    "restart_gap",
    "generation_gap",
    "overflow",
    "stale_ack_generation",
    "missing_cold_zero_walk_oracle",
    "distributed_without_peer_invalidation",
];

const SCOPE_FALLBACK_REASONS: [&str; 6] = [
    "dirty_snapshot_incomplete",
    "dirty_generation_exhausted",
    "dirty_bucket_overflow",
    "no_listed_dirty_buckets",
    "baseline_unavailable",
    "peer_scope_unavailable",
];

/// A storage read result, independent of later metadata decoding or ILM work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScannerMetadataReadOutcome {
    Success,
    NotFound,
    Error,
    Cancelled,
}

/// The existing path that actually copied a cached subtree instead of walking it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScannerCacheReuseKind {
    CleanPrefix,
    CoveredNamespace,
    SampledNamespace,
    ClassifiedNamespace,
}

impl ScannerCacheReuseKind {
    const ALL: [Self; 4] = [
        Self::CleanPrefix,
        Self::CoveredNamespace,
        Self::SampledNamespace,
        Self::ClassifiedNamespace,
    ];

    fn label(self) -> &'static str {
        match self {
            Self::CleanPrefix => "clean_prefix",
            Self::CoveredNamespace => "covered_namespace",
            Self::SampledNamespace => "sampled_namespace",
            Self::ClassifiedNamespace => "classified_namespace",
        }
    }
}

/// Scope admission and end-of-cycle evidence are separate observations. An
/// activated completion proof does not mean this cycle used prefix reuse.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScannerPrefixReuseStage {
    ScopeResolution,
    CycleCompletion,
}

/// The first existing baseline validation check that rejected a snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScannerBaselineRejection {
    Missing,
    Decode,
    Incomplete,
    Partial,
    Marks,
    Epoch,
    SourceCount,
    PlanMissing,
    PlanMismatch,
    SourceIndex,
    SourceMembership,
    SourceDuplicate,
    SetIncomplete,
    SetTombstone,
    SetEpoch,
    SetCycle,
    SetPlan,
    SourceCoverage,
    AuthoritativeIdentity,
    NotNewer,
}

impl ScannerBaselineRejection {
    const ALL: [Self; 20] = [
        Self::Missing,
        Self::Decode,
        Self::Incomplete,
        Self::Partial,
        Self::Marks,
        Self::Epoch,
        Self::SourceCount,
        Self::PlanMissing,
        Self::PlanMismatch,
        Self::SourceIndex,
        Self::SourceMembership,
        Self::SourceDuplicate,
        Self::SetIncomplete,
        Self::SetTombstone,
        Self::SetEpoch,
        Self::SetCycle,
        Self::SetPlan,
        Self::SourceCoverage,
        Self::AuthoritativeIdentity,
        Self::NotNewer,
    ];

    fn label(self) -> &'static str {
        match self {
            Self::Missing => "missing",
            Self::Decode => "decode",
            Self::Incomplete => "incomplete",
            Self::Partial => "partial",
            Self::Marks => "marks",
            Self::Epoch => "epoch",
            Self::SourceCount => "source_count",
            Self::PlanMissing => "plan_missing",
            Self::PlanMismatch => "plan_mismatch",
            Self::SourceIndex => "source_index",
            Self::SourceMembership => "source_membership",
            Self::SourceDuplicate => "source_duplicate",
            Self::SetIncomplete => "set_incomplete",
            Self::SetTombstone => "set_tombstone",
            Self::SetEpoch => "set_epoch",
            Self::SetCycle => "set_cycle",
            Self::SetPlan => "set_plan",
            Self::SourceCoverage => "source_coverage",
            Self::AuthoritativeIdentity => "authoritative_identity",
            Self::NotNewer => "not_newer",
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ScannerMetadataReadSnapshot {
    pub attempts: u64,
    pub succeeded: u64,
    pub not_found: u64,
    pub errors: u64,
    pub cancelled: u64,
    /// Bytes returned by successful storage reads, including corrupt metadata.
    pub bytes: u64,
    /// Elapsed read-future time, including failed and cancelled reads.
    pub elapsed_nanos: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ScannerCacheReuseSnapshot {
    /// Completed subtree reuse decisions; not cache entries or objects skipped.
    pub count: u64,
    /// Time spent copying and attaching the reused subtree, excluding listing.
    pub elapsed_nanos: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ScannerPrefixReuseSnapshot {
    pub evaluated: u64,
    pub activated: u64,
    /// All failed checks are counted; one evaluation can have several blockers.
    pub blockers: BTreeMap<String, u64>,
}

/// Missing on an older node means unobserved, not zero work or healthy reuse.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ScannerScanObservationSnapshot {
    pub metadata_reads: ScannerMetadataReadSnapshot,
    pub cache_reuse: BTreeMap<String, ScannerCacheReuseSnapshot>,
    /// Clean root children still walked because EC scans also check health.
    pub erasure_clean_prefix_walks: u64,
    pub prefix_scope_resolution: ScannerPrefixReuseSnapshot,
    pub prefix_cycle_completion: ScannerPrefixReuseSnapshot,
    pub scope_fallbacks: BTreeMap<String, u64>,
    /// Rejections only for cycles with a baseline_unavailable scope fallback.
    pub authoritative_baseline_rejections: BTreeMap<String, u64>,
    pub observed_baseline_rejections: BTreeMap<String, u64>,
}

#[derive(Default)]
pub struct ScannerScanObservation {
    metadata: [AtomicU64; 7],
    cache_count: [AtomicU64; 4],
    cache_nanos: [AtomicU64; 4],
    erasure_clean_prefix_walks: AtomicU64,
    prefix_evaluated: [AtomicU64; 2],
    prefix_activated: [AtomicU64; 2],
    prefix_blockers: [[AtomicU64; PREFIX_REUSE_BLOCKERS.len()]; 2],
    scope_fallbacks: [AtomicU64; SCOPE_FALLBACK_REASONS.len()],
    baseline_rejections: [[AtomicU64; ScannerBaselineRejection::ALL.len()]; 2],
}

fn duration_nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn labelled_counts(labels: impl IntoIterator<Item = &'static str>, counters: &[AtomicU64]) -> BTreeMap<String, u64> {
    labels
        .into_iter()
        .zip(counters)
        .map(|(label, count)| (label.to_owned(), count.load(Ordering::Relaxed)))
        .collect()
}

impl ScannerScanObservation {
    pub(crate) fn start_metadata_read(&self) {
        self.metadata[0].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn finish_metadata_read(&self, outcome: ScannerMetadataReadOutcome, bytes: u64, elapsed: Duration) {
        self.metadata[1 + outcome as usize].fetch_add(1, Ordering::Relaxed);
        if outcome == ScannerMetadataReadOutcome::Success {
            self.metadata[5].fetch_add(bytes, Ordering::Relaxed);
        }
        self.metadata[6].fetch_add(duration_nanos(elapsed), Ordering::Relaxed);
    }

    pub fn record_cache_reuse(&self, kind: ScannerCacheReuseKind, elapsed: Duration) {
        self.cache_count[kind as usize].fetch_add(1, Ordering::Relaxed);
        self.cache_nanos[kind as usize].fetch_add(duration_nanos(elapsed), Ordering::Relaxed);
    }

    pub fn record_erasure_clean_prefix_walk(&self) {
        self.erasure_clean_prefix_walks.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_prefix_reuse(&self, stage: ScannerPrefixReuseStage, activated: bool, blockers: &[Option<&str>; 8]) {
        let stage = stage as usize;
        self.prefix_evaluated[stage].fetch_add(1, Ordering::Relaxed);
        if activated {
            self.prefix_activated[stage].fetch_add(1, Ordering::Relaxed);
        }
        for blocker in blockers.iter().flatten() {
            if let Some(index) = PREFIX_REUSE_BLOCKERS.iter().position(|label| label == blocker) {
                self.prefix_blockers[stage][index].fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub(crate) fn record_scope_fallback(&self, reason: &str) {
        if let Some(index) = SCOPE_FALLBACK_REASONS.iter().position(|label| *label == reason) {
            self.scope_fallbacks[index].fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_baseline_rejection(&self, authoritative: ScannerBaselineRejection, observed: Option<ScannerBaselineRejection>) {
        self.baseline_rejections[0][authoritative as usize].fetch_add(1, Ordering::Relaxed);
        if let Some(observed) = observed {
            self.baseline_rejections[1][observed as usize].fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn snapshot(&self) -> ScannerScanObservationSnapshot {
        let prefix = |stage: ScannerPrefixReuseStage| ScannerPrefixReuseSnapshot {
            evaluated: self.prefix_evaluated[stage as usize].load(Ordering::Relaxed),
            activated: self.prefix_activated[stage as usize].load(Ordering::Relaxed),
            blockers: labelled_counts(PREFIX_REUSE_BLOCKERS, &self.prefix_blockers[stage as usize]),
        };
        ScannerScanObservationSnapshot {
            metadata_reads: ScannerMetadataReadSnapshot {
                attempts: self.metadata[0].load(Ordering::Relaxed),
                succeeded: self.metadata[1].load(Ordering::Relaxed),
                not_found: self.metadata[2].load(Ordering::Relaxed),
                errors: self.metadata[3].load(Ordering::Relaxed),
                cancelled: self.metadata[4].load(Ordering::Relaxed),
                bytes: self.metadata[5].load(Ordering::Relaxed),
                elapsed_nanos: self.metadata[6].load(Ordering::Relaxed),
            },
            cache_reuse: ScannerCacheReuseKind::ALL
                .into_iter()
                .map(|kind| {
                    (
                        kind.label().to_owned(),
                        ScannerCacheReuseSnapshot {
                            count: self.cache_count[kind as usize].load(Ordering::Relaxed),
                            elapsed_nanos: self.cache_nanos[kind as usize].load(Ordering::Relaxed),
                        },
                    )
                })
                .collect(),
            erasure_clean_prefix_walks: self.erasure_clean_prefix_walks.load(Ordering::Relaxed),
            prefix_scope_resolution: prefix(ScannerPrefixReuseStage::ScopeResolution),
            prefix_cycle_completion: prefix(ScannerPrefixReuseStage::CycleCompletion),
            scope_fallbacks: labelled_counts(SCOPE_FALLBACK_REASONS, &self.scope_fallbacks),
            authoritative_baseline_rejections: labelled_counts(
                ScannerBaselineRejection::ALL.map(ScannerBaselineRejection::label),
                &self.baseline_rejections[0],
            ),
            observed_baseline_rejections: labelled_counts(
                ScannerBaselineRejection::ALL.map(ScannerBaselineRejection::label),
                &self.baseline_rejections[1],
            ),
        }
    }
}

/// Dropping an unfinished read records cancellation. The guard observes the
/// read future only; it cannot cancel it or change its storage result.
#[must_use]
pub struct ScannerMetadataReadGuard<'a> {
    pub(crate) metrics: &'a Metrics,
    pub(crate) started: Instant,
    pub(crate) outcome: ScannerMetadataReadOutcome,
    pub(crate) bytes: u64,
}

impl ScannerMetadataReadGuard<'_> {
    pub fn finish(mut self, outcome: ScannerMetadataReadOutcome, bytes: u64) {
        self.outcome = outcome;
        self.bytes = bytes;
    }
}

impl Drop for ScannerMetadataReadGuard<'_> {
    fn drop(&mut self) {
        self.metrics
            .finish_scanner_metadata_read(self.outcome, self.bytes, self.started.elapsed());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn metadata_read_reports_storage_results_and_existing_read_metric() {
        let metrics = Metrics::new();
        metrics
            .start_scanner_metadata_read()
            .finish(ScannerMetadataReadOutcome::Success, 127);
        metrics
            .start_scanner_metadata_read()
            .finish(ScannerMetadataReadOutcome::Success, 0);
        metrics
            .start_scanner_metadata_read()
            .finish(ScannerMetadataReadOutcome::NotFound, 0);
        metrics
            .start_scanner_metadata_read()
            .finish(ScannerMetadataReadOutcome::Error, 0);
        let report = metrics.report().await;
        let reads = &report.scan_observation.metadata_reads;
        assert_eq!(
            (reads.attempts, reads.succeeded, reads.not_found, reads.errors, reads.cancelled),
            (4, 2, 1, 1, 0)
        );
        assert_eq!(reads.bytes, 127);
        assert_eq!(report.life_time_ops["read_metadata"], 4);
        assert_eq!(report.last_minute.actions["read_metadata"].count, 4);
        assert_eq!(report.last_minute.actions["read_metadata"].bytes, 127);
    }

    #[tokio::test]
    async fn metadata_read_cancellation_balances_an_actually_polled_future_once() {
        let metrics = Arc::new(Metrics::new());
        let worker_metrics = Arc::clone(&metrics);
        let (started, admitted) = tokio::sync::oneshot::channel();
        let worker = tokio::spawn(async move {
            let read = worker_metrics.start_scanner_metadata_read();
            started.send(()).expect("observe started read");
            std::future::pending::<()>().await;
            read.finish(ScannerMetadataReadOutcome::Success, 99);
        });
        tokio::time::timeout(Duration::from_secs(5), admitted)
            .await
            .expect("read starts within deadline")
            .expect("read started");
        let in_flight = metrics.scan_observation().snapshot().metadata_reads;
        assert_eq!((in_flight.attempts, in_flight.cancelled, in_flight.succeeded), (1, 0, 0));
        worker.abort();
        assert!(worker.await.expect_err("read task was aborted").is_cancelled());
        let report = metrics.report().await;
        let reads = &report.scan_observation.metadata_reads;
        assert_eq!((reads.attempts, reads.cancelled, reads.succeeded, reads.bytes), (1, 1, 0, 0));
        assert_eq!(report.life_time_ops["read_metadata"], 1);
        assert_eq!(report.last_minute.actions["read_metadata"].count, 1);
        assert_eq!(
            Duration::from_nanos(reads.elapsed_nanos).as_secs(),
            report.last_minute.actions["read_metadata"].acc_time
        );
    }

    #[tokio::test]
    async fn observation_report_keeps_stages_reasons_and_copy_costs_separate() {
        let metrics = Metrics::new();
        let observation = metrics.scan_observation();
        observation.record_cache_reuse(ScannerCacheReuseKind::CleanPrefix, Duration::from_nanos(321));
        observation.record_erasure_clean_prefix_walk();
        observation.record_prefix_reuse(
            ScannerPrefixReuseStage::ScopeResolution,
            false,
            &[
                Some("missing_producer_identity"),
                Some("missing_durable_journal_replay"),
                None,
                None,
                None,
                None,
                None,
                None,
            ],
        );
        observation.record_prefix_reuse(ScannerPrefixReuseStage::CycleCompletion, true, &[None; 8]);
        metrics.record_scanner_scope_fallback("baseline_unavailable");
        observation.record_baseline_rejection(ScannerBaselineRejection::Epoch, Some(ScannerBaselineRejection::Missing));
        let report = metrics.report().await;
        let snapshot = &report.scan_observation;
        assert_eq!(snapshot.cache_reuse["clean_prefix"].count, 1);
        assert_eq!(snapshot.cache_reuse["clean_prefix"].elapsed_nanos, 321);
        assert_eq!(snapshot.cache_reuse["sampled_namespace"].count, 0);
        assert_eq!(snapshot.erasure_clean_prefix_walks, 1);
        assert_eq!(
            (snapshot.prefix_scope_resolution.evaluated, snapshot.prefix_scope_resolution.activated),
            (1, 0)
        );
        assert_eq!(snapshot.prefix_scope_resolution.blockers["missing_producer_identity"], 1);
        assert_eq!(snapshot.prefix_scope_resolution.blockers["missing_durable_journal_replay"], 1);
        assert_eq!(
            (snapshot.prefix_cycle_completion.evaluated, snapshot.prefix_cycle_completion.activated),
            (1, 1)
        );
        assert_eq!(
            snapshot.scope_fallbacks["baseline_unavailable"],
            report.usage_freshness.scope_fallback_total
        );
        assert_eq!(snapshot.authoritative_baseline_rejections["epoch"], 1);
        assert_eq!(snapshot.observed_baseline_rejections["missing"], 1);

        let mut legacy = serde_json::to_value(&report).expect("encode report");
        legacy
            .as_object_mut()
            .expect("report is an object")
            .remove("scan_observation");
        let decoded: crate::metrics::ScannerMetricsReport = serde_json::from_value(legacy).expect("read legacy report");
        assert!(decoded.scan_observation.cache_reuse.is_empty(), "legacy counters are unobserved");
        assert_eq!(decoded.scan_observation.metadata_reads.attempts, 0);
    }
}
