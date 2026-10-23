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

//! Assert exact exported terminal outcomes on the real candidate paths.

use super::*;
use rustfs_scanner_metrics::usage_candidate::{ScannerUsageCandidateKind as Kind, ScannerUsageCandidateOutcome as Outcome};
use std::sync::atomic::AtomicU64;

#[derive(Default)]
struct CandidateRecorder(StdMutex<HashMap<metrics::Key, Arc<AtomicU64>>>);

impl metrics::Recorder for CandidateRecorder {
    fn describe_counter(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}
    fn describe_gauge(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}
    fn describe_histogram(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}
    fn register_counter(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Counter {
        if key.name() != "rustfs_scanner_usage_candidates_total" {
            return metrics::Counter::noop();
        }
        metrics::Counter::from_arc(
            self.0
                .lock()
                .expect("candidate recorder")
                .entry(key.clone())
                .or_default()
                .clone(),
        )
    }
    fn register_gauge(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
        metrics::Gauge::noop()
    }
    fn register_histogram(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Histogram {
        metrics::Histogram::noop()
    }
}

fn assert_one_outcome<T>(kind: Kind, outcome: Outcome, operation: impl FnOnce() -> Option<T>) -> Option<T> {
    let recorder = CandidateRecorder::default();
    let value = metrics::with_local_recorder(&recorder, operation);
    assert_eq!(value.is_some(), outcome == Outcome::Accepted);
    let rows = recorder.0.lock().expect("candidate recorder");
    assert_eq!(rows.len(), 1, "one builder return must emit one terminal result, not its nested checks");
    let (key, count) = rows.iter().next().expect("terminal result");
    assert_eq!(count.load(Ordering::Relaxed), 1);
    let labels = key
        .labels()
        .map(|label| (label.key(), label.value()))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(labels, BTreeMap::from([("kind", kind.as_label()), ("outcome", outcome.as_label())]));
    value
}

fn completed_outcome(
    caches: &[DataUsageCache],
    buckets: &[String],
    sources: &HashSet<DataUsageCacheSource>,
    outcome: Outcome,
    plan_complete: bool,
    budget_elapsed: bool,
) {
    let scope = ScannerSnapshotScope {
        sources,
        buckets,
        identity: ScannerSnapshotIdentity {
            cycle: 0,
            leader_epoch: 0,
            plan_digest: TEST_PLAN_DIGEST,
            coverage_digest: TEST_COVERAGE_DIGEST,
            tier_registry_generation: None,
        },
    };
    assert_one_outcome(Kind::Completed, outcome, || {
        completed_usage_candidate(caches, &scope, &[], plan_complete, budget_elapsed, false)
    });
}

#[test]
fn usage_candidate_metrics_classify_completed_first_rejection_without_reordering_checks() {
    let source = DataUsageCacheSource::new(0, 0);
    let sources = HashSet::from([source]);
    let buckets = vec!["bucket".to_string()];
    let cache = completed_root_cache("bucket", 1, 10, source);
    completed_outcome(std::slice::from_ref(&cache), &buckets, &sources, Outcome::Accepted, true, false);
    // The same input also has generation/root defects. The original inventory gate wins.
    let mut broken = cache.clone();
    broken.info.next_cycle = 2;
    broken.cache.clear();
    completed_outcome(&[broken], &buckets, &sources, Outcome::InventoryIncomplete, false, true);
    completed_outcome(std::slice::from_ref(&cache), &buckets, &sources, Outcome::PublishGate, true, true);
    let mut broken = cache.clone();
    broken.info.snapshot_complete = false;
    completed_outcome(&[broken], &buckets, &sources, Outcome::IncompleteSets, true, false);
    let mut broken = cache.clone();
    broken.info.next_cycle = 2;
    completed_outcome(&[broken], &buckets, &sources, Outcome::GenerationMismatch, true, false);
    completed_outcome(
        std::slice::from_ref(&cache),
        &["bucket".to_string(), "bucket".to_string()],
        &sources,
        Outcome::InvalidInventory,
        true,
        false,
    );
    let mut broken = cache.clone();
    broken.cache.clear();
    completed_outcome(&[broken], &buckets, &sources, Outcome::InvalidRoot, true, false);
    let mut broken = cache;
    broken
        .cache
        .get_mut(&hash_path("bucket").key())
        .expect("bucket entry")
        .tier_accounting_proof = None;
    completed_outcome(&[broken], &buckets, &sources, Outcome::TierProof, true, false);
    let source2 = DataUsageCacheSource::new(0, 1);
    let mut max = completed_root_cache("bucket", 0, 10, source);
    max.cache.get_mut(&hash_path("bucket").key()).expect("bucket entry").objects = usize::MAX;
    let mut one = completed_root_cache("bucket", 0, 10, source2);
    one.cache.get_mut(&hash_path("bucket").key()).expect("bucket entry").objects = 1;
    completed_outcome(&[max, one], &buckets, &HashSet::from([source, source2]), Outcome::Merge, true, false);
}

#[test]
fn usage_candidate_metrics_classify_observational_rejections_and_keep_partial_acceptance() {
    let source = DataUsageCacheSource::new(0, 0);
    let source2 = DataUsageCacheSource::new(0, 1);
    let sources = HashSet::from([source]);
    let buckets = vec!["bucket".to_string()];
    let cache = completed_root_cache("bucket", 1, 10, source);
    let observed = |caches: &[DataUsageCache], sources: &HashSet<DataUsageCacheSource>, outcome| {
        assert_one_outcome(Kind::Observational, outcome, || {
            observational_data_usage_info(caches, sources, &buckets, &[], TEST_PLAN_DIGEST, 0, 0)
        })
    };
    let mut broken = cache.clone();
    broken.info.source = None;
    observed(&[broken], &sources, Outcome::MissingSource);
    observed(std::slice::from_ref(&cache), &HashSet::from([source2]), Outcome::SourceMembership);
    observed(&[cache.clone(), cache.clone()], &sources, Outcome::SourceMembership);
    observed(&[], &sources, Outcome::NoUsableSnapshots);
    let mut stale = cache.clone();
    stale.info.next_cycle = 1;
    observed(&[stale], &sources, Outcome::NoUsableSnapshots);
    let mut first = cache.clone();
    first.info.tier_registry_generation = Some(1);
    let mut second = completed_root_cache("bucket", 1, 10, source2);
    second.info.tier_registry_generation = Some(2);
    observed(&[first, second], &HashSet::from([source, source2]), Outcome::TierGeneration);
    let mut broken = cache.clone();
    broken.info.last_update = None;
    observed(&[broken], &sources, Outcome::MissingLastUpdate);
    let mut broken = cache.clone();
    broken
        .cache
        .get_mut(&hash_path("bucket").key())
        .expect("bucket entry")
        .tier_accounting_proof = None;
    observed(&[broken], &sources, Outcome::TierProof);
    let mut max = completed_root_cache("bucket", 0, 10, source);
    max.cache.get_mut(&hash_path("bucket").key()).expect("bucket entry").objects = usize::MAX;
    let mut one = completed_root_cache("bucket", 0, 10, source2);
    one.cache.get_mut(&hash_path("bucket").key()).expect("bucket entry").objects = 1;
    observed(&[max, one], &HashSet::from([source, source2]), Outcome::Merge);
    let candidate = observed(std::slice::from_ref(&cache), &sources, Outcome::Accepted)
        .expect("observational candidate")
        .0;
    assert_eq!(candidate.objects_total_count, 1);
    assert!(!candidate.usage_snapshot_complete);
    assert_eq!(candidate.usage_snapshot_converged, Some(false));
    // Missing bucket data remains the existing observational skip, not an invented rejection.
    let mut missing = cache;
    missing.cache.clear();
    let candidate = observed(&[missing], &sources, Outcome::Accepted)
        .expect("partial observational candidate")
        .0;
    assert_eq!(candidate.buckets_count, 0);
    assert!(!candidate.usage_snapshot_complete);
}

#[test]
fn usage_candidate_metrics_empty_namespace_does_not_make_missing_sources_authoritative() {
    let source = DataUsageCacheSource::new(0, 0);
    let sources = HashSet::from([source]);
    let identity = ScannerSnapshotIdentity {
        cycle: 0,
        leader_epoch: 0,
        plan_digest: TEST_PLAN_DIGEST,
        coverage_digest: TEST_COVERAGE_DIGEST,
        tier_registry_generation: None,
    };
    assert_one_outcome(Kind::Completed, Outcome::EmptyNamespaceGuard, || {
        empty_namespace_usage_candidate(&[], &sources, &HashMap::new(), identity)
    });
    let candidate = assert_one_outcome(Kind::Completed, Outcome::Accepted, || {
        empty_namespace_usage_candidate(&[], &sources, &HashMap::from([(source, Vec::new())]), identity)
    })
    .expect("validated empty namespace");
    let (candidate, expectation) = candidate.prepare(ScannerCycleStatus::Complete, &ScannerCyclePendingBuckets::new());
    assert_eq!(candidate.objects_total_count, 0);
    assert!(candidate.usage_snapshot_complete);
    assert!(expectation.is_some());
}

#[test]
fn usage_candidate_metrics_export_all_fixed_buckets_with_only_bounded_labels() {
    let metrics = Metrics::new();
    let recorder = CandidateRecorder::default();
    metrics::with_local_recorder(&recorder, || {
        for kind in Kind::ALL {
            for outcome in Outcome::ALL {
                metrics.record_usage_candidate(kind, outcome);
            }
        }
    });
    let rows = recorder.0.lock().expect("candidate recorder");
    assert_eq!(rows.len(), 38);
    for (key, value) in rows.iter() {
        assert_eq!(value.load(Ordering::Relaxed), 1);
        assert_eq!(key.labels().count(), 2);
    }
    let labels = metrics
        .usage_candidate_counts()
        .into_iter()
        .map(|row| row.outcome.as_label())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        labels,
        BTreeSet::from([
            "accepted",
            "inventory_incomplete",
            "publish_gate",
            "incomplete_sets",
            "generation_mismatch",
            "invalid_inventory",
            "invalid_root",
            "flatten",
            "merge",
            "tier_proof",
            "usage_counters",
            "set_state",
            "count_overflow",
            "missing_source",
            "source_membership",
            "no_usable_snapshots",
            "tier_generation",
            "missing_last_update",
            "empty_namespace_guard"
        ])
    );
}
