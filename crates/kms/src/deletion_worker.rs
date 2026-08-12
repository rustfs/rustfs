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

//! Background worker that completes scheduled key deletions.
//!
//! Every sweep lists keys, picks the ones whose persisted deletion deadline
//! has passed (plus tombstones left by a crashed removal) and hands each to
//! [`KmsBackend::remove_expired_key`], which re-checks state under the
//! backend's own synchronization. The sweep is idempotent and keeps no state
//! of its own, so it is safe to re-run after a restart and safe to run on
//! every node of a deployment concurrently — a key is only ever removed while
//! its (re-read) record is an expired pending deletion or a tombstone.

use crate::audit::{KmsAuditOperation, KmsAuditRecord, KmsAuditSink};
use crate::backends::{ExpiredKeyRemoval, KmsBackend};
use crate::error::Result;
use crate::types::{KeyInfo, KeyStatus, ListKeysRequest, OperationContext};
use async_trait::async_trait;
use jiff::Zoned;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// How often the worker looks for expired pending deletions.
pub const DEFAULT_SWEEP_INTERVAL: Duration = Duration::from_secs(60);

/// Keys requested per `ListKeys` call while sweeping. The sweep follows
/// `truncated`/`next_marker` until the key set is exhausted, so this bounds the
/// working set of one call, not how many keys a sweep can reach.
const SWEEP_PAGE_SIZE: u32 = 100;

// ---------------------------------------------------------------------------
// Metrics
//
// The sweep already pages through the whole key set, so everything below is
// derived from the pages it has in hand: observing the key lifecycle costs no
// extra backend call. Each metric is an aggregate — the gauges carry no labels
// at all and the counter only a static outcome — because a per-key label would
// carry key identifiers into the metric stream and grow the series count with
// the key set. "This key is overdue for rotation" is a threshold on the
// aggregate age and belongs in an alerting rule, not in a label.
// ---------------------------------------------------------------------------

/// Gauge: keys scheduled for deletion whose deadline has not passed, as of the
/// end of the last sweep that saw the whole key set.
const METRIC_PENDING_DELETION_KEYS: &str = "rustfs_kms_pending_deletion_keys";
/// Gauge: keys left tombstoned by an interrupted removal and still awaiting
/// the sweep, as of the end of the last sweep that saw the whole key set.
const METRIC_TOMBSTONE_KEYS: &str = "rustfs_kms_deletion_tombstone_keys";
/// Gauge: seconds since the least recently rotated usable key was rotated
/// (its creation time when it was never rotated); `0` when there are none.
const METRIC_OLDEST_ROTATION_AGE_SECONDS: &str = "rustfs_kms_oldest_key_rotation_age_seconds";
/// Gauge: the largest persisted wrap-operation reservation across usable keys,
/// as of the end of the last sweep that saw the whole key set. Published only
/// when the backend counts wraps (the Vault KV2 backend today); an aggregate
/// that by design overestimates actual wraps. The value to alert on against
/// the AES-256-GCM bound of 2^32 wraps per key material.
const METRIC_MAX_KEY_WRAP_OPERATIONS: &str = "rustfs_kms_max_key_wrap_operations";
/// Counter: keys the sweep acted on, by `outcome` (`removed`, `blocked`,
/// `skipped`, `failed`, `unreadable`).
const METRIC_SWEEP_KEYS_TOTAL: &str = "rustfs_kms_deletion_sweep_keys_total";

/// Register metric descriptions once per process.
fn describe_metrics() {
    static DESCRIBE: std::sync::Once = std::sync::Once::new();
    DESCRIBE.call_once(|| {
        metrics::describe_gauge!(
            METRIC_PENDING_DELETION_KEYS,
            "KMS keys scheduled for deletion whose deadline has not passed yet"
        );
        metrics::describe_gauge!(
            METRIC_TOMBSTONE_KEYS,
            "KMS keys left tombstoned by an interrupted removal, still awaiting the deletion sweep"
        );
        metrics::describe_gauge!(
            METRIC_OLDEST_ROTATION_AGE_SECONDS,
            "Seconds since the least recently rotated usable KMS key was last rotated, counting from creation for keys that were never rotated"
        );
        metrics::describe_gauge!(
            METRIC_MAX_KEY_WRAP_OPERATIONS,
            "Largest reserved wrap-operation count across usable KMS keys; overestimates actual wraps and is only reported by backends that count them"
        );
        metrics::describe_counter!(METRIC_SWEEP_KEYS_TOTAL, "Total keys acted on by the KMS deletion sweep, by outcome");
    });
}

/// Aggregate view of the key set, accumulated while the sweep pages through it.
///
/// Counts and one maximum age only: these feed label-less gauges, so no key
/// identifier can reach the metric stream through them.
#[derive(Debug, Default, Clone, Copy, PartialEq)]
struct KeyCensus {
    pending_deletion: usize,
    tombstones: usize,
    /// Longest time since last rotation across keys that are still usable,
    /// counting from creation for keys that were never rotated. Keys on their
    /// way out are excluded: they will never be rotated again, and would
    /// otherwise pin the gauge high until the sweep finishes removing them.
    oldest_rotation_age_seconds: f64,
    /// Largest reserved wrap count across usable keys, `None` when no key
    /// reported one — either the backend does not count wraps, or no usable
    /// key was seen. Excluding departing keys mirrors the rotation age: their
    /// material will never wrap again, so its consumed nonce budget is moot.
    max_wrap_operations: Option<u64>,
}

impl KeyCensus {
    fn observe(&mut self, key: &KeyInfo, now: &Zoned) {
        match key.status {
            KeyStatus::PendingDeletion => self.pending_deletion += 1,
            KeyStatus::Deleted => self.tombstones += 1,
            KeyStatus::Active | KeyStatus::Disabled => {
                if let Some(reserved) = key.wrap_budget_reserved {
                    self.max_wrap_operations = Some(self.max_wrap_operations.unwrap_or(0).max(reserved));
                }
                // A missing rotation time means either "never rotated" or "the
                // build that rotated it did not record when". Both fall back to
                // creation, and the two are not worth separate series: for the
                // first, creation *is* the correct baseline; for the second it
                // is an upper bound on the true age, so the gauge can only
                // overstate how overdue a key is, never hide an overdue one.
                // The overstatement is self-healing — the next rotation stamps
                // the record — and erring loud is the right direction for a
                // rotation-readiness alert. Backends never fabricate a
                // timestamp to close the gap, so nothing here can turn an
                // unstamped key into a freshly rotated one.
                let rotated_at = key.rotated_at.as_ref().unwrap_or(&key.created_at);
                self.oldest_rotation_age_seconds = self.oldest_rotation_age_seconds.max(seconds_between(rotated_at, now));
            }
        }
    }
}

/// Seconds from `earlier` to `now`, clamped at zero so clock skew (or a
/// timestamp persisted by a node running ahead) cannot produce a negative age.
fn seconds_between(earlier: &Zoned, now: &Zoned) -> f64 {
    (now.timestamp().as_second() - earlier.timestamp().as_second()).max(0) as f64
}

/// Publish one sweep's counters, plus the lifecycle gauges when `census` covers
/// the whole key set.
fn record_sweep(report: &SweepReport, census: Option<KeyCensus>) {
    describe_metrics();
    for (outcome, count) in [
        ("removed", report.removed.len()),
        ("blocked", report.blocked.len()),
        ("skipped", report.skipped),
        ("failed", report.failed),
        ("unreadable", report.unreadable),
    ] {
        // Emitted even at zero so every outcome series exists from the first
        // sweep on and a rate over it is defined.
        metrics::counter!(METRIC_SWEEP_KEYS_TOTAL, "outcome" => outcome).increment(count as u64);
    }

    // A sweep that could not finish listing saw only part of the key set;
    // publishing its counts would understate every gauge, so the previous
    // (complete) values are left standing instead.
    let Some(census) = census else { return };
    metrics::gauge!(METRIC_PENDING_DELETION_KEYS).set(census.pending_deletion as f64);
    metrics::gauge!(METRIC_TOMBSTONE_KEYS).set(census.tombstones as f64);
    metrics::gauge!(METRIC_OLDEST_ROTATION_AGE_SECONDS).set(census.oldest_rotation_age_seconds);
    // Only emitted when a usable key reported a count: backends that do not
    // count wraps must not publish a `0` that reads as "no wraps consumed".
    if let Some(max_wrap_operations) = census.max_wrap_operations {
        metrics::gauge!(METRIC_MAX_KEY_WRAP_OPERATIONS).set(max_wrap_operations as f64);
    }
}

/// Reports configuration that still references a KMS key.
///
/// Consulted before any material is destroyed; a non-empty result blocks the
/// removal until the references disappear. Implementations live where the
/// referencing configuration lives (for example bucket encryption settings in
/// the server) and are injected via
/// [`crate::service_manager::KmsServiceManager::set_deletion_reference_checker`].
///
/// # Blocks only
///
/// This gate is one-directional and must stay that way: a checker may add a
/// reason to keep material, never a reason to destroy it. Nothing that
/// enumerates key usage — least of all a scan whose coverage depends on how
/// far a background sweep got — may ever be wired in as a condition that
/// releases a removal this gate is holding. One incomplete scan would then be
/// enough to destroy the only copy of a key that still has data behind it,
/// which is why [`crate::key_impact::KeyImpactReport`] exposes no clearance
/// and is consumed only where a refusal is being decided.
#[async_trait]
pub trait DeletionReferenceChecker: Send + Sync {
    /// Identifiers of configuration still referencing `key_id` (bucket names,
    /// settings paths, ...). Errors must be reported as a reference so that
    /// an unavailable checker never unblocks a deletion.
    async fn references(&self, key_id: &str) -> Vec<String>;
}

/// Outcome of one sweep, for logging and tests.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SweepReport {
    /// Keys whose record and material were removed this sweep.
    pub removed: Vec<String>,
    /// Keys left in place because configuration still references them.
    pub blocked: Vec<String>,
    /// Keys that were pending but not yet due, without a persisted deadline,
    /// or whose state changed between inspection and removal.
    pub skipped: usize,
    /// Keys whose removal attempt failed; retried on the next sweep.
    pub failed: usize,
    /// Keys the backend listed but could not describe.
    ///
    /// The sweep keeps going past them — one damaged record must not stop every
    /// other expired key from being destroyed — but their existence means the
    /// key set was only partially observed, so the lifecycle gauges are
    /// withheld for this round rather than published over an incomplete census.
    pub unreadable: usize,
}

pub(crate) struct DeletionWorker {
    backend: Arc<dyn KmsBackend>,
    default_key_id: Option<String>,
    reference_checker: Option<Arc<dyn DeletionReferenceChecker>>,
    interval: Duration,
    backend_kind: &'static str,
    audit_sink: Option<Arc<dyn KmsAuditSink>>,
}

impl DeletionWorker {
    pub(crate) fn new(
        backend: Arc<dyn KmsBackend>,
        default_key_id: Option<String>,
        reference_checker: Option<Arc<dyn DeletionReferenceChecker>>,
        backend_kind: &'static str,
    ) -> Self {
        Self {
            backend,
            default_key_id,
            reference_checker,
            backend_kind,
            audit_sink: None,
            interval: DEFAULT_SWEEP_INTERVAL,
        }
    }

    /// Audit each irreversible key removal to `sink`.
    pub(crate) fn with_audit_sink(mut self, sink: Option<Arc<dyn KmsAuditSink>>) -> Self {
        self.audit_sink = sink;
        self
    }

    pub(crate) fn spawn(self, cancel: CancellationToken) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move { self.run(cancel).await })
    }

    async fn run(self, cancel: CancellationToken) {
        let mut ticker = tokio::time::interval(self.interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    debug!("KMS deletion worker stopped");
                    return;
                }
                _ = ticker.tick() => {}
            }
            let report = self.sweep(&Zoned::now()).await;
            if !report.removed.is_empty() || !report.blocked.is_empty() || report.failed > 0 || report.unreadable > 0 {
                info!(
                    removed = ?report.removed,
                    blocked = ?report.blocked,
                    skipped = report.skipped,
                    failed = report.failed,
                    unreadable = report.unreadable,
                    "KMS deletion sweep completed"
                );
            }
        }
    }

    /// Run one sweep at the given time. Exposed separately so tests can drive
    /// the expiry logic deterministically.
    ///
    /// Also feeds the lifecycle gauges: every key it lists is counted into a
    /// [`KeyCensus`] on the way past, so the observability comes out of the
    /// pages the sweep already had to fetch.
    pub(crate) async fn sweep(&self, now: &Zoned) -> SweepReport {
        let mut report = SweepReport::default();
        let mut census = KeyCensus::default();
        let mut marker: Option<String> = None;
        let listed_everything = loop {
            let request = ListKeysRequest {
                limit: Some(SWEEP_PAGE_SIZE),
                marker: marker.clone(),
                usage_filter: None,
                status_filter: None,
            };
            let response = match self.backend.list_keys(request).await {
                Ok(response) => response,
                Err(error) => {
                    warn!(%error, "KMS deletion sweep could not list keys");
                    report.failed += 1;
                    break false;
                }
            };
            if !response.unreadable_key_ids.is_empty() {
                warn!(
                    key_ids = ?response.unreadable_key_ids,
                    "KMS deletion sweep listed key records it cannot describe; census withheld this round"
                );
                report.unreadable += response.unreadable_key_ids.len();
            }
            for key in &response.keys {
                // Keys this sweep destroys are left out of the census: the
                // gauges describe the key set as it stands once the sweep is
                // done, not as it was when the page was listed.
                let removed = if matches!(key.status, KeyStatus::PendingDeletion | KeyStatus::Deleted) {
                    self.process_key(&key.key_id, now, &mut report).await
                } else {
                    false
                };
                if !removed {
                    census.observe(key, now);
                }
            }
            if !response.truncated {
                break true;
            }
            match response.next_marker {
                // A backend that hands back the cursor it was just given cannot
                // advance. Following it would re-list the same page forever, so
                // the sweep stops and reports the key set as partially seen.
                Some(ref next_marker) if marker.as_ref() == Some(next_marker) => {
                    warn!(marker = %next_marker, "KMS deletion sweep listing did not advance");
                    break false;
                }
                Some(next_marker) => marker = Some(next_marker),
                // Truncated without a marker: the rest of the key set is out
                // of reach, so the census is incomplete.
                None => break false,
            }
        };
        // A census taken over a key set with unreadable members would report an
        // oldest-rotation age and a pending-deletion count computed from the
        // keys that happened to be readable, which is exactly the kind of quiet
        // undercount the gauges exist to catch.
        let observed_every_key = listed_everything && report.unreadable == 0;
        record_sweep(&report, observed_every_key.then_some(census));
        report
    }

    /// Handle one key that is on its way out; reports whether it was removed.
    async fn process_key(&self, key_id: &str, now: &Zoned, report: &mut SweepReport) -> bool {
        // Never remove a key that live configuration still points at. The
        // default key check is built in; broader references (bucket
        // encryption settings, ...) come from the injected checker.
        if self.default_key_id.as_deref() == Some(key_id) {
            warn!(key_id, "expired KMS key is still the default key; refusing removal");
            report.blocked.push(key_id.to_string());
            return false;
        }
        if let Some(checker) = &self.reference_checker {
            let references = checker.references(key_id).await;
            if !references.is_empty() {
                warn!(key_id, ?references, "expired KMS key is still referenced; refusing removal");
                report.blocked.push(key_id.to_string());
                return false;
            }
        }

        // The backend re-checks state and deadline under its own write
        // synchronization, so a cancellation racing this sweep wins there.
        let started = Instant::now();
        let outcome = self.backend.remove_expired_key(key_id, now).await;
        match &outcome {
            Ok(ExpiredKeyRemoval::Removed) => {
                report.removed.push(key_id.to_string());
                self.audit_removal(key_id, started, &outcome);
                true
            }
            Ok(ExpiredKeyRemoval::StateChanged | ExpiredKeyRemoval::NotExpired) => {
                report.skipped += 1;
                false
            }
            Err(error) => {
                warn!(key_id, %error, "failed to remove expired KMS key; will retry next sweep");
                report.failed += 1;
                self.audit_removal(key_id, started, &outcome);
                false
            }
        }
    }

    /// Record an attempted destruction of key material.
    ///
    /// Only attempts that reached the backend are audited: a key the sweep
    /// declined to touch (not yet due, still referenced, state changed under
    /// us) was never at risk, and recording it as a deletion event would
    /// drown the real ones.
    fn audit_removal(&self, key_id: &str, started: Instant, outcome: &Result<ExpiredKeyRemoval>) {
        let Some(sink) = self.audit_sink.as_ref() else {
            return;
        };

        // Removal runs on a background sweep, so there is no request
        // principal to attribute it to.
        let context = OperationContext::internal();
        sink.emit(
            KmsAuditRecord::new(KmsAuditOperation::DeleteKey, &context, self.backend_kind)
                .with_key_id(Some(key_id))
                .with_latency(started.elapsed())
                .with_result(outcome),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::local::LocalKmsBackend;
    use crate::config::KmsConfig;
    use crate::error::KmsError;
    use crate::types::{CreateKeyRequest, DeleteKeyRequest, DescribeKeyRequest, KeyState, KeyUsage};

    async fn local_backend(temp_dir: &tempfile::TempDir) -> Arc<LocalKmsBackend> {
        let config = KmsConfig::local(temp_dir.path().to_path_buf()).with_insecure_development_defaults();
        Arc::new(LocalKmsBackend::new(config).await.expect("local backend should build"))
    }

    async fn create_key(backend: &LocalKmsBackend, key_name: &str) -> String {
        backend
            .create_key(CreateKeyRequest {
                key_name: Some(key_name.to_string()),
                key_usage: KeyUsage::EncryptDecrypt,
                ..Default::default()
            })
            .await
            .expect("key should be created")
            .key_id
    }

    async fn schedule(backend: &LocalKmsBackend, key_id: &str) {
        backend
            .delete_key(DeleteKeyRequest {
                key_id: key_id.to_string(),
                pending_window_in_days: Some(7),
                force_immediate: None,
                confirm_key_id: None,
            })
            .await
            .expect("deletion should be scheduled");
    }

    fn worker(backend: Arc<LocalKmsBackend>) -> DeletionWorker {
        DeletionWorker::new(backend, None, None, "local")
    }

    fn after_window() -> Zoned {
        Zoned::now() + Duration::from_secs(8 * 86400)
    }

    async fn assert_key_gone(backend: &LocalKmsBackend, key_id: &str) {
        let error = backend
            .describe_key(DescribeKeyRequest {
                key_id: key_id.to_string(),
            })
            .await
            .expect_err("removed key must not be describable");
        assert!(matches!(error, KmsError::KeyNotFound { .. }), "expected KeyNotFound, got {error:?}");
    }

    #[tokio::test]
    async fn sweep_removes_expired_pending_key_and_is_idempotent() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let backend = local_backend(&temp_dir).await;
        let key_id = create_key(&backend, "expired-key").await;
        schedule(&backend, &key_id).await;

        let worker = worker(backend.clone());

        // Not yet due: nothing happens.
        let report = worker.sweep(&Zoned::now()).await;
        assert!(report.removed.is_empty());
        assert_eq!(report.skipped, 1);
        assert_eq!(report.failed, 0);

        // Past the deadline: the key is removed.
        let report = worker.sweep(&after_window()).await;
        assert_eq!(report.removed, vec![key_id.clone()]);
        assert_eq!(report.failed, 0);
        assert_key_gone(&backend, &key_id).await;

        // Re-running the sweep after the removal is a no-op.
        let report = worker.sweep(&after_window()).await;
        assert_eq!(report, SweepReport::default());
    }

    /// Schedule `count` keys for deletion and report their identifiers.
    async fn schedule_keys(backend: &LocalKmsBackend, count: usize) -> Vec<String> {
        let mut key_ids = Vec::with_capacity(count);
        for index in 0..count {
            let key_id = create_key(backend, &format!("expiring-{index:04}")).await;
            schedule(backend, &key_id).await;
            key_ids.push(key_id);
        }
        key_ids
    }

    /// A deployment with more keys than fit in one listing page must still have
    /// every expired key destroyed: the sweep has to follow the cursor instead
    /// of stopping at the first page.
    #[tokio::test]
    async fn sweep_removes_expired_keys_beyond_the_first_page() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let backend = local_backend(&temp_dir).await;
        let total = SWEEP_PAGE_SIZE as usize + 5;
        let key_ids = schedule_keys(&backend, total).await;

        let report = worker(backend.clone()).sweep(&after_window()).await;
        assert_eq!(report.failed, 0);
        assert_eq!(
            report.removed.len(),
            total,
            "every expired key must be swept, not just the ones on the first page"
        );
        // The keys sorting last are the ones a first-page-only sweep leaves
        // behind for good.
        for key_id in key_ids.iter().rev().take(5) {
            assert_key_gone(&backend, key_id).await;
        }
    }

    #[tokio::test]
    async fn cancelled_deletion_always_beats_the_sweep() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let backend = local_backend(&temp_dir).await;
        let cancelled = create_key(&backend, "cancelled-key").await;
        let doomed = create_key(&backend, "doomed-key").await;
        schedule(&backend, &cancelled).await;
        schedule(&backend, &doomed).await;

        backend
            .cancel_key_deletion(crate::types::CancelKeyDeletionRequest {
                key_id: cancelled.clone(),
            })
            .await
            .expect("cancel should succeed");

        let report = worker(backend.clone()).sweep(&after_window()).await;
        assert_eq!(report.removed, vec![doomed.clone()]);
        assert_eq!(report.failed, 0);

        // The cancelled key survives, enabled and usable.
        let described = backend
            .describe_key(DescribeKeyRequest {
                key_id: cancelled.clone(),
            })
            .await
            .expect("cancelled key must still exist");
        assert_eq!(described.key_metadata.key_state, KeyState::Enabled);
        assert_key_gone(&backend, &doomed).await;
    }

    #[tokio::test]
    async fn default_key_and_external_references_block_removal() {
        struct StaticReferences(Vec<String>);

        #[async_trait]
        impl DeletionReferenceChecker for StaticReferences {
            async fn references(&self, _key_id: &str) -> Vec<String> {
                self.0.clone()
            }
        }

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let backend = local_backend(&temp_dir).await;
        let key_id = create_key(&backend, "referenced-key").await;
        schedule(&backend, &key_id).await;

        // Blocked while it is the configured default key.
        let as_default = DeletionWorker::new(backend.clone(), Some(key_id.clone()), None, "local");
        let report = as_default.sweep(&after_window()).await;
        assert_eq!(report.blocked, vec![key_id.clone()]);
        assert!(report.removed.is_empty());

        // Blocked while external configuration still references it.
        let with_references = DeletionWorker::new(
            backend.clone(),
            None,
            Some(Arc::new(StaticReferences(vec!["bucket:sse-bucket".to_string()]))),
            "local",
        );
        let report = with_references.sweep(&after_window()).await;
        assert_eq!(report.blocked, vec![key_id.clone()]);
        assert!(report.removed.is_empty());
        backend
            .describe_key(DescribeKeyRequest { key_id: key_id.clone() })
            .await
            .expect("blocked key must still exist");

        // Removed once nothing references it anymore.
        let unreferenced = DeletionWorker::new(backend.clone(), None, Some(Arc::new(StaticReferences(Vec::new()))), "local");
        let report = unreferenced.sweep(&after_window()).await;
        assert_eq!(report.removed, vec![key_id.clone()]);
    }

    #[tokio::test]
    async fn only_attempted_removals_are_audited() {
        #[derive(Default)]
        struct CapturingSink {
            records: std::sync::Mutex<Vec<KmsAuditRecord>>,
        }

        impl KmsAuditSink for CapturingSink {
            fn emit(&self, record: KmsAuditRecord) {
                self.records.lock().expect("sink lock should not be poisoned").push(record);
            }
        }

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let backend = local_backend(&temp_dir).await;
        let key_id = create_key(&backend, "audited-removal").await;
        schedule(&backend, &key_id).await;

        let sink = Arc::new(CapturingSink::default());
        let worker = DeletionWorker::new(backend.clone(), None, None, "local").with_audit_sink(Some(sink.clone()));

        // A key that is not yet due was never at risk, so it produces no record.
        worker.sweep(&Zoned::now()).await;
        assert!(
            sink.records.lock().expect("sink lock").is_empty(),
            "a key the sweep declined to touch must not be audited as a deletion"
        );

        worker.sweep(&after_window()).await;

        let records = sink.records.lock().expect("sink lock");
        assert_eq!(records.len(), 1, "the removal should be audited exactly once");
        let record = &records[0];
        assert_eq!(record.operation, crate::audit::KmsAuditOperation::DeleteKey);
        assert_eq!(record.event, rustfs_s3_types::EventName::KmsKeyDeleted);
        assert_eq!(record.outcome, crate::audit::KmsAuditOutcome::Success);
        assert_eq!(record.key_id.as_deref(), Some(key_id.as_str()));
        assert_eq!(record.backend, "local");
        // Background work has no request principal to attribute.
        assert_eq!(record.principal, OperationContext::INTERNAL_PRINCIPAL);
    }

    #[tokio::test]
    async fn deadline_survives_backend_restart_and_sweep_completes_it() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let key_id;
        {
            let backend = local_backend(&temp_dir).await;
            key_id = create_key(&backend, "restart-key").await;
            schedule(&backend, &key_id).await;
        }

        // "Restart": a fresh backend over the same directory must still see
        // the persisted deadline...
        let backend = local_backend(&temp_dir).await;
        let described = backend
            .describe_key(DescribeKeyRequest { key_id: key_id.clone() })
            .await
            .expect("key must survive the restart");
        assert_eq!(described.key_metadata.key_state, KeyState::PendingDeletion);
        assert!(
            described.key_metadata.deletion_date.is_some(),
            "deletion deadline must survive a backend restart"
        );

        // ...and the worker completes the deletion without any new schedule call.
        let report = worker(backend.clone()).sweep(&after_window()).await;
        assert_eq!(report.removed, vec![key_id.clone()]);
        assert_key_gone(&backend, &key_id).await;
    }

    #[tokio::test(start_paused = true)]
    async fn worker_loop_removes_due_keys_and_stops_on_cancel() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let backend = local_backend(&temp_dir).await;
        let key_id = create_key(&backend, "loop-key").await;
        // A zero-day window through the lifecycle client produces a deadline
        // that is already due for the worker's wall-clock sweep.
        backend
            .lifecycle_client()
            .schedule_key_deletion(&key_id, 0, None)
            .await
            .expect("schedule with zero window");

        let cancel = CancellationToken::new();
        let task = worker(backend.clone()).spawn(cancel.clone());

        // The paused clock auto-advances through the worker's interval ticks.
        let mut removed = false;
        for _ in 0..100 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if backend
                .describe_key(DescribeKeyRequest { key_id: key_id.clone() })
                .await
                .is_err()
            {
                removed = true;
                break;
            }
        }
        assert!(removed, "worker loop must remove the due key");

        cancel.cancel();
        task.await.expect("worker task must stop after cancellation");
    }

    // -- Metric emission ----------------------------------------------------

    use metrics_util::MetricKind;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    type MetricEntry = (
        metrics_util::CompositeKey,
        Option<metrics::Unit>,
        Option<metrics::SharedString>,
        DebugValue,
    );

    /// Run `test` on a current-thread runtime under a debugging recorder and
    /// return one snapshot of everything it emitted.
    ///
    /// A single snapshot per test on purpose: `Snapshotter::snapshot` drains
    /// the recorded state, so taking it per assertion would only show the
    /// first assertion any data.
    fn record_metrics<Out>(
        test: impl FnOnce() -> std::pin::Pin<Box<dyn std::future::Future<Output = Out>>>,
    ) -> (Vec<MetricEntry>, Out) {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let out = metrics::with_local_recorder(&recorder, || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("current-thread runtime must build");
            runtime.block_on(test())
        });
        (snapshotter.snapshot().into_vec(), out)
    }

    fn gauge_value(snapshot: &[MetricEntry], name: &str) -> Option<f64> {
        snapshot.iter().find_map(|(composite, _unit, _description, value)| {
            let matches = composite.kind() == MetricKind::Gauge && composite.key().name() == name;
            match (matches, value) {
                (true, DebugValue::Gauge(value)) => Some(value.into_inner()),
                _ => None,
            }
        })
    }

    fn counter_value(snapshot: &[MetricEntry], name: &str, outcome: &str) -> u64 {
        snapshot
            .iter()
            .filter_map(|(composite, _unit, _description, value)| {
                let matches = composite.kind() == MetricKind::Counter
                    && composite.key().name() == name
                    && composite
                        .key()
                        .labels()
                        .any(|label| label.key() == "outcome" && label.value() == outcome);
                match (matches, value) {
                    (true, DebugValue::Counter(count)) => Some(*count),
                    _ => None,
                }
            })
            .sum()
    }

    fn key_info(key_id: &str, status: KeyStatus, created_at: Zoned, rotated_at: Option<Zoned>) -> KeyInfo {
        KeyInfo {
            key_id: key_id.to_string(),
            description: None,
            algorithm: "AES-256".to_string(),
            usage: KeyUsage::EncryptDecrypt,
            status,
            version: 1,
            metadata: std::collections::HashMap::new(),
            tags: std::collections::HashMap::new(),
            created_at,
            rotated_at,
            created_by: None,
            rotation_due: false,
            rotation_due_reason: None,
            wrap_budget_reserved: None,
        }
    }

    #[test]
    fn census_ages_from_rotation_and_ignores_departing_keys() {
        let now = Zoned::now();
        let day = Duration::from_secs(86400);
        let mut census = KeyCensus::default();

        // Rotation beats creation as the age baseline...
        census.observe(
            &key_info("rotated", KeyStatus::Active, now.clone() - 30 * day, Some(now.clone() - 2 * day)),
            &now,
        );
        // ...and a key that was never rotated ages from its creation.
        census.observe(&key_info("never-rotated", KeyStatus::Disabled, now.clone() - 5 * day, None), &now);
        // Keys on their way out only ever move the counts: they will not be
        // rotated again, so their age must not drive the rotation gauge.
        census.observe(&key_info("pending", KeyStatus::PendingDeletion, now.clone() - 400 * day, None), &now);
        census.observe(&key_info("tombstone", KeyStatus::Deleted, now.clone() - 400 * day, None), &now);

        assert_eq!(census.pending_deletion, 1);
        assert_eq!(census.tombstones, 1);
        let expected = 5.0 * 86400.0;
        assert!(
            (census.oldest_rotation_age_seconds - expected).abs() < 1.0,
            "expected the never-rotated key to set the age, got {}",
            census.oldest_rotation_age_seconds
        );
    }

    /// The wrap census is the max over usable keys that report a counter.
    /// Keys without one (backends that do not count wraps) leave it `None`
    /// rather than dragging in a zero, and departing keys are excluded — their
    /// material never wraps again, so its consumed nonce budget is moot.
    #[test]
    fn census_takes_the_max_wrap_reservation_of_usable_keys_only() {
        let now = Zoned::now();
        let mut census = KeyCensus::default();

        census.observe(&key_info("uncounted", KeyStatus::Active, now.clone(), None), &now);
        assert_eq!(census.max_wrap_operations, None, "a key without a counter must not report zero");

        let mut low = key_info("low", KeyStatus::Active, now.clone(), None);
        low.wrap_budget_reserved = Some(1_000_000);
        let mut high = key_info("high", KeyStatus::Disabled, now.clone(), None);
        high.wrap_budget_reserved = Some(3_000_000);
        let mut departing = key_info("departing", KeyStatus::PendingDeletion, now.clone(), None);
        departing.wrap_budget_reserved = Some(9_000_000);
        census.observe(&low, &now);
        census.observe(&high, &now);
        census.observe(&departing, &now);

        assert_eq!(census.max_wrap_operations, Some(3_000_000));
    }

    /// The wrap gauge is a single aggregate: one value, no labels at all — a
    /// per-key label would carry key identifiers into the metric stream and
    /// grow the series count with the key set.
    #[test]
    fn wrap_budget_gauge_is_aggregate_and_carries_no_key_label() {
        let (snapshot, ()) = record_metrics(|| {
            Box::pin(async {
                let now = Zoned::now();
                let mut census = KeyCensus::default();
                let mut wrapped = key_info("wrapped-key-id", KeyStatus::Active, now.clone(), None);
                wrapped.wrap_budget_reserved = Some(2_000_000);
                census.observe(&wrapped, &now);
                record_sweep(&SweepReport::default(), Some(census));
            })
        });

        assert_eq!(gauge_value(&snapshot, METRIC_MAX_KEY_WRAP_OPERATIONS), Some(2_000_000.0));
        for (composite, ..) in &snapshot {
            if composite.key().name() == METRIC_MAX_KEY_WRAP_OPERATIONS {
                assert_eq!(composite.key().labels().count(), 0, "the wrap gauge must stay label-less");
            }
            for label in composite.key().labels() {
                assert!(
                    !label.value().contains("wrapped-key-id"),
                    "metric {} leaked a key identifier through label {}",
                    composite.key().name(),
                    label.key()
                );
            }
        }
    }

    #[test]
    fn sweep_publishes_lifecycle_gauges_without_key_labels() {
        let (snapshot, key_ids) = record_metrics(|| {
            Box::pin(async {
                let temp_dir = tempfile::tempdir().expect("temp dir");
                let backend = local_backend(&temp_dir).await;
                let live = create_key(&backend, "live-key").await;
                let doomed = create_key(&backend, "doomed-key").await;
                schedule(&backend, &doomed).await;

                // Three days in, still inside the seven-day window: the sweep
                // observes the scheduled key instead of removing it.
                let report = worker(backend.clone())
                    .sweep(&(Zoned::now() + Duration::from_secs(3 * 86400)))
                    .await;
                assert_eq!(report.skipped, 1);
                assert!(report.removed.is_empty());
                vec![live, doomed]
            })
        });

        assert_eq!(gauge_value(&snapshot, METRIC_PENDING_DELETION_KEYS), Some(1.0));
        assert_eq!(gauge_value(&snapshot, METRIC_TOMBSTONE_KEYS), Some(0.0));
        let age = gauge_value(&snapshot, METRIC_OLDEST_ROTATION_AGE_SECONDS).expect("rotation age gauge must be published");
        // Both keys were just created, and only the usable one counts, so the
        // reported age is the sweep's own offset into the future.
        assert!(
            (age - 3.0 * 86400.0).abs() < 60.0,
            "expected the sweep offset as the rotation age, got {age}"
        );
        assert_eq!(counter_value(&snapshot, METRIC_SWEEP_KEYS_TOTAL, "skipped"), 1);
        assert_eq!(counter_value(&snapshot, METRIC_SWEEP_KEYS_TOTAL, "removed"), 0);
        assert_eq!(
            gauge_value(&snapshot, METRIC_MAX_KEY_WRAP_OPERATIONS),
            None,
            "a backend that does not count wraps must not publish a wrap gauge that reads as zero consumption"
        );

        for (composite, ..) in &snapshot {
            for label in composite.key().labels() {
                for key_id in &key_ids {
                    assert!(
                        !label.value().contains(key_id.as_str()),
                        "metric {} leaked a key identifier through label {}",
                        composite.key().name(),
                        label.key()
                    );
                }
            }
        }
    }

    /// The census counts the whole key set, not the first page of it. A sweep
    /// that stops after one page would publish a gauge that understates every
    /// large deployment by exactly the keys it never listed.
    #[test]
    fn lifecycle_gauges_count_keys_beyond_the_first_page() {
        let total = SWEEP_PAGE_SIZE as usize + 5;
        let (snapshot, ()) = record_metrics(|| {
            Box::pin(async move {
                let temp_dir = tempfile::tempdir().expect("temp dir");
                let backend = local_backend(&temp_dir).await;
                schedule_keys(&backend, total).await;

                // Well inside the seven-day window: every key is observed as
                // pending rather than removed.
                let report = worker(backend.clone()).sweep(&Zoned::now()).await;
                assert_eq!(report.skipped, total, "every scheduled key must be inspected");
                assert!(report.removed.is_empty());
            })
        });

        assert_eq!(gauge_value(&snapshot, METRIC_PENDING_DELETION_KEYS), Some(total as f64));
    }

    /// One key record this build cannot read must not stop the sweep.
    ///
    /// Before the listing reported unreadable identifiers, the damaged record
    /// either vanished from the page — so the census counted a key set it had
    /// not fully seen — or failed the listing outright, which aborted the sweep
    /// and left every expired key on the node undeleted for as long as the
    /// damage lasted. Now the expired key is still destroyed, and the gauges are
    /// withheld for the round instead of being published over a partial census.
    #[test]
    fn sweep_destroys_expired_keys_past_a_record_it_cannot_read() {
        let (snapshot, ()) = record_metrics(|| {
            Box::pin(async move {
                let temp_dir = tempfile::tempdir().expect("temp dir");
                let backend = local_backend(&temp_dir).await;
                let expired = create_key(&backend, "zz-expired").await;
                schedule(&backend, &expired).await;
                let damaged = create_key(&backend, "aa-damaged").await;

                // Stamp a protection marker no build understands, exactly as a
                // record written by a newer node would look here.
                let key_path = temp_dir.path().join(format!("{damaged}.key"));
                assert!(key_path.exists(), "the key record must exist before it is damaged");
                let mut record: serde_json::Value =
                    serde_json::from_slice(&tokio::fs::read(&key_path).await.expect("read record")).expect("decode record");
                record["at_rest_protection"] = serde_json::json!({ "future_mode": ["opaque"] });
                tokio::fs::write(&key_path, serde_json::to_vec_pretty(&record).expect("encode record"))
                    .await
                    .expect("write record");

                let report = worker(backend.clone()).sweep(&after_window()).await;
                assert_eq!(report.unreadable, 1, "the damaged record must be reported, not hidden");
                assert_eq!(report.removed, vec![expired.clone()], "the expired key must still be destroyed");
                assert_eq!(report.failed, 0, "an unreadable record is not a removal failure");
                assert_key_gone(&backend, &expired).await;
            })
        });

        assert_eq!(
            gauge_value(&snapshot, METRIC_PENDING_DELETION_KEYS),
            None,
            "a census taken over a partially readable key set must not be published"
        );
        // The runbook points operators at this series as the signal that the
        // gauges above have gone quiet on purpose, so it has to be emitted.
        assert_eq!(counter_value(&snapshot, METRIC_SWEEP_KEYS_TOTAL, "unreadable"), 1);
        assert_eq!(counter_value(&snapshot, METRIC_SWEEP_KEYS_TOTAL, "removed"), 1);
    }
}
