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

use crate::backends::{ExpiredKeyRemoval, KmsBackend};
use crate::types::{KeyStatus, ListKeysRequest};
use async_trait::async_trait;
use jiff::Zoned;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// How often the worker looks for expired pending deletions.
pub const DEFAULT_SWEEP_INTERVAL: Duration = Duration::from_secs(60);

/// Reports configuration that still references a KMS key.
///
/// Consulted before any material is destroyed; a non-empty result blocks the
/// removal until the references disappear. Implementations live where the
/// referencing configuration lives (for example bucket encryption settings in
/// the server) and are injected via
/// [`crate::service_manager::KmsServiceManager::set_deletion_reference_checker`].
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
}

pub(crate) struct DeletionWorker {
    backend: Arc<dyn KmsBackend>,
    default_key_id: Option<String>,
    reference_checker: Option<Arc<dyn DeletionReferenceChecker>>,
    interval: Duration,
}

impl DeletionWorker {
    pub(crate) fn new(
        backend: Arc<dyn KmsBackend>,
        default_key_id: Option<String>,
        reference_checker: Option<Arc<dyn DeletionReferenceChecker>>,
    ) -> Self {
        Self {
            backend,
            default_key_id,
            reference_checker,
            interval: DEFAULT_SWEEP_INTERVAL,
        }
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
            if !report.removed.is_empty() || !report.blocked.is_empty() || report.failed > 0 {
                info!(
                    removed = ?report.removed,
                    blocked = ?report.blocked,
                    skipped = report.skipped,
                    failed = report.failed,
                    "KMS deletion sweep completed"
                );
            }
        }
    }

    /// Run one sweep at the given time. Exposed separately so tests can drive
    /// the expiry logic deterministically.
    pub(crate) async fn sweep(&self, now: &Zoned) -> SweepReport {
        let mut report = SweepReport::default();
        let mut marker: Option<String> = None;
        loop {
            let request = ListKeysRequest {
                limit: Some(100),
                marker: marker.clone(),
                usage_filter: None,
                status_filter: None,
            };
            let response = match self.backend.list_keys(request).await {
                Ok(response) => response,
                Err(error) => {
                    warn!(%error, "KMS deletion sweep could not list keys");
                    report.failed += 1;
                    return report;
                }
            };
            for key in &response.keys {
                if matches!(key.status, KeyStatus::PendingDeletion | KeyStatus::Deleted) {
                    self.process_key(&key.key_id, now, &mut report).await;
                }
            }
            if !response.truncated {
                break;
            }
            match response.next_marker {
                Some(next_marker) => marker = Some(next_marker),
                None => break,
            }
        }
        report
    }

    async fn process_key(&self, key_id: &str, now: &Zoned, report: &mut SweepReport) {
        // Never remove a key that live configuration still points at. The
        // default key check is built in; broader references (bucket
        // encryption settings, ...) come from the injected checker.
        if self.default_key_id.as_deref() == Some(key_id) {
            warn!(key_id, "expired KMS key is still the default key; refusing removal");
            report.blocked.push(key_id.to_string());
            return;
        }
        if let Some(checker) = &self.reference_checker {
            let references = checker.references(key_id).await;
            if !references.is_empty() {
                warn!(key_id, ?references, "expired KMS key is still referenced; refusing removal");
                report.blocked.push(key_id.to_string());
                return;
            }
        }

        // The backend re-checks state and deadline under its own write
        // synchronization, so a cancellation racing this sweep wins there.
        match self.backend.remove_expired_key(key_id, now).await {
            Ok(ExpiredKeyRemoval::Removed) => report.removed.push(key_id.to_string()),
            Ok(ExpiredKeyRemoval::StateChanged | ExpiredKeyRemoval::NotExpired) => report.skipped += 1,
            Err(error) => {
                warn!(key_id, %error, "failed to remove expired KMS key; will retry next sweep");
                report.failed += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::KmsClient as _;
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
            })
            .await
            .expect("deletion should be scheduled");
    }

    fn worker(backend: Arc<LocalKmsBackend>) -> DeletionWorker {
        DeletionWorker::new(backend, None, None)
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
        let as_default = DeletionWorker::new(backend.clone(), Some(key_id.clone()), None);
        let report = as_default.sweep(&after_window()).await;
        assert_eq!(report.blocked, vec![key_id.clone()]);
        assert!(report.removed.is_empty());

        // Blocked while external configuration still references it.
        let with_references = DeletionWorker::new(
            backend.clone(),
            None,
            Some(Arc::new(StaticReferences(vec!["bucket:sse-bucket".to_string()]))),
        );
        let report = with_references.sweep(&after_window()).await;
        assert_eq!(report.blocked, vec![key_id.clone()]);
        assert!(report.removed.is_empty());
        backend
            .describe_key(DescribeKeyRequest { key_id: key_id.clone() })
            .await
            .expect("blocked key must still exist");

        // Removed once nothing references it anymore.
        let unreferenced = DeletionWorker::new(backend.clone(), None, Some(Arc::new(StaticReferences(Vec::new()))));
        let report = unreferenced.sweep(&after_window()).await;
        assert_eq!(report.removed, vec![key_id.clone()]);
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
}
