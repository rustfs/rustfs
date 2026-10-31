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
/// Data-usage snapshot persistence: CAS store pipeline, epoch baselines, and observed-snapshot cleanup.
use super::*;
use crate::data_usage_define::usage_floor_primary_read_error_allows_backup;
use crate::storage_api::owner::ScannerPublicationCommitState;
use rustfs_scanner_metrics::metrics::{
    ScannerUsagePublicationClass, ScannerUsagePublicationOutcome, ScannerUsagePublicationStatus,
};
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) enum DataUsagePersistOutcome {
    #[default]
    NoUpdate,
    Current,
    AlreadyDurable,
    PriorCycleDurable,
    Saved,
    /// The metadata route is temporarily unavailable (for example while a
    /// terminal decommission state keeps the source pool suspended).  The
    /// caller must retry without acknowledging dirty usage.
    Deferred(ScannerCycleDeferReason),
    Failed,
}

#[derive(Debug)]
pub(crate) struct RootPublicationProof {
    candidate: crate::scanner_io::ScannerPublicationExpectation,
    root_version: (String, [u8; 32]),
}

impl RootPublicationProof {
    pub(crate) fn verified_version_for(
        &self,
        expected: &crate::scanner_io::ScannerPublicationExpectation,
    ) -> Option<&(String, [u8; 32])> {
        self.candidate.same_candidate(expected).then_some(&self.root_version)
    }
}

#[derive(Debug)]
pub(super) struct DataUsagePublicationResult {
    outcome: DataUsagePersistOutcome,
    proof: Option<RootPublicationProof>,
    observation: Option<ScannerUsagePublicationStatus>,
}

impl From<DataUsagePersistOutcome> for DataUsagePublicationResult {
    fn from(outcome: DataUsagePersistOutcome) -> Self {
        Self {
            outcome,
            proof: None,
            observation: None,
        }
    }
}

impl DataUsagePublicationResult {
    pub(super) fn outcome(&self) -> DataUsagePersistOutcome {
        self.outcome
    }
    pub(super) fn restrict_outcome(&mut self, outcome: DataUsagePersistOutcome) {
        if outcome != self.outcome {
            self.proof = None;
            self.observation = None;
        }
        self.outcome = outcome;
    }
    pub(super) fn into_parts(self) -> (DataUsagePersistOutcome, Option<RootPublicationProof>) {
        (self.outcome, self.proof)
    }

    #[cfg(feature = "e2e-test-hooks")]
    pub(super) fn e2e_durable_observation(
        &self,
        epoch: u64,
        cycle: u64,
        expected: &crate::scanner_io::ScannerPublicationExpectation,
    ) -> Option<serde_json::Value> {
        let outcome = match self.outcome {
            DataUsagePersistOutcome::Saved => "saved",
            DataUsagePersistOutcome::AlreadyDurable => "already_durable",
            _ => return None,
        };
        let (root_version, root_digest) = self.proof.as_ref()?.verified_version_for(expected)?;
        let (candidate_digest, coverage_digest) = expected.e2e_digests();
        Some(serde_json::json!({
            "epoch": epoch, "cycle": cycle, "outcome": outcome,
            "candidate_sha256": candidate_digest, "coverage_sha256": coverage_digest,
            "root_version": root_version, "root_sha256": crate::e2e_barrier::hex(root_digest),
        }))
    }

    pub(super) fn observation_for_attempt(
        &self,
        scanner_epoch: u64,
        cycle: u64,
        progress_only: bool,
        final_outcome: DataUsagePersistOutcome,
    ) -> ScannerUsagePublicationStatus {
        let outcome = match final_outcome {
            DataUsagePersistOutcome::NoUpdate => ScannerUsagePublicationOutcome::NoUpdate,
            DataUsagePersistOutcome::Current => ScannerUsagePublicationOutcome::Current,
            DataUsagePersistOutcome::AlreadyDurable => ScannerUsagePublicationOutcome::AlreadyDurable,
            DataUsagePersistOutcome::PriorCycleDurable => ScannerUsagePublicationOutcome::PriorCycleDurable,
            DataUsagePersistOutcome::Saved => ScannerUsagePublicationOutcome::Saved,
            DataUsagePersistOutcome::Deferred(_) => ScannerUsagePublicationOutcome::Deferred,
            DataUsagePersistOutcome::Failed => ScannerUsagePublicationOutcome::Failed,
        };
        let mut status = ScannerUsagePublicationStatus {
            scanner_epoch: Some(scanner_epoch),
            cycle: Some(cycle),
            outcome,
            ..Default::default()
        };
        if final_outcome != self.outcome {
            return status;
        }
        match final_outcome {
            DataUsagePersistOutcome::NoUpdate if progress_only => status.class = ScannerUsagePublicationClass::ProgressOnly,
            DataUsagePersistOutcome::Saved
            | DataUsagePersistOutcome::AlreadyDurable
            | DataUsagePersistOutcome::PriorCycleDurable => {
                if let Some(observation) = &self.observation
                    && observation.scanner_epoch == status.scanner_epoch
                    && observation.cycle == status.cycle
                {
                    status = observation.clone();
                    status.outcome = outcome;
                }
            }
            _ => {}
        }
        status
    }
}

/// Project only the bytes confirmed by this writer/read, without granting ACK or repair proof.
fn usage_snapshot_observation(info: &DataUsageInfo) -> ScannerUsagePublicationStatus {
    let mut status = ScannerUsagePublicationStatus {
        scanner_epoch: info.scanner_epoch,
        cycle: info.scanner_cycle,
        snapshot_updated_at_unix_secs: info
            .last_update
            .and_then(|time| time.duration_since(std::time::SystemTime::UNIX_EPOCH).ok())
            .map(|elapsed| elapsed.as_secs()),
        ..Default::default()
    };
    if status.scanner_epoch.is_none() || status.cycle.is_none() || status.snapshot_updated_at_unix_secs.is_none() {
        return status;
    }
    let pending = info.usage_snapshot_pending_bucket_count;
    if info.usage_snapshot_observational {
        if info.usage_snapshot_converged == Some(false)
            && pending == 0
            && info.usage_snapshot_pending_buckets.is_empty()
            && ((info.is_complete_bucket_usage_snapshot() && !info.usage_snapshot_partial)
                || (info.is_valid_partial_snapshot() && !info.usage_snapshot_complete))
        {
            status.class = ScannerUsagePublicationClass::Observational;
        }
        return status;
    }
    if !info.is_complete_bucket_usage_snapshot()
        || info.usage_snapshot_partial
        || u64::try_from(info.usage_snapshot_pending_buckets.len()).ok() != Some(pending)
        || !info.usage_snapshot_pending_buckets.windows(2).all(|pair| pair[0] < pair[1])
    {
        return status;
    }
    status.class = match (info.usage_snapshot_converged, pending) {
        (Some(true), 0) => ScannerUsagePublicationClass::CompleteConverged,
        (Some(false), 1..) => ScannerUsagePublicationClass::CompletePending,
        _ => return status,
    };
    status.pending_bucket_count = Some(pending);
    status
}

#[cfg(test)]
mod publication_observation_tests {
    use super::*;

    fn converged_snapshot() -> DataUsageInfo {
        DataUsageInfo {
            last_update: Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(20)),
            scanner_epoch: Some(8),
            scanner_cycle: Some(12),
            usage_snapshot_complete: true,
            usage_snapshot_converged: Some(true),
            ..Default::default()
        }
    }

    #[test]
    fn publication_observation_distinguishes_snapshot_classes_and_unknown_counts() {
        let mut info = converged_snapshot();
        let converged = usage_snapshot_observation(&info);
        assert_eq!(converged.class, ScannerUsagePublicationClass::CompleteConverged);
        assert_eq!(converged.pending_bucket_count, Some(0));
        assert_eq!(converged.snapshot_updated_at_unix_secs, Some(20));

        info.usage_snapshot_converged = Some(false);
        info.usage_snapshot_pending_buckets = vec!["new-a".into(), "new-b".into()];
        info.usage_snapshot_pending_bucket_count = 2;
        let pending = usage_snapshot_observation(&info);
        assert_eq!(pending.class, ScannerUsagePublicationClass::CompletePending);
        assert_eq!(pending.pending_bucket_count, Some(2));

        info.usage_snapshot_pending_bucket_count = 0;
        assert_eq!(
            usage_snapshot_observation(&info).pending_bucket_count,
            None,
            "inconsistent count is unknown"
        );
        info.usage_snapshot_pending_buckets.clear();
        assert_eq!(usage_snapshot_observation(&info).class, ScannerUsagePublicationClass::Unknown);
        info.usage_snapshot_observational = true;
        let observed = usage_snapshot_observation(&info);
        assert_eq!(observed.class, ScannerUsagePublicationClass::Observational);
        assert_eq!(observed.pending_bucket_count, None, "observational zero is not a backlog measurement");
        info.usage_snapshot_partial = true;
        assert_eq!(usage_snapshot_observation(&info).class, ScannerUsagePublicationClass::Unknown);
        info.usage_snapshot_partial = false;

        info.usage_snapshot_observational = false;
        info.usage_snapshot_converged = None;
        assert_eq!(usage_snapshot_observation(&info).class, ScannerUsagePublicationClass::Unknown);
        assert_eq!(usage_snapshot_observation(&info).pending_bucket_count, None);
        assert_eq!(usage_snapshot_observation(&DataUsageInfo::default()).pending_bucket_count, None);

        let progress = DataUsagePublicationResult::from(DataUsagePersistOutcome::NoUpdate).observation_for_attempt(
            8,
            12,
            true,
            DataUsagePersistOutcome::NoUpdate,
        );
        assert_eq!(progress.class, ScannerUsagePublicationClass::ProgressOnly);
        assert_eq!(progress.pending_bucket_count, None);
    }

    #[test]
    fn publication_observation_is_revoked_by_final_fence_or_failed_attempt() {
        let mut publication = DataUsagePublicationResult {
            outcome: DataUsagePersistOutcome::Saved,
            proof: None,
            observation: Some(usage_snapshot_observation(&converged_snapshot())),
        };
        let saved = publication.observation_for_attempt(8, 12, false, DataUsagePersistOutcome::Saved);
        assert_eq!(saved.pending_bucket_count, Some(0));
        let another_cycle = publication.observation_for_attempt(8, 13, false, DataUsagePersistOutcome::Saved);
        assert_eq!(another_cycle.class, ScannerUsagePublicationClass::Unknown);
        assert_eq!(another_cycle.pending_bucket_count, None);
        let deferred = DataUsagePersistOutcome::Deferred(ScannerCycleDeferReason::PublicationLeaseReleaseFailed);
        let fenced = publication.observation_for_attempt(8, 12, false, deferred);
        assert_eq!(fenced.outcome, ScannerUsagePublicationOutcome::Deferred);
        assert_eq!(fenced.class, ScannerUsagePublicationClass::Unknown);
        assert_eq!(fenced.pending_bucket_count, None);
        publication.restrict_outcome(deferred);
        assert!(publication.observation.is_none());
        assert_eq!(
            publication
                .observation_for_attempt(8, 12, false, deferred)
                .pending_bucket_count,
            None
        );

        for outcome in [
            DataUsagePersistOutcome::Failed,
            DataUsagePersistOutcome::Current,
            DataUsagePersistOutcome::NoUpdate,
        ] {
            let status = DataUsagePublicationResult::from(outcome).observation_for_attempt(8, 12, false, outcome);
            assert_eq!(status.class, ScannerUsagePublicationClass::Unknown);
            assert_eq!(status.pending_bucket_count, None);
        }
    }
}

fn root_write_publication_proof(
    outcome: DataUsagePersistOutcome,
    write_confirmed: bool,
    written_etag: Option<&str>,
    expected: &crate::scanner_io::ScannerPublicationExpectation,
    data_digest: &[u8; 32],
) -> Option<RootPublicationProof> {
    if outcome != DataUsagePersistOutcome::Saved || !write_confirmed || !expected.matches_encoded_candidate(data_digest) {
        return None;
    }

    let etag = written_etag.filter(|etag| !etag.is_empty())?;
    Some(RootPublicationProof {
        candidate: expected.clone(),
        root_version: (etag.to_string(), *data_digest),
    })
}

fn root_ack_write_is_confirmed<T, E>(
    result: &std::result::Result<T, E>,
    state: Option<ScannerPublicationCommitState>,
    written_etag: Option<&str>,
) -> bool {
    result.is_ok() && state == Some(ScannerPublicationCommitState::Committed) && written_etag.is_some_and(|etag| !etag.is_empty())
}

#[cfg(test)]
mod root_publication_confirmation_tests {
    use super::*;

    #[test]
    fn root_publication_confirmation_requires_committed_state_and_write_revision() {
        let saved = Ok::<(), ()>(());
        for state in [
            None,
            Some(ScannerPublicationCommitState::Admitted),
            Some(ScannerPublicationCommitState::InFlight),
            Some(ScannerPublicationCommitState::AbortedBeforeCommit),
            Some(ScannerPublicationCommitState::Indeterminate),
        ] {
            assert!(!root_ack_write_is_confirmed(&saved, state, Some("revision")));
        }
        for etag in [None, Some("")] {
            assert!(!root_ack_write_is_confirmed(&saved, Some(ScannerPublicationCommitState::Committed), etag));
        }
        assert!(root_ack_write_is_confirmed(
            &saved,
            Some(ScannerPublicationCommitState::Committed),
            Some("revision")
        ));
    }

    #[test]
    fn root_publication_confirmation_does_not_carry_state_across_cas_attempts() {
        let attempts = [
            (Err(()), Some(ScannerPublicationCommitState::Committed), Some("first")),
            (Ok(()), Some(ScannerPublicationCommitState::AbortedBeforeCommit), Some("second")),
            (Ok(()), None, Some("legacy")),
            (Ok(()), Some(ScannerPublicationCommitState::Committed), Some("confirmed")),
        ];
        let confirmations = attempts
            .iter()
            .map(|(result, state, etag)| root_ack_write_is_confirmed(result, *state, *etag))
            .collect::<Vec<_>>();
        assert_eq!(confirmations, [false, false, false, true]);
    }
}

#[cfg(test)]
mod root_write_publication_proof_tests {
    use super::*;

    fn expectation_for(info: &DataUsageInfo) -> crate::scanner_io::ScannerPublicationExpectation {
        let data = serde_json::to_vec(info).expect("usage info should encode");
        crate::scanner_io::ScannerPublicationExpectation::for_tests(
            Sha256::digest(data).into(),
            crate::data_usage_define::DataUsageScanPlanDigest([7; 32]),
        )
    }

    #[test]
    fn root_write_publication_proof_requires_committed_root_write_identity() {
        let info = DataUsageInfo {
            scanner_cycle: Some(11),
            scanner_epoch: Some(7),
            usage_snapshot_complete: true,
            ..Default::default()
        };
        let expected = expectation_for(&info);
        let data = serde_json::to_vec(&info).expect("usage info should encode");
        let digest: [u8; 32] = Sha256::digest(&data).into();
        let stale_digest = Sha256::digest(b"stale").into();

        assert!(
            root_write_publication_proof(DataUsagePersistOutcome::AlreadyDurable, true, Some("etag"), &expected, &digest)
                .is_none()
        );
        assert!(root_write_publication_proof(DataUsagePersistOutcome::Saved, false, Some("etag"), &expected, &digest).is_none());
        assert!(root_write_publication_proof(DataUsagePersistOutcome::Saved, true, None, &expected, &digest).is_none());
        assert!(root_write_publication_proof(DataUsagePersistOutcome::Saved, true, Some(""), &expected, &digest).is_none());
        assert!(
            root_write_publication_proof(DataUsagePersistOutcome::Saved, true, Some("etag"), &expected, &stale_digest).is_none()
        );

        let proof = root_write_publication_proof(DataUsagePersistOutcome::Saved, true, Some("etag"), &expected, &digest)
            .expect("a confirmed root CAS write should prove its own candidate");
        let (etag, raw_digest) = proof
            .verified_version_for(&expected)
            .expect("proof should bind the expected candidate");
        assert_eq!(etag, "etag");
        assert_eq!(*raw_digest, digest);
    }
}

async fn read_root_publication_proof<S: ScannerObjectIO + ScannerConfigObjectDelete>(
    store: Arc<S>,
    ctx: &CancellationToken,
    deadline: tokio::time::Instant,
    epoch: u64,
    expected: &crate::scanner_io::ScannerPublicationExpectation,
    candidate: &DataUsageInfo,
    written_etag: Option<&str>,
) -> Option<RootPublicationProof> {
    let read = async {
        let _admission = scanner_publication_admission_for_epoch(store.clone(), epoch).await?;
        let (bytes, revision) = read_config_with_revision(store, DATA_USAGE_OBJ_NAME_PATH.as_str())
            .await
            .ok()?;
        let bytes = bytes?;
        let DataUsageCacheRevision::Etag(etag) = revision else {
            return None;
        };
        if etag.is_empty() || written_etag.is_some_and(|written| written != etag) {
            return None;
        }
        let persisted: DataUsageInfo = serde_json::from_slice(&bytes).ok()?;
        if &persisted != candidate {
            return None;
        }
        let root_digest = Sha256::digest(&bytes).into();
        if ctx.is_cancelled() || tokio::time::Instant::now() >= deadline {
            return None;
        }
        Some(RootPublicationProof {
            candidate: expected.clone(),
            root_version: (etag, root_digest),
        })
    };
    tokio::select! {
        biased;
        _ = ctx.cancelled() => None,
        result = tokio::time::timeout_at(deadline, read) => result.ok().flatten(),
    }
}

fn remote_lease_expired(deadline: Option<std::time::Instant>) -> bool {
    deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline)
}

pub(super) fn scanner_publication_scope_deadline(
    persist_timeout: Duration,
    remote_lease_deadline: Option<std::time::Instant>,
) -> tokio::time::Instant {
    let configured_deadline = tokio::time::Instant::now() + persist_timeout;
    remote_lease_deadline
        .map(tokio::time::Instant::from_std)
        .map_or(configured_deadline, |lease_deadline| configured_deadline.min(lease_deadline))
}

#[derive(Clone, Debug)]
pub(super) struct DataUsagePersistBaseline {
    pub(super) data: Option<Bytes>,
    pub(super) revision: DataUsageCacheRevision,
}

async fn read_usage_persist_candidate(
    storeapi: Arc<impl ScannerObjectIO>,
    path: &str,
) -> Result<(Option<Vec<u8>>, DataUsageCacheRevision), EcstoreError> {
    let primary = read_config_with_revision(storeapi.clone(), path).await;
    if path != LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str() {
        return primary;
    }
    let Err(primary_error) = &primary else {
        return primary;
    };
    if !usage_floor_primary_read_error_allows_backup(primary_error) {
        return primary;
    }
    let backup_path = format!("{path}.bkp");
    let backup = read_config_with_revision(storeapi, &backup_path).await?;
    if backup.0.as_deref().is_some_and(|data| {
        serde_json::from_slice::<DataUsageInfo>(data).is_ok_and(|usage| data_usage_info_has_persisted_baseline_identity(&usage))
    }) {
        return Ok(backup);
    }
    primary
}

/// Read the bytes used as the baseline for a usage publication while keeping
/// the v2 primary revision as the CAS fence. During an interrupted upgrade the
/// primary can be valid JSON without a baseline identity; in that case a
/// same-or-newer durable companion may still be used, but an older legacy
/// snapshot must not cross the primary's epoch fence.
pub(super) async fn read_data_usage_persist_baseline(
    storeapi: Arc<impl ScannerObjectIO>,
) -> Result<DataUsagePersistBaseline, EcstoreError> {
    let (primary, revision) = read_config_with_revision(storeapi.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str()).await?;
    let Some(primary) = primary else {
        for path in [
            format!("{}.bkp", DATA_USAGE_OBJ_NAME_PATH.as_str()),
            LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str().to_string(),
            format!("{}.bkp", LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str()),
        ] {
            let (candidate, _) = read_usage_persist_candidate(storeapi.clone(), &path).await?;
            let Some(candidate) = candidate else {
                continue;
            };
            let Ok(usage) = serde_json::from_slice::<DataUsageInfo>(&candidate) else {
                continue;
            };
            if data_usage_info_has_persisted_baseline_identity(&usage) {
                return Ok(DataUsagePersistBaseline {
                    data: Some(Bytes::from(candidate)),
                    revision,
                });
            }
        }
        return Ok(DataUsagePersistBaseline { data: None, revision });
    };

    let Ok(primary_info) = serde_json::from_slice::<DataUsageInfo>(&primary) else {
        // Preserve the original bytes and revision. A completed scan may
        // replace the invalid primary under this CAS fence; an observation
        // will still reject it below because it has no verifiable identity.
        return Ok(DataUsagePersistBaseline {
            data: Some(Bytes::from(primary)),
            revision,
        });
    };
    if data_usage_info_has_persisted_baseline_identity(&primary_info) || data_usage_info_is_bootstrap_pending(&primary_info) {
        return Ok(DataUsagePersistBaseline {
            data: Some(Bytes::from(primary)),
            revision,
        });
    }

    let invalid_primary_epoch = primary_info.scanner_epoch;
    for path in [
        format!("{}.bkp", DATA_USAGE_OBJ_NAME_PATH.as_str()),
        LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str().to_string(),
        format!("{}.bkp", LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str()),
    ] {
        let (candidate, _) = read_usage_persist_candidate(storeapi.clone(), &path).await?;
        let Some(candidate) = candidate else {
            continue;
        };
        let Ok(usage) = serde_json::from_slice::<DataUsageInfo>(&candidate) else {
            continue;
        };
        let candidate_epoch = usage.scanner_epoch.unwrap_or_default();
        if data_usage_info_has_persisted_baseline_identity(&usage)
            && invalid_primary_epoch.is_none_or(|epoch| candidate_epoch >= epoch)
        {
            return Ok(DataUsagePersistBaseline {
                data: Some(Bytes::from(candidate)),
                revision,
            });
        }
    }

    Ok(DataUsagePersistBaseline {
        data: Some(Bytes::from(primary)),
        revision,
    })
}

/// Short-lived publication inputs captured for one usage persistence attempt.
/// Keeping the movement epoch, lease deadline, and target fence together makes
/// it explicit that they are one proof rather than independent options.
#[derive(Clone, Debug, Default)]
pub(super) struct ScannerPublicationFence {
    pub(super) expected_publication_epoch: Option<u64>,
    pub(super) remote_lease_deadline: Option<std::time::Instant>,
    pub(super) scanner_publication_lease_fence: Option<String>,
    pub(super) remote_lease_tokens: Vec<Uuid>,
    pub(super) lease_release_safe: Arc<AtomicBool>,
    pub(super) ack_expectation: Option<crate::scanner_io::ScannerPublicationExpectation>,
}

impl ScannerPublicationFence {
    pub(super) fn new(
        expected_publication_epoch: Option<u64>,
        remote_lease_deadline: Option<std::time::Instant>,
        scanner_publication_lease_fence: Option<String>,
    ) -> Self {
        Self {
            expected_publication_epoch,
            remote_lease_deadline,
            scanner_publication_lease_fence,
            remote_lease_tokens: Vec::new(),
            lease_release_safe: Arc::new(AtomicBool::new(true)),
            ack_expectation: None,
        }
    }

    pub(super) fn with_remote_lease_tokens(mut self, remote_lease_tokens: Vec<Uuid>) -> Self {
        self.remote_lease_tokens = remote_lease_tokens;
        self
    }

    pub(super) fn with_lease_release_flag(mut self, lease_release_safe: Arc<AtomicBool>) -> Self {
        self.lease_release_safe = lease_release_safe;
        self
    }

    pub(super) fn with_ack_expectation(mut self, expected: Option<crate::scanner_io::ScannerPublicationExpectation>) -> Self {
        self.ack_expectation = expected;
        self
    }
}

#[derive(Debug)]
pub(super) enum DataUsagePersistTaskResult<T = DataUsagePersistOutcome> {
    Completed(T),
    Cancelled,
    TimedOut,
    JoinFailed(tokio::task::JoinError),
}

pub(super) async fn wait_for_data_usage_persist_task<T>(
    ctx: &CancellationToken,
    task: &mut AbortOnDropHandle<T>,
    timeout: Duration,
) -> DataUsagePersistTaskResult<T> {
    tokio::select! {
        biased;
        result = &mut *task => match result {
            Ok(outcome) => DataUsagePersistTaskResult::Completed(outcome),
            Err(err) => DataUsagePersistTaskResult::JoinFailed(err),
        },
        _ = ctx.cancelled() => {
            task.abort();
            let _ = (&mut *task).await;
            DataUsagePersistTaskResult::Cancelled
        },
        _ = tokio::time::sleep(timeout) => {
            task.abort();
            let _ = (&mut *task).await;
            DataUsagePersistTaskResult::TimedOut
        }
    }
}

#[instrument(skip(ctx, storeapi))]
pub async fn store_data_usage_in_backend(
    ctx: CancellationToken,
    storeapi: Arc<impl ScannerObjectIO + ScannerConfigObjectDelete>,
    receiver: mpsc::Receiver<DataUsageInfo>,
) {
    let _ = store_data_usage_in_backend_with_outcome(ctx, storeapi, receiver).await;
}

pub(super) async fn store_data_usage_in_backend_with_outcome(
    ctx: CancellationToken,
    storeapi: Arc<impl ScannerObjectIO + ScannerConfigObjectDelete>,
    receiver: mpsc::Receiver<DataUsageInfo>,
) -> DataUsagePersistOutcome {
    store_data_usage_in_backend_with_outcome_for_epoch(ctx, storeapi, receiver, None).await
}

pub(super) async fn store_data_usage_in_backend_with_outcome_for_epoch(
    ctx: CancellationToken,
    storeapi: Arc<impl ScannerObjectIO + ScannerConfigObjectDelete>,
    receiver: mpsc::Receiver<DataUsageInfo>,
    leader_epoch: Option<u64>,
) -> DataUsagePersistOutcome {
    store_data_usage_in_backend_with_outcome_for_epoch_and_baseline(ctx, storeapi, receiver, leader_epoch, None).await
}

pub(super) async fn store_data_usage_in_backend_with_outcome_for_epoch_and_baseline(
    ctx: CancellationToken,
    storeapi: Arc<impl ScannerObjectIO + ScannerConfigObjectDelete>,
    receiver: mpsc::Receiver<DataUsageInfo>,
    leader_epoch: Option<u64>,
    initial_baseline: Option<DataUsagePersistBaseline>,
) -> DataUsagePersistOutcome {
    store_data_usage_in_backend_with_outcome_for_epoch_and_baseline_and_route_probe(
        ctx,
        storeapi,
        receiver,
        leader_epoch,
        initial_baseline,
        || async { None },
    )
    .await
}

pub(super) async fn store_data_usage_in_backend_with_outcome_for_epoch_and_baseline_and_route_probe<F, Fut>(
    ctx: CancellationToken,
    storeapi: Arc<impl ScannerObjectIO + ScannerConfigObjectDelete>,
    receiver: mpsc::Receiver<DataUsageInfo>,
    leader_epoch: Option<u64>,
    initial_baseline: Option<DataUsagePersistBaseline>,
    route_probe: F,
) -> DataUsagePersistOutcome
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = Option<ScannerCycleDeferReason>> + Send,
{
    store_data_usage_in_backend_with_outcome_for_epoch_and_baseline_and_route_probe_for_publication_epoch(
        ctx,
        storeapi,
        receiver,
        leader_epoch,
        initial_baseline,
        ScannerPublicationFence::default(),
        route_probe,
    )
    .await
}

pub(super) async fn store_data_usage_in_backend_with_outcome_for_epoch_and_baseline_and_route_probe_for_publication_epoch<
    F,
    Fut,
>(
    ctx: CancellationToken,
    storeapi: Arc<impl ScannerObjectIO + ScannerConfigObjectDelete>,
    receiver: mpsc::Receiver<DataUsageInfo>,
    leader_epoch: Option<u64>,
    initial_baseline: Option<DataUsagePersistBaseline>,
    publication_fence: ScannerPublicationFence,
    route_probe: F,
) -> DataUsagePersistOutcome
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = Option<ScannerCycleDeferReason>> + Send,
{
    store_data_usage_in_backend_with_outcome_for_epoch_and_baseline_and_route_probe_for_publication_epoch_and_lease_fence(
        ctx,
        storeapi,
        receiver,
        leader_epoch,
        initial_baseline,
        publication_fence,
        route_probe,
    )
    .await
    .outcome()
}

pub(super) async fn store_data_usage_in_backend_with_outcome_for_epoch_and_baseline_and_route_probe_for_publication_epoch_and_lease_fence<
    F,
    Fut,
>(
    ctx: CancellationToken,
    storeapi: Arc<impl ScannerObjectIO + ScannerConfigObjectDelete>,
    mut receiver: mpsc::Receiver<DataUsageInfo>,
    leader_epoch: Option<u64>,
    initial_baseline: Option<DataUsagePersistBaseline>,
    publication_fence: ScannerPublicationFence,
    route_probe: F,
) -> DataUsagePublicationResult
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = Option<ScannerCycleDeferReason>> + Send,
{
    let ScannerPublicationFence {
        expected_publication_epoch,
        remote_lease_deadline,
        scanner_publication_lease_fence,
        remote_lease_tokens,
        lease_release_safe,
        ack_expectation,
    } = publication_fence;
    let ack_deadline = scanner_publication_scope_deadline(data_usage_persist_timeout(), remote_lease_deadline);
    let mut outcome = DataUsagePersistOutcome::NoUpdate;
    let mut proof = None;
    let mut observation = None;
    let mut next_baseline = initial_baseline;

    'updates: while let Some(mut data_usage_info) = receiver.recv().await {
        proof = None;
        observation = None;
        let _activity_guard = ScannerActivityGuard::new();
        if ctx.is_cancelled() {
            break;
        }
        if let Some(leader_epoch) = leader_epoch {
            data_usage_info.scanner_epoch = Some(leader_epoch);
        }
        if remote_lease_expired(remote_lease_deadline) {
            outcome = DataUsagePersistOutcome::Deferred(ScannerCycleDeferReason::PublicationLeaseDeadlineExceeded);
            break 'updates;
        }
        if let Some(expected_epoch) = expected_publication_epoch
            && scanner_publication_admission_for_epoch(storeapi.clone(), expected_epoch)
                .await
                .is_none()
        {
            outcome = DataUsagePersistOutcome::Deferred(ScannerCycleDeferReason::DataMovement);
            break 'updates;
        }
        // Routing keys on the explicit producer mark, not on convergence: a
        // `Complete` cycle with pending buckets is authoritative but not
        // converged, while a vetoed cycle is observational.
        let observational = data_usage_info.usage_snapshot_observational;
        let target_path = if observational {
            DATA_USAGE_OBSERVED_OBJ_NAME_PATH.as_str()
        } else {
            DATA_USAGE_OBJ_NAME_PATH.as_str()
        };
        if let Some(reason) = route_probe().await {
            debug!(
                target: "rustfs::scanner",
                event = EVENT_SCANNER_PERSIST_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                state = "publication_blocked_before_reconcile",
                reason = reason.as_str(),
                path = %target_path,
                "Scanner data usage publication deferred by the publication fence"
            );
            global_metrics().record_scanner_usage_deferred(reason.as_str());
            outcome = DataUsagePersistOutcome::Deferred(reason);
            break;
        }

        let mut publication_epoch = expected_publication_epoch;
        if observational && data_usage_info.usage_snapshot_authoritative_baseline.is_none() {
            let read_epoch = match expected_publication_epoch {
                Some(expected_epoch) => {
                    if scanner_publication_admission_for_epoch(storeapi.clone(), expected_epoch)
                        .await
                        .is_none()
                    {
                        outcome = DataUsagePersistOutcome::Deferred(ScannerCycleDeferReason::DataMovement);
                        break 'updates;
                    }
                    expected_epoch
                }
                None => {
                    let Some(read_epoch) = scanner_publication_epoch(storeapi.clone()).await else {
                        outcome = DataUsagePersistOutcome::Deferred(ScannerCycleDeferReason::DataMovement);
                        break 'updates;
                    };
                    read_epoch
                }
            };
            publication_epoch = Some(read_epoch);
            let authoritative_data = match next_baseline.as_ref() {
                Some(baseline) => baseline.data.clone(),
                None => match read_data_usage_persist_baseline(storeapi.clone()).await {
                    Ok(baseline) => baseline.data,
                    Err(err) => {
                        error!(
                            target: "rustfs::scanner",
                            event = EVENT_SCANNER_PERSIST_STATE,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_RUNTIME,
                            path = %DATA_USAGE_OBJ_NAME_PATH.as_str(),
                            state = "observed_baseline_load_failed",
                            error = %err,
                            "Scanner could not identify the authoritative baseline for an observation"
                        );
                        outcome = DataUsagePersistOutcome::Failed;
                        continue;
                    }
                },
            };
            let Some(authoritative_data) = authoritative_data else {
                warn!(
                    target: "rustfs::scanner",
                    event = EVENT_SCANNER_PERSIST_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    path = %DATA_USAGE_OBJ_NAME_PATH.as_str(),
                    state = "observed_baseline_missing",
                    "Scanner deferred observational publication until an authoritative usage baseline exists"
                );
                outcome = DataUsagePersistOutcome::Deferred(ScannerCycleDeferReason::DataMovement);
                break 'updates;
            };
            let authoritative = match serde_json::from_slice::<DataUsageInfo>(&authoritative_data) {
                // The bootstrap placeholder is a valid baseline identity: on a
                // site that has never converged (every cycle superseded by a
                // sustained write stream, #6852) it is the only authoritative
                // object that will ever exist, and refusing it here means the
                // observed snapshot — the only usage data such a site can
                // produce — is never published at all.
                Ok(info)
                    if data_usage_info_has_persisted_baseline_identity(&info) || data_usage_info_is_bootstrap_pending(&info) =>
                {
                    info
                }
                Ok(_) => {
                    error!(
                        target: "rustfs::scanner",
                        event = EVENT_SCANNER_PERSIST_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_RUNTIME,
                        path = %DATA_USAGE_OBJ_NAME_PATH.as_str(),
                        state = "observed_baseline_identity_missing",
                        "Scanner refused to publish an observation without authoritative baseline identity"
                    );
                    outcome = DataUsagePersistOutcome::Failed;
                    continue;
                }
                Err(err) => {
                    error!(
                        target: "rustfs::scanner",
                        event = EVENT_SCANNER_PERSIST_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_RUNTIME,
                        path = %DATA_USAGE_OBJ_NAME_PATH.as_str(),
                        state = "observed_baseline_decode_failed",
                        error = %err,
                        "Scanner refused to publish an observation from an invalid authoritative baseline"
                    );
                    outcome = DataUsagePersistOutcome::Failed;
                    continue;
                }
            };
            data_usage_info.usage_snapshot_authoritative_baseline = Some(authoritative.snapshot_identity());
        }

        if !data_usage_info.is_complete_bucket_usage_snapshot() && !data_usage_info.usage_snapshot_partial {
            error!(
                target: "rustfs::scanner",
                event = EVENT_SCANNER_PERSIST_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                path = %target_path,
                state = "reject_incomplete_snapshot",
                "Scanner refused to persist an incomplete data usage snapshot"
            );
            global_metrics().record_scanner_usage_publication("failed", "incomplete_snapshot");
            global_metrics().record_scanner_usage_save_result(ScannerUsageSaveResult::Failed);
            outcome = DataUsagePersistOutcome::Failed;
            continue;
        }

        let data = match serde_json::to_vec(&data_usage_info) {
            Ok(data) => data,
            Err(e) => {
                error!(
                    target: "rustfs::scanner",
                    event = EVENT_SCANNER_PERSIST_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    path = %target_path,
                    state = "encode_failed",
                    error = %e,
                    "Scanner data usage encode failed"
                );
                global_metrics().record_scanner_usage_publication("failed", "encode_failed");
                global_metrics().record_scanner_usage_save_result(ScannerUsageSaveResult::EncodeFailed);
                outcome = DataUsagePersistOutcome::Failed;
                continue;
            }
        };
        let data_digest: [u8; 32] = Sha256::digest(&data).into();
        let sha256hex = (!data.is_empty()).then(|| hex_simd::encode_to_string(data_digest, hex_simd::AsciiCase::Lower));
        let data = Bytes::from(data);
        let backup_due = !observational && data_usage_backup_due(&data_usage_info);
        let mut cas_retry = 0usize;
        let mut ack_epoch = None;
        let mut write_confirmed = false;
        let mut written_etag = None;
        let save_outcome = loop {
            if ctx.is_cancelled() {
                break 'updates;
            }

            let publication_epoch_for_save = match expected_publication_epoch {
                Some(expected_epoch) => {
                    if scanner_publication_admission_for_epoch(storeapi.clone(), expected_epoch)
                        .await
                        .is_none()
                    {
                        break DataUsagePersistOutcome::Deferred(ScannerCycleDeferReason::DataMovement);
                    }
                    expected_epoch
                }
                None => match publication_epoch.take() {
                    Some(epoch) => epoch,
                    None => {
                        let Some(epoch) = scanner_publication_epoch(storeapi.clone()).await else {
                            break DataUsagePersistOutcome::Deferred(ScannerCycleDeferReason::DataMovement);
                        };
                        epoch
                    }
                },
            };
            let baseline = if !observational && cas_retry == 0 {
                next_baseline.take()
            } else {
                None
            };
            // An initial baseline can be a companion or an old primary; it is not a current target observation.
            let read_target = baseline.is_none();
            ack_epoch = Some(publication_epoch_for_save);
            let (existing_data, revision) = match baseline {
                Some(baseline) => (baseline.data, baseline.revision),
                None => match read_config_with_revision(storeapi.clone(), target_path).await {
                    Ok((data, revision)) => (data.map(Bytes::from), revision),
                    Err(e) => {
                        error!(
                            target: "rustfs::scanner",
                            event = EVENT_SCANNER_PERSIST_STATE,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_RUNTIME,
                            path = %target_path,
                            state = "revision_load_failed",
                            error = %e,
                            "Scanner data usage revision load failed"
                        );
                        break DataUsagePersistOutcome::Failed;
                    }
                },
            };
            let existing = existing_data
                .as_deref()
                .and_then(|buf| serde_json::from_slice::<DataUsageInfo>(buf).ok());
            let read_target = read_target && matches!(&revision, DataUsageCacheRevision::Etag(etag) if !etag.is_empty());
            if cas_retry > 0 && data_usage_reintroduces_missing_bucket(&data_usage_info, existing.as_ref()) {
                debug!(
                    target: "rustfs::scanner",
                    event = EVENT_SCANNER_PERSIST_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    path = %target_path,
                    incoming_scanner_epoch = ?data_usage_info.scanner_epoch,
                    incoming_scanner_cycle = ?data_usage_info.scanner_cycle,
                    state = "skip_deleted_bucket_reintroduction",
                    "Scanner usage update skipped after a concurrent bucket removal"
                );
                break DataUsagePersistOutcome::Current;
            }
            if let Some(existing) = existing.as_ref() {
                if existing == &data_usage_info {
                    if read_target {
                        observation = Some(usage_snapshot_observation(existing));
                    }
                    break DataUsagePersistOutcome::AlreadyDurable;
                }
                if existing.scanner_epoch.is_some()
                    && existing.scanner_epoch == data_usage_info.scanner_epoch
                    && existing.scanner_cycle.is_some()
                    && existing.scanner_cycle == data_usage_info.scanner_cycle
                {
                    // The cycle identity matches, but its pending set and payload can differ from this retry.
                    if read_target {
                        observation = Some(usage_snapshot_observation(existing));
                    }
                    break DataUsagePersistOutcome::PriorCycleDurable;
                }
                if let Some(reason) = stale_data_usage_update_reason(&data_usage_info, existing, std::time::SystemTime::now()) {
                    debug!(
                        target: "rustfs::scanner",
                        event = EVENT_SCANNER_PERSIST_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_RUNTIME,
                        path = %target_path,
                        incoming_scanner_epoch = ?data_usage_info.scanner_epoch,
                        existing_scanner_epoch = ?existing.scanner_epoch,
                        incoming_scanner_cycle = ?data_usage_info.scanner_cycle,
                        existing_scanner_cycle = ?existing.scanner_cycle,
                        incoming_last_update = ?data_usage_info.last_update,
                        existing_last_update = ?existing.last_update,
                        reason = reason,
                        state = "skip_stale_update",
                        "Scanner stale data usage update skipped"
                    );
                    break DataUsagePersistOutcome::Current;
                }
            }
            if ctx.is_cancelled() {
                break 'updates;
            }
            if let Some(reason) = route_probe().await {
                debug!(
                    target: "rustfs::scanner",
                    event = EVENT_SCANNER_PERSIST_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    state = "publication_blocked_before_save",
                    reason = reason.as_str(),
                    path = %target_path,
                    "Scanner data usage publication deferred by the final publication fence"
                );
                break DataUsagePersistOutcome::Deferred(reason);
            }
            if remote_lease_expired(remote_lease_deadline) {
                break DataUsagePersistOutcome::Deferred(ScannerCycleDeferReason::PublicationLeaseDeadlineExceeded);
            }

            #[cfg(feature = "e2e-test-hooks")]
            if !observational
                && target_path == DATA_USAGE_OBJ_NAME_PATH.as_str()
                && let (Some(expected), Some(epoch), Some(cycle)) =
                    (ack_expectation.as_ref(), data_usage_info.scanner_epoch, data_usage_info.scanner_cycle)
                && expected.matches_encoded_candidate(&data_digest)
            {
                let (candidate_digest, coverage_digest) = expected.e2e_digests();
                let _ = crate::e2e_barrier::checkpoint(
                    "root_before_publish",
                    serde_json::json!({
                        "epoch": epoch, "cycle": cycle,
                        "candidate_sha256": candidate_digest, "coverage_sha256": coverage_digest,
                    }),
                )
                .await;
            }
            let done_save = Metrics::time(Metric::SaveUsage);
            let (save_result, commit_state) = {
                let publication_scope = storeapi
                    .scanner_data_usage_publication_commit_scope_with_release_flag(
                        publication_epoch_for_save,
                        scanner_publication_scope_deadline(data_usage_persist_timeout(), remote_lease_deadline),
                        remote_lease_tokens.clone(),
                        Arc::clone(&lease_release_safe),
                    )
                    .await;
                let legacy_publication_admission = if publication_scope.is_none() {
                    let Some(admission) =
                        scanner_publication_admission_for_epoch(storeapi.clone(), publication_epoch_for_save).await
                    else {
                        done_save();
                        break DataUsagePersistOutcome::Deferred(ScannerCycleDeferReason::DataMovement);
                    };
                    Some(admission)
                } else {
                    None
                };
                if remote_lease_expired(remote_lease_deadline) {
                    done_save();
                    break DataUsagePersistOutcome::Deferred(ScannerCycleDeferReason::PublicationLeaseDeadlineExceeded);
                }
                let save_result = crate::save_config_shared_with_preconditions_and_lease_fence_and_scope(
                    storeapi.clone(),
                    target_path,
                    data.clone(),
                    sha256hex.clone(),
                    revision.preconditions(),
                    scanner_publication_lease_fence.as_deref(),
                    publication_scope.clone(),
                )
                .await;
                drop(legacy_publication_admission);
                if let Some(scope) = publication_scope {
                    let state = scope.wait_for_completion().await;
                    let result = match state {
                        ScannerPublicationCommitState::Committed => save_result,
                        ScannerPublicationCommitState::AbortedBeforeCommit => save_result,
                        ScannerPublicationCommitState::Indeterminate
                        | ScannerPublicationCommitState::Admitted
                        | ScannerPublicationCommitState::InFlight => Err(EcstoreError::other(
                            "scanner publication commit scope did not reach a safe terminal state",
                        )),
                    };
                    (result, Some(state))
                } else {
                    (save_result, None)
                }
            };
            done_save();

            let attempt_confirmed = root_ack_write_is_confirmed(
                &save_result,
                commit_state,
                save_result.as_ref().ok().and_then(|info| info.etag.as_deref()),
            );

            match save_result {
                Ok(object_info) => {
                    write_confirmed = attempt_confirmed;
                    if write_confirmed {
                        observation = Some(usage_snapshot_observation(&data_usage_info));
                    }
                    written_etag = object_info.etag.as_ref().filter(|etag| !etag.is_empty()).cloned();
                    if !observational {
                        next_baseline = object_info
                            .etag
                            .filter(|etag| !etag.is_empty())
                            .map(|etag| DataUsagePersistBaseline {
                                data: Some(data.clone()),
                                revision: DataUsageCacheRevision::Etag(etag),
                            });
                    }
                    break DataUsagePersistOutcome::Saved;
                }
                Err(EcstoreError::PreconditionFailed) if cas_retry < SCANNER_PERSIST_CAS_RETRIES => {
                    cas_retry += 1;
                    debug!(
                        target: "rustfs::scanner",
                        event = EVENT_SCANNER_PERSIST_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_RUNTIME,
                        path = %target_path,
                        state = "conflict_retry",
                        retry = cas_retry,
                        "Scanner data usage CAS conflict will be reconciled"
                    );
                }
                Err(e @ EcstoreError::ObjectNotFound(_, _)) => {
                    if let Some(reason) = route_probe().await {
                        warn!(
                            target: "rustfs::scanner",
                            event = EVENT_SCANNER_PERSIST_STATE,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_RUNTIME,
                            state = "publication_deferred",
                            reason = reason.as_str(),
                            path = %target_path,
                            error = %e,
                            "Scanner data usage route remains blocked; retrying later"
                        );
                        break DataUsagePersistOutcome::Deferred(reason);
                    }
                    error!(
                        target: "rustfs::scanner",
                        event = EVENT_SCANNER_PERSIST_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_RUNTIME,
                        path = %target_path,
                        state = "save_failed",
                        error = %e,
                        "Scanner data usage save failed"
                    );
                    break DataUsagePersistOutcome::Failed;
                }
                Err(e) => {
                    error!(
                        target: "rustfs::scanner",
                        event = EVENT_SCANNER_PERSIST_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_RUNTIME,
                        path = %target_path,
                        state = if matches!(e, EcstoreError::PreconditionFailed) {
                            "conflict_retries_exhausted"
                        } else {
                            "save_failed"
                        },
                        error = %e,
                        "Scanner data usage save failed"
                    );
                    break DataUsagePersistOutcome::Failed;
                }
            }
        };

        match save_outcome {
            DataUsagePersistOutcome::Current => {
                if observational {
                    invalidate_admin_data_usage_snapshot_cache().await;
                } else {
                    invalidate_data_usage_snapshot_cache().await;
                }
                global_metrics().record_scanner_usage_save_result(ScannerUsageSaveResult::SkippedStale);
                outcome = DataUsagePersistOutcome::Current;
                continue;
            }
            DataUsagePersistOutcome::AlreadyDurable => {
                if observational {
                    invalidate_admin_data_usage_snapshot_cache().await;
                } else {
                    let cleanup_ok = cleanup_observed_data_usage_snapshot_for_epoch_and_lease(
                        storeapi.clone(),
                        &data_usage_info,
                        expected_publication_epoch,
                        remote_lease_deadline,
                        scanner_publication_lease_fence.as_deref(),
                        &remote_lease_tokens,
                        Arc::clone(&lease_release_safe),
                    )
                    .await;
                    if expected_publication_epoch.is_some() && !cleanup_ok {
                        outcome = DataUsagePersistOutcome::Deferred(ScannerCycleDeferReason::DataMovement);
                        break 'updates;
                    }
                    invalidate_data_usage_snapshot_cache().await;
                    replace_bucket_usage_memory_from_info(&data_usage_info).await;
                }
                global_metrics().record_scanner_usage_save_result(ScannerUsageSaveResult::Success);
                global_metrics().record_scanner_usage_durable_success();
                outcome = DataUsagePersistOutcome::AlreadyDurable;
            }
            DataUsagePersistOutcome::PriorCycleDurable => {
                if observational {
                    invalidate_admin_data_usage_snapshot_cache().await;
                } else {
                    let cleanup_ok = cleanup_observed_data_usage_snapshot_for_epoch_and_lease(
                        storeapi.clone(),
                        &data_usage_info,
                        expected_publication_epoch,
                        remote_lease_deadline,
                        scanner_publication_lease_fence.as_deref(),
                        &remote_lease_tokens,
                        Arc::clone(&lease_release_safe),
                    )
                    .await;
                    if expected_publication_epoch.is_some() && !cleanup_ok {
                        outcome = DataUsagePersistOutcome::Deferred(ScannerCycleDeferReason::DataMovement);
                        break 'updates;
                    }
                    invalidate_data_usage_snapshot_cache().await;
                }
                global_metrics().record_scanner_usage_save_result(ScannerUsageSaveResult::Success);
                global_metrics().record_scanner_usage_durable_success();
                outcome = DataUsagePersistOutcome::PriorCycleDurable;
            }
            DataUsagePersistOutcome::NoUpdate => {
                global_metrics().record_scanner_usage_publication("no_update", "no_update");
                global_metrics().record_scanner_usage_save_result(ScannerUsageSaveResult::Failed);
                outcome = DataUsagePersistOutcome::NoUpdate;
                continue;
            }
            DataUsagePersistOutcome::Failed => {
                global_metrics().record_scanner_usage_publication("failed", "save_failed");
                global_metrics().record_scanner_usage_save_result(ScannerUsageSaveResult::Failed);
                outcome = DataUsagePersistOutcome::Failed;
                continue;
            }
            DataUsagePersistOutcome::Deferred(reason) => {
                // A deferred publication is an intentional retryable state, not a
                // failed save. Keep the last real save result so admin freshness
                // reporting does not turn a pool-recovery fence into a false error.
                global_metrics().record_scanner_usage_deferred(reason.as_str());
                outcome = DataUsagePersistOutcome::Deferred(reason);
                break 'updates;
            }
            DataUsagePersistOutcome::Saved => {
                if observational {
                    invalidate_admin_data_usage_snapshot_cache().await;
                } else {
                    let cleanup_ok = cleanup_observed_data_usage_snapshot_for_epoch_and_lease(
                        storeapi.clone(),
                        &data_usage_info,
                        expected_publication_epoch,
                        remote_lease_deadline,
                        scanner_publication_lease_fence.as_deref(),
                        &remote_lease_tokens,
                        Arc::clone(&lease_release_safe),
                    )
                    .await;
                    if expected_publication_epoch.is_some() && !cleanup_ok {
                        outcome = DataUsagePersistOutcome::Deferred(ScannerCycleDeferReason::DataMovement);
                        break 'updates;
                    }
                    invalidate_data_usage_snapshot_cache().await;
                    replace_bucket_usage_memory_from_info(&data_usage_info).await;
                }
                global_metrics().record_scanner_usage_save_result(ScannerUsageSaveResult::Success);
                global_metrics().record_scanner_usage_durable_success();
                outcome = DataUsagePersistOutcome::Saved;
                debug!(
                    target: "rustfs::scanner",
                    event = EVENT_SCANNER_PERSIST_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    state = "saved",
                    snapshot_kind = if observational { "observed" } else { "authoritative" },
                    cycle = ?data_usage_info.scanner_cycle,
                    "Scanner usage snapshot saved"
                );
            }
        }

        if backup_due {
            let done_save = Metrics::time(Metric::SaveUsage);
            let backup_result = sync_data_usage_backup_from_primary_for_epoch_and_lease_and_fence_and_scope(
                &ctx,
                storeapi.clone(),
                expected_publication_epoch,
                remote_lease_deadline,
                scanner_publication_lease_fence.as_deref(),
                remote_lease_tokens.clone(),
                Arc::clone(&lease_release_safe),
            )
            .await;
            done_save();
            if let Err(e) = backup_result {
                warn!(
                    target: "rustfs::scanner",
                    event = EVENT_SCANNER_PERSIST_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    path = %format!("{}.bkp", DATA_USAGE_OBJ_NAME_PATH.as_str()),
                    state = "backup_save_failed",
                    error = %e,
                    "Scanner data usage backup save failed"
                );
                if scanner_publication_epoch_changed(&e) {
                    outcome = DataUsagePersistOutcome::Deferred(ScannerCycleDeferReason::DataMovement);
                    break 'updates;
                }
                outcome = DataUsagePersistOutcome::Failed;
                break 'updates;
            }
        }
        if !observational
            && matches!(outcome, DataUsagePersistOutcome::Saved | DataUsagePersistOutcome::AlreadyDurable)
            && (outcome == DataUsagePersistOutcome::AlreadyDurable || write_confirmed)
            && let (Some(expected), Some(epoch)) = (ack_expectation.as_ref(), ack_epoch)
            && expected.matches_encoded_candidate(&data_digest)
        {
            proof = read_root_publication_proof(
                storeapi.clone(),
                &ctx,
                ack_deadline,
                epoch,
                expected,
                &data_usage_info,
                written_etag.as_deref(),
            )
            .await;
            if proof.is_none() {
                proof = root_write_publication_proof(outcome, write_confirmed, written_etag.as_deref(), expected, &data_digest);
            }
            if outcome == DataUsagePersistOutcome::AlreadyDurable {
                observation = proof.as_ref().map(|_| usage_snapshot_observation(&data_usage_info));
            }
        }
    }

    if !matches!(
        outcome,
        DataUsagePersistOutcome::Saved | DataUsagePersistOutcome::AlreadyDurable | DataUsagePersistOutcome::PriorCycleDurable
    ) {
        observation = None;
    }
    DataUsagePublicationResult {
        outcome,
        proof,
        observation,
    }
}

async fn cleanup_observed_data_usage_snapshot_for_epoch_and_lease(
    storeapi: Arc<impl ScannerObjectIO + ScannerConfigObjectDelete>,
    authoritative: &DataUsageInfo,
    expected_publication_epoch: Option<u64>,
    remote_lease_deadline: Option<std::time::Instant>,
    scanner_publication_lease_fence: Option<&str>,
    remote_lease_tokens: &[Uuid],
    lease_release_safe: Arc<AtomicBool>,
) -> bool {
    if remote_lease_expired(remote_lease_deadline) {
        return false;
    }
    let read_epoch = match expected_publication_epoch {
        Some(expected_epoch) => {
            if scanner_publication_admission_for_epoch(storeapi.clone(), expected_epoch)
                .await
                .is_none()
            {
                return false;
            }
            expected_epoch
        }
        None => match scanner_publication_epoch(storeapi.clone()).await {
            Some(read_epoch) => read_epoch,
            None => return false,
        },
    };
    if remote_lease_expired(remote_lease_deadline)
        || expected_publication_epoch.is_some()
            && scanner_publication_admission_for_epoch(storeapi.clone(), read_epoch)
                .await
                .is_none()
    {
        return false;
    }
    let (observed_data, revision) =
        match read_config_with_revision(storeapi.clone(), DATA_USAGE_OBSERVED_OBJ_NAME_PATH.as_str()).await {
            Ok((Some(data), revision)) => (data, revision),
            Ok((None, _)) => return true,
            Err(err) => {
                error!(
                    target: "rustfs::scanner",
                    event = EVENT_SCANNER_PERSIST_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    path = %DATA_USAGE_OBSERVED_OBJ_NAME_PATH.as_str(),
                    state = "observed_cleanup_read_failed",
                    error = %err,
                    "Scanner could not inspect observational data usage snapshot before authoritative cleanup"
                );
                return true;
            }
        };
    let observed = match serde_json::from_slice::<DataUsageInfo>(&observed_data) {
        Ok(observed) => observed,
        Err(err) => {
            error!(
                target: "rustfs::scanner",
                event = EVENT_SCANNER_PERSIST_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                path = %DATA_USAGE_OBSERVED_OBJ_NAME_PATH.as_str(),
                state = "observed_cleanup_decode_failed",
                error = %err,
                "Scanner refused to remove an invalid observational data usage snapshot after authoritative save"
            );
            return true;
        }
    };
    if observed_data_usage_is_newer(&observed, authoritative) {
        return true;
    }
    if remote_lease_expired(remote_lease_deadline) {
        return false;
    }

    let publication_scope = storeapi
        .scanner_data_usage_publication_commit_scope_with_release_flag(
            read_epoch,
            scanner_publication_scope_deadline(data_usage_persist_timeout(), remote_lease_deadline),
            remote_lease_tokens.to_vec(),
            Arc::clone(&lease_release_safe),
        )
        .await;
    let result = crate::delete_config_with_publication_scope_for_epoch(
        storeapi,
        RUSTFS_META_BUCKET,
        DATA_USAGE_OBSERVED_OBJ_NAME_PATH.as_str(),
        ScannerObjectOptions {
            delete_prefix: true,
            delete_prefix_object: true,
            http_preconditions: Some(revision.preconditions()),
            user_defined: scanner_publication_lease_fence
                .map(|fence| {
                    HashMap::from([(
                        crate::storage_api::owner::SCANNER_PUBLICATION_LEASE_FENCE_METADATA_KEY.to_string(),
                        fence.to_string(),
                    )])
                })
                .unwrap_or_default(),
            ..Default::default()
        },
        read_epoch,
        publication_scope.clone(),
    )
    .await;

    let result = if let Some(scope) = publication_scope {
        match scope.wait_for_completion().await {
            ScannerPublicationCommitState::Committed | ScannerPublicationCommitState::AbortedBeforeCommit => result,
            ScannerPublicationCommitState::Indeterminate
            | ScannerPublicationCommitState::Admitted
            | ScannerPublicationCommitState::InFlight => Err(EcstoreError::other(
                "scanner publication cleanup scope did not reach a safe terminal state",
            )),
        }
    } else {
        result
    };

    match result {
        Ok(_)
        | Err(
            EcstoreError::FileNotFound
            | EcstoreError::ConfigNotFound
            | EcstoreError::ObjectNotFound(_, _)
            | EcstoreError::PreconditionFailed,
        ) => {}
        Err(err) => {
            error!(
                target: "rustfs::scanner",
                event = EVENT_SCANNER_PERSIST_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                path = %DATA_USAGE_OBSERVED_OBJ_NAME_PATH.as_str(),
                state = "observed_cleanup_failed",
                error = %err,
                "Scanner could not remove stale observational data usage snapshot after authoritative save"
            );
            if scanner_publication_epoch_changed(&err) {
                return false;
            }
        }
    }
    true
}
