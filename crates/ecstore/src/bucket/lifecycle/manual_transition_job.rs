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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use rustfs_utils::crypto::{hex_sha256, is_sha256_checksum};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use uuid::Uuid;

#[cfg(test)]
use crate::bucket::lifecycle::bucket_lifecycle_ops::encode_manual_transition_continuation_token;
use crate::bucket::lifecycle::bucket_lifecycle_ops::{
    ManualTransitionQueueSnapshot, ManualTransitionRunOptions, ManualTransitionRunReport,
};
use crate::bucket::lifecycle::config_boundary;
use crate::bucket::lifecycle::durable_namespace::{
    MANUAL_TRANSITION_JOB_NAMESPACE, MANUAL_TRANSITION_SCOPE_NAMESPACE, MANUAL_TRANSITION_TASK_NAMESPACE,
    MANUAL_TRANSITION_WORKER_RESULT_NAMESPACE,
};
use crate::disk::RUSTFS_META_BUCKET;
use crate::error::{Error, Result as EcstoreResult};
use crate::object_api::ObjectOptions;
use crate::storage_api_contracts::list::ListOperations as _;
use crate::storage_api_contracts::object::HTTPPreconditions;
use crate::store::ECStore;

pub const MANUAL_TRANSITION_JOB_SCHEMA: &str = "rustfs-manual-transition-job-v1";
pub const MANUAL_TRANSITION_TASK_SCHEMA: &str = "rustfs-manual-transition-task-v1";
pub const MANUAL_TRANSITION_WORKER_RESULT_SCHEMA: &str = "rustfs-manual-transition-worker-result-v1";
pub const MANUAL_TRANSITION_JOB_RECORD_PREFIX: &str = MANUAL_TRANSITION_JOB_NAMESPACE.prefix;
pub const MANUAL_TRANSITION_SCOPE_RECORD_PREFIX: &str = MANUAL_TRANSITION_SCOPE_NAMESPACE.prefix;
pub const MANUAL_TRANSITION_TASK_PREFIX: &str = MANUAL_TRANSITION_TASK_NAMESPACE.prefix;
pub const MANUAL_TRANSITION_WORKER_RESULT_PREFIX: &str = MANUAL_TRANSITION_WORKER_RESULT_NAMESPACE.prefix;
pub const MAX_MANUAL_TRANSITION_JOB_RECORD_SIZE: usize = 64 * 1024;
pub const MAX_MANUAL_TRANSITION_TASK_RECORD_SIZE: usize = 16 * 1024;
pub const MAX_MANUAL_TRANSITION_WORKER_RESULT_RECORD_SIZE: usize = 8 * 1024;
const MANUAL_TRANSITION_JOB_LEASE_SECONDS: i128 = 60;
const MANUAL_TRANSITION_LEGACY_SCOPE_SCAN_LIMIT: i32 = 1000;
const MANUAL_TRANSITION_TASK_SCAN_LIMIT: i32 = 1000;
const MANUAL_TRANSITION_WORKER_RESULT_SCAN_LIMIT: i32 = 1000;
const MANUAL_TRANSITION_JOB_CAS_RETRIES: usize = 4;

#[cfg(test)]
struct ManualTransitionJobCasBarrierState {
    job_id: Uuid,
    paused: std::sync::atomic::AtomicBool,
    arrived: tokio::sync::Notify,
    release: tokio::sync::Semaphore,
}

#[cfg(test)]
pub(crate) struct ManualTransitionJobCasBarrier {
    state: Arc<ManualTransitionJobCasBarrierState>,
}

#[cfg(test)]
static MANUAL_TRANSITION_JOB_CAS_BARRIER: std::sync::OnceLock<std::sync::Mutex<Option<Arc<ManualTransitionJobCasBarrierState>>>> =
    std::sync::OnceLock::new();

#[cfg(test)]
impl ManualTransitionJobCasBarrier {
    pub(crate) fn install(job_id: Uuid) -> Self {
        let state = Arc::new(ManualTransitionJobCasBarrierState {
            job_id,
            paused: std::sync::atomic::AtomicBool::new(false),
            arrived: tokio::sync::Notify::new(),
            release: tokio::sync::Semaphore::new(0),
        });
        let mut slot = MANUAL_TRANSITION_JOB_CAS_BARRIER
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("manual transition progress CAS barrier mutex should not poison");
        assert!(
            slot.is_none(),
            "manual transition job CAS barrier must be installed by one test at a time"
        );
        *slot = Some(Arc::clone(&state));
        drop(slot);
        Self { state }
    }

    pub(crate) async fn wait_until_paused(&self) {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                let arrived = self.state.arrived.notified();
                if self.state.paused.load(std::sync::atomic::Ordering::Acquire) {
                    return;
                }
                arrived.await;
            }
        })
        .await
        .expect("manual transition job update should reach the deterministic CAS barrier");
    }

    pub(crate) fn release(&self) {
        self.state.release.add_permits(1);
    }
}

#[cfg(test)]
impl Drop for ManualTransitionJobCasBarrier {
    fn drop(&mut self) {
        self.release();
        let mut slot = MANUAL_TRANSITION_JOB_CAS_BARRIER
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("manual transition progress CAS barrier mutex should not poison");
        if slot.as_ref().is_some_and(|state| Arc::ptr_eq(state, &self.state)) {
            *slot = None;
        }
    }
}

#[cfg(test)]
async fn pause_manual_transition_job_before_first_cas(job_id: Uuid) {
    let barrier = MANUAL_TRANSITION_JOB_CAS_BARRIER
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("manual transition progress CAS barrier mutex should not poison")
        .as_ref()
        .filter(|barrier| barrier.job_id == job_id)
        .cloned();
    if let Some(barrier) = barrier
        && barrier
            .paused
            .compare_exchange(false, true, std::sync::atomic::Ordering::AcqRel, std::sync::atomic::Ordering::Acquire)
            .is_ok()
    {
        barrier.arrived.notify_one();
        barrier
            .release
            .acquire()
            .await
            .expect("manual transition job CAS barrier should remain open")
            .forget();
    }
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Debug, thiserror::Error)]
pub enum ManualTransitionJobError {
    #[error("manual transition job is corrupt: {0}")]
    Corrupt(&'static str),
    #[error("manual transition job schema is unsupported: {0}")]
    UnsupportedSchema(String),
    #[error("manual transition job checksum mismatch")]
    ChecksumMismatch,
    #[error("manual transition job json error: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManualTransitionJobState {
    Running,
    Completed,
    Partial,
    Failed,
    Cancelled,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManualTransitionJobRecord {
    pub job_id: Uuid,
    pub scope_key: String,
    pub bucket: String,
    pub prefix: String,
    pub tier: Option<String>,
    pub dry_run: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_objects: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_duration: Option<std::time::Duration>,
    pub owner_id: String,
    pub lease_id: Uuid,
    pub lease_expires_at_unix_nanos: i128,
    pub state: ManualTransitionJobState,
    #[serde(default, skip_serializing_if = "is_false")]
    pub scan_completed: bool,
    pub cancel_requested: bool,
    pub created_at_unix_nanos: i128,
    pub updated_at_unix_nanos: i128,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at_unix_nanos: Option<i128>,
    #[serde(default, skip_serializing)]
    pub cursor_revision: Option<u64>,
    pub report: ManualTransitionRunReport,
    pub queue_snapshot: ManualTransitionQueueSnapshot,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl ManualTransitionJobRecord {
    pub fn new(job_id: Uuid, bucket: &str, options: &ManualTransitionRunOptions, owner_id: impl Into<String>) -> Self {
        let scope_key = manual_transition_scope_key(bucket, options);
        let now = OffsetDateTime::now_utc().unix_timestamp_nanos();
        let lease_id = Uuid::new_v4();
        Self {
            job_id,
            scope_key,
            bucket: bucket.to_string(),
            prefix: options.prefix.clone(),
            tier: options.tier.clone(),
            dry_run: options.dry_run,
            max_objects: options.max_objects,
            max_duration: options.max_duration,
            owner_id: owner_id.into(),
            lease_id,
            lease_expires_at_unix_nanos: manual_transition_job_lease_expires_at(now),
            state: ManualTransitionJobState::Running,
            scan_completed: false,
            cancel_requested: false,
            created_at_unix_nanos: now,
            updated_at_unix_nanos: now,
            completed_at_unix_nanos: None,
            cursor_revision: None,
            report: ManualTransitionRunReport {
                bucket: bucket.to_string(),
                prefix: options.prefix.clone(),
                tier: options.tier.clone(),
                dry_run: options.dry_run,
                ..Default::default()
            },
            queue_snapshot: ManualTransitionQueueSnapshot::default(),
            error: None,
        }
    }

    pub fn complete(&mut self, report: ManualTransitionRunReport, queue_snapshot: ManualTransitionQueueSnapshot) {
        self.scan_completed = true;
        self.merge_scan_report(&report);
        self.queue_snapshot = queue_snapshot;
        self.error = None;
        self.mark_terminal_if_worker_drained();
    }

    pub fn fail(&mut self, error: impl Into<String>) {
        self.state = ManualTransitionJobState::Failed;
        self.error = Some(error.into());
        self.mark_updated_terminal();
    }

    pub fn mark_unknown_for_worker_result_journal_error(
        &mut self,
        error: impl Into<String>,
        queue_snapshot: ManualTransitionQueueSnapshot,
    ) -> bool {
        if self.state != ManualTransitionJobState::Running || !self.scan_completed || !self.report.worker_transition_pending() {
            return false;
        }
        self.queue_snapshot = queue_snapshot;
        self.state = ManualTransitionJobState::Unknown;
        self.error = Some(format!("manual transition worker result journal is corrupt: {}", error.into()));
        self.mark_updated_terminal();
        true
    }

    pub fn mark_unknown_for_task_journal_error(
        &mut self,
        error: impl Into<String>,
        queue_snapshot: ManualTransitionQueueSnapshot,
    ) -> bool {
        if self.state != ManualTransitionJobState::Running || !self.scan_completed {
            return false;
        }
        self.queue_snapshot = queue_snapshot;
        self.state = ManualTransitionJobState::Unknown;
        self.error = Some(format!("manual transition task journal is corrupt: {}", error.into()));
        self.mark_updated_terminal();
        true
    }

    pub fn cancel_after_recovery(&mut self, queue_snapshot: ManualTransitionQueueSnapshot) {
        if self.state == ManualTransitionJobState::Running && self.cancel_requested {
            self.scan_completed = true;
            self.report.cancelled = true;
            self.queue_snapshot = queue_snapshot;
            self.error = None;
            self.state = ManualTransitionJobState::Cancelled;
            self.mark_updated_terminal();
        }
    }

    pub fn record_worker_result(&mut self, result: ManualTransitionWorkerResult, queue_snapshot: ManualTransitionQueueSnapshot) {
        self.record_worker_result_with_reason(result, queue_snapshot, None);
    }

    pub fn record_worker_result_with_reason(
        &mut self,
        result: ManualTransitionWorkerResult,
        queue_snapshot: ManualTransitionQueueSnapshot,
        failure_reason: Option<ManualTransitionWorkerFailureReason>,
    ) {
        if self.is_terminal() {
            return;
        }
        match result {
            ManualTransitionWorkerResult::Completed => {
                self.report.transition_completed = self.report.transition_completed.saturating_add(1);
            }
            ManualTransitionWorkerResult::TierFailure => {
                self.report.transition_failed = self.report.transition_failed.saturating_add(1);
                self.report.tier_failure = self.report.tier_failure.saturating_add(1);
                let reason = failure_reason.unwrap_or(ManualTransitionWorkerFailureReason::Unknown);
                *self.report.tier_failure_by_reason.entry(reason).or_insert(0) += 1;
            }
        }
        self.queue_snapshot = queue_snapshot;
        self.advance_updated_at();
        self.mark_terminal_if_worker_drained();
    }

    fn apply_worker_result_counts(
        &mut self,
        completed: u64,
        failed: u64,
        failure_reasons: &BTreeMap<ManualTransitionWorkerFailureReason, u64>,
        task_queued: u64,
        queue_snapshot: ManualTransitionQueueSnapshot,
    ) -> bool {
        if self.is_terminal() {
            return false;
        }
        let enqueued = self.report.enqueued.max(task_queued);
        if completed.saturating_add(failed) > enqueued {
            self.queue_snapshot = queue_snapshot;
            self.state = ManualTransitionJobState::Unknown;
            self.error = Some("manual transition worker result journal exceeds enqueued count".to_string());
            self.mark_updated_terminal();
            return true;
        }
        let transition_completed = self.report.transition_completed.max(completed);
        let transition_failed = self.report.transition_failed.max(failed);
        if enqueued == self.report.enqueued
            && transition_completed == self.report.transition_completed
            && transition_failed == self.report.transition_failed
        {
            return false;
        }
        let mut scan_tier_failure_by_reason = self.report.tier_failure_by_reason.clone();
        for (reason, count) in failure_reasons {
            let current = scan_tier_failure_by_reason.get(reason).copied().unwrap_or_default();
            scan_tier_failure_by_reason.insert(*reason, current.max(*count));
        }
        let scan_tier_failure = self.report.tier_failure.saturating_sub(self.report.transition_failed);
        self.report.enqueued = enqueued;
        self.report.transition_completed = transition_completed;
        self.report.transition_failed = transition_failed;
        self.report.tier_failure = scan_tier_failure.saturating_add(transition_failed);
        self.report.tier_failure_by_reason = scan_tier_failure_by_reason;
        self.queue_snapshot = queue_snapshot;
        self.advance_updated_at();
        self.mark_terminal_if_worker_drained();
        true
    }

    pub fn mark_cancel_requested(&mut self) {
        self.cancel_requested = true;
        self.advance_updated_at();
    }

    pub fn claim_recovery_lease(&mut self, owner_id: impl Into<String>, queue_snapshot: ManualTransitionQueueSnapshot) {
        if self.state == ManualTransitionJobState::Running {
            self.owner_id = owner_id.into();
            self.lease_id = Uuid::new_v4();
            self.renew_lease(queue_snapshot);
        }
    }

    pub fn abandon_recovery_lease(&mut self, lease_id: Uuid) {
        if self.state == ManualTransitionJobState::Running && self.lease_id == lease_id {
            self.lease_expires_at_unix_nanos = 0;
            self.advance_updated_at();
        }
    }

    pub fn resume_options(&self) -> ManualTransitionRunOptions {
        ManualTransitionRunOptions {
            prefix: self.prefix.clone(),
            continuation_token: self.report.continuation_token.clone(),
            tier: self.tier.clone(),
            dry_run: self.dry_run,
            max_objects: self.max_objects,
            max_duration: self.max_duration,
            ..Default::default()
        }
    }

    pub fn renew_lease(&mut self, queue_snapshot: ManualTransitionQueueSnapshot) {
        let now = OffsetDateTime::now_utc().unix_timestamp_nanos();
        self.updated_at_unix_nanos = self.updated_at_unix_nanos.saturating_add(1).max(now);
        self.lease_expires_at_unix_nanos = manual_transition_job_lease_expires_at(now);
        self.queue_snapshot = queue_snapshot;
    }

    pub fn mark_unknown_if_worker_results_lost(&mut self, queue_snapshot: ManualTransitionQueueSnapshot) -> bool {
        if self.state != ManualTransitionJobState::Running
            || !self.scan_completed
            || !self.report.worker_transition_pending()
            || queue_snapshot.queued > 0
            || queue_snapshot.active > 0
        {
            return false;
        }
        self.queue_snapshot = queue_snapshot;
        self.state = ManualTransitionJobState::Unknown;
        self.error = Some("manual transition worker result was not persisted before the transition queue drained".to_string());
        self.mark_updated_terminal();
        true
    }

    pub fn mark_unknown_if_recovery_would_skip_pending_page(&mut self, queue_snapshot: ManualTransitionQueueSnapshot) -> bool {
        if self.state != ManualTransitionJobState::Running
            || self.report.continuation_token.is_none()
            || !self.report.worker_transition_pending()
            || queue_snapshot.queued > 0
            || queue_snapshot.active > 0
        {
            return false;
        }
        self.queue_snapshot = queue_snapshot;
        self.state = ManualTransitionJobState::Unknown;
        self.error =
            Some("manual transition page/task journal is missing for pending work before the durable cursor".to_string());
        self.mark_updated_terminal();
        true
    }

    pub fn update_running_progress(&mut self, report: ManualTransitionRunReport, queue_snapshot: ManualTransitionQueueSnapshot) {
        if self.state == ManualTransitionJobState::Running {
            self.merge_scan_report(&report);
            self.renew_lease(queue_snapshot);
        }
    }

    fn merge_scan_report(&mut self, report: &ManualTransitionRunReport) {
        self.report.merge_scan_report_preserving_worker(report);
        self.cursor_revision = manual_transition_cursor_revision(&self.report);
    }

    pub fn mark_unknown_if_unowned(&mut self) {
        if self.state == ManualTransitionJobState::Running {
            self.state = ManualTransitionJobState::Unknown;
            self.error = Some("manual transition job outcome is unknown after restart or owner loss".to_string());
            self.mark_updated_terminal();
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self.state,
            ManualTransitionJobState::Completed
                | ManualTransitionJobState::Partial
                | ManualTransitionJobState::Failed
                | ManualTransitionJobState::Cancelled
                | ManualTransitionJobState::Unknown
        )
    }

    fn mark_updated_terminal(&mut self) {
        self.advance_updated_at();
        self.completed_at_unix_nanos = Some(self.updated_at_unix_nanos);
    }

    fn advance_updated_at(&mut self) {
        let now = OffsetDateTime::now_utc().unix_timestamp_nanos();
        self.updated_at_unix_nanos = self.updated_at_unix_nanos.saturating_add(1).max(now);
    }

    fn mark_terminal_if_worker_drained(&mut self) {
        if !self.scan_completed || self.report.worker_transition_pending() {
            return;
        }
        if self.cancel_requested {
            self.report.cancelled = true;
        }
        self.state = if self.report.cancelled {
            ManualTransitionJobState::Cancelled
        } else if self.report.was_truncated()
            || self.report.has_partial_enqueue()
            || self.report.tier_failure > 0
            || self.report.transition_failed > 0
        {
            ManualTransitionJobState::Partial
        } else {
            ManualTransitionJobState::Completed
        };
        self.mark_updated_terminal();
    }

    pub fn encode(&self) -> Result<Vec<u8>, ManualTransitionJobError> {
        self.validate()?;
        let job_bytes = serde_json::to_vec(self)?;
        let content_sha256 = hex_sha256(&job_bytes, ToOwned::to_owned);
        let persisted = PersistedManualTransitionJobRecord {
            schema: MANUAL_TRANSITION_JOB_SCHEMA.to_string(),
            content_sha256,
            job: self.clone(),
        };
        let encoded = serde_json::to_vec(&persisted)?;
        if encoded.len() > MAX_MANUAL_TRANSITION_JOB_RECORD_SIZE {
            return Err(ManualTransitionJobError::Corrupt("encoded job exceeds maximum size"));
        }
        Ok(encoded)
    }

    pub fn decode(expected_job_id: Uuid, data: &[u8]) -> Result<Self, ManualTransitionJobError> {
        if data.len() > MAX_MANUAL_TRANSITION_JOB_RECORD_SIZE {
            return Err(ManualTransitionJobError::Corrupt("encoded job exceeds maximum size"));
        }
        let persisted: PersistedManualTransitionJobRecord = serde_json::from_slice(data)?;
        if persisted.schema != MANUAL_TRANSITION_JOB_SCHEMA {
            return Err(ManualTransitionJobError::UnsupportedSchema(persisted.schema));
        }
        if !is_sha256_checksum(&persisted.content_sha256) {
            return Err(ManualTransitionJobError::Corrupt("content checksum is not a sha256 checksum"));
        }
        let mut job = persisted.job;
        let mut job_bytes = serde_json::to_vec(&job)?;
        let actual_checksum = hex_sha256(&job_bytes, ToOwned::to_owned);
        let checksum_match = if persisted.content_sha256 == actual_checksum {
            true
        } else {
            let mut job_value = serde_json::to_value(&job)?;
            if let Some(job_obj) = job_value.as_object_mut() {
                if let Some(report) = job_obj.get_mut("report").and_then(|value| value.as_object_mut()) {
                    report.remove("tier_failure_by_reason");
                }
            } else {
                return Err(ManualTransitionJobError::ChecksumMismatch);
            }
            job_bytes = serde_json::to_vec(&job_value)?;
            let fallback_checksum = hex_sha256(&job_bytes, ToOwned::to_owned);
            persisted.content_sha256 == fallback_checksum
        };
        if !checksum_match {
            return Err(ManualTransitionJobError::ChecksumMismatch);
        }
        if job.job_id != expected_job_id {
            return Err(ManualTransitionJobError::Corrupt("job_id does not match record key"));
        }
        if job.state == ManualTransitionJobState::Cancelled && job.cancel_requested {
            job.report.cancelled = true;
        }
        job.cursor_revision = manual_transition_cursor_revision(&job.report);
        job.validate()?;
        Ok(job)
    }

    fn validate(&self) -> Result<(), ManualTransitionJobError> {
        if self.job_id.is_nil() {
            return Err(ManualTransitionJobError::Corrupt("job_id is nil"));
        }
        if self.lease_id.is_nil() {
            return Err(ManualTransitionJobError::Corrupt("lease_id is nil"));
        }
        if self.scope_key.is_empty() {
            return Err(ManualTransitionJobError::Corrupt("scope_key is empty"));
        }
        if self.bucket.is_empty() {
            return Err(ManualTransitionJobError::Corrupt("bucket is empty"));
        }
        if self.owner_id.trim().is_empty() {
            return Err(ManualTransitionJobError::Corrupt("owner_id is empty"));
        }
        if self.completed_at_unix_nanos.is_some() && !self.is_terminal() {
            return Err(ManualTransitionJobError::Corrupt("non-terminal job has completed timestamp"));
        }
        if self.state == ManualTransitionJobState::Cancelled && !self.cancel_requested {
            return Err(ManualTransitionJobError::Corrupt("cancelled job is missing cancel request"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManualTransitionWorkerResult {
    Completed,
    TierFailure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManualTransitionWorkerFailureReason {
    Unknown,
    NotFound,
    Network,
    PermissionDenied,
    Timeout,
    Quorum,
    SlowDown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManualTransitionTaskRecord {
    pub schema: String,
    pub job_id: Uuid,
    pub task_key: String,
    pub bucket: String,
    pub object: String,
    pub version_id: Option<Uuid>,
    pub storage_class: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mod_time_unix_nanos: Option<i128>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_latest: Option<bool>,
    pub queued_at_unix_nanos: i128,
}

impl ManualTransitionTaskRecord {
    pub fn new(
        job_id: Uuid,
        task_key: impl Into<String>,
        bucket: impl Into<String>,
        object: impl Into<String>,
        version_id: Option<Uuid>,
        storage_class: impl Into<String>,
    ) -> Self {
        Self {
            schema: MANUAL_TRANSITION_TASK_SCHEMA.to_string(),
            job_id,
            task_key: task_key.into(),
            bucket: bucket.into(),
            object: object.into(),
            version_id,
            storage_class: storage_class.into(),
            etag: None,
            mod_time_unix_nanos: None,
            size: None,
            is_latest: None,
            queued_at_unix_nanos: OffsetDateTime::now_utc().unix_timestamp_nanos(),
        }
    }

    pub fn with_object_metadata(
        mut self,
        etag: Option<String>,
        mod_time: Option<OffsetDateTime>,
        size: i64,
        is_latest: bool,
    ) -> Self {
        self.etag = etag;
        self.mod_time_unix_nanos = mod_time.map(|time| time.unix_timestamp_nanos());
        self.size = Some(size);
        self.is_latest = Some(is_latest);
        self
    }

    pub fn encode(&self) -> Result<Vec<u8>, ManualTransitionJobError> {
        self.validate()?;
        let record_bytes = serde_json::to_vec(self)?;
        let content_sha256 = hex_sha256(&record_bytes, ToOwned::to_owned);
        let persisted = PersistedManualTransitionTaskRecord {
            schema: MANUAL_TRANSITION_TASK_SCHEMA.to_string(),
            content_sha256,
            record: self.clone(),
        };
        let encoded = serde_json::to_vec(&persisted)?;
        if encoded.len() > MAX_MANUAL_TRANSITION_TASK_RECORD_SIZE {
            return Err(ManualTransitionJobError::Corrupt("encoded task record exceeds maximum size"));
        }
        Ok(encoded)
    }

    pub fn decode(expected_job_id: Uuid, expected_task_key: &str, data: &[u8]) -> Result<Self, ManualTransitionJobError> {
        if data.len() > MAX_MANUAL_TRANSITION_TASK_RECORD_SIZE {
            return Err(ManualTransitionJobError::Corrupt("encoded task record exceeds maximum size"));
        }
        let persisted: PersistedManualTransitionTaskRecord = serde_json::from_slice(data)?;
        if persisted.schema != MANUAL_TRANSITION_TASK_SCHEMA {
            return Err(ManualTransitionJobError::UnsupportedSchema(persisted.schema));
        }
        if !is_sha256_checksum(&persisted.content_sha256) {
            return Err(ManualTransitionJobError::Corrupt("task record content checksum is not a sha256 checksum"));
        }
        let record = persisted.record;
        let record_bytes = serde_json::to_vec(&record)?;
        let actual_checksum = hex_sha256(&record_bytes, ToOwned::to_owned);
        if persisted.content_sha256 != actual_checksum {
            return Err(ManualTransitionJobError::ChecksumMismatch);
        }
        if record.job_id != expected_job_id {
            return Err(ManualTransitionJobError::Corrupt("task record job_id does not match record key"));
        }
        if record.task_key != expected_task_key {
            return Err(ManualTransitionJobError::Corrupt("task record task_key does not match record key"));
        }
        record.validate()?;
        Ok(record)
    }

    fn validate(&self) -> Result<(), ManualTransitionJobError> {
        if self.job_id.is_nil() {
            return Err(ManualTransitionJobError::Corrupt("task record job_id is nil"));
        }
        if !is_sha256_checksum(&self.task_key) {
            return Err(ManualTransitionJobError::Corrupt("task record task_key is not a sha256 checksum"));
        }
        if self.bucket.is_empty() {
            return Err(ManualTransitionJobError::Corrupt("task record bucket is empty"));
        }
        if self.object.is_empty() {
            return Err(ManualTransitionJobError::Corrupt("task record object is empty"));
        }
        if self.storage_class.trim().is_empty() {
            return Err(ManualTransitionJobError::Corrupt("task record storage_class is empty"));
        }
        if self.size.is_some_and(|size| size < 0) {
            return Err(ManualTransitionJobError::Corrupt("task record size is negative"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedManualTransitionTaskRecord {
    schema: String,
    content_sha256: String,
    record: ManualTransitionTaskRecord,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ManualTransitionWorkerResultStats {
    pub completed: u64,
    pub failed: u64,
    pub tier_failure_by_reason: BTreeMap<ManualTransitionWorkerFailureReason, u64>,
}

impl ManualTransitionWorkerResultStats {
    fn record(&mut self, result: ManualTransitionWorkerResult, failure_reason: Option<ManualTransitionWorkerFailureReason>) {
        match result {
            ManualTransitionWorkerResult::Completed => self.completed = self.completed.saturating_add(1),
            ManualTransitionWorkerResult::TierFailure => {
                self.failed = self.failed.saturating_add(1);
                let reason = failure_reason.unwrap_or(ManualTransitionWorkerFailureReason::Unknown);
                *self.tier_failure_by_reason.entry(reason).or_insert(0) += 1;
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManualTransitionWorkerResultRecord {
    pub schema: String,
    pub job_id: Uuid,
    pub task_key: String,
    pub result: ManualTransitionWorkerResult,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<ManualTransitionWorkerFailureReason>,
    pub completed_at_unix_nanos: i128,
}

impl ManualTransitionWorkerResultRecord {
    #[allow(
        dead_code,
        reason = "MinIO-parity tier/lifecycle entry point that this port never wired (backlog#1823)"
    )]
    pub fn new(job_id: Uuid, task_key: impl Into<String>, result: ManualTransitionWorkerResult) -> Self {
        Self::new_with_reason(job_id, task_key, result, None)
    }

    pub fn new_with_reason(
        job_id: Uuid,
        task_key: impl Into<String>,
        result: ManualTransitionWorkerResult,
        failure_reason: Option<ManualTransitionWorkerFailureReason>,
    ) -> Self {
        Self {
            schema: MANUAL_TRANSITION_WORKER_RESULT_SCHEMA.to_string(),
            job_id,
            task_key: task_key.into(),
            result,
            failure_reason,
            completed_at_unix_nanos: OffsetDateTime::now_utc().unix_timestamp_nanos(),
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, ManualTransitionJobError> {
        self.validate()?;
        let record_bytes = serde_json::to_vec(self)?;
        let content_sha256 = hex_sha256(&record_bytes, ToOwned::to_owned);
        let persisted = PersistedManualTransitionWorkerResultRecord {
            schema: MANUAL_TRANSITION_WORKER_RESULT_SCHEMA.to_string(),
            content_sha256,
            record: self.clone(),
        };
        let encoded = serde_json::to_vec(&persisted)?;
        if encoded.len() > MAX_MANUAL_TRANSITION_WORKER_RESULT_RECORD_SIZE {
            return Err(ManualTransitionJobError::Corrupt("encoded worker result exceeds maximum size"));
        }
        Ok(encoded)
    }

    pub fn decode(expected_job_id: Uuid, expected_task_key: &str, data: &[u8]) -> Result<Self, ManualTransitionJobError> {
        if data.len() > MAX_MANUAL_TRANSITION_WORKER_RESULT_RECORD_SIZE {
            return Err(ManualTransitionJobError::Corrupt("encoded worker result exceeds maximum size"));
        }
        let persisted: PersistedManualTransitionWorkerResultRecord = serde_json::from_slice(data)?;
        if persisted.schema != MANUAL_TRANSITION_WORKER_RESULT_SCHEMA {
            return Err(ManualTransitionJobError::UnsupportedSchema(persisted.schema));
        }
        if !is_sha256_checksum(&persisted.content_sha256) {
            return Err(ManualTransitionJobError::Corrupt(
                "worker result content checksum is not a sha256 checksum",
            ));
        }
        let record = persisted.record;
        let mut record_bytes = serde_json::to_vec(&record)?;
        let actual_checksum = hex_sha256(&record_bytes, ToOwned::to_owned);
        let checksum_match = if persisted.content_sha256 == actual_checksum {
            true
        } else {
            if let Some(record_value) = serde_json::to_value(&record)?.as_object_mut() {
                if record.failure_reason.is_none() {
                    record_value.remove("failure_reason");
                }
                record_bytes = serde_json::to_vec(record_value)?;
            } else {
                return Err(ManualTransitionJobError::ChecksumMismatch);
            }
            let fallback_checksum = hex_sha256(&record_bytes, ToOwned::to_owned);
            persisted.content_sha256 == fallback_checksum
        };
        if !checksum_match {
            return Err(ManualTransitionJobError::ChecksumMismatch);
        }
        if record.job_id != expected_job_id {
            return Err(ManualTransitionJobError::Corrupt("worker result job_id does not match record key"));
        }
        if record.task_key != expected_task_key {
            return Err(ManualTransitionJobError::Corrupt("worker result task_key does not match record key"));
        }
        record.validate()?;
        Ok(record)
    }

    fn validate(&self) -> Result<(), ManualTransitionJobError> {
        if self.schema != MANUAL_TRANSITION_WORKER_RESULT_SCHEMA {
            return Err(ManualTransitionJobError::UnsupportedSchema(self.schema.clone()));
        }
        if self.job_id.is_nil() {
            return Err(ManualTransitionJobError::Corrupt("worker result job_id is nil"));
        }
        if !is_sha256_checksum(&self.task_key) {
            return Err(ManualTransitionJobError::Corrupt("worker result task_key is not a sha256 checksum"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedManualTransitionWorkerResultRecord {
    schema: String,
    content_sha256: String,
    record: ManualTransitionWorkerResultRecord,
}

fn manual_transition_job_lease_expires_at(now_unix_nanos: i128) -> i128 {
    now_unix_nanos.saturating_add(MANUAL_TRANSITION_JOB_LEASE_SECONDS.saturating_mul(1_000_000_000))
}

pub fn manual_transition_job_lease_expired(record: &ManualTransitionJobRecord) -> bool {
    OffsetDateTime::now_utc().unix_timestamp_nanos() > record.lease_expires_at_unix_nanos
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedManualTransitionJobRecord {
    schema: String,
    content_sha256: String,
    job: ManualTransitionJobRecord,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManualTransitionScopeAdmission {
    pub schema: String,
    pub scope_key: String,
    pub job_id: Uuid,
    pub lease_id: Uuid,
    pub owner_id: String,
    pub bucket: String,
    pub prefix: String,
    pub tier: Option<String>,
    pub dry_run: bool,
    pub lease_expires_at_unix_nanos: i128,
    pub updated_at_unix_nanos: i128,
}

impl ManualTransitionScopeAdmission {
    pub fn from_job(record: &ManualTransitionJobRecord) -> Self {
        Self {
            schema: MANUAL_TRANSITION_JOB_SCHEMA.to_string(),
            scope_key: record.scope_key.clone(),
            job_id: record.job_id,
            lease_id: record.lease_id,
            owner_id: record.owner_id.clone(),
            bucket: record.bucket.clone(),
            prefix: record.prefix.clone(),
            tier: record.tier.clone(),
            dry_run: record.dry_run,
            lease_expires_at_unix_nanos: record.lease_expires_at_unix_nanos,
            updated_at_unix_nanos: record.updated_at_unix_nanos,
        }
    }

    pub fn validate(&self) -> Result<(), ManualTransitionJobError> {
        if self.schema != MANUAL_TRANSITION_JOB_SCHEMA {
            return Err(ManualTransitionJobError::UnsupportedSchema(self.schema.clone()));
        }
        if self.job_id.is_nil() {
            return Err(ManualTransitionJobError::Corrupt("job_id is nil"));
        }
        if self.lease_id.is_nil() {
            return Err(ManualTransitionJobError::Corrupt("lease_id is nil"));
        }
        if self.scope_key.is_empty() {
            return Err(ManualTransitionJobError::Corrupt("scope_key is empty"));
        }
        if self.bucket.is_empty() {
            return Err(ManualTransitionJobError::Corrupt("bucket is empty"));
        }
        if self.owner_id.trim().is_empty() {
            return Err(ManualTransitionJobError::Corrupt("owner_id is empty"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManualTransitionScopeAdmissionClaim {
    Claimed,
    Conflict(Box<ManualTransitionScopeAdmission>),
}

pub fn manual_transition_scope_key(bucket: &str, options: &ManualTransitionRunOptions) -> String {
    let mut scope = String::new();
    scope.push_str(bucket);
    scope.push('\0');
    // Durable admission v1 is bucket-level: a single CAS alias must cover prefix
    // overlap and wildcard-tier conflicts without a non-atomic scope scan.
    scope.push_str(if options.dry_run { "dry_run" } else { "run" });
    hex_sha256(scope.as_bytes(), ToOwned::to_owned)
}

pub(crate) fn legacy_manual_transition_scope_key(bucket: &str, options: &ManualTransitionRunOptions) -> String {
    let mut scope = String::new();
    scope.push_str(bucket);
    scope.push('\0');
    scope.push_str(&options.prefix);
    scope.push('\0');
    if let Some(tier) = &options.tier {
        scope.push_str(&tier.to_ascii_uppercase());
    }
    scope.push('\0');
    scope.push_str(if options.dry_run { "dry_run" } else { "run" });
    hex_sha256(scope.as_bytes(), ToOwned::to_owned)
}

pub fn manual_transition_job_record_object_name(job_id: Uuid) -> Result<String, ManualTransitionJobError> {
    if job_id.is_nil() {
        return Err(ManualTransitionJobError::Corrupt("job_id is nil"));
    }
    let job_key = job_id.simple().to_string();
    Ok(format!(
        "{}/{}/{}/{}.json",
        MANUAL_TRANSITION_JOB_RECORD_PREFIX,
        &job_key[..2],
        &job_key[2..4],
        job_key
    ))
}

fn manual_transition_job_sharded_prefix(prefix: &str, job_id: Uuid) -> Result<String, ManualTransitionJobError> {
    if job_id.is_nil() {
        return Err(ManualTransitionJobError::Corrupt("job_id is nil"));
    }
    let job_key = job_id.simple().to_string();
    Ok(format!("{}/{}/{}/{}", prefix, &job_key[..2], &job_key[2..4], job_key))
}

pub fn manual_transition_worker_result_task_key(bucket: &str, object: &str, version_id: Option<Uuid>) -> String {
    let version = version_id.map(|version| version.to_string()).unwrap_or_default();
    let mut material = Vec::with_capacity(bucket.len() + object.len() + version.len() + 32);
    push_len_prefixed(&mut material, bucket.as_bytes());
    push_len_prefixed(&mut material, object.as_bytes());
    push_len_prefixed(&mut material, version.as_bytes());
    hex_sha256(&material, ToOwned::to_owned)
}

fn push_len_prefixed(out: &mut Vec<u8>, value: &[u8]) {
    out.extend_from_slice(value.len().to_string().as_bytes());
    out.push(b':');
    out.extend_from_slice(value);
}

pub fn manual_transition_worker_result_object_prefix(job_id: Uuid) -> Result<String, ManualTransitionJobError> {
    manual_transition_job_sharded_prefix(MANUAL_TRANSITION_WORKER_RESULT_PREFIX, job_id)
}

pub fn manual_transition_task_object_prefix(job_id: Uuid) -> Result<String, ManualTransitionJobError> {
    manual_transition_job_sharded_prefix(MANUAL_TRANSITION_TASK_PREFIX, job_id)
}

pub fn manual_transition_task_object_name(job_id: Uuid, task_key: &str) -> Result<String, ManualTransitionJobError> {
    if !is_sha256_checksum(task_key) {
        return Err(ManualTransitionJobError::Corrupt("task record task_key is not a sha256 checksum"));
    }
    Ok(format!("{}/{}.json", manual_transition_task_object_prefix(job_id)?, task_key))
}

pub fn manual_transition_worker_result_object_name(job_id: Uuid, task_key: &str) -> Result<String, ManualTransitionJobError> {
    if !is_sha256_checksum(task_key) {
        return Err(ManualTransitionJobError::Corrupt("worker result task_key is not a sha256 checksum"));
    }
    Ok(format!("{}/{}.json", manual_transition_worker_result_object_prefix(job_id)?, task_key))
}

fn manual_transition_worker_result_task_key_from_object_name(
    job_id: Uuid,
    object_name: &str,
) -> Result<String, ManualTransitionJobError> {
    let prefix = manual_transition_worker_result_object_prefix(job_id)?;
    let rest = object_name
        .strip_prefix(&prefix)
        .ok_or(ManualTransitionJobError::Corrupt("worker result object prefix is invalid"))?
        .strip_prefix('/')
        .ok_or(ManualTransitionJobError::Corrupt("worker result object path is invalid"))?;
    let task_key = rest
        .strip_suffix(".json")
        .ok_or(ManualTransitionJobError::Corrupt("worker result suffix is invalid"))?;
    if task_key.contains('/') || !is_sha256_checksum(task_key) {
        return Err(ManualTransitionJobError::Corrupt("worker result task_key is invalid"));
    }
    Ok(task_key.to_string())
}

fn manual_transition_task_key_from_object_name(job_id: Uuid, object_name: &str) -> Result<String, ManualTransitionJobError> {
    let prefix = manual_transition_task_object_prefix(job_id)?;
    let rest = object_name
        .strip_prefix(&prefix)
        .ok_or(ManualTransitionJobError::Corrupt("task record object prefix is invalid"))?
        .strip_prefix('/')
        .ok_or(ManualTransitionJobError::Corrupt("task record object path is invalid"))?;
    let task_key = rest
        .strip_suffix(".json")
        .ok_or(ManualTransitionJobError::Corrupt("task record suffix is invalid"))?;
    if task_key.contains('/') || !is_sha256_checksum(task_key) {
        return Err(ManualTransitionJobError::Corrupt("task record task_key is invalid"));
    }
    Ok(task_key.to_string())
}

pub fn manual_transition_job_id_from_record_object_name(object_name: &str) -> Result<Uuid, ManualTransitionJobError> {
    let Some(rest) = object_name.strip_prefix(MANUAL_TRANSITION_JOB_RECORD_PREFIX) else {
        return Err(ManualTransitionJobError::Corrupt("job record object prefix is invalid"));
    };
    let rest = rest
        .strip_prefix('/')
        .ok_or(ManualTransitionJobError::Corrupt("job record object path is invalid"))?;
    let mut parts = rest.split('/');
    let first = parts
        .next()
        .ok_or(ManualTransitionJobError::Corrupt("job record first shard is missing"))?;
    let second = parts
        .next()
        .ok_or(ManualTransitionJobError::Corrupt("job record second shard is missing"))?;
    let file = parts
        .next()
        .ok_or(ManualTransitionJobError::Corrupt("job record file is missing"))?;
    if parts.next().is_some() {
        return Err(ManualTransitionJobError::Corrupt("job record object path has extra components"));
    }
    let job_key = file
        .strip_suffix(".json")
        .ok_or(ManualTransitionJobError::Corrupt("job record suffix is invalid"))?;
    if job_key.len() != 32 || first.len() != 2 || second.len() != 2 || first != &job_key[..2] || second != &job_key[2..4] {
        return Err(ManualTransitionJobError::Corrupt("job record shards do not match job id"));
    }
    Uuid::parse_str(job_key).map_err(|_| ManualTransitionJobError::Corrupt("job record job id is invalid"))
}

pub fn manual_transition_scope_record_object_name(scope_key: &str) -> Result<String, ManualTransitionJobError> {
    if scope_key.len() != 64
        || !scope_key
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(ManualTransitionJobError::Corrupt("scope_key is not a lowercase sha256 hex digest"));
    }
    Ok(format!(
        "{}/{}/{}/{}.json",
        MANUAL_TRANSITION_SCOPE_RECORD_PREFIX,
        &scope_key[..2],
        &scope_key[2..4],
        scope_key
    ))
}

pub async fn save_manual_transition_job_record(api: Arc<ECStore>, job: &ManualTransitionJobRecord) -> EcstoreResult<()> {
    let object = manual_transition_job_record_object_name(job.job_id).map_err(manual_transition_job_store_error)?;
    let data = job.encode().map_err(manual_transition_job_store_error)?;
    config_boundary::save_config(api.clone(), &object, data.clone()).await?;
    api.record_durable_ilm_decommission_progress(&object, &data).await
}

pub async fn load_manual_transition_job_record(api: Arc<ECStore>, job_id: Uuid) -> EcstoreResult<ManualTransitionJobRecord> {
    let (record, _) = load_manual_transition_job_record_with_etag(api, job_id).await?;
    Ok(record)
}

pub async fn load_manual_transition_job_record_with_etag(
    api: Arc<ECStore>,
    job_id: Uuid,
) -> EcstoreResult<(ManualTransitionJobRecord, String)> {
    let object = manual_transition_job_record_object_name(job_id).map_err(manual_transition_job_store_error)?;
    let (data, object_info) = config_boundary::read_config_with_metadata(api, &object, &ObjectOptions::default()).await?;
    let etag = object_info
        .etag
        .filter(|etag| !etag.trim().is_empty())
        .ok_or_else(|| Error::other("manual transition job record is missing an ETag"))?;
    let record = ManualTransitionJobRecord::decode(job_id, &data).map_err(manual_transition_job_store_error)?;
    Ok((record, etag))
}

pub async fn save_manual_transition_job_record_if_current(
    api: Arc<ECStore>,
    job: &ManualTransitionJobRecord,
    current_etag: &str,
) -> EcstoreResult<()> {
    if current_etag.trim().is_empty() {
        return Err(Error::other("manual transition job current ETag is empty"));
    }
    let object = manual_transition_job_record_object_name(job.job_id).map_err(manual_transition_job_store_error)?;
    let data = job.encode().map_err(manual_transition_job_store_error)?;
    config_boundary::save_config_with_opts_quiet(
        api.clone(),
        &object,
        data.clone(),
        &ObjectOptions {
            max_parity: true,
            http_preconditions: Some(HTTPPreconditions {
                if_match: Some(current_etag.to_string()),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await?;
    api.record_durable_ilm_decommission_progress(&object, &data).await
}

/// Applies a job-record mutation with optimistic concurrency control.
///
/// The mutation returns whether the record needs to be persisted. When a lease
/// is supplied, ownership is checked again after every conflicting write.
pub async fn update_manual_transition_job_record<F>(
    api: Arc<ECStore>,
    job_id: Uuid,
    expected_lease_id: Option<Uuid>,
    update: F,
) -> EcstoreResult<ManualTransitionJobRecord>
where
    F: FnMut(&mut ManualTransitionJobRecord) -> bool,
{
    update_manual_transition_job_record_from(api, job_id, expected_lease_id, None, update).await
}

async fn update_manual_transition_job_record_from<F>(
    api: Arc<ECStore>,
    job_id: Uuid,
    expected_lease_id: Option<Uuid>,
    mut current: Option<(ManualTransitionJobRecord, String)>,
    mut update: F,
) -> EcstoreResult<ManualTransitionJobRecord>
where
    F: FnMut(&mut ManualTransitionJobRecord) -> bool,
{
    for _ in 0..MANUAL_TRANSITION_JOB_CAS_RETRIES {
        let (mut record, etag) = match current.take() {
            Some(current) => current,
            None => load_manual_transition_job_record_with_etag(api.clone(), job_id).await?,
        };
        if expected_lease_id.is_some_and(|lease_id| record.lease_id != lease_id) {
            return Err(Error::PreconditionFailed);
        }
        if !update(&mut record) {
            return Ok(record);
        }
        #[cfg(test)]
        pause_manual_transition_job_before_first_cas(job_id).await;
        match save_manual_transition_job_record_if_current(api.clone(), &record, &etag).await {
            Ok(()) => return Ok(record),
            Err(Error::PreconditionFailed) => continue,
            Err(err) => return Err(err),
        }
    }
    Err(Error::PreconditionFailed)
}

pub(crate) async fn save_manual_transition_worker_result_if_absent(
    api: Arc<ECStore>,
    record: &ManualTransitionWorkerResultRecord,
) -> EcstoreResult<bool> {
    let object = manual_transition_worker_result_object_name(record.job_id, &record.task_key)
        .map_err(manual_transition_job_store_error)?;
    let data = record.encode().map_err(manual_transition_job_store_error)?;
    match config_boundary::save_config_with_opts(
        api,
        &object,
        data,
        &ObjectOptions {
            max_parity: true,
            http_preconditions: Some(HTTPPreconditions {
                if_none_match: Some("*".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await
    {
        Ok(()) => Ok(true),
        Err(Error::PreconditionFailed) => Ok(false),
        Err(err) => Err(err),
    }
}

pub(crate) async fn save_manual_transition_task_if_absent(
    api: Arc<ECStore>,
    record: &ManualTransitionTaskRecord,
) -> EcstoreResult<bool> {
    let object =
        manual_transition_task_object_name(record.job_id, &record.task_key).map_err(manual_transition_job_store_error)?;
    let data = record.encode().map_err(manual_transition_job_store_error)?;
    match config_boundary::save_config_with_opts(
        api,
        &object,
        data,
        &ObjectOptions {
            max_parity: true,
            http_preconditions: Some(HTTPPreconditions {
                if_none_match: Some("*".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await
    {
        Ok(()) => Ok(true),
        Err(Error::PreconditionFailed) => Ok(false),
        Err(err) => Err(err),
    }
}

#[allow(
    dead_code,
    reason = "MinIO-parity tier/lifecycle entry point that this port never wired (backlog#1823)"
)]
pub async fn load_manual_transition_task_record(
    api: Arc<ECStore>,
    job_id: Uuid,
    task_key: &str,
) -> EcstoreResult<ManualTransitionTaskRecord> {
    let object = manual_transition_task_object_name(job_id, task_key).map_err(manual_transition_job_store_error)?;
    let data = config_boundary::read_config(api, &object).await?;
    ManualTransitionTaskRecord::decode(job_id, task_key, &data).map_err(manual_transition_job_store_error)
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct ManualTransitionTaskJournalStats {
    queued: u64,
}

enum ManualTransitionTaskJournal {
    Stats(ManualTransitionTaskJournalStats),
    Corrupt(String),
}

async fn scan_manual_transition_task_journal(api: Arc<ECStore>, job_id: Uuid) -> EcstoreResult<ManualTransitionTaskJournal> {
    let prefix = manual_transition_task_object_prefix(job_id).map_err(manual_transition_job_store_error)?;
    let mut marker = None;
    let mut stats = ManualTransitionTaskJournalStats::default();
    loop {
        let page = api
            .clone()
            .list_objects_v2(
                RUSTFS_META_BUCKET,
                &prefix,
                marker,
                None,
                MANUAL_TRANSITION_TASK_SCAN_LIMIT,
                false,
                None,
                false,
            )
            .await?;
        for object in page.objects {
            let task_key = match manual_transition_task_key_from_object_name(job_id, &object.name) {
                Ok(task_key) => task_key,
                Err(err) => return Ok(ManualTransitionTaskJournal::Corrupt(err.to_string())),
            };
            let object_name = manual_transition_task_object_name(job_id, &task_key).map_err(manual_transition_job_store_error)?;
            let data = match config_boundary::read_config(api.clone(), &object_name).await {
                Ok(data) => data,
                Err(err) => return Err(err),
            };
            if let Err(err) = ManualTransitionTaskRecord::decode(job_id, &task_key, &data) {
                return Ok(ManualTransitionTaskJournal::Corrupt(err.to_string()));
            }
            stats.queued = stats.queued.saturating_add(1);
        }
        if !page.is_truncated {
            return Ok(ManualTransitionTaskJournal::Stats(stats));
        }
        let Some(next_marker) = page.next_continuation_token else {
            return Err(Error::other("manual transition task journal page is truncated without a next marker"));
        };
        marker = Some(next_marker);
    }
}

#[allow(
    dead_code,
    reason = "MinIO-parity tier/lifecycle entry point that this port never wired (backlog#1823)"
)]
pub async fn load_manual_transition_worker_result_stats(
    api: Arc<ECStore>,
    job_id: Uuid,
) -> EcstoreResult<ManualTransitionWorkerResultStats> {
    match scan_manual_transition_worker_result_journal(api, job_id).await? {
        ManualTransitionWorkerResultJournal::Stats(stats) => Ok(stats.stats),
        ManualTransitionWorkerResultJournal::Corrupt(error) => Err(Error::other(error)),
    }
}

enum ManualTransitionWorkerResultJournal {
    Stats(ManualTransitionWorkerResultJournalStats),
    Corrupt(String),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ManualTransitionWorkerResultJournalStats {
    stats: ManualTransitionWorkerResultStats,
    task_keys: BTreeSet<String>,
}

impl ManualTransitionWorkerResultJournalStats {
    fn record(&mut self, result: ManualTransitionWorkerResultRecord) {
        self.task_keys.insert(result.task_key.clone());
        self.stats.record(result.result, result.failure_reason);
    }
}

pub async fn load_manual_transition_pending_task_records(
    api: Arc<ECStore>,
    job_id: Uuid,
    limit: usize,
) -> EcstoreResult<Vec<ManualTransitionTaskRecord>> {
    if limit == 0 {
        return Ok(Vec::new());
    }

    let result_keys = match scan_manual_transition_worker_result_journal(api.clone(), job_id).await? {
        ManualTransitionWorkerResultJournal::Stats(stats) => stats.task_keys,
        ManualTransitionWorkerResultJournal::Corrupt(error) => return Err(Error::other(error)),
    };
    let prefix = manual_transition_task_object_prefix(job_id).map_err(manual_transition_job_store_error)?;
    let mut marker = None;
    let scan_limit = usize::try_from(MANUAL_TRANSITION_TASK_SCAN_LIMIT)
        .map_err(|_| Error::other("manual transition task scan limit is invalid"))?;
    let mut pending = Vec::with_capacity(limit.min(scan_limit));

    loop {
        let page = api
            .clone()
            .list_objects_v2(
                RUSTFS_META_BUCKET,
                &prefix,
                marker,
                None,
                MANUAL_TRANSITION_TASK_SCAN_LIMIT,
                false,
                None,
                false,
            )
            .await?;

        for object in page.objects {
            let task_key =
                manual_transition_task_key_from_object_name(job_id, &object.name).map_err(manual_transition_job_store_error)?;
            if result_keys.contains(&task_key) {
                continue;
            }
            let object_name = manual_transition_task_object_name(job_id, &task_key).map_err(manual_transition_job_store_error)?;
            let data = config_boundary::read_config(api.clone(), &object_name).await?;
            let task = ManualTransitionTaskRecord::decode(job_id, &task_key, &data).map_err(manual_transition_job_store_error)?;
            pending.push(task);
            if pending.len() == limit {
                return Ok(pending);
            }
        }

        if !page.is_truncated {
            return Ok(pending);
        }
        let Some(next_marker) = page.next_continuation_token else {
            return Err(Error::other("manual transition task journal page is truncated without a next marker"));
        };
        marker = Some(next_marker);
    }
}

async fn scan_manual_transition_worker_result_journal(
    api: Arc<ECStore>,
    job_id: Uuid,
) -> EcstoreResult<ManualTransitionWorkerResultJournal> {
    let prefix = manual_transition_worker_result_object_prefix(job_id).map_err(manual_transition_job_store_error)?;
    let mut marker = None;
    let mut stats = ManualTransitionWorkerResultJournalStats::default();
    loop {
        let page = api
            .clone()
            .list_objects_v2(
                RUSTFS_META_BUCKET,
                &prefix,
                marker,
                None,
                MANUAL_TRANSITION_WORKER_RESULT_SCAN_LIMIT,
                false,
                None,
                false,
            )
            .await?;
        for object in page.objects {
            let task_key = match manual_transition_worker_result_task_key_from_object_name(job_id, &object.name) {
                Ok(task_key) => task_key,
                Err(err) => return Ok(ManualTransitionWorkerResultJournal::Corrupt(err.to_string())),
            };
            let object_name =
                manual_transition_worker_result_object_name(job_id, &task_key).map_err(manual_transition_job_store_error)?;
            let data = match config_boundary::read_config(api.clone(), &object_name).await {
                Ok(data) => data,
                Err(err) => return Err(err),
            };
            let result = match ManualTransitionWorkerResultRecord::decode(job_id, &task_key, &data) {
                Ok(result) => result,
                Err(err) => return Ok(ManualTransitionWorkerResultJournal::Corrupt(err.to_string())),
            };
            stats.record(result);
        }
        if !page.is_truncated {
            return Ok(ManualTransitionWorkerResultJournal::Stats(stats));
        }
        let Some(next_marker) = page.next_continuation_token else {
            return Err(Error::other("manual transition worker result page is truncated without a next marker"));
        };
        marker = Some(next_marker);
    }
}

#[allow(
    dead_code,
    reason = "MinIO-parity tier/lifecycle entry point that this port never wired (backlog#1823)"
)]
pub async fn reconcile_manual_transition_worker_results(
    api: Arc<ECStore>,
    job_id: Uuid,
    queue_snapshot: ManualTransitionQueueSnapshot,
) -> EcstoreResult<ManualTransitionJobRecord> {
    reconcile_manual_transition_worker_results_inner(api, job_id, None, queue_snapshot, false).await
}

pub(crate) async fn reconcile_manual_transition_worker_results_if_owned(
    api: Arc<ECStore>,
    job_id: Uuid,
    expected_lease_id: Uuid,
    queue_snapshot: ManualTransitionQueueSnapshot,
) -> EcstoreResult<ManualTransitionJobRecord> {
    reconcile_manual_transition_worker_results_inner(api, job_id, Some(expected_lease_id), queue_snapshot, false).await
}

async fn reconcile_manual_transition_worker_results_inner(
    api: Arc<ECStore>,
    job_id: Uuid,
    expected_lease_id: Option<Uuid>,
    queue_snapshot: ManualTransitionQueueSnapshot,
    mark_missing_results_unknown: bool,
) -> EcstoreResult<ManualTransitionJobRecord> {
    let task_stats = match scan_manual_transition_task_journal(api.clone(), job_id).await? {
        ManualTransitionTaskJournal::Stats(stats) => stats,
        ManualTransitionTaskJournal::Corrupt(error) => {
            return mark_manual_transition_job_unknown_for_task_journal_error(
                api,
                job_id,
                expected_lease_id,
                error,
                queue_snapshot,
            )
            .await;
        }
    };
    let stats = match scan_manual_transition_worker_result_journal(api.clone(), job_id).await? {
        ManualTransitionWorkerResultJournal::Stats(stats) => stats,
        ManualTransitionWorkerResultJournal::Corrupt(error) => {
            return mark_manual_transition_job_unknown_for_worker_result_journal_error(
                api,
                job_id,
                expected_lease_id,
                error,
                queue_snapshot,
            )
            .await;
        }
    };
    let mut changed = false;
    let record = update_manual_transition_job_record(api.clone(), job_id, expected_lease_id, |record| {
        let counts_changed = record.apply_worker_result_counts(
            stats.stats.completed,
            stats.stats.failed,
            &stats.stats.tier_failure_by_reason,
            task_stats.queued,
            queue_snapshot,
        );
        let became_unknown = mark_missing_results_unknown && record.mark_unknown_if_worker_results_lost(queue_snapshot);
        changed = counts_changed || became_unknown;
        changed
    })
    .await?;
    if !changed {
        return Ok(record);
    }
    if record.is_terminal() {
        delete_manual_transition_scope_admission_if_current(api, &record.scope_key, record.job_id, record.lease_id).await?;
    } else {
        renew_manual_transition_scope_admission_from_job(api, &record).await?;
    }
    Ok(record)
}

async fn mark_manual_transition_job_unknown_for_task_journal_error(
    api: Arc<ECStore>,
    job_id: Uuid,
    expected_lease_id: Option<Uuid>,
    error: String,
    queue_snapshot: ManualTransitionQueueSnapshot,
) -> EcstoreResult<ManualTransitionJobRecord> {
    let mut changed = false;
    let record = update_manual_transition_job_record(api.clone(), job_id, expected_lease_id, |record| {
        changed = record.mark_unknown_for_task_journal_error(error.clone(), queue_snapshot);
        changed
    })
    .await?;
    if changed && record.is_terminal() {
        delete_manual_transition_scope_admission_if_current(api, &record.scope_key, record.job_id, record.lease_id).await?;
    }
    Ok(record)
}

async fn mark_manual_transition_job_unknown_for_worker_result_journal_error(
    api: Arc<ECStore>,
    job_id: Uuid,
    expected_lease_id: Option<Uuid>,
    error: String,
    queue_snapshot: ManualTransitionQueueSnapshot,
) -> EcstoreResult<ManualTransitionJobRecord> {
    let mut changed = false;
    let record = update_manual_transition_job_record(api.clone(), job_id, expected_lease_id, |record| {
        changed = record.mark_unknown_for_worker_result_journal_error(error.clone(), queue_snapshot);
        changed
    })
    .await?;
    if changed && record.is_terminal() {
        delete_manual_transition_scope_admission_if_current(api, &record.scope_key, record.job_id, record.lease_id).await?;
    }
    Ok(record)
}

pub async fn save_manual_transition_scope_admission_if_absent(
    api: Arc<ECStore>,
    admission: &ManualTransitionScopeAdmission,
) -> EcstoreResult<()> {
    admission.validate().map_err(manual_transition_job_store_error)?;
    let object = manual_transition_scope_record_object_name(&admission.scope_key).map_err(manual_transition_job_store_error)?;
    let data = serde_json::to_vec(admission).map_err(Error::other)?;
    config_boundary::save_config_with_opts(
        api.clone(),
        &object,
        data.clone(),
        &ObjectOptions {
            max_parity: true,
            http_preconditions: Some(HTTPPreconditions {
                if_none_match: Some("*".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await?;
    api.record_durable_ilm_decommission_progress(&object, &data).await
}

pub async fn load_manual_transition_scope_admission(
    api: Arc<ECStore>,
    scope_key: &str,
) -> EcstoreResult<ManualTransitionScopeAdmission> {
    let (admission, _) = load_manual_transition_scope_admission_with_etag(api, scope_key).await?;
    Ok(admission)
}

pub async fn load_manual_transition_scope_admission_with_etag(
    api: Arc<ECStore>,
    scope_key: &str,
) -> EcstoreResult<(ManualTransitionScopeAdmission, String)> {
    let object = manual_transition_scope_record_object_name(scope_key).map_err(manual_transition_job_store_error)?;
    let (data, object_info) = config_boundary::read_config_with_metadata(api, &object, &ObjectOptions::default()).await?;
    let etag = object_info
        .etag
        .filter(|etag| !etag.trim().is_empty())
        .ok_or_else(|| Error::other("manual transition scope admission is missing an ETag"))?;
    let admission: ManualTransitionScopeAdmission = serde_json::from_slice(&data).map_err(Error::other)?;
    admission.validate().map_err(manual_transition_job_store_error)?;
    Ok((admission, etag))
}

pub async fn save_manual_transition_scope_admission_if_current(
    api: Arc<ECStore>,
    admission: &ManualTransitionScopeAdmission,
    current_etag: &str,
) -> EcstoreResult<()> {
    if current_etag.trim().is_empty() {
        return Err(Error::other("manual transition scope admission current ETag is empty"));
    }
    admission.validate().map_err(manual_transition_job_store_error)?;
    let object = manual_transition_scope_record_object_name(&admission.scope_key).map_err(manual_transition_job_store_error)?;
    let data = serde_json::to_vec(admission).map_err(Error::other)?;
    match config_boundary::save_config_with_opts(
        api.clone(),
        &object,
        data.clone(),
        &ObjectOptions {
            max_parity: true,
            http_preconditions: Some(HTTPPreconditions {
                if_match: Some(current_etag.to_string()),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await
    {
        Err(Error::ObjectNotFound(bucket, current_object)) if bucket == RUSTFS_META_BUCKET && current_object == object => {
            Err(Error::PreconditionFailed)
        }
        result => result,
    }?;
    api.record_durable_ilm_decommission_progress(&object, &data).await
}

pub async fn claim_manual_transition_scope_admission(
    api: Arc<ECStore>,
    admission: &ManualTransitionScopeAdmission,
) -> EcstoreResult<ManualTransitionScopeAdmissionClaim> {
    loop {
        match save_manual_transition_scope_admission_if_absent(api.clone(), admission).await {
            Ok(()) => return finish_manual_transition_scope_admission_claim(api, admission).await,
            Err(Error::PreconditionFailed) => {}
            Err(err) => return Err(err),
        }

        let (active, etag) = match load_manual_transition_scope_admission_with_etag(api.clone(), &admission.scope_key).await {
            Ok(active) => active,
            Err(Error::ConfigNotFound) => continue,
            Err(err) => return Err(err),
        };
        let scope_lease_expired = manual_transition_scope_admission_lease_expired(&active);
        let active_job_reclaimable = if active.job_id == admission.job_id {
            scope_lease_expired
        } else {
            match load_manual_transition_job_record(api.clone(), active.job_id).await {
                Ok(active_job) => {
                    active_job.is_terminal() || (scope_lease_expired && manual_transition_job_lease_expired(&active_job))
                }
                // Missing active job metadata can be transient (for example, immediately after admission creation);
                // require an expired scope lease before treating it as reclaimable.
                Err(Error::ConfigNotFound) => scope_lease_expired,
                Err(err) => return Err(err),
            }
        };
        if active_job_reclaimable {
            match save_manual_transition_scope_admission_if_current(api.clone(), admission, &etag).await {
                Ok(()) => return finish_manual_transition_scope_admission_claim(api, admission).await,
                Err(Error::PreconditionFailed) => continue,
                Err(err) => return Err(err),
            }
        }

        return Ok(ManualTransitionScopeAdmissionClaim::Conflict(Box::new(active)));
    }
}

async fn finish_manual_transition_scope_admission_claim(
    api: Arc<ECStore>,
    admission: &ManualTransitionScopeAdmission,
) -> EcstoreResult<ManualTransitionScopeAdmissionClaim> {
    if let Some(active) = find_active_legacy_manual_transition_scope_conflict(api.clone(), admission).await? {
        delete_manual_transition_scope_admission_if_current(api, &admission.scope_key, admission.job_id, admission.lease_id)
            .await?;
        return Ok(ManualTransitionScopeAdmissionClaim::Conflict(Box::new(active)));
    }
    Ok(ManualTransitionScopeAdmissionClaim::Claimed)
}

async fn find_active_legacy_manual_transition_scope_conflict(
    api: Arc<ECStore>,
    admission: &ManualTransitionScopeAdmission,
) -> EcstoreResult<Option<ManualTransitionScopeAdmission>> {
    let mut marker = None;
    loop {
        let page = api
            .clone()
            .list_objects_v2(
                RUSTFS_META_BUCKET,
                MANUAL_TRANSITION_JOB_RECORD_PREFIX,
                marker,
                None,
                MANUAL_TRANSITION_LEGACY_SCOPE_SCAN_LIMIT,
                false,
                None,
                false,
            )
            .await?;
        for object in page.objects {
            let job_id =
                manual_transition_job_id_from_record_object_name(&object.name).map_err(manual_transition_job_store_error)?;
            if job_id == admission.job_id {
                continue;
            }
            let record = match load_manual_transition_job_record(api.clone(), job_id).await {
                Ok(record) => record,
                Err(Error::ConfigNotFound) => continue,
                Err(err) => return Err(err),
            };
            if record.scope_key == admission.scope_key {
                continue;
            }
            let legacy_scope_key = legacy_manual_transition_scope_key(
                &record.bucket,
                &ManualTransitionRunOptions {
                    prefix: record.prefix.clone(),
                    tier: record.tier.clone(),
                    dry_run: record.dry_run,
                    ..Default::default()
                },
            );
            if record.scope_key != legacy_scope_key {
                continue;
            }
            if record.bucket == admission.bucket
                && record.dry_run == admission.dry_run
                && !record.is_terminal()
                && !manual_transition_job_lease_expired(&record)
            {
                return Ok(Some(ManualTransitionScopeAdmission::from_job(&record)));
            }
        }
        if !page.is_truncated {
            return Ok(None);
        }
        marker = page.next_continuation_token;
    }
}

pub async fn request_manual_transition_job_cancel(api: Arc<ECStore>, job_id: Uuid) -> EcstoreResult<ManualTransitionJobRecord> {
    update_manual_transition_job_record(api, job_id, None, |record| {
        if record.is_terminal() || record.cancel_requested {
            return false;
        }
        record.mark_cancel_requested();
        true
    })
    .await
}

pub async fn persist_manual_transition_job_progress(
    api: Arc<ECStore>,
    job_id: Uuid,
    report: &ManualTransitionRunReport,
    queue_snapshot: ManualTransitionQueueSnapshot,
) -> EcstoreResult<ManualTransitionJobRecord> {
    let current = load_manual_transition_job_record_with_etag(api.clone(), job_id).await?;
    persist_manual_transition_job_progress_inner(api, job_id, current.0.lease_id, Some(current), report, queue_snapshot).await
}

pub async fn persist_manual_transition_job_progress_if_owned(
    api: Arc<ECStore>,
    job_id: Uuid,
    expected_lease_id: Uuid,
    report: &ManualTransitionRunReport,
    queue_snapshot: ManualTransitionQueueSnapshot,
) -> EcstoreResult<ManualTransitionJobRecord> {
    persist_manual_transition_job_progress_inner(api, job_id, expected_lease_id, None, report, queue_snapshot).await
}

async fn persist_manual_transition_job_progress_inner(
    api: Arc<ECStore>,
    job_id: Uuid,
    expected_lease_id: Uuid,
    current: Option<(ManualTransitionJobRecord, String)>,
    report: &ManualTransitionRunReport,
    queue_snapshot: ManualTransitionQueueSnapshot,
) -> EcstoreResult<ManualTransitionJobRecord> {
    let record = update_manual_transition_job_record_from(api.clone(), job_id, Some(expected_lease_id), current, |record| {
        if record.state != ManualTransitionJobState::Running {
            return false;
        }
        record.update_running_progress(report.clone(), queue_snapshot);
        true
    })
    .await?;
    if record.state == ManualTransitionJobState::Running {
        renew_manual_transition_scope_admission_from_job(api, &record).await?;
    }
    Ok(record)
}

pub async fn record_manual_transition_worker_result(
    api: Arc<ECStore>,
    job_id: Uuid,
    task_key: &str,
    result: ManualTransitionWorkerResult,
    queue_snapshot: ManualTransitionQueueSnapshot,
) -> EcstoreResult<ManualTransitionJobRecord> {
    record_manual_transition_worker_result_with_reason(api, job_id, task_key, result, queue_snapshot, None).await
}

pub async fn record_manual_transition_worker_result_with_reason(
    api: Arc<ECStore>,
    job_id: Uuid,
    task_key: &str,
    result: ManualTransitionWorkerResult,
    _queue_snapshot: ManualTransitionQueueSnapshot,
    failure_reason: Option<ManualTransitionWorkerFailureReason>,
) -> EcstoreResult<ManualTransitionJobRecord> {
    let result_record = ManualTransitionWorkerResultRecord::new_with_reason(job_id, task_key, result, failure_reason);
    if !save_manual_transition_worker_result_if_absent(api.clone(), &result_record).await? {
        return load_manual_transition_job_record(api, job_id).await;
    }
    load_manual_transition_job_record(api, job_id).await
}

pub async fn renew_manual_transition_job_lease(
    api: Arc<ECStore>,
    job_id: Uuid,
    queue_snapshot: ManualTransitionQueueSnapshot,
) -> EcstoreResult<ManualTransitionJobRecord> {
    let current = load_manual_transition_job_record_with_etag(api.clone(), job_id).await?;
    renew_manual_transition_job_lease_inner(api, job_id, current.0.lease_id, Some(current), queue_snapshot).await
}

pub async fn renew_manual_transition_job_lease_if_owned(
    api: Arc<ECStore>,
    job_id: Uuid,
    expected_lease_id: Uuid,
    queue_snapshot: ManualTransitionQueueSnapshot,
) -> EcstoreResult<ManualTransitionJobRecord> {
    renew_manual_transition_job_lease_inner(api, job_id, expected_lease_id, None, queue_snapshot).await
}

async fn renew_manual_transition_job_lease_inner(
    api: Arc<ECStore>,
    job_id: Uuid,
    expected_lease_id: Uuid,
    current: Option<(ManualTransitionJobRecord, String)>,
    queue_snapshot: ManualTransitionQueueSnapshot,
) -> EcstoreResult<ManualTransitionJobRecord> {
    let (current, current_etag) = match current {
        Some(current) => current,
        None => load_manual_transition_job_record_with_etag(api.clone(), job_id).await?,
    };
    if current.lease_id != expected_lease_id {
        return Err(Error::PreconditionFailed);
    }
    if current.state != ManualTransitionJobState::Running {
        return Ok(current);
    }
    if current.scan_completed && queue_snapshot.queued == 0 && queue_snapshot.active == 0 {
        return reconcile_manual_transition_worker_results_inner(api, job_id, Some(expected_lease_id), queue_snapshot, true)
            .await;
    }
    let record = update_manual_transition_job_record_from(
        api.clone(),
        job_id,
        Some(expected_lease_id),
        Some((current, current_etag)),
        |record| {
            if record.state != ManualTransitionJobState::Running {
                return false;
            }
            record.renew_lease(queue_snapshot);
            true
        },
    )
    .await?;
    if record.is_terminal() {
        delete_manual_transition_scope_admission_if_current(api, &record.scope_key, record.job_id, record.lease_id).await?;
    } else if record.state == ManualTransitionJobState::Running {
        renew_manual_transition_scope_admission_from_job(api, &record).await?;
    }
    Ok(record)
}

async fn renew_manual_transition_scope_admission_from_job(
    api: Arc<ECStore>,
    record: &ManualTransitionJobRecord,
) -> EcstoreResult<()> {
    for _ in 0..MANUAL_TRANSITION_JOB_CAS_RETRIES {
        let (admission, admission_etag) =
            match load_manual_transition_scope_admission_with_etag(api.clone(), &record.scope_key).await {
                Ok(admission) => admission,
                Err(Error::ConfigNotFound) => return Ok(()),
                Err(err) => return Err(err),
            };
        if admission.job_id != record.job_id || admission.lease_id != record.lease_id {
            return Err(Error::PreconditionFailed);
        }
        let mut renewed_admission = ManualTransitionScopeAdmission::from_job(record);
        renewed_admission.lease_expires_at_unix_nanos = renewed_admission
            .lease_expires_at_unix_nanos
            .max(admission.lease_expires_at_unix_nanos);
        renewed_admission.updated_at_unix_nanos = renewed_admission.updated_at_unix_nanos.max(admission.updated_at_unix_nanos);
        if renewed_admission == admission {
            return Ok(());
        }
        match save_manual_transition_scope_admission_if_current(api.clone(), &renewed_admission, &admission_etag).await {
            Ok(()) => return Ok(()),
            Err(Error::PreconditionFailed) => continue,
            Err(err) => return Err(err),
        }
    }
    Err(Error::PreconditionFailed)
}

pub async fn delete_manual_transition_scope_admission_if_current(
    api: Arc<ECStore>,
    scope_key: &str,
    job_id: Uuid,
    lease_id: Uuid,
) -> EcstoreResult<bool> {
    let (admission, etag) = match load_manual_transition_scope_admission_with_etag(api.clone(), scope_key).await {
        Ok((admission, etag)) if admission.job_id == job_id && admission.lease_id == lease_id => (admission, etag),
        Ok(_) => return Ok(false),
        Err(Error::ConfigNotFound) => return Ok(true),
        Err(err) => return Err(err),
    };
    let object = manual_transition_scope_record_object_name(scope_key).map_err(manual_transition_job_store_error)?;
    let data = serde_json::to_vec(&admission).map_err(Error::other)?;
    api.record_durable_ilm_decommission_terminal(&object, &data).await?;
    match config_boundary::delete_config_if_match(api, &object, &etag).await {
        Ok(()) | Err(Error::ConfigNotFound) => Ok(true),
        Err(Error::PreconditionFailed) => Ok(false),
        Err(err) => Err(err),
    }
}

fn manual_transition_job_store_error(err: ManualTransitionJobError) -> Error {
    Error::other(err)
}

fn manual_transition_cursor_revision(report: &ManualTransitionRunReport) -> Option<u64> {
    report.continuation_token.as_ref()?;
    (report.scanned > 0).then_some(report.scanned)
}

pub fn manual_transition_scope_admission_lease_expired(admission: &ManualTransitionScopeAdmission) -> bool {
    OffsetDateTime::now_utc().unix_timestamp_nanos() > admission.lease_expires_at_unix_nanos
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_OWNER: &str = "owner-a";

    #[test]
    fn manual_transition_job_record_rejects_nil_job_id() {
        let options = ManualTransitionRunOptions::default();
        let mut record = ManualTransitionJobRecord::new(Uuid::new_v4(), "bucket", &options, TEST_OWNER);
        record.job_id = Uuid::nil();

        let err = record.encode().expect_err("nil job id must fail closed");

        assert!(err.to_string().contains("job_id is nil"));
    }

    #[test]
    fn manual_transition_job_record_round_trips_with_checksum() {
        let options = ManualTransitionRunOptions {
            prefix: "logs/".to_string(),
            tier: Some("warm".to_string()),
            max_objects: Some(17),
            ..Default::default()
        };
        let mut record = ManualTransitionJobRecord::new(Uuid::new_v4(), "bucket", &options, TEST_OWNER);
        record.report.scanned = 3;
        record.report.continuation_token = Some("opaque".to_string());
        record.mark_cancel_requested();
        record.complete(
            ManualTransitionRunReport {
                bucket: "bucket".to_string(),
                prefix: "logs/".to_string(),
                tier: Some("warm".to_string()),
                cancelled: true,
                ..Default::default()
            },
            ManualTransitionQueueSnapshot::default(),
        );

        let encoded = record.encode().expect("job record should encode");
        let decoded = ManualTransitionJobRecord::decode(record.job_id, &encoded).expect("job record should decode");

        assert_eq!(decoded.job_id, record.job_id);
        assert_eq!(decoded.state, ManualTransitionJobState::Cancelled);
        assert!(decoded.cancel_requested);
        assert_eq!(decoded.max_objects, Some(17));
    }

    #[test]
    fn manual_transition_job_record_waits_for_worker_results() {
        let options = ManualTransitionRunOptions::default();
        let mut record = ManualTransitionJobRecord::new(Uuid::new_v4(), "bucket", &options, TEST_OWNER);

        record.complete(
            ManualTransitionRunReport {
                bucket: "bucket".to_string(),
                enqueued: 2,
                ..Default::default()
            },
            ManualTransitionQueueSnapshot::default(),
        );

        assert!(record.scan_completed);
        assert_eq!(record.state, ManualTransitionJobState::Running);
        assert!(record.completed_at_unix_nanos.is_none());

        record.record_worker_result(ManualTransitionWorkerResult::Completed, ManualTransitionQueueSnapshot::default());
        assert_eq!(record.state, ManualTransitionJobState::Running);
        assert_eq!(record.report.transition_completed, 1);

        record.record_worker_result(ManualTransitionWorkerResult::TierFailure, ManualTransitionQueueSnapshot::default());
        assert_eq!(record.state, ManualTransitionJobState::Partial);
        assert_eq!(record.report.transition_completed, 1);
        assert_eq!(record.report.transition_failed, 1);
        assert_eq!(record.report.tier_failure, 1);
        assert!(record.completed_at_unix_nanos.is_some());
    }

    #[test]
    fn manual_transition_job_record_marks_report_cancelled_after_worker_drain() {
        let options = ManualTransitionRunOptions::default();
        let mut record = ManualTransitionJobRecord::new(Uuid::new_v4(), "bucket", &options, TEST_OWNER);

        record.complete(
            ManualTransitionRunReport {
                bucket: "bucket".to_string(),
                enqueued: 1,
                ..Default::default()
            },
            ManualTransitionQueueSnapshot::default(),
        );
        record.mark_cancel_requested();
        record.record_worker_result(ManualTransitionWorkerResult::TierFailure, ManualTransitionQueueSnapshot::default());

        assert_eq!(record.state, ManualTransitionJobState::Cancelled);
        assert!(record.cancel_requested);
        assert!(record.report.cancelled);
        assert_eq!(record.report.transition_failed, 1);
        assert_eq!(record.report.tier_failure, 1);
    }

    #[test]
    fn manual_transition_job_record_records_worker_failure_reasons() {
        let options = ManualTransitionRunOptions::default();
        let mut record = ManualTransitionJobRecord::new(Uuid::new_v4(), "bucket", &options, TEST_OWNER);

        record.record_worker_result_with_reason(
            ManualTransitionWorkerResult::TierFailure,
            ManualTransitionQueueSnapshot::default(),
            Some(ManualTransitionWorkerFailureReason::NotFound),
        );
        record.record_worker_result_with_reason(
            ManualTransitionWorkerResult::TierFailure,
            ManualTransitionQueueSnapshot::default(),
            Some(ManualTransitionWorkerFailureReason::Network),
        );
        record.record_worker_result_with_reason(
            ManualTransitionWorkerResult::TierFailure,
            ManualTransitionQueueSnapshot::default(),
            None,
        );

        assert_eq!(record.report.transition_failed, 3);
        assert_eq!(record.report.tier_failure, 3);
        assert_eq!(
            record
                .report
                .tier_failure_by_reason
                .get(&ManualTransitionWorkerFailureReason::NotFound),
            Some(&1)
        );
        assert_eq!(
            record
                .report
                .tier_failure_by_reason
                .get(&ManualTransitionWorkerFailureReason::Network),
            Some(&1)
        );
        assert_eq!(
            record
                .report
                .tier_failure_by_reason
                .get(&ManualTransitionWorkerFailureReason::Unknown),
            Some(&1)
        );
    }

    #[test]
    fn manual_transition_job_cancel_recovery_finishes_after_worker_drain() {
        let options = ManualTransitionRunOptions::default();
        let mut record = ManualTransitionJobRecord::new(Uuid::new_v4(), "bucket", &options, TEST_OWNER);
        record.report.enqueued = 2;
        record.report.transition_completed = 2;
        record.mark_cancel_requested();

        record.cancel_after_recovery(ManualTransitionQueueSnapshot::default());

        assert_eq!(record.state, ManualTransitionJobState::Cancelled);
        assert!(record.report.cancelled);
        assert_eq!(record.report.enqueued, 2);
        assert_eq!(record.report.transition_completed, 2);
    }

    #[test]
    fn manual_transition_job_record_decode_normalizes_legacy_cancelled_report() {
        let options = ManualTransitionRunOptions::default();
        let mut record = ManualTransitionJobRecord::new(Uuid::new_v4(), "bucket", &options, TEST_OWNER);
        record.mark_cancel_requested();
        record.state = ManualTransitionJobState::Cancelled;
        record.completed_at_unix_nanos = Some(OffsetDateTime::now_utc().unix_timestamp_nanos());
        record.report.cancelled = false;
        let encoded = record.encode().expect("legacy-shaped cancelled job should encode");

        let decoded =
            ManualTransitionJobRecord::decode(record.job_id, &encoded).expect("legacy-shaped cancelled job should decode");

        assert_eq!(decoded.state, ManualTransitionJobState::Cancelled);
        assert!(decoded.cancel_requested);
        assert!(decoded.report.cancelled);
    }

    #[test]
    fn manual_transition_job_scan_progress_preserves_worker_counters() {
        let options = ManualTransitionRunOptions::default();
        let mut record = ManualTransitionJobRecord::new(Uuid::new_v4(), "bucket", &options, TEST_OWNER);
        record.record_worker_result(ManualTransitionWorkerResult::TierFailure, ManualTransitionQueueSnapshot::default());

        record.update_running_progress(
            ManualTransitionRunReport {
                bucket: "bucket".to_string(),
                scanned: 3,
                enqueued: 1,
                tier_failure: 2,
                ..Default::default()
            },
            ManualTransitionQueueSnapshot::default(),
        );

        assert_eq!(record.report.scanned, 3);
        assert_eq!(record.report.enqueued, 1);
        assert_eq!(record.report.transition_failed, 1);
        assert_eq!(record.report.tier_failure, 3);
    }

    #[test]
    fn manual_transition_job_scan_progress_preserves_worker_failure_reasons() {
        let options = ManualTransitionRunOptions::default();
        let mut record = ManualTransitionJobRecord::new(Uuid::new_v4(), "bucket", &options, TEST_OWNER);
        record.record_worker_result_with_reason(
            ManualTransitionWorkerResult::TierFailure,
            ManualTransitionQueueSnapshot::default(),
            Some(ManualTransitionWorkerFailureReason::NotFound),
        );

        record.update_running_progress(
            ManualTransitionRunReport {
                bucket: "bucket".to_string(),
                scanned: 3,
                enqueued: 1,
                tier_failure: 2,
                ..Default::default()
            },
            ManualTransitionQueueSnapshot::default(),
        );

        assert_eq!(
            record
                .report
                .tier_failure_by_reason
                .get(&ManualTransitionWorkerFailureReason::NotFound),
            Some(&1)
        );
    }

    #[test]
    fn manual_transition_job_scan_progress_accumulates_resumed_checkpoint_counters() {
        let options = ManualTransitionRunOptions::default();
        let mut record = ManualTransitionJobRecord::new(Uuid::new_v4(), "bucket", &options, TEST_OWNER);
        let first_token =
            encode_manual_transition_continuation_token(Some("logs/page-a".to_string()), Some("version-a".to_string()));
        record.update_running_progress(
            ManualTransitionRunReport {
                bucket: "bucket".to_string(),
                lifecycle_config_found: true,
                scanned: 1000,
                eligible: 800,
                enqueued: 50,
                dry_run_eligible: 10,
                skipped_not_transition: 2,
                skipped_tier: 3,
                skipped_delete_marker: 4,
                skipped_directory: 5,
                skipped_replication: 6,
                skipped_already_transitioned: 7,
                skipped_already_in_flight: 8,
                skipped_queue_full: 9,
                skipped_queue_closed: 10,
                skipped_queue_timeout: 11,
                tier_failure: 12,
                truncated_by_duration: true,
                continuation_token: first_token,
                ..Default::default()
            },
            ManualTransitionQueueSnapshot::default(),
        );

        let next_token =
            encode_manual_transition_continuation_token(Some("logs/page-b".to_string()), Some("version-b".to_string()));
        record.update_running_progress(
            ManualTransitionRunReport {
                bucket: "bucket".to_string(),
                scanned: 3,
                eligible: 2,
                enqueued: 1,
                dry_run_eligible: 1,
                skipped_not_transition: 1,
                tier_failure: 1,
                continuation_token: next_token.clone(),
                ..Default::default()
            },
            ManualTransitionQueueSnapshot::default(),
        );

        assert_eq!(record.report.scanned, 1003);
        assert_eq!(record.report.eligible, 802);
        assert_eq!(record.report.enqueued, 51);
        assert_eq!(record.report.dry_run_eligible, 11);
        assert_eq!(record.report.skipped_not_transition, 3);
        assert_eq!(record.report.skipped_tier, 3);
        assert_eq!(record.report.skipped_delete_marker, 4);
        assert_eq!(record.report.skipped_directory, 5);
        assert_eq!(record.report.skipped_replication, 6);
        assert_eq!(record.report.skipped_already_transitioned, 7);
        assert_eq!(record.report.skipped_already_in_flight, 8);
        assert_eq!(record.report.skipped_queue_full, 9);
        assert_eq!(record.report.skipped_queue_closed, 10);
        assert_eq!(record.report.skipped_queue_timeout, 11);
        assert_eq!(record.report.tier_failure, 13);
        assert!(record.report.lifecycle_config_found);
        assert!(record.report.truncated_by_duration);
        assert_eq!(record.report.continuation_token, next_token);
        assert_eq!(record.cursor_revision, Some(1003));
    }

    #[test]
    fn manual_transition_job_apply_worker_result_counts_preserves_existing_failure_reasons() {
        let options = ManualTransitionRunOptions::default();
        let mut record = ManualTransitionJobRecord::new(Uuid::new_v4(), "bucket", &options, TEST_OWNER);
        record.record_worker_result_with_reason(
            ManualTransitionWorkerResult::TierFailure,
            ManualTransitionQueueSnapshot::default(),
            Some(ManualTransitionWorkerFailureReason::NotFound),
        );

        let mut failure_reasons = BTreeMap::new();
        failure_reasons.insert(ManualTransitionWorkerFailureReason::PermissionDenied, 2);

        record.apply_worker_result_counts(0, 1, &failure_reasons, 1, ManualTransitionQueueSnapshot::default());

        assert_eq!(
            record
                .report
                .tier_failure_by_reason
                .get(&ManualTransitionWorkerFailureReason::NotFound),
            Some(&1)
        );
        assert_eq!(
            record
                .report
                .tier_failure_by_reason
                .get(&ManualTransitionWorkerFailureReason::PermissionDenied),
            Some(&2)
        );
    }

    #[test]
    fn manual_transition_job_unknown_checkpoint_persists_counters_and_cursor() {
        let options = ManualTransitionRunOptions {
            prefix: "logs/".to_string(),
            tier: Some("warm".to_string()),
            max_objects: Some(5),
            ..Default::default()
        };
        let mut record = ManualTransitionJobRecord::new(Uuid::new_v4(), "bucket", &options, TEST_OWNER);
        record.update_running_progress(
            ManualTransitionRunReport {
                bucket: "bucket".to_string(),
                prefix: "logs/".to_string(),
                tier: Some("warm".to_string()),
                scanned: 5,
                eligible: 4,
                enqueued: 3,
                skipped_queue_full: 2,
                skipped_queue_timeout: 1,
                tier_failure: 1,
                truncated_by_limit: true,
                continuation_token: Some("opaque-page-token".to_string()),
                ..Default::default()
            },
            ManualTransitionQueueSnapshot {
                queue_capacity: 8,
                queued: 3,
                active: 2,
                workers: 4,
                queue_full: 2,
                queue_send_timeout: 1,
                ..Default::default()
            },
        );
        record.record_worker_result(
            ManualTransitionWorkerResult::TierFailure,
            ManualTransitionQueueSnapshot {
                queue_capacity: 8,
                queued: 2,
                active: 1,
                workers: 4,
                queue_full: 2,
                queue_send_timeout: 1,
                ..Default::default()
            },
        );

        assert!(record.mark_unknown_if_recovery_would_skip_pending_page(ManualTransitionQueueSnapshot {
            queue_capacity: 8,
            workers: 4,
            queue_full: 2,
            queue_send_timeout: 1,
            ..Default::default()
        }));
        let encoded = record.encode().expect("unknown checkpoint should encode");
        let decoded = ManualTransitionJobRecord::decode(record.job_id, &encoded).expect("unknown checkpoint should decode");

        assert_eq!(decoded.state, ManualTransitionJobState::Unknown);
        assert!(decoded.is_terminal());
        assert!(decoded.completed_at_unix_nanos.is_some());
        assert_eq!(decoded.report.continuation_token.as_deref(), Some("opaque-page-token"));
        assert!(decoded.report.was_truncated());
        assert!(decoded.report.has_partial_enqueue());
        assert_eq!(decoded.report.scanned, 5);
        assert_eq!(decoded.report.eligible, 4);
        assert_eq!(decoded.report.enqueued, 3);
        assert_eq!(decoded.report.skipped_queue_full, 2);
        assert_eq!(decoded.report.skipped_queue_timeout, 1);
        assert_eq!(decoded.report.transition_failed, 1);
        assert_eq!(decoded.report.tier_failure, 2);
        assert_eq!(decoded.queue_snapshot.queue_capacity, 8);
        assert_eq!(decoded.queue_snapshot.workers, 4);
        assert_eq!(decoded.queue_snapshot.queue_full, 2);
        assert_eq!(decoded.queue_snapshot.queue_send_timeout, 1);
        assert!(
            decoded
                .error
                .as_deref()
                .is_some_and(|error| error.contains("page/task journal"))
        );
    }

    #[test]
    fn manual_transition_job_marks_unknown_when_worker_results_are_lost() {
        let options = ManualTransitionRunOptions::default();
        let mut record = ManualTransitionJobRecord::new(Uuid::new_v4(), "bucket", &options, TEST_OWNER);

        record.complete(
            ManualTransitionRunReport {
                bucket: "bucket".to_string(),
                enqueued: 1,
                ..Default::default()
            },
            ManualTransitionQueueSnapshot {
                queued: 1,
                ..Default::default()
            },
        );

        assert_eq!(record.state, ManualTransitionJobState::Running);
        assert!(!record.mark_unknown_if_worker_results_lost(ManualTransitionQueueSnapshot {
            queued: 1,
            ..Default::default()
        }));

        assert!(record.mark_unknown_if_worker_results_lost(ManualTransitionQueueSnapshot::default()));
        assert_eq!(record.state, ManualTransitionJobState::Unknown);
        assert!(record.error.as_deref().is_some_and(|err| err.contains("worker result")));
        assert!(record.completed_at_unix_nanos.is_some());
    }

    #[test]
    fn manual_transition_job_marks_unknown_when_recovery_would_skip_pending_page() {
        let options = ManualTransitionRunOptions::default();
        let mut record = ManualTransitionJobRecord::new(Uuid::new_v4(), "bucket", &options, TEST_OWNER);
        record.report.enqueued = 2;
        record.report.transition_completed = 1;

        assert!(!record.mark_unknown_if_recovery_would_skip_pending_page(ManualTransitionQueueSnapshot {
            queued: 1,
            ..Default::default()
        }));

        record.report.continuation_token = Some("opaque".to_string());
        assert!(!record.mark_unknown_if_recovery_would_skip_pending_page(ManualTransitionQueueSnapshot {
            active: 1,
            ..Default::default()
        }));

        assert!(record.mark_unknown_if_recovery_would_skip_pending_page(ManualTransitionQueueSnapshot::default()));
        assert_eq!(record.state, ManualTransitionJobState::Unknown);
        assert!(record.error.as_deref().is_some_and(|err| err.contains("page/task journal")));
        assert!(record.completed_at_unix_nanos.is_some());
    }

    #[test]
    fn manual_transition_job_marks_unknown_for_corrupt_worker_result_journal() {
        let options = ManualTransitionRunOptions::default();
        let mut record = ManualTransitionJobRecord::new(Uuid::new_v4(), "bucket", &options, TEST_OWNER);

        assert!(!record.mark_unknown_for_worker_result_journal_error("bad marker", ManualTransitionQueueSnapshot::default()));

        record.complete(
            ManualTransitionRunReport {
                bucket: "bucket".to_string(),
                enqueued: 1,
                ..Default::default()
            },
            ManualTransitionQueueSnapshot::default(),
        );
        assert!(record.mark_unknown_for_worker_result_journal_error("bad marker", ManualTransitionQueueSnapshot::default()));

        assert_eq!(record.state, ManualTransitionJobState::Unknown);
        assert!(
            record
                .error
                .as_deref()
                .is_some_and(|error| error.contains("worker result journal is corrupt"))
        );
        assert!(record.completed_at_unix_nanos.is_some());
    }

    #[test]
    fn manual_transition_job_record_builds_resume_options() {
        let options = ManualTransitionRunOptions {
            prefix: "logs/".to_string(),
            continuation_token: Some("start-token".to_string()),
            tier: Some("warm".to_string()),
            dry_run: true,
            max_objects: Some(3),
            max_duration: Some(std::time::Duration::from_secs(5)),
            ..Default::default()
        };
        let mut record = ManualTransitionJobRecord::new(Uuid::new_v4(), "bucket", &options, TEST_OWNER);
        record.report.continuation_token = Some("resume-token".to_string());

        let resume = record.resume_options();

        assert_eq!(resume.prefix, "logs/");
        assert_eq!(resume.continuation_token.as_deref(), Some("resume-token"));
        assert_eq!(resume.tier.as_deref(), Some("warm"));
        assert!(resume.dry_run);
        assert_eq!(resume.max_objects, Some(3));
        assert_eq!(resume.max_duration, Some(std::time::Duration::from_secs(5)));
        assert!(resume.cancel_token.is_none());
        assert!(resume.cancel_check.is_none());
        assert!(resume.progress_sink.is_none());
    }

    #[test]
    fn manual_transition_job_record_object_name_round_trips_job_id() {
        let job_id = Uuid::new_v4();
        let object_name = manual_transition_job_record_object_name(job_id).expect("job record path should encode");

        let decoded = manual_transition_job_id_from_record_object_name(&object_name).expect("job record path should decode");

        assert_eq!(decoded, job_id);
    }

    #[test]
    fn manual_transition_job_record_object_name_rejects_shard_mismatch() {
        let job_id = Uuid::new_v4();
        let object_name = manual_transition_job_record_object_name(job_id).expect("job record path should encode");
        let job_key = job_id.simple().to_string();
        let bad_first_shard = if &job_key[..2] == "ff" { "00" } else { "ff" };
        let object_name = object_name.replacen(
            &format!("{MANUAL_TRANSITION_JOB_RECORD_PREFIX}/{}/", &job_key[..2]),
            &format!("{MANUAL_TRANSITION_JOB_RECORD_PREFIX}/{bad_first_shard}/"),
            1,
        );

        let err = manual_transition_job_id_from_record_object_name(&object_name).expect_err("bad shard must fail closed");

        assert!(err.to_string().contains("shards"));
    }

    #[test]
    fn manual_transition_job_record_rejects_checksum_drift() {
        let options = ManualTransitionRunOptions::default();
        let record = ManualTransitionJobRecord::new(Uuid::new_v4(), "bucket", &options, TEST_OWNER);
        let encoded = record.encode().expect("job record should encode");
        let mut value: serde_json::Value = serde_json::from_slice(&encoded).expect("encoded job should be json");
        value["job"]["bucket"] = serde_json::Value::String("other-bucket".to_string());
        let mutated = serde_json::to_vec(&value).expect("mutated job should encode");

        let err = ManualTransitionJobRecord::decode(record.job_id, &mutated).expect_err("checksum drift must fail closed");

        assert!(matches!(err, ManualTransitionJobError::ChecksumMismatch));
    }

    #[test]
    fn manual_transition_worker_result_record_rejects_checksum_drift() {
        let job_id = Uuid::new_v4();
        let task_key = manual_transition_worker_result_task_key("bucket", "logs/a", None);
        let record = ManualTransitionWorkerResultRecord::new(job_id, &task_key, ManualTransitionWorkerResult::Completed);
        let encoded = record.encode().expect("worker result record should encode");
        let mut value: serde_json::Value = serde_json::from_slice(&encoded).expect("encoded worker result should be json");
        value["record"]["result"] = serde_json::Value::String("tier_failure".to_string());
        let mutated = serde_json::to_vec(&value).expect("mutated worker result should encode");

        let err = ManualTransitionWorkerResultRecord::decode(job_id, &task_key, &mutated)
            .expect_err("worker result checksum drift must fail closed");

        assert!(matches!(err, ManualTransitionJobError::ChecksumMismatch));
    }

    #[test]
    fn manual_transition_worker_result_record_round_trips_without_failure_reason() {
        let job_id = Uuid::new_v4();
        let task_key = manual_transition_worker_result_task_key("bucket", "logs/a", None);
        let record = ManualTransitionWorkerResultRecord::new_with_reason(
            job_id,
            &task_key,
            ManualTransitionWorkerResult::TierFailure,
            Some(ManualTransitionWorkerFailureReason::Network),
        );
        let encoded = record.encode().expect("worker result record should encode");
        let mut value: serde_json::Value = serde_json::from_slice(&encoded).expect("encoded worker result should be json");
        let record_obj = value["record"].as_object_mut().expect("record object should be present");
        record_obj.remove("failure_reason");
        let record_bytes = serde_json::to_vec(&value["record"]).expect("record without reason should encode");
        value["content_sha256"] = serde_json::Value::String(hex_sha256(&record_bytes, ToOwned::to_owned));
        let stripped = serde_json::to_vec(&value).expect("stripped worker result should encode");

        let decoded = ManualTransitionWorkerResultRecord::decode(job_id, &task_key, &stripped)
            .expect("worker result without reason should decode");

        assert_eq!(decoded.failure_reason, None);
        assert_eq!(decoded.result, ManualTransitionWorkerResult::TierFailure);
    }

    #[test]
    fn manual_transition_worker_result_stats_aggregates_reasons() {
        let mut stats = ManualTransitionWorkerResultStats::default();

        stats.record(ManualTransitionWorkerResult::Completed, None);
        stats.record(
            ManualTransitionWorkerResult::TierFailure,
            Some(ManualTransitionWorkerFailureReason::PermissionDenied),
        );
        stats.record(
            ManualTransitionWorkerResult::TierFailure,
            Some(ManualTransitionWorkerFailureReason::PermissionDenied),
        );
        stats.record(ManualTransitionWorkerResult::TierFailure, None);

        assert_eq!(stats.completed, 1);
        assert_eq!(stats.failed, 3);
        assert_eq!(
            stats
                .tier_failure_by_reason
                .get(&ManualTransitionWorkerFailureReason::PermissionDenied),
            Some(&2)
        );
        assert_eq!(
            stats
                .tier_failure_by_reason
                .get(&ManualTransitionWorkerFailureReason::Unknown),
            Some(&1)
        );
    }

    #[test]
    fn manual_transition_task_record_rejects_checksum_drift() {
        let job_id = Uuid::new_v4();
        let task_key = manual_transition_worker_result_task_key("bucket", "logs/a", Some(Uuid::new_v4()));
        let record = ManualTransitionTaskRecord::new(job_id, &task_key, "bucket", "logs/a", Some(Uuid::new_v4()), "WARM");
        let encoded = record.encode().expect("task record should encode");
        let mut value: serde_json::Value = serde_json::from_slice(&encoded).expect("encoded task record should be json");
        value["record"]["storage_class"] = serde_json::Value::String("COLD".to_string());
        let mutated = serde_json::to_vec(&value).expect("mutated task record should encode");

        let err =
            ManualTransitionTaskRecord::decode(job_id, &task_key, &mutated).expect_err("task checksum drift must fail closed");

        assert!(matches!(err, ManualTransitionJobError::ChecksumMismatch));
    }

    #[test]
    fn manual_transition_job_record_round_trips_legacy_report_without_failure_reason_map() {
        let options = ManualTransitionRunOptions::default();
        let record = ManualTransitionJobRecord::new(Uuid::new_v4(), "bucket", &options, TEST_OWNER);
        let encoded = record.encode().expect("job record should encode");
        let mut value: serde_json::Value = serde_json::from_slice(&encoded).expect("encoded job should be json");
        let report = value["job"]["report"].as_object_mut().expect("report should be object");
        report.remove("tier_failure_by_reason");
        let record_bytes = serde_json::to_vec(&value["job"]).expect("job without report failure map should encode");
        value["content_sha256"] = serde_json::Value::String(hex_sha256(&record_bytes, ToOwned::to_owned));
        let legacy = serde_json::to_vec(&value).expect("job without report failure map should encode");

        let decoded = ManualTransitionJobRecord::decode(record.job_id, &legacy).expect("legacy job report should decode");

        assert_eq!(decoded.state, ManualTransitionJobState::Running);
        assert!(decoded.report.tier_failure_by_reason.is_empty());
    }

    #[test]
    fn manual_transition_job_record_derives_revision_from_legacy_cursor() {
        let options = ManualTransitionRunOptions::default();
        let mut record = ManualTransitionJobRecord::new(Uuid::new_v4(), "bucket", &options, TEST_OWNER);
        let continuation_token =
            encode_manual_transition_continuation_token(Some("logs/page-a".to_string()), Some("version-a".to_string()));
        record.update_running_progress(
            ManualTransitionRunReport {
                bucket: "bucket".to_string(),
                scanned: 9,
                continuation_token,
                ..Default::default()
            },
            ManualTransitionQueueSnapshot::default(),
        );
        let encoded = record.encode().expect("job record should encode");
        let mut value: serde_json::Value = serde_json::from_slice(&encoded).expect("encoded job should be json");
        value["job"]
            .as_object_mut()
            .expect("job should be object")
            .remove("cursor_revision");
        let record_bytes = serde_json::to_vec(&value["job"]).expect("legacy job should encode");
        value["content_sha256"] = serde_json::Value::String(hex_sha256(&record_bytes, ToOwned::to_owned));
        let legacy = serde_json::to_vec(&value).expect("legacy envelope should encode");

        let decoded = ManualTransitionJobRecord::decode(record.job_id, &legacy).expect("legacy job should decode");

        assert_eq!(decoded.cursor_revision, Some(9));
    }

    #[test]
    fn manual_transition_job_record_omits_cursor_revision_for_old_readers() {
        #[allow(dead_code)]
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct LegacyPersistedManualTransitionJobRecord {
            schema: String,
            content_sha256: String,
            job: LegacyManualTransitionJobRecord,
        }

        #[allow(dead_code)]
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct LegacyManualTransitionJobRecord {
            job_id: Uuid,
            scope_key: String,
            bucket: String,
            prefix: String,
            tier: Option<String>,
            dry_run: bool,
            max_objects: Option<u64>,
            max_duration: Option<std::time::Duration>,
            owner_id: String,
            lease_id: Uuid,
            lease_expires_at_unix_nanos: i128,
            state: ManualTransitionJobState,
            scan_completed: bool,
            cancel_requested: bool,
            created_at_unix_nanos: i128,
            updated_at_unix_nanos: i128,
            completed_at_unix_nanos: Option<i128>,
            report: ManualTransitionRunReport,
            queue_snapshot: ManualTransitionQueueSnapshot,
            error: Option<String>,
        }

        let options = ManualTransitionRunOptions::default();
        let mut record = ManualTransitionJobRecord::new(Uuid::new_v4(), "bucket", &options, TEST_OWNER);
        let continuation_token =
            encode_manual_transition_continuation_token(Some("logs/page-a".to_string()), Some("version-a".to_string()));
        record.update_running_progress(
            ManualTransitionRunReport {
                bucket: "bucket".to_string(),
                scanned: 7,
                continuation_token: continuation_token.clone(),
                ..Default::default()
            },
            ManualTransitionQueueSnapshot::default(),
        );
        assert_eq!(record.cursor_revision, Some(7));

        let encoded = record.encode().expect("job record should encode");
        let value: serde_json::Value = serde_json::from_slice(&encoded).expect("encoded job should be json");
        assert!(value["job"].get("cursor_revision").is_none());
        let legacy: LegacyPersistedManualTransitionJobRecord =
            serde_json::from_slice(&encoded).expect("old reader should accept new job record");

        assert_eq!(legacy.job.job_id, record.job_id);
        assert_eq!(legacy.job.report.continuation_token, continuation_token);
    }

    #[test]
    fn manual_transition_job_record_rejects_unknown_report_fields() {
        let options = ManualTransitionRunOptions::default();
        let record = ManualTransitionJobRecord::new(Uuid::new_v4(), "bucket", &options, TEST_OWNER);
        let encoded = record.encode().expect("job record should encode");
        let mut value: serde_json::Value = serde_json::from_slice(&encoded).expect("encoded job should be json");
        let checksum = value.get_mut("content_sha256").expect("encoded job has checksum");
        *checksum = serde_json::Value::String("0".repeat(64));
        value["job"]["report"]["unexpected"] = serde_json::Value::Bool(true);
        let mutated_job_bytes = serde_json::to_vec(&value["job"]).expect("mutated job should encode");
        value["content_sha256"] = serde_json::Value::String(hex_sha256(&mutated_job_bytes, ToOwned::to_owned));
        let mutated = serde_json::to_vec(&value).expect("mutated envelope should encode");

        let err = ManualTransitionJobRecord::decode(record.job_id, &mutated).expect_err("unknown report field must fail");

        assert!(err.to_string().contains("unknown field"));
    }

    #[test]
    fn manual_transition_job_record_marks_restart_unknown_terminal() {
        let options = ManualTransitionRunOptions::default();
        let mut record = ManualTransitionJobRecord::new(Uuid::new_v4(), "bucket", &options, TEST_OWNER);

        record.mark_unknown_if_unowned();

        assert_eq!(record.state, ManualTransitionJobState::Unknown);
        assert!(record.is_terminal());
        assert!(record.completed_at_unix_nanos.is_some());
        assert!(
            record
                .error
                .as_deref()
                .is_some_and(|error| error.contains("unknown after restart"))
        );
    }

    #[test]
    fn manual_transition_job_record_persists_restart_unknown_state() {
        let options = ManualTransitionRunOptions::default();
        let mut record = ManualTransitionJobRecord::new(Uuid::new_v4(), "bucket", &options, TEST_OWNER);
        record.mark_unknown_if_unowned();

        let encoded = record.encode().expect("unknown restart job should encode");
        let decoded = ManualTransitionJobRecord::decode(record.job_id, &encoded).expect("unknown restart job should decode");

        assert_eq!(decoded.state, ManualTransitionJobState::Unknown);
        assert!(decoded.is_terminal());
        assert!(decoded.completed_at_unix_nanos.is_some());
        assert!(
            decoded
                .error
                .as_deref()
                .is_some_and(|error| error.contains("unknown after restart"))
        );
    }

    #[test]
    fn manual_transition_job_record_control_plane_failure_does_not_count_tier_failure() {
        let options = ManualTransitionRunOptions::default();
        let mut record = ManualTransitionJobRecord::new(Uuid::new_v4(), "bucket", &options, TEST_OWNER);

        record.fail("missing tier");

        assert_eq!(record.state, ManualTransitionJobState::Failed);
        assert_eq!(record.report.tier_failure, 0);
        assert_eq!(record.error.as_deref(), Some("missing tier"));
    }

    #[test]
    fn manual_transition_job_record_tier_failure_report_is_partial() {
        let options = ManualTransitionRunOptions::default();
        let mut record = ManualTransitionJobRecord::new(Uuid::new_v4(), "bucket", &options, TEST_OWNER);

        record.complete(
            ManualTransitionRunReport {
                tier_failure: 1,
                ..Default::default()
            },
            ManualTransitionQueueSnapshot::default(),
        );

        assert_eq!(record.state, ManualTransitionJobState::Partial);
        assert_eq!(record.report.tier_failure, 1);
        assert!(record.error.is_none());
    }

    #[test]
    fn manual_transition_job_record_queue_pressure_report_is_partial() {
        let options = ManualTransitionRunOptions::default();
        let mut record = ManualTransitionJobRecord::new(Uuid::new_v4(), "bucket", &options, TEST_OWNER);
        let queue_snapshot = ManualTransitionQueueSnapshot {
            queue_capacity: 1,
            queue_full: 1,
            ..Default::default()
        };

        record.complete(
            ManualTransitionRunReport {
                skipped_queue_full: 1,
                ..Default::default()
            },
            queue_snapshot,
        );

        assert_eq!(record.state, ManualTransitionJobState::Partial);
        assert_eq!(record.report.skipped_queue_full, 1);
        assert_eq!(record.queue_snapshot.queue_capacity, 1);
        assert_eq!(record.queue_snapshot.queue_full, 1);
        assert!(record.error.is_none());
    }

    #[test]
    fn manual_transition_job_record_in_flight_skip_report_is_partial() {
        let options = ManualTransitionRunOptions::default();
        let mut record = ManualTransitionJobRecord::new(Uuid::new_v4(), "bucket", &options, TEST_OWNER);

        record.complete(
            ManualTransitionRunReport {
                eligible: 1,
                skipped_already_in_flight: 1,
                ..Default::default()
            },
            ManualTransitionQueueSnapshot::default(),
        );

        assert_eq!(record.state, ManualTransitionJobState::Partial);
        assert_eq!(record.report.skipped_already_in_flight, 1);
        assert!(record.error.is_none());
    }

    #[test]
    fn manual_transition_job_record_queue_pressure_reports_persist_readback() {
        let options = ManualTransitionRunOptions {
            prefix: "logs/".to_string(),
            tier: Some("warm".to_string()),
            max_objects: Some(10),
            ..Default::default()
        };

        for (bucket, skipped_queue_full, skipped_queue_closed, skipped_queue_timeout, active_snapshot, terminal_snapshot) in [
            (
                "manual-queue-full-readback-bucket",
                2,
                0,
                0,
                ManualTransitionQueueSnapshot {
                    queue_capacity: 4,
                    queued: 4,
                    workers: 2,
                    queue_full: 2,
                    ..Default::default()
                },
                ManualTransitionQueueSnapshot {
                    queue_capacity: 4,
                    workers: 2,
                    queue_full: 2,
                    ..Default::default()
                },
            ),
            (
                "manual-queue-closed-readback-bucket",
                0,
                1,
                0,
                ManualTransitionQueueSnapshot {
                    queue_capacity: 4,
                    workers: 2,
                    ..Default::default()
                },
                ManualTransitionQueueSnapshot {
                    queue_capacity: 4,
                    workers: 2,
                    ..Default::default()
                },
            ),
            (
                "manual-queue-timeout-readback-bucket",
                0,
                0,
                1,
                ManualTransitionQueueSnapshot {
                    queue_capacity: 4,
                    active: 1,
                    workers: 2,
                    queue_send_timeout: 1,
                    ..Default::default()
                },
                ManualTransitionQueueSnapshot {
                    queue_capacity: 4,
                    workers: 2,
                    queue_send_timeout: 1,
                    ..Default::default()
                },
            ),
        ] {
            let mut record = ManualTransitionJobRecord::new(Uuid::new_v4(), bucket, &options, TEST_OWNER);
            record.complete(
                ManualTransitionRunReport {
                    enqueued: 1,
                    skipped_queue_full,
                    skipped_queue_closed,
                    skipped_queue_timeout,
                    continuation_token: Some("opaque-queue-pressure-token".to_string()),
                    ..Default::default()
                },
                active_snapshot,
            );

            assert_eq!(record.state, ManualTransitionJobState::Running);
            assert!(record.report.has_partial_enqueue());
            assert!(record.report.worker_transition_pending());
            assert_eq!(record.queue_snapshot, active_snapshot);

            let encoded = record.encode().expect("queue-pressure partial job should encode");
            let decoded =
                ManualTransitionJobRecord::decode(record.job_id, &encoded).expect("running queue-pressure job should decode");
            assert_eq!(decoded.state, ManualTransitionJobState::Running);
            assert!(decoded.report.worker_transition_pending());
            assert_eq!(decoded.report.skipped_queue_full, skipped_queue_full);
            assert_eq!(decoded.report.skipped_queue_closed, skipped_queue_closed);
            assert_eq!(decoded.report.skipped_queue_timeout, skipped_queue_timeout);
            assert_eq!(decoded.report.continuation_token.as_deref(), Some("opaque-queue-pressure-token"));
            assert_eq!(decoded.queue_snapshot, active_snapshot);

            record.record_worker_result(ManualTransitionWorkerResult::Completed, terminal_snapshot);

            let encoded = record.encode().expect("terminal queue-pressure partial job should encode");
            let decoded =
                ManualTransitionJobRecord::decode(record.job_id, &encoded).expect("terminal queue-pressure job should decode");
            assert_eq!(decoded.state, ManualTransitionJobState::Partial);
            assert!(decoded.is_terminal());
            assert_eq!(decoded.report.enqueued, 1);
            assert_eq!(decoded.report.transition_completed, 1);
            assert_eq!(decoded.report.skipped_queue_full, skipped_queue_full);
            assert_eq!(decoded.report.skipped_queue_closed, skipped_queue_closed);
            assert_eq!(decoded.report.skipped_queue_timeout, skipped_queue_timeout);
            assert!(decoded.report.has_partial_enqueue());
            assert_eq!(decoded.report.continuation_token.as_deref(), Some("opaque-queue-pressure-token"));
            assert_eq!(decoded.queue_snapshot, terminal_snapshot);
            assert!(decoded.completed_at_unix_nanos.is_some());
            assert!(decoded.error.is_none());
        }
    }

    #[test]
    fn manual_transition_job_record_budget_reports_are_partial_and_resumable() {
        let options = ManualTransitionRunOptions {
            prefix: "logs/".to_string(),
            tier: Some("warm".to_string()),
            max_objects: Some(1),
            max_duration: Some(std::time::Duration::from_secs(10)),
            ..Default::default()
        };

        for (bucket, truncated_by_limit, truncated_by_duration) in [
            ("manual-budget-limit-bucket", true, false),
            ("manual-budget-duration-bucket", false, true),
        ] {
            let mut record = ManualTransitionJobRecord::new(Uuid::new_v4(), bucket, &options, TEST_OWNER);
            let queue_snapshot = ManualTransitionQueueSnapshot {
                queue_capacity: 8,
                queued: 1,
                active: 1,
                workers: 2,
                ..Default::default()
            };
            record.complete(
                ManualTransitionRunReport {
                    bucket: bucket.to_string(),
                    prefix: "logs/".to_string(),
                    tier: Some("warm".to_string()),
                    scanned: 1,
                    eligible: 1,
                    dry_run_eligible: 1,
                    truncated_by_limit,
                    truncated_by_duration,
                    continuation_token: Some("opaque-budget-token".to_string()),
                    ..Default::default()
                },
                queue_snapshot,
            );

            assert_eq!(record.state, ManualTransitionJobState::Partial);
            assert_eq!(record.report.continuation_token.as_deref(), Some("opaque-budget-token"));
            assert!(record.report.was_truncated());
            assert_eq!(record.queue_snapshot, queue_snapshot);
            assert!(record.completed_at_unix_nanos.is_some());
            assert!(record.error.is_none());

            let encoded = record.encode().expect("budget partial job should encode");
            let decoded = ManualTransitionJobRecord::decode(record.job_id, &encoded).expect("budget partial job should decode");
            assert_eq!(decoded.state, ManualTransitionJobState::Partial);
            assert_eq!(decoded.max_duration, options.max_duration);
            assert_eq!(decoded.report.truncated_by_limit, truncated_by_limit);
            assert_eq!(decoded.report.truncated_by_duration, truncated_by_duration);
            assert_eq!(decoded.report.continuation_token.as_deref(), Some("opaque-budget-token"));
            assert_eq!(decoded.queue_snapshot, queue_snapshot);
        }
    }

    #[test]
    fn manual_transition_job_budget_report_waits_for_pending_workers() {
        let options = ManualTransitionRunOptions {
            prefix: "logs/".to_string(),
            tier: Some("warm".to_string()),
            max_objects: Some(1),
            ..Default::default()
        };
        let mut record = ManualTransitionJobRecord::new(Uuid::new_v4(), "manual-budget-pending-bucket", &options, TEST_OWNER);
        let active_snapshot = ManualTransitionQueueSnapshot {
            queue_capacity: 4,
            queued: 1,
            active: 1,
            workers: 2,
            ..Default::default()
        };

        record.complete(
            ManualTransitionRunReport {
                bucket: "manual-budget-pending-bucket".to_string(),
                prefix: "logs/".to_string(),
                tier: Some("warm".to_string()),
                scanned: 1,
                eligible: 1,
                enqueued: 1,
                truncated_by_limit: true,
                continuation_token: Some("opaque-budget-token".to_string()),
                ..Default::default()
            },
            active_snapshot,
        );

        assert_eq!(record.state, ManualTransitionJobState::Running);
        assert!(record.scan_completed);
        assert!(record.completed_at_unix_nanos.is_none());
        assert!(record.report.was_truncated());
        assert!(record.report.worker_transition_pending());
        assert_eq!(record.report.continuation_token.as_deref(), Some("opaque-budget-token"));
        assert_eq!(record.queue_snapshot, active_snapshot);

        let drained_snapshot = ManualTransitionQueueSnapshot {
            queue_capacity: 4,
            workers: 2,
            ..Default::default()
        };
        record.record_worker_result(ManualTransitionWorkerResult::Completed, drained_snapshot);

        assert_eq!(record.state, ManualTransitionJobState::Partial);
        assert_eq!(record.report.transition_completed, 1);
        assert_eq!(record.queue_snapshot, drained_snapshot);
        assert!(record.completed_at_unix_nanos.is_some());
        assert!(record.error.is_none());
    }

    #[test]
    fn manual_transition_scope_key_is_stable_and_sanitized() {
        let first = manual_transition_scope_key(
            "bucket",
            &ManualTransitionRunOptions {
                prefix: "logs/".to_string(),
                tier: Some("warm".to_string()),
                ..Default::default()
            },
        );
        let second = manual_transition_scope_key(
            "bucket",
            &ManualTransitionRunOptions {
                prefix: "logs/".to_string(),
                tier: Some("WARM".to_string()),
                marker: Some("ignored".to_string()),
                ..Default::default()
            },
        );

        assert_eq!(first, second);
        assert!(
            manual_transition_scope_record_object_name(&first)
                .expect("scope path should encode")
                .starts_with(MANUAL_TRANSITION_SCOPE_RECORD_PREFIX)
        );
    }

    #[test]
    fn manual_transition_scope_key_uses_bucket_level_admission_for_durable_v1() {
        let broad = manual_transition_scope_key(
            "bucket",
            &ManualTransitionRunOptions {
                prefix: "logs/".to_string(),
                tier: None,
                ..Default::default()
            },
        );
        let nested = manual_transition_scope_key(
            "bucket",
            &ManualTransitionRunOptions {
                prefix: "logs/2026/".to_string(),
                tier: Some("warm".to_string()),
                ..Default::default()
            },
        );
        let disjoint = manual_transition_scope_key(
            "bucket",
            &ManualTransitionRunOptions {
                prefix: "archive/".to_string(),
                tier: Some("cold".to_string()),
                ..Default::default()
            },
        );
        let dry_run = manual_transition_scope_key(
            "bucket",
            &ManualTransitionRunOptions {
                prefix: "logs/".to_string(),
                dry_run: true,
                ..Default::default()
            },
        );
        let other_bucket = manual_transition_scope_key(
            "bucket-b",
            &ManualTransitionRunOptions {
                prefix: "logs/".to_string(),
                tier: Some("warm".to_string()),
                ..Default::default()
            },
        );

        assert_eq!(broad, nested);
        assert_eq!(broad, disjoint);
        assert_ne!(broad, dry_run);
        assert_ne!(broad, other_bucket);
    }

    #[test]
    fn manual_transition_scope_admission_carries_job_lease_fence() {
        let options = ManualTransitionRunOptions::default();
        let record = ManualTransitionJobRecord::new(Uuid::new_v4(), "bucket", &options, TEST_OWNER);

        let admission = ManualTransitionScopeAdmission::from_job(&record);

        assert_eq!(admission.job_id, record.job_id);
        assert_eq!(admission.lease_id, record.lease_id);
        assert_eq!(admission.owner_id, TEST_OWNER);
        assert_eq!(admission.scope_key, record.scope_key);
        assert!(admission.validate().is_ok());
    }
}
