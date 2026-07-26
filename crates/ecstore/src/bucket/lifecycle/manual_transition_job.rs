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

use std::sync::Arc;

use rustfs_utils::crypto::{hex_sha256, is_sha256_checksum};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::bucket::lifecycle::bucket_lifecycle_ops::{
    ManualTransitionQueueSnapshot, ManualTransitionRunOptions, ManualTransitionRunReport,
};
use crate::bucket::lifecycle::config_boundary;
use crate::disk::RUSTFS_META_BUCKET;
use crate::error::{Error, Result as EcstoreResult};
use crate::object_api::ObjectOptions;
use crate::storage_api_contracts::list::ListOperations as _;
use crate::storage_api_contracts::object::HTTPPreconditions;
use crate::store::ECStore;

pub const MANUAL_TRANSITION_JOB_SCHEMA: &str = "rustfs-manual-transition-job-v1";
pub const MANUAL_TRANSITION_TASK_SCHEMA: &str = "rustfs-manual-transition-task-v1";
pub const MANUAL_TRANSITION_WORKER_RESULT_SCHEMA: &str = "rustfs-manual-transition-worker-result-v1";
pub const MANUAL_TRANSITION_JOB_RECORD_PREFIX: &str = "ilm/manual-transition/jobs";
pub const MANUAL_TRANSITION_SCOPE_RECORD_PREFIX: &str = "ilm/manual-transition/scopes";
pub const MANUAL_TRANSITION_TASK_PREFIX: &str = "ilm/manual-transition/tasks";
pub const MANUAL_TRANSITION_WORKER_RESULT_PREFIX: &str = "ilm/manual-transition/results";
pub const MAX_MANUAL_TRANSITION_JOB_RECORD_SIZE: usize = 64 * 1024;
pub const MAX_MANUAL_TRANSITION_TASK_RECORD_SIZE: usize = 16 * 1024;
pub const MAX_MANUAL_TRANSITION_WORKER_RESULT_RECORD_SIZE: usize = 8 * 1024;
const MANUAL_TRANSITION_JOB_LEASE_SECONDS: i128 = 60;
const MANUAL_TRANSITION_LEGACY_SCOPE_SCAN_LIMIT: i32 = 1000;
const MANUAL_TRANSITION_TASK_SCAN_LIMIT: i32 = 1000;
const MANUAL_TRANSITION_WORKER_RESULT_SCAN_LIMIT: i32 = 1000;

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
            owner_id: owner_id.into(),
            lease_id,
            lease_expires_at_unix_nanos: manual_transition_job_lease_expires_at(now),
            state: ManualTransitionJobState::Running,
            scan_completed: false,
            cancel_requested: false,
            created_at_unix_nanos: now,
            updated_at_unix_nanos: now,
            completed_at_unix_nanos: None,
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
        self.report.merge_scan_report_preserving_worker(&report);
        self.queue_snapshot = queue_snapshot;
        self.error = None;
        self.mark_terminal_if_worker_drained();
    }

    pub fn fail(&mut self, error: impl Into<String>) {
        self.state = ManualTransitionJobState::Failed;
        self.report.tier_failure = self.report.tier_failure.saturating_add(1);
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
            }
        }
        self.queue_snapshot = queue_snapshot;
        self.updated_at_unix_nanos = OffsetDateTime::now_utc().unix_timestamp_nanos();
        self.mark_terminal_if_worker_drained();
    }

    fn apply_worker_result_counts(
        &mut self,
        completed: u64,
        failed: u64,
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
        let scan_tier_failure = self.report.tier_failure.saturating_sub(self.report.transition_failed);
        self.report.enqueued = enqueued;
        self.report.transition_completed = transition_completed;
        self.report.transition_failed = transition_failed;
        self.report.tier_failure = scan_tier_failure.saturating_add(transition_failed);
        self.queue_snapshot = queue_snapshot;
        self.updated_at_unix_nanos = OffsetDateTime::now_utc().unix_timestamp_nanos();
        self.mark_terminal_if_worker_drained();
        true
    }

    pub fn mark_cancel_requested(&mut self) {
        self.cancel_requested = true;
        self.updated_at_unix_nanos = OffsetDateTime::now_utc().unix_timestamp_nanos();
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
            self.updated_at_unix_nanos = OffsetDateTime::now_utc().unix_timestamp_nanos();
        }
    }

    pub fn resume_options(&self) -> ManualTransitionRunOptions {
        ManualTransitionRunOptions {
            prefix: self.prefix.clone(),
            continuation_token: self.report.continuation_token.clone(),
            tier: self.tier.clone(),
            dry_run: self.dry_run,
            max_objects: self.max_objects,
            ..Default::default()
        }
    }

    pub fn renew_lease(&mut self, queue_snapshot: ManualTransitionQueueSnapshot) {
        let now = OffsetDateTime::now_utc().unix_timestamp_nanos();
        self.updated_at_unix_nanos = now;
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
            self.report.merge_scan_report_preserving_worker(&report);
            self.renew_lease(queue_snapshot);
        }
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
        let now = OffsetDateTime::now_utc().unix_timestamp_nanos();
        self.updated_at_unix_nanos = now;
        self.completed_at_unix_nanos = Some(now);
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
        let job_bytes = serde_json::to_vec(&job)?;
        let actual_checksum = hex_sha256(&job_bytes, ToOwned::to_owned);
        if persisted.content_sha256 != actual_checksum {
            return Err(ManualTransitionJobError::ChecksumMismatch);
        }
        if job.job_id != expected_job_id {
            return Err(ManualTransitionJobError::Corrupt("job_id does not match record key"));
        }
        if job.state == ManualTransitionJobState::Cancelled && job.cancel_requested {
            job.report.cancelled = true;
        }
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
            queued_at_unix_nanos: OffsetDateTime::now_utc().unix_timestamp_nanos(),
        }
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ManualTransitionWorkerResultStats {
    pub completed: u64,
    pub failed: u64,
}

impl ManualTransitionWorkerResultStats {
    fn record(&mut self, result: ManualTransitionWorkerResult) {
        match result {
            ManualTransitionWorkerResult::Completed => self.completed = self.completed.saturating_add(1),
            ManualTransitionWorkerResult::TierFailure => self.failed = self.failed.saturating_add(1),
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
    pub completed_at_unix_nanos: i128,
}

impl ManualTransitionWorkerResultRecord {
    pub fn new(job_id: Uuid, task_key: impl Into<String>, result: ManualTransitionWorkerResult) -> Self {
        Self {
            schema: MANUAL_TRANSITION_WORKER_RESULT_SCHEMA.to_string(),
            job_id,
            task_key: task_key.into(),
            result,
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
        let record_bytes = serde_json::to_vec(&record)?;
        let actual_checksum = hex_sha256(&record_bytes, ToOwned::to_owned);
        if persisted.content_sha256 != actual_checksum {
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
    config_boundary::save_config(api, &object, data).await
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
    config_boundary::save_config_with_opts(
        api,
        &object,
        data,
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

pub async fn load_manual_transition_worker_result_stats(
    api: Arc<ECStore>,
    job_id: Uuid,
) -> EcstoreResult<ManualTransitionWorkerResultStats> {
    match scan_manual_transition_worker_result_journal(api, job_id).await? {
        ManualTransitionWorkerResultJournal::Stats(stats) => Ok(stats),
        ManualTransitionWorkerResultJournal::Corrupt(error) => Err(Error::other(error)),
    }
}

enum ManualTransitionWorkerResultJournal {
    Stats(ManualTransitionWorkerResultStats),
    Corrupt(String),
}

async fn scan_manual_transition_worker_result_journal(
    api: Arc<ECStore>,
    job_id: Uuid,
) -> EcstoreResult<ManualTransitionWorkerResultJournal> {
    let prefix = manual_transition_worker_result_object_prefix(job_id).map_err(manual_transition_job_store_error)?;
    let mut marker = None;
    let mut stats = ManualTransitionWorkerResultStats::default();
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
            stats.record(result.result);
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

pub async fn reconcile_manual_transition_worker_results(
    api: Arc<ECStore>,
    job_id: Uuid,
    queue_snapshot: ManualTransitionQueueSnapshot,
) -> EcstoreResult<ManualTransitionJobRecord> {
    let task_stats = match scan_manual_transition_task_journal(api.clone(), job_id).await? {
        ManualTransitionTaskJournal::Stats(stats) => stats,
        ManualTransitionTaskJournal::Corrupt(error) => {
            return mark_manual_transition_job_unknown_for_task_journal_error(api, job_id, error, queue_snapshot).await;
        }
    };
    let stats = match scan_manual_transition_worker_result_journal(api.clone(), job_id).await? {
        ManualTransitionWorkerResultJournal::Stats(stats) => stats,
        ManualTransitionWorkerResultJournal::Corrupt(error) => {
            return mark_manual_transition_job_unknown_for_worker_result_journal_error(api, job_id, error, queue_snapshot).await;
        }
    };
    for _ in 0..4 {
        let (mut record, etag) = load_manual_transition_job_record_with_etag(api.clone(), job_id).await?;
        let changed = record.apply_worker_result_counts(stats.completed, stats.failed, task_stats.queued, queue_snapshot);
        if !changed {
            return Ok(record);
        }
        match save_manual_transition_job_record_if_current(api.clone(), &record, &etag).await {
            Ok(()) => {
                if record.is_terminal() {
                    delete_manual_transition_scope_admission_if_current(
                        api.clone(),
                        &record.scope_key,
                        record.job_id,
                        record.lease_id,
                    )
                    .await?;
                } else {
                    renew_manual_transition_scope_admission_from_job(api, &record).await?;
                }
                return Ok(record);
            }
            Err(Error::PreconditionFailed) => continue,
            Err(err) => return Err(err),
        }
    }
    Err(Error::PreconditionFailed)
}

async fn mark_manual_transition_job_unknown_for_task_journal_error(
    api: Arc<ECStore>,
    job_id: Uuid,
    error: String,
    queue_snapshot: ManualTransitionQueueSnapshot,
) -> EcstoreResult<ManualTransitionJobRecord> {
    for _ in 0..4 {
        let (mut record, etag) = load_manual_transition_job_record_with_etag(api.clone(), job_id).await?;
        if !record.mark_unknown_for_task_journal_error(error.clone(), queue_snapshot) {
            return Ok(record);
        }
        match save_manual_transition_job_record_if_current(api.clone(), &record, &etag).await {
            Ok(()) => {
                delete_manual_transition_scope_admission_if_current(api, &record.scope_key, record.job_id, record.lease_id)
                    .await?;
                return Ok(record);
            }
            Err(Error::PreconditionFailed) => continue,
            Err(err) => return Err(err),
        }
    }
    Err(Error::PreconditionFailed)
}

async fn mark_manual_transition_job_unknown_for_worker_result_journal_error(
    api: Arc<ECStore>,
    job_id: Uuid,
    error: String,
    queue_snapshot: ManualTransitionQueueSnapshot,
) -> EcstoreResult<ManualTransitionJobRecord> {
    for _ in 0..4 {
        let (mut record, etag) = load_manual_transition_job_record_with_etag(api.clone(), job_id).await?;
        if !record.mark_unknown_for_worker_result_journal_error(error.clone(), queue_snapshot) {
            return Ok(record);
        }
        match save_manual_transition_job_record_if_current(api.clone(), &record, &etag).await {
            Ok(()) => {
                delete_manual_transition_scope_admission_if_current(api, &record.scope_key, record.job_id, record.lease_id)
                    .await?;
                return Ok(record);
            }
            Err(Error::PreconditionFailed) => continue,
            Err(err) => return Err(err),
        }
    }
    Err(Error::PreconditionFailed)
}

pub async fn save_manual_transition_scope_admission_if_absent(
    api: Arc<ECStore>,
    admission: &ManualTransitionScopeAdmission,
) -> EcstoreResult<()> {
    admission.validate().map_err(manual_transition_job_store_error)?;
    let object = manual_transition_scope_record_object_name(&admission.scope_key).map_err(manual_transition_job_store_error)?;
    let data = serde_json::to_vec(admission).map_err(Error::other)?;
    config_boundary::save_config_with_opts(
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
    config_boundary::save_config_with_opts(
        api,
        &object,
        data,
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
                Err(Error::ConfigNotFound) => true,
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
    for _ in 0..4 {
        let (mut record, etag) = load_manual_transition_job_record_with_etag(api.clone(), job_id).await?;
        if record.is_terminal() || record.cancel_requested {
            return Ok(record);
        }
        record.mark_cancel_requested();
        match save_manual_transition_job_record_if_current(api.clone(), &record, &etag).await {
            Ok(()) => return Ok(record),
            Err(Error::PreconditionFailed) => continue,
            Err(err) => return Err(err),
        }
    }
    Err(Error::PreconditionFailed)
}

pub async fn persist_manual_transition_job_progress(
    api: Arc<ECStore>,
    job_id: Uuid,
    report: &ManualTransitionRunReport,
    queue_snapshot: ManualTransitionQueueSnapshot,
) -> EcstoreResult<ManualTransitionJobRecord> {
    let (mut record, etag) = load_manual_transition_job_record_with_etag(api.clone(), job_id).await?;
    record.update_running_progress(report.clone(), queue_snapshot);
    save_manual_transition_job_record_if_current(api.clone(), &record, &etag).await?;
    renew_manual_transition_scope_admission_from_job(api, &record).await?;
    Ok(record)
}

pub async fn record_manual_transition_worker_result(
    api: Arc<ECStore>,
    job_id: Uuid,
    task_key: &str,
    result: ManualTransitionWorkerResult,
    queue_snapshot: ManualTransitionQueueSnapshot,
) -> EcstoreResult<ManualTransitionJobRecord> {
    let result_record = ManualTransitionWorkerResultRecord::new(job_id, task_key, result);
    if !save_manual_transition_worker_result_if_absent(api.clone(), &result_record).await? {
        return load_manual_transition_job_record(api, job_id).await;
    }

    for _ in 0..4 {
        let (mut record, etag) = load_manual_transition_job_record_with_etag(api.clone(), job_id).await?;
        if record.is_terminal() {
            return Ok(record);
        }
        record.record_worker_result(result, queue_snapshot);
        match save_manual_transition_job_record_if_current(api.clone(), &record, &etag).await {
            Ok(()) => {
                if record.is_terminal() {
                    delete_manual_transition_scope_admission_if_current(
                        api.clone(),
                        &record.scope_key,
                        record.job_id,
                        record.lease_id,
                    )
                    .await?;
                } else {
                    renew_manual_transition_scope_admission_from_job(api, &record).await?;
                }
                return Ok(record);
            }
            Err(Error::PreconditionFailed) => continue,
            Err(err) => return Err(err),
        }
    }
    Err(Error::PreconditionFailed)
}

pub async fn renew_manual_transition_job_lease(
    api: Arc<ECStore>,
    job_id: Uuid,
    queue_snapshot: ManualTransitionQueueSnapshot,
) -> EcstoreResult<ManualTransitionJobRecord> {
    let (mut record, mut etag) = load_manual_transition_job_record_with_etag(api.clone(), job_id).await?;
    if record.state == ManualTransitionJobState::Running {
        if record.scan_completed && queue_snapshot.queued == 0 && queue_snapshot.active == 0 {
            record = reconcile_manual_transition_worker_results(api.clone(), job_id, queue_snapshot).await?;
            if record.is_terminal() || !record.report.worker_transition_pending() {
                return Ok(record);
            }
            (record, etag) = load_manual_transition_job_record_with_etag(api.clone(), job_id).await?;
        }
        let became_terminal = record.mark_unknown_if_worker_results_lost(queue_snapshot);
        if !became_terminal {
            record.renew_lease(queue_snapshot);
        }
        save_manual_transition_job_record_if_current(api.clone(), &record, &etag).await?;
        if became_terminal {
            delete_manual_transition_scope_admission_if_current(api, &record.scope_key, record.job_id, record.lease_id).await?;
        } else {
            renew_manual_transition_scope_admission_from_job(api, &record).await?;
        }
    }
    Ok(record)
}

async fn renew_manual_transition_scope_admission_from_job(
    api: Arc<ECStore>,
    record: &ManualTransitionJobRecord,
) -> EcstoreResult<()> {
    if let Ok((admission, admission_etag)) =
        load_manual_transition_scope_admission_with_etag(api.clone(), &record.scope_key).await
        && admission.job_id == record.job_id
        && admission.lease_id == record.lease_id
    {
        let renewed_admission = ManualTransitionScopeAdmission::from_job(record);
        save_manual_transition_scope_admission_if_current(api, &renewed_admission, &admission_etag).await?;
    }
    Ok(())
}

pub async fn delete_manual_transition_scope_admission_if_current(
    api: Arc<ECStore>,
    scope_key: &str,
    job_id: Uuid,
    lease_id: Uuid,
) -> EcstoreResult<bool> {
    let etag = match load_manual_transition_scope_admission_with_etag(api.clone(), scope_key).await {
        Ok((admission, etag)) if admission.job_id == job_id && admission.lease_id == lease_id => etag,
        Ok(_) => return Ok(false),
        Err(Error::ConfigNotFound) => return Ok(true),
        Err(err) => return Err(err),
    };
    let object = manual_transition_scope_record_object_name(scope_key).map_err(manual_transition_job_store_error)?;
    match config_boundary::delete_config_if_match(api, &object, &etag).await {
        Ok(()) | Err(Error::ConfigNotFound) => Ok(true),
        Err(Error::PreconditionFailed) => Ok(false),
        Err(err) => Err(err),
    }
}

fn manual_transition_job_store_error(err: ManualTransitionJobError) -> Error {
    Error::other(err)
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
    fn manual_transition_job_record_failure_counts_tier_failure() {
        let options = ManualTransitionRunOptions::default();
        let mut record = ManualTransitionJobRecord::new(Uuid::new_v4(), "bucket", &options, TEST_OWNER);

        record.fail("missing tier");

        assert_eq!(record.state, ManualTransitionJobState::Failed);
        assert_eq!(record.report.tier_failure, 1);
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
