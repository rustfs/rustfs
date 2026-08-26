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

//! Bulk DEK rekey sweep: walk stored objects and rewrap their KMS-wrapped
//! data keys onto current master key versions, touching only `xl.meta`.
//!
//! The sweep is convergent rather than checkpointed: every step is idempotent
//! (an already-current envelope costs one describe-shaped KMS call and no
//! write), so recovery from a crash, a cancel, or partial failure is simply
//! running the sweep again. Failures are counted and logged per object, never
//! silently dropped, and never abort the sweep — a sweep that stopped at the
//! first unreadable object would hide every object behind it.
//!
//! One sweep runs at a time. Concurrent sweeps would double every KMS
//! round-trip for zero extra coverage and interleave their metadata writes.
//!
//! Cross-site note: replication strips encryption metadata in transit, so a
//! rewrap here never propagates to a replica site — each site runs its own
//! sweep.

use crate::storage_api::kms::contract::bucket::{BucketOperations, BucketOptions};
use crate::storage_api::kms::contract::list::ListOperations;
use crate::storage_api::kms::contract::object::ObjectOperations;
use crate::storage_api::kms::{ECStore, ObjectDekRewrapOutcome, StorageObjectOptions, rewrap_object_encryption_metadata};
use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

const LOG_COMPONENT: &str = "kms";
const LOG_SUBSYSTEM: &str = "rekey";

/// Terminal and non-terminal states of a sweep.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum RekeyJobState {
    Running,
    Completed,
    Cancelled,
}

/// Wire shape of the status response; also the internal progress record.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct RekeyJobSnapshot {
    pub job_id: String,
    pub state: RekeyJobState,
    /// Buckets the sweep covers, in sweep order.
    pub buckets: Vec<String>,
    /// Bucket currently being walked; `None` before the first and after the last.
    pub current_bucket: Option<String>,
    /// Object versions the walk yielded.
    pub scanned: u64,
    /// Envelopes rewrapped and persisted.
    pub rewrapped: u64,
    /// Envelopes already on the current version and format.
    pub already_current: u64,
    /// Versions with nothing to rewrap (plaintext, SSE-C, MinIO-sealed).
    pub not_applicable: u64,
    /// Versions whose rewrap or metadata write failed; details are in the log.
    /// A re-run retries exactly these, because everything else converges to
    /// `already_current`.
    pub failed: u64,
}

struct RekeyJob {
    id: String,
    cancel: CancellationToken,
    buckets: Vec<String>,
    scanned: AtomicU64,
    rewrapped: AtomicU64,
    already_current: AtomicU64,
    not_applicable: AtomicU64,
    failed: AtomicU64,
    /// `(state, current_bucket)` under one lock so a snapshot never pairs a
    /// terminal state with a bucket still marked in progress.
    progress: Mutex<(RekeyJobState, Option<String>)>,
}

impl RekeyJob {
    fn snapshot(&self) -> RekeyJobSnapshot {
        let (state, current_bucket) = self.progress.lock().expect("rekey progress lock").clone();
        RekeyJobSnapshot {
            job_id: self.id.clone(),
            state,
            buckets: self.buckets.clone(),
            current_bucket,
            scanned: self.scanned.load(Ordering::Relaxed),
            rewrapped: self.rewrapped.load(Ordering::Relaxed),
            already_current: self.already_current.load(Ordering::Relaxed),
            not_applicable: self.not_applicable.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
        }
    }

    fn set_progress(&self, state: RekeyJobState, current_bucket: Option<String>) {
        *self.progress.lock().expect("rekey progress lock") = (state, current_bucket);
    }
}

/// The single sweep slot. Holding the finished job (rather than clearing it)
/// keeps the final counters queryable until the next sweep starts.
static ACTIVE_JOB: LazyLock<Mutex<Option<Arc<RekeyJob>>>> = LazyLock::new(|| Mutex::new(None));

/// Why a sweep could not be started.
#[derive(Debug)]
pub(crate) enum RekeyStartError {
    /// A sweep is already running; its id is carried for the error message.
    AlreadyRunning(String),
    /// The requested bucket list could not be resolved.
    Storage(String),
}

/// Start a sweep over `buckets` (all buckets when `None`) under `prefix`.
///
/// Returns the job id. The sweep runs on a background task; progress and the
/// terminal state are read through [`status`].
pub(crate) async fn start(
    store: Arc<ECStore>,
    buckets: Option<Vec<String>>,
    prefix: String,
) -> Result<RekeyJobSnapshot, RekeyStartError> {
    let buckets = match buckets {
        Some(buckets) if !buckets.is_empty() => buckets,
        _ => store
            .list_bucket(&BucketOptions::default())
            .await
            .map_err(|e| RekeyStartError::Storage(format!("failed to list buckets: {e}")))?
            .into_iter()
            .map(|bucket| bucket.name)
            .collect(),
    };

    let job = {
        let mut slot = ACTIVE_JOB.lock().expect("rekey job slot lock");
        if let Some(existing) = slot.as_ref()
            && existing.snapshot().state == RekeyJobState::Running
        {
            return Err(RekeyStartError::AlreadyRunning(existing.id.clone()));
        }
        let job = Arc::new(RekeyJob {
            id: uuid::Uuid::new_v4().to_string(),
            cancel: CancellationToken::new(),
            buckets,
            scanned: AtomicU64::new(0),
            rewrapped: AtomicU64::new(0),
            already_current: AtomicU64::new(0),
            not_applicable: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            progress: Mutex::new((RekeyJobState::Running, None)),
        });
        *slot = Some(job.clone());
        job
    };

    info!(
        component = LOG_COMPONENT,
        subsystem = LOG_SUBSYSTEM,
        event = "kms_rekey_started",
        job_id = %job.id,
        buckets = job.buckets.len(),
        "KMS rekey sweep started"
    );
    let task_job = job.clone();
    tokio::spawn(async move { run_sweep(store, task_job, prefix).await });

    Ok(job.snapshot())
}

/// Progress of the current or most recent sweep, if any.
pub(crate) fn status() -> Option<RekeyJobSnapshot> {
    ACTIVE_JOB
        .lock()
        .expect("rekey job slot lock")
        .as_ref()
        .map(|job| job.snapshot())
}

/// Request cancellation of the running sweep. Returns the snapshot the caller
/// can report, or `None` when no sweep exists.
pub(crate) fn cancel() -> Option<RekeyJobSnapshot> {
    let slot = ACTIVE_JOB.lock().expect("rekey job slot lock");
    let job = slot.as_ref()?;
    job.cancel.cancel();
    Some(job.snapshot())
}

async fn run_sweep(store: Arc<ECStore>, job: Arc<RekeyJob>, prefix: String) {
    for bucket in job.buckets.clone() {
        if job.cancel.is_cancelled() {
            break;
        }
        job.set_progress(RekeyJobState::Running, Some(bucket.clone()));
        sweep_bucket(&store, &job, &bucket, &prefix).await;
    }

    let state = if job.cancel.is_cancelled() {
        RekeyJobState::Cancelled
    } else {
        RekeyJobState::Completed
    };
    job.set_progress(state, None);
    let snapshot = job.snapshot();
    info!(
        component = LOG_COMPONENT,
        subsystem = LOG_SUBSYSTEM,
        event = "kms_rekey_finished",
        job_id = %job.id,
        state = ?snapshot.state,
        scanned = snapshot.scanned,
        rewrapped = snapshot.rewrapped,
        already_current = snapshot.already_current,
        not_applicable = snapshot.not_applicable,
        failed = snapshot.failed,
        "KMS rekey sweep finished"
    );
}

async fn sweep_bucket(store: &Arc<ECStore>, job: &Arc<RekeyJob>, bucket: &str, prefix: &str) {
    let (tx, mut rx) = tokio::sync::mpsc::channel(256);
    let walk_cancel = job.cancel.child_token();
    // Every version's envelope needs rewrapping, not just the latest.
    type WalkOptionsOf = <ECStore as ListOperations>::WalkOptions;
    let walk_options = WalkOptionsOf {
        latest_only: false,
        ..Default::default()
    };
    let walk_store = store.clone();
    let walk_bucket = bucket.to_string();
    let walk_prefix = prefix.to_string();
    let walk_task = tokio::spawn(async move {
        walk_store
            .walk(walk_cancel, &walk_bucket, &walk_prefix, tx, walk_options)
            .await
    });

    while let Some(entry) = rx.recv().await {
        if job.cancel.is_cancelled() {
            break;
        }
        if let Some(error) = entry.err {
            job.failed.fetch_add(1, Ordering::Relaxed);
            warn!(
                component = LOG_COMPONENT,
                subsystem = LOG_SUBSYSTEM,
                event = "kms_rekey_walk_entry_failed",
                job_id = %job.id,
                bucket,
                error = %error,
                "KMS rekey sweep could not list an object"
            );
            continue;
        }
        let Some(object_info) = entry.item else {
            continue;
        };
        if object_info.delete_marker || object_info.is_dir {
            continue;
        }
        job.scanned.fetch_add(1, Ordering::Relaxed);

        let object = object_info.name.clone();
        match rewrap_object_encryption_metadata(bucket, &object, object_info.user_defined.as_ref()).await {
            Ok(ObjectDekRewrapOutcome::NotApplicable) => {
                job.not_applicable.fetch_add(1, Ordering::Relaxed);
            }
            Ok(ObjectDekRewrapOutcome::AlreadyCurrent) => {
                job.already_current.fetch_add(1, Ordering::Relaxed);
            }
            Ok(ObjectDekRewrapOutcome::Rewrapped { metadata }) => {
                let options = StorageObjectOptions {
                    version_id: object_info.version_id.map(|version| version.to_string()),
                    eval_metadata: Some(metadata),
                    ..Default::default()
                };
                match store.put_object_metadata(bucket, &object, &options).await {
                    Ok(_) => {
                        job.rewrapped.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(error) => {
                        job.failed.fetch_add(1, Ordering::Relaxed);
                        warn!(
                            component = LOG_COMPONENT,
                            subsystem = LOG_SUBSYSTEM,
                            event = "kms_rekey_persist_failed",
                            job_id = %job.id,
                            bucket,
                            object = %object,
                            error = %error,
                            "KMS rekey sweep rewrapped an envelope but could not persist it; the object keeps its \
                             previous (still valid) wrapping and a re-run retries it"
                        );
                    }
                }
            }
            Err(error) => {
                job.failed.fetch_add(1, Ordering::Relaxed);
                warn!(
                    component = LOG_COMPONENT,
                    subsystem = LOG_SUBSYSTEM,
                    event = "kms_rekey_object_failed",
                    job_id = %job.id,
                    bucket,
                    object = %object,
                    error = %error,
                    "KMS rekey sweep could not rewrap an object's data key; a re-run retries it"
                );
            }
        }
    }
    // Dropping the receiver ends the walk on early break; a cancelled walk is
    // not a sweep failure.
    drop(rx);
    match walk_task.await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            if !job.cancel.is_cancelled() {
                job.failed.fetch_add(1, Ordering::Relaxed);
                warn!(
                    component = LOG_COMPONENT,
                    subsystem = LOG_SUBSYSTEM,
                    event = "kms_rekey_walk_failed",
                    job_id = %job.id,
                    bucket,
                    error = %error,
                    "KMS rekey sweep walk ended with an error; coverage of this bucket is incomplete"
                );
            }
        }
        Err(error) => {
            job.failed.fetch_add(1, Ordering::Relaxed);
            warn!(
                component = LOG_COMPONENT,
                subsystem = LOG_SUBSYSTEM,
                event = "kms_rekey_walk_failed",
                job_id = %job.id,
                bucket,
                error = %error,
                "KMS rekey sweep walk task failed; coverage of this bucket is incomplete"
            );
        }
    }
}
