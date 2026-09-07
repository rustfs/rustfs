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
/// Scanner cycle-state codec, persisted usage floors, and cycle-state persistence.
use super::*;
use crate::ScannerGetObjectReader;
use crate::data_usage_define::{
    DATA_USAGE_BLOOM_RECOVERY_PATH, DATA_USAGE_RECOVERY_PATH, usage_floor_primary_read_error_allows_backup,
};
use crate::storage_api::owner::ObjectIO as _;
use std::sync::atomic::AtomicU64;
use tokio::io::AsyncReadExt as _;

const SCANNER_CYCLE_RECOVERY_SCHEMA_VERSION: u16 = 1;
const MAX_SCANNER_CYCLE_STATE_BYTES: u64 = 1024 * 1024;
pub(super) const MAX_SCANNER_CYCLE_RECOVERY_RETRIES: u32 = 5;
const METRIC_SCANNER_CYCLE_RECOVERY_REQUIRED: &str = "rustfs_scanner_cycle_recovery_required";
const METRIC_SCANNER_CYCLE_RECOVERY_RETRY_COUNT: &str = "rustfs_scanner_cycle_recovery_retry_count";
const USAGE_FLOOR_LOAD_FAILED: &str = "usage_floor_load_failed";
// Keep the published status value stable for operators that already alert on
// the empty-fence recovery introduced by backlog-2102. The same durable marker
// now also covers strictly validated data-bearing legacy fences.
const LEGACY_INCOMPLETE_USAGE_FLOOR_RECOVERY: &str = "legacy_empty_usage_floor";
const CACHE_CYCLE_AHEAD: &str = "cache_cycle_ahead";

const SCANNER_USAGE_STATE_RESET_MODE_FULL_REBUILD: &str = "full-rebuild";
const SCANNER_RECOVERY_INTENT_SCHEMA_VERSION: u16 = 1;
const SCANNER_RECOVERY_INTENT_PREFIX: &str = ".usage.v2.recovery-intents";
const MAX_SCANNER_RECOVERY_INTENT_BYTES: u64 = 16 * 1024;
const SCANNER_RECOVERY_INTENT_STATE_ACCEPTED: &str = "accepted";
const SCANNER_RECOVERY_INTENT_STATE_RUNNING: &str = "running";
const SCANNER_RECOVERY_INTENT_STATE_COMPLETED: &str = "completed";
const SCANNER_RECOVERY_INTENT_STATE_FAILED: &str = "failed";
pub const SCANNER_RECOVERY_INTENT_ACTION_USAGE_FULL_REBUILD: &str = "scanner-usage-full-rebuild";

#[cfg(test)]
pub(super) mod cleanup_io_fault {
    use super::*;

    #[derive(Clone, Copy, PartialEq, Eq)]
    pub(in crate::scanner) enum Stage {
        PrimaryRead,
        PrimaryWrite,
        UsageFence,
    }

    struct Injection {
        store: std::sync::Weak<ECStore>,
        stage: Stage,
        fired: AtomicBool,
        owned: AtomicBool,
        newer_completion: bool,
    }

    static INJECTION: StdMutex<Option<Arc<Injection>>> = StdMutex::new(None);
    pub(in crate::scanner) struct Guard(Arc<Injection>);

    impl Guard {
        pub(in crate::scanner) fn fired_while_owned(&self) -> bool {
            self.0.fired.load(Ordering::Relaxed) && self.0.owned.load(Ordering::Relaxed)
        }
    }

    impl Drop for Guard {
        fn drop(&mut self) {
            let mut slot = INJECTION.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            if slot.as_ref().is_some_and(|current| Arc::ptr_eq(current, &self.0)) {
                *slot = None;
            }
        }
    }

    pub(in crate::scanner) fn install(store: &Arc<ECStore>, stage: Stage, newer_completion: bool) -> Guard {
        let injection = Arc::new(Injection {
            store: Arc::downgrade(store),
            stage,
            fired: AtomicBool::new(false),
            owned: AtomicBool::new(false),
            newer_completion,
        });
        let mut slot = INJECTION.lock().expect("cleanup injection slot");
        assert!(slot.is_none(), "only one cleanup I/O injection may be installed");
        *slot = Some(injection.clone());
        Guard(injection)
    }

    pub(super) fn check(store: &Arc<ECStore>, stage: Stage, owned: bool) -> Result<(), ScannerError> {
        let injection = {
            let mut slot = INJECTION.lock().expect("cleanup injection slot");
            if slot
                .as_ref()
                .is_some_and(|injection| injection.stage == stage && injection.store.ptr_eq(&Arc::downgrade(store)))
            {
                slot.take()
            } else {
                None
            }
        };
        let Some(injection) = injection else {
            return Ok(());
        };
        injection.fired.store(true, Ordering::Relaxed);
        injection.owned.store(owned, Ordering::Relaxed);
        if injection.newer_completion {
            set_scanner_cycle_recovery_status(recovery_status("healthy", None, false));
        }
        let reason = match stage {
            Stage::PrimaryRead => "injected primary read failure",
            Stage::PrimaryWrite => "injected primary write failure",
            Stage::UsageFence => "injected usage fence failure",
        };
        Err(ScannerError::Io(std::io::Error::other(reason)))
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct ScannerCycleRecoveryStatus {
    /// The immutable primary object whose revision is being guarded.
    pub path: String,
    /// The companion marker/quarantine object containing the recovery evidence.
    pub quarantine_path: Option<String>,
    pub state: String,
    pub classification: Option<String>,
    pub primary_revision: Option<String>,
    pub generation: Option<u64>,
    pub leader_epoch: Option<u64>,
    pub first_detected_at_unix_secs: Option<u64>,
    pub last_attempt_at_unix_secs: Option<u64>,
    pub retry_count: u64,
    /// Maximum automatic retries, or zero when the recovery is unbounded.
    pub max_retries: u32,
    /// Whether the scanner may retry this state automatically.
    pub retryable: bool,
    pub reason: Option<String>,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ScannerUsageStateResetResult {
    pub status: String,
    pub mode: String,
    pub usage_state: String,
    pub leader_epoch: u64,
    pub next_cycle: u64,
    pub reset_paths: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ScannerRecoveryIntentRequest {
    pub action: String,
    pub mode: String,
    pub idempotency_key: String,
    pub actor_sha256: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ScannerRecoveryIntentRecord {
    pub schema_version: u16,
    pub intent_id: String,
    pub action: String,
    pub mode: String,
    pub state: String,
    pub actor_sha256: String,
    pub idempotency_key_sha256: String,
    pub request_sha256: String,
    pub accepted_at_unix_secs: u64,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ScannerRecoveryIntentConflict {
    pub intent_id: String,
    pub state: String,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum ScannerRecoveryIntentAcceptResult {
    Accepted { record: ScannerRecoveryIntentRecord },
    Replayed { record: ScannerRecoveryIntentRecord },
    Conflict { existing: ScannerRecoveryIntentConflict },
}

#[derive(Clone, Debug)]
pub(super) struct ScannerUsageStateResetSlot {
    path: String,
    data: Option<Vec<u8>>,
    revision: DataUsageCacheRevision,
}

static SCANNER_CYCLE_RECOVERY_STATUS: LazyLock<RwLock<ScannerCycleRecoveryStatus>> = LazyLock::new(|| {
    RwLock::new(ScannerCycleRecoveryStatus {
        path: DATA_USAGE_BLOOM_NAME_PATH.clone(),
        quarantine_path: Some(DATA_USAGE_BLOOM_RECOVERY_PATH.clone()),
        state: "healthy".to_string(),
        max_retries: MAX_SCANNER_CYCLE_RECOVERY_RETRIES,
        ..Default::default()
    })
});
// An old startup observation must not overwrite a newer explicit reset status.
static SCANNER_CYCLE_RECOVERY_STATUS_VERSION: AtomicU64 = AtomicU64::new(0);

pub fn scanner_cycle_recovery_status() -> ScannerCycleRecoveryStatus {
    SCANNER_CYCLE_RECOVERY_STATUS
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}

fn set_scanner_cycle_recovery_status(status: ScannerCycleRecoveryStatus) {
    let _ = publish_scanner_cleanup_status(status, None);
}

pub(super) fn scanner_cleanup_status_version() -> u64 {
    let _status = SCANNER_CYCLE_RECOVERY_STATUS
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    SCANNER_CYCLE_RECOVERY_STATUS_VERSION.load(Ordering::Relaxed)
}

pub(super) fn publish_scanner_cleanup_status(status: ScannerCycleRecoveryStatus, expected: Option<u64>) -> Option<u64> {
    let mut current = SCANNER_CYCLE_RECOVERY_STATUS
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let version = SCANNER_CYCLE_RECOVERY_STATUS_VERSION.load(Ordering::Relaxed);
    if expected.is_some_and(|expected| expected == u64::MAX || expected != version) {
        return None;
    }
    let recovery_required = if matches!(
        status.state.as_str(),
        "blocked"
            | "paused"
            | "recovery-required"
            | "cleanup-pending"
            | "usage_floor_load_failed"
            | "usage_floor_recovery_pending"
            | "cache_cycle_ahead"
    ) {
        1.0
    } else {
        0.0
    };
    metrics::gauge!(METRIC_SCANNER_CYCLE_RECOVERY_REQUIRED).set(recovery_required);
    metrics::gauge!(METRIC_SCANNER_CYCLE_RECOVERY_RETRY_COUNT).set(status.retry_count as f64);
    *current = status;
    let next = version.saturating_add(1);
    SCANNER_CYCLE_RECOVERY_STATUS_VERSION.store(next, Ordering::Relaxed);
    Some(next)
}

fn publish_scanner_cleanup_failure(reason: String, expected: u64) {
    let mut status = {
        let current = SCANNER_CYCLE_RECOVERY_STATUS
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if SCANNER_CYCLE_RECOVERY_STATUS_VERSION.load(Ordering::Relaxed) != expected {
            return;
        }
        current.clone()
    };
    // Preserve the latest core progress, including a newly written primary's
    // revision and epoch. The original marker may predate that durable write.
    status.reason = Some(reason);
    status.last_attempt_at_unix_secs = Some(unix_now_secs());
    let _ = publish_scanner_cleanup_status(status, Some(expected));
}

pub(super) fn record_scanner_usage_floor_failure(reason: String) {
    let previous = scanner_cycle_recovery_status();
    let same_failure = previous.classification.as_deref() == Some(USAGE_FLOOR_LOAD_FAILED);
    let now = unix_now_secs();
    let (first_detected_at_unix_secs, retry_count) = if same_failure {
        (previous.first_detected_at_unix_secs.or(Some(now)), previous.retry_count)
    } else {
        (Some(now), 0)
    };
    set_scanner_cycle_recovery_status(ScannerCycleRecoveryStatus {
        path: DATA_USAGE_OBJ_NAME_PATH.clone(),
        state: USAGE_FLOOR_LOAD_FAILED.to_string(),
        classification: Some(USAGE_FLOOR_LOAD_FAILED.to_string()),
        first_detected_at_unix_secs,
        last_attempt_at_unix_secs: Some(now),
        retry_count,
        max_retries: MAX_SCANNER_CYCLE_RECOVERY_RETRIES,
        retryable: true,
        reason: Some(reason),
        ..Default::default()
    });
}

pub(super) fn clear_scanner_usage_floor_failure() {
    if scanner_cycle_recovery_status().classification.as_deref() == Some(USAGE_FLOOR_LOAD_FAILED) {
        set_scanner_cycle_recovery_status(recovery_status("healthy", None, false));
    }
}

pub(super) fn record_scanner_cache_cycle_ahead(requested_cycle: u64, required_cycle: u64, leader_epoch: u64) {
    let previous = scanner_cycle_recovery_status();
    let same_floor = previous.classification.as_deref() == Some(CACHE_CYCLE_AHEAD)
        && previous.generation == Some(required_cycle)
        && previous.leader_epoch == Some(leader_epoch);
    let now = unix_now_secs();
    let (first_detected_at_unix_secs, retry_count) = if same_floor {
        (previous.first_detected_at_unix_secs.or(Some(now)), previous.retry_count)
    } else {
        (Some(now), 0)
    };
    set_scanner_cycle_recovery_status(ScannerCycleRecoveryStatus {
        path: DATA_USAGE_BLOOM_NAME_PATH.clone(),
        state: CACHE_CYCLE_AHEAD.to_string(),
        classification: Some(CACHE_CYCLE_AHEAD.to_string()),
        generation: Some(required_cycle),
        leader_epoch: Some(leader_epoch),
        first_detected_at_unix_secs,
        last_attempt_at_unix_secs: Some(now),
        retry_count,
        max_retries: 0,
        retryable: true,
        reason: Some(format!(
            "persisted scanner cache cycle {required_cycle} is ahead of requested cycle {requested_cycle}"
        )),
        ..Default::default()
    });
}

pub(super) fn record_scanner_cache_cycle_recovery_attempt() {
    let mut status = scanner_cycle_recovery_status();
    if status.classification.as_deref() != Some(CACHE_CYCLE_AHEAD) {
        return;
    }
    status.retry_count = status.retry_count.saturating_add(1);
    status.last_attempt_at_unix_secs = Some(unix_now_secs());
    set_scanner_cycle_recovery_status(status);
}

pub(super) fn clear_scanner_cache_cycle_ahead() {
    if scanner_cycle_recovery_status().classification.as_deref() == Some(CACHE_CYCLE_AHEAD) {
        set_scanner_cycle_recovery_status(recovery_status("healthy", None, false));
    }
}

pub(super) fn record_legacy_incomplete_usage_floor_recovery_pending(leader_epoch: u64) {
    let previous = scanner_cycle_recovery_status();
    let same_recovery = previous.classification.as_deref() == Some(LEGACY_INCOMPLETE_USAGE_FLOOR_RECOVERY)
        && previous.leader_epoch == Some(leader_epoch);
    let now = unix_now_secs();
    let (first_detected_at_unix_secs, retry_count) = if same_recovery {
        (previous.first_detected_at_unix_secs.or(Some(now)), previous.retry_count)
    } else {
        (Some(now), 0)
    };
    set_scanner_cycle_recovery_status(ScannerCycleRecoveryStatus {
        path: DATA_USAGE_OBJ_NAME_PATH.clone(),
        quarantine_path: Some(DATA_USAGE_RECOVERY_PATH.clone()),
        state: "usage_floor_recovery_pending".to_string(),
        classification: Some(LEGACY_INCOMPLETE_USAGE_FLOOR_RECOVERY.to_string()),
        leader_epoch: Some(leader_epoch),
        first_detected_at_unix_secs,
        last_attempt_at_unix_secs: Some(now),
        retry_count,
        max_retries: MAX_SCANNER_CYCLE_RECOVERY_RETRIES,
        retryable: true,
        reason: Some("legacy incomplete usage floor recovery is awaiting a fenced leadership claim".to_string()),
        ..Default::default()
    });
}

pub(super) fn clear_legacy_incomplete_usage_floor_recovery_status() {
    if scanner_cycle_recovery_status().classification.as_deref() == Some(LEGACY_INCOMPLETE_USAGE_FLOOR_RECOVERY) {
        set_scanner_cycle_recovery_status(recovery_status("healthy", None, false));
    }
}

pub(super) fn record_scanner_cycle_recovery_retry(attempt: u32) -> bool {
    let mut status = scanner_cycle_recovery_status();
    if status.classification.as_deref() == Some(CACHE_CYCLE_AHEAD) {
        return true;
    }
    status.retry_count = status.retry_count.max(u64::from(attempt));
    status.last_attempt_at_unix_secs = Some(unix_now_secs());
    if status.max_retries != 0 && status.retry_count >= u64::from(status.max_retries) {
        status.state = "paused".to_string();
        status.retryable = false;
        status.reason = Some("scanner cycle recovery retry budget reached; sparse backend probes continue".to_string());
        set_scanner_cycle_recovery_status(status);
        false
    } else {
        status.retryable = true;
        set_scanner_cycle_recovery_status(status);
        true
    }
}

fn unix_now_secs() -> u64 {
    u64::try_from(Utc::now().timestamp()).unwrap_or(0)
}

fn sha256_hex(parts: &[&[u8]]) -> String {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part);
        hasher.update([0]);
    }
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let digest = hasher.finalize();
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn is_canonical_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn validate_idempotency_key(key: &str) -> Result<(), ScannerError> {
    if !(8..=256).contains(&key.len()) {
        return Err(ScannerError::Other(
            "scanner recovery intent idempotency key length is unsupported".to_string(),
        ));
    }
    if !key
        .bytes()
        .all(|byte| byte.is_ascii() && !byte.is_ascii_control() && !byte.is_ascii_whitespace())
    {
        return Err(ScannerError::Other(
            "scanner recovery intent idempotency key must be printable ASCII without whitespace".to_string(),
        ));
    }
    Ok(())
}

fn validate_recovery_intent_request(request: &ScannerRecoveryIntentRequest) -> Result<(), ScannerError> {
    if request.action != SCANNER_RECOVERY_INTENT_ACTION_USAGE_FULL_REBUILD {
        return Err(ScannerError::Other("scanner recovery intent action is unsupported".to_string()));
    }
    if request.mode != SCANNER_USAGE_STATE_RESET_MODE_FULL_REBUILD {
        return Err(ScannerError::Other("scanner recovery intent mode is unsupported".to_string()));
    }
    if !is_canonical_sha256(&request.actor_sha256) {
        return Err(ScannerError::Other("scanner recovery intent actor identity is invalid".to_string()));
    }
    validate_idempotency_key(&request.idempotency_key)
}

fn scanner_recovery_intent_path(intent_id: &str) -> Result<String, ScannerError> {
    if !is_canonical_sha256(intent_id) {
        return Err(ScannerError::Other("scanner recovery intent id is invalid".to_string()));
    }
    Ok(format!("{SCANNER_RECOVERY_INTENT_PREFIX}/{intent_id}.json"))
}

pub fn scanner_recovery_actor_sha256(actor: &str) -> String {
    sha256_hex(&[b"scanner-recovery-actor-v1", actor.as_bytes()])
}

fn scanner_recovery_intent_candidate(
    request: &ScannerRecoveryIntentRequest,
) -> Result<ScannerRecoveryIntentRecord, ScannerError> {
    validate_recovery_intent_request(request)?;
    let intent_id = sha256_hex(&[
        b"scanner-recovery-intent-v1",
        request.actor_sha256.as_bytes(),
        request.idempotency_key.as_bytes(),
    ]);
    Ok(ScannerRecoveryIntentRecord {
        schema_version: SCANNER_RECOVERY_INTENT_SCHEMA_VERSION,
        intent_id,
        action: request.action.clone(),
        mode: request.mode.clone(),
        state: SCANNER_RECOVERY_INTENT_STATE_ACCEPTED.to_string(),
        actor_sha256: request.actor_sha256.clone(),
        idempotency_key_sha256: sha256_hex(&[b"scanner-recovery-idempotency-key-v1", request.idempotency_key.as_bytes()]),
        request_sha256: sha256_hex(&[
            b"scanner-recovery-request-v1",
            request.actor_sha256.as_bytes(),
            request.action.as_bytes(),
            request.mode.as_bytes(),
            request.idempotency_key.as_bytes(),
        ]),
        accepted_at_unix_secs: unix_now_secs(),
    })
}

fn validate_recovery_intent_record(record: &ScannerRecoveryIntentRecord) -> Result<(), ScannerError> {
    if record.schema_version != SCANNER_RECOVERY_INTENT_SCHEMA_VERSION {
        return Err(ScannerError::Other("scanner recovery intent schema is unsupported".to_string()));
    }
    if !is_canonical_sha256(&record.intent_id)
        || !is_canonical_sha256(&record.actor_sha256)
        || !is_canonical_sha256(&record.idempotency_key_sha256)
        || !is_canonical_sha256(&record.request_sha256)
    {
        return Err(ScannerError::Other("scanner recovery intent identity is invalid".to_string()));
    }
    if record.action != SCANNER_RECOVERY_INTENT_ACTION_USAGE_FULL_REBUILD
        || record.mode != SCANNER_USAGE_STATE_RESET_MODE_FULL_REBUILD
        || !matches!(
            record.state.as_str(),
            SCANNER_RECOVERY_INTENT_STATE_ACCEPTED
                | SCANNER_RECOVERY_INTENT_STATE_RUNNING
                | SCANNER_RECOVERY_INTENT_STATE_COMPLETED
                | SCANNER_RECOVERY_INTENT_STATE_FAILED
        )
    {
        return Err(ScannerError::Other("scanner recovery intent state is invalid".to_string()));
    }
    Ok(())
}

fn decode_recovery_intent_record(data: &[u8]) -> Result<ScannerRecoveryIntentRecord, ScannerError> {
    let record: ScannerRecoveryIntentRecord =
        serde_json::from_slice(data).map_err(|err| ScannerError::Other(format!("scanner recovery intent is invalid: {err}")))?;
    validate_recovery_intent_record(&record)?;
    Ok(record)
}

fn compare_recovery_intent(
    expected: &ScannerRecoveryIntentRecord,
    existing: ScannerRecoveryIntentRecord,
) -> ScannerRecoveryIntentAcceptResult {
    if existing.actor_sha256 == expected.actor_sha256
        && existing.action == expected.action
        && existing.mode == expected.mode
        && existing.idempotency_key_sha256 == expected.idempotency_key_sha256
        && existing.request_sha256 == expected.request_sha256
    {
        ScannerRecoveryIntentAcceptResult::Replayed { record: existing }
    } else {
        ScannerRecoveryIntentAcceptResult::Conflict {
            existing: ScannerRecoveryIntentConflict {
                intent_id: existing.intent_id,
                state: existing.state,
            },
        }
    }
}

async fn read_recovery_intent_record(
    storeapi: Arc<impl ScannerObjectIO>,
    path: &str,
) -> Result<Option<ScannerRecoveryIntentRecord>, ScannerError> {
    read_recovery_intent_record_with_revision(storeapi, path)
        .await
        .map(|record| record.map(|(record, _)| record))
}

async fn read_recovery_intent_record_with_revision(
    storeapi: Arc<impl ScannerObjectIO>,
    path: &str,
) -> Result<Option<(ScannerRecoveryIntentRecord, DataUsageCacheRevision)>, ScannerError> {
    let mut reader = match storeapi
        .get_object_reader(
            RUSTFS_META_BUCKET,
            path,
            None,
            http::HeaderMap::new(),
            &ScannerObjectOptions {
                no_lock: true,
                ..Default::default()
            },
        )
        .await
    {
        Ok(reader) => reader,
        Err(
            EcstoreError::FileNotFound
            | EcstoreError::VolumeNotFound
            | EcstoreError::ObjectNotFound(_, _)
            | EcstoreError::BucketNotFound(_)
            | EcstoreError::ConfigNotFound,
        ) => return Ok(None),
        Err(err) => return Err(ScannerError::Other(format!("failed to read scanner recovery intent: {err}"))),
    };
    let revision = reader
        .object_info
        .etag
        .as_ref()
        .filter(|etag| !etag.is_empty())
        .cloned()
        .map(DataUsageCacheRevision::Etag)
        .ok_or_else(|| ScannerError::Other("scanner recovery intent has no revision".to_string()))?;
    let max_object_size = i64::try_from(MAX_SCANNER_RECOVERY_INTENT_BYTES).unwrap_or(i64::MAX);
    if reader.object_info.is_dir || reader.object_info.size < 0 || reader.object_info.size > max_object_size {
        return Err(ScannerError::Other("scanner recovery intent exceeds the bounded object size".to_string()));
    }
    let max_len = usize::try_from(MAX_SCANNER_RECOVERY_INTENT_BYTES).unwrap_or(usize::MAX);
    let mut data = Vec::new();
    (&mut reader)
        .take(MAX_SCANNER_RECOVERY_INTENT_BYTES.saturating_add(1))
        .read_to_end(&mut data)
        .await
        .map_err(|err| ScannerError::Other(format!("failed to read scanner recovery intent: {err}")))?;
    if data.is_empty() {
        return Err(ScannerError::Other("scanner recovery intent is empty".to_string()));
    }
    if data.len() > max_len {
        return Err(ScannerError::Other("scanner recovery intent exceeds the bounded object size".to_string()));
    }
    decode_recovery_intent_record(&data).map(|record| Some((record, revision)))
}

pub async fn get_scanner_usage_recovery_intent(
    storeapi: Arc<impl ScannerObjectIO>,
    intent_id: &str,
) -> Result<Option<ScannerRecoveryIntentRecord>, ScannerError> {
    let path = scanner_recovery_intent_path(intent_id)?;
    read_recovery_intent_record(storeapi, &path).await
}

pub async fn accept_scanner_usage_recovery_intent(
    storeapi: Arc<impl ScannerObjectIO>,
    request: ScannerRecoveryIntentRequest,
) -> Result<ScannerRecoveryIntentAcceptResult, ScannerError> {
    let candidate = scanner_recovery_intent_candidate(&request)?;
    let path = scanner_recovery_intent_path(&candidate.intent_id)?;
    let encoded = serde_json::to_vec(&candidate)
        .map_err(|err| ScannerError::Other(format!("failed to encode scanner recovery intent: {err}")))?;
    match save_config_with_preconditions(storeapi.clone(), &path, encoded, DataUsageCacheRevision::Missing.preconditions()).await
    {
        Ok(_) => Ok(ScannerRecoveryIntentAcceptResult::Accepted { record: candidate }),
        Err(EcstoreError::PreconditionFailed) => {
            let existing = read_recovery_intent_record(storeapi, &path).await?;
            let Some(existing) = existing else {
                return Err(ScannerError::Other(
                    "scanner recovery intent disappeared after creation conflict".to_string(),
                ));
            };
            Ok(compare_recovery_intent(&candidate, existing))
        }
        Err(err) => Err(ScannerError::Other(format!("failed to persist scanner recovery intent: {err}"))),
    }
}

async fn transition_scanner_usage_recovery_intent(
    storeapi: Arc<ECStore>,
    intent_id: &str,
    expected_states: &[&str],
    next_state: &str,
) -> Result<Option<ScannerRecoveryIntentRecord>, ScannerError> {
    let path = scanner_recovery_intent_path(intent_id)?;
    for _ in 0..8 {
        let Some((mut record, revision)) = read_recovery_intent_record_with_revision(storeapi.clone(), &path).await? else {
            return Err(ScannerError::Other("scanner recovery intent disappeared before execution".to_string()));
        };
        if record.state == next_state {
            return Ok(Some(record));
        }
        if !expected_states.contains(&record.state.as_str()) {
            return Ok(None);
        }
        record.state = next_state.to_string();
        let encoded = serde_json::to_vec(&record)
            .map_err(|err| ScannerError::Other(format!("failed to encode scanner recovery intent: {err}")))?;
        match save_config_with_preconditions(storeapi.clone(), &path, encoded, revision.preconditions()).await {
            Ok(_) => return Ok(Some(record)),
            Err(EcstoreError::PreconditionFailed) => continue,
            Err(err) => return Err(ScannerError::Other(format!("failed to persist scanner recovery intent: {err}"))),
        }
    }
    Err(ScannerError::Other(
        "scanner recovery intent state changed repeatedly during transition".to_string(),
    ))
}

pub async fn run_scanner_usage_recovery_intent(
    ctx: CancellationToken,
    storeapi: Arc<ECStore>,
    intent_id: String,
) -> Result<Option<ScannerUsageStateResetResult>, ScannerError> {
    let Some(record) = transition_scanner_usage_recovery_intent(
        storeapi.clone(),
        &intent_id,
        &[SCANNER_RECOVERY_INTENT_STATE_ACCEPTED, SCANNER_RECOVERY_INTENT_STATE_RUNNING],
        SCANNER_RECOVERY_INTENT_STATE_RUNNING,
    )
    .await?
    else {
        return Ok(None);
    };
    if record.action != SCANNER_RECOVERY_INTENT_ACTION_USAGE_FULL_REBUILD
        || record.mode != SCANNER_USAGE_STATE_RESET_MODE_FULL_REBUILD
    {
        return Err(ScannerError::Other(
            "scanner recovery intent action or mode changed before execution".to_string(),
        ));
    }

    match reset_scanner_usage_state_for_full_rebuild(ctx, storeapi.clone()).await {
        Ok(result) => {
            transition_scanner_usage_recovery_intent(
                storeapi,
                &intent_id,
                &[SCANNER_RECOVERY_INTENT_STATE_RUNNING, SCANNER_RECOVERY_INTENT_STATE_FAILED],
                SCANNER_RECOVERY_INTENT_STATE_COMPLETED,
            )
            .await?;
            Ok(Some(result))
        }
        Err(err) => {
            let _ = transition_scanner_usage_recovery_intent(
                storeapi,
                &intent_id,
                &[SCANNER_RECOVERY_INTENT_STATE_RUNNING],
                SCANNER_RECOVERY_INTENT_STATE_FAILED,
            )
            .await;
            Err(err)
        }
    }
}

fn recovery_status(state: &str, reason: Option<&str>, retryable: bool) -> ScannerCycleRecoveryStatus {
    ScannerCycleRecoveryStatus {
        path: DATA_USAGE_BLOOM_NAME_PATH.clone(),
        quarantine_path: Some(DATA_USAGE_BLOOM_RECOVERY_PATH.clone()),
        state: state.to_string(),
        max_retries: MAX_SCANNER_CYCLE_RECOVERY_RETRIES,
        retryable,
        last_attempt_at_unix_secs: Some(unix_now_secs()),
        reason: reason.map(str::to_string),
        ..Default::default()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScannerCycleRecoveryMarker {
    pub schema_version: u16,
    pub primary_revision: String,
    pub generation: u64,
    pub leader_epoch: u64,
    pub classification: String,
    pub first_detected_at_unix_secs: u64,
    pub last_attempt_at_unix_secs: u64,
    pub retry_count: u64,
    pub reason: String,
    pub path: String,
    pub quarantine_path: String,
    /// `blocked` means the marker guards the primary revision; `cleanup-pending`
    /// means an operator reset is in progress and must remain fenced across a
    /// restart, even if the primary object is subsequently rewritten.
    #[serde(default = "default_recovery_marker_state")]
    pub state: String,
}

fn default_recovery_marker_state() -> String {
    "blocked".to_string()
}

#[derive(Debug, Deserialize)]
struct ScannerCycleRecoveryMarkerCompat {
    schema_version: Option<u16>,
    primary_revision: Option<String>,
    classification: Option<String>,
    first_detected_at_unix_secs: Option<u64>,
    last_attempt_at_unix_secs: Option<u64>,
    retry_count: Option<u64>,
    reason: Option<String>,
    path: Option<String>,
    quarantine_path: Option<String>,
    state: Option<String>,
}

#[derive(Debug)]
pub(crate) enum ScannerCycleStateStartup {
    Ready {
        cycle: CurrentCycle,
        leader_epoch: u64,
        revision: DataUsageCacheRevision,
    },
    Blocked,
    Transient(ScannerError),
}

#[derive(Debug, thiserror::Error)]
enum CycleRecoveryMarkerReadError {
    #[error("cycle recovery marker backend read failed: {0}")]
    Backend(#[source] EcstoreError),
    #[error("cycle recovery marker publication is blocked by data movement")]
    PublicationBlocked,
    #[error("invalid cycle recovery marker: {0}")]
    Invalid(&'static str),
    #[error("cycle recovery marker revision changed while publishing")]
    Conflict,
}

#[derive(Debug, thiserror::Error)]
enum CycleStateBodyReadError {
    #[error("scanner cycle state exceeds the bounded object size")]
    TooLarge,
    #[error("scanner cycle state body read failed: {0}")]
    Backend(#[source] EcstoreError),
}

fn recovery_status_from_marker(marker: &ScannerCycleRecoveryMarker, state: &str) -> ScannerCycleRecoveryStatus {
    ScannerCycleRecoveryStatus {
        path: marker.path.clone(),
        quarantine_path: Some(marker.quarantine_path.clone()),
        state: state.to_string(),
        classification: Some(marker.classification.clone()),
        primary_revision: Some(marker.primary_revision.clone()),
        generation: Some(marker.generation),
        leader_epoch: Some(marker.leader_epoch),
        first_detected_at_unix_secs: Some(marker.first_detected_at_unix_secs),
        last_attempt_at_unix_secs: Some(marker.last_attempt_at_unix_secs),
        retry_count: marker.retry_count,
        max_retries: MAX_SCANNER_CYCLE_RECOVERY_RETRIES,
        retryable: false,
        reason: Some(marker.reason.clone()),
    }
}

fn marker_matches_revision(marker: &ScannerCycleRecoveryMarker, revision: &DataUsageCacheRevision) -> bool {
    matches!(revision, DataUsageCacheRevision::Etag(etag) if marker.primary_revision == *etag)
}

fn validate_recovery_marker(marker: &ScannerCycleRecoveryMarker) -> Result<(), &'static str> {
    if marker.schema_version != SCANNER_CYCLE_RECOVERY_SCHEMA_VERSION {
        return Err("cycle recovery marker schema is unsupported");
    }
    if marker.primary_revision.is_empty() {
        return Err("cycle recovery marker has no primary revision");
    }
    if marker.path != *DATA_USAGE_BLOOM_NAME_PATH {
        return Err("cycle recovery marker path does not match the scanner scope");
    }
    if marker.quarantine_path != *DATA_USAGE_BLOOM_RECOVERY_PATH {
        return Err("cycle recovery marker quarantine path does not match the scanner scope");
    }
    if !matches!(marker.classification.as_str(), "corrupt" | "future_schema") {
        return Err("cycle recovery marker classification is invalid");
    }
    if !matches!(marker.state.as_str(), "blocked" | "cleanup-pending") {
        return Err("cycle recovery marker state is invalid");
    }
    Ok(())
}

/// Decode only the stable scope and revision fields needed by an authenticated
/// full-rescan reset. Startup keeps the strict decoder above so a newer marker
/// cannot be interpreted as a trusted cursor; reset deliberately rebuilds from
/// the persisted usage floor instead.
pub(super) fn decode_recovery_marker_for_reset(
    data: &[u8],
    marker_revision: &DataUsageCacheRevision,
) -> Result<ScannerCycleRecoveryMarker, ScannerError> {
    if !matches!(marker_revision, DataUsageCacheRevision::Etag(_)) {
        return Err(ScannerError::Other("cycle recovery marker has no object revision".to_string()));
    }
    if let Ok(value) = serde_json::from_slice::<serde_json::Value>(data)
        && let Some(state) = value.get("state")
        && !matches!(state.as_str(), Some("blocked" | "cleanup-pending"))
    {
        return Err(ScannerError::Other("cycle recovery marker state is unsupported".to_string()));
    }
    let compat = serde_json::from_slice::<ScannerCycleRecoveryMarkerCompat>(data).ok();
    let _schema_version = compat.as_ref().and_then(|marker| marker.schema_version);
    let primary_revision = compat
        .as_ref()
        .and_then(|marker| marker.primary_revision.clone())
        .filter(|revision| !revision.is_empty())
        .unwrap_or_default();
    let path = compat
        .as_ref()
        .and_then(|marker| marker.path.clone())
        .unwrap_or_else(|| DATA_USAGE_BLOOM_NAME_PATH.clone());
    let quarantine_path = compat
        .as_ref()
        .and_then(|marker| marker.quarantine_path.clone())
        .unwrap_or_else(|| DATA_USAGE_BLOOM_RECOVERY_PATH.clone());
    if path != *DATA_USAGE_BLOOM_NAME_PATH || quarantine_path != *DATA_USAGE_BLOOM_RECOVERY_PATH {
        return Err(ScannerError::Other(
            "cycle recovery marker path does not match the scanner scope".to_string(),
        ));
    }
    let classification = match compat.as_ref().and_then(|marker| marker.classification.as_deref()) {
        Some("corrupt") => "corrupt",
        Some("future_schema") | None => "future_schema",
        Some(_) => "future_schema",
    };
    let state = match compat.as_ref().and_then(|marker| marker.state.as_deref()) {
        Some("cleanup-pending") => "cleanup-pending",
        Some("blocked") | None => "blocked",
        Some(_) => {
            return Err(ScannerError::Other("cycle recovery marker state is unsupported".to_string()));
        }
    };
    let now = unix_now_secs();
    Ok(ScannerCycleRecoveryMarker {
        schema_version: SCANNER_CYCLE_RECOVERY_SCHEMA_VERSION,
        primary_revision,
        // Cursor and epoch values from an unknown marker are audit-only data;
        // the reset path intentionally rebuilds both from the verified usage
        // floor instead of carrying them across a version boundary.
        generation: 0,
        leader_epoch: 0,
        classification: classification.to_string(),
        first_detected_at_unix_secs: compat
            .as_ref()
            .and_then(|marker| marker.first_detected_at_unix_secs)
            .unwrap_or(now),
        last_attempt_at_unix_secs: compat
            .as_ref()
            .and_then(|marker| marker.last_attempt_at_unix_secs)
            .unwrap_or(now),
        retry_count: compat.as_ref().and_then(|marker| marker.retry_count).unwrap_or(0),
        reason: compat
            .as_ref()
            .and_then(|marker| marker.reason.clone())
            .unwrap_or_else(|| "operator requested full scanner rescan".to_string()),
        path: DATA_USAGE_BLOOM_NAME_PATH.clone(),
        quarantine_path: DATA_USAGE_BLOOM_RECOVERY_PATH.clone(),
        state: state.to_string(),
    })
}

async fn read_cycle_state_body(reader: &mut ScannerGetObjectReader) -> Result<Vec<u8>, CycleStateBodyReadError> {
    let max_len = usize::try_from(MAX_SCANNER_CYCLE_STATE_BYTES).unwrap_or(usize::MAX);
    let mut data = Vec::new();
    reader
        .take(MAX_SCANNER_CYCLE_STATE_BYTES.saturating_add(1))
        .read_to_end(&mut data)
        .await
        .map_err(|err| CycleStateBodyReadError::Backend(EcstoreError::other(err)))?;
    if data.len() > max_len {
        return Err(CycleStateBodyReadError::TooLarge);
    }
    Ok(data)
}

fn cycle_state_classification(buf: &[u8]) -> (&'static str, &'static str) {
    if buf.len() >= 16 && &buf[8..12] == b"RSCY" && &buf[8..16] != SCANNER_CYCLE_STATE_MAGIC {
        ("future_schema", "scanner cycle state schema is newer than this reader")
    } else {
        ("corrupt", "scanner cycle state failed validation")
    }
}

fn cycle_state_generation_and_epoch(buf: &[u8]) -> (u64, u64) {
    let generation = buf
        .get(..8)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u64::from_le_bytes)
        .unwrap_or(0);
    let leader_epoch = if buf.len() >= SCANNER_CYCLE_STATE_HEADER_LEN && &buf[8..16] == SCANNER_CYCLE_STATE_MAGIC {
        u64::from_le_bytes(buf[16..24].try_into().unwrap_or([0; 8]))
    } else {
        0
    };
    (generation, leader_epoch)
}

async fn persist_cycle_recovery_marker(
    storeapi: Arc<impl ScannerObjectIO + ScannerConfigObjectDelete>,
    primary_revision: &DataUsageCacheRevision,
    generation: u64,
    leader_epoch: u64,
    classification: &'static str,
    reason: &'static str,
    expected_epoch: u64,
) -> Result<ScannerCycleRecoveryMarker, CycleRecoveryMarkerReadError> {
    let now = unix_now_secs();
    let (existing, existing_revision) = match read_cycle_recovery_marker_bytes(storeapi.clone()).await {
        Ok(result) => result,
        Err(err) => return Err(err),
    };
    let existing_marker = existing
        .as_deref()
        .and_then(|bytes| serde_json::from_slice::<ScannerCycleRecoveryMarker>(bytes).ok());
    let primary_revision = match primary_revision {
        DataUsageCacheRevision::Etag(etag) => etag.clone(),
        DataUsageCacheRevision::Missing => {
            return Err(CycleRecoveryMarkerReadError::Invalid("cycle state recovery requires a primary revision"));
        }
    };
    let marker = ScannerCycleRecoveryMarker {
        schema_version: SCANNER_CYCLE_RECOVERY_SCHEMA_VERSION,
        primary_revision: primary_revision.clone(),
        generation,
        leader_epoch,
        classification: classification.to_string(),
        first_detected_at_unix_secs: existing_marker
            .as_ref()
            .filter(|marker| marker.primary_revision == primary_revision)
            .map(|marker| marker.first_detected_at_unix_secs)
            .unwrap_or(now),
        last_attempt_at_unix_secs: now,
        retry_count: existing_marker
            .as_ref()
            .filter(|marker| marker.primary_revision == primary_revision)
            .map(|marker| marker.retry_count.saturating_add(1))
            .unwrap_or(0),
        reason: reason.to_string(),
        path: DATA_USAGE_BLOOM_NAME_PATH.clone(),
        quarantine_path: DATA_USAGE_BLOOM_RECOVERY_PATH.clone(),
        state: "blocked".to_string(),
    };
    let bytes = serde_json::to_vec(&marker).map_err(|_| CycleRecoveryMarkerReadError::Invalid("marker serialization failed"))?;
    let Some(_publication_admission) = scanner_publication_admission_for_epoch(storeapi.clone(), expected_epoch).await else {
        return Err(CycleRecoveryMarkerReadError::PublicationBlocked);
    };
    let save_result = save_config_with_preconditions(
        storeapi.clone(),
        DATA_USAGE_BLOOM_RECOVERY_PATH.as_str(),
        bytes,
        existing_revision.preconditions(),
    )
    .await;
    match save_result {
        Ok(_) => Ok(marker),
        Err(EcstoreError::PreconditionFailed) => Err(CycleRecoveryMarkerReadError::Conflict),
        Err(err) => Err(CycleRecoveryMarkerReadError::Backend(err)),
    }
}

async fn read_cycle_recovery_marker_bytes(
    storeapi: Arc<impl ScannerObjectIO>,
) -> Result<(Option<Vec<u8>>, DataUsageCacheRevision), CycleRecoveryMarkerReadError> {
    let mut reader = match storeapi
        .get_object_reader(
            RUSTFS_META_BUCKET,
            DATA_USAGE_BLOOM_RECOVERY_PATH.as_str(),
            None,
            http::HeaderMap::new(),
            &ScannerObjectOptions {
                no_lock: true,
                ..Default::default()
            },
        )
        .await
    {
        Ok(reader) => reader,
        Err(
            EcstoreError::FileNotFound
            | EcstoreError::VolumeNotFound
            | EcstoreError::ObjectNotFound(_, _)
            | EcstoreError::BucketNotFound(_)
            | EcstoreError::ConfigNotFound,
        ) => {
            return Ok((None, DataUsageCacheRevision::Missing));
        }
        Err(err) => return Err(CycleRecoveryMarkerReadError::Backend(err)),
    };
    let revision = reader
        .object_info
        .etag
        .as_ref()
        .filter(|etag| !etag.is_empty())
        .cloned()
        .map(DataUsageCacheRevision::Etag)
        .ok_or(CycleRecoveryMarkerReadError::Invalid("marker has no revision"))?;
    if reader.object_info.is_dir || reader.object_info.size < 0 || reader.object_info.size > 64 * 1024 {
        return Err(CycleRecoveryMarkerReadError::Invalid("marker exceeds the bounded object size"));
    }
    let mut data = Vec::new();
    (&mut reader)
        .take(64 * 1024 + 1)
        .read_to_end(&mut data)
        .await
        .map_err(|err| CycleRecoveryMarkerReadError::Backend(EcstoreError::other(err)))?;
    if data.len() > 64 * 1024 {
        return Err(CycleRecoveryMarkerReadError::Invalid("marker exceeds the bounded object size"));
    }
    if data.is_empty() {
        return Err(CycleRecoveryMarkerReadError::Invalid("marker is empty"));
    }
    Ok((Some(data), revision))
}

async fn read_cycle_recovery_marker_revision(
    storeapi: Arc<impl ScannerObjectIO>,
) -> Result<DataUsageCacheRevision, CycleRecoveryMarkerReadError> {
    let reader = match storeapi
        .get_object_reader(
            RUSTFS_META_BUCKET,
            DATA_USAGE_BLOOM_RECOVERY_PATH.as_str(),
            None,
            http::HeaderMap::new(),
            &ScannerObjectOptions {
                no_lock: true,
                ..Default::default()
            },
        )
        .await
    {
        Ok(reader) => reader,
        Err(
            EcstoreError::FileNotFound
            | EcstoreError::VolumeNotFound
            | EcstoreError::ObjectNotFound(_, _)
            | EcstoreError::BucketNotFound(_)
            | EcstoreError::ConfigNotFound,
        ) => return Ok(DataUsageCacheRevision::Missing),
        Err(err) => return Err(CycleRecoveryMarkerReadError::Backend(err)),
    };
    if reader.object_info.is_dir || reader.object_info.size < 0 {
        return Err(CycleRecoveryMarkerReadError::Invalid("marker is not a regular object"));
    }
    reader
        .object_info
        .etag
        .as_ref()
        .filter(|etag| !etag.is_empty())
        .cloned()
        .map(DataUsageCacheRevision::Etag)
        .ok_or(CycleRecoveryMarkerReadError::Invalid("marker has no revision"))
}

async fn quarantine_invalid_cycle_state(
    storeapi: Arc<impl ScannerObjectIO + ScannerConfigObjectDelete>,
    revision: &DataUsageCacheRevision,
    buf: &[u8],
) -> ScannerCycleStateStartup {
    let (classification, reason) = cycle_state_classification(buf);
    let (generation, leader_epoch) = cycle_state_generation_and_epoch(buf);
    quarantine_invalid_cycle_state_with_reason(storeapi, revision, generation, leader_epoch, classification, reason).await
}

async fn quarantine_invalid_cycle_state_with_reason(
    storeapi: Arc<impl ScannerObjectIO + ScannerConfigObjectDelete>,
    revision: &DataUsageCacheRevision,
    generation: u64,
    leader_epoch: u64,
    classification: &'static str,
    reason: &'static str,
) -> ScannerCycleStateStartup {
    let now = unix_now_secs();
    let base_status = ScannerCycleRecoveryStatus {
        path: DATA_USAGE_BLOOM_NAME_PATH.clone(),
        quarantine_path: Some(DATA_USAGE_BLOOM_RECOVERY_PATH.clone()),
        state: "recovery-required".to_string(),
        classification: Some(classification.to_string()),
        primary_revision: match revision {
            DataUsageCacheRevision::Etag(etag) => Some(etag.clone()),
            DataUsageCacheRevision::Missing => None,
        },
        generation: Some(generation),
        leader_epoch: Some(leader_epoch),
        first_detected_at_unix_secs: Some(now),
        last_attempt_at_unix_secs: Some(now),
        retry_count: 0,
        max_retries: MAX_SCANNER_CYCLE_RECOVERY_RETRIES,
        retryable: true,
        reason: Some(reason.to_string()),
    };
    set_scanner_cycle_recovery_status(base_status);
    let Some(expected_epoch) = scanner_publication_epoch(storeapi.clone()).await else {
        set_scanner_cycle_recovery_status(recovery_status(
            "transient",
            Some("cycle recovery marker publication is blocked by data movement"),
            true,
        ));
        return ScannerCycleStateStartup::Transient(ScannerError::Other(
            "cycle recovery marker publication is blocked by data movement".to_string(),
        ));
    };
    match persist_cycle_recovery_marker(storeapi, revision, generation, leader_epoch, classification, reason, expected_epoch)
        .await
    {
        Ok(marker) => set_scanner_cycle_recovery_status(recovery_status_from_marker(&marker, "blocked")),
        Err(CycleRecoveryMarkerReadError::Backend(_)) => {
            // Keep the poison object untouched and retry marker creation with the
            // bounded startup backoff; recovery-required never becomes healthy.
            return ScannerCycleStateStartup::Transient(ScannerError::Other(
                "failed to persist scanner cycle recovery marker".to_string(),
            ));
        }
        Err(CycleRecoveryMarkerReadError::PublicationBlocked) => {
            set_scanner_cycle_recovery_status(recovery_status(
                "transient",
                Some("cycle recovery marker publication is blocked by data movement"),
                true,
            ));
            return ScannerCycleStateStartup::Transient(ScannerError::Other(
                "cycle recovery marker publication is blocked by data movement".to_string(),
            ));
        }
        Err(CycleRecoveryMarkerReadError::Conflict) => {
            set_scanner_cycle_recovery_status(recovery_status(
                "transient",
                Some("cycle recovery marker revision changed while publishing"),
                true,
            ));
            return ScannerCycleStateStartup::Transient(ScannerError::Other(
                "cycle recovery marker revision changed while publishing".to_string(),
            ));
        }
        Err(CycleRecoveryMarkerReadError::Invalid(reason)) => {
            set_scanner_cycle_recovery_status(recovery_status("recovery-required", Some(reason), false));
            return ScannerCycleStateStartup::Blocked;
        }
    }
    ScannerCycleStateStartup::Blocked
}

async fn mark_cycle_recovery_cleanup_pending(
    storeapi: Arc<ECStore>,
    mut marker: ScannerCycleRecoveryMarker,
    marker_revision: &DataUsageCacheRevision,
    expected_epoch: u64,
    owns_reset: &(impl Fn() -> bool + Sync),
) -> Result<(ScannerCycleRecoveryMarker, DataUsageCacheRevision), ScannerError> {
    marker.state = "cleanup-pending".to_string();
    marker.last_attempt_at_unix_secs = unix_now_secs();
    let bytes = serde_json::to_vec(&marker)
        .map_err(|err| ScannerError::Other(format!("failed to encode cycle recovery marker: {err}")))?;
    let info = save_reset_config(
        storeapi.clone(),
        DATA_USAGE_BLOOM_RECOVERY_PATH.as_str(),
        bytes,
        marker_revision.preconditions(),
        expected_epoch,
        owns_reset,
    )
    .await
    .map_err(|err| ScannerError::Other(format!("failed to mark cycle recovery cleanup pending: {err}")))?;
    let revision = info
        .etag
        .filter(|etag| !etag.is_empty())
        .map(DataUsageCacheRevision::Etag)
        .ok_or_else(|| ScannerError::Other("cycle recovery marker save returned no revision".to_string()))?;
    Ok((marker, revision))
}

pub(crate) async fn load_scanner_cycle_state_for_startup(
    storeapi: Arc<impl ScannerObjectIO + ScannerConfigObjectDelete>,
) -> ScannerCycleStateStartup {
    let marker = match read_cycle_recovery_marker_bytes(storeapi.clone()).await {
        Ok((None, _)) => None,
        Ok((Some(data), marker_revision)) => match serde_json::from_slice::<ScannerCycleRecoveryMarker>(&data) {
            Ok(marker) => match validate_recovery_marker(&marker) {
                Ok(()) => Some((marker, marker_revision)),
                Err(reason) => {
                    set_scanner_cycle_recovery_status(recovery_status("recovery-required", Some(reason), false));
                    return ScannerCycleStateStartup::Blocked;
                }
            },
            Err(_) => {
                set_scanner_cycle_recovery_status(recovery_status(
                    "recovery-required",
                    Some("cycle recovery marker is invalid"),
                    false,
                ));
                return ScannerCycleStateStartup::Blocked;
            }
        },
        Err(CycleRecoveryMarkerReadError::Backend(err)) => {
            let status = recovery_status("transient", Some("cycle recovery marker I/O is temporarily unavailable"), true);
            set_scanner_cycle_recovery_status(status);
            return ScannerCycleStateStartup::Transient(ScannerError::Other(format!(
                "failed to read scanner cycle recovery marker: {err}"
            )));
        }
        Err(CycleRecoveryMarkerReadError::PublicationBlocked) => {
            set_scanner_cycle_recovery_status(recovery_status(
                "transient",
                Some("cycle recovery marker publication is blocked by data movement"),
                true,
            ));
            return ScannerCycleStateStartup::Transient(ScannerError::Other(
                "cycle recovery marker publication is blocked by data movement".to_string(),
            ));
        }
        Err(CycleRecoveryMarkerReadError::Invalid(reason)) => {
            set_scanner_cycle_recovery_status(recovery_status("recovery-required", Some(reason), false));
            return ScannerCycleStateStartup::Blocked;
        }
        Err(CycleRecoveryMarkerReadError::Conflict) => {
            set_scanner_cycle_recovery_status(recovery_status(
                "transient",
                Some("cycle recovery marker revision changed while being inspected"),
                true,
            ));
            return ScannerCycleStateStartup::Transient(ScannerError::Other(
                "cycle recovery marker revision changed while being inspected".to_string(),
            ));
        }
    };

    let mut reader = match storeapi
        .get_object_reader(
            RUSTFS_META_BUCKET,
            DATA_USAGE_BLOOM_NAME_PATH.as_str(),
            None,
            http::HeaderMap::new(),
            &ScannerObjectOptions {
                no_lock: true,
                ..Default::default()
            },
        )
        .await
    {
        Ok(reader) => reader,
        Err(
            EcstoreError::FileNotFound
            | EcstoreError::VolumeNotFound
            | EcstoreError::ObjectNotFound(_, _)
            | EcstoreError::BucketNotFound(_)
            | EcstoreError::ConfigNotFound,
        ) => {
            if let Some((marker, _)) = marker {
                let state = if marker.state == "cleanup-pending" {
                    "cleanup-pending"
                } else {
                    "recovery-required"
                };
                set_scanner_cycle_recovery_status(recovery_status_from_marker(&marker, state));
                return ScannerCycleStateStartup::Blocked;
            }
            set_scanner_cycle_recovery_status(recovery_status("healthy", None, false));
            return ScannerCycleStateStartup::Ready {
                cycle: CurrentCycle::default(),
                leader_epoch: 0,
                revision: DataUsageCacheRevision::Missing,
            };
        }
        Err(err) => {
            set_scanner_cycle_recovery_status(recovery_status("transient", Some("cycle state could not be inspected"), true));
            return ScannerCycleStateStartup::Transient(ScannerError::Other(format!(
                "failed to inspect scanner cycle state: {err}"
            )));
        }
    };
    let revision = reader
        .object_info
        .etag
        .as_ref()
        .filter(|etag| !etag.is_empty())
        .cloned()
        .map(DataUsageCacheRevision::Etag);
    let Some(revision) = revision else {
        set_scanner_cycle_recovery_status(recovery_status("recovery-required", Some("cycle state has no revision"), false));
        return ScannerCycleStateStartup::Blocked;
    };
    let max_size = i64::try_from(MAX_SCANNER_CYCLE_STATE_BYTES).unwrap_or(i64::MAX);
    if reader.object_info.is_dir || reader.object_info.size < 0 || reader.object_info.size > max_size {
        return quarantine_invalid_cycle_state_with_reason(
            storeapi,
            &revision,
            0,
            0,
            "corrupt",
            "scanner cycle state object is oversized or not a regular object",
        )
        .await;
    }
    if let Some((marker, _)) = marker
        .as_ref()
        .filter(|(marker, _)| marker.state == "cleanup-pending" || marker_matches_revision(marker, &revision))
    {
        let state = if marker.state == "cleanup-pending" {
            "cleanup-pending"
        } else {
            "blocked"
        };
        set_scanner_cycle_recovery_status(recovery_status_from_marker(marker, state));
        return ScannerCycleStateStartup::Blocked;
    }
    let data = match read_cycle_state_body(&mut reader).await {
        Ok(data) => data,
        Err(CycleStateBodyReadError::TooLarge) => {
            return quarantine_invalid_cycle_state_with_reason(
                storeapi,
                &revision,
                0,
                0,
                "corrupt",
                "scanner cycle state exceeds the bounded object size",
            )
            .await;
        }
        Err(CycleStateBodyReadError::Backend(err)) => {
            set_scanner_cycle_recovery_status(recovery_status("transient", Some("cycle state read failed"), true));
            return ScannerCycleStateStartup::Transient(ScannerError::Other(format!(
                "failed to read scanner cycle state: {err}"
            )));
        }
    };
    if data.is_empty() {
        return quarantine_invalid_cycle_state_with_reason(
            storeapi,
            &revision,
            0,
            0,
            "corrupt",
            "scanner cycle state object is empty",
        )
        .await;
    }
    match decode_scanner_cycle_state_for_startup(&data) {
        Ok((cycle, leader_epoch)) => {
            set_scanner_cycle_recovery_status(recovery_status("healthy", None, false));
            ScannerCycleStateStartup::Ready {
                cycle,
                leader_epoch,
                revision,
            }
        }
        Err(_) => quarantine_invalid_cycle_state(storeapi, &revision, &data).await,
    }
}

/// Reset a blocked cycle state after an explicit full-rescan request. Durable
/// cleanup state fences primary rewrites; marker removal retains its ETag and
/// usage-epoch checks.
pub async fn reset_scanner_cycle_recovery(ctx: CancellationToken, storeapi: Arc<ECStore>) -> Result<(), ScannerError> {
    reset_scanner_cycle_recovery_for_intent(ctx, storeapi, None, None).await
}

/// Resume only an operator reset whose cleanup phase is already durable.
/// Missing or merely blocked markers never authorize an automatic reset.
pub(super) async fn resume_scanner_cycle_cleanup(ctx: CancellationToken, storeapi: Arc<ECStore>) -> Result<(), ScannerError> {
    let status_version = scanner_cleanup_status_version();
    let (marker, revision) = match read_scanner_cleanup_marker(storeapi.clone(), &ctx).await {
        Ok(Some(marker)) => marker,
        Ok(None) => return Ok(()),
        Err(error) => {
            let _ =
                publish_scanner_cleanup_status(recovery_status("blocked", Some(&error.to_string()), false), Some(status_version));
            return Err(error);
        }
    };
    let mut observation =
        publish_scanner_cleanup_status(recovery_status_from_marker(&marker, &marker.state), Some(status_version));
    if marker.state != "cleanup-pending" {
        return Ok(());
    }
    let result = reset_scanner_cycle_recovery_for_intent(ctx, storeapi, Some(revision), Some(&mut observation)).await;
    if let Err(error) = &result
        && let Some(observation) = observation
    {
        publish_scanner_cleanup_failure(error.to_string(), observation);
    }
    result
}

pub(super) async fn read_scanner_cleanup_marker(
    storeapi: Arc<impl ScannerObjectIO>,
    ctx: &CancellationToken,
) -> Result<Option<(ScannerCycleRecoveryMarker, DataUsageCacheRevision)>, ScannerError> {
    let (data, revision) = tokio::select! {
        biased;
        _ = ctx.cancelled() => return Err(ScannerError::Other("scanner cleanup recovery was cancelled".to_string())),
        result = tokio::time::timeout(data_usage_persist_timeout(), read_cycle_recovery_marker_bytes(storeapi)) => {
            result.map_err(|_| ScannerError::Other("scanner cleanup marker inspection timed out".to_string()))?
                .map_err(|err| ScannerError::Other(format!("failed to inspect pending scanner cleanup: {err}")))?
        }
    };
    let Some(data) = data else {
        return Ok(None);
    };
    let marker: ScannerCycleRecoveryMarker = serde_json::from_slice(&data)
        .map_err(|err| ScannerError::Other(format!("pending scanner cleanup marker is invalid: {err}")))?;
    validate_recovery_marker(&marker)
        .map_err(|err| ScannerError::Other(format!("pending scanner cleanup marker is invalid: {err}")))?;
    Ok(Some((marker, revision)))
}

pub(super) async fn reset_scanner_cycle_recovery_for_intent(
    ctx: CancellationToken,
    storeapi: Arc<ECStore>,
    expected_cleanup_revision: Option<DataUsageCacheRevision>,
    mut observation: Option<&mut Option<u64>>,
) -> Result<(), ScannerError> {
    #[cfg(test)]
    let resume_only = expected_cleanup_revision.is_some();
    // The outer Some distinguishes a tracked resume whose version may be
    // invalidated from an explicit v3 reset with no observation owner.
    let mut publish_status = |status| {
        if let Some(version) = observation.as_deref_mut() {
            if let Some(expected) = *version {
                *version = publish_scanner_cleanup_status(status, Some(expected));
            }
        } else {
            set_scanner_cycle_recovery_status(status);
        }
    };
    let lock = storeapi
        .new_ns_lock(RUSTFS_META_BUCKET, "leader.lock")
        .await
        .map_err(|err| ScannerError::Other(format!("failed to acquire scanner leader lock: {err}")))?;
    let guard = lock
        .get_write_lock_quiet(Duration::from_secs(5))
        .await
        .map_err(|err| ScannerError::Other(format!("scanner leader lock is busy: {err}")))?;
    let owns_reset = || !guard.is_lock_lost() && !ctx.is_cancelled();

    if guard.is_lock_lost() {
        return Err(ScannerError::Other("scanner leader lock was lost before recovery reset".to_string()));
    }

    let Some(reset_epoch) = scanner_publication_epoch(storeapi.clone()).await else {
        return Err(ScannerError::Other("scanner recovery reset is blocked by data movement".to_string()));
    };

    let (marker_data, marker_revision, marker_body_invalid) = match read_cycle_recovery_marker_bytes(storeapi.clone()).await {
        Ok((marker_data, marker_revision)) => (marker_data, marker_revision, false),
        Err(CycleRecoveryMarkerReadError::Invalid(_)) => {
            let marker_revision = read_cycle_recovery_marker_revision(storeapi.clone())
                .await
                .map_err(|err| ScannerError::Other(format!("failed to read cycle recovery marker: {err}")))?;
            (Some(Vec::new()), marker_revision, true)
        }
        Err(err) => return Err(ScannerError::Other(format!("failed to read cycle recovery marker: {err}"))),
    };
    if let Some(expected) = expected_cleanup_revision {
        if marker_revision != expected || !owns_reset() {
            return Err(ScannerError::Other(
                "pending scanner cleanup changed before recovery acquired ownership".to_string(),
            ));
        }
        let marker: ScannerCycleRecoveryMarker = marker_data
            .as_deref()
            .and_then(|data| serde_json::from_slice(data).ok())
            .filter(|marker| validate_recovery_marker(marker).is_ok() && marker.state == "cleanup-pending")
            .ok_or_else(|| {
                ScannerError::Other("scanner cleanup recovery requires an unchanged cleanup-pending marker".to_string())
            })?;
        publish_status(recovery_status_from_marker(&marker, "cleanup-pending"));
    }
    let Some(marker_data) = marker_data else {
        // A delete may commit before its reply is lost. Confirm both durable
        // fences before treating a retry without its marker as completed.
        let (cycle, epoch, revision) = read_cycle_state_for_usage_reset(storeapi.clone()).await?;
        let floor = persisted_usage_floor(storeapi.clone()).await?;
        if !matches!(revision, DataUsageCacheRevision::Etag(_))
            || epoch < floor.leader_epoch
            || cycle.next < floor.next_cycle
            || !owns_reset()
            || scanner_publication_admission_for_epoch(storeapi.clone(), reset_epoch)
                .await
                .is_none()
        {
            return Err(ScannerError::Other(
                "scanner cycle recovery marker is absent without a completed reset fence".to_string(),
            ));
        }
        publish_status(recovery_status("healthy", None, false));
        super::notify_scanner_cycle_recovery_wake();
        return Ok(());
    };
    let (marker, force_full_rescan) = match serde_json::from_slice::<ScannerCycleRecoveryMarker>(&marker_data) {
        Ok(marker) if validate_recovery_marker(&marker).is_ok() => (marker, false),
        _ => (decode_recovery_marker_for_reset(&marker_data, &marker_revision)?, true),
    };
    let force_full_rescan = force_full_rescan || marker_body_invalid;

    if guard.is_lock_lost() {
        return Err(ScannerError::Other(
            "scanner leader lock was lost while reading recovery state".to_string(),
        ));
    }

    #[cfg(test)]
    if resume_only {
        cleanup_io_fault::check(&storeapi, cleanup_io_fault::Stage::PrimaryRead, owns_reset())?;
    }
    let (mut primary_reader, primary_revision) = match storeapi
        .get_object_reader(
            RUSTFS_META_BUCKET,
            DATA_USAGE_BLOOM_NAME_PATH.as_str(),
            None,
            http::HeaderMap::new(),
            &ScannerObjectOptions {
                no_lock: true,
                ..Default::default()
            },
        )
        .await
    {
        Ok(reader) => {
            let revision = reader
                .object_info
                .etag
                .as_ref()
                .filter(|etag| !etag.is_empty())
                .cloned()
                .ok_or_else(|| ScannerError::Other("scanner cycle state has no revision".to_string()))?;
            (Some(reader), DataUsageCacheRevision::Etag(revision))
        }
        Err(
            EcstoreError::FileNotFound
            | EcstoreError::VolumeNotFound
            | EcstoreError::ObjectNotFound(_, _)
            | EcstoreError::BucketNotFound(_)
            | EcstoreError::ConfigNotFound,
        ) => (None, DataUsageCacheRevision::Missing),
        Err(err) => return Err(ScannerError::Other(format!("failed to inspect scanner cycle state: {err}"))),
    };
    let marker_cleanup_pending = marker.state == "cleanup-pending";
    let marker_matches_primary = marker_matches_revision(&marker, &primary_revision);
    if (marker_cleanup_pending || !marker_matches_primary)
        && let Some(mut reader) = primary_reader.take()
    {
        // A newer, independently fenced primary is authoritative. A
        // full-rescan reset must not overwrite that progress; it only
        // removes the stale recovery marker after validating and re-fencing
        // the state.
        let max_size = i64::try_from(MAX_SCANNER_CYCLE_STATE_BYTES).unwrap_or(i64::MAX);
        if reader.object_info.is_dir || reader.object_info.size < 0 {
            return Err(ScannerError::Other("scanner cycle state changed since recovery was recorded".to_string()));
        }
        let primary_is_oversized = reader.object_info.size > max_size;
        let primary_state = if primary_is_oversized {
            None
        } else {
            match read_cycle_state_body(&mut reader).await {
                Ok(data) if data.is_empty() => None,
                Ok(data) => decode_scanner_cycle_state_for_startup(&data).ok(),
                Err(CycleStateBodyReadError::TooLarge) if force_full_rescan || marker_cleanup_pending => None,
                Err(err) => {
                    return Err(ScannerError::Other(format!(
                        "scanner cycle state changed since recovery was recorded: {err}"
                    )));
                }
            }
        };
        if let Some((primary_cycle, primary_epoch)) = primary_state {
            verify_cycle_reset_intent(storeapi.clone(), &marker_revision, &owns_reset).await?;
            let (cleanup_marker, cleanup_marker_revision) =
                mark_cycle_recovery_cleanup_pending(storeapi.clone(), marker.clone(), &marker_revision, reset_epoch, &owns_reset)
                    .await?;
            publish_status(recovery_status_from_marker(&cleanup_marker, "cleanup-pending"));
            let usage_floor = persisted_usage_floor(storeapi.clone()).await?;
            let fence_epoch = primary_epoch
                .max(usage_floor.leader_epoch)
                .checked_add(1)
                .filter(|epoch| *epoch < u64::MAX)
                .ok_or_else(|| ScannerError::Other("scanner leader epoch is exhausted".to_string()))?;
            if guard.is_lock_lost() {
                return Err(ScannerError::Other(
                    "scanner leader lock was lost before preserving newer cycle state".to_string(),
                ));
            }
            let preserved_data = encode_scanner_cycle_state(&primary_cycle, fence_epoch)
                .map_err(|err| ScannerError::Other(format!("failed to encode preserved scanner cycle state: {err}")))?;
            if u64::try_from(preserved_data.len()).unwrap_or(u64::MAX) > MAX_SCANNER_CYCLE_STATE_BYTES {
                return Err(ScannerError::Other(
                    "preserved scanner cycle state exceeds the bounded object size".to_string(),
                ));
            }
            verify_cycle_reset_intent(storeapi.clone(), &cleanup_marker_revision, &owns_reset).await?;
            #[cfg(test)]
            if resume_only {
                cleanup_io_fault::check(&storeapi, cleanup_io_fault::Stage::PrimaryWrite, owns_reset())?;
            }
            let preserved_info = save_reset_config(
                storeapi.clone(),
                DATA_USAGE_BLOOM_NAME_PATH.as_str(),
                preserved_data,
                primary_revision.preconditions(),
                reset_epoch,
                &owns_reset,
            )
            .await
            .map_err(|err| {
                if scanner_publication_epoch_changed(&err) {
                    ScannerError::Other("scanner recovery reset deferred by a movement epoch change".to_string())
                } else {
                    ScannerError::Other(format!("failed to fence preserved scanner cycle state: {err}"))
                }
            })?;
            let preserved_revision = preserved_info
                .etag
                .filter(|etag| !etag.is_empty())
                .map(DataUsageCacheRevision::Etag)
                .ok_or_else(|| ScannerError::Other("preserved scanner cycle state has no revision".to_string()))?;
            if guard.is_lock_lost() {
                return Err(ScannerError::Other(
                    "scanner leader lock was lost after fencing newer cycle state".to_string(),
                ));
            }
            verify_cycle_reset_intent(storeapi.clone(), &cleanup_marker_revision, &owns_reset).await?;
            fence_scanner_usage_epoch_with_expected_epoch(
                &ctx,
                storeapi.clone(),
                fence_epoch,
                Some(reset_epoch),
                false,
                &owns_reset,
            )
            .await
            .map_err(|err| ScannerError::Other(format!("failed to fence preserved scanner usage epoch: {err}")))?;
            if guard.is_lock_lost() {
                return Err(ScannerError::Other(
                    "scanner leader lock was lost after fencing newer cycle state".to_string(),
                ));
            }
            let current_revision = read_config_revision(storeapi.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str())
                .await
                .map_err(|err| ScannerError::Other(format!("failed to verify preserved scanner cycle state: {err}")))?;
            if current_revision != preserved_revision {
                return Err(ScannerError::Other(
                    "scanner cycle state changed before recovery marker cleanup".to_string(),
                ));
            }
            verify_cycle_reset_intent(storeapi.clone(), &cleanup_marker_revision, &owns_reset).await?;
            delete_reset_config(
                storeapi.clone(),
                RUSTFS_META_BUCKET,
                DATA_USAGE_BLOOM_RECOVERY_PATH.as_str(),
                ScannerObjectOptions {
                    // This is one exact metadata object. Prefix-delete mode
                    // bypasses HTTP preconditions in the ECStore path.
                    delete_prefix: false,
                    http_preconditions: Some(cleanup_marker_revision.preconditions()),
                    ..Default::default()
                },
                reset_epoch,
                &owns_reset,
            )
            .await
            .map_err(|err| {
                if scanner_publication_epoch_changed(&err) {
                    ScannerError::Other("scanner recovery reset deferred by a movement epoch change".to_string())
                } else {
                    ScannerError::Other(format!("failed to clear stale cycle recovery marker: {err}"))
                }
            })?;
            publish_status(recovery_status("healthy", None, false));
            super::notify_scanner_cycle_recovery_wake();
            return Ok(());
        } else if !force_full_rescan && !marker_cleanup_pending {
            // An invalid compatibility marker cannot fence a corrupt primary
            // by revision, so rebuild it from the verified usage floor below.
            // A strict marker keeps the existing fail-closed behavior for an
            // unexpected stale-primary mutation.
            return Err(ScannerError::Other("scanner cycle state changed since recovery was recorded".to_string()));
        }
    }

    if guard.is_lock_lost() {
        return Err(ScannerError::Other(
            "scanner leader lock was lost before rebuilding cycle state".to_string(),
        ));
    }

    let floor = persisted_usage_floor(storeapi.clone()).await?;
    // A full rescan must not trust a cursor recovered from a corrupt, future,
    // or mixed-version marker. The durable usage floor is the only verified
    // starting point; marker generation/epoch fields remain audit evidence.
    let next = floor.next_cycle;
    if next == u64::MAX {
        return Err(ScannerError::Other("scanner cycle counter is exhausted".to_string()));
    }
    let leader_epoch = floor
        .leader_epoch
        .checked_add(1)
        .filter(|epoch| *epoch < u64::MAX)
        .ok_or_else(|| ScannerError::Other("scanner leader epoch is exhausted".to_string()))?;
    let cycle = CurrentCycle {
        next,
        ..Default::default()
    };
    let data = encode_scanner_cycle_state(&cycle, leader_epoch)
        .map_err(|err| ScannerError::Other(format!("failed to encode rebuilt scanner cycle state: {err}")))?;
    // Persist the cleanup-pending phase before rewriting the primary. If the
    // process dies after the rewrite, startup still sees a durable fence and
    // cannot mistake the partially completed reset for a healthy state.
    verify_cycle_reset_intent(storeapi.clone(), &marker_revision, &owns_reset).await?;
    let (marker, marker_revision) = if marker.state == "cleanup-pending" {
        (marker, marker_revision)
    } else {
        mark_cycle_recovery_cleanup_pending(storeapi.clone(), marker, &marker_revision, reset_epoch, &owns_reset).await?
    };
    verify_cycle_reset_intent(storeapi.clone(), &marker_revision, &owns_reset).await?;
    let rebuilt_info = save_reset_config(
        storeapi.clone(),
        DATA_USAGE_BLOOM_NAME_PATH.as_str(),
        data,
        primary_revision.preconditions(),
        reset_epoch,
        &owns_reset,
    )
    .await
    .map_err(|err| {
        if scanner_publication_epoch_changed(&err) {
            ScannerError::Other("scanner recovery reset deferred by a movement epoch change".to_string())
        } else {
            ScannerError::Other(format!("failed to persist rebuilt scanner cycle state: {err}"))
        }
    })?;
    let rebuilt_revision = rebuilt_info
        .etag
        .filter(|etag| !etag.is_empty())
        .ok_or_else(|| ScannerError::Other("rebuilt scanner cycle state has no revision".to_string()))?;
    if guard.is_lock_lost() {
        return Err(ScannerError::Other(
            "scanner leader lock was lost after rebuilding cycle state".to_string(),
        ));
    }
    verify_cycle_reset_intent(storeapi.clone(), &marker_revision, &owns_reset).await?;
    let usage_fence = fence_scanner_usage_epoch_with_expected_epoch(
        &ctx,
        storeapi.clone(),
        leader_epoch,
        Some(reset_epoch),
        false,
        &owns_reset,
    );
    #[cfg(test)]
    let usage_fence = async {
        if resume_only {
            cleanup_io_fault::check(&storeapi, cleanup_io_fault::Stage::UsageFence, owns_reset())?;
        }
        usage_fence.await
    };
    if let Err(err) = usage_fence.await {
        publish_status(ScannerCycleRecoveryStatus {
            path: DATA_USAGE_BLOOM_NAME_PATH.clone(),
            quarantine_path: Some(DATA_USAGE_BLOOM_RECOVERY_PATH.clone()),
            state: "cleanup-pending".to_string(),
            classification: Some(marker.classification.clone()),
            primary_revision: Some(rebuilt_revision.clone()),
            generation: Some(next),
            leader_epoch: Some(leader_epoch),
            first_detected_at_unix_secs: Some(marker.first_detected_at_unix_secs),
            last_attempt_at_unix_secs: Some(unix_now_secs()),
            retry_count: marker.retry_count,
            max_retries: MAX_SCANNER_CYCLE_RECOVERY_RETRIES,
            retryable: false,
            reason: Some("cycle state rebuilt but usage epoch fencing failed".to_string()),
        });
        return Err(err);
    }

    let current_revision = match read_config_revision(storeapi.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str())
        .await
        .map_err(|err| ScannerError::Other(format!("failed to verify rebuilt scanner cycle state: {err}")))?
    {
        DataUsageCacheRevision::Etag(etag) => etag,
        DataUsageCacheRevision::Missing => {
            return Err(ScannerError::Other("rebuilt scanner cycle state lost its revision".to_string()));
        }
    };
    if current_revision != rebuilt_revision {
        publish_status(ScannerCycleRecoveryStatus {
            path: DATA_USAGE_BLOOM_NAME_PATH.clone(),
            quarantine_path: Some(DATA_USAGE_BLOOM_RECOVERY_PATH.clone()),
            state: "cleanup-pending".to_string(),
            classification: Some(marker.classification.clone()),
            primary_revision: Some(current_revision),
            generation: Some(next),
            leader_epoch: Some(leader_epoch),
            first_detected_at_unix_secs: Some(marker.first_detected_at_unix_secs),
            last_attempt_at_unix_secs: Some(unix_now_secs()),
            retry_count: marker.retry_count,
            max_retries: MAX_SCANNER_CYCLE_RECOVERY_RETRIES,
            retryable: false,
            reason: Some("rebuilt scanner cycle state changed before marker cleanup".to_string()),
        });
        return Err(ScannerError::Other(
            "rebuilt scanner cycle state changed before recovery marker cleanup".to_string(),
        ));
    }

    if guard.is_lock_lost() {
        publish_status(ScannerCycleRecoveryStatus {
            path: DATA_USAGE_BLOOM_NAME_PATH.clone(),
            quarantine_path: Some(DATA_USAGE_BLOOM_RECOVERY_PATH.clone()),
            state: "cleanup-pending".to_string(),
            classification: Some(marker.classification.clone()),
            primary_revision: Some(rebuilt_revision.clone()),
            generation: Some(next),
            leader_epoch: Some(leader_epoch),
            retry_count: marker.retry_count,
            max_retries: MAX_SCANNER_CYCLE_RECOVERY_RETRIES,
            retryable: false,
            reason: Some("cycle state rebuilt but recovery marker was not cleared".to_string()),
            ..Default::default()
        });
        return Err(ScannerError::Other(
            "scanner leader lock was lost before clearing recovery marker".to_string(),
        ));
    }

    verify_cycle_reset_intent(storeapi.clone(), &marker_revision, &owns_reset).await?;
    if let Err(err) = delete_reset_config(
        storeapi.clone(),
        RUSTFS_META_BUCKET,
        DATA_USAGE_BLOOM_RECOVERY_PATH.as_str(),
        ScannerObjectOptions {
            // This is one exact metadata object. Prefix-delete mode
            // bypasses HTTP preconditions in the ECStore path.
            delete_prefix: false,
            http_preconditions: Some(marker_revision.preconditions()),
            ..Default::default()
        },
        reset_epoch,
        &owns_reset,
    )
    .await
    {
        if scanner_publication_epoch_changed(&err) {
            publish_status(ScannerCycleRecoveryStatus {
                path: DATA_USAGE_BLOOM_NAME_PATH.clone(),
                quarantine_path: Some(DATA_USAGE_BLOOM_RECOVERY_PATH.clone()),
                state: "cleanup-pending".to_string(),
                classification: Some(marker.classification.clone()),
                primary_revision: Some(rebuilt_revision.clone()),
                generation: Some(next),
                leader_epoch: Some(leader_epoch),
                retry_count: marker.retry_count,
                max_retries: MAX_SCANNER_CYCLE_RECOVERY_RETRIES,
                retryable: true,
                reason: Some("movement epoch changed before recovery marker cleanup".to_string()),
                ..Default::default()
            });
            return Err(ScannerError::Other(
                "scanner recovery reset deferred by a movement epoch change".to_string(),
            ));
        }
        publish_status(ScannerCycleRecoveryStatus {
            path: DATA_USAGE_BLOOM_NAME_PATH.clone(),
            quarantine_path: Some(DATA_USAGE_BLOOM_RECOVERY_PATH.clone()),
            state: "cleanup-pending".to_string(),
            classification: Some(marker.classification.clone()),
            primary_revision: Some(rebuilt_revision.clone()),
            generation: Some(next),
            leader_epoch: Some(leader_epoch),
            retry_count: marker.retry_count,
            max_retries: MAX_SCANNER_CYCLE_RECOVERY_RETRIES,
            retryable: false,
            reason: Some("cycle state rebuilt but recovery marker cleanup failed".to_string()),
            ..Default::default()
        });
        return Err(ScannerError::Other(format!("failed to clear cycle recovery marker: {err}")));
    }
    publish_status(ScannerCycleRecoveryStatus {
        path: DATA_USAGE_BLOOM_NAME_PATH.clone(),
        quarantine_path: Some(DATA_USAGE_BLOOM_RECOVERY_PATH.clone()),
        state: "healthy".to_string(),
        max_retries: MAX_SCANNER_CYCLE_RECOVERY_RETRIES,
        ..Default::default()
    });
    super::notify_scanner_cycle_recovery_wake();
    Ok(())
}

async fn verify_cycle_reset_intent(
    storeapi: Arc<impl ScannerObjectIO>,
    expected_revision: &DataUsageCacheRevision,
    owns_reset: &(impl Fn() -> bool + Sync),
) -> Result<(), ScannerError> {
    let revision = read_config_revision(storeapi, DATA_USAGE_BLOOM_RECOVERY_PATH.as_str())
        .await
        .map_err(|err| ScannerError::Other(format!("failed to verify scanner cycle reset intent: {err}")))?;
    if &revision != expected_revision {
        return Err(ScannerError::Other("scanner cycle reset intent changed".to_string()));
    }
    if !owns_reset() {
        return Err(ScannerError::Other("scanner cycle reset ownership was lost".to_string()));
    }
    Ok(())
}

async fn save_reset_config(
    storeapi: Arc<impl ScannerObjectIO + ScannerConfigObjectDelete>,
    path: &str,
    data: Vec<u8>,
    preconditions: crate::HTTPPreconditions,
    expected_epoch: u64,
    owns_reset: &(impl Fn() -> bool + Sync),
) -> Result<crate::ScannerObjectInfo, EcstoreError> {
    let Some(_admission) = scanner_publication_admission_for_epoch(storeapi.clone(), expected_epoch).await else {
        return Err(EcstoreError::other(SCANNER_PUBLICATION_EPOCH_CHANGED));
    };
    if !owns_reset() {
        return Err(EcstoreError::other("scanner reset ownership was lost before write"));
    }
    save_config_with_preconditions(storeapi, path, data, preconditions).await
}

async fn delete_reset_config(
    storeapi: Arc<impl ScannerObjectIO + ScannerConfigObjectDelete>,
    bucket: &str,
    path: &str,
    options: ScannerObjectOptions,
    expected_epoch: u64,
    owns_reset: &(impl Fn() -> bool + Sync),
) -> Result<crate::ScannerObjectInfo, EcstoreError> {
    let Some(_admission) = scanner_publication_admission_for_epoch(storeapi.clone(), expected_epoch).await else {
        return Err(EcstoreError::other(SCANNER_PUBLICATION_EPOCH_CHANGED));
    };
    if !owns_reset() {
        return Err(EcstoreError::other("scanner reset ownership was lost before delete"));
    }
    storeapi.delete_config_object(bucket, path, options).await
}

fn scanner_usage_state_reset_paths() -> Vec<String> {
    vec![
        DATA_USAGE_OBJ_NAME_PATH.as_str().to_string(),
        format!("{}.bkp", DATA_USAGE_OBJ_NAME_PATH.as_str()),
        LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str().to_string(),
        format!("{}.bkp", LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str()),
        DATA_USAGE_OBSERVED_OBJ_NAME_PATH.as_str().to_string(),
    ]
}

pub(super) async fn read_usage_state_reset_slots(
    storeapi: Arc<impl ScannerObjectIO + ScannerConfigObjectDelete>,
) -> Result<Vec<ScannerUsageStateResetSlot>, ScannerError> {
    let mut slots = Vec::new();
    for path in scanner_usage_state_reset_paths() {
        let (data, revision) = read_config_with_revision(storeapi.clone(), &path)
            .await
            .map_err(|err| ScannerError::Other(format!("failed to inspect scanner usage reset slot {path}: {err}")))?;
        slots.push(ScannerUsageStateResetSlot { path, data, revision });
    }
    Ok(slots)
}

enum ScannerUsageResetFloor {
    Missing,
    Trusted(PersistedUsageFloor),
    Corrupt,
}

fn usage_state_reset_floor(slots: &[ScannerUsageStateResetSlot]) -> Result<ScannerUsageResetFloor, ScannerError> {
    let mut floor = None;
    for slot in slots {
        let Some(data) = slot.data.as_deref() else {
            continue;
        };
        let Ok(usage) = serde_json::from_slice::<DataUsageInfo>(data) else {
            continue;
        };
        if !data_usage_info_has_persisted_baseline_identity(&usage)
            && !(slot.path == DATA_USAGE_OBJ_NAME_PATH.as_str() && data_usage_info_is_bootstrap_pending(&usage))
            && legacy_incomplete_usage_fence(data, &usage)
                .and_then(|fence| fence.claimable_epoch())
                .is_none()
        {
            continue;
        }
        update_persisted_usage_floor(floor.get_or_insert_with(PersistedUsageFloor::default), &usage, &slot.path)?;
    }
    Ok(match floor {
        Some(floor) => ScannerUsageResetFloor::Trusted(floor),
        None if slots.iter().any(|slot| slot.data.is_some()) => ScannerUsageResetFloor::Corrupt,
        None => ScannerUsageResetFloor::Missing,
    })
}

async fn read_cycle_state_for_usage_reset(
    storeapi: Arc<ECStore>,
) -> Result<(CurrentCycle, u64, DataUsageCacheRevision), ScannerError> {
    let mut reader = match storeapi
        .get_object_reader(
            RUSTFS_META_BUCKET,
            DATA_USAGE_BLOOM_NAME_PATH.as_str(),
            None,
            http::HeaderMap::new(),
            &ScannerObjectOptions {
                no_lock: true,
                ..Default::default()
            },
        )
        .await
    {
        Ok(reader) => reader,
        Err(
            EcstoreError::FileNotFound
            | EcstoreError::VolumeNotFound
            | EcstoreError::ObjectNotFound(_, _)
            | EcstoreError::BucketNotFound(_)
            | EcstoreError::ConfigNotFound,
        ) => return Ok((CurrentCycle::default(), 0, DataUsageCacheRevision::Missing)),
        Err(err) => return Err(ScannerError::Other(format!("failed to inspect scanner cycle state: {err}"))),
    };
    if reader.object_info.is_dir || reader.object_info.size < 0 {
        return Err(ScannerError::Other(
            "scanner usage reset requires a regular scanner cycle state object".to_string(),
        ));
    }
    let revision = reader
        .object_info
        .etag
        .as_ref()
        .filter(|etag| !etag.is_empty())
        .cloned()
        .map(DataUsageCacheRevision::Etag)
        .ok_or_else(|| ScannerError::Other("scanner cycle state has no revision".to_string()))?;
    let data = read_cycle_state_body(&mut reader)
        .await
        .map_err(|err| ScannerError::Other(format!("failed to read scanner cycle state for usage reset: {err}")))?;
    let (cycle, leader_epoch) = decode_scanner_cycle_state(&data).map_err(|err| {
        ScannerError::Other(format!(
            "scanner usage reset requires a valid scanner cycle state; reset scanner cycle state first: {err}"
        ))
    })?;
    Ok((cycle, leader_epoch, revision))
}

async fn delete_usage_state_reset_slot(
    storeapi: Arc<impl ScannerObjectIO + ScannerConfigObjectDelete>,
    slot: &ScannerUsageStateResetSlot,
    expected_epoch: u64,
    owns_reset: &(impl Fn() -> bool + Sync),
) -> Result<bool, ScannerError> {
    if matches!(slot.revision, DataUsageCacheRevision::Missing) {
        return Ok(false);
    }
    let delete_result = delete_reset_config(
        storeapi.clone(),
        RUSTFS_META_BUCKET,
        &slot.path,
        ScannerObjectOptions {
            delete_prefix: false,
            http_preconditions: Some(slot.revision.preconditions()),
            ..Default::default()
        },
        expected_epoch,
        owns_reset,
    )
    .await;
    match delete_result {
        Ok(_) => Ok(true),
        Err(EcstoreError::FileNotFound | EcstoreError::ConfigNotFound | EcstoreError::ObjectNotFound(_, _)) => Ok(false),
        Err(EcstoreError::PreconditionFailed) => Err(ScannerError::Other(format!(
            "scanner usage reset slot changed while deleting {}",
            slot.path
        ))),
        Err(err) if scanner_publication_epoch_changed(&err) => {
            Err(ScannerError::Other("scanner usage reset deferred by a movement epoch change".to_string()))
        }
        Err(err) => Err(ScannerError::Other(format!(
            "failed to delete scanner usage reset slot {}: {err}",
            slot.path
        ))),
    }
}

#[derive(Clone, Copy)]
pub(super) enum ScannerUsageBootstrapPublishContext {
    Initial,
    Recovery,
    Reset,
}

enum ScannerUsageBootstrapPublishError {
    Encode(serde_json::Error),
    Reconcile(EcstoreError),
    MissingEtag,
    Save(EcstoreError),
}

impl ScannerUsageBootstrapPublishError {
    fn into_scanner_error(self, context: ScannerUsageBootstrapPublishContext) -> ScannerError {
        let message = match context {
            ScannerUsageBootstrapPublishContext::Initial => match self {
                Self::Encode(err) => format!("failed to encode scanner usage baseline bootstrap: {err}"),
                Self::Reconcile(err) => format!("failed to reconcile scanner usage bootstrap: {err}"),
                Self::MissingEtag => "scanner usage bootstrap returned no ETag and could not be confirmed".to_string(),
                Self::Save(err) => format!("failed to persist scanner usage bootstrap: {err}"),
            },
            ScannerUsageBootstrapPublishContext::Recovery => match self {
                Self::Encode(err) => format!("failed to encode recovered scanner usage bootstrap: {err}"),
                Self::Reconcile(err) => format!("failed to reconcile recovered scanner usage bootstrap: {err}"),
                Self::MissingEtag => "recovered scanner usage bootstrap returned no ETag and could not be confirmed".to_string(),
                Self::Save(err) => format!("failed to recover legacy incomplete scanner usage floor: {err}"),
            },
            ScannerUsageBootstrapPublishContext::Reset => match self {
                Self::Encode(err) => format!("failed to encode scanner usage reset bootstrap marker: {err}"),
                Self::Reconcile(err) => format!("failed to reconcile scanner usage reset bootstrap marker: {err}"),
                Self::MissingEtag => "scanner usage reset bootstrap returned no ETag and could not be confirmed".to_string(),
                Self::Save(err) if scanner_publication_epoch_changed(&err) => {
                    "scanner usage reset deferred by a movement epoch change".to_string()
                }
                Self::Save(EcstoreError::PreconditionFailed) => {
                    "scanner usage reset primary slot changed before bootstrap publish".to_string()
                }
                Self::Save(err) => format!("failed to persist scanner usage reset bootstrap: {err}"),
            },
        };
        ScannerError::Other(message)
    }
}

pub(super) async fn publish_scanner_usage_bootstrap_primary(
    storeapi: Arc<impl ScannerObjectIO + ScannerConfigObjectDelete>,
    expected_revision: &DataUsageCacheRevision,
    expected_publication_epoch: u64,
    leader_epoch: Option<u64>,
    context: ScannerUsageBootstrapPublishContext,
    owns_publication: impl Fn() -> bool + Sync,
) -> Result<(), ScannerError> {
    async fn inner(
        storeapi: Arc<impl ScannerObjectIO + ScannerConfigObjectDelete>,
        expected_revision: &DataUsageCacheRevision,
        expected_publication_epoch: u64,
        leader_epoch: Option<u64>,
        owns_publication: &(impl Fn() -> bool + Sync),
    ) -> Result<(), ScannerUsageBootstrapPublishError> {
        let marker = scanner_usage_bootstrap_marker(std::time::SystemTime::now(), leader_epoch);
        let data = serde_json::to_vec(&marker).map_err(ScannerUsageBootstrapPublishError::Encode)?;
        let save_result = save_reset_config(
            storeapi.clone(),
            DATA_USAGE_OBJ_NAME_PATH.as_str(),
            data.clone(),
            expected_revision.preconditions(),
            expected_publication_epoch,
            owns_publication,
        )
        .await;
        if save_result
            .as_ref()
            .ok()
            .and_then(|info| info.etag.as_deref())
            .is_some_and(|etag| !etag.is_empty())
        {
            return Ok(());
        }

        let (persisted, revision) = read_config_with_revision(storeapi, DATA_USAGE_OBJ_NAME_PATH.as_str())
            .await
            .map_err(ScannerUsageBootstrapPublishError::Reconcile)?;
        if persisted.as_deref() == Some(data.as_slice()) && matches!(revision, DataUsageCacheRevision::Etag(_)) {
            return Ok(());
        }
        Err(match save_result {
            Ok(_) => ScannerUsageBootstrapPublishError::MissingEtag,
            Err(err) => ScannerUsageBootstrapPublishError::Save(err),
        })
    }

    inner(storeapi, expected_revision, expected_publication_epoch, leader_epoch, &owns_publication)
        .await
        .map_err(|err| err.into_scanner_error(context))
}

pub(super) async fn reset_scanner_usage_state_slots_for_full_rebuild(
    storeapi: Arc<impl ScannerObjectIO + ScannerConfigObjectDelete>,
    slots: &[ScannerUsageStateResetSlot],
    expected_epoch: u64,
    leader_epoch: u64,
    owns_reset: impl Fn() -> bool + Sync,
) -> Result<Vec<String>, ScannerError> {
    let mut reset_paths = Vec::new();
    let primary = slots
        .iter()
        .find(|slot| slot.path == DATA_USAGE_OBJ_NAME_PATH.as_str())
        .ok_or_else(|| ScannerError::Other("scanner usage reset primary slot was not inspected".to_string()))?;
    if !owns_reset() {
        return Err(ScannerError::Other("scanner usage reset ownership was lost".to_string()));
    }
    let resume_epoch = usage_state_reset_resume_epoch(slots)?;
    match resume_epoch {
        Some(epoch) if epoch == leader_epoch => {}
        Some(_) => return Err(ScannerError::Other("scanner usage reset bootstrap epoch changed".to_string())),
        None => {
            publish_scanner_usage_bootstrap_primary(
                storeapi.clone(),
                &primary.revision,
                expected_epoch,
                Some(leader_epoch),
                ScannerUsageBootstrapPublishContext::Reset,
                &owns_reset,
            )
            .await?;
        }
    }
    let (data, intent_revision) = read_config_with_revision(storeapi.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
        .await
        .map_err(|err| ScannerError::Other(format!("failed to inspect scanner usage reset intent: {err}")))?;
    data.as_deref()
        .and_then(|data| serde_json::from_slice::<DataUsageInfo>(data).ok())
        .filter(|usage| data_usage_info_is_bootstrap_pending(usage) && usage.scanner_epoch == Some(leader_epoch))
        .ok_or_else(|| ScannerError::Other("scanner usage reset intent changed before cleanup".to_string()))?;
    if !matches!(intent_revision, DataUsageCacheRevision::Etag(_))
        || (resume_epoch.is_some() && intent_revision != primary.revision)
    {
        return Err(ScannerError::Other("scanner usage reset intent revision changed".to_string()));
    }
    reset_paths.push(DATA_USAGE_OBJ_NAME_PATH.as_str().to_string());

    for slot in slots.iter().filter(|slot| slot.path != DATA_USAGE_OBJ_NAME_PATH.as_str()) {
        if let Some(usage) = slot
            .data
            .as_deref()
            .and_then(|data| serde_json::from_slice::<DataUsageInfo>(data).ok())
            && usage_epoch(&usage) >= leader_epoch
        {
            return Err(ScannerError::Other(format!(
                "scanner usage reset slot is not older than its intent: {}",
                slot.path
            )));
        }
        let revision = read_config_revision(storeapi.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
            .await
            .map_err(|err| ScannerError::Other(format!("failed to verify scanner usage reset intent: {err}")))?;
        if revision != intent_revision {
            return Err(ScannerError::Other("scanner usage reset intent changed during cleanup".to_string()));
        }
        if !owns_reset() {
            return Err(ScannerError::Other("scanner usage reset ownership was lost".to_string()));
        }
        if delete_usage_state_reset_slot(storeapi.clone(), slot, expected_epoch, &owns_reset).await? {
            reset_paths.push(slot.path.clone());
        }
    }
    let revision = read_config_revision(storeapi.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
        .await
        .map_err(|err| ScannerError::Other(format!("failed to confirm scanner usage reset intent: {err}")))?;
    if revision != intent_revision || !owns_reset() {
        return Err(ScannerError::Other(
            "scanner usage reset intent or ownership changed before completion".to_string(),
        ));
    }
    invalidate_admin_data_usage_snapshot_cache().await;
    invalidate_data_usage_snapshot_cache().await;
    Ok(reset_paths)
}

fn usage_state_reset_resume_epoch(slots: &[ScannerUsageStateResetSlot]) -> Result<Option<u64>, ScannerError> {
    let primary = slots.iter().find(|slot| slot.path == DATA_USAGE_OBJ_NAME_PATH.as_str());
    let usage = primary
        .and_then(|slot| slot.data.as_deref())
        .and_then(|data| serde_json::from_slice::<DataUsageInfo>(data).ok());
    match usage {
        Some(usage) if usage.usage_snapshot_bootstrap_pending => {
            if !data_usage_info_is_bootstrap_pending(&usage) {
                return Err(ScannerError::Other("scanner usage reset bootstrap is invalid".to_string()));
            }
            if usage.scanner_epoch.is_none() {
                // Initial bootstrap has no reset owner yet.
                return Ok(None);
            }
            usage
                .scanner_epoch
                .filter(|epoch| *epoch > 0 && *epoch < u64::MAX)
                .map(Some)
                .ok_or_else(|| ScannerError::Other("scanner usage reset bootstrap has no valid epoch".to_string()))
        }
        _ => Ok(None),
    }
}

pub async fn reset_scanner_usage_state_for_full_rebuild(
    ctx: CancellationToken,
    storeapi: Arc<ECStore>,
) -> Result<ScannerUsageStateResetResult, ScannerError> {
    let lock = storeapi
        .new_ns_lock(RUSTFS_META_BUCKET, "leader.lock")
        .await
        .map_err(|err| ScannerError::Other(format!("failed to acquire scanner leader lock: {err}")))?;
    let guard = lock
        .get_write_lock_quiet(Duration::from_secs(5))
        .await
        .map_err(|err| ScannerError::Other(format!("scanner leader lock is busy: {err}")))?;
    if guard.is_lock_lost() {
        return Err(ScannerError::Other("scanner leader lock was lost before usage reset".to_string()));
    }
    if ctx.is_cancelled() {
        return Err(ScannerError::Other("scanner usage reset was cancelled".to_string()));
    }

    let Some(reset_epoch) = scanner_publication_epoch(storeapi.clone()).await else {
        return Err(ScannerError::Other("scanner usage reset is blocked by data movement".to_string()));
    };
    let (cycle, cycle_epoch, cycle_revision) = read_cycle_state_for_usage_reset(storeapi.clone()).await?;
    let slots = read_usage_state_reset_slots(storeapi.clone()).await?;
    let usage_floor = match usage_state_reset_floor(&slots)? {
        ScannerUsageResetFloor::Trusted(floor) => floor,
        ScannerUsageResetFloor::Corrupt if matches!(cycle_revision, DataUsageCacheRevision::Missing) => {
            return Err(ScannerError::Other("scanner usage reset has no trusted cycle or usage floor".to_string()));
        }
        ScannerUsageResetFloor::Missing | ScannerUsageResetFloor::Corrupt => PersistedUsageFloor {
            next_cycle: cycle.next,
            leader_epoch: cycle_epoch,
        },
    };
    let resume_epoch = usage_state_reset_resume_epoch(&slots)?;
    let leader_epoch = if let Some(epoch) = resume_epoch {
        if epoch != cycle_epoch || usage_floor.leader_epoch > epoch || usage_floor.next_cycle > cycle.next {
            return Err(ScannerError::Other(
                "scanner usage reset bootstrap conflicts with the persisted cycle fence".to_string(),
            ));
        }
        epoch
    } else {
        cycle_epoch
            .max(usage_floor.leader_epoch)
            .checked_add(1)
            .filter(|epoch| *epoch < u64::MAX)
            .ok_or_else(|| ScannerError::Other("scanner leader epoch is exhausted".to_string()))?
    };
    let rebuilt_cycle = CurrentCycle {
        next: cycle.next.max(usage_floor.next_cycle),
        ..Default::default()
    };
    let cycle_data = encode_scanner_cycle_state(&rebuilt_cycle, leader_epoch)
        .map_err(|err| ScannerError::Other(format!("failed to encode scanner cycle state for usage reset: {err}")))?;

    if guard.is_lock_lost() {
        return Err(ScannerError::Other(
            "scanner leader lock was lost before fencing usage reset cycle state".to_string(),
        ));
    }
    if resume_epoch.is_none() {
        save_reset_config(
            storeapi.clone(),
            DATA_USAGE_BLOOM_NAME_PATH.as_str(),
            cycle_data,
            cycle_revision.preconditions(),
            reset_epoch,
            &|| !guard.is_lock_lost() && !ctx.is_cancelled(),
        )
        .await
        .map_err(|err| {
            if scanner_publication_epoch_changed(&err) {
                ScannerError::Other("scanner usage reset deferred by a movement epoch change".to_string())
            } else {
                ScannerError::Other(format!("failed to fence scanner cycle state for usage reset: {err}"))
            }
        })?;
    }

    if guard.is_lock_lost() {
        return Err(ScannerError::Other(
            "scanner leader lock was lost before publishing usage reset marker".to_string(),
        ));
    }
    let reset_paths =
        reset_scanner_usage_state_slots_for_full_rebuild(storeapi.clone(), &slots, reset_epoch, leader_epoch, || {
            !guard.is_lock_lost() && !ctx.is_cancelled()
        })
        .await?;
    if guard.is_lock_lost() {
        return Err(ScannerError::Other(
            "scanner leader lock was lost after publishing usage reset marker".to_string(),
        ));
    }

    clear_scanner_usage_floor_failure();
    clear_legacy_incomplete_usage_floor_recovery_status();
    set_scanner_cycle_recovery_status(recovery_status("healthy", None, false));
    super::notify_scanner_cycle_recovery_wake();
    info!(
        target: "rustfs::scanner",
        event = EVENT_SCANNER_PERSIST_STATE,
        component = LOG_COMPONENT_SCANNER,
        subsystem = LOG_SUBSYSTEM_RUNTIME,
        state = "usage_state_reset",
        mode = SCANNER_USAGE_STATE_RESET_MODE_FULL_REBUILD,
        leader_epoch,
        next_cycle = rebuilt_cycle.next,
        reset_paths = reset_paths.len(),
        "Scanner usage state reset was published"
    );

    Ok(ScannerUsageStateResetResult {
        status: "reset".to_string(),
        mode: SCANNER_USAGE_STATE_RESET_MODE_FULL_REBUILD.to_string(),
        usage_state: "bootstrap-pending".to_string(),
        leader_epoch,
        next_cycle: rebuilt_cycle.next,
        reset_paths,
    })
}

#[derive(Debug, thiserror::Error)]
pub(super) enum ScannerCycleStateError {
    #[error("failed to encode scanner cycle state: {0}")]
    Encode(#[from] rmp_serde::encode::Error),
    #[error("failed to decode scanner cycle state: {0}")]
    Decode(#[from] rmp_serde::decode::Error),
    #[error("{0}")]
    InvalidData(&'static str),
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct PersistedUsageFloor {
    pub(super) next_cycle: u64,
    pub(super) leader_epoch: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum PersistedUsageFloorStartup {
    Authoritative,
    Missing,
    BootstrapPending,
    RecoveredLegacyIncompleteFence,
}

#[derive(Clone, Debug)]
struct LegacyIncompleteUsageFloorPrimary {
    revision: DataUsageCacheRevision,
    epoch: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct LegacyIncompleteUsageFloorRecoveryMarker {
    schema_version: u16,
    primary_revision: String,
    leader_epoch: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct LegacyIncompleteUsageFence {
    claimable_epoch: Option<u64>,
}

impl LegacyIncompleteUsageFence {
    fn new(claimable_epoch: Option<u64>) -> Self {
        Self { claimable_epoch }
    }

    fn claimable_epoch(self) -> Option<u64> {
        self.claimable_epoch
    }
}

async fn read_legacy_incomplete_usage_floor_recovery_marker(
    storeapi: Arc<impl ScannerObjectIO>,
) -> Result<Option<(LegacyIncompleteUsageFloorRecoveryMarker, DataUsageCacheRevision)>, ScannerError> {
    let (data, revision) = read_config_with_revision(storeapi, DATA_USAGE_RECOVERY_PATH.as_str())
        .await
        .map_err(|err| ScannerError::Other(format!("failed to read scanner usage recovery marker: {err}")))?;
    let Some(data) = data else {
        return Ok(None);
    };
    let marker = serde_json::from_slice::<LegacyIncompleteUsageFloorRecoveryMarker>(&data)
        .map_err(|err| ScannerError::Other(format!("failed to decode scanner usage recovery marker: {err}")))?;
    if marker.schema_version != 1 || marker.primary_revision.is_empty() || marker.leader_epoch == 0 {
        return Err(ScannerError::Other("scanner usage recovery marker is invalid".to_string()));
    }
    if !matches!(revision, DataUsageCacheRevision::Etag(_)) {
        return Err(ScannerError::Other("scanner usage recovery marker has no revision".to_string()));
    }
    Ok(Some((marker, revision)))
}

async fn clear_legacy_incomplete_usage_floor_recovery_marker(
    storeapi: Arc<impl ScannerObjectIO + ScannerConfigObjectDelete>,
    marker_revision: &DataUsageCacheRevision,
    expected_publication_epoch: u64,
) -> Result<(), ScannerError> {
    let delete_result = delete_config_with_publication_admission_for_epoch(
        storeapi.clone(),
        RUSTFS_META_BUCKET,
        DATA_USAGE_RECOVERY_PATH.as_str(),
        ScannerObjectOptions {
            delete_prefix: false,
            http_preconditions: Some(marker_revision.preconditions()),
            ..Default::default()
        },
        expected_publication_epoch,
    )
    .await;
    match delete_result {
        Ok(_) => Ok(()),
        Err(err) => {
            let (_, revision) = read_config_with_revision(storeapi, DATA_USAGE_RECOVERY_PATH.as_str())
                .await
                .map_err(|read_err| {
                    ScannerError::Other(format!("failed to reconcile scanner usage recovery cleanup: {read_err}"))
                })?;
            if matches!(revision, DataUsageCacheRevision::Missing) {
                Ok(())
            } else {
                Err(ScannerError::Other(format!("failed to clear scanner usage recovery marker: {err}")))
            }
        }
    }
}

pub(super) async fn complete_legacy_incomplete_usage_floor_recovery(
    storeapi: Arc<impl ScannerObjectIO + ScannerConfigObjectDelete>,
    claimed_epoch: u64,
) -> Result<(), ScannerError> {
    let Some((marker, marker_revision)) = read_legacy_incomplete_usage_floor_recovery_marker(storeapi.clone()).await? else {
        return Ok(());
    };
    if claimed_epoch <= marker.leader_epoch {
        return Err(ScannerError::Other("scanner usage recovery did not advance the leader epoch".to_string()));
    }
    let (primary, _) = read_config_with_revision(storeapi.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
        .await
        .map_err(|err| ScannerError::Other(format!("failed to verify recovered scanner usage bootstrap: {err}")))?;
    let primary = primary.ok_or_else(|| ScannerError::Other("recovered scanner usage bootstrap is missing".to_string()))?;
    let usage = serde_json::from_slice::<DataUsageInfo>(&primary)
        .map_err(|err| ScannerError::Other(format!("failed to decode recovered scanner usage bootstrap: {err}")))?;
    if !data_usage_info_is_bootstrap_pending(&usage) || usage.scanner_epoch != Some(claimed_epoch) {
        return Err(ScannerError::Other(
            "recovered scanner usage bootstrap does not match the claimed epoch".to_string(),
        ));
    }
    let expected_publication_epoch = scanner_publication_epoch(storeapi.clone())
        .await
        .ok_or_else(|| ScannerError::Other("scanner usage recovery cleanup is blocked by data movement".to_string()))?;
    clear_legacy_incomplete_usage_floor_recovery_marker(storeapi, &marker_revision, expected_publication_epoch).await?;
    clear_legacy_incomplete_usage_floor_recovery_status();
    Ok(())
}

struct LegacyOptional<T> {
    present: bool,
    value: Option<T>,
}

impl<T> Default for LegacyOptional<T> {
    fn default() -> Self {
        Self {
            present: false,
            value: None,
        }
    }
}

fn deserialize_legacy_optional<'de, D, T>(deserializer: D) -> Result<LegacyOptional<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer).map(|value| LegacyOptional { present: true, value })
}

struct LegacyUniqueMap<V>(std::collections::HashMap<String, V>);

impl<'de, V> Deserialize<'de> for LegacyUniqueMap<V>
where
    V: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct UniqueMapVisitor<V>(std::marker::PhantomData<V>);

        impl<'de, V> serde::de::Visitor<'de> for UniqueMapVisitor<V>
        where
            V: Deserialize<'de>,
        {
            type Value = LegacyUniqueMap<V>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a JSON object without duplicate keys")
            }

            fn visit_map<A>(self, mut entries: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut values = std::collections::HashMap::new();
                while let Some((key, value)) = entries.next_entry::<String, V>()? {
                    match values.entry(key) {
                        std::collections::hash_map::Entry::Vacant(entry) => {
                            entry.insert(value);
                        }
                        std::collections::hash_map::Entry::Occupied(entry) => {
                            return Err(serde::de::Error::custom(format!("duplicate map key `{}`", entry.key())));
                        }
                    }
                }
                Ok(LegacyUniqueMap(values))
            }
        }

        deserializer.deserialize_map(UniqueMapVisitor(std::marker::PhantomData))
    }
}

impl<V> LegacyUniqueMap<V> {
    fn len(&self) -> usize {
        self.0.len()
    }

    fn get(&self, key: &str) -> Option<&V> {
        self.0.get(key)
    }

    fn iter(&self) -> impl Iterator<Item = (&String, &V)> {
        self.0.iter()
    }

    fn values(&self) -> impl Iterator<Item = &V> {
        self.0.values()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyBucketTargetUsageWire {
    #[serde(rename = "replication_pending_size")]
    _replication_pending_size: serde::de::IgnoredAny,
    #[serde(rename = "replication_failed_size")]
    _replication_failed_size: serde::de::IgnoredAny,
    #[serde(rename = "replicated_size")]
    _replicated_size: serde::de::IgnoredAny,
    #[serde(rename = "replica_size")]
    _replica_size: serde::de::IgnoredAny,
    #[serde(rename = "replication_pending_count")]
    _replication_pending_count: serde::de::IgnoredAny,
    #[serde(rename = "replication_failed_count")]
    _replication_failed_count: serde::de::IgnoredAny,
    #[serde(rename = "replicated_count")]
    _replicated_count: serde::de::IgnoredAny,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyBucketUsageWire {
    size: u64,
    #[serde(rename = "replication_pending_size_v1")]
    _replication_pending_size_v1: serde::de::IgnoredAny,
    #[serde(rename = "replication_failed_size_v1")]
    _replication_failed_size_v1: serde::de::IgnoredAny,
    #[serde(rename = "replicated_size_v1")]
    _replicated_size_v1: serde::de::IgnoredAny,
    #[serde(rename = "replication_pending_count_v1")]
    _replication_pending_count_v1: serde::de::IgnoredAny,
    #[serde(rename = "replication_failed_count_v1")]
    _replication_failed_count_v1: serde::de::IgnoredAny,
    objects_count: u64,
    #[serde(rename = "object_size_histogram")]
    _object_size_histogram: LegacyUniqueMap<u64>,
    #[serde(rename = "object_versions_histogram")]
    _object_versions_histogram: LegacyUniqueMap<u64>,
    versions_count: u64,
    delete_markers_count: u64,
    #[serde(rename = "replica_size")]
    _replica_size: serde::de::IgnoredAny,
    #[serde(rename = "replica_count")]
    _replica_count: serde::de::IgnoredAny,
    #[serde(rename = "replication_info")]
    _replication_info: LegacyUniqueMap<LegacyBucketTargetUsageWire>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyDiskUsageStatusWire {
    #[serde(rename = "disk_id")]
    _disk_id: serde::de::IgnoredAny,
    #[serde(rename = "pool_index")]
    _pool_index: serde::de::IgnoredAny,
    #[serde(rename = "set_index")]
    _set_index: serde::de::IgnoredAny,
    #[serde(rename = "disk_index")]
    _disk_index: serde::de::IgnoredAny,
    #[serde(rename = "last_update")]
    _last_update: serde::de::IgnoredAny,
    #[serde(rename = "snapshot_exists")]
    _snapshot_exists: serde::de::IgnoredAny,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyTierStatsWire {
    #[serde(rename = "total_size")]
    _total_size: serde::de::IgnoredAny,
    #[serde(rename = "num_versions")]
    _num_versions: serde::de::IgnoredAny,
    #[serde(rename = "num_objects")]
    _num_objects: serde::de::IgnoredAny,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyAllTierStatsWire {
    #[serde(rename = "tiers")]
    _tiers: LegacyUniqueMap<LegacyTierStatsWire>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyUsageWire {
    #[serde(rename = "total_capacity")]
    _total_capacity: serde::de::IgnoredAny,
    #[serde(rename = "total_used_capacity")]
    _total_used_capacity: serde::de::IgnoredAny,
    #[serde(rename = "total_free_capacity")]
    _total_free_capacity: serde::de::IgnoredAny,
    #[serde(rename = "last_update")]
    _last_update: serde::de::IgnoredAny,
    #[serde(default, deserialize_with = "deserialize_legacy_optional")]
    scanner_epoch: LegacyOptional<u64>,
    objects_total_count: u64,
    versions_total_count: u64,
    delete_markers_total_count: u64,
    objects_total_size: u64,
    #[serde(rename = "replication_info")]
    _replication_info: LegacyUniqueMap<LegacyBucketTargetUsageWire>,
    #[serde(default, deserialize_with = "deserialize_legacy_optional")]
    tier_stats: LegacyOptional<LegacyAllTierStatsWire>,
    buckets_count: u64,
    buckets_usage: LegacyUniqueMap<LegacyBucketUsageWire>,
    usage_snapshot_complete: bool,
    bucket_sizes: LegacyUniqueMap<u64>,
    #[serde(rename = "disk_usage_status")]
    _disk_usage_status: Vec<LegacyDiskUsageStatusWire>,
}

fn decode_legacy_usage_wire(data: &[u8], usage: &DataUsageInfo) -> Option<LegacyUsageWire> {
    let wire = serde_json::from_slice::<LegacyUsageWire>(data).ok()?;
    if wire.scanner_epoch.present != usage.scanner_epoch.is_some()
        || wire.scanner_epoch.value != usage.scanner_epoch
        || wire.tier_stats.present != usage.tier_stats.is_some()
    {
        return None;
    }
    Some(wire)
}

fn legacy_empty_usage_fence(data: &[u8], usage: &DataUsageInfo) -> Option<LegacyIncompleteUsageFence> {
    if usage.last_update.is_none() || usage.scanner_cycle.is_some() {
        return None;
    }
    if usage.scanner_epoch.is_some_and(|epoch| epoch == 0 || epoch >= u64::MAX - 1) {
        return None;
    }
    let expected = DataUsageInfo {
        last_update: usage.last_update,
        scanner_epoch: usage.scanner_epoch,
        ..Default::default()
    };
    if usage != &expected {
        return None;
    }

    // RUSTFS_COMPAT_TODO(backlog-2102): accept only the exact empty usage fence serialized by rc.2/rc.3. Remove after those releases are no longer supported direct-upgrade sources.
    let wire = decode_legacy_usage_wire(data, usage)?;
    Some(LegacyIncompleteUsageFence::new(wire.scanner_epoch.value))
}

fn legacy_incomplete_usage_fence(data: &[u8], usage: &DataUsageInfo) -> Option<LegacyIncompleteUsageFence> {
    legacy_empty_usage_fence(data, usage).or_else(|| legacy_non_empty_usage_fence(data, usage))
}

// RUSTFS_COMPAT_TODO(backlog-2122): accept rc.1-rc.3 usage floors that were fenced before a scanner cycle completed. Remove after those releases are no longer supported direct-upgrade sources.
fn legacy_non_empty_usage_fence(data: &[u8], usage: &DataUsageInfo) -> Option<LegacyIncompleteUsageFence> {
    if usage.last_update.is_none()
        || usage.scanner_cycle.is_some()
        || usage.usage_snapshot_bootstrap_pending
        || usage.usage_snapshot_complete
        || usage.usage_snapshot_converged.is_some()
        || usage.usage_snapshot_authoritative_baseline.is_some()
        || !usage.usage_snapshot_set_states.is_empty()
        || usage.usage_snapshot_partial
        || usage.buckets_count == 0
        || u64::try_from(usage.buckets_usage.len()).ok() != Some(usage.buckets_count)
    {
        return None;
    }
    if usage.scanner_epoch.is_some_and(|epoch| epoch == 0 || epoch >= u64::MAX - 1) {
        return None;
    }
    let wire = decode_legacy_usage_wire(data, usage)?;
    if wire.usage_snapshot_complete
        || wire.buckets_count == 0
        || u64::try_from(wire.buckets_usage.len()).ok() != Some(wire.buckets_count)
        || wire.bucket_sizes.len() != wire.buckets_usage.len()
        || wire
            .buckets_usage
            .iter()
            .any(|(bucket, bucket_usage)| wire.bucket_sizes.get(bucket) != Some(&bucket_usage.size))
    {
        return None;
    }
    let (objects, versions, delete_markers, size) = wire.buckets_usage.values().try_fold(
        (0_u64, 0_u64, 0_u64, 0_u64),
        |(objects, versions, delete_markers, size), bucket| {
            Some((
                objects.checked_add(bucket.objects_count)?,
                versions.checked_add(bucket.versions_count)?,
                delete_markers.checked_add(bucket.delete_markers_count)?,
                size.checked_add(bucket.size)?,
            ))
        },
    )?;
    if (objects, versions, delete_markers, size)
        != (
            wire.objects_total_count,
            wire.versions_total_count,
            wire.delete_markers_total_count,
            wire.objects_total_size,
        )
    {
        return None;
    }
    Some(LegacyIncompleteUsageFence::new(wire.scanner_epoch.value))
}

async fn recover_legacy_incomplete_usage_floor(
    storeapi: Arc<impl ScannerObjectIO + ScannerConfigObjectDelete>,
    primary: LegacyIncompleteUsageFloorPrimary,
    expected_publication_epoch: u64,
) -> Result<(), ScannerError> {
    let DataUsageCacheRevision::Etag(primary_revision) = &primary.revision else {
        return Err(ScannerError::Other("legacy incomplete scanner usage floor has no revision".to_string()));
    };
    let marker = LegacyIncompleteUsageFloorRecoveryMarker {
        schema_version: 1,
        primary_revision: primary_revision.clone(),
        leader_epoch: primary.epoch,
    };
    let marker_data = serde_json::to_vec(&marker)
        .map_err(|err| ScannerError::Other(format!("failed to encode scanner usage recovery marker: {err}")))?;
    match read_legacy_incomplete_usage_floor_recovery_marker(storeapi.clone()).await? {
        Some((persisted, _)) if persisted != marker => {
            return Err(ScannerError::Other(
                "scanner usage recovery marker conflicts with the persisted incomplete floor".to_string(),
            ));
        }
        Some(_) => {}
        None => {
            let marker_save = save_config_with_publication_admission_for_epoch(
                storeapi.clone(),
                DATA_USAGE_RECOVERY_PATH.as_str(),
                marker_data.clone(),
                DataUsageCacheRevision::Missing.preconditions(),
                expected_publication_epoch,
            )
            .await;
            if !marker_save
                .as_ref()
                .ok()
                .and_then(|info| info.etag.as_deref())
                .is_some_and(|etag| !etag.is_empty())
            {
                let persisted = read_legacy_incomplete_usage_floor_recovery_marker(storeapi.clone()).await?;
                if persisted.as_ref().map(|(persisted, _)| persisted) != Some(&marker) {
                    return Err(ScannerError::Other(match marker_save {
                        Ok(_) => "scanner usage recovery marker returned no ETag and could not be confirmed".to_string(),
                        Err(err) => format!("failed to persist scanner usage recovery marker: {err}"),
                    }));
                }
            }
        }
    }

    publish_scanner_usage_bootstrap_primary(
        storeapi.clone(),
        &primary.revision,
        expected_publication_epoch,
        Some(primary.epoch),
        ScannerUsageBootstrapPublishContext::Recovery,
        || true,
    )
    .await?;
    warn!(
        target: "rustfs::scanner",
        event = EVENT_SCANNER_PERSIST_STATE,
        component = LOG_COMPONENT_SCANNER,
        subsystem = LOG_SUBSYSTEM_RUNTIME,
        // Keep the published state stable for existing empty-floor alerts.
        state = "legacy_empty_usage_floor_recovered",
        path = %DATA_USAGE_OBJ_NAME_PATH.as_str(),
        scanner_epoch = primary.epoch,
        "Scanner recovered a legacy incomplete usage floor"
    );
    Ok(())
}

pub(super) fn encode_scanner_cycle_state(
    cycle_info: &CurrentCycle,
    leader_epoch: u64,
) -> Result<Vec<u8>, ScannerCycleStateError> {
    if cycle_info.next == u64::MAX {
        return Err(ScannerCycleStateError::InvalidData("scanner cycle counter is exhausted"));
    }
    let cycle_info_buf = rmp_serde::to_vec(cycle_info)?;
    let mut buf = Vec::with_capacity(cycle_info_buf.len() + SCANNER_CYCLE_STATE_HEADER_LEN);
    buf.extend_from_slice(&cycle_info.next.to_le_bytes());
    buf.extend_from_slice(SCANNER_CYCLE_STATE_MAGIC);
    buf.extend_from_slice(&leader_epoch.to_le_bytes());
    buf.extend_from_slice(&cycle_info_buf);
    Ok(buf)
}

pub(super) fn decode_scanner_cycle_state(buf: &[u8]) -> Result<(CurrentCycle, u64), ScannerCycleStateError> {
    if buf.len() < 8 {
        return Err(ScannerCycleStateError::InvalidData("scanner cycle state is truncated"));
    }

    let persisted_next = u64::from_le_bytes(
        buf[0..8]
            .try_into()
            .map_err(|_| ScannerCycleStateError::InvalidData("scanner cycle counter is truncated"))?,
    );
    if persisted_next == u64::MAX {
        return Err(ScannerCycleStateError::InvalidData("scanner cycle counter is exhausted"));
    }
    if buf.len() == 8 {
        return Ok((
            CurrentCycle {
                next: persisted_next,
                ..Default::default()
            },
            0,
        ));
    }

    let (leader_epoch, payload) = if buf.len() >= 16 && &buf[8..16] == SCANNER_CYCLE_STATE_MAGIC {
        if buf.len() < SCANNER_CYCLE_STATE_HEADER_LEN {
            return Err(ScannerCycleStateError::InvalidData("scanner cycle fencing header is truncated"));
        }
        let epoch = u64::from_le_bytes(
            buf[16..24]
                .try_into()
                .map_err(|_| ScannerCycleStateError::InvalidData("scanner leader epoch is truncated"))?,
        );
        if epoch == 0 {
            return Err(ScannerCycleStateError::InvalidData("scanner leader epoch is zero"));
        }
        (epoch, &buf[SCANNER_CYCLE_STATE_HEADER_LEN..])
    } else {
        (0, &buf[8..])
    };

    let mut deserializer = rmp_serde::Deserializer::new(std::io::Cursor::new(payload));
    let cycle_info = CurrentCycle::deserialize(&mut deserializer)?;
    if deserializer.position() != u64::try_from(payload.len()).unwrap_or(u64::MAX) {
        return Err(ScannerCycleStateError::InvalidData("scanner cycle state has trailing bytes"));
    }
    if cycle_info.next != persisted_next {
        return Err(ScannerCycleStateError::InvalidData("scanner cycle counter disagrees with encoded state"));
    }
    Ok((cycle_info, leader_epoch))
}

pub(crate) fn decode_persisted_scanner_cycle_fence(buf: &[u8]) -> Result<(u64, u64), ScannerError> {
    decode_scanner_cycle_state(buf)
        .map(|(cycle, leader_epoch)| (cycle.next, leader_epoch))
        .map_err(|err| ScannerError::Other(format!("persisted scanner cycle state is invalid: {err}")))
}

#[cfg(test)]
pub(crate) fn encode_scanner_cycle_fence_for_test(next_cycle: u64, leader_epoch: u64) -> Vec<u8> {
    encode_scanner_cycle_state(
        &CurrentCycle {
            next: next_cycle,
            ..Default::default()
        },
        leader_epoch,
    )
    .expect("test scanner cycle fence should encode")
}

pub(crate) async fn current_scanner_leader_epoch() -> Result<u64, ScannerError> {
    let store = crate::resolve_scanner_object_store_handle()
        .ok_or_else(|| ScannerError::Other("scanner object layer is unavailable".to_string()))?;
    match read_config(store, &DATA_USAGE_BLOOM_NAME_PATH).await {
        Ok(buf) => {
            let (_, leader_epoch) = decode_persisted_scanner_cycle_fence(&buf)?;
            if leader_epoch == 0 {
                return Err(ScannerError::Other("persisted scanner cycle state has no leader epoch".to_string()));
            }
            Ok(leader_epoch)
        }
        Err(err) => Err(ScannerError::Other(format!("failed to read persisted scanner leader epoch: {err}"))),
    }
}

pub(super) fn decode_scanner_cycle_state_for_startup(buf: &[u8]) -> Result<(CurrentCycle, u64), ScannerCycleStateError> {
    if buf.is_empty() {
        Ok((CurrentCycle::default(), 0))
    } else {
        decode_scanner_cycle_state(buf)
    }
}

pub(super) fn advance_scanner_cycle(cycle_info: &mut CurrentCycle) -> Result<(), ScannerCycleStateError> {
    let next = cycle_info
        .next
        .checked_add(1)
        .filter(|next| *next < u64::MAX)
        .ok_or(ScannerCycleStateError::InvalidData("scanner cycle counter is exhausted"))?;
    cycle_info.next = next;
    Ok(())
}

pub(super) async fn persisted_usage_floor(
    storeapi: Arc<impl ScannerObjectIO + ScannerConfigObjectDelete>,
) -> Result<PersistedUsageFloor, ScannerError> {
    let (floor, state) = persisted_usage_floor_for_startup(storeapi, false).await?;
    if state != PersistedUsageFloorStartup::Authoritative {
        return Err(ScannerError::Other(
            "persisted scanner usage floor has no authoritative baseline".to_string(),
        ));
    }
    Ok(floor)
}

fn usage_epoch(usage: &DataUsageInfo) -> u64 {
    usage.scanner_epoch.unwrap_or_default()
}

fn usage_is_older_than_bootstrap(usage: &DataUsageInfo, bootstrap_epoch: Option<u64>) -> bool {
    bootstrap_epoch.is_some_and(|epoch| usage_epoch(usage) < epoch)
}

fn decode_usage_floor_slot(data: &[u8], path: &str) -> Result<DataUsageInfo, ScannerError> {
    serde_json::from_slice::<DataUsageInfo>(data)
        .map_err(|err| ScannerError::Other(format!("failed to decode scanner usage floor from {path}: {err}")))
}

fn update_persisted_usage_floor(floor: &mut PersistedUsageFloor, usage: &DataUsageInfo, path: &str) -> Result<(), ScannerError> {
    floor.leader_epoch = floor.leader_epoch.max(usage_epoch(usage));
    if let Some(completed_cycle) = usage.scanner_cycle {
        let next_cycle = completed_cycle
            .checked_add(1)
            .filter(|next| *next < u64::MAX)
            .ok_or_else(|| ScannerError::Other(format!("persisted scanner usage cycle is exhausted in {path}")))?;
        floor.next_cycle = floor.next_cycle.max(next_cycle);
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BootstrapBackupAction {
    Resume,
    StopV2Pair,
}

struct BootstrapBackupSlot<'a> {
    data: &'a [u8],
    usage: &'a DataUsageInfo,
    backup_path: &'a str,
    primary_path: &'a str,
    bootstrap_epoch: Option<u64>,
    recovered_bootstrap: bool,
    recovered_primary_companion_epoch: Option<u64>,
}

struct BootstrapBackupResolution<'a> {
    floor: &'a mut PersistedUsageFloor,
    unrecoverable_baseline_path: &'a mut Option<String>,
}

fn resolve_bootstrap_backup_slot(
    slot: BootstrapBackupSlot<'_>,
    resolution: BootstrapBackupResolution<'_>,
) -> Result<BootstrapBackupAction, ScannerError> {
    if usage_is_older_than_bootstrap(slot.usage, slot.bootstrap_epoch) {
        return Ok(BootstrapBackupAction::Resume);
    }
    if let Some(primary_epoch) = slot.recovered_primary_companion_epoch {
        if data_usage_info_has_persisted_baseline_identity(slot.usage) {
            let backup_epoch = usage_epoch(slot.usage);
            if backup_epoch >= primary_epoch {
                update_persisted_usage_floor(resolution.floor, slot.usage, slot.backup_path)?;
            }
        } else if let Some(fence) = legacy_incomplete_usage_fence(slot.data, slot.usage) {
            if let Some(epoch) = fence.claimable_epoch() {
                resolution.floor.leader_epoch = resolution.floor.leader_epoch.max(epoch);
            }
        } else {
            resolution
                .unrecoverable_baseline_path
                .get_or_insert_with(|| slot.backup_path.to_string());
        }
        return Ok(BootstrapBackupAction::Resume);
    }
    let compatible_incomplete_fence = legacy_incomplete_usage_fence(slot.data, slot.usage).is_some_and(|fence| {
        fence
            .claimable_epoch()
            .is_none_or(|epoch| slot.bootstrap_epoch.is_some_and(|bootstrap_epoch| epoch <= bootstrap_epoch))
    });
    if compatible_incomplete_fence {
        return Ok(BootstrapBackupAction::Resume);
    }
    if slot.recovered_bootstrap && data_usage_info_has_persisted_baseline_identity(slot.usage) {
        let epoch = usage_epoch(slot.usage);
        if epoch >= resolution.floor.leader_epoch {
            update_persisted_usage_floor(resolution.floor, slot.usage, slot.backup_path)?;
            if slot.primary_path == DATA_USAGE_OBJ_NAME_PATH.as_str() {
                return Ok(BootstrapBackupAction::StopV2Pair);
            }
            return Ok(BootstrapBackupAction::Resume);
        }
    }
    Err(ScannerError::Other(
        "scanner usage bootstrap conflicts with a persisted backup".to_string(),
    ))
}

pub(super) async fn persisted_usage_floor_for_startup(
    storeapi: Arc<impl ScannerObjectIO + ScannerConfigObjectDelete>,
    allow_missing_for_bootstrap: bool,
) -> Result<(PersistedUsageFloor, PersistedUsageFloorStartup), ScannerError> {
    let Some(read_epoch) = scanner_publication_epoch(storeapi.clone()).await else {
        return Err(ScannerError::Other("scanner usage floor read is blocked by data movement".to_string()));
    };
    let recovery_marker = read_legacy_incomplete_usage_floor_recovery_marker(storeapi.clone()).await?;
    let mut floor = PersistedUsageFloor::default();
    let mut found_any = false;
    let mut bootstrap_pending = false;
    let mut recovered_bootstrap = false;
    let mut bootstrap_epoch = None;
    // A valid JSON object without a baseline identity is not a floor and must
    // never be treated as an empty one.  It can, however, be a partially
    // written v2 primary left behind during an upgrade.  Keep its epoch as a
    // fence while looking for a durable companion snapshot; if no companion
    // is new enough, the caller still fails closed below.
    let mut invalid_baseline_path: Option<String> = None;
    let mut invalid_baseline_epoch = recovery_marker.as_ref().map(|(marker, _)| marker.leader_epoch);
    let mut unrecoverable_baseline_path: Option<String> = None;
    let mut stale_authoritative_path: Option<String> = None;
    let mut legacy_incomplete_primary: Option<LegacyIncompleteUsageFloorPrimary> = None;
    for primary_path in [DATA_USAGE_OBJ_NAME_PATH.as_str(), LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str()] {
        let backup_path = format!("{primary_path}.bkp");
        let is_v2_path = primary_path == DATA_USAGE_OBJ_NAME_PATH.as_str();
        let mut recovered_primary_companion_epoch = None;
        let mut primary_read_error = None;
        let primary_epoch = match read_config_with_revision(storeapi.clone(), primary_path).await {
            Ok((Some(data), revision)) => {
                let usage = match decode_usage_floor_slot(&data, primary_path) {
                    Ok(usage) => Some(usage),
                    Err(err) if bootstrap_pending && !is_v2_path => {
                        warn!(
                            target: "rustfs::scanner",
                            event = EVENT_SCANNER_PERSIST_STATE,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_RUNTIME,
                            state = "usage_reset_cleanup_deferred",
                            path = %primary_path,
                            error = %err,
                            "Scanner usage reset ignored a stale legacy usage primary after bootstrap marker"
                        );
                        None
                    }
                    Err(err) => {
                        return Err(err);
                    }
                };
                if let Some(usage) = usage {
                    if data_usage_info_is_bootstrap_pending(&usage) && primary_path == DATA_USAGE_OBJ_NAME_PATH.as_str() {
                        if bootstrap_pending {
                            return Err(ScannerError::Other("multiple scanner usage bootstrap markers were found".to_string()));
                        }
                        bootstrap_pending = true;
                        bootstrap_epoch = usage.scanner_epoch;
                        if let Some((marker, _)) = recovery_marker.as_ref() {
                            if usage.scanner_epoch.is_none_or(|epoch| epoch < marker.leader_epoch) {
                                return Err(ScannerError::Other(
                                    "scanner usage bootstrap is older than its recovery marker".to_string(),
                                ));
                            }
                            recovered_bootstrap = true;
                        }
                        update_persisted_usage_floor(&mut floor, &usage, primary_path)?;
                        None
                    } else if bootstrap_pending && !is_v2_path && usage_is_older_than_bootstrap(&usage, bootstrap_epoch) {
                        None
                    } else if !data_usage_info_has_persisted_baseline_identity(&usage) {
                        invalid_baseline_path.get_or_insert_with(|| primary_path.to_string());
                        invalid_baseline_epoch = invalid_baseline_epoch.max(usage.scanner_epoch);
                        if let Some(fence) = legacy_incomplete_usage_fence(&data, &usage) {
                            if is_v2_path && let Some(epoch) = fence.claimable_epoch() {
                                legacy_incomplete_primary = Some(LegacyIncompleteUsageFloorPrimary { revision, epoch });
                            }
                        } else {
                            unrecoverable_baseline_path.get_or_insert_with(|| primary_path.to_string());
                        }
                        None
                    } else {
                        let epoch = usage_epoch(&usage);
                        if recovered_bootstrap && !is_v2_path {
                            if epoch < floor.leader_epoch {
                                unrecoverable_baseline_path.get_or_insert_with(|| primary_path.to_string());
                            } else {
                                update_persisted_usage_floor(&mut floor, &usage, primary_path)?;
                                recovered_primary_companion_epoch = Some(epoch);
                            }
                            None
                        // A legacy snapshot may be structurally valid but older
                        // than an incomplete v2 snapshot left by a newer leader.
                        // Do not let that candidate regress the startup floor.
                        } else if invalid_baseline_epoch.is_some_and(|fenced_epoch| epoch < fenced_epoch)
                            && (!is_v2_path || recovery_marker.is_some())
                        {
                            stale_authoritative_path.get_or_insert_with(|| primary_path.to_string());
                            None
                        } else {
                            update_persisted_usage_floor(&mut floor, &usage, primary_path)?;
                            Some(epoch)
                        }
                    }
                } else {
                    None
                }
            }
            Ok((None, _)) => None,
            Err(err) if !is_v2_path && usage_floor_primary_read_error_allows_backup(&err) => {
                primary_read_error = Some(format!("failed to read scanner usage epoch floor from {primary_path}: {err}"));
                invalid_baseline_path.get_or_insert_with(|| primary_path.to_string());
                unrecoverable_baseline_path.get_or_insert_with(|| primary_path.to_string());
                None
            }
            Err(err) => {
                return Err(ScannerError::Other(format!(
                    "failed to read scanner usage epoch floor from {primary_path}: {err}"
                )));
            }
        };
        let mut any_found = primary_epoch.is_some();
        match read_config_with_revision(storeapi.clone(), &backup_path).await {
            Ok((Some(data), _)) => {
                let usage = match decode_usage_floor_slot(&data, &backup_path) {
                    Ok(usage) => usage,
                    Err(err) if bootstrap_pending => {
                        warn!(
                            target: "rustfs::scanner",
                            event = EVENT_SCANNER_PERSIST_STATE,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_RUNTIME,
                            state = "usage_reset_cleanup_deferred",
                            path = %backup_path,
                            error = %err,
                            "Scanner usage reset ignored a stale usage backup after bootstrap marker"
                        );
                        continue;
                    }
                    Err(err) => {
                        return Err(err);
                    }
                };
                if bootstrap_pending {
                    match resolve_bootstrap_backup_slot(
                        BootstrapBackupSlot {
                            data: &data,
                            usage: &usage,
                            backup_path: &backup_path,
                            primary_path,
                            bootstrap_epoch,
                            recovered_bootstrap,
                            recovered_primary_companion_epoch,
                        },
                        BootstrapBackupResolution {
                            floor: &mut floor,
                            unrecoverable_baseline_path: &mut unrecoverable_baseline_path,
                        },
                    )? {
                        BootstrapBackupAction::Resume => continue,
                        BootstrapBackupAction::StopV2Pair => break,
                    }
                }
                if !data_usage_info_has_persisted_baseline_identity(&usage) {
                    invalid_baseline_path.get_or_insert_with(|| backup_path.clone());
                    invalid_baseline_epoch = invalid_baseline_epoch.max(usage.scanner_epoch);
                    if legacy_incomplete_usage_fence(&data, &usage).is_none() {
                        unrecoverable_baseline_path.get_or_insert_with(|| backup_path.clone());
                    }
                    // This is still persisted state, so it must not enable a
                    // missing-state bootstrap.  Continue to a legacy pair in
                    // case it contains a complete, fenced snapshot.
                } else {
                    let backup_epoch = usage_epoch(&usage);
                    // A backup write from an older leader may complete after the
                    // primary epoch has been fenced. It must not advance the startup
                    // floor unless its epoch is at least as new as the primary.
                    if primary_epoch.is_none_or(|epoch| backup_epoch >= epoch)
                        && invalid_baseline_epoch.is_none_or(|epoch| backup_epoch >= epoch)
                    {
                        update_persisted_usage_floor(&mut floor, &usage, &backup_path)?;
                        any_found = true;
                    } else {
                        stale_authoritative_path.get_or_insert_with(|| backup_path.clone());
                    }
                }
            }
            Ok((None, _)) => {}
            Err(err) => {
                return Err(ScannerError::Other(format!(
                    "failed to read scanner usage epoch floor from {backup_path}: {err}"
                )));
            }
        }
        if let Some(primary_read_error) = primary_read_error
            && !any_found
        {
            return Err(ScannerError::Other(format!(
                "{}; no valid scanner usage floor backup was available at {backup_path}",
                primary_read_error
            )));
        }
        if any_found {
            if bootstrap_pending {
                return Err(ScannerError::Other(
                    "scanner usage bootstrap conflicts with an authoritative usage floor".to_string(),
                ));
            }
            found_any = true;
            break;
        }
    }

    if !found_any && !bootstrap_pending {
        if allow_missing_for_bootstrap
            && unrecoverable_baseline_path.is_none()
            && stale_authoritative_path.is_none()
            && let Some(mut primary) = legacy_incomplete_primary
        {
            primary.epoch = primary.epoch.max(invalid_baseline_epoch.unwrap_or_default());
            let leader_epoch = primary.epoch;
            recover_legacy_incomplete_usage_floor(storeapi.clone(), primary, read_epoch).await?;
            record_legacy_incomplete_usage_floor_recovery_pending(leader_epoch);
            return Ok((
                PersistedUsageFloor {
                    next_cycle: 0,
                    leader_epoch,
                },
                PersistedUsageFloorStartup::RecoveredLegacyIncompleteFence,
            ));
        }
        if let Some(path) = stale_authoritative_path {
            return Err(ScannerError::Other(format!(
                "persisted scanner usage floor from {path} is older than the required recovery fence"
            )));
        }
        if recovery_marker.is_some() {
            return Err(ScannerError::Other("scanner usage recovery marker has no matching primary".to_string()));
        }
        if let Some(path) = invalid_baseline_path {
            return Err(ScannerError::Other(format!(
                "persisted scanner usage floor from {path} has no authoritative baseline or newer valid backup; recover with POST /rustfs/admin/v3/scanner/usage-state/reset using mode full-rebuild"
            )));
        }
        if !allow_missing_for_bootstrap {
            return Err(ScannerError::Other(
                "persisted scanner usage floor has no authoritative baseline".to_string(),
            ));
        }
        let Some(publication_admission) = scanner_publication_admission_for_epoch(storeapi.clone(), read_epoch).await else {
            return Err(ScannerError::Other(
                "scanner usage floor changed before missing-state confirmation".to_string(),
            ));
        };
        for path in [
            DATA_USAGE_OBJ_NAME_PATH.as_str().to_string(),
            format!("{}.bkp", DATA_USAGE_OBJ_NAME_PATH.as_str()),
            LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str().to_string(),
            format!("{}.bkp", LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str()),
        ] {
            match read_config_with_revision(storeapi.clone(), &path).await {
                Ok((None, _)) => {}
                Ok((Some(_), _)) => {
                    return Err(ScannerError::Other(format!(
                        "scanner usage floor changed while confirming missing state: {path} appeared"
                    )));
                }
                Err(err) => {
                    return Err(ScannerError::Other(format!(
                        "failed to confirm missing scanner usage floor at {path}: {err}"
                    )));
                }
            }
        }
        drop(publication_admission);
    }
    let Some(publication_admission) = scanner_publication_admission_for_epoch(storeapi.clone(), read_epoch).await else {
        return Err(ScannerError::Other(
            "scanner usage floor changed while its epoch proof was being confirmed".to_string(),
        ));
    };
    let state = if found_any {
        PersistedUsageFloorStartup::Authoritative
    } else if recovered_bootstrap {
        if let Some(path) = unrecoverable_baseline_path {
            return Err(ScannerError::Other(format!(
                "scanner usage recovery conflicts with persisted usage state at {path}"
            )));
        }
        let recovery_epoch = recovery_marker
            .as_ref()
            .map(|(marker, _)| marker.leader_epoch)
            .unwrap_or(floor.leader_epoch);
        record_legacy_incomplete_usage_floor_recovery_pending(recovery_epoch);
        PersistedUsageFloorStartup::RecoveredLegacyIncompleteFence
    } else if bootstrap_pending {
        if let Some(path) = unrecoverable_baseline_path {
            return Err(ScannerError::Other(format!(
                "scanner usage bootstrap conflicts with persisted usage state at {path}"
            )));
        }
        PersistedUsageFloorStartup::BootstrapPending
    } else {
        PersistedUsageFloorStartup::Missing
    };
    if found_any && let Some((_, marker_revision)) = recovery_marker.as_ref() {
        drop(publication_admission);
        let marker_cleared =
            match clear_legacy_incomplete_usage_floor_recovery_marker(storeapi.clone(), marker_revision, read_epoch).await {
                Ok(()) => true,
                Err(err) => {
                    warn!(
                        target: "rustfs::scanner",
                        event = EVENT_SCANNER_PERSIST_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_RUNTIME,
                        state = "usage_floor_recovery_cleanup_deferred",
                        path = %DATA_USAGE_RECOVERY_PATH.as_str(),
                        error = %err,
                        "Scanner usage floor recovery marker cleanup was deferred"
                    );
                    false
                }
            };
        let Some(_final_publication_admission) = scanner_publication_admission_for_epoch(storeapi.clone(), read_epoch).await
        else {
            return Err(ScannerError::Other(
                "scanner usage floor changed after recovery marker cleanup".to_string(),
            ));
        };
        if marker_cleared {
            clear_legacy_incomplete_usage_floor_recovery_status();
        }
        clear_scanner_usage_floor_failure();
        return Ok((floor, state));
    }
    clear_scanner_usage_floor_failure();
    Ok((floor, state))
}

pub(super) fn apply_persisted_usage_floor(cycle_info: &mut CurrentCycle, leader_epoch: &mut u64, floor: PersistedUsageFloor) {
    cycle_info.next = cycle_info.next.max(floor.next_cycle);
    *leader_epoch = (*leader_epoch).max(floor.leader_epoch);
}

#[derive(Clone, Copy)]
pub(super) struct ScannerCycleFloorOptions {
    pub(super) required_cycle: u64,
    pub(super) expected_publication_epoch: Option<u64>,
}

#[cfg(test)]
pub(super) async fn persist_scanner_cycle_state(
    ctx: &CancellationToken,
    storeapi: Arc<impl ScannerObjectIO + ScannerConfigObjectDelete>,
    cycle_info: &mut CurrentCycle,
    revision: &mut DataUsageCacheRevision,
    leader_epoch: u64,
) -> bool {
    persist_scanner_cycle_state_for_epoch(ctx, storeapi, cycle_info, revision, leader_epoch, None).await
}

pub(super) async fn persist_scanner_cycle_state_for_epoch(
    ctx: &CancellationToken,
    storeapi: Arc<impl ScannerObjectIO + ScannerConfigObjectDelete>,
    cycle_info: &mut CurrentCycle,
    revision: &mut DataUsageCacheRevision,
    leader_epoch: u64,
    expected_publication_epoch: Option<u64>,
) -> bool {
    let buf = match encode_scanner_cycle_state(cycle_info, leader_epoch) {
        Ok(buf) => buf,
        Err(e) => {
            error!(
                target: "rustfs::scanner",
                event = EVENT_SCANNER_PERSIST_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                state = "encode_failed",
                error = %e,
                "Scanner state encoding failed"
            );
            return false;
        }
    };

    for retry in 0..=SCANNER_PERSIST_CAS_RETRIES {
        if ctx.is_cancelled() {
            debug!(
                target: "rustfs::scanner",
                event = EVENT_SCANNER_PERSIST_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                state = "cancelled_before_save",
                retry,
                "Scanner state persistence cancelled by the leader fence"
            );
            return false;
        }

        #[cfg(test)]
        notify_scanner_cycle_state_persist_test_hook(leader_epoch);
        let Some(read_epoch) = scanner_publication_epoch(storeapi.clone()).await else {
            return false;
        };
        if expected_publication_epoch.is_some_and(|expected| expected != read_epoch) {
            return false;
        }
        let save_result = {
            let Some(_publication_admission) = scanner_publication_admission_for_epoch(storeapi.clone(), read_epoch).await else {
                error!(
                    target: "rustfs::scanner",
                    event = EVENT_SCANNER_PERSIST_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                    state = "publication_admission_unavailable",
                    "Scanner state persistence skipped without movement admission"
                );
                return false;
            };
            save_config_with_preconditions(storeapi.clone(), &DATA_USAGE_BLOOM_NAME_PATH, buf.clone(), revision.preconditions())
                .await
        };
        match save_result {
            Ok(object_info) => {
                let Some(etag) = object_info.etag.filter(|etag| !etag.is_empty()) else {
                    error!(
                        target: "rustfs::scanner",
                        event = EVENT_SCANNER_PERSIST_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_RUNTIME,
                        path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                        state = "missing_revision",
                        "Scanner state save returned no ETag"
                    );
                    return false;
                };
                *revision = DataUsageCacheRevision::Etag(etag);
                if ctx.is_cancelled() {
                    debug!(
                        target: "rustfs::scanner",
                        event = EVENT_SCANNER_PERSIST_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_RUNTIME,
                        path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                        state = "cancelled_after_save",
                        retry,
                        "Scanner state save completed after the leader fence was cancelled"
                    );
                    return false;
                }
                if let Some(expected_epoch) = expected_publication_epoch
                    && scanner_publication_admission_for_epoch(storeapi.clone(), expected_epoch)
                        .await
                        .is_none()
                {
                    return false;
                }
                debug!(
                    target: "rustfs::scanner",
                    event = EVENT_SCANNER_PERSIST_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                    state = "saved",
                    "Scanner state saved"
                );
                return true;
            }
            Err(EcstoreError::PreconditionFailed) => {
                let (persisted, persisted_revision) =
                    match read_config_with_revision(storeapi.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str()).await {
                        Ok(result) => result,
                        Err(e) => {
                            error!(
                                target: "rustfs::scanner",
                                event = EVENT_SCANNER_PERSIST_STATE,
                                component = LOG_COMPONENT_SCANNER,
                                subsystem = LOG_SUBSYSTEM_RUNTIME,
                                path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                                state = "conflict_reload_failed",
                                error = %e,
                                "Scanner state conflict reconciliation failed"
                            );
                            return false;
                        }
                    };
                *revision = persisted_revision;
                if ctx.is_cancelled() {
                    debug!(
                        target: "rustfs::scanner",
                        event = EVENT_SCANNER_PERSIST_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_RUNTIME,
                        path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                        state = "cancelled_after_conflict",
                        retry,
                        "Scanner state conflict reconciliation cancelled by the leader fence"
                    );
                    return false;
                }

                if let Some(persisted) = persisted {
                    if persisted.len() < 8 {
                        error!(
                            target: "rustfs::scanner",
                            event = EVENT_SCANNER_PERSIST_STATE,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_RUNTIME,
                            path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                            state = "conflict_state_invalid",
                            length = persisted.len(),
                            "Scanner state conflict winner is truncated"
                        );
                        return false;
                    }

                    let (persisted_cycle, persisted_epoch) = match decode_scanner_cycle_state(&persisted) {
                        Ok(state) => state,
                        Err(e) => {
                            error!(
                                target: "rustfs::scanner",
                                event = EVENT_SCANNER_PERSIST_STATE,
                                component = LOG_COMPONENT_SCANNER,
                                subsystem = LOG_SUBSYSTEM_RUNTIME,
                                path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                                state = "conflict_state_decode_failed",
                                error = %e,
                                "Scanner state conflict winner could not be decoded"
                            );
                            return false;
                        }
                    };
                    if persisted_epoch != leader_epoch {
                        error!(
                            target: "rustfs::scanner",
                            event = EVENT_SCANNER_PERSIST_STATE,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_RUNTIME,
                            path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                            state = "leader_epoch_fenced",
                            expected_epoch = leader_epoch,
                            persisted_epoch,
                            "Scanner state save rejected by a newer leadership epoch"
                        );
                        return false;
                    }

                    if persisted_cycle.next >= cycle_info.next {
                        if let Some(expected_epoch) = expected_publication_epoch
                            && scanner_publication_admission_for_epoch(storeapi.clone(), expected_epoch)
                                .await
                                .is_none()
                        {
                            return false;
                        }
                        *cycle_info = persisted_cycle;
                        debug!(
                            target: "rustfs::scanner",
                            event = EVENT_SCANNER_PERSIST_STATE,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_RUNTIME,
                            path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                            state = "conflict_reconciled",
                            retry,
                            "Scanner state adopted the current persisted cycle"
                        );
                        return true;
                    }
                }

                if retry < SCANNER_PERSIST_CAS_RETRIES {
                    debug!(
                        target: "rustfs::scanner",
                        event = EVENT_SCANNER_PERSIST_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_RUNTIME,
                        path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                        state = "conflict_retry",
                        retry = retry + 1,
                        "Scanner state CAS conflict will be retried"
                    );
                    continue;
                }

                error!(
                    target: "rustfs::scanner",
                    event = EVENT_SCANNER_PERSIST_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                    state = "conflict_retries_exhausted",
                    retries = SCANNER_PERSIST_CAS_RETRIES,
                    "Scanner state CAS conflict retries exhausted"
                );
                return false;
            }
            Err(e) => {
                error!(
                    target: "rustfs::scanner",
                    event = EVENT_SCANNER_PERSIST_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                    state = "failed",
                    error = %e,
                    "Scanner state persistence failed"
                );
                return false;
            }
        }
    }

    false
}

#[cfg(test)]
pub(super) async fn finalize_partial_scan_cycle(
    ctx: &CancellationToken,
    storeapi: Arc<impl ScannerObjectIO + ScannerConfigObjectDelete>,
    cycle_info: &mut CurrentCycle,
    revision: &mut DataUsageCacheRevision,
    leader_epoch: u64,
    cycle_metrics_guard: &mut ScannerCycleMetricsGuard,
) -> bool {
    finalize_partial_scan_cycle_for_epoch(ctx, storeapi, cycle_info, revision, leader_epoch, cycle_metrics_guard, None).await
}

pub(super) async fn finalize_partial_scan_cycle_for_epoch(
    ctx: &CancellationToken,
    storeapi: Arc<impl ScannerObjectIO + ScannerConfigObjectDelete>,
    cycle_info: &mut CurrentCycle,
    revision: &mut DataUsageCacheRevision,
    leader_epoch: u64,
    cycle_metrics_guard: &mut ScannerCycleMetricsGuard,
    expected_publication_epoch: Option<u64>,
) -> bool {
    // A budget-limited cycle is deliberate pacing, not a failure. The cycle counter
    // must still advance (and persist) because per-bucket next_cycle is stamped from
    // it and compacted folders are only rescanned when their hash matches
    // next_cycle % DATA_USAGE_UPDATE_DIR_CYCLES; a pinned counter starves lifecycle
    // expiry and usage refresh on every folder outside the stuck window.
    let previous_cycle_info = cycle_info.clone();
    if let Err(err) = advance_scanner_cycle(cycle_info) {
        error!(
            target: "rustfs::scanner",
            event = EVENT_SCANNER_PERSIST_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_RUNTIME,
            state = "cycle_counter_exhausted",
            error = %err,
            "Scanner partial cycle could not advance"
        );
        mark_scan_cycle_idle(cycle_info, cycle_metrics_guard).await;
        return false;
    }
    cycle_info.current = 0;
    global_metrics().clear_current_scan_mode();
    let persisted = persist_scanner_cycle_state_for_epoch(
        ctx,
        storeapi.clone(),
        cycle_info,
        revision,
        leader_epoch,
        expected_publication_epoch,
    )
    .await;
    if !persisted
        && let Some(expected_epoch) = expected_publication_epoch
        && scanner_publication_admission_for_epoch(storeapi, expected_epoch)
            .await
            .is_none()
    {
        *cycle_info = previous_cycle_info;
    }
    cycle_metrics_guard.finish(cycle_info.clone()).await;
    persisted
}

#[cfg(test)]
pub(super) async fn persist_required_scanner_cycle_floor(
    ctx: &CancellationToken,
    storeapi: Arc<impl ScannerObjectIO + ScannerConfigObjectDelete>,
    cycle_info: &mut CurrentCycle,
    revision: &mut DataUsageCacheRevision,
    leader_epoch: u64,
    required_cycle: u64,
    cycle_metrics_guard: &mut ScannerCycleMetricsGuard,
) -> bool {
    persist_required_scanner_cycle_floor_for_epoch(
        ctx,
        storeapi,
        cycle_info,
        revision,
        leader_epoch,
        cycle_metrics_guard,
        ScannerCycleFloorOptions {
            required_cycle,
            expected_publication_epoch: None,
        },
    )
    .await
}

pub(super) async fn persist_required_scanner_cycle_floor_for_epoch(
    ctx: &CancellationToken,
    storeapi: Arc<impl ScannerObjectIO + ScannerConfigObjectDelete>,
    cycle_info: &mut CurrentCycle,
    revision: &mut DataUsageCacheRevision,
    leader_epoch: u64,
    cycle_metrics_guard: &mut ScannerCycleMetricsGuard,
    options: ScannerCycleFloorOptions,
) -> bool {
    if options.required_cycle <= cycle_info.current || options.required_cycle == u64::MAX {
        error!(
            target: "rustfs::scanner",
            event = EVENT_SCANNER_PERSIST_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_RUNTIME,
            current_cycle = cycle_info.current,
            required_cycle = options.required_cycle,
            state = "invalid_cache_cycle_floor",
            "Scanner cache cycle floor is invalid"
        );
        mark_scan_cycle_idle(cycle_info, cycle_metrics_guard).await;
        return false;
    }

    let previous_cycle_info = cycle_info.clone();
    cycle_info.next = cycle_info.next.max(options.required_cycle);
    cycle_info.current = 0;
    global_metrics().clear_current_scan_mode();
    let persisted = persist_scanner_cycle_state_for_epoch(
        ctx,
        storeapi.clone(),
        cycle_info,
        revision,
        leader_epoch,
        options.expected_publication_epoch,
    )
    .await;
    if !persisted
        && let Some(expected_epoch) = options.expected_publication_epoch
        && scanner_publication_admission_for_epoch(storeapi, expected_epoch)
            .await
            .is_none()
    {
        *cycle_info = previous_cycle_info;
    }
    cycle_metrics_guard.finish(cycle_info.clone()).await;
    persisted
}

pub(super) async fn await_scanner_cycle_with_lock_fence<Cycle, LockLost>(
    cycle_ctx: &CancellationToken,
    cycle: Cycle,
    lock_lost: LockLost,
) -> Option<Cycle::Output>
where
    Cycle: Future,
    LockLost: Future<Output = ()>,
{
    tokio::pin!(cycle);
    tokio::pin!(lock_lost);
    tokio::select! {
        biased;
        _ = &mut lock_lost => {
            cycle_ctx.cancel();
            tokio::time::timeout(SCANNER_LOCK_LOSS_SHUTDOWN_TIMEOUT, &mut cycle).await.ok()
        }
        output = &mut cycle => Some(output),
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum ScannerCycleWaitOutcome<T> {
    Completed(T),
    LockLost,
    Cancelled,
    Deadline { worker_stopped: bool },
}

pub(super) async fn await_scanner_cycle_with_budget_fence<Cycle, LockLost>(
    cycle_ctx: &CancellationToken,
    budget: &ScannerCycleBudget,
    cycle: Cycle,
    lock_lost: LockLost,
) -> ScannerCycleWaitOutcome<Cycle::Output>
where
    Cycle: Future,
    LockLost: Future<Output = ()>,
{
    tokio::pin!(cycle);
    tokio::pin!(lock_lost);
    let deadline = async {
        if let Some(deadline) = budget.deadline() {
            tokio::time::sleep_until(deadline).await;
        } else {
            std::future::pending::<()>().await;
        }
    };
    tokio::pin!(deadline);
    tokio::select! {
        biased;
        _ = &mut lock_lost => {
            cycle_ctx.cancel();
            let _ = tokio::time::timeout(SCANNER_LOCK_LOSS_SHUTDOWN_TIMEOUT, &mut cycle).await;
            ScannerCycleWaitOutcome::LockLost
        }
        _ = &mut deadline => {
            budget.cancel_for_runtime();
            // Let the budget cancellation reach the scanner first so it can
            // persist a partial cursor. Only an uncooperative worker gets the
            // parent cancellation, and it is dropped after the bounded window;
            // the caller fences its epoch next.
            let worker_stopped = if tokio::time::timeout(SCANNER_LOCK_LOSS_SHUTDOWN_TIMEOUT, &mut cycle)
                .await
                .is_ok()
            {
                true
            } else {
                cycle_ctx.cancel();
                false
            };
            ScannerCycleWaitOutcome::Deadline { worker_stopped }
        }
        _ = cycle_ctx.cancelled() => {
            let _ = tokio::time::timeout(SCANNER_LOCK_LOSS_SHUTDOWN_TIMEOUT, &mut cycle).await;
            ScannerCycleWaitOutcome::Cancelled
        }
        output = &mut cycle => ScannerCycleWaitOutcome::Completed(output),
    }
}
