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
use crate::data_usage_define::DATA_USAGE_BLOOM_RECOVERY_PATH;
use crate::storage_api::owner::ObjectIO as _;
use tokio::io::AsyncReadExt as _;

const SCANNER_CYCLE_RECOVERY_SCHEMA_VERSION: u16 = 1;
const MAX_SCANNER_CYCLE_STATE_BYTES: u64 = 1024 * 1024;
pub(super) const MAX_SCANNER_CYCLE_RECOVERY_RETRIES: u32 = 5;
const METRIC_SCANNER_CYCLE_RECOVERY_REQUIRED: &str = "rustfs_scanner_cycle_recovery_required";
const METRIC_SCANNER_CYCLE_RECOVERY_RETRY_COUNT: &str = "rustfs_scanner_cycle_recovery_retry_count";

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
    pub max_retries: u32,
    /// Whether the scanner may retry this state automatically.
    pub retryable: bool,
    pub reason: Option<String>,
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

pub fn scanner_cycle_recovery_status() -> ScannerCycleRecoveryStatus {
    SCANNER_CYCLE_RECOVERY_STATUS
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}

fn set_scanner_cycle_recovery_status(status: ScannerCycleRecoveryStatus) {
    let recovery_required = if matches!(status.state.as_str(), "blocked" | "paused" | "recovery-required" | "cleanup-pending") {
        1.0
    } else {
        0.0
    };
    metrics::gauge!(METRIC_SCANNER_CYCLE_RECOVERY_REQUIRED).set(recovery_required);
    metrics::gauge!(METRIC_SCANNER_CYCLE_RECOVERY_RETRY_COUNT).set(status.retry_count as f64);
    *SCANNER_CYCLE_RECOVERY_STATUS
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = status;
}

pub(super) fn record_scanner_cycle_recovery_retry(attempt: u32) -> bool {
    let mut status = scanner_cycle_recovery_status();
    status.retry_count = u64::from(attempt);
    status.last_attempt_at_unix_secs = Some(unix_now_secs());
    if attempt >= MAX_SCANNER_CYCLE_RECOVERY_RETRIES {
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
        _ => "blocked",
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
) -> Result<(ScannerCycleRecoveryMarker, DataUsageCacheRevision), ScannerError> {
    marker.state = "cleanup-pending".to_string();
    marker.last_attempt_at_unix_secs = unix_now_secs();
    let bytes = serde_json::to_vec(&marker)
        .map_err(|err| ScannerError::Other(format!("failed to encode cycle recovery marker: {err}")))?;
    let info = save_config_with_publication_admission_for_epoch(
        storeapi.clone(),
        DATA_USAGE_BLOOM_RECOVERY_PATH.as_str(),
        bytes,
        marker_revision.preconditions(),
        expected_epoch,
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

/// Reset a blocked cycle state after an operator has explicitly requested a full
/// usage rebuild. The primary object is changed first with its observed ETag;
/// the recovery marker is removed only when its own ETag still matches.
pub async fn reset_scanner_cycle_recovery(ctx: CancellationToken, storeapi: Arc<ECStore>) -> Result<(), ScannerError> {
    let lock = storeapi
        .new_ns_lock(RUSTFS_META_BUCKET, "leader.lock")
        .await
        .map_err(|err| ScannerError::Other(format!("failed to acquire scanner leader lock: {err}")))?;
    let guard = lock
        .get_write_lock_quiet(Duration::from_secs(5))
        .await
        .map_err(|err| ScannerError::Other(format!("scanner leader lock is busy: {err}")))?;

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
    let marker_data = marker_data.ok_or_else(|| ScannerError::Other("scanner cycle recovery marker is absent".to_string()))?;
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
            let (cleanup_marker, cleanup_marker_revision) =
                mark_cycle_recovery_cleanup_pending(storeapi.clone(), marker.clone(), &marker_revision, reset_epoch).await?;
            set_scanner_cycle_recovery_status(recovery_status_from_marker(&cleanup_marker, "cleanup-pending"));
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
            let preserved_info = save_config_with_publication_admission_for_epoch(
                storeapi.clone(),
                DATA_USAGE_BLOOM_NAME_PATH.as_str(),
                preserved_data,
                primary_revision.preconditions(),
                reset_epoch,
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
            fence_scanner_usage_epoch_with_expected_epoch(&ctx, storeapi.clone(), fence_epoch, Some(reset_epoch))
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
            delete_config_with_publication_admission_for_epoch(
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
            )
            .await
            .map_err(|err| {
                if scanner_publication_epoch_changed(&err) {
                    ScannerError::Other("scanner recovery reset deferred by a movement epoch change".to_string())
                } else {
                    ScannerError::Other(format!("failed to clear stale cycle recovery marker: {err}"))
                }
            })?;
            set_scanner_cycle_recovery_status(recovery_status("healthy", None, false));
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
    let (marker, marker_revision) = if marker.state == "cleanup-pending" {
        (marker, marker_revision)
    } else {
        mark_cycle_recovery_cleanup_pending(storeapi.clone(), marker, &marker_revision, reset_epoch).await?
    };
    let rebuilt_info = save_config_with_publication_admission_for_epoch(
        storeapi.clone(),
        DATA_USAGE_BLOOM_NAME_PATH.as_str(),
        data,
        primary_revision.preconditions(),
        reset_epoch,
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
    if let Err(err) = fence_scanner_usage_epoch_with_expected_epoch(&ctx, storeapi.clone(), leader_epoch, Some(reset_epoch)).await
    {
        set_scanner_cycle_recovery_status(ScannerCycleRecoveryStatus {
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
        set_scanner_cycle_recovery_status(ScannerCycleRecoveryStatus {
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
        set_scanner_cycle_recovery_status(ScannerCycleRecoveryStatus {
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

    if let Err(err) = delete_config_with_publication_admission_for_epoch(
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
    )
    .await
    {
        if scanner_publication_epoch_changed(&err) {
            set_scanner_cycle_recovery_status(ScannerCycleRecoveryStatus {
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
        set_scanner_cycle_recovery_status(ScannerCycleRecoveryStatus {
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
    set_scanner_cycle_recovery_status(ScannerCycleRecoveryStatus {
        path: DATA_USAGE_BLOOM_NAME_PATH.clone(),
        quarantine_path: Some(DATA_USAGE_BLOOM_RECOVERY_PATH.clone()),
        state: "healthy".to_string(),
        max_retries: MAX_SCANNER_CYCLE_RECOVERY_RETRIES,
        ..Default::default()
    });
    super::notify_scanner_cycle_recovery_wake();
    Ok(())
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
    let Some(read_epoch) = scanner_publication_epoch(storeapi.clone()).await else {
        return Err(ScannerError::Other("scanner usage floor read is blocked by data movement".to_string()));
    };
    let mut floor = PersistedUsageFloor::default();
    let mut found_any = false;
    let update_floor = |floor: &mut PersistedUsageFloor, usage: &DataUsageInfo, path: &str| -> Result<(), ScannerError> {
        floor.leader_epoch = floor.leader_epoch.max(usage.scanner_epoch.unwrap_or_default());
        if let Some(completed_cycle) = usage.scanner_cycle {
            let next_cycle = completed_cycle
                .checked_add(1)
                .filter(|next| *next < u64::MAX)
                .ok_or_else(|| ScannerError::Other(format!("persisted scanner usage cycle is exhausted in {path}")))?;
            floor.next_cycle = floor.next_cycle.max(next_cycle);
        }
        Ok(())
    };
    for primary_path in [DATA_USAGE_OBJ_NAME_PATH.as_str(), LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str()] {
        let backup_path = format!("{primary_path}.bkp");
        let primary_epoch = match read_config(storeapi.clone(), primary_path).await {
            Ok(data) => {
                let usage = serde_json::from_slice::<DataUsageInfo>(&data).map_err(|err| {
                    ScannerError::Other(format!("failed to decode scanner usage floor from {primary_path}: {err}"))
                })?;
                if !data_usage_info_has_persisted_baseline_identity(&usage) {
                    return Err(ScannerError::Other(format!(
                        "scanner usage floor from {primary_path} has no persisted baseline identity"
                    )));
                }
                let epoch = usage.scanner_epoch.unwrap_or_default();
                update_floor(&mut floor, &usage, primary_path)?;
                Some(epoch)
            }
            Err(EcstoreError::ConfigNotFound) => None,
            Err(err) => {
                return Err(ScannerError::Other(format!(
                    "failed to read scanner usage epoch floor from {primary_path}: {err}"
                )));
            }
        };
        let mut any_found = primary_epoch.is_some();
        match read_config(storeapi.clone(), &backup_path).await {
            Ok(data) => {
                any_found = true;
                let usage = serde_json::from_slice::<DataUsageInfo>(&data).map_err(|err| {
                    ScannerError::Other(format!("failed to decode scanner usage floor from {backup_path}: {err}"))
                })?;
                if !data_usage_info_has_persisted_baseline_identity(&usage) {
                    return Err(ScannerError::Other(format!(
                        "scanner usage floor from {backup_path} has no persisted baseline identity"
                    )));
                }
                let backup_epoch = usage.scanner_epoch.unwrap_or_default();
                // A backup write from an older leader may complete after the
                // primary epoch has been fenced. It must not advance the startup
                // floor unless its epoch is at least as new as the primary.
                if primary_epoch.is_none_or(|epoch| backup_epoch >= epoch) {
                    update_floor(&mut floor, &usage, &backup_path)?;
                }
            }
            Err(EcstoreError::ConfigNotFound) => {}
            Err(err) => {
                return Err(ScannerError::Other(format!(
                    "failed to read scanner usage epoch floor from {backup_path}: {err}"
                )));
            }
        }
        if any_found {
            found_any = true;
            break;
        }
    }

    if !found_any {
        return Err(ScannerError::Other(
            "persisted scanner usage floor has no authoritative baseline".to_string(),
        ));
    }
    let Some(_publication_admission) = scanner_publication_admission_for_epoch(storeapi, read_epoch).await else {
        return Err(ScannerError::Other(
            "scanner usage floor changed while its epoch proof was being confirmed".to_string(),
        ));
    };
    Ok(floor)
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
