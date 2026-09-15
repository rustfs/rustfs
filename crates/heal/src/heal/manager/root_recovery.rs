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

//! Graceful-shutdown handoff for administrator heals. This namespace is
//! separate from erasure-set checkpoints and replacement generations, which
//! cannot represent an admitted admin control-plane request. One coordinator
//! disk owns each record; never create a fallback copy after an uncertain write
//! or deletion.

use super::*;
use crate::heal::resume::CheckpointManager;
use crate::heal::storage_api::owner::{EcstoreConditionalFileUpdate, EcstoreDiskAPI, EcstoreDiskBytes};
use crate::heal::{DiskStore, RUSTFS_META_BUCKET};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;
use uuid::Uuid;

mod report;
use report::{ROOT_REPORT_PREFIX, RootHealReport, read_report, remove_report, write_report};

// The metadata bucket already exists and its parent is durable. Creating a
// nested journal directory here would also require syncing every ancestor.
const ROOT_RECOVERY_PREFIX: &str = "root-heal-";
const ROOT_TERMINAL_PREFIX: &str = "terminal-root-heal-";
const LEGACY_ROOT_RECOVERY_SCHEMA: u32 = 1;
const SCOPED_ROOT_RECOVERY_SCHEMA: u32 = 2;
const ROOT_RECOVERY_SCHEMA: u32 = 3;
const ROOT_TERMINAL_SCHEMA: u32 = 1;
const ROOT_QUARANTINE_INTENT_PREFIX: &str = "quarantined-root-heal-intent-";
const ROOT_QUARANTINE_TERMINAL_PREFIX: &str = "quarantined-root-heal-terminal-";
const ROOT_QUARANTINE_SCHEMA: u32 = 1;
pub(super) const MAX_ROOT_RECOVERY_BYTES: usize = 64 * 1024;
const ROOT_TERMINAL_GC_SCAN_BUDGET: usize = 1024;
const ROOT_TERMINAL_GC_DELETE_BUDGET: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RecoveryHealType {
    Cluster,
    Bucket {
        bucket: String,
    },
    Object {
        bucket: String,
        object: String,
        version_id: Option<String>,
    },
    Prefix {
        bucket: String,
        prefix: String,
    },
    ErasureSet {
        buckets: Vec<String>,
        set_disk_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        all_buckets: Option<bool>,
    },
    Metadata {
        bucket: String,
        object: String,
    },
    EcDecode {
        bucket: String,
        object: String,
        version_id: Option<String>,
    },
}

impl RecoveryHealType {
    fn normalize_legacy_scope(&mut self) {
        if let Self::ErasureSet {
            buckets, all_buckets, ..
        } = self
            && buckets.is_empty()
            && all_buckets.is_none()
        {
            *all_buckets = Some(true);
        }
        if let Self::ErasureSet {
            buckets, all_buckets, ..
        } = self
            && !buckets.is_empty()
            && *all_buckets == Some(false)
        {
            *all_buckets = None;
        }
    }

    fn validate(&self) -> Result<()> {
        match self {
            Self::Cluster => {}
            Self::Bucket { bucket } => validate_recovery_component("bucket", bucket)?,
            Self::Object {
                bucket,
                object,
                version_id,
            }
            | Self::EcDecode {
                bucket,
                object,
                version_id,
            } => {
                validate_recovery_component("bucket", bucket)?;
                validate_recovery_component("object", object)?;
                if let Some(version_id) = version_id {
                    validate_recovery_component("version id", version_id)?;
                }
            }
            Self::Prefix { bucket, prefix } => {
                validate_recovery_component("bucket", bucket)?;
                validate_recovery_component("prefix", prefix)?;
            }
            Self::ErasureSet {
                buckets,
                set_disk_id,
                all_buckets,
            } => {
                crate::heal::utils::parse_set_disk_id(set_disk_id)?;
                // RUSTFS_COMPAT_TODO(backlog-2539): Remove after all pre-marker administrator ErasureSet intents are retired.
                // Admission historically emitted an omitted marker with an empty list.
                let all_buckets = all_buckets.unwrap_or(buckets.is_empty());
                if all_buckets != buckets.is_empty() {
                    return Err(Error::Other(
                        "Admin heal recovery bucket scope conflicts with its bucket list".to_string(),
                    ));
                }
                for bucket in buckets {
                    validate_recovery_component("bucket", bucket)?;
                }
            }
            Self::Metadata { bucket, object } => {
                validate_recovery_component("bucket", bucket)?;
                validate_recovery_component("object", object)?;
            }
        }
        Ok(())
    }
}

impl TryFrom<&HealType> for RecoveryHealType {
    type Error = Error;

    fn try_from(heal_type: &HealType) -> Result<Self> {
        Ok(match heal_type {
            HealType::Cluster => Self::Cluster,
            HealType::Bucket { bucket } => Self::Bucket { bucket: bucket.clone() },
            HealType::Object {
                bucket,
                object,
                version_id,
            } => Self::Object {
                bucket: bucket.clone(),
                object: object.clone(),
                version_id: version_id.clone(),
            },
            HealType::Prefix { bucket, prefix } => Self::Prefix {
                bucket: bucket.clone(),
                prefix: prefix.clone(),
            },
            HealType::ErasureSet { buckets, set_disk_id } => Self::ErasureSet {
                buckets: buckets.clone(),
                set_disk_id: set_disk_id.clone(),
                all_buckets: buckets.is_empty().then_some(true),
            },
            HealType::Metadata { bucket, object } => Self::Metadata {
                bucket: bucket.clone(),
                object: object.clone(),
            },
            HealType::ECDecode {
                bucket,
                object,
                version_id,
            } => Self::EcDecode {
                bucket: bucket.clone(),
                object: object.clone(),
                version_id: version_id.clone(),
            },
            HealType::DeleteMarkerPurge { .. } => {
                return Err(Error::Other("Delete-marker purge recovery is owned by the MRF journal".to_string()));
            }
        })
    }
}

impl From<RecoveryHealType> for HealType {
    fn from(heal_type: RecoveryHealType) -> Self {
        match heal_type {
            RecoveryHealType::Cluster => Self::Cluster,
            RecoveryHealType::Bucket { bucket } => Self::Bucket { bucket },
            RecoveryHealType::Object {
                bucket,
                object,
                version_id,
            } => Self::Object {
                bucket,
                object,
                version_id,
            },
            RecoveryHealType::Prefix { bucket, prefix } => Self::Prefix { bucket, prefix },
            RecoveryHealType::ErasureSet {
                buckets, set_disk_id, ..
            } => Self::ErasureSet { buckets, set_disk_id },
            RecoveryHealType::Metadata { bucket, object } => Self::Metadata { bucket, object },
            RecoveryHealType::EcDecode {
                bucket,
                object,
                version_id,
            } => Self::ECDecode {
                bucket,
                object,
                version_id,
            },
        }
    }
}

fn validate_recovery_component(label: &str, value: &str) -> Result<()> {
    if value.is_empty() || value.contains('\0') {
        return Err(Error::Other(format!("Invalid admin heal recovery {label}")));
    }
    Ok(())
}

fn default_recovery_heal_type() -> RecoveryHealType {
    RecoveryHealType::Cluster
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RootHealIntent {
    schema: u32,
    task_id: String,
    #[serde(default = "default_recovery_heal_type")]
    heal_type: RecoveryHealType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    bucket_incarnation_id: Option<Uuid>,
    #[serde(deserialize_with = "decode_options")]
    options: HealOptions,
    priority: HealPriority,
    retry_attempts: u32,
    created_at: SystemTime,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RootHealTerminal {
    schema: u32,
    task_id: String,
    heal_type: RecoveryHealType,
    status: HealTaskStatus,
    #[serde(default, deserialize_with = "decode_options")]
    options: HealOptions,
    progress: Option<HealProgress>,
    completed_at: SystemTime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RootHealRecordKind {
    Intent,
    Terminal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RootHealQuarantine {
    schema: u32,
    task_id: String,
    record_kind: RootHealRecordKind,
    source_prefix_len: usize,
    source_prefix_sha256: Vec<u8>,
    source_oversized: bool,
    reason: String,
    quarantined_at: SystemTime,
}

struct RootHealSource {
    bytes: EcstoreDiskBytes,
    source_prefix_sha256: Vec<u8>,
    source_oversized: bool,
}

#[derive(Debug, Default)]
pub(super) struct RootHealInventory {
    pub(super) requests: Vec<HealRequest>,
    pub(super) quarantined_task_ids: HashSet<String>,
}

impl RootHealTerminal {
    fn validate(&self, task_id: &str) -> Result<()> {
        let _ = terminal_path(task_id)?;
        if self.schema != ROOT_TERMINAL_SCHEMA || self.task_id != task_id {
            return Err(Error::Other(format!("Unsupported or mismatched root heal terminal record {task_id}")));
        }
        self.heal_type.validate()?;
        if !matches!(
            self.status,
            HealTaskStatus::Completed | HealTaskStatus::Cancelled | HealTaskStatus::Failed { .. }
        ) {
            return Err(Error::Other(format!("Non-terminal root heal receipt {task_id}")));
        }
        Ok(())
    }

    fn from_completed(task_id: &str, completed: &CompletedHealStatus) -> Result<Self> {
        Ok(Self {
            schema: ROOT_TERMINAL_SCHEMA,
            task_id: task_id.to_owned(),
            heal_type: RecoveryHealType::try_from(&completed.heal_type)?,
            status: completed.status.clone(),
            options: completed.options.clone(),
            progress: completed.progress.clone(),
            completed_at: completed.completed_at,
        })
    }

    fn cancelled(task_id: &str, heal_type: &HealType, options: HealOptions) -> Result<Self> {
        Ok(Self {
            schema: ROOT_TERMINAL_SCHEMA,
            task_id: task_id.to_owned(),
            heal_type: RecoveryHealType::try_from(heal_type)?,
            status: HealTaskStatus::Cancelled,
            options,
            progress: None,
            completed_at: SystemTime::now(),
        })
    }

    fn into_completed(self) -> CompletedHealStatus {
        CompletedHealStatus {
            outcome: None,
            progress: self.progress,
            retained_bytes: std::sync::OnceLock::new(),
            heal_type: self.heal_type.into(),
            options: self.options,
            status: self.status,
            result_items_truncated: false,
            completed_at: self.completed_at,
            seqed_items: Vec::new(),
            next_seq: 0,
            min_seq: 0,
        }
    }

    fn retained_at(&self, now: SystemTime) -> bool {
        now.duration_since(self.completed_at)
            .map(|age| age <= KEEP_HEAL_TASK_STATUS_DURATION)
            .unwrap_or(true)
    }
}

impl RootHealIntent {
    fn from_request(request: &HealRequest) -> Result<Self> {
        Ok(Self {
            schema: ROOT_RECOVERY_SCHEMA,
            task_id: request.id.clone(),
            heal_type: RecoveryHealType::try_from(&request.heal_type)?,
            bucket_incarnation_id: request.bucket_incarnation_id,
            options: request.options.clone(),
            priority: request.priority,
            retry_attempts: request.retry_attempts,
            created_at: request.created_at,
        })
    }

    fn into_request(self) -> HealRequest {
        let mut request = HealRequest::new(self.heal_type.into(), self.options, self.priority);
        request.id = self.task_id;
        request.bucket_incarnation_id = self.bucket_incarnation_id;
        request.source = HealRequestSource::Admin;
        request.retry_attempts = self.retry_attempts;
        request.created_at = self.created_at;
        request
    }
}

#[derive(Default)]
pub(crate) struct RootHealRecovery {
    mutation: Mutex<()>,
    #[cfg(any(test, feature = "test-util"))]
    disabled_for_tests: bool,
    #[cfg(test)]
    disks: Option<Vec<DiskStore>>,
    #[cfg(test)]
    pub(super) fail_after_terminal_write: std::sync::atomic::AtomicBool,
}

pub(super) fn is_admin_heal_recovery(heal_type: &HealType, source: HealRequestSource) -> bool {
    source == HealRequestSource::Admin
        && matches!(
            heal_type,
            HealType::Cluster
                | HealType::Bucket { .. }
                | HealType::Object { .. }
                | HealType::Prefix { .. }
                | HealType::ErasureSet { .. }
                | HealType::Metadata { .. }
                | HealType::ECDecode { .. }
        )
}

fn decode_options<'de, D: serde::Deserializer<'de>>(deserializer: D) -> std::result::Result<HealOptions, D::Error> {
    let value = serde_json::Value::deserialize(deserializer)?;
    let object = value
        .as_object()
        .ok_or_else(|| serde::de::Error::custom("root heal options must be an object"))?;
    const FIELDS: &[&str] = &[
        "scan_mode",
        "remove_corrupted",
        "recreate_missing",
        "update_parity",
        "recursive",
        "dry_run",
        "no_lock",
        "timeout",
        "pool_index",
        "set_index",
    ];
    if object.keys().any(|key| !FIELDS.contains(&key.as_str())) {
        return Err(serde::de::Error::custom("unknown root heal recovery option"));
    }
    let options: HealOptions = serde_json::from_value(value).map_err(serde::de::Error::custom)?;
    if options.no_lock {
        return Err(serde::de::Error::custom("administrator root heal cannot skip namespace locking"));
    }
    Ok(options)
}

fn intent_path(task_id: &str) -> Result<String> {
    let parsed = uuid::Uuid::parse_str(task_id).map_err(|_| Error::Other("Invalid root heal recovery task id".to_string()))?;
    if parsed.to_string() != task_id {
        return Err(Error::Other("Noncanonical root heal recovery task id".to_string()));
    }
    Ok(format!("{ROOT_RECOVERY_PREFIX}{task_id}.json"))
}

fn terminal_path(task_id: &str) -> Result<String> {
    let parsed = uuid::Uuid::parse_str(task_id).map_err(|_| Error::Other("Invalid root heal terminal task id".to_string()))?;
    if parsed.to_string() != task_id {
        return Err(Error::Other("Noncanonical root heal terminal task id".to_string()));
    }
    Ok(format!("{ROOT_TERMINAL_PREFIX}{task_id}.json"))
}

fn quarantine_path(record_kind: RootHealRecordKind, task_id: &str) -> Result<String> {
    let _ = match record_kind {
        RootHealRecordKind::Intent => intent_path(task_id)?,
        RootHealRecordKind::Terminal => terminal_path(task_id)?,
    };
    let prefix = match record_kind {
        RootHealRecordKind::Intent => ROOT_QUARANTINE_INTENT_PREFIX,
        RootHealRecordKind::Terminal => ROOT_QUARANTINE_TERMINAL_PREFIX,
    };
    Ok(format!("{prefix}{task_id}.json"))
}

fn parse_quarantine_entry(entry: &str) -> Option<(RootHealRecordKind, &str)> {
    let (record_kind, suffix) = if let Some(suffix) = entry
        .strip_prefix(ROOT_QUARANTINE_INTENT_PREFIX)
        .and_then(|entry| entry.strip_suffix(".json"))
    {
        (RootHealRecordKind::Intent, suffix)
    } else {
        let suffix = entry
            .strip_prefix(ROOT_QUARANTINE_TERMINAL_PREFIX)
            .and_then(|entry| entry.strip_suffix(".json"))?;
        (RootHealRecordKind::Terminal, suffix)
    };
    Uuid::parse_str(suffix).ok().filter(|id| id.to_string() == suffix)?;
    Some((record_kind, suffix))
}

async fn read_bounded(disk: &DiskStore, path: &str) -> Result<Option<EcstoreDiskBytes>> {
    let reader = match EcstoreDiskAPI::read_file(disk.as_ref(), RUSTFS_META_BUCKET, path).await {
        Ok(reader) => reader,
        Err(DiskError::FileNotFound) => return Ok(None),
        Err(error) => return Err(Error::Disk(error)),
    };
    let mut bytes = Vec::new();
    reader
        .take(u64::try_from(MAX_ROOT_RECOVERY_BYTES + 1).map_err(Error::other)?)
        .read_to_end(&mut bytes)
        .await?;
    Ok(Some(bytes.into()))
}

async fn read_source(disk: &DiskStore, path: &str) -> Result<Option<RootHealSource>> {
    let Some(bytes) = read_bounded(disk, path).await? else {
        return Ok(None);
    };
    Ok(Some(RootHealSource {
        source_prefix_sha256: Sha256::digest(&bytes).to_vec(),
        source_oversized: bytes.len() > MAX_ROOT_RECOVERY_BYTES,
        bytes,
    }))
}

fn decode_quarantine(task_id: &str, record_kind: RootHealRecordKind, bytes: &[u8]) -> Result<RootHealQuarantine> {
    let marker: RootHealQuarantine = serde_json::from_slice(bytes)
        .map_err(|error| Error::Other(format!("Invalid root heal quarantine marker {task_id}: {error}")))?;
    if marker.schema != ROOT_QUARANTINE_SCHEMA
        || marker.task_id != task_id
        || marker.record_kind != record_kind
        || marker.source_prefix_sha256.len() != 32
    {
        return Err(Error::Other(format!("Unsupported or mismatched root heal quarantine marker {task_id}")));
    }
    let _ = quarantine_path(record_kind, task_id)?;
    Ok(marker)
}

fn encode_quarantine(
    task_id: &str,
    record_kind: RootHealRecordKind,
    source: &RootHealSource,
    reason: &str,
) -> Result<EcstoreDiskBytes> {
    let marker = RootHealQuarantine {
        schema: ROOT_QUARANTINE_SCHEMA,
        task_id: task_id.to_owned(),
        record_kind,
        source_prefix_len: source.bytes.len(),
        source_prefix_sha256: source.source_prefix_sha256.clone(),
        source_oversized: source.source_oversized,
        reason: reason.chars().take(256).collect(),
        quarantined_at: SystemTime::now(),
    };
    serde_json::to_vec(&marker)
        .map(EcstoreDiskBytes::from)
        .map_err(|error| Error::Serialization(format!("Serialize root heal quarantine marker: {error}")))
}

fn decode_intent(task_id: &str, bytes: &[u8]) -> Result<RootHealIntent> {
    let _ = intent_path(task_id)?;
    if bytes.len() > MAX_ROOT_RECOVERY_BYTES {
        return Err(Error::Other(format!("Root heal recovery record exceeds its size limit {task_id}")));
    }
    let mut intent: RootHealIntent = serde_json::from_slice(bytes)
        .map_err(|error| Error::Other(format!("Invalid root heal recovery record {task_id}: {error}")))?;
    if intent.task_id != task_id {
        return Err(Error::Other(format!("Unsupported or mismatched root heal recovery record {task_id}")));
    }
    match intent.schema {
        LEGACY_ROOT_RECOVERY_SCHEMA
            if intent.heal_type == RecoveryHealType::Cluster && intent.bucket_incarnation_id.is_none() => {}
        SCOPED_ROOT_RECOVERY_SCHEMA if intent.bucket_incarnation_id.is_none() => {}
        ROOT_RECOVERY_SCHEMA => {}
        _ => return Err(Error::Other(format!("Unsupported or mismatched root heal recovery record {task_id}"))),
    }
    if !matches!(intent.heal_type, RecoveryHealType::Bucket { .. }) && intent.bucket_incarnation_id.is_some() {
        return Err(Error::Other(format!(
            "Unexpected bucket incarnation in root heal recovery record {task_id}"
        )));
    }
    intent.heal_type.normalize_legacy_scope();
    intent.heal_type.validate()?;
    Ok(intent)
}

fn decode_terminal(task_id: &str, bytes: &[u8]) -> Result<RootHealTerminal> {
    let _ = terminal_path(task_id)?;
    if bytes.len() > MAX_ROOT_RECOVERY_BYTES {
        return Err(Error::Other(format!("Root heal terminal record exceeds its size limit {task_id}")));
    }
    let terminal: RootHealTerminal = serde_json::from_slice(bytes)
        .map_err(|error| Error::Other(format!("Invalid root heal terminal record {task_id}: {error}")))?;
    terminal.validate(task_id)?;
    Ok(terminal)
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct RootTerminalGcReport {
    pub(super) scanned: usize,
    pub(super) retained: usize,
    pub(super) pending_removed: usize,
    pub(super) terminals_removed: usize,
    pub(super) reports_removed: usize,
    pub(super) budget_exhausted: bool,
}

impl RootHealRecovery {
    #[cfg(test)]
    pub(super) fn with_disks(disks: Vec<DiskStore>) -> Self {
        Self {
            mutation: Mutex::new(()),
            disabled_for_tests: false,
            disks: Some(disks),
            fail_after_terminal_write: Default::default(),
        }
    }

    #[cfg(any(test, feature = "test-util"))]
    pub(super) fn disabled_for_tests() -> Self {
        Self {
            mutation: Mutex::new(()),
            disabled_for_tests: true,
            #[cfg(test)]
            disks: None,
            #[cfg(test)]
            fail_after_terminal_write: Default::default(),
        }
    }

    async fn disks(&self) -> Result<Vec<DiskStore>> {
        #[cfg(any(test, feature = "test-util"))]
        if self.disabled_for_tests {
            return Ok(Vec::new());
        }
        #[cfg(test)]
        if let Some(disks) = &self.disks {
            return Ok(disks.clone());
        }
        let map = local_disk_map_read().await;
        if map.values().any(Option::is_none) {
            return Err(Error::Other("Root heal recovery owner may be on an unavailable local disk".to_string()));
        }
        let mut disks = map.values().flatten().cloned().collect::<Vec<_>>();
        disks.sort_by_key(|disk| EcstoreDiskAPI::endpoint(disk.as_ref()).to_string());
        Ok(disks)
    }

    pub(crate) async fn resume_disk(&self, task_id: &str) -> Result<Option<DiskStore>> {
        #[cfg(any(test, feature = "test-util"))]
        if self.disabled_for_tests {
            return Ok(None);
        }
        let _guard = self.mutation.lock().await;
        Self::find(&self.disks().await?, task_id)
            .await?
            .map(|(disk, _)| Some(disk))
            .ok_or_else(|| Error::other("Administrator heal recovery owner is missing"))
    }

    async fn find(disks: &[DiskStore], task_id: &str) -> Result<Option<(DiskStore, EcstoreDiskBytes)>> {
        let path = intent_path(task_id)?;
        let mut found = None;
        for disk in disks {
            // read_all reports FileNotFound even when the whole metadata
            // volume is absent; that is an unknown owner, not empty state.
            EcstoreDiskAPI::stat_volume(disk.as_ref(), RUSTFS_META_BUCKET).await?;
            if let Some(bytes) = read_bounded(disk, &path).await? {
                decode_intent(task_id, &bytes)?;
                if found.is_some() {
                    return Err(Error::Other(format!("Multiple root heal recovery owners for {task_id}")));
                }
                found = Some((disk.clone(), bytes));
            }
        }
        Ok(found)
    }

    async fn find_terminal(disks: &[DiskStore], task_id: &str) -> Result<Option<(DiskStore, EcstoreDiskBytes)>> {
        let path = terminal_path(task_id)?;
        let mut found = None;
        for disk in disks {
            EcstoreDiskAPI::stat_volume(disk.as_ref(), RUSTFS_META_BUCKET).await?;
            if let Some(bytes) = read_bounded(disk, &path).await? {
                decode_terminal(task_id, &bytes)?;
                if found.is_some() {
                    return Err(Error::Other(format!("Multiple root heal terminal owners for {task_id}")));
                }
                found = Some((disk.clone(), bytes));
            }
        }
        Ok(found)
    }

    async fn find_retained_terminal(
        disks: &[DiskStore],
        task_id: &str,
        now: SystemTime,
    ) -> Result<Option<(DiskStore, EcstoreDiskBytes)>> {
        let Some((disk, bytes)) = Self::find_terminal(disks, task_id).await? else {
            return Ok(None);
        };
        if decode_terminal(task_id, &bytes)?.retained_at(now) {
            return Ok(Some((disk, bytes)));
        }
        Ok(None)
    }

    async fn read_raw(disks: &[DiskStore], path: &str) -> Result<Vec<(DiskStore, RootHealSource)>> {
        let mut found = Vec::new();
        for disk in disks {
            if let Some(source) = read_source(disk, path).await? {
                found.push((disk.clone(), source));
            }
        }
        Ok(found)
    }

    async fn write_quarantine(
        owner: &DiskStore,
        task_id: &str,
        record_kind: RootHealRecordKind,
        source: &RootHealSource,
        reason: &str,
    ) -> Result<RootHealQuarantine> {
        let marker = encode_quarantine(task_id, record_kind, source, reason)?;
        let path = quarantine_path(record_kind, task_id)?;
        let result =
            EcstoreDiskAPI::compare_and_update_file(owner.as_ref(), RUSTFS_META_BUCKET, &path, None, Some(marker.clone()))
                .await?;
        let marker_bytes = match result {
            EcstoreConditionalFileUpdate::Updated => marker,
            EcstoreConditionalFileUpdate::Mismatch => read_bounded(owner, &path)
                .await?
                .ok_or_else(|| Error::other(format!("Root heal quarantine marker disappeared {task_id}")))?,
            EcstoreConditionalFileUpdate::Missing => {
                return Err(Error::other(format!("Root heal quarantine owner is unavailable {task_id}")));
            }
        };
        let marker = decode_quarantine(task_id, record_kind, &marker_bytes)?;
        if marker.source_prefix_len != source.bytes.len()
            || marker.source_prefix_sha256 != source.source_prefix_sha256
            || marker.source_oversized != source.source_oversized
        {
            return Err(Error::other(format!("Root heal quarantine source changed {task_id}")));
        }
        Ok(marker)
    }

    async fn quarantine_marker_map(
        disks: &[DiskStore],
    ) -> Result<HashMap<(RootHealRecordKind, String), (DiskStore, RootHealQuarantine)>> {
        let mut markers = HashMap::new();
        for disk in disks {
            let entries = match EcstoreDiskAPI::list_dir(disk.as_ref(), "", RUSTFS_META_BUCKET, "", -1).await {
                Ok(entries) => entries,
                Err(DiskError::FileNotFound) => continue,
                Err(error) => return Err(Error::Disk(error)),
            };
            for entry in entries {
                let Some((record_kind, task_id)) = parse_quarantine_entry(&entry) else {
                    continue;
                };
                let Some(bytes) = read_bounded(disk, &entry).await? else {
                    return Err(Error::other(format!("Root heal quarantine marker disappeared {task_id}")));
                };
                let marker = decode_quarantine(task_id, record_kind, &bytes)?;
                let key = (record_kind, task_id.to_owned());
                if markers.insert(key, (disk.clone(), marker)).is_some() {
                    return Err(Error::other(format!("Multiple root heal quarantine owners for {task_id}")));
                }
            }
        }
        Ok(markers)
    }

    async fn inventory_locked(&self, disks: &[DiskStore]) -> Result<RootHealInventory> {
        let mut intent_ids = HashSet::new();
        let mut terminal_ids = HashSet::new();
        for disk in disks {
            let entries = match EcstoreDiskAPI::list_dir(disk.as_ref(), "", RUSTFS_META_BUCKET, "", -1).await {
                Ok(entries) => entries,
                Err(DiskError::FileNotFound) => continue,
                Err(error) => return Err(Error::Disk(error)),
            };
            for entry in entries {
                if let Some(task_id) = entry
                    .strip_prefix(ROOT_RECOVERY_PREFIX)
                    .and_then(|entry| entry.strip_suffix(".json"))
                {
                    let _ = intent_path(task_id)?;
                    intent_ids.insert(task_id.to_owned());
                } else if let Some(task_id) = entry
                    .strip_prefix(ROOT_TERMINAL_PREFIX)
                    .and_then(|entry| entry.strip_suffix(".json"))
                {
                    let _ = terminal_path(task_id)?;
                    terminal_ids.insert(task_id.to_owned());
                }
            }
        }

        let markers = Self::quarantine_marker_map(disks).await?;
        for (record_kind, task_id) in markers.keys() {
            match record_kind {
                RootHealRecordKind::Intent => {
                    intent_ids.insert(task_id.clone());
                }
                RootHealRecordKind::Terminal => {
                    terminal_ids.insert(task_id.clone());
                }
            }
        }
        let mut inventory = RootHealInventory {
            requests: Vec::new(),
            quarantined_task_ids: markers.keys().map(|(_, task_id)| task_id.clone()).collect(),
        };
        for task_id in intent_ids.union(&terminal_ids) {
            let terminal_raw = Self::read_raw(disks, &terminal_path(task_id)?).await?;
            if terminal_raw.len() > 1 {
                return Err(Error::other(format!("Multiple root heal terminal owners for {task_id}")));
            }
            if let Some((owner, source)) = terminal_raw.into_iter().next() {
                if let Some(marker) = markers.get(&(RootHealRecordKind::Terminal, task_id.clone())) {
                    if EcstoreDiskAPI::endpoint(marker.0.as_ref()) != EcstoreDiskAPI::endpoint(owner.as_ref())
                        || marker.1.source_prefix_len != source.bytes.len()
                        || marker.1.source_prefix_sha256 != source.source_prefix_sha256
                        || marker.1.source_oversized != source.source_oversized
                    {
                        return Err(Error::other(format!("Root heal quarantine source changed {task_id}")));
                    }
                    inventory.quarantined_task_ids.insert(task_id.clone());
                    continue;
                }
                match decode_terminal(task_id, &source.bytes) {
                    Ok(_) => continue,
                    Err(error) => {
                        Self::write_quarantine(&owner, task_id, RootHealRecordKind::Terminal, &source, &error.to_string())
                            .await?;
                        inventory.quarantined_task_ids.insert(task_id.clone());
                        continue;
                    }
                }
            }
            if let Some(marker) = markers.get(&(RootHealRecordKind::Terminal, task_id.clone())) {
                return Err(Error::other(format!(
                    "Root heal quarantine marker has no retained terminal source {} ({} bytes)",
                    task_id, marker.1.source_prefix_len
                )));
            }

            let intent_raw = Self::read_raw(disks, &intent_path(task_id)?).await?;
            if intent_raw.len() > 1 {
                return Err(Error::other(format!("Multiple root heal recovery owners for {task_id}")));
            }
            let Some((owner, source)) = intent_raw.into_iter().next() else {
                if let Some(marker) = markers.get(&(RootHealRecordKind::Intent, task_id.clone())) {
                    return Err(Error::other(format!(
                        "Root heal quarantine marker has no retained intent source {} ({} bytes)",
                        task_id, marker.1.source_prefix_len
                    )));
                }
                continue;
            };
            if let Some(marker) = markers.get(&(RootHealRecordKind::Intent, task_id.clone())) {
                if EcstoreDiskAPI::endpoint(marker.0.as_ref()) != EcstoreDiskAPI::endpoint(owner.as_ref())
                    || marker.1.source_prefix_len != source.bytes.len()
                    || marker.1.source_prefix_sha256 != source.source_prefix_sha256
                    || marker.1.source_oversized != source.source_oversized
                {
                    return Err(Error::other(format!("Root heal quarantine source changed {task_id}")));
                }
                inventory.quarantined_task_ids.insert(task_id.clone());
                continue;
            }
            match decode_intent(task_id, &source.bytes) {
                Ok(intent) => inventory.requests.push(intent.into_request()),
                Err(error) => {
                    Self::write_quarantine(&owner, task_id, RootHealRecordKind::Intent, &source, &error.to_string()).await?;
                    inventory.quarantined_task_ids.insert(task_id.clone());
                }
            }
        }
        inventory
            .requests
            .sort_by(|left, right| left.created_at.cmp(&right.created_at).then_with(|| left.id.cmp(&right.id)));
        Ok(inventory)
    }

    async fn persist_terminal_locked(
        disks: &[DiskStore],
        task_id: &str,
        terminal: RootHealTerminal,
        completed: &CompletedHealStatus,
    ) -> Result<Option<(DiskStore, EcstoreDiskBytes)>> {
        let path = terminal_path(task_id)?;
        if let Some((disk, bytes)) = Self::find_terminal(disks, task_id).await? {
            let current = decode_terminal(task_id, &bytes)?;
            // Only cancellation has a second publication: the worker may finish
            // recording its last object after cancellation retired active ownership.
            let cancelled_refinement = current.status == HealTaskStatus::Cancelled
                && terminal.status == HealTaskStatus::Cancelled
                && current.heal_type == terminal.heal_type
                && current.options == terminal.options;
            if current != terminal && !cancelled_refinement {
                return Err(Error::Other(format!("Root heal terminal record changed for {task_id}")));
            }
            write_report(&disk, task_id, &RootHealReport::from_completed(current, completed)).await?;
            return Self::find(disks, task_id).await;
        }
        let pending = Self::find(disks, task_id).await?;
        let disk = pending
            .as_ref()
            .map(|(disk, _)| disk.clone())
            .or_else(|| disks.first().cloned())
            .ok_or_else(|| Error::Other("No local disk available for root heal terminal receipt".to_string()))?;
        let bytes = serde_json::to_vec(&terminal)
            .map_err(|error| Error::Other(format!("Serialize root heal terminal receipt: {error}")))?;
        // RUSTFS_COMPAT_TODO(backlog-2519): preserve rollback fences. Remove after supported readers accept standalone reports.
        // A crash before the marker leaves the pending responsibility intact.
        write_report(&disk, task_id, &RootHealReport::from_completed(terminal, completed)).await?;
        match EcstoreDiskAPI::compare_and_update_file(disk.as_ref(), RUSTFS_META_BUCKET, &path, None, Some(bytes.into())).await? {
            EcstoreConditionalFileUpdate::Updated => Ok(pending),
            _ => Err(Error::Other(format!("Root heal terminal record changed for {task_id}"))),
        }
    }

    pub(super) async fn persist(&self, request: &HealRequest) -> Result<()> {
        if !is_admin_heal_recovery(&request.heal_type, request.source) {
            return Ok(());
        }
        #[cfg(any(test, feature = "test-util"))]
        if self.disabled_for_tests {
            return Ok(());
        }
        let _guard = self.mutation.lock().await;
        let disks = self.disks().await?;
        let existing = Self::find(&disks, &request.id).await?;
        if request.options.no_lock {
            return Err(Error::Other("Administrator root heal cannot skip namespace locking".to_string()));
        }
        let intent = RootHealIntent::from_request(request)?;
        intent.heal_type.validate()?;
        let bytes =
            serde_json::to_vec(&intent).map_err(|error| Error::Other(format!("Serialize root heal recovery record: {error}")))?;
        let path = intent_path(&request.id)?;
        if let Some((disk, expected)) = existing {
            return match EcstoreDiskAPI::compare_and_update_file(
                disk.as_ref(),
                RUSTFS_META_BUCKET,
                &path,
                Some(expected),
                Some(bytes.into()),
            )
            .await?
            {
                EcstoreConditionalFileUpdate::Updated => Ok(()),
                _ => Err(Error::Other(format!("Root heal recovery record changed for {}", request.id))),
            };
        }

        if disks.is_empty() {
            return Err(Error::Other("No local disk available for root heal shutdown recovery".to_string()));
        }
        let mut last_not_committed = None;
        for disk in &disks {
            match EcstoreDiskAPI::compare_and_update_file(
                disk.as_ref(),
                RUSTFS_META_BUCKET,
                &path,
                None,
                Some(bytes.clone().into()),
            )
            .await
            {
                Ok(EcstoreConditionalFileUpdate::Updated) => return Ok(()),
                Ok(_) => return Err(Error::Other(format!("Root heal recovery record changed for {}", request.id))),
                Err(error) if error.is_conditional_file_not_committed() => last_not_committed = Some(error),
                Err(error) => return Err(Error::Disk(error)),
            }
        }
        match last_not_committed {
            Some(error) => Err(Error::Disk(error)),
            None => Err(Error::Other("No local disk accepted the root heal recovery record".to_string())),
        }
    }

    pub(super) async fn remove(&self, task_id: &str, heal_type: &HealType, source: HealRequestSource) -> Result<bool> {
        if !is_admin_heal_recovery(heal_type, source) {
            return Ok(false);
        }
        #[cfg(any(test, feature = "test-util"))]
        if self.disabled_for_tests {
            return Ok(false);
        }
        self.remove_pending_by_id(task_id).await
    }

    async fn remove_pending_by_id(&self, task_id: &str) -> Result<bool> {
        let _guard = self.mutation.lock().await;
        let Some((disk, bytes)) = Self::find(&self.disks().await?, task_id).await? else {
            return Ok(false);
        };
        match EcstoreDiskAPI::compare_and_update_file(
            disk.as_ref(),
            RUSTFS_META_BUCKET,
            &intent_path(task_id)?,
            Some(bytes),
            None,
        )
        .await?
        {
            EcstoreConditionalFileUpdate::Updated => Ok(true),
            _ => Err(Error::Other(format!("Root heal recovery record changed while retiring {task_id}"))),
        }
    }

    pub(super) async fn checkpoint_failed_execution(&self, task: &HealTask) -> Result<()> {
        if !is_admin_heal_recovery(&task.heal_type, task.source) {
            return Ok(());
        }
        let remaining = match task.retry_request_with_remaining_timeout().await {
            Ok(request) => request.options.timeout,
            Err(Error::TaskTimeout) => Some(Duration::ZERO),
            Err(error) => return Err(error),
        };
        let _guard = self.mutation.lock().await;
        let Some((disk, expected)) = Self::find(&self.disks().await?, &task.id).await? else {
            // A first execution that failed has no restart handoff to update.
            return Ok(());
        };
        let mut intent = decode_intent(&task.id, &expected)?;
        if HealType::from(intent.heal_type.clone()) != task.heal_type
            || intent.bucket_incarnation_id != task.bucket_incarnation_id
        {
            return Err(Error::Other(format!("Root heal recovery owner changed for {}", task.id)));
        }
        let mut expected_options = intent.options.clone();
        expected_options.timeout = task.options.timeout;
        if intent.created_at != task.created_at || intent.priority != task.priority || expected_options != task.options {
            return Err(Error::Other(format!("Root heal recovery owner changed for {}", task.id)));
        }
        // A terminal timeout leaves no runtime owner for stop() to snapshot.
        // Checkpoint its consumed budget before publishing terminal status;
        // never refund time if an earlier checkpoint is already stricter.
        intent.options.timeout = match (intent.options.timeout, remaining) {
            (Some(previous), Some(remaining)) => Some(previous.min(remaining)),
            (previous, remaining) => previous.or(remaining),
        };
        intent.retry_attempts = intent.retry_attempts.max(task.retry_attempts);
        let bytes = serde_json::to_vec(&intent)
            .map_err(|error| Error::Other(format!("Serialize root heal recovery checkpoint: {error}")))?;
        match EcstoreDiskAPI::compare_and_update_file(
            disk.as_ref(),
            RUSTFS_META_BUCKET,
            &intent_path(&task.id)?,
            Some(expected),
            Some(bytes.into()),
        )
        .await?
        {
            EcstoreConditionalFileUpdate::Updated => Ok(()),
            _ => Err(Error::Other(format!("Root heal recovery record changed while checkpointing {}", task.id))),
        }
    }

    pub(super) async fn cancel_pending(&self, task_id: &str) -> Result<bool> {
        #[cfg(any(test, feature = "test-util"))]
        if self.disabled_for_tests {
            return Ok(false);
        }
        if intent_path(task_id).is_err() {
            return Ok(false);
        }
        let _guard = self.mutation.lock().await;
        let disks = self.disks().await?;
        if Self::find_terminal(&disks, task_id).await?.is_some() {
            if let Some((disk, bytes)) = Self::find(&disks, task_id).await? {
                match EcstoreDiskAPI::compare_and_update_file(
                    disk.as_ref(),
                    RUSTFS_META_BUCKET,
                    &intent_path(task_id)?,
                    Some(bytes),
                    None,
                )
                .await?
                {
                    EcstoreConditionalFileUpdate::Updated => {}
                    _ => return Err(Error::Other(format!("Root heal recovery record changed while cancelling {task_id}"))),
                }
            }
            return Ok(true);
        }
        let Some((disk, bytes)) = Self::find(&disks, task_id).await? else {
            return Ok(false);
        };
        let pending = decode_intent(task_id, &bytes)?;
        let heal_type = HealType::from(pending.heal_type);
        let terminal = RootHealTerminal::cancelled(task_id, &heal_type, pending.options)?;
        let completed = terminal.clone().into_completed();
        let _ = Self::persist_terminal_locked(&disks, task_id, terminal, &completed).await?;
        match EcstoreDiskAPI::compare_and_update_file(
            disk.as_ref(),
            RUSTFS_META_BUCKET,
            &intent_path(task_id)?,
            Some(bytes),
            None,
        )
        .await?
        {
            EcstoreConditionalFileUpdate::Updated => Ok(true),
            _ => Err(Error::Other(format!("Root heal recovery record changed while cancelling {task_id}"))),
        }
    }

    pub(super) async fn persist_terminal(
        &self,
        task_id: &str,
        heal_type: &HealType,
        source: HealRequestSource,
        completed: &CompletedHealStatus,
    ) -> Result<bool> {
        if !is_admin_heal_recovery(heal_type, source) || completed.heal_type != *heal_type {
            return Ok(false);
        }
        #[cfg(any(test, feature = "test-util"))]
        if self.disabled_for_tests {
            return Ok(false);
        }
        let _guard = self.mutation.lock().await;
        let disks = self.disks().await?;
        let pending =
            Self::persist_terminal_locked(&disks, task_id, RootHealTerminal::from_completed(task_id, completed)?, completed)
                .await?;
        #[cfg(test)]
        if self
            .fail_after_terminal_write
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(Error::Io(std::io::Error::other("injected failure after terminal report publication")));
        }
        if let Some((disk, bytes)) = pending {
            match EcstoreDiskAPI::compare_and_update_file(
                disk.as_ref(),
                RUSTFS_META_BUCKET,
                &intent_path(task_id)?,
                Some(bytes),
                None,
            )
            .await?
            {
                EcstoreConditionalFileUpdate::Updated => {
                    if matches!(heal_type, HealType::ErasureSet { .. }) && CheckpointManager::has_checkpoint(&disk, task_id).await
                    {
                        let checkpoint = CheckpointManager::load_from_disk(disk.clone(), task_id).await?;
                        if checkpoint.get_checkpoint().await.admin.is_some_and(|admin| admin.completed) {
                            checkpoint.cleanup().await?;
                            if ResumeManager::has_resume_state(&disk, task_id).await {
                                ResumeManager::load_from_disk(disk, task_id).await?.cleanup().await?;
                            }
                        }
                    }
                }
                _ => {
                    return Err(Error::Other(format!(
                        "Root heal recovery record changed while publishing terminal {task_id}"
                    )));
                }
            }
        }
        Ok(true)
    }

    pub(super) async fn completed(&self, task_id: &str) -> Result<Option<CompletedHealStatus>> {
        #[cfg(any(test, feature = "test-util"))]
        if self.disabled_for_tests {
            return Ok(None);
        }
        if terminal_path(task_id).is_err() {
            return Ok(None);
        }
        let _guard = self.mutation.lock().await;
        let disks = self.disks().await?;
        let Some((disk, bytes)) = Self::find_retained_terminal(&disks, task_id, SystemTime::now()).await? else {
            return Ok(None);
        };
        let terminal = decode_terminal(task_id, &bytes)?;
        if let Some((report, _)) = read_report(&disk, task_id).await? {
            if *report.terminal() != terminal {
                return Err(Error::Other(format!("Root heal report does not match its terminal receipt {task_id}")));
            }
            return Ok(Some(report.into_completed()));
        }
        // Legacy receipts cannot reconstruct counters or the retained result window.
        let mut completed = terminal.into_completed();
        completed.result_items_truncated = true;
        Ok(Some(completed))
    }

    pub(super) async fn completed_matches_path(&self, heal_path: &str) -> Result<bool> {
        #[cfg(any(test, feature = "test-util"))]
        if self.disabled_for_tests {
            return Ok(false);
        }
        let _guard = self.mutation.lock().await;
        let disks = self.disks().await?;
        for disk in &disks {
            EcstoreDiskAPI::stat_volume(disk.as_ref(), RUSTFS_META_BUCKET).await?;
            let entries = match EcstoreDiskAPI::list_dir(disk.as_ref(), "", RUSTFS_META_BUCKET, "", -1).await {
                Ok(entries) => entries,
                Err(DiskError::FileNotFound) => continue,
                Err(error) => return Err(Error::Disk(error)),
            };
            for entry in entries {
                let Some(task_id) = entry
                    .strip_prefix(ROOT_TERMINAL_PREFIX)
                    .and_then(|entry| entry.strip_suffix(".json"))
                else {
                    continue;
                };
                let Some((_, bytes)) = Self::find_retained_terminal(&disks, task_id, SystemTime::now()).await? else {
                    continue;
                };
                let heal_type = HealType::from(decode_terminal(task_id, &bytes)?.heal_type);
                if heal_type_matches_path(&heal_type, heal_path) {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    pub(super) async fn gc_terminal_receipts_once(&self, now: SystemTime) -> Result<RootTerminalGcReport> {
        #[cfg(any(test, feature = "test-util"))]
        if self.disabled_for_tests {
            return Ok(RootTerminalGcReport::default());
        }
        let _guard = self.mutation.lock().await;
        let disks = self.disks().await?;
        let mut report = RootTerminalGcReport::default();
        let mut ids = HashSet::new();
        for disk in &disks {
            if report.scanned >= ROOT_TERMINAL_GC_SCAN_BUDGET {
                report.budget_exhausted = true;
                break;
            }
            EcstoreDiskAPI::stat_volume(disk.as_ref(), RUSTFS_META_BUCKET).await?;
            let remaining = ROOT_TERMINAL_GC_SCAN_BUDGET.saturating_sub(report.scanned);
            let count = i32::try_from(remaining).unwrap_or(i32::MAX);
            let mut entries = match EcstoreDiskAPI::list_dir(disk.as_ref(), "", RUSTFS_META_BUCKET, "", count).await {
                Ok(entries) => entries,
                Err(DiskError::FileNotFound) => continue,
                Err(error) => return Err(Error::Disk(error)),
            };
            entries.sort_unstable();
            for entry in entries {
                if report.scanned >= ROOT_TERMINAL_GC_SCAN_BUDGET {
                    report.budget_exhausted = true;
                    break;
                }
                report.scanned += 1;
                let Some(task_id) = entry
                    .strip_prefix(ROOT_TERMINAL_PREFIX)
                    .or_else(|| entry.strip_prefix(ROOT_REPORT_PREFIX))
                    .and_then(|entry| entry.strip_suffix(".json"))
                else {
                    continue;
                };
                let _ = terminal_path(task_id)?;
                ids.insert(task_id.to_string());
            }
        }

        let mut ids = ids.into_iter().collect::<Vec<_>>();
        ids.sort();
        let mut deletes = 0usize;
        for task_id in ids {
            if deletes >= ROOT_TERMINAL_GC_DELETE_BUDGET {
                report.budget_exhausted = true;
                break;
            }
            let Some((terminal_disk, terminal_bytes)) = Self::find_terminal(&disks, &task_id).await? else {
                // A crash before terminal publication, or an old-version GC,
                // can leave an uncommitted report. It never authorizes replay.
                for disk in &disks {
                    if deletes >= ROOT_TERMINAL_GC_DELETE_BUDGET {
                        report.budget_exhausted = true;
                        break;
                    }
                    if let Some((snapshot, bytes)) = read_report(disk, &task_id).await? {
                        if snapshot.terminal().retained_at(now) {
                            report.retained += 1;
                        } else {
                            remove_report(disk, &task_id, bytes).await?;
                            deletes += 1;
                            report.reports_removed += 1;
                        }
                    }
                }
                continue;
            };
            let terminal = decode_terminal(&task_id, &terminal_bytes)?;
            if terminal.retained_at(now) {
                report.retained += 1;
                continue;
            }
            if let Some((pending_disk, pending_bytes)) = Self::find(&disks, &task_id).await? {
                match EcstoreDiskAPI::compare_and_update_file(
                    pending_disk.as_ref(),
                    RUSTFS_META_BUCKET,
                    &intent_path(&task_id)?,
                    Some(pending_bytes),
                    None,
                )
                .await?
                {
                    EcstoreConditionalFileUpdate::Updated => {
                        deletes += 1;
                        report.pending_removed += 1;
                        report.retained += 1;
                        continue;
                    }
                    _ => {
                        return Err(Error::Other(format!(
                            "Root heal recovery record changed while pruning terminal receipt {task_id}"
                        )));
                    }
                }
            }
            if let Some((snapshot, bytes)) = read_report(&terminal_disk, &task_id).await? {
                if *snapshot.terminal() != terminal {
                    return Err(Error::Other(format!("Root heal report does not match expired receipt {task_id}")));
                }
                remove_report(&terminal_disk, &task_id, bytes).await?;
                deletes += 1;
                report.reports_removed += 1;
                if deletes >= ROOT_TERMINAL_GC_DELETE_BUDGET {
                    report.retained += 1;
                    report.budget_exhausted = true;
                    break;
                }
            }
            match EcstoreDiskAPI::compare_and_update_file(
                terminal_disk.as_ref(),
                RUSTFS_META_BUCKET,
                &terminal_path(&task_id)?,
                Some(terminal_bytes),
                None,
            )
            .await?
            {
                EcstoreConditionalFileUpdate::Updated => {
                    deletes += 1;
                    report.terminals_removed += 1;
                }
                _ => return Err(Error::Other(format!("Root heal terminal record changed while pruning {task_id}"))),
            }
        }
        Ok(report)
    }

    pub(super) async fn inventory(&self) -> Result<RootHealInventory> {
        #[cfg(any(test, feature = "test-util"))]
        if self.disabled_for_tests {
            return Ok(RootHealInventory::default());
        }
        let _guard = self.mutation.lock().await;
        let disks = self.disks().await?;
        for disk in &disks {
            EcstoreDiskAPI::stat_volume(disk.as_ref(), RUSTFS_META_BUCKET).await?;
        }
        self.inventory_locked(&disks).await
    }

    pub(super) async fn pending(&self) -> Result<Vec<HealRequest>> {
        Ok(self.inventory().await?.requests)
    }
}

impl HealManager {
    pub(super) async fn replay_root_heals(&self) -> Result<()> {
        // Decode every record before admitting anything. These are already
        // accepted responsibilities, so restore distinct IDs even when their
        // paths overlap or the configured admission capacity has changed.
        let pending = self.root_recovery.inventory().await?.requests;
        let mut requests = Vec::with_capacity(pending.len());
        for request in pending {
            if let HealType::Bucket { bucket } = &request.heal_type {
                match self
                    .storage
                    .validate_bucket_incarnation(bucket, request.bucket_incarnation_id)
                    .await
                {
                    Ok(()) => {}
                    Err(error @ Error::StaleBucketIncarnation { .. }) => {
                        let mut terminal = RootHealTerminal::cancelled(&request.id, &request.heal_type, request.options.clone())?;
                        terminal.status = HealTaskStatus::Failed {
                            error: error.to_string(),
                        };
                        self.root_recovery
                            .persist_terminal(&request.id, &request.heal_type, request.source, &terminal.into_completed())
                            .await?;
                        continue;
                    }
                    Err(error) => return Err(error),
                }
            }
            requests.push(request);
        }
        let active = self.active_heals.lock().await;
        let mut queue = self.heal_queue.lock().await;
        let retrying = self.retrying_heals.lock().await;
        for mut request in requests {
            request.force_start = true;
            let existing = active
                .get(&request.id)
                .map(|task| request_matches_task(&request, task))
                .or_else(|| {
                    queue
                        .requests()
                        .find(|queued| queued.id == request.id)
                        .map(|queued| request_matches_request(&request, queued))
                })
                .or_else(|| {
                    retrying
                        .get(&request.id)
                        .map(|retrying| request_matches_request(&request, &retrying.request))
                });
            match existing {
                Some(true) => continue,
                Some(false) => return Err(Error::Other(format!("Conflicting root heal recovery task {}", request.id))),
                None => {}
            }
            queue.push(request);
        }
        publish_heal_queue_length(&queue);
        Ok(())
    }
}
