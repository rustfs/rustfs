// Copyright 2024 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::{IntegrityError, Result};
use crate::object_api::ObjectInfo;
use rustfs_rio::{Checksum, read_checksums};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashSet};
use uuid::Uuid;

pub const MAX_ITEMS: usize = 64;
pub const MAX_OBJECT_BYTES: u64 = 5 * 1024 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ItemRequest {
    pub key: String,
    pub version_id: Option<String>,
    /// Base64 SHA-256 supplied by the administrator from an independent source.
    pub expected_sha256: Option<String>,
    pub target_key: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobMode {
    Audit,
    Migrate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobRequest {
    pub mode: JobMode,
    pub items: Vec<ItemRequest>,
    /// Bounds each source read, destination PUT, and verification stream; one worker runs per cluster.
    pub bytes_per_second: u64,
    pub max_object_bytes: u64,
}

impl JobRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.items.is_empty() || self.items.len() > MAX_ITEMS {
            return Err(IntegrityError::Invalid("a job requires 1..=64 explicit source versions"));
        }
        if !(64 * 1024..=1024 * 1024 * 1024).contains(&self.bytes_per_second)
            || self.max_object_bytes == 0
            || self.max_object_bytes > MAX_OBJECT_BYTES
        {
            return Err(IntegrityError::Invalid("invalid byte rate or staging limit"));
        }
        let mut targets = HashSet::new();
        let sources: HashSet<_> = self.items.iter().map(|i| i.key.as_str()).collect();
        for item in &self.items {
            validate_key(&item.key)?;
            if let Some(version) = &item.version_id
                && version != "null"
                && Uuid::parse_str(version).ok().is_none_or(|id| id.is_nil())
            {
                return Err(IntegrityError::Invalid("invalid source version"));
            }
            if let Some(value) = &item.expected_sha256 {
                sha256_bytes(value)?;
            }
            match (self.mode, &item.target_key) {
                (JobMode::Audit, None) => {}
                (JobMode::Migrate, Some(target)) => {
                    validate_key(target)?;
                    if sources.contains(target.as_str()) || !targets.insert(target) {
                        return Err(IntegrityError::Invalid("targets must be distinct from every source and target"));
                    }
                }
                _ => return Err(IntegrityError::Invalid("only migrations require an explicit target key")),
            }
        }
        Ok(())
    }
}

fn validate_key(key: &str) -> Result<()> {
    if key.is_empty() || key.len() > 1024 || key.ends_with('/') || key.contains('\0') {
        return Err(IntegrityError::Invalid("invalid object key"));
    }
    Ok(())
}

pub(crate) fn sha256_bytes(value: &str) -> Result<Vec<u8>> {
    let raw = base64_simd::STANDARD
        .decode_to_vec(value)
        .map_err(|_| IntegrityError::Invalid("SHA256 must be canonical Base64"))?;
    if raw.len() != 32 || base64_simd::STANDARD.encode_to_string(&raw) != value {
        return Err(IntegrityError::Invalid("SHA256 must be canonical Base64 of 32 bytes"));
    }
    Ok(raw)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Protection {
    Unknown,
    Legacy,
    IndependentCommitment,
    InvalidDeclaration,
    NoPayload,
}

pub(crate) fn protection(info: &ObjectInfo) -> Protection {
    if info.delete_marker {
        return Protection::NoPayload;
    }
    match rustfs_filemeta::shard_integrity::descriptor_from_metadata(&info.user_defined) {
        Ok(None) if info.parts.iter().all(|p| p.integrity.is_none()) => Protection::Legacy,
        Ok(Some(parts))
            if parts.len() == info.parts.len()
                && !parts.is_empty()
                && parts
                    .iter()
                    .zip(info.parts.iter())
                    .all(|(declared, part)| part.integrity.as_ref() == Some(declared)) =>
        {
            Protection::IndependentCommitment
        }
        _ => Protection::InvalidDeclaration,
    }
}

/// Checksums shown in inventory are not automatically accepted as migration evidence.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InventoryItem {
    pub key: String,
    /// S3 selector, including the literal "null" for the null version slot.
    pub version_id: String,
    pub data_dir: Option<Uuid>,
    pub size: i64,
    pub protection: Protection,
    pub checksums: BTreeMap<String, String>,
    pub checksum_is_multipart: bool,
    pub audit_unsupported: Option<String>,
    pub source_fingerprint: String,
    pub observation_error: Option<String>,
}

impl InventoryItem {
    pub(crate) fn from_info(info: &ObjectInfo) -> Self {
        let (checksums, checksum_is_multipart) = info.checksum.as_ref().map_or_else(Default::default, |b| read_checksums(b, 0));
        Self {
            key: info.name.clone(),
            version_id: info
                .version_id
                .filter(|id| !id.is_nil())
                .map_or_else(|| "null".into(), |id| id.to_string()),
            data_dir: info.data_dir,
            size: info.size,
            protection: protection(info),
            checksums: checksums.into_iter().collect(),
            checksum_is_multipart,
            audit_unsupported: unsupported(info).map(str::to_owned),
            source_fingerprint: fingerprint(info),
            observation_error: None,
        }
    }
}

pub(crate) fn unsupported(info: &ObjectInfo) -> Option<&'static str> {
    if info.delete_marker {
        Some("delete_marker")
    } else if !info.transitioned_object.status.is_empty() {
        Some("transitioned_object")
    } else if info.is_encrypted() || info.is_compressed() {
        Some("transformed_object")
    } else if info.is_multipart() {
        Some("multipart_object")
    } else if protection(info) == Protection::InvalidDeclaration {
        Some("invalid_protection_declaration")
    } else {
        None
    }
}

pub(crate) fn stored_sha256(info: &ObjectInfo) -> Option<String> {
    let bytes = info.checksum.as_ref()?;
    let (sums, multipart) = read_checksums(bytes, 0);
    let value = sums.get("SHA256")?;
    let canonical = Checksum::new_from_string("SHA256", value)?.to_bytes(&[]);
    // The general display decoder tolerates truncated suffixes. Eligibility does not.
    (!multipart && canonical.as_ref() == bytes.as_ref()).then(|| value.clone())
}

pub(crate) fn fingerprint(info: &ObjectInfo) -> String {
    let mut hash = Sha256::new();
    // Ordered metadata includes transformation and protection declarations as well as user metadata.
    for value in [
        info.bucket.clone(),
        info.name.clone(),
        format!("{:?}", info.version_id),
        format!("{:?}", info.data_dir),
        format!("{:?}", info.mod_time),
        info.size.to_string(),
        info.actual_size.to_string(),
        format!("{:?}", info.etag),
        format!("{:?}", info.checksum),
        format!("{:?}", info.transitioned_object),
        info.user_tags.to_string(),
        format!("{:?}", info.expires),
        format!("{:?}", info.content_type),
        format!("{:?}", info.content_encoding),
        format!("{:?}", info.storage_class),
    ] {
        hash.update(value.len().to_le_bytes());
        hash.update(value.as_bytes());
    }
    for part in info.parts.iter() {
        let values = format!(
            "{}:{}:{}:{:?}:{:?}:{:?}:{:?}",
            part.number, part.size, part.actual_size, part.etag, part.mod_time, part.index, part.integrity
        );
        hash.update(values.len().to_le_bytes());
        hash.update(values.as_bytes());
        if let Some(checksums) = &part.checksums {
            let mut checksums: Vec<_> = checksums.iter().collect();
            checksums.sort_unstable_by_key(|(key, _)| *key);
            for (key, value) in checksums {
                hash.update(key.len().to_le_bytes());
                hash.update(key.as_bytes());
                hash.update(value.len().to_le_bytes());
                hash.update(value.as_bytes());
            }
        }
    }
    let mut metadata: Vec<_> = info.user_defined.iter().collect();
    metadata.sort_unstable_by_key(|(key, _)| *key);
    for (key, value) in metadata {
        hash.update(key.len().to_le_bytes());
        hash.update(key.as_bytes());
        hash.update(value.len().to_le_bytes());
        hash.update(value.as_bytes());
    }
    hex_simd::encode_to_string(hash.finalize(), hex_simd::AsciiCase::Lower)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Paused,
    Running,
    PauseRequested,
    CancelRequested,
    Cancelled,
    Complete,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ItemState {
    Pending,
    Prepared,
    Verified,
    Migrated,
    Unsupported,
    Unavailable,
    Mismatch,
    Stale,
    Conflict,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ItemResult {
    pub state: ItemState,
    pub source_fingerprint: Option<String>,
    pub expected_sha256: Option<String>,
    pub evidence_source: Option<String>,
    pub source_size: Option<u64>,
    pub target_version_id: Option<Uuid>,
    pub detail: Option<String>,
}

impl Default for ItemResult {
    fn default() -> Self {
        Self {
            state: ItemState::Pending,
            source_fingerprint: None,
            expected_sha256: None,
            evidence_source: None,
            source_size: None,
            target_version_id: None,
            detail: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Job {
    pub format_version: u32,
    pub id: Uuid,
    pub bucket: String,
    pub bucket_incarnation: Uuid,
    pub request: JobRequest,
    pub state: JobState,
    pub revision: u64,
    pub results: Vec<ItemResult>,
    pub last_error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct InventoryPage {
    pub observed_at: String,
    pub bucket_incarnation: Uuid,
    pub items: Vec<InventoryItem>,
    pub is_truncated: bool,
    pub next_key_marker: Option<String>,
    pub next_version_marker: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct Readiness {
    pub local_reader_supported: bool,
    pub write_requested: bool,
    pub fleet_operator_attested: bool,
    pub new_writes_enabled_here: bool,
    pub fleet_capability_verified: bool,
    pub unverified_requirements: [&'static str; 3],
}

pub fn readiness() -> Readiness {
    let write = rustfs_utils::get_env_bool(rustfs_config::ENV_SHARD_INTEGRITY_WRITE, false);
    let fleet = rustfs_utils::get_env_bool(rustfs_config::ENV_SHARD_INTEGRITY_FLEET_CONFIRMED, false);
    Readiness {
        local_reader_supported: true,
        write_requested: write,
        fleet_operator_attested: fleet,
        new_writes_enabled_here: write && fleet,
        fleet_capability_verified: false,
        unverified_requirements: [
            "peer_reader_writer_and_repair_capabilities",
            "old_process_storage_admission",
            "deployment_failure_and_performance_qualification",
        ],
    }
}
