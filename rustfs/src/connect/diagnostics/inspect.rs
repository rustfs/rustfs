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

//! Local-only, bounded object integrity analysis for `inspect.object@1`.
//!
//! Object names, versions, metadata, shards, hashes, paths, and reconstructed
//! bytes stay inside this module. The signed artifact contains only the closed
//! rule conclusions defined by the Connect Inspect contract.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Cursor, Read as _, Write as _};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64_simd::URL_SAFE_NO_PAD;
use p256::ecdsa::{Signature, SigningKey, signature::Signer as _};
use p256::pkcs8::DecodePrivateKey as _;
use rustfs_filemeta::{FileInfo, FileMeta};
use rustfs_utils::HashAlgorithm;
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio_util::sync::CancellationToken;
use uuid::{Uuid, Variant, Version};
use zip::{CompressionMethod, ZipWriter, write::SimpleFileOptions};

use crate::connect::DeviceIdentity;
use crate::storage_api::inspect::{BitrotReader, Erasure, check_valid_bucket_name_strict, file_info_quorum_hash};

pub const INSPECT_SCHEMA_VERSION: u16 = 1;
pub const INSPECT_CAPABILITY: &str = "inspect.object@1";
pub const MAX_INSPECT_DURATION: Duration = Duration::from_secs(30);
pub const MAX_LOCAL_READ_BYTES: u64 = 256 * 1024 * 1024;
pub const MAX_WORKING_MEMORY_BYTES: u64 = 64 * 1024 * 1024;
const MAX_XL_META_BYTES: u64 = 4 * 1024 * 1024;
const MAX_FINDINGS: usize = 3;
const MAX_RESULT_BYTES: usize = 64 * 1024;
const MAX_ENVELOPE_BYTES: usize = 16 * 1024;
const MAX_ARCHIVE_BYTES: usize = 96 * 1024;
const MAX_VALIDITY_SECONDS: i64 = 30 * 24 * 60 * 60;
const MAX_FUTURE_SKEW_SECONDS: i64 = 300;
const OUTPUT_MODE: u32 = 0o600;
const SIGNATURE_DOMAIN: &[u8] = b"rustfs-diagnostic-envelope-v1\0";
const RESULT_PATH: &str = "result.json";
const ENVELOPE_PATH: &str = "envelope.json";
const SIGNATURE_PATH: &str = "envelope.sig";
static INSPECT_ACTIVE: AtomicBool = AtomicBool::new(false);

#[derive(Clone, Debug)]
pub struct InspectRequest {
    pub organization_name: String,
    pub cluster_name: String,
    pub device_name: String,
    pub run_uid: String,
    pub artifact_uid: String,
    pub schema_version: u16,
    pub capability: String,
    pub consent: InspectArtifactConsent,
    pub produced_at_unix: i64,
    pub expires_at_unix: i64,
    pub nonce: [u8; 32],
    pub drive_roots: Vec<PathBuf>,
    pub bucket: String,
    pub object: String,
    pub version_id: Option<String>,
    pub rules: Vec<InspectRule>,
    pub max_duration: Duration,
    pub max_read_bytes: u64,
    pub max_memory_bytes: u64,
    pub provenance: InspectProvenance,
}

#[derive(Clone, Debug)]
pub struct InspectArtifactConsent {
    pub consent_uid: String,
    pub policy_revision: u64,
    pub expires_at_unix: i64,
    pub confirmed: bool,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InspectProvenance {
    repository: &'static str,
    source_commit: String,
    executable_sha256: String,
    rustfs_version: String,
    os_family: OsFamily,
    architecture: Architecture,
    build_features: Vec<String>,
}

impl InspectProvenance {
    pub fn new(source_commit: String, executable_sha256: String, rustfs_version: String, build_features: Vec<String>) -> Self {
        Self {
            repository: "rustfs/rustfs",
            source_commit,
            executable_sha256,
            rustfs_version,
            os_family: OsFamily::current(),
            architecture: Architecture::current(),
            build_features,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum InspectRule {
    ShardBitrot,
    ShardAvailability,
    MetadataIdentity,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum InspectOutcome {
    Succeeded,
    Partial,
    Failed,
    Unsupported,
    Cancelled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum InspectReasonCode {
    Complete,
    LimitExceeded,
    SourceUnavailable,
    PermissionDenied,
    UnsupportedVersion,
    Cancelled,
    CollectionFailed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum InspectRuleOutcome {
    Pass,
    Fail,
    Indeterminate,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum InspectReason {
    Verified,
    CorruptShard,
    MissingShard,
    IdentityMismatch,
    InvalidMetadata,
    NoUsableMetadata,
    UnsupportedFormat,
    ReadDenied,
    LimitExceeded,
    Cancelled,
    SourceChanged,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Reconstruction {
    Possible,
    Impossible,
    Unknown,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InspectFinding {
    resource_alias: &'static str,
    rule_id: RuleId,
    outcome: InspectRuleOutcome,
    reason: InspectReason,
    #[serde(skip_serializing_if = "Option::is_none")]
    missing_shard_count: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    corrupt_shard_count: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata_match: Option<bool>,
    reconstruction: Reconstruction,
}

impl InspectFinding {
    pub fn rule(&self) -> InspectRule {
        self.rule_id.into()
    }

    pub fn outcome(&self) -> InspectRuleOutcome {
        self.outcome
    }

    pub fn reason(&self) -> InspectReason {
        self.reason
    }

    pub fn missing_shard_count(&self) -> Option<u16> {
        self.missing_shard_count
    }

    pub fn corrupt_shard_count(&self) -> Option<u16> {
        self.corrupt_shard_count
    }

    pub fn metadata_match(&self) -> Option<bool> {
        self.metadata_match
    }

    pub fn reconstruction(&self) -> Reconstruction {
        self.reconstruction
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InspectDiagnosticResult {
    schema_version: u16,
    run_uid: String,
    tool_id: &'static str,
    capability: &'static str,
    outcome: InspectOutcome,
    reason_code: InspectReasonCode,
    duration_millis: u64,
    provenance: InspectProvenance,
    coverage: Coverage,
    data: Option<InspectData>,
}

impl InspectDiagnosticResult {
    pub fn outcome(&self) -> InspectOutcome {
        self.outcome
    }

    pub fn reason_code(&self) -> InspectReasonCode {
        self.reason_code
    }

    pub fn findings(&self) -> &[InspectFinding] {
        self.data.as_ref().map_or(&[], |data| data.findings.as_slice())
    }
}

#[derive(Clone, Debug)]
pub enum InspectRun {
    Signed(SignedInspectExport),
    Terminal(InspectDiagnosticResult),
}

#[derive(Clone, Debug)]
pub struct SignedInspectExport {
    pub artifact_uid: String,
    pub envelope_json: Vec<u8>,
    pub envelope_signature: Vec<u8>,
    pub result_json: Vec<u8>,
    pub archive_bytes: Vec<u8>,
    pub archive_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SavedInspectExport {
    pub artifact_uid: String,
    pub archive_size_bytes: u64,
    pub archive_sha256: String,
}

#[derive(Debug, Error)]
pub enum InspectError {
    #[error("inspect_invalid_request")]
    InvalidRequest,
    #[error("inspect_consent_required")]
    ConsentRequired,
    #[error("inspect_consent_expired")]
    ConsentExpired,
    #[error("inspect_expired")]
    Expired,
    #[error("inspect_unsupported_version")]
    UnsupportedVersion,
    #[error("inspect_unsupported_capability")]
    UnsupportedCapability,
    #[error("inspect_limit_exceeded")]
    LimitExceeded,
    #[error("inspect_cancelled")]
    Cancelled,
    #[error("inspect_already_running")]
    Busy,
    #[error("inspect_signing_failed")]
    Signing,
    #[error("inspect_export_exists")]
    AlreadyExists,
    #[error("inspect_io_failed")]
    Io(#[source] std::io::Error),
    #[error("inspect_encoding_failed")]
    Encoding,
    #[error("inspect_durability_failed_after_commit")]
    DurabilityAfterCommit(#[source] std::io::Error),
}

pub fn export_inspect_summary(
    request: &InspectRequest,
    key: &DeviceIdentity,
    cancel: &CancellationToken,
) -> Result<InspectRun, InspectError> {
    request.validate(unix_now()?)?;
    let _lease = CollectorLease::acquire()?;
    let started = Instant::now();
    let result = analyze(request, cancel, started)?;
    if !matches!(result.outcome, InspectOutcome::Succeeded | InspectOutcome::Partial) {
        return Ok(InspectRun::Terminal(result));
    }
    Ok(InspectRun::Signed(sign_result(request, result, key, cancel)?))
}

fn analyze(
    request: &InspectRequest,
    cancel: &CancellationToken,
    started: Instant,
) -> Result<InspectDiagnosticResult, InspectError> {
    if cancel.is_cancelled() {
        return Ok(terminal(request, InspectOutcome::Cancelled, InspectReasonCode::Cancelled, started));
    }
    let mut budget = ReadBudget::new(request.max_read_bytes);
    let mut readable = Vec::new();
    let mut retained_bytes = 0u64;
    let mut saw_permission_denied = false;
    let mut saw_unsupported = false;
    let mut saw_invalid = false;

    for root in &request.drive_roots {
        if cancel.is_cancelled() {
            return Ok(terminal(request, InspectOutcome::Cancelled, InspectReasonCode::Cancelled, started));
        }
        if started.elapsed() >= request.max_duration {
            return Ok(terminal(request, InspectOutcome::Failed, InspectReasonCode::LimitExceeded, started));
        }
        let memory_remaining = request.max_memory_bytes.saturating_sub(retained_bytes);
        match read_drive(root, request, &mut budget, memory_remaining, cancel, started) {
            Ok(shard) => {
                let Some(next_retained_bytes) = retained_bytes
                    .checked_add(shard.retained_bytes())
                    .filter(|used| *used <= request.max_memory_bytes)
                else {
                    return Ok(terminal(request, InspectOutcome::Failed, InspectReasonCode::LimitExceeded, started));
                };
                retained_bytes = next_retained_bytes;
                readable.push(shard);
            }
            Err(ReadFailure::Missing) => {}
            Err(ReadFailure::PermissionDenied) => saw_permission_denied = true,
            Err(ReadFailure::Unsupported) => saw_unsupported = true,
            Err(ReadFailure::Invalid) => saw_invalid = true,
            Err(ReadFailure::Limit) => {
                return Ok(terminal(request, InspectOutcome::Failed, InspectReasonCode::LimitExceeded, started));
            }
            Err(ReadFailure::Changed) => {
                return Ok(terminal(request, InspectOutcome::Failed, InspectReasonCode::SourceUnavailable, started));
            }
            Err(ReadFailure::Cancelled) => {
                return Ok(terminal(request, InspectOutcome::Cancelled, InspectReasonCode::Cancelled, started));
            }
        }
    }

    if readable.is_empty() {
        if saw_permission_denied {
            return Ok(terminal(request, InspectOutcome::Failed, InspectReasonCode::PermissionDenied, started));
        }
        if saw_unsupported {
            return Ok(terminal(
                request,
                InspectOutcome::Unsupported,
                InspectReasonCode::UnsupportedVersion,
                started,
            ));
        }
        let findings = request
            .rules
            .iter()
            .copied()
            .map(|rule| {
                indeterminate(
                    rule,
                    if saw_invalid {
                        InspectReason::InvalidMetadata
                    } else {
                        InspectReason::NoUsableMetadata
                    },
                )
            })
            .collect();
        return Ok(success_result(request, findings, started));
    }

    let incomplete_reason = if saw_permission_denied {
        Some(InspectReason::ReadDenied)
    } else if saw_unsupported {
        Some(InspectReason::UnsupportedFormat)
    } else if saw_invalid {
        Some(InspectReason::InvalidMetadata)
    } else {
        None
    };
    let findings = match evaluate(request, readable, retained_bytes, incomplete_reason, cancel, started) {
        Ok(findings) => findings,
        Err(InspectError::LimitExceeded) => {
            return Ok(terminal(request, InspectOutcome::Failed, InspectReasonCode::LimitExceeded, started));
        }
        Err(InspectError::Cancelled) => {
            return Ok(terminal(request, InspectOutcome::Cancelled, InspectReasonCode::Cancelled, started));
        }
        Err(error) => return Err(error),
    };
    Ok(success_result(request, findings, started))
}

fn evaluate(
    request: &InspectRequest,
    shards: Vec<DriveShard>,
    retained_bytes: u64,
    incomplete_reason: Option<InspectReason>,
    cancel: &CancellationToken,
    started: Instant,
) -> Result<Vec<InspectFinding>, InspectError> {
    let mut groups: BTreeMap<[u8; 32], Vec<DriveShard>> = BTreeMap::new();
    for shard in shards {
        groups.entry(file_info_quorum_hash(&shard.file_info)).or_default().push(shard);
    }
    let metadata_mismatch = groups.len() > 1;
    let mut selected = groups
        .into_values()
        .max_by_key(Vec::len)
        .ok_or(InspectError::InvalidRequest)?;
    selected.sort_by_key(|shard| shard.index);
    selected.dedup_by_key(|shard| shard.index);
    let file_info = selected.first().ok_or(InspectError::InvalidRequest)?.file_info.clone();
    let total = file_info
        .erasure
        .data_blocks
        .checked_add(file_info.erasure.parity_blocks)
        .ok_or(InspectError::LimitExceeded)?;
    let total = u16::try_from(total).map_err(|_| InspectError::LimitExceeded)?;
    if total == 0 || total > 256 {
        return Err(InspectError::LimitExceeded);
    }
    let indices = selected.iter().map(|shard| shard.index).collect::<BTreeSet<_>>();
    let corrupt = selected
        .iter()
        .filter(|shard| matches!(shard.state, ShardState::Corrupt))
        .count();
    let locally_missing = selected
        .iter()
        .filter(|shard| matches!(shard.state, ShardState::Missing))
        .count();
    let verified = selected
        .iter()
        .filter(|shard| matches!(shard.state, ShardState::Verified(_)))
        .count();
    let absent = usize::from(total).saturating_sub(indices.len());
    let missing = absent.saturating_add(locally_missing);
    let reconstruction = reconstruct(
        &file_info,
        &selected,
        retained_bytes,
        request.max_memory_bytes,
        request.max_duration,
        cancel,
        started,
    )?;
    let missing = u16::try_from(missing).map_err(|_| InspectError::LimitExceeded)?;
    let corrupt = u16::try_from(corrupt).map_err(|_| InspectError::LimitExceeded)?;
    let mut findings = Vec::with_capacity(request.rules.len());
    for rule in &request.rules {
        findings.push(match rule {
            InspectRule::ShardBitrot if corrupt > 0 => finding(
                *rule,
                InspectRuleOutcome::Fail,
                InspectReason::CorruptShard,
                Some(missing),
                Some(corrupt),
                None,
                Reconstruction::Unknown,
            ),
            InspectRule::ShardBitrot if let Some(reason) = incomplete_reason => indeterminate(*rule, reason),
            InspectRule::ShardBitrot => finding(
                *rule,
                InspectRuleOutcome::Pass,
                InspectReason::Verified,
                Some(missing),
                Some(0),
                None,
                Reconstruction::Unknown,
            ),
            InspectRule::ShardAvailability if let Some(reason) = incomplete_reason => indeterminate(*rule, reason),
            InspectRule::ShardAvailability if missing > 0 => finding(
                *rule,
                InspectRuleOutcome::Fail,
                InspectReason::MissingShard,
                Some(missing),
                Some(corrupt),
                None,
                reconstruction,
            ),
            InspectRule::ShardAvailability if verified == usize::from(total) => finding(
                *rule,
                InspectRuleOutcome::Pass,
                InspectReason::Verified,
                Some(0),
                Some(0),
                None,
                reconstruction,
            ),
            InspectRule::ShardAvailability => indeterminate(*rule, InspectReason::NoUsableMetadata),
            InspectRule::MetadataIdentity if metadata_mismatch => finding(
                *rule,
                InspectRuleOutcome::Fail,
                InspectReason::IdentityMismatch,
                None,
                None,
                Some(false),
                Reconstruction::Unknown,
            ),
            InspectRule::MetadataIdentity if let Some(reason) = incomplete_reason => indeterminate(*rule, reason),
            InspectRule::MetadataIdentity => finding(
                *rule,
                InspectRuleOutcome::Pass,
                InspectReason::Verified,
                None,
                None,
                Some(true),
                Reconstruction::Unknown,
            ),
        });
    }
    Ok(findings)
}

fn reconstruct(
    file_info: &FileInfo,
    shards: &[DriveShard],
    retained_bytes: u64,
    memory_limit: u64,
    max_duration: Duration,
    cancel: &CancellationToken,
    started: Instant,
) -> Result<Reconstruction, InspectError> {
    check_bounds(cancel, started, max_duration)?;
    let k = file_info.erasure.data_blocks;
    let m = file_info.erasure.parity_blocks;
    let verified = shards
        .iter()
        .filter(|shard| matches!(shard.state, ShardState::Verified(_)))
        .count();
    if verified < k {
        return Ok(Reconstruction::Impossible);
    }
    let size = usize::try_from(file_info.size).map_err(|_| InspectError::InvalidRequest)?;
    let erasure = Erasure::try_new_with_options(k, m, file_info.erasure.block_size, file_info.uses_legacy_checksum)
        .map_err(|_| InspectError::InvalidRequest)?;
    let total = k.checked_add(m).ok_or(InspectError::LimitExceeded)?;
    let shard_total = erasure.shard_file_offset(0, size, size);
    let mut object_offset = 0usize;
    let mut shard_offset = 0usize;
    while object_offset < size {
        check_bounds(cancel, started, max_duration)?;
        let block_len = (size - object_offset).min(erasure.block_size);
        let block_shard_len = if object_offset + block_len >= size {
            shard_total.checked_sub(shard_offset).ok_or(InspectError::InvalidRequest)?
        } else {
            erasure.shard_size()
        };
        let verified_slots = shards
            .iter()
            .filter(|shard| matches!(shard.state, ShardState::Verified(_)))
            .count();
        let slot_bytes = total
            .checked_mul(std::mem::size_of::<Option<Vec<u8>>>())
            .and_then(|overhead| {
                block_shard_len
                    .checked_mul(verified_slots)
                    .and_then(|data| overhead.checked_add(data))
            })
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or(InspectError::LimitExceeded)?;
        if retained_bytes.checked_add(slot_bytes).is_none_or(|peak| peak > memory_limit) {
            return Err(InspectError::LimitExceeded);
        }
        let mut slots = vec![None; total];
        for shard in shards {
            let ShardState::Verified(bytes) = &shard.state else { continue };
            let end = shard_offset.checked_add(block_shard_len).ok_or(InspectError::LimitExceeded)?;
            if shard.index > 0 && shard.index <= total && end <= bytes.len() {
                slots[shard.index - 1] = Some(bytes[shard_offset..end].to_vec());
            }
        }
        erasure.decode_data(&mut slots).map_err(|_| InspectError::InvalidRequest)?;
        for slot in slots.iter().take(k) {
            if slot.is_none() {
                return Ok(Reconstruction::Impossible);
            }
        }
        object_offset = object_offset.checked_add(block_len).ok_or(InspectError::LimitExceeded)?;
        shard_offset = shard_offset.checked_add(block_shard_len).ok_or(InspectError::LimitExceeded)?;
    }
    Ok(Reconstruction::Possible)
}

fn read_drive(
    root: &Path,
    request: &InspectRequest,
    budget: &mut ReadBudget,
    memory_remaining: u64,
    cancel: &CancellationToken,
    started: Instant,
) -> Result<DriveShard, ReadFailure> {
    check_read_bounds(cancel, started, request.max_duration)?;
    let object_dir = object_dir(root, &request.bucket, &request.object)?;
    let xl_path = object_dir.join("xl.meta");
    let before = fs::metadata(&xl_path).map_err(map_read_error)?;
    if before.len() > MAX_XL_META_BYTES {
        return Err(ReadFailure::Limit);
    }
    let bytes = budget.read(&xl_path, MAX_XL_META_BYTES)?;
    if bytes.len() >= 6 && &bytes[..4] == b"XL2 " && u16::from_le_bytes([bytes[4], bytes[5]]) > 1 {
        return Err(ReadFailure::Unsupported);
    }
    let metadata = FileMeta::load(&bytes).map_err(|_| ReadFailure::Invalid)?;
    let version = request.version_id.as_deref().unwrap_or_default();
    let mut file_info = metadata
        .into_fileinfo(&request.bucket, &request.object, version, true, false, true)
        .map_err(|_| ReadFailure::Invalid)?;
    file_info.validate_for_metadata_read().map_err(|_| ReadFailure::Invalid)?;
    if file_info.parts.len() != 1 || file_info.parts.first().is_none_or(|part| part.number != 1) {
        return Err(ReadFailure::Unsupported);
    }
    let index = file_info.erasure.index;
    let state =
        match read_verified_shard(&object_dir, &file_info, budget, memory_remaining, request.max_duration, cancel, started) {
            Ok(bytes) => ShardState::Verified(bytes),
            Err(ReadFailure::Missing) => ShardState::Missing,
            Err(ReadFailure::Invalid) => ShardState::Corrupt,
            Err(error) => return Err(error),
        };
    file_info.data = None;
    let after = fs::metadata(&xl_path).map_err(map_read_error)?;
    if before.len() != after.len() || before.modified().ok() != after.modified().ok() {
        return Err(ReadFailure::Changed);
    }
    Ok(DriveShard { index, file_info, state })
}

fn read_verified_shard(
    object_dir: &Path,
    file_info: &FileInfo,
    budget: &mut ReadBudget,
    memory_limit: u64,
    max_duration: Duration,
    cancel: &CancellationToken,
    started: Instant,
) -> Result<Vec<u8>, ReadFailure> {
    check_read_bounds(cancel, started, max_duration)?;
    let checksum = file_info.erasure.get_checksum_info(1);
    let algorithm = if file_info.uses_legacy_checksum && checksum.algorithm == HashAlgorithm::HighwayHash256S {
        HashAlgorithm::HighwayHash256SLegacy
    } else {
        checksum.algorithm
    };
    let erasure = Erasure::try_new_with_options(
        file_info.erasure.data_blocks,
        file_info.erasure.parity_blocks,
        file_info.erasure.block_size,
        file_info.uses_legacy_checksum,
    )
    .map_err(|_| ReadFailure::Invalid)?;
    let size = usize::try_from(file_info.size).map_err(|_| ReadFailure::Invalid)?;
    let expected = erasure.shard_file_offset(0, size, size);
    let source = if let Some(inline) = file_info.data.as_ref() {
        let inline_bytes = u64::try_from(inline.len()).map_err(|_| ReadFailure::Limit)?;
        if inline_bytes.checked_mul(2).is_none_or(|peak| peak > memory_limit) {
            return Err(ReadFailure::Limit);
        }
        budget.charge(inline_bytes)?;
        inline.to_vec()
    } else {
        let data_dir = file_info.data_dir.ok_or(ReadFailure::Invalid)?;
        budget.read(&object_dir.join(data_dir.to_string()).join("part.1"), memory_limit)?
    };
    let streaming = matches!(algorithm, HashAlgorithm::HighwayHash256S | HashAlgorithm::HighwayHash256SLegacy);
    if !streaming {
        if checksum.hash.len() != algorithm.size() || algorithm.hash_encode(&source).as_ref() != checksum.hash.as_ref() {
            return Err(ReadFailure::Invalid);
        }
        return Ok(source);
    }
    let transient_bytes = u64::try_from(source.len())
        .ok()
        .and_then(|source_len| source_len.checked_add(u64::try_from(expected).ok()?))
        .ok_or(ReadFailure::Limit)?;
    let inline_bytes = file_info
        .data
        .as_ref()
        .map_or(0, |data| u64::try_from(data.len()).unwrap_or(u64::MAX));
    if transient_bytes
        .checked_add(inline_bytes)
        .is_none_or(|peak| peak > memory_limit)
    {
        return Err(ReadFailure::Limit);
    }
    let blocks = if expected == 0 {
        0
    } else {
        expected.div_ceil(erasure.shard_size().max(1))
    };
    let framed = expected
        .checked_add(blocks.checked_mul(algorithm.size()).ok_or(ReadFailure::Limit)?)
        .ok_or(ReadFailure::Limit)?;
    let (expected, blocks) = if framed == source.len() {
        (expected, blocks)
    } else if source.len() >= algorithm.size() && blocks <= 1 {
        (source.len() - algorithm.size(), 1)
    } else {
        return Err(ReadFailure::Invalid);
    };
    let frame_size = if blocks <= 1 {
        expected.max(1)
    } else {
        erasure.shard_size().max(1)
    };
    let mut reader = BitrotReader::new(Cursor::new(source), frame_size, algorithm, false);
    let mut output = vec![0; expected];
    let mut offset = 0usize;
    for _ in 0..blocks {
        check_read_bounds(cancel, started, max_duration)?;
        let wanted = (expected - offset).min(frame_size);
        let read =
            futures::executor::block_on(reader.read(&mut output[offset..offset + wanted])).map_err(|_| ReadFailure::Invalid)?;
        offset = offset.checked_add(read).ok_or(ReadFailure::Limit)?;
    }
    if offset != expected {
        return Err(ReadFailure::Invalid);
    }
    Ok(output)
}

fn object_dir(root: &Path, bucket: &str, object: &str) -> Result<PathBuf, ReadFailure> {
    if check_valid_bucket_name_strict(bucket).is_err()
        || object.is_empty()
        || object.len() > 1_024
        || Path::new(object).is_absolute()
        || Path::new(object)
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(ReadFailure::Invalid);
    }
    let canonical = root.canonicalize().map_err(map_read_error)?;
    let path = canonical.join(bucket).join(object);
    let parent = path.parent().ok_or(ReadFailure::Invalid)?;
    let resolved = parent.canonicalize().map_err(map_read_error)?;
    if !resolved.starts_with(&canonical) {
        return Err(ReadFailure::Invalid);
    }
    Ok(path)
}

struct DriveShard {
    index: usize,
    file_info: FileInfo,
    state: ShardState,
}

impl DriveShard {
    fn retained_bytes(&self) -> u64 {
        match &self.state {
            ShardState::Verified(bytes) => u64::try_from(bytes.len()).unwrap_or(u64::MAX),
            ShardState::Missing | ShardState::Corrupt => 0,
        }
    }
}

enum ShardState {
    Verified(Vec<u8>),
    Missing,
    Corrupt,
}

struct ReadBudget {
    remaining: u64,
}

impl ReadBudget {
    fn new(limit: u64) -> Self {
        Self { remaining: limit }
    }

    fn charge(&mut self, amount: u64) -> Result<(), ReadFailure> {
        self.remaining = self.remaining.checked_sub(amount).ok_or(ReadFailure::Limit)?;
        Ok(())
    }

    fn read(&mut self, path: &Path, ceiling: u64) -> Result<Vec<u8>, ReadFailure> {
        let before = fs::metadata(path).map_err(map_read_error)?;
        if before.len() > ceiling {
            return Err(ReadFailure::Limit);
        }
        self.charge(before.len())?;
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.custom_flags(libc::O_NOFOLLOW);
        }
        let file = options.open(path).map_err(map_read_error)?;
        let capacity = usize::try_from(before.len()).map_err(|_| ReadFailure::Limit)?;
        let mut bytes = Vec::with_capacity(capacity);
        file.take(ceiling.saturating_add(1))
            .read_to_end(&mut bytes)
            .map_err(map_read_error)?;
        let after = fs::metadata(path).map_err(map_read_error)?;
        if u64::try_from(bytes.len()).map_err(|_| ReadFailure::Limit)? != before.len()
            || before.len() != after.len()
            || before.modified().ok() != after.modified().ok()
        {
            return Err(ReadFailure::Changed);
        }
        Ok(bytes)
    }
}

#[derive(Clone, Copy)]
enum ReadFailure {
    Missing,
    PermissionDenied,
    Unsupported,
    Invalid,
    Limit,
    Changed,
    Cancelled,
}

fn map_read_error(error: std::io::Error) -> ReadFailure {
    match error.kind() {
        std::io::ErrorKind::NotFound => ReadFailure::Missing,
        std::io::ErrorKind::PermissionDenied => ReadFailure::PermissionDenied,
        _ => ReadFailure::Invalid,
    }
}

fn check_read_cancel(cancel: &CancellationToken) -> Result<(), ReadFailure> {
    if cancel.is_cancelled() {
        Err(ReadFailure::Cancelled)
    } else {
        Ok(())
    }
}

fn check_read_bounds(cancel: &CancellationToken, started: Instant, max_duration: Duration) -> Result<(), ReadFailure> {
    check_read_cancel(cancel)?;
    if started.elapsed() >= max_duration {
        Err(ReadFailure::Limit)
    } else {
        Ok(())
    }
}

fn check_bounds(cancel: &CancellationToken, started: Instant, max_duration: Duration) -> Result<(), InspectError> {
    check_cancel(cancel)?;
    if started.elapsed() >= max_duration {
        Err(InspectError::LimitExceeded)
    } else {
        Ok(())
    }
}

fn success_result(request: &InspectRequest, findings: Vec<InspectFinding>, started: Instant) -> InspectDiagnosticResult {
    InspectDiagnosticResult {
        schema_version: INSPECT_SCHEMA_VERSION,
        run_uid: request.run_uid.clone(),
        tool_id: "inspect.object",
        capability: INSPECT_CAPABILITY,
        outcome: InspectOutcome::Succeeded,
        reason_code: InspectReasonCode::Complete,
        duration_millis: duration_millis(started.elapsed()),
        provenance: request.provenance.clone(),
        coverage: Coverage {
            requested_units: u32::try_from(request.rules.len()).unwrap_or(0),
            completed_units: u32::try_from(findings.len()).unwrap_or(0),
            unit: "CHECK",
        },
        data: Some(InspectData {
            scope: "LOCAL_OBJECT_SUMMARY",
            findings,
        }),
    }
}

fn terminal(
    request: &InspectRequest,
    outcome: InspectOutcome,
    reason_code: InspectReasonCode,
    started: Instant,
) -> InspectDiagnosticResult {
    InspectDiagnosticResult {
        schema_version: INSPECT_SCHEMA_VERSION,
        run_uid: request.run_uid.clone(),
        tool_id: "inspect.object",
        capability: INSPECT_CAPABILITY,
        outcome,
        reason_code,
        duration_millis: duration_millis(started.elapsed()),
        provenance: request.provenance.clone(),
        coverage: Coverage {
            requested_units: u32::try_from(request.rules.len()).unwrap_or(0),
            completed_units: 0,
            unit: "CHECK",
        },
        data: None,
    }
}

fn finding(
    rule: InspectRule,
    outcome: InspectRuleOutcome,
    reason: InspectReason,
    missing: Option<u16>,
    corrupt: Option<u16>,
    metadata_match: Option<bool>,
    reconstruction: Reconstruction,
) -> InspectFinding {
    InspectFinding {
        resource_alias: "object-1",
        rule_id: rule.into(),
        outcome,
        reason,
        missing_shard_count: missing,
        corrupt_shard_count: corrupt,
        metadata_match,
        reconstruction,
    }
}

fn indeterminate(rule: InspectRule, reason: InspectReason) -> InspectFinding {
    finding(rule, InspectRuleOutcome::Indeterminate, reason, None, None, None, Reconstruction::Unknown)
}

fn sign_result(
    request: &InspectRequest,
    result: InspectDiagnosticResult,
    key: &DeviceIdentity,
    cancel: &CancellationToken,
) -> Result<SignedInspectExport, InspectError> {
    check_cancel(cancel)?;
    if unix_now()? >= request.expires_at_unix {
        return Err(InspectError::Expired);
    }
    let result_json = serde_json::to_vec(&result).map_err(|_| InspectError::Encoding)?;
    if result_json.is_empty() || result_json.len() > MAX_RESULT_BYTES {
        return Err(InspectError::LimitExceeded);
    }
    let device_key_id = hex_lower(&Sha256::digest(key.public_key_der()));
    let envelope = Envelope {
        format_version: "rustfs.connect.diagnosticEnvelope/1",
        protocol_version: "v1",
        organization_name: &request.organization_name,
        cluster_name: &request.cluster_name,
        device_name: &request.device_name,
        run_uid: &request.run_uid,
        artifact_uid: &request.artifact_uid,
        tool_id: "inspect.object",
        schema_version: INSPECT_SCHEMA_VERSION,
        classification: "L3",
        consent_uid: &request.consent.consent_uid,
        policy_revision: request.consent.policy_revision,
        produced_at: timestamp(request.produced_at_unix)?,
        expires_at: timestamp(request.expires_at_unix)?,
        nonce: URL_SAFE_NO_PAD.encode_to_string(request.nonce),
        device_key_id: &device_key_id,
        payload: Payload {
            path: RESULT_PATH,
            media_type: "application/json",
            size_bytes: u64::try_from(result_json.len()).map_err(|_| InspectError::LimitExceeded)?,
            sha256: hex_lower(&Sha256::digest(&result_json)),
        },
    };
    let envelope_json = serde_json::to_vec(&envelope).map_err(|_| InspectError::Encoding)?;
    if envelope_json.is_empty() || envelope_json.len() > MAX_ENVELOPE_BYTES {
        return Err(InspectError::LimitExceeded);
    }
    let envelope_signature = signature_document(key, &device_key_id, &envelope_json)?;
    check_cancel(cancel)?;
    let archive_bytes = archive(&envelope_json, &envelope_signature, &result_json)?;
    if archive_bytes.len() > MAX_ARCHIVE_BYTES {
        return Err(InspectError::LimitExceeded);
    }
    Ok(SignedInspectExport {
        artifact_uid: request.artifact_uid.clone(),
        envelope_json,
        envelope_signature,
        result_json,
        archive_sha256: hex_lower(&Sha256::digest(&archive_bytes)),
        archive_bytes,
    })
}

pub fn save_signed_inspect_export(
    output: &Path,
    export: &SignedInspectExport,
    cancel: &CancellationToken,
) -> Result<SavedInspectExport, InspectError> {
    check_cancel(cancel)?;
    if !uuid7(&export.artifact_uid) {
        return Err(InspectError::InvalidRequest);
    }
    let parent = output
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let filename = output.file_name().ok_or(InspectError::InvalidRequest)?.to_string_lossy();
    let temporary = parent.join(format!(".{filename}.{}.partial", export.artifact_uid));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(OUTPUT_MODE);
    }
    let mut file = options.open(&temporary).map_err(map_create_error)?;
    let saved = (|| {
        file.write_all(&export.archive_bytes).map_err(InspectError::Io)?;
        check_cancel(cancel)?;
        file.sync_all().map_err(InspectError::Io)?;
        check_cancel(cancel)?;
        fs::hard_link(&temporary, output).map_err(map_publish_error)?;
        fs::remove_file(&temporary).map_err(InspectError::DurabilityAfterCommit)?;
        #[cfg(unix)]
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(InspectError::DurabilityAfterCommit)?;
        Ok(SavedInspectExport {
            artifact_uid: export.artifact_uid.clone(),
            archive_size_bytes: u64::try_from(export.archive_bytes.len()).map_err(|_| InspectError::LimitExceeded)?,
            archive_sha256: export.archive_sha256.clone(),
        })
    })();
    if saved.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    saved
}

impl InspectRequest {
    fn validate(&self, now: i64) -> Result<(), InspectError> {
        if self.schema_version != INSPECT_SCHEMA_VERSION {
            return Err(InspectError::UnsupportedVersion);
        }
        if self.capability != INSPECT_CAPABILITY {
            return Err(InspectError::UnsupportedCapability);
        }
        if !self.consent.confirmed || self.consent.policy_revision == 0 {
            return Err(InspectError::ConsentRequired);
        }
        if self.consent.expires_at_unix <= now || self.expires_at_unix > self.consent.expires_at_unix {
            return Err(InspectError::ConsentExpired);
        }
        let validity = self
            .expires_at_unix
            .checked_sub(self.produced_at_unix)
            .ok_or(InspectError::Expired)?;
        if self.produced_at_unix > now.saturating_add(MAX_FUTURE_SKEW_SECONDS)
            || validity <= 0
            || validity > MAX_VALIDITY_SECONDS
            || self.expires_at_unix <= now
        {
            return Err(InspectError::Expired);
        }
        if self.drive_roots.is_empty()
            || self.drive_roots.len() > 256
            || self.rules.is_empty()
            || self.rules.len() > MAX_FINDINGS
            || self.rules.iter().copied().collect::<BTreeSet<_>>().len() != self.rules.len()
            || self.max_duration.is_zero()
            || self.max_duration > MAX_INSPECT_DURATION
            || self.max_read_bytes == 0
            || self.max_read_bytes > MAX_LOCAL_READ_BYTES
            || self.max_memory_bytes == 0
            || self.max_memory_bytes > MAX_WORKING_MEMORY_BYTES
        {
            return Err(InspectError::LimitExceeded);
        }
        if check_valid_bucket_name_strict(&self.bucket).is_err()
            || self.object.is_empty()
            || self.object.len() > 1_024
            || Path::new(&self.object).is_absolute()
            || Path::new(&self.object)
                .components()
                .any(|component| !matches!(component, Component::Normal(_)))
            || self
                .version_id
                .as_deref()
                .is_some_and(|value| Uuid::parse_str(value).is_err())
        {
            return Err(InspectError::InvalidRequest);
        }
        if !uuid7(&self.run_uid)
            || !uuid7(&self.artifact_uid)
            || !uuid7(&self.consent.consent_uid)
            || !resource_names_match(self)
            || !lower_hex(&self.provenance.source_commit, 40)
            || !lower_hex(&self.provenance.executable_sha256, 64)
            || !version(&self.provenance.rustfs_version)
            || self.provenance.build_features.len() > 64
            || !self.provenance.build_features.iter().all(|feature| build_feature(feature))
        {
            return Err(InspectError::InvalidRequest);
        }
        Ok(())
    }
}

struct CollectorLease;

impl CollectorLease {
    fn acquire() -> Result<Self, InspectError> {
        INSPECT_ACTIVE
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| Self)
            .map_err(|_| InspectError::Busy)
    }
}

impl Drop for CollectorLease {
    fn drop(&mut self) {
        INSPECT_ACTIVE.store(false, Ordering::Release);
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Coverage {
    requested_units: u32,
    completed_units: u32,
    unit: &'static str,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct InspectData {
    scope: &'static str,
    findings: Vec<InspectFinding>,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum RuleId {
    ShardBitrot,
    ShardAvailability,
    MetadataIdentity,
}

impl From<InspectRule> for RuleId {
    fn from(value: InspectRule) -> Self {
        match value {
            InspectRule::ShardBitrot => Self::ShardBitrot,
            InspectRule::ShardAvailability => Self::ShardAvailability,
            InspectRule::MetadataIdentity => Self::MetadataIdentity,
        }
    }
}

impl From<RuleId> for InspectRule {
    fn from(value: RuleId) -> Self {
        match value {
            RuleId::ShardBitrot => Self::ShardBitrot,
            RuleId::ShardAvailability => Self::ShardAvailability,
            RuleId::MetadataIdentity => Self::MetadataIdentity,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum OsFamily {
    Linux,
    Darwin,
    Windows,
    Freebsd,
    Other,
}

impl OsFamily {
    const fn current() -> Self {
        if cfg!(target_os = "linux") {
            Self::Linux
        } else if cfg!(target_os = "macos") {
            Self::Darwin
        } else if cfg!(target_os = "windows") {
            Self::Windows
        } else if cfg!(target_os = "freebsd") {
            Self::Freebsd
        } else {
            Self::Other
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
enum Architecture {
    #[serde(rename = "x86_64")]
    X86_64,
    #[serde(rename = "aarch64")]
    Aarch64,
    #[serde(rename = "other")]
    Other,
}

impl Architecture {
    const fn current() -> Self {
        if cfg!(target_arch = "x86_64") {
            Self::X86_64
        } else if cfg!(target_arch = "aarch64") {
            Self::Aarch64
        } else {
            Self::Other
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Envelope<'a> {
    format_version: &'static str,
    protocol_version: &'static str,
    organization_name: &'a str,
    cluster_name: &'a str,
    device_name: &'a str,
    run_uid: &'a str,
    artifact_uid: &'a str,
    tool_id: &'static str,
    schema_version: u16,
    classification: &'static str,
    consent_uid: &'a str,
    policy_revision: u64,
    produced_at: String,
    expires_at: String,
    nonce: String,
    device_key_id: &'a str,
    payload: Payload,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Payload {
    path: &'static str,
    media_type: &'static str,
    size_bytes: u64,
    sha256: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SignatureDocument<'a> {
    algorithm: &'static str,
    key_id: &'a str,
    value: String,
}

fn signature_document(key: &DeviceIdentity, key_id: &str, envelope: &[u8]) -> Result<Vec<u8>, InspectError> {
    let pkcs8 = key.to_pkcs8_der().map_err(|_| InspectError::Signing)?;
    let signing_key = SigningKey::from_pkcs8_der(pkcs8.as_slice()).map_err(|_| InspectError::Signing)?;
    let mut input = Vec::with_capacity(SIGNATURE_DOMAIN.len() + envelope.len());
    input.extend_from_slice(SIGNATURE_DOMAIN);
    input.extend_from_slice(envelope);
    let signature: Signature = signing_key.sign(&input);
    serde_json::to_vec(&SignatureDocument {
        algorithm: "ES256",
        key_id,
        value: URL_SAFE_NO_PAD.encode_to_string(signature.normalize_s().to_bytes()),
    })
    .map_err(|_| InspectError::Encoding)
}

fn archive(envelope: &[u8], signature: &[u8], result: &[u8]) -> Result<Vec<u8>, InspectError> {
    let cursor = Cursor::new(Vec::with_capacity(envelope.len() + signature.len() + result.len() + 512));
    let mut writer = ZipWriter::new(cursor);
    let options = SimpleFileOptions::DEFAULT
        .compression_method(CompressionMethod::Stored)
        .unix_permissions(OUTPUT_MODE);
    for (name, bytes) in [(ENVELOPE_PATH, envelope), (SIGNATURE_PATH, signature), (RESULT_PATH, result)] {
        writer.start_file(name, options).map_err(|_| InspectError::Encoding)?;
        writer.write_all(bytes).map_err(InspectError::Io)?;
    }
    writer
        .finish()
        .map(|cursor| cursor.into_inner())
        .map_err(|_| InspectError::Encoding)
}

fn map_create_error(error: std::io::Error) -> InspectError {
    if error.kind() == std::io::ErrorKind::AlreadyExists {
        InspectError::AlreadyExists
    } else {
        InspectError::Io(error)
    }
}

fn map_publish_error(error: std::io::Error) -> InspectError {
    if error.kind() == std::io::ErrorKind::AlreadyExists {
        InspectError::AlreadyExists
    } else {
        InspectError::Io(error)
    }
}

fn resource_names_match(request: &InspectRequest) -> bool {
    let Some(organization_uid) = request.organization_name.strip_prefix("organizations/") else {
        return false;
    };
    let cluster_prefix = format!("{}/clusters/", request.organization_name);
    let Some(cluster_uid) = request.cluster_name.strip_prefix(&cluster_prefix) else { return false };
    let device_prefix = format!("{}/clusterDevices/", request.cluster_name);
    let Some(device_uid) = request.device_name.strip_prefix(&device_prefix) else { return false };
    uuid7(organization_uid) && uuid7(cluster_uid) && uuid7(device_uid)
}

fn uuid7(value: &str) -> bool {
    Uuid::parse_str(value).is_ok_and(|uuid| {
        uuid.get_version() == Some(Version::SortRand) && uuid.get_variant() == Variant::RFC4122 && uuid.to_string() == value
    })
}

fn lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn version(value: &str) -> bool {
    if value.is_empty()
        || value.len() > 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-'))
    {
        return false;
    }
    let (core, suffix) = value
        .split_once('-')
        .map_or((value, None), |(core, suffix)| (core, Some(suffix)));
    if suffix.is_some_and(str::is_empty) {
        return false;
    }
    let mut parts = core.split('.');
    parts.clone().count() == 3 && parts.all(|part| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit()))
}

fn build_feature(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value.as_bytes()[0].is_ascii_lowercase()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-'))
}

fn timestamp(unix: i64) -> Result<String, InspectError> {
    OffsetDateTime::from_unix_timestamp(unix)
        .map_err(|_| InspectError::InvalidRequest)?
        .format(&Rfc3339)
        .map_err(|_| InspectError::InvalidRequest)
}

fn unix_now() -> Result<i64, InspectError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| InspectError::InvalidRequest)?;
    i64::try_from(duration.as_secs()).map_err(|_| InspectError::InvalidRequest)
}

fn check_cancel(cancel: &CancellationToken) -> Result<(), InspectError> {
    if cancel.is_cancelled() {
        Err(InspectError::Cancelled)
    } else {
        Ok(())
    }
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX).min(30_000)
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut value, "{byte:02x}").expect("writing hexadecimal to a string cannot fail");
    }
    value
}
