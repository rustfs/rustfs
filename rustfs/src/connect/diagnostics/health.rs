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

//! Bounded online HealthCheck observation and signed export.
//!
//! Only the approved L0 cluster aggregate is exported: capacity counters and
//! the existing coarse flag allow-list. Per-drive identity, endpoints, paths,
//! raw admin responses and object data never enter the result.

use std::collections::BTreeSet;
use std::io::{Cursor, Write as _};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use base64_simd::URL_SAFE_NO_PAD;
use chrono::{DateTime, SecondsFormat, Utc};
use p256::ecdsa::{Signature, SigningKey, signature::Signer as _};
use p256::pkcs8::DecodePrivateKey as _;
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::{Uuid, Variant, Version};
use zip::{CompressionMethod, ZipWriter, write::SimpleFileOptions};

use super::ProfileProvenance;
use crate::connect::DeviceIdentity;
use crate::storage::storage_api::contract::admin::{DiskSetSelector, StorageAdminApi};
use crate::storage::storage_api::{DiskInfoOptions, StorageDiskRpcExt};

pub const HEALTH_SCHEMA_VERSION: u16 = 1;
pub const HEALTH_TOOL_ID: &str = "health.check";
pub const HEALTH_SERVICE_CAPABILITY: &str = "health.check.service@1";
pub const HEALTH_TIMEOUT_SECONDS: u64 = 30;
pub const MAX_HEALTH_OUTPUT_BYTES: u64 = 262_144;
pub const MAX_HEALTH_MEMORY_BYTES: u64 = 67_108_864;
pub const MAX_HEALTH_CPU_MILLIS: u64 = 5_000;
pub const MAX_EVIDENCE_AGE_SECONDS: u64 = 86_400;
pub const HEALTH_CATALOG_CHECKS: usize = 13;

const MAX_HEALTH_DRIVES: usize = 1_024;
const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
const MAX_ENVELOPE_BYTES: usize = 16_384;
const SIGNATURE_DOMAIN: &[u8] = b"rustfs-diagnostic-envelope-v1\0";
const ENVELOPE_PATH: &str = "envelope.json";
const SIGNATURE_PATH: &str = "envelope.sig";
const RESULT_PATH: &str = "result.json";
const RESOURCE_ALIAS: &str = "cluster-1";

const ALLOWED_FLAGS: [&str; 8] = [
    "capacity.critical",
    "capacity.warning",
    "clock.skew",
    "cluster.degraded",
    "cluster.healing",
    "cluster.readonly",
    "drive.offline",
    "node.offline",
];

const UNSUPPORTED_CHECKS: [&str; 11] = [
    "memory.reportedPressure",
    "tls.validity",
    "cpu.sufficiency",
    "swap.pressure",
    "drive.health",
    "inode.available",
    "filesystem.compatibility",
    "osKernel.compatibility",
    "launch.permissions",
    "version.consistency",
    "kubernetes.configuration",
];

static HEALTH_COLLECTOR_ACTIVE: AtomicBool = AtomicBool::new(false);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalHealthConsent {
    pub consent_uid: String,
    pub policy_revision: u64,
    pub expires_at_unix: i64,
    pub active: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HealthServiceRequest {
    pub organization_name: String,
    pub cluster_name: String,
    pub device_name: String,
    pub run_uid: String,
    pub artifact_uid: String,
    pub schema_version: u16,
    pub capability: String,
    pub consent: LocalHealthConsent,
    pub produced_at_unix: i64,
    pub expires_at_unix: i64,
    pub nonce: [u8; 32],
    pub max_evidence_age_seconds: u64,
    pub provenance: ProfileProvenance,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HealthSourceObservation {
    pub observed_at_unix: Option<i64>,
    pub capacity_complete: bool,
    pub total_bytes: Option<u64>,
    pub used_bytes: Option<u64>,
    pub coarse_flags_complete: bool,
    pub coarse_flags: Option<Vec<String>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum HealthOutcome {
    Partial,
}

impl HealthOutcome {
    pub const fn as_str(self) -> &'static str {
        "PARTIAL"
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum HealthResultReason {
    CatalogPartial,
    EvidenceIncomplete,
    EvidenceStale,
    EvidenceFreshnessUnknown,
    ClockSkew,
}

impl HealthResultReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CatalogPartial => "CATALOG_PARTIAL",
            Self::EvidenceIncomplete => "EVIDENCE_INCOMPLETE",
            Self::EvidenceStale => "EVIDENCE_STALE",
            Self::EvidenceFreshnessUnknown => "EVIDENCE_FRESHNESS_UNKNOWN",
            Self::ClockSkew => "CLOCK_SKEW",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum HealthRuleOutcome {
    Pass,
    Fail,
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum HealthFreshness {
    Current,
    Stale,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthCheckResult {
    pub check_id: &'static str,
    pub outcome: HealthRuleOutcome,
    pub reason_code: &'static str,
    pub resource_alias: &'static str,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UnsupportedHealthCheck {
    pub check_id: &'static str,
    pub reason_code: &'static str,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthCapacityObservation {
    source: &'static str,
    observed_at: Option<String>,
    freshness: HealthFreshness,
    complete: bool,
    total_bytes: Option<u64>,
    used_bytes: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthFlagsObservation {
    source: &'static str,
    observed_at: Option<String>,
    freshness: HealthFreshness,
    complete: bool,
    values: Option<Vec<String>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthObservations {
    capacity: HealthCapacityObservation,
    coarse_flags: HealthFlagsObservation,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthData {
    catalog_version: u16,
    resource_alias: &'static str,
    observations: HealthObservations,
    checks: Vec<HealthCheckResult>,
    unsupported_checks: Vec<UnsupportedHealthCheck>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthCoverage {
    requested_units: u32,
    completed_units: u32,
    unsupported_units: u32,
    unit: &'static str,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthDiagnosticResult {
    schema_version: u16,
    run_uid: String,
    tool_id: &'static str,
    capability: &'static str,
    outcome: HealthOutcome,
    reason_code: HealthResultReason,
    duration_millis: u64,
    provenance: ProfileProvenance,
    coverage: HealthCoverage,
    data: HealthData,
}

impl HealthDiagnosticResult {
    pub const fn outcome(&self) -> HealthOutcome {
        self.outcome
    }

    pub const fn reason_code(&self) -> HealthResultReason {
        self.reason_code
    }

    pub fn data(&self) -> &HealthData {
        &self.data
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignedHealthExport {
    pub artifact_uid: String,
    pub outcome: HealthOutcome,
    pub reason_code: HealthResultReason,
    pub envelope_json: Vec<u8>,
    pub envelope_signature: Vec<u8>,
    pub result_json: Vec<u8>,
    pub archive_bytes: Vec<u8>,
    pub archive_sha256: String,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum HealthError {
    #[error("health_consent_required")]
    ConsentRequired,
    #[error("health_consent_expired")]
    ConsentExpired,
    #[error("health_request_expired")]
    Expired,
    #[error("health_invalid_request")]
    InvalidRequest,
    #[error("health_unsupported_contract")]
    Unsupported,
    #[error("health_limit_exceeded")]
    LimitExceeded,
    #[error("health_collection_cancelled")]
    Cancelled,
    #[error("health_collection_already_running")]
    Busy,
    #[error("health_source_unavailable")]
    SourceUnavailable,
    #[error("health_collection_failed")]
    CollectionFailed,
    #[error("health_export_signing_failed")]
    Signing,
    #[error("health_export_encoding_failed")]
    Encoding,
}

struct CollectorLease;

impl CollectorLease {
    fn acquire() -> Result<Self, HealthError> {
        HEALTH_COLLECTOR_ACTIVE
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| Self)
            .map_err(|_| HealthError::Busy)
    }
}

impl Drop for CollectorLease {
    fn drop(&mut self) {
        HEALTH_COLLECTOR_ACTIVE.store(false, Ordering::Release);
    }
}

pub async fn collect_runtime_health(
    request: &HealthServiceRequest,
    identity: &DeviceIdentity,
    cancel: &CancellationToken,
) -> Result<SignedHealthExport, HealthError> {
    validate_request(request, Utc::now().timestamp())?;
    check_cancel(cancel)?;
    let _lease = CollectorLease::acquire()?;
    let started = Instant::now();
    let observation = collect_runtime_observation(cancel).await?;
    check_cancel(cancel)?;
    let result = evaluate_health_observation(request, observation, Utc::now(), started.elapsed());
    sign_health_export(request, &result, identity, cancel)
}

pub fn evaluate_health_observation(
    request: &HealthServiceRequest,
    observation: HealthSourceObservation,
    evaluated_at: DateTime<Utc>,
    elapsed: std::time::Duration,
) -> HealthDiagnosticResult {
    let observed_at = observation.observed_at_unix.and_then(DateTime::<Utc>::from_timestamp_secs);
    let freshness = freshness(observed_at, evaluated_at, request.max_evidence_age_seconds);
    let observed_at_text = observed_at.map(|value| value.to_rfc3339_opts(SecondsFormat::Secs, true));

    let capacity = capacity_check(&observation, freshness);
    let flags = flags_check(&observation, freshness);
    let result_reason = result_reason(freshness, &observation);
    let unsupported_checks = UNSUPPORTED_CHECKS
        .into_iter()
        .map(|check_id| UnsupportedHealthCheck {
            check_id,
            reason_code: "EVIDENCE_CONTRACT_UNAVAILABLE",
        })
        .collect();

    HealthDiagnosticResult {
        schema_version: HEALTH_SCHEMA_VERSION,
        run_uid: request.run_uid.clone(),
        tool_id: HEALTH_TOOL_ID,
        capability: HEALTH_SERVICE_CAPABILITY,
        outcome: HealthOutcome::Partial,
        reason_code: result_reason,
        duration_millis: u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
        provenance: request.provenance.clone(),
        coverage: HealthCoverage {
            requested_units: u32::try_from(HEALTH_CATALOG_CHECKS).unwrap_or(u32::MAX),
            completed_units: 2,
            unsupported_units: u32::try_from(UNSUPPORTED_CHECKS.len()).unwrap_or(u32::MAX),
            unit: "CHECK",
        },
        data: HealthData {
            catalog_version: 1,
            resource_alias: RESOURCE_ALIAS,
            observations: HealthObservations {
                capacity: HealthCapacityObservation {
                    source: "LIVE_DISK_PROBE",
                    observed_at: observed_at_text.clone(),
                    freshness,
                    complete: observation.capacity_complete,
                    total_bytes: observation.total_bytes,
                    used_bytes: observation.used_bytes,
                },
                coarse_flags: HealthFlagsObservation {
                    source: "LIVE_DISK_PROBE",
                    observed_at: observed_at_text,
                    freshness,
                    complete: observation.coarse_flags_complete,
                    values: observation.coarse_flags,
                },
            },
            checks: vec![capacity, flags],
            unsupported_checks,
        },
    }
}

async fn collect_runtime_observation(cancel: &CancellationToken) -> Result<HealthSourceObservation, HealthError> {
    let store = crate::runtime_sources::current_object_store_handle().ok_or(HealthError::SourceUnavailable)?;
    let backend = tokio::select! {
        _ = cancel.cancelled() => return Err(HealthError::Cancelled),
        backend = StorageAdminApi::backend_info(store.as_ref()) => backend,
    };
    let drive_counts = StorageAdminApi::set_drive_counts(store.as_ref());
    if backend.total_sets.is_empty()
        || backend.total_sets.len() > MAX_HEALTH_DRIVES
        || backend.total_sets.len() != drive_counts.len()
        || drive_counts != backend.drives_per_set
        || backend
            .total_sets
            .iter()
            .zip(&drive_counts)
            .any(|(&sets, &drives)| sets == 0 || drives == 0)
    {
        return Err(HealthError::SourceUnavailable);
    }
    let data_widths = if backend.standard_sc_data.is_empty() {
        None
    } else if backend.standard_sc_data.len() == drive_counts.len()
        && backend
            .standard_sc_data
            .iter()
            .zip(&drive_counts)
            .all(|(&data, &drives)| (1..=drives).contains(&data))
    {
        Some(backend.standard_sc_data.as_slice())
    } else {
        return Err(HealthError::SourceUnavailable);
    };
    let expected = backend
        .total_sets
        .iter()
        .zip(&drive_counts)
        .try_fold(0usize, |total, (&sets, &drives)| {
            sets.checked_mul(drives).and_then(|count| total.checked_add(count))
        })
        .filter(|count| (1..=MAX_HEALTH_DRIVES).contains(count))
        .ok_or(HealthError::LimitExceeded)?;

    let mut disks = Vec::with_capacity(expected);
    for (pool_idx, (&sets, &drives)) in backend.total_sets.iter().zip(&drive_counts).enumerate() {
        for set_idx in 0..sets {
            let set = tokio::select! {
                _ = cancel.cancelled() => return Err(HealthError::Cancelled),
                result = StorageAdminApi::disk_set_inventory(store.as_ref(), DiskSetSelector::new(pool_idx, set_idx)) => {
                    result.map_err(|_| HealthError::SourceUnavailable)?
                }
            };
            if set.len() != drives || disks.len().saturating_add(set.len()) > expected {
                return Err(HealthError::SourceUnavailable);
            }
            disks.extend(
                set.into_iter()
                    .enumerate()
                    .map(|(disk_idx, disk)| (data_widths.is_none_or(|widths| disk_idx < widths[pool_idx]), disk)),
            );
        }
    }
    if disks.len() != expected {
        return Err(HealthError::SourceUnavailable);
    }

    let observed_at_unix = Utc::now().timestamp();
    let mut total_bytes = 0u64;
    let mut used_bytes = 0u64;
    let mut capacity_complete = true;
    let mut flags_complete = true;
    let mut flags = BTreeSet::new();

    for (include_capacity, disk) in disks {
        check_cancel(cancel)?;
        let Some(disk) = disk else {
            capacity_complete = false;
            flags.insert("cluster.degraded".to_owned());
            flags.insert("drive.offline".to_owned());
            continue;
        };
        let options = DiskInfoOptions {
            fresh_capacity: true,
            ..Default::default()
        };
        let info = tokio::select! {
            _ = cancel.cancelled() => return Err(HealthError::Cancelled),
            result = disk.disk_info(&options) => result,
        };
        let Ok(info) = info else {
            capacity_complete = false;
            flags_complete = false;
            continue;
        };
        if !info.fresh_capacity || info.used > info.total {
            capacity_complete = false;
            flags_complete = false;
            continue;
        }
        match disk.runtime_state().as_str() {
            "online" | "returning" => {}
            "offline" => {
                flags.insert("cluster.degraded".to_owned());
                flags.insert("drive.offline".to_owned());
            }
            _ => {
                flags.insert("cluster.degraded".to_owned());
            }
        }
        if include_capacity {
            match (
                total_bytes.checked_add(info.total).filter(|value| *value <= MAX_SAFE_INTEGER),
                used_bytes.checked_add(info.used).filter(|value| *value <= MAX_SAFE_INTEGER),
            ) {
                (Some(total), Some(used)) => {
                    total_bytes = total;
                    used_bytes = used;
                }
                _ => capacity_complete = false,
            }
        }
        if info.healing {
            flags.insert("cluster.healing".to_owned());
        }
    }

    Ok(HealthSourceObservation {
        observed_at_unix: Some(observed_at_unix),
        capacity_complete,
        total_bytes: capacity_complete.then_some(total_bytes),
        used_bytes: capacity_complete.then_some(used_bytes),
        coarse_flags_complete: flags_complete,
        coarse_flags: flags_complete.then(|| flags.into_iter().collect()),
    })
}

pub fn sign_health_export(
    request: &HealthServiceRequest,
    result: &HealthDiagnosticResult,
    identity: &DeviceIdentity,
    cancel: &CancellationToken,
) -> Result<SignedHealthExport, HealthError> {
    validate_request(request, Utc::now().timestamp())?;
    check_cancel(cancel)?;
    if result.schema_version != HEALTH_SCHEMA_VERSION
        || result.run_uid != request.run_uid
        || result.tool_id != HEALTH_TOOL_ID
        || result.capability != HEALTH_SERVICE_CAPABILITY
    {
        return Err(HealthError::InvalidRequest);
    }
    let result_json = serde_json::to_vec(result).map_err(|_| HealthError::Encoding)?;
    if result_json.is_empty() || u64::try_from(result_json.len()).unwrap_or(u64::MAX) > MAX_HEALTH_OUTPUT_BYTES {
        return Err(HealthError::LimitExceeded);
    }
    let device_key_id = hex_lower(&Sha256::digest(identity.public_key_der()));
    let result_sha256 = hex_lower(&Sha256::digest(&result_json));
    let envelope = HealthEnvelope {
        format_version: "rustfs.connect.diagnosticEnvelope/1",
        protocol_version: "v1",
        organization_name: &request.organization_name,
        cluster_name: &request.cluster_name,
        device_name: &request.device_name,
        run_uid: &request.run_uid,
        artifact_uid: &request.artifact_uid,
        tool_id: HEALTH_TOOL_ID,
        schema_version: HEALTH_SCHEMA_VERSION,
        classification: "L0",
        consent_uid: &request.consent.consent_uid,
        policy_revision: request.consent.policy_revision,
        produced_at: timestamp(request.produced_at_unix)?,
        expires_at: timestamp(request.expires_at_unix)?,
        nonce: URL_SAFE_NO_PAD.encode_to_string(request.nonce),
        device_key_id: &device_key_id,
        payload: HealthPayload {
            path: RESULT_PATH,
            media_type: "application/json",
            size_bytes: u64::try_from(result_json.len()).unwrap_or(u64::MAX),
            sha256: &result_sha256,
        },
    };
    let envelope_json = serde_json::to_vec(&envelope).map_err(|_| HealthError::Encoding)?;
    if envelope_json.is_empty() || envelope_json.len() > MAX_ENVELOPE_BYTES {
        return Err(HealthError::LimitExceeded);
    }
    let envelope_signature = signature_document(identity, &device_key_id, &envelope_json)?;
    check_cancel(cancel)?;
    let archive_bytes = archive(&envelope_json, &envelope_signature, &result_json)?;
    if u64::try_from(archive_bytes.len()).unwrap_or(u64::MAX) > MAX_HEALTH_OUTPUT_BYTES {
        return Err(HealthError::LimitExceeded);
    }
    Ok(SignedHealthExport {
        artifact_uid: request.artifact_uid.clone(),
        outcome: result.outcome,
        reason_code: result.reason_code,
        envelope_json,
        envelope_signature,
        result_json,
        archive_sha256: hex_lower(&Sha256::digest(&archive_bytes)),
        archive_bytes,
    })
}

fn result_reason(freshness: HealthFreshness, observation: &HealthSourceObservation) -> HealthResultReason {
    match freshness {
        HealthFreshness::Stale => HealthResultReason::EvidenceStale,
        HealthFreshness::Unknown if observation.observed_at_unix.is_some() => HealthResultReason::ClockSkew,
        HealthFreshness::Unknown => HealthResultReason::EvidenceFreshnessUnknown,
        HealthFreshness::Current if !observation.capacity_complete || !observation.coarse_flags_complete => {
            HealthResultReason::EvidenceIncomplete
        }
        HealthFreshness::Current => HealthResultReason::CatalogPartial,
    }
}

fn capacity_check(observation: &HealthSourceObservation, freshness: HealthFreshness) -> HealthCheckResult {
    let (outcome, reason_code) = match freshness {
        HealthFreshness::Stale => (HealthRuleOutcome::Unknown, "EVIDENCE_STALE"),
        HealthFreshness::Unknown if observation.observed_at_unix.is_some() => (HealthRuleOutcome::Unknown, "CLOCK_SKEW"),
        HealthFreshness::Unknown => (HealthRuleOutcome::Unknown, "EVIDENCE_FRESHNESS_UNKNOWN"),
        HealthFreshness::Current if !observation.capacity_complete => (HealthRuleOutcome::Unknown, "EVIDENCE_MISSING"),
        HealthFreshness::Current => match (observation.total_bytes, observation.used_bytes) {
            (Some(0), Some(_)) => (HealthRuleOutcome::Unknown, "CAPACITY_UNAVAILABLE"),
            (Some(total), Some(used)) if used > total => (HealthRuleOutcome::Unknown, "INVALID_EVIDENCE"),
            (Some(total), Some(used)) if used == total => (HealthRuleOutcome::Fail, "CAPACITY_EXHAUSTED"),
            (Some(total), Some(used)) if used < total => (HealthRuleOutcome::Pass, "CAPACITY_REMAINING"),
            _ => (HealthRuleOutcome::Unknown, "EVIDENCE_MISSING"),
        },
    };
    HealthCheckResult {
        check_id: "capacity.exhausted",
        outcome,
        reason_code,
        resource_alias: RESOURCE_ALIAS,
    }
}

fn flags_check(observation: &HealthSourceObservation, freshness: HealthFreshness) -> HealthCheckResult {
    let (outcome, reason_code) = match freshness {
        HealthFreshness::Stale => (HealthRuleOutcome::Unknown, "EVIDENCE_STALE"),
        HealthFreshness::Unknown if observation.observed_at_unix.is_some() => (HealthRuleOutcome::Unknown, "CLOCK_SKEW"),
        HealthFreshness::Unknown => (HealthRuleOutcome::Unknown, "EVIDENCE_FRESHNESS_UNKNOWN"),
        HealthFreshness::Current if !observation.coarse_flags_complete => (HealthRuleOutcome::Unknown, "EVIDENCE_MISSING"),
        HealthFreshness::Current => match observation.coarse_flags.as_deref() {
            Some(flags) if flags.iter().any(|flag| !ALLOWED_FLAGS.contains(&flag.as_str())) => {
                (HealthRuleOutcome::Unknown, "INVALID_EVIDENCE")
            }
            Some(flags) if flags.is_empty() => (HealthRuleOutcome::Pass, "NO_COARSE_CONDITION_REPORTED"),
            Some(_) => (HealthRuleOutcome::Fail, "COARSE_CONDITION_REPORTED"),
            None => (HealthRuleOutcome::Unknown, "EVIDENCE_MISSING"),
        },
    };
    HealthCheckResult {
        check_id: "cluster.reportedFlags",
        outcome,
        reason_code,
        resource_alias: RESOURCE_ALIAS,
    }
}

fn freshness(observed_at: Option<DateTime<Utc>>, evaluated_at: DateTime<Utc>, max_age: u64) -> HealthFreshness {
    let Some(observed_at) = observed_at else {
        return HealthFreshness::Unknown;
    };
    let Ok(age) = evaluated_at.signed_duration_since(observed_at).to_std() else {
        return HealthFreshness::Unknown;
    };
    if age.as_secs() > max_age {
        HealthFreshness::Stale
    } else {
        HealthFreshness::Current
    }
}

fn validate_request(request: &HealthServiceRequest, now: i64) -> Result<(), HealthError> {
    if request.schema_version != HEALTH_SCHEMA_VERSION || request.capability != HEALTH_SERVICE_CAPABILITY {
        return Err(HealthError::Unsupported);
    }
    if !request.consent.active || request.consent.policy_revision == 0 {
        return Err(HealthError::ConsentRequired);
    }
    if request.consent.expires_at_unix <= now || request.expires_at_unix > request.consent.expires_at_unix {
        return Err(HealthError::ConsentExpired);
    }
    if request.produced_at_unix > now.saturating_add(300)
        || request.expires_at_unix <= now
        || request.expires_at_unix <= request.produced_at_unix
    {
        return Err(HealthError::Expired);
    }
    if !(1..=MAX_EVIDENCE_AGE_SECONDS).contains(&request.max_evidence_age_seconds)
        || !uuid7(&request.run_uid)
        || !uuid7(&request.artifact_uid)
        || !uuid7(&request.consent.consent_uid)
        || !resource_names_match(request)
        || !request.provenance.is_valid()
    {
        return Err(HealthError::InvalidRequest);
    }
    Ok(())
}

fn check_cancel(cancel: &CancellationToken) -> Result<(), HealthError> {
    if cancel.is_cancelled() {
        Err(HealthError::Cancelled)
    } else {
        Ok(())
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct HealthEnvelope<'a> {
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
    payload: HealthPayload<'a>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct HealthPayload<'a> {
    path: &'static str,
    media_type: &'static str,
    size_bytes: u64,
    sha256: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct HealthSignature<'a> {
    algorithm: &'static str,
    key_id: &'a str,
    value: String,
}

fn signature_document(identity: &DeviceIdentity, key_id: &str, envelope: &[u8]) -> Result<Vec<u8>, HealthError> {
    let pkcs8 = identity.to_pkcs8_der().map_err(|_| HealthError::Signing)?;
    let signing_key = SigningKey::from_pkcs8_der(pkcs8.as_slice()).map_err(|_| HealthError::Signing)?;
    let mut input = Vec::with_capacity(SIGNATURE_DOMAIN.len() + envelope.len());
    input.extend_from_slice(SIGNATURE_DOMAIN);
    input.extend_from_slice(envelope);
    let signature: Signature = signing_key.sign(&input);
    serde_json::to_vec(&HealthSignature {
        algorithm: "ES256",
        key_id,
        value: URL_SAFE_NO_PAD.encode_to_string(signature.normalize_s().to_bytes()),
    })
    .map_err(|_| HealthError::Encoding)
}

fn archive(envelope: &[u8], signature: &[u8], result: &[u8]) -> Result<Vec<u8>, HealthError> {
    let cursor = Cursor::new(Vec::with_capacity(envelope.len() + signature.len() + result.len() + 512));
    let mut writer = ZipWriter::new(cursor);
    let options = SimpleFileOptions::DEFAULT
        .compression_method(CompressionMethod::Stored)
        .unix_permissions(0o600);
    for (name, bytes) in [(ENVELOPE_PATH, envelope), (SIGNATURE_PATH, signature), (RESULT_PATH, result)] {
        writer.start_file(name, options).map_err(|_| HealthError::Encoding)?;
        writer.write_all(bytes).map_err(|_| HealthError::Encoding)?;
    }
    writer
        .finish()
        .map(|cursor| cursor.into_inner())
        .map_err(|_| HealthError::Encoding)
}

fn timestamp(value: i64) -> Result<String, HealthError> {
    DateTime::<Utc>::from_timestamp(value, 0)
        .map(|value| value.to_rfc3339_opts(SecondsFormat::Secs, true))
        .ok_or(HealthError::InvalidRequest)
}

fn resource_names_match(request: &HealthServiceRequest) -> bool {
    let Some(organization_uid) = request.organization_name.strip_prefix("organizations/") else {
        return false;
    };
    if !uuid7(organization_uid) {
        return false;
    }
    let cluster_prefix = format!("{}/clusters/", request.organization_name);
    let Some(cluster_uid) = request.cluster_name.strip_prefix(&cluster_prefix) else {
        return false;
    };
    if !uuid7(cluster_uid) {
        return false;
    }
    let device_prefix = format!("{}/clusterDevices/", request.cluster_name);
    request.device_name.strip_prefix(&device_prefix).is_some_and(uuid7)
}

fn uuid7(value: &str) -> bool {
    Uuid::parse_str(value).is_ok_and(|uuid| {
        uuid.get_version() == Some(Version::SortRand) && uuid.get_variant() == Variant::RFC4122 && uuid.to_string() == value
    })
}

fn hex_lower(bytes: &[u8]) -> String {
    bytes.iter().fold(String::with_capacity(bytes.len() * 2), |mut value, byte| {
        use std::fmt::Write as _;
        let _ = write!(value, "{byte:02x}");
        value
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collector_lease_limits_health_jobs_to_one() {
        let active = CollectorLease::acquire().expect("first health collector");
        assert!(matches!(CollectorLease::acquire(), Err(HealthError::Busy)));
        drop(active);
        CollectorLease::acquire().expect("collector lease should be released");
    }
}
