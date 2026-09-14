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

//! Offline relay export and destination-side receipt for Connect service licenses.

use std::collections::BTreeMap;
use std::fs;
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use super::license::{
    LicenseArtifactError, LicenseClaims, LicenseReport, LicenseVerificationContext, apply_license_bytes,
    read_license_artifact_file, verify_license_bytes,
};
use super::relay::{
    DestinationReceiptSigner, RelayDirection, RelayEnvelope, RelayError, RelayMaterialKind, RelayParty, RelayReceiptOutcome,
    RelayReview, decode_relay_envelope, prepare_approved_artifact, read_protected_relay_artifact,
};

const LEDGER_SCHEMA: &str = "rustfs.connect.serviceLicenseRelayLedger/1";
const LEDGER_FILE: &str = "service-license-relay-ledger.json";
const LOCK_FILE: &str = ".service-license-relay.lock";
const MAX_LEDGER_BYTES: u64 = 4 * 1024 * 1024;
const MAX_LEDGER_ENTRIES: usize = 4096;

#[cfg(unix)]
const STATE_FILE_MODE: u32 = 0o600;

static STAGING_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, thiserror::Error)]
pub enum ServiceLicenseRelayError {
    #[error(transparent)]
    Relay(#[from] RelayError),
    #[error(transparent)]
    License(#[from] LicenseArtifactError),
    #[error("the relay producer does not match the verified license issuer")]
    ProducerMismatch,
    #[error("the relay destination does not match the verified license deployment")]
    DestinationMismatch,
    #[error("the relay transfer identifier is already bound to different material")]
    TransferConflict,
    #[error("the service-license relay state is unavailable")]
    StateUnavailable,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceLicenseRelayExport {
    pub review: RelayReview,
    pub license: LicenseClaims,
}

#[derive(Debug)]
pub struct ServiceLicenseRelayReceipt {
    pub receipt_bytes: Vec<u8>,
    pub outcome: RelayReceiptOutcome,
    pub received_at: String,
    pub license: LicenseClaims,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
struct TransferBinding {
    material_kind: RelayMaterialKind,
    direction: RelayDirection,
    artifact_sha256: String,
    producer: RelayParty,
    destination: RelayParty,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
struct ReplayRecord {
    received_at: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
struct RelayLedger {
    schema: String,
    transfers: BTreeMap<String, TransferBinding>,
    replays: BTreeMap<String, ReplayRecord>,
}

impl Default for RelayLedger {
    fn default() -> Self {
        Self {
            schema: LEDGER_SCHEMA.to_owned(),
            transfers: BTreeMap::new(),
            replays: BTreeMap::new(),
        }
    }
}

pub fn export_service_license_relay(
    artifact_path: &Path,
    envelope_path: &Path,
    transfer_uid: &str,
    state_directory: &Path,
    context: &LicenseVerificationContext,
    confirmed: bool,
) -> Result<ServiceLicenseRelayExport, ServiceLicenseRelayError> {
    let artifact_bytes = read_license_artifact_file(artifact_path)?;
    let report = verify_license_bytes(&artifact_bytes, state_directory, context)?;
    let license = verified_license(report)?;
    let producer = producer(&license);
    let destination = destination(&license);
    let prepared = prepare_approved_artifact(
        transfer_uid,
        RelayMaterialKind::ServiceLicense,
        &artifact_bytes,
        producer,
        destination,
        |_| confirmed,
    )?;
    let envelope_bytes = serde_json::to_vec(&prepared.envelope).map_err(|_| RelayError::EnvelopeEncoding)?;
    write_new_file(envelope_path, &envelope_bytes)?;
    Ok(ServiceLicenseRelayExport {
        review: prepared.review,
        license,
    })
}

pub fn receive_service_license_relay(
    envelope_path: &Path,
    state_directory: &Path,
    context: &LicenseVerificationContext,
    receipt_signer: &DestinationReceiptSigner,
    confirmed: bool,
) -> Result<ServiceLicenseRelayReceipt, ServiceLicenseRelayError> {
    if !confirmed {
        return Err(RelayError::ApprovalRequired.into());
    }
    let envelope_bytes = read_protected_relay_artifact(envelope_path)?;
    let (envelope, artifact_bytes) = decode_relay_envelope(&envelope_bytes)?;
    if envelope.material_kind != RelayMaterialKind::ServiceLicense || envelope.direction != RelayDirection::ConnectToCluster {
        return Err(RelayError::UnsupportedMaterial.into());
    }

    let report = verify_license_bytes(&artifact_bytes, state_directory, context)?;
    let license = verified_license(report)?;
    let verified_producer = producer(&license);
    let verified_destination = destination(&license);
    if envelope.asserted_producer != verified_producer {
        return Err(ServiceLicenseRelayError::ProducerMismatch);
    }
    if envelope.destination != verified_destination {
        return Err(ServiceLicenseRelayError::DestinationMismatch);
    }

    fs::create_dir_all(state_directory).map_err(|_| ServiceLicenseRelayError::StateUnavailable)?;
    let _lock = lock_state(state_directory)?;
    let ledger_path = state_directory.join(LEDGER_FILE);
    let mut ledger = load_ledger(&ledger_path)?;
    let binding = TransferBinding {
        material_kind: envelope.material_kind,
        direction: envelope.direction,
        artifact_sha256: envelope.artifact.sha256.clone(),
        producer: verified_producer.clone(),
        destination: verified_destination,
    };
    if ledger
        .transfers
        .get(&envelope.transfer_uid)
        .is_some_and(|existing| existing != &binding)
    {
        return Err(ServiceLicenseRelayError::TransferConflict);
    }
    if !ledger.transfers.contains_key(&envelope.transfer_uid) {
        if ledger.transfers.len() >= MAX_LEDGER_ENTRIES {
            return Err(ServiceLicenseRelayError::StateUnavailable);
        }
        ledger.transfers.insert(envelope.transfer_uid.clone(), binding);
        persist_ledger(&ledger_path, &ledger)?;
    }

    let replay_id = replay_id(&envelope);
    let (outcome, received_at) = if let Some(existing) = ledger.replays.get(&replay_id) {
        (RelayReceiptOutcome::Duplicate, existing.received_at.clone())
    } else {
        if ledger.replays.len() >= MAX_LEDGER_ENTRIES {
            return Err(ServiceLicenseRelayError::StateUnavailable);
        }
        let applied = apply_license_bytes(&artifact_bytes, state_directory, context)?;
        let received_at = receipt_time(context.now_unix)?;
        let outcome = if applied.idempotent {
            RelayReceiptOutcome::Duplicate
        } else {
            RelayReceiptOutcome::Applied
        };
        ledger.replays.insert(
            replay_id,
            ReplayRecord {
                received_at: received_at.clone(),
            },
        );
        persist_ledger(&ledger_path, &ledger)?;
        (outcome, received_at)
    };
    let receipt_bytes = receipt_signer.sign(&envelope, verified_producer, outcome, received_at.clone())?;
    Ok(ServiceLicenseRelayReceipt {
        receipt_bytes,
        outcome,
        received_at,
        license,
    })
}

fn verified_license(report: LicenseReport) -> Result<LicenseClaims, ServiceLicenseRelayError> {
    report.license.ok_or(ServiceLicenseRelayError::StateUnavailable)
}

fn producer(license: &LicenseClaims) -> RelayParty {
    RelayParty {
        party_type: "CONNECT_LICENSE_ISSUER".to_owned(),
        name: license.issuer.clone(),
        key_id: Some(license.key_id.clone()),
    }
}

fn destination(license: &LicenseClaims) -> RelayParty {
    RelayParty {
        party_type: "CLUSTER".to_owned(),
        name: license.deployment.clone(),
        key_id: None,
    }
}

fn replay_id(envelope: &RelayEnvelope) -> String {
    let mut digest = Sha256::new();
    digest.update(b"SERVICE_LICENSE\0");
    digest.update(envelope.artifact.sha256.as_bytes());
    digest.update([0]);
    digest.update(envelope.destination.party_type.as_bytes());
    digest.update([0]);
    digest.update(envelope.destination.name.as_bytes());
    hex_simd::encode_to_string(digest.finalize(), hex_simd::AsciiCase::Lower)
}

fn receipt_time(now_unix: i64) -> Result<String, ServiceLicenseRelayError> {
    let time = OffsetDateTime::from_unix_timestamp(now_unix).map_err(|_| ServiceLicenseRelayError::StateUnavailable)?;
    time.format(&Rfc3339).map_err(|_| ServiceLicenseRelayError::StateUnavailable)
}

fn load_ledger(path: &Path) -> Result<RelayLedger, ServiceLicenseRelayError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(RelayLedger::default()),
        Err(_) => return Err(ServiceLicenseRelayError::StateUnavailable),
    };
    check_mode(path)?;
    if bytes.len() as u64 > MAX_LEDGER_BYTES {
        return Err(ServiceLicenseRelayError::StateUnavailable);
    }
    let ledger: RelayLedger = serde_json::from_slice(&bytes).map_err(|_| ServiceLicenseRelayError::StateUnavailable)?;
    if ledger.schema != LEDGER_SCHEMA || ledger.transfers.len() > MAX_LEDGER_ENTRIES || ledger.replays.len() > MAX_LEDGER_ENTRIES
    {
        return Err(ServiceLicenseRelayError::StateUnavailable);
    }
    Ok(ledger)
}

fn persist_ledger(path: &Path, ledger: &RelayLedger) -> Result<(), ServiceLicenseRelayError> {
    let bytes = serde_json::to_vec(ledger).map_err(|_| ServiceLicenseRelayError::StateUnavailable)?;
    if bytes.len() as u64 > MAX_LEDGER_BYTES {
        return Err(ServiceLicenseRelayError::StateUnavailable);
    }
    let parent = path.parent().ok_or(ServiceLicenseRelayError::StateUnavailable)?;
    let temporary = temporary_path(path);
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(STATE_FILE_MODE);
    }
    let mut file = options
        .open(&temporary)
        .map_err(|_| ServiceLicenseRelayError::StateUnavailable)?;
    let result = file.write_all(&bytes).and_then(|()| file.sync_all());
    drop(file);
    if result.is_err() || fs::rename(&temporary, path).is_err() || sync_directory(parent).is_err() {
        let _ = fs::remove_file(&temporary);
        return Err(ServiceLicenseRelayError::StateUnavailable);
    }
    Ok(())
}

fn write_new_file(path: &Path, bytes: &[u8]) -> Result<(), ServiceLicenseRelayError> {
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(STATE_FILE_MODE);
    }
    let mut file = options.open(path).map_err(|_| ServiceLicenseRelayError::StateUnavailable)?;
    if file.write_all(bytes).and_then(|()| file.sync_all()).is_err() {
        drop(file);
        let _ = fs::remove_file(path);
        return Err(ServiceLicenseRelayError::StateUnavailable);
    }
    Ok(())
}

fn lock_state(directory: &Path) -> Result<fs::File, ServiceLicenseRelayError> {
    let path = directory.join(LOCK_FILE);
    let mut options = fs::OpenOptions::new();
    options.create(true).truncate(false).read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(STATE_FILE_MODE).custom_flags(libc::O_NOFOLLOW);
    }
    let file = options.open(path).map_err(|_| ServiceLicenseRelayError::StateUnavailable)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        file.set_permissions(fs::Permissions::from_mode(STATE_FILE_MODE))
            .map_err(|_| ServiceLicenseRelayError::StateUnavailable)?;
    }
    file.lock().map_err(|_| ServiceLicenseRelayError::StateUnavailable)?;
    Ok(file)
}

fn temporary_path(path: &Path) -> PathBuf {
    let file_name = path.file_name().and_then(|name| name.to_str()).unwrap_or(LEDGER_FILE);
    path.with_file_name(format!(
        ".{file_name}.{}.{}.tmp",
        std::process::id(),
        STAGING_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ))
}

#[cfg(unix)]
fn check_mode(path: &Path) -> Result<(), ServiceLicenseRelayError> {
    use std::os::unix::fs::PermissionsExt as _;
    let metadata = fs::symlink_metadata(path).map_err(|_| ServiceLicenseRelayError::StateUnavailable)?;
    if !metadata.is_file() || metadata.permissions().mode() & 0o7777 != STATE_FILE_MODE {
        return Err(ServiceLicenseRelayError::StateUnavailable);
    }
    Ok(())
}

#[cfg(not(unix))]
fn check_mode(_path: &Path) -> Result<(), ServiceLicenseRelayError> {
    Ok(())
}

fn sync_directory(directory: &Path) -> io::Result<()> {
    #[cfg(unix)]
    fs::File::open(directory)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = directory;
    Ok(())
}
