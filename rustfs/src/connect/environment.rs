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

//! Bounded, identifier-free deployment environment inventory.

use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Cursor, Write as _};
use std::path::Path;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use base64_simd::URL_SAFE_NO_PAD;
use p256::ecdsa::{Signature, SigningKey, signature::Signer as _};
use p256::pkcs8::DecodePrivateKey as _;
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use sysinfo::{Disks, Networks, RefreshKind, System};
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use uuid::{Uuid, Variant, Version};
use zip::{CompressionMethod, ZipWriter, write::SimpleFileOptions};

use super::{DeviceIdentity, inventory::InventorySnapshot};

pub const ENVIRONMENT_CAPABILITY: &str = "inventory.environment@1";
pub const ENVIRONMENT_SCHEMA_VERSION: u16 = 1;
pub const MAX_ENVIRONMENT_DURATION: Duration = Duration::from_secs(30);

static SYSTEM_SCAN_PERMIT: LazyLock<Arc<Semaphore>> = LazyLock::new(|| Arc::new(Semaphore::new(1)));

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EnvironmentCollectionRequest {
    timeout: Duration,
}

impl EnvironmentCollectionRequest {
    pub fn negotiate(schema_version: u16, capability: &str, timeout: Duration) -> Result<Self, EnvironmentError> {
        if schema_version != ENVIRONMENT_SCHEMA_VERSION {
            return Err(EnvironmentError::UnsupportedVersion);
        }
        if capability != ENVIRONMENT_CAPABILITY {
            return Err(EnvironmentError::UnsupportedCapability);
        }
        if timeout.is_zero() || timeout > MAX_ENVIRONMENT_DURATION {
            return Err(EnvironmentError::InvalidTimeout);
        }
        Ok(Self { timeout })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum EnvironmentOsFamily {
    Linux,
    Darwin,
    Windows,
    Freebsd,
    Other,
}

impl EnvironmentOsFamily {
    fn current() -> Self {
        match std::env::consts::OS {
            "linux" => Self::Linux,
            "macos" => Self::Darwin,
            "windows" => Self::Windows,
            "freebsd" => Self::Freebsd,
            _ => Self::Other,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum EnvironmentFilesystemType {
    Ext4,
    Xfs,
    Zfs,
    Apfs,
    Other,
}

impl EnvironmentFilesystemType {
    fn from_reported(value: &str) -> Self {
        match value.to_ascii_lowercase().as_str() {
            "ext4" => Self::Ext4,
            "xfs" => Self::Xfs,
            "zfs" => Self::Zfs,
            "apfs" => Self::Apfs,
            _ => Self::Other,
        }
    }
}

fn filesystem_types<'a>(reported: impl IntoIterator<Item = &'a str>) -> Result<Vec<EnvironmentFilesystemType>, EnvironmentError> {
    let values = reported
        .into_iter()
        .map(EnvironmentFilesystemType::from_reported)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if values.is_empty() {
        return Err(EnvironmentError::SourceUnavailable(EnvironmentSource::Filesystem));
    }
    Ok(values)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EnvironmentInventory {
    node_count: u16,
    drive_count: u32,
    os_family: EnvironmentOsFamily,
    filesystem_types: Vec<EnvironmentFilesystemType>,
}

impl EnvironmentInventory {
    pub fn node_count(&self) -> u16 {
        self.node_count
    }

    pub fn drive_count(&self) -> u32 {
        self.drive_count
    }

    pub fn os_family(&self) -> EnvironmentOsFamily {
        self.os_family
    }

    pub fn filesystem_types(&self) -> &[EnvironmentFilesystemType] {
        &self.filesystem_types
    }
}

const SIGNATURE_DOMAIN: &[u8] = b"rustfs-diagnostic-envelope-v1\0";
const OUTPUT_MODE: u32 = 0o600;

#[derive(Clone, Debug)]
pub struct EnvironmentExportRequest {
    pub confirmed: bool,
    pub organization_name: String,
    pub cluster_name: String,
    pub device_name: String,
    pub run_uid: String,
    pub artifact_uid: String,
    pub consent_uid: String,
    pub policy_revision: u64,
    pub produced_at_unix: i64,
    pub expires_at_unix: i64,
    pub nonce: [u8; 32],
    pub source_commit: String,
    pub executable_sha256: String,
    pub rustfs_version: String,
    pub build_features: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct SignedEnvironmentExport {
    pub artifact_uid: String,
    pub archive_bytes: Vec<u8>,
    pub archive_sha256: String,
}

#[derive(Clone, Debug)]
pub struct SavedEnvironmentExport {
    pub artifact_uid: String,
    pub archive_size_bytes: u64,
    pub archive_sha256: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EnvironmentResult<'a> {
    schema_version: u16,
    run_uid: &'a str,
    tool_id: &'static str,
    capability: &'static str,
    outcome: &'static str,
    reason_code: &'static str,
    duration_millis: u64,
    provenance: EnvironmentProvenance<'a>,
    coverage: EnvironmentCoverage,
    data: &'a EnvironmentInventory,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EnvironmentProvenance<'a> {
    repository: &'static str,
    source_commit: &'a str,
    executable_sha256: &'a str,
    rustfs_version: &'a str,
    os_family: EnvironmentOsFamily,
    architecture: &'static str,
    build_features: &'a [String],
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EnvironmentCoverage {
    requested_units: u8,
    completed_units: u8,
    unit: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EnvironmentEnvelope<'a> {
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
    payload: EnvironmentPayload,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EnvironmentPayload {
    path: &'static str,
    media_type: &'static str,
    size_bytes: u64,
    sha256: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EnvironmentSignature<'a> {
    algorithm: &'static str,
    key_id: &'a str,
    value: String,
}

pub fn sign_environment_inventory(
    inventory: &EnvironmentInventory,
    request: &EnvironmentExportRequest,
    key: &DeviceIdentity,
    duration: Duration,
    cancel: &CancellationToken,
) -> Result<SignedEnvironmentExport, EnvironmentError> {
    if cancel.is_cancelled() {
        return Err(EnvironmentError::Cancelled);
    }
    let now = OffsetDateTime::now_utc().unix_timestamp();
    if !request.confirmed
        || duration.is_zero()
        || duration > MAX_ENVIRONMENT_DURATION
        || request.policy_revision == 0
        || request.produced_at_unix > now.saturating_add(300)
        || request.expires_at_unix <= now
        || request.expires_at_unix <= request.produced_at_unix
        || request.expires_at_unix - request.produced_at_unix > 2_592_000
        || !uuid7(&request.run_uid)
        || !uuid7(&request.artifact_uid)
        || !uuid7(&request.consent_uid)
        || !request.organization_name.starts_with("organizations/")
        || !request
            .cluster_name
            .starts_with(&(request.organization_name.clone() + "/clusters/"))
        || !request
            .device_name
            .starts_with(&(request.cluster_name.clone() + "/clusterDevices/"))
        || !lower_hex(&request.source_commit, 40)
        || !lower_hex(&request.executable_sha256, 64)
        || !version(&request.rustfs_version)
        || request.build_features.len() > 64
        || !request.build_features.iter().all(|feature| build_feature(feature))
    {
        return Err(EnvironmentError::InvalidExport);
    }
    let result = EnvironmentResult {
        schema_version: 1,
        run_uid: &request.run_uid,
        tool_id: "inventory.environment",
        capability: ENVIRONMENT_CAPABILITY,
        outcome: "SUCCEEDED",
        reason_code: "COMPLETE",
        duration_millis: u64::try_from(duration.as_millis()).unwrap_or(30_000).clamp(1, 30_000),
        provenance: EnvironmentProvenance {
            repository: "rustfs/rustfs",
            source_commit: &request.source_commit,
            executable_sha256: &request.executable_sha256,
            rustfs_version: &request.rustfs_version,
            os_family: EnvironmentOsFamily::current(),
            architecture: match std::env::consts::ARCH {
                "x86_64" => "x86_64",
                "aarch64" => "aarch64",
                _ => "other",
            },
            build_features: &request.build_features,
        },
        coverage: EnvironmentCoverage {
            requested_units: 1,
            completed_units: 1,
            unit: "RESOURCE",
        },
        data: inventory,
    };
    let result_bytes = serde_json::to_vec(&result).map_err(|_| EnvironmentError::ExportEncoding)?;
    let key_id = hex_lower(&Sha256::digest(key.public_key_der()));
    let envelope = EnvironmentEnvelope {
        format_version: "rustfs.connect.diagnosticEnvelope/1",
        protocol_version: "v1",
        organization_name: &request.organization_name,
        cluster_name: &request.cluster_name,
        device_name: &request.device_name,
        run_uid: &request.run_uid,
        artifact_uid: &request.artifact_uid,
        tool_id: "inventory.environment",
        schema_version: 1,
        classification: "L1",
        consent_uid: &request.consent_uid,
        policy_revision: request.policy_revision,
        produced_at: timestamp(request.produced_at_unix)?,
        expires_at: timestamp(request.expires_at_unix)?,
        nonce: URL_SAFE_NO_PAD.encode_to_string(request.nonce),
        device_key_id: &key_id,
        payload: EnvironmentPayload {
            path: "result.json",
            media_type: "application/json",
            size_bytes: result_bytes.len() as u64,
            sha256: hex_lower(&Sha256::digest(&result_bytes)),
        },
    };
    let envelope_bytes = serde_json::to_vec(&envelope).map_err(|_| EnvironmentError::ExportEncoding)?;
    let pkcs8 = key.to_pkcs8_der().map_err(|_| EnvironmentError::ExportSigning)?;
    let signing_key = SigningKey::from_pkcs8_der(pkcs8.as_slice()).map_err(|_| EnvironmentError::ExportSigning)?;
    let mut input = Vec::with_capacity(SIGNATURE_DOMAIN.len() + envelope_bytes.len());
    input.extend_from_slice(SIGNATURE_DOMAIN);
    input.extend_from_slice(&envelope_bytes);
    let signature: Signature = signing_key.sign(&input);
    let signature_bytes = serde_json::to_vec(&EnvironmentSignature {
        algorithm: "ES256",
        key_id: &key_id,
        value: URL_SAFE_NO_PAD.encode_to_string(signature.normalize_s().to_bytes()),
    })
    .map_err(|_| EnvironmentError::ExportEncoding)?;
    if cancel.is_cancelled() {
        return Err(EnvironmentError::Cancelled);
    }
    let cursor = Cursor::new(Vec::new());
    let mut zip = ZipWriter::new(cursor);
    let options = SimpleFileOptions::DEFAULT
        .compression_method(CompressionMethod::Stored)
        .unix_permissions(OUTPUT_MODE);
    for (name, bytes) in [
        ("envelope.json", envelope_bytes.as_slice()),
        ("envelope.sig", signature_bytes.as_slice()),
        ("result.json", result_bytes.as_slice()),
    ] {
        zip.start_file(name, options).map_err(|_| EnvironmentError::ExportEncoding)?;
        zip.write_all(bytes).map_err(|_| EnvironmentError::ExportIo)?;
    }
    let archive_bytes = zip.finish().map_err(|_| EnvironmentError::ExportEncoding)?.into_inner();
    if archive_bytes.len() > 65_536 {
        return Err(EnvironmentError::InvalidExport);
    }
    Ok(SignedEnvironmentExport {
        artifact_uid: request.artifact_uid.clone(),
        archive_sha256: hex_lower(&Sha256::digest(&archive_bytes)),
        archive_bytes,
    })
}

pub fn save_signed_environment_export(
    output: &Path,
    export: &SignedEnvironmentExport,
    cancel: &CancellationToken,
) -> Result<SavedEnvironmentExport, EnvironmentError> {
    if cancel.is_cancelled() {
        return Err(EnvironmentError::Cancelled);
    }
    let parent = output
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let temporary = parent.join(format!(
        ".{}.{}.partial",
        output.file_name().ok_or(EnvironmentError::InvalidExport)?.to_string_lossy(),
        export.artifact_uid
    ));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(OUTPUT_MODE);
    }
    let mut file = options.open(&temporary).map_err(|_| EnvironmentError::ExportIo)?;
    let saved = (|| {
        file.write_all(&export.archive_bytes)
            .map_err(|_| EnvironmentError::ExportIo)?;
        if cancel.is_cancelled() {
            return Err(EnvironmentError::Cancelled);
        }
        file.sync_all().map_err(|_| EnvironmentError::ExportIo)?;
        fs::hard_link(&temporary, output).map_err(|_| EnvironmentError::ExportIo)?;
        fs::remove_file(&temporary).map_err(|_| EnvironmentError::ExportIo)?;
        #[cfg(unix)]
        File::open(parent)
            .and_then(|d| d.sync_all())
            .map_err(|_| EnvironmentError::ExportIo)?;
        Ok(SavedEnvironmentExport {
            artifact_uid: export.artifact_uid.clone(),
            archive_size_bytes: export.archive_bytes.len() as u64,
            archive_sha256: export.archive_sha256.clone(),
        })
    })();
    if saved.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    saved
}

fn timestamp(unix: i64) -> Result<String, EnvironmentError> {
    OffsetDateTime::from_unix_timestamp(unix)
        .map_err(|_| EnvironmentError::InvalidExport)?
        .format(&Rfc3339)
        .map_err(|_| EnvironmentError::ExportEncoding)
}
fn uuid7(value: &str) -> bool {
    Uuid::parse_str(value).is_ok_and(|u| u.get_version() == Some(Version::SortRand) && u.get_variant() == Variant::RFC4122)
}
fn lower_hex(value: &str, len: usize) -> bool {
    value.len() == len && value.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
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
fn hex_lower(bytes: &[u8]) -> String {
    const H: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(H[(b >> 4) as usize] as char);
        out.push(H[(b & 15) as usize] as char);
    }
    out
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EnvironmentSource {
    Filesystem,
    Cpu,
    Memory,
    Network,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum EnvironmentError {
    #[error("inventory_environment_unsupported_version")]
    UnsupportedVersion,
    #[error("inventory_environment_unsupported_capability")]
    UnsupportedCapability,
    #[error("inventory_environment_invalid_timeout")]
    InvalidTimeout,
    #[error("inventory_environment_source_unavailable")]
    SourceUnavailable(EnvironmentSource),
    #[error("inventory_environment_cancelled")]
    Cancelled,
    #[error("inventory_environment_timed_out")]
    TimedOut,
    #[error("inventory_environment_task_failed")]
    TaskFailed,
    #[error("inventory_environment_invalid_export")]
    InvalidExport,
    #[error("inventory_environment_export_encoding_failed")]
    ExportEncoding,
    #[error("inventory_environment_export_signing_failed")]
    ExportSigning,
    #[error("inventory_environment_export_io_failed")]
    ExportIo,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct HostEnvironment {
    pub(crate) os_summary: String,
    pub(crate) kernel_summary: String,
    pub(crate) architecture: &'static str,
    pub(crate) cores: usize,
    pub(crate) total_memory_bytes: u64,
    pub(crate) under_memory_pressure: bool,
    pub(crate) filesystem_types: Vec<EnvironmentFilesystemType>,
    pub(crate) interface_count: usize,
    pub(crate) bond_count: usize,
}

impl HostEnvironment {
    fn collect() -> Result<Self, EnvironmentError> {
        #[cfg(test)]
        let scan = test_support::ScanGuard::start();
        #[cfg(test)]
        if scan.controlled() {
            return Ok(Self {
                os_summary: "test-os".to_owned(),
                kernel_summary: "test-kernel".to_owned(),
                architecture: std::env::consts::ARCH,
                cores: 1,
                total_memory_bytes: 1,
                under_memory_pressure: false,
                filesystem_types: vec![EnvironmentFilesystemType::Other],
                interface_count: 1,
                bond_count: 0,
            });
        }

        // Processes, names, addresses, paths, mount options and device labels are outside this schema.
        let system = System::new_with_specifics(RefreshKind::everything().without_processes());
        let cores = system.cpus().len();
        if cores == 0 {
            return Err(EnvironmentError::SourceUnavailable(EnvironmentSource::Cpu));
        }
        let total_memory_bytes = system.total_memory();
        if total_memory_bytes == 0 {
            return Err(EnvironmentError::SourceUnavailable(EnvironmentSource::Memory));
        }
        let available_memory = system.available_memory();
        let disks = Disks::new_with_refreshed_list();
        let reported_filesystems = disks
            .iter()
            .map(|disk| disk.file_system().to_string_lossy())
            .collect::<Vec<_>>();
        let filesystem_types = filesystem_types(reported_filesystems.iter().map(AsRef::as_ref))?;
        let networks = Networks::new_with_refreshed_list();
        if networks.is_empty() {
            return Err(EnvironmentError::SourceUnavailable(EnvironmentSource::Network));
        }

        Ok(Self {
            os_summary: System::long_os_version().unwrap_or_else(|| "unknown".to_owned()),
            kernel_summary: System::kernel_long_version(),
            architecture: std::env::consts::ARCH,
            cores,
            total_memory_bytes,
            under_memory_pressure: available_memory.saturating_mul(10) < total_memory_bytes,
            filesystem_types,
            interface_count: networks.len(),
            bond_count: networks.keys().filter(|name| name.starts_with("bond")).count(),
        })
    }
}

pub async fn collect_environment(
    inventory: &InventorySnapshot,
    request: EnvironmentCollectionRequest,
    cancel: &CancellationToken,
) -> Result<EnvironmentInventory, EnvironmentError> {
    let host = collect_host_environment(request.timeout, cancel).await?;
    Ok(EnvironmentInventory {
        node_count: inventory.node_count(),
        drive_count: inventory.drive_count(),
        os_family: EnvironmentOsFamily::current(),
        filesystem_types: host.filesystem_types,
    })
}

pub(crate) async fn collect_host_environment(
    timeout: Duration,
    cancel: &CancellationToken,
) -> Result<HostEnvironment, EnvironmentError> {
    let deadline = tokio::time::Instant::now() + timeout;
    let permit = tokio::select! {
        biased;
        () = cancel.cancelled() => return Err(EnvironmentError::Cancelled),
        result = tokio::time::timeout_at(deadline, SYSTEM_SCAN_PERMIT.clone().acquire_owned()) => {
            match result {
                Ok(Ok(permit)) => permit,
                Ok(Err(_)) => return Err(EnvironmentError::TaskFailed),
                Err(_) => return Err(EnvironmentError::TimedOut),
            }
        }
    };
    let task = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        HostEnvironment::collect()
    });
    tokio::select! {
        biased;
        () = cancel.cancelled() => Err(EnvironmentError::Cancelled),
        result = tokio::time::timeout_at(deadline, task) => {
            match result {
                Ok(Ok(environment)) => environment,
                Ok(Err(_)) => Err(EnvironmentError::TaskFailed),
                Err(_) => Err(EnvironmentError::TimedOut),
            }
        }
    }
}

#[cfg(test)]
mod test_support {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Condvar, Mutex};

    use tokio::sync::Semaphore;

    pub(super) static ACTIVE: AtomicUsize = AtomicUsize::new(0);
    pub(super) static MAX_ACTIVE: AtomicUsize = AtomicUsize::new(0);
    static CONTROLLED_SCAN: Mutex<Option<Arc<ScanBarrier>>> = Mutex::new(None);

    struct ScanBarrier {
        reached: Semaphore,
        completed: Semaphore,
        released: Mutex<bool>,
        release: Condvar,
    }

    impl ScanBarrier {
        fn new() -> Self {
            Self {
                reached: Semaphore::new(0),
                completed: Semaphore::new(0),
                released: Mutex::new(false),
                release: Condvar::new(),
            }
        }

        fn wait(&self) {
            self.reached.add_permits(1);
            let mut released = self.released.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            while !*released {
                released = self.release.wait(released).unwrap_or_else(|poisoned| poisoned.into_inner());
            }
        }

        fn release(&self) {
            *self.released.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
            self.release.notify_all();
        }
    }

    pub(super) struct ControlledScan {
        barrier: Arc<ScanBarrier>,
    }

    impl ControlledScan {
        pub(super) async fn wait_until_reached(&self) {
            let permit = self
                .barrier
                .reached
                .acquire()
                .await
                .expect("controlled scan barrier should stay open");
            permit.forget();
        }

        pub(super) fn release(&self) {
            self.barrier.release();
        }

        pub(super) async fn wait_until_completed(&self) {
            let permit = self
                .barrier
                .completed
                .acquire()
                .await
                .expect("controlled scan completion should stay open");
            permit.forget();
        }
    }

    impl Drop for ControlledScan {
        fn drop(&mut self) {
            self.barrier.release();
            let mut controlled = CONTROLLED_SCAN.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            if controlled.as_ref().is_some_and(|barrier| Arc::ptr_eq(barrier, &self.barrier)) {
                controlled.take();
            }
        }
    }

    pub(super) fn control_next_scan() -> ControlledScan {
        let barrier = Arc::new(ScanBarrier::new());
        let mut controlled = CONTROLLED_SCAN.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        assert!(controlled.is_none(), "only one controlled system scan may be installed");
        *controlled = Some(Arc::clone(&barrier));
        ControlledScan { barrier }
    }

    pub(super) struct ScanGuard {
        barrier: Option<Arc<ScanBarrier>>,
    }

    impl ScanGuard {
        pub(super) fn start() -> Self {
            let active = ACTIVE.fetch_add(1, Ordering::SeqCst) + 1;
            MAX_ACTIVE.fetch_max(active, Ordering::SeqCst);
            let barrier = CONTROLLED_SCAN
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone();
            if let Some(barrier) = &barrier {
                barrier.wait();
            }
            Self { barrier }
        }

        pub(super) fn controlled(&self) -> bool {
            self.barrier.is_some()
        }
    }

    impl Drop for ScanGuard {
        fn drop(&mut self) {
            ACTIVE.fetch_sub(1, Ordering::SeqCst);
            if let Some(barrier) = &self.barrier {
                barrier.completed.add_permits(1);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read as _;
    use std::sync::atomic::Ordering;

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn timed_out_and_cancelled_scans_remain_single_flight() {
        test_support::MAX_ACTIVE.store(0, Ordering::SeqCst);
        let controlled = test_support::control_next_scan();

        let first = tokio::spawn(async {
            let cancel = CancellationToken::new();
            collect_host_environment(Duration::from_millis(20), &cancel).await
        });
        controlled.wait_until_reached().await;
        tokio::time::advance(Duration::from_millis(20)).await;
        assert_eq!(first.await.expect("first scan"), Err(EnvironmentError::TimedOut));
        assert_eq!(test_support::ACTIVE.load(Ordering::SeqCst), 1);
        assert_eq!(SYSTEM_SCAN_PERMIT.available_permits(), 0);

        let second_cancel = CancellationToken::new();
        let second = collect_host_environment(Duration::from_secs(1), &second_cancel);
        tokio::pin!(second);
        assert!(futures::poll!(second.as_mut()).is_pending());
        assert_eq!(test_support::MAX_ACTIVE.load(Ordering::SeqCst), 1);
        second_cancel.cancel();
        assert_eq!(second.await, Err(EnvironmentError::Cancelled));
        controlled.release();
        controlled.wait_until_completed().await;
        assert_eq!(test_support::ACTIVE.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn filesystem_projection_drops_paths_options_and_secret_like_values() {
        assert_eq!(
            filesystem_types(["xfs", "ext4", "/srv/customer-a", "rw,password=SYNTHETIC_SECRET_123"]),
            Ok(vec![
                EnvironmentFilesystemType::Ext4,
                EnvironmentFilesystemType::Xfs,
                EnvironmentFilesystemType::Other,
            ])
        );
        assert_eq!(
            filesystem_types(std::iter::empty()),
            Err(EnvironmentError::SourceUnavailable(EnvironmentSource::Filesystem))
        );
    }

    #[test]
    fn signed_export_contains_only_the_allow_list_and_never_clobbers() {
        let inventory = EnvironmentInventory {
            node_count: 1,
            drive_count: 0,
            os_family: EnvironmentOsFamily::Linux,
            filesystem_types: vec![EnvironmentFilesystemType::Xfs],
        };
        let now = OffsetDateTime::now_utc().unix_timestamp();
        let request = EnvironmentExportRequest {
            confirmed: true,
            organization_name: "organizations/019e3ae0-0000-7000-8000-000000000001".into(),
            cluster_name: "organizations/019e3ae0-0000-7000-8000-000000000001/clusters/019e3ae0-0000-7000-8000-000000000002".into(),
            device_name: "organizations/019e3ae0-0000-7000-8000-000000000001/clusters/019e3ae0-0000-7000-8000-000000000002/clusterDevices/019e3ae0-0000-7000-8000-000000000003".into(),
            run_uid: "019e3ae0-0000-7000-8000-000000000004".into(),
            artifact_uid: "019e3ae0-0000-7000-8000-000000000005".into(),
            consent_uid: "019e3ae0-0000-7000-8000-000000000006".into(),
            policy_revision: 1,
            produced_at_unix: now,
            expires_at_unix: now + 60,
            nonce: [7; 32],
            source_commit: "a".repeat(40),
            executable_sha256: "b".repeat(64),
            rustfs_version: "1.0.0-rc.6".into(),
            build_features: vec![],
        };
        let cancel = CancellationToken::new();
        let mut unconfirmed = request.clone();
        unconfirmed.confirmed = false;
        assert!(matches!(
            sign_environment_inventory(&inventory, &unconfirmed, &DeviceIdentity::generate(), Duration::from_millis(1), &cancel,),
            Err(EnvironmentError::InvalidExport)
        ));
        let export =
            sign_environment_inventory(&inventory, &request, &DeviceIdentity::generate(), Duration::from_millis(1), &cancel)
                .expect("signed export");
        let mut zip = zip::ZipArchive::new(Cursor::new(export.archive_bytes.as_slice())).expect("zip");
        let mut result = String::new();
        zip.by_name("result.json")
            .expect("result")
            .read_to_string(&mut result)
            .expect("read");
        let value: serde_json::Value = serde_json::from_str(&result).expect("json");
        assert_eq!(value["data"]["driveCount"], 0);
        assert_eq!(value["data"].as_object().expect("data").len(), 4);
        assert!(!result.contains("secret"));

        let directory = tempfile::tempdir().expect("tempdir");
        let output = directory.path().join("environment.zip");
        save_signed_environment_export(&output, &export, &cancel).expect("save");
        assert!(save_signed_environment_export(&output, &export, &cancel).is_err());
    }
}
