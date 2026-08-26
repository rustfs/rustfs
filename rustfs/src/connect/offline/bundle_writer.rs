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

//! Deterministic, bounded support-bundle archive production.

#[cfg(target_os = "linux")]
use std::ffi::{CStr, CString};
#[cfg(target_os = "linux")]
use std::fs::{self, File};
#[cfg(target_os = "linux")]
use std::io::{Read as _, Seek as _, SeekFrom, Write as _};
use std::path::Path;
#[cfg(target_os = "linux")]
use std::path::PathBuf;

#[cfg(target_os = "linux")]
use base64_simd::URL_SAFE_NO_PAD;
#[cfg(target_os = "linux")]
use p256::ecdsa::{Signature, SigningKey, signature::Signer as _};
#[cfg(target_os = "linux")]
use p256::pkcs8::DecodePrivateKey as _;
#[cfg(target_os = "linux")]
use rand::{TryRng as _, rngs::SysRng};
#[cfg(target_os = "linux")]
use sha2::{Digest as _, Sha256};
use thiserror::Error;
#[cfg(target_os = "linux")]
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio_util::sync::CancellationToken;
#[cfg(target_os = "linux")]
use zip::{CompressionMethod, ZipWriter, write::SimpleFileOptions};

#[cfg(target_os = "linux")]
use super::collectors::{DataClassification, OfflineCollector};
#[cfg(target_os = "linux")]
use super::manifest::{
    BundleIdentity, BundleManifest, BundleManifestEntry, BundleSignature, DOMAIN_TAG, SIGNATURE_FILE, SIGNED_FILE,
};
use super::manifest_entry::ManifestEntry;
#[cfg(target_os = "linux")]
use super::redaction::{REDACTION_VERSION, RULESET_HASH, RedactionSource, redact_json};
use crate::connect::identity::DeviceIdentity;

#[cfg(target_os = "linux")]
const ENTRY_TYPE: &str = "offline-diagnostic";
#[cfg(target_os = "linux")]
const ENTRY_LIMIT: usize = 16 * 1024;
#[cfg(target_os = "linux")]
const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
#[cfg(target_os = "linux")]
const MANIFEST_LIMIT: usize = 1024 * 1024;
#[cfg(target_os = "linux")]
const SIGNATURE_LIMIT: usize = 4 * 1024;
#[cfg(target_os = "linux")]
const ARCHIVE_LIMIT: u64 = 256 * 1024 * 1024;
#[cfg(target_os = "linux")]
const OUTPUT_MODE: u32 = 0o600;

#[cfg(target_os = "linux")]
const ENTRY_SPECS: [(OfflineCollector, &str); 12] = [
    (OfflineCollector::RustfsVersion, "offline/rustfs-version.json"),
    (OfflineCollector::NodeCount, "offline/node-count.json"),
    (OfflineCollector::DriveCount, "offline/drive-count.json"),
    (OfflineCollector::CapacityUsedBytes, "offline/capacity-used-bytes.json"),
    (OfflineCollector::CapacityTotalBytes, "offline/capacity-total-bytes.json"),
    (OfflineCollector::CoarseHealthFlags, "offline/coarse-health-flags.json"),
    (OfflineCollector::OsSummary, "offline/os-summary.json"),
    (OfflineCollector::KernelSummary, "offline/kernel-summary.json"),
    (OfflineCollector::CpuSummary, "offline/cpu-summary.json"),
    (OfflineCollector::MemorySummary, "offline/memory-summary.json"),
    (OfflineCollector::FilesystemSummary, "offline/filesystem-summary.json"),
    (OfflineCollector::NetworkSummary, "offline/network-summary.json"),
];

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BundleContext {
    pub bundle_uid: String,
    pub device_name: String,
    pub nonce: [u8; 32],
    pub produced_at_unix: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BundleReceipt {
    pub bundle_uid: String,
    pub device_name: String,
    pub archive_size_bytes: u64,
    pub archive_sha256: String,
    pub l0_count: usize,
    pub l0_bytes: u64,
    pub l1_count: usize,
    pub l1_bytes: u64,
}

#[derive(Debug, Error)]
pub enum BundleError {
    #[error("offline bundle production was cancelled")]
    Cancelled,
    #[error("bundleUid or deviceName is not a canonical UUIDv7 resource name")]
    InvalidIdentity,
    #[error("offline diagnostic entries do not match the fixed bundle schema")]
    InvalidEntries,
    #[error("offline bundle metadata is not representable")]
    InvalidMetadata,
    #[error("offline bundle exceeds its {kind} size limit")]
    TooLarge { kind: &'static str },
    #[error("the device key cannot sign the offline bundle")]
    Signing,
    #[error("the operating system random source failed")]
    Random,
    #[error("offline bundles require Linux file security semantics")]
    UnsupportedPlatform,
    #[error("the offline bundle output directory is not private and stable")]
    UnsafeOutput,
    #[error("cannot write the offline bundle: {0}")]
    Io(#[from] std::io::Error),
    #[error("cannot encode the offline bundle archive: {0}")]
    Zip(#[from] zip::result::ZipError),
    #[error("the bundle was published, but its directory could not be made durable: {0}")]
    DurabilityAfterCommit(std::io::Error),
}

pub fn write_offline_bundle(
    output: &Path,
    context: &BundleContext,
    entries: &[ManifestEntry],
    key: &DeviceIdentity,
    cancel: &CancellationToken,
) -> Result<BundleReceipt, BundleError> {
    #[cfg(target_os = "linux")]
    {
        write_offline_bundle_unix(output, context, entries, key, cancel)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (output, context, entries, key, cancel);
        Err(BundleError::UnsupportedPlatform)
    }
}

#[cfg(target_os = "linux")]
fn write_offline_bundle_unix(
    output: &Path,
    context: &BundleContext,
    entries: &[ManifestEntry],
    key: &DeviceIdentity,
    cancel: &CancellationToken,
) -> Result<BundleReceipt, BundleError> {
    check_cancel(cancel)?;
    let identity = BundleIdentity::parse(&context.bundle_uid, &context.device_name).ok_or(BundleError::InvalidIdentity)?;
    validate_entries(entries)?;

    let produced_at = OffsetDateTime::from_unix_timestamp(context.produced_at_unix).map_err(|_| BundleError::InvalidMetadata)?;
    let produced_at = produced_at.format(&Rfc3339).map_err(|_| BundleError::InvalidMetadata)?;
    let nonce = URL_SAFE_NO_PAD.encode_to_string(context.nonce);
    let device_key_id = hex_lower(&Sha256::digest(key.public_key_der()));
    let manifest_entries = entries
        .iter()
        .zip(ENTRY_SPECS)
        .map(|(entry, (_, path))| BundleManifestEntry {
            path,
            entry_type: ENTRY_TYPE,
            size_bytes: entry.canonical_json.len() as u64,
            sha256: hex_lower(&Sha256::digest(entry.canonical_json.as_bytes())),
            classification: entry.classification,
        })
        .collect::<Vec<_>>();
    let manifest = BundleManifest::new(&identity, &device_key_id, &nonce, &produced_at, &manifest_entries);
    let manifest_bytes = serde_json::to_vec(&manifest).map_err(|_| BundleError::InvalidMetadata)?;
    if manifest_bytes.len() > MANIFEST_LIMIT {
        return Err(BundleError::TooLarge { kind: "manifest" });
    }
    let signature = sign(key, &manifest_bytes)?;
    let signature_bytes =
        serde_json::to_vec(&BundleSignature::new(&device_key_id, &signature)).map_err(|_| BundleError::InvalidMetadata)?;
    if signature_bytes.len() > SIGNATURE_LIMIT {
        return Err(BundleError::TooLarge { kind: "signature" });
    }

    let (mut temporary, file) = TemporaryBundle::create(output)?;
    let options = SimpleFileOptions::DEFAULT
        .compression_method(CompressionMethod::Stored)
        .system(zip::System::Unix)
        .unix_permissions(OUTPUT_MODE);
    let mut archive = ZipWriter::new(file);
    #[cfg(test)]
    test_support::at(test_support::Stage::EntryWrite)?;
    for ((entry, (_, path)), manifest_entry) in entries.iter().zip(ENTRY_SPECS).zip(&manifest_entries) {
        check_cancel(cancel)?;
        debug_assert_eq!(entry.canonical_json.len() as u64, manifest_entry.size_bytes);
        archive.start_file(path, options)?;
        archive.write_all(entry.canonical_json.as_bytes())?;
    }
    check_cancel(cancel)?;
    archive.start_file(SIGNED_FILE, options)?;
    archive.write_all(&manifest_bytes)?;
    archive.start_file(SIGNATURE_FILE, options)?;
    archive.write_all(&signature_bytes)?;
    let mut file = archive.finish()?;
    #[cfg(test)]
    test_support::at(test_support::Stage::FileSync)?;
    file.sync_all()?;
    let archive_size_bytes = file.metadata()?.len();
    if archive_size_bytes > ARCHIVE_LIMIT {
        return Err(BundleError::TooLarge { kind: "archive" });
    }
    file.seek(SeekFrom::Start(0))?;
    let mut digest = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    #[cfg(test)]
    test_support::at(test_support::Stage::BeforePublish)?;
    check_cancel(cancel)?;
    drop(file);
    temporary.publish()?;

    let mut receipt = BundleReceipt {
        bundle_uid: identity.bundle_uid,
        device_name: identity.device_name,
        archive_size_bytes,
        archive_sha256: hex_lower(&digest.finalize()),
        l0_count: 0,
        l0_bytes: 0,
        l1_count: 0,
        l1_bytes: 0,
    };
    for entry in &manifest_entries {
        match entry.classification {
            DataClassification::L0 => {
                receipt.l0_count += 1;
                receipt.l0_bytes += entry.size_bytes;
            }
            DataClassification::L1 => {
                receipt.l1_count += 1;
                receipt.l1_bytes += entry.size_bytes;
            }
        }
    }
    Ok(receipt)
}

#[cfg(target_os = "linux")]
fn validate_entries(entries: &[ManifestEntry]) -> Result<(), BundleError> {
    if entries.len() != ENTRY_SPECS.len() {
        return Err(BundleError::InvalidEntries);
    }
    for (entry, (collector, _)) in entries.iter().zip(ENTRY_SPECS) {
        if entry.canonical_json.len() > ENTRY_LIMIT
            || entry.field_id != collector.field_id()
            || entry.classification != collector.classification()
            || entry.redaction_version != REDACTION_VERSION
            || entry.ruleset_hash != RULESET_HASH
        {
            return Err(BundleError::InvalidEntries);
        }
        let Ok(value) = serde_json::from_str::<serde_json::Value>(&entry.canonical_json) else {
            return Err(BundleError::InvalidEntries);
        };
        let Some(object) = value.as_object() else {
            return Err(BundleError::InvalidEntries);
        };
        let Some(payload) = object.get(collector.field_name()) else {
            return Err(BundleError::InvalidEntries);
        };
        let Ok(redacted) = redact_json(RedactionSource::OfflineDiagnostic, entry.canonical_json.as_bytes()) else {
            return Err(BundleError::InvalidEntries);
        };
        if redacted.canonical_json != entry.canonical_json
            || redacted.redaction_version != REDACTION_VERSION
            || redacted.ruleset_hash != RULESET_HASH
            || object.len() != 1
            || !valid_payload(collector, payload)
            || serde_json::to_string(&value).ok().as_deref() != Some(entry.canonical_json.as_str())
        {
            return Err(BundleError::InvalidEntries);
        }
    }
    let used = entry_u64(&entries[3], "capacityUsedBytes").ok_or(BundleError::InvalidEntries)?;
    let total = entry_u64(&entries[4], "capacityTotalBytes").ok_or(BundleError::InvalidEntries)?;
    if used > total {
        return Err(BundleError::InvalidEntries);
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn valid_payload(collector: OfflineCollector, value: &serde_json::Value) -> bool {
    match collector {
        OfflineCollector::RustfsVersion => value.as_str().is_some_and(valid_rustfs_version),
        OfflineCollector::NodeCount => value.as_u64().is_some_and(|count| (1..=4096).contains(&count)),
        OfflineCollector::DriveCount => value.as_u64().is_some_and(|count| count <= 1_048_576),
        OfflineCollector::CapacityUsedBytes | OfflineCollector::CapacityTotalBytes => {
            value.as_u64().is_some_and(|bytes| bytes <= MAX_SAFE_INTEGER)
        }
        OfflineCollector::CoarseHealthFlags => value.as_array().is_some_and(|flags| {
            ordered_known_strings(
                flags,
                &[
                    "capacity.critical",
                    "capacity.warning",
                    "clock.skew",
                    "cluster.degraded",
                    "cluster.healing",
                    "cluster.readonly",
                    "drive.offline",
                    "node.offline",
                ],
            )
        }),
        OfflineCollector::OsSummary | OfflineCollector::KernelSummary => value.is_string(),
        OfflineCollector::CpuSummary => value.as_object().is_some_and(|object| {
            object.len() == 2
                && object.get("architecture").and_then(serde_json::Value::as_str) == Some(std::env::consts::ARCH)
                && object.get("cores").and_then(serde_json::Value::as_u64).is_some()
        }),
        OfflineCollector::MemorySummary => value.as_object().is_some_and(|object| {
            object.len() == 2
                && object.get("totalBytes").and_then(serde_json::Value::as_u64).is_some()
                && object.get("underPressure").and_then(serde_json::Value::as_bool).is_some()
        }),
        OfflineCollector::FilesystemSummary => value.as_array().is_some_and(|values| ordered_strings(values.as_slice())),
        OfflineCollector::NetworkSummary => value.as_object().is_some_and(|object| {
            object.len() == 2
                && object.get("bondCount").and_then(serde_json::Value::as_u64).is_some()
                && object.get("interfaceCount").and_then(serde_json::Value::as_u64).is_some()
        }),
    }
}

#[cfg(target_os = "linux")]
fn valid_rustfs_version(version: &str) -> bool {
    let mut components = version.split('.');
    (0..3).all(|_| {
        components.next().is_some_and(|component| {
            !component.is_empty()
                && component.len() <= 4
                && (component == "0" || !component.starts_with('0'))
                && component.parse::<u16>().is_ok_and(|value| value <= 9999)
        })
    }) && components.next().is_none()
}

#[cfg(target_os = "linux")]
fn ordered_strings(values: &[serde_json::Value]) -> bool {
    let mut previous = None;
    for value in values {
        let Some(value) = value.as_str() else {
            return false;
        };
        if previous.is_some_and(|previous| previous >= value) {
            return false;
        }
        previous = Some(value);
    }
    true
}

#[cfg(target_os = "linux")]
fn ordered_known_strings(values: &[serde_json::Value], allowed: &[&str]) -> bool {
    ordered_strings(values)
        && values
            .iter()
            .all(|value| value.as_str().is_some_and(|value| allowed.binary_search(&value).is_ok()))
}

#[cfg(target_os = "linux")]
fn entry_u64(entry: &ManifestEntry, field: &str) -> Option<u64> {
    serde_json::from_str::<serde_json::Value>(&entry.canonical_json)
        .ok()?
        .get(field)?
        .as_u64()
}

#[cfg(target_os = "linux")]
fn sign(key: &DeviceIdentity, manifest: &[u8]) -> Result<String, BundleError> {
    let pkcs8 = key.to_pkcs8_der().map_err(|_| BundleError::Signing)?;
    let signing_key = SigningKey::from_pkcs8_der(pkcs8.as_slice()).map_err(|_| BundleError::Signing)?;
    let mut input = Vec::with_capacity(DOMAIN_TAG.len() + 1 + manifest.len());
    input.extend_from_slice(DOMAIN_TAG.as_bytes());
    input.push(0);
    input.extend_from_slice(manifest);
    let signature: Signature = signing_key.sign(&input);
    Ok(URL_SAFE_NO_PAD.encode_to_string(signature.normalize_s().to_bytes()))
}

#[cfg(target_os = "linux")]
fn check_cancel(cancel: &CancellationToken) -> Result<(), BundleError> {
    if cancel.is_cancelled() {
        Err(BundleError::Cancelled)
    } else {
        Ok(())
    }
}

#[cfg(target_os = "linux")]
fn hex_lower(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut output, "{byte:02x}").expect("writing to a string cannot fail");
    }
    output
}

#[cfg(target_os = "linux")]
struct TemporaryBundle {
    directory: File,
    directory_identity: (u64, u64),
    parent_path: PathBuf,
    temporary_name: CString,
    output_name: CString,
    identity: (u64, u64),
    published: bool,
}

#[cfg(target_os = "linux")]
impl TemporaryBundle {
    fn create(output: &Path) -> Result<(Self, File), BundleError> {
        let parent = output
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let directory = open_directory(parent)?;
        validate_directory(&directory)?;
        let directory_identity = file_identity(&directory)?;
        let output_name = c_name(
            output
                .file_name()
                .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidInput))?,
        )?;
        for _ in 0..16 {
            let mut random = [0u8; 16];
            SysRng.try_fill_bytes(&mut random).map_err(|_| BundleError::Random)?;
            let temporary_name = CString::new(format!(".bundle.{}.tmp", hex_lower(&random)))
                .map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
            match create_file_at(&directory, &temporary_name) {
                Ok(file) => {
                    use std::os::unix::fs::PermissionsExt as _;

                    let mut created = CreatedFile::new(&directory, &temporary_name, &file);
                    #[cfg(test)]
                    test_support::at(test_support::Stage::Identity)?;
                    let identity = file_identity(&file)?;
                    created.disarm();
                    drop(created);
                    let temporary = Self {
                        directory,
                        directory_identity,
                        parent_path: parent.to_owned(),
                        temporary_name,
                        output_name,
                        identity,
                        published: false,
                    };
                    #[cfg(test)]
                    test_support::at(test_support::Stage::Permissions)?;
                    file.set_permissions(fs::Permissions::from_mode(OUTPUT_MODE))?;
                    if validate_regular_file(&file)? != identity {
                        return Err(BundleError::UnsafeOutput);
                    }
                    return Ok((temporary, file));
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(std::io::Error::new(std::io::ErrorKind::AlreadyExists, "cannot allocate a unique temporary bundle").into())
    }

    fn publish(&mut self) -> Result<(), BundleError> {
        validate_directory(&self.directory)?;
        self.validate_parent_path()?;
        let staged = open_file_at(&self.directory, &self.temporary_name)?;
        if validate_regular_file(&staged)? != self.identity {
            return Err(BundleError::UnsafeOutput);
        }
        #[cfg(test)]
        test_support::at(test_support::Stage::Rename)?;
        rename_at(&self.directory, &self.temporary_name, &self.output_name)?;
        self.published = true;
        let published = open_file_at(&self.directory, &self.output_name).map_err(BundleError::DurabilityAfterCommit)?;
        if validate_regular_file(&published).map_err(|error| match error {
            BundleError::Io(error) => BundleError::DurabilityAfterCommit(error),
            _ => BundleError::DurabilityAfterCommit(std::io::Error::other("published bundle identity is unsafe")),
        })? != self.identity
        {
            return Err(BundleError::DurabilityAfterCommit(std::io::Error::other(
                "published bundle identity changed",
            )));
        }
        #[cfg(test)]
        test_support::at(test_support::Stage::DirectorySync).map_err(BundleError::DurabilityAfterCommit)?;
        self.directory.sync_all().map_err(BundleError::DurabilityAfterCommit)?;
        self.validate_parent_path().map_err(|error| {
            BundleError::DurabilityAfterCommit(std::io::Error::other(format!(
                "offline bundle output directory changed after publication: {error}"
            )))
        })?;
        Ok(())
    }

    fn validate_parent_path(&self) -> Result<(), BundleError> {
        let current_directory = open_directory(&self.parent_path)?;
        validate_directory(&current_directory)?;
        if file_identity(&current_directory)? != self.directory_identity {
            return Err(BundleError::UnsafeOutput);
        }
        Ok(())
    }
}

#[cfg(target_os = "linux")]
impl Drop for TemporaryBundle {
    fn drop(&mut self) {
        if !self.published
            && path_identity_at(&self.directory, &self.temporary_name).is_ok_and(|identity| identity == self.identity)
        {
            let _ = unlink_at(&self.directory, &self.temporary_name);
        }
    }
}

#[cfg(target_os = "linux")]
struct CreatedFile<'a> {
    directory: &'a File,
    name: &'a CStr,
    file: &'a File,
    armed: bool,
}

#[cfg(target_os = "linux")]
impl<'a> CreatedFile<'a> {
    fn new(directory: &'a File, name: &'a CStr, file: &'a File) -> Self {
        Self {
            directory,
            name,
            file,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

#[cfg(target_os = "linux")]
impl Drop for CreatedFile<'_> {
    fn drop(&mut self) {
        if self.armed
            && file_identity(self.file)
                .is_ok_and(|expected| path_identity_at(self.directory, self.name).is_ok_and(|identity| identity == expected))
        {
            let _ = unlink_at(self.directory, self.name);
        }
    }
}

#[cfg(target_os = "linux")]
fn open_directory(path: &Path) -> Result<File, BundleError> {
    use std::os::fd::AsRawFd as _;
    use std::path::Component;

    let root = if path.is_absolute() { c"/" } else { c"." };
    let mut directory = open_directory_at(libc::AT_FDCWD, root)?;
    for component in path.components() {
        match component {
            Component::RootDir | Component::CurDir => {}
            Component::Normal(name) => {
                let name = c_name(name)?;
                directory = open_directory_at(directory.as_raw_fd(), &name)?;
            }
            Component::ParentDir | Component::Prefix(_) => return Err(BundleError::UnsafeOutput),
        }
    }
    Ok(directory)
}

#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
fn open_directory_at(parent: std::os::fd::RawFd, name: &CStr) -> std::io::Result<File> {
    use std::os::fd::FromRawFd as _;

    // SAFETY: the parent descriptor and C string are live; a successful descriptor is transferred to File.
    let descriptor = unsafe {
        libc::openat(
            parent,
            name.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_DIRECTORY,
        )
    };
    if descriptor < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: openat returned a new owned descriptor.
    Ok(unsafe { File::from_raw_fd(descriptor) })
}

#[cfg(target_os = "linux")]
fn validate_directory(directory: &File) -> Result<(), BundleError> {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

    let metadata = directory.metadata()?;
    let mode = metadata.permissions().mode() & 0o7777;
    if !metadata.is_dir() || metadata.uid() != process_uid() || mode & 0o022 != 0 {
        return Err(BundleError::UnsafeOutput);
    }
    Ok(())
}

#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
fn create_file_at(directory: &File, name: &CStr) -> std::io::Result<File> {
    use std::os::fd::{AsRawFd as _, FromRawFd as _};

    // SAFETY: the directory descriptor and C string are live; a successful descriptor is transferred to File.
    let descriptor = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDWR | libc::O_CREAT | libc::O_EXCL | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            OUTPUT_MODE,
        )
    };
    if descriptor < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: openat returned a new owned descriptor.
    Ok(unsafe { File::from_raw_fd(descriptor) })
}

#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
fn open_file_at(directory: &File, name: &CStr) -> std::io::Result<File> {
    use std::os::fd::{AsRawFd as _, FromRawFd as _};

    // SAFETY: the directory descriptor and C string are live; a successful descriptor is transferred to File.
    let descriptor = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
        )
    };
    if descriptor < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: openat returned a new owned descriptor.
    Ok(unsafe { File::from_raw_fd(descriptor) })
}

#[cfg(target_os = "linux")]
fn validate_regular_file(file: &File) -> Result<(u64, u64), BundleError> {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

    let metadata = file.metadata()?;
    if !metadata.is_file()
        || metadata.uid() != process_uid()
        || metadata.permissions().mode() & 0o7777 != OUTPUT_MODE
        || metadata.nlink() != 1
    {
        return Err(BundleError::UnsafeOutput);
    }
    Ok((metadata.dev(), metadata.ino()))
}

#[cfg(target_os = "linux")]
fn file_identity(file: &File) -> Result<(u64, u64), BundleError> {
    use std::os::unix::fs::MetadataExt as _;

    let metadata = file.metadata()?;
    Ok((metadata.dev(), metadata.ino()))
}

#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
fn path_identity_at(directory: &File, name: &CStr) -> std::io::Result<(u64, u64)> {
    use std::mem::MaybeUninit;
    use std::os::fd::AsRawFd as _;

    let mut metadata = MaybeUninit::<libc::stat>::uninit();
    // SAFETY: the directory descriptor and C string remain live, and metadata points to writable storage.
    if unsafe { libc::fstatat(directory.as_raw_fd(), name.as_ptr(), metadata.as_mut_ptr(), libc::AT_SYMLINK_NOFOLLOW) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: fstatat initialized metadata after returning success.
    let metadata = unsafe { metadata.assume_init() };
    if metadata.st_mode & libc::S_IFMT != libc::S_IFREG {
        return Err(std::io::Error::other("staged bundle is not a regular file"));
    }
    Ok((metadata.st_dev, metadata.st_ino))
}

#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
fn rename_at(directory: &File, source: &CStr, destination: &CStr) -> std::io::Result<()> {
    use std::os::fd::AsRawFd as _;

    // SAFETY: both C strings and the directory descriptor remain live for the call.
    if unsafe { libc::renameat(directory.as_raw_fd(), source.as_ptr(), directory.as_raw_fd(), destination.as_ptr()) } == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
fn unlink_at(directory: &File, name: &CStr) -> std::io::Result<()> {
    use std::os::fd::AsRawFd as _;

    // SAFETY: the C string and directory descriptor remain live for the call.
    if unsafe { libc::unlinkat(directory.as_raw_fd(), name.as_ptr(), 0) } == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(target_os = "linux")]
fn c_name(name: &std::ffi::OsStr) -> Result<CString, BundleError> {
    use std::os::unix::ffi::OsStrExt as _;

    CString::new(name.as_bytes())
        .map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidInput))
        .map_err(Into::into)
}

#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
fn process_uid() -> u32 {
    // SAFETY: geteuid has no pointer arguments or caller preconditions.
    unsafe { libc::geteuid() }
}

#[cfg(all(test, target_os = "linux"))]
mod test_support {
    use std::cell::RefCell;
    use std::io;

    use tokio_util::sync::CancellationToken;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(super) enum Stage {
        Identity,
        Permissions,
        EntryWrite,
        FileSync,
        BeforePublish,
        Rename,
        DirectorySync,
    }

    pub(super) enum Action {
        Error(i32),
        Cancel(CancellationToken),
        Run(Box<dyn FnOnce()>),
    }

    thread_local! {
        static NEXT: RefCell<Option<(Stage, Action)>> = const { RefCell::new(None) };
    }

    pub(super) fn set(stage: Stage, action: Action) {
        NEXT.with(|next| {
            assert!(next.borrow_mut().replace((stage, action)).is_none());
        });
    }

    pub(super) fn at(stage: Stage) -> io::Result<()> {
        let action = NEXT.with(|next| {
            let mut next = next.borrow_mut();
            next.as_ref()
                .is_some_and(|(expected, _)| *expected == stage)
                .then(|| next.take().expect("fault action"))
        });
        match action {
            Some((_, Action::Error(code))) => Err(io::Error::from_raw_os_error(code)),
            Some((_, Action::Cancel(cancel))) => {
                cancel.cancel();
                Ok(())
            }
            Some((_, Action::Run(action))) => {
                action();
                Ok(())
            }
            None => Ok(()),
        }
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use serde_json::{Map, json};

    use super::test_support::{self, Action, Stage};
    use super::*;

    fn context() -> BundleContext {
        BundleContext {
            bundle_uid: "0198f3a1-8000-7e50-8f61-4a5b6c7d8e94".to_owned(),
            device_name: "organizations/0198f3a1-4c00-7a10-8b21-0c1d2e3f4a50/clusters/0198f3a1-5d00-7b20-9c31-1d2e3f4a5b61/clusterDevices/0198f3a1-6e00-7c30-ad41-2e3f4a5b6c72".to_owned(),
            nonce: [7; 32],
            produced_at_unix: 1_777_860_000,
        }
    }

    fn entries() -> Vec<ManifestEntry> {
        ENTRY_SPECS
            .iter()
            .map(|(collector, _)| {
                let mut value = Map::new();
                value.insert(
                    collector.field_name().to_owned(),
                    match collector {
                        OfflineCollector::RustfsVersion => json!("1.4.2"),
                        OfflineCollector::NodeCount => json!(2),
                        OfflineCollector::DriveCount => json!(3),
                        OfflineCollector::CapacityUsedBytes => json!(1500),
                        OfflineCollector::CapacityTotalBytes => json!(6000),
                        OfflineCollector::CoarseHealthFlags => json!(["cluster.degraded", "drive.offline"]),
                        OfflineCollector::OsSummary => json!("Linux"),
                        OfflineCollector::KernelSummary => json!("6.8.0"),
                        OfflineCollector::CpuSummary => json!({"architecture": std::env::consts::ARCH, "cores": 8}),
                        OfflineCollector::MemorySummary => json!({"totalBytes": 17179869184_u64, "underPressure": false}),
                        OfflineCollector::FilesystemSummary => json!(["ext4", "xfs"]),
                        OfflineCollector::NetworkSummary => json!({"bondCount": 1, "interfaceCount": 4}),
                    },
                );
                ManifestEntry {
                    field_id: collector.field_id(),
                    classification: collector.classification(),
                    canonical_json: serde_json::to_string(&value).expect("test entry"),
                    redaction_version: REDACTION_VERSION,
                    ruleset_hash: RULESET_HASH,
                    redacted_count: 0,
                }
            })
            .collect()
    }

    fn temp_residue(directory: &Path) -> usize {
        fs::read_dir(directory)
            .expect("read test directory")
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().starts_with(".bundle."))
            .count()
    }

    #[test]
    fn connect_offline_bundle_faults_preserve_the_commit_boundary() {
        let temp = tempfile::tempdir().expect("bundle tempdir");
        let output = temp.path().join("bundle.zip");
        let key = DeviceIdentity::generate();

        for stage in [
            Stage::Identity,
            Stage::Permissions,
            Stage::EntryWrite,
            Stage::FileSync,
            Stage::Rename,
        ] {
            fs::write(&output, b"old bundle").expect("old output");
            test_support::set(stage, Action::Error(libc::ENOSPC));
            assert!(matches!(
                write_offline_bundle(&output, &context(), &entries(), &key, &CancellationToken::new()),
                Err(BundleError::Io(_))
            ));
            assert_eq!(fs::read(&output).expect("preserved output"), b"old bundle");
            assert_eq!(temp_residue(temp.path()), 0);
        }

        fs::write(&output, b"old bundle").expect("old output");
        let cancel = CancellationToken::new();
        test_support::set(Stage::BeforePublish, Action::Cancel(cancel.clone()));
        assert!(matches!(
            write_offline_bundle(&output, &context(), &entries(), &key, &cancel),
            Err(BundleError::Cancelled)
        ));
        assert_eq!(fs::read(&output).expect("preserved output"), b"old bundle");
        assert_eq!(temp_residue(temp.path()), 0);

        let original = temp.path().join("original");
        let moved = temp.path().join("moved");
        fs::create_dir(&original).expect("original output directory");
        let swapped_output = original.join("bundle.zip");
        let swap_original = original;
        let swap_moved = moved.clone();
        test_support::set(
            Stage::BeforePublish,
            Action::Run(Box::new(move || {
                fs::rename(&swap_original, &swap_moved).expect("move anchored directory");
                fs::create_dir(&swap_original).expect("replacement directory");
            })),
        );
        assert!(matches!(
            write_offline_bundle(&swapped_output, &context(), &entries(), &key, &CancellationToken::new()),
            Err(BundleError::UnsafeOutput)
        ));
        assert!(!swapped_output.exists());
        assert_eq!(temp_residue(&moved), 0);

        use std::os::unix::fs::symlink;

        let target = temp.path().join("ancestor-target");
        let private = target.join("private");
        fs::create_dir_all(&private).expect("symlink target directory");
        let link = temp.path().join("ancestor-link");
        symlink(&target, &link).expect("ancestor symlink");
        let escaped_output = link.join("private/bundle.zip");
        assert!(write_offline_bundle(&escaped_output, &context(), &entries(), &key, &CancellationToken::new()).is_err());
        assert!(!private.join("bundle.zip").exists());

        let original = temp.path().join("publish-original");
        let moved = temp.path().join("publish-moved");
        fs::create_dir(&original).expect("original publish directory");
        let swapped_output = original.join("bundle.zip");
        let swap_original = original;
        let swap_moved = moved.clone();
        test_support::set(
            Stage::Rename,
            Action::Run(Box::new(move || {
                fs::rename(&swap_original, &swap_moved).expect("move directory during publication");
                fs::create_dir(&swap_original).expect("replacement publish directory");
            })),
        );
        assert!(matches!(
            write_offline_bundle(&swapped_output, &context(), &entries(), &key, &CancellationToken::new()),
            Err(BundleError::DurabilityAfterCommit(_))
        ));
        assert!(!swapped_output.exists());
        assert!(
            fs::read(moved.join("bundle.zip"))
                .expect("committed bundle")
                .starts_with(b"PK")
        );
        assert_eq!(temp_residue(&moved), 0);

        test_support::set(Stage::DirectorySync, Action::Error(libc::EIO));
        assert!(matches!(
            write_offline_bundle(&output, &context(), &entries(), &key, &CancellationToken::new()),
            Err(BundleError::DurabilityAfterCommit(_))
        ));
        assert!(fs::read(&output).expect("published output").starts_with(b"PK"));
        assert_eq!(temp_residue(temp.path()), 0);
    }
}
