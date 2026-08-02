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

//! Local backend backup export: sealed, KEK-protected bundle production.
//!
//! This is the producer side; the consumer side lives in
//! [`crate::backup::local_restore`]. The admin API is not wired here —
//! callers construct the request and supply the backup KEK explicitly.
//!
//! # Bundle layout
//!
//! A bundle is a directory (simple to produce, artifacts stream one file at a
//! time, and partial output is trivially recognizable because the manifest is
//! written last):
//!
//! ```text
//! <destination>/
//!   manifest.json                      # sealed BackupManifest, written last
//!   artifacts/keys/<key_id>.key.enc    # one per stored key record
//!   artifacts/master-key.salt.enc      # present when the salt file exists
//! ```
//!
//! # Artifact payload framing
//!
//! Every artifact payload is `nonce (12 bytes) || AES-256-GCM ciphertext`,
//! encrypted under the caller-supplied backup KEK with an AAD binding of
//! `(context, backup_id, snapshot_generation, artifact path)`, so an artifact
//! cannot be swapped into another bundle or renamed within its own bundle.
//! Records already encrypted at rest stay encrypted inside the wrap;
//! plaintext-dev-only records become ciphertext-only in the bundle, which is
//! their mandatory re-wrap under the backup KEK.
//!
//! # Write protocol
//!
//! Artifacts are written and fsynced first, re-read and digest-verified, and
//! only then is the sealed manifest (completeness marker plus final digest)
//! published. A crash at any earlier point leaves a bundle without a
//! manifest, which decodes as an incomplete bundle and can never be restored.

use crate::backends::local::{LocalKmsClient, StoredKeyProtection, unknown_protection_marker};
use crate::backup::capability::{AtRestProtection, BackupBackendKind, BackupResponsibility};
use crate::backup::error::BackupError;
use crate::backup::manifest::{
    AeadAlgorithm, ArtifactDescriptor, ArtifactKind, BackupKekDescriptor, BackupManifest, CompletenessState, ContentDigest,
    DigestAlgorithm, LocalKdfDescriptor, LocalKeyDerivation,
};
use crate::error::{KmsError, Result};
use aes_gcm::{
    Aes256Gcm, Key, Nonce,
    aead::{Aead, KeyInit, Payload},
};
use jiff::Zoned;
use rand::RngExt;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use zeroize::Zeroizing;

/// File name of the sealed manifest inside a bundle directory.
pub const LOCAL_BUNDLE_MANIFEST_FILE: &str = "manifest.json";
const ARTIFACTS_DIR: &str = "artifacts";
const KEYS_DIR: &str = "artifacts/keys";
const SALT_ARTIFACT_PATH: &str = "artifacts/master-key.salt.enc";
const CONFIG_ARTIFACT_PATH: &str = "artifacts/kms-config.json.enc";
pub(crate) const AEAD_NONCE_LEN: usize = 12;
/// Domain-separation context for the artifact AAD binding.
const BUNDLE_AAD_CONTEXT: &str = "rustfs-kms-local-backup:v1";
/// Domain-separation context for the master-key verifier.
const MASTER_KEY_VERIFIER_CONTEXT: &str = "rustfs-kms-local-master-key-verifier:v1";
/// Verifier scheme prefix for the Argon2id (salted) derivation.
pub(crate) const MASTER_KEY_VERIFIER_ARGON2ID_PREFIX: &str = "argon2id-v1:";
/// Verifier scheme prefix for the legacy pre-beta.9 SHA-256 derivation.
pub(crate) const MASTER_KEY_VERIFIER_LEGACY_PREFIX: &str = "legacy-sha256-v1:";

/// Caller-supplied backup KEK: a trust root deliberately separate from the
/// business KMS hierarchy (it must not be a key that is itself part of the
/// state being backed up). Where the KEK comes from is the admin layer's
/// concern; this module only consumes it.
pub struct BackupKek {
    kek_id: String,
    kek_version: u32,
    key: Zeroizing<[u8; 32]>,
}

impl BackupKek {
    /// Wrap 32 bytes of KEK material. The material is zeroized on drop;
    /// callers should zeroize their own copy of the input.
    pub fn new(kek_id: impl Into<String>, kek_version: u32, key: [u8; 32]) -> Result<Self> {
        let kek_id = kek_id.into();
        if kek_id.is_empty() {
            return Err(KmsError::validation_error("backup KEK id must not be empty"));
        }
        Ok(Self {
            kek_id,
            kek_version,
            key: Zeroizing::new(key),
        })
    }

    /// Manifest descriptor for this KEK.
    pub fn descriptor(&self) -> BackupKekDescriptor {
        BackupKekDescriptor {
            kek_id: self.kek_id.clone(),
            kek_version: self.kek_version,
            aead_algorithm: AeadAlgorithm::Aes256Gcm,
        }
    }

    pub(crate) fn cipher(&self) -> Aes256Gcm {
        Aes256Gcm::new(&Key::<Aes256Gcm>::from(*self.key))
    }
}

/// Parameters of one export run.
///
/// `snapshot_generation` is injected by the caller: the contract only
/// requires it to be monotonic per deployment, and the source (persisted
/// counter, coordinated clock) is decided by the admin layer, which keeps
/// this module free of ambient time or state lookups.
#[derive(Debug, Clone)]
pub struct LocalBackupExportRequest {
    /// Unique identifier for this backup.
    pub backup_id: String,
    /// Opaque identity of the producing deployment.
    pub deployment_identity: String,
    /// RustFS version string recorded in the manifest.
    pub rustfs_version: String,
    /// Monotonic snapshot generation this bundle belongs to.
    pub snapshot_generation: u64,
    /// Bundle output directory; must not exist yet or must be empty.
    pub destination: PathBuf,
    /// Serialized sanitized KMS configuration to seal into the bundle, if the
    /// caller produced one.
    ///
    /// The persisted `KmsConfig` carries plaintext credentials (Vault token,
    /// AppRole secret id, Local master key), so the sanitized projection is
    /// owned by the admin layer and this module only seals the bytes it is
    /// handed. The artifact is evidence for an operator decision: restore
    /// verifies it opens but never applies a configuration.
    pub sanitized_config: Option<Vec<u8>>,
}

impl LocalBackupExportRequest {
    fn validate(&self) -> Result<()> {
        for (field, value) in [
            ("backup_id", &self.backup_id),
            ("deployment_identity", &self.deployment_identity),
            ("rustfs_version", &self.rustfs_version),
        ] {
            if value.is_empty() {
                return Err(KmsError::validation_error(format!("backup export {field} must not be empty")));
            }
        }
        Ok(())
    }
}

/// Minimal projection of a stored key record: only the fields the exporter
/// needs. Unknown fields are ignored on purpose — the record travels into the
/// bundle byte-identical, so the exporter must not constrain its schema.
#[derive(Deserialize)]
struct StoredRecordProbe {
    key_id: String,
    #[serde(default)]
    at_rest_protection: StoredKeyProtection,
}

struct CollectedRecord {
    key_id: String,
    protection: StoredKeyProtection,
    /// Raw record bytes exactly as stored. Zeroized on drop because
    /// plaintext-dev-only records embed key material.
    raw: Zeroizing<Vec<u8>>,
}

struct CollectedSnapshot {
    records: Vec<CollectedRecord>,
    salt: Option<Vec<u8>>,
}

/// Export the Local backend's key directory as a sealed backup bundle.
///
/// The directory scan runs under the export fence, so concurrent
/// create/update/delete operations are either fully included or fully
/// excluded — never half a record. Encryption and bundle writing happen after
/// the fence is released to keep it short.
///
/// Returns the sealed manifest that was written to the bundle.
pub async fn export_local_backup(
    client: &LocalKmsClient,
    kek: &BackupKek,
    request: &LocalBackupExportRequest,
) -> Result<BackupManifest> {
    request.validate()?;
    prepare_destination(&request.destination).await?;

    let snapshot = collect_snapshot(client).await?;
    if snapshot.records.is_empty() {
        return Err(KmsError::invalid_operation(
            "Local backup export found no key records; refusing to publish an empty bundle",
        ));
    }

    let has_encrypted = snapshot
        .records
        .iter()
        .any(|record| record.protection == StoredKeyProtection::EncryptedMasterKey);
    if has_encrypted && snapshot.salt.is_none() {
        return Err(KmsError::invalid_operation(
            "key directory contains encrypted-master-key records but the master key salt file is missing; \
             the bundle would be unrestorable",
        ));
    }

    // The verifier lets a restore detect a wrong operator-supplied master key
    // before touching any target state; dev-mode directories have no master
    // key to verify.
    let master_key_verifier = match client.configured_master_key() {
        Some(master_key) => Some(compute_master_key_verifier(master_key, snapshot.salt.as_deref(), &request.backup_id)?),
        None => None,
    };

    let manifest = build_and_write_bundle(kek, request, &snapshot, master_key_verifier).await?;
    Ok(manifest)
}

/// Read and fully validate the manifest of a bundle directory, whatever
/// backend produced it.
///
/// A directory without a manifest is an interrupted export: the manifest is
/// written last, so its absence means the bundle never sealed. The manifest
/// file name and framing are bundle-wide, not Local-specific, so consumers of
/// other backends' bundles read them through here too.
pub async fn read_bundle_manifest(bundle_dir: &Path) -> Result<BackupManifest> {
    let manifest_path = bundle_dir.join(LOCAL_BUNDLE_MANIFEST_FILE);
    let bytes = match fs::read(&manifest_path).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(BackupError::incomplete_bundle("bundle has no manifest; the export never sealed it").into());
        }
        Err(error) => return Err(error.into()),
    };
    Ok(BackupManifest::decode(&bytes)?)
}

/// Read and fully validate the manifest of a *local* bundle directory.
pub async fn read_local_bundle_manifest(bundle_dir: &Path) -> Result<BackupManifest> {
    let manifest = read_bundle_manifest(bundle_dir).await?;
    if manifest.backend != BackupBackendKind::Local {
        return Err(
            BackupError::corrupted(format!("bundle manifest declares backend {:?}, expected Local", manifest.backend)).into(),
        );
    }
    Ok(manifest)
}

/// Read, verify, and decrypt one artifact of a local bundle.
///
/// Fail-closed order: KEK identity, artifact presence, declared length,
/// encrypted digest, then AEAD authentication. The returned plaintext is
/// zeroized on drop.
pub async fn decrypt_bundle_artifact(
    bundle_dir: &Path,
    manifest: &BackupManifest,
    descriptor: &ArtifactDescriptor,
    kek: &BackupKek,
) -> Result<Zeroizing<Vec<u8>>> {
    manifest.backup_kek.ensure_matches(&kek.kek_id, kek.kek_version)?;
    if descriptor.aead_algorithm != AeadAlgorithm::Aes256Gcm {
        return Err(KmsError::unsupported_algorithm(format!(
            "{:?} (local bundles are produced with AES-256-GCM)",
            descriptor.aead_algorithm
        )));
    }

    let artifact_path = bundle_dir.join(&descriptor.path);
    let payload = match fs::read(&artifact_path).await {
        Ok(payload) => payload,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(BackupError::missing_artifact(descriptor.path.clone()).into());
        }
        Err(error) => return Err(error.into()),
    };

    decrypt_artifact_payload(&manifest.backup_id, manifest.snapshot_generation, descriptor, kek, &payload)
}

/// Verify and decrypt one artifact payload already read into memory.
///
/// Fail-closed order: declared length, encrypted digest, nonce framing, then
/// AEAD authentication. Shared by [`decrypt_bundle_artifact`] and the export
/// pre-seal probe so producer and consumer can never drift on the framing.
fn decrypt_artifact_payload(
    backup_id: &str,
    snapshot_generation: u64,
    descriptor: &ArtifactDescriptor,
    kek: &BackupKek,
    payload: &[u8],
) -> Result<Zeroizing<Vec<u8>>> {
    if (payload.len() as u64) < descriptor.len {
        return Err(BackupError::truncated(format!(
            "artifact '{}' is {} bytes, manifest declares {}",
            descriptor.path,
            payload.len(),
            descriptor.len
        ))
        .into());
    }
    if payload.len() as u64 != descriptor.len {
        return Err(BackupError::corrupted(format!(
            "artifact '{}' is {} bytes, manifest declares {}",
            descriptor.path,
            payload.len(),
            descriptor.len
        ))
        .into());
    }
    if ContentDigest::sha256_of(payload) != descriptor.encrypted_digest {
        return Err(BackupError::corrupted(format!("artifact '{}' does not match its manifest digest", descriptor.path)).into());
    }
    if payload.len() < AEAD_NONCE_LEN {
        return Err(BackupError::corrupted(format!("artifact '{}' is too short to carry a nonce", descriptor.path)).into());
    }

    let (nonce_bytes, ciphertext) = payload.split_at(AEAD_NONCE_LEN);
    let mut nonce = [0u8; AEAD_NONCE_LEN];
    nonce.copy_from_slice(nonce_bytes);
    let aad = artifact_aad(backup_id, snapshot_generation, &descriptor.path);
    let plaintext = kek
        .cipher()
        .decrypt(
            &Nonce::from(nonce),
            Payload {
                msg: ciphertext,
                aad: &aad,
            },
        )
        .map_err(|_| {
            KmsError::from(BackupError::corrupted(format!(
                "artifact '{}' failed authenticated decryption under the supplied backup KEK",
                descriptor.path
            )))
        })?;
    Ok(Zeroizing::new(plaintext))
}

/// Scan the key directory under the export fence.
async fn collect_snapshot(client: &LocalKmsClient) -> Result<CollectedSnapshot> {
    let _fence = client.acquire_export_fence().await;

    let mut records = Vec::new();
    let mut entries = fs::read_dir(client.key_directory()).await?;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if !path.extension().is_some_and(|extension| extension == "key") {
            continue;
        }
        let stem = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .ok_or_else(|| KmsError::configuration_error("Local KMS key file name must be valid UTF-8"))?
            .to_string();

        let raw = Zeroizing::new(fs::read(&path).await?);
        // Any unreadable record aborts the export: a bundle silently missing
        // one key is worse than no bundle at all. The protection marker is
        // classified first so a record from a newer build keeps its own
        // verdict — an operator who reads "material corrupt" starts a
        // disaster recovery for what is only a version mismatch.
        let unknown_marker = unknown_protection_marker(&raw)
            .map_err(|error| KmsError::material_corrupt(&stem, format!("stored key record is not valid JSON: {error}")))?;
        if let Some(version) = unknown_marker {
            return Err(KmsError::unsupported_format_version(&stem, version));
        }
        let probe: StoredRecordProbe = serde_json::from_slice(&raw)
            .map_err(|error| KmsError::material_corrupt(&stem, format!("stored key record does not deserialize: {error}")))?;
        if probe.key_id != stem {
            return Err(KmsError::invalid_key(format!(
                "Local KMS key file identity mismatch: expected {stem:?}, found {:?}",
                probe.key_id
            )));
        }

        records.push(CollectedRecord {
            key_id: stem,
            protection: probe.at_rest_protection,
            raw,
        });
    }

    let salt_path = client.master_key_salt_file();
    let salt = match fs::read(&salt_path).await {
        Ok(bytes) => Some(bytes),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };

    records.sort_by(|a, b| a.key_id.cmp(&b.key_id));
    Ok(CollectedSnapshot { records, salt })
}

async fn build_and_write_bundle(
    kek: &BackupKek,
    request: &LocalBackupExportRequest,
    snapshot: &CollectedSnapshot,
    master_key_verifier: Option<String>,
) -> Result<BackupManifest> {
    let mut artifacts = Vec::with_capacity(snapshot.records.len() + 1);
    for record in &snapshot.records {
        let artifact_path = format!("{KEYS_DIR}/{}.key.enc", record.key_id);
        let descriptor = encrypt_and_write_artifact(kek, request, ArtifactKind::KeyMaterial, &artifact_path, &record.raw).await?;
        artifacts.push(descriptor);
    }
    if let Some(salt) = &snapshot.salt {
        let descriptor = encrypt_and_write_artifact(kek, request, ArtifactKind::MasterKeySalt, SALT_ARTIFACT_PATH, salt).await?;
        artifacts.push(descriptor);
    }
    if let Some(config) = &request.sanitized_config {
        let descriptor = encrypt_and_write_artifact(kek, request, ArtifactKind::KmsConfig, CONFIG_ARTIFACT_PATH, config).await?;
        artifacts.push(descriptor);
    }

    // Make the artifact directory entries durable before sealing: the sealed
    // manifest must never survive a crash that its artifacts did not.
    fsync_dir(&request.destination.join(KEYS_DIR)).await?;
    fsync_dir(&request.destination.join(ARTIFACTS_DIR)).await?;

    let manifest = BackupManifest {
        format_version: BackupManifest::FORMAT_VERSION,
        backup_id: request.backup_id.clone(),
        // Normalized to UTC so the stored spelling is host-independent: the
        // local zone's name (or a POSIX TZ string in minimal containers) has
        // no business inside a portable bundle.
        created_at: Zoned::now().with_time_zone(jiff::tz::TimeZone::UTC),
        rustfs_version: request.rustfs_version.clone(),
        deployment_identity: request.deployment_identity.clone(),
        backend: BackupBackendKind::Local,
        at_rest_protection: weakest_observed_protection(&snapshot.records),
        responsibility: BackupResponsibility::FullMaterial,
        snapshot_generation: request.snapshot_generation,
        backup_kek: kek.descriptor(),
        artifacts,
        local_kdf: Some(local_kdf_descriptor(snapshot, master_key_verifier)),
        external_references: None,
        key_versions: None,
        capability_discovery: None,
        completeness: CompletenessState::InProgress,
        manifest_digest: ContentDigest {
            algorithm: DigestAlgorithm::Sha256,
            hex: String::new(),
        },
    };
    let manifest = manifest.seal()?;
    let manifest_bytes = manifest.encode()?;

    write_new_file(&request.destination.join(LOCAL_BUNDLE_MANIFEST_FILE), &manifest_bytes).await?;
    fsync_dir(&request.destination).await?;
    Ok(manifest)
}

/// Encrypt one artifact, write it durably, and re-read it to verify the
/// digest before it is allowed into the manifest.
async fn encrypt_and_write_artifact(
    kek: &BackupKek,
    request: &LocalBackupExportRequest,
    kind: ArtifactKind,
    artifact_path: &str,
    plaintext: &[u8],
) -> Result<ArtifactDescriptor> {
    let mut nonce = [0u8; AEAD_NONCE_LEN];
    rand::rng().fill(&mut nonce[..]);
    let aad = artifact_aad(&request.backup_id, request.snapshot_generation, artifact_path);
    let ciphertext = kek
        .cipher()
        .encrypt(
            &Nonce::from(nonce),
            Payload {
                msg: plaintext,
                aad: &aad,
            },
        )
        .map_err(|error| KmsError::cryptographic_error("backup_artifact_encrypt", error.to_string()))?;

    let mut payload = Vec::with_capacity(AEAD_NONCE_LEN + ciphertext.len());
    payload.extend_from_slice(&nonce);
    payload.extend_from_slice(&ciphertext);

    let absolute_path = request.destination.join(artifact_path);
    write_new_file(&absolute_path, &payload).await?;

    // Verify what actually landed on disk, not the in-memory buffer.
    let written = fs::read(&absolute_path).await?;
    let digest = ContentDigest::sha256_of(&written);
    if written != payload {
        return Err(KmsError::internal_error(format!(
            "bundle artifact '{artifact_path}' read back differently than written"
        )));
    }

    let descriptor = ArtifactDescriptor {
        kind,
        path: artifact_path.to_string(),
        len: payload.len() as u64,
        aead_algorithm: AeadAlgorithm::Aes256Gcm,
        encrypted_digest: digest,
    };

    // Pre-seal decryption probe: digest equality only proves the ciphertext
    // landed intact; this proves the stored artifact actually opens under the
    // backup KEK and AAD binding before the manifest may reference it.
    let reopened = decrypt_artifact_payload(&request.backup_id, request.snapshot_generation, &descriptor, kek, &written)?;
    if reopened.as_slice() != plaintext {
        return Err(KmsError::internal_error(format!(
            "bundle artifact '{artifact_path}' failed the pre-seal decryption probe"
        )));
    }

    Ok(descriptor)
}

/// AAD binding an artifact to its bundle identity and path. A JSON tuple
/// gives unambiguous field boundaries without a hand-rolled framing format.
pub(crate) fn artifact_aad(backup_id: &str, snapshot_generation: u64, artifact_path: &str) -> Vec<u8> {
    serde_json::to_vec(&(BUNDLE_AAD_CONTEXT, backup_id, snapshot_generation, artifact_path))
        .expect("AAD tuple of strings and integers always serializes")
}

/// Compute the opaque one-way master-key verifier recorded in the manifest:
/// `<scheme-prefix>` + `hex(SHA-256(json(context, backup_id) || derived_key))`.
///
/// The derivation follows the directory's KDF state: Argon2id over the
/// persistent salt when one exists, the legacy SHA-256 derivation otherwise.
/// The verifier is not an offline-guessing oracle: computing a candidate
/// requires the salt, which exists only inside the KEK-sealed bundle, and a
/// party holding the KEK already has the strictly stronger oracle of the
/// artifact AEAD tags — while every guess still pays the full Argon2id cost.
/// Binding `backup_id` prevents cross-bundle fingerprint correlation.
pub(crate) fn compute_master_key_verifier(master_key: &str, salt: Option<&[u8]>, backup_id: &str) -> Result<String> {
    let framing = serde_json::to_vec(&(MASTER_KEY_VERIFIER_CONTEXT, backup_id))
        .expect("verifier framing tuple of strings always serializes");
    let (prefix, derived) = match salt {
        Some(salt) => (MASTER_KEY_VERIFIER_ARGON2ID_PREFIX, LocalKmsClient::derive_master_key(master_key, salt)?),
        None => (MASTER_KEY_VERIFIER_LEGACY_PREFIX, LocalKmsClient::derive_legacy_master_key(master_key)?),
    };
    let mut hasher = Sha256::new();
    hasher.update(&framing);
    hasher.update(derived.as_slice());
    Ok(format!("{prefix}{}", hex::encode(hasher.finalize())))
}

/// The bundle-level protection label is the weakest state observed across
/// records: any plaintext-dev-only record marks the whole bundle, then any
/// legacy-unspecified marker (unknown until read), and only a uniformly
/// encrypted directory is labeled encrypted-master-key.
fn weakest_observed_protection(records: &[CollectedRecord]) -> AtRestProtection {
    let mut has_legacy = false;
    for record in records {
        match record.protection {
            StoredKeyProtection::PlaintextDevOnly => return AtRestProtection::PlaintextDevOnly,
            StoredKeyProtection::LegacyUnspecified => has_legacy = true,
            StoredKeyProtection::EncryptedMasterKey => {}
        }
    }
    if has_legacy {
        AtRestProtection::LegacyUnspecified
    } else {
        AtRestProtection::EncryptedMasterKey
    }
}

fn local_kdf_descriptor(snapshot: &CollectedSnapshot, master_key_verifier: Option<String>) -> LocalKdfDescriptor {
    let mut modes = Vec::new();
    for (marker, mode) in [
        (StoredKeyProtection::EncryptedMasterKey, AtRestProtection::EncryptedMasterKey),
        (StoredKeyProtection::PlaintextDevOnly, AtRestProtection::PlaintextDevOnly),
        (StoredKeyProtection::LegacyUnspecified, AtRestProtection::LegacyUnspecified),
    ] {
        if snapshot.records.iter().any(|record| record.protection == marker) {
            modes.push(mode);
        }
    }

    // With a salt on disk the backend derives via Argon2id; without one only
    // the pre-beta.9 SHA-256 derivation can apply. For plaintext-only
    // directories the derivation is informational.
    let derivation = if snapshot.salt.is_some() {
        LocalKeyDerivation::current_argon2id()
    } else {
        LocalKeyDerivation::LegacySha256
    };

    LocalKdfDescriptor {
        derivation,
        protection_modes: modes,
        master_key_verifier,
    }
}

async fn prepare_destination(destination: &Path) -> Result<()> {
    if fs::try_exists(destination).await? {
        let mut entries = fs::read_dir(destination)
            .await
            .map_err(|error| KmsError::invalid_operation(format!("backup destination is not a readable directory: {error}")))?;
        if entries.next_entry().await?.is_some() {
            return Err(KmsError::invalid_operation(
                "backup destination directory is not empty; refusing to mix bundles",
            ));
        }
    }
    fs::create_dir_all(destination.join(KEYS_DIR)).await?;
    Ok(())
}

async fn write_new_file(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .await
        .map_err(|error| KmsError::io_error(format!("failed to create bundle file {}: {error}", path.display())))?;
    file.write_all(bytes).await?;
    file.sync_all().await?;
    Ok(())
}

/// Fsync a directory so freshly created bundle entries survive power loss.
/// No-op on non-Unix platforms where directories cannot be opened for
/// syncing (mirrors the local backend's durable commit helper). Shared with
/// the restore module for the same durability points on the target side.
pub(crate) async fn fsync_dir(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        let path = path.to_path_buf();
        tokio::task::spawn_blocking(move || std::fs::File::open(&path)?.sync_all())
            .await
            .map_err(|error| KmsError::io_error(error.to_string()))??;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::LocalConfig;
    use std::sync::Arc;
    use tempfile::TempDir;

    async fn encrypted_client() -> (LocalKmsClient, TempDir) {
        let temp = TempDir::new().expect("temp dir");
        let client = LocalKmsClient::new(LocalConfig {
            key_dir: temp.path().to_path_buf(),
            master_key: Some("test-master-key".to_string()),
            file_permissions: Some(0o600),
        })
        .await
        .expect("client should initialize");
        (client, temp)
    }

    async fn dev_client() -> (LocalKmsClient, TempDir) {
        let temp = TempDir::new().expect("temp dir");
        let client = LocalKmsClient::new(LocalConfig {
            key_dir: temp.path().to_path_buf(),
            master_key: None,
            file_permissions: Some(0o600),
        })
        .await
        .expect("client should initialize");
        (client, temp)
    }

    fn test_kek() -> BackupKek {
        BackupKek::new("backup-kek-test", 1, [0x42; 32]).expect("kek")
    }

    fn export_request(destination: PathBuf) -> LocalBackupExportRequest {
        LocalBackupExportRequest {
            backup_id: "backup-0001".to_string(),
            deployment_identity: "deployment-test".to_string(),
            rustfs_version: "1.0.0-test".to_string(),
            snapshot_generation: 7,
            destination,
            sanitized_config: None,
        }
    }

    fn walk_files(dir: &Path, out: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(dir).expect("read dir") {
            let path = entry.expect("dir entry").path();
            if path.is_dir() {
                walk_files(&path, out);
            } else {
                out.push(path);
            }
        }
    }

    fn contains_subslice(haystack: &[u8], needle: &[u8]) -> bool {
        !needle.is_empty() && haystack.windows(needle.len()).any(|window| window == needle)
    }

    #[tokio::test]
    async fn export_round_trips_and_decrypts_to_source_records() {
        let (client, _key_dir) = encrypted_client().await;
        client.create_key("alpha", "AES_256", None).await.expect("create alpha");
        client.create_key("beta", "AES_256", None).await.expect("create beta");

        let bundle = TempDir::new().expect("bundle dir");
        let destination = bundle.path().join("bundle");
        let kek = test_kek();
        let manifest = export_local_backup(&client, &kek, &export_request(destination.clone()))
            .await
            .expect("export should succeed");

        assert_eq!(manifest.backend, BackupBackendKind::Local);
        assert_eq!(manifest.responsibility, BackupResponsibility::FullMaterial);
        assert_eq!(manifest.at_rest_protection, AtRestProtection::EncryptedMasterKey);
        assert_eq!(manifest.snapshot_generation, 7);
        let kdf = manifest.local_kdf.as_ref().expect("local kdf descriptor");
        assert_eq!(kdf.derivation, LocalKeyDerivation::current_argon2id());
        assert_eq!(kdf.protection_modes, vec![AtRestProtection::EncryptedMasterKey]);

        // alpha, beta (sorted), then the salt artifact.
        assert_eq!(manifest.artifacts.len(), 3);
        assert_eq!(manifest.artifacts[0].path, "artifacts/keys/alpha.key.enc");
        assert_eq!(manifest.artifacts[1].path, "artifacts/keys/beta.key.enc");
        assert_eq!(manifest.artifacts[2].kind, ArtifactKind::MasterKeySalt);

        let reread = read_local_bundle_manifest(&destination)
            .await
            .expect("manifest should decode");
        assert_eq!(reread, manifest);

        for (artifact, key_id) in [(&manifest.artifacts[0], "alpha"), (&manifest.artifacts[1], "beta")] {
            let decrypted = decrypt_bundle_artifact(&destination, &manifest, artifact, &kek)
                .await
                .expect("artifact should decrypt");
            let source = fs::read(client.key_directory().join(format!("{key_id}.key")))
                .await
                .expect("source record");
            assert_eq!(decrypted.as_slice(), source.as_slice(), "record {key_id} must round-trip verbatim");
        }

        let salt = decrypt_bundle_artifact(&destination, &manifest, &manifest.artifacts[2], &kek)
            .await
            .expect("salt should decrypt");
        let source_salt = fs::read(client.master_key_salt_file()).await.expect("source salt");
        assert_eq!(salt.as_slice(), source_salt.as_slice());
    }

    #[tokio::test]
    async fn plaintext_dev_only_material_is_rewrapped_and_absent_from_bundle() {
        let (client, _key_dir) = dev_client().await;
        client.create_key("dev-key", "AES_256", None).await.expect("create key");

        let material = client
            .decrypt_key_material_for_export("dev-key")
            .await
            .expect("material should be readable");
        let source_record = fs::read(client.key_directory().join("dev-key.key")).await.expect("record");
        let record_json: serde_json::Value = serde_json::from_slice(&source_record).expect("record parses");
        let material_base64 = record_json
            .get("encrypted_key_material")
            .and_then(|value| value.as_str())
            .expect("material field")
            .to_string();

        let bundle = TempDir::new().expect("bundle dir");
        let destination = bundle.path().join("bundle");
        let kek = test_kek();
        let manifest = export_local_backup(&client, &kek, &export_request(destination.clone()))
            .await
            .expect("export should succeed");

        assert_eq!(manifest.at_rest_protection, AtRestProtection::PlaintextDevOnly);
        let kdf = manifest.local_kdf.as_ref().expect("local kdf descriptor");
        assert_eq!(kdf.protection_modes, vec![AtRestProtection::PlaintextDevOnly]);
        assert_eq!(kdf.derivation, LocalKeyDerivation::LegacySha256);
        assert!(
            !manifest.artifacts.iter().any(|a| a.kind == ArtifactKind::MasterKeySalt),
            "dev-mode directory has no salt to bundle"
        );

        // Byte-level: neither the raw material nor its base64 form may appear
        // anywhere in the bundle. The mandatory KEK re-wrap is what hides it.
        let mut files = Vec::new();
        walk_files(&destination, &mut files);
        assert!(!files.is_empty());
        for file in files {
            let bytes = std::fs::read(&file).expect("bundle file");
            assert!(
                !contains_subslice(&bytes, material.as_ref()),
                "raw key material leaked into {}",
                file.display()
            );
            assert!(
                !contains_subslice(&bytes, material_base64.as_bytes()),
                "base64 key material leaked into {}",
                file.display()
            );
        }

        // The wrapped record still round-trips for restore.
        let decrypted = decrypt_bundle_artifact(&destination, &manifest, &manifest.artifacts[0], &kek)
            .await
            .expect("artifact should decrypt");
        assert_eq!(decrypted.as_slice(), source_record.as_slice());
    }

    #[tokio::test]
    async fn export_records_a_master_key_verifier_matching_recomputation() {
        let (client, _key_dir) = encrypted_client().await;
        client.create_key("verified", "AES_256", None).await.expect("create key");

        let bundle = TempDir::new().expect("bundle dir");
        let manifest = export_local_backup(&client, &test_kek(), &export_request(bundle.path().join("bundle")))
            .await
            .expect("export should succeed");

        let verifier = manifest
            .local_kdf
            .as_ref()
            .expect("local kdf descriptor")
            .master_key_verifier
            .as_deref()
            .expect("encrypted bundles must record a verifier");
        assert!(verifier.starts_with(MASTER_KEY_VERIFIER_ARGON2ID_PREFIX), "got {verifier:?}");

        // The verifier is a pure function of (master key, salt, backup id):
        // the restore side recomputes it from the operator-supplied key and
        // the bundled salt.
        let salt = fs::read(client.master_key_salt_file()).await.expect("salt");
        let recomputed = compute_master_key_verifier("test-master-key", Some(&salt), "backup-0001").expect("recompute");
        assert_eq!(recomputed, verifier);
        let wrong_key = compute_master_key_verifier("wrong-master-key", Some(&salt), "backup-0001").expect("recompute");
        assert_ne!(wrong_key, verifier, "a different master key must change the verifier");
        let other_bundle = compute_master_key_verifier("test-master-key", Some(&salt), "backup-0002").expect("recompute");
        assert_ne!(other_bundle, verifier, "verifiers must be bundle-bound");
    }

    #[tokio::test]
    async fn export_fence_blocks_writers_until_released() {
        let (client, _key_dir) = encrypted_client().await;
        client.create_key("existing", "AES_256", None).await.expect("create key");
        let client = Arc::new(client);

        let fence = client.acquire_export_fence().await;

        let writer = {
            let client = Arc::clone(&client);
            tokio::spawn(async move {
                client.create_key("new-key", "AES_256", None).await.expect("create");
                client.disable_key("existing", None).await.expect("disable");
            })
        };

        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
        assert!(!writer.is_finished(), "writers must stay blocked while the export fence is held");

        drop(fence);
        writer.await.expect("writer should finish after fence release");
        assert!(
            fs::try_exists(client.key_directory().join("new-key.key"))
                .await
                .expect("exists")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_writers_yield_complete_records() {
        let (client, _key_dir) = encrypted_client().await;
        for index in 0..5 {
            client
                .create_key(&format!("seed-{index}"), "AES_256", None)
                .await
                .expect("seed key");
        }
        let client = Arc::new(client);

        let writer = {
            let client = Arc::clone(&client);
            tokio::spawn(async move {
                for index in 0..30 {
                    client
                        .create_key(&format!("concurrent-{index}"), "AES_256", None)
                        .await
                        .expect("create");
                    let target = format!("seed-{}", index % 5);
                    if index % 2 == 0 {
                        client.disable_key(&target, None).await.expect("disable");
                    } else {
                        client.enable_key(&target, None).await.expect("enable");
                    }
                }
            })
        };

        let bundle = TempDir::new().expect("bundle dir");
        let destination = bundle.path().join("bundle");
        let kek = test_kek();
        let manifest = export_local_backup(&client, &kek, &export_request(destination.clone()))
            .await
            .expect("export should succeed under concurrent writers");
        writer.await.expect("writer task");

        // Whatever subset of writers landed before the fence, every record in
        // the bundle must be complete: parseable, self-identifying, and with
        // non-empty material. No torn records, no half-updates.
        let reread = read_local_bundle_manifest(&destination).await.expect("manifest decodes");
        assert_eq!(reread, manifest);
        for artifact in manifest.artifacts.iter().filter(|a| a.kind == ArtifactKind::KeyMaterial) {
            let record = decrypt_bundle_artifact(&destination, &manifest, artifact, &kek)
                .await
                .expect("record decrypts");
            let value: serde_json::Value = serde_json::from_slice(&record).expect("record is complete JSON");
            let key_id = value.get("key_id").and_then(|v| v.as_str()).expect("key_id present");
            assert_eq!(artifact.path, format!("artifacts/keys/{key_id}.key.enc"));
            let material = value
                .get("encrypted_key_material")
                .and_then(|v| v.as_str())
                .expect("material present");
            assert!(!material.is_empty());
        }
    }

    #[tokio::test]
    async fn tampered_and_truncated_bundles_fail_closed() {
        let (client, _key_dir) = encrypted_client().await;
        client.create_key("victim", "AES_256", None).await.expect("create key");

        let bundle = TempDir::new().expect("bundle dir");
        let destination = bundle.path().join("bundle");
        let kek = test_kek();
        let manifest = export_local_backup(&client, &kek, &export_request(destination.clone()))
            .await
            .expect("export should succeed");
        let artifact = &manifest.artifacts[0];
        let artifact_file = destination.join(&artifact.path);
        let original_artifact = std::fs::read(&artifact_file).expect("artifact bytes");

        // Tampered artifact byte: digest verification rejects it.
        let mut tampered = original_artifact.clone();
        let last = tampered.len() - 1;
        tampered[last] ^= 0x01;
        std::fs::write(&artifact_file, &tampered).expect("write tampered");
        let error = decrypt_bundle_artifact(&destination, &manifest, artifact, &kek)
            .await
            .expect_err("tampered artifact must be rejected");
        assert!(matches!(error, KmsError::Backup(BackupError::Corrupted { .. })), "got {error:?}");

        // Truncated artifact: typed truncation error.
        std::fs::write(&artifact_file, &original_artifact[..original_artifact.len() - 4]).expect("truncate");
        let error = decrypt_bundle_artifact(&destination, &manifest, artifact, &kek)
            .await
            .expect_err("truncated artifact must be rejected");
        assert!(matches!(error, KmsError::Backup(BackupError::Truncated { .. })), "got {error:?}");
        std::fs::write(&artifact_file, &original_artifact).expect("restore artifact");

        // Tampered manifest (generation flip): sealed digest mismatch.
        let manifest_file = destination.join(LOCAL_BUNDLE_MANIFEST_FILE);
        let original_manifest = std::fs::read(&manifest_file).expect("manifest bytes");
        let tampered_manifest = String::from_utf8(original_manifest.clone())
            .expect("manifest is utf-8")
            .replace("\"snapshot_generation\":7", "\"snapshot_generation\":8");
        assert_ne!(tampered_manifest.as_bytes(), original_manifest.as_slice(), "tamper must apply");
        std::fs::write(&manifest_file, tampered_manifest).expect("write tampered manifest");
        let error = read_local_bundle_manifest(&destination)
            .await
            .expect_err("tampered manifest must be rejected");
        assert!(matches!(error, KmsError::Backup(BackupError::Corrupted { .. })), "got {error:?}");

        // Truncated manifest.
        std::fs::write(&manifest_file, &original_manifest[..original_manifest.len() / 2]).expect("truncate manifest");
        let error = read_local_bundle_manifest(&destination)
            .await
            .expect_err("truncated manifest must be rejected");
        assert!(matches!(error, KmsError::Backup(BackupError::Truncated { .. })), "got {error:?}");

        // Missing manifest: the bundle never sealed.
        std::fs::remove_file(&manifest_file).expect("remove manifest");
        let error = read_local_bundle_manifest(&destination)
            .await
            .expect_err("bundle without manifest must be rejected");
        assert!(matches!(error, KmsError::Backup(BackupError::IncompleteBundle { .. })), "got {error:?}");
    }

    #[tokio::test]
    async fn wrong_kek_is_rejected_before_decryption() {
        let (client, _key_dir) = encrypted_client().await;
        client.create_key("victim", "AES_256", None).await.expect("create key");

        let bundle = TempDir::new().expect("bundle dir");
        let destination = bundle.path().join("bundle");
        let kek = test_kek();
        let manifest = export_local_backup(&client, &kek, &export_request(destination.clone()))
            .await
            .expect("export should succeed");
        let artifact = &manifest.artifacts[0];

        let wrong_id = BackupKek::new("other-kek", 1, [0x42; 32]).expect("kek");
        let error = decrypt_bundle_artifact(&destination, &manifest, artifact, &wrong_id)
            .await
            .expect_err("mismatched KEK id must be rejected");
        assert!(matches!(error, KmsError::Backup(BackupError::WrongKek { .. })), "got {error:?}");

        let wrong_version = BackupKek::new("backup-kek-test", 2, [0x42; 32]).expect("kek");
        let error = decrypt_bundle_artifact(&destination, &manifest, artifact, &wrong_version)
            .await
            .expect_err("mismatched KEK version must be rejected");
        assert!(matches!(error, KmsError::Backup(BackupError::WrongKek { .. })), "got {error:?}");

        // Right identity, wrong material: AEAD authentication fails closed.
        let wrong_material = BackupKek::new("backup-kek-test", 1, [0x24; 32]).expect("kek");
        let error = decrypt_bundle_artifact(&destination, &manifest, artifact, &wrong_material)
            .await
            .expect_err("wrong KEK material must be rejected");
        assert!(matches!(error, KmsError::Backup(BackupError::Corrupted { .. })), "got {error:?}");
    }

    #[tokio::test]
    async fn missing_salt_with_encrypted_records_fails_export() {
        let (client, _key_dir) = encrypted_client().await;
        client.create_key("victim", "AES_256", None).await.expect("create key");
        fs::remove_file(client.master_key_salt_file()).await.expect("remove salt");

        let bundle = TempDir::new().expect("bundle dir");
        let error = export_local_backup(&client, &test_kek(), &export_request(bundle.path().join("bundle")))
            .await
            .expect_err("export without salt must fail");
        assert!(matches!(error, KmsError::InvalidOperation { .. }), "got {error:?}");
        assert!(error.to_string().contains("salt"), "got {error}");
    }

    #[tokio::test]
    async fn refuses_empty_key_dir_and_nonempty_destination() {
        let (client, _key_dir) = dev_client().await;
        let bundle = TempDir::new().expect("bundle dir");
        let error = export_local_backup(&client, &test_kek(), &export_request(bundle.path().join("bundle")))
            .await
            .expect_err("empty key dir must not produce a bundle");
        assert!(matches!(error, KmsError::InvalidOperation { .. }), "got {error:?}");

        client.create_key("dev-key", "AES_256", None).await.expect("create key");
        let occupied = bundle.path().join("occupied");
        std::fs::create_dir_all(&occupied).expect("mkdir");
        std::fs::write(occupied.join("stale"), b"leftover").expect("occupy");
        let error = export_local_backup(&client, &test_kek(), &export_request(occupied))
            .await
            .expect_err("non-empty destination must be refused");
        assert!(matches!(error, KmsError::InvalidOperation { .. }), "got {error:?}");
    }

    #[tokio::test]
    async fn legacy_records_export_verbatim_with_weakest_protection_label() {
        let (client, _key_dir) = encrypted_client().await;
        client.create_key("modern", "AES_256", None).await.expect("create key");
        client.create_key("legacy-key", "AES_256", None).await.expect("create key");

        // Strip the protection marker to fabricate a pre-beta.9 record, the
        // same way the local backend's own legacy-compat tests do.
        let legacy_path = client.key_directory().join("legacy-key.key");
        let mut record: serde_json::Value =
            serde_json::from_slice(&fs::read(&legacy_path).await.expect("record")).expect("record parses");
        record
            .as_object_mut()
            .expect("record is an object")
            .remove("at_rest_protection");
        let legacy_bytes = serde_json::to_vec_pretty(&record).expect("record serializes");
        fs::write(&legacy_path, &legacy_bytes).await.expect("write legacy record");

        let bundle = TempDir::new().expect("bundle dir");
        let destination = bundle.path().join("bundle");
        let kek = test_kek();
        let manifest = export_local_backup(&client, &kek, &export_request(destination.clone()))
            .await
            .expect("export should succeed");

        assert_eq!(manifest.at_rest_protection, AtRestProtection::LegacyUnspecified);
        let kdf = manifest.local_kdf.as_ref().expect("local kdf descriptor");
        assert_eq!(
            kdf.protection_modes,
            vec![AtRestProtection::EncryptedMasterKey, AtRestProtection::LegacyUnspecified]
        );

        let legacy_artifact = manifest
            .artifacts
            .iter()
            .find(|a| a.path == "artifacts/keys/legacy-key.key.enc")
            .expect("legacy artifact");
        let decrypted = decrypt_bundle_artifact(&destination, &manifest, legacy_artifact, &kek)
            .await
            .expect("legacy artifact decrypts");
        assert_eq!(decrypted.as_slice(), legacy_bytes.as_slice(), "legacy record must travel verbatim");
    }

    #[tokio::test]
    async fn record_identity_mismatch_aborts_export() {
        let (client, _key_dir) = encrypted_client().await;
        client.create_key("good", "AES_256", None).await.expect("create key");
        std::fs::copy(client.key_directory().join("good.key"), client.key_directory().join("evil.key"))
            .expect("plant mismatched record");

        let bundle = TempDir::new().expect("bundle dir");
        let error = export_local_backup(&client, &test_kek(), &export_request(bundle.path().join("bundle")))
            .await
            .expect_err("identity mismatch must abort the export");
        assert!(matches!(error, KmsError::InvalidKey { .. }), "got {error:?}");
    }

    /// A record written by a newer build must abort the export as an
    /// unsupported format, not as corrupt material: the two verdicts send the
    /// operator down completely different runbooks, and only one of them is
    /// true here.
    #[tokio::test]
    async fn record_from_a_newer_build_aborts_export_as_unsupported_format() {
        let (client, _key_dir) = encrypted_client().await;
        client.create_key("alpha", "AES_256", None).await.expect("create key");

        let record_path = client.key_directory().join("alpha.key");
        let mut record: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&record_path).expect("read record")).expect("decode record");
        record["at_rest_protection"] = serde_json::json!("post-quantum-v2");
        std::fs::write(&record_path, serde_json::to_vec_pretty(&record).expect("encode record")).expect("write record");

        let bundle = TempDir::new().expect("bundle dir");
        let error = export_local_backup(&client, &test_kek(), &export_request(bundle.path().join("bundle")))
            .await
            .expect_err("an uninterpretable record must abort the export");
        assert!(
            matches!(&error, KmsError::UnsupportedFormatVersion { key_id, version }
                if key_id == "alpha" && version == "post-quantum-v2"),
            "got {error:?}"
        );
    }
}
