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

//! Versioned backup manifest schema (format version 1).
//!
//! Schema evolution policy: every struct rejects unknown fields, so adding,
//! removing, or reshaping any field requires bumping
//! [`BackupManifest::FORMAT_VERSION`]. Decoders reject unknown versions
//! outright — there is no best-effort read of a manifest this build does not
//! understand.

use crate::backup::capability::{AtRestProtection, BackupBackendKind, BackupResponsibility};
use crate::backup::error::BackupError;
use jiff::Zoned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::path::{Component, Path};

/// AEAD algorithm identifiers for bundle protection.
///
/// This is deliberately not [`crate::types::EncryptionAlgorithm`]: that enum
/// carries S3-facing wire names and the non-AEAD `aws:kms` marker. The backup
/// domain only ever names a concrete AEAD.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AeadAlgorithm {
    /// AES-256-GCM.
    #[serde(rename = "aes-256-gcm")]
    Aes256Gcm,
    /// ChaCha20-Poly1305.
    #[serde(rename = "chacha20-poly1305")]
    ChaCha20Poly1305,
}

/// Digest algorithm identifiers for manifest and artifact digests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DigestAlgorithm {
    /// SHA-256.
    #[serde(rename = "sha-256")]
    Sha256,
}

/// A content digest: algorithm plus lowercase hex value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentDigest {
    /// Digest algorithm.
    pub algorithm: DigestAlgorithm,
    /// Lowercase hex encoding of the digest value.
    pub hex: String,
}

impl ContentDigest {
    /// Hex length of a SHA-256 digest.
    pub const SHA256_HEX_LEN: usize = 64;

    /// Compute the SHA-256 digest of `bytes`.
    pub fn sha256_of(bytes: &[u8]) -> Self {
        Self {
            algorithm: DigestAlgorithm::Sha256,
            hex: hex::encode(Sha256::digest(bytes)),
        }
    }

    /// The placeholder written into the digest slot while computing the
    /// canonical manifest bytes (see [`BackupManifest::compute_digest`]).
    fn placeholder(algorithm: DigestAlgorithm) -> Self {
        Self {
            algorithm,
            hex: String::new(),
        }
    }

    /// Whether the hex value is well-formed for the declared algorithm.
    pub fn is_well_formed(&self) -> bool {
        match self.algorithm {
            DigestAlgorithm::Sha256 => {
                self.hex.len() == Self::SHA256_HEX_LEN
                    && self.hex.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            }
        }
    }
}

/// Identity of the backup KEK protecting a bundle.
///
/// The backup KEK is a trust root separate from the business KMS hierarchy:
/// a bundle must never be encrypted with a key that is itself part of the
/// state being backed up. The manifest records only the KEK identity — never
/// material that could open the bundle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupKekDescriptor {
    /// Stable identifier of the backup KEK.
    pub kek_id: String,
    /// Version of the backup KEK used for this bundle.
    pub kek_version: u32,
    /// AEAD algorithm protecting the bundle artifacts.
    pub aead_algorithm: AeadAlgorithm,
}

impl BackupKekDescriptor {
    /// Fail closed unless the supplied KEK identity matches the one this
    /// bundle was sealed with.
    pub fn ensure_matches(&self, supplied_kek_id: &str, supplied_kek_version: u32) -> Result<(), BackupError> {
        if self.kek_id != supplied_kek_id || self.kek_version != supplied_kek_version {
            return Err(BackupError::WrongKek {
                required_kek_id: self.kek_id.clone(),
                required_kek_version: self.kek_version,
                supplied_kek_id: supplied_kek_id.to_string(),
                supplied_kek_version,
            });
        }
        Ok(())
    }
}

/// Kind of an artifact referenced by the manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ArtifactKind {
    /// Encrypted master key material (all versions of one or more keys).
    KeyMaterial,
    /// Key metadata records (status, usage, timestamps, tags).
    KeyMetadata,
    /// The Local backend's persistent master-key KDF salt.
    MasterKeySalt,
    /// The sanitized KMS configuration. The persisted `KmsConfig` contains
    /// plaintext credentials (Vault token / AppRole secret id); the config
    /// artifact must be the sanitized form and never those secrets.
    KmsConfig,
    /// Reserved: no alias feature exists in the codebase. The name is frozen
    /// so a future format version can use it; format version 1 manifests
    /// containing it are rejected.
    Alias,
    /// Reserved: key policies are accepted on `CreateKeyRequest` but never
    /// consumed. Same rejection rule as [`Self::Alias`].
    Policy,
}

impl ArtifactKind {
    /// Whether this kind is reserved for a future format version.
    pub fn is_reserved(&self) -> bool {
        matches!(self, Self::Alias | Self::Policy)
    }
}

/// One artifact in the bundle payload set.
///
/// Every artifact payload is AEAD-encrypted under the backup KEK regardless
/// of how the source material was protected at rest. In particular,
/// plaintext-dev-only Local key files must never enter a bundle unwrapped.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactDescriptor {
    /// What the artifact contains.
    pub kind: ArtifactKind,
    /// Bundle-relative path of the artifact payload. Absolute paths and path
    /// traversal are rejected.
    pub path: String,
    /// Length of the encrypted payload in bytes.
    pub len: u64,
    /// AEAD algorithm the payload is encrypted with.
    pub aead_algorithm: AeadAlgorithm,
    /// Digest of the encrypted payload bytes (not of the plaintext, so
    /// verification never requires decryption).
    pub encrypted_digest: ContentDigest,
}

/// Key-derivation description for a Local backend bundle.
///
/// Values mirror the constants in `backends/local.rs`; recording them in the
/// manifest lets a restore detect a KDF parameter drift instead of silently
/// deriving a different master key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub enum LocalKeyDerivation {
    /// Argon2id with explicit parameters and a persistent on-disk salt.
    Argon2id {
        /// Argon2 version (0x13 = 19).
        version: u32,
        /// Memory cost in KiB.
        memory_kib: u32,
        /// Iteration count (time cost).
        iterations: u32,
        /// Lane count (parallelism).
        parallelism: u32,
        /// Derived key length in bytes.
        output_len: u32,
        /// Persistent salt length in bytes.
        salt_len: u32,
    },
    /// Pre-beta.9 SHA-256 derivation without a salt. Recorded so legacy key
    /// directories can be restored; new bundles always use Argon2id.
    LegacySha256,
}

impl LocalKeyDerivation {
    /// The derivation currently performed by `backends/local.rs`
    /// (`derive_master_key`): Argon2id v0x13 with the crate's compiled-in
    /// parameters.
    pub fn current_argon2id() -> Self {
        use crate::backends::local::{
            LOCAL_KMS_ARGON2_M_COST_KIB, LOCAL_KMS_ARGON2_P_COST, LOCAL_KMS_ARGON2_T_COST, LOCAL_KMS_MASTER_KEY_LEN,
            LOCAL_KMS_MASTER_KEY_SALT_LEN,
        };

        Self::Argon2id {
            version: 0x13,
            memory_kib: LOCAL_KMS_ARGON2_M_COST_KIB,
            iterations: LOCAL_KMS_ARGON2_T_COST,
            parallelism: LOCAL_KMS_ARGON2_P_COST,
            output_len: LOCAL_KMS_MASTER_KEY_LEN as u32,
            salt_len: LOCAL_KMS_MASTER_KEY_SALT_LEN as u32,
        }
    }

    fn validate(&self) -> Result<(), BackupError> {
        match self {
            Self::Argon2id {
                version,
                memory_kib,
                iterations,
                parallelism,
                output_len,
                salt_len,
            } => {
                if *version == 0 || *memory_kib == 0 || *iterations == 0 || *parallelism == 0 || *salt_len == 0 {
                    return Err(BackupError::corrupted("local KDF descriptor has zero-valued Argon2 parameters"));
                }
                if *output_len != 32 {
                    return Err(BackupError::corrupted("local KDF descriptor must derive a 32-byte key for AES-256"));
                }
                Ok(())
            }
            Self::LegacySha256 => Ok(()),
        }
    }
}

/// Local backend section of the manifest.
///
/// The Local master key itself is outside the backup domain: it is supplied
/// via environment or configuration and the operator must re-supply it before
/// restore. The manifest stores at most an opaque one-way verifier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalKdfDescriptor {
    /// How the master key string is turned into the file-encryption key.
    pub derivation: LocalKeyDerivation,
    /// Protection states observed across the stored key files in this
    /// snapshot (deduplicated). Restricted to the Local rows of the
    /// responsibility matrix.
    pub protection_modes: Vec<AtRestProtection>,
    /// Opaque one-way verifier of the operator-supplied master key, used to
    /// detect a wrong master key before restore touches anything. Never key
    /// material; the exact derivation is defined by the Local export
    /// implementation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub master_key_verifier: Option<String>,
}

impl LocalKdfDescriptor {
    /// Build a descriptor for the current compiled-in derivation.
    pub fn current(protection_modes: Vec<AtRestProtection>, master_key_verifier: Option<String>) -> Self {
        Self {
            derivation: LocalKeyDerivation::current_argon2id(),
            protection_modes,
            master_key_verifier,
        }
    }

    fn validate(&self) -> Result<(), BackupError> {
        self.derivation.validate()?;
        if self.protection_modes.is_empty() {
            return Err(BackupError::corrupted("local KDF descriptor lists no protection modes"));
        }
        let mut seen = BTreeSet::new();
        for mode in &self.protection_modes {
            if BackupResponsibility::for_backend(BackupBackendKind::Local, *mode).is_none() {
                return Err(BackupError::corrupted(format!(
                    "local KDF descriptor lists non-local protection mode {mode:?}"
                )));
            }
            if !seen.insert(format!("{mode:?}")) {
                return Err(BackupError::corrupted(format!(
                    "local KDF descriptor lists protection mode {mode:?} more than once"
                )));
            }
        }
        if self.master_key_verifier.as_deref() == Some("") {
            return Err(BackupError::corrupted("local master key verifier must not be empty when present"));
        }
        Ok(())
    }
}

/// Completeness marker of a bundle.
///
/// A producer writes `in-progress` state (or no manifest at all) until the
/// final integrity checks pass, then seals the bundle as `complete`. Anything
/// other than `complete` must never be restored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CompletenessState {
    /// The bundle never reached its final integrity checks.
    InProgress,
    /// The bundle passed its final integrity checks and was sealed.
    Complete,
}

/// Placeholder for a manifest field whose shape is intentionally not frozen
/// yet.
///
/// Reserved fields hold their name in the schema but may not carry data in
/// format version 1: deserializing any non-null value fails, and
/// [`BackupManifest::validate`] rejects a populated slot. A later format
/// version replaces the slot with the real type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ReservedSlot;

impl<'de> Deserialize<'de> for ReservedSlot {
    fn deserialize<D>(_deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Err(serde::de::Error::custom(
            "reserved manifest field must not carry data in format version 1",
        ))
    }
}

/// Version probe decoded before the full manifest so that version and
/// completeness failures surface as their own typed errors instead of
/// generic decode errors.
#[derive(Deserialize)]
struct ManifestProbe {
    format_version: u32,
    #[serde(default)]
    completeness: Option<serde_json::Value>,
}

/// Versioned backup manifest (format version 1).
///
/// The manifest is the authoritative description of one backup bundle: what
/// was captured, under which snapshot generation, protected by which backup
/// KEK, and which restore responsibility applies. The digest's canonical
/// form is the manifest's JSON value with the digest hex emptied (see
/// [`Self::compute_digest`]); decoders verify it against the raw stored
/// bytes and never re-serialize parsed fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupManifest {
    /// Manifest format version; see [`Self::FORMAT_VERSION`].
    pub format_version: u32,
    /// Unique identifier of this backup.
    pub backup_id: String,
    /// Creation time of the bundle.
    #[serde(with = "crate::time_serde::zoned")]
    pub created_at: Zoned,
    /// RustFS version that produced the bundle.
    pub rustfs_version: String,
    /// Opaque identity of the producing deployment, used by restore preflight
    /// to detect cross-deployment restores. The value source is decided by
    /// the producing implementation; the contract freezes only that it is an
    /// opaque non-empty string.
    pub deployment_identity: String,
    /// Backend the bundle was captured from.
    pub backend: BackupBackendKind,
    /// At-rest protection state observed at snapshot time.
    pub at_rest_protection: AtRestProtection,
    /// Declared restore responsibility. Must equal the responsibility matrix
    /// row for `(backend, at_rest_protection)`; a mismatch is rejected.
    pub responsibility: BackupResponsibility,
    /// Monotonic snapshot generation. All state in one bundle belongs to this
    /// single generation; restore preflight uses it to block rollbacks onto a
    /// target that has already observed a higher generation.
    pub snapshot_generation: u64,
    /// Identity of the backup KEK protecting the bundle.
    pub backup_kek: BackupKekDescriptor,
    /// The bundle payload set.
    pub artifacts: Vec<ArtifactDescriptor>,
    /// Local backend section; required exactly when `backend` is `Local`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_kdf: Option<LocalKdfDescriptor>,
    /// Reserved for the per-key version/envelope inventory defined by the
    /// backlog#1565 contract. May not carry data in format version 1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_versions: Option<ReservedSlot>,
    /// Reserved for the backlog#1571 capability-discovery contract. May not
    /// carry data in format version 1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capability_discovery: Option<ReservedSlot>,
    /// Completeness marker; anything but `complete` is never restorable.
    pub completeness: CompletenessState,
    /// Digest over the canonical manifest bytes (see
    /// [`Self::compute_digest`]); the final integrity anchor of the bundle.
    pub manifest_digest: ContentDigest,
}

impl BackupManifest {
    /// The manifest format version this build reads and writes.
    pub const FORMAT_VERSION: u32 = 1;

    /// Decode and fully validate a manifest from JSON bytes.
    ///
    /// Fail-closed order: truncated or malformed input, then unknown format
    /// version, then a missing completeness marker, then schema decoding
    /// (unknown fields, duplicate fields, missing fields), then digest
    /// verification against the raw input bytes, then semantic validation.
    ///
    /// The digest is verified against the bytes as stored — parsed typed
    /// fields are never re-serialized for verification, so a field whose
    /// string form does not round-trip byte-identically through its parsed
    /// representation (timestamps in environment-dependent time zone
    /// spellings, for example) cannot produce a spurious mismatch.
    pub fn decode(bytes: &[u8]) -> Result<Self, BackupError> {
        let probe: ManifestProbe = serde_json::from_slice(bytes).map_err(map_serde_error)?;
        if probe.format_version != Self::FORMAT_VERSION {
            return Err(BackupError::UnknownVersion {
                found: probe.format_version,
                supported: Self::FORMAT_VERSION,
            });
        }
        if probe.completeness.is_none() {
            return Err(BackupError::incomplete_bundle("manifest has no completeness marker"));
        }
        let manifest: Self = serde_json::from_slice(bytes).map_err(map_serde_error)?;
        manifest.validate_pre_digest()?;
        Self::verify_digest_in_bytes(bytes, &manifest.manifest_digest)?;
        manifest.validate_content()?;
        Ok(manifest)
    }

    /// Serialize a sealed, valid manifest to its canonical JSON bytes.
    ///
    /// Encoding validates first so an unsealed or inconsistent manifest can
    /// never be published.
    pub fn encode(&self) -> Result<Vec<u8>, BackupError> {
        self.validate()?;
        serde_json::to_vec(self).map_err(|error| BackupError::corrupted(format!("manifest serialization failed: {error}")))
    }

    /// Seal the manifest: mark it complete and stamp the canonical digest.
    pub fn seal(mut self) -> Result<Self, BackupError> {
        self.completeness = CompletenessState::Complete;
        self.manifest_digest = self.compute_digest()?;
        Ok(self)
    }

    /// Compute the digest over the canonical manifest form.
    ///
    /// Canonical form: the JSON *value* of the manifest with the digest hex
    /// emptied, serialized compactly by `serde_json`'s value serializer. The
    /// value layer is what makes sealing and decoding agree byte-for-byte:
    /// a decoder recovers the identical value from the raw stored bytes
    /// without round-tripping any typed field through parse-and-reprint.
    pub fn compute_digest(&self) -> Result<ContentDigest, BackupError> {
        let mut unsealed = self.clone();
        unsealed.manifest_digest = ContentDigest::placeholder(self.manifest_digest.algorithm);
        let value = serde_json::to_value(&unsealed)
            .map_err(|error| BackupError::corrupted(format!("manifest canonicalization failed: {error}")))?;
        Self::digest_of_canonical_value(&value, self.manifest_digest.algorithm)
    }

    /// Verify the sealed digest against the current in-memory content.
    ///
    /// This is the producer-side check (sealing and [`Self::encode`]).
    /// Decoders must use the raw stored bytes instead (see [`Self::decode`]):
    /// re-serializing parsed fields is not guaranteed to reproduce the
    /// stored spelling byte-for-byte.
    pub fn verify_digest(&self) -> Result<(), BackupError> {
        if !self.manifest_digest.is_well_formed() {
            return Err(BackupError::corrupted("manifest digest is not a well-formed digest value"));
        }
        if self.compute_digest()? != self.manifest_digest {
            return Err(BackupError::corrupted(
                "manifest digest mismatch: content does not match the sealed digest",
            ));
        }
        Ok(())
    }

    /// Verify a declared digest against raw manifest bytes, normalizing only
    /// through the JSON value layer and emptying the digest slot in place.
    fn verify_digest_in_bytes(bytes: &[u8], declared: &ContentDigest) -> Result<(), BackupError> {
        if !declared.is_well_formed() {
            return Err(BackupError::corrupted("manifest digest is not a well-formed digest value"));
        }
        let mut value: serde_json::Value = serde_json::from_slice(bytes).map_err(map_serde_error)?;
        let Some(slot) = value.get_mut("manifest_digest").and_then(|digest| digest.get_mut("hex")) else {
            return Err(BackupError::corrupted("manifest has no digest slot"));
        };
        *slot = serde_json::Value::String(String::new());
        if Self::digest_of_canonical_value(&value, declared.algorithm)? != *declared {
            return Err(BackupError::corrupted(
                "manifest digest mismatch: content does not match the sealed digest",
            ));
        }
        Ok(())
    }

    fn digest_of_canonical_value(value: &serde_json::Value, algorithm: DigestAlgorithm) -> Result<ContentDigest, BackupError> {
        let canonical = serde_json::to_vec(value)
            .map_err(|error| BackupError::corrupted(format!("manifest canonicalization failed: {error}")))?;
        match algorithm {
            DigestAlgorithm::Sha256 => Ok(ContentDigest::sha256_of(&canonical)),
        }
    }

    /// Look up a required artifact by kind, failing closed when absent.
    pub fn require_artifact(&self, kind: ArtifactKind) -> Result<&ArtifactDescriptor, BackupError> {
        self.artifacts
            .iter()
            .find(|artifact| artifact.kind == kind)
            .ok_or_else(|| BackupError::missing_artifact(artifact_kind_name(kind)))
    }

    /// Validate the full manifest contract against the in-memory content.
    ///
    /// This guards [`Self::encode`], so a producer cannot publish a manifest
    /// a decoder would reject. [`Self::decode`] runs the same checks but
    /// verifies the digest against the raw input bytes instead.
    pub fn validate(&self) -> Result<(), BackupError> {
        self.validate_pre_digest()?;
        self.verify_digest()?;
        self.validate_content()
    }

    /// Checks that must run before any digest verification: an unknown
    /// version or an unsealed bundle is reported as its own typed error, not
    /// as a digest mismatch.
    fn validate_pre_digest(&self) -> Result<(), BackupError> {
        if self.format_version != Self::FORMAT_VERSION {
            return Err(BackupError::UnknownVersion {
                found: self.format_version,
                supported: Self::FORMAT_VERSION,
            });
        }
        if self.completeness != CompletenessState::Complete {
            return Err(BackupError::incomplete_bundle("completeness marker records an in-progress bundle"));
        }
        Ok(())
    }

    /// Semantic validation of everything except version, completeness, and
    /// digest integrity.
    fn validate_content(&self) -> Result<(), BackupError> {
        require_non_empty("backup_id", &self.backup_id)?;
        require_non_empty("rustfs_version", &self.rustfs_version)?;
        require_non_empty("deployment_identity", &self.deployment_identity)?;
        require_non_empty("backup_kek.kek_id", &self.backup_kek.kek_id)?;
        if self.key_versions.is_some() {
            return Err(BackupError::corrupted("reserved field key_versions must not carry data"));
        }
        if self.capability_discovery.is_some() {
            return Err(BackupError::corrupted("reserved field capability_discovery must not carry data"));
        }
        self.validate_responsibility()?;
        self.validate_local_kdf()?;
        self.validate_artifacts()?;
        Ok(())
    }

    fn validate_responsibility(&self) -> Result<(), BackupError> {
        match BackupResponsibility::for_backend(self.backend, self.at_rest_protection) {
            None => Err(BackupError::corrupted(format!(
                "at-rest protection {:?} is not a defined state for backend {:?}",
                self.at_rest_protection, self.backend
            ))),
            Some(expected) if expected != self.responsibility => Err(BackupError::corrupted(format!(
                "declared responsibility {:?} contradicts the matrix row {expected:?} for ({:?}, {:?})",
                self.responsibility, self.backend, self.at_rest_protection
            ))),
            Some(_) => Ok(()),
        }
    }

    fn validate_local_kdf(&self) -> Result<(), BackupError> {
        match (self.backend, &self.local_kdf) {
            (BackupBackendKind::Local, None) => {
                Err(BackupError::corrupted("local backend manifest must carry a local_kdf descriptor"))
            }
            (BackupBackendKind::Local, Some(descriptor)) => descriptor.validate(),
            (_, Some(_)) => Err(BackupError::corrupted("local_kdf descriptor is only valid for the Local backend")),
            (_, None) => Ok(()),
        }
    }

    fn validate_artifacts(&self) -> Result<(), BackupError> {
        let mut paths = BTreeSet::new();
        for artifact in &self.artifacts {
            if artifact.kind.is_reserved() {
                return Err(BackupError::corrupted(format!(
                    "artifact kind {:?} is reserved and not valid in format version 1",
                    artifact.kind
                )));
            }
            validate_bundle_relative_path(&artifact.path)?;
            if artifact.len == 0 {
                return Err(BackupError::corrupted(format!(
                    "artifact '{}' declares a zero-length payload; an AEAD payload is never empty",
                    artifact.path
                )));
            }
            if !artifact.encrypted_digest.is_well_formed() {
                return Err(BackupError::corrupted(format!(
                    "artifact '{}' has a malformed encrypted digest",
                    artifact.path
                )));
            }
            if !paths.insert(artifact.path.as_str()) {
                return Err(BackupError::corrupted(format!(
                    "artifact path '{}' appears more than once",
                    artifact.path
                )));
            }
        }

        let has_key_material = self
            .artifacts
            .iter()
            .any(|artifact| artifact.kind == ArtifactKind::KeyMaterial);
        match self.responsibility {
            BackupResponsibility::FullMaterial => {
                if !has_key_material {
                    return Err(BackupError::missing_artifact(artifact_kind_name(ArtifactKind::KeyMaterial)));
                }
            }
            BackupResponsibility::ReferenceOnly | BackupResponsibility::MetadataPlusExternalRoot => {
                if has_key_material {
                    return Err(BackupError::corrupted(format!(
                        "a {:?} bundle must not embed key material artifacts",
                        self.responsibility
                    )));
                }
            }
        }
        Ok(())
    }
}

fn require_non_empty(field: &str, value: &str) -> Result<(), BackupError> {
    if value.is_empty() {
        return Err(BackupError::corrupted(format!("security-critical field {field} must not be empty")));
    }
    Ok(())
}

fn artifact_kind_name(kind: ArtifactKind) -> String {
    // Wire name of the kind, reusing the serde rename rules so error text and
    // schema never diverge.
    serde_json::to_value(kind)
        .ok()
        .and_then(|value| value.as_str().map(str::to_string))
        .unwrap_or_else(|| format!("{kind:?}"))
}

fn validate_bundle_relative_path(path: &str) -> Result<(), BackupError> {
    if path.is_empty() {
        return Err(BackupError::corrupted("artifact path must not be empty"));
    }
    if path.contains('\\') || path.contains('\0') {
        return Err(BackupError::corrupted(format!(
            "artifact path {path:?} must not contain backslashes or NUL"
        )));
    }
    let parsed = Path::new(path);
    if parsed.is_absolute() {
        return Err(BackupError::corrupted(format!("artifact path {path:?} must be bundle-relative")));
    }
    for component in parsed.components() {
        match component {
            Component::Normal(_) => {}
            _ => {
                return Err(BackupError::corrupted(format!(
                    "artifact path {path:?} must not contain traversal or special components"
                )));
            }
        }
    }
    Ok(())
}

fn map_serde_error(error: serde_json::Error) -> BackupError {
    if error.classify() == serde_json::error::Category::Eof {
        BackupError::truncated(error.to_string())
    } else {
        BackupError::corrupted(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DUMMY_ARTIFACT_DIGEST: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    /// Handwritten independently of the serializer so a silent serde-side
    /// change cannot keep the round-trip test green while breaking the wire
    /// format. The digest hex is the sealed digest of exactly this content.
    const FIXTURE: &str = r#"{
        "format_version": 1,
        "backup_id": "backup-0001",
        "created_at": "2026-07-30T00:00:00+00:00[UTC]",
        "rustfs_version": "1.0.0-test",
        "deployment_identity": "deployment-a",
        "backend": "Local",
        "at_rest_protection": "encrypted-master-key",
        "responsibility": "full-material",
        "snapshot_generation": 7,
        "backup_kek": {
            "kek_id": "backup-kek-1",
            "kek_version": 1,
            "aead_algorithm": "aes-256-gcm"
        },
        "artifacts": [
            {
                "kind": "key-material",
                "path": "keys/key-material.enc",
                "len": 256,
                "aead_algorithm": "aes-256-gcm",
                "encrypted_digest": {
                    "algorithm": "sha-256",
                    "hex": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                }
            },
            {
                "kind": "key-metadata",
                "path": "keys/key-metadata.enc",
                "len": 256,
                "aead_algorithm": "aes-256-gcm",
                "encrypted_digest": {
                    "algorithm": "sha-256",
                    "hex": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                }
            },
            {
                "kind": "master-key-salt",
                "path": "master-key.salt.enc",
                "len": 256,
                "aead_algorithm": "aes-256-gcm",
                "encrypted_digest": {
                    "algorithm": "sha-256",
                    "hex": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                }
            },
            {
                "kind": "kms-config",
                "path": "config/kms-config.enc",
                "len": 256,
                "aead_algorithm": "aes-256-gcm",
                "encrypted_digest": {
                    "algorithm": "sha-256",
                    "hex": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                }
            }
        ],
        "local_kdf": {
            "derivation": {
                "argon2id": {
                    "version": 19,
                    "memory_kib": 19456,
                    "iterations": 2,
                    "parallelism": 1,
                    "output_len": 32,
                    "salt_len": 16
                }
            },
            "protection_modes": ["encrypted-master-key"],
            "master_key_verifier": "verifier-opaque-1"
        },
        "completeness": "complete",
        "manifest_digest": {
            "algorithm": "sha-256",
            "hex": "SEALED_DIGEST_HEX"
        }
    }"#;

    /// Sealed digest of the fixture content above; computed once from the
    /// canonical form and frozen. If serialization layout or field order
    /// changes, this value changes and the fixture test fails — which is the
    /// point: that is a format-version bump, not a patch.
    const FIXTURE_DIGEST_HEX: &str = "a8b104d61c358cd75cc9a7691d2f7d2d96f73c53653bef8940a49e96dbdf7775";

    fn fixture() -> String {
        FIXTURE.replace("SEALED_DIGEST_HEX", FIXTURE_DIGEST_HEX)
    }

    fn artifact(kind: ArtifactKind, path: &str) -> ArtifactDescriptor {
        ArtifactDescriptor {
            kind,
            path: path.to_string(),
            len: 256,
            aead_algorithm: AeadAlgorithm::Aes256Gcm,
            encrypted_digest: ContentDigest {
                algorithm: DigestAlgorithm::Sha256,
                hex: DUMMY_ARTIFACT_DIGEST.to_string(),
            },
        }
    }

    fn local_manifest_unsealed() -> BackupManifest {
        BackupManifest {
            format_version: BackupManifest::FORMAT_VERSION,
            backup_id: "backup-0001".to_string(),
            created_at: "2026-07-30T00:00:00+00:00[UTC]"
                .parse()
                .expect("fixture timestamp should parse"),
            rustfs_version: "1.0.0-test".to_string(),
            deployment_identity: "deployment-a".to_string(),
            backend: BackupBackendKind::Local,
            at_rest_protection: AtRestProtection::EncryptedMasterKey,
            responsibility: BackupResponsibility::FullMaterial,
            snapshot_generation: 7,
            backup_kek: BackupKekDescriptor {
                kek_id: "backup-kek-1".to_string(),
                kek_version: 1,
                aead_algorithm: AeadAlgorithm::Aes256Gcm,
            },
            artifacts: vec![
                artifact(ArtifactKind::KeyMaterial, "keys/key-material.enc"),
                artifact(ArtifactKind::KeyMetadata, "keys/key-metadata.enc"),
                artifact(ArtifactKind::MasterKeySalt, "master-key.salt.enc"),
                artifact(ArtifactKind::KmsConfig, "config/kms-config.enc"),
            ],
            local_kdf: Some(LocalKdfDescriptor::current(
                vec![AtRestProtection::EncryptedMasterKey],
                Some("verifier-opaque-1".to_string()),
            )),
            key_versions: None,
            capability_discovery: None,
            completeness: CompletenessState::InProgress,
            manifest_digest: ContentDigest {
                algorithm: DigestAlgorithm::Sha256,
                hex: String::new(),
            },
        }
    }

    fn static_manifest_unsealed() -> BackupManifest {
        BackupManifest {
            backend: BackupBackendKind::Static,
            at_rest_protection: AtRestProtection::ExternalSecretDelivery,
            responsibility: BackupResponsibility::ReferenceOnly,
            artifacts: vec![artifact(ArtifactKind::KmsConfig, "config/kms-config.enc")],
            local_kdf: None,
            ..local_manifest_unsealed()
        }
    }

    fn seal(manifest: BackupManifest) -> BackupManifest {
        manifest.seal().expect("sealing should succeed")
    }

    fn expect_corrupted(result: Result<BackupManifest, BackupError>, needle: &str) {
        match result {
            Err(BackupError::Corrupted { reason }) => {
                assert!(
                    reason.contains(needle),
                    "expected corruption reason containing {needle:?}, got {reason:?}"
                );
            }
            other => panic!("expected Corrupted error containing {needle:?}, got {other:?}"),
        }
    }

    #[test]
    fn sealed_manifest_round_trips() {
        let sealed = seal(local_manifest_unsealed());
        let bytes = sealed.encode().expect("encoding should succeed");
        let decoded = BackupManifest::decode(&bytes).expect("decoding should succeed");
        assert_eq!(decoded, sealed);
        assert_eq!(decoded.completeness, CompletenessState::Complete);
    }

    #[test]
    fn handwritten_fixture_decodes_and_matches_serialization() {
        let decoded = BackupManifest::decode(fixture().as_bytes()).expect("fixture should decode");
        let expected = seal(local_manifest_unsealed());
        assert_eq!(decoded, expected);

        // The serializer must produce exactly the handwritten wire content.
        let serialized: serde_json::Value =
            serde_json::from_slice(&expected.encode().expect("encoding should succeed")).expect("re-parse should succeed");
        let fixture_value: serde_json::Value = serde_json::from_str(&fixture()).expect("fixture should parse");
        assert_eq!(serialized, fixture_value);
    }

    #[test]
    fn unknown_format_version_is_rejected() {
        let tampered = fixture().replace("\"format_version\": 1", "\"format_version\": 99");
        match BackupManifest::decode(tampered.as_bytes()) {
            Err(BackupError::UnknownVersion { found, supported }) => {
                assert_eq!(found, 99);
                assert_eq!(supported, BackupManifest::FORMAT_VERSION);
            }
            other => panic!("expected UnknownVersion, got {other:?}"),
        }
    }

    #[test]
    fn missing_completeness_marker_fails_closed() {
        let mut value: serde_json::Value = serde_json::from_str(&fixture()).expect("fixture should parse");
        value
            .as_object_mut()
            .expect("fixture should be an object")
            .remove("completeness");
        let bytes = serde_json::to_vec(&value).expect("serialization should succeed");
        match BackupManifest::decode(&bytes) {
            Err(BackupError::IncompleteBundle { reason }) => {
                assert!(reason.contains("no completeness marker"), "unexpected reason {reason:?}");
            }
            other => panic!("expected IncompleteBundle, got {other:?}"),
        }
    }

    #[test]
    fn in_progress_completeness_fails_closed() {
        let tampered = fixture().replace("\"complete\"", "\"in-progress\"");
        match BackupManifest::decode(tampered.as_bytes()) {
            Err(BackupError::IncompleteBundle { reason }) => {
                assert!(reason.contains("in-progress"), "unexpected reason {reason:?}");
            }
            other => panic!("expected IncompleteBundle, got {other:?}"),
        }
    }

    #[test]
    fn missing_security_critical_field_fails_closed() {
        let mut value: serde_json::Value = serde_json::from_str(&fixture()).expect("fixture should parse");
        value
            .as_object_mut()
            .expect("fixture should be an object")
            .remove("backup_kek");
        let bytes = serde_json::to_vec(&value).expect("serialization should succeed");
        expect_corrupted(BackupManifest::decode(&bytes), "missing field");
    }

    #[test]
    fn duplicate_field_fails_closed() {
        let tampered = fixture().replace("\"snapshot_generation\": 7", "\"snapshot_generation\": 7, \"snapshot_generation\": 7");
        expect_corrupted(BackupManifest::decode(tampered.as_bytes()), "duplicate field");
    }

    #[test]
    fn unknown_field_fails_closed() {
        let tampered = fixture().replace("\"snapshot_generation\": 7", "\"surprise\": true, \"snapshot_generation\": 7");
        expect_corrupted(BackupManifest::decode(tampered.as_bytes()), "unknown field");
    }

    #[test]
    fn truncated_manifest_fails_closed() {
        let full = fixture();
        let truncated = &full.as_bytes()[..full.len() / 2];
        match BackupManifest::decode(truncated) {
            Err(BackupError::Truncated { .. }) => {}
            other => panic!("expected Truncated, got {other:?}"),
        }
    }

    #[test]
    fn tampered_generation_is_rejected() {
        let tampered = fixture().replace("\"snapshot_generation\": 7", "\"snapshot_generation\": 8");
        expect_corrupted(BackupManifest::decode(tampered.as_bytes()), "digest mismatch");
    }

    #[test]
    fn tampered_digest_is_rejected() {
        let last_char = FIXTURE_DIGEST_HEX.as_bytes()[FIXTURE_DIGEST_HEX.len() - 1];
        let flipped = if last_char == b'0' { '1' } else { '0' };
        let mut tampered_digest = FIXTURE_DIGEST_HEX[..FIXTURE_DIGEST_HEX.len() - 1].to_string();
        tampered_digest.push(flipped);
        let tampered = fixture().replace(FIXTURE_DIGEST_HEX, &tampered_digest);
        expect_corrupted(BackupManifest::decode(tampered.as_bytes()), "digest mismatch");
    }

    #[test]
    fn malformed_digest_value_is_rejected() {
        let tampered = fixture().replace(FIXTURE_DIGEST_HEX, "not-hex");
        expect_corrupted(BackupManifest::decode(tampered.as_bytes()), "well-formed");
    }

    #[test]
    fn wrong_kek_fails_closed() {
        let sealed = seal(local_manifest_unsealed());
        sealed
            .backup_kek
            .ensure_matches("backup-kek-1", 1)
            .expect("matching KEK should pass");

        match sealed.backup_kek.ensure_matches("backup-kek-2", 1) {
            Err(BackupError::WrongKek {
                required_kek_id,
                supplied_kek_id,
                ..
            }) => {
                assert_eq!(required_kek_id, "backup-kek-1");
                assert_eq!(supplied_kek_id, "backup-kek-2");
            }
            other => panic!("expected WrongKek, got {other:?}"),
        }
        assert!(matches!(
            sealed.backup_kek.ensure_matches("backup-kek-1", 2),
            Err(BackupError::WrongKek { .. })
        ));
    }

    #[test]
    fn full_material_bundle_requires_key_material_artifact() {
        let mut manifest = local_manifest_unsealed();
        manifest
            .artifacts
            .retain(|artifact| artifact.kind != ArtifactKind::KeyMaterial);
        match seal(manifest).validate() {
            Err(BackupError::MissingArtifact { artifact }) => assert_eq!(artifact, "key-material"),
            other => panic!("expected MissingArtifact, got {other:?}"),
        }
    }

    #[test]
    fn reference_only_bundle_must_not_embed_key_material() {
        let mut manifest = static_manifest_unsealed();
        manifest
            .artifacts
            .push(artifact(ArtifactKind::KeyMaterial, "keys/forbidden.enc"));
        match seal(manifest).validate() {
            Err(BackupError::Corrupted { reason }) => {
                assert!(reason.contains("must not embed key material"), "unexpected reason {reason:?}");
            }
            other => panic!("expected Corrupted, got {other:?}"),
        }
    }

    #[test]
    fn reserved_artifact_kinds_are_rejected() {
        for kind in [ArtifactKind::Alias, ArtifactKind::Policy] {
            assert!(kind.is_reserved());
            let mut manifest = local_manifest_unsealed();
            manifest.artifacts.push(artifact(kind, "reserved/artifact.enc"));
            match seal(manifest).validate() {
                Err(BackupError::Corrupted { reason }) => {
                    assert!(reason.contains("reserved"), "unexpected reason {reason:?}");
                }
                other => panic!("expected Corrupted for {kind:?}, got {other:?}"),
            }
        }
    }

    #[test]
    fn reserved_fields_reject_data() {
        let with_data = fixture().replace("\"completeness\"", "\"key_versions\": {}, \"completeness\"");
        expect_corrupted(BackupManifest::decode(with_data.as_bytes()), "reserved");

        let with_discovery = fixture().replace("\"completeness\"", "\"capability_discovery\": [1], \"completeness\"");
        expect_corrupted(BackupManifest::decode(with_discovery.as_bytes()), "reserved");

        // An explicit null slot decodes as absence at the schema layer, but
        // sealed bundles never contain the key (`skip_serializing_if`), so
        // inserting one after sealing is a byte-level modification and the
        // raw-bytes digest check rejects it.
        let with_null = fixture().replace("\"completeness\"", "\"key_versions\": null, \"completeness\"");
        expect_corrupted(BackupManifest::decode(with_null.as_bytes()), "digest mismatch");
    }

    #[test]
    fn digest_verification_survives_non_round_tripping_timestamp_spellings() {
        // A legacy `created_at` spelling (no time zone annotation) parses via
        // the compat fallback and re-serializes differently ("+00:00[UTC]"),
        // and host-dependent zone spellings can do the same. Digest
        // verification therefore operates on the raw stored bytes and must
        // never re-serialize parsed fields.
        let sealed = seal(local_manifest_unsealed());
        let mut value = serde_json::to_value(&sealed).expect("manifest should convert to a value");
        value["created_at"] = serde_json::Value::String("2026-07-30T00:00:00+00:00".to_string());
        value["manifest_digest"]["hex"] = serde_json::Value::String(String::new());
        let canonical = serde_json::to_vec(&value).expect("canonical bytes");
        let digest = ContentDigest::sha256_of(&canonical);
        value["manifest_digest"]["hex"] = serde_json::Value::String(digest.hex);
        let bytes = serde_json::to_vec(&value).expect("manifest bytes");

        let decoded = BackupManifest::decode(&bytes).expect("a non-round-tripping timestamp spelling must not break decoding");

        // Precondition: the spelling really does not survive a typed
        // round-trip — otherwise this test is vacuous.
        let reserialized = serde_json::to_value(&decoded).expect("decoded manifest should convert to a value");
        assert_ne!(reserialized["created_at"], value["created_at"]);
        // Which is exactly why the producer-side (in-memory) digest check
        // cannot be used on decoded manifests.
        assert!(decoded.verify_digest().is_err());
    }

    #[test]
    fn responsibility_contradicting_matrix_is_rejected() {
        let mut manifest = local_manifest_unsealed();
        manifest.responsibility = BackupResponsibility::ReferenceOnly;
        match seal(manifest).validate() {
            Err(BackupError::Corrupted { reason }) => {
                assert!(reason.contains("contradicts the matrix"), "unexpected reason {reason:?}");
            }
            other => panic!("expected Corrupted, got {other:?}"),
        }

        let mut manifest = local_manifest_unsealed();
        manifest.at_rest_protection = AtRestProtection::StorageOnly;
        match seal(manifest).validate() {
            Err(BackupError::Corrupted { reason }) => {
                assert!(reason.contains("not a defined state"), "unexpected reason {reason:?}");
            }
            other => panic!("expected Corrupted, got {other:?}"),
        }
    }

    #[test]
    fn local_kdf_descriptor_presence_is_tied_to_backend() {
        let mut manifest = local_manifest_unsealed();
        manifest.local_kdf = None;
        match seal(manifest).validate() {
            Err(BackupError::Corrupted { reason }) => {
                assert!(reason.contains("local_kdf"), "unexpected reason {reason:?}");
            }
            other => panic!("expected Corrupted, got {other:?}"),
        }

        let mut manifest = static_manifest_unsealed();
        manifest.local_kdf = Some(LocalKdfDescriptor::current(vec![AtRestProtection::EncryptedMasterKey], None));
        match seal(manifest).validate() {
            Err(BackupError::Corrupted { reason }) => {
                assert!(reason.contains("only valid for the Local backend"), "unexpected reason {reason:?}");
            }
            other => panic!("expected Corrupted, got {other:?}"),
        }
    }

    #[test]
    fn local_kdf_descriptor_rejects_non_local_and_duplicate_modes() {
        let mut manifest = local_manifest_unsealed();
        manifest.local_kdf = Some(LocalKdfDescriptor::current(vec![AtRestProtection::StorageOnly], None));
        match seal(manifest).validate() {
            Err(BackupError::Corrupted { reason }) => {
                assert!(reason.contains("non-local protection mode"), "unexpected reason {reason:?}");
            }
            other => panic!("expected Corrupted, got {other:?}"),
        }

        let mut manifest = local_manifest_unsealed();
        manifest.local_kdf = Some(LocalKdfDescriptor::current(
            vec![AtRestProtection::EncryptedMasterKey, AtRestProtection::EncryptedMasterKey],
            None,
        ));
        match seal(manifest).validate() {
            Err(BackupError::Corrupted { reason }) => {
                assert!(reason.contains("more than once"), "unexpected reason {reason:?}");
            }
            other => panic!("expected Corrupted, got {other:?}"),
        }
    }

    #[test]
    fn artifact_paths_must_be_bundle_relative_and_unique() {
        for bad_path in ["/etc/keys.enc", "../escape.enc", "a/../b.enc", ""] {
            let mut manifest = local_manifest_unsealed();
            manifest.artifacts.push(artifact(ArtifactKind::KeyMetadata, bad_path));
            assert!(
                matches!(seal(manifest).validate(), Err(BackupError::Corrupted { .. })),
                "path {bad_path:?} should be rejected"
            );
        }

        let mut manifest = local_manifest_unsealed();
        let duplicate = manifest.artifacts[0].clone();
        manifest.artifacts.push(duplicate);
        match seal(manifest).validate() {
            Err(BackupError::Corrupted { reason }) => {
                assert!(reason.contains("more than once"), "unexpected reason {reason:?}");
            }
            other => panic!("expected Corrupted, got {other:?}"),
        }
    }

    #[test]
    fn zero_length_artifacts_are_rejected() {
        let mut manifest = local_manifest_unsealed();
        manifest.artifacts[0].len = 0;
        match seal(manifest).validate() {
            Err(BackupError::Corrupted { reason }) => {
                assert!(reason.contains("zero-length"), "unexpected reason {reason:?}");
            }
            other => panic!("expected Corrupted, got {other:?}"),
        }
    }

    #[test]
    fn unsealed_manifest_cannot_be_encoded() {
        let manifest = local_manifest_unsealed();
        assert!(matches!(manifest.encode(), Err(BackupError::IncompleteBundle { .. })));
    }

    #[test]
    fn require_artifact_reports_missing_kind() {
        let sealed = seal(static_manifest_unsealed());
        sealed
            .require_artifact(ArtifactKind::KmsConfig)
            .expect("config artifact should be present");
        match sealed.require_artifact(ArtifactKind::MasterKeySalt) {
            Err(BackupError::MissingArtifact { artifact }) => assert_eq!(artifact, "master-key-salt"),
            other => panic!("expected MissingArtifact, got {other:?}"),
        }
    }

    #[test]
    fn current_local_kdf_matches_backend_constants() {
        // The descriptor is defined in terms of the local.rs constants, so
        // this pins the concrete values: changing a KDF parameter must be a
        // conscious contract decision, not a drive-by edit.
        assert_eq!(
            LocalKeyDerivation::current_argon2id(),
            LocalKeyDerivation::Argon2id {
                version: 0x13,
                memory_kib: 19 * 1024,
                iterations: 2,
                parallelism: 1,
                output_len: 32,
                salt_len: 16,
            }
        );
    }

    #[test]
    fn static_bundle_round_trips() {
        let sealed = seal(static_manifest_unsealed());
        let bytes = sealed.encode().expect("encoding should succeed");
        let decoded = BackupManifest::decode(&bytes).expect("decoding should succeed");
        assert_eq!(decoded, sealed);
        assert_eq!(decoded.responsibility, BackupResponsibility::ReferenceOnly);
    }
}
