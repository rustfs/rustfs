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

//! Wraps a signed typed diagnostic export in the signed support-bundle envelope Connect imports.

use std::fs::File;
use std::io::{Read as _, Seek as _, SeekFrom, Write as _};
use std::path::Path;

use base64_simd::URL_SAFE_NO_PAD;
use p256::ecdsa::{Signature, SigningKey, VerifyingKey, signature::Signer as _, signature::Verifier as _};
use p256::pkcs8::{DecodePrivateKey as _, DecodePublicKey as _};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::{Uuid, Variant, Version};
use zip::{CompressionMethod, ZipArchive, ZipWriter, write::SimpleFileOptions};

use super::identity::DeviceIdentity;
use super::identity_store::IdentityStore;
use super::offline::redaction::{REDACTION_VERSION, RULESET_HASH};

const MANIFEST_PATH: &str = "manifest.json";
const MANIFEST_SIGNATURE_PATH: &str = "manifest.sig";
const ENVELOPE_PATH: &str = "envelope.json";
const ENVELOPE_SIGNATURE_PATH: &str = "envelope.sig";
const RESULT_PATH: &str = "result.json";
const MANIFEST_DOMAIN: &[u8] = b"rustfs-support-bundle-v1";
const ENVELOPE_DOMAIN: &[u8] = b"rustfs-diagnostic-envelope-v1";
const MAX_ENVELOPE_BYTES: u64 = 16 * 1024;
const MAX_SIGNATURE_BYTES: u64 = 4 * 1024;
const MAX_RESULT_BYTES: u64 = 256 * 1024;
const OUTPUT_MODE: u32 = 0o600;

pub(crate) struct UploadSource {
    pub(crate) file: File,
    pub(crate) bundle_uid: String,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ReportBundleError {
    #[error("the diagnostic report archive is invalid")]
    Invalid,
    #[error("the diagnostic report archive has expired")]
    Expired,
    #[error("the Connect device private key is missing")]
    IdentityMissing,
    #[error("the diagnostic report archive could not be read")]
    Io(#[from] std::io::Error),
    #[error("the diagnostic report ZIP is invalid")]
    Zip(#[from] zip::result::ZipError),
}

pub(crate) fn upload_source(
    path: &Path,
    generated_bundle_uid: &str,
    identities: &IdentityStore,
) -> Result<UploadSource, ReportBundleError> {
    if !is_uuid_v7(generated_bundle_uid) {
        return Err(ReportBundleError::Invalid);
    }
    let mut file = File::open(path)?;
    let mut archive = match ZipArchive::new(file) {
        Ok(archive) => archive,
        Err(_) => return original_source(path, generated_bundle_uid),
    };
    let names = (0..archive.len())
        .map(|index| archive.by_index(index).map(|entry| entry.name().to_owned()))
        .collect::<Result<Vec<_>, _>>()?;

    if names
        .iter()
        .any(|name| name == MANIFEST_PATH || name == MANIFEST_SIGNATURE_PATH)
    {
        if !names.iter().any(|name| name == MANIFEST_PATH) || !names.iter().any(|name| name == MANIFEST_SIGNATURE_PATH) {
            return Err(ReportBundleError::Invalid);
        }
        let manifest = read_member(&mut archive, MANIFEST_PATH, 1024 * 1024)?;
        let document: ExistingManifest = serde_json::from_slice(&manifest).map_err(|_| ReportBundleError::Invalid)?;
        if !is_uuid_v7(&document.bundle_uid) {
            return Err(ReportBundleError::Invalid);
        }
        file = archive.into_inner();
        file.seek(SeekFrom::Start(0))?;
        return Ok(UploadSource {
            file,
            bundle_uid: document.bundle_uid,
        });
    }

    let touches_diagnostic = names
        .iter()
        .any(|name| matches!(name.as_str(), ENVELOPE_PATH | ENVELOPE_SIGNATURE_PATH | RESULT_PATH));
    if !touches_diagnostic {
        return original_source(path, generated_bundle_uid);
    }
    if names.len() != 3
        || ![ENVELOPE_PATH, ENVELOPE_SIGNATURE_PATH, RESULT_PATH]
            .iter()
            .all(|expected| names.iter().filter(|name| name.as_str() == *expected).count() == 1)
    {
        return Err(ReportBundleError::Invalid);
    }

    let envelope_bytes = read_member(&mut archive, ENVELOPE_PATH, MAX_ENVELOPE_BYTES)?;
    let envelope_signature_bytes = read_member(&mut archive, ENVELOPE_SIGNATURE_PATH, MAX_SIGNATURE_BYTES)?;
    let result_bytes = read_member(&mut archive, RESULT_PATH, MAX_RESULT_BYTES)?;
    let envelope: DiagnosticEnvelope = serde_json::from_slice(&envelope_bytes).map_err(|_| ReportBundleError::Invalid)?;
    let signature: DiagnosticSignature =
        serde_json::from_slice(&envelope_signature_bytes).map_err(|_| ReportBundleError::Invalid)?;
    let now = OffsetDateTime::now_utc();
    let expires_at = OffsetDateTime::parse(&envelope.expires_at, &Rfc3339).map_err(|_| ReportBundleError::Invalid)?;
    if expires_at <= now {
        return Err(ReportBundleError::Expired);
    }
    validate_scope(&envelope)?;
    validate_payload(&envelope.payload, &result_bytes)?;
    if envelope.format_version != "rustfs.connect.diagnosticEnvelope/1"
        || envelope.protocol_version != "v1"
        || !matches!(envelope.classification.as_str(), "L0" | "L1" | "L2" | "L3")
        || !is_nonce(&envelope.nonce)
    {
        return Err(ReportBundleError::Invalid);
    }

    let identity = identities
        .load()
        .map_err(|_| ReportBundleError::IdentityMissing)?
        .ok_or(ReportBundleError::IdentityMissing)?;
    let device_key_id = hex_lower(&Sha256::digest(identity.public_key_der()));
    if signature.algorithm != "ES256" || signature.key_id != device_key_id || envelope.device_key_id != device_key_id {
        return Err(ReportBundleError::Invalid);
    }
    verify_envelope_signature(&identity, &envelope_bytes, &signature.value)?;

    let produced_at = now
        .replace_nanosecond(0)
        .map_err(|_| ReportBundleError::Invalid)?
        .format(&Rfc3339)
        .map_err(|_| ReportBundleError::Invalid)?;
    let entries = [
        manifest_entry(ENVELOPE_PATH, &envelope_bytes, &envelope.classification),
        manifest_entry(ENVELOPE_SIGNATURE_PATH, &envelope_signature_bytes, &envelope.classification),
        manifest_entry(RESULT_PATH, &result_bytes, &envelope.classification),
    ];
    let manifest = BundleManifest {
        format_version: "rustfs.connect.support.bundleManifest/1",
        protocol_version: "v1",
        bundle_uid: generated_bundle_uid,
        organization_name: &envelope.organization_name,
        cluster_name: &envelope.cluster_name,
        device_name: &envelope.device_name,
        device_key_id: &device_key_id,
        nonce: &envelope.nonce,
        produced_at: &produced_at,
        redaction_version: REDACTION_VERSION,
        ruleset_hash: RULESET_HASH,
        classification_registry_version: 1,
        entries: &entries,
    };
    let manifest_bytes = serde_json::to_vec(&manifest).map_err(|_| ReportBundleError::Invalid)?;
    let manifest_signature = sign_manifest(&identity, &device_key_id, &manifest_bytes)?;
    let mut file = tempfile::tempfile()?;
    {
        let options = SimpleFileOptions::DEFAULT
            .compression_method(CompressionMethod::Stored)
            .system(zip::System::Unix)
            .unix_permissions(OUTPUT_MODE);
        let mut output = ZipWriter::new(&mut file);
        for (name, bytes) in [
            (ENVELOPE_PATH, envelope_bytes.as_slice()),
            (ENVELOPE_SIGNATURE_PATH, envelope_signature_bytes.as_slice()),
            (RESULT_PATH, result_bytes.as_slice()),
            (MANIFEST_PATH, manifest_bytes.as_slice()),
            (MANIFEST_SIGNATURE_PATH, manifest_signature.as_slice()),
        ] {
            output.start_file(name, options)?;
            output.write_all(bytes)?;
        }
        output.finish()?;
    }
    file.seek(SeekFrom::Start(0))?;
    Ok(UploadSource {
        file,
        bundle_uid: generated_bundle_uid.to_owned(),
    })
}

fn original_source(path: &Path, bundle_uid: &str) -> Result<UploadSource, ReportBundleError> {
    Ok(UploadSource {
        file: File::open(path)?,
        bundle_uid: bundle_uid.to_owned(),
    })
}

fn read_member(archive: &mut ZipArchive<File>, name: &str, maximum: u64) -> Result<Vec<u8>, ReportBundleError> {
    let entry = archive.by_name(name)?;
    let size = entry.size();
    if size == 0 || size > maximum || !entry.is_file() {
        return Err(ReportBundleError::Invalid);
    }
    let mut bytes = Vec::with_capacity(size as usize);
    entry.take(maximum + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 != size {
        return Err(ReportBundleError::Invalid);
    }
    Ok(bytes)
}

fn validate_scope(envelope: &DiagnosticEnvelope) -> Result<(), ReportBundleError> {
    let organization = envelope.organization_name.split('/').collect::<Vec<_>>();
    let cluster = envelope.cluster_name.split('/').collect::<Vec<_>>();
    let device = envelope.device_name.split('/').collect::<Vec<_>>();
    if organization.len() != 2
        || organization[0] != "organizations"
        || !is_uuid_v7(organization[1])
        || cluster.len() != 4
        || cluster[..2] != organization[..]
        || cluster[2] != "clusters"
        || !is_uuid_v7(cluster[3])
        || device.len() != 6
        || device[..4] != cluster[..]
        || device[4] != "clusterDevices"
        || !is_uuid_v7(device[5])
    {
        return Err(ReportBundleError::Invalid);
    }
    Ok(())
}

fn validate_payload(payload: &DiagnosticPayload, result: &[u8]) -> Result<(), ReportBundleError> {
    if payload.path != RESULT_PATH
        || payload.media_type != "application/json"
        || payload.size_bytes != result.len() as u64
        || payload.sha256 != hex_lower(&Sha256::digest(result))
    {
        return Err(ReportBundleError::Invalid);
    }
    Ok(())
}

fn is_uuid_v7(value: &str) -> bool {
    Uuid::parse_str(value).is_ok_and(|uuid| {
        uuid.get_version() == Some(Version::SortRand) && uuid.get_variant() == Variant::RFC4122 && uuid.to_string() == value
    })
}

fn is_nonce(value: &str) -> bool {
    URL_SAFE_NO_PAD
        .decode_to_vec(value)
        .is_ok_and(|bytes| bytes.len() == 32 && URL_SAFE_NO_PAD.encode_to_string(&bytes) == value)
}

fn verify_envelope_signature(
    identity: &DeviceIdentity,
    envelope: &[u8],
    encoded_signature: &str,
) -> Result<(), ReportBundleError> {
    let raw = URL_SAFE_NO_PAD
        .decode_to_vec(encoded_signature)
        .map_err(|_| ReportBundleError::Invalid)?;
    if URL_SAFE_NO_PAD.encode_to_string(&raw) != encoded_signature {
        return Err(ReportBundleError::Invalid);
    }
    let signature = Signature::from_slice(&raw).map_err(|_| ReportBundleError::Invalid)?;
    if signature.normalize_s() != signature {
        return Err(ReportBundleError::Invalid);
    }
    let key = VerifyingKey::from_public_key_der(&identity.public_key_der()).map_err(|_| ReportBundleError::Invalid)?;
    let mut signed = Vec::with_capacity(ENVELOPE_DOMAIN.len() + 1 + envelope.len());
    signed.extend_from_slice(ENVELOPE_DOMAIN);
    signed.push(0);
    signed.extend_from_slice(envelope);
    key.verify(&signed, &signature).map_err(|_| ReportBundleError::Invalid)
}

fn sign_manifest(identity: &DeviceIdentity, key_id: &str, manifest: &[u8]) -> Result<Vec<u8>, ReportBundleError> {
    let key = identity.to_pkcs8_der().map_err(|_| ReportBundleError::Invalid)?;
    let key = SigningKey::from_pkcs8_der(key.as_slice()).map_err(|_| ReportBundleError::Invalid)?;
    let mut signed = Vec::with_capacity(MANIFEST_DOMAIN.len() + 1 + manifest.len());
    signed.extend_from_slice(MANIFEST_DOMAIN);
    signed.push(0);
    signed.extend_from_slice(manifest);
    let signature: Signature = key.sign(&signed);
    serde_json::to_vec(&BundleSignature {
        algorithm: "ES256",
        key_id,
        value: URL_SAFE_NO_PAD.encode_to_string(signature.normalize_s().to_bytes()),
    })
    .map_err(|_| ReportBundleError::Invalid)
}

fn manifest_entry<'a>(path: &'static str, bytes: &[u8], classification: &'a str) -> BundleManifestEntry<'a> {
    BundleManifestEntry {
        path,
        entry_type: "offline-diagnostic",
        size_bytes: bytes.len() as u64,
        sha256: hex_lower(&Sha256::digest(bytes)),
        classification,
    }
}

fn hex_lower(bytes: &[u8]) -> String {
    hex_simd::encode_to_string(bytes, hex_simd::AsciiCase::Lower)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExistingManifest {
    bundle_uid: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DiagnosticEnvelope {
    format_version: String,
    protocol_version: String,
    organization_name: String,
    cluster_name: String,
    device_name: String,
    classification: String,
    expires_at: String,
    nonce: String,
    device_key_id: String,
    payload: DiagnosticPayload,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DiagnosticPayload {
    path: String,
    media_type: String,
    size_bytes: u64,
    sha256: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DiagnosticSignature {
    algorithm: String,
    key_id: String,
    value: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BundleManifest<'a> {
    format_version: &'static str,
    protocol_version: &'static str,
    bundle_uid: &'a str,
    organization_name: &'a str,
    cluster_name: &'a str,
    device_name: &'a str,
    device_key_id: &'a str,
    nonce: &'a str,
    produced_at: &'a str,
    redaction_version: &'static str,
    ruleset_hash: &'static str,
    classification_registry_version: u8,
    entries: &'a [BundleManifestEntry<'a>],
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BundleManifestEntry<'a> {
    path: &'static str,
    #[serde(rename = "type")]
    entry_type: &'static str,
    size_bytes: u64,
    sha256: String,
    classification: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BundleSignature<'a> {
    algorithm: &'static str,
    key_id: &'a str,
    value: String,
}

#[cfg(test)]
mod tests {
    use std::io::{Read as _, Seek as _, SeekFrom, Write as _};

    use base64_simd::URL_SAFE_NO_PAD;
    use p256::ecdsa::{Signature, SigningKey, VerifyingKey, signature::Signer as _, signature::Verifier as _};
    use p256::pkcs8::{DecodePrivateKey as _, DecodePublicKey as _};
    use serde_json::{Value, json};
    use sha2::{Digest as _, Sha256};
    use time::{Duration, OffsetDateTime, format_description::well_known::Rfc3339};
    use uuid::Uuid;
    use zip::{CompressionMethod, ZipArchive, ZipWriter, write::SimpleFileOptions};

    use super::{ENVELOPE_DOMAIN, IdentityStore, MANIFEST_DOMAIN, ReportBundleError, hex_lower, upload_source};

    #[test]
    fn wraps_signed_diagnostic_without_changing_original_members() {
        let fixture = Fixture::new(Duration::minutes(5));
        let bundle_uid = Uuid::now_v7().to_string();
        let mut source = upload_source(&fixture.path, &bundle_uid, &fixture.identities).expect("wrap diagnostic");
        source.file.seek(SeekFrom::Start(0)).expect("seek wrapped archive");
        let mut archive = ZipArchive::new(source.file).expect("open wrapped archive");

        assert_eq!(archive.len(), 5);
        assert_eq!(member(&mut archive, "envelope.json"), fixture.envelope);
        assert_eq!(member(&mut archive, "envelope.sig"), fixture.signature);
        assert_eq!(member(&mut archive, "result.json"), fixture.result);

        let manifest_bytes = member(&mut archive, "manifest.json");
        let manifest: Value = serde_json::from_slice(&manifest_bytes).expect("manifest JSON");
        assert_eq!(manifest["bundleUid"], bundle_uid);
        assert_eq!(manifest["nonce"], fixture.nonce);
        assert_eq!(manifest["producedAt"].as_str().expect("producedAt").len(), 20);
        assert_eq!(manifest["entries"].as_array().expect("entries").len(), 3);
        for (index, (path, bytes)) in [
            ("envelope.json", fixture.envelope.as_slice()),
            ("envelope.sig", fixture.signature.as_slice()),
            ("result.json", fixture.result.as_slice()),
        ]
        .into_iter()
        .enumerate()
        {
            let entry = &manifest["entries"][index];
            assert_eq!(entry["path"], path);
            assert_eq!(entry["type"], "offline-diagnostic");
            assert_eq!(entry["classification"], "L3");
            assert_eq!(entry["sizeBytes"], bytes.len());
            assert_eq!(entry["sha256"], hex_lower(&Sha256::digest(bytes)));
        }

        let signature: Value = serde_json::from_slice(&member(&mut archive, "manifest.sig")).expect("signature JSON");
        let mut keys = signature.as_object().expect("signature object").keys().collect::<Vec<_>>();
        keys.sort();
        assert_eq!(keys, ["algorithm", "keyId", "value"]);
        let encoded = signature["value"].as_str().expect("signature value");
        let signature =
            Signature::from_slice(&URL_SAFE_NO_PAD.decode_to_vec(encoded).expect("decode signature")).expect("parse signature");
        let identity = fixture.identities.load().expect("load identity").expect("identity exists");
        let key = VerifyingKey::from_public_key_der(&identity.public_key_der()).expect("public key");
        let mut input = Vec::from(MANIFEST_DOMAIN);
        input.push(0);
        input.extend_from_slice(&manifest_bytes);
        key.verify(&input, &signature).expect("valid manifest signature");
    }

    #[test]
    fn refuses_expired_or_tampered_diagnostics() {
        let expired = Fixture::new(Duration::seconds(-1));
        assert!(matches!(
            upload_source(&expired.path, &Uuid::now_v7().to_string(), &expired.identities),
            Err(ReportBundleError::Expired)
        ));

        let tampered = Fixture::new(Duration::minutes(5));
        tamper_envelope(&tampered.path);
        assert!(matches!(
            upload_source(&tampered.path, &Uuid::now_v7().to_string(), &tampered.identities),
            Err(ReportBundleError::Invalid)
        ));
    }

    struct Fixture {
        _directory: tempfile::TempDir,
        identities: IdentityStore,
        path: std::path::PathBuf,
        envelope: Vec<u8>,
        signature: Vec<u8>,
        result: Vec<u8>,
        nonce: String,
    }

    impl Fixture {
        fn new(validity: Duration) -> Self {
            let directory = tempfile::tempdir().expect("temporary directory");
            let identities = IdentityStore::new(directory.path().join("identity"));
            let identity = identities.load_or_create().expect("device identity");
            let key_id = hex_lower(&Sha256::digest(identity.public_key_der()));
            let now = OffsetDateTime::now_utc().replace_nanosecond(0).expect("whole second");
            let nonce = URL_SAFE_NO_PAD.encode_to_string([7u8; 32]);
            let run_uid = Uuid::now_v7().to_string();
            let result = serde_json::to_vec(&json!({
                "schemaVersion": 1,
                "runUid": run_uid,
                "toolId": "top.net",
                "capability": "top.net@1",
                "outcome": "SUCCEEDED",
                "reasonCode": "COMPLETE",
                "durationMillis": 1000,
                "provenance": {
                    "repository": "rustfs/rustfs",
                    "sourceCommit": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "executableSha256": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    "rustfsVersion": "1.0.0",
                    "osFamily": "LINUX",
                    "architecture": "x86_64",
                    "buildFeatures": []
                },
                "coverage": {"requestedUnits": 1, "completedUnits": 1, "unit": "WINDOW"},
                "data": {"receivedBytes": 4096, "sentBytes": 4096, "windowMillis": 1000}
            }))
            .expect("result JSON");
            let organization = format!("organizations/{}", Uuid::now_v7());
            let cluster = format!("{organization}/clusters/{}", Uuid::now_v7());
            let device = format!("{cluster}/clusterDevices/{}", Uuid::now_v7());
            let envelope = serde_json::to_vec(&json!({
                "formatVersion": "rustfs.connect.diagnosticEnvelope/1",
                "protocolVersion": "v1",
                "organizationName": organization,
                "clusterName": cluster,
                "deviceName": device,
                "runUid": run_uid,
                "artifactUid": Uuid::now_v7().to_string(),
                "toolId": "top.net",
                "schemaVersion": 1,
                "classification": "L3",
                "consentUid": Uuid::now_v7().to_string(),
                "policyRevision": 1,
                "producedAt": now.format(&Rfc3339).expect("producedAt"),
                "expiresAt": (now + validity).format(&Rfc3339).expect("expiresAt"),
                "nonce": nonce,
                "deviceKeyId": key_id,
                "payload": {
                    "path": "result.json",
                    "mediaType": "application/json",
                    "sizeBytes": result.len(),
                    "sha256": hex_lower(&Sha256::digest(&result))
                }
            }))
            .expect("envelope JSON");
            let key = identity.to_pkcs8_der().expect("private key");
            let key = SigningKey::from_pkcs8_der(key.as_slice()).expect("signing key");
            let mut input = Vec::from(ENVELOPE_DOMAIN);
            input.push(0);
            input.extend_from_slice(&envelope);
            let signature: Signature = key.sign(&input);
            let signature = serde_json::to_vec(&json!({
                "algorithm": "ES256",
                "keyId": key_id,
                "value": URL_SAFE_NO_PAD.encode_to_string(signature.normalize_s().to_bytes())
            }))
            .expect("signature JSON");
            let path = directory.path().join("diagnostic.zip");
            write_archive(&path, &envelope, &signature, &result);
            Self {
                _directory: directory,
                identities,
                path,
                envelope,
                signature,
                result,
                nonce,
            }
        }
    }

    fn write_archive(path: &std::path::Path, envelope: &[u8], signature: &[u8], result: &[u8]) {
        let file = std::fs::File::create(path).expect("create archive");
        let options = SimpleFileOptions::DEFAULT
            .compression_method(CompressionMethod::Stored)
            .system(zip::System::Unix)
            .unix_permissions(0o600);
        let mut archive = ZipWriter::new(file);
        for (name, bytes) in [
            ("envelope.json", envelope),
            ("envelope.sig", signature),
            ("result.json", result),
        ] {
            archive.start_file(name, options).expect("start member");
            archive.write_all(bytes).expect("write member");
        }
        archive.finish().expect("finish archive");
    }

    fn member(archive: &mut ZipArchive<std::fs::File>, name: &str) -> Vec<u8> {
        let mut member = archive.by_name(name).expect("archive member");
        let mut bytes = Vec::new();
        member.read_to_end(&mut bytes).expect("read member");
        bytes
    }

    fn tamper_envelope(path: &std::path::Path) {
        let file = std::fs::File::open(path).expect("open archive");
        let mut archive = ZipArchive::new(file).expect("read archive");
        let mut envelope = member(&mut archive, "envelope.json");
        let signature = member(&mut archive, "envelope.sig");
        let result = member(&mut archive, "result.json");
        drop(archive);
        envelope.push(b' ');
        write_archive(path, &envelope, &signature, &result);
    }
}
