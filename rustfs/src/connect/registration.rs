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

use std::io::Read;
use std::sync::Arc;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as BASE64_URL_NO_PAD;
use p256::ecdsa::signature::Signer as _;
use p256::ecdsa::{Signature, SigningKey};
use p256::pkcs8::DecodePrivateKey as _;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, UnixTime, pem::PemObject as _};
use rustls::server::WebPkiClientVerifier;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use uuid::{Uuid, Version};
use x509_parser::extensions::GeneralName;
use x509_parser::oid_registry::OID_SIG_ECDSA_WITH_SHA256;
use x509_parser::prelude::{FromDer as _, X509Certificate, X509CertificationRequest};
use zeroize::Zeroizing;

use super::credential_store::DeviceCredential;
use super::identity::{DeviceIdentity, RegistrationProof};

pub const PROTOCOL_VERSION: &str = "v1";

const CERTIFICATE_LIFETIME_SECONDS: i64 = 86_400;
const ROTATION_DOMAIN: &[u8] = b"RUSTFS-CONNECT-CREDENTIAL-ROTATION-V1";
const MAX_TOKEN_BYTES: u64 = 16 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct RegistrationTokenDocument {
    registration_token_uid: String,
    registration_token_secret: String,
    organization_uid: String,
    cluster_uid: String,
    challenge_nonce: String,
    expires_unix: i64,
}

pub struct RegistrationToken {
    pub registration_token_uid: String,
    registration_token_secret: Zeroizing<String>,
    pub organization_uid: String,
    pub cluster_uid: String,
    pub challenge_nonce: String,
    pub expires_unix: i64,
}

impl std::fmt::Debug for RegistrationToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegistrationToken")
            .field("registration_token_uid", &self.registration_token_uid)
            .field("expires_unix", &self.expires_unix)
            .finish_non_exhaustive()
    }
}

impl RegistrationToken {
    pub fn from_reader(reader: impl Read) -> Result<Self, TokenError> {
        let mut bytes = Zeroizing::new(Vec::new());
        reader
            .take(MAX_TOKEN_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(TokenError::Read)?;
        if bytes.len() as u64 > MAX_TOKEN_BYTES {
            return Err(TokenError::TooLarge);
        }
        let document: RegistrationTokenDocument = serde_json::from_slice(&bytes).map_err(TokenError::Invalid)?;
        let decoded = BASE64_URL_NO_PAD
            .decode(&document.registration_token_secret)
            .map(Zeroizing::new)
            .map_err(|_| TokenError::SecretShape)?;
        if decoded.len() != 32 || BASE64_URL_NO_PAD.encode(&decoded) != document.registration_token_secret {
            return Err(TokenError::SecretShape);
        }
        if !is_uuid_v7(&document.registration_token_uid)
            || !is_uuid_v7(&document.organization_uid)
            || !is_uuid_v7(&document.cluster_uid)
            || document.expires_unix < 0
            || document.challenge_nonce.len() != 64
            || !document
                .challenge_nonce
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(TokenError::Shape);
        }

        Ok(Self {
            registration_token_uid: document.registration_token_uid,
            registration_token_secret: Zeroizing::new(document.registration_token_secret),
            organization_uid: document.organization_uid,
            cluster_uid: document.cluster_uid,
            challenge_nonce: document.challenge_nonce,
            expires_unix: document.expires_unix,
        })
    }

    pub(crate) fn secret(&self) -> &str {
        &self.registration_token_secret
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TokenError {
    #[error("failed to read the Connect registration token")]
    Read(#[source] std::io::Error),
    #[error("Connect registration token configuration is invalid")]
    Invalid(#[source] serde_json::Error),
    #[error("Connect registration token secret must be 32-byte unpadded base64url")]
    SecretShape,
    #[error("Connect registration token configuration exceeds 16 KiB")]
    TooLarge,
    #[error("Connect registration token fields do not match the protocol schema")]
    Shape,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RegistrationRequest<'a> {
    protocol_version: &'static str,
    request_id: &'a str,
    registration_token_uid: &'a str,
    registration_token_secret: &'a str,
    certificate_request: &'a str,
    proof: ProofRef<'a>,
}

#[derive(Serialize)]
struct ProofRef<'a> {
    algorithm: &'a str,
    value: &'a str,
}

impl<'a> RegistrationRequest<'a> {
    pub(crate) fn new(
        token: &'a RegistrationToken,
        request_id: &'a str,
        certificate_request: &'a str,
        proof: &'a RegistrationProof,
    ) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            request_id,
            registration_token_uid: &token.registration_token_uid,
            registration_token_secret: token.secret(),
            certificate_request,
            proof: ProofRef {
                algorithm: &proof.algorithm,
                value: &proof.value,
            },
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RotationRequest<'a> {
    protocol_version: &'static str,
    request_id: &'a str,
    certificate_request: &'a str,
    proof: ProofOwned,
}

#[derive(Serialize)]
struct ProofOwned {
    algorithm: String,
    value: String,
}

impl<'a> RotationRequest<'a> {
    pub(crate) fn new(
        identity: &DeviceIdentity,
        credential_fingerprint: &str,
        device_name: &str,
        request_id: &'a str,
        certificate_request: &'a str,
    ) -> Result<Self, CredentialValidationError> {
        let csr_der = base64::engine::general_purpose::STANDARD
            .decode(certificate_request)
            .map_err(|_| CredentialValidationError::CertificateRequest)?;
        let csr_digest = BASE64_URL_NO_PAD.encode(Sha256::digest(&csr_der));
        let transcript = rotation_transcript(credential_fingerprint, device_name, request_id, &csr_digest)?;
        let key = identity
            .to_pkcs8_der()
            .map_err(|_| CredentialValidationError::CertificateRequest)?;
        let signing_key = SigningKey::from_pkcs8_der(&key).map_err(|_| CredentialValidationError::CertificateRequest)?;
        let signature: Signature = signing_key.sign(&transcript);
        let canonical = signature.normalize_s().unwrap_or(signature);

        Ok(Self {
            protocol_version: PROTOCOL_VERSION,
            request_id,
            certificate_request,
            proof: ProofOwned {
                algorithm: "ES256".to_string(),
                value: BASE64_URL_NO_PAD.encode(canonical.to_bytes()),
            },
        })
    }
}

fn rotation_transcript(
    credential_fingerprint: &str,
    device_name: &str,
    request_id: &str,
    csr_digest: &str,
) -> Result<Vec<u8>, CredentialValidationError> {
    let fields = [credential_fingerprint, device_name, request_id, csr_digest];
    if fields
        .iter()
        .any(|field| !field.is_ascii() || field.as_bytes().contains(&b'\n'))
    {
        return Err(CredentialValidationError::RotationTranscript);
    }

    let mut transcript = Vec::with_capacity(346);
    transcript.extend_from_slice(ROTATION_DOMAIN);
    transcript.push(b'\n');
    for field in fields {
        transcript.extend_from_slice(field.len().to_string().as_bytes());
        transcript.push(b':');
        transcript.extend_from_slice(field.as_bytes());
        transcript.push(b'\n');
    }
    Ok(transcript)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CredentialResponse {
    pub name: String,
    #[serde(default)]
    pub uid: String,
    #[serde(default)]
    pub cluster: String,
    pub protocol_version: String,
    pub key_id: String,
    pub certificate_serial: String,
    pub certificate: String,
    pub certificate_chain: String,
    pub not_before: String,
    pub not_after: String,
}

pub(crate) enum ExpectedDevice<'a> {
    Registration { cluster: &'a str },
    Rotation { name: &'a str },
    Stored,
}

#[derive(Debug, thiserror::Error)]
pub enum CredentialValidationError {
    #[error("Connect returned malformed certificate material")]
    Certificate,
    #[error("Connect returned a certificate chain that is not trusted")]
    Chain,
    #[error("Connect returned a certificate for the wrong device identity")]
    Identity,
    #[error("Connect returned a certificate for a different device key")]
    Key,
    #[error("Connect returned an invalid certificate validity window")]
    Validity,
    #[error("the device certificate request could not be prepared")]
    CertificateRequest,
    #[error("the credential rotation transcript contains an invalid field")]
    RotationTranscript,
}

pub(crate) fn validate_credential(
    response: CredentialResponse,
    identity: &DeviceIdentity,
    roots: &RootCertStore,
    root_certificates: &[CertificateDer<'static>],
    expected: ExpectedDevice<'_>,
) -> Result<DeviceCredential, CredentialValidationError> {
    validate_credential_at(response, identity, roots, root_certificates, expected, true)
}

pub(crate) fn validate_stored_credential(
    credential: &DeviceCredential,
    identity: &DeviceIdentity,
    roots: &RootCertStore,
    root_certificates: &[CertificateDer<'static>],
) -> Result<(), CredentialValidationError> {
    let not_before = OffsetDateTime::from_unix_timestamp(credential.not_before_unix)
        .map_err(|_| CredentialValidationError::Validity)?
        .format(&Rfc3339)
        .map_err(|_| CredentialValidationError::Validity)?;
    let not_after = OffsetDateTime::from_unix_timestamp(credential.not_after_unix)
        .map_err(|_| CredentialValidationError::Validity)?
        .format(&Rfc3339)
        .map_err(|_| CredentialValidationError::Validity)?;
    let response = CredentialResponse {
        name: credential.name.clone(),
        uid: credential.uid.clone(),
        cluster: String::new(),
        protocol_version: credential.protocol_version.clone(),
        key_id: credential.key_id.clone(),
        certificate_serial: credential.certificate_serial.clone(),
        certificate: credential.certificate.clone(),
        certificate_chain: credential.certificate_chain.clone(),
        not_before,
        not_after,
    };
    validate_credential_at(response, identity, roots, root_certificates, ExpectedDevice::Stored, false).map(|_| ())
}

fn validate_credential_at(
    response: CredentialResponse,
    identity: &DeviceIdentity,
    roots: &RootCertStore,
    root_certificates: &[CertificateDer<'static>],
    expected: ExpectedDevice<'_>,
    verify_now: bool,
) -> Result<DeviceCredential, CredentialValidationError> {
    if response.protocol_version != PROTOCOL_VERSION {
        return Err(CredentialValidationError::Identity);
    }

    let leaves = CertificateDer::pem_slice_iter(response.certificate.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| CredentialValidationError::Certificate)?;
    if leaves.len() != 1 {
        return Err(CredentialValidationError::Certificate);
    }
    let chain = CertificateDer::pem_slice_iter(response.certificate_chain.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| CredentialValidationError::Certificate)?;
    if chain.is_empty()
        || chain[0].as_ref() != leaves[0].as_ref()
        || chain
            .iter()
            .skip(1)
            .any(|certificate| root_certificates.iter().any(|root| root.as_ref() == certificate.as_ref()))
    {
        return Err(CredentialValidationError::Chain);
    }

    let (remaining, certificate) =
        X509Certificate::from_der(leaves[0].as_ref()).map_err(|_| CredentialValidationError::Certificate)?;
    if !remaining.is_empty() {
        return Err(CredentialValidationError::Certificate);
    }

    let uid = match expected {
        ExpectedDevice::Registration { cluster } => {
            if response.uid.is_empty()
                || response.cluster != cluster
                || response.name != format!("{cluster}/clusterDevices/{}", response.uid)
            {
                return Err(CredentialValidationError::Identity);
            }
            response.uid.clone()
        }
        ExpectedDevice::Rotation { name } => {
            if response.name != name || !response.uid.is_empty() || !response.cluster.is_empty() {
                return Err(CredentialValidationError::Identity);
            }
            name.rsplit_once("/clusterDevices/")
                .map(|(_, uid)| uid.to_string())
                .ok_or(CredentialValidationError::Identity)?
        }
        ExpectedDevice::Stored => {
            let (cluster, name_uid) = response
                .name
                .rsplit_once("/clusterDevices/")
                .ok_or(CredentialValidationError::Identity)?;
            if response.uid != name_uid
                || !response.cluster.is_empty()
                || !valid_cluster_name(cluster)
                || response.name.matches("/clusterDevices/").count() != 1
            {
                return Err(CredentialValidationError::Identity);
            }
            response.uid.clone()
        }
    };
    let parsed_uid = Uuid::parse_str(&uid).map_err(|_| CredentialValidationError::Identity)?;
    if parsed_uid.get_version() != Some(Version::SortRand) || parsed_uid.to_string() != uid {
        return Err(CredentialValidationError::Identity);
    }
    let expected_uri = format!("urn:rustfs:connect:device:{uid}");
    let common_names = certificate
        .subject()
        .iter_common_name()
        .map(|name| name.as_str())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| CredentialValidationError::Identity)?;
    let san = certificate
        .subject_alternative_name()
        .map_err(|_| CredentialValidationError::Identity)?
        .ok_or(CredentialValidationError::Identity)?;
    let san_matches = matches!(san.value.general_names.as_slice(), [GeneralName::URI(uri)] if *uri == expected_uri);

    if common_names.as_slice() != [uid.as_str()]
        || !san_matches
        || certificate.subject().iter().count() != 1
        || certificate.subject().iter_attributes().count() != 1
    {
        return Err(CredentialValidationError::Identity);
    }
    if certificate.public_key().raw != identity.public_key_der() {
        return Err(CredentialValidationError::Key);
    }

    let not_before = OffsetDateTime::parse(&response.not_before, &Rfc3339)
        .map_err(|_| CredentialValidationError::Validity)?
        .unix_timestamp();
    let not_after = OffsetDateTime::parse(&response.not_after, &Rfc3339)
        .map_err(|_| CredentialValidationError::Validity)?
        .unix_timestamp();
    let verify_unix = if verify_now {
        UnixTime::now()
    } else {
        let midpoint = certificate.validity().not_before.timestamp()
            + (certificate.validity().not_after.timestamp() - certificate.validity().not_before.timestamp()) / 2;
        UnixTime::since_unix_epoch(std::time::Duration::from_secs(
            midpoint.try_into().map_err(|_| CredentialValidationError::Validity)?,
        ))
    };
    let verifier = WebPkiClientVerifier::builder(Arc::new(roots.clone()))
        .build()
        .map_err(|_| CredentialValidationError::Chain)?;
    verifier
        .verify_client_cert(&leaves[0], &chain[1..], verify_unix)
        .map_err(|_| CredentialValidationError::Chain)?;
    if not_before != certificate.validity().not_before.timestamp()
        || not_after != certificate.validity().not_after.timestamp()
        || not_after - not_before != CERTIFICATE_LIFETIME_SECONDS
        || certificate.signature_algorithm.algorithm != OID_SIG_ECDSA_WITH_SHA256
        || response.certificate_serial != canonical_serial(certificate.raw_serial())?
        || response.key_id != format!("x509-{}", response.certificate_serial)
    {
        return Err(CredentialValidationError::Validity);
    }

    Ok(DeviceCredential {
        name: response.name,
        uid,
        protocol_version: response.protocol_version,
        key_id: response.key_id,
        certificate_serial: response.certificate_serial,
        certificate: response.certificate,
        certificate_chain: response.certificate_chain,
        not_before_unix: not_before,
        not_after_unix: not_after,
    })
}

pub(crate) fn public_key_fingerprint(identity: &DeviceIdentity) -> String {
    hex_lower(&Sha256::digest(identity.public_key_der()))
}

pub(crate) fn certificate_request_matches(encoded: &str, identity: &DeviceIdentity) -> Result<bool, CredentialValidationError> {
    let der = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|_| CredentialValidationError::CertificateRequest)?;
    let (remaining, request) =
        X509CertificationRequest::from_der(&der).map_err(|_| CredentialValidationError::CertificateRequest)?;
    Ok(remaining.is_empty() && request.certification_request_info.subject_pki.raw == identity.public_key_der())
}

fn canonical_serial(raw: &[u8]) -> Result<String, CredentialValidationError> {
    let magnitude = match raw {
        [0, first, rest @ ..] if first & 0x80 != 0 => {
            if rest.len() + 1 > 16 {
                return Err(CredentialValidationError::Validity);
            }
            &raw[1..]
        }
        [0] => raw,
        [0, ..] | [] => return Err(CredentialValidationError::Validity),
        [first, ..] if first & 0x80 != 0 => return Err(CredentialValidationError::Validity),
        _ if raw.len() > 16 => return Err(CredentialValidationError::Validity),
        _ => raw,
    };
    let mut padded = [0u8; 16];
    padded[16 - magnitude.len()..].copy_from_slice(magnitude);
    Ok(hex_lower(&padded))
}

fn is_uuid_v7(value: &str) -> bool {
    Uuid::parse_str(value).is_ok_and(|uuid| uuid.get_version() == Some(Version::SortRand) && uuid.to_string() == value)
}

fn valid_cluster_name(name: &str) -> bool {
    let Some((organization, cluster)) = name
        .strip_prefix("organizations/")
        .and_then(|rest| rest.split_once("/clusters/"))
    else {
        return false;
    };
    !cluster.contains('/') && is_uuid_v7(organization) && is_uuid_v7(cluster)
}

pub(crate) fn certificate_fingerprint(certificate_pem: &str) -> Result<String, CredentialValidationError> {
    let certificate = CertificateDer::pem_slice_iter(certificate_pem.as_bytes())
        .next()
        .ok_or(CredentialValidationError::Certificate)?
        .map_err(|_| CredentialValidationError::Certificate)?;
    Ok(hex_lower(&Sha256::digest(certificate.as_ref())))
}

fn hex_lower(bytes: &[u8]) -> String {
    bytes.iter().fold(String::with_capacity(bytes.len() * 2), |mut output, byte| {
        use std::fmt::Write as _;
        let _ = write!(output, "{byte:02x}");
        output
    })
}
