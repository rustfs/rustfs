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

//! Customer-operated transfer of opaque, signed Connect material.
//!
//! The relay has no signing authority. It carries the exact approved bytes and
//! accepts success only from a receipt signed by the preconfigured destination.

use base64_simd::{STANDARD as BASE64_STANDARD, URL_SAFE_NO_PAD};
use ed25519_dalek::{Signature, Signer as _, SigningKey, VerifyingKey};
use reqwest::{Client, StatusCode, Url, header};
use rustls::pki_types::{CertificateDer, pem::PemObject as _};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
#[cfg(unix)]
use std::fs::OpenOptions;
use std::io::Read as _;
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _};
use std::path::Path;
use std::time::Duration;
use uuid::{Uuid, Variant, Version};

use super::client::build_client;
use super::config::ProxyConfig;

pub const RELAY_ENVELOPE_FORMAT: &str = "rustfs.connect.relayEnvelope/1";
pub const RELAY_RECEIPT_FORMAT: &str = "rustfs.connect.relayReceipt/1";
pub const RELAY_RECEIPT_DOMAIN_SEPARATION_TAG: &str = "rustfs-connect-relay-receipt-v1";
pub const MAX_RELAY_ARTIFACT_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_DELIVERY_ATTEMPTS: usize = 3;
const MAX_RECEIPT_BYTES: usize = 64 * 1024;
const RETRY_DELAY: Duration = Duration::from_millis(250);
const MAX_AUTHENTICATION_BYTES: u64 = 8 * 1024;
const MAX_PUBLIC_KEY_BYTES: u64 = 256;
const MAX_PRIVATE_KEY_BYTES: u64 = 256;
const MAX_RELAY_ENVELOPE_BYTES: usize = MAX_RELAY_ARTIFACT_BYTES * 2 + 64 * 1024;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RelayMaterialKind {
    OfflineEnrollmentResponse,
    DiagnosticBundleManifest,
    ServiceLicense,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RelayDirection {
    ClusterToConnect,
    ConnectToCluster,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct RelayParty {
    #[serde(rename = "type")]
    pub party_type: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct RelayArtifact {
    pub encoding: String,
    pub bytes: String,
    pub sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct RelayEnvelope {
    pub format_version: String,
    pub protocol_version: String,
    pub transfer_uid: String,
    pub material_kind: RelayMaterialKind,
    pub direction: RelayDirection,
    pub artifact: RelayArtifact,
    pub asserted_producer: RelayParty,
    pub destination: RelayParty,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RelayReview {
    pub transfer_uid: String,
    pub material_kind: RelayMaterialKind,
    pub direction: RelayDirection,
    pub artifact_sha256: String,
    pub artifact_size_bytes: usize,
    pub asserted_producer: RelayParty,
    pub destination: RelayParty,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RelayReceiptOutcome {
    Applied,
    Duplicate,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct RelayReceiptPayload {
    pub format_version: String,
    pub protocol_version: String,
    pub transfer_uid: String,
    pub material_kind: RelayMaterialKind,
    pub direction: RelayDirection,
    pub artifact_sha256: String,
    pub producer: RelayParty,
    pub destination: RelayParty,
    pub outcome: RelayReceiptOutcome,
    pub received_at: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
struct RelayReceiptSignature {
    algorithm: String,
    key_id: String,
    value: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SignedRelayReceipt {
    payload: String,
    signature: RelayReceiptSignature,
}

#[derive(Clone, Debug)]
pub struct TrustedReceiptSigner {
    key_id: String,
    verifying_key: VerifyingKey,
}

#[derive(Clone)]
pub struct DestinationReceiptSigner {
    key_id: String,
    signing_key: SigningKey,
}

impl DestinationReceiptSigner {
    pub fn new(key_id: String, seed: [u8; 32]) -> Result<Self, RelayError> {
        let signing_key = SigningKey::from_bytes(&seed);
        let actual_key_id = hex_lower(&Sha256::digest(signing_key.verifying_key().as_bytes()));
        if !is_sha256(&key_id) || key_id != actual_key_id {
            return Err(RelayError::ReceiptTrustInvalid);
        }
        Ok(Self { key_id, signing_key })
    }

    pub fn from_private_key_file(path: &Path, key_id: String) -> Result<Self, RelayError> {
        let encoded = read_protected_bytes(path, MAX_PRIVATE_KEY_BYTES)?;
        let encoded = std::str::from_utf8(&encoded)
            .map_err(|_| RelayError::ReceiptTrustInvalid)?
            .trim();
        let seed = decode_canonical_base64url(encoded).ok_or(RelayError::ReceiptTrustInvalid)?;
        let seed: [u8; 32] = seed.try_into().map_err(|_| RelayError::ReceiptTrustInvalid)?;
        Self::new(key_id, seed)
    }

    pub(crate) fn sign(
        &self,
        envelope: &RelayEnvelope,
        producer: RelayParty,
        outcome: RelayReceiptOutcome,
        received_at: String,
    ) -> Result<Vec<u8>, RelayError> {
        let payload = RelayReceiptPayload {
            format_version: RELAY_RECEIPT_FORMAT.to_owned(),
            protocol_version: envelope.protocol_version.clone(),
            transfer_uid: envelope.transfer_uid.clone(),
            material_kind: envelope.material_kind,
            direction: envelope.direction,
            artifact_sha256: envelope.artifact.sha256.clone(),
            producer,
            destination: envelope.destination.clone(),
            outcome,
            received_at,
        };
        let payload = serde_json::to_vec(&payload).map_err(|_| RelayError::ReceiptEncoding)?;
        let mut signed = Vec::with_capacity(RELAY_RECEIPT_DOMAIN_SEPARATION_TAG.len() + 1 + payload.len());
        signed.extend_from_slice(RELAY_RECEIPT_DOMAIN_SEPARATION_TAG.as_bytes());
        signed.push(0);
        signed.extend_from_slice(&payload);
        let signature = self.signing_key.sign(&signed).to_bytes();
        serde_json::to_vec(&SignedRelayReceipt {
            payload: URL_SAFE_NO_PAD.encode_to_string(&payload),
            signature: RelayReceiptSignature {
                algorithm: "Ed25519".to_owned(),
                key_id: self.key_id.clone(),
                value: URL_SAFE_NO_PAD.encode_to_string(signature),
            },
        })
        .map_err(|_| RelayError::ReceiptEncoding)
    }
}

impl TrustedReceiptSigner {
    pub fn new(key_id: String, public_key: [u8; 32]) -> Result<Self, RelayError> {
        if !is_sha256(&key_id) || hex_lower(&Sha256::digest(public_key)) != key_id {
            return Err(RelayError::ReceiptTrustInvalid);
        }
        let verifying_key = VerifyingKey::from_bytes(&public_key).map_err(|_| RelayError::ReceiptTrustInvalid)?;
        Ok(Self { key_id, verifying_key })
    }

    pub fn from_public_key_file(path: &Path, key_id: String) -> Result<Self, RelayError> {
        let encoded = read_protected_bytes(path, MAX_PUBLIC_KEY_BYTES)?;
        let encoded = std::str::from_utf8(&encoded)
            .map_err(|_| RelayError::ReceiptTrustInvalid)?
            .trim();
        let public_key = decode_canonical_base64url(encoded).ok_or(RelayError::ReceiptTrustInvalid)?;
        let public_key: [u8; 32] = public_key.try_into().map_err(|_| RelayError::ReceiptTrustInvalid)?;
        Self::new(key_id, public_key)
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RelayReplayKey {
    material_kind: RelayMaterialKind,
    artifact_sha256: String,
    destination_type: String,
    destination_name: String,
}

impl RelayEnvelope {
    /// The durable destination uses this key after material-specific signature,
    /// freshness, revocation, and scope verification. It must be persisted with
    /// the terminal receipt before acknowledging a side effect.
    pub fn replay_key(&self) -> RelayReplayKey {
        RelayReplayKey {
            material_kind: self.material_kind,
            artifact_sha256: self.artifact.sha256.clone(),
            destination_type: self.destination.party_type.clone(),
            destination_name: self.destination.name.clone(),
        }
    }
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum RelayError {
    #[error("the relay material kind or direction is unsupported")]
    UnsupportedMaterial,
    #[error("the relay transfer identifier is not a canonical UUIDv7")]
    InvalidTransferUid,
    #[error("the relay producer or destination identity is invalid")]
    InvalidParty,
    #[error("the relay artifact is empty or exceeds its size limit")]
    InvalidArtifact,
    #[error("the customer did not approve this relay transfer")]
    ApprovalRequired,
    #[error("the relay envelope could not be encoded")]
    EnvelopeEncoding,
    #[error("the relay envelope is malformed")]
    EnvelopeInvalid,
    #[error("the relay artifact digest does not match the exact decoded bytes")]
    ArtifactDigestMismatch,
    #[error("the destination receipt trust configuration is invalid")]
    ReceiptTrustInvalid,
    #[error("the destination receipt is malformed")]
    ReceiptInvalid,
    #[error("the destination receipt key is not trusted")]
    ReceiptSignerUntrusted,
    #[error("the destination receipt signature is invalid")]
    ReceiptSignatureInvalid,
    #[error("the destination receipt does not bind this transfer")]
    ReceiptMismatch,
    #[error("the destination receipt could not be encoded")]
    ReceiptEncoding,
    #[error("delivery produced no verified receipt after three attempts")]
    DeliveryUnknown,
    #[error("the relay control API rejected delivery with HTTP {0}")]
    DeliveryRejected(u16),
    #[error("the relay control API response exceeded its size limit")]
    ResponseTooLarge,
    #[error("the relay HTTP authentication header is invalid")]
    AuthenticationInvalid,
    #[error("the relay input file is unavailable or exceeds its size limit")]
    InputFile,
    #[error("the relay credential file is not an owner-only regular file")]
    CredentialFileSecurity,
    #[error("the relay HTTPS client configuration is invalid")]
    ClientConfiguration,
    #[error("the relay HTTPS request failed")]
    Transport,
}

pub trait RelayTransport {
    fn deliver(&mut self, envelope: &[u8]) -> Result<Option<Vec<u8>>, RelayError>;
}

#[derive(Debug)]
pub struct RelayDelivery {
    pub review: RelayReview,
    pub envelope: RelayEnvelope,
    pub receipt: RelayReceiptPayload,
    pub receipt_bytes: Vec<u8>,
    pub attempts: usize,
}

pub struct PreparedRelay {
    pub review: RelayReview,
    pub envelope: RelayEnvelope,
}

pub struct RelayHttpClient {
    client: Client,
    receive_url: Url,
    cookie: header::HeaderValue,
    csrf_token: header::HeaderValue,
    approval_reference: String,
}

impl RelayHttpClient {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        endpoint: &str,
        root_ca_pem: &[u8],
        organization_uid: &str,
        approval_reference: String,
        cookie: &str,
        csrf_token: &str,
        timeout: Duration,
        proxy: Option<&ProxyConfig>,
    ) -> Result<Self, RelayError> {
        let endpoint = Url::parse(endpoint).map_err(|_| RelayError::ClientConfiguration)?;
        if endpoint.scheme() != "https"
            || endpoint.cannot_be_a_base()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || !endpoint.path().ends_with("/api/")
            || !is_uuid_v7(organization_uid)
            || approval_reference.is_empty()
            || approval_reference.len() > 512
        {
            return Err(RelayError::ClientConfiguration);
        }
        let receive_url = endpoint
            .join(&format!("organizations/{organization_uid}/relayTransfers:receive"))
            .map_err(|_| RelayError::ClientConfiguration)?;
        let roots = CertificateDer::pem_slice_iter(root_ca_pem)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| RelayError::ClientConfiguration)?;
        if roots.is_empty() {
            return Err(RelayError::ClientConfiguration);
        }
        let client = build_client(&roots, timeout, None, proxy).map_err(|_| RelayError::ClientConfiguration)?;
        let mut cookie = header::HeaderValue::from_str(cookie).map_err(|_| RelayError::AuthenticationInvalid)?;
        let mut csrf_token = header::HeaderValue::from_str(csrf_token).map_err(|_| RelayError::AuthenticationInvalid)?;
        cookie.set_sensitive(true);
        csrf_token.set_sensitive(true);
        Ok(Self {
            client,
            receive_url,
            cookie,
            csrf_token,
            approval_reference,
        })
    }

    pub async fn deliver(
        &self,
        prepared: PreparedRelay,
        trusted_receipt_signer: &TrustedReceiptSigner,
    ) -> Result<RelayDelivery, RelayError> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Request<'a> {
            approval_reference: &'a str,
            envelope: &'a RelayEnvelope,
        }

        for attempts in 1..=MAX_DELIVERY_ATTEMPTS {
            let response = self
                .client
                .post(self.receive_url.clone())
                .header(header::COOKIE, self.cookie.clone())
                .header("X-XSRF-TOKEN", self.csrf_token.clone())
                .header(header::ACCEPT, "application/json")
                .json(&Request {
                    approval_reference: &self.approval_reference,
                    envelope: &prepared.envelope,
                })
                .send()
                .await;
            let response = match response {
                Ok(response) => response,
                Err(_) if attempts < MAX_DELIVERY_ATTEMPTS => {
                    tokio::time::sleep(RETRY_DELAY).await;
                    continue;
                }
                Err(_) => return Err(RelayError::Transport),
            };
            if retryable_status(response.status()) && attempts < MAX_DELIVERY_ATTEMPTS {
                tokio::time::sleep(RETRY_DELAY).await;
                continue;
            }
            if response.status() != StatusCode::OK {
                return Err(RelayError::DeliveryRejected(response.status().as_u16()));
            }
            let receipt_bytes = bounded_body(response).await?;
            let receipt = verify_receipt(&receipt_bytes, &prepared.envelope, trusted_receipt_signer)?;
            return Ok(RelayDelivery {
                review: prepared.review,
                envelope: prepared.envelope,
                receipt,
                receipt_bytes,
                attempts,
            });
        }
        Err(RelayError::DeliveryUnknown)
    }
}

#[allow(clippy::too_many_arguments)]
/// Build, approve, and deliver one relay envelope without changing the signed
/// material bytes. Producer fields are advisory until the destination derives
/// and compares them using its material-specific verifier.
pub fn relay_approved_artifact<T, A>(
    transfer_uid: &str,
    material_kind: RelayMaterialKind,
    artifact_bytes: &[u8],
    asserted_producer: RelayParty,
    destination: RelayParty,
    trusted_receipt_signer: &TrustedReceiptSigner,
    approve: A,
    transport: &mut T,
) -> Result<RelayDelivery, RelayError>
where
    T: RelayTransport,
    A: FnOnce(&RelayReview) -> bool,
{
    let prepared =
        prepare_approved_artifact(transfer_uid, material_kind, artifact_bytes, asserted_producer, destination, approve)?;
    let PreparedRelay { review, envelope } = prepared;
    let encoded = serde_json::to_vec(&envelope).map_err(|_| RelayError::EnvelopeEncoding)?;

    for attempts in 1..=MAX_DELIVERY_ATTEMPTS {
        let Ok(Some(receipt_bytes)) = transport.deliver(&encoded) else {
            continue;
        };
        let receipt = verify_receipt(&receipt_bytes, &envelope, trusted_receipt_signer)?;
        return Ok(RelayDelivery {
            review,
            envelope,
            receipt,
            receipt_bytes,
            attempts,
        });
    }
    Err(RelayError::DeliveryUnknown)
}

pub fn prepare_approved_artifact<A>(
    transfer_uid: &str,
    material_kind: RelayMaterialKind,
    artifact_bytes: &[u8],
    asserted_producer: RelayParty,
    destination: RelayParty,
    approve: A,
) -> Result<PreparedRelay, RelayError>
where
    A: FnOnce(&RelayReview) -> bool,
{
    validate_transfer_uid(transfer_uid)?;
    validate_route(material_kind, &asserted_producer, &destination)?;
    if artifact_bytes.is_empty() || artifact_bytes.len() > MAX_RELAY_ARTIFACT_BYTES {
        return Err(RelayError::InvalidArtifact);
    }

    let artifact_sha256 = hex_lower(&Sha256::digest(artifact_bytes));
    let review = RelayReview {
        transfer_uid: transfer_uid.to_owned(),
        material_kind,
        direction: direction_for(material_kind),
        artifact_sha256: artifact_sha256.clone(),
        artifact_size_bytes: artifact_bytes.len(),
        asserted_producer: asserted_producer.clone(),
        destination: destination.clone(),
    };
    if !approve(&review) {
        return Err(RelayError::ApprovalRequired);
    }

    let envelope = RelayEnvelope {
        format_version: RELAY_ENVELOPE_FORMAT.to_owned(),
        protocol_version: "v1".to_owned(),
        transfer_uid: transfer_uid.to_owned(),
        material_kind,
        direction: review.direction,
        artifact: RelayArtifact {
            encoding: "base64".to_owned(),
            bytes: BASE64_STANDARD.encode_to_string(artifact_bytes),
            sha256: artifact_sha256,
        },
        asserted_producer,
        destination,
    };
    Ok(PreparedRelay { review, envelope })
}

pub fn decode_relay_envelope(envelope_bytes: &[u8]) -> Result<(RelayEnvelope, Vec<u8>), RelayError> {
    if envelope_bytes.is_empty() || envelope_bytes.len() > MAX_RELAY_ENVELOPE_BYTES {
        return Err(RelayError::EnvelopeInvalid);
    }
    let envelope: RelayEnvelope = serde_json::from_slice(envelope_bytes).map_err(|_| RelayError::EnvelopeInvalid)?;
    if envelope.format_version != RELAY_ENVELOPE_FORMAT
        || envelope.protocol_version != "v1"
        || validate_transfer_uid(&envelope.transfer_uid).is_err()
        || validate_route(envelope.material_kind, &envelope.asserted_producer, &envelope.destination).is_err()
        || envelope.direction != direction_for(envelope.material_kind)
        || envelope.artifact.encoding != "base64"
        || !is_sha256(&envelope.artifact.sha256)
    {
        return Err(RelayError::EnvelopeInvalid);
    }
    let artifact_bytes = BASE64_STANDARD
        .decode_to_vec(envelope.artifact.bytes.as_bytes())
        .map_err(|_| RelayError::EnvelopeInvalid)?;
    if artifact_bytes.is_empty()
        || artifact_bytes.len() > MAX_RELAY_ARTIFACT_BYTES
        || BASE64_STANDARD.encode_to_string(&artifact_bytes) != envelope.artifact.bytes
    {
        return Err(RelayError::InvalidArtifact);
    }
    if hex_lower(&Sha256::digest(&artifact_bytes)) != envelope.artifact.sha256 {
        return Err(RelayError::ArtifactDigestMismatch);
    }
    Ok((envelope, artifact_bytes))
}

pub fn verify_receipt(
    receipt_bytes: &[u8],
    envelope: &RelayEnvelope,
    trusted_signer: &TrustedReceiptSigner,
) -> Result<RelayReceiptPayload, RelayError> {
    let signed: SignedRelayReceipt = serde_json::from_slice(receipt_bytes).map_err(|_| RelayError::ReceiptInvalid)?;
    if signed.signature.algorithm != "Ed25519" || signed.signature.key_id != trusted_signer.key_id {
        return Err(RelayError::ReceiptSignerUntrusted);
    }

    let payload = decode_canonical_base64url(&signed.payload).ok_or(RelayError::ReceiptInvalid)?;
    let signature = decode_canonical_base64url(&signed.signature.value).ok_or(RelayError::ReceiptInvalid)?;
    let signature: [u8; 64] = signature.try_into().map_err(|_| RelayError::ReceiptInvalid)?;
    let mut signed_bytes = Vec::with_capacity(RELAY_RECEIPT_DOMAIN_SEPARATION_TAG.len() + 1 + payload.len());
    signed_bytes.extend_from_slice(RELAY_RECEIPT_DOMAIN_SEPARATION_TAG.as_bytes());
    signed_bytes.push(0);
    signed_bytes.extend_from_slice(&payload);
    trusted_signer
        .verifying_key
        .verify_strict(&signed_bytes, &Signature::from_bytes(&signature))
        .map_err(|_| RelayError::ReceiptSignatureInvalid)?;

    let receipt: RelayReceiptPayload = serde_json::from_slice(&payload).map_err(|_| RelayError::ReceiptInvalid)?;
    if receipt.format_version != RELAY_RECEIPT_FORMAT
        || receipt.protocol_version != envelope.protocol_version
        || receipt.transfer_uid != envelope.transfer_uid
        || receipt.material_kind != envelope.material_kind
        || receipt.direction != envelope.direction
        || receipt.artifact_sha256 != envelope.artifact.sha256
        || receipt.producer != envelope.asserted_producer
        || receipt.destination != envelope.destination
    {
        return Err(RelayError::ReceiptMismatch);
    }
    Ok(receipt)
}

fn validate_route(
    material_kind: RelayMaterialKind,
    asserted_producer: &RelayParty,
    destination: &RelayParty,
) -> Result<(), RelayError> {
    if !valid_party(asserted_producer) || !valid_party(destination) {
        return Err(RelayError::InvalidParty);
    }
    let valid = match material_kind {
        RelayMaterialKind::OfflineEnrollmentResponse | RelayMaterialKind::DiagnosticBundleManifest => {
            asserted_producer.party_type == "DEVICE"
                && asserted_producer.key_id.is_some()
                && destination.party_type == "CONNECT"
                && destination.key_id.is_none()
        }
        RelayMaterialKind::ServiceLicense => {
            asserted_producer.party_type == "CONNECT_LICENSE_ISSUER"
                && asserted_producer.key_id.is_some()
                && destination.party_type == "CLUSTER"
                && destination.key_id.is_none()
        }
    };
    valid.then_some(()).ok_or(RelayError::UnsupportedMaterial)
}

const fn direction_for(material_kind: RelayMaterialKind) -> RelayDirection {
    match material_kind {
        RelayMaterialKind::OfflineEnrollmentResponse | RelayMaterialKind::DiagnosticBundleManifest => {
            RelayDirection::ClusterToConnect
        }
        RelayMaterialKind::ServiceLicense => RelayDirection::ConnectToCluster,
    }
}

fn valid_party(party: &RelayParty) -> bool {
    !party.party_type.is_empty()
        && party.party_type.len() <= 64
        && !party.name.is_empty()
        && party.name.len() <= 1024
        && party.key_id.as_deref().is_none_or(is_sha256)
}

fn validate_transfer_uid(value: &str) -> Result<(), RelayError> {
    is_uuid_v7(value).then_some(()).ok_or(RelayError::InvalidTransferUid)
}

fn is_uuid_v7(value: &str) -> bool {
    Uuid::parse_str(value).is_ok_and(|uuid| {
        uuid.get_version() == Some(Version::SortRand) && uuid.get_variant() == Variant::RFC4122 && uuid.to_string() == value
    })
}

fn retryable_status(status: StatusCode) -> bool {
    matches!(status.as_u16(), 408 | 425 | 429 | 500 | 502 | 503 | 504)
}

async fn bounded_body(mut response: reqwest::Response) -> Result<Vec<u8>, RelayError> {
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| RelayError::Transport)? {
        if body.len().saturating_add(chunk.len()) > MAX_RECEIPT_BYTES {
            return Err(RelayError::ResponseTooLarge);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

pub fn read_protected_relay_artifact(path: &Path) -> Result<Vec<u8>, RelayError> {
    read_protected_bytes(path, MAX_RELAY_ARTIFACT_BYTES as u64)
}

pub fn read_protected_relay_authentication(path: &Path) -> Result<String, RelayError> {
    let bytes = read_protected_bytes(path, MAX_AUTHENTICATION_BYTES)?;
    let value = std::str::from_utf8(&bytes)
        .map_err(|_| RelayError::AuthenticationInvalid)?
        .trim()
        .to_owned();
    if value.is_empty() || value.bytes().any(|byte| byte == b'\r' || byte == b'\n') {
        return Err(RelayError::AuthenticationInvalid);
    }
    Ok(value)
}

#[cfg(unix)]
fn read_protected_bytes(path: &Path, maximum: u64) -> Result<Vec<u8>, RelayError> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|_| RelayError::InputFile)?;
    let metadata = file.metadata().map_err(|_| RelayError::InputFile)?;
    if !metadata.is_file() || metadata.uid() != process_uid() || metadata.permissions().mode() & 0o077 != 0 {
        return Err(RelayError::CredentialFileSecurity);
    }
    if metadata.len() == 0 || metadata.len() > maximum {
        return Err(RelayError::InputFile);
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(maximum + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| RelayError::InputFile)?;
    if bytes.is_empty() || bytes.len() as u64 > maximum {
        return Err(RelayError::InputFile);
    }
    Ok(bytes)
}

#[cfg(unix)]
#[allow(unsafe_code)]
fn process_uid() -> u32 {
    // SAFETY: geteuid has no pointer arguments or caller preconditions.
    unsafe { libc::geteuid() }
}

#[cfg(not(unix))]
fn read_protected_bytes(_path: &Path, _maximum: u64) -> Result<Vec<u8>, RelayError> {
    Err(RelayError::CredentialFileSecurity)
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn decode_canonical_base64url(value: &str) -> Option<Vec<u8>> {
    if value.contains('=') {
        return None;
    }
    let decoded = URL_SAFE_NO_PAD.decode_to_vec(value.as_bytes()).ok()?;
    (URL_SAFE_NO_PAD.encode_to_string(&decoded) == value).then_some(decoded)
}

fn hex_lower(bytes: &[u8]) -> String {
    hex_simd::encode_to_string(bytes, hex_simd::AsciiCase::Lower)
}
