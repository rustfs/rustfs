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

use std::time::Duration;

use base64::Engine as _;
use reqwest::{Client, StatusCode, Url};
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, pem::PemObject as _};
use serde::Deserialize;
use uuid::Uuid;
use zeroize::Zeroizing;

use super::credential_store::{
    CompletedRegistration, CredentialStore, CredentialStoreError, DeviceCredential, PendingRegistration, PendingRotation,
};
use super::identity::{IdentityError, RegistrationTranscript};
use super::identity_store::{IdentityStore, StoreError};
use super::registration::{
    CredentialResponse, CredentialValidationError, ExpectedDevice, RegistrationRequest, RegistrationToken, RotationRequest,
    certificate_fingerprint, certificate_request_matches, public_key_fingerprint, validate_credential,
    validate_stored_credential,
};

const MAX_ATTEMPTS: usize = 3;
const MAX_RESPONSE_BYTES: usize = 1024 * 1024;
const ROTATION_THRESHOLD_SECONDS: i64 = 8 * 60 * 60;

pub struct ConnectConfig<'a> {
    pub endpoint: &'a str,
    pub root_ca_pem: &'a [u8],
    pub timeout: Duration,
}

pub struct ConnectClient {
    endpoint: Url,
    roots: RootCertStore,
    root_certificates: Vec<CertificateDer<'static>>,
    client: Client,
    timeout: Duration,
}

impl ConnectClient {
    pub fn from_optional_config(config: Option<ConnectConfig<'_>>) -> Result<Option<Self>, ClientError> {
        config.map(Self::new).transpose()
    }

    pub fn new(config: ConnectConfig<'_>) -> Result<Self, ClientError> {
        let mut endpoint = Url::parse(config.endpoint).map_err(|_| ClientError::Endpoint)?;
        if endpoint.scheme() != "https"
            || endpoint.cannot_be_a_base()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
        {
            return Err(ClientError::Endpoint);
        }
        if !endpoint.path().ends_with('/') {
            let path = format!("{}/", endpoint.path());
            endpoint.set_path(&path);
        }

        let root_certificates = CertificateDer::pem_slice_iter(config.root_ca_pem)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| ClientError::RootCertificate)?;
        if root_certificates.is_empty() {
            return Err(ClientError::RootCertificate);
        }
        let mut roots = RootCertStore::empty();
        let (accepted, rejected) = roots.add_parsable_certificates(root_certificates.clone());
        if accepted != root_certificates.len() || rejected != 0 {
            return Err(ClientError::RootCertificate);
        }

        let client = build_client(&root_certificates, config.timeout, None)?;
        Ok(Self {
            endpoint,
            roots,
            root_certificates,
            client,
            timeout: config.timeout,
        })
    }

    pub async fn register(
        &self,
        identity_store: &IdentityStore,
        credential_store: &CredentialStore,
        token: &RegistrationToken,
    ) -> Result<DeviceCredential, ClientError> {
        let _lock = credential_store.lock().await?;
        if let Some((credential, _)) = self.load_valid_credential(identity_store, credential_store)? {
            ensure_credential_time(&credential, unix_now())?;
            return Ok(credential);
        }

        let identity = identity_store.load_or_create()?;
        let candidate = PendingRegistration {
            token_uid: token.registration_token_uid.clone(),
            request_id: Uuid::new_v4().to_string(),
            certificate_request: identity.certificate_request_base64()?,
            previous_credential_fingerprint: None,
            next_public_key_sha256: None,
        };
        let pending = credential_store.claim_pending_registration(&candidate)?;
        if pending.token_uid != token.registration_token_uid
            || pending.previous_credential_fingerprint.is_some()
            || pending.next_public_key_sha256.is_some()
            || !is_request_id(&pending.request_id)
            || !certificate_request_matches(&pending.certificate_request, &identity)?
        {
            return Err(ClientError::PendingRegistration);
        }

        let credential = match self.exchange_registration(token, &pending, &identity).await {
            Ok(credential) => credential,
            Err(error @ (ClientError::AccessRevoked { .. } | ClientError::Rejected { .. })) => {
                credential_store.clear_pending_registration()?;
                return Err(error);
            }
            Err(error) => return Err(error),
        };
        credential_store.save(&credential)?;
        credential_store.clear_pending_registration()?;
        Ok(credential)
    }

    pub async fn reenroll(
        &self,
        identity_store: &IdentityStore,
        credential_store: &CredentialStore,
        token: &RegistrationToken,
    ) -> Result<DeviceCredential, ClientError> {
        let _lock = credential_store.lock().await?;
        let (credential, _) = self
            .load_valid_credential(identity_store, credential_store)?
            .ok_or(ClientError::NotRegistered)?;
        let fingerprint = certificate_fingerprint(&credential.certificate)?;
        if credential_store.load_completed_registration()?.is_some_and(|completed| {
            completed.token_uid == token.registration_token_uid && completed.credential_fingerprint == fingerprint
        }) {
            return Ok(credential);
        }
        credential_store.clear_pending_rotation()?;
        let next = identity_store.load_or_create_next()?;
        let next_fingerprint = public_key_fingerprint(&next);
        let candidate = PendingRegistration {
            token_uid: token.registration_token_uid.clone(),
            request_id: Uuid::new_v4().to_string(),
            certificate_request: next.certificate_request_base64()?,
            previous_credential_fingerprint: Some(fingerprint.clone()),
            next_public_key_sha256: Some(next_fingerprint.clone()),
        };
        let pending = credential_store.claim_pending_registration(&candidate)?;
        if pending.token_uid != token.registration_token_uid
            || pending.previous_credential_fingerprint.as_deref() != Some(&fingerprint)
            || pending.next_public_key_sha256.as_deref() != Some(&next_fingerprint)
            || !is_request_id(&pending.request_id)
            || !certificate_request_matches(&pending.certificate_request, &next)?
        {
            return Err(ClientError::PendingRegistration);
        }
        let enrolled = match self.exchange_registration(token, &pending, &next).await {
            Ok(credential) => credential,
            Err(error @ (ClientError::AccessRevoked { .. } | ClientError::Rejected { .. })) => {
                credential_store.clear_pending_registration()?;
                identity_store.clear_next()?;
                return Err(error);
            }
            Err(error) => return Err(error),
        };
        credential_store.save(&enrolled)?;
        identity_store.commit_next(&next)?;
        credential_store.save_completed_registration(&CompletedRegistration {
            token_uid: token.registration_token_uid.clone(),
            credential_fingerprint: certificate_fingerprint(&enrolled.certificate)?,
        })?;
        credential_store.clear_pending_registration()?;
        Ok(enrolled)
    }

    async fn exchange_registration(
        &self,
        token: &RegistrationToken,
        pending: &PendingRegistration,
        identity: &super::identity::DeviceIdentity,
    ) -> Result<DeviceCredential, ClientError> {
        let csr_der = base64::engine::general_purpose::STANDARD
            .decode(&pending.certificate_request)
            .map_err(|_| ClientError::PendingRegistration)?;
        let transcript = RegistrationTranscript::build(
            &token.registration_token_uid,
            &token.organization_uid,
            &token.cluster_uid,
            &pending.request_id,
            &token.challenge_nonce,
            token.expires_unix,
            &csr_der,
        )?;
        let proof = identity.sign_registration(&transcript);
        let body = RegistrationRequest::new(token, &pending.request_id, &pending.certificate_request, &proof);
        let url = self.url("./registrationTokens:exchange")?;
        let response = self
            .send(StatusCode::CREATED, || self.client.post(url.clone()).json(&body))
            .await?;
        let cluster = format!("organizations/{}/clusters/{}", token.organization_uid, token.cluster_uid);
        let credential = validate_credential(
            response,
            identity,
            &self.roots,
            &self.root_certificates,
            ExpectedDevice::Registration { cluster: &cluster },
        )?;
        Ok(credential)
    }

    pub async fn rotate_if_due(
        &self,
        identity_store: &IdentityStore,
        credential_store: &CredentialStore,
        now_unix: i64,
    ) -> Result<Option<DeviceCredential>, ClientError> {
        let _lock = credential_store.lock().await?;
        let (credential, identity) = self
            .load_valid_credential(identity_store, credential_store)?
            .ok_or(ClientError::NotRegistered)?;
        if credential_store.load_pending_registration()?.is_some() {
            return Err(ClientError::PendingRegistration);
        }
        ensure_credential_time(&credential, now_unix)?;
        if credential.not_after_unix - now_unix > ROTATION_THRESHOLD_SECONDS {
            return Ok(None);
        }
        let fingerprint = certificate_fingerprint(&credential.certificate)?;
        let next = identity_store.load_or_create_next()?;
        let candidate = PendingRotation {
            credential_fingerprint: fingerprint.clone(),
            device_name: credential.name.clone(),
            request_id: Uuid::new_v4().to_string(),
            certificate_request: next.certificate_request_base64()?,
            next_public_key_sha256: public_key_fingerprint(&next),
        };
        let pending = credential_store.claim_pending_rotation(&candidate)?;
        if pending.credential_fingerprint != fingerprint
            || pending.device_name != credential.name
            || !is_request_id(&pending.request_id)
            || pending.next_public_key_sha256 != public_key_fingerprint(&next)
            || !certificate_request_matches(&pending.certificate_request, &next)?
        {
            return Err(ClientError::PendingRotation);
        }
        let body = RotationRequest::new(
            &identity,
            &fingerprint,
            &credential.name,
            &pending.request_id,
            &pending.certificate_request,
        )?;
        let private_key = identity.to_pkcs8_pem()?;
        let mut identity_pem = Zeroizing::new(Vec::with_capacity(credential.certificate_chain.len() + private_key.len() + 1));
        identity_pem.extend_from_slice(credential.certificate_chain.as_bytes());
        identity_pem.push(b'\n');
        identity_pem.extend_from_slice(private_key.as_bytes());
        let tls_identity = reqwest::Identity::from_pem(&identity_pem).map_err(|_| ClientError::IdentityCertificate)?;
        let client = build_client(&self.root_certificates, self.timeout, Some(tls_identity))?;
        let path = format!("clusterDevices/{}:rotateCredential", credential.uid);
        let url = self.url(&path)?;
        let response = self.send(StatusCode::OK, || client.post(url.clone()).json(&body)).await?;
        let rotated = validate_credential(
            response,
            &next,
            &self.roots,
            &self.root_certificates,
            ExpectedDevice::Rotation { name: &credential.name },
        )?;
        if rotated.name != credential.name || rotated.uid != credential.uid {
            return Err(ClientError::Credential(CredentialValidationError::Identity));
        }
        credential_store.save(&rotated)?;
        identity_store.commit_next(&next)?;
        credential_store.clear_pending_rotation()?;
        Ok(Some(rotated))
    }

    fn load_valid_credential(
        &self,
        identity_store: &IdentityStore,
        credential_store: &CredentialStore,
    ) -> Result<Option<(DeviceCredential, super::identity::DeviceIdentity)>, ClientError> {
        let Some(credential) = credential_store.load()? else {
            return Ok(None);
        };
        let current = identity_store.load()?.ok_or(ClientError::IdentityMissing)?;
        if let Some(pending) = credential_store.load_pending_registration()? {
            let Some(previous) = pending.previous_credential_fingerprint.as_deref() else {
                if pending.next_public_key_sha256.is_some()
                    || !is_request_id(&pending.request_id)
                    || !certificate_request_matches(&pending.certificate_request, &current)?
                {
                    return Err(ClientError::PendingRegistration);
                }
                validate_stored_credential(&credential, &current, &self.roots, &self.root_certificates)?;
                credential_store.clear_pending_registration()?;
                return Ok(Some((credential, current)));
            };
            let next_fingerprint = pending
                .next_public_key_sha256
                .as_deref()
                .ok_or(ClientError::PendingRegistration)?;
            if !is_request_id(&pending.request_id) {
                return Err(ClientError::PendingRegistration);
            }
            let fingerprint = certificate_fingerprint(&credential.certificate)?;
            if fingerprint == previous {
                validate_stored_credential(&credential, &current, &self.roots, &self.root_certificates)?;
                let next = identity_store.load_next()?.ok_or(ClientError::PendingRegistration)?;
                if public_key_fingerprint(&next) != next_fingerprint
                    || !certificate_request_matches(&pending.certificate_request, &next)?
                {
                    return Err(ClientError::PendingRegistration);
                }
                return Ok(Some((credential, current)));
            }
            if public_key_fingerprint(&current) == next_fingerprint {
                if !certificate_request_matches(&pending.certificate_request, &current)? {
                    return Err(ClientError::PendingRegistration);
                }
                validate_stored_credential(&credential, &current, &self.roots, &self.root_certificates)?;
            } else {
                let next = identity_store.load_next()?.ok_or(ClientError::PendingRegistration)?;
                if public_key_fingerprint(&next) != next_fingerprint
                    || !certificate_request_matches(&pending.certificate_request, &next)?
                {
                    return Err(ClientError::PendingRegistration);
                }
                validate_stored_credential(&credential, &next, &self.roots, &self.root_certificates)?;
                identity_store.commit_next(&next)?;
            }
            credential_store.save_completed_registration(&CompletedRegistration {
                token_uid: pending.token_uid,
                credential_fingerprint: fingerprint,
            })?;
            credential_store.clear_pending_registration()?;
            let current = identity_store.load()?.ok_or(ClientError::IdentityMissing)?;
            return Ok(Some((credential, current)));
        }
        let Some(pending) = credential_store.load_pending_rotation()? else {
            validate_stored_credential(&credential, &current, &self.roots, &self.root_certificates)?;
            return Ok(Some((credential, current)));
        };
        let fingerprint = certificate_fingerprint(&credential.certificate)?;
        if pending.device_name != credential.name || !is_request_id(&pending.request_id) {
            return Err(ClientError::PendingRotation);
        }
        if fingerprint == pending.credential_fingerprint {
            validate_stored_credential(&credential, &current, &self.roots, &self.root_certificates)?;
            let next = identity_store.load_next()?.ok_or(ClientError::PendingRotation)?;
            if pending.next_public_key_sha256 != public_key_fingerprint(&next)
                || !certificate_request_matches(&pending.certificate_request, &next)?
            {
                return Err(ClientError::PendingRotation);
            }
            return Ok(Some((credential, current)));
        }

        if public_key_fingerprint(&current) == pending.next_public_key_sha256 {
            if !certificate_request_matches(&pending.certificate_request, &current)? {
                return Err(ClientError::PendingRotation);
            }
            validate_stored_credential(&credential, &current, &self.roots, &self.root_certificates)?;
        } else {
            let next = identity_store.load_next()?.ok_or(ClientError::PendingRotation)?;
            if public_key_fingerprint(&next) != pending.next_public_key_sha256
                || !certificate_request_matches(&pending.certificate_request, &next)?
            {
                return Err(ClientError::PendingRotation);
            }
            validate_stored_credential(&credential, &next, &self.roots, &self.root_certificates)?;
            identity_store.commit_next(&next)?;
        }
        credential_store.clear_pending_rotation()?;
        let current = identity_store.load()?.ok_or(ClientError::IdentityMissing)?;
        Ok(Some((credential, current)))
    }

    async fn send<F>(&self, success: StatusCode, mut request: F) -> Result<CredentialResponse, ClientError>
    where
        F: FnMut() -> reqwest::RequestBuilder,
    {
        let mut last_status = None;
        for attempt in 0..MAX_ATTEMPTS {
            match request().send().await {
                Ok(response) if response.status() == success => return decode_response(response).await,
                Ok(response) if matches!(response.status(), StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN) => {
                    let status = response.status();
                    let reason = decode_reason(response).await;
                    return Err(ClientError::AccessRevoked { status, reason });
                }
                Ok(response) if matches!(response.status(), StatusCode::REQUEST_TIMEOUT | StatusCode::TOO_MANY_REQUESTS) => {
                    last_status = Some(response.status());
                }
                Ok(response) if response.status().is_client_error() => {
                    let status = response.status();
                    let reason = decode_reason(response).await;
                    return Err(ClientError::Rejected { status, reason });
                }
                Ok(response) if response.status().is_server_error() => {
                    last_status = Some(response.status());
                }
                Ok(response) => {
                    let status = response.status();
                    let reason = decode_reason(response).await;
                    return Err(ClientError::Rejected { status, reason });
                }
                Err(error) if !error.is_timeout() && !error.is_connect() => return Err(ClientError::Transport(error)),
                Err(_) => {}
            }

            if attempt + 1 < MAX_ATTEMPTS {
                tokio::time::sleep(Duration::from_millis(50 * (attempt as u64 + 1))).await;
            }
        }
        Err(ClientError::Unavailable { status: last_status })
    }

    fn url(&self, path: &str) -> Result<Url, ClientError> {
        self.endpoint.join(path).map_err(|_| ClientError::Endpoint)
    }
}

fn is_request_id(value: &str) -> bool {
    Uuid::parse_str(value).is_ok_and(|uuid| uuid.get_version() == Some(uuid::Version::Random) && uuid.to_string() == value)
}

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs() as i64)
}

fn ensure_credential_time(credential: &DeviceCredential, now_unix: i64) -> Result<(), ClientError> {
    if credential.not_before_unix > now_unix {
        return Err(ClientError::CredentialNotYetValid);
    }
    if credential.not_after_unix <= now_unix {
        return Err(ClientError::CredentialExpired);
    }
    Ok(())
}

fn build_client(
    roots: &[CertificateDer<'static>],
    timeout: Duration,
    identity: Option<reqwest::Identity>,
) -> Result<Client, ClientError> {
    let certificates = roots
        .iter()
        .map(|root| reqwest::Certificate::from_der(root.as_ref()))
        .collect::<Result<Vec<_>, _>>()?;
    let mut builder = Client::builder()
        .https_only(true)
        .redirect(reqwest::redirect::Policy::none())
        .timeout(timeout)
        .tls_certs_only(certificates);
    if let Some(identity) = identity {
        builder = builder.identity(identity);
    }
    builder.build().map_err(ClientError::Transport)
}

async fn decode_response(mut response: reqwest::Response) -> Result<CredentialResponse, ClientError> {
    let body = read_body(&mut response).await?;
    serde_json::from_slice(&body).map_err(|_| ClientError::Response)
}

async fn read_body(response: &mut reqwest::Response) -> Result<Vec<u8>, ClientError> {
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(ClientError::Transport)? {
        if body.len() + chunk.len() > MAX_RESPONSE_BYTES {
            return Err(ClientError::ResponseTooLarge);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

#[derive(Deserialize)]
struct ErrorEnvelope {
    #[serde(default)]
    details: Vec<ErrorDetail>,
}

#[derive(Deserialize)]
struct ErrorDetail {
    #[serde(default)]
    reason: String,
}

async fn decode_reason(mut response: reqwest::Response) -> Option<String> {
    let body = read_body(&mut response).await.ok()?;
    serde_json::from_slice::<ErrorEnvelope>(&body)
        .ok()?
        .details
        .into_iter()
        .find_map(|detail| (!detail.reason.is_empty()).then_some(detail.reason))
}

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("Connect endpoint must be an HTTPS base URL without credentials, query, or fragment")]
    Endpoint,
    #[error("Connect root CA configuration is invalid")]
    RootCertificate,
    #[error(
        "Connect registration has a pending attempt for a different token; restore the original protected token configuration"
    )]
    PendingRegistration,
    #[error(
        "Connect credential rotation has an unfinished attempt for a different current certificate; inspect the local credential store"
    )]
    PendingRotation,
    #[error("RustFS is not registered with Connect")]
    NotRegistered,
    #[error("the Connect device private key is missing; restore device.key before using the stored certificate")]
    IdentityMissing,
    #[error("the Connect device certificate has expired; call ConnectClient::reenroll with a fresh registration token")]
    CredentialExpired,
    #[error("the Connect device certificate is not yet valid; fix local clock skew or call ConnectClient::reenroll")]
    CredentialNotYetValid,
    #[error("the stored Connect certificate and device private key cannot form a TLS identity")]
    IdentityCertificate,
    #[error(
        "Connect rejected the device credential with HTTP {status}; reason={reason:?}; call ConnectClient::reenroll with a fresh registration token if revoked"
    )]
    AccessRevoked { status: StatusCode, reason: Option<String> },
    #[error("Connect rejected the request with HTTP {status}; reason={reason:?}")]
    Rejected { status: StatusCode, reason: Option<String> },
    #[error("Connect remained unavailable after bounded retries; last_status={status:?}")]
    Unavailable { status: Option<StatusCode> },
    #[error("Connect response exceeded the 1 MiB credential-response limit")]
    ResponseTooLarge,
    #[error("Connect returned an invalid credential response")]
    Response,
    #[error(transparent)]
    Transport(#[from] reqwest::Error),
    #[error(transparent)]
    Identity(#[from] IdentityError),
    #[error(transparent)]
    IdentityStore(#[from] StoreError),
    #[error(transparent)]
    CredentialStore(#[from] CredentialStoreError),
    #[error(transparent)]
    Credential(#[from] CredentialValidationError),
}
