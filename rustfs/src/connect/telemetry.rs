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

use chrono::{DateTime, SecondsFormat, Utc};
use reqwest::{Client, StatusCode, Url, header};
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, pem::PemObject as _};
use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

use super::client::{ClientError, ConnectClient};
use super::config::HeartbeatConfig;
use super::credential_store::{CredentialStoreError, DeviceCredential};
use super::identity::IdentityError;
use super::identity_store::StoreError;
use super::registration::{CredentialValidationError, certificate_fingerprint};

const MAX_RESPONSE_BYTES: usize = 64 * 1024;

pub(crate) enum TelemetryDelivery {
    Accepted { cluster_name: String, body: Vec<u8> },
    Retry { retry_after: Option<Duration> },
    AuthenticationStopped { status: u16, reason: Option<String> },
    Rejected { status: u16, reason: Option<String> },
}

pub(crate) struct TelemetryTransport {
    endpoint: Url,
    root_store: RootCertStore,
    roots: Vec<CertificateDer<'static>>,
    config: HeartbeatConfig,
}

struct AuthenticatedClient {
    cluster_name: String,
    cluster_uid: String,
    credential_fingerprint: String,
    client: Client,
}

impl TelemetryTransport {
    pub(crate) fn new(config: HeartbeatConfig) -> Result<Self, TelemetryError> {
        let mut endpoint = Url::parse(&config.endpoint).map_err(|_| TelemetryError::Endpoint)?;
        if endpoint.scheme() != "https"
            || endpoint.cannot_be_a_base()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
        {
            return Err(TelemetryError::Endpoint);
        }
        if !endpoint.path().ends_with('/') {
            endpoint.set_path(&format!("{}/", endpoint.path()));
        }
        let roots = CertificateDer::pem_slice_iter(&config.root_ca_pem)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| TelemetryError::RootCertificate)?;
        if roots.is_empty() {
            return Err(TelemetryError::RootCertificate);
        }
        let mut root_store = RootCertStore::empty();
        let (accepted, rejected) = root_store.add_parsable_certificates(roots.clone());
        if accepted != roots.len() || rejected != 0 {
            return Err(TelemetryError::RootCertificate);
        }
        let schedule = config.schedule;
        if schedule.timeout.is_zero()
            || schedule.timeout > Duration::from_secs(5)
            || schedule.initial_backoff.is_zero()
            || schedule.max_backoff < schedule.initial_backoff
            || schedule.max_backoff > Duration::from_secs(5 * 60)
        {
            return Err(TelemetryError::Schedule);
        }
        Ok(Self {
            endpoint,
            root_store,
            roots,
            config,
        })
    }

    pub(crate) async fn post<T: Serialize>(&self, collection: &str, value: &T) -> Result<TelemetryDelivery, TelemetryError> {
        let mut authenticated = self.authenticated_client().await?;
        let mut refreshed = false;
        loop {
            let url = self
                .endpoint
                .join(&format!("clusters/{}/{collection}", authenticated.cluster_uid))?;
            let response = match authenticated.client.post(url).json(value).send().await {
                Ok(response) => response,
                Err(error) if error.is_timeout() || error.is_connect() || error.is_request() => {
                    return Ok(TelemetryDelivery::Retry { retry_after: None });
                }
                Err(error) => return Err(error.into()),
            };
            let status = response.status();
            if matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN) && !refreshed {
                let current = self.authenticated_client().await?;
                if current.credential_fingerprint != authenticated.credential_fingerprint {
                    authenticated = current;
                    refreshed = true;
                    continue;
                }
            }
            if status == StatusCode::TOO_MANY_REQUESTS {
                return Ok(TelemetryDelivery::Retry {
                    retry_after: retry_after(response.headers(), Utc::now(), self.config.schedule.max_backoff),
                });
            }
            if status == StatusCode::REQUEST_TIMEOUT || status.is_server_error() {
                return Ok(TelemetryDelivery::Retry { retry_after: None });
            }
            if matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN) {
                return Ok(TelemetryDelivery::AuthenticationStopped {
                    status: status.as_u16(),
                    reason: response_reason(response).await,
                });
            }
            if status != StatusCode::OK {
                return Ok(TelemetryDelivery::Rejected {
                    status: status.as_u16(),
                    reason: response_reason(response).await,
                });
            }
            return Ok(TelemetryDelivery::Accepted {
                cluster_name: authenticated.cluster_name,
                body: bounded_body(response).await?,
            });
        }
    }

    async fn authenticated_client(&self) -> Result<AuthenticatedClient, TelemetryError> {
        let lock = self.config.credential_store.lock().await?;
        let (credential, identity, _) = ConnectClient::recover_valid_credential_locked(
            &lock,
            &self.config.identity_store,
            &self.config.credential_store,
            &self.root_store,
            &self.roots,
        )
        .map_err(credential_recovery_error)?
        .ok_or(TelemetryError::NotRegistered)?;
        let now = Utc::now().timestamp();
        if now < credential.not_before_unix || now >= credential.not_after_unix {
            return Err(TelemetryError::CredentialExpired);
        }
        let (organization_uid, cluster_uid) = credential_parent(&credential)?;
        let cluster_name = format!("organizations/{organization_uid}/clusters/{cluster_uid}");
        let credential_fingerprint = certificate_fingerprint(&credential.certificate)?;
        let client = self.client(&credential, &identity.to_pkcs8_pem()?)?;
        Ok(AuthenticatedClient {
            cluster_name,
            cluster_uid: cluster_uid.to_owned(),
            credential_fingerprint,
            client,
        })
    }

    fn client(&self, credential: &DeviceCredential, key: &Zeroizing<String>) -> Result<Client, TelemetryError> {
        let mut pem = Zeroizing::new(Vec::with_capacity(credential.certificate_chain.len() + key.len() + 1));
        pem.extend_from_slice(credential.certificate_chain.as_bytes());
        pem.push(b'\n');
        pem.extend_from_slice(key.as_bytes());
        let identity = reqwest::Identity::from_pem(&pem).map_err(|_| TelemetryError::IdentityCertificate)?;
        let roots = self
            .roots
            .iter()
            .map(|root| reqwest::Certificate::from_der(root.as_ref()))
            .collect::<Result<Vec<_>, _>>()?;
        Client::builder()
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .timeout(self.config.schedule.timeout)
            .tls_certs_only(roots)
            .identity(identity)
            .build()
            .map_err(Into::into)
    }
}

fn credential_recovery_error(error: ClientError) -> TelemetryError {
    match error {
        ClientError::Endpoint => TelemetryError::Endpoint,
        ClientError::RootCertificate => TelemetryError::RootCertificate,
        ClientError::PendingRegistration | ClientError::PendingRotation => TelemetryError::StateConflict,
        ClientError::NotRegistered => TelemetryError::NotRegistered,
        ClientError::IdentityMissing => TelemetryError::IdentityMissing,
        ClientError::CredentialExpired | ClientError::CredentialNotYetValid => TelemetryError::CredentialExpired,
        ClientError::IdentityCertificate => TelemetryError::IdentityCertificate,
        ClientError::Identity(error) => TelemetryError::Identity(error),
        ClientError::IdentityStore(error) => TelemetryError::IdentityStore(error),
        ClientError::CredentialStore(error) => TelemetryError::CredentialStore(error),
        ClientError::Credential(error) => TelemetryError::CredentialValidation(error),
        ClientError::AccessRevoked { .. }
        | ClientError::Rejected { .. }
        | ClientError::Unavailable { .. }
        | ClientError::ResponseTooLarge
        | ClientError::Response
        | ClientError::Transport(_) => TelemetryError::StateConflict,
    }
}

fn credential_parent(credential: &DeviceCredential) -> Result<(&str, &str), TelemetryError> {
    let mut parts = credential.name.split('/');
    let valid = parts.next() == Some("organizations");
    let organization_uid = parts.next();
    let valid = valid && parts.next() == Some("clusters");
    let cluster_uid = parts.next();
    let valid = valid && parts.next() == Some("clusterDevices");
    let device_uid = parts.next();
    if !valid
        || organization_uid.is_none_or(str::is_empty)
        || cluster_uid.is_none_or(str::is_empty)
        || device_uid != Some(credential.uid.as_str())
        || parts.next().is_some()
    {
        return Err(TelemetryError::CredentialName);
    }
    Ok((
        organization_uid.ok_or(TelemetryError::CredentialName)?,
        cluster_uid.ok_or(TelemetryError::CredentialName)?,
    ))
}

fn retry_after(headers: &header::HeaderMap, now: DateTime<Utc>, maximum: Duration) -> Option<Duration> {
    let value = headers.get(header::RETRY_AFTER)?.to_str().ok()?;
    let delay = value.parse::<u64>().ok().map(Duration::from_secs).or_else(|| {
        DateTime::parse_from_rfc2822(value)
            .ok()
            .and_then(|at| (at.with_timezone(&Utc) - now).to_std().ok())
    })?;
    Some(delay.min(maximum))
}

pub(crate) fn is_exact_utc_seconds(value: &str) -> bool {
    DateTime::parse_from_rfc3339(value).is_ok_and(|time| {
        time.offset().local_minus_utc() == 0
            && value.ends_with('Z')
            && time.with_timezone(&Utc).to_rfc3339_opts(SecondsFormat::Secs, true) == value
    })
}

async fn response_reason(response: reqwest::Response) -> Option<String> {
    #[derive(Deserialize)]
    struct Envelope {
        #[serde(default)]
        details: Vec<Detail>,
    }
    #[derive(Deserialize)]
    struct Detail {
        #[serde(default)]
        reason: String,
    }

    serde_json::from_slice::<Envelope>(&bounded_body(response).await.ok()?)
        .ok()?
        .details
        .into_iter()
        .find_map(|detail| (!detail.reason.is_empty()).then_some(detail.reason))
}

async fn bounded_body(mut response: reqwest::Response) -> Result<Vec<u8>, TelemetryError> {
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if body.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
            return Err(TelemetryError::ResponseTooLarge);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TelemetryError {
    #[error("Connect telemetry endpoint must be an HTTPS base URL without credentials, query, or fragment")]
    Endpoint,
    #[error("Connect telemetry root CA configuration is invalid")]
    RootCertificate,
    #[error("Connect telemetry retry schedule is invalid")]
    Schedule,
    #[error("RustFS is not registered with Connect")]
    NotRegistered,
    #[error("the Connect device private key is missing")]
    IdentityMissing,
    #[error("the stored Connect certificate and device private key cannot form a TLS identity")]
    IdentityCertificate,
    #[error("the stored Connect credential name is invalid")]
    CredentialName,
    #[error("the stored Connect device certificate is not currently valid")]
    CredentialExpired,
    #[error("the persisted Connect credential transition is invalid")]
    StateConflict,
    #[error("Connect telemetry response exceeded 64 KiB")]
    ResponseTooLarge,
    #[error(transparent)]
    Url(#[from] url::ParseError),
    #[error(transparent)]
    Transport(#[from] reqwest::Error),
    #[error(transparent)]
    Identity(#[from] IdentityError),
    #[error(transparent)]
    IdentityStore(#[from] StoreError),
    #[error(transparent)]
    CredentialStore(#[from] CredentialStoreError),
    #[error(transparent)]
    CredentialValidation(#[from] CredentialValidationError),
}
