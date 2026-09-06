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
#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_assignments)]

use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

use crate::services::tier::{
    tier_config::TierS3,
    warm_backend::{
        TransitionCandidateIdentity, TransitionCandidateProbe, TransitionCandidateReconciler, WarmBackend, WarmBackendGetOpts,
        build_transition_put_options, endpoint_authority, transition_client_timeouts_from_env,
    },
};
use http::HeaderMap;
use rustfs_s3_client::{
    api_error_response::to_error_response,
    api_get_options::GetObjectOptions,
    api_list::ListObjectsOptions,
    api_put_object::PutObjectOptions,
    api_remove::{RemoveObjectOptions, RemoveObjectResult},
    api_s3_datatypes::ListVersionsResult,
    credentials::{Credentials, SignatureType, Static, Value},
    provider_versions::validate_remote_version_id,
    transition_api::{BucketLookupType, Options, TransitionClient, TransitionCore},
    transition_api::{ReadCloser, ReaderImpl},
};
use rustfs_utils::egress::validate_outbound_url;
use rustfs_utils::path::SLASH_SEPARATOR;
use s3s::{S3ErrorCode, dto::BucketVersioningStatus};

pub struct WarmBackendS3 {
    pub client: Arc<TransitionClient>,
    pub core: TransitionCore,
    pub bucket: String,
    pub prefix: String,
    pub storage_class: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RemoteBucketVersioning {
    Disabled,
    Suspended,
    Enabled,
}

fn remote_bucket_versioning_from_status(status: Option<&str>) -> Result<RemoteBucketVersioning, std::io::Error> {
    Ok(match status {
        Some(BucketVersioningStatus::ENABLED) => RemoteBucketVersioning::Enabled,
        Some(BucketVersioningStatus::SUSPENDED) => RemoteBucketVersioning::Suspended,
        Some(status) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("remote tier bucket returned unsupported versioning status {status}"),
            ));
        }
        None => RemoteBucketVersioning::Disabled,
    })
}

fn bounded_get_range(opts: &WarmBackendGetOpts) -> Result<Option<(i64, i64)>, std::io::Error> {
    if opts.start_offset < 0 || opts.length <= 0 {
        return Ok(None);
    }
    usize::try_from(opts.length)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid range: length does not fit in memory"))?;
    let end_offset = opts
        .start_offset
        .checked_add(opts.length - 1)
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid range: end offset overflow"))?;
    Ok(Some((opts.start_offset, end_offset)))
}

impl WarmBackendS3 {
    pub async fn new(conf: &TierS3, _tier: &str) -> Result<Self, std::io::Error> {
        Self::new_with_bucket_lookup(conf, BucketLookupType::BucketLookupAuto, "s3").await
    }

    pub(crate) async fn new_with_bucket_lookup(
        conf: &TierS3,
        bucket_lookup: BucketLookupType,
        tier_type: &str,
    ) -> Result<Self, std::io::Error> {
        let u = match Url::parse(&conf.endpoint) {
            Ok(u) => u,
            Err(err) => {
                return Err(std::io::Error::other(err.to_string()));
            }
        };
        validate_outbound_url(&u).map_err(|err| std::io::Error::other(format!("tier endpoint is not allowed: {err}")))?;

        let has_web_identity_token_file = !conf.aws_role_web_identity_token_file.is_empty();
        let has_role_arn = !conf.aws_role_arn.is_empty();
        let has_access_key = !conf.access_key.is_empty();
        let has_secret_key = !conf.secret_key.is_empty();

        if has_web_identity_token_file != has_role_arn {
            return Err(std::io::Error::other("both the token file and the role ARN are required"));
        } else if has_access_key != has_secret_key {
            return Err(std::io::Error::other("both the access and secret keys are required"));
        } else if conf.aws_role && (has_web_identity_token_file || has_role_arn || has_access_key || has_secret_key) {
            return Err(std::io::Error::other(
                "AWS Role cannot be activated with static credentials or the web identity token file",
            ));
        } else if conf.bucket.is_empty() {
            return Err(std::io::Error::other("no bucket name was provided"));
        }

        let creds = if has_access_key && has_secret_key {
            //creds = Credentials::new_static_v4(conf.access_key, conf.secret_key, "");
            Credentials::new(Static(Value {
                access_key_id: conf.access_key.clone(),
                secret_access_key: conf.secret_key.clone(),
                session_token: "".to_string(),
                signer_type: SignatureType::SignatureV4,
                ..Default::default()
            }))
        } else {
            return Err(std::io::Error::other("insufficient parameters for S3 backend authentication"));
        };
        let timeouts = transition_client_timeouts_from_env();
        let opts = Options {
            creds,
            secure: u.scheme() == "https",
            region: conf.region.clone(),
            bucket_lookup,
            ..Default::default()
        };
        let endpoint = endpoint_authority(&u)?;
        let client = TransitionClient::new_with_timeouts(&endpoint, opts, tier_type, timeouts).await?;

        let client = Arc::new(client);
        let core = TransitionCore(Arc::clone(&client));
        Ok(Self {
            client,
            core,
            bucket: conf.bucket.clone(),
            prefix: conf.prefix.clone().trim_matches('/').to_string(),
            storage_class: conf.storage_class.clone(),
        })
    }

    pub fn get_dest(&self, object: &str) -> String {
        if self.prefix.is_empty() {
            object.to_string()
        } else {
            format!("{}/{}", self.prefix, object)
        }
    }

    pub(crate) async fn remove_with_result(&self, object: &str, rv: &str) -> Result<RemoveObjectResult, std::io::Error> {
        let mut opts = RemoveObjectOptions::default();
        if !rv.is_empty() {
            opts.version_id = rv.to_string();
        }
        self.client
            .remove_object_inner(&self.bucket, &self.get_dest(object), opts)
            .await
    }

    pub(crate) async fn get_with_headers(
        &self,
        object: &str,
        rv: &str,
        opts: WarmBackendGetOpts,
    ) -> Result<(HeaderMap, ReadCloser), std::io::Error> {
        let mut gopts = GetObjectOptions::default();

        if !rv.is_empty() {
            gopts.version_id = rv.to_string();
        }
        if let Some((start_offset, end_offset)) = bounded_get_range(&opts)? {
            gopts.set_range(start_offset, end_offset)?;
        }
        let (_, headers, reader) = self.core.get_object(&self.bucket, &self.get_dest(object), &gopts).await?;
        Ok((headers, reader))
    }

    async fn remote_bucket_versioning(&self) -> Result<RemoteBucketVersioning, std::io::Error> {
        let config = self.client.get_bucket_versioning(&self.bucket).await?;
        remote_bucket_versioning_from_status(config.status.as_ref().map(|status| status.as_str()))
    }

    async fn probe_current_transition_candidate_with_header(
        &self,
        object: &str,
        raw_version_header: Option<&'static str>,
    ) -> Result<TransitionCandidateProbe, std::io::Error> {
        match self
            .get_with_headers(
                object,
                "",
                WarmBackendGetOpts {
                    start_offset: 0,
                    length: 1,
                },
            )
            .await
        {
            Ok((headers, _)) => {
                let version_id = match raw_version_header {
                    Some(header_name) => match headers.get(header_name) {
                        Some(value) => {
                            let version_id = value.to_str().map_err(|_| {
                                std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    "remote object version id is not valid ASCII",
                                )
                            })?;
                            validate_remote_version_id(version_id)?;
                            Some(version_id)
                        }
                        None => None,
                    },
                    None => self.client.raw_version_id(&headers)?,
                };
                Ok(match version_id {
                    Some(version_id) => TransitionCandidateProbe::VersionedPresent(version_id.to_string()),
                    None => TransitionCandidateProbe::UnversionedPresent,
                })
            }
            Err(err) => {
                let response = to_error_response(&err);
                if response.code == S3ErrorCode::NoSuchKey {
                    Ok(TransitionCandidateProbe::Missing)
                } else {
                    Err(err)
                }
            }
        }
    }

    pub(crate) async fn probe_transition_candidate_with_raw_version_header(
        &self,
        object: &str,
        raw_version_header: &'static str,
    ) -> Result<TransitionCandidateProbe, std::io::Error> {
        self.probe_current_transition_candidate_with_header(object, Some(raw_version_header))
            .await
    }

    async fn probe_transition_candidate_identity(
        &self,
        object: &str,
        identity: TransitionCandidateIdentity,
        bucket_versioning: RemoteBucketVersioning,
    ) -> Result<TransitionCandidateProbe, std::io::Error> {
        let remote_object = self.get_dest(object);
        let mut opts = ListObjectsOptions::default();
        opts.set("prefix", &remote_object);
        opts.set("max-keys", "1000");
        let mut key_marker = String::new();
        let mut version_id_marker = String::new();
        let mut matched_version = None;
        let mut saw_unproven_candidate = false;

        loop {
            let versions = self
                .client
                .list_object_versions_query(&self.bucket, &opts, &key_marker, &version_id_marker, "")
                .await?;
            for version in versions.versions.iter().filter(|version| version.key == remote_object) {
                let mut stat_opts = GetObjectOptions::default();
                stat_opts.version_id.clone_from(&version.version_id);
                let info = self.client.stat_object(&self.bucket, &remote_object, &stat_opts).await?;
                let mut metadata = info.user_metadata;
                for (name, value) in &info.metadata {
                    if (name
                        .as_str()
                        .starts_with(rustfs_utils::http::metadata_compat::RUSTFS_INTERNAL_PREFIX)
                        || name
                            .as_str()
                            .starts_with(rustfs_utils::http::metadata_compat::MINIO_INTERNAL_PREFIX))
                        && let Ok(value) = value.to_str()
                    {
                        metadata.insert(name.as_str().to_string(), value.to_string());
                    }
                }
                if transition_candidate_metadata_matches(&metadata, identity)? {
                    if matched_version.is_some() {
                        return Ok(TransitionCandidateProbe::Ambiguous);
                    }
                    matched_version = Some(version.version_id.clone());
                } else {
                    saw_unproven_candidate = true;
                }
            }
            if !versions.is_truncated {
                if matched_version.is_none() && saw_unproven_candidate {
                    return Ok(TransitionCandidateProbe::Unsupported);
                }
                let candidates = TransitionCandidateVersions {
                    version_id: matched_version,
                    ambiguous: false,
                };
                return classify_transition_candidates(candidates, bucket_versioning);
            }
            advance_version_markers(&mut key_marker, &mut version_id_marker, &versions)?;
        }
    }
}

fn transition_candidate_metadata_matches(
    metadata: &HashMap<String, String>,
    identity: TransitionCandidateIdentity,
) -> Result<bool, std::io::Error> {
    use rustfs_utils::http::metadata_compat::{
        SUFFIX_TRANSITION_TIER_DESTINATION_ID, SUFFIX_TRANSITION_TRANSACTION_ID, contains_key_str, get_consistent_str,
    };

    let transaction_id = get_consistent_str(metadata, SUFFIX_TRANSITION_TRANSACTION_ID);
    let destination_id = get_consistent_str(metadata, SUFFIX_TRANSITION_TIER_DESTINATION_ID);
    if transaction_id.is_none() || destination_id.is_none() {
        if contains_key_str(metadata, SUFFIX_TRANSITION_TRANSACTION_ID)
            || contains_key_str(metadata, SUFFIX_TRANSITION_TIER_DESTINATION_ID)
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "transition candidate identity metadata is empty or conflicting",
            ));
        }
        return Ok(false);
    }
    let expected_transaction_id = identity.transaction_id.to_string();
    let expected_destination_id = rustfs_utils::crypto::hex(identity.destination_id);
    Ok(transaction_id == Some(expected_transaction_id.as_str()) && destination_id == Some(expected_destination_id.as_str()))
}

fn classify_transition_candidates(
    candidates: TransitionCandidateVersions,
    bucket_versioning: RemoteBucketVersioning,
) -> Result<TransitionCandidateProbe, std::io::Error> {
    let probe = candidates.classify(bucket_versioning);
    if let TransitionCandidateProbe::VersionedPresent(version_id) = &probe {
        validate_remote_version_id(version_id)?;
    }
    Ok(probe)
}

fn advance_version_markers(
    key_marker: &mut String,
    version_id_marker: &mut String,
    versions: &ListVersionsResult,
) -> Result<(), std::io::Error> {
    let next_markers = (&versions.next_key_marker, &versions.next_version_id_marker);
    if next_markers == (&*key_marker, &*version_id_marker) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "ListObjectVersions pagination markers did not advance",
        ));
    }
    key_marker.clone_from(&versions.next_key_marker);
    version_id_marker.clone_from(&versions.next_version_id_marker);
    Ok(())
}

#[derive(Default)]
struct TransitionCandidateVersions {
    version_id: Option<String>,
    ambiguous: bool,
}

impl TransitionCandidateVersions {
    #[cfg(test)]
    fn extend(&mut self, remote_object: &str, versions: &ListVersionsResult) {
        for version in versions.versions.iter().filter(|version| version.key == remote_object) {
            if self.version_id.is_some() {
                self.ambiguous = true;
                return;
            }
            self.version_id = Some(version.version_id.clone());
        }
    }

    fn classify(self, bucket_versioning: RemoteBucketVersioning) -> TransitionCandidateProbe {
        if self.ambiguous {
            return TransitionCandidateProbe::Ambiguous;
        }
        let Some(version_id) = self.version_id else {
            return TransitionCandidateProbe::Missing;
        };

        match bucket_versioning {
            RemoteBucketVersioning::Disabled => TransitionCandidateProbe::UnversionedPresent,
            RemoteBucketVersioning::Suspended if version_id == "null" => TransitionCandidateProbe::VersionedPresent(version_id),
            RemoteBucketVersioning::Suspended | RemoteBucketVersioning::Enabled if !version_id.is_empty() => {
                TransitionCandidateProbe::VersionedPresent(version_id)
            }
            RemoteBucketVersioning::Suspended | RemoteBucketVersioning::Enabled => TransitionCandidateProbe::Ambiguous,
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::items_after_test_module,
    reason = "keep parsing tests adjacent to the helpers they cover"
)]
mod tests {
    use super::*;
    use rustfs_s3_client::api_s3_datatypes::{ListVersionsResult, Version};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn new_rejects_loopback_endpoint_before_network_setup() {
        let conf = TierS3 {
            endpoint: "https://127.0.0.1:9000".to_string(),
            bucket: "tier-bucket".to_string(),
            access_key: "access".to_string(),
            secret_key: "secret".to_string(),
            region: "us-east-1".to_string(),
            ..Default::default()
        };

        match WarmBackendS3::new(&conf, "tier").await {
            Ok(_) => panic!("loopback endpoint should be rejected"),
            Err(err) => assert!(err.to_string().contains("not allowed")),
        }
    }

    #[tokio::test]
    async fn new_preserves_an_explicit_endpoint_port() {
        let conf = TierS3 {
            endpoint: "https://tier.example.com:9443".to_string(),
            bucket: "tier-bucket".to_string(),
            access_key: "access".to_string(),
            secret_key: "secret".to_string(),
            region: "us-east-1".to_string(),
            ..Default::default()
        };

        let backend = WarmBackendS3::new(&conf, "tier")
            .await
            .expect("a well-formed S3 endpoint should initialize without network I/O");
        assert_eq!(backend.client.endpoint_url.host_str(), Some("tier.example.com"));
        assert_eq!(backend.client.endpoint_url.port(), Some(9443));
    }

    #[tokio::test]
    async fn overflowing_get_range_is_rejected_before_network_io() {
        let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return,
            Err(err) => panic!("test listener should bind: {err}"),
        };
        let endpoint = listener
            .local_addr()
            .expect("listener local address should be available")
            .to_string();
        let client = Arc::new(
            TransitionClient::new(
                &endpoint,
                Options {
                    creds: Credentials::new(Static(Value {
                        access_key_id: "access-key".to_string(),
                        secret_access_key: "secret-key".to_string(),
                        signer_type: SignatureType::SignatureV4,
                        ..Default::default()
                    })),
                    region: "us-east-1".to_string(),
                    bucket_lookup: BucketLookupType::BucketLookupPath,
                    max_retries: 1,
                    ..Default::default()
                },
                "s3",
            )
            .await
            .expect("fixture client should build"),
        );
        let backend = WarmBackendS3 {
            core: TransitionCore(Arc::clone(&client)),
            client,
            bucket: "bucket".to_string(),
            prefix: String::new(),
            storage_class: String::new(),
        };

        let err = backend
            .get_with_headers(
                "probe",
                "",
                WarmBackendGetOpts {
                    start_offset: i64::MAX,
                    length: 2,
                },
            )
            .await
            .expect_err("an overflowing range must fail before issuing a GET");

        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err()
        );
    }

    async fn candidate_probe_fixture() -> Option<(WarmBackendS3, tokio::task::JoinHandle<Vec<String>>)> {
        let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return None,
            Err(err) => panic!("test listener should bind: {err}"),
        };
        let endpoint = listener
            .local_addr()
            .expect("listener local address should be available")
            .to_string();
        let fixture = tokio::spawn(async move {
            let responses = [
                "HTTP/1.1 206 Partial Content\r\nContent-Length: 1\r\nx-amz-version-id: opaque-version\r\nConnection: close\r\n\r\nx",
                "HTTP/1.1 206 Partial Content\r\nContent-Length: 1\r\nConnection: close\r\n\r\nx",
                "HTTP/1.1 404 Not Found\r\nContent-Type: application/xml\r\nContent-Length: 63\r\nConnection: close\r\n\r\n<Error><Code>NoSuchKey</Code><Message>missing</Message></Error>",
                "HTTP/1.1 404 Not Found\r\nContent-Type: application/xml\r\nContent-Length: 66\r\nConnection: close\r\n\r\n<Error><Code>NoSuchObject</Code><Message>missing</Message></Error>",
                "HTTP/1.1 403 Forbidden\r\nContent-Type: application/xml\r\nContent-Length: 65\r\nConnection: close\r\n\r\n<Error><Code>AccessDenied</Code><Message>denied</Message></Error>",
                "HTTP/1.1 404 Not Found\r\nContent-Type: application/xml\r\nContent-Length: 63\r\nConnection: close\r\n\r\n<Error><Code>NoSuchKey</Code><Message>missing</Message></Error>",
                "HTTP/1.1 416 Range Not Satisfiable\r\nContent-Type: application/xml\r\nContent-Length: 72\r\nConnection: close\r\n\r\n<Error><Code>InvalidRange</Code><Message>empty version</Message></Error>",
                "HTTP/1.1 404 Not Found\r\nContent-Type: application/xml\r\nContent-Length: 67\r\nConnection: close\r\n\r\n<Error><Code>NoSuchVersion</Code><Message>missing</Message></Error>",
                "HTTP/1.1 404 Not Found\r\nContent-Type: application/xml\r\nContent-Length: 63\r\nConnection: close\r\n\r\n<Error><Code>NoSuchKey</Code><Message>missing</Message></Error>",
            ];
            let mut requests = Vec::new();
            for response in responses {
                let (mut stream, _) = listener.accept().await.expect("fixture should accept candidate GET");
                let mut request = Vec::new();
                let mut buffer = [0; 1024];
                loop {
                    let read = stream.read(&mut buffer).await.expect("fixture should read request headers");
                    assert_ne!(read, 0, "connection closed before request headers were received");
                    request.extend_from_slice(&buffer[..read]);
                    if request.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }
                requests.push(String::from_utf8_lossy(&request).into_owned());
                stream
                    .write_all(response.as_bytes())
                    .await
                    .expect("fixture should write candidate response");
            }
            requests
        });
        let client = Arc::new(
            TransitionClient::new(
                &endpoint,
                Options {
                    creds: Credentials::new(Static(Value {
                        access_key_id: "access-key".to_string(),
                        secret_access_key: "secret-key".to_string(),
                        signer_type: SignatureType::SignatureV4,
                        ..Default::default()
                    })),
                    region: "us-east-1".to_string(),
                    bucket_lookup: BucketLookupType::BucketLookupPath,
                    max_retries: 1,
                    ..Default::default()
                },
                "s3",
            )
            .await
            .expect("fixture client should build"),
        );
        Some((
            WarmBackendS3 {
                core: TransitionCore(Arc::clone(&client)),
                client,
                bucket: "bucket".to_string(),
                prefix: String::new(),
                storage_class: String::new(),
            },
            fixture,
        ))
    }

    #[tokio::test]
    async fn candidate_probe_uses_only_exact_bounded_get_permissions() {
        let Some((backend, fixture)) = candidate_probe_fixture().await else {
            return;
        };

        assert_eq!(
            backend
                .probe_transition_candidate("versioned-probe")
                .await
                .expect("versioned candidate should be discovered"),
            TransitionCandidateProbe::VersionedPresent("opaque-version".to_string())
        );
        assert_eq!(
            backend
                .probe_transition_candidate("unversioned-probe")
                .await
                .expect("unversioned candidate should be discovered"),
            TransitionCandidateProbe::UnversionedPresent
        );
        assert_eq!(
            backend
                .probe_transition_candidate("missing-probe")
                .await
                .expect("a missing key should be classified"),
            TransitionCandidateProbe::Missing
        );
        assert_eq!(
            backend
                .probe_transition_candidate("provider-missing-probe")
                .await
                .expect("a provider-specific missing code should be classified"),
            TransitionCandidateProbe::Missing
        );
        let err = backend
            .probe_transition_candidate("forbidden-probe")
            .await
            .expect_err("an authorization failure must not be mistaken for a missing key");
        assert_eq!(to_error_response(&err).code, S3ErrorCode::AccessDenied);
        assert_eq!(
            backend
                .probe_transition_candidate("delete-marker-hidden")
                .await
                .expect("a current delete marker should hide the data version"),
            TransitionCandidateProbe::Missing
        );
        assert_eq!(
            backend
                .probe_transition_version("delete-marker-hidden", "historical-version")
                .await
                .expect("the stored historical version should be probed exactly"),
            TransitionCandidateProbe::VersionedPresent("historical-version".to_string())
        );
        assert_eq!(
            backend
                .probe_transition_version("delete-marker-hidden", "missing-version")
                .await
                .expect("a missing exact version should be classified"),
            TransitionCandidateProbe::Missing
        );
        assert_eq!(
            backend
                .probe_transition_version("missing-object", "historical-version")
                .await
                .expect("a missing key for an exact version probe should be classified"),
            TransitionCandidateProbe::Missing
        );

        let requests = fixture.await.expect("candidate fixture should join");
        for request in &requests[..6] {
            let request = request.to_ascii_lowercase();
            assert!(request.starts_with("get /bucket/"), "candidate discovery must use object GET");
            assert!(request.contains("\r\nrange: bytes=0-0\r\n"));
            assert!(!request.contains("?versioning"));
            assert!(!request.contains("?versions"));
        }
        for request in &requests[6..] {
            let request = request.to_ascii_lowercase();
            assert!(request.starts_with("get /bucket/"), "exact discovery must use object GET");
            assert!(request.contains("\r\nrange: bytes=0-0\r\n"));
        }
        assert!(!requests[5].to_ascii_lowercase().contains("versionid="));
        assert!(requests[6].to_ascii_lowercase().contains("?versionid=historical-version"));
        assert!(requests[7].to_ascii_lowercase().contains("?versionid=missing-version"));
        assert!(requests[8].to_ascii_lowercase().contains("?versionid=historical-version"));
    }

    fn list_versions(versions: &[(&str, &str)], delete_markers: &[(&str, &str)], is_truncated: bool) -> ListVersionsResult {
        ListVersionsResult {
            versions: versions
                .iter()
                .map(|(key, version_id)| Version {
                    key: (*key).to_string(),
                    version_id: (*version_id).to_string(),
                    ..Default::default()
                })
                .collect(),
            delete_markers: delete_markers
                .iter()
                .map(|(key, version_id)| Version {
                    key: (*key).to_string(),
                    version_id: (*version_id).to_string(),
                    ..Default::default()
                })
                .collect(),
            is_truncated,
            ..Default::default()
        }
    }

    fn classify_pages(bucket_versioning: RemoteBucketVersioning, pages: &[ListVersionsResult]) -> TransitionCandidateProbe {
        let mut candidates = TransitionCandidateVersions::default();
        for page in pages {
            candidates.extend("archive/object", page);
        }
        candidates.classify(bucket_versioning)
    }

    fn candidate_identity() -> TransitionCandidateIdentity {
        TransitionCandidateIdentity {
            transaction_id: uuid::Uuid::parse_str("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa").unwrap(),
            destination_id: [0x5a; 32],
        }
    }

    fn candidate_metadata(identity: TransitionCandidateIdentity) -> HashMap<String, String> {
        let mut metadata = HashMap::new();
        rustfs_utils::http::metadata_compat::insert_str(
            &mut metadata,
            rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TRANSACTION_ID,
            identity.transaction_id.to_string(),
        );
        rustfs_utils::http::metadata_compat::insert_str(
            &mut metadata,
            rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TIER_DESTINATION_ID,
            rustfs_utils::crypto::hex(identity.destination_id),
        );
        metadata
    }

    #[test]
    fn transition_candidate_identity_requires_exact_compatible_metadata() {
        let identity = candidate_identity();
        let metadata = candidate_metadata(identity);
        assert!(transition_candidate_metadata_matches(&metadata, identity).unwrap());

        let mut adjacent = metadata;
        rustfs_utils::http::metadata_compat::insert_str(
            &mut adjacent,
            rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TRANSACTION_ID,
            uuid::Uuid::parse_str("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
                .unwrap()
                .to_string(),
        );
        assert!(!transition_candidate_metadata_matches(&adjacent, identity).unwrap());

        assert!(!transition_candidate_metadata_matches(&HashMap::new(), identity).unwrap());
    }

    #[test]
    fn transition_candidate_identity_rejects_conflicting_compatibility_keys() {
        let identity = candidate_identity();
        let mut metadata = candidate_metadata(identity);
        metadata.insert(
            rustfs_utils::http::metadata_compat::internal_key_rustfs(
                rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TRANSACTION_ID,
            ),
            uuid::Uuid::new_v4().to_string(),
        );
        assert!(transition_candidate_metadata_matches(&metadata, identity).is_err());
    }

    #[test]
    fn transition_candidate_probe_classifier_is_fail_closed() {
        assert_eq!(
            classify_pages(RemoteBucketVersioning::Disabled, &[list_versions(&[], &[], false)],),
            TransitionCandidateProbe::Missing
        );
        assert_eq!(
            classify_pages(RemoteBucketVersioning::Disabled, &[list_versions(&[("archive/object", "")], &[], false)],),
            TransitionCandidateProbe::UnversionedPresent
        );
        assert_eq!(
            classify_pages(
                RemoteBucketVersioning::Enabled,
                &[list_versions(&[("archive/object", "version-a")], &[], false)],
            ),
            TransitionCandidateProbe::VersionedPresent("version-a".to_string())
        );
        assert_eq!(
            classify_pages(
                RemoteBucketVersioning::Suspended,
                &[list_versions(&[("archive/object", "null")], &[], false)],
            ),
            TransitionCandidateProbe::VersionedPresent("null".to_string())
        );
        assert_eq!(
            classify_pages(RemoteBucketVersioning::Enabled, &[list_versions(&[("archive/object", "")], &[], false)],),
            TransitionCandidateProbe::Ambiguous
        );
        assert_eq!(
            classify_pages(
                RemoteBucketVersioning::Enabled,
                &[list_versions(
                    &[("archive/object", "version-a"), ("archive/object", "version-b")],
                    &[],
                    false,
                )],
            ),
            TransitionCandidateProbe::Ambiguous
        );
    }

    #[test]
    fn transition_candidate_probe_reconciles_all_pages_and_ignores_delete_markers() {
        assert_eq!(
            classify_pages(
                RemoteBucketVersioning::Enabled,
                &[
                    list_versions(&[], &[("archive/object", "marker-a")], true),
                    list_versions(&[("archive/object", "version-a"), ("archive/object-adjacent", "unrelated"),], &[], false,),
                ],
            ),
            TransitionCandidateProbe::VersionedPresent("version-a".to_string())
        );
        assert_eq!(
            classify_pages(
                RemoteBucketVersioning::Enabled,
                &[
                    list_versions(&[("archive/object", "version-a")], &[], true),
                    list_versions(&[("archive/object", "version-b")], &[], false),
                ],
            ),
            TransitionCandidateProbe::Ambiguous
        );
    }

    #[test]
    fn transition_candidate_pagination_advances_both_markers() {
        let mut key_marker = "old-key".to_string();
        let mut version_id_marker = "old-version".to_string();
        let page = ListVersionsResult {
            next_key_marker: "next-key".to_string(),
            next_version_id_marker: "next-version".to_string(),
            ..Default::default()
        };

        advance_version_markers(&mut key_marker, &mut version_id_marker, &page)
            .expect("new ListObjectVersions markers should advance pagination");
        assert_eq!(key_marker, "next-key");
        assert_eq!(version_id_marker, "next-version");

        let err = advance_version_markers(&mut key_marker, &mut version_id_marker, &page)
            .expect_err("repeated ListObjectVersions markers must fail closed");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn transition_candidate_probe_rejects_untrusted_version_ids() {
        let mut candidates = TransitionCandidateVersions::default();
        candidates.extend("archive/object", &list_versions(&[("archive/object", "version\ninjection")], &[], false));

        let err = classify_transition_candidates(candidates, RemoteBucketVersioning::Enabled)
            .expect_err("control characters in listed version IDs must fail closed");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn remote_bucket_versioning_status_parser_fails_closed() {
        assert_eq!(
            remote_bucket_versioning_from_status(None).expect("absent status means disabled"),
            RemoteBucketVersioning::Disabled
        );
        assert_eq!(
            remote_bucket_versioning_from_status(Some(BucketVersioningStatus::ENABLED)).expect("enabled status should parse"),
            RemoteBucketVersioning::Enabled
        );
        assert_eq!(
            remote_bucket_versioning_from_status(Some(BucketVersioningStatus::SUSPENDED)).expect("suspended status should parse"),
            RemoteBucketVersioning::Suspended
        );
        let err = remote_bucket_versioning_from_status(Some("UnexpectedStatus"))
            .expect_err("unknown versioning status must fail closed");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }
}

#[async_trait::async_trait]
impl WarmBackend for WarmBackendS3 {
    async fn put_with_meta(
        &self,
        object: &str,
        r: ReaderImpl,
        length: i64,
        meta: HashMap<String, String>,
    ) -> Result<String, std::io::Error> {
        let client = self.client.clone();
        let res = client
            .put_object(&self.bucket, &self.get_dest(object), r, length, &{
                let mut opts = build_transition_put_options(self.storage_class.clone(), meta);
                opts.send_content_md5 = true;
                opts
            })
            .await?;
        Ok(res.version_id)
    }

    async fn put(&self, object: &str, r: ReaderImpl, length: i64) -> Result<String, std::io::Error> {
        self.put_with_meta(object, r, length, HashMap::new()).await
    }

    async fn get(&self, object: &str, rv: &str, opts: WarmBackendGetOpts) -> Result<ReadCloser, std::io::Error> {
        self.get_with_headers(object, rv, opts).await.map(|(_, reader)| reader)
    }

    async fn remove(&self, object: &str, rv: &str) -> Result<(), std::io::Error> {
        self.remove_with_result(object, rv).await.map(|_| ())
    }

    async fn probe_transition_candidate(&self, object: &str) -> Result<TransitionCandidateProbe, std::io::Error> {
        self.probe_current_transition_candidate_with_header(object, None).await
    }

    async fn in_use(&self) -> Result<bool, std::io::Error> {
        let result = self
            .core
            .list_objects_v2(&self.bucket, &self.prefix, "", "", SLASH_SEPARATOR, 1)
            .await?;

        Ok(!result.common_prefixes.is_empty() || !result.contents.is_empty())
    }
}

#[async_trait::async_trait]
impl TransitionCandidateReconciler for WarmBackendS3 {
    async fn probe_transition_candidate_for(
        &self,
        object: &str,
        identity: TransitionCandidateIdentity,
    ) -> Result<TransitionCandidateProbe, std::io::Error> {
        let bucket_versioning = self.remote_bucket_versioning().await?;
        self.probe_transition_candidate_identity(object, identity, bucket_versioning)
            .await
    }
}
