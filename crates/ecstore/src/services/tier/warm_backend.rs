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

use crate::error::is_err_bucket_not_found;
#[cfg(feature = "gcs")]
use crate::services::tier::warm_backend_gcs::WarmBackendGCS;
use crate::services::tier::{
    tier::{ERR_TIER_BACKEND_IN_USE, ERR_TIER_INVALID_CONFIG, ERR_TIER_TYPE_UNSUPPORTED},
    tier_config::{TierConfig, TierType},
    tier_handlers::{ERR_TIER_BUCKET_NOT_FOUND, ERR_TIER_NOT_FOUND, ERR_TIER_PERM_ERR},
    warm_backend_aliyun::WarmBackendAliyun,
    warm_backend_azure::WarmBackendAzure,
    warm_backend_huaweicloud::WarmBackendHuaweicloud,
    warm_backend_minio::WarmBackendMinIO,
    warm_backend_r2::WarmBackendR2,
    warm_backend_rustfs::WarmBackendRustFS,
    warm_backend_s3::WarmBackendS3,
    warm_backend_tencent::WarmBackendTencent,
    warm_backend_wasabi::WarmBackendWasabi,
};
use bytes::Bytes;
use http::StatusCode;
use rustfs_s3_client::credentials::{Credentials, SignatureType, Static, Value};
use rustfs_s3_client::transition_api::{BucketLookupType, Options, TransitionClient, TransitionClientTimeouts, TransitionCore};
use rustfs_s3_client::{
    admin_handler_utils::AdminError,
    api_error_response::to_error_response,
    api_put_object::{AdvancedPutOptions, PutObjectOptions},
    transition_api::{ReadCloser, ReaderImpl},
};
use rustfs_scanner_metrics::metrics::{TierRequestOperation, TierRequestOutcome, global_metrics};
use rustfs_utils::egress::validate_outbound_url;
use rustfs_utils::http::headers::{
    CACHE_CONTROL, CONTENT_DISPOSITION, CONTENT_ENCODING, CONTENT_LANGUAGE, CONTENT_TYPE, EXPIRES, HeaderExt as _,
};
use s3s::header::{
    X_AMZ_OBJECT_LOCK_LEGAL_HOLD, X_AMZ_OBJECT_LOCK_MODE, X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE, X_AMZ_REPLICATION_STATUS,
    X_AMZ_STORAGE_CLASS,
};
use s3s::{
    S3ErrorCode,
    dto::{ObjectLockLegalHoldStatus, ObjectLockRetentionMode, ReplicationStatus},
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use time::OffsetDateTime;
use time::format_description::well_known::{Rfc2822, Rfc3339};
use tokio::io::AsyncReadExt;
use tracing::{info, warn};

pub type WarmBackendImpl = Box<dyn WarmBackend + Send + Sync + 'static>;

/// Largest object the S3-compatible warm backends accept for a multipart put.
pub(crate) const MAX_MULTIPART_PUT_OBJECT_SIZE: i64 = 1024 * 1024 * 1024 * 1024 * 5;
/// Part-count ceiling S3-compatible services impose on a multipart upload.
pub(crate) const MAX_PARTS_COUNT: i64 = 10000;
pub(crate) const WARM_BACKEND_PROBE_TIMEOUT: Duration = Duration::from_secs(30);
const WARM_BACKEND_PROBE_RECONCILE_INTERVAL: Duration = Duration::from_secs(1);
const WARM_BACKEND_PROBE_FINAL_RECONCILE_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Default)]
pub struct WarmBackendGetOpts {
    pub start_offset: i64,
    pub length: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransitionCandidateProbe {
    Missing,
    UnversionedPresent,
    VersionedPresent(String),
    Ambiguous,
    Unsupported,
}

#[derive(Clone, Copy)]
pub(crate) struct TransitionCandidateIdentity {
    pub transaction_id: uuid::Uuid,
    pub destination_id: [u8; 32],
}

#[async_trait::async_trait]
pub(crate) trait TransitionCandidateReconciler {
    async fn probe_transition_candidate_for(
        &self,
        object: &str,
        identity: TransitionCandidateIdentity,
    ) -> Result<TransitionCandidateProbe, std::io::Error>;
}

#[async_trait::async_trait]
pub trait WarmBackend {
    async fn validate(&self) -> Result<(), std::io::Error> {
        Ok(())
    }

    fn validate_remote_version_id(&self, _remote_version_id: &str) -> Result<(), std::io::Error> {
        Ok(())
    }

    /// Return `Ok` only after the backend has consumed the complete declared
    /// body and its storage service has acknowledged the PUT. The built-in S3
    /// family uses the transition client's declared-length request plus
    /// Content-MD5 for multipart parts, while GCS materializes the body before
    /// awaiting its buffered write response. Test backends may deliberately
    /// violate this contract to exercise transition compensation.
    async fn put(&self, object: &str, r: ReaderImpl, length: i64) -> Result<String, std::io::Error>;
    /// The same completion contract as [`WarmBackend::put`] applies when
    /// metadata is attached.
    async fn put_with_meta(
        &self,
        object: &str,
        r: ReaderImpl,
        length: i64,
        meta: HashMap<String, String>,
    ) -> Result<String, std::io::Error>;
    async fn get(&self, object: &str, rv: &str, opts: WarmBackendGetOpts) -> Result<ReadCloser, std::io::Error>;
    async fn remove(&self, object: &str, rv: &str) -> Result<(), std::io::Error>;
    async fn remove_exact(&self, object: &str, rv: &str) -> Result<(), std::io::Error> {
        if rv.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "an exact tier delete requires a remote version ID",
            ));
        }
        self.remove(object, rv).await
    }
    async fn probe_transition_candidate(&self, _object: &str) -> Result<TransitionCandidateProbe, std::io::Error> {
        Ok(TransitionCandidateProbe::Unsupported)
    }
    async fn probe_transition_version(
        &self,
        object: &str,
        remote_version_id: &str,
    ) -> Result<TransitionCandidateProbe, std::io::Error> {
        if remote_version_id.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "an exact tier probe requires a remote version ID",
            ));
        }
        self.validate_remote_version_id(remote_version_id)?;
        match self
            .get(
                object,
                remote_version_id,
                WarmBackendGetOpts {
                    start_offset: 0,
                    length: 1,
                },
            )
            .await
        {
            Ok(_) => Ok(TransitionCandidateProbe::VersionedPresent(remote_version_id.to_string())),
            Err(err) if matches!(to_error_response(&err).code, S3ErrorCode::InvalidRange) => {
                Ok(TransitionCandidateProbe::VersionedPresent(remote_version_id.to_string()))
            }
            Err(err)
                if err.kind() == std::io::ErrorKind::NotFound
                    || matches!(to_error_response(&err).code, S3ErrorCode::NoSuchKey | S3ErrorCode::NoSuchVersion) =>
            {
                Ok(TransitionCandidateProbe::Missing)
            }
            Err(err) => Err(err),
        }
    }
    async fn in_use(&self) -> Result<bool, std::io::Error>;
}

fn parse_http_timestamp(value: &str) -> Option<OffsetDateTime> {
    OffsetDateTime::parse(value, &Rfc3339)
        .or_else(|_| OffsetDateTime::parse(value, &Rfc2822))
        .ok()
}

pub fn build_transition_put_options(storage_class: String, mut metadata: HashMap<String, String>) -> PutObjectOptions {
    let mut opts = PutObjectOptions {
        storage_class,
        send_content_md5: true,
        legalhold: ObjectLockLegalHoldStatus::from_static(""),
        internal: AdvancedPutOptions {
            replication_status: ReplicationStatus::from_static(""),
            ..Default::default()
        },
        ..Default::default()
    };

    if let Some(content_type) = metadata.lookup(CONTENT_TYPE) {
        opts.content_type = content_type.to_string();
    }

    if let Some(content_encoding) = metadata.lookup(CONTENT_ENCODING) {
        opts.content_encoding = content_encoding.to_string();
    }

    if let Some(content_language) = metadata.lookup(CONTENT_LANGUAGE) {
        opts.content_language = content_language.to_string();
    }

    if let Some(content_disposition) = metadata.lookup(CONTENT_DISPOSITION) {
        opts.content_disposition = content_disposition.to_string();
    }

    if let Some(cache_control) = metadata.lookup(CACHE_CONTROL) {
        opts.cache_control = cache_control.to_string();
    }

    if let Some(expires) = metadata.lookup(EXPIRES).and_then(parse_http_timestamp) {
        opts.expires = expires;
    }

    if let Some(mode) = metadata.lookup(X_AMZ_OBJECT_LOCK_MODE.as_str()) {
        opts.mode = ObjectLockRetentionMode::from(mode.to_ascii_uppercase());
    }

    if let Some(retain_until_date) = metadata
        .lookup(X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.as_str())
        .and_then(parse_http_timestamp)
    {
        opts.retain_until_date = retain_until_date;
    }

    if let Some(legalhold) = metadata.lookup(X_AMZ_OBJECT_LOCK_LEGAL_HOLD.as_str()) {
        opts.legalhold = ObjectLockLegalHoldStatus::from(legalhold.to_ascii_uppercase());
    }

    for key in [
        CONTENT_TYPE,
        CONTENT_ENCODING,
        CONTENT_LANGUAGE,
        CONTENT_DISPOSITION,
        CACHE_CONTROL,
        EXPIRES,
        X_AMZ_OBJECT_LOCK_MODE.as_str(),
        X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.as_str(),
        X_AMZ_OBJECT_LOCK_LEGAL_HOLD.as_str(),
        X_AMZ_REPLICATION_STATUS.as_str(),
        X_AMZ_STORAGE_CLASS.as_str(),
    ] {
        metadata.remove(key);
    }

    for suffix in [
        rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TRANSACTION_ID,
        rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TIER_DESTINATION_ID,
    ] {
        for key in [
            rustfs_utils::http::metadata_compat::internal_key_rustfs(suffix),
            format!("{}{}", rustfs_utils::http::metadata_compat::MINIO_INTERNAL_PREFIX, suffix),
        ] {
            if let Some(value) = metadata.remove(&key) {
                metadata.insert(format!("x-amz-meta-{key}"), value);
            }
        }
    }

    opts.user_metadata = metadata;
    opts
}

/// Connection parameters every S3-compatible warm backend provider supplies.
///
/// The Aliyun, Azure, Huaweicloud, Tencent, MinIO, R2, and RustFS backends all
/// wrap [`WarmBackendS3`] around a statically-credentialed [`TransitionClient`]
/// built from exactly these values. `bucket_lookup` is a parameter rather than a
/// constant because the providers split into two families: Aliyun, Azure,
/// Huaweicloud, and Tencent pin [`BucketLookupType::BucketLookupDNS`], while
/// MinIO, R2, and RustFS leave it at [`BucketLookupType::BucketLookupAuto`].
pub(crate) struct S3CompatibleWarmBackendParams<'a> {
    pub endpoint: &'a str,
    pub access_key: &'a str,
    pub secret_key: &'a str,
    pub bucket: &'a str,
    pub prefix: &'a str,
    pub region: &'a str,
    pub bucket_lookup: BucketLookupType,
    /// Tag handed to [`TransitionClient::new`] so per-provider client behavior
    /// and metrics stay attributable.
    pub provider_tag: &'a str,
    /// SSRF guard run against the parsed endpoint once it's known to have a
    /// host. Almost every provider passes [`rustfs_utils::egress::validate_outbound_url`]
    /// unchanged; RustFS passes its own wrapper that adds a debug-only,
    /// env-gated loopback exception for its e2e tier tests (see
    /// rustfs/rustfs#6773) — the shared constructor stays the single call
    /// site either way, so no provider can silently end up unvalidated.
    pub validate_endpoint: fn(&url::Url) -> Result<(), rustfs_utils::egress::OutboundUrlError>,
}

/// Return the authority format accepted by `TransitionClient::new` while
/// retaining an explicitly configured port. `url::Url::host_str()` omits the
/// brackets needed when an IPv6 literal is combined with a port.
pub(crate) fn endpoint_authority(url: &url::Url) -> Result<String, std::io::Error> {
    let host = url
        .host_str()
        .ok_or_else(|| std::io::Error::other("Invalid endpoint URL: missing host"))?;
    let port = url.port().unwrap_or(if url.scheme() == "https" { 443 } else { 80 });
    if host.starts_with('[') && host.ends_with(']') {
        Ok(format!("{host}:{port}"))
    } else if host.contains(':') {
        Ok(format!("[{host}]:{port}"))
    } else {
        Ok(format!("{host}:{port}"))
    }
}

fn transition_timeout_from_env(env_key: &str, default_secs: u64) -> Duration {
    Duration::from_secs(rustfs_utils::get_env_u64(env_key, default_secs))
}

pub(crate) fn transition_client_timeouts_from_env() -> TransitionClientTimeouts {
    TransitionClientTimeouts::new(
        transition_timeout_from_env(
            rustfs_config::ENV_TIER_REMOTE_CONNECT_TIMEOUT_SECS,
            rustfs_config::DEFAULT_TIER_REMOTE_CONNECT_TIMEOUT_SECS,
        ),
        transition_timeout_from_env(
            rustfs_config::ENV_TIER_REMOTE_REQUEST_TIMEOUT_SECS,
            rustfs_config::DEFAULT_TIER_REMOTE_REQUEST_TIMEOUT_SECS,
        ),
        transition_timeout_from_env(
            rustfs_config::ENV_TIER_REMOTE_RESPONSE_BODY_IDLE_TIMEOUT_SECS,
            rustfs_config::DEFAULT_TIER_REMOTE_RESPONSE_BODY_IDLE_TIMEOUT_SECS,
        ),
    )
}

/// Build the [`WarmBackendS3`] shared by the S3-compatible warm backend providers.
///
/// Credential, bucket, and endpoint validation run in this order because the
/// existing provider constructors report the first failure they hit, and their
/// error texts are user-visible through the tier admin API.
pub(crate) async fn new_s3_compatible_warm_backend(
    params: S3CompatibleWarmBackendParams<'_>,
) -> Result<WarmBackendS3, std::io::Error> {
    if params.access_key.is_empty() || params.secret_key.is_empty() {
        return Err(std::io::Error::other("both access and secret keys are required"));
    }

    if params.bucket.is_empty() {
        return Err(std::io::Error::other("no bucket name was provided"));
    }

    let u = match url::Url::parse(params.endpoint) {
        Ok(u) => u,
        Err(e) => {
            return Err(std::io::Error::other(e.to_string()));
        }
    };

    let creds = Credentials::new(Static(Value {
        access_key_id: params.access_key.to_string(),
        secret_access_key: params.secret_key.to_string(),
        session_token: "".to_string(),
        signer_type: SignatureType::SignatureV4,
        ..Default::default()
    }));
    let timeouts = transition_client_timeouts_from_env();
    let opts = Options {
        creds,
        secure: u.scheme() == "https",
        trailing_headers: true,
        region: params.region.to_string(),
        bucket_lookup: params.bucket_lookup,
        ..Default::default()
    };
    let endpoint = endpoint_authority(&u)?;
    // Run the SSRF guard after the host-presence check so a host-less endpoint
    // keeps this constructor's stable error text.
    (params.validate_endpoint)(&u).map_err(|err| std::io::Error::other(format!("tier endpoint is not allowed: {err}")))?;
    let client = TransitionClient::new_with_timeouts(&endpoint, opts, params.provider_tag, timeouts).await?;

    let client = Arc::new(client);
    let core = TransitionCore(Arc::clone(&client));
    Ok(WarmBackendS3 {
        client,
        core,
        bucket: params.bucket.to_string(),
        prefix: params.prefix.strip_suffix("/").unwrap_or(params.prefix).to_owned(),
        storage_class: "".to_string(),
    })
}

/// Round the multipart part size up to a whole multiple of `min_part_size` that
/// keeps the upload within [`MAX_PARTS_COUNT`] parts.
///
/// `object_size == -1` means "length unknown", so the caller is charged the
/// worst case of a full [`MAX_MULTIPART_PUT_OBJECT_SIZE`] object.
pub(crate) fn optimal_part_size(object_size: i64, min_part_size: i64) -> Result<i64, std::io::Error> {
    let mut object_size = object_size;
    if object_size == -1 {
        object_size = MAX_MULTIPART_PUT_OBJECT_SIZE;
    }

    if object_size > MAX_MULTIPART_PUT_OBJECT_SIZE {
        return Err(std::io::Error::other("entity too large"));
    }

    let configured_part_size = min_part_size;
    let mut part_size_flt = object_size as f64 / MAX_PARTS_COUNT as f64;
    part_size_flt = (part_size_flt / configured_part_size as f64).ceil() * configured_part_size as f64;

    let part_size = part_size_flt as i64;
    if part_size == 0 {
        return Ok(min_part_size);
    }
    Ok(part_size)
}

/// Counts every remote-tier request exactly once, whatever backend performs it.
///
/// The counters are the only production update path for the `tier` request
/// metrics, and they must not grow with tier names, endpoints or object keys,
/// so the wrapper deliberately records nothing but the fixed operation and
/// outcome labels. Wrapping here rather than inside each provider keeps a new
/// backend counted by construction, and keeps one call per request: the
/// overridden `remove_exact` delegates to the inner backend, so the inner
/// default's own `remove` cannot count the same request a second time.
struct MeteredWarmBackend {
    inner: WarmBackendImpl,
}

impl MeteredWarmBackend {
    fn record<T>(operation: TierRequestOperation, result: Result<T, std::io::Error>) -> Result<T, std::io::Error> {
        let outcome = match &result {
            Ok(_) => TierRequestOutcome::Success,
            Err(err) => TierRequestOutcome::from_error(err),
        };
        global_metrics().record_tier_request(operation, outcome);
        result
    }
}

#[async_trait::async_trait]
impl WarmBackend for MeteredWarmBackend {
    /// Delegated without a counter: only one backend issues a remote request
    /// here, and every other one takes the trait default, so a `validate`
    /// counter would mostly record requests that never happened.
    async fn validate(&self) -> Result<(), std::io::Error> {
        self.inner.validate().await
    }

    /// A local check of a value this process already holds; it issues no
    /// remote request, so it is delegated without a counter.
    fn validate_remote_version_id(&self, remote_version_id: &str) -> Result<(), std::io::Error> {
        self.inner.validate_remote_version_id(remote_version_id)
    }

    async fn put(&self, object: &str, r: ReaderImpl, length: i64) -> Result<String, std::io::Error> {
        Self::record(TierRequestOperation::Put, self.inner.put(object, r, length).await)
    }

    async fn put_with_meta(
        &self,
        object: &str,
        r: ReaderImpl,
        length: i64,
        meta: HashMap<String, String>,
    ) -> Result<String, std::io::Error> {
        Self::record(TierRequestOperation::Put, self.inner.put_with_meta(object, r, length, meta).await)
    }

    async fn get(&self, object: &str, rv: &str, opts: WarmBackendGetOpts) -> Result<ReadCloser, std::io::Error> {
        Self::record(TierRequestOperation::Get, self.inner.get(object, rv, opts).await)
    }

    async fn remove(&self, object: &str, rv: &str) -> Result<(), std::io::Error> {
        Self::record(TierRequestOperation::Remove, self.inner.remove(object, rv).await)
    }

    async fn remove_exact(&self, object: &str, rv: &str) -> Result<(), std::io::Error> {
        Self::record(TierRequestOperation::Remove, self.inner.remove_exact(object, rv).await)
    }

    async fn probe_transition_candidate(&self, object: &str) -> Result<TransitionCandidateProbe, std::io::Error> {
        let result = self.inner.probe_transition_candidate(object).await;
        // `Unsupported` is the trait default: the backend issued no request,
        // so counting it would inflate the probe counter on every backend that
        // does not implement probing.
        if matches!(result, Ok(TransitionCandidateProbe::Unsupported)) {
            return result;
        }
        Self::record(TierRequestOperation::Probe, result)
    }

    async fn probe_transition_version(
        &self,
        object: &str,
        remote_version_id: &str,
    ) -> Result<TransitionCandidateProbe, std::io::Error> {
        Self::record(
            TierRequestOperation::Probe,
            self.inner.probe_transition_version(object, remote_version_id).await,
        )
    }

    async fn in_use(&self) -> Result<bool, std::io::Error> {
        Self::record(TierRequestOperation::InUse, self.inner.in_use().await)
    }
}

/// The reconciler is a second remote probe path, reached from recovery rather
/// than from the `WarmBackend` handle, so it needs its own counter: a `probe`
/// counter that saw only one of the two paths would read as a complete count
/// while missing every recovery probe.
struct MeteredTransitionCandidateReconciler {
    inner: Box<dyn TransitionCandidateReconciler + Send + Sync + 'static>,
}

#[async_trait::async_trait]
impl TransitionCandidateReconciler for MeteredTransitionCandidateReconciler {
    async fn probe_transition_candidate_for(
        &self,
        object: &str,
        identity: TransitionCandidateIdentity,
    ) -> Result<TransitionCandidateProbe, std::io::Error> {
        let result = self.inner.probe_transition_candidate_for(object, identity).await;
        if matches!(result, Ok(TransitionCandidateProbe::Unsupported)) {
            return result;
        }
        MeteredWarmBackend::record(TierRequestOperation::Probe, result)
    }
}

async fn remove_discovered_probe_candidate(
    w: &WarmBackendImpl,
    probe_object: &str,
    candidate: TransitionCandidateProbe,
) -> Result<bool, std::io::Error> {
    match candidate {
        TransitionCandidateProbe::Missing => Ok(false),
        TransitionCandidateProbe::VersionedPresent(remote_version_id) => {
            w.remove_exact(probe_object, &remote_version_id).await?;
            Ok(true)
        }
        TransitionCandidateProbe::UnversionedPresent => {
            w.remove(probe_object, "").await?;
            Ok(true)
        }
        TransitionCandidateProbe::Ambiguous => {
            Err(std::io::Error::other("remote tier probe PUT produced multiple possible versions"))
        }
        TransitionCandidateProbe::Unsupported => Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "remote tier cannot discover the outcome of a probe PUT",
        )),
    }
}

async fn compensate_uncertain_probe_put(
    w: &WarmBackendImpl,
    probe_object: &str,
    settle_deadline: tokio::time::Instant,
) -> Result<(), std::io::Error> {
    let final_deadline = settle_deadline + WARM_BACKEND_PROBE_FINAL_RECONCILE_TIMEOUT;
    let mut removed_any = false;
    while tokio::time::Instant::now() < settle_deadline {
        let candidate = match tokio::time::timeout_at(settle_deadline, w.probe_transition_candidate(probe_object)).await {
            Ok(candidate) => candidate?,
            Err(_) => break,
        };
        if matches!(candidate, TransitionCandidateProbe::Missing) && removed_any {
            break;
        }
        removed_any |= tokio::time::timeout_at(settle_deadline, remove_discovered_probe_candidate(w, probe_object, candidate))
            .await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "timed out reconciling a remote tier probe PUT"))??;

        let now = tokio::time::Instant::now();
        if now >= settle_deadline {
            break;
        }
        tokio::time::sleep_until(std::cmp::min(settle_deadline, now + WARM_BACKEND_PROBE_RECONCILE_INTERVAL)).await;
    }

    let candidate = tokio::time::timeout_at(final_deadline, w.probe_transition_candidate(probe_object))
        .await
        .map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::TimedOut, "timed out confirming the final remote tier probe state")
        })??;
    if !tokio::time::timeout_at(final_deadline, remove_discovered_probe_candidate(w, probe_object, candidate))
        .await
        .map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::TimedOut, "timed out removing the final remote tier probe candidate")
        })??
    {
        return Ok(());
    }

    let final_candidate = tokio::time::timeout_at(final_deadline, w.probe_transition_candidate(probe_object))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "timed out confirming remote tier probe cleanup"))??;
    match final_candidate {
        TransitionCandidateProbe::Missing => Ok(()),
        _ => Err(std::io::Error::other("remote tier probe cleanup could not be confirmed")),
    }
}

fn probe_cleanup_incomplete_error() -> AdminError {
    let mut err = ERR_TIER_PERM_ERR.clone();
    err.message = "Remote tier probe outcome is uncertain; cleanup is incomplete".to_string();
    err
}

async fn check_warm_backend_with_deadlines(
    w: Option<&WarmBackendImpl>,
    deadline: tokio::time::Instant,
    cleanup_deadline: tokio::time::Instant,
) -> Result<(), AdminError> {
    let w = w.ok_or_else(|| ERR_TIER_NOT_FOUND.clone())?;
    let probe_object = format!("rustfs-tier-probe-{}", uuid::Uuid::new_v4());
    let timeout_error = || {
        let mut err = ERR_TIER_BACKEND_IN_USE.clone();
        err.message = "Timed out validating the remote tier mutation".to_string();
        err
    };
    tokio::time::timeout_at(deadline, w.validate())
        .await
        .map_err(|_| timeout_error())?
        .map_err(|_| ERR_TIER_INVALID_CONFIG.clone())?;
    let put_result =
        tokio::time::timeout_at(deadline, w.put(&probe_object, ReaderImpl::Body(Bytes::from_static(b"RustFS")), 6)).await;
    let remote_version_id = match put_result {
        Ok(Ok(remote_version_id)) => remote_version_id,
        Ok(Err(_)) => {
            return Err(match compensate_uncertain_probe_put(w, &probe_object, cleanup_deadline).await {
                Ok(()) => ERR_TIER_PERM_ERR.clone(),
                Err(_) => probe_cleanup_incomplete_error(),
            });
        }
        Err(_) => {
            let err = timeout_error();
            return Err(match compensate_uncertain_probe_put(w, &probe_object, cleanup_deadline).await {
                Ok(()) => err,
                Err(_) => probe_cleanup_incomplete_error(),
            });
        }
    };

    // S3-family backends do not replay a failed request before returning `Ok`,
    // while GCS discovers every matching generation. The authoritative probe
    // below therefore closes the acknowledged-PUT path; only an error or
    // timeout needs the longer visibility reconciliation above.
    let authoritative_candidate = match tokio::time::timeout_at(deadline, w.probe_transition_candidate(&probe_object)).await {
        Ok(Ok(candidate)) => candidate,
        Ok(Err(_)) | Err(_) => {
            return Err(match compensate_uncertain_probe_put(w, &probe_object, cleanup_deadline).await {
                Ok(()) => ERR_TIER_INVALID_CONFIG.clone(),
                Err(_) => probe_cleanup_incomplete_error(),
            });
        }
    };
    let response_version_is_valid = w.validate_remote_version_id(&remote_version_id).is_ok();
    let response_matches_candidate = match &authoritative_candidate {
        TransitionCandidateProbe::UnversionedPresent => remote_version_id.is_empty(),
        TransitionCandidateProbe::VersionedPresent(candidate_version) => candidate_version == &remote_version_id,
        TransitionCandidateProbe::Missing | TransitionCandidateProbe::Ambiguous | TransitionCandidateProbe::Unsupported => false,
    };
    if !response_version_is_valid || !response_matches_candidate {
        return Err(match compensate_uncertain_probe_put(w, &probe_object, cleanup_deadline).await {
            Ok(()) => ERR_TIER_INVALID_CONFIG.clone(),
            Err(_) => probe_cleanup_incomplete_error(),
        });
    }

    let read_result = tokio::time::timeout_at(deadline, async {
        let mut reader = w
            .get(
                &probe_object,
                &remote_version_id,
                WarmBackendGetOpts {
                    start_offset: 0,
                    length: 7,
                },
            )
            .await
            .map_err(|_| ERR_TIER_PERM_ERR.clone())?;
        let mut body = Vec::new();
        reader
            .take(7)
            .read_to_end(&mut body)
            .await
            .map_err(|_| ERR_TIER_PERM_ERR.clone())?;
        if body != b"RustFS" {
            return Err(ERR_TIER_PERM_ERR.clone());
        }
        Ok(())
    })
    .await
    .map_err(|_| timeout_error())
    .and_then(|result| result);
    let cleanup_result = tokio::time::timeout_at(cleanup_deadline, async {
        if !remove_discovered_probe_candidate(w, &probe_object, authoritative_candidate).await? {
            return Err(std::io::Error::other("remote tier probe disappeared before cleanup"));
        }
        match w.probe_transition_candidate(&probe_object).await? {
            TransitionCandidateProbe::Missing => Ok(()),
            _ => Err(std::io::Error::other("remote tier probe remained after cleanup")),
        }
    })
    .await;
    if !matches!(cleanup_result, Ok(Ok(()))) {
        return Err(probe_cleanup_incomplete_error());
    }
    read_result?;
    Ok(())
}

/// Validate a backend using a caller-owned deadline while retaining a bounded
/// reconciliation window for an uncertain probe PUT. The validation future is
/// kept alive through cleanup so an outer timeout cannot abandon the remote
/// probe object.
pub(crate) async fn check_warm_backend_until(
    w: Option<&WarmBackendImpl>,
    deadline: tokio::time::Instant,
) -> Result<(), AdminError> {
    check_warm_backend_with_deadlines(w, deadline, deadline + WARM_BACKEND_PROBE_FINAL_RECONCILE_TIMEOUT).await
}

pub async fn check_warm_backend(w: Option<&WarmBackendImpl>) -> Result<(), AdminError> {
    let deadline = tokio::time::Instant::now() + WARM_BACKEND_PROBE_TIMEOUT;
    check_warm_backend_with_deadlines(w, deadline, deadline + WARM_BACKEND_PROBE_TIMEOUT).await
}

pub async fn new_warm_backend(tier: &TierConfig, probe: bool) -> Result<WarmBackendImpl, AdminError> {
    let mut d: Option<WarmBackendImpl> = None;
    match tier.tier_type {
        TierType::S3 => {
            if let Some(s3_config) = tier.s3.as_ref() {
                let dd = WarmBackendS3::new(s3_config, &tier.name).await;
                if let Err(err) = dd {
                    warn!("{}", err);
                    return Err(AdminError {
                        code: "XRustFSAdminTierInvalidConfig".to_string(),
                        message: format!("Unable to setup remote tier, check tier configuration: {err}"),
                        status_code: StatusCode::BAD_REQUEST,
                    });
                }
                d = Some(Box::new(dd.expect("Failed to create S3 backend")));
            } else {
                return Err(AdminError {
                    code: "XRustFSAdminTierInvalidConfig".to_string(),
                    message: "S3 tier configuration not found".to_string(),
                    status_code: StatusCode::BAD_REQUEST,
                });
            }
        }
        TierType::Wasabi => {
            if let Some(wasabi_config) = tier.wasabi.as_ref() {
                match WarmBackendWasabi::new(wasabi_config, &tier.name).await {
                    Ok(backend) => d = Some(Box::new(backend)),
                    Err(err) => {
                        warn!("{}", err);
                        return Err(AdminError {
                            code: "XRustFSAdminTierInvalidConfig".to_string(),
                            message: format!("Unable to setup remote tier, check tier configuration: {err}"),
                            status_code: StatusCode::BAD_REQUEST,
                        });
                    }
                }
            } else {
                return Err(AdminError {
                    code: "XRustFSAdminTierInvalidConfig".to_string(),
                    message: "Wasabi tier configuration not found".to_string(),
                    status_code: StatusCode::BAD_REQUEST,
                });
            }
        }
        TierType::RustFS => {
            if let Some(rustfs_config) = tier.rustfs.as_ref() {
                let dd = WarmBackendRustFS::new(rustfs_config, &tier.name).await;
                if let Err(err) = dd {
                    warn!("{}", err);
                    return Err(AdminError {
                        code: "XRustFSAdminTierInvalidConfig".to_string(),
                        message: format!("Unable to setup remote tier, check tier configuration: {err}"),
                        status_code: StatusCode::BAD_REQUEST,
                    });
                }
                d = Some(Box::new(dd.expect("Failed to create RustFS backend")));
            } else {
                return Err(AdminError {
                    code: "XRustFSAdminTierInvalidConfig".to_string(),
                    message: "RustFS tier configuration not found".to_string(),
                    status_code: StatusCode::BAD_REQUEST,
                });
            }
        }
        TierType::MinIO => {
            if let Some(minio_config) = tier.minio.as_ref() {
                let dd = WarmBackendMinIO::new(minio_config, &tier.name).await;
                if let Err(err) = dd {
                    warn!("{}", err);
                    return Err(AdminError {
                        code: "XRustFSAdminTierInvalidConfig".to_string(),
                        message: format!("Unable to setup remote tier, check tier configuration: {err}"),
                        status_code: StatusCode::BAD_REQUEST,
                    });
                }
                d = Some(Box::new(dd.expect("Failed to create MinIO backend")));
            } else {
                return Err(AdminError {
                    code: "XRustFSAdminTierInvalidConfig".to_string(),
                    message: "MinIO tier configuration not found".to_string(),
                    status_code: StatusCode::BAD_REQUEST,
                });
            }
        }
        TierType::Aliyun => {
            if let Some(aliyun_config) = tier.aliyun.as_ref() {
                let dd = WarmBackendAliyun::new(aliyun_config, &tier.name).await;
                if let Err(err) = dd {
                    warn!("{}", err);
                    return Err(AdminError {
                        code: "XRustFSAdminTierInvalidConfig".to_string(),
                        message: format!("Unable to setup remote tier, check tier configuration: {err}"),
                        status_code: StatusCode::BAD_REQUEST,
                    });
                }
                d = Some(Box::new(dd.expect("Failed to create Aliyun backend")));
            } else {
                return Err(AdminError {
                    code: "XRustFSAdminTierInvalidConfig".to_string(),
                    message: "Aliyun tier configuration not found".to_string(),
                    status_code: StatusCode::BAD_REQUEST,
                });
            }
        }
        TierType::Tencent => {
            if let Some(tencent_config) = tier.tencent.as_ref() {
                let dd = WarmBackendTencent::new(tencent_config, &tier.name).await;
                if let Err(err) = dd {
                    warn!("{}", err);
                    return Err(AdminError {
                        code: "XRustFSAdminTierInvalidConfig".to_string(),
                        message: format!("Unable to setup remote tier, check tier configuration: {err}"),
                        status_code: StatusCode::BAD_REQUEST,
                    });
                }
                d = Some(Box::new(dd.expect("Failed to create Tencent backend")));
            } else {
                return Err(AdminError {
                    code: "XRustFSAdminTierInvalidConfig".to_string(),
                    message: "Tencent tier configuration not found".to_string(),
                    status_code: StatusCode::BAD_REQUEST,
                });
            }
        }
        TierType::Huaweicloud => {
            if let Some(huaweicloud_config) = tier.huaweicloud.as_ref() {
                let dd = WarmBackendHuaweicloud::new(huaweicloud_config, &tier.name).await;
                if let Err(err) = dd {
                    warn!("{}", err);
                    return Err(AdminError {
                        code: "XRustFSAdminTierInvalidConfig".to_string(),
                        message: format!("Unable to setup remote tier, check tier configuration: {err}"),
                        status_code: StatusCode::BAD_REQUEST,
                    });
                }
                d = Some(Box::new(dd.expect("Failed to create Huaweicloud backend")));
            } else {
                return Err(AdminError {
                    code: "XRustFSAdminTierInvalidConfig".to_string(),
                    message: "Huaweicloud tier configuration not found".to_string(),
                    status_code: StatusCode::BAD_REQUEST,
                });
            }
        }
        TierType::Azure => {
            if let Some(azure_config) = tier.azure.as_ref() {
                let dd = WarmBackendAzure::new(azure_config, &tier.name).await;
                if let Err(err) = dd {
                    warn!("{}", err);
                    return Err(AdminError {
                        code: "XRustFSAdminTierInvalidConfig".to_string(),
                        message: format!("Unable to setup remote tier, check tier configuration: {err}"),
                        status_code: StatusCode::BAD_REQUEST,
                    });
                }
                d = Some(Box::new(dd.expect("Failed to create Azure backend")));
            } else {
                return Err(AdminError {
                    code: "XRustFSAdminTierInvalidConfig".to_string(),
                    message: "Azure tier configuration not found".to_string(),
                    status_code: StatusCode::BAD_REQUEST,
                });
            }
        }
        #[cfg(not(feature = "gcs"))]
        TierType::GCS => {
            return Err(AdminError {
                code: ERR_TIER_TYPE_UNSUPPORTED.code.clone(),
                message: "This build does not include the GCS backend; rebuild with the gcs feature".to_string(),
                status_code: StatusCode::NOT_IMPLEMENTED,
            });
        }
        #[cfg(feature = "gcs")]
        TierType::GCS => {
            if let Some(gcs_config) = tier.gcs.as_ref() {
                let dd = WarmBackendGCS::new(gcs_config, &tier.name).await;
                if let Err(err) = dd {
                    warn!("{}", err);
                    return Err(AdminError {
                        code: "XRustFSAdminTierInvalidConfig".to_string(),
                        message: format!("Unable to setup remote tier, check tier configuration: {err}"),
                        status_code: StatusCode::BAD_REQUEST,
                    });
                }
                d = Some(Box::new(dd.expect("Failed to create GCS backend")));
            } else {
                return Err(AdminError {
                    code: "XRustFSAdminTierInvalidConfig".to_string(),
                    message: "GCS tier configuration not found".to_string(),
                    status_code: StatusCode::BAD_REQUEST,
                });
            }
        }
        TierType::R2 => {
            if let Some(r2_config) = tier.r2.as_ref() {
                let dd = WarmBackendR2::new(r2_config, &tier.name).await;
                if let Err(err) = dd {
                    warn!("{}", err);
                    return Err(AdminError {
                        code: "XRustFSAdminTierInvalidConfig".to_string(),
                        message: format!("Unable to setup remote tier, check tier configuration: {err}"),
                        status_code: StatusCode::BAD_REQUEST,
                    });
                }
                d = Some(Box::new(dd.expect("Failed to create R2 backend")));
            } else {
                return Err(AdminError {
                    code: "XRustFSAdminTierInvalidConfig".to_string(),
                    message: "R2 tier configuration not found".to_string(),
                    status_code: StatusCode::BAD_REQUEST,
                });
            }
        }
        _ => {
            return Err(ERR_TIER_TYPE_UNSUPPORTED.clone());
        }
    }

    let d = d.ok_or_else(|| AdminError {
        code: "XRustFSAdminTierInvalidConfig".to_string(),
        message: "Tier backend not initialized".to_string(),
        status_code: StatusCode::BAD_REQUEST,
    })?;

    let d: WarmBackendImpl = Box::new(MeteredWarmBackend { inner: d });

    if probe {
        check_warm_backend(Some(&d)).await?;
    }
    Ok(d)
}

pub(crate) async fn new_transition_candidate_reconciler(
    tier: &TierConfig,
) -> Result<Option<Box<dyn TransitionCandidateReconciler + Send + Sync + 'static>>, AdminError> {
    let reconciler: Box<dyn TransitionCandidateReconciler + Send + Sync + 'static> = match tier.tier_type {
        TierType::S3 => Box::new(
            WarmBackendS3::new(tier.s3.as_ref().ok_or_else(|| ERR_TIER_INVALID_CONFIG.clone())?, &tier.name)
                .await
                .map_err(|err| {
                    let mut admin_err = ERR_TIER_INVALID_CONFIG.clone();
                    admin_err.message = err.to_string();
                    admin_err
                })?,
        ),
        TierType::MinIO => Box::new(
            WarmBackendMinIO::new(tier.minio.as_ref().ok_or_else(|| ERR_TIER_INVALID_CONFIG.clone())?, &tier.name)
                .await
                .map_err(|err| {
                    let mut admin_err = ERR_TIER_INVALID_CONFIG.clone();
                    admin_err.message = err.to_string();
                    admin_err
                })?,
        ),
        TierType::RustFS => Box::new(
            WarmBackendRustFS::new(tier.rustfs.as_ref().ok_or_else(|| ERR_TIER_INVALID_CONFIG.clone())?, &tier.name)
                .await
                .map_err(|err| {
                    let mut admin_err = ERR_TIER_INVALID_CONFIG.clone();
                    admin_err.message = err.to_string();
                    admin_err
                })?,
        ),
        TierType::R2 => Box::new(
            WarmBackendR2::new(tier.r2.as_ref().ok_or_else(|| ERR_TIER_INVALID_CONFIG.clone())?, &tier.name)
                .await
                .map_err(|err| {
                    let mut admin_err = ERR_TIER_INVALID_CONFIG.clone();
                    admin_err.message = err.to_string();
                    admin_err
                })?,
        ),
        _ => return Ok(None),
    };
    Ok(Some(Box::new(MeteredTransitionCandidateReconciler { inner: reconciler })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::tier::test_util::{MockWarmBackend, MockWarmOp};
    use crate::services::tier::tier_config::TierWasabi;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    const PROBE_VERSION: &str = "remote-v2";

    #[cfg(not(feature = "gcs"))]
    #[tokio::test]
    async fn gcs_backend_not_compiled_preserves_config() {
        let json = r#"{"name":"ARCHIVE","type":"gcs","gcs":{"bucket":"archive","creds":"secret"}}"#;
        let tier: TierConfig = serde_json::from_str(json).expect("GCS config remains readable without the backend");
        assert_eq!(tier.tier_type, TierType::GCS);
        let encoded = serde_json::to_vec(&tier).expect("GCS config remains writable");
        let restored: TierConfig = serde_json::from_slice(&encoded).expect("GCS config round trips");
        assert_eq!(restored.tier_type, TierType::GCS);
        let restored_gcs = restored.gcs.as_ref().expect("GCS settings preserved");
        assert_eq!(restored_gcs.bucket, "archive");
        assert_eq!(restored_gcs.creds, "secret");
        assert_eq!(tier.redacted().gcs.expect("redacted GCS settings").creds, "REDACTED");
        let error = match new_warm_backend(&tier, false).await {
            Ok(_) => panic!("an excluded GCS backend cannot be constructed"),
            Err(error) => error,
        };
        assert_eq!(error.code, ERR_TIER_TYPE_UNSUPPORTED.code);
        assert_eq!(error.status_code, StatusCode::NOT_IMPLEMENTED);
    }

    struct CountingBackend {
        put_result: fn() -> Result<String, std::io::Error>,
        removes: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl WarmBackend for CountingBackend {
        async fn put(&self, _object: &str, _r: ReaderImpl, _length: i64) -> Result<String, std::io::Error> {
            (self.put_result)()
        }

        async fn put_with_meta(
            &self,
            object: &str,
            r: ReaderImpl,
            length: i64,
            _meta: HashMap<String, String>,
        ) -> Result<String, std::io::Error> {
            self.put(object, r, length).await
        }

        async fn get(&self, _object: &str, _rv: &str, _opts: WarmBackendGetOpts) -> Result<ReadCloser, std::io::Error> {
            Err(std::io::Error::other("unused"))
        }

        async fn remove(&self, _object: &str, _rv: &str) -> Result<(), std::io::Error> {
            self.removes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn in_use(&self) -> Result<bool, std::io::Error> {
            Ok(false)
        }
    }

    fn tier_cell(operation: TierRequestOperation, outcome: TierRequestOutcome) -> u64 {
        global_metrics()
            .tier_request_counts()
            .into_iter()
            .find(|count| count.operation == operation && count.outcome == outcome)
            .map(|count| count.count)
            .expect("every operation/outcome cell must be reported")
    }

    #[tokio::test]
    async fn a_metered_put_counts_its_outcome_once() {
        let before_success = tier_cell(TierRequestOperation::Put, TierRequestOutcome::Success);
        let before_failure = tier_cell(TierRequestOperation::Put, TierRequestOutcome::BackendError);
        let backend = MeteredWarmBackend {
            inner: Box::new(CountingBackend {
                put_result: || Ok("remote-v1".to_string()),
                removes: Arc::new(AtomicUsize::new(0)),
            }),
        };

        backend
            .put("object", ReaderImpl::Body(Bytes::from_static(b"payload")), 7)
            .await
            .expect("the fake backend accepts the put");

        assert_eq!(
            tier_cell(TierRequestOperation::Put, TierRequestOutcome::Success),
            before_success + 1,
            "an acknowledged put must be counted once"
        );
        assert_eq!(
            tier_cell(TierRequestOperation::Put, TierRequestOutcome::BackendError),
            before_failure,
            "a success must not also increment a failure cell"
        );
    }

    #[tokio::test]
    async fn a_metered_put_failure_is_classified_by_error_kind() {
        let before_timeout = tier_cell(TierRequestOperation::Put, TierRequestOutcome::Timeout);
        let backend = MeteredWarmBackend {
            inner: Box::new(CountingBackend {
                put_result: || Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "endpoint stalled")),
                removes: Arc::new(AtomicUsize::new(0)),
            }),
        };

        backend
            .put("object", ReaderImpl::Body(Bytes::from_static(b"payload")), 7)
            .await
            .expect_err("the fake backend rejects the put");

        assert_eq!(
            tier_cell(TierRequestOperation::Put, TierRequestOutcome::Timeout),
            before_timeout + 1,
            "a timed-out request must land in the timeout cell"
        );
    }

    fn tier_probe_total() -> u64 {
        global_metrics()
            .tier_request_counts()
            .into_iter()
            .filter(|count| count.operation == TierRequestOperation::Probe)
            .map(|count| count.count)
            .sum()
    }

    #[tokio::test]
    async fn an_unsupported_probe_is_not_counted_as_a_request() {
        let before = tier_probe_total();
        let backend = MeteredWarmBackend {
            inner: Box::new(CountingBackend {
                put_result: || Ok(String::new()),
                removes: Arc::new(AtomicUsize::new(0)),
            }),
        };

        let probe = backend
            .probe_transition_candidate("object")
            .await
            .expect("the trait default answers without a remote request");

        assert_eq!(probe, TransitionCandidateProbe::Unsupported);
        assert_eq!(
            tier_probe_total(),
            before,
            "a backend that issues no probe request must not appear in the probe counters"
        );
    }

    #[tokio::test]
    async fn an_exact_remove_is_counted_once_not_twice() {
        let before = tier_cell(TierRequestOperation::Remove, TierRequestOutcome::Success);
        let removes = Arc::new(AtomicUsize::new(0));
        let backend = MeteredWarmBackend {
            inner: Box::new(CountingBackend {
                put_result: || Ok(String::new()),
                removes: Arc::clone(&removes),
            }),
        };

        backend
            .remove_exact("object", "remote-v1")
            .await
            .expect("the fake backend accepts the remove");

        assert_eq!(removes.load(Ordering::SeqCst), 1, "the inner backend performs one request");
        assert_eq!(
            tier_cell(TierRequestOperation::Remove, TierRequestOutcome::Success),
            before + 1,
            "the default remove_exact must not count its own inner remove a second time"
        );
    }

    struct RejectingValidationBackend {
        validations: Arc<AtomicUsize>,
        puts: Arc<AtomicUsize>,
        removes: Arc<AtomicUsize>,
    }

    struct RejectingProbeVersionBackend {
        gets: Arc<AtomicUsize>,
        present: Arc<std::sync::atomic::AtomicBool>,
        removed_versions: Arc<tokio::sync::Mutex<Vec<String>>>,
        returned_version: String,
    }

    struct RecordingProbeBackend {
        get_versions: Arc<tokio::sync::Mutex<Vec<String>>>,
        present: Arc<std::sync::atomic::AtomicBool>,
        removed_versions: Arc<tokio::sync::Mutex<Vec<String>>>,
        remove_clears_candidate: bool,
        fail_get: bool,
        body: ProbeBody,
    }

    struct HangingProbePutBackend {
        put_started: Arc<tokio::sync::Notify>,
        present: Arc<std::sync::atomic::AtomicBool>,
        probes: Arc<AtomicUsize>,
        removed_versions: Arc<tokio::sync::Mutex<Vec<String>>>,
    }

    struct LateVisibleProbeBackend {
        visible_at: tokio::time::Instant,
        removed: Arc<std::sync::atomic::AtomicBool>,
        probes: Arc<AtomicUsize>,
        removed_versions: Arc<tokio::sync::Mutex<Vec<String>>>,
    }

    #[derive(Clone, Copy)]
    enum ProbeBody {
        Exact,
        Mismatch,
    }

    #[async_trait::async_trait]
    impl WarmBackend for RejectingValidationBackend {
        async fn validate(&self) -> Result<(), std::io::Error> {
            self.validations.fetch_add(1, Ordering::SeqCst);
            Err(std::io::Error::other("invalid backend configuration"))
        }

        async fn put(&self, _object: &str, _r: ReaderImpl, _length: i64) -> Result<String, std::io::Error> {
            self.puts.fetch_add(1, Ordering::SeqCst);
            Ok(String::new())
        }

        async fn put_with_meta(
            &self,
            object: &str,
            r: ReaderImpl,
            length: i64,
            _meta: HashMap<String, String>,
        ) -> Result<String, std::io::Error> {
            self.put(object, r, length).await
        }

        async fn get(&self, _object: &str, _rv: &str, _opts: WarmBackendGetOpts) -> Result<ReadCloser, std::io::Error> {
            Err(std::io::Error::other("get must not run after validation failure"))
        }

        async fn remove(&self, _object: &str, _rv: &str) -> Result<(), std::io::Error> {
            self.removes.fetch_add(1, Ordering::SeqCst);
            Err(std::io::Error::other("remove must not run after validation failure"))
        }

        async fn in_use(&self) -> Result<bool, std::io::Error> {
            Err(std::io::Error::other("in_use must not run after validation failure"))
        }
    }

    #[async_trait::async_trait]
    impl WarmBackend for RejectingProbeVersionBackend {
        fn validate_remote_version_id(&self, remote_version_id: &str) -> Result<(), std::io::Error> {
            if remote_version_id.is_empty() {
                Ok(())
            } else {
                Err(std::io::Error::other("probe returned a version ID"))
            }
        }

        async fn put(&self, _object: &str, _r: ReaderImpl, _length: i64) -> Result<String, std::io::Error> {
            Ok(self.returned_version.clone())
        }

        async fn put_with_meta(
            &self,
            object: &str,
            r: ReaderImpl,
            length: i64,
            _meta: HashMap<String, String>,
        ) -> Result<String, std::io::Error> {
            self.put(object, r, length).await
        }

        async fn get(&self, _object: &str, _rv: &str, _opts: WarmBackendGetOpts) -> Result<ReadCloser, std::io::Error> {
            self.gets.fetch_add(1, Ordering::SeqCst);
            Err(std::io::Error::other("GET must not run for a rejected probe version"))
        }

        async fn remove(&self, _object: &str, _rv: &str) -> Result<(), std::io::Error> {
            Err(std::io::Error::other("generic remove must not run for a rejected fresh PUT response"))
        }

        async fn remove_exact(&self, _object: &str, rv: &str) -> Result<(), std::io::Error> {
            self.present.store(false, Ordering::SeqCst);
            self.removed_versions.lock().await.push(rv.to_string());
            Ok(())
        }

        async fn probe_transition_candidate(&self, _object: &str) -> Result<TransitionCandidateProbe, std::io::Error> {
            if self.present.load(Ordering::SeqCst) {
                Ok(TransitionCandidateProbe::VersionedPresent(PROBE_VERSION.to_string()))
            } else {
                Ok(TransitionCandidateProbe::Missing)
            }
        }

        async fn in_use(&self) -> Result<bool, std::io::Error> {
            Ok(false)
        }
    }

    #[async_trait::async_trait]
    impl WarmBackend for RecordingProbeBackend {
        async fn put(&self, _object: &str, _r: ReaderImpl, _length: i64) -> Result<String, std::io::Error> {
            Ok(PROBE_VERSION.to_string())
        }

        async fn put_with_meta(
            &self,
            object: &str,
            r: ReaderImpl,
            length: i64,
            _meta: HashMap<String, String>,
        ) -> Result<String, std::io::Error> {
            self.put(object, r, length).await
        }

        async fn get(&self, _object: &str, rv: &str, _opts: WarmBackendGetOpts) -> Result<ReadCloser, std::io::Error> {
            self.get_versions.lock().await.push(rv.to_string());
            if self.fail_get {
                Err(std::io::Error::other("probe GET failed"))
            } else {
                match self.body {
                    ProbeBody::Exact => Ok(ReadCloser::new(std::io::Cursor::new(b"RustFS".to_vec()))),
                    ProbeBody::Mismatch => Ok(ReadCloser::new(std::io::Cursor::new(b"RustFT".to_vec()))),
                }
            }
        }

        async fn remove(&self, _object: &str, rv: &str) -> Result<(), std::io::Error> {
            if self.remove_clears_candidate {
                self.present.store(false, Ordering::SeqCst);
            }
            self.removed_versions.lock().await.push(rv.to_string());
            Ok(())
        }

        async fn probe_transition_candidate(&self, _object: &str) -> Result<TransitionCandidateProbe, std::io::Error> {
            if self.present.load(Ordering::SeqCst) {
                Ok(TransitionCandidateProbe::VersionedPresent(PROBE_VERSION.to_string()))
            } else {
                Ok(TransitionCandidateProbe::Missing)
            }
        }

        async fn in_use(&self) -> Result<bool, std::io::Error> {
            Ok(false)
        }
    }

    #[async_trait::async_trait]
    impl WarmBackend for HangingProbePutBackend {
        async fn put(&self, _object: &str, _r: ReaderImpl, _length: i64) -> Result<String, std::io::Error> {
            self.put_started.notify_one();
            std::future::pending().await
        }

        async fn put_with_meta(
            &self,
            object: &str,
            r: ReaderImpl,
            length: i64,
            _meta: HashMap<String, String>,
        ) -> Result<String, std::io::Error> {
            self.put(object, r, length).await
        }

        async fn get(&self, _object: &str, _rv: &str, _opts: WarmBackendGetOpts) -> Result<ReadCloser, std::io::Error> {
            Err(std::io::Error::other("GET must not run after a timed out probe PUT"))
        }

        async fn remove(&self, _object: &str, _rv: &str) -> Result<(), std::io::Error> {
            Err(std::io::Error::other("generic remove must not replace exact probe cleanup"))
        }

        async fn remove_exact(&self, _object: &str, rv: &str) -> Result<(), std::io::Error> {
            self.present.store(false, Ordering::SeqCst);
            self.removed_versions.lock().await.push(rv.to_string());
            Ok(())
        }

        async fn probe_transition_candidate(&self, _object: &str) -> Result<TransitionCandidateProbe, std::io::Error> {
            self.probes.fetch_add(1, Ordering::SeqCst);
            if self.present.load(Ordering::SeqCst) {
                Ok(TransitionCandidateProbe::VersionedPresent(PROBE_VERSION.to_string()))
            } else {
                Ok(TransitionCandidateProbe::Missing)
            }
        }

        async fn in_use(&self) -> Result<bool, std::io::Error> {
            Ok(false)
        }
    }

    #[async_trait::async_trait]
    impl WarmBackend for LateVisibleProbeBackend {
        async fn put(&self, _object: &str, _r: ReaderImpl, _length: i64) -> Result<String, std::io::Error> {
            Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "probe PUT response was lost before the object became visible",
            ))
        }

        async fn put_with_meta(
            &self,
            object: &str,
            r: ReaderImpl,
            length: i64,
            _meta: HashMap<String, String>,
        ) -> Result<String, std::io::Error> {
            self.put(object, r, length).await
        }

        async fn get(&self, _object: &str, _rv: &str, _opts: WarmBackendGetOpts) -> Result<ReadCloser, std::io::Error> {
            Err(std::io::Error::other("GET must not run after a lost probe PUT response"))
        }

        async fn remove(&self, _object: &str, _rv: &str) -> Result<(), std::io::Error> {
            Err(std::io::Error::other("generic remove must not replace exact probe cleanup"))
        }

        async fn remove_exact(&self, _object: &str, rv: &str) -> Result<(), std::io::Error> {
            self.removed.store(true, Ordering::SeqCst);
            self.removed_versions.lock().await.push(rv.to_string());
            Ok(())
        }

        async fn probe_transition_candidate(&self, _object: &str) -> Result<TransitionCandidateProbe, std::io::Error> {
            self.probes.fetch_add(1, Ordering::SeqCst);
            if tokio::time::Instant::now() >= self.visible_at && !self.removed.load(Ordering::SeqCst) {
                Ok(TransitionCandidateProbe::VersionedPresent(PROBE_VERSION.to_string()))
            } else {
                Ok(TransitionCandidateProbe::Missing)
            }
        }

        async fn in_use(&self) -> Result<bool, std::io::Error> {
            Ok(false)
        }
    }

    #[tokio::test]
    async fn check_warm_backend_validates_before_probe_io() {
        let validations = Arc::new(AtomicUsize::new(0));
        let puts = Arc::new(AtomicUsize::new(0));
        let removes = Arc::new(AtomicUsize::new(0));
        let backend: WarmBackendImpl = Box::new(RejectingValidationBackend {
            validations: validations.clone(),
            puts: puts.clone(),
            removes: removes.clone(),
        });

        let err = check_warm_backend(Some(&backend))
            .await
            .expect_err("invalid backend configuration should fail before probe I/O");

        assert_eq!(err.code, ERR_TIER_INVALID_CONFIG.code);
        assert_eq!(validations.load(Ordering::SeqCst), 1);
        assert_eq!(puts.load(Ordering::SeqCst), 0);
        assert_eq!(removes.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn default_exact_remove_rejects_an_empty_version() {
        let removes = Arc::new(AtomicUsize::new(0));
        let backend = RejectingValidationBackend {
            validations: Arc::new(AtomicUsize::new(0)),
            puts: Arc::new(AtomicUsize::new(0)),
            removes: removes.clone(),
        };

        let err = backend
            .remove_exact("remote-object", "")
            .await
            .expect_err("an empty exact constraint must fail closed");

        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(removes.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn default_transition_candidate_probe_is_unsupported() {
        let backend = RejectingValidationBackend {
            validations: Arc::new(AtomicUsize::new(0)),
            puts: Arc::new(AtomicUsize::new(0)),
            removes: Arc::new(AtomicUsize::new(0)),
        };

        let probe = backend
            .probe_transition_candidate("remote-object")
            .await
            .expect("default candidate probe should be a safe capability response");

        assert_eq!(probe, TransitionCandidateProbe::Unsupported);
    }

    #[tokio::test(start_paused = true)]
    async fn check_warm_backend_removes_exact_probe_when_versioning_drifts() {
        let gets = Arc::new(AtomicUsize::new(0));
        let removed_versions = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let backend: WarmBackendImpl = Box::new(RejectingProbeVersionBackend {
            gets: gets.clone(),
            present: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            removed_versions: removed_versions.clone(),
            returned_version: uuid::Uuid::nil().to_string(),
        });

        let err = check_warm_backend(Some(&backend))
            .await
            .expect_err("a probe version ID must fail an unversioned backend check");

        assert_eq!(err.code, ERR_TIER_INVALID_CONFIG.code);
        assert_eq!(gets.load(Ordering::SeqCst), 0);
        assert_eq!(removed_versions.lock().await.as_slice(), [PROBE_VERSION]);
    }

    #[tokio::test(start_paused = true)]
    async fn check_warm_backend_rejects_empty_put_version_for_a_versioned_candidate() {
        let gets = Arc::new(AtomicUsize::new(0));
        let removed_versions = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let backend: WarmBackendImpl = Box::new(RejectingProbeVersionBackend {
            gets: gets.clone(),
            present: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            removed_versions: removed_versions.clone(),
            returned_version: String::new(),
        });

        let err = check_warm_backend(Some(&backend))
            .await
            .expect_err("an empty PUT version must not read or generically delete a versioned object");

        assert_eq!(err.code, ERR_TIER_INVALID_CONFIG.code);
        assert_eq!(gets.load(Ordering::SeqCst), 0);
        assert_eq!(removed_versions.lock().await.as_slice(), [PROBE_VERSION]);
    }

    #[tokio::test]
    async fn check_warm_backend_forwards_probe_version_to_get_and_remove() {
        let get_versions = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let removed_versions = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let backend: WarmBackendImpl = Box::new(RecordingProbeBackend {
            get_versions: get_versions.clone(),
            present: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            removed_versions: removed_versions.clone(),
            remove_clears_candidate: true,
            fail_get: false,
            body: ProbeBody::Exact,
        });

        check_warm_backend(Some(&backend))
            .await
            .expect("a successful probe should validate, read, and remove its object");

        assert_eq!(get_versions.lock().await.as_slice(), [PROBE_VERSION]);
        assert_eq!(removed_versions.lock().await.as_slice(), [PROBE_VERSION]);
    }

    #[tokio::test]
    async fn check_warm_backend_removes_probe_after_get_failure() {
        let get_versions = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let removed_versions = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let backend: WarmBackendImpl = Box::new(RecordingProbeBackend {
            get_versions: get_versions.clone(),
            present: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            removed_versions: removed_versions.clone(),
            remove_clears_candidate: true,
            fail_get: true,
            body: ProbeBody::Exact,
        });

        let err = check_warm_backend(Some(&backend))
            .await
            .expect_err("a failed probe GET should return a permission error after cleanup");

        assert_eq!(err.code, ERR_TIER_PERM_ERR.code);
        assert_eq!(get_versions.lock().await.as_slice(), [PROBE_VERSION]);
        assert_eq!(removed_versions.lock().await.as_slice(), [PROBE_VERSION]);
    }

    #[tokio::test]
    async fn check_warm_backend_removes_probe_after_body_mismatch() {
        let get_versions = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let removed_versions = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let backend: WarmBackendImpl = Box::new(RecordingProbeBackend {
            get_versions,
            present: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            removed_versions: removed_versions.clone(),
            remove_clears_candidate: true,
            fail_get: false,
            body: ProbeBody::Mismatch,
        });

        let err = check_warm_backend(Some(&backend))
            .await
            .expect_err("a mismatched body should fail after cleanup");

        assert_eq!(err.code, ERR_TIER_PERM_ERR.code);
        assert_eq!(removed_versions.lock().await.as_slice(), [PROBE_VERSION]);
    }

    #[tokio::test]
    async fn check_warm_backend_rejects_a_stale_candidate_after_successful_delete() {
        let removed_versions = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let backend: WarmBackendImpl = Box::new(RecordingProbeBackend {
            get_versions: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            present: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            removed_versions: removed_versions.clone(),
            remove_clears_candidate: false,
            fail_get: false,
            body: ProbeBody::Exact,
        });

        let err = check_warm_backend(Some(&backend))
            .await
            .expect_err("cleanup must not succeed while the deleted candidate remains visible");

        assert_eq!(err.code, ERR_TIER_PERM_ERR.code);
        assert!(err.message.contains("cleanup is incomplete"));
        assert_eq!(removed_versions.lock().await.as_slice(), [PROBE_VERSION]);
    }

    #[tokio::test(start_paused = true)]
    async fn check_warm_backend_reconciles_a_lost_put_response() {
        let backend = MockWarmBackend::new();
        backend.lose_next_put_response();
        let driver: WarmBackendImpl = Box::new(backend.clone());

        let err = check_warm_backend(Some(&driver))
            .await
            .expect_err("a lost probe PUT response must fail after compensation");

        assert_eq!(err.code, ERR_TIER_PERM_ERR.code);
        assert_eq!(backend.object_count().await, 0);
        assert_eq!(backend.exact_remove_count(), 1);
        let operations = backend.op_log().await;
        let put = operations.iter().find_map(|operation| match operation {
            MockWarmOp::Put { object } => Some(object),
            _ => None,
        });
        let probe = operations.iter().find_map(|operation| match operation {
            MockWarmOp::Probe { object } => Some(object),
            _ => None,
        });
        let remove = operations.iter().find_map(|operation| match operation {
            MockWarmOp::Remove { object } => Some(object),
            _ => None,
        });
        let (Some(put), Some(probe), Some(remove)) = (put, probe, remove) else {
            panic!("lost-response compensation should PUT, probe, and remove");
        };
        assert_eq!(put, probe);
        assert_eq!(probe, remove);
    }

    #[tokio::test(start_paused = true)]
    async fn check_warm_backend_retries_until_a_late_put_becomes_visible() {
        let probes = Arc::new(AtomicUsize::new(0));
        let removed_versions = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let driver: WarmBackendImpl = Box::new(LateVisibleProbeBackend {
            visible_at: tokio::time::Instant::now() + Duration::from_secs(5),
            removed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            probes: probes.clone(),
            removed_versions: removed_versions.clone(),
        });

        let err = check_warm_backend(Some(&driver))
            .await
            .expect_err("a late-visible probe PUT must still report the lost response");

        assert_eq!(err.code, ERR_TIER_PERM_ERR.code);
        assert!(
            probes.load(Ordering::SeqCst) > 5,
            "reconciliation must not stop at the first Missing result"
        );
        assert_eq!(removed_versions.lock().await.as_slice(), [PROBE_VERSION]);
    }

    #[tokio::test]
    async fn check_warm_backend_reports_incomplete_cleanup_without_guessing() {
        for candidate in [TransitionCandidateProbe::Unsupported, TransitionCandidateProbe::Ambiguous] {
            let backend = MockWarmBackend::new();
            backend.set_transition_candidate_probe_override(Some(candidate)).await;
            backend.lose_next_put_response();
            let driver: WarmBackendImpl = Box::new(backend.clone());

            let err = check_warm_backend(Some(&driver))
                .await
                .expect_err("an uncertain candidate must fail without a guessed delete");

            assert_eq!(err.code, ERR_TIER_PERM_ERR.code);
            assert!(err.message.contains("cleanup is incomplete"));
            assert_eq!(backend.remove_count().await, 0);
            assert_eq!(backend.object_count().await, 1);
        }
    }

    #[tokio::test]
    async fn check_warm_backend_reports_an_exact_cleanup_failure() {
        let backend = MockWarmBackend::new();
        backend.set_remove_failure(true);
        backend.lose_next_put_response();
        let driver: WarmBackendImpl = Box::new(backend.clone());

        let err = check_warm_backend(Some(&driver))
            .await
            .expect_err("an exact cleanup failure must replace the ambiguous PUT error");

        assert_eq!(err.code, ERR_TIER_PERM_ERR.code);
        assert!(err.message.contains("cleanup is incomplete"));
        assert_eq!(backend.exact_remove_count(), 1);
        assert_eq!(backend.object_count().await, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn check_warm_backend_reconciles_a_timed_out_put() {
        let put_started = Arc::new(tokio::sync::Notify::new());
        let probes = Arc::new(AtomicUsize::new(0));
        let removed_versions = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let driver: WarmBackendImpl = Box::new(HangingProbePutBackend {
            put_started: put_started.clone(),
            present: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            probes: probes.clone(),
            removed_versions: removed_versions.clone(),
        });
        let check = check_warm_backend(Some(&driver));
        tokio::pin!(check);
        tokio::select! {
            _ = put_started.notified() => {}
            result = &mut check => panic!("probe completed before the PUT timeout: {result:?}"),
        }

        tokio::time::advance(WARM_BACKEND_PROBE_TIMEOUT + Duration::from_millis(1)).await;
        let err = check.await.expect_err("a timed out probe PUT must fail after compensation");

        assert_eq!(err.code, ERR_TIER_BACKEND_IN_USE.code);
        assert!(
            probes.load(Ordering::SeqCst) > 1,
            "timed-out PUT reconciliation must keep checking through the visibility window"
        );
        assert_eq!(removed_versions.lock().await.as_slice(), [PROBE_VERSION]);
    }

    #[tokio::test]
    async fn new_wasabi_backend_honors_probe_flag() {
        let tier = TierConfig {
            name: "WASABI".to_string(),
            tier_type: TierType::Wasabi,
            wasabi: Some(TierWasabi {
                name: "WASABI".to_string(),
                access_key: "invalid\naccess-key".to_string(),
                secret_key: "secret-key".to_string(),
                bucket: "tier-bucket".to_string(),
                prefix: "archive".to_string(),
                region: "us-east-1".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };

        let backend = new_warm_backend(&tier, false)
            .await
            .expect("valid Wasabi config should initialize without probing the remote service");

        assert!(backend.validate_remote_version_id("").is_ok());
        assert!(backend.validate_remote_version_id("unexpected-version").is_err());

        let err = match new_warm_backend(&tier, true).await {
            Ok(_) => panic!("probing a Wasabi backend must validate its credentials"),
            Err(err) => err,
        };
        assert_eq!(err.code, ERR_TIER_INVALID_CONFIG.code);
    }

    /// Every S3-compatible provider file pins this same floor today.
    const PROVIDER_MIN_PART_SIZE: i64 = 1024 * 1024 * 128;

    fn s3_compatible_params(endpoint: &str) -> S3CompatibleWarmBackendParams<'_> {
        S3CompatibleWarmBackendParams {
            endpoint,
            access_key: "access",
            secret_key: "secret",
            bucket: "tier-bucket",
            prefix: "archive",
            region: "us-east-1",
            bucket_lookup: BucketLookupType::BucketLookupDNS,
            provider_tag: "aliyun",
            validate_endpoint: validate_outbound_url,
        }
    }

    /// `WarmBackendS3` has no `Debug`, so `Result::expect_err` is unavailable.
    async fn init_error(params: S3CompatibleWarmBackendParams<'_>, must_fail_because: &str) -> std::io::Error {
        match new_s3_compatible_warm_backend(params).await {
            Ok(_) => panic!("{must_fail_because}"),
            Err(err) => err,
        }
    }

    #[tokio::test]
    async fn s3_compatible_backend_rejects_missing_credentials_before_parsing_the_endpoint() {
        let mut params = s3_compatible_params("://not-a-url");
        params.access_key = "";
        let err = init_error(params, "an empty access key must be rejected").await;
        assert_eq!(err.to_string(), "both access and secret keys are required");

        let mut params = s3_compatible_params("://not-a-url");
        params.secret_key = "";
        let err = init_error(params, "an empty secret key must be rejected").await;
        assert_eq!(err.to_string(), "both access and secret keys are required");
    }

    #[tokio::test]
    async fn s3_compatible_backend_rejects_an_empty_bucket_before_parsing_the_endpoint() {
        let mut params = s3_compatible_params("://not-a-url");
        params.bucket = "";

        let err = init_error(params, "an empty bucket must be rejected").await;

        assert_eq!(err.to_string(), "no bucket name was provided");
    }

    #[tokio::test]
    async fn s3_compatible_backend_rejects_an_unparsable_endpoint() {
        let err = init_error(s3_compatible_params("://not-a-url"), "an endpoint that is not a URL must be rejected").await;

        assert_eq!(err.to_string(), url::ParseError::RelativeUrlWithoutBase.to_string());
    }

    #[tokio::test]
    async fn s3_compatible_backend_rejects_an_endpoint_without_a_host() {
        let err = init_error(s3_compatible_params("rustfs://"), "an endpoint without a host must be rejected").await;

        assert_eq!(err.to_string(), "Invalid endpoint URL: missing host");
    }

    /// Every migrated provider that uses `validate_outbound_url` directly (all
    /// but RustFS, which injects its own debug-only, env-gated wrapper — see
    /// rustfs/rustfs#6773) goes through this one construction path, so the
    /// SSRF guard only needs to be pinned here rather than once per provider
    /// file (see backlog#2040's migrate steps and rustfs/rustfs#6764).
    #[tokio::test]
    async fn s3_compatible_backend_rejects_a_loopback_endpoint_before_any_network_setup() {
        let err = init_error(s3_compatible_params("https://127.0.0.1:9000"), "a loopback endpoint must be rejected").await;

        assert!(err.to_string().contains("not allowed"), "unexpected error: {err}");
    }

    #[tokio::test]
    async fn s3_compatible_backend_carries_provider_options_to_the_transition_client() {
        let backend = new_s3_compatible_warm_backend(s3_compatible_params("http://tier.example.com:9000"))
            .await
            .expect("a well-formed S3-compatible tier config should initialize offline");

        assert_eq!(backend.bucket, "tier-bucket");
        assert_eq!(backend.prefix, "archive");
        assert_eq!(backend.storage_class, "");
        assert!(!backend.client.secure);
        assert_eq!(backend.client.endpoint_url.scheme(), "http");
        assert_eq!(backend.client.endpoint_url.host_str(), Some("tier.example.com"));
        assert_eq!(backend.client.endpoint_url.port(), Some(9000));
        assert_eq!(backend.client.region, "us-east-1");
        assert_eq!(backend.client.lookup, BucketLookupType::BucketLookupDNS);
        // The provider constructors all request `trailing_headers: true`, but
        // `TransitionClient` gates the feature on an explicitly overridden SigV4
        // signer, which none of them set. Pin the resulting `false` so migrating
        // a provider onto this constructor cannot silently flip wire behavior.
        assert!(!backend.client.trailing_header_support);
        assert_eq!(backend.client.tier_type, "aliyun");
    }

    #[tokio::test]
    async fn s3_compatible_backend_derives_tls_and_the_default_port_from_the_scheme() {
        let secure = new_s3_compatible_warm_backend(s3_compatible_params("https://tier.example.com"))
            .await
            .expect("an https endpoint should initialize offline");
        assert!(secure.client.secure);
        assert_eq!(secure.client.endpoint_url.scheme(), "https");
        assert_eq!(secure.client.endpoint_url.port_or_known_default(), Some(443));

        let insecure = new_s3_compatible_warm_backend(s3_compatible_params("http://tier.example.com"))
            .await
            .expect("an http endpoint should initialize offline");
        assert!(!insecure.client.secure);
        assert_eq!(insecure.client.endpoint_url.scheme(), "http");
        assert_eq!(insecure.client.endpoint_url.port_or_known_default(), Some(80));
    }

    #[test]
    fn endpoint_authority_preserves_ipv6_brackets_and_explicit_port() {
        let url = url::Url::parse("https://[2001:db8::1]:9443").expect("the IPv6 endpoint should parse");
        assert_eq!(
            endpoint_authority(&url).expect("the endpoint should have an authority"),
            "[2001:db8::1]:9443"
        );
    }

    #[tokio::test]
    async fn s3_compatible_backend_strips_only_a_trailing_prefix_separator() {
        let mut params = s3_compatible_params("http://tier.example.com:9000");
        params.prefix = "archive/";
        let trimmed = new_s3_compatible_warm_backend(params)
            .await
            .expect("a prefix with a trailing separator should initialize offline");
        assert_eq!(trimmed.prefix, "archive");

        let mut params = s3_compatible_params("http://tier.example.com:9000");
        params.prefix = "archive/nested";
        let untouched = new_s3_compatible_warm_backend(params)
            .await
            .expect("a nested prefix should initialize offline");
        assert_eq!(untouched.prefix, "archive/nested");

        let mut params = s3_compatible_params("http://tier.example.com:9000");
        params.prefix = "";
        let empty = new_s3_compatible_warm_backend(params)
            .await
            .expect("an empty prefix should initialize offline");
        assert_eq!(empty.prefix, "");
    }

    #[tokio::test]
    async fn s3_compatible_backend_honors_the_auto_bucket_lookup_family() {
        let mut params = s3_compatible_params("http://tier.example.com:9000");
        params.bucket_lookup = BucketLookupType::BucketLookupAuto;
        params.provider_tag = "minio";

        let backend = new_s3_compatible_warm_backend(params)
            .await
            .expect("the auto-lookup provider family should initialize offline");

        assert_eq!(backend.client.lookup, BucketLookupType::BucketLookupAuto);
        assert_eq!(backend.client.tier_type, "minio");
    }

    #[test]
    fn optimal_part_size_charges_an_unknown_length_the_multipart_ceiling() {
        let unknown = optimal_part_size(-1, PROVIDER_MIN_PART_SIZE).expect("an unknown length must be accepted");
        let ceiling =
            optimal_part_size(MAX_MULTIPART_PUT_OBJECT_SIZE, PROVIDER_MIN_PART_SIZE).expect("the exact ceiling must be accepted");

        assert_eq!(unknown, ceiling);
        assert_eq!(unknown, 5 * PROVIDER_MIN_PART_SIZE);
        assert!(unknown * MAX_PARTS_COUNT >= MAX_MULTIPART_PUT_OBJECT_SIZE);
    }

    #[test]
    fn optimal_part_size_rejects_an_object_above_the_multipart_ceiling() {
        let err = optimal_part_size(MAX_MULTIPART_PUT_OBJECT_SIZE + 1, PROVIDER_MIN_PART_SIZE)
            .expect_err("an object past the multipart ceiling must fail closed");

        assert_eq!(err.to_string(), "entity too large");
    }

    #[test]
    fn optimal_part_size_never_returns_less_than_one_part() {
        assert_eq!(
            optimal_part_size(0, PROVIDER_MIN_PART_SIZE).expect("a zero-length object must be accepted"),
            PROVIDER_MIN_PART_SIZE
        );
        assert_eq!(
            optimal_part_size(1024, PROVIDER_MIN_PART_SIZE).expect("a tiny object must be accepted"),
            PROVIDER_MIN_PART_SIZE
        );
        assert_eq!(
            optimal_part_size(PROVIDER_MIN_PART_SIZE, PROVIDER_MIN_PART_SIZE)
                .expect("an object of exactly one part must be accepted"),
            PROVIDER_MIN_PART_SIZE
        );
    }

    #[test]
    fn build_transition_put_options_preserves_content_headers() {
        let mut metadata = HashMap::new();
        metadata.insert("content-type".to_string(), "text/plain".to_string());
        metadata.insert("content-encoding".to_string(), "gzip".to_string());
        metadata.insert("cache-control".to_string(), "max-age=60".to_string());

        let opts = build_transition_put_options("COLD".to_string(), metadata);

        assert_eq!(opts.content_type, "text/plain");
        assert_eq!(opts.content_encoding, "gzip");
        assert_eq!(opts.cache_control, "max-age=60");
        assert_eq!(opts.internal.replication_status.as_str(), "");
        assert_eq!(opts.legalhold.as_str(), "");
    }

    #[test]
    fn build_transition_put_options_preserves_object_lock_headers_when_present() {
        let mut metadata = HashMap::new();
        metadata.insert(X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.to_string(), "2026-03-23T00:00:00Z".to_string());
        metadata.insert(X_AMZ_OBJECT_LOCK_LEGAL_HOLD.to_string(), ObjectLockLegalHoldStatus::ON.to_string());
        metadata.insert(X_AMZ_OBJECT_LOCK_MODE.to_string(), ObjectLockRetentionMode::GOVERNANCE.to_string());

        let opts = build_transition_put_options("COLD".to_string(), metadata);

        assert_eq!(opts.mode.as_str(), ObjectLockRetentionMode::GOVERNANCE);
        assert_eq!(opts.legalhold.as_str(), ObjectLockLegalHoldStatus::ON);
        assert_ne!(opts.retain_until_date, OffsetDateTime::UNIX_EPOCH);
    }

    #[test]
    fn build_transition_put_options_filters_promoted_headers_from_user_metadata() {
        let mut metadata = HashMap::new();
        metadata.insert("name".to_string(), "object".to_string());
        metadata.insert(CONTENT_TYPE.to_string(), "text/plain".to_string());
        metadata.insert(X_AMZ_OBJECT_LOCK_LEGAL_HOLD.to_string(), ObjectLockLegalHoldStatus::ON.to_string());
        metadata.insert(X_AMZ_REPLICATION_STATUS.to_string(), "PENDING".to_string());

        let opts = build_transition_put_options("COLD".to_string(), metadata);

        assert_eq!(opts.user_metadata.get("name"), Some(&"object".to_string()));
        assert!(!opts.user_metadata.contains_key(CONTENT_TYPE));
        assert!(!opts.user_metadata.contains_key(X_AMZ_OBJECT_LOCK_LEGAL_HOLD.as_str()));
        assert!(!opts.user_metadata.contains_key(X_AMZ_REPLICATION_STATUS.as_str()));
    }

    #[test]
    fn build_transition_put_options_persists_both_candidate_identity_keys_as_s3_metadata() {
        let mut metadata = HashMap::new();
        rustfs_utils::http::metadata_compat::insert_str(
            &mut metadata,
            rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TRANSACTION_ID,
            "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa".to_string(),
        );
        rustfs_utils::http::metadata_compat::insert_str(
            &mut metadata,
            rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TIER_DESTINATION_ID,
            "5a".repeat(32),
        );

        let opts = build_transition_put_options("COLD".to_string(), metadata);

        for suffix in [
            rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TRANSACTION_ID,
            rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TIER_DESTINATION_ID,
        ] {
            assert!(opts.user_metadata.contains_key(&format!(
                "x-amz-meta-{}",
                rustfs_utils::http::metadata_compat::internal_key_rustfs(suffix)
            )));
            assert!(opts.user_metadata.contains_key(&format!(
                "x-amz-meta-{}{suffix}",
                rustfs_utils::http::metadata_compat::MINIO_INTERNAL_PREFIX
            )));
        }
    }

    #[test]
    fn build_transition_put_options_requests_no_checksum_and_content_md5() {
        // Regression for rustfs/rustfs#4811: transition uploads must leave the
        // additional-checksum modes unset and rely on Content-MD5. If `checksum`
        // were (incorrectly) reported as set, the >128 MiB multipart put path
        // would call `ChecksumNone.hasher()` and fail with "unsupported checksum
        // type". Objects <=128 MiB take the single-part path and only worked by
        // silently dropping the checksum, so pin both invariants here.
        let opts = build_transition_put_options("COLD".to_string(), HashMap::new());

        assert!(!opts.checksum.is_set(), "transition put must not request an additional checksum");
        assert!(!opts.auto_checksum.is_set(), "transition put must not preset auto_checksum");
        assert!(opts.send_content_md5, "transition put must send Content-MD5");
    }
}
