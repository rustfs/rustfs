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

use crate::bucket::metadata::BucketMetadata;
use crate::bucket::metadata_sys::get_bucket_targets_config;
use crate::bucket::metadata_sys::get_replication_config;
use crate::bucket::remote_s3_client::{
    PathStyle, REPLICATION_TARGET_RETRY_POLICY, RemoteCredentials, RemoteS3EndpointSpec, build_remote_s3_client,
};
use crate::bucket::replication::{ObjectLockIntegrity, object_lock_put_integrity, replication_etags_match};
use crate::bucket::replication::{ReplicationStatusType, ReplicationTargetConfigBridge};
use crate::bucket::target::ARN;
use crate::bucket::target::BucketTargetType;
use crate::bucket::target::{self, BucketTarget, BucketTargets, Credentials};
use crate::bucket::versioning_sys::BucketVersioningSys;
use crate::runtime::sources as runtime_sources;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput;
use aws_sdk_s3::operation::delete_object_tagging::{DeleteObjectTaggingError, DeleteObjectTaggingOutput};
use aws_sdk_s3::operation::get_object::{GetObjectError, GetObjectOutput};
use aws_sdk_s3::operation::get_object_tagging::{GetObjectTaggingError, GetObjectTaggingOutput};
use aws_sdk_s3::operation::head_bucket::HeadBucketError;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::operation::put_object_tagging::{PutObjectTaggingError, PutObjectTaggingOutput};
use aws_sdk_s3::operation::upload_part::UploadPartOutput;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::BucketVersioningStatus;
use aws_sdk_s3::types::Tagging as SdkTagging;
use aws_sdk_s3::types::{
    ChecksumAlgorithm, ChecksumMode, CompletedMultipartUpload, CompletedPart, ObjectLockLegalHoldStatus, ObjectLockRetentionMode,
    ServerSideEncryption,
};
use aws_sdk_s3::{Client as S3Client, operation::head_object::HeadObjectOutput};
use aws_smithy_runtime_api::client::orchestrator::HttpRequest;
use futures::{StreamExt, stream};
use http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use reqwest::Client as HttpClient;
use rustfs_utils::http::{
    AMZ_BUCKET_REPLICATION_STATUS, AMZ_OBJECT_LOCK_BYPASS_GOVERNANCE, AMZ_OBJECT_LOCK_LEGAL_HOLD, AMZ_OBJECT_LOCK_MODE,
    AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE, AMZ_OBJECT_TAGGING_LOWER, AMZ_STORAGE_CLASS, AMZ_WEBSITE_REDIRECT_LOCATION, is_amz_header,
    is_minio_header, is_rustfs_header, is_standard_header, is_storageclass_header,
};
use rustfs_utils::http::{
    SUFFIX_FORCE_DELETE, SUFFIX_SOURCE_DELETEMARKER, SUFFIX_SOURCE_ETAG, SUFFIX_SOURCE_MTIME, SUFFIX_SOURCE_PROXY_REQUEST,
    SUFFIX_SOURCE_REPLICATION_CHECK, SUFFIX_SOURCE_REPLICATION_LEGALHOLD_TIMESTAMP, SUFFIX_SOURCE_REPLICATION_REQUEST,
    SUFFIX_SOURCE_REPLICATION_RETENTION_TIMESTAMP, SUFFIX_SOURCE_REPLICATION_TAGGING_TIMESTAMP, SUFFIX_SOURCE_VERSION_ID,
    insert_header,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt;
use std::str::FromStr as _;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::Weak;
use std::time::{Duration, Instant, SystemTime};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tracing::error;
use tracing::warn;
use url::Url;
use uuid::Uuid;

const MAX_CONCURRENT_TARGET_HEALTH_CHECKS: usize = 16;
const REDACTED_CREDENTIAL: &str = "<redacted>";

fn remote_credentials(credentials: &Credentials, account_id: &str) -> RemoteCredentials {
    RemoteCredentials {
        access_key: credentials.access_key.clone(),
        secret_key: credentials.secret_key.clone(),
        session_token: credentials.effective_session_token().map(str::to_string),
        expiration: credentials.effective_expiration().map(SystemTime::from),
        account_id: account_id.to_string(),
    }
}

fn target_path_style(path: &str) -> PathStyle {
    match path.trim().to_ascii_lowercase().as_str() {
        // Explicit DNS/virtual-hosted-style requested by user.
        "dns" | "off" | "false" => PathStyle::VirtualHost,
        // Explicit path-style or legacy boolean-like values.
        "path" | "on" | "true" => PathStyle::Path,
        // `auto` and empty are defaulted to path-style for custom S3-compatible endpoints.
        "auto" | "" => PathStyle::Auto,
        // Unknown values: prefer compatibility with S3-compatible services.
        _ => PathStyle::Path,
    }
}

impl From<&BucketTarget> for RemoteS3EndpointSpec {
    fn from(target: &BucketTarget) -> Self {
        RemoteS3EndpointSpec {
            endpoint: target.endpoint.clone(),
            secure: target.secure,
            region: target.region.clone(),
            path_style: target_path_style(&target.path),
            credentials: target
                .credentials
                .as_ref()
                .map(|credentials| remote_credentials(credentials, &target.reset_id)),
            skip_tls_verify: target.skip_tls_verify,
            ca_cert_pem: (!target.ca_cert_pem.trim().is_empty()).then(|| target.ca_cert_pem.clone()),
            connect_timeout: None,
            read_timeout: None,
            // Replication has no retry budget of its own on the request path,
            // so it keeps the SDK's standard three attempts; stating it here
            // pins the behaviour to this line instead of an SDK default.
            retry: REPLICATION_TARGET_RETRY_POLICY,
            user_agent_suffix: "",
        }
    }
}

pub type HeadObjectSdkError = Box<SdkError<HeadObjectError>>;

/// Whether an edited bucket target still addresses the same remote service
/// (endpoint, bucket, path style, TLS and identity), so a verdict learned
/// about that service stays valid across the edit.
fn same_replication_service(edited: &BucketTarget, previous: &BucketTarget) -> bool {
    let access_key = |target: &BucketTarget| target.credentials.as_ref().map(|credentials| credentials.access_key.clone());
    edited.endpoint == previous.endpoint
        && edited.target_bucket == previous.target_bucket
        && edited.secure == previous.secure
        && edited.path == previous.path
        && access_key(edited) == access_key(previous)
}

/// Page size and page budget for [`TargetClient::find_version_by_etag`].
const FIND_VERSION_BY_ETAG_PAGE_SIZE: i32 = 1000;
const FIND_VERSION_BY_ETAG_MAX_PAGES: usize = 8;
pub type GetObjectSdkError = Box<SdkError<GetObjectError>>;
pub type GetObjectTaggingSdkError = Box<SdkError<GetObjectTaggingError>>;
pub type PutObjectTaggingSdkError = Box<SdkError<PutObjectTaggingError>>;
pub type DeleteObjectTaggingSdkError = Box<SdkError<DeleteObjectTaggingError>>;

pub static GLOBAL_BUCKET_TARGET_SYS: OnceLock<BucketTargetSys> = OnceLock::new();

fn replication_target_versioning_enabled(versioning: Option<&BucketVersioningStatus>) -> bool {
    matches!(versioning, Some(BucketVersioningStatus::Enabled))
}

#[derive(Debug, Clone)]
pub struct ArnTarget {
    pub client: Option<Arc<TargetClient>>,
    pub last_refresh: OffsetDateTime,
}

impl Default for ArnTarget {
    fn default() -> Self {
        Self {
            client: None,
            last_refresh: OffsetDateTime::UNIX_EPOCH,
        }
    }
}

impl ArnTarget {
    pub fn with_client(client: Arc<TargetClient>) -> Self {
        Self {
            client: Some(client),
            last_refresh: OffsetDateTime::now_utc(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ArnErrs {
    pub count: i64,
    pub update_in_progress: bool,
    pub bucket: String,
}

/// A single latency sample tagged with the instant it was recorded.
///
/// Only `dur` participates in (de)serialization; `at` is not `Serialize`
/// (`Instant` isn't) and is reconstructed as `Instant::now()` on deserialize.
/// A reloaded window therefore simply restarts aging, which is acceptable for
/// the in-memory endpoint health stats this type backs (see backlog#806-16).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LatencySample {
    dur: Duration,
    #[serde(skip, default = "instant_now")]
    at: Instant,
}

fn instant_now() -> Instant {
    Instant::now()
}

/// A rolling one-minute latency window.
///
/// backlog#806-16: the previous implementation stored bare `Vec<Duration>`
/// samples plus a single, never-updated `start_time`, and its `retain`
/// predicate ignored the element entirely — it evaluated the constant
/// `now.duration_since(self.start_time) < 60s`. Once the window had existed
/// for 60s, that predicate became `false` for every element, so each `add`
/// dropped ALL samples and the "last minute" average degenerated to the most
/// recent single sample. Each sample now carries its own timestamp and is
/// retained by its individual age.
///
/// This type is only used for in-memory endpoint health (`EpHealth`) and
/// admin-API display; it is not persisted or wire-serialized across versions
/// (the on-the-wire latency shape is `crate::bucket::target::LatencyStat`,
/// which carries only `curr`/`avg`/`max`). The `Serialize`/`Deserialize`
/// derives are retained solely so the enclosing `LatencyStat` keeps deriving
/// them; only the `Duration` part of each sample is serialized.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LastMinuteLatency {
    times: Vec<LatencySample>,
}

impl LastMinuteLatency {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, duration: Duration) {
        self.add_at(Instant::now(), duration);
    }

    /// Records a sample at an explicit instant, dropping samples older than one
    /// minute relative to `now`. Split out from `add` so the aging logic can be
    /// exercised with a synthetic clock in tests.
    fn add_at(&mut self, now: Instant, duration: Duration) {
        self.times
            .retain(|sample| now.duration_since(sample.at) < Duration::from_secs(60));
        self.times.push(LatencySample { dur: duration, at: now });
    }

    pub fn get_total(&self) -> LatencyAverage {
        if self.times.is_empty() {
            return LatencyAverage {
                avg: Duration::from_secs(0),
            };
        }
        let total: Duration = self.times.iter().map(|sample| sample.dur).sum();
        LatencyAverage {
            avg: total / self.times.len() as u32,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LatencyAverage {
    pub avg: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LatencyStat {
    pub lastmin: LastMinuteLatency,
    pub curr: Duration,
    pub avg: Duration,
    pub peak: Duration,
    pub n: i64,
}

impl LatencyStat {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn update(&mut self, duration: Duration) {
        self.lastmin.add(duration);
        self.n += 1;
        if duration > self.peak {
            self.peak = duration;
        }
        self.curr = self.lastmin.get_total().avg;
        self.avg = Duration::from_nanos(
            (self.avg.as_nanos() as i64 * (self.n - 1) + self.curr.as_nanos() as i64) as u64 / self.n as u64,
        );
    }
}

#[derive(Debug, Clone)]
pub struct EpHealth {
    pub endpoint: String,
    pub scheme: String,
    pub online: bool,
    pub last_online: Option<OffsetDateTime>,
    pub last_hc_at: Option<OffsetDateTime>,
    pub offline_duration: Duration,
    pub offline_count: u64,
    pub latency: LatencyStat,
}

impl Default for EpHealth {
    fn default() -> Self {
        Self {
            endpoint: String::new(),
            scheme: String::new(),
            online: true,
            last_online: None,
            last_hc_at: None,
            offline_duration: Duration::from_secs(0),
            offline_count: 0,
            latency: LatencyStat::new(),
        }
    }
}

fn endpoint_health_key(url: &Url) -> String {
    let host = url.host_str().unwrap_or_default();
    match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    }
}

fn target_health(target: &TargetClient) -> EpHealth {
    let url = target.to_url();
    EpHealth {
        endpoint: endpoint_health_key(&url),
        scheme: url.scheme().to_string(),
        online: true,
        ..Default::default()
    }
}

fn update_endpoint_health(health: &mut EpHealth, online: bool, latency: Duration, now: OffsetDateTime) {
    let prev_online = health.online;
    health.online = online;
    health.last_hc_at = Some(now);
    health.latency.update(latency);

    if online {
        health.last_online = Some(now);
        return;
    }

    if prev_online {
        health.offline_count += 1;
    }
    health.offline_duration += latency;
}

#[cfg(test)]
#[derive(Clone, Debug)]
struct TargetClientBuildProbe {
    arn: String,
    started: Arc<tokio::sync::Semaphore>,
    release: Arc<tokio::sync::Semaphore>,
}

/// SSE-C passthrough capability verdicts (see the enum's own docs in
/// `rustfs-replication`) are cached here per target ARN: entries follow the
/// `arn_remotes_map` lifecycle (rebuilding or removing a target resets its
/// capability to `Unknown`) and additionally expire after
/// [`SSEC_PASSTHROUGH_CAPABILITY_TTL`], after which the next attempt
/// re-audits. Re-exported so existing `bucket_target_sys` consumers keep
/// their import path while the verdict vocabulary lives with the
/// replication decision logic.
pub use crate::bucket::replication::SsecPassthroughCapability;
/// Version-identity verdicts (see the enum's own docs in
/// `rustfs-replication`) are cached here per target ARN and follow the same
/// `arn_remotes_map` lifecycle. They carry no TTL: the verdict is refreshed
/// by every replication write's response, so it can only go stale on a
/// target that receives no writes — and a stale `MintsOwn` costs one extra
/// content-identity lookup before a PUT, never a lost replica.
pub use crate::bucket::replication::VersionIdentityCapability;

/// How long an audited SSE-C passthrough verdict stays authoritative.
///
/// Trade-off: without a TTL a verdict is sticky for the process lifetime —
/// an `Unsupported` target that gets upgraded (or re-probed only via
/// replication-check) would keep failing SSE-C replication forever, and the
/// fail-open twin: a `Supported` verdict would outlive a backend swapped
/// behind the same endpoint/ARN. With the TTL, a bad target costs at most
/// one wasted PUT+HEAD audit per TTL window, and a changed backend is
/// re-discovered within the same window.
pub const SSEC_PASSTHROUGH_CAPABILITY_TTL: Duration = Duration::from_secs(10 * 60);

/// A recorded SSE-C passthrough verdict plus when it was recorded, so reads
/// can report staleness against [`SSEC_PASSTHROUGH_CAPABILITY_TTL`].
#[derive(Debug, Clone, Copy)]
struct SsecPassthroughRecord {
    capability: SsecPassthroughCapability,
    recorded_at: Instant,
}

#[derive(Debug, Default)]
pub struct BucketTargetSys {
    pub arn_remotes_map: Arc<RwLock<HashMap<String, ArnTarget>>>,
    /// SSE-C passthrough capability verdicts keyed by target ARN. See
    /// [`SsecPassthroughCapability`]; reset alongside `arn_remotes_map`.
    ssec_passthrough_map: Arc<RwLock<HashMap<String, SsecPassthroughRecord>>>,
    /// Version-identity verdicts keyed by target ARN. See
    /// [`VersionIdentityCapability`]; reset alongside `arn_remotes_map`. A std
    /// lock (never held across an await) so the replication worker can record
    /// a verdict from inside its synchronous PUT-response audit.
    version_identity_map: Arc<std::sync::RwLock<HashMap<String, VersionIdentityCapability>>>,
    pub targets_map: Arc<RwLock<HashMap<String, Vec<BucketTarget>>>>,
    /// Buckets whose persisted `bucket-targets.json` exists but cannot be
    /// decoded (rustfs/backlog#2282). Written under the bucket's update mutex
    /// alongside `targets_map`, and read before it so an unreadable
    /// configuration surfaces as a typed error instead of an empty target set.
    unreadable_targets: Arc<RwLock<HashSet<String>>>,
    pub h_mutex: Arc<RwLock<HashMap<String, EpHealth>>>,
    target_h_mutex: Arc<RwLock<HashMap<String, EpHealth>>>,
    pub hc_client: Arc<HttpClient>,
    pub a_mutex: Arc<Mutex<HashMap<String, ArnErrs>>>,
    pub arn_errs_map: Arc<RwLock<HashMap<String, ArnErrs>>>,
    target_update_mutexes: Arc<Mutex<HashMap<String, Weak<Mutex<()>>>>>,
    #[cfg(test)]
    target_client_build_probe: Arc<Mutex<Option<TargetClientBuildProbe>>>,
    heartbeat_started: OnceLock<()>,
}

/// Build the bucket-target health-check HTTP client without panicking when
/// the host has no system CA bundle (issue #6734).
///
/// `BucketTargetSys::get()` initializes lazily on the startup path (bucket
/// metadata install calls it on the main thread), and `reqwest::Client::new()`
/// panics when the TLS backend cannot load any system trust root — the state
/// of a minimal container image. Fall back to a client with an explicit empty
/// trust store: HTTP health checks keep working, and HTTPS targets fail closed
/// at the TLS handshake with a clear certificate error instead of aborting
/// the whole process at startup.
fn build_health_check_client() -> HttpClient {
    HttpClient::builder().build().unwrap_or_else(|error| {
        warn!(
            "bucket target health-check HTTP client could not load system TLS roots ({error}); continuing with an empty trust store — HTTPS target health checks will fail until a CA bundle is installed"
        );
        HttpClient::builder()
            .tls_certs_only(std::iter::empty::<reqwest::Certificate>())
            .build()
            .expect("HTTP client construction must succeed with an explicit empty trust store")
    })
}

impl BucketTargetSys {
    pub fn get() -> &'static Self {
        GLOBAL_BUCKET_TARGET_SYS.get_or_init(Self::new)
    }

    fn new() -> Self {
        Self {
            arn_remotes_map: Arc::new(RwLock::new(HashMap::new())),
            ssec_passthrough_map: Arc::new(RwLock::new(HashMap::new())),
            version_identity_map: Arc::new(std::sync::RwLock::new(HashMap::new())),
            targets_map: Arc::new(RwLock::new(HashMap::new())),
            unreadable_targets: Arc::new(RwLock::new(HashSet::new())),
            h_mutex: Arc::new(RwLock::new(HashMap::new())),
            target_h_mutex: Arc::new(RwLock::new(HashMap::new())),
            hc_client: Arc::new(build_health_check_client()),
            a_mutex: Arc::new(Mutex::new(HashMap::new())),
            arn_errs_map: Arc::new(RwLock::new(HashMap::new())),
            target_update_mutexes: Arc::new(Mutex::new(HashMap::new())),
            #[cfg(test)]
            target_client_build_probe: Arc::new(Mutex::new(None)),
            heartbeat_started: OnceLock::new(),
        }
    }

    pub(crate) fn start_heartbeat(&'static self) {
        if self.heartbeat_started.set(()).is_err() {
            return;
        }

        tokio::spawn(async move {
            self.heartbeat().await;
        });
    }

    async fn target_update_mutex(&self, bucket: &str) -> Arc<Mutex<()>> {
        let mut mutexes = self.target_update_mutexes.lock().await;
        mutexes.retain(|_, mutex| mutex.strong_count() > 0);
        if let Some(mutex) = mutexes.get(bucket).and_then(Weak::upgrade) {
            return mutex;
        }
        let mutex = Arc::new(Mutex::new(()));
        mutexes.insert(bucket.to_string(), Arc::downgrade(&mutex));
        mutex
    }

    /// Snapshot the heartbeat-tracked health of `url`'s endpoint.
    ///
    /// Returns `None` when the heartbeat has never seen the endpoint. Unlike
    /// [`Self::is_offline`] this deliberately does not call `init_hc`: a caller
    /// that only reports metrics must not create health entries as a side
    /// effect, or merely rendering a status page would mark an unknown peer
    /// online.
    pub async fn endpoint_health(&self, url: &Url) -> Option<EpHealth> {
        let key = endpoint_health_key(url);
        let health_map = self.h_mutex.read().await;
        health_map.get(&key).cloned()
    }

    pub async fn is_offline(&self, url: &Url) -> bool {
        let key = endpoint_health_key(url);
        {
            let health_map = self.h_mutex.read().await;
            if let Some(health) = health_map.get(&key) {
                return !health.online;
            }
        }
        self.init_hc(url).await;
        false
    }

    pub async fn mark_offline(&self, url: &Url) {
        let key = endpoint_health_key(url);
        let mut health_map = self.h_mutex.write().await;
        if let Some(health) = health_map.get_mut(&key) {
            update_endpoint_health(health, false, Duration::from_secs(0), OffsetDateTime::now_utc());
        }
    }

    pub async fn init_hc(&self, url: &Url) {
        let mut health_map = self.h_mutex.write().await;
        let host = endpoint_health_key(url);
        health_map.insert(
            host.clone(),
            EpHealth {
                endpoint: host,
                scheme: url.scheme().to_string(),
                online: true,
                ..Default::default()
            },
        );
    }

    pub(crate) async fn is_target_offline(&self, target: &Arc<TargetClient>) -> bool {
        // Lock order: arn_remotes_map, then target_h_mutex. A stale client must not
        // read or initialize the health state of its replacement.
        let remotes = self.arn_remotes_map.read().await;
        let Some(current) = remotes.get(&target.arn).and_then(|remote| remote.client.as_ref()) else {
            return true;
        };
        if !Arc::ptr_eq(current, target) {
            return true;
        }
        {
            let health_map = self.target_h_mutex.read().await;
            if let Some(health) = health_map.get(&target.arn) {
                return !health.online;
            }
        }
        let mut health_map = self.target_h_mutex.write().await;
        let health = health_map.entry(target.arn.clone()).or_insert_with(|| target_health(target));
        !health.online
    }

    pub(crate) async fn mark_target_offline(&self, target: &Arc<TargetClient>) {
        // Lock order: arn_remotes_map, then target_h_mutex. Ignore failures reported
        // by a client that has already been replaced.
        let remotes = self.arn_remotes_map.read().await;
        let Some(current) = remotes.get(&target.arn).and_then(|remote| remote.client.as_ref()) else {
            return;
        };
        if !Arc::ptr_eq(current, target) {
            return;
        }
        let mut health_map = self.target_h_mutex.write().await;
        let health = health_map.entry(target.arn.clone()).or_insert_with(|| target_health(target));
        update_endpoint_health(health, false, Duration::from_secs(0), OffsetDateTime::now_utc());
    }

    #[cfg(test)]
    async fn init_target_health(&self, target: &TargetClient) {
        let mut health_map = self.target_h_mutex.write().await;
        health_map.insert(target.arn.clone(), target_health(target));
        drop(health_map);
        self.init_hc(&target.to_url()).await;
    }

    pub async fn heartbeat(&self) {
        // Probe interval: `RUSTFS_REPL_HEALTH_CHECK_INTERVAL_MS` (default 5000ms,
        // clamped to >=10ms), read once when the heartbeat task starts.
        let mut interval = tokio::time::interval(crate::bucket::replication::replication_timing::health_check_interval());
        loop {
            interval.tick().await;
            self.heartbeat_once().await;
        }
    }

    async fn heartbeat_once(&self) {
        let targets = {
            let remotes = self.arn_remotes_map.read().await;
            remotes
                .values()
                .filter_map(|target| target.client.clone())
                .collect::<Vec<_>>()
        };

        let checks = stream::iter(targets.into_iter().map(|target| async move {
            let start = Instant::now();
            let online = Self::check_endpoint_health(&target).await;
            (target, online, start.elapsed())
        }));
        let mut checks = checks.buffer_unordered(MAX_CONCURRENT_TARGET_HEALTH_CHECKS);
        let mut endpoint_checks = HashMap::<String, (String, bool, Duration)>::new();

        while let Some((target, online, duration)) = checks.next().await {
            let url = target.to_url();

            {
                // Lock order: arn_remotes_map, then target_h_mutex. Keeping the remote
                // read guard prevents a replaced client from receiving stale health.
                let remotes = self.arn_remotes_map.read().await;
                let Some(current) = remotes.get(&target.arn).and_then(|remote| remote.client.as_ref()) else {
                    continue;
                };
                if !Arc::ptr_eq(current, &target) {
                    continue;
                }
                let mut health_map = self.target_h_mutex.write().await;
                let health = health_map.entry(target.arn.clone()).or_insert_with(|| target_health(&target));
                update_endpoint_health(health, online, duration, OffsetDateTime::now_utc());
            }

            let endpoint = endpoint_health_key(&url);
            endpoint_checks
                .entry(endpoint)
                .and_modify(|(_, endpoint_online, endpoint_duration)| {
                    *endpoint_online |= online;
                    *endpoint_duration = (*endpoint_duration).max(duration);
                })
                .or_insert_with(|| (url.scheme().to_string(), online, duration));
        }

        let mut health_map = self.h_mutex.write().await;
        for (endpoint, (scheme, online, duration)) in endpoint_checks {
            let health = health_map.entry(endpoint.clone()).or_insert_with(|| EpHealth {
                endpoint,
                scheme,
                online: true,
                ..Default::default()
            });
            update_endpoint_health(health, online, duration, OffsetDateTime::now_utc());
        }
    }

    async fn check_endpoint_health(target: &TargetClient) -> bool {
        match tokio::time::timeout(Duration::from_secs(3), target.client.head_bucket().bucket(&target.bucket).send()).await {
            Ok(Ok(_)) => true,
            Ok(Err(err)) => err.raw_response().is_some_and(|response| response.status().as_u16() < 500),
            Err(_) => false,
        }
    }

    pub async fn health_stats(&self) -> HashMap<String, EpHealth> {
        let health_map = self.h_mutex.read().await;
        health_map.clone()
    }

    async fn target_health_stats(&self) -> HashMap<String, EpHealth> {
        let health_map = self.target_h_mutex.read().await;
        health_map.clone()
    }

    /// Targets of one bucket, or of every bucket when `bucket` is empty.
    ///
    /// A bucket that simply has no targets yields an empty list; a bucket
    /// whose persisted configuration cannot be decoded is an error, so an
    /// admin listing reports the fault instead of an empty list that reads as
    /// "replication is not configured" (rustfs/backlog#2282).
    pub async fn list_targets(&self, bucket: &str, arn_type: &str) -> Result<Vec<BucketTarget>, BucketTargetError> {
        let health_stats = self.target_health_stats().await;
        let mut targets = Vec::new();

        if !bucket.is_empty() {
            match self.list_bucket_targets(bucket).await {
                Ok(bucket_targets) => {
                    for mut target in bucket_targets.targets {
                        if arn_type.is_empty() || target.target_type.to_string() == arn_type {
                            if let Some(health) = health_stats.get(&target.arn) {
                                target.total_downtime = health.offline_duration;
                                target.online = health.online;
                                target.last_online = health.last_online;
                                target.latency = target::LatencyStat {
                                    curr: health.latency.curr,
                                    avg: health.latency.avg,
                                    max: health.latency.peak,
                                };
                                target.offline_count = health.offline_count;
                            }
                            targets.push(target);
                        }
                    }
                }
                Err(BucketTargetError::BucketRemoteTargetNotFound { .. }) => {}
                Err(err) => return Err(err),
            }
            return Ok(targets);
        }

        let targets_map = self.targets_map.read().await;
        for bucket_targets in targets_map.values() {
            for mut target in bucket_targets.iter().cloned() {
                if arn_type.is_empty() || target.target_type.to_string() == arn_type {
                    if let Some(health) = health_stats.get(&target.arn) {
                        target.total_downtime = health.offline_duration;
                        target.online = health.online;
                        target.last_online = health.last_online;
                        target.latency = target::LatencyStat {
                            curr: health.latency.curr,
                            avg: health.latency.avg,
                            max: health.latency.peak,
                        };
                        target.offline_count = health.offline_count;
                    }
                    targets.push(target);
                }
            }
        }

        Ok(targets)
    }

    pub async fn list_bucket_targets(&self, bucket: &str) -> Result<BucketTargets, BucketTargetError> {
        if self.unreadable_targets.read().await.contains(bucket) {
            return Err(BucketTargetError::BucketRemoteTargetsUnreadable {
                bucket: bucket.to_string(),
            });
        }

        let targets_map = self.targets_map.read().await;
        if let Some(targets) = targets_map.get(bucket) {
            Ok(BucketTargets {
                targets: targets.clone(),
            })
        } else {
            Err(BucketTargetError::BucketRemoteTargetNotFound {
                bucket: bucket.to_string(),
            })
        }
    }

    /// Record that this bucket's persisted targets configuration exists but
    /// cannot be decoded (rustfs/backlog#2282).
    ///
    /// Any snapshot published from an earlier readable load is deliberately
    /// left in place: withdrawing it would produce exactly the silent "no
    /// targets configured" state this marker exists to prevent. The marker is
    /// cleared by the next successful publish, which is what makes a repaired
    /// configuration take effect without a restart.
    pub async fn mark_targets_unreadable(&self, bucket: &str) {
        let update_mutex = self.target_update_mutex(bucket).await;
        let _update_guard = update_mutex.lock().await;

        self.unreadable_targets.write().await.insert(bucket.to_string());
    }

    pub async fn delete(&self, bucket: &str) {
        let update_mutex = self.target_update_mutex(bucket).await;
        let _update_guard = update_mutex.lock().await;

        // Lock order: unreadable_targets, then targets_map, then
        // arn_remotes_map, then target_h_mutex, then ssec_passthrough_map
        // (always last; also taken standalone by the capability accessors).
        self.unreadable_targets.write().await.remove(bucket);

        let mut targets_map = self.targets_map.write().await;
        let mut arn_remotes_map = self.arn_remotes_map.write().await;
        let mut health_map = self.target_h_mutex.write().await;

        if let Some(targets) = targets_map.remove(bucket) {
            let mut ssec_map = self.ssec_passthrough_map.write().await;
            for target in targets {
                arn_remotes_map.remove(&target.arn);
                health_map.remove(&target.arn);
                ssec_map.remove(&target.arn);
                self.forget_version_identity_capability(&target.arn);
            }
        }
    }

    /// Cached version-identity verdict for a target ARN; `Unknown` until a
    /// replication write or a replication-check VersionFidelity probe judged
    /// it since the target was built.
    pub fn version_identity_capability(&self, arn: &str) -> VersionIdentityCapability {
        self.version_identity_map
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(arn)
            .copied()
            .unwrap_or_default()
    }

    /// Record a version-identity verdict for a target ARN. Written by the
    /// replication worker after every PutObject / CompleteMultipartUpload
    /// response and by the replication-check VersionFidelity phase.
    pub fn record_version_identity_capability(&self, arn: &str, capability: VersionIdentityCapability) {
        self.version_identity_map
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(arn.to_string(), capability);
    }

    fn forget_version_identity_capability(&self, arn: &str) {
        self.version_identity_map
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(arn);
    }

    /// Cached SSE-C passthrough capability for a target ARN, plus whether the
    /// verdict is older than [`SSEC_PASSTHROUGH_CAPABILITY_TTL`]. `(Unknown,
    /// false)` when no verdict has been recorded since the target was built.
    /// Staleness is computed here so the gate policy stays a pure function.
    pub async fn ssec_passthrough_capability(&self, arn: &str) -> (SsecPassthroughCapability, bool) {
        match self.ssec_passthrough_map.read().await.get(arn) {
            Some(record) => (record.capability, record.recorded_at.elapsed() >= SSEC_PASSTHROUGH_CAPABILITY_TTL),
            None => (SsecPassthroughCapability::Unknown, false),
        }
    }

    /// Record an audited SSE-C passthrough verdict for a target ARN. Written by
    /// the replication worker's HEAD-back audit and by the replication-check
    /// SsecPassthrough probe phase.
    pub async fn record_ssec_passthrough_capability(&self, arn: &str, capability: SsecPassthroughCapability) {
        self.ssec_passthrough_map.write().await.insert(
            arn.to_string(),
            SsecPassthroughRecord {
                capability,
                recorded_at: Instant::now(),
            },
        );
    }

    /// Test hook: age an existing verdict so TTL expiry is observable without
    /// waiting out the real window.
    #[cfg(test)]
    pub(crate) async fn backdate_ssec_passthrough_capability(&self, arn: &str, age: Duration) {
        let backdated = Instant::now()
            .checked_sub(age)
            .expect("system uptime must exceed the backdate age");
        if let Some(record) = self.ssec_passthrough_map.write().await.get_mut(arn) {
            record.recorded_at = backdated;
        }
    }

    pub async fn set_target(
        &self,
        bucket: &str,
        target: &BucketTarget,
        update: bool,
    ) -> Result<BucketTargets, BucketTargetError> {
        self.validate_target(bucket, target).await?;

        let mut bucket_targets = self.targets_base_for_write(bucket).await?;

        Self::upsert_target_entry(&mut bucket_targets.targets, target, update)?;

        Ok(bucket_targets)
    }

    /// Ordinary writes must not turn an unreadable cached snapshot into an
    /// empty configuration. Explicit repair belongs to the metadata transaction
    /// that can inspect the current persisted state.
    async fn targets_base_for_write(&self, bucket: &str) -> Result<BucketTargets, BucketTargetError> {
        match self.list_bucket_targets(bucket).await {
            Ok(targets) => Ok(targets),
            Err(BucketTargetError::BucketRemoteTargetNotFound { .. }) => Ok(BucketTargets::default()),
            Err(err) => Err(err),
        }
    }

    pub async fn validate_target(&self, bucket: &str, target: &BucketTarget) -> Result<(), BucketTargetError> {
        if !target.target_type.is_valid() {
            return Err(BucketTargetError::BucketRemoteArnTypeInvalid {
                bucket: bucket.to_string(),
            });
        }

        let target_client = self.get_remote_target_client_internal(target).await?;

        // Validate target credentials
        if !self.validate_target_credentials(target).await? {
            return Err(BucketTargetError::BucketRemoteTargetNotFound {
                bucket: target.target_bucket.clone(),
            });
        }

        match target_client.bucket_exists(&target.target_bucket).await {
            Ok(false) => {
                return Err(BucketTargetError::BucketRemoteTargetNotFound {
                    bucket: target.target_bucket.clone(),
                });
            }
            Err(e) => {
                return Err(BucketTargetError::RemoteTargetConnectionErr {
                    bucket: target.target_bucket.clone(),
                    access_key: target.credentials.as_ref().map(|c| c.access_key.clone()).unwrap_or_default(),
                    error: e.to_string(),
                });
            }
            Ok(true) => {}
        }

        if target.target_type == BucketTargetType::ReplicationService {
            if !BucketVersioningSys::enabled(bucket).await {
                return Err(BucketTargetError::BucketReplicationSourceNotVersioned {
                    bucket: bucket.to_string(),
                });
            }

            let versioning = target_client
                .get_bucket_versioning(&target.target_bucket)
                .await
                .map_err(|e| BucketTargetError::RemoteTargetConnectionErr {
                    bucket: target.target_bucket.clone(),
                    access_key: target.credentials.as_ref().map(|c| c.access_key.clone()).unwrap_or_default(),
                    error: e.to_string(),
                })?;

            if !replication_target_versioning_enabled(versioning.as_ref()) {
                return Err(BucketTargetError::BucketRemoteTargetNotVersioned {
                    bucket: target.target_bucket.to_string(),
                });
            }
        }

        Ok(())
    }

    /// Merge a validated target into a caller-owned snapshot. The caller must
    /// protect that snapshot through persistence.
    pub fn upsert_target_entry(
        bucket_targets: &mut Vec<BucketTarget>,
        target: &BucketTarget,
        update: bool,
    ) -> Result<(), BucketTargetError> {
        let mut found = false;

        for (idx, existing_target) in bucket_targets.iter().enumerate() {
            if existing_target.target_type.to_string() == target.target_type.to_string() {
                if existing_target.arn == target.arn {
                    if !update {
                        return Err(BucketTargetError::BucketRemoteAlreadyExists {
                            bucket: existing_target.target_bucket.clone(),
                        });
                    }
                    bucket_targets[idx] = target.clone();
                    found = true;
                    break;
                }
                if existing_target.endpoint == target.endpoint {
                    return Err(BucketTargetError::BucketRemoteAlreadyExists {
                        bucket: existing_target.target_bucket.clone(),
                    });
                }
            }
        }

        if !found && !update {
            bucket_targets.push(target.clone());
        }

        Ok(())
    }

    pub async fn remove_target(&self, bucket: &str, arn_str: &str) -> Result<BucketTargets, BucketTargetError> {
        if arn_str.is_empty() {
            return Err(BucketTargetError::BucketRemoteArnInvalid {
                bucket: bucket.to_string(),
            });
        }

        let arn = ARN::from_str(arn_str).map_err(|_e| BucketTargetError::BucketRemoteArnInvalid {
            bucket: bucket.to_string(),
        })?;

        if arn.arn_type == BucketTargetType::ReplicationService
            && let Ok((config, _)) = get_replication_config(bucket).await
            && ReplicationTargetConfigBridge::target_is_used_by_rules(&config, arn_str)
        {
            let arn_remotes_map = self.arn_remotes_map.read().await;
            if arn_remotes_map.get(arn_str).is_some() {
                return Err(BucketTargetError::BucketRemoteRemoveDisallowed {
                    bucket: bucket.to_string(),
                });
            }
        }

        let targets = self.list_bucket_targets(bucket).await?;
        let new_targets: Vec<BucketTarget> = targets.targets.iter().filter(|t| t.arn != arn_str).cloned().collect();

        if new_targets.len() == targets.targets.len() {
            return Err(BucketTargetError::BucketRemoteTargetNotFound {
                bucket: bucket.to_string(),
            });
        }

        Ok(BucketTargets { targets: new_targets })
    }

    async fn mark_refresh_attempt(&self, arn: &str) {
        // Rate-limit a failed config fetch as well as a failed client build.
        // A successful rebuild replaces this timestamp during publication.
        self.arn_remotes_map
            .write()
            .await
            .entry(arn.to_string())
            .or_default()
            .last_refresh = OffsetDateTime::now_utc();
    }

    pub async fn mark_refresh_in_progress(&self, bucket: &str, arn: &str) {
        let mut arn_errs = self.arn_errs_map.write().await;
        let err = arn_errs.entry(arn.to_string()).or_insert_with(|| ArnErrs {
            count: 1,
            bucket: bucket.to_string(),
            ..Default::default()
        });
        err.update_in_progress = true;
        err.bucket = bucket.to_string();
    }

    pub async fn mark_refresh_done(&self, bucket: &str, arn: &str) {
        let mut arn_errs = self.arn_errs_map.write().await;
        if let Some(err) = arn_errs.get_mut(arn) {
            err.update_in_progress = false;
            err.bucket = bucket.to_string();
        }
    }

    pub async fn is_reloading_target(&self, _bucket: &str, arn: &str) -> bool {
        self.arn_errs_map
            .read()
            .await
            .get(arn)
            .is_some_and(|err| err.update_in_progress)
    }

    pub async fn inc_arn_errs(&self, bucket: &str, arn: &str) {
        let mut arn_errs = self.arn_errs_map.write().await;
        let err = arn_errs.entry(arn.to_string()).or_insert_with(|| ArnErrs {
            bucket: bucket.to_string(),
            ..Default::default()
        });
        err.count += 1;
        err.bucket = bucket.to_string();
    }

    pub async fn get_remote_target_client(&self, bucket: &str, arn: &str) -> Option<Arc<TargetClient>> {
        let (cli, last_refresh) = {
            self.arn_remotes_map
                .read()
                .await
                .get(arn)
                .map(|target| (target.client.clone(), Some(target.last_refresh)))
                .unwrap_or((None, None))
        };

        let credentials_expired = cli
            .as_ref()
            .is_some_and(|client| client.credentials_expired_at(jiff::Timestamp::now()));
        if let Some(cli) = cli
            && !credentials_expired
        {
            return Some(cli);
        }

        if let Some(last_refresh) = last_refresh {
            let now = OffsetDateTime::now_utc();
            if now - last_refresh < Duration::from_secs(60 * 5) {
                return None;
            }
        }

        // The existing per-bucket publication lock is also the reload claim:
        // try-locking keeps the request path non-blocking, is cancellation-safe,
        // and prevents a stale reload from publishing after a credential update.
        let update_mutex = self.target_update_mutex(bucket).await;
        let Ok(update_guard) = update_mutex.try_lock() else {
            return None;
        };
        self.mark_refresh_attempt(arn).await;

        match get_bucket_targets_config(bucket).await {
            Ok(bucket_targets) => {
                self.update_all_targets_locked(bucket, Some(&bucket_targets)).await;
            }
            Err(e) => {
                error!("get bucket targets config error:{}", e);
            }
        };
        drop(update_guard);

        let cli = self
            .arn_remotes_map
            .read()
            .await
            .get(arn)
            .and_then(|target| target.client.clone());
        if let Some(cli) = cli
            && !cli.credentials_expired_at(jiff::Timestamp::now())
        {
            return Some(cli);
        }

        self.inc_arn_errs(bucket, arn).await;
        None
    }

    pub async fn get_remote_target_client_internal(&self, target: &BucketTarget) -> Result<TargetClient, BucketTargetError> {
        #[cfg(test)]
        {
            let probe = self.target_client_build_probe.lock().await.clone();
            if let Some(probe) = probe
                && probe.arn == target.arn
            {
                probe.started.add_permits(1);
                probe
                    .release
                    .acquire()
                    .await
                    .expect("test probe semaphore should remain open")
                    .forget();
            }
        }

        let Some(credentials) = &target.credentials else {
            return Err(BucketTargetError::BucketRemoteTargetNotFound {
                bucket: target.target_bucket.clone(),
            });
        };

        let spec = RemoteS3EndpointSpec::from(target);
        let client = build_remote_s3_client(&spec)
            .await
            .map_err(|err| BucketTargetError::RemoteTargetConnectionErr {
                bucket: target.target_bucket.clone(),
                access_key: credentials.access_key.clone(),
                error: err.to_string(),
            })?;

        Ok(TargetClient {
            endpoint: spec.endpoint_url(),
            credentials: target.credentials.clone(),
            bucket: target.target_bucket.clone(),
            storage_class: target.storage_class.clone(),
            disable_proxy: target.disable_proxy,
            arn: target.arn.clone(),
            reset_id: target.reset_id.clone(),
            secure: target.secure,
            health_check_duration: target.health_check_duration,
            replicate_sync: target.replication_sync,
            client: Arc::new(client),
        })
    }

    async fn validate_target_credentials(&self, _target: &BucketTarget) -> Result<bool, BucketTargetError> {
        // In a real implementation, you would validate the credentials
        // by making actual API calls to the target
        Ok(true)
    }

    fn update_bandwidth_limit(&self, bucket: &str, arn: &str, limit: i64) {
        if let Some(bucket_monitor) = runtime_sources::bucket_monitor() {
            if limit == 0 {
                bucket_monitor.delete_bucket_throttle(bucket, arn);
                return;
            }
            bucket_monitor.set_bandwidth_limit(bucket, arn, limit);
        } else {
            error!(
                "Global bucket monitor uninitialized; skipping bandwidth limit update for bucket '{}' and ARN '{}'",
                bucket, arn
            );
        }
    }

    pub async fn get_remote_target_client_by_arn(&self, _bucket: &str, arn: &str) -> Option<Arc<TargetClient>> {
        let arn_remotes_map = self.arn_remotes_map.read().await;
        arn_remotes_map.get(arn).and_then(|target| target.client.clone())
    }

    pub async fn get_remote_bucket_target_by_arn(&self, bucket: &str, arn: &str) -> Option<BucketTarget> {
        let targets_map = self.targets_map.read().await;
        targets_map
            .get(bucket)
            .and_then(|targets| targets.iter().find(|t| t.arn == arn).cloned())
    }

    pub async fn update_all_targets(&self, bucket: &str, targets: Option<&BucketTargets>) {
        let update_mutex = self.target_update_mutex(bucket).await;
        let _update_guard = update_mutex.lock().await;

        self.update_all_targets_locked(bucket, targets).await;
    }

    /// Builds and publishes one bucket snapshot while its update mutex is held.
    /// Keeping persisted-config reads under the same mutex prevents a stale
    /// reload from overwriting a concurrent credential rotation.
    async fn update_all_targets_locked(&self, bucket: &str, targets: Option<&BucketTargets>) {
        // Reaching here means the persisted configuration decoded, so the
        // unreadable marker (if any) is stale. Cleared before the maps below
        // so `unreadable_targets` stays the outermost of this module's locks.
        self.unreadable_targets.write().await.remove(bucket);

        let mut clients = Vec::new();
        if let Some(new_targets) = targets {
            for target in &new_targets.targets {
                clients.push((target, self.get_remote_target_client_internal(target).await.map(Arc::new)));
            }
        }

        // Lock order: unreadable_targets (above), then targets_map, then
        // arn_remotes_map, then target_h_mutex, then ssec_passthrough_map
        // (always last; also taken standalone by the capability accessors).
        let mut targets_map = self.targets_map.write().await;
        let mut arn_remotes_map = self.arn_remotes_map.write().await;
        let mut health_map = self.target_h_mutex.write().await;
        // Remove existing targets
        if let Some(existing_targets) = targets_map.remove(bucket) {
            let mut ssec_map = self.ssec_passthrough_map.write().await;
            let unchanged_service: HashMap<&str, &BucketTarget> = targets
                .map(|new_targets| {
                    new_targets
                        .targets
                        .iter()
                        .map(|target| (target.arn.as_str(), target))
                        .collect()
                })
                .unwrap_or_default();
            for target in existing_targets {
                arn_remotes_map.remove(&target.arn);
                health_map.remove(&target.arn);
                // A rebuilt/edited target may point at a different service:
                // the SSE-C passthrough verdict must be re-audited from Unknown.
                ssec_map.remove(&target.arn);
                // The version-identity verdict survives an edit that keeps the
                // same remote service (a resync start or a bandwidth change
                // rewrites the entry in place): forgetting it there would make
                // the very resync that follows re-drive every object as a
                // duplicate on a target that mints its own version ids.
                if unchanged_service
                    .get(target.arn.as_str())
                    .is_none_or(|edited| !same_replication_service(edited, &target))
                {
                    self.forget_version_identity_capability(&target.arn);
                }
                self.update_bandwidth_limit(bucket, &target.arn, 0);
            }
        }

        // Add new targets
        if let Some(new_targets) = targets
            && !new_targets.is_empty()
        {
            for (target, client) in clients {
                // Keep a timestamped placeholder for configured targets whose
                // client cannot be built. Replication records these attempts as
                // failed, while the placeholder prevents every object from
                // triggering another metadata reload/client build for five minutes.
                arn_remotes_map.insert(
                    target.arn.clone(),
                    ArnTarget {
                        client: None,
                        last_refresh: OffsetDateTime::now_utc(),
                    },
                );
                match client {
                    Ok(client) => {
                        arn_remotes_map.insert(
                            target.arn.clone(),
                            ArnTarget {
                                client: Some(client.clone()),
                                last_refresh: OffsetDateTime::now_utc(),
                            },
                        );
                        health_map.insert(client.arn.clone(), target_health(&client));
                        self.update_bandwidth_limit(bucket, &target.arn, target.bandwidth_limit);
                    }
                    Err(err) => warn!(
                        bucket = %bucket,
                        arn = %target.arn,
                        endpoint = %target.endpoint,
                        error = %err,
                        "replication target client unavailable; objects for this ARN will not replicate"
                    ),
                }
            }
            targets_map.insert(bucket.to_string(), new_targets.targets.clone());
        }
    }

    pub async fn set(&self, bucket: &str, meta: &BucketMetadata) {
        if meta.bucket_targets_unreadable() {
            self.mark_targets_unreadable(bucket).await;
            return;
        }

        let Some(config) = &meta.bucket_target_config else {
            return;
        };

        if config.is_empty() {
            return;
        }

        self.update_all_targets(bucket, Some(config)).await;
    }

    // getRemoteARN gets existing ARN for an endpoint or generates a new one.
    pub async fn get_remote_arn(&self, bucket: &str, target: Option<&BucketTarget>, depl_id: &str) -> (String, bool) {
        let Some(target) = target else {
            return (String::new(), false);
        };

        let targets_map = self.targets_map.read().await;
        let targets = targets_map.get(bucket).map(Vec::as_slice).unwrap_or_default();
        Self::remote_arn_for_targets(targets, target, depl_id)
    }

    /// Resolve create idempotency against the snapshot the caller will persist.
    pub fn remote_arn_for_targets(targets: &[BucketTarget], target: &BucketTarget, depl_id: &str) -> (String, bool) {
        for tgt in targets {
            if tgt.target_type == target.target_type
                && tgt.target_bucket == target.target_bucket
                && target.endpoint == tgt.endpoint
                && tgt
                    .credentials
                    .as_ref()
                    .map(|c| {
                        let default_creds = Credentials::default();
                        c.access_key == target.credentials.as_ref().unwrap_or(&default_creds).access_key
                    })
                    .unwrap_or(false)
            {
                return (tgt.arn.clone(), true);
            }
        }

        if !target.target_type.is_valid() {
            return (String::new(), false);
        }
        let arn = generate_arn(target, depl_id);
        (arn, false)
    }
}

// generate ARN that is unique to this target type
fn generate_arn(t: &BucketTarget, depl_id: &str) -> String {
    let uuid = if depl_id.is_empty() {
        Uuid::new_v4().to_string()
    } else {
        depl_id.to_string()
    };
    let arn = ARN {
        arn_type: t.target_type.clone(),
        id: uuid,
        region: t.region.clone(),
        bucket: t.target_bucket.clone(),
    };
    arn.to_string()
}

pub struct RemoveObjectOptions {
    pub force_delete: bool,
    pub governance_bypass: bool,
    pub replication_delete_marker: bool,
    pub replication_mtime: Option<OffsetDateTime>,
    pub replication_status: ReplicationStatusType,
    pub replication_request: bool,
    pub replication_validity_check: bool,
}

fn build_remove_object_headers(version_id: Option<&str>, opts: &RemoveObjectOptions) -> HeaderMap {
    let mut headers = HeaderMap::new();
    if opts.force_delete {
        insert_header(&mut headers, SUFFIX_FORCE_DELETE, "true");
    }
    if opts.governance_bypass {
        headers.insert(AMZ_OBJECT_LOCK_BYPASS_GOVERNANCE, "true".parse().unwrap());
    }

    if opts.replication_delete_marker {
        insert_header(&mut headers, SUFFIX_SOURCE_DELETEMARKER, "true");
    }

    if let Some(t) = opts.replication_mtime {
        insert_header(&mut headers, SUFFIX_SOURCE_MTIME, t.format(&Rfc3339).unwrap_or_default());
    }

    if !opts.replication_status.is_empty() {
        headers.insert(AMZ_BUCKET_REPLICATION_STATUS, opts.replication_status.as_str().parse().unwrap());
    }

    if let Some(version_id) = version_id {
        insert_header(&mut headers, SUFFIX_SOURCE_VERSION_ID, version_id);
    }

    if opts.replication_request {
        insert_header(&mut headers, SUFFIX_SOURCE_REPLICATION_REQUEST, "true");
    }
    if opts.replication_validity_check {
        insert_header(&mut headers, SUFFIX_SOURCE_REPLICATION_CHECK, "true");
    }

    headers
}

/// Resolve the S3 `versionId` query parameter for a target DELETE.
///
/// A replication delete omits the `versionId` query param ONLY when it is
/// propagating a delete-marker CREATION (`replication_delete_marker`), so the
/// target mints its own marker. A version purge / delete-marker purge / force
/// delete must address the exact version — otherwise a generic (non-MinIO /
/// non-RustFS) S3 target ignores the internal `x-*-source-version-id` header
/// and silently creates a delete marker instead of removing the version, while
/// the source stamps `VersionPurgeStatus=Complete` (backlog#799 B8 / #857).
/// Non-replication callers always pass the version through unchanged.
fn resolve_delete_api_version_id(version_id: Option<String>, opts: &RemoveObjectOptions) -> Option<String> {
    if opts.replication_request && opts.replication_delete_marker {
        None
    } else {
        version_id
    }
}

/// Resolve the S3 `versionId` query parameter for a replication PUT /
/// CreateMultipartUpload against a remote target.
///
/// MinIO reads the replicated version only from the query string
/// (`putOptsFromReq`); the internal `x-*-source-version-id` headers do not
/// exist there, so without the query a MinIO target mints fresh version ids
/// and the deployments drift apart. RustFS represents the null version
/// internally as the nil UUID while the S3 API addresses it as the literal
/// "null" (the delete path already maps it via `target_delete_version_id`),
/// and an empty id means the source object carries no version: send no query
/// so an unversioned target stays valid.
fn resolve_put_api_version_id(source_version_id: &str) -> Option<&str> {
    if source_version_id.is_empty() {
        None
    } else if Uuid::parse_str(source_version_id).is_ok_and(|uuid| uuid.is_nil()) {
        Some(rustfs_filemeta::NULL_VERSION_ID)
    } else {
        Some(source_version_id)
    }
}

/// Resolve the S3 `versionId` for a proxied read against a remote target.
/// RustFS represents the null version internally as the nil UUID while the S3
/// API addresses it as the literal "null" (same mapping as
/// [`resolve_put_api_version_id`]); empty means "no version requested".
pub(crate) fn resolve_read_api_version_id(version_id: Option<String>) -> Option<String> {
    let version_id = version_id?;
    let trimmed = version_id.trim();
    if trimmed.is_empty() {
        None
    } else if Uuid::parse_str(trimmed).is_ok_and(|uuid| uuid.is_nil()) {
        Some(rustfs_filemeta::NULL_VERSION_ID.to_string())
    } else {
        Some(trimmed.to_string())
    }
}

/// Outbound header set for a proxied read: the caller-provided passthrough
/// headers (client SSE-C key family, conditional headers) plus the anti-loop
/// `source-proxy-request` marker in both the x-rustfs- and x-minio- prefixes
/// (a MinIO target only understands the latter). Never adds
/// `source-replication-check`: that exemption channel belongs exclusively to
/// the replication worker's HEAD.
fn proxy_outbound_headers(mut extra_headers: HeaderMap) -> HeaderMap {
    insert_header(&mut extra_headers, SUFFIX_SOURCE_PROXY_REQUEST, "true");
    extra_headers
}

/// Copy `headers` onto an SDK request inside `customize().map_request` (runs
/// before signing, so the headers join the SigV4 canonical request).
fn apply_extra_headers(mut req: HttpRequest, headers: &HeaderMap) -> Result<HttpRequest, std::convert::Infallible> {
    for (k, v) in headers.iter() {
        req.headers_mut()
            .insert(k.as_str().to_string(), v.to_str().unwrap_or("").to_string());
    }
    Ok(req)
}

/// Append `versionId=<id>` to an already-built request URI. aws-sdk-s3's
/// `PutObjectInput` / `CreateMultipartUploadInput` expose no version id
/// member, so the query is spliced in via `map_request`, which runs at
/// `modify_before_signing`: the parameter becomes part of the SigV4 canonical
/// request.
pub fn append_version_id_query(uri: &str, version_id: &str) -> String {
    let separator = if uri.contains('?') { '&' } else { '?' };
    format!("{uri}{separator}versionId={}", urlencoding::encode(version_id))
}

#[derive(Debug, Clone)]
pub struct AdvancedPutOptions {
    pub source_version_id: String,
    pub source_etag: String,
    pub replication_status: ReplicationStatusType,
    pub source_mtime: OffsetDateTime,
    pub replication_request: bool,
    pub retention_timestamp: OffsetDateTime,
    pub tagging_timestamp: OffsetDateTime,
    pub legalhold_timestamp: OffsetDateTime,
    pub replication_validity_check: bool,
}

impl Default for AdvancedPutOptions {
    fn default() -> Self {
        Self {
            source_version_id: "".to_string(),
            source_etag: "".to_string(),
            replication_status: ReplicationStatusType::Pending,
            source_mtime: OffsetDateTime::now_utc(),
            replication_request: false,
            // UNIX_EPOCH means "never modified": header() must not emit a
            // timestamp header for it, otherwise a receiver would treat an
            // unset category as a modification made right now.
            retention_timestamp: OffsetDateTime::UNIX_EPOCH,
            tagging_timestamp: OffsetDateTime::UNIX_EPOCH,
            legalhold_timestamp: OffsetDateTime::UNIX_EPOCH,
            replication_validity_check: false,
        }
    }
}

/// Decide how a replication PUT satisfies the Object Lock integrity rule from
/// the headers it is about to send (the pure decision lives in the replication crate, re-exported through the replication boundary).
fn object_lock_put_integrity_for(headers: &HeaderMap, opts: &PutObjectOptions) -> ObjectLockIntegrity {
    let lock_params = opts.mode.is_some() || opts.retain_until_date.unix_timestamp() != 0 || opts.legalhold.is_some();
    let has_integrity_header = headers.keys().any(|name| {
        let name = name.as_str();
        name.starts_with("x-amz-checksum-") || name == "x-amz-sdk-checksum-algorithm" || name == "content-md5"
    });
    let plaintext_end_to_end = !headers.contains_key("x-amz-server-side-encryption")
        && !headers.contains_key("x-amz-server-side-encryption-customer-algorithm")
        && !rustfs_utils::http::has_ssec_transport_headers(headers);
    object_lock_put_integrity(
        lock_params,
        has_integrity_header,
        plaintext_end_to_end,
        Some(opts.internal.source_etag.as_str()),
    )
}

/// The subset of the target's PutObject response replication audits.
#[derive(Debug, Clone)]
pub struct RemotePutObjectResponse {
    /// Version id the target assigned (`x-amz-version-id`).
    pub version_id: Option<String>,
    /// ETag of what the target stored; `None` when the target withheld it or
    /// when its encryption mode (SSE-KMS / SSE-C) makes it incomparable to
    /// the source ETag. `None` is therefore "not decidable", never evidence.
    pub etag: Option<String>,
}

#[derive(Clone)]
pub struct PutObjectOptions {
    pub user_metadata: HashMap<String, String>,
    pub user_tags: HashMap<String, String>,
    //pub progress: ReaderImpl,
    pub content_type: String,
    pub content_encoding: String,
    pub content_disposition: String,
    pub content_language: String,
    pub cache_control: String,
    pub expires: OffsetDateTime,
    pub mode: Option<ObjectLockRetentionMode>,
    pub retain_until_date: OffsetDateTime,
    //pub server_side_encryption: encrypt::ServerSide,
    pub num_threads: u64,
    pub storage_class: String,
    pub website_redirect_location: String,
    pub part_size: u64,
    pub legalhold: Option<ObjectLockLegalHoldStatus>,
    pub send_content_md5: bool,
    pub disable_content_sha256: bool,
    pub disable_multipart: bool,
    pub auto_checksum: Option<ChecksumMode>,
    pub checksum: Option<ChecksumMode>,
    pub concurrent_stream_parts: bool,
    pub internal: AdvancedPutOptions,
    pub custom_header: HeaderMap,
}

impl Default for PutObjectOptions {
    fn default() -> Self {
        Self {
            user_metadata: HashMap::new(),
            user_tags: HashMap::new(),
            //progress: ReaderImpl::Body(Bytes::new()),
            content_type: "".to_string(),
            content_encoding: "".to_string(),
            content_disposition: "".to_string(),
            content_language: "".to_string(),
            cache_control: "".to_string(),
            expires: OffsetDateTime::UNIX_EPOCH,
            mode: None,
            retain_until_date: OffsetDateTime::UNIX_EPOCH,
            //server_side_encryption: encrypt.ServerSide::default(),
            num_threads: 0,
            storage_class: "".to_string(),
            website_redirect_location: "".to_string(),
            part_size: 0,
            legalhold: None,
            send_content_md5: false,
            disable_content_sha256: false,
            disable_multipart: false,
            auto_checksum: None,
            checksum: None,
            concurrent_stream_parts: false,
            internal: AdvancedPutOptions::default(),
            custom_header: HeaderMap::new(),
        }
    }
}

impl PutObjectOptions {
    #[allow(dead_code, reason = "MinIO-parity surface with no caller in this port (backlog#1823)")]
    fn set_match_etag(&mut self, etag: &str) {
        if etag == "*" {
            self.custom_header
                .insert("If-Match", HeaderValue::from_str("*").expect("err"));
        } else {
            self.custom_header
                .insert("If-Match", HeaderValue::from_str(&format!("\"{etag}\"")).expect("err"));
        }
    }

    #[allow(dead_code, reason = "MinIO-parity surface with no caller in this port (backlog#1823)")]
    fn set_match_etag_except(&mut self, etag: &str) {
        if etag == "*" {
            self.custom_header
                .insert("If-None-Match", HeaderValue::from_str("*").expect("err"));
        } else {
            self.custom_header
                .insert("If-None-Match", HeaderValue::from_str(&format!("\"{etag}\"")).expect("err"));
        }
    }

    /// Insert `value` as header `name`, skipping values that are not valid
    /// HTTP header values (with a warning) instead of panicking mid-replication.
    fn insert_checked(header: &mut HeaderMap, name: &'static str, value: &str) {
        match HeaderValue::from_str(value) {
            Ok(v) => {
                header.insert(name, v);
            }
            Err(_) => warn!("skipping header {} with invalid value", name),
        }
    }

    pub fn header(&self) -> HeaderMap {
        let mut header = HeaderMap::new();

        let mut content_type = self.content_type.clone();
        if content_type.is_empty() {
            content_type = "application/octet-stream".to_string();
        }
        match HeaderValue::from_str(&content_type) {
            Ok(v) => {
                header.insert("Content-Type", v);
            }
            Err(_) => {
                warn!("invalid Content-Type header value, falling back to application/octet-stream");
                header.insert("Content-Type", HeaderValue::from_static("application/octet-stream"));
            }
        }

        if !self.content_encoding.is_empty() {
            Self::insert_checked(&mut header, "Content-Encoding", &self.content_encoding);
        }
        if !self.content_disposition.is_empty() {
            Self::insert_checked(&mut header, "Content-Disposition", &self.content_disposition);
        }
        if !self.content_language.is_empty() {
            Self::insert_checked(&mut header, "Content-Language", &self.content_language);
        }
        if !self.cache_control.is_empty() {
            Self::insert_checked(&mut header, "Cache-Control", &self.cache_control);
        }

        if self.expires.unix_timestamp() != 0 {
            match self.expires.format(&Rfc3339) {
                Ok(expires) => Self::insert_checked(&mut header, "Expires", &expires),
                Err(err) => warn!("skipping Expires header, format failed: {}", err),
            }
        }

        if let Some(mode) = &self.mode {
            Self::insert_checked(&mut header, AMZ_OBJECT_LOCK_MODE, mode.as_str());
        }

        if self.retain_until_date.unix_timestamp() != 0 {
            match self.retain_until_date.format(&Rfc3339) {
                Ok(retain_until) => Self::insert_checked(&mut header, AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE, &retain_until),
                Err(err) => warn!("skipping object-lock retain-until-date header, format failed: {}", err),
            }
        }

        if let Some(legalhold) = &self.legalhold {
            Self::insert_checked(&mut header, AMZ_OBJECT_LOCK_LEGAL_HOLD, legalhold.as_str());
        }

        if !self.storage_class.is_empty() {
            Self::insert_checked(&mut header, AMZ_STORAGE_CLASS, &self.storage_class);
        }

        if !self.website_redirect_location.is_empty() {
            Self::insert_checked(&mut header, AMZ_WEBSITE_REDIRECT_LOCATION, &self.website_redirect_location);
        }

        if !self.internal.replication_status.as_str().is_empty() {
            Self::insert_checked(&mut header, AMZ_BUCKET_REPLICATION_STATUS, self.internal.replication_status.as_str());
        }

        // MinIO PutObjectOptions.Header parity: object tags travel on the
        // `x-amz-tagging` header (form-urlencoded). `replication_put_object_options`
        // fills `user_tags` from the source version; without this header the
        // whole-object transport delivered a tagless replica, so tag edits
        // never reached the peer and the receiver-side LWW comparison
        // (rustfs/backlog#1953) had nothing to judge.
        if !self.user_tags.is_empty() {
            let mut tags: Vec<(&String, &String)> = self.user_tags.iter().collect();
            tags.sort();
            let mut encoded = url::form_urlencoded::Serializer::new(String::new());
            for (key, value) in tags {
                encoded.append_pair(key, value);
            }
            Self::insert_checked(&mut header, AMZ_OBJECT_TAGGING_LOWER, &encoded.finish());
        }

        for (k, v) in &self.user_metadata {
            let Ok(header_value) = HeaderValue::from_str(v) else {
                warn!("skipping user metadata header with invalid value: {}", k);
                continue;
            };
            if is_amz_header(k) || is_standard_header(k) || is_storageclass_header(k) || is_rustfs_header(k) || is_minio_header(k)
            {
                if let Ok(header_name) = HeaderName::from_bytes(k.as_bytes()) {
                    header.insert(header_name, header_value);
                }
            } else if let Ok(header_name) = HeaderName::from_bytes(format!("x-amz-meta-{k}").as_bytes()) {
                header.insert(header_name, header_value);
            }
        }

        for (k, v) in self.custom_header.iter() {
            header.insert(k.clone(), v.clone());
        }

        if !self.internal.source_version_id.is_empty() {
            insert_header(&mut header, SUFFIX_SOURCE_VERSION_ID, &self.internal.source_version_id);
        }
        if !self.internal.source_etag.is_empty() {
            insert_header(&mut header, SUFFIX_SOURCE_ETAG, &self.internal.source_etag);
        }
        if self.internal.source_mtime.unix_timestamp() != 0 {
            insert_header(
                &mut header,
                SUFFIX_SOURCE_MTIME,
                self.internal.source_mtime.format(&Rfc3339).unwrap_or_default(),
            );
        }

        for (suffix, timestamp) in [
            (SUFFIX_SOURCE_REPLICATION_TAGGING_TIMESTAMP, self.internal.tagging_timestamp),
            (SUFFIX_SOURCE_REPLICATION_RETENTION_TIMESTAMP, self.internal.retention_timestamp),
            (SUFFIX_SOURCE_REPLICATION_LEGALHOLD_TIMESTAMP, self.internal.legalhold_timestamp),
        ] {
            if timestamp.unix_timestamp() != 0 {
                insert_header(&mut header, suffix, timestamp.format(&Rfc3339).unwrap_or_default());
            }
        }

        if self.internal.replication_request {
            insert_header(&mut header, SUFFIX_SOURCE_REPLICATION_REQUEST, "true");
        }

        header
    }

    #[allow(dead_code, reason = "MinIO-parity surface with no caller in this port (backlog#1823)")]
    fn validate(&self, _c: Arc<TargetClient>) -> Result<(), std::io::Error> {
        //if self.checksum.is_set() {
        /*if !self.trailing_header_support {
            return Err(Error::from(err_invalid_argument("Checksum requires Client with TrailingHeaders enabled")));
        }*/
        /*else if self.override_signer_type == SignatureType::SignatureV2 {
            return Err(Error::from(err_invalid_argument("Checksum cannot be used with v2 signatures")));
        }*/
        //}

        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct PutObjectPartOptions {
    pub md5_base64: String,
    pub sha256_hex: String,
    //pub sse: encrypt.ServerSide,
    pub custom_header: HeaderMap,
    pub trailer: HeaderMap,
    pub disable_content_sha256: bool,
}

#[derive(Debug)]
pub struct S3ClientError {
    pub error: String,
    pub status_code: Option<StatusCode>,
    pub code: Option<String>,
    pub message: Option<String>,
}
impl S3ClientError {
    pub fn new(value: impl Into<String>) -> Self {
        Self::with_metadata(value, None, None, None)
    }

    pub fn with_metadata(
        error: impl Into<String>,
        status_code: Option<StatusCode>,
        code: Option<String>,
        message: Option<String>,
    ) -> Self {
        S3ClientError {
            error: error.into(),
            status_code,
            code,
            message,
        }
    }

    pub fn add_message(self, message: impl Into<String>) -> Self {
        S3ClientError {
            error: format!("{}: {}", message.into(), self.error),
            status_code: self.status_code,
            code: self.code,
            message: self.message,
        }
    }
}

impl<T: aws_sdk_s3::error::ProvideErrorMetadata> From<T> for S3ClientError {
    fn from(value: T) -> Self {
        let code = value.code().map(String::from);
        let message = value.message().map(String::from);
        let error = match (code.as_deref(), message.as_deref()) {
            (Some(code), Some(message)) => format!("{code}: {message}"),
            (Some(code), None) => code.to_string(),
            (None, Some(message)) => message.to_string(),
            (None, None) => "unknown remote error".to_string(),
        };

        S3ClientError::with_metadata(error, None, code, message)
    }
}

impl std::error::Error for S3ClientError {}

impl std::fmt::Display for S3ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.error)
    }
}

#[derive(Debug)]
pub struct TargetClient {
    pub endpoint: String,
    pub credentials: Option<Credentials>,
    pub bucket: String,
    pub storage_class: String,
    pub disable_proxy: bool,
    pub arn: String,
    pub reset_id: String,
    pub secure: bool,
    pub health_check_duration: Duration,
    pub replicate_sync: bool,
    pub client: Arc<S3Client>,
}

impl TargetClient {
    fn credentials_expired_at(&self, now: jiff::Timestamp) -> bool {
        self.credentials
            .as_ref()
            .and_then(Credentials::effective_expiration)
            .is_some_and(|expiration| expiration <= now)
    }

    pub fn to_url(&self) -> Url {
        Url::parse(&self.endpoint).unwrap()
    }

    pub async fn bucket_exists(&self, bucket: &str) -> Result<bool, S3ClientError> {
        match self.client.head_bucket().bucket(bucket).send().await {
            Ok(_) => Ok(true),
            Err(e) => match e {
                SdkError::ServiceError(oe) => match oe.into_err() {
                    HeadBucketError::NotFound(_) => Ok(false),
                    other => {
                        warn!(
                            "failed to check bucket exists for bucket:{bucket} please check the bucket name and credentials, error:{:?}",
                            other
                        );
                        let message = other.meta().meta();
                        Err(S3ClientError::with_metadata(
                            format!(
                                "failed to check bucket exists for bucket:{bucket} please check the bucket name and credentials, error:{:?}",
                                message
                            ),
                            None,
                            message.code().map(ToOwned::to_owned),
                            message.message().map(ToOwned::to_owned),
                        ))
                    }
                },
                SdkError::DispatchFailure(e) => Err(S3ClientError::new(format!(
                    "failed to dispatch bucket exists for bucket:{bucket} error:{e:?}"
                ))),

                _ => Err(S3ClientError::new(format!(
                    "failed to check bucket exists for bucket:{bucket} error:{e:?}"
                ))),
            },
        }
    }

    pub async fn get_bucket_versioning(&self, bucket: &str) -> Result<Option<BucketVersioningStatus>, S3ClientError> {
        match self.client.get_bucket_versioning().bucket(bucket).send().await {
            Ok(res) => Ok(res.status),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn head_object(
        &self,
        bucket: &str,
        object: &str,
        version_id: Option<String>,
    ) -> Result<HeadObjectOutput, HeadObjectSdkError> {
        // Announce the replication check so a RustFS target returns SSE-C
        // object metadata (etag/size) without the customer key the replication
        // worker cannot hold; otherwise SSE-C replicas never converge on HEAD.
        let mut headers = HeaderMap::new();
        insert_header(&mut headers, SUFFIX_SOURCE_REPLICATION_CHECK, "true");
        // `source-proxy-request: false` (MinIO `ProxyHeaderSet` semantics):
        // the header's mere presence tells the receiver to answer LOCALLY
        // instead of proxying the miss back to us. Without it, a not-found on
        // the target gets read-proxied back to this source, echoes the source
        // object with an identical ETag, and the worker concludes the object
        // already converged — so it never actually replicates it.
        insert_header(&mut headers, SUFFIX_SOURCE_PROXY_REQUEST, "false");
        self.client
            .head_object()
            .bucket(bucket)
            .key(object)
            .set_version_id(version_id)
            .customize()
            .map_request(move |mut req| {
                for (k, v) in headers.clone().into_iter() {
                    if let Some(key_str) = k.map(|k| k.as_str().to_string()) {
                        let value_str = v.to_str().unwrap_or("").to_string();
                        req.headers_mut().insert(key_str, value_str);
                    }
                }
                Result::<_, std::convert::Infallible>::Ok(req)
            })
            .send()
            .await
            .map_err(Box::new)
    }

    /// Locate a replica by content identity on a target that mints its own
    /// version ids: page `ListObjectVersions` under the exact key and return
    /// the newest live version whose ETag matches `source_etag`. Delete
    /// markers and prefix siblings never match. Bounded to
    /// [`FIND_VERSION_BY_ETAG_MAX_PAGES`] pages so a key with a very deep
    /// history cannot turn one convergence check into an unbounded scan; a
    /// replica beyond that window reads as missing, which only costs a
    /// re-PUT (today's behaviour), never a lost object.
    pub async fn find_version_by_etag(
        &self,
        bucket: &str,
        object: &str,
        source_etag: &str,
    ) -> Result<Option<String>, Box<SdkError<aws_sdk_s3::operation::list_object_versions::ListObjectVersionsError>>> {
        let mut key_marker: Option<String> = None;
        let mut version_id_marker: Option<String> = None;
        for _ in 0..FIND_VERSION_BY_ETAG_MAX_PAGES {
            let page = self
                .client
                .list_object_versions()
                .bucket(bucket)
                .prefix(object)
                .max_keys(FIND_VERSION_BY_ETAG_PAGE_SIZE)
                .set_key_marker(key_marker.take())
                .set_version_id_marker(version_id_marker.take())
                .send()
                .await
                .map_err(Box::new)?;
            if let Some(version) = page.versions().iter().find(|version| {
                version.key() == Some(object)
                    && version.version_id().is_some_and(|id| !id.is_empty())
                    && replication_etags_match(Some(source_etag), version.e_tag())
            }) {
                return Ok(version.version_id().map(str::to_string));
            }
            // Every listed key is >= the prefix; once the listing moved past
            // the exact key there is nothing left to find.
            if page
                .versions()
                .iter()
                .any(|version| version.key().is_some_and(|key| key > object))
            {
                return Ok(None);
            }
            if !page.is_truncated().unwrap_or(false) {
                return Ok(None);
            }
            key_marker = page.next_key_marker().map(str::to_string);
            version_id_marker = page.next_version_id_marker().map(str::to_string);
            if key_marker.is_none() {
                return Ok(None);
            }
        }
        Ok(None)
    }

    /// HEAD used by the read-proxy path (GET/HEAD of an object not yet
    /// replicated locally, MinIO `proxyHeadToRepTarget`).
    ///
    /// Deliberately different from [`TargetClient::head_object`]: it must NOT
    /// send `source-replication-check` — that header is the replication
    /// worker's SSE-C metadata exemption channel. A proxied client request
    /// instead forwards the client's own SSE-C headers (`extra_headers`) so
    /// the target performs the real SSE-C validation/decryption. The
    /// `source-proxy-request` marker is always added so the target does not
    /// proxy the request onward (anti-loop).
    pub async fn head_object_for_proxy(
        &self,
        bucket: &str,
        object: &str,
        version_id: Option<String>,
        range: Option<String>,
        part_number: Option<i32>,
        extra_headers: HeaderMap,
    ) -> Result<HeadObjectOutput, HeadObjectSdkError> {
        let headers = proxy_outbound_headers(extra_headers);
        self.client
            .head_object()
            .bucket(bucket)
            .key(object)
            .set_version_id(resolve_read_api_version_id(version_id))
            .set_range(range)
            .set_part_number(part_number)
            .customize()
            .map_request(move |req| apply_extra_headers(req, &headers))
            .send()
            .await
            .map_err(Box::new)
    }

    /// GET used by the read-proxy path (MinIO `proxyGetToReplicationTarget`).
    /// Returns the streaming SDK output; callers must forward the body without
    /// buffering it. Same header contract as [`Self::head_object_for_proxy`]:
    /// anti-loop marker on, replication-check never sent, client SSE-C /
    /// conditional headers forwarded verbatim via `extra_headers`.
    pub async fn get_object(
        &self,
        bucket: &str,
        object: &str,
        version_id: Option<String>,
        range: Option<String>,
        part_number: Option<i32>,
        extra_headers: HeaderMap,
    ) -> Result<GetObjectOutput, GetObjectSdkError> {
        let headers = proxy_outbound_headers(extra_headers);
        self.client
            .get_object()
            .bucket(bucket)
            .key(object)
            .set_version_id(resolve_read_api_version_id(version_id))
            .set_range(range)
            .set_part_number(part_number)
            .customize()
            .map_request(move |req| apply_extra_headers(req, &headers))
            .send()
            .await
            .map_err(Box::new)
    }

    /// GetObjectTagging for the tagging read-proxy path
    /// (MinIO `proxyGetTaggingToRepTarget`). Anti-loop marker always added.
    pub async fn get_object_tagging(
        &self,
        bucket: &str,
        object: &str,
        version_id: Option<String>,
    ) -> Result<GetObjectTaggingOutput, GetObjectTaggingSdkError> {
        let headers = proxy_outbound_headers(HeaderMap::new());
        self.client
            .get_object_tagging()
            .bucket(bucket)
            .key(object)
            .set_version_id(resolve_read_api_version_id(version_id))
            .customize()
            .map_request(move |req| apply_extra_headers(req, &headers))
            .send()
            .await
            .map_err(Box::new)
    }

    /// PutObjectTagging for the tagging proxy path
    /// (MinIO `proxyTaggingToRepTarget`). Anti-loop marker always added.
    pub async fn put_object_tagging(
        &self,
        bucket: &str,
        object: &str,
        version_id: Option<String>,
        tagging: SdkTagging,
    ) -> Result<PutObjectTaggingOutput, PutObjectTaggingSdkError> {
        let headers = proxy_outbound_headers(HeaderMap::new());
        self.client
            .put_object_tagging()
            .bucket(bucket)
            .key(object)
            .set_version_id(resolve_read_api_version_id(version_id))
            .tagging(tagging)
            .customize()
            .map_request(move |req| apply_extra_headers(req, &headers))
            .send()
            .await
            .map_err(Box::new)
    }

    /// DeleteObjectTagging for the tagging proxy path
    /// (MinIO `proxyTaggingToRepTarget`). Anti-loop marker always added.
    pub async fn delete_object_tagging(
        &self,
        bucket: &str,
        object: &str,
        version_id: Option<String>,
    ) -> Result<DeleteObjectTaggingOutput, DeleteObjectTaggingSdkError> {
        let headers = proxy_outbound_headers(HeaderMap::new());
        self.client
            .delete_object_tagging()
            .bucket(bucket)
            .key(object)
            .set_version_id(resolve_read_api_version_id(version_id))
            .customize()
            .map_request(move |req| apply_extra_headers(req, &headers))
            .send()
            .await
            .map_err(Box::new)
    }

    /// On success returns the version id the target assigned (from
    /// `x-amz-version-id`), letting callers audit the version-identity
    /// contract — a target that adopts the source version echoes it back —
    /// together with the ETag of what the target actually stored, so callers
    /// can detect a target that persisted transformed bytes (#6853).
    pub async fn put_object(
        &self,
        bucket: &str,
        object: &str,
        size: i64,
        body: ByteStream,
        opts: &PutObjectOptions,
    ) -> Result<RemotePutObjectResponse, S3ClientError> {
        let mut headers = opts.header();

        let mut builder = self.client.put_object();

        let version_id = opts.internal.source_version_id.clone();
        if !version_id.is_empty() {
            insert_header(&mut headers, SUFFIX_SOURCE_VERSION_ID, &version_id);
        }
        let api_version_id = resolve_put_api_version_id(&version_id).map(ToOwned::to_owned);

        // A PUT carrying Object Lock parameters must also carry Content-MD5 or
        // an x-amz-checksum-* header on AWS-compatible targets (rustfs#7082).
        // The plain-payload default (rustfs#6853) sends neither, so supply one
        // here: the source ETag when it is the MD5 of the wire bytes, else an
        // SDK-computed checksum.
        match object_lock_put_integrity_for(&headers, opts) {
            ObjectLockIntegrity::NotRequired => {}
            ObjectLockIntegrity::ContentMd5Hex(md5_hex) => {
                let digest = hex_simd::decode_to_vec(md5_hex.as_bytes())
                    .map_err(|err| S3ClientError::new(format!("source etag is not hex: {err}")))?;
                let encoded = base64_simd::STANDARD.encode_to_string(digest);
                headers.insert(
                    http::header::HeaderName::from_static("content-md5"),
                    HeaderValue::from_str(&encoded).map_err(|err| S3ClientError::new(format!("invalid Content-MD5: {err}")))?,
                );
            }
            ObjectLockIntegrity::SdkChecksum => {
                builder = builder.checksum_algorithm(ChecksumAlgorithm::Crc32);
            }
        }

        match builder
            .bucket(bucket)
            .key(object)
            .content_length(size)
            .body(body)
            .customize()
            .map_request(move |mut req| {
                for (k, v) in headers.clone().into_iter() {
                    if let Some(key_str) = k.map(|k| k.as_str().to_string()) {
                        let value_str = v.to_str().unwrap_or("").to_string();
                        req.headers_mut().insert(key_str, value_str);
                    }
                }
                if let Some(version_id) = &api_version_id {
                    let uri = append_version_id_query(req.uri(), version_id);
                    req.set_uri(uri)
                        .map_err(aws_smithy_types::error::operation::BuildError::other)?;
                }

                Result::<_, aws_smithy_types::error::operation::BuildError>::Ok(req)
            })
            .send()
            .await
        {
            Ok(output) => {
                // Under SSE-KMS/DSSE or SSE-C the target's ETag is not the MD5
                // of the stored plaintext, so it cannot be compared against the
                // source ETag; withhold it rather than let a caller conclude
                // corruption from an opaque value.
                let etag_comparable = output.sse_customer_algorithm().is_none()
                    && !matches!(
                        output.server_side_encryption(),
                        Some(ServerSideEncryption::AwsKms) | Some(ServerSideEncryption::AwsKmsDsse)
                    );
                Ok(RemotePutObjectResponse {
                    version_id: output.version_id().map(ToOwned::to_owned),
                    etag: if etag_comparable {
                        output.e_tag().map(ToOwned::to_owned)
                    } else {
                        None
                    },
                })
            }
            Err(e) => match e {
                SdkError::ServiceError(service_err) => {
                    let err = service_err.into_err();
                    let meta = err.meta();
                    let error = match (meta.code(), meta.message()) {
                        (Some(code), Some(message)) => format!("put_object failed: {code}: {message}"),
                        (Some(code), None) => format!("put_object failed: {code}"),
                        (None, Some(message)) => format!("put_object failed: {message}"),
                        (None, None) => format!("put_object failed: {err:?}"),
                    };
                    Err(S3ClientError::with_metadata(
                        error,
                        None,
                        meta.code().map(ToOwned::to_owned),
                        meta.message().map(ToOwned::to_owned),
                    ))
                }
                SdkError::DispatchFailure(dispatch_err) => Err(S3ClientError::new(format!(
                    "put_object dispatch failure for bucket:{bucket} object:{object}: {dispatch_err:?}"
                ))),
                other => Err(S3ClientError::new(format!(
                    "put_object request failed for bucket:{bucket} object:{object}: {other:?}"
                ))),
            },
        }
    }

    pub async fn create_multipart_upload(
        &self,
        bucket: &str,
        object: &str,
        opts: &PutObjectOptions,
    ) -> Result<String, S3ClientError> {
        // Object metadata belongs to CreateMultipartUpload in S3 semantics;
        // building only the source-version headers here used to drop user
        // metadata, content-type, and the SSE intent for multipart replicas.
        let headers = opts.header();
        let version_id = opts.internal.source_version_id.clone();
        // The remote version of a multipart replication is decided at initiate
        // time; CompleteMultipartUpload does not read a versionId.
        let api_version_id = resolve_put_api_version_id(&version_id).map(ToOwned::to_owned);

        match self
            .client
            .create_multipart_upload()
            .bucket(bucket)
            .key(object)
            .customize()
            .map_request(move |mut req| {
                for (k, v) in headers.clone().into_iter() {
                    if let Some(key_str) = k.map(|k| k.as_str().to_string()) {
                        let value_str = v.to_str().unwrap_or("").to_string();
                        req.headers_mut().insert(key_str, value_str);
                    }
                }
                if let Some(version_id) = &api_version_id {
                    let uri = append_version_id_query(req.uri(), version_id);
                    req.set_uri(uri)
                        .map_err(aws_smithy_types::error::operation::BuildError::other)?;
                }
                Result::<_, aws_smithy_types::error::operation::BuildError>::Ok(req)
            })
            .send()
            .await
        {
            Ok(res) => Ok(res.upload_id.unwrap_or_default()),
            Err(e) => Err(e.into()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn put_object_part(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        part_id: i32,
        size: i64,
        body: ByteStream,
        opts: &PutObjectPartOptions,
    ) -> Result<UploadPartOutput, S3ClientError> {
        let headers = opts.custom_header.clone();

        match self
            .client
            .upload_part()
            .bucket(bucket)
            .key(object)
            .upload_id(upload_id)
            .part_number(part_id)
            .content_length(size)
            .body(body)
            .customize()
            .map_request(move |mut req| {
                for (k, v) in headers.clone().into_iter() {
                    if let Some(key_str) = k.map(|k| k.as_str().to_string()) {
                        let value_str = v.to_str().unwrap_or("").to_string();
                        req.headers_mut().insert(key_str, value_str);
                    }
                }
                Result::<_, aws_smithy_types::error::operation::BuildError>::Ok(req)
            })
            .send()
            .await
        {
            Ok(res) => Ok(res),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn complete_multipart_upload(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        parts: Vec<CompletedPart>,
        opts: &PutObjectOptions,
    ) -> Result<CompleteMultipartUploadOutput, S3ClientError> {
        let multipart_upload = CompletedMultipartUpload::builder().set_parts(Some(parts)).build();

        let headers = opts.header();

        match self
            .client
            .complete_multipart_upload()
            .bucket(bucket)
            .key(object)
            .upload_id(upload_id)
            .multipart_upload(multipart_upload)
            .customize()
            .map_request(move |mut req| {
                for (k, v) in headers.clone().into_iter() {
                    if let Some(key_str) = k.map(|k| k.as_str().to_string()) {
                        let value_str = v.to_str().unwrap_or("").to_string();
                        req.headers_mut().insert(key_str, value_str);
                    }
                }
                Result::<_, aws_smithy_types::error::operation::BuildError>::Ok(req)
            })
            .send()
            .await
        {
            Ok(res) => Ok(res),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn abort_multipart_upload(&self, bucket: &str, object: &str, upload_id: &str) -> Result<(), S3ClientError> {
        match self
            .client
            .abort_multipart_upload()
            .bucket(bucket)
            .key(object)
            .upload_id(upload_id)
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn remove_object(
        &self,
        bucket: &str,
        object: &str,
        version_id: Option<String>,
        opts: RemoveObjectOptions,
    ) -> Result<Option<String>, S3ClientError> {
        let headers = build_remove_object_headers(version_id.as_deref(), &opts);
        let api_version_id = resolve_delete_api_version_id(version_id, &opts);

        match self
            .client
            .delete_object()
            .bucket(bucket)
            .key(object)
            .set_version_id(api_version_id)
            .customize()
            .map_request(move |mut req| {
                for (k, v) in headers.clone().into_iter() {
                    if let Some(key_str) = k.map(|k| k.as_str().to_string()) {
                        let value_str = v.to_str().unwrap_or("").to_string();
                        req.headers_mut().insert(key_str, value_str);
                    }
                }
                Result::<_, aws_smithy_types::error::operation::BuildError>::Ok(req)
            })
            .send()
            .await
        {
            // A DELETE without a version id on a versioned target creates a delete
            // marker and reports the version it assigned. That id is the only
            // reliable handle for purging the marker later: a generic S3 target
            // does not mirror source version ids.
            Ok(res) => Ok(res.version_id().map(ToOwned::to_owned)),
            Err(e) => match e {
                SdkError::ServiceError(service_err) => {
                    let err = service_err.into_err();
                    let meta = err.meta();
                    let error = match (meta.code(), meta.message()) {
                        (Some(code), Some(message)) => format!("remove_object failed: {code}: {message}"),
                        (Some(code), None) => format!("remove_object failed: {code}"),
                        (None, Some(message)) => format!("remove_object failed: {message}"),
                        (None, None) => format!("remove_object failed: {err:?}"),
                    };
                    Err(S3ClientError::with_metadata(
                        error,
                        None,
                        meta.code().map(ToOwned::to_owned),
                        meta.message().map(ToOwned::to_owned),
                    ))
                }
                SdkError::DispatchFailure(dispatch_err) => Err(S3ClientError::new(format!(
                    "remove_object dispatch failure for bucket:{bucket} object:{object}: {dispatch_err:?}"
                ))),
                other => Err(S3ClientError::new(format!(
                    "remove_object request failed for bucket:{bucket} object:{object}: {other:?}"
                ))),
            },
        }
    }
}

#[derive(Debug)]
pub enum BucketTargetError {
    BucketRemoteTargetNotFound {
        bucket: String,
    },
    /// The bucket's persisted targets configuration exists but cannot be
    /// decoded. Distinct from `BucketRemoteTargetNotFound`, which means the
    /// bucket genuinely has no targets: callers must not degrade this one to
    /// an empty target set (rustfs/backlog#2282).
    BucketRemoteTargetsUnreadable {
        bucket: String,
    },
    BucketRemoteArnTypeInvalid {
        bucket: String,
    },
    BucketRemoteAlreadyExists {
        bucket: String,
    },
    BucketRemoteArnInvalid {
        bucket: String,
    },
    RemoteTargetConnectionErr {
        bucket: String,
        access_key: String,
        error: String,
    },
    BucketReplicationSourceNotVersioned {
        bucket: String,
    },
    BucketRemoteTargetNotVersioned {
        bucket: String,
    },
    BucketRemoteRemoveDisallowed {
        bucket: String,
    },

    Io(std::io::Error),
}

impl fmt::Display for BucketTargetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BucketTargetError::BucketRemoteTargetNotFound { bucket } => {
                write!(f, "Remote target not found for bucket: {bucket}")
            }
            BucketTargetError::BucketRemoteTargetsUnreadable { bucket } => {
                write!(f, "Persisted replication target configuration is unreadable for bucket: {bucket}")
            }
            BucketTargetError::BucketRemoteArnTypeInvalid { bucket } => {
                write!(f, "Invalid ARN type for bucket: {bucket}")
            }
            BucketTargetError::BucketRemoteAlreadyExists { bucket } => {
                write!(f, "Remote target already exists for bucket: {bucket}")
            }
            BucketTargetError::BucketRemoteArnInvalid { bucket } => {
                write!(f, "Invalid ARN for bucket: {bucket}")
            }
            BucketTargetError::RemoteTargetConnectionErr {
                bucket,
                access_key: _,
                error,
            } => {
                write!(
                    f,
                    "Connection error for bucket: {bucket}, access key: {REDACTED_CREDENTIAL}, error: {error}"
                )
            }
            BucketTargetError::BucketReplicationSourceNotVersioned { bucket } => {
                write!(f, "Replication source bucket not versioned: {bucket}")
            }
            BucketTargetError::BucketRemoteTargetNotVersioned { bucket } => {
                write!(f, "Remote target bucket not versioned: {bucket}")
            }
            BucketTargetError::BucketRemoteRemoveDisallowed { bucket } => {
                write!(f, "Remote target removal disallowed for bucket: {bucket}")
            }
            BucketTargetError::Io(e) => write!(f, "IO error: {e}"),
        }
    }
}

impl From<std::io::Error> for BucketTargetError {
    fn from(e: std::io::Error) -> Self {
        BucketTargetError::Io(e)
    }
}

impl Error for BucketTargetError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bucket::remote_s3_client::{
        EXPIRED_REMOTE_TARGET_CREDENTIALS, RemoteS3RetryPolicy, RemoteTargetCredentialsProvider,
        build_aws_s3_http_client_for_spec, build_aws_s3_http_client_from_target_ca_pem,
        build_aws_s3_http_client_with_trust_store, build_insecure_aws_s3_http_client, compose_replication_trust_store,
        ensure_rustls_crypto_provider, load_tls_path_ca_bundles, remote_sdk_credentials,
        replication_request_checksum_calculation, validate_remote_endpoint_inner, validate_target_ca_pem,
    };
    use aws_credential_types::Credentials as SdkCredentials;
    use aws_sdk_s3::Config as S3Config;
    use aws_sdk_s3::config::{Region as SdkRegion, RequestChecksumCalculation, SharedCredentialsProvider, SharedHttpClient};
    use aws_smithy_runtime_api::client::http::{
        HttpConnector as SmithyHttpConnector, HttpConnectorFuture, SharedHttpConnector, http_client_fn,
    };
    use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
    use aws_smithy_types::body::SdkBody;
    use rcgen::generate_simple_self_signed;
    use rustfs_config::{RUSTFS_CA_CERT, RUSTFS_TLS_CERT};
    use rustfs_utils::egress::OutboundUrlError;
    use rustfs_utils::http::AMZ_SERVER_SIDE_ENCRYPTION;

    // The startup panic fix for hosts without a CA bundle (issue #6734) rests
    // on two properties: the health-check client constructor never panics, and
    // its degraded fallback — an explicit empty trust store — always builds.
    #[test]
    fn health_check_client_construction_never_panics() {
        let _ = build_health_check_client();
        HttpClient::builder()
            .tls_certs_only(std::iter::empty::<reqwest::Certificate>())
            .build()
            .expect("empty-trust-store client build must succeed without touching system roots");
    }

    #[derive(Clone, Debug)]
    struct RecordingHttpConnector {
        request_uris: Arc<std::sync::Mutex<Vec<String>>>,
    }

    impl SmithyHttpConnector for RecordingHttpConnector {
        fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
            self.request_uris
                .lock()
                .expect("recorded request lock should not be poisoned")
                .push(request.uri().to_string());
            HttpConnectorFuture::ready(Ok(HttpResponse::new(
                aws_smithy_runtime_api::http::StatusCode::try_from(204_u16).expect("204 should be a valid response status"),
                SdkBody::empty(),
            )))
        }
    }

    type RecordedHeaders = Arc<std::sync::Mutex<Vec<Vec<(String, String)>>>>;

    /// Records full request headers and answers with canned response headers,
    /// for asserting wire framing and response parsing.
    #[derive(Clone, Debug)]
    struct RecordingHeaderConnector {
        request_headers: RecordedHeaders,
        response_headers: Vec<(String, String)>,
    }

    impl SmithyHttpConnector for RecordingHeaderConnector {
        fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
            self.request_headers
                .lock()
                .expect("recorded header lock should not be poisoned")
                .push(
                    request
                        .headers()
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect(),
                );
            let mut response = HttpResponse::new(
                aws_smithy_runtime_api::http::StatusCode::try_from(200_u16).expect("200 should be a valid response status"),
                SdkBody::empty(),
            );
            for (name, value) in &self.response_headers {
                response.headers_mut().insert(name.clone(), value.clone());
            }
            HttpConnectorFuture::ready(Ok(response))
        }
    }

    fn header_recording_target_client(response_headers: Vec<(String, String)>) -> (TargetClient, RecordedHeaders) {
        let request_headers: RecordedHeaders = Arc::new(std::sync::Mutex::new(Vec::new()));
        let connector = SharedHttpConnector::new(RecordingHeaderConnector {
            request_headers: Arc::clone(&request_headers),
            response_headers,
        });
        let http_client = http_client_fn(move |_settings, _components| connector.clone());
        let client = s3_client_for_test(443, Some(http_client));
        (
            TargetClient {
                endpoint: "https://localhost:443".to_string(),
                credentials: None,
                bucket: "target-bucket".to_string(),
                storage_class: String::new(),
                disable_proxy: false,
                arn: "arn:rustfs:replication:us-east-1:target:bucket".to_string(),
                reset_id: String::new(),
                secure: true,
                health_check_duration: Duration::from_secs(5),
                replicate_sync: false,
                client: Arc::new(client),
            },
            request_headers,
        )
    }

    fn streaming_test_body(payload: &'static [u8]) -> ByteStream {
        let stream = tokio_util::io::ReaderStream::new(std::io::Cursor::new(payload));
        let body = http_body_util::StreamBody::new(futures::StreamExt::map(stream, |r| r.map(http_body::Frame::data)));
        ByteStream::new(SdkBody::from_body_1_x(body))
    }

    #[test]
    fn replication_checksums_default_to_plain_payloads() {
        assert!(matches!(
            replication_request_checksum_calculation(),
            RequestChecksumCalculation::WhenRequired
        ));
    }

    #[tokio::test]
    async fn replication_put_object_sends_plain_signed_payloads_by_default() {
        let (client, recorded) = header_recording_target_client(Vec::new());
        client
            .put_object("target-bucket", "object", 4, streaming_test_body(b"data"), &PutObjectOptions::default())
            .await
            .expect("recorded put_object should succeed");

        let recorded = recorded.lock().expect("recorded header lock should not be poisoned");
        let headers = &recorded[0];
        let header = |name: &str| {
            headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(name))
                .map(|(_, v)| v.as_str())
        };
        // The #6853 regression shape: trailer checksums force aws-chunked
        // framing, which a non-decoding target stores verbatim as the object.
        assert_eq!(header("x-amz-trailer"), None, "streaming uploads must not carry a trailer checksum");
        assert!(
            header("content-encoding").is_none_or(|v| !v.contains("aws-chunked")),
            "streaming uploads must not be aws-chunked framed"
        );
        assert_eq!(header("x-amz-decoded-content-length"), None);
        assert_eq!(header("content-length"), Some("4"));
    }

    fn recorded_header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
        headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    fn locked_put_options(source_etag: &str) -> PutObjectOptions {
        PutObjectOptions {
            mode: Some(ObjectLockRetentionMode::Governance),
            retain_until_date: OffsetDateTime::from_unix_timestamp(4_102_444_800).expect("valid timestamp"),
            internal: AdvancedPutOptions {
                source_etag: source_etag.to_string(),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// rustfs#7082: a locked PUT of a plaintext single-part object carries a
    /// Content-MD5 derived from the source ETag, still as a plain payload.
    #[tokio::test]
    async fn locked_put_object_carries_content_md5_from_the_source_etag() {
        let (client, recorded) = header_recording_target_client(Vec::new());
        client
            .put_object(
                "target-bucket",
                "object",
                4,
                streaming_test_body(b"data"),
                &locked_put_options("\"8d777f385d3dfec8815d20f7496026dc\""),
            )
            .await
            .expect("recorded put_object should succeed");

        let recorded = recorded.lock().expect("recorded header lock should not be poisoned");
        let headers = &recorded[0];
        // base64 of the MD5 bytes of "data".
        assert_eq!(recorded_header(headers, "content-md5"), Some("jXd/OF09/siBXSD3SWAm3A=="));
        assert_eq!(recorded_header(headers, "x-amz-object-lock-mode"), Some("GOVERNANCE"));
        assert_eq!(
            recorded_header(headers, "x-amz-trailer"),
            None,
            "Content-MD5 must not change the payload framing"
        );
        assert!(
            recorded_header(headers, "content-encoding").is_none_or(|v| !v.contains("aws-chunked")),
            "locked uploads stay plain signed payloads"
        );
        assert_eq!(recorded_header(headers, "content-length"), Some("4"));
    }

    /// A legal hold is an Object Lock parameter too.
    #[tokio::test]
    async fn legal_hold_put_object_carries_content_md5() {
        let (client, recorded) = header_recording_target_client(Vec::new());
        let opts = PutObjectOptions {
            legalhold: Some(ObjectLockLegalHoldStatus::On),
            internal: AdvancedPutOptions {
                source_etag: "8d777f385d3dfec8815d20f7496026dc".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        client
            .put_object("target-bucket", "object", 4, streaming_test_body(b"data"), &opts)
            .await
            .expect("recorded put_object should succeed");
        let recorded = recorded.lock().expect("recorded header lock should not be poisoned");
        assert_eq!(recorded_header(&recorded[0], "content-md5"), Some("jXd/OF09/siBXSD3SWAm3A=="));
        assert_eq!(recorded_header(&recorded[0], "x-amz-object-lock-legal-hold"), Some("ON"));
    }

    /// When the source ETag is not the MD5 of the wire bytes (multipart
    /// layout, or an encrypted object) the SDK computes the checksum instead;
    /// with a streaming body that is a CRC32 trailer.
    #[tokio::test]
    async fn locked_put_object_without_a_usable_etag_uses_an_sdk_checksum() {
        for (label, opts) in [
            ("multipart etag", locked_put_options("8d777f385d3dfec8815d20f7496026dc-3")),
            ("managed sse", {
                let mut opts = locked_put_options("8d777f385d3dfec8815d20f7496026dc");
                opts.user_metadata
                    .insert(AMZ_SERVER_SIDE_ENCRYPTION.to_string(), "AES256".to_string());
                opts
            }),
        ] {
            let (client, recorded) = header_recording_target_client(Vec::new());
            client
                .put_object("target-bucket", "object", 4, streaming_test_body(b"data"), &opts)
                .await
                .expect("recorded put_object should succeed");
            let recorded = recorded.lock().expect("recorded header lock should not be poisoned");
            let headers = &recorded[0];
            assert_eq!(
                recorded_header(headers, "content-md5"),
                None,
                "{label}: the source etag is not the wire MD5"
            );
            assert!(
                recorded_header(headers, "x-amz-sdk-checksum-algorithm").is_some_and(|v| v.eq_ignore_ascii_case("CRC32"))
                    || recorded_header(headers, "x-amz-checksum-crc32").is_some(),
                "{label}: the SDK must announce a CRC32 checksum; headers: {headers:?}"
            );
        }
    }

    /// A forwarded source checksum already satisfies the rule; nothing is added.
    #[tokio::test]
    async fn locked_put_object_keeps_a_forwarded_source_checksum() {
        let (client, recorded) = header_recording_target_client(Vec::new());
        let mut opts = locked_put_options("8d777f385d3dfec8815d20f7496026dc");
        opts.user_metadata
            .insert("x-amz-checksum-crc32".to_string(), "rfPzYw==".to_string());
        client
            .put_object("target-bucket", "object", 4, streaming_test_body(b"data"), &opts)
            .await
            .expect("recorded put_object should succeed");
        let recorded = recorded.lock().expect("recorded header lock should not be poisoned");
        let headers = &recorded[0];
        assert_eq!(recorded_header(headers, "x-amz-checksum-crc32"), Some("rfPzYw=="));
        assert_eq!(recorded_header(headers, "content-md5"), None);
        assert_eq!(recorded_header(headers, "x-amz-trailer"), None);
    }

    /// Without Object Lock parameters the plain-payload default is untouched.
    #[tokio::test]
    async fn unlocked_put_object_adds_no_integrity_header() {
        let (client, recorded) = header_recording_target_client(Vec::new());
        let opts = PutObjectOptions {
            internal: AdvancedPutOptions {
                source_etag: "8d777f385d3dfec8815d20f7496026dc".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        client
            .put_object("target-bucket", "object", 4, streaming_test_body(b"data"), &opts)
            .await
            .expect("recorded put_object should succeed");
        let recorded = recorded.lock().expect("recorded header lock should not be poisoned");
        let headers = &recorded[0];
        assert_eq!(recorded_header(headers, "content-md5"), None);
        assert_eq!(recorded_header(headers, "x-amz-trailer"), None);
        assert_eq!(recorded_header(headers, "x-amz-sdk-checksum-algorithm"), None);
    }

    #[tokio::test]
    async fn put_object_returns_the_etag_the_target_stored() {
        let (client, _) =
            header_recording_target_client(vec![("etag".to_string(), "\"9a0364b9e99bb480dd25e1f0284c8555\"".to_string())]);
        let response = client
            .put_object(
                "target-bucket",
                "object",
                4,
                ByteStream::from_static(b"data"),
                &PutObjectOptions::default(),
            )
            .await
            .expect("recorded put_object should succeed");
        assert_eq!(response.etag.as_deref(), Some("\"9a0364b9e99bb480dd25e1f0284c8555\""));
    }

    #[tokio::test]
    async fn put_object_withholds_the_etag_under_target_side_kms() {
        let (client, _) = header_recording_target_client(vec![
            ("etag".to_string(), "\"9a0364b9e99bb480dd25e1f0284c8555\"".to_string()),
            ("x-amz-server-side-encryption".to_string(), "aws:kms".to_string()),
        ]);
        let response = client
            .put_object(
                "target-bucket",
                "object",
                4,
                ByteStream::from_static(b"data"),
                &PutObjectOptions::default(),
            )
            .await
            .expect("recorded put_object should succeed");
        assert!(
            response.etag.is_none(),
            "a KMS-encrypted replica's etag is not the content MD5 and must be withheld"
        );
    }

    #[derive(Clone, Debug)]
    struct RecordingAuthConnector {
        signed_requests: Arc<std::sync::Mutex<Vec<(bool, bool)>>>,
    }

    impl SmithyHttpConnector for RecordingAuthConnector {
        fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
            let has_expected_token = request.headers().get("x-amz-security-token") == Some("temporary-session-token");
            let has_authorization = request.headers().contains_key("authorization");
            self.signed_requests
                .lock()
                .expect("recorded auth request lock should not be poisoned")
                .push((has_expected_token, has_authorization));
            HttpConnectorFuture::ready(Ok(HttpResponse::new(
                aws_smithy_runtime_api::http::StatusCode::try_from(200_u16).expect("200 should be a valid response status"),
                SdkBody::empty(),
            )))
        }
    }

    fn recording_target_client() -> (TargetClient, Arc<std::sync::Mutex<Vec<String>>>) {
        let request_uris = Arc::new(std::sync::Mutex::new(Vec::new()));
        let connector = SharedHttpConnector::new(RecordingHttpConnector {
            request_uris: Arc::clone(&request_uris),
        });
        let http_client = http_client_fn(move |_settings, _components| connector.clone());
        let client = s3_client_for_test(443, Some(http_client));
        (
            TargetClient {
                endpoint: "https://localhost:443".to_string(),
                credentials: None,
                bucket: "target-bucket".to_string(),
                storage_class: String::new(),
                disable_proxy: false,
                arn: "arn:rustfs:replication:us-east-1:target:bucket".to_string(),
                reset_id: String::new(),
                secure: true,
                health_check_duration: Duration::from_secs(5),
                replicate_sync: false,
                client: Arc::new(client),
            },
            request_uris,
        )
    }

    #[test]
    fn remote_target_sdk_credentials_preserve_temporary_credential_fields() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        let expiration = SystemTime::UNIX_EPOCH + Duration::from_secs(2_000);
        let credentials = Credentials {
            access_key: "access".to_string(),
            secret_key: "secret".to_string(),
            session_token: Some("temporary-session-token".to_string()),
            expiration: Some(jiff::Timestamp::try_from(expiration).expect("test expiration should convert")),
        };

        let sdk_credentials = remote_sdk_credentials(&remote_credentials(&credentials, "account"), now)
            .expect("unexpired temporary credentials should build");

        assert_eq!(sdk_credentials.session_token(), Some("temporary-session-token"));
        assert_eq!(sdk_credentials.expiry(), Some(expiration));
        assert_eq!(sdk_credentials.account_id().map(|id| id.as_str()), Some("account"));
    }

    #[test]
    fn remote_target_sdk_credentials_normalize_go_zero_expiration() {
        let credentials = Credentials {
            access_key: "access".to_string(),
            secret_key: "secret".to_string(),
            session_token: None,
            expiration: Some("0001-01-01T00:00:00Z".parse().expect("Go zero time should parse")),
        };

        let sdk_credentials = remote_sdk_credentials(&remote_credentials(&credentials, ""), SystemTime::now())
            .expect("Go zero expiration should remain compatible with static credentials");

        assert!(sdk_credentials.session_token().is_none());
        assert!(sdk_credentials.expiry().is_none());
    }

    #[test]
    fn remote_target_sdk_credentials_reject_invalid_expiration_boundaries() {
        let expiration = SystemTime::UNIX_EPOCH + Duration::from_secs(2_000);
        let mut credentials = Credentials {
            access_key: "access".to_string(),
            secret_key: "secret".to_string(),
            session_token: None,
            expiration: Some(jiff::Timestamp::try_from(expiration).expect("test expiration should convert")),
        };

        assert_eq!(
            remote_sdk_credentials(&remote_credentials(&credentials, ""), SystemTime::UNIX_EPOCH + Duration::from_secs(1_000))
                .expect_err("expiration without a session token must fail"),
            "remote target credential expiration requires a session token"
        );

        credentials.session_token = Some("temporary-session-token".to_string());
        assert_eq!(
            remote_sdk_credentials(&remote_credentials(&credentials, ""), expiration)
                .expect_err("credentials expire at the exact expiration boundary"),
            EXPIRED_REMOTE_TARGET_CREDENTIALS
        );
    }

    #[test]
    fn remote_target_credentials_provider_fails_closed_after_expiration() {
        let expiration = SystemTime::UNIX_EPOCH + Duration::from_secs(2_000);
        let provider = RemoteTargetCredentialsProvider {
            credentials: SdkCredentials::new(
                "access",
                "secret",
                Some("temporary-session-token".to_string()),
                Some(expiration),
                "test",
            ),
        };

        assert!(provider.resolve_at(expiration - Duration::from_nanos(1)).is_ok());
        let err = provider
            .resolve_at(expiration)
            .expect_err("expired credentials must not be returned");
        assert_eq!(err.source().map(ToString::to_string).as_deref(), Some(EXPIRED_REMOTE_TARGET_CREDENTIALS));
        assert!(!format!("{provider:?}").contains("temporary-session-token"));
        assert!(!format!("{provider:?}").contains("secret"));
    }

    #[test]
    fn target_client_detects_expiration_for_cache_refresh() {
        let expiration: jiff::Timestamp = "2099-01-01T00:00:00Z".parse().expect("expiration should parse");
        let (mut client, _) = recording_target_client();
        client.credentials = Some(Credentials {
            access_key: "access".to_string(),
            secret_key: "secret".to_string(),
            session_token: Some("temporary-session-token".to_string()),
            expiration: Some(expiration),
        });

        assert!(!client.credentials_expired_at("2098-12-31T23:59:59Z".parse().expect("pre-expiration timestamp should parse")));
        assert!(client.credentials_expired_at(expiration));

        client.credentials.as_mut().expect("credentials should exist").expiration =
            Some("0001-01-01T00:00:00Z".parse().expect("Go zero time should parse"));
        assert!(!client.credentials_expired_at(jiff::Timestamp::now()));
    }

    #[tokio::test]
    async fn temporary_credentials_add_security_token_to_sigv4_requests() {
        let signed_requests = Arc::new(std::sync::Mutex::new(Vec::new()));
        let connector = SharedHttpConnector::new(RecordingAuthConnector {
            signed_requests: Arc::clone(&signed_requests),
        });
        let http_client = http_client_fn(move |_settings, _components| connector.clone());
        let credentials = Credentials {
            access_key: "access".to_string(),
            secret_key: "secret".to_string(),
            session_token: Some("temporary-session-token".to_string()),
            expiration: Some("2099-01-01T00:00:00Z".parse().expect("future expiration should parse")),
        };
        let sdk_credentials = remote_sdk_credentials(&remote_credentials(&credentials, ""), SystemTime::now())
            .expect("unexpired temporary credentials should build");
        let client = S3Client::from_conf(
            S3Config::builder()
                .endpoint_url("https://target.example")
                .credentials_provider(SharedCredentialsProvider::new(RemoteTargetCredentialsProvider {
                    credentials: sdk_credentials,
                }))
                .region(SdkRegion::new("us-east-1"))
                .http_client(http_client)
                .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
                .build(),
        );

        client
            .head_bucket()
            .bucket("target-bucket")
            .send()
            .await
            .expect("recording connector should accept the signed request");

        assert_eq!(
            signed_requests
                .lock()
                .expect("recorded auth request lock should not be poisoned")
                .as_slice(),
            &[(true, true)],
            "SigV4 request must include both authorization and the session-token header"
        );
    }

    fn spawn_https_server(cert: &rcgen::CertifiedKey<rcgen::KeyPair>, requests: usize) -> (u16, std::thread::JoinHandle<()>) {
        use std::io::{Read, Write};

        ensure_rustls_crypto_provider();
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).expect("test TLS listener should bind");
        let port = listener
            .local_addr()
            .expect("test TLS listener should have an address")
            .port();
        let server_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![cert.cert.der().clone()],
                rustls_pki_types::PrivateKeyDer::try_from(cert.signing_key.serialize_der())
                    .expect("test TLS private key should convert"),
            )
            .expect("test TLS server config should build");

        let handle = std::thread::spawn(move || {
            let server_config = Arc::new(server_config);
            for _ in 0..requests {
                let (stream, _) = listener.accept().expect("test TLS client should connect");
                stream
                    .set_read_timeout(Some(Duration::from_secs(10)))
                    .expect("test TLS read timeout should configure");
                stream
                    .set_write_timeout(Some(Duration::from_secs(10)))
                    .expect("test TLS write timeout should configure");
                let connection = rustls::ServerConnection::new(server_config.clone()).expect("test TLS connection should build");
                let mut stream = rustls::StreamOwned::new(connection, stream);
                let mut request = [0_u8; 8192];
                if stream.read(&mut request).is_err() {
                    continue;
                }
                stream
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                    .expect("test TLS response should be written");
                stream.flush().expect("test TLS response should flush");
            }
        });

        (port, handle)
    }

    fn spawn_http_status_server(status: u16) -> (u16, std::thread::JoinHandle<()>) {
        use std::io::{Read, Write};

        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).expect("test HTTP listener should bind");
        let port = listener
            .local_addr()
            .expect("test HTTP listener should have an address")
            .port();
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("test HTTP client should connect");
            let mut request = [0_u8; 8192];
            let bytes_read = stream.read(&mut request).expect("test HTTP request should be read");
            assert!(bytes_read > 0, "test HTTP request should not be empty");
            write!(stream, "HTTP/1.1 {status} Test\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .expect("test HTTP response should be written");
        });
        (port, handle)
    }

    fn spawn_delayed_http_server() -> (
        u16,
        tokio::sync::oneshot::Receiver<()>,
        std::sync::mpsc::Sender<()>,
        std::thread::JoinHandle<()>,
    ) {
        use std::io::{Read, Write};

        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).expect("test HTTP listener should bind");
        let port = listener
            .local_addr()
            .expect("test HTTP listener should have an address")
            .port();
        let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("test HTTP client should connect");
            let mut request = [0_u8; 8192];
            let bytes_read = stream.read(&mut request).expect("test HTTP request should be read");
            assert!(bytes_read > 0, "test HTTP request should not be empty");
            accepted_tx.send(()).expect("test should wait for request");
            release_rx.recv().expect("test should release response");
            stream
                .write_all(b"HTTP/1.1 500 Test\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .expect("test HTTP response should be written");
        });
        (port, accepted_rx, release_tx, handle)
    }

    fn s3_client_for_test(port: u16, http_client: Option<SharedHttpClient>) -> S3Client {
        s3_client_for_endpoint_test(format!("https://localhost:{port}"), http_client)
    }

    fn s3_client_for_endpoint_test(endpoint: String, http_client: Option<SharedHttpClient>) -> S3Client {
        let credentials = SdkCredentials::builder()
            .access_key_id("test-access")
            .secret_access_key("test-secret")
            .provider_name("bucket_target_tls_test")
            .build();
        let mut config = S3Config::builder()
            .endpoint_url(endpoint)
            .credentials_provider(SharedCredentialsProvider::new(credentials))
            .region(SdkRegion::new("us-east-1"))
            .force_path_style(true)
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            // Mirror the production remote-target builder so recorded requests
            // exercise the same checksum/framing behavior (#6853).
            .request_checksum_calculation(replication_request_checksum_calculation());
        if let Some(http_client) = http_client {
            config = config.http_client(http_client);
        }

        S3Client::from_conf(config.build())
    }

    fn target_client_for_test(arn: &str, endpoint: String, client: S3Client) -> Arc<TargetClient> {
        Arc::new(TargetClient {
            endpoint,
            credentials: None,
            bucket: "target-bucket".to_string(),
            storage_class: String::new(),
            disable_proxy: false,
            arn: arn.to_string(),
            reset_id: String::new(),
            secure: true,
            health_check_duration: Duration::from_secs(5),
            replicate_sync: false,
            client: Arc::new(client),
        })
    }

    #[test]
    fn replication_target_versioning_enabled_requires_enabled_status() {
        let enabled = BucketVersioningStatus::Enabled;
        let suspended = BucketVersioningStatus::Suspended;

        assert!(replication_target_versioning_enabled(Some(&enabled)));
        assert!(!replication_target_versioning_enabled(Some(&suspended)));
        assert!(!replication_target_versioning_enabled(None));
    }

    #[test]
    fn remote_endpoint_spec_from_target_keeps_legacy_path_style_and_trust_semantics() {
        for (path, expected) in [
            ("dns", PathStyle::VirtualHost),
            ("OFF", PathStyle::VirtualHost),
            ("false", PathStyle::VirtualHost),
            ("path", PathStyle::Path),
            ("on", PathStyle::Path),
            ("true", PathStyle::Path),
            (" auto ", PathStyle::Auto),
            ("", PathStyle::Auto),
            ("something-else", PathStyle::Path),
        ] {
            assert_eq!(target_path_style(path), expected, "path={path:?}");
        }

        let spec = RemoteS3EndpointSpec::from(&BucketTarget {
            endpoint: "192.168.1.10:9000".to_string(),
            secure: true,
            region: "us-east-1".to_string(),
            ca_cert_pem: "   ".to_string(),
            reset_id: "reset-1".to_string(),
            credentials: Some(Credentials {
                access_key: "access".to_string(),
                secret_key: "secret".to_string(),
                session_token: Some("  ".to_string()),
                expiration: Some("0001-01-01T00:00:00Z".parse().expect("Go zero time should parse")),
            }),
            ..Default::default()
        });
        assert_eq!(spec.endpoint_url(), "https://192.168.1.10:9000");
        assert!(spec.ca_cert_pem.is_none(), "whitespace-only CA PEM means unset");
        assert!(spec.connect_timeout.is_none() && spec.read_timeout.is_none());
        assert_eq!(spec.user_agent_suffix, "");
        assert_eq!(
            spec.retry,
            RemoteS3RetryPolicy::Standard { max_attempts: 3 },
            "replication targets keep the SDK's historical three attempts"
        );
        let credentials = spec.credentials.expect("credentials carry over");
        assert_eq!(credentials.account_id, "reset-1");
        assert!(credentials.session_token.is_none(), "blank session token is absent");
        assert!(credentials.expiration.is_none(), "Go zero expiration is absent");
    }

    fn parse_url(raw: &str) -> Url {
        Url::parse(raw).expect("test URL should parse")
    }

    #[test]
    fn replication_endpoint_always_allows_public_and_private() {
        // Public hosts and private-network targets are allowed regardless of the
        // loopback opt-in — replication commonly runs across trusted private infra.
        for allow_loopback in [false, true] {
            assert!(validate_remote_endpoint_inner(&parse_url("https://s3.example.com"), allow_loopback).is_ok());
            assert!(validate_remote_endpoint_inner(&parse_url("http://10.0.0.5:9000"), allow_loopback).is_ok());
            assert!(validate_remote_endpoint_inner(&parse_url("http://192.168.1.20"), allow_loopback).is_ok());
        }
    }

    #[test]
    fn replication_endpoint_rejects_loopback_without_opt_in() {
        // Default (production) behaviour: loopback IP and localhost host both rejected.
        let err = validate_remote_endpoint_inner(&parse_url("http://127.0.0.1:9000"), false)
            .expect_err("loopback IP must be rejected by default");
        assert!(matches!(
            err,
            OutboundUrlError::ForbiddenHost {
                reason: "loopback address",
                ..
            }
        ));
        let err = validate_remote_endpoint_inner(&parse_url("http://localhost:9000"), false)
            .expect_err("localhost must be rejected by default");
        assert!(matches!(
            err,
            OutboundUrlError::ForbiddenHost {
                reason: "loopback host",
                ..
            }
        ));
    }

    #[test]
    fn replication_endpoint_allows_loopback_with_opt_in() {
        // e2e harness / single-host multi-instance: opt-in re-enables loopback in
        // both IP (127.0.0.1, ::1) and hostname (localhost) forms.
        assert!(validate_remote_endpoint_inner(&parse_url("http://127.0.0.1:9000"), true).is_ok());
        assert!(validate_remote_endpoint_inner(&parse_url("http://[::1]:9000"), true).is_ok());
        assert!(validate_remote_endpoint_inner(&parse_url("http://localhost:9000"), true).is_ok());
    }

    #[test]
    fn replication_endpoint_opt_in_does_not_open_other_ssrf_targets() {
        // The loopback opt-in must not widen into link-local / metadata endpoints.
        let err = validate_remote_endpoint_inner(&parse_url("http://169.254.169.254/latest/meta-data"), true)
            .expect_err("metadata endpoint must stay rejected even with loopback opt-in");
        assert!(matches!(
            err,
            OutboundUrlError::ForbiddenHost {
                reason: "metadata endpoint",
                ..
            }
        ));
        let err = validate_remote_endpoint_inner(&parse_url("http://[fe80::1]:9000"), true)
            .expect_err("link-local must stay rejected even with loopback opt-in");
        assert!(matches!(
            err,
            OutboundUrlError::ForbiddenHost {
                reason: "link-local address",
                ..
            }
        ));
    }

    #[test]
    fn remote_target_connection_error_display_redacts_access_key() {
        let err = BucketTargetError::RemoteTargetConnectionErr {
            bucket: "target".to_string(),
            access_key: "sensitive-access-key".to_string(),
            error: "connection refused".to_string(),
        };
        let message = err.to_string();

        assert!(message.contains(REDACTED_CREDENTIAL));
        assert!(!message.contains("sensitive-access-key"));
        assert!(message.contains("connection refused"));
    }

    #[test]
    fn same_replication_service_ignores_resync_and_bandwidth_edits() {
        let base = BucketTarget {
            endpoint: "target.example:9000".to_string(),
            target_bucket: "replica".to_string(),
            secure: true,
            path: "on".to_string(),
            arn: "arn:rustfs:replication:us-east-1:bucket:same".to_string(),
            credentials: Some(Credentials {
                access_key: "access".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let resync_edit = BucketTarget {
            reset_id: "reset-1".to_string(),
            bandwidth_limit: 1024,
            ..base.clone()
        };
        assert!(same_replication_service(&resync_edit, &base));
        for moved in [
            BucketTarget {
                endpoint: "other.example:9000".to_string(),
                ..base.clone()
            },
            BucketTarget {
                target_bucket: "other".to_string(),
                ..base.clone()
            },
            BucketTarget {
                secure: false,
                ..base.clone()
            },
            BucketTarget {
                credentials: Some(Credentials {
                    access_key: "rotated".to_string(),
                    ..Default::default()
                }),
                ..base.clone()
            },
        ] {
            assert!(!same_replication_service(&moved, &base));
        }
    }

    #[test]
    fn version_identity_verdict_is_per_arn_and_forgotten_with_the_target() {
        let sys = BucketTargetSys::default();
        let arn = "arn:rustfs:replication:us-east-1:bucket:identity";
        assert_eq!(sys.version_identity_capability(arn), VersionIdentityCapability::Unknown);
        sys.record_version_identity_capability(arn, VersionIdentityCapability::MintsOwn);
        assert_eq!(sys.version_identity_capability(arn), VersionIdentityCapability::MintsOwn);
        assert_eq!(sys.version_identity_capability("other"), VersionIdentityCapability::Unknown);
        // A rebuilt target may point at a different service.
        sys.forget_version_identity_capability(arn);
        assert_eq!(sys.version_identity_capability(arn), VersionIdentityCapability::Unknown);
    }

    #[test]
    fn endpoint_health_key_preserves_explicit_port() {
        let url = Url::parse("https://remote.example:9443").expect("url should parse");

        assert_eq!(endpoint_health_key(&url), "remote.example:9443");
    }

    #[test]
    fn update_endpoint_health_counts_offline_transitions() {
        let mut health = EpHealth::default();
        let now = OffsetDateTime::now_utc();

        update_endpoint_health(&mut health, false, Duration::from_millis(25), now);
        update_endpoint_health(&mut health, false, Duration::from_millis(25), now);
        update_endpoint_health(&mut health, true, Duration::from_millis(10), now);
        update_endpoint_health(&mut health, false, Duration::from_millis(25), now);

        assert_eq!(health.offline_count, 2);
        assert_eq!(health.offline_duration, Duration::from_millis(75));
        assert_eq!(health.last_online, Some(now));
    }

    /// N2 TTL contract, both flip directions: a recorded verdict is fresh
    /// until [`SSEC_PASSTHROUGH_CAPABILITY_TTL`], then reads as expired; a
    /// re-audit that records the OPPOSITE verdict replaces it as fresh. The
    /// worker gate maps expired verdicts to ProceedWithAudit (pinned in
    /// `replication_target_boundary`), so together this proves an Unsupported
    /// target recovers to Supported through the audit once its verdict ages
    /// out — and a stale Supported one is re-proven rather than trusted.
    #[tokio::test]
    async fn ssec_passthrough_capability_ttl_expires_and_reaudit_flips_verdict() {
        let sys = BucketTargetSys::default();
        let arn = "arn:rustfs:replication:us-east-1:bucket:ssec-ttl";
        let expired_age = SSEC_PASSTHROUGH_CAPABILITY_TTL + Duration::from_secs(1);

        assert_eq!(
            sys.ssec_passthrough_capability(arn).await,
            (SsecPassthroughCapability::Unknown, false),
            "an unrecorded target must read Unknown and never expired"
        );

        sys.record_ssec_passthrough_capability(arn, SsecPassthroughCapability::Unsupported)
            .await;
        assert_eq!(
            sys.ssec_passthrough_capability(arn).await,
            (SsecPassthroughCapability::Unsupported, false)
        );

        sys.backdate_ssec_passthrough_capability(arn, expired_age).await;
        assert_eq!(
            sys.ssec_passthrough_capability(arn).await,
            (SsecPassthroughCapability::Unsupported, true),
            "an aged-out Unsupported verdict must read expired so the gate re-audits"
        );

        // The re-audit against an upgraded target records Supported afresh.
        sys.record_ssec_passthrough_capability(arn, SsecPassthroughCapability::Supported)
            .await;
        assert_eq!(
            sys.ssec_passthrough_capability(arn).await,
            (SsecPassthroughCapability::Supported, false),
            "a fresh Supported verdict replaces the expired Unsupported one"
        );

        // And the fail-open twin: Supported also ages out.
        sys.backdate_ssec_passthrough_capability(arn, expired_age).await;
        assert_eq!(
            sys.ssec_passthrough_capability(arn).await,
            (SsecPassthroughCapability::Supported, true),
            "an aged-out Supported verdict must read expired so the gate re-proves it"
        );
    }

    #[tokio::test]
    async fn list_targets_applies_health_stats_by_arn_and_preserves_endpoint_port() {
        let sys = BucketTargetSys::default();
        let arn = "arn:rustfs:replication:us-east-1:bucket:id";
        let endpoint = "https://remote.example:9443".to_string();
        let client = target_client_for_test(
            arn,
            endpoint.clone(),
            S3Client::from_conf(
                S3Config::builder()
                    .endpoint_url(endpoint)
                    .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
                    .build(),
            ),
        );
        sys.arn_remotes_map
            .write()
            .await
            .insert(arn.to_string(), ArnTarget::with_client(client.clone()));
        sys.init_target_health(&client).await;
        sys.mark_target_offline(&client).await;

        sys.targets_map.write().await.insert(
            "bucket".to_string(),
            vec![BucketTarget {
                endpoint: "remote.example:9443".to_string(),
                arn: arn.to_string(),
                target_type: BucketTargetType::ReplicationService,
                ..Default::default()
            }],
        );

        let targets = sys.list_targets("", "").await.expect("listing every bucket's targets");

        assert_eq!(targets.len(), 1);
        assert!(!targets[0].online);
        assert_eq!(targets[0].offline_count, 1);
        assert_eq!(sys.target_health_stats().await[arn].endpoint, "remote.example:9443");
    }

    #[tokio::test]
    async fn target_health_is_isolated_by_arn_for_shared_endpoint() {
        let sys = BucketTargetSys::default();
        let endpoint = "https://shared.example:9443".to_string();
        let config = || {
            S3Config::builder()
                .endpoint_url(endpoint.clone())
                .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
                .build()
        };
        let first = target_client_for_test("arn:first", endpoint.clone(), S3Client::from_conf(config()));
        let second = target_client_for_test("arn:second", endpoint.clone(), S3Client::from_conf(config()));

        sys.arn_remotes_map
            .write()
            .await
            .insert(first.arn.clone(), ArnTarget::with_client(first.clone()));
        sys.arn_remotes_map
            .write()
            .await
            .insert(second.arn.clone(), ArnTarget::with_client(second.clone()));
        sys.init_target_health(&first).await;
        sys.init_target_health(&second).await;
        sys.mark_target_offline(&first).await;

        assert!(sys.is_target_offline(&first).await);
        assert!(!sys.is_target_offline(&second).await);
    }

    #[tokio::test]
    async fn stale_client_cannot_change_replacement_health_for_same_arn() {
        let sys = BucketTargetSys::default();
        let arn = "arn:replacement";
        let config = |endpoint: &str| {
            S3Config::builder()
                .endpoint_url(endpoint)
                .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
                .build()
        };
        let stale = target_client_for_test(
            arn,
            "https://stale.example:9443".to_string(),
            S3Client::from_conf(config("https://stale.example:9443")),
        );
        let current = target_client_for_test(
            arn,
            "https://current.example:9443".to_string(),
            S3Client::from_conf(config("https://current.example:9443")),
        );
        sys.arn_remotes_map
            .write()
            .await
            .insert(arn.to_string(), ArnTarget::with_client(current.clone()));
        sys.init_target_health(&current).await;

        sys.mark_target_offline(&stale).await;

        assert!(sys.is_target_offline(&stale).await);
        assert!(!sys.is_target_offline(&current).await);
    }

    #[tokio::test]
    async fn delete_removes_target_health_by_arn() {
        let sys = BucketTargetSys::default();
        let arn = "arn:delete";
        let endpoint = "https://delete.example:9443".to_string();
        let client = target_client_for_test(
            arn,
            endpoint.clone(),
            S3Client::from_conf(
                S3Config::builder()
                    .endpoint_url(endpoint)
                    .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
                    .build(),
            ),
        );
        sys.targets_map.write().await.insert(
            "bucket".to_string(),
            vec![BucketTarget {
                arn: arn.to_string(),
                ..Default::default()
            }],
        );
        sys.init_target_health(&client).await;

        sys.delete("bucket").await;

        assert!(!sys.target_health_stats().await.contains_key(arn));
    }

    #[test]
    fn build_remove_object_headers_includes_internal_version_id_for_replication_delete() {
        let version_id = Uuid::new_v4().to_string();
        let headers = build_remove_object_headers(
            Some(version_id.as_str()),
            &RemoveObjectOptions {
                force_delete: false,
                governance_bypass: false,
                replication_delete_marker: true,
                replication_mtime: None,
                replication_status: ReplicationStatusType::Replica,
                replication_request: true,
                replication_validity_check: false,
            },
        );

        assert_eq!(
            rustfs_utils::http::get_header(&headers, SUFFIX_SOURCE_VERSION_ID).as_deref(),
            Some(version_id.as_str()),
            "replication delete requests must preserve the version id in internal headers"
        );
    }

    #[test]
    fn build_remove_object_headers_omits_delete_marker_flag_for_marker_version_purge() {
        let version_id = Uuid::new_v4().to_string();
        let headers = build_remove_object_headers(
            Some(version_id.as_str()),
            &RemoveObjectOptions {
                force_delete: false,
                governance_bypass: false,
                replication_delete_marker: false,
                replication_mtime: None,
                replication_status: ReplicationStatusType::Replica,
                replication_request: true,
                replication_validity_check: false,
            },
        );

        assert!(
            rustfs_utils::http::get_header(&headers, SUFFIX_SOURCE_DELETEMARKER).is_none(),
            "delete-marker version purges must not masquerade as delete-marker creations"
        );
    }

    fn remove_opts(replication_request: bool, replication_delete_marker: bool) -> RemoveObjectOptions {
        RemoveObjectOptions {
            force_delete: false,
            governance_bypass: false,
            replication_delete_marker,
            replication_mtime: None,
            replication_status: ReplicationStatusType::Replica,
            replication_request,
            replication_validity_check: false,
        }
    }

    #[test]
    fn version_purge_sends_versionid_query_param_to_generic_target() {
        // A replication VERSION PURGE (delete_marker=false) must carry the S3
        // `?versionId=` query param so a generic S3 target removes that exact
        // version instead of silently creating a delete marker (backlog#799 B8).
        let vid = Uuid::new_v4().to_string();
        let got = resolve_delete_api_version_id(Some(vid.clone()), &remove_opts(true, false));
        assert_eq!(got.as_deref(), Some(vid.as_str()));
    }

    #[test]
    fn delete_marker_propagation_omits_versionid_query_param() {
        // Propagating a delete-marker CREATION (delete_marker=true): the target
        // must mint its own marker, so no `versionId` query param is sent.
        let vid = Uuid::new_v4().to_string();
        let got = resolve_delete_api_version_id(Some(vid), &remove_opts(true, true));
        assert_eq!(got, None);
    }

    #[test]
    fn non_replication_delete_passes_version_through() {
        let vid = Uuid::new_v4().to_string();
        let got = resolve_delete_api_version_id(Some(vid.clone()), &remove_opts(false, false));
        assert_eq!(got.as_deref(), Some(vid.as_str()));
    }

    #[test]
    fn delete_marker_purge_addresses_exact_version() {
        // Purging a specific delete-marker version on the target
        // (replication_delete_marker_purge_remove_options → delete_marker=false)
        // must target that version, not degenerate to a new marker.
        let vid = Uuid::new_v4().to_string();
        let got = resolve_delete_api_version_id(Some(vid.clone()), &remove_opts(true, false));
        assert_eq!(got.as_deref(), Some(vid.as_str()));
    }

    #[tokio::test]
    async fn remove_object_writes_null_purge_and_omits_marker_creation_version_queries() {
        let (client, request_uris) = recording_target_client();
        client
            .remove_object("target-bucket", "object", Some("null".to_string()), remove_opts(true, false))
            .await
            .expect("explicit null version purge should reach the target client");
        client
            .remove_object("target-bucket", "object", Some(Uuid::new_v4().to_string()), remove_opts(true, true))
            .await
            .expect("delete marker creation should reach the target client");

        let request_uris = request_uris.lock().expect("recorded request lock should not be poisoned");
        assert_eq!(request_uris.len(), 2);
        assert!(
            request_uris[0].contains("versionId=null"),
            "an explicit null purge must be emitted as a target versionId query: {}",
            request_uris[0]
        );
        assert!(
            !request_uris[1].contains("versionId="),
            "delete marker creation must omit the target versionId query: {}",
            request_uris[1]
        );
    }

    #[tokio::test]
    async fn put_object_sends_source_version_id_query_to_target() {
        // MinIO reads the replicated version only from the `versionId` query
        // parameter (its receive path ignores the x-*-source-version-id
        // headers), so the query must carry the source version: a real UUID
        // as-is, the internal nil-UUID null-version representation as the
        // literal "null", and no query at all when the source object has no
        // version (P0-5 RustFS->MinIO version drift).
        let (client, request_uris) = recording_target_client();
        let version_id = Uuid::new_v4().to_string();
        let nil_version = Uuid::nil().to_string();
        for source_version in [version_id.as_str(), nil_version.as_str(), ""] {
            let mut opts = PutObjectOptions::default();
            opts.internal.source_version_id = source_version.to_string();
            opts.internal.replication_request = true;
            client
                .put_object("target-bucket", "object", 4, ByteStream::from_static(b"data"), &opts)
                .await
                .expect("recorded put_object should succeed");
        }

        let request_uris = request_uris.lock().expect("recorded request lock should not be poisoned");
        assert_eq!(request_uris.len(), 3);
        assert!(
            request_uris[0].contains(&format!("versionId={version_id}")),
            "replication put_object must carry the source version as a versionId query: {}",
            request_uris[0]
        );
        assert!(
            request_uris[1].contains("versionId=null"),
            "a nil-UUID (null) source version must be sent as the literal null: {}",
            request_uris[1]
        );
        assert!(
            !request_uris[2].contains("versionId="),
            "put_object without a source version must omit the versionId query: {}",
            request_uris[2]
        );
    }

    #[tokio::test]
    async fn create_multipart_upload_sends_source_version_id_query_to_target() {
        // The remote version of a multipart replication is decided at initiate
        // time: CreateMultipartUpload must carry the source version in the
        // `versionId` query (CompleteMultipartUpload does not read one).
        let (client, request_uris) = recording_target_client();
        let version_id = Uuid::new_v4().to_string();
        let nil_version = Uuid::nil().to_string();
        for source_version in [version_id.as_str(), nil_version.as_str()] {
            let mut opts = PutObjectOptions::default();
            opts.internal.source_version_id = source_version.to_string();
            opts.internal.replication_request = true;
            let _ = client.create_multipart_upload("target-bucket", "object", &opts).await;
        }

        let request_uris = request_uris.lock().expect("recorded request lock should not be poisoned");
        assert_eq!(request_uris.len(), 2);
        assert!(
            request_uris[0].contains(&format!("versionId={version_id}")),
            "replication create_multipart_upload must carry the source version as a versionId query: {}",
            request_uris[0]
        );
        assert!(
            request_uris[1].contains("versionId=null"),
            "a nil-UUID (null) source version must be sent as the literal null: {}",
            request_uris[1]
        );
    }

    #[test]
    fn put_object_headers_keep_source_version_id_for_legacy_receivers() {
        // Older RustFS receivers have no versionId query support and fall back
        // to the internal source-version-id headers (rolling-upgrade path);
        // the query addition must never remove them.
        let mut opts = PutObjectOptions::default();
        let version_id = Uuid::new_v4().to_string();
        opts.internal.source_version_id = version_id.clone();

        assert_eq!(
            rustfs_utils::http::get_header(&opts.header(), SUFFIX_SOURCE_VERSION_ID).as_deref(),
            Some(version_id.as_str()),
            "replication put requests must keep the internal source-version-id headers"
        );
    }

    #[test]
    fn put_object_headers_include_non_empty_source_etag_only() {
        let mut opts = PutObjectOptions::default();

        assert!(
            rustfs_utils::http::get_header(&opts.header(), SUFFIX_SOURCE_ETAG).is_none(),
            "empty source etag must not be sent to replication targets"
        );

        opts.internal.source_etag = "etag-1".to_string();

        assert_eq!(
            rustfs_utils::http::get_header(&opts.header(), SUFFIX_SOURCE_ETAG).as_deref(),
            Some("etag-1"),
            "replication targets need the source etag for idempotency checks"
        );
    }

    #[test]
    fn put_object_headers_carry_replication_timestamp_headers() {
        // MinIO receivers resolve concurrent tag/retention/legal-hold edits by
        // last-writer-wins on these headers (object-api-options.go parses them
        // as RFC3339); a replica without them loses every conflict resolution.
        let mut opts = PutObjectOptions::default();
        opts.internal.replication_request = true;
        let tagging = OffsetDateTime::from_unix_timestamp(1_700_000_001).expect("valid timestamp");
        let retention = OffsetDateTime::from_unix_timestamp(1_700_000_002).expect("valid timestamp");
        let legalhold = OffsetDateTime::from_unix_timestamp(1_700_000_003).expect("valid timestamp");
        opts.internal.tagging_timestamp = tagging;
        opts.internal.retention_timestamp = retention;
        opts.internal.legalhold_timestamp = legalhold;

        let header = opts.header();
        for (suffix, expected) in [
            ("source-replication-tagging-timestamp", tagging),
            ("source-replication-retention-timestamp", retention),
            ("source-replication-legalhold-timestamp", legalhold),
        ] {
            assert_eq!(
                rustfs_utils::http::get_header(&header, suffix).as_deref(),
                Some(expected.format(&Rfc3339).expect("RFC3339 timestamp").as_str()),
                "replication put requests must carry the {suffix} header"
            );
        }
    }

    #[test]
    fn put_object_headers_carry_user_tags_on_x_amz_tagging() {
        // rustfs/backlog#1953: tag edits replicate through the whole-object
        // transport, so the source tags must travel on x-amz-tagging.
        let mut opts = PutObjectOptions::default();
        opts.user_tags.insert("owner".to_string(), "site a".to_string());
        opts.user_tags.insert("env".to_string(), "prod".to_string());

        let header = opts.header();
        let tagging = header
            .get(AMZ_OBJECT_TAGGING_LOWER)
            .expect("user tags must be transported on x-amz-tagging")
            .to_str()
            .expect("tag header must be ASCII");
        // Deterministic key order; values are form-urlencoded.
        assert_eq!(tagging, "env=prod&owner=site+a");

        assert!(
            PutObjectOptions::default().header().get(AMZ_OBJECT_TAGGING_LOWER).is_none(),
            "a tagless source must not send an empty x-amz-tagging header"
        );
    }

    #[test]
    fn put_object_headers_omit_unset_replication_timestamps() {
        // UNIX_EPOCH means "never modified on the source"; sending it would
        // make the receiver treat an unset category as a fresh modification.
        let mut opts = PutObjectOptions::default();
        opts.internal.replication_request = true;
        opts.internal.tagging_timestamp = OffsetDateTime::UNIX_EPOCH;
        opts.internal.retention_timestamp = OffsetDateTime::UNIX_EPOCH;
        opts.internal.legalhold_timestamp = OffsetDateTime::UNIX_EPOCH;

        let header = opts.header();
        for suffix in [
            "source-replication-tagging-timestamp",
            "source-replication-retention-timestamp",
            "source-replication-legalhold-timestamp",
        ] {
            assert!(
                rustfs_utils::http::get_header(&header, suffix).is_none(),
                "unset {suffix} must not be sent to replication targets"
            );
        }
    }

    #[tokio::test]
    async fn get_remote_target_client_internal_rejects_loopback_endpoint() {
        let sys = BucketTargetSys::default();
        let err = sys
            .get_remote_target_client_internal(&BucketTarget {
                endpoint: "127.0.0.1:9000".to_string(),
                secure: true,
                target_bucket: "bucket".to_string(),
                region: "us-east-1".to_string(),
                credentials: Some(Credentials {
                    access_key: "access".to_string(),
                    secret_key: "secret".to_string(),
                    session_token: None,
                    expiration: None,
                }),
                ..Default::default()
            })
            .await
            .expect_err("loopback endpoint should be rejected");

        assert!(err.to_string().contains("not allowed"));
    }

    #[tokio::test]
    async fn get_remote_target_client_internal_allows_private_ip_endpoint() {
        let sys = BucketTargetSys::default();
        let client = sys
            .get_remote_target_client_internal(&BucketTarget {
                endpoint: "192.168.1.10:9000".to_string(),
                secure: true,
                skip_tls_verify: true,
                target_bucket: "bucket".to_string(),
                region: "us-east-1".to_string(),
                credentials: Some(Credentials {
                    access_key: "access".to_string(),
                    secret_key: "secret".to_string(),
                    session_token: None,
                    expiration: None,
                }),
                ..Default::default()
            })
            .await
            .expect("private IP endpoints should be allowed for replication targets");

        assert_eq!(client.endpoint, "https://192.168.1.10:9000");
    }

    #[tokio::test]
    async fn get_remote_target_client_internal_allows_custom_ca_pem() {
        let sys = BucketTargetSys::default();
        let cert = generate_simple_self_signed(vec!["192.168.1.10".to_string()]).expect("certificate should generate");
        let client = sys
            .get_remote_target_client_internal(&BucketTarget {
                endpoint: "192.168.1.10:9000".to_string(),
                secure: true,
                target_bucket: "bucket".to_string(),
                region: "us-east-1".to_string(),
                ca_cert_pem: cert.cert.pem(),
                credentials: Some(Credentials {
                    access_key: "access".to_string(),
                    secret_key: "secret".to_string(),
                    session_token: None,
                    expiration: None,
                }),
                ..Default::default()
            })
            .await
            .expect("custom CA PEM should build a target client");

        assert_eq!(client.endpoint, "https://192.168.1.10:9000");
    }

    #[tokio::test]
    async fn target_health_check_rejects_untrusted_self_signed_certificate() {
        let cert = generate_simple_self_signed(vec!["localhost".to_string()]).expect("certificate should generate");
        let (port, server) = spawn_https_server(&cert, 1);
        let target =
            target_client_for_test("arn:default-tls", format!("https://localhost:{port}"), s3_client_for_test(port, None));

        assert!(!BucketTargetSys::check_endpoint_health(&target).await);
        server.join().expect("test TLS server should stop");
    }

    #[tokio::test]
    async fn target_health_check_honors_skip_tls_verify_client() {
        let cert = generate_simple_self_signed(vec!["localhost".to_string()]).expect("certificate should generate");
        let (port, server) = spawn_https_server(&cert, 1);
        let target = target_client_for_test(
            "arn:skip-tls",
            format!("https://localhost:{port}"),
            s3_client_for_test(port, Some(build_insecure_aws_s3_http_client())),
        );

        assert!(BucketTargetSys::check_endpoint_health(&target).await);
        server.join().expect("test TLS server should stop");
    }

    #[tokio::test]
    async fn target_health_check_honors_custom_ca_client() {
        let cert = generate_simple_self_signed(vec!["localhost".to_string()]).expect("certificate should generate");
        let http_client = build_aws_s3_http_client_from_target_ca_pem(&cert.cert.pem())
            .await
            .expect("custom CA client should build");
        let (port, server) = spawn_https_server(&cert, 1);
        let target = target_client_for_test(
            "arn:custom-ca",
            format!("https://localhost:{port}"),
            s3_client_for_test(port, Some(http_client)),
        );

        assert!(BucketTargetSys::check_endpoint_health(&target).await);
        server.join().expect("test TLS server should stop");
    }

    #[tokio::test]
    async fn target_health_check_treats_client_errors_as_online_and_server_errors_as_offline() {
        for (status, expected_online) in [(403, true), (500, false)] {
            let (port, server) = spawn_http_status_server(status);
            let endpoint = format!("http://127.0.0.1:{port}");
            let target = target_client_for_test(
                &format!("arn:http-{status}"),
                endpoint.clone(),
                s3_client_for_endpoint_test(endpoint, None),
            );

            assert_eq!(BucketTargetSys::check_endpoint_health(&target).await, expected_online);
            server.join().expect("test HTTP server should stop");
        }
    }

    #[tokio::test]
    async fn heartbeat_keeps_tls_health_isolated_by_arn_for_shared_endpoint() {
        let sys = BucketTargetSys::default();
        let cert = generate_simple_self_signed(vec!["localhost".to_string()]).expect("certificate should generate");
        let (port, server) = spawn_https_server(&cert, 2);
        let endpoint = format!("https://localhost:{port}");
        let strict = target_client_for_test("arn:strict", endpoint.clone(), s3_client_for_test(port, None));
        let insecure = target_client_for_test(
            "arn:insecure",
            endpoint,
            s3_client_for_test(port, Some(build_insecure_aws_s3_http_client())),
        );
        {
            let mut remotes = sys.arn_remotes_map.write().await;
            remotes.insert(strict.arn.clone(), ArnTarget::with_client(strict.clone()));
            remotes.insert(insecure.arn.clone(), ArnTarget::with_client(insecure.clone()));
        }

        sys.heartbeat_once().await;

        assert!(sys.is_target_offline(&strict).await);
        assert!(!sys.is_target_offline(&insecure).await);
        server.join().expect("test TLS server should stop");
    }

    #[tokio::test]
    async fn heartbeat_discards_result_from_replaced_client_with_same_arn() {
        let sys = Arc::new(BucketTargetSys::default());
        let (port, accepted, release, server) = spawn_delayed_http_server();
        let endpoint = format!("http://127.0.0.1:{port}");
        let stale = target_client_for_test("arn:replacement", endpoint.clone(), s3_client_for_endpoint_test(endpoint, None));
        sys.arn_remotes_map
            .write()
            .await
            .insert(stale.arn.clone(), ArnTarget::with_client(stale));
        let heartbeat_sys = sys.clone();
        let heartbeat = tokio::spawn(async move { heartbeat_sys.heartbeat_once().await });
        accepted.await.expect("heartbeat request should reach test server");

        let replacement_endpoint = "https://replacement.example:9443".to_string();
        let replacement = target_client_for_test(
            "arn:replacement",
            replacement_endpoint.clone(),
            S3Client::from_conf(
                S3Config::builder()
                    .endpoint_url(replacement_endpoint)
                    .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
                    .build(),
            ),
        );
        sys.arn_remotes_map
            .write()
            .await
            .insert(replacement.arn.clone(), ArnTarget::with_client(replacement.clone()));
        sys.init_target_health(&replacement).await;
        release.send(()).expect("stale heartbeat response should be released");
        heartbeat.await.expect("heartbeat should finish");

        assert!(!sys.is_target_offline(&replacement).await);
        server.join().expect("test HTTP server should stop");
    }

    #[tokio::test]
    async fn target_update_mutex_reuses_live_lock_and_reclaims_dead_entries() {
        let sys = BucketTargetSys::default();
        let first = sys.target_update_mutex("first").await;
        let same = sys.target_update_mutex("first").await;
        assert!(Arc::ptr_eq(&first, &same));
        drop(first);
        drop(same);

        let _second = sys.target_update_mutex("second").await;
        let mutexes = sys.target_update_mutexes.lock().await;
        assert!(!mutexes.contains_key("first"));
        assert!(mutexes.contains_key("second"));
    }

    #[tokio::test]
    async fn target_refresh_attempt_updates_retry_timestamp_and_error_count() {
        let sys = BucketTargetSys::default();

        sys.mark_refresh_attempt("arn:reload").await;
        let last_refresh = sys.arn_remotes_map.read().await["arn:reload"].last_refresh;
        assert!(OffsetDateTime::now_utc() - last_refresh < Duration::from_secs(5));

        sys.inc_arn_errs("bucket", "arn:reload").await;
        sys.inc_arn_errs("bucket", "arn:reload").await;
        let errors = sys.arn_errs_map.read().await;
        assert_eq!(errors["arn:reload"].count, 2);
        assert_eq!(errors["arn:reload"].bucket, "bucket");
        drop(errors);

        sys.mark_refresh_in_progress("bucket", "arn:reload").await;
        assert!(sys.is_reloading_target("bucket", "arn:reload").await);
        sys.mark_refresh_done("bucket", "arn:reload").await;
        assert!(!sys.is_reloading_target("bucket", "arn:reload").await);
        sys.mark_refresh_in_progress("bucket", "arn:reload").await;
        assert!(sys.is_reloading_target("bucket", "arn:reload").await);
    }

    #[tokio::test]
    async fn update_all_targets_publishes_disable_proxy_on_target_client() {
        // The read-proxy selector (replication_proxy::get_proxy_targets) skips
        // targets whose TargetClient carries disable_proxy — the persisted
        // per-target opt-out must survive client publication.
        let sys = BucketTargetSys::default();
        let target = |arn: &str, disable_proxy: bool| BucketTarget {
            arn: arn.to_string(),
            endpoint: "192.168.1.10:9000".to_string(),
            target_bucket: "target-bucket".to_string(),
            region: "us-east-1".to_string(),
            disable_proxy,
            credentials: Some(Credentials {
                access_key: "access".to_string(),
                secret_key: "secret".to_string(),
                session_token: None,
                expiration: None,
            }),
            ..Default::default()
        };
        let targets = BucketTargets {
            targets: vec![target("arn:proxied", false), target("arn:opted-out", true)],
        };

        sys.update_all_targets("bucket", Some(&targets)).await;

        let proxied = sys
            .get_remote_target_client("bucket", "arn:proxied")
            .await
            .expect("client should be published");
        assert!(!proxied.disable_proxy);
        let opted_out = sys
            .get_remote_target_client("bucket", "arn:opted-out")
            .await
            .expect("client should be published");
        assert!(opted_out.disable_proxy, "disable_proxy must reach the published TargetClient");
    }

    #[tokio::test]
    async fn update_all_targets_keeps_failed_client_placeholder() {
        let sys = BucketTargetSys::default();
        let target = BucketTarget {
            arn: "arn:expired".to_string(),
            endpoint: "192.168.1.10:9000".to_string(),
            target_bucket: "target-bucket".to_string(),
            region: "us-east-1".to_string(),
            credentials: Some(Credentials {
                access_key: "access".to_string(),
                secret_key: "secret".to_string(),
                session_token: Some("temporary-session-token".to_string()),
                expiration: Some("2000-01-01T00:00:00Z".parse().expect("expired timestamp should parse")),
            }),
            ..Default::default()
        };
        let targets = BucketTargets { targets: vec![target] };

        sys.update_all_targets("bucket", Some(&targets)).await;

        let remotes = sys.arn_remotes_map.read().await;
        let placeholder = remotes
            .get("arn:expired")
            .expect("configured target should retain a cache entry");
        assert!(placeholder.client.is_none());
        assert!(OffsetDateTime::now_utc() - placeholder.last_refresh < Duration::from_secs(5));
        drop(remotes);
        assert!(sys.get_remote_target_client("bucket", "arn:expired").await.is_none());
    }

    #[tokio::test]
    async fn credential_rotation_atomically_replaces_published_client() {
        let sys = BucketTargetSys::default();
        let target = |session_token: &str| BucketTarget {
            arn: "arn:rotating".to_string(),
            endpoint: "192.168.1.10:9000".to_string(),
            target_bucket: "target-bucket".to_string(),
            region: "us-east-1".to_string(),
            credentials: Some(Credentials {
                access_key: "access".to_string(),
                secret_key: "secret".to_string(),
                session_token: Some(session_token.to_string()),
                expiration: None,
            }),
            ..Default::default()
        };

        sys.update_all_targets(
            "bucket",
            Some(&BucketTargets {
                targets: vec![target("old-session-token")],
            }),
        )
        .await;
        let old_client = sys
            .get_remote_target_client("bucket", "arn:rotating")
            .await
            .expect("initial client should be published");

        sys.update_all_targets(
            "bucket",
            Some(&BucketTargets {
                targets: vec![target("new-session-token")],
            }),
        )
        .await;
        let new_client = sys
            .get_remote_target_client("bucket", "arn:rotating")
            .await
            .expect("rotated client should be published");

        assert!(!Arc::ptr_eq(&old_client, &new_client));
        assert_eq!(
            old_client.credentials.as_ref().and_then(Credentials::effective_session_token),
            Some("old-session-token")
        );
        assert_eq!(
            new_client.credentials.as_ref().and_then(Credentials::effective_session_token),
            Some("new-session-token")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn target_updates_serialize_client_build_through_publication_per_bucket() {
        let sys = Arc::new(BucketTargetSys::default());
        let started = Arc::new(tokio::sync::Semaphore::new(0));
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        *sys.target_client_build_probe.lock().await = Some(TargetClientBuildProbe {
            arn: "arn:first".to_string(),
            started: started.clone(),
            release: release.clone(),
        });
        let target = |arn: &str| BucketTarget {
            arn: arn.to_string(),
            endpoint: "192.168.1.10:9000".to_string(),
            target_bucket: "target-bucket".to_string(),
            region: "us-east-1".to_string(),
            credentials: Some(Credentials {
                access_key: "access".to_string(),
                secret_key: "secret".to_string(),
                session_token: None,
                expiration: None,
            }),
            ..Default::default()
        };
        let first_targets = BucketTargets {
            targets: vec![target("arn:first")],
        };
        let second_targets = BucketTargets {
            targets: vec![target("arn:second")],
        };
        let first_sys = sys.clone();
        let first = tokio::spawn(async move {
            first_sys.update_all_targets("bucket", Some(&first_targets)).await;
        });
        tokio::time::timeout(Duration::from_secs(2), started.acquire())
            .await
            .expect("first client build should start")
            .expect("first started semaphore should remain open")
            .forget();
        let second_started = Arc::new(tokio::sync::Semaphore::new(0));
        let second_release = Arc::new(tokio::sync::Semaphore::new(0));
        *sys.target_client_build_probe.lock().await = Some(TargetClientBuildProbe {
            arn: "arn:second".to_string(),
            started: second_started.clone(),
            release: second_release.clone(),
        });
        let second_sys = sys.clone();
        let second = tokio::spawn(async move {
            second_sys.update_all_targets("bucket", Some(&second_targets)).await;
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(50), second_started.acquire())
                .await
                .is_err()
        );
        assert!(!sys.targets_map.read().await.contains_key("bucket"));

        release.add_permits(1);
        tokio::time::timeout(Duration::from_secs(2), first)
            .await
            .expect("first target update should not stall")
            .expect("first target update should finish");
        tokio::time::timeout(Duration::from_secs(1), second_started.acquire())
            .await
            .expect("second client build should start after first update publishes")
            .expect("second started semaphore should remain open")
            .forget();
        second_release.add_permits(1);
        tokio::time::timeout(Duration::from_secs(2), second)
            .await
            .expect("second target update should not stall")
            .expect("second target update should finish");
        let targets = sys.targets_map.read().await;
        assert_eq!(targets["bucket"][0].arn, "arn:second");
    }

    #[tokio::test]
    async fn replication_trust_store_composes_system_global_and_target_roots_for_real_tls() {
        let tls_dir = tempfile::tempdir().expect("temporary TLS directory should be created");
        let global_ca =
            generate_simple_self_signed(vec!["localhost".to_string()]).expect("global CA certificate should generate");
        let target_ca =
            generate_simple_self_signed(vec!["localhost".to_string()]).expect("target CA certificate should generate");

        tokio::fs::write(tls_dir.path().join(RUSTFS_CA_CERT), global_ca.cert.pem())
            .await
            .expect("global CA bundle should be written");

        let mut certificate_bundles = load_tls_path_ca_bundles(tls_dir.path(), false).await;
        certificate_bundles.push(target_ca.cert.pem().into_bytes());
        let (trust_store, _) = compose_replication_trust_store(certificate_bundles);
        assert!(
            format!("{trust_store:?}").contains("enable_native_roots: true"),
            "per-target trust must retain the SDK's platform-native roots"
        );
        let http_client = build_aws_s3_http_client_with_trust_store(trust_store).expect("composed TLS client should build");

        let (global_port, global_server) = spawn_https_server(&global_ca, 1);
        s3_client_for_test(global_port, Some(http_client.clone()))
            .head_bucket()
            .bucket("test-bucket")
            .send()
            .await
            .expect("global RUSTFS_TLS_PATH CA should authenticate its TLS server");
        global_server.join().expect("global CA TLS server should finish");

        let (target_port, target_server) = spawn_https_server(&target_ca, 1);
        s3_client_for_test(target_port, Some(http_client))
            .head_bucket()
            .bucket("test-bucket")
            .send()
            .await
            .expect("per-target CA should authenticate its TLS server alongside global roots");
        target_server.join().expect("target CA TLS server should finish");
    }

    #[tokio::test]
    async fn tls_path_leaf_trust_remains_opt_in() {
        let tls_dir = tempfile::tempdir().expect("temporary TLS directory should be created");
        let global_ca =
            generate_simple_self_signed(vec!["global-ca.example".to_string()]).expect("global CA certificate should generate");
        let trusted_leaf =
            generate_simple_self_signed(vec!["leaf.example".to_string()]).expect("trusted leaf certificate should generate");
        tokio::fs::write(tls_dir.path().join(RUSTFS_CA_CERT), global_ca.cert.pem())
            .await
            .expect("global CA bundle should be written");
        tokio::fs::write(tls_dir.path().join(RUSTFS_TLS_CERT), trusted_leaf.cert.pem())
            .await
            .expect("trusted leaf certificate should be written");

        assert_eq!(load_tls_path_ca_bundles(tls_dir.path(), true).await.len(), 2);
        assert_eq!(load_tls_path_ca_bundles(tls_dir.path(), false).await.len(), 1);
    }

    #[tokio::test]
    async fn skip_tls_verify_takes_priority_over_invalid_custom_ca_pem() {
        let client = build_aws_s3_http_client_for_spec(&RemoteS3EndpointSpec::from(&BucketTarget {
            secure: true,
            skip_tls_verify: true,
            ca_cert_pem: "not a pem".to_string(),
            ..Default::default()
        }))
        .await
        .expect("skip verification should bypass custom CA parsing");

        assert!(client.is_some(), "secure targets with skip verification need a custom HTTP client");
    }

    #[tokio::test]
    async fn get_remote_target_client_internal_rejects_invalid_custom_ca_pem() {
        let sys = BucketTargetSys::default();
        let err = sys
            .get_remote_target_client_internal(&BucketTarget {
                endpoint: "192.168.1.10:9000".to_string(),
                secure: true,
                target_bucket: "bucket".to_string(),
                region: "us-east-1".to_string(),
                ca_cert_pem: "not a pem".to_string(),
                credentials: Some(Credentials {
                    access_key: "access".to_string(),
                    secret_key: "secret".to_string(),
                    session_token: None,
                    expiration: None,
                }),
                ..Default::default()
            })
            .await
            .expect_err("invalid custom CA PEM should be rejected");

        assert!(err.to_string().contains("invalid target CA PEM"));
    }

    #[test]
    fn target_ca_rejects_pem_wrapped_invalid_der_before_smithy_builds() {
        let err = validate_target_ca_pem("-----BEGIN CERTIFICATE-----\nAQID\n-----END CERTIFICATE-----\n")
            .expect_err("PEM-wrapped invalid DER must be rejected");

        assert!(err.to_string().contains("invalid target CA PEM"));
        assert!(err.to_string().contains("invalid X.509 certificate"));
    }

    #[tokio::test]
    async fn invalid_global_ca_is_ignored_without_reaching_smithy() {
        let tls_dir = tempfile::tempdir().expect("temporary TLS directory should be created");
        tokio::fs::write(
            tls_dir.path().join(RUSTFS_CA_CERT),
            b"-----BEGIN CERTIFICATE-----\nAQID\n-----END CERTIFICATE-----\n",
        )
        .await
        .expect("invalid global CA fixture should be written");

        assert!(
            load_tls_path_ca_bundles(tls_dir.path(), false).await.is_empty(),
            "invalid global CA must fall back to default roots instead of reaching Smithy's panic path"
        );
    }

    // backlog#806-16 regression tests for the rolling one-minute latency window.

    #[test]
    fn last_minute_latency_averages_only_samples_within_window() {
        let base = Instant::now();
        let mut window = LastMinuteLatency::new();

        // Two samples that will fall outside the 60s window once the fresh
        // sample is added, plus one fresh sample.
        window.add_at(base, Duration::from_millis(100));
        window.add_at(base + Duration::from_secs(10), Duration::from_millis(200));
        // 61s after `base`: both earlier samples are now >60s old relative to
        // the 200ms sample? No — measured against `now`. Add a fresh sample far
        // in the future so the two old samples age out.
        window.add_at(base + Duration::from_secs(61), Duration::from_millis(400));

        // Only the fresh sample survives: 100ms (age 61s) and 200ms (age 51s)
        // vs the fresh one — 51s is still < 60s, so the 200ms sample stays.
        // Assert the window kept exactly the in-window samples.
        assert_eq!(window.get_total().avg, Duration::from_millis(300)); // (200 + 400) / 2
    }

    #[test]
    fn last_minute_latency_drops_all_stale_samples() {
        let base = Instant::now();
        let mut window = LastMinuteLatency::new();

        // Two stale samples, then one sample far enough in the future that both
        // are strictly older than 60s.
        window.add_at(base, Duration::from_millis(100));
        window.add_at(base + Duration::from_secs(5), Duration::from_millis(300));
        window.add_at(base + Duration::from_secs(120), Duration::from_millis(500));

        // Both old samples aged out (>=60s); only the fresh 500ms remains. Under
        // the OLD all-or-nothing bug the result would still have been the last
        // single sample by coincidence, so also verify the mixed-window case below.
        assert_eq!(window.get_total().avg, Duration::from_millis(500));
    }

    #[test]
    fn last_minute_latency_two_fresh_samples_average_both() {
        let base = Instant::now();
        let mut window = LastMinuteLatency::new();

        window.add_at(base, Duration::from_millis(100));
        window.add_at(base + Duration::from_secs(1), Duration::from_millis(300));

        // Both within the window -> average of both. The OLD bug degenerated the
        // window to a single sample after 60s; here samples are close in time so
        // the correct behaviour is a genuine two-sample average.
        assert_eq!(window.get_total().avg, Duration::from_millis(200));
    }

    #[test]
    fn last_minute_latency_empty_window_is_zero() {
        let window = LastMinuteLatency::new();
        assert_eq!(window.get_total().avg, Duration::from_secs(0));
    }

    fn repair_target(bucket: &str, id: &str) -> BucketTarget {
        BucketTarget {
            source_bucket: bucket.to_string(),
            endpoint: "remote.example.com".to_string(),
            target_bucket: "remote".to_string(),
            arn: format!("arn:rustfs:replication:us-east-1:{bucket}:{id}"),
            target_type: BucketTargetType::ReplicationService,
            region: "us-east-1".to_string(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn an_unreadable_target_set_refuses_cached_writes() {
        let sys = BucketTargetSys::default();
        let bucket = "targets-repair-opt-in";
        sys.mark_targets_unreadable(bucket).await;
        assert!(matches!(
            sys.targets_base_for_write(bucket).await,
            Err(BucketTargetError::BucketRemoteTargetsUnreadable { .. })
        ));
    }

    #[tokio::test]
    async fn a_readable_target_set_remains_the_write_base() {
        let sys = BucketTargetSys::default();
        let bucket = "targets-repair-readable";
        let existing = repair_target(bucket, "keep");
        sys.targets_map
            .write()
            .await
            .insert(bucket.to_string(), vec![existing.clone()]);
        let base = sys.targets_base_for_write(bucket).await.expect("read targets");
        assert_eq!(base.targets.len(), 1);
        assert_eq!(base.targets[0].arn, existing.arn);
    }
}
