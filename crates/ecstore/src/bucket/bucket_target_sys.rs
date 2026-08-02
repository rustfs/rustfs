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
use crate::bucket::replication::{ReplicationStatusType, ReplicationTargetConfigBridge};
use crate::bucket::target::ARN;
use crate::bucket::target::BucketTargetType;
use crate::bucket::target::{self, BucketTarget, BucketTargets, Credentials};
use crate::bucket::versioning_sys::BucketVersioningSys;
use crate::runtime::sources as runtime_sources;
use aws_credential_types::Credentials as SdkCredentials;
use aws_sdk_s3::config::Region as SdkRegion;
use aws_sdk_s3::config::SharedHttpClient;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput;
use aws_sdk_s3::operation::delete_object::DeleteObjectOutput;
use aws_sdk_s3::operation::head_bucket::HeadBucketError;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::operation::upload_part::UploadPartOutput;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    ChecksumMode, CompletedMultipartUpload, CompletedPart, ObjectLockLegalHoldStatus, ObjectLockRetentionMode,
};
use aws_sdk_s3::{Client as S3Client, Config as S3Config, operation::head_object::HeadObjectOutput};
use aws_sdk_s3::{config::SharedCredentialsProvider, types::BucketVersioningStatus};
use aws_smithy_http_client::{Builder as SmithyHttpClientBuilder, tls as smithy_tls};
use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::http::{
    HttpConnector as SmithyHttpConnector, HttpConnectorFuture, SharedHttpConnector, http_client_fn,
};
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use aws_smithy_runtime_api::client::result::ConnectorError;
use aws_smithy_types::body::SdkBody;
use futures::{StreamExt, stream};
use http::{HeaderMap, HeaderName, HeaderValue, StatusCode, Uri};
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::rt::{TokioExecutor, TokioTimer};
use reqwest::Client as HttpClient;
use rustfs_config::{DEFAULT_TRUST_LEAF_CERT_AS_CA, ENV_TRUST_LEAF_CERT_AS_CA, RUSTFS_CA_CERT, RUSTFS_TLS_CERT};
use rustfs_utils::egress::{OutboundUrlError, validate_outbound_url};
use rustfs_utils::http::{
    AMZ_BUCKET_REPLICATION_STATUS, AMZ_OBJECT_LOCK_BYPASS_GOVERNANCE, AMZ_OBJECT_LOCK_LEGAL_HOLD, AMZ_OBJECT_LOCK_MODE,
    AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE, AMZ_STORAGE_CLASS, AMZ_WEBSITE_REDIRECT_LOCATION, is_amz_header, is_minio_header,
    is_rustfs_header, is_standard_header, is_storageclass_header,
};
use rustfs_utils::http::{
    SUFFIX_FORCE_DELETE, SUFFIX_SOURCE_DELETEMARKER, SUFFIX_SOURCE_ETAG, SUFFIX_SOURCE_MTIME, SUFFIX_SOURCE_REPLICATION_CHECK,
    SUFFIX_SOURCE_REPLICATION_REQUEST, SUFFIX_SOURCE_VERSION_ID, insert_header,
};
use rustls_pki_types::pem::PemObject;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::path::Path;
use std::str::FromStr as _;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::Weak;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::sync::Mutex;
use tokio::sync::OwnedMutexGuard;
use tokio::sync::RwLock;
use tower::Service;
use tracing::error;
use tracing::warn;
use url::Url;
use uuid::Uuid;

const DEFAULT_HEALTH_CHECK_RELOAD_DURATION: Duration = Duration::from_secs(30 * 60);
const MAX_CONCURRENT_TARGET_HEALTH_CHECKS: usize = 16;
const REDACTED_CREDENTIAL: &str = "<redacted>";
const ERR_TARGET_CLIENT_RETIRED: &str = "replication target client retired";
const ERR_TARGET_CLIENT_RETIRED_CODE: &str = "TargetClientRetired";
const TARGET_CLIENT_CLEANUP_PROBES_PER_REGISTRATION: usize = 8;

pub static GLOBAL_BUCKET_TARGET_SYS: OnceLock<BucketTargetSys> = OnceLock::new();
static TARGET_CLIENT_GENERATIONS: OnceLock<std::sync::RwLock<TargetClientRegistry>> = OnceLock::new();

#[derive(Default)]
struct TargetClientRegistry {
    registrations: HashMap<usize, TargetClientRegistration>,
    cleanup_queue: VecDeque<usize>,
}

struct TargetClientRegistration {
    client: Weak<TargetClient>,
    generation: Arc<EndpointProbeState>,
    health_key: Arc<str>,
}

struct TargetUpdateGuards {
    _bucket: OwnedMutexGuard<()>,
    _arns: Vec<OwnedMutexGuard<()>>,
}

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
        register_target_client(&client);
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

fn canonical_endpoint_health_key(url: &Url) -> String {
    url.origin().ascii_serialization()
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

fn target_endpoint_health_key(target: &BucketTarget) -> Option<String> {
    target.url().ok().map(|url| canonical_endpoint_health_key(&url))
}

fn public_health_key_from_canonical(endpoint: &str) -> Option<String> {
    Url::parse(endpoint).ok().map(|url| endpoint_health_key(&url))
}

fn target_client_config_unchanged(old: &BucketTarget, new: &BucketTarget) -> bool {
    let credentials_unchanged = match (&old.credentials, &new.credentials) {
        (Some(old), Some(new)) => {
            old.access_key == new.access_key
                && old.secret_key == new.secret_key
                && old.session_token == new.session_token
                && old.expiration == new.expiration
        }
        (None, None) => true,
        _ => false,
    };

    old.endpoint == new.endpoint
        && old.secure == new.secure
        && credentials_unchanged
        && old.target_bucket == new.target_bucket
        && old.storage_class == new.storage_class
        && old.disable_proxy == new.disable_proxy
        && old.arn == new.arn
        && old.reset_id == new.reset_id
        && old.health_check_duration == new.health_check_duration
        && old.replication_sync == new.replication_sync
        && old.region == new.region
        && old.path == new.path
        && old.skip_tls_verify == new.skip_tls_verify
        && old.ca_cert_pem == new.ca_cert_pem
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

#[derive(Debug, Default)]
struct EndpointProbeState {
    retired: AtomicBool,
    in_flight: Arc<RwLock<()>>,
}

impl EndpointProbeState {
    async fn acquire(self: &Arc<Self>) -> Option<tokio::sync::OwnedRwLockReadGuard<()>> {
        let guard = Arc::clone(&self.in_flight).read_owned().await;
        (!self.retired.load(Ordering::Acquire)).then_some(guard)
    }

    fn retire(&self) {
        self.retired.store(true, Ordering::Release);
    }

    async fn wait_for_idle(&self) {
        let _guard = self.in_flight.write().await;
    }
}

fn target_client_identity(client: &TargetClient) -> usize {
    std::ptr::from_ref(client).addr()
}

fn target_client_health_key_from_endpoint(client: &TargetClient) -> Arc<str> {
    Url::parse(&client.endpoint)
        .map(|url| Arc::from(canonical_endpoint_health_key(&url)))
        .unwrap_or_else(|_| Arc::from(client.endpoint.as_str()))
}

fn target_client_registration(client: &TargetClient) -> Option<(Arc<EndpointProbeState>, Arc<str>)> {
    let registrations = TARGET_CLIENT_GENERATIONS
        .get()?
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    registrations
        .registrations
        .get(&target_client_identity(client))
        .filter(|registration| std::ptr::eq(registration.client.as_ptr(), client))
        .map(|registration| (Arc::clone(&registration.generation), Arc::clone(&registration.health_key)))
}

fn register_target_clients<'a>(clients: impl IntoIterator<Item = &'a Arc<TargetClient>>) {
    let clients = clients
        .into_iter()
        .filter(|client| target_client_registration(client).is_none())
        .collect::<Vec<_>>();
    if clients.is_empty() {
        return;
    }
    let mut registrations = TARGET_CLIENT_GENERATIONS
        .get_or_init(Default::default)
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let cleanup_probes = registrations
        .cleanup_queue
        .len()
        .min(clients.len().saturating_mul(TARGET_CLIENT_CLEANUP_PROBES_PER_REGISTRATION));
    for _ in 0..cleanup_probes {
        let Some(identity) = registrations.cleanup_queue.pop_front() else {
            break;
        };
        if registrations
            .registrations
            .get(&identity)
            .is_some_and(|registration| registration.client.strong_count() == 0)
        {
            registrations.registrations.remove(&identity);
        } else {
            registrations.cleanup_queue.push_back(identity);
        }
    }
    for client in clients {
        let identity = target_client_identity(client);
        if registrations
            .registrations
            .get(&identity)
            .is_some_and(|registration| std::ptr::eq(registration.client.as_ptr(), client.as_ref()))
        {
            continue;
        }
        registrations.registrations.insert(
            identity,
            TargetClientRegistration {
                client: Arc::downgrade(client),
                generation: Arc::new(EndpointProbeState::default()),
                health_key: target_client_health_key_from_endpoint(client),
            },
        );
        registrations.cleanup_queue.push_back(identity);
    }
}

fn register_target_client(client: &Arc<TargetClient>) -> Arc<EndpointProbeState> {
    if let Some(generation) = target_client_generation(client) {
        return generation;
    }
    register_target_clients(std::iter::once(client));
    target_client_generation(client).expect("registered target client generation should exist")
}

fn target_client_generation(client: &TargetClient) -> Option<Arc<EndpointProbeState>> {
    target_client_registration(client).map(|(generation, _)| generation)
}

fn target_client_health_key(client: &TargetClient) -> Arc<str> {
    target_client_registration(client)
        .map(|(_, health_key)| health_key)
        .unwrap_or_else(|| target_client_health_key_from_endpoint(client))
}

#[derive(Debug, Default)]
pub struct BucketTargetSys {
    pub arn_remotes_map: Arc<RwLock<HashMap<String, ArnTarget>>>,
    pub targets_map: Arc<RwLock<HashMap<String, Vec<BucketTarget>>>>,
    pub h_mutex: Arc<RwLock<HashMap<String, EpHealth>>>,
    pub(crate) target_h_mutex: Arc<RwLock<HashMap<String, EpHealth>>>,
    health_probe_states: RwLock<HashMap<String, Arc<EndpointProbeState>>>,
    retired_health_endpoints: RwLock<HashSet<String>>,
    pub hc_client: Arc<HttpClient>,
    pub a_mutex: Arc<Mutex<HashMap<String, ArnErrs>>>,
    pub arn_errs_map: Arc<RwLock<HashMap<String, ArnErrs>>>,
    update_mutexes: Mutex<HashMap<String, Weak<Mutex<()>>>>,
    arn_update_mutexes: Mutex<HashMap<String, Weak<Mutex<()>>>>,
    #[cfg(test)]
    target_client_build_probe: Arc<Mutex<Option<TargetClientBuildProbe>>>,
    heartbeat_started: OnceLock<()>,
}

impl BucketTargetSys {
    pub fn get() -> &'static Self {
        GLOBAL_BUCKET_TARGET_SYS.get_or_init(Self::new)
    }

    fn new() -> Self {
        Self {
            arn_remotes_map: Arc::new(RwLock::new(HashMap::new())),
            targets_map: Arc::new(RwLock::new(HashMap::new())),
            h_mutex: Arc::new(RwLock::new(HashMap::new())),
            target_h_mutex: Arc::new(RwLock::new(HashMap::new())),
            health_probe_states: RwLock::new(HashMap::new()),
            retired_health_endpoints: RwLock::new(HashSet::new()),
            hc_client: Arc::new(HttpClient::new()),
            a_mutex: Arc::new(Mutex::new(HashMap::new())),
            arn_errs_map: Arc::new(RwLock::new(HashMap::new())),
            update_mutexes: Mutex::new(HashMap::new()),
            arn_update_mutexes: Mutex::new(HashMap::new()),
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

    async fn keyed_update_mutex(update_mutexes: &Mutex<HashMap<String, Weak<Mutex<()>>>>, key: &str) -> Arc<Mutex<()>> {
        let mut update_mutexes = update_mutexes.lock().await;
        if let Some(update_mutex) = update_mutexes.get(key).and_then(Weak::upgrade) {
            return update_mutex;
        }
        update_mutexes.retain(|_, update_mutex| update_mutex.strong_count() > 0);
        let update_mutex = Arc::new(Mutex::new(()));
        update_mutexes.insert(key.to_string(), Arc::downgrade(&update_mutex));
        update_mutex
    }

    async fn target_update_mutex(&self, bucket: &str) -> Arc<Mutex<()>> {
        Self::keyed_update_mutex(&self.update_mutexes, bucket).await
    }

    async fn arn_update_mutex(&self, arn: &str) -> Arc<Mutex<()>> {
        Self::keyed_update_mutex(&self.arn_update_mutexes, arn).await
    }

    async fn lock_target_arns(&self, arns: &mut Vec<String>) -> Vec<OwnedMutexGuard<()>> {
        // Lock order: bucket, sorted ARNs, targets, ARN clients, health, probes,
        // retired endpoints. Drain only after the map locks are released.
        arns.sort_unstable();
        arns.dedup();
        let mut guards = Vec::with_capacity(arns.len());
        for arn in arns {
            guards.push(self.arn_update_mutex(arn).await.lock_owned().await);
        }
        guards
    }

    async fn drain_retired_states(update_guards: TargetUpdateGuards, states: Vec<Arc<EndpointProbeState>>) {
        if states.is_empty() {
            return;
        }
        let drain = tokio::spawn(async move {
            let _update_guards = update_guards;
            for state in states {
                state.wait_for_idle().await;
            }
        });
        if let Err(err) = drain.await {
            error!(error = %err, "retired replication target drain failed");
        }
    }

    pub async fn is_offline(&self, url: &Url) -> bool {
        let endpoint = canonical_endpoint_health_key(url);
        let health_key = endpoint_health_key(url);
        let health_map = self.h_mutex.read().await;
        let retired_endpoints = self.retired_health_endpoints.read().await;
        if retired_endpoints.contains(&endpoint) {
            return true;
        }
        if let Some(health) = health_map.get(&health_key) {
            return !health.online;
        }
        drop(retired_endpoints);
        drop(health_map);

        !self.init_hc_if_active(url, false).await
    }

    pub(crate) async fn target_is_offline(&self, target: &TargetClient) -> bool {
        let Ok(_lease) = target.operation_lease().await else {
            return true;
        };
        let remotes = self.arn_remotes_map.read().await;
        let Some(current) = remotes.get(&target.arn).and_then(|remote| remote.client.as_ref()) else {
            return true;
        };
        if !std::ptr::eq(current.as_ref(), target) {
            return true;
        }
        {
            let health_map = self.target_h_mutex.read().await;
            if target_client_generation(target).is_some_and(|generation| generation.retired.load(Ordering::Acquire)) {
                return true;
            }
            if let Some(health) = health_map.get(&target.arn) {
                return !health.online;
            }
        }
        let mut health_map = self.target_h_mutex.write().await;
        if target_client_generation(target).is_some_and(|generation| generation.retired.load(Ordering::Acquire)) {
            return true;
        }
        let health = health_map.entry(target.arn.clone()).or_insert_with(|| target_health(target));
        !health.online
    }

    pub async fn mark_offline(&self, url: &Url) {
        let key = endpoint_health_key(url);
        let mut health_map = self.h_mutex.write().await;
        let retired_endpoints = self.retired_health_endpoints.read().await;
        if retired_endpoints.contains(&canonical_endpoint_health_key(url)) {
            return;
        }
        if let Some(health) = health_map.get_mut(&key) {
            update_endpoint_health(health, false, Duration::from_secs(0), OffsetDateTime::now_utc());
        }
    }

    pub(crate) async fn mark_target_offline(&self, target: &TargetClient) {
        let Ok(_lease) = target.operation_lease().await else {
            return;
        };
        let remotes = self.arn_remotes_map.read().await;
        let Some(current) = remotes.get(&target.arn).and_then(|remote| remote.client.as_ref()) else {
            return;
        };
        if !std::ptr::eq(current.as_ref(), target) {
            return;
        }
        let mut health_map = self.target_h_mutex.write().await;
        if target_client_generation(target).is_some_and(|generation| generation.retired.load(Ordering::Acquire)) {
            return;
        }
        let health = health_map.entry(target.arn.clone()).or_insert_with(|| target_health(target));
        update_endpoint_health(health, false, Duration::from_secs(0), OffsetDateTime::now_utc());
    }

    #[cfg(test)]
    async fn init_target_health(&self, target: &TargetClient) {
        self.target_h_mutex
            .write()
            .await
            .insert(target.arn.clone(), target_health(target));
        self.init_hc(&target.to_url()).await;
    }

    pub async fn init_hc(&self, url: &Url) {
        self.init_hc_if_active(url, true).await;
    }

    async fn init_hc_if_active(&self, url: &Url, replace: bool) -> bool {
        let mut health_map = self.h_mutex.write().await;
        let mut probe_states = self.health_probe_states.write().await;
        let endpoint = canonical_endpoint_health_key(url);
        let health_key = endpoint_health_key(url);
        let mut retired_endpoints = self.retired_health_endpoints.write().await;
        if replace {
            retired_endpoints.remove(&endpoint);
        } else if retired_endpoints.contains(&endpoint) {
            return false;
        }
        let health = EpHealth {
            endpoint: health_key,
            scheme: url.scheme().to_string(),
            online: true,
            ..Default::default()
        };
        if replace {
            if let Some(previous) = health_map.insert(health.endpoint.clone(), health) {
                let previous_scheme = if previous.scheme.is_empty() {
                    "https"
                } else {
                    previous.scheme.as_str()
                };
                if previous_scheme != url.scheme()
                    && let Ok(previous_url) = Url::parse(&format!("{previous_scheme}://{}", previous.endpoint))
                {
                    let previous_endpoint = canonical_endpoint_health_key(&previous_url);
                    retired_endpoints.insert(previous_endpoint.clone());
                    if let Some(previous_probe) = probe_states.remove(&previous_endpoint) {
                        previous_probe.retire();
                    }
                }
            }
        } else {
            health_map.entry(health.endpoint.clone()).or_insert(health);
        }
        probe_states
            .entry(endpoint)
            .or_insert_with(|| Arc::new(EndpointProbeState::default()));
        true
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
            let Ok(lease) = target.operation_lease().await else {
                return None;
            };
            let start = Instant::now();
            let online = Self::check_endpoint_health(&target).await;
            Some((target, lease, online, start.elapsed()))
        }));
        let mut checks = checks.buffer_unordered(MAX_CONCURRENT_TARGET_HEALTH_CHECKS);
        let mut endpoint_checks = HashMap::<String, (String, bool, Duration)>::new();

        while let Some(Some((target, _lease, online, duration))) = checks.next().await {
            let url = target.to_url();
            {
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

    async fn apply_probe_result(&self, health_key: &str, probe_state: &EndpointProbeState, online: bool, duration: Duration) {
        let mut health_map = self.h_mutex.write().await;
        if probe_state.retired.load(Ordering::Acquire) {
            return;
        }
        if let Some(health) = health_map.get_mut(health_key) {
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
        self.h_mutex.read().await.clone()
    }

    async fn target_health_stats(&self) -> HashMap<String, EpHealth> {
        self.target_h_mutex.read().await.clone()
    }

    pub async fn list_targets(&self, bucket: &str, arn_type: &str) -> Vec<BucketTarget> {
        let health_stats = self.target_health_stats().await;
        let mut targets = Vec::new();

        if !bucket.is_empty() {
            if let Ok(bucket_targets) = self.list_bucket_targets(bucket).await {
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
            return targets;
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

        targets
    }

    pub async fn list_bucket_targets(&self, bucket: &str) -> Result<BucketTargets, BucketTargetError> {
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

    pub async fn delete(&self, bucket: &str) {
        let bucket_guard = self.target_update_mutex(bucket).await.lock_owned().await;
        let mut arns = self
            .targets_map
            .read()
            .await
            .get(bucket)
            .into_iter()
            .flatten()
            .map(|target| target.arn.clone())
            .collect::<Vec<_>>();
        let arn_guards = self.lock_target_arns(&mut arns).await;
        let update_guards = TargetUpdateGuards {
            _bucket: bucket_guard,
            _arns: arn_guards,
        };
        let mut targets_map = self.targets_map.write().await;
        let mut arn_remotes_map = self.arn_remotes_map.write().await;
        let mut target_health_map = self.target_h_mutex.write().await;
        let mut health_map = self.h_mutex.write().await;
        let mut probe_states = self.health_probe_states.write().await;
        let mut retired_endpoints = self.retired_health_endpoints.write().await;
        let mut retired_clients = Vec::new();
        let mut retired_endpoint_candidates = HashSet::new();

        if let Some(targets) = targets_map.remove(bucket) {
            let remaining_arns = targets_map
                .values()
                .flatten()
                .map(|target| target.arn.as_str())
                .collect::<HashSet<_>>();
            for target in targets {
                if let Some(endpoint) = target_endpoint_health_key(&target) {
                    retired_endpoint_candidates.insert(endpoint);
                }
                if remaining_arns.contains(target.arn.as_str()) {
                    continue;
                }
                if let Some(client) = arn_remotes_map.remove(&target.arn).and_then(|target| target.client) {
                    target_health_map.remove(&target.arn);
                    retired_endpoint_candidates.insert(target_client_health_key(&client).to_string());
                    if let Some(generation) = target_client_generation(&client) {
                        generation.retire();
                        retired_clients.push(generation);
                    }
                }
            }
        }

        let retired_probes = Self::retire_inactive_endpoint_health(
            &arn_remotes_map,
            &retired_endpoint_candidates,
            &mut health_map,
            &mut probe_states,
            &mut retired_endpoints,
        );
        drop(retired_endpoints);
        drop(probe_states);
        drop(health_map);
        drop(target_health_map);
        drop(arn_remotes_map);
        drop(targets_map);
        retired_clients.extend(retired_probes);
        Self::drain_retired_states(update_guards, retired_clients).await;
    }

    fn retire_inactive_endpoint_health(
        arn_remotes_map: &HashMap<String, ArnTarget>,
        retired_endpoint_candidates: &HashSet<String>,
        health_map: &mut HashMap<String, EpHealth>,
        probe_states: &mut HashMap<String, Arc<EndpointProbeState>>,
        retired_endpoints: &mut HashSet<String>,
    ) -> Vec<Arc<EndpointProbeState>> {
        if retired_endpoint_candidates.is_empty() {
            return Vec::new();
        }

        let active_endpoints = arn_remotes_map
            .values()
            .filter_map(|target| target.client.as_ref())
            .map(|client| target_client_health_key(client).to_string())
            .collect::<HashSet<_>>();
        let active_public_endpoints = active_endpoints
            .iter()
            .filter_map(|endpoint| {
                let url = Url::parse(endpoint).ok()?;
                Some((endpoint_health_key(&url), url.scheme().to_string()))
            })
            .collect::<HashMap<_, _>>();
        let mut retired_probes = Vec::new();

        for endpoint in retired_endpoint_candidates {
            if active_endpoints.contains(endpoint) {
                retired_endpoints.remove(endpoint);
                continue;
            }
            retired_endpoints.insert(endpoint.clone());
            if let Some(probe_state) = probe_states.remove(endpoint) {
                probe_state.retire();
                retired_probes.push(probe_state);
            }
            if let Some(health_key) = public_health_key_from_canonical(endpoint) {
                if let Some(active_scheme) = active_public_endpoints.get(&health_key) {
                    if let Some(health) = health_map.get_mut(&health_key) {
                        health.scheme.clone_from(active_scheme);
                    }
                } else {
                    health_map.remove(&health_key);
                }
            }
        }

        retired_probes
    }

    pub async fn set_target(
        &self,
        bucket: &str,
        target: &BucketTarget,
        update: bool,
    ) -> Result<BucketTargets, BucketTargetError> {
        self.validate_target(bucket, target).await?;

        let mut bucket_targets = match self.list_bucket_targets(bucket).await {
            Ok(targets) => targets,
            Err(BucketTargetError::BucketRemoteTargetNotFound { .. }) => BucketTargets::default(),
            Err(err) => return Err(err),
        };

        Self::upsert_target_entry(&mut bucket_targets.targets, target, update)?;

        Ok(bucket_targets)
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

    fn upsert_target_entry(
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

    pub async fn mark_refresh_in_progress(&self, bucket: &str, arn: &str) {
        let mut arn_errs = self.arn_errs_map.write().await;
        arn_errs.entry(arn.to_string()).or_insert_with(|| ArnErrs {
            bucket: bucket.to_string(),
            update_in_progress: true,
            count: 1,
        });
    }

    pub async fn mark_refresh_done(&self, bucket: &str, arn: &str) {
        let mut arn_errs = self.arn_errs_map.write().await;
        if let Some(err) = arn_errs.get_mut(arn) {
            err.update_in_progress = false;
            err.bucket = bucket.to_string();
        }
    }

    pub async fn is_reloading_target(&self, _bucket: &str, arn: &str) -> bool {
        let arn_errs = self.arn_errs_map.read().await;
        arn_errs.get(arn).map(|err| err.update_in_progress).unwrap_or(false)
    }

    pub async fn inc_arn_errs(&self, _bucket: &str, arn: &str) {
        let mut arn_errs = self.arn_errs_map.write().await;
        if let Some(err) = arn_errs.get_mut(arn) {
            err.count += 1;
        }
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

        if let Some(cli) = cli {
            return Some(cli);
        }

        // TODO: spawn a task to reload the target
        if self.is_reloading_target(bucket, arn).await {
            return None;
        }

        if let Some(last_refresh) = last_refresh {
            let now = OffsetDateTime::now_utc();
            if now - last_refresh < Duration::from_secs(60 * 5) {
                return None;
            }
        }

        match get_bucket_targets_config(bucket).await {
            Ok(bucket_targets) => {
                self.mark_refresh_in_progress(bucket, arn).await;
                self.update_all_targets(bucket, Some(&bucket_targets)).await;
                self.mark_refresh_done(bucket, arn).await;
            }
            Err(e) => {
                error!("get bucket targets config error:{}", e);
            }
        };

        let cli = self
            .arn_remotes_map
            .read()
            .await
            .get(arn)
            .and_then(|target| target.client.clone());
        if cli.is_some() {
            return cli;
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

        let creds = SdkCredentials::builder()
            .access_key_id(credentials.access_key.clone())
            .secret_access_key(credentials.secret_key.clone())
            .account_id(target.reset_id.clone())
            .provider_name("bucket_target_sys")
            .build();

        let endpoint = if target.secure {
            format!("https://{}", target.endpoint)
        } else {
            format!("http://{}", target.endpoint)
        };
        let parsed_endpoint = Url::parse(&endpoint).map_err(|err| BucketTargetError::RemoteTargetConnectionErr {
            bucket: target.target_bucket.clone(),
            access_key: credentials.access_key.clone(),
            error: format!("invalid target endpoint: {err}"),
        })?;
        validate_replication_target_endpoint(&parsed_endpoint).map_err(|err| BucketTargetError::RemoteTargetConnectionErr {
            bucket: target.target_bucket.clone(),
            access_key: credentials.access_key.clone(),
            error: format!("target endpoint is not allowed: {err}"),
        })?;

        let mut config_builder = S3Config::builder()
            .endpoint_url(endpoint.clone())
            .credentials_provider(SharedCredentialsProvider::new(creds))
            .region(SdkRegion::new(target.region.clone()))
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest());

        if should_force_path_style(target) {
            config_builder = config_builder.force_path_style(true);
        }

        if let Some(http_client) =
            build_aws_s3_http_client_for_target(target)
                .await
                .map_err(|err| BucketTargetError::RemoteTargetConnectionErr {
                    bucket: target.target_bucket.clone(),
                    access_key: credentials.access_key.clone(),
                    error: err.to_string(),
                })?
        {
            config_builder = config_builder.http_client(http_client);
        }

        let config = config_builder.build();

        Ok(TargetClient {
            endpoint,
            credentials: target.credentials.clone(),
            bucket: target.target_bucket.clone(),
            storage_class: target.storage_class.clone(),
            disable_proxy: target.disable_proxy,
            arn: target.arn.clone(),
            reset_id: target.reset_id.clone(),
            secure: target.secure,
            health_check_duration: target.health_check_duration,
            replicate_sync: target.replication_sync,
            client: Arc::new(S3Client::from_conf(config)),
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

    #[doc(hidden)]
    pub async fn get_remote_target_client_if_current(&self, bucket: &str, target: &BucketTarget) -> Option<Arc<TargetClient>> {
        let targets_map = self.targets_map.read().await;
        let current = targets_map.get(bucket)?.iter().find(|current| {
            current.arn == target.arn
                && current.target_type == target.target_type
                && current.deployment_id == target.deployment_id
                && target_client_config_unchanged(current, target)
        })?;
        let arn_remotes_map = self.arn_remotes_map.read().await;
        let client = arn_remotes_map.get(&current.arn)?.client.clone()?;
        target_client_generation(&client)
            .is_none_or(|generation| !generation.retired.load(Ordering::Acquire))
            .then_some(client)
    }

    pub async fn get_remote_bucket_target_by_arn(&self, bucket: &str, arn: &str) -> Option<BucketTarget> {
        let targets_map = self.targets_map.read().await;
        targets_map
            .get(bucket)
            .and_then(|targets| targets.iter().find(|t| t.arn == arn).cloned())
    }

    pub async fn update_all_targets(&self, bucket: &str, targets: Option<&BucketTargets>) {
        let bucket_guard = self.target_update_mutex(bucket).await.lock_owned().await;
        let existing_targets = self.targets_map.read().await.get(bucket).cloned().unwrap_or_default();
        let mut arns = existing_targets.iter().map(|target| target.arn.clone()).collect::<Vec<_>>();
        if let Some(targets) = targets {
            arns.extend(targets.targets.iter().map(|target| target.arn.clone()));
        }
        let arn_guards = self.lock_target_arns(&mut arns).await;
        let update_guards = TargetUpdateGuards {
            _bucket: bucket_guard,
            _arns: arn_guards,
        };
        let existing_clients = {
            let arn_remotes_map = self.arn_remotes_map.read().await;
            existing_targets
                .iter()
                .filter_map(|target| {
                    arn_remotes_map
                        .get(&target.arn)
                        .and_then(|target| target.client.clone())
                        .map(|client| (target.arn.clone(), client))
                })
                .collect::<HashMap<_, _>>()
        };
        let existing_configs = existing_targets
            .iter()
            .map(|target| (target.arn.as_str(), target))
            .collect::<HashMap<_, _>>();
        let mut prepared_clients = HashMap::new();

        if let Some(new_targets) = targets
            && !new_targets.is_empty()
        {
            for target in &new_targets.targets {
                let reusable = existing_configs
                    .get(target.arn.as_str())
                    .filter(|old| target_client_config_unchanged(old, target))
                    .and_then(|_| existing_clients.get(&target.arn).cloned())
                    .filter(|client| {
                        target_client_generation(client).is_none_or(|generation| !generation.retired.load(Ordering::Acquire))
                    });
                let client = if reusable.is_some() {
                    reusable
                } else {
                    match self.get_remote_target_client_internal(target).await {
                        Ok(client) => Some(Arc::new(client)),
                        // The target stays in `targets_map`, so it keeps showing up in
                        // `bucket remote ls` while no client exists to replicate through it —
                        // replication then drops every object for this ARN. Without this the
                        // rejection (loopback endpoint, bad CA, unparseable URL) left no trace
                        // anywhere.
                        Err(err) => {
                            warn!(
                                bucket = %bucket,
                                arn = %target.arn,
                                endpoint = %target.endpoint,
                                error = %err,
                                "replication target client unavailable; objects for this ARN will not replicate"
                            );
                            None
                        }
                    }
                };
                prepared_clients.insert(target.arn.clone(), client);
            }
        }
        register_target_clients(prepared_clients.values().flatten());

        let mut targets_map = self.targets_map.write().await;
        let mut arn_remotes_map = self.arn_remotes_map.write().await;
        let mut target_health_map = self.target_h_mutex.write().await;
        let mut health_map = self.h_mutex.write().await;
        let mut probe_states = self.health_probe_states.write().await;
        let mut retired_endpoints = self.retired_health_endpoints.write().await;
        let mut retired_clients = Vec::new();
        let mut retired_endpoint_candidates = HashSet::new();
        if let Some(old_targets) = targets_map.remove(bucket) {
            let needs_remaining_arns = old_targets.iter().any(|target| !prepared_clients.contains_key(&target.arn));
            let remaining_arns = if needs_remaining_arns {
                targets_map
                    .values()
                    .flatten()
                    .map(|target| target.arn.as_str())
                    .collect::<HashSet<_>>()
            } else {
                HashSet::new()
            };
            for target in old_targets {
                if !prepared_clients.contains_key(&target.arn) && remaining_arns.contains(target.arn.as_str()) {
                    if let Some(endpoint) = target_endpoint_health_key(&target) {
                        retired_endpoint_candidates.insert(endpoint);
                    }
                    self.update_bandwidth_limit(bucket, &target.arn, 0);
                    continue;
                }
                let mut reused = false;
                if let Some(client) = arn_remotes_map.remove(&target.arn).and_then(|target| target.client) {
                    reused = prepared_clients
                        .get(&target.arn)
                        .and_then(Option::as_ref)
                        .is_some_and(|prepared| Arc::ptr_eq(prepared, &client));
                    if !reused {
                        target_health_map.remove(&target.arn);
                        retired_endpoint_candidates.insert(target_client_health_key(&client).to_string());
                        if let Some(generation) = target_client_generation(&client) {
                            generation.retire();
                            retired_clients.push(generation);
                        }
                    }
                }
                if !reused && let Some(endpoint) = target_endpoint_health_key(&target) {
                    retired_endpoint_candidates.insert(endpoint);
                }
                self.update_bandwidth_limit(bucket, &target.arn, 0);
            }
        }
        if let Some(new_targets) = targets {
            for target in &new_targets.targets {
                if let Some(client) = prepared_clients.get(&target.arn).and_then(Option::as_ref) {
                    retired_endpoints.remove(target_client_health_key(client).as_ref());
                    target_health_map
                        .entry(target.arn.clone())
                        .or_insert_with(|| target_health(client));
                    arn_remotes_map.insert(
                        target.arn.clone(),
                        ArnTarget {
                            client: Some(Arc::clone(client)),
                            last_refresh: OffsetDateTime::now_utc(),
                        },
                    );
                    self.update_bandwidth_limit(bucket, &target.arn, target.bandwidth_limit);
                }
            }
        }
        if let Some(new_targets) = targets
            && !new_targets.is_empty()
        {
            targets_map.insert(bucket.to_string(), new_targets.targets.clone());
        }

        let retired_probes = Self::retire_inactive_endpoint_health(
            &arn_remotes_map,
            &retired_endpoint_candidates,
            &mut health_map,
            &mut probe_states,
            &mut retired_endpoints,
        );
        drop(retired_endpoints);
        drop(probe_states);
        drop(health_map);
        drop(target_health_map);
        drop(arn_remotes_map);
        drop(targets_map);
        retired_clients.extend(retired_probes);
        Self::drain_retired_states(update_guards, retired_clients).await;
    }

    pub async fn set(&self, bucket: &str, meta: &BucketMetadata) {
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

        {
            let targets_map = self.targets_map.read().await;
            if let Some(targets) = targets_map.get(bucket) {
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
            }
        }

        if !target.target_type.is_valid() {
            return (String::new(), false);
        }
        let arn = generate_arn(target, depl_id);
        (arn, false)
    }
}

#[derive(Debug)]
struct AcceptAnyServerCertVerifier;

impl rustls::client::danger::ServerCertVerifier for AcceptAnyServerCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls_pki_types::CertificateDer<'_>,
        _intermediates: &[rustls_pki_types::CertificateDer<'_>],
        _server_name: &rustls_pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls_pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls_pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls_pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::aws_lc_rs::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[derive(Clone)]
struct TargetHyperHttpConnector<C> {
    client: HyperClient<C, SdkBody>,
}

impl<C> fmt::Debug for TargetHyperHttpConnector<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TargetHyperHttpConnector")
            .field("client", &"** hyper client **")
            .finish()
    }
}

impl<C> SmithyHttpConnector for TargetHyperHttpConnector<C>
where
    C: Clone + Send + Sync + 'static,
    C: Service<Uri>,
    C::Response:
        hyper::rt::Read + hyper::rt::Write + hyper_util::client::legacy::connect::Connection + Send + Sync + Unpin + 'static,
    C::Future: Unpin + Send + 'static,
    C::Error: Into<BoxError>,
{
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        let request = match request.try_into_http1x() {
            Ok(request) => request,
            Err(err) => return HttpConnectorFuture::ready(Err(ConnectorError::user(err.into()))),
        };

        let mut client = self.client.clone();
        let fut = client.call(request);
        HttpConnectorFuture::new(async move {
            let response = fut
                .await
                .map_err(|err| ConnectorError::io(err.into()))?
                .map(SdkBody::from_body_1_x);
            HttpResponse::try_from(response).map_err(|err| ConnectorError::other(err.into(), None))
        })
    }
}

fn ensure_rustls_crypto_provider() {
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    }
}

fn has_custom_ca_pem(target: &BucketTarget) -> bool {
    !target.ca_cert_pem.trim().is_empty()
}

/// Env opt-in that re-enables loopback replication targets. Loopback (`127.0.0.1`,
/// `::1`, `localhost`) is a classic SSRF vector and stays rejected by default, but
/// single-host multi-instance dev setups and the e2e harness legitimately replicate
/// over loopback. Never set this in production.
const ALLOW_LOOPBACK_REPLICATION_TARGET_ENV: &str = "RUSTFS_REPLICATION_ALLOW_LOOPBACK_TARGET";

fn loopback_replication_targets_allowed() -> bool {
    std::env::var(ALLOW_LOOPBACK_REPLICATION_TARGET_ENV)
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false)
}

fn validate_replication_target_endpoint(url: &Url) -> Result<(), OutboundUrlError> {
    validate_replication_target_endpoint_inner(url, loopback_replication_targets_allowed())
}

fn validate_replication_target_endpoint_inner(url: &Url, allow_loopback: bool) -> Result<(), OutboundUrlError> {
    match validate_outbound_url(url) {
        Ok(()) => Ok(()),
        // Replication targets are trusted infrastructure the operator configures, and
        // legitimately live on private networks, so private addresses are always allowed.
        Err(OutboundUrlError::ForbiddenHost {
            reason: "private address",
            ..
        }) => Ok(()),
        // Loopback is far higher SSRF risk, so it is allowed only under the explicit,
        // off-by-default opt-in above (single-host multi-instance / the e2e harness).
        Err(OutboundUrlError::ForbiddenHost {
            reason: "loopback address" | "loopback host",
            ..
        }) if allow_loopback => Ok(()),
        Err(err) => Err(err),
    }
}

fn build_insecure_aws_s3_http_client() -> SharedHttpClient {
    ensure_rustls_crypto_provider();

    let tls_config = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AcceptAnyServerCertVerifier))
        .with_no_client_auth();

    let https = hyper_rustls::HttpsConnectorBuilder::new()
        .with_tls_config(tls_config)
        .https_or_http()
        .enable_http1()
        .enable_http2()
        .build();
    let mut client_builder = HyperClient::builder(TokioExecutor::new());
    client_builder.pool_timer(TokioTimer::new());
    let client = client_builder.build(https);
    let connector = SharedHttpConnector::new(TargetHyperHttpConnector { client });

    http_client_fn(move |_settings, _components| connector.clone())
}

fn validate_ca_pem_bundle(ca_cert_pem: &[u8]) -> Result<(), String> {
    let certs = rustls_pki_types::CertificateDer::pem_slice_iter(ca_cert_pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| format!("invalid PEM encoding: {err}"))?;

    if certs.is_empty() {
        return Err("no certificates found".to_string());
    }

    // Smithy's rustls adapter defers parsing custom certificates and assumes
    // they are valid when the HTTPS connector is built. Validate every DER
    // certificate first so malformed configuration is reported rather than
    // reaching an `expect` in the dependency.
    let mut validation_store = rustls::RootCertStore::empty();
    for cert in certs {
        validation_store
            .add(cert)
            .map_err(|err| format!("invalid X.509 certificate: {err}"))?;
    }

    Ok(())
}

fn validate_target_ca_pem(ca_cert_pem: &str) -> Result<(), BucketTargetError> {
    validate_ca_pem_bundle(ca_cert_pem.as_bytes())
        .map_err(|err| BucketTargetError::Io(std::io::Error::other(format!("invalid target CA PEM: {err}"))))
}

fn compose_replication_trust_store(certificate_bundles: impl IntoIterator<Item = Vec<u8>>) -> (smithy_tls::TrustStore, usize) {
    // `TrustStore::default()` keeps the platform-native roots enabled. Target
    // and RUSTFS_TLS_PATH certificates extend that baseline instead of
    // replacing it with a target-specific trust island.
    let mut trust_store = smithy_tls::TrustStore::default();
    let mut custom_bundle_count = 0;
    for pem in certificate_bundles {
        trust_store.add_pem_certificate(pem);
        custom_bundle_count += 1;
    }

    (trust_store, custom_bundle_count)
}

fn build_aws_s3_http_client_with_trust_store(trust_store: smithy_tls::TrustStore) -> Result<SharedHttpClient, BucketTargetError> {
    let tls_context = smithy_tls::TlsContext::builder()
        .with_trust_store(trust_store)
        .build()
        .map_err(|err| BucketTargetError::Io(std::io::Error::other(format!("invalid target CA PEM: {err}"))))?;

    Ok(SmithyHttpClientBuilder::new()
        .tls_provider(smithy_tls::Provider::rustls(smithy_tls::rustls_provider::CryptoMode::AwsLc))
        .tls_context(tls_context)
        .build_https())
}

async fn load_tls_path_ca_bundles(tls_dir: &Path, trust_leaf_cert_as_ca: bool) -> Vec<Vec<u8>> {
    let mut certificate_bundles = Vec::new();

    let ca_path = tls_dir.join(RUSTFS_CA_CERT);
    match tokio::fs::read(&ca_path).await {
        Ok(pem) => match validate_ca_pem_bundle(&pem) {
            Ok(()) => certificate_bundles.push(pem),
            Err(err) => warn!("ignoring invalid custom CA bundle {:?} for replication client: {}", ca_path, err),
        },
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => warn!("failed to read custom CA bundle {:?} for replication client: {}", ca_path, e),
    }

    if trust_leaf_cert_as_ca {
        let leaf_cert_path = tls_dir.join(RUSTFS_TLS_CERT);
        match tokio::fs::read(&leaf_cert_path).await {
            Ok(pem) => match validate_ca_pem_bundle(&pem) {
                Ok(()) => certificate_bundles.push(pem),
                Err(err) => warn!(
                    "ignoring invalid leaf certificate {:?} for replication client trust store: {}",
                    leaf_cert_path, err
                ),
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => warn!("failed to read leaf cert {:?} for replication client trust store: {}", leaf_cert_path, e),
        }
    }

    certificate_bundles
}

async fn load_configured_tls_ca_bundles() -> Vec<Vec<u8>> {
    let tls_path = rustfs_utils::get_env_str(rustfs_config::ENV_RUSTFS_TLS_PATH, rustfs_config::DEFAULT_RUSTFS_TLS_PATH);
    if tls_path.is_empty() {
        return Vec::new();
    }

    load_tls_path_ca_bundles(
        Path::new(&tls_path),
        rustfs_utils::get_env_bool(ENV_TRUST_LEAF_CERT_AS_CA, DEFAULT_TRUST_LEAF_CERT_AS_CA),
    )
    .await
}

async fn build_aws_s3_http_client_from_target_ca_pem(ca_cert_pem: &str) -> Result<SharedHttpClient, BucketTargetError> {
    validate_target_ca_pem(ca_cert_pem)?;

    let mut certificate_bundles = load_configured_tls_ca_bundles().await;
    certificate_bundles.push(ca_cert_pem.as_bytes().to_vec());
    let (trust_store, _) = compose_replication_trust_store(certificate_bundles);

    build_aws_s3_http_client_with_trust_store(trust_store)
}

async fn build_aws_s3_http_client_for_target(target: &BucketTarget) -> Result<Option<SharedHttpClient>, BucketTargetError> {
    if !target.secure {
        return Ok(None);
    }

    if target.skip_tls_verify {
        return Ok(Some(build_insecure_aws_s3_http_client()));
    }

    if has_custom_ca_pem(target) {
        return build_aws_s3_http_client_from_target_ca_pem(&target.ca_cert_pem)
            .await
            .map(Some);
    }

    Ok(build_aws_s3_http_client_from_tls_path().await)
}

async fn build_aws_s3_http_client_from_tls_path() -> Option<aws_sdk_s3::config::SharedHttpClient> {
    let certificate_bundles = load_configured_tls_ca_bundles().await;
    if certificate_bundles.is_empty() {
        return None;
    }

    let (trust_store, _) = compose_replication_trust_store(certificate_bundles);
    match build_aws_s3_http_client_with_trust_store(trust_store) {
        Ok(client) => Some(client),
        Err(e) => {
            warn!("failed to build AWS SDK TLS context for replication client: {}", e);
            None
        }
    }
}

fn should_force_path_style(target: &BucketTarget) -> bool {
    match target.path.trim().to_ascii_lowercase().as_str() {
        // Explicit DNS/virtual-hosted-style requested by user.
        "dns" | "off" | "false" => false,
        // Explicit path-style or legacy boolean-like values.
        "path" | "on" | "true" => true,
        // `auto` and empty are defaulted to path-style for custom S3-compatible endpoints.
        "auto" | "" => true,
        // Unknown values: prefer compatibility with S3-compatible services.
        _ => true,
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
            retention_timestamp: OffsetDateTime::now_utc(),
            tagging_timestamp: OffsetDateTime::now_utc(),
            legalhold_timestamp: OffsetDateTime::now_utc(),
            replication_validity_check: false,
        }
    }
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

#[allow(dead_code)]
impl PutObjectOptions {
    fn set_match_etag(&mut self, etag: &str) {
        if etag == "*" {
            self.custom_header
                .insert("If-Match", HeaderValue::from_str("*").expect("err"));
        } else {
            self.custom_header
                .insert("If-Match", HeaderValue::from_str(&format!("\"{etag}\"")).expect("err"));
        }
    }

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

        if self.internal.replication_request {
            insert_header(&mut header, SUFFIX_SOURCE_REPLICATION_REQUEST, "true");
        }

        header
    }

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

struct TargetOperationLease {
    _guard: Option<tokio::sync::OwnedRwLockReadGuard<()>>,
}

impl TargetClient {
    pub fn to_url(&self) -> Url {
        Url::parse(&self.endpoint).unwrap()
    }

    async fn operation_lease(&self) -> Result<TargetOperationLease, S3ClientError> {
        let Some(generation) = target_client_generation(self) else {
            return Ok(TargetOperationLease { _guard: None });
        };
        let guard = generation.acquire().await.ok_or_else(|| {
            S3ClientError::with_metadata(ERR_TARGET_CLIENT_RETIRED, None, Some(ERR_TARGET_CLIENT_RETIRED_CODE.to_string()), None)
        })?;
        Ok(TargetOperationLease { _guard: Some(guard) })
    }

    #[doc(hidden)]
    pub async fn send_with_operation_lease<F: Future>(&self, operation: F) -> Result<F::Output, S3ClientError> {
        let _lease = self.operation_lease().await?;
        Ok(operation.await)
    }

    pub async fn bucket_exists(&self, bucket: &str) -> Result<bool, S3ClientError> {
        let _lease = self.operation_lease().await?;
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
        let _lease = self.operation_lease().await?;
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
    ) -> Result<HeadObjectOutput, SdkError<HeadObjectError>> {
        let _lease = self.operation_lease().await.map_err(SdkError::construction_failure)?;
        match self
            .client
            .head_object()
            .bucket(bucket)
            .key(object)
            .set_version_id(version_id)
            .send()
            .await
        {
            Ok(res) => Ok(res),
            Err(e) => Err(e),
        }
    }

    pub async fn put_object(
        &self,
        bucket: &str,
        object: &str,
        size: i64,
        body: ByteStream,
        opts: &PutObjectOptions,
    ) -> Result<(), S3ClientError> {
        let _lease = self.operation_lease().await?;
        let mut headers = opts.header();

        let builder = self.client.put_object();

        let version_id = opts.internal.source_version_id.clone();
        if !version_id.is_empty() {
            insert_header(&mut headers, SUFFIX_SOURCE_VERSION_ID, &version_id);
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

                Result::<_, aws_smithy_types::error::operation::BuildError>::Ok(req)
            })
            .send()
            .await
        {
            Ok(_) => Ok(()),
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
        let _lease = self.operation_lease().await?;
        let mut headers = HeaderMap::new();
        let version_id = opts.internal.source_version_id.clone();
        if !version_id.is_empty() {
            insert_header(&mut headers, SUFFIX_SOURCE_VERSION_ID, &version_id);
        }
        if opts.internal.replication_request {
            insert_header(&mut headers, SUFFIX_SOURCE_REPLICATION_REQUEST, "true");
        }

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
        let _lease = self.operation_lease().await?;
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
        let _lease = self.operation_lease().await?;
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

    pub async fn remove_object(
        &self,
        bucket: &str,
        object: &str,
        version_id: Option<String>,
        opts: RemoveObjectOptions,
    ) -> Result<(), S3ClientError> {
        self.remove_object_with_output(bucket, object, version_id, opts)
            .await
            .map(|_| ())
    }

    pub async fn remove_object_with_output(
        &self,
        bucket: &str,
        object: &str,
        version_id: Option<String>,
        opts: RemoveObjectOptions,
    ) -> Result<DeleteObjectOutput, S3ClientError> {
        let _lease = self.operation_lease().await?;
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
            Ok(res) => Ok(res),
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
    use rcgen::generate_simple_self_signed;

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

    #[derive(Clone, Debug)]
    struct BlockingHttpConnector {
        started: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
        calls: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl SmithyHttpConnector for BlockingHttpConnector {
        fn call(&self, _request: HttpRequest) -> HttpConnectorFuture {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let started = Arc::clone(&self.started);
            let release = Arc::clone(&self.release);
            HttpConnectorFuture::new(async move {
                started.notify_one();
                release.notified().await;
                Ok(HttpResponse::new(
                    aws_smithy_runtime_api::http::StatusCode::try_from(204_u16).expect("204 should be a valid response status"),
                    SdkBody::empty(),
                ))
            })
        }
    }

    fn recording_target_client() -> (TargetClient, Arc<std::sync::Mutex<Vec<String>>>) {
        let request_uris = Arc::new(std::sync::Mutex::new(Vec::new()));
        let connector = SharedHttpConnector::new(RecordingHttpConnector {
            request_uris: Arc::clone(&request_uris),
        });
        let http_client = http_client_fn(move |_settings, _components| connector.clone());
        let client = s3_client_with_http_client(443, http_client);
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

    fn blocking_target_client() -> (
        TargetClient,
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
        Arc<std::sync::atomic::AtomicUsize>,
    ) {
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let connector = SharedHttpConnector::new(BlockingHttpConnector {
            started: Arc::clone(&started),
            release: Arc::clone(&release),
            calls: Arc::clone(&calls),
        });
        let http_client = http_client_fn(move |_settings, _components| connector.clone());
        let client = s3_client_with_http_client(443, http_client);
        (
            TargetClient {
                endpoint: "https://localhost:443".to_string(),
                credentials: None,
                bucket: "target-bucket".to_string(),
                storage_class: String::new(),
                disable_proxy: false,
                arn: "old-arn".to_string(),
                reset_id: String::new(),
                secure: true,
                health_check_duration: Duration::from_secs(5),
                replicate_sync: false,
                client: Arc::new(client),
            },
            started,
            release,
            calls,
        )
    }

    fn spawn_single_request_https_server(cert: &rcgen::CertifiedKey<rcgen::KeyPair>) -> (u16, std::thread::JoinHandle<()>) {
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
            let (stream, _) = listener.accept().expect("test TLS client should connect");
            stream
                .set_read_timeout(Some(Duration::from_secs(10)))
                .expect("test TLS read timeout should configure");
            stream
                .set_write_timeout(Some(Duration::from_secs(10)))
                .expect("test TLS write timeout should configure");
            let connection = rustls::ServerConnection::new(Arc::new(server_config)).expect("test TLS connection should build");
            let mut stream = rustls::StreamOwned::new(connection, stream);
            let mut request = [0_u8; 8192];
            let _ = stream.read(&mut request).expect("test TLS request should be readable");
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .expect("test TLS response should be written");
            stream.flush().expect("test TLS response should flush");
        });

        (port, handle)
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
                let connection =
                    rustls::ServerConnection::new(Arc::clone(&server_config)).expect("test TLS connection should build");
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

    fn s3_client_with_http_client(port: u16, http_client: SharedHttpClient) -> S3Client {
        let credentials = SdkCredentials::builder()
            .access_key_id("test-access")
            .secret_access_key("test-secret")
            .provider_name("bucket_target_tls_test")
            .build();
        let config = S3Config::builder()
            .endpoint_url(format!("https://localhost:{port}"))
            .credentials_provider(SharedCredentialsProvider::new(credentials))
            .region(SdkRegion::new("us-east-1"))
            .force_path_style(true)
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .http_client(http_client)
            .build();

        S3Client::from_conf(config)
    }

    fn s3_client_for_endpoint_test(endpoint: String, http_client: Option<SharedHttpClient>) -> S3Client {
        let credentials = SdkCredentials::builder()
            .access_key_id("test-access")
            .secret_access_key("test-secret")
            .provider_name("bucket_target_health_test")
            .build();
        let mut config = S3Config::builder()
            .endpoint_url(endpoint)
            .credentials_provider(SharedCredentialsProvider::new(credentials))
            .region(SdkRegion::new("us-east-1"))
            .force_path_style(true)
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest());
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

    fn parse_url(raw: &str) -> Url {
        Url::parse(raw).expect("test URL should parse")
    }

    #[test]
    fn replication_endpoint_always_allows_public_and_private() {
        // Public hosts and private-network targets are allowed regardless of the
        // loopback opt-in — replication commonly runs across trusted private infra.
        for allow_loopback in [false, true] {
            assert!(validate_replication_target_endpoint_inner(&parse_url("https://s3.example.com"), allow_loopback).is_ok());
            assert!(validate_replication_target_endpoint_inner(&parse_url("http://10.0.0.5:9000"), allow_loopback).is_ok());
            assert!(validate_replication_target_endpoint_inner(&parse_url("http://192.168.1.20"), allow_loopback).is_ok());
        }
    }

    #[test]
    fn replication_endpoint_rejects_loopback_without_opt_in() {
        // Default (production) behaviour: loopback IP and localhost host both rejected.
        let err = validate_replication_target_endpoint_inner(&parse_url("http://127.0.0.1:9000"), false)
            .expect_err("loopback IP must be rejected by default");
        assert!(matches!(
            err,
            OutboundUrlError::ForbiddenHost {
                reason: "loopback address",
                ..
            }
        ));
        let err = validate_replication_target_endpoint_inner(&parse_url("http://localhost:9000"), false)
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
        assert!(validate_replication_target_endpoint_inner(&parse_url("http://127.0.0.1:9000"), true).is_ok());
        assert!(validate_replication_target_endpoint_inner(&parse_url("http://[::1]:9000"), true).is_ok());
        assert!(validate_replication_target_endpoint_inner(&parse_url("http://localhost:9000"), true).is_ok());
    }

    #[test]
    fn replication_endpoint_opt_in_does_not_open_other_ssrf_targets() {
        // The loopback opt-in must not widen into link-local / metadata endpoints.
        let err = validate_replication_target_endpoint_inner(&parse_url("http://169.254.169.254/latest/meta-data"), true)
            .expect_err("metadata endpoint must stay rejected even with loopback opt-in");
        assert!(matches!(
            err,
            OutboundUrlError::ForbiddenHost {
                reason: "metadata endpoint",
                ..
            }
        ));
        let err = validate_replication_target_endpoint_inner(&parse_url("http://[fe80::1]:9000"), true)
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
    fn canonical_endpoint_health_key_keeps_scheme_while_public_key_stays_compatible() {
        let explicit_port = Url::parse("https://REMOTE.example:9443").expect("url should parse");
        let default_port = Url::parse("https://REMOTE.example:443").expect("url should parse");
        let ipv6 = Url::parse("http://[2001:0db8::1]:9000").expect("url should parse");

        assert_eq!(canonical_endpoint_health_key(&explicit_port), "https://remote.example:9443");
        assert_eq!(canonical_endpoint_health_key(&default_port), "https://remote.example");
        assert_eq!(canonical_endpoint_health_key(&ipv6), "http://[2001:db8::1]:9000");
        assert_eq!(endpoint_health_key(&explicit_port), "remote.example:9443");
        assert_eq!(endpoint_health_key(&default_port), "remote.example");

        for (target, url) in [
            (
                BucketTarget {
                    endpoint: "REMOTE.example:443".to_string(),
                    secure: true,
                    ..Default::default()
                },
                default_port,
            ),
            (
                BucketTarget {
                    endpoint: "[2001:0db8::1]:9000".to_string(),
                    ..Default::default()
                },
                ipv6,
            ),
        ] {
            assert_eq!(
                target_endpoint_health_key(&target).as_deref(),
                Some(canonical_endpoint_health_key(&url).as_str())
            );
        }
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

    #[tokio::test]
    async fn target_health_check_rejects_untrusted_self_signed_certificate() {
        let cert = generate_simple_self_signed(vec!["localhost".to_string()]).expect("certificate should generate");
        let (port, server) = spawn_https_server(&cert, 1);
        let endpoint = format!("https://localhost:{port}");
        let target = target_client_for_test("arn:default-tls", endpoint.clone(), s3_client_for_endpoint_test(endpoint, None));

        assert!(!BucketTargetSys::check_endpoint_health(&target).await);
        server.join().expect("test TLS server should stop");
    }

    #[tokio::test]
    async fn target_health_check_honors_skip_tls_verify_client() {
        let cert = generate_simple_self_signed(vec!["localhost".to_string()]).expect("certificate should generate");
        let (port, server) = spawn_https_server(&cert, 1);
        let endpoint = format!("https://localhost:{port}");
        let target = target_client_for_test(
            "arn:skip-tls",
            endpoint.clone(),
            s3_client_for_endpoint_test(endpoint, Some(build_insecure_aws_s3_http_client())),
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
        let endpoint = format!("https://localhost:{port}");
        let target = target_client_for_test(
            "arn:custom-ca",
            endpoint.clone(),
            s3_client_for_endpoint_test(endpoint, Some(http_client)),
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
        let strict = target_client_for_test("arn:strict", endpoint.clone(), s3_client_for_endpoint_test(endpoint.clone(), None));
        let insecure = target_client_for_test(
            "arn:insecure",
            endpoint.clone(),
            s3_client_for_endpoint_test(endpoint, Some(build_insecure_aws_s3_http_client())),
        );
        {
            let mut remotes = sys.arn_remotes_map.write().await;
            remotes.insert(strict.arn.clone(), ArnTarget::with_client(strict.clone()));
            remotes.insert(insecure.arn.clone(), ArnTarget::with_client(insecure.clone()));
        }

        sys.heartbeat_once().await;

        assert!(sys.target_is_offline(&strict).await);
        assert!(!sys.target_is_offline(&insecure).await);
        server.join().expect("test TLS server should stop");
    }

    #[tokio::test]
    async fn heartbeat_discards_result_from_replaced_client_with_same_arn() {
        let sys = Arc::new(BucketTargetSys::default());
        let (stale, started, release, _) = blocking_target_client();
        let stale = Arc::new(stale);
        let target_arn = stale.arn.clone();
        sys.arn_remotes_map
            .write()
            .await
            .insert(stale.arn.clone(), ArnTarget::with_client(Arc::clone(&stale)));

        let heartbeat_sys = Arc::clone(&sys);
        let heartbeat = tokio::spawn(async move { heartbeat_sys.heartbeat_once().await });
        tokio::time::timeout(Duration::from_secs(10), started.notified())
            .await
            .expect("heartbeat should reach the stale target connector");

        let replacement_endpoint = "https://replacement.example:9443".to_string();
        let replacement = target_client_for_test(
            &target_arn,
            replacement_endpoint.clone(),
            s3_client_for_endpoint_test(replacement_endpoint, None),
        );
        sys.arn_remotes_map
            .write()
            .await
            .insert(replacement.arn.clone(), ArnTarget::with_client(Arc::clone(&replacement)));
        sys.init_target_health(&replacement).await;

        release.notify_one();
        tokio::time::timeout(Duration::from_secs(10), heartbeat)
            .await
            .expect("heartbeat should finish after the stale response")
            .expect("heartbeat task should not panic");

        assert!(!sys.target_is_offline(&replacement).await);
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
        let mutexes = sys.update_mutexes.lock().await;
        assert!(!mutexes.contains_key("first"));
        assert!(mutexes.contains_key("second"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn target_updates_serialize_client_build_through_publication_per_bucket() {
        let sys = Arc::new(BucketTargetSys::default());
        let started = Arc::new(tokio::sync::Semaphore::new(0));
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        *sys.target_client_build_probe.lock().await = Some(TargetClientBuildProbe {
            arn: "arn:first".to_string(),
            started: Arc::clone(&started),
            release: Arc::clone(&release),
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
        let first_sys = Arc::clone(&sys);
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
            started: Arc::clone(&second_started),
            release: Arc::clone(&second_release),
        });
        let second_sys = Arc::clone(&sys);
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
    async fn retired_probe_cannot_pollute_reactivated_public_health() {
        let sys = BucketTargetSys::default();
        let url = Url::parse("https://remote.example:9000").expect("URL should parse");
        sys.init_hc(&url).await;
        let old_probe = Arc::new(EndpointProbeState::default());
        old_probe.retire();

        sys.apply_probe_result("remote.example:9000", &old_probe, false, Duration::from_secs(1))
            .await;

        let health = sys.h_mutex.read().await;
        assert!(health["remote.example:9000"].online);
        assert_eq!(health["remote.example:9000"].scheme, "https");
    }

    #[tokio::test]
    async fn public_is_offline_keeps_the_unknown_endpoint_initialization_contract() {
        let sys = BucketTargetSys::default();
        let url = Url::parse("https://remote.example:9443").expect("url should parse");

        assert!(!sys.is_offline(&url).await);
        let health = sys.h_mutex.read().await;
        assert_eq!(health["remote.example:9443"].endpoint, "remote.example:9443");
        drop(health);
        assert!(sys.health_stats().await.contains_key("remote.example:9443"));
    }

    #[tokio::test]
    async fn public_init_replaces_health_without_letting_old_scheme_mark_it_offline() {
        let sys = BucketTargetSys::default();
        let http_url = Url::parse("http://remote.example:9000").expect("HTTP URL should parse");
        let https_url = Url::parse("https://remote.example:9000").expect("HTTPS URL should parse");
        sys.init_hc(&http_url).await;
        sys.mark_offline(&http_url).await;
        let old_probe = Arc::clone(
            sys.health_probe_states
                .read()
                .await
                .get("http://remote.example:9000")
                .expect("HTTP probe should exist"),
        );

        sys.init_hc(&https_url).await;
        sys.mark_offline(&http_url).await;

        let health = sys.h_mutex.read().await;
        assert!(health["remote.example:9000"].online);
        assert_eq!(health["remote.example:9000"].scheme, "https");
        drop(health);
        assert!(old_probe.acquire().await.is_none());
        assert!(
            sys.health_probe_states
                .read()
                .await
                .contains_key("https://remote.example:9000")
        );

        let https_probe = Arc::clone(
            sys.health_probe_states
                .read()
                .await
                .get("https://remote.example:9000")
                .expect("HTTPS probe should exist"),
        );
        sys.init_hc(&http_url).await;
        let health = sys.h_mutex.read().await;
        assert!(health["remote.example:9000"].online);
        assert_eq!(health["remote.example:9000"].scheme, "http");
        drop(health);
        assert!(https_probe.acquire().await.is_none());
    }

    #[tokio::test]
    async fn list_targets_applies_health_stats_for_endpoint_with_port() {
        let sys = BucketTargetSys::default();
        let url = Url::parse("https://remote.example:9443").expect("url should parse");
        sys.init_hc(&url).await;
        sys.mark_offline(&url).await;
        let public_stats = sys.health_stats().await;
        assert_eq!(public_stats["remote.example:9443"].endpoint, "remote.example:9443");
        assert!(!public_stats.contains_key("https://remote.example:9443"));

        let arn = "arn:rustfs:replication:us-east-1:bucket:id";
        sys.targets_map.write().await.insert(
            "bucket".to_string(),
            vec![BucketTarget {
                endpoint: "remote.example:9443".to_string(),
                arn: arn.to_string(),
                target_type: BucketTargetType::ReplicationService,
                secure: true,
                ..Default::default()
            }],
        );
        sys.target_h_mutex.write().await.insert(
            arn.to_string(),
            EpHealth {
                endpoint: "remote.example:9443".to_string(),
                scheme: "https".to_string(),
                online: false,
                offline_count: 1,
                ..Default::default()
            },
        );

        let targets = sys.list_targets("", "").await;

        assert_eq!(targets.len(), 1);
        assert!(!targets[0].online);
        assert_eq!(targets[0].offline_count, 1);
    }

    #[tokio::test]
    async fn endpoint_refresh_preserves_shared_health_then_delete_retires_it() {
        let sys = Arc::new(BucketTargetSys::default());
        sys.targets_map.write().await.extend([
            (
                "edited-bucket".to_string(),
                vec![BucketTarget {
                    endpoint: "old.example:9000".to_string(),
                    arn: "old-arn".to_string(),
                    ..Default::default()
                }],
            ),
            (
                "shared-bucket".to_string(),
                vec![BucketTarget {
                    endpoint: "old.example:9000".to_string(),
                    arn: "shared-arn".to_string(),
                    ..Default::default()
                }],
            ),
        ]);
        let old_url = Url::parse("http://old.example:9000").expect("old URL should parse");
        sys.init_hc(&old_url).await;
        let (mut old_client, _) = recording_target_client();
        old_client.endpoint = canonical_endpoint_health_key(&old_url);
        old_client.arn = "old-arn".to_string();
        let old_client = Arc::new(old_client);
        let (mut shared_client, _) = recording_target_client();
        shared_client.endpoint = canonical_endpoint_health_key(&old_url);
        shared_client.arn = "shared-arn".to_string();
        let mut arn_remotes_map = sys.arn_remotes_map.write().await;
        arn_remotes_map.insert("old-arn".to_string(), ArnTarget::with_client(Arc::clone(&old_client)));
        arn_remotes_map.insert("shared-arn".to_string(), ArnTarget::with_client(Arc::new(shared_client)));
        drop(arn_remotes_map);
        let old_probe = Arc::clone(
            sys.health_probe_states
                .read()
                .await
                .get("http://old.example:9000")
                .expect("old endpoint probe should exist"),
        );

        let updated = BucketTargets {
            targets: vec![BucketTarget {
                endpoint: "new.example:9000".to_string(),
                arn: "old-arn".to_string(),
                ..Default::default()
            }],
        };
        sys.update_all_targets("edited-bucket", Some(&updated)).await;
        let (mut current_client, _) = recording_target_client();
        current_client.endpoint = "http://new.example:9000".to_string();
        current_client.arn = "old-arn".to_string();
        sys.arn_remotes_map
            .write()
            .await
            .insert("old-arn".to_string(), ArnTarget::with_client(Arc::new(current_client)));

        assert!(sys.h_mutex.read().await.contains_key("old.example:9000"));
        assert!(sys.target_is_offline(&old_client).await);

        let probe_lease = old_probe.acquire().await.expect("shared endpoint should remain active");
        let delete_sys = Arc::clone(&sys);
        let delete = tokio::spawn(async move { delete_sys.delete("shared-bucket").await });
        tokio::time::timeout(Duration::from_secs(10), async {
            while sys.h_mutex.read().await.contains_key("old.example:9000") {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("delete should retire the last shared endpoint");
        assert!(!delete.is_finished(), "delete returned while an old endpoint probe was in flight");
        drop(probe_lease);
        tokio::time::timeout(Duration::from_secs(10), delete)
            .await
            .expect("delete should finish after the old probe exits")
            .expect("delete task should not panic");

        assert!(!sys.h_mutex.read().await.contains_key("old.example:9000"));
        assert!(old_probe.acquire().await.is_none());
        assert!(sys.target_is_offline(&old_client).await);
        assert!(sys.is_offline(&old_url).await);
        assert!(!sys.h_mutex.read().await.contains_key("old.example:9000"));
        assert!(!sys.health_probe_states.read().await.contains_key("http://old.example:9000"));

        sys.update_all_targets(
            "reactivated-bucket",
            Some(&BucketTargets {
                targets: vec![BucketTarget {
                    endpoint: "old.example:9000".to_string(),
                    arn: "reactivated-arn".to_string(),
                    credentials: Some(Credentials::default()),
                    ..Default::default()
                }],
            }),
        )
        .await;
        assert!(!sys.is_offline(&old_url).await);
        assert!(sys.h_mutex.read().await.contains_key("old.example:9000"));
    }

    #[tokio::test]
    async fn deleting_target_without_a_client_retires_its_health_probe() {
        let sys = BucketTargetSys::default();
        let old_url = Url::parse("http://old.example:9000").expect("old URL should parse");
        sys.targets_map.write().await.insert(
            "bucket".to_string(),
            vec![BucketTarget {
                endpoint: "old.example:9000".to_string(),
                arn: "missing-client-arn".to_string(),
                ..Default::default()
            }],
        );
        sys.init_hc(&old_url).await;

        sys.delete("bucket").await;

        assert!(sys.is_offline(&old_url).await);
        assert!(!sys.h_mutex.read().await.contains_key("old.example:9000"));
        assert!(!sys.health_probe_states.read().await.contains_key("http://old.example:9000"));
    }

    #[tokio::test]
    async fn editing_target_without_a_client_retires_its_old_health_probe() {
        let sys = BucketTargetSys::default();
        let old_url = Url::parse("http://old.example:9000").expect("old URL should parse");
        sys.targets_map.write().await.insert(
            "bucket".to_string(),
            vec![BucketTarget {
                endpoint: "old.example:9000".to_string(),
                arn: "missing-client-arn".to_string(),
                ..Default::default()
            }],
        );
        sys.init_hc(&old_url).await;

        sys.update_all_targets(
            "bucket",
            Some(&BucketTargets {
                targets: vec![BucketTarget {
                    endpoint: "new.example:9000".to_string(),
                    arn: "missing-client-arn".to_string(),
                    credentials: Some(Credentials::default()),
                    ..Default::default()
                }],
            }),
        )
        .await;

        assert!(sys.is_offline(&old_url).await);
        assert!(!sys.h_mutex.read().await.contains_key("old.example:9000"));
        assert!(!sys.health_probe_states.read().await.contains_key("http://old.example:9000"));
    }

    #[tokio::test]
    async fn deleting_one_bucket_preserves_a_client_referenced_by_another_bucket() {
        let sys = BucketTargetSys::default();
        let target = BucketTarget {
            arn: "shared-arn".to_string(),
            ..Default::default()
        };
        sys.targets_map.write().await.extend([
            ("bucket-a".to_string(), vec![target.clone()]),
            ("bucket-b".to_string(), vec![target.clone()]),
        ]);
        let (client, _) = recording_target_client();
        let client = Arc::new(client);
        sys.arn_remotes_map
            .write()
            .await
            .insert(target.arn.clone(), ArnTarget::with_client(Arc::clone(&client)));
        let generation = target_client_generation(&client).expect("registered client should have a generation");

        sys.delete("bucket-a").await;

        let current = sys
            .get_remote_target_client_by_arn("bucket-b", &target.arn)
            .await
            .expect("shared client should remain installed");
        assert!(Arc::ptr_eq(&client, &current));
        assert!(!generation.retired.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn same_arn_refresh_snapshots_the_client_after_arn_serialization() {
        let sys = Arc::new(BucketTargetSys::default());
        let target = BucketTarget {
            endpoint: "old.example:9000".to_string(),
            arn: "shared-arn".to_string(),
            ..Default::default()
        };
        sys.targets_map.write().await.extend([
            ("bucket-a".to_string(), vec![target.clone()]),
            ("bucket-b".to_string(), vec![target.clone()]),
        ]);
        let (mut old_client, _) = recording_target_client();
        old_client.endpoint = "http://old.example:9000".to_string();
        old_client.arn = target.arn.clone();
        let old_client = Arc::new(old_client);
        sys.arn_remotes_map
            .write()
            .await
            .insert(target.arn.clone(), ArnTarget::with_client(Arc::clone(&old_client)));
        let old_generation = target_client_generation(&old_client).expect("old client should have a generation");
        let arn_guard = sys.arn_update_mutex(&target.arn).await.lock_owned().await;

        let refresh_sys = Arc::clone(&sys);
        let refreshed_target = target.clone();
        let refresh = tokio::spawn(async move {
            refresh_sys
                .update_all_targets(
                    "bucket-b",
                    Some(&BucketTargets {
                        targets: vec![refreshed_target],
                    }),
                )
                .await;
        });
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let bucket_mutex = sys.target_update_mutex("bucket-b").await;
                if bucket_mutex.try_lock().is_err() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("refresh should wait on the shared ARN");

        old_generation.retire();
        let (mut new_client, _) = recording_target_client();
        new_client.endpoint = "http://new.example:9000".to_string();
        new_client.arn = target.arn.clone();
        let new_client = Arc::new(new_client);
        sys.arn_remotes_map
            .write()
            .await
            .insert(target.arn.clone(), ArnTarget::with_client(Arc::clone(&new_client)));
        drop(arn_guard);
        tokio::time::timeout(Duration::from_secs(10), refresh)
            .await
            .expect("refresh should finish after ARN release")
            .expect("refresh task should not panic");

        let current = sys
            .get_remote_target_client_by_arn("bucket-b", &target.arn)
            .await
            .expect("new client should remain installed");
        assert!(Arc::ptr_eq(&new_client, &current));
        assert!(old_generation.retired.load(Ordering::Acquire));
        assert!(
            !target_client_generation(&new_client)
                .expect("new client should have a generation")
                .retired
                .load(Ordering::Acquire)
        );
    }

    #[tokio::test]
    async fn endpoint_refresh_waits_for_in_flight_heartbeat_probe() {
        let sys = Arc::new(BucketTargetSys::default());
        let (mut old_client, started, release, _) = blocking_target_client();
        old_client.arn = "old-arn".to_string();
        old_client.endpoint = "https://localhost:444".to_string();
        let old_endpoint = old_client.endpoint.trim_start_matches("https://").to_string();
        sys.targets_map.write().await.insert(
            "edited-bucket".to_string(),
            vec![BucketTarget {
                endpoint: old_endpoint.clone(),
                arn: "old-arn".to_string(),
                secure: true,
                ..Default::default()
            }],
        );
        let old_url = Url::parse(&format!("https://{old_endpoint}")).expect("old URL should parse");
        sys.init_hc(&old_url).await;
        let old_client = Arc::new(old_client);
        let old_generation = register_target_client(&old_client);
        sys.arn_remotes_map
            .write()
            .await
            .insert("old-arn".to_string(), ArnTarget::with_client(old_client));

        let heartbeat_sys = Arc::clone(&sys);
        let heartbeat = tokio::spawn(async move { heartbeat_sys.heartbeat_once().await });
        tokio::time::timeout(Duration::from_secs(10), started.notified())
            .await
            .expect("heartbeat should connect to the old endpoint");

        let refresh_sys = Arc::clone(&sys);
        let refresh = tokio::spawn(async move {
            refresh_sys
                .update_all_targets(
                    "edited-bucket",
                    Some(&BucketTargets {
                        targets: vec![BucketTarget {
                            endpoint: "new.example:9000".to_string(),
                            arn: "old-arn".to_string(),
                            ..Default::default()
                        }],
                    }),
                )
                .await;
        });
        tokio::time::timeout(Duration::from_secs(10), async {
            while !old_generation.retired.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("endpoint refresh should retire the old client generation");
        assert!(!refresh.is_finished(), "refresh returned while an old endpoint probe was in flight");

        release.notify_one();
        tokio::time::timeout(Duration::from_secs(10), heartbeat)
            .await
            .expect("heartbeat should finish after the response")
            .expect("heartbeat task should not panic");
        tokio::time::timeout(Duration::from_secs(10), refresh)
            .await
            .expect("refresh should finish after the old probe exits")
            .expect("refresh task should not panic");
        let health = sys.h_mutex.read().await;
        assert!(
            !health.contains_key(&endpoint_health_key(&old_url)),
            "retired endpoint health remained: {health:?}"
        );
    }

    #[tokio::test]
    async fn target_delete_waits_for_in_flight_heartbeat_probe() {
        let sys = Arc::new(BucketTargetSys::default());
        let (mut old_client, started, release, _) = blocking_target_client();
        old_client.arn = "old-arn".to_string();
        old_client.endpoint = "https://localhost:444".to_string();
        let old_endpoint = old_client.endpoint.trim_start_matches("https://").to_string();
        let old_url = old_client.to_url();
        sys.targets_map.write().await.insert(
            "bucket".to_string(),
            vec![BucketTarget {
                endpoint: old_endpoint,
                arn: "old-arn".to_string(),
                secure: true,
                ..Default::default()
            }],
        );
        sys.init_hc(&old_url).await;
        let old_client = Arc::new(old_client);
        let old_generation = register_target_client(&old_client);
        sys.arn_remotes_map
            .write()
            .await
            .insert("old-arn".to_string(), ArnTarget::with_client(old_client));

        let heartbeat_sys = Arc::clone(&sys);
        let heartbeat = tokio::spawn(async move { heartbeat_sys.heartbeat_once().await });
        tokio::time::timeout(Duration::from_secs(10), started.notified())
            .await
            .expect("heartbeat should connect to the old endpoint");
        let delete_sys = Arc::clone(&sys);
        let delete = tokio::spawn(async move { delete_sys.delete("bucket").await });
        tokio::time::timeout(Duration::from_secs(10), async {
            while !old_generation.retired.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("delete should retire the old client generation");
        assert!(!delete.is_finished(), "delete returned while an old endpoint probe was in flight");

        release.notify_one();
        tokio::time::timeout(Duration::from_secs(10), heartbeat)
            .await
            .expect("heartbeat should finish after the response")
            .expect("heartbeat task should not panic");
        tokio::time::timeout(Duration::from_secs(10), delete)
            .await
            .expect("delete should finish after the old probe exits")
            .expect("delete task should not panic");
        assert!(!sys.h_mutex.read().await.contains_key(&endpoint_health_key(&old_url)));
        sys.heartbeat_once().await;
    }

    #[tokio::test]
    async fn unchanged_target_refresh_reuses_the_active_client() {
        let sys = BucketTargetSys::default();
        let target = BucketTarget {
            endpoint: "old.example:9000".to_string(),
            arn: "old-arn".to_string(),
            ..Default::default()
        };
        sys.targets_map
            .write()
            .await
            .insert("bucket".to_string(), vec![target.clone()]);
        let (mut client, _) = recording_target_client();
        client.endpoint = "http://old.example:9000".to_string();
        client.arn = target.arn.clone();
        let client = Arc::new(client);
        sys.arn_remotes_map
            .write()
            .await
            .insert(target.arn.clone(), ArnTarget::with_client(Arc::clone(&client)));

        sys.update_all_targets(
            "bucket",
            Some(&BucketTargets {
                targets: vec![target.clone()],
            }),
        )
        .await;

        let current = sys
            .get_remote_target_client_by_arn("bucket", &target.arn)
            .await
            .expect("unchanged target should retain its client");
        assert!(Arc::ptr_eq(&client, &current));
        assert!(
            target_client_generation(&client)
                .expect("registered client should have a generation")
                .acquire()
                .await
                .is_some()
        );
    }

    #[test]
    fn target_client_config_detects_generation_changing_fields() {
        let target = BucketTarget {
            endpoint: "old.example:9000".to_string(),
            arn: "old-arn".to_string(),
            target_bucket: "target-bucket".to_string(),
            storage_class: "STANDARD".to_string(),
            reset_id: "reset-id".to_string(),
            health_check_duration: Duration::from_secs(5),
            replication_sync: true,
            region: "us-east-1".to_string(),
            path: "path".to_string(),
            credentials: Some(Credentials {
                access_key: "access".to_string(),
                secret_key: "secret".to_string(),
                session_token: Some("session".to_string()),
                expiration: Some(chrono::Utc::now()),
            }),
            ca_cert_pem: "ca".to_string(),
            ..Default::default()
        };
        assert!(target_client_config_unchanged(&target, &target.clone()));

        let mut changed_targets = Vec::new();
        macro_rules! change {
            ($field:ident, $value:expr) => {{
                let mut changed = target.clone();
                changed.$field = $value;
                changed_targets.push(changed);
            }};
        }
        change!(endpoint, "new.example:9000".to_string());
        change!(secure, true);
        change!(target_bucket, "other-bucket".to_string());
        change!(storage_class, "GLACIER".to_string());
        change!(disable_proxy, true);
        change!(arn, "new-arn".to_string());
        change!(reset_id, "new-reset".to_string());
        change!(health_check_duration, Duration::from_secs(6));
        change!(replication_sync, false);
        change!(region, "eu-west-1".to_string());
        change!(path, "dns".to_string());
        change!(skip_tls_verify, true);
        change!(ca_cert_pem, "new-ca".to_string());
        for (field, value) in [
            ("access_key", "changed-access"),
            ("secret_key", "changed-secret"),
            ("session_token", "changed-session"),
        ] {
            let mut changed = target.clone();
            let credentials = changed.credentials.as_mut().expect("credentials should exist");
            match field {
                "access_key" => credentials.access_key = value.to_string(),
                "secret_key" => credentials.secret_key = value.to_string(),
                "session_token" => credentials.session_token = Some(value.to_string()),
                _ => unreachable!(),
            }
            changed_targets.push(changed);
        }
        let mut expiration = target.clone();
        expiration.credentials.as_mut().expect("credentials should exist").expiration = None;
        changed_targets.push(expiration);
        let mut no_credentials = target.clone();
        no_credentials.credentials = None;
        changed_targets.push(no_credentials);

        for changed in changed_targets {
            assert!(!target_client_config_unchanged(&target, &changed));
        }
    }

    #[tokio::test]
    async fn current_target_client_rejects_stale_probe_snapshot_fields() {
        let sys = BucketTargetSys::default();
        let target = BucketTarget {
            arn: "current-arn".to_string(),
            target_bucket: "current-target-bucket".to_string(),
            target_type: BucketTargetType::ReplicationService,
            deployment_id: "current-deployment".to_string(),
            ..Default::default()
        };
        sys.targets_map
            .write()
            .await
            .insert("source-bucket".to_string(), vec![target.clone()]);
        let (client, _) = recording_target_client();
        sys.arn_remotes_map
            .write()
            .await
            .insert(target.arn.clone(), ArnTarget::with_client(Arc::new(client)));

        assert!(
            sys.get_remote_target_client_if_current("source-bucket", &target)
                .await
                .is_some()
        );
        for stale in [
            BucketTarget {
                target_bucket: "stale-target-bucket".to_string(),
                ..target.clone()
            },
            BucketTarget {
                target_type: BucketTargetType::IlmService,
                ..target.clone()
            },
            BucketTarget {
                deployment_id: "stale-deployment".to_string(),
                ..target.clone()
            },
        ] {
            assert!(
                sys.get_remote_target_client_if_current("source-bucket", &stale)
                    .await
                    .is_none()
            );
        }
    }

    #[tokio::test]
    async fn retired_client_cannot_mark_the_current_endpoint_offline() {
        let sys = BucketTargetSys::default();
        let (client, _) = recording_target_client();
        let client = Arc::new(client);
        let generation = register_target_client(&client);
        let url = client.to_url();
        sys.init_hc(&url).await;
        generation.retire();

        sys.mark_target_offline(&client).await;

        assert!(
            sys.h_mutex
                .read()
                .await
                .get(&endpoint_health_key(&client.to_url()))
                .expect("endpoint health should remain present")
                .online
        );
    }

    #[tokio::test]
    async fn client_retired_while_waiting_for_health_lock_cannot_reinsert_old_endpoint() {
        let sys = Arc::new(BucketTargetSys::default());
        let (client, _) = recording_target_client();
        let client = Arc::new(client);
        let generation = register_target_client(&client);
        sys.arn_remotes_map
            .write()
            .await
            .insert(client.arn.clone(), ArnTarget::with_client(Arc::clone(&client)));
        let health_guard = sys.target_h_mutex.write().await;

        let check_sys = Arc::clone(&sys);
        let check_client = Arc::clone(&client);
        let check = tokio::spawn(async move { check_sys.target_is_offline(&check_client).await });
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if generation.in_flight.try_write().is_err() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("offline check should acquire the client generation lease");

        generation.retire();
        drop(health_guard);
        assert!(
            tokio::time::timeout(Duration::from_secs(10), check)
                .await
                .expect("offline check should finish")
                .expect("offline check task should not panic")
        );
        assert!(!sys.target_h_mutex.read().await.contains_key(&client.arn));
    }

    #[tokio::test]
    async fn retired_client_rejects_every_target_operation_before_dispatch() {
        let (client, request_uris) = recording_target_client();
        let client = Arc::new(client);
        let generation = register_target_client(&client);
        generation.retire();
        let put_options = PutObjectOptions::default();

        assert!(client.bucket_exists("target-bucket").await.is_err());
        assert!(client.get_bucket_versioning("target-bucket").await.is_err());
        assert!(client.head_object("target-bucket", "object", None).await.is_err());
        assert!(
            client
                .put_object("target-bucket", "object", 1, ByteStream::from_static(b"x"), &put_options,)
                .await
                .is_err()
        );
        assert!(
            client
                .create_multipart_upload("target-bucket", "object", &put_options)
                .await
                .is_err()
        );
        assert!(
            client
                .put_object_part(
                    "target-bucket",
                    "object",
                    "upload-id",
                    1,
                    1,
                    ByteStream::from_static(b"x"),
                    &PutObjectPartOptions::default(),
                )
                .await
                .is_err()
        );
        assert!(
            client
                .complete_multipart_upload("target-bucket", "object", "upload-id", Vec::new(), &put_options)
                .await
                .is_err()
        );
        assert!(
            client
                .remove_object("target-bucket", "object", None, remove_opts(false, false))
                .await
                .is_err()
        );
        assert!(
            client
                .send_with_operation_lease(client.client.list_object_versions().bucket("target-bucket").send())
                .await
                .is_err()
        );
        assert!(
            request_uris
                .lock()
                .expect("recorded request lock should not be poisoned")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn endpoint_refresh_waits_for_in_flight_target_request_and_rejects_late_dispatch() {
        let sys = Arc::new(BucketTargetSys::default());
        let old_target = BucketTarget {
            endpoint: "localhost:443".to_string(),
            secure: true,
            arn: "old-arn".to_string(),
            ..Default::default()
        };
        sys.targets_map.write().await.extend([
            ("bucket".to_string(), vec![old_target.clone()]),
            ("same-arn-bucket".to_string(), vec![old_target]),
        ]);
        let (client, started, release, calls) = blocking_target_client();
        let client = Arc::new(client);
        sys.arn_remotes_map
            .write()
            .await
            .insert("old-arn".to_string(), ArnTarget::with_client(Arc::clone(&client)));
        let generation = target_client_generation(&client).expect("registered client should have a generation");

        let request_client = Arc::clone(&client);
        let request = tokio::spawn(async move { request_client.bucket_exists("target-bucket").await });
        tokio::time::timeout(Duration::from_secs(10), started.notified())
            .await
            .expect("target request should reach the connector");

        let refresh_sys = Arc::clone(&sys);
        let refresh = tokio::spawn(async move {
            refresh_sys
                .update_all_targets(
                    "bucket",
                    Some(&BucketTargets {
                        targets: vec![BucketTarget {
                            endpoint: "new.example:9000".to_string(),
                            arn: "old-arn".to_string(),
                            ..Default::default()
                        }],
                    }),
                )
                .await;
        });
        tokio::time::timeout(Duration::from_secs(10), async {
            while !generation.retired.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("endpoint refresh should retire the old client");
        assert!(!refresh.is_finished(), "refresh returned while an old target request was in flight");
        let bucket_update_mutex = sys.target_update_mutex("bucket").await;
        assert!(
            bucket_update_mutex.try_lock().is_err(),
            "target updates must remain serialized while retired requests drain"
        );
        tokio::time::timeout(Duration::from_secs(10), sys.update_all_targets("other-bucket", None))
            .await
            .expect("an unrelated bucket update must not wait for the retired request");
        let same_arn_sys = Arc::clone(&sys);
        let same_arn_refresh = tokio::spawn(async move {
            same_arn_sys
                .update_all_targets(
                    "same-arn-bucket",
                    Some(&BucketTargets {
                        targets: vec![BucketTarget {
                            endpoint: "new.example:9000".to_string(),
                            arn: "old-arn".to_string(),
                            ..Default::default()
                        }],
                    }),
                )
                .await;
        });
        let same_arn_bucket_mutex = sys.target_update_mutex("same-arn-bucket").await;
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if same_arn_bucket_mutex.try_lock().is_err() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("same-ARN refresh should reach the ARN serialization gate");
        assert!(
            !same_arn_refresh.is_finished(),
            "same-ARN updates must remain serialized while retired requests drain"
        );

        release.notify_one();
        assert!(
            tokio::time::timeout(Duration::from_secs(10), request)
                .await
                .expect("target request should finish after release")
                .expect("target request task should not panic")
                .expect("mock target should return success")
        );
        tokio::time::timeout(Duration::from_secs(10), refresh)
            .await
            .expect("refresh should finish after the target request exits")
            .expect("refresh task should not panic");
        tokio::time::timeout(Duration::from_secs(10), same_arn_refresh)
            .await
            .expect("same-ARN refresh should finish after the target request exits")
            .expect("same-ARN refresh task should not panic");

        let call_count = calls.load(Ordering::SeqCst);
        let error = client
            .bucket_exists("target-bucket")
            .await
            .expect_err("retired client must reject late requests");
        assert_eq!(error.to_string(), ERR_TARGET_CLIENT_RETIRED);
        assert_eq!(calls.load(Ordering::SeqCst), call_count);
    }

    #[tokio::test]
    async fn cancelled_refresh_before_commit_preserves_the_old_target() {
        let sys = Arc::new(BucketTargetSys::default());
        let target = BucketTarget {
            arn: "old-arn".to_string(),
            ..Default::default()
        };
        sys.targets_map
            .write()
            .await
            .insert("bucket".to_string(), vec![target.clone()]);
        let (client, _) = recording_target_client();
        let client = Arc::new(client);
        sys.arn_remotes_map
            .write()
            .await
            .insert(target.arn.clone(), ArnTarget::with_client(Arc::clone(&client)));
        let generation = target_client_generation(&client).expect("registered client should have a generation");
        let health_guard = sys.h_mutex.write().await;

        let refresh_sys = Arc::clone(&sys);
        let refresh = tokio::spawn(async move {
            refresh_sys
                .update_all_targets(
                    "bucket",
                    Some(&BucketTargets {
                        targets: vec![BucketTarget {
                            endpoint: "new.example:9000".to_string(),
                            arn: "old-arn".to_string(),
                            credentials: Some(Credentials::default()),
                            ..Default::default()
                        }],
                    }),
                )
                .await;
        });
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if sys.targets_map.try_write().is_err() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("refresh should reach the pre-commit health lock");
        refresh.abort();
        let _ = refresh.await;
        drop(health_guard);

        let targets_map = sys.targets_map.read().await;
        let current_targets = targets_map.get("bucket").expect("old target config should remain installed");
        assert_eq!(current_targets.len(), 1);
        assert_eq!(current_targets[0].arn, target.arn);
        drop(targets_map);
        let current = sys
            .get_remote_target_client_by_arn("bucket", "old-arn")
            .await
            .expect("old client should remain installed");
        assert!(Arc::ptr_eq(&client, &current));
        assert!(!generation.retired.load(Ordering::Acquire));
        assert!(
            client
                .bucket_exists("target-bucket")
                .await
                .expect("old client should remain usable")
        );
    }

    #[tokio::test]
    async fn cancelled_refresh_keeps_the_bucket_serialized_until_retired_requests_drain() {
        let sys = Arc::new(BucketTargetSys::default());
        sys.targets_map.write().await.insert(
            "bucket".to_string(),
            vec![BucketTarget {
                endpoint: "localhost:443".to_string(),
                secure: true,
                arn: "old-arn".to_string(),
                ..Default::default()
            }],
        );
        let (client, started, release, _) = blocking_target_client();
        let client = Arc::new(client);
        sys.arn_remotes_map
            .write()
            .await
            .insert("old-arn".to_string(), ArnTarget::with_client(Arc::clone(&client)));
        let generation = target_client_generation(&client).expect("registered client should have a generation");

        let request_client = Arc::clone(&client);
        let request = tokio::spawn(async move { request_client.bucket_exists("target-bucket").await });
        tokio::time::timeout(Duration::from_secs(10), started.notified())
            .await
            .expect("target request should reach the connector");

        let refresh_sys = Arc::clone(&sys);
        let refresh = tokio::spawn(async move { refresh_sys.update_all_targets("bucket", None).await });
        tokio::time::timeout(Duration::from_secs(10), async {
            while !generation.retired.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("refresh should retire the client");
        refresh.abort();
        let _ = refresh.await;

        let bucket_update_mutex = sys.target_update_mutex("bucket").await;
        assert!(
            bucket_update_mutex.try_lock().is_err(),
            "cancellation must not release the bucket update lease before drain"
        );

        release.notify_one();
        tokio::time::timeout(Duration::from_secs(10), request)
            .await
            .expect("target request should finish after release")
            .expect("target request task should not panic")
            .expect("mock target should return success");
        tokio::time::timeout(Duration::from_secs(10), sys.update_all_targets("bucket", None))
            .await
            .expect("next update should finish after drain");
    }

    #[tokio::test]
    async fn target_delete_waits_for_in_flight_target_request() {
        let sys = Arc::new(BucketTargetSys::default());
        sys.targets_map.write().await.insert(
            "bucket".to_string(),
            vec![BucketTarget {
                arn: "old-arn".to_string(),
                ..Default::default()
            }],
        );
        let (client, started, release, _) = blocking_target_client();
        let client = Arc::new(client);
        sys.arn_remotes_map
            .write()
            .await
            .insert("old-arn".to_string(), ArnTarget::with_client(Arc::clone(&client)));
        let generation = target_client_generation(&client).expect("registered client should have a generation");

        let request_client = Arc::clone(&client);
        let request = tokio::spawn(async move { request_client.bucket_exists("target-bucket").await });
        tokio::time::timeout(Duration::from_secs(10), started.notified())
            .await
            .expect("target request should reach the connector");
        let delete_sys = Arc::clone(&sys);
        let delete = tokio::spawn(async move { delete_sys.delete("bucket").await });
        tokio::time::timeout(Duration::from_secs(10), async {
            while !generation.retired.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("delete should retire the client");
        assert!(!delete.is_finished(), "delete returned while a target request was in flight");

        release.notify_one();
        tokio::time::timeout(Duration::from_secs(10), request)
            .await
            .expect("target request should finish after release")
            .expect("target request task should not panic")
            .expect("mock target should return success");
        tokio::time::timeout(Duration::from_secs(10), delete)
            .await
            .expect("delete should finish after the target request exits")
            .expect("delete task should not panic");
    }

    #[tokio::test]
    async fn cancelled_delete_keeps_the_bucket_serialized_until_retired_requests_drain() {
        let sys = Arc::new(BucketTargetSys::default());
        sys.targets_map.write().await.insert(
            "bucket".to_string(),
            vec![BucketTarget {
                arn: "old-arn".to_string(),
                ..Default::default()
            }],
        );
        let (client, started, release, _) = blocking_target_client();
        let client = Arc::new(client);
        sys.arn_remotes_map
            .write()
            .await
            .insert("old-arn".to_string(), ArnTarget::with_client(Arc::clone(&client)));
        let generation = target_client_generation(&client).expect("registered client should have a generation");
        let request_client = Arc::clone(&client);
        let request = tokio::spawn(async move { request_client.bucket_exists("target-bucket").await });
        tokio::time::timeout(Duration::from_secs(10), started.notified())
            .await
            .expect("target request should reach the connector");

        let delete_sys = Arc::clone(&sys);
        let delete = tokio::spawn(async move { delete_sys.delete("bucket").await });
        tokio::time::timeout(Duration::from_secs(10), async {
            while !generation.retired.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("delete should retire the client");
        delete.abort();
        let _ = delete.await;
        let bucket_update_mutex = sys.target_update_mutex("bucket").await;
        assert!(bucket_update_mutex.try_lock().is_err(), "cancelled delete must keep the drain lease");

        release.notify_one();
        tokio::time::timeout(Duration::from_secs(10), request)
            .await
            .expect("target request should finish after release")
            .expect("target request task should not panic")
            .expect("mock target should return success");
        tokio::time::timeout(Duration::from_secs(10), sys.update_all_targets("bucket", None))
            .await
            .expect("next update should finish after delete drain");
    }

    #[tokio::test]
    async fn endpoint_refresh_retires_health_for_the_old_scheme() {
        let sys = BucketTargetSys::default();
        sys.targets_map.write().await.insert(
            "edited-bucket".to_string(),
            vec![BucketTarget {
                endpoint: "remote.example:9000".to_string(),
                ..Default::default()
            }],
        );
        sys.init_hc(&Url::parse("http://remote.example:9000").expect("HTTP URL should parse"))
            .await;

        sys.update_all_targets(
            "edited-bucket",
            Some(&BucketTargets {
                targets: vec![BucketTarget {
                    endpoint: "remote.example:9000".to_string(),
                    secure: true,
                    ..Default::default()
                }],
            }),
        )
        .await;

        assert!(!sys.h_mutex.read().await.contains_key("remote.example:9000"));
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

        let (global_port, global_server) = spawn_single_request_https_server(&global_ca);
        s3_client_with_http_client(global_port, http_client.clone())
            .head_bucket()
            .bucket("test-bucket")
            .send()
            .await
            .expect("global RUSTFS_TLS_PATH CA should authenticate its TLS server");
        global_server.join().expect("global CA TLS server should finish");

        let (target_port, target_server) = spawn_single_request_https_server(&target_ca);
        s3_client_with_http_client(target_port, http_client)
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
        let client = build_aws_s3_http_client_for_target(&BucketTarget {
            secure: true,
            skip_tls_verify: true,
            ca_cert_pem: "not a pem".to_string(),
            ..Default::default()
        })
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
}
