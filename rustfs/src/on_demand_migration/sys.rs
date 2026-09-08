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

//! Per-node on-demand migration runtime (rustfs/backlog#2152).
//!
//! `OnDemandMigrationSys` turns each bucket's persisted
//! [`OnDemandMigrationConfig`] into a live [`BucketOdmState`]: a
//! [`SourceClient`], a circuit breaker, a negative cache, a per-key
//! singleflight table, a pull concurrency limit and counters. Its lifecycle
//! follows the bucket metadata cache through the publish hook registered in
//! [`BUCKET_CONFIG_PUBLISH_HOOK`]; the hook fires on every cache install
//! path (initial load, admin update, peer reload, refresh loop, lazy load).
//!
//! Change detection compares the config by value (`PartialEq`) rather than
//! by `updated_at`. The bucket incarnation is part of this comparison:
//! recreating a bucket must cancel old work even with identical configuration.
//!
//! Client construction is async (TLS material may be read from disk), so
//! the hook does not build inline: `publish` removes state synchronously and
//! spawns `apply` for installs, and a per-call generation number makes sure
//! a slower, older install can never overwrite a newer one.
//!
//! The module switch (`RUSTFS_ON_DEMAND_MIGRATION_ENABLED`, default on) is
//! injected by the `rustfs` binary through [`OnDemandMigrationSys::set_module_enabled`]
//! before bucket metadata loads; this crate never reads the environment.
//! The same startup step injects the [`OdmWriteBack`] the pull pipeline
//! (`pull.rs`) stores objects with; each bucket state captures it at build
//! time together with its lazily started [`PullQueue`].

use super::backfill::{PriorityPullPermits, PullPermit, PullPriority};
use super::breaker::{Breaker, BreakerState, BreakerTransition, BreakerVerdict};
use super::config::{OnDemandMigrationConfig, PathStyle as ConfigPathStyle, Provider, SourceConfig};
use super::list_through::{SOURCE_LIST_RATE_PER_SEC, SourceListRateLimiter};
use super::negative_cache::NegativeCache;
use super::pull::{OdmWriteBack, PullQueue};
use super::source_client::{
    AzureAuth, AzureSourceSpec, GcsSourceSpec, SourceBackendSpec, SourceClient, SourceClientSpec, SourceError, SourceProvider,
    SourceTimeouts,
};
use super::stats::{GaugeGuard, OdmStats, OdmStatsSnapshot, PullFailureReason};
use super::storage_api::remote_s3_client::{
    PathStyle as ClientPathStyle, RemoteCredentials, RemoteS3ClientError, RemoteS3RetryPolicy,
};
use super::storage_api::{BUCKET_CONFIG_PUBLISH_HOOK, BUCKET_ON_DEMAND_MIGRATION_CONFIG};
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use time::OffsetDateTime;
use tokio::sync::watch;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use url::Url;

const EVENT_ODM_BUCKET_STATE_APPLIED: &str = "odm_bucket_state_applied";
const EVENT_ODM_BREAKER_TRANSITION: &str = "odm_breaker_transition";
const EVENT_ODM_SOURCE_ERROR: &str = "odm_source_error";
const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_ON_DEMAND_MIGRATION: &str = "on_demand_migration";

/// Minimum spacing between `EVENT_ODM_SOURCE_ERROR` records per bucket.
const SOURCE_ERROR_LOG_INTERVAL: Duration = Duration::from_secs(10);

pub static GLOBAL_ON_DEMAND_MIGRATION_SYS: OnceLock<OnDemandMigrationSys> = OnceLock::new();

/// Why a configured bucket has no usable source client. Surfaced through
/// `resolve` as [`OdmLookup::Unavailable`] and through status snapshots.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum OdmStateError {
    #[error("the {0} backend is not included in this build")]
    BackendNotCompiled(&'static str),
    /// `source.credentials` is `null`; the shared client builder has no
    /// anonymous mode yet (rustfs/backlog#2149 follow-up).
    #[error("anonymous source access is not supported yet; configure source credentials")]
    AnonymousUnsupported,
    #[error("source client could not be built: {0}")]
    ClientBuild(String),
}

/// One-shot verdict for a `(bucket, key)` lookup. `None` from
/// [`OnDemandMigrationSys::resolve`] means "do not intervene"; every `Some`
/// carries the bucket state so the handler can record its outcome.
#[derive(Clone, Debug)]
pub enum OdmLookup {
    /// The source answered 404 for this key recently.
    NegativeCached { state: Arc<BucketOdmState> },
    /// The breaker rejects source traffic right now.
    BreakerOpen { state: Arc<BucketOdmState> },
    /// The bucket is configured but its client could not be built.
    Unavailable {
        state: Arc<BucketOdmState>,
        error: OdmStateError,
    },
    /// Go to the source.
    Ready { state: Arc<BucketOdmState> },
}

impl OdmLookup {
    pub fn state(&self) -> &Arc<BucketOdmState> {
        match self {
            OdmLookup::NegativeCached { state }
            | OdmLookup::BreakerOpen { state }
            | OdmLookup::Unavailable { state, .. }
            | OdmLookup::Ready { state } => state,
        }
    }
}

/// What `apply` did with a bucket.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ApplyOutcome {
    /// No state before, none after.
    NotDesired,
    /// The state was removed and its cancellation token fired.
    Removed,
    /// Same config as the installed state; nothing rebuilt.
    Unchanged,
    /// First state for this bucket.
    Installed,
    /// A newer config replaced the previous state (counters carried over).
    Rebuilt,
    /// A later `apply`/`publish` for the same bucket won the race.
    Superseded,
}

/// Result of one pull as seen by the singleflight leader and its followers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PullOutcome {
    pub etag: Option<String>,
    pub size: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{}: {message}", reason.as_str())]
pub struct PullError {
    pub reason: PullFailureReason,
    pub message: String,
}

impl PullError {
    pub fn new(reason: PullFailureReason, message: impl Into<String>) -> Self {
        Self {
            reason,
            message: message.into(),
        }
    }

    pub fn canceled(message: impl Into<String>) -> Self {
        Self::new(PullFailureReason::Canceled, message)
    }
}

impl From<&SourceError> for PullError {
    fn from(err: &SourceError) -> Self {
        Self::new(PullFailureReason::from(err), err.to_string())
    }
}

pub type PullResult = Result<PullOutcome, PullError>;

/// Outcome of [`BucketOdmState::acquire_pull_slot`].
#[derive(Debug)]
pub enum PullSlot {
    /// This caller performs the pull and must call [`PullLeader::complete`].
    Leader(PullLeader),
    /// Another caller is pulling the same key; await [`PullFollower::wait`].
    Follower(PullFollower),
}

/// Held by the single puller of a key. Dropping it without `complete`
/// fails every follower with [`PullFailureReason::Canceled`].
pub struct PullLeader {
    state: Arc<BucketOdmState>,
    key: String,
    tx: watch::Sender<Option<PullResult>>,
    _permit: PullPermit,
    _inflight: GaugeGuard,
    completed: bool,
}

impl fmt::Debug for PullLeader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PullLeader")
            .field("bucket", &self.state.bucket)
            .field("key", &self.key)
            .field("completed", &self.completed)
            .finish_non_exhaustive()
    }
}

impl PullLeader {
    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn state(&self) -> &Arc<BucketOdmState> {
        &self.state
    }

    /// Publishes the result to every follower and releases the key.
    pub fn complete(mut self, result: PullResult) {
        self.completed = true;
        self.tx.send_replace(Some(result));
    }
}

impl Drop for PullLeader {
    fn drop(&mut self) {
        if !self.completed {
            self.tx
                .send_replace(Some(Err(PullError::canceled("pull leader exited without a result"))));
        }
        self.state.inflight.lock().remove(&self.key);
    }
}

#[derive(Debug)]
pub struct PullFollower {
    rx: watch::Receiver<Option<PullResult>>,
}

impl PullFollower {
    /// Resolves once the leader completes (or disappears).
    pub async fn wait(mut self) -> PullResult {
        match self.rx.wait_for(|result| result.is_some()).await {
            Ok(result) => result
                .clone()
                .unwrap_or_else(|| Err(PullError::canceled("pull leader vanished"))),
            Err(_) => Err(PullError::canceled("pull leader vanished")),
        }
    }
}

/// Removes the singleflight entry if the leader gives up before it exists.
struct InflightEntryGuard<'a> {
    state: &'a BucketOdmState,
    key: &'a str,
    tx: &'a watch::Sender<Option<PullResult>>,
    armed: bool,
}

impl Drop for InflightEntryGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.tx
                .send_replace(Some(Err(PullError::canceled("pull leader canceled before starting"))));
            self.state.inflight.lock().remove(self.key);
        }
    }
}

/// Live runtime for one bucket. Built by `apply`, replaced wholesale on a
/// config change (counters excepted), removed when the config goes away.
pub struct BucketOdmState {
    bucket: String,
    incarnation_id: uuid::Uuid,
    config: OnDemandMigrationConfig,
    applied_at: OffsetDateTime,
    endpoint_host: String,
    client: Result<Arc<SourceClient>, OdmStateError>,
    breaker: Breaker,
    negative_cache: NegativeCache,
    inflight: Mutex<HashMap<String, watch::Receiver<Option<PullResult>>>>,
    /// Online misses first, backfill pulls when nobody waits (ODM-12).
    pull_permits: Arc<PriorityPullPermits>,
    stats: Arc<OdmStats>,
    cancel: CancellationToken,
    last_source_error_logged_at: Mutex<Option<Instant>>,
    write_back: Option<Arc<dyn OdmWriteBack>>,
    /// Started by `pull::BucketOdmState::pull_queue` on first enqueue.
    pub(super) pull_queue: OnceLock<Arc<PullQueue>>,
    /// Caps source listings for this bucket under `policy.list_through`.
    list_rate_limiter: SourceListRateLimiter,
}

impl fmt::Debug for BucketOdmState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BucketOdmState")
            .field("bucket", &self.bucket)
            .field("provider", &self.config.source.provider)
            .field("endpoint_host", &self.endpoint_host)
            .field("applied_at", &self.applied_at)
            .field("client", &self.client.as_ref().map(|_| "ready"))
            .field("breaker", &self.breaker.state())
            .field("cancelled", &self.cancel.is_cancelled())
            .finish_non_exhaustive()
    }
}

impl BucketOdmState {
    async fn build(
        bucket: &str,
        config: &OnDemandMigrationConfig,
        incarnation_id: uuid::Uuid,
        stats: Arc<OdmStats>,
        write_back: Option<Arc<dyn OdmWriteBack>>,
    ) -> Arc<Self> {
        let spec = source_client_spec(config);
        let client = if config.source.credentials.is_none() && !config.source.provider.is_native() {
            Err(OdmStateError::AnonymousUnsupported)
        } else {
            SourceClient::new(&spec).await.map(Arc::new).map_err(|err| match err {
                RemoteS3ClientError::MissingCredentials => OdmStateError::AnonymousUnsupported,
                RemoteS3ClientError::BackendNotCompiled(provider) => OdmStateError::BackendNotCompiled(provider),
                other => OdmStateError::ClientBuild(other.to_string()),
            })
        };
        let policy = &config.policy;
        Arc::new(Self {
            bucket: bucket.to_string(),
            incarnation_id,
            endpoint_host: endpoint_host(&config.source),
            config: config.clone(),
            applied_at: OffsetDateTime::now_utc(),
            client,
            breaker: Breaker::new(),
            negative_cache: NegativeCache::new(Duration::from_secs(policy.negative_cache_ttl_secs)),
            inflight: Mutex::new(HashMap::new()),
            pull_permits: PriorityPullPermits::new(policy.max_concurrent_pulls.max(1) as usize),
            stats,
            cancel: CancellationToken::new(),
            last_source_error_logged_at: Mutex::new(None),
            write_back,
            pull_queue: OnceLock::new(),
            list_rate_limiter: SourceListRateLimiter::new(SOURCE_LIST_RATE_PER_SEC),
        })
    }

    pub fn filter_incarnation(self: Arc<Self>, incarnation_id: uuid::Uuid) -> Option<Arc<Self>> {
        (self.incarnation_id == incarnation_id && !self.is_cancelled()).then_some(self)
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub fn incarnation_id(&self) -> uuid::Uuid {
        self.incarnation_id
    }

    pub fn config(&self) -> &OnDemandMigrationConfig {
        &self.config
    }

    pub fn applied_at(&self) -> OffsetDateTime {
        self.applied_at
    }

    /// Host of the source endpoint, safe to log.
    pub fn endpoint_host(&self) -> &str {
        &self.endpoint_host
    }

    pub fn client(&self) -> Result<&Arc<SourceClient>, &OdmStateError> {
        self.client.as_ref()
    }

    pub fn breaker(&self) -> &Breaker {
        &self.breaker
    }

    pub fn negative_cache(&self) -> &NegativeCache {
        &self.negative_cache
    }

    /// Per-bucket rate limit on source `ListObjectsV2` calls, consulted by the
    /// list-through merge (rustfs/backlog#2164).
    pub fn list_rate_limiter(&self) -> &SourceListRateLimiter {
        &self.list_rate_limiter
    }

    pub fn stats(&self) -> &Arc<OdmStats> {
        &self.stats
    }

    /// The write-back injected by the binary when this state was built.
    pub fn write_back(&self) -> Option<&Arc<dyn OdmWriteBack>> {
        self.write_back.as_ref()
    }

    /// Fires when this state is replaced or removed; background pulls
    /// started for it must exit.
    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel.clone()
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }

    /// Whether `filter.prefix` admits this local key.
    pub fn matches_prefix(&self, key: &str) -> bool {
        self.config
            .filter
            .prefix
            .as_deref()
            .is_none_or(|prefix| key.starts_with(prefix))
    }

    /// The bucket-level part of [`OnDemandMigrationSys::resolve`].
    pub fn resolve_key(self: &Arc<Self>, key: &str) -> Option<OdmLookup> {
        if !self.matches_prefix(key) {
            return None;
        }
        if let Err(error) = &self.client {
            return Some(OdmLookup::Unavailable {
                state: Arc::clone(self),
                error: error.clone(),
            });
        }
        if self.negative_cache.contains(key) {
            return Some(OdmLookup::NegativeCached { state: Arc::clone(self) });
        }
        if !self.breaker.allow_request() {
            return Some(OdmLookup::BreakerOpen { state: Arc::clone(self) });
        }
        Some(OdmLookup::Ready { state: Arc::clone(self) })
    }

    /// Records the outcome of one source call: latency, breaker scoring,
    /// `last_source_error`, a rate-limited log line, and the negative cache
    /// on `NotFound`.
    pub fn observe_source(&self, latency: Duration, key: &str, error: Option<&SourceError>) {
        self.stats.record_source_latency(latency);
        if let Some(transition) = self.breaker.record(BreakerVerdict::for_result(error)) {
            self.log_breaker_transition(transition);
        }
        let Some(err) = error else {
            return;
        };
        if matches!(err, SourceError::NotFound) {
            self.negative_cache.insert(key);
            return;
        }
        self.stats.record_source_error(err);
        if self.should_log_source_error() {
            warn!(
                event = EVENT_ODM_SOURCE_ERROR,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_ON_DEMAND_MIGRATION,
                bucket = %self.bucket,
                provider = %self.config.source.provider,
                endpoint_host = %self.endpoint_host,
                error_class = err.class_label(),
                "On-demand migration source request failed"
            );
        }
    }

    fn should_log_source_error(&self) -> bool {
        let now = Instant::now();
        let mut last = self.last_source_error_logged_at.lock();
        if last.is_some_and(|at| now.saturating_duration_since(at) < SOURCE_ERROR_LOG_INTERVAL) {
            return false;
        }
        *last = Some(now);
        true
    }

    fn log_breaker_transition(&self, transition: BreakerTransition) {
        match transition.to {
            BreakerState::Open => warn!(
                event = EVENT_ODM_BREAKER_TRANSITION,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_ON_DEMAND_MIGRATION,
                state = transition.to.as_str(),
                previous_state = transition.from.as_str(),
                bucket = %self.bucket,
                provider = %self.config.source.provider,
                endpoint_host = %self.endpoint_host,
                "On-demand migration breaker opened; source traffic suspended"
            ),
            BreakerState::Closed | BreakerState::HalfOpen => info!(
                event = EVENT_ODM_BREAKER_TRANSITION,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_ON_DEMAND_MIGRATION,
                state = transition.to.as_str(),
                previous_state = transition.from.as_str(),
                bucket = %self.bucket,
                provider = %self.config.source.provider,
                endpoint_host = %self.endpoint_host,
                "On-demand migration breaker state changed"
            ),
        }
    }

    /// Singleflight plus concurrency limit for pulling `key`. The first
    /// caller per key becomes the leader and waits for one of
    /// `max_concurrent_pulls` permits (`queue_depth` counts that wait);
    /// later callers for the same key become followers and never touch the
    /// semaphore. Fails with `Canceled` when the state is torn down.
    pub async fn acquire_pull_slot(self: &Arc<Self>, key: &str) -> Result<PullSlot, PullError> {
        self.acquire_pull_slot_with_priority(key, PullPriority::Online).await
    }

    /// [`Self::acquire_pull_slot`] at the given permit priority; the pull
    /// queue passes `Backfill` for backfill jobs.
    pub async fn acquire_pull_slot_with_priority(
        self: &Arc<Self>,
        key: &str,
        priority: PullPriority,
    ) -> Result<PullSlot, PullError> {
        if self.cancel.is_cancelled() {
            return Err(PullError::canceled("bucket on-demand migration state was removed"));
        }
        let tx = {
            let mut inflight = self.inflight.lock();
            if let Some(rx) = inflight.get(key) {
                return Ok(PullSlot::Follower(PullFollower { rx: rx.clone() }));
            }
            let (tx, rx) = watch::channel(None);
            inflight.insert(key.to_string(), rx);
            tx
        };

        let mut entry_guard = InflightEntryGuard {
            state: self,
            key,
            tx: &tx,
            armed: true,
        };
        let permit = {
            let _queued = self.stats.queue_guard();
            tokio::select! {
                permit = self.pull_permits.acquire(priority) => permit,
                _ = self.cancel.cancelled() => {
                    return Err(PullError::canceled("bucket on-demand migration state was removed"));
                }
            }
        };
        let permit = permit.map_err(|_| PullError::canceled("pull semaphore closed"))?;
        entry_guard.armed = false;
        drop(entry_guard);

        Ok(PullSlot::Leader(PullLeader {
            state: Arc::clone(self),
            key: key.to_string(),
            tx,
            _permit: permit,
            _inflight: self.stats.inflight_guard(),
            completed: false,
        }))
    }

    /// Keys currently being pulled (leader registered).
    pub fn inflight_keys(&self) -> usize {
        self.inflight.lock().len()
    }

    pub fn pull_permits(&self) -> &Arc<PriorityPullPermits> {
        &self.pull_permits
    }

    pub fn snapshot(&self) -> OdmBucketSnapshot {
        OdmBucketSnapshot {
            bucket: self.bucket.clone(),
            provider: self.config.source.provider.as_str().to_string(),
            endpoint_host: self.endpoint_host.clone(),
            applied_at: self.applied_at,
            client_error: self.client.as_ref().err().map(|err| err.to_string()),
            negative_cache_entries: self.negative_cache.len(),
            inflight_keys: self.inflight_keys() as u64,
            max_concurrent_pulls: self.config.policy.max_concurrent_pulls,
            stats: self.stats.snapshot(self.breaker.state()),
        }
    }
}

/// Read-only status of one bucket's runtime, for admin/status consumers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OdmBucketSnapshot {
    pub bucket: String,
    pub provider: String,
    pub endpoint_host: String,
    #[serde(with = "time::serde::rfc3339")]
    pub applied_at: OffsetDateTime,
    /// `Some` when the source client could not be built.
    pub client_error: Option<String>,
    pub negative_cache_entries: u64,
    pub inflight_keys: u64,
    pub max_concurrent_pulls: u32,
    pub stats: OdmStatsSnapshot,
}

/// Maps the persisted config onto the client spec. `path_style`
/// `virtual` becomes the client's `VirtualHost`; `auto` is left for the
/// client to resolve per provider. `first_byte_ms` is the SDK read timeout;
/// `idle_ms` applies to body streaming and is enforced by the pull pipeline.
pub fn source_client_spec(config: &OnDemandMigrationConfig) -> SourceClientSpec {
    let source = &config.source;
    let policy = &config.policy;
    SourceClientSpec {
        endpoint: source.effective_endpoint(),
        region: source.effective_region().to_string(),
        bucket: source.bucket.clone(),
        source_prefix: config.filter.source_prefix.clone(),
        provider: source_provider(source.provider),
        path_style: match source.path_style {
            ConfigPathStyle::Auto => ClientPathStyle::Auto,
            ConfigPathStyle::Path => ClientPathStyle::Path,
            ConfigPathStyle::Virtual => ClientPathStyle::VirtualHost,
        },
        credentials: source.credentials.as_ref().map(|credentials| RemoteCredentials {
            access_key: credentials.access_key.clone(),
            secret_key: credentials.secret_key.clone(),
            session_token: credentials.session_token.clone(),
            expiration: None,
            account_id: String::new(),
        }),
        skip_tls_verify: source.tls.skip_verify,
        ca_cert_pem: source.tls.ca_cert_pem.clone(),
        timeouts: SourceTimeouts {
            connect: Duration::from_millis(policy.source_timeout.connect_ms),
            read: Duration::from_millis(policy.source_timeout.first_byte_ms),
        },
        // The pull pipeline and the backfill job already retry, and the
        // breaker counts logical calls: an SDK retry on top would triple the
        // load on a source that is already failing.
        retry: RemoteS3RetryPolicy::Disabled,
        bandwidth_limit: policy.bandwidth_limit_bytes_per_sec.and_then(NonZeroU64::new),
        backend: source_backend_spec(source),
    }
}

fn source_provider(provider: Provider) -> SourceProvider {
    match provider {
        Provider::S3 => SourceProvider::S3,
        Provider::Aws => SourceProvider::Aws,
        Provider::Minio => SourceProvider::Minio,
        Provider::Rustfs => SourceProvider::Rustfs,
        Provider::R2 => SourceProvider::R2,
        Provider::Gcs => SourceProvider::Gcs,
        Provider::Azure => SourceProvider::Azure,
        Provider::GcsNative => SourceProvider::GcsNative,
    }
}

/// Which backend the client builds. A native provider whose block is missing
/// falls back to the S3 spec, where the builder reports the missing
/// credentials: the config layer already refuses to store that shape, so this
/// only covers a config written by an older or hand-edited build.
pub fn source_backend_spec(source: &SourceConfig) -> SourceBackendSpec {
    match (source.provider, source.azure.as_ref(), source.gcs.as_ref()) {
        (Provider::Azure, Some(azure), _) => SourceBackendSpec::Azure(AzureSourceSpec {
            account: azure.account.clone(),
            auth: match (&azure.account_key, &azure.sas_token) {
                (Some(key), _) => AzureAuth::SharedKey(key.clone()),
                (None, Some(sas)) => AzureAuth::Sas(sas.clone()),
                // Refused by `SourceConfig::validate`; an empty shared key
                // fails closed at the builder rather than signing with none.
                (None, None) => AzureAuth::SharedKey(String::new()),
            },
        }),
        (Provider::GcsNative, _, Some(gcs)) => SourceBackendSpec::Gcs(GcsSourceSpec {
            service_account_json: gcs.service_account_json.clone(),
        }),
        _ => SourceBackendSpec::S3,
    }
}

fn endpoint_host(source: &SourceConfig) -> String {
    Url::parse(&source.effective_endpoint())
        .ok()
        .and_then(|url| url.host_str().map(str::to_ascii_lowercase))
        .unwrap_or_default()
}

#[derive(Default)]
struct BucketSlot {
    /// Generation of the last `apply`/`publish` that touched this bucket.
    generation: u64,
    state: Option<Arc<BucketOdmState>>,
}

/// Process-wide ODM runtime; see the module docs.
pub struct OnDemandMigrationSys {
    module_enabled: AtomicBool,
    buckets: RwLock<HashMap<String, BucketSlot>>,
    generation: AtomicU64,
    write_back: RwLock<Option<Arc<dyn OdmWriteBack>>>,
}

impl fmt::Debug for OnDemandMigrationSys {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OnDemandMigrationSys")
            .field("module_enabled", &self.is_module_enabled())
            .field("buckets", &self.bucket_names())
            .finish()
    }
}

impl Default for OnDemandMigrationSys {
    fn default() -> Self {
        Self::new()
    }
}

impl OnDemandMigrationSys {
    /// A detached instance; production code uses [`Self::get`].
    pub fn new() -> Self {
        Self {
            module_enabled: AtomicBool::new(false),
            buckets: RwLock::new(HashMap::new()),
            generation: AtomicU64::new(0),
            write_back: RwLock::new(None),
        }
    }

    pub fn get() -> &'static Self {
        GLOBAL_ON_DEMAND_MIGRATION_SYS.get_or_init(Self::new)
    }

    /// Publishes the module switch resolved by the binary. Default `false`.
    pub fn set_module_enabled(&self, enabled: bool) {
        self.module_enabled.store(enabled, Ordering::Relaxed);
    }

    pub fn is_module_enabled(&self) -> bool {
        self.module_enabled.load(Ordering::Relaxed)
    }

    /// Installs the local write path used by every bucket state built from
    /// now on (states built earlier keep what they captured). The binary
    /// calls this before bucket metadata loads.
    pub fn set_write_back(&self, write_back: Arc<dyn OdmWriteBack>) {
        *self.write_back.write() = Some(write_back);
    }

    pub fn write_back(&self) -> Option<Arc<dyn OdmWriteBack>> {
        self.write_back.read().clone()
    }

    /// Registers `publish` as the bucket-metadata publish hook. Returns
    /// `false` when a hook was already registered.
    pub fn register_config_hook(&'static self) -> bool {
        BUCKET_CONFIG_PUBLISH_HOOK
            .set(Box::new(move |bucket, config_file, stored| {
                if config_file == BUCKET_ON_DEMAND_MIGRATION_CONFIG {
                    self.publish_stored(bucket, stored.map(|(bytes, _, incarnation)| (bytes, incarnation)));
                }
            }))
            .is_ok()
    }

    /// Corrupt persisted bytes withdraw state synchronously, just like deletion.
    fn publish_stored(&'static self, bucket: &str, stored: Option<(&[u8], uuid::Uuid)>) {
        let incarnation_id = stored.map(|(_, id)| id).unwrap_or_default();
        match stored.map(|(bytes, _)| OnDemandMigrationConfig::from_json(bytes)).transpose() {
            Ok(config) => self.publish_for_incarnation(bucket, incarnation_id, config.as_ref()),
            Err(err) => {
                warn!(
                    event = EVENT_ODM_BUCKET_STATE_APPLIED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_ON_DEMAND_MIGRATION,
                    result = "invalid",
                    bucket = %bucket,
                    error = %err,
                    "Failed to parse on-demand migration config"
                );
                self.publish_for_incarnation(bucket, incarnation_id, None);
            }
        }
    }

    /// Hook entry point: removals apply immediately, installs are spawned
    /// (client construction is async). Requires a Tokio runtime for the
    /// install path; without one the config is logged and skipped.
    pub fn publish_for_incarnation(
        &'static self,
        bucket: &str,
        incarnation_id: uuid::Uuid,
        config: Option<&OnDemandMigrationConfig>,
    ) {
        let config = self.desired(config).filter(|_| !incarnation_id.is_nil());
        let generation = self.reserve_generation(bucket, config.is_some());
        let Some(config) = config else {
            self.remove_with_generation(bucket, generation);
            return;
        };
        if self.is_unchanged(bucket, incarnation_id, config, generation) {
            return;
        }
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            warn!(
                event = EVENT_ODM_BUCKET_STATE_APPLIED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_ON_DEMAND_MIGRATION,
                result = "skipped",
                bucket = %bucket,
                provider = %config.source.provider,
                endpoint_host = %endpoint_host(&config.source),
                "On-demand migration config published outside a Tokio runtime; state not built"
            );
            return;
        };
        let bucket = bucket.to_string();
        let config = config.clone();
        handle.spawn(async move {
            self.apply_with_generation(&bucket, incarnation_id, Some(&config), generation)
                .await;
        });
    }

    /// Installs, rebuilds, or removes the bucket state for `config`.
    /// Idempotent: the same config on an installed bucket is a no-op.
    #[cfg(test)]
    pub async fn apply(&self, bucket: &str, config: Option<&OnDemandMigrationConfig>) -> ApplyOutcome {
        self.apply_for_incarnation(bucket, uuid::Uuid::from_u128(1), config).await
    }

    #[cfg(test)]
    pub fn publish(&'static self, bucket: &str, config: Option<&OnDemandMigrationConfig>) {
        self.publish_for_incarnation(bucket, uuid::Uuid::from_u128(1), config);
    }

    pub async fn apply_for_incarnation(
        &self,
        bucket: &str,
        incarnation_id: uuid::Uuid,
        config: Option<&OnDemandMigrationConfig>,
    ) -> ApplyOutcome {
        let config = self.desired(config).filter(|_| !incarnation_id.is_nil());
        let generation = self.reserve_generation(bucket, config.is_some());
        self.apply_with_generation(bucket, incarnation_id, config, generation).await
    }

    async fn apply_with_generation(
        &self,
        bucket: &str,
        incarnation_id: uuid::Uuid,
        config: Option<&OnDemandMigrationConfig>,
        generation: u64,
    ) -> ApplyOutcome {
        let Some(config) = self.desired(config) else {
            return self.remove_with_generation(bucket, generation);
        };
        if self.is_unchanged(bucket, incarnation_id, config, generation) {
            return ApplyOutcome::Unchanged;
        }
        let stats = self.state(bucket).map(|state| Arc::clone(&state.stats)).unwrap_or_default();
        let state = BucketOdmState::build(bucket, config, incarnation_id, stats, self.write_back()).await;

        let (outcome, previous) = {
            let mut buckets = self.buckets.write();
            let slot = buckets.entry(bucket.to_string()).or_default();
            if slot.generation > generation {
                (ApplyOutcome::Superseded, None)
            } else {
                slot.generation = generation;
                let previous = slot.state.replace(Arc::clone(&state));
                let outcome = if previous.is_some() {
                    ApplyOutcome::Rebuilt
                } else {
                    ApplyOutcome::Installed
                };
                (outcome, previous)
            }
        };
        match outcome {
            ApplyOutcome::Superseded => {
                state.cancel.cancel();
            }
            _ => {
                if let Some(previous) = previous {
                    previous.cancel.cancel();
                }
                info!(
                    event = EVENT_ODM_BUCKET_STATE_APPLIED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_ON_DEMAND_MIGRATION,
                    result = if outcome == ApplyOutcome::Rebuilt { "rebuilt" } else { "installed" },
                    bucket = %bucket,
                    provider = %config.source.provider,
                    endpoint_host = %state.endpoint_host,
                    client_ready = state.client.is_ok(),
                    "On-demand migration bucket state applied"
                );
            }
        }
        outcome
    }

    /// Removes a bucket's state (idempotent), cancelling its token.
    pub fn remove(&self, bucket: &str) -> ApplyOutcome {
        let generation = self.reserve_generation(bucket, false);
        self.remove_with_generation(bucket, generation)
    }

    /// One-shot lookup: module switch, bucket state, prefix filter,
    /// client availability, negative cache, breaker, in that order.
    #[cfg(test)]
    pub fn resolve(&self, bucket: &str, key: &str) -> Option<OdmLookup> {
        if !self.is_module_enabled() {
            return None;
        }
        self.state(bucket)?.resolve_key(key)
    }

    pub fn resolve_for_incarnation(&self, bucket: &str, key: &str, incarnation_id: uuid::Uuid) -> Option<OdmLookup> {
        if !self.is_module_enabled() {
            return None;
        }
        self.state(bucket)?.filter_incarnation(incarnation_id)?.resolve_key(key)
    }

    pub fn state(&self, bucket: &str) -> Option<Arc<BucketOdmState>> {
        self.buckets.read().get(bucket).and_then(|slot| slot.state.clone())
    }

    pub fn bucket_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .buckets
            .read()
            .iter()
            .filter(|(_, slot)| slot.state.is_some())
            .map(|(name, _)| name.clone())
            .collect();
        names.sort();
        names
    }

    pub fn bucket_snapshot(&self, bucket: &str) -> Option<OdmBucketSnapshot> {
        self.state(bucket).map(|state| state.snapshot())
    }

    /// Snapshot of every configured bucket, sorted by name.
    pub fn snapshot(&self) -> Vec<OdmBucketSnapshot> {
        let states: Vec<Arc<BucketOdmState>> = self.buckets.read().values().filter_map(|slot| slot.state.clone()).collect();
        let mut snapshots: Vec<OdmBucketSnapshot> = states.iter().map(|state| state.snapshot()).collect();
        snapshots.sort_by(|a, b| a.bucket.cmp(&b.bucket));
        snapshots
    }

    fn reserve_generation(&self, bucket: &str, installing: bool) -> u64 {
        // Reserve a desired install before its async client build, under the
        // same lock that orders removals. Unconfigured buckets need no slot.
        let mut buckets = self.buckets.write();
        let generation = self.generation.fetch_add(1, Ordering::Relaxed) + 1;
        if installing {
            buckets.entry(bucket.to_string()).or_default().generation = generation;
        } else if let Some(slot) = buckets.get_mut(bucket) {
            slot.generation = generation;
        }
        generation
    }

    fn desired<'c>(&self, config: Option<&'c OnDemandMigrationConfig>) -> Option<&'c OnDemandMigrationConfig> {
        config.filter(|config| config.enabled && self.is_module_enabled())
    }

    /// Claims `generation` for the bucket when the installed state already
    /// matches `config` and has a usable client.
    fn is_unchanged(&self, bucket: &str, incarnation_id: uuid::Uuid, config: &OnDemandMigrationConfig, generation: u64) -> bool {
        let mut buckets = self.buckets.write();
        let Some(slot) = buckets.get_mut(bucket) else {
            return false;
        };
        if slot.generation > generation {
            return false;
        }
        if slot
            .state
            .as_ref()
            .is_some_and(|state| state.incarnation_id != incarnation_id)
            && let Some(previous) = slot.state.take()
        {
            previous.cancel.cancel();
        }
        let unchanged = slot
            .state
            .as_ref()
            .is_some_and(|state| state.client.is_ok() && state.incarnation_id == incarnation_id && state.config == *config);
        if unchanged && slot.generation < generation {
            slot.generation = generation;
        }
        unchanged
    }

    fn remove_with_generation(&self, bucket: &str, generation: u64) -> ApplyOutcome {
        let removed = {
            let mut buckets = self.buckets.write();
            let Some(slot) = buckets.get_mut(bucket) else {
                return ApplyOutcome::NotDesired;
            };
            if slot.generation > generation {
                return ApplyOutcome::Superseded;
            }
            slot.generation = generation;
            slot.state.take()
        };
        let Some(state) = removed else {
            return ApplyOutcome::NotDesired;
        };
        state.cancel.cancel();
        info!(
            event = EVENT_ODM_BUCKET_STATE_APPLIED,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_ON_DEMAND_MIGRATION,
            result = "removed",
            bucket = %bucket,
            provider = %state.config.source.provider,
            endpoint_host = %state.endpoint_host,
            "On-demand migration bucket state removed"
        );
        ApplyOutcome::Removed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::on_demand_migration::breaker::BREAKER_FAILURE_THRESHOLD;
    use crate::on_demand_migration::config::{FilterConfig, PolicyConfig, SourceCredentials, SourceTimeout, TlsConfig};
    use std::sync::atomic::AtomicUsize;
    use tokio::sync::Barrier;

    fn config(prefix: Option<&str>) -> OnDemandMigrationConfig {
        OnDemandMigrationConfig {
            version: 1,
            enabled: true,
            source: SourceConfig {
                provider: Provider::Minio,
                endpoint: Some("https://Source.Example.com:9000".to_string()),
                region: "auto".to_string(),
                bucket: "legacy".to_string(),
                path_style: ConfigPathStyle::Auto,
                credentials: Some(SourceCredentials {
                    access_key: "AK".to_string(),
                    secret_key: "SK".to_string(),
                    session_token: None,
                }),
                tls: TlsConfig::default(),
                azure: None,
                gcs: None,
            },
            filter: FilterConfig {
                prefix: prefix.map(str::to_string),
                source_prefix: None,
            },
            policy: PolicyConfig::default(),
        }
    }

    fn enabled_sys() -> OnDemandMigrationSys {
        let sys = OnDemandMigrationSys::new();
        sys.set_module_enabled(true);
        sys
    }

    fn ready_state(lookup: Option<OdmLookup>) -> Arc<BucketOdmState> {
        match lookup {
            Some(OdmLookup::Ready { state }) => state,
            other => panic!("expected Ready, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn apply_is_idempotent_rebuilds_on_change_and_removes() {
        let sys = enabled_sys();
        let cfg = config(None);
        assert_eq!(sys.apply("b", Some(&cfg)).await, ApplyOutcome::Installed);
        let first = ready_state(sys.resolve("b", "any"));
        assert!(first.client().is_ok());

        assert_eq!(sys.apply("b", Some(&cfg)).await, ApplyOutcome::Unchanged);
        let again = ready_state(sys.resolve("b", "any"));
        assert!(Arc::ptr_eq(&first, &again), "identical config must not rebuild");
        assert!(!first.is_cancelled());

        let mut changed = cfg.clone();
        changed.filter.prefix = Some("docs/".to_string());
        assert_eq!(sys.apply("b", Some(&changed)).await, ApplyOutcome::Rebuilt);
        let rebuilt = ready_state(sys.resolve("b", "docs/x"));
        assert!(!Arc::ptr_eq(&first, &rebuilt));
        assert!(first.is_cancelled(), "old state token fires on rebuild");
        assert!(!rebuilt.is_cancelled());
        assert!(Arc::ptr_eq(first.stats(), rebuilt.stats()), "counters survive a rebuild");

        assert_eq!(sys.apply("b", None).await, ApplyOutcome::Removed);
        assert!(rebuilt.is_cancelled());
        assert!(sys.resolve("b", "docs/x").is_none());
        assert!(sys.state("b").is_none());
        assert_eq!(sys.apply("b", None).await, ApplyOutcome::NotDesired);
    }

    #[tokio::test]
    async fn disabled_config_or_module_switch_removes_state() {
        let sys = enabled_sys();
        let cfg = config(None);
        sys.apply("b", Some(&cfg)).await;
        let state = sys.state("b").expect("installed");

        let mut disabled = cfg.clone();
        disabled.enabled = false;
        assert_eq!(sys.apply("b", Some(&disabled)).await, ApplyOutcome::Removed);
        assert!(state.is_cancelled());

        sys.apply("b", Some(&cfg)).await;
        let state = sys.state("b").expect("installed again");
        sys.set_module_enabled(false);
        assert!(sys.resolve("b", "k").is_none(), "switch off: resolve never intervenes");
        assert_eq!(sys.apply("b", Some(&cfg)).await, ApplyOutcome::Removed);
        assert!(state.is_cancelled());

        let off = OnDemandMigrationSys::new();
        assert!(!off.is_module_enabled(), "default switch is off");
        assert_eq!(off.apply("c", Some(&cfg)).await, ApplyOutcome::NotDesired);
    }

    #[tokio::test]
    async fn resolve_order_prefix_negative_cache_breaker() {
        let sys = enabled_sys();
        sys.apply("b", Some(&config(Some("a/")))).await;

        assert!(sys.resolve("b", "b/x").is_none(), "prefix mismatch");
        assert!(sys.resolve("other", "a/x").is_none(), "unknown bucket");
        let state = ready_state(sys.resolve("b", "a/x"));

        state.observe_source(Duration::from_millis(1), "a/gone", Some(&SourceError::NotFound));
        assert!(matches!(sys.resolve("b", "a/gone"), Some(OdmLookup::NegativeCached { .. })));
        assert!(matches!(sys.resolve("b", "a/x"), Some(OdmLookup::Ready { .. })));

        for _ in 0..BREAKER_FAILURE_THRESHOLD {
            state.observe_source(Duration::from_millis(5), "a/x", Some(&SourceError::ServerError(503)));
        }
        assert_eq!(state.breaker().state(), BreakerState::Open);
        assert!(matches!(sys.resolve("b", "a/x"), Some(OdmLookup::BreakerOpen { .. })));
        // Negative cache still wins over the breaker for its own keys.
        assert!(matches!(sys.resolve("b", "a/gone"), Some(OdmLookup::NegativeCached { .. })));
        assert_eq!(state.stats().last_source_error().map(|e| e.class), Some("server_error".to_string()));
    }

    #[tokio::test]
    async fn anonymous_source_is_a_typed_state_error() {
        let sys = enabled_sys();
        let mut cfg = config(None);
        cfg.source.credentials = None;
        assert_eq!(sys.apply("b", Some(&cfg)).await, ApplyOutcome::Installed);
        let state = sys.state("b").unwrap();
        assert_eq!(state.client().err(), Some(&OdmStateError::AnonymousUnsupported));
        match sys.resolve("b", "k") {
            Some(OdmLookup::Unavailable { error, .. }) => assert_eq!(error, OdmStateError::AnonymousUnsupported),
            other => panic!("expected Unavailable, got {other:?}"),
        }
        let snapshot = sys.bucket_snapshot("b").unwrap();
        assert!(snapshot.client_error.as_deref().unwrap().contains("anonymous"));
        // A failed client is rebuilt on the next apply of the same config.
        assert_eq!(sys.apply("b", Some(&cfg)).await, ApplyOutcome::Rebuilt);
    }

    #[tokio::test]
    async fn native_azure_uses_provider_credentials_without_s3_credentials() {
        let sys = enabled_sys();
        let mut cfg = config(None);
        cfg.source.provider = Provider::Azure;
        cfg.source.endpoint = None;
        cfg.source.credentials = None;
        cfg.source.azure = Some(super::super::config::AzureSourceConfig {
            account: "legacyaccount".to_string(),
            account_key: Some("c2VjcmV0LWtleQ==".to_string()),
            sas_token: None,
        });
        assert_eq!(sys.apply("b", Some(&cfg)).await, ApplyOutcome::Installed);
        let state = ready_state(sys.resolve("b", "k"));
        assert!(state.client().is_ok(), "native credentials must not be classified as anonymous S3");
    }

    #[cfg(not(feature = "gcs"))]
    #[tokio::test]
    async fn gcs_backend_not_compiled_is_unavailable_not_anonymous() {
        let sys = enabled_sys();
        let mut cfg = config(None);
        cfg.source.provider = Provider::GcsNative;
        cfg.source.credentials = None;
        cfg.source.gcs = Some(super::super::config::GcsSourceConfig {
            service_account_json: "{}".to_string(),
        });
        let encoded = cfg.to_json().expect("GCS config is serializable without the backend");
        let restored: OnDemandMigrationConfig = serde_json::from_slice(&encoded).expect("GCS config stays readable");
        assert_eq!(restored, cfg);
        assert_eq!(sys.apply("b", Some(&cfg)).await, ApplyOutcome::Installed);
        match sys.resolve("b", "k") {
            Some(OdmLookup::Unavailable { error, .. }) => {
                assert_eq!(error, OdmStateError::BackendNotCompiled("gcs_native"));
            }
            other => panic!("expected unavailable backend, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn singleflight_admits_one_leader_per_key() {
        let sys = enabled_sys();
        sys.apply("b", Some(&config(None))).await;
        let state = sys.state("b").unwrap();
        let barrier = Arc::new(Barrier::new(100));
        let leaders = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..100 {
            let state = Arc::clone(&state);
            let barrier = Arc::clone(&barrier);
            let leaders = Arc::clone(&leaders);
            tasks.push(tokio::spawn(async move {
                let slot = state.acquire_pull_slot("same-key").await.unwrap();
                barrier.wait().await;
                match slot {
                    PullSlot::Leader(leader) => {
                        leaders.fetch_add(1, Ordering::SeqCst);
                        assert_eq!(state.stats().inflight_pulls(), 1);
                        leader.complete(Ok(PullOutcome {
                            etag: Some("abc".into()),
                            size: 7,
                        }));
                        Ok(PullOutcome {
                            etag: Some("abc".into()),
                            size: 7,
                        })
                    }
                    PullSlot::Follower(follower) => follower.wait().await,
                }
            }));
        }
        for task in tasks {
            let result = task.await.unwrap();
            assert_eq!(result.unwrap().size, 7);
        }
        assert_eq!(leaders.load(Ordering::SeqCst), 1);
        assert_eq!(state.inflight_keys(), 0);
        assert_eq!(state.stats().inflight_pulls(), 0);
        assert_eq!(state.stats().queue_depth(), 0);
    }

    #[tokio::test]
    async fn followers_fail_when_leader_drops_without_result() {
        let sys = enabled_sys();
        sys.apply("b", Some(&config(None))).await;
        let state = sys.state("b").unwrap();
        let leader = match state.acquire_pull_slot("k").await.unwrap() {
            PullSlot::Leader(leader) => leader,
            PullSlot::Follower(_) => panic!("first caller must lead"),
        };
        let follower = match state.acquire_pull_slot("k").await.unwrap() {
            PullSlot::Follower(follower) => follower,
            PullSlot::Leader(_) => panic!("second caller must follow"),
        };
        drop(leader);
        let err = follower.wait().await.unwrap_err();
        assert_eq!(err.reason, PullFailureReason::Canceled);
        // The key is free again.
        assert!(matches!(state.acquire_pull_slot("k").await.unwrap(), PullSlot::Leader(_)));
    }

    #[tokio::test]
    async fn pull_semaphore_waits_and_reports_queue_depth() {
        let sys = enabled_sys();
        let mut cfg = config(None);
        cfg.policy.max_concurrent_pulls = 2;
        sys.apply("b", Some(&cfg)).await;
        let state = sys.state("b").unwrap();

        let first = state.acquire_pull_slot("k1").await.unwrap();
        let second = state.acquire_pull_slot("k2").await.unwrap();
        assert_eq!(state.stats().inflight_pulls(), 2);

        let waiter = {
            let state = Arc::clone(&state);
            tokio::spawn(async move { state.acquire_pull_slot("k3").await })
        };
        tokio::time::timeout(Duration::from_millis(200), async {
            while state.stats().queue_depth() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("third caller must register as queued");
        assert_eq!(state.stats().queue_depth(), 1);
        assert!(!waiter.is_finished(), "third caller waits instead of being rejected");

        drop(first);
        let third = tokio::time::timeout(Duration::from_secs(2), waiter)
            .await
            .expect("permit handed over")
            .unwrap()
            .unwrap();
        assert!(matches!(third, PullSlot::Leader(_)));
        assert_eq!(state.stats().queue_depth(), 0);
        assert_eq!(state.stats().inflight_pulls(), 2);
        drop(second);
        drop(third);
        assert_eq!(state.stats().inflight_pulls(), 0);
    }

    #[tokio::test]
    async fn removal_cancels_queued_pull_and_rejects_new_ones() {
        let sys = enabled_sys();
        let mut cfg = config(None);
        cfg.policy.max_concurrent_pulls = 1;
        sys.apply("b", Some(&cfg)).await;
        let state = sys.state("b").unwrap();
        let _held = state.acquire_pull_slot("k1").await.unwrap();
        let waiter = {
            let state = Arc::clone(&state);
            tokio::spawn(async move { state.acquire_pull_slot("k2").await })
        };
        tokio::time::timeout(Duration::from_millis(200), async {
            while state.stats().queue_depth() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(sys.remove("b"), ApplyOutcome::Removed);
        let err = waiter.await.unwrap().unwrap_err();
        assert_eq!(err.reason, PullFailureReason::Canceled);
        assert_eq!(state.stats().queue_depth(), 0);
        assert_eq!(state.inflight_keys(), 1, "held leader still registered");
        assert_eq!(state.acquire_pull_slot("k3").await.unwrap_err().reason, PullFailureReason::Canceled);
    }

    #[test]
    fn source_client_spec_maps_every_field() {
        let mut cfg = config(Some("local/"));
        cfg.source.path_style = ConfigPathStyle::Virtual;
        cfg.source.credentials.as_mut().unwrap().session_token = Some("tok".to_string());
        cfg.source.tls = TlsConfig {
            skip_verify: true,
            ca_cert_pem: Some("-----BEGIN CERTIFICATE-----".to_string()),
        };
        cfg.filter.source_prefix = Some("old/".to_string());
        cfg.policy.source_timeout = SourceTimeout {
            connect_ms: 1500,
            first_byte_ms: 2500,
            idle_ms: 3500,
        };
        cfg.policy.bandwidth_limit_bytes_per_sec = Some(1 << 20);

        let spec = source_client_spec(&cfg);
        assert_eq!(spec.endpoint, "https://Source.Example.com:9000");
        assert_eq!(spec.region, "us-east-1", "auto maps to the signing fallback");
        assert_eq!(spec.bucket, "legacy");
        assert_eq!(spec.source_prefix.as_deref(), Some("old/"));
        assert_eq!(spec.provider, SourceProvider::Minio);
        assert_eq!(spec.path_style, ClientPathStyle::VirtualHost);
        let credentials = spec.credentials.as_ref().unwrap();
        assert_eq!(credentials.access_key, "AK");
        assert_eq!(credentials.secret_key, "SK");
        assert_eq!(credentials.session_token.as_deref(), Some("tok"));
        assert_eq!(credentials.expiration, None);
        assert!(spec.skip_tls_verify);
        assert!(spec.ca_cert_pem.is_some());
        assert_eq!(spec.timeouts.connect, Duration::from_millis(1500));
        assert_eq!(spec.timeouts.read, Duration::from_millis(2500));
        assert_eq!(spec.bandwidth_limit, NonZeroU64::new(1 << 20));
        assert_eq!(
            spec.retry,
            RemoteS3RetryPolicy::Disabled,
            "the pull pipeline owns the retry budget, so one source call is one wire request"
        );

        let mut aws = config(None);
        aws.source.provider = Provider::Aws;
        aws.source.endpoint = None;
        aws.source.region = "eu-west-1".to_string();
        aws.source.credentials = None;
        let spec = source_client_spec(&aws);
        assert_eq!(spec.endpoint, "https://s3.eu-west-1.amazonaws.com");
        assert_eq!(spec.provider, SourceProvider::Aws);
        assert_eq!(spec.path_style, ClientPathStyle::Auto);
        assert!(spec.credentials.is_none());
        assert_eq!(endpoint_host(&aws.source), "s3.eu-west-1.amazonaws.com");
        assert_eq!(endpoint_host(&cfg.source), "source.example.com");

        for (provider, expected) in [
            (Provider::S3, SourceProvider::S3),
            (Provider::Rustfs, SourceProvider::Rustfs),
            (Provider::R2, SourceProvider::R2),
            (Provider::Gcs, SourceProvider::Gcs),
        ] {
            assert_eq!(source_provider(provider), expected);
        }
    }

    #[tokio::test]
    async fn publish_spawns_install_and_removes_synchronously() {
        let sys: &'static OnDemandMigrationSys = Box::leak(Box::new(enabled_sys()));
        let cfg = config(None);
        sys.publish("p", Some(&cfg));
        tokio::time::timeout(Duration::from_secs(5), async {
            while sys.state("p").is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("publish must install the state in the background");
        let state = sys.state("p").unwrap();

        sys.publish("p", Some(&cfg));
        tokio::task::yield_now().await;
        assert!(Arc::ptr_eq(&state, &sys.state("p").unwrap()), "unchanged publish is a no-op");

        sys.publish("p", None);
        assert!(sys.state("p").is_none(), "removal is synchronous");
        assert!(state.is_cancelled());
    }

    #[tokio::test]
    async fn identical_config_on_recreated_bucket_cancels_old_state() {
        let sys = enabled_sys();
        let cfg = config(None);
        let old_id = uuid::Uuid::new_v4();
        let new_id = uuid::Uuid::new_v4();
        sys.apply_for_incarnation("recreated", old_id, Some(&cfg)).await;
        let old = sys.state("recreated").expect("old state installed");
        assert!(sys.resolve_for_incarnation("recreated", "key", new_id).is_none());
        sys.apply_for_incarnation("recreated", new_id, Some(&cfg)).await;
        let replacement = sys.state("recreated").expect("replacement state installed");
        assert!(old.is_cancelled());
        assert!(!Arc::ptr_eq(&old, &replacement));
        assert_eq!(replacement.incarnation_id(), new_id);
        assert!(sys.resolve_for_incarnation("recreated", "key", old_id).is_none());
        assert!(sys.resolve_for_incarnation("recreated", "key", new_id).is_some());
    }

    #[tokio::test]
    async fn changed_delete_marker_policy_withdraws_the_captured_lookup() {
        let sys = enabled_sys();
        let incarnation = uuid::Uuid::new_v4();
        let mut cfg = config(None);
        cfg.policy.respect_local_delete_marker = false;
        sys.apply_for_incarnation("policy-snapshot", incarnation, Some(&cfg)).await;
        let captured = sys.state("policy-snapshot").expect("policy A installed");
        assert!(!captured.config().policy.respect_local_delete_marker);

        cfg.policy.respect_local_delete_marker = true;
        sys.apply_for_incarnation("policy-snapshot", incarnation, Some(&cfg)).await;
        let replacement = sys.state("policy-snapshot").expect("policy B installed");
        assert!(replacement.config().policy.respect_local_delete_marker);
        assert!(captured.is_cancelled());
        assert!(
            captured
                .filter_incarnation(incarnation)
                .and_then(|state| state.resolve_key("key"))
                .is_none(),
            "a request that evaluated policy A cannot continue through policy B"
        );
        assert!(
            replacement
                .clone()
                .filter_incarnation(incarnation)
                .and_then(|state| state.resolve_key("key"))
                .is_some()
        );
        assert_eq!(
            replacement
                .stats()
                .snapshot(replacement.breaker().state())
                .source_latency
                .count,
            0
        );
    }

    #[tokio::test]
    async fn missing_incarnation_cannot_install_or_retain_source_state() {
        let sys: &'static OnDemandMigrationSys = Box::leak(Box::new(enabled_sys()));
        let cfg = config(None);
        assert_eq!(
            sys.apply_for_incarnation("missing", uuid::Uuid::nil(), Some(&cfg)).await,
            ApplyOutcome::NotDesired
        );
        sys.publish_for_incarnation("missing", uuid::Uuid::nil(), Some(&cfg));
        assert!(sys.state("missing").is_none());

        sys.apply_for_incarnation("missing", uuid::Uuid::new_v4(), Some(&cfg)).await;
        let state = sys.state("missing").expect("valid identity installed");
        sys.publish_for_incarnation("missing", uuid::Uuid::nil(), Some(&cfg));
        assert!(sys.state("missing").is_none());
        assert!(state.is_cancelled());
    }

    #[tokio::test]
    async fn corrupt_stored_config_withdraws_runtime_state() {
        let sys: &'static OnDemandMigrationSys = Box::leak(Box::new(enabled_sys()));
        let cfg = config(None);
        assert_eq!(sys.apply("corrupt", Some(&cfg)).await, ApplyOutcome::Installed);
        let state = sys.state("corrupt").expect("state installed");
        sys.publish_stored("corrupt", Some((b"not-json", uuid::Uuid::from_u128(1))));
        assert!(sys.state("corrupt").is_none(), "corruption cannot keep an older source active");
        assert!(state.is_cancelled(), "corruption cancels in-flight work");
    }

    #[tokio::test]
    async fn absent_config_updates_do_not_allocate_bucket_slots() {
        let sys = enabled_sys();
        for index in 0..1000 {
            let bucket = format!("unconfigured-{index}");
            assert_eq!(sys.apply(&bucket, None).await, ApplyOutcome::NotDesired);
            assert_eq!(sys.remove(&bucket), ApplyOutcome::NotDesired);
        }
        assert!(sys.buckets.read().is_empty(), "unconfigured buckets must not accumulate tombstones");
    }

    #[tokio::test]
    async fn stale_install_cannot_overwrite_a_later_removal() {
        let sys = enabled_sys();
        let cfg = config(None);
        let older = sys.reserve_generation("b", true);
        let newer = sys.reserve_generation("b", false);
        assert_eq!(sys.remove_with_generation("b", newer), ApplyOutcome::NotDesired);
        assert_eq!(
            sys.apply_with_generation("b", uuid::Uuid::from_u128(1), Some(&cfg), older)
                .await,
            ApplyOutcome::Superseded
        );
        assert!(sys.state("b").is_none(), "removal must supersede an in-flight first install");

        assert_eq!(sys.apply("b", Some(&cfg)).await, ApplyOutcome::Installed);
        let installed = sys.state("b").unwrap();
        let older = sys.reserve_generation("b", true);
        let newer = sys.reserve_generation("b", false);
        assert_eq!(sys.remove_with_generation("b", newer), ApplyOutcome::Removed);
        assert!(installed.is_cancelled());
        assert_eq!(
            sys.apply_with_generation("b", uuid::Uuid::from_u128(1), Some(&cfg), older)
                .await,
            ApplyOutcome::Superseded
        );
        assert!(sys.state("b").is_none(), "the stale install is discarded");
    }

    #[tokio::test]
    async fn snapshot_lists_buckets_sorted_and_serializes() {
        let sys = enabled_sys();
        sys.apply("zeta", Some(&config(None))).await;
        sys.apply("alpha", Some(&config(Some("a/")))).await;
        assert_eq!(sys.bucket_names(), vec!["alpha".to_string(), "zeta".to_string()]);
        let snapshots = sys.snapshot();
        assert_eq!(snapshots.len(), 2);
        assert_eq!(snapshots[0].bucket, "alpha");
        assert_eq!(snapshots[0].provider, "minio");
        assert_eq!(snapshots[0].endpoint_host, "source.example.com");
        assert_eq!(snapshots[0].client_error, None);
        assert_eq!(snapshots[0].stats.breaker_state, BreakerState::Closed);
        let json = serde_json::to_string(&snapshots[0]).unwrap();
        assert!(!json.contains("SK"), "snapshot must not carry credentials");
        let round_trip: OdmBucketSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(round_trip, snapshots[0]);
        let debug = format!("{:?}", sys.state("alpha").unwrap());
        assert!(!debug.contains("SK"), "Debug must not carry credentials: {debug}");
    }
}
