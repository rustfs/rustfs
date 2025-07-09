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

use reqwest::Client as HttpClient;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use time::OffsetDateTime;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tracing::error;
use url::Url;
use uuid::Uuid;

use crate::bucket::metadata::BucketMetadata;

use crate::bucket::metadata_sys::get_bucket_targets_config;
use crate::bucket::target::ARN;
use crate::bucket::target::{self, BucketTarget, BucketTargets, Credentials};
use crate::client;
use crate::client::credentials::SignatureType;
use crate::client::credentials::Static;
use crate::client::credentials::Value;
use crate::client::transition_api::Options;
use crate::client::transition_api::TransitionClient;

const DEFAULT_HEALTH_CHECK_DURATION: Duration = Duration::from_secs(5);
const DEFAULT_HEALTH_CHECK_RELOAD_DURATION: Duration = Duration::from_secs(30 * 60);

pub static GLOBAL_BUCKET_TARGET_SYS: OnceLock<BucketTargetSys> = OnceLock::new();

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LastMinuteLatency {
    times: Vec<Duration>,
    #[serde(skip, default = "instant_now")]
    start_time: Instant,
}

fn instant_now() -> Instant {
    Instant::now()
}

impl Default for LastMinuteLatency {
    fn default() -> Self {
        Self {
            times: Vec::new(),
            start_time: Instant::now(),
        }
    }
}

impl LastMinuteLatency {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, duration: Duration) {
        let now = Instant::now();
        // Remove entries older than 1 minute
        self.times
            .retain(|_| now.duration_since(self.start_time) < Duration::from_secs(60));
        self.times.push(duration);
    }

    pub fn get_total(&self) -> LatencyAverage {
        if self.times.is_empty() {
            return LatencyAverage {
                avg: Duration::from_secs(0),
            };
        }
        let total: Duration = self.times.iter().sum();
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
            latency: LatencyStat::new(),
        }
    }
}

#[derive(Debug, Default)]
pub struct BucketTargetSys {
    pub arn_remotes_map: Arc<RwLock<HashMap<String, ArnTarget>>>,
    pub targets_map: Arc<RwLock<HashMap<String, Vec<BucketTarget>>>>,
    pub h_mutex: Arc<RwLock<HashMap<String, EpHealth>>>,
    pub hc_client: Arc<HttpClient>,
    pub a_mutex: Arc<Mutex<HashMap<String, ArnErrs>>>,
    pub arn_errs_map: Arc<RwLock<HashMap<String, ArnErrs>>>,
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
            hc_client: Arc::new(HttpClient::new()),
            a_mutex: Arc::new(Mutex::new(HashMap::new())),
            arn_errs_map: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn is_offline(&self, url: &Url) -> bool {
        let health_map = self.h_mutex.read().await;
        if let Some(health) = health_map.get(url.host_str().unwrap_or("")) {
            return !health.online;
        }
        // Initialize health check if not exists
        self.init_hc(url).await;
        false
    }

    pub async fn mark_offline(&self, url: &Url) {
        let mut health_map = self.h_mutex.write().await;
        if let Some(health) = health_map.get_mut(url.host_str().unwrap_or("")) {
            health.online = false;
        }
    }

    pub async fn init_hc(&self, url: &Url) {
        let mut health_map = self.h_mutex.write().await;
        let host = url.host_str().unwrap_or("").to_string();
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

    pub async fn heartbeat(&self) {
        let mut interval = tokio::time::interval(DEFAULT_HEALTH_CHECK_DURATION);
        loop {
            interval.tick().await;

            let endpoints = {
                let health_map = self.h_mutex.read().await;
                health_map.keys().cloned().collect::<Vec<_>>()
            };

            for endpoint in endpoints {
                // Perform health check
                let start = Instant::now();
                let online = self.check_endpoint_health(&endpoint).await;
                let duration = start.elapsed();

                {
                    let mut health_map = self.h_mutex.write().await;
                    if let Some(health) = health_map.get_mut(&endpoint) {
                        let prev_online = health.online;
                        health.online = online;
                        health.last_hc_at = Some(OffsetDateTime::now_utc());
                        health.latency.update(duration);

                        if online {
                            health.last_online = Some(OffsetDateTime::now_utc());
                        } else if prev_online {
                            // Just went offline
                            health.offline_duration += duration;
                        }
                    }
                }
            }
        }
    }

    async fn check_endpoint_health(&self, _endpoint: &str) -> bool {
        todo!()
        // // Simple health check implementation
        // // In a real implementation, you would make actual HTTP requests
        // match self
        //     .hc_client
        //     .get(format!("https://{}/rustfs/health/ready", endpoint))
        //     .timeout(Duration::from_secs(3))
        //     .send()
        //     .await
        // {
        //     Ok(response) => response.status().is_success(),
        //     Err(_) => false,
        // }
    }

    pub async fn health_stats(&self) -> HashMap<String, EpHealth> {
        let health_map = self.h_mutex.read().await;
        health_map.clone()
    }

    pub async fn list_targets(&self, bucket: &str, arn_type: &str) -> Vec<BucketTarget> {
        let health_stats = self.health_stats().await;
        let mut targets = Vec::new();

        if !bucket.is_empty() {
            if let Ok(bucket_targets) = self.list_bucket_targets(bucket).await {
                for mut target in bucket_targets.targets {
                    if arn_type.is_empty() || target.target_type.to_string() == arn_type {
                        if let Some(health) = health_stats.get(&target.endpoint) {
                            target.total_downtime = health.offline_duration;
                            target.online = health.online;
                            target.last_online = health.last_online;
                            target.latency = target::LatencyStat {
                                curr: health.latency.curr,
                                avg: health.latency.avg,
                                max: health.latency.peak,
                            };
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
                    if let Some(health) = health_stats.get(&target.endpoint) {
                        target.total_downtime = health.offline_duration;
                        target.online = health.online;
                        target.last_online = health.last_online;
                        target.latency = target::LatencyStat {
                            curr: health.latency.curr,
                            avg: health.latency.avg,
                            max: health.latency.peak,
                        };
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
        let mut targets_map = self.targets_map.write().await;
        let mut arn_remotes_map = self.arn_remotes_map.write().await;

        if let Some(targets) = targets_map.remove(bucket) {
            for target in targets {
                arn_remotes_map.remove(&target.arn);
            }
        }
    }

    pub async fn set_target(&self, bucket: &str, target: &BucketTarget, update: bool) -> Result<(), BucketTargetError> {
        if !target.target_type.is_valid() && !update {
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

        {
            let mut targets_map = self.targets_map.write().await;
            let bucket_targets = targets_map.entry(bucket.to_string()).or_insert_with(Vec::new);
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
        }

        {
            let mut arn_remotes_map = self.arn_remotes_map.write().await;
            arn_remotes_map.insert(
                target.arn.clone(),
                ArnTarget {
                    client: Some(Arc::new(target_client)),
                    last_refresh: OffsetDateTime::now_utc(),
                },
            );
        }

        self.update_bandwidth_limit(bucket, &target.arn, target.bandwidth_limit);
        Ok(())
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
            return Some(cli.clone());
        }

        // TODO: spawn a task to reload the target
        if self.is_reloading_target(bucket, arn).await {
            return None;
        }

        if let Some(last_refresh) = last_refresh {
            let now = OffsetDateTime::now_utc();
            if now - last_refresh > Duration::from_secs(60 * 5) {
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

        self.inc_arn_errs(bucket, arn).await;
        None
    }

    pub async fn get_remote_target_client_internal(&self, target: &BucketTarget) -> Result<TargetClient, BucketTargetError> {
        let Some(credentials) = &target.credentials else {
            return Err(BucketTargetError::BucketRemoteTargetNotFound {
                bucket: target.target_bucket.clone(),
            });
        };

        let creds = client::credentials::Credentials::new(Static(Value {
            access_key_id: credentials.access_key.clone(),
            secret_access_key: credentials.secret_key.clone(),
            session_token: "".to_string(),
            signer_type: SignatureType::SignatureV4,
            ..Default::default()
        }));

        Ok(TargetClient {
            endpoint: target.endpoint.clone(),
            credentials: target.credentials.clone(),
            bucket: target.target_bucket.clone(),
            storage_class: target.storage_class.clone(),
            disable_proxy: target.disable_proxy,
            arn: target.arn.clone(),
            reset_id: target.reset_id.clone(),
            secure: target.secure,
            health_check_duration: target.health_check_duration,
            replicate_sync: target.replication_sync,
            client: Arc::new(
                TransitionClient::new(
                    &target.endpoint,
                    Options {
                        creds,
                        secure: target.secure,
                        region: target.region.clone(),
                        ..Default::default()
                    },
                )
                .await?,
            ),
        })
    }

    async fn validate_target_credentials(&self, _target: &BucketTarget) -> Result<bool, BucketTargetError> {
        // In a real implementation, you would validate the credentials
        // by making actual API calls to the target
        Ok(true)
    }

    fn update_bandwidth_limit(&self, _bucket: &str, _arn: &str, _limit: i64) {
        // Implementation for bandwidth limit update
        // This would interact with the global bucket monitor
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
        let mut targets_map = self.targets_map.write().await;
        let mut arn_remotes_map = self.arn_remotes_map.write().await;
        // Remove existing targets
        if let Some(existing_targets) = targets_map.remove(bucket) {
            for target in existing_targets {
                arn_remotes_map.remove(&target.arn);
            }
        }

        // Add new targets
        if let Some(new_targets) = targets {
            if !new_targets.is_empty() {
                for target in &new_targets.targets {
                    if let Ok(client) = self.get_remote_target_client_internal(target).await {
                        arn_remotes_map.insert(
                            target.arn.clone(),
                            ArnTarget {
                                client: Some(Arc::new(client)),
                                last_refresh: OffsetDateTime::now_utc(),
                            },
                        );
                        self.update_bandwidth_limit(bucket, &target.arn, target.bandwidth_limit);
                    }
                }
                targets_map.insert(bucket.to_string(), new_targets.targets.clone());
            }
        }
    }

    pub async fn set(&self, bucket: &str, meta: &BucketMetadata) {
        let Some(config) = &meta.bucket_target_config else {
            return;
        };

        if config.is_empty() {
            return;
        }

        for target in config.targets.iter() {
            let cli = match self.get_remote_target_client_internal(target).await {
                Ok(cli) => cli,
                Err(e) => {
                    error!("set bucket target:{} error:{}", bucket, e);
                    continue;
                }
            };

            {
                let arn_target = ArnTarget::with_client(Arc::new(cli));
                let mut arn_remotes_map = self.arn_remotes_map.write().await;
                arn_remotes_map.insert(target.arn.clone(), arn_target);
            }
            self.update_bandwidth_limit(bucket, &target.arn, target.bandwidth_limit);
        }

        let mut targets_map = self.targets_map.write().await;
        targets_map.insert(bucket.to_string(), config.targets.clone());
    }

    // getRemoteARN gets existing ARN for an endpoint or generates a new one.
    pub async fn get_remote_arn(&self, bucket: &str, target: Option<&BucketTarget>, depl_id: &str) -> (String, bool) {
        let Some(target) = target else {
            return (String::new(), false);
        };

        let targets_map = self.targets_map.read().await;
        let Some(targets) = targets_map.get(bucket) else {
            return (String::new(), false);
        };

        for tgt in targets {
            if tgt.target_type == target.target_type
                && tgt.target_bucket == target.target_bucket
                && target.endpoint == tgt.endpoint
                && tgt
                    .credentials
                    .as_ref()
                    .map(|c| c.access_key == target.credentials.as_ref().unwrap_or(&Credentials::default()).access_key)
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
        arn_type: t.target_type.to_string(),
        id: uuid,
        region: t.region.clone(),
        bucket: t.target_bucket.clone(),
    };
    arn.to_string()
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
    pub client: Arc<TransitionClient>,
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
                access_key,
                error,
            } => {
                write!(f, "Connection error for bucket: {bucket}, access key: {access_key}, error: {error}")
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
