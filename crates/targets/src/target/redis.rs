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

use crate::{
    StoreError, Target, TargetLog,
    arn::TargetID,
    error::TargetError,
    store::{Key, QueueStore, Store},
    target::{
        ChannelTargetType, EntityTarget, QueuedPayload, QueuedPayloadMeta, TargetDeliveryCounters, TargetDeliverySnapshot,
        TargetType,
    },
};
use async_trait::async_trait;
use redis::{
    AsyncCommands, Client, ClientTlsConfig, ConnectionInfo, IntoConnectionInfo, RedisError, TlsCertificates,
    aio::{ConnectionManager, ConnectionManagerConfig},
    cmd,
    io::tcp::{TcpSettings, socket2},
};
use rustfs_config::{REDIS_TLS_CA, REDIS_TLS_CLIENT_CERT, REDIS_TLS_CLIENT_KEY, REDIS_TLS_POLICY};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{debug, error, info, instrument, warn};
use url::Url;

const DEFAULT_REDIS_CHANNEL: &str = "redis_target";
const DEFAULT_PUBLISH_RETRIES: usize = 3;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedisTlsPolicy {
    SystemCa,
    CustomCa,
}

impl RedisTlsPolicy {
    fn parse(value: &str) -> Result<Self, TargetError> {
        match value.trim() {
            value if value.eq_ignore_ascii_case("system_ca") => Ok(Self::SystemCa),
            value if value.eq_ignore_ascii_case("custom_ca") => Ok(Self::CustomCa),
            _ => Err(TargetError::Configuration(
                "Redis tls_policy must be one of: system_ca, custom_ca".to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RedisTlsConfig {
    pub policy: Option<RedisTlsPolicy>,
    pub ca_path: String,
    pub client_cert_path: String,
    pub client_key_path: String,
    pub allow_insecure: bool,
}

impl RedisTlsConfig {
    pub fn from_values(
        policy: Option<&str>,
        ca_path: Option<&str>,
        client_cert_path: Option<&str>,
        client_key_path: Option<&str>,
        allow_insecure: Option<&str>,
    ) -> Result<Self, TargetError> {
        let policy = match policy.map(str::trim).filter(|value| !value.is_empty()) {
            Some(value) => Some(RedisTlsPolicy::parse(value)?),
            None => None,
        };
        let allow_insecure = allow_insecure
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| {
                value
                    .parse::<rustfs_config::EnableState>()
                    .map(rustfs_config::EnableState::is_enabled)
                    .or_else(|_| value.parse::<bool>())
                    .map_err(|_| TargetError::Configuration("Redis tls_allow_insecure must be a boolean value".to_string()))
            })
            .transpose()?
            .unwrap_or(false);

        Ok(Self {
            policy,
            ca_path: ca_path.unwrap_or_default().trim().to_string(),
            client_cert_path: client_cert_path.unwrap_or_default().trim().to_string(),
            client_key_path: client_key_path.unwrap_or_default().trim().to_string(),
            allow_insecure,
        })
    }
}

#[derive(Debug, Clone)]
pub struct RedisArgs {
    /// Whether the target is enabled
    pub enable: bool,
    /// The Redis server URL in format: `{redis|rediss|valkey|valkeys}://[<username>][:<password>@]<hostname>[:port][/<db>]`
    pub url: Url,
    /// The Redis pub/sub channel to publish to
    pub channel: String,
    /// The username for the Redis connection (leave it empty if you parse with url)
    pub username: Option<String>,
    /// The password for the Redis connection (leave it empty if you parse with url)
    pub password: Option<String>,
    /// TLS configuration
    pub tls: RedisTlsConfig,
    /// The keep alive interval
    pub keep_alive: Duration,
    /// The directory to store events in case of failure
    pub queue_dir: String,
    /// The maximum number of events to store
    pub queue_limit: u64,
    /// Maximum number of synchronous publish retries per payload
    pub max_retry_attempts: usize,
    /// Maximum number of reconnect retries in the underlying connection manager (6 if not provided)
    pub reconnect_retry_attempts: Option<usize>,
    /// Minimum retry delay between publish retry attempts (100ms if not provided)
    pub min_retry_delay: Option<Duration>,
    /// Maximum retry delay between publish retry attempts (2s if not provided)
    pub max_retry_delay: Option<Duration>,
    /// Timeout for establishing a Redis connection (5s if not provided)
    pub connection_timeout: Option<Duration>,
    /// Timeout for command responses (5s if not provided)
    pub response_timeout: Option<Duration>,
    /// Internal command buffer size for the multiplexed connection (50 if not provided)
    pub pipeline_buffer_size: Option<usize>,
    /// the target type
    pub target_type: TargetType,
}

impl RedisArgs {
    pub fn validate(&self) -> Result<(), TargetError> {
        if !self.enable {
            return Ok(());
        }

        validate_redis_url(&self.url)?;
        validate_redis_tls_config(&self.url, &self.tls)?;

        if self.channel.trim().is_empty() {
            return Err(TargetError::Configuration("Redis channel cannot be empty".to_string()));
        }

        if self.username.as_deref().unwrap_or_default().is_empty() != self.password.as_deref().unwrap_or_default().is_empty()
            && !(self.username.is_none() && self.password.is_none())
        {
            return Err(TargetError::Configuration(
                "Redis username and password must be specified together when provided explicitly".to_string(),
            ));
        }

        if self.max_retry_attempts == 0 {
            return Err(TargetError::Configuration(
                "Redis max_retry_attempts must be greater than zero".to_string(),
            ));
        }

        if self.pipeline_buffer_size == Some(0) {
            return Err(TargetError::Configuration(
                "Redis pipeline_buffer_size must be greater than zero".to_string(),
            ));
        }

        if let (Some(min_retry_delay), Some(max_retry_delay)) = (self.min_retry_delay, self.max_retry_delay)
            && max_retry_delay < min_retry_delay
        {
            return Err(TargetError::Configuration(
                "Redis max_retry_delay must be greater than or equal to min_retry_delay".to_string(),
            ));
        }

        if !self.queue_dir.is_empty() && !Path::new(&self.queue_dir).is_absolute() {
            return Err(TargetError::Configuration("redis queue_dir path should be absolute".to_string()));
        }

        Ok(())
    }
}

impl Default for RedisArgs {
    fn default() -> Self {
        Self {
            enable: false,
            url: Url::parse("redis://127.0.0.1:6379").expect("static redis URL should parse"),
            channel: String::from(DEFAULT_REDIS_CHANNEL),
            username: None,
            password: None,
            tls: RedisTlsConfig::default(),
            keep_alive: Duration::from_secs(15),
            queue_dir: String::new(),
            queue_limit: 0,
            max_retry_attempts: DEFAULT_PUBLISH_RETRIES,
            reconnect_retry_attempts: None,
            min_retry_delay: None,
            max_retry_delay: None,
            connection_timeout: None,
            response_timeout: None,
            pipeline_buffer_size: None,
            target_type: TargetType::NotifyEvent,
        }
    }
}

pub fn validate_redis_url(url: &Url) -> Result<(), TargetError> {
    let _: ConnectionInfo = url.clone().into_connection_info().map_err(map_redis_error)?;
    Ok(())
}

fn validate_redis_tls_config(url: &Url, tls: &RedisTlsConfig) -> Result<(), TargetError> {
    let secure_scheme = matches!(url.scheme(), "rediss" | "valkeys");

    if !tls.client_cert_path.is_empty() && !Path::new(&tls.client_cert_path).is_absolute() {
        return Err(TargetError::Configuration(format!("{REDIS_TLS_CLIENT_CERT} must be an absolute path")));
    }
    if !tls.client_key_path.is_empty() && !Path::new(&tls.client_key_path).is_absolute() {
        return Err(TargetError::Configuration(format!("{REDIS_TLS_CLIENT_KEY} must be an absolute path")));
    }
    if tls.client_cert_path.is_empty() != tls.client_key_path.is_empty() {
        return Err(TargetError::Configuration(
            "Redis tls_client_cert and tls_client_key must be specified together".to_string(),
        ));
    }

    if !secure_scheme {
        if tls.policy.is_some()
            || !tls.ca_path.is_empty()
            || !tls.client_cert_path.is_empty()
            || !tls.client_key_path.is_empty()
            || tls.allow_insecure
        {
            return Err(TargetError::Configuration(
                "TLS settings are only allowed for rediss/valkeys schemes".to_string(),
            ));
        }
        return Ok(());
    }

    if let Some(policy) = tls.policy {
        match policy {
            RedisTlsPolicy::SystemCa => {
                if !tls.ca_path.is_empty() {
                    return Err(TargetError::Configuration(format!(
                        "{REDIS_TLS_CA} is not allowed when {REDIS_TLS_POLICY}=system_ca"
                    )));
                }
            }
            RedisTlsPolicy::CustomCa => {
                if tls.ca_path.is_empty() {
                    return Err(TargetError::Configuration(format!(
                        "{REDIS_TLS_CA} is required when {REDIS_TLS_POLICY}=custom_ca"
                    )));
                }
                if !Path::new(&tls.ca_path).is_absolute() {
                    return Err(TargetError::Configuration(format!("{REDIS_TLS_CA} must be an absolute path")));
                }
            }
        }
    } else if !tls.ca_path.is_empty() && !Path::new(&tls.ca_path).is_absolute() {
        return Err(TargetError::Configuration(format!("{REDIS_TLS_CA} must be an absolute path")));
    }

    Ok(())
}

fn ensure_rustls_provider_installed() {
    if rustls::crypto::CryptoProvider::get_default().is_none()
        && rustls::crypto::aws_lc_rs::default_provider().install_default().is_err()
    {
        debug!("rustls crypto provider was installed concurrently, skipping aws-lc-rs install");
    }
}

pub struct RedisTarget<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    id: TargetID,
    args: RedisArgs,
    publisher_client: Client,
    publisher: Arc<Mutex<Option<ConnectionManager>>>,
    store: Option<Box<dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync>>,
    /// Business-level liveness flag.
    ///
    /// We only flip this to `false` on final/terminal failure paths (for example: init failed,
    /// publish exhausted retries, or the target was explicitly closed). Temporary reconnectable
    /// errors only invalidate the cached publisher so that a later request can lazily rebuild it.
    connected: Arc<AtomicBool>,
    delivery_counters: Arc<TargetDeliveryCounters>,
    _phantom: std::marker::PhantomData<E>,
}

impl<E> RedisTarget<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    #[instrument(skip(args), fields(target_id_as_string = %id))]
    pub fn new(id: String, args: RedisArgs) -> Result<Self, TargetError> {
        args.validate()?;

        let target_id = TargetID::new(id, ChannelTargetType::Redis.as_str().to_string());
        let publisher_client = build_redis_client(&args)?;

        let queue_store = if !args.queue_dir.is_empty() {
            let base_path = PathBuf::from(&args.queue_dir);
            let specific_queue_path = base_path.join(format!("rustfs-{}-{}", ChannelTargetType::Redis.as_str(), target_id.id));
            let extension = match args.target_type {
                TargetType::AuditLog => rustfs_config::audit::AUDIT_STORE_EXTENSION,
                TargetType::NotifyEvent => rustfs_config::notify::NOTIFY_STORE_EXTENSION,
            };
            let store = QueueStore::<QueuedPayload>::new(specific_queue_path, args.queue_limit, extension);
            if let Err(e) = store.open() {
                error!(target_id = %target_id, error = %e, "Failed to open store for Redis target");
                return Err(TargetError::Storage(format!("{e}")));
            }
            Some(Box::new(store) as Box<dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync>)
        } else {
            None
        };

        info!(target_id = %target_id, "Redis target created");
        Ok(Self {
            id: target_id,
            args,
            publisher_client,
            publisher: Arc::new(Mutex::new(None)),
            store: queue_store,
            connected: Arc::new(AtomicBool::new(false)),
            delivery_counters: Arc::new(TargetDeliveryCounters::default()),
            _phantom: std::marker::PhantomData,
        })
    }

    pub fn clone_box(&self) -> Box<dyn Target<E> + Send + Sync> {
        Box::new(Self {
            id: self.id.clone(),
            args: self.args.clone(),
            publisher_client: self.publisher_client.clone(),
            publisher: Arc::clone(&self.publisher),
            store: self.store.as_ref().map(|s| s.boxed_clone()),
            connected: Arc::clone(&self.connected),
            delivery_counters: Arc::clone(&self.delivery_counters),
            _phantom: std::marker::PhantomData,
        })
    }

    fn build_queued_payload(&self, event: &EntityTarget<E>) -> Result<QueuedPayload, TargetError> {
        let object_name = crate::target::decode_object_name(&event.object_name)?;
        let key = format!("{}/{}", event.bucket_name, object_name);
        let log = TargetLog {
            event_name: event.event_name,
            key,
            records: vec![event.clone()],
        };
        let body = serde_json::to_vec(&log).map_err(|e| TargetError::Serialization(format!("Failed to serialize event: {e}")))?;
        let meta = QueuedPayloadMeta::new(
            event.event_name,
            event.bucket_name.clone(),
            event.object_name.clone(),
            "application/json",
            body.len(),
        );
        Ok(QueuedPayload::new(meta, body))
    }

    async fn get_or_create_publisher(&self) -> Result<ConnectionManager, TargetError> {
        let mut guard = self.publisher.lock().await;
        if let Some(manager) = guard.clone() {
            return Ok(manager);
        }

        let manager = self
            .publisher_client
            .get_connection_manager_lazy(build_redis_connection_manager_config(&self.args))
            .map_err(map_redis_error)?;

        *guard = Some(manager.clone());
        Ok(manager)
    }

    async fn invalidate_cached_publisher(&self) {
        // Intentionally does not touch `connected`: invalidating the current manager only means
        // "recreate the publisher on the next attempt", not "this target is now definitively
        // inactive". That distinction preserves the business semantics of `is_active()`.
        *self.publisher.lock().await = None;
    }

    async fn ensure_publisher_ready(&self) -> Result<(), TargetError> {
        let mut publisher = self.get_or_create_publisher().await?;
        match cmd("PING").query_async::<String>(&mut publisher).await {
            Ok(_) => Ok(()),
            Err(err) => {
                let mapped = map_redis_error(err);
                if is_retryable_target_error(&mapped) {
                    self.invalidate_cached_publisher().await;
                }
                Err(mapped)
            }
        }
    }

    async fn init_inner(&self) -> Result<(), TargetError> {
        if let Err(err) = self.ensure_publisher_ready().await {
            self.connected.store(false, Ordering::SeqCst);
            return Err(err);
        }
        self.connected.store(true, Ordering::SeqCst);
        Ok(())
    }

    #[instrument(skip(self, body, meta), fields(target_id = %self.id))]
    async fn send_body(&self, body: Vec<u8>, meta: &QueuedPayloadMeta) -> Result<(), TargetError> {
        debug!(
            target = %self.id,
            bucket = %meta.bucket_name,
            object = %meta.object_name,
            event = %meta.event_name,
            payload_len = body.len(),
            channel = %self.args.channel,
            "Sending Redis payload"
        );

        let mut attempt = 0usize;
        let mut last_error = None;
        while attempt < self.args.max_retry_attempts {
            attempt += 1;

            let mut publisher = self.get_or_create_publisher().await?;
            match publisher
                .publish::<_, _, i64>(self.args.channel.as_str(), body.as_slice())
                .await
            {
                Ok(_) => {
                    debug!(target_id = %self.id, channel = %self.args.channel, attempt, "Event published to Redis channel");
                    self.delivery_counters.record_success();
                    return Ok(());
                }
                Err(err) => {
                    let mapped = map_redis_error(err);
                    if is_retryable_target_error(&mapped) {
                        self.invalidate_cached_publisher().await;
                    }

                    warn!(
                        target_id = %self.id,
                        channel = %self.args.channel,
                        attempt,
                        max_attempts = self.args.max_retry_attempts,
                        error = %mapped,
                        "Redis publish attempt failed"
                    );

                    if !is_retryable_target_error(&mapped) || attempt >= self.args.max_retry_attempts {
                        last_error = Some(mapped);
                        break;
                    }

                    last_error = Some(mapped);
                    tokio::time::sleep(compute_retry_delay(
                        attempt,
                        self.args.min_retry_delay.unwrap_or(Duration::from_millis(100)),
                        self.args.max_retry_delay.unwrap_or(Duration::from_secs(2)),
                    ))
                    .await;
                }
            }
        }

        self.connected.store(false, Ordering::SeqCst);

        Err(last_error.unwrap_or(TargetError::Unknown("Redis publish failed without a captured error".to_string())))
    }
}

#[async_trait]
impl<E> Target<E> for RedisTarget<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    fn id(&self) -> TargetID {
        self.id.clone()
    }

    async fn is_active(&self) -> Result<bool, TargetError> {
        match tokio::time::timeout(Duration::from_secs(5), ping_redis_server(&self.publisher_client, &self.args)).await {
            Ok(Ok(())) => {
                self.connected.store(true, Ordering::SeqCst);
                Ok(true)
            }
            Ok(Err(err)) => {
                self.invalidate_cached_publisher().await;
                self.connected.store(false, Ordering::SeqCst);
                Err(err)
            }
            Err(_) => {
                self.invalidate_cached_publisher().await;
                self.connected.store(false, Ordering::SeqCst);
                Err(TargetError::Timeout("Redis connection timed out".to_string()))
            }
        }
    }

    async fn save(&self, event: Arc<EntityTarget<E>>) -> Result<(), TargetError> {
        let queued = match self.build_queued_payload(&event) {
            Ok(queued) => queued,
            Err(err) => {
                self.delivery_counters.record_final_failure();
                return Err(err);
            }
        };

        if let Some(store) = &self.store {
            let encoded = match queued.encode() {
                Ok(encoded) => encoded,
                Err(err) => {
                    self.delivery_counters.record_final_failure();
                    return Err(TargetError::Storage(format!("Failed to encode queued payload: {err}")));
                }
            };

            if let Err(e) = store.put_raw(&encoded) {
                self.delivery_counters.record_final_failure();
                return Err(TargetError::Storage(format!("Failed to save event to store: {e}")));
            }

            debug!(target_id = %self.id, "Event saved to store for Redis target");
            Ok(())
        } else {
            if !self.is_enabled() {
                return Err(TargetError::Disabled);
            }

            if let Err(err) = self.init_inner().await {
                self.delivery_counters.record_final_failure();
                return Err(err);
            }

            if let Err(err) = self.send_body(queued.body, &queued.meta).await {
                self.delivery_counters.record_final_failure();
                return Err(err);
            }

            Ok(())
        }
    }

    async fn send_raw_from_store(&self, key: Key, body: Vec<u8>, meta: QueuedPayloadMeta) -> Result<(), TargetError> {
        debug!(target_id = %self.id, ?key, "Attempting to send queued payload from Redis store");

        if !self.is_enabled() {
            return Err(TargetError::Disabled);
        }

        if let Err(err) = self.init_inner().await {
            if matches!(err, TargetError::NotConnected | TargetError::Timeout(_) | TargetError::Network(_)) {
                warn!(target_id = %self.id, error = %err, "Redis target not ready; queued event remains in store");
            }
            return Err(err);
        }

        if let Err(err) = self.send_body(body, &meta).await {
            if matches!(err, TargetError::NotConnected | TargetError::Timeout(_) | TargetError::Network(_)) {
                warn!(target_id = %self.id, error = %err, "Failed to send Redis event from store: target not connected. Event remains queued.");
            }
            return Err(err);
        }

        debug!(target_id = %self.id, ?key, "Queued Redis payload sent successfully");
        Ok(())
    }

    async fn close(&self) -> Result<(), TargetError> {
        self.invalidate_cached_publisher().await;
        self.connected.store(false, Ordering::SeqCst);
        info!(target_id = %self.id, "Redis target closed");
        Ok(())
    }

    fn store(&self) -> Option<&(dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync)> {
        self.store.as_deref()
    }

    fn clone_dyn(&self) -> Box<dyn Target<E> + Send + Sync> {
        self.clone_box()
    }

    async fn init(&self) -> Result<(), TargetError> {
        if !self.is_enabled() {
            return Ok(());
        }
        self.init_inner().await
    }

    fn is_enabled(&self) -> bool {
        self.args.enable
    }

    fn delivery_snapshot(&self) -> TargetDeliverySnapshot {
        self.delivery_counters
            .snapshot(self.store.as_deref().map_or(0, |store| store.len() as u64))
    }

    fn record_final_failure(&self) {
        self.delivery_counters.record_final_failure();
    }
}

pub(crate) fn build_redis_client(args: &RedisArgs) -> Result<Client, TargetError> {
    let mut url = args.url.clone();
    if args.tls.allow_insecure {
        url.set_fragment(Some("insecure"));
    }

    let mut connection_info: ConnectionInfo = url.into_connection_info().map_err(map_redis_error)?;

    let base_redis = connection_info.redis_settings().clone();

    let mut redis_settings = base_redis.clone().set_lib_name("rustfs-targets", env!("CARGO_PKG_VERSION"));

    if let Some(username) = args.username.as_deref().filter(|value| !value.is_empty()) {
        if base_redis.username().is_some_and(|base| base != username) {
            warn!(url_username = ?base_redis.username(), arg_username = %username, "Redis target protocol username from URL is being overridden");
        }
        redis_settings = redis_settings.set_username(username);
    }
    if let Some(password) = args.password.as_deref().filter(|value| !value.is_empty()) {
        if base_redis.password().is_some() {
            warn!("RedisArgs.password overrides password from Redis URL");
        }
        redis_settings = redis_settings.set_password(password);
    }

    let mut tcp_settings = TcpSettings::default().set_nodelay(true);
    #[cfg(not(target_family = "wasm"))]
    {
        if !args.keep_alive.is_zero() {
            tcp_settings = tcp_settings.set_keepalive(socket2::TcpKeepalive::new().with_time(args.keep_alive));
        }
    }

    connection_info = connection_info
        .set_redis_settings(redis_settings)
        .set_tcp_settings(tcp_settings);

    let secure_scheme = matches!(args.url.scheme(), "rediss" | "valkeys");
    if secure_scheme {
        ensure_rustls_provider_installed();
        let tls_certs = TlsCertificates {
            client_tls: read_client_tls(&args.tls)?,
            root_cert: read_root_cert(&args.tls)?,
        };
        Client::build_with_tls(connection_info, tls_certs).map_err(map_redis_error)
    } else {
        Client::open(connection_info).map_err(map_redis_error)
    }
}

pub(crate) fn build_redis_connection_manager_config(args: &RedisArgs) -> ConnectionManagerConfig {
    let mut config = ConnectionManagerConfig::new();

    if let Some(reconnect_retry_attempts) = args.reconnect_retry_attempts {
        config = config.set_number_of_retries(reconnect_retry_attempts);
    }
    if let Some(min_retry_delay) = args.min_retry_delay {
        config = config.set_min_delay(min_retry_delay);
    }
    if let Some(max_retry_delay) = args.max_retry_delay {
        config = config.set_max_delay(max_retry_delay);
    }
    if let Some(connection_timeout) = args.connection_timeout {
        config = config.set_connection_timeout(Some(connection_timeout));
    }
    if let Some(response_timeout) = args.response_timeout {
        config = config.set_response_timeout(Some(response_timeout));
    }
    if let Some(pipeline_buffer_size) = args.pipeline_buffer_size {
        config = config.set_pipeline_buffer_size(pipeline_buffer_size);
    }

    config
}

pub(crate) async fn ping_redis_server(client: &Client, args: &RedisArgs) -> Result<(), TargetError> {
    let config = build_redis_connection_manager_config(args);
    let mut conn = client
        .get_connection_manager_with_config(config)
        .await
        .map_err(map_redis_error)?;

    cmd("PING")
        .query_async::<String>(&mut conn)
        .await
        .map_err(map_redis_error)?;

    Ok(())
}

fn read_client_tls(tls: &RedisTlsConfig) -> Result<Option<ClientTlsConfig>, TargetError> {
    if tls.client_cert_path.is_empty() {
        return Ok(None);
    }

    let client_cert = std::fs::read(&tls.client_cert_path)
        .map_err(|e| TargetError::Configuration(format!("Failed to read Redis client cert: {e}")))?;
    let client_key = std::fs::read(&tls.client_key_path)
        .map_err(|e| TargetError::Configuration(format!("Failed to read Redis client key: {e}")))?;

    Ok(Some(ClientTlsConfig { client_cert, client_key }))
}

fn read_root_cert(tls: &RedisTlsConfig) -> Result<Option<Vec<u8>>, TargetError> {
    if tls.ca_path.is_empty() {
        return Ok(None);
    }

    std::fs::read(&tls.ca_path)
        .map(Some)
        .map_err(|e| TargetError::Configuration(format!("Failed to read Redis root CA cert: {e}")))
}

fn map_redis_error(err: RedisError) -> TargetError {
    use redis::ErrorKind;

    match err.kind() {
        ErrorKind::AuthenticationFailed => TargetError::Authentication(err.to_string()),
        ErrorKind::RESP3NotSupported => TargetError::Initialization(err.to_string()),
        ErrorKind::InvalidClientConfig => TargetError::Configuration(err.to_string()),
        ErrorKind::Io if err.is_timeout() => TargetError::Timeout(err.to_string()),
        ErrorKind::Io if err.is_connection_dropped() || err.is_connection_refusal() => TargetError::NotConnected,
        ErrorKind::Io => TargetError::Network(err.to_string()),
        _ if err.is_unrecoverable_error() => TargetError::NotConnected,
        _ => TargetError::Request(err.to_string()),
    }
}

fn is_retryable_target_error(err: &TargetError) -> bool {
    matches!(err, TargetError::NotConnected | TargetError::Timeout(_) | TargetError::Network(_))
}

fn compute_retry_delay(attempt: usize, min_delay: Duration, max_delay: Duration) -> Duration {
    let shift = attempt.saturating_sub(1).min(16) as u32;
    let factor = 1u32 << shift;
    min_delay.saturating_mul(factor).min(max_delay)
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis::ProtocolVersion;
    use std::sync::atomic::Ordering;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    fn base_args() -> RedisArgs {
        RedisArgs {
            enable: true,
            url: Url::parse("redis://127.0.0.1:6379").unwrap(),
            channel: "rustfs-events".to_string(),
            username: None,
            password: None,
            tls: RedisTlsConfig::default(),
            keep_alive: Duration::from_secs(15),
            queue_dir: String::new(),
            queue_limit: 0,
            max_retry_attempts: 3,
            reconnect_retry_attempts: None,
            min_retry_delay: None,
            max_retry_delay: None,
            connection_timeout: None,
            response_timeout: None,
            pipeline_buffer_size: None,
            target_type: TargetType::NotifyEvent,
        }
    }

    #[test]
    fn validate_rejects_empty_channel() {
        let args = RedisArgs {
            channel: String::new(),
            ..base_args()
        };
        assert!(args.validate().is_err());
    }

    #[test]
    fn validate_accepts_embedded_credentials_in_url() {
        let url = Url::parse("redis://user:pass@127.0.0.1:6379").unwrap();
        assert!(validate_redis_url(&url).is_ok());
    }

    #[test]
    fn validate_rejects_relative_queue_dir() {
        let args = RedisArgs {
            queue_dir: "relative/path".to_string(),
            ..base_args()
        };
        assert!(args.validate().is_err());
    }

    #[test]
    fn validate_accepts_custom_ca_tls_policy() {
        let args = RedisArgs {
            url: Url::parse("rediss://127.0.0.1:6379").unwrap(),
            tls: RedisTlsConfig {
                policy: Some(RedisTlsPolicy::CustomCa),
                ca_path: "/tmp/ca.pem".to_string(),
                ..RedisTlsConfig::default()
            },
            ..base_args()
        };
        assert!(args.validate().is_ok());
    }

    #[test]
    fn validate_rejects_insecure_tls_for_non_secure_scheme() {
        let args = RedisArgs {
            tls: RedisTlsConfig {
                allow_insecure: true,
                ..RedisTlsConfig::default()
            },
            ..base_args()
        };
        assert!(args.validate().is_err());
    }

    #[test]
    fn build_redis_client_preserves_url_auth_when_args_are_none() {
        let args = RedisArgs {
            url: Url::parse("redis://user:pass@127.0.0.1:6379/2").unwrap(),
            ..base_args()
        };

        let client = build_redis_client(&args).expect("client should build");
        let info = client.get_connection_info();
        let redis = info.redis_settings();

        assert_eq!(redis.username(), Some("user"));
        assert_eq!(redis.password(), Some("pass"));
        assert_eq!(redis.db(), 2);
    }

    #[test]
    fn build_redis_client_overrides_url_auth_when_args_are_set() {
        let args = RedisArgs {
            url: Url::parse("redis://user:pass@127.0.0.1:6379/2").unwrap(),
            username: Some("override-user".to_string()),
            password: Some("override-pass".to_string()),
            ..base_args()
        };

        let client = build_redis_client(&args).expect("client should build");
        let redis = client.get_connection_info().redis_settings();

        assert_eq!(redis.username(), Some("override-user"));
        assert_eq!(redis.password(), Some("override-pass"));
        assert_eq!(redis.db(), 2);
    }

    #[test]
    fn build_redis_client_preserves_url_protocol_when_args_do_not_override_it() {
        let args = RedisArgs {
            url: Url::parse("redis://127.0.0.1:6379/?protocol=resp3").unwrap(),
            ..base_args()
        };

        let client = build_redis_client(&args).expect("client should build");
        let redis = client.get_connection_info().redis_settings();

        assert_eq!(redis.protocol(), ProtocolVersion::RESP3);
    }

    #[test]
    fn build_redis_client_enables_insecure_tls_when_requested() {
        let args = RedisArgs {
            url: Url::parse("rediss://127.0.0.1:6379").unwrap(),
            tls: RedisTlsConfig {
                allow_insecure: true,
                ..RedisTlsConfig::default()
            },
            ..base_args()
        };

        let client = build_redis_client(&args).expect("client should build");
        match client.get_connection_info().addr() {
            redis::ConnectionAddr::TcpTls { insecure, .. } => assert!(*insecure),
            other => panic!("expected TLS address, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn invalidate_cached_publisher_keeps_connected_state() {
        let target = RedisTarget::<String>::new("redis:test".to_string(), base_args()).expect("target should build");
        target.connected.store(true, Ordering::SeqCst);

        target.invalidate_cached_publisher().await;

        assert!(target.connected.load(Ordering::SeqCst));
        assert!(target.publisher.lock().await.is_none());
    }

    #[tokio::test]
    async fn is_active_succeeds_when_ping_returns_pong() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind fake redis");
        let addr = listener.local_addr().expect("listener addr");
        tokio::spawn(run_fake_redis_server(listener, false));

        let mut args = base_args();
        args.url = Url::parse(&format!("redis://{}:{}/0", addr.ip(), addr.port())).unwrap();

        let target = RedisTarget::<String>::new("redis:test".to_string(), args).expect("target should build");
        target.connected.store(false, Ordering::SeqCst);

        assert!(target.is_active().await.expect("ping should succeed"));
        assert!(target.connected.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn is_active_returns_error_when_ping_fails() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind fake redis");
        let addr = listener.local_addr().expect("listener addr");
        tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    return;
                };
                drop(socket);
            }
        });

        let mut args = base_args();
        args.url = Url::parse(&format!("redis://{}:{}/0", addr.ip(), addr.port())).unwrap();

        let target = RedisTarget::<String>::new("redis:test".to_string(), args).expect("target should build");
        target.connected.store(true, Ordering::SeqCst);

        let err = target.is_active().await.expect_err("ping should fail");
        assert!(matches!(
            err,
            TargetError::NotConnected | TargetError::Network(_) | TargetError::Timeout(_)
        ));
        assert!(!target.connected.load(Ordering::SeqCst));
        assert!(target.publisher.lock().await.is_none());
    }

    #[test]
    fn compute_retry_delay_is_bounded() {
        let min = Duration::from_millis(100);
        let max = Duration::from_secs(2);

        assert_eq!(compute_retry_delay(1, min, max), min);
        assert!(compute_retry_delay(5, min, max) <= max);
        assert_eq!(compute_retry_delay(50, min, max), max);
    }

    fn parse_resp_array(input: &[u8]) -> Option<(Vec<String>, usize)> {
        if input.first()? != &b'*' {
            return None;
        }

        let mut index = 1;
        let len_end = input[index..].windows(2).position(|w| w == b"\r\n")? + index;
        let items: usize = std::str::from_utf8(&input[index..len_end]).ok()?.parse().ok()?;
        index = len_end + 2;

        let mut out = Vec::with_capacity(items);
        for _ in 0..items {
            if input.get(index)? != &b'$' {
                return None;
            }
            index += 1;
            let bulk_end = input[index..].windows(2).position(|w| w == b"\r\n")? + index;
            let bulk_len: usize = std::str::from_utf8(&input[index..bulk_end]).ok()?.parse().ok()?;
            index = bulk_end + 2;

            let data_end = index.checked_add(bulk_len)?;
            let data = std::str::from_utf8(input.get(index..data_end)?).ok()?.to_string();
            out.push(data);
            index = data_end + 2;
        }

        Some((out, index))
    }

    async fn run_fake_redis_server(listener: TcpListener, close_first_connection: bool) {
        let mut first = close_first_connection;
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };

            if first {
                first = false;
                drop(socket);
                continue;
            }

            tokio::spawn(async move {
                let mut buf = vec![0_u8; 4096];
                let mut pending = Vec::new();

                loop {
                    let Ok(read) = socket.read(&mut buf).await else {
                        return;
                    };
                    if read == 0 {
                        return;
                    }

                    pending.extend_from_slice(&buf[..read]);

                    while let Some((command, consumed)) = parse_resp_array(&pending) {
                        pending.drain(..consumed);
                        let response = match command.first().map(|s| s.as_str()) {
                            Some("PING") => b"+PONG\r\n".as_slice(),
                            Some("PUBLISH") => b":1\r\n".as_slice(),
                            Some("CLIENT") => b"+OK\r\n".as_slice(),
                            Some("AUTH") => b"+OK\r\n".as_slice(),
                            Some("SELECT") => b"+OK\r\n".as_slice(),
                            Some("HELLO") => b"%1\r\n+server\r\n+redis\r\n".as_slice(),
                            _ => b"+OK\r\n".as_slice(),
                        };

                        if socket.write_all(response).await.is_err() {
                            return;
                        }
                    }
                }
            });
        }
    }

    #[tokio::test]
    async fn send_body_keeps_connected_true_when_retryable_error_eventually_recovers() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind fake redis");
        let addr = listener.local_addr().expect("listener addr");
        tokio::spawn(run_fake_redis_server(listener, true));

        let mut args = base_args();
        args.url = Url::parse(&format!("redis://{}:{}/0", addr.ip(), addr.port())).unwrap();
        args.max_retry_attempts = 3;
        args.reconnect_retry_attempts = Some(0);
        args.min_retry_delay = Some(Duration::from_millis(10));
        args.max_retry_delay = Some(Duration::from_millis(20));

        let target = RedisTarget::<String>::new("redis:test".to_string(), args).expect("target should build");
        let meta = QueuedPayloadMeta::new(
            rustfs_s3_common::EventName::ObjectCreatedPut,
            "bucket".to_string(),
            "object".to_string(),
            "application/json",
            2,
        );

        target.connected.store(true, Ordering::SeqCst);
        target
            .send_body(b"{}".to_vec(), &meta)
            .await
            .expect("eventual retry should succeed");

        assert!(target.connected.load(Ordering::SeqCst));
        assert_eq!(target.delivery_snapshot().total_messages, 1);
    }

    #[tokio::test]
    async fn send_body_sets_connected_false_after_retry_exhaustion() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind fake redis");
        let addr = listener.local_addr().expect("listener addr");
        tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    return;
                };
                drop(socket);
            }
        });

        let mut args = base_args();
        args.url = Url::parse(&format!("redis://{}:{}/0", addr.ip(), addr.port())).unwrap();
        args.max_retry_attempts = 2;
        args.reconnect_retry_attempts = Some(0);
        args.min_retry_delay = Some(Duration::from_millis(10));
        args.max_retry_delay = Some(Duration::from_millis(20));

        let target = RedisTarget::<String>::new("redis:test".to_string(), args).expect("target should build");
        let meta = QueuedPayloadMeta::new(
            rustfs_s3_common::EventName::ObjectCreatedPut,
            "bucket".to_string(),
            "object".to_string(),
            "application/json",
            2,
        );

        let err = target
            .send_body(b"{}".to_vec(), &meta)
            .await
            .expect_err("all retries should fail");
        assert!(matches!(
            err,
            TargetError::NotConnected | TargetError::Network(_) | TargetError::Timeout(_)
        ));
        assert!(!target.connected.load(Ordering::SeqCst));
        assert_eq!(target.delivery_snapshot().total_messages, 0);
    }

    #[tokio::test]
    async fn send_raw_from_store_failure_does_not_count_as_success() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind fake redis");
        let addr = listener.local_addr().expect("listener addr");
        tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    return;
                };
                drop(socket);
            }
        });

        let mut args = base_args();
        args.url = Url::parse(&format!("redis://{}:{}/0", addr.ip(), addr.port())).unwrap();
        args.max_retry_attempts = 1;
        args.reconnect_retry_attempts = Some(0);

        let target = RedisTarget::<String>::new("redis:test".to_string(), args).expect("target should build");
        let meta = QueuedPayloadMeta::new(
            rustfs_s3_common::EventName::ObjectCreatedPut,
            "bucket".to_string(),
            "object".to_string(),
            "application/json",
            2,
        );

        let err = target
            .send_raw_from_store(
                crate::store::Key {
                    name: "key".to_string(),
                    extension: String::new(),
                    item_count: 1,
                    compress: false,
                },
                b"{}".to_vec(),
                meta,
            )
            .await
            .expect_err("send from store should fail");

        assert!(matches!(
            err,
            TargetError::NotConnected | TargetError::Network(_) | TargetError::Timeout(_)
        ));
        assert_eq!(target.delivery_snapshot().total_messages, 0);
    }
}
