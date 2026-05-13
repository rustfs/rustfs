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
    StoreError, Target,
    arn::TargetID,
    error::TargetError,
    store::{Key, Store},
    target::{
        ChannelTargetType, EntityTarget, QueuedPayload, QueuedPayloadMeta, TargetDeliveryCounters, TargetDeliverySnapshot,
        TargetType, build_queued_payload_with_records, open_target_queue_store, persist_queued_payload_to_store,
    },
};
use async_trait::async_trait;
use hyper_rustls::ConfigBuilderExt;
use rumqttc::{
    AsyncClient, Broker, ConnectionError, EventLoop, Incoming, MqttOptions, Outgoing, QoS, Transport,
    mqttbytes::Error as MqttBytesError,
};
use rustfs_config::{
    EnableState, MQTT_TLS_CA, MQTT_TLS_CLIENT_CERT, MQTT_TLS_CLIENT_KEY, MQTT_TLS_TRUST_LEAF_AS_CA, MQTT_WS_PATH_ALLOWLIST,
};
use rustls::ClientConfig;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::sync::Arc;
use std::{
    marker::PhantomData,
    path::Path,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};
use tokio::sync::{Mutex, OnceCell, mpsc};
use tracing::{debug, error, info, instrument, trace, warn};
use url::Url;

const DEFAULT_CONNECTION_TIMEOUT: Duration = Duration::from_secs(15);
const EVENT_LOOP_POLL_TIMEOUT: Duration = Duration::from_secs(10); // For initial connection check in task
const DEFAULT_MQTT_TCP_PORT: u16 = 1883;
const DEFAULT_MQTT_TLS_PORT: u16 = 8883;
const DEFAULT_MQTT_WSS_PORT: u16 = 443;
const MAX_MQTT_PACKET_SIZE_BYTES: u32 = 100 * 1024 * 1024;
const DEFAULT_MQTT_WS_PATH_ALLOWLIST: &[&str] = &["/", "/mqtt"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MQTTTlsPolicy {
    SystemCa,
    CustomCa,
}

impl MQTTTlsPolicy {
    fn parse(value: &str) -> Result<Self, TargetError> {
        match value.trim() {
            value if value.eq_ignore_ascii_case("system_ca") => Ok(Self::SystemCa),
            value if value.eq_ignore_ascii_case("custom_ca") => Ok(Self::CustomCa),
            _ => Err(TargetError::Configuration(
                "MQTT tls_policy must be one of: system_ca, custom_ca".to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MQTTTlsConfig {
    pub policy: Option<MQTTTlsPolicy>,
    pub ca_path: String,
    pub client_cert_path: String,
    pub client_key_path: String,
    pub trust_leaf_as_ca: bool,
    pub ws_path_allowlist: Vec<String>,
}

impl MQTTTlsConfig {
    pub fn from_values(
        policy: Option<&str>,
        ca_path: Option<&str>,
        client_cert_path: Option<&str>,
        client_key_path: Option<&str>,
        trust_leaf_as_ca: Option<&str>,
        ws_path_allowlist: Option<&str>,
    ) -> Result<Self, TargetError> {
        let policy = match policy.map(str::trim).filter(|value| !value.is_empty()) {
            Some(value) => Some(MQTTTlsPolicy::parse(value)?),
            None => None,
        };

        let trust_leaf_as_ca = match trust_leaf_as_ca.map(str::trim).filter(|value| !value.is_empty()) {
            Some(value) => value
                .parse::<EnableState>()
                .map(EnableState::is_enabled)
                .map_err(|_| TargetError::Configuration(format!("Invalid value for {MQTT_TLS_TRUST_LEAF_AS_CA}")))?,
            None => false,
        };

        let ws_path_allowlist = match ws_path_allowlist.map(str::trim).filter(|value| !value.is_empty()) {
            Some(value) => parse_ws_path_allowlist(value)?,
            None => Vec::new(),
        };

        Ok(Self {
            policy,
            ca_path: ca_path.unwrap_or_default().trim().to_string(),
            client_cert_path: client_cert_path.unwrap_or_default().trim().to_string(),
            client_key_path: client_key_path.unwrap_or_default().trim().to_string(),
            trust_leaf_as_ca,
            ws_path_allowlist,
        })
    }

    fn effective_ws_path_allowlist(&self) -> Vec<&str> {
        if self.ws_path_allowlist.is_empty() {
            DEFAULT_MQTT_WS_PATH_ALLOWLIST.to_vec()
        } else {
            self.ws_path_allowlist.iter().map(String::as_str).collect()
        }
    }
}

fn parse_ws_path_allowlist(value: &str) -> Result<Vec<String>, TargetError> {
    let mut allowlist = Vec::new();
    for raw in value.split(',') {
        let path = raw.trim();
        if path.is_empty() {
            continue;
        }
        if !path.starts_with('/') || path.contains('?') || path.contains('#') {
            return Err(TargetError::Configuration(format!(
                "{MQTT_WS_PATH_ALLOWLIST} entries must be absolute paths without query or fragment"
            )));
        }
        allowlist.push(path.to_string());
    }

    if allowlist.is_empty() {
        return Err(TargetError::Configuration(format!(
            "{MQTT_WS_PATH_ALLOWLIST} must contain at least one websocket path"
        )));
    }

    Ok(allowlist)
}

fn keep_alive_seconds(duration: Duration) -> u16 {
    duration.as_secs().min(u64::from(u16::MAX)) as u16
}

fn default_broker_port(scheme: &str) -> u16 {
    match scheme {
        "ssl" | "tls" | "tcps" | "mqtts" => DEFAULT_MQTT_TLS_PORT,
        "wss" => DEFAULT_MQTT_WSS_PORT,
        _ => DEFAULT_MQTT_TCP_PORT,
    }
}

fn websocket_broker_url(broker: &Url, secure: bool) -> Result<String, TargetError> {
    let mut url = broker.clone();
    url.set_scheme("ws")
        .map_err(|_| TargetError::Configuration("Failed to normalize websocket broker URL scheme".to_string()))?;

    if secure && url.port().is_none() {
        url.set_port(Some(DEFAULT_MQTT_WSS_PORT))
            .map_err(|_| TargetError::Configuration("Failed to set default secure websocket broker port".to_string()))?;
    }

    Ok(url.to_string())
}

fn validate_path_is_absolute(path: &str, field: &str) -> Result<(), TargetError> {
    if !Path::new(path).is_absolute() {
        return Err(TargetError::Configuration(format!("{field} must be an absolute path")));
    }
    Ok(())
}

fn build_root_store(ca_path: &str, trust_leaf_as_ca: bool) -> Result<rustls::RootCertStore, TargetError> {
    let certs =
        rustfs_utils::load_certs(ca_path).map_err(|e| TargetError::Configuration(format!("Failed to load MQTT tls_ca: {e}")))?;
    let mut store = rustls::RootCertStore::empty();

    if trust_leaf_as_ca {
        let (valid, invalid) = store.add_parsable_certificates(certs);
        if valid == 0 {
            return Err(TargetError::Configuration(format!(
                "MQTT tls_ca did not contain any parsable trust anchors (ignored {invalid} entries)"
            )));
        }
    } else {
        for cert in certs {
            store
                .add(cert)
                .map_err(|e| TargetError::Configuration(format!("Failed to add MQTT tls_ca to root store: {e}")))?;
        }
    }

    Ok(store)
}

fn build_mqtt_tls_transport(broker: &Url, tls: &MQTTTlsConfig) -> Result<Transport, TargetError> {
    super::ensure_rustls_provider_installed();

    let client_config = match tls
        .policy
        .ok_or_else(|| TargetError::Configuration("Secure MQTT schemes require an explicit tls_policy".to_string()))?
    {
        MQTTTlsPolicy::SystemCa => {
            let builder = ClientConfig::builder()
                .with_native_roots()
                .map_err(|e| TargetError::Configuration(format!("Failed to load native root certificates: {e}")))?;

            if tls.client_cert_path.is_empty() {
                builder.with_no_client_auth()
            } else {
                let certs = rustfs_utils::load_certs(&tls.client_cert_path)
                    .map_err(|e| TargetError::Configuration(format!("Failed to load MQTT tls_client_cert: {e}")))?;
                let key = rustfs_utils::load_private_key(&tls.client_key_path)
                    .map_err(|e| TargetError::Configuration(format!("Failed to load MQTT tls_client_key: {e}")))?;
                builder
                    .with_client_auth_cert(certs, key)
                    .map_err(|e| TargetError::Configuration(format!("Failed to build MQTT client mTLS identity: {e}")))?
            }
        }
        MQTTTlsPolicy::CustomCa => {
            let builder = ClientConfig::builder().with_root_certificates(build_root_store(&tls.ca_path, tls.trust_leaf_as_ca)?);

            if tls.client_cert_path.is_empty() {
                builder.with_no_client_auth()
            } else {
                let certs = rustfs_utils::load_certs(&tls.client_cert_path)
                    .map_err(|e| TargetError::Configuration(format!("Failed to load MQTT tls_client_cert: {e}")))?;
                let key = rustfs_utils::load_private_key(&tls.client_key_path)
                    .map_err(|e| TargetError::Configuration(format!("Failed to load MQTT tls_client_key: {e}")))?;
                builder
                    .with_client_auth_cert(certs, key)
                    .map_err(|e| TargetError::Configuration(format!("Failed to build MQTT client mTLS identity: {e}")))?
            }
        }
    };

    if matches!(broker.scheme(), "wss") {
        Ok(Transport::wss_with_config(client_config.into()))
    } else {
        Ok(Transport::tls_with_config(client_config.into()))
    }
}

pub fn validate_mqtt_broker_url(broker: &Url, tls: &MQTTTlsConfig) -> Result<(), TargetError> {
    match broker.scheme() {
        "ws" | "wss" | "tcp" | "ssl" | "tls" | "tcps" | "mqtt" | "mqtts" => {}
        _ => {
            return Err(TargetError::Configuration("unknown protocol in broker address".to_string()));
        }
    }

    if !broker.username().is_empty() || broker.password().is_some() {
        return Err(TargetError::Configuration("Broker URL must not embed username or password".to_string()));
    }

    broker
        .host_str()
        .ok_or_else(|| TargetError::Configuration("Broker is missing host".to_string()))?;

    let secure_scheme = matches!(broker.scheme(), "wss" | "ssl" | "tls" | "tcps" | "mqtts");
    let websocket_scheme = matches!(broker.scheme(), "ws" | "wss");

    if !websocket_scheme {
        if !matches!(broker.path(), "" | "/") {
            return Err(TargetError::Configuration(
                "Broker URL path is only supported for ws/wss schemes".to_string(),
            ));
        }

        if broker.query().is_some() {
            return Err(TargetError::Configuration(
                "Broker URL query is only supported for ws/wss schemes".to_string(),
            ));
        }

        if broker.fragment().is_some() {
            return Err(TargetError::Configuration(
                "Broker URL fragment is only supported for ws/wss schemes".to_string(),
            ));
        }

        if !tls.ws_path_allowlist.is_empty() {
            return Err(TargetError::Configuration(format!(
                "{MQTT_WS_PATH_ALLOWLIST} is only supported for ws/wss schemes"
            )));
        }
    } else if !tls
        .effective_ws_path_allowlist()
        .iter()
        .any(|allowed_path| *allowed_path == broker.path())
    {
        return Err(TargetError::Configuration(format!(
            "Websocket broker path '{}' is not in the {MQTT_WS_PATH_ALLOWLIST} allowlist",
            broker.path()
        )));
    }

    if secure_scheme {
        let policy = tls
            .policy
            .ok_or_else(|| TargetError::Configuration("Secure MQTT schemes require an explicit tls_policy".to_string()))?;

        if !tls.client_cert_path.is_empty() {
            validate_path_is_absolute(&tls.client_cert_path, MQTT_TLS_CLIENT_CERT)?;
        }

        if !tls.client_key_path.is_empty() {
            validate_path_is_absolute(&tls.client_key_path, MQTT_TLS_CLIENT_KEY)?;
        }

        if tls.client_cert_path.is_empty() != tls.client_key_path.is_empty() {
            return Err(TargetError::Configuration(
                "MQTT tls_client_cert and tls_client_key must be specified together".to_string(),
            ));
        }

        match policy {
            MQTTTlsPolicy::SystemCa => {
                if !tls.ca_path.is_empty() {
                    return Err(TargetError::Configuration(format!(
                        "{MQTT_TLS_CA} is not allowed when tls_policy=system_ca"
                    )));
                }
                if tls.trust_leaf_as_ca {
                    return Err(TargetError::Configuration(format!(
                        "{MQTT_TLS_TRUST_LEAF_AS_CA} requires tls_policy=custom_ca"
                    )));
                }
            }
            MQTTTlsPolicy::CustomCa => {
                if tls.ca_path.is_empty() {
                    return Err(TargetError::Configuration(format!("{MQTT_TLS_CA} is required when tls_policy=custom_ca")));
                }
                validate_path_is_absolute(&tls.ca_path, MQTT_TLS_CA)?;
            }
        }
    } else if tls.policy.is_some()
        || !tls.ca_path.is_empty()
        || !tls.client_cert_path.is_empty()
        || !tls.client_key_path.is_empty()
        || tls.trust_leaf_as_ca
    {
        return Err(TargetError::Configuration(
            "TLS settings are only allowed for mqtts/ssl/tls/tcps/wss schemes".to_string(),
        ));
    }

    Ok(())
}

pub(crate) fn build_mqtt_options(
    client_id: String,
    broker: &Url,
    username: Option<&str>,
    password: Option<&str>,
    tls: &MQTTTlsConfig,
    keep_alive: Duration,
    max_packet_size: Option<u32>,
) -> Result<MqttOptions, TargetError> {
    validate_mqtt_broker_url(broker, tls)?;

    let host = broker
        .host_str()
        .ok_or_else(|| TargetError::Configuration("Broker is missing host".to_string()))?;
    let port = broker.port().unwrap_or_else(|| default_broker_port(broker.scheme()));
    let mut mqtt_options = match broker.scheme() {
        "tcp" | "mqtt" => MqttOptions::new(client_id, (host, port)),
        "ssl" | "tls" | "tcps" | "mqtts" => {
            let mut options = MqttOptions::new(client_id, (host, port));
            options.set_transport(build_mqtt_tls_transport(broker, tls)?);
            options
        }
        "ws" => {
            let websocket_broker = Broker::websocket(broker.as_str().to_string())
                .map_err(|e| TargetError::Configuration(format!("Invalid websocket broker URL: {e}")))?;
            MqttOptions::new(client_id, websocket_broker)
        }
        "wss" => {
            let websocket_broker = Broker::websocket(websocket_broker_url(broker, true)?)
                .map_err(|e| TargetError::Configuration(format!("Invalid secure websocket broker URL: {e}")))?;
            let mut options = MqttOptions::new(client_id, websocket_broker);
            options.set_transport(build_mqtt_tls_transport(broker, tls)?);
            options
        }
        _ => {
            return Err(TargetError::Configuration("unknown protocol in broker address".to_string()));
        }
    };

    mqtt_options.set_keep_alive(keep_alive_seconds(keep_alive));

    if let Some(max_packet_size) = max_packet_size {
        mqtt_options.set_max_packet_size(Some(max_packet_size));
    }

    if let Some(user) = username
        && !user.is_empty()
    {
        mqtt_options.set_credentials(user.to_string(), password.unwrap_or("").to_string());
    }

    Ok(mqtt_options)
}

/// Arguments for configuring an MQTT target
#[derive(Debug, Clone)]
pub struct MQTTArgs {
    /// Whether the target is enabled
    pub enable: bool,
    /// The broker URL
    pub broker: Url,
    /// The topic to publish to
    pub topic: String,
    /// The quality of service level
    pub qos: QoS,
    /// The username for the broker
    pub username: String,
    /// The password for the broker
    pub password: String,
    /// Explicit TLS configuration for secure MQTT transports
    pub tls: MQTTTlsConfig,
    /// The maximum interval for reconnection attempts (Note: rumqttc has internal strategy)
    pub max_reconnect_interval: Duration,
    /// The keep alive interval
    pub keep_alive: Duration,
    /// The directory to store events in case of failure
    pub queue_dir: String,
    /// The maximum number of events to store
    pub queue_limit: u64,
    /// the target type
    pub target_type: TargetType,
}

impl MQTTArgs {
    pub fn validate(&self) -> Result<(), TargetError> {
        if !self.enable {
            return Ok(());
        }

        validate_mqtt_broker_url(&self.broker, &self.tls)?;

        if self.topic.is_empty() {
            return Err(TargetError::Configuration("MQTT topic cannot be empty".to_string()));
        }

        if !self.queue_dir.is_empty() {
            let path = Path::new(&self.queue_dir);
            if !path.is_absolute() {
                return Err(TargetError::Configuration("mqtt queue_dir path should be absolute".to_string()));
            }

            if self.qos == QoS::AtMostOnce {
                return Err(TargetError::Configuration(
                    "QoS should be AtLeastOnce (1) or ExactlyOnce (2) if queue_dir is set".to_string(),
                ));
            }
        }
        Ok(())
    }
}

struct BgTaskManager {
    init_cell: OnceCell<tokio::task::JoinHandle<()>>,
    cancel_tx: mpsc::Sender<()>,
    initial_cancel_rx: Mutex<Option<mpsc::Receiver<()>>>,
}

/// A target that sends events to an MQTT broker
pub struct MQTTTarget<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    id: TargetID,
    args: MQTTArgs,
    client: Arc<Mutex<Option<AsyncClient>>>,
    store: Option<Box<dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync>>,
    connected: Arc<AtomicBool>,
    bg_task_manager: Arc<BgTaskManager>,
    delivery_counters: Arc<TargetDeliveryCounters>,
    _phantom: PhantomData<E>,
}

impl<E> MQTTTarget<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    /// Creates a new MQTTTarget
    #[instrument(skip(args), fields(target_id_as_string = %id))]
    pub fn new(id: String, args: MQTTArgs) -> Result<Self, TargetError> {
        args.validate()?;
        let target_id = TargetID::new(id, ChannelTargetType::Mqtt.as_str().to_string());
        let queue_store = open_target_queue_store(
            &args.queue_dir,
            args.queue_limit,
            args.target_type,
            ChannelTargetType::Mqtt.as_str(),
            &target_id,
            "Failed to open store for MQTT target",
        )?;

        let (cancel_tx, cancel_rx) = mpsc::channel(1);
        let bg_task_manager = Arc::new(BgTaskManager {
            init_cell: OnceCell::new(),
            cancel_tx,
            initial_cancel_rx: Mutex::new(Some(cancel_rx)),
        });

        info!(target_id = %target_id, "MQTT target created");
        Ok(MQTTTarget::<E> {
            id: target_id,
            args,
            client: Arc::new(Mutex::new(None)),
            store: queue_store,
            connected: Arc::new(AtomicBool::new(false)),
            bg_task_manager,
            delivery_counters: Arc::new(TargetDeliveryCounters::default()),
            _phantom: PhantomData,
        })
    }

    #[instrument(skip(self), fields(target_id = %self.id))]
    async fn init(&self) -> Result<(), TargetError> {
        if self.connected.load(Ordering::SeqCst) {
            debug!(target_id = %self.id, "Already connected.");
            return Ok(());
        }

        let bg_task_manager = Arc::clone(&self.bg_task_manager);
        let client_arc = Arc::clone(&self.client);
        let connected_arc = Arc::clone(&self.connected);
        let target_id_clone = self.id.clone();
        let args_clone = self.args.clone();

        let _ = bg_task_manager
            .init_cell
            .get_or_try_init(|| async {
                debug!(target_id = %target_id_clone, "Initializing MQTT background task.");
                let mqtt_options = build_mqtt_options(
                    format!("rustfs_notify_{}", uuid::Uuid::new_v4()),
                    &args_clone.broker,
                    Some(args_clone.username.as_str()),
                    Some(args_clone.password.as_str()),
                    &args_clone.tls,
                    args_clone.keep_alive,
                    Some(MAX_MQTT_PACKET_SIZE_BYTES),
                )?;

                let (new_client, eventloop) = AsyncClient::builder(mqtt_options).capacity(10).build();

                if let Err(e) = new_client.subscribe(&args_clone.topic, args_clone.qos).await {
                    error!(target_id = %target_id_clone, error = %e, "Failed to subscribe to MQTT topic during init");
                    return Err(TargetError::Network(format!("MQTT subscribe failed: {e}")));
                }

                let mut rx_guard = bg_task_manager.initial_cancel_rx.lock().await;
                let cancel_rx = rx_guard.take().ok_or_else(|| {
                    error!(target_id = %target_id_clone, "MQTT cancel receiver already taken for task.");
                    TargetError::Configuration("MQTT cancel receiver already taken for task".to_string())
                })?;
                drop(rx_guard);

                *client_arc.lock().await = Some(new_client.clone());

                info!(target_id = %target_id_clone, "Spawning MQTT event loop task.");
                let task_handle =
                    tokio::spawn(run_mqtt_event_loop(eventloop, connected_arc.clone(), target_id_clone.clone(), cancel_rx));
                Ok(task_handle)
            })
            .await
            .map_err(|e: TargetError| {
                error!(target_id = %self.id, error = %e, "Failed to initialize MQTT background task");
                e
            })?;
        debug!(target_id = %self.id, "MQTT background task initialized successfully.");

        match tokio::time::timeout(DEFAULT_CONNECTION_TIMEOUT, async {
            while !self.connected.load(Ordering::SeqCst) {
                if let Some(handle) = self.bg_task_manager.init_cell.get()
                    && handle.is_finished()
                    && !self.connected.load(Ordering::SeqCst)
                {
                    error!(target_id = %self.id, "MQTT background task exited prematurely before connection was established.");
                    return Err(TargetError::Network("MQTT background task exited prematurely".to_string()));
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            debug!(target_id = %self.id, "MQTT target connected successfully.");
            Ok(())
        })
        .await
        {
            Ok(Ok(_)) => {
                info!(target_id = %self.id, "MQTT target initialized and connected.");
                Ok(())
            }
            Ok(Err(e)) => Err(e),
            Err(_) => {
                error!(target_id = %self.id, "Timeout waiting for MQTT connection after task spawn.");
                Err(TargetError::Network("Timeout waiting for MQTT connection".to_string()))
            }
        }
    }

    fn build_queued_payload(&self, event: &EntityTarget<E>) -> Result<QueuedPayload, TargetError> {
        build_queued_payload_with_records(event, vec![event.clone()])
    }

    #[instrument(skip(self, body, meta), fields(target_id = %self.id))]
    async fn send_body(&self, body: Vec<u8>, meta: &QueuedPayloadMeta) -> Result<(), TargetError> {
        let client_guard = self.client.lock().await;
        let client = client_guard
            .as_ref()
            .ok_or_else(|| TargetError::Configuration("MQTT client not initialized".to_string()))?;

        debug!(
            target = %self.id,
            bucket = %meta.bucket_name,
            object = %meta.object_name,
            event = %meta.event_name,
            payload_len = body.len(),
            "Sending MQTT payload"
        );

        client
            .publish(&self.args.topic, self.args.qos, false, body)
            .await
            .map_err(|e| {
                if e.to_string().contains("Connection") || e.to_string().contains("Timeout") {
                    self.connected.store(false, Ordering::SeqCst);
                    warn!(target_id = %self.id, error = %e, "Publish failed due to connection issue, marking as not connected.");
                    TargetError::NotConnected
                } else {
                    TargetError::Request(format!("Failed to publish message: {e}"))
                }
            })?;

        debug!(target_id = %self.id, topic = %self.args.topic, "Event published to MQTT topic");
        self.delivery_counters.record_success();
        Ok(())
    }

    pub fn clone_target(&self) -> Box<dyn Target<E> + Send + Sync> {
        Box::new(MQTTTarget::<E> {
            id: self.id.clone(),
            args: self.args.clone(),
            client: self.client.clone(),
            store: self.store.as_ref().map(|s| s.boxed_clone()),
            connected: self.connected.clone(),
            bg_task_manager: self.bg_task_manager.clone(),
            delivery_counters: self.delivery_counters.clone(),
            _phantom: PhantomData,
        })
    }
}

async fn run_mqtt_event_loop(
    mut eventloop: EventLoop,
    connected_status: Arc<AtomicBool>,
    target_id: TargetID,
    mut cancel_rx: mpsc::Receiver<()>,
) {
    info!(target_id = %target_id, "MQTT event loop task started.");
    let mut initial_connection_established = false;

    loop {
        tokio::select! {
            biased;
            _ = cancel_rx.recv() => {
                info!(target_id = %target_id, "MQTT event loop task received cancellation signal. Shutting down.");
                break;
            }
            polled_event_result = async {
                if !initial_connection_established || !connected_status.load(Ordering::SeqCst) {
                    match tokio::time::timeout(EVENT_LOOP_POLL_TIMEOUT, eventloop.poll()).await {
                        Ok(result) => Some(result),
                        Err(_) => {
                            debug!(target_id = %target_id, "MQTT poll timed out (EVENT_LOOP_POLL_TIMEOUT) while not connected or status pending.");
                            connected_status.store(false, Ordering::SeqCst);
                            None
                        }
                    }
                } else {
                    Some(eventloop.poll().await)
                }
            } => {
                match polled_event_result {
                    Some(Ok(notification)) => {
                        trace!(target_id = %target_id, event = ?notification, "Received MQTT event");
                        match notification {
                            rumqttc::Event::Incoming(Incoming::ConnAck(_conn_ack)) => {
                                info!(target_id = %target_id, "MQTT connected (ConnAck).");
                                connected_status.store(true, Ordering::SeqCst);
                                initial_connection_established = true;
                            }
                            rumqttc::Event::Incoming(Incoming::Publish(publish)) => {
                                debug!(target_id = %target_id, topic = ?publish.topic, payload_len = publish.payload.len(), "Received message on subscribed topic.");
                            }
                            rumqttc::Event::Incoming(Incoming::Disconnect(_)) => {
                                info!(target_id = %target_id, "Received Disconnect packet from broker. MQTT connection lost.");
                                connected_status.store(false, Ordering::SeqCst);
                            }
                            rumqttc::Event::Incoming(Incoming::PingResp(_)) => {
                                trace!(target_id = %target_id, "Received PingResp from broker. Connection is alive.");
                            }
                            rumqttc::Event::Incoming(Incoming::SubAck(suback)) => {
                                trace!(target_id = %target_id, "Received SubAck for pkid: {}", suback.pkid);
                            }
                            rumqttc::Event::Incoming(Incoming::PubAck(puback)) => {
                                trace!(target_id = %target_id, "Received PubAck for pkid: {}", puback.pkid);
                            }
                            // Process other incoming packet types as needed (PubRec, PubRel, PubComp, UnsubAck)
                            rumqttc::Event::Outgoing(Outgoing::Disconnect) => {
                                info!(target_id = %target_id, "MQTT outgoing disconnect initiated by client.");
                                connected_status.store(false, Ordering::SeqCst);
                            }
                            rumqttc::Event::Outgoing(Outgoing::PingReq) => {
                                trace!(target_id = %target_id, "Client sent PingReq to broker.");
                            }
                            // Other Outgoing events (Subscribe, Unsubscribe, Publish) usually do not need to handle connection status here,
                            // Because they are actions initiated by the client.
                            _ => {
                                // Log other unspecified MQTT events that are not handled, which helps debug
                                trace!(target_id = %target_id, "Unhandled or generic MQTT event: {:?}", notification);
                            }
                        }
                    }
                    Some(Err(e)) => {
                        connected_status.store(false, Ordering::SeqCst);
                        error!(target_id = %target_id, error = %e, "Error from MQTT event loop poll");

                        if matches!(e,
                            ConnectionError::Io(_) |
                            ConnectionError::Timeout(_) |
                            ConnectionError::ConnectionRefused(_) |
                            ConnectionError::Tls(_)
                        ) {
                           warn!(target_id = %target_id, error = %e, "MQTT connection error. Relying on rumqttc for reconnection if applicable.");
                        }
                        // Here you can decide whether to break loops based on the error type.
                        // For example, for some unrecoverable errors.
                        if is_fatal_mqtt_error(&e) {
                            error!(target_id = %target_id, error = %e, "Fatal MQTT error, terminating event loop.");
                            break;
                        }
                        // rumqttc's eventloop.poll() may return Err and terminate after some errors,
                        // Or it will handle reconnection internally. To continue here will make select! wait again.
                        // If the error is temporary and rumqttc is handling reconnection, poll() should eventually succeed or return a different error again.
                        // Sleep briefly to avoid busy cycles in case of rapid failure.
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                    None => {
                        warn!(target_id = %target_id, "Timeout during initial poll or pending state, will retry.");
                        continue;
                    }
                }
            }
        }
    }
    connected_status.store(false, Ordering::SeqCst);
    info!(target_id = %target_id, "MQTT event loop task finished.");
}

/// Check whether the given MQTT connection error should be considered a fatal error,
/// For fatal errors, the event loop should terminate.
fn is_fatal_mqtt_error(err: &ConnectionError) -> bool {
    match err {
        // If the client request has been processed all (for example, AsyncClient is dropped), the event loop can end.
        ConnectionError::RequestsDone => true,

        // Check for the underlying MQTT status error
        ConnectionError::MqttState(state_err) => {
            // The type of state_err is &rumqttc::StateError
            match state_err {
                // If StateError is caused by deserialization issues, check the underlying MqttBytesError
                rumqttc::StateError::Deserialization(mqtt_bytes_err) => { // The type of mqtt_bytes_err is &rumqttc::mqttbytes::Error
                    matches!(
                        mqtt_bytes_err,
                        MqttBytesError::InvalidProtocol // Invalid agreement
                        | MqttBytesError::InvalidProtocolLevel(_) // Invalid protocol level
                        | MqttBytesError::IncorrectPacketFormat // Package format is incorrect
                        | MqttBytesError::InvalidPacketType(_) // Invalid package type
                        | MqttBytesError::MalformedPacket // Package format error
                        | MqttBytesError::PayloadTooLong // Too long load
                        | MqttBytesError::PayloadSizeLimitExceeded { .. } // Load size limit exceeded
                        | MqttBytesError::TopicNotUtf8 // Topic Non-UTF-8 (Serious Agreement Violation)
                    )
                }
                // Others that are fatal StateError variants
                rumqttc::StateError::InvalidState          // The internal state machine is in invalid state
                | rumqttc::StateError::WrongPacket         // Agreement Violation: Unexpected Data Packet Received
                | rumqttc::StateError::Unsolicited(_)      // Agreement Violation: Unsolicited ACK Received
                | rumqttc::StateError::CollisionTimeout    // Agreement Violation (if this stage occurs)
                | rumqttc::StateError::EmptySubscription   // Agreement violation (if this stage occurs)
                => true,

                // Other StateErrors (such as Io, AwaitPingResp, CollisionTimeout) are not considered deadly here.
                // They may be processed internally by rumqttc or upgraded to other ConnectionError types.
                _ => false,
            }
        }

        // Other types of ConnectionErrors (such as Io, Tls, NetworkTimeout, ConnectionRefused, NotConnAck, etc.)
        // It is usually considered temporary, or the reconnect logic inside rumqttc will be processed.
        _ => false,
    }
}

#[async_trait]
impl<E> Target<E> for MQTTTarget<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    fn id(&self) -> TargetID {
        self.id.clone()
    }

    #[instrument(skip(self), fields(target_id = %self.id))]
    async fn is_active(&self) -> Result<bool, TargetError> {
        debug!(target_id = %self.id, "Checking if MQTT target is active.");
        if self.client.lock().await.is_none() && !self.connected.load(Ordering::SeqCst) {
            // Check if the background task is running and has not panicked
            if let Some(handle) = self.bg_task_manager.init_cell.get()
                && handle.is_finished()
            {
                error!(target_id = %self.id, "MQTT background task has finished, possibly due to an error. Target is not active.");
                return Err(TargetError::Network("MQTT background task terminated".to_string()));
            }
            debug!(target_id = %self.id, "MQTT client not yet initialized or task not running/connected.");
            return Err(TargetError::Configuration(
                "MQTT client not available or not initialized/connected".to_string(),
            ));
        }

        if self.connected.load(Ordering::SeqCst) {
            debug!(target_id = %self.id, "MQTT target is active (connected flag is true).");
            Ok(true)
        } else {
            debug!(target_id = %self.id, "MQTT target is not connected (connected flag is false).");
            Err(TargetError::NotConnected)
        }
    }

    #[instrument(skip(self, event), fields(target_id = %self.id))]
    async fn save(&self, event: Arc<EntityTarget<E>>) -> Result<(), TargetError> {
        let queued = match self.build_queued_payload(&event) {
            Ok(queued) => queued,
            Err(err) => {
                self.delivery_counters.record_final_failure();
                return Err(err);
            }
        };

        if let Some(store) = &self.store {
            debug!(target_id = %self.id, "Event saved to store start");
            match persist_queued_payload_to_store(store.as_ref(), &queued) {
                Ok(_) => {
                    debug!(target_id = %self.id, "Event saved to store for MQTT target successfully.");
                    Ok(())
                }
                Err(e) => {
                    error!(target_id = %self.id, error = %e, "Failed to save event to store");
                    self.delivery_counters.record_final_failure();
                    Err(TargetError::Storage(format!("Failed to save event to store: {e}")))
                }
            }
        } else {
            if !self.is_enabled() {
                return Err(TargetError::Disabled);
            }

            if !self.connected.load(Ordering::SeqCst) {
                warn!(target_id = %self.id, "Attempting to send directly but not connected; trying to init.");
                // Call the struct's init method, not the trait's default
                match MQTTTarget::<E>::init(self).await {
                    Ok(_) => debug!(target_id = %self.id, "MQTT target initialized successfully."),
                    Err(e) => {
                        error!(target_id = %self.id, error = %e, "Failed to initialize MQTT target.");
                        self.delivery_counters.record_final_failure();
                        return Err(TargetError::NotConnected);
                    }
                }
                if !self.connected.load(Ordering::SeqCst) {
                    error!(target_id = %self.id, "Cannot save (send directly) as target is not active after init attempt.");
                    self.delivery_counters.record_final_failure();
                    return Err(TargetError::NotConnected);
                }
            }
            if let Err(err) = self.send_body(queued.body, &queued.meta).await {
                self.delivery_counters.record_final_failure();
                return Err(err);
            }
            Ok(())
        }
    }

    #[instrument(skip(self, body, meta), fields(target_id = %self.id))]
    async fn send_raw_from_store(&self, key: Key, body: Vec<u8>, meta: QueuedPayloadMeta) -> Result<(), TargetError> {
        debug!(target_id = %self.id, ?key, "Attempting to send queued payload from store.");

        if !self.is_enabled() {
            return Err(TargetError::Disabled);
        }

        if !self.connected.load(Ordering::SeqCst) {
            warn!(target_id = %self.id, "Not connected; trying to init before sending from store.");
            match MQTTTarget::<E>::init(self).await {
                Ok(_) => debug!(target_id = %self.id, "MQTT target initialized successfully."),
                Err(e) => {
                    error!(target_id = %self.id, error = %e, "Failed to initialize MQTT target.");
                    return Err(TargetError::NotConnected);
                }
            }
            if !self.connected.load(Ordering::SeqCst) {
                error!(target_id = %self.id, "Cannot send from store as target is not active after init attempt.");
                return Err(TargetError::NotConnected);
            }
        }

        debug!(target_id = %self.id, ?key, "Sending event from store.");
        if let Err(e) = self.send_body(body, &meta).await {
            if matches!(e, TargetError::NotConnected) {
                warn!(target_id = %self.id, "Failed to send event from store: Not connected. Event remains in store.");
                return Err(TargetError::NotConnected);
            }
            error!(target_id = %self.id, error = %e, "Failed to send event from store with an unexpected error.");
            return Err(e);
        }
        debug!(target_id = %self.id, ?key, "Event sent from store successfully.");
        Ok(())
    }

    async fn close(&self) -> Result<(), TargetError> {
        info!(target_id = %self.id, "Attempting to close MQTT target.");

        if let Err(e) = self.bg_task_manager.cancel_tx.send(()).await {
            warn!(target_id = %self.id, error = %e, "Failed to send cancel signal to MQTT background task. It might have already exited.");
        }

        // Wait for the task to finish if it was initialized
        if let Some(_task_handle) = self.bg_task_manager.init_cell.get() {
            debug!(target_id = %self.id, "Waiting for MQTT background task to complete...");
            // It's tricky to await here if close is called from a sync context or Drop
            // For async close, this is fine. Consider a timeout.
            // let _ = tokio::time::timeout(Duration::from_secs(5), task_handle.await).await;
            // If task_handle.await is directly used, ensure it's not awaited multiple times if close can be called multiple times.
            // For now, we rely on the signal and the task's self-termination.
        }

        if let Some(client_instance) = self.client.lock().await.take() {
            info!(target_id = %self.id, "Disconnecting MQTT client.");
            if let Err(e) = client_instance.disconnect().await {
                warn!(target_id = %self.id, error = %e, "Error during MQTT client disconnect.");
            }
        }

        self.connected.store(false, Ordering::SeqCst);
        info!(target_id = %self.id, "MQTT target close method finished.");
        Ok(())
    }

    fn store(&self) -> Option<&(dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync)> {
        self.store.as_deref()
    }

    fn clone_dyn(&self) -> Box<dyn Target<E> + Send + Sync> {
        self.clone_target()
    }

    async fn init(&self) -> Result<(), TargetError> {
        if !self.is_enabled() {
            debug!(target_id = %self.id, "Target is disabled, skipping init.");
            return Ok(());
        }
        // Call the internal init logic
        MQTTTarget::<E>::init(self).await
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

#[cfg(test)]
mod tests {
    use super::{MQTTTlsConfig, validate_mqtt_broker_url};
    use url::Url;

    #[test]
    fn validate_mqtt_broker_url_rejects_non_websocket_path() {
        let url = Url::parse("mqtt://broker.example.com:1883/custom").expect("valid url");
        let err = validate_mqtt_broker_url(&url, &MQTTTlsConfig::default()).expect_err("non-websocket path should be rejected");
        assert!(err.to_string().contains("path is only supported"));
    }

    #[test]
    fn validate_mqtt_broker_url_rejects_non_websocket_query() {
        let url = Url::parse("mqtt://broker.example.com:1883?client_id=test").expect("valid url");
        let err = validate_mqtt_broker_url(&url, &MQTTTlsConfig::default()).expect_err("non-websocket query should be rejected");
        assert!(err.to_string().contains("query is only supported"));
    }

    #[test]
    fn validate_mqtt_broker_url_rejects_non_websocket_fragment() {
        let url = Url::parse("mqtt://broker.example.com:1883/#section").expect("valid url");
        let err =
            validate_mqtt_broker_url(&url, &MQTTTlsConfig::default()).expect_err("non-websocket fragment should be rejected");
        assert!(err.to_string().contains("fragment is only supported"));
    }

    #[test]
    fn validate_mqtt_broker_url_allows_websocket_path_and_query() {
        let url = Url::parse("ws://broker.example.com:8080/mqtt?client_id=test").expect("valid url");
        validate_mqtt_broker_url(&url, &MQTTTlsConfig::default()).expect("websocket path and query should be allowed");
    }

    #[test]
    fn validate_mqtt_broker_url_rejects_url_embedded_credentials() {
        let url = Url::parse("mqtt://user:pass@broker.example.com:1883").expect("valid url");
        let err = validate_mqtt_broker_url(&url, &MQTTTlsConfig::default()).expect_err("url credentials should be rejected");
        assert!(err.to_string().contains("must not embed username or password"));
    }

    #[test]
    fn validate_mqtt_broker_url_requires_explicit_tls_policy_for_secure_scheme() {
        let url = Url::parse("mqtts://broker.example.com:8883").expect("valid url");
        let err = validate_mqtt_broker_url(&url, &MQTTTlsConfig::default())
            .expect_err("secure scheme should require explicit tls policy");
        assert!(err.to_string().contains("explicit tls_policy"));
    }

    #[test]
    fn validate_mqtt_broker_url_rejects_disallowed_websocket_path() {
        let url = Url::parse("wss://broker.example.com/private").expect("valid url");
        let tls = MQTTTlsConfig::from_values(Some("system_ca"), None, None, None, None, Some("/mqtt")).expect("valid tls config");
        let err = validate_mqtt_broker_url(&url, &tls).expect_err("path outside allowlist should be rejected");
        assert!(err.to_string().contains("allowlist"));
    }

    #[test]
    fn validate_mqtt_broker_url_requires_tls_ca_for_custom_ca_policy() {
        let url = Url::parse("mqtts://broker.example.com:8883").expect("valid url");
        let tls = MQTTTlsConfig::from_values(Some("custom_ca"), None, None, None, None, None).expect("valid tls config");
        let err = validate_mqtt_broker_url(&url, &tls).expect_err("custom_ca policy without path should be rejected");
        assert!(err.to_string().contains("tls_ca"));
    }
}
