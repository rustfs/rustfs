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

use crate::store::Key;
use crate::target::{ChannelTargetType, EntityTarget, TargetType};
use crate::{StoreError, Target, TargetLog, arn::TargetID, error::TargetError, store::Store};
use async_trait::async_trait;
use rumqttc::{AsyncClient, EventLoop, MqttOptions, Outgoing, Packet, QoS};
use rumqttc::{ConnectionError, mqttbytes::Error as MqttBytesError};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::sync::Arc;
use std::{
    path::PathBuf,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};
use tokio::sync::{Mutex, OnceCell, mpsc};
use tracing::{debug, error, info, instrument, trace, warn};
use url::Url;
use urlencoding;

const DEFAULT_CONNECTION_TIMEOUT: Duration = Duration::from_secs(15);
const EVENT_LOOP_POLL_TIMEOUT: Duration = Duration::from_secs(10); // For initial connection check in task

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

        match self.broker.scheme() {
            "ws" | "wss" | "tcp" | "ssl" | "tls" | "tcps" | "mqtt" | "mqtts" => {}
            _ => {
                return Err(TargetError::Configuration("unknown protocol in broker address".to_string()));
            }
        }

        if self.topic.is_empty() {
            return Err(TargetError::Configuration("MQTT topic cannot be empty".to_string()));
        }

        if !self.queue_dir.is_empty() {
            let path = std::path::Path::new(&self.queue_dir);
            if !path.is_absolute() {
                return Err(TargetError::Configuration("mqtt queueDir path should be absolute".to_string()));
            }

            if self.qos == QoS::AtMostOnce {
                return Err(TargetError::Configuration(
                    "QoS should be AtLeastOnce (1) or ExactlyOnce (2) if queueDir is set".to_string(),
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
    store: Option<Box<dyn Store<EntityTarget<E>, Error = StoreError, Key = Key> + Send + Sync>>,
    connected: Arc<AtomicBool>,
    bg_task_manager: Arc<BgTaskManager>,
}

impl<E> MQTTTarget<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    /// Creates a new MQTTTarget
    #[instrument(skip(args), fields(target_id_as_string = %id))]
    pub fn new(id: String, args: MQTTArgs) -> Result<Self, TargetError> {
        args.validate()?;
        let target_id = TargetID::new(id.clone(), ChannelTargetType::Mqtt.as_str().to_string());
        let queue_store = if !args.queue_dir.is_empty() {
            let base_path = PathBuf::from(&args.queue_dir);
            let unique_dir_name = format!("rustfs-{}-{}", ChannelTargetType::Mqtt.as_str(), target_id.id).replace(":", "_");
            // Ensure the directory name is valid for filesystem
            let specific_queue_path = base_path.join(unique_dir_name);
            debug!(target_id = %target_id, path = %specific_queue_path.display(), "Initializing queue store for MQTT target");
            let extension = match args.target_type {
                TargetType::AuditLog => rustfs_config::audit::AUDIT_STORE_EXTENSION,
                TargetType::NotifyEvent => rustfs_config::notify::STORE_EXTENSION,
            };

            let store = crate::store::QueueStore::<EntityTarget<E>>::new(specific_queue_path, args.queue_limit, extension);
            if let Err(e) = store.open() {
                error!(
                    target_id = %target_id,
                    error = %e,
                    "Failed to open store for MQTT target"
                );
                return Err(TargetError::Storage(format!("{e}")));
            }
            Some(Box::new(store) as Box<dyn Store<EntityTarget<E>, Error = StoreError, Key = Key> + Send + Sync>)
        } else {
            None
        };

        let (cancel_tx, cancel_rx) = mpsc::channel(1);
        let bg_task_manager = Arc::new(BgTaskManager {
            init_cell: OnceCell::new(),
            cancel_tx,
            initial_cancel_rx: Mutex::new(Some(cancel_rx)),
        });

        info!(target_id = %target_id, "MQTT target created");
        Ok(MQTTTarget {
            id: target_id,
            args,
            client: Arc::new(Mutex::new(None)),
            store: queue_store,
            connected: Arc::new(AtomicBool::new(false)),
            bg_task_manager,
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
                let host = args_clone.broker.host_str().unwrap_or("localhost");
                let port = args_clone.broker.port().unwrap_or(1883);
                let mut mqtt_options = MqttOptions::new(format!("rustfs_notify_{}", uuid::Uuid::new_v4()), host, port);
                mqtt_options
                    .set_keep_alive(args_clone.keep_alive)
                    .set_max_packet_size(100 * 1024 * 1024, 100 * 1024 * 1024); // 100MB

                if !args_clone.username.is_empty() {
                    mqtt_options.set_credentials(args_clone.username.clone(), args_clone.password.clone());
                }

                let (new_client, eventloop) = AsyncClient::new(mqtt_options, 10);

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
                if let Some(handle) = self.bg_task_manager.init_cell.get() {
                    if handle.is_finished() && !self.connected.load(Ordering::SeqCst) {
                        error!(target_id = %self.id, "MQTT background task exited prematurely before connection was established.");
                        return Err(TargetError::Network("MQTT background task exited prematurely".to_string()));
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            debug!(target_id = %self.id, "MQTT target connected successfully.");
            Ok(())
        }).await {
            Ok(Ok(_)) => {
                info!(target_id = %self.id, "MQTT target initialized and connected.");
                Ok(())
            }
            Ok(Err(e)) => Err(e),
            Err(_) => {
                error!(target_id = %self.id, "Timeout waiting for MQTT connection after task spawn.");
                Err(TargetError::Network(
                    "Timeout waiting for MQTT connection".to_string(),
                ))
            }
        }
    }

    #[instrument(skip(self, event), fields(target_id = %self.id))]
    async fn send(&self, event: &EntityTarget<E>) -> Result<(), TargetError> {
        let client_guard = self.client.lock().await;
        let client = client_guard
            .as_ref()
            .ok_or_else(|| TargetError::Configuration("MQTT client not initialized".to_string()))?;

        let object_name = urlencoding::decode(&event.object_name)
            .map_err(|e| TargetError::Encoding(format!("Failed to decode object key: {e}")))?;

        let key = format!("{}/{}", event.bucket_name, object_name);

        let log = TargetLog {
            event_name: event.event_name,
            key,
            records: vec![event.clone()],
        };

        let data = serde_json::to_vec(&log).map_err(|e| TargetError::Serialization(format!("Failed to serialize event: {e}")))?;

        let data_string = String::from_utf8(data.clone())
            .map_err(|e| TargetError::Encoding(format!("Failed to convert event data to UTF-8: {e}")))?;
        debug!("Sending event to mqtt target: {}, event log: {}", self.id, data_string);

        client
            .publish(&self.args.topic, self.args.qos, false, data)
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
        Ok(())
    }

    pub fn clone_target(&self) -> Box<dyn Target<E> + Send + Sync> {
        Box::new(MQTTTarget {
            id: self.id.clone(),
            args: self.args.clone(),
            client: self.client.clone(),
            store: self.store.as_ref().map(|s| s.boxed_clone()),
            connected: self.connected.clone(),
            bg_task_manager: self.bg_task_manager.clone(),
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
                        Ok(Ok(event)) => Ok(event),
                        Ok(Err(e)) => Err(e),
                        Err(_) => {
                            debug!(target_id = %target_id, "MQTT poll timed out (EVENT_LOOP_POLL_TIMEOUT) while not connected or status pending.");
                            Err(rumqttc::ConnectionError::NetworkTimeout)
                        }
                    }
                } else {
                    eventloop.poll().await
                }
            } => {
                match polled_event_result {
                    Ok(notification) => {
                        trace!(target_id = %target_id, event = ?notification, "Received MQTT event");
                        match notification {
                            rumqttc::Event::Incoming(Packet::ConnAck(_conn_ack)) => {
                                info!(target_id = %target_id, "MQTT connected (ConnAck).");
                                connected_status.store(true, Ordering::SeqCst);
                                initial_connection_established = true;
                            }
                            rumqttc::Event::Incoming(Packet::Publish(publish)) => {
                                debug!(target_id = %target_id, topic = %publish.topic, payload_len = publish.payload.len(), "Received message on subscribed topic.");
                            }
                            rumqttc::Event::Incoming(Packet::Disconnect) => {
                                info!(target_id = %target_id, "Received Disconnect packet from broker. MQTT connection lost.");
                                connected_status.store(false, Ordering::SeqCst);
                            }
                            rumqttc::Event::Incoming(Packet::PingResp) => {
                                trace!(target_id = %target_id, "Received PingResp from broker. Connection is alive.");
                            }
                            rumqttc::Event::Incoming(Packet::SubAck(suback)) => {
                                trace!(target_id = %target_id, "Received SubAck for pkid: {}", suback.pkid);
                            }
                            rumqttc::Event::Incoming(Packet::PubAck(puback)) => {
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
                    Err(e) => {
                        connected_status.store(false, Ordering::SeqCst);
                        error!(target_id = %target_id, error = %e, "Error from MQTT event loop poll");

                        if matches!(e, rumqttc::ConnectionError::NetworkTimeout) && (!initial_connection_established || !connected_status.load(Ordering::SeqCst)) {
                           warn!(target_id = %target_id, "Timeout during initial poll or pending state, will retry.");
                           continue;
                        }

                        if matches!(e,
                            ConnectionError::Io(_) |
                            ConnectionError::NetworkTimeout |
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
                        // Or it will handle reconnection internally. The continue here will make select! wait again.
                        // If the error is temporary and rumqttc is handling reconnection, poll() should eventually succeed or return a different error again.
                        // Sleep briefly to avoid busy cycles in case of rapid failure.
                        tokio::time::sleep(Duration::from_secs(1)).await;
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
                        | MqttBytesError::PayloadSizeLimitExceeded(_) // Load size limit exceeded
                        | MqttBytesError::TopicNotUtf8 // Topic Non-UTF-8 (Serious Agreement Violation)
                    )
                }
                // Others that are fatal StateError variants
                rumqttc::StateError::InvalidState          // The internal state machine is in invalid state
                | rumqttc::StateError::WrongPacket         // Agreement Violation: Unexpected Data Packet Received
                | rumqttc::StateError::Unsolicited(_)      // Agreement Violation: Unsolicited ACK Received
                | rumqttc::StateError::OutgoingPacketTooLarge { .. } // Try to send too large packets
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
            if let Some(handle) = self.bg_task_manager.init_cell.get() {
                if handle.is_finished() {
                    error!(target_id = %self.id, "MQTT background task has finished, possibly due to an error. Target is not active.");
                    return Err(TargetError::Network("MQTT background task terminated".to_string()));
                }
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
        if let Some(store) = &self.store {
            debug!(target_id = %self.id, "Event saved to store start");
            // If store is configured, ONLY put the event into the store.
            // Do NOT send it directly here.
            match store.put(event.clone()) {
                Ok(_) => {
                    debug!(target_id = %self.id, "Event saved to store for MQTT target successfully.");
                    Ok(())
                }
                Err(e) => {
                    error!(target_id = %self.id, error = %e, "Failed to save event to store");
                    return Err(TargetError::Storage(format!("Failed to save event to store: {e}")));
                }
            }
        } else {
            if !self.is_enabled() {
                return Err(TargetError::Disabled);
            }

            if !self.connected.load(Ordering::SeqCst) {
                warn!(target_id = %self.id, "Attempting to send directly but not connected; trying to init.");
                // Call the struct's init method, not the trait's default
                match MQTTTarget::init(self).await {
                    Ok(_) => debug!(target_id = %self.id, "MQTT target initialized successfully."),
                    Err(e) => {
                        error!(target_id = %self.id, error = %e, "Failed to initialize MQTT target.");
                        return Err(TargetError::NotConnected);
                    }
                }
                if !self.connected.load(Ordering::SeqCst) {
                    error!(target_id = %self.id, "Cannot save (send directly) as target is not active after init attempt.");
                    return Err(TargetError::NotConnected);
                }
            }
            self.send(&event).await
        }
    }

    #[instrument(skip(self), fields(target_id = %self.id))]
    async fn send_from_store(&self, key: Key) -> Result<(), TargetError> {
        debug!(target_id = %self.id, ?key, "Attempting to send event from store with key.");

        if !self.is_enabled() {
            return Err(TargetError::Disabled);
        }

        if !self.connected.load(Ordering::SeqCst) {
            warn!(target_id = %self.id, "Not connected; trying to init before sending from store.");
            match MQTTTarget::init(self).await {
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

        let store = self
            .store
            .as_ref()
            .ok_or_else(|| TargetError::Configuration("No store configured".to_string()))?;

        let event = match store.get(&key) {
            Ok(event) => {
                debug!(target_id = %self.id, ?key, "Retrieved event from store for sending.");
                event
            }
            Err(StoreError::NotFound) => {
                // Assuming NotFound takes the key
                debug!(target_id = %self.id, ?key, "Event not found in store for sending.");
                return Ok(());
            }
            Err(e) => {
                error!(
                    target_id = %self.id,
                    error = %e,
                    "Failed to get event from store"
                );
                return Err(TargetError::Storage(format!("Failed to get event from store: {e}")));
            }
        };

        debug!(target_id = %self.id, ?key, "Sending event from store.");
        if let Err(e) = self.send(&event).await {
            if matches!(e, TargetError::NotConnected) {
                warn!(target_id = %self.id, "Failed to send event from store: Not connected. Event remains in store.");
                return Err(TargetError::NotConnected);
            }
            error!(target_id = %self.id, error = %e, "Failed to send event from store with an unexpected error.");
            return Err(e);
        }
        debug!(target_id = %self.id, ?key, "Event sent from store successfully. deleting from store. ");

        match store.del(&key) {
            Ok(_) => {
                debug!(target_id = %self.id, ?key, "Event deleted from store after successful send.")
            }
            Err(StoreError::NotFound) => {
                debug!(target_id = %self.id, ?key, "Event already deleted from store.");
            }
            Err(e) => {
                error!(target_id = %self.id, error = %e, "Failed to delete event from store after send.");
                return Err(TargetError::Storage(format!("Failed to delete event from store: {e}")));
            }
        }

        debug!(target_id = %self.id, ?key, "Event deleted from store.");
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

    fn store(&self) -> Option<&(dyn Store<EntityTarget<E>, Error = StoreError, Key = Key> + Send + Sync)> {
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
        MQTTTarget::init(self).await
    }

    fn is_enabled(&self) -> bool {
        self.args.enable
    }
}
