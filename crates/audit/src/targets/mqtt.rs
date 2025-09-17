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

//! MQTT audit target implementation

use crate::entity::AuditEntry;
use crate::error::{TargetError, TargetResult};
use crate::registry::{AuditTarget, AuditTargetConfig, TargetStatus};
use async_trait::async_trait;
use parking_lot::RwLock;
use rumqttc::{AsyncClient, MqttOptions, QoS};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

/// MQTT audit target implementation with enhanced reliability and monitoring
pub struct MqttAuditTarget {
    id: String,
    topic: String,
    qos: QoS,
    client: AsyncClient,
    enabled: Arc<RwLock<bool>>,
    running: Arc<RwLock<bool>>,
    // Enhanced statistics and error tracking
    success_count: AtomicU64,
    error_count: AtomicU64,
    last_error: Arc<RwLock<Option<String>>>,
    last_error_time: Arc<RwLock<Option<chrono::DateTime<chrono::Utc>>>>,
    last_success_time: Arc<RwLock<Option<chrono::DateTime<chrono::Utc>>>>,
    // Connection state management
    connection_state: Arc<RwLock<ConnectionState>>,
    shutdown_tx: broadcast::Sender<()>,
    queue_size: Arc<AtomicU64>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ConnectionState {
    Disconnected,
    Connecting,
    Connected,
    Error(String),
}

impl MqttAuditTarget {
    /// Create from configuration with enhanced connection management
    pub async fn from_config(config: &AuditTargetConfig) -> TargetResult<Self> {
        Self::validate_config(config)?;

        let args = &config.args;
        let broker_url = args
            .get("broker_url")
            .and_then(|v| v.as_str())
            .ok_or_else(|| TargetError::InvalidConfig {
                message: "Missing required 'broker_url' field".to_string(),
            })?;

        let topic = args
            .get("topic")
            .and_then(|v| v.as_str())
            .ok_or_else(|| TargetError::InvalidConfig {
                message: "Missing required 'topic' field".to_string(),
            })?
            .to_string();

        // Parse broker URL
        let url = url::Url::parse(broker_url).map_err(|e| TargetError::InvalidConfig {
            message: format!("Invalid broker URL: {}", e),
        })?;

        let host = url.host_str().ok_or_else(|| TargetError::InvalidConfig {
            message: "Missing host in broker URL".to_string(),
        })?;

        let port = match url.scheme() {
            "mqtt" => url.port().unwrap_or(1883),
            "mqtts" => url.port().unwrap_or(8883),
            scheme => {
                return Err(TargetError::InvalidConfig {
                    message: format!("Unsupported MQTT scheme '{}', use mqtt:// or mqtts://", scheme),
                });
            }
        };

        // Enhanced MQTT options with better reliability settings
        let mut mqttoptions = MqttOptions::new(&config.id, host, port);
        mqttoptions.set_keep_alive(Duration::from_secs(30));
        mqttoptions.set_clean_session(true);

        // Set max packet size to handle large audit entries
        mqttoptions.set_max_packet_size(1024 * 1024, 1024 * 1024); // 1MB

        // Configure TLS for mqtts
        if url.scheme() == "mqtts" {
            // For now, skip TLS configuration - would need proper setup in production
            warn!("MQTTS requested but TLS configuration not implemented, using plain MQTT");
        }

        // Set credentials if provided
        if let (Some(username), Some(password)) = (
            args.get("username").and_then(|v| v.as_str()),
            args.get("password").and_then(|v| v.as_str()),
        ) {
            mqttoptions.set_credentials(username, password);
        }

        // Enhanced auth support
        if let Some(auth) = args.get("auth").and_then(|v| v.as_object()) {
            if let (Some(username), Some(password)) = (
                auth.get("username").and_then(|v| v.as_str()),
                auth.get("password").and_then(|v| v.as_str()),
            ) {
                mqttoptions.set_credentials(username, password);
            }
        }

        // Configure QoS
        let qos_level = args.get("qos").and_then(|v| v.as_u64()).unwrap_or(1);
        let qos = match qos_level {
            0 => QoS::AtMostOnce,
            1 => QoS::AtLeastOnce,
            2 => QoS::ExactlyOnce,
            _ => {
                return Err(TargetError::InvalidConfig {
                    message: format!("Invalid QoS level {}, must be 0, 1, or 2", qos_level),
                });
            }
        };

        // Create client with enhanced buffer size
        let (client, mut eventloop) = AsyncClient::new(mqttoptions, 100);

        let connection_state = Arc::new(RwLock::new(ConnectionState::Disconnected));
        let (shutdown_tx, mut shutdown_rx) = broadcast::channel(1);

        // Start enhanced event loop with better error handling and reconnection
        let state_clone = connection_state.clone();
        let target_id = config.id.clone();
        let mut reconnect_attempts = 0u32;
        const MAX_RECONNECT_ATTEMPTS: u32 = 10;

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        info!("MQTT target {} event loop shutting down", target_id);
                        break;
                    }
                    result = eventloop.poll() => {
                        match result {
                            Ok(notification) => {
                                match notification {
                                    rumqttc::Event::Incoming(rumqttc::Packet::ConnAck(_)) => {
                                        *state_clone.write() = ConnectionState::Connected;
                                        reconnect_attempts = 0;
                                        info!("MQTT target {} connected", target_id);
                                    }
                                    rumqttc::Event::Incoming(rumqttc::Packet::Disconnect) => {
                                        *state_clone.write() = ConnectionState::Disconnected;
                                        warn!("MQTT target {} disconnected", target_id);
                                    }
                                    _ => {
                                        // Handle other notifications if needed
                                        debug!("MQTT target {} received notification: {:?}", target_id, notification);
                                    }
                                }
                            }
                            Err(e) => {
                                let error_msg = format!("MQTT connection error: {}", e);
                                *state_clone.write() = ConnectionState::Error(error_msg.clone());

                                reconnect_attempts += 1;
                                if reconnect_attempts <= MAX_RECONNECT_ATTEMPTS {
                                    let backoff_secs = (2_u64.pow(reconnect_attempts.min(6))).min(60);
                                    warn!(
                                        "MQTT target {} connection failed (attempt {}), retrying in {}s: {}",
                                        target_id, reconnect_attempts, backoff_secs, e
                                    );
                                    tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                                } else {
                                    error!(
                                        "MQTT target {} failed after {} attempts, giving up: {}",
                                        target_id, MAX_RECONNECT_ATTEMPTS, e
                                    );
                                    tokio::time::sleep(Duration::from_secs(60)).await; // Wait before trying again
                                    reconnect_attempts = 0; // Reset for next cycle
                                }
                            }
                        }
                    }
                }
            }
        });

        Ok(Self {
            id: config.id.clone(),
            topic,
            qos,
            client,
            enabled: Arc::new(RwLock::new(config.enabled)),
            running: Arc::new(RwLock::new(false)),
            success_count: AtomicU64::new(0),
            error_count: AtomicU64::new(0),
            last_error: Arc::new(RwLock::new(None)),
            last_error_time: Arc::new(RwLock::new(None)),
            last_success_time: Arc::new(RwLock::new(None)),
            connection_state,
            shutdown_tx,
            queue_size: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Enhanced configuration validation
    pub fn validate_config(config: &AuditTargetConfig) -> TargetResult<()> {
        let args = &config.args;

        // Check required fields
        let broker_url = args
            .get("broker_url")
            .and_then(|v| v.as_str())
            .ok_or_else(|| TargetError::InvalidConfig {
                message: "Missing required 'broker_url' field".to_string(),
            })?;

        args.get("topic")
            .and_then(|v| v.as_str())
            .ok_or_else(|| TargetError::InvalidConfig {
                message: "Missing required 'topic' field".to_string(),
            })?;

        // Validate broker URL and scheme
        let url = url::Url::parse(broker_url).map_err(|e| TargetError::InvalidConfig {
            message: format!("Invalid broker URL '{}': {}", broker_url, e),
        })?;

        match url.scheme() {
            "mqtt" | "mqtts" => {}
            scheme => {
                return Err(TargetError::InvalidConfig {
                    message: format!("Unsupported MQTT scheme '{}', use mqtt:// or mqtts://", scheme),
                });
            }
        }

        // Validate QoS level if specified
        if let Some(qos) = args.get("qos").and_then(|v| v.as_u64()) {
            if qos > 2 {
                return Err(TargetError::InvalidConfig {
                    message: format!("Invalid QoS level {}, must be 0, 1, or 2", qos),
                });
            }
        }

        // Validate timeout if specified
        if let Some(timeout_ms) = args.get("timeout_ms").and_then(|v| v.as_u64()) {
            if timeout_ms < 100 || timeout_ms > 60000 {
                return Err(TargetError::InvalidConfig {
                    message: format!("timeout_ms must be between 100 and 60000, got {}", timeout_ms),
                });
            }
        }

        // Validate retries if specified
        if let Some(retries) = args.get("retries").and_then(|v| v.as_u64()) {
            if retries > 10 {
                return Err(TargetError::InvalidConfig {
                    message: format!("retries must be <= 10, got {}", retries),
                });
            }
        }

        // Validate target_type if specified in args (per specification)
        if let Some(target_type) = args.get("target_type").and_then(|v| v.as_str()) {
            if target_type != "AuditLog" {
                return Err(TargetError::InvalidConfig {
                    message: format!("target_type must be 'AuditLog', got '{}'", target_type),
                });
            }
        }

        Ok(())
    }

    /// Get current connection state
    pub fn get_connection_state(&self) -> ConnectionState {
        self.connection_state.read().clone()
    }

    /// Check if currently connected
    pub fn is_connected(&self) -> bool {
        match *self.connection_state.read() {
            ConnectionState::Connected => true,
            _ => false,
        }
    }

    /// Record error with enhanced tracking
    fn record_error(&self, error_msg: &str) {
        self.error_count.fetch_add(1, Ordering::Relaxed);
        *self.last_error.write() = Some(error_msg.to_string());
        *self.last_error_time.write() = Some(chrono::Utc::now());
        error!("MQTT target {} error: {}", self.id, error_msg);
    }

    /// Send MQTT message with enhanced error handling
    async fn send_mqtt_message(&self, payload: String) -> TargetResult<()> {
        // Check connection state
        if !self.is_connected() {
            return Err(TargetError::Connection {
                message: format!("MQTT target {} not connected", self.id),
            });
        }

        self.queue_size.fetch_add(1, Ordering::Relaxed);

        match self.client.publish(&self.topic, self.qos, false, payload).await {
            Ok(()) => {
                self.success_count.fetch_add(1, Ordering::Relaxed);
                *self.last_success_time.write() = Some(chrono::Utc::now());
                self.queue_size.fetch_sub(1, Ordering::Relaxed);
                debug!("MQTT target {} message sent successfully", self.id);
                Ok(())
            }
            Err(e) => {
                self.queue_size.fetch_sub(1, Ordering::Relaxed);
                let error_msg = format!("Failed to publish to MQTT: {}", e);
                self.record_error(&error_msg);
                Err(TargetError::from(e))
            }
        }
    }

    /// Get current queue size
    pub fn get_queue_size(&self) -> u64 {
        self.queue_size.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl AuditTarget for MqttAuditTarget {
    async fn send(&self, entry: Arc<AuditEntry>) -> TargetResult<()> {
        if !*self.enabled.read() {
            return Err(TargetError::Disabled);
        }

        if !*self.running.read() {
            return Err(TargetError::ShuttingDown);
        }

        let json = entry.to_json().map_err(|e| TargetError::Serialization {
            message: format!("Failed to serialize audit entry: {}", e),
        })?;

        self.send_mqtt_message(json).await
    }

    async fn start(&self) -> TargetResult<()> {
        *self.running.write() = true;
        debug!("MQTT target {} started", self.id);
        Ok(())
    }

    async fn pause(&self) -> TargetResult<()> {
        *self.running.write() = false;
        debug!("MQTT target {} paused", self.id);
        Ok(())
    }

    async fn resume(&self) -> TargetResult<()> {
        if *self.enabled.read() {
            *self.running.write() = true;
            debug!("MQTT target {} resumed", self.id);
        }
        Ok(())
    }

    async fn close(&self) -> TargetResult<()> {
        *self.running.write() = false;
        *self.enabled.write() = false;

        // Signal shutdown to event loop
        let _ = self.shutdown_tx.send(());

        debug!("MQTT target {} closed", self.id);
        Ok(())
    }

    fn id(&self) -> &str {
        &self.id
    }

    fn target_type(&self) -> &str {
        "mqtt"
    }

    fn is_enabled(&self) -> bool {
        *self.enabled.read()
    }

    fn status(&self) -> TargetStatus {
        let connection_state = self.get_connection_state();
        let is_connected = matches!(connection_state, ConnectionState::Connected);

        TargetStatus {
            id: self.id.clone(),
            target_type: "mqtt".to_string(),
            enabled: *self.enabled.read(),
            running: *self.running.read() && is_connected,
            last_error: match connection_state {
                ConnectionState::Error(ref msg) => Some(msg.clone()),
                _ => self.last_error.read().clone(),
            },
            last_error_time: *self.last_error_time.read(),
            success_count: self.success_count.load(Ordering::Relaxed),
            error_count: self.error_count.load(Ordering::Relaxed),
            last_success_time: *self.last_success_time.read(),
            queue_size: Some(self.get_queue_size() as usize),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_mqtt_config_validation() {
        // Valid config
        let config = AuditTargetConfig {
            id: "mqtt-test".to_string(),
            target_type: "mqtt".to_string(),
            enabled: true,
            args: json!({
                "broker_url": "mqtt://localhost:1883",
                "topic": "test/audit",
                "qos": 1
            }),
        };
        assert!(MqttAuditTarget::validate_config(&config).is_ok());

        // Valid MQTTS config
        let config = AuditTargetConfig {
            id: "mqtt-tls-test".to_string(),
            target_type: "mqtt".to_string(),
            enabled: true,
            args: json!({
                "broker_url": "mqtts://secure.example.com:8883",
                "topic": "secure/audit",
                "qos": 2
            }),
        };
        assert!(MqttAuditTarget::validate_config(&config).is_ok());

        // Missing broker_url
        let config = AuditTargetConfig {
            id: "mqtt-test".to_string(),
            target_type: "mqtt".to_string(),
            enabled: true,
            args: json!({
                "topic": "test/audit"
            }),
        };
        assert!(MqttAuditTarget::validate_config(&config).is_err());

        // Missing topic
        let config = AuditTargetConfig {
            id: "mqtt-test".to_string(),
            target_type: "mqtt".to_string(),
            enabled: true,
            args: json!({
                "broker_url": "mqtt://localhost:1883"
            }),
        };
        assert!(MqttAuditTarget::validate_config(&config).is_err());

        // Invalid scheme
        let config = AuditTargetConfig {
            id: "mqtt-test".to_string(),
            target_type: "mqtt".to_string(),
            enabled: true,
            args: json!({
                "broker_url": "http://localhost:1883",
                "topic": "test/audit"
            }),
        };
        assert!(MqttAuditTarget::validate_config(&config).is_err());

        // Invalid QoS
        let config = AuditTargetConfig {
            id: "mqtt-test".to_string(),
            target_type: "mqtt".to_string(),
            enabled: true,
            args: json!({
                "broker_url": "mqtt://localhost:1883",
                "topic": "test/audit",
                "qos": 3
            }),
        };
        assert!(MqttAuditTarget::validate_config(&config).is_err());

        // Valid target_type in args
        let config = AuditTargetConfig {
            id: "mqtt-test".to_string(),
            target_type: "mqtt".to_string(),
            enabled: true,
            args: json!({
                "broker_url": "mqtt://localhost:1883",
                "topic": "test/audit",
                "target_type": "AuditLog"
            }),
        };
        assert!(MqttAuditTarget::validate_config(&config).is_ok());

        // Invalid target_type in args
        let config = AuditTargetConfig {
            id: "mqtt-test".to_string(),
            target_type: "mqtt".to_string(),
            enabled: true,
            args: json!({
                "broker_url": "mqtt://localhost:1883",
                "topic": "test/audit",
                "target_type": "InvalidType"
            }),
        };
        assert!(MqttAuditTarget::validate_config(&config).is_err());
    }

    #[tokio::test]
    async fn test_mqtt_lifecycle() {
        let config = AuditTargetConfig {
            id: "mqtt-lifecycle-test".to_string(),
            target_type: "mqtt".to_string(),
            enabled: true,
            args: json!({
                "broker_url": "mqtt://localhost:1883",
                "topic": "test/audit/lifecycle"
            }),
        };

        let target = MqttAuditTarget::from_config(&config).await.unwrap();

        // Test initial state
        assert_eq!(target.id(), "mqtt-lifecycle-test");
        assert_eq!(target.target_type(), "mqtt");
        assert!(target.is_enabled());

        // Test lifecycle
        target.start().await.unwrap();
        let status = target.status();
        assert!(status.enabled);

        target.pause().await.unwrap();
        let status = target.status();
        assert!(!status.running);

        target.resume().await.unwrap();
        // Note: running state depends on connection state

        target.close().await.unwrap();
        let status = target.status();
        assert!(!status.running);
        assert!(!target.is_enabled());
    }

    #[tokio::test]
    async fn test_mqtt_connection_state() {
        let config = AuditTargetConfig {
            id: "mqtt-connection-test".to_string(),
            target_type: "mqtt".to_string(),
            enabled: true,
            args: json!({
                "broker_url": "mqtt://localhost:1883",
                "topic": "test/audit/connection"
            }),
        };

        let target = MqttAuditTarget::from_config(&config).await.unwrap();

        // Initial state should be disconnected
        let state = target.get_connection_state();
        assert_eq!(state, ConnectionState::Disconnected);
        assert!(!target.is_connected());

        // Queue size should be 0 initially
        assert_eq!(target.get_queue_size(), 0);
    }

    #[test]
    fn test_mqtt_qos_mapping() {
        // Test QoS level 0
        let config = AuditTargetConfig {
            id: "mqtt-qos0".to_string(),
            target_type: "mqtt".to_string(),
            enabled: true,
            args: json!({
                "broker_url": "mqtt://localhost:1883",
                "topic": "test/audit",
                "qos": 0
            }),
        };
        assert!(MqttAuditTarget::validate_config(&config).is_ok());

        // Test QoS level 1
        let config = AuditTargetConfig {
            id: "mqtt-qos1".to_string(),
            target_type: "mqtt".to_string(),
            enabled: true,
            args: json!({
                "broker_url": "mqtt://localhost:1883",
                "topic": "test/audit",
                "qos": 1
            }),
        };
        assert!(MqttAuditTarget::validate_config(&config).is_ok());

        // Test QoS level 2
        let config = AuditTargetConfig {
            id: "mqtt-qos2".to_string(),
            target_type: "mqtt".to_string(),
            enabled: true,
            args: json!({
                "broker_url": "mqtt://localhost:1883",
                "topic": "test/audit",
                "qos": 2
            }),
        };
        assert!(MqttAuditTarget::validate_config(&config).is_ok());
    }

    #[test]
    fn test_mqtt_auth_validation() {
        // Test with username/password in root args
        let config = AuditTargetConfig {
            id: "mqtt-auth-test".to_string(),
            target_type: "mqtt".to_string(),
            enabled: true,
            args: json!({
                "broker_url": "mqtt://localhost:1883",
                "topic": "test/audit",
                "username": "testuser",
                "password": "testpass"
            }),
        };
        assert!(MqttAuditTarget::validate_config(&config).is_ok());

        // Test with auth object (per specification)
        let config = AuditTargetConfig {
            id: "mqtt-auth-obj-test".to_string(),
            target_type: "mqtt".to_string(),
            enabled: true,
            args: json!({
                "broker_url": "mqtt://localhost:1883",
                "topic": "test/audit",
                "auth": {
                    "username": "testuser",
                    "password": "testpass"
                }
            }),
        };
        assert!(MqttAuditTarget::validate_config(&config).is_ok());
    }
}
