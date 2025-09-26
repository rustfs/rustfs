//  Copyright 2024 RustFS Team
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use crate::AuditEntry;
use crate::{AuditError, AuditResult};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use rustfs_config::audit::AUDIT_ROUTE_PREFIX;
use rustfs_config::{
    DEFAULT_DELIMITER, ENABLE_KEY, ENV_PREFIX, MQTT_BROKER, MQTT_KEEP_ALIVE_INTERVAL, MQTT_PASSWORD, MQTT_QOS, MQTT_QUEUE_DIR,
    MQTT_QUEUE_LIMIT, MQTT_RECONNECT_INTERVAL, MQTT_TOPIC, MQTT_USERNAME, WEBHOOK_AUTH_TOKEN, WEBHOOK_BATCH_SIZE,
    WEBHOOK_CLIENT_CERT, WEBHOOK_CLIENT_KEY, WEBHOOK_ENDPOINT, WEBHOOK_HTTP_TIMEOUT, WEBHOOK_MAX_RETRY, WEBHOOK_QUEUE_DIR,
    WEBHOOK_QUEUE_LIMIT, WEBHOOK_RETRY_INTERVAL,
};
use rustfs_ecstore::config::{Config, KVS};
use rustfs_targets::target::{ChannelTargetType, TargetType, mqtt::MQTTArgs, webhook::WebhookArgs};
use rustfs_targets::{Target, TargetError};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};
use url::Url;

/// Registry for managing audit targets
pub struct AuditRegistry {
    /// Storage for created targets
    targets: HashMap<String, Box<dyn Target<AuditEntry> + Send + Sync>>,
}

impl Default for AuditRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl AuditRegistry {
    /// Creates a new AuditRegistry
    pub fn new() -> Self {
        Self { targets: HashMap::new() }
    }

    /// Creates all audit targets from system configuration and environment variables.
    /// This method processes the creation of each target concurrently as follows:
    /// 1. Iterate through supported target types (webhook, mqtt).
    /// 2. For each type, resolve its configuration from file and environment variables.
    /// 3. Identify all target instance IDs that need to be created.
    /// 4. Merge configurations with precedence: ENV > file instance > file default.
    /// 5. Create async tasks for enabled instances.
    /// 6. Execute tasks concurrently and collect successful targets.
    /// 7. Persist successful configurations back to system storage.
    pub async fn create_targets_from_config(
        &mut self,
        config: &Config,
    ) -> AuditResult<Vec<Box<dyn Target<AuditEntry> + Send + Sync>>> {
        // Collect only environment variables with the relevant prefix to reduce memory usage
        let all_env: Vec<(String, String)> = std::env::vars().filter(|(key, _)| key.starts_with(ENV_PREFIX)).collect();

        // A collection of asynchronous tasks for concurrently executing target creation
        let mut tasks = FuturesUnordered::new();
        let mut final_config = config.clone();

        // Supported target types for audit
        let target_types = vec![ChannelTargetType::Webhook.as_str(), ChannelTargetType::Mqtt.as_str()];

        // 1. Traverse all target types and process them
        for target_type in target_types {
            let span = tracing::Span::current();
            span.record("target_type", target_type);
            info!(target_type = %target_type, "Starting audit target type processing");

            // 2. Prepare the configuration source
            let section_name = format!("{AUDIT_ROUTE_PREFIX}{target_type}");
            let file_configs = config.0.get(&section_name).cloned().unwrap_or_default();
            let default_cfg = file_configs.get(DEFAULT_DELIMITER).cloned().unwrap_or_default();
            debug!(?default_cfg, "Retrieved default configuration");

            // Get valid fields for the target type
            let valid_fields = match target_type {
                "webhook" => get_webhook_valid_fields(),
                "mqtt" => get_mqtt_valid_fields(),
                _ => {
                    warn!(target_type = %target_type, "Unknown target type, skipping");
                    continue;
                }
            };
            debug!(?valid_fields, "Retrieved valid configuration fields");

            // 3. Resolve instance IDs and configuration overrides from environment variables
            let mut instance_ids_from_env = HashSet::new();
            let mut env_overrides: HashMap<String, HashMap<String, String>> = HashMap::new();

            for (env_key, env_value) in &all_env {
                let audit_prefix = format!("{ENV_PREFIX}AUDIT_{}", target_type.to_uppercase());
                if !env_key.starts_with(&audit_prefix) {
                    continue;
                }

                let suffix = &env_key[audit_prefix.len()..];
                if suffix.is_empty() {
                    continue;
                }

                // Parse field and instance from suffix (FIELD_INSTANCE or FIELD)
                let (field_name, instance_id) = if let Some(last_underscore) = suffix.rfind('_') {
                    let potential_field = &suffix[1..last_underscore]; // Skip leading _
                    let potential_instance = &suffix[last_underscore + 1..];

                    // Check if the part before the last underscore is a valid field
                    if valid_fields.contains(&potential_field.to_lowercase()) {
                        (potential_field.to_lowercase(), potential_instance.to_lowercase())
                    } else {
                        // Treat the entire suffix as field name with default instance
                        (suffix[1..].to_lowercase(), DEFAULT_DELIMITER.to_string())
                    }
                } else {
                    // No underscore, treat as field with default instance
                    (suffix[1..].to_lowercase(), DEFAULT_DELIMITER.to_string())
                };

                if valid_fields.contains(&field_name) {
                    if instance_id != DEFAULT_DELIMITER {
                        instance_ids_from_env.insert(instance_id.clone());
                    }
                    env_overrides
                        .entry(instance_id)
                        .or_default()
                        .insert(field_name, env_value.clone());
                } else {
                    debug!(
                        env_key = %env_key,
                        field_name = %field_name,
                        "Ignoring environment variable field not found in valid fields for target type {}",
                        target_type
                    );
                }
            }
            debug!(?env_overrides, "Completed environment variable analysis");

            // 4. Determine all instance IDs that need to be processed
            let mut all_instance_ids: HashSet<String> =
                file_configs.keys().filter(|k| *k != DEFAULT_DELIMITER).cloned().collect();
            all_instance_ids.extend(instance_ids_from_env);
            debug!(?all_instance_ids, "Determined all instance IDs");

            // 5. Merge configurations and create tasks for each instance
            for id in all_instance_ids {
                // 5.1. Merge configuration, priority: Environment variables > File instance > File default
                let mut merged_config = default_cfg.clone();

                // Apply file instance configuration if available
                if let Some(file_instance_cfg) = file_configs.get(&id) {
                    merged_config.extend(file_instance_cfg.clone());
                }

                // Apply environment variable overrides
                if let Some(env_instance_cfg) = env_overrides.get(&id) {
                    let mut kvs_from_env = KVS::new();
                    for (k, v) in env_instance_cfg {
                        kvs_from_env.insert(k.clone(), v.clone());
                    }
                    merged_config.extend(kvs_from_env);
                }
                debug!(instance_id = %id, ?merged_config, "Completed configuration merge");

                // 5.2. Check if the instance is enabled
                let enabled = merged_config
                    .lookup(ENABLE_KEY)
                    .map(|v| parse_enable_value(&v))
                    .unwrap_or(false);

                if enabled {
                    info!(instance_id = %id, "Creating audit target");

                    // Create task for concurrent execution
                    let target_type_clone = target_type.to_string();
                    let id_clone = id.clone();
                    let merged_config_arc = Arc::new(merged_config.clone());
                    let final_config_arc = Arc::new(final_config.clone());

                    let task = tokio::spawn(async move {
                        let result = create_audit_target(&target_type_clone, &id_clone, &merged_config_arc).await;
                        (target_type_clone, id_clone, result, final_config_arc)
                    });

                    tasks.push(task);

                    // Update final config with successful instance
                    final_config
                        .0
                        .entry(section_name.clone())
                        .or_default()
                        .insert(id, merged_config);
                } else {
                    info!(instance_id = %id, "Skipping disabled audit target, will be removed from final configuration");
                    // Remove disabled target from final configuration
                    final_config.0.entry(section_name.clone()).or_default().remove(&id);
                }
            }
        }

        // 6. Concurrently execute all creation tasks and collect results
        let mut successful_targets = Vec::new();

        while let Some(task_result) = tasks.next().await {
            match task_result {
                Ok((target_type, id, result, _final_config)) => match result {
                    Ok(target) => {
                        info!(target_type = %target_type, instance_id = %id, "Created audit target successfully");
                        successful_targets.push(target);
                    }
                    Err(e) => {
                        error!(target_type = %target_type, instance_id = %id, error = %e, "Failed to create audit target");
                    }
                },
                Err(e) => {
                    error!(error = %e, "Task execution failed");
                }
            }
        }

        // 7. Save the new configuration to the system
        let Some(store) = rustfs_ecstore::new_object_layer_fn() else {
            return Err(AuditError::ServerNotInitialized(
                "Failed to save target configuration: server storage not initialized".to_string(),
            ));
        };

        match rustfs_ecstore::config::com::save_server_config(store, &final_config).await {
            Ok(_) => info!("New audit configuration saved to system successfully"),
            Err(e) => {
                error!(error = %e, "Failed to save new audit configuration");
                return Err(AuditError::SaveConfig(e.to_string()));
            }
        }

        Ok(successful_targets)
    }

    /// Adds a target to the registry
    pub fn add_target(&mut self, id: String, target: Box<dyn Target<AuditEntry> + Send + Sync>) {
        self.targets.insert(id, target);
    }

    /// Removes a target from the registry
    pub fn remove_target(&mut self, id: &str) -> Option<Box<dyn Target<AuditEntry> + Send + Sync>> {
        self.targets.remove(id)
    }

    /// Gets a target from the registry
    pub fn get_target(&self, id: &str) -> Option<&(dyn Target<AuditEntry> + Send + Sync)> {
        self.targets.get(id).map(|t| t.as_ref())
    }

    /// Lists all target IDs
    pub fn list_targets(&self) -> Vec<String> {
        self.targets.keys().cloned().collect()
    }

    /// Closes all targets and clears the registry
    pub async fn close_all(&mut self) -> AuditResult<()> {
        let mut errors = Vec::new();

        for (id, target) in self.targets.drain() {
            if let Err(e) = target.close().await {
                error!(target_id = %id, error = %e, "Failed to close audit target");
                errors.push(e);
            }
        }

        if !errors.is_empty() {
            return Err(AuditError::Target(errors.into_iter().next().unwrap()));
        }

        Ok(())
    }
}

/// Creates an audit target based on type and configuration
async fn create_audit_target(
    target_type: &str,
    id: &str,
    config: &KVS,
) -> Result<Box<dyn Target<AuditEntry> + Send + Sync>, TargetError> {
    match target_type {
        val if val == ChannelTargetType::Webhook.as_str() => {
            let args = parse_webhook_args(id, config)?;
            let target = rustfs_targets::target::webhook::WebhookTarget::new(id.to_string(), args)?;
            Ok(Box::new(target))
        }
        val if val == ChannelTargetType::Mqtt.as_str() => {
            let args = parse_mqtt_args(id, config)?;
            let target = rustfs_targets::target::mqtt::MQTTTarget::new(id.to_string(), args)?;
            Ok(Box::new(target))
        }
        _ => Err(TargetError::Configuration(format!("Unknown target type: {target_type}"))),
    }
}

/// Gets valid field names for webhook configuration
fn get_webhook_valid_fields() -> HashSet<String> {
    vec![
        ENABLE_KEY.to_string(),
        WEBHOOK_ENDPOINT.to_string(),
        WEBHOOK_AUTH_TOKEN.to_string(),
        WEBHOOK_CLIENT_CERT.to_string(),
        WEBHOOK_CLIENT_KEY.to_string(),
        WEBHOOK_BATCH_SIZE.to_string(),
        WEBHOOK_QUEUE_LIMIT.to_string(),
        WEBHOOK_QUEUE_DIR.to_string(),
        WEBHOOK_MAX_RETRY.to_string(),
        WEBHOOK_RETRY_INTERVAL.to_string(),
        WEBHOOK_HTTP_TIMEOUT.to_string(),
    ]
    .into_iter()
    .collect()
}

/// Gets valid field names for MQTT configuration
fn get_mqtt_valid_fields() -> HashSet<String> {
    vec![
        ENABLE_KEY.to_string(),
        MQTT_BROKER.to_string(),
        MQTT_TOPIC.to_string(),
        MQTT_USERNAME.to_string(),
        MQTT_PASSWORD.to_string(),
        MQTT_QOS.to_string(),
        MQTT_KEEP_ALIVE_INTERVAL.to_string(),
        MQTT_RECONNECT_INTERVAL.to_string(),
        MQTT_QUEUE_DIR.to_string(),
        MQTT_QUEUE_LIMIT.to_string(),
    ]
    .into_iter()
    .collect()
}

/// Parses webhook arguments from KVS configuration
fn parse_webhook_args(_id: &str, config: &KVS) -> Result<WebhookArgs, TargetError> {
    let endpoint = config
        .lookup(WEBHOOK_ENDPOINT)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| TargetError::Configuration("webhook endpoint is required".to_string()))?;

    let endpoint_url =
        Url::parse(&endpoint).map_err(|e| TargetError::Configuration(format!("invalid webhook endpoint URL: {e}")))?;

    let args = WebhookArgs {
        enable: true, // Already validated as enabled
        endpoint: endpoint_url,
        auth_token: config.lookup(WEBHOOK_AUTH_TOKEN).unwrap_or_default(),
        queue_dir: config.lookup(WEBHOOK_QUEUE_DIR).unwrap_or_default(),
        queue_limit: config
            .lookup(WEBHOOK_QUEUE_LIMIT)
            .and_then(|s| s.parse().ok())
            .unwrap_or(100000),
        client_cert: config.lookup(WEBHOOK_CLIENT_CERT).unwrap_or_default(),
        client_key: config.lookup(WEBHOOK_CLIENT_KEY).unwrap_or_default(),
        target_type: TargetType::AuditLog,
    };

    args.validate()?;
    Ok(args)
}

/// Parses MQTT arguments from KVS configuration  
fn parse_mqtt_args(_id: &str, config: &KVS) -> Result<MQTTArgs, TargetError> {
    let broker = config
        .lookup(MQTT_BROKER)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| TargetError::Configuration("MQTT broker is required".to_string()))?;

    let broker_url = Url::parse(&broker).map_err(|e| TargetError::Configuration(format!("invalid MQTT broker URL: {e}")))?;

    let topic = config
        .lookup(MQTT_TOPIC)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| TargetError::Configuration("MQTT topic is required".to_string()))?;

    let qos = config
        .lookup(MQTT_QOS)
        .and_then(|s| s.parse::<u8>().ok())
        .and_then(|q| match q {
            0 => Some(rumqttc::QoS::AtMostOnce),
            1 => Some(rumqttc::QoS::AtLeastOnce),
            2 => Some(rumqttc::QoS::ExactlyOnce),
            _ => None,
        })
        .unwrap_or(rumqttc::QoS::AtLeastOnce);

    let args = MQTTArgs {
        enable: true, // Already validated as enabled
        broker: broker_url,
        topic,
        qos,
        username: config.lookup(MQTT_USERNAME).unwrap_or_default(),
        password: config.lookup(MQTT_PASSWORD).unwrap_or_default(),
        max_reconnect_interval: parse_duration(&config.lookup(MQTT_RECONNECT_INTERVAL).unwrap_or_else(|| "5s".to_string()))
            .unwrap_or(Duration::from_secs(5)),
        keep_alive: parse_duration(&config.lookup(MQTT_KEEP_ALIVE_INTERVAL).unwrap_or_else(|| "60s".to_string()))
            .unwrap_or(Duration::from_secs(60)),
        queue_dir: config.lookup(MQTT_QUEUE_DIR).unwrap_or_default(),
        queue_limit: config.lookup(MQTT_QUEUE_LIMIT).and_then(|s| s.parse().ok()).unwrap_or(100000),
        target_type: TargetType::AuditLog,
    };

    args.validate()?;
    Ok(args)
}

/// Parses enable value from string
fn parse_enable_value(value: &str) -> bool {
    matches!(value.to_lowercase().as_str(), "1" | "on" | "true" | "yes")
}

/// Parses duration from string (e.g., "3s", "5m")
fn parse_duration(s: &str) -> Option<Duration> {
    if let Some(stripped) = s.strip_suffix('s') {
        stripped.parse::<u64>().ok().map(Duration::from_secs)
    } else if let Some(stripped) = s.strip_suffix('m') {
        stripped.parse::<u64>().ok().map(|m| Duration::from_secs(m * 60))
    } else if let Some(stripped) = s.strip_suffix("ms") {
        stripped.parse::<u64>().ok().map(Duration::from_millis)
    } else {
        s.parse::<u64>().ok().map(Duration::from_secs)
    }
}
