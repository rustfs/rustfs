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

use crate::error::TargetError;
use crate::target::{
    TargetType,
    mqtt::{MQTTArgs, MQTTTlsConfig, validate_mqtt_broker_url},
    webhook::WebhookArgs,
};
use rumqttc::QoS;
use rustfs_config::{
    DEFAULT_DELIMITER, DEFAULT_LIMIT, ENABLE_KEY, ENV_PREFIX, EnableState, MQTT_BROKER, MQTT_KEEP_ALIVE_INTERVAL, MQTT_PASSWORD,
    MQTT_QOS, MQTT_QUEUE_DIR, MQTT_QUEUE_LIMIT, MQTT_RECONNECT_INTERVAL, MQTT_TLS_CA, MQTT_TLS_CLIENT_CERT, MQTT_TLS_CLIENT_KEY,
    MQTT_TLS_POLICY, MQTT_TLS_TRUST_LEAF_AS_CA, MQTT_TOPIC, MQTT_USERNAME, MQTT_WS_PATH_ALLOWLIST,
    RUSTFS_WEBHOOK_SKIP_TLS_VERIFY_DEFAULT, WEBHOOK_AUTH_TOKEN, WEBHOOK_CLIENT_CA, WEBHOOK_CLIENT_CERT, WEBHOOK_CLIENT_KEY,
    WEBHOOK_ENDPOINT, WEBHOOK_QUEUE_DIR, WEBHOOK_QUEUE_LIMIT, WEBHOOK_SKIP_TLS_VERIFY,
};
use rustfs_ecstore::config::{Config, KVS};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::str::FromStr;
use std::time::Duration;
use tracing::{debug, warn};
use url::Url;

pub fn collect_target_configs(
    config: &Config,
    route_prefix: &str,
    target_type: &str,
    valid_fields: &HashSet<String>,
) -> Vec<(String, KVS)> {
    collect_target_configs_from_env(config, route_prefix, target_type, valid_fields, std::env::vars())
}

pub fn collect_target_configs_from_env<I>(
    config: &Config,
    route_prefix: &str,
    target_type: &str,
    valid_fields: &HashSet<String>,
    env_vars: I,
) -> Vec<(String, KVS)>
where
    I: IntoIterator<Item = (String, String)>,
{
    let all_env: Vec<(String, String)> = env_vars.into_iter().filter(|(key, _)| key.starts_with(ENV_PREFIX)).collect();
    let section_name = format!("{route_prefix}{target_type}").to_lowercase();
    let file_configs = config.0.get(&section_name).cloned().unwrap_or_default();
    let default_cfg = file_configs.get(DEFAULT_DELIMITER).cloned().unwrap_or_default();

    let enable_prefix =
        format!("{ENV_PREFIX}{route_prefix}{target_type}{DEFAULT_DELIMITER}{ENABLE_KEY}{DEFAULT_DELIMITER}").to_uppercase();
    let env_prefix = format!("{ENV_PREFIX}{route_prefix}{target_type}{DEFAULT_DELIMITER}").to_uppercase();

    let mut instance_ids_from_env = HashSet::new();
    let mut env_overrides: HashMap<String, KVS> = HashMap::new();
    for (key, value) in &all_env {
        if EnableState::from_str(value).ok().map(|s| s.is_enabled()).unwrap_or(false)
            && let Some(id) = key.strip_prefix(&enable_prefix)
            && !id.is_empty()
        {
            instance_ids_from_env.insert(id.to_lowercase());
        }

        let Some(rest) = key.strip_prefix(&env_prefix) else {
            continue;
        };

        let Some((field_name, instance_id)) = split_env_field_and_instance(rest, valid_fields) else {
            warn!(
                field_name = %rest.to_lowercase(),
                "Ignore environment variable field not found in the valid field list for target type {}",
                target_type
            );
            continue;
        };

        debug!(
            instance_id = %if instance_id == DEFAULT_DELIMITER { DEFAULT_DELIMITER } else { &instance_id },
            %field_name,
            %value,
            "Parsed target environment override"
        );
        env_overrides
            .entry(instance_id)
            .or_default()
            .insert(field_name, value.clone());
    }

    let mut effective_default = default_cfg;
    if let Some(default_env_cfg) = env_overrides.remove(DEFAULT_DELIMITER) {
        effective_default.extend(default_env_cfg);
    }

    let mut all_instance_ids: Vec<String> = file_configs
        .keys()
        .filter(|key| key.as_str() != DEFAULT_DELIMITER)
        .cloned()
        .collect();
    all_instance_ids.extend(instance_ids_from_env);
    all_instance_ids.sort();
    all_instance_ids.dedup();

    let mut merged_configs = Vec::new();
    for id in all_instance_ids {
        let mut merged_config = effective_default.clone();
        if let Some(file_instance_cfg) = file_configs.get(&id) {
            merged_config.extend(file_instance_cfg.clone());
        }
        if let Some(env_instance_cfg) = env_overrides.get(&id) {
            merged_config.extend(env_instance_cfg.clone());
        }

        debug!(instance_id = %id, ?merged_config, "Merged target configuration");
        if is_target_enabled(&merged_config) {
            merged_configs.push((id, merged_config));
        }
    }

    merged_configs
}

pub fn build_webhook_args(config: &KVS, default_queue_dir: &str, target_type: TargetType) -> Result<WebhookArgs, TargetError> {
    let endpoint = config
        .lookup(WEBHOOK_ENDPOINT)
        .ok_or_else(|| TargetError::Configuration("Missing webhook endpoint".to_string()))?;
    let parsed_endpoint = endpoint.trim();
    let endpoint_url = Url::parse(parsed_endpoint)
        .map_err(|e| TargetError::Configuration(format!("Invalid endpoint URL: {e} (value: '{parsed_endpoint}')")))?;

    Ok(WebhookArgs {
        enable: true,
        endpoint: endpoint_url,
        auth_token: config.lookup(WEBHOOK_AUTH_TOKEN).unwrap_or_default(),
        queue_dir: config
            .lookup(WEBHOOK_QUEUE_DIR)
            .unwrap_or_else(|| default_queue_dir.to_string()),
        queue_limit: config
            .lookup(WEBHOOK_QUEUE_LIMIT)
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(DEFAULT_LIMIT),
        client_cert: config.lookup(WEBHOOK_CLIENT_CERT).unwrap_or_default(),
        client_key: config.lookup(WEBHOOK_CLIENT_KEY).unwrap_or_default(),
        client_ca: config.lookup(WEBHOOK_CLIENT_CA).unwrap_or_default(),
        skip_tls_verify: config
            .lookup(WEBHOOK_SKIP_TLS_VERIFY)
            .and_then(|v| v.parse::<bool>().ok())
            .unwrap_or(RUSTFS_WEBHOOK_SKIP_TLS_VERIFY_DEFAULT),
        target_type,
    })
}

pub fn validate_webhook_config(config: &KVS, default_queue_dir: &str) -> Result<(), TargetError> {
    let endpoint = config
        .lookup(WEBHOOK_ENDPOINT)
        .ok_or_else(|| TargetError::Configuration("Missing webhook endpoint".to_string()))?;
    debug!("endpoint: {}", endpoint);
    let parsed_endpoint = endpoint.trim();
    Url::parse(parsed_endpoint)
        .map_err(|e| TargetError::Configuration(format!("Invalid endpoint URL: {e} (value: '{parsed_endpoint}')")))?;

    let client_cert = config.lookup(WEBHOOK_CLIENT_CERT).unwrap_or_default();
    let client_key = config.lookup(WEBHOOK_CLIENT_KEY).unwrap_or_default();
    if client_cert.is_empty() != client_key.is_empty() {
        return Err(TargetError::Configuration(
            "Both client_cert and client_key must be specified together".to_string(),
        ));
    }

    let queue_dir = config
        .lookup(WEBHOOK_QUEUE_DIR)
        .unwrap_or_else(|| default_queue_dir.to_string());
    if !queue_dir.is_empty() && !Path::new(&queue_dir).is_absolute() {
        return Err(TargetError::Configuration("Webhook queue directory must be an absolute path".to_string()));
    }

    Ok(())
}

pub fn build_mqtt_args(config: &KVS, default_queue_dir: &str, target_type: TargetType) -> Result<MQTTArgs, TargetError> {
    let broker = config
        .lookup(MQTT_BROKER)
        .ok_or_else(|| TargetError::Configuration("Missing MQTT broker".to_string()))?;
    let broker_url =
        Url::parse(&broker).map_err(|e| TargetError::Configuration(format!("Invalid broker URL: {e} (value: '{broker}')")))?;

    let topic = config
        .lookup(MQTT_TOPIC)
        .ok_or_else(|| TargetError::Configuration("Missing MQTT topic".to_string()))?;

    Ok(MQTTArgs {
        enable: true,
        broker: broker_url,
        topic,
        qos: config
            .lookup(MQTT_QOS)
            .and_then(|v| v.parse::<u8>().ok())
            .map(|q| match q {
                0 => QoS::AtMostOnce,
                1 => QoS::AtLeastOnce,
                2 => QoS::ExactlyOnce,
                _ => QoS::AtLeastOnce,
            })
            .unwrap_or(QoS::AtLeastOnce),
        username: config.lookup(MQTT_USERNAME).unwrap_or_default(),
        password: config.lookup(MQTT_PASSWORD).unwrap_or_default(),
        max_reconnect_interval: config
            .lookup(MQTT_RECONNECT_INTERVAL)
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or_else(|| Duration::from_secs(5)),
        keep_alive: config
            .lookup(MQTT_KEEP_ALIVE_INTERVAL)
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or_else(|| Duration::from_secs(30)),
        tls: MQTTTlsConfig::from_values(
            config.lookup(MQTT_TLS_POLICY).as_deref(),
            config.lookup(MQTT_TLS_CA).as_deref(),
            config.lookup(MQTT_TLS_CLIENT_CERT).as_deref(),
            config.lookup(MQTT_TLS_CLIENT_KEY).as_deref(),
            config.lookup(MQTT_TLS_TRUST_LEAF_AS_CA).as_deref(),
            config.lookup(MQTT_WS_PATH_ALLOWLIST).as_deref(),
        )?,
        queue_dir: config.lookup(MQTT_QUEUE_DIR).unwrap_or_else(|| default_queue_dir.to_string()),
        queue_limit: config
            .lookup(MQTT_QUEUE_LIMIT)
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(DEFAULT_LIMIT),
        target_type,
    })
}

pub fn validate_mqtt_config(config: &KVS) -> Result<(), TargetError> {
    let broker = config
        .lookup(MQTT_BROKER)
        .ok_or_else(|| TargetError::Configuration("Missing MQTT broker".to_string()))?;
    let url =
        Url::parse(&broker).map_err(|e| TargetError::Configuration(format!("Invalid broker URL: {e} (value: '{broker}')")))?;

    let tls = MQTTTlsConfig::from_values(
        config.lookup(MQTT_TLS_POLICY).as_deref(),
        config.lookup(MQTT_TLS_CA).as_deref(),
        config.lookup(MQTT_TLS_CLIENT_CERT).as_deref(),
        config.lookup(MQTT_TLS_CLIENT_KEY).as_deref(),
        config.lookup(MQTT_TLS_TRUST_LEAF_AS_CA).as_deref(),
        config.lookup(MQTT_WS_PATH_ALLOWLIST).as_deref(),
    )?;
    validate_mqtt_broker_url(&url, &tls)?;

    if config.lookup(MQTT_TOPIC).is_none() {
        return Err(TargetError::Configuration("Missing MQTT topic".to_string()));
    }

    if let Some(qos_str) = config.lookup(MQTT_QOS) {
        let qos = qos_str
            .parse::<u8>()
            .map_err(|_| TargetError::Configuration("Invalid QoS value".to_string()))?;
        if qos > 2 {
            return Err(TargetError::Configuration("QoS must be 0, 1, or 2".to_string()));
        }
    }

    let queue_dir = config.lookup(MQTT_QUEUE_DIR).unwrap_or_default();
    if !queue_dir.is_empty() {
        if !Path::new(&queue_dir).is_absolute() {
            return Err(TargetError::Configuration("MQTT queue directory must be an absolute path".to_string()));
        }
        if let Some(qos_str) = config.lookup(MQTT_QOS)
            && qos_str == "0"
        {
            warn!("Using queue_dir with QoS 0 may result in event loss");
        }
    }

    Ok(())
}

fn split_env_field_and_instance(rest: &str, valid_fields: &HashSet<String>) -> Option<(String, String)> {
    let normalized = rest.to_lowercase();
    if valid_fields.contains(&normalized) {
        return Some((normalized, DEFAULT_DELIMITER.to_string()));
    }

    valid_fields
        .iter()
        .filter_map(|field| {
            normalized
                .strip_prefix(field)
                .and_then(|suffix| suffix.strip_prefix(DEFAULT_DELIMITER))
                .filter(|instance_id| !instance_id.is_empty())
                .map(|instance_id| (field.clone(), instance_id.to_string()))
        })
        .max_by_key(|(field, _)| field.len())
}

fn is_target_enabled(config: &KVS) -> bool {
    config
        .lookup(ENABLE_KEY)
        .map(|v| {
            EnableState::from_str(v.as_str())
                .ok()
                .map(|s| s.is_enabled())
                .unwrap_or(false)
        })
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::collect_target_configs_from_env;
    use rustfs_config::notify::NOTIFY_ROUTE_PREFIX;
    use rustfs_config::{ENABLE_KEY, WEBHOOK_ENDPOINT, WEBHOOK_QUEUE_LIMIT};
    use rustfs_ecstore::config::{Config, KVS};
    use std::collections::{HashMap, HashSet};

    #[test]
    fn collect_target_configs_applies_default_env_overrides_to_file_targets() {
        let mut cfg = Config(HashMap::new());
        let mut subsystem = HashMap::new();

        let mut default_kvs = KVS::new();
        default_kvs.insert(ENABLE_KEY.to_string(), "off".to_string());
        subsystem.insert("_".to_string(), default_kvs);

        let mut primary = KVS::new();
        primary.insert(WEBHOOK_ENDPOINT.to_string(), "https://example.com/primary".to_string());
        subsystem.insert("primary".to_string(), primary);

        let mut secondary = KVS::new();
        secondary.insert(WEBHOOK_ENDPOINT.to_string(), "https://example.com/secondary".to_string());
        subsystem.insert("secondary".to_string(), secondary);

        cfg.0.insert("notify_webhook".to_string(), subsystem);

        let configs = collect_target_configs_from_env(
            &cfg,
            NOTIFY_ROUTE_PREFIX,
            "webhook",
            &HashSet::from([
                ENABLE_KEY.to_string(),
                WEBHOOK_ENDPOINT.to_string(),
                WEBHOOK_QUEUE_LIMIT.to_string(),
            ]),
            vec![
                ("RUSTFS_NOTIFY_WEBHOOK_ENABLE".to_string(), "on".to_string()),
                ("RUSTFS_NOTIFY_WEBHOOK_QUEUE_LIMIT".to_string(), "42".to_string()),
            ],
        );

        let configs: HashMap<String, KVS> = configs.into_iter().collect();
        assert_eq!(configs.len(), 2);
        assert_eq!(configs["primary"].lookup(ENABLE_KEY).as_deref(), Some("on"));
        assert_eq!(configs["secondary"].lookup(ENABLE_KEY).as_deref(), Some("on"));
        assert_eq!(configs["primary"].lookup(WEBHOOK_QUEUE_LIMIT).as_deref(), Some("42"));
        assert_eq!(configs["secondary"].lookup(WEBHOOK_QUEUE_LIMIT).as_deref(), Some("42"));
    }

    #[test]
    fn collect_target_configs_discovers_enabled_instance_from_env() {
        let cfg = Config(HashMap::new());
        let configs = collect_target_configs_from_env(
            &cfg,
            NOTIFY_ROUTE_PREFIX,
            "webhook",
            &HashSet::from([ENABLE_KEY.to_string(), WEBHOOK_ENDPOINT.to_string()]),
            vec![
                ("RUSTFS_NOTIFY_WEBHOOK_ENABLE_PRIMARY".to_string(), "on".to_string()),
                (
                    "RUSTFS_NOTIFY_WEBHOOK_ENDPOINT_PRIMARY".to_string(),
                    "https://example.com/from-env".to_string(),
                ),
            ],
        );

        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].0, "primary");
        assert_eq!(configs[0].1.lookup(WEBHOOK_ENDPOINT).as_deref(), Some("https://example.com/from-env"));
    }
}
