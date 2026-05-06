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

use super::common::{is_target_enabled, split_env_field_and_instance};
use rustfs_config::{DEFAULT_DELIMITER, ENABLE_KEY, ENV_PREFIX, EnableState};
use rustfs_ecstore::config::{Config, KVS};
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use tracing::{debug, warn};

pub fn collect_target_configs(
    config: &Config,
    route_prefix: &str,
    target_type: &str,
    valid_fields: &HashSet<String>,
) -> Vec<(String, KVS)> {
    collect_target_configs_from_env(config, route_prefix, target_type, valid_fields, std::env::vars())
}

fn is_sensitive_target_field(field_name: &str) -> bool {
    let field_name = field_name.to_ascii_lowercase();
    field_name.contains("password")
        || field_name.contains("secret")
        || field_name.contains("token")
        || field_name.contains("credential")
        || field_name.contains("private_key")
        || field_name.contains("client_key")
        || field_name.contains("access_key")
        || field_name.contains("auth")
        || field_name.contains(rustfs_config::MYSQL_DSN_STRING)
}

fn redact_target_field_value(field_name: &str, value: &str) -> String {
    if value.is_empty() {
        return value.to_string();
    }
    // MySQL DSN fields need partial redaction instead of full masking so the
    // remaining connection details (host, port, database) remain visible in
    // debug logs while the password is hidden.
    if field_name == rustfs_config::MYSQL_DSN_STRING {
        return crate::target::mysql::redact_mysql_dsn(value);
    }
    if is_sensitive_target_field(field_name) {
        return "***redacted***".to_string();
    }
    value.to_string()
}

fn redacted_target_config(config: &KVS) -> Vec<(String, String)> {
    config
        .0
        .iter()
        .map(|kv| (kv.key.clone(), redact_target_field_value(&kv.key, &kv.value)))
        .collect()
}

pub fn collect_env_target_instance_ids(route_prefix: &str, target_type: &str, valid_fields: &HashSet<String>) -> HashSet<String> {
    collect_env_target_instance_ids_from_env(route_prefix, target_type, valid_fields, std::env::vars())
}

pub fn collect_env_target_instance_ids_from_env<I>(
    route_prefix: &str,
    target_type: &str,
    valid_fields: &HashSet<String>,
    env_vars: I,
) -> HashSet<String>
where
    I: IntoIterator<Item = (String, String)>,
{
    let env_prefix = format!("{ENV_PREFIX}{route_prefix}{target_type}{DEFAULT_DELIMITER}").to_uppercase();
    let mut instance_ids = HashSet::new();

    for (key, _value) in env_vars.into_iter().filter(|(key, _)| key.starts_with(ENV_PREFIX)) {
        let Some(rest) = key.strip_prefix(&env_prefix) else {
            continue;
        };
        let Some((_field_name, instance_id)) = split_env_field_and_instance(rest, valid_fields) else {
            continue;
        };
        if instance_id != DEFAULT_DELIMITER && !instance_id.is_empty() {
            instance_ids.insert(instance_id);
        }
    }

    instance_ids
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
            value = %redact_target_field_value(&field_name, value),
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

        if tracing::enabled!(tracing::Level::DEBUG) {
            let redacted_config = redacted_target_config(&merged_config);
            debug!(instance_id = %id, ?redacted_config, "Merged target configuration");
        }
        if is_target_enabled(&merged_config) {
            merged_configs.push((id, merged_config));
        }
    }

    merged_configs
}

#[cfg(test)]
mod tests {
    use super::{
        collect_env_target_instance_ids_from_env, collect_target_configs_from_env, redact_target_field_value,
        redacted_target_config,
    };
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

    #[test]
    fn collect_env_target_instance_ids_handles_keys_with_internal_underscores() {
        let ids = collect_env_target_instance_ids_from_env(
            NOTIFY_ROUTE_PREFIX,
            "webhook",
            &HashSet::from([
                ENABLE_KEY.to_string(),
                WEBHOOK_ENDPOINT.to_string(),
                WEBHOOK_QUEUE_LIMIT.to_string(),
            ]),
            vec![
                ("RUSTFS_NOTIFY_WEBHOOK_ENABLE_PRIMARY".to_string(), "on".to_string()),
                ("RUSTFS_NOTIFY_WEBHOOK_QUEUE_LIMIT_PRIMARY".to_string(), "42".to_string()),
            ],
        );

        assert_eq!(ids, HashSet::from(["primary".to_string()]));
    }

    #[test]
    fn redact_target_field_value_redacts_sensitive_fields() {
        assert_eq!(redact_target_field_value("password", "secret"), "***redacted***");
        assert_eq!(redact_target_field_value("auth_token", "token"), "***redacted***");
        assert_eq!(redact_target_field_value("credentials_file", "/tmp/creds"), "***redacted***");
    }

    #[test]
    fn redact_target_field_value_keeps_non_sensitive_fields() {
        assert_eq!(redact_target_field_value("endpoint", "https://example.com"), "https://example.com");
        assert_eq!(redact_target_field_value("queue_limit", "1000"), "1000");
    }

    #[test]
    fn redact_dsn_string_partial_redaction() {
        let dsn = "rustfs:secret123@tcp(mysql.example.com:3306)/rustfs_events";
        let redacted = redact_target_field_value(rustfs_config::MYSQL_DSN_STRING, dsn);
        assert_eq!(redacted, "rustfs:***@tcp(mysql.example.com:3306)/rustfs_events");
        // empty dsn_string value
        assert_eq!(redact_target_field_value(rustfs_config::MYSQL_DSN_STRING, ""), "");
    }

    #[test]
    fn redacted_target_config_masks_sensitive_values_without_mutating_shape() {
        let mut config = KVS::new();
        config.insert("endpoint".to_string(), "https://example.com/hook".to_string());
        config.insert("password".to_string(), "super-secret".to_string());
        config.insert("client_key".to_string(), "private-key".to_string());
        config.insert("auth_token".to_string(), "bearer-token".to_string());
        config.insert("empty_secret".to_string(), String::new());

        let redacted = redacted_target_config(&config);

        assert_eq!(
            redacted,
            vec![
                ("endpoint".to_string(), "https://example.com/hook".to_string()),
                ("password".to_string(), "***redacted***".to_string()),
                ("client_key".to_string(), "***redacted***".to_string()),
                ("auth_token".to_string(), "***redacted***".to_string()),
                ("empty_secret".to_string(), String::new()),
            ]
        );
    }
}
