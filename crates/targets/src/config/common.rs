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

use crate::TargetError;
use crate::target::pulsar::validate_pulsar_broker;
use async_nats::ServerAddr;
use rustfs_config::{
    DEFAULT_DELIMITER, ENABLE_KEY, EnableState, NATS_CREDENTIALS_FILE, NATS_PASSWORD, NATS_QUEUE_DIR, NATS_SUBJECT, NATS_TLS_CA,
    NATS_TLS_CLIENT_CERT, NATS_TLS_CLIENT_KEY, NATS_TOKEN, NATS_USERNAME, PULSAR_AUTH_TOKEN, PULSAR_PASSWORD, PULSAR_QUEUE_DIR,
    PULSAR_TLS_ALLOW_INSECURE, PULSAR_TLS_CA, PULSAR_TLS_HOSTNAME_VERIFICATION, PULSAR_TOPIC, PULSAR_USERNAME,
};
use rustfs_ecstore::config::KVS;
use std::collections::HashSet;
use std::path::Path;
use std::str::FromStr;
use url::Url;

pub(super) fn split_env_field_and_instance(rest: &str, valid_fields: &HashSet<String>) -> Option<(String, String)> {
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

pub(super) fn is_target_enabled(config: &KVS) -> bool {
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

pub(super) fn parse_target_bool(value: Option<&str>) -> Option<bool> {
    let value = value?.trim();
    if value.is_empty() {
        return None;
    }
    value
        .parse::<EnableState>()
        .map(EnableState::is_enabled)
        .or_else(|_| value.parse::<bool>())
        .ok()
}

pub(super) fn validate_nats_server_config(server: &ServerAddr, config: &KVS, default_queue_dir: &str) -> Result<(), TargetError> {
    if config.lookup(NATS_SUBJECT).unwrap_or_default().trim().is_empty() {
        return Err(TargetError::Configuration("Missing NATS subject".to_string()));
    }

    if server.has_user_pass() {
        return Err(TargetError::Configuration("NATS address must not embed username or password".to_string()));
    }

    let username = config.lookup(NATS_USERNAME).unwrap_or_default();
    let password = config.lookup(NATS_PASSWORD).unwrap_or_default();
    let token = config.lookup(NATS_TOKEN).unwrap_or_default();
    let credentials_file = config.lookup(NATS_CREDENTIALS_FILE).unwrap_or_default();

    let mut auth_methods = 0usize;
    if !token.is_empty() {
        auth_methods += 1;
    }
    if !credentials_file.is_empty() {
        auth_methods += 1;
        if !Path::new(&credentials_file).is_absolute() {
            return Err(TargetError::Configuration(format!("{NATS_CREDENTIALS_FILE} must be an absolute path")));
        }
    }
    if !username.is_empty() || !password.is_empty() {
        if username.is_empty() != password.is_empty() {
            return Err(TargetError::Configuration(
                "NATS username and password must be specified together".to_string(),
            ));
        }
        auth_methods += 1;
    }
    if auth_methods > 1 {
        return Err(TargetError::Configuration(
            "NATS supports only one auth method at a time: token, username/password, or credentials_file".to_string(),
        ));
    }

    let tls_ca = config.lookup(NATS_TLS_CA).unwrap_or_default();
    let tls_client_cert = config.lookup(NATS_TLS_CLIENT_CERT).unwrap_or_default();
    let tls_client_key = config.lookup(NATS_TLS_CLIENT_KEY).unwrap_or_default();
    if !tls_ca.is_empty() && !Path::new(&tls_ca).is_absolute() {
        return Err(TargetError::Configuration(format!("{NATS_TLS_CA} must be an absolute path")));
    }
    if !tls_client_cert.is_empty() && !Path::new(&tls_client_cert).is_absolute() {
        return Err(TargetError::Configuration(format!("{NATS_TLS_CLIENT_CERT} must be an absolute path")));
    }
    if !tls_client_key.is_empty() && !Path::new(&tls_client_key).is_absolute() {
        return Err(TargetError::Configuration(format!("{NATS_TLS_CLIENT_KEY} must be an absolute path")));
    }
    if tls_client_cert.is_empty() != tls_client_key.is_empty() {
        return Err(TargetError::Configuration(
            "NATS tls_client_cert and tls_client_key must be specified together".to_string(),
        ));
    }

    let queue_dir = config.lookup(NATS_QUEUE_DIR).unwrap_or_else(|| default_queue_dir.to_string());
    if !queue_dir.is_empty() && !Path::new(&queue_dir).is_absolute() {
        return Err(TargetError::Configuration("NATS queue directory must be an absolute path".to_string()));
    }

    let _ = server;
    Ok(())
}

pub(super) fn validate_pulsar_broker_config(broker: &str, config: &KVS, default_queue_dir: &str) -> Result<(), TargetError> {
    let url = validate_pulsar_broker(broker)?;

    if config.lookup(PULSAR_TOPIC).unwrap_or_default().trim().is_empty() {
        return Err(TargetError::Configuration("Missing Pulsar topic".to_string()));
    }

    let auth_token = config.lookup(PULSAR_AUTH_TOKEN).unwrap_or_default();
    let username = config.lookup(PULSAR_USERNAME).unwrap_or_default();
    let password = config.lookup(PULSAR_PASSWORD).unwrap_or_default();
    if !auth_token.is_empty() && (!username.is_empty() || !password.is_empty()) {
        return Err(TargetError::Configuration(
            "Pulsar supports either auth_token or username/password auth, not both".to_string(),
        ));
    }
    if username.is_empty() != password.is_empty() {
        return Err(TargetError::Configuration(
            "Pulsar username and password must be specified together".to_string(),
        ));
    }

    let tls_ca = config.lookup(PULSAR_TLS_CA).unwrap_or_default();
    let tls_allow_insecure = parse_target_bool(config.lookup(PULSAR_TLS_ALLOW_INSECURE).as_deref()).unwrap_or(false);
    let tls_hostname_verification = parse_target_bool(config.lookup(PULSAR_TLS_HOSTNAME_VERIFICATION).as_deref()).unwrap_or(true);

    if !tls_ca.is_empty() && !Path::new(&tls_ca).is_absolute() {
        return Err(TargetError::Configuration("Pulsar tls_ca must be an absolute path".to_string()));
    }
    if url.scheme() != "pulsar+ssl" && (!tls_ca.is_empty() || tls_allow_insecure || !tls_hostname_verification) {
        return Err(TargetError::Configuration(
            "Pulsar TLS settings are only allowed with pulsar+ssl brokers".to_string(),
        ));
    }

    let queue_dir = config
        .lookup(PULSAR_QUEUE_DIR)
        .unwrap_or_else(|| default_queue_dir.to_string());
    if !queue_dir.is_empty() && !Path::new(&queue_dir).is_absolute() {
        return Err(TargetError::Configuration("Pulsar queue directory must be an absolute path".to_string()));
    }

    Ok(())
}

pub(super) fn parse_url(value: &str, field_label: &str) -> Result<Url, TargetError> {
    Url::parse(value).map_err(|e| TargetError::Configuration(format!("Invalid {field_label}: {e} (value: '{value}')")))
}

#[cfg(test)]
mod tests {
    use super::{validate_nats_server_config, validate_pulsar_broker_config};
    use async_nats::ServerAddr;
    use rustfs_config::{
        NATS_PASSWORD, NATS_QUEUE_DIR, NATS_SUBJECT, NATS_TOKEN, NATS_USERNAME, PULSAR_TLS_ALLOW_INSECURE, PULSAR_TOPIC,
    };
    use rustfs_ecstore::config::KVS;
    use std::str::FromStr;

    #[test]
    fn validate_nats_server_config_rejects_multiple_auth_methods() {
        let server = ServerAddr::from_str("nats://127.0.0.1:4222").expect("valid nats address");
        let mut config = KVS::new();
        config.insert(NATS_SUBJECT.to_string(), "events".to_string());
        config.insert(NATS_TOKEN.to_string(), "token".to_string());
        config.insert(NATS_USERNAME.to_string(), "user".to_string());
        config.insert(NATS_PASSWORD.to_string(), "password".to_string());

        let err = validate_nats_server_config(&server, &config, "").expect_err("conflicting auth should be rejected");

        assert!(err.to_string().contains("only one auth method"));
    }

    #[test]
    fn validate_nats_server_config_rejects_relative_queue_dir() {
        let server = ServerAddr::from_str("nats://127.0.0.1:4222").expect("valid nats address");
        let mut config = KVS::new();
        config.insert(NATS_SUBJECT.to_string(), "events".to_string());
        config.insert(NATS_QUEUE_DIR.to_string(), "relative-queue".to_string());

        let err = validate_nats_server_config(&server, &config, "").expect_err("relative queue_dir should be rejected");

        assert!(err.to_string().contains("absolute path"));
    }

    #[test]
    fn validate_pulsar_broker_config_rejects_tls_flags_without_tls_scheme() {
        let mut config = KVS::new();
        config.insert(PULSAR_TOPIC.to_string(), "events".to_string());
        config.insert(PULSAR_TLS_ALLOW_INSECURE.to_string(), "on".to_string());

        let err = validate_pulsar_broker_config("pulsar://127.0.0.1:6650", &config, "")
            .expect_err("TLS flags should require pulsar+ssl");

        assert!(err.to_string().contains("only allowed with pulsar+ssl"));
    }
}
