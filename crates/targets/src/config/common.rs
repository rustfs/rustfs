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
use crate::target::nats::validate_jetstream_settings;
use crate::target::pulsar::validate_pulsar_broker;
use async_nats::ServerAddr;
use rustfs_config::server_config::KVS;
use rustfs_config::{
    DEFAULT_DELIMITER, ENABLE_KEY, EnableState, NATS_CREDENTIALS_FILE, NATS_JETSTREAM_ACK_TIMEOUT_SECS, NATS_JETSTREAM_ENABLE,
    NATS_JETSTREAM_STREAM_NAME, NATS_PASSWORD, NATS_QUEUE_DIR, NATS_SUBJECT, NATS_TLS_CA, NATS_TLS_CLIENT_CERT,
    NATS_TLS_CLIENT_KEY, NATS_TOKEN, NATS_USERNAME, PULSAR_AUTH_TOKEN, PULSAR_PASSWORD, PULSAR_QUEUE_DIR, PULSAR_TLS_CA,
    PULSAR_TOPIC, PULSAR_USERNAME,
};
use rustfs_utils::egress::OutboundPolicy;
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

pub(super) fn is_target_enabled(config: &KVS) -> Result<bool, TargetError> {
    let Some(value) = config.lookup(ENABLE_KEY) else {
        return Ok(false);
    };
    EnableState::from_str(value.as_str())
        .map(EnableState::is_enabled)
        .map_err(|_| TargetError::Configuration(format!("Invalid {ENABLE_KEY} value '{value}'")))
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

    let jetstream_enable = parse_jetstream_enable(config)?.unwrap_or(false);
    let jetstream_stream_name = config.lookup(NATS_JETSTREAM_STREAM_NAME).unwrap_or_default();
    let jetstream_ack_timeout_secs = parse_jetstream_ack_timeout_secs(config)?;
    validate_jetstream_settings(jetstream_enable, &jetstream_stream_name, &queue_dir, jetstream_ack_timeout_secs)?;

    let _ = server;
    Ok(())
}

/// Parses the jetstream_enable flag, distinguishing an absent or blank key (None, disabled) from a
/// key that is present but carries an unparsable value, which is a configuration error rather than
/// a silent disable. Unwrapping an unparsable value to disabled would drop the durability guarantee
/// the operator asked for with no signal.
pub(super) fn parse_jetstream_enable(config: &KVS) -> Result<Option<bool>, TargetError> {
    match config.lookup(NATS_JETSTREAM_ENABLE) {
        Some(value) if !value.trim().is_empty() => parse_target_bool(Some(value.as_str())).map(Some).ok_or_else(|| {
            TargetError::Configuration(format!(
                "Invalid {NATS_JETSTREAM_ENABLE} boolean value: {value} (accepted values include on, off, true, false, yes, no, 1, 0)"
            ))
        }),
        _ => Ok(None),
    }
}

pub(super) fn parse_jetstream_ack_timeout_secs(config: &KVS) -> Result<Option<u64>, TargetError> {
    match config.lookup(NATS_JETSTREAM_ACK_TIMEOUT_SECS) {
        Some(value) if !value.trim().is_empty() => {
            let secs = value.trim().parse::<u64>().map_err(|_| {
                TargetError::Configuration(format!("{NATS_JETSTREAM_ACK_TIMEOUT_SECS} must be a whole number of seconds"))
            })?;
            Ok(Some(secs))
        }
        _ => Ok(None),
    }
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

    if !tls_ca.is_empty() && !Path::new(&tls_ca).is_absolute() {
        return Err(TargetError::Configuration("Pulsar tls_ca must be an absolute path".to_string()));
    }
    // A CA bundle is TLS trust material a plaintext `pulsar://` broker silently
    // ignores, so treat it as a genuine misconfiguration. The
    // `tls_allow_insecure` / `tls_hostname_verification` toggles, however, are
    // inert on a non-TLS broker (the Pulsar client only honours them for
    // `pulsar+ssl`); rejecting non-default values would leave a persisted
    // target permanently offline after a restart even though the flags do
    // nothing — see issue #4796.
    if url.scheme() != "pulsar+ssl" && !tls_ca.is_empty() {
        return Err(TargetError::Configuration(
            "Pulsar tls_ca is only allowed with pulsar+ssl brokers".to_string(),
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
    Url::parse(value).map_err(|e| TargetError::Configuration(format!("Invalid {field_label}: {e}")))
}

pub(super) fn validate_outbound_http_url(value: &Url, field_label: &str) -> Result<(), TargetError> {
    let policy =
        OutboundPolicy::from_env_cached().map_err(|err| TargetError::Configuration(format!("invalid outbound policy: {err}")))?;
    policy
        .validate_url(value)
        .map_err(|e| TargetError::Configuration(format!("{field_label} is not allowed: {e}")))
}

#[cfg(test)]
mod tests {
    use super::{parse_jetstream_enable, parse_url, validate_nats_server_config, validate_pulsar_broker_config};
    use async_nats::ServerAddr;
    use rustfs_config::server_config::KVS;
    use rustfs_config::{
        NATS_JETSTREAM_ACK_TIMEOUT_SECS, NATS_JETSTREAM_ENABLE, NATS_JETSTREAM_STREAM_NAME, NATS_PASSWORD, NATS_QUEUE_DIR,
        NATS_SUBJECT, NATS_TOKEN, NATS_USERNAME, PULSAR_TLS_ALLOW_INSECURE, PULSAR_TLS_CA, PULSAR_TLS_HOSTNAME_VERIFICATION,
        PULSAR_TOPIC,
    };
    use std::str::FromStr;

    fn nats_server() -> ServerAddr {
        ServerAddr::from_str("nats://127.0.0.1:4222").expect("valid nats address")
    }

    #[test]
    fn parse_url_error_does_not_echo_the_configured_value() {
        let err = parse_url("not a URL containing secret-token", "endpoint URL").expect_err("invalid URL should fail");
        assert!(!err.to_string().contains("secret-token"));
    }

    // Absolute on Linux, macOS, and Windows. temp_dir needs no filesystem to exist for a
    // validation-only test, and Path::is_absolute stays true across platforms.
    fn nats_queue_dir() -> String {
        std::env::temp_dir().join("rustfs-nats-queue").to_string_lossy().into_owned()
    }

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
    fn validate_nats_server_config_rejects_jetstream_without_stream() {
        let mut config = KVS::new();
        config.insert(NATS_SUBJECT.to_string(), "events".to_string());
        config.insert(NATS_QUEUE_DIR.to_string(), nats_queue_dir());
        config.insert(NATS_JETSTREAM_ENABLE.to_string(), "on".to_string());

        let err = validate_nats_server_config(&nats_server(), &config, "")
            .expect_err("enabled jetstream without a stream should be rejected");
        assert!(err.to_string().contains(NATS_JETSTREAM_STREAM_NAME));
    }

    #[test]
    fn validate_nats_server_config_rejects_jetstream_without_queue_dir() {
        let mut config = KVS::new();
        config.insert(NATS_SUBJECT.to_string(), "events".to_string());
        config.insert(NATS_JETSTREAM_ENABLE.to_string(), "on".to_string());
        config.insert(NATS_JETSTREAM_STREAM_NAME.to_string(), "RUSTFS_EVENTS".to_string());

        let err = validate_nats_server_config(&nats_server(), &config, "")
            .expect_err("enabled jetstream without a queue_dir should be rejected");
        assert!(err.to_string().contains(NATS_QUEUE_DIR));
    }

    #[test]
    fn validate_nats_server_config_rejects_out_of_range_ack_timeout() {
        let mut config = KVS::new();
        config.insert(NATS_SUBJECT.to_string(), "events".to_string());
        config.insert(NATS_JETSTREAM_ACK_TIMEOUT_SECS.to_string(), "5".to_string());

        let err =
            validate_nats_server_config(&nats_server(), &config, "").expect_err("out-of-range ack timeout should be rejected");
        assert!(err.to_string().contains("between"));
    }

    #[test]
    fn validate_nats_server_config_accepts_enabled_jetstream_with_stream_and_queue() {
        let mut config = KVS::new();
        config.insert(NATS_SUBJECT.to_string(), "events".to_string());
        config.insert(NATS_QUEUE_DIR.to_string(), nats_queue_dir());
        config.insert(NATS_JETSTREAM_ENABLE.to_string(), "on".to_string());
        config.insert(NATS_JETSTREAM_STREAM_NAME.to_string(), "RUSTFS_EVENTS".to_string());

        validate_nats_server_config(&nats_server(), &config, "").expect("a stream name and queue dir accept enabling jetstream");
    }

    #[test]
    fn validate_nats_server_config_audit_parity_matches_notify() {
        let mut config = KVS::new();
        config.insert(NATS_SUBJECT.to_string(), "events".to_string());
        config.insert(NATS_QUEUE_DIR.to_string(), nats_queue_dir());
        config.insert(NATS_JETSTREAM_ENABLE.to_string(), "on".to_string());

        // The same invalid combination (enabled, stream name missing) is rejected identically on the
        // notify and the audit path, proving one shared validator with no drift.
        let notify = validate_nats_server_config(&nats_server(), &config, "/opt/rustfs/events").expect_err("notify rejects");
        let audit = validate_nats_server_config(&nats_server(), &config, "/opt/rustfs/audit").expect_err("audit rejects");
        assert_eq!(notify.to_string(), audit.to_string());
    }

    #[test]
    fn validate_nats_server_config_accepts_jetstream_absent() {
        let mut config = KVS::new();
        config.insert(NATS_SUBJECT.to_string(), "events".to_string());
        config.insert(NATS_QUEUE_DIR.to_string(), nats_queue_dir());

        validate_nats_server_config(&nats_server(), &config, "").expect("baseline config validates");
    }

    #[test]
    fn parse_jetstream_enable_accepts_the_standard_boolean_forms() {
        // An absent or blank key is None, legitimately disabled.
        assert_eq!(parse_jetstream_enable(&KVS::new()).unwrap(), None, "an absent key parses as unset");
        let mut blank = KVS::new();
        blank.insert(NATS_JETSTREAM_ENABLE.to_string(), "  ".to_string());
        assert_eq!(parse_jetstream_enable(&blank).unwrap(), None, "a blank value parses as unset");

        for (value, expected) in [
            ("on", true),
            ("off", false),
            ("true", true),
            ("false", false),
            ("yes", true),
            ("no", false),
            ("1", true),
            ("0", false),
            ("enabled", true),
            ("disabled", false),
        ] {
            let mut config = KVS::new();
            config.insert(NATS_JETSTREAM_ENABLE.to_string(), value.to_string());
            assert_eq!(
                parse_jetstream_enable(&config).unwrap(),
                Some(expected),
                "value {value} parses as {expected}"
            );
        }
    }

    #[test]
    fn parse_jetstream_enable_rejects_an_unparsable_value_naming_the_key() {
        // A typo must fail validation rather than silently disabling the durability guarantee.
        for value in ["y", "t", "enabl", "10"] {
            let mut config = KVS::new();
            config.insert(NATS_JETSTREAM_ENABLE.to_string(), value.to_string());
            let err = parse_jetstream_enable(&config).expect_err("an unparsable value is rejected");
            assert!(
                err.to_string().contains(NATS_JETSTREAM_ENABLE),
                "the error names the key for value {value}: {err}"
            );
        }
    }

    #[test]
    fn validate_nats_server_config_rejects_an_invalid_jetstream_enable_value() {
        let mut config = KVS::new();
        config.insert(NATS_SUBJECT.to_string(), "events".to_string());
        config.insert(NATS_QUEUE_DIR.to_string(), nats_queue_dir());
        config.insert(NATS_JETSTREAM_ENABLE.to_string(), "y".to_string());

        let err = validate_nats_server_config(&nats_server(), &config, "")
            .expect_err("an unparsable enable value fails validation instead of passing as disabled");
        assert!(err.to_string().contains(NATS_JETSTREAM_ENABLE), "the error names the key: {err}");
    }

    #[test]
    fn validate_pulsar_broker_config_rejects_tls_ca_without_tls_scheme() {
        let mut config = KVS::new();
        config.insert(PULSAR_TOPIC.to_string(), "events".to_string());
        config.insert(PULSAR_TLS_CA.to_string(), "/etc/ssl/certs/ca.pem".to_string());

        let err = validate_pulsar_broker_config("pulsar://127.0.0.1:6650", &config, "")
            .expect_err("a CA bundle should require pulsar+ssl");

        assert!(err.to_string().contains("only allowed with pulsar+ssl"));
    }

    #[test]
    fn validate_pulsar_broker_config_accepts_inert_tls_toggles_without_tls_scheme() {
        // The TLS toggles are no-ops on a plaintext broker, so a persisted
        // config carrying non-default values must still load — otherwise the
        // target is stuck offline after a restart (issue #4796).
        let mut config = KVS::new();
        config.insert(PULSAR_TOPIC.to_string(), "events".to_string());
        config.insert(PULSAR_TLS_ALLOW_INSECURE.to_string(), "on".to_string());
        config.insert(PULSAR_TLS_HOSTNAME_VERIFICATION.to_string(), "off".to_string());

        validate_pulsar_broker_config("pulsar://127.0.0.1:6650", &config, "")
            .expect("inert TLS toggles must not fail a plaintext broker");
    }
}
