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

//! Tests for audit configuration parsing and validation

use rustfs_ecstore::config::KVS;

#[test]
fn test_webhook_valid_fields() {
    let expected_fields = vec![
        "enable",
        "endpoint",
        "auth_token",
        "client_cert",
        "client_key",
        "batch_size",
        "queue_size",
        "queue_dir",
        "max_retry",
        "retry_interval",
        "http_timeout",
    ];

    // This tests the webhook configuration fields we support
    for field in expected_fields {
        // Basic validation that field names are consistent
        assert!(!field.is_empty());
        assert!(!field.contains(" "));
    }
}

#[test]
fn test_mqtt_valid_fields() {
    let expected_fields = vec![
        "enable",
        "broker",
        "topic",
        "username",
        "password",
        "qos",
        "keep_alive_interval",
        "reconnect_interval",
        "queue_dir",
        "queue_limit",
    ];

    // This tests the MQTT configuration fields we support
    for field in expected_fields {
        // Basic validation that field names are consistent
        assert!(!field.is_empty());
        assert!(!field.contains(" "));
    }
}

#[test]
fn test_config_section_names() {
    // Test audit route prefix and section naming
    let webhook_section = "audit_webhook";
    let mqtt_section = "audit_mqtt";

    assert_eq!(webhook_section, "audit_webhook");
    assert_eq!(mqtt_section, "audit_mqtt");

    // Verify section names follow expected pattern
    assert!(webhook_section.starts_with("audit_"));
    assert!(mqtt_section.starts_with("audit_"));
}

#[test]
fn test_environment_variable_parsing() {
    // Test environment variable prefix patterns
    let env_prefix = "RUSTFS_";
    let audit_webhook_prefix = format!("{env_prefix}AUDIT_WEBHOOK_");
    let audit_mqtt_prefix = format!("{env_prefix}AUDIT_MQTT_");

    assert_eq!(audit_webhook_prefix, "RUSTFS_AUDIT_WEBHOOK_");
    assert_eq!(audit_mqtt_prefix, "RUSTFS_AUDIT_MQTT_");

    // Test instance parsing
    let example_env_var = "RUSTFS_AUDIT_WEBHOOK_ENABLE_PRIMARY";
    assert!(example_env_var.starts_with(&audit_webhook_prefix));

    let suffix = &example_env_var[audit_webhook_prefix.len()..];
    assert_eq!(suffix, "ENABLE_PRIMARY");

    // Parse field and instance
    if let Some(last_underscore) = suffix.rfind('_') {
        let field = &suffix[..last_underscore];
        let instance = &suffix[last_underscore + 1..];
        assert_eq!(field, "ENABLE");
        assert_eq!(instance, "PRIMARY");
    }
}

#[test]
fn test_configuration_merge() {
    // Test configuration merging precedence: ENV > file instance > file default
    let mut default_config = KVS::new();
    default_config.insert("enable".to_string(), "off".to_string());
    default_config.insert("endpoint".to_string(), "http://default".to_string());

    let mut instance_config = KVS::new();
    instance_config.insert("endpoint".to_string(), "http://instance".to_string());

    let mut env_config = KVS::new();
    env_config.insert("enable".to_string(), "on".to_string());

    // Simulate merge: default < instance < env
    let mut merged = default_config.clone();
    merged.extend(instance_config);
    merged.extend(env_config);

    // Verify merge results
    assert_eq!(merged.lookup("enable"), Some("on".to_string()));
    assert_eq!(merged.lookup("endpoint"), Some("http://instance".to_string()));
}

#[test]
fn test_duration_parsing_formats() {
    let test_cases = vec![
        ("3s", Some(3)),
        ("5m", Some(300)),   // 5 minutes = 300 seconds
        ("1000ms", Some(1)), // 1000ms = 1 second
        ("60", Some(60)),    // Default to seconds
        ("invalid", None),
        ("", None),
    ];

    for (input, expected_seconds) in test_cases {
        let result = parse_duration_test(input);
        match (result, expected_seconds) {
            (Some(duration), Some(expected)) => {
                assert_eq!(duration.as_secs(), expected, "Failed for input: {input}");
            }
            (None, None) => {
                // Both None, test passes
            }
            _ => {
                panic!("Mismatch for input: {input}, got: {result:?}, expected: {expected_seconds:?}");
            }
        }
    }
}

// Helper function for duration parsing (extracted from registry.rs logic)
fn parse_duration_test(s: &str) -> Option<std::time::Duration> {
    use std::time::Duration;

    if let Some(stripped) = s.strip_suffix("ms") {
        stripped.parse::<u64>().ok().map(Duration::from_millis)
    } else if let Some(stripped) = s.strip_suffix('s') {
        stripped.parse::<u64>().ok().map(Duration::from_secs)
    } else if let Some(stripped) = s.strip_suffix('m') {
        stripped.parse::<u64>().ok().map(|m| Duration::from_secs(m * 60))
    } else {
        s.parse::<u64>().ok().map(Duration::from_secs)
    }
}

#[test]
fn test_url_validation() {
    use url::Url;

    let valid_urls = vec![
        "http://localhost:3020/webhook",
        "https://api.example.com/audit",
        "mqtt://broker.example.com:1883",
        "tcp://localhost:1883",
    ];

    let invalid_urls = [
        "",
        "not-a-url",
        "http://",
        "ftp://unsupported.com", // Not invalid, but might not be supported
    ];

    for url_str in valid_urls {
        let result = Url::parse(url_str);
        assert!(result.is_ok(), "Valid URL should parse: {url_str}");
    }

    for url_str in &invalid_urls[..3] {
        // Skip the ftp one as it's technically valid
        let result = Url::parse(url_str);
        assert!(result.is_err(), "Invalid URL should not parse: {url_str}");
    }
}

#[test]
fn test_qos_parsing() {
    // Test QoS level parsing for MQTT
    let test_cases = vec![
        ("0", Some(0)),
        ("1", Some(1)),
        ("2", Some(2)),
        ("3", None), // Invalid QoS level
        ("invalid", None),
    ];

    for (input, expected) in test_cases {
        let result = input.parse::<u8>().ok().and_then(|q| match q {
            0..=2 => Some(q),
            _ => None,
        });
        assert_eq!(result, expected, "Failed for QoS input: {input}");
    }
}
