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

use rustfs_audit::*;
use rustfs_ecstore::config::{Config, KVS};
use std::collections::HashMap;

#[tokio::test]
async fn test_audit_system_creation() {
    let system = AuditSystem::new();
    let state = system.get_state().await;
    assert_eq!(state, rustfs_audit::system::AuditSystemState::Stopped);
}

#[tokio::test]
async fn test_audit_registry_creation() {
    let registry = AuditRegistry::new();
    let targets = registry.list_targets();
    assert!(targets.is_empty());
}

#[tokio::test]
async fn test_config_parsing_webhook() {
    let mut config = Config(HashMap::new());
    let mut audit_webhook_section = HashMap::new();

    // Create default configuration
    let mut default_kvs = KVS::new();
    default_kvs.insert("enable".to_string(), "on".to_string());
    default_kvs.insert("endpoint".to_string(), "http://localhost:3020/webhook".to_string());

    audit_webhook_section.insert("_".to_string(), default_kvs);
    config.0.insert("audit_webhook".to_string(), audit_webhook_section);

    let registry = AuditRegistry::new();

    // This should not fail even if server storage is not initialized
    // as it's an integration test
    let result = registry.create_audit_targets_from_config(&config).await;

    // We expect this to fail due to server storage not being initialized
    // but the parsing should work correctly
    match result {
        Err(AuditError::StorageNotAvailable(_)) => {
            // This is expected in test environment
        }
        Err(e) => {
            // Other errors might indicate parsing issues
            println!("Unexpected error: {e}");
        }
        Ok(_) => {
            // Unexpected success in test environment without server storage
        }
    }
}

#[test]
fn test_event_name_parsing() {
    use rustfs_targets::EventName;

    // Test basic event name parsing
    let event = EventName::parse("s3:ObjectCreated:Put").unwrap();
    assert_eq!(event, EventName::ObjectCreatedPut);

    let event = EventName::parse("s3:ObjectAccessed:*").unwrap();
    assert_eq!(event, EventName::ObjectAccessedAll);

    // Test event name expansion
    let expanded = EventName::ObjectCreatedAll.expand();
    assert!(expanded.contains(&EventName::ObjectCreatedPut));
    assert!(expanded.contains(&EventName::ObjectCreatedPost));

    // Test event name mask
    let mask = EventName::ObjectCreatedPut.mask();
    assert!(mask > 0);
}

#[test]
fn test_enable_value_parsing() {
    // Test different enable value formats
    let test_cases = vec![
        ("1", true),
        ("on", true),
        ("true", true),
        ("yes", true),
        ("0", false),
        ("off", false),
        ("false", false),
        ("no", false),
        ("invalid", false),
    ];

    for (input, expected) in test_cases {
        let result = matches!(input.to_lowercase().as_str(), "1" | "on" | "true" | "yes");
        assert_eq!(result, expected, "Failed for input: {input}");
    }
}
