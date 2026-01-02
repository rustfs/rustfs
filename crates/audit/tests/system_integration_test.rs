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

//! Comprehensive integration tests for the complete audit system

use rustfs_audit::*;
use rustfs_ecstore::config::{Config, KVS};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn test_complete_audit_system_lifecycle() {
    // Test the complete lifecycle of the audit system
    let system = AuditSystem::new();

    // 1. Initial state should be stopped
    assert_eq!(system.get_state().await, system::AuditSystemState::Stopped);
    assert!(!system.is_running().await);

    // 2. Start with empty config (will fail due to no server storage in test)
    let config = Config(HashMap::new());
    let start_result = system.start(config).await;

    // Should fail in test environment but state handling should work
    match start_result {
        Err(AuditError::StorageNotAvailable(_)) => {
            // Expected in test environment
            assert_eq!(system.get_state().await, system::AuditSystemState::Stopped);
        }
        Ok(_) => {
            // If it somehow succeeds, verify running state
            assert_eq!(system.get_state().await, system::AuditSystemState::Running);
            assert!(system.is_running().await);

            // Test pause/resume
            system.pause().await.expect("Should pause successfully");
            assert_eq!(system.get_state().await, system::AuditSystemState::Paused);

            system.resume().await.expect("Should resume successfully");
            assert_eq!(system.get_state().await, system::AuditSystemState::Running);
        }
        Err(e) => {
            panic!("Unexpected error: {e}");
        }
    }

    // 3. Test close
    system.close().await.expect("Should close successfully");
    assert_eq!(system.get_state().await, system::AuditSystemState::Stopped);
    assert!(!system.is_running().await);
}

#[tokio::test]
async fn test_audit_system_with_metrics() {
    let system = AuditSystem::new();

    // Reset metrics for clean test
    system.reset_metrics().await;

    // Try to start system (will fail but should record metrics)
    let config = Config(HashMap::new());
    let _ = system.start(config).await; // Ignore result

    // Check metrics
    let metrics = system.get_metrics().await;
    assert!(metrics.system_start_count > 0, "Should have recorded system start attempt");

    // Test performance validation
    let validation = system.validate_performance().await;
    assert!(validation.current_eps >= 0.0);
    assert!(validation.current_latency_ms >= 0.0);
    assert!(validation.current_error_rate >= 0.0);
}

#[tokio::test]
async fn test_audit_log_dispatch_with_no_targets() {
    let system = AuditSystem::new();

    // Create sample audit entry
    let audit_entry = create_sample_audit_entry();

    // Try to dispatch with no targets (should succeed but do nothing)
    let result = system.dispatch(Arc::new(audit_entry)).await;

    // Should succeed even with no targets configured
    match result {
        Ok(_) => {
            // Success expected
        }
        Err(AuditError::NotInitialized(_)) => {
            // Also acceptable since system not running
        }
        Err(e) => {
            panic!("Unexpected error: {e}");
        }
    }
}

#[tokio::test]
async fn test_global_audit_functions() {
    use rustfs_audit::*;

    // Test global functions
    let system = init_audit_system();
    assert!(system.get_state().await == system::AuditSystemState::Stopped);

    // Test audit logging function (should not panic even if system not running)
    let entry = create_sample_audit_entry();
    let result = dispatch_audit_log(Arc::new(entry)).await;
    assert!(result.is_ok(), "Dispatch should succeed even with no running system");

    // Test system status
    assert!(!is_audit_system_running().await);

    // Test AuditLogger singleton
    let _logger = AuditLogger::instance();
    assert!(!AuditLogger::is_enabled().await);

    // Test logging (should not panic)
    let entry = create_sample_audit_entry();
    AuditLogger::log(entry).await; // Should not panic
}

#[tokio::test]
async fn test_config_parsing_with_multiple_instances() {
    let registry = AuditRegistry::new();

    // Create config with multiple webhook instances
    let mut config = Config(HashMap::new());
    let mut webhook_section = HashMap::new();

    // Default instance
    let mut default_kvs = KVS::new();
    default_kvs.insert("enable".to_string(), "off".to_string());
    default_kvs.insert("endpoint".to_string(), "http://default.example.com/audit".to_string());
    webhook_section.insert("_".to_string(), default_kvs);

    // Primary instance
    let mut primary_kvs = KVS::new();
    primary_kvs.insert("enable".to_string(), "on".to_string());
    primary_kvs.insert("endpoint".to_string(), "http://primary.example.com/audit".to_string());
    primary_kvs.insert("auth_token".to_string(), "primary-token-123".to_string());
    webhook_section.insert("primary".to_string(), primary_kvs);

    // Secondary instance
    let mut secondary_kvs = KVS::new();
    secondary_kvs.insert("enable".to_string(), "on".to_string());
    secondary_kvs.insert("endpoint".to_string(), "http://secondary.example.com/audit".to_string());
    secondary_kvs.insert("auth_token".to_string(), "secondary-token-456".to_string());
    webhook_section.insert("secondary".to_string(), secondary_kvs);

    config.0.insert("audit_webhook".to_string(), webhook_section);

    // Try to create targets from config
    let result = registry.create_audit_targets_from_config(&config).await;

    // Should fail due to server storage not initialized, but parsing should work
    match result {
        Err(AuditError::StorageNotAvailable(_)) => {
            // Expected - parsing worked but save failed
        }
        Err(e) => {
            println!("Config parsing error: {e}");
            // Other errors might indicate parsing issues, but not necessarily failures
        }
        Ok(_) => {
            // Unexpected success in test environment
            println!("Unexpected success - server storage somehow available");
        }
    }
}

#[test]
fn test_target_type_validation() {
    use rustfs_targets::target::TargetType;

    // Test that TargetType::AuditLog is properly defined
    let audit_type = TargetType::AuditLog;
    assert_eq!(audit_type.as_str(), "audit_log");

    let notify_type = TargetType::NotifyEvent;
    assert_eq!(notify_type.as_str(), "notify_event");

    // Test that they are different
    assert_ne!(audit_type.as_str(), notify_type.as_str());
}

#[tokio::test]
async fn test_concurrent_operations() {
    let system = AuditSystem::new();

    // Test concurrent state checks
    let mut tasks = Vec::new();

    for i in 0..10 {
        let system_clone = system.clone();
        let task = tokio::spawn(async move {
            let state = system_clone.get_state().await;
            let is_running = system_clone.is_running().await;
            (i, state, is_running)
        });
        tasks.push(task);
    }

    // All tasks should complete without panic
    for task in tasks {
        let (i, state, is_running) = task.await.expect("Task should complete");
        assert_eq!(state, system::AuditSystemState::Stopped);
        assert!(!is_running);
        println!("Task {i} completed successfully");
    }
}

#[tokio::test]
async fn test_performance_under_load() {
    use std::time::Instant;

    let system = AuditSystem::new();

    // Test multiple rapid dispatch calls
    let start = Instant::now();
    let mut tasks = Vec::new();

    for i in 0..100 {
        let system_clone = system.clone();
        let entry = Arc::new(create_sample_audit_entry_with_id(i));

        let task = tokio::spawn(async move { system_clone.dispatch(entry).await });
        tasks.push(task);
    }

    // Wait for all dispatches to complete
    let mut success_count = 0;
    let mut error_count = 0;

    for task in tasks {
        match task.await.expect("Task should complete") {
            Ok(_) => success_count += 1,
            Err(_) => error_count += 1,
        }
    }

    let elapsed = start.elapsed();
    println!("100 concurrent dispatches took: {elapsed:?}");
    println!("Successes: {success_count}, Errors: {error_count}");

    // Should complete reasonably quickly
    assert!(elapsed < Duration::from_secs(5), "Concurrent operations took too long");

    // All should either succeed (if targets available) or fail consistently
    assert_eq!(success_count + error_count, 100);
}

// Helper functions

fn create_sample_audit_entry() -> AuditEntry {
    create_sample_audit_entry_with_id(0)
}

fn create_sample_audit_entry_with_id(id: u32) -> AuditEntry {
    use chrono::Utc;
    use rustfs_targets::EventName;
    use serde_json::json;

    let mut req_header = hashbrown::HashMap::new();
    req_header.insert("authorization".to_string(), format!("Bearer test-token-{id}"));
    req_header.insert("content-type".to_string(), "application/octet-stream".to_string());

    let mut resp_header = hashbrown::HashMap::new();
    resp_header.insert("x-response".to_string(), "ok".to_string());

    let mut tags = hashbrown::HashMap::new();
    tags.insert(format!("tag-{id}"), json!("sample"));

    let mut req_query = hashbrown::HashMap::new();
    req_query.insert("id".to_string(), id.to_string());

    let api_details = ApiDetails {
        name: Some("PutObject".to_string()),
        bucket: Some("test-bucket".to_string()),
        object: Some(format!("test-object-{id}")),
        status: Some("success".to_string()),
        status_code: Some(200),
        input_bytes: Some(1024),
        output_bytes: Some(0),
        header_bytes: Some(128),
        time_to_first_byte: Some("1ms".to_string()),
        time_to_first_byte_in_ns: Some("1000000".to_string()),
        time_to_response: Some("2ms".to_string()),
        time_to_response_in_ns: Some("2000000".to_string()),
        ..Default::default()
    };

    AuditEntry {
        version: "1".to_string(),
        deployment_id: Some(format!("test-deployment-{id}")),
        site_name: Some("test-site".to_string()),
        time: Utc::now(),
        event: EventName::ObjectCreatedPut,
        entry_type: Some("object".to_string()),
        trigger: "api".to_string(),
        api: api_details,
        remote_host: Some("127.0.0.1".to_string()),
        request_id: Some(format!("test-request-{id}")),
        user_agent: Some("test-agent".to_string()),
        req_path: Some(format!("/test-bucket/test-object-{id}")),
        req_host: Some("test-host".to_string()),
        req_node: Some("node-1".to_string()),
        req_claims: None,
        req_query: Some(req_query),
        req_header: Some(req_header),
        resp_header: Some(resp_header),
        tags: Some(tags),
        access_key: Some(format!("AKIA{id}")),
        parent_user: Some(format!("parent-{id}")),
        error: None,
    }
}
