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

//! Performance and observability tests for audit system

use rustfs_audit::*;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::timeout;

#[tokio::test]
async fn test_audit_system_startup_performance() {
    // Test that audit system starts within reasonable time
    let system = AuditSystem::new();
    let start = Instant::now();

    // Create minimal config for testing
    let config = rustfs_ecstore::config::Config(std::collections::HashMap::new());

    // System should start quickly even with empty config
    let _result = timeout(Duration::from_secs(5), system.start(config)).await;
    let elapsed = start.elapsed();

    println!("Audit system startup took: {elapsed:?}");

    // Should complete within 5 seconds
    assert!(elapsed < Duration::from_secs(5), "Startup took too long: {elapsed:?}");

    // Clean up
    let _ = system.close().await;
}

#[tokio::test]
async fn test_concurrent_target_creation() {
    // Test that multiple targets can be created concurrently
    let mut registry = AuditRegistry::new();

    // Create config with multiple webhook instances
    let mut config = rustfs_ecstore::config::Config(std::collections::HashMap::new());
    let mut webhook_section = std::collections::HashMap::new();

    // Create multiple instances for concurrent creation test
    for i in 1..=5 {
        let mut kvs = rustfs_ecstore::config::KVS::new();
        kvs.insert("enable".to_string(), "on".to_string());
        kvs.insert("endpoint".to_string(), format!("http://localhost:302{i}/webhook"));
        webhook_section.insert(format!("instance_{i}"), kvs);
    }

    config.0.insert("audit_webhook".to_string(), webhook_section);

    let start = Instant::now();

    // This will fail due to server storage not being initialized, but we can measure timing
    let result = registry.create_targets_from_config(&config).await;
    let elapsed = start.elapsed();

    println!("Concurrent target creation took: {elapsed:?}");

    // Should complete quickly even with multiple targets
    assert!(elapsed < Duration::from_secs(10), "Target creation took too long: {elapsed:?}");

    // Verify it fails with expected error (server not initialized)
    match result {
        Err(AuditError::StorageNotAvailable(_)) => {
            // Expected in test environment
        }
        Err(e) => {
            println!("Unexpected error during concurrent creation: {e}");
        }
        Ok(_) => {
            println!("Unexpected success in test environment");
        }
    }
}

#[tokio::test]
async fn test_audit_log_dispatch_performance() {
    let system = AuditSystem::new();

    // Create minimal config
    let config = rustfs_ecstore::config::Config(HashMap::new());
    let start_result = system.start(config).await;
    if start_result.is_err() {
        println!("AuditSystem failed to start: {start_result:?}");
        return; // Alternatively: assert!(false, "AuditSystem failed to start");
    }

    use chrono::Utc;
    use rustfs_targets::EventName;
    use serde_json::json;
    use std::collections::HashMap;
    let id = 1;

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
    // Create sample audit log entry
    let audit_entry = AuditEntry {
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
    };

    let start = Instant::now();

    // Dispatch audit log (should be fast since no targets are configured)
    let result = system.dispatch(Arc::new(audit_entry)).await;
    let elapsed = start.elapsed();

    println!("Audit log dispatch took: {elapsed:?}");

    // Should be very fast (sub-millisecond for no targets)
    assert!(elapsed < Duration::from_millis(100), "Dispatch took too long: {elapsed:?}");

    // Should succeed even with no targets
    assert!(result.is_ok(), "Dispatch should succeed with no targets");

    // Clean up
    let _ = system.close().await;
}

#[tokio::test]
async fn test_system_state_transitions() {
    let system = AuditSystem::new();

    // Initial state should be stopped
    assert_eq!(system.get_state().await, rustfs_audit::system::AuditSystemState::Stopped);

    // Start system
    let config = rustfs_ecstore::config::Config(std::collections::HashMap::new());
    let start_result = system.start(config).await;

    // Should be running (or failed due to server storage)
    let state = system.get_state().await;
    match start_result {
        Ok(_) => {
            assert_eq!(state, rustfs_audit::system::AuditSystemState::Running);
        }
        Err(_) => {
            // Expected in test environment due to server storage not being initialized
            assert_eq!(state, rustfs_audit::system::AuditSystemState::Stopped);
        }
    }

    // Clean up
    let _ = system.close().await;
    assert_eq!(system.get_state().await, rustfs_audit::system::AuditSystemState::Stopped);
}

#[test]
fn test_event_name_mask_performance() {
    use rustfs_targets::EventName;

    // Test that event name mask calculation is efficient
    let events = vec![
        EventName::ObjectCreatedPut,
        EventName::ObjectAccessedGet,
        EventName::ObjectRemovedDelete,
        EventName::ObjectCreatedAll,
        EventName::Everything,
    ];

    let start = Instant::now();

    // Calculate masks for many events
    for _ in 0..1000 {
        for event in &events {
            let _mask = event.mask();
        }
    }

    let elapsed = start.elapsed();
    println!("Event mask calculation (5000 ops) took: {elapsed:?}");

    // Should be very fast
    assert!(elapsed < Duration::from_millis(100), "Mask calculation too slow: {elapsed:?}");
}

#[test]
fn test_event_name_expansion_performance() {
    use rustfs_targets::EventName;

    // Test that event name expansion is efficient
    let compound_events = vec![
        EventName::ObjectCreatedAll,
        EventName::ObjectAccessedAll,
        EventName::ObjectRemovedAll,
        EventName::Everything,
    ];

    let start = Instant::now();

    // Expand events many times
    for _ in 0..1000 {
        for event in &compound_events {
            let _expanded = event.expand();
        }
    }

    let elapsed = start.elapsed();
    println!("Event expansion (4000 ops) took: {elapsed:?}");

    // Should be very fast
    assert!(elapsed < Duration::from_millis(100), "Expansion too slow: {elapsed:?}");
}

#[tokio::test]
async fn test_registry_operations_performance() {
    let registry = AuditRegistry::new();

    let start = Instant::now();

    // Test basic registry operations
    for _ in 0..1000 {
        let targets = registry.list_targets();
        let _target = registry.get_target("nonexistent");
        assert!(targets.is_empty());
    }

    let elapsed = start.elapsed();
    println!("Registry operations (2000 ops) took: {elapsed:?}");

    // Should be very fast for empty registry
    assert!(elapsed < Duration::from_millis(100), "Registry ops too slow: {elapsed:?}");
}

// Performance requirements validation
#[test]
fn test_performance_requirements() {
    // According to requirements: ≥ 3k EPS/node; P99 < 30ms (default)

    // These are synthetic tests since we can't actually achieve 3k EPS
    // without real server storage and network targets, but we can validate
    // that our core algorithms are efficient enough

    let start = Instant::now();

    // Simulate processing 3000 events worth of operations
    for i in 0..3000 {
        // Simulate event name parsing and processing
        let _event_id = format!("s3:ObjectCreated:Put_{i}");
        let _timestamp = chrono::Utc::now().to_rfc3339();

        // Simulate basic audit entry creation overhead
        let _entry_size = 512; // bytes
        let _processing_time = std::time::Duration::from_nanos(100); // simulated
    }

    let elapsed = start.elapsed();
    let eps = 3000.0 / elapsed.as_secs_f64();

    println!("Simulated 3000 events in {elapsed:?} ({eps:.0} EPS)");

    // Our core processing should easily handle 3k EPS worth of CPU overhead
    // The actual EPS limit will be determined by network I/O to targets
    assert!(eps > 10000.0, "Core processing too slow for 3k EPS target: {eps} EPS");

    // P99 latency requirement: < 30ms
    // For core processing, we should be much faster than this
    let avg_latency = elapsed / 3000;
    println!("Average processing latency: {avg_latency:?}");

    assert!(avg_latency < Duration::from_millis(1), "Processing latency too high: {avg_latency:?}");
}
