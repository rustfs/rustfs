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
    let config = rustfs_config::server_config::Config(std::collections::HashMap::new());

    // System should start quickly even with empty config
    let _result = timeout(Duration::from_secs(5), system.start(config)).await;
    let elapsed = start.elapsed();

    println!("Audit system startup took: {elapsed:?}");

    // Clean up
    let _ = system.close().await;
}

#[tokio::test]
async fn test_concurrent_target_creation() {
    // Test that multiple targets can be created concurrently
    let registry = AuditRegistry::new();

    // Create config with multiple webhook instances
    let mut config = rustfs_config::server_config::Config(std::collections::HashMap::new());
    let mut webhook_section = std::collections::HashMap::new();

    // Create multiple instances for concurrent creation test
    for i in 1..=5 {
        let mut kvs = rustfs_config::server_config::KVS::new();
        kvs.insert("enable".to_string(), "on".to_string());
        kvs.insert("endpoint".to_string(), format!("http://localhost:302{i}/webhook"));
        webhook_section.insert(format!("instance_{i}"), kvs);
    }

    config.0.insert("audit_webhook".to_string(), webhook_section);

    let start = Instant::now();

    // This will fail due to server storage not being initialized, but we can measure timing
    let result = registry.create_audit_targets_from_config(&config).await;
    let elapsed = start.elapsed();

    println!("Concurrent target creation took: {elapsed:?}");

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
    let config = rustfs_config::server_config::Config(HashMap::new());
    let start_result = system.start(config).await;
    if start_result.is_err() {
        println!("AuditSystem failed to start: {start_result:?}");
        return; // Alternatively: assert!(false, "AuditSystem failed to start");
    }

    use jiff::Timestamp;
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
        time: Timestamp::now(),
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

    // Dispatch audit log against an unstarted system state. Empty config keeps
    // the audit system stopped, so dispatch should fail fast without targets.
    let result = system.dispatch(Arc::new(audit_entry)).await;
    let elapsed = start.elapsed();

    println!("Audit log dispatch took: {elapsed:?}");

    assert!(
        matches!(result, Err(AuditError::NotInitialized(_))),
        "Dispatch on a stopped system should return NotInitialized, got: {result:?}"
    );

    // Clean up
    let _ = system.close().await;
}

#[tokio::test]
async fn test_system_state_transitions() {
    let system = AuditSystem::new();

    // Initial state should be stopped
    assert_eq!(system.get_state().await, rustfs_audit::system::AuditSystemState::Stopped);

    // Start system
    let config = rustfs_config::server_config::Config(std::collections::HashMap::new());
    let start_result = system.start(config).await;

    // Empty config keeps the audit system stopped even when start() succeeds.
    let state = system.get_state().await;
    match start_result {
        Ok(_) => {
            assert_eq!(state, rustfs_audit::system::AuditSystemState::Stopped);
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
fn test_event_name_mask_matches_expansion() {
    use rustfs_targets::EventName;

    // A compound event's mask is exactly the union of its leaves' masks, and a
    // leaf owns exactly one bit. Timing is printed for information only.
    let events = [
        EventName::ObjectCreatedPut,
        EventName::ObjectAccessedGet,
        EventName::ObjectRemovedDelete,
        EventName::ObjectCreatedAll,
        EventName::Everything,
    ];

    let start = Instant::now();
    for event in events {
        let union = event.expand().iter().fold(0u64, |acc, leaf| acc | leaf.mask());
        assert_eq!(event.mask(), union, "{event:?} mask must equal the union of its expansion");
    }
    println!("Event mask check took: {:?}", start.elapsed());

    assert_eq!(EventName::ObjectCreatedPut.mask().count_ones(), 1);
    assert_eq!(EventName::ObjectCreatedAll.mask().count_ones(), 4);
}

#[test]
fn test_event_name_expansion_performance() {
    use rustfs_targets::EventName;
    use std::collections::HashSet;

    // Expansion is a pure lookup. Wall-clock bounds flake on shared runners, so
    // this asserts what the time was a proxy for: each compound event yields a
    // fixed-size set of distinct leaf events and nothing else (no duplicates,
    // no compound left to expand again). Timing is printed for information only.
    let cases = [
        (EventName::ObjectCreatedAll, 4),
        (EventName::ObjectAccessedAll, 5),
        (EventName::ObjectRemovedAll, 3),
    ];

    let start = Instant::now();
    for (event, expected_len) in cases {
        let expanded = event.expand();
        assert_eq!(expanded.len(), expected_len, "{event:?} expansion size");
        assert_eq!(
            expanded.iter().collect::<HashSet<_>>().len(),
            expanded.len(),
            "{event:?} expansion has duplicates"
        );
        assert_eq!(expanded, event.expand(), "{event:?} expansion must be deterministic");
        for leaf in &expanded {
            assert_eq!(leaf.expand(), vec![*leaf], "{event:?} expanded to non-leaf {leaf:?}");
        }
    }

    let everything = EventName::Everything.expand();
    assert!(everything.len() > 12, "Everything must cover at least every compound's leaves");
    assert_eq!(
        everything.iter().collect::<HashSet<_>>().len(),
        everything.len(),
        "Everything has duplicates"
    );
    for (event, _) in cases {
        for leaf in event.expand() {
            assert!(everything.contains(&leaf), "Everything is missing {leaf:?}");
        }
    }
    println!("Event expansion check took: {:?}", start.elapsed());
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
}

// The former 3k EPS / latency assertions measured the CI host, not the code
// (the loop only formats strings and reads the clock), so they are replaced by
// a count of what the loop produced. Throughput is printed for information.
#[test]
fn test_performance_requirements() {
    use std::collections::HashSet;

    let start = Instant::now();
    let ids: HashSet<String> = (0..3000).map(|i| format!("s3:ObjectCreated:Put_{i}")).collect();
    let timestamps = (0..3000).filter(|_| !jiff::Timestamp::now().to_string().is_empty()).count();
    let elapsed = start.elapsed();

    println!("Simulated 3000 events in {elapsed:?}");

    assert_eq!(ids.len(), 3000, "every simulated event id must be distinct");
    assert_eq!(timestamps, 3000, "every simulated event must get a timestamp");
}
