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

//! Comprehensive integration tests for the audit system
//!
//! Tests cover:
//! - Concurrent dispatch to multiple targets
//! - Hot reload functionality
//! - Error isolation and recovery
//! - Performance characteristics
//! - EventName support

use rustfs_audit::{
    AuditConfig, AuditEntry, AuditSystem, AuditTargetConfig, DefaultAuditTargetFactory, PerformanceConfig, RedactionConfig,
    TargetRegistry, s3_events,
};
use rustfs_targets::EventName;
use serde_json::json;
use std::collections::HashMap;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant};
use tokio::time::timeout;
use tracing_subscriber;

/// Mock HTTP server for testing webhook targets
struct MockWebhookServer {
    port: u16,
    received_messages: Arc<Mutex<Vec<String>>>,
    fail_requests: Arc<std::sync::atomic::AtomicBool>,
}

impl MockWebhookServer {
    async fn new() -> Self {
        let received_messages = Arc::new(Mutex::new(Vec::new()));
        let fail_requests = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let messages_clone = received_messages.clone();
        let fail_clone = fail_requests.clone();

        // Start simple HTTP server
        let port = 0; // Let OS choose port
        // Note: In a real test, we'd start an actual HTTP server
        // For now, we'll simulate the behavior

        Self {
            port: 8080, // Fixed for testing
            received_messages,
            fail_requests,
        }
    }

    fn get_received_messages(&self) -> Vec<String> {
        self.received_messages.lock().unwrap().clone()
    }

    fn set_fail_requests(&self, fail: bool) {
        self.fail_requests.store(fail, Ordering::Relaxed);
    }

    fn get_port(&self) -> u16 {
        self.port
    }
}

/// Test EventName parsing, expansion, and masking
#[tokio::test]
async fn test_event_name_support() {
    // Test various S3 events
    let events = vec![
        "s3:ObjectCreated:Put",
        "s3:ObjectCreated:Post",
        "s3:ObjectCreated:Copy",
        "s3:ObjectCreated:CompleteMultipartUpload",
        "s3:ObjectRemoved:Delete",
        "s3:ObjectRemoved:DeleteMarkerCreated",
        "s3:BucketCreated",
        "s3:BucketRemoved",
    ];

    for event_str in events {
        // Test parsing
        let event_name = EventName::parse(event_str);
        assert!(event_name.is_ok(), "Failed to parse event: {}", event_str);

        let event_name = event_name.unwrap();

        // Test as_str() round-trip
        assert_eq!(event_name.as_str(), event_str);

        // Test expand() - should include patterns
        let expanded = event_name.expand();
        assert!(!expanded.is_empty(), "Expand should return non-empty for {}", event_str);
        assert!(expanded.contains(event_str), "Expand should include original event");

        // Test mask() - should provide masked version
        let masked = event_name.mask();
        assert!(!masked.is_empty(), "Mask should return non-empty for {}", event_str);
    }
}

/// Test concurrent dispatch to multiple targets with failure isolation
#[tokio::test]
async fn test_concurrent_dispatch_with_failure_isolation() {
    let _ = tracing_subscriber::fmt::try_init();

    let factory = Arc::new(DefaultAuditTargetFactory::new());
    let registry = Arc::new(TargetRegistry::new(factory));

    // Add multiple targets - some will fail
    let webhook_config = AuditTargetConfig {
        id: "webhook-success".to_string(),
        target_type: "webhook".to_string(),
        enabled: true,
        args: json!({
            "url": "http://httpbin.org/post", // This will work
            "timeout_ms": 5000,
            "retries": 1
        }),
    };

    let webhook_fail_config = AuditTargetConfig {
        id: "webhook-fail".to_string(),
        target_type: "webhook".to_string(),
        enabled: true,
        args: json!({
            "url": "http://nonexistent-domain-12345.com/webhook", // This will fail
            "timeout_ms": 1000,
            "retries": 1
        }),
    };

    let mqtt_config = AuditTargetConfig {
        id: "mqtt-test".to_string(),
        target_type: "mqtt".to_string(),
        enabled: false, // Disabled to avoid connection attempts
        args: json!({
            "broker_url": "mqtt://localhost:1883",
            "topic": "test/audit"
        }),
    };

    // Add targets to registry
    registry.add_target(webhook_config).await.unwrap();
    registry.add_target(webhook_fail_config).await.unwrap();
    registry.add_target(mqtt_config).await.unwrap();

    // Create audit system
    let config = AuditConfig {
        enabled: true,
        targets: vec![], // Targets already added to registry
        redaction: RedactionConfig::default(),
        performance: PerformanceConfig {
            batch_size: 1, // Process immediately
            batch_timeout_ms: 10,
            ..Default::default()
        },
    };

    let system = AuditSystem::new(registry.clone(), config);

    // Create test audit entries
    let entries = vec![
        s3_events::put_object("test-bucket", "file1.txt"),
        s3_events::get_object("test-bucket", "file2.txt"),
        s3_events::delete_object("test-bucket", "file3.txt"),
    ];

    // Send entries concurrently
    let mut tasks = vec![];
    for entry in entries {
        let system_clone = &system;
        tasks.push(tokio::spawn(async move { system_clone.log(Arc::new(entry)).await }));
    }

    // Wait for all to complete
    for task in tasks {
        let result = task.await.unwrap();
        // Logging should succeed even if some targets fail
        assert!(result.is_ok(), "Logging should succeed despite target failures");
    }

    // Wait for processing
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Check that system processed entries despite failures
    let stats = system.get_stats().await.unwrap();
    assert!(stats.entries_processed > 0 || stats.entries_dropped > 0);

    // Check target statuses
    let targets = registry.list_targets();
    assert_eq!(targets.len(), 3);

    // Find the failing target
    let failing_target = targets.iter().find(|t| t.id == "webhook-fail").unwrap();
    // It may have errors, but that's expected and isolated

    system.close().await.unwrap();
}

/// Test hot reload functionality
#[tokio::test]
async fn test_hot_reload_functionality() {
    let _ = tracing_subscriber::fmt::try_init();

    let factory = Arc::new(DefaultAuditTargetFactory::new());
    let registry = Arc::new(TargetRegistry::new(factory));

    // Initial configuration with one target
    let initial_config = AuditTargetConfig {
        id: "initial-webhook".to_string(),
        target_type: "webhook".to_string(),
        enabled: true,
        args: json!({
            "url": "http://httpbin.org/post",
            "timeout_ms": 3000
        }),
    };

    registry.add_target(initial_config).await.unwrap();

    let config = AuditConfig::default();
    let system = AuditSystem::new(registry.clone(), config);

    // Verify initial state
    assert_eq!(registry.list_targets().len(), 1);
    assert_eq!(registry.enabled_target_count(), 1);

    // Hot add a new target
    let new_target_config = AuditTargetConfig {
        id: "hot-added-webhook".to_string(),
        target_type: "webhook".to_string(),
        enabled: true,
        args: json!({
            "url": "http://httpbin.org/anything",
            "timeout_ms": 2000
        }),
    };

    registry.add_target(new_target_config).await.unwrap();

    // Verify hot add worked
    assert_eq!(registry.list_targets().len(), 2);
    assert_eq!(registry.enabled_target_count(), 2);

    // Hot disable a target
    registry.disable_target("initial-webhook").unwrap();
    assert_eq!(registry.enabled_target_count(), 1);

    // Hot re-enable
    registry.enable_target("initial-webhook").unwrap();
    assert_eq!(registry.enabled_target_count(), 2);

    // Hot remove a target
    registry.remove_target("hot-added-webhook").await.unwrap();
    assert_eq!(registry.list_targets().len(), 1);
    assert_eq!(registry.enabled_target_count(), 1);

    // Test that logging still works after changes
    let entry = s3_events::create_bucket("hot-reload-test");
    system.log(Arc::new(entry)).await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    system.close().await.unwrap();
}

/// Test performance characteristics and P99 latency
#[tokio::test]
async fn test_performance_characteristics() {
    let _ = tracing_subscriber::fmt::try_init();

    let factory = Arc::new(DefaultAuditTargetFactory::new());
    let registry = Arc::new(TargetRegistry::new(factory));

    // Add fast target (disabled to avoid actual network calls)
    let fast_target = AuditTargetConfig {
        id: "perf-test-webhook".to_string(),
        target_type: "webhook".to_string(),
        enabled: false, // Disabled for performance test
        args: json!({
            "url": "http://httpbin.org/post",
            "timeout_ms": 1000
        }),
    };

    registry.add_target(fast_target).await.unwrap();

    let config = AuditConfig {
        enabled: true,
        targets: vec![],
        redaction: RedactionConfig::default(),
        performance: PerformanceConfig {
            batch_size: 10,
            batch_timeout_ms: 50,
            max_concurrent_dispatches: 50,
            ..Default::default()
        },
    };

    let system = AuditSystem::new(registry, config);

    // Performance test: measure latency for 100 entries
    let entry_count = 100;
    let mut latencies = Vec::with_capacity(entry_count);

    for i in 0..entry_count {
        let start = Instant::now();

        let entry = s3_events::put_object("perf-bucket", &format!("file-{}.txt", i));
        system.log(Arc::new(entry)).await.unwrap();

        let latency = start.elapsed();
        latencies.push(latency);
    }

    // Wait for processing
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Calculate statistics
    latencies.sort();
    let p50 = latencies[entry_count / 2];
    let p95 = latencies[(entry_count * 95) / 100];
    let p99 = latencies[(entry_count * 99) / 100];

    println!("Performance metrics:");
    println!("P50: {:?}", p50);
    println!("P95: {:?}", p95);
    println!("P99: {:?}", p99);

    // Verify P99 is reasonable (should be very fast since targets are disabled)
    assert!(p99 < Duration::from_millis(100), "P99 latency too high: {:?}", p99);

    // Verify throughput
    let stats = system.get_stats().await.unwrap();
    println!("System stats: {:?}", stats);

    system.close().await.unwrap();
}

/// Test header redaction functionality
#[tokio::test]
async fn test_header_redaction() {
    let _ = tracing_subscriber::fmt::try_init();

    let factory = Arc::new(DefaultAuditTargetFactory::new());
    let registry = Arc::new(TargetRegistry::new(factory));

    let config = AuditConfig {
        enabled: true,
        targets: vec![],
        redaction: RedactionConfig {
            headers_blacklist: vec!["Authorization".to_string(), "X-Api-Key".to_string(), "Cookie".to_string()],
        },
        performance: PerformanceConfig::default(),
    };

    let system = AuditSystem::new(registry, config);

    // Create entry with sensitive headers
    let mut entry = s3_events::get_object("test-bucket", "sensitive-file.txt");

    let mut headers = HashMap::new();
    headers.insert("Authorization".to_string(), "Bearer secret-token".to_string());
    headers.insert("X-Api-Key".to_string(), "api-key-12345".to_string());
    headers.insert("Content-Type".to_string(), "application/json".to_string());
    headers.insert("Cookie".to_string(), "session=abc123".to_string());

    entry.request_header = Some(headers);

    // Log the entry (redaction should happen during processing)
    system.log(Arc::new(entry.clone())).await.unwrap();

    // Wait for processing
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify redaction by checking if original entry gets modified during processing
    // Note: The actual redaction happens in the system processing, so we'll test
    // the redaction function directly on the entry
    let mut test_entry = entry.clone();
    let blacklist = vec!["Authorization".to_string(), "X-Api-Key".to_string()];
    test_entry.redact_headers(&blacklist);

    if let Some(headers) = &test_entry.request_header {
        assert_eq!(headers.get("Authorization").unwrap(), "***REDACTED***");
        assert_eq!(headers.get("X-Api-Key").unwrap(), "***REDACTED***");
        assert_eq!(headers.get("Content-Type").unwrap(), "application/json"); // Not redacted
    } else {
        panic!("Headers should be present");
    }

    system.close().await.unwrap();
}

/// Test error recovery and resilience
#[tokio::test]
async fn test_error_recovery_resilience() {
    let _ = tracing_subscriber::fmt::try_init();

    let factory = Arc::new(DefaultAuditTargetFactory::new());
    let registry = Arc::new(TargetRegistry::new(factory));

    // Add target that will initially fail
    let flaky_target = AuditTargetConfig {
        id: "flaky-webhook".to_string(),
        target_type: "webhook".to_string(),
        enabled: true,
        args: json!({
            "url": "http://definitely-nonexistent-domain.invalid/webhook",
            "timeout_ms": 500,
            "retries": 2
        }),
    };

    registry.add_target(flaky_target).await.unwrap();

    let config = AuditConfig::default();
    let system = AuditSystem::new(registry.clone(), config);

    // Send multiple entries despite target failures
    for i in 0..10 {
        let entry = s3_events::put_object("resilience-bucket", &format!("file-{}.txt", i));

        // This should not panic or block, even though target will fail
        let result = timeout(Duration::from_millis(2000), system.log(Arc::new(entry))).await;
        assert!(result.is_ok(), "Log operation should not timeout");
        assert!(result.unwrap().is_ok(), "Log operation should succeed despite target failure");
    }

    // Wait for processing
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // System should still be operational
    let stats = system.get_stats().await.unwrap();
    println!("Resilience test stats: {:?}", stats);

    // Check target status - should show errors but system continues
    let targets = registry.list_targets();
    let flaky_target = targets.iter().find(|t| t.id == "flaky-webhook").unwrap();
    assert!(flaky_target.error_count > 0, "Target should have recorded errors");

    // System should still accept new entries
    let final_entry = s3_events::delete_object("resilience-bucket", "final-file.txt");
    system.log(Arc::new(final_entry)).await.unwrap();

    system.close().await.unwrap();
}

/// Test configuration validation edge cases
#[tokio::test]
async fn test_configuration_validation() {
    let factory = DefaultAuditTargetFactory::new();

    // Test invalid webhook configurations
    let invalid_configs = vec![
        // Missing URL
        AuditTargetConfig {
            id: "no-url".to_string(),
            target_type: "webhook".to_string(),
            enabled: true,
            args: json!({}),
        },
        // Invalid URL scheme
        AuditTargetConfig {
            id: "bad-scheme".to_string(),
            target_type: "webhook".to_string(),
            enabled: true,
            args: json!({"url": "ftp://example.com"}),
        },
        // Invalid method
        AuditTargetConfig {
            id: "bad-method".to_string(),
            target_type: "webhook".to_string(),
            enabled: true,
            args: json!({
                "url": "https://example.com",
                "method": "INVALID"
            }),
        },
        // Invalid timeout
        AuditTargetConfig {
            id: "bad-timeout".to_string(),
            target_type: "webhook".to_string(),
            enabled: true,
            args: json!({
                "url": "https://example.com",
                "timeout_ms": 50
            }),
        },
        // Invalid target_type in args
        AuditTargetConfig {
            id: "bad-target-type".to_string(),
            target_type: "webhook".to_string(),
            enabled: true,
            args: json!({
                "url": "https://example.com",
                "target_type": "InvalidType"
            }),
        },
    ];

    for config in invalid_configs {
        let result = factory.validate_config(&config);
        assert!(result.is_err(), "Config should be invalid: {:?}", config);
    }

    // Test invalid MQTT configurations
    let invalid_mqtt_configs = vec![
        // Missing broker_url
        AuditTargetConfig {
            id: "no-broker".to_string(),
            target_type: "mqtt".to_string(),
            enabled: true,
            args: json!({"topic": "test"}),
        },
        // Missing topic
        AuditTargetConfig {
            id: "no-topic".to_string(),
            target_type: "mqtt".to_string(),
            enabled: true,
            args: json!({"broker_url": "mqtt://localhost:1883"}),
        },
        // Invalid scheme
        AuditTargetConfig {
            id: "bad-scheme".to_string(),
            target_type: "mqtt".to_string(),
            enabled: true,
            args: json!({
                "broker_url": "http://localhost:1883",
                "topic": "test"
            }),
        },
        // Invalid QoS
        AuditTargetConfig {
            id: "bad-qos".to_string(),
            target_type: "mqtt".to_string(),
            enabled: true,
            args: json!({
                "broker_url": "mqtt://localhost:1883",
                "topic": "test",
                "qos": 5
            }),
        },
    ];

    for config in invalid_mqtt_configs {
        let result = factory.validate_config(&config);
        assert!(result.is_err(), "MQTT config should be invalid: {:?}", config);
    }

    // Test valid configurations
    let valid_webhook = AuditTargetConfig {
        id: "valid-webhook".to_string(),
        target_type: "webhook".to_string(),
        enabled: true,
        args: json!({
            "url": "https://example.com/webhook",
            "method": "POST",
            "timeout_ms": 3000,
            "retries": 2,
            "target_type": "AuditLog"
        }),
    };

    assert!(factory.validate_config(&valid_webhook).is_ok());

    let valid_mqtt = AuditTargetConfig {
        id: "valid-mqtt".to_string(),
        target_type: "mqtt".to_string(),
        enabled: true,
        args: json!({
            "broker_url": "mqtts://secure.example.com:8883",
            "topic": "audit/logs",
            "qos": 1,
            "target_type": "AuditLog"
        }),
    };

    assert!(factory.validate_config(&valid_mqtt).is_ok());
}

/// Test system lifecycle management
#[tokio::test]
async fn test_system_lifecycle_management() {
    let _ = tracing_subscriber::fmt::try_init();

    let factory = Arc::new(DefaultAuditTargetFactory::new());
    let registry = Arc::new(TargetRegistry::new(factory));

    let config = AuditConfig::default();
    let system = AuditSystem::new(registry.clone(), config);

    // Test start
    system.start().await.unwrap();

    // Test logging while running
    let entry = s3_events::list_bucket("lifecycle-bucket");
    system.log(Arc::new(entry)).await.unwrap();

    // Test pause
    system.pause().await.unwrap();

    // Logging should still work (just gets dropped due to disabled state)
    let entry = s3_events::put_object("lifecycle-bucket", "paused-file.txt");
    system.log(Arc::new(entry)).await.unwrap();

    // Test resume
    system.resume().await.unwrap();

    // Test logging after resume
    let entry = s3_events::get_object("lifecycle-bucket", "resumed-file.txt");
    system.log(Arc::new(entry)).await.unwrap();

    // Test close
    system.close().await.unwrap();

    // Should handle close gracefully
}
