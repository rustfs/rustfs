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

//! Example demonstrating the RustFS audit system usage

use rustfs_audit_logger::{
    audit_logger, initialize, log_audit_entry, AuditConfig, AuditLogEntry, AuditTargetConfig,
    ApiDetails,
};
use tokio;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    // 1. Load configuration and register targets on startup
    let config = create_sample_config();

    // 2. Initialize audit system
    initialize(Some(config)).expect("Audit system should init once");

    // Wait a moment for async initialization
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // 3. Log collection entry
    let _logger = audit_logger().expect("Logger should be initialized");

    // Create an audit entry
    let audit_entry = AuditLogEntry::new()
        .set_version("1.0".to_string())
        .set_deployment_id(Some("test-deployment".to_string()))
        .set_event("s3:GetObject".to_string())
        .set_entry_type(Some("S3".to_string()))
        .set_api(
            ApiDetails::new()
                .set_name(Some("GetObject".to_string()))
                .set_bucket(Some("test-bucket".to_string()))
                .set_object(Some("test-object.txt".to_string()))
                .set_status(Some("OK".to_string()))
                .set_status_code(Some(200))
                .set_input_bytes(0)
                .set_output_bytes(1024),
        )
        .set_remote_host(Some("192.168.1.100".to_string()))
        .set_user_agent(Some("aws-sdk-rust/1.0".to_string()))
        .set_access_key(Some("testuser".to_string()));

    // Log the entry
    println!("Logging audit entry...");
    log_audit_entry(audit_entry).await?;

    // Give some time for async processing
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // Show target statuses
    if let Some(logger) = audit_logger() {
        let statuses = logger.manager().registry().get_all_target_statuses();
        println!("\nTarget Statuses:");
        for status in statuses {
            println!("  Target {}: enabled={}, running={}, success_count={}, error_count={}",
                status.id, status.enabled, status.running, status.success_count, status.error_count);
            if let Some(error) = &status.last_error {
                println!("    Last error: {}", error);
            }
        }
    }

    // Shutdown gracefully
    println!("\nShutting down audit system...");
    if let Some(logger) = audit_logger() {
        logger.shutdown().await?;
    }

    println!("Example completed successfully!");
    Ok(())
}

/// Create a sample audit configuration with webhook and MQTT targets
fn create_sample_config() -> AuditConfig {
    let mut config = AuditConfig::default();

    // Add a webhook target
    let webhook_args = serde_json::json!({
        "endpoint": "https://httpbin.org/post",
        "auth_token": "test-token"
    });

    let webhook_target = AuditTargetConfig::new("webhook-1".to_string(), "webhook".to_string())
        .with_args(webhook_args)
        .with_enabled(true)
        .with_batch_size(5);

    // Add an MQTT target (this will fail to connect but shows the configuration)
    let mqtt_args = serde_json::json!({
        "broker": "localhost",
        "port": 1883,
        "topic": "rustfs/audit",
        "username": "testuser",
        "password": "testpass"
    });

    let mqtt_target = AuditTargetConfig::new("mqtt-1".to_string(), "mqtt".to_string())
        .with_args(mqtt_args)
        .with_enabled(false) // Disabled since we don't have a real MQTT broker
        .with_batch_size(10);

    config.targets = vec![webhook_target, mqtt_target];
    config.enabled = true;

    config
}
