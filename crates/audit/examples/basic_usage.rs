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

//! Basic usage example for the RustFS audit system

use rustfs_audit::{
    AuditConfig, AuditTargetConfig, DefaultAuditTargetFactory, close_audit_system, get_audit_stats, initialize_audit_logger,
    list_audit_targets, log_audit_entry, s3_events,
};
use std::sync::Arc;
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    println!("🚀 RustFS Audit System Example");
    println!("================================");

    // 1. Create audit configuration
    let mut config = AuditConfig {
        enabled: true,
        targets: vec![
            // Webhook target (disabled to avoid network calls)
            AuditTargetConfig {
                id: "audit-webhook".to_string(),
                target_type: "webhook".to_string(),
                enabled: false, // Disabled to prevent actual HTTP calls
                args: serde_json::json!({
                    "url": "http://localhost:8080/audit-webhook",
                    "method": "POST",
                    "timeout_ms": 5000,
                    "retries": 3,
                    "headers": {
                        "Authorization": "Bearer audit-token",
                        "X-Source": "rustfs-audit"
                    }
                }),
            },
            // MQTT target (disabled to avoid connection attempts)
            AuditTargetConfig {
                id: "audit-mqtt".to_string(),
                target_type: "mqtt".to_string(),
                enabled: false, // Disabled to prevent actual MQTT connections
                args: serde_json::json!({
                    "broker_url": "mqtt://localhost:1883",
                    "topic": "rustfs/audit/events",
                    "qos": 1,
                    "username": "audit-user",
                    "password": "audit-pass"
                }),
            },
        ],
        redaction: Default::default(),
        performance: Default::default(),
    };

    // 2. Initialize the global audit system
    println!("\n📝 Initializing audit system...");
    let factory = Arc::new(DefaultAuditTargetFactory::new());
    initialize_audit_logger(factory, config).await?;

    // 3. Create and log some S3 audit entries
    println!("\n📊 Logging S3 operations...");

    // GetObject audit
    let get_audit = s3_events::get_object("user-data", "documents/report.pdf")
        .with_request_context(
            Some("192.168.1.100".to_string()),
            Some("aws-cli/2.15.0".to_string()),
            Some("/user-data/documents/report.pdf".to_string()),
            Some("s3.example.com".to_string()),
        )
        .with_auth_context(Some("AKIAIOSFODNN7EXAMPLE".to_string()), None, None)
        .with_response_status(
            Some("OK".to_string()),
            Some(200),
            None,
            Some(125_000_000), // 125ms
        )
        .with_byte_counts(0, 2_048_576, Some(512)); // 2MB response

    log_audit_entry(get_audit).await?;
    println!("  ✅ Logged GetObject operation");

    // PutObject audit
    let put_audit = s3_events::put_object("user-uploads", "photos/vacation.jpg")
        .with_request_context(
            Some("10.0.1.50".to_string()),
            Some("MinIO (linux; amd64) minio-go/v7.0.0".to_string()),
            Some("/user-uploads/photos/vacation.jpg".to_string()),
            Some("s3.example.com".to_string()),
        )
        .with_auth_context(Some("AKIAI44QH8DHBEXAMPLE".to_string()), None, None)
        .with_response_status(
            Some("Created".to_string()),
            Some(201),
            None,
            Some(89_000_000), // 89ms
        )
        .with_byte_counts(5_242_880, 0, Some(256)); // 5MB upload

    log_audit_entry(put_audit).await?;
    println!("  ✅ Logged PutObject operation");

    // CreateBucket audit
    let create_bucket_audit = s3_events::create_bucket("new-project-bucket")
        .with_request_context(
            Some("203.0.113.42".to_string()),
            Some("Terraform/1.5.0".to_string()),
            Some("/new-project-bucket".to_string()),
            Some("s3.example.com".to_string()),
        )
        .with_auth_context(Some("AKIA6ODQ552EXAMPLE".to_string()), None, None)
        .with_response_status(
            Some("Created".to_string()),
            Some(201),
            None,
            Some(45_000_000), // 45ms
        );

    log_audit_entry(create_bucket_audit).await?;
    println!("  ✅ Logged CreateBucket operation");

    // CompleteMultipartUpload audit
    let complete_multipart_audit = s3_events::complete_multipart_upload("big-data", "datasets/logs-2024.tar.gz")
        .with_request_context(
            Some("172.16.0.25".to_string()),
            Some("boto3/1.28.0 Python/3.11.0".to_string()),
            Some("/big-data/datasets/logs-2024.tar.gz".to_string()),
            Some("s3.example.com".to_string()),
        )
        .with_auth_context(Some("AKIAYVP4CIPPEREXAMPLE".to_string()), None, None)
        .with_response_status(
            Some("OK".to_string()),
            Some(200),
            None,
            Some(2_150_000_000), // 2.15 seconds
        )
        .with_byte_counts(0, 1_073_741_824, Some(1024)); // 1GB file

    log_audit_entry(complete_multipart_audit).await?;
    println!("  ✅ Logged CompleteMultipartUpload operation");

    // 4. Wait a moment for processing
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // 5. Display system statistics
    println!("\n📈 System Statistics:");
    match get_audit_stats().await {
        Ok(stats) => {
            println!("  • Entries processed: {}", stats.entries_processed);
            println!("  • Entries dropped: {}", stats.entries_dropped);
            println!("  • Current queue size: {}", stats.current_queue_size);
            println!("  • Active targets: {}", stats.active_targets);
            println!("  • Avg processing time: {}μs", stats.avg_processing_time_ns / 1000);
        }
        Err(e) => println!("  ❌ Failed to get stats: {}", e),
    }

    // 6. Display target information
    println!("\n🎯 Target Status:");
    match list_audit_targets() {
        Ok(targets) => {
            for target in targets {
                println!(
                    "  • {} ({}): enabled={}, running={}",
                    target.id, target.target_type, target.enabled, target.running
                );
                if let Some(error) = &target.last_error {
                    println!("    Last error: {}", error);
                }
                println!("    Success: {}, Errors: {}", target.success_count, target.error_count);
            }
        }
        Err(e) => println!("  ❌ Failed to list targets: {}", e),
    }

    // 7. Demonstrate JSON serialization
    println!("\n🔍 Sample Audit Entry JSON:");
    let sample_entry = s3_events::delete_object("temp-bucket", "old-file.txt")
        .with_request_context(
            Some("192.168.1.200".to_string()),
            Some("curl/7.88.1".to_string()),
            Some("/temp-bucket/old-file.txt".to_string()),
            Some("s3.example.com".to_string()),
        )
        .with_auth_context(Some("AKIAIOSFODNN7EXAMPLE".to_string()), Some("admin".to_string()), None);

    match sample_entry.to_json_pretty() {
        Ok(json) => {
            // Only show first few lines to keep output manageable
            let lines: Vec<&str> = json.lines().take(15).collect();
            println!("{}", lines.join("\n"));
            if json.lines().count() > 15 {
                println!("  ... (truncated)");
            }
        }
        Err(e) => println!("  ❌ Failed to serialize: {}", e),
    }

    // 8. Graceful shutdown
    println!("\n🛑 Shutting down audit system...");
    close_audit_system().await?;
    println!("  ✅ Audit system shut down gracefully");

    println!("\n🎉 Example completed successfully!");
    Ok(())
}
