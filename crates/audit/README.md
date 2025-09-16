# RustFS Audit System

Enterprise-grade audit logging system for RustFS distributed object storage with S3/MinIO compatibility.

## Features

- **Multi-Target Dispatch**: Send audit logs to MQTT, Webhook, and other configurable targets
- **High Performance**: Async processing with batching and concurrent dispatch (3k EPS/node, P99 < 30ms)
- **S3 Compatible**: Audit log format matches AWS S3 and MinIO audit logs
- **Hot Reload**: Runtime configuration updates without restart
- **Error Isolation**: Individual target failures don't affect other targets
- **Header Redaction**: Configurable sensitive header masking for security
- **Global Integration**: OnceCell-based singleton for easy system-wide integration

## Quick Start

```rust
use rustfs_audit::{
    initialize_audit_logger, log_audit_entry, s3_events,
    AuditConfig, AuditTargetConfig, DefaultAuditTargetFactory
};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Create configuration
    let mut config = AuditConfig::default();
    config.targets.push(AuditTargetConfig {
        id: "webhook-audit".to_string(),
        target_type: "webhook".to_string(),
        enabled: true,
        args: serde_json::json!({
            "url": "http://localhost:8080/audit",
            "timeout_ms": 5000
        }),
    });
    
    // 2. Initialize global audit system
    let factory = Arc::new(DefaultAuditTargetFactory::new());
    initialize_audit_logger(factory, config).await?;
    
    // 3. Log S3 operations
    let audit_entry = s3_events::get_object("my-bucket", "my-file.txt")
        .with_request_context(
            Some("192.168.1.100".to_string()),
            Some("aws-cli/2.0".to_string()),
            Some("/my-bucket/my-file.txt".to_string()),
            Some("s3.amazonaws.com".to_string()),
        )
        .with_response_status(
            Some("OK".to_string()),
            Some(200),
            None,
            Some(150_000_000), // 150ms
        );
    
    log_audit_entry(audit_entry).await?;
    Ok(())
}
```

## Configuration

### JSON Configuration

```json
{
  "enabled": true,
  "targets": [
    {
      "id": "audit-webhook-1",
      "target_type": "webhook", 
      "enabled": true,
      "args": {
        "url": "https://audit.company.com/webhook",
        "method": "POST",
        "headers": {"Authorization": "Bearer token"},
        "timeout_ms": 2000,
        "retries": 3
      }
    },
    {
      "id": "audit-mqtt-1",
      "target_type": "mqtt",
      "enabled": true, 
      "args": {
        "broker_url": "mqtts://mqtt.company.com:8883",
        "topic": "rustfs/audit",
        "qos": 1,
        "username": "audit-user",
        "password": "secret"
      }
    }
  ],
  "redaction": {
    "headers_blacklist": ["Authorization", "Cookie", "X-Api-Key"]
  },
  "performance": {
    "batch_size": 50,
    "batch_timeout_ms": 100,
    "max_concurrent_dispatches": 100
  }
}
```

### Webhook Target Configuration

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `url` | String | Yes | HTTP endpoint URL |
| `method` | String | No | HTTP method (default: POST) |
| `headers` | Object | No | Custom HTTP headers |
| `timeout_ms` | Number | No | Request timeout (default: 5000) |
| `retries` | Number | No | Retry attempts (default: 3) |

### MQTT Target Configuration

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `broker_url` | String | Yes | MQTT broker URL (mqtt:// or mqtts://) |
| `topic` | String | Yes | MQTT topic to publish to |
| `qos` | Number | No | Quality of Service level (default: 1) |
| `username` | String | No | MQTT username |
| `password` | String | No | MQTT password |

## S3 Compatible Audit Log Format

The audit system generates logs compatible with AWS S3 and MinIO formats:

```json
{
  "version": "1",
  "deploymentid": "rustfs-prod",
  "time": "2024-01-15T10:30:45.123456789Z",
  "event": "s3:GetObject",
  "type": "S3",
  "api": {
    "name": "GetObject",
    "bucket": "user-data",
    "object": "documents/report.pdf",
    "status": "OK",
    "statusCode": 200,
    "rx": 0,
    "tx": 2048576,
    "timeToResponse": "125ms",
    "timeToResponseInNS": 125000000
  },
  "remotehost": "192.168.1.100",
  "userAgent": "aws-cli/2.15.0",
  "requestPath": "/user-data/documents/report.pdf",
  "requestHost": "s3.example.com",
  "accessKey": "AKIAIOSFODNN7EXAMPLE"
}
```

## Global Functions

- `initialize_audit_logger()` - Initialize the global audit system
- `log_audit_entry()` - Log a single audit entry
- `log_audit()` - Log an Arc<AuditEntry>
- `get_audit_stats()` - Get system statistics
- `list_audit_targets()` - List all configured targets
- `close_audit_system()` - Graceful shutdown

## S3 Event Helpers

Convenience functions for common S3 operations:

```rust
use rustfs_audit::s3_events;

// Object operations
let get_audit = s3_events::get_object("bucket", "key");
let put_audit = s3_events::put_object("bucket", "key");
let delete_audit = s3_events::delete_object("bucket", "key");

// Bucket operations
let create_bucket = s3_events::create_bucket("bucket");
let delete_bucket = s3_events::delete_bucket("bucket");
let list_bucket = s3_events::list_bucket("bucket");

// Multipart operations
let complete_multipart = s3_events::complete_multipart_upload("bucket", "key");
let abort_multipart = s3_events::abort_multipart_upload("bucket", "key");
```

## Performance Characteristics

- **Throughput**: 3,000+ entries per second per node
- **Latency**: P99 < 30ms dispatch time
- **Memory**: Configurable batching and queue sizes
- **Concurrency**: Async processing with failure isolation
- **Reliability**: Retry mechanisms with exponential backoff

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Application   │    │   Audit System   │    │     Targets     │
│                 │    │                  │    │                 │
│ log_audit_entry │───▶│  Global Logger   │───▶│ Webhook Target  │
│                 │    │                  │    │                 │
│   S3 Handler    │    │ Target Registry  │    │  MQTT Target    │
│                 │    │                  │    │                 │
│  S3 Operations  │    │ Batch Processor  │    │ Custom Target   │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## Integration with RustFS

The audit system integrates seamlessly with RustFS components:

- **Configuration**: Loads from `.rustfs.sys` via `rustfs-ecstore`
- **Storage**: Persists configurations and state
- **Hot Reload**: Runtime configuration updates
- **Performance**: Non-blocking async operations
- **Monitoring**: Built-in statistics and health checks

## Examples

Run the basic usage example:

```bash
cargo run --example basic_usage
```

This will demonstrate:
- System initialization
- Logging various S3 operations
- Statistics reporting  
- Target status monitoring
- Graceful shutdown

## Error Handling

The audit system is designed for reliability:

- **Error Isolation**: Target failures don't affect other targets
- **Retry Logic**: Configurable retry attempts with backoff
- **Graceful Degradation**: System continues operating with partial failures
- **Detailed Logging**: Comprehensive error reporting and debugging

## License

Licensed under the Apache License, Version 2.0.