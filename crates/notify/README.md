[![RustFS](https://rustfs.com/images/rustfs-github.png)](https://rustfs.com)

# RustFS Notify - Event Notification System

<p align="center">
  <strong>Real-time event notification system for RustFS distributed object storage</strong>
</p>

<p align="center">
  <a href="https://github.com/rustfs/rustfs/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/rustfs/rustfs/actions/workflows/ci.yml/badge.svg" /></a>
  <a href="https://docs.rustfs.com/en/">📖 Documentation</a>
  · <a href="https://github.com/rustfs/rustfs/issues">🐛 Bug Reports</a>
  · <a href="https://github.com/rustfs/rustfs/discussions">💬 Discussions</a>
</p>

---

## 📖 Overview

**RustFS Notify** is the event notification system for the [RustFS](https://rustfs.com) distributed object storage platform. It provides real-time event publishing and delivery to various targets including webhooks, MQTT brokers, and message queues, enabling seamless integration with external systems and workflows.

> **Note:** This is a core submodule of RustFS that provides essential event notification capabilities for the distributed object storage system. For the complete RustFS experience, please visit the [main RustFS repository](https://github.com/rustfs/rustfs).

## ✨ Features

### 📡 Event Publishing

- **Real-time Events**: Instant notification of storage events
- **Event Filtering**: Advanced filtering based on object patterns and event types
- **Reliable Delivery**: Guaranteed delivery with retry mechanisms
- **Batch Processing**: Efficient batch event delivery

### 🎯 Multiple Targets

- **Webhooks**: HTTP/HTTPS webhook notifications
- **MQTT**: MQTT broker integration for IoT scenarios
- **Message Queues**: Integration with popular message queue systems
- **Custom Targets**: Extensible target system for custom integrations

### 🔧 Advanced Features

- **Event Transformation**: Custom event payload transformation
- **Pattern Matching**: Flexible pattern-based event filtering
- **Rate Limiting**: Configurable rate limiting for targets
- **Dead Letter Queue**: Failed event handling and recovery

## 📦 Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
rustfs-notify = "0.1.0"
```

## 🔧 Usage

### Basic Event Notification

```rust
use rustfs_notify::{Event, EventType, NotificationTarget, NotifySystem};
use rustfs_notify::target::WebhookTarget;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create notification system
    let notify_system = NotifySystem::new().await?;

    // Create webhook target
    let webhook = WebhookTarget::new(
        "webhook-1",
        "https://api.example.com/webhook",
        vec![EventType::ObjectCreated, EventType::ObjectRemoved],
    );

    // Add target to notification system
    notify_system.add_target(Box::new(webhook)).await?;

    // Create and send event
    let event = Event::new(
        EventType::ObjectCreated,
        "my-bucket",
        "path/to/object.txt",
        "user123",
    );

    notify_system.publish_event(event).await?;

    Ok(())
}
```

### Advanced Event Configuration

```rust
use rustfs_notify::{Event, EventType, NotificationConfig};
use rustfs_notify::target::{WebhookTarget, MqttTarget};
use rustfs_notify::filter::{EventFilter, PatternRule};

async fn setup_advanced_notifications() -> Result<(), Box<dyn std::error::Error>> {
    let notify_system = NotifySystem::new().await?;

    // Create webhook with custom configuration
    let webhook_config = NotificationConfig {
        retry_attempts: 3,
        retry_delay: std::time::Duration::from_secs(5),
        timeout: std::time::Duration::from_secs(30),
        rate_limit: Some(100), // 100 events per minute
        ..Default::default()
    };

    let webhook = WebhookTarget::builder()
        .id("production-webhook")
        .url("https://api.example.com/events")
        .events(vec![EventType::ObjectCreated, EventType::ObjectRemoved])
        .config(webhook_config)
        .headers(vec![
            ("Authorization".to_string(), "Bearer token123".to_string()),
            ("Content-Type".to_string(), "application/json".to_string()),
        ])
        .build()?;

    // Create MQTT target
    let mqtt_target = MqttTarget::builder()
        .id("iot-mqtt")
        .broker_url("mqtt://broker.example.com:1883")
        .topic("storage/events")
        .qos(1)
        .events(vec![EventType::ObjectCreated])
        .build()?;

    // Add targets
    notify_system.add_target(Box::new(webhook)).await?;
    notify_system.add_target(Box::new(mqtt_target)).await?;

    Ok(())
}
```

### Event Filtering and Pattern Matching

```rust
use rustfs_notify::filter::{EventFilter, PatternRule, ConditionRule};

fn setup_event_filters() -> Result<EventFilter, Box<dyn std::error::Error>> {
    let filter = EventFilter::builder()
        // Only images and documents
        .pattern_rule(PatternRule::new(
            "suffix",
            vec!["*.jpg", "*.png", "*.pdf", "*.doc"]
        ))
        // Exclude temporary files
        .pattern_rule(PatternRule::new(
            "exclude",
            vec!["*/tmp/*", "*.tmp"]
        ))
        // Only files larger than 1MB
        .condition_rule(ConditionRule::new(
            "object_size",
            ">",
            1024 * 1024
        ))
        // Only from specific buckets
        .bucket_filter(vec!["important-bucket", "backup-bucket"])
        .build()?;

    Ok(filter)
}
```

### Custom Target Implementation

```rust
use rustfs_notify::{Event, NotificationTarget, TargetResult};
use async_trait::async_trait;

pub struct SlackTarget {
    id: String,
    webhook_url: String,
    channel: String,
}

#[async_trait]
impl NotificationTarget for SlackTarget {
    fn id(&self) -> &str {
        &self.id
    }

    async fn deliver_event(&self, event: &Event) -> TargetResult<()> {
        let message = format!(
            "🔔 Storage Event: {} in bucket `{}` - object `{}`",
            event.event_type,
            event.bucket_name,
            event.object_name
        );

        let payload = serde_json::json!({
            "text": message,
            "channel": self.channel,
        });

        let client = reqwest::Client::new();
        let response = client
            .post(&self.webhook_url)
            .json(&payload)
            .send()
            .await?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(format!("Slack delivery failed: {}", response.status()).into())
        }
    }

    fn supports_event_type(&self, event_type: &EventType) -> bool {
        // Support all event types
        true
    }
}
```

### Event Transformation

```rust
use rustfs_notify::{Event, EventTransformer};
use serde_json::{json, Value};

pub struct CustomEventTransformer;

impl EventTransformer for CustomEventTransformer {
    fn transform(&self, event: &Event) -> Value {
        json!({
            "eventVersion": "2.1",
            "eventSource": "rustfs:s3",
            "eventTime": event.timestamp.to_rfc3339(),
            "eventName": event.event_type.to_string(),
            "s3": {
                "bucket": {
                    "name": event.bucket_name,
                    "arn": format!("arn:aws:s3:::{}", event.bucket_name)
                },
                "object": {
                    "key": event.object_name,
                    "size": event.object_size.unwrap_or(0),
                    "eTag": event.etag.as_ref().unwrap_or(&"".to_string()),
                }
            },
            "userIdentity": {
                "principalId": event.user_identity
            }
        })
    }
}
```

## 🏗️ Architecture

### Notification System Architecture

```
Notify Architecture:
┌─────────────────────────────────────────────────────────────┐
│                    Event Publisher                          │
├─────────────────────────────────────────────────────────────┤
│   Event Filter   │   Event Queue   │   Event Transformer   │
├─────────────────────────────────────────────────────────────┤
│              Target Manager                                  │
├─────────────────────────────────────────────────────────────┤
│  Webhook Target  │   MQTT Target   │   Custom Targets       │
├─────────────────────────────────────────────────────────────┤
│              Delivery Engine                                 │
└─────────────────────────────────────────────────────────────┘
```

### Supported Event Types

| Event Type | Description | Triggers |
|-----------|-------------|----------|
| `ObjectCreated` | Object creation events | PUT, POST, COPY |
| `ObjectRemoved` | Object deletion events | DELETE |
| `ObjectAccessed` | Object access events | GET, HEAD |
| `ObjectRestore` | Object restoration events | Restore operations |
| `BucketCreated` | Bucket creation events | CreateBucket |
| `BucketRemoved` | Bucket deletion events | DeleteBucket |

### Target Types

| Target | Protocol | Use Case | Reliability |
|--------|----------|----------|------------|
| Webhook | HTTP/HTTPS | Web applications, APIs | High |
| MQTT | MQTT | IoT devices, real-time systems | Medium |
| Message Queue | AMQP, Redis | Microservices, async processing | High |
| Custom | Any | Specialized integrations | Configurable |

## 🧪 Testing

Run the test suite:

```bash
# Run all tests
cargo test

# Run integration tests
cargo test --test integration

# Test webhook delivery
cargo test webhook

# Test MQTT integration
cargo test mqtt

# Run with coverage
cargo test --features test-coverage
```

## ⚙️ Configuration

### Basic Configuration

```toml
[notify]
# Global settings
enabled = true
max_concurrent_deliveries = 100
default_retry_attempts = 3
default_timeout = "30s"

# Queue settings
event_queue_size = 10000
batch_size = 100
batch_timeout = "5s"

# Dead letter queue
dlq_enabled = true
dlq_max_size = 1000
```

### Target Configuration

```toml
[[notify.targets]]
type = "webhook"
id = "primary-webhook"
url = "https://api.example.com/webhook"
events = ["ObjectCreated", "ObjectRemoved"]
retry_attempts = 5
timeout = "30s"

[[notify.targets]]
type = "mqtt"
id = "iot-broker"
broker_url = "mqtt://broker.example.com:1883"
topic = "storage/events"
qos = 1
events = ["ObjectCreated"]
```

## 🚀 Performance

The notification system is designed for high-throughput scenarios:

- **Async Processing**: Non-blocking event delivery
- **Batch Delivery**: Efficient batch processing for high-volume events
- **Connection Pooling**: Reused connections for better performance
- **Rate Limiting**: Configurable rate limiting to prevent overwhelming targets

## 📋 Requirements

- **Rust**: 1.70.0 or later
- **Platforms**: Linux, macOS, Windows
- **Network**: Outbound connectivity for target delivery
- **Memory**: Scales with event queue size

## 🌍 Related Projects

This module is part of the RustFS ecosystem:

- [RustFS Main](https://github.com/rustfs/rustfs) - Core distributed storage system
- [RustFS ECStore](../ecstore) - Erasure coding storage engine
- [RustFS Config](../config) - Configuration management
- [RustFS Utils](../utils) - Utility functions

## 📚 Documentation

For comprehensive documentation, visit:

- [RustFS Documentation](https://docs.rustfs.com)
- [Notify API Reference](https://docs.rustfs.com/notify/)
- [Event Configuration Guide](https://docs.rustfs.com/events/)

## 🔗 Links

- [Documentation](https://docs.rustfs.com) - Complete RustFS manual
- [Changelog](https://github.com/rustfs/rustfs/releases) - Release notes and updates
- [GitHub Discussions](https://github.com/rustfs/rustfs/discussions) - Community support

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](https://github.com/rustfs/rustfs/blob/main/CONTRIBUTING.md) for details.

## 📄 License

Licensed under the Apache License, Version 2.0. See [LICENSE](https://github.com/rustfs/rustfs/blob/main/LICENSE) for details.

---

<p align="center">
  <strong>RustFS</strong> is a trademark of RustFS, Inc.<br>
  All other trademarks are the property of their respective owners.
</p>

<p align="center">
  Made with 📡 by the RustFS Team
</p>
