[![RustFS](https://rustfs.com/images/rustfs-github.png)](https://rustfs.com)

# RustFS Obs - Observability & Monitoring

<p align="center">
  <strong>Comprehensive observability and monitoring solution for RustFS distributed object storage</strong>
</p>

<p align="center">
  <a href="https://github.com/rustfs/rustfs/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/rustfs/rustfs/actions/workflows/ci.yml/badge.svg" /></a>
  <a href="https://docs.rustfs.com/en/">📖 Documentation</a>
  · <a href="https://github.com/rustfs/rustfs/issues">🐛 Bug Reports</a>
  · <a href="https://github.com/rustfs/rustfs/discussions">💬 Discussions</a>
</p>

---

## 📖 Overview

**RustFS Obs** provides comprehensive observability and monitoring capabilities for the [RustFS](https://rustfs.com) distributed object storage system. It includes metrics collection, distributed tracing, logging, alerting, and performance monitoring to ensure optimal system operation and troubleshooting.

> **Note:** This is a critical operational submodule of RustFS that provides essential observability capabilities for the distributed object storage system. For the complete RustFS experience, please visit the [main RustFS repository](https://github.com/rustfs/rustfs).

## ✨ Features

### 📊 Metrics Collection

- **Prometheus Integration**: Native Prometheus metrics export
- **Custom Metrics**: Application-specific performance metrics
- **System Metrics**: CPU, memory, disk, and network monitoring
- **Business Metrics**: Storage usage, request rates, and error tracking

### 🔍 Distributed Tracing

- **OpenTelemetry Support**: Standard distributed tracing
- **Request Tracking**: End-to-end request lifecycle tracking
- **Performance Analysis**: Latency and bottleneck identification
- **Cross-Service Correlation**: Trace requests across microservices

### 📝 Structured Logging

- **JSON Logging**: Machine-readable structured logs
- **Log Levels**: Configurable log levels and filtering
- **Context Propagation**: Request context in all logs
- **Log Aggregation**: Centralized log collection support

### 🚨 Alerting & Notifications

- **Rule-Based Alerts**: Configurable alerting rules
- **Multiple Channels**: Email, Slack, webhook notifications
- **Alert Escalation**: Tiered alerting and escalation policies
- **Alert Correlation**: Group related alerts together

## 📦 Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
rustfs-obs = "0.1.0"
```

## 🔧 Usage

### Basic Observability Setup

```rust
use rustfs_obs::{ObservabilityConfig, MetricsCollector, TracingProvider};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure observability
    let config = ObservabilityConfig {
        service_name: "rustfs-storage".to_string(),
        metrics_endpoint: "http://prometheus:9090".to_string(),
        tracing_endpoint: "http://jaeger:14268/api/traces".to_string(),
        log_level: "info".to_string(),
        enable_metrics: true,
        enable_tracing: true,
    };

    // Initialize observability
    let obs = rustfs_obs::init(config).await?;

    // Your application code here
    run_application().await?;

    // Shutdown observability
    obs.shutdown().await?;

    Ok(())
}
```

### Metrics Collection

```rust
use rustfs_obs::metrics::{Counter, Histogram, Gauge, register_counter};

// Define metrics
lazy_static! {
    static ref REQUESTS_TOTAL: Counter = register_counter!(
        "rustfs_requests_total",
        "Total number of requests",
        &["method", "status"]
    ).unwrap();

    static ref REQUEST_DURATION: Histogram = register_histogram!(
        "rustfs_request_duration_seconds",
        "Request duration in seconds",
        &["method"]
    ).unwrap();

    static ref ACTIVE_CONNECTIONS: Gauge = register_gauge!(
        "rustfs_active_connections",
        "Number of active connections"
    ).unwrap();
}

async fn handle_request(method: &str) -> Result<(), Box<dyn std::error::Error>> {
    let _timer = REQUEST_DURATION.with_label_values(&[method]).start_timer();

    // Increment active connections
    ACTIVE_CONNECTIONS.inc();

    // Simulate request processing
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Record request completion
    REQUESTS_TOTAL.with_label_values(&[method, "success"]).inc();

    // Decrement active connections
    ACTIVE_CONNECTIONS.dec();

    Ok(())
}
```

### Distributed Tracing

```rust
use rustfs_obs::tracing::{trace_fn, Span, SpanContext};
use tracing::{info, instrument};

#[instrument(skip(data))]
async fn process_upload(bucket: &str, key: &str, data: &[u8]) -> Result<String, Box<dyn std::error::Error>> {
    let span = Span::current();
    span.set_attribute("bucket", bucket);
    span.set_attribute("key", key);
    span.set_attribute("size", data.len() as i64);

    info!("Starting upload process");

    // Validate data
    let validation_result = validate_data(data).await?;
    span.add_event("data_validated", &[("result", &validation_result)]);

    // Store data
    let storage_result = store_data(bucket, key, data).await?;
    span.add_event("data_stored", &[("etag", &storage_result.etag)]);

    // Update metadata
    update_metadata(bucket, key, &storage_result).await?;
    span.add_event("metadata_updated", &[]);

    info!("Upload completed successfully");
    Ok(storage_result.etag)
}

#[instrument]
async fn validate_data(data: &[u8]) -> Result<String, Box<dyn std::error::Error>> {
    // Validation logic
    tokio::time::sleep(Duration::from_millis(50)).await;
    Ok("valid".to_string())
}

#[instrument]
async fn store_data(bucket: &str, key: &str, data: &[u8]) -> Result<StorageResult, Box<dyn std::error::Error>> {
    // Storage logic
    tokio::time::sleep(Duration::from_millis(200)).await;
    Ok(StorageResult {
        etag: "d41d8cd98f00b204e9800998ecf8427e".to_string(),
    })
}
```

### Structured Logging

```rust
use rustfs_obs::logging::{LogEvent, LogLevel, StructuredLogger};
use serde_json::json;

async fn logging_example() -> Result<(), Box<dyn std::error::Error>> {
    let logger = StructuredLogger::new();

    // Basic logging
    logger.info("Application started").await;

    // Structured logging with context
    logger.log(LogEvent {
        level: LogLevel::Info,
        message: "Processing upload request".to_string(),
        context: json!({
            "bucket": "example-bucket",
            "key": "example-object",
            "size": 1024,
            "user_id": "user123",
            "request_id": "req-456"
        }),
        timestamp: chrono::Utc::now(),
    }).await;

    // Error logging with details
    logger.error_with_context(
        "Failed to process upload",
        json!({
            "error_code": "STORAGE_FULL",
            "bucket": "example-bucket",
            "available_space": 0,
            "required_space": 1024
        })
    ).await;

    Ok(())
}
```

### Alerting Configuration

```rust
use rustfs_obs::alerting::{AlertManager, AlertRule, NotificationChannel};

async fn setup_alerting() -> Result<(), Box<dyn std::error::Error>> {
    let alert_manager = AlertManager::new().await?;

    // Configure notification channels
    let slack_channel = NotificationChannel::Slack {
        webhook_url: "https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK".to_string(),
        channel: "#rustfs-alerts".to_string(),
    };

    let email_channel = NotificationChannel::Email {
        smtp_server: "smtp.example.com".to_string(),
        recipients: vec!["admin@example.com".to_string()],
    };

    alert_manager.add_notification_channel("slack", slack_channel).await?;
    alert_manager.add_notification_channel("email", email_channel).await?;

    // Define alert rules
    let high_error_rate = AlertRule {
        name: "high_error_rate".to_string(),
        description: "High error rate detected".to_string(),
        condition: "rate(rustfs_requests_total{status!=\"success\"}[5m]) > 0.1".to_string(),
        severity: "critical".to_string(),
        notifications: vec!["slack".to_string(), "email".to_string()],
        cooldown: Duration::from_minutes(15),
    };

    let low_disk_space = AlertRule {
        name: "low_disk_space".to_string(),
        description: "Disk space running low".to_string(),
        condition: "rustfs_disk_usage_percent > 85".to_string(),
        severity: "warning".to_string(),
        notifications: vec!["slack".to_string()],
        cooldown: Duration::from_minutes(30),
    };

    alert_manager.add_rule(high_error_rate).await?;
    alert_manager.add_rule(low_disk_space).await?;

    // Start alert monitoring
    alert_manager.start().await?;

    Ok(())
}
```

### Performance Monitoring

```rust
use rustfs_obs::monitoring::{PerformanceMonitor, SystemMetrics, ApplicationMetrics};

async fn performance_monitoring() -> Result<(), Box<dyn std::error::Error>> {
    let monitor = PerformanceMonitor::new().await?;

    // Start system monitoring
    monitor.start_system_monitoring(Duration::from_secs(10)).await?;

    // Custom application metrics
    let app_metrics = ApplicationMetrics::new();

    // Monitor specific operations
    let upload_metrics = app_metrics.create_operation_monitor("upload");
    let download_metrics = app_metrics.create_operation_monitor("download");

    // Simulate operations with monitoring
    tokio::spawn(async move {
        loop {
            // Monitor upload operation
            let upload_timer = upload_metrics.start_timer();
            simulate_upload().await;
            upload_timer.record_success();

            // Monitor download operation
            let download_timer = download_metrics.start_timer();
            match simulate_download().await {
                Ok(_) => download_timer.record_success(),
                Err(_) => download_timer.record_error(),
            }

            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });

    // Periodic metrics reporting
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));

        loop {
            interval.tick().await;

            let system_metrics = monitor.get_system_metrics().await;
            let app_metrics = monitor.get_application_metrics().await;

            println!("=== System Metrics ===");
            println!("CPU Usage: {:.2}%", system_metrics.cpu_usage);
            println!("Memory Usage: {:.2}%", system_metrics.memory_usage);
            println!("Disk Usage: {:.2}%", system_metrics.disk_usage);

            println!("=== Application Metrics ===");
            println!("Upload Throughput: {:.2} ops/sec", app_metrics.upload_throughput);
            println!("Download Throughput: {:.2} ops/sec", app_metrics.download_throughput);
            println!("Error Rate: {:.2}%", app_metrics.error_rate);
        }
    });

    Ok(())
}
```

### Health Checks

```rust
use rustfs_obs::health::{HealthChecker, HealthStatus, HealthCheck};

async fn setup_health_checks() -> Result<(), Box<dyn std::error::Error>> {
    let health_checker = HealthChecker::new();

    // Add component health checks
    health_checker.add_check("database", Box::new(DatabaseHealthCheck)).await;
    health_checker.add_check("storage", Box::new(StorageHealthCheck)).await;
    health_checker.add_check("cache", Box::new(CacheHealthCheck)).await;

    // Start health monitoring
    health_checker.start_monitoring(Duration::from_secs(30)).await?;

    // Expose health endpoint
    health_checker.expose_http_endpoint("0.0.0.0:8080").await?;

    Ok(())
}

struct DatabaseHealthCheck;

#[async_trait::async_trait]
impl HealthCheck for DatabaseHealthCheck {
    async fn check(&self) -> HealthStatus {
        // Perform database health check
        match check_database_connection().await {
            Ok(_) => HealthStatus::Healthy,
            Err(e) => HealthStatus::Unhealthy(e.to_string()),
        }
    }
}
```

## 🏗️ Architecture

### Observability Architecture

```
Observability Architecture:
┌─────────────────────────────────────────────────────────────┐
│                    Observability API                        │
├─────────────────────────────────────────────────────────────┤
│   Metrics     │   Tracing     │   Logging    │   Alerting   │
├─────────────────────────────────────────────────────────────┤
│              Data Collection & Processing                    │
├─────────────────────────────────────────────────────────────┤
│  Prometheus   │  OpenTelemetry │  Structured  │  Alert Mgr  │
├─────────────────────────────────────────────────────────────┤
│              External Integrations                          │
└─────────────────────────────────────────────────────────────┘
```

### Monitoring Stack

| Component | Purpose | Integration |
|-----------|---------|-------------|
| Prometheus | Metrics storage | Pull-based metrics collection |
| Jaeger | Distributed tracing | OpenTelemetry traces |
| Grafana | Visualization | Dashboards and alerts |
| ELK Stack | Log aggregation | Structured log processing |

## 🧪 Testing

Run the test suite:

```bash
# Run all tests
cargo test

# Test metrics collection
cargo test metrics

# Test tracing functionality
cargo test tracing

# Test alerting
cargo test alerting

# Integration tests
cargo test --test integration
```

## 📋 Requirements

- **Rust**: 1.70.0 or later
- **Platforms**: Linux, macOS, Windows
- **External Services**: Prometheus, Jaeger (optional)
- **Network**: HTTP endpoint exposure capability

## 🌍 Related Projects

This module is part of the RustFS ecosystem:

- [RustFS Main](https://github.com/rustfs/rustfs) - Core distributed storage system
- [RustFS Common](../common) - Common types and utilities
- [RustFS Config](../config) - Configuration management

## 📚 Documentation

For comprehensive documentation, visit:

- [RustFS Documentation](https://docs.rustfs.com)
- [Obs API Reference](https://docs.rustfs.com/obs/)
- [Monitoring Guide](https://docs.rustfs.com/monitoring/)

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
  Made with 📊 by the RustFS Team
</p>
