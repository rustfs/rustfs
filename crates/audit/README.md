# rustfs-audit

**Audit Target Management System for RustFS**

`rustfs-audit` is a comprehensive audit logging system designed for RustFS. It provides multi-target fan-out, hot reload
capabilities, and rich observability features for distributed storage and event-driven systems.

## Features

- **Multi-Target Fan-Out:** Dispatch audit logs to multiple targets (e.g., Webhook, MQTT) concurrently.
- **Hot Reload:** Dynamically reload configuration and update targets without downtime.
- **Observability:** Collect metrics such as EPS (Events Per Second), average latency, error rate, and target success
  rate.
- **Performance Validation:** Validate system performance against requirements and receive optimization recommendations.
- **Extensible Registry:** Manage audit targets with add, remove, enable, disable, and upsert operations.
- **Global Singleton:** Easy-to-use global audit system and logger.
- **Async & Thread-Safe:** Built on Tokio and Rust async primitives for high concurrency.

## Getting Started

### Add Dependency

Add to your `Cargo.toml`:

```toml
[dependencies]
rustfs-audit = "0.1"
```

### Basic Usage

#### Initialize and Start Audit System

```rust
use rustfs_audit::{start_audit_system, AuditLogger};
use rustfs_ecstore::config::Config;

#[tokio::main]
async fn main() {
    let config = Config::load("path/to/config.toml").await.unwrap();
    start_audit_system(config).await.unwrap();
}
```

#### Log an Audit Entry

```rust
use rustfs_audit::{AuditEntry, AuditLogger, ApiDetails};
use chrono::Utc;
use rustfs_targets::EventName;

let entry = AuditEntry::new(
"v1".to_string(),
Some("deployment-123".to_string()),
Some("siteA".to_string()),
Utc::now(),
EventName::ObjectCreatedPut,
Some("type".to_string()),
"trigger".to_string(),
ApiDetails::default (),
);

AuditLogger::log(entry).await;
```

#### Observability & Metrics

```rust
use rustfs_audit::{get_metrics_report, validate_performance};

let report = get_metrics_report().await;
println!("{}", report.format());

let validation = validate_performance().await;
println!("{}", validation.format());
```

## Configuration

Targets are configured via TOML files and environment variables. Supported target types:

- **Webhook**
- **MQTT**

Environment variables override file configuration.
See [docs.rs/rustfs-audit](https://docs.rs/rustfs-audit/latest/rustfs_audit/) for details.

## API Overview

- `AuditSystem`: Main system for managing targets and dispatching logs.
- `AuditRegistry`: Registry for audit targets.
- `AuditEntry`: Audit log entry structure.
- `ApiDetails`: API call details for audit logs.
- `AuditLogger`: Global logger singleton.
- `AuditMetrics`, `AuditMetricsReport`: Metrics and reporting.
- `PerformanceValidation`: Performance validation and recommendations.

## Observability

- **Metrics:** EPS, average latency, error rate, target success rate, processed/failed events, config reloads, system
  starts.
- **Validation:** Checks if EPS ≥ 3000, latency ≤ 30ms, error rate ≤ 1%. Provides actionable recommendations.

## Contributing

Issues and PRs are welcome!  
See [docs.rs/rustfs-audit](https://docs.rs/rustfs-audit/latest/rustfs_audit/) for detailed developer documentation.

## License

Apache License 2.0

## Documentation

For detailed API documentation, refer to source code comments
and [docs.rs documentation](https://docs.rs/rustfs-audit/latest/rustfs_audit/).

---

**Note:**  
This crate is designed for use within the RustFS ecosystem and may depend on other RustFS crates such as
`rustfs-targets`, `rustfs-config`, and `rustfs-ecstore`.  
For integration examples and advanced usage, see the [docs.rs](https://docs.rs/rustfs-audit/latest/rustfs_audit/)
documentation.