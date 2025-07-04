[![RustFS](https://rustfs.com/images/rustfs-github.png)](https://rustfs.com)

# RustFS MadAdmin - Administrative Interface

<p align="center">
  <strong>Administrative interface and management APIs for RustFS distributed object storage</strong>
</p>

<p align="center">
  <a href="https://github.com/rustfs/rustfs/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/rustfs/rustfs/actions/workflows/ci.yml/badge.svg" /></a>
  <a href="https://docs.rustfs.com/en/">📖 Documentation</a>
  · <a href="https://github.com/rustfs/rustfs/issues">🐛 Bug Reports</a>
  · <a href="https://github.com/rustfs/rustfs/discussions">💬 Discussions</a>
</p>

---

## 📖 Overview

**RustFS MadAdmin** provides comprehensive administrative interfaces and management APIs for the [RustFS](https://rustfs.com) distributed object storage system. It enables cluster management, monitoring, configuration, and administrative operations through both programmatic APIs and interactive interfaces.

> **Note:** This is a core submodule of RustFS that provides essential administrative capabilities for the distributed object storage system. For the complete RustFS experience, please visit the [main RustFS repository](https://github.com/rustfs/rustfs).

## ✨ Features

### 🎛️ Cluster Management

- **Node Management**: Add, remove, and monitor cluster nodes
- **Service Discovery**: Automatic service discovery and registration
- **Load Balancing**: Distribute load across cluster nodes
- **Health Monitoring**: Real-time cluster health monitoring

### 📊 System Monitoring

- **Performance Metrics**: CPU, memory, disk, and network metrics
- **Storage Analytics**: Capacity planning and usage analytics
- **Alert Management**: Configurable alerts and notifications
- **Dashboard Interface**: Web-based monitoring dashboard

### ⚙️ Configuration Management

- **Dynamic Configuration**: Runtime configuration updates
- **Policy Management**: Access control and bucket policies
- **User Management**: User and group administration
- **Backup Configuration**: Backup and restore settings

### 🔧 Administrative Operations

- **Data Migration**: Cross-cluster data migration
- **Healing Operations**: Data integrity repair and healing
- **Rebalancing**: Storage rebalancing operations
- **Maintenance Mode**: Graceful maintenance operations

## 📦 Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
rustfs-madmin = "0.1.0"
```

## 🔧 Usage

### Basic Admin Client

```rust
use rustfs_madmin::{AdminClient, AdminConfig, ServerInfo};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create admin client
    let config = AdminConfig {
        endpoint: "https://admin.rustfs.local:9001".to_string(),
        access_key: "admin".to_string(),
        secret_key: "password".to_string(),
        region: "us-east-1".to_string(),
    };

    let client = AdminClient::new(config).await?;

    // Get server information
    let server_info = client.server_info().await?;
    println!("Server Version: {}", server_info.version);
    println!("Uptime: {}", server_info.uptime);

    Ok(())
}
```

### Cluster Management

```rust
use rustfs_madmin::{AdminClient, AddServerRequest, RemoveServerRequest};

async fn cluster_management(client: &AdminClient) -> Result<(), Box<dyn std::error::Error>> {
    // List cluster nodes
    let nodes = client.list_servers().await?;
    for node in nodes {
        println!("Node: {} - Status: {}", node.endpoint, node.state);
    }

    // Add new server to cluster
    let add_request = AddServerRequest {
        endpoint: "https://new-node.rustfs.local:9000".to_string(),
        access_key: "node-key".to_string(),
        secret_key: "node-secret".to_string(),
    };

    client.add_server(add_request).await?;
    println!("New server added successfully");

    // Remove server from cluster
    let remove_request = RemoveServerRequest {
        endpoint: "https://old-node.rustfs.local:9000".to_string(),
    };

    client.remove_server(remove_request).await?;
    println!("Server removed successfully");

    Ok(())
}
```

### System Monitoring

```rust
use rustfs_madmin::{AdminClient, MetricsRequest, AlertConfig};

async fn monitoring_operations(client: &AdminClient) -> Result<(), Box<dyn std::error::Error>> {
    // Get system metrics
    let metrics = client.get_metrics(MetricsRequest::default()).await?;

    println!("CPU Usage: {:.2}%", metrics.cpu_usage);
    println!("Memory Usage: {:.2}%", metrics.memory_usage);
    println!("Disk Usage: {:.2}%", metrics.disk_usage);

    // Get storage information
    let storage_info = client.storage_info().await?;
    println!("Total Capacity: {} GB", storage_info.total_capacity / 1024 / 1024 / 1024);
    println!("Used Capacity: {} GB", storage_info.used_capacity / 1024 / 1024 / 1024);

    // Configure alerts
    let alert_config = AlertConfig {
        name: "high-cpu-usage".to_string(),
        condition: "cpu_usage > 80".to_string(),
        notification_endpoint: "https://webhook.example.com/alerts".to_string(),
        enabled: true,
    };

    client.set_alert_config(alert_config).await?;

    Ok(())
}
```

### User and Policy Management

```rust
use rustfs_madmin::{AdminClient, UserInfo, PolicyDocument};

async fn user_management(client: &AdminClient) -> Result<(), Box<dyn std::error::Error>> {
    // Create user
    let user_info = UserInfo {
        access_key: "user123".to_string(),
        secret_key: "user-secret".to_string(),
        status: "enabled".to_string(),
        policy: Some("readwrite-policy".to_string()),
    };

    client.add_user("new-user", user_info).await?;

    // List users
    let users = client.list_users().await?;
    for (username, info) in users {
        println!("User: {} - Status: {}", username, info.status);
    }

    // Set user policy
    let policy_doc = PolicyDocument {
        version: "2012-10-17".to_string(),
        statement: vec![/* policy statements */],
    };

    client.set_user_policy("new-user", policy_doc).await?;

    Ok(())
}
```

### Data Operations

```rust
use rustfs_madmin::{AdminClient, HealRequest, RebalanceRequest};

async fn data_operations(client: &AdminClient) -> Result<(), Box<dyn std::error::Error>> {
    // Start healing operation
    let heal_request = HealRequest {
        bucket: Some("important-bucket".to_string()),
        prefix: Some("documents/".to_string()),
        recursive: true,
        dry_run: false,
    };

    let heal_result = client.heal(heal_request).await?;
    println!("Healing started: {}", heal_result.heal_sequence);

    // Check healing status
    let heal_status = client.heal_status(&heal_result.heal_sequence).await?;
    println!("Healing progress: {:.2}%", heal_status.progress);

    // Start rebalancing
    let rebalance_request = RebalanceRequest {
        servers: vec![], // Empty means all servers
    };

    client.start_rebalance(rebalance_request).await?;
    println!("Rebalancing started");

    Ok(())
}
```

### Configuration Management

```rust
use rustfs_madmin::{AdminClient, ConfigUpdate, NotificationTarget};

async fn configuration_management(client: &AdminClient) -> Result<(), Box<dyn std::error::Error>> {
    // Get current configuration
    let config = client.get_config().await?;
    println!("Current config version: {}", config.version);

    // Update configuration
    let config_update = ConfigUpdate {
        region: Some("us-west-2".to_string()),
        browser: Some(true),
        compression: Some(true),
        // ... other config fields
    };

    client.set_config(config_update).await?;

    // Configure notification targets
    let notification_target = NotificationTarget {
        arn: "arn:aws:sns:us-east-1:123456789012:my-topic".to_string(),
        target_type: "webhook".to_string(),
        endpoint: "https://webhook.example.com/notifications".to_string(),
    };

    client.set_notification_target("bucket1", notification_target).await?;

    Ok(())
}
```

## 🏗️ Architecture

### MadAdmin Architecture

```
MadAdmin Architecture:
┌─────────────────────────────────────────────────────────────┐
│                    Admin API Layer                          │
├─────────────────────────────────────────────────────────────┤
│   Cluster Mgmt   │   Monitoring    │   User Management      │
├─────────────────────────────────────────────────────────────┤
│   Data Ops       │   Config Mgmt   │   Notification         │
├─────────────────────────────────────────────────────────────┤
│              HTTP/gRPC Client Layer                         │
├─────────────────────────────────────────────────────────────┤
│              Storage System Integration                      │
└─────────────────────────────────────────────────────────────┘
```

### Administrative Operations

| Category | Operations | Description |
|----------|------------|-------------|
| Cluster | Add/Remove nodes, Health checks | Cluster management |
| Monitoring | Metrics, Alerts, Dashboard | System monitoring |
| Data | Healing, Rebalancing, Migration | Data operations |
| Config | Settings, Policies, Notifications | Configuration |
| Users | Authentication, Authorization | User management |

## 🧪 Testing

Run the test suite:

```bash
# Run all tests
cargo test

# Test admin operations
cargo test admin_ops

# Test cluster management
cargo test cluster

# Test monitoring
cargo test monitoring
```

## 📋 Requirements

- **Rust**: 1.70.0 or later
- **Platforms**: Linux, macOS, Windows
- **Network**: Administrative access to RustFS cluster
- **Permissions**: Administrative credentials required

## 🌍 Related Projects

This module is part of the RustFS ecosystem:

- [RustFS Main](https://github.com/rustfs/rustfs) - Core distributed storage system
- [RustFS IAM](../iam) - Identity and access management
- [RustFS Policy](../policy) - Policy engine
- [RustFS Common](../common) - Common types and utilities

## 📚 Documentation

For comprehensive documentation, visit:

- [RustFS Documentation](https://docs.rustfs.com)
- [MadAdmin API Reference](https://docs.rustfs.com/madmin/)
- [Administrative Guide](https://docs.rustfs.com/admin/)

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
  Made with 🎛️ by the RustFS Team
</p>
