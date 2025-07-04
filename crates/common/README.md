[![RustFS](https://rustfs.com/images/rustfs-github.png)](https://rustfs.com)

# RustFS Common - Shared Components

<p align="center">
  <strong>Common types, utilities, and shared components for RustFS distributed object storage</strong>
</p>

<p align="center">
  <a href="https://github.com/rustfs/rustfs/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/rustfs/rustfs/actions/workflows/ci.yml/badge.svg" /></a>
  <a href="https://docs.rustfs.com/en/">📖 Documentation</a>
  · <a href="https://github.com/rustfs/rustfs/issues">🐛 Bug Reports</a>
  · <a href="https://github.com/rustfs/rustfs/discussions">💬 Discussions</a>
</p>

---

## 📖 Overview

**RustFS Common** provides shared components, types, and utilities used across all RustFS modules. This foundational library ensures consistency, reduces code duplication, and provides essential building blocks for the [RustFS](https://rustfs.com) distributed object storage system.

> **Note:** This is a foundational submodule of RustFS that provides essential shared components for the distributed object storage system. For the complete RustFS experience, please visit the [main RustFS repository](https://github.com/rustfs/rustfs).

## ✨ Features

### 🔧 Core Types

- **Common Data Structures**: Shared types and enums
- **Error Handling**: Unified error types and utilities
- **Result Types**: Consistent result handling patterns
- **Constants**: System-wide constants and defaults

### 🛠️ Utilities

- **Async Helpers**: Common async patterns and utilities
- **Serialization**: Shared serialization utilities
- **Logging**: Common logging and tracing setup
- **Metrics**: Shared metrics and observability

### 🌐 Network Components

- **gRPC Common**: Shared gRPC types and utilities
- **Protocol Helpers**: Common protocol implementations
- **Connection Management**: Shared connection utilities
- **Request/Response Types**: Common API types

## 📦 Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
rustfs-common = "0.1.0"
```

## 🔧 Usage

### Basic Common Types

```rust
use rustfs_common::{Result, Error, ObjectInfo, BucketInfo};

fn main() -> Result<()> {
    // Use common result type
    let result = some_operation()?;

    // Use common object info
    let object = ObjectInfo {
        name: "example.txt".to_string(),
        size: 1024,
        etag: "d41d8cd98f00b204e9800998ecf8427e".to_string(),
        last_modified: chrono::Utc::now(),
        content_type: "text/plain".to_string(),
    };

    println!("Object: {} ({} bytes)", object.name, object.size);
    Ok(())
}
```

### Error Handling

```rust
use rustfs_common::{Error, ErrorKind, Result};

fn example_operation() -> Result<String> {
    // Different error types
    match some_condition {
        true => Ok("Success".to_string()),
        false => Err(Error::new(
            ErrorKind::InvalidInput,
            "Invalid operation parameters"
        )),
    }
}

fn handle_errors() {
    match example_operation() {
        Ok(value) => println!("Success: {}", value),
        Err(e) => {
            match e.kind() {
                ErrorKind::InvalidInput => println!("Input error: {}", e),
                ErrorKind::NotFound => println!("Not found: {}", e),
                ErrorKind::PermissionDenied => println!("Access denied: {}", e),
                _ => println!("Other error: {}", e),
            }
        }
    }
}
```

### Async Utilities

```rust
use rustfs_common::async_utils::{timeout_with_default, retry_with_backoff, spawn_task};
use std::time::Duration;

async fn async_operations() -> Result<()> {
    // Timeout with default value
    let result = timeout_with_default(
        Duration::from_secs(5),
        expensive_operation(),
        "default_value".to_string()
    ).await;

    // Retry with exponential backoff
    let result = retry_with_backoff(
        3, // max attempts
        Duration::from_millis(100), // initial delay
        || async { fallible_operation().await }
    ).await?;

    // Spawn background task
    spawn_task("background-worker", async {
        background_work().await;
    });

    Ok(())
}
```

### Metrics and Observability

```rust
use rustfs_common::metrics::{Counter, Histogram, Gauge, MetricsRegistry};

fn setup_metrics() -> Result<()> {
    let registry = MetricsRegistry::new();

    // Create metrics
    let requests_total = Counter::new("requests_total", "Total number of requests")?;
    let request_duration = Histogram::new(
        "request_duration_seconds",
        "Request duration in seconds"
    )?;
    let active_connections = Gauge::new(
        "active_connections",
        "Number of active connections"
    )?;

    // Register metrics
    registry.register(Box::new(requests_total))?;
    registry.register(Box::new(request_duration))?;
    registry.register(Box::new(active_connections))?;

    Ok(())
}
```

### gRPC Common Types

```rust
use rustfs_common::grpc::{GrpcResult, GrpcError, TonicStatus};
use tonic::{Request, Response, Status};

async fn grpc_service_example(
    request: Request<MyRequest>
) -> GrpcResult<MyResponse> {
    let req = request.into_inner();

    // Validate request
    if req.name.is_empty() {
        return Err(GrpcError::invalid_argument("Name cannot be empty"));
    }

    // Process request
    let response = MyResponse {
        result: format!("Processed: {}", req.name),
        status: "success".to_string(),
    };

    Ok(Response::new(response))
}

// Error conversion
impl From<Error> for Status {
    fn from(err: Error) -> Self {
        match err.kind() {
            ErrorKind::NotFound => Status::not_found(err.to_string()),
            ErrorKind::PermissionDenied => Status::permission_denied(err.to_string()),
            ErrorKind::InvalidInput => Status::invalid_argument(err.to_string()),
            _ => Status::internal(err.to_string()),
        }
    }
}
```

## 🏗️ Architecture

### Common Module Structure

```
Common Architecture:
┌─────────────────────────────────────────────────────────────┐
│                    Public API Layer                         │
├─────────────────────────────────────────────────────────────┤
│  Core Types    │   Error Types   │   Result Types           │
├─────────────────────────────────────────────────────────────┤
│  Async Utils   │   Metrics       │   gRPC Common            │
├─────────────────────────────────────────────────────────────┤
│  Constants     │   Serialization │   Logging                │
├─────────────────────────────────────────────────────────────┤
│              Foundation Types                                │
└─────────────────────────────────────────────────────────────┘
```

### Core Components

| Component | Purpose | Usage |
|-----------|---------|-------|
| Types | Common data structures | Shared across all modules |
| Errors | Unified error handling | Consistent error reporting |
| Async Utils | Async patterns | Common async operations |
| Metrics | Observability | Performance monitoring |
| gRPC | Protocol support | Service communication |

## 🧪 Testing

Run the test suite:

```bash
# Run all tests
cargo test

# Test specific components
cargo test types
cargo test errors
cargo test async_utils
```

## 📋 Requirements

- **Rust**: 1.70.0 or later
- **Platforms**: Linux, macOS, Windows
- **Dependencies**: Minimal, focused on essential functionality

## 🌍 Related Projects

This module is part of the RustFS ecosystem:

- [RustFS Main](https://github.com/rustfs/rustfs) - Core distributed storage system
- [RustFS Utils](../utils) - Utility functions
- [RustFS Config](../config) - Configuration management

## 📚 Documentation

For comprehensive documentation, visit:

- [RustFS Documentation](https://docs.rustfs.com)
- [Common API Reference](https://docs.rustfs.com/common/)

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
  Made with 🔧 by the RustFS Team
</p>
