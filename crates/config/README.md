[![RustFS](https://rustfs.com/images/rustfs-github.png)](https://rustfs.com)

# RustFS Config - Configuration Management

<p align="center">
  <strong>Centralized configuration management for RustFS distributed object storage</strong>
</p>

<p align="center">
  <a href="https://github.com/rustfs/rustfs/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/rustfs/rustfs/actions/workflows/ci.yml/badge.svg" /></a>
  <a href="https://docs.rustfs.com/en/">📖 Documentation</a>
  · <a href="https://github.com/rustfs/rustfs/issues">🐛 Bug Reports</a>
  · <a href="https://github.com/rustfs/rustfs/discussions">💬 Discussions</a>
</p>

---

## 📖 Overview

**RustFS Config** is the configuration management module for the [RustFS](https://rustfs.com) distributed object storage system. It provides centralized configuration handling, environment-based configuration loading, validation, and runtime configuration updates for all RustFS components.

> **Note:** This is a foundational submodule of RustFS that provides essential configuration management capabilities for the distributed object storage system. For the complete RustFS experience, please visit the [main RustFS repository](https://github.com/rustfs/rustfs).

## ✨ Features

### ⚙️ Configuration Management

- **Multi-Format Support**: JSON, YAML, TOML configuration formats
- **Environment Variables**: Automatic environment variable override
- **Default Values**: Comprehensive default configuration
- **Validation**: Configuration validation and error reporting

### 🔧 Advanced Features

- **Hot Reload**: Runtime configuration updates without restart
- **Profile Support**: Environment-specific configuration profiles
- **Secret Management**: Secure handling of sensitive configuration
- **Configuration Merging**: Hierarchical configuration composition

### 🛠️ Developer Features

- **Type Safety**: Strongly typed configuration structures
- **Documentation**: Auto-generated configuration documentation
- **CLI Integration**: Command-line configuration override
- **Testing Support**: Configuration mocking for tests

## 📦 Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
rustfs-config = "0.1.0"

# With specific features
rustfs-config = { version = "0.1.0", features = ["constants", "notify"] }
```

### Feature Flags

Available features:

- `constants` - Configuration constants and compile-time values
- `notify` - Configuration change notification support
- `observability` - Observability and metrics configuration
- `default` - Core configuration functionality

## 🔧 Usage

### Basic Configuration Loading

```rust
use rustfs_config::{Config, ConfigBuilder, ConfigFormat};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load configuration from file
    let config = Config::from_file("config.yaml")?;

    // Load with environment overrides
    let config = ConfigBuilder::new()
        .add_file("config.yaml")
        .add_env_prefix("RUSTFS")
        .build()?;

    // Access configuration values
    println!("Server address: {}", config.server.address);
    println!("Storage path: {}", config.storage.path);

    Ok(())
}
```

### Environment-Based Configuration

```rust
use rustfs_config::{Config, Environment};

async fn load_environment_config() -> Result<(), Box<dyn std::error::Error>> {
    // Load configuration based on environment
    let env = Environment::detect()?;
    let config = Config::for_environment(env).await?;

    match env {
        Environment::Development => {
            println!("Using development configuration");
            println!("Debug mode: {}", config.debug.enabled);
        }
        Environment::Production => {
            println!("Using production configuration");
            println!("Log level: {}", config.logging.level);
        }
        Environment::Testing => {
            println!("Using test configuration");
            println!("Test database: {}", config.database.test_url);
        }
    }

    Ok(())
}
```

### Configuration Structure

```rust
use rustfs_config::{Config, ServerConfig, StorageConfig, SecurityConfig};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct ApplicationConfig {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub security: SecurityConfig,
    pub logging: LoggingConfig,
    pub monitoring: MonitoringConfig,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ServerConfig {
    pub address: String,
    pub port: u16,
    pub workers: usize,
    pub timeout: std::time::Duration,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct StorageConfig {
    pub path: String,
    pub max_size: u64,
    pub compression: bool,
    pub erasure_coding: ErasureCodingConfig,
}

fn load_typed_config() -> Result<ApplicationConfig, Box<dyn std::error::Error>> {
    let config: ApplicationConfig = Config::builder()
        .add_file("config.yaml")
        .add_env_prefix("RUSTFS")
        .set_default("server.port", 9000)?
        .set_default("server.workers", 4)?
        .build_typed()?;

    Ok(config)
}
```

### Configuration Validation

```rust
use rustfs_config::{Config, ValidationError, Validator};

#[derive(Debug)]
pub struct ConfigValidator;

impl Validator<ApplicationConfig> for ConfigValidator {
    fn validate(&self, config: &ApplicationConfig) -> Result<(), ValidationError> {
        // Validate server configuration
        if config.server.port < 1024 {
            return Err(ValidationError::new("server.port", "Port must be >= 1024"));
        }

        if config.server.workers == 0 {
            return Err(ValidationError::new("server.workers", "Workers must be > 0"));
        }

        // Validate storage configuration
        if !std::path::Path::new(&config.storage.path).exists() {
            return Err(ValidationError::new("storage.path", "Storage path does not exist"));
        }

        // Validate erasure coding parameters
        if config.storage.erasure_coding.data_drives + config.storage.erasure_coding.parity_drives > 16 {
            return Err(ValidationError::new("storage.erasure_coding", "Total drives cannot exceed 16"));
        }

        Ok(())
    }
}

fn validate_configuration() -> Result<(), Box<dyn std::error::Error>> {
    let config: ApplicationConfig = Config::load_with_validation(
        "config.yaml",
        ConfigValidator,
    )?;

    println!("Configuration is valid!");
    Ok(())
}
```

### Hot Configuration Reload

```rust
use rustfs_config::{ConfigWatcher, ConfigEvent};
use tokio::sync::mpsc;

async fn watch_configuration_changes() -> Result<(), Box<dyn std::error::Error>> {
    let (tx, mut rx) = mpsc::channel::<ConfigEvent>(100);

    // Start configuration watcher
    let watcher = ConfigWatcher::new("config.yaml", tx)?;
    watcher.start().await?;

    // Handle configuration changes
    while let Some(event) = rx.recv().await {
        match event {
            ConfigEvent::Changed(new_config) => {
                println!("Configuration changed, reloading...");
                // Apply new configuration
                apply_configuration(new_config).await?;
            }
            ConfigEvent::Error(err) => {
                eprintln!("Configuration error: {}", err);
            }
        }
    }

    Ok(())
}

async fn apply_configuration(config: ApplicationConfig) -> Result<(), Box<dyn std::error::Error>> {
    // Update server configuration
    // Update storage configuration
    // Update security settings
    // etc.
    Ok(())
}
```

### Configuration Profiles

```rust
use rustfs_config::{Config, Profile, ProfileManager};

fn load_profile_based_config() -> Result<(), Box<dyn std::error::Error>> {
    let profile_manager = ProfileManager::new("configs/")?;

    // Load specific profile
    let config = profile_manager.load_profile("production")?;

    // Load with fallback
    let config = profile_manager
        .load_profile("staging")
        .or_else(|_| profile_manager.load_profile("default"))?;

    // Merge multiple profiles
    let config = profile_manager
        .merge_profiles(&["base", "production", "regional"])?;

    Ok(())
}
```

## 🏗️ Architecture

### Configuration Architecture

```
Config Architecture:
┌─────────────────────────────────────────────────────────────┐
│                    Configuration API                        │
├─────────────────────────────────────────────────────────────┤
│   File Loader   │   Env Loader    │   CLI Parser           │
├─────────────────────────────────────────────────────────────┤
│              Configuration Merger                           │
├─────────────────────────────────────────────────────────────┤
│   Validation    │    Watching     │   Hot Reload           │
├─────────────────────────────────────────────────────────────┤
│              Type System Integration                        │
└─────────────────────────────────────────────────────────────┘
```

### Configuration Sources

| Source | Priority | Format | Example |
|--------|----------|---------|---------|
| Command Line | 1 (Highest) | Key-Value | `--server.port=8080` |
| Environment Variables | 2 | Key-Value | `RUSTFS_SERVER_PORT=8080` |
| Configuration File | 3 | JSON/YAML/TOML | `config.yaml` |
| Default Values | 4 (Lowest) | Code | Compile-time defaults |

## 📋 Configuration Reference

### Server Configuration

```yaml
server:
  address: "0.0.0.0"
  port: 9000
  workers: 4
  timeout: "30s"
  tls:
    enabled: true
    cert_file: "/etc/ssl/server.crt"
    key_file: "/etc/ssl/server.key"
```

### Storage Configuration

```yaml
storage:
  path: "/var/lib/rustfs"
  max_size: "1TB"
  compression: true
  erasure_coding:
    data_drives: 8
    parity_drives: 4
    stripe_size: "1MB"
```

### Security Configuration

```yaml
security:
  auth:
    enabled: true
    method: "jwt"
    secret_key: "${JWT_SECRET}"
  encryption:
    algorithm: "AES-256-GCM"
    key_rotation_interval: "24h"
```

## 🧪 Testing

Run the test suite:

```bash
# Run all tests
cargo test

# Test configuration loading
cargo test config_loading

# Test validation
cargo test validation

# Test hot reload
cargo test hot_reload
```

## 📋 Requirements

- **Rust**: 1.70.0 or later
- **Platforms**: Linux, macOS, Windows
- **Dependencies**: Minimal external dependencies

## 🌍 Related Projects

This module is part of the RustFS ecosystem:

- [RustFS Main](https://github.com/rustfs/rustfs) - Core distributed storage system
- [RustFS Utils](../utils) - Utility functions
- [RustFS Common](../common) - Common types and utilities

## 📚 Documentation

For comprehensive documentation, visit:

- [RustFS Documentation](https://docs.rustfs.com)
- [Config API Reference](https://docs.rustfs.com/config/)

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
  Made with ⚙️ by the RustFS Team
</p>
