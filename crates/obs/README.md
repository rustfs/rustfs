[![RustFS](https://rustfs.com/images/rustfs-github.png)](https://rustfs.com)

# RustFS Obs - Observability & Monitoring

<p align="center">
  <strong>Comprehensive observability and monitoring system for RustFS distributed object storage</strong>
</p>

<p align="center">
  <a href="https://github.com/rustfs/rustfs/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/rustfs/rustfs/actions/workflows/ci.yml/badge.svg" /></a>
  <a href="https://docs.rustfs.com/">📖 Documentation</a>
  · <a href="https://github.com/rustfs/rustfs/issues">🐛 Bug Reports</a>
  · <a href="https://github.com/rustfs/rustfs/discussions">💬 Discussions</a>
</p>

---

## 📖 Overview

**RustFS Obs** provides comprehensive observability and monitoring capabilities for the [RustFS](https://rustfs.com) distributed object storage system. For the complete RustFS experience, please visit the [main RustFS repository](https://github.com/rustfs/rustfs).

## ✨ Features

- **Environment-Aware Logging**: Automatically configures logging behavior based on deployment environment
  - Production: File-only logging (stdout disabled by default for security and log aggregation)
  - Development/Test: Full logging with stdout support for debugging
- OpenTelemetry integration for distributed tracing
- Prometheus metrics collection and exposition
- Structured logging with configurable levels and rotation
- Performance profiling and analytics
- Real-time health checks and status monitoring
- Custom dashboards and alerting integration
- Enhanced error handling and resilience

## 🚀 Environment-Aware Logging

The obs module automatically adapts logging behavior based on your deployment environment:

### Production Environment
```bash
# Set production environment - disables stdout logging by default
export RUSTFS_OBS_ENVIRONMENT=production

# All logs go to files only (no stdout) for security and log aggregation
# Enhanced error handling with clear failure diagnostics
```

### Development/Test Environment
```bash
# Set development environment - enables stdout logging
export RUSTFS_OBS_ENVIRONMENT=development

# Logs appear both in files and stdout for easier debugging
# Full span tracking and verbose error messages
```

### Configuration Override
You can always override the environment defaults:
```rust
use rustfs_obs::OtelConfig;

let config = OtelConfig {
    endpoint: "".to_string(),
    use_stdout: Some(true), // Explicit override - forces stdout even in production
    environment: Some("production".to_string()),
    ..Default::default()
};
```

### Supported Environment Values
- `production` - Secure file-only logging
- `development` - Full debugging with stdout
- `test` - Test environment with stdout support  
- `staging` - Staging environment with stdout support

## 📚 Documentation

For comprehensive documentation, examples, and usage guides, please visit the main [RustFS repository](https://github.com/rustfs/rustfs).

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](../../LICENSE) file for details.
