[![RustFS](https://rustfs.com/images/rustfs-github.png)](https://rustfs.com)

# RustFS Config - Configuration Management

<p align="center">
  <strong>Configuration management and validation module for RustFS distributed object storage</strong>
</p>

<p align="center">
  <a href="https://github.com/rustfs/rustfs/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/rustfs/rustfs/actions/workflows/ci.yml/badge.svg" /></a>
  <a href="https://docs.rustfs.com/">📖 Documentation</a>
  · <a href="https://github.com/rustfs/rustfs/issues">🐛 Bug Reports</a>
  · <a href="https://github.com/rustfs/rustfs/discussions">💬 Discussions</a>
</p>

---

## 📖 Overview

**RustFS Config** provides configuration management and validation capabilities for the [RustFS](https://rustfs.com) distributed object storage system. For the complete RustFS experience, please visit the [main RustFS repository](https://github.com/rustfs/rustfs).

## ✨ Features

- Multi-format configuration support (TOML, YAML, JSON, ENV)
- Environment variable integration and override
- Configuration validation and type safety
- Hot-reload capabilities for dynamic updates
- Default value management and fallbacks
- Secure credential handling and encryption

## 📚 Documentation

For comprehensive documentation, examples, and usage guides, please visit the main [RustFS repository](https://github.com/rustfs/rustfs).

## Environment Variable Naming Conventions

RustFS uses a flat naming style for top-level configuration: environment variables are `RUSTFS_*` without nested module segments.

Examples:
- `RUSTFS_REGION`
- `RUSTFS_ADDRESS`
- `RUSTFS_VOLUMES`
- `RUSTFS_LICENSE`

Current guidance:
- Prefer module-specific names only when they are not top-level product configuration.
- Renamed variables must keep backward-compatible aliases until before beta.
- Alias usage must emit deprecation warnings and be treated as transitional only.
- Deprecated example:
  - `RUSTFS_ENABLE_SCANNER` -> `RUSTFS_SCANNER_ENABLED`
  - `RUSTFS_ENABLE_HEAL` -> `RUSTFS_HEAL_ENABLED`
  - `RUSTFS_DATA_SCANNER_START_DELAY_SECS` -> `RUSTFS_SCANNER_START_DELAY_SECS`

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](../../LICENSE) file for details.
