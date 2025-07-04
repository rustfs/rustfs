[![RustFS](https://rustfs.com/images/rustfs-github.png)](https://rustfs.com)

# RustFS FileMeta - File Metadata Management

<p align="center">
  <strong>High-performance file metadata management for RustFS distributed object storage</strong>
</p>

<p align="center">
  <a href="https://github.com/rustfs/rustfs/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/rustfs/rustfs/actions/workflows/ci.yml/badge.svg" /></a>
  <a href="https://docs.rustfs.com/en/">📖 Documentation</a>
  · <a href="https://github.com/rustfs/rustfs/issues">🐛 Bug Reports</a>
  · <a href="https://github.com/rustfs/rustfs/discussions">💬 Discussions</a>
</p>

---

## 📖 Overview

**RustFS FileMeta** is the metadata management module for the [RustFS](https://rustfs.com) distributed object storage system. It provides efficient storage, retrieval, and management of file metadata, supporting features like versioning, tagging, and extended attributes with high performance and reliability.

> **Note:** This is a core submodule of RustFS that provides essential metadata management capabilities for the distributed object storage system. For the complete RustFS experience, please visit the [main RustFS repository](https://github.com/rustfs/rustfs).

## ✨ Features

### 📝 Metadata Management
- **File Information**: Complete file metadata including size, timestamps, and checksums
- **Object Versioning**: Version-aware metadata management
- **Extended Attributes**: Custom metadata and tagging support
- **Inline Metadata**: Optimized storage for small metadata

### 🚀 Performance Features
- **FlatBuffers Serialization**: Zero-copy metadata serialization
- **Efficient Storage**: Optimized metadata storage layout
- **Fast Lookups**: High-performance metadata queries
- **Batch Operations**: Bulk metadata operations

### 🔧 Advanced Capabilities
- **Schema Evolution**: Forward and backward compatible metadata schemas
- **Compression**: Metadata compression for space efficiency
- **Validation**: Metadata integrity verification
- **Migration**: Seamless metadata format migration

## 📦 Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
rustfs-filemeta = "0.1.0"
```

## 🔧 Usage

### Basic Metadata Operations

```rust
use rustfs_filemeta::{FileInfo, XLMeta};
use std::collections::HashMap;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create file metadata
    let mut file_info = FileInfo::new();
    file_info.name = "example.txt".to_string();
    file_info.size = 1024;
    file_info.mod_time = chrono::Utc::now();

    // Add custom metadata
    let mut user_defined = HashMap::new();
    user_defined.insert("author".to_string(), "john@example.com".to_string());
    user_defined.insert("department".to_string(), "engineering".to_string());
    file_info.user_defined = user_defined;

    // Create XL metadata
    let xl_meta = XLMeta::new(file_info);

    // Serialize metadata
    let serialized = xl_meta.serialize()?;

    // Deserialize metadata
    let deserialized = XLMeta::deserialize(&serialized)?;

    println!("File: {}, Size: {}", deserialized.file_info.name, deserialized.file_info.size);

    Ok(())
}
```

### Advanced Metadata Management

```rust
use rustfs_filemeta::{XLMeta, FileInfo, VersionInfo};

async fn advanced_metadata_example() -> Result<(), Box<dyn std::error::Error>> {
    // Create versioned metadata
    let mut xl_meta = XLMeta::new(FileInfo::default());

    // Set version information
    xl_meta.set_version_info(VersionInfo {
        version_id: "v1.0.0".to_string(),
        is_latest: true,
        delete_marker: false,
        restore_ongoing: false,
    });

    // Add checksums
    xl_meta.add_checksum("md5", "d41d8cd98f00b204e9800998ecf8427e");
    xl_meta.add_checksum("sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");

    // Set object tags
    xl_meta.set_tags(vec![
        ("Environment".to_string(), "Production".to_string()),
        ("Owner".to_string(), "DataTeam".to_string()),
    ]);

    // Set retention information
    xl_meta.set_retention_info(
        chrono::Utc::now() + chrono::Duration::days(365),
        "GOVERNANCE".to_string(),
    );

    // Validate metadata
    xl_meta.validate()?;

    Ok(())
}
```

### Inline Metadata Operations

```rust
use rustfs_filemeta::{InlineMetadata, MetadataSize};

fn inline_metadata_example() -> Result<(), Box<dyn std::error::Error>> {
    // Create inline metadata for small files
    let mut inline_meta = InlineMetadata::new();

    // Set basic properties
    inline_meta.set_content_type("text/plain");
    inline_meta.set_content_encoding("gzip");
    inline_meta.set_cache_control("max-age=3600");

    // Add custom headers
    inline_meta.add_header("x-custom-field", "custom-value");
    inline_meta.add_header("x-app-version", "1.2.3");

    // Check if metadata fits inline storage
    if inline_meta.size() <= MetadataSize::INLINE_THRESHOLD {
        println!("Metadata can be stored inline");
    } else {
        println!("Metadata requires separate storage");
    }

    // Serialize for storage
    let bytes = inline_meta.to_bytes()?;

    // Deserialize from storage
    let restored = InlineMetadata::from_bytes(&bytes)?;

    Ok(())
}
```

## 🏗️ Architecture

### Metadata Storage Layout

```
FileMeta Architecture:
┌─────────────────────────────────────────────────────────────┐
│                    Metadata API Layer                       │
├─────────────────────────────────────────────────────────────┤
│    XL Metadata    │   Inline Metadata   │   Version Info    │
├─────────────────────────────────────────────────────────────┤
│              FlatBuffers Serialization                      │
├─────────────────────────────────────────────────────────────┤
│    Compression    │    Validation       │    Migration      │
├─────────────────────────────────────────────────────────────┤
│              Storage Backend Integration                     │
└─────────────────────────────────────────────────────────────┘
```

### Metadata Types

| Type | Use Case | Storage | Performance |
|------|----------|---------|-------------|
| XLMeta | Large objects with rich metadata | Separate file | High durability |
| InlineMeta | Small objects with minimal metadata | Embedded | Fastest access |
| VersionMeta | Object versioning information | Version-specific | Version-aware |

## 🧪 Testing

Run the test suite:

```bash
# Run all tests
cargo test

# Run serialization benchmarks
cargo bench

# Test metadata validation
cargo test validation

# Test schema migration
cargo test migration
```

## 🚀 Performance

FileMeta is optimized for high-performance metadata operations:

- **Serialization**: Zero-copy FlatBuffers serialization
- **Storage**: Compact binary format reduces I/O
- **Caching**: Intelligent metadata caching
- **Batch Operations**: Efficient bulk metadata processing

## 📋 Requirements

- **Rust**: 1.70.0 or later
- **Platforms**: Linux, macOS, Windows
- **Memory**: Minimal memory footprint
- **Storage**: Compatible with RustFS storage backend

## 🌍 Related Projects

This module is part of the RustFS ecosystem:
- [RustFS Main](https://github.com/rustfs/rustfs) - Core distributed storage system
- [RustFS ECStore](../ecstore) - Erasure coding storage engine
- [RustFS Utils](../utils) - Utility functions
- [RustFS Proto](../protos) - Protocol definitions

## 📚 Documentation

For comprehensive documentation, visit:
- [RustFS Documentation](https://docs.rustfs.com)
- [FileMeta API Reference](https://docs.rustfs.com/filemeta/)

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
  Made with ❤️ by the RustFS Team
</p>
