[![RustFS](https://rustfs.com/images/rustfs-github.png)](https://rustfs.com)

# RustFS Utils - Utility Library

<p align="center">
  <strong>Essential utility functions and common tools for RustFS distributed object storage</strong>
</p>

<p align="center">
  <a href="https://github.com/rustfs/rustfs/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/rustfs/rustfs/actions/workflows/ci.yml/badge.svg" /></a>
  <a href="https://docs.rustfs.com/en/">📖 Documentation</a>
  · <a href="https://github.com/rustfs/rustfs/issues">🐛 Bug Reports</a>
  · <a href="https://github.com/rustfs/rustfs/discussions">💬 Discussions</a>
</p>

---

## 📖 Overview

**RustFS Utils** is the utility library for the [RustFS](https://rustfs.com) distributed object storage system. It provides a comprehensive collection of utility functions, helper tools, and common functionality used across all RustFS modules, including system operations, cryptographic utilities, compression, and cross-platform compatibility tools.

> **Note:** This is a foundational submodule of RustFS that provides essential utility functions for the distributed object storage system. For the complete RustFS experience, please visit the [main RustFS repository](https://github.com/rustfs/rustfs).

## ✨ Features

### 🔧 System Utilities

- **Cross-Platform Operations**: Unified system operations across platforms
- **Process Management**: Process spawning and management utilities
- **Resource Monitoring**: CPU, memory, and disk usage monitoring
- **Network Utilities**: Network interface and connectivity tools

### 📁 File System Utilities

- **Path Manipulation**: Advanced path handling and normalization
- **File Operations**: Safe file operations with atomic writes
- **Directory Management**: Recursive directory operations
- **Symbolic Link Handling**: Cross-platform symlink management

### 🗜️ Compression & Encoding

- **Multiple Algorithms**: Support for gzip, zstd, lz4, and more
- **Streaming Compression**: Memory-efficient streaming compression
- **Base64 Encoding**: High-performance base64 operations
- **URL Encoding**: Safe URL encoding and decoding

### 🔐 Cryptographic Utilities

- **Hash Functions**: MD5, SHA1, SHA256, XXHash implementations
- **Random Generation**: Cryptographically secure random utilities
- **Certificate Handling**: X.509 certificate parsing and validation
- **Key Generation**: Secure key generation utilities

### 🌐 Network Utilities

- **HTTP Helpers**: HTTP client and server utilities
- **DNS Resolution**: DNS lookup and resolution tools
- **Network Interface**: Interface detection and configuration
- **Protocol Utilities**: Various network protocol helpers

## 📦 Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
rustfs-utils = "0.1.0"

# Or with specific features
rustfs-utils = { version = "0.1.0", features = ["compression", "crypto", "network"] }
```

### Feature Flags

```toml
[dependencies]
rustfs-utils = { version = "0.1.0", features = ["full"] }
```

Available features:

- `compression` - Compression and decompression utilities
- `crypto` - Cryptographic functions and utilities
- `network` - Network-related utilities
- `path` - Advanced path manipulation tools
- `system` - System monitoring and management
- `full` - All features enabled

## 🔧 Usage

### File System Utilities

```rust
use rustfs_utils::fs::{ensure_dir, atomic_write, safe_remove};
use rustfs_utils::path::{normalize_path, is_subdirectory};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Ensure directory exists
    ensure_dir("/path/to/directory")?;

    // Atomic file write
    atomic_write("/path/to/file.txt", b"Hello, World!")?;

    // Path normalization
    let normalized = normalize_path("./some/../path/./file.txt");
    println!("Normalized: {}", normalized.display());

    // Check if path is subdirectory
    if is_subdirectory("/safe/path", "/safe/path/subdir") {
        println!("Path is safe");
    }

    Ok(())
}
```

### Compression Utilities

```rust
use rustfs_utils::compress::{compress_data, decompress_data, Algorithm};

fn compression_example() -> Result<(), Box<dyn std::error::Error>> {
    let data = b"This is some test data to compress";

    // Compress with different algorithms
    let gzip_compressed = compress_data(data, Algorithm::Gzip)?;
    let zstd_compressed = compress_data(data, Algorithm::Zstd)?;
    let lz4_compressed = compress_data(data, Algorithm::Lz4)?;

    // Decompress
    let decompressed = decompress_data(&gzip_compressed, Algorithm::Gzip)?;
    assert_eq!(data, decompressed.as_slice());

    println!("Original size: {}", data.len());
    println!("Gzip compressed: {}", gzip_compressed.len());
    println!("Zstd compressed: {}", zstd_compressed.len());
    println!("LZ4 compressed: {}", lz4_compressed.len());

    Ok(())
}
```

### Cryptographic Utilities

```rust
use rustfs_utils::crypto::{hash_data, random_bytes, generate_key};
use rustfs_utils::crypto::HashAlgorithm;

fn crypto_example() -> Result<(), Box<dyn std::error::Error>> {
    let data = b"Important data to hash";

    // Generate hashes
    let md5_hash = hash_data(data, HashAlgorithm::MD5)?;
    let sha256_hash = hash_data(data, HashAlgorithm::SHA256)?;
    let xxhash = hash_data(data, HashAlgorithm::XXHash64)?;

    println!("MD5: {}", hex::encode(md5_hash));
    println!("SHA256: {}", hex::encode(sha256_hash));
    println!("XXHash64: {}", hex::encode(xxhash));

    // Generate secure random data
    let random_data = random_bytes(32)?;
    println!("Random data: {}", hex::encode(random_data));

    // Generate cryptographic key
    let key = generate_key(256)?; // 256-bit key
    println!("Generated key: {}", hex::encode(key));

    Ok(())
}
```

### System Monitoring

```rust
use rustfs_utils::sys::{get_system_info, monitor_resources, DiskUsage};

async fn system_monitoring_example() -> Result<(), Box<dyn std::error::Error>> {
    // Get system information
    let sys_info = get_system_info().await?;
    println!("OS: {} {}", sys_info.os_name, sys_info.os_version);
    println!("CPU: {} cores", sys_info.cpu_cores);
    println!("Total Memory: {} GB", sys_info.total_memory / 1024 / 1024 / 1024);

    // Monitor disk usage
    let disk_usage = DiskUsage::for_path("/var/data")?;
    println!("Disk space: {} / {} bytes", disk_usage.used, disk_usage.total);
    println!("Available: {} bytes", disk_usage.available);

    // Monitor resources
    let resources = monitor_resources().await?;
    println!("CPU Usage: {:.2}%", resources.cpu_percent);
    println!("Memory Usage: {:.2}%", resources.memory_percent);

    Ok(())
}
```

### Network Utilities

```rust
use rustfs_utils::net::{resolve_hostname, get_local_ip, is_port_available};

async fn network_example() -> Result<(), Box<dyn std::error::Error>> {
    // DNS resolution
    let addresses = resolve_hostname("example.com").await?;
    for addr in addresses {
        println!("Resolved address: {}", addr);
    }

    // Get local IP
    let local_ip = get_local_ip().await?;
    println!("Local IP: {}", local_ip);

    // Check port availability
    if is_port_available(8080).await? {
        println!("Port 8080 is available");
    } else {
        println!("Port 8080 is in use");
    }

    Ok(())
}
```

### Certificate Utilities

```rust
use rustfs_utils::certs::{parse_certificate, validate_certificate_chain, CertificateInfo};

fn certificate_example() -> Result<(), Box<dyn std::error::Error>> {
    let cert_pem = include_str!("../test_data/certificate.pem");

    // Parse certificate
    let cert_info = parse_certificate(cert_pem)?;
    println!("Subject: {}", cert_info.subject);
    println!("Issuer: {}", cert_info.issuer);
    println!("Valid from: {}", cert_info.not_before);
    println!("Valid until: {}", cert_info.not_after);

    // Validate certificate chain
    let ca_certs = vec![/* CA certificates */];
    let is_valid = validate_certificate_chain(&cert_info, &ca_certs)?;

    if is_valid {
        println!("Certificate chain is valid");
    } else {
        println!("Certificate chain is invalid");
    }

    Ok(())
}
```

### Encoding Utilities

```rust
use rustfs_utils::encoding::{base64_encode, base64_decode, url_encode, url_decode};

fn encoding_example() -> Result<(), Box<dyn std::error::Error>> {
    let data = b"Hello, World!";

    // Base64 encoding
    let encoded = base64_encode(data);
    let decoded = base64_decode(&encoded)?;
    assert_eq!(data, decoded.as_slice());

    // URL encoding
    let url = "https://example.com/path with spaces?param=value&other=data";
    let encoded_url = url_encode(url);
    let decoded_url = url_decode(&encoded_url)?;
    assert_eq!(url, decoded_url);

    println!("Base64 encoded: {}", encoded);
    println!("URL encoded: {}", encoded_url);

    Ok(())
}
```

## 🏗️ Architecture

### Utils Module Structure

```
Utils Architecture:
┌─────────────────────────────────────────────────────────────┐
│                    Public API Layer                         │
├─────────────────────────────────────────────────────────────┤
│  File System  │  Compression  │  Crypto     │  Network      │
├─────────────────────────────────────────────────────────────┤
│  System Info  │  Encoding     │  Certs      │  Path Utils   │
├─────────────────────────────────────────────────────────────┤
│              Platform Abstraction Layer                     │
├─────────────────────────────────────────────────────────────┤
│              Operating System Integration                    │
└─────────────────────────────────────────────────────────────┘
```

### Feature Overview

| Category | Features | Platform Support |
|----------|----------|------------------|
| File System | Atomic operations, path manipulation | All platforms |
| Compression | Gzip, Zstd, LZ4, Brotli | All platforms |
| Cryptography | Hashing, random generation, keys | All platforms |
| System | Resource monitoring, process management | Linux, macOS, Windows |
| Network | DNS, connectivity, interface detection | All platforms |

## 🧪 Testing

Run the test suite:

```bash
# Run all tests
cargo test

# Run tests for specific features
cargo test --features compression
cargo test --features crypto
cargo test --features network

# Run tests with all features
cargo test --features full

# Run benchmarks
cargo bench
```

## 📊 Performance

The utils library is optimized for performance:

- **Zero-Copy Operations**: Minimize memory allocations where possible
- **Lazy Evaluation**: Defer expensive operations until needed
- **Platform Optimization**: Use platform-specific optimizations
- **Efficient Algorithms**: Choose the most efficient algorithms for each task

### Benchmarks

| Operation | Performance | Notes |
|-----------|-------------|-------|
| Path Normalization | ~50 ns | Uses efficient string operations |
| Base64 Encoding | ~1.2 GB/s | SIMD-optimized implementation |
| XXHash64 | ~15 GB/s | Hardware-accelerated when available |
| File Copy | ~2 GB/s | Platform-optimized copy operations |

## 📋 Requirements

- **Rust**: 1.70.0 or later
- **Platforms**: Linux, macOS, Windows
- **Architecture**: x86_64, aarch64, and others
- **Dependencies**: Minimal external dependencies

## 🌍 Related Projects

This module is part of the RustFS ecosystem:

- [RustFS Main](https://github.com/rustfs/rustfs) - Core distributed storage system
- [RustFS ECStore](../ecstore) - Erasure coding storage engine
- [RustFS Crypto](../crypto) - Cryptographic operations
- [RustFS Config](../config) - Configuration management

## 📚 Documentation

For comprehensive documentation, visit:

- [RustFS Documentation](https://docs.rustfs.com)
- [Utils API Reference](https://docs.rustfs.com/utils/)

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
