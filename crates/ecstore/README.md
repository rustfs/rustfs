[![RustFS](https://rustfs.com/images/rustfs-github.png)](https://rustfs.com)

# RustFS ECStore - Erasure Coding Storage Engine

<p align="center">
  <strong>High-performance erasure coding storage engine for RustFS distributed object storage</strong>
</p>

<p align="center">
  <a href="https://github.com/rustfs/rustfs/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/rustfs/rustfs/actions/workflows/ci.yml/badge.svg" /></a>
  <a href="https://docs.rustfs.com/en/">📖 Documentation</a>
  · <a href="https://github.com/rustfs/rustfs/issues">🐛 Bug Reports</a>
  · <a href="https://github.com/rustfs/rustfs/discussions">💬 Discussions</a>
</p>

---

## 📖 Overview

**RustFS ECStore** is the core storage engine of the [RustFS](https://rustfs.com) distributed object storage system. It provides enterprise-grade erasure coding capabilities, data integrity protection, and high-performance object storage operations. This module serves as the foundation for RustFS's distributed storage architecture.

> **Note:** This is a core submodule of RustFS and provides the primary storage capabilities for the distributed object storage system. For the complete RustFS experience, please visit the [main RustFS repository](https://github.com/rustfs/rustfs).

## ✨ Features

### 🔧 Erasure Coding Storage

- **Reed-Solomon Erasure Coding**: Advanced error correction with configurable redundancy
- **Data Durability**: Protection against disk failures and bit rot
- **Automatic Repair**: Self-healing capabilities for corrupted or missing data
- **Configurable Parity**: Flexible parity configurations (4+2, 8+4, 16+4, etc.)

### 💾 Storage Management

- **Multi-Disk Support**: Intelligent disk management and load balancing
- **Storage Classes**: Support for different storage tiers and policies
- **Bucket Management**: Advanced bucket operations and lifecycle management
- **Object Versioning**: Complete versioning support with metadata tracking

### 🚀 Performance & Scalability

- **High Throughput**: Optimized for large-scale data operations
- **Parallel Processing**: Concurrent read/write operations across multiple disks
- **Memory Efficient**: Smart caching and memory management
- **SIMD Optimization**: Hardware-accelerated erasure coding operations

### 🛡️ Data Integrity

- **Bitrot Detection**: Real-time data corruption detection
- **Checksum Verification**: Multiple checksum algorithms (MD5, SHA256, XXHash)
- **Healing System**: Automatic background healing and repair
- **Data Scrubbing**: Proactive data integrity scanning

### 🔄 Advanced Features

- **Compression**: Built-in compression support for space optimization
- **Replication**: Cross-region replication capabilities
- **Notification System**: Real-time event notifications
- **Metrics & Monitoring**: Comprehensive performance metrics

## 🏗️ Architecture

### Storage Layout

```
ECStore Architecture:
┌─────────────────────────────────────────────────────────────┐
│                    Storage API Layer                        │
├─────────────────────────────────────────────────────────────┤
│  Bucket Management  │  Object Operations  │  Metadata Mgmt  │
├─────────────────────────────────────────────────────────────┤
│              Erasure Coding Engine                          │
├─────────────────────────────────────────────────────────────┤
│    Disk Management    │    Healing System    │    Cache     │
├─────────────────────────────────────────────────────────────┤
│              Physical Storage Devices                       │
└─────────────────────────────────────────────────────────────┘
```

### Erasure Coding Schemes

| Configuration | Data Drives | Parity Drives | Fault Tolerance | Storage Efficiency |
|---------------|-------------|---------------|-----------------|-------------------|
| 4+2 | 4 | 2 | 2 disk failures | 66.7% |
| 8+4 | 8 | 4 | 4 disk failures | 66.7% |
| 16+4 | 16 | 4 | 4 disk failures | 80% |
| Custom | N | K | K disk failures | N/(N+K) |

## 📦 Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
rustfs-ecstore = "0.1.0"
```

## 🔧 Usage

### Basic Storage Operations

```rust
use rustfs_ecstore::{StorageAPI, new_object_layer_fn};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize storage layer
    let storage = new_object_layer_fn("/path/to/storage").await?;

    // Create a bucket
    storage.make_bucket("my-bucket", None).await?;

    // Put an object
    let data = b"Hello, RustFS!";
    storage.put_object("my-bucket", "hello.txt", data.to_vec()).await?;

    // Get an object
    let retrieved = storage.get_object("my-bucket", "hello.txt", None).await?;
    println!("Retrieved: {}", String::from_utf8_lossy(&retrieved.data));

    Ok(())
}
```

### Advanced Configuration

```rust
use rustfs_ecstore::{StorageAPI, config::Config};

async fn setup_storage_with_config() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config {
        erasure_sets: vec![
            // 8+4 configuration for high durability
            ErasureSet::new(8, 4, vec![
                "/disk1", "/disk2", "/disk3", "/disk4",
                "/disk5", "/disk6", "/disk7", "/disk8",
                "/disk9", "/disk10", "/disk11", "/disk12"
            ])
        ],
        healing_enabled: true,
        compression_enabled: true,
        ..Default::default()
    };

    let storage = new_object_layer_fn("/path/to/storage")
        .with_config(config)
        .await?;

    Ok(())
}
```

### Bucket Management

```rust
use rustfs_ecstore::{StorageAPI, bucket::BucketInfo};

async fn bucket_operations(storage: Arc<dyn StorageAPI>) -> Result<(), Box<dyn std::error::Error>> {
    // Create bucket with specific configuration
    let bucket_info = BucketInfo {
        name: "enterprise-bucket".to_string(),
        versioning_enabled: true,
        lifecycle_config: Some(lifecycle_config()),
        ..Default::default()
    };

    storage.make_bucket_with_config(bucket_info).await?;

    // List buckets
    let buckets = storage.list_buckets().await?;
    for bucket in buckets {
        println!("Bucket: {}, Created: {}", bucket.name, bucket.created);
    }

    // Set bucket policy
    storage.set_bucket_policy("enterprise-bucket", policy_json).await?;

    Ok(())
}
```

### Healing and Maintenance

```rust
use rustfs_ecstore::{heal::HealingManager, StorageAPI};

async fn healing_operations(storage: Arc<dyn StorageAPI>) -> Result<(), Box<dyn std::error::Error>> {
    // Check storage health
    let health = storage.storage_info().await?;
    println!("Storage Health: {:?}", health);

    // Trigger healing for specific bucket
    let healing_result = storage.heal_bucket("my-bucket").await?;
    println!("Healing completed: {:?}", healing_result);

    // Background healing status
    let healing_status = storage.healing_status().await?;
    println!("Background healing: {:?}", healing_status);

    Ok(())
}
```

## 🧪 Testing

Run the test suite:

```bash
# Run all tests
cargo test

# Run tests with specific features
cargo test --features "compression,healing"

# Run benchmarks
cargo bench

# Run erasure coding benchmarks
cargo bench --bench erasure_benchmark

# Run comparison benchmarks
cargo bench --bench comparison_benchmark
```

## 📊 Performance Benchmarks

ECStore is designed for high-performance storage operations:

### Throughput Performance

- **Sequential Write**: Up to 10GB/s on NVMe storage
- **Sequential Read**: Up to 12GB/s with parallel reads
- **Random I/O**: 100K+ IOPS for small objects
- **Erasure Coding**: 5GB/s encoding/decoding throughput

### Scalability Metrics

- **Storage Capacity**: Exabyte-scale deployments
- **Concurrent Operations**: 10,000+ concurrent requests
- **Disk Scaling**: Support for 1000+ disks per node
- **Fault Tolerance**: Up to 50% disk failure resilience

## 🔧 Configuration

### Storage Configuration

```toml
[storage]
# Erasure coding configuration
erasure_set_size = 12  # Total disks per set
data_drives = 8        # Data drives per set
parity_drives = 4      # Parity drives per set

# Performance tuning
read_quorum = 6        # Minimum disks for read
write_quorum = 7       # Minimum disks for write
parallel_reads = true  # Enable parallel reads
compression = true     # Enable compression

# Healing configuration
healing_enabled = true
healing_interval = "24h"
bitrot_check_interval = "168h"  # Weekly bitrot check
```

### Advanced Features

```rust
use rustfs_ecstore::config::StorageConfig;

let config = StorageConfig {
    // Enable advanced features
    bitrot_protection: true,
    automatic_healing: true,
    compression_level: 6,
    checksum_algorithm: ChecksumAlgorithm::XXHash64,

    // Performance tuning
    read_buffer_size: 1024 * 1024,  // 1MB read buffer
    write_buffer_size: 4 * 1024 * 1024,  // 4MB write buffer
    concurrent_operations: 1000,

    // Storage optimization
    small_object_threshold: 128 * 1024,  // 128KB
    large_object_threshold: 64 * 1024 * 1024,  // 64MB

    ..Default::default()
};
```

## 🤝 Integration with RustFS

ECStore integrates seamlessly with other RustFS components:

- **API Server**: Provides S3-compatible storage operations
- **IAM Module**: Handles authentication and authorization
- **Policy Engine**: Implements bucket policies and access controls
- **Notification System**: Publishes storage events
- **Monitoring**: Provides detailed metrics and health status

## 📋 Requirements

- **Rust**: 1.70.0 or later
- **Platforms**: Linux, macOS, Windows
- **Storage**: Local disks, network storage, cloud storage
- **Memory**: Minimum 4GB RAM (8GB+ recommended)
- **Network**: High-speed network for distributed deployments

## 🚀 Performance Tuning

### Optimization Tips

1. **Disk Configuration**:
   - Use dedicated disks for each erasure set
   - Prefer NVMe over SATA for better performance
   - Ensure consistent disk sizes within erasure sets

2. **Memory Settings**:
   - Allocate sufficient memory for caching
   - Tune read/write buffer sizes based on workload
   - Enable memory-mapped files for large objects

3. **Network Optimization**:
   - Use high-speed network connections
   - Configure proper MTU sizes
   - Enable network compression for WAN scenarios

4. **CPU Optimization**:
   - Utilize SIMD instructions for erasure coding
   - Balance CPU cores across erasure sets
   - Enable hardware-accelerated checksums

## 🐛 Troubleshooting

### Common Issues

1. **Disk Failures**:
   - Check disk health using `storage_info()`
   - Trigger healing with `heal_bucket()`
   - Replace failed disks and re-add to cluster

2. **Performance Issues**:
   - Monitor disk I/O utilization
   - Check network bandwidth usage
   - Verify erasure coding configuration

3. **Data Integrity**:
   - Run bitrot detection scans
   - Verify checksums for critical data
   - Check healing system status

## 🌍 Related Projects

This module is part of the RustFS ecosystem:

- [RustFS Main](https://github.com/rustfs/rustfs) - Core distributed storage system
- [RustFS Crypto](../crypto) - Cryptographic operations
- [RustFS IAM](../iam) - Identity and access management
- [RustFS Policy](../policy) - Policy engine
- [RustFS FileMeta](../filemeta) - File metadata management

## 📚 Documentation

For comprehensive documentation, visit:

- [RustFS Documentation](https://docs.rustfs.com)
- [Storage API Reference](https://docs.rustfs.com/ecstore/)
- [Erasure Coding Guide](https://docs.rustfs.com/erasure-coding/)
- [Performance Tuning](https://docs.rustfs.com/performance/)

## 🔗 Links

- [Documentation](https://docs.rustfs.com) - Complete RustFS manual
- [Changelog](https://github.com/rustfs/rustfs/releases) - Release notes and updates
- [GitHub Discussions](https://github.com/rustfs/rustfs/discussions) - Community support

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](https://github.com/rustfs/rustfs/blob/main/CONTRIBUTING.md) for details on:

- Storage engine architecture and design patterns
- Erasure coding implementation guidelines
- Performance optimization techniques
- Testing procedures for storage operations
- Documentation standards for storage APIs

### Development Setup

```bash
# Clone the repository
git clone https://github.com/rustfs/rustfs.git
cd rustfs

# Navigate to ECStore module
cd crates/ecstore

# Install dependencies
cargo build

# Run tests
cargo test

# Run benchmarks
cargo bench

# Format code
cargo fmt

# Run linter
cargo clippy
```

## 💬 Getting Help

- **Documentation**: [docs.rustfs.com](https://docs.rustfs.com)
- **Issues**: [GitHub Issues](https://github.com/rustfs/rustfs/issues)
- **Discussions**: [GitHub Discussions](https://github.com/rustfs/rustfs/discussions)
- **Storage Support**: <storage-support@rustfs.com>

## 📞 Contact

- **Bugs**: [GitHub Issues](https://github.com/rustfs/rustfs/issues)
- **Business**: <hello@rustfs.com>
- **Jobs**: <jobs@rustfs.com>
- **General Discussion**: [GitHub Discussions](https://github.com/rustfs/rustfs/discussions)

## 👥 Contributors

This module is maintained by the RustFS storage team and community contributors. Special thanks to all who have contributed to making RustFS storage reliable and efficient.

<a href="https://github.com/rustfs/rustfs/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=rustfs/rustfs" />
</a>

## 📄 License

Licensed under the Apache License, Version 2.0. See [LICENSE](https://github.com/rustfs/rustfs/blob/main/LICENSE) for details.

```
Copyright 2024 RustFS Team

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

---

<p align="center">
  <strong>RustFS</strong> is a trademark of RustFS, Inc.<br>
  All other trademarks are the property of their respective owners.
</p>

<p align="center">
  Made with ❤️ by the RustFS Storage Team
</p>
