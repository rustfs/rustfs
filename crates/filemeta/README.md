# RustFS FileMeta

A high-performance Rust implementation of xl-storage-format-v2, providing complete compatibility with S3-compatible metadata format while offering enhanced performance and safety.

## Overview

This crate implements the XL (Erasure Coded) metadata format used for distributed object storage. It provides:

- **Full S3 Compatibility**: 100% compatible with xl.meta file format
- **High Performance**: Optimized for speed with sub-microsecond parsing times
- **Memory Safety**: Written in safe Rust with comprehensive error handling
- **Comprehensive Testing**: Extensive test suite with real metadata validation
- **Cross-Platform**: Supports multiple CPU architectures (x86_64, aarch64)

## Features

### Core Functionality
- ✅ XL v2 file format parsing and serialization
- ✅ MessagePack-based metadata encoding/decoding
- ✅ Version management with modification time sorting
- ✅ Erasure coding information storage
- ✅ Inline data support for small objects
- ✅ CRC32 integrity verification using xxHash64
- ✅ Delete marker handling
- ✅ Legacy version support

### Advanced Features
- ✅ Signature calculation for version integrity
- ✅ Metadata validation and compatibility checking
- ✅ Version statistics and analytics
- ✅ Async I/O support with tokio
- ✅ Comprehensive error handling
- ✅ Performance benchmarking

## Performance

Based on our benchmarks:

| Operation | Time | Description |
|-----------|------|-------------|
| Parse Real xl.meta | ~255 ns | Parse authentic xl metadata |
| Parse Complex xl.meta | ~1.1 µs | Parse multi-version metadata |
| Serialize Real xl.meta | ~659 ns | Serialize to xl format |
| Round-trip Real xl.meta | ~1.3 µs | Parse + serialize cycle |
| Version Statistics | ~5.2 ns | Calculate version stats |
| Integrity Validation | ~7.8 ns | Validate metadata integrity |

## Usage

### Basic Usage

```rust
use rustfs_filemeta::file_meta::FileMeta;

// Load metadata from bytes
let metadata = FileMeta::load(&xl_meta_bytes)?;

// Access version information
for version in &metadata.versions {
    println!("Version ID: {:?}", version.header.version_id);
    println!("Mod Time: {:?}", version.header.mod_time);
}

// Serialize back to bytes
let serialized = metadata.marshal_msg()?;
```

### Advanced Usage

```rust
use rustfs_filemeta::file_meta::FileMeta;

// Load with validation
let mut metadata = FileMeta::load(&xl_meta_bytes)?;

// Validate integrity
metadata.validate_integrity()?;

// Check xl format compatibility
if metadata.is_compatible_with_meta() {
    println!("Compatible with xl format");
}

// Get version statistics
let stats = metadata.get_version_stats();
println!("Total versions: {}", stats.total_versions);
println!("Object versions: {}", stats.object_versions);
println!("Delete markers: {}", stats.delete_markers);
```

### Working with FileInfo

```rust
use rustfs_filemeta::fileinfo::FileInfo;
use rustfs_filemeta::file_meta::FileMetaVersion;

// Convert FileInfo to metadata version
let file_info = FileInfo::new("bucket", "object.txt");
let meta_version = FileMetaVersion::from(file_info);

// Add version to metadata
metadata.add_version(file_info)?;
```

## Data Structures

### FileMeta
The main metadata container that holds all versions and inline data:

```rust
pub struct FileMeta {
    pub versions: Vec<FileMetaShallowVersion>,
    pub data: InlineData,
    pub meta_ver: u8,
}
```

### FileMetaVersion
Represents a single object version:

```rust
pub struct FileMetaVersion {
    pub version_type: VersionType,
    pub object: Option<MetaObject>,
    pub delete_marker: Option<MetaDeleteMarker>,
    pub write_version: u64,
}
```

### MetaObject
Contains object-specific metadata including erasure coding information:

```rust
pub struct MetaObject {
    pub version_id: Option<Uuid>,
    pub data_dir: Option<Uuid>,
    pub erasure_algorithm: ErasureAlgo,
    pub erasure_m: usize,
    pub erasure_n: usize,
    // ... additional fields
}
```

## File Format Compatibility

This implementation is fully compatible with xl-storage-format-v2:

- **Header Format**: XL2 v1 format with proper version checking
- **Serialization**: MessagePack encoding identical to standard format
- **Checksums**: xxHash64-based CRC validation
- **Version Types**: Support for Object, Delete, and Legacy versions
- **Inline Data**: Compatible inline data storage for small objects

## Testing

The crate includes comprehensive tests with real xl metadata:

```bash
# Run all tests
cargo test

# Run benchmarks
cargo bench

# Run with coverage
cargo test --features coverage
```

### Test Coverage
- ✅ Real xl.meta file compatibility
- ✅ Complex multi-version scenarios
- ✅ Error handling and recovery
- ✅ Inline data processing
- ✅ Signature calculation
- ✅ Round-trip serialization
- ✅ Performance benchmarks
- ✅ Edge cases and boundary conditions

## Architecture

The crate follows a modular design:

```
src/
├── file_meta.rs      # Core metadata structures and logic
├── file_meta_inline.rs # Inline data handling
├── fileinfo.rs       # File information structures
├── test_data.rs      # Test data generation
└── lib.rs           # Public API exports
```

## Error Handling

Comprehensive error handling with detailed error messages:

```rust
use rustfs_filemeta::error::Error;

match FileMeta::load(&invalid_data) {
    Ok(metadata) => { /* process metadata */ },
    Err(Error::InvalidFormat(msg)) => {
        eprintln!("Invalid format: {}", msg);
    },
    Err(Error::CorruptedData(msg)) => {
        eprintln!("Corrupted data: {}", msg);
    },
    Err(e) => {
        eprintln!("Other error: {}", e);
    }
}
```

## Dependencies

- `rmp` - MessagePack serialization
- `uuid` - UUID handling
- `time` - Date/time operations
- `xxhash-rust` - Fast hashing
- `tokio` - Async runtime (optional)
- `criterion` - Benchmarking (dev dependency)

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.

## Acknowledgments

- Original xl-storage-format-v2 implementation contributors
- Rust community for excellent crates and tooling
- Contributors and testers who helped improve this implementation 