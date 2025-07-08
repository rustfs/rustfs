[![RustFS](https://rustfs.com/images/rustfs-github.png)](https://rustfs.com)

# RustFS Rio - High-Performance I/O

<p align="center">
  <strong>High-performance asynchronous I/O operations for RustFS distributed object storage</strong>
</p>

<p align="center">
  <a href="https://github.com/rustfs/rustfs/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/rustfs/rustfs/actions/workflows/ci.yml/badge.svg" /></a>
  <a href="https://docs.rustfs.com/en/">📖 Documentation</a>
  · <a href="https://github.com/rustfs/rustfs/issues">🐛 Bug Reports</a>
  · <a href="https://github.com/rustfs/rustfs/discussions">💬 Discussions</a>
</p>

---

## 📖 Overview

**RustFS Rio** provides high-performance asynchronous I/O operations for the [RustFS](https://rustfs.com) distributed object storage system. It implements efficient data streaming, encryption, compression, and integrity checking with zero-copy operations and optimized buffering strategies.

> **Note:** This is a performance-critical submodule of RustFS that provides essential I/O capabilities for the distributed object storage system. For the complete RustFS experience, please visit the [main RustFS repository](https://github.com/rustfs/rustfs).

## ✨ Features

### 🚀 High-Performance I/O

- **Zero-Copy Operations**: Efficient data movement without unnecessary copying
- **Async Streaming**: Non-blocking streaming I/O with backpressure handling
- **Vectored I/O**: Scatter-gather operations for improved throughput
- **Buffer Management**: Intelligent buffer pooling and reuse

### 🔐 Cryptographic Operations

- **AES-GCM Encryption**: Hardware-accelerated encryption/decryption
- **Streaming Encryption**: Encrypt data on-the-fly without buffering
- **Key Management**: Secure key derivation and rotation
- **Digital Signatures**: Data integrity verification

### 📦 Compression Support

- **Multi-Algorithm**: Support for various compression algorithms
- **Streaming Compression**: Real-time compression during transfer
- **Adaptive Compression**: Dynamic algorithm selection based on data
- **Compression Levels**: Configurable compression vs. speed tradeoffs

### 🔧 Data Integrity

- **CRC32 Checksums**: Fast integrity checking
- **MD5 Hashing**: Legacy compatibility and verification
- **Merkle Trees**: Hierarchical integrity verification
- **Error Correction**: Automatic error detection and correction

## 📦 Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
rustfs-rio = "0.1.0"
```

## 🔧 Usage

### Basic Streaming I/O

```rust
use rustfs_rio::{StreamReader, StreamWriter, BufferPool};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create buffer pool for efficient memory management
    let buffer_pool = BufferPool::new(64 * 1024, 100); // 64KB buffers, 100 in pool

    // Create streaming reader
    let mut reader = StreamReader::new(input_source, buffer_pool.clone());

    // Create streaming writer
    let mut writer = StreamWriter::new(output_destination, buffer_pool.clone());

    // High-performance streaming copy
    let mut buffer = vec![0u8; 8192];
    loop {
        let n = reader.read(&mut buffer).await?;
        if n == 0 {
            break;
        }
        writer.write_all(&buffer[..n]).await?;
    }

    writer.flush().await?;
    Ok(())
}
```

### Encrypted Streaming

```rust
use rustfs_rio::{EncryptedWriter, EncryptedReader, EncryptionKey};
use aes_gcm::{Aes256Gcm, Key, Nonce};

async fn encrypted_streaming_example() -> Result<(), Box<dyn std::error::Error>> {
    // Generate encryption key
    let key = EncryptionKey::generate()?;

    // Create encrypted writer
    let mut encrypted_writer = EncryptedWriter::new(
        output_stream,
        key.clone(),
        Aes256Gcm::new(&key.into())
    )?;

    // Write encrypted data
    encrypted_writer.write_all(b"Hello, encrypted world!").await?;
    encrypted_writer.finalize().await?;

    // Create encrypted reader
    let mut encrypted_reader = EncryptedReader::new(
        input_stream,
        key.clone(),
        Aes256Gcm::new(&key.into())
    )?;

    // Read decrypted data
    let mut decrypted_data = Vec::new();
    encrypted_reader.read_to_end(&mut decrypted_data).await?;

    println!("Decrypted: {}", String::from_utf8(decrypted_data)?);
    Ok(())
}
```

### Compressed Streaming

```rust
use rustfs_rio::{CompressedWriter, CompressedReader, CompressionAlgorithm};

async fn compressed_streaming_example() -> Result<(), Box<dyn std::error::Error>> {
    // Create compressed writer
    let mut compressed_writer = CompressedWriter::new(
        output_stream,
        CompressionAlgorithm::Zstd,
        6 // compression level
    )?;

    // Write compressed data
    compressed_writer.write_all(b"This data will be compressed").await?;
    compressed_writer.write_all(b"and streamed efficiently").await?;
    compressed_writer.finish().await?;

    // Create compressed reader
    let mut compressed_reader = CompressedReader::new(
        input_stream,
        CompressionAlgorithm::Zstd
    )?;

    // Read decompressed data
    let mut decompressed_data = Vec::new();
    compressed_reader.read_to_end(&mut decompressed_data).await?;

    println!("Decompressed: {}", String::from_utf8(decompressed_data)?);
    Ok(())
}
```

### Integrity Checking

```rust
use rustfs_rio::{ChecksumWriter, ChecksumReader, ChecksumAlgorithm};

async fn integrity_checking_example() -> Result<(), Box<dyn std::error::Error>> {
    // Create checksum writer
    let mut checksum_writer = ChecksumWriter::new(
        output_stream,
        ChecksumAlgorithm::Crc32
    );

    // Write data with checksum calculation
    checksum_writer.write_all(b"Data with integrity checking").await?;
    let write_checksum = checksum_writer.finalize().await?;

    println!("Write checksum: {:08x}", write_checksum);

    // Create checksum reader
    let mut checksum_reader = ChecksumReader::new(
        input_stream,
        ChecksumAlgorithm::Crc32
    );

    // Read data with checksum verification
    let mut data = Vec::new();
    checksum_reader.read_to_end(&mut data).await?;
    let read_checksum = checksum_reader.checksum();

    println!("Read checksum: {:08x}", read_checksum);

    // Verify integrity
    if write_checksum == read_checksum {
        println!("Data integrity verified!");
    } else {
        println!("Data corruption detected!");
    }

    Ok(())
}
```

### Multi-Layer Streaming

```rust
use rustfs_rio::{MultiLayerWriter, MultiLayerReader, Layer};

async fn multi_layer_streaming_example() -> Result<(), Box<dyn std::error::Error>> {
    // Create multi-layer writer (compression + encryption + checksum)
    let mut writer = MultiLayerWriter::new(output_stream)
        .add_layer(Layer::Compression(CompressionAlgorithm::Zstd, 6))
        .add_layer(Layer::Encryption(encryption_key.clone()))
        .add_layer(Layer::Checksum(ChecksumAlgorithm::Crc32))
        .build()?;

    // Write data through all layers
    writer.write_all(b"This data will be compressed, encrypted, and checksummed").await?;
    let final_checksum = writer.finalize().await?;

    // Create multi-layer reader (reverse order)
    let mut reader = MultiLayerReader::new(input_stream)
        .add_layer(Layer::Checksum(ChecksumAlgorithm::Crc32))
        .add_layer(Layer::Decryption(encryption_key.clone()))
        .add_layer(Layer::Decompression(CompressionAlgorithm::Zstd))
        .build()?;

    // Read data through all layers
    let mut data = Vec::new();
    reader.read_to_end(&mut data).await?;

    // Verify final checksum
    if reader.verify_checksum(final_checksum)? {
        println!("All layers verified successfully!");
    }

    Ok(())
}
```

### Vectored I/O Operations

```rust
use rustfs_rio::{VectoredWriter, VectoredReader, IoVec};

async fn vectored_io_example() -> Result<(), Box<dyn std::error::Error>> {
    // Create vectored writer
    let mut vectored_writer = VectoredWriter::new(output_stream);

    // Prepare multiple buffers
    let header = b"HEADER";
    let data = b"Important data content";
    let footer = b"FOOTER";

    // Write multiple buffers in one operation
    let io_vecs = vec![
        IoVec::new(header),
        IoVec::new(data),
        IoVec::new(footer),
    ];

    let bytes_written = vectored_writer.write_vectored(&io_vecs).await?;
    println!("Wrote {} bytes in vectored operation", bytes_written);

    // Create vectored reader
    let mut vectored_reader = VectoredReader::new(input_stream);

    // Read into multiple buffers
    let mut header_buf = vec![0u8; 6];
    let mut data_buf = vec![0u8; 22];
    let mut footer_buf = vec![0u8; 6];

    let mut read_vecs = vec![
        IoVec::new_mut(&mut header_buf),
        IoVec::new_mut(&mut data_buf),
        IoVec::new_mut(&mut footer_buf),
    ];

    let bytes_read = vectored_reader.read_vectored(&mut read_vecs).await?;
    println!("Read {} bytes in vectored operation", bytes_read);

    Ok(())
}
```

### Async Stream Processing

```rust
use rustfs_rio::{AsyncStreamProcessor, ProcessorChain};
use futures::StreamExt;

async fn stream_processing_example() -> Result<(), Box<dyn std::error::Error>> {
    // Create processor chain
    let processor = ProcessorChain::new()
        .add_processor(Box::new(CompressionProcessor::new(CompressionAlgorithm::Zstd)))
        .add_processor(Box::new(EncryptionProcessor::new(encryption_key)))
        .add_processor(Box::new(ChecksumProcessor::new(ChecksumAlgorithm::Crc32)));

    // Create async stream processor
    let mut stream_processor = AsyncStreamProcessor::new(input_stream, processor);

    // Process stream chunks
    while let Some(chunk) = stream_processor.next().await {
        let processed_chunk = chunk?;

        // Handle processed chunk
        output_stream.write_all(&processed_chunk).await?;
    }

    Ok(())
}
```

## 🏗️ Architecture

### Rio Architecture

```
Rio I/O Architecture:
┌─────────────────────────────────────────────────────────────┐
│                    Stream API Layer                         │
├─────────────────────────────────────────────────────────────┤
│ Encryption  │ Compression │ Checksum    │ Vectored I/O     │
├─────────────────────────────────────────────────────────────┤
│              Buffer Management                               │
├─────────────────────────────────────────────────────────────┤
│   Zero-Copy   │   Async I/O   │   Backpressure Control     │
├─────────────────────────────────────────────────────────────┤
│              Tokio Runtime Integration                       │
└─────────────────────────────────────────────────────────────┘
```

### Performance Features

| Feature | Benefit | Implementation |
|---------|---------|----------------|
| Zero-Copy | Reduced memory usage | Direct buffer operations |
| Async I/O | High concurrency | Tokio-based operations |
| Vectored I/O | Reduced syscalls | Scatter-gather operations |
| Buffer Pooling | Memory efficiency | Reusable buffer management |

## 🧪 Testing

Run the test suite:

```bash
# Run all tests
cargo test

# Test streaming operations
cargo test streaming

# Test encryption
cargo test encryption

# Test compression
cargo test compression

# Run benchmarks
cargo bench
```

## 📋 Requirements

- **Rust**: 1.70.0 or later
- **Platforms**: Linux, macOS, Windows
- **Dependencies**: Tokio async runtime
- **Hardware**: AES-NI support recommended for encryption

## 🌍 Related Projects

This module is part of the RustFS ecosystem:

- [RustFS Main](https://github.com/rustfs/rustfs) - Core distributed storage system
- [RustFS Utils](../utils) - Utility functions
- [RustFS Crypto](../crypto) - Cryptographic operations

## 📚 Documentation

For comprehensive documentation, visit:

- [RustFS Documentation](https://docs.rustfs.com)
- [Rio API Reference](https://docs.rustfs.com/rio/)
- [Performance Guide](https://docs.rustfs.com/performance/)

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
  Made with 🚀 by the RustFS Team
</p>
