[![RustFS](https://rustfs.com/images/rustfs-github.png)](https://rustfs.com)

# RustFS Zip - Compression & Archiving

<p align="center">
  <strong>High-performance compression and archiving for RustFS object storage</strong>
</p>

<p align="center">
  <a href="https://github.com/rustfs/rustfs/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/rustfs/rustfs/actions/workflows/ci.yml/badge.svg" /></a>
  <a href="https://docs.rustfs.com/en/">📖 Documentation</a>
  · <a href="https://github.com/rustfs/rustfs/issues">🐛 Bug Reports</a>
  · <a href="https://github.com/rustfs/rustfs/discussions">💬 Discussions</a>
</p>

---

## 📖 Overview

**RustFS Zip** provides high-performance compression and archiving capabilities for the [RustFS](https://rustfs.com) distributed object storage system. It supports multiple compression algorithms, streaming compression, and efficient archiving operations optimized for storage systems.

> **Note:** This is a performance-critical submodule of RustFS that provides essential compression capabilities for the distributed object storage system. For the complete RustFS experience, please visit the [main RustFS repository](https://github.com/rustfs/rustfs).

## ✨ Features

### 📦 Compression Algorithms

- **Zstandard (Zstd)**: Fast compression with excellent ratios
- **LZ4**: Ultra-fast compression for real-time applications
- **Gzip**: Industry-standard compression for compatibility
- **Brotli**: Web-optimized compression for text content

### 🚀 Performance Features

- **Streaming Compression**: Compress data on-the-fly without buffering
- **Parallel Processing**: Multi-threaded compression for large files
- **Adaptive Compression**: Automatic algorithm selection based on data
- **Hardware Acceleration**: Leverage CPU-specific optimizations

### 🔧 Archive Management

- **ZIP Format**: Standard ZIP archive creation and extraction
- **TAR Format**: UNIX-style tar archive support
- **Custom Formats**: RustFS-optimized archive formats
- **Metadata Preservation**: Maintain file attributes and timestamps

### 📊 Compression Analytics

- **Ratio Analysis**: Detailed compression statistics
- **Performance Metrics**: Compression speed and efficiency
- **Content-Type Detection**: Automatic compression algorithm selection
- **Deduplication**: Identify and handle duplicate content

## 📦 Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
rustfs-zip = "0.1.0"
```

## 🔧 Usage

### Basic Compression

```rust
use rustfs_zip::{Compressor, CompressionLevel, CompressionAlgorithm};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create compressor
    let compressor = Compressor::new(CompressionAlgorithm::Zstd, CompressionLevel::Default);

    // Compress data
    let input_data = b"Hello, World! This is some test data to compress.";
    let compressed = compressor.compress(input_data).await?;

    println!("Original size: {} bytes", input_data.len());
    println!("Compressed size: {} bytes", compressed.len());
    println!("Compression ratio: {:.2}%",
        (1.0 - compressed.len() as f64 / input_data.len() as f64) * 100.0);

    // Decompress data
    let decompressed = compressor.decompress(&compressed).await?;
    assert_eq!(input_data, decompressed.as_slice());

    Ok(())
}
```

### Streaming Compression

```rust
use rustfs_zip::{StreamingCompressor, StreamingDecompressor};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn streaming_compression_example() -> Result<(), Box<dyn std::error::Error>> {
    // Create streaming compressor
    let mut compressor = StreamingCompressor::new(
        CompressionAlgorithm::Zstd,
        CompressionLevel::Fast,
    )?;

    // Compress streaming data
    let input = tokio::fs::File::open("large_file.txt").await?;
    let output = tokio::fs::File::create("compressed_file.zst").await?;

    let mut reader = tokio::io::BufReader::new(input);
    let mut writer = tokio::io::BufWriter::new(output);

    // Stream compression
    let mut buffer = vec![0u8; 8192];
    loop {
        let bytes_read = reader.read(&mut buffer).await?;
        if bytes_read == 0 {
            break;
        }

        let compressed_chunk = compressor.compress_chunk(&buffer[..bytes_read]).await?;
        writer.write_all(&compressed_chunk).await?;
    }

    // Finalize compression
    let final_chunk = compressor.finalize().await?;
    writer.write_all(&final_chunk).await?;
    writer.flush().await?;

    Ok(())
}
```

### Archive Creation

```rust
use rustfs_zip::{ZipArchive, ArchiveBuilder, CompressionMethod};

async fn create_archive_example() -> Result<(), Box<dyn std::error::Error>> {
    // Create archive builder
    let mut builder = ArchiveBuilder::new("backup.zip".to_string());

    // Add files to archive
    builder.add_file("config.json", "config/app.json", CompressionMethod::Deflate).await?;
    builder.add_file("data.txt", "data/sample.txt", CompressionMethod::Store).await?;

    // Add directory
    builder.add_directory("logs/", "application_logs/").await?;

    // Create archive
    let archive = builder.build().await?;

    println!("Archive created: {}", archive.path());
    println!("Total files: {}", archive.file_count());
    println!("Compressed size: {} bytes", archive.compressed_size());
    println!("Uncompressed size: {} bytes", archive.uncompressed_size());

    Ok(())
}
```

### Archive Extraction

```rust
use rustfs_zip::{ZipExtractor, ExtractionOptions};

async fn extract_archive_example() -> Result<(), Box<dyn std::error::Error>> {
    // Create extractor
    let extractor = ZipExtractor::new("backup.zip".to_string());

    // List archive contents
    let entries = extractor.list_entries().await?;
    for entry in &entries {
        println!("File: {} ({} bytes)", entry.name, entry.size);
    }

    // Extract specific file
    let file_data = extractor.extract_file("config.json").await?;
    println!("Extracted config.json: {} bytes", file_data.len());

    // Extract all files
    let extraction_options = ExtractionOptions {
        output_directory: "extracted/".to_string(),
        preserve_paths: true,
        overwrite_existing: false,
    };

    extractor.extract_all(extraction_options).await?;
    println!("Archive extracted successfully");

    Ok(())
}
```

### Adaptive Compression

```rust
use rustfs_zip::{AdaptiveCompressor, ContentAnalyzer, CompressionProfile};

async fn adaptive_compression_example() -> Result<(), Box<dyn std::error::Error>> {
    // Create adaptive compressor
    let mut compressor = AdaptiveCompressor::new();

    // Configure compression profiles
    compressor.add_profile(CompressionProfile {
        content_type: "text/*".to_string(),
        algorithm: CompressionAlgorithm::Brotli,
        level: CompressionLevel::High,
        min_size: 1024,
    });

    compressor.add_profile(CompressionProfile {
        content_type: "image/*".to_string(),
        algorithm: CompressionAlgorithm::Lz4,
        level: CompressionLevel::Fast,
        min_size: 10240,
    });

    // Compress different types of content
    let text_content = std::fs::read("document.txt")?;
    let image_content = std::fs::read("photo.jpg")?;

    // Analyze and compress
    let text_result = compressor.compress_adaptive(&text_content, Some("text/plain")).await?;
    let image_result = compressor.compress_adaptive(&image_content, Some("image/jpeg")).await?;

    println!("Text compression: {} -> {} bytes ({})",
        text_content.len(), text_result.compressed_size, text_result.algorithm);
    println!("Image compression: {} -> {} bytes ({})",
        image_content.len(), image_result.compressed_size, image_result.algorithm);

    Ok(())
}
```

### Parallel Compression

```rust
use rustfs_zip::{ParallelCompressor, CompressionJob};

async fn parallel_compression_example() -> Result<(), Box<dyn std::error::Error>> {
    // Create parallel compressor
    let compressor = ParallelCompressor::new(4); // 4 worker threads

    // Prepare compression jobs
    let jobs = vec![
        CompressionJob {
            id: "file1".to_string(),
            data: std::fs::read("file1.txt")?,
            algorithm: CompressionAlgorithm::Zstd,
            level: CompressionLevel::Default,
        },
        CompressionJob {
            id: "file2".to_string(),
            data: std::fs::read("file2.txt")?,
            algorithm: CompressionAlgorithm::Lz4,
            level: CompressionLevel::Fast,
        },
        CompressionJob {
            id: "file3".to_string(),
            data: std::fs::read("file3.txt")?,
            algorithm: CompressionAlgorithm::Gzip,
            level: CompressionLevel::High,
        },
    ];

    // Execute parallel compression
    let results = compressor.compress_batch(jobs).await?;

    for result in results {
        println!("Job {}: {} -> {} bytes",
            result.job_id, result.original_size, result.compressed_size);
    }

    Ok(())
}
```

### Content Deduplication

```rust
use rustfs_zip::{DeduplicationCompressor, ContentHash};

async fn deduplication_example() -> Result<(), Box<dyn std::error::Error>> {
    // Create deduplication compressor
    let mut compressor = DeduplicationCompressor::new();

    // Add files for compression
    let file1 = std::fs::read("document1.txt")?;
    let file2 = std::fs::read("document2.txt")?;
    let file3 = std::fs::read("document1.txt")?; // Duplicate of file1

    // Compress with deduplication
    let result1 = compressor.compress_with_dedup("doc1", &file1).await?;
    let result2 = compressor.compress_with_dedup("doc2", &file2).await?;
    let result3 = compressor.compress_with_dedup("doc3", &file3).await?;

    println!("File 1: {} bytes -> {} bytes", file1.len(), result1.compressed_size);
    println!("File 2: {} bytes -> {} bytes", file2.len(), result2.compressed_size);
    println!("File 3: {} bytes -> {} bytes (deduplicated: {})",
        file3.len(), result3.compressed_size, result3.is_deduplicated);

    // Get deduplication statistics
    let stats = compressor.get_dedup_stats();
    println!("Deduplication saved: {} bytes", stats.bytes_saved);
    println!("Duplicate files found: {}", stats.duplicate_count);

    Ok(())
}
```

## 🏗️ Architecture

### Zip Module Architecture

```
Zip Architecture:
┌─────────────────────────────────────────────────────────────┐
│                    Compression API                          │
├─────────────────────────────────────────────────────────────┤
│  Algorithm    │  Streaming    │  Archive     │  Adaptive   │
│  Selection    │  Compression  │  Management  │  Compression│
├─────────────────────────────────────────────────────────────┤
│              Compression Engines                            │
├─────────────────────────────────────────────────────────────┤
│   Zstd    │    LZ4    │   Gzip   │   Brotli  │   Custom   │
├─────────────────────────────────────────────────────────────┤
│              Low-Level Compression                          │
└─────────────────────────────────────────────────────────────┘
```

### Compression Algorithms

| Algorithm | Speed | Ratio | Use Case |
|-----------|-------|-------|----------|
| LZ4 | Very Fast | Good | Real-time compression |
| Zstd | Fast | Excellent | General purpose |
| Gzip | Medium | Good | Web compatibility |
| Brotli | Slow | Excellent | Text/web content |

## 🧪 Testing

Run the test suite:

```bash
# Run all tests
cargo test

# Test compression algorithms
cargo test algorithms

# Test streaming compression
cargo test streaming

# Test archive operations
cargo test archive

# Benchmark compression performance
cargo bench
```

## 📋 Requirements

- **Rust**: 1.70.0 or later
- **Platforms**: Linux, macOS, Windows
- **Dependencies**: Native compression libraries
- **Memory**: Sufficient RAM for compression buffers

## 🌍 Related Projects

This module is part of the RustFS ecosystem:

- [RustFS Main](https://github.com/rustfs/rustfs) - Core distributed storage system
- [RustFS Rio](../rio) - High-performance I/O
- [RustFS Utils](../utils) - Utility functions

## 📚 Documentation

For comprehensive documentation, visit:

- [RustFS Documentation](https://docs.rustfs.com)
- [Zip API Reference](https://docs.rustfs.com/zip/)
- [Compression Guide](https://docs.rustfs.com/compression/)

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
  Made with 📦 by the RustFS Team
</p>
