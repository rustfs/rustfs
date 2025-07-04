# rustfs-zip

[![Crates.io](https://img.shields.io/crates/v/rustfs-zip.svg)](https://crates.io/crates/rustfs-zip)
[![Docs.rs](https://docs.rs/rustfs-zip/badge.svg)](https://docs.rs/rustfs-zip)
[![License](https://img.shields.io/crates/l/rustfs-zip.svg)](../../LICENSE)

`rustfs-zip` is a crate within the RustFS ecosystem designed for efficient, asynchronous handling of ZIP archives. It
provides robust support for both reading and writing ZIP files, leveraging the power of Tokio for non-blocking I/O
operations.

## Features

* **Fully Asynchronous:** Built on `tokio` for high-performance, non-blocking ZIP file manipulation.
* **Read & Write Support:** Comprehensive APIs for creating new archives and extracting existing ones.
* **Multiple Compression Algorithms:** Supports a wide range of compression methods, including `zlib`, `bzip2`, `gzip`,
  `zstd`, and `xz`.
* **Streaming API:** Designed to handle large files efficiently without consuming excessive memory.

## Installation

Add `rustfs-zip` to your `Cargo.toml` dependencies:

```toml
[dependencies]
rustfs-zip = "0.0.3" # Replace with the latest version
```

## Usage

Here is a basic example of how to create a ZIP archive:

```rust
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use rustfs_zip::ZipWriter;
use rustfs_zip::write::FileOptions;

async fn create_archive() -> Result<(), Box<dyn std::error::Error>> {
    let file = File::create("archive.zip").await?;
    let mut zip = ZipWriter::new(file);

    let options = FileOptions::default().compression_method(rustfs_zip::CompressionMethod::Deflated);

    zip.start_file("file1.txt", options).await?;
    zip.write_all(b"Hello, world!").await?;

    zip.start_file("another/file.txt", options).await?;
    zip.write_all(b"This is another file in a directory.").await?;

    // Finish the archive.
    zip.finish().await?;

    Ok(())
}
```

## License

This project is licensed under the Apache License, Version 2.0. See the [`LICENSE`](../../LICENSE) file for details.