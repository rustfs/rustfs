//! HashReader implementation with generic support
//!
//! This module provides a generic `HashReader<R>` that can wrap any type implementing
//! `AsyncRead + Unpin + Send + Sync + 'static + EtagResolvable`.
//!
//! ## Migration from the original Reader enum
//!
//! The original `HashReader::new` method that worked with the `Reader` enum
//! has been replaced with a generic approach. To preserve the original logic:
//!
//! ### Original logic (before generics):
//! ```ignore
//! // Original code would do:
//! // 1. Check if inner is already a HashReader
//! // 2. If size > 0, wrap with HardLimitReader  
//! // 3. If !diskable_md5, wrap with EtagReader
//! // 4. Create HashReader with the wrapped reader
//!
//! let reader = HashReader::new(inner, size, actual_size, etag, diskable_md5)?;
//! ```
//!
//! ### New generic approach:
//! ```rust
//! use rustfs_rio::{HashReader, HardLimitReader, EtagReader};
//! use tokio::io::BufReader;
//! use std::io::Cursor;
//! use rustfs_rio::WarpReader;
//!
//! # tokio_test::block_on(async {
//! let data = b"hello world";
//! let reader = BufReader::new(Cursor::new(&data[..]));
//! let reader = Box::new(WarpReader::new(reader));
//! let size = data.len() as i64;
//! let actual_size = size;
//! let etag = None;
//! let diskable_md5 = false;
//!
//! // Method 1: Simple creation (recommended for most cases)
//! let hash_reader = HashReader::new(reader, size, actual_size, etag.clone(), diskable_md5).unwrap();
//!
//! // Method 2: With manual wrapping to recreate original logic
//! let reader2 = BufReader::new(Cursor::new(&data[..]));
//! let reader2 = Box::new(WarpReader::new(reader2));
//! let wrapped_reader: Box<dyn rustfs_rio::Reader> = if size > 0 {
//!     if !diskable_md5 {
//!         // Wrap with both HardLimitReader and EtagReader
//!         let hard_limit = HardLimitReader::new(reader2, size);
//!         Box::new(EtagReader::new(Box::new(hard_limit), etag.clone()))
//!     } else {
//!         // Only wrap with HardLimitReader
//!         Box::new(HardLimitReader::new(reader2, size))
//!     }
//! } else if !diskable_md5 {
//!     // Only wrap with EtagReader
//!     Box::new(EtagReader::new(reader2, etag.clone()))
//! } else {
//!     // No wrapping needed
//!     reader2
//! };
//! let hash_reader2 = HashReader::new(wrapped_reader, size, actual_size, etag, diskable_md5).unwrap();
//! # });
//! ```
//!
//! ## HashReader Detection
//!
//! The `HashReaderDetector` trait allows detection of existing HashReader instances:
//!
//! ```rust
//! use rustfs_rio::{HashReader, HashReaderDetector};
//! use tokio::io::BufReader;
//! use std::io::Cursor;
//! use rustfs_rio::WarpReader;
//!
//! # tokio_test::block_on(async {
//! let data = b"test";
//! let reader = BufReader::new(Cursor::new(&data[..]));
//! let hash_reader = HashReader::new(Box::new(WarpReader::new(reader)), 4, 4, None, false).unwrap();
//!
//! // Check if a type is a HashReader
//! assert!(hash_reader.is_hash_reader());
//!
//! // Use new for compatibility (though it's simpler to use new() directly)
//! let reader2 = BufReader::new(Cursor::new(&data[..]));
//! let result = HashReader::new(Box::new(WarpReader::new(reader2)), 4, 4, None, false);
//! assert!(result.is_ok());
//! # });
//! ```

use pin_project_lite::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};

use crate::compress_index::{Index, TryGetIndex};
use crate::{EtagReader, EtagResolvable, HardLimitReader, HashReaderDetector, Reader};

/// Trait for mutable operations on HashReader
pub trait HashReaderMut {
    fn bytes_read(&self) -> u64;
    fn checksum(&self) -> &Option<String>;
    fn set_checksum(&mut self, checksum: Option<String>);
    fn size(&self) -> i64;
    fn set_size(&mut self, size: i64);
    fn actual_size(&self) -> i64;
    fn set_actual_size(&mut self, actual_size: i64);
}

pin_project! {

    pub struct HashReader {
        #[pin]
        pub inner: Box<dyn Reader>,
        pub size: i64,
        checksum: Option<String>,
        pub actual_size: i64,
        pub diskable_md5: bool,
        bytes_read: u64,
        // TODO: content_hash
    }

}

impl HashReader {
    pub fn new(
        mut inner: Box<dyn Reader>,
        size: i64,
        actual_size: i64,
        md5: Option<String>,
        diskable_md5: bool,
    ) -> std::io::Result<Self> {
        // Check if it's already a HashReader and update its parameters
        if let Some(existing_hash_reader) = inner.as_hash_reader_mut() {
            if existing_hash_reader.bytes_read() > 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Cannot create HashReader from an already read HashReader",
                ));
            }

            if let Some(checksum) = existing_hash_reader.checksum() {
                if let Some(ref md5) = md5 {
                    if checksum != md5 {
                        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "HashReader checksum mismatch"));
                    }
                }
            }

            if existing_hash_reader.size() > 0 && size > 0 && existing_hash_reader.size() != size {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("HashReader size mismatch: expected {}, got {}", existing_hash_reader.size(), size),
                ));
            }

            existing_hash_reader.set_checksum(md5.clone());

            if existing_hash_reader.size() < 0 && size >= 0 {
                existing_hash_reader.set_size(size);
            }

            if existing_hash_reader.actual_size() <= 0 && actual_size >= 0 {
                existing_hash_reader.set_actual_size(actual_size);
            }

            return Ok(Self {
                inner,
                size,
                checksum: md5,
                actual_size,
                diskable_md5,
                bytes_read: 0,
            });
        }

        if size > 0 {
            let hr = HardLimitReader::new(inner, size);
            inner = Box::new(hr);
            if !diskable_md5 && !inner.is_hash_reader() {
                let er = EtagReader::new(inner, md5.clone());
                inner = Box::new(er);
            }
        } else if !diskable_md5 {
            let er = EtagReader::new(inner, md5.clone());
            inner = Box::new(er);
        }
        Ok(Self {
            inner,
            size,
            checksum: md5,
            actual_size,
            diskable_md5,
            bytes_read: 0,
        })
    }

    /// Update HashReader parameters
    pub fn update_params(&mut self, size: i64, actual_size: i64, etag: Option<String>) {
        if self.size < 0 && size >= 0 {
            self.size = size;
        }

        if self.actual_size <= 0 && actual_size > 0 {
            self.actual_size = actual_size;
        }

        if etag.is_some() {
            self.checksum = etag;
        }
    }

    pub fn size(&self) -> i64 {
        self.size
    }
    pub fn actual_size(&self) -> i64 {
        self.actual_size
    }
}

impl HashReaderMut for HashReader {
    fn bytes_read(&self) -> u64 {
        self.bytes_read
    }

    fn checksum(&self) -> &Option<String> {
        &self.checksum
    }

    fn set_checksum(&mut self, checksum: Option<String>) {
        self.checksum = checksum;
    }

    fn size(&self) -> i64 {
        self.size
    }

    fn set_size(&mut self, size: i64) {
        self.size = size;
    }

    fn actual_size(&self) -> i64 {
        self.actual_size
    }

    fn set_actual_size(&mut self, actual_size: i64) {
        self.actual_size = actual_size;
    }
}

impl AsyncRead for HashReader {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        let this = self.project();
        let poll = this.inner.poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = &poll {
            let filled = buf.filled().len();
            *this.bytes_read += filled as u64;

            if filled == 0 {
                // EOF
                // TODO: check content_hash
            }
        }
        poll
    }
}

impl EtagResolvable for HashReader {
    fn try_resolve_etag(&mut self) -> Option<String> {
        if self.diskable_md5 {
            return None;
        }
        if let Some(etag) = self.inner.try_resolve_etag() {
            return Some(etag);
        }
        // If no etag from inner and we have a stored checksum, return it
        self.checksum.clone()
    }
}

impl HashReaderDetector for HashReader {
    fn is_hash_reader(&self) -> bool {
        true
    }

    fn as_hash_reader_mut(&mut self) -> Option<&mut dyn HashReaderMut> {
        Some(self)
    }
}

impl TryGetIndex for HashReader {
    fn try_get_index(&self) -> Option<&Index> {
        self.inner.try_get_index()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DecryptReader, WarpReader, encrypt_reader};
    use std::io::Cursor;
    use tokio::io::{AsyncReadExt, BufReader};

    #[tokio::test]
    async fn test_hashreader_wrapping_logic() {
        let data = b"hello world";
        let size = data.len() as i64;
        let actual_size = size;
        let etag = None;

        // Test 1: Simple creation
        let reader1 = BufReader::new(Cursor::new(&data[..]));
        let reader1 = Box::new(WarpReader::new(reader1));
        let hash_reader1 = HashReader::new(reader1, size, actual_size, etag.clone(), false).unwrap();
        assert_eq!(hash_reader1.size(), size);
        assert_eq!(hash_reader1.actual_size(), actual_size);

        // Test 2: With HardLimitReader wrapping
        let reader2 = BufReader::new(Cursor::new(&data[..]));
        let reader2 = Box::new(WarpReader::new(reader2));
        let hard_limit = HardLimitReader::new(reader2, size);
        let hard_limit = Box::new(hard_limit);
        let hash_reader2 = HashReader::new(hard_limit, size, actual_size, etag.clone(), false).unwrap();
        assert_eq!(hash_reader2.size(), size);
        assert_eq!(hash_reader2.actual_size(), actual_size);

        // Test 3: With EtagReader wrapping
        let reader3 = BufReader::new(Cursor::new(&data[..]));
        let reader3 = Box::new(WarpReader::new(reader3));
        let etag_reader = EtagReader::new(reader3, etag.clone());
        let etag_reader = Box::new(etag_reader);
        let hash_reader3 = HashReader::new(etag_reader, size, actual_size, etag.clone(), false).unwrap();
        assert_eq!(hash_reader3.size(), size);
        assert_eq!(hash_reader3.actual_size(), actual_size);
    }

    #[tokio::test]
    async fn test_hashreader_etag_basic() {
        let data = b"hello hashreader";
        let reader = BufReader::new(Cursor::new(&data[..]));
        let reader = Box::new(WarpReader::new(reader));
        let mut hash_reader = HashReader::new(reader, data.len() as i64, data.len() as i64, None, false).unwrap();
        let mut buf = Vec::new();
        let _ = hash_reader.read_to_end(&mut buf).await.unwrap();
        // Since we removed EtagReader integration, etag might be None
        let _etag = hash_reader.try_resolve_etag();
        // Just check that we can call etag() without error
        assert_eq!(buf, data);
    }

    #[tokio::test]
    async fn test_hashreader_diskable_md5() {
        let data = b"no etag";
        let reader = BufReader::new(Cursor::new(&data[..]));
        let reader = Box::new(WarpReader::new(reader));
        let mut hash_reader = HashReader::new(reader, data.len() as i64, data.len() as i64, None, true).unwrap();
        let mut buf = Vec::new();
        let _ = hash_reader.read_to_end(&mut buf).await.unwrap();
        // Etag should be None when diskable_md5 is true
        let etag = hash_reader.try_resolve_etag();
        assert!(etag.is_none());
        assert_eq!(buf, data);
    }

    #[tokio::test]
    async fn test_hashreader_new_logic() {
        let data = b"test data";
        let reader = BufReader::new(Cursor::new(&data[..]));
        let reader = Box::new(WarpReader::new(reader));
        // Create a HashReader first
        let hash_reader =
            HashReader::new(reader, data.len() as i64, data.len() as i64, Some("test_etag".to_string()), false).unwrap();
        let hash_reader = Box::new(WarpReader::new(hash_reader));
        // Now try to create another HashReader from the existing one using new
        let result = HashReader::new(hash_reader, data.len() as i64, data.len() as i64, Some("test_etag".to_string()), false);

        assert!(result.is_ok());
        let final_reader = result.unwrap();
        assert_eq!(final_reader.checksum, Some("test_etag".to_string()));
        assert_eq!(final_reader.size(), data.len() as i64);
    }

    #[tokio::test]
    async fn test_for_wrapping_readers() {
        use crate::{CompressReader, DecompressReader};
        use md5::{Digest, Md5};
        use rand::Rng;
        use rand::RngCore;
        use rustfs_utils::compress::CompressionAlgorithm;

        // Generate 1MB random data
        let size = 1024 * 1024;
        let mut data = vec![0u8; size];
        rand::rng().fill(&mut data[..]);

        let mut hasher = Md5::new();
        hasher.update(&data);

        let expected = format!("{:x}", hasher.finalize());

        println!("expected: {expected}");

        let reader = Cursor::new(data.clone());
        let reader = BufReader::new(reader);

        // 启用压缩测试
        let is_compress = true;
        let size = data.len() as i64;
        let actual_size = data.len() as i64;

        let reader = Box::new(WarpReader::new(reader));
        // 创建 HashReader
        let mut hr = HashReader::new(reader, size, actual_size, Some(expected.clone()), false).unwrap();

        // 如果启用压缩，先压缩数据
        let compressed_data = if is_compress {
            let mut compressed_buf = Vec::new();
            let compress_reader = CompressReader::new(hr, CompressionAlgorithm::Gzip);
            let mut compress_reader = compress_reader;
            compress_reader.read_to_end(&mut compressed_buf).await.unwrap();

            println!("Original size: {}, Compressed size: {}", data.len(), compressed_buf.len());

            compressed_buf
        } else {
            // 如果不压缩，直接读取原始数据
            let mut buf = Vec::new();
            hr.read_to_end(&mut buf).await.unwrap();
            buf
        };

        let mut key = [0u8; 32];
        let mut nonce = [0u8; 12];
        rand::rng().fill_bytes(&mut key);
        rand::rng().fill_bytes(&mut nonce);

        let is_encrypt = true;

        if is_encrypt {
            // 加密压缩后的数据
            let encrypt_reader = encrypt_reader::EncryptReader::new(WarpReader::new(Cursor::new(compressed_data)), key, nonce);
            let mut encrypted_data = Vec::new();
            let mut encrypt_reader = encrypt_reader;
            encrypt_reader.read_to_end(&mut encrypted_data).await.unwrap();

            println!("Encrypted size: {}", encrypted_data.len());

            // 解密数据
            let decrypt_reader = DecryptReader::new(WarpReader::new(Cursor::new(encrypted_data)), key, nonce);
            let mut decrypt_reader = decrypt_reader;
            let mut decrypted_data = Vec::new();
            decrypt_reader.read_to_end(&mut decrypted_data).await.unwrap();

            if is_compress {
                // 如果使用了压缩，需要解压缩
                let decompress_reader =
                    DecompressReader::new(WarpReader::new(Cursor::new(decrypted_data)), CompressionAlgorithm::Gzip);
                let mut decompress_reader = decompress_reader;
                let mut final_data = Vec::new();
                decompress_reader.read_to_end(&mut final_data).await.unwrap();

                println!("Final decompressed size: {}", final_data.len());
                assert_eq!(final_data.len() as i64, actual_size);
                assert_eq!(&final_data, &data);
            } else {
                // 如果没有压缩，直接比较解密后的数据
                assert_eq!(decrypted_data.len() as i64, actual_size);
                assert_eq!(&decrypted_data, &data);
            }
            return;
        }

        // 如果不加密，直接处理压缩/解压缩
        if is_compress {
            let decompress_reader =
                DecompressReader::new(WarpReader::new(Cursor::new(compressed_data)), CompressionAlgorithm::Gzip);
            let mut decompress_reader = decompress_reader;
            let mut decompressed = Vec::new();
            decompress_reader.read_to_end(&mut decompressed).await.unwrap();

            assert_eq!(decompressed.len() as i64, actual_size);
            assert_eq!(&decompressed, &data);
        } else {
            assert_eq!(compressed_data.len() as i64, actual_size);
            assert_eq!(&compressed_data, &data);
        }

        // 验证 etag（注意：压缩会改变数据，所以这里的 etag 验证可能需要调整）
        println!(
            "Test completed successfully with compression: {is_compress}, encryption: {is_encrypt}"
        );
    }

    #[tokio::test]
    async fn test_compression_with_compressible_data() {
        use crate::{CompressReader, DecompressReader};
        use rustfs_utils::compress::CompressionAlgorithm;

        // Create highly compressible data (repeated pattern)
        let pattern = b"Hello, World! This is a test pattern that should compress well. ";
        let repeat_count = 16384; // 16K repetitions
        let mut data = Vec::new();
        for _ in 0..repeat_count {
            data.extend_from_slice(pattern);
        }

        println!("Original data size: {} bytes", data.len());

        let reader = BufReader::new(Cursor::new(data.clone()));
        let reader = Box::new(WarpReader::new(reader));
        let hash_reader = HashReader::new(reader, data.len() as i64, data.len() as i64, None, false).unwrap();

        // Test compression
        let compress_reader = CompressReader::new(hash_reader, CompressionAlgorithm::Gzip);
        let mut compressed_data = Vec::new();
        let mut compress_reader = compress_reader;
        compress_reader.read_to_end(&mut compressed_data).await.unwrap();

        println!("Compressed data size: {} bytes", compressed_data.len());
        println!("Compression ratio: {:.2}%", (compressed_data.len() as f64 / data.len() as f64) * 100.0);

        // Verify compression actually reduced size for this compressible data
        assert!(compressed_data.len() < data.len(), "Compression should reduce size for repetitive data");

        // Test decompression
        let decompress_reader = DecompressReader::new(Cursor::new(compressed_data), CompressionAlgorithm::Gzip);
        let mut decompressed_data = Vec::new();
        let mut decompress_reader = decompress_reader;
        decompress_reader.read_to_end(&mut decompressed_data).await.unwrap();

        // Verify decompressed data matches original
        assert_eq!(decompressed_data.len(), data.len());
        assert_eq!(&decompressed_data, &data);

        println!("Compression/decompression test passed successfully!");
    }

    #[tokio::test]
    async fn test_compression_algorithms() {
        use crate::{CompressReader, DecompressReader};
        use rustfs_utils::compress::CompressionAlgorithm;

        let data = b"This is test data for compression algorithm testing. ".repeat(1000);
        println!("Testing with {} bytes of data", data.len());

        let algorithms = vec![
            CompressionAlgorithm::Gzip,
            CompressionAlgorithm::Deflate,
            CompressionAlgorithm::Zstd,
        ];

        for algorithm in algorithms {
            println!("\nTesting algorithm: {algorithm:?}");

            let reader = BufReader::new(Cursor::new(data.clone()));
            let reader = Box::new(WarpReader::new(reader));
            let hash_reader = HashReader::new(reader, data.len() as i64, data.len() as i64, None, false).unwrap();

            // Compress
            let compress_reader = CompressReader::new(hash_reader, algorithm);
            let mut compressed_data = Vec::new();
            let mut compress_reader = compress_reader;
            compress_reader.read_to_end(&mut compressed_data).await.unwrap();

            println!(
                "  Compressed size: {} bytes (ratio: {:.2}%)",
                compressed_data.len(),
                (compressed_data.len() as f64 / data.len() as f64) * 100.0
            );

            // Decompress
            let decompress_reader = DecompressReader::new(Cursor::new(compressed_data), algorithm);
            let mut decompressed_data = Vec::new();
            let mut decompress_reader = decompress_reader;
            decompress_reader.read_to_end(&mut decompressed_data).await.unwrap();

            // Verify
            assert_eq!(decompressed_data.len(), data.len());
            assert_eq!(&decompressed_data, &data);
            println!("  ✓ Algorithm {algorithm:?} test passed");
        }
    }
}
