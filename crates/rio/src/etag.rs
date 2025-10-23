// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/*!
# AsyncRead Wrapper Types with ETag Support

This module demonstrates a pattern for handling wrapped AsyncRead types where:
- Reader types contain the actual ETag capability
- Wrapper types need to be recursively unwrapped
- The system can handle arbitrary nesting like `CompressReader<EncryptReader<EtagReader<R>>>`

## Key Components

### Trait-Based Approach
The `EtagResolvable` trait provides a clean way to handle recursive unwrapping:
- Reader types implement it by returning their ETag directly
- Wrapper types implement it by delegating to their inner type

## Usage Examples

```rust
use rustfs_rio::{CompressReader, EtagReader, resolve_etag_generic};
use rustfs_rio::WarpReader;
use rustfs_utils::compress::CompressionAlgorithm;
use tokio::io::BufReader;
use std::io::Cursor;

// Direct usage with trait-based approach
let data = b"test data";
let reader = BufReader::new(Cursor::new(&data[..]));
let reader = Box::new(WarpReader::new(reader));
let etag_reader = EtagReader::new(reader, Some("test_etag".to_string()));
let mut reader = CompressReader::new(etag_reader, CompressionAlgorithm::Gzip);
let etag = resolve_etag_generic(&mut reader);
```
*/

#[cfg(test)]
mod tests {

    use crate::{CompressReader, EncryptReader, EtagReader, HashReader};
    use crate::{WarpReader, resolve_etag_generic};
    use md5::Md5;
    use rustfs_utils::compress::CompressionAlgorithm;
    use std::io::Cursor;
    use tokio::io::BufReader;

    #[test]
    fn test_etag_reader_resolution() {
        let data = b"test data";
        let reader = BufReader::new(Cursor::new(&data[..]));
        let reader = Box::new(WarpReader::new(reader));
        let mut etag_reader = EtagReader::new(reader, Some("test_etag".to_string()));

        // Test direct ETag resolution
        assert_eq!(resolve_etag_generic(&mut etag_reader), Some("test_etag".to_string()));
    }

    #[test]
    fn test_hash_reader_resolution() {
        let data = b"test data";
        let reader = BufReader::new(Cursor::new(&data[..]));
        let reader = Box::new(WarpReader::new(reader));
        let mut hash_reader =
            HashReader::new(reader, data.len() as i64, data.len() as i64, Some("hash_etag".to_string()), None, false).unwrap();

        // Test HashReader ETag resolution
        assert_eq!(resolve_etag_generic(&mut hash_reader), Some("hash_etag".to_string()));
    }

    #[test]
    fn test_compress_reader_delegation() {
        let data = b"test data for compression";
        let reader = BufReader::new(Cursor::new(&data[..]));
        let reader = Box::new(WarpReader::new(reader));
        let etag_reader = EtagReader::new(reader, Some("compress_etag".to_string()));
        let mut compress_reader = CompressReader::new(etag_reader, CompressionAlgorithm::Gzip);

        // Test that CompressReader delegates to inner EtagReader
        assert_eq!(resolve_etag_generic(&mut compress_reader), Some("compress_etag".to_string()));
    }

    #[test]
    fn test_encrypt_reader_delegation() {
        let data = b"test data for encryption";
        let reader = BufReader::new(Cursor::new(&data[..]));
        let reader = Box::new(WarpReader::new(reader));
        let etag_reader = EtagReader::new(reader, Some("encrypt_etag".to_string()));

        let key = [0u8; 32];
        let nonce = [0u8; 12];
        let mut encrypt_reader = EncryptReader::new(etag_reader, key, nonce);

        // Test that EncryptReader delegates to inner EtagReader
        assert_eq!(resolve_etag_generic(&mut encrypt_reader), Some("encrypt_etag".to_string()));
    }

    #[tokio::test]
    async fn test_complex_nesting() {
        use md5::Digest;
        use tokio::io::AsyncReadExt;
        let data = b"test data for complex nesting";

        let mut hasher = Md5::new();
        hasher.update(data);
        let etag = hasher.finalize();
        let etag_hex = hex_simd::encode_to_string(etag, hex_simd::AsciiCase::Lower);

        let reader = BufReader::new(Cursor::new(&data[..]));
        let reader = Box::new(WarpReader::new(reader));
        // Create a complex nested structure: CompressReader<EncryptReader<EtagReader<BufReader<Cursor>>>>
        let etag_reader = EtagReader::new(reader, Some(etag_hex.clone()));
        let key = [0u8; 32];
        let nonce = [0u8; 12];
        let encrypt_reader = EncryptReader::new(etag_reader, key, nonce);
        let mut compress_reader = CompressReader::new(encrypt_reader, CompressionAlgorithm::Gzip);

        compress_reader.read_to_end(&mut Vec::new()).await.unwrap();

        // Test that nested structure can resolve ETag
        assert_eq!(resolve_etag_generic(&mut compress_reader), Some(etag_hex));
    }

    #[test]
    fn test_hash_reader_in_nested_structure() {
        let data = b"test data for hash reader nesting";
        let reader = BufReader::new(Cursor::new(&data[..]));
        let reader = Box::new(WarpReader::new(reader));
        // Create nested structure: CompressReader<HashReader<BufReader<Cursor>>>
        let hash_reader = HashReader::new(
            reader,
            data.len() as i64,
            data.len() as i64,
            Some("hash_nested_etag".to_string()),
            None,
            false,
        )
        .unwrap();
        let mut compress_reader = CompressReader::new(hash_reader, CompressionAlgorithm::Deflate);

        // Test that nested HashReader can be resolved
        assert_eq!(resolve_etag_generic(&mut compress_reader), Some("hash_nested_etag".to_string()));
    }

    #[tokio::test]
    async fn test_comprehensive_etag_extraction() {
        use md5::Digest;
        use tokio::io::AsyncReadExt;
        println!("🔍 Testing comprehensive ETag extraction with real reader types...");

        // Test 1: Simple EtagReader
        let data1 = b"simple test";
        let mut hasher = Md5::new();
        hasher.update(data1);
        let etag = hasher.finalize();
        let etag_hex = hex_simd::encode_to_string(etag, hex_simd::AsciiCase::Lower);
        let reader1 = BufReader::new(Cursor::new(&data1[..]));
        let reader1 = Box::new(WarpReader::new(reader1));
        let mut etag_reader = EtagReader::new(reader1, Some(etag_hex.clone()));
        etag_reader.read_to_end(&mut Vec::new()).await.unwrap();
        assert_eq!(resolve_etag_generic(&mut etag_reader), Some(etag_hex.clone()));

        // Test 2: HashReader with ETag
        let data2 = b"hash test";
        let mut hasher = Md5::new();
        hasher.update(data2);
        let etag = hasher.finalize();
        let etag_hex = hex_simd::encode_to_string(etag, hex_simd::AsciiCase::Lower);
        let reader2 = BufReader::new(Cursor::new(&data2[..]));
        let reader2 = Box::new(WarpReader::new(reader2));
        let mut hash_reader =
            HashReader::new(reader2, data2.len() as i64, data2.len() as i64, Some(etag_hex.clone()), None, false).unwrap();
        hash_reader.read_to_end(&mut Vec::new()).await.unwrap();
        assert_eq!(resolve_etag_generic(&mut hash_reader), Some(etag_hex.clone()));

        // Test 3: Single wrapper - CompressReader<EtagReader>
        let data3 = b"compress test";
        let mut hasher = Md5::new();
        hasher.update(data3);
        let etag = hasher.finalize();
        let etag_hex = hex_simd::encode_to_string(etag, hex_simd::AsciiCase::Lower);
        let reader3 = BufReader::new(Cursor::new(&data3[..]));
        let reader3 = Box::new(WarpReader::new(reader3));
        let etag_reader3 = EtagReader::new(reader3, Some(etag_hex.clone()));
        let mut compress_reader = CompressReader::new(etag_reader3, CompressionAlgorithm::Zstd);
        compress_reader.read_to_end(&mut Vec::new()).await.unwrap();
        assert_eq!(resolve_etag_generic(&mut compress_reader), Some(etag_hex.clone()));

        // Test 4: Double wrapper - CompressReader<EncryptReader<EtagReader>>
        let data4 = b"double wrap test";
        let mut hasher = Md5::new();
        hasher.update(data4);
        let etag = hasher.finalize();
        let etag_hex = hex_simd::encode_to_string(etag, hex_simd::AsciiCase::Lower);
        let reader4 = BufReader::new(Cursor::new(&data4[..]));
        let reader4 = Box::new(WarpReader::new(reader4));
        let etag_reader4 = EtagReader::new(reader4, Some(etag_hex.clone()));
        let key = [1u8; 32];
        let nonce = [1u8; 12];
        let encrypt_reader4 = EncryptReader::new(etag_reader4, key, nonce);
        let mut compress_reader4 = CompressReader::new(encrypt_reader4, CompressionAlgorithm::Gzip);
        compress_reader4.read_to_end(&mut Vec::new()).await.unwrap();
        assert_eq!(resolve_etag_generic(&mut compress_reader4), Some(etag_hex.clone()));

        println!("✅ All ETag extraction methods work correctly!");
        println!("✅ Trait-based approach handles recursive unwrapping!");
        println!("✅ Complex nesting patterns with real reader types are supported!");
    }

    #[test]
    fn test_real_world_scenario() {
        println!("🔍 Testing real-world ETag extraction scenario with actual reader types...");

        // Simulate a real-world scenario where we have nested AsyncRead wrappers
        // and need to extract ETag information from deeply nested structures

        let data = b"Real world test data that might be compressed and encrypted";
        let base_reader = BufReader::new(Cursor::new(&data[..]));
        let base_reader = Box::new(WarpReader::new(base_reader));
        // Create a complex nested structure that might occur in practice:
        // CompressReader<EncryptReader<HashReader<BufReader<Cursor>>>>
        let hash_reader = HashReader::new(
            base_reader,
            data.len() as i64,
            data.len() as i64,
            Some("real_world_etag".to_string()),
            None,
            false,
        )
        .unwrap();
        let key = [42u8; 32];
        let nonce = [24u8; 12];
        let encrypt_reader = EncryptReader::new(hash_reader, key, nonce);
        let mut compress_reader = CompressReader::new(encrypt_reader, CompressionAlgorithm::Deflate);

        // Extract ETag using our generic system
        let extracted_etag = resolve_etag_generic(&mut compress_reader);
        println!("📋 Extracted ETag: {extracted_etag:?}");

        assert_eq!(extracted_etag, Some("real_world_etag".to_string()));

        // Test another complex nesting with EtagReader at the core
        let data2 = b"Another real world scenario";
        let base_reader2 = BufReader::new(Cursor::new(&data2[..]));
        let base_reader2 = Box::new(WarpReader::new(base_reader2));
        let etag_reader = EtagReader::new(base_reader2, Some("core_etag".to_string()));
        let key2 = [99u8; 32];
        let nonce2 = [88u8; 12];
        let encrypt_reader2 = EncryptReader::new(etag_reader, key2, nonce2);
        let mut compress_reader2 = CompressReader::new(encrypt_reader2, CompressionAlgorithm::Zstd);

        let trait_etag = resolve_etag_generic(&mut compress_reader2);
        println!("📋 Trait-based ETag: {trait_etag:?}");

        assert_eq!(trait_etag, Some("core_etag".to_string()));

        println!("✅ Real-world scenario test passed!");
        println!("   - Successfully extracted ETag from nested CompressReader<EncryptReader<HashReader<AsyncRead>>>");
        println!("   - Successfully extracted ETag from nested CompressReader<EncryptReader<EtagReader<AsyncRead>>>");
        println!("   - Trait-based approach works with real reader types");
        println!("   - System handles arbitrary nesting depths with actual implementations");
    }

    #[test]
    fn test_no_etag_scenarios() {
        println!("🔍 Testing scenarios where no ETag is available...");

        // Test with HashReader that has no etag
        let data = b"no etag test";
        let reader = BufReader::new(Cursor::new(&data[..]));
        let reader = Box::new(WarpReader::new(reader));
        let mut hash_reader_no_etag = HashReader::new(reader, data.len() as i64, data.len() as i64, None, None, false).unwrap();
        assert_eq!(resolve_etag_generic(&mut hash_reader_no_etag), None);

        // Test with EtagReader that has None etag
        let data2 = b"no etag test 2";
        let reader2 = BufReader::new(Cursor::new(&data2[..]));
        let reader2 = Box::new(WarpReader::new(reader2));
        let mut etag_reader_none = EtagReader::new(reader2, None);
        assert_eq!(resolve_etag_generic(&mut etag_reader_none), None);

        // Test nested structure with no ETag at the core
        let data3 = b"nested no etag test";
        let reader3 = BufReader::new(Cursor::new(&data3[..]));
        let reader3 = Box::new(WarpReader::new(reader3));
        let etag_reader3 = EtagReader::new(reader3, None);
        let mut compress_reader3 = CompressReader::new(etag_reader3, CompressionAlgorithm::Gzip);
        assert_eq!(resolve_etag_generic(&mut compress_reader3), None);

        println!("✅ No ETag scenarios handled correctly!");
    }
}
