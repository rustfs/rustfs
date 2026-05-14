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

#[cfg(feature = "rio-v2")]
pub use rustfs_rio_v2::*;

#[cfg(not(feature = "rio-v2"))]
pub use rustfs_rio::*;

use bytes::Bytes;
use rustfs_utils::CompressionAlgorithm;
use std::str::FromStr;
use tokio::io::AsyncRead;

#[cfg(feature = "rio-v2")]
const MINIO_S2_COMPRESSION_SCHEME: &str = "klauspost/compress/s2";
#[cfg(feature = "rio-v2")]
const S2_INDEX_HEADER: &[u8] = b"s2idx\x00";
#[cfg(feature = "rio-v2")]
const S2_INDEX_TRAILER: &[u8] = b"\x00xdi2s";
#[cfg(feature = "rio-v2")]
const ENCRYPTED_S2_PADDING_MULTIPLE: usize = 256;

pub const fn backend_name() -> &'static str {
    #[cfg(feature = "rio-v2")]
    {
        "rio-v2"
    }

    #[cfg(not(feature = "rio-v2"))]
    {
        "legacy-rio"
    }
}

pub fn compression_metadata_value(algorithm: CompressionAlgorithm) -> String {
    #[cfg(feature = "rio-v2")]
    {
        let _ = algorithm;
        MINIO_S2_COMPRESSION_SCHEME.to_string()
    }

    #[cfg(not(feature = "rio-v2"))]
    {
        algorithm.to_string()
    }
}

pub fn compression_scheme_to_algorithm(scheme: &str) -> std::io::Result<CompressionAlgorithm> {
    #[cfg(feature = "rio-v2")]
    if scheme.eq_ignore_ascii_case(MINIO_S2_COMPRESSION_SCHEME) {
        // rio_v2 currently routes all compressed-object handling through the S2
        // reader implementation, so the enum is only a placeholder token here.
        return Ok(CompressionAlgorithm::default());
    }

    CompressionAlgorithm::from_str(scheme)
}

pub fn compression_index_storage_bytes(index: &Index) -> Bytes {
    #[cfg(feature = "rio-v2")]
    {
        let encoded = index.clone().into_vec();
        return strip_index_headers(encoded.as_ref())
            .map(Bytes::copy_from_slice)
            .unwrap_or(encoded);
    }

    #[cfg(not(feature = "rio-v2"))]
    {
        index.clone().into_vec()
    }
}

pub fn decode_compression_index_bytes(bytes: &Bytes) -> Option<Index> {
    let mut decoded = Index::new();
    if decoded.load(bytes.as_ref()).is_ok() {
        return Some(decoded);
    }

    #[cfg(feature = "rio-v2")]
    {
        let restored = restore_index_headers(bytes.as_ref());
        let mut decoded = Index::new();
        if decoded.load(&restored).is_ok() {
            return Some(decoded);
        }
    }

    None
}

pub fn compression_reader<R>(reader: R, algorithm: CompressionAlgorithm, encrypted: bool) -> CompressReader<R>
where
    R: AsyncRead + Unpin + Send + Sync,
{
    #[cfg(feature = "rio-v2")]
    {
        if encrypted {
            return CompressReader::with_encrypted_padding(reader, algorithm);
        }
    }

    #[cfg(not(feature = "rio-v2"))]
    let _ = encrypted;

    CompressReader::new(reader, algorithm)
}

#[cfg(feature = "rio-v2")]
fn strip_index_headers(bytes: &[u8]) -> Option<&[u8]> {
    let header_len = 4 + S2_INDEX_HEADER.len();
    let trailer_len = 4 + S2_INDEX_TRAILER.len();
    if bytes.len() < header_len + trailer_len {
        return None;
    }

    if &bytes[4..header_len] != S2_INDEX_HEADER {
        return None;
    }

    if &bytes[bytes.len() - S2_INDEX_TRAILER.len()..] != S2_INDEX_TRAILER {
        return None;
    }

    Some(&bytes[header_len..bytes.len() - trailer_len])
}

#[cfg(feature = "rio-v2")]
fn restore_index_headers(bytes: &[u8]) -> Vec<u8> {
    if bytes.is_empty() {
        return Vec::new();
    }

    let mut restored = Vec::with_capacity(4 + S2_INDEX_HEADER.len() + bytes.len() + 4 + S2_INDEX_TRAILER.len());
    restored.extend_from_slice(&[0x50, 0x2A, 0x4D, 0x18]);
    restored.extend_from_slice(S2_INDEX_HEADER);
    restored.extend_from_slice(bytes);

    let total_size = (restored.len() + 4 + S2_INDEX_TRAILER.len()) as u32;
    restored.extend_from_slice(&total_size.to_le_bytes());
    restored.extend_from_slice(S2_INDEX_TRAILER);

    let chunk_len = restored.len() - 4;
    restored[1] = chunk_len as u8;
    restored[2] = (chunk_len >> 8) as u8;
    restored[3] = (chunk_len >> 16) as u8;
    restored
}

#[derive(Debug, Clone, Copy)]
pub struct WriteEncryption {
    key_bytes: [u8; 32],
    mode: WriteEncryptionMode,
}

#[derive(Debug, Clone, Copy)]
enum WriteEncryptionMode {
    SinglepartObjectKey,
    Singlepart {
        base_nonce: [u8; 12],
    },
    MultipartLegacy {
        base_nonce: [u8; 12],
        multipart_part_number: usize,
    },
    MultipartObjectKey {
        multipart_part_number: u32,
    },
}

impl WriteEncryption {
    pub const fn singlepart_object_key(object_key: [u8; 32]) -> Self {
        Self {
            key_bytes: object_key,
            mode: WriteEncryptionMode::SinglepartObjectKey,
        }
    }

    pub const fn singlepart(key_bytes: [u8; 32], base_nonce: [u8; 12]) -> Self {
        Self {
            key_bytes,
            mode: WriteEncryptionMode::Singlepart { base_nonce },
        }
    }

    pub const fn multipart(key_bytes: [u8; 32], base_nonce: [u8; 12], multipart_part_number: usize) -> Self {
        Self {
            key_bytes,
            mode: WriteEncryptionMode::MultipartLegacy {
                base_nonce,
                multipart_part_number,
            },
        }
    }

    pub const fn multipart_object_key(object_key: [u8; 32], multipart_part_number: u32) -> Self {
        Self {
            key_bytes: object_key,
            mode: WriteEncryptionMode::MultipartObjectKey { multipart_part_number },
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct WritePlan {
    compression: Option<CompressionAlgorithm>,
    encryption: Option<WriteEncryption>,
}

impl WritePlan {
    pub const fn new() -> Self {
        Self {
            compression: None,
            encryption: None,
        }
    }

    pub const fn with_compression(mut self, algorithm: CompressionAlgorithm) -> Self {
        self.compression = Some(algorithm);
        self
    }

    pub const fn with_encryption(mut self, encryption: WriteEncryption) -> Self {
        self.encryption = Some(encryption);
        self
    }

    pub const fn is_passthrough(&self) -> bool {
        self.compression.is_none() && self.encryption.is_none()
    }

    pub fn apply(self, mut reader: HashReader, actual_size: i64) -> std::io::Result<HashReader> {
        let encrypted = self.encryption.is_some();
        if let Some(algorithm) = self.compression {
            reader = HashReader::from_reader(
                compression_reader(reader, algorithm, encrypted),
                HashReader::SIZE_PRESERVE_LAYER,
                actual_size,
                None,
                None,
                false,
            )?;
        }

        if let Some(encryption) = self.encryption {
            reader = match encryption.mode {
                WriteEncryptionMode::SinglepartObjectKey => HashReader::from_reader(
                    #[cfg(feature = "rio-v2")]
                    EncryptReader::new_with_object_key(reader, encryption.key_bytes),
                    #[cfg(not(feature = "rio-v2"))]
                    EncryptReader::new(reader, encryption.key_bytes, [0u8; 12]),
                    HashReader::SIZE_PRESERVE_LAYER,
                    actual_size,
                    None,
                    None,
                    false,
                )?,
                WriteEncryptionMode::Singlepart { base_nonce } => HashReader::from_reader(
                    EncryptReader::new(reader, encryption.key_bytes, base_nonce),
                    HashReader::SIZE_PRESERVE_LAYER,
                    actual_size,
                    None,
                    None,
                    false,
                )?,
                WriteEncryptionMode::MultipartLegacy {
                    base_nonce,
                    multipart_part_number,
                } => HashReader::from_reader(
                    EncryptReader::new_multipart(reader, encryption.key_bytes, base_nonce, multipart_part_number),
                    HashReader::SIZE_PRESERVE_LAYER,
                    actual_size,
                    None,
                    None,
                    false,
                )?,
                WriteEncryptionMode::MultipartObjectKey { multipart_part_number } => HashReader::from_reader(
                    #[cfg(feature = "rio-v2")]
                    EncryptReader::new_multipart_with_object_key(reader, encryption.key_bytes, multipart_part_number),
                    #[cfg(not(feature = "rio-v2"))]
                    EncryptReader::new_multipart(reader, encryption.key_bytes, [0u8; 12], multipart_part_number as usize),
                    HashReader::SIZE_PRESERVE_LAYER,
                    actual_size,
                    None,
                    None,
                    false,
                )?,
            };
        }

        Ok(reader)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_utils::CompressionAlgorithm;
    use std::io::Cursor;
    use tokio::io::AsyncReadExt;

    #[cfg(feature = "rio-v2")]
    fn s2_chunk_types(stream: &[u8]) -> Vec<u8> {
        let mut chunk_types = Vec::new();
        let mut offset = 0usize;
        while offset + 4 <= stream.len() {
            let chunk_type = stream[offset];
            let chunk_len =
                (stream[offset + 1] as usize) | ((stream[offset + 2] as usize) << 8) | ((stream[offset + 3] as usize) << 16);
            chunk_types.push(chunk_type);
            offset += 4 + chunk_len;
        }
        chunk_types
    }

    #[tokio::test]
    async fn write_plan_passthrough_keeps_plaintext() {
        let plaintext = b"write-plan-plain".to_vec();
        let reader = HashReader::from_stream(
            Cursor::new(plaintext.clone()),
            plaintext.len() as i64,
            plaintext.len() as i64,
            None,
            None,
            false,
        )
        .expect("create hash reader");

        let mut reader = WritePlan::new()
            .apply(reader, plaintext.len() as i64)
            .expect("apply passthrough plan");

        let mut actual = Vec::new();
        reader.read_to_end(&mut actual).await.expect("read passthrough stream");

        assert_eq!(actual, plaintext);
    }

    #[tokio::test]
    async fn write_plan_compress_then_encrypt_multipart_roundtrip() {
        let plaintext = b"abcdefghijklmnopqrstuvwxyz".repeat(128);
        let actual_size = plaintext.len() as i64;
        let key_bytes = [0x5Au8; 32];
        let base_nonce = [0xA5u8; 12];
        let part_number = 7;

        let reader = HashReader::from_stream(Cursor::new(plaintext.clone()), actual_size, actual_size, None, None, false)
            .expect("create hash reader");

        let mut transformed = WritePlan::new()
            .with_compression(CompressionAlgorithm::default())
            .with_encryption(WriteEncryption::multipart(key_bytes, base_nonce, part_number))
            .apply(reader, actual_size)
            .expect("apply transform plan");

        let mut ciphertext = Vec::new();
        transformed
            .read_to_end(&mut ciphertext)
            .await
            .expect("read transformed ciphertext");

        let decrypt_reader = DecryptReader::new_multipart(Cursor::new(ciphertext), key_bytes, base_nonce, vec![part_number]);
        let mut decompressed = DecompressReader::new(Box::new(decrypt_reader), CompressionAlgorithm::default());

        let mut actual = Vec::new();
        decompressed
            .read_to_end(&mut actual)
            .await
            .expect("decrypt and decompress transformed stream");

        assert_eq!(actual, plaintext);
    }

    #[cfg(feature = "rio-v2")]
    #[tokio::test]
    async fn write_plan_supports_singlepart_object_key_encryption_roundtrip() {
        let plaintext = b"singlepart-object-key".repeat(512);
        let actual_size = plaintext.len() as i64;
        let object_key = [0x7Cu8; 32];

        let reader = HashReader::from_stream(Cursor::new(plaintext.clone()), actual_size, actual_size, None, None, false)
            .expect("create hash reader");

        let mut transformed = WritePlan::new()
            .with_encryption(WriteEncryption::singlepart_object_key(object_key))
            .apply(reader, actual_size)
            .expect("apply singlepart object-key plan");

        let mut encrypted = Vec::new();
        transformed
            .read_to_end(&mut encrypted)
            .await
            .expect("read encrypted object-key stream");

        let mut decrypted = DecryptReader::new_with_object_key(Cursor::new(encrypted), object_key);
        let mut actual = Vec::new();
        decrypted.read_to_end(&mut actual).await.expect("decrypt object-key stream");

        assert_eq!(actual, plaintext);
    }

    #[cfg(feature = "rio-v2")]
    #[tokio::test]
    async fn write_plan_supports_multipart_object_key_encryption_roundtrip() {
        let plaintext = b"multipart-object-key-".repeat(4096);
        let actual_size = plaintext.len() as i64;
        let object_key = [0x2Du8; 32];
        let part_number = 3u32;

        let reader = HashReader::from_stream(Cursor::new(plaintext.clone()), actual_size, actual_size, None, None, false)
            .expect("create hash reader");

        let mut transformed = WritePlan::new()
            .with_encryption(WriteEncryption::multipart_object_key(object_key, part_number))
            .apply(reader, actual_size)
            .expect("apply multipart object-key encryption");

        let mut ciphertext = Vec::new();
        transformed
            .read_to_end(&mut ciphertext)
            .await
            .expect("read multipart object-key ciphertext");

        let mut actual = Vec::new();
        DecryptReader::new_multipart_with_object_key(Cursor::new(ciphertext), object_key, vec![part_number as usize])
            .read_to_end(&mut actual)
            .await
            .expect("decrypt multipart object-key ciphertext");

        assert_eq!(actual, plaintext);
    }

    #[cfg(feature = "rio-v2")]
    #[tokio::test]
    async fn write_plan_rio_v2_compression_emits_s2_stream_and_seekable_index() {
        let plaintext = b"rustfs-rio-v2-s2-".repeat(600_000);
        let actual_size = plaintext.len() as i64;
        let reader = HashReader::from_stream(Cursor::new(plaintext.clone()), actual_size, actual_size, None, None, false)
            .expect("create hash reader");

        let mut transformed = WritePlan::new()
            .with_compression(CompressionAlgorithm::default())
            .apply(reader, actual_size)
            .expect("apply compression plan");

        let mut compressed = Vec::new();
        transformed
            .read_to_end(&mut compressed)
            .await
            .expect("read compressed stream");

        assert!(
            compressed.starts_with(b"\xff\x06\x00\x00S2sTwO"),
            "rio_v2 compressed stream must start with the S2 stream identifier"
        );

        let index = transformed
            .try_get_index()
            .cloned()
            .expect("rio_v2 compressed stream should expose a compression index");
        let (compressed_offset, uncompressed_offset) = index.find(2 * 1024 * 1024).expect("seek into compression index");

        assert!(compressed_offset > 0, "expected a non-zero compressed offset for the second block");
        assert!(uncompressed_offset > 0, "expected a non-zero uncompressed offset for the second block");

        let mut decompressed = DecompressReader::new(Cursor::new(compressed), CompressionAlgorithm::default());
        let mut actual = Vec::new();
        decompressed.read_to_end(&mut actual).await.expect("decompress rio_v2 stream");

        assert_eq!(actual, plaintext);
    }

    #[cfg(feature = "rio-v2")]
    #[tokio::test]
    async fn write_plan_rio_v2_small_compression_skips_index_below_minio_threshold() {
        let plaintext = b"rustfs-rio-v2-s2-".repeat(32_768);
        let actual_size = plaintext.len() as i64;
        let reader = HashReader::from_stream(Cursor::new(plaintext.clone()), actual_size, actual_size, None, None, false)
            .expect("create hash reader");

        let mut transformed = WritePlan::new()
            .with_compression(CompressionAlgorithm::default())
            .apply(reader, actual_size)
            .expect("apply compression plan");

        let mut compressed = Vec::new();
        transformed
            .read_to_end(&mut compressed)
            .await
            .expect("read compressed stream");

        assert!(
            transformed.try_get_index().is_none(),
            "rio_v2 should match MinIO and skip compression indexes for small objects"
        );

        let mut decompressed = DecompressReader::new(Cursor::new(compressed), CompressionAlgorithm::default());
        let mut actual = Vec::new();
        decompressed.read_to_end(&mut actual).await.expect("decompress rio_v2 stream");

        assert_eq!(actual, plaintext);
    }

    #[cfg(feature = "rio-v2")]
    #[tokio::test]
    async fn rio_v2_singlepart_encrypt_decrypt_roundtrip_preserves_small_compressed_stream() {
        let plaintext = b"abcdefghijklmnopqrstuvwxyz".to_vec();
        let key_bytes = [0x33u8; 32];
        let base_nonce = [0x55u8; 12];

        let mut compressed = Vec::new();
        CompressReader::new(Cursor::new(plaintext.clone()), CompressionAlgorithm::default())
            .read_to_end(&mut compressed)
            .await
            .expect("compress plaintext");

        let mut encrypted = Vec::new();
        EncryptReader::new(Cursor::new(compressed), key_bytes, base_nonce)
            .read_to_end(&mut encrypted)
            .await
            .expect("encrypt compressed stream");

        let decrypt_reader = DecryptReader::new(Cursor::new(encrypted), key_bytes, base_nonce);
        let mut decompressed = DecompressReader::new(Box::new(decrypt_reader), CompressionAlgorithm::default());
        let mut actual = Vec::new();
        decompressed
            .read_to_end(&mut actual)
            .await
            .expect("decrypt and decompress small stream");

        assert_eq!(actual, plaintext);
    }

    #[cfg(feature = "rio-v2")]
    #[tokio::test]
    async fn rio_v2_compress_then_encrypt_adds_s2_padding_frames() {
        let plaintext = b"padding-check-".repeat(4097);
        let actual_size = plaintext.len() as i64;
        let key_bytes = [0x1Bu8; 32];
        let base_nonce = [0xC4u8; 12];

        let reader = HashReader::from_stream(Cursor::new(plaintext.clone()), actual_size, actual_size, None, None, false)
            .expect("create hash reader");

        let mut transformed = WritePlan::new()
            .with_compression(CompressionAlgorithm::default())
            .with_encryption(WriteEncryption::singlepart(key_bytes, base_nonce))
            .apply(reader, actual_size)
            .expect("apply transform plan");

        let mut ciphertext = Vec::new();
        transformed
            .read_to_end(&mut ciphertext)
            .await
            .expect("read transformed ciphertext");

        let mut decrypted_compressed = Vec::new();
        DecryptReader::new(Cursor::new(ciphertext), key_bytes, base_nonce)
            .read_to_end(&mut decrypted_compressed)
            .await
            .expect("decrypt compressed stream");

        assert_eq!(decrypted_compressed.len() % ENCRYPTED_S2_PADDING_MULTIPLE, 0);

        let chunk_types = s2_chunk_types(&decrypted_compressed);
        assert!(
            chunk_types.contains(&0xfe),
            "rio_v2 compressed+encrypted streams must include S2 padding frames before encryption"
        );

        let mut actual = Vec::new();
        DecompressReader::new(Cursor::new(decrypted_compressed), CompressionAlgorithm::default())
            .read_to_end(&mut actual)
            .await
            .expect("decompress padded stream");

        assert_eq!(actual, plaintext);
    }

    #[cfg(feature = "rio-v2")]
    #[tokio::test]
    async fn rio_v2_decompress_reader_returns_bytes_on_first_read() {
        let plaintext = b"abcdefghijklmnopqrstuvwxyz".to_vec();

        let mut compressed = Vec::new();
        CompressReader::new(Cursor::new(plaintext.clone()), CompressionAlgorithm::default())
            .read_to_end(&mut compressed)
            .await
            .expect("compress plaintext");

        let mut decompressor = DecompressReader::new(Cursor::new(compressed), CompressionAlgorithm::default());
        let mut buf = [0u8; 64];
        let n = decompressor.read(&mut buf).await.expect("read first decompressed chunk");

        assert!(n > 0);
        assert_eq!(&buf[..n], plaintext.as_slice());
    }

    #[cfg(feature = "rio-v2")]
    #[tokio::test]
    async fn rio_v2_decompress_reader_returns_bytes_on_first_large_read() {
        let plaintext = b"abcdefghijklmnopqrstuvwxyz".to_vec();

        let mut compressed = Vec::new();
        CompressReader::new(Cursor::new(plaintext.clone()), CompressionAlgorithm::default())
            .read_to_end(&mut compressed)
            .await
            .expect("compress plaintext");

        let mut decompressor = DecompressReader::new(Cursor::new(compressed), CompressionAlgorithm::default());
        let mut buf = [0u8; 8192];
        let n = decompressor.read(&mut buf).await.expect("read first decompressed chunk");

        assert!(n > 0);
        assert_eq!(&buf[..n], plaintext.as_slice());
    }
}
