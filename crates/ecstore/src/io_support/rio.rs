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

//! The stream layer the storage engine reads and writes objects through.
//!
//! A build compiles in exactly one variant of it. `native` and `m_compatible`
//! export the same functions, so everything outside this module calls them
//! without naming the build; the selection below is the only place that does.

#[cfg(feature = "m-compatible")]
mod m_compatible;
#[cfg(not(feature = "m-compatible"))]
mod native;

#[cfg(feature = "m-compatible")]
use m_compatible as variant;
#[cfg(not(feature = "m-compatible"))]
use native as variant;

pub use variant::*;

use rustfs_utils::CompressionAlgorithm;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadCompressionBackend {
    Legacy,
    V2,
}

/// Validate that replicated compression metadata can be read by this build.
///
/// # Errors
/// Returns an error for an unsupported compression scheme.
pub fn validate_compression_scheme(scheme: &str) -> std::io::Result<()> {
    compression_scheme_to_read_plan(scheme).map(|_| ())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadEncryptionBackend {
    Legacy,
    // Never constructed today — every read still selects Legacy — but the
    // decrypt paths of the variants carry live match arms for it. This is the
    // rio-v2 read seam (backlog#1638 / #1835), not dead code: deleting the
    // variant would delete those arms with it.
    #[allow(dead_code, reason = "rio-v2 read seam; match arms in the variants are live (backlog#1823)")]
    V2,
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
        // Transformations create new HashReaders around the plaintext reader. Keep
        // the request checksum metadata on the final reader for multipart/single
        // PUT persistence, but leave verification to the plaintext reader.
        let checksum = reader.content_hash().clone();
        let trailer = reader.get_trailer().cloned();

        let delegate_inner_etag = self.compression.is_none() && reader.is_etag_reader();
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
            let etag_nonce = match encryption.mode {
                WriteEncryptionMode::Singlepart { base_nonce } => base_nonce,
                WriteEncryptionMode::SinglepartObjectKey => [0u8; 12],
                // Each part write needs an independent fingerprint, including a
                // retry at the same part number. This salt affects only the ETag
                // MAC; encryption framing and its data nonce stay unchanged.
                WriteEncryptionMode::MultipartLegacy { .. } | WriteEncryptionMode::MultipartObjectKey { .. } => rand::random(),
            };
            let protected_reader = rustfs_rio::EtagReader::new_encrypted(reader, encryption.key_bytes, etag_nonce)?
                .with_inner_etag_delegation(delegate_inner_etag);
            let key = encryption.key_bytes;
            let encrypt_reader = match encryption.mode {
                WriteEncryptionMode::SinglepartObjectKey => variant::encrypt_under_object_key(protected_reader, key),
                WriteEncryptionMode::Singlepart { base_nonce } => variant::encrypt_singlepart(protected_reader, key, base_nonce),
                WriteEncryptionMode::MultipartLegacy {
                    base_nonce,
                    multipart_part_number,
                } => variant::encrypt_part(protected_reader, key, base_nonce, multipart_part_number),
                WriteEncryptionMode::MultipartObjectKey { multipart_part_number } => {
                    variant::encrypt_part_under_object_key(protected_reader, key, multipart_part_number)
                }
            };
            reader = HashReader::from_reader(encrypt_reader, HashReader::SIZE_PRESERVE_LAYER, actual_size, None, None, false)?;
        }

        // `ignore_value` deliberately avoids a second hasher over compressed or
        // encrypted bytes. The inner reader still validates the plaintext request
        // checksum while this outer reader exposes the request checksum context.
        reader.encrypted_etag = encrypted;
        reader.add_non_trailing_checksum(checksum, true)?;
        reader.set_trailer(trailer);

        Ok(reader)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, HeaderValue};
    use rustfs_rio::{Checksum, ChecksumType};
    use rustfs_utils::CompressionAlgorithm;
    use std::io::Cursor;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn write_plan_encryption_persists_protected_etags_for_every_mode() {
        let data = b"Salary 2026: 85000 EUR";
        let size = i64::try_from(data.len()).expect("test data size");
        let mut control =
            HashReader::from_stream(Cursor::new(data), size, size, None, None, false).expect("create control reader");
        control.read_to_end(&mut Vec::new()).await.expect("read control plaintext");
        let md5 = control.try_resolve_etag().expect("control MD5");
        for encryption in [
            WriteEncryption::singlepart([1; 32], [1; 12]),
            WriteEncryption::singlepart_object_key([1; 32]),
            WriteEncryption::multipart([1; 32], [1; 12], 1),
            WriteEncryption::multipart_object_key([1; 32], 1),
        ] {
            let plaintext = HashReader::from_stream(Cursor::new(data), size, size, Some(md5.clone()), None, false)
                .expect("create checked reader");
            let mut reader = WritePlan::new()
                .with_encryption(encryption)
                .apply(plaintext, size)
                .expect("apply encryption");
            let mut ciphertext = Vec::new();
            reader.read_to_end(&mut ciphertext).await.expect("stream ciphertext");
            assert_ne!(ciphertext, data);
            let tag = reader.try_resolve_etag().expect("resolve persisted encrypted ETag");
            assert_ne!(tag, md5, "stored ETag must not reveal the plaintext MD5");
            assert_eq!(tag.len(), 32, "multipart ETags require a 16-byte hexadecimal part tag");
            assert_eq!(reader.try_resolve_etag().as_ref(), Some(&tag));
        }
    }

    #[tokio::test]
    async fn multipart_etags_are_unique_per_write_without_changing_data_encryption() {
        let plaintext = b"same confidential part";
        let size = i64::try_from(plaintext.len()).expect("test length");
        for object_key_mode in [false, true] {
            let mut tags = std::collections::HashSet::new();
            for part_number in [1, 2, 1] {
                let encryption = if object_key_mode {
                    WriteEncryption::multipart_object_key([7; 32], part_number)
                } else {
                    WriteEncryption::multipart([7; 32], [9; 12], part_number as usize)
                };
                let source = HashReader::from_stream(Cursor::new(plaintext), size, size, None, None, false).expect("plaintext");
                let mut reader = WritePlan::new()
                    .with_encryption(encryption)
                    .apply(source, size)
                    .expect("part plan");
                let mut ciphertext = Vec::new();
                reader.read_to_end(&mut ciphertext).await.expect("encrypt part");
                let tag = reader.try_resolve_etag().expect("part fingerprint");
                assert_eq!(reader.try_resolve_etag().as_ref(), Some(&tag), "reading a stored result stays stable");
                assert!(
                    tags.insert(tag),
                    "different part numbers and retries must not identify matching plaintext"
                );
                let stored = Cursor::new(ciphertext);
                let mut decrypt = if object_key_mode {
                    decrypt_multipart_reader_with_object_key(stored, [7; 32], vec![part_number as usize], 0)
                } else {
                    // The backend that reads what this build's writer produced:
                    // the build's own reader, which a default build has only one of.
                    decrypt_multipart_reader(stored, [7; 32], [9; 12], vec![part_number as usize], ReadEncryptionBackend::V2, 0)
                };
                let mut decoded = Vec::new();
                decrypt
                    .read_to_end(&mut decoded)
                    .await
                    .expect("existing data nonce still decrypts");
                assert_eq!(decoded, plaintext);
            }
        }
    }

    async fn assert_non_trailing_checksum_survives(plan: WritePlan) {
        let plaintext = b"checksum-context-through-write-plan".repeat(256);
        let actual_size = plaintext.len() as i64;
        let checksum = Checksum::new_from_data(ChecksumType::CRC32, &plaintext).expect("create CRC32 checksum");
        let mut reader = HashReader::from_stream(Cursor::new(plaintext), actual_size, actual_size, None, None, false)
            .expect("create hash reader");
        reader
            .add_non_trailing_checksum(Some(checksum.clone()), false)
            .expect("attach plaintext checksum");

        let mut transformed = plan.apply(reader, actual_size).expect("apply write plan");
        assert_eq!(transformed.content_crc_type(), Some(ChecksumType::CRC32));

        let mut transformed_bytes = Vec::new();
        transformed
            .read_to_end(&mut transformed_bytes)
            .await
            .expect("stream transformed data without rehashing ciphertext");

        assert!(!transformed_bytes.is_empty());
        assert_eq!(transformed.content_crc().get("CRC32"), Some(&checksum.encoded));
    }

    #[tokio::test]
    async fn write_plan_preserves_non_trailing_checksum_context_across_transforms() {
        assert_non_trailing_checksum_survives(WritePlan::new().with_compression(CompressionAlgorithm::default())).await;
        assert_non_trailing_checksum_survives(
            WritePlan::new().with_encryption(WriteEncryption::singlepart([0x5Au8; 32], [0xA5u8; 12])),
        )
        .await;
        assert_non_trailing_checksum_survives(
            WritePlan::new()
                .with_compression(CompressionAlgorithm::default())
                .with_encryption(WriteEncryption::singlepart([0x5Au8; 32], [0xA5u8; 12])),
        )
        .await;
    }

    #[tokio::test]
    async fn write_plan_preserves_trailing_checksum_type_across_transforms() {
        let plaintext = b"trailing-checksum-context".to_vec();
        let actual_size = plaintext.len() as i64;
        let mut reader = HashReader::from_stream(Cursor::new(plaintext), actual_size, actual_size, None, None, false)
            .expect("create hash reader");
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-trailer", HeaderValue::from_static("x-amz-checksum-crc32"));
        reader
            .add_checksum(&headers, None, false)
            .expect("attach trailing checksum metadata");

        let transformed = WritePlan::new()
            .with_encryption(WriteEncryption::singlepart([0x5Au8; 32], [0xA5u8; 12]))
            .apply(reader, actual_size)
            .expect("apply encryption plan");

        assert_eq!(
            transformed.content_crc_type(),
            Some(ChecksumType(ChecksumType::CRC32.0 | ChecksumType::TRAILING.0))
        );
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
}
