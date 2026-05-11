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

use rustfs_utils::CompressionAlgorithm;

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

#[derive(Debug, Clone, Copy)]
pub struct WriteEncryption {
    key_bytes: [u8; 32],
    base_nonce: [u8; 12],
    multipart_part_number: Option<usize>,
}

impl WriteEncryption {
    pub const fn singlepart(key_bytes: [u8; 32], base_nonce: [u8; 12]) -> Self {
        Self {
            key_bytes,
            base_nonce,
            multipart_part_number: None,
        }
    }

    pub const fn multipart(key_bytes: [u8; 32], base_nonce: [u8; 12], multipart_part_number: usize) -> Self {
        Self {
            key_bytes,
            base_nonce,
            multipart_part_number: Some(multipart_part_number),
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
        if let Some(algorithm) = self.compression {
            reader = HashReader::from_reader(
                CompressReader::new(reader, algorithm),
                HashReader::SIZE_PRESERVE_LAYER,
                actual_size,
                None,
                None,
                false,
            )?;
        }

        if let Some(encryption) = self.encryption {
            reader = match encryption.multipart_part_number {
                Some(part_number) => HashReader::from_reader(
                    EncryptReader::new_multipart(reader, encryption.key_bytes, encryption.base_nonce, part_number),
                    HashReader::SIZE_PRESERVE_LAYER,
                    actual_size,
                    None,
                    None,
                    false,
                )?,
                None => HashReader::from_reader(
                    EncryptReader::new(reader, encryption.key_bytes, encryption.base_nonce),
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
