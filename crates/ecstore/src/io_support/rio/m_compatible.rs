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

//! The stream formats of an `m-compatible` build: it writes the compatible
//! framing and reads both that and what a default build wrote.

pub use rustfs_rio_v2::*;

use super::{ReadCompressionBackend, ReadEncryptionBackend};
use bytes::Bytes;
use rustfs_utils::CompressionAlgorithm;
use std::str::FromStr;
use tokio::io::AsyncRead;

/// The compression scheme name the compatible format records.
const S2_COMPRESSION_SCHEME: &str = "klauspost/compress/s2";
// The S2 padding multiple rio-v2 pads compressed streams to before
// encryption. Only the padding test asserts it today, so the lib target sees
// it as unused (backlog#1823).
#[allow(dead_code, reason = "on-disk contract asserted by the rio-v2 padding test (backlog#1823)")]
pub(super) const ENCRYPTED_S2_PADDING_MULTIPLE: usize = 256;

/// Which rio implementation this build compiled in. Only the feature-seam
/// guard test in lib.rs reads it, so the lib target sees it as unused
/// (backlog#1823).
#[allow(
    dead_code,
    reason = "asserted by the rio backend feature-seam test in lib.rs (backlog#1823)"
)]
pub const fn backend_name() -> &'static str {
    "rio-v2"
}

pub fn compression_metadata_value(_algorithm: CompressionAlgorithm) -> String {
    S2_COMPRESSION_SCHEME.to_string()
}

pub fn compression_scheme_to_read_plan(scheme: &str) -> std::io::Result<(CompressionAlgorithm, ReadCompressionBackend)> {
    if scheme.eq_ignore_ascii_case(S2_COMPRESSION_SCHEME) {
        return Ok((CompressionAlgorithm::default(), ReadCompressionBackend::V2));
    }

    Ok((CompressionAlgorithm::from_str(scheme)?, ReadCompressionBackend::Legacy))
}

pub fn compression_index_storage_bytes(index: &Index) -> Bytes {
    minio_index_storage_bytes(index)
}

pub fn decode_compression_index_bytes(bytes: &Bytes) -> Option<Index> {
    if let Some(decoded) = decode_minio_index_bytes(bytes) {
        return Some(decoded);
    }

    let mut decoded = Index::new();
    if decoded.load(bytes.as_ref()).is_ok() {
        return Some(decoded);
    }

    let restored = restore_legacy_index_headers(bytes.as_ref());
    let mut decoded = Index::new();
    decoded.load(&restored).is_ok().then_some(decoded)
}

pub fn compression_reader<R>(reader: R, algorithm: CompressionAlgorithm, encrypted: bool) -> CompressReader<R>
where
    R: AsyncRead + Unpin + Send + Sync,
{
    if encrypted {
        return CompressReader::with_encrypted_padding(reader, algorithm);
    }

    CompressReader::new(reader, algorithm)
}

pub fn decompression_reader<R>(
    reader: R,
    algorithm: CompressionAlgorithm,
    backend: ReadCompressionBackend,
) -> Box<dyn AsyncRead + Unpin + Send + Sync>
where
    R: AsyncRead + Unpin + Send + Sync + 'static,
{
    match backend {
        ReadCompressionBackend::Legacy => Box::new(rustfs_rio::DecompressReader::new(reader, algorithm)),
        ReadCompressionBackend::V2 => Box::new(DecompressReader::new(reader, algorithm)),
    }
}

pub fn decrypt_reader<R>(
    reader: R,
    key: [u8; 32],
    base_nonce: [u8; 12],
    backend: ReadEncryptionBackend,
    sequence_number: u32,
) -> Box<dyn AsyncRead + Unpin + Send + Sync>
where
    R: AsyncRead + Unpin + Send + Sync + 'static,
{
    match backend {
        // The legacy family includes v2 fixed-frame objects, whose plans
        // seek by frame; honor the starting index here too so this build
        // can range-read objects a default-build node wrote.
        ReadEncryptionBackend::Legacy if sequence_number > 0 => {
            Box::new(rustfs_rio::DecryptReader::new_at_block(reader, key, base_nonce, sequence_number as usize))
        }
        ReadEncryptionBackend::Legacy => Box::new(rustfs_rio::DecryptReader::new(reader, key, base_nonce)),
        ReadEncryptionBackend::V2 => Box::new(DecryptReader::new_with_sequence(reader, key, base_nonce, sequence_number)),
    }
}

pub fn decrypt_reader_with_object_key<R>(
    reader: R,
    object_key: [u8; 32],
    sequence_number: u32,
) -> Box<dyn AsyncRead + Unpin + Send + Sync>
where
    R: AsyncRead + Unpin + Send + Sync + 'static,
{
    Box::new(DecryptReader::new_with_object_key_and_sequence(reader, object_key, sequence_number))
}

pub fn decrypt_multipart_reader<R>(
    reader: R,
    key: [u8; 32],
    base_nonce: [u8; 12],
    multipart_parts: Vec<usize>,
    backend: ReadEncryptionBackend,
    sequence_number: u32,
) -> Box<dyn AsyncRead + Unpin + Send + Sync>
where
    R: AsyncRead + Unpin + Send + Sync + 'static,
{
    match backend {
        ReadEncryptionBackend::Legacy => {
            Box::new(rustfs_rio::DecryptReader::new_multipart(reader, key, base_nonce, multipart_parts))
        }
        ReadEncryptionBackend::V2 => Box::new(DecryptReader::new_multipart_with_sequence(
            reader,
            key,
            base_nonce,
            multipart_parts,
            sequence_number,
        )),
    }
}

pub fn decrypt_multipart_reader_with_object_key<R>(
    reader: R,
    object_key: [u8; 32],
    multipart_parts: Vec<usize>,
    sequence_number: u32,
) -> Box<dyn AsyncRead + Unpin + Send + Sync>
where
    R: AsyncRead + Unpin + Send + Sync + 'static,
{
    Box::new(DecryptReader::new_multipart_with_object_key_and_sequence(
        reader,
        object_key,
        multipart_parts,
        sequence_number,
    ))
}

fn restore_legacy_index_headers(bytes: &[u8]) -> Vec<u8> {
    if bytes.is_empty() {
        return Vec::new();
    }

    const S2_INDEX_HEADER: &[u8] = b"s2idx\x00";
    const S2_INDEX_TRAILER: &[u8] = b"\x00xdi2s";
    let mut restored = Vec::with_capacity(4 + S2_INDEX_HEADER.len() + bytes.len() + 4 + S2_INDEX_TRAILER.len());
    restored.extend_from_slice(&[0x99, 0x2A, 0x4D, 0x18]);
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

/// The fixed-frame legacy layout is a default-build write format; this build
/// never writes it, so the switch that selects it is always off here.
pub(crate) const fn encryption_frame_v2_enabled() -> bool {
    false
}

pub(super) fn encrypt_under_object_key<R>(reader: R, object_key: [u8; 32]) -> EncryptReader<R>
where
    R: AsyncRead + Unpin + Send + Sync,
{
    EncryptReader::new_with_object_key(reader, object_key)
}

pub(super) fn encrypt_singlepart<R>(reader: R, key: [u8; 32], base_nonce: [u8; 12]) -> EncryptReader<R>
where
    R: AsyncRead + Unpin + Send + Sync,
{
    EncryptReader::new(reader, key, base_nonce)
}

pub(super) fn encrypt_part<R>(reader: R, key: [u8; 32], base_nonce: [u8; 12], part_number: usize) -> EncryptReader<R>
where
    R: AsyncRead + Unpin + Send + Sync,
{
    EncryptReader::new_multipart(reader, key, base_nonce, part_number)
}

pub(super) fn encrypt_part_under_object_key<R>(reader: R, object_key: [u8; 32], part_number: u32) -> EncryptReader<R>
where
    R: AsyncRead + Unpin + Send + Sync,
{
    EncryptReader::new_multipart_with_object_key(reader, object_key, part_number)
}

#[cfg(test)]
mod tests {
    use crate::io_support::rio::*;
    use rustfs_utils::CompressionAlgorithm;
    use std::io::Cursor;
    use tokio::io::AsyncReadExt;

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

    #[tokio::test]
    async fn write_plan_rio_v2_small_compression_skips_index_below_the_index_threshold() {
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
            "rio_v2 should skip compression indexes for small objects, as the compatible format does"
        );

        let mut decompressed = DecompressReader::new(Cursor::new(compressed), CompressionAlgorithm::default());
        let mut actual = Vec::new();
        decompressed.read_to_end(&mut actual).await.expect("decompress rio_v2 stream");

        assert_eq!(actual, plaintext);
    }

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
