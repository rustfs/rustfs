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

//! The stream formats of a default build: RustFS's own framing, written and read.

pub use rustfs_rio::*;

use super::{ReadCompressionBackend, ReadEncryptionBackend};
use bytes::Bytes;
use rustfs_utils::CompressionAlgorithm;
use std::str::FromStr;
use tokio::io::AsyncRead;

/// Which rio implementation this build compiled in. Only the feature-seam
/// guard test in lib.rs reads it, so the lib target sees it as unused
/// (backlog#1823).
#[allow(
    dead_code,
    reason = "asserted by the rio backend feature-seam test in lib.rs (backlog#1823)"
)]
pub const fn backend_name() -> &'static str {
    "legacy-rio"
}

pub fn compression_metadata_value(algorithm: CompressionAlgorithm) -> String {
    algorithm.to_string()
}

pub fn compression_scheme_to_read_plan(scheme: &str) -> std::io::Result<(CompressionAlgorithm, ReadCompressionBackend)> {
    Ok((CompressionAlgorithm::from_str(scheme)?, ReadCompressionBackend::Legacy))
}

pub fn compression_index_storage_bytes(index: &Index) -> Bytes {
    index.clone().into_vec()
}

/// Decodes one complete stored compression index for Range GET planning.
pub fn decode_compression_index_bytes(bytes: &Bytes) -> Option<Index> {
    // Metadata contains exactly one index, so reject partial parses.
    let mut decoded = Index::new();
    decoded
        .load(bytes.as_ref())
        .is_ok_and(|remaining| remaining.is_empty())
        .then_some(decoded)
}

pub fn compression_reader<R>(reader: R, algorithm: CompressionAlgorithm, _encrypted: bool) -> CompressReader<R>
where
    R: AsyncRead + Unpin + Send + Sync,
{
    CompressReader::new(reader, algorithm)
}

pub fn decompression_reader<R>(
    reader: R,
    algorithm: CompressionAlgorithm,
    _backend: ReadCompressionBackend,
) -> Box<dyn AsyncRead + Unpin + Send + Sync>
where
    R: AsyncRead + Unpin + Send + Sync + 'static,
{
    Box::new(DecompressReader::new(reader, algorithm))
}

pub fn decrypt_reader<R>(
    reader: R,
    key: [u8; 32],
    base_nonce: [u8; 12],
    _backend: ReadEncryptionBackend,
    sequence_number: u32,
) -> Box<dyn AsyncRead + Unpin + Send + Sync>
where
    R: AsyncRead + Unpin + Send + Sync + 'static,
{
    if sequence_number > 0 {
        // Single-part v2 frame seek: the plan positioned the storage read
        // at frame `sequence_number`; nonce and AAD bind absolute indices.
        Box::new(DecryptReader::new_at_block(reader, key, base_nonce, sequence_number as usize))
    } else {
        Box::new(DecryptReader::new(reader, key, base_nonce))
    }
}

pub fn decrypt_reader_with_object_key<R>(
    reader: R,
    object_key: [u8; 32],
    _sequence_number: u32,
) -> Box<dyn AsyncRead + Unpin + Send + Sync>
where
    R: AsyncRead + Unpin + Send + Sync + 'static,
{
    Box::new(DecryptReader::new(reader, object_key, [0u8; 12]))
}

pub fn decrypt_multipart_reader<R>(
    reader: R,
    key: [u8; 32],
    base_nonce: [u8; 12],
    multipart_parts: Vec<usize>,
    _backend: ReadEncryptionBackend,
    _sequence_number: u32,
) -> Box<dyn AsyncRead + Unpin + Send + Sync>
where
    R: AsyncRead + Unpin + Send + Sync + 'static,
{
    Box::new(DecryptReader::new_multipart(reader, key, base_nonce, multipart_parts))
}

pub fn decrypt_multipart_reader_with_object_key<R>(
    reader: R,
    object_key: [u8; 32],
    multipart_parts: Vec<usize>,
    _sequence_number: u32,
) -> Box<dyn AsyncRead + Unpin + Send + Sync>
where
    R: AsyncRead + Unpin + Send + Sync + 'static,
{
    Box::new(DecryptReader::new_multipart(reader, object_key, [0u8; 12], multipart_parts))
}

/// Write-side switch for the authenticated, fixed-frame legacy v2 layout.
///
/// Off by default for rolling-upgrade safety: nodes without v2 read support
/// cannot decrypt v2 frames, and encrypted ciphertext travels verbatim through
/// transition, decommission and SSE-C replication passthrough. Turn on only
/// after every node (and every RustFS warm/replication target that receives
/// raw ciphertext) runs a release with v2 read support. Reading v2 objects
/// needs no switch — the decrypt reader dispatches on the frame type byte.
pub(crate) const ENV_RUSTFS_ENCRYPTION_FRAME_V2: &str = "RUSTFS_ENCRYPTION_FRAME_V2";
pub(crate) const DEFAULT_RUSTFS_ENCRYPTION_FRAME_V2: bool = false;

pub(crate) fn encryption_frame_v2_enabled() -> bool {
    #[cfg(test)]
    {
        rustfs_utils::get_env_bool(ENV_RUSTFS_ENCRYPTION_FRAME_V2, DEFAULT_RUSTFS_ENCRYPTION_FRAME_V2)
    }
    #[cfg(not(test))]
    {
        static CACHED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        *CACHED.get_or_init(|| rustfs_utils::get_env_bool(ENV_RUSTFS_ENCRYPTION_FRAME_V2, DEFAULT_RUSTFS_ENCRYPTION_FRAME_V2))
    }
}

pub(super) fn encrypt_under_object_key<R>(reader: R, object_key: [u8; 32]) -> EncryptReader<R>
where
    R: AsyncRead + Unpin + Send + Sync,
{
    EncryptReader::new(reader, object_key, [0u8; 12])
}

pub(super) fn encrypt_singlepart<R>(reader: R, key: [u8; 32], base_nonce: [u8; 12]) -> EncryptReader<R>
where
    R: AsyncRead + Unpin + Send + Sync,
{
    if encryption_frame_v2_enabled() {
        EncryptReader::new_v2(reader, key, base_nonce)
    } else {
        EncryptReader::new(reader, key, base_nonce)
    }
}

pub(super) fn encrypt_part<R>(reader: R, key: [u8; 32], base_nonce: [u8; 12], part_number: usize) -> EncryptReader<R>
where
    R: AsyncRead + Unpin + Send + Sync,
{
    if encryption_frame_v2_enabled() {
        EncryptReader::new_multipart_v2(reader, key, base_nonce, part_number)
    } else {
        EncryptReader::new_multipart(reader, key, base_nonce, part_number)
    }
}

pub(super) fn encrypt_part_under_object_key<R>(reader: R, object_key: [u8; 32], part_number: u32) -> EncryptReader<R>
where
    R: AsyncRead + Unpin + Send + Sync,
{
    EncryptReader::new_multipart(reader, object_key, [0u8; 12], part_number as usize)
}
