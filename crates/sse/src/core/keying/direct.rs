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

//! Direct keying: the data key encrypts the object itself, under a stored base nonce.

use crate::core::s3::ssec::md5_bytes;
use base64_simd::STANDARD as BASE64_STANDARD;
use rustfs_utils::http::object_encryption_keys::{INTERNAL_ENCRYPTION_IV_HEADER, MINIO_INTERNAL_ENCRYPTION_IV_HEADER};
use std::collections::HashMap;

/// Resolve the SSE-C Direct base nonce for decryption.
///
/// New objects persist a random nonce at encryption time (see
/// `encryption_material_to_metadata`) under `INTERNAL_ENCRYPTION_IV_HEADER` (preferred) or,
/// for MinIO interop, `MINIO_INTERNAL_ENCRYPTION_IV_HEADER`. Legacy objects written before
/// random nonces were persisted carry no stored IV; for those we fall back to the
/// deterministic `generate_ssec_nonce(bucket, key)` value they were originally encrypted
/// with, so previously stored objects still decrypt correctly.
pub(crate) fn read_stored_ssec_nonce(metadata: &HashMap<String, String>, bucket: &str, key: &str) -> [u8; 12] {
    metadata
        .get(INTERNAL_ENCRYPTION_IV_HEADER)
        .or_else(|| metadata.get(MINIO_INTERNAL_ENCRYPTION_IV_HEADER))
        .and_then(|encoded| BASE64_STANDARD.decode_to_vec(encoded).ok())
        .and_then(|bytes| <[u8; 12]>::try_from(bytes.as_slice()).ok())
        .unwrap_or_else(|| generate_ssec_nonce(bucket, key))
}

/// Generate deterministic nonce for SSE-C encryption
///
/// The nonce is derived from the bucket and key to ensure:
/// 1. Same object always gets the same nonce (required for SSE-C)
/// 2. Different objects get different nonces
pub(crate) fn generate_ssec_nonce(bucket: &str, key: &str) -> [u8; 12] {
    let nonce_source = format!("{bucket}-{key}");
    let nonce_hash = md5_bytes(nonce_source.as_bytes());
    let mut nonce = [0u8; 12];
    nonce.copy_from_slice(&nonce_hash[..12]);
    nonce
}
