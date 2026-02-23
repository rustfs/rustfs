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

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Erasure Coding information for an object.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErasureInfo {
    pub algorithm: String,
    pub data_blocks: usize,
    pub parity_blocks: usize,
    pub block_size: usize,
    pub index: usize,
    pub distribution: Vec<usize>,
    pub checksums: Vec<Option<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkInfo {
    pub hash: String,
    pub size: u64,
    pub offset: u64,
}

/// Unified Metadata Structure stored in SurrealKV.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectMetadata {
    pub bucket: String,
    pub key: String,
    pub version_id: String,

    /// Content Addressable Storage (CAS) Hash (SHA256/BLAKE3).
    /// Used for deduplication and data integrity.
    /// For chunked objects, this is the hash of the entire content.
    pub content_hash: String,

    pub size: u64,
    pub created_at: i64,
    pub mod_time: i64,

    /// User-defined metadata (x-amz-meta-*)
    pub user_metadata: HashMap<String, String>,

    /// System metadata (Content-Type, etc.)
    pub system_metadata: HashMap<String, String>,

    /// Erasure Coding info (if applicable at this layer)
    pub erasure_info: Option<ErasureInfo>,

    /// Inline Data flag. If true, data is stored directly in KV value or adjacent key.
    pub is_inline: bool,

    /// Inline data content (if size < 128KB).
    /// Option: Could be stored in a separate KV key to keep metadata small.
    // #[serde(skip)]
    pub inline_data: Option<Bytes>,

    /// Chunks info for large objects
    pub chunks: Option<Vec<ChunkInfo>>,
}

impl Default for ObjectMetadata {
    fn default() -> Self {
        Self {
            bucket: String::new(),
            key: String::new(),
            version_id: String::new(),
            content_hash: String::new(),
            size: 0,
            created_at: 0,
            mod_time: 0,
            user_metadata: HashMap::new(),
            system_metadata: HashMap::new(),
            erasure_info: None,
            is_inline: false,
            inline_data: None,
            chunks: None,
        }
    }
}

/// Minimal metadata stored in Ferntree index for fast listing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexMetadata {
    pub size: u64,
    pub mod_time: i64,
    pub etag: String,
}
