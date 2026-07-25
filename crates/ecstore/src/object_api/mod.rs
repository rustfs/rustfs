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

// #730: object API readers keep staged compatibility paths during facade migration.
#![allow(dead_code)]

use crate::bucket::metadata_sys::get_versioning_config;
use crate::bucket::replication::{
    ReplicateDecision, ReplicationState, ReplicationStatusType, VersionPurgeStatusType, replication_status_from_filemeta,
    replication_statuses_map, version_purge_status_from_filemeta, version_purge_statuses_map,
};
use crate::bucket::versioning::VersioningApi as _;
use crate::config::storageclass;
use crate::error::{Error, Result};
use crate::io_support::rio::{HashReader, LimitReader};
use crate::storage_api_contracts::{
    lifecycle::{ExpirationOptions, TransitionedObject},
    range::HTTPRangeSpec,
};
use crate::store::utils::clean_metadata;
use crate::{bucket::lifecycle::bucket_lifecycle_audit::LcAuditEvent, bucket::lifecycle::lifecycle::TransitionOptions};
use bytes::Bytes;
use http::{HeaderMap, HeaderValue};
use rustfs_filemeta::{FileInfo, MetaCacheEntriesSorted, ObjectPartInfo, RestoreStatusOps as _, parse_restore_obj_status};
use rustfs_rio::Checksum;
use rustfs_utils::CompressionAlgorithm;
use rustfs_utils::http::headers::AMZ_OBJECT_TAGGING;
use rustfs_utils::http::{AMZ_BUCKET_REPLICATION_STATUS, AMZ_RESTORE, AMZ_STORAGE_CLASS};
use rustfs_utils::path::decode_dir_object;
use std::collections::HashMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use time::OffsetDateTime;
use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};
use tracing::warn;
use uuid::Uuid;

pub const ERASURE_ALGORITHM: &str = "rs-vandermonde";
pub const BLOCK_SIZE_V2: usize = 1024 * 1024; // 1M
pub(crate) const ENCRYPTED_PART_LAYOUT_CANDIDATE_SUFFIX: &str = "encrypted-part-layout-quorum-candidate-v1";
pub(crate) const ENCRYPTED_PART_LAYOUT_QUORUM_SUFFIX: &str = "encrypted-part-layout-quorum-v1";
pub(crate) const ENV_RUSTFS_ENCRYPTED_RANGE_SEEK: &str = "RUSTFS_ENCRYPTED_RANGE_SEEK";
pub(crate) const DEFAULT_RUSTFS_ENCRYPTED_RANGE_SEEK: bool = false;

pub(crate) fn has_encrypted_part_layout_marker(metadata: &HashMap<String, String>, suffix: &str, expected: &str) -> bool {
    let mut value = None;
    for (key, candidate) in metadata {
        if !rustfs_utils::http::has_internal_suffix(key, suffix) {
            continue;
        }
        if candidate.is_empty() || value.is_some_and(|current| current != candidate) {
            return false;
        }
        value = Some(candidate);
    }
    value.is_some_and(|value| value == expected)
}

pub(crate) fn legacy_encrypted_range_seek_enabled() -> bool {
    // RUSTFS_COMPAT_TODO(backlog-1316): mixed-version MPUs need opt-in. Remove after all servers use candidate markers and uploadId locks.
    #[cfg(test)]
    {
        rustfs_utils::get_env_bool(ENV_RUSTFS_ENCRYPTED_RANGE_SEEK, DEFAULT_RUSTFS_ENCRYPTED_RANGE_SEEK)
    }
    #[cfg(not(test))]
    {
        static CACHED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        *CACHED.get_or_init(|| rustfs_utils::get_env_bool(ENV_RUSTFS_ENCRYPTED_RANGE_SEEK, DEFAULT_RUSTFS_ENCRYPTED_RANGE_SEEK))
    }
}

mod body_cache_hook;
mod hook_slot;
mod object_mutation_hook;
mod readers;
mod types;

#[cfg(test)]
pub(crate) use body_cache_hook::clear_get_object_body_cache_hook;
pub use body_cache_hook::{
    GetObjectBodyCacheHook, GetObjectBodyCacheHookLookup, get_object_body_cache_plaintext_len, lookup_get_object_body_cache_hook,
    register_get_object_body_cache_hook, unregister_get_object_body_cache_hook,
};
pub(crate) use body_cache_hook::{
    get_object_body_cache_hook, get_object_body_cache_hook_suppressed, without_get_object_body_cache_hook,
};
pub(crate) use object_mutation_hook::notify_object_mutation;
pub use object_mutation_hook::{ObjectMutationHook, register_object_mutation_hook, unregister_object_mutation_hook};
pub use readers::*;
pub use types::*;
