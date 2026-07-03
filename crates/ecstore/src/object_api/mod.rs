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
    ReplicateDecision, ReplicationState, ReplicationStatusType, VersionPurgeStatusType, replication_statuses_map,
    version_purge_statuses_map,
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

mod readers;
mod types;

pub use readers::*;
pub use types::*;
