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

use crate::bucket::metadata_sys::get_versioning_config;
use crate::bucket::versioning::VersioningApi as _;
use crate::config::storageclass;
use crate::disk::DiskStore;
use crate::error::{Error, Result};
use crate::store_utils::clean_metadata;
use crate::{
    bucket::lifecycle::bucket_lifecycle_audit::LcAuditEvent,
    bucket::lifecycle::lifecycle::ExpirationOptions,
    bucket::lifecycle::{bucket_lifecycle_ops::TransitionedObject, lifecycle::TransitionOptions},
};
use bytes::Bytes;
use http::{HeaderMap, HeaderValue};
use rustfs_common::heal_channel::HealOpts;
use rustfs_filemeta::{
    FileInfo, MetaCacheEntriesSorted, ObjectPartInfo, REPLICATION_RESET, REPLICATION_STATUS, ReplicateDecision, ReplicationState,
    ReplicationStatusType, RestoreStatusOps as _, VersionPurgeStatusType, parse_restore_obj_status, replication_statuses_map,
    version_purge_statuses_map,
};
use rustfs_lock::NamespaceLockWrapper;
use rustfs_madmin::heal_commands::HealResultItem;
use rustfs_rio::Checksum;
use rustfs_rio::{DecompressReader, HashReader, LimitReader, WarpReader};
use rustfs_utils::CompressionAlgorithm;
use rustfs_utils::http::headers::{AMZ_OBJECT_TAGGING, RESERVED_METADATA_PREFIX_LOWER};
use rustfs_utils::http::{AMZ_BUCKET_REPLICATION_STATUS, AMZ_RESTORE, AMZ_STORAGE_CLASS};
use rustfs_utils::path::decode_dir_object;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::pin::Pin;
use std::str::FromStr as _;
use std::sync::Arc;
use std::task::{Context, Poll};
use time::OffsetDateTime;
use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};
use tokio_util::sync::CancellationToken;
use tracing::warn;
use uuid::Uuid;

pub const ERASURE_ALGORITHM: &str = "rs-vandermonde";
pub const BLOCK_SIZE_V2: usize = 1024 * 1024; // 1M

mod readers;
mod traits;
mod types;

pub use readers::*;
pub use traits::*;
pub use types::*;
