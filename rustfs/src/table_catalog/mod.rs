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

//! Internal table catalog primitives for the Iceberg REST Catalog framework.
//!
//! This module intentionally does not expose HTTP handlers or mutate existing
//! S3 object behavior. It defines the stable internal boundary that later
//! catalog routes and object guards can share.

use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroUsize,
    ops::Bound,
    sync::Arc,
    time::{Duration as StdDuration, Instant},
};

use crate::storage_api::table::contract::http::HTTPPreconditions;
use crate::storage_api::table::contract::list::{
    ListObjectVersionsInfo as StorageListObjectVersionsInfo, ListObjectsV2Info as StorageListObjectsV2Info,
    ListOperations as StorageListOperations, ObjectInfoOrErr as StorageObjectInfoOrErr, WalkOptions as StorageWalkOptions,
};
use crate::storage_api::table::contract::namespace::NamespaceLocking as StorageNamespaceLocking;
use crate::storage_api::table::contract::object::{ObjectIO as StorageObjectIO, ObjectOperations as StorageObjectOperations};
use crate::storage_api::table::contract::range::HTTPRangeSpec;
use crate::storage_api::table::{
    BUCKET_TABLE_CATALOG_META_PREFIX, BUCKET_TABLE_CATALOG_TABLE_BUCKETS_PREFIX, BUCKET_TABLE_CONFIG,
    BUCKET_TABLE_RESERVED_PREFIX, Error as EcstoreError, RUSTFS_META_BUCKET, StorageError, get_bucket_metadata,
    get_lock_acquire_timeout, table_catalog_path_hash,
};
use bytes::Bytes;
use datafusion::{
    arrow::datatypes::SchemaRef,
    parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
};
use http::HeaderMap;
use metrics::{counter, histogram};
use rustfs_filemeta::FileInfo;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use time::{Duration, OffsetDateTime};
use tokio::io::AsyncReadExt;
use uuid::Uuid;

use crate::storage_api::table::{
    StorageDeletedObject as DeletedObject, StorageGetObjectReader as GetObjectReader, StorageObjectInfo as ObjectInfo,
    StorageObjectOptions as ObjectOptions, StorageObjectToDelete as ObjectToDelete, StoragePutObjReader as PutObjReader,
};

mod error;
mod iceberg;
mod identifier;
mod maintenance;
mod model;
mod store;

pub use error::{CatalogIdentifierError, TableObjectMutationError};
pub(crate) use error::{TableCatalogStoreError, TableCatalogStoreResult};
pub(crate) use iceberg::*;
pub use identifier::{IdentifierSegment, Namespace, is_reserved_table_object_key};
pub(crate) use identifier::{
    default_table_bucket_publication_lock_path, default_table_data_dir_path, default_table_delete_dir_path,
    default_table_metadata_dir_path, default_table_metadata_file_path, default_table_publication_lock_path,
    default_view_metadata_file_path, is_valid_table_metadata_location, is_valid_table_metadata_location_for_entry,
    is_valid_view_metadata_location, metadata_location_from_metadata_file_path, table_metadata_dir_path_for_entry,
    table_metadata_file_path_for_entry, validate_bucket_object_mutation,
};
pub(crate) use maintenance::*;
pub(crate) use model::*;
pub(crate) use store::*;

pub(crate) const TABLE_BUCKET_MARKER_CONFIG: &str = BUCKET_TABLE_CONFIG;
pub(crate) const RESERVED_CATALOG_OBJECT_MESSAGE: &str = "Object key is reserved for the table catalog";
pub(crate) const TABLE_BUCKET_CATALOG_TYPE: &str = "iceberg-rest";
pub(crate) const TABLE_BUCKET_CONFIG_VERSION: u16 = 1;
pub(crate) const DEFAULT_WAREHOUSE_ID: &str = "default";
#[allow(
    dead_code,
    reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
)]
pub(crate) const TABLE_NAMESPACE_MARKER_VERSION: u16 = 1;
#[allow(
    dead_code,
    reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
)]
pub(crate) const TABLE_RESOURCE_MARKER_VERSION: u16 = 1;
#[allow(
    dead_code,
    reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
)]
pub(crate) const TABLE_METADATA_POINTER_VERSION: u16 = 1;
pub(crate) const TABLE_CATALOG_ENTRY_VERSION: u16 = 1;
pub(crate) const TABLE_WAREHOUSE_INDEX_STATE_VERSION: u16 = 2;
pub(crate) const TABLE_MAINTENANCE_CONFIG_VERSION: u16 = 1;
pub(crate) const TABLE_EXTERNAL_CATALOG_BRIDGE_VERSION: u16 = 1;
pub(crate) const TABLE_CATALOG_BACKING_MANIFEST_VERSION: u16 = 1;
pub(crate) const ENV_TABLE_CATALOG_BACKING: &str = "RUSTFS_TABLE_CATALOG_BACKING";
pub(crate) const ENV_TABLE_CATALOG_PUBLICATION_FENCE_FLEET_CONFIRMED: &str =
    "RUSTFS_TABLE_CATALOG_PUBLICATION_FENCE_FLEET_CONFIRMED";
pub(crate) const ENV_TABLE_CATALOG_STRONG_SNAPSHOT_V2: &str = "RUSTFS_TABLE_CATALOG_STRONG_SNAPSHOT_V2";
pub(crate) const ENV_TABLE_CATALOG_STRONG_SNAPSHOT_V2_FLEET_CONFIRMED: &str =
    "RUSTFS_TABLE_CATALOG_STRONG_SNAPSHOT_V2_FLEET_CONFIRMED";
pub(crate) const TABLE_CATALOG_BACKING_OBJECT: &str = "object";
pub(crate) const TABLE_CATALOG_BACKING_DURABLE_STRONG: &str = "durable-strong";
pub(crate) const TABLE_METADATA_DIGEST_REQUIREMENT_TYPE: &str = "assert-rustfs-metadata-sha256";
pub(crate) const TABLE_METADATA_FILE_NAME_MAX_LEN: usize = 128;
pub(crate) const TABLE_METADATA_JSON_MAX_SIZE: usize = 50 * 1024 * 1024;
pub(crate) const TABLE_MANIFEST_AVRO_MAX_SIZE: usize = 128 * 1024 * 1024;
const TABLE_MANIFEST_AVRO_MAX_DECODED_SIZE: usize = 128 * 1024 * 1024;
const TABLE_MANIFEST_AVRO_MAX_RECORDS: usize = 1_000_000;
const TABLE_MANIFEST_AVRO_MAX_HEADER_ENTRIES: usize = 1_024;
const TABLE_COMMIT_MAX_MANIFESTS: usize = 10_000;
const TABLE_COMMIT_MAX_MANIFEST_TRAVERSALS: usize = 20_000;
const TABLE_COMMIT_MAX_AVRO_BYTES: usize = 512 * 1024 * 1024;
const TABLE_COMMIT_MAX_FILE_REFERENCES: usize = 1_000_000;
const TABLE_COMMIT_MAX_STATISTICS_OBJECTS: usize = 1_024;
const TABLE_COMMIT_MAX_STATISTICS_BYTES: usize = 512 * 1024 * 1024;
const TABLE_STATISTICS_FILE_MAX_SIZE: usize = 128 * 1024 * 1024;
pub(crate) const TABLE_COMMIT_OBJECT_VALIDATION_CONCURRENCY: usize = 16;
pub const TABLE_RESERVED_PREFIX: &str = BUCKET_TABLE_RESERVED_PREFIX;
const WAREHOUSE_ROOT: &str = "warehouses";
const NAMESPACE_ROOT: &str = "namespaces";
const TABLE_ROOT: &str = "tables";
const VIEW_ROOT: &str = "views";
#[allow(
    dead_code,
    reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
)]
const NAMESPACE_MARKER_FILE: &str = "namespace.json";
#[allow(
    dead_code,
    reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
)]
const TABLE_MARKER_FILE: &str = "table.json";
#[allow(
    dead_code,
    reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
)]
const CURRENT_POINTER_FILE: &str = "current.json";
#[allow(
    dead_code,
    reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
)]
const LIFECYCLE_FILE: &str = "lifecycle.json";
const METADATA_DIR: &str = "metadata";
const DATA_DIR: &str = "data";
const DELETE_DIR: &str = "delete";
const TABLE_BUCKET_ENTRY_FILE: &str = "table-bucket.json";
const NAMESPACE_ENTRY_FILE: &str = "namespace-entry.json";
const TABLE_ENTRY_FILE: &str = "table-entry.json";
const VIEW_ENTRY_FILE: &str = "view-entry.json";
const INTERNAL_CATALOG_ROOT: &str = BUCKET_TABLE_CATALOG_META_PREFIX;
const TABLE_BUCKET_ROOT: &str = BUCKET_TABLE_CATALOG_TABLE_BUCKETS_PREFIX;
const COMMIT_LOG_ROOT: &str = "commits";
const COMMIT_IDEMPOTENCY_ROOT: &str = "commit-idempotency";
const WAREHOUSE_INDEX_ROOT: &str = "warehouse-index";
const WAREHOUSE_INDEX_STATE_FILE: &str = "state.json";
const WAREHOUSE_INDEX_MAX_PREFIX_DEPTH: usize = 64;
const EXTERNAL_CATALOG_ROOT: &str = "external-catalog";
const EXTERNAL_CATALOG_BRIDGE_FILE: &str = "bridge.json";
const MAINTENANCE_ROOT: &str = "maintenance";
const MAINTENANCE_CONFIG_FILE: &str = "config.json";
const MAINTENANCE_JOB_ROOT: &str = "jobs";
const MAINTENANCE_LATEST_JOB_FILE: &str = "latest.json";
const MAINTENANCE_CURRENT_JOB_FILE: &str = "current.json";
const MAINTENANCE_JOB_ALIAS_LATEST: &str = "latest";
const MAINTENANCE_JOB_ALIAS_CURRENT: &str = "current";
const TABLE_CATALOG_LIST_MAX_KEYS: usize = 1000;
const OBJECT_CATALOG_LIST_CURSOR_PREFIX: &str = "object:";
const STRONG_CATALOG_LIST_CURSOR_PREFIX: &str = "strong:";
const TABLE_METADATA_CLEANUP_SAFETY_WINDOW_SECONDS: i64 = 15 * 60;
const TABLE_MAINTENANCE_RETRY_BACKOFF_MAX_SECONDS: u64 = 24 * 60 * 60;
const TABLE_MAINTENANCE_WORKER_LEASE_TIMEOUT_DEFAULT_SECONDS: u64 = 15 * 60;
const TABLE_MAINTENANCE_WORKER_LEASE_TIMEOUT_MAX_SECONDS: u64 = 24 * 60 * 60;
const TABLE_MAINTENANCE_SCHEDULER_AUDIT_LIMIT: usize = 10;
const TABLE_MAINTENANCE_DELETE_DISABLED_REASON: &str = "metadata delete is disabled by maintenance config";
const TABLE_COMMIT_SLOW_LOG_THRESHOLD: StdDuration = StdDuration::from_secs(2);
const ICEBERG_MAIN_REF: &str = "main";
const ICEBERG_MIN_SNAPSHOTS_TO_KEEP_PROPERTY: &str = "history.expire.min-snapshots-to-keep";
const ICEBERG_MAX_SNAPSHOT_AGE_MS_PROPERTY: &str = "history.expire.max-snapshot-age-ms";
const ICEBERG_MAX_REF_AGE_MS_PROPERTY: &str = "history.expire.max-ref-age-ms";
const ICEBERG_REF_MIN_SNAPSHOTS_TO_KEEP_FIELD: &str = "min-snapshots-to-keep";
const ICEBERG_REF_MAX_SNAPSHOT_AGE_MS_FIELD: &str = "max-snapshot-age-ms";
const ICEBERG_REF_MAX_REF_AGE_MS_FIELD: &str = "max-ref-age-ms";
const STRONG_TABLE_CATALOG_SNAPSHOT_MIN_READ_VERSION: u16 = 1;
const STRONG_TABLE_CATALOG_SNAPSHOT_VERSION: u16 = 2;
const STRONG_TABLE_CATALOG_BACKING_ROOT: &str = "strong-backing";
const STRONG_TABLE_CATALOG_SNAPSHOT_FILE: &str = "snapshot.json";
const TABLE_CATALOG_MIGRATION_MIN_READ_VERSION: u16 = 1;
const TABLE_CATALOG_MIGRATION_VERSION: u16 = 2;
const TABLE_CATALOG_MIGRATION_ROOT: &str = "backing-migration";
const TABLE_CATALOG_MIGRATION_FENCE_FILE: &str = "durable-strong-fence.json";
const TABLE_CATALOG_MIGRATION_FENCE_LOCK: &str = "durable-strong-fence.lock";
const TABLE_CATALOG_MIGRATION_GLOBAL_FENCE_FILE: &str = "durable-strong-global-fence.json";
const TABLE_CATALOG_MIGRATION_GLOBAL_FENCE_LOCK: &str = "durable-strong-global-fence.lock";

type CatalogListObjectsV2Info = StorageListObjectsV2Info<ObjectInfo>;
type CatalogListObjectVersionsInfo = StorageListObjectVersionsInfo<ObjectInfo>;
type CatalogObjectInfoOrErr = StorageObjectInfoOrErr<ObjectInfo, EcstoreError>;
type CatalogWalkOptions = StorageWalkOptions<fn(&FileInfo) -> bool>;

pub(crate) trait TableCatalogStorage:
    StorageObjectIO<
        Error = EcstoreError,
        RangeSpec = HTTPRangeSpec,
        HeaderMap = HeaderMap,
        ObjectOptions = ObjectOptions,
        ObjectInfo = ObjectInfo,
        GetObjectReader = GetObjectReader,
        PutObjectReader = PutObjReader,
    > + StorageObjectOperations<
        Error = EcstoreError,
        ObjectInfo = ObjectInfo,
        ObjectOptions = ObjectOptions,
        FileInfo = FileInfo,
        ObjectToDelete = ObjectToDelete,
        DeletedObject = DeletedObject,
    > + StorageListOperations<
        Error = EcstoreError,
        ListObjectsV2Info = CatalogListObjectsV2Info,
        ListObjectVersionsInfo = CatalogListObjectVersionsInfo,
        ObjectInfoOrErr = CatalogObjectInfoOrErr,
        WalkOptions = CatalogWalkOptions,
        WalkCancellation = tokio_util::sync::CancellationToken,
        WalkResultSender = tokio::sync::mpsc::Sender<CatalogObjectInfoOrErr>,
    > + StorageNamespaceLocking<Error = EcstoreError, NamespaceLock = rustfs_lock::NamespaceLockWrapper>
{
}

impl<T> TableCatalogStorage for T where
    T: StorageObjectIO<
            Error = EcstoreError,
            RangeSpec = HTTPRangeSpec,
            HeaderMap = HeaderMap,
            ObjectOptions = ObjectOptions,
            ObjectInfo = ObjectInfo,
            GetObjectReader = GetObjectReader,
            PutObjectReader = PutObjReader,
        > + StorageObjectOperations<
            Error = EcstoreError,
            ObjectInfo = ObjectInfo,
            ObjectOptions = ObjectOptions,
            FileInfo = FileInfo,
            ObjectToDelete = ObjectToDelete,
            DeletedObject = DeletedObject,
        > + StorageListOperations<
            Error = EcstoreError,
            ListObjectsV2Info = CatalogListObjectsV2Info,
            ListObjectVersionsInfo = CatalogListObjectVersionsInfo,
            ObjectInfoOrErr = CatalogObjectInfoOrErr,
            WalkOptions = CatalogWalkOptions,
            WalkCancellation = tokio_util::sync::CancellationToken,
            WalkResultSender = tokio::sync::mpsc::Sender<CatalogObjectInfoOrErr>,
        > + StorageNamespaceLocking<Error = EcstoreError, NamespaceLock = rustfs_lock::NamespaceLockWrapper>
{
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TableCatalogBackingMode {
    ObjectBacked,
    DurableStrong,
}

impl TableCatalogBackingMode {
    pub(crate) fn from_env() -> TableCatalogStoreResult<Self> {
        match std::env::var(ENV_TABLE_CATALOG_BACKING) {
            Ok(value) => Self::parse(&value),
            Err(std::env::VarError::NotPresent) => Ok(Self::ObjectBacked),
            Err(std::env::VarError::NotUnicode(_)) => Err(TableCatalogStoreError::Invalid(format!(
                "{ENV_TABLE_CATALOG_BACKING} must be valid UTF-8"
            ))),
        }
    }

    fn parse(value: &str) -> TableCatalogStoreResult<Self> {
        match value.trim() {
            "" | TABLE_CATALOG_BACKING_OBJECT => Ok(Self::ObjectBacked),
            TABLE_CATALOG_BACKING_DURABLE_STRONG => Ok(Self::DurableStrong),
            value => Err(TableCatalogStoreError::Invalid(format!(
                "unsupported table catalog backing {value}; expected {TABLE_CATALOG_BACKING_OBJECT} or {TABLE_CATALOG_BACKING_DURABLE_STRONG}"
            ))),
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::ObjectBacked => TABLE_CATALOG_BACKING_OBJECT,
            Self::DurableStrong => TABLE_CATALOG_BACKING_DURABLE_STRONG,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableCatalogListPage<T> {
    pub entries: Vec<T>,
    pub next_cursor: Option<String>,
}

fn finish_catalog_list_page<T, F>(
    mut entries: Vec<T>,
    limit: NonZeroUsize,
    cursor_prefix: &str,
    key: F,
) -> TableCatalogListPage<T>
where
    F: Fn(&T) -> &str,
{
    let next_cursor = if entries.len() > limit.get() {
        entries.truncate(limit.get());
        entries.last().map(|entry| format!("{cursor_prefix}{}", key(entry)))
    } else {
        None
    };
    TableCatalogListPage { entries, next_cursor }
}

pub(crate) fn catalog_list_page_from_entries<T, F>(
    mut entries: Vec<T>,
    cursor: Option<&str>,
    limit: NonZeroUsize,
    key: F,
) -> TableCatalogListPage<T>
where
    F: Fn(&T) -> &str,
{
    entries.sort_by(|left, right| key(left).cmp(key(right)));
    let start = cursor.map_or(0, |cursor| entries.partition_point(|entry| key(entry) <= cursor));
    let entries = entries.into_iter().skip(start).take(limit.get().saturating_add(1)).collect();
    finish_catalog_list_page(entries, limit, "", key)
}

fn catalog_list_cursor<'a>(cursor: Option<&'a str>, prefix: &str) -> TableCatalogStoreResult<Option<&'a str>> {
    cursor
        .map(|cursor| {
            cursor
                .strip_prefix(prefix)
                .filter(|cursor| !cursor.is_empty())
                .ok_or_else(|| {
                    TableCatalogStoreError::Invalid("page cursor does not match the active table catalog backing".to_string())
                })
        })
        .transpose()
}

fn parse_namespace_for_store(namespace: &str) -> TableCatalogStoreResult<Namespace> {
    Namespace::parse(namespace).map_err(|err| TableCatalogStoreError::Invalid(format!("invalid namespace: {err}")))
}

fn parse_table_for_store(table: &str) -> TableCatalogStoreResult<IdentifierSegment> {
    IdentifierSegment::parse(table).map_err(|err| TableCatalogStoreError::Invalid(format!("invalid table name: {err}")))
}

fn http_preconditions_for_catalog_put(precondition: TableCatalogPutPrecondition) -> Option<HTTPPreconditions> {
    match precondition {
        TableCatalogPutPrecondition::Any => None,
        TableCatalogPutPrecondition::IfAbsent => Some(HTTPPreconditions {
            if_none_match: Some("*".to_string()),
            ..Default::default()
        }),
        TableCatalogPutPrecondition::IfMatch(etag) => Some(HTTPPreconditions {
            if_match: Some(etag),
            ..Default::default()
        }),
    }
}

fn is_missing_storage_error(err: &StorageError) -> bool {
    matches!(
        err,
        StorageError::ObjectNotFound(_, _) | StorageError::FileNotFound | StorageError::ConfigNotFound
    )
}

fn storage_error_to_catalog(action: &str, err: StorageError) -> TableCatalogStoreError {
    match err {
        StorageError::ObjectNotFound(bucket, object) => TableCatalogStoreError::NotFound(format!("{action}: {bucket}/{object}")),
        StorageError::BucketNotFound(bucket) => TableCatalogStoreError::NotFound(format!("{action}: bucket {bucket}")),
        StorageError::PreconditionFailed => TableCatalogStoreError::Conflict(format!("{action}: precondition failed")),
        other => TableCatalogStoreError::Internal(format!("{action}: {other}")),
    }
}

#[cfg(test)]
pub(crate) mod test_support;

#[cfg(test)]
mod tests;
