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

#![allow(dead_code)]

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
mod identifier;
mod model;
mod store;

pub use error::{CatalogIdentifierError, TableObjectMutationError};
pub(crate) use error::{TableCatalogStoreError, TableCatalogStoreResult};
pub use identifier::{IdentifierSegment, Namespace, is_reserved_table_object_key};
pub(crate) use identifier::{
    default_table_data_dir_path, default_table_delete_dir_path, default_table_metadata_dir_path,
    default_table_metadata_file_path, default_view_metadata_file_path, is_valid_table_metadata_location,
    is_valid_view_metadata_location, metadata_location_from_metadata_file_path, validate_bucket_object_mutation,
};
pub(crate) use model::*;
pub(crate) use store::*;

pub(crate) const TABLE_BUCKET_MARKER_CONFIG: &str = BUCKET_TABLE_CONFIG;
pub(crate) const RESERVED_CATALOG_OBJECT_MESSAGE: &str = "Object key is reserved for the table catalog";
pub(crate) const TABLE_BUCKET_CATALOG_TYPE: &str = "iceberg-rest";
pub(crate) const TABLE_BUCKET_CONFIG_VERSION: u16 = 1;
pub(crate) const DEFAULT_WAREHOUSE_ID: &str = "default";
pub(crate) const TABLE_NAMESPACE_MARKER_VERSION: u16 = 1;
pub(crate) const TABLE_RESOURCE_MARKER_VERSION: u16 = 1;
pub(crate) const TABLE_METADATA_POINTER_VERSION: u16 = 1;
pub(crate) const TABLE_CATALOG_ENTRY_VERSION: u16 = 1;
pub(crate) const TABLE_MAINTENANCE_CONFIG_VERSION: u16 = 1;
pub(crate) const TABLE_EXTERNAL_CATALOG_BRIDGE_VERSION: u16 = 1;
pub(crate) const TABLE_CATALOG_BACKING_MANIFEST_VERSION: u16 = 1;
pub(crate) const ENV_TABLE_CATALOG_BACKING: &str = "RUSTFS_TABLE_CATALOG_BACKING";
pub(crate) const TABLE_CATALOG_BACKING_OBJECT: &str = "object";
pub(crate) const TABLE_CATALOG_BACKING_DURABLE_STRONG: &str = "durable-strong";
pub(crate) const TABLE_METADATA_FILE_NAME_MAX_LEN: usize = 128;
pub const TABLE_RESERVED_PREFIX: &str = BUCKET_TABLE_RESERVED_PREFIX;
const WAREHOUSE_ROOT: &str = "warehouses";
const NAMESPACE_ROOT: &str = "namespaces";
const TABLE_ROOT: &str = "tables";
const VIEW_ROOT: &str = "views";
const NAMESPACE_MARKER_FILE: &str = "namespace.json";
const TABLE_MARKER_FILE: &str = "table.json";
const CURRENT_POINTER_FILE: &str = "current.json";
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
const STRONG_TABLE_CATALOG_SNAPSHOT_VERSION: u16 = 1;
const STRONG_TABLE_CATALOG_BACKING_ROOT: &str = "strong-backing";
const STRONG_TABLE_CATALOG_SNAPSHOT_FILE: &str = "snapshot.json";
const TABLE_CATALOG_MIGRATION_VERSION: u16 = 1;
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

fn normalize_warehouse_object_prefix(object_prefix: &str, max_prefix_depth: Option<usize>) -> TableCatalogStoreResult<String> {
    let object_prefix = object_prefix.strip_suffix('/').unwrap_or(object_prefix);
    if object_prefix.is_empty() {
        return Err(TableCatalogStoreError::Invalid(
            "table warehouse location must include an object prefix".to_string(),
        ));
    }
    if object_prefix.contains('\\') {
        return Err(TableCatalogStoreError::Invalid(
            "table warehouse location contains an invalid path separator".to_string(),
        ));
    }
    let mut segment_count = 0;
    for segment in object_prefix.split('/') {
        segment_count += 1;
        if segment.is_empty() || segment == "." || segment == ".." {
            return Err(TableCatalogStoreError::Invalid(
                "table warehouse location contains an invalid path segment".to_string(),
            ));
        }
    }
    if max_prefix_depth.is_some_and(|max_prefix_depth| segment_count > max_prefix_depth) {
        return Err(TableCatalogStoreError::Invalid(
            "table warehouse location exceeds the maximum prefix depth".to_string(),
        ));
    }

    let mut normalized = object_prefix.to_string();
    normalized.push('/');
    Ok(normalized)
}

fn warehouse_object_prefix_from_location(
    table_bucket: &str,
    warehouse_location: &str,
    max_prefix_depth: Option<usize>,
) -> TableCatalogStoreResult<String> {
    let location = warehouse_location
        .strip_prefix("s3://")
        .ok_or_else(|| TableCatalogStoreError::Invalid("table warehouse location must be an s3 URI".to_string()))?;
    let (bucket, object_prefix) = location
        .split_once('/')
        .ok_or_else(|| TableCatalogStoreError::Invalid("table warehouse location must include an object prefix".to_string()))?;
    if bucket != table_bucket {
        return Err(TableCatalogStoreError::Invalid(
            "table warehouse location must be inside the table bucket".to_string(),
        ));
    }
    normalize_warehouse_object_prefix(object_prefix, max_prefix_depth)
}

fn table_warehouse_object_prefix_from_location(table_bucket: &str, warehouse_location: &str) -> TableCatalogStoreResult<String> {
    warehouse_object_prefix_from_location(table_bucket, warehouse_location, Some(WAREHOUSE_INDEX_MAX_PREFIX_DEPTH))
}

fn view_warehouse_object_prefix_from_location(table_bucket: &str, warehouse_location: &str) -> TableCatalogStoreResult<String> {
    warehouse_object_prefix_from_location(table_bucket, warehouse_location, None)
}

pub(crate) fn validate_table_warehouse_location(table_bucket: &str, warehouse_location: &str) -> TableCatalogStoreResult<()> {
    table_warehouse_object_prefix_from_location(table_bucket, warehouse_location).map(|_| ())
}

pub(crate) fn validate_view_warehouse_location(table_bucket: &str, warehouse_location: &str) -> TableCatalogStoreResult<()> {
    view_warehouse_object_prefix_from_location(table_bucket, warehouse_location).map(|_| ())
}

pub(crate) fn table_warehouse_object_prefix(entry: &TableEntry) -> TableCatalogStoreResult<String> {
    table_warehouse_object_prefix_from_location(&entry.table_bucket, &entry.warehouse_location)
}

fn table_warehouse_index_entry(entry: &TableEntry) -> TableCatalogStoreResult<TableWarehouseIndexEntry> {
    Ok(TableWarehouseIndexEntry {
        version: TABLE_CATALOG_ENTRY_VERSION,
        table_bucket: entry.table_bucket.clone(),
        namespace: entry.namespace.clone(),
        table: entry.table.clone(),
        table_id: entry.table_id.clone(),
        warehouse_object_prefix: table_warehouse_object_prefix(entry)?,
        state: entry.state.clone(),
    })
}

fn table_warehouse_data_dir_path(entry: &TableEntry) -> TableCatalogStoreResult<String> {
    Ok(format!("{}{}", table_warehouse_object_prefix(entry)?, DATA_DIR))
}

fn table_object_s3_location(table_bucket: &str, object_key: &str) -> String {
    format!("s3://{table_bucket}/{object_key}")
}

fn metadata_warehouse_location(
    table_bucket: &str,
    metadata_location: &str,
    metadata_object: &TableCatalogObject,
    validate_location: fn(&str, &str) -> TableCatalogStoreResult<()>,
) -> TableCatalogStoreResult<Option<String>> {
    let metadata: serde_json::Value = serde_json::from_slice(&metadata_object.data)
        .map_err(|err| TableCatalogStoreError::Invalid(format!("failed to parse new metadata {metadata_location}: {err}")))?;
    let Some(location) = metadata.get("location").and_then(serde_json::Value::as_str) else {
        return Ok(None);
    };
    validate_location(table_bucket, location)?;
    Ok(Some(location.to_string()))
}

fn table_metadata_warehouse_location(
    table_bucket: &str,
    metadata_location: &str,
    metadata_object: &TableCatalogObject,
) -> TableCatalogStoreResult<Option<String>> {
    metadata_warehouse_location(table_bucket, metadata_location, metadata_object, validate_table_warehouse_location)
}

fn view_metadata_warehouse_location(
    table_bucket: &str,
    metadata_location: &str,
    metadata_object: &TableCatalogObject,
) -> TableCatalogStoreResult<Option<String>> {
    metadata_warehouse_location(table_bucket, metadata_location, metadata_object, validate_view_warehouse_location)
}

fn warehouse_index_candidate_prefixes(object: &str) -> Vec<&str> {
    let mut prefixes = Vec::new();
    for (index, byte) in object.as_bytes().iter().enumerate() {
        if *byte == b'/' {
            prefixes.push(&object[..=index]);
            if prefixes.len() >= WAREHOUSE_INDEX_MAX_PREFIX_DEPTH {
                break;
            }
        }
    }
    prefixes.reverse();
    prefixes
}

fn table_data_plane_resource_from_entry(table: TableEntry, warehouse_object_prefix: String) -> TableDataPlaneResource {
    TableDataPlaneResource {
        table_bucket: table.table_bucket,
        namespace: table.namespace,
        table: table.table,
        table_id: table.table_id,
        warehouse_object_prefix,
    }
}

pub(crate) async fn table_data_plane_resource_for_object<S>(
    store: &S,
    bucket: &str,
    object: &str,
) -> TableCatalogStoreResult<Option<TableDataPlaneResource>>
where
    S: TableCatalogStore + ?Sized,
{
    store.resolve_table_data_plane_resource(bucket, object).await
}

async fn scan_table_data_plane_resource_for_object<S>(
    store: &S,
    bucket: &str,
    object: &str,
) -> TableCatalogStoreResult<Option<TableDataPlaneResource>>
where
    S: TableCatalogStore + ?Sized,
{
    if bucket.is_empty() || object.is_empty() {
        return Ok(None);
    }

    let Some(table_bucket) = store.get_table_bucket(bucket).await? else {
        return Ok(None);
    };
    if table_bucket.state != TableCatalogEntryState::Active {
        return Ok(None);
    }

    let mut matched: Option<TableDataPlaneResource> = None;
    for namespace in store.list_namespaces(bucket).await? {
        if namespace.state != TableCatalogEntryState::Active {
            continue;
        }
        for table in store.list_tables(bucket, &namespace.namespace).await? {
            if table.state != TableCatalogEntryState::Active {
                continue;
            }
            let Ok(warehouse_object_prefix) = table_warehouse_object_prefix(&table) else {
                continue;
            };
            if !object.starts_with(&warehouse_object_prefix) {
                continue;
            }
            if matched
                .as_ref()
                .is_some_and(|current| current.warehouse_object_prefix.len() >= warehouse_object_prefix.len())
            {
                continue;
            }
            matched = Some(table_data_plane_resource_from_entry(table, warehouse_object_prefix));
        }
    }

    Ok(matched)
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

fn catalog_list_page_from_entries<T, F>(
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

fn insert_metadata_maintenance_reason(
    reasons_by_location: &mut BTreeMap<String, BTreeSet<TableMetadataMaintenanceReason>>,
    metadata_location: String,
    reason: TableMetadataMaintenanceReason,
) {
    reasons_by_location.entry(metadata_location).or_default().insert(reason);
}

fn metadata_maintenance_object_reports(
    reasons_by_location: BTreeMap<String, BTreeSet<TableMetadataMaintenanceReason>>,
) -> Vec<TableMetadataMaintenanceObjectReport> {
    reasons_by_location
        .into_iter()
        .map(|(metadata_location, reasons)| {
            let reasons = reasons.into_iter().collect::<Vec<_>>();
            let state = if reasons.contains(&TableMetadataMaintenanceReason::SafetyWindowSatisfied) {
                TableMetadataMaintenanceObjectState::Deletable
            } else if reasons.contains(&TableMetadataMaintenanceReason::SafetyWindowPending) {
                TableMetadataMaintenanceObjectState::PendingSafetyWindow
            } else {
                TableMetadataMaintenanceObjectState::Retained
            };
            TableMetadataMaintenanceObjectReport {
                metadata_location,
                state,
                reasons,
            }
        })
        .collect()
}

#[derive(Debug, Clone)]
struct TableMetadataMaintenanceReferencedObjectAccumulator {
    object_kind: TableMetadataMaintenanceObjectKind,
    state: TableMetadataMaintenanceObjectState,
    reasons: BTreeSet<TableMetadataMaintenanceReason>,
}

fn insert_referenced_object_report(
    reports: &mut BTreeMap<String, TableMetadataMaintenanceReferencedObjectAccumulator>,
    object_location: String,
    object_kind: TableMetadataMaintenanceObjectKind,
    state: TableMetadataMaintenanceObjectState,
    reason: TableMetadataMaintenanceReason,
) {
    let report = reports
        .entry(object_location)
        .or_insert_with(|| TableMetadataMaintenanceReferencedObjectAccumulator {
            object_kind,
            state: TableMetadataMaintenanceObjectState::Retained,
            reasons: BTreeSet::new(),
        });
    if state == TableMetadataMaintenanceObjectState::ManualReviewRequired {
        report.state = TableMetadataMaintenanceObjectState::ManualReviewRequired;
    }
    report.reasons.insert(reason);
}

async fn metadata_maintenance_referenced_object_reports<B>(
    backend: &B,
    table_bucket: &str,
    namespace: &Namespace,
    table: &IdentifierSegment,
    warehouse_object_prefix: Option<&str>,
    current_metadata: &serde_json::Value,
    retained_metadata_locations: &[String],
) -> TableCatalogStoreResult<Vec<TableMetadataMaintenanceReferencedObjectReport>>
where
    B: TableCatalogObjectBackend,
{
    let mut reports = BTreeMap::<String, TableMetadataMaintenanceReferencedObjectAccumulator>::new();
    metadata_maintenance_referenced_object_reports_for_metadata(
        backend,
        table_bucket,
        namespace,
        table,
        warehouse_object_prefix,
        current_metadata,
        &mut reports,
    )
    .await?;

    for metadata_location in retained_metadata_locations {
        let Some(metadata_object) = backend.read_object(table_bucket, metadata_location).await? else {
            insert_referenced_object_report(
                &mut reports,
                metadata_location.clone(),
                TableMetadataMaintenanceObjectKind::MetadataFile,
                TableMetadataMaintenanceObjectState::ManualReviewRequired,
                TableMetadataMaintenanceReason::UnreadableMetadata,
            );
            continue;
        };
        let Ok(metadata) = serde_json::from_slice::<serde_json::Value>(&metadata_object.data) else {
            insert_referenced_object_report(
                &mut reports,
                metadata_location.clone(),
                TableMetadataMaintenanceObjectKind::MetadataFile,
                TableMetadataMaintenanceObjectState::ManualReviewRequired,
                TableMetadataMaintenanceReason::UnreadableMetadata,
            );
            continue;
        };
        if !metadata.is_object() {
            insert_referenced_object_report(
                &mut reports,
                metadata_location.clone(),
                TableMetadataMaintenanceObjectKind::MetadataFile,
                TableMetadataMaintenanceObjectState::ManualReviewRequired,
                TableMetadataMaintenanceReason::UnreadableMetadata,
            );
            continue;
        }
        metadata_maintenance_referenced_object_reports_for_metadata(
            backend,
            table_bucket,
            namespace,
            table,
            warehouse_object_prefix,
            &metadata,
            &mut reports,
        )
        .await?;
    }

    Ok(reports
        .into_iter()
        .map(|(object_location, report)| TableMetadataMaintenanceReferencedObjectReport {
            object_location,
            object_kind: report.object_kind,
            state: report.state,
            reasons: report.reasons.into_iter().collect(),
        })
        .collect())
}

async fn metadata_maintenance_referenced_object_reports_for_metadata<B>(
    backend: &B,
    table_bucket: &str,
    namespace: &Namespace,
    table: &IdentifierSegment,
    warehouse_object_prefix: Option<&str>,
    metadata: &serde_json::Value,
    reports: &mut BTreeMap<String, TableMetadataMaintenanceReferencedObjectAccumulator>,
) -> TableCatalogStoreResult<()>
where
    B: TableCatalogObjectBackend,
{
    let Some(snapshots) = metadata.get("snapshots").and_then(serde_json::Value::as_array) else {
        return Ok(());
    };

    for snapshot in snapshots {
        if let Some(manifest_list_location) = snapshot.get("manifest-list").and_then(serde_json::Value::as_str) {
            metadata_maintenance_referenced_manifest_list(
                backend,
                table_bucket,
                namespace,
                table,
                warehouse_object_prefix,
                manifest_list_location,
                reports,
            )
            .await?;
            continue;
        }

        let Some(manifests) = snapshot.get("manifests").and_then(serde_json::Value::as_array) else {
            continue;
        };
        for manifest in manifests {
            let Some(manifest_location) = manifest.as_str() else {
                insert_referenced_object_report(
                    reports,
                    "snapshots[].manifests".to_string(),
                    TableMetadataMaintenanceObjectKind::ManifestFile,
                    TableMetadataMaintenanceObjectState::ManualReviewRequired,
                    TableMetadataMaintenanceReason::UnsupportedManifestAvro,
                );
                continue;
            };
            metadata_maintenance_referenced_manifest_file(
                backend,
                table_bucket,
                namespace,
                table,
                warehouse_object_prefix,
                manifest_location,
                reports,
            )
            .await?;
        }
    }

    Ok(())
}

async fn metadata_maintenance_referenced_manifest_list<B>(
    backend: &B,
    table_bucket: &str,
    namespace: &Namespace,
    table: &IdentifierSegment,
    warehouse_object_prefix: Option<&str>,
    manifest_list_location: &str,
    reports: &mut BTreeMap<String, TableMetadataMaintenanceReferencedObjectAccumulator>,
) -> TableCatalogStoreResult<()>
where
    B: TableCatalogObjectBackend,
{
    let Some(manifest_list_key) = table_catalog_object_key_from_location(table_bucket, manifest_list_location) else {
        insert_referenced_object_report(
            reports,
            manifest_list_location.to_string(),
            TableMetadataMaintenanceObjectKind::ManifestList,
            TableMetadataMaintenanceObjectState::ManualReviewRequired,
            TableMetadataMaintenanceReason::UnsupportedManifestAvro,
        );
        return Ok(());
    };
    if table_maintenance_object_kind(namespace, table, warehouse_object_prefix, &manifest_list_key)
        != Some(TableMetadataMaintenanceObjectKind::ManifestList)
    {
        insert_referenced_object_report(
            reports,
            manifest_list_key,
            TableMetadataMaintenanceObjectKind::ManifestList,
            TableMetadataMaintenanceObjectState::ManualReviewRequired,
            TableMetadataMaintenanceReason::UnsupportedManifestAvro,
        );
        return Ok(());
    }
    insert_referenced_object_report(
        reports,
        manifest_list_key.clone(),
        TableMetadataMaintenanceObjectKind::ManifestList,
        TableMetadataMaintenanceObjectState::Retained,
        TableMetadataMaintenanceReason::ManifestList,
    );

    let Some(manifest_list_object) = backend.read_object(table_bucket, &manifest_list_key).await? else {
        mark_referenced_object_manual_review(
            reports,
            &manifest_list_key,
            TableMetadataMaintenanceReason::UnsupportedManifestAvro,
        );
        return Ok(());
    };
    let Ok(manifest_paths) = manifest_paths_from_manifest_list_avro(&manifest_list_object.data) else {
        mark_referenced_object_manual_review(
            reports,
            &manifest_list_key,
            TableMetadataMaintenanceReason::UnsupportedManifestAvro,
        );
        return Ok(());
    };
    for manifest_location in manifest_paths {
        metadata_maintenance_referenced_manifest_file(
            backend,
            table_bucket,
            namespace,
            table,
            warehouse_object_prefix,
            &manifest_location,
            reports,
        )
        .await?;
    }

    Ok(())
}

async fn metadata_maintenance_referenced_manifest_file<B>(
    backend: &B,
    table_bucket: &str,
    namespace: &Namespace,
    table: &IdentifierSegment,
    warehouse_object_prefix: Option<&str>,
    manifest_location: &str,
    reports: &mut BTreeMap<String, TableMetadataMaintenanceReferencedObjectAccumulator>,
) -> TableCatalogStoreResult<()>
where
    B: TableCatalogObjectBackend,
{
    let Some(manifest_key) = table_catalog_object_key_from_location(table_bucket, manifest_location) else {
        insert_referenced_object_report(
            reports,
            manifest_location.to_string(),
            TableMetadataMaintenanceObjectKind::ManifestFile,
            TableMetadataMaintenanceObjectState::ManualReviewRequired,
            TableMetadataMaintenanceReason::UnsupportedManifestAvro,
        );
        return Ok(());
    };
    if table_maintenance_object_kind(namespace, table, warehouse_object_prefix, &manifest_key)
        != Some(TableMetadataMaintenanceObjectKind::ManifestFile)
    {
        insert_referenced_object_report(
            reports,
            manifest_key,
            TableMetadataMaintenanceObjectKind::ManifestFile,
            TableMetadataMaintenanceObjectState::ManualReviewRequired,
            TableMetadataMaintenanceReason::UnsupportedManifestAvro,
        );
        return Ok(());
    }
    insert_referenced_object_report(
        reports,
        manifest_key.clone(),
        TableMetadataMaintenanceObjectKind::ManifestFile,
        TableMetadataMaintenanceObjectState::Retained,
        TableMetadataMaintenanceReason::ManifestFile,
    );

    let Some(manifest_object) = backend.read_object(table_bucket, &manifest_key).await? else {
        mark_referenced_object_manual_review(reports, &manifest_key, TableMetadataMaintenanceReason::UnsupportedManifestAvro);
        return Ok(());
    };
    let Ok(file_references) = file_references_from_manifest_avro(&manifest_object.data) else {
        mark_referenced_object_manual_review(reports, &manifest_key, TableMetadataMaintenanceReason::UnsupportedManifestAvro);
        return Ok(());
    };
    for (file_location, object_kind) in file_references {
        let Some(file_key) = table_catalog_object_key_from_location(table_bucket, &file_location) else {
            insert_referenced_object_report(
                reports,
                file_location,
                object_kind,
                TableMetadataMaintenanceObjectState::ManualReviewRequired,
                TableMetadataMaintenanceReason::UnsupportedManifestAvro,
            );
            continue;
        };
        if table_maintenance_object_kind(namespace, table, warehouse_object_prefix, &file_key) != Some(object_kind.clone()) {
            insert_referenced_object_report(
                reports,
                file_key,
                object_kind,
                TableMetadataMaintenanceObjectState::ManualReviewRequired,
                TableMetadataMaintenanceReason::UnsupportedManifestAvro,
            );
            continue;
        }
        insert_referenced_object_report(
            reports,
            file_key,
            object_kind.clone(),
            TableMetadataMaintenanceObjectState::Retained,
            table_metadata_maintenance_reason_for_object_kind(&object_kind),
        );
    }

    Ok(())
}

fn mark_referenced_object_manual_review(
    reports: &mut BTreeMap<String, TableMetadataMaintenanceReferencedObjectAccumulator>,
    object_location: &str,
    reason: TableMetadataMaintenanceReason,
) {
    if let Some(report) = reports.get_mut(object_location) {
        report.state = TableMetadataMaintenanceObjectState::ManualReviewRequired;
        report.reasons.insert(reason);
    }
}

fn manifest_paths_from_manifest_list_avro(data: &[u8]) -> TableCatalogStoreResult<Vec<String>> {
    Ok(manifest_list_references_from_manifest_list_avro(data)?
        .into_iter()
        .map(|reference| reference.manifest_path)
        .collect())
}

pub(crate) fn manifest_list_references_from_manifest_list_avro(
    data: &[u8],
) -> TableCatalogStoreResult<Vec<ManifestListReference>> {
    let reader = apache_avro::Reader::new(data)
        .map_err(|err| TableCatalogStoreError::Invalid(format!("failed to read manifest list Avro: {err}")))?;
    let mut manifest_paths = Vec::new();
    for value in reader {
        let value =
            value.map_err(|err| TableCatalogStoreError::Invalid(format!("failed to read manifest list record: {err}")))?;
        let manifest_path = avro_record_field(&value, "manifest_path")
            .and_then(avro_string_value)
            .ok_or_else(|| TableCatalogStoreError::Invalid("manifest list entry missing manifest_path".to_string()))?;
        manifest_paths.push(ManifestListReference {
            manifest_path: manifest_path.to_string(),
            partition_spec_id: avro_record_field(&value, "partition_spec_id").and_then(avro_i32_value),
            sequence_number: avro_record_field(&value, "sequence_number").and_then(avro_i64_value),
            added_snapshot_id: avro_record_field(&value, "added_snapshot_id").and_then(avro_i64_value),
        });
    }
    Ok(manifest_paths)
}

fn file_references_from_manifest_avro(data: &[u8]) -> TableCatalogStoreResult<Vec<(String, TableMetadataMaintenanceObjectKind)>> {
    Ok(data_file_references_from_manifest_avro(data)?
        .into_iter()
        .map(|reference| (reference.location, reference.object_kind))
        .collect())
}

pub(crate) fn data_file_references_from_manifest_avro(data: &[u8]) -> TableCatalogStoreResult<Vec<ManifestDataFileReference>> {
    let reader = apache_avro::Reader::new(data)
        .map_err(|err| TableCatalogStoreError::Invalid(format!("failed to read manifest Avro: {err}")))?;
    let mut files = Vec::new();
    for value in reader {
        let value = value.map_err(|err| TableCatalogStoreError::Invalid(format!("failed to read manifest record: {err}")))?;
        let data_file = avro_record_field(&value, "data_file")
            .ok_or_else(|| TableCatalogStoreError::Invalid("manifest entry missing data_file".to_string()))?;
        let file_path = avro_record_field(data_file, "file_path")
            .and_then(avro_string_value)
            .ok_or_else(|| TableCatalogStoreError::Invalid("manifest data file missing file_path".to_string()))?;
        let content = avro_record_field(data_file, "content")
            .and_then(avro_i32_value)
            .ok_or_else(|| TableCatalogStoreError::Invalid("manifest data file missing content".to_string()))?;
        let (content, object_kind) = match content {
            0 => (ManifestDataFileContent::Data, TableMetadataMaintenanceObjectKind::DataFile),
            1 => (ManifestDataFileContent::PositionDelete, TableMetadataMaintenanceObjectKind::DeleteFile),
            2 => (ManifestDataFileContent::EqualityDelete, TableMetadataMaintenanceObjectKind::DeleteFile),
            _ => continue,
        };
        files.push(ManifestDataFileReference {
            location: file_path.to_string(),
            content,
            object_kind,
            entry_status: avro_record_field(&value, "status").and_then(avro_i32_value),
            snapshot_id: avro_record_field(&value, "snapshot_id").and_then(avro_i64_value),
            sequence_number: avro_record_field(&value, "sequence_number").and_then(avro_i64_value),
            file_sequence_number: avro_record_field(&value, "file_sequence_number").and_then(avro_i64_value),
            record_count: avro_record_field(data_file, "record_count")
                .and_then(avro_i64_value)
                .and_then(|value| u64::try_from(value).ok()),
            file_size_bytes: avro_record_field(data_file, "file_size_in_bytes")
                .and_then(avro_i64_value)
                .and_then(|value| u64::try_from(value).ok()),
            partition: avro_record_field(data_file, "partition")
                .and_then(avro_record_value_fields)
                .unwrap_or_default(),
            sort_order_id: avro_record_field(data_file, "sort_order_id").and_then(avro_i32_value),
        });
    }
    Ok(files)
}

fn avro_record_field<'a>(value: &'a apache_avro::types::Value, name: &str) -> Option<&'a apache_avro::types::Value> {
    let value = avro_non_union_value(value);
    let apache_avro::types::Value::Record(fields) = value else {
        return None;
    };
    fields
        .iter()
        .find_map(|(field_name, field_value)| (field_name == name).then_some(avro_non_union_value(field_value)))
}

fn avro_record_value_fields(value: &apache_avro::types::Value) -> Option<Vec<(String, apache_avro::types::Value)>> {
    let value = avro_non_union_value(value);
    let apache_avro::types::Value::Record(fields) = value else {
        return None;
    };
    Some(
        fields
            .iter()
            .map(|(field_name, field_value)| (field_name.clone(), avro_non_union_value(field_value).clone()))
            .collect(),
    )
}

fn avro_non_union_value(value: &apache_avro::types::Value) -> &apache_avro::types::Value {
    match value {
        apache_avro::types::Value::Union(_, inner) => avro_non_union_value(inner),
        value => value,
    }
}

fn avro_string_value(value: &apache_avro::types::Value) -> Option<&str> {
    match avro_non_union_value(value) {
        apache_avro::types::Value::String(value) => Some(value),
        _ => None,
    }
}

fn avro_i32_value(value: &apache_avro::types::Value) -> Option<i32> {
    match avro_non_union_value(value) {
        apache_avro::types::Value::Int(value) => Some(*value),
        _ => None,
    }
}

fn avro_i64_value(value: &apache_avro::types::Value) -> Option<i64> {
    match avro_non_union_value(value) {
        apache_avro::types::Value::Long(value) => Some(*value),
        _ => None,
    }
}

pub(crate) fn table_catalog_object_key_from_location(table_bucket: &str, location: &str) -> Option<String> {
    let object = if let Some(location) = location.strip_prefix("s3://") {
        let (bucket, object) = location.split_once('/')?;
        if bucket != table_bucket {
            return None;
        }
        object
    } else {
        location
    };

    if object.is_empty()
        || object.starts_with('/')
        || object.contains("..")
        || object.contains('\\')
        || object.bytes().any(|byte| byte.is_ascii_control())
    {
        return None;
    }

    Some(object.to_string())
}

pub(crate) fn table_maintenance_object_kind(
    namespace: &Namespace,
    table: &IdentifierSegment,
    warehouse_object_prefix: Option<&str>,
    object_location: &str,
) -> Option<TableMetadataMaintenanceObjectKind> {
    let metadata_prefix = format!("{}/", default_table_metadata_dir_path(namespace, table));
    if let Some(kind) = table_maintenance_metadata_object_kind(&metadata_prefix, object_location) {
        return Some(kind);
    }

    let data_prefix = format!("{}/", default_table_data_dir_path(namespace, table));
    if object_location
        .strip_prefix(&data_prefix)
        .is_some_and(is_valid_table_maintenance_nested_object)
    {
        return Some(TableMetadataMaintenanceObjectKind::DataFile);
    }

    let delete_prefix = format!("{}/", default_table_delete_dir_path(namespace, table));
    if object_location
        .strip_prefix(&delete_prefix)
        .is_some_and(is_valid_table_maintenance_nested_object)
    {
        return Some(TableMetadataMaintenanceObjectKind::DeleteFile);
    }

    if let Some(warehouse_object_prefix) = warehouse_object_prefix {
        let metadata_prefix = format!("{warehouse_object_prefix}{METADATA_DIR}/");
        if let Some(kind) = table_maintenance_metadata_object_kind(&metadata_prefix, object_location) {
            return Some(kind);
        }

        let data_prefix = format!("{warehouse_object_prefix}{DATA_DIR}/");
        if object_location
            .strip_prefix(&data_prefix)
            .is_some_and(is_valid_table_maintenance_nested_object)
        {
            return Some(TableMetadataMaintenanceObjectKind::DataFile);
        }

        let delete_prefix = format!("{warehouse_object_prefix}{DELETE_DIR}/");
        if object_location
            .strip_prefix(&delete_prefix)
            .is_some_and(is_valid_table_maintenance_nested_object)
        {
            return Some(TableMetadataMaintenanceObjectKind::DeleteFile);
        }
    }

    None
}

fn table_maintenance_metadata_object_kind(
    metadata_prefix: &str,
    object_location: &str,
) -> Option<TableMetadataMaintenanceObjectKind> {
    let file_name = object_location.strip_prefix(metadata_prefix)?;
    if file_name.is_empty()
        || file_name.contains('/')
        || file_name.contains('\\')
        || file_name.contains("..")
        || file_name.bytes().any(|byte| byte.is_ascii_control())
        || !file_name.ends_with(".avro")
    {
        return None;
    }
    if file_name.starts_with("snap-") {
        return Some(TableMetadataMaintenanceObjectKind::ManifestList);
    }
    Some(TableMetadataMaintenanceObjectKind::ManifestFile)
}

fn is_valid_table_maintenance_nested_object(suffix: &str) -> bool {
    !suffix.is_empty()
        && !suffix.starts_with('/')
        && !suffix.contains("..")
        && !suffix.contains('\\')
        && !suffix.bytes().any(|byte| byte.is_ascii_control())
}

fn table_metadata_maintenance_reason_for_object_kind(
    object_kind: &TableMetadataMaintenanceObjectKind,
) -> TableMetadataMaintenanceReason {
    match object_kind {
        TableMetadataMaintenanceObjectKind::MetadataFile => TableMetadataMaintenanceReason::CurrentMetadata,
        TableMetadataMaintenanceObjectKind::ManifestList => TableMetadataMaintenanceReason::ManifestList,
        TableMetadataMaintenanceObjectKind::ManifestFile => TableMetadataMaintenanceReason::ManifestFile,
        TableMetadataMaintenanceObjectKind::DataFile => TableMetadataMaintenanceReason::DataFile,
        TableMetadataMaintenanceObjectKind::DeleteFile => TableMetadataMaintenanceReason::DeleteFile,
    }
}

fn metadata_maintenance_reachability_graph_report(
    metadata_file_count: usize,
    referenced_object_reports: &[TableMetadataMaintenanceReferencedObjectReport],
) -> TableMaintenanceReachabilityGraphReport {
    let manifest_list_count = referenced_object_reports
        .iter()
        .filter(|report| report.object_kind == TableMetadataMaintenanceObjectKind::ManifestList)
        .count();
    let manifest_file_count = referenced_object_reports
        .iter()
        .filter(|report| report.object_kind == TableMetadataMaintenanceObjectKind::ManifestFile)
        .count();
    let data_file_count = referenced_object_reports
        .iter()
        .filter(|report| report.object_kind == TableMetadataMaintenanceObjectKind::DataFile)
        .count();
    let delete_file_count = referenced_object_reports
        .iter()
        .filter(|report| report.object_kind == TableMetadataMaintenanceObjectKind::DeleteFile)
        .count();
    let manual_review_count = referenced_object_reports
        .iter()
        .filter(|report| report.state == TableMetadataMaintenanceObjectState::ManualReviewRequired)
        .count();
    let mut reasons = BTreeSet::from([TableMaintenanceReachabilityGraphReason::MetadataJsonParsed]);
    if manifest_list_count > 0 {
        reasons.insert(TableMaintenanceReachabilityGraphReason::ManifestListAvroReferenced);
    }
    if referenced_object_reports.iter().any(|report| {
        report
            .reasons
            .contains(&TableMetadataMaintenanceReason::UnsupportedManifestAvro)
    }) {
        reasons.insert(TableMaintenanceReachabilityGraphReason::ManifestAvroReaderUnavailable);
    }

    TableMaintenanceReachabilityGraphReport {
        status: if manual_review_count == 0 {
            TableMaintenanceReachabilityGraphStatus::Complete
        } else {
            TableMaintenanceReachabilityGraphStatus::ManualReviewRequired
        },
        metadata_file_count,
        manifest_list_count,
        manifest_file_count,
        data_file_count,
        delete_file_count,
        manual_review_count,
        reasons: reasons.into_iter().collect(),
    }
}

async fn metadata_maintenance_object_cleanup_reports<B>(
    backend: &B,
    table_bucket: &str,
    namespace: &Namespace,
    table: &IdentifierSegment,
    warehouse_object_prefix: Option<&str>,
    referenced_object_reports: &[TableMetadataMaintenanceReferencedObjectReport],
    now: OffsetDateTime,
) -> TableCatalogStoreResult<(usize, Vec<String>, Vec<String>, Vec<TableMetadataMaintenanceObjectCleanupReport>)>
where
    B: TableCatalogObjectBackend,
{
    let scanned_objects =
        table_maintenance_cleanup_objects(backend, table_bucket, namespace, table, warehouse_object_prefix).await?;
    if referenced_object_reports
        .iter()
        .any(|report| report.state == TableMetadataMaintenanceObjectState::ManualReviewRequired)
    {
        return Ok((scanned_objects.len(), Vec::new(), Vec::new(), Vec::new()));
    }

    let referenced_locations = referenced_object_reports
        .iter()
        .filter_map(|report| table_catalog_object_key_from_location(table_bucket, &report.object_location))
        .collect::<BTreeSet<_>>();
    let mut cleanup_candidate_locations = Vec::new();
    let mut deletable_object_locations = Vec::new();
    let mut cleanup_reports = Vec::new();

    for (object_location, object_kind) in scanned_objects {
        if referenced_locations.contains(&object_location) {
            continue;
        }
        let mut reasons = BTreeSet::from([
            table_metadata_maintenance_reason_for_object_kind(&object_kind),
            TableMetadataMaintenanceReason::NoCurrentReachability,
        ]);
        let state = match backend.read_object(table_bucket, &object_location).await? {
            Some(object) if metadata_candidate_is_past_safety_window(object.mod_time, now) => {
                reasons.insert(TableMetadataMaintenanceReason::SafetyWindowSatisfied);
                cleanup_candidate_locations.push(object_location.clone());
                deletable_object_locations.push(object_location.clone());
                TableMetadataMaintenanceObjectState::Deletable
            }
            _ => {
                reasons.insert(TableMetadataMaintenanceReason::SafetyWindowPending);
                cleanup_candidate_locations.push(object_location.clone());
                TableMetadataMaintenanceObjectState::PendingSafetyWindow
            }
        };
        cleanup_reports.push(TableMetadataMaintenanceObjectCleanupReport {
            object_location,
            object_kind,
            state,
            reasons: reasons.into_iter().collect(),
        });
    }

    Ok((
        referenced_locations.len() + cleanup_reports.len(),
        cleanup_candidate_locations,
        deletable_object_locations,
        cleanup_reports,
    ))
}

async fn table_maintenance_cleanup_objects<B>(
    backend: &B,
    table_bucket: &str,
    namespace: &Namespace,
    table: &IdentifierSegment,
    warehouse_object_prefix: Option<&str>,
) -> TableCatalogStoreResult<BTreeMap<String, TableMetadataMaintenanceObjectKind>>
where
    B: TableCatalogObjectBackend,
{
    let mut objects = BTreeMap::new();
    let mut metadata_prefixes = vec![format!("{}/", default_table_metadata_dir_path(namespace, table))];
    let mut data_prefixes = vec![format!("{}/", default_table_data_dir_path(namespace, table))];
    let mut delete_prefixes = vec![format!("{}/", default_table_delete_dir_path(namespace, table))];
    if let Some(warehouse_object_prefix) = warehouse_object_prefix {
        metadata_prefixes.push(format!("{warehouse_object_prefix}{METADATA_DIR}/"));
        data_prefixes.push(format!("{warehouse_object_prefix}{DATA_DIR}/"));
        delete_prefixes.push(format!("{warehouse_object_prefix}{DELETE_DIR}/"));
    }
    metadata_prefixes.sort();
    metadata_prefixes.dedup();
    data_prefixes.sort();
    data_prefixes.dedup();
    delete_prefixes.sort();
    delete_prefixes.dedup();

    for metadata_prefix in metadata_prefixes {
        for object in backend.list_objects(table_bucket, &metadata_prefix).await? {
            if let Some(kind) = table_maintenance_object_kind(namespace, table, warehouse_object_prefix, &object)
                && matches!(
                    kind,
                    TableMetadataMaintenanceObjectKind::ManifestList | TableMetadataMaintenanceObjectKind::ManifestFile
                )
            {
                objects.insert(object, kind);
            }
        }
    }

    for data_prefix in data_prefixes {
        for object in backend.list_objects(table_bucket, &data_prefix).await? {
            if table_maintenance_object_kind(namespace, table, warehouse_object_prefix, &object)
                == Some(TableMetadataMaintenanceObjectKind::DataFile)
            {
                objects.insert(object, TableMetadataMaintenanceObjectKind::DataFile);
            }
        }
    }

    for delete_prefix in delete_prefixes {
        for object in backend.list_objects(table_bucket, &delete_prefix).await? {
            if table_maintenance_object_kind(namespace, table, warehouse_object_prefix, &object)
                == Some(TableMetadataMaintenanceObjectKind::DeleteFile)
            {
                objects.insert(object, TableMetadataMaintenanceObjectKind::DeleteFile);
            }
        }
    }

    Ok(objects)
}

fn mark_deleted_metadata_object_reports(
    object_reports: &mut [TableMetadataMaintenanceObjectReport],
    deleted_locations: &BTreeSet<String>,
) {
    for object_report in object_reports {
        if !deleted_locations.contains(&object_report.metadata_location) {
            continue;
        }
        object_report.state = TableMetadataMaintenanceObjectState::Deleted;
        if !object_report
            .reasons
            .contains(&TableMetadataMaintenanceReason::DeletedByMaintenance)
        {
            object_report
                .reasons
                .push(TableMetadataMaintenanceReason::DeletedByMaintenance);
        }
    }
}

fn mark_deleted_object_cleanup_reports(
    object_reports: &mut [TableMetadataMaintenanceObjectCleanupReport],
    deleted_locations: &BTreeSet<String>,
) {
    for object_report in object_reports {
        if !deleted_locations.contains(&object_report.object_location) {
            continue;
        }
        object_report.state = TableMetadataMaintenanceObjectState::Deleted;
        if !object_report
            .reasons
            .contains(&TableMetadataMaintenanceReason::DeletedByMaintenance)
        {
            object_report
                .reasons
                .push(TableMetadataMaintenanceReason::DeletedByMaintenance);
        }
    }
}

fn metadata_log_locations(
    current_metadata: &serde_json::Value,
    namespace: &Namespace,
    table: &IdentifierSegment,
) -> BTreeSet<String> {
    let mut locations = BTreeSet::new();
    let Some(metadata_log) = current_metadata.get("metadata-log").and_then(serde_json::Value::as_array) else {
        return locations;
    };

    for entry in metadata_log {
        let Some(metadata_location) = entry.get("metadata-file").and_then(serde_json::Value::as_str) else {
            continue;
        };
        if is_valid_table_metadata_location(namespace, table, metadata_location) {
            locations.insert(metadata_location.to_string());
        }
    }

    locations
}

async fn metadata_locations_for_protected_snapshot_refs<B>(
    backend: &B,
    table_bucket: &str,
    namespace: &Namespace,
    table: &IdentifierSegment,
    current_metadata: &serde_json::Value,
    metadata_locations: &[String],
) -> TableCatalogStoreResult<BTreeSet<String>>
where
    B: TableCatalogObjectBackend,
{
    let protected_snapshot_ids = protected_ref_snapshot_ids(current_metadata);
    if protected_snapshot_ids.is_empty() {
        return Ok(BTreeSet::new());
    }

    let mut retained = BTreeSet::new();
    for metadata_location in metadata_locations {
        if !is_valid_table_metadata_location(namespace, table, metadata_location) {
            continue;
        }
        let Some(metadata_object) = backend.read_object(table_bucket, metadata_location).await? else {
            continue;
        };
        let Ok(metadata) = serde_json::from_slice::<serde_json::Value>(&metadata_object.data) else {
            continue;
        };
        if metadata_contains_protected_snapshot_ref(&metadata, &protected_snapshot_ids) {
            retained.insert(metadata_location.clone());
        }
    }
    Ok(retained)
}

fn protected_ref_snapshot_ids(current_metadata: &serde_json::Value) -> BTreeSet<i64> {
    let mut snapshot_ids = BTreeSet::new();
    let current_snapshot_id = current_metadata
        .get("current-snapshot-id")
        .and_then(serde_json::Value::as_i64);
    let Some(refs) = current_metadata.get("refs").and_then(serde_json::Value::as_object) else {
        return snapshot_ids;
    };

    for reference in refs.values() {
        if let Some(snapshot_id) = reference.get("snapshot-id").and_then(serde_json::Value::as_i64)
            && Some(snapshot_id) != current_snapshot_id
        {
            snapshot_ids.insert(snapshot_id);
        }
    }
    snapshot_ids
}

fn metadata_contains_protected_snapshot_ref(metadata: &serde_json::Value, protected_snapshot_ids: &BTreeSet<i64>) -> bool {
    let current_snapshot_matches = metadata
        .get("current-snapshot-id")
        .and_then(serde_json::Value::as_i64)
        .is_some_and(|snapshot_id| protected_snapshot_ids.contains(&snapshot_id));
    if current_snapshot_matches {
        return true;
    }

    let Some(refs) = metadata.get("refs").and_then(serde_json::Value::as_object) else {
        return false;
    };
    refs.values().any(|reference| {
        reference
            .get("snapshot-id")
            .and_then(serde_json::Value::as_i64)
            .is_some_and(|snapshot_id| protected_snapshot_ids.contains(&snapshot_id))
    })
}

fn metadata_candidate_is_past_safety_window(mod_time: Option<OffsetDateTime>, now: OffsetDateTime) -> bool {
    let Some(mod_time) = mod_time else {
        return false;
    };
    mod_time <= now - Duration::seconds(TABLE_METADATA_CLEANUP_SAFETY_WINDOW_SECONDS)
}

#[derive(Debug)]
struct TableSnapshotExpirationDraft {
    snapshot_id: Option<i64>,
    sequence_number: Option<i64>,
    timestamp_ms: Option<i64>,
    manifest_list: Option<String>,
    reasons: BTreeSet<TableSnapshotExpirationReason>,
}

fn table_snapshot_expiration_report(
    table_bucket: &str,
    namespace: &Namespace,
    table: &IdentifierSegment,
    entry: &TableEntry,
    current_metadata: &serde_json::Value,
    config: TableSnapshotExpirationConfig,
    now: OffsetDateTime,
) -> TableSnapshotExpirationReport {
    let current_snapshot_id = current_metadata
        .get("current-snapshot-id")
        .and_then(serde_json::Value::as_i64);
    let expiration_watermark_ms = unix_timestamp_millis(now).saturating_sub(config.max_snapshot_age_ms);
    let (protected_ref_snapshot_ids, user_defined_ref_snapshot_ids, ref_retention_conflict_snapshot_ids) =
        snapshot_expiration_ref_state(current_metadata, current_snapshot_id);
    let table_retention_property_conflict = snapshot_expiration_table_property_conflicts(current_metadata, &config);

    let mut drafts = snapshot_expiration_drafts(current_metadata, current_snapshot_id);
    mark_recent_snapshots_to_keep(&mut drafts, config.min_snapshots_to_keep);

    let mut snapshot_reports = Vec::with_capacity(drafts.len());
    for mut draft in drafts {
        if let Some(snapshot_id) = draft.snapshot_id {
            if protected_ref_snapshot_ids.contains(&snapshot_id) {
                draft.reasons.insert(TableSnapshotExpirationReason::ProtectedSnapshotRef);
            }
            if user_defined_ref_snapshot_ids.contains(&snapshot_id) {
                draft.reasons.insert(TableSnapshotExpirationReason::UserDefinedSnapshotRef);
            }
            if ref_retention_conflict_snapshot_ids.contains(&snapshot_id) {
                draft
                    .reasons
                    .insert(TableSnapshotExpirationReason::SnapshotRefRetentionConflict);
            }
        }
        if table_retention_property_conflict {
            draft
                .reasons
                .insert(TableSnapshotExpirationReason::TableRetentionPropertyConflict);
        }

        let state = if snapshot_expiration_requires_manual_review(&draft.reasons) {
            TableSnapshotExpirationSnapshotState::ManualReviewRequired
        } else if snapshot_expiration_is_retained(&draft.reasons) {
            TableSnapshotExpirationSnapshotState::Retained
        } else if let Some(timestamp_ms) = draft.timestamp_ms {
            if timestamp_ms <= expiration_watermark_ms {
                draft.reasons.insert(TableSnapshotExpirationReason::SnapshotAgeExpired);
                TableSnapshotExpirationSnapshotState::ExpirationCandidate
            } else {
                draft
                    .reasons
                    .insert(TableSnapshotExpirationReason::SnapshotAgeWithinRetention);
                TableSnapshotExpirationSnapshotState::Retained
            }
        } else {
            draft.reasons.insert(TableSnapshotExpirationReason::MissingSnapshotTimestamp);
            TableSnapshotExpirationSnapshotState::ManualReviewRequired
        };

        snapshot_reports.push(TableSnapshotExpirationSnapshotReport {
            snapshot_id: draft.snapshot_id,
            sequence_number: draft.sequence_number,
            timestamp_ms: draft.timestamp_ms,
            manifest_list: draft.manifest_list,
            state,
            reasons: draft.reasons.into_iter().collect(),
        });
    }

    let retained_snapshot_count = snapshot_reports
        .iter()
        .filter(|snapshot| snapshot.state == TableSnapshotExpirationSnapshotState::Retained)
        .count();
    let expiration_candidate_count = snapshot_reports
        .iter()
        .filter(|snapshot| snapshot.state == TableSnapshotExpirationSnapshotState::ExpirationCandidate)
        .count();
    let manual_review_count = snapshot_reports
        .iter()
        .filter(|snapshot| snapshot.state == TableSnapshotExpirationSnapshotState::ManualReviewRequired)
        .count();

    TableSnapshotExpirationReport {
        table_bucket: table_bucket.to_string(),
        namespace: namespace.public_name(),
        table: table.as_str().to_string(),
        table_id: entry.table_id.clone(),
        current_metadata_location: entry.metadata_location.clone(),
        current_snapshot_id,
        config,
        expiration_watermark_ms,
        retained_snapshot_count,
        expiration_candidate_count,
        manual_review_count,
        expired_snapshot_ids: Vec::new(),
        committed_metadata_location: None,
        snapshot_reports,
    }
}

#[derive(Debug, Clone)]
struct TableCompactionDataFileCandidate {
    location: String,
    size_bytes: u64,
    rewrite_prefix: String,
    sort_order_id: Option<i32>,
}

#[derive(Debug, Default)]
struct CompactionManifestPlanning {
    candidates: Vec<TableCompactionDataFileCandidate>,
    row_level_planning: TableRowLevelMaintenancePlanningReport,
}

struct CompactedParquetFile {
    data: Vec<u8>,
    record_count: u64,
}

#[derive(Debug, Clone)]
struct CompactedDataFile {
    object_key: String,
    file_path: String,
    file_size_bytes: u64,
    record_count: u64,
    partition_spec_id: i32,
    partition: Vec<(String, apache_avro::types::Value)>,
    sort_order_id: Option<i32>,
    status: i32,
    snapshot_id: i64,
    sequence_number: i64,
    file_sequence_number: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ManifestDataFileReference {
    pub location: String,
    pub content: ManifestDataFileContent,
    pub object_kind: TableMetadataMaintenanceObjectKind,
    pub entry_status: Option<i32>,
    pub snapshot_id: Option<i64>,
    pub sequence_number: Option<i64>,
    pub file_sequence_number: Option<i64>,
    pub record_count: Option<u64>,
    pub file_size_bytes: Option<u64>,
    pub partition: Vec<(String, apache_avro::types::Value)>,
    pub sort_order_id: Option<i32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ManifestDataFileContent {
    Data,
    PositionDelete,
    EqualityDelete,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ManifestListReference {
    pub manifest_path: String,
    pub partition_spec_id: Option<i32>,
    pub sequence_number: Option<i64>,
    pub added_snapshot_id: Option<i64>,
}

struct CompactionManifestListSummary<'a> {
    manifest_path: &'a str,
    manifest_length: u64,
    partition_spec_id: i32,
    snapshot_id: i64,
    sequence_number: i64,
    added_files_count: usize,
    existing_files_count: usize,
    added_rows_count: u64,
    existing_rows_count: u64,
}

async fn table_compaction_planning_report<B>(
    backend: &B,
    table_bucket: &str,
    namespace: &Namespace,
    table: &IdentifierSegment,
    entry: &TableEntry,
    current_metadata: &serde_json::Value,
    config: TableCompactionPlanningConfig,
) -> TableCatalogStoreResult<TableCompactionPlanningReport>
where
    B: TableCatalogObjectBackend,
{
    let current_snapshot_id = current_metadata
        .get("current-snapshot-id")
        .and_then(serde_json::Value::as_i64);
    let warehouse_object_prefix = table_warehouse_object_prefix(entry).ok();
    let mut snapshot_reports = Vec::new();
    let mut candidates = Vec::new();
    let mut rewrite_groups = Vec::new();
    let mut row_level_planning = TableRowLevelMaintenancePlanningReport::default();

    if let Some(current_snapshot_id) = current_snapshot_id {
        let current_snapshot = current_metadata
            .get("snapshots")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .find(|snapshot| {
                snapshot
                    .get("snapshot-id")
                    .and_then(serde_json::Value::as_i64)
                    .is_some_and(|snapshot_id| snapshot_id == current_snapshot_id)
            });
        match current_snapshot {
            Some(snapshot) => {
                let manifest_list = snapshot
                    .get("manifest-list")
                    .and_then(serde_json::Value::as_str)
                    .map(ToString::to_string);
                match manifest_list.as_deref() {
                    Some(manifest_list) => {
                        let planning = match compaction_data_file_candidates(
                            backend,
                            table_bucket,
                            namespace,
                            table,
                            warehouse_object_prefix.as_deref(),
                            manifest_list,
                            &config,
                        )
                        .await
                        {
                            Ok(planning) => planning,
                            Err(_) => {
                                snapshot_reports.push(TableCompactionSnapshotReport {
                                    snapshot_id: Some(current_snapshot_id),
                                    manifest_list: Some(manifest_list.to_string()),
                                    status: TableCompactionPlanningStatus::ManualReviewRequired,
                                    reasons: vec![
                                        TableCompactionPlanningReason::ManifestList,
                                        TableCompactionPlanningReason::ManifestAvroReaderUnavailable,
                                    ],
                                });
                                CompactionManifestPlanning::default()
                            }
                        };
                        row_level_planning = planning.row_level_planning;
                        candidates = planning.candidates;
                        if row_level_planning.status == TableRowLevelMaintenancePlanningStatus::ManualReviewRequired
                            && snapshot_reports.is_empty()
                        {
                            snapshot_reports.push(TableCompactionSnapshotReport {
                                snapshot_id: Some(current_snapshot_id),
                                manifest_list: Some(manifest_list.to_string()),
                                status: TableCompactionPlanningStatus::ManualReviewRequired,
                                reasons: compaction_row_level_planning_reasons(&row_level_planning),
                            });
                        } else if !candidates.is_empty() && snapshot_reports.is_empty() {
                            rewrite_groups = compaction_rewrite_groups(&candidates, &config);
                            let (status, reasons) = if rewrite_groups.is_empty() {
                                (
                                    TableCompactionPlanningStatus::NoCandidates,
                                    vec![
                                        TableCompactionPlanningReason::ManifestList,
                                        TableCompactionPlanningReason::ManifestFile,
                                    ],
                                )
                            } else {
                                (
                                    TableCompactionPlanningStatus::RewriteCandidates,
                                    vec![
                                        TableCompactionPlanningReason::ManifestList,
                                        TableCompactionPlanningReason::ManifestFile,
                                        TableCompactionPlanningReason::SmallDataFile,
                                        TableCompactionPlanningReason::RewriteGroup,
                                    ],
                                )
                            };
                            snapshot_reports.push(TableCompactionSnapshotReport {
                                snapshot_id: Some(current_snapshot_id),
                                manifest_list: Some(manifest_list.to_string()),
                                status,
                                reasons,
                            });
                        }
                    }
                    None => snapshot_reports.push(TableCompactionSnapshotReport {
                        snapshot_id: Some(current_snapshot_id),
                        manifest_list: None,
                        status: TableCompactionPlanningStatus::ManualReviewRequired,
                        reasons: vec![TableCompactionPlanningReason::MissingManifestList],
                    }),
                }
            }
            None => snapshot_reports.push(TableCompactionSnapshotReport {
                snapshot_id: Some(current_snapshot_id),
                manifest_list: None,
                status: TableCompactionPlanningStatus::ManualReviewRequired,
                reasons: vec![TableCompactionPlanningReason::MissingCurrentSnapshot],
            }),
        }
    }

    let manual_review_count = snapshot_reports
        .iter()
        .filter(|snapshot| snapshot.status == TableCompactionPlanningStatus::ManualReviewRequired)
        .count();
    let status = if manual_review_count > 0 {
        TableCompactionPlanningStatus::ManualReviewRequired
    } else if rewrite_groups.is_empty() {
        TableCompactionPlanningStatus::NoCandidates
    } else {
        TableCompactionPlanningStatus::RewriteCandidates
    };

    Ok(TableCompactionPlanningReport {
        table_bucket: table_bucket.to_string(),
        namespace: namespace.public_name(),
        table: table.as_str().to_string(),
        table_id: entry.table_id.clone(),
        current_metadata_location: entry.metadata_location.clone(),
        current_snapshot_id,
        config,
        status,
        candidate_file_count: candidates.len(),
        rewrite_group_count: rewrite_groups.len(),
        manual_review_count,
        committed_metadata_location: None,
        row_level_planning,
        rewrite_groups,
        snapshot_reports,
    })
}

async fn compaction_data_file_candidates<B>(
    backend: &B,
    table_bucket: &str,
    namespace: &Namespace,
    table: &IdentifierSegment,
    warehouse_object_prefix: Option<&str>,
    manifest_list: &str,
    config: &TableCompactionPlanningConfig,
) -> TableCatalogStoreResult<CompactionManifestPlanning>
where
    B: TableCatalogObjectBackend,
{
    let Some(manifest_list_key) = table_catalog_object_key_from_location(table_bucket, manifest_list) else {
        return Err(TableCatalogStoreError::Invalid(
            "compaction manifest list must be inside the table bucket".to_string(),
        ));
    };
    if table_maintenance_object_kind(namespace, table, warehouse_object_prefix, &manifest_list_key)
        != Some(TableMetadataMaintenanceObjectKind::ManifestList)
    {
        return Err(TableCatalogStoreError::Invalid(
            "compaction manifest list must be inside the table metadata directory".to_string(),
        ));
    }
    let Some(manifest_list_object) = backend.read_object(table_bucket, &manifest_list_key).await? else {
        return Err(TableCatalogStoreError::NotFound(format!("compaction manifest list {manifest_list_key}")));
    };
    let manifest_paths = manifest_paths_from_manifest_list_avro(&manifest_list_object.data)?;
    let mut planning = CompactionManifestPlanning::default();
    for manifest_location in manifest_paths {
        let Some(manifest_key) = table_catalog_object_key_from_location(table_bucket, &manifest_location) else {
            return Err(TableCatalogStoreError::Invalid(
                "compaction manifest must be inside the table bucket".to_string(),
            ));
        };
        if table_maintenance_object_kind(namespace, table, warehouse_object_prefix, &manifest_key)
            != Some(TableMetadataMaintenanceObjectKind::ManifestFile)
        {
            return Err(TableCatalogStoreError::Invalid(
                "compaction manifest must be inside the table metadata directory".to_string(),
            ));
        }
        let Some(manifest_object) = backend.read_object(table_bucket, &manifest_key).await? else {
            return Err(TableCatalogStoreError::NotFound(format!("compaction manifest {manifest_key}")));
        };
        for reference in data_file_references_from_manifest_avro(&manifest_object.data)? {
            if reference.object_kind != TableMetadataMaintenanceObjectKind::DataFile {
                record_compaction_row_level_delete_file(
                    backend,
                    table_bucket,
                    namespace,
                    table,
                    warehouse_object_prefix,
                    &mut planning.row_level_planning,
                    &reference,
                )
                .await?;
                continue;
            }
            validate_compaction_manifest_entry_status(reference.entry_status)?;
            let Some(data_key) = table_catalog_object_key_from_location(table_bucket, &reference.location) else {
                return Err(TableCatalogStoreError::Invalid(
                    "compaction data file must be inside the table bucket".to_string(),
                ));
            };
            if table_maintenance_object_kind(namespace, table, warehouse_object_prefix, &data_key)
                != Some(TableMetadataMaintenanceObjectKind::DataFile)
            {
                return Err(TableCatalogStoreError::Invalid(
                    "compaction data file must be inside the table data directory".to_string(),
                ));
            }
            let Some(data_object) = backend.read_object(table_bucket, &data_key).await? else {
                return Err(TableCatalogStoreError::NotFound(format!("compaction data file {data_key}")));
            };
            let size_bytes = u64::try_from(data_object.data.len()).unwrap_or(u64::MAX);
            if size_bytes <= config.small_file_threshold_bytes {
                planning.candidates.push(TableCompactionDataFileCandidate {
                    rewrite_prefix: compaction_data_file_rewrite_prefix(namespace, table, warehouse_object_prefix, &data_key)
                        .unwrap_or_else(|| data_key.clone()),
                    location: data_key,
                    size_bytes,
                    sort_order_id: reference.sort_order_id,
                });
            }
        }
    }
    Ok(planning)
}

async fn record_compaction_row_level_delete_file<B>(
    backend: &B,
    table_bucket: &str,
    namespace: &Namespace,
    table: &IdentifierSegment,
    warehouse_object_prefix: Option<&str>,
    planning: &mut TableRowLevelMaintenancePlanningReport,
    reference: &ManifestDataFileReference,
) -> TableCatalogStoreResult<()>
where
    B: TableCatalogObjectBackend,
{
    let Some(content) = row_level_delete_file_content(reference.content) else {
        return Ok(());
    };
    let Some(delete_key) = table_catalog_object_key_from_location(table_bucket, &reference.location) else {
        return Err(TableCatalogStoreError::Invalid(
            "compaction delete file must be inside the table bucket".to_string(),
        ));
    };
    if table_maintenance_object_kind(namespace, table, warehouse_object_prefix, &delete_key)
        != Some(TableMetadataMaintenanceObjectKind::DeleteFile)
    {
        return Err(TableCatalogStoreError::Invalid(
            "compaction delete file must be inside the table delete directory".to_string(),
        ));
    }

    let object_exists = backend.read_object(table_bucket, &delete_key).await?.is_some();
    planning.status = TableRowLevelMaintenancePlanningStatus::ManualReviewRequired;
    planning.delete_file_count = planning.delete_file_count.saturating_add(1);
    planning.manual_review_count = planning.manual_review_count.saturating_add(1);
    push_row_level_planning_reason(planning, TableRowLevelMaintenancePlanningReason::DeleteFileRewriteUnsupported);
    match content {
        TableRowLevelDeleteFileContent::PositionDelete => {
            planning.position_delete_file_count = planning.position_delete_file_count.saturating_add(1);
            push_row_level_planning_reason(planning, TableRowLevelMaintenancePlanningReason::PositionDeleteFile);
        }
        TableRowLevelDeleteFileContent::EqualityDelete => {
            planning.equality_delete_file_count = planning.equality_delete_file_count.saturating_add(1);
            push_row_level_planning_reason(planning, TableRowLevelMaintenancePlanningReason::EqualityDeleteFile);
        }
    }
    if !object_exists {
        push_row_level_planning_reason(planning, TableRowLevelMaintenancePlanningReason::MissingDeleteFile);
    }
    planning.delete_files.push(TableRowLevelDeleteFilePlanningReport {
        file_location: delete_key,
        content,
        object_exists,
        record_count: reference.record_count,
        file_size_bytes: reference.file_size_bytes,
        sequence_number: reference.sequence_number,
        file_sequence_number: reference.file_sequence_number,
    });
    Ok(())
}

fn row_level_delete_file_content(content: ManifestDataFileContent) -> Option<TableRowLevelDeleteFileContent> {
    match content {
        ManifestDataFileContent::Data => None,
        ManifestDataFileContent::PositionDelete => Some(TableRowLevelDeleteFileContent::PositionDelete),
        ManifestDataFileContent::EqualityDelete => Some(TableRowLevelDeleteFileContent::EqualityDelete),
    }
}

fn push_row_level_planning_reason(
    planning: &mut TableRowLevelMaintenancePlanningReport,
    reason: TableRowLevelMaintenancePlanningReason,
) {
    if !planning.reasons.contains(&reason) {
        planning.reasons.push(reason);
    }
}

fn compaction_row_level_planning_reasons(
    planning: &TableRowLevelMaintenancePlanningReport,
) -> Vec<TableCompactionPlanningReason> {
    let mut reasons = vec![
        TableCompactionPlanningReason::ManifestList,
        TableCompactionPlanningReason::ManifestFile,
        TableCompactionPlanningReason::DeleteFile,
        TableCompactionPlanningReason::RowLevelRewriteUnsupported,
    ];
    if planning.position_delete_file_count > 0 {
        reasons.push(TableCompactionPlanningReason::PositionDeleteFile);
    }
    if planning.equality_delete_file_count > 0 {
        reasons.push(TableCompactionPlanningReason::EqualityDeleteFile);
    }
    reasons
}

async fn compaction_current_data_files<B>(
    backend: &B,
    table_bucket: &str,
    namespace: &Namespace,
    table: &IdentifierSegment,
    entry: &TableEntry,
    current_metadata: &serde_json::Value,
) -> TableCatalogStoreResult<Vec<CompactedDataFile>>
where
    B: TableCatalogObjectBackend,
{
    let current_snapshot_id = current_metadata
        .get("current-snapshot-id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| TableCatalogStoreError::Invalid("compaction requires current snapshot metadata".to_string()))?;
    let current_snapshot = current_metadata
        .get("snapshots")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .find(|snapshot| {
            snapshot
                .get("snapshot-id")
                .and_then(serde_json::Value::as_i64)
                .is_some_and(|snapshot_id| snapshot_id == current_snapshot_id)
        })
        .ok_or_else(|| TableCatalogStoreError::Invalid("compaction requires current snapshot entry".to_string()))?;
    let manifest_list = current_snapshot
        .get("manifest-list")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| TableCatalogStoreError::Invalid("compaction requires current snapshot manifest list".to_string()))?;

    let warehouse_object_prefix = table_warehouse_object_prefix(entry).ok();
    let Some(manifest_list_key) = table_catalog_object_key_from_location(table_bucket, manifest_list) else {
        return Err(TableCatalogStoreError::Invalid(
            "compaction manifest list must be inside the table bucket".to_string(),
        ));
    };
    if table_maintenance_object_kind(namespace, table, warehouse_object_prefix.as_deref(), &manifest_list_key)
        != Some(TableMetadataMaintenanceObjectKind::ManifestList)
    {
        return Err(TableCatalogStoreError::Invalid(
            "compaction manifest list must be inside the table metadata directory".to_string(),
        ));
    }
    let Some(manifest_list_object) = backend.read_object(table_bucket, &manifest_list_key).await? else {
        return Err(TableCatalogStoreError::NotFound(format!("compaction manifest list {manifest_list_key}")));
    };

    let mut data_files = Vec::new();
    for manifest_reference in manifest_list_references_from_manifest_list_avro(&manifest_list_object.data)? {
        let Some(manifest_key) = table_catalog_object_key_from_location(table_bucket, &manifest_reference.manifest_path) else {
            return Err(TableCatalogStoreError::Invalid(
                "compaction manifest must be inside the table bucket".to_string(),
            ));
        };
        if table_maintenance_object_kind(namespace, table, warehouse_object_prefix.as_deref(), &manifest_key)
            != Some(TableMetadataMaintenanceObjectKind::ManifestFile)
        {
            return Err(TableCatalogStoreError::Invalid(
                "compaction manifest must be inside the table metadata directory".to_string(),
            ));
        }
        let Some(manifest_object) = backend.read_object(table_bucket, &manifest_key).await? else {
            return Err(TableCatalogStoreError::NotFound(format!("compaction manifest {manifest_key}")));
        };
        for reference in data_file_references_from_manifest_avro(&manifest_object.data)? {
            if reference.object_kind != TableMetadataMaintenanceObjectKind::DataFile {
                return Err(TableCatalogStoreError::Invalid(
                    "compaction currently does not support delete files".to_string(),
                ));
            }
            validate_compaction_manifest_entry_status(reference.entry_status)?;
            let Some(data_key) = table_catalog_object_key_from_location(table_bucket, &reference.location) else {
                return Err(TableCatalogStoreError::Invalid(
                    "compaction data file must be inside the table bucket".to_string(),
                ));
            };
            if table_maintenance_object_kind(namespace, table, warehouse_object_prefix.as_deref(), &data_key)
                != Some(TableMetadataMaintenanceObjectKind::DataFile)
            {
                return Err(TableCatalogStoreError::Invalid(
                    "compaction data file must be inside the table data directory".to_string(),
                ));
            }
            let Some(data_object) = backend.read_object(table_bucket, &data_key).await? else {
                return Err(TableCatalogStoreError::NotFound(format!("compaction data file {data_key}")));
            };
            let snapshot_id = reference
                .snapshot_id
                .or(manifest_reference.added_snapshot_id)
                .ok_or_else(|| {
                    TableCatalogStoreError::Invalid("compaction manifest data file missing snapshot id".to_string())
                })?;
            let sequence_number = reference
                .sequence_number
                .or(manifest_reference.sequence_number)
                .ok_or_else(|| {
                    TableCatalogStoreError::Invalid("compaction manifest data file missing sequence number".to_string())
                })?;
            let file_sequence_number = reference
                .file_sequence_number
                .or(manifest_reference.sequence_number)
                .ok_or_else(|| {
                    TableCatalogStoreError::Invalid("compaction manifest data file missing file sequence number".to_string())
                })?;
            data_files.push(CompactedDataFile {
                object_key: data_key,
                file_path: reference.location,
                file_size_bytes: reference
                    .file_size_bytes
                    .unwrap_or_else(|| u64::try_from(data_object.data.len()).unwrap_or(u64::MAX)),
                record_count: match reference.record_count {
                    Some(record_count) => record_count,
                    None => parquet_record_count(&data_object.data)?,
                },
                partition_spec_id: manifest_reference.partition_spec_id.unwrap_or(0),
                partition: reference.partition,
                sort_order_id: reference.sort_order_id,
                status: 0,
                snapshot_id,
                sequence_number,
                file_sequence_number,
            });
        }
    }

    Ok(data_files)
}

fn compaction_rewrite_groups(
    candidates: &[TableCompactionDataFileCandidate],
    config: &TableCompactionPlanningConfig,
) -> Vec<TableCompactionRewriteGroup> {
    let mut groups = Vec::new();
    let mut candidates_by_prefix = BTreeMap::<(&str, Option<i32>), Vec<&TableCompactionDataFileCandidate>>::new();
    for candidate in candidates {
        candidates_by_prefix
            .entry((candidate.rewrite_prefix.as_str(), candidate.sort_order_id))
            .or_default()
            .push(candidate);
    }

    for ((_, sort_order_id), prefix_candidates) in candidates_by_prefix {
        push_compaction_rewrite_groups_for_prefix(&mut groups, prefix_candidates.as_slice(), sort_order_id, config);
    }
    groups
}

fn push_compaction_rewrite_groups_for_prefix(
    groups: &mut Vec<TableCompactionRewriteGroup>,
    candidates: &[&TableCompactionDataFileCandidate],
    sort_order_id: Option<i32>,
    config: &TableCompactionPlanningConfig,
) {
    let mut current_locations = Vec::new();
    let mut current_bytes = 0_u64;
    for candidate in candidates {
        let next_bytes = current_bytes.saturating_add(candidate.size_bytes);
        if !current_locations.is_empty() && next_bytes > config.max_rewrite_bytes_per_job {
            push_compaction_rewrite_group(groups, &mut current_locations, &mut current_bytes, sort_order_id, config);
        }
        current_locations.push(candidate.location.clone());
        current_bytes = current_bytes.saturating_add(candidate.size_bytes);
    }
    push_compaction_rewrite_group(groups, &mut current_locations, &mut current_bytes, sort_order_id, config);
}

fn compaction_data_file_rewrite_prefix(
    namespace: &Namespace,
    table: &IdentifierSegment,
    warehouse_object_prefix: Option<&str>,
    location: &str,
) -> Option<String> {
    let warehouse_data_prefix = warehouse_object_prefix
        .map(|prefix| format!("{prefix}{DATA_DIR}"))
        .unwrap_or_else(|| default_table_data_dir_path(namespace, table));
    let default_data_prefix = format!("{}/", default_table_data_dir_path(namespace, table));
    if let Some(relative_path) = location.strip_prefix(&default_data_prefix) {
        return Some(compaction_data_file_output_prefix(&warehouse_data_prefix, relative_path));
    }
    if let Some(warehouse_object_prefix) = warehouse_object_prefix {
        let warehouse_input_prefix = format!("{warehouse_object_prefix}{DATA_DIR}/");
        if let Some(relative_path) = location.strip_prefix(&warehouse_input_prefix) {
            return Some(compaction_data_file_output_prefix(&warehouse_data_prefix, relative_path));
        }
    }
    None
}

fn compaction_data_file_output_prefix(output_data_prefix: &str, relative_path: &str) -> String {
    relative_path
        .rsplit_once('/')
        .map(|(partition_path, _)| format!("{output_data_prefix}/{partition_path}"))
        .unwrap_or_else(|| output_data_prefix.to_string())
}

fn push_compaction_rewrite_group(
    groups: &mut Vec<TableCompactionRewriteGroup>,
    current_locations: &mut Vec<String>,
    current_bytes: &mut u64,
    sort_order_id: Option<i32>,
    config: &TableCompactionPlanningConfig,
) {
    if current_locations.len() >= config.min_input_files {
        let input_file_count = current_locations.len();
        groups.push(TableCompactionRewriteGroup {
            group_id: format!("{:04}", groups.len() + 1),
            sort_order_id,
            input_file_locations: std::mem::take(current_locations),
            input_file_count,
            input_bytes: *current_bytes,
            output_file_location: None,
            output_bytes: None,
        });
    } else {
        current_locations.clear();
    }
    *current_bytes = 0;
}

fn compaction_rewrite_group_partition(
    data_files_by_key: &BTreeMap<&str, &CompactedDataFile>,
    rewrite_group: &TableCompactionRewriteGroup,
) -> TableCatalogStoreResult<(i32, Vec<(String, apache_avro::types::Value)>)> {
    let mut partition_spec_id = None;
    let mut partition = None;
    for input in &rewrite_group.input_file_locations {
        let Some(data_file) = data_files_by_key.get(input.as_str()) else {
            return Err(TableCatalogStoreError::Invalid(
                "compaction rewrite input is missing from current manifest".to_string(),
            ));
        };
        match (partition_spec_id, partition.as_ref()) {
            (None, None) => {
                partition_spec_id = Some(data_file.partition_spec_id);
                partition = Some(data_file.partition.clone());
            }
            (Some(expected_spec_id), Some(expected_partition))
                if expected_spec_id == data_file.partition_spec_id && expected_partition == &data_file.partition => {}
            _ => {
                return Err(TableCatalogStoreError::Invalid(
                    "compaction rewrite group must contain a single partition tuple".to_string(),
                ));
            }
        }
    }
    Ok((partition_spec_id.unwrap_or(0), partition.unwrap_or_default()))
}

fn compaction_rewrite_group_sort_order(
    data_files_by_key: &BTreeMap<&str, &CompactedDataFile>,
    rewrite_group: &TableCompactionRewriteGroup,
) -> TableCatalogStoreResult<Option<i32>> {
    let mut sort_order_id = None;
    let mut initialized = false;
    for input in &rewrite_group.input_file_locations {
        let Some(data_file) = data_files_by_key.get(input.as_str()) else {
            return Err(TableCatalogStoreError::Invalid(
                "compaction rewrite input is missing from current manifest".to_string(),
            ));
        };
        if !initialized {
            sort_order_id = data_file.sort_order_id;
            initialized = true;
        } else if sort_order_id != data_file.sort_order_id {
            return Err(TableCatalogStoreError::Invalid(
                "compaction rewrite group must contain a single sort order".to_string(),
            ));
        }
    }
    if rewrite_group.sort_order_id != sort_order_id {
        return Err(TableCatalogStoreError::Invalid(
            "compaction rewrite group sort order changed after planning".to_string(),
        ));
    }
    Ok(sort_order_id)
}

fn compaction_manifest_partition_spec_id(data_files: &[CompactedDataFile]) -> TableCatalogStoreResult<i32> {
    let Some(first) = data_files.first() else {
        return Ok(0);
    };
    let partition_spec_id = first.partition_spec_id;
    if data_files
        .iter()
        .any(|data_file| data_file.partition_spec_id != partition_spec_id)
    {
        return Err(TableCatalogStoreError::Invalid(
            "compaction manifest cannot mix partition spec ids".to_string(),
        ));
    }
    Ok(partition_spec_id)
}

fn compact_parquet_data_files(input_files: &[(String, Vec<u8>)]) -> TableCatalogStoreResult<CompactedParquetFile> {
    let mut schema: Option<SchemaRef> = None;
    let mut batches = Vec::new();
    let mut record_count = 0_u64;

    for (location, data) in input_files {
        let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(data.clone())).map_err(|err| {
            TableCatalogStoreError::Invalid(format!("failed to read compaction input parquet {location}: {err}"))
        })?;
        let file_schema = builder.schema().clone();
        match schema.as_ref() {
            Some(expected_schema) if expected_schema.as_ref() != file_schema.as_ref() => {
                return Err(TableCatalogStoreError::Invalid("compaction input parquet schemas must match".to_string()));
            }
            Some(_) => {}
            None => schema = Some(file_schema),
        }

        let reader = builder.build().map_err(|err| {
            TableCatalogStoreError::Invalid(format!("failed to build compaction parquet reader {location}: {err}"))
        })?;
        for batch in reader {
            let batch = batch.map_err(|err| {
                TableCatalogStoreError::Invalid(format!("failed to read compaction parquet batch {location}: {err}"))
            })?;
            record_count = record_count.saturating_add(u64::try_from(batch.num_rows()).unwrap_or(u64::MAX));
            batches.push(batch);
        }
    }

    let Some(schema) = schema else {
        return Err(TableCatalogStoreError::Invalid(
            "compaction requires at least one parquet input file".to_string(),
        ));
    };
    let mut data = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut data, schema, None)
            .map_err(|err| TableCatalogStoreError::Internal(format!("failed to build compaction parquet writer: {err}")))?;
        for batch in batches {
            writer
                .write(&batch)
                .map_err(|err| TableCatalogStoreError::Internal(format!("failed to write compaction parquet batch: {err}")))?;
        }
        writer
            .close()
            .map_err(|err| TableCatalogStoreError::Internal(format!("failed to close compaction parquet writer: {err}")))?;
    }

    Ok(CompactedParquetFile { data, record_count })
}

fn validate_compaction_manifest_entry_status(entry_status: Option<i32>) -> TableCatalogStoreResult<()> {
    match entry_status {
        Some(0 | 1) => Ok(()),
        Some(2) => Err(TableCatalogStoreError::Invalid(
            "compaction currently does not support deleted manifest entries".to_string(),
        )),
        Some(_) => Err(TableCatalogStoreError::Invalid(
            "compaction manifest entry status is unsupported".to_string(),
        )),
        None => Err(TableCatalogStoreError::Invalid("compaction manifest entry missing status".to_string())),
    }
}

fn parquet_record_count(data: &[u8]) -> TableCatalogStoreResult<u64> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(data))
        .map_err(|err| TableCatalogStoreError::Invalid(format!("failed to read compaction parquet metadata: {err}")))?;
    u64::try_from(builder.metadata().file_metadata().num_rows())
        .map_err(|_| TableCatalogStoreError::Invalid("compaction parquet record count must not be negative".to_string()))
}

fn compacted_manifest_list_avro_bytes(summary: CompactionManifestListSummary<'_>) -> TableCatalogStoreResult<Vec<u8>> {
    let schema = apache_avro::Schema::parse_str(
        r#"
        {
          "type": "record",
          "name": "manifest_file",
          "fields": [
            {"name": "manifest_path", "type": "string"},
            {"name": "manifest_length", "type": "long"},
            {"name": "partition_spec_id", "type": "int"},
            {"name": "content", "type": "int"},
            {"name": "sequence_number", "type": "long"},
            {"name": "min_sequence_number", "type": "long"},
            {"name": "added_snapshot_id", "type": "long"},
            {"name": "added_files_count", "type": "int"},
            {"name": "existing_files_count", "type": "int"},
            {"name": "deleted_files_count", "type": "int"},
            {"name": "added_rows_count", "type": "long"},
            {"name": "existing_rows_count", "type": "long"},
            {"name": "deleted_rows_count", "type": "long"},
            {"name": "partitions", "type": ["null", {"type": "array", "items": {"type": "record", "name": "field_summary", "fields": [
              {"name": "contains_null", "type": "boolean"},
              {"name": "lower_bound", "type": ["null", "bytes"], "default": null},
              {"name": "upper_bound", "type": ["null", "bytes"], "default": null}
            ]}}], "default": null}
          ]
        }
        "#,
    )
    .map_err(|err| TableCatalogStoreError::Internal(format!("failed to build compaction manifest list schema: {err}")))?;
    let mut writer = apache_avro::Writer::new(&schema, Vec::new());
    writer
        .append(apache_avro::types::Value::Record(vec![
            (
                "manifest_path".to_string(),
                apache_avro::types::Value::String(summary.manifest_path.to_string()),
            ),
            (
                "manifest_length".to_string(),
                apache_avro::types::Value::Long(i64::try_from(summary.manifest_length).unwrap_or(i64::MAX)),
            ),
            ("partition_spec_id".to_string(), apache_avro::types::Value::Int(summary.partition_spec_id)),
            ("content".to_string(), apache_avro::types::Value::Int(0)),
            ("sequence_number".to_string(), apache_avro::types::Value::Long(summary.sequence_number)),
            (
                "min_sequence_number".to_string(),
                apache_avro::types::Value::Long(summary.sequence_number),
            ),
            ("added_snapshot_id".to_string(), apache_avro::types::Value::Long(summary.snapshot_id)),
            (
                "added_files_count".to_string(),
                apache_avro::types::Value::Int(i32::try_from(summary.added_files_count).unwrap_or(i32::MAX)),
            ),
            (
                "existing_files_count".to_string(),
                apache_avro::types::Value::Int(i32::try_from(summary.existing_files_count).unwrap_or(i32::MAX)),
            ),
            ("deleted_files_count".to_string(), apache_avro::types::Value::Int(0)),
            (
                "added_rows_count".to_string(),
                apache_avro::types::Value::Long(i64::try_from(summary.added_rows_count).unwrap_or(i64::MAX)),
            ),
            (
                "existing_rows_count".to_string(),
                apache_avro::types::Value::Long(i64::try_from(summary.existing_rows_count).unwrap_or(i64::MAX)),
            ),
            ("deleted_rows_count".to_string(), apache_avro::types::Value::Long(0)),
            (
                "partitions".to_string(),
                apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null)),
            ),
        ]))
        .map_err(|err| TableCatalogStoreError::Internal(format!("failed to write compaction manifest list: {err}")))?;
    writer
        .into_inner()
        .map_err(|err| TableCatalogStoreError::Internal(format!("failed to flush compaction manifest list: {err}")))
}

fn compacted_manifest_avro_schema(data_files: &[CompactedDataFile]) -> TableCatalogStoreResult<apache_avro::Schema> {
    let partition_fields = compaction_partition_schema_fields(data_files)?;
    let partition_schema_fields = partition_fields
        .into_iter()
        .map(|(name, field_type)| {
            serde_json::json!({
                "name": name,
                "type": field_type
            })
        })
        .collect::<Vec<_>>();
    let schema = serde_json::json!({
        "type": "record",
        "name": "manifest_entry",
        "fields": [
            {"name": "status", "type": "int"},
            {"name": "snapshot_id", "type": "long"},
            {"name": "sequence_number", "type": "long"},
            {"name": "file_sequence_number", "type": "long"},
            {
                "name": "data_file",
                "type": {
                    "type": "record",
                    "name": "data_file",
                    "fields": [
                        {"name": "content", "type": "int"},
                        {"name": "file_path", "type": "string"},
                        {"name": "file_format", "type": "string"},
                        {
                            "name": "partition",
                            "type": {
                                "type": "record",
                                "name": "partition",
                                "fields": partition_schema_fields
                            }
                        },
                        {"name": "record_count", "type": "long"},
                        {"name": "file_size_in_bytes", "type": "long"},
                        {"name": "column_sizes", "type": ["null", {"type": "map", "values": "long"}], "default": null},
                        {"name": "value_counts", "type": ["null", {"type": "map", "values": "long"}], "default": null},
                        {"name": "null_value_counts", "type": ["null", {"type": "map", "values": "long"}], "default": null},
                        {"name": "nan_value_counts", "type": ["null", {"type": "map", "values": "long"}], "default": null},
                        {"name": "lower_bounds", "type": ["null", {"type": "map", "values": "bytes"}], "default": null},
                        {"name": "upper_bounds", "type": ["null", {"type": "map", "values": "bytes"}], "default": null},
                        {"name": "key_metadata", "type": ["null", "bytes"], "default": null},
                        {"name": "split_offsets", "type": ["null", {"type": "array", "items": "long"}], "default": null},
                        {"name": "equality_ids", "type": ["null", {"type": "array", "items": "int"}], "default": null},
                        {"name": "sort_order_id", "type": ["null", "int"], "default": null}
                    ]
                }
            }
        ]
    });
    apache_avro::Schema::parse_str(&schema.to_string())
        .map_err(|err| TableCatalogStoreError::Internal(format!("failed to build compaction manifest schema: {err}")))
}

fn compaction_partition_schema_fields(
    data_files: &[CompactedDataFile],
) -> TableCatalogStoreResult<Vec<(String, serde_json::Value)>> {
    let Some(first) = data_files.first() else {
        return Ok(Vec::new());
    };
    let mut expected = Vec::with_capacity(first.partition.len());
    for (field_name, field_value) in &first.partition {
        let Some(field_type) = compaction_partition_field_schema(field_value) else {
            return Err(TableCatalogStoreError::Invalid(
                "compaction partition value type is unsupported".to_string(),
            ));
        };
        expected.push((field_name.clone(), field_type));
    }

    for data_file in data_files.iter().skip(1) {
        if data_file.partition.len() != expected.len() {
            return Err(TableCatalogStoreError::Invalid(
                "compaction manifest partition schemas must match".to_string(),
            ));
        }
        for ((expected_name, expected_type), (field_name, field_value)) in expected.iter().zip(&data_file.partition) {
            let Some(field_type) = compaction_partition_field_schema(field_value) else {
                return Err(TableCatalogStoreError::Invalid(
                    "compaction partition value type is unsupported".to_string(),
                ));
            };
            if expected_name != field_name || expected_type != &field_type {
                return Err(TableCatalogStoreError::Invalid(
                    "compaction manifest partition schemas must match".to_string(),
                ));
            }
        }
    }
    Ok(expected)
}

fn compaction_partition_field_schema(value: &apache_avro::types::Value) -> Option<serde_json::Value> {
    match avro_non_union_value(value) {
        apache_avro::types::Value::Boolean(_) => Some(serde_json::json!("boolean")),
        apache_avro::types::Value::Int(_) => Some(serde_json::json!("int")),
        apache_avro::types::Value::Long(_) => Some(serde_json::json!("long")),
        apache_avro::types::Value::Float(_) => Some(serde_json::json!("float")),
        apache_avro::types::Value::Double(_) => Some(serde_json::json!("double")),
        apache_avro::types::Value::Bytes(_) => Some(serde_json::json!("bytes")),
        apache_avro::types::Value::String(_) => Some(serde_json::json!("string")),
        apache_avro::types::Value::Date(_) => Some(serde_json::json!({"type": "int", "logicalType": "date"})),
        apache_avro::types::Value::TimeMillis(_) => Some(serde_json::json!({"type": "int", "logicalType": "time-millis"})),
        apache_avro::types::Value::TimeMicros(_) => Some(serde_json::json!({"type": "long", "logicalType": "time-micros"})),
        apache_avro::types::Value::TimestampMillis(_) => {
            Some(serde_json::json!({"type": "long", "logicalType": "timestamp-millis"}))
        }
        apache_avro::types::Value::TimestampMicros(_) => {
            Some(serde_json::json!({"type": "long", "logicalType": "timestamp-micros"}))
        }
        apache_avro::types::Value::LocalTimestampMillis(_) => {
            Some(serde_json::json!({"type": "long", "logicalType": "local-timestamp-millis"}))
        }
        apache_avro::types::Value::LocalTimestampMicros(_) => {
            Some(serde_json::json!({"type": "long", "logicalType": "local-timestamp-micros"}))
        }
        apache_avro::types::Value::Uuid(_) => Some(serde_json::json!({"type": "string", "logicalType": "uuid"})),
        _ => None,
    }
}

fn compacted_manifest_avro_bytes(data_files: &[CompactedDataFile]) -> TableCatalogStoreResult<Vec<u8>> {
    let schema = compacted_manifest_avro_schema(data_files)?;
    let mut writer = apache_avro::Writer::new(&schema, Vec::new());
    for data_file in data_files {
        let sort_order_id = match data_file.sort_order_id {
            Some(sort_order_id) => apache_avro::types::Value::Union(1, Box::new(apache_avro::types::Value::Int(sort_order_id))),
            None => apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null)),
        };
        writer
            .append(apache_avro::types::Value::Record(vec![
                ("status".to_string(), apache_avro::types::Value::Int(data_file.status)),
                ("snapshot_id".to_string(), apache_avro::types::Value::Long(data_file.snapshot_id)),
                ("sequence_number".to_string(), apache_avro::types::Value::Long(data_file.sequence_number)),
                (
                    "file_sequence_number".to_string(),
                    apache_avro::types::Value::Long(data_file.file_sequence_number),
                ),
                (
                    "data_file".to_string(),
                    apache_avro::types::Value::Record(vec![
                        ("content".to_string(), apache_avro::types::Value::Int(0)),
                        ("file_path".to_string(), apache_avro::types::Value::String(data_file.file_path.clone())),
                        ("file_format".to_string(), apache_avro::types::Value::String("PARQUET".to_string())),
                        ("partition".to_string(), apache_avro::types::Value::Record(data_file.partition.clone())),
                        (
                            "record_count".to_string(),
                            apache_avro::types::Value::Long(i64::try_from(data_file.record_count).unwrap_or(i64::MAX)),
                        ),
                        (
                            "file_size_in_bytes".to_string(),
                            apache_avro::types::Value::Long(i64::try_from(data_file.file_size_bytes).unwrap_or(i64::MAX)),
                        ),
                        (
                            "column_sizes".to_string(),
                            apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null)),
                        ),
                        (
                            "value_counts".to_string(),
                            apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null)),
                        ),
                        (
                            "null_value_counts".to_string(),
                            apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null)),
                        ),
                        (
                            "nan_value_counts".to_string(),
                            apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null)),
                        ),
                        (
                            "lower_bounds".to_string(),
                            apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null)),
                        ),
                        (
                            "upper_bounds".to_string(),
                            apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null)),
                        ),
                        (
                            "key_metadata".to_string(),
                            apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null)),
                        ),
                        (
                            "split_offsets".to_string(),
                            apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null)),
                        ),
                        (
                            "equality_ids".to_string(),
                            apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null)),
                        ),
                        ("sort_order_id".to_string(), sort_order_id),
                    ]),
                ),
            ]))
            .map_err(|err| TableCatalogStoreError::Internal(format!("failed to write compaction manifest: {err}")))?;
    }
    writer
        .into_inner()
        .map_err(|err| TableCatalogStoreError::Internal(format!("failed to flush compaction manifest: {err}")))
}

fn compaction_snapshot_id(current_metadata: &serde_json::Value, entry: &TableEntry, now: OffsetDateTime) -> i64 {
    let generation = i64::try_from(entry.generation).unwrap_or(i64::MAX);
    let mut snapshot_id = unix_timestamp_millis(now).saturating_mul(1000).saturating_add(generation);
    let existing_snapshot_ids = current_metadata
        .get("snapshots")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|snapshot| snapshot.get("snapshot-id").and_then(serde_json::Value::as_i64))
        .collect::<BTreeSet<_>>();
    while existing_snapshot_ids.contains(&snapshot_id) {
        snapshot_id = snapshot_id.saturating_add(1);
    }
    snapshot_id
}

fn next_compaction_sequence_number(current_metadata: &serde_json::Value) -> i64 {
    current_metadata
        .get("last-sequence-number")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0)
        .saturating_add(1)
}

fn compaction_metadata_json(
    current_metadata: &serde_json::Value,
    entry: &TableEntry,
    snapshot_id: i64,
    sequence_number: i64,
    manifest_list: &str,
    previous_metadata_location: &str,
    now: OffsetDateTime,
) -> TableCatalogStoreResult<Vec<u8>> {
    let mut metadata = current_metadata.clone();
    let now_ms = unix_timestamp_millis(now);
    let Some(metadata_object) = metadata.as_object_mut() else {
        return Err(TableCatalogStoreError::Invalid(
            "compaction metadata source must be a JSON object".to_string(),
        ));
    };
    metadata_object.insert("last-sequence-number".to_string(), serde_json::json!(sequence_number));
    metadata_object.insert("last-updated-ms".to_string(), serde_json::json!(now_ms));
    metadata_object.insert("current-snapshot-id".to_string(), serde_json::json!(snapshot_id));

    let snapshots = metadata_object
        .entry("snapshots".to_string())
        .or_insert_with(|| serde_json::json!([]));
    let Some(snapshots) = snapshots.as_array_mut() else {
        return Err(TableCatalogStoreError::Invalid(
            "compaction metadata snapshots must be an array".to_string(),
        ));
    };
    snapshots.push(serde_json::json!({
        "snapshot-id": snapshot_id,
        "sequence-number": sequence_number,
        "timestamp-ms": now_ms,
        "manifest-list": manifest_list,
        "summary": {
            "operation": "rewrite",
            "rustfs.maintenance": "compaction"
        }
    }));

    let snapshot_log = metadata_object
        .entry("snapshot-log".to_string())
        .or_insert_with(|| serde_json::json!([]));
    let Some(snapshot_log) = snapshot_log.as_array_mut() else {
        return Err(TableCatalogStoreError::Invalid(
            "compaction metadata snapshot log must be an array".to_string(),
        ));
    };
    snapshot_log.push(serde_json::json!({
        "timestamp-ms": now_ms,
        "snapshot-id": snapshot_id
    }));

    let metadata_log = metadata_object
        .entry("metadata-log".to_string())
        .or_insert_with(|| serde_json::json!([]));
    let Some(metadata_log) = metadata_log.as_array_mut() else {
        return Err(TableCatalogStoreError::Invalid("compaction metadata log must be an array".to_string()));
    };
    metadata_log.push(serde_json::json!({
        "timestamp-ms": now_ms,
        "metadata-file": previous_metadata_location
    }));

    let refs = metadata_object
        .entry("refs".to_string())
        .or_insert_with(|| serde_json::json!({}));
    let Some(refs) = refs.as_object_mut() else {
        return Err(TableCatalogStoreError::Invalid("compaction metadata refs must be an object".to_string()));
    };
    refs.insert(
        ICEBERG_MAIN_REF.to_string(),
        serde_json::json!({
            "snapshot-id": snapshot_id,
            "type": "branch"
        }),
    );
    metadata_object
        .entry("location".to_string())
        .or_insert_with(|| serde_json::json!(entry.warehouse_location));

    serde_json::to_vec(&metadata)
        .map_err(|err| TableCatalogStoreError::Internal(format!("failed to serialize compaction metadata: {err}")))
}

fn snapshot_expiration_drafts(
    current_metadata: &serde_json::Value,
    current_snapshot_id: Option<i64>,
) -> Vec<TableSnapshotExpirationDraft> {
    let Some(snapshots) = current_metadata.get("snapshots").and_then(serde_json::Value::as_array) else {
        return Vec::new();
    };

    snapshots
        .iter()
        .map(|snapshot| {
            let snapshot_id = snapshot.get("snapshot-id").and_then(serde_json::Value::as_i64);
            let timestamp_ms = snapshot.get("timestamp-ms").and_then(serde_json::Value::as_i64);
            let mut reasons = BTreeSet::new();
            if snapshot_id.is_none() {
                reasons.insert(TableSnapshotExpirationReason::MissingSnapshotId);
            }
            if timestamp_ms.is_none() {
                reasons.insert(TableSnapshotExpirationReason::MissingSnapshotTimestamp);
            }
            if snapshot_id.is_some() && snapshot_id == current_snapshot_id {
                reasons.insert(TableSnapshotExpirationReason::CurrentSnapshot);
            }

            TableSnapshotExpirationDraft {
                snapshot_id,
                sequence_number: snapshot.get("sequence-number").and_then(serde_json::Value::as_i64),
                timestamp_ms,
                manifest_list: snapshot
                    .get("manifest-list")
                    .and_then(serde_json::Value::as_str)
                    .map(ToString::to_string),
                reasons,
            }
        })
        .collect()
}

fn mark_recent_snapshots_to_keep(drafts: &mut [TableSnapshotExpirationDraft], min_snapshots_to_keep: usize) {
    let mut snapshots_by_time = drafts
        .iter()
        .enumerate()
        .filter_map(|(index, draft)| Some((draft.timestamp_ms?, index)))
        .collect::<Vec<_>>();
    snapshots_by_time.sort_by(|(left_timestamp, left_index), (right_timestamp, right_index)| {
        right_timestamp.cmp(left_timestamp).then_with(|| left_index.cmp(right_index))
    });

    for (_, index) in snapshots_by_time.into_iter().take(min_snapshots_to_keep) {
        drafts[index]
            .reasons
            .insert(TableSnapshotExpirationReason::MinSnapshotsToKeep);
    }
}

fn snapshot_expiration_ref_state(
    current_metadata: &serde_json::Value,
    current_snapshot_id: Option<i64>,
) -> (BTreeSet<i64>, BTreeSet<i64>, BTreeSet<i64>) {
    let mut protected_ref_snapshot_ids = BTreeSet::new();
    let mut user_defined_ref_snapshot_ids = BTreeSet::new();
    let mut ref_retention_conflict_snapshot_ids = BTreeSet::new();
    let Some(refs) = current_metadata.get("refs").and_then(serde_json::Value::as_object) else {
        return (
            protected_ref_snapshot_ids,
            user_defined_ref_snapshot_ids,
            ref_retention_conflict_snapshot_ids,
        );
    };

    for (name, reference) in refs {
        let Some(snapshot_id) = reference.get("snapshot-id").and_then(serde_json::Value::as_i64) else {
            continue;
        };
        if name != ICEBERG_MAIN_REF || Some(snapshot_id) != current_snapshot_id {
            protected_ref_snapshot_ids.insert(snapshot_id);
        }
        if name != ICEBERG_MAIN_REF {
            user_defined_ref_snapshot_ids.insert(snapshot_id);
        }
        if snapshot_ref_has_retention_policy(reference) {
            ref_retention_conflict_snapshot_ids.insert(snapshot_id);
        }
    }

    (
        protected_ref_snapshot_ids,
        user_defined_ref_snapshot_ids,
        ref_retention_conflict_snapshot_ids,
    )
}

fn snapshot_ref_has_retention_policy(reference: &serde_json::Value) -> bool {
    reference.get(ICEBERG_REF_MIN_SNAPSHOTS_TO_KEEP_FIELD).is_some()
        || reference.get(ICEBERG_REF_MAX_SNAPSHOT_AGE_MS_FIELD).is_some()
        || reference.get(ICEBERG_REF_MAX_REF_AGE_MS_FIELD).is_some()
}

fn snapshot_expiration_table_property_conflicts(
    current_metadata: &serde_json::Value,
    config: &TableSnapshotExpirationConfig,
) -> bool {
    let Some(properties) = current_metadata.get("properties").and_then(serde_json::Value::as_object) else {
        return false;
    };

    if properties.contains_key(ICEBERG_MAX_REF_AGE_MS_PROPERTY) {
        return true;
    }
    if retention_property_conflicts_usize(properties, ICEBERG_MIN_SNAPSHOTS_TO_KEEP_PROPERTY, config.min_snapshots_to_keep) {
        return true;
    }
    retention_property_conflicts_i64(properties, ICEBERG_MAX_SNAPSHOT_AGE_MS_PROPERTY, config.max_snapshot_age_ms)
}

fn retention_property_conflicts_usize(
    properties: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    expected: usize,
) -> bool {
    let Some(value) = properties.get(key) else {
        return false;
    };
    serde_json_i64(value).and_then(|value| usize::try_from(value).ok()) != Some(expected)
}

fn retention_property_conflicts_i64(properties: &serde_json::Map<String, serde_json::Value>, key: &str, expected: i64) -> bool {
    let Some(value) = properties.get(key) else {
        return false;
    };
    serde_json_i64(value) != Some(expected)
}

fn serde_json_i64(value: &serde_json::Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse::<i64>().ok())
}

fn snapshot_expiration_requires_manual_review(reasons: &BTreeSet<TableSnapshotExpirationReason>) -> bool {
    reasons.contains(&TableSnapshotExpirationReason::MissingSnapshotId)
        || reasons.contains(&TableSnapshotExpirationReason::MissingSnapshotTimestamp)
        || reasons.contains(&TableSnapshotExpirationReason::UserDefinedSnapshotRef)
        || reasons.contains(&TableSnapshotExpirationReason::SnapshotRefRetentionConflict)
        || reasons.contains(&TableSnapshotExpirationReason::TableRetentionPropertyConflict)
}

fn snapshot_expiration_is_retained(reasons: &BTreeSet<TableSnapshotExpirationReason>) -> bool {
    reasons.contains(&TableSnapshotExpirationReason::CurrentSnapshot)
        || reasons.contains(&TableSnapshotExpirationReason::MinSnapshotsToKeep)
        || reasons.contains(&TableSnapshotExpirationReason::ProtectedSnapshotRef)
}

fn unix_timestamp_millis(now: OffsetDateTime) -> i64 {
    now.unix_timestamp()
        .saturating_mul(1000)
        .saturating_add(i64::from(now.millisecond()))
}

fn maintenance_timestamp(now: OffsetDateTime) -> String {
    now.format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| now.unix_timestamp().to_string())
}

fn default_table_maintenance_worker_lease_timeout_seconds() -> u64 {
    TABLE_MAINTENANCE_WORKER_LEASE_TIMEOUT_DEFAULT_SECONDS
}

fn parse_maintenance_timestamp(timestamp: &str) -> Option<OffsetDateTime> {
    OffsetDateTime::parse(timestamp, &time::format_description::well_known::Rfc3339).ok()
}

fn table_maintenance_quarantine_operator_reason(action: &str, reason: Option<&str>) -> String {
    let reason = reason.map(str::trim).filter(|reason| !reason.is_empty());
    match reason {
        Some(reason) => format!("maintenance quarantine {action} by operator: {reason}"),
        None => format!("maintenance quarantine {action} by operator"),
    }
}

fn push_table_maintenance_audit_event(
    report: &mut TableMetadataMaintenanceReport,
    timestamp: OffsetDateTime,
    actor: TableMaintenanceAuditActor,
    action: TableMaintenanceAuditAction,
    reason: Option<String>,
    before_status: Option<TableMetadataMaintenanceJobStatus>,
    before_quarantined_object_count: Option<usize>,
) {
    report.audit_events.push(TableMaintenanceAuditEvent {
        timestamp: maintenance_timestamp(timestamp),
        actor,
        action,
        reason,
        before_status,
        after_status: Some(report.job.status.clone()),
        before_quarantined_object_count,
        after_quarantined_object_count: Some(report.job.quarantined_object_count),
        recommended_actions: report.job.recommended_actions.clone(),
    });
}

fn table_maintenance_recommended_actions(job: &TableMetadataMaintenanceJob) -> Vec<TableMaintenanceRecommendedAction> {
    let mut actions = Vec::new();
    match job.status {
        TableMetadataMaintenanceJobStatus::NotYetRun => {}
        TableMetadataMaintenanceJobStatus::Queued => {
            actions.push(TableMaintenanceRecommendedAction::RunMaintenanceWorker);
        }
        TableMetadataMaintenanceJobStatus::Running => {
            actions.push(TableMaintenanceRecommendedAction::WaitForActiveWorker);
        }
        TableMetadataMaintenanceJobStatus::Successful => {
            if matches!(job.operation, TableMetadataMaintenanceOperation::DryRun)
                && (job.deletable_metadata_file_count > 0 || job.deletable_object_count > 0)
            {
                actions.push(TableMaintenanceRecommendedAction::ReviewAndRunDelete);
            } else {
                actions.push(TableMaintenanceRecommendedAction::NoActionRequired);
            }
        }
        TableMetadataMaintenanceJobStatus::Failed => {
            if job
                .failure_reason
                .as_deref()
                .is_some_and(|reason| reason == TABLE_MAINTENANCE_DELETE_DISABLED_REASON)
            {
                actions.push(TableMaintenanceRecommendedAction::EnableDelete);
            }
            if job.quarantine_enabled && job.quarantined_object_count > 0 {
                actions.push(TableMaintenanceRecommendedAction::ReviewQuarantine);
            }
            if job.next_retry_after.is_some() {
                actions.push(TableMaintenanceRecommendedAction::WaitForRetryBackoff);
            }
            if actions.is_empty() {
                actions.push(TableMaintenanceRecommendedAction::InvestigateFailure);
            }
        }
        TableMetadataMaintenanceJobStatus::Disabled => {
            actions.push(TableMaintenanceRecommendedAction::EnableBackgroundMaintenance);
        }
        TableMetadataMaintenanceJobStatus::Paused => {
            actions.push(TableMaintenanceRecommendedAction::ResumeMaintenanceWorker);
        }
    }
    actions
}

fn push_unique_maintenance_action(
    actions: &mut Vec<TableMaintenanceRecommendedAction>,
    action: TableMaintenanceRecommendedAction,
) {
    if !actions.contains(&action) {
        actions.push(action);
    }
}

fn table_maintenance_report_order_timestamp(report: &TableMetadataMaintenanceReport) -> String {
    report
        .job
        .finished_at
        .clone()
        .or_else(|| report.job.heartbeat_at.clone())
        .or_else(|| report.job.started_at.clone())
        .or_else(|| report.job.scheduled_at.clone())
        .unwrap_or_default()
}

fn table_maintenance_scheduler_job_summary(report: &TableMetadataMaintenanceReport) -> TableMaintenanceSchedulerJobSummary {
    TableMaintenanceSchedulerJobSummary {
        job_id: report.job.job_id.clone(),
        operation: report.job.operation.clone(),
        status: report.job.status.clone(),
        scheduler_id: report.job.scheduler_id.clone(),
        scheduled_at: report.job.scheduled_at.clone(),
        worker_id: report.job.worker_id.clone(),
        attempt: report.job.attempt,
        started_at: report.job.started_at.clone(),
        finished_at: report.job.finished_at.clone(),
        heartbeat_at: report.job.heartbeat_at.clone(),
        next_retry_after: report.job.next_retry_after.clone(),
        recommended_actions: report.job.recommended_actions.clone(),
        audit_events: report.audit_events.clone(),
    }
}

fn table_maintenance_scheduler_quarantine_boundary(
    config: &TableMaintenanceConfig,
    reports: &[TableMetadataMaintenanceReport],
) -> TableMaintenanceSchedulerQuarantineBoundary {
    let source = reports
        .iter()
        .find(|report| report.job.quarantine_enabled && report.job.quarantined_object_count > 0);
    TableMaintenanceSchedulerQuarantineBoundary {
        enabled: config.quarantine_enabled,
        active: source.is_some(),
        retention_seconds: source.map_or(config.quarantine_retention_seconds, |report| report.job.quarantine_retention_seconds),
        quarantined_object_count: source.map_or(0, |report| report.job.quarantined_object_count),
        source_job_id: source.map(|report| report.job.job_id.clone()),
    }
}

fn refresh_table_maintenance_report_recommended_actions(report: &mut TableMetadataMaintenanceReport) {
    report.job.recommended_actions = table_maintenance_recommended_actions(&report.job);
}

fn table_maintenance_report_with_recommended_actions(
    mut report: TableMetadataMaintenanceReport,
) -> TableMetadataMaintenanceReport {
    refresh_table_maintenance_report_recommended_actions(&mut report);
    report
}

fn table_maintenance_scheduler_lease_is_active(
    job: &TableMetadataMaintenanceJob,
    scheduler_lease_timeout_seconds: u64,
    now: OffsetDateTime,
) -> bool {
    let Some(scheduled_at) = job.scheduled_at.as_deref().and_then(parse_maintenance_timestamp) else {
        return false;
    };
    let timeout_seconds = i64::try_from(scheduler_lease_timeout_seconds).unwrap_or(i64::MAX);
    scheduled_at.saturating_add(Duration::seconds(timeout_seconds)) > now
}

fn table_maintenance_job_lease_is_active(
    job: &TableMetadataMaintenanceJob,
    worker_lease_timeout_seconds: u64,
    now: OffsetDateTime,
) -> bool {
    let Some(heartbeat_at) = job.heartbeat_at.as_deref().and_then(parse_maintenance_timestamp) else {
        return false;
    };
    let timeout_seconds = i64::try_from(worker_lease_timeout_seconds).unwrap_or(i64::MAX);
    heartbeat_at.saturating_add(Duration::seconds(timeout_seconds)) > now
}

fn table_maintenance_job_retry_is_pending(job: &TableMetadataMaintenanceJob, now: OffsetDateTime) -> bool {
    if !matches!(job.status, TableMetadataMaintenanceJobStatus::Failed) {
        return false;
    }
    let Some(next_retry_after) = job.next_retry_after.as_deref().and_then(parse_maintenance_timestamp) else {
        return false;
    };
    next_retry_after > now
}

fn validate_catalog_entry_version(kind: &str, version: u16) -> TableCatalogStoreResult<()> {
    if version != TABLE_CATALOG_ENTRY_VERSION {
        return Err(TableCatalogStoreError::Invalid(format!("unsupported {kind} entry version")));
    }
    Ok(())
}

fn validate_table_maintenance_config_version(version: u16) -> TableCatalogStoreResult<()> {
    if version != TABLE_MAINTENANCE_CONFIG_VERSION {
        return Err(TableCatalogStoreError::Invalid(
            "unsupported table maintenance config entry version".to_string(),
        ));
    }
    Ok(())
}

fn validate_table_maintenance_config(config: &TableMaintenanceConfig) -> TableCatalogStoreResult<()> {
    validate_table_maintenance_config_version(config.version)?;
    if config.worker_lease_timeout_seconds == 0 {
        return Err(TableCatalogStoreError::Invalid(
            "worker-lease-timeout-seconds must be greater than zero".to_string(),
        ));
    }
    if config.worker_lease_timeout_seconds > TABLE_MAINTENANCE_WORKER_LEASE_TIMEOUT_MAX_SECONDS {
        return Err(TableCatalogStoreError::Invalid(format!(
            "worker-lease-timeout-seconds cannot exceed {TABLE_MAINTENANCE_WORKER_LEASE_TIMEOUT_MAX_SECONDS}"
        )));
    }
    if config.max_retry_attempts > 10 {
        return Err(TableCatalogStoreError::Invalid("max-retry-attempts cannot exceed 10".to_string()));
    }
    if config.max_retry_attempts > 0 && config.retry_initial_backoff_seconds == 0 {
        return Err(TableCatalogStoreError::Invalid(
            "retry-initial-backoff-seconds must be greater than zero when retry is enabled".to_string(),
        ));
    }
    if config.max_retry_attempts > 0 && config.retry_initial_backoff_seconds > TABLE_MAINTENANCE_RETRY_BACKOFF_MAX_SECONDS {
        return Err(TableCatalogStoreError::Invalid(format!(
            "retry-initial-backoff-seconds cannot exceed {TABLE_MAINTENANCE_RETRY_BACKOFF_MAX_SECONDS}"
        )));
    }
    if config.max_retry_attempts > 0 && config.retry_max_backoff_seconds > TABLE_MAINTENANCE_RETRY_BACKOFF_MAX_SECONDS {
        return Err(TableCatalogStoreError::Invalid(format!(
            "retry-max-backoff-seconds cannot exceed {TABLE_MAINTENANCE_RETRY_BACKOFF_MAX_SECONDS}"
        )));
    }
    if config.max_retry_attempts > 0 && config.retry_max_backoff_seconds < config.retry_initial_backoff_seconds {
        return Err(TableCatalogStoreError::Invalid(
            "retry-max-backoff-seconds must be greater than or equal to retry-initial-backoff-seconds".to_string(),
        ));
    }
    if config.quarantine_enabled && config.quarantine_retention_seconds == 0 {
        return Err(TableCatalogStoreError::Invalid(
            "quarantine-retention-seconds must be greater than zero when quarantine is enabled".to_string(),
        ));
    }
    Ok(())
}

fn apply_maintenance_retry_after(job: &mut TableMetadataMaintenanceJob, config: &TableMaintenanceConfig, now: OffsetDateTime) {
    if config.max_retry_attempts == 0 || job.attempt >= config.max_retry_attempts {
        job.next_retry_after = None;
        return;
    }
    let attempt_index = u32::from(job.attempt.saturating_sub(1));
    let multiplier = 1_u64.checked_shl(attempt_index).unwrap_or(u64::MAX);
    let delay_seconds = config
        .retry_initial_backoff_seconds
        .saturating_mul(multiplier)
        .min(config.retry_max_backoff_seconds);
    let delay_seconds = i64::try_from(delay_seconds).unwrap_or(i64::MAX);
    job.next_retry_after = Some(maintenance_timestamp(now.saturating_add(Duration::seconds(delay_seconds))));
}

fn validate_table_snapshot_expiration_config(config: &TableSnapshotExpirationConfig) -> TableCatalogStoreResult<()> {
    if config.min_snapshots_to_keep == 0 {
        return Err(TableCatalogStoreError::Invalid(
            "min-snapshots-to-keep must be greater than zero".to_string(),
        ));
    }
    if config.max_snapshot_age_ms < 0 {
        return Err(TableCatalogStoreError::Invalid("max-snapshot-age-ms cannot be negative".to_string()));
    }
    Ok(())
}

fn validate_table_compaction_planning_config(config: &TableCompactionPlanningConfig) -> TableCatalogStoreResult<()> {
    if config.target_file_size_bytes == 0 {
        return Err(TableCatalogStoreError::Invalid(
            "target-file-size-bytes must be greater than zero".to_string(),
        ));
    }
    if config.small_file_threshold_bytes == 0 {
        return Err(TableCatalogStoreError::Invalid(
            "small-file-threshold-bytes must be greater than zero".to_string(),
        ));
    }
    if config.small_file_threshold_bytes > config.target_file_size_bytes {
        return Err(TableCatalogStoreError::Invalid(
            "small-file-threshold-bytes cannot exceed target-file-size-bytes".to_string(),
        ));
    }
    if config.min_input_files < 2 {
        return Err(TableCatalogStoreError::Invalid("min-input-files must be at least two".to_string()));
    }
    if config.max_rewrite_bytes_per_job < config.target_file_size_bytes {
        return Err(TableCatalogStoreError::Invalid(
            "max-rewrite-bytes-per-job must be at least target-file-size-bytes".to_string(),
        ));
    }
    Ok(())
}

fn commit_log_matches_request(commit_log: &CommitLogEntry, request: &TableCommitRequest, table_id: &str) -> bool {
    commit_log.version == TABLE_CATALOG_ENTRY_VERSION
        && commit_log.commit_id == request.commit_id
        && commit_log.idempotency_key == request.idempotency_key
        && commit_log.table_id == table_id
        && commit_log.operation == request.operation
        && commit_log.expected_version_token == request.expected_version_token
        && commit_log.previous_metadata_location == request.expected_metadata_location
        && commit_log.new_metadata_location == request.new_metadata_location
        && commit_log.requirements == request.requirements
        && commit_log.writer == request.writer
}

fn table_matches_committed_log(table: &TableEntry, commit_log: &CommitLogEntry) -> bool {
    table.table_id == commit_log.table_id
        && table.metadata_location == commit_log.new_metadata_location
        && table.version_token == commit_log.new_version_token
}

fn table_matches_staged_base(table: &TableEntry, commit_log: &CommitLogEntry) -> bool {
    table.table_id == commit_log.table_id
        && table.metadata_location == commit_log.previous_metadata_location
        && table.version_token == commit_log.expected_version_token
}

fn table_catalog_recovery_summary(
    metadata_status: &TableMetadataPointerStatus,
    commit_recovery: &TableCommitRecoveryReport,
) -> (TableCatalogRecoveryStatus, Vec<TableCatalogRecoveryAction>) {
    let mut actions = Vec::new();
    let metadata_status = match metadata_status {
        TableMetadataPointerStatus::Valid => None,
        TableMetadataPointerStatus::MissingObject => {
            actions.push(TableCatalogRecoveryAction::RestoreCurrentMetadataObject);
            Some(TableCatalogRecoveryStatus::ReadOnlyRecommended)
        }
        TableMetadataPointerStatus::InvalidJson => {
            actions.push(TableCatalogRecoveryAction::FixCurrentMetadataJson);
            Some(TableCatalogRecoveryStatus::ReadOnlyRecommended)
        }
        TableMetadataPointerStatus::InvalidLocation => {
            actions.push(TableCatalogRecoveryAction::MoveCurrentMetadataInsideTable);
            Some(TableCatalogRecoveryStatus::ReadOnlyRecommended)
        }
    };

    if commit_recovery.manual_review_count > 0 {
        actions.push(TableCatalogRecoveryAction::ReviewCommitLog);
        return (metadata_status.unwrap_or(TableCatalogRecoveryStatus::ManualReviewRequired), actions);
    }
    if commit_recovery.finalization_required_count > 0 || commit_recovery.idempotency_repair_required_count > 0 {
        actions.push(TableCatalogRecoveryAction::RunCommitRecovery);
        return (metadata_status.unwrap_or(TableCatalogRecoveryStatus::Recoverable), actions);
    }
    if commit_recovery.staged_before_table_update_count > 0 {
        actions.push(TableCatalogRecoveryAction::RetryCommit);
        return (metadata_status.unwrap_or(TableCatalogRecoveryStatus::Recoverable), actions);
    }

    (metadata_status.unwrap_or(TableCatalogRecoveryStatus::Healthy), actions)
}

fn commit_logs_share_recovery_payload(left: &CommitLogEntry, right: &CommitLogEntry) -> bool {
    left.version == right.version
        && left.commit_id == right.commit_id
        && left.idempotency_key == right.idempotency_key
        && left.table_id == right.table_id
        && left.operation == right.operation
        && left.expected_version_token == right.expected_version_token
        && left.new_version_token == right.new_version_token
        && left.previous_metadata_location == right.previous_metadata_location
        && left.new_metadata_location == right.new_metadata_location
        && left.requirements == right.requirements
        && left.writer == right.writer
}

fn commit_idempotency_index_status(
    commit_log: &CommitLogEntry,
    idempotency_commit: Option<&CommitLogEntry>,
) -> TableCommitIdempotencyIndexStatus {
    match (commit_log.idempotency_key.as_ref(), idempotency_commit) {
        (None, _) => TableCommitIdempotencyIndexStatus::NotRequired,
        (Some(_), None) => TableCommitIdempotencyIndexStatus::Missing,
        (Some(_), Some(indexed)) if indexed == commit_log => TableCommitIdempotencyIndexStatus::Matches,
        (Some(_), Some(indexed)) if commit_logs_share_recovery_payload(indexed, commit_log) => {
            TableCommitIdempotencyIndexStatus::Stale
        }
        (Some(_), Some(_)) => TableCommitIdempotencyIndexStatus::Conflicting,
    }
}

fn table_commit_recovery_entry(
    table: &TableEntry,
    commit_log: &CommitLogEntry,
    idempotency_commit: Option<&CommitLogEntry>,
) -> TableCommitRecoveryEntry {
    let idempotency_index_status = commit_idempotency_index_status(commit_log, idempotency_commit);
    let idempotency_index_present = matches!(
        idempotency_index_status,
        TableCommitIdempotencyIndexStatus::Matches
            | TableCommitIdempotencyIndexStatus::Stale
            | TableCommitIdempotencyIndexStatus::Conflicting
    );
    let idempotency_index_repair_required = matches!(
        idempotency_index_status,
        TableCommitIdempotencyIndexStatus::Missing | TableCommitIdempotencyIndexStatus::Stale
    );

    let (recovery_state, reason) = if matches!(idempotency_index_status, TableCommitIdempotencyIndexStatus::Conflicting) {
        (
            TableCommitRecoveryState::ManualReview,
            "idempotency index points at a different commit payload".to_string(),
        )
    } else if table_matches_committed_log(table, commit_log) {
        if matches!(commit_log.status, CommitLogStatus::Committed) {
            if idempotency_index_repair_required {
                (
                    TableCommitRecoveryState::IdempotencyIndexRepairRequired,
                    "committed table pointer is durable but idempotency index needs repair".to_string(),
                )
            } else {
                (
                    TableCommitRecoveryState::Committed,
                    "commit log and current table pointer agree".to_string(),
                )
            }
        } else {
            (
                TableCommitRecoveryState::FinalizationRequired,
                "current table pointer already advanced but commit log is not finalized".to_string(),
            )
        }
    } else if matches!(commit_log.status, CommitLogStatus::Committed) {
        if idempotency_index_repair_required {
            (
                TableCommitRecoveryState::IdempotencyIndexRepairRequired,
                "historical committed log needs idempotency index repair".to_string(),
            )
        } else {
            (
                TableCommitRecoveryState::Committed,
                "commit is finalized and may be older than the current table pointer".to_string(),
            )
        }
    } else if table_matches_staged_base(table, commit_log) {
        (
            TableCommitRecoveryState::StagedBeforeTableUpdate,
            "staged commit exists but table pointer has not advanced".to_string(),
        )
    } else {
        (
            TableCommitRecoveryState::ManualReview,
            "staged commit no longer matches the current table pointer or its expected base".to_string(),
        )
    };

    TableCommitRecoveryEntry {
        commit_id: commit_log.commit_id.clone(),
        idempotency_key: commit_log.idempotency_key.clone(),
        operation: commit_log.operation.clone(),
        status: commit_log.status.clone(),
        recovery_state,
        previous_metadata_location: commit_log.previous_metadata_location.clone(),
        new_metadata_location: commit_log.new_metadata_location.clone(),
        expected_version_token: commit_log.expected_version_token.clone(),
        new_version_token: commit_log.new_version_token.clone(),
        idempotency_index_present,
        idempotency_index_status,
        reason,
    }
}

fn record_table_commit_attempt(operation: &str) {
    counter!("rustfs_table_catalog_commit_attempts_total", "operation" => operation.to_string()).increment(1);
}

fn table_catalog_store_result_label<T>(result: &TableCatalogStoreResult<T>) -> &'static str {
    match result {
        Ok(_) => "success",
        Err(TableCatalogStoreError::Conflict(_)) => "conflict",
        Err(TableCatalogStoreError::Invalid(_)) => "invalid",
        Err(TableCatalogStoreError::NotFound(_)) => "not_found",
        Err(TableCatalogStoreError::Internal(_)) => "failure",
    }
}

fn duration_millis_u64(duration: StdDuration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn record_table_commit_cas_result(operation: &str, started: Instant, result: &TableCatalogStoreResult<()>) {
    let elapsed = started.elapsed();
    let result_label = table_catalog_store_result_label(result);
    counter!(
        "rustfs_table_catalog_commit_cas_results_total",
        "operation" => operation.to_string(),
        "result" => result_label.to_string()
    )
    .increment(1);
    histogram!(
        "rustfs_table_catalog_commit_cas_duration_seconds",
        "operation" => operation.to_string(),
        "result" => result_label.to_string()
    )
    .record(elapsed.as_secs_f64());
}

fn record_table_commit_result(
    table_bucket: &str,
    namespace: &str,
    table: &str,
    commit_id: &str,
    operation: &str,
    started: Instant,
    result: &TableCatalogStoreResult<TableCommitResult>,
) {
    let elapsed = started.elapsed();
    let result_label = table_catalog_store_result_label(result);
    counter!(
        "rustfs_table_catalog_commit_results_total",
        "operation" => operation.to_string(),
        "result" => result_label.to_string()
    )
    .increment(1);
    if matches!(result, Err(TableCatalogStoreError::Conflict(_))) {
        counter!("rustfs_table_catalog_commit_conflicts_total", "operation" => operation.to_string()).increment(1);
    }
    histogram!(
        "rustfs_table_catalog_commit_duration_seconds",
        "operation" => operation.to_string(),
        "result" => result_label.to_string()
    )
    .record(elapsed.as_secs_f64());

    match result {
        Ok(commit) if elapsed >= TABLE_COMMIT_SLOW_LOG_THRESHOLD => {
            tracing::warn!(
                table_bucket,
                namespace,
                table,
                commit_id,
                operation,
                generation = commit.table.generation,
                duration_ms = duration_millis_u64(elapsed),
                "slow table catalog commit"
            );
        }
        Ok(commit) => {
            tracing::debug!(
                table_bucket,
                namespace,
                table,
                commit_id,
                operation,
                generation = commit.table.generation,
                duration_ms = duration_millis_u64(elapsed),
                "table catalog commit completed"
            );
        }
        Err(error) => {
            tracing::warn!(
                table_bucket,
                namespace,
                table,
                commit_id,
                operation,
                result = result_label,
                duration_ms = duration_millis_u64(elapsed),
                error = %error,
                "table catalog commit did not complete"
            );
        }
    }
}

fn table_commit_result(
    table_bucket: &str,
    namespace: &str,
    table: &str,
    commit_id: &str,
    operation: &str,
    started: Instant,
    result: TableCatalogStoreResult<TableCommitResult>,
) -> TableCatalogStoreResult<TableCommitResult> {
    record_table_commit_result(table_bucket, namespace, table, commit_id, operation, started, &result);
    result
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
mod tests;
