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

use super::super::*;

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

pub(crate) fn table_warehouse_index_entry(entry: &TableEntry) -> TableCatalogStoreResult<TableWarehouseIndexEntry> {
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

pub(crate) fn table_object_s3_location(table_bucket: &str, object_key: &str) -> String {
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

pub(crate) fn table_metadata_warehouse_location(
    table_bucket: &str,
    metadata_location: &str,
    metadata_object: &TableCatalogObject,
) -> TableCatalogStoreResult<Option<String>> {
    metadata_warehouse_location(table_bucket, metadata_location, metadata_object, validate_table_warehouse_location)
}

pub(crate) fn view_metadata_warehouse_location(
    table_bucket: &str,
    metadata_location: &str,
    metadata_object: &TableCatalogObject,
) -> TableCatalogStoreResult<Option<String>> {
    metadata_warehouse_location(table_bucket, metadata_location, metadata_object, validate_view_warehouse_location)
}

pub(crate) fn warehouse_index_candidate_prefixes(object: &str) -> Vec<&str> {
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

pub(crate) fn table_data_plane_resource_from_entry(table: TableEntry, warehouse_object_prefix: String) -> TableDataPlaneResource {
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

pub(crate) async fn scan_table_data_plane_resource_for_object<S>(
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
