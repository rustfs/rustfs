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

use std::io::Read;

use futures::{StreamExt, TryStreamExt, stream};

use super::super::*;

const ICEBERG_MAX_USER_FIELD_ID: i32 = i32::MAX - 200;
pub(crate) const ICEBERG_MAX_SCHEMA_NESTING_DEPTH: usize = 128;

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

#[allow(
    dead_code,
    reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
)]
fn table_warehouse_data_dir_path(entry: &TableEntry) -> TableCatalogStoreResult<String> {
    Ok(format!("{}{}", table_warehouse_object_prefix(entry)?, DATA_DIR))
}

pub(crate) fn table_object_s3_location(table_bucket: &str, object_key: &str) -> String {
    format!("s3://{table_bucket}/{object_key}")
}

pub(crate) struct TableMetadataCommitState {
    pub(crate) warehouse_location: Option<String>,
    pub(crate) format_version: Option<u16>,
}

pub(crate) fn table_metadata_commit_state(
    table_bucket: &str,
    metadata_location: &str,
    metadata_object: &TableCatalogObject,
) -> TableCatalogStoreResult<TableMetadataCommitState> {
    let metadata = decode_table_metadata_json(metadata_location, &metadata_object.data)?;
    let warehouse_location = metadata
        .get("location")
        .and_then(serde_json::Value::as_str)
        .map(|location| {
            validate_table_warehouse_location(table_bucket, location)?;
            Ok(location.to_string())
        })
        .transpose()?;
    let format_version = metadata
        .get("format-version")
        .map(|_| table_metadata_format_version(&metadata))
        .transpose()?;
    Ok(TableMetadataCommitState {
        warehouse_location,
        format_version,
    })
}

pub(crate) fn decode_table_metadata_json(metadata_location: &str, data: &[u8]) -> TableCatalogStoreResult<serde_json::Value> {
    if data.len() > TABLE_METADATA_JSON_MAX_SIZE {
        return Err(TableCatalogStoreError::Invalid(format!(
            "table metadata {metadata_location} exceeds the maximum encoded size of {TABLE_METADATA_JSON_MAX_SIZE} bytes"
        )));
    }

    if !table_metadata_location_is_gzip(metadata_location) {
        return parse_table_metadata_json(metadata_location, data);
    }

    let decoded_limit = TABLE_METADATA_JSON_MAX_SIZE
        .checked_add(1)
        .ok_or_else(|| TableCatalogStoreError::Internal("table metadata size limit overflowed".to_string()))?;
    let decoded_limit = u64::try_from(decoded_limit)
        .map_err(|_| TableCatalogStoreError::Internal("table metadata size limit is invalid".to_string()))?;
    let mut decoder = flate2::read::GzDecoder::new(data).take(decoded_limit);
    let mut decoded = Vec::new();
    decoder.read_to_end(&mut decoded).map_err(|err| {
        TableCatalogStoreError::Invalid(format!("failed to decompress table metadata {metadata_location}: {err}"))
    })?;
    if decoded.len() > TABLE_METADATA_JSON_MAX_SIZE {
        return Err(TableCatalogStoreError::Invalid(format!(
            "table metadata {metadata_location} exceeds the maximum decoded size of {TABLE_METADATA_JSON_MAX_SIZE} bytes"
        )));
    }
    parse_table_metadata_json(metadata_location, &decoded)
}

fn parse_table_metadata_json(metadata_location: &str, data: &[u8]) -> TableCatalogStoreResult<serde_json::Value> {
    let metadata = serde_json::from_slice::<serde_json::Value>(data)
        .map_err(|err| TableCatalogStoreError::Invalid(format!("failed to parse table metadata {metadata_location}: {err}")))?;
    if !metadata.is_object() {
        return Err(TableCatalogStoreError::Invalid(format!(
            "table metadata {metadata_location} must be a JSON object"
        )));
    }
    Ok(metadata)
}

fn table_metadata_location_is_gzip(metadata_location: &str) -> bool {
    metadata_location.ends_with(".gz.metadata.json") || metadata_location.ends_with(".metadata.json.gz")
}

pub(crate) fn canonical_json_sha256(metadata: &serde_json::Value) -> TableCatalogStoreResult<String> {
    let canonical = serde_json::to_vec(metadata)
        .map_err(|err| TableCatalogStoreError::Internal(format!("failed to encode metadata digest input: {err}")))?;
    Ok(hex_simd::encode_to_string(Sha256::digest(canonical), hex_simd::AsciiCase::Lower))
}

pub(crate) fn validate_commit_metadata_digest(
    request: &TableCommitRequest,
    metadata_object: &TableCatalogObject,
) -> TableCatalogStoreResult<()> {
    let mut expected_digest = None;
    for requirement in &request.requirements {
        if requirement.get("type").and_then(serde_json::Value::as_str) != Some(TABLE_METADATA_DIGEST_REQUIREMENT_TYPE) {
            continue;
        }
        if expected_digest.is_some() {
            return Err(TableCatalogStoreError::Invalid(
                "commit contains duplicate metadata digest requirements".to_string(),
            ));
        }
        expected_digest = Some(
            requirement
                .get("sha256")
                .and_then(serde_json::Value::as_str)
                .filter(|digest| rustfs_utils::crypto::is_sha256_checksum(digest))
                .ok_or_else(|| TableCatalogStoreError::Invalid("commit metadata digest is invalid".to_string()))?,
        );
    }
    let Some(expected_digest) = expected_digest else {
        return Ok(());
    };
    let metadata = decode_table_metadata_json(&request.new_metadata_location, &metadata_object.data)?;
    if canonical_json_sha256(&metadata)? != expected_digest {
        return Err(TableCatalogStoreError::Conflict(
            "new metadata object changed after commit validation".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn view_metadata_warehouse_location(
    table_bucket: &str,
    metadata_location: &str,
    metadata_object: &TableCatalogObject,
) -> TableCatalogStoreResult<Option<String>> {
    let metadata = decode_table_metadata_json(metadata_location, &metadata_object.data)?;
    let Some(location) = metadata.get("location").and_then(serde_json::Value::as_str) else {
        return Ok(None);
    };
    validate_view_warehouse_location(table_bucket, location)?;
    Ok(Some(location.to_string()))
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

pub(crate) fn warehouse_object_prefixes_overlap(left: &str, right: &str) -> bool {
    left.starts_with(right) || right.starts_with(left)
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
    for table in store.list_all_tables(bucket).await? {
        if table.state != TableCatalogEntryState::Active {
            continue;
        }
        let warehouse_object_prefix = table_warehouse_object_prefix(&table)?;
        if !object.starts_with(&warehouse_object_prefix) {
            continue;
        }
        if let Some(current) = matched.as_ref() {
            return Err(TableCatalogStoreError::Invalid(format!(
                "object {object} matches overlapping active table warehouse prefixes {} and {warehouse_object_prefix}",
                current.warehouse_object_prefix
            )));
        }
        matched = Some(table_data_plane_resource_from_entry(table, warehouse_object_prefix));
    }

    Ok(matched)
}

pub(crate) fn table_metadata_uuid(metadata: &serde_json::Value) -> TableCatalogStoreResult<&str> {
    metadata
        .get("table-uuid")
        .and_then(serde_json::Value::as_str)
        .filter(|uuid| !uuid.is_empty())
        .ok_or_else(|| TableCatalogStoreError::Invalid("table metadata is missing table-uuid".to_string()))
}

pub(crate) fn table_metadata_location(metadata: &serde_json::Value) -> TableCatalogStoreResult<&str> {
    metadata
        .get("location")
        .and_then(serde_json::Value::as_str)
        .filter(|location| !location.is_empty())
        .ok_or_else(|| TableCatalogStoreError::Invalid("table metadata is missing location".to_string()))
}

pub(crate) fn table_metadata_format_version(metadata: &serde_json::Value) -> TableCatalogStoreResult<u16> {
    let version = metadata
        .get("format-version")
        .and_then(serde_json::Value::as_u64)
        .filter(|version| *version > 0)
        .ok_or_else(|| TableCatalogStoreError::Invalid("table metadata is missing format-version".to_string()))?;
    let version = u16::try_from(version)
        .map_err(|_| TableCatalogStoreError::Invalid("table metadata format-version is too large".to_string()))?;
    if !(1..=2).contains(&version) {
        return Err(TableCatalogStoreError::Unsupported(format!(
            "unsupported Iceberg table format-version: {version}"
        )));
    }
    Ok(version)
}

fn normalize_v1_table_metadata_update_fields(metadata: &mut serde_json::Value) -> TableCatalogStoreResult<()> {
    if metadata.get("schemas").is_none() {
        let mut schema = metadata
            .get("schema")
            .cloned()
            .ok_or_else(|| TableCatalogStoreError::Invalid("Iceberg v1 table metadata is missing schema".to_string()))?;
        schema
            .as_object_mut()
            .ok_or_else(|| TableCatalogStoreError::Invalid("Iceberg v1 schema must be an object".to_string()))?
            .entry("schema-id".to_string())
            .or_insert_with(|| serde_json::Value::from(0));
        let schema_id = schema
            .get("schema-id")
            .and_then(serde_json::Value::as_i64)
            .ok_or_else(|| TableCatalogStoreError::Invalid("Iceberg v1 schema-id must be an integer".to_string()))?;
        let object = metadata_object_mut(metadata)?;
        object.insert("schemas".to_string(), serde_json::json!([schema]));
        object.insert("current-schema-id".to_string(), serde_json::Value::from(schema_id));
    } else if metadata.get("current-schema-id").is_none() {
        let schema_id = metadata
            .get("schema")
            .and_then(|schema| schema.get("schema-id"))
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0);
        if !require_metadata_array(metadata, "schemas")?
            .iter()
            .any(|schema| schema.get("schema-id").and_then(serde_json::Value::as_i64) == Some(schema_id))
        {
            return Err(TableCatalogStoreError::Invalid("Iceberg v1 current schema does not exist".to_string()));
        }
        metadata_object_mut(metadata)?.insert("current-schema-id".to_string(), serde_json::Value::from(schema_id));
    }

    if metadata.get("partition-specs").is_none() {
        let mut fields = metadata
            .get("partition-spec")
            .cloned()
            .ok_or_else(|| TableCatalogStoreError::Invalid("Iceberg v1 table metadata is missing partition-spec".to_string()))?;
        let fields = fields
            .as_array_mut()
            .ok_or_else(|| TableCatalogStoreError::Invalid("Iceberg v1 partition-spec must be an array".to_string()))?;
        for (index, field) in fields.iter_mut().enumerate() {
            let field_id = i32::try_from(index)
                .ok()
                .and_then(|index| 1000_i32.checked_add(index))
                .ok_or_else(|| TableCatalogStoreError::Invalid("Iceberg v1 partition spec has too many fields".to_string()))?;
            field
                .as_object_mut()
                .ok_or_else(|| TableCatalogStoreError::Invalid("Iceberg v1 partition spec fields must be objects".to_string()))?
                .entry("field-id".to_string())
                .or_insert_with(|| serde_json::Value::from(field_id));
        }
        let object = metadata_object_mut(metadata)?;
        object.insert("partition-specs".to_string(), serde_json::json!([{"spec-id": 0, "fields": fields}]));
        object.insert("default-spec-id".to_string(), serde_json::Value::from(0));
    } else if metadata.get("default-spec-id").is_none() {
        let default_spec_id = require_metadata_array(metadata, "partition-specs")?
            .last()
            .and_then(|spec| spec.get("spec-id"))
            .and_then(serde_json::Value::as_i64)
            .ok_or_else(|| TableCatalogStoreError::Invalid("Iceberg v1 default partition spec does not exist".to_string()))?;
        metadata_object_mut(metadata)?.insert("default-spec-id".to_string(), serde_json::Value::from(default_spec_id));
    }

    if metadata.get("sort-orders").is_none() {
        let object = metadata_object_mut(metadata)?;
        object.insert("sort-orders".to_string(), serde_json::json!([{"order-id": 0, "fields": []}]));
        object.insert("default-sort-order-id".to_string(), serde_json::Value::from(0));
    } else if metadata.get("default-sort-order-id").is_none() {
        let default_sort_order_id = require_metadata_array(metadata, "sort-orders")?
            .last()
            .and_then(|order| order.get("order-id"))
            .and_then(serde_json::Value::as_i64)
            .ok_or_else(|| TableCatalogStoreError::Invalid("Iceberg v1 default sort order does not exist".to_string()))?;
        metadata_object_mut(metadata)?
            .insert("default-sort-order-id".to_string(), serde_json::Value::from(default_sort_order_id));
    }

    if metadata.get("last-partition-id").is_none() {
        let last_partition_id = require_metadata_array(metadata, "partition-specs")?
            .iter()
            .map(max_partition_field_id)
            .max()
            .unwrap_or(999);
        metadata_object_mut(metadata)?.insert("last-partition-id".to_string(), serde_json::Value::from(last_partition_id));
    }
    Ok(())
}

pub(crate) fn synchronize_table_metadata_version_fields(metadata: &mut serde_json::Value) -> TableCatalogStoreResult<()> {
    match table_metadata_format_version(metadata)? {
        1 => {
            normalize_v1_table_metadata_update_fields(metadata)?;
            if let Some(schemas) = metadata.get("schemas").and_then(serde_json::Value::as_array)
                && !schemas.is_empty()
            {
                let current_schema_id = metadata
                    .get("current-schema-id")
                    .and_then(serde_json::Value::as_i64)
                    .or_else(|| {
                        schemas
                            .last()
                            .and_then(|schema| schema.get("schema-id"))
                            .and_then(serde_json::Value::as_i64)
                    });
                let schema = current_schema_id
                    .and_then(|schema_id| {
                        schemas
                            .iter()
                            .find(|schema| schema.get("schema-id").and_then(serde_json::Value::as_i64) == Some(schema_id))
                    })
                    .cloned()
                    .ok_or_else(|| TableCatalogStoreError::Invalid("Iceberg v1 current schema does not exist".to_string()))?;
                metadata_object_mut(metadata)?.insert("schema".to_string(), schema);
            }
            if let Some(specs) = metadata.get("partition-specs").and_then(serde_json::Value::as_array)
                && !specs.is_empty()
            {
                let default_spec_id = metadata
                    .get("default-spec-id")
                    .and_then(serde_json::Value::as_i64)
                    .or_else(|| {
                        specs
                            .last()
                            .and_then(|spec| spec.get("spec-id"))
                            .and_then(serde_json::Value::as_i64)
                    });
                let fields = default_spec_id
                    .and_then(|spec_id| {
                        specs
                            .iter()
                            .find(|spec| spec.get("spec-id").and_then(serde_json::Value::as_i64) == Some(spec_id))
                    })
                    .and_then(|spec| spec.get("fields"))
                    .cloned()
                    .ok_or_else(|| {
                        TableCatalogStoreError::Invalid("Iceberg v1 default partition spec does not exist".to_string())
                    })?;
                metadata_object_mut(metadata)?.insert("partition-spec".to_string(), fields);
            }
        }
        2 => {
            if metadata.get("schemas").is_none() {
                let mut schema = metadata
                    .get("schema")
                    .cloned()
                    .ok_or_else(|| TableCatalogStoreError::Invalid("Iceberg v1 table metadata is missing schema".to_string()))?;
                schema
                    .as_object_mut()
                    .ok_or_else(|| TableCatalogStoreError::Invalid("Iceberg v1 schema must be an object".to_string()))?
                    .entry("schema-id".to_string())
                    .or_insert_with(|| serde_json::Value::from(0));
                let schema_id = schema
                    .get("schema-id")
                    .and_then(serde_json::Value::as_i64)
                    .ok_or_else(|| TableCatalogStoreError::Invalid("Iceberg v1 schema-id must be an integer".to_string()))?;
                metadata_object_mut(metadata)?.insert("schemas".to_string(), serde_json::json!([schema]));
                metadata_object_mut(metadata)?.insert("current-schema-id".to_string(), serde_json::Value::from(schema_id));
            }
            if metadata.get("partition-specs").is_none() {
                let mut fields = metadata.get("partition-spec").cloned().ok_or_else(|| {
                    TableCatalogStoreError::Invalid("Iceberg v1 table metadata is missing partition-spec".to_string())
                })?;
                let fields = fields
                    .as_array_mut()
                    .ok_or_else(|| TableCatalogStoreError::Invalid("Iceberg v1 partition-spec must be an array".to_string()))?;
                for (index, field) in fields.iter_mut().enumerate() {
                    let field_id = i32::try_from(index)
                        .ok()
                        .and_then(|index| 1000_i32.checked_add(index))
                        .ok_or_else(|| {
                            TableCatalogStoreError::Invalid("Iceberg v1 partition spec has too many fields".to_string())
                        })?;
                    field
                        .as_object_mut()
                        .ok_or_else(|| {
                            TableCatalogStoreError::Invalid("Iceberg v1 partition spec fields must be objects".to_string())
                        })?
                        .entry("field-id".to_string())
                        .or_insert_with(|| serde_json::Value::from(field_id));
                }
                metadata_object_mut(metadata)?
                    .insert("partition-specs".to_string(), serde_json::json!([{"spec-id": 0, "fields": fields}]));
                metadata_object_mut(metadata)?.insert("default-spec-id".to_string(), serde_json::Value::from(0));
            }
            if metadata.get("sort-orders").is_none() {
                metadata_object_mut(metadata)?
                    .insert("sort-orders".to_string(), serde_json::json!([{"order-id": 0, "fields": []}]));
                metadata_object_mut(metadata)?.insert("default-sort-order-id".to_string(), serde_json::Value::from(0));
            }
            if metadata.get("last-partition-id").is_none() {
                let last_partition_id = metadata
                    .get("partition-specs")
                    .and_then(serde_json::Value::as_array)
                    .map(|specs| specs.iter().map(max_partition_field_id).max().unwrap_or(999))
                    .unwrap_or(999);
                metadata_object_mut(metadata)?
                    .insert("last-partition-id".to_string(), serde_json::Value::from(last_partition_id));
            }
            if metadata.get("last-sequence-number").is_none() {
                let last_sequence_number = metadata
                    .get("snapshots")
                    .and_then(serde_json::Value::as_array)
                    .and_then(|snapshots| {
                        snapshots
                            .iter()
                            .filter_map(|snapshot| snapshot.get("sequence-number").and_then(serde_json::Value::as_i64))
                            .max()
                    })
                    .unwrap_or(0);
                metadata_object_mut(metadata)?
                    .insert("last-sequence-number".to_string(), serde_json::Value::from(last_sequence_number));
            }
            let object = metadata_object_mut(metadata)?;
            object.remove("schema");
            object.remove("partition-spec");
        }
        version => {
            return Err(TableCatalogStoreError::Internal(format!(
                "validated Iceberg format version {version} is unsupported"
            )));
        }
    }
    Ok(())
}

fn validate_table_history_logs(metadata: &serde_json::Value) -> TableCatalogStoreResult<()> {
    for field in ["snapshot-log", "metadata-log"] {
        let Some(entries) = metadata.get(field) else {
            continue;
        };
        let entries = entries
            .as_array()
            .ok_or_else(|| TableCatalogStoreError::Invalid(format!("{field} must be an array")))?;
        for entry in entries {
            let entry = entry
                .as_object()
                .ok_or_else(|| TableCatalogStoreError::Invalid(format!("{field} entries must be JSON objects")))?;
            if entry.get("timestamp-ms").and_then(serde_json::Value::as_i64).is_none() {
                return Err(TableCatalogStoreError::Invalid(format!("{field} entries require integer timestamp-ms")));
            }
            match field {
                "snapshot-log" if entry.get("snapshot-id").and_then(serde_json::Value::as_i64).is_none() => {
                    return Err(TableCatalogStoreError::Invalid(
                        "snapshot-log entries require integer snapshot-id".to_string(),
                    ));
                }
                "metadata-log"
                    if !entry
                        .get("metadata-file")
                        .and_then(serde_json::Value::as_str)
                        .is_some_and(|location| !location.is_empty()) =>
                {
                    return Err(TableCatalogStoreError::Invalid(
                        "metadata-log entries require non-empty metadata-file".to_string(),
                    ));
                }
                _ => {}
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_supported_table_metadata(metadata: &serde_json::Value) -> TableCatalogStoreResult<()> {
    validate_supported_table_metadata_fields(metadata)?;
    validate_table_history_logs(metadata)?;
    validate_table_metadata_references(metadata)
}

pub(crate) fn validate_table_metadata_transition(
    current_metadata: &serde_json::Value,
    target_metadata: &serde_json::Value,
) -> TableCatalogStoreResult<()> {
    let current_last_column_id = require_metadata_i32(current_metadata, "last-column-id")?;
    let target_last_column_id = require_metadata_i32(target_metadata, "last-column-id")?;
    if target_last_column_id < current_last_column_id {
        return Err(TableCatalogStoreError::Invalid(
            "last-column-id must not decrease across table metadata commits".to_string(),
        ));
    }

    let current_format_version = table_metadata_format_version(current_metadata)?;
    let target_format_version = table_metadata_format_version(target_metadata)?;
    let current_last_partition_id = table_metadata_last_partition_id(current_metadata, current_format_version)?;
    let target_last_partition_id = table_metadata_last_partition_id(target_metadata, target_format_version)?;
    if target_last_partition_id < current_last_partition_id {
        return Err(TableCatalogStoreError::Invalid(
            "last-partition-id must not decrease across table metadata commits".to_string(),
        ));
    }
    let current_last_sequence_number = table_metadata_last_sequence_number(current_metadata, current_format_version)?;
    let target_last_sequence_number = table_metadata_last_sequence_number(target_metadata, target_format_version)?;
    if target_last_sequence_number < current_last_sequence_number {
        return Err(TableCatalogStoreError::Invalid(
            "last-sequence-number must not decrease across table metadata commits".to_string(),
        ));
    }

    validate_existing_partition_specs_unchanged(current_metadata, target_metadata)?;
    validate_existing_metadata_entries_unchanged(
        &normalized_sort_order_definitions(current_metadata, current_format_version)?,
        &normalized_sort_order_definitions(target_metadata, target_format_version)?,
        "sort order",
    )?;
    validate_existing_snapshots_unchanged(current_metadata, target_metadata, current_format_version, target_format_version)?;

    let current_schemas = table_metadata_schemas_by_id(current_metadata, current_format_version)?;
    let target_schemas = table_metadata_schemas_by_id(target_metadata, target_format_version)?;
    for (schema_id, current_schema) in &current_schemas {
        if let Some(target_schema) = target_schemas.get(schema_id)
            && normalized_schema_definition(current_schema, *schema_id)?
                != normalized_schema_definition(target_schema, *schema_id)?
        {
            return Err(TableCatalogStoreError::Invalid(format!(
                "existing schema {schema_id} must not be modified"
            )));
        }
    }

    let current_schema_id = table_metadata_current_schema_id(current_metadata, current_format_version)?;
    let current_schema = current_schemas
        .get(&current_schema_id)
        .ok_or_else(|| TableCatalogStoreError::Invalid("current table metadata schema does not exist".to_string()))?;
    let current_fields = validate_iceberg_schema_fields(current_schema, "current schema")?;
    for (target_schema_id, target_schema) in &target_schemas {
        if current_schemas.contains_key(target_schema_id) {
            continue;
        }
        let target_fields = validate_iceberg_schema_fields(target_schema, "target schema")?;
        for (field_id, target_field) in &target_fields.descriptors {
            match current_fields.descriptors.get(field_id) {
                Some(current_field) => validate_schema_field_evolution(*field_id, current_field, target_field)?,
                None if *field_id <= current_last_column_id => {
                    return Err(TableCatalogStoreError::Invalid(format!(
                        "schema field {field_id} cannot reuse a previously assigned field id"
                    )));
                }
                None => {}
            }
        }
    }
    Ok(())
}

fn table_metadata_last_sequence_number(metadata: &serde_json::Value, format_version: u16) -> TableCatalogStoreResult<i64> {
    if format_version == 1 {
        return Ok(0);
    }
    require_metadata_i64(metadata, "last-sequence-number")
}

fn normalized_sort_order_definitions(
    metadata: &serde_json::Value,
    format_version: u16,
) -> TableCatalogStoreResult<BTreeMap<i64, serde_json::Value>> {
    let Some(sort_orders) = metadata.get("sort-orders") else {
        return if format_version == 1 {
            Ok(BTreeMap::from([(0, serde_json::json!({"order-id": 0, "fields": []}))]))
        } else {
            Err(TableCatalogStoreError::Invalid("sort-orders must be an array".to_string()))
        };
    };
    metadata_entries_by_id(sort_orders, "order-id", "sort order")
}

fn validate_existing_snapshots_unchanged(
    current_metadata: &serde_json::Value,
    target_metadata: &serde_json::Value,
    current_format_version: u16,
    target_format_version: u16,
) -> TableCatalogStoreResult<()> {
    let current = current_metadata
        .get("snapshots")
        .map(|snapshots| metadata_entries_by_id(snapshots, "snapshot-id", "snapshot"))
        .transpose()?
        .unwrap_or_default();
    let target = target_metadata
        .get("snapshots")
        .map(|snapshots| metadata_entries_by_id(snapshots, "snapshot-id", "snapshot"))
        .transpose()?
        .unwrap_or_default();
    for (snapshot_id, current_snapshot) in current {
        let Some(target_snapshot) = target.get(&snapshot_id) else {
            continue;
        };
        let mut current_snapshot = current_snapshot;
        let mut target_snapshot = target_snapshot.clone();
        if current_format_version == 1 && target_format_version == 2 {
            for snapshot in [&mut current_snapshot, &mut target_snapshot] {
                if snapshot.get("sequence-number").and_then(serde_json::Value::as_i64) == Some(0)
                    && let Some(object) = snapshot.as_object_mut()
                {
                    object.remove("sequence-number");
                }
            }
        }
        if current_snapshot != target_snapshot {
            return Err(TableCatalogStoreError::Invalid(format!(
                "existing snapshot {snapshot_id} must not be modified"
            )));
        }
    }
    Ok(())
}

fn validate_existing_metadata_entries_unchanged(
    current: &BTreeMap<i64, serde_json::Value>,
    target: &BTreeMap<i64, serde_json::Value>,
    label: &str,
) -> TableCatalogStoreResult<()> {
    for (id, current_value) in current {
        if let Some(target_value) = target.get(id)
            && target_value != current_value
        {
            return Err(TableCatalogStoreError::Invalid(format!("existing {label} {id} must not be modified")));
        }
    }
    Ok(())
}

fn validate_existing_partition_specs_unchanged(
    current_metadata: &serde_json::Value,
    target_metadata: &serde_json::Value,
) -> TableCatalogStoreResult<()> {
    let current = normalized_partition_spec_definitions(current_metadata)?;
    let target = normalized_partition_spec_definitions(target_metadata)?;
    for (spec_id, current_fields) in current {
        if let Some(target_fields) = target.get(&spec_id)
            && target_fields != &current_fields
        {
            return Err(TableCatalogStoreError::Invalid(format!(
                "existing partition spec {spec_id} must not be modified"
            )));
        }
    }
    Ok(())
}

fn metadata_entries_by_id(
    value: &serde_json::Value,
    id_field: &str,
    label: &str,
) -> TableCatalogStoreResult<BTreeMap<i64, serde_json::Value>> {
    if value.is_null() {
        return Ok(BTreeMap::new());
    }
    let values = value
        .as_array()
        .ok_or_else(|| TableCatalogStoreError::Invalid(format!("{label}s must be an array")))?;
    let mut entries = BTreeMap::new();
    for value in values {
        let id = value
            .get(id_field)
            .and_then(serde_json::Value::as_i64)
            .ok_or_else(|| TableCatalogStoreError::Invalid(format!("{label} is missing {id_field}")))?;
        if entries.insert(id, value.clone()).is_some() {
            return Err(TableCatalogStoreError::Invalid(format!("duplicate {label} id {id}")));
        }
    }
    Ok(entries)
}

fn normalized_schema_definition(schema: &serde_json::Value, schema_id: i64) -> TableCatalogStoreResult<serde_json::Value> {
    let mut schema = schema.clone();
    schema
        .as_object_mut()
        .ok_or_else(|| TableCatalogStoreError::Invalid("schema must be a JSON object".to_string()))?
        .entry("schema-id".to_string())
        .or_insert_with(|| serde_json::Value::from(schema_id));
    Ok(schema)
}

fn table_metadata_last_partition_id(metadata: &serde_json::Value, format_version: u16) -> TableCatalogStoreResult<i32> {
    if format_version != 1 {
        return require_metadata_i32(metadata, "last-partition-id");
    }
    let field_count = require_metadata_array(metadata, "partition-spec")?.len();
    i32::try_from(field_count)
        .ok()
        .and_then(|field_count| 999_i32.checked_add(field_count))
        .ok_or_else(|| TableCatalogStoreError::Invalid("Iceberg v1 partition spec has too many fields".to_string()))
}

fn table_metadata_current_schema_id(metadata: &serde_json::Value, format_version: u16) -> TableCatalogStoreResult<i64> {
    if format_version == 1 {
        return Ok(metadata
            .get("schema")
            .and_then(|schema| schema.get("schema-id"))
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0));
    }
    metadata
        .get("current-schema-id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| TableCatalogStoreError::Invalid("current-schema-id must be an integer".to_string()))
}

fn table_metadata_schemas_by_id(
    metadata: &serde_json::Value,
    format_version: u16,
) -> TableCatalogStoreResult<BTreeMap<i64, &serde_json::Value>> {
    let mut schemas = BTreeMap::new();
    if let Some(values) = metadata.get("schemas") {
        for schema in values
            .as_array()
            .ok_or_else(|| TableCatalogStoreError::Invalid("schemas must be an array".to_string()))?
        {
            let schema_id = schema
                .get("schema-id")
                .and_then(serde_json::Value::as_i64)
                .ok_or_else(|| TableCatalogStoreError::Invalid("schema-id must be an integer".to_string()))?;
            if schemas.insert(schema_id, schema).is_some() {
                return Err(TableCatalogStoreError::Invalid(format!("duplicate schema id {schema_id}")));
            }
        }
    }
    if format_version == 1 {
        let schema = metadata
            .get("schema")
            .ok_or_else(|| TableCatalogStoreError::Invalid("Iceberg v1 table metadata is missing schema".to_string()))?;
        let schema_id = schema.get("schema-id").and_then(serde_json::Value::as_i64).unwrap_or(0);
        if let Some(known_schema) = schemas.get(&schema_id) {
            let mut normalized_schema = schema.clone();
            normalized_schema
                .as_object_mut()
                .ok_or_else(|| TableCatalogStoreError::Invalid("Iceberg v1 schema must be an object".to_string()))?
                .entry("schema-id".to_string())
                .or_insert_with(|| serde_json::Value::from(schema_id));
            if *known_schema != &normalized_schema {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "Iceberg v1 current schema {schema_id} does not match schemas"
                )));
            }
        } else {
            schemas.insert(schema_id, schema);
        }
    }
    Ok(schemas)
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum IcebergStatisticsFileKind {
    Table,
    Partition,
}

pub(crate) fn validate_iceberg_statistics_file(
    value: &serde_json::Value,
    label: &str,
    kind: IcebergStatisticsFileKind,
) -> TableCatalogStoreResult<i64> {
    let object = value
        .as_object()
        .ok_or_else(|| TableCatalogStoreError::Invalid(format!("{label} must be a JSON object")))?;
    let snapshot_id = object
        .get("snapshot-id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| TableCatalogStoreError::Invalid(format!("{label}.snapshot-id must be an integer")))?;
    if !object
        .get("statistics-path")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|path| !path.is_empty())
    {
        return Err(TableCatalogStoreError::Invalid(format!(
            "{label}.statistics-path must be a non-empty string"
        )));
    }
    let file_size = statistics_non_negative_i64(object, "file-size-in-bytes", label)?;
    if matches!(kind, IcebergStatisticsFileKind::Table) {
        let footer_size = statistics_non_negative_i64(object, "file-footer-size-in-bytes", label)?;
        if footer_size > file_size {
            return Err(TableCatalogStoreError::Invalid(format!(
                "{label}.file-footer-size-in-bytes must not exceed file-size-in-bytes"
            )));
        }
        let blobs = object
            .get("blob-metadata")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| TableCatalogStoreError::Invalid(format!("{label}.blob-metadata must be an array")))?;
        for blob in blobs {
            validate_statistics_blob_metadata(blob, label)?;
        }
    }
    Ok(snapshot_id)
}

fn validate_statistics_blob_metadata(value: &serde_json::Value, label: &str) -> TableCatalogStoreResult<()> {
    let object = value
        .as_object()
        .ok_or_else(|| TableCatalogStoreError::Invalid(format!("{label}.blob-metadata entries must be JSON objects")))?;
    if !object
        .get("type")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|value| !value.is_empty())
    {
        return Err(TableCatalogStoreError::Invalid(format!(
            "{label}.blob-metadata type must be a non-empty string"
        )));
    }
    for field in ["snapshot-id", "sequence-number"] {
        if object.get(field).and_then(serde_json::Value::as_i64).is_none() {
            return Err(TableCatalogStoreError::Invalid(format!(
                "{label}.blob-metadata {field} must be an integer"
            )));
        }
    }
    let fields = object
        .get("fields")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| TableCatalogStoreError::Invalid(format!("{label}.blob-metadata fields must be an array")))?;
    if fields.iter().any(|field| field.as_i64().is_none()) {
        return Err(TableCatalogStoreError::Invalid(format!(
            "{label}.blob-metadata fields must contain integers"
        )));
    }
    if let Some(properties) = object.get("properties") {
        let properties = properties
            .as_object()
            .ok_or_else(|| TableCatalogStoreError::Invalid(format!("{label}.blob-metadata properties must be a JSON object")))?;
        if properties.values().any(|value| !value.is_string()) {
            return Err(TableCatalogStoreError::Invalid(format!(
                "{label}.blob-metadata property values must be strings"
            )));
        }
    }
    Ok(())
}

fn statistics_non_negative_i64(
    object: &serde_json::Map<String, serde_json::Value>,
    field: &str,
    label: &str,
) -> TableCatalogStoreResult<i64> {
    let value = object
        .get(field)
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| TableCatalogStoreError::Invalid(format!("{label}.{field} must be an integer")))?;
    if value < 0 {
        return Err(TableCatalogStoreError::Invalid(format!("{label}.{field} must not be negative")));
    }
    Ok(value)
}

fn validate_table_statistics_references(
    metadata: &serde_json::Value,
    snapshot_ids: &BTreeSet<i64>,
) -> TableCatalogStoreResult<()> {
    for (field, kind) in [
        ("statistics", IcebergStatisticsFileKind::Table),
        ("partition-statistics", IcebergStatisticsFileKind::Partition),
    ] {
        let Some(values) = metadata.get(field) else {
            continue;
        };
        let values = values
            .as_array()
            .ok_or_else(|| TableCatalogStoreError::Invalid(format!("table metadata field {field} must be an array")))?;
        let mut snapshot_ids_with_statistics = BTreeSet::new();
        for value in values {
            let snapshot_id = validate_iceberg_statistics_file(value, field, kind)?;
            if !snapshot_ids.contains(&snapshot_id) {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "{field} references missing snapshot {snapshot_id}"
                )));
            }
            if !snapshot_ids_with_statistics.insert(snapshot_id) {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "{field} contains duplicate entries for snapshot {snapshot_id}"
                )));
            }
        }
    }
    Ok(())
}

fn validate_supported_table_metadata_fields(metadata: &serde_json::Value) -> TableCatalogStoreResult<()> {
    table_metadata_uuid(metadata)?;
    table_metadata_location(metadata)?;
    require_metadata_i64(metadata, "last-updated-ms")?;
    require_metadata_i32(metadata, "last-column-id")?;

    match table_metadata_format_version(metadata)? {
        1 => {
            if !metadata.get("schema").is_some_and(serde_json::Value::is_object) {
                return Err(TableCatalogStoreError::Invalid("Iceberg v1 table metadata is missing schema".to_string()));
            }
            require_metadata_array(metadata, "partition-spec")?;
            if let Some(snapshots) = metadata.get("snapshots").and_then(serde_json::Value::as_array) {
                for snapshot in snapshots {
                    validate_table_snapshot_fields(snapshot, 1)?;
                    if snapshot
                        .get("sequence-number")
                        .is_some_and(|sequence_number| sequence_number.as_i64() != Some(0))
                    {
                        return Err(TableCatalogStoreError::Invalid(
                            "Iceberg v1 snapshot sequence-number must be zero when present".to_string(),
                        ));
                    }
                }
            }
        }
        2 => {
            let last_sequence_number = require_metadata_i64(metadata, "last-sequence-number")?;
            if last_sequence_number < 0 {
                return Err(TableCatalogStoreError::Invalid("last-sequence-number must not be negative".to_string()));
            }
            if require_metadata_array(metadata, "schemas")?.is_empty() {
                return Err(TableCatalogStoreError::Invalid("table metadata schemas must not be empty".to_string()));
            }
            require_metadata_i32(metadata, "current-schema-id")?;
            if require_metadata_array(metadata, "partition-specs")?.is_empty() {
                return Err(TableCatalogStoreError::Invalid(
                    "table metadata partition-specs must not be empty".to_string(),
                ));
            }
            require_metadata_i32(metadata, "default-spec-id")?;
            require_metadata_i32(metadata, "last-partition-id")?;
            if require_metadata_array(metadata, "sort-orders")?.is_empty() {
                return Err(TableCatalogStoreError::Invalid(
                    "table metadata sort-orders must not be empty".to_string(),
                ));
            }
            require_metadata_i32(metadata, "default-sort-order-id")?;
            if let Some(snapshots) = metadata.get("snapshots").and_then(serde_json::Value::as_array) {
                for snapshot in snapshots {
                    validate_table_snapshot_fields(snapshot, 2)?;
                    let sequence_number = match snapshot.get("sequence-number") {
                        Some(sequence_number) => sequence_number.as_i64().ok_or_else(|| {
                            TableCatalogStoreError::Invalid("Iceberg v2 snapshot sequence-number must be an integer".to_string())
                        })?,
                        None => {
                            return Err(TableCatalogStoreError::Invalid(
                                "Iceberg v2 snapshot sequence-number is required".to_string(),
                            ));
                        }
                    };
                    if sequence_number < 0 || sequence_number > last_sequence_number {
                        return Err(TableCatalogStoreError::Invalid(
                            "Iceberg v2 snapshot sequence-number must be between zero and last-sequence-number".to_string(),
                        ));
                    }
                }
            }
        }
        version => {
            return Err(TableCatalogStoreError::Internal(format!(
                "validated Iceberg format version {version} is unsupported"
            )));
        }
    }
    Ok(())
}

pub(crate) fn validate_table_metadata_references(metadata: &serde_json::Value) -> TableCatalogStoreResult<()> {
    let format_version = table_metadata_format_version(metadata)?;
    let schema_fields = validate_table_schemas(metadata, format_version)?;
    let current_schema_fields = current_table_schema_fields(metadata, format_version)?;
    let last_column_id = require_metadata_i32(metadata, "last-column-id")?;
    if last_column_id < 0
        || schema_fields
            .field_ids
            .last()
            .is_some_and(|field_id| *field_id > last_column_id)
    {
        return Err(TableCatalogStoreError::Invalid(
            "last-column-id must be non-negative and cover every assigned schema field id".to_string(),
        ));
    }
    let mut schema_ids = metadata_array_i32_ids(metadata, "schemas", "schema-id", "schema")?;
    if format_version == 1 && schema_ids.is_empty() {
        let schema = metadata
            .get("schema")
            .and_then(serde_json::Value::as_object)
            .ok_or_else(|| TableCatalogStoreError::Invalid("Iceberg v1 table metadata is missing schema".to_string()))?;
        let schema_id = match schema.get("schema-id") {
            Some(schema_id) => schema_id
                .as_i64()
                .ok_or_else(|| TableCatalogStoreError::Invalid("Iceberg v1 schema-id must be an integer".to_string()))?,
            None => 0,
        };
        if schema_id < 0 {
            return Err(TableCatalogStoreError::Invalid(format!("schema id {schema_id} must not be negative")));
        }
        if i32::try_from(schema_id).is_err() {
            return Err(TableCatalogStoreError::Invalid(format!(
                "schema id {schema_id} exceeds the signed 32-bit range"
            )));
        }
        schema_ids.insert(schema_id);
    }
    validate_metadata_id_reference(metadata, "current-schema-id", &schema_ids, "schema")?;
    validate_partition_specs(metadata, format_version, &schema_fields, &current_schema_fields)?;
    let spec_ids = metadata_array_i32_ids(metadata, "partition-specs", "spec-id", "partition spec")?;
    validate_metadata_id_reference(metadata, "default-spec-id", &spec_ids, "partition spec")?;
    validate_sort_orders(metadata, &schema_fields, &current_schema_fields)?;
    let sort_order_ids = metadata_array_i32_ids(metadata, "sort-orders", "order-id", "sort order")?;
    validate_metadata_id_reference(metadata, "default-sort-order-id", &sort_order_ids, "sort order")?;
    let snapshot_ids = metadata_array_ids(metadata, "snapshots", "snapshot-id", "snapshot")?;
    validate_table_statistics_references(metadata, &snapshot_ids)?;

    let current_snapshot_id = match metadata.get("current-snapshot-id").filter(|value| !value.is_null()) {
        Some(current_snapshot_id) => {
            let current_snapshot_id = current_snapshot_id
                .as_i64()
                .ok_or_else(|| TableCatalogStoreError::Invalid("current-snapshot-id must be an integer".to_string()))?;
            if current_snapshot_id != -1 && !snapshot_ids.contains(&current_snapshot_id) {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "current snapshot {current_snapshot_id} does not exist in table metadata"
                )));
            }
            (current_snapshot_id != -1).then_some(current_snapshot_id)
        }
        None => None,
    };
    if let Some(refs) = metadata.get("refs").filter(|value| !value.is_null()) {
        let refs = refs
            .as_object()
            .ok_or_else(|| TableCatalogStoreError::Invalid("refs must be an object".to_string()))?;
        for (name, reference) in refs {
            if name.is_empty() {
                return Err(TableCatalogStoreError::Invalid("snapshot ref name must not be empty".to_string()));
            }
            let reference = reference
                .as_object()
                .ok_or_else(|| TableCatalogStoreError::Invalid(format!("snapshot ref {name} must be an object")))?;
            let snapshot_id = reference
                .get("snapshot-id")
                .and_then(serde_json::Value::as_i64)
                .ok_or_else(|| TableCatalogStoreError::Invalid(format!("snapshot ref {name} is missing snapshot-id")))?;
            if !snapshot_ids.contains(&snapshot_id) {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "snapshot ref {name} targets snapshot {snapshot_id}, which does not exist"
                )));
            }
            let reference_type = reference
                .get("type")
                .and_then(serde_json::Value::as_str)
                .filter(|reference_type| matches!(*reference_type, "branch" | "tag"))
                .ok_or_else(|| TableCatalogStoreError::Invalid(format!("snapshot ref {name} must have type branch or tag")))?;
            if name == "main" && (reference_type != "branch" || current_snapshot_id != Some(snapshot_id)) {
                return Err(TableCatalogStoreError::Invalid(
                    "main snapshot ref must be a branch pointing to current-snapshot-id".to_string(),
                ));
            }
            validate_snapshot_ref_retention(name, reference_type, reference)?;
        }
    }
    if let Some(snapshots) = metadata.get("snapshots").and_then(serde_json::Value::as_array) {
        for snapshot in snapshots {
            if let Some(schema_id) = snapshot.get("schema-id") {
                let schema_id = schema_id
                    .as_i64()
                    .ok_or_else(|| TableCatalogStoreError::Invalid("snapshot schema-id must be an integer".to_string()))?;
                if !schema_ids.contains(&schema_id) {
                    return Err(TableCatalogStoreError::Invalid(format!(
                        "snapshot schema-id targets schema {schema_id}, which does not exist"
                    )));
                }
            }
        }
    }
    Ok(())
}

fn validate_table_schemas(metadata: &serde_json::Value, format_version: u16) -> TableCatalogStoreResult<IcebergSchemaFields> {
    let mut schemas = Vec::new();
    if format_version == 1 {
        let schema = metadata
            .get("schema")
            .ok_or_else(|| TableCatalogStoreError::Invalid("Iceberg v1 table metadata is missing schema".to_string()))?;
        let schema_id = schema.get("schema-id").and_then(serde_json::Value::as_i64).unwrap_or(0);
        schemas.push((schema_id, schema));
    }
    if let Some(metadata_schemas) = metadata.get("schemas") {
        let metadata_schemas = metadata_schemas
            .as_array()
            .ok_or_else(|| TableCatalogStoreError::Invalid("schemas must be an array".to_string()))?;
        for schema in metadata_schemas {
            let schema_id = schema
                .get("schema-id")
                .and_then(serde_json::Value::as_i64)
                .ok_or_else(|| TableCatalogStoreError::Invalid("schema-id must be an integer".to_string()))?;
            schemas.push((schema_id, schema));
        }
    }
    schemas.sort_by_key(|(schema_id, _)| *schema_id);

    let mut historical = BTreeMap::new();
    let mut active_fields = BTreeSet::new();
    let mut retired_fields = BTreeSet::new();
    let mut all_fields = IcebergSchemaFields::default();
    for (_, schema) in schemas {
        let schema_fields = validate_iceberg_schema_fields(schema, "schema")?;
        if let Some(field_id) = schema_fields
            .field_ids
            .iter()
            .find(|field_id| retired_fields.contains(*field_id))
        {
            return Err(TableCatalogStoreError::Invalid(format!(
                "schema field {field_id} cannot be reused after removal"
            )));
        }
        for (field_id, descriptor) in &schema_fields.descriptors {
            if let Some(previous) = historical.get(field_id) {
                validate_schema_field_evolution(*field_id, previous, descriptor)?;
            }
            historical.insert(*field_id, descriptor.clone());
        }
        retired_fields.extend(active_fields.difference(&schema_fields.field_ids).copied());
        active_fields = schema_fields.field_ids.clone();
        all_fields.field_ids.extend(schema_fields.field_ids);
        all_fields.identifier_eligible.extend(schema_fields.identifier_eligible);
        all_fields.descriptors.extend(schema_fields.descriptors);
    }
    Ok(all_fields)
}

fn current_table_schema_fields(
    metadata: &serde_json::Value,
    format_version: u16,
) -> TableCatalogStoreResult<IcebergSchemaFields> {
    if metadata.get("schemas").is_some() {
        let current_schema_id = require_metadata_i32(metadata, "current-schema-id")?;
        let schema = require_metadata_array(metadata, "schemas")?
            .iter()
            .find(|schema| schema.get("schema-id").and_then(serde_json::Value::as_i64) == Some(i64::from(current_schema_id)))
            .ok_or_else(|| {
                TableCatalogStoreError::Invalid(format!(
                    "current-schema-id targets schema {current_schema_id}, which does not exist"
                ))
            })?;
        return validate_iceberg_schema_fields(schema, "current schema");
    }
    if format_version == 1 {
        let schema = metadata
            .get("schema")
            .ok_or_else(|| TableCatalogStoreError::Invalid("Iceberg v1 table metadata is missing schema".to_string()))?;
        return validate_iceberg_schema_fields(schema, "schema");
    }
    Err(TableCatalogStoreError::Invalid("schemas must be an array".to_string()))
}

pub(crate) fn validate_partition_spec_sources_against_current_schema(
    metadata: &serde_json::Value,
    spec: &serde_json::Value,
) -> TableCatalogStoreResult<()> {
    let current_schema_fields = current_table_schema_fields(metadata, table_metadata_format_version(metadata)?)?;
    let fields = spec
        .get("fields")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| TableCatalogStoreError::Invalid("partition spec fields must be an array".to_string()))?;
    for field in fields {
        let field = field
            .as_object()
            .ok_or_else(|| TableCatalogStoreError::Invalid("partition spec fields must be JSON objects".to_string()))?;
        let source_id = required_positive_i32_value(field, "source-id", "partition source-id")?;
        let transform = field
            .get("transform")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| TableCatalogStoreError::Invalid("partition field transform must be a non-empty string".to_string()))?;
        if transform == "void" {
            continue;
        }
        let source_type = current_schema_fields.descriptors.get(&source_id).ok_or_else(|| {
            TableCatalogStoreError::Invalid(format!("partition source-id {source_id} does not reference the current schema"))
        })?;
        if source_type.inside_collection {
            return Err(TableCatalogStoreError::Invalid(format!(
                "partition source-id {source_id} must not be nested in a list or map"
            )));
        }
        validate_transform_for_source(transform, source_type, "partition field")?;
    }
    Ok(())
}

pub(crate) fn validate_sort_order_sources_against_current_schema(
    metadata: &serde_json::Value,
    sort_order: &serde_json::Value,
) -> TableCatalogStoreResult<()> {
    let current_schema_fields = current_table_schema_fields(metadata, table_metadata_format_version(metadata)?)?;
    let fields = sort_order
        .get("fields")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| TableCatalogStoreError::Invalid("sort order fields must be an array".to_string()))?;
    for field in fields {
        let field = field
            .as_object()
            .ok_or_else(|| TableCatalogStoreError::Invalid("sort order fields must be JSON objects".to_string()))?;
        let source_id = required_positive_i32_value(field, "source-id", "sort field source-id")?;
        let source_type = current_schema_fields.descriptors.get(&source_id).ok_or_else(|| {
            TableCatalogStoreError::Invalid(format!("sort field source-id {source_id} does not reference the current schema"))
        })?;
        let transform = field
            .get("transform")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| TableCatalogStoreError::Invalid("sort field transform must be a non-empty string".to_string()))?;
        validate_transform_for_source(transform, source_type, "sort field")?;
    }
    Ok(())
}

fn validate_iceberg_schema(schema: &serde_json::Value, label: &str) -> TableCatalogStoreResult<BTreeSet<i32>> {
    Ok(validate_iceberg_schema_fields(schema, label)?.field_ids)
}

pub(crate) fn assign_fresh_create_schema_ids(
    schema: &mut serde_json::Value,
    partition_spec: Option<&mut serde_json::Value>,
    sort_order: Option<&mut serde_json::Value>,
) -> TableCatalogStoreResult<()> {
    let mut assigner = FreshCreateSchemaIdAssigner::new();
    assigner.assign_schema(schema)?;
    assigner.remap_identifier_field_ids(schema)?;
    if let Some(partition_spec) = partition_spec {
        assigner.remap_source_ids(partition_spec, "partition spec")?;
    }
    if let Some(sort_order) = sort_order {
        assigner.remap_source_ids(sort_order, "sort order")?;
    }
    Ok(())
}

struct FreshCreateSchemaIdAssigner {
    next_id: i32,
    old_to_new: BTreeMap<i32, i32>,
}

impl FreshCreateSchemaIdAssigner {
    fn new() -> Self {
        Self {
            next_id: 1,
            old_to_new: BTreeMap::new(),
        }
    }

    fn assign_schema(&mut self, schema: &mut serde_json::Value) -> TableCatalogStoreResult<()> {
        let schema = schema
            .as_object_mut()
            .ok_or_else(|| TableCatalogStoreError::Invalid("create schema must be a JSON object".to_string()))?;
        if schema.get("type").and_then(serde_json::Value::as_str) != Some("struct") {
            return Err(TableCatalogStoreError::Invalid("create schema type must be struct".to_string()));
        }
        let fields = schema
            .get_mut("fields")
            .and_then(serde_json::Value::as_array_mut)
            .ok_or_else(|| TableCatalogStoreError::Invalid("create schema fields must be an array".to_string()))?;
        self.assign_struct_fields(fields, 0)
    }

    fn assign_struct_fields(&mut self, fields: &mut [serde_json::Value], depth: usize) -> TableCatalogStoreResult<()> {
        for field in fields.iter_mut() {
            let field = field
                .as_object_mut()
                .ok_or_else(|| TableCatalogStoreError::Invalid("create schema fields must be JSON objects".to_string()))?;
            self.assign_object_id(field, "id", "create schema field id")?;
        }
        for field in fields {
            let field = field
                .as_object_mut()
                .ok_or_else(|| TableCatalogStoreError::Invalid("create schema fields must be JSON objects".to_string()))?;
            let field_type = field
                .get_mut("type")
                .ok_or_else(|| TableCatalogStoreError::Invalid("create schema field type is required".to_string()))?;
            self.assign_type_ids(field_type, depth)?;
        }
        Ok(())
    }

    fn assign_type_ids(&mut self, field_type: &mut serde_json::Value, depth: usize) -> TableCatalogStoreResult<()> {
        if field_type.is_string() {
            return Ok(());
        }
        if depth >= ICEBERG_MAX_SCHEMA_NESTING_DEPTH {
            return Err(TableCatalogStoreError::Invalid(
                "create schema exceeds the maximum nesting depth".to_string(),
            ));
        }
        let nested_depth = depth + 1;
        let field_type = field_type.as_object_mut().ok_or_else(|| {
            TableCatalogStoreError::Invalid("create schema field type must be a string or JSON object".to_string())
        })?;
        match field_type.get("type").and_then(serde_json::Value::as_str) {
            Some("struct") => {
                let fields = field_type
                    .get_mut("fields")
                    .and_then(serde_json::Value::as_array_mut)
                    .ok_or_else(|| TableCatalogStoreError::Invalid("create schema struct fields must be an array".to_string()))?;
                self.assign_struct_fields(fields, nested_depth)
            }
            Some("list") => {
                self.assign_object_id(field_type, "element-id", "create schema list element-id")?;
                let element = field_type
                    .get_mut("element")
                    .ok_or_else(|| TableCatalogStoreError::Invalid("create schema list element is required".to_string()))?;
                self.assign_type_ids(element, nested_depth)
            }
            Some("map") => {
                self.assign_object_id(field_type, "key-id", "create schema map key-id")?;
                self.assign_object_id(field_type, "value-id", "create schema map value-id")?;
                let key = field_type
                    .get_mut("key")
                    .ok_or_else(|| TableCatalogStoreError::Invalid("create schema map key is required".to_string()))?;
                self.assign_type_ids(key, nested_depth)?;
                let value = field_type
                    .get_mut("value")
                    .ok_or_else(|| TableCatalogStoreError::Invalid("create schema map value is required".to_string()))?;
                self.assign_type_ids(value, nested_depth)
            }
            _ => Err(TableCatalogStoreError::Invalid(
                "create schema contains an unsupported field type".to_string(),
            )),
        }
    }

    fn assign_object_id(
        &mut self,
        object: &mut serde_json::Map<String, serde_json::Value>,
        field: &str,
        label: &str,
    ) -> TableCatalogStoreResult<()> {
        let old_id = required_i32_value(object, field, label)?;
        let entry = match self.old_to_new.entry(old_id) {
            std::collections::btree_map::Entry::Occupied(_) => {
                return Err(TableCatalogStoreError::Invalid(format!("duplicate create schema field id {old_id}")));
            }
            std::collections::btree_map::Entry::Vacant(entry) => entry,
        };
        let new_id = self.next_id;
        if new_id > ICEBERG_MAX_USER_FIELD_ID {
            return Err(TableCatalogStoreError::Invalid(
                "create schema exceeds the available Iceberg field ID range".to_string(),
            ));
        }
        self.next_id = new_id.checked_add(1).ok_or_else(|| {
            TableCatalogStoreError::Invalid("create schema exceeds the available Iceberg field ID range".to_string())
        })?;
        entry.insert(new_id);
        object.insert(field.to_string(), serde_json::Value::from(new_id));
        Ok(())
    }

    fn remap_identifier_field_ids(&self, schema: &mut serde_json::Value) -> TableCatalogStoreResult<()> {
        let Some(identifier_field_ids) = schema
            .as_object_mut()
            .and_then(|schema| schema.get_mut("identifier-field-ids"))
        else {
            return Ok(());
        };
        let identifier_field_ids = identifier_field_ids
            .as_array_mut()
            .ok_or_else(|| TableCatalogStoreError::Invalid("create schema identifier-field-ids must be an array".to_string()))?;
        for field_id in identifier_field_ids {
            let old_id = required_i32(field_id, "create schema identifier field id")?;
            let new_id = self.old_to_new.get(&old_id).ok_or_else(|| {
                TableCatalogStoreError::Invalid(format!(
                    "create schema identifier field id {old_id} does not reference a schema field"
                ))
            })?;
            *field_id = serde_json::Value::from(*new_id);
        }
        Ok(())
    }

    fn remap_source_ids(&self, value: &mut serde_json::Value, label: &str) -> TableCatalogStoreResult<()> {
        let value = value
            .as_object_mut()
            .ok_or_else(|| TableCatalogStoreError::Invalid(format!("{label} must be a JSON object")))?;
        let Some(fields) = value.get_mut("fields") else {
            return Ok(());
        };
        let fields = fields
            .as_array_mut()
            .ok_or_else(|| TableCatalogStoreError::Invalid(format!("{label} fields must be an array")))?;
        for field in fields {
            let field = field
                .as_object_mut()
                .ok_or_else(|| TableCatalogStoreError::Invalid(format!("{label} fields must be JSON objects")))?;
            let old_id = required_i32_value(field, "source-id", &format!("{label} source-id"))?;
            let new_id = self.old_to_new.get(&old_id).ok_or_else(|| {
                TableCatalogStoreError::Invalid(format!("{label} source-id {old_id} does not reference the create schema"))
            })?;
            field.insert("source-id".to_string(), serde_json::Value::from(*new_id));
        }
        Ok(())
    }
}

fn validate_iceberg_schema_fields(schema: &serde_json::Value, label: &str) -> TableCatalogStoreResult<IcebergSchemaFields> {
    let schema = schema
        .as_object()
        .ok_or_else(|| TableCatalogStoreError::Invalid(format!("{label} must be a JSON object")))?;
    if schema.get("type").and_then(serde_json::Value::as_str) != Some("struct") {
        return Err(TableCatalogStoreError::Invalid(format!("{label} type must be struct")));
    }
    let fields = schema
        .get("fields")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| TableCatalogStoreError::Invalid(format!("{label} fields must be an array")))?;
    let mut schema_fields = IcebergSchemaFields::default();
    validate_struct_fields(fields, label, true, false, &mut schema_fields)?;
    if let Some(identifier_field_ids) = schema.get("identifier-field-ids") {
        let identifier_field_ids = identifier_field_ids
            .as_array()
            .ok_or_else(|| TableCatalogStoreError::Invalid(format!("{label} identifier-field-ids must be an array")))?;
        let mut seen = BTreeSet::new();
        for field_id in identifier_field_ids {
            let field_id = required_positive_i32(field_id, &format!("{label} identifier field id"))?;
            if !seen.insert(field_id) || schema_fields.identifier_eligible.get(&field_id) != Some(&true) {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "{label} identifier field id {field_id} must uniquely reference a required non-floating primitive outside lists, maps, and optional structs"
                )));
            }
        }
    }
    Ok(schema_fields)
}

#[derive(Default)]
struct IcebergSchemaFields {
    field_ids: BTreeSet<i32>,
    identifier_eligible: BTreeMap<i32, bool>,
    descriptors: BTreeMap<i32, IcebergSchemaFieldDescriptor>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct IcebergSchemaFieldDescriptor {
    field_type: IcebergFieldType,
    required: bool,
    inside_collection: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum IcebergFieldType {
    Primitive(String),
    Struct,
    List,
    Map,
}

fn iceberg_field_type(value: &serde_json::Value, label: &str) -> TableCatalogStoreResult<IcebergFieldType> {
    if let Some(primitive) = value.as_str() {
        validate_iceberg_primitive_type(primitive, label)?;
        return Ok(IcebergFieldType::Primitive(primitive.to_string()));
    }
    match value.get("type").and_then(serde_json::Value::as_str) {
        Some("struct") => Ok(IcebergFieldType::Struct),
        Some("list") => Ok(IcebergFieldType::List),
        Some("map") => Ok(IcebergFieldType::Map),
        _ => Err(TableCatalogStoreError::Invalid(format!("{label} contains an unsupported field type"))),
    }
}

fn insert_schema_field(
    schema_fields: &mut IcebergSchemaFields,
    field_id: i32,
    field_type: &serde_json::Value,
    required: bool,
    inside_collection: bool,
    label: &str,
) -> TableCatalogStoreResult<()> {
    if !schema_fields.field_ids.insert(field_id) {
        return Err(TableCatalogStoreError::Invalid(format!("duplicate {label} field id {field_id}")));
    }
    schema_fields.descriptors.insert(
        field_id,
        IcebergSchemaFieldDescriptor {
            field_type: iceberg_field_type(field_type, label)?,
            required,
            inside_collection,
        },
    );
    Ok(())
}

fn validate_schema_field_evolution(
    field_id: i32,
    previous: &IcebergSchemaFieldDescriptor,
    next: &IcebergSchemaFieldDescriptor,
) -> TableCatalogStoreResult<()> {
    if !previous.required && next.required {
        return Err(TableCatalogStoreError::Invalid(format!(
            "schema field {field_id} cannot evolve from optional to required"
        )));
    }
    if previous.inside_collection != next.inside_collection {
        return Err(TableCatalogStoreError::Invalid(format!(
            "schema field {field_id} cannot move into or out of a list or map"
        )));
    }
    if previous.field_type == next.field_type {
        return Ok(());
    }
    let compatible = match (&previous.field_type, &next.field_type) {
        (IcebergFieldType::Primitive(previous), IcebergFieldType::Primitive(next)) => {
            primitive_type_promotion_is_valid(previous, next)
        }
        _ => false,
    };
    if !compatible {
        return Err(TableCatalogStoreError::Invalid(format!(
            "schema field {field_id} has an incompatible type evolution"
        )));
    }
    Ok(())
}

fn primitive_type_promotion_is_valid(previous: &str, next: &str) -> bool {
    if matches!((previous, next), ("int", "long") | ("float", "double")) {
        return true;
    }
    let decimal = |value: &str| {
        value
            .strip_prefix("decimal(")
            .and_then(|value| value.strip_suffix(')'))
            .and_then(|parameters| parameters.split_once(','))
            .and_then(|(precision, scale)| Some((precision.trim().parse::<u8>().ok()?, scale.trim().parse::<u8>().ok()?)))
    };
    matches!((decimal(previous), decimal(next)), (Some((previous_precision, previous_scale)), Some((next_precision, next_scale))) if previous_scale == next_scale && next_precision >= previous_precision)
}

fn validate_struct_fields(
    fields: &[serde_json::Value],
    label: &str,
    required_ancestors: bool,
    inside_collection: bool,
    schema_fields: &mut IcebergSchemaFields,
) -> TableCatalogStoreResult<()> {
    for field in fields {
        let field = field
            .as_object()
            .ok_or_else(|| TableCatalogStoreError::Invalid(format!("{label} fields must be JSON objects")))?;
        let field_id = required_schema_field_id_value(field, "id", &format!("{label} field id"))?;
        if !field
            .get("name")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|name| !name.is_empty())
        {
            return Err(TableCatalogStoreError::Invalid(format!("{label} field name must be a non-empty string")));
        }
        let required = field
            .get("required")
            .and_then(serde_json::Value::as_bool)
            .ok_or_else(|| TableCatalogStoreError::Invalid(format!("{label} field required must be a boolean")))?;
        let field_type = field
            .get("type")
            .ok_or_else(|| TableCatalogStoreError::Invalid(format!("{label} field type is required")))?;
        insert_schema_field(schema_fields, field_id, field_type, required, inside_collection, label)?;
        let identifier_eligible = required_ancestors
            && required
            && !inside_collection
            && field_type
                .as_str()
                .is_some_and(|primitive| !matches!(primitive, "float" | "double"));
        schema_fields.identifier_eligible.insert(field_id, identifier_eligible);
        validate_iceberg_type(field_type, label, required_ancestors && required, inside_collection, schema_fields)?;
    }
    Ok(())
}

fn validate_iceberg_type(
    field_type: &serde_json::Value,
    label: &str,
    required_ancestors: bool,
    inside_collection: bool,
    schema_fields: &mut IcebergSchemaFields,
) -> TableCatalogStoreResult<()> {
    if let Some(primitive) = field_type.as_str() {
        return validate_iceberg_primitive_type(primitive, label);
    }
    let field_type = field_type
        .as_object()
        .ok_or_else(|| TableCatalogStoreError::Invalid(format!("{label} field type must be a string or JSON object")))?;
    match field_type.get("type").and_then(serde_json::Value::as_str) {
        Some("struct") => {
            let fields = field_type
                .get("fields")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| TableCatalogStoreError::Invalid(format!("{label} struct fields must be an array")))?;
            validate_struct_fields(fields, label, required_ancestors, inside_collection, schema_fields)
        }
        Some("list") => {
            let element_id = required_schema_field_id_value(field_type, "element-id", &format!("{label} list element-id"))?;
            let element_required = field_type
                .get("element-required")
                .and_then(serde_json::Value::as_bool)
                .ok_or_else(|| TableCatalogStoreError::Invalid(format!("{label} list element-required must be a boolean")))?;
            let element = field_type
                .get("element")
                .ok_or_else(|| TableCatalogStoreError::Invalid(format!("{label} list element is required")))?;
            insert_schema_field(schema_fields, element_id, element, element_required, true, label)?;
            validate_iceberg_type(element, label, false, true, schema_fields)
        }
        Some("map") => {
            let value_required = field_type
                .get("value-required")
                .and_then(serde_json::Value::as_bool)
                .ok_or_else(|| TableCatalogStoreError::Invalid(format!("{label} map value-required must be a boolean")))?;
            for (id_field, value_field, required) in [("key-id", "key", true), ("value-id", "value", value_required)] {
                let field_id = required_schema_field_id_value(field_type, id_field, &format!("{label} map {id_field}"))?;
                let value = field_type
                    .get(value_field)
                    .ok_or_else(|| TableCatalogStoreError::Invalid(format!("{label} map {value_field} is required")))?;
                insert_schema_field(schema_fields, field_id, value, required, true, label)?;
                validate_iceberg_type(value, label, false, true, schema_fields)?;
            }
            Ok(())
        }
        _ => Err(TableCatalogStoreError::Invalid(format!("{label} contains an unsupported field type"))),
    }
}

fn validate_iceberg_primitive_type(primitive: &str, label: &str) -> TableCatalogStoreResult<()> {
    if matches!(
        primitive,
        "boolean"
            | "int"
            | "long"
            | "float"
            | "double"
            | "date"
            | "time"
            | "timestamp"
            | "timestamptz"
            | "string"
            | "uuid"
            | "binary"
    ) {
        return Ok(());
    }
    if let Some(length) = primitive.strip_prefix("fixed[").and_then(|value| value.strip_suffix(']'))
        && length.trim().parse::<i32>().is_ok_and(|length| length > 0)
    {
        return Ok(());
    }
    if let Some(parameters) = primitive.strip_prefix("decimal(").and_then(|value| value.strip_suffix(')'))
        && let Some((precision, scale)) = parameters.split_once(',')
        && !scale.contains(',')
        && let (Ok(precision), Ok(scale)) = (precision.trim().parse::<u8>(), scale.trim().parse::<u8>())
        && (1..=38).contains(&precision)
        && scale <= precision
    {
        return Ok(());
    }
    Err(TableCatalogStoreError::Invalid(format!(
        "{label} contains unsupported primitive type {primitive}"
    )))
}

fn required_i32_value(
    object: &serde_json::Map<String, serde_json::Value>,
    field: &str,
    label: &str,
) -> TableCatalogStoreResult<i32> {
    let value = object
        .get(field)
        .ok_or_else(|| TableCatalogStoreError::Invalid(format!("{label} is required")))?;
    required_i32(value, label)
}

fn required_positive_i32_value(
    object: &serde_json::Map<String, serde_json::Value>,
    field: &str,
    label: &str,
) -> TableCatalogStoreResult<i32> {
    let value = object
        .get(field)
        .ok_or_else(|| TableCatalogStoreError::Invalid(format!("{label} is required")))?;
    required_positive_i32(value, label)
}

fn required_schema_field_id_value(
    object: &serde_json::Map<String, serde_json::Value>,
    field: &str,
    label: &str,
) -> TableCatalogStoreResult<i32> {
    let field_id = required_positive_i32_value(object, field, label)?;
    if field_id > ICEBERG_MAX_USER_FIELD_ID {
        return Err(TableCatalogStoreError::Invalid(format!(
            "{label} must not use the reserved Iceberg field ID range"
        )));
    }
    Ok(field_id)
}

fn required_i32(value: &serde_json::Value, label: &str) -> TableCatalogStoreResult<i32> {
    let value = value
        .as_i64()
        .ok_or_else(|| TableCatalogStoreError::Invalid(format!("{label} must be an integer")))?;
    i32::try_from(value).map_err(|_| TableCatalogStoreError::Invalid(format!("{label} exceeds the signed 32-bit range")))
}

fn required_positive_i32(value: &serde_json::Value, label: &str) -> TableCatalogStoreResult<i32> {
    let value = required_i32(value, label)?;
    if value <= 0 {
        return Err(TableCatalogStoreError::Invalid(format!("{label} must be positive")));
    }
    Ok(value)
}

fn validate_partition_specs(
    metadata: &serde_json::Value,
    format_version: u16,
    schema_fields: &IcebergSchemaFields,
    current_schema_fields: &IcebergSchemaFields,
) -> TableCatalogStoreResult<()> {
    let spec_fields = if format_version == 1 {
        vec![(0, require_metadata_array(metadata, "partition-spec")?)]
    } else {
        metadata
            .get("partition-specs")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| TableCatalogStoreError::Invalid("partition-specs must be an array".to_string()))?
            .iter()
            .map(|spec| {
                let spec_id = required_i32_value(
                    spec.as_object()
                        .ok_or_else(|| TableCatalogStoreError::Invalid("partition specs must be JSON objects".to_string()))?,
                    "spec-id",
                    "partition spec-id",
                )?;
                let fields = spec
                    .get("fields")
                    .and_then(serde_json::Value::as_array)
                    .ok_or_else(|| TableCatalogStoreError::Invalid("partition spec fields must be an array".to_string()))?;
                Ok((spec_id, fields))
            })
            .collect::<TableCatalogStoreResult<Vec<_>>>()?
    };
    let default_spec_id = if format_version == 1 {
        Some(0)
    } else {
        Some(require_metadata_i32(metadata, "default-spec-id")?)
    };
    let last_partition_id = (format_version != 1)
        .then(|| require_metadata_i32(metadata, "last-partition-id"))
        .transpose()?;
    if last_partition_id.is_some_and(|last_partition_id| last_partition_id < 0) {
        return Err(TableCatalogStoreError::Invalid("last-partition-id must not be negative".to_string()));
    }
    let mut assigned_fields = BTreeMap::new();
    for (spec_id, fields) in spec_fields {
        let mut field_ids = BTreeSet::new();
        for (field_index, field) in fields.iter().enumerate() {
            let field = field
                .as_object()
                .ok_or_else(|| TableCatalogStoreError::Invalid("partition spec fields must be JSON objects".to_string()))?;
            let source_id = required_positive_i32_value(field, "source-id", "partition source-id")?;
            let transform = field
                .get("transform")
                .and_then(serde_json::Value::as_str)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    TableCatalogStoreError::Invalid("partition field transform must be a non-empty string".to_string())
                })?;
            let source_fields = if default_spec_id == Some(spec_id) {
                current_schema_fields
            } else {
                schema_fields
            };
            let source_type = if transform == "void" {
                None
            } else {
                let source_type = source_fields.descriptors.get(&source_id).ok_or_else(|| {
                    TableCatalogStoreError::Invalid(format!("partition source-id {source_id} does not reference a schema field"))
                })?;
                if source_type.inside_collection {
                    return Err(TableCatalogStoreError::Invalid(format!(
                        "partition source-id {source_id} must not be nested in a list or map"
                    )));
                }
                Some(source_type)
            };
            let field_id = if format_version == 1 {
                let expected_field_id = i32::try_from(field_index)
                    .ok()
                    .and_then(|field_index| 1000_i32.checked_add(field_index))
                    .ok_or_else(|| {
                        TableCatalogStoreError::Invalid("Iceberg v1 partition spec has too many fields".to_string())
                    })?;
                match field.get("field-id") {
                    Some(field_id) => {
                        let field_id = required_positive_i32(field_id, "partition field-id")?;
                        if field_id != expected_field_id {
                            return Err(TableCatalogStoreError::Invalid(
                                "Iceberg v1 partition field-id must be sequential from 1000".to_string(),
                            ));
                        }
                        field_id
                    }
                    None => expected_field_id,
                }
            } else {
                required_positive_i32_value(field, "field-id", "partition field-id")?
            };
            if last_partition_id.is_some_and(|last_partition_id| field_id > last_partition_id) || !field_ids.insert(field_id) {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "partition field-id {field_id} must be unique and not exceed last-partition-id"
                )));
            }
            if !field
                .get("name")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|value| !value.is_empty())
            {
                return Err(TableCatalogStoreError::Invalid(
                    "partition field name must be a non-empty string".to_string(),
                ));
            }
            if let Some(source_type) = source_type {
                validate_transform_for_source(transform, source_type, "partition field")?;
            }
            let identity = (source_id, transform);
            if format_version != 1
                && let Some(previous) = assigned_fields.insert(field_id, identity)
                && previous != identity
            {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "partition field-id {field_id} is assigned to multiple partition fields"
                )));
            }
        }
    }
    Ok(())
}

fn validate_sort_orders(
    metadata: &serde_json::Value,
    schema_fields: &IcebergSchemaFields,
    current_schema_fields: &IcebergSchemaFields,
) -> TableCatalogStoreResult<()> {
    let Some(sort_orders) = metadata.get("sort-orders") else {
        return Ok(());
    };
    let sort_orders = sort_orders
        .as_array()
        .ok_or_else(|| TableCatalogStoreError::Invalid("sort-orders must be an array".to_string()))?;
    let default_sort_order_id = metadata.get("default-sort-order-id").and_then(serde_json::Value::as_i64);
    for sort_order in sort_orders {
        let sort_order = sort_order
            .as_object()
            .ok_or_else(|| TableCatalogStoreError::Invalid("sort orders must be JSON objects".to_string()))?;
        let order_id = required_i32_value(sort_order, "order-id", "sort order-id")?;
        if order_id < 0 {
            return Err(TableCatalogStoreError::Invalid("sort order-id must not be negative".to_string()));
        }
        let fields = sort_order
            .get("fields")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| TableCatalogStoreError::Invalid("sort order fields must be an array".to_string()))?;
        if order_id == 0 && !fields.is_empty() {
            return Err(TableCatalogStoreError::Invalid(
                "sort order 0 is reserved for the unsorted order".to_string(),
            ));
        }
        if order_id > 0 && fields.is_empty() {
            return Err(TableCatalogStoreError::Invalid(
                "empty sort orders must use the reserved unsorted order-id 0".to_string(),
            ));
        }
        for field in fields {
            let field = field
                .as_object()
                .ok_or_else(|| TableCatalogStoreError::Invalid("sort order fields must be JSON objects".to_string()))?;
            let source_id = required_positive_i32_value(field, "source-id", "sort field source-id")?;
            let source_fields = if default_sort_order_id == Some(i64::from(order_id)) {
                current_schema_fields
            } else {
                schema_fields
            };
            let source_type = source_fields.descriptors.get(&source_id).ok_or_else(|| {
                TableCatalogStoreError::Invalid(format!("sort field source-id {source_id} does not reference a schema field"))
            })?;
            let transform = field
                .get("transform")
                .and_then(serde_json::Value::as_str)
                .filter(|transform| !transform.is_empty())
                .ok_or_else(|| TableCatalogStoreError::Invalid("sort field transform must be a non-empty string".to_string()))?;
            validate_transform_for_source(transform, source_type, "sort field")?;
            if !field
                .get("direction")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|direction| matches!(direction, "asc" | "desc"))
            {
                return Err(TableCatalogStoreError::Invalid("sort field direction must be asc or desc".to_string()));
            }
            if !field
                .get("null-order")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|null_order| matches!(null_order, "nulls-first" | "nulls-last"))
            {
                return Err(TableCatalogStoreError::Invalid(
                    "sort field null-order must be nulls-first or nulls-last".to_string(),
                ));
            }
        }
    }
    Ok(())
}

fn validate_transform_for_source(
    transform: &str,
    source: &IcebergSchemaFieldDescriptor,
    label: &str,
) -> TableCatalogStoreResult<()> {
    if transform == "void" {
        return Ok(());
    }
    let IcebergFieldType::Primitive(source_type) = &source.field_type else {
        return Err(TableCatalogStoreError::Invalid(format!(
            "{label} transform {transform} requires a primitive source type"
        )));
    };
    let valid = match transform {
        "identity" => true,
        "year" | "month" | "day" => matches!(source_type.as_str(), "date" | "timestamp" | "timestamptz"),
        "hour" => matches!(source_type.as_str(), "timestamp" | "timestamptz"),
        _ => match transform_parameter(transform, "bucket") {
            Some(width) => {
                width > 0
                    && (matches!(
                        source_type.as_str(),
                        "int" | "long" | "date" | "time" | "timestamp" | "timestamptz" | "string" | "uuid" | "binary"
                    ) || source_type.starts_with("decimal(")
                        || source_type.starts_with("fixed["))
            }
            None => match transform_parameter(transform, "truncate") {
                Some(width) => {
                    width > 0
                        && (matches!(source_type.as_str(), "int" | "long" | "string" | "binary")
                            || source_type.starts_with("decimal("))
                }
                None => false,
            },
        },
    };
    if !valid {
        return Err(TableCatalogStoreError::Invalid(format!(
            "{label} transform {transform} is invalid for source type {source_type}"
        )));
    }
    Ok(())
}

fn transform_parameter(transform: &str, name: &str) -> Option<i32> {
    transform
        .strip_prefix(name)
        .and_then(|value| value.strip_prefix('['))
        .and_then(|value| value.strip_suffix(']'))
        .and_then(|value| value.parse::<i32>().ok())
}

fn validate_table_snapshot_fields(snapshot: &serde_json::Value, format_version: u16) -> TableCatalogStoreResult<()> {
    let snapshot = snapshot
        .as_object()
        .ok_or_else(|| TableCatalogStoreError::Invalid("snapshot must be an object".to_string()))?;
    if snapshot.get("snapshot-id").and_then(serde_json::Value::as_i64).is_none() {
        return Err(TableCatalogStoreError::Invalid("snapshot-id must be an integer".to_string()));
    }
    if snapshot.get("timestamp-ms").and_then(serde_json::Value::as_i64).is_none() {
        return Err(TableCatalogStoreError::Invalid("snapshot timestamp-ms must be an integer".to_string()));
    }
    if snapshot
        .get("parent-snapshot-id")
        .is_some_and(|parent_snapshot_id| parent_snapshot_id.as_i64().is_none())
    {
        return Err(TableCatalogStoreError::Invalid(
            "snapshot parent-snapshot-id must be an integer".to_string(),
        ));
    }
    let manifest_list = snapshot
        .get("manifest-list")
        .and_then(serde_json::Value::as_str)
        .filter(|location| !location.is_empty());
    let manifests = snapshot.get("manifests");
    if manifests.is_some_and(|manifests| !manifests.is_array()) {
        return Err(TableCatalogStoreError::Invalid("snapshot manifests must be an array".to_string()));
    }
    let summary = snapshot.get("summary");
    if let Some(summary) = summary {
        validate_string_map(summary, "snapshot summary")?;
    }

    match format_version {
        1 => {
            if manifest_list.is_some() && manifests.is_some() {
                return Err(TableCatalogStoreError::Invalid(
                    "Iceberg v1 snapshot must not contain both manifest-list and manifests".to_string(),
                ));
            }
            if manifest_list.is_none() && manifests.is_none() {
                return Err(TableCatalogStoreError::Invalid(
                    "Iceberg v1 snapshot requires manifest-list or manifests".to_string(),
                ));
            }
        }
        2 => {
            if manifest_list.is_some() && manifests.is_some() {
                return Err(TableCatalogStoreError::Invalid(
                    "Iceberg v2 snapshot must not contain both manifest-list and manifests".to_string(),
                ));
            }
            if manifest_list.is_none() && manifests.is_none() {
                return Err(TableCatalogStoreError::Invalid(
                    "Iceberg v2 snapshot requires manifest-list or v1-compatible manifests".to_string(),
                ));
            }
            let summary = summary
                .and_then(serde_json::Value::as_object)
                .ok_or_else(|| TableCatalogStoreError::Invalid("Iceberg v2 snapshot requires summary".to_string()))?;
            if !summary
                .get("operation")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|operation| !operation.is_empty())
            {
                return Err(TableCatalogStoreError::Invalid(
                    "Iceberg v2 snapshot summary requires a non-empty operation".to_string(),
                ));
            }
        }
        _ => {
            return Err(TableCatalogStoreError::Internal(format!(
                "validated Iceberg format version {format_version} is unsupported"
            )));
        }
    }
    Ok(())
}

fn validate_snapshot_ref_retention(
    name: &str,
    reference_type: &str,
    reference: &serde_json::Map<String, serde_json::Value>,
) -> TableCatalogStoreResult<()> {
    for field in ["min-snapshots-to-keep", "max-snapshot-age-ms", "max-ref-age-ms"] {
        if let Some(value) = reference.get(field)
            && !value.as_i64().is_some_and(|value| value > 0)
        {
            return Err(TableCatalogStoreError::Invalid(format!(
                "snapshot ref {name} field {field} must be a positive integer"
            )));
        }
    }
    if reference_type == "tag"
        && (reference.contains_key("min-snapshots-to-keep") || reference.contains_key("max-snapshot-age-ms"))
    {
        return Err(TableCatalogStoreError::Invalid(format!(
            "snapshot tag {name} contains branch-only retention fields"
        )));
    }
    Ok(())
}

pub(crate) fn table_metadata_partition_spec_ids(metadata: &serde_json::Value) -> TableCatalogStoreResult<BTreeSet<i32>> {
    let ids = metadata_array_i32_ids(metadata, "partition-specs", "spec-id", "partition spec")?;
    if !ids.is_empty() {
        return ids
            .into_iter()
            .map(|id| {
                i32::try_from(id).map_err(|_| {
                    TableCatalogStoreError::Invalid(format!("partition spec id {id} exceeds the signed 32-bit range"))
                })
            })
            .collect();
    }
    if metadata.get("partition-spec").is_some_and(serde_json::Value::is_array) || metadata.get("partition-specs").is_none() {
        return Ok(BTreeSet::from([0]));
    }
    Err(TableCatalogStoreError::Invalid("table metadata has no partition specs".to_string()))
}

pub(crate) fn validate_supported_view_metadata(metadata: &serde_json::Value) -> TableCatalogStoreResult<()> {
    let object = metadata
        .as_object()
        .ok_or_else(|| TableCatalogStoreError::Invalid("view metadata must be a JSON object".to_string()))?;
    let format_version = object
        .get("format-version")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| TableCatalogStoreError::Invalid("view metadata is missing integer field format-version".to_string()))?;
    if format_version != 1 {
        return Err(TableCatalogStoreError::Unsupported(format!(
            "Iceberg view format-version {format_version}"
        )));
    }
    for field in ["view-uuid", "location"] {
        if !object
            .get(field)
            .and_then(serde_json::Value::as_str)
            .is_some_and(|value| !value.is_empty())
        {
            return Err(TableCatalogStoreError::Invalid(format!(
                "view metadata is missing non-empty string field {field}"
            )));
        }
    }
    let schemas = view_metadata_array(metadata, "schemas")?;
    if schemas.is_empty() {
        return Err(TableCatalogStoreError::Invalid("view metadata schemas must not be empty".to_string()));
    }
    for schema in schemas {
        validate_iceberg_schema(schema, "view schema")?;
    }
    let schema_ids = metadata_array_i32_ids(metadata, "schemas", "schema-id", "schema")?;
    validate_metadata_id_reference(metadata, "current-schema-id", &schema_ids, "schema")?;
    let versions = view_metadata_array(metadata, "versions")?;
    if versions.is_empty() {
        return Err(TableCatalogStoreError::Invalid("view metadata versions must not be empty".to_string()));
    }
    if object.get("current-version-id").and_then(serde_json::Value::as_i64).is_none() {
        return Err(TableCatalogStoreError::Invalid(
            "view metadata is missing integer field current-version-id".to_string(),
        ));
    }
    let version_ids = metadata_array_i32_ids(metadata, "versions", "version-id", "view version")?;
    validate_metadata_id_reference(metadata, "current-version-id", &version_ids, "view version")?;
    for version in versions {
        validate_view_version(version)?;
        let schema_id = version
            .get("schema-id")
            .and_then(serde_json::Value::as_i64)
            .ok_or_else(|| TableCatalogStoreError::Invalid("view version is missing schema-id".to_string()))?;
        if !schema_ids.contains(&schema_id) {
            return Err(TableCatalogStoreError::Invalid(format!(
                "view version schema-id targets schema {schema_id}, which does not exist"
            )));
        }
    }
    let version_log = view_metadata_array(metadata, "version-log")?;
    for entry in version_log {
        let version_id = entry
            .get("version-id")
            .and_then(serde_json::Value::as_i64)
            .ok_or_else(|| TableCatalogStoreError::Invalid("view version-log entry is missing version-id".to_string()))?;
        if entry.get("timestamp-ms").and_then(serde_json::Value::as_i64).is_none() {
            return Err(TableCatalogStoreError::Invalid(
                "view version-log entries require integer version-id and timestamp-ms".to_string(),
            ));
        }
        if !version_ids.contains(&version_id) {
            return Err(TableCatalogStoreError::Invalid(format!(
                "view version-log targets view version {version_id}, which does not exist"
            )));
        }
    }
    if let Some(properties) = object.get("properties") {
        validate_string_map(properties, "view metadata properties")?;
    }
    Ok(())
}

fn view_metadata_array<'a>(metadata: &'a serde_json::Value, field: &str) -> TableCatalogStoreResult<&'a Vec<serde_json::Value>> {
    metadata
        .get(field)
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| TableCatalogStoreError::Invalid(format!("view metadata is missing array field {field}")))
}

fn validate_view_version(version: &serde_json::Value) -> TableCatalogStoreResult<()> {
    let version = version
        .as_object()
        .ok_or_else(|| TableCatalogStoreError::Invalid("view version must be a JSON object".to_string()))?;
    for field in ["version-id", "timestamp-ms", "schema-id"] {
        if version.get(field).and_then(serde_json::Value::as_i64).is_none() {
            return Err(TableCatalogStoreError::Invalid(format!("view version is missing integer field {field}")));
        }
    }
    let summary = version
        .get("summary")
        .ok_or_else(|| TableCatalogStoreError::Invalid("view version is missing summary".to_string()))?;
    validate_string_map(summary, "view version summary")?;
    let default_namespace = version
        .get("default-namespace")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| TableCatalogStoreError::Invalid("view version is missing default-namespace".to_string()))?;
    if default_namespace.iter().any(|segment| !segment.is_string()) {
        return Err(TableCatalogStoreError::Invalid(
            "view version default-namespace must contain strings".to_string(),
        ));
    }
    if version.get("default-catalog").is_some_and(|value| !value.is_string()) {
        return Err(TableCatalogStoreError::Invalid(
            "view version default-catalog must be a string".to_string(),
        ));
    }
    let representations = version
        .get("representations")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| TableCatalogStoreError::Invalid("view version is missing representations".to_string()))?;
    let mut dialects = BTreeSet::new();
    for representation in representations {
        let representation = representation
            .as_object()
            .ok_or_else(|| TableCatalogStoreError::Invalid("view representation must be a JSON object".to_string()))?;
        let dialect = representation
            .get("dialect")
            .and_then(serde_json::Value::as_str)
            .filter(|dialect| !dialect.is_empty());
        if representation.get("type").and_then(serde_json::Value::as_str) != Some("sql")
            || representation.get("sql").and_then(serde_json::Value::as_str).is_none()
            || dialect.is_none()
        {
            return Err(TableCatalogStoreError::Invalid(
                "view representation requires type sql, sql, and dialect strings".to_string(),
            ));
        }
        if !dialects.insert(
            dialect
                .ok_or_else(|| {
                    TableCatalogStoreError::Invalid("view representation dialect must be a non-empty string".to_string())
                })?
                .to_lowercase(),
        ) {
            return Err(TableCatalogStoreError::Invalid(
                "view version contains duplicate SQL dialect representations".to_string(),
            ));
        }
    }
    Ok(())
}

fn validate_string_map(value: &serde_json::Value, label: &str) -> TableCatalogStoreResult<()> {
    let values = value
        .as_object()
        .ok_or_else(|| TableCatalogStoreError::Invalid(format!("{label} must be a JSON object")))?;
    if values.values().any(|value| !value.is_string()) {
        return Err(TableCatalogStoreError::Invalid(format!("{label} values must be strings")));
    }
    Ok(())
}

fn metadata_object_mut(
    metadata: &mut serde_json::Value,
) -> TableCatalogStoreResult<&mut serde_json::Map<String, serde_json::Value>> {
    metadata
        .as_object_mut()
        .ok_or_else(|| TableCatalogStoreError::Invalid("table metadata must be a JSON object".to_string()))
}

fn metadata_array_ids(
    metadata: &serde_json::Value,
    array_field: &str,
    id_field: &str,
    label: &str,
) -> TableCatalogStoreResult<BTreeSet<i64>> {
    let Some(values) = metadata.get(array_field) else {
        return Ok(BTreeSet::new());
    };
    let values = values
        .as_array()
        .ok_or_else(|| TableCatalogStoreError::Invalid(format!("{array_field} must be an array")))?;
    let mut ids = BTreeSet::new();
    for value in values {
        let id = value
            .get(id_field)
            .and_then(serde_json::Value::as_i64)
            .ok_or_else(|| TableCatalogStoreError::Invalid(format!("{label} is missing {id_field}")))?;
        if !ids.insert(id) {
            return Err(TableCatalogStoreError::Invalid(format!("duplicate {label} id {id}")));
        }
    }
    Ok(ids)
}

fn metadata_array_i32_ids(
    metadata: &serde_json::Value,
    array_field: &str,
    id_field: &str,
    label: &str,
) -> TableCatalogStoreResult<BTreeSet<i64>> {
    let ids = metadata_array_ids(metadata, array_field, id_field, label)?;
    if let Some(id) = ids.iter().find(|id| **id < 0) {
        return Err(TableCatalogStoreError::Invalid(format!("{label} id {id} must not be negative")));
    }
    if let Some(id) = ids.iter().find(|id| i32::try_from(**id).is_err()) {
        return Err(TableCatalogStoreError::Invalid(format!(
            "{label} id {id} exceeds the signed 32-bit range"
        )));
    }
    Ok(ids)
}

fn validate_metadata_id_reference(
    metadata: &serde_json::Value,
    reference_field: &str,
    ids: &BTreeSet<i64>,
    label: &str,
) -> TableCatalogStoreResult<()> {
    let Some(value) = metadata.get(reference_field) else {
        return Ok(());
    };
    let id = value
        .as_i64()
        .ok_or_else(|| TableCatalogStoreError::Invalid(format!("{reference_field} must be an integer")))?;
    if !ids.contains(&id) {
        return Err(TableCatalogStoreError::Invalid(format!(
            "{reference_field} targets {label} {id}, which does not exist"
        )));
    }
    Ok(())
}

fn require_metadata_i64(metadata: &serde_json::Value, field: &str) -> TableCatalogStoreResult<i64> {
    metadata
        .get(field)
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| TableCatalogStoreError::Invalid(format!("table metadata is missing integer field {field}")))
}

fn require_metadata_i32(metadata: &serde_json::Value, field: &str) -> TableCatalogStoreResult<i32> {
    let value = require_metadata_i64(metadata, field)?;
    i32::try_from(value)
        .map_err(|_| TableCatalogStoreError::Invalid(format!("table metadata field {field} exceeds the signed 32-bit range")))
}

fn require_metadata_array<'a>(
    metadata: &'a serde_json::Value,
    field: &str,
) -> TableCatalogStoreResult<&'a Vec<serde_json::Value>> {
    metadata
        .get(field)
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| TableCatalogStoreError::Invalid(format!("table metadata is missing array field {field}")))
}

fn max_partition_field_id(value: &serde_json::Value) -> i64 {
    let mut max_id = 999;
    let Some(fields) = value.get("fields").and_then(serde_json::Value::as_array) else {
        return max_id;
    };
    for field in fields {
        if let Some(field_id) = field.get("field-id").and_then(serde_json::Value::as_i64) {
            max_id = max_id.max(field_id);
        }
    }
    max_id
}

pub(crate) struct TableSnapshotGraphValidationContext<'a, B> {
    backend: &'a B,
    table_bucket: &'a str,
    entry: &'a TableEntry,
}

impl<'a, B> TableSnapshotGraphValidationContext<'a, B> {
    pub(crate) fn new(backend: &'a B, table_bucket: &'a str, entry: &'a TableEntry) -> Self {
        Self {
            backend,
            table_bucket,
            entry,
        }
    }
}

#[derive(Default)]
struct SnapshotGraphReadBudget {
    manifest_count: usize,
    manifest_traversal_count: usize,
    avro_bytes: usize,
    decoded_avro_bytes: usize,
    file_reference_count: usize,
    manifest_lists: BTreeMap<String, Arc<Vec<SnapshotGraphManifestLocation>>>,
    manifests: BTreeMap<String, CachedSnapshotGraphManifest>,
    validated_live_objects: BTreeSet<String>,
}

#[derive(Clone)]
struct CachedSnapshotGraphManifest {
    object_size: usize,
    partition_spec_id: Option<i32>,
    references: Arc<Vec<ManifestDataFileReference>>,
}

impl SnapshotGraphReadBudget {
    fn charge_manifests(&mut self, count: usize) -> TableCatalogStoreResult<()> {
        self.manifest_count = self
            .manifest_count
            .checked_add(count)
            .ok_or_else(|| TableCatalogStoreError::Invalid("snapshot manifest count exceeds the commit limit".to_string()))?;
        if self.manifest_count > TABLE_COMMIT_MAX_MANIFESTS {
            return Err(TableCatalogStoreError::Invalid(
                "snapshot manifest count exceeds the commit limit".to_string(),
            ));
        }
        Ok(())
    }

    fn charge_manifest_traversals(&mut self, count: usize) -> TableCatalogStoreResult<()> {
        self.manifest_traversal_count = self.manifest_traversal_count.checked_add(count).ok_or_else(|| {
            TableCatalogStoreError::Invalid("snapshot manifest traversal count exceeds the commit limit".to_string())
        })?;
        if self.manifest_traversal_count > TABLE_COMMIT_MAX_MANIFEST_TRAVERSALS {
            return Err(TableCatalogStoreError::Invalid(
                "snapshot manifest traversal count exceeds the commit limit".to_string(),
            ));
        }
        Ok(())
    }

    fn charge_avro_bytes(&mut self, count: usize) -> TableCatalogStoreResult<()> {
        self.avro_bytes = self
            .avro_bytes
            .checked_add(count)
            .ok_or_else(|| TableCatalogStoreError::Invalid("snapshot Avro bytes exceed the commit limit".to_string()))?;
        if self.avro_bytes > TABLE_COMMIT_MAX_AVRO_BYTES {
            return Err(TableCatalogStoreError::Invalid("snapshot Avro bytes exceed the commit limit".to_string()));
        }
        Ok(())
    }

    fn charge_decoded_avro_bytes(&mut self, count: usize) -> TableCatalogStoreResult<()> {
        self.decoded_avro_bytes = self
            .decoded_avro_bytes
            .checked_add(count)
            .ok_or_else(|| TableCatalogStoreError::Invalid("snapshot decoded Avro bytes exceed the commit limit".to_string()))?;
        if self.decoded_avro_bytes > TABLE_COMMIT_MAX_AVRO_BYTES {
            return Err(TableCatalogStoreError::Invalid(
                "snapshot decoded Avro bytes exceed the commit limit".to_string(),
            ));
        }
        Ok(())
    }

    fn charge_file_references(&mut self, count: usize) -> TableCatalogStoreResult<()> {
        self.file_reference_count = self
            .file_reference_count
            .checked_add(count)
            .ok_or_else(|| TableCatalogStoreError::Invalid("snapshot file references exceed the commit limit".to_string()))?;
        if self.file_reference_count > TABLE_COMMIT_MAX_FILE_REFERENCES {
            return Err(TableCatalogStoreError::Invalid(
                "snapshot file references exceed the commit limit".to_string(),
            ));
        }
        Ok(())
    }
}

struct SnapshotGraphManifestLocation {
    manifest_path: String,
    format_version: u16,
    manifest_length: Option<u64>,
    partition_spec_id: Option<i32>,
    content: Option<i32>,
    sequence_number: Option<i64>,
    min_sequence_number: Option<i64>,
    added_snapshot_id: Option<i64>,
    #[allow(
        dead_code,
        reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
    )]
    added_files_count: Option<u64>,
    #[allow(
        dead_code,
        reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
    )]
    existing_files_count: Option<u64>,
    #[allow(
        dead_code,
        reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
    )]
    deleted_files_count: Option<u64>,
    #[allow(
        dead_code,
        reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
    )]
    added_rows_count: Option<u64>,
    #[allow(
        dead_code,
        reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
    )]
    existing_rows_count: Option<u64>,
    #[allow(
        dead_code,
        reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
    )]
    deleted_rows_count: Option<u64>,
    from_manifest_list: bool,
}

pub(crate) async fn validate_table_snapshot_changes<B>(
    context: &TableSnapshotGraphValidationContext<'_, B>,
    current_metadata: Option<&serde_json::Value>,
    metadata: &serde_json::Value,
) -> TableCatalogStoreResult<()>
where
    B: TableCatalogObjectBackend,
{
    validate_supported_table_metadata_fields(metadata)?;
    let snapshot_ids = metadata_array_ids(metadata, "snapshots", "snapshot-id", "snapshot")?;
    validate_table_statistics_references(metadata, &snapshot_ids)?;
    let mut budget = SnapshotGraphReadBudget::default();
    validate_table_statistics_objects(context, metadata).await?;
    let format_version = table_metadata_format_version(metadata)?;
    let snapshots = snapshots_requiring_graph_validation(current_metadata, metadata)?;
    for snapshot in snapshots {
        snapshot
            .get("snapshot-id")
            .and_then(serde_json::Value::as_i64)
            .ok_or_else(|| TableCatalogStoreError::Invalid("snapshot-id must be an integer".to_string()))?;
        if format_version == 2
            && snapshot.get("manifests").is_some()
            && !snapshot_is_retained_v1_history(current_metadata, snapshot)
        {
            return Err(TableCatalogStoreError::Invalid(
                "new Iceberg v2 snapshots require manifest-list".to_string(),
            ));
        }
        let snapshot_sequence_number = snapshot
            .get("sequence-number")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0);
        let manifests = snapshot_graph_manifest_references(
            context,
            metadata,
            snapshot,
            format_version,
            snapshot_sequence_number,
            &mut budget,
        )
        .await?;
        let mut seen_files = BTreeSet::new();
        for references in &manifests {
            for reference in references.iter() {
                if !seen_files.insert(reference.location.as_str()) {
                    return Err(TableCatalogStoreError::Invalid(
                        "snapshot contains a duplicate file reference".to_string(),
                    ));
                }
            }
        }
    }
    Ok(())
}

fn snapshot_is_retained_v1_history(current_metadata: Option<&serde_json::Value>, target_snapshot: &serde_json::Value) -> bool {
    let Some(current_metadata) = current_metadata else {
        return false;
    };
    if table_metadata_format_version(current_metadata).ok() != Some(1) {
        return false;
    }
    let Some(snapshot_id) = target_snapshot.get("snapshot-id").and_then(serde_json::Value::as_i64) else {
        return false;
    };
    let Some(current_snapshot) = current_metadata
        .get("snapshots")
        .and_then(serde_json::Value::as_array)
        .and_then(|snapshots| {
            snapshots
                .iter()
                .find(|snapshot| snapshot.get("snapshot-id").and_then(serde_json::Value::as_i64) == Some(snapshot_id))
        })
    else {
        return false;
    };
    let mut current_snapshot = current_snapshot.clone();
    let mut target_snapshot = target_snapshot.clone();
    for snapshot in [&mut current_snapshot, &mut target_snapshot] {
        if snapshot.get("sequence-number").and_then(serde_json::Value::as_i64) == Some(0)
            && let Some(object) = snapshot.as_object_mut()
        {
            object.remove("sequence-number");
        }
    }
    current_snapshot == target_snapshot
}

async fn validate_table_statistics_objects<B>(
    context: &TableSnapshotGraphValidationContext<'_, B>,
    metadata: &serde_json::Value,
) -> TableCatalogStoreResult<()>
where
    B: TableCatalogObjectBackend,
{
    if context.entry.table_bucket != context.table_bucket {
        return Err(TableCatalogStoreError::Invalid(
            "statistics object is outside the table bucket".to_string(),
        ));
    }
    let warehouse_object_prefix = table_warehouse_object_prefix(context.entry)?;
    let mut objects = BTreeMap::new();
    let mut total_size = 0usize;
    for (field, kind) in [
        ("statistics", IcebergStatisticsFileKind::Table),
        ("partition-statistics", IcebergStatisticsFileKind::Partition),
    ] {
        let Some(values) = metadata.get(field).and_then(serde_json::Value::as_array) else {
            continue;
        };
        for value in values {
            let location = value
                .get("statistics-path")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| TableCatalogStoreError::Invalid(format!("{field}.statistics-path must be a string")))?;
            let object_key = table_catalog_object_key_from_location(context.table_bucket, location)
                .ok_or_else(|| TableCatalogStoreError::Invalid(format!("{field} object location is invalid")))?;
            if !object_key.starts_with(&warehouse_object_prefix) {
                return Err(TableCatalogStoreError::Invalid(format!("{field} object is outside the table warehouse")));
            }
            let file_size = value
                .get("file-size-in-bytes")
                .and_then(serde_json::Value::as_u64)
                .and_then(|size| usize::try_from(size).ok())
                .filter(|size| *size <= TABLE_STATISTICS_FILE_MAX_SIZE)
                .ok_or_else(|| {
                    TableCatalogStoreError::Invalid(format!("{field} file-size-in-bytes exceeds the validation limit"))
                })?;
            if let Some(previous) = objects.get(&object_key) {
                if *previous != (file_size, kind) {
                    return Err(TableCatalogStoreError::Invalid(
                        "statistics object is declared with inconsistent metadata".to_string(),
                    ));
                }
                continue;
            }
            total_size = total_size
                .checked_add(file_size)
                .filter(|size| *size <= TABLE_COMMIT_MAX_STATISTICS_BYTES)
                .ok_or_else(|| {
                    TableCatalogStoreError::Invalid("statistics bytes exceed the commit validation limit".to_string())
                })?;
            objects.insert(object_key, (file_size, kind));
        }
    }

    if objects.len() > TABLE_COMMIT_MAX_STATISTICS_OBJECTS {
        return Err(TableCatalogStoreError::Invalid(
            "statistics object count exceeds the commit limit".to_string(),
        ));
    }
    let backend = context.backend.clone();
    let bucket = context.table_bucket.to_string();
    stream::iter(objects)
        .map(move |(object_key, (expected_size, kind))| {
            let backend = backend.clone();
            let bucket = bucket.clone();
            async move {
                let object = backend
                    .read_object_limited(&bucket, &object_key, expected_size)
                    .await?
                    .ok_or_else(|| TableCatalogStoreError::Invalid("statistics object is missing".to_string()))?;
                if object.data.len() != expected_size {
                    return Err(TableCatalogStoreError::Invalid(
                        "statistics file-size-in-bytes does not match the object".to_string(),
                    ));
                }
                let valid_magic = match kind {
                    IcebergStatisticsFileKind::Table => object.data.starts_with(b"PFA1") && object.data.ends_with(b"PFA1"),
                    IcebergStatisticsFileKind::Partition => object.data.starts_with(b"PAR1") && object.data.ends_with(b"PAR1"),
                };
                if !valid_magic {
                    return Err(TableCatalogStoreError::Invalid(match kind {
                        IcebergStatisticsFileKind::Table => "table statistics object is not a Puffin file".to_string(),
                        IcebergStatisticsFileKind::Partition => "partition statistics object is not a Parquet file".to_string(),
                    }));
                }
                Ok(())
            }
        })
        .buffered(TABLE_COMMIT_OBJECT_VALIDATION_CONCURRENCY)
        .try_for_each(|()| async { Ok(()) })
        .await
}

async fn validate_object_keys_exist<B>(
    backend: &B,
    bucket: &str,
    object_keys: impl IntoIterator<Item = String>,
    missing_message: &'static str,
) -> TableCatalogStoreResult<()>
where
    B: TableCatalogObjectBackend,
{
    let backend = backend.clone();
    let bucket = bucket.to_string();
    stream::iter(object_keys)
        .map(move |object_key| {
            let backend = backend.clone();
            let bucket = bucket.clone();
            async move {
                if !backend.object_exists(&bucket, &object_key).await? {
                    return Err(TableCatalogStoreError::Invalid(missing_message.to_string()));
                }
                Ok(())
            }
        })
        .buffered(TABLE_COMMIT_OBJECT_VALIDATION_CONCURRENCY)
        .try_for_each(|()| async { Ok(()) })
        .await
}

fn snapshots_requiring_graph_validation<'a>(
    current_metadata: Option<&serde_json::Value>,
    metadata: &'a serde_json::Value,
) -> TableCatalogStoreResult<Vec<&'a serde_json::Value>> {
    let Some(snapshots) = metadata.get("snapshots") else {
        return Ok(Vec::new());
    };
    let snapshots = snapshots
        .as_array()
        .ok_or_else(|| TableCatalogStoreError::Invalid("snapshots must be an array".to_string()))?;

    if let Some(current_metadata) = current_metadata {
        if !partition_specs_preserve_existing_definitions(current_metadata, metadata)? {
            return Ok(snapshots.iter().collect());
        }
        let current_snapshots = current_metadata
            .get("snapshots")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|snapshot| {
                snapshot
                    .get("snapshot-id")
                    .and_then(serde_json::Value::as_i64)
                    .map(|snapshot_id| (snapshot_id, snapshot))
            })
            .collect::<BTreeMap<_, _>>();
        return Ok(snapshots
            .iter()
            .filter_map(|snapshot| {
                let snapshot_id = snapshot.get("snapshot-id").and_then(serde_json::Value::as_i64)?;
                (current_snapshots.get(&snapshot_id).copied() != Some(snapshot)).then_some(snapshot)
            })
            .collect());
    }

    Ok(snapshots.iter().collect())
}

fn partition_specs_preserve_existing_definitions(
    current_metadata: &serde_json::Value,
    metadata: &serde_json::Value,
) -> TableCatalogStoreResult<bool> {
    let current_specs = normalized_partition_spec_definitions(current_metadata)?;
    let target_specs = normalized_partition_spec_definitions(metadata)?;
    Ok(current_specs
        .iter()
        .all(|(spec_id, fields)| target_specs.get(spec_id) == Some(fields)))
}

type NormalizedPartitionFieldDefinition = (i32, i32, String, String);
type NormalizedPartitionSpecDefinitions = BTreeMap<i32, Vec<NormalizedPartitionFieldDefinition>>;

fn normalized_partition_spec_definitions(
    metadata: &serde_json::Value,
) -> TableCatalogStoreResult<NormalizedPartitionSpecDefinitions> {
    let format_version = table_metadata_format_version(metadata)?;
    let specs = if format_version == 1 {
        vec![(0, require_metadata_array(metadata, "partition-spec")?)]
    } else {
        require_metadata_array(metadata, "partition-specs")?
            .iter()
            .map(|spec| {
                let spec = spec
                    .as_object()
                    .ok_or_else(|| TableCatalogStoreError::Invalid("partition specs must be JSON objects".to_string()))?;
                let spec_id = required_i32_value(spec, "spec-id", "partition spec-id")?;
                let fields = spec
                    .get("fields")
                    .and_then(serde_json::Value::as_array)
                    .ok_or_else(|| TableCatalogStoreError::Invalid("partition spec fields must be an array".to_string()))?;
                Ok((spec_id, fields))
            })
            .collect::<TableCatalogStoreResult<Vec<_>>>()?
    };
    specs
        .into_iter()
        .map(|(spec_id, fields)| {
            let fields = fields
                .iter()
                .enumerate()
                .map(|(index, field)| {
                    let field = field.as_object().ok_or_else(|| {
                        TableCatalogStoreError::Invalid("partition spec fields must be JSON objects".to_string())
                    })?;
                    let source_id = required_positive_i32_value(field, "source-id", "partition source-id")?;
                    let field_id = match field.get("field-id") {
                        Some(field_id) => required_positive_i32(field_id, "partition field-id")?,
                        None if format_version == 1 => i32::try_from(index)
                            .ok()
                            .and_then(|index| 1000_i32.checked_add(index))
                            .ok_or_else(|| {
                                TableCatalogStoreError::Invalid("Iceberg v1 partition spec has too many fields".to_string())
                            })?,
                        None => {
                            return Err(TableCatalogStoreError::Invalid("partition field-id is required".to_string()));
                        }
                    };
                    let transform = field.get("transform").and_then(serde_json::Value::as_str).ok_or_else(|| {
                        TableCatalogStoreError::Invalid("partition field transform must be a string".to_string())
                    })?;
                    let name = field
                        .get("name")
                        .and_then(serde_json::Value::as_str)
                        .ok_or_else(|| TableCatalogStoreError::Invalid("partition field name must be a string".to_string()))?;
                    Ok((source_id, field_id, name.to_string(), transform.to_string()))
                })
                .collect::<TableCatalogStoreResult<Vec<_>>>()?;
            Ok((spec_id, fields))
        })
        .collect()
}

async fn snapshot_graph_manifest_references<B>(
    context: &TableSnapshotGraphValidationContext<'_, B>,
    metadata: &serde_json::Value,
    snapshot: &serde_json::Value,
    format_version: u16,
    snapshot_sequence_number: i64,
    budget: &mut SnapshotGraphReadBudget,
) -> TableCatalogStoreResult<Vec<Arc<Vec<ManifestDataFileReference>>>>
where
    B: TableCatalogObjectBackend,
{
    let manifest_locations =
        snapshot_graph_manifest_locations(context, snapshot, format_version, snapshot_sequence_number, budget).await?;
    let partition_spec_ids = table_metadata_partition_spec_ids(metadata)?;
    let mut manifests = Vec::with_capacity(manifest_locations.len());
    let mut seen_manifest_paths = BTreeSet::new();
    for manifest_location in manifest_locations.iter() {
        budget.charge_manifest_traversals(1)?;
        validate_snapshot_graph_manifest_location(manifest_location, format_version, snapshot_sequence_number)?;
        match manifest_location.partition_spec_id {
            Some(partition_spec_id) if !partition_spec_ids.contains(&partition_spec_id) => {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "snapshot manifest references missing partition spec {partition_spec_id}"
                )));
            }
            None if manifest_location.from_manifest_list => {
                return Err(TableCatalogStoreError::Invalid(
                    "manifest-list entry is missing partition_spec_id".to_string(),
                ));
            }
            _ => {}
        }
        if !seen_manifest_paths.insert(manifest_location.manifest_path.as_str()) {
            return Err(TableCatalogStoreError::Invalid(
                "snapshot contains a duplicate manifest reference".to_string(),
            ));
        }
        let manifest_key = snapshot_graph_object_key(
            context,
            &manifest_location.manifest_path,
            TableMetadataMaintenanceObjectKind::ManifestFile,
        )?;
        let cached_manifest = if let Some(manifest) = budget.manifests.get(&manifest_key) {
            manifest.clone()
        } else {
            budget.charge_manifests(1)?;
            let manifest_object = context
                .backend
                .read_object_limited(context.table_bucket, &manifest_key, TABLE_MANIFEST_AVRO_MAX_SIZE)
                .await?
                .ok_or_else(|| TableCatalogStoreError::Invalid("snapshot manifest object is missing".to_string()))?;
            let manifest_size = manifest_object.data.len();
            budget.charge_avro_bytes(manifest_size)?;
            let decoded_manifest = decode_manifest_avro_async(manifest_object.data).await?;
            budget.charge_decoded_avro_bytes(decoded_manifest.decoded_size)?;
            let manifest = CachedSnapshotGraphManifest {
                object_size: manifest_size,
                partition_spec_id: decoded_manifest.partition_spec_id,
                references: Arc::new(decoded_manifest.references),
            };
            budget.manifests.insert(manifest_key, manifest.clone());
            manifest
        };
        if manifest_location
            .manifest_length
            .is_some_and(|declared| u64::try_from(cached_manifest.object_size).ok() != Some(declared))
        {
            return Err(TableCatalogStoreError::Invalid(
                "manifest-list manifest_length does not match the manifest object".to_string(),
            ));
        }
        if manifest_location.from_manifest_list
            && cached_manifest
                .partition_spec_id
                .is_some_and(|manifest_spec_id| Some(manifest_spec_id) != manifest_location.partition_spec_id)
        {
            return Err(TableCatalogStoreError::Invalid(
                "manifest partition-spec-id does not match its manifest-list entry".to_string(),
            ));
        }
        let references = cached_manifest.references;
        validate_snapshot_graph_manifest_content(manifest_location, references.as_ref())?;
        budget.charge_file_references(references.len())?;
        validate_snapshot_graph_data_files(
            context,
            references.as_ref(),
            budget,
            format_version,
            if manifest_location.from_manifest_list && manifest_location.format_version == 1 {
                0
            } else {
                manifest_location.sequence_number.unwrap_or(snapshot_sequence_number)
            },
        )
        .await?;
        manifests.push(references);
    }
    Ok(manifests)
}

fn validate_snapshot_graph_manifest_content(
    manifest: &SnapshotGraphManifestLocation,
    references: &[ManifestDataFileReference],
) -> TableCatalogStoreResult<()> {
    let content_matches = match manifest.content {
        Some(0) => references
            .iter()
            .all(|reference| reference.content == ManifestDataFileContent::Data),
        Some(1) => references
            .iter()
            .all(|reference| reference.content != ManifestDataFileContent::Data),
        None => true,
        Some(_) => false,
    };
    if !content_matches {
        return Err(TableCatalogStoreError::Invalid(
            "manifest-list content does not match manifest file content".to_string(),
        ));
    }
    Ok(())
}

async fn snapshot_graph_manifest_locations<B>(
    context: &TableSnapshotGraphValidationContext<'_, B>,
    snapshot: &serde_json::Value,
    format_version: u16,
    snapshot_sequence_number: i64,
    budget: &mut SnapshotGraphReadBudget,
) -> TableCatalogStoreResult<Arc<Vec<SnapshotGraphManifestLocation>>>
where
    B: TableCatalogObjectBackend,
{
    if let Some(manifest_list_location) = snapshot.get("manifest-list").and_then(serde_json::Value::as_str) {
        let manifest_list_key =
            snapshot_graph_object_key(context, manifest_list_location, TableMetadataMaintenanceObjectKind::ManifestList)?;
        let references = if let Some(references) = budget.manifest_lists.get(&manifest_list_key) {
            return Ok(Arc::clone(references));
        } else {
            let manifest_list_object = context
                .backend
                .read_object_limited(context.table_bucket, &manifest_list_key, TABLE_MANIFEST_AVRO_MAX_SIZE)
                .await?
                .ok_or_else(|| TableCatalogStoreError::Invalid("snapshot manifest-list object is missing".to_string()))?;
            budget.charge_avro_bytes(manifest_list_object.data.len())?;
            let decoded_manifest_list = decode_manifest_list_avro_async(manifest_list_object.data).await?;
            budget.charge_decoded_avro_bytes(decoded_manifest_list.decoded_size)?;
            decoded_manifest_list.references
        };
        let references = Arc::new(
            references
                .into_iter()
                .map(|reference| SnapshotGraphManifestLocation {
                    manifest_path: reference.manifest_path,
                    format_version: reference.format_version,
                    manifest_length: reference.manifest_length,
                    partition_spec_id: reference.partition_spec_id,
                    content: reference.content,
                    sequence_number: reference.sequence_number,
                    min_sequence_number: reference.min_sequence_number,
                    added_snapshot_id: reference.added_snapshot_id,
                    added_files_count: reference.added_files_count,
                    existing_files_count: reference.existing_files_count,
                    deleted_files_count: reference.deleted_files_count,
                    added_rows_count: reference.added_rows_count,
                    existing_rows_count: reference.existing_rows_count,
                    deleted_rows_count: reference.deleted_rows_count,
                    from_manifest_list: true,
                })
                .collect(),
        );
        budget.manifest_lists.insert(manifest_list_key, Arc::clone(&references));
        return Ok(references);
    }

    let Some(manifests) = snapshot.get("manifests").and_then(serde_json::Value::as_array) else {
        return Err(TableCatalogStoreError::Invalid("snapshot manifest-list is required".to_string()));
    };
    Ok(Arc::new(
        manifests
            .iter()
            .map(|manifest| {
                manifest
                    .as_str()
                    .filter(|manifest| !manifest.is_empty())
                    .map(|manifest| SnapshotGraphManifestLocation {
                        manifest_path: manifest.to_string(),
                        format_version,
                        manifest_length: None,
                        partition_spec_id: None,
                        content: None,
                        sequence_number: Some(snapshot_sequence_number),
                        min_sequence_number: None,
                        added_snapshot_id: None,
                        added_files_count: None,
                        existing_files_count: None,
                        deleted_files_count: None,
                        added_rows_count: None,
                        existing_rows_count: None,
                        deleted_rows_count: None,
                        from_manifest_list: false,
                    })
                    .ok_or_else(|| TableCatalogStoreError::Invalid("snapshot manifest location must be a string".to_string()))
            })
            .collect::<TableCatalogStoreResult<Vec<_>>>()?,
    ))
}

fn validate_snapshot_graph_manifest_location(
    manifest: &SnapshotGraphManifestLocation,
    table_format_version: u16,
    snapshot_sequence_number: i64,
) -> TableCatalogStoreResult<()> {
    if !manifest.from_manifest_list {
        return Ok(());
    }
    if !manifest.manifest_length.is_some_and(|length| length > 0) {
        return Err(TableCatalogStoreError::Invalid(
            "manifest-list entry is missing a positive manifest_length".to_string(),
        ));
    }
    manifest
        .added_snapshot_id
        .ok_or_else(|| TableCatalogStoreError::Invalid("manifest-list entry is missing added_snapshot_id".to_string()))?;

    if manifest.format_version > table_format_version {
        return Err(TableCatalogStoreError::Invalid(
            "manifest-list format version exceeds the table format version".to_string(),
        ));
    }

    match manifest.format_version {
        1 => {
            if manifest.content.is_some_and(|content| content != 0)
                || manifest.sequence_number.is_some_and(|sequence_number| sequence_number != 0)
                || manifest
                    .min_sequence_number
                    .is_some_and(|min_sequence_number| min_sequence_number != 0)
            {
                return Err(TableCatalogStoreError::Invalid(
                    "Iceberg v1 manifest-list compatibility fields must use data content and sequence zero".to_string(),
                ));
            }
        }
        2 => {
            if !matches!(manifest.content, Some(0 | 1)) {
                return Err(TableCatalogStoreError::Invalid(
                    "Iceberg v2 manifest-list content must be data or deletes".to_string(),
                ));
            }
            let sequence_number = manifest.sequence_number.ok_or_else(|| {
                TableCatalogStoreError::Invalid("Iceberg v2 manifest-list entry is missing sequence_number".to_string())
            })?;
            let min_sequence_number = manifest.min_sequence_number.ok_or_else(|| {
                TableCatalogStoreError::Invalid("Iceberg v2 manifest-list entry is missing min_sequence_number".to_string())
            })?;
            if min_sequence_number < 0 || sequence_number < min_sequence_number || sequence_number > snapshot_sequence_number {
                return Err(TableCatalogStoreError::Invalid(
                    "Iceberg v2 manifest-list sequence numbers are inconsistent with the snapshot".to_string(),
                ));
            }
        }
        _ => {
            return Err(TableCatalogStoreError::Internal(format!(
                "decoded Iceberg manifest-list format version {} is unsupported",
                manifest.format_version
            )));
        }
    }
    Ok(())
}

fn validate_snapshot_graph_data_file_reference(
    reference: &ManifestDataFileReference,
    table_format_version: u16,
    manifest_sequence_number: i64,
) -> TableCatalogStoreResult<()> {
    if reference.record_count.is_none() {
        return Err(TableCatalogStoreError::Invalid(
            "manifest data file is missing a non-negative record_count".to_string(),
        ));
    }
    if reference.file_size_bytes.is_none() {
        return Err(TableCatalogStoreError::Invalid(
            "manifest data file is missing a non-negative file_size_in_bytes".to_string(),
        ));
    }

    if reference.format_version > table_format_version {
        return Err(TableCatalogStoreError::Invalid(
            "manifest format version exceeds the table format version".to_string(),
        ));
    }

    match reference.format_version {
        1 => {
            if reference.snapshot_id.is_none() {
                return Err(TableCatalogStoreError::Invalid(
                    "Iceberg v1 manifest entry is missing snapshot_id".to_string(),
                ));
            }
            if reference.content_id.is_some_and(|content| content != 0) {
                return Err(TableCatalogStoreError::Invalid(
                    "Iceberg v1 manifest data file content must be data".to_string(),
                ));
            }
            if reference.sequence_number.is_some_and(|sequence_number| sequence_number != 0)
                || reference
                    .file_sequence_number
                    .is_some_and(|sequence_number| sequence_number != 0)
            {
                return Err(TableCatalogStoreError::Invalid(
                    "Iceberg v1 manifest sequence numbers must be zero when present".to_string(),
                ));
            }
        }
        2 => {
            if reference.content_id.is_none() {
                return Err(TableCatalogStoreError::Invalid(
                    "Iceberg v2 manifest data file is missing content".to_string(),
                ));
            }
            for sequence_number in [reference.sequence_number, reference.file_sequence_number]
                .into_iter()
                .flatten()
            {
                if sequence_number < 0 || sequence_number > manifest_sequence_number {
                    return Err(TableCatalogStoreError::Invalid(
                        "Iceberg v2 manifest entry sequence number exceeds its manifest sequence".to_string(),
                    ));
                }
            }
            if !matches!(reference.entry_status, Some(1))
                && (reference.sequence_number.is_none() || reference.file_sequence_number.is_none())
            {
                return Err(TableCatalogStoreError::Invalid(
                    "Iceberg v2 existing and deleted manifest entries require sequence numbers".to_string(),
                ));
            }
        }
        _ => {
            return Err(TableCatalogStoreError::Internal(format!(
                "decoded Iceberg manifest format version {} is unsupported",
                reference.format_version
            )));
        }
    }
    Ok(())
}

async fn validate_snapshot_graph_data_files<B>(
    context: &TableSnapshotGraphValidationContext<'_, B>,
    references: &[ManifestDataFileReference],
    budget: &mut SnapshotGraphReadBudget,
    format_version: u16,
    manifest_sequence_number: i64,
) -> TableCatalogStoreResult<()>
where
    B: TableCatalogObjectBackend,
{
    let mut live_object_keys = Vec::with_capacity(references.len());
    for reference in references {
        validate_snapshot_graph_data_file_reference(reference, format_version, manifest_sequence_number)?;
        let object_key = snapshot_graph_object_key(context, &reference.location, reference.object_kind.clone())?;
        match reference.entry_status {
            Some(0 | 1) if budget.validated_live_objects.insert(object_key.clone()) => {
                live_object_keys.push(object_key);
            }
            Some(0 | 1) => {}
            Some(2) => {}
            Some(_) => {
                return Err(TableCatalogStoreError::Invalid("manifest entry status is unsupported".to_string()));
            }
            None => {
                return Err(TableCatalogStoreError::Invalid("manifest entry status is required".to_string()));
            }
        }
    }

    validate_object_keys_exist(
        context.backend,
        context.table_bucket,
        live_object_keys,
        "manifest referenced data file is missing",
    )
    .await
}

fn snapshot_graph_object_key<B>(
    context: &TableSnapshotGraphValidationContext<'_, B>,
    location: &str,
    expected_kind: TableMetadataMaintenanceObjectKind,
) -> TableCatalogStoreResult<String> {
    if context.entry.table_bucket != context.table_bucket {
        return Err(TableCatalogStoreError::Invalid("snapshot object is outside the table bucket".to_string()));
    }
    let object_key = table_catalog_object_key_from_location(context.table_bucket, location)
        .ok_or_else(|| TableCatalogStoreError::Invalid("snapshot object location is invalid".to_string()))?;
    let warehouse_object_prefix = table_warehouse_object_prefix(context.entry)?;
    let object_kind = table_maintenance_object_kind_for_entry(context.entry, Some(&warehouse_object_prefix), &object_key)
        .ok_or_else(|| TableCatalogStoreError::Invalid("snapshot object is outside the table warehouse".to_string()))?;
    if !table_maintenance_object_kind_matches_reference(&object_kind, &expected_kind) {
        return Err(TableCatalogStoreError::Invalid(
            "snapshot object kind does not match manifest metadata".to_string(),
        ));
    }
    Ok(object_key)
}
