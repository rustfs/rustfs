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
    let metadata = decode_table_metadata_json(metadata_location, &metadata_object.data)?;
    let Some(location) = metadata.get("location").and_then(serde_json::Value::as_str) else {
        return Ok(None);
    };
    validate_location(table_bucket, location)?;
    Ok(Some(location.to_string()))
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

pub(crate) fn table_metadata_warehouse_location(
    table_bucket: &str,
    metadata_location: &str,
    metadata_object: &TableCatalogObject,
) -> TableCatalogStoreResult<Option<String>> {
    metadata_warehouse_location(table_bucket, metadata_location, metadata_object, validate_table_warehouse_location)
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
    for table in store.list_all_tables(bucket).await? {
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

pub(crate) fn synchronize_table_metadata_version_fields(metadata: &mut serde_json::Value) -> TableCatalogStoreResult<()> {
    match table_metadata_format_version(metadata)? {
        1 => {
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
                let fields = metadata.get("partition-spec").cloned().ok_or_else(|| {
                    TableCatalogStoreError::Invalid("Iceberg v1 table metadata is missing partition-spec".to_string())
                })?;
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

pub(crate) fn validate_supported_table_metadata(metadata: &serde_json::Value) -> TableCatalogStoreResult<()> {
    validate_supported_table_metadata_fields(metadata)?;
    validate_table_metadata_references(metadata)
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
                    let sequence_number = snapshot
                        .get("sequence-number")
                        .and_then(serde_json::Value::as_i64)
                        .ok_or_else(|| {
                            TableCatalogStoreError::Invalid("Iceberg v2 snapshot sequence-number must be an integer".to_string())
                        })?;
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
        if i32::try_from(schema_id).is_err() {
            return Err(TableCatalogStoreError::Invalid(format!(
                "schema id {schema_id} exceeds the signed 32-bit range"
            )));
        }
        schema_ids.insert(schema_id);
    }
    validate_metadata_id_reference(metadata, "current-schema-id", &schema_ids, "schema")?;
    let spec_ids = metadata_array_i32_ids(metadata, "partition-specs", "spec-id", "partition spec")?;
    validate_metadata_id_reference(metadata, "default-spec-id", &spec_ids, "partition spec")?;
    let sort_order_ids = metadata_array_i32_ids(metadata, "sort-orders", "order-id", "sort order")?;
    validate_metadata_id_reference(metadata, "default-sort-order-id", &sort_order_ids, "sort order")?;
    let snapshot_ids = metadata_array_ids(metadata, "snapshots", "snapshot-id", "snapshot")?;

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
    let manifest_list = snapshot
        .get("manifest-list")
        .and_then(serde_json::Value::as_str)
        .filter(|location| !location.is_empty());
    let manifests = snapshot.get("manifests");
    if manifests.is_some_and(|manifests| !manifests.is_array()) {
        return Err(TableCatalogStoreError::Invalid("snapshot manifests must be an array".to_string()));
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
            let summary = snapshot
                .get("summary")
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

pub(crate) fn validate_view_metadata_references(metadata: &serde_json::Value) -> TableCatalogStoreResult<()> {
    let schema_ids = metadata_array_i32_ids(metadata, "schemas", "schema-id", "schema")?;
    validate_metadata_id_reference(metadata, "current-schema-id", &schema_ids, "schema")?;
    let version_ids = metadata_array_i32_ids(metadata, "versions", "version-id", "view version")?;
    validate_metadata_id_reference(metadata, "current-version-id", &version_ids, "view version")?;
    if let Some(versions) = metadata.get("versions").and_then(serde_json::Value::as_array) {
        for version in versions {
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
    namespace: &'a Namespace,
    table: &'a IdentifierSegment,
    entry: &'a TableEntry,
}

impl<'a, B> TableSnapshotGraphValidationContext<'a, B> {
    pub(crate) fn new(
        backend: &'a B,
        table_bucket: &'a str,
        namespace: &'a Namespace,
        table: &'a IdentifierSegment,
        entry: &'a TableEntry,
    ) -> Self {
        Self {
            backend,
            table_bucket,
            namespace,
            table,
            entry,
        }
    }
}

#[derive(Default)]
struct SnapshotGraphReadBudget {
    manifest_count: usize,
    avro_bytes: usize,
    decoded_avro_bytes: usize,
    file_reference_count: usize,
    manifest_lists: BTreeMap<String, Vec<ManifestListReference>>,
    manifests: BTreeMap<String, Vec<ManifestDataFileReference>>,
    validated_live_objects: BTreeSet<String>,
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
    added_files_count: Option<u64>,
    existing_files_count: Option<u64>,
    deleted_files_count: Option<u64>,
    added_rows_count: Option<u64>,
    existing_rows_count: Option<u64>,
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
    let format_version = table_metadata_format_version(metadata)?;
    let snapshots = snapshots_requiring_graph_validation(current_metadata, metadata)?;
    let mut budget = SnapshotGraphReadBudget::default();
    for snapshot in snapshots {
        snapshot
            .get("snapshot-id")
            .and_then(serde_json::Value::as_i64)
            .ok_or_else(|| TableCatalogStoreError::Invalid("snapshot-id must be an integer".to_string()))?;
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
        for references in manifests {
            for reference in references {
                if !seen_files.insert(reference.location) {
                    return Err(TableCatalogStoreError::Invalid(
                        "snapshot contains a duplicate file reference".to_string(),
                    ));
                }
            }
        }
    }
    Ok(())
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

    let mut active_snapshot_ids = BTreeSet::new();
    if let Some(snapshot_id) = metadata
        .get("current-snapshot-id")
        .and_then(serde_json::Value::as_i64)
        .filter(|snapshot_id| *snapshot_id != -1)
    {
        active_snapshot_ids.insert(snapshot_id);
    }
    if let Some(refs) = metadata.get("refs").and_then(serde_json::Value::as_object) {
        active_snapshot_ids.extend(
            refs.values()
                .filter_map(|reference| reference.get("snapshot-id").and_then(serde_json::Value::as_i64)),
        );
    }
    Ok(snapshots
        .iter()
        .filter(|snapshot| {
            snapshot
                .get("snapshot-id")
                .and_then(serde_json::Value::as_i64)
                .is_some_and(|snapshot_id| active_snapshot_ids.contains(&snapshot_id))
        })
        .collect())
}

async fn snapshot_graph_manifest_references<B>(
    context: &TableSnapshotGraphValidationContext<'_, B>,
    metadata: &serde_json::Value,
    snapshot: &serde_json::Value,
    format_version: u16,
    snapshot_sequence_number: i64,
    budget: &mut SnapshotGraphReadBudget,
) -> TableCatalogStoreResult<Vec<Vec<ManifestDataFileReference>>>
where
    B: TableCatalogObjectBackend,
{
    let manifest_locations =
        snapshot_graph_manifest_locations(context, snapshot, format_version, snapshot_sequence_number, budget).await?;
    let partition_spec_ids = table_metadata_partition_spec_ids(metadata)?;
    let mut manifests = Vec::with_capacity(manifest_locations.len());
    let mut seen_manifest_paths = BTreeSet::new();
    for manifest_location in manifest_locations {
        validate_snapshot_graph_manifest_location(&manifest_location, format_version, snapshot_sequence_number)?;
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
        if !seen_manifest_paths.insert(manifest_location.manifest_path.clone()) {
            return Err(TableCatalogStoreError::Invalid(
                "snapshot contains a duplicate manifest reference".to_string(),
            ));
        }
        let manifest_key = snapshot_graph_object_key(
            context,
            &manifest_location.manifest_path,
            TableMetadataMaintenanceObjectKind::ManifestFile,
        )?;
        let references = if let Some(references) = budget.manifests.get(&manifest_key).cloned() {
            references
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
            budget.charge_file_references(decoded_manifest.references.len())?;
            budget.manifests.insert(manifest_key, decoded_manifest.references.clone());
            decoded_manifest.references
        };
        validate_snapshot_graph_data_files(
            context,
            &references,
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

async fn snapshot_graph_manifest_locations<B>(
    context: &TableSnapshotGraphValidationContext<'_, B>,
    snapshot: &serde_json::Value,
    format_version: u16,
    snapshot_sequence_number: i64,
    budget: &mut SnapshotGraphReadBudget,
) -> TableCatalogStoreResult<Vec<SnapshotGraphManifestLocation>>
where
    B: TableCatalogObjectBackend,
{
    if let Some(manifest_list_location) = snapshot.get("manifest-list").and_then(serde_json::Value::as_str) {
        let manifest_list_key =
            snapshot_graph_object_key(context, manifest_list_location, TableMetadataMaintenanceObjectKind::ManifestList)?;
        let references = if let Some(references) = budget.manifest_lists.get(&manifest_list_key).cloned() {
            references
        } else {
            let manifest_list_object = context
                .backend
                .read_object_limited(context.table_bucket, &manifest_list_key, TABLE_MANIFEST_AVRO_MAX_SIZE)
                .await?
                .ok_or_else(|| TableCatalogStoreError::Invalid("snapshot manifest-list object is missing".to_string()))?;
            budget.charge_avro_bytes(manifest_list_object.data.len())?;
            let decoded_manifest_list = decode_manifest_list_avro_async(manifest_list_object.data).await?;
            budget.charge_decoded_avro_bytes(decoded_manifest_list.decoded_size)?;
            budget
                .manifest_lists
                .insert(manifest_list_key, decoded_manifest_list.references.clone());
            decoded_manifest_list.references
        };
        return Ok(references
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
            .collect());
    }

    let Some(manifests) = snapshot.get("manifests").and_then(serde_json::Value::as_array) else {
        return Err(TableCatalogStoreError::Invalid("snapshot manifest-list is required".to_string()));
    };
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
        .collect()
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
            if [
                manifest.added_files_count,
                manifest.existing_files_count,
                manifest.deleted_files_count,
                manifest.added_rows_count,
                manifest.existing_rows_count,
                manifest.deleted_rows_count,
            ]
            .into_iter()
            .any(|count| count.is_none())
            {
                return Err(TableCatalogStoreError::Invalid(
                    "Iceberg v2 manifest-list entry is missing required file or row counts".to_string(),
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
            Some(0 | 1) if budget.validated_live_objects.insert(object_key.clone()) => live_object_keys.push(object_key),
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

    for object_keys in live_object_keys.chunks(TABLE_COMMIT_OBJECT_VALIDATION_CONCURRENCY) {
        let backend = context.backend.clone();
        let bucket = context.table_bucket.to_string();
        stream::iter(object_keys.iter().cloned())
            .map(move |object_key| {
                let backend = backend.clone();
                let bucket = bucket.clone();
                async move {
                    if !backend.object_exists(&bucket, &object_key).await? {
                        return Err(TableCatalogStoreError::Invalid("manifest referenced data file is missing".to_string()));
                    }
                    Ok(())
                }
            })
            .buffered(TABLE_COMMIT_OBJECT_VALIDATION_CONCURRENCY)
            .try_for_each(|()| async { Ok(()) })
            .await?;
    }
    Ok(())
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
    let object_kind =
        table_maintenance_object_kind(context.namespace, context.table, Some(&warehouse_object_prefix), &object_key)
            .ok_or_else(|| TableCatalogStoreError::Invalid("snapshot object is outside the table warehouse".to_string()))?;
    if !table_maintenance_object_kind_matches_reference(&object_kind, &expected_kind) {
        return Err(TableCatalogStoreError::Invalid(
            "snapshot object kind does not match manifest metadata".to_string(),
        ));
    }
    Ok(object_key)
}
