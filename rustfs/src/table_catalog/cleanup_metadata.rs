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

use super::*;

/// Validates one metadata object; callers must also validate the complete object inventory.
pub(crate) fn validate_empty_table_cleanup_metadata(
    table_bucket: &str,
    object_key: &str,
    metadata: &[u8],
) -> TableCatalogStoreResult<()> {
    let (namespace, table) = table_identity_from_metadata_object_key(object_key).ok_or_else(|| {
        TableCatalogStoreError::Invalid("cleanup metadata must use the default table metadata directory".to_string())
    })?;
    let metadata = decode_table_metadata_json(object_key, metadata)?;
    validate_supported_table_metadata(&metadata)?;

    // Future metadata fields may introduce additional object references.
    for field in metadata.as_object().expect("validated metadata is a JSON object").keys() {
        if !matches!(
            field.as_str(),
            "format-version"
                | "table-uuid"
                | "location"
                | "last-sequence-number"
                | "last-updated-ms"
                | "last-column-id"
                | "schema"
                | "schemas"
                | "current-schema-id"
                | "partition-spec"
                | "partition-specs"
                | "default-spec-id"
                | "last-partition-id"
                | "properties"
                | "current-snapshot-id"
                | "snapshots"
                | "snapshot-log"
                | "metadata-log"
                | "sort-orders"
                | "default-sort-order-id"
                | "refs"
                | "statistics"
                | "partition-statistics"
        ) {
            return Err(TableCatalogStoreError::Unsupported(format!(
                "empty table cleanup does not support metadata field {field}"
            )));
        }
    }

    for field in ["snapshots", "snapshot-log", "statistics", "partition-statistics"] {
        let Some(value) = metadata.get(field) else {
            continue;
        };
        let entries = value
            .as_array()
            .ok_or_else(|| TableCatalogStoreError::Invalid(format!("cleanup metadata field {field} must be an array")))?;
        if !entries.is_empty() {
            return Err(TableCatalogStoreError::Conflict(format!(
                "empty table cleanup requires metadata field {field} to be empty"
            )));
        }
    }
    if let Some(value) = metadata.get("refs") {
        let refs = value
            .as_object()
            .ok_or_else(|| TableCatalogStoreError::Invalid("cleanup metadata refs must be an object".to_string()))?;
        if !refs.is_empty() {
            return Err(TableCatalogStoreError::Conflict(
                "empty table cleanup requires metadata refs to be empty".to_string(),
            ));
        }
    }
    let table_uuid = table_metadata_uuid(&metadata)?;
    if Uuid::parse_str(table_uuid).ok().filter(|uuid| !uuid.is_nil()).is_none() {
        return Err(TableCatalogStoreError::Invalid(
            "cleanup metadata table-uuid must be a non-nil UUID".to_string(),
        ));
    }
    let warehouse_prefix = table_warehouse_object_prefix_from_location(table_bucket, table_metadata_location(&metadata)?)?;
    if warehouse_prefix
        .strip_prefix("tables/")
        .and_then(|prefix| prefix.strip_suffix('/'))
        .and_then(|table_id| Uuid::parse_str(table_id).ok())
        .filter(|table_id| !table_id.is_nil())
        .is_none()
    {
        return Err(TableCatalogStoreError::Conflict(
            "empty table cleanup requires the default tables/<table-id> warehouse location".to_string(),
        ));
    }
    if let Some(entries) = metadata.get("metadata-log").and_then(serde_json::Value::as_array) {
        for entry in entries {
            let entry = entry.as_object().expect("validated metadata log entries are JSON objects");
            if entry
                .keys()
                .any(|field| !matches!(field.as_str(), "timestamp-ms" | "metadata-file"))
            {
                return Err(TableCatalogStoreError::Unsupported(
                    "empty table cleanup does not support unknown metadata-log fields".to_string(),
                ));
            }
            let location = entry["metadata-file"]
                .as_str()
                .expect("validated metadata log entries contain metadata-file");
            table_catalog_object_key_from_location(table_bucket, location)
                .filter(|location| is_valid_table_metadata_location(&namespace, &table, location))
                .filter(|location| location != object_key)
                .ok_or_else(|| {
                    TableCatalogStoreError::Invalid(
                        "cleanup metadata-log must reference another file in the same table metadata directory".to_string(),
                    )
                })?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table_catalog::test_support::table_metadata_json;
    use serde_json::{Value, json};
    use std::io::Write;

    const BUCKET: &str = "warehouse";
    const TABLE_UUID: &str = "f6dd46c2-4971-4d41-b188-452d5f5c2c01";
    const WAREHOUSE_LOCATION: &str = "s3://warehouse/tables/a9d8f3b1-abde-4a0b-b6fb-e36a30abff16";
    const OBJECT: &str =
        ".rustfs-table/warehouses/default/namespaces/analytics/events/tables/orders/metadata/00002.metadata.json";
    const PREVIOUS: &str =
        ".rustfs-table/warehouses/default/namespaces/analytics/events/tables/orders/metadata/00001.metadata.json";

    fn empty_metadata() -> Value {
        table_metadata_json(TABLE_UUID, WAREHOUSE_LOCATION)
    }

    fn validate(metadata: &Value) -> TableCatalogStoreResult<()> {
        validate_empty_table_cleanup_metadata(BUCKET, OBJECT, &serde_json::to_vec(metadata).unwrap())
    }

    #[test]
    fn empty_table_cleanup_accepts_v2_and_same_directory_history() {
        let mut metadata = empty_metadata();
        metadata["metadata-log"] = json!([
            {"timestamp-ms": 1, "metadata-file": PREVIOUS},
            {"timestamp-ms": 1, "metadata-file": format!("s3://{BUCKET}/{PREVIOUS}")}
        ]);
        assert!(validate(&metadata).is_ok());
    }

    #[test]
    fn empty_table_cleanup_accepts_v1_without_snapshots() {
        let mut metadata = json!({
            "format-version": 1,
            "table-uuid": TABLE_UUID,
            "location": WAREHOUSE_LOCATION,
            "last-updated-ms": 1,
            "last-column-id": 0,
            "schema": {"type": "struct", "fields": []},
            "partition-spec": [],
            "current-snapshot-id": -1
        });
        assert!(validate(&metadata).is_ok());
        metadata["snapshots"] = json!([{
            "snapshot-id": 1,
            "timestamp-ms": 1,
            "manifests": ["s3://warehouse/tables/table-id/metadata/manifest.avro"]
        }]);
        validate_supported_table_metadata(&metadata).unwrap();
        assert!(matches!(validate(&metadata), Err(TableCatalogStoreError::Conflict(_))));
    }

    #[test]
    fn empty_table_cleanup_rejects_unsupported_versions_and_unknown_references() {
        for version in [0, 3, 4] {
            let mut metadata = empty_metadata();
            metadata["format-version"] = json!(version);
            assert!(validate(&metadata).is_err(), "version {version}");
        }
        for field in [
            "manifest-list",
            "manifests",
            "data-files",
            "delete-files",
            "next-row-id",
            "future-field",
        ] {
            let mut metadata = empty_metadata();
            metadata[field] = json!([]);
            assert!(validate(&metadata).is_err(), "field {field}");
        }
    }

    #[test]
    fn empty_table_cleanup_rejects_snapshot_and_history_references() {
        let mut metadata = empty_metadata();
        metadata["snapshots"] = json!([{
            "snapshot-id": 1,
            "sequence-number": 0,
            "timestamp-ms": 1,
            "manifest-list": "s3://warehouse/tables/table-id/metadata/list.avro",
            "summary": {"operation": "append"}
        }]);
        validate_supported_table_metadata(&metadata).unwrap();
        assert!(matches!(validate(&metadata), Err(TableCatalogStoreError::Conflict(_))));

        let mut metadata = empty_metadata();
        metadata["snapshot-log"] = json!([{"timestamp-ms": 1, "snapshot-id": 1}]);
        assert!(validate(&metadata).is_err());
        metadata = empty_metadata();
        metadata["refs"] = json!({"main": {"snapshot-id": 1, "type": "branch"}});
        assert!(validate(&metadata).is_err());
        metadata = empty_metadata();
        metadata["current-snapshot-id"] = json!(1);
        assert!(validate(&metadata).is_err());
    }

    #[test]
    fn empty_table_cleanup_accepts_only_absent_null_or_minus_one_current_snapshot() {
        for version in [1, 2] {
            let mut metadata = empty_metadata();
            metadata["format-version"] = json!(version);
            if version == 1 {
                metadata["schema"] = metadata["schemas"][0].clone();
                metadata["partition-spec"] = json!([]);
            }
            assert!(validate(&metadata).is_ok());
            for value in [Value::Null, json!(-1)] {
                metadata["current-snapshot-id"] = value;
                assert!(validate(&metadata).is_ok(), "version {version}");
            }
            for value in [json!(1), json!(0), json!(-2), json!("1"), json!(false), json!([])] {
                metadata["current-snapshot-id"] = value;
                assert!(validate(&metadata).is_err(), "version {version}");
            }
        }
    }

    #[test]
    fn empty_table_cleanup_rejects_statistics_references_and_malformed_empty_fields() {
        for field in ["snapshots", "snapshot-log", "statistics", "partition-statistics"] {
            for value in [Value::Null, json!({}), json!("empty"), json!([{}])] {
                let mut metadata = empty_metadata();
                metadata[field] = value;
                assert!(validate(&metadata).is_err(), "field {field}");
            }
        }
        for value in [Value::Null, json!([]), json!("empty")] {
            let mut metadata = empty_metadata();
            metadata["refs"] = value;
            assert!(validate(&metadata).is_err());
        }
    }

    #[test]
    fn empty_table_cleanup_rejects_unknown_or_cross_table_history() {
        for location in [
            format!("s3://other/{PREVIOUS}"),
            PREVIOUS.replace("/orders/", "/other/"),
            PREVIOUS.replace("/analytics/events/", "/another/"),
            PREVIOUS.replace("00001.metadata.json", "../00001.metadata.json"),
            PREVIOUS.replace("00001.metadata.json", "%2e%2e.json"),
            format!("{PREVIOUS}?versionId=previous"),
            format!("/{PREVIOUS}"),
            OBJECT.to_string(),
        ] {
            let mut metadata = empty_metadata();
            metadata["metadata-log"] = json!([{"timestamp-ms": 1, "metadata-file": location}]);
            assert!(validate(&metadata).is_err(), "location {location}");
        }
        let mut metadata = empty_metadata();
        metadata["metadata-log"] = json!([{"timestamp-ms": 1, "metadata-file": PREVIOUS, "future-file": "data.parquet"}]);
        assert!(validate(&metadata).is_err());
    }

    #[test]
    fn empty_table_cleanup_requires_valid_schema_uuid_and_warehouse() {
        for uuid in ["table-uuid", "00000000-0000-0000-0000-000000000000", ""] {
            let mut metadata = empty_metadata();
            metadata["table-uuid"] = json!(uuid);
            assert!(validate(&metadata).is_err());
        }
        for location in [
            "s3://other/tables/table-id",
            "s3://warehouse/",
            "s3://warehouse/.rustfs-table/metadata",
            "file:///table",
            "s3://warehouse/custom/table-id",
            "s3://warehouse/tables/table-id",
            "s3://warehouse/tables/00000000-0000-0000-0000-000000000000",
            "s3://warehouse/tables/a9d8f3b1-abde-4a0b-b6fb-e36a30abff16/nested",
        ] {
            let mut metadata = empty_metadata();
            metadata["location"] = json!(location);
            assert!(validate(&metadata).is_err());
        }
        let mut metadata = empty_metadata();
        metadata["schemas"] = json!([]);
        assert!(validate(&metadata).is_err());
    }

    #[test]
    fn empty_table_cleanup_rejects_non_default_metadata_paths() {
        let bytes = serde_json::to_vec(&empty_metadata()).unwrap();
        for object in [
            OBJECT.replace("/default/", "/custom/"),
            OBJECT.replace("/tables/", "/views/"),
            OBJECT.replace("/metadata/", "/data/"),
            OBJECT.replace("00002.metadata.json", "part.parquet"),
            OBJECT.replace("00002.metadata.json", "nested/00002.metadata.json"),
            format!("s3://{BUCKET}/{OBJECT}"),
        ] {
            assert!(validate_empty_table_cleanup_metadata(BUCKET, &object, &bytes).is_err(), "object {object}");
        }
    }

    #[test]
    fn empty_table_cleanup_decodes_gzip_and_rejects_invalid_json() {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&serde_json::to_vec(&empty_metadata()).unwrap()).unwrap();
        let bytes = encoder.finish().unwrap();
        let object = OBJECT.replace(".metadata.json", ".gz.metadata.json");
        assert!(validate_empty_table_cleanup_metadata(BUCKET, &object, &bytes).is_ok());
        assert!(validate_empty_table_cleanup_metadata(BUCKET, OBJECT, b"{}").is_err());
        assert!(validate_empty_table_cleanup_metadata(BUCKET, OBJECT, b"[").is_err());
        assert!(validate_empty_table_cleanup_metadata(BUCKET, &object, b"invalid gzip").is_err());
    }
}
