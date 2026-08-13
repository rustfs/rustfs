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

//! Shared table-catalog test fixtures (backlog#1837).
//!
//! Pure data constructors for Iceberg metadata JSON and avro
//! manifest-list/manifest bytes, shared by the store-level tests
//! (`table_catalog/tests.rs`) and the admin handler tests
//! (`admin/handlers/table_catalog/tests.rs`). The parameterized admin
//! variants are canonical; the store tests wrap them with their historical
//! fixed values (sequence 7 / snapshot 20), which keeps every produced byte
//! identical to the pre-extraction fixtures.

pub(crate) fn table_metadata_json(table_uuid: &str, location: &str) -> serde_json::Value {
    serde_json::json!({
        "format-version": 2,
        "table-uuid": table_uuid,
        "location": location,
        "last-sequence-number": 0,
        "last-updated-ms": 1,
        "last-column-id": 1,
        "schemas": [{
            "type": "struct",
            "schema-id": 0,
            "fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]
        }],
        "current-schema-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "default-spec-id": 0,
        "last-partition-id": 999,
        "sort-orders": [{"order-id": 0, "fields": []}],
        "default-sort-order-id": 0,
        "properties": {},
        "snapshots": [],
        "snapshot-log": [],
        "metadata-log": [],
        "refs": {}
    })
}

pub(crate) fn manifest_list_avro_bytes(manifest_paths: &[&str], sequence_number: i64, snapshot_id: i64) -> Vec<u8> {
    let manifests = manifest_paths
        .iter()
        .map(|manifest_path| (*manifest_path, 0, sequence_number, snapshot_id))
        .collect::<Vec<_>>();
    manifest_list_avro_entries_with_partition_specs(&manifests)
}

pub(crate) fn manifest_list_avro_entries(manifests: &[(&str, i64, i64)]) -> Vec<u8> {
    let manifests = manifests
        .iter()
        .map(|(manifest_path, sequence_number, snapshot_id)| (*manifest_path, 0, *sequence_number, *snapshot_id))
        .collect::<Vec<_>>();
    manifest_list_avro_entries_with_partition_specs(&manifests)
}

pub(crate) fn manifest_list_avro_entries_with_partition_specs(manifests: &[(&str, i32, i64, i64)]) -> Vec<u8> {
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
                {"name": "deleted_rows_count", "type": "long"}
              ]
            }
            "#,
    )
    .expect("manifest list avro schema should parse");
    let mut writer = apache_avro::Writer::new(&schema, Vec::new()).expect("manifest list writer should initialize");
    for (manifest_path, partition_spec_id, sequence_number, snapshot_id) in manifests {
        writer
            .append_value(apache_avro::types::Value::Record(vec![
                (
                    "manifest_path".to_string(),
                    apache_avro::types::Value::String((*manifest_path).to_string()),
                ),
                ("manifest_length".to_string(), apache_avro::types::Value::Long(1)),
                ("partition_spec_id".to_string(), apache_avro::types::Value::Int(*partition_spec_id)),
                ("content".to_string(), apache_avro::types::Value::Int(0)),
                ("sequence_number".to_string(), apache_avro::types::Value::Long(*sequence_number)),
                ("min_sequence_number".to_string(), apache_avro::types::Value::Long(*sequence_number)),
                ("added_snapshot_id".to_string(), apache_avro::types::Value::Long(*snapshot_id)),
                ("added_files_count".to_string(), apache_avro::types::Value::Int(1)),
                ("existing_files_count".to_string(), apache_avro::types::Value::Int(0)),
                ("deleted_files_count".to_string(), apache_avro::types::Value::Int(0)),
                ("added_rows_count".to_string(), apache_avro::types::Value::Long(1)),
                ("existing_rows_count".to_string(), apache_avro::types::Value::Long(0)),
                ("deleted_rows_count".to_string(), apache_avro::types::Value::Long(0)),
            ]))
            .expect("manifest list record should append");
    }
    writer.into_inner().expect("manifest list avro bytes should flush")
}

pub(crate) fn manifest_avro_bytes(files: &[(&str, i32, i32, i64, i64)]) -> Vec<u8> {
    let schema = apache_avro::Schema::parse_str(
        r#"
            {
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
                      {"name": "record_count", "type": "long"},
                      {"name": "file_size_in_bytes", "type": "long"}
                    ]
                  }
                }
              ]
            }
            "#,
    )
    .expect("manifest avro schema should parse");
    let mut writer = apache_avro::Writer::new(&schema, Vec::new()).expect("manifest writer should initialize");
    for (file_path, content, status, snapshot_id, sequence_number) in files {
        writer
            .append_value(apache_avro::types::Value::Record(vec![
                ("status".to_string(), apache_avro::types::Value::Int(*status)),
                ("snapshot_id".to_string(), apache_avro::types::Value::Long(*snapshot_id)),
                ("sequence_number".to_string(), apache_avro::types::Value::Long(*sequence_number)),
                ("file_sequence_number".to_string(), apache_avro::types::Value::Long(*sequence_number)),
                (
                    "data_file".to_string(),
                    apache_avro::types::Value::Record(vec![
                        ("content".to_string(), apache_avro::types::Value::Int(*content)),
                        ("file_path".to_string(), apache_avro::types::Value::String((*file_path).to_string())),
                        ("record_count".to_string(), apache_avro::types::Value::Long(1)),
                        ("file_size_in_bytes".to_string(), apache_avro::types::Value::Long(1)),
                    ]),
                ),
            ]))
            .expect("manifest record should append");
    }
    writer.into_inner().expect("manifest avro bytes should flush")
}

pub(crate) fn nullable_long(value: Option<i64>) -> apache_avro::types::Value {
    match value {
        Some(value) => apache_avro::types::Value::Union(1, Box::new(apache_avro::types::Value::Long(value))),
        None => apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null)),
    }
}

pub(crate) fn manifest_avro_bytes_with_nullable_sequences(files: &[(&str, i32, i32, i64, Option<i64>)]) -> Vec<u8> {
    let schema = apache_avro::Schema::parse_str(
        r#"
            {
              "type": "record",
              "name": "manifest_entry",
              "fields": [
                {"name": "status", "type": "int"},
                {"name": "snapshot_id", "type": "long"},
                {"name": "sequence_number", "type": ["null", "long"], "default": null},
                {"name": "file_sequence_number", "type": ["null", "long"], "default": null},
                {
                  "name": "data_file",
                  "type": {
                    "type": "record",
                    "name": "data_file",
                    "fields": [
                      {"name": "content", "type": "int"},
                      {"name": "file_path", "type": "string"},
                      {"name": "record_count", "type": "long"},
                      {"name": "file_size_in_bytes", "type": "long"}
                    ]
                  }
                }
              ]
            }
            "#,
    )
    .expect("manifest avro schema should parse");
    let mut writer = apache_avro::Writer::new(&schema, Vec::new()).expect("manifest writer should initialize");
    for (file_path, content, status, snapshot_id, sequence_number) in files {
        writer
            .append_value(apache_avro::types::Value::Record(vec![
                ("status".to_string(), apache_avro::types::Value::Int(*status)),
                ("snapshot_id".to_string(), apache_avro::types::Value::Long(*snapshot_id)),
                ("sequence_number".to_string(), nullable_long(*sequence_number)),
                ("file_sequence_number".to_string(), nullable_long(*sequence_number)),
                (
                    "data_file".to_string(),
                    apache_avro::types::Value::Record(vec![
                        ("content".to_string(), apache_avro::types::Value::Int(*content)),
                        ("file_path".to_string(), apache_avro::types::Value::String((*file_path).to_string())),
                        ("record_count".to_string(), apache_avro::types::Value::Long(1)),
                        ("file_size_in_bytes".to_string(), apache_avro::types::Value::Long(1)),
                    ]),
                ),
            ]))
            .expect("manifest record should append");
    }
    writer.into_inner().expect("manifest avro bytes should flush")
}
