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

pub(crate) fn manifest_paths_from_manifest_list_avro(data: &[u8]) -> TableCatalogStoreResult<Vec<String>> {
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

pub(crate) fn file_references_from_manifest_avro(
    data: &[u8],
) -> TableCatalogStoreResult<Vec<(String, TableMetadataMaintenanceObjectKind)>> {
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

pub(crate) fn avro_non_union_value(value: &apache_avro::types::Value) -> &apache_avro::types::Value {
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
