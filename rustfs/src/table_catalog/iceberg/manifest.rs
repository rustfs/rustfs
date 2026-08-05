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

use super::super::*;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ManifestDataFileReference {
    pub location: String,
    pub format_version: u16,
    pub content_id: Option<i32>,
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
    pub format_version: u16,
    pub manifest_length: Option<u64>,
    pub partition_spec_id: Option<i32>,
    pub content: Option<i32>,
    pub sequence_number: Option<i64>,
    pub min_sequence_number: Option<i64>,
    pub added_snapshot_id: Option<i64>,
    pub added_files_count: Option<u64>,
    pub existing_files_count: Option<u64>,
    pub deleted_files_count: Option<u64>,
    pub added_rows_count: Option<u64>,
    pub existing_rows_count: Option<u64>,
    pub deleted_rows_count: Option<u64>,
}

pub(crate) struct DecodedManifestList {
    pub references: Vec<ManifestListReference>,
    pub decoded_size: usize,
}

pub(crate) struct DecodedManifest {
    pub references: Vec<ManifestDataFileReference>,
    pub decoded_size: usize,
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
    Ok(decode_manifest_list_avro(data)?.references)
}

pub(crate) fn decode_manifest_list_avro(data: &[u8]) -> TableCatalogStoreResult<DecodedManifestList> {
    if data.len() > TABLE_MANIFEST_AVRO_MAX_SIZE {
        return Err(TableCatalogStoreError::Invalid(format!(
            "manifest list exceeds the maximum size of {TABLE_MANIFEST_AVRO_MAX_SIZE} bytes"
        )));
    }
    let decoded_size = validate_avro_container(data)?;
    let reader = apache_avro::Reader::new(data)
        .map_err(|err| TableCatalogStoreError::Invalid(format!("failed to read manifest list Avro: {err}")))?;
    let format_version =
        avro_record_format_version(reader.writer_schema(), &["sequence_number", "min_sequence_number"], "manifest list")?;
    let mut manifest_paths = Vec::new();
    for value in reader {
        if manifest_paths.len() >= TABLE_MANIFEST_AVRO_MAX_RECORDS {
            return Err(TableCatalogStoreError::Invalid(format!(
                "manifest list exceeds the maximum record count of {TABLE_MANIFEST_AVRO_MAX_RECORDS}"
            )));
        }
        let value =
            value.map_err(|err| TableCatalogStoreError::Invalid(format!("failed to read manifest list record: {err}")))?;
        let manifest_path = avro_record_field(&value, "manifest_path")
            .and_then(avro_string_value)
            .ok_or_else(|| TableCatalogStoreError::Invalid("manifest list entry missing manifest_path".to_string()))?;
        manifest_paths.push(ManifestListReference {
            manifest_path: manifest_path.to_string(),
            format_version,
            manifest_length: avro_record_field(&value, "manifest_length")
                .and_then(avro_i64_value)
                .and_then(|value| u64::try_from(value).ok()),
            partition_spec_id: avro_record_field(&value, "partition_spec_id").and_then(avro_i32_value),
            content: avro_record_field(&value, "content").and_then(avro_i32_value),
            sequence_number: avro_record_field(&value, "sequence_number").and_then(avro_i64_value),
            min_sequence_number: avro_record_field(&value, "min_sequence_number").and_then(avro_i64_value),
            added_snapshot_id: avro_record_field(&value, "added_snapshot_id").and_then(avro_i64_value),
            added_files_count: avro_record_field(&value, "added_files_count")
                .and_then(avro_i32_value)
                .and_then(|value| u64::try_from(value).ok()),
            existing_files_count: avro_record_field(&value, "existing_files_count")
                .and_then(avro_i32_value)
                .and_then(|value| u64::try_from(value).ok()),
            deleted_files_count: avro_record_field(&value, "deleted_files_count")
                .and_then(avro_i32_value)
                .and_then(|value| u64::try_from(value).ok()),
            added_rows_count: avro_record_field(&value, "added_rows_count")
                .and_then(avro_i64_value)
                .and_then(|value| u64::try_from(value).ok()),
            existing_rows_count: avro_record_field(&value, "existing_rows_count")
                .and_then(avro_i64_value)
                .and_then(|value| u64::try_from(value).ok()),
            deleted_rows_count: avro_record_field(&value, "deleted_rows_count")
                .and_then(avro_i64_value)
                .and_then(|value| u64::try_from(value).ok()),
        });
    }
    Ok(DecodedManifestList {
        references: manifest_paths,
        decoded_size,
    })
}

pub(crate) async fn decode_manifest_list_avro_async(data: Vec<u8>) -> TableCatalogStoreResult<DecodedManifestList> {
    tokio::task::spawn_blocking(move || decode_manifest_list_avro(&data))
        .await
        .map_err(|err| TableCatalogStoreError::Internal(format!("manifest-list parser task failed: {err}")))?
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
    Ok(decode_manifest_avro(data)?.references)
}

pub(crate) fn decode_manifest_avro(data: &[u8]) -> TableCatalogStoreResult<DecodedManifest> {
    if data.len() > TABLE_MANIFEST_AVRO_MAX_SIZE {
        return Err(TableCatalogStoreError::Invalid(format!(
            "manifest exceeds the maximum size of {TABLE_MANIFEST_AVRO_MAX_SIZE} bytes"
        )));
    }
    let decoded_size = validate_avro_container(data)?;
    let reader = apache_avro::Reader::new(data)
        .map_err(|err| TableCatalogStoreError::Invalid(format!("failed to read manifest Avro: {err}")))?;
    let format_version =
        avro_record_format_version(reader.writer_schema(), &["sequence_number", "file_sequence_number"], "manifest")?;
    let mut files = Vec::new();
    for value in reader {
        if files.len() >= TABLE_MANIFEST_AVRO_MAX_RECORDS {
            return Err(TableCatalogStoreError::Invalid(format!(
                "manifest exceeds the maximum record count of {TABLE_MANIFEST_AVRO_MAX_RECORDS}"
            )));
        }
        let value = value.map_err(|err| TableCatalogStoreError::Invalid(format!("failed to read manifest record: {err}")))?;
        let data_file = avro_record_field(&value, "data_file")
            .ok_or_else(|| TableCatalogStoreError::Invalid("manifest entry missing data_file".to_string()))?;
        let file_path = avro_record_field(data_file, "file_path")
            .and_then(avro_string_value)
            .ok_or_else(|| TableCatalogStoreError::Invalid("manifest data file missing file_path".to_string()))?;
        let content_id = match avro_record_field(data_file, "content") {
            Some(content) => Some(
                avro_i32_value(content)
                    .ok_or_else(|| TableCatalogStoreError::Invalid("manifest data file content must be an int".to_string()))?,
            ),
            None => None,
        };
        let content_value = content_id.unwrap_or(0);
        let (content, object_kind) = match content_value {
            0 => (ManifestDataFileContent::Data, TableMetadataMaintenanceObjectKind::DataFile),
            1 => (ManifestDataFileContent::PositionDelete, TableMetadataMaintenanceObjectKind::DeleteFile),
            2 => (ManifestDataFileContent::EqualityDelete, TableMetadataMaintenanceObjectKind::DeleteFile),
            _ => {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "manifest data file has unsupported content value {content_value}"
                )));
            }
        };
        files.push(ManifestDataFileReference {
            location: file_path.to_string(),
            format_version,
            content_id,
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
    Ok(DecodedManifest {
        references: files,
        decoded_size,
    })
}

pub(crate) async fn decode_manifest_avro_async(data: Vec<u8>) -> TableCatalogStoreResult<DecodedManifest> {
    tokio::task::spawn_blocking(move || decode_manifest_avro(&data))
        .await
        .map_err(|err| TableCatalogStoreError::Internal(format!("manifest parser task failed: {err}")))?
}

#[derive(Clone, Copy)]
enum AvroContainerCodec {
    Null,
    Deflate,
}

fn validate_avro_container(data: &[u8]) -> TableCatalogStoreResult<usize> {
    if !data.starts_with(b"Obj\x01") {
        return Err(TableCatalogStoreError::Invalid("Avro container has invalid header magic".to_string()));
    }
    let mut offset = 4;
    let mut entry_count = 0usize;
    let mut codec = None;
    loop {
        let block_count = read_avro_long(data, &mut offset)?;
        if block_count == 0 {
            break;
        }
        let (block_count, expected_end) = if block_count < 0 {
            let block_count = block_count
                .checked_neg()
                .and_then(|count| usize::try_from(count).ok())
                .ok_or_else(|| TableCatalogStoreError::Invalid("Avro header block count is invalid".to_string()))?;
            let block_size = read_avro_long(data, &mut offset)?;
            let block_size = usize::try_from(block_size)
                .map_err(|_| TableCatalogStoreError::Invalid("Avro header block size is invalid".to_string()))?;
            let expected_end = offset
                .checked_add(block_size)
                .filter(|end| *end <= data.len())
                .ok_or_else(|| TableCatalogStoreError::Invalid("Avro header block exceeds the object size".to_string()))?;
            (block_count, Some(expected_end))
        } else {
            let block_count = usize::try_from(block_count)
                .map_err(|_| TableCatalogStoreError::Invalid("Avro header block count is invalid".to_string()))?;
            (block_count, None)
        };
        entry_count = entry_count
            .checked_add(block_count)
            .filter(|count| *count <= TABLE_MANIFEST_AVRO_MAX_HEADER_ENTRIES)
            .ok_or_else(|| TableCatalogStoreError::Invalid("Avro header has too many metadata entries".to_string()))?;
        for _ in 0..block_count {
            let key = read_avro_bytes(data, &mut offset)?;
            let value = read_avro_bytes(data, &mut offset)?;
            if key == b"avro.codec" {
                codec = Some(value);
            }
        }
        if expected_end.is_some_and(|expected_end| offset != expected_end) {
            return Err(TableCatalogStoreError::Invalid(
                "Avro header block size does not match its contents".to_string(),
            ));
        }
    }
    let marker_end = offset
        .checked_add(16)
        .filter(|end| *end <= data.len())
        .ok_or_else(|| TableCatalogStoreError::Invalid("Avro container is missing its sync marker".to_string()))?;
    let sync_marker: [u8; 16] = data[offset..marker_end]
        .try_into()
        .map_err(|_| TableCatalogStoreError::Invalid("Avro container has an invalid sync marker".to_string()))?;
    offset = marker_end;
    let codec = match codec.unwrap_or(b"null") {
        b"null" => AvroContainerCodec::Null,
        b"deflate" => AvroContainerCodec::Deflate,
        codec => {
            return Err(TableCatalogStoreError::Invalid(format!(
                "Avro codec {} is not supported for table commit validation",
                String::from_utf8_lossy(codec)
            )));
        }
    };

    let mut record_count = 0usize;
    let mut decoded_size = 0usize;
    while offset < data.len() {
        let block_count = read_avro_long(data, &mut offset)?;
        let block_count = usize::try_from(block_count)
            .ok()
            .filter(|count| *count > 0)
            .ok_or_else(|| TableCatalogStoreError::Invalid("Avro data block count is invalid".to_string()))?;
        record_count = record_count
            .checked_add(block_count)
            .filter(|count| *count <= TABLE_MANIFEST_AVRO_MAX_RECORDS)
            .ok_or_else(|| {
                TableCatalogStoreError::Invalid(format!(
                    "Avro container exceeds the maximum record count of {TABLE_MANIFEST_AVRO_MAX_RECORDS}"
                ))
            })?;
        let block_size = read_avro_long(data, &mut offset)?;
        let block_size = usize::try_from(block_size)
            .map_err(|_| TableCatalogStoreError::Invalid("Avro data block size is invalid".to_string()))?;
        let block_end = offset
            .checked_add(block_size)
            .ok_or_else(|| TableCatalogStoreError::Invalid("Avro data block exceeds the object size".to_string()))?;
        let block_marker_end = block_end
            .checked_add(16)
            .filter(|marker_end| *marker_end <= data.len())
            .ok_or_else(|| TableCatalogStoreError::Invalid("Avro data block exceeds the object size".to_string()))?;
        let remaining_decoded_size = TABLE_MANIFEST_AVRO_MAX_DECODED_SIZE
            .checked_sub(decoded_size)
            .ok_or_else(|| TableCatalogStoreError::Invalid("Avro decoded data exceeds the commit limit".to_string()))?;
        let block_decoded_size = avro_block_decoded_size(codec, &data[offset..block_end], remaining_decoded_size)?;
        decoded_size = decoded_size
            .checked_add(block_decoded_size)
            .filter(|size| *size <= TABLE_MANIFEST_AVRO_MAX_DECODED_SIZE)
            .ok_or_else(|| {
                TableCatalogStoreError::Invalid(format!(
                    "Avro decoded data exceeds the maximum size of {TABLE_MANIFEST_AVRO_MAX_DECODED_SIZE} bytes"
                ))
            })?;
        if data[block_end..block_marker_end] != sync_marker {
            return Err(TableCatalogStoreError::Invalid("Avro data block has an invalid sync marker".to_string()));
        }
        offset = block_marker_end;
    }
    Ok(decoded_size)
}

fn avro_block_decoded_size(codec: AvroContainerCodec, block: &[u8], remaining_size: usize) -> TableCatalogStoreResult<usize> {
    match codec {
        AvroContainerCodec::Null => Ok(block.len()),
        AvroContainerCodec::Deflate => {
            let limit = remaining_size
                .checked_add(1)
                .ok_or_else(|| TableCatalogStoreError::Invalid("Avro decoded data size limit overflowed".to_string()))?;
            let limit = u64::try_from(limit)
                .map_err(|_| TableCatalogStoreError::Invalid("Avro decoded data size limit is invalid".to_string()))?;
            let decoder = flate2::read::DeflateDecoder::new(block);
            let decoded_size = std::io::copy(&mut decoder.take(limit), &mut std::io::sink())
                .map_err(|err| TableCatalogStoreError::Invalid(format!("failed to decompress Avro deflate block: {err}")))?;
            usize::try_from(decoded_size)
                .map_err(|_| TableCatalogStoreError::Invalid("Avro decoded data size is invalid".to_string()))
        }
    }
}

fn read_avro_long(data: &[u8], offset: &mut usize) -> TableCatalogStoreResult<i64> {
    let mut raw = 0u64;
    for shift in (0..=63).step_by(7) {
        let byte = *data
            .get(*offset)
            .ok_or_else(|| TableCatalogStoreError::Invalid("Avro header is truncated".to_string()))?;
        *offset = offset
            .checked_add(1)
            .ok_or_else(|| TableCatalogStoreError::Invalid("Avro header offset overflowed".to_string()))?;
        if shift == 63 && byte & 0xfe != 0 {
            return Err(TableCatalogStoreError::Invalid("Avro long is out of range".to_string()));
        }
        raw |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            let magnitude =
                i64::try_from(raw >> 1).map_err(|_| TableCatalogStoreError::Invalid("Avro long is out of range".to_string()))?;
            let sign = if raw & 1 == 0 { 0 } else { -1 };
            return Ok(magnitude ^ sign);
        }
    }
    Err(TableCatalogStoreError::Invalid("Avro long is out of range".to_string()))
}

fn read_avro_bytes<'a>(data: &'a [u8], offset: &mut usize) -> TableCatalogStoreResult<&'a [u8]> {
    let length = read_avro_long(data, offset)?;
    let length = usize::try_from(length)
        .map_err(|_| TableCatalogStoreError::Invalid("Avro header byte string length is invalid".to_string()))?;
    let end = offset
        .checked_add(length)
        .filter(|end| *end <= data.len())
        .ok_or_else(|| TableCatalogStoreError::Invalid("Avro header byte string is truncated".to_string()))?;
    let value = &data[*offset..end];
    *offset = end;
    Ok(value)
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

fn avro_record_format_version(schema: &apache_avro::Schema, v2_fields: &[&str], label: &str) -> TableCatalogStoreResult<u16> {
    let apache_avro::Schema::Record(record) = schema else {
        return Err(TableCatalogStoreError::Invalid(format!("{label} Avro schema must be a record")));
    };
    let present = v2_fields.iter().filter(|field| record.lookup.contains_key(**field)).count();
    match present {
        0 => Ok(1),
        count if count == v2_fields.len() => Ok(2),
        _ => Err(TableCatalogStoreError::Invalid(format!(
            "{label} Avro schema contains an incomplete set of Iceberg v2 fields"
        ))),
    }
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
