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
use datafusion::arrow::array::{
    Array, BinaryArray, BinaryViewArray, FixedSizeListArray, LargeBinaryArray, LargeListArray, LargeListViewArray,
    LargeStringArray, ListArray, ListViewArray, MapArray, StringArray, StringViewArray, StructArray, downcast_dictionary_array,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::parquet::arrow::{
    add_encoded_arrow_schema_to_metadata, arrow_reader::ArrowReaderOptions, arrow_writer::ArrowWriterOptions,
};
use datafusion::parquet::file::FOOTER_SIZE;
use datafusion::parquet::file::metadata::{ColumnChunkMetaData, FooterTail, RowGroupMetaData};
use datafusion::parquet::file::properties::{EnabledStatistics, WriterProperties};
use std::io::Write;
use std::sync::OnceLock;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const COMPACTION_WRITE_BYTES: usize = 8 * 1024 * 1024;
const COMPACTION_BATCH_ROWS: usize = 256;
const COMPACTION_ROW_GROUP_ROWS: usize = 65_536;
const COMPACTION_MAX_ROW_GROUPS: usize = 256;
const COMPACTION_MAX_COLUMN_CHUNKS: usize = 16_384;
const COMPACTION_SCHEMA_DEPTH: usize = 64;
const COMPACTION_SCHEMA_FIELDS: usize = 1024;

pub(crate) fn try_admit_compaction() -> TableCatalogStoreResult<Arc<OwnedSemaphorePermit>> {
    static ADMISSION: OnceLock<Arc<Semaphore>> = OnceLock::new();
    ADMISSION
        .get_or_init(|| Arc::new(Semaphore::new(1)))
        .clone()
        .try_acquire_owned()
        .map(Arc::new)
        .map_err(|_| TableCatalogStoreError::Unavailable("table compaction capacity is full".to_owned()))
}

fn budget_error() -> TableCatalogStoreError {
    TableCatalogStoreError::Invalid("table compaction exceeds the server memory or expanded-value budget".to_owned())
}

/// Stored-input ceiling failure for one rewrite group, naming the limit the
/// operator must stay under with `max-rewrite-bytes-per-job`.
pub(crate) fn compaction_stored_input_ceiling_error(group_id: &str) -> TableCatalogStoreError {
    TableCatalogStoreError::Invalid(format!(
        "compaction rewrite group {group_id} stored inputs exceed the {TABLE_COMPACTION_MAX_STORED_BYTES} byte server ceiling; lower max-rewrite-bytes-per-job to at most that value and replan"
    ))
}

/// The same ceiling, reached while reading a group's inputs (defence in depth
/// behind the planner cap and the commit pre-check).
fn compaction_stored_inputs_ceiling_error() -> TableCatalogStoreError {
    TableCatalogStoreError::Invalid(format!(
        "compaction stored inputs exceed the {TABLE_COMPACTION_MAX_STORED_BYTES} byte server ceiling; lower max-rewrite-bytes-per-job to at most that value and replan"
    ))
}

fn add_budget(total: &mut usize, amount: usize, limit: usize) -> TableCatalogStoreResult<()> {
    *total = total
        .checked_add(amount)
        .filter(|value| *value <= limit)
        .ok_or_else(budget_error)?;
    Ok(())
}

/// Sizes read from one Parquet page header (Thrift compact encoding).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct PageHeaderSizes {
    uncompressed: usize,
    compressed: usize,
}

const THRIFT_COMPACT_STOP: u8 = 0;
const THRIFT_COMPACT_BOOL_TRUE: u8 = 1;
const THRIFT_COMPACT_BOOL_FALSE: u8 = 2;
const THRIFT_COMPACT_BYTE: u8 = 3;
const THRIFT_COMPACT_I16: u8 = 4;
const THRIFT_COMPACT_I32: u8 = 5;
const THRIFT_COMPACT_I64: u8 = 6;
const THRIFT_COMPACT_DOUBLE: u8 = 7;
const THRIFT_COMPACT_BINARY: u8 = 8;
const THRIFT_COMPACT_LIST: u8 = 9;
const THRIFT_COMPACT_SET: u8 = 10;
const THRIFT_COMPACT_MAP: u8 = 11;
const THRIFT_COMPACT_STRUCT: u8 = 12;
const THRIFT_MAX_NESTING: usize = 16;
const PAGE_HEADER_FIELD_UNCOMPRESSED_SIZE: i16 = 2;
const PAGE_HEADER_FIELD_COMPRESSED_SIZE: i16 = 3;
const FILE_METADATA_FIELD_ROW_GROUPS: i16 = 4;

/// Minimal Thrift compact-protocol cursor: enough to read a `PageHeader`'s
/// size fields and skip everything else so the next header can be located.
struct ThriftCompactCursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl ThriftCompactCursor<'_> {
    fn error() -> TableCatalogStoreError {
        TableCatalogStoreError::Invalid("compaction parquet thrift metadata is malformed".to_owned())
    }

    fn byte(&mut self) -> TableCatalogStoreResult<u8> {
        let byte = *self.bytes.get(self.pos).ok_or_else(Self::error)?;
        self.pos += 1;
        Ok(byte)
    }

    fn skip_bytes(&mut self, count: usize) -> TableCatalogStoreResult<()> {
        self.pos = self
            .pos
            .checked_add(count)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(Self::error)?;
        Ok(())
    }

    fn varint(&mut self) -> TableCatalogStoreResult<u64> {
        let mut value = 0_u64;
        for shift in (0..64).step_by(7) {
            let byte = self.byte()?;
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(Self::error())
    }

    fn zigzag(&mut self) -> TableCatalogStoreResult<i64> {
        let raw = self.varint()?;
        Ok(((raw >> 1) as i64) ^ -((raw & 1) as i64))
    }

    /// Every collection element occupies at least one byte, so a declared
    /// count beyond the remaining bytes is malformed. This bounds the walk to
    /// the chunk's size instead of the count a hostile header declares.
    fn collection_count(&self, count: u64) -> TableCatalogStoreResult<usize> {
        usize::try_from(count)
            .ok()
            .filter(|count| *count <= self.bytes.len() - self.pos)
            .ok_or_else(Self::error)
    }

    fn list_header(&mut self) -> TableCatalogStoreResult<(u8, usize)> {
        let header = self.byte()?;
        let count = match header >> 4 {
            15 => self.varint()?,
            short => u64::from(short),
        };
        Ok((header & 0x0f, self.collection_count(count)?))
    }

    fn field_id(&mut self, last: i16, header: u8) -> TableCatalogStoreResult<i16> {
        match header >> 4 {
            0 => i16::try_from(self.zigzag()?).map_err(|_| Self::error()),
            delta => last.checked_add(i16::from(delta)).ok_or_else(Self::error),
        }
    }

    /// parquet reads the page size fields by id as a zigzag varint whatever
    /// the wire kind says. Follow the id; a kind that is not an integer is
    /// rejected rather than skipped, so the walker never records a smaller
    /// size than the decoder will allocate.
    fn size_field(&mut self, kind: u8) -> TableCatalogStoreResult<usize> {
        match kind {
            THRIFT_COMPACT_I16 | THRIFT_COMPACT_I32 | THRIFT_COMPACT_I64 => {
                usize::try_from(self.zigzag()?).map_err(|_| Self::error())
            }
            _ => Err(Self::error()),
        }
    }

    fn skip_value(&mut self, kind: u8, depth: usize) -> TableCatalogStoreResult<()> {
        if depth > THRIFT_MAX_NESTING {
            return Err(Self::error());
        }
        match kind {
            // A struct field's bool value lives in its header byte.
            THRIFT_COMPACT_BOOL_TRUE | THRIFT_COMPACT_BOOL_FALSE => Ok(()),
            THRIFT_COMPACT_BYTE => self.skip_bytes(1),
            THRIFT_COMPACT_I16 | THRIFT_COMPACT_I32 | THRIFT_COMPACT_I64 => self.varint().map(drop),
            THRIFT_COMPACT_DOUBLE => self.skip_bytes(8),
            THRIFT_COMPACT_BINARY => {
                let len = usize::try_from(self.varint()?).map_err(|_| Self::error())?;
                self.skip_bytes(len)
            }
            THRIFT_COMPACT_LIST | THRIFT_COMPACT_SET => {
                let (element, count) = self.list_header()?;
                for _ in 0..count {
                    self.skip_element(element, depth + 1)?;
                }
                Ok(())
            }
            THRIFT_COMPACT_MAP => {
                let count = self.varint()?;
                let count = self.collection_count(count)?;
                if count == 0 {
                    return Ok(());
                }
                let kinds = self.byte()?;
                for _ in 0..count {
                    self.skip_element(kinds >> 4, depth + 1)?;
                    self.skip_element(kinds & 0x0f, depth + 1)?;
                }
                Ok(())
            }
            THRIFT_COMPACT_STRUCT => self.skip_struct(depth + 1),
            _ => Err(Self::error()),
        }
    }

    /// A collection element carries its own bytes, but parquet skips a bool
    /// element without consuming any. Either width would let the walker and
    /// the decoder disagree on where the next header starts; no Parquet
    /// struct holds a bool collection, so fail closed instead.
    fn skip_element(&mut self, kind: u8, depth: usize) -> TableCatalogStoreResult<()> {
        match kind {
            THRIFT_COMPACT_BOOL_TRUE | THRIFT_COMPACT_BOOL_FALSE => Err(Self::error()),
            kind => self.skip_value(kind, depth),
        }
    }

    fn skip_struct(&mut self, depth: usize) -> TableCatalogStoreResult<()> {
        let mut field_id = 0_i16;
        loop {
            let header = self.byte()?;
            if header == THRIFT_COMPACT_STOP {
                return Ok(());
            }
            field_id = self.field_id(field_id, header)?;
            self.skip_value(header & 0x0f, depth)?;
        }
    }

    /// Read a `PageHeader` struct, returning its sizes and leaving the cursor at
    /// the first page byte.
    fn page_header(&mut self) -> TableCatalogStoreResult<PageHeaderSizes> {
        let mut sizes = PageHeaderSizes::default();
        let mut field_id = 0_i16;
        loop {
            let header = self.byte()?;
            if header == THRIFT_COMPACT_STOP {
                return Ok(sizes);
            }
            let kind = header & 0x0f;
            field_id = self.field_id(field_id, header)?;
            match field_id {
                PAGE_HEADER_FIELD_UNCOMPRESSED_SIZE => sizes.uncompressed = self.size_field(kind)?,
                PAGE_HEADER_FIELD_COMPRESSED_SIZE => sizes.compressed = self.size_field(kind)?,
                _ => self.skip_value(kind, 1)?,
            }
        }
    }

    /// Walk the footer's `FileMetaData`. The pinned parquet reserves the
    /// declared `row_groups` capacity before reading one element; charge that
    /// reservation to the budget so a small footer cannot request gigabytes.
    fn file_metadata(&mut self, limit: usize) -> TableCatalogStoreResult<()> {
        let mut field_id = 0_i16;
        let mut reserved = 0_usize;
        loop {
            let header = self.byte()?;
            if header == THRIFT_COMPACT_STOP {
                return Ok(());
            }
            let kind = header & 0x0f;
            field_id = self.field_id(field_id, header)?;
            if field_id == FILE_METADATA_FIELD_ROW_GROUPS && kind == THRIFT_COMPACT_LIST {
                let (element, count) = self.list_header()?;
                if element != THRIFT_COMPACT_STRUCT {
                    return Err(Self::error());
                }
                let reservation = count.checked_mul(size_of::<RowGroupMetaData>()).ok_or_else(Self::error)?;
                add_budget(&mut reserved, reservation, limit)?;
                for _ in 0..count {
                    self.skip_struct(2)?;
                }
            } else {
                self.skip_value(kind, 0)?;
            }
        }
    }
}

/// Reject footer collection counts the decoder would allocate for before it
/// reads a single element. Bytes after the struct are ignored, as parquet does.
fn validate_footer_metadata(data: &Bytes, limit: usize) -> TableCatalogStoreResult<()> {
    let error = || TableCatalogStoreError::Invalid("compaction parquet footer is malformed".to_owned());
    let tail_start = data.len().checked_sub(FOOTER_SIZE).ok_or_else(error)?;
    let tail: &[u8; FOOTER_SIZE] = data[tail_start..].try_into().map_err(|_| error())?;
    let tail = FooterTail::try_new(tail).map_err(|_| error())?;
    let metadata_start = tail_start.checked_sub(tail.metadata_length()).ok_or_else(error)?;
    let mut cursor = ThriftCompactCursor {
        bytes: &data[metadata_start..tail_start],
        pos: 0,
    };
    cursor.file_metadata(limit)
}

/// Sum the page headers of one column chunk (`[start, end)` in the file) and
/// hold every page and the chunk total to `limit`. Pages are never decompressed.
fn validate_page_headers_in_range(chunk: &[u8], chunk_declared: usize, limit: usize) -> TableCatalogStoreResult<usize> {
    let mut cursor = ThriftCompactCursor { bytes: chunk, pos: 0 };
    let mut total = 0_usize;
    while cursor.pos < chunk.len() {
        let sizes = cursor.page_header()?;
        add_budget(&mut total, sizes.uncompressed, limit)?;
        if total > chunk_declared.max(limit.min(chunk_declared.saturating_mul(2))) {
            return Err(TableCatalogStoreError::Invalid(
                "compaction parquet page headers exceed the footer's declared chunk size".to_owned(),
            ));
        }
        cursor.skip_bytes(sizes.compressed)?;
    }
    Ok(total)
}

fn validate_column_chunk_page_headers(
    data: &Bytes,
    column: &ColumnChunkMetaData,
    chunk_declared: usize,
    limit: usize,
    location: &str,
) -> TableCatalogStoreResult<()> {
    // parquet's `byte_range`: the chunk starts at the dictionary page when the
    // footer declares one, wherever that is. Walk exactly the bytes it decodes.
    let start = column.dictionary_page_offset().unwrap_or(column.data_page_offset());
    let start = usize::try_from(start).map_err(|_| budget_error())?;
    let length = usize::try_from(column.compressed_size()).map_err(|_| budget_error())?;
    let end = start.checked_add(length).filter(|end| *end <= data.len()).ok_or_else(|| {
        TableCatalogStoreError::Invalid(format!("compaction parquet {location} column chunk lies outside the file"))
    })?;
    validate_page_headers_in_range(&data[start..end], chunk_declared, limit).map_err(|err| match err {
        TableCatalogStoreError::Invalid(message) => TableCatalogStoreError::Invalid(format!("{location}: {message}")),
        other => other,
    })?;
    Ok(())
}

fn view_field(field: &Field, depth: usize, fields: &mut usize) -> TableCatalogStoreResult<Arc<Field>> {
    add_budget(fields, 1, COMPACTION_SCHEMA_FIELDS)?;
    Ok(Arc::new(field.clone().with_data_type(view_type(field.data_type(), depth, fields)?)))
}

fn view_type(data_type: &DataType, depth: usize, fields: &mut usize) -> TableCatalogStoreResult<DataType> {
    if depth > COMPACTION_SCHEMA_DEPTH {
        return Err(budget_error());
    }
    Ok(match data_type {
        DataType::Utf8 | DataType::LargeUtf8 => DataType::Utf8View,
        DataType::Binary | DataType::LargeBinary => DataType::BinaryView,
        DataType::List(field) => DataType::List(view_field(field, depth + 1, fields)?),
        DataType::LargeList(field) => DataType::LargeList(view_field(field, depth + 1, fields)?),
        DataType::ListView(field) => DataType::ListView(view_field(field, depth + 1, fields)?),
        DataType::LargeListView(field) => DataType::LargeListView(view_field(field, depth + 1, fields)?),
        DataType::FixedSizeList(field, size) => DataType::FixedSizeList(view_field(field, depth + 1, fields)?, *size),
        DataType::Map(field, sorted) => DataType::Map(view_field(field, depth + 1, fields)?, *sorted),
        DataType::Struct(children) => DataType::Struct(
            children
                .iter()
                .map(|field| view_field(field, depth + 1, fields))
                .collect::<TableCatalogStoreResult<Vec<_>>>()?
                .into(),
        ),
        DataType::Dictionary(_, value) => {
            // The pinned reader does not support Dictionary<ByteView>. Keep
            // its already shared values, while still checking nested schema limits.
            let _ = view_type(value, depth + 1, fields)?;
            data_type.clone()
        }
        other => other.clone(),
    })
}

fn view_schema(schema: &Schema) -> TableCatalogStoreResult<SchemaRef> {
    let mut fields = 0;
    let mapped = schema
        .fields()
        .iter()
        .map(|field| view_field(field, 0, &mut fields))
        .collect::<TableCatalogStoreResult<Vec<_>>>()?;
    Ok(Arc::new(Schema::new_with_metadata(mapped, schema.metadata().clone())))
}

/// Charge the values the writer may expand, not the small view/dictionary
/// buffers. Even a null or empty nested cell consumes structural capacity.
fn charge_value(array: &dyn Array, row: usize, depth: usize, total: &mut usize, limit: usize) -> TableCatalogStoreResult<()> {
    if depth > COMPACTION_SCHEMA_DEPTH {
        return Err(budget_error());
    }
    add_budget(total, 8, limit)?;
    if array.is_null(row) {
        return Ok(());
    }
    macro_rules! scalar_bytes {
        ($ty:ty) => {
            add_budget(
                total,
                array
                    .as_any()
                    .downcast_ref::<$ty>()
                    .ok_or_else(budget_error)?
                    .value(row)
                    .len(),
                limit,
            )
        };
    }
    macro_rules! nested_values {
        ($ty:ty) => {{
            let value = array.as_any().downcast_ref::<$ty>().ok_or_else(budget_error)?.value(row);
            for index in 0..value.len() {
                charge_value(value.as_ref(), index, depth + 1, total, limit)?;
            }
            Ok(())
        }};
    }
    match array.data_type() {
        DataType::Utf8 => scalar_bytes!(StringArray),
        DataType::LargeUtf8 => scalar_bytes!(LargeStringArray),
        DataType::Utf8View => scalar_bytes!(StringViewArray),
        DataType::Binary => scalar_bytes!(BinaryArray),
        DataType::LargeBinary => scalar_bytes!(LargeBinaryArray),
        DataType::BinaryView => scalar_bytes!(BinaryViewArray),
        DataType::Boolean | DataType::Null => Ok(()),
        DataType::FixedSizeBinary(size) => add_budget(total, usize::try_from(*size).map_err(|_| budget_error())?, limit),
        DataType::List(_) => nested_values!(ListArray),
        DataType::LargeList(_) => nested_values!(LargeListArray),
        DataType::ListView(_) => nested_values!(ListViewArray),
        DataType::LargeListView(_) => nested_values!(LargeListViewArray),
        DataType::FixedSizeList(_, _) => nested_values!(FixedSizeListArray),
        DataType::Map(_, _) => {
            let value = array.as_any().downcast_ref::<MapArray>().ok_or_else(budget_error)?.value(row);
            for index in 0..value.len() {
                charge_value(&value, index, depth + 1, total, limit)?;
            }
            Ok(())
        }
        DataType::Struct(_) => {
            for column in array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(budget_error)?
                .columns()
            {
                charge_value(column.as_ref(), row, depth + 1, total, limit)?;
            }
            Ok(())
        }
        DataType::Dictionary(_, _) => downcast_dictionary_array!(array => {
            if let Some(key) = array.key(row) { charge_value(array.values().as_ref(), key, depth + 1, total, limit)?; }
            Ok(())
        }, _ => Err(budget_error())),
        data_type => add_budget(total, data_type.primitive_width().ok_or_else(budget_error)?, limit),
    }
}

struct BoundedOutput {
    bytes: Vec<u8>,
    limit: usize,
}
impl Write for BoundedOutput {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let Some(required) = self.bytes.len().checked_add(bytes.len()).filter(|size| *size <= self.limit) else {
            return Err(std::io::Error::other("compaction output exceeds the server size limit"));
        };
        if required > self.bytes.capacity() {
            // Parquet writes successive pages and buffered footer fragments.
            // Amortize their copies without allocating past the output budget.
            let capacity = required.max(self.bytes.capacity().saturating_mul(2)).min(self.limit);
            self.bytes
                .try_reserve_exact(capacity - self.bytes.len())
                .map_err(std::io::Error::other)?;
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn compact_parquet_data_files(input_files: Vec<(String, Vec<u8>)>) -> TableCatalogStoreResult<CompactedParquetFile> {
    compact_parquet_with_limits(input_files, TABLE_COMPACTION_MAX_DECODED_BYTES, COMPACTION_WRITE_BYTES)
}

fn compact_parquet_with_limits(
    input_files: Vec<(String, Vec<u8>)>,
    limit: usize,
    write_limit: usize,
) -> TableCatalogStoreResult<CompactedParquetFile> {
    compact_parquet_with_budgets(input_files, limit, write_limit, COMPACTION_MAX_ROW_GROUPS, COMPACTION_ROW_GROUP_ROWS)
}

fn compact_parquet_with_budgets(
    input_files: Vec<(String, Vec<u8>)>,
    limit: usize,
    write_limit: usize,
    group_limit: usize,
    row_group_rows: usize,
) -> TableCatalogStoreResult<CompactedParquetFile> {
    let mut schema: Option<SchemaRef> = None;
    let mut declared = 0;
    let mut stored = 0_usize;
    let mut files = Vec::new();
    let mut leaf_columns = 0;
    // Validate every footer before decoding any page or creating output.
    for (location, data) in input_files {
        stored = stored
            .checked_add(data.len())
            .filter(|total| *total <= TABLE_COMPACTION_MAX_STORED_BYTES)
            .ok_or_else(compaction_stored_inputs_ceiling_error)?;
        let data = Bytes::from(data);
        validate_footer_metadata(&data, limit).map_err(|err| match err {
            TableCatalogStoreError::Invalid(message) => TableCatalogStoreError::Invalid(format!("{location}: {message}")),
            other => other,
        })?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(data.clone())
            .map_err(|err| TableCatalogStoreError::Invalid(format!("failed to read compaction parquet {location}: {err}")))?;
        let file_schema = builder.schema().clone();
        leaf_columns = leaf_columns.max(builder.metadata().file_metadata().schema_descr().num_columns());
        match &schema {
            Some(expected) if expected.as_ref() != file_schema.as_ref() => {
                return Err(TableCatalogStoreError::Invalid("compaction input parquet schemas must match".to_owned()));
            }
            None => schema = Some(file_schema),
            _ => {}
        }
        for group in builder.metadata().row_groups() {
            let mut columns = 0;
            for column in group.columns() {
                let chunk_declared = usize::try_from(column.uncompressed_size()).map_err(|_| budget_error())?;
                add_budget(&mut columns, chunk_declared, limit)?;
                // The footer is one claim; the page headers the decoder actually
                // sizes its buffers from are another. Walk them without
                // decompressing and hold both to the budget.
                validate_column_chunk_page_headers(&data, column, chunk_declared, limit, &location)?;
            }
            let group_bytes = usize::try_from(group.total_byte_size()).map_err(|_| budget_error())?;
            add_budget(&mut declared, group_bytes.max(columns), limit)?;
        }
        files.push((location, data));
    }
    let schema = schema
        .ok_or_else(|| TableCatalogStoreError::Invalid("compaction requires at least one parquet input file".to_owned()))?;
    let read_schema = view_schema(&schema)?;
    let group_limit = group_limit.min(COMPACTION_MAX_COLUMN_CHUNKS / leaf_columns.max(1));
    // These bounds include metadata retained until the footer is serialized.
    // Page/offset indexes would add a separate, page-count-dependent inventory.
    let mut properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(row_group_rows))
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_statistics_truncate_length(Some(64))
        .set_offset_index_disabled(true)
        .build();
    // The writer requires nested array types to match its input schema exactly.
    // Views share the same Parquet physical/logical types; retain the original
    // Arrow schema in the footer so readers recover its types and field metadata.
    // Do not cast sliced lists/maps: Arrow casts their entire child buffers.
    add_encoded_arrow_schema_to_metadata(&schema, &mut properties);
    let options = ArrowWriterOptions::new()
        .with_properties(properties)
        .with_skip_arrow_metadata(true);
    let mut writer = ArrowWriter::try_new_with_options(
        BoundedOutput {
            bytes: Vec::new(),
            limit,
        },
        read_schema.clone(),
        options,
    )
    .map_err(|err| TableCatalogStoreError::Invalid(format!("failed to build compaction writer: {err}")))?;
    let mut record_count = 0_u64;
    let mut decoded = 0;
    for (location, data) in files {
        let options = ArrowReaderOptions::new().with_schema(read_schema.clone());
        let reader = ParquetRecordBatchReaderBuilder::try_new_with_options(data, options)
            .and_then(|builder| builder.with_batch_size(COMPACTION_BATCH_ROWS).build())
            .map_err(|err| TableCatalogStoreError::Invalid(format!("failed to build compaction reader {location}: {err}")))?;
        for batch in reader {
            let batch = batch
                .map_err(|err| TableCatalogStoreError::Invalid(format!("failed to decode compaction batch {location}: {err}")))?;
            add_budget(&mut decoded, batch.get_array_memory_size(), limit)?;
            let mut start = 0;
            while start < batch.num_rows() {
                let mut end = start;
                let mut expanded = 0_usize;
                let remaining_rows = row_group_rows.saturating_sub(writer.in_progress_rows());
                while end < batch.num_rows() && end - start < remaining_rows {
                    let mut row_bytes = 0;
                    for column in batch.columns() {
                        charge_value(column.as_ref(), end, 0, &mut row_bytes, write_limit)?;
                    }
                    if expanded.checked_add(row_bytes).is_none_or(|size| size > write_limit) {
                        break;
                    }
                    expanded += row_bytes;
                    end += 1;
                }
                if end == start {
                    return Err(budget_error());
                }
                if writer
                    .memory_size()
                    .checked_add(expanded)
                    .is_none_or(|size| size > write_limit)
                {
                    writer.flush().map_err(|err| {
                        TableCatalogStoreError::Invalid(format!("failed to flush bounded compaction output: {err}"))
                    })?;
                }
                if writer.flushed_row_groups().len() >= group_limit {
                    return Err(TableCatalogStoreError::Invalid(
                        "compaction output row-group metadata exceeds the server limit".to_owned(),
                    ));
                }
                writer.write(&batch.slice(start, end - start)).map_err(|err| {
                    TableCatalogStoreError::Invalid(format!("failed to write bounded compaction output: {err}"))
                })?;
                record_count = record_count.checked_add((end - start) as u64).ok_or_else(budget_error)?;
                start = end;
            }
        }
    }
    let output = writer
        .into_inner()
        .map_err(|err| TableCatalogStoreError::Invalid(format!("failed to finalize bounded compaction output: {err}")))?;
    Ok(CompactedParquetFile {
        data: output.bytes,
        record_count,
    })
}

pub(crate) async fn run_compaction_worker(
    input_files: Vec<(String, Vec<u8>)>,
    admission: Arc<OwnedSemaphorePermit>,
) -> TableCatalogStoreResult<CompactedParquetFile> {
    #[cfg(test)]
    let barrier = COMPACTION_WORKER_BARRIER.try_with(Arc::clone).ok();
    tokio::task::spawn_blocking(move || {
        let _admission = admission;
        #[cfg(test)]
        if let Some(barrier) = barrier {
            barrier.entered.notify_one();
            let mut released = barrier.released.lock().unwrap();
            while !*released {
                released = barrier.release.wait(released).unwrap();
            }
        }
        compact_parquet_data_files(input_files)
    })
    .await
    .map_err(|_| TableCatalogStoreError::Internal("compaction decoder worker failed".to_owned()))?
}

pub(crate) async fn run_compaction_output<F>(operation: F, admission: Arc<OwnedSemaphorePermit>) -> TableCatalogStoreResult<()>
where
    F: std::future::Future<Output = TableCatalogStoreResult<()>> + Send + 'static,
{
    let credentials = rustfs_keystone::KEYSTONE_CREDENTIALS.try_with(Clone::clone).ok().flatten();
    let operation = rustfs_keystone::KEYSTONE_CREDENTIALS.scope(credentials, async move {
        let _admission = admission;
        operation.await
    });
    let operation = rustfs_io_metrics::s3_http_metrics::with_s3_http_operation_context(
        rustfs_io_metrics::s3_http_metrics::current_s3_http_operation_context(),
        operation,
    );
    crate::storage_api::table::spawn_traced_join(operation)
        .await
        .map_err(|_| TableCatalogStoreError::Internal("compaction output worker failed".to_owned()))?
}

#[cfg(test)]
struct WorkerBarrier {
    entered: tokio::sync::Notify,
    released: std::sync::Mutex<bool>,
    release: std::sync::Condvar,
}
#[cfg(test)]
tokio::task_local! { static COMPACTION_WORKER_BARRIER: Arc<WorkerBarrier>; }

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::parquet::basic::{Compression, ZstdLevel};

    fn parquet_strings(value: &str, rows: usize, dictionary: bool) -> Vec<u8> {
        let schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new("value", DataType::Utf8, false)],
            [("custom-key".to_owned(), "preserved".to_owned())].into_iter().collect(),
        ));
        let values = StringArray::from(vec![value; rows]);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(values)]).unwrap();
        let properties = WriterProperties::builder()
            .set_dictionary_enabled(dictionary)
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .build();
        let mut writer = ArrowWriter::try_new(Vec::new(), schema, Some(properties)).unwrap();
        writer.write(&batch).unwrap();
        writer.into_inner().unwrap()
    }

    fn varint(mut value: u64) -> Vec<u8> {
        let mut out = Vec::new();
        loop {
            let byte = (value & 0x7f) as u8;
            value >>= 7;
            if value == 0 {
                out.push(byte);
                return out;
            }
            out.push(byte | 0x80);
        }
    }

    fn compact_field(field_delta: u8, kind: u8, value: i64) -> Vec<u8> {
        let mut out = vec![(field_delta << 4) | kind];
        out.extend(varint(((value << 1) ^ (value >> 63)) as u64));
        out
    }

    fn compact_i32_field(field_delta: u8, value: i64) -> Vec<u8> {
        compact_field(field_delta, THRIFT_COMPACT_I32, value)
    }

    /// `PageHeader { type: 0, uncompressed_page_size, compressed_page_size: 4 }`
    /// with `extra` spliced in as further fields, then four bytes of page.
    fn page_chunk(uncompressed: Vec<u8>, extra: &[u8]) -> Vec<u8> {
        let mut chunk = compact_i32_field(1, 0);
        chunk.extend(uncompressed);
        chunk.extend(compact_i32_field(1, 4));
        chunk.extend_from_slice(extra);
        chunk.push(THRIFT_COMPACT_STOP);
        chunk.extend([0_u8; 4]);
        chunk
    }

    /// `"PAR1" + footer + len + "PAR1"`: a file whose `FileMetaData` declares
    /// one schema element and a `row_groups` list of `row_groups` elements
    /// followed by `padding` bytes before the list's elements would start.
    fn footer_file(row_groups: u64, padding: usize) -> Vec<u8> {
        let mut footer = compact_i32_field(1, 1);
        footer.extend([(1 << 4) | THRIFT_COMPACT_LIST, (1 << 4) | THRIFT_COMPACT_STRUCT]);
        footer.extend([(4 << 4) | THRIFT_COMPACT_BINARY, 4]);
        footer.extend(b"root");
        footer.extend(compact_i32_field(1, 0));
        footer.push(THRIFT_COMPACT_STOP);
        footer.extend(compact_field(1, THRIFT_COMPACT_I64, 0));
        footer.extend([(1 << 4) | THRIFT_COMPACT_LIST, 0xf0 | THRIFT_COMPACT_STRUCT]);
        footer.extend(varint(row_groups));
        footer.extend(std::iter::repeat_n(THRIFT_COMPACT_STOP, padding));
        footer.push(THRIFT_COMPACT_STOP);
        let mut file = b"PAR1".to_vec();
        file.extend(&footer);
        file.extend(u32::try_from(footer.len()).unwrap().to_le_bytes());
        file.extend(b"PAR1");
        file
    }

    #[test]
    fn compaction_page_header_walker_matches_the_footer_of_an_honest_file() {
        let file = Bytes::from(parquet_strings("value", 1024, true));
        let builder = ParquetRecordBatchReaderBuilder::try_new(file.clone()).unwrap();
        for group in builder.metadata().row_groups() {
            for column in group.columns() {
                let declared = usize::try_from(column.uncompressed_size()).unwrap();
                validate_column_chunk_page_headers(&file, column, declared, usize::MAX, "honest.parquet")
                    .expect("an honest file's page headers fit its footer");
                let start = usize::try_from(column.dictionary_page_offset().unwrap_or(column.data_page_offset())).unwrap();
                let end = start + usize::try_from(column.compressed_size()).unwrap();
                let total = validate_page_headers_in_range(&file[start..end], declared, usize::MAX).unwrap();
                // The footer's uncompressed size also counts the page headers themselves.
                assert!(total > 0 && total <= declared, "page payload sizes ({total}) fit the footer's {declared}");
            }
        }
    }

    #[test]
    fn compaction_page_header_walker_rejects_a_page_that_lies_about_its_size() {
        // data_page_header struct (field 5, delta 2): one BOOL field, then STOP.
        let data_page_header = [
            (2 << 4) | THRIFT_COMPACT_STRUCT,
            (1 << 4) | THRIFT_COMPACT_BOOL_TRUE,
            THRIFT_COMPACT_STOP,
        ];
        let chunk = page_chunk(compact_i32_field(1, i64::from(i32::MAX)), &data_page_header);
        let error = validate_page_headers_in_range(&chunk, 4096, 64 * 1024 * 1024)
            .expect_err("a page header above the budget must be rejected before any allocation");
        assert!(matches!(error, TableCatalogStoreError::Invalid(_)), "{error:?}");
        // The same header within budget but far above the footer's chunk size is also a lie.
        let mut chunk = compact_i32_field(1, 0);
        chunk.extend(compact_i32_field(1, 1024 * 1024));
        chunk.extend(compact_i32_field(1, 4));
        chunk.push(THRIFT_COMPACT_STOP);
        chunk.extend([0_u8; 4]);
        let error = validate_page_headers_in_range(&chunk, 4096, 64 * 1024 * 1024)
            .expect_err("page headers far above the footer's chunk size are rejected");
        assert!(matches!(error, TableCatalogStoreError::Invalid(message) if message.contains("footer")));
        // Truncated header: the cursor must fail closed, never read past the chunk.
        assert!(validate_page_headers_in_range(&compact_i32_field(1, 0), 4096, 4096).is_err());
    }

    #[test]
    fn compaction_page_header_walker_reads_size_fields_by_id_like_parquet() {
        let limit = 64 * 1024 * 1024;
        // parquet reads field 2 as a zigzag varint whatever the wire kind says,
        // so an I64 or I16 kind must still charge the budget, never read as 0.
        for kind in [THRIFT_COMPACT_I64, THRIFT_COMPACT_I16] {
            let chunk = page_chunk(compact_field(1, kind, i64::from(i32::MAX)), &[]);
            let error = validate_page_headers_in_range(&chunk, 4096, limit)
                .expect_err("a size field with an integer kind other than I32 must still be charged");
            assert!(
                matches!(&error, TableCatalogStoreError::Invalid(message) if message.contains("memory or expanded-value budget")),
                "{error:?}"
            );
        }
        // The same kinds within budget are honest encodings and still walk.
        let chunk = page_chunk(compact_field(1, THRIFT_COMPACT_I64, 16), &[]);
        assert_eq!(validate_page_headers_in_range(&chunk, 4096, limit).unwrap(), 16);
        // A kind parquet would still read a varint for, but that is not an
        // integer, desynchronizes the two parsers: fail closed.
        for kind in [THRIFT_COMPACT_BOOL_TRUE, THRIFT_COMPACT_BINARY, THRIFT_COMPACT_STRUCT] {
            let mut chunk = compact_i32_field(1, 0);
            chunk.push((1 << 4) | kind);
            chunk.extend(compact_i32_field(1, 4));
            chunk.push(THRIFT_COMPACT_STOP);
            chunk.extend([0_u8; 4]);
            let error = validate_page_headers_in_range(&chunk, 4096, limit).expect_err("non-integer size kinds are malformed");
            assert!(
                matches!(&error, TableCatalogStoreError::Invalid(message) if message.contains("malformed")),
                "{error:?}"
            );
        }
    }

    #[test]
    fn compaction_page_header_walker_bounds_collection_counts_by_remaining_bytes() {
        let limit = 64 * 1024 * 1024;
        // Unknown field 4: LIST<BOOL> with long-form count 2^63. Parquet's skip
        // would spin; the walker must reject it before iterating.
        let mut list = vec![(1 << 4) | THRIFT_COMPACT_LIST, 0xf0 | THRIFT_COMPACT_BOOL_TRUE];
        list.extend(varint(1 << 63));
        let started = std::time::Instant::now();
        let error = validate_page_headers_in_range(&page_chunk(compact_i32_field(1, 8), &list), 4096, limit)
            .expect_err("a collection count beyond the chunk is malformed");
        assert!(
            matches!(&error, TableCatalogStoreError::Invalid(message) if message.contains("malformed")),
            "{error:?}"
        );
        assert!(started.elapsed() < std::time::Duration::from_secs(1));
        // A bool collection of any size is rejected: parquet consumes no bytes
        // per element, so either width would desynchronize the two parsers.
        let list = [(1 << 4) | THRIFT_COMPACT_LIST, (2 << 4) | THRIFT_COMPACT_BOOL_TRUE, 1, 1];
        assert!(validate_page_headers_in_range(&page_chunk(compact_i32_field(1, 8), &list), 4096, limit).is_err());
        // MAP<BOOL, BOOL> with a huge count is rejected the same way.
        let mut map = vec![(1 << 4) | THRIFT_COMPACT_MAP];
        map.extend(varint(1 << 62));
        map.push((THRIFT_COMPACT_BOOL_TRUE << 4) | THRIFT_COMPACT_BOOL_TRUE);
        assert!(validate_page_headers_in_range(&page_chunk(compact_i32_field(1, 8), &map), 4096, limit).is_err());
        // Collections whose elements carry bytes still skip when the count fits.
        let mut list = vec![(1 << 4) | THRIFT_COMPACT_LIST, (2 << 4) | THRIFT_COMPACT_I32];
        list.extend(compact_i32_field(0, 7)[1..].iter());
        list.extend(compact_i32_field(0, -7)[1..].iter());
        assert_eq!(
            validate_page_headers_in_range(&page_chunk(compact_i32_field(1, 8), &list), 4096, limit).unwrap(),
            8
        );
        // A LIST<I32> whose count exceeds the chunk is malformed, not truncated.
        let list = [(1 << 4) | THRIFT_COMPACT_LIST, 0xf0 | THRIFT_COMPACT_I32, 0x7f];
        assert!(validate_page_headers_in_range(&page_chunk(compact_i32_field(1, 8), &list), 4096, limit).is_err());
    }

    #[test]
    fn compaction_page_header_walker_starts_at_the_dictionary_offset_like_parquet() {
        // An honest file followed by a lying page header. The footer points the
        // data pages at the honest chain but the dictionary page at the lie;
        // parquet's byte_range decodes from the dictionary offset, so the
        // walker must validate exactly those bytes.
        let honest = parquet_strings("value", 1024, true);
        let lie = page_chunk(compact_i32_field(1, i64::from(i32::MAX)), &[]);
        let mut file = honest.clone();
        let lie_offset = i64::try_from(file.len()).unwrap();
        file.extend(&lie);
        let file = Bytes::from(file);
        let honest_builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(honest)).unwrap();
        let column = honest_builder.metadata().row_group(0).column(0);
        let descr = honest_builder.metadata().file_metadata().schema_descr().column(0);
        let declared = usize::try_from(column.uncompressed_size()).unwrap();
        let limit = 64 * 1024 * 1024;
        let crafted = ColumnChunkMetaData::builder(descr)
            .set_data_page_offset(column.data_page_offset())
            .set_dictionary_page_offset(Some(lie_offset))
            .set_total_compressed_size(i64::try_from(lie.len()).unwrap())
            .set_total_uncompressed_size(column.uncompressed_size())
            .build()
            .unwrap();
        validate_column_chunk_page_headers(&file, column, declared, limit, "honest.parquet")
            .expect("the honest footer still validates its own chunk");
        let error = validate_column_chunk_page_headers(&file, &crafted, declared, limit, "crafted.parquet")
            .expect_err("the lying dictionary page is the chunk parquet decodes first");
        assert!(
            matches!(&error, TableCatalogStoreError::Invalid(message) if message.contains("crafted.parquet") && message.contains("budget")),
            "{error:?}"
        );
    }

    #[test]
    fn compaction_footer_prewalk_rejects_row_group_counts_before_parquet_allocates() {
        let limit = 64 * 1024 * 1024;
        // Honest footers, including their trailing created_by/key-value fields, walk.
        validate_footer_metadata(&Bytes::from(parquet_strings("value", 64, true)), limit).expect("honest footer");
        validate_footer_metadata(&Bytes::from(parquet_strings("value", 64, false)), limit).expect("honest footer");
        // A ~40-byte footer declaring 2^31 - 1 row groups: parquet 59 would
        // reserve the whole Vec before reading one element.
        let bomb = footer_file(u64::from(u32::MAX >> 1), 0);
        assert!(bomb.len() < 64, "fixture must be a tiny file");
        let error = validate_footer_metadata(&Bytes::from(bomb.clone()), limit).expect_err("row-group count beyond the footer");
        assert!(
            matches!(&error, TableCatalogStoreError::Invalid(message) if message.contains("malformed")),
            "{error:?}"
        );
        let error = compact_parquet_with_limits(vec![("bomb.parquet".to_owned(), bomb)], limit, 64 * 1024)
            .err()
            .expect("the footer must be rejected before parquet decodes it");
        assert!(
            matches!(&error, TableCatalogStoreError::Invalid(message) if message.starts_with("bomb.parquet: ") && message.contains("malformed")),
            "{error:?}"
        );
        // A count that fits the footer bytes is still charged as parquet's
        // reservation: 50 empty structs fit in 64 padding bytes, but
        // 50 row-group reservations do not fit a 1 KiB budget.
        let reserved = footer_file(50, 64);
        validate_footer_metadata(&Bytes::from(reserved.clone()), limit).expect("a fitting count walks");
        let error = validate_footer_metadata(&Bytes::from(reserved), 1024).expect_err("reservation over budget");
        assert!(
            matches!(&error, TableCatalogStoreError::Invalid(message) if message.contains("budget")),
            "{error:?}"
        );
        // Trailers that are not Parquet are malformed, not panics.
        for broken in [Vec::new(), b"PAR1".to_vec(), b"PAR1\xff\xff\xff\x7fPAR1".to_vec()] {
            assert!(validate_footer_metadata(&Bytes::from(broken), limit).is_err());
        }
    }

    #[test]
    fn compaction_rejects_compressed_expansion_before_decoding_pages() {
        let file = parquet_strings(&"x".repeat(64 * 1024), 32, false);
        assert!(file.len() < 16 * 1024, "fixture must really be highly compressed");
        let error = compact_parquet_with_limits(vec![("bomb.parquet".to_owned(), file)], 128 * 1024, 64 * 1024)
            .err()
            .expect("footer must reject decoded expansion before allocation");
        assert!(matches!(error, TableCatalogStoreError::Invalid(_)));
    }

    #[test]
    fn compaction_dictionary_views_bound_each_writer_input_and_preserve_schema() {
        let value = "x".repeat(16 * 1024);
        let file = parquet_strings(&value, 32, true);
        let original_schema = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(file.clone()))
            .unwrap()
            .schema()
            .clone();
        let output =
            compact_parquet_with_limits(vec![("dictionary.parquet".to_owned(), file)], 2 * 1024 * 1024, 32 * 1024).unwrap();
        assert_eq!(output.record_count, 32);
        let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(output.data)).unwrap();
        assert_eq!(builder.schema(), &original_schema);
        assert!(builder.metadata().num_row_groups() <= 32);
        for batch in builder.build().unwrap() {
            let batch = batch.unwrap();
            let values = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            assert!(values.iter().all(|item| item == Some(value.as_str())));
        }
    }

    #[test]
    fn compaction_rejects_a_single_expanded_value_before_writer_allocation() {
        let file = parquet_strings(&"x".repeat(4096), 16, true);
        let error = compact_parquet_with_limits(vec![("single-row.parquet".to_owned(), file)], 1024 * 1024, 1024)
            .err()
            .expect("one expanded row cannot exceed the writer input budget");
        assert!(matches!(error, TableCatalogStoreError::Invalid(_)));
    }

    #[test]
    fn compaction_nested_views_keep_field_and_schema_metadata() {
        use datafusion::arrow::array::{ListBuilder, StringBuilder};
        let child = Arc::new(
            Field::new("text", DataType::Utf8, false)
                .with_metadata([("PARQUET:field_id".to_owned(), "2".to_owned())].into_iter().collect()),
        );
        let array = StructArray::from(vec![(child.clone(), Arc::new(StringArray::from(vec!["nested"; 4])) as Arc<dyn Array>)]);
        let value = "nested".repeat(32);
        let mut lists = ListBuilder::new(StringBuilder::new());
        for _ in 0..4 {
            lists.values().append_value(&value);
            lists.append(true);
        }
        let lists = lists.finish();
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("outer", DataType::Struct(vec![child].into()), false),
                Field::new("list", lists.data_type().clone(), false),
            ],
            [("source".to_owned(), "nested-compaction".to_owned())].into_iter().collect(),
        ));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array), Arc::new(lists)]).unwrap();
        let mut writer = ArrowWriter::try_new(Vec::new(), schema.clone(), None).unwrap();
        writer.write(&batch).unwrap();
        let file = writer.into_inner().unwrap();
        let rejected = compact_parquet_with_limits(vec![("nested.parquet".to_owned(), file.clone())], 1024 * 1024, 200)
            .err()
            .expect("a nested row must fit before reaching the writer");
        assert!(matches!(rejected, TableCatalogStoreError::Invalid(message) if message.contains("expanded-value budget")));
        // Force slices within the decoded nested batch. They must not require
        // expanding all values in the retained child buffer before writing.
        let output = compact_parquet_with_limits(vec![("nested.parquet".to_owned(), file)], 1024 * 1024, 300).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(output.data)).unwrap();
        assert_eq!(builder.schema(), &schema);
        let batches = builder.build().unwrap().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(batches.iter().map(|batch| batch.num_rows()).sum::<usize>(), 4);
        for batch in batches {
            let outer = batch.column(0).as_any().downcast_ref::<StructArray>().unwrap();
            let texts = outer.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            assert!(texts.iter().all(|text| text == Some("nested")));
            let lists = batch.column(1).as_any().downcast_ref::<ListArray>().unwrap();
            for row in 0..batch.num_rows() {
                let values = lists.value(row);
                let values = values.as_any().downcast_ref::<StringArray>().unwrap();
                assert_eq!(values.len(), 1);
                assert_eq!(values.value(0), value);
            }
        }
    }

    #[test]
    fn compaction_arrow_dictionary_schema_round_trips_through_view_reader() {
        use datafusion::arrow::array::{DictionaryArray, Int8Array};
        use datafusion::arrow::datatypes::Int8Type;
        let values = Arc::new(StringArray::from(vec!["dictionary-value".repeat(1024)]));
        let array = DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![0; 16]), values).unwrap();
        let field = Arc::new(Field::new("dictionary", array.data_type().clone(), false));
        let array = Arc::new(array);
        let nested = StructArray::from(vec![(field.clone(), array.clone() as Arc<dyn Array>)]);
        let schema = Arc::new(Schema::new(vec![
            field.as_ref().clone(),
            Field::new("nested", nested.data_type().clone(), false),
        ]));
        let batch = RecordBatch::try_new(schema.clone(), vec![array, Arc::new(nested)]).unwrap();
        let mut writer = ArrowWriter::try_new(Vec::new(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        let file = writer.into_inner().unwrap();
        let expected = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(file.clone()))
            .unwrap()
            .schema()
            .clone();
        let output =
            compact_parquet_with_limits(vec![("arrow-dictionary.parquet".to_owned(), file)], 1024 * 1024, 40 * 1024).unwrap();
        assert_eq!(output.record_count, 16);
        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(output.data)).unwrap();
        assert_eq!(reader.schema(), &expected);
        let batches = reader.build().unwrap().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 16);
        for batch in batches {
            let nested = batch.column(1).as_any().downcast_ref::<StructArray>().unwrap();
            for column in [batch.column(0), nested.column(0)] {
                let dictionary = column.as_any().downcast_ref::<DictionaryArray<Int8Type>>().unwrap();
                let values = dictionary.values().as_any().downcast_ref::<StringArray>().unwrap();
                for row in 0..dictionary.len() {
                    assert_eq!(values.value(dictionary.key(row).unwrap()), "dictionary-value".repeat(1024));
                }
            }
        }
    }

    #[test]
    fn compaction_retained_row_group_metadata_is_bounded_before_another_group() {
        let file = parquet_strings("", 1024, true);
        let output =
            compact_parquet_with_budgets(vec![("rle.parquet".to_owned(), file)], 1024 * 1024, 64 * 1024, 2, 512).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(output.data)).unwrap();
        assert_eq!(
            reader.metadata().num_row_groups(),
            2,
            "256-row input batches must not force separate row groups"
        );
        assert_eq!(output.record_count, 1024);
        let file = parquet_strings("", 1025, true);
        let error = compact_parquet_with_budgets(vec![("rle.parquet".to_owned(), file)], 1024 * 1024, 64 * 1024, 2, 512)
            .err()
            .expect("a third row group must not allocate retained metadata");
        assert!(matches!(error, TableCatalogStoreError::Invalid(message) if message.contains("row-group metadata")));
    }

    #[test]
    fn compaction_output_sink_rejects_before_growing() {
        let mut output = BoundedOutput {
            bytes: Vec::new(),
            limit: 4,
        };
        output.write_all(b"1234").unwrap();
        let capacity = output.bytes.capacity();
        assert!(output.write_all(b"5").is_err());
        assert_eq!(&output.bytes, b"1234");
        assert_eq!(output.bytes.capacity(), capacity, "rejected writes must not allocate");
    }

    #[test]
    fn compaction_output_sink_amortizes_small_writes_within_the_capacity_limit() {
        // Include a non-power-of-two cap so the last growth must stop at the
        // limit instead of using Vec's unconstrained doubling policy.
        for chunk_size in [1, 7, 64] {
            let mut output = BoundedOutput {
                bytes: Vec::new(),
                limit: 4099,
            };
            let mut growths = 0;
            let mut capacity = 0;
            while output.bytes.len() < output.limit {
                let count = chunk_size.min(output.limit - output.bytes.len());
                output.write_all(&[42; 64][..count]).expect("bounded fragment");
                if output.bytes.capacity() != capacity {
                    growths += 1;
                    capacity = output.bytes.capacity();
                }
                assert!(capacity >= output.bytes.len() && capacity <= output.limit);
            }
            assert!(growths <= 14, "small writes must require logarithmic capacity growth, got {growths}");
            assert_eq!(output.bytes, vec![42; output.limit]);
            assert!(output.write_all(b"x").is_err());
            assert_eq!(output.bytes.capacity(), capacity);
            assert_eq!(output.bytes.len(), output.limit);
        }
    }

    #[tokio::test]
    #[serial_test::serial(table_compaction)]
    async fn compaction_output_worker_preserves_keystone_authorization_context() {
        let credentials = rustfs_credentials::Credentials {
            access_key: "keystone:compaction".to_owned(),
            ..Default::default()
        };
        let admission = try_admit_compaction().unwrap();
        rustfs_keystone::KEYSTONE_CREDENTIALS
            .scope(Some(credentials), async {
                run_compaction_output(
                    async {
                        assert_eq!(
                            rustfs_keystone::KEYSTONE_CREDENTIALS
                                .with(|credentials| credentials.as_ref().map(|value| value.access_key.clone())),
                            Some("keystone:compaction".to_owned())
                        );
                        Ok(())
                    },
                    admission,
                )
                .await
                .unwrap();
            })
            .await;
        assert!(rustfs_keystone::KEYSTONE_CREDENTIALS.try_with(Clone::clone).is_err());
    }

    #[tokio::test]
    #[serial_test::serial(table_compaction)]
    async fn compaction_cancelled_caller_keeps_actual_decoder_worker_admitted() {
        let barrier = Arc::new(WorkerBarrier {
            entered: tokio::sync::Notify::new(),
            released: std::sync::Mutex::new(false),
            release: std::sync::Condvar::new(),
        });
        let admission = try_admit_compaction().unwrap();
        let file = parquet_strings("valid", 4, true);
        let scope_barrier = barrier.clone();
        let caller = tokio::spawn(COMPACTION_WORKER_BARRIER.scope(scope_barrier, async move {
            run_compaction_worker(vec![("valid.parquet".to_owned(), file)], admission).await
        }));
        barrier.entered.notified().await;
        caller.abort();
        let _ = caller.await;
        assert!(matches!(try_admit_compaction(), Err(TableCatalogStoreError::Unavailable(_))));
        *barrier.released.lock().unwrap() = true;
        barrier.release.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                if try_admit_compaction().is_ok() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("worker completion must return the permit");
    }
}
