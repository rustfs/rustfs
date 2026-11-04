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

fn add_budget(total: &mut usize, amount: usize, limit: usize) -> TableCatalogStoreResult<()> {
    *total = total
        .checked_add(amount)
        .filter(|value| *value <= limit)
        .ok_or_else(budget_error)?;
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

pub(crate) fn compact_parquet_data_files(input_files: Vec<(String, Vec<u8>)>) -> TableCatalogStoreResult<CompactedParquetFile> {
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
    let mut stored = 0;
    let mut files = Vec::new();
    let mut leaf_columns = 0;
    // Validate every footer before decoding any page or creating output.
    for (location, data) in input_files {
        add_budget(&mut stored, data.len(), TABLE_COMPACTION_MAX_STORED_BYTES)?;
        let data = Bytes::from(data);
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
                add_budget(
                    &mut columns,
                    usize::try_from(column.uncompressed_size()).map_err(|_| budget_error())?,
                    limit,
                )?;
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
