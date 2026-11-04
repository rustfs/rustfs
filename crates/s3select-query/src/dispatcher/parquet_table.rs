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

use std::{fmt, ops::Range, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::{
    arrow::{
        array::{Array, cast::AsArray, downcast_dictionary_array},
        datatypes::{DataType, FieldRef, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    catalog::Session,
    common::{DataFusionError, Result as DFResult},
    datasource::{
        TableProvider,
        listing::{ListingTableUrl, PartitionedFile},
        physical_plan::{FileScanConfigBuilder, ParquetSource, parquet::ParquetAccessPlan},
        source::DataSourceExec,
    },
    execution::{memory_pool::MemoryLimit, object_store::ObjectStoreUrl},
    logical_expr::{Expr, TableProviderFilterPushDown, TableType},
    object_store::{Error as ObjectStoreError, ObjectStore, ObjectStoreExt, path::Path},
    parquet::{
        arrow::{ParquetRecordBatchStreamBuilder, arrow_reader::ArrowReaderOptions, async_reader::AsyncFileReader},
        errors::{ParquetError, Result as ParquetResult},
        file::metadata::{ParquetMetaData, ParquetMetaDataReader, RowGroupMetaData},
    },
    physical_plan::{
        ExecutionPlan, SendableRecordBatchStream,
        stream::RecordBatchStreamAdapter,
        streaming::{PartitionStream, StreamingTableExec},
    },
};
use futures::{FutureExt, TryFutureExt, TryStreamExt, future::BoxFuture, stream};
use rustfs_s3select_api::{
    QueryResult,
    object_store::{SelectScanRange, scan_range_from_bounds},
};
use s3s::dto::SelectObjectContentInput;

#[derive(Clone)]
pub struct ParquetSelectTable {
    schema: SchemaRef,
    object_store_url: ObjectStoreUrl,
    object_path: String,
    object_size: u64,
    access_plan: Option<Arc<ParquetAccessPlan>>,
    expanded_row_budget: usize,
}

impl fmt::Debug for ParquetSelectTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ParquetSelectTable")
            .field("object_store_url", &self.object_store_url)
            .field("object_path", &self.object_path)
            .field("object_size", &self.object_size)
            .field("has_access_plan", &self.access_plan.is_some())
            .finish()
    }
}

struct ObjectStoreParquetReader {
    store: Arc<dyn ObjectStore>,
    path: Path,
    file_size: u64,
    metadata_read_limit: usize,
}

impl AsyncFileReader for ObjectStoreParquetReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        if range.end.saturating_sub(range.start) > self.metadata_read_limit as u64 {
            return async { Err(ParquetError::External(Box::new(parquet_budget_error()))) }.boxed();
        }
        self.store.get_range(&self.path, range).map_err(parquet_store_error).boxed()
    }

    fn get_byte_ranges(&mut self, ranges: Vec<Range<u64>>) -> BoxFuture<'_, ParquetResult<Vec<Bytes>>> {
        let total = ranges
            .iter()
            .try_fold(0u64, |sum, range| sum.checked_add(range.end.saturating_sub(range.start)));
        if total.is_none_or(|total| total > self.metadata_read_limit as u64) {
            return async { Err(ParquetError::External(Box::new(parquet_budget_error()))) }.boxed();
        }
        async move { self.store.get_ranges(&self.path, &ranges).await.map_err(parquet_store_error) }.boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
        async move {
            let metadata_options = options.map(|options| options.metadata_options().clone());
            let mut metadata_reader = ParquetMetaDataReader::new().with_metadata_options(metadata_options);

            if let Some(options) = options {
                metadata_reader = metadata_reader
                    .with_column_index_policy(options.column_index_policy())
                    .with_offset_index_policy(options.offset_index_policy());
            }

            let file_size = self.file_size;
            let metadata = metadata_reader.load_and_finish(self, file_size).await?;
            Ok(Arc::new(metadata))
        }
        .boxed()
    }
}

impl ParquetSelectTable {
    pub async fn try_new(state: &dyn Session, input: &SelectObjectContentInput) -> QueryResult<Arc<dyn TableProvider>> {
        let table_path = ListingTableUrl::parse(format!("s3://{}/{}", input.bucket, input.key))?;
        let object_store_url = table_path.object_store();
        let object_location = Path::from(input.key.clone());
        let store = state.runtime_env().object_store(&object_store_url)?;
        let object_meta = store
            .head(&object_location)
            .await
            .map_err(datafusion::common::DataFusionError::from)?;

        let memory_budget = match state.runtime_env().memory_pool.memory_limit() {
            MemoryLimit::Finite(bytes) => bytes,
            _ => rustfs_s3select_api::query::session::DEFAULT_S3SELECT_MEMORY_LIMIT_BYTES,
        };
        let reader = ObjectStoreParquetReader {
            store: Arc::clone(&store),
            path: object_location,
            file_size: object_meta.size,
            metadata_read_limit: (memory_budget / 16).max(1),
        };
        let builder = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .map_err(datafusion::common::DataFusionError::from)?;
        let schema = Arc::new(Schema::new_with_metadata(
            builder
                .schema()
                .fields()
                .iter()
                .map(|field| parquet_view_field(field, 0))
                .collect::<DFResult<Vec<_>>>()?,
            builder.schema().metadata().clone(),
        ));
        let metadata = Arc::clone(builder.metadata());
        // This is admission for valid Parquet declarations, not a replacement
        // for limits inside the footer parser or native compression codecs.
        let expanded_row_budget = (memory_budget / 16).max(1);
        let access_plan = parquet_access_plan(input, object_meta.size, metadata.as_ref())?;
        admit_parquet_decode(metadata.as_ref(), access_plan.as_deref(), memory_budget / 4)?;

        Ok(Arc::new(Self {
            schema,
            object_store_url,
            object_path: input.key.clone(),
            object_size: object_meta.size,
            access_plan,
            expanded_row_budget,
        }))
    }

    fn partitioned_file(&self) -> PartitionedFile {
        let file = PartitionedFile::new(self.object_path.clone(), self.object_size).with_arrow_schema(Arc::clone(&self.schema));
        if let Some(access_plan) = self.access_plan.as_ref() {
            file.with_extension(access_plan.as_ref().clone())
        } else {
            file
        }
    }
}

#[async_trait]
impl TableProvider for ParquetSelectTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if state.config_options().execution.batch_size.get() != 1 || state.config_options().execution.coalesce_batches {
            return Err(DataFusionError::Plan(
                "S3 Select Parquet requires single-row execution without batch coalescing".into(),
            ));
        }
        let scan_limit = if filters.is_empty() { limit } else { None };
        let file_source = Arc::new(ParquetSource::new(Arc::clone(&self.schema)));
        let config = FileScanConfigBuilder::new(self.object_store_url.clone(), file_source)
            .with_file(self.partitioned_file())
            .with_projection_indices(projection.cloned())?
            .with_limit(scan_limit)
            .with_batch_size(Some(1))
            .build();
        let input: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(config);
        let schema = input.schema();
        let partition: Arc<dyn PartitionStream> = Arc::new(ParquetBudgetPartition {
            input,
            schema: Arc::clone(&schema),
            expanded_row_budget: self.expanded_row_budget,
        });
        // An opaque source boundary keeps expression/cast pushdown outside the
        // expansion check. One file group is read serially, without repartitioning.
        Ok(Arc::new(StreamingTableExec::try_new(
            schema,
            vec![partition],
            None,
            [],
            false,
            scan_limit,
        )?))
    }

    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}

const PARQUET_MAX_TYPE_DEPTH: usize = 64;

fn parquet_budget_error() -> DataFusionError {
    DataFusionError::ResourcesExhausted("S3 Select Parquet decoded data exceeds the query memory budget".into())
}

fn parquet_view_field(field: &FieldRef, depth: usize) -> DFResult<FieldRef> {
    if depth >= PARQUET_MAX_TYPE_DEPTH {
        return Err(parquet_budget_error());
    }
    let recurse = |child: &FieldRef| parquet_view_field(child, depth + 1);
    let data_type = match field.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 => DataType::Utf8View,
        DataType::Binary | DataType::LargeBinary => DataType::BinaryView,
        DataType::List(child) => DataType::List(recurse(child)?),
        DataType::LargeList(child) => DataType::LargeList(recurse(child)?),
        DataType::ListView(child) => DataType::ListView(recurse(child)?),
        DataType::LargeListView(child) => DataType::LargeListView(recurse(child)?),
        DataType::FixedSizeList(child, size) => DataType::FixedSizeList(recurse(child)?, *size),
        DataType::Map(child, sorted) => DataType::Map(recurse(child)?, *sorted),
        DataType::Struct(fields) => DataType::Struct(fields.iter().map(recurse).collect::<DFResult<Vec<_>>>()?.into()),
        // Preserve the dictionary: its indices already share the value buffer.
        // A later cast is still checked by the actual per-row expansion budget.
        other => other.clone(),
    };
    Ok(Arc::new(field.as_ref().clone().with_data_type(data_type)))
}

fn admit_parquet_decode(metadata: &ParquetMetaData, access_plan: Option<&ParquetAccessPlan>, limit: usize) -> DFResult<()> {
    // The pinned DataFusion reader drains one active row group before fetching
    // the next. Parquet's DataRequest hands its ranges to that group and clears
    // them from PushBuffers. Do not reject a large file of many small groups.
    for (index, group) in metadata.row_groups().iter().enumerate() {
        if access_plan.is_some_and(|plan| !plan.should_scan(index)) {
            continue;
        }
        let mut resident = 0usize;
        for column in group.columns() {
            let decoded = usize::try_from(column.uncompressed_size()).map_err(|_| parquet_budget_error())?;
            let compressed = usize::try_from(column.compressed_size()).map_err(|_| parquet_budget_error())?;
            let values = usize::try_from(column.num_values()).map_err(|_| parquet_budget_error())?;
            // Include a conservative view/offset/validity allowance, including
            // repeated nested values that can all belong to a single row.
            resident = resident
                .checked_add(compressed)
                .and_then(|sum| sum.checked_add(decoded))
                .and_then(|sum| values.checked_mul(32).and_then(|views| sum.checked_add(views)))
                .ok_or_else(parquet_budget_error)?;
            if resident > limit {
                return Err(parquet_budget_error());
            }
        }
    }
    Ok(())
}

#[derive(Debug)]
struct ParquetBudgetPartition {
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    expanded_row_budget: usize,
}

impl PartitionStream for ParquetBudgetPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, context: Arc<datafusion::execution::TaskContext>) -> SendableRecordBatchStream {
        let schema = Arc::clone(&self.schema);
        let input = match self.input.execute(0, context) {
            Ok(input) => input,
            Err(error) => return Box::pin(RecordBatchStreamAdapter::new(schema, stream::once(async move { Err(error) }))),
        };
        let budget = self.expanded_row_budget;
        let rows = stream::try_unfold((input, None::<RecordBatch>, 0usize), move |(mut input, mut batch, mut row)| async move {
            loop {
                if let Some(current) = batch.as_ref()
                    && row < current.num_rows()
                {
                    let mut remaining = budget;
                    for column in current.columns() {
                        charge_parquet_value(column.as_ref(), row, 0, &mut remaining)?;
                    }
                    let output = current.slice(row, 1);
                    row += 1;
                    return Ok(Some((output, (input, batch, row))));
                }
                batch = input.try_next().await?;
                row = 0;
                if batch.is_none() {
                    return Ok(None);
                }
            }
        });
        Box::pin(RecordBatchStreamAdapter::new(schema, rows))
    }
}

fn charge_parquet_value(array: &dyn Array, row: usize, depth: usize, remaining: &mut usize) -> DFResult<()> {
    if depth >= PARQUET_MAX_TYPE_DEPTH {
        return Err(parquet_budget_error());
    }
    // Offset, validity and view bookkeeping still costs memory for empty cells.
    *remaining = remaining.checked_sub(32).ok_or_else(parquet_budget_error)?;
    if array.is_null(row) {
        return Ok(());
    }
    let bytes = match array.data_type() {
        DataType::Utf8View => array.as_string_view().value(row).len(),
        DataType::BinaryView => array.as_binary_view().value(row).len(),
        DataType::Utf8 => array.as_string::<i32>().value(row).len(),
        DataType::LargeUtf8 => array.as_string::<i64>().value(row).len(),
        DataType::Binary => array.as_binary::<i32>().value(row).len(),
        DataType::LargeBinary => array.as_binary::<i64>().value(row).len(),
        DataType::FixedSizeBinary(size) => usize::try_from(*size).map_err(|_| parquet_budget_error())?,
        DataType::Struct(_) => {
            for child in array.as_struct().columns() {
                charge_parquet_value(child.as_ref(), row, depth + 1, remaining)?;
            }
            return Ok(());
        }
        DataType::Dictionary(_, _) => {
            return downcast_dictionary_array!(array => {
                let key = array.key(row).ok_or_else(parquet_budget_error)?;
                charge_parquet_value(array.values().as_ref(), key, depth + 1, remaining)
            }, _ => Err(parquet_budget_error()));
        }
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::FixedSizeList(_, _)
        | DataType::Map(_, _) => {
            let values: Arc<dyn Array> = match array.data_type() {
                DataType::List(_) => array.as_list::<i32>().value(row),
                DataType::LargeList(_) => array.as_list::<i64>().value(row),
                DataType::ListView(_) => array.as_list_view::<i32>().value(row),
                DataType::LargeListView(_) => array.as_list_view::<i64>().value(row),
                DataType::FixedSizeList(_, _) => array.as_fixed_size_list().value(row),
                DataType::Map(_, _) => Arc::new(array.as_map().value(row)),
                _ => return Err(parquet_budget_error()),
            };
            for child_row in 0..values.len() {
                charge_parquet_value(values.as_ref(), child_row, depth + 1, remaining)?;
            }
            return Ok(());
        }
        DataType::Boolean | DataType::Null => 1,
        other => other.primitive_width().ok_or_else(parquet_budget_error)?,
    };
    *remaining = remaining.checked_sub(bytes).ok_or_else(parquet_budget_error)?;
    Ok(())
}

fn parquet_access_plan(
    input: &SelectObjectContentInput,
    object_size: u64,
    metadata: &ParquetMetaData,
) -> QueryResult<Option<Arc<ParquetAccessPlan>>> {
    let Some(scan_range) = input.request.scan_range.as_ref() else {
        return Ok(None);
    };
    let scan_range = scan_range_from_bounds(scan_range.start, scan_range.end, object_size)
        .map_err(datafusion::common::DataFusionError::from)?;
    Ok(scan_range.map(|range| Arc::new(access_plan_for_scan_range(range, metadata))))
}

fn access_plan_for_scan_range(scan_range: SelectScanRange, metadata: &ParquetMetaData) -> ParquetAccessPlan {
    let mut access_plan = ParquetAccessPlan::new_none(metadata.num_row_groups());
    for (idx, row_group) in metadata.row_groups().iter().enumerate() {
        // S3 Select processes a parquet row group when its on-disk start offset
        // falls inside the requested scan range.
        if let Some(start) = row_group_start_offset(row_group) {
            if start >= scan_range.start() && start <= scan_range.end() {
                access_plan.scan(idx);
            }
        } else {
            // If row-group start offset is unavailable, keep existing behavior and
            // scan conservatively.
            access_plan.scan(idx);
        }
    }
    access_plan
}

fn row_group_start_offset(row_group: &RowGroupMetaData) -> Option<u64> {
    row_group.file_offset().and_then(non_negative_offset)
}

fn non_negative_offset(offset: i64) -> Option<u64> {
    u64::try_from(offset).ok()
}

fn parquet_store_error(err: ObjectStoreError) -> ParquetError {
    ParquetError::External(Box::new(err))
}

#[cfg(test)]
pub(super) fn dictionary_parquet_for_test(value_bytes: usize, rows: usize) -> Vec<u8> {
    tests::dictionary_file(value_bytes, rows, false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::{
        arrow::{
            array::Int32Array,
            datatypes::{DataType, Field, Schema, SchemaRef},
            record_batch::RecordBatch,
        },
        object_store::memory::InMemory,
        parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
        prelude::SessionContext,
    };
    use rustfs_s3select_api::SelectError;
    use s3s::dto::{
        CSVOutput, ExpressionType, InputSerialization, OutputSerialization, ParquetInput, ScanRange, SelectObjectContentRequest,
    };
    use std::{
        fs::File,
        sync::Arc,
        time::{SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn access_plan_selects_row_group_by_row_group_start() {
        let metadata = two_row_group_metadata();
        let first_start = row_group_start_offset(&metadata.row_groups()[0]).expect("first row group should have start offset");
        let second_start = row_group_start_offset(&metadata.row_groups()[1]).expect("second row group should have start offset");

        let first_start_plan = access_plan_for_scan_range(SelectScanRange::new(first_start, first_start), metadata.as_ref());
        assert!(first_start_plan.should_scan(0));
        let second_start_plan = access_plan_for_scan_range(SelectScanRange::new(second_start, second_start), metadata.as_ref());
        assert!(second_start_plan.should_scan(1));
        if first_start != second_start {
            assert!(!first_start_plan.should_scan(1));
            assert!(!second_start_plan.should_scan(0));
        }

        let ((lower_start, lower_idx), (higher_start, higher_idx)) = if first_start <= second_start {
            ((first_start, 0usize), (second_start, 1usize))
        } else {
            ((second_start, 1usize), (first_start, 0usize))
        };

        if let Some(before_higher) = higher_start.checked_sub(1)
            && lower_start <= before_higher
        {
            let boundary_plan = access_plan_for_scan_range(SelectScanRange::new(lower_start, before_higher), metadata.as_ref());
            assert!(boundary_plan.should_scan(lower_idx));
            assert!(!boundary_plan.should_scan(higher_idx));
        }
    }

    #[test]
    fn access_plan_uses_row_group_start_not_column_span_overlap() {
        let metadata = synthetic_overlap_metadata();
        // Range ends inside the first row group byte span, but should only include
        // the row group whose start offset is within the requested range.
        let plan = access_plan_for_scan_range(SelectScanRange::new(100, 190), metadata.as_ref());
        assert!(plan.should_scan(0));
        assert!(!plan.should_scan(1));
    }

    #[test]
    fn parquet_access_plan_has_typed_invalid_scan_range_error() {
        let metadata = two_row_group_metadata();
        let mut input = parquet_input("test.parquet");
        input.request.scan_range = Some(ScanRange {
            start: Some(10),
            end: None,
        });

        let error = parquet_access_plan(&input, 10, metadata.as_ref()).expect_err("out-of-bounds range must fail");

        assert_eq!(error.select_error(), SelectError::InvalidScanRange);
    }

    #[tokio::test]
    async fn try_new_preserves_missing_object_error() {
        let store = Arc::new(InMemory::new());
        let context = parquet_session(store);
        let state = context.state();

        let error = match ParquetSelectTable::try_new(&state, &parquet_input("missing.parquet")).await {
            Ok(_) => panic!("missing parquet object must fail"),
            Err(error) => error,
        };

        assert_eq!(error.select_error(), SelectError::ObjectNotFound);
    }

    #[tokio::test]
    async fn try_new_preserves_parquet_metadata_error() {
        let store = Arc::new(InMemory::new());
        let object = Path::from("corrupt.parquet");
        store
            .put(&object, Bytes::from_static(b"not a parquet file").into())
            .await
            .expect("put corrupt parquet object");
        let context = parquet_session(store);
        let state = context.state();

        let error = match ParquetSelectTable::try_new(&state, &parquet_input(object.as_ref())).await {
            Ok(_) => panic!("corrupt parquet metadata must fail"),
            Err(error) => error,
        };

        assert_eq!(error.select_error(), SelectError::ParquetParsingError);
    }

    pub(super) fn dictionary_file(value_bytes: usize, rows: usize, compressed: bool) -> Vec<u8> {
        use datafusion::arrow::array::{DictionaryArray, StringArray};
        use datafusion::arrow::datatypes::Int32Type;
        use datafusion::parquet::{
            arrow::arrow_writer::ArrowWriterOptions, basic::Compression, file::properties::WriterProperties,
        };
        let values = Arc::new(StringArray::from(vec!["x".repeat(value_bytes)]));
        let keys = Int32Array::from(vec![0; rows]);
        let column = DictionaryArray::<Int32Type>::try_new(keys, values).expect("dictionary");
        let schema = Arc::new(Schema::new(vec![Field::new("v", column.data_type().clone(), true)]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(column)]).expect("dictionary batch");
        let properties = WriterProperties::builder()
            .set_dictionary_enabled(true)
            .set_dictionary_page_size_limit(value_bytes * 2)
            .set_compression(if compressed {
                Compression::SNAPPY
            } else {
                Compression::UNCOMPRESSED
            })
            .build();
        let options = ArrowWriterOptions::new()
            .with_properties(properties)
            .with_skip_arrow_metadata(true);
        let mut bytes = Vec::new();
        let mut writer = ArrowWriter::try_new_with_options(&mut bytes, schema, options).expect("writer");
        writer.write(&batch).expect("dictionary data");
        writer.close().expect("footer");
        bytes
    }

    async fn register_parquet_fixture(context: &SessionContext, store: &Arc<InMemory>, bytes: Vec<u8>) {
        store
            .put(&Path::from("dictionary.parquet"), Bytes::from(bytes).into())
            .await
            .expect("fixture");
        let table = ParquetSelectTable::try_new(&context.state(), &parquet_input("dictionary.parquet"))
            .await
            .expect("valid dictionary metadata");
        context.register_table("source", table).expect("table");
    }

    #[tokio::test]
    async fn dictionary_byte_array_uses_shared_views_and_single_row_casts_after_filter() {
        let store = Arc::new(InMemory::new());
        let context = parquet_session(store.clone());
        let bytes = dictionary_file(1024 * 1024, 8192, false);
        assert!(bytes.len() < 2 * 1024 * 1024, "legal small dictionary file");
        register_parquet_fixture(&context, &store, bytes).await;
        let mut rows = context
            .sql("SELECT v FROM source LIMIT 2")
            .await
            .expect("plan")
            .execute_stream()
            .await
            .expect("stream");
        let first = rows.try_next().await.expect("decode").expect("first row");
        let second = rows.try_next().await.expect("decode").expect("second row");
        assert_eq!(first.num_rows(), 1);
        assert_eq!(second.num_rows(), 1);
        assert_eq!(first.column(0).data_type(), &DataType::Utf8View);
        assert!(first.get_array_memory_size() < 2 * 1024 * 1024, "no 8192-fold dictionary expansion");
        let first_views = first.column(0).as_string_view();
        let second_views = second.column(0).as_string_view();
        assert!(
            first_views.data_buffers()[0].ptr_eq(&second_views.data_buffers()[0]),
            "rows share the dictionary bytes"
        );
        drop(rows);

        // BYTEA requests a materializing Binary cast. FilterExec must not
        // rebuild an 8192-row batch before evaluating that cast.
        let frame = context
            .sql("SELECT CAST(v AS BYTEA) FROM source WHERE length(v) > 1 LIMIT 2")
            .await
            .expect("cast plan");
        let physical = frame.clone().create_physical_plan().await.expect("physical plan");
        let display = datafusion::physical_plan::displayable(physical.as_ref())
            .indent(false)
            .to_string();
        assert!(display.contains("FilterExec"), "{display}");
        assert!(!display.contains("CoalesceBatchesExec"), "{display}");
        let mut rows = frame.execute_stream().await.expect("cast stream");
        for _ in 0..2 {
            let batch = rows.try_next().await.expect("bounded cast").expect("cast row");
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(batch.column(0).data_type(), &DataType::Binary);
            assert_eq!(batch.column(0).as_binary::<i32>().value(0).len(), 1024 * 1024);
            assert!(batch.get_array_memory_size() < 2 * 1024 * 1024);
        }
        let frame = context
            .table("source")
            .await
            .expect("table")
            .filter(datafusion::prelude::col("v").is_not_null())
            .expect("filter")
            .select(vec![datafusion::logical_expr::cast(
                datafusion::prelude::col("v"),
                DataType::LargeUtf8,
            )])
            .expect("large string cast")
            .limit(0, Some(2))
            .expect("limit");
        let mut rows = frame.execute_stream().await.expect("LargeUtf8 stream");
        for _ in 0..2 {
            let batch = rows.try_next().await.expect("bounded large string cast").expect("row");
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(batch.column(0).data_type(), &DataType::LargeUtf8);
            assert!(batch.get_array_memory_size() < 2 * 1024 * 1024);
        }
    }

    #[tokio::test]
    async fn parquet_actual_expanded_single_row_is_rejected_before_cast() {
        let store = Arc::new(InMemory::new());
        let context = parquet_session(store.clone());
        // Compression keeps the stored input small. Valid footer declarations
        // fit the decode allowance, but this one expanded row exceeds 4 MiB.
        register_parquet_fixture(&context, &store, dictionary_file(5 * 1024 * 1024, 2, true)).await;
        let mut stream = context
            .sql("SELECT CAST(v AS BYTEA) FROM source")
            .await
            .expect("plan")
            .execute_stream()
            .await
            .expect("stream");
        let error = stream.try_next().await.expect_err("reject before materializing the cast");
        assert!(error.to_string().contains("query memory budget"));
    }

    #[tokio::test]
    async fn parquet_many_small_row_groups_can_exceed_the_query_budget_in_total() {
        use datafusion::arrow::array::StringArray;
        use datafusion::parquet::{
            arrow::arrow_writer::ArrowWriterOptions, basic::Compression, file::properties::WriterProperties,
        };
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, false)]));
        let column = Arc::new(StringArray::from(vec!["x".repeat(64 * 1024)]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![column]).expect("small row group");
        let properties = WriterProperties::builder().set_compression(Compression::UNCOMPRESSED).build();
        let options = ArrowWriterOptions::new()
            .with_properties(properties)
            .with_skip_arrow_metadata(true);
        let mut bytes = Vec::new();
        let mut writer = ArrowWriter::try_new_with_options(&mut bytes, schema, options).expect("writer");
        for _ in 0..80 {
            writer.write(&batch).expect("row");
            writer.flush().expect("separate row group");
        }
        writer.close().expect("footer");
        assert!(bytes.len() > 4 * 1024 * 1024);
        let store = Arc::new(InMemory::new());
        let context = parquet_session_with_memory(store.clone(), 4 * 1024 * 1024);
        register_parquet_fixture(&context, &store, bytes).await;
        let mut rows = context
            .sql("SELECT length(v) FROM source")
            .await
            .expect("plan")
            .execute_stream()
            .await
            .expect("stream");
        let mut count = 0;
        while let Some(batch) = rows.try_next().await.expect("serial row group decode") {
            assert_eq!(batch.num_rows(), 1);
            count += batch.num_rows();
        }
        assert_eq!(count, 80);
    }

    #[tokio::test]
    async fn parquet_scan_range_admits_only_the_selected_row_groups() {
        use datafusion::arrow::array::StringArray;
        use datafusion::parquet::{
            arrow::arrow_writer::ArrowWriterOptions, basic::Compression, file::properties::WriterProperties,
        };
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, false)]));
        let properties = WriterProperties::builder().set_compression(Compression::SNAPPY).build();
        let options = ArrowWriterOptions::new()
            .with_properties(properties)
            .with_skip_arrow_metadata(true);
        let mut bytes = Vec::new();
        let mut writer = ArrowWriter::try_new_with_options(&mut bytes, Arc::clone(&schema), options).expect("writer");
        for value in ["selected".to_string(), "x".repeat(2 * 1024 * 1024)] {
            let column = Arc::new(StringArray::from(vec![value]));
            let batch = RecordBatch::try_new(Arc::clone(&schema), vec![column]).expect("row");
            writer.write(&batch).expect("write row group");
            writer.flush().expect("separate row groups");
        }
        writer.close().expect("footer");
        let bytes = Bytes::from(bytes);
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes.clone()).expect("metadata");
        let starts = builder
            .metadata()
            .row_groups()
            .iter()
            .map(|group| i64::try_from(row_group_start_offset(group).expect("row group start")).expect("offset"))
            .collect::<Vec<_>>();
        assert_eq!(starts.len(), 2);
        assert_ne!(starts[0], starts[1]);
        let store = Arc::new(InMemory::new());
        store.put(&Path::from("range.parquet"), bytes.into()).await.expect("fixture");
        let context = parquet_session_with_memory(store, 4 * 1024 * 1024);
        let mut input = parquet_input("range.parquet");
        input.request.scan_range = Some(ScanRange {
            start: Some(starts[0]),
            end: Some(starts[0]),
        });
        let table = ParquetSelectTable::try_new(&context.state(), &input)
            .await
            .expect("an unselected large row group must not reject the small range");
        context.register_table("source", table).expect("table");
        let rows = context
            .sql("SELECT v FROM source")
            .await
            .expect("query")
            .collect()
            .await
            .expect("decode only the selected row group");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].num_rows(), 1);
        assert_eq!(rows[0].column(0).as_string_view().value(0), "selected");
        input.request.scan_range = Some(ScanRange {
            start: Some(starts[1]),
            end: Some(starts[1]),
        });
        let error = match ParquetSelectTable::try_new(&context.state(), &input).await {
            Ok(_) => panic!("the selected oversized row group must still be rejected before decoding"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("query memory budget"));
    }

    #[tokio::test]
    async fn parquet_actual_row_allowance_tracks_the_configured_query_budget() {
        let store = Arc::new(InMemory::new());
        let context = parquet_session_with_memory(store.clone(), 4 * 1024 * 1024);
        register_parquet_fixture(&context, &store, dictionary_file(512 * 1024, 2, true)).await;
        let mut rows = context
            .sql("SELECT v FROM source")
            .await
            .expect("plan")
            .execute_stream()
            .await
            .expect("stream");
        let error = rows
            .try_next()
            .await
            .expect_err("512 KiB row exceeds the configured 256 KiB allowance");
        assert!(error.to_string().contains("query memory budget"));
    }

    #[tokio::test]
    async fn parquet_valid_compressed_dictionary_is_rejected_before_decoder_allocation() {
        let store = Arc::new(InMemory::new());
        let context = parquet_session(store.clone());
        let bytes = dictionary_file(20 * 1024 * 1024, 1, true);
        assert!(bytes.len() < 2 * 1024 * 1024);
        store
            .put(&Path::from("large.parquet"), Bytes::from(bytes).into())
            .await
            .expect("fixture");
        let error = match ParquetSelectTable::try_new(&context.state(), &parquet_input("large.parquet")).await {
            Ok(_) => panic!("valid declared decoded size must be admitted before page decode"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("query memory budget"));
    }

    #[tokio::test]
    async fn parquet_nested_views_decode_through_the_actual_file_schema() {
        use datafusion::arrow::array::{ListArray, StringArray};
        use datafusion::arrow::buffer::OffsetBuffer;
        use datafusion::parquet::arrow::arrow_writer::ArrowWriterOptions;
        let store = Arc::new(InMemory::new());
        let context = parquet_session(store.clone());
        let child = Arc::new(Field::new("item", DataType::Utf8, true));
        let values = Arc::new(StringArray::from(vec![Some("first value beyond inline size"), None, Some("last")]));
        let list = Arc::new(ListArray::new(child, OffsetBuffer::new(vec![0, 2, 3].into()), values, None));
        let schema = Arc::new(Schema::new(vec![Field::new("nested", list.data_type().clone(), false)]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![list]).expect("nested rows");
        let mut bytes = Vec::new();
        let mut writer =
            ArrowWriter::try_new_with_options(&mut bytes, schema, ArrowWriterOptions::new().with_skip_arrow_metadata(true))
                .expect("nested writer");
        writer.write(&batch).expect("rows");
        writer.close().expect("footer");
        register_parquet_fixture(&context, &store, bytes).await;
        let mut rows = context
            .sql("SELECT nested FROM source")
            .await
            .expect("plan")
            .execute_stream()
            .await
            .expect("stream");
        let first = rows.try_next().await.expect("decode").expect("first row");
        let values = first.column(0).as_list::<i32>().value(0);
        assert_eq!(values.data_type(), &DataType::Utf8View);
        assert_eq!(values.as_string_view().value(0), "first value beyond inline size");
        assert!(values.is_null(1));
        let second = rows.try_next().await.expect("decode").expect("second row");
        assert_eq!(second.column(0).as_list::<i32>().value(0).as_string_view().value(0), "last");
        assert!(rows.try_next().await.expect("EOF").is_none());
    }

    #[test]
    fn parquet_nested_view_schema_and_expansion_follow_logical_references() {
        use datafusion::arrow::array::{ListArray, StringViewArray};
        use datafusion::arrow::buffer::OffsetBuffer;
        let child = Arc::new(
            Field::new("item", DataType::Utf8, true)
                .with_metadata(std::collections::HashMap::from([("custom".to_owned(), "kept".to_owned())])),
        );
        let field = Arc::new(Field::new("nested", DataType::List(child), false));
        let converted = parquet_view_field(&field, 0).expect("recursive views");
        let DataType::List(child) = converted.data_type() else { panic!("list preserved") };
        assert_eq!(child.data_type(), &DataType::Utf8View);
        assert_eq!(child.metadata().get("custom").map(String::as_str), Some("kept"));
        let values = Arc::new(StringViewArray::from(vec!["long repeated dictionary value"; 4]));
        let list = ListArray::new(Arc::clone(child), OffsetBuffer::new(vec![0, 4].into()), values, None);
        let mut budget = 64;
        assert!(
            charge_parquet_value(&list, 0, 0, &mut budget).is_err(),
            "nested references count separately"
        );
        let mut budget = 1024;
        charge_parquet_value(&list, 0, 0, &mut budget).expect("small nested row");
    }

    fn parquet_session(store: Arc<dyn ObjectStore>) -> SessionContext {
        parquet_session_with_memory(store, 64 * 1024 * 1024)
    }

    fn parquet_session_with_memory(store: Arc<dyn ObjectStore>, budget: usize) -> SessionContext {
        let mut config = datafusion::prelude::SessionConfig::new()
            .with_batch_size(1)
            .with_target_partitions(1);
        config.options_mut().execution.coalesce_batches = false;
        let runtime = datafusion::execution::runtime_env::RuntimeEnvBuilder::new()
            .with_memory_limit(budget, 1.0)
            .build()
            .expect("query memory pool");
        let context = SessionContext::new_with_config_rt(config, Arc::new(runtime));
        let store_url = ObjectStoreUrl::parse("s3://test-bucket").expect("valid test object store URL");
        context.register_object_store(store_url.as_ref(), store);
        context
    }

    fn parquet_input(key: &str) -> SelectObjectContentInput {
        SelectObjectContentInput {
            bucket: "test-bucket".to_string(),
            expected_bucket_owner: None,
            key: key.to_string(),
            sse_customer_algorithm: None,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            request: SelectObjectContentRequest {
                expression: "SELECT * FROM S3Object".to_string(),
                expression_type: ExpressionType::from_static(ExpressionType::SQL),
                input_serialization: InputSerialization {
                    parquet: Some(ParquetInput::default()),
                    ..Default::default()
                },
                output_serialization: OutputSerialization {
                    csv: Some(CSVOutput::default()),
                    ..Default::default()
                },
                request_progress: None,
                scan_range: None,
            },
        }
    }

    fn two_row_group_metadata() -> Arc<ParquetMetaData> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("rustfs_s3select_parquet_scan_range_{now}.parquet"));
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        {
            let file = File::create(&path).expect("create parquet test file");
            let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None).expect("create parquet writer");
            writer
                .write(&single_i32_batch(Arc::clone(&schema), 1))
                .expect("write first row group");
            writer.flush().expect("flush first row group");
            writer
                .write(&single_i32_batch(Arc::clone(&schema), 2))
                .expect("write second row group");
            writer.close().expect("close parquet writer");
        }

        let file = File::open(&path).expect("open parquet test file");
        let metadata = ParquetRecordBatchReaderBuilder::try_new(file)
            .expect("read parquet metadata")
            .metadata()
            .clone();
        std::fs::remove_file(&path).expect("remove parquet test file");
        metadata
    }

    fn single_i32_batch(schema: SchemaRef, value: i32) -> RecordBatch {
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![value]))]).expect("create test record batch")
    }

    fn synthetic_overlap_metadata() -> Arc<ParquetMetaData> {
        let metadata = two_row_group_metadata();
        let mut metadata_builder = metadata.as_ref().clone().into_builder();
        let mut row_groups = metadata_builder.take_row_groups();
        assert_eq!(row_groups.len(), 2, "test metadata should contain two row groups");

        let first = row_group_with_offsets(row_groups.remove(0), 100, 100, 250);
        let second = row_group_with_offsets(row_groups.remove(0), 500, 160, 90);
        let row_groups = vec![first, second];

        metadata_builder = metadata_builder.set_row_groups(row_groups);
        Arc::new(metadata_builder.build())
    }

    fn row_group_with_offsets(
        row_group: RowGroupMetaData,
        file_offset: i64,
        column_offset: i64,
        column_len: i64,
    ) -> RowGroupMetaData {
        let mut builder = row_group.into_builder().set_file_offset(file_offset);
        let columns = builder
            .take_columns()
            .into_iter()
            .map(|column| {
                column
                    .into_builder()
                    .set_data_page_offset(column_offset)
                    .set_total_compressed_size(column_len)
                    .build()
                    .expect("rewrite test column metadata")
            })
            .collect::<Vec<_>>();
        builder
            .set_column_metadata(columns)
            .build()
            .expect("rewrite test row-group metadata")
    }
}
