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
    arrow::datatypes::SchemaRef,
    catalog::Session,
    common::Result as DFResult,
    datasource::{
        TableProvider,
        listing::{ListingTableUrl, PartitionedFile},
        physical_plan::{FileScanConfigBuilder, ParquetSource, parquet::ParquetAccessPlan},
        source::DataSourceExec,
    },
    execution::object_store::ObjectStoreUrl,
    logical_expr::{Expr, TableProviderFilterPushDown, TableType},
    object_store::{Error as ObjectStoreError, ObjectStore, ObjectStoreExt, path::Path},
    parquet::{
        arrow::{ParquetRecordBatchStreamBuilder, arrow_reader::ArrowReaderOptions, async_reader::AsyncFileReader},
        errors::{ParquetError, Result as ParquetResult},
        file::metadata::{ParquetMetaData, ParquetMetaDataReader, RowGroupMetaData},
    },
    physical_plan::ExecutionPlan,
};
use futures::{FutureExt, TryFutureExt, future::BoxFuture};
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
}

impl AsyncFileReader for ObjectStoreParquetReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        self.store.get_range(&self.path, range).map_err(parquet_store_error).boxed()
    }

    fn get_byte_ranges(&mut self, ranges: Vec<Range<u64>>) -> BoxFuture<'_, ParquetResult<Vec<Bytes>>> {
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

        let reader = ObjectStoreParquetReader {
            store: Arc::clone(&store),
            path: object_location,
            file_size: object_meta.size,
        };
        let builder = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .map_err(datafusion::common::DataFusionError::from)?;
        let schema = Arc::clone(builder.schema());
        let metadata = Arc::clone(builder.metadata());
        let access_plan = parquet_access_plan(input, object_meta.size, metadata.as_ref())?;

        Ok(Arc::new(Self {
            schema,
            object_store_url,
            object_path: input.key.clone(),
            object_size: object_meta.size,
            access_plan,
        }))
    }

    fn partitioned_file(&self) -> PartitionedFile {
        let file = PartitionedFile::new(self.object_path.clone(), self.object_size);
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
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let scan_limit = if filters.is_empty() { limit } else { None };
        let file_source = Arc::new(ParquetSource::new(Arc::clone(&self.schema)));
        let config = FileScanConfigBuilder::new(self.object_store_url.clone(), file_source)
            .with_file(self.partitioned_file())
            .with_projection_indices(projection.cloned())?
            .with_limit(scan_limit)
            .build();
        let plan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(config);
        Ok(plan)
    }

    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
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

    fn parquet_session(store: Arc<dyn ObjectStore>) -> SessionContext {
        let context = SessionContext::new();
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
