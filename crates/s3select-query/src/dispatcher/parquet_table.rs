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

use std::{any::Any, fmt, sync::Arc};

use async_trait::async_trait;
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
    object_store::{ObjectStoreExt, path::Path},
    parquet::{
        arrow::{ParquetRecordBatchStreamBuilder, async_reader::ParquetObjectReader},
        file::metadata::{ColumnChunkMetaData, ParquetMetaData, RowGroupMetaData},
    },
    physical_plan::ExecutionPlan,
};
use rustfs_s3select_api::{
    QueryError, QueryResult,
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

impl ParquetSelectTable {
    pub async fn try_new(state: &dyn Session, input: &SelectObjectContentInput) -> QueryResult<Arc<dyn TableProvider>> {
        let table_path = ListingTableUrl::parse(format!("s3://{}/{}", input.bucket, input.key))?;
        let object_store_url = table_path.object_store();
        let object_location = Path::from(input.key.clone());
        let store = state.runtime_env().object_store(&object_store_url)?;
        let object_meta = store.head(&object_location).await.map_err(query_store_error)?;

        let reader = ParquetObjectReader::new(Arc::clone(&store), object_location).with_file_size(object_meta.size);
        let builder = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .map_err(query_store_error)?;
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
            let extensions: Arc<dyn Any + Send + Sync> = access_plan.clone();
            file.with_extensions(extensions)
        } else {
            file
        }
    }
}

#[async_trait]
impl TableProvider for ParquetSelectTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

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
    let scan_range = scan_range_from_bounds(scan_range.start, scan_range.end, object_size).map_err(query_store_error)?;
    Ok(scan_range.map(|range| Arc::new(access_plan_for_scan_range(range, metadata))))
}

fn access_plan_for_scan_range(scan_range: SelectScanRange, metadata: &ParquetMetaData) -> ParquetAccessPlan {
    let mut access_plan = ParquetAccessPlan::new_none(metadata.num_row_groups());
    for (idx, row_group) in metadata.row_groups().iter().enumerate() {
        // S3 Select parquet ScanRange semantics apply to byte ranges; include any
        // row group whose on-disk byte span overlaps the requested range.
        if let Some((start, end)) = row_group_byte_range(row_group) {
            if spans_overlap(start, end, scan_range.start(), scan_range.end()) {
                access_plan.scan(idx);
            }
        } else {
            // If row-group byte span is unavailable, fall back to current behavior
            // and keep compatibility by scanning conservatively.
            access_plan.scan(idx);
        }
    }
    access_plan
}

fn row_group_byte_range(row_group: &RowGroupMetaData) -> Option<(u64, u64)> {
    let mut start: Option<u64> = None;
    let mut end: Option<u64> = None;

    for column in row_group.columns() {
        if let Some((column_start, column_end)) = column_byte_range(column) {
            start = Some(start.map_or(column_start, |value| value.min(column_start)));
            end = Some(end.map_or(column_end, |value| value.max(column_end)));
        }
    }

    if let (Some(start), Some(end)) = (start, end) {
        return Some((start, end));
    }

    let fallback_start = row_group.file_offset().and_then(non_negative_offset);
    let fallback_len = non_negative_size(row_group.compressed_size());
    fallback_start.zip(fallback_len).map(|(start, len)| {
        let end = if len == 0 {
            start
        } else {
            start.saturating_add(len.saturating_sub(1))
        };
        (start, end)
    })
}

fn column_byte_range(column: &ColumnChunkMetaData) -> Option<(u64, u64)> {
    let start = column_start_offset(column)?;
    let size = non_negative_size(column.compressed_size())?;
    let end = if size == 0 {
        start
    } else {
        start.saturating_add(size.saturating_sub(1))
    };
    Some((start, end))
}

fn spans_overlap(range_start: u64, range_end: u64, scan_start: u64, scan_end: u64) -> bool {
    range_start <= scan_end && range_end >= scan_start
}

fn column_start_offset(column: &ColumnChunkMetaData) -> Option<u64> {
    [
        column.dictionary_page_offset().and_then(non_negative_offset),
        non_negative_offset(column.data_page_offset()),
    ]
    .into_iter()
    .flatten()
    .min()
}

fn non_negative_offset(offset: i64) -> Option<u64> {
    u64::try_from(offset).ok()
}

fn non_negative_size(size: i64) -> Option<u64> {
    u64::try_from(size).ok()
}

fn query_store_error(err: impl fmt::Display) -> QueryError {
    QueryError::StoreError { e: err.to_string() }
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
        parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    };
    use std::{
        fs::File,
        sync::Arc,
        time::{SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn access_plan_selects_row_group_by_overlapping_offsets() {
        let metadata = two_row_group_metadata();
        assert_eq!(metadata.num_row_groups(), 2);

        let ranges = metadata
            .row_groups()
            .iter()
            .map(row_group_byte_range)
            .collect::<Option<Vec<_>>>()
            .expect("test parquet row groups should have byte ranges");
        assert!(ranges[0].0 <= ranges[0].1);
        assert!(ranges[0].1 <= ranges[1].0);

        let (first_start, first_end) = ranges[0];
        let (second_start, second_end) = ranges[1];

        let overlap_scan = access_plan_for_scan_range(SelectScanRange::new(second_start, second_start), metadata.as_ref());
        assert!(!overlap_scan.should_scan(0));
        assert!(overlap_scan.should_scan(1));

        let overlap_inside_plan = access_plan_for_scan_range(
            SelectScanRange::new(second_start.saturating_add(1), second_start.saturating_add(1)),
            metadata.as_ref(),
        );
        if second_start < second_end {
            assert!(overlap_inside_plan.should_scan(1));
            assert!(!overlap_inside_plan.should_scan(0));
        }

        let fallback_plan = access_plan_for_scan_range(SelectScanRange::new(first_start, first_start), metadata.as_ref());
        assert!(fallback_plan.should_scan(0));

        let boundary_on_first_end = access_plan_for_scan_range(SelectScanRange::new(first_end, first_end), metadata.as_ref());
        assert!(boundary_on_first_end.should_scan(0));

        if first_end < second_start {
            assert!(!boundary_on_first_end.should_scan(1));
        }

        let boundary_after_first_end = access_plan_for_scan_range(
            SelectScanRange::new(first_end.saturating_add(1), first_end.saturating_add(1)),
            metadata.as_ref(),
        );
        if first_end < second_start {
            assert!(!boundary_after_first_end.should_scan(0));
            if second_start <= first_end.saturating_add(1) {
                assert!(boundary_after_first_end.should_scan(1));
            }
        }

        if second_start < second_end {
            let boundary_on_second_end =
                access_plan_for_scan_range(SelectScanRange::new(second_end, second_end), metadata.as_ref());
            assert!(boundary_on_second_end.should_scan(1));
            if first_end < second_end {
                assert!(!boundary_on_second_end.should_scan(0));
            }
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
}
