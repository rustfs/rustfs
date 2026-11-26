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

//! CSV and JSON Lines input decoded in byte-bounded, memory-pool-accounted batches.
//!
//! DataFusion's stock CSV and JSON listing sources buffer `batch_size` rows regardless
//! of record byte length or schema width and reserve nothing from the query memory pool.
//! This table preserves DataFusion's file-scan integration while driving Arrow's CSV
//! and JSON decoders directly: rows per batch are bounded by schema width, batches
//! flush once `DECODE_BATCH_BYTES` of complete records accumulate (or less under tight
//! memory limits), and buffered decoder memory is charged to the session memory pool.

use std::{
    fmt,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use async_trait::async_trait;
use bytes::{Buf as _, Bytes};
use datafusion::{
    arrow::{csv, datatypes::SchemaRef, error::ArrowError, json, record_batch::RecordBatch},
    catalog::Session,
    common::{DataFusionError, Result as DFResult, config::CsvOptions, tree_node::TreeNodeRecursion},
    datasource::{
        TableProvider,
        listing::PartitionedFile,
        physical_plan::{FileOpenFuture, FileOpener, FileScanConfig, FileScanConfigBuilder, FileSource},
        projection::{ProjectionOpener, SplitProjection},
        source::DataSourceExec,
        table_schema::TableSchema,
    },
    execution::{
        memory_pool::{MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation},
        object_store::ObjectStoreUrl,
    },
    logical_expr::{Expr, TableType},
    object_store::{GetOptions, ObjectStore, ObjectStoreExt as _, path::Path},
    physical_expr::projection::ProjectionExprs,
    physical_plan::{DisplayFormatType, ExecutionPlan, PhysicalExpr, apply_expression_roots, metrics::ExecutionPlanMetricsSet},
};
use futures::{Stream, StreamExt as _, TryStreamExt as _, stream::BoxStream};
use rustfs_s3select_api::QueryResult;

use super::json_document_table::json_decoder_layout;

/// Input bytes of complete records after which a batch is flushed. Records are
/// capped at 1 MiB upstream, so a batch holds at most this plus one record.
const DECODE_BATCH_BYTES: usize = 1024 * 1024;
/// Maximum input chunk fed to a decoder before re-evaluating flush and memory state.
const DECODE_STEP_BYTES: usize = 64 * 1024;
/// Pool charge per input byte held by a decoder: unescaped bytes or JSON tape,
/// offsets, materialized columns, and builder growth. A batch of conformant 1 MiB
/// records peaks around 32 MiB, half the default 64 MiB query limit.
const DECODE_MEMORY_MULTIPLIER: usize = 16;
/// Held input is charged in steps of this size to avoid per-line reservation churn.
const DECODE_RESERVATION_STEP_BYTES: usize = 4 * 1024;
/// Arrow's CSV decoder zero-fills a `usize` offset and 8 data bytes for every
/// field of a batch before reading the first record.
const CSV_DECODER_BYTES_PER_FIELD: usize = 2 * std::mem::size_of::<usize>();
const CSV_DECODER_BUFFER_TARGET_BYTES: usize = 4 * 1024 * 1024;
const CSV_DECODER_PER_COLUMN_METADATA_BYTES: usize = 256;

/// The record syntax of a CSV or JSON Lines object.
#[derive(Clone, Debug)]
pub(super) enum DelimitedFormat {
    Csv(Box<CsvOptions>),
    JsonLines,
}

pub(super) struct DelimitedSelectTable {
    schema: SchemaRef,
    object_store_url: ObjectStoreUrl,
    object_path: String,
    object_size: u64,
    format: DelimitedFormat,
    memory_pool: Arc<dyn MemoryPool>,
}

impl fmt::Debug for DelimitedSelectTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DelimitedSelectTable")
            .field("object_store_url", &self.object_store_url)
            .field("object_path", &self.object_path)
            .field("object_size", &self.object_size)
            .field("format", &self.format)
            .finish_non_exhaustive()
    }
}

impl DelimitedSelectTable {
    pub(super) async fn try_new(
        state: &dyn Session,
        object_store_url: ObjectStoreUrl,
        key: &str,
        schema: SchemaRef,
        format: DelimitedFormat,
    ) -> QueryResult<Arc<dyn TableProvider>> {
        let store = state.runtime_env().object_store(&object_store_url)?;
        let object_meta = store.head(&Path::from(key)).await.map_err(DataFusionError::from)?;
        Ok(Arc::new(Self {
            schema,
            object_store_url,
            object_path: key.to_string(),
            object_size: object_meta.size,
            format,
            memory_pool: Arc::clone(&state.runtime_env().memory_pool),
        }))
    }
}

#[async_trait]
impl TableProvider for DelimitedSelectTable {
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
        let file_source = Arc::new(DelimitedSource::new(
            Arc::clone(&self.schema),
            self.format.clone(),
            Arc::clone(&self.memory_pool),
        ));
        let config = FileScanConfigBuilder::new(self.object_store_url.clone(), file_source)
            .with_file(PartitionedFile::new(self.object_path.clone(), self.object_size))
            .with_projection_indices(projection.cloned())?
            .with_limit(scan_limit)
            .build();
        let plan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(config);
        Ok(plan)
    }
}

/// A [`FileSource`] shaped like DataFusion's `CsvSource` and `JsonSource`,
/// except that its opener decodes through [`DelimitedBatchStream`].
#[derive(Clone)]
struct DelimitedSource {
    format: DelimitedFormat,
    table_schema: TableSchema,
    projection: SplitProjection,
    batch_size: Option<usize>,
    metrics: ExecutionPlanMetricsSet,
    memory_pool: Arc<dyn MemoryPool>,
}

impl DelimitedSource {
    fn new(schema: SchemaRef, format: DelimitedFormat, memory_pool: Arc<dyn MemoryPool>) -> Self {
        let table_schema = TableSchema::from(schema);
        Self {
            format,
            projection: SplitProjection::unprojected(&table_schema),
            table_schema,
            batch_size: None,
            metrics: ExecutionPlanMetricsSet::new(),
            memory_pool,
        }
    }
}

impl FileSource for DelimitedSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        _base_config: &FileScanConfig,
        _partition: usize,
    ) -> DFResult<Arc<dyn FileOpener>> {
        let file_schema = self.table_schema.file_schema();
        let opener = Arc::new(DelimitedOpener {
            format: self.format.clone(),
            schema: Arc::clone(file_schema),
            projection: self.projection.file_indices.clone(),
            batch_size: self
                .batch_size
                .ok_or_else(|| DataFusionError::Internal("batch size must be set before opening S3 Select input".to_string()))?,
            object_store,
            memory_pool: Arc::clone(&self.memory_pool),
        }) as Arc<dyn FileOpener>;
        ProjectionOpener::try_new(self.projection.clone(), opener, file_schema)
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        let mut source = self.clone();
        source.batch_size = Some(batch_size);
        Arc::new(source)
    }

    fn try_pushdown_projection(&self, projection: &ProjectionExprs) -> DFResult<Option<Arc<dyn FileSource>>> {
        let mut source = self.clone();
        let projection = self.projection.source.try_merge(projection)?;
        source.projection = SplitProjection::new(self.table_schema.file_schema(), &projection);
        Ok(Some(Arc::new(source)))
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        Some(&self.projection.source)
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn file_type(&self) -> &str {
        match self.format {
            DelimitedFormat::Csv(_) => "csv",
            DelimitedFormat::JsonLines => "json",
        }
    }

    fn supports_repartitioning(&self) -> bool {
        // S3 Select reads one object through a single metered input stream.
        false
    }

    fn fmt_extra(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match (t, &self.format) {
            (DisplayFormatType::Default | DisplayFormatType::Verbose, DelimitedFormat::Csv(options)) => {
                write!(f, ", has_header={}", csv_has_header(options))
            }
            _ => Ok(()),
        }
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DFResult<TreeNodeRecursion>,
    ) -> DFResult<TreeNodeRecursion> {
        apply_expression_roots(self.projection.source.iter(), f)
    }
}

struct DelimitedOpener {
    format: DelimitedFormat,
    schema: SchemaRef,
    projection: Vec<usize>,
    batch_size: usize,
    object_store: Arc<dyn ObjectStore>,
    memory_pool: Arc<dyn MemoryPool>,
}

impl FileOpener for DelimitedOpener {
    fn open(&self, partitioned_file: PartitionedFile) -> DFResult<FileOpenFuture> {
        let store = Arc::clone(&self.object_store);
        let (decoder, batch_size, decoder_reservation) =
            RecordDecoder::try_new(&self.format, &self.schema, &self.projection, self.batch_size.max(1))?;
        let reservation = MemoryConsumer::new("S3 Select record decoder").register(&self.memory_pool);
        reservation.try_resize(decoder_reservation)?;
        // A tighter limit is met with smaller batches, leaving half of the budget
        // for the record that crosses the flush threshold.
        let batch_bytes = match self.memory_pool.memory_limit() {
            MemoryLimit::Finite(limit) => DECODE_BATCH_BYTES.min(limit / (2 * DECODE_MEMORY_MULTIPLIER)),
            MemoryLimit::Infinite | MemoryLimit::Unknown => DECODE_BATCH_BYTES,
        };
        Ok(Box::pin(async move {
            let result = store
                .get_opts(&partitioned_file.object_meta.location, GetOptions::default())
                .await?;
            let stream = DelimitedBatchStream {
                input: result.into_stream().map_err(DataFusionError::from).boxed(),
                current: Bytes::new(),
                decoder,
                reservation,
                decoder_reservation,
                batch_size,
                batch_bytes,
                row_bytes: 0,
                pending_bytes: 0,
                reserved_bytes: 0,
                input_finished: false,
                done: false,
            };
            Ok(stream.boxed())
        }))
    }
}

fn csv_has_header(options: &CsvOptions) -> bool {
    options.has_header.unwrap_or(true)
}

/// Mirrors what DataFusion's `CsvSource` passes to Arrow for these options.
fn csv_decoder(options: &CsvOptions, schema: SchemaRef, projection: &[usize], batch_size: usize) -> csv::reader::Decoder {
    let mut builder = csv::ReaderBuilder::new(schema)
        .with_delimiter(options.delimiter)
        .with_batch_size(batch_size)
        .with_header(csv_has_header(options))
        .with_quote(options.quote)
        .with_truncated_rows(options.truncated_rows.unwrap_or(false))
        .with_projection(projection.to_vec());
    if let Some(terminator) = options.terminator {
        builder = builder.with_terminator(terminator);
    }
    if let Some(escape) = options.escape {
        builder = builder.with_escape(escape);
    }
    if let Some(comment) = options.comment {
        builder = builder.with_comment(comment);
    }
    builder.build_decoder()
}

/// Bounds the rows of a CSV batch so the decoder's upfront buffers fit the
/// target for this schema width, and returns them with their pool charge.
fn csv_decoder_layout(columns: usize, requested_batch_size: usize) -> DFResult<(usize, usize)> {
    let bytes_per_row = columns
        .max(1)
        .checked_mul(CSV_DECODER_BYTES_PER_FIELD)
        .ok_or_else(decoder_resource_error)?;
    let metadata_bytes = columns
        .checked_mul(CSV_DECODER_PER_COLUMN_METADATA_BYTES)
        .ok_or_else(decoder_resource_error)?;
    let buffer_budget = CSV_DECODER_BUFFER_TARGET_BYTES.saturating_sub(metadata_bytes);
    let batch_size = requested_batch_size.min((buffer_budget / bytes_per_row).max(1));
    let reservation = bytes_per_row
        .checked_mul(batch_size)
        .and_then(|bytes| bytes.checked_add(metadata_bytes))
        .ok_or_else(decoder_resource_error)?;
    Ok((batch_size, reservation))
}

/// Where Arrow's CSV parser stands after the physical lines it has consumed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CsvPosition {
    /// Between records: the decoder holds complete records only.
    RecordStart,
    /// Inside a comment line that started between records.
    Comment,
    /// Possibly partway through a record.
    Record,
}

enum RecordDecoder {
    Csv {
        decoder: Box<csv::reader::Decoder>,
        /// `None` keeps Arrow's default, where `\r`, `\n`, and `\r\n` end a record.
        terminator: Option<u8>,
        comment: Option<u8>,
        quote: u8,
        escape: Option<u8>,
        header_pending: bool,
        in_header_quotes: bool,
        header_escaped: bool,
        position: CsvPosition,
        batch_size: usize,
    },
    Json(json::reader::Decoder),
}

impl RecordDecoder {
    /// Returns the decoder along with its bounded batch size and upfront pool charge.
    fn try_new(
        format: &DelimitedFormat,
        schema: &SchemaRef,
        projection: &[usize],
        requested_batch_size: usize,
    ) -> DFResult<(Self, usize, usize)> {
        match format {
            DelimitedFormat::Csv(options) => {
                let (batch_size, reservation) = csv_decoder_layout(schema.fields().len(), requested_batch_size)?;
                let decoder = Self::Csv {
                    decoder: Box::new(csv_decoder(options, Arc::clone(schema), projection, batch_size)),
                    terminator: options.terminator,
                    comment: options.comment,
                    quote: options.quote,
                    escape: options.escape,
                    header_pending: csv_has_header(options),
                    in_header_quotes: false,
                    header_escaped: false,
                    position: CsvPosition::RecordStart,
                    batch_size,
                };
                Ok((decoder, batch_size, reservation))
            }
            DelimitedFormat::JsonLines => {
                let schema = Arc::new(schema.project(projection)?);
                let (batch_size, reservation) = json_decoder_layout(&schema, requested_batch_size)?;
                let decoder = json::ReaderBuilder::new(schema).with_batch_size(batch_size).build_decoder()?;
                Ok((Self::Json(decoder), batch_size, reservation))
            }
        }
    }

    /// How many bytes of `buf` to feed next.
    ///
    /// Arrow's CSV decoder does not expose whether it is partway through a record,
    /// so it is fed one physical line at a time: Arrow completes a record only on
    /// an unquoted terminator, and each fed slice contains at most one terminator,
    /// so a slice that advances the row count leaves the decoder between records.
    /// The JSON decoder reports partial records directly and takes up to `bulk`
    /// bytes before feeding line by line toward a record boundary.
    fn next_input_len(&self, buf: &[u8], bulk: usize) -> usize {
        let buf = &buf[..buf.len().min(DECODE_STEP_BYTES)];
        let line_end = match self {
            Self::Csv {
                terminator: Some(terminator),
                ..
            } => buf.iter().position(|byte| *byte == *terminator),
            Self::Csv { terminator: None, .. } => buf.iter().position(|byte| matches!(*byte, b'\r' | b'\n')),
            Self::Json(_) if bulk > 0 => return buf.len().min(bulk),
            Self::Json(_) => buf.iter().position(|byte| *byte == b'\n'),
        };
        line_end.map_or(buf.len(), |end| end + 1)
    }

    fn decode(&mut self, buf: &[u8]) -> Result<usize, ArrowError> {
        match self {
            Self::Csv {
                decoder,
                terminator,
                comment,
                quote,
                escape,
                header_pending,
                in_header_quotes,
                header_escaped,
                position,
                ..
            } => {
                let capacity = decoder.capacity();
                let decoded = decoder.decode(buf)?;
                let line = &buf[..decoded];
                let Some(&last) = line.last() else {
                    return Ok(decoded);
                };
                let ends_line = decoded == buf.len()
                    && match terminator {
                        Some(terminator) => last == *terminator,
                        None => matches!(last, b'\r' | b'\n'),
                    };
                if decoder.capacity() < capacity {
                    *header_pending = false;
                    *in_header_quotes = false;
                    *header_escaped = false;
                    *position = CsvPosition::RecordStart;
                } else {
                    match *position {
                        // Arrow skips empty lines and, up to the next `\n`,
                        // lines starting with the comment byte.
                        CsvPosition::RecordStart if ends_line && line.len() == 1 => {
                            *position = CsvPosition::RecordStart;
                        }
                        CsvPosition::RecordStart if *comment == Some(line[0]) => {
                            *position = if ends_line && last == b'\n' {
                                CsvPosition::RecordStart
                            } else {
                                CsvPosition::Comment
                            };
                        }
                        CsvPosition::Comment => {
                            if ends_line && last == b'\n' {
                                *position = CsvPosition::RecordStart;
                            }
                        }
                        CsvPosition::RecordStart | CsvPosition::Record if *header_pending => {
                            // Track quote state across the header record so completing the
                            // header returns to `RecordStart` and subsequent blank/comment
                            // lines before the first data row are not charged as pending data.
                            let content = if ends_line { &line[..line.len() - 1] } else { line };
                            for &byte in content {
                                if *header_escaped {
                                    *header_escaped = false;
                                } else if *in_header_quotes && *escape == Some(byte) {
                                    *header_escaped = true;
                                } else if byte == *quote {
                                    *in_header_quotes = !*in_header_quotes;
                                }
                            }
                            if ends_line && !*in_header_quotes {
                                *header_pending = false;
                                *header_escaped = false;
                                *position = CsvPosition::RecordStart;
                            } else {
                                *position = CsvPosition::Record;
                            }
                        }
                        CsvPosition::RecordStart | CsvPosition::Record => {
                            *position = CsvPosition::Record;
                        }
                    }
                }
                Ok(decoded)
            }
            Self::Json(decoder) => decoder.decode(buf),
        }
    }

    fn flush(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        match self {
            Self::Csv { decoder, .. } => decoder.flush(),
            Self::Json(decoder) => decoder.flush(),
        }
    }

    /// Complete records buffered since the last flush.
    fn rows(&self) -> usize {
        match self {
            Self::Csv { decoder, batch_size, .. } => batch_size.saturating_sub(decoder.capacity()),
            Self::Json(decoder) => decoder.len().saturating_sub(usize::from(decoder.has_partial_record())),
        }
    }

    /// Whether the decoder is known to hold complete records only.
    fn at_record_boundary(&self) -> bool {
        match self {
            Self::Csv { position, .. } => *position == CsvPosition::RecordStart,
            Self::Json(decoder) => !decoder.has_partial_record(),
        }
    }
}

/// Decodes the object stream into batches bounded by rows, bytes, and the memory pool.
struct DelimitedBatchStream {
    input: BoxStream<'static, DFResult<Bytes>>,
    current: Bytes,
    decoder: RecordDecoder,
    reservation: MemoryReservation,
    decoder_reservation: usize,
    batch_size: usize,
    batch_bytes: usize,
    /// Input bytes belonging to complete records buffered since the last flush.
    row_bytes: usize,
    /// Input bytes fed since the decoder was last known to be between records.
    pending_bytes: usize,
    /// Held input bytes currently covered by the pool reservation.
    reserved_bytes: usize,
    input_finished: bool,
    done: bool,
}

impl DelimitedBatchStream {
    /// Charges the memory pool for `bytes` of additional input held by the decoder.
    fn reserve_input(&mut self, bytes: usize) -> DFResult<()> {
        let held = self
            .row_bytes
            .checked_add(self.pending_bytes)
            .and_then(|held| held.checked_add(bytes))
            .ok_or_else(decoder_resource_error)?;
        if held <= self.reserved_bytes {
            return Ok(());
        }
        let reserved = held
            .checked_next_multiple_of(DECODE_RESERVATION_STEP_BYTES)
            .ok_or_else(decoder_resource_error)?;
        let reservation = reserved
            .checked_mul(DECODE_MEMORY_MULTIPLIER)
            .and_then(|work| work.checked_add(self.decoder_reservation))
            .ok_or_else(decoder_resource_error)?;
        self.reservation.try_resize(reservation)?;
        self.reserved_bytes = reserved;
        Ok(())
    }

    fn flush(&mut self) -> DFResult<Option<RecordBatch>> {
        let batch = self.decoder.flush()?;
        self.row_bytes = 0;
        self.pending_bytes = 0;
        self.reserved_bytes = 0;
        self.reservation.try_resize(self.decoder_reservation)?;
        Ok(batch)
    }

    fn fail(&mut self, error: DataFusionError) -> Poll<Option<DFResult<RecordBatch>>> {
        self.done = true;
        self.current = Bytes::new();
        self.reservation.free();
        Poll::Ready(Some(Err(error)))
    }
}

impl Stream for DelimitedBatchStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let stream = self.as_mut().get_mut();
        loop {
            if stream.done {
                return Poll::Ready(None);
            }
            // Both conditions first hold right after a record completes, so the
            // decoder is between records whenever it is flushed.
            let rows = stream.decoder.rows();
            if rows >= stream.batch_size || (rows > 0 && stream.pending_bytes == 0 && stream.row_bytes >= stream.batch_bytes) {
                return match stream.flush() {
                    Ok(Some(batch)) => Poll::Ready(Some(Ok(batch))),
                    Ok(None) => stream.fail(DataFusionError::Execution(
                        "Arrow decoder reached a flush boundary without producing a batch".to_string(),
                    )),
                    Err(error) => stream.fail(error),
                };
            }
            if stream.current.is_empty() {
                if stream.input_finished {
                    stream.done = true;
                    // Arrow completes a final record without trailing terminator on empty input.
                    if let Err(error) = stream.decoder.decode(&[]) {
                        return stream.fail(DataFusionError::from(error));
                    }
                    return match stream.flush() {
                        Ok(batch) => Poll::Ready(batch.map(Ok)),
                        Err(error) => stream.fail(error),
                    };
                }
                match stream.input.poll_next_unpin(context) {
                    Poll::Ready(Some(Ok(bytes))) => stream.current = bytes,
                    Poll::Ready(Some(Err(error))) => return stream.fail(error),
                    Poll::Ready(None) => stream.input_finished = true,
                    Poll::Pending => return Poll::Pending,
                }
                continue;
            }

            let bulk = stream
                .batch_bytes
                .saturating_sub(stream.row_bytes.saturating_add(stream.pending_bytes));
            let available = stream.decoder.next_input_len(&stream.current, bulk);
            if let Err(error) = stream.reserve_input(available) {
                return stream.fail(error);
            }
            let decoded = match stream.decoder.decode(&stream.current[..available]) {
                Ok(decoded) => decoded,
                Err(error) => return stream.fail(DataFusionError::from(error)),
            };
            if decoded == 0 {
                return stream.fail(DataFusionError::Execution("Arrow decoder made no progress".to_string()));
            }
            stream.current.advance(decoded);
            stream.pending_bytes = stream.pending_bytes.saturating_add(decoded);
            if stream.decoder.at_record_boundary() {
                // Input that completed no record (header, comment, or blank line) holds no row memory.
                if stream.decoder.rows() > rows {
                    stream.row_bytes = stream.row_bytes.saturating_add(stream.pending_bytes);
                }
                stream.pending_bytes = 0;
            }
        }
    }
}

fn decoder_resource_error() -> DataFusionError {
    DataFusionError::ResourcesExhausted("S3 Select record decoder exceeds the query memory limit".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::{
        arrow::{
            array::{Array as _, Int64Array, StringArray},
            datatypes::{DataType, Field, Schema},
        },
        datasource::{
            file_format::csv::CsvFormat,
            listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
        },
        execution::runtime_env::RuntimeEnvBuilder,
        object_store::memory::InMemory,
        prelude::{SessionConfig, SessionContext},
    };

    const BUCKET: &str = "bucket";
    const MIB: usize = 1024 * 1024;

    fn session(memory_limit: usize) -> (SessionContext, Arc<InMemory>) {
        let store = Arc::new(InMemory::new());
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(memory_limit, 1.0)
            .build_arc()
            .expect("runtime");
        let context = SessionContext::new_with_config_rt(SessionConfig::new().with_target_partitions(1), runtime);
        let url = ObjectStoreUrl::parse(format!("s3://{BUCKET}")).expect("store URL");
        context.register_object_store(url.as_ref(), Arc::clone(&store) as Arc<dyn ObjectStore>);
        (context, store)
    }

    fn utf8_schema(columns: &[&str]) -> SchemaRef {
        Arc::new(Schema::new(
            columns
                .iter()
                .map(|name| Field::new(*name, DataType::Utf8, true))
                .collect::<Vec<_>>(),
        ))
    }

    async fn register(
        context: &SessionContext,
        store: &InMemory,
        key: &str,
        data: Vec<u8>,
        schema: SchemaRef,
        format: DelimitedFormat,
    ) {
        store
            .put(&Path::from(key), Bytes::from(data).into())
            .await
            .expect("put fixture");
        let url = ObjectStoreUrl::parse(format!("s3://{BUCKET}")).expect("store URL");
        let provider = DelimitedSelectTable::try_new(&context.state(), url, key, schema, format)
            .await
            .expect("prepare delimited table");
        context.register_table("S3Object", provider).expect("register table");
    }

    fn csv_format(options: CsvOptions) -> DelimitedFormat {
        DelimitedFormat::Csv(Box::new(options))
    }

    fn csv_options(has_header: bool) -> CsvOptions {
        CsvOptions {
            has_header: Some(has_header),
            ..Default::default()
        }
    }

    async fn collect(context: &SessionContext, sql: &str) -> DFResult<Vec<RecordBatch>> {
        context.sql(sql).await.expect("plan query").collect().await
    }

    fn strings(batches: &[RecordBatch], column: usize) -> Vec<Option<String>> {
        batches
            .iter()
            .flat_map(|batch| {
                let values = batch
                    .column(column)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("utf8 column");
                (0..values.len())
                    .map(|index| values.is_valid(index).then(|| values.value(index).to_string()))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    fn ids(range: std::ops::Range<usize>) -> Vec<Option<String>> {
        range.map(|id| Some(id.to_string())).collect()
    }

    #[tokio::test]
    async fn batches_flush_on_input_bytes_at_record_boundaries() {
        const ROWS: usize = 6;
        const PAYLOAD_BYTES: usize = 600 * 1024;
        let payload = "x".repeat(PAYLOAD_BYTES);
        let mut csv = b"id,payload\n".to_vec();
        let mut json = Vec::new();
        for id in 0..ROWS {
            csv.extend_from_slice(format!("{id},{payload}\n").as_bytes());
            json.extend_from_slice(format!("{{\"id\":\"{id}\",\"payload\":\"{payload}\"}}\n").as_bytes());
        }
        for (data, format) in [(csv, csv_format(csv_options(true))), (json, DelimitedFormat::JsonLines)] {
            let (context, store) = session(64 * MIB);
            register(&context, &store, "rows", data, utf8_schema(&["id", "payload"]), format).await;

            let batches = collect(&context, "SELECT id FROM S3Object").await.expect("execute query");
            // At most 1 MiB of buffered input plus the record that crosses the threshold.
            assert!(batches.iter().all(|batch| batch.num_rows() <= 2), "{batches:?}");
            assert_eq!(strings(&batches, 0), ids(0..ROWS));
        }
    }

    #[tokio::test]
    async fn records_of_the_maximum_size_fit_the_default_memory_limit_including_empty_projection() {
        const ROWS: usize = 24;
        let payload = "x".repeat(MIB - 4);
        let mut csv = Vec::new();
        let mut json = Vec::new();
        for id in 10..10 + ROWS {
            csv.extend_from_slice(format!("{id},{payload}\n").as_bytes());
            json.extend_from_slice(format!("{{\"id\":\"{id}\",\"payload\":\"{}\"}}\n", &payload[..MIB - 24]).as_bytes());
        }
        for (data, format) in [(csv, csv_format(csv_options(false))), (json, DelimitedFormat::JsonLines)] {
            for sql in [
                "SELECT count(*) FROM S3Object",
                "SELECT count(*) FROM S3Object WHERE id <> ''",
            ] {
                let (context, store) = session(64 * MIB);
                register(&context, &store, "rows", data.clone(), utf8_schema(&["id", "payload"]), format.clone()).await;

                let batches = collect(&context, sql)
                    .await
                    .expect("conformant records must not exhaust the default limit");
                let count = batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("count column")
                    .value(0);
                assert_eq!(count, ROWS as i64, "sql={sql}");
            }
        }
    }

    #[tokio::test]
    async fn tight_memory_limits_are_met_with_smaller_batches() {
        const ROWS: usize = 30_000;
        let payload = "x".repeat(90);
        let mut csv = Vec::new();
        let mut json = Vec::new();
        for id in 0..ROWS {
            csv.extend_from_slice(format!("{id},{payload}\n").as_bytes());
            json.extend_from_slice(format!("{{\"id\":\"{id}\",\"payload\":\"{payload}\"}}\n").as_bytes());
        }
        for (data, format) in [(csv, csv_format(csv_options(false))), (json, DelimitedFormat::JsonLines)] {
            let (context, store) = session(4 * MIB);
            register(&context, &store, "rows", data, utf8_schema(&["id", "payload"]), format).await;

            let batches = collect(&context, "SELECT id FROM S3Object")
                .await
                .expect("small records fit a tight memory limit");
            assert!(batches.len() > 16, "{}", batches.len());
            assert_eq!(strings(&batches, 0), ids(0..ROWS));
        }
    }

    #[tokio::test]
    async fn logical_records_beyond_the_memory_limit_are_rejected() {
        const LINE_BYTES: usize = 512 * 1024;
        let line = "x".repeat(LINE_BYTES);

        let quoted = [line.as_str(); 8].join("\n");
        let (context, store) = session(32 * MIB);
        let data = format!("0,\"{quoted}\"\n").into_bytes();
        let format = csv_format(csv_options(false));
        register(&context, &store, "record.csv", data, utf8_schema(&["_1", "_2"]), format).await;
        let error = collect(&context, "SELECT _1 FROM S3Object")
            .await
            .expect_err("an oversized CSV record must not be buffered outside the pool");
        assert!(matches!(error, DataFusionError::ResourcesExhausted(_)), "{error}");

        let items = vec![format!("\"{line}\""); 8].join(",\n");
        let (context, store) = session(32 * MIB);
        let data = format!("{{\"id\":\"0\",\"items\":[{items}]}}\n").into_bytes();
        register(&context, &store, "record.json", data, utf8_schema(&["id"]), DelimitedFormat::JsonLines).await;
        let error = collect(&context, "SELECT id FROM S3Object")
            .await
            .expect_err("an oversized JSON record must not be buffered outside the pool");
        assert!(matches!(error, DataFusionError::ResourcesExhausted(_)), "{error}");
    }

    #[tokio::test]
    async fn skipped_lines_are_not_charged_to_the_memory_pool() {
        const SKIPPED_BYTES: usize = 3 * MIB;
        for (newline, comment) in [("\n", false), ("\r\n", false), ("\n", true), ("\r\n", true)] {
            let skipped = if comment {
                format!("# {}{newline}", "x".repeat(62)).repeat(SKIPPED_BYTES / 64)
            } else {
                newline.repeat(SKIPPED_BYTES)
            };
            // Include skipped runs immediately after the header AND between data rows.
            let data = format!("id,name{newline}{skipped}0,a{newline}{skipped}1,b{newline}{skipped}").into_bytes();
            let options = CsvOptions {
                comment: comment.then_some(b'#'),
                ..csv_options(true)
            };
            // Each skipped run alone would be charged 48 MiB if retained in pending_bytes.
            let (context, store) = session(8 * MIB);
            let format = csv_format(options);
            register(&context, &store, "sparse.csv", data, utf8_schema(&["id", "name"]), format).await;
            let batches = collect(&context, "SELECT id FROM S3Object")
                .await
                .expect("blank and comment lines hold no decoder memory");
            assert_eq!(strings(&batches, 0), ids(0..2), "newline={newline:?} comment={comment}");
        }

        let (context, store) = session(8 * MIB);
        let data = format!("{{\"id\":\"0\"}}\n{}{{\"id\":\"1\"}}\n", " \n".repeat(SKIPPED_BYTES / 2)).into_bytes();
        register(&context, &store, "sparse.json", data, utf8_schema(&["id"]), DelimitedFormat::JsonLines).await;
        let batches = collect(&context, "SELECT id FROM S3Object")
            .await
            .expect("blank lines hold no decoder memory");
        assert_eq!(strings(&batches, 0), ids(0..2));
    }

    #[tokio::test]
    async fn wide_schemas_bound_the_rows_of_a_csv_batch() {
        const COLUMNS: usize = 10_000;
        assert_eq!(csv_decoder_layout(2, 8192).expect("narrow layout"), (8192, 8192 * 2 * 16 + 2 * 256));
        let (batch_size, reservation) = csv_decoder_layout(COLUMNS, 8192).expect("wide layout");
        assert_eq!(batch_size, 10);
        assert!(reservation <= CSV_DECODER_BUFFER_TARGET_BYTES);
        let (batch_size, reservation) = csv_decoder_layout(1_000_000, 8192).expect("widest layout");
        assert_eq!((batch_size, reservation), (1, 1_000_000 * (16 + 256)));

        let names = (1..=COLUMNS).map(|index| format!("_{index}")).collect::<Vec<_>>();
        let schema = utf8_schema(&names.iter().map(String::as_str).collect::<Vec<_>>());
        let row = format!("{}\n", vec!["v"; COLUMNS].join(","));

        let (context, store) = session(16 * MIB);
        let format = csv_format(csv_options(false));
        register(
            &context,
            &store,
            "wide.csv",
            row.repeat(25).into_bytes(),
            Arc::clone(&schema),
            format.clone(),
        )
        .await;
        let batches = collect(&context, "SELECT _1, _10000 FROM S3Object").await.expect("wide rows");
        assert!(batches.iter().all(|batch| batch.num_rows() <= 10), "{batches:?}");
        assert_eq!(strings(&batches, 1), vec![Some("v".to_string()); 25]);

        let (context, store) = session(MIB);
        register(&context, &store, "wide.csv", row.into_bytes(), schema, format).await;
        let error = collect(&context, "SELECT _1 FROM S3Object")
            .await
            .expect_err("the decoder's per-column buffers are charged to the pool");
        assert!(matches!(error, DataFusionError::ResourcesExhausted(_)), "{error}");
    }

    #[tokio::test]
    async fn projection_filter_and_limit_are_applied() {
        let (context, store) = session(64 * MIB);
        let data = b"id,name\n1,Alice\n2,Bob\n3,Carol\n".to_vec();
        let format = csv_format(csv_options(true));
        register(&context, &store, "people.csv", data, utf8_schema(&["id", "name"]), format).await;

        let batches = collect(&context, "SELECT name FROM S3Object WHERE id = '2'")
            .await
            .expect("filter");
        assert_eq!(batches[0].schema().fields().len(), 1);
        assert_eq!(strings(&batches, 0), [Some("Bob".to_string())]);

        let batches = collect(&context, "SELECT name, id FROM S3Object LIMIT 2")
            .await
            .expect("limit");
        assert_eq!(strings(&batches, 0), [Some("Alice".to_string()), Some("Bob".to_string())]);
        assert_eq!(strings(&batches, 1), ids(1..3));

        let (context, store) = session(64 * MIB);
        let data = b"{\"id\":\"1\",\"name\":\"Alice\"}\n{\"name\":\"Bob\"}\n{\"id\":\"3\"}".to_vec();
        register(
            &context,
            &store,
            "people.json",
            data,
            utf8_schema(&["id", "name"]),
            DelimitedFormat::JsonLines,
        )
        .await;
        let batches = collect(&context, "SELECT name FROM S3Object").await.expect("projection");
        assert_eq!(strings(&batches, 0), [Some("Alice".to_string()), Some("Bob".to_string()), None]);
    }

    #[tokio::test]
    async fn csv_batches_match_the_stock_datafusion_scan() {
        let padding = "p".repeat(2000);
        let fixture = |newline: &str, terminator: &str, comments: bool| {
            let mut data = format!("{terminator}id,text,pad{terminator}");
            if comments {
                data.insert_str(0, &format!("# leading comment{newline}"));
            }
            for id in 0..2000 {
                let text = match id % 8 {
                    0 => format!("\"multi{newline}line {id}\""),
                    1 => "\"quoted \"\"text\"\", with delimiter\"".to_string(),
                    2 => String::new(),
                    3 => format!("\"#{id}\""),
                    _ => format!("plain {id}"),
                };
                data.push_str(&format!("{id},{text},{padding}{terminator}"));
                match id % 16 {
                    5 => data.push_str(terminator),
                    9 if comments => data.push_str(&format!("# comment {id}{newline}")),
                    13 if comments => data.push_str(&format!("#\"open quote{newline}{terminator}{terminator}")),
                    _ => {}
                }
            }
            data.push_str("2000,last,record without terminator");
            data.into_bytes()
        };
        let cases = [
            ("\n", "\n", None, true),
            ("\r\n", "\r\n", None, true),
            ("\n", "\r", None, true),
            ("\n", "\n", Some(b'\n'), true),
            ("\n", ";", Some(b';'), false),
        ];
        for (newline, terminator, custom_terminator, comments) in cases {
            let options = CsvOptions {
                comment: comments.then_some(b'#'),
                terminator: custom_terminator,
                ..csv_options(true)
            };
            let data = fixture(newline, terminator, comments);
            let schema = utf8_schema(&["id", "text", "pad"]);
            let sql = "SELECT id, text FROM S3Object";

            let (context, store) = session(64 * MIB);
            store
                .put(&Path::from("stock.csv"), Bytes::from(data.clone()).into())
                .await
                .expect("put fixture");
            let file_format = CsvFormat::default().with_options(options.clone());
            let config = ListingTableConfig::new(ListingTableUrl::parse(format!("s3://{BUCKET}/stock.csv")).expect("table URL"))
                .with_listing_options(ListingOptions::new(Arc::new(file_format)).with_file_extension(""))
                .with_schema(Arc::clone(&schema));
            let provider = Arc::new(ListingTable::try_new(config).expect("stock table"));
            context.register_table("S3Object", provider).expect("register table");
            let expected = collect(&context, sql).await.expect("stock scan");
            assert_eq!(strings(&expected, 0), ids(0..2001), "terminator={terminator:?}");

            let (context, store) = session(64 * MIB);
            register(&context, &store, "bounded.csv", data, schema, csv_format(options)).await;
            let batches = collect(&context, sql).await.expect("bounded scan");
            assert!(batches.len() > 2, "the byte target must split the scan into several batches");
            assert_eq!(strings(&batches, 0), strings(&expected, 0), "terminator={terminator:?}");
            assert_eq!(strings(&batches, 1), strings(&expected, 1), "terminator={terminator:?}");
        }
    }
}
