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

use async_trait::async_trait;
use bytes::{Buf as _, Bytes};
use datafusion::{
    arrow::{datatypes::SchemaRef, json::ReaderBuilder, record_batch::RecordBatch},
    catalog::Session,
    common::{DataFusionError, Result as DFResult, project_schema, runtime::SpawnedTask},
    datasource::TableProvider,
    execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation},
    logical_expr::{Expr, TableProviderFilterPushDown, TableType},
    object_store::{Error as ObjectStoreError, ObjectStoreExt as _, path::Path},
    physical_plan::{
        ExecutionPlan, RecordBatchStream, SendableRecordBatchStream,
        stream::RecordBatchStreamAdapter,
        streaming::{PartitionStream, StreamingTableExec},
    },
};
use futures::{Stream, StreamExt as _, stream};
use parking_lot::Mutex;
use rustfs_s3select_api::QueryResult;
use std::{
    fmt,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

use super::json_document_schema::infer_schema;

const SCHEMA_INFERENCE_MAX_RECORDS: usize = 1000;
const SCHEMA_INFERENCE_MAX_BYTES: usize = 4 * 1024 * 1024;
const SCHEMA_INFERENCE_MAX_SCHEMA_BYTES: usize = 4 * 1024 * 1024;
const SCHEMA_INFERENCE_WORK_MULTIPLIER: usize = 16;
// Once the byte target is reached, finish only the current protocol-sized
// logical record before flushing. The multiplier covers Arrow's tape, offsets,
// structural positions, output buffers, and Vec growth for dense minified arrays.
const JSON_DECODE_BATCH_BYTES: usize = 1024 * 1024;
const JSON_DECODE_MEMORY_MULTIPLIER: usize = 32;
const JSON_DECODE_POLL_BYTES: usize = 64 * 1024;
const JSON_DECODER_TAPE_TARGET_BYTES: usize = 4 * 1024 * 1024;
const JSON_DECODER_PER_FIELD_METADATA_BYTES: usize = 256;

pub(super) struct JsonDocumentTable {
    schema: SchemaRef,
    source: Arc<Mutex<Option<JsonDocumentSource>>>,
}

impl fmt::Debug for JsonDocumentTable {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JsonDocumentTable")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl JsonDocumentTable {
    pub(super) async fn try_new(state: &dyn Session, bucket: &str, key: &str) -> QueryResult<Arc<dyn TableProvider>> {
        let object_store_url = datafusion::execution::object_store::ObjectStoreUrl::parse(format!("s3://{bucket}"))?;
        let object_store = state.runtime_env().object_store(&object_store_url)?;
        let result = object_store.get(&Path::from(key)).await.map_err(DataFusionError::from)?;
        let memory_pool = Arc::clone(&state.runtime_env().memory_pool);
        let (schema, source) = prepare_source(result.into_stream(), memory_pool).await?;
        Ok(Arc::new(Self {
            schema,
            source: Arc::new(Mutex::new(Some(source))),
        }))
    }
}

#[async_trait]
impl TableProvider for JsonDocumentTable {
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
        let scan_limit = filters.is_empty().then_some(limit).flatten();
        let default_batch_size = usize::from(state.config_options().execution.batch_size);
        let batch_size = scan_limit
            .filter(|limit| *limit > 0)
            .map_or(default_batch_size, |limit| limit.min(default_batch_size));
        let projected_schema = project_schema(&self.schema, projection)?;
        let partition: Arc<dyn PartitionStream> = Arc::new(JsonDocumentPartition {
            schema: Arc::clone(&projected_schema),
            source: Arc::clone(&self.source),
            batch_size,
        });
        let plan = StreamingTableExec::try_new(projected_schema, vec![partition], None, [], false, scan_limit)?;
        Ok(Arc::new(plan))
    }

    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}

#[derive(Debug)]
struct JsonDocumentPartition {
    schema: SchemaRef,
    source: Arc<Mutex<Option<JsonDocumentSource>>>,
    batch_size: usize,
}

impl PartitionStream for JsonDocumentPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _context: Arc<datafusion::execution::TaskContext>) -> SendableRecordBatchStream {
        let source = self.source.lock().take();
        let Some(source) = source else {
            return error_stream(
                Arc::clone(&self.schema),
                DataFusionError::Execution("JSON DOCUMENT input stream was consumed more than once".to_string()),
            );
        };
        match JsonDocumentBatchStream::try_new(Arc::clone(&self.schema), source, self.batch_size) {
            Ok(stream) => Box::pin(stream),
            Err(error) => error_stream(Arc::clone(&self.schema), error),
        }
    }
}

fn error_stream(schema: SchemaRef, error: DataFusionError) -> SendableRecordBatchStream {
    Box::pin(RecordBatchStreamAdapter::new(schema, stream::once(async move { Err(error) })))
}

async fn prepare_source(
    stream: futures::stream::BoxStream<'static, Result<Bytes, ObjectStoreError>>,
    memory_pool: Arc<dyn MemoryPool>,
) -> DFResult<(SchemaRef, JsonDocumentSource)> {
    let reservation = MemoryConsumer::new("S3 Select JSON schema inference").register(&memory_pool);
    let mut input = JsonDocumentInput::new(stream);
    let mut sample = Vec::new();
    let mut records = 0;
    let mut largest_record = 0;

    while records < SCHEMA_INFERENCE_MAX_RECORDS {
        let Some(row) = input.next_row().await? else {
            break;
        };
        let required = sample.len().checked_add(row.len()).ok_or_else(schema_prefix_resource_error)?;
        if required > SCHEMA_INFERENCE_MAX_BYTES {
            return Err(schema_prefix_too_large_error());
        }
        reserve_schema_prefix(&mut sample, required, &reservation)?;
        largest_record = largest_record.max(row.len());
        sample.extend_from_slice(&row);
        records += 1;
    }

    reservation.try_resize(schema_inference_reservation_bytes(sample.capacity(), largest_record)?)?;
    let cancellation = Arc::new(AtomicBool::new(false));
    let _cancel_on_drop = SchemaInferenceCancellation(Arc::clone(&cancellation));
    let inference_task = SpawnedTask::spawn_blocking(move || {
        let result = infer_schema(&sample, records, cancellation.as_ref(), SCHEMA_INFERENCE_MAX_SCHEMA_BYTES);
        (sample, reservation, input, result)
    });
    let (sample, reservation, input, inference) = inference_task
        .join()
        .await
        .map_err(|error| DataFusionError::Execution(format!("JSON DOCUMENT schema inference worker failed: {error}")))?;
    let (schema, inferred_records, schema_bytes) = inference?;
    if inferred_records != records {
        return Err(DataFusionError::Execution(format!(
            "JSON DOCUMENT schema prefix contained {records} records but Arrow decoded {inferred_records}"
        )));
    }
    let replay_capacity = sample.capacity();
    let retained_bytes = replay_capacity
        .checked_add(schema_bytes)
        .ok_or_else(schema_prefix_resource_error)?;
    reservation.try_resize(retained_bytes)?;
    let replay = (!sample.is_empty()).then(|| Bytes::from(sample));
    Ok((
        Arc::new(schema),
        JsonDocumentSource {
            replay,
            replay_reservation: replay_capacity,
            input,
            reservation,
            memory_pool,
        },
    ))
}

struct SchemaInferenceCancellation(Arc<AtomicBool>);

impl Drop for SchemaInferenceCancellation {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

fn schema_inference_reservation_bytes(sample_capacity: usize, largest_record: usize) -> DFResult<usize> {
    largest_record
        .checked_mul(SCHEMA_INFERENCE_WORK_MULTIPLIER)
        .and_then(|work| work.checked_add(sample_capacity))
        .and_then(|bytes| bytes.checked_add(SCHEMA_INFERENCE_MAX_SCHEMA_BYTES))
        .ok_or_else(schema_prefix_resource_error)
}

fn reserve_schema_prefix(buffer: &mut Vec<u8>, required: usize, reservation: &MemoryReservation) -> DFResult<()> {
    if required <= buffer.capacity() {
        return Ok(());
    }
    let target = required
        .checked_next_power_of_two()
        .ok_or_else(schema_prefix_resource_error)?;
    reservation.try_resize(target)?;
    buffer
        .try_reserve_exact(target.saturating_sub(buffer.len()))
        .map_err(|_| schema_prefix_resource_error())?;
    reservation.try_resize(buffer.capacity())
}

fn schema_prefix_resource_error() -> DataFusionError {
    DataFusionError::ResourcesExhausted("JSON DOCUMENT schema prefix exceeds the query memory limit".to_string())
}

fn schema_prefix_too_large_error() -> DataFusionError {
    DataFusionError::ResourcesExhausted(format!(
        "JSON DOCUMENT schema inference exceeds the bounded {SCHEMA_INFERENCE_MAX_BYTES}-byte prefix"
    ))
}

/// The object-store adapter emits chunks containing one or more complete
/// newline-terminated rows. A row split across chunks violates that boundary.
struct JsonDocumentInput {
    stream: futures::stream::BoxStream<'static, Result<Bytes, ObjectStoreError>>,
    current: Bytes,
    finished: bool,
}

impl fmt::Debug for JsonDocumentInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JsonDocumentInput")
            .field("current_bytes", &self.current.len())
            .field("finished", &self.finished)
            .finish_non_exhaustive()
    }
}

impl JsonDocumentInput {
    fn new(stream: futures::stream::BoxStream<'static, Result<Bytes, ObjectStoreError>>) -> Self {
        Self {
            stream,
            current: Bytes::new(),
            finished: false,
        }
    }

    async fn next_row(&mut self) -> DFResult<Option<Bytes>> {
        loop {
            if let Some(newline) = self.current.iter().position(|byte| *byte == b'\n') {
                return Ok(Some(self.current.split_to(newline + 1)));
            }
            if !self.current.is_empty() {
                return Err(DataFusionError::Execution(
                    "JSON DOCUMENT row stream produced a non-newline-terminated chunk".to_string(),
                ));
            }
            if self.finished {
                return Ok(None);
            }
            match self.stream.next().await {
                Some(Ok(bytes)) => self.current = bytes,
                Some(Err(error)) => return Err(DataFusionError::ObjectStore(Box::new(error))),
                None => self.finished = true,
            }
        }
    }

    fn poll_next_bytes(&mut self, context: &mut Context<'_>) -> Poll<Option<DFResult<Bytes>>> {
        if !self.current.is_empty() {
            return Poll::Ready(Some(Ok(std::mem::take(&mut self.current))));
        }
        if self.finished {
            return Poll::Ready(None);
        }
        match self.stream.as_mut().poll_next(context) {
            Poll::Ready(Some(Ok(bytes))) => Poll::Ready(Some(Ok(bytes))),
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(DataFusionError::ObjectStore(Box::new(error))))),
            Poll::Ready(None) => {
                self.finished = true;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

struct JsonDocumentSource {
    replay: Option<Bytes>,
    replay_reservation: usize,
    input: JsonDocumentInput,
    reservation: MemoryReservation,
    memory_pool: Arc<dyn MemoryPool>,
}

impl fmt::Debug for JsonDocumentSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JsonDocumentSource")
            .field("replay_bytes", &self.replay.as_ref().map_or(0, Bytes::len))
            .field("input", &self.input)
            .finish_non_exhaustive()
    }
}

impl JsonDocumentSource {
    fn poll_next(&mut self, context: &mut Context<'_>) -> Poll<Option<DFResult<(Bytes, usize)>>> {
        if let Some(replay) = self.replay.take() {
            return Poll::Ready(Some(Ok((replay, self.replay_reservation))));
        }
        self.input
            .poll_next_bytes(context)
            .map(|item| item.map(|result| result.map(|bytes| (bytes, 0))))
    }
}

struct JsonDocumentBatchStream {
    schema: SchemaRef,
    current: Bytes,
    ready: Option<JsonDocumentDecoderReady>,
    flush_task: Option<SpawnedTask<JsonDocumentFlushOutput>>,
    decoder_base_reservation: usize,
    batch_size: usize,
    batch_input_bytes: usize,
    row_bytes_since_newline: usize,
    row_probe_bytes: usize,
    row_boundary_verified: bool,
    release_decoder_work: bool,
    current_reservation: usize,
    done: bool,
}

struct JsonDocumentDecoderReady {
    source: JsonDocumentSource,
    decoder: datafusion::arrow::json::reader::Decoder,
    reservation: MemoryReservation,
}

struct JsonDocumentFlushOutput {
    ready: JsonDocumentDecoderReady,
    result: DFResult<Option<RecordBatch>>,
    final_flush: bool,
}

impl JsonDocumentBatchStream {
    fn try_new(schema: SchemaRef, source: JsonDocumentSource, batch_size: usize) -> DFResult<Self> {
        let (batch_size, decoder_base_reservation) = json_decoder_layout(&schema, batch_size.max(1))?;
        let reservation = MemoryConsumer::new("S3 Select JSON decoder").register(&source.memory_pool);
        reservation.try_resize(decoder_base_reservation)?;
        let decoder = ReaderBuilder::new(Arc::clone(&schema))
            .with_batch_size(batch_size)
            .build_decoder()?;
        Ok(Self {
            schema,
            current: Bytes::new(),
            ready: Some(JsonDocumentDecoderReady {
                source,
                decoder,
                reservation,
            }),
            flush_task: None,
            decoder_base_reservation,
            batch_size,
            batch_input_bytes: 0,
            row_bytes_since_newline: 0,
            row_probe_bytes: 0,
            row_boundary_verified: false,
            release_decoder_work: false,
            current_reservation: 0,
            done: false,
        })
    }

    fn release_current(&mut self) -> DFResult<()> {
        self.current = Bytes::new();
        self.row_bytes_since_newline = 0;
        self.row_probe_bytes = 0;
        self.row_boundary_verified = false;
        let reservation = std::mem::take(&mut self.current_reservation);
        if reservation > 0
            && let Some(ready) = self.ready.as_ref()
        {
            ready.source.reservation.try_shrink(reservation)?;
        }
        Ok(())
    }

    fn reserve_decoder_input(&mut self, bytes: usize) -> DFResult<()> {
        let input_bytes = self.batch_input_bytes.checked_add(bytes).ok_or_else(decoder_resource_error)?;
        let work = input_bytes
            .checked_mul(JSON_DECODE_MEMORY_MULTIPLIER)
            .ok_or_else(decoder_resource_error)?;
        let reservation = self
            .decoder_base_reservation
            .checked_add(work)
            .ok_or_else(decoder_resource_error)?;
        self.ready
            .as_ref()
            .ok_or_else(decoder_worker_error)?
            .reservation
            .try_resize(reservation)
    }

    fn release_decoder_work(&mut self) -> DFResult<()> {
        if let Some(ready) = self.ready.as_ref() {
            ready.reservation.try_resize(self.decoder_base_reservation)?;
        }
        Ok(())
    }

    fn start_flush(&mut self, context: &mut Context<'_>, final_flush: bool) -> Poll<Option<DFResult<RecordBatch>>> {
        let Some(mut ready) = self.ready.take() else {
            return self.fail(decoder_worker_error());
        };
        self.flush_task = Some(SpawnedTask::spawn_blocking(move || {
            let result = ready.decoder.flush().map_err(DataFusionError::from);
            JsonDocumentFlushOutput {
                ready,
                result,
                final_flush,
            }
        }));
        self.poll_flush(context)
    }

    fn poll_flush(&mut self, context: &mut Context<'_>) -> Poll<Option<DFResult<RecordBatch>>> {
        let poll = match self.flush_task.as_mut() {
            Some(task) => Pin::new(task).poll(context),
            None => return self.fail(decoder_worker_error()),
        };
        let Poll::Ready(joined) = poll else {
            return Poll::Pending;
        };
        self.flush_task = None;
        let output = match joined {
            Ok(output) => output,
            Err(_) => return self.fail(decoder_worker_error()),
        };
        let final_flush = output.final_flush;
        self.ready = Some(output.ready);
        match output.result {
            Ok(Some(batch)) => {
                self.batch_input_bytes = 0;
                self.release_decoder_work = true;
                self.done = final_flush;
                Poll::Ready(Some(Ok(batch)))
            }
            Ok(None) if final_flush => {
                self.batch_input_bytes = 0;
                if let Err(error) = self.release_decoder_work() {
                    return self.fail(error);
                }
                self.done = true;
                Poll::Ready(None)
            }
            Ok(None) => self.fail(DataFusionError::Execution(
                "Arrow JSON decoder reached a flush boundary without producing a batch".to_string(),
            )),
            Err(error) => self.fail(error),
        }
    }

    fn fail(&mut self, mut error: DataFusionError) -> Poll<Option<DFResult<RecordBatch>>> {
        self.done = true;
        if let Err(release_error) = self.release_current() {
            error = release_error;
        }
        if let Err(release_error) = self.release_decoder_work() {
            error = release_error;
        }
        Poll::Ready(Some(Err(error)))
    }
}

impl RecordBatchStream for JsonDocumentBatchStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Stream for JsonDocumentBatchStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.flush_task.is_some() {
            return self.poll_flush(context);
        }
        if self.release_decoder_work {
            if let Err(error) = self.release_decoder_work() {
                return self.fail(error);
            }
            self.release_decoder_work = false;
        }
        if self.done {
            return Poll::Ready(None);
        }
        let mut poll_budget = JSON_DECODE_POLL_BYTES;
        loop {
            let Some(ready) = self.ready.as_ref() else {
                return self.fail(decoder_worker_error());
            };
            if ready.decoder.len() >= self.batch_size
                || (self.batch_input_bytes >= JSON_DECODE_BATCH_BYTES
                    && !ready.decoder.has_partial_record()
                    && !ready.decoder.is_empty())
            {
                return self.start_flush(context, false);
            }
            if poll_budget == 0 {
                context.waker().wake_by_ref();
                return Poll::Pending;
            }
            if self.current.is_empty() {
                if let Err(error) = self.release_current() {
                    return self.fail(error);
                }
                let source_poll = match self.ready.as_mut() {
                    Some(ready) => ready.source.poll_next(context),
                    None => return self.fail(decoder_worker_error()),
                };
                match source_poll {
                    Poll::Ready(Some(Ok((bytes, reservation)))) => {
                        self.current = bytes;
                        self.current_reservation = reservation;
                    }
                    Poll::Ready(Some(Err(error))) => return self.fail(error),
                    Poll::Ready(None) => return self.start_flush(context, true),
                    Poll::Pending => return Poll::Pending,
                }
            }

            let batch_budget = JSON_DECODE_BATCH_BYTES.saturating_sub(self.batch_input_bytes);
            let decoder_has_complete_rows = self
                .ready
                .as_ref()
                .is_some_and(|ready| !ready.decoder.has_partial_record() && !ready.decoder.is_empty())
                && self.row_bytes_since_newline == 0;
            if decoder_has_complete_rows && self.current.len() > batch_budget && !self.row_boundary_verified {
                let probe_start = self.row_probe_bytes;
                let probe_end = probe_start.saturating_add(poll_budget).min(batch_budget);
                if let Some(newline) = self.current[probe_start..probe_end].iter().position(|byte| *byte == b'\n') {
                    poll_budget = poll_budget.saturating_sub(newline + 1);
                    self.row_probe_bytes = 0;
                    self.row_boundary_verified = true;
                } else if probe_end == batch_budget {
                    self.row_probe_bytes = 0;
                    self.row_boundary_verified = false;
                    return self.start_flush(context, false);
                } else {
                    self.row_probe_bytes = probe_end;
                    context.waker().wake_by_ref();
                    return Poll::Pending;
                }
            }
            if poll_budget == 0 {
                context.waker().wake_by_ref();
                return Poll::Pending;
            }
            let mut available = self.current.len().min(poll_budget);
            if batch_budget > 0 {
                available = available.min(batch_budget);
            }
            if self.row_bytes_since_newline > 0 {
                if let Some(newline) = self.current[..available].iter().position(|byte| *byte == b'\n') {
                    available = newline + 1;
                }
            } else if decoder_has_complete_rows
                && let Some(newline) = self.current[..available].iter().rposition(|byte| *byte == b'\n')
            {
                available = newline + 1;
            }
            if let Err(error) = self.reserve_decoder_input(available) {
                return self.fail(error);
            }
            let decode_result = {
                let stream = self.as_mut().get_mut();
                match stream.ready.as_mut() {
                    Some(ready) => ready.decoder.decode(&stream.current[..available]),
                    None => return stream.fail(decoder_worker_error()),
                }
            };
            let decoded = match decode_result {
                Ok(decoded) => decoded,
                Err(error) => return self.fail(DataFusionError::from(error)),
            };
            if decoded == 0 {
                return self.fail(DataFusionError::Execution("Arrow JSON decoder made no progress".to_string()));
            }
            self.row_bytes_since_newline = match self.current[..decoded].iter().rposition(|byte| *byte == b'\n') {
                Some(newline) => decoded - newline - 1,
                None => match self.row_bytes_since_newline.checked_add(decoded) {
                    Some(bytes) => bytes,
                    None => return self.fail(decoder_resource_error()),
                },
            };
            self.current.advance(decoded);
            self.row_boundary_verified = false;
            self.batch_input_bytes = match self.batch_input_bytes.checked_add(decoded) {
                Some(bytes) => bytes,
                None => return self.fail(decoder_resource_error()),
            };
            poll_budget = poll_budget.saturating_sub(decoded);
        }
    }
}

fn json_decoder_layout(schema: &SchemaRef, requested_batch_size: usize) -> DFResult<(usize, usize)> {
    let fields = schema.flattened_fields().len();
    let field_slots = fields.checked_mul(2).ok_or_else(decoder_resource_error)?;
    let offset_bytes_per_row = field_slots
        .checked_mul(std::mem::size_of::<usize>())
        .ok_or_else(decoder_resource_error)?;
    let tape_bytes_per_row = field_slots
        .checked_add(2)
        .and_then(|elements| elements.checked_mul(std::mem::size_of::<u64>()))
        .ok_or_else(decoder_resource_error)?;
    let bytes_per_row = offset_bytes_per_row
        .checked_add(tape_bytes_per_row)
        .ok_or_else(decoder_resource_error)?;
    let metadata_bytes = fields
        .checked_mul(JSON_DECODER_PER_FIELD_METADATA_BYTES)
        .and_then(|bytes| bytes.checked_add(field_slots.checked_mul(std::mem::size_of::<u64>())?))
        .and_then(|bytes| bytes.checked_add(10 * std::mem::size_of::<u64>()))
        .ok_or_else(decoder_resource_error)?;
    let tape_budget = JSON_DECODER_TAPE_TARGET_BYTES.saturating_sub(metadata_bytes);
    let bounded_batch_size = requested_batch_size.min((tape_budget / bytes_per_row).max(1));
    let tape_bytes = bytes_per_row
        .checked_mul(bounded_batch_size)
        .and_then(|bytes| bytes.checked_add(std::mem::size_of::<usize>() + std::mem::size_of::<u64>()))
        .ok_or_else(decoder_resource_error)?;
    let reservation = metadata_bytes.checked_add(tape_bytes).ok_or_else(decoder_resource_error)?;
    Ok((bounded_batch_size, reservation))
}

fn decoder_resource_error() -> DataFusionError {
    DataFusionError::ResourcesExhausted("JSON DOCUMENT decoder exceeds the query memory limit".to_string())
}

fn decoder_worker_error() -> DataFusionError {
    DataFusionError::Execution("JSON DOCUMENT decoder worker terminated unexpectedly".to_string())
}
