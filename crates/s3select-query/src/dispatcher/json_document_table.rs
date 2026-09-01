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

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::{
        arrow::{array::Int64Array, datatypes::Schema},
        object_store::memory::InMemory,
        prelude::SessionContext,
    };
    use futures::TryStreamExt as _;

    #[tokio::test]
    async fn schema_prefix_is_replayed_without_reopening_the_object() {
        let store = Arc::new(InMemory::new());
        let path = Path::from("input.json");
        store
            .put(&path, Bytes::from_static(b"{\"id\":1}\n{\"id\":2}\n").into())
            .await
            .expect("put fixture");
        let context = SessionContext::new();
        let url = datafusion::execution::object_store::ObjectStoreUrl::parse("s3://bucket").expect("store URL");
        context.register_object_store(url.as_ref(), store);
        let provider = JsonDocumentTable::try_new(&context.state(), "bucket", "input.json")
            .await
            .expect("prepare streaming provider");
        context.register_table("S3Object", provider).expect("register table");

        let batches = context
            .sql("SELECT id FROM S3Object ORDER BY id")
            .await
            .expect("plan query")
            .collect()
            .await
            .expect("execute query");
        let values: Vec<i64> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("id column")
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        assert_eq!(values, vec![1, 2]);

        let error = context
            .sql("SELECT id FROM S3Object")
            .await
            .expect("plan second scan")
            .collect()
            .await
            .expect_err("JSON DOCUMENT source must be consumed exactly once");
        assert!(error.to_string().contains("consumed more than once"));
    }

    #[tokio::test]
    async fn schema_prefix_fails_closed_at_its_byte_limit() {
        const ROWS: usize = 6;
        const PAYLOAD_BYTES: usize = 700 * 1024;
        let rows = (0..ROWS)
            .map(|id| {
                Ok::<_, ObjectStoreError>(Bytes::from(format!("{{\"id\":{id},\"payload\":\"{}\"}}\n", "x".repeat(PAYLOAD_BYTES))))
            })
            .collect::<Vec<_>>();
        let memory_pool: Arc<dyn MemoryPool> = Arc::new(datafusion::execution::memory_pool::UnboundedMemoryPool::default());
        let error = prepare_source(stream::iter(rows).boxed(), memory_pool)
            .await
            .expect_err("an incomplete schema prefix must not silently drop later fields");
        assert!(matches!(error, DataFusionError::ResourcesExhausted(_)));
    }

    #[tokio::test]
    async fn empty_row_stream_produces_an_empty_query_result() {
        let memory_pool: Arc<dyn MemoryPool> = Arc::new(datafusion::execution::memory_pool::UnboundedMemoryPool::default());
        let (schema, source) = prepare_source(stream::empty().boxed(), memory_pool)
            .await
            .expect("prepare empty JSON DOCUMENT stream");
        assert!(schema.fields().is_empty());

        let provider = Arc::new(JsonDocumentTable {
            schema,
            source: Arc::new(Mutex::new(Some(source))),
        });
        let context = SessionContext::new();
        context.register_table("S3Object", provider).expect("register empty table");
        let batches = context
            .sql("SELECT COUNT(*) FROM S3Object")
            .await
            .expect("plan empty query")
            .collect()
            .await
            .expect("execute empty query");
        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count column")
            .value(0);
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn object_row_with_an_empty_schema_is_counted() {
        let input = stream::once(async { Ok::<_, ObjectStoreError>(Bytes::from_static(b"{}\n")) }).boxed();
        let memory_pool: Arc<dyn MemoryPool> = Arc::new(datafusion::execution::memory_pool::UnboundedMemoryPool::default());
        let (schema, source) = prepare_source(input, memory_pool)
            .await
            .expect("prepare empty-object JSON DOCUMENT stream");
        assert!(schema.fields().is_empty());

        let provider = Arc::new(JsonDocumentTable {
            schema,
            source: Arc::new(Mutex::new(Some(source))),
        });
        let context = SessionContext::new();
        context.register_table("S3Object", provider).expect("register table");
        let batches = context
            .sql("SELECT COUNT(*) FROM S3Object")
            .await
            .expect("plan empty-object query")
            .collect()
            .await
            .expect("execute empty-object query");
        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count column")
            .value(0);
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn limit_drops_a_stalled_tail_after_the_bounded_schema_prefix() {
        const EXPECTED_SCHEMA_PREFIX_RECORDS: usize = 1000;
        assert_eq!(SCHEMA_INFERENCE_MAX_RECORDS, EXPECTED_SCHEMA_PREFIX_RECORDS);
        let rows = (0..EXPECTED_SCHEMA_PREFIX_RECORDS).map(|id| Ok(Bytes::from(format!("{{\"id\":{id}}}\n"))));
        let input = stream::iter(rows).chain(stream::pending()).boxed();
        let memory_pool = Arc::new(datafusion::execution::memory_pool::GreedyMemoryPool::new(16 * 1024 * 1024));
        let (schema, source) = prepare_source(input, memory_pool.clone())
            .await
            .expect("prepare bounded schema prefix");
        let provider = Arc::new(JsonDocumentTable {
            schema,
            source: Arc::new(Mutex::new(Some(source))),
        });
        let context = SessionContext::new();
        context
            .register_table("S3Object", provider)
            .expect("register streaming table");

        let batches = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            context
                .sql("SELECT id FROM S3Object LIMIT 1")
                .await
                .expect("plan limited query")
                .collect()
                .await
        })
        .await
        .expect("LIMIT should not wait for the unread object tail")
        .expect("execute limited query");
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        assert_eq!(memory_pool.reserved(), 0);
    }

    #[tokio::test]
    async fn row_chunks_must_end_on_a_record_boundary() {
        let chunks = stream::iter([
            Ok::<_, ObjectStoreError>(Bytes::from_static(b"{\"id\":")),
            Ok(Bytes::from_static(b"1}\n")),
        ])
        .boxed();
        let mut input = JsonDocumentInput::new(chunks);

        let error = input
            .next_row()
            .await
            .expect_err("a producer must not split one JSON row across chunks");
        assert!(error.to_string().contains("non-newline-terminated chunk"));
    }

    #[test]
    fn schema_inference_has_no_fixed_field_count_limit() {
        const FIELDS: usize = 4097;
        let cancellation = AtomicBool::new(false);
        let mut input = json_object_with_unique_keys(FIELDS);
        input.push(b'\n');

        let (schema, records, _) = infer_schema(&input, 1, &cancellation, SCHEMA_INFERENCE_MAX_SCHEMA_BYTES)
            .expect("a protocol-valid wide object should be governed by memory, not a field-count constant");
        assert_eq!(records, 1);
        assert_eq!(schema.fields().len(), FIELDS);
    }

    #[tokio::test]
    async fn schema_prefix_respects_the_query_memory_pool() {
        let memory_pool = Arc::new(datafusion::execution::memory_pool::GreedyMemoryPool::new(1));
        let input = stream::once(async { Ok::<_, ObjectStoreError>(Bytes::from_static(b"{\"id\":1}\n")) }).boxed();

        let error = prepare_source(input, memory_pool.clone())
            .await
            .expect_err("schema prefix allocation must use the query memory pool");
        assert!(matches!(error, DataFusionError::ResourcesExhausted(_)));
        assert_eq!(memory_pool.reserved(), 0);
    }

    #[test]
    fn schema_inference_cancellation_reaches_cpu_boundaries() {
        let cancellation = Arc::new(AtomicBool::new(false));
        drop(SchemaInferenceCancellation(Arc::clone(&cancellation)));
        assert!(cancellation.load(Ordering::Acquire));

        let error = infer_schema(b"{}\n", 1, cancellation.as_ref(), SCHEMA_INFERENCE_MAX_SCHEMA_BYTES)
            .expect_err("schema inference must observe cancellation");
        assert!(error.to_string().contains("schema inference canceled"));
    }

    #[test]
    fn queued_schema_inference_retains_query_admission() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .expect("build test runtime");

        runtime.block_on(async {
            let (blocking_started_tx, blocking_started_rx) = tokio::sync::oneshot::channel();
            let (release_blocking_tx, release_blocking_rx) = std::sync::mpsc::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                let _ = blocking_started_tx.send(());
                release_blocking_rx.recv().expect("release blocking worker");
            });
            blocking_started_rx.await.expect("blocking worker should start");

            let admission = Arc::new(tokio::sync::Semaphore::new(1));
            let query_guard = Arc::new(
                Arc::clone(&admission)
                    .acquire_owned()
                    .await
                    .expect("query admission should be available"),
            );
            let input = stream::iter([Ok::<_, ObjectStoreError>(Bytes::from_static(b"{\"id\":1}\n"))])
                .map(move |row| {
                    let _query_guard = &query_guard;
                    row
                })
                .boxed();
            let memory_pool: Arc<dyn MemoryPool> = Arc::new(datafusion::execution::memory_pool::UnboundedMemoryPool::default());
            {
                let inference = prepare_source(input, memory_pool);
                futures::pin_mut!(inference);
                assert!(futures::poll!(inference.as_mut()).is_pending());
            }

            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(100), Arc::clone(&admission).acquire_owned(),)
                    .await
                    .is_err(),
                "queued schema inference must retain query admission through teardown"
            );

            release_blocking_tx.send(()).expect("release blocking worker");
            blocker.await.expect("blocking worker should finish");
            let recovered = tokio::time::timeout(std::time::Duration::from_secs(1), Arc::clone(&admission).acquire_owned())
                .await
                .expect("schema worker teardown should release query admission")
                .expect("query admission should remain open");
            drop(recovered);
        });
    }

    #[tokio::test]
    async fn projection_is_applied_before_json_value_decoding() {
        let rows = stream::iter(
            (0..SCHEMA_INFERENCE_MAX_RECORDS)
                .map(|id| Ok::<_, ObjectStoreError>(Bytes::from(format!("{{\"id\":{id},\"payload\":\"ok\"}}\n")))),
        )
        .chain(stream::once(async {
            Ok(Bytes::from_static(b"{\"id\":1000,\"payload\":{\"shape\":\"incompatible\"}}\n"))
        }))
        .boxed();
        let memory_pool: Arc<dyn MemoryPool> = Arc::new(datafusion::execution::memory_pool::UnboundedMemoryPool::default());
        let (schema, source) = prepare_source(rows, memory_pool).await.expect("prepare projection fixture");
        let provider = Arc::new(JsonDocumentTable {
            schema,
            source: Arc::new(Mutex::new(Some(source))),
        });
        let context = SessionContext::new();
        context.register_table("S3Object", provider).expect("register table");

        let batches = context
            .sql("SELECT id FROM S3Object")
            .await
            .expect("plan projected query")
            .collect()
            .await
            .expect("unprojected type changes must not affect selected columns");
        let values: Vec<i64> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("id column")
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        assert_eq!(values.len(), SCHEMA_INFERENCE_MAX_RECORDS + 1);
        assert_eq!(values.last(), Some(&1000));
    }

    #[tokio::test]
    async fn filter_columns_remain_available_to_the_physical_filter() {
        let rows = stream::iter([
            Ok::<_, ObjectStoreError>(Bytes::from_static(b"{\"id\":1,\"payload\":\"keep\"}\n")),
            Ok(Bytes::from_static(b"{\"id\":2,\"payload\":\"drop\"}\n")),
        ])
        .boxed();
        let memory_pool: Arc<dyn MemoryPool> = Arc::new(datafusion::execution::memory_pool::UnboundedMemoryPool::default());
        let (schema, source) = prepare_source(rows, memory_pool).await.expect("prepare filter fixture");
        let provider = Arc::new(JsonDocumentTable {
            schema,
            source: Arc::new(Mutex::new(Some(source))),
        });
        let context = SessionContext::new();
        context.register_table("S3Object", provider).expect("register table");

        let batches = context
            .sql("SELECT id FROM S3Object WHERE payload = 'keep'")
            .await
            .expect("plan filtered query")
            .collect()
            .await
            .expect("execute filtered query");
        let values: Vec<i64> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("id column")
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        assert_eq!(values, vec![1]);
    }

    #[tokio::test]
    async fn decoder_flushes_large_input_on_a_byte_boundary() {
        use datafusion::arrow::datatypes::{DataType, Field};

        const ROWS: usize = 10;
        const PAYLOAD_BYTES: usize = 512 * 1024;
        let rows = (0..ROWS)
            .map(|id| {
                Ok::<_, ObjectStoreError>(Bytes::from(format!("{{\"id\":{id},\"payload\":\"{}\"}}\n", "x".repeat(PAYLOAD_BYTES))))
            })
            .collect::<Vec<_>>();
        let memory_pool: Arc<dyn MemoryPool> = Arc::new(datafusion::execution::memory_pool::UnboundedMemoryPool::default());
        let source = JsonDocumentSource {
            replay: None,
            replay_reservation: 0,
            input: JsonDocumentInput::new(stream::iter(rows).boxed()),
            reservation: MemoryConsumer::new("byte-batch source").register(&memory_pool),
            memory_pool,
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("payload", DataType::Utf8, true),
        ]));
        let batches = JsonDocumentBatchStream::try_new(schema, source, 1024)
            .expect("build JSON decoder")
            .try_collect::<Vec<_>>()
            .await
            .expect("decode large input");

        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), ROWS);
        assert!(batches.len() >= 2, "the decoder must not retain more than one byte-bounded batch");
    }

    #[tokio::test]
    async fn decoder_flushes_before_a_large_row_exceeds_the_remaining_budget() {
        use datafusion::arrow::datatypes::{DataType, Field};

        let row = |total_bytes: usize| {
            const PREFIX: &str = "{\"payload\":\"";
            const SUFFIX: &str = "\"}\n";
            let payload_bytes = total_bytes
                .checked_sub(PREFIX.len() + SUFFIX.len())
                .expect("row fixture must fit its JSON wrapper");
            Bytes::from(format!("{PREFIX}{}{SUFFIX}", "x".repeat(payload_bytes)))
        };
        let rows = stream::iter([
            Ok::<_, ObjectStoreError>(row(JSON_DECODE_BATCH_BYTES - JSON_DECODE_POLL_BYTES + 1)),
            Ok(row(JSON_DECODE_BATCH_BYTES)),
        ])
        .boxed();
        let memory_pool = Arc::new(datafusion::execution::memory_pool::GreedyMemoryPool::new(40 * 1024 * 1024));
        let query_memory_pool: Arc<dyn MemoryPool> = memory_pool.clone();
        let source = JsonDocumentSource {
            replay: None,
            replay_reservation: 0,
            input: JsonDocumentInput::new(rows),
            reservation: MemoryConsumer::new("large-row boundary source").register(&query_memory_pool),
            memory_pool: query_memory_pool,
        };
        let schema = Arc::new(Schema::new(vec![Field::new("payload", DataType::Utf8, true)]));

        let batches = JsonDocumentBatchStream::try_new(schema, source, 1024)
            .expect("build JSON decoder")
            .try_collect::<Vec<_>>()
            .await
            .expect("a legal large row must start in a fresh byte-bounded batch");

        assert_eq!(batches.len(), 2);
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        assert_eq!(memory_pool.reserved(), 0);
    }

    #[tokio::test]
    async fn decoder_yields_after_its_per_poll_byte_budget() {
        const EXPECTED_POLL_BYTES: usize = 64 * 1024;
        assert_eq!(JSON_DECODE_POLL_BYTES, EXPECTED_POLL_BYTES);
        let row = Bytes::from(format!("{{\"payload\":\"{}\"}}\n", "x".repeat(256 * 1024)));
        let memory_pool: Arc<dyn MemoryPool> = Arc::new(datafusion::execution::memory_pool::UnboundedMemoryPool::default());
        let (schema, source) = prepare_source(stream::once(async move { Ok::<_, ObjectStoreError>(row) }).boxed(), memory_pool)
            .await
            .expect("prepare cooperative decoder fixture");
        let mut decoder = JsonDocumentBatchStream::try_new(schema, source, 1024).expect("build JSON decoder");
        let waker = futures::task::noop_waker();
        let mut context = Context::from_waker(&waker);

        assert!(
            Pin::new(&mut decoder).poll_next(&mut context).is_pending(),
            "one poll must not decode an entire large row"
        );
        let batches = decoder
            .try_collect::<Vec<_>>()
            .await
            .expect("finish decoding after cooperative yield");
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    }

    #[tokio::test]
    async fn dense_nested_values_are_charged_before_arrow_allocation() {
        use datafusion::arrow::datatypes::{DataType, Field};

        let replay = Bytes::from(format!("{{\"values\":[{}0]}}\n", "0,".repeat(32 * 1024)));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "values",
            DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true))),
            true,
        )]));
        let (_, decoder_base_reservation) = json_decoder_layout(&schema, 1024).expect("calculate decoder reservation");
        let pool_size = replay
            .len()
            .checked_mul(JSON_DECODE_MEMORY_MULTIPLIER - 1)
            .and_then(|work| work.checked_add(replay.len()))
            .and_then(|bytes| bytes.checked_add(decoder_base_reservation))
            .and_then(|bytes| bytes.checked_sub(1))
            .expect("decoder memory fixture should fit");
        let memory_pool = Arc::new(datafusion::execution::memory_pool::GreedyMemoryPool::new(pool_size));
        let query_memory_pool: Arc<dyn MemoryPool> = memory_pool.clone();
        let reservation = MemoryConsumer::new("JSON decoder memory test").register(&query_memory_pool);
        reservation
            .try_resize(replay.len())
            .expect("reserve the retained replay buffer");
        let source = JsonDocumentSource {
            replay_reservation: replay.len(),
            replay: Some(replay),
            input: JsonDocumentInput::new(stream::empty().boxed()),
            reservation,
            memory_pool: query_memory_pool,
        };
        let mut decoder = JsonDocumentBatchStream::try_new(schema, source, 1024).expect("build JSON decoder");

        let error = decoder
            .next()
            .await
            .expect("decoder must emit a memory error")
            .expect_err("dense nested values must be charged before Arrow allocates their tape");
        assert!(matches!(error, DataFusionError::ResourcesExhausted(_)));
        drop(decoder);
        assert_eq!(memory_pool.reserved(), 0);
    }

    #[test]
    fn wide_schema_bounds_decoder_preallocation() {
        use datafusion::arrow::datatypes::{DataType, Field};

        const FIELDS: usize = 1000;
        const REQUESTED_BATCH_SIZE: usize = 8192;
        let schema = Arc::new(Schema::new(
            (0..FIELDS)
                .map(|index| Field::new(format!("field_{index}"), DataType::Utf8, true))
                .collect::<Vec<_>>(),
        ));
        let (batch_size, reservation) = json_decoder_layout(&schema, REQUESTED_BATCH_SIZE).expect("calculate decoder layout");

        assert!(batch_size < REQUESTED_BATCH_SIZE);
        assert!(reservation <= JSON_DECODER_TAPE_TARGET_BYTES + FIELDS * JSON_DECODER_PER_FIELD_METADATA_BYTES);
        let memory_pool = Arc::new(datafusion::execution::memory_pool::GreedyMemoryPool::new(reservation));
        let query_memory_pool: Arc<dyn MemoryPool> = memory_pool.clone();
        let source = JsonDocumentSource {
            replay: None,
            replay_reservation: 0,
            input: JsonDocumentInput::new(stream::empty().boxed()),
            reservation: MemoryConsumer::new("wide schema source").register(&query_memory_pool),
            memory_pool: query_memory_pool,
        };
        let decoder = JsonDocumentBatchStream::try_new(schema, source, REQUESTED_BATCH_SIZE)
            .expect("bounded decoder should fit its registered reservation");
        assert_eq!(decoder.batch_size, batch_size);
        assert_eq!(memory_pool.reserved(), reservation);
        drop(decoder);
        assert_eq!(memory_pool.reserved(), 0);
    }

    #[test]
    fn queued_decoder_flush_retains_upstream_query_admission() {
        use datafusion::arrow::datatypes::{DataType, Field};

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .expect("build test runtime");

        runtime.block_on(async {
            let (blocking_started_tx, blocking_started_rx) = tokio::sync::oneshot::channel();
            let (release_blocking_tx, release_blocking_rx) = std::sync::mpsc::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                let _ = blocking_started_tx.send(());
                release_blocking_rx.recv().expect("release blocking worker");
            });
            blocking_started_rx.await.expect("blocking worker should start");

            let admission = Arc::new(tokio::sync::Semaphore::new(1));
            let query_guard = Arc::new(
                Arc::clone(&admission)
                    .acquire_owned()
                    .await
                    .expect("query admission should be available"),
            );
            let input = stream::iter([Ok::<_, ObjectStoreError>(Bytes::from_static(b"{\"id\":1}\n"))])
                .map(move |row| {
                    let _query_guard = &query_guard;
                    row
                })
                .boxed();
            let memory_pool: Arc<dyn MemoryPool> = Arc::new(datafusion::execution::memory_pool::UnboundedMemoryPool::default());
            let source = JsonDocumentSource {
                replay: None,
                replay_reservation: 0,
                input: JsonDocumentInput::new(input),
                reservation: MemoryConsumer::new("queued decoder source").register(&memory_pool),
                memory_pool,
            };
            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
            {
                let mut decoder = JsonDocumentBatchStream::try_new(schema, source, 1).expect("build JSON decoder");
                let waker = futures::task::noop_waker();
                let mut context = Context::from_waker(&waker);
                assert!(Pin::new(&mut decoder).poll_next(&mut context).is_pending());
            }

            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(100), Arc::clone(&admission).acquire_owned(),)
                    .await
                    .is_err(),
                "queued decoder teardown must retain upstream query admission"
            );

            release_blocking_tx.send(()).expect("release blocking worker");
            blocker.await.expect("blocking worker should finish");
            let recovered = tokio::time::timeout(std::time::Duration::from_secs(1), Arc::clone(&admission).acquire_owned())
                .await
                .expect("decoder teardown should release query admission")
                .expect("query admission should remain open");
            drop(recovered);
        });
    }

    fn json_object_with_unique_keys(keys: usize) -> Vec<u8> {
        use std::fmt::Write as _;

        let mut object = String::from("{");
        for index in 0..keys {
            if index > 0 {
                object.push(',');
            }
            write!(&mut object, "\"field_{index}\":0").expect("write key fixture");
        }
        object.push('}');
        object.into_bytes()
    }
}
