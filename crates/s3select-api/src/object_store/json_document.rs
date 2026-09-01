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

use super::MAX_JSON_DOCUMENT_BYTES;
use crate::{
    QueryError, SelectError, SelectInputMetrics,
    input_stream::{
        BlockingChannelReader, MAX_SELECT_RECORD_BYTES, SELECT_DECODE_CHUNK_BYTES, SelectInputReader, find_error_source,
    },
    metrics::SelectInputMetricsRecorder,
    query::{
        ast::{JsonPathSegment, JsonSource},
        session::{QueryExecutionGuard, QueryExecutionTracker},
    },
};
use bytes::Bytes;
use datafusion::{
    common::runtime::SpawnedTask,
    execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation},
    object_store::{Error as ObjectStoreError, Result as ObjectStoreResult},
};
use futures::{StreamExt as _, TryStreamExt as _, stream};
use futures_core::Stream;
use std::{
    fmt,
    future::Future as _,
    io,
    pin::Pin,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};
use tokio::{
    io::AsyncReadExt,
    sync::{Semaphore, mpsc, oneshot},
};
use tokio_util::io::ReaderStream;

const JSON_DOCUMENT_INPUT_CHANNEL_CAPACITY: usize = 2;
const JSON_DOCUMENT_ROW_CHANNEL_CAPACITY: usize = 1;
const JSON_DOCUMENT_OUTPUT_BATCH_BYTES: usize = SELECT_DECODE_CHUNK_BYTES;
const JSON_DOCUMENT_MAX_DEPTH: usize = 100;
const JSON_DOCUMENT_RAW_MEMORY_RESERVATION_MULTIPLIER: usize = 2;
const JSON_DOCUMENT_DOM_MEMORY_RESERVATION_MULTIPLIER: usize = 32;
const JSON_SCALAR_COLUMN_MEMORY_RESERVATION_MULTIPLIER: usize = 2;
const JSON_DOCUMENT_MIN_PARSER_THREADS: usize = 4;
const JSON_DOCUMENT_MAX_PARSER_THREADS: usize = 32;
pub(super) const JSON_CANCELLATION_CHECK_BYTES: usize = 64 * 1024;
pub(super) const JSON_CANCELLATION_CHECK_KEYS: usize = 1024;

static JSON_DOCUMENT_PARSER_THREADS: LazyLock<usize> = LazyLock::new(|| {
    std::thread::available_parallelism().map_or(JSON_DOCUMENT_MIN_PARSER_THREADS, |parallelism| {
        parallelism
            .get()
            .saturating_mul(2)
            .clamp(JSON_DOCUMENT_MIN_PARSER_THREADS, JSON_DOCUMENT_MAX_PARSER_THREADS)
    })
});
static JSON_DOCUMENT_PARSER_ADMISSION: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(*JSON_DOCUMENT_PARSER_THREADS));

pub(super) fn json_document_ndjson_stream(
    stream: SelectInputReader,
    original_size: u64,
    json_source: JsonSource,
    input_metrics: Arc<SelectInputMetrics>,
    memory_pool: Arc<dyn MemoryPool>,
    query_tracker: Option<QueryExecutionTracker>,
) -> ObjectStoreResult<Pin<Box<dyn Stream<Item = ObjectStoreResult<Bytes>> + Send + 'static>>> {
    validate_json_document_size(original_size)?;
    Ok(json_document_stream(
        stream,
        JsonDocumentReadMode::Exact {
            original_size,
            input_metrics: input_metrics.recorder(),
        },
        json_source,
        memory_pool,
        query_tracker,
    ))
}

pub(super) fn compressed_json_document_ndjson_stream(
    stream: SelectInputReader,
    json_source: JsonSource,
    memory_pool: Arc<dyn MemoryPool>,
    query_tracker: Option<QueryExecutionTracker>,
) -> Pin<Box<dyn Stream<Item = ObjectStoreResult<Bytes>> + Send + 'static>> {
    json_document_stream(stream, JsonDocumentReadMode::Bounded, json_source, memory_pool, query_tracker)
}

enum JsonDocumentReadMode {
    Exact {
        original_size: u64,
        input_metrics: SelectInputMetricsRecorder,
    },
    Bounded,
}

fn json_document_stream(
    stream: SelectInputReader,
    read_mode: JsonDocumentReadMode,
    json_source: JsonSource,
    memory_pool: Arc<dyn MemoryPool>,
    query_tracker: Option<QueryExecutionTracker>,
) -> Pin<Box<dyn Stream<Item = ObjectStoreResult<Bytes>> + Send + 'static>> {
    let config = JsonDocumentPipelineConfig {
        stream,
        read_mode,
        json_source,
        memory_pool,
        query_tracker,
    };
    Box::pin(stream::once(JsonDocumentPipeline::start(config)).try_flatten())
}

struct JsonDocumentPipelineConfig {
    stream: SelectInputReader,
    read_mode: JsonDocumentReadMode,
    json_source: JsonSource,
    memory_pool: Arc<dyn MemoryPool>,
    query_tracker: Option<QueryExecutionTracker>,
}

struct JsonDocumentPipeline {
    receiver: mpsc::Receiver<Bytes>,
    read_task: Option<SpawnedTask<()>>,
    parse_result: oneshot::Receiver<ObjectStoreResult<()>>,
    done: bool,
    cancellation: Arc<AtomicBool>,
    _resources: Arc<JsonDocumentPipelineResources>,
}

struct JsonDocumentPipelineResources {
    _channel_reservation: MemoryReservation,
    _query_guard: Option<QueryExecutionGuard>,
}

impl JsonDocumentPipeline {
    async fn start(config: JsonDocumentPipelineConfig) -> ObjectStoreResult<Self> {
        let parser_permit = JSON_DOCUMENT_PARSER_ADMISSION
            .acquire()
            .await
            .map_err(|_| json_document_worker_store_error("parser admission"))?;
        let JsonDocumentPipelineConfig {
            stream,
            read_mode,
            json_source,
            memory_pool,
            query_tracker,
        } = config;
        let query_guard = match query_tracker.as_ref() {
            Some(query_tracker) => Some(query_tracker.query_guard().ok_or_else(json_document_cancelled_store_error)?),
            None => None,
        };
        let channel_reservation = MemoryConsumer::new("S3 Select JSON document channels").register(&memory_pool);
        channel_reservation
            .try_resize(json_document_channel_reservation_bytes())
            .map_err(resource_exhausted_store_error)?;
        let resources = Arc::new(JsonDocumentPipelineResources {
            _channel_reservation: channel_reservation,
            _query_guard: query_guard,
        });

        let (input_tx, input_rx) = mpsc::channel(JSON_DOCUMENT_INPUT_CHANNEL_CAPACITY);
        let (row_tx, row_rx) = mpsc::channel(JSON_DOCUMENT_ROW_CHANNEL_CAPACITY);
        let cancellation = Arc::new(AtomicBool::new(false));

        let parse_cancellation = Arc::clone(&cancellation);
        let parser_memory_pool = Arc::clone(&memory_pool);
        let parser_resources = Arc::clone(&resources);
        let (parse_result_tx, parse_result_rx) = oneshot::channel();
        std::thread::Builder::new()
            .name("s3select-json-document".to_string())
            .spawn(move || {
                let _parser_permit = parser_permit;
                let _resources = parser_resources;
                let reservation = MemoryConsumer::new("S3 Select JSON document record").register(&parser_memory_pool);
                let result = parse_json_document(
                    BlockingChannelReader::new(input_rx),
                    &json_source,
                    &reservation,
                    parse_cancellation.as_ref(),
                    |rows| row_tx.blocking_send(rows).map_err(|_| json_document_cancelled_error()),
                )
                .map_err(classify_json_document_error);
                let _ = parse_result_tx.send(result);
            })
            .map_err(|error| json_document_worker_spawn_store_error("parser", error))?;

        // The parser waits on bounded channels, so it must not occupy Tokio's
        // blocking pool. Query admission bounds the dedicated worker threads.
        let read_cancellation = Arc::clone(&cancellation);
        let read_resources = Arc::clone(&resources);
        let read_task = SpawnedTask::spawn(async move {
            let _resources = read_resources;
            forward_json_document_input(stream, read_mode, input_tx, read_cancellation).await;
        });

        Ok(Self {
            receiver: row_rx,
            read_task: Some(read_task),
            parse_result: parse_result_rx,
            done: false,
            cancellation,
            _resources: resources,
        })
    }

    fn poll_terminal(&mut self, context: &mut Context<'_>) -> Poll<Option<ObjectStoreResult<Bytes>>> {
        if let Some(read_task) = self.read_task.as_mut() {
            match Pin::new(read_task).poll(context) {
                Poll::Ready(Ok(())) => self.read_task = None,
                Poll::Ready(Err(_)) => {
                    self.done = true;
                    return Poll::Ready(Some(Err(json_document_worker_store_error("reader"))));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
        match Pin::new(&mut self.parse_result).poll(context) {
            Poll::Ready(Ok(Ok(()))) => {
                self.done = true;
                Poll::Ready(None)
            }
            Poll::Ready(Ok(Err(error))) => {
                self.done = true;
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(Err(_)) => {
                self.done = true;
                Poll::Ready(Some(Err(json_document_worker_store_error("parser"))))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Stream for JsonDocumentPipeline {
    type Item = ObjectStoreResult<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        match self.receiver.poll_recv(context) {
            Poll::Ready(Some(bytes)) => Poll::Ready(Some(Ok(bytes))),
            Poll::Ready(None) => self.poll_terminal(context),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for JsonDocumentPipeline {
    fn drop(&mut self) {
        self.cancellation.store(true, Ordering::Release);
    }
}

async fn forward_json_document_input(
    stream: SelectInputReader,
    read_mode: JsonDocumentReadMode,
    input_tx: mpsc::Sender<io::Result<Bytes>>,
    cancellation: Arc<AtomicBool>,
) {
    let (stream, expected_size, input_metrics): (SelectInputReader, Option<u64>, Option<SelectInputMetricsRecorder>) =
        match read_mode {
            JsonDocumentReadMode::Exact {
                original_size,
                input_metrics,
            } => (Box::new(stream.take(original_size)), Some(original_size), Some(input_metrics)),
            JsonDocumentReadMode::Bounded => (stream, None, None),
        };
    let mut stream = ReaderStream::with_capacity(stream, SELECT_DECODE_CHUNK_BYTES);
    let mut remaining = expected_size;
    let mut total = 0_u64;

    loop {
        if cancellation.load(Ordering::Acquire) || input_tx.is_closed() {
            return;
        }
        let bytes = tokio::select! {
            biased;
            _ = input_tx.closed() => return,
            result = stream.next() => match result {
                Some(Ok(bytes)) => bytes,
                Some(Err(error)) => {
                    send_input_error(&input_tx, error).await;
                    return;
                }
                None => {
                    if let Some(remaining) = remaining
                        && remaining > 0
                    {
                        send_input_error(
                            &input_tx,
                            io::Error::new(
                                io::ErrorKind::UnexpectedEof,
                                format!("object stream ended with {remaining} bytes remaining"),
                            ),
                        )
                        .await;
                    }
                    return;
                }
            },
        };

        if let Some(input_metrics) = input_metrics.as_ref() {
            input_metrics.record_uncompressed(bytes.len());
        }
        let read = match u64::try_from(bytes.len()) {
            Ok(read) => read,
            Err(_) => {
                send_input_error(&input_tx, resource_exhausted_io_error()).await;
                return;
            }
        };
        total = match total.checked_add(read) {
            Some(total) => total,
            None => {
                send_input_error(&input_tx, resource_exhausted_io_error()).await;
                return;
            }
        };
        if total > MAX_JSON_DOCUMENT_BYTES {
            send_input_error(&input_tx, resource_exhausted_io_error()).await;
            return;
        }
        if let Some(value) = remaining.as_mut() {
            let Some(new_remaining) = value.checked_sub(read) else {
                send_input_error(&input_tx, resource_exhausted_io_error()).await;
                return;
            };
            *value = new_remaining;
        }
        let sent = tokio::select! {
            biased;
            _ = input_tx.closed() => false,
            result = input_tx.send(Ok(bytes)) => result.is_ok(),
        };
        if !sent {
            return;
        }
    }
}

async fn send_input_error(input_tx: &mpsc::Sender<io::Result<Bytes>>, error: io::Error) {
    let _ = input_tx.send(Err(error)).await;
}

fn parse_json_document<R, F>(
    reader: R,
    json_source: &JsonSource,
    reservation: &MemoryReservation,
    cancellation: &AtomicBool,
    emit: F,
) -> io::Result<()>
where
    R: io::Read,
    F: FnMut(Bytes) -> io::Result<()>,
{
    let reader = io::BufReader::with_capacity(SELECT_DECODE_CHUNK_BYTES, reader);
    let mut decoder = JsonDocumentDecoder::new(json_source, reservation, cancellation, emit);
    match parse_json_document_reader(reader, &mut decoder, cancellation) {
        Ok(()) => Ok(()),
        Err(error) => {
            decoder.flush_output()?;
            Err(error)
        }
    }
}

fn parse_json_document_reader<R, F>(
    mut reader: R,
    decoder: &mut JsonDocumentDecoder<'_, F>,
    cancellation: &AtomicBool,
) -> io::Result<()>
where
    R: io::BufRead,
    F: FnMut(Bytes) -> io::Result<()>,
{
    loop {
        ensure_json_parse_active(cancellation)?;
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return decoder.finish();
        }
        let consumed = available.len();
        for byte in available.iter().copied() {
            decoder.push(byte)?;
        }
        reader.consume(consumed);
    }
}

struct JsonDocumentDecoder<'a, F> {
    source: &'a JsonSource,
    reservation: &'a MemoryReservation,
    cancellation: &'a AtomicBool,
    emit: F,
    state: JsonRootState,
    record: Vec<u8>,
    stack: Vec<u8>,
    in_string: bool,
    escape_next: bool,
    array_after_comma: bool,
    array_value_terminated: bool,
    array_index: usize,
    output: Vec<u8>,
    emitted_rows: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum JsonRootState {
    Start,
    Array(RootArrayPlan),
    Single,
    Done,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RootArrayPlan {
    Each { path_start: usize },
    Index { index: usize, path_start: usize },
}

impl<'a, F> JsonDocumentDecoder<'a, F>
where
    F: FnMut(Bytes) -> io::Result<()>,
{
    fn new(source: &'a JsonSource, reservation: &'a MemoryReservation, cancellation: &'a AtomicBool, emit: F) -> Self {
        Self {
            source,
            reservation,
            cancellation,
            emit,
            state: JsonRootState::Start,
            record: Vec::new(),
            stack: Vec::with_capacity(JSON_DOCUMENT_MAX_DEPTH),
            in_string: false,
            escape_next: false,
            array_after_comma: false,
            array_value_terminated: false,
            array_index: 0,
            output: Vec::new(),
            emitted_rows: 0,
        }
    }

    fn push(&mut self, byte: u8) -> io::Result<()> {
        match self.state {
            JsonRootState::Start => self.push_start(byte),
            JsonRootState::Array(plan) => self.push_array(byte, plan),
            JsonRootState::Single => self.push_single(byte),
            JsonRootState::Done => {
                if is_json_whitespace(byte) {
                    Ok(())
                } else {
                    Err(json_document_parse_error(JsonDocumentParseError::Malformed))
                }
            }
        }
    }

    fn push_start(&mut self, byte: u8) -> io::Result<()> {
        if is_json_whitespace(byte) {
            return Ok(());
        }
        if byte == b'[' {
            let plan = root_array_plan(self.source.path())?;
            self.stack.push(b']');
            self.state = JsonRootState::Array(plan);
            return Ok(());
        }

        self.state = JsonRootState::Single;
        self.push_single(byte)
    }

    fn push_single(&mut self, byte: u8) -> io::Result<()> {
        if is_json_whitespace(byte) && self.stack.is_empty() && !self.in_string && record_has_value(&self.record) {
            return self.emit_single_record(false);
        }
        self.reserve_record_byte()?;
        self.record.push(byte);
        if self.observe_nested_byte(byte)? {
            self.emit_single_record(false)?;
        }
        Ok(())
    }

    fn push_array(&mut self, byte: u8, plan: RootArrayPlan) -> io::Result<()> {
        if self.in_string {
            self.reserve_record_byte()?;
            self.record.push(byte);
            if self.escape_next {
                self.escape_next = false;
            } else if byte == b'\\' {
                self.escape_next = true;
            } else if byte == b'"' {
                self.in_string = false;
            }
            return Ok(());
        }

        if self.stack.len() == 1 {
            if is_json_whitespace(byte) {
                self.array_value_terminated |= record_has_value(&self.record);
                return Ok(());
            }
            if self.array_value_terminated && !matches!(byte, b',' | b']') {
                return Err(json_document_parse_error(JsonDocumentParseError::Malformed));
            }
        }

        match byte {
            b'"' => {
                self.reserve_record_byte()?;
                self.record.push(byte);
                self.in_string = true;
                self.array_after_comma = false;
                Ok(())
            }
            b'{' | b'[' => {
                self.reserve_record_byte()?;
                self.record.push(byte);
                self.push_container(byte)?;
                self.array_after_comma = false;
                Ok(())
            }
            b'}' => {
                if self.stack.last().copied() != Some(b'}') {
                    return Err(json_document_parse_error(JsonDocumentParseError::Malformed));
                }
                self.stack.pop();
                self.reserve_record_byte()?;
                self.record.push(byte);
                self.array_after_comma = false;
                Ok(())
            }
            b']' if self.stack.len() == 1 => self.finish_array(plan),
            b']' => {
                if self.stack.last().copied() != Some(b']') {
                    return Err(json_document_parse_error(JsonDocumentParseError::Malformed));
                }
                self.stack.pop();
                self.reserve_record_byte()?;
                self.record.push(byte);
                self.array_after_comma = false;
                Ok(())
            }
            b',' if self.stack.len() == 1 => {
                if !record_has_value(&self.record) {
                    return Err(json_document_parse_error(JsonDocumentParseError::Malformed));
                }
                self.emit_array_record(plan)?;
                self.array_after_comma = true;
                Ok(())
            }
            _ => {
                if !is_json_whitespace(byte) {
                    self.array_after_comma = false;
                }
                self.reserve_record_byte()?;
                self.record.push(byte);
                Ok(())
            }
        }
    }

    fn observe_nested_byte(&mut self, byte: u8) -> io::Result<bool> {
        if self.in_string {
            if self.escape_next {
                self.escape_next = false;
            } else if byte == b'\\' {
                self.escape_next = true;
            } else if byte == b'"' {
                self.in_string = false;
            }
            return Ok(!self.in_string && self.stack.is_empty());
        }

        match byte {
            b'"' => self.in_string = true,
            b'{' | b'[' => self.push_container(byte)?,
            b'}' | b']' => {
                let expected = if byte == b'}' { b'}' } else { b']' };
                if self.stack.last().copied() != Some(expected) {
                    return Err(json_document_parse_error(JsonDocumentParseError::Malformed));
                }
                self.stack.pop();
            }
            _ => {}
        }
        Ok(!self.in_string && self.stack.is_empty() && matches!(byte, b'}' | b']'))
    }

    fn push_container(&mut self, opening: u8) -> io::Result<()> {
        if self.stack.len() >= JSON_DOCUMENT_MAX_DEPTH {
            return Err(json_document_parse_error(JsonDocumentParseError::DepthLimit {
                limit: JSON_DOCUMENT_MAX_DEPTH,
            }));
        }
        self.stack.push(if opening == b'{' { b'}' } else { b']' });
        Ok(())
    }

    fn finish_array(&mut self, plan: RootArrayPlan) -> io::Result<()> {
        if record_has_value(&self.record) {
            self.emit_array_record(plan)?;
        } else if self.array_after_comma {
            return Err(json_document_parse_error(JsonDocumentParseError::Malformed));
        }
        self.stack.clear();
        self.record = Vec::new();
        self.reservation.free();
        self.state = JsonRootState::Done;
        Ok(())
    }

    fn emit_array_record(&mut self, plan: RootArrayPlan) -> io::Result<()> {
        ensure_json_parse_active(self.cancellation)?;
        let path = match plan {
            RootArrayPlan::Each { path_start } => Some(&self.source.path()[path_start..]),
            RootArrayPlan::Index { index, path_start } if index == self.array_index => Some(&self.source.path()[path_start..]),
            RootArrayPlan::Index { .. } => None,
        };
        if let Some(path) = path {
            self.process_record(path, false, false)?;
        } else {
            validate_json_value(&self.record, self.cancellation, false)?;
        }
        self.array_index = self.array_index.checked_add(1).ok_or_else(resource_exhausted_io_error)?;
        self.array_value_terminated = false;
        self.reset_record_buffer()?;
        Ok(())
    }

    fn finish(&mut self) -> io::Result<()> {
        ensure_json_parse_active(self.cancellation)?;
        match self.state {
            JsonRootState::Start => Err(json_document_parse_error(JsonDocumentParseError::Malformed)),
            JsonRootState::Array(_) => {
                if record_has_value(&self.record) {
                    validate_json_value(&self.record, self.cancellation, true)?;
                }
                Err(json_document_parse_error(JsonDocumentParseError::Truncated))
            }
            JsonRootState::Done => self.flush_output(),
            JsonRootState::Single => self.emit_single_record(true).and_then(|()| self.flush_output()),
        }
    }

    fn emit_single_record(&mut self, eof: bool) -> io::Result<()> {
        let source_path = self.source.path();
        let path = source_path
            .strip_prefix(&[JsonPathSegment::ArrayWildcard])
            .unwrap_or(source_path);
        let implicitly_expand_arrays = matches!(source_path, [] | [JsonPathSegment::Key { .. }]);
        self.process_record(path, implicitly_expand_arrays, eof)?;
        self.record = Vec::new();
        self.reservation.free();
        self.state = JsonRootState::Done;
        Ok(())
    }

    fn process_record(&mut self, path: &[JsonPathSegment], implicitly_expand_arrays: bool, eof: bool) -> io::Result<()> {
        if path.is_empty() {
            validate_json_value(&self.record, self.cancellation, eof)?;
            return self.append_raw_value(is_json_object(&self.record));
        }

        resize_json_document_dom_reservation(self.reservation, self.record.capacity(), self.source)?;
        let value = deserialize_json_value(&self.record, self.cancellation, eof)?;
        let defer_output_flush = !implicitly_expand_arrays
            && !path
                .iter()
                .any(|segment| matches!(segment, JsonPathSegment::ArrayWildcard | JsonPathSegment::ObjectWildcard));
        let emitted_rows = self.emitted_rows;
        let result = visit_json_path(value, path, self.cancellation, &mut |value| {
            self.emit_value(value, implicitly_expand_arrays, defer_output_flush)
        });
        let shrink_result = resize_json_document_record_reservation(self.reservation, self.record.capacity(), self.source);
        result?;
        shrink_result?;
        if defer_output_flush
            && self.emitted_rows > emitted_rows
            && (self.emitted_rows == 1 || self.output.len() >= JSON_DOCUMENT_OUTPUT_BATCH_BYTES)
        {
            self.flush_output()?;
        }
        Ok(())
    }

    fn emit_value(
        &mut self,
        value: serde_json::Value,
        implicitly_expand_arrays: bool,
        defer_output_flush: bool,
    ) -> io::Result<()> {
        if implicitly_expand_arrays && let serde_json::Value::Array(array) = value {
            for item in array {
                ensure_json_parse_active(self.cancellation)?;
                self.append_json_value(item, defer_output_flush)?;
            }
            return Ok(());
        }
        self.append_json_value(value, defer_output_flush)
    }

    fn append_json_value(&mut self, value: serde_json::Value, defer_output_flush: bool) -> io::Result<()> {
        let scalar_column = (!value.is_object()).then(|| scalar_column(self.source));
        let record_start = self.output.len();
        let (serialize_result, limit_exceeded) = {
            let mut writer = CancellableJsonWriter {
                inner: &mut self.output,
                record_start,
                cancellation: self.cancellation,
                bytes_since_check: 0,
                limit_exceeded: false,
            };
            let serialize_result = match scalar_column {
                Some(column) => {
                    io::Write::write_all(&mut writer, b"{").is_ok()
                        && serde_json::to_writer(&mut writer, column).is_ok()
                        && io::Write::write_all(&mut writer, b":").is_ok()
                        && serde_json::to_writer(&mut writer, &value).is_ok()
                        && io::Write::write_all(&mut writer, b"}").is_ok()
                }
                None => serde_json::to_writer(&mut writer, &value).is_ok(),
            };
            (serialize_result, writer.limit_exceeded)
        };
        if limit_exceeded {
            self.output.truncate(record_start);
            return Err(over_max_record_size_error());
        }
        if !serialize_result {
            self.output.truncate(record_start);
            ensure_json_parse_active(self.cancellation)?;
            return Err(json_document_parse_error(JsonDocumentParseError::Malformed));
        }
        if let Err(error) = ensure_json_parse_active(self.cancellation) {
            self.output.truncate(record_start);
            return Err(error);
        }
        drop(value);
        self.output.push(b'\n');
        self.complete_output_row(!defer_output_flush)
    }

    fn append_raw_value(&mut self, object: bool) -> io::Result<()> {
        let record = trim_json_whitespace(&self.record);
        if record.len() > MAX_SELECT_RECORD_BYTES {
            return Err(over_max_record_size_error());
        }
        let scalar_column = (!object).then(|| scalar_column(self.source));
        let wrapper_bytes = scalar_column
            .map_or(Ok(0), |column| {
                column
                    .len()
                    .checked_mul(6)
                    .and_then(|bytes| bytes.checked_add(5))
                    .ok_or_else(resource_exhausted_io_error)
            })?
            .min(MAX_SELECT_RECORD_BYTES);
        let estimated_record_bytes = record
            .len()
            .checked_add(wrapper_bytes)
            .ok_or_else(resource_exhausted_io_error)?
            .min(MAX_SELECT_RECORD_BYTES);
        let required = self
            .output
            .len()
            .checked_add(estimated_record_bytes)
            .and_then(|length| length.checked_add(1))
            .ok_or_else(resource_exhausted_io_error)?;
        self.output
            .try_reserve(required.saturating_sub(self.output.len()))
            .map_err(|_| resource_exhausted_io_error())?;
        let record_start = self.output.len();
        if let Some(column) = scalar_column {
            let (serialize_result, limit_exceeded) = {
                let mut writer = CancellableJsonWriter {
                    inner: &mut self.output,
                    record_start,
                    cancellation: self.cancellation,
                    bytes_since_check: 0,
                    limit_exceeded: false,
                };
                let serialize_result = io::Write::write_all(&mut writer, b"{").is_ok()
                    && serde_json::to_writer(&mut writer, column).is_ok()
                    && io::Write::write_all(&mut writer, b":").is_ok();
                (serialize_result, writer.limit_exceeded)
            };
            if limit_exceeded {
                self.output.truncate(record_start);
                return Err(over_max_record_size_error());
            }
            if !serialize_result {
                self.output.truncate(record_start);
                ensure_json_parse_active(self.cancellation)?;
                return Err(json_document_parse_error(JsonDocumentParseError::Malformed));
            }
        }
        let mut in_string = false;
        let mut escape_next = false;
        for chunk in record.chunks(JSON_CANCELLATION_CHECK_BYTES) {
            if let Err(error) = ensure_json_parse_active(self.cancellation) {
                self.output.truncate(record_start);
                return Err(error);
            }
            for byte in chunk.iter().copied() {
                let emit_byte = in_string || byte == b'"' || !is_json_whitespace(byte);
                if emit_byte && self.output.len().saturating_sub(record_start) >= MAX_SELECT_RECORD_BYTES {
                    self.output.truncate(record_start);
                    return Err(over_max_record_size_error());
                }
                if in_string {
                    self.output.push(byte);
                    if escape_next {
                        escape_next = false;
                    } else if byte == b'\\' {
                        escape_next = true;
                    } else if byte == b'"' {
                        in_string = false;
                    }
                } else if byte == b'"' {
                    in_string = true;
                    self.output.push(byte);
                } else if !is_json_whitespace(byte) {
                    self.output.push(byte);
                }
            }
        }
        if !object {
            if self.output.len().saturating_sub(record_start) >= MAX_SELECT_RECORD_BYTES {
                self.output.truncate(record_start);
                return Err(over_max_record_size_error());
            }
            self.output.push(b'}');
        }
        self.output.push(b'\n');
        self.complete_output_row(true)
    }

    fn complete_output_row(&mut self, flush_allowed: bool) -> io::Result<()> {
        self.emitted_rows = self.emitted_rows.checked_add(1).ok_or_else(resource_exhausted_io_error)?;
        if flush_allowed && (self.emitted_rows == 1 || self.output.len() >= JSON_DOCUMENT_OUTPUT_BATCH_BYTES) {
            self.flush_output()?;
        }
        Ok(())
    }

    fn flush_output(&mut self) -> io::Result<()> {
        if self.output.is_empty() {
            return Ok(());
        }
        (self.emit)(Bytes::from(std::mem::take(&mut self.output)))
    }

    fn reset_record_buffer(&mut self) -> io::Result<()> {
        if self.record.capacity() > SELECT_DECODE_CHUNK_BYTES.saturating_mul(2) {
            self.record = Vec::new();
            self.reservation.free();
        } else {
            self.record.clear();
            resize_json_document_record_reservation(self.reservation, self.record.capacity(), self.source)?;
        }
        Ok(())
    }

    fn reserve_record_byte(&mut self) -> io::Result<()> {
        let required = self.record.len().checked_add(1).ok_or_else(resource_exhausted_io_error)?;
        if required > MAX_SELECT_RECORD_BYTES {
            return Err(over_max_record_size_error());
        }
        if required <= self.record.capacity() {
            return Ok(());
        }
        let target_capacity = required
            .checked_next_power_of_two()
            .ok_or_else(resource_exhausted_io_error)?
            .min(MAX_SELECT_RECORD_BYTES);
        if target_capacity < required {
            return Err(resource_exhausted_io_error());
        }
        resize_json_document_record_reservation(self.reservation, target_capacity, self.source)?;
        self.record
            .try_reserve_exact(target_capacity.saturating_sub(self.record.len()))
            .map_err(|_| resource_exhausted_io_error())?;
        resize_json_document_record_reservation(self.reservation, self.record.capacity(), self.source)
    }
}

fn deserialize_json_value(bytes: &[u8], cancellation: &AtomicBool, eof: bool) -> io::Result<serde_json::Value> {
    let bytes = trim_json_whitespace(bytes);
    if bytes.len() <= JSON_CANCELLATION_CHECK_BYTES {
        return serde_json::from_slice(bytes).map_err(|error| classify_serde_json_error(error, cancellation, eof));
    }
    let reader = io::BufReader::with_capacity(
        JSON_CANCELLATION_CHECK_BYTES,
        CancellableSliceReader {
            inner: io::Cursor::new(bytes),
            cancellation,
        },
    );
    serde_json::from_reader(reader).map_err(|error| classify_serde_json_error(error, cancellation, eof))
}

fn validate_json_value(bytes: &[u8], cancellation: &AtomicBool, eof: bool) -> io::Result<()> {
    ensure_json_parse_active(cancellation)?;
    let bytes = trim_json_whitespace(bytes);
    if bytes.len() <= JSON_CANCELLATION_CHECK_BYTES {
        serde_json::from_slice::<&serde_json::value::RawValue>(bytes)
            .map(drop)
            .map_err(|error| classify_serde_json_error(error, cancellation, eof))?;
    } else {
        let reader = io::BufReader::with_capacity(
            JSON_CANCELLATION_CHECK_BYTES,
            CancellableSliceReader {
                inner: io::Cursor::new(bytes),
                cancellation,
            },
        );
        serde_json::from_reader::<_, Box<serde_json::value::RawValue>>(reader)
            .map(drop)
            .map_err(|error| classify_serde_json_error(error, cancellation, eof))?;
    }
    ensure_json_parse_active(cancellation)
}

fn classify_serde_json_error(error: serde_json::Error, cancellation: &AtomicBool, eof: bool) -> io::Error {
    match ensure_json_parse_active(cancellation) {
        Err(cancelled) => cancelled,
        Ok(()) => json_document_parse_error(if eof && error.is_eof() {
            JsonDocumentParseError::Truncated
        } else {
            JsonDocumentParseError::Malformed
        }),
    }
}

struct CancellableSliceReader<'a> {
    inner: io::Cursor<&'a [u8]>,
    cancellation: &'a AtomicBool,
}

impl io::Read for CancellableSliceReader<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        ensure_json_parse_active(self.cancellation)?;
        io::Read::read(&mut self.inner, buffer)
    }
}

fn root_array_plan(path: &[JsonPathSegment]) -> io::Result<RootArrayPlan> {
    if let Some(rest) = path.strip_prefix(&[JsonPathSegment::ArrayWildcard]) {
        return Ok(match rest.first() {
            Some(JsonPathSegment::ArrayWildcard) => RootArrayPlan::Each { path_start: 2 },
            Some(JsonPathSegment::Index(index)) => RootArrayPlan::Index {
                index: *index,
                path_start: 2,
            },
            Some(JsonPathSegment::Key { .. } | JsonPathSegment::ObjectWildcard) | None => RootArrayPlan::Each { path_start: 1 },
        });
    }
    Ok(match path.first() {
        None => RootArrayPlan::Each { path_start: 0 },
        Some(JsonPathSegment::Index(index)) => RootArrayPlan::Index {
            index: *index,
            path_start: 1,
        },
        Some(JsonPathSegment::Key { .. } | JsonPathSegment::ArrayWildcard | JsonPathSegment::ObjectWildcard) => {
            return Err(invalid_json_source_path("JSON source path segment does not match the input value"));
        }
    })
}

fn visit_json_path(
    root: serde_json::Value,
    path: &[JsonPathSegment],
    cancellation: &AtomicBool,
    emit: &mut impl FnMut(serde_json::Value) -> io::Result<()>,
) -> io::Result<()> {
    if path.is_empty() {
        ensure_json_parse_active(cancellation)?;
        return emit(root);
    }
    let mut values = vec![root];
    for segment in path {
        ensure_json_parse_active(cancellation)?;
        let mut expanded = Vec::new();
        for value in values {
            ensure_json_parse_active(cancellation)?;
            match (segment, value) {
                (JsonPathSegment::Key { name, quoted }, serde_json::Value::Object(mut object)) => {
                    if let Some(value) = remove_json_source_key(&mut object, name, *quoted, cancellation)? {
                        expanded.push(value);
                    }
                }
                (JsonPathSegment::Index(index), serde_json::Value::Array(array)) => {
                    if let Some(value) = array.into_iter().nth(*index) {
                        expanded.push(value);
                    }
                }
                (JsonPathSegment::ArrayWildcard, serde_json::Value::Array(array)) => expanded.extend(array),
                (JsonPathSegment::ObjectWildcard, serde_json::Value::Object(object)) => {
                    expanded.extend(object.into_values());
                }
                (JsonPathSegment::Key { .. }, _)
                | (JsonPathSegment::Index(_), _)
                | (JsonPathSegment::ArrayWildcard, _)
                | (JsonPathSegment::ObjectWildcard, _) => {
                    return Err(invalid_json_source_path("JSON source path segment does not match the input value"));
                }
            }
        }
        values = expanded;
        if values.is_empty() {
            return Ok(());
        }
    }
    for value in values {
        ensure_json_parse_active(cancellation)?;
        emit(value)?;
    }
    Ok(())
}

fn remove_json_source_key(
    object: &mut serde_json::Map<String, serde_json::Value>,
    name: &str,
    quoted: bool,
    cancellation: &AtomicBool,
) -> io::Result<Option<serde_json::Value>> {
    let mut checkpoint = || ensure_json_parse_active(cancellation);
    remove_json_source_key_with_checkpoint(object, name, quoted, &mut checkpoint)
}

pub(super) fn remove_json_source_key_with_checkpoint(
    object: &mut serde_json::Map<String, serde_json::Value>,
    name: &str,
    quoted: bool,
    checkpoint: &mut impl FnMut() -> io::Result<()>,
) -> io::Result<Option<serde_json::Value>> {
    if quoted {
        return Ok(object.remove(name));
    }

    let mut matched = None;
    for (index, key) in object.keys().enumerate() {
        if index % JSON_CANCELLATION_CHECK_KEYS == 0 {
            checkpoint()?;
        }
        if json_key_eq_ignore_ascii_case_with_checkpoint(key, name, checkpoint)? {
            if matched.is_some() {
                return Err(io::Error::new(io::ErrorKind::InvalidData, SelectError::AmbiguousFieldName));
            }
            matched = Some(key.clone());
        }
    }
    Ok(matched.and_then(|key| object.remove(&key)))
}

pub(super) fn json_key_eq_ignore_ascii_case_with_checkpoint(
    key: &str,
    expected: &str,
    checkpoint: &mut impl FnMut() -> io::Result<()>,
) -> io::Result<bool> {
    if key.len() != expected.len() {
        return Ok(false);
    }

    for (key_chunk, expected_chunk) in key
        .as_bytes()
        .chunks(JSON_CANCELLATION_CHECK_BYTES)
        .zip(expected.as_bytes().chunks(JSON_CANCELLATION_CHECK_BYTES))
    {
        checkpoint()?;
        if !key_chunk.eq_ignore_ascii_case(expected_chunk) {
            return Ok(false);
        }
    }
    Ok(true)
}

fn invalid_json_source_path(message: &'static str) -> io::Error {
    json_document_parse_error(JsonDocumentParseError::SourcePathMismatch(message))
}

fn scalar_column(source: &JsonSource) -> &str {
    source.scalar_column().unwrap_or_else(|| match source.path().last() {
        Some(JsonPathSegment::Key { name, .. }) => name,
        Some(JsonPathSegment::Index(_) | JsonPathSegment::ArrayWildcard | JsonPathSegment::ObjectWildcard) | None => "_1",
    })
}

struct CancellableJsonWriter<'a> {
    inner: &'a mut Vec<u8>,
    record_start: usize,
    cancellation: &'a AtomicBool,
    bytes_since_check: usize,
    limit_exceeded: bool,
}

impl io::Write for CancellableJsonWriter<'_> {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let Some(new_len) = self.inner.len().checked_add(buffer.len()) else {
            self.limit_exceeded = true;
            return Err(io::Error::new(io::ErrorKind::InvalidData, SelectError::OverMaxRecordSize));
        };
        if new_len.saturating_sub(self.record_start) > MAX_SELECT_RECORD_BYTES {
            self.limit_exceeded = true;
            return Err(io::Error::new(io::ErrorKind::InvalidData, SelectError::OverMaxRecordSize));
        }
        self.bytes_since_check = self.bytes_since_check.saturating_add(buffer.len());
        if self.bytes_since_check >= JSON_CANCELLATION_CHECK_BYTES {
            ensure_json_parse_active(self.cancellation)?;
            self.bytes_since_check %= JSON_CANCELLATION_CHECK_BYTES;
        }
        self.inner.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        ensure_json_parse_active(self.cancellation)?;
        self.bytes_since_check = 0;
        Ok(())
    }
}

fn is_json_whitespace(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\r' | b'\n')
}

fn trim_json_whitespace(bytes: &[u8]) -> &[u8] {
    let start = bytes
        .iter()
        .position(|byte| !is_json_whitespace(*byte))
        .unwrap_or(bytes.len());
    let end = bytes
        .iter()
        .rposition(|byte| !is_json_whitespace(*byte))
        .map_or(start, |index| index + 1);
    &bytes[start..end]
}

fn record_has_value(bytes: &[u8]) -> bool {
    bytes.iter().any(|byte| !is_json_whitespace(*byte))
}

fn is_json_object(bytes: &[u8]) -> bool {
    trim_json_whitespace(bytes).first() == Some(&b'{')
}

fn ensure_json_parse_active(cancellation: &AtomicBool) -> io::Result<()> {
    if cancellation.load(Ordering::Acquire) {
        Err(json_document_cancelled_error())
    } else {
        Ok(())
    }
}

fn json_document_channel_reservation_bytes() -> usize {
    // Include queued chunks, a blocked send, and both endpoint read buffers.
    let input_chunks = JSON_DOCUMENT_INPUT_CHANNEL_CAPACITY.saturating_add(3);
    let input_bytes = input_chunks.saturating_mul(SELECT_DECODE_CHUNK_BYTES);
    // Include queued output, the consumer's current chunk, and the producer's
    // current or blocked chunk after Vec growth.
    let output_rows = JSON_DOCUMENT_ROW_CHANNEL_CAPACITY.saturating_add(2);
    let row_capacity = MAX_SELECT_RECORD_BYTES
        .saturating_add(1)
        .checked_next_power_of_two()
        .unwrap_or(usize::MAX);
    let row_bytes = output_rows.saturating_mul(row_capacity);
    input_bytes.saturating_add(row_bytes)
}

pub(super) fn json_document_memory_reservation_bytes(
    record_capacity: usize,
    json_source: &JsonSource,
) -> datafusion::common::Result<usize> {
    json_document_record_reservation_bytes(record_capacity, json_source, JSON_DOCUMENT_RAW_MEMORY_RESERVATION_MULTIPLIER)
}

fn json_document_dom_memory_reservation_bytes(
    record_capacity: usize,
    json_source: &JsonSource,
) -> datafusion::common::Result<usize> {
    json_document_record_reservation_bytes(record_capacity, json_source, JSON_DOCUMENT_DOM_MEMORY_RESERVATION_MULTIPLIER)
}

fn json_document_record_reservation_bytes(
    record_capacity: usize,
    json_source: &JsonSource,
    multiplier: usize,
) -> datafusion::common::Result<usize> {
    let record_bytes = record_capacity.checked_mul(multiplier).ok_or_else(|| {
        datafusion::common::DataFusionError::ResourcesExhausted(format!(
            "JSON DOCUMENT memory reservation overflow for {record_capacity} input bytes"
        ))
    })?;
    let scalar_bytes = scalar_column(json_source)
        .len()
        .checked_mul(JSON_SCALAR_COLUMN_MEMORY_RESERVATION_MULTIPLIER)
        .ok_or_else(|| {
            datafusion::common::DataFusionError::ResourcesExhausted(format!(
                "JSON DOCUMENT scalar column reservation overflow for {record_capacity} input bytes"
            ))
        })?;
    record_bytes.checked_add(scalar_bytes).ok_or_else(|| {
        datafusion::common::DataFusionError::ResourcesExhausted(format!(
            "JSON DOCUMENT memory reservation overflow for {record_capacity} input bytes"
        ))
    })
}

fn resize_json_document_record_reservation(
    reservation: &MemoryReservation,
    record_capacity: usize,
    json_source: &JsonSource,
) -> io::Result<()> {
    let bytes =
        json_document_memory_reservation_bytes(record_capacity, json_source).map_err(|_| resource_exhausted_io_error())?;
    reservation.try_resize(bytes).map_err(|_| resource_exhausted_io_error())
}

fn resize_json_document_dom_reservation(
    reservation: &MemoryReservation,
    record_capacity: usize,
    json_source: &JsonSource,
) -> io::Result<()> {
    let bytes =
        json_document_dom_memory_reservation_bytes(record_capacity, json_source).map_err(|_| resource_exhausted_io_error())?;
    reservation.try_resize(bytes).map_err(|_| resource_exhausted_io_error())
}

pub(super) fn validate_json_document_size(original_size: u64) -> ObjectStoreResult<()> {
    if original_size <= MAX_JSON_DOCUMENT_BYTES {
        Ok(())
    } else {
        Err(resource_exhausted_store_error(datafusion::common::DataFusionError::ResourcesExhausted(
            format!(
                "JSON DOCUMENT object is {original_size} bytes, which exceeds the maximum allowed size of \
                 {MAX_JSON_DOCUMENT_BYTES} bytes ({} MiB). Convert the input to JSON LINES (NDJSON) to process large files.",
                MAX_JSON_DOCUMENT_BYTES / (1024 * 1024)
            ),
        )))
    }
}

fn resource_exhausted_io_error() -> io::Error {
    io::Error::new(io::ErrorKind::OutOfMemory, SelectError::ResourceExhausted)
}

fn over_max_record_size_error() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, SelectError::OverMaxRecordSize)
}

fn json_document_cancelled_error() -> io::Error {
    io::Error::new(io::ErrorKind::Interrupted, SelectError::Canceled)
}

fn json_document_cancelled_store_error() -> ObjectStoreError {
    ObjectStoreError::Generic {
        store: "EcObjectStore",
        source: Box::new(QueryError::Cancel),
    }
}

fn json_document_worker_store_error(worker: &'static str) -> ObjectStoreError {
    ObjectStoreError::Generic {
        store: "EcObjectStore",
        source: Box::new(io::Error::other(format!("JSON DOCUMENT {worker} worker terminated unexpectedly"))),
    }
}

fn json_document_worker_spawn_store_error(worker: &'static str, source: io::Error) -> ObjectStoreError {
    ObjectStoreError::Generic {
        store: "EcObjectStore",
        source: Box::new(io::Error::new(
            source.kind(),
            format!("failed to start JSON DOCUMENT {worker} worker: {source}"),
        )),
    }
}

fn resource_exhausted_store_error(source: datafusion::common::DataFusionError) -> ObjectStoreError {
    ObjectStoreError::Generic {
        store: "EcObjectStore",
        source: Box::new(source),
    }
}

#[derive(Debug)]
enum JsonDocumentParseError {
    Malformed,
    Truncated,
    DepthLimit { limit: usize },
    SourcePathMismatch(&'static str),
}

impl fmt::Display for JsonDocumentParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed => formatter.write_str("malformed JSON document"),
            Self::Truncated => formatter.write_str("truncated JSON document"),
            Self::DepthLimit { limit } => write!(formatter, "JSON document exceeds the maximum nesting depth of {limit}"),
            Self::SourcePathMismatch(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for JsonDocumentParseError {}

fn json_document_parse_error(source: JsonDocumentParseError) -> io::Error {
    let kind = if matches!(source, JsonDocumentParseError::Truncated) {
        io::ErrorKind::UnexpectedEof
    } else {
        io::ErrorKind::InvalidData
    };
    io::Error::new(kind, source)
}

fn classify_json_document_error(error: io::Error) -> ObjectStoreError {
    let source: Box<dyn std::error::Error + Send + Sync> = if let Some(select_error) = find_error_source::<SelectError>(&error) {
        Box::new(select_error.clone())
    } else if find_error_source::<JsonDocumentParseError>(&error).is_some() {
        Box::new(SelectError::JsonParsingError)
    } else {
        Box::new(error)
    };
    ObjectStoreError::Generic {
        store: "EcObjectStore",
        source,
    }
}

#[cfg(test)]
pub(super) fn parse_json_document_to_lines(bytes: &[u8], json_source: &JsonSource) -> io::Result<Vec<Bytes>> {
    let memory_pool: Arc<dyn MemoryPool> = Arc::new(datafusion::execution::memory_pool::UnboundedMemoryPool::default());
    let reservation = MemoryConsumer::new("JSON document parser test").register(&memory_pool);
    let mut rows = Vec::new();
    parse_json_document(io::Cursor::new(bytes), json_source, &reservation, &AtomicBool::new(false), |batch| {
        split_test_rows(batch, &mut rows);
        Ok(())
    })?;
    Ok(rows)
}

#[cfg(test)]
fn split_test_rows(mut batch: Bytes, rows: &mut Vec<Bytes>) {
    while let Some(newline) = batch.iter().position(|byte| *byte == b'\n') {
        rows.push(batch.split_to(newline + 1));
    }
    assert!(batch.is_empty(), "parser batches must end on a row boundary");
}

#[cfg(test)]
pub(super) fn flatten_json_document_to_ndjson(bytes: &[u8], json_source_path: &[JsonPathSegment]) -> io::Result<Bytes> {
    let rows = parse_json_document_to_lines(bytes, &JsonSource::from_path(json_source_path.to_vec()))?;
    let total = rows.iter().map(Bytes::len).sum();
    let mut output = Vec::with_capacity(total);
    for row in rows {
        output.extend_from_slice(&row);
    }
    Ok(Bytes::from(output))
}

#[cfg(test)]
pub(super) fn escaped_json_object_with_size(size: usize) -> Vec<u8> {
    const PREFIX: &[u8] = br#"{"value":""#;
    const SUFFIX: &[u8] = br#""}"#;
    const ESCAPED_A: &[u8] = br#"\u0061"#;
    let payload = size
        .checked_sub(PREFIX.len() + SUFFIX.len())
        .expect("fixture size must fit an object");
    let mut input = Vec::with_capacity(size);
    input.extend_from_slice(PREFIX);
    for _ in 0..payload / ESCAPED_A.len() {
        input.extend_from_slice(ESCAPED_A);
    }
    input.extend(std::iter::repeat_n(b'a', payload % ESCAPED_A.len()));
    input.extend_from_slice(SUFFIX);
    assert_eq!(input.len(), size);
    input
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt as _;

    struct ByteAtATime<R>(R);

    impl<R: io::Read> io::Read for ByteAtATime<R> {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            if buffer.is_empty() {
                return Ok(0);
            }
            self.0.read(&mut buffer[..1])
        }
    }

    fn parse_reader(reader: impl io::Read) -> io::Result<Vec<Bytes>> {
        let memory_pool: Arc<dyn MemoryPool> = Arc::new(datafusion::execution::memory_pool::UnboundedMemoryPool::default());
        let reservation = MemoryConsumer::new("JSON document chunk test").register(&memory_pool);
        let mut rows = Vec::new();
        parse_json_document(reader, &JsonSource::default(), &reservation, &AtomicBool::new(false), |batch| {
            split_test_rows(batch, &mut rows);
            Ok(())
        })?;
        Ok(rows)
    }

    #[test]
    fn root_array_plan_preserves_existing_source_semantics() {
        assert_eq!(root_array_plan(&[]).expect("default source"), RootArrayPlan::Each { path_start: 0 });
        assert_eq!(
            root_array_plan(&[JsonPathSegment::ArrayWildcard]).expect("array wildcard source"),
            RootArrayPlan::Each { path_start: 1 }
        );
        assert_eq!(
            root_array_plan(&[JsonPathSegment::ArrayWildcard, JsonPathSegment::ArrayWildcard])
                .expect("double array wildcard source"),
            RootArrayPlan::Each { path_start: 2 }
        );
        assert_eq!(
            root_array_plan(&[JsonPathSegment::Index(2)]).expect("array index source"),
            RootArrayPlan::Index { index: 2, path_start: 1 }
        );
        assert!(
            root_array_plan(&[JsonPathSegment::Key {
                name: "x".to_string(),
                quoted: false,
            }])
            .is_err()
        );
    }

    #[tokio::test]
    async fn first_array_row_is_available_before_input_finishes() {
        let (mut writer, reader) = tokio::io::duplex(128);
        let stream = json_document_ndjson_stream(
            Box::new(reader),
            19,
            JsonSource::default(),
            Arc::new(SelectInputMetrics::default()),
            Arc::new(datafusion::execution::memory_pool::GreedyMemoryPool::new(8 * 1024 * 1024)),
            None,
        )
        .expect("build lazy JSON document stream");
        let write_task = tokio::spawn(async move {
            writer.write_all(b"[{\"id\":1},").await.expect("write first row");
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        });
        tokio::pin!(stream);

        let first = tokio::time::timeout(std::time::Duration::from_secs(1), stream.try_next())
            .await
            .expect("first row should not wait for the document tail")
            .expect("stream should remain valid")
            .expect("first row should be present");
        assert_eq!(first, Bytes::from_static(b"{\"id\":1}\n"));

        write_task.abort();
    }

    #[tokio::test]
    async fn root_object_is_available_when_its_value_finishes() {
        let (mut writer, reader) = tokio::io::duplex(128);
        let stream = json_document_ndjson_stream(
            Box::new(reader),
            16,
            JsonSource::default(),
            Arc::new(SelectInputMetrics::default()),
            Arc::new(datafusion::execution::memory_pool::GreedyMemoryPool::new(8 * 1024 * 1024)),
            None,
        )
        .expect("build lazy JSON document stream");
        let write_task = tokio::spawn(async move {
            writer.write_all(b"{\"id\":1}").await.expect("write root object");
            futures::future::pending::<()>().await;
        });
        tokio::pin!(stream);

        let first = tokio::time::timeout(std::time::Duration::from_secs(1), stream.try_next())
            .await
            .expect("root object should not wait for trailing input")
            .expect("stream should remain valid")
            .expect("root object should be present");
        assert_eq!(first, Bytes::from_static(b"{\"id\":1}\n"));

        write_task.abort();
    }

    #[tokio::test]
    async fn malformed_tail_is_reported_after_completed_rows() {
        let input = br#"[{"id":1},{"id":"#.to_vec();
        let input_len = u64::try_from(input.len()).expect("fixture size should fit in u64");
        let mut stream = json_document_ndjson_stream(
            Box::new(io::Cursor::new(input)),
            input_len,
            JsonSource::default(),
            Arc::new(SelectInputMetrics::default()),
            Arc::new(datafusion::execution::memory_pool::GreedyMemoryPool::new(8 * 1024 * 1024)),
            None,
        )
        .expect("build lazy JSON document stream");

        assert_eq!(
            stream.try_next().await.expect("read completed prefix row"),
            Some(Bytes::from_static(b"{\"id\":1}\n"))
        );
        let error = stream
            .try_next()
            .await
            .expect_err("malformed tail must terminate the stream with an error");
        assert_eq!(
            QueryError::from(datafusion::common::DataFusionError::ObjectStore(Box::new(error))).select_error(),
            SelectError::JsonParsingError
        );
        assert!(stream.try_next().await.expect("error must terminate the stream").is_none());
    }

    #[test]
    fn chunk_boundaries_preserve_escapes_and_utf8() {
        let input = "[\"\\\"\u{1f980}\",{\"name\":\"中\"}]".as_bytes();
        let rows = parse_reader(ByteAtATime(io::Cursor::new(input))).expect("parse split-sensitive JSON");
        assert_eq!(rows.len(), 2);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&rows[0]).expect("first row"),
            serde_json::json!({"_1": "\"🦀"})
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&rows[1]).expect("second row"),
            serde_json::json!({"name": "中"})
        );
    }

    #[test]
    fn multiline_objects_are_compacted_to_one_row() {
        let root = parse_json_document_to_lines(b"{\n  \"id\": 1,\n  \"text\": \"a b\"\n}", &JsonSource::default())
            .expect("parse multiline root object");
        assert_eq!(root, vec![Bytes::from_static(b"{\"id\":1,\"text\":\"a b\"}\n")]);

        let array = parse_json_document_to_lines(b"[{\n  \"id\": 1\n}, {\n  \"id\": 2\n}]", &JsonSource::default())
            .expect("parse multiline array objects");
        assert_eq!(array, vec![Bytes::from_static(b"{\"id\":1}\n"), Bytes::from_static(b"{\"id\":2}\n")]);

        let nested_array =
            parse_json_document_to_lines(b"[[\n  1, 2\n]]", &JsonSource::default()).expect("parse multiline nested array");
        assert_eq!(nested_array, vec![Bytes::from_static(b"{\"_1\":[1,2]}\n")]);
    }

    #[test]
    fn scalar_root_yields_one_record() {
        let rows = parse_json_document_to_lines(b"  42  ", &JsonSource::default()).expect("parse scalar root");
        assert_eq!(rows, vec![Bytes::from_static(b"{\"_1\":42}\n")]);
    }

    #[test]
    fn raw_scalar_output_limit_has_an_exact_boundary() {
        const SCALAR_WRAPPER_BYTES: usize = br#"{"":0}"#.len();
        let alias = "a".repeat(MAX_SELECT_RECORD_BYTES - SCALAR_WRAPPER_BYTES);
        let source = JsonSource::new(Vec::new(), Some(alias));
        let rows = parse_json_document_to_lines(b"0", &source).expect("maximum scalar output record should parse");
        assert_eq!(rows[0].len(), MAX_SELECT_RECORD_BYTES + 1);

        let alias = "a".repeat(MAX_SELECT_RECORD_BYTES + 1 - SCALAR_WRAPPER_BYTES);
        let source = JsonSource::new(Vec::new(), Some(alias));
        let error =
            parse_json_document_to_lines(b"0", &source).expect_err("scalar output above one megabyte must fail while streaming");
        assert!(find_error_source::<SelectError>(&error).is_some_and(|error| error == &SelectError::OverMaxRecordSize));
    }

    #[test]
    fn depth_limit_is_distinct_before_protocol_mapping() {
        let mut at_limit = vec![b'['; JSON_DOCUMENT_MAX_DEPTH];
        at_limit.extend(std::iter::repeat_n(b']', JSON_DOCUMENT_MAX_DEPTH));
        parse_json_document_to_lines(&at_limit, &JsonSource::default())
            .expect("document at the nesting-depth limit should parse");

        let mut input = vec![b'['; JSON_DOCUMENT_MAX_DEPTH + 1];
        input.extend(std::iter::repeat_n(b']', JSON_DOCUMENT_MAX_DEPTH + 1));
        let error = parse_json_document_to_lines(&input, &JsonSource::default()).expect_err("depth limit should fail");
        assert!(find_error_source::<JsonDocumentParseError>(&error).is_some_and(|error| {
            matches!(
                error,
                JsonDocumentParseError::DepthLimit {
                    limit: JSON_DOCUMENT_MAX_DEPTH
                }
            )
        }));
    }

    #[test]
    fn source_path_length_is_not_a_document_depth_limit() {
        let path = (0..JSON_DOCUMENT_MAX_DEPTH * 100)
            .map(|index| JsonPathSegment::Key {
                name: format!("missing_{index}"),
                quoted: false,
            })
            .collect();
        let rows = parse_json_document_to_lines(b"{}", &JsonSource::new(path, None))
            .expect("a shallow document must not inherit the source-path depth");
        assert!(rows.is_empty());
    }

    #[test]
    fn json_whitespace_is_limited_to_the_rfc_tokens() {
        for input in [b"\x0b{}".as_slice(), b"{}\x0c".as_slice(), b"[\x0b]".as_slice()] {
            let error = parse_json_document_to_lines(input, &JsonSource::default())
                .expect_err("vertical tab and form feed are not JSON whitespace");
            assert!(matches!(
                find_error_source::<JsonDocumentParseError>(&error),
                Some(JsonDocumentParseError::Malformed)
            ));
        }
    }

    #[test]
    fn root_array_separator_whitespace_is_not_a_record() {
        let mut empty = Vec::with_capacity(MAX_SELECT_RECORD_BYTES + 3);
        empty.push(b'[');
        empty.extend(std::iter::repeat_n(b' ', MAX_SELECT_RECORD_BYTES + 1));
        empty.push(b']');
        assert!(
            parse_json_document_to_lines(&empty, &JsonSource::default())
                .expect("parse empty array")
                .is_empty()
        );

        let mut input = Vec::with_capacity(MAX_SELECT_RECORD_BYTES + 4);
        input.push(b'[');
        input.extend(escaped_json_object_with_size(MAX_SELECT_RECORD_BYTES));
        input.extend_from_slice(b" ]");
        assert_eq!(
            parse_json_document_to_lines(&input, &JsonSource::default())
                .expect("separator whitespace must not increase the record size")
                .len(),
            1
        );

        let error = parse_json_document_to_lines(b"[1 2]", &JsonSource::default())
            .expect_err("separator whitespace must not merge adjacent values");
        assert!(matches!(
            find_error_source::<JsonDocumentParseError>(&error),
            Some(JsonDocumentParseError::Malformed)
        ));
    }

    #[test]
    fn malformed_open_record_is_not_classified_as_truncated() {
        let error = parse_json_document_to_lines(b"[!", &JsonSource::default())
            .expect_err("an invalid token is malformed, even when the root array is unfinished");
        assert!(matches!(
            find_error_source::<JsonDocumentParseError>(&error),
            Some(JsonDocumentParseError::Malformed)
        ));
    }

    #[test]
    fn raw_input_record_limit_has_exact_boundary() {
        let at_limit = escaped_json_object_with_size(MAX_SELECT_RECORD_BYTES);
        parse_json_document_to_lines(&at_limit, &JsonSource::default()).expect("one-megabyte input record should be accepted");

        let over_limit = escaped_json_object_with_size(MAX_SELECT_RECORD_BYTES + 1);
        let error = parse_json_document_to_lines(&over_limit, &JsonSource::default())
            .expect_err("input record above one megabyte must fail before deserialization");
        assert!(find_error_source::<SelectError>(&error).is_some_and(|error| error == &SelectError::OverMaxRecordSize));
    }

    #[test]
    fn truncated_input_is_distinct_before_protocol_mapping() {
        let error =
            parse_json_document_to_lines(br#"[{"id":1}"#, &JsonSource::default()).expect_err("truncated document should fail");
        assert!(matches!(
            find_error_source::<JsonDocumentParseError>(&error),
            Some(JsonDocumentParseError::Truncated)
        ));
    }

    #[tokio::test]
    async fn large_root_array_uses_bounded_query_memory() {
        const RECORDS: usize = 20_000;
        let mut input = Vec::with_capacity(RECORDS * 9 + 1);
        input.push(b'[');
        for index in 0..RECORDS {
            if index > 0 {
                input.push(b',');
            }
            input.extend_from_slice(br#"{"id":0}"#);
        }
        input.push(b']');
        let input_len = u64::try_from(input.len()).expect("fixture size should fit in u64");

        let memory_pool = Arc::new(datafusion::execution::memory_pool::GreedyMemoryPool::new(8 * 1024 * 1024));
        let mut stream = json_document_ndjson_stream(
            Box::new(io::Cursor::new(input)),
            input_len,
            JsonSource::default(),
            Arc::new(SelectInputMetrics::default()),
            memory_pool.clone(),
            None,
        )
        .expect("build lazy JSON document stream");
        let mut rows = 0;
        let mut batches = 0;
        while let Some(batch) = stream.try_next().await.expect("stream large root array") {
            rows += batch.iter().filter(|byte| **byte == b'\n').count();
            batches += 1;
        }

        assert_eq!(rows, RECORDS);
        assert!(batches < 100, "small records should cross the worker boundary in bounded batches");
        assert_eq!(memory_pool.reserved(), 0);
    }

    #[tokio::test]
    async fn legal_large_record_fits_the_default_query_memory_limit() {
        let value = "x".repeat(600 * 1024);
        let input = format!(r#"{{"value":"{value}"}}"#).into_bytes();
        let memory_pool = Arc::new(datafusion::execution::memory_pool::GreedyMemoryPool::new(64 * 1024 * 1024));
        let output = json_document_ndjson_stream(
            Box::new(io::Cursor::new(input.clone())),
            u64::try_from(input.len()).expect("fixture size should fit in u64"),
            JsonSource::default(),
            Arc::new(SelectInputMetrics::default()),
            memory_pool.clone(),
            None,
        )
        .expect("build large-record JSON document stream")
        .try_collect::<Vec<_>>()
        .await
        .expect("a legal record below one megabyte should fit the default pool")
        .concat();

        assert_eq!(output.len(), input.len() + 1);
        assert_eq!(memory_pool.reserved(), 0);
    }

    #[test]
    fn non_expanding_path_releases_dom_memory_before_blocking_emit() {
        let input = format!("[{{\"payload\":\"{}\"}}]", "x".repeat(512 * 1024));
        let source = JsonSource::from_path(vec![
            JsonPathSegment::ArrayWildcard,
            JsonPathSegment::Key {
                name: "payload".to_string(),
                quoted: false,
            },
        ]);
        let memory_pool = Arc::new(datafusion::execution::memory_pool::GreedyMemoryPool::new(64 * 1024 * 1024));
        let query_memory_pool: Arc<dyn MemoryPool> = memory_pool.clone();
        let reservation = MemoryConsumer::new("JSON DOM emit overlap test").register(&query_memory_pool);
        let mut observed_reservation = None;

        parse_json_document(io::Cursor::new(input), &source, &reservation, &AtomicBool::new(false), |batch| {
            observed_reservation = Some(memory_pool.reserved());
            assert!(batch.ends_with(b"\n"));
            Ok(())
        })
        .expect("non-expanding source path should stream");

        assert!(
            observed_reservation.is_some_and(|bytes| bytes < 8 * 1024 * 1024),
            "DOM reservation must shrink before a potentially blocking output send"
        );
        assert_eq!(memory_pool.reserved(), 0);
    }

    #[tokio::test]
    async fn parser_worker_failure_is_a_terminal_internal_error() {
        let (row_tx, row_rx) = mpsc::channel(1);
        drop(row_tx);
        let (parse_result_tx, parse_result_rx) = oneshot::channel();
        drop(parse_result_tx);
        let memory_pool: Arc<dyn MemoryPool> = Arc::new(datafusion::execution::memory_pool::UnboundedMemoryPool::default());
        let reservation = MemoryConsumer::new("JSON worker failure test").register(&memory_pool);
        let resources = Arc::new(JsonDocumentPipelineResources {
            _channel_reservation: reservation,
            _query_guard: None,
        });
        let mut pipeline = JsonDocumentPipeline {
            receiver: row_rx,
            read_task: Some(SpawnedTask::spawn(async {})),
            parse_result: parse_result_rx,
            done: false,
            cancellation: Arc::new(AtomicBool::new(false)),
            _resources: resources,
        };

        let error = pipeline
            .try_next()
            .await
            .expect_err("a failed parser task must not become successful EOF");
        assert_eq!(
            QueryError::from(datafusion::common::DataFusionError::ObjectStore(Box::new(error))).select_error(),
            SelectError::InternalError
        );
        assert!(
            pipeline
                .try_next()
                .await
                .expect("terminal error should close the stream")
                .is_none()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn trackerless_parser_waits_for_global_admission() {
        let permits = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            JSON_DOCUMENT_PARSER_ADMISSION
                .acquire_many(u32::try_from(*JSON_DOCUMENT_PARSER_THREADS).expect("parser thread limit should fit in u32")),
        )
        .await
        .expect("other parser tests should release global admission")
        .expect("global parser admission should remain open");
        let memory_pool = Arc::new(datafusion::execution::memory_pool::GreedyMemoryPool::new(8 * 1024 * 1024));
        let mut stream = json_document_ndjson_stream(
            Box::new(io::Cursor::new(b"{}")),
            2,
            JsonSource::default(),
            Arc::new(SelectInputMetrics::default()),
            memory_pool.clone(),
            None,
        )
        .expect("build trackerless JSON document stream");

        {
            let next = stream.try_next();
            futures::pin_mut!(next);
            assert!(futures::poll!(next.as_mut()).is_pending());
        }
        assert_eq!(memory_pool.reserved(), 0, "waiting streams must not reserve parser memory");

        drop(permits);
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(1), stream.try_next())
                .await
                .expect("parser should start after admission is released")
                .expect("trackerless parser should remain valid"),
            Some(Bytes::from_static(b"{}\n"))
        );
        drop(stream);
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while memory_pool.reserved() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("trackerless parser should release query memory");
    }

    #[test]
    fn cancelled_row_build_keeps_only_completed_output() {
        let memory_pool: Arc<dyn MemoryPool> = Arc::new(datafusion::execution::memory_pool::UnboundedMemoryPool::default());
        let reservation = MemoryConsumer::new("JSON cancellation output test").register(&memory_pool);
        let cancellation = AtomicBool::new(true);
        let source = JsonSource::default();
        let mut decoder = JsonDocumentDecoder::new(&source, &reservation, &cancellation, |_batch| Ok(()));
        decoder.output.extend_from_slice(b"{\"done\":true}\n");
        let completed = decoder.output.clone();

        decoder.record.extend_from_slice(b"0");
        let error = decoder
            .append_raw_value(false)
            .expect_err("raw scalar construction must observe cancellation");
        assert_eq!(error.kind(), io::ErrorKind::Interrupted);
        assert_eq!(decoder.output, completed);

        let error = decoder
            .append_json_value(serde_json::json!(1), false)
            .expect_err("DOM scalar construction must observe cancellation");
        assert_eq!(error.kind(), io::ErrorKind::Interrupted);
        assert_eq!(decoder.output, completed);
    }

    #[tokio::test]
    async fn scalar_alias_expansion_is_reserved_per_streamed_row() {
        const EXPECTED_SCALAR_ALIAS_MULTIPLIER: usize = 2;
        assert_eq!(JSON_SCALAR_COLUMN_MEMORY_RESERVATION_MULTIPLIER, EXPECTED_SCALAR_ALIAS_MULTIPLIER);
        let alias = "alias".repeat(40_000);
        let source = JsonSource::new(vec![JsonPathSegment::ArrayWildcard], Some(alias.clone()));
        let alias_reservation = alias
            .len()
            .checked_mul(EXPECTED_SCALAR_ALIAS_MULTIPLIER)
            .expect("alias reservation should fit");
        let insufficient = json_document_channel_reservation_bytes() + alias_reservation - 1;
        let mut stream = json_document_ndjson_stream(
            Box::new(io::Cursor::new(b"[0]".to_vec())),
            3,
            source.clone(),
            Arc::new(SelectInputMetrics::default()),
            Arc::new(datafusion::execution::memory_pool::GreedyMemoryPool::new(insufficient)),
            None,
        )
        .expect("build constrained JSON document stream");
        let error = stream
            .try_next()
            .await
            .expect_err("unreserved scalar alias expansion must fail");
        assert_eq!(
            QueryError::from(datafusion::common::DataFusionError::ObjectStore(Box::new(error))).select_error(),
            SelectError::ResourceExhausted
        );

        let memory_pool = Arc::new(datafusion::execution::memory_pool::GreedyMemoryPool::new(insufficient + 1024 * 1024));
        let rows = json_document_ndjson_stream(
            Box::new(io::Cursor::new(b"[0]".to_vec())),
            3,
            source,
            Arc::new(SelectInputMetrics::default()),
            memory_pool.clone(),
            None,
        )
        .expect("build JSON document stream")
        .try_collect::<Vec<_>>()
        .await
        .expect("reserved scalar alias expansion should stream");
        assert_eq!(rows.len(), 1);
        assert_eq!(memory_pool.reserved(), 0);
    }

    #[tokio::test]
    async fn dropping_active_stream_releases_parser_memory() {
        let (mut writer, reader) = tokio::io::duplex(128);
        let memory_pool = Arc::new(datafusion::execution::memory_pool::GreedyMemoryPool::new(8 * 1024 * 1024));
        let mut stream = json_document_ndjson_stream(
            Box::new(reader),
            19,
            JsonSource::default(),
            Arc::new(SelectInputMetrics::default()),
            memory_pool.clone(),
            None,
        )
        .expect("build active JSON document stream");
        let writer_task = tokio::spawn(async move {
            writer.write_all(b"[{\"id\":1},").await.expect("write first row");
            futures::future::pending::<()>().await;
        });

        assert!(stream.try_next().await.expect("read first row").is_some());
        drop(stream);
        writer_task.abort();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while memory_pool.reserved() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("active parser cancellation should release memory");
    }
}
