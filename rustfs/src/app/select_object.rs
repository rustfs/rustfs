#[cfg(test)]
use super::storage_api::select_object::StorageError;
use super::storage_api::select_object::options::get_opts;
use super::storage_api::select_object::request_context::spawn_traced;
use super::storage_api::select_object::sse::{SseKmsPrincipal, authorize_sse_kms_object_read, project_sse_read_response_headers};
use super::storage_api::select_object::{
    StoragePrepareSelectObjectSnapshotError, StorageSelectObjectSnapshot, get_validated_store, validate_sse_headers_for_read,
    validate_ssec_for_read,
};
use crate::app::runtime_sources::current_s3select_db;
use crate::error::ApiError;
use bytes::{Bytes, BytesMut};
use datafusion::arrow::{
    array::{Array, ListLikeArray, MapArray, cast::AsArray},
    datatypes::{
        ArrowNativeType, DataType, FieldRef, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type,
        UInt64Type,
    },
    error::ArrowError,
    json::writer::{EncoderOptions, NullableEncoder, make_encoder},
    record_batch::RecordBatch,
    util::display::{ArrayFormatter, FormatOptions},
};
#[cfg(test)]
use datafusion::common::DataFusionError;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use http::{HeaderMap, StatusCode, header::RANGE};
use rustfs_s3select_api::{
    QueryError, SelectError, SelectInputMetrics,
    object_store::{INVALID_SCAN_RANGE_MESSAGE, validate_scan_range_bounds},
    query::{Context, Query},
};
use rustfs_s3select_query::instance::s3_select_query_timeout;
use s3s::dto::{
    CSVOutput, CompressionType, ContinuationEvent, EndEvent, ExpressionType, FileHeaderInfo, InputSerialization, JSONInput,
    JSONOutput, JSONType, OutputSerialization, Progress, ProgressEvent, QuoteFields, RecordsEvent, SelectObjectContentEvent,
    SelectObjectContentEventStream, SelectObjectContentInput, SelectObjectContentOutput, SelectObjectContentRequest, Stats,
    StatsEvent,
};
use s3s::{S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use std::{
    fmt,
    future::poll_fn,
    io::{self, Write},
    ops::Range,
    pin::Pin,
    sync::Arc,
    time::Duration,
};
use tokio::sync::mpsc;
use tokio::time::{Instant, Interval, MissedTickBehavior, Sleep, timeout_at};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::PollSender;
use tracing::info;

const MAX_SELECT_EXPRESSION_BYTES: usize = 256 * 1024;
const MAX_COMPAT_EVENT_STREAM_MESSAGE_BYTES: usize = 128 * 1024 - 256;
const RECORDS_EVENT_STREAM_OVERHEAD_BYTES: usize = 101;
const RECORDS_CHUNK_TARGET: usize = MAX_COMPAT_EVENT_STREAM_MESSAGE_BYTES - RECORDS_EVENT_STREAM_OVERHEAD_BYTES;
const ENCODE_TURN_TARGET_BYTES: usize = 64 * 1024;
const MAX_ENCODE_ROWS_PER_TURN: usize = 1024;
const MAX_SELECT_OUTPUT_RECORD_BYTES: usize = 1024 * 1024;
const RECORDS_FLUSH_INTERVAL: Duration = Duration::from_millis(500);
const CONTINUATION_INTERVAL: Duration = Duration::from_secs(1);
const PROGRESS_INTERVAL: Duration = Duration::from_secs(60);
const DATA_SOURCE_PATH_UNSUPPORTED_CODE: &str = "DataSourcePathUnsupported";
const INVALID_QUERY_CODE: &str = "InvalidQuery";
const PARSE_SELECT_FAILURE_CODE: &str = "ParseSelectFailure";
const BUSY_MESSAGE: &str = "The service is unavailable. Try again later.";
const EMPTY_SELECT_EXPRESSION_MESSAGE: &str = "empty SQL expression";
const SLOW_DOWN_MESSAGE: &str = "Reduce your request rate.";
const UNSUPPORTED_SQL_STRUCTURE_MESSAGE: &str = "We encountered an unsupported SQL structure. Check the SQL Reference.";
const OVER_MAX_RECORD_SIZE_MESSAGE: &str =
    "The length of a record in the input or result is greater than the maxCharsPerRecord limit of 1 MB.";

#[derive(Clone, Debug)]
struct SelectValidation {
    output_format: SelectOutputFormat,
    progress_enabled: bool,
    reports_input_metrics: bool,
}

#[derive(Clone, Debug)]
enum SelectOutputFormat {
    Csv(CSVOutput),
    Json(JSONOutput),
}

enum SelectProducerOutcome {
    Terminal(S3Result<SelectObjectContentEvent>),
    ReceiverClosed,
}

enum TerminalRecordsMode {
    Complete,
    PrefixBeforeError,
}

struct SelectEventChannel {
    tx: mpsc::Sender<S3Result<SelectObjectContentEvent>>,
    terminal_records_permit: Option<mpsc::OwnedPermit<S3Result<SelectObjectContentEvent>>>,
    terminal_permit: mpsc::OwnedPermit<S3Result<SelectObjectContentEvent>>,
}

trait SelectSnapshotFence {
    fn ensure_snapshot_valid(&self) -> S3Result<()>;
}

impl SelectSnapshotFence for Arc<StorageSelectObjectSnapshot> {
    fn ensure_snapshot_valid(&self) -> S3Result<()> {
        self.ensure_valid().map_err(internal_select_error)
    }
}

pub async fn execute_select_object_content(
    req: S3Request<SelectObjectContentInput>,
) -> S3Result<S3Response<SelectObjectContentOutput>> {
    let read_principal = SseKmsPrincipal::from_request(&req);
    let mut input = req.input;
    let validation = validate_select_request(&req.headers, &mut input)?;
    log_select_request_summary(&input, &validation);
    let query_timeout = s3_select_query_timeout();
    let query_deadline = Instant::now() + query_timeout;
    let input = Arc::new(input);
    let db = timeout_at(query_deadline, current_s3select_db((*input).clone(), false))
        .await
        .map_err(|_| select_query_timeout_error(query_timeout.as_secs()))?
        .map_err(map_query_error_to_s3)?;
    let admission = db.try_reserve_query().map_err(map_query_error_to_s3)?;
    let (snapshot, response_headers) = timeout_at(
        query_deadline,
        prepare_select_object_snapshot(&req.headers, &input, read_principal.as_ref()),
    )
    .await
    .map_err(|_| select_query_timeout_error(query_timeout.as_secs()))??;
    validate_scan_range_for_object_size(&input.request, snapshot.logical_size())?;
    let snapshot = Arc::new(snapshot);
    let query =
        Query::new_with_snapshot(Context { input: input.clone() }, input.request.expression.clone(), Arc::clone(&snapshot));
    let query_handle = timeout_at(query_deadline, db.execute_admitted(&query, admission))
        .await
        .map_err(|_| select_query_timeout_error(query_timeout.as_secs()))?
        .map_err(map_query_error_to_s3)?;
    let input_metrics = Arc::clone(query_handle.query().input_metrics());
    let output = query_handle
        .result()
        .into_record_batch_stream()
        .map_err(map_query_error_to_s3)?;

    let (tx, rx) = mpsc::channel::<S3Result<SelectObjectContentEvent>>(10);
    let terminal_records_permit = tx
        .clone()
        .try_reserve_owned()
        .map_err(|_| map_select_error_to_s3(&SelectError::InternalError))?;
    let terminal_permit = tx
        .clone()
        .try_reserve_owned()
        .map_err(|_| map_select_error_to_s3(&SelectError::InternalError))?;
    let mut response = S3Response::new(SelectObjectContentOutput {
        payload: Some(SelectObjectContentEventStream::new(ReceiverStream::new(rx))),
    });
    response.headers = response_headers;
    spawn_traced(async move {
        send_select_events_until_deadline(
            output,
            SelectEventChannel {
                tx,
                terminal_records_permit: Some(terminal_records_permit),
                terminal_permit,
            },
            validation,
            input_metrics,
            query_deadline,
            query_timeout.as_secs(),
            snapshot,
        )
        .await;
    });

    Ok(response)
}

async fn send_select_events_until_deadline<L: SelectSnapshotFence>(
    output: SendableRecordBatchStream,
    mut event_channel: SelectEventChannel,
    validation: SelectValidation,
    input_metrics: Arc<SelectInputMetrics>,
    deadline: Instant,
    timeout_seconds: u64,
    snapshot_lease: L,
) {
    let outcome = send_select_events(
        output,
        &mut event_channel,
        validation,
        input_metrics,
        deadline,
        timeout_seconds,
        &snapshot_lease,
    )
    .await;
    if let SelectProducerOutcome::Terminal(event) = outcome {
        drop(event_channel.terminal_records_permit.take());
        event_channel.terminal_permit.send(event);
    }
    drop(snapshot_lease);
}

async fn send_select_events(
    mut output: SendableRecordBatchStream,
    event_channel: &mut SelectEventChannel,
    validation: SelectValidation,
    input_metrics: Arc<SelectInputMetrics>,
    deadline: Instant,
    timeout_seconds: u64,
    snapshot_fence: &impl SelectSnapshotFence,
) -> SelectProducerOutcome {
    let SelectValidation {
        output_format,
        progress_enabled,
        reports_input_metrics,
    } = validation;
    let mut encoder = SelectOutputEncoder::new(output_format);
    let mut progress = SelectProgress::new(reports_input_metrics.then_some(input_metrics));
    let started_at = Instant::now();
    let records_flush = tokio::time::sleep_until(deadline);
    tokio::pin!(records_flush);
    let mut records_flush_armed = false;
    let mut continuation = delayed_select_interval(started_at, CONTINUATION_INTERVAL);
    let mut progress_interval = progress_enabled.then(|| delayed_select_interval(started_at, PROGRESS_INTERVAL));
    let deadline_sleep = tokio::time::sleep_until(deadline);
    tokio::pin!(deadline_sleep);
    let tx = event_channel.tx.clone();
    let mut periodic_sender = PollSender::new(tx.clone());
    let receiver_closed = tx.closed();
    tokio::pin!(receiver_closed);

    let mut records_buffer = BytesMut::new();
    let mut pending_event: Option<SelectObjectContentEvent> = None;
    let mut pending_batch: Option<RecordBatch> = None;
    let mut pending_batch_offset = 0;
    let mut continuation_due = false;
    let mut progress_due = false;

    loop {
        if pending_event.is_some() {
            periodic_sender.abort_send();
        }
        let periodic_due = progress_due || continuation_due;
        let finishing_success = matches!(pending_event.as_ref(), Some(SelectObjectContentEvent::Stats(_)));
        let progress_armed = progress_interval.is_some();

        tokio::select! {
            biased;

            _ = &mut receiver_closed => return SelectProducerOutcome::ReceiverClosed,

            _ = &mut deadline_sleep => {
                return finish_select_with_error(
                    select_query_timeout_error(timeout_seconds),
                    event_channel,
                    &mut pending_event,
                    &mut records_buffer,
                    &mut progress,
                );
            }

            _ = tick_optional_interval(&mut progress_interval), if progress_armed && !progress_due && !finishing_success => {
                progress_due = true;
            }

            _ = continuation.tick(), if !continuation_due && !finishing_success => {
                continuation_due = true;
            }

            permit = tx.reserve(), if pending_event.is_some() => {
                let permit = match permit {
                    Ok(permit) => permit,
                    Err(_) => return SelectProducerOutcome::ReceiverClosed,
                };
                let Some(event) = pending_event.take() else {
                    return SelectProducerOutcome::Terminal(Err(map_select_error_to_s3(&SelectError::InternalError)));
                };
                let finishes_successfully = matches!(&event, SelectObjectContentEvent::Stats(_));
                if finishes_successfully
                    && let Err(error) = snapshot_fence.ensure_snapshot_valid()
                {
                    return SelectProducerOutcome::Terminal(Err(error));
                }
                let returned = records_payload_len(&event);
                permit.send(Ok(event));
                if let Some(returned) = returned {
                    progress.add_returned(returned);
                }
                if finishes_successfully {
                    return SelectProducerOutcome::Terminal(Ok(SelectObjectContentEvent::End(EndEvent::default())));
                }
                if !periodic_due {
                    schedule_buffered_records(
                        &mut records_buffer,
                        records_flush.as_mut(),
                        &mut records_flush_armed,
                        &mut pending_event,
                        deadline,
                    );
                }
            }

            _ = &mut records_flush, if records_flush_armed && !finishing_success && pending_event.is_none() => {
                records_flush_armed = false;
                records_flush.as_mut().reset(deadline);
                pending_event = take_records_payload(&mut records_buffer).map(records_event);
            }

            permit = poll_fn(|cx| periodic_sender.poll_reserve(cx)), if periodic_due && pending_event.is_none() => {
                if permit.is_err() {
                    return SelectProducerOutcome::ReceiverClosed;
                }
                if progress_due {
                    if periodic_sender
                        .send_item(Ok(SelectObjectContentEvent::Progress(ProgressEvent {
                            details: Some(progress.to_progress()),
                        })))
                        .is_err()
                    {
                        return SelectProducerOutcome::ReceiverClosed;
                    }
                    progress_due = false;
                    if let Some(interval) = progress_interval.as_mut() {
                        interval.reset();
                    }
                } else {
                    if periodic_sender
                        .send_item(Ok(SelectObjectContentEvent::Cont(ContinuationEvent::default())))
                        .is_err()
                    {
                        return SelectProducerOutcome::ReceiverClosed;
                    }
                    continuation_due = false;
                    continuation.reset();
                }
                if !progress_due && !continuation_due {
                    schedule_buffered_records(
                        &mut records_buffer,
                        records_flush.as_mut(),
                        &mut records_flush_armed,
                        &mut pending_event,
                        deadline,
                    );
                }
            }

            _ = tokio::task::yield_now(), if pending_batch.is_some() && pending_event.is_none() && !periodic_due => {
                let Some(batch) = pending_batch.as_ref() else {
                    return SelectProducerOutcome::Terminal(Err(map_select_error_to_s3(&SelectError::InternalError)));
                };
                let remaining_rows = batch.num_rows().saturating_sub(pending_batch_offset);
                if remaining_rows == 0 {
                    pending_batch = None;
                    pending_batch_offset = 0;
                    continue;
                }
                let encoded_rows = match encode_batch_turn(
                    &mut encoder,
                    batch,
                    pending_batch_offset,
                    &mut records_buffer,
                ) {
                    Ok(encoded_rows) => encoded_rows,
                    Err(error) => {
                        return finish_select_with_error(
                            error,
                            event_channel,
                            &mut pending_event,
                            &mut records_buffer,
                            &mut progress,
                        );
                    }
                };
                if encoded_rows == 0 {
                    return SelectProducerOutcome::Terminal(Err(map_select_error_to_s3(&SelectError::InternalError)));
                }
                pending_batch_offset += encoded_rows;
                if pending_batch_offset == batch.num_rows() {
                    pending_batch = None;
                    pending_batch_offset = 0;
                }
                schedule_buffered_records(
                    &mut records_buffer,
                    records_flush.as_mut(),
                    &mut records_flush_armed,
                    &mut pending_event,
                    deadline,
                );
            }

            result = output.next(), if !finishing_success && pending_event.is_none() && pending_batch.is_none() && !periodic_due => {
                match result {
                    Some(Ok(batch)) => {
                        pending_batch = Some(batch);
                        pending_batch_offset = 0;
                    }
                    Some(Err(error)) => {
                        return finish_select_with_error(
                            map_query_error_to_s3(error.into()),
                            event_channel,
                            &mut pending_event,
                            &mut records_buffer,
                            &mut progress,
                        );
                    }
                    None => {
                        if let Err(error) = snapshot_fence.ensure_snapshot_valid() {
                            return finish_select_with_error(
                                error,
                                event_channel,
                                &mut pending_event,
                                &mut records_buffer,
                                &mut progress,
                            );
                        }
                        if let Err(error) = flush_terminal_records(
                            event_channel,
                            &mut pending_event,
                            &mut records_buffer,
                            &mut progress,
                            TerminalRecordsMode::Complete,
                        ) {
                            return SelectProducerOutcome::Terminal(Err(error));
                        }
                        pending_event = Some(SelectObjectContentEvent::Stats(StatsEvent {
                            details: Some(progress.to_stats()),
                        }));
                    }
                }
            }

        }
    }
}

fn delayed_select_interval(started_at: Instant, period: Duration) -> Interval {
    let mut interval = tokio::time::interval_at(started_at + period, period);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    interval
}

fn encode_batch_turn(
    encoder: &mut SelectOutputEncoder,
    batch: &RecordBatch,
    offset: usize,
    buffer: &mut BytesMut,
) -> S3Result<usize> {
    let remaining_rows = batch.num_rows().saturating_sub(offset);
    if remaining_rows == 0 {
        return Ok(0);
    }

    let original_len = buffer.len();
    let candidate_rows = remaining_rows.min(MAX_ENCODE_ROWS_PER_TURN);
    let output_limit = if candidate_rows == 1 {
        MAX_SELECT_OUTPUT_RECORD_BYTES
    } else {
        ENCODE_TURN_TARGET_BYTES
    };
    match encoder.encode_batch_limited(batch, offset..offset + candidate_rows, buffer, output_limit) {
        Ok(encoded_rows) if encoded_rows > 0 => return Ok(encoded_rows),
        Ok(_) if candidate_rows > 1 => {}
        Ok(_) => return Err(over_max_record_size_error()),
        Err(error) => {
            buffer.truncate(original_len);
            return Err(error);
        }
    }

    match encoder.encode_batch_limited(batch, offset..offset + 1, buffer, MAX_SELECT_OUTPUT_RECORD_BYTES) {
        Ok(1) => Ok(1),
        Ok(_) => {
            buffer.truncate(original_len);
            Err(over_max_record_size_error())
        }
        Err(error) => {
            buffer.truncate(original_len);
            Err(error)
        }
    }
}

async fn tick_optional_interval(interval: &mut Option<Interval>) {
    match interval {
        Some(interval) => {
            interval.tick().await;
        }
        None => std::future::pending().await,
    }
}

fn schedule_buffered_records(
    buffer: &mut BytesMut,
    mut flush: Pin<&mut Sleep>,
    flush_armed: &mut bool,
    pending_event: &mut Option<SelectObjectContentEvent>,
    idle_deadline: Instant,
) {
    if pending_event.is_some() || buffer.is_empty() {
        return;
    }
    if buffer.len() >= RECORDS_CHUNK_TARGET {
        *flush_armed = false;
        flush.as_mut().reset(idle_deadline);
        *pending_event = Some(records_event(buffer.split_to(RECORDS_CHUNK_TARGET).freeze()));
    } else if !*flush_armed {
        flush.as_mut().reset(Instant::now() + RECORDS_FLUSH_INTERVAL);
        *flush_armed = true;
    }
}

fn take_records_payload(buffer: &mut BytesMut) -> Option<Bytes> {
    (!buffer.is_empty()).then(|| buffer.split().freeze())
}

fn records_event(payload: Bytes) -> SelectObjectContentEvent {
    SelectObjectContentEvent::Records(RecordsEvent { payload: Some(payload) })
}

fn records_payload_len(event: &SelectObjectContentEvent) -> Option<usize> {
    match event {
        SelectObjectContentEvent::Records(records) => records.payload.as_ref().map(Bytes::len),
        _ => None,
    }
}

fn flush_terminal_records(
    event_channel: &mut SelectEventChannel,
    pending_event: &mut Option<SelectObjectContentEvent>,
    records_buffer: &mut BytesMut,
    progress: &mut SelectProgress,
    mode: TerminalRecordsMode,
) -> S3Result<()> {
    let pending = pending_event.take();
    let pending_payload = match pending {
        Some(SelectObjectContentEvent::Records(records)) => records.payload,
        _ => None,
    };
    let payload = pending_payload.or_else(|| take_records_payload(records_buffer));
    if matches!(mode, TerminalRecordsMode::PrefixBeforeError) {
        records_buffer.clear();
    }
    let permit = event_channel.terminal_records_permit.take();
    let Some(payload) = payload else {
        drop(permit);
        return Ok(());
    };
    let payload = match mode {
        TerminalRecordsMode::Complete if payload.len() > RECORDS_CHUNK_TARGET => {
            return Err(map_select_error_to_s3(&SelectError::InternalError));
        }
        TerminalRecordsMode::PrefixBeforeError if payload.len() > RECORDS_CHUNK_TARGET => payload.slice(..RECORDS_CHUNK_TARGET),
        _ => payload,
    };
    let Some(permit) = permit else {
        return Err(map_select_error_to_s3(&SelectError::InternalError));
    };
    let returned = payload.len();
    permit.send(Ok(records_event(payload)));
    progress.add_returned(returned);
    Ok(())
}

fn finish_select_with_error(
    error: S3Error,
    event_channel: &mut SelectEventChannel,
    pending_event: &mut Option<SelectObjectContentEvent>,
    records_buffer: &mut BytesMut,
    progress: &mut SelectProgress,
) -> SelectProducerOutcome {
    if let Err(error) = flush_terminal_records(
        event_channel,
        pending_event,
        records_buffer,
        progress,
        TerminalRecordsMode::PrefixBeforeError,
    ) {
        return SelectProducerOutcome::Terminal(Err(error));
    }
    SelectProducerOutcome::Terminal(Err(error))
}

fn validate_select_request(headers: &http::HeaderMap, input: &mut SelectObjectContentInput) -> S3Result<SelectValidation> {
    if headers.contains_key(RANGE) {
        return Err(S3Error::new(S3ErrorCode::UnsupportedRangeHeader));
    }
    if input.request.expression.len() > MAX_SELECT_EXPRESSION_BYTES {
        return Err(S3Error::new(S3ErrorCode::ExpressionTooLong));
    }
    if input.request.expression_type.as_str() != ExpressionType::SQL {
        return Err(S3Error::new(S3ErrorCode::InvalidExpressionType));
    }

    normalize_input_serialization(&mut input.request.input_serialization)?;
    validate_scan_range(&input.request)?;

    let output_format = normalize_output_serialization(&mut input.request.output_serialization)?;
    if input.request.expression.trim().is_empty() {
        return Err(map_select_error_to_s3(&SelectError::ParseSelectFailure {
            message: EMPTY_SELECT_EXPRESSION_MESSAGE.to_string(),
        }));
    }
    let progress_enabled = input
        .request
        .request_progress
        .as_ref()
        .and_then(|progress| progress.enabled)
        .unwrap_or(false);

    Ok(SelectValidation {
        output_format,
        progress_enabled,
        reports_input_metrics: input.request.input_serialization.parquet.is_none(),
    })
}

fn normalize_input_serialization(input: &mut InputSerialization) -> S3Result<()> {
    let format_count =
        usize::from(input.csv.is_some()) + usize::from(input.json.is_some()) + usize::from(input.parquet.is_some());
    if format_count == 0 {
        return Err(S3Error::new(S3ErrorCode::MissingRequiredParameter));
    }
    if format_count > 1 {
        return Err(S3Error::new(S3ErrorCode::ObjectSerializationConflict));
    }

    if let Some(compression) = input.compression_type.as_ref() {
        match compression.as_str() {
            CompressionType::NONE => {}
            CompressionType::GZIP | CompressionType::BZIP2 => {
                return Err(s3_error!(
                    NotImplemented,
                    "SelectObjectContent currently supports only uncompressed input"
                ));
            }
            _ => return Err(map_select_error_to_s3(&SelectError::InvalidCompressionFormat)),
        }
    }
    input.compression_type = Some(CompressionType::from_static(CompressionType::NONE));

    if let Some(csv) = input.csv.as_mut() {
        if csv.allow_quoted_record_delimiter.unwrap_or(false) {
            return Err(s3_error!(
                NotImplemented,
                "CSV AllowQuotedRecordDelimiter is not supported by SelectObjectContent"
            ));
        }
        let file_header_info = csv
            .file_header_info
            .get_or_insert_with(|| FileHeaderInfo::from_static(FileHeaderInfo::NONE));
        if !matches!(
            file_header_info.as_str(),
            FileHeaderInfo::NONE | FileHeaderInfo::USE | FileHeaderInfo::IGNORE
        ) {
            return Err(S3Error::with_message(
                S3ErrorCode::InvalidFileHeaderInfo,
                "The FileHeaderInfo value is not valid. Only NONE, USE, and IGNORE are supported.",
            ));
        }
        validate_single_byte(csv.comments.as_deref(), S3ErrorCode::InvalidRequestParameter)?;
        validate_single_byte(csv.quote_character.as_deref(), S3ErrorCode::InvalidRequestParameter)?;
        validate_single_byte(csv.quote_escape_character.as_deref(), S3ErrorCode::InvalidRequestParameter)?;
        validate_input_record_delimiter(csv.record_delimiter.as_deref())?;
        validate_input_delimiter_pair(csv.field_delimiter.as_deref(), csv.record_delimiter.as_deref())?;
    }

    if let Some(json) = input.json.as_mut() {
        let json_type = json.type_.get_or_insert_with(|| JSONType::from_static(JSONType::LINES));
        if !matches!(json_type.as_str(), JSONType::DOCUMENT | JSONType::LINES) {
            return Err(S3Error::new(S3ErrorCode::InvalidJsonType));
        }
    }

    Ok(())
}

fn normalize_output_serialization(output: &mut OutputSerialization) -> S3Result<SelectOutputFormat> {
    let format_count = usize::from(output.csv.is_some()) + usize::from(output.json.is_some());
    if format_count == 0 {
        return Err(S3Error::new(S3ErrorCode::MissingRequiredParameter));
    }
    if format_count > 1 {
        return Err(S3Error::new(S3ErrorCode::ObjectSerializationConflict));
    }

    if let Some(csv) = output.csv.as_ref() {
        validate_single_byte(csv.field_delimiter.as_deref(), S3ErrorCode::InvalidRequestParameter)?;
        validate_single_byte(csv.quote_character.as_deref(), S3ErrorCode::InvalidRequestParameter)?;
        validate_single_byte(csv.quote_escape_character.as_deref(), S3ErrorCode::InvalidRequestParameter)?;
        validate_output_record_delimiter(csv.record_delimiter.as_deref())?;
        if let Some(quote_fields) = csv.quote_fields.as_ref()
            && !matches!(quote_fields.as_str(), QuoteFields::ALWAYS | QuoteFields::ASNEEDED)
        {
            return Err(S3Error::new(S3ErrorCode::InvalidQuoteFields));
        }
        return Ok(SelectOutputFormat::Csv(csv.clone()));
    }

    let json = output.json.as_ref().expect("checked exactly one output format");
    Ok(SelectOutputFormat::Json(json.clone()))
}

fn validate_scan_range(request: &SelectObjectContentRequest) -> S3Result<()> {
    let Some(scan_range) = request.scan_range.as_ref() else {
        return Ok(());
    };
    let start = scan_range.start;
    let end = scan_range.end;
    if start.is_none() && end.is_none() {
        return Err(invalid_scan_range_error());
    }
    if validate_scan_range_bounds(start, end, u64::MAX).is_err() {
        return Err(invalid_scan_range_error());
    }
    validate_scan_range_protocol(request).map_err(|_| invalid_scan_range_error())?;
    Ok(())
}

fn validate_scan_range_protocol(request: &SelectObjectContentRequest) -> Result<(), ()> {
    let input_serialization = &request.input_serialization;

    let supports_scan_range = match (
        input_serialization.csv.as_ref(),
        input_serialization.json.as_ref(),
        input_serialization.parquet.as_ref(),
    ) {
        (Some(_), None, None) => true,
        (None, Some(json), None) if !is_json_document(json) => true,
        (None, None, Some(_)) => true,
        _ => false,
    };

    if supports_scan_range { Ok(()) } else { Err(()) }
}

fn validate_scan_range_for_object_size(request: &SelectObjectContentRequest, object_size: u64) -> S3Result<()> {
    let Some(scan_range) = request.scan_range.as_ref() else {
        return Ok(());
    };
    if validate_scan_range_bounds(scan_range.start, scan_range.end, object_size).is_err() {
        return Err(invalid_scan_range_error());
    }
    Ok(())
}

fn invalid_scan_range_error() -> S3Error {
    S3Error::with_message(S3ErrorCode::InvalidRequestParameter, INVALID_SCAN_RANGE_MESSAGE.to_string())
}

fn validate_single_byte(value: Option<&str>, code: S3ErrorCode) -> S3Result<()> {
    if let Some(value) = value
        && value.len() != 1
    {
        return Err(S3Error::new(code));
    }
    Ok(())
}

fn validate_output_record_delimiter(value: Option<&str>) -> S3Result<()> {
    if let Some(value) = value
        && value.len() != 1
        && value != "\r\n"
    {
        return Err(S3Error::new(S3ErrorCode::InvalidRequestParameter));
    }
    Ok(())
}

fn validate_input_record_delimiter(value: Option<&str>) -> S3Result<()> {
    if let Some(value) = value
        && !(1..=2).contains(&value.len())
    {
        return Err(S3Error::new(S3ErrorCode::InvalidRequestParameter));
    }
    Ok(())
}

fn validate_input_delimiter_pair(field_delimiter: Option<&str>, record_delimiter: Option<&str>) -> S3Result<()> {
    let field_delimiter = field_delimiter.unwrap_or(",");
    let record_delimiter = record_delimiter.unwrap_or("\n");
    let normalized_field_delimiter = if field_delimiter.len() > 1 { "," } else { field_delimiter };
    let normalized_record_delimiter = if record_delimiter.len() == 2 {
        "\r\n"
    } else {
        record_delimiter
    };
    if record_delimiter.starts_with(field_delimiter) || normalized_record_delimiter.contains(normalized_field_delimiter) {
        return Err(S3Error::new(S3ErrorCode::InvalidRequestParameter));
    }
    Ok(())
}

async fn prepare_select_object_snapshot(
    headers: &http::HeaderMap,
    input: &SelectObjectContentInput,
    read_principal: Option<&SseKmsPrincipal>,
) -> S3Result<(StorageSelectObjectSnapshot, HeaderMap)> {
    let opts = get_opts(&input.bucket, &input.key, None, None, headers)
        .await
        .map_err(ApiError::from)?;
    let store = get_validated_store(&input.bucket).await?;
    let snapshot = store
        .prepare_select_object_snapshot(&input.bucket, &input.key, headers, &opts)
        .await
        .map_err(map_prepare_snapshot_error)?;
    let info = snapshot.object_info();
    validate_sse_headers_for_read(&info.user_defined, headers)?;
    validate_ssec_for_read(&info.user_defined, input.sse_customer_key.as_ref(), input.sse_customer_key_md5.as_ref())?;
    authorize_sse_kms_object_read(read_principal, &info.user_defined).await?;
    let response_headers = project_sse_read_response_headers(
        &info.user_defined,
        input.sse_customer_algorithm.as_ref(),
        input.sse_customer_key_md5.as_ref(),
    )?;
    Ok((snapshot, response_headers))
}

fn map_prepare_snapshot_error(err: StoragePrepareSelectObjectSnapshotError) -> S3Error {
    match err {
        StoragePrepareSelectObjectSnapshotError::Storage(err) => {
            let mut s3_error: S3Error = ApiError::from(err).into();
            if s3_error.code() == &S3ErrorCode::InternalError {
                s3_error.set_message(SelectError::InternalError.to_string());
            }
            s3_error
        }
        err => internal_select_error(err),
    }
}

fn log_select_request_summary(input: &SelectObjectContentInput, validation: &SelectValidation) {
    let output_format = match &validation.output_format {
        SelectOutputFormat::Csv(_) => "csv",
        SelectOutputFormat::Json(_) => "json",
    };
    let input_format = if input.request.input_serialization.csv.is_some() {
        "csv"
    } else if input.request.input_serialization.json.is_some() {
        "json"
    } else {
        "parquet"
    };
    info!(
        bucket = %input.bucket,
        key = %input.key,
        expression_len = input.request.expression.len(),
        input_format,
        output_format,
        has_scan_range = input.request.scan_range.is_some(),
        has_sse_customer_key = input.sse_customer_key.is_some(),
        "handle select_object_content"
    );
}

struct SelectOutputEncoder {
    format: SelectOutputFormat,
}

impl SelectOutputEncoder {
    fn new(format: SelectOutputFormat) -> Self {
        Self { format }
    }

    fn encode_batch_limited(
        &mut self,
        batch: &RecordBatch,
        rows: Range<usize>,
        buffer: &mut BytesMut,
        max_bytes: usize,
    ) -> S3Result<usize> {
        match &self.format {
            SelectOutputFormat::Csv(config) => encode_csv_batch_limited(batch, rows, config, buffer, max_bytes),
            SelectOutputFormat::Json(config) => encode_json_batch_limited(batch, rows, config, buffer, max_bytes),
        }
    }
}

#[cfg(test)]
fn encode_csv_batch(batch: &RecordBatch, config: &CSVOutput, buffer: &mut BytesMut) -> S3Result<()> {
    if encode_csv_batch_limited(batch, 0..batch.num_rows(), config, buffer, usize::MAX)? == batch.num_rows() {
        Ok(())
    } else {
        Err(internal_select_error(io::Error::other("S3 Select output length overflow")))
    }
}

fn encode_csv_batch_limited(
    batch: &RecordBatch,
    rows: Range<usize>,
    config: &CSVOutput,
    buffer: &mut BytesMut,
    max_bytes: usize,
) -> S3Result<usize> {
    let options = FormatOptions::default();
    let mut formatters = Vec::new();
    let field_delimiter = config.field_delimiter.as_deref().unwrap_or(",").as_bytes()[0];
    let quote = config.quote_character.as_deref().unwrap_or("\"").as_bytes()[0];
    let quote_escape = config.quote_escape_character.as_deref().unwrap_or("\"").as_bytes()[0];
    let record_delimiter = config.record_delimiter.as_deref().unwrap_or("\n").as_bytes();
    let quote_all = config
        .quote_fields
        .as_ref()
        .is_some_and(|quote_fields| quote_fields.as_str() == QuoteFields::ALWAYS);
    let mut output = LimitedBytesWriter::new(buffer, max_bytes);
    let mut field = BoundedText::default();
    let mut encoded_rows = 0;
    for row in rows {
        let row_start = output.checkpoint();
        for column in 0..batch.num_columns() {
            if column > 0 && output.write_all(&[field_delimiter]).is_err() {
                output.rollback_to(row_start);
                return Ok(encoded_rows);
            }
            if formatters.len() == column {
                let array = batch.column(column);
                if array.data_type().is_nested() {
                    return Err(internal_select_error(datafusion::arrow::error::ArrowError::CsvError(format!(
                        "Nested type {} is not supported in CSV",
                        array.data_type()
                    ))));
                }
                formatters.push(ArrayFormatter::try_new(array.as_ref(), &options).map_err(internal_select_error)?);
            }
            let formatter = formatters
                .get(column)
                .ok_or_else(|| internal_select_error(io::Error::other("missing S3 Select CSV formatter")))?;
            field.reset(output.remaining());
            let result = formatter.value(row).write(&mut field);
            if field.limit_exceeded {
                output.rollback_to(row_start);
                return Ok(encoded_rows);
            }
            result.map_err(internal_select_error)?;
            if write_csv_field(&mut output, field.value.as_bytes(), field_delimiter, quote, quote_escape, quote_all).is_err() {
                output.rollback_to(row_start);
                return Ok(encoded_rows);
            }
        }
        if output.write_all(record_delimiter).is_err() {
            output.rollback_to(row_start);
            return Ok(encoded_rows);
        }
        encoded_rows += 1;
    }
    Ok(encoded_rows)
}

fn write_csv_field(
    output: &mut LimitedBytesWriter<'_>,
    field: &[u8],
    delimiter: u8,
    quote: u8,
    quote_escape: u8,
    quote_all: bool,
) -> io::Result<()> {
    let quote_field = quote_all || csv_field_needs_quotes(field, delimiter, quote);
    if !quote_field {
        return output.write_all(field);
    }

    output.write_all(&[quote])?;
    let mut start = 0;
    while let Some(relative) = field[start..].iter().position(|byte| *byte == quote) {
        let position = start + relative;
        output.write_all(&field[start..position])?;
        output.write_all(&[quote_escape, quote])?;
        start = position + 1;
    }
    output.write_all(&field[start..])?;
    output.write_all(&[quote])
}

fn csv_field_needs_quotes(field: &[u8], delimiter: u8, quote: u8) -> bool {
    if field.is_empty() {
        return false;
    }
    if field == b"\\."
        || field
            .iter()
            .copied()
            .any(|byte| matches!(byte, b'\r' | b'\n') || byte == delimiter || byte == quote)
    {
        return true;
    }
    std::str::from_utf8(field)
        .ok()
        .and_then(|value| value.chars().next())
        .is_some_and(char::is_whitespace)
}

#[derive(Default)]
struct BoundedText {
    value: String,
    max_bytes: usize,
    limit_exceeded: bool,
}

impl BoundedText {
    fn reset(&mut self, max_bytes: usize) {
        self.value.clear();
        self.max_bytes = max_bytes;
        self.limit_exceeded = false;
    }
}

impl fmt::Write for BoundedText {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        let Some(len) = self.value.len().checked_add(value.len()) else {
            self.limit_exceeded = true;
            return Err(fmt::Error);
        };
        if len > self.max_bytes {
            self.limit_exceeded = true;
            return Err(fmt::Error);
        }
        self.value.push_str(value);
        Ok(())
    }
}

struct LimitedJsonValueEncoder<'a> {
    array: &'a dyn Array,
    options: &'a EncoderOptions,
    kind: LimitedJsonValueKind<'a>,
}

enum LimitedJsonValueKind<'a> {
    Scalar(NullableEncoder<'a>),
    List {
        array: &'a dyn ListLikeArray,
        value_field: &'a FieldRef,
        value_array: &'a dyn Array,
        values: Option<Box<LimitedJsonValueEncoder<'a>>>,
    },
    Struct {
        fields: &'a [FieldRef],
        arrays: &'a [datafusion::arrow::array::ArrayRef],
        values: Vec<LimitedJsonValueEncoder<'a>>,
    },
    Dictionary {
        value_index: Box<dyn Fn(usize) -> usize + 'a>,
        value_field: &'a FieldRef,
        value_array: &'a dyn Array,
        values: Option<Box<LimitedJsonValueEncoder<'a>>>,
    },
    Indexed {
        value_index: Box<dyn Fn(usize) -> usize + 'a>,
        value_field: &'a FieldRef,
        value_array: &'a dyn Array,
        values: Option<Box<LimitedJsonValueEncoder<'a>>>,
    },
    Map {
        array: &'a MapArray,
        field: &'a FieldRef,
        keys: Option<Box<LimitedJsonValueEncoder<'a>>>,
        values: Option<Box<LimitedJsonValueEncoder<'a>>>,
    },
}

impl<'a> LimitedJsonValueEncoder<'a> {
    fn try_new(field: &'a FieldRef, array: &'a dyn Array, options: &'a EncoderOptions) -> Result<Self, ArrowError> {
        macro_rules! dictionary {
            ($key:ty) => {{
                let dictionary = array.as_dictionary::<$key>();
                LimitedJsonValueKind::Dictionary {
                    value_index: Box::new(move |row| dictionary.keys().value(row).as_usize()),
                    value_field: field,
                    value_array: dictionary.values().as_ref(),
                    values: None,
                }
            }};
        }
        macro_rules! run_end_encoded {
            ($run_end:ty) => {{
                let run = array.as_run::<$run_end>();
                LimitedJsonValueKind::Indexed {
                    value_index: Box::new(move |row| run.get_physical_index(row)),
                    value_field: field,
                    value_array: run.values().as_ref(),
                    values: None,
                }
            }};
        }

        let kind = match array.data_type() {
            DataType::List(value_field) => {
                let list = array.as_list::<i32>();
                LimitedJsonValueKind::List {
                    array: list,
                    value_field,
                    value_array: list.values().as_ref(),
                    values: None,
                }
            }
            DataType::LargeList(value_field) => {
                let list = array.as_list::<i64>();
                LimitedJsonValueKind::List {
                    array: list,
                    value_field,
                    value_array: list.values().as_ref(),
                    values: None,
                }
            }
            DataType::ListView(value_field) => {
                let list = array.as_list_view::<i32>();
                LimitedJsonValueKind::List {
                    array: list,
                    value_field,
                    value_array: list.values().as_ref(),
                    values: None,
                }
            }
            DataType::LargeListView(value_field) => {
                let list = array.as_list_view::<i64>();
                LimitedJsonValueKind::List {
                    array: list,
                    value_field,
                    value_array: list.values().as_ref(),
                    values: None,
                }
            }
            DataType::FixedSizeList(value_field, _) => {
                let list = array.as_fixed_size_list();
                LimitedJsonValueKind::List {
                    array: list,
                    value_field,
                    value_array: list.values().as_ref(),
                    values: None,
                }
            }
            DataType::Struct(fields) => LimitedJsonValueKind::Struct {
                fields: fields.as_ref(),
                arrays: array.as_struct().columns(),
                values: Vec::new(),
            },
            DataType::Dictionary(key_type, _) => match key_type.as_ref() {
                DataType::Int8 => dictionary!(Int8Type),
                DataType::Int16 => dictionary!(Int16Type),
                DataType::Int32 => dictionary!(Int32Type),
                DataType::Int64 => dictionary!(Int64Type),
                DataType::UInt8 => dictionary!(UInt8Type),
                DataType::UInt16 => dictionary!(UInt16Type),
                DataType::UInt32 => dictionary!(UInt32Type),
                DataType::UInt64 => dictionary!(UInt64Type),
                key_type => {
                    return Err(ArrowError::JsonError(format!(
                        "Unsupported dictionary key type for JSON encoding: {key_type:?}"
                    )));
                }
            },
            DataType::RunEndEncoded(run_ends, _) => match run_ends.data_type() {
                DataType::Int16 => run_end_encoded!(Int16Type),
                DataType::Int32 => run_end_encoded!(Int32Type),
                DataType::Int64 => run_end_encoded!(Int64Type),
                run_end_type => {
                    return Err(ArrowError::JsonError(format!(
                        "Unsupported run-end type for JSON encoding: {run_end_type:?}"
                    )));
                }
            },
            DataType::Map(_, _) => {
                let map = array.as_map();
                if !matches!(map.keys().data_type(), DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View) {
                    return Err(ArrowError::JsonError(format!(
                        "Only UTF8 keys supported by JSON MapArray Writer: got {:?}",
                        map.keys().data_type()
                    )));
                }
                if map.keys().null_count() != 0 {
                    return Err(ArrowError::InvalidArgumentError("Encountered nulls in MapArray keys".to_string()));
                }
                if map.entries().nulls().is_some_and(|nulls| nulls.null_count() != 0) {
                    return Err(ArrowError::InvalidArgumentError("Encountered nulls in MapArray entries".to_string()));
                }
                LimitedJsonValueKind::Map {
                    array: map,
                    field,
                    keys: None,
                    values: None,
                }
            }
            _ => LimitedJsonValueKind::Scalar(make_encoder(field, array, options)?),
        };
        Ok(Self { array, options, kind })
    }

    fn encode(&mut self, row: usize, output: &mut LimitedBytesWriter<'_>, scratch: &mut Vec<u8>) -> Result<bool, ArrowError> {
        if self.array.is_null(row) {
            return Ok(write_limited(output, b"null"));
        }
        self.encode_non_null(row, output, scratch)
    }

    fn encode_non_null(
        &mut self,
        row: usize,
        output: &mut LimitedBytesWriter<'_>,
        scratch: &mut Vec<u8>,
    ) -> Result<bool, ArrowError> {
        let array = self.array;
        // NullArray has no physical null buffer, including behind a dictionary index.
        if matches!(array.data_type(), DataType::Null) {
            return Ok(write_limited(output, b"null"));
        }
        let options = self.options;
        match &mut self.kind {
            LimitedJsonValueKind::Scalar(encoder) => Ok(write_json_scalar_limited(array, encoder, row, output, scratch)),
            LimitedJsonValueKind::List {
                array,
                value_field,
                value_array,
                values,
            } => {
                if !write_limited(output, b"[") {
                    return Ok(false);
                }
                for (index, value_row) in array.element_range(row).enumerate() {
                    if index > 0 && !write_limited(output, b",") {
                        return Ok(false);
                    }
                    let values = Self::lazy_value_encoder(values, value_field, *value_array, options)?;
                    if !values.encode(value_row, output, scratch)? {
                        return Ok(false);
                    }
                }
                Ok(write_limited(output, b"]"))
            }
            LimitedJsonValueKind::Struct { fields, arrays, values } => {
                if !write_limited(output, b"{") {
                    return Ok(false);
                }
                for (index, field) in fields.iter().enumerate() {
                    if (index > 0 && !write_limited(output, b","))
                        || !write_json_string_limited(output, field.name())
                        || !write_limited(output, b":")
                    {
                        return Ok(false);
                    }
                    if values.len() == index {
                        values.push(Self::try_new(field, arrays[index].as_ref(), options)?);
                    }
                    let value = values
                        .get_mut(index)
                        .ok_or_else(|| ArrowError::JsonError("S3 Select JSON encoder state is inconsistent".to_string()))?;
                    if !value.encode(row, output, scratch)? {
                        return Ok(false);
                    }
                }
                Ok(write_limited(output, b"}"))
            }
            // Arrow checks dictionary key nulls but delegates value nulls to the value encoder.
            LimitedJsonValueKind::Dictionary {
                value_index,
                value_field,
                value_array,
                values,
            } => Self::lazy_value_encoder(values, value_field, *value_array, options)?.encode_non_null(
                value_index(row),
                output,
                scratch,
            ),
            LimitedJsonValueKind::Indexed {
                value_index,
                value_field,
                value_array,
                values,
            } => Self::lazy_value_encoder(values, value_field, *value_array, options)?.encode(value_index(row), output, scratch),
            LimitedJsonValueKind::Map {
                array,
                field,
                keys,
                values,
            } => {
                if !write_limited(output, b"{") {
                    return Ok(false);
                }
                let offsets = array.value_offsets();
                let start = offsets[row].as_usize();
                let end = offsets[row + 1].as_usize();
                for (index, value_row) in (start..end).enumerate() {
                    if index > 0 && !write_limited(output, b",") {
                        return Ok(false);
                    }
                    let keys = Self::lazy_value_encoder(keys, field, array.keys(), options)?;
                    if !keys.encode(value_row, output, scratch)? || !write_limited(output, b":") {
                        return Ok(false);
                    }
                    let values = Self::lazy_value_encoder(values, field, array.values(), options)?;
                    if !values.encode(value_row, output, scratch)? {
                        return Ok(false);
                    }
                }
                Ok(write_limited(output, b"}"))
            }
        }
    }

    fn lazy_value_encoder<'b>(
        slot: &'b mut Option<Box<LimitedJsonValueEncoder<'a>>>,
        field: &'a FieldRef,
        array: &'a dyn Array,
        options: &'a EncoderOptions,
    ) -> Result<&'b mut LimitedJsonValueEncoder<'a>, ArrowError> {
        if slot.is_none() {
            *slot = Some(Box::new(Self::try_new(field, array, options)?));
        }
        slot.as_deref_mut()
            .ok_or_else(|| ArrowError::JsonError("S3 Select JSON encoder state is inconsistent".to_string()))
    }
}

fn write_json_scalar_limited(
    array: &dyn Array,
    encoder: &mut NullableEncoder<'_>,
    row: usize,
    output: &mut LimitedBytesWriter<'_>,
    scratch: &mut Vec<u8>,
) -> bool {
    match array.data_type() {
        DataType::Utf8 => write_json_string_limited(output, array.as_string::<i32>().value(row)),
        DataType::LargeUtf8 => write_json_string_limited(output, array.as_string::<i64>().value(row)),
        DataType::Utf8View => write_json_string_limited(output, array.as_string_view().value(row)),
        DataType::Binary => write_json_binary_limited(output, array.as_binary::<i32>().value(row), scratch),
        DataType::LargeBinary => write_json_binary_limited(output, array.as_binary::<i64>().value(row), scratch),
        DataType::BinaryView => write_json_binary_limited(output, array.as_binary_view().value(row), scratch),
        DataType::FixedSizeBinary(_) => write_json_binary_limited(output, array.as_fixed_size_binary().value(row), scratch),
        _ => {
            scratch.clear();
            encoder.encode(row, scratch);
            write_limited(output, scratch)
        }
    }
}

fn write_json_string_limited(output: &mut LimitedBytesWriter<'_>, value: &str) -> bool {
    if value.len().checked_add(2).is_none_or(|minimum| minimum > output.remaining()) {
        return false;
    }
    serde_json::to_writer(output, value).is_ok()
}

fn write_json_binary_limited(output: &mut LimitedBytesWriter<'_>, value: &[u8], scratch: &mut Vec<u8>) -> bool {
    let Some(encoded_len) = value.len().checked_mul(2).and_then(|bytes| bytes.checked_add(2)) else {
        return false;
    };
    if encoded_len > output.remaining() || !write_limited(output, b"\"") {
        return false;
    }
    const HEX: &[u8; 16] = b"0123456789abcdef";
    const INPUT_CHUNK_BYTES: usize = 2048;
    for chunk in value.chunks(INPUT_CHUNK_BYTES) {
        scratch.clear();
        scratch.reserve(chunk.len() * 2);
        for byte in chunk {
            scratch.push(HEX[(byte >> 4) as usize]);
            scratch.push(HEX[(byte & 0x0f) as usize]);
        }
        if !write_limited(output, scratch) {
            return false;
        }
    }
    write_limited(output, b"\"")
}

fn write_limited(output: &mut LimitedBytesWriter<'_>, bytes: &[u8]) -> bool {
    output.write_all(bytes).is_ok()
}

fn json_record_delimiter(config: &JSONOutput) -> &[u8] {
    if let Some(delimiter) = config.record_delimiter.as_deref() {
        delimiter.as_bytes()
    } else {
        b"\n"
    }
}

#[cfg(test)]
fn encode_json_batch(batch: &RecordBatch, config: &JSONOutput, buffer: &mut BytesMut) -> S3Result<()> {
    if encode_json_batch_limited(batch, 0..batch.num_rows(), config, buffer, usize::MAX)? == batch.num_rows() {
        Ok(())
    } else {
        Err(internal_select_error(io::Error::other("S3 Select output length overflow")))
    }
}

fn encode_json_batch_limited(
    batch: &RecordBatch,
    rows: Range<usize>,
    config: &JSONOutput,
    buffer: &mut BytesMut,
    max_bytes: usize,
) -> S3Result<usize> {
    let options = EncoderOptions::default().with_explicit_nulls(true);
    let schema = batch.schema();
    let fields = schema.fields();
    let arrays = batch.columns();
    let mut values = Vec::new();
    let delimiter = json_record_delimiter(config);
    let mut output = LimitedBytesWriter::new(buffer, max_bytes);
    let mut scratch = Vec::new();
    let mut encoded_rows = 0;
    for row in rows {
        let row_start = output.checkpoint();
        if !write_limited(&mut output, b"{") {
            output.rollback_to(row_start);
            return Ok(encoded_rows);
        }
        for (index, field) in fields.iter().enumerate() {
            if (index > 0 && !write_limited(&mut output, b","))
                || !write_json_string_limited(&mut output, field.name())
                || !write_limited(&mut output, b":")
            {
                output.rollback_to(row_start);
                return Ok(encoded_rows);
            }
            if values.len() == index {
                values.push(
                    LimitedJsonValueEncoder::try_new(field, arrays[index].as_ref(), &options).map_err(internal_select_error)?,
                );
            }
            let value = values
                .get_mut(index)
                .ok_or_else(|| internal_select_error(io::Error::other("S3 Select JSON encoder state is inconsistent")))?;
            if !value.encode(row, &mut output, &mut scratch).map_err(internal_select_error)? {
                output.rollback_to(row_start);
                return Ok(encoded_rows);
            }
        }
        if !write_limited(&mut output, b"}") || !write_limited(&mut output, delimiter) {
            output.rollback_to(row_start);
            return Ok(encoded_rows);
        }
        encoded_rows += 1;
    }
    Ok(encoded_rows)
}

struct LimitedBytesWriter<'a> {
    buffer: &'a mut BytesMut,
    max_bytes: usize,
    written: usize,
}

impl<'a> LimitedBytesWriter<'a> {
    fn new(buffer: &'a mut BytesMut, max_bytes: usize) -> Self {
        Self {
            buffer,
            max_bytes,
            written: 0,
        }
    }

    fn remaining(&self) -> usize {
        self.max_bytes.saturating_sub(self.written)
    }

    fn checkpoint(&self) -> usize {
        self.buffer.len()
    }

    fn rollback_to(&mut self, checkpoint: usize) {
        self.buffer.truncate(checkpoint);
    }
}

impl Write for LimitedBytesWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let Some(written) = self.written.checked_add(bytes.len()) else {
            return Err(io::Error::other("S3 Select output length overflow"));
        };
        if written > self.max_bytes {
            return Err(io::Error::other("S3 Select encode turn limit exceeded"));
        }
        self.buffer.extend_from_slice(bytes);
        self.written = written;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct SelectProgress {
    input_metrics: Option<Arc<SelectInputMetrics>>,
    bytes_returned: u64,
}

impl SelectProgress {
    fn new(input_metrics: Option<Arc<SelectInputMetrics>>) -> Self {
        Self {
            input_metrics,
            bytes_returned: 0,
        }
    }

    fn add_returned(&mut self, bytes: usize) {
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        self.bytes_returned = self.bytes_returned.saturating_add(bytes);
    }

    fn to_progress(&self) -> Progress {
        let input = self.input_metrics.as_ref().map(|metrics| metrics.snapshot());
        Progress {
            bytes_processed: input.map(|metrics| clamp_i64(metrics.bytes_processed)),
            bytes_returned: Some(clamp_i64(self.bytes_returned)),
            bytes_scanned: input.map(|metrics| clamp_i64(metrics.bytes_scanned)),
        }
    }

    fn to_stats(&self) -> Stats {
        let input = self.input_metrics.as_ref().map(|metrics| metrics.snapshot());
        Stats {
            bytes_processed: input.map(|metrics| clamp_i64(metrics.bytes_processed)),
            bytes_returned: Some(clamp_i64(self.bytes_returned)),
            bytes_scanned: input.map(|metrics| clamp_i64(metrics.bytes_scanned)),
        }
    }
}

fn clamp_i64(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}

fn map_query_error_to_s3(err: QueryError) -> S3Error {
    let select_error = err.select_error();
    map_select_error_to_s3(&select_error)
}

fn map_select_error_to_s3(err: &SelectError) -> S3Error {
    match err {
        SelectError::InvalidCompressionFormat => S3Error::with_message(S3ErrorCode::InvalidCompressionFormat, err.to_string()),
        SelectError::InvalidDataSource => S3Error::with_message(S3ErrorCode::InvalidDataSource, err.to_string()),
        SelectError::TruncatedInput => S3Error::with_message(S3ErrorCode::TruncatedInput, err.to_string()),
        SelectError::CsvParsingError => S3Error::with_message(S3ErrorCode::CSVParsingError, err.to_string()),
        SelectError::JsonParsingError => S3Error::with_message(S3ErrorCode::JSONParsingError, err.to_string()),
        SelectError::ParquetParsingError => S3Error::with_message(S3ErrorCode::ParquetParsingError, err.to_string()),
        SelectError::ParseSelectFailure { message } => custom_bad_request(PARSE_SELECT_FAILURE_CODE, message.clone()),
        SelectError::InvalidQuery => custom_bad_request(INVALID_QUERY_CODE, err.to_string()),
        SelectError::InvalidDataType => S3Error::with_message(S3ErrorCode::InvalidDataType, err.to_string()),
        SelectError::IncorrectSqlFunctionArgumentType => {
            S3Error::with_message(S3ErrorCode::IncorrectSqlFunctionArgumentType, err.to_string())
        }
        SelectError::DataSourcePathUnsupported => custom_bad_request(DATA_SOURCE_PATH_UNSUPPORTED_CODE, err.to_string()),
        SelectError::UnsupportedSqlStructure { .. } => {
            S3Error::with_message(S3ErrorCode::UnsupportedSqlStructure, UNSUPPORTED_SQL_STRUCTURE_MESSAGE)
        }
        SelectError::UnsupportedSqlOperation => S3Error::with_message(S3ErrorCode::UnsupportedSqlOperation, err.to_string()),
        SelectError::EvaluatorBindingDoesNotExist => {
            S3Error::with_message(S3ErrorCode::EvaluatorBindingDoesNotExist, err.to_string())
        }
        SelectError::AmbiguousFieldName => S3Error::with_message(S3ErrorCode::AmbiguousFieldName, err.to_string()),
        SelectError::InvalidScanRange => {
            S3Error::with_message(S3ErrorCode::InvalidRequestParameter, INVALID_SCAN_RANGE_MESSAGE.to_string())
        }
        SelectError::QueryConcurrencyLimit => S3Error::with_message(S3ErrorCode::SlowDown, SLOW_DOWN_MESSAGE),
        SelectError::QueryTimeout { .. } | SelectError::ResourceExhausted => {
            S3Error::with_message(S3ErrorCode::Busy, BUSY_MESSAGE)
        }
        SelectError::BucketNotFound => S3Error::with_message(S3ErrorCode::NoSuchBucket, err.to_string()),
        SelectError::ObjectNotFound => S3Error::with_message(S3ErrorCode::NoSuchKey, err.to_string()),
        SelectError::Canceled | SelectError::InternalError => {
            S3Error::with_message(S3ErrorCode::InternalError, SelectError::InternalError.to_string())
        }
    }
}

fn internal_select_error(_error: impl std::error::Error + Send + Sync + 'static) -> S3Error {
    map_select_error_to_s3(&SelectError::InternalError)
}

fn over_max_record_size_error() -> S3Error {
    S3Error::with_message(S3ErrorCode::OverMaxRecordSize, OVER_MAX_RECORD_SIZE_MESSAGE)
}

fn custom_bad_request(code: &'static str, message: String) -> S3Error {
    let mut err = S3Error::with_message(S3ErrorCode::Custom(code.into()), message);
    err.set_status_code(StatusCode::BAD_REQUEST);
    err
}

fn select_query_timeout_error(seconds: u64) -> S3Error {
    map_query_error_to_s3(SelectError::QueryTimeout { seconds }.into())
}

fn is_json_document(json: &JSONInput) -> bool {
    json.type_
        .as_ref()
        .is_some_and(|json_type| json_type.as_str() == JSONType::DOCUMENT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::{
        arrow::{
            array::{
                Array, ArrayRef, BinaryArray, BinaryViewArray, DictionaryArray, FixedSizeBinaryArray, Int32Array,
                LargeBinaryArray, LargeListArray, LargeListViewArray, LargeStringArray, ListArray, ListViewArray, MapArray,
                NullArray, RunArray, StringArray, StringDictionaryBuilder, StringViewArray, StructArray,
                builder::{BooleanBuilder, FixedSizeListBuilder, Int32Builder, ListBuilder},
            },
            datatypes::{DataType, Field, Int32Type, Schema},
            error::ArrowError,
            json::writer::{LineDelimited, WriterBuilder},
        },
        physical_plan::stream::RecordBatchStreamAdapter,
        sql::sqlparser::parser::ParserError,
    };
    use rustfs_test_utils::TestECStoreEnv;
    use s3s::dto::{CSVInput, ParquetInput, ScanRange};

    fn event_stream_headers(mut bytes: &[u8]) -> Vec<Vec<(String, String)>> {
        let mut messages = Vec::new();
        while !bytes.is_empty() {
            assert!(bytes.len() >= 16, "event-stream message is truncated");
            let total_len = u32::from_be_bytes(bytes[0..4].try_into().expect("event-stream total length")) as usize;
            let headers_len = u32::from_be_bytes(bytes[4..8].try_into().expect("event-stream headers length")) as usize;
            assert!(total_len >= 16 && total_len <= bytes.len(), "invalid event-stream message length");
            assert!(12 + headers_len <= total_len - 4, "invalid event-stream headers length");

            let mut headers = &bytes[12..12 + headers_len];
            let mut decoded = Vec::new();
            while !headers.is_empty() {
                let name_len = headers[0] as usize;
                assert!(headers.len() >= name_len + 4, "event-stream header is truncated");
                let name = std::str::from_utf8(&headers[1..1 + name_len])
                    .expect("event-stream header name should be UTF-8")
                    .to_string();
                assert_eq!(headers[1 + name_len], 7, "expected an event-stream string header");
                let value_len = u16::from_be_bytes(
                    headers[2 + name_len..4 + name_len]
                        .try_into()
                        .expect("event-stream header value length"),
                ) as usize;
                assert!(headers.len() >= name_len + 4 + value_len, "event-stream header value is truncated");
                let value = std::str::from_utf8(&headers[4 + name_len..4 + name_len + value_len])
                    .expect("event-stream header value should be UTF-8")
                    .to_string();
                decoded.push((name, value));
                headers = &headers[4 + name_len + value_len..];
            }
            messages.push(decoded);
            bytes = &bytes[total_len..];
        }
        messages
    }

    async fn http_xml_error(error: S3Error) -> (StatusCode, String) {
        let response = error.to_http_response().expect("S3 error should serialize to HTTP");
        let status = response.status();
        let body = http_body_util::BodyExt::collect(response.into_body())
            .await
            .expect("S3 error body should be readable")
            .to_bytes();
        let body = std::str::from_utf8(&body).expect("S3 error XML should be UTF-8").to_string();
        (status, body)
    }

    struct LeaseDropSignal(Option<tokio::sync::oneshot::Sender<()>>);

    impl Drop for LeaseDropSignal {
        fn drop(&mut self) {
            if let Some(tx) = self.0.take() {
                let _ = tx.send(());
            }
        }
    }

    impl SelectSnapshotFence for LeaseDropSignal {
        fn ensure_snapshot_valid(&self) -> S3Result<()> {
            Ok(())
        }
    }

    struct FailingSnapshotFence;

    impl SelectSnapshotFence for FailingSnapshotFence {
        fn ensure_snapshot_valid(&self) -> S3Result<()> {
            Err(S3Error::with_message(S3ErrorCode::InternalError, "snapshot lease was lost"))
        }
    }

    struct FailsAfterFirstSnapshotFence(std::sync::atomic::AtomicUsize);

    impl SelectSnapshotFence for FailsAfterFirstSnapshotFence {
        fn ensure_snapshot_valid(&self) -> S3Result<()> {
            if self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed) == 0 {
                Ok(())
            } else {
                Err(S3Error::with_message(S3ErrorCode::InternalError, "snapshot lease was lost"))
            }
        }
    }

    fn lease_drop_signal() -> (LeaseDropSignal, tokio::sync::oneshot::Receiver<()>) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        (LeaseDropSignal(Some(tx)), rx)
    }

    #[tokio::test]
    async fn storage_snapshot_fence_forwards_lost_lease() {
        let env = TestECStoreEnv::builder()
            .prefix("select_snapshot_fence_adapter")
            .build()
            .await;
        env.make_bucket("select-snapshot-fence-adapter", false).await;
        env.put_object_bytes("select-snapshot-fence-adapter", "input.csv", b"value\nold\n".to_vec())
            .await;
        let snapshot = env
            .prepare_select_object_snapshot("select-snapshot-fence-adapter", "input.csv")
            .await;
        snapshot.mark_lost_for_test();

        let error = SelectSnapshotFence::ensure_snapshot_valid(&snapshot)
            .expect_err("production fence adapter must reject a lost storage snapshot");

        assert_eq!(error.code(), &S3ErrorCode::InternalError);
        assert_eq!(error.message(), Some("An internal error occurred."));
        assert!(error.source().is_none());
    }

    fn base_input() -> SelectObjectContentInput {
        SelectObjectContentInput {
            bucket: "bucket".to_string(),
            expected_bucket_owner: None,
            key: "object.csv".to_string(),
            sse_customer_algorithm: None,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            request: SelectObjectContentRequest {
                expression: "SELECT * FROM s3object".to_string(),
                expression_type: ExpressionType::from_static(ExpressionType::SQL),
                input_serialization: InputSerialization {
                    csv: Some(CSVInput::default()),
                    compression_type: None,
                    json: None,
                    parquet: None,
                },
                output_serialization: OutputSerialization {
                    csv: Some(CSVOutput::default()),
                    json: None,
                },
                request_progress: None,
                scan_range: None,
            },
        }
    }

    fn csv_validation() -> SelectValidation {
        SelectValidation {
            output_format: SelectOutputFormat::Csv(CSVOutput::default()),
            progress_enabled: false,
            reports_input_metrics: true,
        }
    }

    fn pending_output() -> SendableRecordBatchStream {
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::new(Schema::empty()),
            futures::stream::pending::<Result<RecordBatch, DataFusionError>>(),
        ))
    }

    fn large_pending_output(chunks: usize) -> SendableRecordBatchStream {
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, false)]));
        let value = "x".repeat(RECORDS_CHUNK_TARGET * chunks);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(vec![value]))])
            .expect("test record batch should be valid");
        Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async move { Ok::<_, DataFusionError>(batch) })
                .chain(futures::stream::pending::<Result<RecordBatch, DataFusionError>>()),
        ))
    }

    fn spawn_test_producer(
        output: SendableRecordBatchStream,
        channel_capacity: usize,
    ) -> (
        tokio::task::JoinHandle<()>,
        mpsc::Receiver<S3Result<SelectObjectContentEvent>>,
        tokio::sync::oneshot::Receiver<()>,
    ) {
        spawn_test_producer_with(output, channel_capacity, csv_validation(), Duration::from_secs(300))
    }

    fn spawn_test_producer_with(
        output: SendableRecordBatchStream,
        channel_capacity: usize,
        validation: SelectValidation,
        deadline_after: Duration,
    ) -> (
        tokio::task::JoinHandle<()>,
        mpsc::Receiver<S3Result<SelectObjectContentEvent>>,
        tokio::sync::oneshot::Receiver<()>,
    ) {
        let (event_channel, rx) = test_event_channel(channel_capacity);
        let (lease, lease_released) = lease_drop_signal();
        let producer = tokio::spawn(send_select_events_until_deadline(
            output,
            event_channel,
            validation,
            Arc::new(SelectInputMetrics::default()),
            Instant::now() + deadline_after,
            300,
            lease,
        ));
        (producer, rx, lease_released)
    }

    fn test_event_channel(channel_capacity: usize) -> (SelectEventChannel, mpsc::Receiver<S3Result<SelectObjectContentEvent>>) {
        let (tx, rx) = mpsc::channel(channel_capacity);
        let terminal_records_permit = tx
            .clone()
            .try_reserve_owned()
            .expect("test channel should reserve terminal Records capacity");
        let terminal_permit = tx
            .clone()
            .try_reserve_owned()
            .expect("test channel should reserve terminal capacity");
        (
            SelectEventChannel {
                tx,
                terminal_records_permit: Some(terminal_records_permit),
                terminal_permit,
            },
            rx,
        )
    }

    #[test]
    fn validate_rejects_http_range() {
        let mut input = base_input();
        let mut headers = HeaderMap::new();
        headers.insert(RANGE, "bytes=0-1".parse().unwrap());
        let err = validate_select_request(&headers, &mut input).unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::UnsupportedRangeHeader);
    }

    #[test]
    fn validate_rejects_empty_select_expression_as_parse_failure() {
        for expression in ["", " \t\n"] {
            let mut input = base_input();
            input.request.expression = expression.to_string();

            let err = validate_select_request(&HeaderMap::new(), &mut input).unwrap_err();
            assert_eq!(err.code(), &S3ErrorCode::Custom("ParseSelectFailure".into()));
            assert_eq!(err.status_code(), Some(http::StatusCode::BAD_REQUEST));
        }
    }

    #[test]
    fn map_parser_error_to_parse_select_failure() {
        let err = map_query_error_to_s3(QueryError::Parser {
            source: ParserError::ParserError("syntax error".to_string()),
        });

        assert_eq!(err.code(), &S3ErrorCode::Custom("ParseSelectFailure".into()));
        assert_eq!(err.status_code(), Some(http::StatusCode::BAD_REQUEST));
        assert_eq!(err.message(), Some("sql parser error: syntax error"));
    }

    #[test]
    fn map_query_policy_errors_to_s3_errors() {
        let unsupported = map_query_error_to_s3(
            SelectError::UnsupportedSqlStructure {
                message: "JOIN is not supported".to_string(),
            }
            .into(),
        );
        let saturated = map_query_error_to_s3(SelectError::QueryConcurrencyLimit.into());
        let timed_out = map_query_error_to_s3(SelectError::QueryTimeout { seconds: 300 }.into());
        let stream_timed_out = map_query_error_to_s3(QueryError::Datafusion {
            source: Box::new(DataFusionError::External(Box::new(SelectError::QueryTimeout { seconds: 300 }))),
        });
        let exhausted = map_query_error_to_s3(QueryError::Datafusion {
            source: Box::new(DataFusionError::ObjectStore(Box::new(datafusion::object_store::Error::Generic {
                store: "EcObjectStore",
                source: Box::new(DataFusionError::ResourcesExhausted("memory limit".to_string())),
            }))),
        });
        let truncated = map_query_error_to_s3(QueryError::Datafusion {
            source: Box::new(DataFusionError::External(Box::new(SelectError::TruncatedInput))),
        });
        let raw_storage_short_read = map_query_error_to_s3(QueryError::Datafusion {
            source: Box::new(DataFusionError::ObjectStore(Box::new(datafusion::object_store::Error::Generic {
                store: "EcObjectStore",
                source: Box::new(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "truncated object stream")),
            }))),
        });
        let invalid_object_size = map_query_error_to_s3(QueryError::Datafusion {
            source: Box::new(DataFusionError::ObjectStore(Box::new(datafusion::object_store::Error::Generic {
                store: "EcObjectStore",
                source: Box::new(u64::try_from(-1_i64).expect_err("negative size must fail conversion")),
            }))),
        });

        assert_eq!(unsupported.code(), &S3ErrorCode::UnsupportedSqlStructure);
        assert_eq!(unsupported.message(), Some(UNSUPPORTED_SQL_STRUCTURE_MESSAGE));
        assert_eq!(saturated.code(), &S3ErrorCode::SlowDown);
        assert_eq!(timed_out.code(), &S3ErrorCode::Busy);
        assert_eq!(stream_timed_out.code(), &S3ErrorCode::Busy);
        assert_eq!(exhausted.code(), &S3ErrorCode::Busy);
        assert_eq!(truncated.code(), &S3ErrorCode::TruncatedInput);
        assert_eq!(raw_storage_short_read.code(), &S3ErrorCode::InternalError);
        assert_eq!(invalid_object_size.code(), &S3ErrorCode::InternalError);
        assert_eq!(invalid_object_size.message(), Some("An internal error occurred."));
    }

    #[test]
    fn every_select_error_has_an_explicit_protocol_mapping() {
        let cases = vec![
            (
                SelectError::InvalidCompressionFormat,
                S3ErrorCode::InvalidCompressionFormat,
                StatusCode::BAD_REQUEST,
            ),
            (SelectError::InvalidDataSource, S3ErrorCode::InvalidDataSource, StatusCode::BAD_REQUEST),
            (SelectError::TruncatedInput, S3ErrorCode::TruncatedInput, StatusCode::BAD_REQUEST),
            (SelectError::CsvParsingError, S3ErrorCode::CSVParsingError, StatusCode::BAD_REQUEST),
            (SelectError::JsonParsingError, S3ErrorCode::JSONParsingError, StatusCode::BAD_REQUEST),
            (
                SelectError::ParquetParsingError,
                S3ErrorCode::ParquetParsingError,
                StatusCode::BAD_REQUEST,
            ),
            (
                SelectError::ParseSelectFailure {
                    message: "invalid SELECT expression".to_string(),
                },
                S3ErrorCode::Custom(PARSE_SELECT_FAILURE_CODE.into()),
                StatusCode::BAD_REQUEST,
            ),
            (
                SelectError::InvalidQuery,
                S3ErrorCode::Custom(INVALID_QUERY_CODE.into()),
                StatusCode::BAD_REQUEST,
            ),
            (SelectError::InvalidDataType, S3ErrorCode::InvalidDataType, StatusCode::BAD_REQUEST),
            (
                SelectError::IncorrectSqlFunctionArgumentType,
                S3ErrorCode::IncorrectSqlFunctionArgumentType,
                StatusCode::BAD_REQUEST,
            ),
            (
                SelectError::DataSourcePathUnsupported,
                S3ErrorCode::Custom(DATA_SOURCE_PATH_UNSUPPORTED_CODE.into()),
                StatusCode::BAD_REQUEST,
            ),
            (
                SelectError::UnsupportedSqlStructure {
                    message: "JOIN is not supported".to_string(),
                },
                S3ErrorCode::UnsupportedSqlStructure,
                StatusCode::BAD_REQUEST,
            ),
            (
                SelectError::UnsupportedSqlOperation,
                S3ErrorCode::UnsupportedSqlOperation,
                StatusCode::BAD_REQUEST,
            ),
            (
                SelectError::EvaluatorBindingDoesNotExist,
                S3ErrorCode::EvaluatorBindingDoesNotExist,
                StatusCode::BAD_REQUEST,
            ),
            (SelectError::AmbiguousFieldName, S3ErrorCode::AmbiguousFieldName, StatusCode::BAD_REQUEST),
            (
                SelectError::InvalidScanRange,
                S3ErrorCode::InvalidRequestParameter,
                StatusCode::BAD_REQUEST,
            ),
            (SelectError::QueryConcurrencyLimit, S3ErrorCode::SlowDown, StatusCode::SERVICE_UNAVAILABLE),
            (
                SelectError::QueryTimeout { seconds: 300 },
                S3ErrorCode::Busy,
                StatusCode::SERVICE_UNAVAILABLE,
            ),
            (SelectError::ResourceExhausted, S3ErrorCode::Busy, StatusCode::SERVICE_UNAVAILABLE),
            (SelectError::BucketNotFound, S3ErrorCode::NoSuchBucket, StatusCode::NOT_FOUND),
            (SelectError::ObjectNotFound, S3ErrorCode::NoSuchKey, StatusCode::NOT_FOUND),
            (SelectError::Canceled, S3ErrorCode::InternalError, StatusCode::INTERNAL_SERVER_ERROR),
            (SelectError::InternalError, S3ErrorCode::InternalError, StatusCode::INTERNAL_SERVER_ERROR),
        ];

        for (select_error, expected_code, expected_status) in cases {
            let error = map_select_error_to_s3(&select_error);
            assert_eq!(error.code(), &expected_code, "wrong mapping for {select_error:?}");
            assert_eq!(error.status_code(), Some(expected_status), "wrong status for {select_error:?}");
            assert!(
                error.message().is_some_and(|message| !message.is_empty()),
                "missing protocol message for {select_error:?}"
            );
        }
    }

    #[test]
    fn internal_query_details_are_not_exposed_to_clients() {
        let private_detail = "node-1:/private/object/path physical_plan=secret";
        let error = map_query_error_to_s3(QueryError::from(DataFusionError::Internal(private_detail.to_string())));

        assert_eq!(error.code(), &S3ErrorCode::InternalError);
        assert_eq!(error.message(), Some("An internal error occurred."));
        assert!(!error.message().is_some_and(|message| message.contains(private_detail)));
        assert!(!format!("{error:?}").contains(private_detail));
        assert!(error.source().is_none());
    }

    #[test]
    fn prepare_snapshot_storage_error_preserves_existing_s3_mapping() {
        let err = map_prepare_snapshot_error(StoragePrepareSelectObjectSnapshotError::Storage(StorageError::ObjectNotFound(
            "bucket".to_string(),
            "object.csv".to_string(),
        )));

        assert_eq!(err.code(), &S3ErrorCode::NoSuchKey);
        assert!(err.source().is_some());
    }

    #[test]
    fn prepare_snapshot_invalid_logical_size_fails_with_redacted_internal_error() {
        let err = map_prepare_snapshot_error(StoragePrepareSelectObjectSnapshotError::InvalidLogicalSize { size: -1 });

        assert_eq!(err.code(), &S3ErrorCode::InternalError);
        assert!(err.source().is_none());
        assert_eq!(err.message(), Some("An internal error occurred."));
    }

    #[tokio::test(start_paused = true)]
    async fn producer_deadline_cancels_backpressured_send() {
        let output = pending_output();
        let (event_channel, mut rx) = test_event_channel(3);
        event_channel
            .tx
            .send(Ok(SelectObjectContentEvent::Cont(ContinuationEvent::default())))
            .await
            .expect("test channel should accept the prefilled event");
        let (lease, lease_released) = lease_drop_signal();
        let producer = tokio::spawn(send_select_events_until_deadline(
            output,
            event_channel,
            csv_validation(),
            Arc::new(SelectInputMetrics::default()),
            Instant::now() + std::time::Duration::from_secs(1),
            300,
            lease,
        ));

        tokio::task::yield_now().await;
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        tokio::task::yield_now().await;

        producer.await.expect("producer should finish without draining the channel");
        assert!(matches!(rx.recv().await, Some(Ok(SelectObjectContentEvent::Cont(_)))));
        let timeout_error = rx
            .recv()
            .await
            .expect("producer should send a terminal timeout error")
            .expect_err("terminal event should be an error");
        assert_eq!(timeout_error.code(), &S3ErrorCode::Busy);
        assert!(rx.recv().await.is_none());
        assert!(lease_released.await.is_ok(), "timeout should release the snapshot lease");
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_preempts_multi_slice_batch_encoding() {
        let value = "x".repeat(64 * 1024);
        let mut builder = StringDictionaryBuilder::<Int32Type>::new();
        for _ in 0..(MAX_ENCODE_ROWS_PER_TURN + 1) {
            builder.append(&value).expect("dictionary value should append");
        }
        let values = builder.finish();
        let schema = Arc::new(Schema::new(vec![Field::new("value", values.data_type().clone(), false)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(values)]).expect("test record batch should be valid");
        let output = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async move { Ok::<_, DataFusionError>(batch) }),
        ));
        let (event_channel, mut rx) = test_event_channel(4);
        let (lease, lease_released) = lease_drop_signal();
        let producer = send_select_events_until_deadline(
            output,
            event_channel,
            csv_validation(),
            Arc::new(SelectInputMetrics::default()),
            Instant::now() + Duration::from_secs(1),
            1,
            lease,
        );
        tokio::pin!(producer);

        assert!(futures::poll!(producer.as_mut()).is_pending());
        assert!(futures::poll!(producer.as_mut()).is_pending());
        assert!(matches!(rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(futures::poll!(producer.as_mut()).is_ready());

        let Some(Ok(SelectObjectContentEvent::Records(records))) = rx.recv().await else {
            panic!("the first encoded slice should flush before the timeout");
        };
        assert_eq!(records.payload.as_ref().map(Bytes::len), Some(value.len() + 1));
        let timeout = rx
            .recv()
            .await
            .expect("deadline should send one terminal error")
            .expect_err("deadline terminal event should be an error");
        assert_eq!(timeout.code(), &S3ErrorCode::Busy);
        assert!(rx.recv().await.is_none());
        assert!(lease_released.await.is_ok(), "deadline should release the snapshot lease");
    }

    #[test]
    fn skewed_dictionary_batch_stays_within_one_encode_turn() {
        let value = "x".repeat(64 * 1024);
        let mut builder = StringDictionaryBuilder::<Int32Type>::new();
        builder.append("").expect("empty dictionary value should append");
        for _ in 0..MAX_ENCODE_ROWS_PER_TURN {
            builder.append(&value).expect("large dictionary value should append");
        }
        let values = builder.finish();
        let schema = Arc::new(Schema::new(vec![Field::new("value", values.data_type().clone(), false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(values)]).expect("test record batch should be valid");
        let mut encoder = SelectOutputEncoder::new(SelectOutputFormat::Csv(CSVOutput::default()));
        let mut buffer = BytesMut::new();

        let encoded_rows =
            encode_batch_turn(&mut encoder, &batch, 0, &mut buffer).expect("dictionary rows should encode successfully");

        assert_eq!(encoded_rows, 1, "shared dictionary values must be charged before batching rows");
        assert_eq!(buffer.as_ref(), b"\n");
    }

    #[test]
    fn unused_large_dictionary_value_does_not_force_per_row_encoding() {
        let row_count = MAX_ENCODE_ROWS_PER_TURN;
        let keys = Int32Array::from(vec![0; row_count]);
        let unused = "x".repeat(64 * 1024);
        let dictionary = Arc::new(StringArray::from(vec!["", unused.as_str()]));
        let values = DictionaryArray::<Int32Type>::try_new(keys, dictionary).expect("test dictionary should be valid");
        let schema = Arc::new(Schema::new(vec![Field::new("value", values.data_type().clone(), false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(values)]).expect("test record batch should be valid");
        let mut encoder = SelectOutputEncoder::new(SelectOutputFormat::Csv(CSVOutput::default()));
        let mut buffer = BytesMut::new();

        let encoded_rows = encode_batch_turn(&mut encoder, &batch, 0, &mut buffer).expect("referenced empty values should batch");

        assert_eq!(encoded_rows, row_count);
        assert_eq!(buffer.as_ref(), "\n".repeat(row_count).as_bytes());
    }

    #[test]
    fn skewed_json_dictionary_keeps_the_complete_bounded_prefix() {
        let row_count = MAX_ENCODE_ROWS_PER_TURN;
        let mut keys = vec![0; row_count];
        keys[row_count - 1] = 1;
        let large = "x".repeat(70 * 1024);
        let dictionary = Arc::new(StringArray::from(vec!["a", large.as_str()]));
        let values =
            DictionaryArray::<Int32Type>::try_new(Int32Array::from(keys), dictionary).expect("test dictionary should be valid");
        let schema = Arc::new(Schema::new(vec![Field::new("value", values.data_type().clone(), false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(values)]).expect("test record batch should be valid");
        let mut encoder = SelectOutputEncoder::new(SelectOutputFormat::Json(JSONOutput::default()));
        let mut buffer = BytesMut::new();

        let encoded_rows =
            encode_batch_turn(&mut encoder, &batch, 0, &mut buffer).expect("the bounded dictionary prefix should encode");

        assert_eq!(encoded_rows, row_count - 1);
        assert_eq!(buffer.iter().filter(|byte| **byte == b'\n').count(), encoded_rows);
    }

    #[test]
    fn short_rows_share_an_encode_turn() {
        let row_count = MAX_ENCODE_ROWS_PER_TURN;
        let values = StringArray::from(vec!["x"; row_count]);
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(values)]).expect("test record batch should be valid");
        let mut encoder = SelectOutputEncoder::new(SelectOutputFormat::Csv(CSVOutput::default()));
        let mut buffer = BytesMut::new();

        let encoded_rows =
            encode_batch_turn(&mut encoder, &batch, 0, &mut buffer).expect("short rows should encode successfully");

        assert!(encoded_rows > 1, "small rows should not construct one Arrow writer per row");
        assert!(encoded_rows <= MAX_ENCODE_ROWS_PER_TURN);
        assert_eq!(buffer.as_ref(), "x\n".repeat(encoded_rows).as_bytes());
    }

    #[test]
    fn small_nested_json_rows_share_an_encode_turn() {
        let values = ListArray::from_iter_primitive::<Int32Type, _, _>([Some([Some(1)]), Some([Some(2)])]);
        let schema = Arc::new(Schema::new(vec![Field::new("items", values.data_type().clone(), false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(values)]).expect("test record batch should be valid");
        let mut encoder = SelectOutputEncoder::new(SelectOutputFormat::Json(JSONOutput::default()));
        let mut buffer = BytesMut::new();

        let encoded_rows = encode_batch_turn(&mut encoder, &batch, 0, &mut buffer).expect("small nested JSON rows should encode");

        assert_eq!(encoded_rows, 2);
        assert_eq!(buffer.as_ref(), b"{\"items\":[1]}\n{\"items\":[2]}\n");
    }

    #[test]
    fn large_second_nested_json_row_stops_after_the_complete_prefix() {
        let mut builder = ListBuilder::new(BooleanBuilder::new());
        builder.values().append_value(true);
        builder.append(true);
        for _ in 0..(ENCODE_TURN_TARGET_BYTES / b"true,".len() + 8) {
            builder.values().append_value(true);
        }
        builder.append(true);
        let values = builder.finish();
        let schema = Arc::new(Schema::new(vec![Field::new("items", values.data_type().clone(), false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(values)]).expect("test record batch should be valid");
        let mut encoder = SelectOutputEncoder::new(SelectOutputFormat::Json(JSONOutput::default()));
        let mut buffer = BytesMut::new();

        let encoded_rows = encode_batch_turn(&mut encoder, &batch, 0, &mut buffer).expect("the complete first row should encode");

        assert_eq!(encoded_rows, 1);
        assert_eq!(buffer.as_ref(), b"{\"items\":[true]}\n");

        buffer.clear();
        let encoded_rows =
            encode_batch_turn(&mut encoder, &batch, 1, &mut buffer).expect("the larger second row should encode alone");
        assert_eq!(encoded_rows, 1);
        assert!(buffer.len() > ENCODE_TURN_TARGET_BYTES);
        assert!(buffer.len() <= MAX_SELECT_OUTPUT_RECORD_BYTES);
    }

    #[test]
    fn csv_turn_rolls_back_a_partially_escaped_second_row() {
        let quoted = "\"".repeat(40 * 1024);
        let values = StringArray::from(vec!["ok", quoted.as_str()]);
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(values)]).expect("test record batch should be valid");
        let mut encoder = SelectOutputEncoder::new(SelectOutputFormat::Csv(CSVOutput::default()));
        let mut buffer = BytesMut::new();

        let encoded_rows =
            encode_batch_turn(&mut encoder, &batch, 0, &mut buffer).expect("the complete first row should remain staged");

        assert_eq!(encoded_rows, 1);
        assert_eq!(buffer.as_ref(), b"ok\n");
    }

    #[test]
    fn csv_encoder_error_discards_the_partial_current_turn() {
        let values = StringArray::from(vec!["ok"]);
        let nested = ListArray::from_iter_primitive::<Int32Type, _, _>([Some([Some(1)])]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Utf8, false),
            Field::new("nested", nested.data_type().clone(), false),
        ]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(values), Arc::new(nested)]).expect("test record batch should be valid");
        let mut encoder = SelectOutputEncoder::new(SelectOutputFormat::Csv(CSVOutput::default()));
        let mut buffer = BytesMut::from(b"staged\n".as_slice());

        let error = encode_batch_turn(&mut encoder, &batch, 0, &mut buffer)
            .expect_err("nested CSV output should fail after the first column");

        assert_eq!(error.code(), &S3ErrorCode::InternalError);
        assert_eq!(buffer.as_ref(), b"staged\n");
    }

    #[test]
    fn oversized_nested_json_stops_at_the_output_budget() {
        let value_count = MAX_SELECT_OUTPUT_RECORD_BYTES / b"true,".len() + 1;
        let mut builder = ListBuilder::new(BooleanBuilder::new());
        for _ in 0..value_count {
            builder.values().append_value(true);
        }
        builder.append(true);
        let values = builder.finish();
        let schema = Arc::new(Schema::new(vec![Field::new("items", values.data_type().clone(), false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(values)]).expect("test record batch should be valid");
        let mut encoder = SelectOutputEncoder::new(SelectOutputFormat::Json(JSONOutput::default()));
        let mut buffer = BytesMut::new();

        let error = encode_batch_turn(&mut encoder, &batch, 0, &mut buffer)
            .expect_err("nested JSON larger than one MiB must stop at the output budget");

        assert_eq!(error.code(), &S3ErrorCode::OverMaxRecordSize);
        assert!(buffer.is_empty());
    }

    #[test]
    fn nested_json_struct_map_dictionary_and_run_end_match_arrow_semantics() {
        let profile = StructArray::from(vec![
            (
                Arc::new(Field::new("name", DataType::Utf8, false)),
                Arc::new(StringArray::from(vec!["a\n"])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("count", DataType::Int32, true)),
                Arc::new(Int32Array::from(vec![None])) as ArrayRef,
            ),
        ]);
        let labels =
            MapArray::from_vec_of_maps::<StringArray, Int32Array, _, _>(vec![Some(vec![("a", Some(1)), ("b", None)])], true);
        let mut dictionary = StringDictionaryBuilder::<Int32Type>::new();
        dictionary.append("small").expect("dictionary value should append");
        let dictionary = dictionary.finish();
        let run_ends = Int32Array::from(vec![1]);
        let run_values = Arc::new(StringArray::from(vec!["run"])) as ArrayRef;
        let run = RunArray::<Int32Type>::try_new(&run_ends, &run_values).expect("run-end encoded value should be valid");
        let schema = Arc::new(Schema::new(vec![
            Field::new("profile", profile.data_type().clone(), false),
            Field::new("labels", labels.data_type().clone(), false),
            Field::new("code", dictionary.data_type().clone(), false),
            Field::new("run", run.data_type().clone(), false),
        ]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(profile), Arc::new(labels), Arc::new(dictionary), Arc::new(run)])
            .expect("test record batch should be valid");
        let mut encoder = SelectOutputEncoder::new(SelectOutputFormat::Json(JSONOutput::default()));
        let mut buffer = BytesMut::new();

        assert_eq!(
            encode_batch_turn(&mut encoder, &batch, 0, &mut buffer).expect("nested JSON values should encode"),
            1
        );
        assert_eq!(
            buffer.as_ref(),
            b"{\"profile\":{\"name\":\"a\\n\",\"count\":null},\"labels\":{\"a\":1,\"b\":null},\"code\":\"small\",\"run\":\"run\"}\n"
        );
    }

    #[test]
    fn json_null_projection_encodes_without_invoking_arrows_null_encoder() {
        let values = NullArray::new(1);
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Null, true)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(values)]).expect("test record batch should be valid");
        let mut encoder = SelectOutputEncoder::new(SelectOutputFormat::Json(JSONOutput::default()));
        let mut buffer = BytesMut::new();

        assert_eq!(
            encode_batch_turn(&mut encoder, &batch, 0, &mut buffer).expect("NULL projection should encode"),
            1
        );
        assert_eq!(buffer.as_ref(), b"{\"value\":null}\n");
    }

    #[test]
    fn json_dictionary_null_value_matches_arrow_writer_semantics() {
        let keys = Int32Array::from(vec![0]);
        let dictionary = Arc::new(StringArray::from(vec![None::<&str>]));
        let values = DictionaryArray::<Int32Type>::try_new(keys, dictionary).expect("test dictionary should be valid");
        let schema = Arc::new(Schema::new(vec![Field::new("value", values.data_type().clone(), true)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(values)]).expect("test record batch should be valid");
        let mut encoder = SelectOutputEncoder::new(SelectOutputFormat::Json(JSONOutput::default()));
        let mut buffer = BytesMut::new();

        assert_eq!(
            encode_batch_turn(&mut encoder, &batch, 0, &mut buffer).expect("dictionary null value should encode"),
            1
        );
        assert_eq!(buffer.as_ref(), b"{\"value\":\"\"}\n");
    }

    #[test]
    fn json_dictionary_of_null_type_does_not_invoke_arrows_null_encoder() {
        let values = DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![0]), Arc::new(NullArray::new(1)))
            .expect("test dictionary should be valid");
        let schema = Arc::new(Schema::new(vec![Field::new("value", values.data_type().clone(), true)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(values)]).expect("test record batch should be valid");
        let mut encoder = SelectOutputEncoder::new(SelectOutputFormat::Json(JSONOutput::default()));
        let mut buffer = BytesMut::new();

        assert_eq!(
            encode_batch_turn(&mut encoder, &batch, 0, &mut buffer).expect("dictionary Null value should encode"),
            1
        );
        assert_eq!(buffer.as_ref(), b"{\"value\":null}\n");
    }

    #[test]
    fn json_escaped_field_name_stops_at_the_output_budget() {
        let field_name = "\n".repeat(MAX_SELECT_OUTPUT_RECORD_BYTES / 2 + 1);
        let values = NullArray::new(1);
        let schema = Arc::new(Schema::new(vec![Field::new(field_name, DataType::Null, true)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(values)]).expect("test record batch should be valid");
        let mut encoder = SelectOutputEncoder::new(SelectOutputFormat::Json(JSONOutput::default()));
        let mut buffer = BytesMut::new();

        let error = encode_batch_turn(&mut encoder, &batch, 0, &mut buffer)
            .expect_err("an escaped field name larger than one MiB must fail at the output budget");

        assert_eq!(error.code(), &S3ErrorCode::OverMaxRecordSize);
        assert!(buffer.is_empty());
    }

    #[test]
    fn null_json_struct_does_not_materialize_its_child_encoder_plan() {
        let child_name = "\n".repeat(MAX_SELECT_OUTPUT_RECORD_BYTES);
        let child = Arc::new(Field::new(child_name, DataType::Utf8, true));
        let values = StructArray::new_null(vec![child].into(), 1);
        let schema = Arc::new(Schema::new(vec![Field::new("nested", values.data_type().clone(), true)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(values)]).expect("test record batch should be valid");
        let mut encoder = SelectOutputEncoder::new(SelectOutputFormat::Json(JSONOutput::default()));
        let mut buffer = BytesMut::new();

        assert_eq!(
            encode_batch_turn(&mut encoder, &batch, 0, &mut buffer).expect("a null struct should encode without its children"),
            1
        );
        assert_eq!(buffer.as_ref(), b"{\"nested\":null}\n");
    }

    #[test]
    fn json_record_delimiter_is_in_the_encode_budget() {
        let delimiter = "x".repeat(64 * 1024);
        let values = StringArray::from(vec!["a", "b"]);
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(values)]).expect("test record batch should be valid");
        let mut encoder = SelectOutputEncoder::new(SelectOutputFormat::Json(JSONOutput {
            record_delimiter: Some(delimiter.clone()),
        }));
        let mut buffer = BytesMut::new();

        let encoded_rows = encode_batch_turn(&mut encoder, &batch, 0, &mut buffer).expect("bounded JSON delimiter should encode");

        assert_eq!(encoded_rows, 1);
        assert_eq!(buffer.len(), br#"{"value":"a"}"#.len() + delimiter.len());
    }

    #[test]
    fn oversized_output_record_fails_before_growing_records_buffer() {
        let value = "x".repeat(MAX_SELECT_OUTPUT_RECORD_BYTES + 1);
        let values = StringArray::from(vec![value.as_str()]);
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(values)]).expect("test record batch should be valid");
        let mut encoder = SelectOutputEncoder::new(SelectOutputFormat::Csv(CSVOutput::default()));
        let mut buffer = BytesMut::new();

        let error =
            encode_batch_turn(&mut encoder, &batch, 0, &mut buffer).expect_err("result records larger than one MiB must fail");

        assert_eq!(error.code(), &S3ErrorCode::OverMaxRecordSize);
        assert!(buffer.is_empty());
    }

    #[test]
    fn repeated_json_projection_is_bounded_per_field() {
        let value = "x".repeat(64 * 1024);
        let values = Arc::new(StringArray::from(vec![value.as_str()]));
        let fields = (0..1024)
            .map(|index| Field::new(format!("value_{index}"), DataType::Utf8, false))
            .collect::<Vec<_>>();
        let columns = (0..fields.len())
            .map(|_| Arc::clone(&values) as datafusion::arrow::array::ArrayRef)
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("test record batch should be valid");
        let mut encoder = SelectOutputEncoder::new(SelectOutputFormat::Json(JSONOutput::default()));
        let mut buffer = BytesMut::new();

        let error = encode_batch_turn(&mut encoder, &batch, 0, &mut buffer)
            .expect_err("a repeated projection larger than one MiB must fail");

        assert_eq!(error.code(), &S3ErrorCode::OverMaxRecordSize);
        assert!(buffer.is_empty());
    }

    #[test]
    fn csv_output_limit_counts_the_record_delimiter() {
        for excess in 0..=1 {
            let value = "x".repeat(MAX_SELECT_OUTPUT_RECORD_BYTES - 1 + excess);
            let values = StringArray::from(vec![value.as_str()]);
            let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, false)]));
            let batch = RecordBatch::try_new(schema, vec![Arc::new(values)]).expect("test record batch should be valid");
            let mut encoder = SelectOutputEncoder::new(SelectOutputFormat::Csv(CSVOutput::default()));
            let mut buffer = BytesMut::new();

            if excess == 0 {
                assert_eq!(
                    encode_batch_turn(&mut encoder, &batch, 0, &mut buffer).expect("a complete one MiB CSV record should encode"),
                    1
                );
                assert_eq!(buffer.len(), MAX_SELECT_OUTPUT_RECORD_BYTES);
            } else {
                let error = encode_batch_turn(&mut encoder, &batch, 0, &mut buffer)
                    .expect_err("a CSV record exceeding one MiB by its delimiter must fail");
                assert_eq!(error.code(), &S3ErrorCode::OverMaxRecordSize);
                assert!(buffer.is_empty());
            }
        }
    }

    #[test]
    fn json_output_limit_counts_the_record_delimiter() {
        let overhead = br#"{"value":""}"#.len() + 1;
        for excess in 0..=1 {
            let value = "x".repeat(MAX_SELECT_OUTPUT_RECORD_BYTES - overhead + excess);
            let values = StringArray::from(vec![value.as_str()]);
            let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, false)]));
            let batch = RecordBatch::try_new(schema, vec![Arc::new(values)]).expect("test record batch should be valid");
            let mut encoder = SelectOutputEncoder::new(SelectOutputFormat::Json(JSONOutput::default()));
            let mut buffer = BytesMut::new();

            if excess == 0 {
                assert_eq!(
                    encode_batch_turn(&mut encoder, &batch, 0, &mut buffer)
                        .expect("a complete one MiB JSON record should encode"),
                    1
                );
                assert_eq!(buffer.len(), MAX_SELECT_OUTPUT_RECORD_BYTES);
            } else {
                let error = encode_batch_turn(&mut encoder, &batch, 0, &mut buffer)
                    .expect_err("a JSON record exceeding one MiB by its delimiter must fail");
                assert_eq!(error.code(), &S3ErrorCode::OverMaxRecordSize);
                assert!(buffer.is_empty());
            }
        }
    }

    #[test]
    fn json_binary_hex_encoding_honors_the_output_limit() {
        let overhead = br#"{"value":""}"#.len() + 1;
        let max_value_bytes = (MAX_SELECT_OUTPUT_RECORD_BYTES - overhead) / 2;
        for excess in 0..=1 {
            let value = vec![0xab; max_value_bytes + excess];
            let values = BinaryArray::from(vec![value.as_slice()]);
            let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Binary, false)]));
            let batch = RecordBatch::try_new(schema, vec![Arc::new(values)]).expect("test record batch should be valid");
            let mut encoder = SelectOutputEncoder::new(SelectOutputFormat::Json(JSONOutput::default()));
            let mut buffer = BytesMut::new();

            if excess == 0 {
                assert_eq!(
                    encode_batch_turn(&mut encoder, &batch, 0, &mut buffer)
                        .expect("the largest binary value within the output limit should encode"),
                    1
                );
                assert_eq!(buffer.len(), overhead + value.len() * 2);
                assert_eq!(buffer.len(), MAX_SELECT_OUTPUT_RECORD_BYTES - 1);
            } else {
                let error =
                    encode_batch_turn(&mut encoder, &batch, 0, &mut buffer).expect_err("binary JSON exceeding one MiB must fail");
                assert_eq!(error.code(), &S3ErrorCode::OverMaxRecordSize);
                assert!(buffer.is_empty());
            }
        }
    }

    #[test]
    fn json_string_escaping_honors_the_exact_output_limit() {
        let overhead = br#"{"value":""}"#.len() + 1;
        let escaped_budget = MAX_SELECT_OUTPUT_RECORD_BYTES - overhead;
        let escaped_newlines = escaped_budget / 2;
        let mut exact_value = "\n".repeat(escaped_newlines);
        exact_value.push_str(&"x".repeat(escaped_budget % 2));

        for excess in 0..=1 {
            let mut value = exact_value.clone();
            value.push_str(&"x".repeat(excess));
            let values = StringArray::from(vec![value.as_str()]);
            let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, false)]));
            let batch = RecordBatch::try_new(schema, vec![Arc::new(values)]).expect("test record batch should be valid");
            let mut encoder = SelectOutputEncoder::new(SelectOutputFormat::Json(JSONOutput::default()));
            let mut buffer = BytesMut::new();

            if excess == 0 {
                assert_eq!(
                    encode_batch_turn(&mut encoder, &batch, 0, &mut buffer)
                        .expect("escaped JSON record of exactly one MiB should encode"),
                    1
                );
                assert_eq!(buffer.len(), MAX_SELECT_OUTPUT_RECORD_BYTES);
            } else {
                let error = encode_batch_turn(&mut encoder, &batch, 0, &mut buffer)
                    .expect_err("escaped JSON record exceeding one MiB must fail");
                assert_eq!(error.code(), &S3ErrorCode::OverMaxRecordSize);
                assert!(buffer.is_empty());
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_preempts_writable_multi_chunk_records() {
        const PAYLOAD_CHUNKS: usize = 8;

        let (event_channel, mut rx) = test_event_channel(4);
        let (lease, lease_released) = lease_drop_signal();
        let producer = send_select_events_until_deadline(
            large_pending_output(PAYLOAD_CHUNKS),
            event_channel,
            csv_validation(),
            Arc::new(SelectInputMetrics::default()),
            Instant::now() + Duration::from_secs(1),
            1,
            lease,
        );
        tokio::pin!(producer);

        for _ in 0..3 {
            assert!(futures::poll!(producer.as_mut()).is_pending());
        }
        for _ in 0..2 {
            assert!(matches!(rx.try_recv(), Ok(Ok(SelectObjectContentEvent::Records(_)))));
        }
        assert!(matches!(rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)));

        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(futures::poll!(producer.as_mut()).is_ready());
        assert!(matches!(rx.recv().await, Some(Ok(SelectObjectContentEvent::Records(_)))));
        let timeout = rx
            .recv()
            .await
            .expect("deadline should send one terminal error")
            .expect_err("deadline terminal event should be an error");
        assert_eq!(timeout.code(), &S3ErrorCode::Busy);
        assert!(rx.recv().await.is_none());
        assert!(lease_released.await.is_ok(), "timeout should release the snapshot lease");
    }

    #[tokio::test(start_paused = true)]
    async fn continuation_starts_at_one_second_without_query_output() {
        let (producer, mut rx, lease_released) = spawn_test_producer(pending_output(), 4);
        tokio::task::yield_now().await;

        assert!(matches!(rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
        tokio::time::advance(Duration::from_millis(999)).await;
        tokio::task::yield_now().await;
        assert!(matches!(rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)));

        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(matches!(rx.recv().await, Some(Ok(SelectObjectContentEvent::Cont(_)))));
        drop(rx);
        producer.await.expect("producer should stop after the receiver closes");
        assert!(lease_released.await.is_ok(), "receiver close should release the snapshot lease");
    }

    #[tokio::test(start_paused = true)]
    async fn progress_starts_at_sixty_seconds_only_when_enabled() {
        let mut validation = csv_validation();
        validation.progress_enabled = true;
        let (producer, mut rx, lease_released) =
            spawn_test_producer_with(pending_output(), 8, validation, Duration::from_secs(300));
        tokio::task::yield_now().await;

        assert!(matches!(rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
        tokio::time::advance(Duration::from_millis(59_999)).await;
        assert!(matches!(rx.recv().await, Some(Ok(SelectObjectContentEvent::Cont(_)))));
        assert!(matches!(rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)));

        tokio::time::advance(Duration::from_millis(1)).await;
        let Some(Ok(SelectObjectContentEvent::Progress(progress))) = rx.recv().await else {
            panic!("enabled progress should fire at sixty seconds");
        };
        let details = progress.details.expect("Progress should contain details");
        assert_eq!(details.bytes_scanned, Some(0));
        assert_eq!(details.bytes_processed, Some(0));
        assert_eq!(details.bytes_returned, Some(0));
        drop(rx);
        producer.await.expect("producer should stop after the receiver closes");
        assert!(lease_released.await.is_ok(), "receiver close should release the snapshot lease");

        let (producer, mut rx, lease_released) = spawn_test_producer(pending_output(), 8);
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(60)).await;
        assert!(matches!(rx.recv().await, Some(Ok(SelectObjectContentEvent::Cont(_)))));
        for _ in 0..3 {
            tokio::task::yield_now().await;
        }
        assert!(matches!(rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
        drop(rx);
        producer.await.expect("producer should stop after the receiver closes");
        assert!(lease_released.await.is_ok(), "receiver close should release the snapshot lease");
    }

    #[tokio::test(start_paused = true)]
    async fn small_records_flush_at_five_hundred_milliseconds() {
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(vec!["row"]))])
            .expect("test record batch should be valid");
        let output = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async move { Ok::<_, DataFusionError>(batch) })
                .chain(futures::stream::pending::<Result<RecordBatch, DataFusionError>>()),
        ));
        let (producer, mut rx, lease_released) = spawn_test_producer(output, 4);
        for _ in 0..3 {
            tokio::task::yield_now().await;
        }

        assert!(matches!(rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
        tokio::time::advance(Duration::from_millis(499)).await;
        tokio::task::yield_now().await;
        assert!(matches!(rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)));

        tokio::time::advance(Duration::from_millis(1)).await;
        let Some(Ok(SelectObjectContentEvent::Records(records))) = rx.recv().await else {
            panic!("small Records payload should flush at five hundred milliseconds");
        };
        assert_eq!(records.payload.as_deref(), Some(b"row\n".as_slice()));
        drop(rx);
        producer.await.expect("producer should stop after the receiver closes");
        assert!(lease_released.await.is_ok(), "receiver close should release the snapshot lease");
    }

    #[tokio::test(start_paused = true)]
    async fn full_records_payload_flushes_without_advancing_time() {
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, false)]));
        let value = "x".repeat(RECORDS_CHUNK_TARGET);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(vec![value]))])
            .expect("test record batch should be valid");
        let output = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async move { Ok::<_, DataFusionError>(batch) })
                .chain(futures::stream::pending::<Result<RecordBatch, DataFusionError>>()),
        ));
        let (producer, mut rx, lease_released) = spawn_test_producer(output, 4);

        let Some(Ok(SelectObjectContentEvent::Records(records))) = rx.recv().await else {
            panic!("a full Records payload should flush without waiting for the timer");
        };
        assert_eq!(records.payload.as_ref().map(Bytes::len), Some(RECORDS_CHUNK_TARGET));
        assert!(matches!(rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
        drop(rx);
        producer.await.expect("producer should stop after the receiver closes");
        assert!(lease_released.await.is_ok(), "receiver close should release the snapshot lease");
    }

    #[tokio::test(start_paused = true)]
    async fn delayed_intervals_do_not_burst_after_time_jump() {
        let (producer, mut rx, lease_released) = spawn_test_producer(pending_output(), 16);
        tokio::task::yield_now().await;

        tokio::time::advance(Duration::from_secs(10)).await;
        assert!(matches!(rx.recv().await, Some(Ok(SelectObjectContentEvent::Cont(_)))));
        for _ in 0..5 {
            tokio::task::yield_now().await;
        }
        assert!(matches!(rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)));

        drop(rx);
        producer.await.expect("producer should stop after the receiver closes");
        assert!(lease_released.await.is_ok(), "receiver close should release the snapshot lease");
    }

    #[tokio::test(start_paused = true)]
    async fn continuous_records_do_not_starve_continuation() {
        let (producer, mut rx, lease_released) = spawn_test_producer(large_pending_output(8), 16);
        tokio::task::yield_now().await;

        tokio::time::advance(CONTINUATION_INTERVAL).await;
        let mut saw_continuation = false;
        for _ in 0..10 {
            let event = rx
                .recv()
                .await
                .expect("scheduler should emit an event")
                .expect("event should not fail");
            if matches!(event, SelectObjectContentEvent::Cont(_)) {
                saw_continuation = true;
                break;
            }
        }
        assert!(saw_continuation, "continuous Records must not starve the continuation ticker");

        drop(rx);
        producer.await.expect("producer should stop after the receiver closes");
        assert!(lease_released.await.is_ok(), "receiver close should release the snapshot lease");
    }

    #[tokio::test(start_paused = true)]
    async fn continuous_records_do_not_starve_progress() {
        let mut validation = csv_validation();
        validation.progress_enabled = true;
        let (producer, mut rx, lease_released) =
            spawn_test_producer_with(large_pending_output(8), 16, validation, Duration::from_secs(300));
        tokio::task::yield_now().await;

        tokio::time::advance(PROGRESS_INTERVAL).await;
        let mut saw_progress = false;
        let mut saw_continuation = false;
        for _ in 0..12 {
            let event = rx
                .recv()
                .await
                .expect("scheduler should emit an event")
                .expect("event should not fail");
            match event {
                SelectObjectContentEvent::Progress(progress) => {
                    assert!(
                        progress
                            .details
                            .is_some_and(|details| details.bytes_returned.is_some_and(|bytes| bytes > 0))
                    );
                    saw_progress = true;
                }
                SelectObjectContentEvent::Cont(_) => saw_continuation = true,
                _ => {}
            }
            if saw_progress && saw_continuation {
                break;
            }
        }
        assert!(saw_progress, "continuous Records must not starve the progress ticker");
        assert!(saw_continuation, "continuous Records must not starve the continuation ticker");

        drop(rx);
        producer.await.expect("producer should stop after the receiver closes");
        assert!(lease_released.await.is_ok(), "receiver close should release the snapshot lease");
    }

    #[tokio::test(start_paused = true)]
    async fn backpressured_records_cannot_starve_periodic_events() {
        let (producer, mut rx, lease_released) = spawn_test_producer(large_pending_output(4), 3);
        tokio::task::yield_now().await;

        tokio::time::advance(CONTINUATION_INTERVAL).await;
        let mut records_before_continuation = 0;
        loop {
            let event = rx
                .recv()
                .await
                .expect("scheduler should emit an event")
                .expect("event should not fail");
            match event {
                SelectObjectContentEvent::Records(_) => {
                    records_before_continuation += 1;
                    assert!(
                        records_before_continuation <= 2,
                        "only the queued and already-pending Records events may precede a due continuation"
                    );
                }
                SelectObjectContentEvent::Cont(_) => break,
                _ => panic!("unexpected event before the due continuation"),
            }
        }

        let Some(Ok(SelectObjectContentEvent::Records(records))) = rx.recv().await else {
            panic!("buffered Records must resume immediately after the due continuation");
        };
        assert_eq!(records.payload.as_ref().map(Bytes::len), Some(RECORDS_CHUNK_TARGET));

        drop(rx);
        producer.await.expect("producer should stop after the receiver closes");
        assert!(lease_released.await.is_ok(), "receiver close should release the snapshot lease");
    }

    #[tokio::test(start_paused = true)]
    async fn producer_preserves_finite_stream_terminal_events() {
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![datafusion::arrow::datatypes::Field::new(
            "value",
            datafusion::arrow::datatypes::DataType::Utf8,
            false,
        )]));
        let batch = |value| {
            RecordBatch::try_new(schema.clone(), vec![Arc::new(datafusion::arrow::array::StringArray::from(vec![value]))])
                .expect("test record batch should be valid")
        };
        let batches = [Ok(batch("a")), Ok(batch("b"))];
        let output = Box::pin(RecordBatchStreamAdapter::new(schema, futures::stream::iter(batches)));
        let (producer, mut rx, lease_released) = spawn_test_producer(output, 8);

        producer.await.expect("producer should finish at query EOF");

        let Some(Ok(SelectObjectContentEvent::Records(records))) = rx.recv().await else {
            panic!("producer should flush buffered records at query EOF");
        };
        assert_eq!(records.payload.as_deref(), Some(b"a\nb\n".as_slice()));
        let Some(Ok(SelectObjectContentEvent::Stats(stats))) = rx.recv().await else {
            panic!("producer should emit final stats");
        };
        assert_eq!(stats.details.and_then(|details| details.bytes_returned), Some(4));
        assert!(matches!(rx.recv().await, Some(Ok(SelectObjectContentEvent::End(_)))));
        assert!(rx.recv().await.is_none());
        assert!(lease_released.await.is_ok(), "End should release the snapshot lease");
    }

    #[tokio::test(start_paused = true)]
    async fn multi_slice_multi_chunk_output_is_complete_and_ordered() {
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, false)]));
        let values = (0..(MAX_ENCODE_ROWS_PER_TURN * 2 + 1))
            .map(|index| format!("{index:04}-{}", "x".repeat(72)))
            .collect::<Vec<_>>();
        let expected = values.iter().map(|value| format!("{value}\n")).collect::<String>();
        assert!(expected.len() > RECORDS_CHUNK_TARGET);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(values))])
            .expect("test record batch should be valid");
        let output = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async move { Ok::<_, DataFusionError>(batch) }),
        ));
        let (producer, mut rx, lease_released) = spawn_test_producer(output, 8);

        producer.await.expect("producer should finish successfully");

        let mut records = Vec::new();
        let mut stats_returned = None;
        let mut saw_end = false;
        while let Some(event) = rx.recv().await {
            match event.expect("successful stream should not emit an error") {
                SelectObjectContentEvent::Records(event) => {
                    records.extend_from_slice(event.payload.expect("Records should contain a payload").as_ref());
                }
                SelectObjectContentEvent::Stats(event) => {
                    assert!(stats_returned.is_none(), "Stats should be emitted once");
                    stats_returned = event.details.and_then(|details| details.bytes_returned);
                }
                SelectObjectContentEvent::End(_) => {
                    assert!(stats_returned.is_some(), "End must follow Stats");
                    saw_end = true;
                }
                _ => panic!("finite query should emit only Records, Stats, and End"),
            }
        }

        assert_eq!(records, expected.as_bytes());
        assert_eq!(
            stats_returned,
            Some(i64::try_from(records.len()).expect("test output length should fit in i64"))
        );
        assert!(saw_end);
        assert!(lease_released.await.is_ok(), "End should release the snapshot lease");
    }

    #[tokio::test(start_paused = true)]
    async fn empty_stream_emits_only_stats_then_end() {
        let schema = Arc::new(Schema::empty());
        let output = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::empty::<Result<RecordBatch, DataFusionError>>(),
        ));
        let (producer, mut rx, lease_released) = spawn_test_producer(output, 4);

        producer.await.expect("empty producer should finish successfully");

        let Some(Ok(SelectObjectContentEvent::Stats(stats))) = rx.recv().await else {
            panic!("empty result should start with Stats");
        };
        assert_eq!(stats.details.and_then(|details| details.bytes_returned), Some(0));
        assert!(matches!(rx.recv().await, Some(Ok(SelectObjectContentEvent::End(_)))));
        assert!(rx.recv().await.is_none());
        assert!(lease_released.await.is_ok(), "End should release the snapshot lease");
    }

    #[tokio::test(start_paused = true)]
    async fn successful_stream_serializes_records_stats_and_end_without_error() {
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(vec!["row"]))])
            .expect("test record batch should be valid");
        let output = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async move { Ok::<_, DataFusionError>(batch) }),
        ));
        let (producer, rx, lease_released) = spawn_test_producer(output, 4);
        producer.await.expect("producer should finish successfully");

        let mut byte_stream = SelectObjectContentEventStream::new(ReceiverStream::new(rx)).into_byte_stream();
        let mut encoded = Vec::new();
        while let Some(chunk) = byte_stream.next().await {
            encoded.extend_from_slice(&chunk.expect("event-stream message should serialize"));
        }
        let messages = event_stream_headers(&encoded);
        let event_types = messages
            .iter()
            .filter_map(|headers| {
                headers
                    .iter()
                    .find_map(|(name, value)| (name == ":event-type").then_some(value.as_str()))
            })
            .collect::<Vec<_>>();
        assert_eq!(event_types, ["Records", "Stats", "End"]);
        assert!(!messages.iter().flatten().any(|(name, value)| {
            (name == ":message-type" && value == "error") || name == ":error-code" || name == ":error-message"
        }));
        assert!(lease_released.await.is_ok(), "End should release the snapshot lease");
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_wins_when_eof_becomes_ready_at_same_instant() {
        let output = Box::pin(RecordBatchStreamAdapter::new(
            Arc::new(Schema::empty()),
            futures::stream::unfold((), |_| async {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                None::<(Result<RecordBatch, DataFusionError>, ())>
            }),
        ));
        let (producer, mut rx, lease_released) = spawn_test_producer_with(output, 3, csv_validation(), Duration::from_secs(1));

        tokio::task::yield_now().await;
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        producer.await.expect("producer should finish at the shared deadline");

        let timeout = rx
            .recv()
            .await
            .expect("deadline should send one terminal error")
            .expect_err("deadline terminal event should be an error");
        assert_eq!(timeout.code(), &S3ErrorCode::Busy);
        assert!(rx.recv().await.is_none());
        assert!(lease_released.await.is_ok(), "deadline should release the snapshot lease");
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_wins_when_stream_error_becomes_ready_at_same_instant() {
        let output = Box::pin(RecordBatchStreamAdapter::new(
            Arc::new(Schema::empty()),
            futures::stream::once(async {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                Err(DataFusionError::External(Box::new(SelectError::QueryConcurrencyLimit)))
            }),
        ));
        let (producer, mut rx, lease_released) = spawn_test_producer_with(output, 3, csv_validation(), Duration::from_secs(1));

        tokio::task::yield_now().await;
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        producer.await.expect("producer should finish at the shared deadline");

        let timeout = rx
            .recv()
            .await
            .expect("deadline should send one terminal error")
            .expect_err("terminal event should be an error");
        assert_eq!(timeout.code(), &S3ErrorCode::Busy);
        assert!(rx.recv().await.is_none());
        assert!(lease_released.await.is_ok(), "deadline should release the snapshot lease");
    }

    #[tokio::test(start_paused = true)]
    async fn select_errors_use_http_codes_before_stream_and_error_frames_after_stream() {
        fn csv_error() -> DataFusionError {
            DataFusionError::ArrowError(Box::new(ArrowError::CsvError("private CSV parser state".to_string())), None)
        }
        fn json_error() -> DataFusionError {
            DataFusionError::ArrowError(Box::new(ArrowError::JsonError("private JSON parser state".to_string())), None)
        }
        fn parquet_error() -> DataFusionError {
            DataFusionError::ParquetError(Box::new(datafusion::parquet::errors::ParquetError::General(
                "private Parquet parser state".to_string(),
            )))
        }
        fn truncated_error() -> DataFusionError {
            DataFusionError::External(Box::new(SelectError::TruncatedInput))
        }
        fn timeout_error() -> DataFusionError {
            DataFusionError::External(Box::new(SelectError::QueryTimeout { seconds: 300 }))
        }

        let cases = [
            (
                csv_error as fn() -> DataFusionError,
                S3ErrorCode::CSVParsingError,
                StatusCode::BAD_REQUEST,
                b"CSVParsingError" as &[u8],
            ),
            (json_error, S3ErrorCode::JSONParsingError, StatusCode::BAD_REQUEST, b"JSONParsingError"),
            (
                parquet_error,
                S3ErrorCode::ParquetParsingError,
                StatusCode::BAD_REQUEST,
                b"ParquetParsingError",
            ),
            (truncated_error, S3ErrorCode::TruncatedInput, StatusCode::BAD_REQUEST, b"TruncatedInput"),
            (timeout_error, S3ErrorCode::Busy, StatusCode::SERVICE_UNAVAILABLE, b"Busy"),
        ];

        for (source, expected_code, expected_status, encoded_code) in cases {
            let pre_stream = map_query_error_to_s3(QueryError::from(source()));
            assert_eq!(pre_stream.code(), &expected_code);
            assert_eq!(pre_stream.status_code(), Some(expected_status));
            let expected_code_text = expected_code.as_str().to_string();
            let (status, body) = http_xml_error(pre_stream).await;
            assert_eq!(status, expected_status);
            assert!(body.contains(&format!("<Code>{expected_code_text}</Code>")));
            assert!(body.contains("<Message>"));

            let output = Box::pin(RecordBatchStreamAdapter::new(
                Arc::new(Schema::empty()),
                futures::stream::once(async move { Err(source()) }),
            ));
            let (producer, rx, lease_released) = spawn_test_producer(output, 2);
            producer.await.expect("producer should emit the terminal Select error");

            let mut byte_stream = SelectObjectContentEventStream::new(ReceiverStream::new(rx)).into_byte_stream();
            let mut encoded = Vec::new();
            while let Some(chunk) = byte_stream.next().await {
                encoded.extend_from_slice(&chunk.expect("event-stream message should serialize"));
            }
            let messages = event_stream_headers(&encoded);
            let terminal_headers = messages.last().expect("event stream should contain a terminal error");
            let encoded_code = std::str::from_utf8(encoded_code).expect("test error code should be UTF-8");
            assert!(
                terminal_headers
                    .iter()
                    .any(|(name, value)| name == ":message-type" && value == "error")
            );
            assert!(
                terminal_headers
                    .iter()
                    .any(|(name, value)| name == ":error-code" && value == encoded_code)
            );
            assert!(
                terminal_headers
                    .iter()
                    .any(|(name, value)| name == ":error-message" && !value.is_empty() && !value.contains("private"))
            );
            assert!(
                !messages
                    .iter()
                    .flatten()
                    .any(|(name, value)| { name == ":event-type" && matches!(value.as_str(), "Stats" | "End") })
            );
            assert!(lease_released.await.is_ok(), "error frame should release the snapshot lease");
        }
    }

    #[tokio::test]
    async fn sql_and_compression_errors_serialize_as_http_xml() {
        let sql_error = map_query_error_to_s3(QueryError::Parser {
            source: ParserError::ParserError("unexpected token".to_string()),
        });
        let (sql_status, sql_body) = http_xml_error(sql_error).await;
        assert_eq!(sql_status, StatusCode::BAD_REQUEST);
        assert!(sql_body.contains("<Code>ParseSelectFailure</Code>"));
        assert!(sql_body.contains("<Message>"));

        let mut input = base_input();
        input.request.input_serialization.compression_type = Some(CompressionType::from_static("SNAPPY"));
        let compression_error =
            validate_select_request(&HeaderMap::new(), &mut input).expect_err("unknown compression must fail before streaming");
        let (compression_status, compression_body) = http_xml_error(compression_error).await;
        assert_eq!(compression_status, StatusCode::BAD_REQUEST);
        assert!(compression_body.contains("<Code>InvalidCompressionFormat</Code>"));
        assert!(compression_body.contains("<Message>"));
    }

    #[tokio::test(start_paused = true)]
    async fn stream_error_after_records_omits_stats_and_end() {
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(vec!["row"]))])
            .expect("test record batch should be valid");
        let output = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::iter([
                Ok(batch),
                Err(DataFusionError::ArrowError(
                    Box::new(ArrowError::CsvError("private CSV parser state".to_string())),
                    None,
                )),
            ]),
        ));
        let (producer, rx, lease_released) = spawn_test_producer(output, 3);

        producer
            .await
            .expect("producer should emit records followed by the terminal error");

        let mut byte_stream = SelectObjectContentEventStream::new(ReceiverStream::new(rx)).into_byte_stream();
        let mut encoded = Vec::new();
        while let Some(chunk) = byte_stream.next().await {
            encoded.extend_from_slice(&chunk.expect("event-stream message should serialize"));
        }
        let messages = event_stream_headers(&encoded);
        assert_eq!(
            messages
                .iter()
                .filter_map(|headers| {
                    headers
                        .iter()
                        .find_map(|(name, value)| (name == ":event-type").then_some(value.as_str()))
                })
                .collect::<Vec<_>>(),
            ["Records"]
        );
        let terminal_headers = messages.last().expect("stream should contain a terminal error");
        assert!(
            terminal_headers
                .iter()
                .any(|(name, value)| name == ":message-type" && value == "error")
        );
        assert!(
            terminal_headers
                .iter()
                .any(|(name, value)| name == ":error-code" && value == "CSVParsingError")
        );
        assert!(
            !messages
                .iter()
                .flatten()
                .any(|(name, value)| { name == ":event-type" && matches!(value.as_str(), "Stats" | "End") })
        );
        assert!(lease_released.await.is_ok(), "error should release the snapshot lease");
    }

    #[tokio::test(start_paused = true)]
    async fn encoder_error_uses_reserved_terminal_slot() {
        let values = ListArray::from_iter_primitive::<Int32Type, _, _>([Some([Some(1)])]);
        let schema = Arc::new(Schema::new(vec![Field::new("items", values.data_type().clone(), false)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(values)]).expect("test batch should be valid");
        let output = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async move { Ok::<_, DataFusionError>(batch) }),
        ));
        let (producer, mut rx, lease_released) = spawn_test_producer(output, 2);

        producer.await.expect("producer should not block on a terminal encoder error");

        let encoder_error = rx
            .recv()
            .await
            .expect("encoder failure should send one terminal error")
            .expect_err("terminal event should be an error");
        assert_eq!(encoder_error.code(), &S3ErrorCode::InternalError);
        assert_eq!(encoder_error.message(), Some("An internal error occurred."));
        assert!(encoder_error.source().is_none());
        assert!(rx.recv().await.is_none());
        assert!(lease_released.await.is_ok(), "encoder error should release the snapshot lease");
    }

    #[tokio::test(start_paused = true)]
    async fn producer_drops_query_stream_and_snapshot_lease_when_receiver_closes() {
        let (stream_dropped_tx, stream_dropped_rx) = tokio::sync::oneshot::channel::<()>();
        let output = Box::pin(RecordBatchStreamAdapter::new(
            Arc::new(Schema::empty()),
            futures::stream::once(async move {
                let _stream_dropped = stream_dropped_tx;
                futures::future::pending::<Result<RecordBatch, DataFusionError>>().await
            }),
        ));
        let (event_channel, rx) = test_event_channel(2);
        let (lease, lease_released) = lease_drop_signal();
        let producer = send_select_events_until_deadline(
            output,
            event_channel,
            csv_validation(),
            Arc::new(SelectInputMetrics::default()),
            Instant::now() + Duration::from_secs(300),
            300,
            lease,
        );
        tokio::pin!(producer);

        assert!(futures::poll!(producer.as_mut()).is_pending());
        drop(rx);

        assert!(
            futures::poll!(producer.as_mut()).is_ready(),
            "producer should observe the closed receiver"
        );
        assert!(
            stream_dropped_rx.await.is_err(),
            "query stream should be dropped when the receiver closes"
        );
        assert!(lease_released.await.is_ok(), "receiver close should release the snapshot lease");
    }

    #[tokio::test(start_paused = true)]
    async fn producer_prefers_closed_receiver_over_ready_query_stream() {
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<()>();
        let (stream_polled_tx, stream_polled_rx) = tokio::sync::oneshot::channel::<()>();
        let output = Box::pin(RecordBatchStreamAdapter::new(
            Arc::new(Schema::empty()),
            futures::stream::once(async move {
                ready_rx.await.expect("test should release the query stream");
                let _ = stream_polled_tx.send(());
                Ok(RecordBatch::new_empty(Arc::new(Schema::empty())))
            }),
        ));
        let (mut event_channel, rx) = test_event_channel(2);
        let snapshot_fence = LeaseDropSignal(None);
        let producer = send_select_events(
            output,
            &mut event_channel,
            csv_validation(),
            Arc::new(SelectInputMetrics::default()),
            Instant::now() + Duration::from_secs(300),
            300,
            &snapshot_fence,
        );
        tokio::pin!(producer);

        assert!(futures::poll!(producer.as_mut()).is_pending());
        drop(rx);
        ready_tx.send(()).expect("test should make the query stream ready");

        assert!(
            futures::poll!(producer.as_mut()).is_ready(),
            "producer should prioritize the closed receiver"
        );
        assert!(
            stream_polled_rx.await.is_err(),
            "closed receiver should win before the ready query stream is consumed"
        );
    }

    #[tokio::test]
    async fn producer_rejects_successful_end_when_final_snapshot_fence_fails() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            datafusion::arrow::datatypes::DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(datafusion::arrow::array::StringArray::from(vec!["old-generation"]))],
        )
        .expect("test record batch should be valid");
        let output = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async move { Ok::<_, DataFusionError>(batch) }),
        ));
        let (mut event_channel, mut rx) = test_event_channel(4);

        let outcome = send_select_events(
            output,
            &mut event_channel,
            csv_validation(),
            Arc::new(SelectInputMetrics::default()),
            Instant::now() + Duration::from_secs(300),
            300,
            &FailingSnapshotFence,
        )
        .await;

        let SelectProducerOutcome::Terminal(Err(error)) = outcome else {
            panic!("failed final snapshot fence must produce a terminal error");
        };
        assert_eq!(error.code(), &S3ErrorCode::InternalError);
        assert!(matches!(rx.recv().await, Some(Ok(SelectObjectContentEvent::Records(_)))));
        assert!(rx.try_recv().is_err(), "failed final fence must not enqueue Stats or End");
    }

    #[tokio::test]
    async fn producer_rechecks_snapshot_after_stats_backpressure() {
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(vec!["row"]))])
            .expect("test record batch should be valid");
        let output = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async move { Ok::<_, DataFusionError>(batch) }),
        ));
        let (mut event_channel, mut rx) = test_event_channel(2);
        let snapshot_fence = FailsAfterFirstSnapshotFence(std::sync::atomic::AtomicUsize::new(0));
        let producer = send_select_events(
            output,
            &mut event_channel,
            csv_validation(),
            Arc::new(SelectInputMetrics::default()),
            Instant::now() + Duration::from_secs(300),
            300,
            &snapshot_fence,
        );
        tokio::pin!(producer);

        assert!(futures::poll!(producer.as_mut()).is_pending());
        assert!(futures::poll!(producer.as_mut()).is_pending());
        assert_eq!(snapshot_fence.0.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert!(matches!(rx.recv().await, Some(Ok(SelectObjectContentEvent::Records(_)))));

        let SelectProducerOutcome::Terminal(Err(error)) = producer.await else {
            panic!("snapshot loss during Stats backpressure must reject successful End");
        };
        assert_eq!(error.code(), &S3ErrorCode::InternalError);
        assert_eq!(snapshot_fence.0.load(std::sync::atomic::Ordering::Relaxed), 2);
        assert!(rx.try_recv().is_err(), "snapshot loss must not enqueue Stats or End");
    }

    #[test]
    fn validate_defaults_csv_header_and_compression() {
        let mut input = base_input();
        let validation = validate_select_request(&HeaderMap::new(), &mut input).unwrap();
        assert!(matches!(validation.output_format, SelectOutputFormat::Csv(_)));
        assert_eq!(
            input
                .request
                .input_serialization
                .csv
                .as_ref()
                .and_then(|csv| csv.file_header_info.as_ref())
                .map(|value| value.as_str()),
            Some(FileHeaderInfo::NONE)
        );
        assert_eq!(
            input
                .request
                .input_serialization
                .compression_type
                .as_ref()
                .map(|value| value.as_str()),
            Some(CompressionType::NONE)
        );
    }

    #[test]
    fn validate_rejects_unknown_csv_header_mode_before_streaming() {
        let mut input = base_input();
        input
            .request
            .input_serialization
            .csv
            .as_mut()
            .expect("base input should use CSV")
            .file_header_info = Some(FileHeaderInfo::from_static("INVALID"));

        let error = validate_select_request(&HeaderMap::new(), &mut input).expect_err("unknown header mode must fail");

        assert_eq!(error.code(), &S3ErrorCode::InvalidFileHeaderInfo);
        assert_eq!(error.status_code(), Some(StatusCode::BAD_REQUEST));
        assert_eq!(
            error.message(),
            Some("The FileHeaderInfo value is not valid. Only NONE, USE, and IGNORE are supported.")
        );
    }

    #[test]
    fn validate_accepts_two_byte_csv_input_record_delimiter() {
        let mut input = base_input();
        input
            .request
            .input_serialization
            .csv
            .as_mut()
            .expect("base input should use CSV")
            .record_delimiter = Some("^Y".to_string());

        assert!(validate_select_request(&HeaderMap::new(), &mut input).is_ok());
    }

    #[test]
    fn validate_accepts_supported_multibyte_csv_delimiter_pairs() {
        let mut input = base_input();
        let csv = input
            .request
            .input_serialization
            .csv
            .as_mut()
            .expect("base input should use CSV");
        csv.field_delimiter = Some("\r\n".to_string());
        csv.record_delimiter = Some("^Y".to_string());

        assert!(validate_select_request(&HeaderMap::new(), &mut input).is_ok());

        let csv = input
            .request
            .input_serialization
            .csv
            .as_mut()
            .expect("input should still use CSV");
        csv.field_delimiter = Some("\nX".to_string());
        csv.record_delimiter = None;
        assert!(validate_select_request(&HeaderMap::new(), &mut input).is_ok());

        let csv = input
            .request
            .input_serialization
            .csv
            .as_mut()
            .expect("input should still use CSV");
        csv.field_delimiter = Some("aa".to_string());
        csv.record_delimiter = Some("a".to_string());
        assert!(validate_select_request(&HeaderMap::new(), &mut input).is_ok());
    }

    #[test]
    fn validate_rejects_csv_input_record_delimiter_outside_one_to_two_bytes() {
        for delimiter in ["", "^YZ"] {
            let mut input = base_input();
            input
                .request
                .input_serialization
                .csv
                .as_mut()
                .expect("base input should use CSV")
                .record_delimiter = Some(delimiter.to_string());

            let err = validate_select_request(&HeaderMap::new(), &mut input)
                .expect_err("record delimiter outside the supported length must be rejected");

            assert_eq!(err.code(), &S3ErrorCode::InvalidRequestParameter);
        }
    }

    #[test]
    fn validate_rejects_overlapping_csv_input_delimiters() {
        for (field_delimiter, record_delimiter) in [
            (Some("^"), Some("^")),
            (Some("\r"), Some("\r\n")),
            (Some("a"), Some("aa")),
            (None, Some(",")),
            (Some("\n"), None),
            (Some("||"), Some(",")),
            (Some("\n"), Some("^Y")),
            (Some("\r"), Some("^Y")),
        ] {
            let mut input = base_input();
            let csv = input
                .request
                .input_serialization
                .csv
                .as_mut()
                .expect("base input should use CSV");
            csv.field_delimiter = field_delimiter.map(str::to_string);
            csv.record_delimiter = record_delimiter.map(str::to_string);

            let err = validate_select_request(&HeaderMap::new(), &mut input)
                .expect_err("overlapping field and record delimiters must be rejected");

            assert_eq!(err.code(), &S3ErrorCode::InvalidRequestParameter);
        }
    }

    #[test]
    fn validate_keeps_csv_output_record_delimiter_restriction() {
        let mut input = base_input();
        input
            .request
            .output_serialization
            .csv
            .as_mut()
            .expect("base output should use CSV")
            .record_delimiter = Some("^Y".to_string());

        let err =
            validate_select_request(&HeaderMap::new(), &mut input).expect_err("multi-byte output delimiter must remain rejected");

        assert_eq!(err.code(), &S3ErrorCode::InvalidRequestParameter);
    }

    fn assert_json_encoder_matches_arrow(array: ArrayRef) {
        let schema = Arc::new(Schema::new(vec![Field::new("value", array.data_type().clone(), true)]));
        let batch = RecordBatch::try_new(schema, vec![array]).expect("test record batch should be valid");
        let mut expected = Vec::new();
        {
            let mut writer = WriterBuilder::new()
                .with_explicit_nulls(true)
                .build::<_, LineDelimited>(&mut expected);
            writer.write(&batch).expect("Arrow JSON reference should encode");
            writer.finish().expect("Arrow JSON reference should finish");
        }
        let mut actual = BytesMut::new();
        encode_json_batch(&batch, &JSONOutput::default(), &mut actual).expect("S3 Select JSON should encode");
        assert_eq!(actual.as_ref(), expected.as_slice(), "type: {}", batch.column(0).data_type());
    }

    #[test]
    fn json_encoder_outputs_line_delimited_records() {
        let schema =
            std::sync::Arc::new(datafusion::arrow::datatypes::Schema::new(vec![datafusion::arrow::datatypes::Field::new(
                "name",
                datafusion::arrow::datatypes::DataType::Utf8,
                false,
            )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![std::sync::Arc::new(datafusion::arrow::array::StringArray::from(vec![
                "a", "b",
            ]))],
        )
        .unwrap();

        let mut bytes = BytesMut::new();
        encode_json_batch(&batch, &JSONOutput::default(), &mut bytes).unwrap();
        assert_eq!(bytes.as_ref(), b"{\"name\":\"a\"}\n{\"name\":\"b\"}\n");
    }

    #[test]
    fn json_encoder_honors_custom_record_delimiter() {
        let schema =
            std::sync::Arc::new(datafusion::arrow::datatypes::Schema::new(vec![datafusion::arrow::datatypes::Field::new(
                "name",
                datafusion::arrow::datatypes::DataType::Utf8,
                false,
            )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![std::sync::Arc::new(datafusion::arrow::array::StringArray::from(vec![
                "a", "b",
            ]))],
        )
        .unwrap();

        let mut bytes = BytesMut::new();
        encode_json_batch(
            &batch,
            &JSONOutput {
                record_delimiter: Some("|".to_string()),
            },
            &mut bytes,
        )
        .unwrap();
        assert_eq!(bytes.as_ref(), b"{\"name\":\"a\"}|{\"name\":\"b\"}|");
    }

    #[test]
    fn json_encoder_matches_arrow_for_limited_encoder_variants() {
        let mut fixed_list = FixedSizeListBuilder::new(Int32Builder::new(), 2);
        fixed_list.values().append_value(1);
        fixed_list.values().append_null();
        fixed_list.append(true);

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(LargeStringArray::from(vec!["a\n"])),
            Arc::new(StringViewArray::from(vec!["a\n"])),
            Arc::new(BinaryArray::from(vec![b"\xab".as_slice()])),
            Arc::new(LargeBinaryArray::from(vec![b"\xab".as_slice()])),
            Arc::new(BinaryViewArray::from(vec![b"\xab".as_slice()])),
            Arc::new(
                FixedSizeBinaryArray::try_from_iter([b"\xab".as_slice()].into_iter())
                    .expect("fixed binary test array should be valid"),
            ),
            Arc::new(LargeListArray::from_iter_primitive::<Int32Type, _, _>([Some([Some(1), None])])),
            Arc::new(ListViewArray::from_iter_primitive::<Int32Type, _, _>([Some([Some(1), None])])),
            Arc::new(LargeListViewArray::from_iter_primitive::<Int32Type, _, _>([Some([Some(1), None])])),
            Arc::new(fixed_list.finish()),
        ];

        for array in arrays {
            assert_json_encoder_matches_arrow(array);
        }
    }

    #[test]
    fn csv_encoder_honors_output_delimiters() {
        let schema = std::sync::Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new("name", datafusion::arrow::datatypes::DataType::Utf8, false),
            datafusion::arrow::datatypes::Field::new("score", datafusion::arrow::datatypes::DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                std::sync::Arc::new(datafusion::arrow::array::StringArray::from(vec!["a", "b"])),
                std::sync::Arc::new(datafusion::arrow::array::Int32Array::from(vec![1, 2])),
            ],
        )
        .unwrap();

        let mut bytes = BytesMut::new();
        encode_csv_batch(
            &batch,
            &CSVOutput {
                field_delimiter: Some("|".to_string()),
                record_delimiter: Some("\r\n".to_string()),
                ..Default::default()
            },
            &mut bytes,
        )
        .unwrap();
        assert_eq!(bytes.as_ref(), b"a|1\r\nb|2\r\n");
    }

    #[test]
    fn csv_encoder_matches_select_as_needed_quote_rules() {
        let values = StringArray::from(vec!["", "\\.", "\u{00a0}value", "line\rbreak", "a\"b", "a|b"]);
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(values)]).expect("test record batch should be valid");
        let mut bytes = BytesMut::new();

        encode_csv_batch(
            &batch,
            &CSVOutput {
                quote_escape_character: Some("\\".to_string()),
                quote_fields: Some(QuoteFields::from_static(QuoteFields::ASNEEDED)),
                record_delimiter: Some("|".to_string()),
                ..Default::default()
            },
            &mut bytes,
        )
        .expect("CSV output should encode");

        let expected = [
            b"|".as_slice(),
            br#""\.""#,
            b"|",
            "\"\u{00a0}value\"|".as_bytes(),
            b"\"line\rbreak\"|",
            br#""a\"b"|"#,
            b"a|b|",
        ]
        .concat();
        assert_eq!(bytes.as_ref(), expected);
    }

    #[test]
    fn csv_encoder_honors_always_and_custom_quote_characters() {
        let values = StringArray::from(vec!["plain", "a'b"]);
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(values)]).expect("test record batch should be valid");
        let mut bytes = BytesMut::new();

        encode_csv_batch(
            &batch,
            &CSVOutput {
                quote_character: Some("'".to_string()),
                quote_escape_character: Some("\\".to_string()),
                quote_fields: Some(QuoteFields::from_static(QuoteFields::ALWAYS)),
                record_delimiter: Some("|".to_string()),
                ..Default::default()
            },
            &mut bytes,
        )
        .expect("CSV output should encode");

        assert_eq!(bytes.as_ref(), br#"'plain'|'a\'b'|"#);
    }

    #[tokio::test(start_paused = true)]
    async fn records_staging_preserves_payload_limit_and_returned_bytes() {
        let mut buffer = BytesMut::from(vec![b'x'; RECORDS_CHUNK_TARGET + 7].as_slice());
        let flush = tokio::time::sleep(Duration::from_secs(300));
        tokio::pin!(flush);
        let mut flush_armed = false;
        let mut pending = None;
        schedule_buffered_records(
            &mut buffer,
            flush.as_mut(),
            &mut flush_armed,
            &mut pending,
            Instant::now() + Duration::from_secs(300),
        );
        let Some(SelectObjectContentEvent::Records(records)) = pending else {
            panic!("full payload should flush immediately");
        };
        let first = records.payload.expect("Records should contain a payload");
        assert_eq!(first.len(), RECORDS_CHUNK_TARGET);
        let second = take_records_payload(&mut buffer).expect("remaining payload should stay staged");
        assert_eq!(second.len(), 7);

        let mut progress = SelectProgress::new(Some(Arc::new(SelectInputMetrics::default())));
        progress.add_returned(first.len());
        progress.add_returned(second.len());
        assert_eq!(progress.to_stats().bytes_returned, Some((RECORDS_CHUNK_TARGET + 7) as i64));
    }

    #[tokio::test]
    async fn terminal_error_sends_at_most_one_compat_records_chunk() {
        let (mut event_channel, mut rx) = test_event_channel(2);
        let mut pending_event = None;
        let mut records_buffer = BytesMut::from(vec![b'x'; RECORDS_CHUNK_TARGET + 17].as_slice());
        let mut progress = SelectProgress::new(Some(Arc::new(SelectInputMetrics::default())));

        flush_terminal_records(
            &mut event_channel,
            &mut pending_event,
            &mut records_buffer,
            &mut progress,
            TerminalRecordsMode::PrefixBeforeError,
        )
        .expect("the reserved terminal Records slot should be available");

        let Some(Ok(SelectObjectContentEvent::Records(records))) = rx.recv().await else {
            panic!("the terminal prefix should be sent as Records");
        };
        assert_eq!(records.payload.as_ref().map(Bytes::len), Some(RECORDS_CHUNK_TARGET));
        assert!(records_buffer.is_empty(), "failed output after the terminal prefix must be discarded");
        assert_eq!(progress.to_stats().bytes_returned, Some(RECORDS_CHUNK_TARGET as i64));
    }

    #[tokio::test]
    async fn maximum_records_payload_stays_within_compat_message_limit() {
        let (tx, rx) = mpsc::channel(1);
        tx.send(Ok(records_event(Bytes::from(vec![b'x'; RECORDS_CHUNK_TARGET]))))
            .await
            .expect("test channel should accept Records");
        drop(tx);

        let mut byte_stream = SelectObjectContentEventStream::new(ReceiverStream::new(rx)).into_byte_stream();
        let mut encoded = Vec::new();
        while let Some(chunk) = byte_stream.next().await {
            encoded.extend_from_slice(&chunk.expect("Records event should serialize"));
        }

        let total_len = usize::try_from(u32::from_be_bytes(
            encoded[0..4]
                .try_into()
                .expect("event-stream message should contain a prelude"),
        ))
        .expect("event-stream message length should fit in usize");
        assert_eq!(total_len, encoded.len());
        assert_eq!(total_len, MAX_COMPAT_EVENT_STREAM_MESSAGE_BYTES);
    }

    #[test]
    fn validate_rejects_scan_range_for_json_document_as_request_parameter() {
        let mut input = base_input();
        input.request.input_serialization = InputSerialization {
            csv: None,
            json: Some(JSONInput {
                type_: Some(JSONType::from_static(JSONType::DOCUMENT)),
            }),
            parquet: None,
            compression_type: None,
        };
        input.request.scan_range = Some(ScanRange {
            start: Some(0),
            end: Some(10),
        });

        let err = validate_select_request(&HeaderMap::new(), &mut input).unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequestParameter);
        assert_eq!(err.message(), Some(INVALID_SCAN_RANGE_MESSAGE));
    }

    #[test]
    fn validate_allows_scan_range_for_json_lines_as_request_parameter() {
        let mut input = base_input();
        input.request.input_serialization = InputSerialization {
            csv: None,
            json: Some(JSONInput {
                type_: Some(JSONType::from_static(JSONType::LINES)),
            }),
            parquet: None,
            compression_type: None,
        };
        input.request.scan_range = Some(ScanRange {
            start: Some(0),
            end: Some(10),
        });

        validate_select_request(&HeaderMap::new(), &mut input).expect("json lines scan range should validate");
        validate_scan_range_for_object_size(&input.request, 16)
            .expect("json lines scan range should validate against object size");
    }

    #[test]
    fn validate_allows_scan_range_for_parquet_as_request_parameter() {
        let mut input = base_input();
        input.request.input_serialization = InputSerialization {
            csv: None,
            json: None,
            parquet: Some(ParquetInput {}),
            compression_type: None,
        };
        input.request.scan_range = Some(ScanRange {
            start: Some(0),
            end: Some(10),
        });

        validate_select_request(&HeaderMap::new(), &mut input).expect("parquet scan range should validate");
        validate_scan_range_for_object_size(&input.request, 16).expect("parquet scan range should validate against object size");
    }

    #[test]
    fn validate_allows_scan_range_for_csv_as_request_parameter() {
        let mut input = base_input();
        input.request.scan_range = Some(ScanRange {
            start: Some(0),
            end: Some(10),
        });

        validate_select_request(&HeaderMap::new(), &mut input).expect("csv scan range should validate");
        validate_scan_range_for_object_size(&input.request, 16).expect("csv scan range should validate against object size");
    }

    #[test]
    fn validate_rejects_scan_range_start_after_object() {
        let mut input = base_input();
        input.request.scan_range = Some(ScanRange {
            start: Some(10),
            end: None,
        });

        validate_select_request(&HeaderMap::new(), &mut input).unwrap();
        let err = validate_scan_range_for_object_size(&input.request, 10).unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequestParameter);
        assert_eq!(err.message(), Some(INVALID_SCAN_RANGE_MESSAGE));
    }

    #[test]
    fn validate_rejects_scan_range_start_after_end() {
        let mut input = base_input();
        input.request.scan_range = Some(ScanRange {
            start: Some(20),
            end: Some(10),
        });

        let err = validate_select_request(&HeaderMap::new(), &mut input).unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequestParameter);
        assert_eq!(err.message(), Some(INVALID_SCAN_RANGE_MESSAGE));
    }

    #[test]
    fn validate_allows_scan_range_end_only_suffix_form() {
        let mut input = base_input();
        input.request.scan_range = Some(ScanRange {
            start: None,
            end: Some(35),
        });

        validate_select_request(&HeaderMap::new(), &mut input).unwrap();
        validate_scan_range_for_object_size(&input.request, 10).unwrap();
    }

    #[test]
    fn progress_reports_zero_for_an_empty_input() {
        let mut progress = SelectProgress::new(Some(Arc::new(SelectInputMetrics::default())));
        progress.add_returned(12);
        let stats = progress.to_stats();
        assert_eq!(stats.bytes_returned, Some(12));
        assert_eq!(stats.bytes_scanned, Some(0));
        assert_eq!(stats.bytes_processed, Some(0));
    }

    #[test]
    fn progress_dto_clamps_counters_to_signed_event_range() {
        assert_eq!(clamp_i64(u64::MAX), i64::MAX);
    }

    #[test]
    fn parquet_progress_keeps_input_metrics_unspecified() {
        let mut input = base_input();
        input.request.input_serialization = InputSerialization {
            csv: None,
            json: None,
            parquet: Some(ParquetInput {}),
            compression_type: None,
        };
        let validation = validate_select_request(&HeaderMap::new(), &mut input).expect("Parquet request should validate");
        let progress = SelectProgress::new(
            validation
                .reports_input_metrics
                .then(|| Arc::new(SelectInputMetrics::default())),
        );

        let stats = progress.to_stats();
        assert_eq!(stats.bytes_scanned, None);
        assert_eq!(stats.bytes_processed, None);
        assert_eq!(stats.bytes_returned, Some(0));
    }

    #[test]
    fn map_typed_scan_range_error_to_invalid_request_parameter() {
        let err = map_query_error_to_s3(SelectError::InvalidScanRange.into());
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequestParameter);
        assert_eq!(err.message(), Some(INVALID_SCAN_RANGE_MESSAGE));
    }
}
