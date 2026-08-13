#[cfg(test)]
use super::storage_api::select_object::StorageError;
use super::storage_api::select_object::options::get_opts;
use super::storage_api::select_object::request_context::spawn_traced;
use super::storage_api::select_object::sse::{SseKmsPrincipal, authorize_sse_kms_object_read};
use super::storage_api::select_object::{
    StoragePrepareSelectObjectSnapshotError, StorageSelectObjectSnapshot, get_validated_store, validate_sse_headers_for_read,
    validate_ssec_for_read,
};
use crate::app::runtime_sources::current_s3select_db;
use crate::error::ApiError;
use bytes::Bytes;
use datafusion::arrow::{
    csv::{QuoteStyle, WriterBuilder as CsvWriterBuilder, writer::Terminator},
    json::{WriterBuilder as JsonWriterBuilder, writer::LineDelimited},
    record_batch::RecordBatch,
};
#[cfg(test)]
use datafusion::common::DataFusionError;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header::RANGE};
use rustfs_s3select_api::{
    QueryError, SelectError,
    object_store::{INVALID_SCAN_RANGE_MESSAGE, validate_scan_range_bounds},
    query::{Context, Query},
};
use rustfs_s3select_query::instance::s3_select_query_timeout;
use rustfs_utils::http::headers::{
    AMZ_ENCRYPTION_AES, AMZ_ENCRYPTION_KMS, AMZ_SERVER_SIDE_ENCRYPTION, AMZ_SERVER_SIDE_ENCRYPTION_KMS_CONTEXT,
    AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID, SSEC_ALGORITHM_HEADER, SSEC_KEY_HEADER, SSEC_KEY_MD5_HEADER,
};
use rustfs_utils::http::object_encryption_keys::{
    INTERNAL_ENCRYPTION_KEY_ID_HEADER, MINIO_INTERNAL_ENCRYPTION_KMS_CONTEXT_HEADER, MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER,
    MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER, MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER,
    MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER,
};
use s3s::dto::{
    CSVOutput, CompressionType, ContinuationEvent, EndEvent, ExpressionType, FileHeaderInfo, InputSerialization, JSONInput,
    JSONOutput, JSONType, OutputSerialization, Progress, ProgressEvent, QuoteFields, RecordsEvent, SelectObjectContentEvent,
    SelectObjectContentEventStream, SelectObjectContentInput, SelectObjectContentOutput, SelectObjectContentRequest, Stats,
    StatsEvent,
};
use s3s::header::{
    X_AMZ_SERVER_SIDE_ENCRYPTION, X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID, X_AMZ_SERVER_SIDE_ENCRYPTION_CONTEXT,
    X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM, X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5,
};
use s3s::{S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{Instant, timeout_at};
use tokio_stream::wrappers::ReceiverStream;
use tracing::info;

const MAX_SELECT_EXPRESSION_BYTES: usize = 256 * 1024;
const RECORDS_CHUNK_TARGET: usize = 128 * 1024;
const DATA_SOURCE_PATH_UNSUPPORTED_CODE: &str = "DataSourcePathUnsupported";
const INVALID_QUERY_CODE: &str = "InvalidQuery";
const PARSE_SELECT_FAILURE_CODE: &str = "ParseSelectFailure";
const BUSY_MESSAGE: &str = "The service is unavailable. Try again later.";
const EMPTY_SELECT_EXPRESSION_MESSAGE: &str = "empty SQL expression";
const SLOW_DOWN_MESSAGE: &str = "Reduce your request rate.";
const UNSUPPORTED_SQL_STRUCTURE_MESSAGE: &str = "We encountered an unsupported SQL structure. Check the SQL Reference.";
// No canonical owner exists for the KMS key ARN prefix; keep it local.
const SELECT_KMS_ARN_PREFIX: &str = "arn:aws:kms:";

#[derive(Clone, Debug)]
struct SelectValidation {
    output_format: SelectOutputFormat,
    progress_enabled: bool,
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
    let snapshot = timeout_at(
        query_deadline,
        prepare_select_object_snapshot(&req.headers, &input, read_principal.as_ref()),
    )
    .await
    .map_err(|_| select_query_timeout_error(query_timeout.as_secs()))??;
    validate_scan_range_for_object_size(&input.request, snapshot.logical_size())?;
    let snapshot = Arc::new(snapshot);
    let query =
        Query::new_with_snapshot(Context { input: input.clone() }, input.request.expression.clone(), Arc::clone(&snapshot));
    let output = timeout_at(query_deadline, db.execute_admitted(&query, admission))
        .await
        .map_err(|_| select_query_timeout_error(query_timeout.as_secs()))?
        .map_err(map_query_error_to_s3)?
        .result()
        .into_record_batch_stream()
        .map_err(map_query_error_to_s3)?;

    let (tx, rx) = mpsc::channel::<S3Result<SelectObjectContentEvent>>(9);
    let terminal_permit = tx
        .clone()
        .try_reserve_owned()
        .map_err(|_| map_select_error_to_s3(&SelectError::InternalError))?;
    let response = select_object_response(rx, &snapshot.object_info().user_defined, &req.headers)?;
    spawn_traced(async move {
        send_select_events_until_deadline(
            output,
            tx,
            terminal_permit,
            validation,
            query_deadline,
            query_timeout.as_secs(),
            snapshot,
        )
        .await;
    });

    Ok(response)
}

fn select_object_response(
    rx: mpsc::Receiver<S3Result<SelectObjectContentEvent>>,
    metadata: &HashMap<String, String>,
    request_headers: &HeaderMap,
) -> S3Result<S3Response<SelectObjectContentOutput>> {
    let response_headers = select_snapshot_sse_response_headers(metadata, request_headers)?;
    let mut response = S3Response::new(SelectObjectContentOutput {
        payload: Some(SelectObjectContentEventStream::new(ReceiverStream::new(rx))),
    });
    response.headers = response_headers;
    Ok(response)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SelectSnapshotSseMode {
    S3,
    Kms,
    Customer,
}

fn invalid_select_snapshot_sse_metadata() -> S3Error {
    S3Error::with_message(
        S3ErrorCode::InternalError,
        "Persisted SelectObjectContent encryption metadata is invalid.",
    )
}

fn select_metadata_value<'a>(metadata: &'a HashMap<String, String>, name: &str) -> S3Result<Option<&'a str>> {
    let mut values = metadata
        .iter()
        .filter_map(|(key, value)| key.eq_ignore_ascii_case(name).then_some(value.as_str()));
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.any(|candidate| candidate != value) {
        return Err(invalid_select_snapshot_sse_metadata());
    }
    Ok(Some(value))
}

fn select_snapshot_kms_key_id(metadata: &HashMap<String, String>) -> S3Result<Option<&str>> {
    let values = [
        select_metadata_value(metadata, AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID)?,
        select_metadata_value(metadata, INTERNAL_ENCRYPTION_KEY_ID_HEADER)?,
        select_metadata_value(metadata, MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER)?,
    ];
    let mut resolved = None;
    for value in values.into_iter().flatten() {
        if resolved.is_some_and(|current| current != value) {
            return Err(invalid_select_snapshot_sse_metadata());
        }
        resolved = Some(value);
    }
    Ok(resolved)
}

fn select_snapshot_sse_mode(metadata: &HashMap<String, String>) -> S3Result<Option<SelectSnapshotSseMode>> {
    let public_mode = select_metadata_value(metadata, AMZ_SERVER_SIDE_ENCRYPTION)?;
    let customer_algorithm = select_metadata_value(metadata, SSEC_ALGORITHM_HEADER)?;
    let has_ssec_marker = select_metadata_value(metadata, MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER)?.is_some();
    let has_s3_marker = select_metadata_value(metadata, MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER)?.is_some();
    let has_kms_marker = select_metadata_value(metadata, MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER)?.is_some();

    let public_mode = match public_mode {
        Some(AMZ_ENCRYPTION_AES) => Some(SelectSnapshotSseMode::S3),
        Some(AMZ_ENCRYPTION_KMS) => Some(SelectSnapshotSseMode::Kms),
        Some(_) => return Err(invalid_select_snapshot_sse_metadata()),
        None => None,
    };
    if customer_algorithm.is_some_and(|algorithm| algorithm != AMZ_ENCRYPTION_AES) {
        return Err(invalid_select_snapshot_sse_metadata());
    }

    let resolved = if customer_algorithm.is_some() {
        if public_mode == Some(SelectSnapshotSseMode::Kms) {
            return Err(invalid_select_snapshot_sse_metadata());
        }
        Some(SelectSnapshotSseMode::Customer)
    } else {
        public_mode
    };
    let internal_modes = [
        has_ssec_marker.then_some(SelectSnapshotSseMode::Customer),
        has_s3_marker.then_some(SelectSnapshotSseMode::S3),
        has_kms_marker.then_some(SelectSnapshotSseMode::Kms),
    ];
    for mode in internal_modes.into_iter().flatten() {
        if resolved != Some(mode) {
            return Err(invalid_select_snapshot_sse_metadata());
        }
    }
    if resolved.is_none()
        && metadata
            .keys()
            .any(|key| rustfs_utils::http::is_object_encryption_marker(key))
    {
        return Err(invalid_select_snapshot_sse_metadata());
    }
    Ok(resolved)
}

fn insert_select_snapshot_header(headers: &mut HeaderMap, name: HeaderName, value: &str) -> S3Result<()> {
    let value = HeaderValue::from_str(value).map_err(|_| invalid_select_snapshot_sse_metadata())?;
    headers.insert(name, value);
    Ok(())
}

fn select_snapshot_sse_response_headers(metadata: &HashMap<String, String>, request_headers: &HeaderMap) -> S3Result<HeaderMap> {
    if select_metadata_value(metadata, SSEC_KEY_HEADER)?.is_some()
        || select_metadata_value(metadata, AMZ_SERVER_SIDE_ENCRYPTION_KMS_CONTEXT)?.is_some()
    {
        return Err(invalid_select_snapshot_sse_metadata());
    }
    let Some(mode) = select_snapshot_sse_mode(metadata)? else {
        return Ok(HeaderMap::new());
    };
    let kms_key_id = select_snapshot_kms_key_id(metadata)?;

    let mut response_headers = HeaderMap::with_capacity(3);
    match mode {
        SelectSnapshotSseMode::S3 => {
            if select_metadata_value(metadata, AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID)?.is_some()
                || select_metadata_value(metadata, MINIO_INTERNAL_ENCRYPTION_KMS_CONTEXT_HEADER)?.is_some()
                || select_metadata_value(metadata, SSEC_KEY_MD5_HEADER)?.is_some()
            {
                return Err(invalid_select_snapshot_sse_metadata());
            }
            response_headers.insert(X_AMZ_SERVER_SIDE_ENCRYPTION, HeaderValue::from_static(AMZ_ENCRYPTION_AES));
        }
        SelectSnapshotSseMode::Kms => {
            if select_metadata_value(metadata, SSEC_KEY_MD5_HEADER)?.is_some() {
                return Err(invalid_select_snapshot_sse_metadata());
            }
            let key_id = kms_key_id
                .filter(|key_id| !key_id.is_empty())
                .ok_or_else(invalid_select_snapshot_sse_metadata)?;
            response_headers.insert(X_AMZ_SERVER_SIDE_ENCRYPTION, HeaderValue::from_static(AMZ_ENCRYPTION_KMS));
            if key_id.starts_with(SELECT_KMS_ARN_PREFIX) {
                insert_select_snapshot_header(&mut response_headers, X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID, key_id)?;
            } else {
                insert_select_snapshot_header(
                    &mut response_headers,
                    X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID,
                    &format!("{SELECT_KMS_ARN_PREFIX}{key_id}"),
                )?;
            }
            if let Some(context) = select_metadata_value(metadata, MINIO_INTERNAL_ENCRYPTION_KMS_CONTEXT_HEADER)? {
                let context = HeaderValue::from_str(context).map_err(|_| invalid_select_snapshot_sse_metadata())?;
                let mut validation_headers = HeaderMap::with_capacity(1);
                validation_headers.insert(X_AMZ_SERVER_SIDE_ENCRYPTION_CONTEXT, context.clone());
                super::storage_api::select_object::sse::extract_ssekms_context_from_headers(&validation_headers)
                    .map_err(|_| invalid_select_snapshot_sse_metadata())?;
                response_headers.insert(X_AMZ_SERVER_SIDE_ENCRYPTION_CONTEXT, context);
            }
        }
        SelectSnapshotSseMode::Customer => {
            if kms_key_id.is_some() || select_metadata_value(metadata, MINIO_INTERNAL_ENCRYPTION_KMS_CONTEXT_HEADER)?.is_some() {
                return Err(invalid_select_snapshot_sse_metadata());
            }
            let algorithm = request_headers
                .get(X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM)
                .and_then(|value| value.to_str().ok())
                .filter(|algorithm| *algorithm == AMZ_ENCRYPTION_AES)
                .ok_or_else(invalid_select_snapshot_sse_metadata)?;
            let key_md5 = request_headers
                .get(X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5)
                .and_then(|value| value.to_str().ok())
                .ok_or_else(invalid_select_snapshot_sse_metadata)?;
            let stored_md5 =
                select_metadata_value(metadata, SSEC_KEY_MD5_HEADER)?.ok_or_else(invalid_select_snapshot_sse_metadata)?;
            if stored_md5 != key_md5 {
                return Err(invalid_select_snapshot_sse_metadata());
            }
            insert_select_snapshot_header(&mut response_headers, X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM, algorithm)?;
            insert_select_snapshot_header(&mut response_headers, X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5, key_md5)?;
        }
    }
    Ok(response_headers)
}

async fn send_select_events_until_deadline<L: SelectSnapshotFence>(
    output: SendableRecordBatchStream,
    tx: mpsc::Sender<S3Result<SelectObjectContentEvent>>,
    terminal_permit: mpsc::OwnedPermit<S3Result<SelectObjectContentEvent>>,
    validation: SelectValidation,
    deadline: Instant,
    timeout_seconds: u64,
    snapshot_lease: L,
) {
    let outcome = match timeout_at(deadline, send_select_events(output, &tx, validation, &snapshot_lease)).await {
        Ok(outcome) => outcome,
        Err(_) => SelectProducerOutcome::Terminal(Err(map_query_error_to_s3(
            SelectError::QueryTimeout {
                seconds: timeout_seconds,
            }
            .into(),
        ))),
    };
    if let SelectProducerOutcome::Terminal(event) = outcome {
        terminal_permit.send(event);
    }
    drop(snapshot_lease);
}

async fn send_select_events(
    mut output: SendableRecordBatchStream,
    tx: &mpsc::Sender<S3Result<SelectObjectContentEvent>>,
    validation: SelectValidation,
    snapshot_fence: &impl SelectSnapshotFence,
) -> SelectProducerOutcome {
    let mut encoder = SelectOutputEncoder::new(validation.output_format);
    let mut progress = SelectProgress::default();

    if tx
        .send(Ok(SelectObjectContentEvent::Cont(ContinuationEvent::default())))
        .await
        .is_err()
    {
        return SelectProducerOutcome::ReceiverClosed;
    }

    let receiver_closed = tx.closed();
    tokio::pin!(receiver_closed);
    while let Some(result) = tokio::select! {
        biased;
        _ = &mut receiver_closed => return SelectProducerOutcome::ReceiverClosed,
        result = output.next() => result,
    } {
        let batch = match result {
            Ok(batch) => batch,
            Err(err) => {
                return SelectProducerOutcome::Terminal(Err(map_query_error_to_s3(err.into())));
            }
        };

        match encoder.encode_batch(&batch) {
            Ok(payloads) => {
                for payload in payloads {
                    progress.add_returned(payload.len());
                    if tx
                        .send(Ok(SelectObjectContentEvent::Records(RecordsEvent { payload: Some(payload) })))
                        .await
                        .is_err()
                    {
                        return SelectProducerOutcome::ReceiverClosed;
                    }
                    if validation.progress_enabled
                        && tx
                            .send(Ok(SelectObjectContentEvent::Progress(ProgressEvent {
                                details: Some(progress.to_progress()),
                            })))
                            .await
                            .is_err()
                    {
                        return SelectProducerOutcome::ReceiverClosed;
                    }
                }
            }
            Err(err) => {
                return SelectProducerOutcome::Terminal(Err(err));
            }
        }
    }

    if let Err(error) = snapshot_fence.ensure_snapshot_valid() {
        return SelectProducerOutcome::Terminal(Err(error));
    }
    let stats = SelectObjectContentEvent::Stats(StatsEvent {
        details: Some(progress.to_stats()),
    });
    let stats_permit = match tx.reserve().await {
        Ok(permit) => permit,
        Err(_) => return SelectProducerOutcome::ReceiverClosed,
    };
    if let Err(error) = snapshot_fence.ensure_snapshot_valid() {
        return SelectProducerOutcome::Terminal(Err(error));
    }
    stats_permit.send(Ok(stats));
    SelectProducerOutcome::Terminal(Ok(SelectObjectContentEvent::End(EndEvent::default())))
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
) -> S3Result<StorageSelectObjectSnapshot> {
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
    Ok(snapshot)
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

    fn encode_batch(&mut self, batch: &RecordBatch) -> S3Result<Vec<Bytes>> {
        let bytes = match &self.format {
            SelectOutputFormat::Csv(config) => encode_csv_batch(batch, config)?,
            SelectOutputFormat::Json(config) => encode_json_batch(batch, config)?,
        };
        Ok(split_records_payload(bytes))
    }
}

fn encode_csv_batch(batch: &RecordBatch, config: &CSVOutput) -> S3Result<Vec<u8>> {
    let mut buffer = Vec::new();
    let mut builder = CsvWriterBuilder::new().with_header(false);
    if let Some(delimiter) = config.field_delimiter.as_deref() {
        builder = builder.with_delimiter(delimiter.as_bytes()[0]);
    }
    if let Some(quote) = config.quote_character.as_deref() {
        builder = builder.with_quote(quote.as_bytes()[0]);
    }
    if let Some(escape) = config.quote_escape_character.as_deref() {
        builder = builder.with_escape(escape.as_bytes()[0]);
    }
    if let Some(record_delimiter) = config.record_delimiter.as_deref() {
        builder = builder.with_line_terminator(csv_terminator(record_delimiter));
    }
    if let Some(quote_fields) = config.quote_fields.as_ref()
        && quote_fields.as_str() == QuoteFields::ALWAYS
    {
        builder = builder.with_quote_style(QuoteStyle::Always);
    }

    let mut writer = builder.build(&mut buffer);
    writer.write(batch).map_err(internal_select_error)?;
    drop(writer);
    Ok(buffer)
}

fn csv_terminator(value: &str) -> Terminator {
    if value == "\r\n" {
        Terminator::CRLF
    } else {
        Terminator::Any(value.as_bytes()[0])
    }
}

fn encode_json_batch(batch: &RecordBatch, config: &JSONOutput) -> S3Result<Vec<u8>> {
    let mut buffer = Vec::new();
    let mut writer = JsonWriterBuilder::new()
        .with_explicit_nulls(true)
        .build::<_, LineDelimited>(&mut buffer);
    writer.write(batch).map_err(internal_select_error)?;
    writer.finish().map_err(internal_select_error)?;
    drop(writer);

    if let Some(delimiter) = config.record_delimiter.as_deref()
        && delimiter != "\n"
    {
        return Ok(replace_json_record_delimiter(&buffer, delimiter.as_bytes()));
    }
    Ok(buffer)
}

fn replace_json_record_delimiter(buffer: &[u8], delimiter: &[u8]) -> Vec<u8> {
    let mut output = Vec::with_capacity(buffer.len());
    for byte in buffer {
        if *byte == b'\n' {
            output.extend_from_slice(delimiter);
        } else {
            output.push(*byte);
        }
    }
    output
}

fn split_records_payload(bytes: Vec<u8>) -> Vec<Bytes> {
    if bytes.is_empty() {
        return Vec::new();
    }
    let bytes = Bytes::from(bytes);
    if bytes.len() <= RECORDS_CHUNK_TARGET {
        return vec![bytes];
    }
    (0..bytes.len())
        .step_by(RECORDS_CHUNK_TARGET)
        .map(|start| bytes.slice(start..(start + RECORDS_CHUNK_TARGET).min(bytes.len())))
        .collect()
}

#[derive(Default)]
struct SelectProgress {
    bytes_returned: u64,
}

impl SelectProgress {
    fn add_returned(&mut self, bytes: usize) {
        self.bytes_returned = self.bytes_returned.saturating_add(bytes as u64);
    }

    fn to_progress(&self) -> Progress {
        Progress {
            bytes_processed: None,
            bytes_returned: Some(clamp_i64(self.bytes_returned)),
            bytes_scanned: None,
        }
    }

    fn to_stats(&self) -> Stats {
        Stats {
            bytes_processed: None,
            bytes_returned: Some(clamp_i64(self.bytes_returned)),
            bytes_scanned: None,
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
            array::{Array, ListArray, StringArray},
            datatypes::{DataType, Field, Int32Type, Schema},
            error::ArrowError,
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
        }
    }

    #[test]
    fn select_snapshot_sse_s3_headers_are_whitelisted() {
        let metadata = HashMap::from([
            (AMZ_SERVER_SIDE_ENCRYPTION.to_string(), AMZ_ENCRYPTION_AES.to_string()),
            (INTERNAL_ENCRYPTION_KEY_ID_HEADER.to_string(), "default".to_string()),
            (MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER.to_string(), "default".to_string()),
            ("x-amz-meta-private".to_string(), "private-value".to_string()),
        ]);

        let headers = select_snapshot_sse_response_headers(&metadata, &HeaderMap::new())
            .expect("valid SSE-S3 snapshot metadata should project response headers");

        assert_eq!(headers.len(), 1);
        assert_eq!(headers.get(X_AMZ_SERVER_SIDE_ENCRYPTION).expect("SSE-S3 mode"), "AES256");
        assert!(headers.get("x-amz-meta-private").is_none());
    }

    #[test]
    fn select_snapshot_sse_kms_headers_use_snapshot_metadata() {
        let context = "eyJ0ZW5hbnQiOiJvbmUifQ==";
        for key_id in ["key-1", "arn:aws:kms:key-2"] {
            let metadata = HashMap::from([
                (AMZ_SERVER_SIDE_ENCRYPTION.to_string(), "aws:kms".to_string()),
                (AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID.to_string(), key_id.to_string()),
                (INTERNAL_ENCRYPTION_KEY_ID_HEADER.to_string(), key_id.to_string()),
                (MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER.to_string(), key_id.to_string()),
                (MINIO_INTERNAL_ENCRYPTION_KMS_CONTEXT_HEADER.to_string(), context.to_string()),
            ]);

            let headers = select_snapshot_sse_response_headers(&metadata, &HeaderMap::new())
                .expect("valid SSE-KMS snapshot metadata should project response headers");
            let expected_key_id = if key_id.starts_with(SELECT_KMS_ARN_PREFIX) {
                key_id.to_string()
            } else {
                format!("{SELECT_KMS_ARN_PREFIX}{key_id}")
            };

            assert_eq!(headers.get(X_AMZ_SERVER_SIDE_ENCRYPTION).expect("SSE-KMS mode"), "aws:kms");
            assert_eq!(
                headers
                    .get(X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID)
                    .expect("SSE-KMS key ID")
                    .to_str()
                    .expect("SSE-KMS key ID should be valid text"),
                expected_key_id
            );
            assert_eq!(headers.get(X_AMZ_SERVER_SIDE_ENCRYPTION_CONTEXT).expect("SSE-KMS context"), context);
        }
    }

    #[test]
    fn select_snapshot_sse_c_headers_never_echo_the_customer_key() {
        let key_md5 = "customer-key-md5";
        let metadata = HashMap::from([
            (AMZ_SERVER_SIDE_ENCRYPTION.to_string(), AMZ_ENCRYPTION_AES.to_string()),
            (SSEC_ALGORITHM_HEADER.to_string(), AMZ_ENCRYPTION_AES.to_string()),
            (SSEC_KEY_MD5_HEADER.to_string(), key_md5.to_string()),
            ("x-amz-meta-private".to_string(), "private-value".to_string()),
        ]);
        let mut request_headers = HeaderMap::new();
        request_headers.insert(X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM, HeaderValue::from_static("AES256"));
        request_headers.insert(X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5, HeaderValue::from_static(key_md5));
        request_headers.insert(
            http::HeaderName::from_static(SSEC_KEY_HEADER),
            HeaderValue::from_static("must-not-be-returned"),
        );

        let headers = select_snapshot_sse_response_headers(&metadata, &request_headers)
            .expect("validated SSE-C request values should project response headers");

        assert_eq!(headers.len(), 2);
        assert_eq!(
            headers
                .get(X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM)
                .expect("SSE-C algorithm"),
            "AES256"
        );
        assert_eq!(
            headers
                .get(X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5)
                .expect("SSE-C key MD5"),
            key_md5
        );
        assert!(headers.get(SSEC_KEY_HEADER).is_none());
        assert!(headers.get("x-amz-meta-private").is_none());
    }

    #[test]
    fn select_snapshot_sse_headers_fail_closed_on_corrupt_metadata() {
        let invalid_context = "not-base64";
        let persisted_key = "must-not-leak";
        let corrupt_metadata = [
            HashMap::from([
                (AMZ_SERVER_SIDE_ENCRYPTION.to_ascii_lowercase(), "AES256".to_string()),
                (AMZ_SERVER_SIDE_ENCRYPTION.to_ascii_uppercase(), "aws:kms".to_string()),
            ]),
            HashMap::from([
                (AMZ_SERVER_SIDE_ENCRYPTION.to_string(), "aws:kms".to_string()),
                (SSEC_ALGORITHM_HEADER.to_string(), "AES256".to_string()),
            ]),
            HashMap::from([(MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER.to_string(), "sealed".to_string())]),
            HashMap::from([
                (AMZ_SERVER_SIDE_ENCRYPTION.to_string(), "aws:kms".to_string()),
                (AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID.to_string(), "key-1".to_string()),
                (INTERNAL_ENCRYPTION_KEY_ID_HEADER.to_string(), "key-2".to_string()),
            ]),
            HashMap::from([
                (AMZ_SERVER_SIDE_ENCRYPTION.to_string(), "aws:kms".to_string()),
                (AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID.to_string(), "key-1".to_string()),
                (MINIO_INTERNAL_ENCRYPTION_KMS_CONTEXT_HEADER.to_string(), invalid_context.to_string()),
            ]),
            HashMap::from([
                (AMZ_SERVER_SIDE_ENCRYPTION.to_string(), AMZ_ENCRYPTION_KMS.to_string()),
                (AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID.to_string(), "key-1".to_string()),
                (AMZ_SERVER_SIDE_ENCRYPTION_KMS_CONTEXT.to_string(), "persisted-context".to_string()),
            ]),
            HashMap::from([
                (SSEC_ALGORITHM_HEADER.to_string(), "AES256".to_string()),
                (SSEC_KEY_MD5_HEADER.to_string(), "customer-key-md5".to_string()),
                (SSEC_KEY_HEADER.to_string(), persisted_key.to_string()),
            ]),
        ];

        for metadata in corrupt_metadata {
            let error = select_snapshot_sse_response_headers(&metadata, &HeaderMap::new())
                .expect_err("corrupt snapshot encryption metadata must fail closed");
            assert_eq!(error.code(), &S3ErrorCode::InternalError);
            assert!(!error.to_string().contains(invalid_context));
            assert!(!error.to_string().contains(persisted_key));
        }
    }

    #[test]
    fn select_response_projects_snapshot_sse_headers() {
        let (_tx, rx) = mpsc::channel(1);
        let metadata = HashMap::from([(AMZ_SERVER_SIDE_ENCRYPTION.to_string(), AMZ_ENCRYPTION_AES.to_string())]);

        let response = select_object_response(rx, &metadata, &HeaderMap::new())
            .expect("valid snapshot metadata should produce a Select response");

        assert_eq!(
            response
                .headers
                .get(X_AMZ_SERVER_SIDE_ENCRYPTION)
                .expect("snapshot SSE response header"),
            AMZ_ENCRYPTION_AES
        );
    }

    fn spawn_test_producer(
        output: SendableRecordBatchStream,
        channel_capacity: usize,
    ) -> (
        tokio::task::JoinHandle<()>,
        mpsc::Receiver<S3Result<SelectObjectContentEvent>>,
        tokio::sync::oneshot::Receiver<()>,
    ) {
        let (tx, rx) = mpsc::channel(channel_capacity);
        let terminal_permit = tx
            .clone()
            .try_reserve_owned()
            .expect("test channel should reserve terminal capacity");
        let (lease, lease_released) = lease_drop_signal();
        let producer = tokio::spawn(send_select_events_until_deadline(
            output,
            tx,
            terminal_permit,
            csv_validation(),
            Instant::now() + std::time::Duration::from_secs(1),
            300,
            lease,
        ));
        (producer, rx, lease_released)
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
        let output = Box::pin(RecordBatchStreamAdapter::new(
            Arc::new(Schema::empty()),
            futures::stream::pending::<Result<RecordBatch, DataFusionError>>(),
        ));
        let (tx, mut rx) = mpsc::channel(2);
        let terminal_permit = tx
            .clone()
            .try_reserve_owned()
            .expect("test channel should reserve terminal capacity");
        tx.send(Ok(SelectObjectContentEvent::Cont(ContinuationEvent::default())))
            .await
            .expect("test channel should accept the prefilled event");
        let (lease, lease_released) = lease_drop_signal();
        let producer = tokio::spawn(send_select_events_until_deadline(
            output,
            tx,
            terminal_permit,
            csv_validation(),
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

        assert!(matches!(rx.recv().await, Some(Ok(SelectObjectContentEvent::Cont(_)))));
        for expected in [b"a\n".as_slice(), b"b\n".as_slice()] {
            let Some(Ok(SelectObjectContentEvent::Records(records))) = rx.recv().await else {
                panic!("producer should emit a records event for each batch");
            };
            assert_eq!(records.payload.as_deref(), Some(expected));
        }
        let Some(Ok(SelectObjectContentEvent::Stats(stats))) = rx.recv().await else {
            panic!("producer should emit final stats");
        };
        assert_eq!(stats.details.and_then(|details| details.bytes_returned), Some(4));
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
        assert_eq!(event_types, ["Cont", "Records", "Stats", "End"]);
        assert!(!messages.iter().flatten().any(|(name, value)| {
            (name == ":message-type" && value == "error") || name == ":error-code" || name == ":error-message"
        }));
        assert!(lease_released.await.is_ok(), "End should release the snapshot lease");
    }

    #[tokio::test(start_paused = true)]
    async fn eof_at_deadline_uses_reserved_slot_for_stats_then_end() {
        let output = Box::pin(RecordBatchStreamAdapter::new(
            Arc::new(Schema::empty()),
            futures::stream::unfold((), |_| async {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                None::<(Result<RecordBatch, DataFusionError>, ())>
            }),
        ));
        let (producer, mut rx, lease_released) = spawn_test_producer(output, 3);

        tokio::task::yield_now().await;
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        producer.await.expect("producer should finish at the shared deadline");

        assert!(matches!(rx.recv().await, Some(Ok(SelectObjectContentEvent::Cont(_)))));
        let stats = rx
            .recv()
            .await
            .expect("successful Select should send stats")
            .expect("stats event should not be an error");
        assert!(matches!(stats, SelectObjectContentEvent::Stats(_)));
        assert!(matches!(rx.recv().await, Some(Ok(SelectObjectContentEvent::End(_)))));
        assert!(rx.recv().await.is_none());
        assert!(lease_released.await.is_ok(), "EOF should release the snapshot lease");
    }

    #[tokio::test(start_paused = true)]
    async fn stream_error_at_deadline_uses_reserved_terminal_slot() {
        let output = Box::pin(RecordBatchStreamAdapter::new(
            Arc::new(Schema::empty()),
            futures::stream::once(async {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                Err(DataFusionError::External(Box::new(SelectError::QueryConcurrencyLimit)))
            }),
        ));
        let (producer, mut rx, lease_released) = spawn_test_producer(output, 2);

        tokio::task::yield_now().await;
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        producer.await.expect("producer should finish at the shared deadline");

        assert!(matches!(rx.recv().await, Some(Ok(SelectObjectContentEvent::Cont(_)))));
        let stream_error = rx
            .recv()
            .await
            .expect("stream failure should send one terminal error")
            .expect_err("terminal event should be an error");
        assert_eq!(stream_error.code(), &S3ErrorCode::SlowDown);
        assert!(rx.recv().await.is_none());
        assert!(lease_released.await.is_ok(), "stream error should release the snapshot lease");
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
            ["Cont", "Records"]
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

        tokio::task::yield_now().await;
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        producer.await.expect("producer should not block on a terminal encoder error");

        assert!(matches!(rx.recv().await, Some(Ok(SelectObjectContentEvent::Cont(_)))));
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
        let (tx, mut rx) = mpsc::channel(2);
        let terminal_permit = tx
            .clone()
            .try_reserve_owned()
            .expect("test channel should reserve terminal capacity");
        let (lease, lease_released) = lease_drop_signal();
        let producer = send_select_events_until_deadline(
            output,
            tx,
            terminal_permit,
            csv_validation(),
            Instant::now() + std::time::Duration::from_secs(1),
            300,
            lease,
        );
        tokio::pin!(producer);

        assert!(futures::poll!(producer.as_mut()).is_pending());
        assert!(matches!(rx.recv().await, Some(Ok(SelectObjectContentEvent::Cont(_)))));
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
        let (tx, mut rx) = mpsc::channel(2);
        let snapshot_fence = LeaseDropSignal(None);
        let producer = send_select_events(output, &tx, csv_validation(), &snapshot_fence);
        tokio::pin!(producer);

        assert!(futures::poll!(producer.as_mut()).is_pending());
        assert!(matches!(rx.recv().await, Some(Ok(SelectObjectContentEvent::Cont(_)))));
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
        let (tx, mut rx) = mpsc::channel(4);

        let outcome = send_select_events(output, &tx, csv_validation(), &FailingSnapshotFence).await;

        let SelectProducerOutcome::Terminal(Err(error)) = outcome else {
            panic!("failed final snapshot fence must produce a terminal error");
        };
        assert_eq!(error.code(), &S3ErrorCode::InternalError);
        assert!(matches!(rx.recv().await, Some(Ok(SelectObjectContentEvent::Cont(_)))));
        assert!(matches!(rx.recv().await, Some(Ok(SelectObjectContentEvent::Records(_)))));
        assert!(rx.try_recv().is_err(), "failed final fence must not enqueue Stats or End");
    }

    #[tokio::test]
    async fn producer_rechecks_snapshot_after_stats_backpressure() {
        let output = Box::pin(RecordBatchStreamAdapter::new(
            Arc::new(Schema::empty()),
            futures::stream::empty::<Result<RecordBatch, DataFusionError>>(),
        ));
        let (tx, mut rx) = mpsc::channel(2);
        let _terminal_permit = tx
            .clone()
            .try_reserve_owned()
            .expect("test channel should reserve terminal capacity");
        let snapshot_fence = FailsAfterFirstSnapshotFence(std::sync::atomic::AtomicUsize::new(0));
        let producer = send_select_events(output, &tx, csv_validation(), &snapshot_fence);
        tokio::pin!(producer);

        assert!(futures::poll!(producer.as_mut()).is_pending());
        assert_eq!(snapshot_fence.0.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert!(matches!(rx.recv().await, Some(Ok(SelectObjectContentEvent::Cont(_)))));

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

        let bytes = encode_json_batch(&batch, &JSONOutput::default()).unwrap();
        let output = String::from_utf8(bytes).unwrap();
        assert_eq!(output, "{\"name\":\"a\"}\n{\"name\":\"b\"}\n");
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

        let bytes = encode_json_batch(
            &batch,
            &JSONOutput {
                record_delimiter: Some("|".to_string()),
            },
        )
        .unwrap();
        let output = String::from_utf8(bytes).unwrap();
        assert_eq!(output, "{\"name\":\"a\"}|{\"name\":\"b\"}|");
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

        let bytes = encode_csv_batch(
            &batch,
            &CSVOutput {
                field_delimiter: Some("|".to_string()),
                record_delimiter: Some("\r\n".to_string()),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(String::from_utf8(bytes).unwrap(), "a|1\r\nb|2\r\n");
    }

    #[test]
    fn split_records_payload_uses_exact_returned_bytes() {
        let payloads = split_records_payload(vec![b'x'; RECORDS_CHUNK_TARGET + 7]);
        let mut progress = SelectProgress::default();
        for payload in &payloads {
            progress.add_returned(payload.len());
        }
        assert_eq!(progress.to_stats().bytes_returned, Some((RECORDS_CHUNK_TARGET + 7) as i64));
        assert!(payloads.len() > 1);
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
    fn progress_does_not_report_unknown_input_bytes_as_zero() {
        let mut progress = SelectProgress::default();
        progress.add_returned(12);
        let stats = progress.to_stats();
        assert_eq!(stats.bytes_returned, Some(12));
        assert_eq!(stats.bytes_scanned, None);
        assert_eq!(stats.bytes_processed, None);
    }

    #[test]
    fn map_typed_scan_range_error_to_invalid_request_parameter() {
        let err = map_query_error_to_s3(SelectError::InvalidScanRange.into());
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequestParameter);
        assert_eq!(err.message(), Some(INVALID_SCAN_RANGE_MESSAGE));
    }
}
