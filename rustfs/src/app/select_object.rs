use super::storage_api::select_object::contract::object::ObjectOperations as _;
use super::storage_api::select_object::options::get_opts;
use super::storage_api::select_object::request_context::spawn_traced;
use super::storage_api::select_object::{get_validated_store, validate_sse_headers_for_read, validate_ssec_for_read};
use crate::app::runtime_sources::current_s3select_db;
use crate::error::ApiError;
use bytes::Bytes;
use datafusion::arrow::{
    csv::{QuoteStyle, WriterBuilder as CsvWriterBuilder, writer::Terminator},
    json::{WriterBuilder as JsonWriterBuilder, writer::LineDelimited},
    record_batch::RecordBatch,
};
use datafusion::common::DataFusionError;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use http::{StatusCode, header::RANGE};
use rustfs_s3select_api::{
    QueryError, S3SelectPolicyError,
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
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{Instant, timeout_at};
use tokio_stream::wrappers::ReceiverStream;
use tracing::info;

const MAX_SELECT_EXPRESSION_BYTES: usize = 256 * 1024;
const RECORDS_CHUNK_TARGET: usize = 128 * 1024;
const PARSE_SELECT_FAILURE_CODE: &str = "ParseSelectFailure";
const EMPTY_SELECT_EXPRESSION_MESSAGE: &str = "empty SQL expression";

#[derive(Clone, Debug)]
struct SelectValidation {
    output_format: SelectOutputFormat,
    progress_enabled: bool,
}

struct SelectObjectMetadata {
    size: u64,
}

#[derive(Clone, Debug)]
enum SelectOutputFormat {
    Csv(CSVOutput),
    Json(JSONOutput),
}

pub async fn execute_select_object_content(
    req: S3Request<SelectObjectContentInput>,
) -> S3Result<S3Response<SelectObjectContentOutput>> {
    let mut input = req.input;
    let validation = validate_select_request(&req.headers, &mut input)?;
    log_select_request_summary(&input, &validation);
    let metadata = preflight_select_object(&req.headers, &input).await?;
    validate_scan_range_for_object_size(&input.request, metadata.size)?;

    let input = Arc::new(input);
    let query_timeout = s3_select_query_timeout();
    let query_deadline = Instant::now() + query_timeout;
    let db = current_s3select_db((*input).clone(), false)
        .await
        .map_err(map_query_error_to_s3)?;
    let query = Query::new(Context { input: input.clone() }, input.request.expression.clone());
    let output = db
        .execute(&query)
        .await
        .map_err(map_query_error_to_s3)?
        .result()
        .into_record_batch_stream()
        .map_err(map_query_error_to_s3)?;

    let (tx, rx) = mpsc::channel::<S3Result<SelectObjectContentEvent>>(9);
    let terminal_permit = tx
        .clone()
        .try_reserve_owned()
        .map_err(|_| s3_error!(InternalError, "can't reserve Select terminal event capacity"))?;
    spawn_traced(async move {
        send_select_events_until_deadline(output, tx, terminal_permit, validation, query_deadline, query_timeout.as_secs()).await;
    });

    Ok(S3Response::new(SelectObjectContentOutput {
        payload: Some(SelectObjectContentEventStream::new(ReceiverStream::new(rx))),
    }))
}

async fn send_select_events_until_deadline(
    output: SendableRecordBatchStream,
    tx: mpsc::Sender<S3Result<SelectObjectContentEvent>>,
    terminal_permit: mpsc::OwnedPermit<S3Result<SelectObjectContentEvent>>,
    validation: SelectValidation,
    deadline: Instant,
    timeout_seconds: u64,
) {
    if timeout_at(deadline, send_select_events(output, &tx, validation))
        .await
        .is_err()
    {
        terminal_permit.send(Err(map_query_error_to_s3(
            S3SelectPolicyError::QueryTimeout {
                seconds: timeout_seconds,
            }
            .into(),
        )));
    }
}

async fn send_select_events(
    mut output: SendableRecordBatchStream,
    tx: &mpsc::Sender<S3Result<SelectObjectContentEvent>>,
    validation: SelectValidation,
) {
    let mut encoder = SelectOutputEncoder::new(validation.output_format);
    let mut progress = SelectProgress::default();

    if tx
        .send(Ok(SelectObjectContentEvent::Cont(ContinuationEvent::default())))
        .await
        .is_err()
    {
        return;
    }

    while let Some(result) = output.next().await {
        let batch = match result {
            Ok(batch) => batch,
            Err(err) => {
                let _ = tx.send(Err(map_query_error_to_s3(err.into()))).await;
                return;
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
                        return;
                    }
                    if validation.progress_enabled
                        && tx
                            .send(Ok(SelectObjectContentEvent::Progress(ProgressEvent {
                                details: Some(progress.to_progress()),
                            })))
                            .await
                            .is_err()
                    {
                        return;
                    }
                }
            }
            Err(err) => {
                let _ = tx.send(Err(err)).await;
                return;
            }
        }
    }

    let stats = SelectObjectContentEvent::Stats(StatsEvent {
        details: Some(progress.to_stats()),
    });
    if tx.send(Ok(stats)).await.is_err() {
        return;
    }
    let _ = tx.send(Ok(SelectObjectContentEvent::End(EndEvent::default()))).await;
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
        return Err(parse_select_failure(EMPTY_SELECT_EXPRESSION_MESSAGE));
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

    if let Some(compression) = input.compression_type.as_ref()
        && compression.as_str() != CompressionType::NONE
    {
        return Err(s3_error!(
            NotImplemented,
            "SelectObjectContent currently supports only uncompressed input"
        ));
    }
    input.compression_type = Some(CompressionType::from_static(CompressionType::NONE));

    if let Some(csv) = input.csv.as_mut() {
        if csv.allow_quoted_record_delimiter.unwrap_or(false) {
            return Err(s3_error!(
                NotImplemented,
                "CSV AllowQuotedRecordDelimiter is not supported by SelectObjectContent"
            ));
        }
        csv.file_header_info
            .get_or_insert_with(|| FileHeaderInfo::from_static(FileHeaderInfo::NONE));
        validate_single_byte(csv.comments.as_deref(), S3ErrorCode::InvalidRequestParameter)?;
        validate_single_byte(csv.quote_character.as_deref(), S3ErrorCode::InvalidRequestParameter)?;
        validate_single_byte(csv.quote_escape_character.as_deref(), S3ErrorCode::InvalidRequestParameter)?;
        validate_record_delimiter(csv.record_delimiter.as_deref())?;
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
        validate_record_delimiter(csv.record_delimiter.as_deref())?;
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

fn parse_select_failure(message: impl Into<String>) -> S3Error {
    let mut err = S3Error::with_message(S3ErrorCode::Custom(PARSE_SELECT_FAILURE_CODE.into()), message.into());
    err.set_status_code(StatusCode::BAD_REQUEST);
    err
}

fn validate_single_byte(value: Option<&str>, code: S3ErrorCode) -> S3Result<()> {
    if let Some(value) = value
        && value.len() != 1
    {
        return Err(S3Error::new(code));
    }
    Ok(())
}

fn validate_record_delimiter(value: Option<&str>) -> S3Result<()> {
    if let Some(value) = value
        && value.len() != 1
        && value != "\r\n"
    {
        return Err(S3Error::new(S3ErrorCode::InvalidRequestParameter));
    }
    Ok(())
}

async fn preflight_select_object(headers: &http::HeaderMap, input: &SelectObjectContentInput) -> S3Result<SelectObjectMetadata> {
    let opts = get_opts(&input.bucket, &input.key, None, None, headers)
        .await
        .map_err(ApiError::from)?;
    let store = get_validated_store(&input.bucket).await?;
    let info = store
        .get_object_info(&input.bucket, &input.key, &opts)
        .await
        .map_err(ApiError::from)?;
    validate_sse_headers_for_read(&info.user_defined, headers)?;
    validate_ssec_for_read(&info.user_defined, input.sse_customer_key.as_ref(), input.sse_customer_key_md5.as_ref())?;
    Ok(SelectObjectMetadata {
        size: info.size.max(0) as u64,
    })
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
    writer
        .write(batch)
        .map_err(|err| s3_error!(InternalError, "can't encode Select output to CSV: {}", err))?;
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
    writer
        .write(batch)
        .map_err(|err| s3_error!(InternalError, "can't encode Select output to JSON: {}", err))?;
    writer
        .finish()
        .map_err(|err| s3_error!(InternalError, "can't finish Select JSON output: {}", err))?;
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
    if let Some(policy_error) = err.s3_select_policy_error() {
        let message = policy_error.to_string();
        return match policy_error {
            S3SelectPolicyError::UnsupportedSqlStructure { .. } => {
                S3Error::with_message(S3ErrorCode::UnsupportedSqlStructure, message)
            }
            S3SelectPolicyError::QueryConcurrencyLimit => S3Error::with_message(S3ErrorCode::SlowDown, message),
            S3SelectPolicyError::QueryTimeout { .. } => S3Error::with_message(S3ErrorCode::Busy, message),
            _ => S3Error::with_message(S3ErrorCode::InternalError, message),
        };
    }
    let message = err.to_string();
    match err {
        QueryError::Parser { .. } => parse_select_failure(message),
        QueryError::MultiStatement { .. } => S3Error::with_message(S3ErrorCode::UnsupportedSqlStructure, message),
        QueryError::NotImplemented { .. } => S3Error::with_message(S3ErrorCode::NotImplemented, message),
        QueryError::Datafusion { source } if is_resource_exhausted(source.as_ref()) => {
            S3Error::with_message(S3ErrorCode::Busy, message)
        }
        QueryError::Datafusion { source } if is_unexpected_eof(source.as_ref()) => {
            S3Error::with_message(S3ErrorCode::InternalError, message)
        }
        QueryError::Datafusion { source } if is_invalid_object_size(source.as_ref()) => {
            S3Error::with_message(S3ErrorCode::InternalError, message)
        }
        QueryError::Datafusion { .. } if looks_like_invalid_scan_range(&message) => {
            S3Error::with_message(S3ErrorCode::InvalidRequestParameter, INVALID_SCAN_RANGE_MESSAGE.to_string())
        }
        QueryError::Datafusion { .. } if looks_like_missing_binding(&message) => {
            S3Error::with_message(S3ErrorCode::EvaluatorBindingDoesNotExist, message)
        }
        QueryError::Datafusion { .. } => S3Error::with_message(S3ErrorCode::UnsupportedSqlOperation, message),
        QueryError::StoreError { .. } if looks_like_invalid_scan_range(&message) => {
            S3Error::with_message(S3ErrorCode::InvalidRequestParameter, INVALID_SCAN_RANGE_MESSAGE.to_string())
        }
        QueryError::StoreError { .. } if looks_like_bucket_not_found(&message) => {
            S3Error::with_message(S3ErrorCode::NoSuchBucket, message)
        }
        QueryError::StoreError { .. } if looks_like_object_not_found(&message) => {
            S3Error::with_message(S3ErrorCode::NoSuchKey, message)
        }
        QueryError::StoreError { .. } => S3Error::with_message(S3ErrorCode::InternalError, message),
        QueryError::BuildQueryDispatcher { .. }
        | QueryError::Cancel
        | QueryError::FunctionNotExists { .. }
        | QueryError::FunctionExists { .. } => S3Error::with_message(S3ErrorCode::InternalError, message),
    }
}

fn looks_like_bucket_not_found(message: &str) -> bool {
    message.contains("NoSuchBucket") || message.contains("bucket not found") || message.contains("BucketNotFound")
}

const MAX_ERROR_SOURCE_DEPTH: usize = 16;

fn error_chain_any(
    mut err: &(dyn std::error::Error + 'static),
    predicate: impl Fn(&(dyn std::error::Error + 'static)) -> bool,
) -> bool {
    for _ in 0..MAX_ERROR_SOURCE_DEPTH {
        if predicate(err) {
            return true;
        }
        let Some(source) = err.source() else {
            return false;
        };
        err = source;
    }
    false
}

fn is_resource_exhausted(err: &(dyn std::error::Error + 'static)) -> bool {
    error_chain_any(err, |err| {
        err.downcast_ref::<DataFusionError>()
            .is_some_and(|err| matches!(err, DataFusionError::ResourcesExhausted(_)))
    })
}

fn is_unexpected_eof(err: &(dyn std::error::Error + 'static)) -> bool {
    error_chain_any(err, |err| {
        err.downcast_ref::<std::io::Error>()
            .is_some_and(|err| err.kind() == std::io::ErrorKind::UnexpectedEof)
    })
}

fn is_invalid_object_size(err: &(dyn std::error::Error + 'static)) -> bool {
    error_chain_any(err, |err| err.downcast_ref::<std::num::TryFromIntError>().is_some())
}

fn looks_like_object_not_found(message: &str) -> bool {
    message.contains("NoSuchKey")
        || message.contains("NoSuchVersion")
        || message.contains("ObjectNotFound")
        || message.contains("object not found")
        || message.contains("NotFound")
}

fn looks_like_missing_binding(message: &str) -> bool {
    message.contains("No field named")
        || message.contains("field not found")
        || message.contains("Schema error")
        || message.contains("No such column")
}

fn looks_like_invalid_scan_range(message: &str) -> bool {
    message.contains("ScanRange:") || message.contains(INVALID_SCAN_RANGE_MESSAGE)
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
        arrow::datatypes::Schema, physical_plan::stream::RecordBatchStreamAdapter, sql::sqlparser::parser::ParserError,
    };
    use http::HeaderMap;
    use s3s::dto::{CSVInput, ParquetInput, ScanRange};

    #[derive(Debug)]
    struct CyclicError;

    impl std::fmt::Display for CyclicError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("cyclic error")
        }
    }

    impl std::error::Error for CyclicError {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(self)
        }
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
            S3SelectPolicyError::UnsupportedSqlStructure {
                message: "JOIN is not supported".to_string(),
            }
            .into(),
        );
        let saturated = map_query_error_to_s3(S3SelectPolicyError::QueryConcurrencyLimit.into());
        let timed_out = map_query_error_to_s3(S3SelectPolicyError::QueryTimeout { seconds: 300 }.into());
        let stream_timed_out = map_query_error_to_s3(QueryError::Datafusion {
            source: Box::new(DataFusionError::External(Box::new(S3SelectPolicyError::QueryTimeout { seconds: 300 }))),
        });
        let exhausted = map_query_error_to_s3(QueryError::Datafusion {
            source: Box::new(DataFusionError::ObjectStore(Box::new(datafusion::object_store::Error::Generic {
                store: "EcObjectStore",
                source: Box::new(DataFusionError::ResourcesExhausted("memory limit".to_string())),
            }))),
        });
        let truncated = map_query_error_to_s3(QueryError::Datafusion {
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
        assert_eq!(unsupported.message(), Some("Unsupported S3 Select SQL structure: JOIN is not supported"));
        assert_eq!(saturated.code(), &S3ErrorCode::SlowDown);
        assert_eq!(saturated.message(), Some("S3 Select query concurrency limit reached"));
        assert_eq!(timed_out.code(), &S3ErrorCode::Busy);
        assert_eq!(timed_out.message(), Some("S3 Select query exceeded the 300-second execution limit"));
        assert_eq!(stream_timed_out.code(), &S3ErrorCode::Busy);
        assert_eq!(
            stream_timed_out.message(),
            Some("S3 Select query exceeded the 300-second execution limit")
        );
        assert_eq!(exhausted.code(), &S3ErrorCode::Busy);
        assert_eq!(truncated.code(), &S3ErrorCode::InternalError);
        assert_eq!(invalid_object_size.code(), &S3ErrorCode::InternalError);
    }

    #[test]
    fn error_source_matching_stops_at_the_depth_bound() {
        let err = CyclicError;

        assert!(!is_resource_exhausted(&err));
        assert!(!is_unexpected_eof(&err));
        assert!(!is_invalid_object_size(&err));
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
        let validation = SelectValidation {
            output_format: SelectOutputFormat::Csv(CSVOutput::default()),
            progress_enabled: false,
        };

        let producer = tokio::spawn(send_select_events_until_deadline(
            output,
            tx,
            terminal_permit,
            validation,
            Instant::now() + std::time::Duration::from_secs(1),
            300,
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
    fn map_store_error_not_found_to_no_such_key() {
        let err = map_query_error_to_s3(QueryError::StoreError {
            e: "ObjectStore NotFound: bucket/object.csv".to_string(),
        });
        assert_eq!(err.code(), &S3ErrorCode::NoSuchKey);
    }

    #[test]
    fn map_store_error_bucket_not_found_to_no_such_bucket() {
        let err = map_query_error_to_s3(QueryError::StoreError {
            e: "bucket not found".to_string(),
        });
        assert_eq!(err.code(), &S3ErrorCode::NoSuchBucket);
    }

    #[test]
    fn map_scan_range_store_error_to_invalid_request_parameter() {
        let err = map_query_error_to_s3(QueryError::StoreError {
            e: "ScanRange: Start after EOF".to_string(),
        });
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequestParameter);
        assert_eq!(err.message(), Some(INVALID_SCAN_RANGE_MESSAGE));
    }
}
