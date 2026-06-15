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
use bytes::Bytes;
use chrono::Utc;
use futures::pin_mut;
use futures::{Stream, StreamExt, future::ready, stream};
use futures_core::stream::BoxStream;
use http::{HeaderMap, HeaderValue, header::HeaderName};
use object_store::{
    Attributes, CopyOptions, Error as o_Error, GetOptions, GetRange, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, path::Path,
};
use pin_project_lite::pin_project;
use rustfs_common::DEFAULT_DELIMITER;
use rustfs_ecstore::error::{StorageError, is_err_bucket_not_found, is_err_object_not_found, is_err_version_not_found};
use rustfs_ecstore::resolve_object_store_handle;
use rustfs_ecstore::set_disk::DEFAULT_READ_BUFFER_SIZE;
use rustfs_ecstore::store::ECStore;
use rustfs_ecstore::store_api::{GetObjectReader, HTTPRangeSpec, ObjectIO, ObjectOperations, ObjectOptions};
use s3s::S3Result;
use s3s::dto::SelectObjectContentInput;
use s3s::header::{
    X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM, X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY,
    X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5,
};
use s3s::s3_error;
use std::collections::VecDeque;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::task::ready;
use tokio::io::AsyncReadExt;
use tokio::io::{AsyncRead, ReadBuf};
use tokio_util::io::ReaderStream;
use transform_stream::AsyncTryStream;

/// Maximum allowed object size for JSON DOCUMENT mode.
///
/// JSON DOCUMENT format requires loading the entire file into memory for DOM
/// parsing, so memory consumption grows linearly with file size.  Objects
/// larger than this threshold are rejected with an error rather than risking
/// an OOM condition.
///
/// To process larger JSON files, convert the input to **JSON LINES** (NDJSON,
/// `type = LINES`), which supports line-by-line streaming with no memory
/// size limit.
///
/// Default: 128 MiB.  This matches the AWS S3 Select limit for JSON DOCUMENT
/// inputs.
pub const MAX_JSON_DOCUMENT_BYTES: u64 = 128 * 1024 * 1024;
pub const INVALID_SCAN_RANGE_MESSAGE: &str =
    "The value of a parameter in ScanRange element is invalid. Check the service API documentation and try again.";

#[derive(Debug)]
pub struct EcObjectStore {
    input: Arc<SelectObjectContentInput>,
    need_convert: bool,
    delimiter: String,
    /// True when the JSON input type is DOCUMENT (multi-line formatted JSON).
    /// In that case the raw bytes are buffered and flattened to NDJSON before
    /// being handed to DataFusion's Arrow JSON reader.
    is_json_document: bool,
    /// Optional JSON sub-path extracted from `FROM s3object.<path>` in the SQL
    /// expression.  When set, `flatten_json_document_to_ndjson` navigates to
    /// this key in the root JSON object before flattening.
    json_sub_path: Option<String>,

    store: Arc<ECStore>,
}

#[derive(Clone, Copy, Debug)]
pub struct SelectScanRange {
    start: u64,
    end: u64,
}

impl SelectScanRange {
    pub const fn new(start: u64, end: u64) -> Self {
        Self { start, end }
    }

    pub const fn start(&self) -> u64 {
        self.start
    }

    pub const fn end(&self) -> u64 {
        self.end
    }
}

#[derive(Clone, Copy, Debug)]
pub struct InvalidScanRange;

impl EcObjectStore {
    pub fn new(input: Arc<SelectObjectContentInput>) -> S3Result<Self> {
        let Some(store) = resolve_object_store_handle() else {
            return Err(s3_error!(InternalError, "ec store not inited"));
        };

        let (need_convert, delimiter) = if let Some(csv) = input.request.input_serialization.csv.as_ref() {
            if let Some(delimiter) = csv.field_delimiter.as_ref() {
                if delimiter.len() > 1 {
                    (true, delimiter.to_owned())
                } else {
                    (false, String::new())
                }
            } else {
                (false, String::new())
            }
        } else {
            (false, String::new())
        };

        // Detect JSON DOCUMENT type: the entire file is a single (possibly
        // multi-line) JSON object/array, NOT newline-delimited JSON.
        let is_json_document = input
            .request
            .input_serialization
            .json
            .as_ref()
            .and_then(|j| j.type_.as_ref())
            .map(|t| t.as_str() == "DOCUMENT")
            .unwrap_or(false);

        // Extract the JSON sub-path from the SQL expression, e.g.
        // `SELECT … FROM s3object.employees e` → `Some("employees")`.
        let json_sub_path = if is_json_document {
            extract_json_sub_path_from_expression(&input.request.expression)
        } else {
            None
        };

        Ok(Self {
            input,
            need_convert,
            delimiter,
            is_json_document,
            json_sub_path,
            store,
        })
    }

    fn object_options(&self, options: &GetOptions) -> ObjectOptions {
        ObjectOptions {
            version_id: options.version.clone(),
            ..Default::default()
        }
    }

    fn read_headers(&self) -> HeaderMap {
        select_read_headers(&self.input)
    }

    fn scan_range(&self, object_size: u64) -> Result<Option<SelectScanRange>> {
        let Some(scan_range) = self.input.request.scan_range.as_ref() else {
            return Ok(None);
        };
        scan_range_from_bounds(scan_range.start, scan_range.end, object_size)
    }

    fn record_delimiter(&self) -> Vec<u8> {
        self.input
            .request
            .input_serialization
            .csv
            .as_ref()
            .and_then(|csv| csv.record_delimiter.as_ref())
            .map(|delimiter| delimiter.as_bytes().to_vec())
            .unwrap_or_else(|| b"\n".to_vec())
    }

    fn csv_has_header(&self) -> bool {
        self.input
            .request
            .input_serialization
            .csv
            .as_ref()
            .and_then(|csv| csv.file_header_info.as_ref())
            .is_some_and(|info| matches!(info.as_str(), "USE" | "IGNORE"))
    }

    async fn object_info(&self, opts: &ObjectOptions) -> Result<rustfs_ecstore::store_api::ObjectInfo> {
        self.store
            .get_object_info(&self.input.bucket, &self.input.key, opts)
            .await
            .map_err(|err| map_storage_error(&self.input.bucket, &self.input.key, err))
    }

    async fn object_reader(&self, range: Option<HTTPRangeSpec>, opts: &ObjectOptions) -> Result<GetObjectReader> {
        let h = self.read_headers();
        self.store
            .get_object_reader(&self.input.bucket, &self.input.key, range, h, opts)
            .await
            .map_err(|err| map_storage_error(&self.input.bucket, &self.input.key, err))
    }

    async fn read_raw_range_with_opts(&self, range: Range<u64>, opts: &ObjectOptions) -> Result<Bytes> {
        if range.is_empty() {
            return Ok(Bytes::new());
        }
        let reader = self.object_reader(Some(http_range_spec_from_range(range)), opts).await?;
        let mut reader = reader.stream;
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).await.map_err(|err| o_Error::Generic {
            store: "EcObjectStore",
            source: Box::new(err),
        })?;
        Ok(Bytes::from(bytes))
    }

    async fn read_raw_range(&self, range: Range<u64>) -> Result<Bytes> {
        self.read_raw_range_with_opts(range, &self.object_options(&GetOptions::new()))
            .await
    }

    async fn read_header_record(&self, object_size: u64, delimiter: &[u8], opts: &ObjectOptions) -> Result<Bytes> {
        if object_size == 0 {
            return Ok(Bytes::new());
        }

        let mut end = (DEFAULT_READ_BUFFER_SIZE as u64).min(object_size);
        loop {
            let bytes = self.read_raw_range_with_opts(0..end, opts).await?;
            if let Some(pos) = find_delimiter(&bytes, delimiter) {
                return Ok(bytes.slice(0..pos + delimiter.len()));
            }
            if end == object_size {
                return Ok(bytes);
            }
            end = end.saturating_mul(2).min(object_size);
        }
    }
}

impl std::fmt::Display for EcObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("EcObjectStore")
    }
}

fn unsupported_store_error(op: &str) -> o_Error {
    o_Error::Generic {
        store: "s3select-api",
        source: Box::new(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            format!("operation {op} is not supported in EcObjectStore"),
        )),
    }
}

fn insert_header(headers: &mut HeaderMap, name: HeaderName, value: Option<&str>) {
    if let Some(value) = value
        && let Ok(value) = HeaderValue::from_str(value)
    {
        headers.insert(name, value);
    }
}

fn select_read_headers(input: &SelectObjectContentInput) -> HeaderMap {
    let mut headers = HeaderMap::new();
    insert_header(
        &mut headers,
        X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM,
        input.sse_customer_algorithm.as_deref(),
    );
    insert_header(&mut headers, X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY, input.sse_customer_key.as_deref());
    insert_header(
        &mut headers,
        X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5,
        input.sse_customer_key_md5.as_deref(),
    );
    headers
}

fn http_range_spec_from_get_range(range: &GetRange) -> HTTPRangeSpec {
    match range {
        GetRange::Bounded(range) => http_range_spec_from_range(range.clone()),
        GetRange::Offset(start) => HTTPRangeSpec {
            is_suffix_length: false,
            start: *start as i64,
            end: -1,
        },
        GetRange::Suffix(length) => HTTPRangeSpec {
            is_suffix_length: true,
            start: *length as i64,
            end: -1,
        },
    }
}

fn http_range_spec_from_range(range: Range<u64>) -> HTTPRangeSpec {
    HTTPRangeSpec {
        is_suffix_length: false,
        start: range.start as i64,
        end: range.end.saturating_sub(1) as i64,
    }
}

fn http_range_spec_from_start(start: u64) -> HTTPRangeSpec {
    HTTPRangeSpec {
        is_suffix_length: false,
        start: start as i64,
        end: -1,
    }
}

fn scan_range_read_start(scan_range: SelectScanRange, delimiter: &[u8]) -> u64 {
    scan_range.start().saturating_sub(delimiter.len() as u64)
}

fn find_delimiter(bytes: &[u8], delimiter: &[u8]) -> Option<usize> {
    if delimiter.is_empty() {
        return None;
    }
    bytes.windows(delimiter.len()).position(|window| window == delimiter)
}

fn map_storage_error(bucket: &str, object: &str, err: StorageError) -> o_Error {
    if is_err_bucket_not_found(&err) || is_err_object_not_found(&err) || is_err_version_not_found(&err) {
        return o_Error::NotFound {
            path: format!("{bucket}/{object}"),
            source: err.to_string().into(),
        };
    }
    o_Error::Generic {
        store: "EcObjectStore",
        source: Box::new(err),
    }
}

pub fn scan_range_from_bounds(start: Option<i64>, end: Option<i64>, object_size: u64) -> Result<Option<SelectScanRange>> {
    parse_scan_range_from_bounds(start, end, object_size).map_err(|_| invalid_scan_range_store_error())
}

pub fn validate_scan_range_bounds(
    start: Option<i64>,
    end: Option<i64>,
    object_size: u64,
) -> std::result::Result<(), InvalidScanRange> {
    parse_scan_range_from_bounds(start, end, object_size).map(|_| ())
}

fn parse_scan_range_from_bounds(
    start: Option<i64>,
    end: Option<i64>,
    object_size: u64,
) -> std::result::Result<Option<SelectScanRange>, InvalidScanRange> {
    if start.is_none() && end.is_none() {
        return Ok(None);
    }
    if start.is_some_and(|value| value < 0) || end.is_some_and(|value| value < 0) {
        return Err(InvalidScanRange);
    }
    if let (Some(start), Some(end)) = (start, end)
        && start > end
    {
        return Err(InvalidScanRange);
    }
    if let Some(start) = start {
        let start = start as u64;
        if object_size == 0 {
            if start > 0 {
                return Err(InvalidScanRange);
            }
            return Ok(Some(SelectScanRange::new(0, 0)));
        }
        if start >= object_size {
            return Err(InvalidScanRange);
        }
    }
    if object_size == 0 {
        return Ok(Some(SelectScanRange::new(0, 0)));
    }

    let last_byte = object_size - 1;
    let (start, end) = match (start, end) {
        (Some(start), Some(end)) => (start as u64, (end as u64).min(last_byte)),
        (Some(start), None) => (start as u64, last_byte),
        (None, Some(suffix_len)) => {
            let suffix_len = suffix_len as u64;
            (object_size.saturating_sub(suffix_len), last_byte)
        }
        (None, None) => return Ok(None),
    };
    Ok(Some(SelectScanRange::new(start, end)))
}

fn invalid_scan_range_store_error() -> o_Error {
    o_Error::Generic {
        store: "EcObjectStore",
        source: format!("ScanRange: {INVALID_SCAN_RANGE_MESSAGE}").into(),
    }
}

#[async_trait]
impl ObjectStore for EcObjectStore {
    async fn put_opts(&self, _location: &Path, _payload: PutPayload, _opts: PutOptions) -> Result<PutResult> {
        Err(unsupported_store_error("put_opts"))
    }

    async fn put_multipart_opts(&self, _location: &Path, _opts: PutMultipartOptions) -> Result<Box<dyn MultipartUpload>> {
        Err(unsupported_store_error("put_multipart_opts"))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let opts = self.object_options(&options);
        let needs_scan_context = options.range.is_none() && !options.head && self.input.request.scan_range.is_some();
        let source_size = if needs_scan_context {
            Some(self.object_info(&opts).await?.size as u64)
        } else {
            None
        };
        let scan_context = if needs_scan_context {
            let original_size = source_size.expect("source size is loaded when scan range is present");
            self.scan_range(original_size)?.map(|scan_range| (original_size, scan_range))
        } else {
            None
        };

        let range = options.range.as_ref().map(http_range_spec_from_get_range);
        let reader = if let Some((original_size, scan_range)) = scan_context.as_ref() {
            let delimiter = self.record_delimiter();
            let read_start = scan_range_read_start(*scan_range, &delimiter);
            let range = (*original_size > 0).then(|| http_range_spec_from_start(read_start));
            self.object_reader(range, &opts).await?
        } else {
            self.object_reader(range, &opts).await?
        };

        let original_size = source_size.unwrap_or(reader.object_info.size as u64);
        let etag = reader.object_info.etag;
        let version = reader.object_info.version_id.map(|version| version.to_string());
        let attributes = Attributes::default();
        let result_range = match options.range.as_ref() {
            Some(range) => range.as_range(original_size).map_err(|err| o_Error::Generic {
                store: "EcObjectStore",
                source: Box::new(err),
            })?,
            None => 0..original_size,
        };

        let payload = if options.head {
            GetResultPayload::Stream(stream::empty().boxed())
        } else if options.range.is_some() {
            let size = (result_range.end - result_range.start) as usize;
            let stream = bytes_stream(ReaderStream::with_capacity(reader.stream, DEFAULT_READ_BUFFER_SIZE), size).boxed();
            GetResultPayload::Stream(stream)
        } else if self.is_json_document {
            // JSON DOCUMENT mode: gate on object size before doing any I/O.
            //
            // Small files (<= MAX_JSON_DOCUMENT_BYTES): build a lazy stream
            // that defers all I/O and JSON parsing until DataFusion first
            // polls it.  Parsing runs inside spawn_blocking so the async
            // runtime thread is never blocked.
            //
            // Large files (> MAX_JSON_DOCUMENT_BYTES): return an error
            // immediately.  JSON DOCUMENT relies on serde_json DOM parsing
            // which must load the whole file into memory; rejecting oversized
            // files upfront is safer than risking OOM.  Users should convert
            // their data to JSON LINES (NDJSON) format for large files.
            if original_size > MAX_JSON_DOCUMENT_BYTES {
                return Err(o_Error::Generic {
                    store: "EcObjectStore",
                    source: format!(
                        "JSON DOCUMENT object is {original_size} bytes, which exceeds the \
                         maximum allowed size of {MAX_JSON_DOCUMENT_BYTES} bytes \
                         ({} MiB). Convert the input to JSON LINES (NDJSON) to process \
                         large files.",
                        MAX_JSON_DOCUMENT_BYTES / (1024 * 1024)
                    )
                    .into(),
                });
            }
            let stream = json_document_ndjson_stream(reader.stream, original_size, self.json_sub_path.clone());
            GetResultPayload::Stream(stream)
        } else if let Some((_, scan_range)) = scan_context {
            let delimiter = self.record_delimiter();
            let include_header = self.csv_has_header();
            let read_start = scan_range_read_start(scan_range, &delimiter);
            let header = if include_header && scan_range.start() > 0 {
                Some(self.read_header_record(original_size, &delimiter, &opts).await?)
            } else {
                None
            };
            let stream = scan_range_stream(
                ReaderStream::with_capacity(reader.stream, DEFAULT_READ_BUFFER_SIZE),
                delimiter,
                scan_range,
                include_header && header.is_none(),
                read_start,
            )
            .boxed();
            let stream = if let Some(header) = header {
                stream::once(ready(Ok(header))).chain(stream).boxed()
            } else {
                stream
            };
            GetResultPayload::Stream(convert_field_delimiter_stream(stream, self.need_convert.then(|| self.delimiter.clone())))
        } else if self.need_convert {
            let stream = bytes_stream(
                ReaderStream::with_capacity(ConvertStream::new(reader.stream, self.delimiter.clone()), DEFAULT_READ_BUFFER_SIZE),
                original_size as usize,
            )
            .boxed();
            GetResultPayload::Stream(stream)
        } else {
            let stream = bytes_stream(
                ReaderStream::with_capacity(reader.stream, DEFAULT_READ_BUFFER_SIZE),
                original_size as usize,
            )
            .boxed();
            GetResultPayload::Stream(stream)
        };

        let meta = ObjectMeta {
            location: location.clone(),
            last_modified: Utc::now(),
            size: original_size,
            e_tag: etag,
            version,
        };

        Ok(GetResult {
            payload,
            meta,
            range: result_range,
            attributes,
        })
    }

    async fn get_ranges(&self, _location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let mut out = Vec::with_capacity(ranges.len());
        for range in ranges {
            out.push(self.read_raw_range(range.clone()).await?);
        }
        Ok(out)
    }

    fn delete_stream(&self, _locations: BoxStream<'static, Result<Path>>) -> BoxStream<'static, Result<Path>> {
        stream::once(ready(Err(unsupported_store_error("delete_stream")))).boxed()
    }

    fn list(&self, _prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        stream::once(ready(Err(unsupported_store_error("list")))).boxed()
    }

    async fn list_with_delimiter(&self, _prefix: Option<&Path>) -> Result<ListResult> {
        Err(unsupported_store_error("list_with_delimiter"))
    }

    async fn copy_opts(&self, _from: &Path, _to: &Path, _options: CopyOptions) -> Result<()> {
        Err(unsupported_store_error("copy_opts"))
    }
}

pin_project! {
    struct ConvertStream<R> {
        inner: R,
        converter: DelimiterConverter,
        read_buf: Vec<u8>,
        pending: Vec<u8>,
        pending_pos: usize,
        eof: bool,
    }
}

impl<R> ConvertStream<R> {
    fn new(inner: R, delimiter: String) -> Self {
        ConvertStream {
            inner,
            converter: DelimiterConverter::new(delimiter.into_bytes()),
            read_buf: vec![0; DEFAULT_READ_BUFFER_SIZE],
            pending: Vec::new(),
            pending_pos: 0,
            eof: false,
        }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for ConvertStream<R> {
    #[tracing::instrument(level = "debug", skip_all)]
    fn poll_read(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let this = self.project();
        loop {
            if drain_pending(this.pending, this.pending_pos, buf) || *this.eof {
                return Poll::Ready(Ok(()));
            }

            let read_len = DEFAULT_READ_BUFFER_SIZE.min(buf.remaining().max(1));
            let bytes_read = {
                let mut read_buf = ReadBuf::new(&mut this.read_buf[..read_len]);
                ready!(Pin::new(&mut *this.inner).poll_read(cx, &mut read_buf))?;
                read_buf.filled().len()
            };
            if bytes_read == 0 {
                *this.eof = true;
                *this.pending = this.converter.finish();
                *this.pending_pos = 0;
                continue;
            }

            *this.pending = this.converter.convert_chunk(&this.read_buf[..bytes_read]);
            *this.pending_pos = 0;
        }
    }
}

struct DelimiterConverter {
    delimiter: Vec<u8>,
    carry: Vec<u8>,
}

impl DelimiterConverter {
    fn new(delimiter: Vec<u8>) -> Self {
        Self {
            delimiter,
            carry: Vec::new(),
        }
    }

    fn convert_chunk(&mut self, chunk: &[u8]) -> Vec<u8> {
        if self.delimiter.is_empty() {
            return chunk.to_vec();
        }

        let mut combined = Vec::with_capacity(self.carry.len() + chunk.len());
        combined.extend_from_slice(&self.carry);
        combined.extend_from_slice(chunk);

        let safe_end = combined.len().saturating_sub(self.delimiter.len().saturating_sub(1));
        let mut converted = Vec::with_capacity(combined.len());
        let mut pos = 0;
        while pos < safe_end {
            if combined[pos..].starts_with(&self.delimiter) {
                converted.push(DEFAULT_DELIMITER);
                pos += self.delimiter.len();
            } else {
                converted.push(combined[pos]);
                pos += 1;
            }
        }
        self.carry.clear();
        self.carry.extend_from_slice(&combined[pos..]);
        converted
    }

    fn finish(&mut self) -> Vec<u8> {
        if self.delimiter.is_empty() {
            return std::mem::take(&mut self.carry);
        }
        let converted = replace_symbol(&self.delimiter, &self.carry);
        self.carry.clear();
        converted
    }
}

fn drain_pending(pending: &mut Vec<u8>, pending_pos: &mut usize, buf: &mut ReadBuf<'_>) -> bool {
    if *pending_pos >= pending.len() {
        pending.clear();
        *pending_pos = 0;
        return false;
    }
    if buf.remaining() == 0 {
        return false;
    }

    let len = buf.remaining().min(pending.len() - *pending_pos);
    buf.put_slice(&pending[*pending_pos..*pending_pos + len]);
    *pending_pos += len;
    if *pending_pos >= pending.len() {
        pending.clear();
        *pending_pos = 0;
    }
    true
}

fn replace_symbol(delimiter: &[u8], slice: &[u8]) -> Vec<u8> {
    if delimiter.is_empty() {
        return slice.to_vec();
    }

    let mut result = Vec::with_capacity(slice.len());
    let mut i = 0;
    while i < slice.len() {
        if slice[i..].starts_with(delimiter) {
            result.push(DEFAULT_DELIMITER);
            i += delimiter.len();
        } else {
            result.push(slice[i]);
            i += 1;
        }
    }
    result
}

fn convert_field_delimiter_stream<S>(stream: S, delimiter: Option<String>) -> BoxStream<'static, Result<Bytes>>
where
    S: Stream<Item = Result<Bytes>> + Send + 'static,
{
    let Some(delimiter) = delimiter else {
        return stream.boxed();
    };
    AsyncTryStream::<Bytes, o_Error, _>::new(|mut y| async move {
        let mut converter = DelimiterConverter::new(delimiter.into_bytes());
        pin_mut!(stream);
        while let Some(result) = stream.next().await {
            let bytes = result?;
            let converted = converter.convert_chunk(&bytes);
            if !converted.is_empty() {
                y.yield_ok(Bytes::from(converted)).await;
            }
        }
        let converted = converter.finish();
        if !converted.is_empty() {
            y.yield_ok(Bytes::from(converted)).await;
        }
        Ok(())
    })
    .boxed()
}

struct ScanRangeState<S> {
    stream: S,
    delimiter: Vec<u8>,
    range: SelectScanRange,
    include_header: bool,
    offset: u64,
    record_start: u64,
    record: Vec<u8>,
    pending: VecDeque<Bytes>,
    done: bool,
}

fn scan_range_stream<S>(
    stream: S,
    delimiter: Vec<u8>,
    range: SelectScanRange,
    include_header: bool,
    base_offset: u64,
) -> BoxStream<'static, Result<Bytes>>
where
    S: Stream<Item = std::io::Result<Bytes>> + Send + Unpin + 'static,
{
    let state = ScanRangeState {
        stream,
        delimiter,
        range,
        include_header,
        offset: base_offset,
        record_start: base_offset,
        record: Vec::new(),
        pending: VecDeque::new(),
        done: false,
    };

    stream::unfold(state, |mut state| async move {
        loop {
            if let Some(bytes) = state.pending.pop_front() {
                return Some((Ok(bytes), state));
            }
            if state.done {
                return None;
            }
            match state.stream.next().await {
                Some(Ok(bytes)) => state.push_chunk(&bytes),
                Some(Err(err)) => {
                    state.done = true;
                    return Some((
                        Err(o_Error::Generic {
                            store: "EcObjectStore",
                            source: Box::new(err),
                        }),
                        state,
                    ));
                }
                None => {
                    state.finish_pending_record();
                    state.done = true;
                }
            }
        }
    })
    .boxed()
}

impl<S> ScanRangeState<S> {
    fn push_chunk(&mut self, bytes: &[u8]) {
        if bytes.is_empty() || self.done {
            return;
        }
        if self.record.is_empty() {
            self.record_start = self.offset;
        }
        let search_start = self.record.len().saturating_sub(self.delimiter.len().saturating_sub(1));
        self.record.extend_from_slice(bytes);
        self.offset = self.offset.saturating_add(bytes.len() as u64);

        let mut search_start = search_start;
        while let Some(pos) = find_delimiter(&self.record[search_start..], &self.delimiter) {
            let record_end = search_start + pos + self.delimiter.len();
            self.finish_record(record_end);
            if self.done {
                break;
            }
            search_start = 0;
        }
    }

    fn finish_record(&mut self, record_end: usize) {
        let record = self.record.drain(..record_end).collect::<Vec<_>>();
        let record_start = self.record_start;
        self.record_start = self.record_start.saturating_add(record_end as u64);
        self.push_record(record, record_start);
    }

    fn finish_pending_record(&mut self) {
        if self.record.is_empty() {
            return;
        }
        let record = std::mem::take(&mut self.record);
        let record_start = self.record_start;
        self.push_record(record, record_start);
    }

    fn push_record(&mut self, record: Vec<u8>, record_start: u64) {
        let include_header = self.include_header && record_start == 0;
        let include_record = record_start >= self.range.start() && record_start <= self.range.end();
        if include_header || include_record {
            self.pending.push_back(Bytes::from(record));
        } else {
            if record_start > self.range.end() {
                self.done = true;
            }
        }
    }
}

/// Extract the JSON sub-path from a SQL expression's FROM clause.
///
/// Given `SELECT e.name FROM s3object.employees e WHERE …` this returns
/// `Some("employees")`.  Returns `None` when the FROM target is plain
/// `s3object` (no sub-path) or when the expression cannot be parsed.
fn extract_json_sub_path_from_expression(expression: &str) -> Option<String> {
    // Find " FROM " (case-insensitive).
    let lower = expression.to_lowercase();
    let from_pos = lower.find(" from ")?;
    let after_from = expression[from_pos + 6..].trim_start();

    // Must start with "s3object" (case-insensitive, ASCII-only for the prefix).
    const S3OBJECT_LOWER: &str = "s3object";
    let mut chars = after_from.char_indices();
    for expected in S3OBJECT_LOWER.chars() {
        let (idx, actual) = chars.next()?;
        if actual.to_ascii_lowercase() != expected {
            return None;
        }
        // When we have consumed the full prefix, `idx` is the byte index of
        // the current character; use it plus its UTF-8 length as the slice
        // boundary for the remaining string.
        if expected == 't' {
            let end_of_prefix = idx + actual.len_utf8();
            let after_s3object = &after_from[end_of_prefix..];

            // If the very next character is '.' there is a sub-path.
            if let Some(rest) = after_s3object.strip_prefix('.') {
                let rest = rest.trim_start();
                if rest.is_empty() {
                    return None;
                }

                // Support quoted identifiers: s3object."my.path" or s3object.'my path'
                let mut chars = rest.chars();
                if let Some(first) = chars.next()
                    && (first == '"' || first == '\'')
                {
                    let quote = first;
                    let inner = &rest[first.len_utf8()..];
                    if let Some(end) = inner.find(quote) {
                        let path = &inner[..end];
                        if !path.trim().is_empty() {
                            return Some(path.to_string());
                        }
                    }
                    // Quoted but no closing quote or empty: treat as no sub-path.
                    return None;
                }

                // Unquoted identifier: collect characters until whitespace, '[', or ']'.
                let end = rest
                    .find(|c: char| c.is_whitespace() || c == '[' || c == ']')
                    .unwrap_or(rest.len());
                let path = rest[..end].trim();
                if !path.is_empty() {
                    return Some(path.to_string());
                }
            }
            return None;
        }
    }

    // We only reach here if the loop completed without hitting the 't'
    // branch above, which would be unexpected given S3OBJECT_LOWER.
    None
}

/// Build a lazy NDJSON stream from a JSON DOCUMENT reader.
///
/// `get_opts` calls this and returns immediately – no I/O is performed until
/// DataFusion begins polling the returned stream.  The pipeline is:
///
/// 1. **Read** – the object bytes are read asynchronously from `stream` only
///    when the returned stream is first polled.
/// 2. **Parse** – JSON deserialization runs inside
///    `tokio::task::spawn_blocking` so the async runtime is never blocked by
///    CPU-bound work, even for very large documents.
/// 3. **Yield** – each NDJSON line (one per array element, or one line for a
///    scalar/object root) is yielded as a separate [`Bytes`] chunk, so
///    DataFusion can pipeline row processing as lines arrive.
fn json_document_ndjson_stream(
    stream: Box<dyn tokio::io::AsyncRead + Unpin + Send + Sync>,
    original_size: u64,
    json_sub_path: Option<String>,
) -> futures_core::stream::BoxStream<'static, Result<Bytes>> {
    AsyncTryStream::<Bytes, o_Error, _>::new(|mut y| async move {
        pin_mut!(stream);
        // ── 1. Read phase (lazy: only runs when the stream is polled) ────
        let mut all_bytes = Vec::with_capacity(original_size as usize);
        stream
            .take(original_size)
            .read_to_end(&mut all_bytes)
            .await
            .map_err(|e| o_Error::Generic {
                store: "EcObjectStore",
                source: Box::new(e),
            })?;

        // ── 2. Parse phase (blocking thread pool, non-blocking runtime) ──
        let lines = tokio::task::spawn_blocking(move || parse_json_document_to_lines(&all_bytes, json_sub_path.as_deref()))
            .await
            .map_err(|e| o_Error::Generic {
                store: "EcObjectStore",
                source: e.to_string().into(),
            })?
            .map_err(|e| o_Error::Generic {
                store: "EcObjectStore",
                source: Box::new(e),
            })?;

        // ── 3. Yield phase (one Bytes per NDJSON line) ───────────────────
        for line in lines {
            y.yield_ok(line).await;
        }
        Ok(())
    })
    .boxed()
}

/// Parse a JSON DOCUMENT (a single JSON value, possibly multi-line) into a
/// list of NDJSON lines – one [`Bytes`] per record.
///
/// `json_sub_path` – when the SQL expression contains `FROM s3object.<key>`,
/// pass `Some(key)` to navigate into that key before flattening.  For
/// example, given `{"employees":[{…},{…}]}` and `json_sub_path =
/// Some("employees")`, each element of the `employees` array becomes one
/// NDJSON line.
///
/// - A JSON array → one line per element.
/// - A JSON object (no sub-path match, or scalar root) → one line.
fn parse_json_document_to_lines(bytes: &[u8], json_sub_path: Option<&str>) -> std::io::Result<Vec<Bytes>> {
    let root: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

    // Navigate into the sub-path when the root is an object and a path was
    // extracted from the SQL FROM clause (e.g. `FROM s3object.employees`).
    let value = if let Some(path) = json_sub_path {
        if let serde_json::Value::Object(ref obj) = root {
            match obj.get(path) {
                Some(sub) => sub.clone(),
                // Path not found – fall back to emitting the whole root object.
                None => root,
            }
        } else {
            // Root is already an array or scalar; ignore the path hint.
            root
        }
    } else {
        root
    };

    let mut lines: Vec<Bytes> = Vec::new();
    match value {
        serde_json::Value::Array(arr) => {
            for item in arr {
                let mut line = serde_json::to_vec(&item).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                line.push(b'\n');
                lines.push(Bytes::from(line));
            }
        }
        other => {
            let mut line = serde_json::to_vec(&other).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            line.push(b'\n');
            lines.push(Bytes::from(line));
        }
    }
    Ok(lines)
}

/// Convert a JSON DOCUMENT to a single concatenated NDJSON [`Bytes`] blob.
///
/// This is a convenience wrapper around [`parse_json_document_to_lines`] used
/// by the unit tests.  Production code uses `json_document_ndjson_stream`
/// instead, which streams lines lazily without constructing this intermediate
/// blob.
#[cfg(test)]
fn flatten_json_document_to_ndjson(bytes: &[u8], json_sub_path: Option<&str>) -> std::io::Result<Bytes> {
    let lines = parse_json_document_to_lines(bytes, json_sub_path)?;
    let total = lines.iter().map(|b| b.len()).sum();
    let mut output = Vec::with_capacity(total);
    for line in lines {
        output.extend_from_slice(&line);
    }
    Ok(Bytes::from(output))
}

pub fn bytes_stream<S>(stream: S, content_length: usize) -> impl Stream<Item = Result<Bytes>> + Send + 'static
where
    S: Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static,
{
    AsyncTryStream::<Bytes, o_Error, _>::new(|mut y| async move {
        pin_mut!(stream);
        let mut remaining: usize = content_length;
        while remaining > 0 {
            let Some(result) = stream.next().await else {
                break;
            };
            let mut bytes = result.map_err(|e| o_Error::Generic {
                store: "",
                source: Box::new(e),
            })?;
            if bytes.len() > remaining {
                bytes.truncate(remaining);
            }
            remaining -= bytes.len();
            y.yield_ok(bytes).await;
        }
        Ok(())
    })
}

#[cfg(test)]
mod test {
    use super::{
        ConvertStream, SelectScanRange, bytes_stream, convert_field_delimiter_stream, extract_json_sub_path_from_expression,
        find_delimiter, flatten_json_document_to_ndjson, http_range_spec_from_get_range, replace_symbol, scan_range_from_bounds,
        scan_range_read_start, scan_range_stream, select_read_headers,
    };
    use bytes::Bytes;
    use futures::{StreamExt, TryStreamExt, stream};
    use object_store::GetRange;
    use s3s::dto::{
        CSVInput, CSVOutput, ExpressionType, InputSerialization, OutputSerialization, SelectObjectContentInput,
        SelectObjectContentRequest,
    };
    use s3s::header::{
        X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM, X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY,
        X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5,
    };
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::io::AsyncReadExt;
    use tokio_util::io::StreamReader;

    #[test]
    fn test_replace() {
        let result = replace_symbol(b"&&", b"dandan&&is&&best");
        assert_eq!(result, b"dandan,is,best");
    }

    #[tokio::test]
    async fn test_convert_stream_replaces_delimiter_across_chunks() {
        let chunks = stream::iter(vec![
            Ok::<_, std::io::Error>(Bytes::from_static(b"a&")),
            Ok::<_, std::io::Error>(Bytes::from_static(b"&b&&c")),
        ]);
        let reader = StreamReader::new(chunks);
        let mut reader = ConvertStream::new(reader, "&&".to_string());
        let mut output = Vec::new();
        reader.read_to_end(&mut output).await.unwrap();
        assert_eq!(output, b"a,b,c");
    }

    #[tokio::test]
    async fn test_convert_stream_replaces_delimiter_at_stream_end() {
        let chunks = stream::iter(vec![
            Ok::<_, std::io::Error>(Bytes::from_static(b"a&")),
            Ok::<_, std::io::Error>(Bytes::from_static(b"&")),
        ]);
        let reader = StreamReader::new(chunks);
        let mut reader = ConvertStream::new(reader, "&&".to_string());
        let mut output = Vec::new();
        reader.read_to_end(&mut output).await.unwrap();
        assert_eq!(output, b"a,");
    }

    #[tokio::test]
    async fn test_scan_range_stream_keeps_header_and_selected_record() {
        let chunks = stream::iter(vec![Ok::<_, std::io::Error>(Bytes::from_static(b"h1,h2\n1,a\n2,b\n3,c\n"))]);
        let mut stream = scan_range_stream(chunks, b"\n".to_vec(), SelectScanRange::new(10, 11), true, 0);
        let mut output = Vec::new();
        while let Some(bytes) = stream.next().await {
            output.extend_from_slice(&bytes.unwrap());
        }
        assert_eq!(output, b"h1,h2\n2,b\n");
    }

    #[tokio::test]
    async fn test_scan_range_stream_skips_record_when_start_is_in_middle() {
        let chunks = stream::iter(vec![Ok::<_, std::io::Error>(Bytes::from_static(b"1,a\n2,b\n3,c\n"))]);
        let mut stream = scan_range_stream(chunks, b"\n".to_vec(), SelectScanRange::new(2, 7), false, 0);
        let mut output = Vec::new();
        while let Some(bytes) = stream.next().await {
            output.extend_from_slice(&bytes.unwrap());
        }
        assert_eq!(output, b"2,b\n");
    }

    #[tokio::test]
    async fn test_scan_range_stream_keeps_record_when_end_is_in_middle() {
        let chunks = stream::iter(vec![Ok::<_, std::io::Error>(Bytes::from_static(b"1,a\n2,b\n3,c\n"))]);
        let mut stream = scan_range_stream(chunks, b"\n".to_vec(), SelectScanRange::new(0, 5), false, 0);
        let mut output = Vec::new();
        while let Some(bytes) = stream.next().await {
            output.extend_from_slice(&bytes.unwrap());
        }
        assert_eq!(output, b"1,a\n2,b\n");
    }

    #[tokio::test]
    async fn test_scan_range_stream_uses_base_offset_for_range_reader() {
        let chunks = stream::iter(vec![Ok::<_, std::io::Error>(Bytes::from_static(b"\n2,b\n3,c\n"))]);
        let mut stream = scan_range_stream(chunks, b"\n".to_vec(), SelectScanRange::new(4, 7), false, 3);
        let mut output = Vec::new();
        while let Some(bytes) = stream.next().await {
            output.extend_from_slice(&bytes.unwrap());
        }
        assert_eq!(output, b"2,b\n");
    }

    #[tokio::test]
    async fn test_scan_range_stream_handles_delimiter_split_across_chunks() {
        let chunks = stream::iter(vec![
            Ok::<_, std::io::Error>(Bytes::from_static(b"h1,h2\r")),
            Ok::<_, std::io::Error>(Bytes::from_static(b"\n1,a\r\n2,b\r")),
            Ok::<_, std::io::Error>(Bytes::from_static(b"\n3,c\r\n")),
        ]);
        let mut stream = scan_range_stream(chunks, b"\r\n".to_vec(), SelectScanRange::new(12, 14), true, 0);
        let mut output = Vec::new();
        while let Some(bytes) = stream.next().await {
            output.extend_from_slice(&bytes.unwrap());
        }
        assert_eq!(output, b"h1,h2\r\n2,b\r\n");
    }

    #[test]
    fn test_scan_range_read_start_keeps_full_delimiter_boundary() {
        let range = SelectScanRange::new(10, 20);
        assert_eq!(scan_range_read_start(range, b"\n"), 9);
        assert_eq!(scan_range_read_start(range, b"\r\n"), 8);
        assert_eq!(scan_range_read_start(range, b"abcdef"), 4);
    }

    #[test]
    fn test_find_delimiter_handles_multi_byte_delimiter() {
        assert_eq!(find_delimiter(b"one\r\ntwo", b"\r\n"), Some(3));
        assert_eq!(find_delimiter(b"one\ntwo", b"\r\n"), None);
    }

    #[test]
    fn test_scan_range_end_only_uses_aws_suffix_semantics() {
        let range = scan_range_from_bounds(None, Some(35), 100).unwrap().unwrap();
        assert_eq!(range.start(), 65);
        assert_eq!(range.end(), 99);
    }

    #[test]
    fn test_scan_range_start_after_object_is_rejected_before_reader() {
        let err = scan_range_from_bounds(Some(100), None, 100).unwrap_err();
        assert!(err.to_string().contains("ScanRange"));
    }

    #[test]
    fn test_scan_range_start_after_end_is_rejected() {
        let err = scan_range_from_bounds(Some(20), Some(10), 100).unwrap_err();
        assert!(err.to_string().contains("ScanRange"));
    }

    #[test]
    fn test_get_range_conversion_for_parquet_bounded_ranges() {
        let range = http_range_spec_from_get_range(&GetRange::Bounded(10..20));
        assert!(!range.is_suffix_length);
        assert_eq!(range.start, 10);
        assert_eq!(range.end, 19);
    }

    #[test]
    fn test_select_read_headers_preserves_ssec_context() {
        let input = SelectObjectContentInput {
            bucket: "bucket".to_string(),
            expected_bucket_owner: None,
            key: "object.csv".to_string(),
            sse_customer_algorithm: Some("AES256".to_string()),
            sse_customer_key: Some("customer-key".to_string()),
            sse_customer_key_md5: Some("customer-key-md5".to_string()),
            request: SelectObjectContentRequest {
                expression: "SELECT * FROM s3object".to_string(),
                expression_type: ExpressionType::from_static(ExpressionType::SQL),
                input_serialization: InputSerialization {
                    csv: Some(CSVInput::default()),
                    ..Default::default()
                },
                output_serialization: OutputSerialization {
                    csv: Some(CSVOutput::default()),
                    ..Default::default()
                },
                request_progress: None,
                scan_range: None,
            },
        };

        let headers = select_read_headers(&input);
        assert_eq!(headers.get(X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM).unwrap(), "AES256");
        assert_eq!(headers.get(X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY).unwrap(), "customer-key");
        assert_eq!(headers.get(X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5).unwrap(), "customer-key-md5");
    }

    #[tokio::test]
    async fn test_scan_range_output_can_convert_field_delimiter() {
        let chunks = stream::iter(vec![Ok::<_, std::io::Error>(Bytes::from_static(b"a&&1\nb&&2\n"))]);
        let stream = scan_range_stream(chunks, b"\n".to_vec(), SelectScanRange::new(0, 10), false, 0);
        let mut stream = convert_field_delimiter_stream(stream, Some("&&".to_string()));
        let mut output = Vec::new();
        while let Some(bytes) = stream.next().await {
            output.extend_from_slice(&bytes.unwrap());
        }
        assert_eq!(output, b"a,1\nb,2\n");
    }

    #[tokio::test]
    async fn test_field_delimiter_stream_converts_delimiter_split_across_chunks() {
        let chunks = stream::iter(vec![
            Ok::<_, object_store::Error>(Bytes::from_static(b"a&")),
            Ok::<_, object_store::Error>(Bytes::from_static(b"&1\nb&&2\n")),
        ]);
        let mut stream = convert_field_delimiter_stream(chunks, Some("&&".to_string()));
        let mut output = Vec::new();
        while let Some(bytes) = stream.next().await {
            output.extend_from_slice(&bytes.unwrap());
        }
        assert_eq!(output, b"a,1\nb,2\n");
    }

    #[tokio::test]
    async fn test_field_delimiter_stream_converts_delimiter_at_stream_end() {
        let chunks = stream::iter(vec![
            Ok::<_, object_store::Error>(Bytes::from_static(b"a&")),
            Ok::<_, object_store::Error>(Bytes::from_static(b"&")),
        ]);
        let mut stream = convert_field_delimiter_stream(chunks, Some("&&".to_string()));
        let mut output = Vec::new();
        while let Some(bytes) = stream.next().await {
            output.extend_from_slice(&bytes.unwrap());
        }
        assert_eq!(output, b"a,");
    }

    #[tokio::test]
    async fn test_bytes_stream_stops_at_content_length() {
        let poll_count = Arc::new(AtomicUsize::new(0));
        let stream_poll_count = Arc::clone(&poll_count);
        let source = stream::unfold(0, move |index| {
            let stream_poll_count = Arc::clone(&stream_poll_count);
            async move {
                stream_poll_count.fetch_add(1, Ordering::SeqCst);
                let bytes = match index {
                    0 => Bytes::from_static(b"abcd"),
                    1 => Bytes::from_static(b"efgh"),
                    _ => return None,
                };
                Some((Ok::<_, std::io::Error>(bytes), index + 1))
            }
        });

        let chunks: Vec<Bytes> = bytes_stream(source, 4).try_collect().await.unwrap();

        assert_eq!(chunks, vec![Bytes::from_static(b"abcd")]);
        assert_eq!(poll_count.load(Ordering::SeqCst), 1);
    }

    /// A JSON array is split into one NDJSON line per element.
    #[test]
    fn test_flatten_array_produces_one_line_per_element() {
        let input = br#"[{"id":1,"name":"Alice"},{"id":2,"name":"Bob"}]"#;
        let result = flatten_json_document_to_ndjson(input, None).expect("should succeed");
        let text = std::str::from_utf8(&result).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2);
        // Each line must be valid JSON
        for line in &lines {
            serde_json::from_str::<serde_json::Value>(line).expect("each line must be valid JSON");
        }
        // Spot-check field values
        let first: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(first["id"], 1);
        assert_eq!(first["name"], "Alice");
    }

    /// A single JSON object emits exactly one NDJSON line.
    #[test]
    fn test_flatten_single_object_produces_one_line() {
        let input = br#"{"id":42,"value":"hello world"}"#;
        let result = flatten_json_document_to_ndjson(input, None).expect("should succeed");
        let text = std::str::from_utf8(&result).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 1);
        let parsed: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(parsed["id"], 42);
        assert_eq!(parsed["value"], "hello world");
    }

    /// An empty JSON array produces empty output (zero bytes).
    #[test]
    fn test_flatten_empty_array_produces_no_output() {
        let input = b"[]";
        let result = flatten_json_document_to_ndjson(input, None).expect("should succeed");
        assert!(result.is_empty(), "empty array should yield zero bytes");
    }

    /// A multi-line (pretty-printed) JSON document is flattened correctly.
    #[test]
    fn test_flatten_pretty_printed_document() {
        let input = b"[\n  {\"a\": 1},\n  {\"a\": 2},\n  {\"a\": 3}\n]";
        let result = flatten_json_document_to_ndjson(input, None).expect("should succeed");
        let text = std::str::from_utf8(&result).unwrap();
        assert_eq!(text.lines().count(), 3);
    }

    /// Nested objects inside array elements are preserved as compact single-line JSON.
    #[test]
    fn test_flatten_array_with_nested_objects() {
        let input = br#"[{"outer":{"inner":99}},{"outer":{"inner":100}}]"#;
        let result = flatten_json_document_to_ndjson(input, None).expect("should succeed");
        let text = std::str::from_utf8(&result).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2);
        // Each line must not contain a newline mid-value
        for line in &lines {
            assert!(!line.is_empty());
            let v: serde_json::Value = serde_json::from_str(line).unwrap();
            assert!(v["outer"]["inner"].as_i64().unwrap() >= 99);
        }
    }

    /// Each output line ends with exactly one newline (no blank lines between records).
    #[test]
    fn test_flatten_output_ends_with_newline_per_record() {
        let input = br#"[{"x":1},{"x":2}]"#;
        let result = flatten_json_document_to_ndjson(input, None).expect("should succeed");
        let text = std::str::from_utf8(&result).unwrap();
        // Exactly 2 newlines for 2 records
        assert_eq!(text.chars().filter(|&c| c == '\n').count(), 2);
        // No leading blank line
        assert!(!text.starts_with('\n'));
    }

    /// Invalid JSON returns an `InvalidData` IO error.
    #[test]
    fn test_flatten_invalid_json_returns_error() {
        let input = b"{ not valid json }";
        let err = flatten_json_document_to_ndjson(input, None).expect_err("should fail on invalid JSON");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    /// Completely empty input returns an error (not valid JSON).
    #[test]
    fn test_flatten_empty_input_returns_error() {
        let err = flatten_json_document_to_ndjson(b"", None).expect_err("empty bytes are not valid JSON");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    // ── sub-path navigation tests ─────────────────────────────────────────

    /// `FROM s3object.employees` with a root JSON object navigates into the
    /// `employees` array and emits one NDJSON line per element.
    #[test]
    fn test_flatten_sub_path_object_with_array() {
        let input = br#"{"employees":[{"id":1,"name":"Alice","salary":75000},{"id":2,"name":"Bob","salary":65000}]}"#;
        let result = flatten_json_document_to_ndjson(input, Some("employees")).expect("should succeed");
        let text = std::str::from_utf8(&result).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2, "each employee should be its own NDJSON line");
        let first: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(first["name"], "Alice");
        assert_eq!(first["salary"], 75000);
        let second: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(second["name"], "Bob");
    }

    /// Sub-path that does not exist in the root object falls back to emitting the
    /// entire root object as one NDJSON line (graceful degradation).
    #[test]
    fn test_flatten_sub_path_missing_key_falls_back() {
        let input = br#"{"employees":[]}"#;
        let result = flatten_json_document_to_ndjson(input, Some("nonexistent")).expect("should succeed");
        let text = std::str::from_utf8(&result).unwrap();
        // Falls back to emitting the whole root object.
        assert_eq!(text.lines().count(), 1);
        let parsed: serde_json::Value = serde_json::from_str(text.trim_end()).unwrap();
        assert!(parsed.get("employees").is_some(), "root object preserved");
    }

    /// Sub-path is ignored when the root is already an array.
    #[test]
    fn test_flatten_sub_path_ignored_for_root_array() {
        let input = br#"[{"id":1},{"id":2}]"#;
        let result = flatten_json_document_to_ndjson(input, Some("employees")).expect("should succeed");
        let text = std::str::from_utf8(&result).unwrap();
        // The root array is flattened directly regardless of the sub-path hint.
        assert_eq!(text.lines().count(), 2);
    }

    // ── SQL path extraction tests ─────────────────────────────────────────

    #[test]
    fn test_extract_json_sub_path_basic() {
        let sql = "SELECT e.name FROM s3object.employees e WHERE e.salary > 70000";
        assert_eq!(extract_json_sub_path_from_expression(sql), Some("employees".to_string()));
    }

    #[test]
    fn test_extract_json_sub_path_uppercase() {
        let sql = "SELECT s.name FROM S3Object.records s";
        assert_eq!(extract_json_sub_path_from_expression(sql), Some("records".to_string()));
    }

    #[test]
    fn test_extract_json_sub_path_no_sub_path() {
        let sql = "SELECT * FROM s3object WHERE s3object.age > 30";
        assert_eq!(extract_json_sub_path_from_expression(sql), None);
    }

    #[test]
    fn test_extract_json_sub_path_with_bracket() {
        // `FROM s3object.employees[*]` — bracket stops path collection.
        let sql = "SELECT e.name FROM s3object.employees[*] e";
        assert_eq!(extract_json_sub_path_from_expression(sql), Some("employees".to_string()));
    }
}
