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

use crate::{
    PrepareSelectObjectSnapshotError, SELECT_DEFAULT_READ_BUFFER_SIZE, SelectError, SelectGetObjectReader, SelectObjectOptions,
    SelectObjectSnapshot, SelectObjectSnapshotReadError, SelectStorageError, SelectStore, SnapshotConsistencyError,
    query::{
        parser::RustFsDialect,
        session::{QueryExecutionGuard, QueryExecutionTracker},
    },
    resolve_select_object_store_handle, select_is_err_bucket_not_found, select_is_err_object_not_found,
    select_is_err_version_not_found,
};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use datafusion::{
    common::{DataFusionError, runtime::SpawnedTask},
    execution::memory_pool::{MemoryConsumer, MemoryPool, UnboundedMemoryPool},
    object_store::{
        Attributes, CopyOptions, Error as o_Error, GetOptions, GetRange, GetResult, GetResultPayload, ListResult,
        MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, path::Path,
    },
    sql::sqlparser::{
        ast::{ObjectNamePart, SetExpr, Statement, TableFactor},
        parser::Parser as SqlParser,
    },
};
use futures::pin_mut;
use futures::{Stream, StreamExt, future::ready, stream};
use futures_core::stream::BoxStream;
use http::{HeaderMap, HeaderValue, header::HeaderName};
use parking_lot::Mutex;
use rustfs_common::DEFAULT_DELIMITER;
use s3s::header::{
    X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM, X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY,
    X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5,
};
use s3s::{S3Error, S3ErrorCode, S3Result, dto::SelectObjectContentInput};
use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::{io::AsyncReadExt, sync::OnceCell};
use tokio_util::io::ReaderStream;
use transform_stream::AsyncTryStream;

use crate::storage_api::object_store::HTTPRangeSpec;

fn select_default_read_buffer_size_u64() -> u64 {
    u64::try_from(SELECT_DEFAULT_READ_BUFFER_SIZE).unwrap_or(u64::MAX)
}

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
/// inputs. The query memory pool also applies: RustFS reserves 64 times the
/// input size for parsing and output. With the default 64 MiB query memory
/// limit, JSON DOCUMENT inputs larger than 1 MiB are rejected; raise
/// `RUSTFS_S3SELECT_MEMORY_LIMIT_BYTES` to process larger inputs, up to this
/// hard cap.
pub const MAX_JSON_DOCUMENT_BYTES: u64 = 128 * 1024 * 1024;
const JSON_DOCUMENT_MEMORY_RESERVATION_MULTIPLIER: usize = 64;
pub const INVALID_SCAN_RANGE_MESSAGE: &str =
    "The value of a parameter in ScanRange element is invalid. Check the service API documentation and try again.";
const NORMALIZED_RECORD_DELIMITER: &[u8] = b"\r\n";
const NORMALIZED_FIELD_DELIMITER: &[u8] = &[DEFAULT_DELIMITER];

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
    memory_pool: Arc<dyn MemoryPool>,
    query_tracker: Option<QueryExecutionTracker>,
    store: Option<Arc<SelectStore>>,
    snapshot: OnceCell<Arc<SelectObjectSnapshot>>,
    #[cfg(test)]
    reader_open_count: Arc<AtomicUsize>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum EcObjectStoreBuildError {
    #[error("ec store not inited")]
    StoreUnavailable,
    #[error("SelectObjectContent snapshot consistency failure: {0}")]
    Snapshot(#[source] SnapshotConsistencyError),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SelectObjectStoreError {
    #[error("SelectObjectContent bucket does not exist")]
    BucketNotFound {
        #[source]
        source: SelectStorageError,
    },
    #[error("SelectObjectContent object does not exist")]
    ObjectNotFound {
        #[source]
        source: SelectStorageError,
    },
    #[error("SelectObjectContent storage failure")]
    Storage {
        #[source]
        source: SelectStorageError,
    },
    #[error("SelectObjectContent ScanRange is invalid")]
    InvalidScanRange,
}

impl SelectObjectStoreError {
    pub(crate) fn select_error(&self) -> SelectError {
        match self {
            Self::BucketNotFound { .. } => SelectError::BucketNotFound,
            Self::ObjectNotFound { .. } => SelectError::ObjectNotFound,
            Self::InvalidScanRange => SelectError::InvalidScanRange,
            Self::Storage { .. } => SelectError::InternalError,
        }
    }
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
        Self::build_lazy(input, Arc::new(UnboundedMemoryPool::default()), None).map_err(map_build_error_to_s3)
    }

    pub fn new_with_snapshot(input: Arc<SelectObjectContentInput>, snapshot: Arc<SelectObjectSnapshot>) -> S3Result<Self> {
        Self::build_with_snapshot(input, Arc::new(UnboundedMemoryPool::default()), None, snapshot).map_err(map_build_error_to_s3)
    }

    pub(crate) fn new_with_memory_pool(
        input: Arc<SelectObjectContentInput>,
        memory_pool: Arc<dyn MemoryPool>,
        snapshot: Option<Arc<SelectObjectSnapshot>>,
    ) -> std::result::Result<Self, EcObjectStoreBuildError> {
        match snapshot {
            Some(snapshot) => Self::build_with_snapshot(input, memory_pool, None, snapshot),
            None => Self::build_lazy(input, memory_pool, None),
        }
    }

    pub(crate) fn new_with_query_tracker(
        input: Arc<SelectObjectContentInput>,
        memory_pool: Arc<dyn MemoryPool>,
        query_tracker: QueryExecutionTracker,
        snapshot: Option<Arc<SelectObjectSnapshot>>,
    ) -> std::result::Result<Self, EcObjectStoreBuildError> {
        match snapshot {
            Some(snapshot) => Self::build_with_snapshot(input, memory_pool, Some(query_tracker), snapshot),
            None => Self::build_lazy(input, memory_pool, Some(query_tracker)),
        }
    }

    fn build_lazy(
        input: Arc<SelectObjectContentInput>,
        memory_pool: Arc<dyn MemoryPool>,
        query_tracker: Option<QueryExecutionTracker>,
    ) -> std::result::Result<Self, EcObjectStoreBuildError> {
        let store = resolve_select_object_store_handle().ok_or(EcObjectStoreBuildError::StoreUnavailable)?;
        Ok(Self::build(input, memory_pool, query_tracker, Some(store), None))
    }

    fn build_with_snapshot(
        input: Arc<SelectObjectContentInput>,
        memory_pool: Arc<dyn MemoryPool>,
        query_tracker: Option<QueryExecutionTracker>,
        snapshot: Arc<SelectObjectSnapshot>,
    ) -> std::result::Result<Self, EcObjectStoreBuildError> {
        if !snapshot.is_for(&input.bucket, &input.key) {
            return Err(EcObjectStoreBuildError::Snapshot(SnapshotConsistencyError::ObjectChanged));
        }
        Ok(Self::build(input, memory_pool, query_tracker, None, Some(snapshot)))
    }

    fn build(
        input: Arc<SelectObjectContentInput>,
        memory_pool: Arc<dyn MemoryPool>,
        query_tracker: Option<QueryExecutionTracker>,
        store: Option<Arc<SelectStore>>,
        snapshot: Option<Arc<SelectObjectSnapshot>>,
    ) -> Self {
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

        Self {
            input,
            need_convert,
            delimiter,
            is_json_document,
            json_sub_path,
            memory_pool,
            query_tracker,
            store,
            snapshot: match snapshot {
                Some(snapshot) => OnceCell::new_with(Some(snapshot)),
                None => OnceCell::new(),
            },
            #[cfg(test)]
            reader_open_count: Arc::new(AtomicUsize::new(0)),
        }
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

    fn record_delimiter_for_conversion(&self) -> Option<Vec<u8>> {
        let delimiter = self.record_delimiter();
        (self.need_convert || (delimiter.len() == 2 && delimiter != NORMALIZED_RECORD_DELIMITER)).then_some(delimiter)
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

    async fn snapshot(&self, version: Option<&str>) -> Result<&Arc<SelectObjectSnapshot>> {
        let snapshot = self
            .snapshot
            .get_or_try_init(|| async {
                let store = self.store.as_ref().ok_or_else(|| o_Error::Generic {
                    store: "EcObjectStore",
                    source: "prepared snapshot is unavailable".into(),
                })?;
                let opts = SelectObjectOptions {
                    version_id: version.map(|version| {
                        let version = version.trim();
                        if version.eq_ignore_ascii_case("null") {
                            uuid::Uuid::nil().to_string()
                        } else {
                            version.to_owned()
                        }
                    }),
                    ..Default::default()
                };
                let snapshot = store
                    .prepare_select_object_snapshot(&self.input.bucket, &self.input.key, &select_read_headers(&self.input), &opts)
                    .await
                    .map_err(|err| map_prepare_snapshot_error(&self.input.bucket, &self.input.key, err))?;
                Ok::<_, o_Error>(Arc::new(snapshot))
            })
            .await?;
        if let Some(requested) = version
            && !snapshot.matches_version(requested)
        {
            return Err(o_Error::Generic {
                store: "EcObjectStore",
                source: "prepared snapshot is pinned to a different object version".into(),
            });
        }
        Ok(snapshot)
    }

    async fn object_reader(&self, range: Option<HTTPRangeSpec>) -> Result<SelectGetObjectReader> {
        #[cfg(test)]
        self.reader_open_count.fetch_add(1, Ordering::Relaxed);
        self.snapshot(None)
            .await?
            .open_reader(range)
            .await
            .map_err(|err| snapshot_read_error(&self.input.bucket, &self.input.key, err))
    }

    async fn read_raw_range(&self, range: Range<u64>) -> Result<Bytes> {
        if range.is_empty() {
            return Ok(Bytes::new());
        }
        let snapshot = self.snapshot(None).await?;
        let reader = self.object_reader(Some(http_range_spec_from_range(range.clone()))).await?;
        let object_size = snapshot.logical_size();
        let resolved_range = GetRange::Bounded(range)
            .as_range(object_size)
            .map_err(|err| o_Error::Generic {
                store: "EcObjectStore",
                source: Box::new(err),
            })?;
        let expected_size = usize::try_from(resolved_range.end - resolved_range.start).map_err(|err| o_Error::Generic {
            store: "EcObjectStore",
            source: Box::new(err),
        })?;
        let mut reader = reader.stream;
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).await.map_err(|err| o_Error::Generic {
            store: "EcObjectStore",
            source: Box::new(err),
        })?;
        if bytes.len() < expected_size {
            return Err(incomplete_object_stream_error(expected_size - bytes.len()));
        }
        bytes.truncate(expected_size);
        Ok(Bytes::from(bytes))
    }

    async fn read_header_record(&self, object_size: u64, delimiter: &[u8]) -> Result<Bytes> {
        if object_size == 0 {
            return Ok(Bytes::new());
        }

        let mut end = select_default_read_buffer_size_u64().min(object_size);
        loop {
            let bytes = self.read_raw_range(0..end).await?;
            if let Some(pos) = find_delimiter(&bytes, delimiter) {
                return Ok(bytes.slice(0..pos + delimiter.len()));
            }
            if end == object_size {
                return Ok(bytes);
            }
            end = end.saturating_mul(2).min(object_size);
        }
    }

    async fn scan_range_read_start(&self, scan_range: SelectScanRange, delimiter: &[u8]) -> Result<u64> {
        let delimiter_len = u64::try_from(delimiter.len()).unwrap_or(u64::MAX);
        let fallback_start = scan_range.start().saturating_sub(delimiter_len);
        if delimiter.len() != 2 || delimiter[0] != delimiter[1] || scan_range.start() == 0 {
            return Ok(fallback_start);
        }

        let context_start = scan_range.start().saturating_sub(select_default_read_buffer_size_u64());
        let context = self.read_raw_range(context_start..scan_range.start()).await?;
        let suffix_len = context.iter().rev().take_while(|byte| **byte == delimiter[0]).count();
        if suffix_len == context.len() && context_start > 0 {
            return Err(o_Error::Generic {
                store: "EcObjectStore",
                source: "self-overlapping CSV record delimiter exceeds the bounded ScanRange context".into(),
            });
        }
        if suffix_len == 0 {
            return Ok(fallback_start);
        }
        Ok(scan_range.start().saturating_sub(u64::from(suffix_len % 2 != 0)))
    }
}

impl std::fmt::Debug for EcObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EcObjectStore")
            .field("bucket", &self.input.bucket)
            .field("object", &self.input.key)
            .field("need_convert", &self.need_convert)
            .field("is_json_document", &self.is_json_document)
            .field("json_sub_path", &self.json_sub_path)
            .finish_non_exhaustive()
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

fn find_delimiter(bytes: &[u8], delimiter: &[u8]) -> Option<usize> {
    if delimiter.is_empty() {
        return None;
    }
    bytes.windows(delimiter.len()).position(|window| window == delimiter)
}

fn map_prepare_snapshot_error(bucket: &str, object: &str, err: PrepareSelectObjectSnapshotError) -> o_Error {
    match err {
        PrepareSelectObjectSnapshotError::Storage(err) => map_storage_error(bucket, object, err),
        err => o_Error::Generic {
            store: "EcObjectStore",
            source: Box::new(err),
        },
    }
}

fn map_build_error_to_s3(error: EcObjectStoreBuildError) -> S3Error {
    let mut s3_error = S3Error::with_message(S3ErrorCode::InternalError, SelectError::InternalError.to_string());
    s3_error.set_source(Box::new(error));
    s3_error
}

fn snapshot_read_error(bucket: &str, object: &str, err: SelectObjectSnapshotReadError) -> o_Error {
    match err {
        SelectObjectSnapshotReadError::Storage(err) => map_storage_error(bucket, object, err),
        err => o_Error::Generic {
            store: "EcObjectStore",
            source: Box::new(err),
        },
    }
}

fn map_storage_error(bucket: &str, object: &str, err: SelectStorageError) -> o_Error {
    if select_is_err_bucket_not_found(&err) {
        return o_Error::NotFound {
            path: format!("{bucket}/{object}"),
            source: Box::new(SelectObjectStoreError::BucketNotFound { source: err }),
        };
    }
    if select_is_err_object_not_found(&err) || select_is_err_version_not_found(&err) {
        return o_Error::NotFound {
            path: format!("{bucket}/{object}"),
            source: Box::new(SelectObjectStoreError::ObjectNotFound { source: err }),
        };
    }
    o_Error::Generic {
        store: "EcObjectStore",
        source: Box::new(SelectObjectStoreError::Storage { source: err }),
    }
}

fn snapshot_last_modified(snapshot: &SelectObjectSnapshot) -> Result<DateTime<Utc>> {
    let mod_time = snapshot.object_info().mod_time.ok_or_else(|| o_Error::Generic {
        store: "EcObjectStore",
        source: std::io::Error::new(std::io::ErrorKind::InvalidData, "snapshot metadata has no modification time").into(),
    })?;
    DateTime::<Utc>::from_timestamp(mod_time.unix_timestamp(), mod_time.nanosecond()).ok_or_else(|| o_Error::Generic {
        store: "EcObjectStore",
        source: std::io::Error::new(std::io::ErrorKind::InvalidData, "snapshot modification time is out of range").into(),
    })
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
        source: Box::new(SelectObjectStoreError::InvalidScanRange),
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
        // SelectObjectContent has no version-id input. For direct ObjectStore
        // compatibility, a version supplied on the first operation defines
        // this instance's immutable snapshot; later operations reuse it.
        let snapshot = self.snapshot(options.version.as_deref()).await?;
        let original_size = snapshot.logical_size();
        let object_info = snapshot.object_info();
        let meta = ObjectMeta {
            location: location.clone(),
            last_modified: snapshot_last_modified(snapshot)?,
            size: original_size,
            e_tag: object_info.etag.clone(),
            version: object_info.version_id.map(|version| version.to_string()),
        };
        let result_range = match options.range.as_ref() {
            Some(range) => range.as_range(original_size).map_err(|err| o_Error::Generic {
                store: "EcObjectStore",
                source: Box::new(err),
            })?,
            None => 0..original_size,
        };
        if options.head {
            return Ok(GetResult {
                payload: GetResultPayload::Stream(stream::empty().boxed()),
                meta,
                range: result_range,
                attributes: Attributes::default(),
            });
        }

        let record_delimiter = self.record_delimiter_for_conversion();
        let needs_scan_context = options.range.is_none() && self.input.request.scan_range.is_some();
        let scan_context = if needs_scan_context {
            if let Some(scan_range) = self.scan_range(original_size)? {
                let delimiter = self.record_delimiter();
                let read_start = self.scan_range_read_start(scan_range, &delimiter).await?;
                Some((scan_range, read_start))
            } else {
                None
            }
        } else {
            None
        };

        let range = options.range.as_ref().map(http_range_spec_from_get_range);
        let reader = if let Some((_, read_start)) = scan_context.as_ref() {
            let range = (original_size > 0).then(|| http_range_spec_from_start(*read_start));
            self.object_reader(range).await?
        } else {
            self.object_reader(range).await?
        };

        let payload = if options.range.is_some() {
            let size = usize::try_from(result_range.end - result_range.start).map_err(|err| o_Error::Generic {
                store: "EcObjectStore",
                source: Box::new(err),
            })?;
            GetResultPayload::Stream(
                bytes_stream(ReaderStream::with_capacity(reader.stream, SELECT_DEFAULT_READ_BUFFER_SIZE), size).boxed(),
            )
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
            validate_json_document_size(original_size)?;
            let stream = json_document_ndjson_stream(
                reader.stream,
                original_size,
                self.json_sub_path.clone(),
                Arc::clone(&self.memory_pool),
                self.query_tracker.clone(),
            );
            GetResultPayload::Stream(stream)
        } else if let Some((scan_range, read_start)) = scan_context {
            let delimiter = self.record_delimiter();
            let include_header = self.csv_has_header();
            let header = if include_header && read_start > 0 {
                Some(self.read_header_record(original_size, &delimiter).await?)
            } else {
                None
            };
            let stream = scan_range_stream(
                ReaderStream::with_capacity(reader.stream, SELECT_DEFAULT_READ_BUFFER_SIZE),
                delimiter,
                scan_range,
                include_header && header.is_none(),
                read_start,
                original_size,
            )
            .boxed();
            let stream = if let Some(header) = header {
                stream::once(ready(Ok(header))).chain(stream).boxed()
            } else {
                stream
            };
            GetResultPayload::Stream(convert_csv_delimiter_stream(
                stream,
                record_delimiter,
                self.need_convert.then(|| self.delimiter.clone()),
            ))
        } else {
            let stream_size = usize::try_from(original_size).map_err(|err| o_Error::Generic {
                store: "EcObjectStore",
                source: Box::new(err),
            })?;
            let stream = bytes_stream(ReaderStream::with_capacity(reader.stream, SELECT_DEFAULT_READ_BUFFER_SIZE), stream_size);
            GetResultPayload::Stream(convert_csv_delimiter_stream(
                stream,
                record_delimiter,
                self.need_convert.then(|| self.delimiter.clone()),
            ))
        };

        Ok(GetResult {
            payload,
            meta,
            range: result_range,
            attributes: Attributes::default(),
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

struct CsvDelimiterConverter {
    record_delimiter: Option<Vec<u8>>,
    field_delimiter: Option<Vec<u8>>,
    carry: Vec<u8>,
}

impl CsvDelimiterConverter {
    fn new(record_delimiter: Option<Vec<u8>>, field_delimiter: Option<Vec<u8>>) -> Self {
        Self {
            record_delimiter: record_delimiter.filter(|delimiter| !delimiter.is_empty()),
            field_delimiter: field_delimiter.filter(|delimiter| !delimiter.is_empty()),
            carry: Vec::new(),
        }
    }

    fn max_delimiter_len(&self) -> usize {
        self.record_delimiter
            .as_ref()
            .into_iter()
            .chain(self.field_delimiter.as_ref())
            .map(Vec::len)
            .max()
            .unwrap_or(1)
    }

    fn convert_prefix(&self, bytes: &[u8], end: usize) -> (Vec<u8>, usize) {
        let mut converted = Vec::with_capacity(bytes.len());
        let mut pos = 0;
        while pos < end {
            let record_match = self
                .record_delimiter
                .as_ref()
                .filter(|delimiter| bytes[pos..].starts_with(delimiter));
            let field_match = self
                .field_delimiter
                .as_ref()
                .filter(|delimiter| bytes[pos..].starts_with(delimiter));
            if let Some(delimiter) = field_match
                && record_match.is_none_or(|record_delimiter| delimiter.len() > record_delimiter.len())
            {
                converted.extend_from_slice(NORMALIZED_FIELD_DELIMITER);
                pos += delimiter.len();
            } else if let Some(delimiter) = record_match {
                if delimiter.len() == 2 && delimiter != NORMALIZED_RECORD_DELIMITER {
                    converted.extend_from_slice(NORMALIZED_RECORD_DELIMITER);
                } else {
                    converted.extend_from_slice(delimiter);
                }
                pos += delimiter.len();
            } else {
                converted.push(bytes[pos]);
                pos += 1;
            }
        }
        (converted, pos)
    }

    fn convert_chunk(&mut self, chunk: &[u8]) -> Vec<u8> {
        let mut combined = Vec::with_capacity(self.carry.len() + chunk.len());
        combined.extend_from_slice(&self.carry);
        combined.extend_from_slice(chunk);

        let safe_end = combined.len().saturating_sub(self.max_delimiter_len().saturating_sub(1));
        let (converted, pos) = self.convert_prefix(&combined, safe_end);
        self.carry.clear();
        self.carry.extend_from_slice(&combined[pos..]);
        converted
    }

    fn finish(&mut self) -> Vec<u8> {
        let (converted, _) = self.convert_prefix(&self.carry, self.carry.len());
        self.carry.clear();
        converted
    }
}

fn convert_delimiter_stream<S>(
    stream: S,
    record_delimiter: Option<Vec<u8>>,
    field_delimiter: Option<Vec<u8>>,
) -> BoxStream<'static, Result<Bytes>>
where
    S: Stream<Item = Result<Bytes>> + Send + 'static,
{
    AsyncTryStream::<Bytes, o_Error, _>::new(|mut y| async move {
        let mut converter = CsvDelimiterConverter::new(record_delimiter, field_delimiter);
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

#[cfg(test)]
fn convert_record_delimiter_stream<S>(stream: S, delimiter: Vec<u8>) -> BoxStream<'static, Result<Bytes>>
where
    S: Stream<Item = Result<Bytes>> + Send + 'static,
{
    // DataFusion's CSV reader treats CRLF as a record terminator.
    convert_delimiter_stream(stream, Some(delimiter), None)
}

#[cfg(test)]
fn convert_field_delimiter_stream<S>(stream: S, delimiter: String) -> BoxStream<'static, Result<Bytes>>
where
    S: Stream<Item = Result<Bytes>> + Send + 'static,
{
    convert_delimiter_stream(stream, None, Some(delimiter.into_bytes()))
}

fn convert_csv_delimiter_stream<S>(
    stream: S,
    record_delimiter: Option<Vec<u8>>,
    field_delimiter: Option<String>,
) -> BoxStream<'static, Result<Bytes>>
where
    S: Stream<Item = Result<Bytes>> + Send + 'static,
{
    match (record_delimiter, field_delimiter) {
        (None, None) => stream.boxed(),
        (record, field) => convert_delimiter_stream(stream, record, field.map(String::into_bytes)),
    }
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
    expected_end: u64,
}

fn scan_range_stream<S>(
    stream: S,
    delimiter: Vec<u8>,
    range: SelectScanRange,
    include_header: bool,
    base_offset: u64,
    expected_end: u64,
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
        expected_end,
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
                    if state.offset < state.expected_end {
                        state.done = true;
                        return Some((Err(incomplete_object_stream_error(state.expected_end - state.offset)), state));
                    }
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

fn extract_json_sub_path_from_expression(expression: &str) -> Option<String> {
    let mut statements = SqlParser::parse_sql(&RustFsDialect, expression).ok()?;
    if statements.len() != 1 {
        return None;
    }
    let Statement::Query(query) = statements.pop()? else {
        return None;
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    let [table] = select.from.as_slice() else {
        return None;
    };
    let TableFactor::Table { name, .. } = &table.relation else {
        return None;
    };
    let [ObjectNamePart::Identifier(table_name), ObjectNamePart::Identifier(sub_path)] = name.0.as_slice() else {
        return None;
    };
    let is_s3_object = if table_name.quote_style.is_some() {
        table_name.value == "S3Object"
    } else {
        table_name.value.eq_ignore_ascii_case("S3Object")
    };
    is_s3_object.then(|| sub_path.value.clone())
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
    memory_pool: Arc<dyn MemoryPool>,
    query_tracker: Option<QueryExecutionTracker>,
) -> futures_core::stream::BoxStream<'static, Result<Bytes>> {
    json_document_ndjson_stream_with_parser(
        stream,
        original_size,
        json_sub_path,
        memory_pool,
        query_tracker,
        |all_bytes, json_sub_path| parse_json_document_to_lines(&all_bytes, json_sub_path.as_deref()),
    )
}

fn json_document_ndjson_stream_with_parser<P>(
    stream: Box<dyn tokio::io::AsyncRead + Unpin + Send + Sync>,
    original_size: u64,
    json_sub_path: Option<String>,
    memory_pool: Arc<dyn MemoryPool>,
    query_tracker: Option<QueryExecutionTracker>,
    parser: P,
) -> futures_core::stream::BoxStream<'static, Result<Bytes>>
where
    P: FnOnce(Vec<u8>, Option<String>) -> std::io::Result<Vec<Bytes>> + Send + 'static,
{
    AsyncTryStream::<Bytes, o_Error, _>::new(|mut y| async move {
        // Compact JSON can expand substantially into a serde_json DOM and
        // per-record output buffers, so reserve a conservative upper bound
        // before the source buffer is allocated.
        let buffer_capacity = usize::try_from(original_size).map_err(|_| o_Error::Generic {
            store: "EcObjectStore",
            source: Box::new(DataFusionError::ResourcesExhausted(format!(
                "JSON DOCUMENT input size {original_size} does not fit in memory"
            ))),
        })?;
        let reservation_bytes = buffer_capacity
            .checked_mul(JSON_DOCUMENT_MEMORY_RESERVATION_MULTIPLIER)
            .ok_or_else(|| o_Error::Generic {
                store: "EcObjectStore",
                source: Box::new(DataFusionError::ResourcesExhausted(format!(
                    "JSON DOCUMENT memory reservation overflow for {original_size} input bytes"
                ))),
            })?;
        let reservation = MemoryConsumer::new("S3 Select JSON document").register(&memory_pool);
        reservation.try_resize(reservation_bytes).map_err(|err| o_Error::Generic {
            store: "EcObjectStore",
            source: Box::new(err),
        })?;

        pin_mut!(stream);
        // ── 1. Read phase (lazy: only runs when the stream is polled) ────
        let mut all_bytes = Vec::with_capacity(buffer_capacity);
        stream
            .take(original_size)
            .read_to_end(&mut all_bytes)
            .await
            .map_err(|e| o_Error::Generic {
                store: "EcObjectStore",
                source: Box::new(e),
            })?;
        if all_bytes.len() != buffer_capacity {
            return Err(incomplete_object_stream_error(buffer_capacity - all_bytes.len()));
        }

        // ── 2. Parse phase (blocking thread pool, non-blocking runtime) ──
        let pending_query_guard = PendingQueryExecutionGuard::new(query_tracker);
        let task_query_guard = pending_query_guard.task_state();
        let (lines, _reservation, _query_guard) = SpawnedTask::spawn_blocking(move || {
            let query_guard = PendingQueryExecutionGuard::start(&task_query_guard)?;
            parser(all_bytes, json_sub_path).map(|lines| (lines, reservation, query_guard))
        })
        .await
        .map_err(|e| o_Error::Generic {
            store: "EcObjectStore",
            source: e.to_string().into(),
        })?
        .map_err(|e| o_Error::Generic {
            store: "EcObjectStore",
            source: if e.kind() == std::io::ErrorKind::InvalidData {
                Box::new(SelectError::JsonParsingError)
            } else {
                Box::new(e)
            },
        })?;

        // ── 3. Yield phase (one Bytes per NDJSON line) ───────────────────
        for line in lines {
            y.yield_ok(line).await;
        }
        Ok(())
    })
    .boxed()
}

struct PendingQueryExecutionGuard {
    state: Arc<Mutex<QueryExecutionGuardState>>,
}

enum QueryExecutionGuardState {
    Pending(Option<QueryExecutionTracker>),
    Started,
    Cancelled,
}

impl PendingQueryExecutionGuard {
    fn new(query_tracker: Option<QueryExecutionTracker>) -> Self {
        Self {
            state: Arc::new(Mutex::new(QueryExecutionGuardState::Pending(query_tracker))),
        }
    }

    fn task_state(&self) -> Arc<Mutex<QueryExecutionGuardState>> {
        Arc::clone(&self.state)
    }

    fn start(state: &Mutex<QueryExecutionGuardState>) -> std::io::Result<Option<QueryExecutionGuard>> {
        let mut state = state.lock();
        match std::mem::replace(&mut *state, QueryExecutionGuardState::Started) {
            QueryExecutionGuardState::Pending(None) => Ok(None),
            QueryExecutionGuardState::Pending(Some(query_tracker)) => query_tracker.query_guard().map(Some).ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::Interrupted, "JSON DOCUMENT parse was cancelled before it started")
            }),
            QueryExecutionGuardState::Cancelled => {
                *state = QueryExecutionGuardState::Cancelled;
                Err(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "JSON DOCUMENT parse was cancelled before it started",
                ))
            }
            QueryExecutionGuardState::Started => {
                *state = QueryExecutionGuardState::Started;
                Err(std::io::Error::other("JSON DOCUMENT parse started more than once"))
            }
        }
    }
}

impl Drop for PendingQueryExecutionGuard {
    fn drop(&mut self) {
        let query_guard = {
            let mut state = self.state.lock();
            match std::mem::replace(&mut *state, QueryExecutionGuardState::Cancelled) {
                QueryExecutionGuardState::Pending(query_guard) => query_guard,
                QueryExecutionGuardState::Started => {
                    *state = QueryExecutionGuardState::Started;
                    None
                }
                QueryExecutionGuardState::Cancelled => None,
            }
        };
        drop(query_guard);
    }
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
    let value = match (root, json_sub_path) {
        (serde_json::Value::Object(mut object), Some(path)) => {
            object.remove(path).unwrap_or_else(|| serde_json::Value::Object(object))
        }
        (root, _) => root,
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
        if remaining > 0 {
            return Err(incomplete_object_stream_error(remaining));
        }
        Ok(())
    })
}

fn validate_json_document_size(original_size: u64) -> Result<()> {
    if original_size <= MAX_JSON_DOCUMENT_BYTES {
        return Ok(());
    }

    Err(o_Error::Generic {
        store: "EcObjectStore",
        source: Box::new(DataFusionError::ResourcesExhausted(format!(
            "JSON DOCUMENT object is {original_size} bytes, which exceeds the maximum allowed size of \
             {MAX_JSON_DOCUMENT_BYTES} bytes ({} MiB). Convert the input to JSON LINES (NDJSON) to process large files.",
            MAX_JSON_DOCUMENT_BYTES / (1024 * 1024)
        ))),
    })
}

fn incomplete_object_stream_error(remaining: impl std::fmt::Display) -> o_Error {
    o_Error::Generic {
        store: "EcObjectStore",
        source: Box::new(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            format!("object stream ended with {remaining} bytes remaining"),
        )),
    }
}

#[cfg(test)]
mod test {
    use super::{
        EcObjectStore, EcObjectStoreBuildError, JSON_DOCUMENT_MEMORY_RESERVATION_MULTIPLIER, OnceCell,
        SELECT_DEFAULT_READ_BUFFER_SIZE, SelectObjectOptions, SelectObjectSnapshot, SelectScanRange, SnapshotConsistencyError,
        bytes_stream, convert_csv_delimiter_stream, convert_field_delimiter_stream, convert_record_delimiter_stream,
        extract_json_sub_path_from_expression, find_delimiter, flatten_json_document_to_ndjson, http_range_spec_from_get_range,
        json_document_ndjson_stream, json_document_ndjson_stream_with_parser, map_storage_error, scan_range_from_bounds,
        scan_range_stream, select_read_headers, snapshot_last_modified, validate_json_document_size,
    };
    use crate::query::session::{QueryExecutionGuard, QueryExecutionOwner, QueryExecutionTracker};
    use crate::storage_api::SelectPutObjReader;
    use crate::storage_api::object_store::ObjectIO as _;
    use crate::{QueryError, SelectError, SelectStorageError};
    use bytes::Bytes;
    use datafusion::{
        common::DataFusionError,
        execution::memory_pool::{GreedyMemoryPool, MemoryPool},
        execution::{config::SessionConfig, context::SessionContext},
        object_store::{self, GetOptions, GetRange, GetResultPayload, ObjectStore as _, path::Path},
        physical_plan::ExecutionPlanProperties,
        prelude::CsvReadOptions,
    };
    use futures::{StreamExt, TryStreamExt, stream};
    use http::HeaderMap;
    use rustfs_test_utils::PutObjectCommitBarrier;
    use s3s::S3ErrorCode;
    use s3s::dto::{
        CSVInput, CSVOutput, ExpressionType, FileHeaderInfo, InputSerialization, OutputSerialization, ScanRange,
        SelectObjectContentInput, SelectObjectContentRequest,
    };
    use s3s::header::{
        X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM, X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY,
        X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5,
    };
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use tokio::{io::AsyncReadExt, sync::Semaphore};

    fn csv_input(bucket: &str, object: &str) -> Arc<SelectObjectContentInput> {
        Arc::new(SelectObjectContentInput {
            bucket: bucket.to_string(),
            expected_bucket_owner: None,
            key: object.to_string(),
            sse_customer_algorithm: None,
            sse_customer_key: None,
            sse_customer_key_md5: None,
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
        })
    }

    #[test]
    fn lazy_snapshot_headers_preserve_ssec_context() {
        let mut input = (*csv_input("bucket", "object.csv")).clone();
        input.sse_customer_algorithm = Some("AES256".to_string());
        input.sse_customer_key = Some("customer-key".to_string());
        input.sse_customer_key_md5 = Some("customer-key-md5".to_string());

        let headers = select_read_headers(&input);

        assert_eq!(
            headers
                .get(X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM)
                .and_then(|value| value.to_str().ok()),
            Some("AES256")
        );
        assert_eq!(
            headers
                .get(X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY)
                .and_then(|value| value.to_str().ok()),
            Some("customer-key")
        );
        assert_eq!(
            headers
                .get(X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5)
                .and_then(|value| value.to_str().ok()),
            Some("customer-key-md5")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial]
    async fn legacy_constructor_retries_snapshot_after_not_found() {
        let env = crate::storage_api::select_test_ecstore_env().await;
        let bucket = "s3select-lazy-snapshot-retry";
        let object = "input.csv";
        env.make_bucket(bucket, false).await;
        let store = EcObjectStore::new(csv_input(bucket, object)).expect("legacy constructor should resolve the global store");

        let error = store
            .get_opts(
                &Path::from(object),
                GetOptions {
                    head: true,
                    ..Default::default()
                },
            )
            .await
            .expect_err("missing object should remain a typed not-found error");
        assert!(matches!(error, object_store::Error::NotFound { .. }));

        env.put_object_bytes(bucket, object, b"id,name\n1,Alice\n".to_vec()).await;
        let result = store
            .get_opts(
                &Path::from(object),
                GetOptions {
                    head: true,
                    ..Default::default()
                },
            )
            .await
            .expect("failed snapshot initialization must not be cached");
        assert_eq!(result.meta.size, 16);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn legacy_constructor_maps_missing_bucket_to_not_found() {
        let _env = crate::storage_api::select_test_ecstore_env().await;
        let bucket = "s3select-lazy-snapshot-missing-bucket";
        let object = "input.csv";
        let store = EcObjectStore::new(csv_input(bucket, object)).expect("legacy constructor should resolve the global store");

        let error = store
            .get_opts(
                &Path::from(object),
                GetOptions {
                    head: true,
                    ..Default::default()
                },
            )
            .await
            .expect_err("missing bucket should remain a typed not-found error");

        assert!(matches!(error, object_store::Error::NotFound { .. }));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial]
    async fn legacy_constructor_reuses_head_snapshot_for_body() {
        const BUCKET: &str = "s3select-lazy-snapshot-head-body";
        const OBJECT: &str = "input.csv";
        const OLD_DATA: &[u8] = b"id,name\n1,old\n";
        const NEW_DATA: &[u8] = b"id,name\n1,new\n";

        let env = crate::storage_api::select_test_ecstore_env().await;
        env.make_bucket(BUCKET, false).await;
        env.put_object_bytes(BUCKET, OBJECT, OLD_DATA.to_vec()).await;
        let store = EcObjectStore::new(csv_input(BUCKET, OBJECT)).expect("legacy constructor should resolve the global store");
        let head = store
            .get_opts(
                &Path::from(OBJECT),
                GetOptions {
                    head: true,
                    ..Default::default()
                },
            )
            .await
            .expect("HEAD should lazily prepare the snapshot");
        assert_eq!(head.meta.size, u64::try_from(OLD_DATA.len()).expect("fixture length should fit in u64"));

        let commit_barrier = PutObjectCommitBarrier::before_namespace(BUCKET, OBJECT);
        let writer = tokio::spawn(async move {
            env.put_object_bytes(BUCKET, OBJECT, NEW_DATA.to_vec()).await;
        });
        commit_barrier.wait_until_paused().await;
        commit_barrier.release_and_wait_until_namespace_pending().await;
        assert!(!writer.is_finished(), "overwrite must wait for the lazy snapshot");

        let result = store
            .get_opts(&Path::from(OBJECT), GetOptions::default())
            .await
            .expect("body should reuse the HEAD snapshot");
        let GetResultPayload::Stream(stream) = result.payload else {
            panic!("expected streaming snapshot body");
        };
        let bytes = stream
            .try_collect::<Vec<_>>()
            .await
            .expect("collect lazy snapshot body")
            .concat();
        assert_eq!(bytes, OLD_DATA);
        assert_eq!(store.reader_open_count.load(Ordering::Relaxed), 1);

        drop(store);
        tokio::time::timeout(std::time::Duration::from_secs(5), writer)
            .await
            .expect("overwrite should finish after the lazy snapshot is released")
            .expect("overwrite task should join");
        assert_eq!(read_current_object(BUCKET, OBJECT).await, NEW_DATA);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial]
    async fn legacy_constructor_first_version_pins_later_reads() {
        const BUCKET: &str = "s3select-lazy-snapshot-version";
        const OBJECT: &str = "input.csv";
        const OLD_DATA: &[u8] = b"old-marker\n";
        const NEW_DATA: &[u8] = b"new-poison-value\n";

        let env = crate::storage_api::select_test_ecstore_env().await;
        env.make_bucket(BUCKET, true).await;
        let versioned_opts = SelectObjectOptions {
            versioned: true,
            ..Default::default()
        };
        let mut old_reader = SelectPutObjReader::from_vec(OLD_DATA.to_vec());
        let old_info = env
            .ecstore
            .put_object(BUCKET, OBJECT, &mut old_reader, &versioned_opts)
            .await
            .expect("put old version fixture");
        let old_version = old_info
            .version_id
            .expect("versioned PUT should return a version ID")
            .to_string();
        let mut new_reader = SelectPutObjReader::from_vec(NEW_DATA.to_vec());
        let new_info = env
            .ecstore
            .put_object(BUCKET, OBJECT, &mut new_reader, &versioned_opts)
            .await
            .expect("put latest version poison fixture");
        let new_version = new_info
            .version_id
            .expect("versioned PUT should return a version ID")
            .to_string();

        let store = EcObjectStore::new(csv_input(BUCKET, OBJECT)).expect("legacy constructor should resolve the global store");
        let head = store
            .get_opts(
                &Path::from(OBJECT),
                GetOptions {
                    head: true,
                    version: Some(old_version.to_uppercase()),
                    ..Default::default()
                },
            )
            .await
            .expect("first HEAD should bind the requested old version");
        assert_eq!(head.meta.version.as_deref(), Some(old_version.as_str()));

        let mismatch = store
            .get_opts(
                &Path::from(OBJECT),
                GetOptions {
                    head: true,
                    version: Some(new_version),
                    ..Default::default()
                },
            )
            .await
            .expect_err("an explicit different version must not reuse the pinned snapshot");
        assert!(mismatch.to_string().contains("different object version"));

        let range = store
            .get_opts(
                &Path::from(OBJECT),
                GetOptions {
                    range: Some(GetRange::Bounded(0..3)),
                    ..Default::default()
                },
            )
            .await
            .expect("later range should reuse the old-version snapshot");
        let GetResultPayload::Stream(range_stream) = range.payload else {
            panic!("expected ranged snapshot stream");
        };
        assert_eq!(
            range_stream
                .try_collect::<Vec<_>>()
                .await
                .expect("collect old-version range")
                .concat(),
            b"old"
        );

        let body = store
            .get_opts(&Path::from(OBJECT), GetOptions::default())
            .await
            .expect("later body should reuse the old-version snapshot");
        let GetResultPayload::Stream(body_stream) = body.payload else {
            panic!("expected full snapshot stream");
        };
        assert_eq!(
            body_stream
                .try_collect::<Vec<_>>()
                .await
                .expect("collect old-version body")
                .concat(),
            OLD_DATA
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial]
    async fn legacy_constructor_normalizes_null_version_before_snapshot_prepare() {
        const BUCKET: &str = "s3select-lazy-snapshot-null-version";
        const OBJECT: &str = "input.csv";
        const DATA: &[u8] = b"null-version-marker\n";

        let env = crate::storage_api::select_test_ecstore_env().await;
        env.make_bucket(BUCKET, false).await;
        env.put_object_bytes(BUCKET, OBJECT, DATA.to_vec()).await;
        let store = EcObjectStore::new(csv_input(BUCKET, OBJECT)).expect("legacy constructor should resolve the global store");

        let head = store
            .get_opts(
                &Path::from(OBJECT),
                GetOptions {
                    head: true,
                    version: Some("NULL".to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect("null version should prepare an unversioned snapshot");
        assert!(head.meta.version.is_none());

        let body = store
            .get_opts(
                &Path::from(OBJECT),
                GetOptions {
                    version: Some(uuid::Uuid::nil().to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect("nil UUID should match the pinned null-version snapshot");
        let GetResultPayload::Stream(body_stream) = body.payload else {
            panic!("expected null-version snapshot stream");
        };
        assert_eq!(
            body_stream
                .try_collect::<Vec<_>>()
                .await
                .expect("collect null-version body")
                .concat(),
            DATA
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial]
    async fn prepared_snapshot_rejects_a_different_query_object() {
        const BUCKET: &str = "s3select-snapshot-identity";
        const OBJECT: &str = "source.csv";

        let env = crate::storage_api::select_test_ecstore_env().await;
        env.make_bucket(BUCKET, false).await;
        env.put_object_bytes(BUCKET, OBJECT, b"source-marker\n".to_vec()).await;
        let snapshot = Arc::new(
            env.ecstore
                .prepare_select_object_snapshot(BUCKET, OBJECT, &HeaderMap::new(), &Default::default())
                .await
                .expect("prepare source snapshot"),
        );

        let error = EcObjectStore::new_with_snapshot(csv_input(BUCKET, "different.csv"), snapshot)
            .expect_err("a snapshot must remain bound to its source object");

        assert_eq!(error.code(), &S3ErrorCode::InternalError);
        assert!(error.source().is_some_and(|source| {
            source
                .downcast_ref::<EcObjectStoreBuildError>()
                .is_some_and(|error| matches!(error, EcObjectStoreBuildError::Snapshot(SnapshotConsistencyError::ObjectChanged)))
        }));
    }

    async fn prepare_test_snapshot(bucket: &str, object: &str) -> Arc<SelectObjectSnapshot> {
        let env = crate::storage_api::select_test_ecstore_env().await;
        Arc::new(
            env.ecstore
                .prepare_select_object_snapshot(bucket, object, &HeaderMap::new(), &Default::default())
                .await
                .expect("prepare SelectObjectContent snapshot"),
        )
    }

    fn scan_range_csv_store(
        bucket: &str,
        object: &str,
        snapshot: Arc<SelectObjectSnapshot>,
        record_delimiter: &str,
        file_header_info: Option<FileHeaderInfo>,
        start: i64,
        end: i64,
    ) -> EcObjectStore {
        EcObjectStore::new_with_snapshot(
            Arc::new(SelectObjectContentInput {
                bucket: bucket.to_string(),
                expected_bucket_owner: None,
                key: object.to_string(),
                sse_customer_algorithm: None,
                sse_customer_key: None,
                sse_customer_key_md5: None,
                request: SelectObjectContentRequest {
                    expression: "SELECT * FROM s3object".to_string(),
                    expression_type: ExpressionType::from_static(ExpressionType::SQL),
                    input_serialization: InputSerialization {
                        csv: Some(CSVInput {
                            record_delimiter: Some(record_delimiter.to_string()),
                            file_header_info,
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                    output_serialization: OutputSerialization {
                        csv: Some(CSVOutput::default()),
                        ..Default::default()
                    },
                    request_progress: None,
                    scan_range: Some(ScanRange {
                        start: Some(start),
                        end: Some(end),
                    }),
                },
            }),
            snapshot,
        )
        .expect("snapshot should match SelectObjectContent input")
    }

    async fn read_current_object(bucket: &str, object: &str) -> Vec<u8> {
        let snapshot = prepare_test_snapshot(bucket, object).await;
        let mut reader = snapshot.open_reader(None).await.expect("current object reader should open");
        let mut bytes = Vec::new();
        reader
            .stream
            .read_to_end(&mut bytes)
            .await
            .expect("current object should be readable");
        bytes
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial]
    async fn head_uses_snapshot_metadata_without_opening_body() {
        let env = crate::storage_api::select_test_ecstore_env().await;
        let bucket = "s3select-snapshot-head";
        let object = "input.csv";
        env.make_bucket(bucket, false).await;
        let mut reader = SelectPutObjReader::from_vec(b"id,name\n1,Alice\n".to_vec());
        env.ecstore
            .put_object(bucket, object, &mut reader, &Default::default())
            .await
            .expect("put HEAD fixture");

        let snapshot = prepare_test_snapshot(bucket, object).await;
        let expected_modified = snapshot_last_modified(&snapshot).expect("snapshot modification time");
        let expected_size = snapshot.logical_size();
        let expected_etag = snapshot.object_info().etag.clone();
        let expected_version = snapshot.object_info().version_id.map(|version| version.to_string());
        let input = Arc::new(SelectObjectContentInput {
            bucket: bucket.to_string(),
            expected_bucket_owner: None,
            key: object.to_string(),
            sse_customer_algorithm: Some("secret-algorithm".to_string()),
            sse_customer_key: Some("secret-customer-key".to_string()),
            sse_customer_key_md5: Some("secret-customer-key-md5".to_string()),
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
        });
        let store = EcObjectStore::new_with_snapshot(input, snapshot).expect("snapshot should match SelectObjectContent input");
        let debug = format!("{store:?}");
        assert!(!debug.contains("secret-customer-key"));

        let result = store
            .get_opts(
                &Path::from(object),
                GetOptions {
                    head: true,
                    ..Default::default()
                },
            )
            .await
            .expect("HEAD from snapshot metadata");

        assert_eq!(result.meta.last_modified, expected_modified);
        assert_eq!(result.meta.size, expected_size);
        assert_eq!(result.meta.e_tag, expected_etag);
        assert_eq!(result.meta.version, expected_version);
        assert_eq!(store.reader_open_count.load(Ordering::Relaxed), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial]
    async fn snapshot_keeps_csv_header_and_body_on_one_generation_during_overwrite() {
        const BUCKET: &str = "s3select-snapshot-header-body-race";
        const OBJECT: &str = "input.csv";
        const OLD_DATA: &[u8] = b"old_header,value\nskip_old,0\nold_body,1\n";
        const NEW_DATA: &[u8] = b"new_header,value\nskip_new,0\nnew_body,1\n";

        let env = crate::storage_api::select_test_ecstore_env().await;
        env.make_bucket(BUCKET, false).await;
        let mut reader = SelectPutObjReader::from_vec(OLD_DATA.to_vec());
        env.ecstore
            .put_object(BUCKET, OBJECT, &mut reader, &Default::default())
            .await
            .expect("put old CSV header/body fixture");

        let selected_start = i64::try_from(b"old_header,value\nskip_old,0\n".len()).expect("fixture offset should fit in i64");
        let snapshot = prepare_test_snapshot(BUCKET, OBJECT).await;
        let store = scan_range_csv_store(
            BUCKET,
            OBJECT,
            snapshot,
            "\n",
            Some(FileHeaderInfo::from_static(FileHeaderInfo::USE)),
            selected_start,
            selected_start,
        );
        let commit_barrier = PutObjectCommitBarrier::before_namespace(BUCKET, OBJECT);
        let writer = tokio::spawn(async move {
            env.put_object_bytes(BUCKET, OBJECT, NEW_DATA.to_vec()).await;
        });
        commit_barrier.wait_until_paused().await;
        commit_barrier.release_and_wait_until_namespace_pending().await;
        assert!(
            !writer.is_finished(),
            "overwrite must remain blocked while the SelectObjectContent snapshot is alive"
        );

        let result = store
            .get_opts(&Path::from(OBJECT), GetOptions::default())
            .await
            .expect("read CSV header and body from one snapshot");
        let GetResultPayload::Stream(stream) = result.payload else {
            panic!("expected streaming CSV header/body payload");
        };
        let bytes = stream
            .try_collect::<Vec<_>>()
            .await
            .expect("collect CSV header/body snapshot")
            .concat();

        assert_eq!(bytes, b"old_header,value\nold_body,1\n");
        assert_eq!(store.reader_open_count.load(Ordering::Relaxed), 2);
        assert!(!writer.is_finished(), "overwrite must remain blocked after both snapshot readers finish");

        drop(store);
        tokio::time::timeout(std::time::Duration::from_secs(5), writer)
            .await
            .expect("overwrite should finish after the snapshot is released")
            .expect("overwrite task should join");
        assert_eq!(read_current_object(BUCKET, OBJECT).await, NEW_DATA);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial]
    async fn snapshot_keeps_scan_range_context_and_main_reader_on_one_generation_during_overwrite() {
        const BUCKET: &str = "s3select-snapshot-scan-context-race";
        const OBJECT: &str = "input.csv";
        const OLD_DATA: &[u8] = b"111aaa222aa333aa";
        const NEW_DATA: &[u8] = b"999aaa888aa777aa";

        let env = crate::storage_api::select_test_ecstore_env().await;
        env.make_bucket(BUCKET, false).await;
        let mut reader = SelectPutObjReader::from_vec(OLD_DATA.to_vec());
        env.ecstore
            .put_object(BUCKET, OBJECT, &mut reader, &Default::default())
            .await
            .expect("put old ScanRange context fixture");

        let snapshot = prepare_test_snapshot(BUCKET, OBJECT).await;
        let store = scan_range_csv_store(BUCKET, OBJECT, snapshot, "aa", None, 4, 5);
        let commit_barrier = PutObjectCommitBarrier::before_namespace(BUCKET, OBJECT);
        let writer = tokio::spawn(async move {
            env.put_object_bytes(BUCKET, OBJECT, NEW_DATA.to_vec()).await;
        });
        commit_barrier.wait_until_paused().await;
        commit_barrier.release_and_wait_until_namespace_pending().await;
        assert!(
            !writer.is_finished(),
            "overwrite must remain blocked while the SelectObjectContent snapshot is alive"
        );

        let result = store
            .get_opts(&Path::from(OBJECT), GetOptions::default())
            .await
            .expect("read ScanRange context and main body from one snapshot");
        let GetResultPayload::Stream(stream) = result.payload else {
            panic!("expected streaming ScanRange context payload");
        };
        let bytes = stream
            .try_collect::<Vec<_>>()
            .await
            .expect("collect ScanRange context snapshot")
            .concat();

        assert_eq!(bytes, b"a222\r\n");
        assert_eq!(store.reader_open_count.load(Ordering::Relaxed), 2);
        assert!(
            !writer.is_finished(),
            "overwrite must remain blocked after context and main readers finish"
        );

        drop(store);
        tokio::time::timeout(std::time::Duration::from_secs(5), writer)
            .await
            .expect("overwrite should finish after the snapshot is released")
            .expect("overwrite task should join");
        assert_eq!(read_current_object(BUCKET, OBJECT).await, NEW_DATA);
    }

    #[tokio::test]
    async fn test_scan_range_stream_keeps_header_and_selected_record() {
        let chunks = stream::iter(vec![Ok::<_, std::io::Error>(Bytes::from_static(b"h1,h2\n1,a\n2,b\n3,c\n"))]);
        let mut stream = scan_range_stream(chunks, b"\n".to_vec(), SelectScanRange::new(10, 11), true, 0, 18);
        let mut output = Vec::new();
        while let Some(bytes) = stream.next().await {
            output.extend_from_slice(&bytes.unwrap());
        }
        assert_eq!(output, b"h1,h2\n2,b\n");
    }

    #[tokio::test]
    async fn test_scan_range_stream_skips_record_when_start_is_in_middle() {
        let chunks = stream::iter(vec![Ok::<_, std::io::Error>(Bytes::from_static(b"1,a\n2,b\n3,c\n"))]);
        let mut stream = scan_range_stream(chunks, b"\n".to_vec(), SelectScanRange::new(2, 7), false, 0, 12);
        let mut output = Vec::new();
        while let Some(bytes) = stream.next().await {
            output.extend_from_slice(&bytes.unwrap());
        }
        assert_eq!(output, b"2,b\n");
    }

    #[tokio::test]
    async fn test_scan_range_stream_keeps_record_when_end_is_in_middle() {
        let chunks = stream::iter(vec![Ok::<_, std::io::Error>(Bytes::from_static(b"1,a\n2,b\n3,c\n"))]);
        let mut stream = scan_range_stream(chunks, b"\n".to_vec(), SelectScanRange::new(0, 5), false, 0, 12);
        let mut output = Vec::new();
        while let Some(bytes) = stream.next().await {
            output.extend_from_slice(&bytes.unwrap());
        }
        assert_eq!(output, b"1,a\n2,b\n");
    }

    #[tokio::test]
    async fn test_scan_range_stream_uses_base_offset_for_range_reader() {
        let chunks = stream::iter(vec![Ok::<_, std::io::Error>(Bytes::from_static(b"\n2,b\n3,c\n"))]);
        let mut stream = scan_range_stream(chunks, b"\n".to_vec(), SelectScanRange::new(4, 7), false, 3, 12);
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
        let mut stream = scan_range_stream(chunks, b"\r\n".to_vec(), SelectScanRange::new(12, 14), true, 0, 22);
        let mut output = Vec::new();
        while let Some(bytes) = stream.next().await {
            output.extend_from_slice(&bytes.unwrap());
        }
        assert_eq!(output, b"h1,h2\r\n2,b\r\n");
    }

    #[tokio::test]
    async fn test_scan_range_stream_converts_custom_delimiter_split_across_chunks() {
        let chunks = stream::iter(vec![
            Ok::<_, std::io::Error>(Bytes::from_static(b"h1,h2^")),
            Ok::<_, std::io::Error>(Bytes::from_static(b"Y1,a^Y2,b^")),
            Ok::<_, std::io::Error>(Bytes::from_static(b"Y3,c^Y")),
        ]);
        let stream = scan_range_stream(chunks, b"^Y".to_vec(), SelectScanRange::new(12, 14), true, 0, 22);
        let mut stream = convert_record_delimiter_stream(stream, b"^Y".to_vec());
        let mut output = Vec::new();
        while let Some(bytes) = stream.next().await {
            output.extend_from_slice(&bytes.expect("custom-delimiter ScanRange chunk should be valid"));
        }
        assert_eq!(output, b"h1,h2\r\n2,b\r\n");
    }

    #[tokio::test]
    async fn test_record_delimiter_conversion_carries_partial_delimiter() {
        let chunks = stream::iter(vec![
            Ok::<_, object_store::Error>(Bytes::from_static(b"a,1^")),
            Ok::<_, object_store::Error>(Bytes::from_static(b"Yb,2^Y")),
        ]);
        let output = convert_record_delimiter_stream(chunks, b"^Y".to_vec())
            .try_collect::<Vec<_>>()
            .await
            .expect("convert record delimiter")
            .concat();
        assert_eq!(output, b"a,1\r\nb,2\r\n");
    }

    #[tokio::test]
    async fn test_record_delimiter_conversion_preserves_overlapping_match_order() {
        let chunks = stream::iter(vec![
            Ok::<_, object_store::Error>(Bytes::from_static(b"a")),
            Ok::<_, object_store::Error>(Bytes::from_static(b"aa")),
            Ok::<_, object_store::Error>(Bytes::from_static(b"a")),
        ]);
        let output = convert_record_delimiter_stream(chunks, b"aa".to_vec())
            .try_collect::<Vec<_>>()
            .await
            .expect("convert overlapping record delimiter")
            .concat();
        assert_eq!(output, b"\r\n\r\n");
    }

    #[tokio::test]
    async fn test_record_and_field_delimiter_conversion_order() {
        let chunks = stream::iter(vec![
            Ok::<_, object_store::Error>(Bytes::from_static(b"a\r")),
            Ok::<_, object_store::Error>(Bytes::from_static(b"\n1^")),
            Ok::<_, object_store::Error>(Bytes::from_static(b"Yb\r\n2^Y")),
        ]);
        let output = convert_csv_delimiter_stream(chunks, Some(b"^Y".to_vec()), Some("\r\n".to_string()))
            .try_collect::<Vec<_>>()
            .await
            .expect("convert record and field delimiters")
            .concat();
        assert_eq!(output, b"a,1\r\nb,2\r\n");
    }

    #[tokio::test]
    async fn test_record_delimiter_takes_precedence_when_delimiters_match() {
        let chunks = stream::iter(vec![
            Ok::<_, object_store::Error>(Bytes::from_static(b"a^")),
            Ok::<_, object_store::Error>(Bytes::from_static(b"Yb^Y")),
        ]);
        let output = convert_csv_delimiter_stream(chunks, Some(b"^Y".to_vec()), Some("^Y".to_string()))
            .try_collect::<Vec<_>>()
            .await
            .expect("convert matching record and field delimiters")
            .concat();
        assert_eq!(output, b"a\r\nb\r\n");

        let chunks = stream::iter(vec![Ok::<_, object_store::Error>(Bytes::from_static(b"a\r\nb\r\n"))]);
        let output = convert_csv_delimiter_stream(chunks, Some(b"\r\n".to_vec()), Some("\r\n".to_string()))
            .try_collect::<Vec<_>>()
            .await
            .expect("preserve matching normalized record delimiter")
            .concat();
        assert_eq!(output, b"a\r\nb\r\n");
    }

    #[tokio::test]
    async fn test_longer_field_delimiter_takes_precedence_over_record_prefix() {
        let chunks = stream::iter(vec![
            Ok::<_, object_store::Error>(Bytes::from_static(b"a^")),
            Ok::<_, object_store::Error>(Bytes::from_static(b"YQb^Y")),
        ]);
        let output = convert_csv_delimiter_stream(chunks, Some(b"^Y".to_vec()), Some("^YQ".to_string()))
            .try_collect::<Vec<_>>()
            .await
            .expect("convert record delimiter that prefixes field delimiter")
            .concat();
        assert_eq!(output, b"a,b\r\n");

        let chunks = stream::iter(vec![Ok::<_, object_store::Error>(Bytes::from_static(b"a\nXb\nc\nXd\n"))]);
        let output = convert_csv_delimiter_stream(chunks, Some(b"\n".to_vec()), Some("\nX".to_string()))
            .try_collect::<Vec<_>>()
            .await
            .expect("preserve longer field delimiter with native record prefix")
            .concat();
        assert_eq!(output, b"a,b\nc,d\n");
    }

    #[tokio::test]
    async fn test_scan_range_stream_rejects_early_eof() {
        let chunks = stream::iter(vec![Ok::<_, std::io::Error>(Bytes::from_static(b"1,a\n"))]);
        let mut output = scan_range_stream(chunks, b"\n".to_vec(), SelectScanRange::new(0, 7), false, 0, 8);

        assert_eq!(output.next().await.expect("first stream item").expect("first record"), b"1,a\n"[..]);
        let err = output
            .next()
            .await
            .expect("early EOF error")
            .expect_err("short ScanRange stream must fail");
        let object_store::Error::Generic { source, .. } = err else {
            panic!("expected generic object store error");
        };
        let source = source.downcast_ref::<std::io::Error>().expect("I/O error source");
        assert_eq!(source.kind(), std::io::ErrorKind::UnexpectedEof);
        assert!(source.to_string().contains("4 bytes remaining"));
        assert!(output.next().await.is_none());
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

    #[tokio::test]
    async fn test_scan_range_output_can_convert_field_delimiter() {
        let chunks = stream::iter(vec![Ok::<_, std::io::Error>(Bytes::from_static(b"a&&1\nb&&2\n"))]);
        let stream = scan_range_stream(chunks, b"\n".to_vec(), SelectScanRange::new(0, 10), false, 0, 10);
        let mut stream = convert_field_delimiter_stream(stream, "&&".to_string());
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
        let mut stream = convert_field_delimiter_stream(chunks, "&&".to_string());
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
        let mut stream = convert_field_delimiter_stream(chunks, "&&".to_string());
        let mut output = Vec::new();
        while let Some(bytes) = stream.next().await {
            output.extend_from_slice(&bytes.unwrap());
        }
        assert_eq!(output, b"a,");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial]
    async fn test_scan_range_self_overlapping_delimiter_retains_record_context() {
        let env = crate::storage_api::select_test_ecstore_env().await;
        let bucket = "s3select-scan-range-record-context";
        let object = "input.csv";
        env.make_bucket(bucket, false).await;
        let mut reader = SelectPutObjReader::from_vec(b"111aaa222aa333aa".to_vec());
        env.ecstore
            .put_object(bucket, object, &mut reader, &Default::default())
            .await
            .expect("put self-overlapping delimiter ScanRange fixture");

        let make_store = |start, end, file_header_info, snapshot| EcObjectStore {
            input: Arc::new(SelectObjectContentInput {
                bucket: bucket.to_string(),
                expected_bucket_owner: None,
                key: object.to_string(),
                sse_customer_algorithm: None,
                sse_customer_key: None,
                sse_customer_key_md5: None,
                request: SelectObjectContentRequest {
                    expression: "SELECT * FROM s3object".to_string(),
                    expression_type: ExpressionType::from_static(ExpressionType::SQL),
                    input_serialization: InputSerialization {
                        csv: Some(CSVInput {
                            record_delimiter: Some("aa".to_string()),
                            file_header_info,
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                    output_serialization: OutputSerialization {
                        csv: Some(CSVOutput::default()),
                        ..Default::default()
                    },
                    request_progress: None,
                    scan_range: Some(ScanRange {
                        start: Some(start),
                        end: Some(end),
                    }),
                },
            }),
            need_convert: false,
            delimiter: String::new(),
            is_json_document: false,
            json_sub_path: None,
            memory_pool: Arc::new(GreedyMemoryPool::new(1024)),
            query_tracker: None,
            store: None,
            snapshot: OnceCell::new_with(Some(snapshot)),
            reader_open_count: Arc::new(AtomicUsize::new(0)),
        };

        let store = make_store(6, 6, None, prepare_test_snapshot(bucket, object).await);
        let result = store
            .get_opts(&Path::from(object), GetOptions::default())
            .await
            .expect("read ScanRange starting inside record data");
        let GetResultPayload::Stream(stream) = result.payload else {
            panic!("expected streaming ScanRange payload");
        };
        let chunks: Vec<Bytes> = stream.try_collect().await.expect("collect record-data ScanRange output");
        assert!(chunks.concat().is_empty());
        drop(store);

        let store = make_store(4, 5, None, prepare_test_snapshot(bucket, object).await);
        let result = store
            .get_opts(&Path::from(object), GetOptions::default())
            .await
            .expect("read ScanRange starting inside overlapping delimiter");
        let GetResultPayload::Stream(stream) = result.payload else {
            panic!("expected streaming overlapping-delimiter payload");
        };
        let chunks: Vec<Bytes> = stream
            .try_collect()
            .await
            .expect("collect overlapping-delimiter ScanRange output");
        assert_eq!(chunks.concat(), b"a222\r\n");
        drop(store);

        let mut reader = SelectPutObjReader::from_vec(b"111aa222aa333aa".to_vec());
        env.ecstore
            .put_object(bucket, object, &mut reader, &Default::default())
            .await
            .expect("put exact review ScanRange fixture");
        let result = make_store(6, 6, None, prepare_test_snapshot(bucket, object).await)
            .get_opts(&Path::from(object), GetOptions::default())
            .await
            .expect("read exact review ScanRange fixture");
        let GetResultPayload::Stream(stream) = result.payload else {
            panic!("expected streaming exact review ScanRange payload");
        };
        let chunks: Vec<Bytes> = stream.try_collect().await.expect("collect exact review ScanRange output");
        assert!(chunks.concat().is_empty());

        let result = make_store(5, 5, None, prepare_test_snapshot(bucket, object).await)
            .get_opts(&Path::from(object), GetOptions::default())
            .await
            .expect("read ScanRange starting after an even delimiter run");
        let GetResultPayload::Stream(stream) = result.payload else {
            panic!("expected streaming even-run ScanRange payload");
        };
        let chunks: Vec<Bytes> = stream.try_collect().await.expect("collect even-run ScanRange output");
        assert_eq!(chunks.concat(), b"222\r\n");

        let mut reader = SelectPutObjReader::from_vec(b"h1aav1aav2aa".to_vec());
        env.ecstore
            .put_object(bucket, object, &mut reader, &Default::default())
            .await
            .expect("put ScanRange header snapshot fixture");
        let result = make_store(
            8,
            8,
            Some(FileHeaderInfo::from_static(FileHeaderInfo::USE)),
            prepare_test_snapshot(bucket, object).await,
        )
        .get_opts(&Path::from(object), GetOptions::default())
        .await
        .expect("read ScanRange with a separate header read");
        let GetResultPayload::Stream(stream) = result.payload else {
            panic!("expected streaming ScanRange header payload");
        };
        let chunks: Vec<Bytes> = stream.try_collect().await.expect("collect ScanRange header output");
        assert_eq!(chunks.concat(), b"h1\r\nv2\r\n");

        let mut reader = SelectPutObjReader::from_vec(b"aaa222aa".to_vec());
        env.ecstore
            .put_object(bucket, object, &mut reader, &Default::default())
            .await
            .expect("put object-start delimiter context fixture");
        let result = make_store(3, 3, None, prepare_test_snapshot(bucket, object).await)
            .get_opts(&Path::from(object), GetOptions::default())
            .await
            .expect("read delimiter context that reaches the object start");
        let GetResultPayload::Stream(stream) = result.payload else {
            panic!("expected streaming object-start context payload");
        };
        let chunks: Vec<Bytes> = stream.try_collect().await.expect("collect object-start context output");
        assert!(chunks.concat().is_empty());

        let run_start = SELECT_DEFAULT_READ_BUFFER_SIZE + 7;
        let mut large_fixture = vec![b'b'; run_start];
        large_fixture.extend_from_slice(b"aaa222aa");
        let mut reader = SelectPutObjReader::from_vec(large_fixture);
        env.ecstore
            .put_object(bucket, object, &mut reader, &Default::default())
            .await
            .expect("put large self-overlapping delimiter ScanRange fixture");
        let scan_start = i64::try_from(run_start + 3).expect("fixture offset should fit in i64");
        let result = make_store(scan_start, scan_start, None, prepare_test_snapshot(bucket, object).await)
            .get_opts(&Path::from(object), GetOptions::default())
            .await
            .expect("read large ScanRange with bounded delimiter context");
        let GetResultPayload::Stream(stream) = result.payload else {
            panic!("expected streaming large ScanRange payload");
        };
        let chunks: Vec<Bytes> = stream.try_collect().await.expect("collect large ScanRange output");
        assert!(chunks.concat().is_empty());

        let mut oversized_run = vec![b'b'];
        oversized_run.resize(SELECT_DEFAULT_READ_BUFFER_SIZE + 2, b'a');
        let scan_start = i64::try_from(oversized_run.len()).expect("fixture offset should fit in i64");
        oversized_run.extend_from_slice(b"222aa");
        let mut reader = SelectPutObjReader::from_vec(oversized_run);
        env.ecstore
            .put_object(bucket, object, &mut reader, &Default::default())
            .await
            .expect("put oversized delimiter context fixture");
        let err = make_store(scan_start, scan_start, None, prepare_test_snapshot(bucket, object).await)
            .get_opts(&Path::from(object), GetOptions::default())
            .await
            .expect_err("oversized self-overlapping delimiter context must fail closed");
        assert!(err.to_string().contains("bounded ScanRange context"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial]
    async fn test_self_overlapping_record_delimiter_uses_single_full_file_partition() {
        const TARGET_PARTITIONS: usize = 4;

        let mut input_bytes = Vec::with_capacity(SELECT_DEFAULT_READ_BUFFER_SIZE + 8);
        input_bytes.extend_from_slice(b"0,");
        input_bytes.resize(SELECT_DEFAULT_READ_BUFFER_SIZE - 1, b'b');
        input_bytes.extend_from_slice(b"aaaX,caa");
        assert!(input_bytes.len() > 1024 * 1024);
        assert_eq!(
            &input_bytes[SELECT_DEFAULT_READ_BUFFER_SIZE - 1..SELECT_DEFAULT_READ_BUFFER_SIZE + 2],
            b"aaa"
        );

        let env = crate::storage_api::select_test_ecstore_env().await;
        let bucket = "s3select-record-delimiter";
        let object = "input.csv";
        env.make_bucket(bucket, false).await;
        let mut reader = SelectPutObjReader::from_vec(input_bytes);
        env.ecstore
            .put_object(bucket, object, &mut reader, &Default::default())
            .await
            .expect("put multi-byte record-delimited test object");

        let input = Arc::new(SelectObjectContentInput {
            bucket: bucket.to_string(),
            expected_bucket_owner: None,
            key: object.to_string(),
            sse_customer_algorithm: None,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            request: SelectObjectContentRequest {
                expression: "SELECT * FROM s3object".to_string(),
                expression_type: ExpressionType::from_static(ExpressionType::SQL),
                input_serialization: InputSerialization {
                    csv: Some(CSVInput {
                        record_delimiter: Some("aa".to_string()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                output_serialization: OutputSerialization {
                    csv: Some(CSVOutput::default()),
                    ..Default::default()
                },
                request_progress: None,
                scan_range: None,
            },
        });
        let snapshot = prepare_test_snapshot(bucket, object).await;
        let store = Arc::new(EcObjectStore {
            input,
            need_convert: false,
            delimiter: String::new(),
            is_json_document: false,
            json_sub_path: None,
            memory_pool: Arc::new(GreedyMemoryPool::new(32 * 1024 * 1024)),
            query_tracker: None,
            store: None,
            snapshot: OnceCell::new_with(Some(snapshot)),
            reader_open_count: Arc::new(AtomicUsize::new(0)),
        });

        let config = SessionConfig::new()
            .with_repartition_file_scans(false)
            .with_repartition_file_min_size(0)
            .with_target_partitions(TARGET_PARTITIONS);
        let context = SessionContext::new_with_config(config);
        let store_url = url::Url::parse(&format!("s3://{bucket}")).expect("valid object store URL");
        context.runtime_env().register_object_store(&store_url, store);
        context
            .register_csv("records", &format!("s3://{bucket}/{object}"), CsvReadOptions::new().has_header(false))
            .await
            .expect("register partitioned CSV");

        let scan_plan = context
            .sql("SELECT * FROM records")
            .await
            .expect("plan partitioned CSV")
            .create_physical_plan()
            .await
            .expect("create partitioned CSV physical plan");
        assert_eq!(scan_plan.output_partitioning().partition_count(), 1);

        let batches = context
            .sql("SELECT column_1 FROM records")
            .await
            .expect("plan exact-result query")
            .collect()
            .await
            .expect("query self-overlapping record-delimited CSV");
        let values = batches
            .iter()
            .flat_map(|batch| {
                let column = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::StringArray>()
                    .expect("mixed first column should be Utf8");
                column.iter().map(|value| value.map(str::to_string)).collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(values, vec![Some("0".to_string()), Some("aX".to_string())]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial]
    async fn test_get_opts_validates_raw_length_before_delimiter_conversion() {
        let env = crate::storage_api::select_test_ecstore_env().await;
        let bucket = "s3select-multi-byte-delimiter";
        let object = "input.csv";
        let input_bytes = b"a\r\n1^Y";
        env.make_bucket(bucket, false).await;
        let mut reader = SelectPutObjReader::from_vec(input_bytes.to_vec());
        env.ecstore
            .put_object(bucket, object, &mut reader, &Default::default())
            .await
            .expect("put multi-byte-delimited test object");

        let input = Arc::new(SelectObjectContentInput {
            bucket: bucket.to_string(),
            expected_bucket_owner: None,
            key: object.to_string(),
            sse_customer_algorithm: None,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            request: SelectObjectContentRequest {
                expression: "SELECT * FROM s3object".to_string(),
                expression_type: ExpressionType::from_static(ExpressionType::SQL),
                input_serialization: InputSerialization {
                    csv: Some(CSVInput {
                        field_delimiter: Some("\r\n".to_string()),
                        record_delimiter: Some("^Y".to_string()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                output_serialization: OutputSerialization {
                    csv: Some(CSVOutput::default()),
                    ..Default::default()
                },
                request_progress: None,
                scan_range: None,
            },
        });
        let snapshot = prepare_test_snapshot(bucket, object).await;
        let store = super::EcObjectStore {
            input,
            need_convert: true,
            delimiter: "\r\n".to_string(),
            is_json_document: false,
            json_sub_path: None,
            memory_pool: Arc::new(GreedyMemoryPool::new(1024)),
            query_tracker: None,
            store: None,
            snapshot: OnceCell::new_with(Some(snapshot)),
            reader_open_count: Arc::new(AtomicUsize::new(0)),
        };

        let result = store
            .get_opts(&Path::from(object), GetOptions::default())
            .await
            .expect("read multi-byte-delimited test object");
        let GetResultPayload::Stream(stream) = result.payload else {
            panic!("expected streaming object payload");
        };
        let chunks: Vec<Bytes> = stream.try_collect().await.expect("collect converted object stream");

        assert_eq!(chunks.concat(), b"a,1\r\n");

        let requested_range = 3..10;
        let ranges = store
            .get_ranges(&Path::from(object), std::slice::from_ref(&requested_range))
            .await
            .expect("bounded range past EOF should return the object remainder");
        assert_eq!(ranges, vec![Bytes::from_static(b"1^Y")]);
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

    #[tokio::test]
    async fn test_bytes_stream_rejects_early_eof() {
        let source = stream::iter(vec![Ok::<_, std::io::Error>(Bytes::from_static(b"ab"))]);
        let output = bytes_stream(source, 4);
        futures::pin_mut!(output);

        assert_eq!(output.next().await.expect("first stream item").expect("first chunk"), b"ab"[..]);
        let err = output
            .next()
            .await
            .expect("early EOF error")
            .expect_err("short stream must fail");
        let object_store::Error::Generic { store, source } = err else {
            panic!("expected generic object store error");
        };
        assert_eq!(store, "EcObjectStore");
        let source = source.downcast_ref::<std::io::Error>().expect("I/O error source");
        assert_eq!(source.kind(), std::io::ErrorKind::UnexpectedEof);
        assert!(source.to_string().contains("2 bytes remaining"));
        assert!(output.next().await.is_none());
    }

    #[tokio::test]
    async fn test_json_document_stream_respects_query_memory_pool() {
        let input = b"{}".to_vec();
        let required = input.len() * JSON_DOCUMENT_MEMORY_RESERVATION_MULTIPLIER;
        let memory_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(required - 1));
        let mut output = json_document_ndjson_stream(
            Box::new(std::io::Cursor::new(input.clone())),
            input.len() as u64,
            None,
            memory_pool,
            None,
        );

        let err = output
            .next()
            .await
            .expect("memory error")
            .expect_err("reservation should exceed the pool");
        let object_store::Error::Generic { source, .. } = err else {
            panic!("expected generic object store error");
        };
        assert!(matches!(
            source.downcast_ref::<DataFusionError>(),
            Some(DataFusionError::ResourcesExhausted(_))
        ));
    }

    #[tokio::test]
    async fn test_json_document_stream_releases_memory_reservation() {
        let input = b"[1,2]".to_vec();
        let required = input.len() * JSON_DOCUMENT_MEMORY_RESERVATION_MULTIPLIER;
        let memory_pool = Arc::new(GreedyMemoryPool::new(required));
        let output: Vec<Bytes> = json_document_ndjson_stream(
            Box::new(std::io::Cursor::new(input.clone())),
            input.len() as u64,
            None,
            memory_pool.clone(),
            None,
        )
        .try_collect()
        .await
        .expect("JSON conversion should fit the pool");

        assert_eq!(output, vec![Bytes::from_static(b"1\n"), Bytes::from_static(b"2\n")]);
        assert_eq!(memory_pool.reserved(), 0);
    }

    #[tokio::test]
    async fn test_json_document_stream_rejects_early_eof() {
        let input = b"{}".to_vec();
        let memory_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(4 * JSON_DOCUMENT_MEMORY_RESERVATION_MULTIPLIER));
        let mut output = json_document_ndjson_stream(Box::new(std::io::Cursor::new(input)), 4, None, memory_pool, None);

        let err = output
            .next()
            .await
            .expect("early EOF error")
            .expect_err("short JSON document must fail");
        let object_store::Error::Generic { source, .. } = err else {
            panic!("expected generic object store error");
        };
        let source = source.downcast_ref::<std::io::Error>().expect("I/O error source");
        assert_eq!(source.kind(), std::io::ErrorKind::UnexpectedEof);
        assert!(source.to_string().contains("2 bytes remaining"));
        assert!(output.next().await.is_none());
    }

    #[tokio::test]
    async fn malformed_json_document_stream_has_typed_select_error() {
        let input = b"{bad".to_vec();
        let memory_pool: Arc<dyn MemoryPool> =
            Arc::new(GreedyMemoryPool::new(input.len() * JSON_DOCUMENT_MEMORY_RESERVATION_MULTIPLIER));
        let mut output = json_document_ndjson_stream(
            Box::new(std::io::Cursor::new(input.clone())),
            input.len() as u64,
            None,
            memory_pool,
            None,
        );

        let source = output
            .next()
            .await
            .expect("malformed JSON should produce one stream error")
            .expect_err("malformed JSON DOCUMENT must fail");
        let error = QueryError::from(DataFusionError::ObjectStore(Box::new(source)));

        assert_eq!(error.select_error(), SelectError::JsonParsingError);
        assert!(output.next().await.is_none());
    }

    #[test]
    fn storage_error_mapper_preserves_protocol_classification() {
        let classify = |source| QueryError::from(DataFusionError::ObjectStore(Box::new(source))).select_error();

        assert_eq!(
            classify(map_storage_error(
                "private-bucket",
                "private-object",
                SelectStorageError::BucketNotFound("private-bucket".to_string()),
            )),
            SelectError::BucketNotFound
        );
        assert_eq!(
            classify(map_storage_error(
                "private-bucket",
                "private-object",
                SelectStorageError::ObjectNotFound("private-bucket".to_string(), "private-object".to_string()),
            )),
            SelectError::ObjectNotFound
        );
        assert_eq!(
            classify(map_storage_error("private-bucket", "private-object", SelectStorageError::LessData)),
            SelectError::InternalError
        );
        assert_eq!(
            classify(scan_range_from_bounds(Some(10), None, 10).expect_err("out-of-bounds range must fail")),
            SelectError::InvalidScanRange
        );
        let parquet_source = map_storage_error(
            "private-bucket",
            "private-object",
            SelectStorageError::ObjectNotFound("private-bucket".to_string(), "private-object".to_string()),
        );
        let parquet_error = QueryError::from(DataFusionError::ParquetError(Box::new(
            datafusion::parquet::errors::ParquetError::External(Box::new(parquet_source)),
        )));
        assert_eq!(parquet_error.select_error(), SelectError::ObjectNotFound);
    }

    #[test]
    fn test_json_document_size_error_is_resource_exhausted() {
        assert!(validate_json_document_size(super::MAX_JSON_DOCUMENT_BYTES).is_ok());

        let err = validate_json_document_size(super::MAX_JSON_DOCUMENT_BYTES + 1).expect_err("oversized JSON document must fail");
        let object_store::Error::Generic { source, .. } = err else {
            panic!("expected generic object store error");
        };
        assert!(matches!(
            source.downcast_ref::<DataFusionError>(),
            Some(DataFusionError::ResourcesExhausted(_))
        ));
    }

    #[test]
    fn test_json_document_queued_parse_releases_query_guard_when_cancelled() {
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

            let admission = Arc::new(Semaphore::new(1));
            let permit = Arc::clone(&admission)
                .acquire_owned()
                .await
                .expect("query permit should be available");
            let query_guard: QueryExecutionGuard = Arc::new(permit);
            let query_tracker = QueryExecutionTracker::new(
                &QueryExecutionOwner::new(),
                query_guard,
                tokio::time::Instant::now() + std::time::Duration::from_secs(30),
                30,
            );
            let input = b"{}".to_vec();
            let memory_pool: Arc<dyn MemoryPool> =
                Arc::new(GreedyMemoryPool::new(input.len() * JSON_DOCUMENT_MEMORY_RESERVATION_MULTIPLIER));
            let mut output = json_document_ndjson_stream(
                Box::new(std::io::Cursor::new(input.clone())),
                input.len() as u64,
                None,
                memory_pool,
                Some(query_tracker),
            );

            {
                let next = output.next();
                futures::pin_mut!(next);
                assert!(futures::poll!(next.as_mut()).is_pending());
            }
            drop(output);

            let recovered_permit =
                tokio::time::timeout(std::time::Duration::from_secs(5), Arc::clone(&admission).acquire_owned())
                    .await
                    .expect("queued JSON parse should be cancelled")
                    .expect("query admission should remain open");
            release_blocking_tx.send(()).expect("release blocking worker");
            blocker.await.expect("blocking worker should finish");
            drop(recovered_permit);
            assert_eq!(admission.available_permits(), 1);
        });
    }

    #[test]
    fn test_json_document_expired_queued_parse_does_not_start() {
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

            let admission = Arc::new(Semaphore::new(1));
            let permit = Arc::clone(&admission)
                .acquire_owned()
                .await
                .expect("query permit should be available");
            let owner = QueryExecutionOwner::new();
            let query_tracker = QueryExecutionTracker::new(
                &owner,
                Arc::new(permit),
                tokio::time::Instant::now() + std::time::Duration::from_secs(30),
                30,
            );
            let input = b"{}".to_vec();
            let memory_pool: Arc<dyn MemoryPool> =
                Arc::new(GreedyMemoryPool::new(input.len() * JSON_DOCUMENT_MEMORY_RESERVATION_MULTIPLIER));
            let parser_started = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let parser_started_in_task = Arc::clone(&parser_started);
            let mut output = json_document_ndjson_stream_with_parser(
                Box::new(std::io::Cursor::new(input.clone())),
                input.len() as u64,
                None,
                memory_pool,
                Some(query_tracker.clone()),
                move |_, _| {
                    parser_started_in_task.store(true, std::sync::atomic::Ordering::SeqCst);
                    Ok(vec![Bytes::from_static(b"{}\n")])
                },
            );

            {
                let next = output.next();
                futures::pin_mut!(next);
                assert!(futures::poll!(next.as_mut()).is_pending());
            }
            query_tracker.expire(&owner);
            assert_eq!(admission.available_permits(), 1);
            release_blocking_tx.send(()).expect("release blocking worker");
            blocker.await.expect("blocking worker should finish");

            let err = tokio::time::timeout(std::time::Duration::from_secs(5), output.next())
                .await
                .expect("queued parser should resume")
                .expect("queued parser should return an error")
                .expect_err("expired queued parser must not run");
            let object_store::Error::Generic { source, .. } = err else {
                panic!("expected generic object store error");
            };
            let source = source.downcast_ref::<std::io::Error>().expect("I/O error source");
            assert_eq!(source.kind(), std::io::ErrorKind::Interrupted);
            assert!(!parser_started.load(std::sync::atomic::Ordering::SeqCst));
        });
    }

    #[test]
    fn test_json_document_started_parse_retains_query_guard_when_cancelled() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .expect("build test runtime");

        runtime.block_on(async {
            let admission = Arc::new(Semaphore::new(1));
            let permit = Arc::clone(&admission)
                .acquire_owned()
                .await
                .expect("query permit should be available");
            let query_guard: QueryExecutionGuard = Arc::new(permit);
            let query_tracker = QueryExecutionTracker::new(
                &QueryExecutionOwner::new(),
                query_guard,
                tokio::time::Instant::now() + std::time::Duration::from_secs(30),
                30,
            );
            let input = b"{}".to_vec();
            let memory_pool: Arc<dyn MemoryPool> =
                Arc::new(GreedyMemoryPool::new(input.len() * JSON_DOCUMENT_MEMORY_RESERVATION_MULTIPLIER));
            let (parse_started_tx, parse_started_rx) = tokio::sync::oneshot::channel();
            let (release_parse_tx, release_parse_rx) = std::sync::mpsc::channel();
            let mut output = json_document_ndjson_stream_with_parser(
                Box::new(std::io::Cursor::new(input.clone())),
                input.len() as u64,
                None,
                memory_pool,
                Some(query_tracker),
                move |_, _| {
                    let _ = parse_started_tx.send(());
                    release_parse_rx.recv().expect("release JSON parser");
                    Ok(vec![Bytes::from_static(b"{}\n")])
                },
            );

            {
                let next = output.next();
                futures::pin_mut!(next);
                assert!(futures::poll!(next.as_mut()).is_pending());
            }
            parse_started_rx.await.expect("JSON parser should start");
            drop(output);

            assert!(Arc::clone(&admission).try_acquire_owned().is_err());
            release_parse_tx.send(()).expect("release JSON parser");
            let recovered_permit =
                tokio::time::timeout(std::time::Duration::from_secs(5), Arc::clone(&admission).acquire_owned())
                    .await
                    .expect("started JSON parse should release the query guard")
                    .expect("query admission should remain open");
            drop(recovered_permit);
            assert_eq!(admission.available_permits(), 1);
        });
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
    fn test_extract_json_sub_path_rejects_unsupported_bracket_path() {
        let sql = "SELECT e.name FROM s3object.employees[*] e";
        assert_eq!(extract_json_sub_path_from_expression(sql), None);
    }

    #[test]
    fn test_extract_json_sub_path_ignores_from_in_string_literal() {
        let sql = "SELECT ' from ' AS marker FROM S3Object.employees";
        assert_eq!(extract_json_sub_path_from_expression(sql), Some("employees".to_string()));
    }

    #[test]
    fn test_extract_json_sub_path_ignores_from_in_comment() {
        let sql = "SELECT /* from S3Object.wrong */ e.name FROM S3Object.employees AS e";
        assert_eq!(extract_json_sub_path_from_expression(sql), Some("employees".to_string()));
    }

    #[test]
    fn test_extract_json_sub_path_supports_quoted_identifier() {
        let sql = "SELECT \" from \" FROM S3Object.\"employee data\"";
        assert_eq!(extract_json_sub_path_from_expression(sql), Some("employee data".to_string()));
    }
}
