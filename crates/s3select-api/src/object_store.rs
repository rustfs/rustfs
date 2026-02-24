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
use futures::{Stream, StreamExt};
use futures_core::stream::BoxStream;
use http::HeaderMap;
use object_store::{
    Attributes, Error as o_Error, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, path::Path,
};
use pin_project_lite::pin_project;
use rustfs_common::DEFAULT_DELIMITER;
use rustfs_ecstore::StorageAPI;
use rustfs_ecstore::new_object_layer_fn;
use rustfs_ecstore::set_disk::DEFAULT_READ_BUFFER_SIZE;
use rustfs_ecstore::store::ECStore;
use rustfs_ecstore::store_api::ObjectIO;
use rustfs_ecstore::store_api::ObjectOptions;
use s3s::S3Result;
use s3s::dto::SelectObjectContentInput;
use s3s::s3_error;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::task::ready;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio_util::io::ReaderStream;
use tracing::info;
use transform_stream::AsyncTryStream;

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
impl EcObjectStore {
    pub fn new(input: Arc<SelectObjectContentInput>) -> S3Result<Self> {
        let Some(store) = new_object_layer_fn() else {
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
}

impl std::fmt::Display for EcObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("EcObjectStore")
    }
}

#[async_trait]
impl ObjectStore for EcObjectStore {
    async fn put_opts(&self, _location: &Path, _payload: PutPayload, _opts: PutOptions) -> Result<PutResult> {
        unimplemented!()
    }

    async fn put_multipart_opts(&self, _location: &Path, _opts: PutMultipartOptions) -> Result<Box<dyn MultipartUpload>> {
        unimplemented!()
    }

    async fn get_opts(&self, location: &Path, _options: GetOptions) -> Result<GetResult> {
        info!("{:?}", location);
        let opts = ObjectOptions::default();
        let h = HeaderMap::new();
        let reader = self
            .store
            .get_object_reader(&self.input.bucket, &self.input.key, None, h, &opts)
            .await
            .map_err(|_| o_Error::NotFound {
                path: format!("{}/{}", self.input.bucket, self.input.key),
                source: "can not get object info".into(),
            })?;

        let original_size = reader.object_info.size as u64;
        let etag = reader.object_info.etag;
        let attributes = Attributes::default();

        let (payload, size) = if self.is_json_document {
            // Buffer the entire object and flatten from a JSON DOCUMENT
            // (multi-line formatted JSON object/array) into NDJSON so that
            // DataFusion's Arrow JSON reader can process it line-by-line.
            let mut all_bytes = Vec::with_capacity(original_size as usize);
            reader
                .stream
                .take(original_size)
                .read_to_end(&mut all_bytes)
                .await
                .map_err(|e| o_Error::Generic {
                    store: "EcObjectStore",
                    source: Box::new(e),
                })?;
            let ndjson_bytes =
                flatten_json_document_to_ndjson(&all_bytes, self.json_sub_path.as_deref()).map_err(|e| o_Error::Generic {
                    store: "EcObjectStore",
                    source: Box::new(e),
                })?;
            let ndjson_size = ndjson_bytes.len() as u64;
            let stream = futures::stream::once(async move { Ok(ndjson_bytes) }).boxed();
            (object_store::GetResultPayload::Stream(stream), ndjson_size)
        } else if self.need_convert {
            let stream = bytes_stream(
                ReaderStream::with_capacity(ConvertStream::new(reader.stream, self.delimiter.clone()), DEFAULT_READ_BUFFER_SIZE),
                original_size as usize,
            )
            .boxed();
            (object_store::GetResultPayload::Stream(stream), original_size)
        } else {
            let stream = bytes_stream(
                ReaderStream::with_capacity(reader.stream, DEFAULT_READ_BUFFER_SIZE),
                original_size as usize,
            )
            .boxed();
            (object_store::GetResultPayload::Stream(stream), original_size)
        };

        let meta = ObjectMeta {
            location: location.clone(),
            last_modified: Utc::now(),
            size,
            e_tag: etag,
            version: None,
        };

        Ok(GetResult {
            payload,
            meta,
            range: 0..size,
            attributes,
        })
    }

    async fn get_ranges(&self, _location: &Path, _ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        unimplemented!()
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        info!("{:?}", location);
        let opts = ObjectOptions::default();
        let info = self
            .store
            .get_object_info(&self.input.bucket, &self.input.key, &opts)
            .await
            .map_err(|_| o_Error::NotFound {
                path: format!("{}/{}", self.input.bucket, self.input.key),
                source: "can not get object info".into(),
            })?;

        Ok(ObjectMeta {
            location: location.clone(),
            last_modified: Utc::now(),
            size: info.size as u64,
            e_tag: info.etag,
            version: None,
        })
    }

    async fn delete(&self, _location: &Path) -> Result<()> {
        unimplemented!()
    }

    fn list(&self, _prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        unimplemented!()
    }

    async fn list_with_delimiter(&self, _prefix: Option<&Path>) -> Result<ListResult> {
        unimplemented!()
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> Result<()> {
        unimplemented!()
    }

    async fn copy_if_not_exists(&self, _from: &Path, _too: &Path) -> Result<()> {
        unimplemented!()
    }
}

pin_project! {
    struct ConvertStream<R> {
        inner: R,
        delimiter: Vec<u8>,
    }
}

impl<R> ConvertStream<R> {
    fn new(inner: R, delimiter: String) -> Self {
        ConvertStream {
            inner,
            delimiter: delimiter.as_bytes().to_vec(),
        }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for ConvertStream<R> {
    #[tracing::instrument(level = "debug", skip_all)]
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let me = self.project();
        ready!(Pin::new(&mut *me.inner).poll_read(cx, buf))?;
        let bytes = buf.filled();
        let replaced = replace_symbol(me.delimiter, bytes);
        buf.clear();
        buf.put_slice(&replaced);
        Poll::Ready(Ok(()))
    }
}

fn replace_symbol(delimiter: &[u8], slice: &[u8]) -> Vec<u8> {
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

    // Must start with "s3object" (case-insensitive).
    if !after_from.to_lowercase().starts_with("s3object") {
        return None;
    }
    let after_s3object = &after_from["s3object".len()..];

    // If the very next character is '.' there is a sub-path.
    if let Some(rest) = after_s3object.strip_prefix('.') {
        // Collect characters until whitespace, '[', or end-of-string.
        let end = rest
            .find(|c: char| c.is_whitespace() || c == '[' || c == ']')
            .unwrap_or(rest.len());
        let path = rest[..end].trim();
        if !path.is_empty() {
            return Some(path.to_string());
        }
    }
    None
}

/// Convert a JSON DOCUMENT (single JSON object or JSON array) to NDJSON
/// (newline-delimited JSON) so that DataFusion's Arrow reader can process
/// it line-by-line.
///
/// `json_sub_path` – when the SQL expression uses `FROM s3object.<key>`,
/// pass `Some(key)` here so that this function navigates to the nested array
/// before emitting rows.  For example, given the document
/// `{"employees":[{…},{…}]}` and `json_sub_path = Some("employees")`,
/// each element of the `employees` array becomes one NDJSON line.
///
/// - A JSON array `[{...}, {...}]` becomes one JSON object per line.
/// - A single JSON object `{...}` (with no sub-path) becomes a single line.
/// - Any other scalar value is emitted as one line.
fn flatten_json_document_to_ndjson(bytes: &[u8], json_sub_path: Option<&str>) -> std::io::Result<Bytes> {
    let root: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

    // Navigate into the sub-path when the root is a JSON object and a path was
    // extracted from the SQL FROM clause (e.g. `FROM s3object.employees`).
    let value = if let Some(path) = json_sub_path {
        if let serde_json::Value::Object(ref obj) = root {
            match obj.get(path) {
                Some(sub) => sub.clone(),
                // Path not found – fall back to emitting the whole root object.
                None => root,
            }
        } else {
            // Root is already an array or scalar; ignore the path.
            root
        }
    } else {
        root
    };

    let mut output: Vec<u8> = Vec::new();
    match value {
        serde_json::Value::Array(arr) => {
            for item in arr {
                let line = serde_json::to_string(&item).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                output.extend_from_slice(line.as_bytes());
                output.push(b'\n');
            }
        }
        other => {
            let line = serde_json::to_string(&other).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            output.extend_from_slice(line.as_bytes());
            output.push(b'\n');
        }
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
        while let Some(result) = stream.next().await {
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
    use super::{extract_json_sub_path_from_expression, flatten_json_document_to_ndjson, replace_symbol};

    #[test]
    fn test_replace() {
        let ss = String::from("dandan&&is&&best");
        let slice = ss.as_bytes();
        let delimiter = b"&&";
        println!("len: {}", "╦".len());
        let result = replace_symbol(delimiter, slice);
        match String::from_utf8(result) {
            Ok(s) => println!("slice: {s}"),
            Err(e) => eprintln!("Error converting to string: {e}"),
        }
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
