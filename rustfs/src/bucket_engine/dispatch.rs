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

//! S3 dispatch for buckets bound to an engine.
//!
//! The S3 handlers call into this module after SigV4 authentication and
//! `S3Access` authorization. Requests for object-engine buckets return
//! `None`/`Ok(())` and continue to the object layer. Requests for an
//! engine-bound bucket are served by the engine when it declares the
//! operation, and otherwise refused with a stable `NotImplemented` error;
//! they never reach the object layer.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use rustfs_bucket_engine::{
    BucketFeature, EngineError, EngineObjectInfo, EngineObjectOperation, ListPage, ListQuery, ObjectWriteMeta,
};
use s3s::dto::{
    CommonPrefix, DeleteObjectInput, DeleteObjectOutput, ETag, GetObjectInput, GetObjectOutput, HeadObjectInput,
    HeadObjectOutput, ListObjectsInput, ListObjectsOutput, ListObjectsV2Input, ListObjectsV2Output, Object, PutObjectInput,
    PutObjectOutput, Range, StreamingBlob, Timestamp,
};
use s3s::{S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use time::OffsetDateTime;

use super::binding::engine_error;
use super::rdma::{self, RDMA_BYTES_TRANSFERRED_HEADER, RDMA_REPLY_HEADER};
use super::{BoundBucket, EngineRegistry, EngineRoute};

/// Largest object body the framework buffers for an engine PutObject.
pub(crate) const MAX_INLINE_OBJECT_BYTES: u64 = 256 * 1024 * 1024;

const DEFAULT_MAX_KEYS: usize = 1000;
const ACCEPT_RANGES_BYTES: &str = "bytes";

/// The engine serving `bucket`, or `None` for the object layer. A bucket
/// whose engine is unavailable on this node is refused.
pub(crate) fn bound_bucket(bucket: &str) -> S3Result<Option<Arc<BoundBucket>>> {
    match EngineRegistry::global().route(bucket) {
        EngineRoute::Object => Ok(None),
        EngineRoute::Engine(bound) => Ok(Some(bound)),
        EngineRoute::Unavailable { engine, reason } => Err(S3Error::with_message(
            S3ErrorCode::ServiceUnavailable,
            format!("bucket engine {engine} is unavailable: {reason}"),
        )),
    }
}

/// Refuses an S3 operation that is never routed to engines when `bucket` is
/// bound to one.
pub(crate) fn refuse_unrouted(bucket: &str, operation: &'static str) -> S3Result<()> {
    match bound_bucket(bucket)? {
        None => Ok(()),
        Some(bound) => Err(not_served(&bound, operation)),
    }
}

/// Refuses configuring a bucket feature the bound engine refuses.
pub(crate) fn refuse_feature(bucket: &str, feature: BucketFeature) -> S3Result<()> {
    match bound_bucket(bucket)? {
        Some(bound) if bound.engine.descriptor().capabilities.refuses(feature) => Err(S3Error::with_message(
            S3ErrorCode::InvalidRequest,
            format!(
                "{feature:?} cannot be configured on buckets bound to engine {}",
                bound.context.binding.engine.as_str()
            ),
        )),
        _ => Ok(()),
    }
}

fn not_served(bound: &BoundBucket, operation: &str) -> S3Error {
    S3Error::with_message(
        S3ErrorCode::NotImplemented,
        format!(
            "{operation} is not supported on buckets bound to engine {}",
            bound.context.binding.engine.as_str()
        ),
    )
}

fn require(bound: &BoundBucket, operation: EngineObjectOperation, name: &str) -> S3Result<()> {
    if bound.engine.descriptor().capabilities.serves(operation) {
        Ok(())
    } else {
        Err(not_served(bound, name))
    }
}

fn refuse_version_id(version_id: Option<&str>) -> S3Result<()> {
    match version_id {
        None | Some("null") => Ok(()),
        Some(_) => Err(s3_error!(InvalidArgument, "engine buckets are unversioned")),
    }
}

fn object_error(err: EngineError) -> S3Error {
    match err {
        EngineError::NotFound => s3_error!(NoSuchKey),
        other => engine_error(other),
    }
}

fn etag(info: &EngineObjectInfo) -> Option<ETag> {
    (!info.etag.is_empty()).then(|| ETag::Strong(info.etag.clone()))
}

fn last_modified(info: &EngineObjectInfo) -> Option<Timestamp> {
    OffsetDateTime::from_unix_timestamp(info.last_modified_unix_secs)
        .ok()
        .map(Timestamp::from)
}

fn size_i64(size: u64) -> i64 {
    i64::try_from(size).unwrap_or(i64::MAX)
}

/// Resolves an HTTP range against an object size: the byte span and the
/// `Content-Range` value.
fn resolve_range(range: &Range, size: u64) -> S3Result<(std::ops::Range<u64>, String)> {
    let span = match *range {
        Range::Int { first, last } if first < size => first..last.map_or(size, |last| last.saturating_add(1).min(size)),
        Range::Suffix { length } if length > 0 && size > 0 => size.saturating_sub(length)..size,
        _ => return Err(s3_error!(InvalidRange, "the requested range is not satisfiable")),
    };
    let content_range = format!("bytes {}-{}/{size}", span.start, span.end - 1);
    Ok((span, content_range))
}

/// The RDMA transfer a request asks for, when the engine moves data over
/// RDMA and the RDMA service runs.
fn rdma_transfer(bound: &BoundBucket, headers: &http::HeaderMap) -> Option<rustfs_bucket_engine::RdmaTransfer> {
    if !bound.engine.descriptor().capabilities.rdma {
        return None;
    }
    rdma::transfer_from_headers(headers)
}

fn rdma_reply_headers(headers: &mut http::HeaderMap, moved: u64, ranged: bool) {
    let status = if ranged { "206" } else { "200" };
    headers.insert(RDMA_REPLY_HEADER, http::HeaderValue::from_static(status));
    headers.insert(RDMA_BYTES_TRANSFERRED_HEADER, http::HeaderValue::from(moved));
}

pub(crate) async fn get_object(bound: &BoundBucket, req: S3Request<GetObjectInput>) -> S3Result<S3Response<GetObjectOutput>> {
    require(bound, EngineObjectOperation::GetObject, "GetObject")?;
    let headers = req.headers;
    let input = req.input;
    refuse_version_id(input.version_id.as_deref())?;
    if input.part_number.is_some() {
        return Err(not_served(bound, "GetObject with partNumber"));
    }
    // Ranged reads stay on HTTP; whole entries move over RDMA when asked.
    // Over RDMA the engine pushes the entry, or one byte range of it, into
    // the client's buffer. Suffix ranges need the size first and stay on HTTP.
    let rdma_range = match &input.range {
        None => Some(None),
        Some(Range::Int { first, last }) => Some(Some(rustfs_bucket_engine::ByteRange {
            start: *first,
            length: last.map_or(u64::MAX, |last| last.saturating_sub(*first).saturating_add(1)),
        })),
        Some(Range::Suffix { .. }) => None,
    };
    let transfer = rdma_range.is_some().then(|| rdma_transfer(bound, &headers)).flatten();
    let object = bound
        .engine
        .get_object(
            &bound.context,
            &input.key,
            if transfer.is_some() { rdma_range.flatten() } else { None },
            transfer,
        )
        .await
        .map_err(object_error)?;
    if let Some(moved) = object.rdma_bytes {
        let content_range = match &input.range {
            Some(Range::Int { first, .. }) if moved > 0 => {
                Some(format!("bytes {first}-{}/{}", first + moved - 1, object.info.size))
            }
            _ => None,
        };
        let ranged = input.range.is_some();
        let output = GetObjectOutput {
            content_length: Some(0),
            body: Some(StreamingBlob::from(s3s::Body::empty())),
            content_range,
            e_tag: etag(&object.info),
            last_modified: last_modified(&object.info),
            accept_ranges: Some(ACCEPT_RANGES_BYTES.to_owned()),
            ..Default::default()
        };
        let mut response = S3Response::new(output);
        rdma_reply_headers(&mut response.headers, moved, ranged);
        if ranged {
            response.status = Some(http::StatusCode::PARTIAL_CONTENT);
        }
        return Ok(response);
    }
    let size = object.body.len() as u64;
    let (body, content_range) = match &input.range {
        None => (object.body, None),
        Some(range) => {
            let (span, content_range) = resolve_range(range, size)?;
            (object.body.slice(span.start as usize..span.end as usize), Some(content_range))
        }
    };
    let ranged = content_range.is_some();
    let output = GetObjectOutput {
        content_length: Some(size_i64(body.len() as u64)),
        body: Some(StreamingBlob::from(s3s::Body::from(body))),
        content_range,
        e_tag: etag(&object.info),
        last_modified: last_modified(&object.info),
        accept_ranges: Some(ACCEPT_RANGES_BYTES.to_owned()),
        ..Default::default()
    };
    let mut response = S3Response::new(output);
    if ranged {
        response.status = Some(http::StatusCode::PARTIAL_CONTENT);
    }
    Ok(response)
}

pub(crate) async fn head_object(bound: &BoundBucket, req: S3Request<HeadObjectInput>) -> S3Result<S3Response<HeadObjectOutput>> {
    require(bound, EngineObjectOperation::HeadObject, "HeadObject")?;
    let input = req.input;
    refuse_version_id(input.version_id.as_deref())?;
    if input.part_number.is_some() {
        return Err(not_served(bound, "HeadObject with partNumber"));
    }
    let info = bound
        .engine
        .head_object(&bound.context, &input.key)
        .await
        .map_err(object_error)?;
    let (content_length, content_range) = match &input.range {
        None => (info.size, None),
        Some(range) => {
            let (span, content_range) = resolve_range(range, info.size)?;
            (span.end - span.start, Some(content_range))
        }
    };
    let ranged = content_range.is_some();
    let output = HeadObjectOutput {
        content_length: Some(size_i64(content_length)),
        content_range,
        e_tag: etag(&info),
        last_modified: last_modified(&info),
        accept_ranges: Some(ACCEPT_RANGES_BYTES.to_owned()),
        ..Default::default()
    };
    let mut response = S3Response::new(output);
    if ranged {
        response.status = Some(http::StatusCode::PARTIAL_CONTENT);
    }
    Ok(response)
}

async fn read_body(body: Option<StreamingBlob>, content_length: u64) -> S3Result<Bytes> {
    let Some(mut body) = body else {
        return Ok(Bytes::new());
    };
    let mut buffer = BytesMut::with_capacity(content_length as usize);
    while let Some(chunk) = body.next().await {
        let chunk = chunk.map_err(|e| s3_error!(IncompleteBody, "failed to read request body: {}", e))?;
        if (buffer.len() + chunk.len()) as u64 > content_length {
            return Err(s3_error!(IncompleteBody, "request body is longer than Content-Length"));
        }
        buffer.extend_from_slice(&chunk);
    }
    if buffer.len() as u64 != content_length {
        return Err(s3_error!(IncompleteBody, "request body is shorter than Content-Length"));
    }
    Ok(buffer.freeze())
}

pub(crate) async fn put_object(bound: &BoundBucket, req: S3Request<PutObjectInput>) -> S3Result<S3Response<PutObjectOutput>> {
    require(bound, EngineObjectOperation::PutObject, "PutObject")?;
    let headers = req.headers;
    let input = req.input;
    let content_length = input
        .content_length
        .and_then(|length| u64::try_from(length).ok())
        .ok_or_else(|| s3_error!(MissingContentLength, "engine buckets need a Content-Length"))?;
    // A bodiless PUT with a transfer descriptor: the owner pulls the value
    // from the client's buffer, whose length is the object size.
    if content_length == 0
        && let Some(transfer) = rdma_transfer(bound, &headers)
    {
        let size = rdma::buffer_len(&transfer).ok_or_else(|| s3_error!(InvalidArgument, "invalid RDMA buffer"))?;
        if size > MAX_INLINE_OBJECT_BYTES {
            return Err(s3_error!(
                EntityTooLarge,
                "engine buckets accept objects up to {} bytes",
                MAX_INLINE_OBJECT_BYTES
            ));
        }
        let meta = ObjectWriteMeta {
            content_length: size,
            rdma: Some(transfer),
        };
        let info = bound
            .engine
            .put_object(&bound.context, &input.key, Bytes::new(), meta)
            .await
            .map_err(object_error)?;
        let mut response = S3Response::new(PutObjectOutput {
            e_tag: etag(&info),
            size: Some(size_i64(info.size)),
            ..Default::default()
        });
        rdma_reply_headers(&mut response.headers, size, false);
        return Ok(response);
    }
    if content_length > MAX_INLINE_OBJECT_BYTES {
        return Err(s3_error!(
            EntityTooLarge,
            "engine buckets accept objects up to {} bytes",
            MAX_INLINE_OBJECT_BYTES
        ));
    }
    let body = read_body(input.body, content_length).await?;
    let meta = ObjectWriteMeta {
        content_length,
        rdma: None,
    };
    let info = bound
        .engine
        .put_object(&bound.context, &input.key, body, meta)
        .await
        .map_err(object_error)?;
    Ok(S3Response::new(PutObjectOutput {
        e_tag: etag(&info),
        size: Some(size_i64(info.size)),
        ..Default::default()
    }))
}

pub(crate) async fn delete_object(
    bound: &BoundBucket,
    req: S3Request<DeleteObjectInput>,
) -> S3Result<S3Response<DeleteObjectOutput>> {
    require(bound, EngineObjectOperation::DeleteObject, "DeleteObject")?;
    refuse_version_id(req.input.version_id.as_deref())?;
    match bound.engine.delete_object(&bound.context, &req.input.key).await {
        // S3 deletes are idempotent: a missing key is a successful delete.
        Ok(()) | Err(EngineError::NotFound) => Ok(S3Response::new(DeleteObjectOutput::default())),
        Err(err) => Err(object_error(err)),
    }
}

fn max_keys(requested: Option<i32>) -> usize {
    requested.map_or(DEFAULT_MAX_KEYS, |keys| usize::try_from(keys).unwrap_or(0).min(DEFAULT_MAX_KEYS))
}

async fn list_page(bound: &BoundBucket, query: &ListQuery<'_>) -> S3Result<ListPage> {
    require(bound, EngineObjectOperation::ListObjects, "ListObjects")?;
    if query.max_keys == 0 {
        return Ok(ListPage::default());
    }
    bound.engine.list_objects(&bound.context, query).await.map_err(object_error)
}

/// S3 returns `CommonPrefixes` only for a request with a delimiter.
fn list_common_prefixes(delimiter: Option<&str>, page: &ListPage) -> Option<Vec<CommonPrefix>> {
    delimiter.filter(|delimiter| !delimiter.is_empty()).map(|_| {
        page.common_prefixes
            .iter()
            .map(|prefix| CommonPrefix {
                prefix: Some(prefix.clone()),
            })
            .collect()
    })
}

fn list_contents(page: &ListPage) -> Vec<Object> {
    page.objects
        .iter()
        .map(|info| Object {
            key: Some(info.key.clone()),
            size: Some(size_i64(info.size)),
            e_tag: etag(info),
            last_modified: last_modified(info),
            ..Default::default()
        })
        .collect()
}

pub(crate) async fn list_objects_v2(
    bound: &BoundBucket,
    req: S3Request<ListObjectsV2Input>,
) -> S3Result<S3Response<ListObjectsV2Output>> {
    let input = req.input;
    let max_keys = max_keys(input.max_keys);
    let query = ListQuery {
        prefix: input.prefix.as_deref().unwrap_or_default(),
        delimiter: input.delimiter.as_deref(),
        // The continuation token is the last name (key or common prefix) of the previous page.
        start_after: input.continuation_token.as_deref().or(input.start_after.as_deref()),
        max_keys,
    };
    let page = list_page(bound, &query).await?;
    let contents = list_contents(&page);
    let common_prefixes = list_common_prefixes(input.delimiter.as_deref(), &page);
    let key_count = contents.len() + page.common_prefixes.len();
    Ok(S3Response::new(ListObjectsV2Output {
        name: Some(input.bucket),
        prefix: input.prefix,
        delimiter: input.delimiter,
        common_prefixes,
        max_keys: Some(i32::try_from(max_keys).unwrap_or(i32::MAX)),
        key_count: Some(i32::try_from(key_count).unwrap_or(i32::MAX)),
        continuation_token: input.continuation_token,
        start_after: input.start_after,
        is_truncated: Some(page.next_start_after.is_some()),
        next_continuation_token: page.next_start_after,
        contents: Some(contents),
        ..Default::default()
    }))
}

pub(crate) async fn list_objects(
    bound: &BoundBucket,
    req: S3Request<ListObjectsInput>,
) -> S3Result<S3Response<ListObjectsOutput>> {
    let input = req.input;
    let max_keys = max_keys(input.max_keys);
    let query = ListQuery {
        prefix: input.prefix.as_deref().unwrap_or_default(),
        delimiter: input.delimiter.as_deref(),
        start_after: input.marker.as_deref(),
        max_keys,
    };
    let page = list_page(bound, &query).await?;
    let contents = list_contents(&page);
    let common_prefixes = list_common_prefixes(input.delimiter.as_deref(), &page);
    Ok(S3Response::new(ListObjectsOutput {
        name: Some(input.bucket),
        prefix: input.prefix,
        delimiter: input.delimiter,
        common_prefixes,
        marker: input.marker,
        max_keys: Some(i32::try_from(max_keys).unwrap_or(i32::MAX)),
        is_truncated: Some(page.next_start_after.is_some()),
        next_marker: page.next_start_after,
        contents: Some(contents),
        ..Default::default()
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bucket_engine::memory::MemoryEngine;
    use http::{HeaderMap, Method, Uri};
    use rustfs_bucket_engine::{
        BucketContext, BucketEngine, BucketEngineBinding, EngineCapabilities, EngineDescriptor, EngineName,
    };

    fn request<T>(input: T, method: Method) -> S3Request<T> {
        S3Request {
            input,
            method,
            uri: Uri::from_static("/"),
            headers: HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    async fn bound() -> BoundBucket {
        let engine = Arc::new(MemoryEngine::new());
        let context = BucketContext {
            bucket: "kv".to_owned(),
            incarnation: 7,
            binding: BucketEngineBinding {
                engine: EngineName::new("memory").expect("valid engine name"),
                config_version: 1,
                config: serde_json::Value::Null,
            },
        };
        engine.attach(&context).await.expect("attach");
        BoundBucket { context, engine }
    }

    async fn put(bound: &BoundBucket, key: &str, body: &'static [u8]) -> PutObjectOutput {
        let input = PutObjectInput::builder()
            .bucket("kv".to_owned())
            .key(key.to_owned())
            .content_length(Some(body.len() as i64))
            .body(Some(StreamingBlob::from(s3s::Body::from(Bytes::from_static(body)))))
            .build()
            .expect("put input");
        put_object(bound, request(input, Method::PUT)).await.expect("put").output
    }

    async fn get(bound: &BoundBucket, key: &str, range: Option<Range>) -> S3Result<S3Response<GetObjectOutput>> {
        let input = GetObjectInput::builder()
            .bucket("kv".to_owned())
            .key(key.to_owned())
            .range(range)
            .build()
            .expect("get input");
        get_object(bound, request(input, Method::GET)).await
    }

    async fn body_bytes(output: GetObjectOutput) -> Vec<u8> {
        let mut body = output.body.expect("body");
        let mut bytes = Vec::new();
        while let Some(chunk) = body.next().await {
            bytes.extend_from_slice(&chunk.expect("chunk"));
        }
        bytes
    }

    fn code<T>(result: S3Result<T>) -> S3ErrorCode {
        result.err().expect("expected an error").code().clone()
    }

    #[tokio::test]
    async fn put_get_head_and_delete_round_trip_through_the_engine() {
        let bound = bound().await;
        let put_output = put(&bound, "page-1", b"kv page bytes").await;
        assert!(put_output.e_tag.is_some());

        let response = get(&bound, "page-1", None).await.expect("get");
        assert_eq!(response.status, None);
        assert_eq!(response.output.content_length, Some(13));
        assert_eq!(response.output.e_tag, put_output.e_tag);
        assert_eq!(body_bytes(response.output).await, b"kv page bytes");

        let head = HeadObjectInput::builder()
            .bucket("kv".to_owned())
            .key("page-1".to_owned())
            .build()
            .expect("head input");
        let head = head_object(&bound, request(head, Method::HEAD)).await.expect("head").output;
        assert_eq!(head.content_length, Some(13));

        let delete = DeleteObjectInput::builder()
            .bucket("kv".to_owned())
            .key("page-1".to_owned())
            .build()
            .expect("delete input");
        delete_object(&bound, request(delete.clone(), Method::DELETE))
            .await
            .expect("delete");
        // Deleting a missing key succeeds, as in S3.
        delete_object(&bound, request(delete, Method::DELETE))
            .await
            .expect("idempotent delete");
        assert_eq!(code(get(&bound, "page-1", None).await), S3ErrorCode::NoSuchKey);
    }

    #[tokio::test]
    async fn ranges_are_served_as_partial_content() {
        let bound = bound().await;
        put(&bound, "page", b"0123456789").await;

        let response = get(&bound, "page", Some(Range::Int { first: 2, last: Some(4) }))
            .await
            .expect("range");
        assert_eq!(response.status, Some(http::StatusCode::PARTIAL_CONTENT));
        assert_eq!(response.output.content_range.as_deref(), Some("bytes 2-4/10"));
        assert_eq!(body_bytes(response.output).await, b"234");

        let response = get(&bound, "page", Some(Range::Suffix { length: 3 })).await.expect("suffix");
        assert_eq!(body_bytes(response.output).await, b"789");

        let response = get(&bound, "page", Some(Range::Int { first: 8, last: None }))
            .await
            .expect("open range");
        assert_eq!(response.output.content_range.as_deref(), Some("bytes 8-9/10"));

        assert_eq!(
            code(get(&bound, "page", Some(Range::Int { first: 10, last: None })).await),
            S3ErrorCode::InvalidRange
        );
    }

    #[tokio::test]
    async fn listing_pages_through_a_prefix() {
        let bound = bound().await;
        for key in ["a/1", "a/2", "a/3", "b/1"] {
            put(&bound, key, b"x").await;
        }
        let page = |token: Option<String>| {
            ListObjectsV2Input::builder()
                .bucket("kv".to_owned())
                .prefix(Some("a/".to_owned()))
                .max_keys(Some(2))
                .continuation_token(token)
                .build()
                .expect("list input")
        };
        let first = list_objects_v2(&bound, request(page(None), Method::GET))
            .await
            .expect("list")
            .output;
        let keys: Vec<_> = first.contents.iter().flatten().filter_map(|o| o.key.clone()).collect();
        assert_eq!(keys, ["a/1", "a/2"]);
        assert_eq!(first.is_truncated, Some(true));

        let second = list_objects_v2(&bound, request(page(first.next_continuation_token), Method::GET))
            .await
            .expect("list")
            .output;
        let keys: Vec<_> = second.contents.iter().flatten().filter_map(|o| o.key.clone()).collect();
        assert_eq!(keys, ["a/3"]);
        assert_eq!(second.is_truncated, Some(false));

        // `ls s3://kv/`: folders are listed as common prefixes, and the
        // continuation token can be a common prefix.
        let delimited = |token: Option<String>| {
            ListObjectsV2Input::builder()
                .bucket("kv".to_owned())
                .delimiter(Some("/".to_owned()))
                .max_keys(Some(1))
                .continuation_token(token)
                .build()
                .expect("list input")
        };
        let first = list_objects_v2(&bound, request(delimited(None), Method::GET))
            .await
            .expect("list")
            .output;
        let prefixes: Vec<_> = first
            .common_prefixes
            .iter()
            .flatten()
            .filter_map(|p| p.prefix.clone())
            .collect();
        assert_eq!(prefixes, ["a/"]);
        assert_eq!(first.contents.iter().flatten().count(), 0);
        assert_eq!((first.key_count, first.is_truncated), (Some(1), Some(true)));
        assert_eq!(first.delimiter.as_deref(), Some("/"));
        let second = list_objects_v2(&bound, request(delimited(first.next_continuation_token), Method::GET))
            .await
            .expect("list")
            .output;
        let prefixes: Vec<_> = second
            .common_prefixes
            .iter()
            .flatten()
            .filter_map(|p| p.prefix.clone())
            .collect();
        assert_eq!(prefixes, ["b/"]);
        assert_eq!(second.is_truncated, Some(false));

        // ListObjects (v1) with a delimiter.
        let v1 = ListObjectsInput::builder()
            .bucket("kv".to_owned())
            .prefix(Some("a/".to_owned()))
            .delimiter(Some("/".to_owned()))
            .build()
            .expect("list input");
        let listed = list_objects(&bound, request(v1, Method::GET)).await.expect("list v1").output;
        let keys: Vec<_> = listed.contents.iter().flatten().filter_map(|o| o.key.clone()).collect();
        assert_eq!(keys, ["a/1", "a/2", "a/3"]);
        assert_eq!(listed.common_prefixes.map(|prefixes| prefixes.len()), Some(0));
    }

    #[tokio::test]
    async fn versions_parts_and_bad_lengths_are_refused() {
        let bound = bound().await;
        put(&bound, "page", b"x").await;

        let versioned = GetObjectInput::builder()
            .bucket("kv".to_owned())
            .key("page".to_owned())
            .version_id(Some("3f1c".to_owned()))
            .build()
            .expect("get input");
        assert_eq!(
            code(get_object(&bound, request(versioned, Method::GET)).await),
            S3ErrorCode::InvalidArgument
        );

        let part = GetObjectInput::builder()
            .bucket("kv".to_owned())
            .key("page".to_owned())
            .part_number(Some(1))
            .build()
            .expect("get input");
        assert_eq!(code(get_object(&bound, request(part, Method::GET)).await), S3ErrorCode::NotImplemented);

        let short = PutObjectInput::builder()
            .bucket("kv".to_owned())
            .key("short".to_owned())
            .content_length(Some(10))
            .body(Some(StreamingBlob::from(s3s::Body::from(Bytes::from_static(b"abc")))))
            .build()
            .expect("put input");
        assert_eq!(code(put_object(&bound, request(short, Method::PUT)).await), S3ErrorCode::IncompleteBody);

        let huge = PutObjectInput::builder()
            .bucket("kv".to_owned())
            .key("huge".to_owned())
            .content_length(Some(MAX_INLINE_OBJECT_BYTES as i64 + 1))
            .build()
            .expect("put input");
        assert_eq!(code(put_object(&bound, request(huge, Method::PUT)).await), S3ErrorCode::EntityTooLarge);
    }

    struct ReadOnlyEngine(EngineDescriptor);

    #[async_trait::async_trait]
    impl BucketEngine for ReadOnlyEngine {
        fn descriptor(&self) -> &EngineDescriptor {
            &self.0
        }
        fn validate_config(&self, _binding: &BucketEngineBinding) -> rustfs_bucket_engine::EngineResult<()> {
            Ok(())
        }
        async fn bind(&self, _bucket: &BucketContext) -> rustfs_bucket_engine::EngineResult<()> {
            Ok(())
        }
        async fn attach(&self, _bucket: &BucketContext) -> rustfs_bucket_engine::EngineResult<()> {
            Ok(())
        }
        async fn reconfigure(
            &self,
            _bucket: &BucketContext,
            _previous: &BucketEngineBinding,
        ) -> rustfs_bucket_engine::EngineResult<()> {
            Ok(())
        }
        async fn holds_data(&self, _bucket: &BucketContext) -> rustfs_bucket_engine::EngineResult<bool> {
            Ok(false)
        }
        async fn purge(&self, _bucket: &BucketContext) -> rustfs_bucket_engine::EngineResult<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn undeclared_operations_are_refused_with_not_implemented() {
        let descriptor = EngineDescriptor {
            name: EngineName::new("read-only").expect("valid engine name"),
            version: "test".to_owned(),
            config_version: 1,
            capabilities: EngineCapabilities {
                object_operations: [EngineObjectOperation::HeadObject].into_iter().collect(),
                ..EngineCapabilities::default()
            },
        };
        let bound = BoundBucket {
            context: BucketContext {
                bucket: "kv".to_owned(),
                incarnation: 7,
                binding: BucketEngineBinding {
                    engine: descriptor.name.clone(),
                    config_version: 1,
                    config: serde_json::Value::Null,
                },
            },
            engine: Arc::new(ReadOnlyEngine(descriptor)),
        };
        let input = PutObjectInput::builder()
            .bucket("kv".to_owned())
            .key("page".to_owned())
            .content_length(Some(1))
            .build()
            .expect("put input");
        let err = put_object(&bound, request(input, Method::PUT)).await.expect_err("refused");
        assert_eq!(*err.code(), S3ErrorCode::NotImplemented);
        assert!(err.message().unwrap_or_default().contains("engine read-only"), "{err:?}");
    }
}
