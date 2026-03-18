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

use crate::server::RPC_PREFIX;
use bytes::Bytes;
use futures_util::TryStreamExt;
use http::{HeaderMap, Method, Request, Response, StatusCode, Uri};
use http_body_util::{BodyExt, Limited};
use hyper::body::Incoming;
use rustfs_common::internode_metrics::global_internode_metrics;
use rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE;
use rustfs_ecstore::disk::{DiskAPI, WalkDirOptions};
use rustfs_ecstore::rpc::verify_rpc_signature;
use rustfs_ecstore::set_disk::DEFAULT_READ_BUFFER_SIZE;
use rustfs_ecstore::store::find_local_disk_by_ref;
use rustfs_utils::net::bytes_stream;
use s3s::Body;
use s3s::dto::StreamingBlob;
use serde::de::DeserializeOwned;
use serde_urlencoded::from_bytes;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{self, AsyncWriteExt};
use tokio_util::io::{ReaderStream, StreamReader};
use tower::Service;
use tracing::warn;

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;
type RpcErrorResponse = Box<Response<Body>>;
const READ_FILE_STREAM_PATH: &str = "/rustfs/rpc/read_file_stream";
const PUT_FILE_STREAM_PATH: &str = "/rustfs/rpc/put_file_stream";
const WALK_DIR_PATH: &str = "/rustfs/rpc/walk_dir";

#[derive(Clone)]
pub struct InternodeRpcService<S> {
    inner: S,
}

impl<S> InternodeRpcService<S> {
    pub fn new(inner: S) -> Self {
        Self { inner }
    }
}

#[derive(Debug, Default, serde::Deserialize)]
struct ReadFileQuery {
    disk: String,
    volume: String,
    path: String,
    offset: usize,
    length: usize,
}

#[derive(Debug, Default, serde::Deserialize)]
struct WalkDirQuery {
    disk: String,
}

#[derive(Debug, Default, serde::Deserialize)]
struct PutFileQuery {
    disk: String,
    volume: String,
    path: String,
    append: bool,
    size: i64,
}

impl<S> Service<Request<Incoming>> for InternodeRpcService<S>
where
    S: Service<Request<Incoming>, Response = Response<Body>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxError> + Send + 'static,
{
    type Response = Response<Body>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<Incoming>) -> Self::Future {
        if !is_internode_rpc_path(req.uri().path()) {
            let mut inner = self.inner.clone();
            return Box::pin(async move { inner.call(req).await });
        }

        Box::pin(async move { Ok(handle_internode_rpc(req).await) })
    }
}

fn is_internode_rpc_path(path: &str) -> bool {
    path.starts_with(RPC_PREFIX)
}

async fn handle_internode_rpc(req: Request<Incoming>) -> Response<Body> {
    if let Err(response) = verify_internode_rpc_signature(req.uri(), req.method(), req.headers()) {
        global_internode_metrics().record_error();
        return *response;
    }

    let method = req.method().clone();
    let path = req.uri().path();

    let response = match (method, path) {
        (Method::GET, READ_FILE_STREAM_PATH) | (Method::HEAD, READ_FILE_STREAM_PATH) => handle_read_file(req).await,
        (Method::GET, WALK_DIR_PATH) | (Method::HEAD, WALK_DIR_PATH) => handle_walk_dir(req).await,
        (Method::PUT, PUT_FILE_STREAM_PATH) => handle_put_file(req).await,
        _ => response_with_status(StatusCode::NOT_FOUND, "internode rpc route not found"),
    };

    if !response.status().is_success() {
        global_internode_metrics().record_error();
    }

    response
}

fn verify_internode_rpc_signature(uri: &Uri, method: &Method, headers: &HeaderMap) -> Result<(), RpcErrorResponse> {
    if method == Method::HEAD {
        return Ok(());
    }

    verify_rpc_signature(&uri.to_string(), method, headers).map_err(|e| {
        Box::new(response_with_status(
            StatusCode::FORBIDDEN,
            format!("rpc signature verification failed: {e}"),
        ))
    })
}

async fn handle_read_file(req: Request<Incoming>) -> Response<Body> {
    if req.method() == Method::HEAD {
        return empty_ok();
    }

    let query = match parse_query::<ReadFileQuery>(&req) {
        Ok(query) => query,
        Err(response) => return *response,
    };

    let Some(disk) = find_local_disk_by_ref(&query.disk).await else {
        return response_with_status(StatusCode::BAD_REQUEST, "disk not found");
    };

    let file = match disk
        .read_file_stream(&query.volume, &query.path, query.offset, query.length)
        .await
    {
        Ok(file) => file,
        Err(e) => return response_with_status(StatusCode::INTERNAL_SERVER_ERROR, format!("read file err {e}")),
    };

    global_internode_metrics().record_incoming_request();
    let metrics = global_internode_metrics().clone();
    let stream = bytes_stream(
        ReaderStream::with_capacity(file, DEFAULT_READ_BUFFER_SIZE).map_ok(move |bytes| {
            metrics.record_sent_bytes(bytes.len());
            bytes
        }),
        query.length,
    );

    Response::builder()
        .status(StatusCode::OK)
        .body(Body::from(StreamingBlob::wrap(stream)))
        .expect("failed to build read file stream response")
}

async fn handle_walk_dir(req: Request<Incoming>) -> Response<Body> {
    if req.method() == Method::HEAD {
        return empty_ok();
    }

    let query = match parse_query::<WalkDirQuery>(&req) {
        Ok(query) => query,
        Err(response) => return *response,
    };

    let Some(disk) = find_local_disk_by_ref(&query.disk).await else {
        return response_with_status(StatusCode::BAD_REQUEST, "disk not found");
    };

    let body = match Limited::new(req.into_body(), MAX_ADMIN_REQUEST_BODY_SIZE).collect().await {
        Ok(body) => body.to_bytes(),
        Err(e) => return response_with_status(StatusCode::PAYLOAD_TOO_LARGE, format!("read body err {e}")),
    };

    let args: WalkDirOptions = match serde_json::from_slice(&body) {
        Ok(args) => args,
        Err(e) => return response_with_status(StatusCode::BAD_REQUEST, format!("unmarshal body err {e}")),
    };

    let (rd, mut wd) = tokio::io::duplex(DEFAULT_READ_BUFFER_SIZE);
    tokio::spawn(async move {
        if let Err(e) = disk.walk_dir(args, &mut wd).await {
            warn!("walk dir err {}", e);
        }
    });

    global_internode_metrics().record_incoming_request();
    let metrics = global_internode_metrics().clone();
    let stream = ReaderStream::with_capacity(rd, DEFAULT_READ_BUFFER_SIZE).map_ok(move |bytes| {
        metrics.record_sent_bytes(bytes.len());
        bytes
    });

    Response::builder()
        .status(StatusCode::OK)
        .body(Body::from(StreamingBlob::wrap(stream)))
        .expect("failed to build walk dir response")
}

async fn handle_put_file(req: Request<Incoming>) -> Response<Body> {
    let query = match parse_query::<PutFileQuery>(&req) {
        Ok(query) => query,
        Err(response) => return *response,
    };

    let Some(disk) = find_local_disk_by_ref(&query.disk).await else {
        return response_with_status(StatusCode::BAD_REQUEST, "disk not found");
    };

    let mut file = if query.append {
        match disk.append_file(&query.volume, &query.path).await {
            Ok(file) => file,
            Err(e) => return response_with_status(StatusCode::INTERNAL_SERVER_ERROR, format!("append file err {e}")),
        }
    } else {
        match disk.create_file("", &query.volume, &query.path, query.size).await {
            Ok(file) => file,
            Err(e) => return response_with_status(StatusCode::INTERNAL_SERVER_ERROR, format!("create file err {e}")),
        }
    };

    let copied = match copy_body_to_writer(req.into_body().into_data_stream(), &mut file).await {
        Ok(copied) => copied,
        Err(e) => return response_with_status(StatusCode::INTERNAL_SERVER_ERROR, format!("write file err {e}")),
    };

    global_internode_metrics().record_incoming_request();
    global_internode_metrics().record_recv_bytes(copied as usize);

    if let Err(e) = file.flush().await {
        return response_with_status(StatusCode::INTERNAL_SERVER_ERROR, format!("write file err {e}"));
    }

    empty_ok()
}

async fn copy_body_to_writer<S, E, W>(body: S, writer: &mut W) -> io::Result<u64>
where
    S: futures::TryStream<Ok = Bytes, Error = E> + Unpin,
    E: Into<BoxError>,
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut reader = StreamReader::new(body.map_err(io::Error::other));
    tokio::io::copy(&mut reader, writer).await
}

fn parse_query<T>(req: &Request<Incoming>) -> Result<T, RpcErrorResponse>
where
    T: DeserializeOwned + Default,
{
    match req.uri().query() {
        Some(query) => from_bytes(query.as_bytes())
            .map_err(|e| Box::new(response_with_status(StatusCode::BAD_REQUEST, format!("get query failed {e}")))),
        None => Ok(T::default()),
    }
}

fn empty_ok() -> Response<Body> {
    Response::builder()
        .status(StatusCode::OK)
        .body(Body::empty())
        .expect("failed to build empty ok response")
}

fn response_with_status(status: StatusCode, message: impl Into<String>) -> Response<Body> {
    Response::builder()
        .status(status)
        .header(http::header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(Body::from(Bytes::from(message.into())))
        .expect("failed to build rpc error response")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;
    use tokio_stream::iter;

    #[test]
    fn internode_rpc_path_matches_rpc_prefix() {
        assert!(is_internode_rpc_path("/rustfs/rpc/read_file_stream"));
        assert!(is_internode_rpc_path("/rustfs/rpc/walk_dir"));
        assert!(!is_internode_rpc_path("/rustfs/admin/v3/info"));
    }

    #[test]
    fn rpc_head_signature_verification_is_skipped() {
        let uri: Uri = READ_FILE_STREAM_PATH.parse().expect("uri");
        let headers = HeaderMap::new();
        assert!(verify_internode_rpc_signature(&uri, &Method::HEAD, &headers).is_ok());
    }

    #[test]
    fn rpc_get_request_requires_signature() {
        let uri: Uri = READ_FILE_STREAM_PATH.parse().expect("uri");
        let headers = HeaderMap::new();
        let response = verify_internode_rpc_signature(&uri, &Method::GET, &headers).expect_err("response");
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn copy_body_to_writer_streams_all_chunks() {
        let (mut reader, mut writer) = tokio::io::duplex(64);
        let body = iter(vec![
            Ok::<Bytes, io::Error>(Bytes::from_static(b"hello ")),
            Ok(Bytes::from_static(b"world")),
        ]);

        let copied = copy_body_to_writer(body, &mut writer).await.expect("copy succeeds");
        drop(writer);

        let mut out = Vec::new();
        reader.read_to_end(&mut out).await.expect("read succeeds");

        assert_eq!(copied, 11);
        assert_eq!(out, b"hello world");
    }
}
