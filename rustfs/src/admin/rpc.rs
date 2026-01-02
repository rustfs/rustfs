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

use super::router::AdminOperation;
use super::router::Operation;
use super::router::S3Router;
use crate::server::RPC_PREFIX;
use futures::StreamExt;
use http::StatusCode;
use hyper::Method;
use matchit::Params;
use rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE;
use rustfs_ecstore::disk::DiskAPI;
use rustfs_ecstore::disk::WalkDirOptions;
use rustfs_ecstore::set_disk::DEFAULT_READ_BUFFER_SIZE;
use rustfs_ecstore::store::find_local_disk;
use rustfs_utils::net::bytes_stream;
use s3s::Body;
use s3s::S3Request;
use s3s::S3Response;
use s3s::S3Result;
use s3s::dto::StreamingBlob;
use s3s::s3_error;
use serde_urlencoded::from_bytes;
use tokio::io::AsyncWriteExt;
use tokio_util::io::ReaderStream;
use tracing::warn;

pub fn register_rpc_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::GET,
        format!("{}{}", RPC_PREFIX, "/read_file_stream").as_str(),
        AdminOperation(&ReadFile {}),
    )?;

    r.insert(
        Method::HEAD,
        format!("{}{}", RPC_PREFIX, "/read_file_stream").as_str(),
        AdminOperation(&ReadFile {}),
    )?;

    r.insert(
        Method::PUT,
        format!("{}{}", RPC_PREFIX, "/put_file_stream").as_str(),
        AdminOperation(&PutFile {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", RPC_PREFIX, "/walk_dir").as_str(),
        AdminOperation(&WalkDir {}),
    )?;

    r.insert(
        Method::HEAD,
        format!("{}{}", RPC_PREFIX, "/walk_dir").as_str(),
        AdminOperation(&WalkDir {}),
    )?;

    Ok(())
}

// /rustfs/rpc/read_file_stream?disk={}&volume={}&path={}&offset={}&length={}"
#[derive(Debug, Default, serde::Deserialize)]
pub struct ReadFileQuery {
    disk: String,
    volume: String,
    path: String,
    offset: usize,
    length: usize,
}
pub struct ReadFile {}
#[async_trait::async_trait]
impl Operation for ReadFile {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        if req.method == Method::HEAD {
            return Ok(S3Response::new((StatusCode::OK, Body::empty())));
        }
        let query = {
            if let Some(query) = req.uri.query() {
                let input: ReadFileQuery =
                    from_bytes(query.as_bytes()).map_err(|e| s3_error!(InvalidArgument, "get query failed1 {:?}", e))?;
                input
            } else {
                ReadFileQuery::default()
            }
        };

        let Some(disk) = find_local_disk(&query.disk).await else {
            return Err(s3_error!(InvalidArgument, "disk not found"));
        };

        let file = disk
            .read_file_stream(&query.volume, &query.path, query.offset, query.length)
            .await
            .map_err(|e| s3_error!(InternalError, "read file err {}", e))?;

        Ok(S3Response::new((
            StatusCode::OK,
            Body::from(StreamingBlob::wrap(bytes_stream(
                ReaderStream::with_capacity(file, DEFAULT_READ_BUFFER_SIZE),
                query.length,
            ))),
        )))
    }
}

#[derive(Debug, Default, serde::Deserialize)]
pub struct WalkDirQuery {
    disk: String,
}

pub struct WalkDir {}

#[async_trait::async_trait]
impl Operation for WalkDir {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        if req.method == Method::HEAD {
            return Ok(S3Response::new((StatusCode::OK, Body::empty())));
        }

        let query = {
            if let Some(query) = req.uri.query() {
                let input: WalkDirQuery =
                    from_bytes(query.as_bytes()).map_err(|e| s3_error!(InvalidArgument, "get query failed1 {:?}", e))?;
                input
            } else {
                WalkDirQuery::default()
            }
        };

        let mut input = req.input;
        let body = match input.store_all_limited(MAX_ADMIN_REQUEST_BODY_SIZE).await {
            Ok(b) => b,
            Err(e) => {
                warn!("get body failed, e: {:?}", e);
                return Err(s3_error!(InvalidRequest, "RPC request body too large or failed to read"));
            }
        };

        // let body_bytes = decrypt_data(input_cred.secret_key.expose().as_bytes(), &body)
        //     .map_err(|e| S3Error::with_message(S3ErrorCode::InvalidArgument, format!("decrypt_data err {}", e)))?;

        let args: WalkDirOptions =
            serde_json::from_slice(&body).map_err(|e| s3_error!(InternalError, "unmarshal body err {}", e))?;
        let Some(disk) = find_local_disk(&query.disk).await else {
            return Err(s3_error!(InvalidArgument, "disk not found"));
        };

        let (rd, mut wd) = tokio::io::duplex(DEFAULT_READ_BUFFER_SIZE);

        tokio::spawn(async move {
            if let Err(e) = disk.walk_dir(args, &mut wd).await {
                warn!("walk dir err {}", e);
            }
        });

        let body = Body::from(StreamingBlob::wrap(ReaderStream::with_capacity(rd, DEFAULT_READ_BUFFER_SIZE)));
        Ok(S3Response::new((StatusCode::OK, body)))
    }
}

// /rustfs/rpc/read_file_stream?disk={}&volume={}&path={}&offset={}&length={}"
#[derive(Debug, Default, serde::Deserialize)]
pub struct PutFileQuery {
    disk: String,
    volume: String,
    path: String,
    append: bool,
    size: i64,
}
pub struct PutFile {}
#[async_trait::async_trait]
impl Operation for PutFile {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let query = {
            if let Some(query) = req.uri.query() {
                let input: PutFileQuery =
                    from_bytes(query.as_bytes()).map_err(|e| s3_error!(InvalidArgument, "get query failed1 {:?}", e))?;
                input
            } else {
                PutFileQuery::default()
            }
        };

        let Some(disk) = find_local_disk(&query.disk).await else {
            return Err(s3_error!(InvalidArgument, "disk not found"));
        };

        let mut file = if query.append {
            disk.append_file(&query.volume, &query.path)
                .await
                .map_err(|e| s3_error!(InternalError, "append file err {}", e))?
        } else {
            disk.create_file("", &query.volume, &query.path, query.size)
                .await
                .map_err(|e| s3_error!(InternalError, "read file err {}", e))?
        };

        let mut body = req.input;
        while let Some(item) = body.next().await {
            let bytes = item.map_err(|e| s3_error!(InternalError, "body stream err {}", e))?;
            let result = file.write_all(&bytes).await;
            result.map_err(|e| s3_error!(InternalError, "write file err {}", e))?;
        }

        Ok(S3Response::new((StatusCode::OK, Body::empty())))
    }
}
