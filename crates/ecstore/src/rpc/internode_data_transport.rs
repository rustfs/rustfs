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

use crate::disk::error::Result;
use crate::disk::{FileReader, FileWriter};
use crate::rpc::build_auth_headers;
use async_trait::async_trait;
use http::{HeaderMap, HeaderValue, Method, header::CONTENT_TYPE};
use rustfs_rio::{HttpReader, HttpWriter};
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;

pub const ENV_RUSTFS_INTERNODE_DATA_TRANSPORT: &str = "RUSTFS_INTERNODE_DATA_TRANSPORT";
pub const INTERNODE_DATA_TRANSPORT_TCP_HTTP: &str = "tcp-http";

#[derive(Debug, Clone)]
pub struct ReadStreamRequest {
    pub endpoint: String,
    pub disk: String,
    pub volume: String,
    pub path: String,
    pub offset: usize,
    pub length: usize,
}

#[derive(Debug, Clone)]
pub struct WriteStreamRequest {
    pub endpoint: String,
    pub disk: String,
    pub volume: String,
    pub path: String,
    pub append: bool,
    pub size: i64,
}

#[derive(Debug, Clone)]
pub struct WalkDirStreamRequest {
    pub endpoint: String,
    pub disk: String,
    pub body: Vec<u8>,
    pub stall_timeout: Option<Duration>,
}

#[async_trait]
pub trait InternodeDataTransport: Send + Sync + std::fmt::Debug {
    async fn open_read(&self, request: ReadStreamRequest) -> Result<FileReader>;
    async fn open_write(&self, request: WriteStreamRequest) -> Result<FileWriter>;
    async fn open_walk_dir(&self, request: WalkDirStreamRequest) -> Result<FileReader>;
    fn name(&self) -> &'static str;
}

#[derive(Debug, Default)]
pub struct TcpHttpInternodeDataTransport;

#[async_trait]
impl InternodeDataTransport for TcpHttpInternodeDataTransport {
    async fn open_read(&self, request: ReadStreamRequest) -> Result<FileReader> {
        let url = format!(
            "{}/rustfs/rpc/read_file_stream?disk={}&volume={}&path={}&offset={}&length={}",
            request.endpoint,
            urlencoding::encode(&request.disk),
            urlencoding::encode(&request.volume),
            urlencoding::encode(&request.path),
            request.offset,
            request.length
        );
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        build_auth_headers(&url, &Method::GET, &mut headers)?;
        Ok(Box::new(HttpReader::new(url, Method::GET, headers, None).await?))
    }

    async fn open_write(&self, request: WriteStreamRequest) -> Result<FileWriter> {
        let url = format!(
            "{}/rustfs/rpc/put_file_stream?disk={}&volume={}&path={}&append={}&size={}",
            request.endpoint,
            urlencoding::encode(&request.disk),
            urlencoding::encode(&request.volume),
            urlencoding::encode(&request.path),
            request.append,
            request.size
        );
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        build_auth_headers(&url, &Method::PUT, &mut headers)?;
        Ok(Box::new(HttpWriter::new(url, Method::PUT, headers).await?))
    }

    async fn open_walk_dir(&self, request: WalkDirStreamRequest) -> Result<FileReader> {
        let url = format!("{}/rustfs/rpc/walk_dir?disk={}", request.endpoint, urlencoding::encode(&request.disk));
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        build_auth_headers(&url, &Method::GET, &mut headers)?;
        Ok(Box::new(
            HttpReader::new_with_stall_timeout(url, Method::GET, headers, Some(request.body), request.stall_timeout).await?,
        ))
    }

    fn name(&self) -> &'static str {
        INTERNODE_DATA_TRANSPORT_TCP_HTTP
    }
}

pub fn build_internode_data_transport_from_env() -> Arc<dyn InternodeDataTransport> {
    match std::env::var(ENV_RUSTFS_INTERNODE_DATA_TRANSPORT) {
        Ok(transport) => {
            if transport.eq_ignore_ascii_case(INTERNODE_DATA_TRANSPORT_TCP_HTTP) {
                Arc::new(TcpHttpInternodeDataTransport)
            } else {
                warn!(
                    env = ENV_RUSTFS_INTERNODE_DATA_TRANSPORT,
                    requested = %transport,
                    fallback = INTERNODE_DATA_TRANSPORT_TCP_HTTP,
                    "unknown internode data transport, using default backend"
                );
                Arc::new(TcpHttpInternodeDataTransport)
            }
        }
        Err(_) => Arc::new(TcpHttpInternodeDataTransport),
    }
}
