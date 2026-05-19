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
use rustfs_config::{DEFAULT_INTERNODE_DATA_TRANSPORT, ENV_RUSTFS_INTERNODE_DATA_TRANSPORT};
use rustfs_rio::{HttpReader, HttpWriter};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tracing::warn;

static INTERNODE_DATA_TRANSPORT: OnceLock<Arc<dyn InternodeDataTransport>> = OnceLock::new();

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
        DEFAULT_INTERNODE_DATA_TRANSPORT
    }
}

fn build_internode_data_transport(configured_transport: Option<&str>) -> Arc<dyn InternodeDataTransport> {
    match configured_transport.map(str::trim).filter(|transport| !transport.is_empty()) {
        Some(transport) if transport.eq_ignore_ascii_case(DEFAULT_INTERNODE_DATA_TRANSPORT) => {
            Arc::new(TcpHttpInternodeDataTransport)
        }
        Some(transport) => {
            warn!(
                env = ENV_RUSTFS_INTERNODE_DATA_TRANSPORT,
                requested = %transport,
                fallback = DEFAULT_INTERNODE_DATA_TRANSPORT,
                "unknown internode data transport, using default backend"
            );
            Arc::new(TcpHttpInternodeDataTransport)
        }
        None => Arc::new(TcpHttpInternodeDataTransport),
    }
}

pub fn build_internode_data_transport_from_env() -> Arc<dyn InternodeDataTransport> {
    Arc::clone(
        INTERNODE_DATA_TRANSPORT
            .get_or_init(|| build_internode_data_transport(std::env::var(ENV_RUSTFS_INTERNODE_DATA_TRANSPORT).ok().as_deref())),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_config_defaults_to_tcp_http() {
        let transport = build_internode_data_transport(None);

        assert_eq!(transport.name(), DEFAULT_INTERNODE_DATA_TRANSPORT);
    }

    #[test]
    fn transport_config_accepts_tcp_http() {
        let transport = build_internode_data_transport(Some("TCP-HTTP"));

        assert_eq!(transport.name(), DEFAULT_INTERNODE_DATA_TRANSPORT);
    }

    #[test]
    fn transport_config_falls_back_to_tcp_http_for_unknown_backend() {
        let transport = build_internode_data_transport(Some("rdma"));

        assert_eq!(transport.name(), DEFAULT_INTERNODE_DATA_TRANSPORT);
    }
}
