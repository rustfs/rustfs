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

use crate::disk::error::{Error, Result};
use crate::disk::{FileReader, FileWriter};
use crate::rpc::build_auth_headers;
use async_trait::async_trait;
use http::{HeaderMap, HeaderValue, Method, header::CONTENT_TYPE};
use rustfs_config::{
    DEFAULT_INTERNODE_DATA_TRANSPORT, ENV_RUSTFS_INTERNODE_DATA_TRANSPORT, INTERNODE_DATA_TRANSPORT_TCP,
    KNOWN_INTERNODE_DATA_TRANSPORT_BACKENDS,
};
use rustfs_rio::{HttpReader, HttpWriter};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

static INTERNODE_DATA_TRANSPORT: OnceLock<std::result::Result<Arc<dyn InternodeDataTransport>, String>> = OnceLock::new();

const READ_FILE_STREAM_PATH: &str = "/rustfs/rpc/read_file_stream";
const PUT_FILE_STREAM_PATH: &str = "/rustfs/rpc/put_file_stream";
const WALK_DIR_PATH: &str = "/rustfs/rpc/walk_dir";
const CONTENT_TYPE_JSON: &str = "application/json";

fn unsupported_transport_message(transport: &str) -> String {
    format!(
        "invalid {ENV_RUSTFS_INTERNODE_DATA_TRANSPORT}={transport:?}; supported values: {}",
        KNOWN_INTERNODE_DATA_TRANSPORT_BACKENDS.join(", ")
    )
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct InternodeDataTransportCapabilities {
    pub stream_read: bool,
    pub stream_write: bool,
    pub walk_dir: bool,
    pub registered_memory: bool,
    pub scatter_gather: bool,
    pub zero_copy_receive: bool,
}

impl InternodeDataTransportCapabilities {
    pub const fn tcp_http() -> Self {
        Self {
            stream_read: true,
            stream_write: true,
            walk_dir: true,
            registered_memory: false,
            scatter_gather: false,
            zero_copy_receive: false,
        }
    }
}

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
    fn capabilities(&self) -> InternodeDataTransportCapabilities;
}

#[derive(Debug, Default)]
pub struct TcpHttpInternodeDataTransport;

#[async_trait]
impl InternodeDataTransport for TcpHttpInternodeDataTransport {
    async fn open_read(&self, request: ReadStreamRequest) -> Result<FileReader> {
        let url = build_read_file_stream_url(&request);
        let mut headers = json_headers();
        build_auth_headers(&url, &Method::GET, &mut headers)?;
        Ok(Box::new(HttpReader::new(url, Method::GET, headers, None).await?))
    }

    async fn open_write(&self, request: WriteStreamRequest) -> Result<FileWriter> {
        let url = build_put_file_stream_url(&request);
        let mut headers = json_headers();
        build_auth_headers(&url, &Method::PUT, &mut headers)?;
        Ok(Box::new(HttpWriter::new(url, Method::PUT, headers).await?))
    }

    async fn open_walk_dir(&self, request: WalkDirStreamRequest) -> Result<FileReader> {
        let url = build_walk_dir_url(&request);
        let mut headers = json_headers();
        build_auth_headers(&url, &Method::GET, &mut headers)?;
        Ok(Box::new(
            HttpReader::new_with_stall_timeout(url, Method::GET, headers, Some(request.body), request.stall_timeout).await?,
        ))
    }

    fn name(&self) -> &'static str {
        DEFAULT_INTERNODE_DATA_TRANSPORT
    }

    fn capabilities(&self) -> InternodeDataTransportCapabilities {
        InternodeDataTransportCapabilities::tcp_http()
    }
}

fn build_read_file_stream_url(request: &ReadStreamRequest) -> String {
    format!(
        "{}{}?disk={}&volume={}&path={}&offset={}&length={}",
        request.endpoint,
        READ_FILE_STREAM_PATH,
        urlencoding::encode(&request.disk),
        urlencoding::encode(&request.volume),
        urlencoding::encode(&request.path),
        request.offset,
        request.length
    )
}

fn build_put_file_stream_url(request: &WriteStreamRequest) -> String {
    format!(
        "{}{}?disk={}&volume={}&path={}&append={}&size={}",
        request.endpoint,
        PUT_FILE_STREAM_PATH,
        urlencoding::encode(&request.disk),
        urlencoding::encode(&request.volume),
        urlencoding::encode(&request.path),
        request.append,
        request.size
    )
}

fn build_walk_dir_url(request: &WalkDirStreamRequest) -> String {
    format!("{}{}?disk={}", request.endpoint, WALK_DIR_PATH, urlencoding::encode(&request.disk))
}

fn json_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static(CONTENT_TYPE_JSON));
    headers
}

fn build_internode_data_transport_result(
    configured_transport: Option<&str>,
) -> std::result::Result<Arc<dyn InternodeDataTransport>, String> {
    match configured_transport.map(str::trim).filter(|transport| !transport.is_empty()) {
        None => Ok(Arc::new(TcpHttpInternodeDataTransport)),
        Some(transport)
            if transport.eq_ignore_ascii_case(DEFAULT_INTERNODE_DATA_TRANSPORT)
                || transport.eq_ignore_ascii_case(INTERNODE_DATA_TRANSPORT_TCP) =>
        {
            Ok(Arc::new(TcpHttpInternodeDataTransport))
        }
        Some(transport) => Err(unsupported_transport_message(transport)),
    }
}

pub fn build_internode_data_transport(configured_transport: Option<&str>) -> Result<Arc<dyn InternodeDataTransport>> {
    build_internode_data_transport_result(configured_transport).map_err(Error::other)
}

pub fn build_internode_data_transport_from_env() -> Result<Arc<dyn InternodeDataTransport>> {
    let configured_transport = std::env::var(ENV_RUSTFS_INTERNODE_DATA_TRANSPORT).ok();
    #[cfg(test)]
    {
        build_internode_data_transport(configured_transport.as_deref())
    }

    #[cfg(not(test))]
    INTERNODE_DATA_TRANSPORT
        .get_or_init(|| build_internode_data_transport_result(configured_transport.as_deref()))
        .as_ref()
        .map(Arc::clone)
        .map_err(|err| Error::other(err.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tcp_http_capabilities_are_behavior_preserving() {
        let transport = TcpHttpInternodeDataTransport;

        assert_eq!(transport.name(), DEFAULT_INTERNODE_DATA_TRANSPORT);
        assert_eq!(
            transport.capabilities(),
            InternodeDataTransportCapabilities {
                stream_read: true,
                stream_write: true,
                walk_dir: true,
                registered_memory: false,
                scatter_gather: false,
                zero_copy_receive: false,
            }
        );
    }

    #[test]
    fn read_file_stream_url_encodes_query_values() {
        let url = build_read_file_stream_url(&ReadStreamRequest {
            endpoint: "http://node1:9000".to_string(),
            disk: "http://node1:9000/data/rustfs0".to_string(),
            volume: ".rustfs.sys".to_string(),
            path: "pool.bin/../part.1".to_string(),
            offset: 7,
            length: 11,
        });

        assert_eq!(
            url,
            "http://node1:9000/rustfs/rpc/read_file_stream?disk=http%3A%2F%2Fnode1%3A9000%2Fdata%2Frustfs0&volume=.rustfs.sys&path=pool.bin%2F..%2Fpart.1&offset=7&length=11"
        );
    }

    #[test]
    fn put_file_stream_url_encodes_query_values() {
        let url = build_put_file_stream_url(&WriteStreamRequest {
            endpoint: "http://node1:9000".to_string(),
            disk: "http://node1:9000/data/rustfs0".to_string(),
            volume: "bucket".to_string(),
            path: "object/part.1".to_string(),
            append: false,
            size: 4096,
        });

        assert_eq!(
            url,
            "http://node1:9000/rustfs/rpc/put_file_stream?disk=http%3A%2F%2Fnode1%3A9000%2Fdata%2Frustfs0&volume=bucket&path=object%2Fpart.1&append=false&size=4096"
        );
    }

    #[test]
    fn walk_dir_url_encodes_disk_ref() {
        let url = build_walk_dir_url(&WalkDirStreamRequest {
            endpoint: "http://node1:9000".to_string(),
            disk: "http://node1:9000/data/rustfs0".to_string(),
            body: Vec::new(),
            stall_timeout: None,
        });

        assert_eq!(
            url,
            "http://node1:9000/rustfs/rpc/walk_dir?disk=http%3A%2F%2Fnode1%3A9000%2Fdata%2Frustfs0"
        );
    }

    #[test]
    fn transport_config_defaults_to_tcp_http() {
        let transport = build_internode_data_transport(None).unwrap();

        assert_eq!(transport.name(), DEFAULT_INTERNODE_DATA_TRANSPORT);
    }

    #[test]
    fn transport_config_blank_value_falls_back_to_default() {
        let transport = build_internode_data_transport(Some("   ")).unwrap();

        assert_eq!(transport.name(), DEFAULT_INTERNODE_DATA_TRANSPORT);
    }

    #[test]
    fn transport_config_accepts_tcp_aliases() {
        for configured in [
            DEFAULT_INTERNODE_DATA_TRANSPORT,
            INTERNODE_DATA_TRANSPORT_TCP,
            "TCP-HTTP",
            "TCP",
        ] {
            let transport = build_internode_data_transport(Some(configured)).unwrap();

            assert_eq!(transport.name(), DEFAULT_INTERNODE_DATA_TRANSPORT);
        }
    }

    #[test]
    fn transport_config_rejects_unknown_backend() {
        let err = build_internode_data_transport(Some("rdma")).expect_err("unknown backend should fail closed");

        assert!(err.to_string().contains(ENV_RUSTFS_INTERNODE_DATA_TRANSPORT));
        assert!(err.to_string().contains("rdma"));
    }

    #[test]
    fn cached_transport_config_error_uses_raw_message() {
        let err = build_internode_data_transport_result(Some("rdma")).expect_err("unknown backend should fail closed");

        assert!(!err.starts_with("io error "));
        assert!(err.contains(ENV_RUSTFS_INTERNODE_DATA_TRANSPORT));
        assert!(err.contains("rdma"));
    }
}
