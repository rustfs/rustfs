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

use crate::cluster::rpc::{build_auth_headers, verify_ns_scanner_capability};
use crate::disk::error::{Error, Result};
use crate::disk::{FileReader, FileWriter};
use crate::storage_api_contracts::internode::{
    NS_SCANNER_BODY_SHA256_QUERY, NS_SCANNER_CAPABILITY_CHALLENGE_QUERY, NS_SCANNER_CYCLE_QUERY, NS_SCANNER_LEADER_EPOCH_QUERY,
    NS_SCANNER_PROTOCOL_VERSION, NS_SCANNER_PROTOCOL_VERSION_QUERY, NS_SCANNER_REQUEST_ID_QUERY, NS_SCANNER_SERVER_EPOCH_QUERY,
    NS_SCANNER_SESSION_ID_QUERY, NS_SCANNER_SESSION_SEQUENCE_QUERY, NsScannerCapabilityResponse, WALK_DIR_BODY_SHA256_QUERY,
    WALK_DIR_STREAM_COMPLETION_QUERY, WALK_DIR_STREAM_COMPLETION_V1,
};
use async_trait::async_trait;
use http::{HeaderMap, HeaderValue, Method, header::CONTENT_TYPE};
use rustfs_config::{
    DEFAULT_INTERNODE_DATA_TRANSPORT, ENV_RUSTFS_INTERNODE_DATA_TRANSPORT, INTERNODE_DATA_TRANSPORT_TCP,
    KNOWN_INTERNODE_DATA_TRANSPORT_BACKENDS,
};
use rustfs_rio::{HttpReader, HttpWriter};
use sha2::{Digest, Sha256};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::io::AsyncReadExt;
use uuid::Uuid;

static INTERNODE_DATA_TRANSPORT: OnceLock<std::result::Result<Arc<dyn InternodeDataTransport>, String>> = OnceLock::new();

const READ_FILE_STREAM_PATH: &str = "/rustfs/rpc/read_file_stream";
const PUT_FILE_STREAM_PATH: &str = "/rustfs/rpc/put_file_stream";
const WALK_DIR_PATH: &str = "/rustfs/rpc/walk_dir";
const NS_SCANNER_PATH: &str = "/rustfs/rpc/ns_scanner";
const NS_SCANNER_MAX_CAPABILITY_RESPONSE_SIZE: usize = 1024;
const CONTENT_TYPE_JSON: &str = "application/json";
const CONTENT_TYPE_MSGPACK: &str = "application/msgpack";

fn unsupported_transport_message(transport: &str) -> String {
    format!(
        "invalid {ENV_RUSTFS_INTERNODE_DATA_TRANSPORT}={transport:?}; supported values: {}",
        KNOWN_INTERNODE_DATA_TRANSPORT_BACKENDS.join(", ")
    )
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct InternodeDataTransportCapabilities {
    /// Backend can open a streaming remote disk reader.
    pub streaming_read: bool,
    /// Backend can open a streaming remote disk writer.
    pub streaming_write: bool,
    /// Backend can stream walk-dir responses.
    pub streaming_walk_dir: bool,
    /// Backend preserves in-order delivery for each opened transfer.
    pub ordered_delivery: bool,
    /// Largest payload the backend accepts for one transfer, or no RustFS-level cap.
    pub max_transfer_size: Option<usize>,
    /// Backend can participate in the behavior-preserving TCP fallback path.
    pub fallback_supported: bool,
}

impl InternodeDataTransportCapabilities {
    pub const fn tcp_http() -> Self {
        Self {
            streaming_read: true,
            streaming_write: true,
            streaming_walk_dir: true,
            ordered_delivery: true,
            max_transfer_size: None,
            fallback_supported: true,
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
    pub stall_timeout: Option<Duration>,
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

#[derive(Debug, Clone)]
pub struct NsScannerStreamRequest {
    pub endpoint: String,
    pub disk: String,
    pub request_id: Uuid,
    pub server_epoch: Uuid,
    pub session_id: Uuid,
    pub session_sequence: u64,
    pub next_cycle: u64,
    pub leader_epoch: u64,
    pub body: Vec<u8>,
    pub stall_timeout: Option<Duration>,
}

#[derive(Debug, Clone)]
pub struct NsScannerCapabilityRequest {
    pub endpoint: String,
}

/// Data-plane stream opener used by `RemoteDisk`.
///
/// This boundary is limited to remote disk streams that can move large payloads.
/// Internode metadata, lock, health, and administrative calls remain on the
/// existing gRPC control plane.
///
/// Buffer ownership, backend selection, and fallback expectations are documented
/// in `crates/ecstore/docs/internode-transport/`.
#[async_trait]
pub trait InternodeDataTransport: Send + Sync + std::fmt::Debug {
    async fn open_read(&self, request: ReadStreamRequest) -> Result<FileReader>;
    async fn open_write(&self, request: WriteStreamRequest) -> Result<FileWriter>;
    async fn open_walk_dir(&self, request: WalkDirStreamRequest) -> Result<FileReader>;
    async fn open_ns_scanner(&self, _request: NsScannerStreamRequest) -> Result<FileReader> {
        Err(Error::MethodNotAllowed)
    }
    async fn probe_ns_scanner(&self, _request: NsScannerCapabilityRequest) -> Result<Uuid> {
        Err(Error::MethodNotAllowed)
    }
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
        Ok(Box::new(
            HttpReader::new_with_stall_timeout(url, Method::GET, headers, None, request.stall_timeout).await?,
        ))
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

    async fn open_ns_scanner(&self, request: NsScannerStreamRequest) -> Result<FileReader> {
        let url = build_ns_scanner_url(&request);
        let mut headers = msgpack_headers();
        build_auth_headers(&url, &Method::POST, &mut headers)?;
        Ok(Box::new(
            HttpReader::new_with_stall_timeout(url, Method::POST, headers, Some(request.body), request.stall_timeout).await?,
        ))
    }

    async fn probe_ns_scanner(&self, request: NsScannerCapabilityRequest) -> Result<Uuid> {
        let challenge = Uuid::new_v4();
        let url = build_ns_scanner_capability_url(&request, challenge);
        let mut headers = msgpack_headers();
        build_auth_headers(&url, &Method::GET, &mut headers)?;
        let reader = HttpReader::new(url, Method::GET, headers, None).await?;
        let mut body = Vec::new();
        reader
            .take(u64::try_from(NS_SCANNER_MAX_CAPABILITY_RESPONSE_SIZE + 1).unwrap_or(u64::MAX))
            .read_to_end(&mut body)
            .await?;
        if body.is_empty() || body.len() > NS_SCANNER_MAX_CAPABILITY_RESPONSE_SIZE {
            return Err(Error::other("invalid remote namespace scanner capability response size"));
        }
        let response: NsScannerCapabilityResponse =
            rmp_serde::from_slice(&body).map_err(|_| Error::other("invalid remote namespace scanner capability response"))?;
        if response.version != NS_SCANNER_PROTOCOL_VERSION || response.server_epoch.is_nil() {
            return Err(Error::other("incompatible remote namespace scanner capability response"));
        }
        verify_ns_scanner_capability(challenge, response.server_epoch, &response.proof)
            .map_err(|err| Error::other(format!("remote namespace scanner capability authentication failed: {err}")))?;
        Ok(response.server_epoch)
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
    let body_sha256 = hex_simd::encode_to_string(Sha256::digest(&request.body), hex_simd::AsciiCase::Lower);
    format!(
        "{}{}?disk={}&{}={}&{}={}",
        request.endpoint,
        WALK_DIR_PATH,
        urlencoding::encode(&request.disk),
        WALK_DIR_STREAM_COMPLETION_QUERY,
        WALK_DIR_STREAM_COMPLETION_V1,
        WALK_DIR_BODY_SHA256_QUERY,
        body_sha256
    )
}

fn build_ns_scanner_url(request: &NsScannerStreamRequest) -> String {
    let body_sha256 = hex_simd::encode_to_string(Sha256::digest(&request.body), hex_simd::AsciiCase::Lower);
    format!(
        "{}{}?disk={}&{}={}&{}={}&{}={}&{}={}&{}={}&{}={}&{}={}",
        request.endpoint,
        NS_SCANNER_PATH,
        urlencoding::encode(&request.disk),
        NS_SCANNER_REQUEST_ID_QUERY,
        request.request_id,
        NS_SCANNER_SERVER_EPOCH_QUERY,
        request.server_epoch,
        NS_SCANNER_SESSION_ID_QUERY,
        request.session_id,
        NS_SCANNER_SESSION_SEQUENCE_QUERY,
        request.session_sequence,
        NS_SCANNER_CYCLE_QUERY,
        request.next_cycle,
        NS_SCANNER_LEADER_EPOCH_QUERY,
        request.leader_epoch,
        NS_SCANNER_BODY_SHA256_QUERY,
        body_sha256
    )
}

fn build_ns_scanner_capability_url(request: &NsScannerCapabilityRequest, challenge: Uuid) -> String {
    format!(
        "{}{}?{}={}&{}={}",
        request.endpoint,
        NS_SCANNER_PATH,
        NS_SCANNER_PROTOCOL_VERSION_QUERY,
        NS_SCANNER_PROTOCOL_VERSION,
        NS_SCANNER_CAPABILITY_CHALLENGE_QUERY,
        challenge
    )
}

fn json_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static(CONTENT_TYPE_JSON));
    headers
}

fn msgpack_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static(CONTENT_TYPE_MSGPACK));
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

    #[derive(Debug)]
    struct LegacyTestTransport;

    #[async_trait::async_trait]
    impl InternodeDataTransport for LegacyTestTransport {
        async fn open_read(&self, _request: ReadStreamRequest) -> Result<FileReader> {
            Ok(Box::new(tokio::io::empty()))
        }

        async fn open_write(&self, _request: WriteStreamRequest) -> Result<FileWriter> {
            Ok(Box::new(tokio::io::sink()))
        }

        async fn open_walk_dir(&self, _request: WalkDirStreamRequest) -> Result<FileReader> {
            Ok(Box::new(tokio::io::empty()))
        }

        fn name(&self) -> &'static str {
            "legacy-test"
        }

        fn capabilities(&self) -> InternodeDataTransportCapabilities {
            InternodeDataTransportCapabilities::tcp_http()
        }
    }

    #[tokio::test]
    async fn legacy_transport_defaults_namespace_scanner_to_unsupported() {
        let transport = LegacyTestTransport;

        let probe_err = transport
            .probe_ns_scanner(NsScannerCapabilityRequest {
                endpoint: "http://node1:9000".to_string(),
            })
            .await
            .expect_err("legacy transport should report namespace scanner as unsupported");
        assert!(matches!(probe_err, Error::MethodNotAllowed));

        let open_result = transport
            .open_ns_scanner(NsScannerStreamRequest {
                endpoint: "http://node1:9000".to_string(),
                disk: "http://node1:9000/data/rustfs0".to_string(),
                request_id: Uuid::new_v4(),
                server_epoch: Uuid::new_v4(),
                session_id: Uuid::new_v4(),
                session_sequence: 0,
                next_cycle: 7,
                leader_epoch: 9,
                body: Vec::new(),
                stall_timeout: None,
            })
            .await;
        let open_err = match open_result {
            Ok(_) => panic!("legacy transport should not open namespace scanner streams"),
            Err(err) => err,
        };
        assert!(matches!(open_err, Error::MethodNotAllowed));
    }

    #[test]
    fn tcp_http_capabilities_are_behavior_preserving() {
        let transport = TcpHttpInternodeDataTransport;

        assert_eq!(transport.name(), DEFAULT_INTERNODE_DATA_TRANSPORT);
        assert_eq!(
            transport.capabilities(),
            InternodeDataTransportCapabilities {
                streaming_read: true,
                streaming_write: true,
                streaming_walk_dir: true,
                ordered_delivery: true,
                max_transfer_size: None,
                fallback_supported: true,
            }
        );
    }

    #[test]
    fn tcp_http_capabilities_are_conservative() {
        let capabilities = TcpHttpInternodeDataTransport.capabilities();

        assert!(capabilities.ordered_delivery);
        assert_eq!(capabilities.max_transfer_size, None);
        assert!(capabilities.fallback_supported);
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
            stall_timeout: None,
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
            concat!(
                "http://node1:9000/rustfs/rpc/walk_dir?disk=http%3A%2F%2Fnode1%3A9000%2Fdata%2Frustfs0",
                "&walk_dir_stream_completion=error-v1",
                "&walk_dir_body_sha256=e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
            )
        );
    }

    #[test]
    fn ns_scanner_url_binds_body_and_encodes_disk_ref() {
        let request_id = Uuid::parse_str("11111111-2222-4333-8444-555555555555").expect("request ID");
        let server_epoch = Uuid::parse_str("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee").expect("server epoch");
        let session_id = Uuid::parse_str("99999999-8888-4777-8666-555555555555").expect("session ID");
        let url = build_ns_scanner_url(&NsScannerStreamRequest {
            endpoint: "http://node1:9000".to_string(),
            disk: "http://node1:9000/data/rustfs0".to_string(),
            request_id,
            server_epoch,
            session_id,
            session_sequence: 3,
            next_cycle: 7,
            leader_epoch: 9,
            body: b"scanner-request".to_vec(),
            stall_timeout: None,
        });

        assert_eq!(
            url,
            concat!(
                "http://node1:9000/rustfs/rpc/ns_scanner?disk=http%3A%2F%2Fnode1%3A9000%2Fdata%2Frustfs0",
                "&ns_scanner_request_id=11111111-2222-4333-8444-555555555555",
                "&ns_scanner_server_epoch=aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
                "&ns_scanner_session_id=99999999-8888-4777-8666-555555555555",
                "&ns_scanner_session_sequence=3",
                "&ns_scanner_cycle=7",
                "&ns_scanner_leader_epoch=9",
                "&ns_scanner_body_sha256=c958f15ca28422275c1245399f4c44eaba628ca453fcd77d6b3d4484573e4387"
            )
        );
    }

    #[test]
    fn ns_scanner_capability_url_binds_version_and_challenge() {
        let challenge = Uuid::parse_str("12345678-1234-4234-8234-123456789abc").expect("challenge");
        let url = build_ns_scanner_capability_url(
            &NsScannerCapabilityRequest {
                endpoint: "http://node1:9000".to_string(),
            },
            challenge,
        );

        assert_eq!(
            url,
            format!(
                "http://node1:9000/rustfs/rpc/ns_scanner?ns_scanner_protocol={NS_SCANNER_PROTOCOL_VERSION}&ns_scanner_challenge={challenge}"
            )
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
    fn transport_config_known_backends_are_current_oss_values() {
        assert_eq!(
            KNOWN_INTERNODE_DATA_TRANSPORT_BACKENDS,
            &[DEFAULT_INTERNODE_DATA_TRANSPORT, INTERNODE_DATA_TRANSPORT_TCP]
        );

        for configured in KNOWN_INTERNODE_DATA_TRANSPORT_BACKENDS {
            let transport = build_internode_data_transport(Some(configured)).unwrap();

            assert_eq!(transport.name(), DEFAULT_INTERNODE_DATA_TRANSPORT);
        }
    }

    #[test]
    fn transport_config_rejects_unknown_backend() {
        let err = build_internode_data_transport(Some("unsupported-backend")).expect_err("unknown backend should fail closed");

        assert!(err.to_string().contains(ENV_RUSTFS_INTERNODE_DATA_TRANSPORT));
        assert!(err.to_string().contains("unsupported-backend"));
        assert!(err.to_string().contains("supported values: tcp-http, tcp"));
    }

    #[test]
    fn cached_transport_config_error_uses_raw_message() {
        let err =
            build_internode_data_transport_result(Some("unsupported-backend")).expect_err("unknown backend should fail closed");

        assert!(!err.starts_with("io error "));
        assert!(err.contains(ENV_RUSTFS_INTERNODE_DATA_TRANSPORT));
        assert!(err.contains("unsupported-backend"));
    }
}
