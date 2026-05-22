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
#[cfg(feature = "experimental-rdma-sim")]
use bytes::Bytes;
#[cfg(feature = "experimental-rdma-sim")]
use futures_util::{Stream, TryStreamExt};
use http::{HeaderMap, HeaderValue, Method, header::CONTENT_TYPE};
#[cfg(feature = "experimental-rdma-sim")]
use pin_project_lite::pin_project;
#[cfg(not(feature = "experimental-rdma-sim"))]
use rustfs_config::INTERNODE_DATA_TRANSPORT_RDMA_SIM_FEATURE;
use rustfs_config::{
    DEFAULT_INTERNODE_DATA_TRANSPORT, ENV_RUSTFS_INTERNODE_DATA_TRANSPORT, INTERNODE_DATA_TRANSPORT_RDMA_SIM,
    INTERNODE_DATA_TRANSPORT_TCP, KNOWN_INTERNODE_DATA_TRANSPORT_BACKENDS,
};
#[cfg(feature = "experimental-rdma-sim")]
use rustfs_io_metrics::internode_metrics::{
    INTERNODE_OPERATION_PUT_FILE_STREAM, INTERNODE_OPERATION_READ_FILE_STREAM, INTERNODE_OPERATION_WALK_DIR,
    INTERNODE_TRANSPORT_BACKEND_RDMA_SIM, global_internode_metrics,
};
use rustfs_rio::{HttpReader, HttpWriter};
#[cfg(feature = "experimental-rdma-sim")]
use std::collections::HashMap;
#[cfg(feature = "experimental-rdma-sim")]
use std::io;
#[cfg(feature = "experimental-rdma-sim")]
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
#[cfg(feature = "experimental-rdma-sim")]
use std::task::{Context, Poll};
use std::time::Duration;
#[cfg(feature = "experimental-rdma-sim")]
use tokio::io::{AsyncRead, ReadBuf};
#[cfg(feature = "experimental-rdma-sim")]
use tokio_util::io::StreamReader;

static INTERNODE_DATA_TRANSPORT: OnceLock<std::result::Result<Arc<dyn InternodeDataTransport>, String>> = OnceLock::new();

const READ_FILE_STREAM_PATH: &str = "/rustfs/rpc/read_file_stream";
const PUT_FILE_STREAM_PATH: &str = "/rustfs/rpc/put_file_stream";
const WALK_DIR_PATH: &str = "/rustfs/rpc/walk_dir";
const CONTENT_TYPE_JSON: &str = "application/json";

fn unsupported_transport_message(transport: &str) -> String {
    format!(
        "invalid {ENV_RUSTFS_INTERNODE_DATA_TRANSPORT}={transport:?}; supported values: {}",
        supported_transport_backends().join(", ")
    )
}

fn supported_transport_backends() -> Vec<&'static str> {
    #[cfg(feature = "experimental-rdma-sim")]
    {
        let mut backends = KNOWN_INTERNODE_DATA_TRANSPORT_BACKENDS.to_vec();
        backends.push(INTERNODE_DATA_TRANSPORT_RDMA_SIM);
        backends
    }

    #[cfg(not(feature = "experimental-rdma-sim"))]
    {
        KNOWN_INTERNODE_DATA_TRANSPORT_BACKENDS.to_vec()
    }
}

#[cfg(not(feature = "experimental-rdma-sim"))]
fn rdma_sim_feature_disabled_message() -> String {
    format!(
        "invalid {ENV_RUSTFS_INTERNODE_DATA_TRANSPORT}={INTERNODE_DATA_TRANSPORT_RDMA_SIM:?}; backend requires feature {INTERNODE_DATA_TRANSPORT_RDMA_SIM_FEATURE}"
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
    /// Backend has an API shape that could avoid an extra user-space copy.
    pub zero_copy_candidate: bool,
    /// Backend cannot transfer payloads unless buffers are registered or pinned.
    pub registered_memory_required: bool,
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
            zero_copy_candidate: false,
            registered_memory_required: false,
            ordered_delivery: true,
            max_transfer_size: None,
            fallback_supported: true,
        }
    }

    #[cfg(feature = "experimental-rdma-sim")]
    pub const fn rdma_sim() -> Self {
        Self {
            streaming_read: true,
            streaming_write: false,
            streaming_walk_dir: false,
            zero_copy_candidate: true,
            registered_memory_required: true,
            ordered_delivery: true,
            max_transfer_size: None,
            fallback_supported: false,
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

/// Data-plane stream opener used by `RemoteDisk`.
///
/// This boundary is limited to remote disk streams that can move large payloads.
/// Internode metadata, lock, health, and administrative calls remain on the
/// existing gRPC control plane.
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

#[cfg(feature = "experimental-rdma-sim")]
#[derive(Debug, Clone)]
pub struct RdmaSimInternodeDataTransport {
    client: reqwest::Client,
    fault: Option<RdmaSimFault>,
}

#[cfg(feature = "experimental-rdma-sim")]
impl Default for RdmaSimInternodeDataTransport {
    fn default() -> Self {
        Self {
            client: reqwest::Client::new(),
            fault: None,
        }
    }
}

#[cfg(all(test, feature = "experimental-rdma-sim"))]
impl RdmaSimInternodeDataTransport {
    fn with_fault(fault: RdmaSimFault) -> Self {
        Self {
            client: reqwest::Client::new(),
            fault: Some(fault),
        }
    }
}

#[cfg(feature = "experimental-rdma-sim")]
#[async_trait]
impl InternodeDataTransport for RdmaSimInternodeDataTransport {
    async fn open_read(&self, request: ReadStreamRequest) -> Result<FileReader> {
        let url = build_read_file_stream_url(&request);
        let mut headers = json_headers();
        build_auth_headers(&url, &Method::GET, &mut headers).map_err(|err| {
            record_rdma_sim_error(INTERNODE_OPERATION_READ_FILE_STREAM);
            Error::other(format!("rdma-sim read_file_stream auth failed: {err}"))
        })?;

        let response = self
            .client
            .request(Method::GET, url)
            .headers(headers)
            .send()
            .await
            .map_err(|err| {
                record_rdma_sim_error(INTERNODE_OPERATION_READ_FILE_STREAM);
                Error::other(format!("rdma-sim read_file_stream request failed: {err}"))
            })?;

        if !response.status().is_success() {
            record_rdma_sim_error(INTERNODE_OPERATION_READ_FILE_STREAM);
            return Err(Error::other(format!(
                "rdma-sim read_file_stream request failed with status {}",
                response.status()
            )));
        }

        global_internode_metrics().record_outgoing_request_for_operation_and_backend(
            INTERNODE_OPERATION_READ_FILE_STREAM,
            INTERNODE_TRANSPORT_BACKEND_RDMA_SIM,
        );

        let stream = response.bytes_stream().map_err(|err| {
            record_rdma_sim_error(INTERNODE_OPERATION_READ_FILE_STREAM);
            io::Error::other(format!("rdma-sim read_file_stream stream failed: {err}"))
        });

        Ok(Box::new(RdmaSimReader::new(StreamReader::new(Box::pin(stream)), self.fault)))
    }

    async fn open_write(&self, _request: WriteStreamRequest) -> Result<FileWriter> {
        record_rdma_sim_error(INTERNODE_OPERATION_PUT_FILE_STREAM);
        Err(Error::other(rdma_sim_unsupported_operation_message("put_file_stream")))
    }

    async fn open_walk_dir(&self, _request: WalkDirStreamRequest) -> Result<FileReader> {
        record_rdma_sim_error(INTERNODE_OPERATION_WALK_DIR);
        Err(Error::other(rdma_sim_unsupported_operation_message("walk_dir")))
    }

    fn name(&self) -> &'static str {
        INTERNODE_DATA_TRANSPORT_RDMA_SIM
    }

    fn capabilities(&self) -> InternodeDataTransportCapabilities {
        InternodeDataTransportCapabilities::rdma_sim()
    }
}

#[cfg(feature = "experimental-rdma-sim")]
fn rdma_sim_unsupported_operation_message(operation: &str) -> String {
    format!("rdma-sim internode transport only supports read_file_stream in this spike; {operation} is unsupported")
}

#[cfg(feature = "experimental-rdma-sim")]
fn record_rdma_sim_error(operation: &'static str) {
    global_internode_metrics().record_error_for_operation_and_backend(operation, INTERNODE_TRANSPORT_BACKEND_RDMA_SIM);
}

#[cfg(feature = "experimental-rdma-sim")]
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
struct SimulatedMemoryKey(u64);

#[cfg(feature = "experimental-rdma-sim")]
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct SimulatedMemoryRegion {
    len: usize,
}

#[cfg(feature = "experimental-rdma-sim")]
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct RegisteredBufferHandle {
    key: SimulatedMemoryKey,
    len: usize,
}

#[cfg(feature = "experimental-rdma-sim")]
#[derive(Debug, Default)]
struct SimulatedMemoryRegistry {
    next_key: u64,
    regions: HashMap<SimulatedMemoryKey, SimulatedMemoryRegion>,
}

#[cfg(feature = "experimental-rdma-sim")]
impl SimulatedMemoryRegistry {
    fn register(&mut self, len: usize) -> io::Result<RegisteredBufferHandle> {
        self.next_key = self
            .next_key
            .checked_add(1)
            .ok_or_else(|| io::Error::other("rdma-sim memory key space exhausted"))?;
        let key = SimulatedMemoryKey(self.next_key);
        self.regions.insert(key, SimulatedMemoryRegion { len });
        Ok(RegisteredBufferHandle { key, len })
    }

    fn validate(&self, handle: RegisteredBufferHandle) -> io::Result<()> {
        let Some(region) = self.regions.get(&handle.key) else {
            return Err(io::Error::other("rdma-sim memory region is not registered"));
        };
        if region.len < handle.len {
            return Err(io::Error::other("rdma-sim memory region length is smaller than requested transfer"));
        }
        Ok(())
    }

    fn unregister(&mut self, handle: RegisteredBufferHandle) -> io::Result<()> {
        self.regions
            .remove(&handle.key)
            .map(|_| ())
            .ok_or_else(|| io::Error::other("rdma-sim memory region is already unregistered"))
    }

    fn invalidate(&mut self, handle: RegisteredBufferHandle) {
        self.regions.remove(&handle.key);
    }
}

#[cfg(feature = "experimental-rdma-sim")]
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum SimulatedCompletionState {
    Idle,
    Submitted,
    Completed,
    Failed,
}

#[cfg(feature = "experimental-rdma-sim")]
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum RdmaSimFault {
    RegistrationFailed,
    InvalidMemoryKey,
    CompletionError,
    Timeout,
    PartialTransfer,
}

#[cfg(feature = "experimental-rdma-sim")]
pin_project! {
    struct RdmaSimReader {
        #[pin]
        inner: StreamReader<Pin<Box<dyn Stream<Item = io::Result<Bytes>> + Send + Sync>>, Bytes>,
        registry: SimulatedMemoryRegistry,
        pending: Option<RegisteredBufferHandle>,
        completion: SimulatedCompletionState,
        fault: Option<RdmaSimFault>,
        fault_fired: bool,
    }
}

#[cfg(feature = "experimental-rdma-sim")]
impl RdmaSimReader {
    fn new(
        inner: StreamReader<Pin<Box<dyn Stream<Item = io::Result<Bytes>> + Send + Sync>>, Bytes>,
        fault: Option<RdmaSimFault>,
    ) -> Self {
        Self {
            inner,
            registry: SimulatedMemoryRegistry::default(),
            pending: None,
            completion: SimulatedCompletionState::Idle,
            fault,
            fault_fired: false,
        }
    }
}

#[cfg(feature = "experimental-rdma-sim")]
impl AsyncRead for RdmaSimReader {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }

        let mut this = self.project();

        if matches!(*this.fault, Some(RdmaSimFault::Timeout)) && !*this.fault_fired {
            *this.fault_fired = true;
            *this.completion = SimulatedCompletionState::Failed;
            record_rdma_sim_error(INTERNODE_OPERATION_READ_FILE_STREAM);
            return Poll::Ready(Err(io::Error::new(io::ErrorKind::TimedOut, "rdma-sim completion timeout")));
        }

        if this.pending.is_none() {
            if matches!(*this.fault, Some(RdmaSimFault::RegistrationFailed)) && !*this.fault_fired {
                *this.fault_fired = true;
                *this.completion = SimulatedCompletionState::Failed;
                record_rdma_sim_error(INTERNODE_OPERATION_READ_FILE_STREAM);
                return Poll::Ready(Err(io::Error::other("rdma-sim memory registration failed")));
            }

            let handle = match this.registry.register(buf.remaining()) {
                Ok(handle) => handle,
                Err(err) => {
                    *this.completion = SimulatedCompletionState::Failed;
                    record_rdma_sim_error(INTERNODE_OPERATION_READ_FILE_STREAM);
                    return Poll::Ready(Err(err));
                }
            };
            *this.pending = Some(handle);
            *this.completion = SimulatedCompletionState::Submitted;

            if matches!(*this.fault, Some(RdmaSimFault::InvalidMemoryKey)) && !*this.fault_fired {
                *this.fault_fired = true;
                this.registry.invalidate(handle);
            }
        }

        let handle = this
            .pending
            .expect("rdma-sim reader must have a registered buffer before polling the inner stream");

        if let Err(err) = this.registry.validate(handle) {
            *this.pending = None;
            *this.completion = SimulatedCompletionState::Failed;
            record_rdma_sim_error(INTERNODE_OPERATION_READ_FILE_STREAM);
            return Poll::Ready(Err(err));
        }

        if matches!(*this.fault, Some(RdmaSimFault::CompletionError)) && !*this.fault_fired {
            *this.fault_fired = true;
            let _ = this.registry.unregister(handle);
            *this.pending = None;
            *this.completion = SimulatedCompletionState::Failed;
            record_rdma_sim_error(INTERNODE_OPERATION_READ_FILE_STREAM);
            return Poll::Ready(Err(io::Error::other("rdma-sim completion error")));
        }

        if matches!(*this.fault, Some(RdmaSimFault::PartialTransfer)) && !*this.fault_fired {
            *this.fault_fired = true;
            let _ = this.registry.unregister(handle);
            *this.pending = None;
            *this.completion = SimulatedCompletionState::Failed;
            record_rdma_sim_error(INTERNODE_OPERATION_READ_FILE_STREAM);
            return Poll::Ready(Err(io::Error::new(io::ErrorKind::UnexpectedEof, "rdma-sim partial transfer")));
        }

        let filled_before = buf.filled().len();

        match this.inner.as_mut().poll_read(cx, buf) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => {
                if let Err(err) = this.registry.unregister(handle) {
                    *this.pending = None;
                    *this.completion = SimulatedCompletionState::Failed;
                    record_rdma_sim_error(INTERNODE_OPERATION_READ_FILE_STREAM);
                    return Poll::Ready(Err(err));
                }

                *this.pending = None;
                let bytes_read = buf.filled().len().saturating_sub(filled_before);

                if bytes_read > 0 {
                    global_internode_metrics().record_recv_bytes_for_operation_and_backend(
                        INTERNODE_OPERATION_READ_FILE_STREAM,
                        INTERNODE_TRANSPORT_BACKEND_RDMA_SIM,
                        bytes_read,
                    );
                }

                *this.completion = SimulatedCompletionState::Completed;
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(err)) => {
                let _ = this.registry.unregister(handle);
                *this.pending = None;
                *this.completion = SimulatedCompletionState::Failed;
                record_rdma_sim_error(INTERNODE_OPERATION_READ_FILE_STREAM);
                Poll::Ready(Err(err))
            }
        }
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
        #[cfg(feature = "experimental-rdma-sim")]
        Some(transport) if transport.eq_ignore_ascii_case(INTERNODE_DATA_TRANSPORT_RDMA_SIM) => {
            Ok(Arc::new(RdmaSimInternodeDataTransport::default()))
        }
        #[cfg(not(feature = "experimental-rdma-sim"))]
        Some(transport) if transport.eq_ignore_ascii_case(INTERNODE_DATA_TRANSPORT_RDMA_SIM) => {
            Err(rdma_sim_feature_disabled_message())
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
                streaming_read: true,
                streaming_write: true,
                streaming_walk_dir: true,
                zero_copy_candidate: false,
                registered_memory_required: false,
                ordered_delivery: true,
                max_transfer_size: None,
                fallback_supported: true,
            }
        );
    }

    #[test]
    fn tcp_http_capabilities_do_not_advertise_rdma_specific_features() {
        let capabilities = TcpHttpInternodeDataTransport.capabilities();

        assert!(!capabilities.zero_copy_candidate);
        assert!(!capabilities.registered_memory_required);
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
    fn transport_config_rejects_real_rdma_backend_names() {
        for configured in ["rdma", "real-rdma"] {
            let err = build_internode_data_transport(Some(configured)).expect_err("real RDMA names should fail closed");

            assert!(err.to_string().contains(ENV_RUSTFS_INTERNODE_DATA_TRANSPORT));
            assert!(err.to_string().contains(configured));
        }
    }

    #[cfg(not(feature = "experimental-rdma-sim"))]
    #[test]
    fn transport_config_rejects_rdma_sim_without_feature() {
        let err = build_internode_data_transport(Some(INTERNODE_DATA_TRANSPORT_RDMA_SIM))
            .expect_err("rdma-sim should require the experimental feature");
        let err = err.to_string();

        assert!(err.contains(ENV_RUSTFS_INTERNODE_DATA_TRANSPORT));
        assert!(err.contains(INTERNODE_DATA_TRANSPORT_RDMA_SIM));
        assert!(err.contains(INTERNODE_DATA_TRANSPORT_RDMA_SIM_FEATURE));
    }

    #[cfg(feature = "experimental-rdma-sim")]
    #[test]
    fn transport_config_accepts_rdma_sim_with_feature() {
        let transport = build_internode_data_transport(Some(INTERNODE_DATA_TRANSPORT_RDMA_SIM)).unwrap();

        assert_eq!(transport.name(), INTERNODE_DATA_TRANSPORT_RDMA_SIM);
    }

    #[cfg(feature = "experimental-rdma-sim")]
    #[test]
    fn rdma_sim_capabilities_are_honest_for_the_spike() {
        let transport = RdmaSimInternodeDataTransport::default();

        assert_eq!(transport.name(), INTERNODE_DATA_TRANSPORT_RDMA_SIM);
        assert_eq!(
            transport.capabilities(),
            InternodeDataTransportCapabilities {
                streaming_read: true,
                streaming_write: false,
                streaming_walk_dir: false,
                zero_copy_candidate: true,
                registered_memory_required: true,
                ordered_delivery: true,
                max_transfer_size: None,
                fallback_supported: false,
            }
        );
    }

    #[cfg(feature = "experimental-rdma-sim")]
    #[test]
    fn rdma_sim_memory_registry_requires_registered_regions() {
        let mut registry = SimulatedMemoryRegistry::default();
        let handle = registry.register(16).expect("registration should succeed");

        registry.validate(handle).expect("registered region should validate");
        registry.unregister(handle).expect("registered region should unregister");

        let validate_err = registry
            .validate(handle)
            .expect_err("unregistered region should fail validation");
        assert!(validate_err.to_string().contains("not registered"));

        let unregister_err = registry.unregister(handle).expect_err("double unregister should fail");
        assert!(unregister_err.to_string().contains("already unregistered"));

        let invalid_handle = RegisteredBufferHandle {
            key: SimulatedMemoryKey(999),
            len: 4,
        };
        let invalid_err = registry.validate(invalid_handle).expect_err("invalid memory key should fail");
        assert!(invalid_err.to_string().contains("not registered"));
    }

    #[cfg(feature = "experimental-rdma-sim")]
    #[tokio::test]
    async fn rdma_sim_read_file_stream_happy_path_records_backend_metrics() {
        use tokio::io::AsyncReadExt as _;

        ensure_test_rpc_secret();
        let body = b"rdma-sim read bytes".to_vec();
        let endpoint = start_read_file_stream_server(body.clone()).await;
        let metrics = global_internode_metrics();
        let before = metrics.snapshot();
        let transport = RdmaSimInternodeDataTransport::default();

        let mut reader = transport
            .open_read(read_request_for_endpoint(endpoint))
            .await
            .expect("rdma-sim read should open");
        let mut actual = Vec::new();
        reader.read_to_end(&mut actual).await.expect("rdma-sim read should complete");

        assert_eq!(actual, body);
        let after = metrics.snapshot();
        assert!(after.outgoing_requests_total > before.outgoing_requests_total);
        assert!(after.recv_bytes_total >= before.recv_bytes_total + body.len() as u64);
    }

    #[cfg(feature = "experimental-rdma-sim")]
    #[tokio::test]
    async fn rdma_sim_completion_error_propagates() {
        use tokio::io::AsyncReadExt as _;

        ensure_test_rpc_secret();
        let endpoint = start_read_file_stream_server(b"completion".to_vec()).await;
        let transport = RdmaSimInternodeDataTransport::with_fault(RdmaSimFault::CompletionError);
        let mut reader = transport
            .open_read(read_request_for_endpoint(endpoint))
            .await
            .expect("rdma-sim read should open before completion failure");
        let mut actual = Vec::new();

        let err = reader
            .read_to_end(&mut actual)
            .await
            .expect_err("completion failure should propagate");

        assert!(err.to_string().contains("completion error"));
    }

    #[cfg(feature = "experimental-rdma-sim")]
    #[tokio::test]
    async fn rdma_sim_invalid_memory_key_fails_lifecycle_validation() {
        use tokio::io::AsyncReadExt as _;

        ensure_test_rpc_secret();
        let endpoint = start_read_file_stream_server(b"invalid-key".to_vec()).await;
        let transport = RdmaSimInternodeDataTransport::with_fault(RdmaSimFault::InvalidMemoryKey);
        let mut reader = transport
            .open_read(read_request_for_endpoint(endpoint))
            .await
            .expect("rdma-sim read should open before lifecycle failure");
        let mut actual = Vec::new();

        let err = reader
            .read_to_end(&mut actual)
            .await
            .expect_err("invalid memory key should propagate");

        assert!(err.to_string().contains("not registered"));
    }

    #[cfg(feature = "experimental-rdma-sim")]
    #[tokio::test]
    async fn rdma_sim_fault_injection_modes_propagate_errors() {
        use tokio::io::AsyncReadExt as _;

        ensure_test_rpc_secret();
        for (fault, expected) in [
            (RdmaSimFault::RegistrationFailed, "registration failed"),
            (RdmaSimFault::Timeout, "completion timeout"),
            (RdmaSimFault::PartialTransfer, "partial transfer"),
        ] {
            let endpoint = start_read_file_stream_server(b"fault".to_vec()).await;
            let transport = RdmaSimInternodeDataTransport::with_fault(fault);
            let mut reader = transport
                .open_read(read_request_for_endpoint(endpoint))
                .await
                .expect("rdma-sim read should open before injected failure");
            let mut actual = Vec::new();

            let err = reader
                .read_to_end(&mut actual)
                .await
                .expect_err("injected failure should propagate");

            assert!(err.to_string().contains(expected), "{err}");
        }
    }

    #[cfg(feature = "experimental-rdma-sim")]
    #[tokio::test]
    async fn rdma_sim_unsupported_operations_return_clear_errors() {
        let transport = RdmaSimInternodeDataTransport::default();

        let write_err = match transport
            .open_write(WriteStreamRequest {
                endpoint: "http://127.0.0.1:1".to_string(),
                disk: "disk".to_string(),
                volume: "bucket".to_string(),
                path: "object".to_string(),
                append: false,
                size: 1,
            })
            .await
        {
            Ok(_) => panic!("rdma-sim write should be unsupported"),
            Err(err) => err,
        };
        assert!(write_err.to_string().contains("rdma-sim"));
        assert!(write_err.to_string().contains("put_file_stream"));
        assert!(write_err.to_string().contains("unsupported"));

        let walk_err = match transport
            .open_walk_dir(WalkDirStreamRequest {
                endpoint: "http://127.0.0.1:1".to_string(),
                disk: "disk".to_string(),
                body: Vec::new(),
                stall_timeout: None,
            })
            .await
        {
            Ok(_) => panic!("rdma-sim walk-dir should be unsupported"),
            Err(err) => err,
        };
        assert!(walk_err.to_string().contains("rdma-sim"));
        assert!(walk_err.to_string().contains("walk_dir"));
        assert!(walk_err.to_string().contains("unsupported"));
    }

    #[test]
    fn cached_transport_config_error_uses_raw_message() {
        let err = build_internode_data_transport_result(Some("rdma")).expect_err("unknown backend should fail closed");

        assert!(!err.starts_with("io error "));
        assert!(err.contains(ENV_RUSTFS_INTERNODE_DATA_TRANSPORT));
        assert!(err.contains("rdma"));
    }

    #[cfg(feature = "experimental-rdma-sim")]
    fn ensure_test_rpc_secret() {
        let _ = rustfs_credentials::GLOBAL_RUSTFS_RPC_SECRET.set("test-rpc-secret".to_string());
    }

    #[cfg(feature = "experimental-rdma-sim")]
    fn read_request_for_endpoint(endpoint: String) -> ReadStreamRequest {
        ReadStreamRequest {
            endpoint,
            disk: "disk".to_string(),
            volume: "bucket".to_string(),
            path: "object/part.1".to_string(),
            offset: 0,
            length: 1024,
        }
    }

    #[cfg(feature = "experimental-rdma-sim")]
    async fn start_read_file_stream_server(body: Vec<u8>) -> String {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test server should bind a loopback port");
        let addr = listener.local_addr().expect("test server should expose its local address");

        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("test server should accept one request");
            let mut request = [0_u8; 4096];
            let _ = socket.read(&mut request).await.expect("test server should read request");
            let response_headers = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len());
            socket
                .write_all(response_headers.as_bytes())
                .await
                .expect("test server should write response headers");
            socket.write_all(&body).await.expect("test server should write response body");
        });

        format!("http://{addr}")
    }
}
