// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Authenticated, transport-neutral ports for optional internode stream providers.
//! RPC authentication and local storage semantics remain owned by RustFS.

pub use crate::storage::storage_api::ecstore_internode::{
    FileReader, FileWriter, InternodeDataTransport, InternodeDataTransportCapabilities, NsScannerCapabilityRequest,
    NsScannerCapabilityResponse, NsScannerStreamRequest, ReadStreamRequest, TcpHttpInternodeDataTransport, TransportError,
    WalkDirStreamRequest, WriteStreamRequest,
};
use crate::storage::storage_api::{InstanceContext, RemoteBackgroundIo, with_remote_background_io};
use crate::storage::storage_api::{check_and_record_signed_rpc_nonce, ecstore_internode};
use async_trait::async_trait;
use bytes::Bytes;
use http::{Request, StatusCode};
use http_body_util::{BodyExt, Limited};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    io,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::Semaphore;

/// Signed node-control route; the enclosing RPC service verifies its signature.
pub const CONTROL_PATH: &str = ecstore_internode::ExternalStreamAuth::PATH;
/// Includes the opaque provider offer and the storage operation binding.
pub const MAX_CONTROL_BYTES: usize = 2 * 1024 * 1024;

/// Capabilities issued only after signature, body digest and replay validation.
pub struct AuthenticatedStreams {
    request_digest: [u8; 32],
    key: [u8; 32],
    operation: Mutex<Option<StorageOperation>>,
}

struct StorageOperation {
    instance: Arc<InstanceContext>,
    background: Option<RemoteBackgroundIo>,
}

impl AuthenticatedStreams {
    /// Identity of the exact signed control body.
    pub fn request_digest(&self) -> [u8; 32] {
        self.request_digest
    }

    /// A purpose-separated key for response and data-channel authentication.
    pub fn transfer_key(&self) -> io::Result<[u8; 32]> {
        Ok(self.key)
    }

    async fn run<T, Fut>(&self, operation: impl FnOnce(Arc<InstanceContext>) -> Fut) -> Result<T, TransportError>
    where
        Fut: std::future::Future<Output = Result<T, TransportError>>,
    {
        // One control describes one local operation. Its worker inherits the receiver's admission.
        let storage = self
            .operation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .ok_or_else(|| TransportError::Io(io::Error::new(io::ErrorKind::InvalidInput, "control already used")))?;
        with_remote_background_io(storage.background, async {
            let result = operation(storage.instance).await;
            if result.is_err() {
                RemoteBackgroundIo::discard_current_progress();
            }
            result
        })
        .await
    }

    /// Opens a local reader with the authenticated disk scope and metadata compatibility rules.
    pub async fn read(&self, request: ReadStreamRequest) -> Result<FileReader, TransportError> {
        self.run(|instance| async move { super::http_service::read_external_stream(&instance, request).await })
            .await
    }

    /// Publishes owned chunks after the provider reports verified EOF. Cancellation,
    /// transport failure or an unverified end must surface as an I/O error.
    pub async fn write_chunks(
        &self,
        request: WriteStreamRequest,
        reader: rustfs_rio::ChunkReaderBox,
    ) -> Result<u64, TransportError> {
        self.run(|instance| async move { super::http_service::write_external_chunks(&instance, request, reader).await })
            .await
    }
}

/// An embedding binary supplies control framing and the native data protocol.
#[async_trait]
pub trait ExternalControlService: Send + Sync {
    /// Maximum authenticated inbound controls, including their detached workers.
    fn max_operations(&self) -> usize;
    /// The returned control stream owns no object payload. Retain `admission` in
    /// native workers until storage and checked cleanup finish, including disconnects.
    async fn open(
        &self,
        control: Bytes,
        storage: AuthenticatedStreams,
        admission: Arc<dyn Send + Sync>,
    ) -> io::Result<FileReader>;
    /// Stops admission and reports physical resource retention by the deadline.
    async fn shutdown(&self, deadline: Instant) -> io::Result<()>;
}

/// Resources and disk namespace belonging to one listener.
pub(crate) struct InstalledControlService {
    service: Arc<dyn ExternalControlService>,
    admission: Arc<Semaphore>,
    instance: Arc<InstanceContext>,
    auth: ecstore_internode::ExternalStreamAuth,
}

impl InstalledControlService {
    pub(crate) fn new(service: Arc<dyn ExternalControlService>, instance: Arc<InstanceContext>) -> io::Result<Self> {
        let limit = service.max_operations();
        if limit == 0 || limit > Semaphore::MAX_PERMITS {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid internode control capacity"));
        }
        Ok(Self {
            service,
            admission: Arc::new(Semaphore::new(limit)),
            auth: ecstore_internode::ExternalStreamAuth::new(&instance),
            instance,
        })
    }

    pub(crate) async fn shutdown(&self, deadline: Instant) -> io::Result<()> {
        self.admission.close();
        tokio::time::timeout_at(deadline.into(), self.service.shutdown(deadline))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "peer transport shutdown exceeded its deadline"))?
    }
}

/// Host capability for sending signed controls using the standard RPC credentials.
#[derive(Clone)]
pub struct PeerControlClient(ecstore_internode::ExternalStreamAuth);

impl PeerControlClient {
    pub(crate) fn new(instance: &InstanceContext) -> Self {
        Self(ecstore_internode::ExternalStreamAuth::new(instance))
    }

    /// Derives the data authentication key for one control body using the node credential.
    pub fn transfer_key(&self, request_digest: &[u8; 32]) -> io::Result<[u8; 32]> {
        self.0.transfer_key(request_digest)
    }

    /// Sends one signed control using the existing pooled node HTTP client, TLS,
    /// proxy configuration and error classification. This function performs no replay.
    pub async fn open_control(&self, endpoint: &str, body: Bytes, deadline: Instant) -> io::Result<FileReader> {
        if body.len() > MAX_CONTROL_BYTES {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "internode control exceeds limit"));
        }
        let (url, headers) = self.0.sign_control(endpoint, &Sha256::digest(&body).into())?;
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or_else(|| io::Error::new(io::ErrorKind::TimedOut, "internode operation deadline elapsed"))?;
        let reader = tokio::time::timeout_at(
            deadline.into(),
            rustfs_rio::HttpReader::post_without_redirects(url, headers, body.to_vec(), remaining),
        )
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "internode control deadline elapsed"))??;
        Ok(Box::new(reader))
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ControlQuery {
    body_sha256: String,
    request_nonce: uuid::Uuid,
}

pub(super) async fn handle<B>(installed: &InstalledControlService, request: Request<B>) -> Result<FileReader, ControlRejection>
where
    B: http_body::Body + Send + 'static,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    let query = match request
        .uri()
        .query()
        .and_then(|query| serde_urlencoded::from_str::<ControlQuery>(query).ok())
    {
        Some(query)
            if !query.request_nonce.is_nil()
                && query.body_sha256.len() == 64
                && query
                    .body_sha256
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)) =>
        {
            query
        }
        _ => return Err(response(StatusCode::BAD_REQUEST, "invalid internode control scope")),
    };
    let admission = match Arc::clone(&installed.admission).try_acquire_owned() {
        Ok(permit) => Arc::new(permit),
        Err(_) => return Err(response(StatusCode::TOO_MANY_REQUESTS, "internode control capacity exhausted")),
    };
    let (parts, body) = request.into_parts();
    let body = match tokio::time::timeout(Duration::from_secs(15), Limited::new(body, MAX_CONTROL_BYTES).collect()).await {
        Ok(Ok(body)) => body.to_bytes(),
        Ok(Err(_)) => return Err(response(StatusCode::PAYLOAD_TOO_LARGE, "invalid internode control body")),
        Err(_) => return Err(response(StatusCode::REQUEST_TIMEOUT, "internode control body deadline elapsed")),
    };
    let request_digest: [u8; 32] = Sha256::digest(&body).into();
    if query.body_sha256 != hex_simd::encode_to_string(request_digest, hex_simd::AsciiCase::Lower) {
        return Err(response(StatusCode::FORBIDDEN, "invalid internode control digest"));
    }
    if let Err(error) =
        check_and_record_signed_rpc_nonce(&parts.headers, query.request_nonce, CONTROL_PATH, "external_stream", "external")
    {
        if crate::storage::storage_api::rpc_auth_is_resource_exhausted(&error) {
            return Err(response(StatusCode::SERVICE_UNAVAILABLE, "internode replay admission deferred"));
        }
        return Err(response(StatusCode::FORBIDDEN, "invalid internode control nonce"));
    }
    let key = installed
        .auth
        .transfer_key(&request_digest)
        .map_err(|_| response(StatusCode::SERVICE_UNAVAILABLE, "internode control key unavailable"))?;
    let storage = AuthenticatedStreams {
        request_digest,
        key,
        operation: Mutex::new(Some(StorageOperation {
            instance: Arc::clone(&installed.instance),
            background: RemoteBackgroundIo::take_current(),
        })),
    };
    installed
        .service
        .open(body, storage, admission)
        .await
        .map_err(|_| response(StatusCode::SERVICE_UNAVAILABLE, "internode stream provider refused control"))
}

#[derive(Debug)]
pub(super) struct ControlRejection {
    pub(super) status: StatusCode,
    pub(super) message: &'static str,
}
fn response(status: StatusCode, message: &'static str) -> ControlRejection {
    ControlRejection { status, message }
}

#[cfg(test)]
mod tests {
    use super::{AuthenticatedStreams, ExternalControlService, FileReader, InstalledControlService, PeerControlClient};
    use crate::storage::storage_api::InstanceContext;
    use crate::storage::storage_api::{
        Endpoint,
        ecstore_layout::{Endpoints, PoolEndpoints},
    };
    use async_trait::async_trait;
    use bytes::Bytes;
    use http::StatusCode;
    use sha2::{Digest as _, Sha256};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::{
        io,
        sync::Arc,
        time::{Duration, Instant},
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[derive(Default)]
    struct TestService {
        opened: AtomicUsize,
        keep_running: bool,
    }

    #[async_trait]
    impl ExternalControlService for TestService {
        fn max_operations(&self) -> usize {
            1
        }
        async fn open(
            &self,
            body: Bytes,
            storage: AuthenticatedStreams,
            _admission: Arc<dyn Send + Sync>,
        ) -> io::Result<FileReader> {
            let digest: [u8; 32] = Sha256::digest(&body).into();
            assert_eq!(storage.request_digest(), digest);
            self.opened.fetch_add(1, Ordering::Relaxed);
            Ok(Box::new(std::io::Cursor::new(body)))
        }
        async fn shutdown(&self, _: Instant) -> io::Result<()> {
            if self.keep_running {
                std::future::pending::<()>().await;
            }
            Ok(())
        }
    }

    fn instance(endpoint: &str) -> Arc<InstanceContext> {
        let instance = Arc::new(InstanceContext::new());
        let disk = Endpoint::try_from(format!("{endpoint}/disk").as_str()).expect("configured disk endpoint");
        instance.set_endpoints(
            vec![PoolEndpoints {
                legacy: false,
                set_count: 1,
                drives_per_set: 1,
                endpoints: Endpoints::from(vec![disk]),
                cmd_line: String::new(),
                platform: String::new(),
            }]
            .into(),
        );
        instance
    }

    #[tokio::test]
    async fn peer_transport_shutdown_closes_admission_and_bounds_pending_workers() {
        let service = TestService {
            keep_running: true,
            ..TestService::default()
        };
        let installed =
            InstalledControlService::new(Arc::new(service), Arc::new(InstanceContext::new())).expect("valid capacity");
        let error = installed.shutdown(Instant::now()).await.expect_err("worker remains active");
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(installed.admission.is_closed());
    }

    #[tokio::test]
    async fn peer_transport_control_checks_signature_digest_replay_and_instance() {
        use hyper_util::{rt::TokioIo, service::TowerToHyperService};
        let _ = rustfs_credentials::set_global_rpc_secret("external-control-test-secret".into());
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("RPC listener");
        let base = format!("http://{}", listener.local_addr().expect("RPC address"));
        let instance = instance(&base);
        let recorder = Arc::new(TestService::default());
        let service = Arc::new(InstalledControlService::new(recorder.clone(), instance.clone()).expect("control service"));
        let server = tokio::spawn(async move {
            loop {
                let (socket, _) = listener.accept().await.expect("RPC connect");
                let fallback = tower::service_fn(|_| async {
                    Ok::<_, std::convert::Infallible>(http::Response::new(rustfs_s3_types::Body::empty()))
                });
                let rpc = super::super::http_service::InternodeRpcService::with_external_control(fallback, Some(service.clone()));
                tokio::spawn(async move {
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(socket), TowerToHyperService::new(rpc))
                        .await;
                });
            }
        });
        let sender = PeerControlClient::new(&instance);
        let other = PeerControlClient::new(&InstanceContext::new());
        let body = Bytes::from_static(b"control");
        let deadline = Instant::now() + Duration::from_secs(5);
        let refused = other
            .open_control(&base, body.clone(), deadline)
            .await
            .err()
            .expect("another instance has no such peer");
        assert_eq!(refused.kind(), io::ErrorKind::PermissionDenied);
        let (url, headers) = sender
            .0
            .sign_control(&base, &Sha256::digest(&body).into())
            .expect("signed request");
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("test HTTP client");
        let status = client
            .post(&url)
            .body(body.clone())
            .send()
            .await
            .expect("unsigned response")
            .status();
        assert_eq!(status, StatusCode::FORBIDDEN);
        let status = client
            .post(&url)
            .headers(headers.clone())
            .body("forged")
            .send()
            .await
            .expect("digest response")
            .status();
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(recorder.opened.load(Ordering::Relaxed), 0);
        let response = client
            .post(&url)
            .headers(headers.clone())
            .body(body.clone())
            .send()
            .await
            .expect("signed response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.bytes().await.expect("control response"), body);
        let replay = client
            .post(&url)
            .headers(headers)
            .body(body.clone())
            .send()
            .await
            .expect("replay response");
        assert_eq!(replay.status(), StatusCode::FORBIDDEN);
        assert_eq!(recorder.opened.load(Ordering::Relaxed), 1);
        let mut reply = sender
            .open_control(&base, body.clone(), deadline)
            .await
            .expect("fresh signed control");
        let mut bytes = Vec::new();
        reply.read_to_end(&mut bytes).await.expect("control response body");
        assert_eq!(bytes.as_slice(), &body[..]);
        assert_eq!(recorder.opened.load(Ordering::Relaxed), 2);
        server.abort();
    }

    #[tokio::test]
    async fn peer_transport_control_does_not_forward_signed_headers_or_body_to_redirects() {
        let _ = rustfs_credentials::set_global_rpc_secret("external-control-test-secret".into());
        let first = TcpListener::bind("127.0.0.1:0").await.expect("registered peer");
        let second = TcpListener::bind("127.0.0.1:0").await.expect("redirect target");
        let base = format!("http://{}", first.local_addr().expect("registered address"));
        let second_addr = second.local_addr().expect("redirect address");
        let server = tokio::spawn(async move {
            let (mut socket, _) = first.accept().await.expect("control connection");
            let mut request = [0; 4096];
            let n = socket.read(&mut request).await.expect("control request");
            assert!(String::from_utf8_lossy(&request[..n]).contains("x-rustfs-signature:"));
            socket.write_all(format!("HTTP/1.1 307 Temporary Redirect\r\nLocation: http://{second_addr}/capture\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes()).await.expect("redirect response");
        });
        let sender = PeerControlClient::new(&instance(&base));
        let body = Bytes::from_static(b"control");
        let deadline = Instant::now() + Duration::from_secs(5);
        let rejected = tokio::select! {
            result = sender.open_control(&base, body, deadline) => result.is_err(),
            _ = second.accept() => panic!("redirect forwarded the signed request"),
        };
        assert!(rejected);
        server.await.expect("registered peer served the redirect");
    }
}
