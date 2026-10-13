//! Opt-in native capture ownership within the RustFS listener lifecycle.
mod config;
#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod tests;

use crate::{config::Config, storage_api::server::http::ServerContextSlot};
use config::CaptureConfig;
#[cfg(any(target_os = "linux", target_os = "macos"))]
use config::CaptureMode;
#[cfg(any(target_os = "linux", target_os = "macos"))]
use rustfs_capture_authority::Store;
use rustfs_capture_authority::{CheckpointLimits, NativeLogWriter, NativeRaftOwner};
use rustfs_ecstore::api::rpc::{CaptureRaftLimits, CaptureRaftNetworkFactory, CaptureRaftRpc, normalize_tonic_rpc_audience};
use rustfs_protos::proto_gen::node_service::{
    CaptureRaftRequest, CaptureRaftResponse,
    capture_raft_service_server::{CaptureRaftService, CaptureRaftServiceServer},
};
use std::{io, sync::Arc};
use tonic::{Request, Response, Status};

const EVENT_NATIVE_CAPTURE_STATE: &str = "native_capture_state";
const LOG_COMPONENT_MAIN: &str = "main";
const LOG_SUBSYSTEM_STARTUP: &str = "startup";

/// Idle HTTP routes retain this holder, never the native storage owner after unmount.
#[derive(Clone)]
pub(crate) struct NativeCaptureService {
    rpc: Arc<tokio::sync::RwLock<Option<CaptureRaftRpc>>>,
    limits: CaptureRaftLimits,
    accepting: Arc<std::sync::atomic::AtomicBool>,
}
impl NativeCaptureService {
    pub(crate) fn to_service(&self) -> CaptureRaftServiceServer<Self> {
        CaptureRaftServiceServer::new(self.clone())
            .max_decoding_message_size(self.limits.max_request_bytes + 64)
            .max_encoding_message_size(self.limits.max_response_bytes + 128)
    }
}
#[tonic::async_trait]
impl CaptureRaftService for NativeCaptureService {
    async fn append_entries(&self, request: Request<CaptureRaftRequest>) -> Result<Response<CaptureRaftResponse>, Status> {
        if !self.accepting.load(std::sync::atomic::Ordering::Acquire) {
            return Err(Status::unavailable("native capture service stopping"));
        }
        let rpc = self.rpc.read().await;
        rpc.as_ref()
            .ok_or_else(|| Status::unavailable("native capture service stopped"))?
            .append_entries(request)
            .await
    }
    async fn vote(&self, request: Request<CaptureRaftRequest>) -> Result<Response<CaptureRaftResponse>, Status> {
        if !self.accepting.load(std::sync::atomic::Ordering::Acquire) {
            return Err(Status::unavailable("native capture service stopping"));
        }
        let rpc = self.rpc.read().await;
        rpc.as_ref()
            .ok_or_else(|| Status::unavailable("native capture service stopped"))?
            .vote(request)
            .await
    }
    async fn install_snapshot(&self, request: Request<CaptureRaftRequest>) -> Result<Response<CaptureRaftResponse>, Status> {
        if !self.accepting.load(std::sync::atomic::Ordering::Acquire) {
            return Err(Status::unavailable("native capture service stopping"));
        }
        let rpc = self.rpc.read().await;
        rpc.as_ref()
            .ok_or_else(|| Status::unavailable("native capture service stopped"))?
            .install_snapshot(request)
            .await
    }
}

pub(crate) struct NativeCaptureRuntime {
    owner: Option<Arc<NativeRaftOwner>>,
    writer: Option<NativeLogWriter>,
    raft_config: Arc<openraft::Config>,
    factory: CaptureRaftNetworkFactory,
    service: Arc<NativeCaptureService>,
    slot: Arc<ServerContextSlot>,
    config: CaptureConfig,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    reader_shutdown: rustfs_capture_authority::source_reader::Shutdown,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    reader_task: tokio::task::JoinHandle<Result<(), rustfs_capture_authority::source_reader::ReaderError>>,
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(crate) async fn start_capture(config: &Config, slot: &Arc<ServerContextSlot>) -> io::Result<Option<NativeCaptureRuntime>> {
    let Some(path) = &config.native_capture_config else {
        return Ok(None);
    };
    let path = path.clone();
    let capture = tokio::task::spawn_blocking(move || CaptureConfig::read(path))
        .await
        .map_err(io::Error::other)??;
    let address = rustfs_utils::net::parse_and_resolve_address(&config.address).map_err(io::Error::other)?;
    if address.port() == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "native capture requires a fixed listener port",
        ));
    }
    // Resolve the existing internode credential before creating durable state.
    rustfs_credentials::try_get_rpc_token()?;
    let raft_config = Arc::new(openraft::Config::default().validate().map_err(io::Error::other)?);
    let limits = CheckpointLimits::default();
    let transport_limits = CaptureRaftLimits::default();
    let factory = CaptureRaftNetworkFactory::new(capture.identity, capture.peers.clone(), raft_config.clone(), transport_limits)
        .map_err(io::Error::other)?;
    let database = capture.database.clone();
    let identity = capture.identity;
    let binding = capture.capture_binding.clone();
    let mode = capture.mode;
    let store = tokio::task::spawn_blocking(move || {
        let mut store = match mode {
            CaptureMode::Create => Store::initialize(database, identity)?,
            CaptureMode::Recover => Store::open_with_checkpoint_limits(database, identity, limits)?,
        };
        match mode {
            CaptureMode::Create => {
                store.pin_capture_binding(binding)?;
                store.enable_native_empty()?;
                store.promote_created_provenance()?;
            }
            CaptureMode::Recover => {
                if store.capture_binding()? != Some(binding) {
                    return Err(rustfs_capture_authority::StoreError::BindingConflict);
                }
                store.validate_native_provenance_state()?;
            }
        }
        Ok::<_, rustfs_capture_authority::StoreError>(store)
    })
    .await
    .map_err(io::Error::other)?
    .map_err(io::Error::other)?;
    let writer = NativeLogWriter::new(store);
    // Refuse an occupied reader path before Raft can perform any background work.
    let (reader, reader_shutdown) =
        rustfs_capture_authority::source_reader::SourceReaderServer::bind(&capture.reader_socket, writer.clone())
            .map_err(io::Error::other)?;
    let service = Arc::new(NativeCaptureService {
        rpc: Arc::new(tokio::sync::RwLock::new(None)),
        limits: transport_limits,
        accepting: Arc::new(std::sync::atomic::AtomicBool::new(true)),
    });
    let runtime = NativeCaptureRuntime {
        owner: None,
        writer: Some(writer),
        raft_config,
        factory,
        service,
        slot: slot.clone(),
        config: capture,
        reader_shutdown,
        reader_task: tokio::spawn(reader.serve()),
    };
    if let Err(error) = slot.install_capture_service(runtime.service.clone()) {
        runtime.shutdown().await?;
        return Err(error);
    }
    Ok(Some(runtime))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub(crate) async fn start_capture(config: &Config, _slot: &Arc<ServerContextSlot>) -> io::Result<Option<NativeCaptureRuntime>> {
    if config.native_capture_config.is_some() {
        return Err(io::Error::new(io::ErrorKind::Unsupported, "native capture requires Linux or macOS"));
    }
    Ok(None)
}

impl NativeCaptureRuntime {
    #[cfg(test)]
    fn owner(&self) -> &NativeRaftOwner {
        self.owner.as_deref().expect("startup finished with a real native owner")
    }

    pub(crate) fn stop_admission(&self) {
        self.service.accepting.store(false, std::sync::atomic::Ordering::Release);
    }

    pub(crate) async fn finish_startup(&mut self, audience: &str) -> io::Result<()> {
        let local = self
            .config
            .peers
            .get(&self.config.identity.node_id)
            .ok_or_else(|| io::Error::other("native capture local member missing"))?;
        let uri: http::Uri = local.rpc_uri().parse().map_err(io::Error::other)?;
        let configured = normalize_tonic_rpc_audience(
            uri.authority()
                .ok_or_else(|| io::Error::other("native capture local authority missing"))?
                .as_str(),
        )?;
        if configured != normalize_tonic_rpc_audience(audience)? {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "native capture local authority differs from Ping audience",
            ));
        }
        if !self.service.accepting.load(std::sync::atomic::Ordering::Acquire) {
            return Err(io::Error::other("native capture service stopping"));
        }
        let writer = self
            .writer
            .take()
            .ok_or_else(|| io::Error::other("native capture owner already started"))?;
        // Raft::new can replay committed entries and restore leader replication.
        // Neither may run until this process's actual Ping audience is verified.
        let owner = Arc::new(
            NativeRaftOwner::start(
                writer,
                self.config.identity.node_id,
                self.raft_config.clone(),
                CheckpointLimits::default(),
                self.factory.clone(),
            )
            .await
            .map_err(io::Error::other)?,
        );
        self.owner = Some(owner.clone());
        let rpc = CaptureRaftRpc::new(owner.clone(), self.config.peers.clone(), CaptureRaftLimits::default())
            .map_err(io::Error::other)?;
        *self.service.rpc.write().await = Some(rpc);
        if self.config.bootstrap {
            owner.bootstrap(self.config.peers.clone()).await.map_err(io::Error::other)?;
        }
        tracing::info!(
            event = EVENT_NATIVE_CAPTURE_STATE,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            state = "ready",
            node_id = self.config.identity.node_id,
            mode = ?self.config.mode,
            "Native capture runtime ready"
        );
        Ok(())
    }

    pub(crate) async fn shutdown(self) -> io::Result<()> {
        let Self {
            owner,
            writer,
            raft_config: _,
            factory: _,
            service,
            slot,
            config: _,
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            reader_shutdown,
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            reader_task,
        } = self;
        service.accepting.store(false, std::sync::atomic::Ordering::Release);
        slot.remove_capture_service(&service);
        let rpc = service.rpc.write().await.take();
        let rpc_result = match &rpc {
            Some(rpc) => rpc.shutdown().await.map_err(io::Error::other),
            None => Ok(()),
        };
        let reader_result = {
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            {
                reader_shutdown.shutdown();
                reader_task
                    .await
                    .map_err(io::Error::other)
                    .and_then(|result| result.map_err(io::Error::other))
            }
            #[cfg(not(any(target_os = "linux", target_os = "macos")))]
            {
                Ok::<_, io::Error>(())
            }
        };
        let owner_result = match &owner {
            Some(owner) => owner.shutdown().await.map_err(io::Error::other),
            None => Ok(()),
        };
        drop(rpc);
        drop(service);
        drop(owner);
        drop(writer);
        rpc_result?;
        reader_result?;
        owner_result?;
        tracing::info!(
            event = EVENT_NATIVE_CAPTURE_STATE,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            state = "stopped",
            "Native capture runtime drained"
        );
        Ok(())
    }
}
