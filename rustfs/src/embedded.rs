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

//! Embedded RustFS server for integration testing.
//!
//! Start a fully-functional S3-compatible server in-process without Docker
//! or child processes. Perfect for integration tests that need a local S3
//! endpoint.
//!
//! # Quick start
//!
//! ```rust,no_run
//! use rustfs::embedded::{find_available_port, RustFSServerBuilder};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//!     let port = find_available_port()?;
//!     let server = RustFSServerBuilder::new()
//!         .address(format!("127.0.0.1:{port}"))
//!         .access_key("rustfsadmin")
//!         .secret_key("rustfsadmin")
//!         .build()
//!         .await?;
//!
//!     println!("S3 endpoint: {}", server.endpoint());
//!     // ... use any S3 client ...
//!     server.shutdown().await;
//!     Ok(())
//! }
//! ```
//!
//! # Limitations
//!
//! Only **one `RustFSServer`** may exist per process because the underlying
//! storage engine uses process-global singletons (`OnceLock`). Attempting to
//! start a second server will return an error.

use crate::app::context::{AppContext, init_global_app_context};
use crate::config::Config;
use crate::init::{add_bucket_notification_configuration, init_buffer_profile_system, init_kms_system};
use crate::server::{init_event_notifier, shutdown_event_notifier, start_audit_system, start_http_server, stop_audit_system};
use rustfs_common::{GlobalReadiness, SystemStage, set_global_addr};
use rustfs_credentials::init_global_action_credentials;
use rustfs_ecstore::store::init_lock_clients;
use rustfs_ecstore::{
    bucket::replication::init_background_replication,
    bucket::{
        metadata_sys::init_bucket_metadata_sys,
        migration::{try_migrate_bucket_metadata, try_migrate_iam_config},
    },
    config as ecconfig,
    endpoints::EndpointServerPools,
    global::set_global_rustfs_port,
    notification_sys::new_global_notification_sys,
    set_global_endpoints,
    store::ECStore,
    store::init_local_disks,
    store_api::BucketOperations,
    store_api::BucketOptions,
    update_erasure_type,
};
use rustfs_iam::init_iam_sys;
use rustfs_obs::{init_obs, set_global_guard};
use rustfs_utils::net::parse_and_resolve_address;
use rustls::crypto::aws_lc_rs::default_provider;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// Tracks whether a server has been started in this process.
static SERVER_STARTED: AtomicBool = AtomicBool::new(false);

/// Error type for embedded server operations.
#[derive(Debug)]
pub enum ServerError {
    /// A server has already been started in this process.
    AlreadyStarted,
    /// The server failed to initialize.
    Init(String),
    /// An I/O error occurred.
    Io(std::io::Error),
}

impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerError::AlreadyStarted => write!(
                f,
                "A RustFS server has already been started in this process. \
                 Only one embedded server is supported due to global state."
            ),
            ServerError::Init(msg) => write!(f, "RustFS initialization failed: {msg}"),
            ServerError::Io(e) => write!(f, "I/O error: {e}"),
        }
    }
}

impl std::error::Error for ServerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ServerError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for ServerError {
    fn from(e: std::io::Error) -> Self {
        ServerError::Io(e)
    }
}

/// Builder for configuring and starting an embedded RustFS server.
///
/// # Examples
///
/// ```rust,no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
/// use rustfs::embedded::RustFSServerBuilder;
///
/// let server = RustFSServerBuilder::new()
///     .address("127.0.0.1:9100")
///     .access_key("mykey")
///     .secret_key("mysecret")
///     .volume("/tmp/rustfs-data")
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct RustFSServerBuilder {
    address: String,
    access_key: String,
    secret_key: String,
    volumes: Vec<String>,
    region: String,
}

impl Default for RustFSServerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl RustFSServerBuilder {
    /// Create a new builder with sensible defaults.
    ///
    /// Defaults:
    /// - address: `"127.0.0.1:9000"`
    /// - access_key / secret_key: `"rustfsadmin"`
    /// - region: `"us-east-1"`
    /// - A temporary directory is created automatically for data storage
    ///
    /// Use [`find_available_port`] to pick a free port when the default is
    /// not suitable.
    pub fn new() -> Self {
        Self {
            address: "127.0.0.1:9000".to_string(),
            access_key: rustfs_credentials::DEFAULT_ACCESS_KEY.to_string(),
            secret_key: rustfs_credentials::DEFAULT_SECRET_KEY.to_string(),
            volumes: Vec::new(),
            region: rustfs_config::RUSTFS_REGION.to_string(),
        }
    }

    /// Set the listen address (e.g. `"127.0.0.1:9000"`).
    ///
    /// Use [`find_available_port`] to obtain a free port when the default is
    /// not suitable. Port `0` is **not** supported because startup requires
    /// a concrete listen address and port during initialization.
    ///
    /// The bound address is available via [`RustFSServer::address`] after
    /// [`build`](Self::build), but that is too late for the earlier
    /// initialization that depends on the configured address.
    pub fn address(mut self, addr: impl Into<String>) -> Self {
        self.address = addr.into();
        self
    }

    /// Set the S3 access key (default: `"rustfsadmin"`).
    pub fn access_key(mut self, key: impl Into<String>) -> Self {
        self.access_key = key.into();
        self
    }

    /// Set the S3 secret key (default: `"rustfsadmin"`).
    pub fn secret_key(mut self, key: impl Into<String>) -> Self {
        self.secret_key = key.into();
        self
    }

    /// Set the AWS region (default: `"us-east-1"`).
    pub fn region(mut self, region: impl Into<String>) -> Self {
        self.region = region.into();
        self
    }

    /// Add a data volume path.
    ///
    /// If no volumes are added, a temporary directory with a single drive is
    /// created automatically (and cleaned up on [`RustFSServer::shutdown`]).
    pub fn volume(mut self, path: impl Into<String>) -> Self {
        self.volumes.push(path.into());
        self
    }

    /// Set multiple volume paths at once, replacing any previously set volumes.
    pub fn volumes(mut self, paths: Vec<String>) -> Self {
        self.volumes = paths;
        self
    }

    /// Build and start the embedded server.
    ///
    /// Returns a [`RustFSServer`] handle that provides the endpoint URL and
    /// a [`shutdown`](RustFSServer::shutdown) method.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError::AlreadyStarted`] if another server is already
    /// running in this process, or if another startup attempt has already
    /// entered irreversible global initialization.
    pub async fn build(mut self) -> Result<RustFSServer, ServerError> {
        self.do_build().await
    }

    /// Inner build implementation. Separated from [`build`] so the outer
    /// method can enforce the one-shot process-global startup guard.
    async fn do_build(&mut self) -> Result<RustFSServer, ServerError> {
        // Build is allowed to fail before irreversible global initialization
        // (for example on temporary I/O or directory setup errors), and in that
        // case callers can retry.
        let mut global_init_started = false;
        let mut set_global_init_guard = || -> Result<(), ServerError> {
            if global_init_started {
                return Ok(());
            }
            if SERVER_STARTED
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_err()
            {
                return Err(ServerError::AlreadyStarted);
            }
            global_init_started = true;
            Ok(())
        };

        // Keep a TempDir guard alive so that if build fails the directory is
        // cleaned up automatically. We disarm (keep) on success.
        let mut temp_dir_guard: Option<tempfile::TempDir> = None;
        if self.volumes.is_empty() {
            let dir = tempfile::tempdir().map_err(|e| ServerError::Init(format!("failed to create temp dir: {e}")))?;
            self.volumes.push(dir.path().display().to_string());
            temp_dir_guard = Some(dir);
        }

        // Ensure volume directories exist.
        for v in &self.volumes {
            let p = Path::new(v);
            if !p.exists() {
                tokio::fs::create_dir_all(p)
                    .await
                    .map_err(|e| ServerError::Init(format!("failed to create volume dir {v}: {e}")))?;
            }
        }

        // Build Config.
        let mut config = Config::new(&self.address, self.volumes.clone());
        config.access_key = self.access_key.clone();
        config.secret_key = self.secret_key.clone();
        config.region = Some(self.region.clone());
        config.console_enable = false;

        // --- Initialization sequence (mirrors main.rs::run) ---

        // Observability (minimal / no-op endpoint for embedded use).
        let guard = init_obs(Some(config.obs_endpoint.clone()))
            .await
            .map_err(|e| ServerError::Init(format!("init_obs: {e}")))?;
        set_global_guard(guard).map_err(|e| ServerError::Init(format!("set_global_guard: {e}")))?;

        // Crypto provider.
        if let Err(err) = default_provider().install_default() {
            debug!("Ignoring crypto provider installation error: {err:?}");
        }

        // Trusted proxies.
        rustfs_trusted_proxies::init();

        // Credentials.
        init_global_action_credentials(Some(config.access_key.clone()), Some(config.secret_key.clone()))
            .map_err(|e| ServerError::Init(format!("credentials: {e:?}")))?;

        // Region.
        if let Some(region_str) = &config.region {
            let region = region_str
                .parse()
                .map_err(|e| ServerError::Init(format!("invalid region '{region_str}': {e}")))?;
            rustfs_ecstore::global::set_global_region(region);
        }

        // Resolve listen address.
        let server_addr =
            parse_and_resolve_address(config.address.as_str()).map_err(|e| ServerError::Init(format!("address: {e}")))?;

        if server_addr.port() == 0 {
            return Err(ServerError::Init(
                "port 0 is not supported in embedded mode because startup requires \
                 a stable listen address and port before endpoint/global initialization. \
                 Use `find_available_port()` to obtain a free port."
                    .to_string(),
            ));
        }

        let server_port = server_addr.port();

        set_global_rustfs_port(server_port);
        set_global_addr(&config.address).await;

        set_global_init_guard()?;

        // Endpoints / erasure setup.
        let server_addr_str = server_addr.to_string();
        let (endpoint_pools, setup_type) = EndpointServerPools::from_volumes(server_addr_str.as_str(), config.volumes.clone())
            .await
            .map_err(|e| ServerError::Init(format!("endpoints: {e}")))?;

        set_global_endpoints(endpoint_pools.as_ref().clone());
        update_erasure_type(setup_type).await;

        // Local disks.
        init_local_disks(endpoint_pools.clone())
            .await
            .map_err(|e| ServerError::Init(format!("local disks: {e}")))?;
        init_lock_clients(endpoint_pools.clone());

        // Service state.
        let readiness = Arc::new(GlobalReadiness::new());

        // Start HTTP server.
        let mut s3_config = config.clone();
        s3_config.console_enable = false;
        let (shutdown_tx, bound_addr) = start_http_server(&s3_config, readiness.clone()).await?;
        let ctx = CancellationToken::new();
        let shutdown_embedded_server = || {
            let _ = shutdown_tx.send(());
            ctx.cancel();
        };

        // Storage engine.
        let store = match ECStore::new(server_addr, endpoint_pools.clone(), ctx.clone()).await {
            Ok(store) => store,
            Err(e) => {
                error!("ECStore::new {:?}", e);
                shutdown_embedded_server();
                return Err(ServerError::Init(format!("ECStore: {e}")));
            }
        };

        ecconfig::init();
        ecconfig::try_migrate_server_config(store.clone()).await;

        // Global config system (with retry).
        let mut retry = 0;
        while let Err(e) = ecconfig::init_global_config_sys(store.clone()).await {
            retry += 1;
            if retry > 15 {
                shutdown_embedded_server();
                return Err(ServerError::Init(format!("init_global_config_sys failed after 15 retries: {e}")));
            }
            debug!("init_global_config_sys retry {retry}: {e}");
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
        readiness.mark_stage(SystemStage::StorageReady);

        // Replication.
        init_background_replication(store.clone()).await;

        // KMS (optional, non-fatal for embedded).
        if let Err(e) = init_kms_system(&config).await {
            warn!("KMS initialization skipped: {e}");
        }

        // Buffer profiles.
        init_buffer_profile_system(&config);

        // Event notifier.
        init_event_notifier().await;

        // Audit (non-fatal).
        if let Err(e) = start_audit_system().await {
            warn!("Audit system: {e}");
        }

        // Bucket listing for metadata + notification init.
        let buckets: Vec<String> = store
            .list_bucket(&BucketOptions {
                no_metadata: true,
                ..Default::default()
            })
            .await
            .map_err(|e| {
                shutdown_embedded_server();
                ServerError::Init(format!("list_bucket: {e}"))
            })?
            .into_iter()
            .map(|v| v.name)
            .collect();

        try_migrate_bucket_metadata(store.clone()).await;
        init_bucket_metadata_sys(store.clone(), buckets.clone()).await;
        try_migrate_iam_config(store.clone()).await;

        // IAM.
        init_iam_sys(store.clone()).await.map_err(|e| {
            shutdown_embedded_server();
            ServerError::Init(format!("IAM: {e}"))
        })?;
        readiness.mark_stage(SystemStage::IamReady);

        // App context.
        let iam_interface = rustfs_iam::get().map_err(|e| {
            shutdown_embedded_server();
            ServerError::Init(format!("IAM get: {e}"))
        })?;
        let kms_interface =
            rustfs_kms::get_global_kms_service_manager().unwrap_or_else(rustfs_kms::init_global_kms_service_manager);
        let _app_context =
            init_global_app_context(AppContext::with_default_interfaces(store.clone(), iam_interface, kms_interface));

        // Bucket notifications.
        add_bucket_notification_configuration(buckets.clone()).await;

        // Notification system.
        if let Err(e) = new_global_notification_sys(endpoint_pools.clone()).await {
            warn!("notification system: {e}");
        }

        // Mark fully ready.
        readiness.mark_stage(SystemStage::FullReady);
        rustfs_common::set_global_init_time_now().await;

        let server = RustFSServer {
            address: bound_addr,
            access_key: self.access_key.clone(),
            secret_key: self.secret_key.clone(),
            region: self.region.clone(),
            shutdown_tx: Some(shutdown_tx),
            cancel_token: ctx,
            temp_dir: temp_dir_guard.map(|g| g.keep()),
        };

        info!(
            target: "rustfs::embedded",
            "RustFS embedded server ready at http://{}",
            server.endpoint_address()
        );

        Ok(server)
    }
}

/// A running embedded RustFS server.
///
/// Use [`endpoint`](Self::endpoint) to get the HTTP URL for S3 clients.
/// Call [`shutdown`](Self::shutdown) to stop the server and clean up resources.
///
/// Dropping the server performs best-effort synchronous cleanup and may leave
/// async or process-global subsystems running until process exit.
pub struct RustFSServer {
    address: SocketAddr,
    access_key: String,
    secret_key: String,
    region: String,
    shutdown_tx: Option<tokio::sync::broadcast::Sender<()>>,
    cancel_token: CancellationToken,
    temp_dir: Option<PathBuf>,
}

impl RustFSServer {
    fn endpoint_address(&self) -> SocketAddr {
        let ip = match self.address.ip() {
            ip @ IpAddr::V4(v4) if !v4.is_unspecified() => ip,
            IpAddr::V4(_) => IpAddr::V4(Ipv4Addr::LOCALHOST),
            ip @ IpAddr::V6(v6) if !v6.is_unspecified() => ip,
            IpAddr::V6(_) => IpAddr::V6(Ipv6Addr::LOCALHOST),
        };

        SocketAddr::new(ip, self.address.port())
    }

    /// The HTTP endpoint URL (e.g. `"http://127.0.0.1:54321"`).
    ///
    /// Pass this to your S3 client's `endpoint_url` setting.
    pub fn endpoint(&self) -> String {
        format!("http://{}", self.endpoint_address())
    }

    /// The bound socket address.
    pub fn address(&self) -> SocketAddr {
        self.address
    }

    /// The configured access key.
    pub fn access_key(&self) -> &str {
        &self.access_key
    }

    /// The configured secret key.
    pub fn secret_key(&self) -> &str {
        &self.secret_key
    }

    /// The configured region.
    pub fn region(&self) -> &str {
        &self.region
    }

    /// Gracefully stop the server and clean up resources.
    pub async fn shutdown(mut self) {
        self.do_shutdown().await;
    }

    async fn do_shutdown(&mut self) {
        info!(target: "rustfs::embedded", "Shutting down embedded RustFS server...");

        // Cancel background services.
        self.cancel_token.cancel();

        // Shutdown event notifier.
        shutdown_event_notifier().await;

        // Stop the audit system.
        if let Err(e) = stop_audit_system().await {
            warn!("Failed to stop audit system during shutdown: {e}");
        }

        // Signal HTTP server to stop.
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }

        // Brief grace period for connections to drain.
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        // Clean up temp directory if we created it.
        if let Some(ref dir) = self.temp_dir
            && let Err(e) = tokio::fs::remove_dir_all(dir).await
        {
            warn!("Failed to clean up temp dir {}: {e}", dir.display());
        }

        info!(target: "rustfs::embedded", "Embedded RustFS server stopped.");
    }
}

impl Drop for RustFSServer {
    fn drop(&mut self) {
        // Best-effort synchronous cleanup.
        self.cancel_token.cancel();
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(ref dir) = self.temp_dir {
            let _ = std::fs::remove_dir_all(dir);
        }
    }
}

/// Find an available TCP port on localhost.
///
/// Binds to port `0`, reads the OS-assigned port, then releases the socket.
/// The port is **best-effort**: another process could claim it before RustFS
/// binds (TOCTOU), but in practice this is reliable for testing.
///
/// Use with [`RustFSServerBuilder::address`]:
///
/// ```rust,no_run
/// use rustfs::embedded::{find_available_port, RustFSServerBuilder};
///
/// async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
///     let port = find_available_port()?;
///     let server = RustFSServerBuilder::new()
///         .address(format!("127.0.0.1:{port}"))
///         .build()
///         .await?;
///     println!("Listening on port {port}");
///      Ok(())
///  }
/// ```
pub fn find_available_port() -> Result<u16, std::io::Error> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}
