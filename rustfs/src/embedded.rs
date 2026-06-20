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

use crate::server::ShutdownHandle;
use crate::startup_lifecycle::{EmbeddedStartupGuard, log_embedded_server_ready, publish_embedded_startup_ready};
use crate::startup_runtime_hooks::init_embedded_runtime_hooks;
use crate::startup_server::{
    EmbeddedStartupConfig, init_embedded_startup_listen_context, prepare_embedded_startup_config, start_embedded_http_server,
};
use crate::startup_services::init_embedded_startup_runtime_services;
use crate::startup_shutdown::{run_embedded_server_shutdown, signal_embedded_startup_shutdown};
use crate::startup_storage::{init_embedded_startup_storage_foundation, init_embedded_startup_storage_runtime};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::PathBuf;
use tokio_util::sync::CancellationToken;

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
        let mut startup_guard = EmbeddedStartupGuard::new();

        let EmbeddedStartupConfig { config, temp_dir_guard } = prepare_embedded_startup_config(
            self.address.clone(),
            self.access_key.clone(),
            self.secret_key.clone(),
            self.volumes.clone(),
            self.region.clone(),
        )
        .await
        .map_err(|e| ServerError::Init(e.to_string()))?;

        // --- Initialization sequence (mirrors main.rs::run) ---

        init_embedded_runtime_hooks(config.obs_endpoint.clone())
            .await
            .map_err(|e| ServerError::Init(e.to_string()))?;

        let listen_context = init_embedded_startup_listen_context(&config)
            .await
            .map_err(|e| ServerError::Init(e.to_string()))?;

        startup_guard
            .mark_global_init_started()
            .map_err(|_| ServerError::AlreadyStarted)?;

        let endpoint_pools = init_embedded_startup_storage_foundation(&listen_context.server_address, &config.volumes)
            .await
            .map_err(|e| ServerError::Init(e.to_string()))?;

        let http_server = start_embedded_http_server(&config, listen_context.readiness.clone()).await?;
        let shutdown_handle = http_server.shutdown_handle;
        let bound_addr = http_server.bound_addr;
        let ctx = CancellationToken::new();
        let storage_runtime = match init_embedded_startup_storage_runtime(
            listen_context.server_addr,
            &endpoint_pools,
            listen_context.readiness.clone(),
            ctx.clone(),
        )
        .await
        {
            Ok(runtime) => runtime,
            Err(e) => {
                signal_embedded_startup_shutdown(&shutdown_handle, &ctx);
                return Err(ServerError::Init(e.to_string()));
            }
        };
        let store = storage_runtime.store;

        let service_runtime =
            init_embedded_startup_runtime_services(&config, endpoint_pools, store, ctx.clone(), listen_context.readiness.clone())
                .await
                .map_err(|e| {
                    signal_embedded_startup_shutdown(&shutdown_handle, &ctx);
                    ServerError::Init(e.to_string())
                })?;

        publish_embedded_startup_ready(service_runtime.iam_bootstrap, listen_context.readiness.as_ref())
            .await
            .map_err(|e| {
                signal_embedded_startup_shutdown(&shutdown_handle, &ctx);
                ServerError::Init(format!("runtime readiness: {e}"))
            })?;

        let server = RustFSServer {
            address: bound_addr,
            access_key: self.access_key.clone(),
            secret_key: self.secret_key.clone(),
            region: self.region.clone(),
            shutdown_handle: Some(shutdown_handle),
            cancel_token: ctx,
            temp_dir: temp_dir_guard.map(|g| g.keep()),
        };

        log_embedded_server_ready(server.endpoint_address());

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
    shutdown_handle: Option<ShutdownHandle>,
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
        run_embedded_server_shutdown(&self.cancel_token, &mut self.shutdown_handle, self.temp_dir.as_deref()).await;
    }
}

impl Drop for RustFSServer {
    fn drop(&mut self) {
        // Best-effort synchronous cleanup.
        self.cancel_token.cancel();
        if let Some(shutdown_handle) = self.shutdown_handle.take() {
            shutdown_handle.signal();
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
