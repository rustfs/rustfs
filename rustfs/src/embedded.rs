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
use crate::startup_embedded::{EmbeddedStartupArgs, EmbeddedStartupError, run_embedded_startup};
use crate::startup_lifecycle::{embedded_endpoint_address, log_embedded_server_ready};
use crate::startup_server::find_embedded_available_port;
use crate::startup_shutdown::{run_embedded_server_drop_cleanup, run_embedded_server_shutdown};
use std::net::SocketAddr;
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

impl From<EmbeddedStartupError> for ServerError {
    fn from(err: EmbeddedStartupError) -> Self {
        match err {
            EmbeddedStartupError::AlreadyStarted => ServerError::AlreadyStarted,
            EmbeddedStartupError::Init(message) => ServerError::Init(message),
            EmbeddedStartupError::Io(err) => ServerError::Io(err),
        }
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
    startup_args: EmbeddedStartupArgs,
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
            startup_args: EmbeddedStartupArgs::new_default(),
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
        self.startup_args.address = addr.into();
        self
    }

    /// Set the S3 access key (default: `"rustfsadmin"`).
    pub fn access_key(mut self, key: impl Into<String>) -> Self {
        self.startup_args.access_key = key.into();
        self
    }

    /// Set the S3 secret key (default: `"rustfsadmin"`).
    pub fn secret_key(mut self, key: impl Into<String>) -> Self {
        self.startup_args.secret_key = key.into();
        self
    }

    /// Set the AWS region (default: `"us-east-1"`).
    pub fn region(mut self, region: impl Into<String>) -> Self {
        self.startup_args.region = region.into();
        self
    }

    /// Add a data volume path.
    ///
    /// If no volumes are added, a temporary directory with a single drive is
    /// created automatically (and cleaned up on [`RustFSServer::shutdown`]).
    pub fn volume(mut self, path: impl Into<String>) -> Self {
        self.startup_args.volumes.push(path.into());
        self
    }

    /// Set multiple volume paths at once, replacing any previously set volumes.
    pub fn volumes(mut self, paths: Vec<String>) -> Self {
        self.startup_args.volumes = paths;
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

    async fn do_build(&mut self) -> Result<RustFSServer, ServerError> {
        let startup_args = self.startup_args.clone();
        let access_key = startup_args.access_key.clone();
        let secret_key = startup_args.secret_key.clone();
        let region = startup_args.region.clone();
        let started = run_embedded_startup(startup_args).await?;

        let server = RustFSServer {
            address: started.bound_addr,
            access_key,
            secret_key,
            region,
            shutdown_handle: Some(started.shutdown_handle),
            cancel_token: started.cancel_token,
            temp_dir: started.temp_dir,
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
        embedded_endpoint_address(self.address)
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
        run_embedded_server_drop_cleanup(&self.cancel_token, &mut self.shutdown_handle, self.temp_dir.as_deref());
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
    find_embedded_available_port()
}
