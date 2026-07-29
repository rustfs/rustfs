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

//! TFTP server entry point implementation.
//!
//! Owns the async-tftp server and translates TFTP read/write requests
//! into S3 GetObject / PutObject calls via the StorageBackend trait.
//!
//! Architecture. Each TFTP RRQ triggers a full object download from S3
//! into an in-memory buffer, which is then served to the TFTP client via
//! a Cursor<Vec<u8>>. Each WRQ accumulates bytes into an in-memory buffer;
//! on transfer completion the buffer is uploaded to S3 in one PutObject
//! call.
//!
//! Authentication. TFTP has no built-in authentication. To compensate,
//! the server supports authenticating as a specific access_key, three
//! access modes (read-only, write-only, read-write), and restricting
//! access to a single bucket.

use super::config::{TftpConfig, TftpInitError};
use super::handler::TftpdHandler;
use crate::common::client::s3::StorageBackend;
use async_tftp::server::TftpServerBuilder;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::info;

const LOG_COMPONENT_PROTOCOLS: &str = "protocols";
const LOG_SUBSYSTEM_TFTP_SERVER: &str = "tftp_server";
const EVENT_TFTP_SERVER_STATE: &str = "tftp_server_state";

// ---------------------------------------------------------------------------
// TftpServer — public entry point
// ---------------------------------------------------------------------------

/// TFTP server that binds a UDP socket and dispatches RRQ/WRQ requests
/// onto the supplied StorageBackend.
pub struct TftpServer<S: StorageBackend + Send + Sync + 'static> {
    config: TftpConfig,
    storage: Arc<S>,
}

impl<S: StorageBackend + Send + Sync + 'static + Debug> TftpServer<S> {
    /// Create a new TFTP server.
    pub fn new(config: TftpConfig, storage: Arc<S>) -> Self {
        TftpServer { config, storage }
    }

    /// Start the TFTP server. This call blocks until the server shuts down.
    pub async fn start(self, mut shutdown_rx: broadcast::Receiver<()>) -> Result<(), TftpInitError> {
        info!(
            event = EVENT_TFTP_SERVER_STATE,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_TFTP_SERVER,
            addr = %self.config.bind_addr,
            "Starting TFTP server"
        );

        let handler = TftpdHandler::new(&self.config, Arc::clone(&self.storage));

        let tftpd = TftpServerBuilder::with_handler(handler)
            .bind(self.config.bind_addr)
            .build()
            .await?;

        info!(
            event = EVENT_TFTP_SERVER_STATE,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_TFTP_SERVER,
            addr = %tftpd.listen_addr()?,
            "TFTP server listening"
        );

        tokio::select! {
            _ = tftpd.serve() => {
                info!(
                    event = EVENT_TFTP_SERVER_STATE,
                    component = LOG_COMPONENT_PROTOCOLS,
                    subsystem = LOG_SUBSYSTEM_TFTP_SERVER,
                    "TFTP server stopped"
                );
            }
            _ = shutdown_rx.recv() => {
                info!(
                    event = EVENT_TFTP_SERVER_STATE,
                    component = LOG_COMPONENT_PROTOCOLS,
                    subsystem = LOG_SUBSYSTEM_TFTP_SERVER,
                    "TFTP server received shutdown signal"
                );
            }
        }
        // The server will run indefinitely until externally stopped.
        Ok(())
    }
}
