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

//! TFTP UDP server lifecycle.

use super::config::{TftpConfig, TftpInitError};
use super::handler::TftpStorageHandler;
use crate::common::client::s3::StorageBackend;
use async_tftp::server::TftpServerBuilder;
use std::fmt::Debug;
use std::time::Duration;
use tokio::sync::broadcast;
use tracing::{debug, error, info};

const LOG_COMPONENT_PROTOCOLS: &str = "protocols";
const LOG_SUBSYSTEM_TFTP_SERVER: &str = "tftp_server";
const EVENT_TFTP_SERVER_STATE: &str = "tftp_server_state";

/// TFTP server entry point.
pub struct TftpServer<S>
where
    S: StorageBackend + Send + Sync + 'static + Debug,
{
    config: TftpConfig,
    handler: TftpStorageHandler<S>,
}

impl<S> TftpServer<S>
where
    S: StorageBackend + Send + Sync + 'static + Debug,
{
    pub fn new(config: TftpConfig, handler: TftpStorageHandler<S>) -> Self {
        Self { config, handler }
    }

    pub async fn start(self, mut shutdown_rx: broadcast::Receiver<()>) -> Result<(), TftpInitError> {
        let bind_addr = self.config.bind_addr;
        let max_block_size = self.config.max_block_size;
        let max_window_size = self.config.max_window_size;
        let max_send_retries = self.config.max_send_retries;

        let tftpd = TftpServerBuilder::with_handler(self.handler)
            .bind(bind_addr)
            .block_size_limit(max_block_size)
            .window_size_limit(max_window_size)
            .timeout(Duration::from_secs(3))
            .max_send_retries(max_send_retries)
            .build()
            .await
            .map_err(|e| TftpInitError::InvalidConfig(format!("TFTP server build failed: {e}")))?;

        info!(
            event = EVENT_TFTP_SERVER_STATE,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_TFTP_SERVER,
            state = "listening",
            bind_addr = %bind_addr,
            "tftp server state changed"
        );

        tokio::select! {
            result = tftpd.serve() => {
                match result {
                    Ok(()) => {
                        debug!(
                            event = EVENT_TFTP_SERVER_STATE,
                            component = LOG_COMPONENT_PROTOCOLS,
                            subsystem = LOG_SUBSYSTEM_TFTP_SERVER,
                            state = "stopped",
                            "tftp server state changed"
                        );
                        Ok(())
                    }
                    Err(e) => {
                        error!(
                            event = EVENT_TFTP_SERVER_STATE,
                            component = LOG_COMPONENT_PROTOCOLS,
                            subsystem = LOG_SUBSYSTEM_TFTP_SERVER,
                            state = "runtime_failed",
                            error = %e,
                            "tftp server state changed"
                        );
                        Err(TftpInitError::InvalidConfig(format!("TFTP serve failed: {e}")))
                    }
                }
            }
            _ = shutdown_rx.recv() => {
                debug!(
                    event = EVENT_TFTP_SERVER_STATE,
                    component = LOG_COMPONENT_PROTOCOLS,
                    subsystem = LOG_SUBSYSTEM_TFTP_SERVER,
                    state = "shutdown_signal",
                    "tftp server state changed"
                );
                Ok(())
            }
        }
    }
}
