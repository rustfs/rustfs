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

#[cfg(feature = "sftp")]
use crate::init::init_sftp_system;
#[cfg(feature = "webdav")]
use crate::init::init_webdav_system;
#[cfg(feature = "ftps")]
use crate::init::{init_ftp_system, init_ftps_system};
use crate::server::ShutdownHandle;
use std::future::Future;
use std::io::{Error, Result};
use tracing::{debug, error, info};

const LOG_COMPONENT_MAIN: &str = "main";
const LOG_SUBSYSTEM_STARTUP: &str = "startup";
const EVENT_PROTOCOL_SYSTEM_STATE: &str = "protocol_system_state";

type ProtocolInitResult = std::result::Result<Option<ShutdownHandle>, Box<dyn std::error::Error + Send + Sync>>;

/// Shutdown channels for every protocol server. None means the protocol was
/// disabled at startup or not compiled in.
pub struct ProtocolShutdownSenders {
    pub ftp: Option<ShutdownHandle>,
    pub ftps: Option<ShutdownHandle>,
    pub webdav: Option<ShutdownHandle>,
    pub sftp: Option<ShutdownHandle>,
}

pub async fn init_protocol_shutdown_senders() -> Result<ProtocolShutdownSenders> {
    Ok(ProtocolShutdownSenders {
        ftp: init_ftp_protocol().await?,
        ftps: init_ftps_protocol().await?,
        webdav: init_webdav_protocol().await?,
        sftp: init_sftp_protocol().await?,
    })
}

#[cfg(feature = "ftps")]
async fn init_ftp_protocol() -> Result<Option<ShutdownHandle>> {
    init_protocol("ftp", init_ftp_system).await
}

#[cfg(not(feature = "ftps"))]
async fn init_ftp_protocol() -> Result<Option<ShutdownHandle>> {
    Ok(None)
}

#[cfg(feature = "ftps")]
async fn init_ftps_protocol() -> Result<Option<ShutdownHandle>> {
    init_protocol("ftps", init_ftps_system).await
}

#[cfg(not(feature = "ftps"))]
async fn init_ftps_protocol() -> Result<Option<ShutdownHandle>> {
    Ok(None)
}

#[cfg(feature = "webdav")]
async fn init_webdav_protocol() -> Result<Option<ShutdownHandle>> {
    init_protocol("webdav", init_webdav_system).await
}

#[cfg(not(feature = "webdav"))]
async fn init_webdav_protocol() -> Result<Option<ShutdownHandle>> {
    Ok(None)
}

#[cfg(feature = "sftp")]
async fn init_sftp_protocol() -> Result<Option<ShutdownHandle>> {
    init_protocol("sftp", init_sftp_system).await
}

#[cfg(not(feature = "sftp"))]
async fn init_sftp_protocol() -> Result<Option<ShutdownHandle>> {
    Ok(None)
}

async fn init_protocol<InitFn, InitFuture>(protocol: &'static str, init: InitFn) -> Result<Option<ShutdownHandle>>
where
    InitFn: FnOnce() -> InitFuture,
    InitFuture: Future<Output = ProtocolInitResult>,
{
    match init().await {
        Ok(Some(tx)) => {
            debug!(
                event = EVENT_PROTOCOL_SYSTEM_STATE,
                component = LOG_COMPONENT_MAIN,
                subsystem = LOG_SUBSYSTEM_STARTUP,
                protocol = protocol,
                state = "started",
                "Protocol runtime started"
            );
            Ok(Some(tx))
        }
        Ok(None) => {
            info!(
                event = EVENT_PROTOCOL_SYSTEM_STATE,
                component = LOG_COMPONENT_MAIN,
                subsystem = LOG_SUBSYSTEM_STARTUP,
                protocol = protocol,
                state = "disabled",
                "Protocol runtime disabled"
            );
            Ok(None)
        }
        Err(err) => {
            error!(
                event = EVENT_PROTOCOL_SYSTEM_STATE,
                component = LOG_COMPONENT_MAIN,
                subsystem = LOG_SUBSYSTEM_STARTUP,
                protocol = protocol,
                state = "initialization_failed",
                error = %err,
                "Protocol runtime initialization failed"
            );
            Err(Error::other(err))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::init_protocol;

    #[tokio::test]
    async fn init_protocol_returns_none_when_disabled() {
        let shutdown = init_protocol("test", || async { Ok(None) }).await;

        assert!(matches!(shutdown, Ok(None)));
    }

    #[tokio::test]
    async fn init_protocol_maps_startup_error() {
        let shutdown = init_protocol("test", || async {
            Err(Box::<dyn std::error::Error + Send + Sync>::from("startup failed"))
        })
        .await;

        assert!(shutdown.is_err());
    }
}
