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

use crate::{
    server::ShutdownHandle,
    startup_lifecycle::{EmbeddedStartupGuard, publish_embedded_startup_ready},
    startup_runtime_hooks::init_embedded_runtime_hooks,
    startup_server::{
        EmbeddedStartupConfig, init_embedded_startup_listen_context, prepare_embedded_startup_config, start_embedded_http_server,
    },
    startup_services::init_embedded_startup_runtime_services,
    startup_shutdown::signal_embedded_startup_shutdown,
    startup_storage::{init_embedded_startup_storage_foundation, init_embedded_startup_storage_runtime},
};
use std::{io, net::SocketAddr, path::PathBuf};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub(crate) struct EmbeddedStartupArgs {
    pub address: String,
    pub access_key: String,
    pub secret_key: String,
    pub volumes: Vec<String>,
    pub region: String,
}

impl EmbeddedStartupArgs {
    pub(crate) fn new_default() -> Self {
        Self {
            address: "127.0.0.1:9000".to_string(),
            access_key: rustfs_credentials::DEFAULT_ACCESS_KEY.to_string(),
            secret_key: rustfs_credentials::DEFAULT_SECRET_KEY.to_string(),
            volumes: Vec::new(),
            region: rustfs_config::RUSTFS_REGION.to_string(),
        }
    }
}

pub(crate) struct EmbeddedStartedServer {
    pub bound_addr: SocketAddr,
    pub shutdown_handle: ShutdownHandle,
    pub cancel_token: CancellationToken,
    pub temp_dir: Option<PathBuf>,
}

#[derive(Debug)]
pub(crate) enum EmbeddedStartupError {
    AlreadyStarted,
    Init(String),
    Io(io::Error),
}

impl From<io::Error> for EmbeddedStartupError {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}

pub(crate) async fn run_embedded_startup(args: EmbeddedStartupArgs) -> Result<EmbeddedStartedServer, EmbeddedStartupError> {
    // Build is allowed to fail before irreversible global initialization
    // (for example on temporary I/O or directory setup errors), and in that
    // case callers can retry.
    let mut startup_guard = EmbeddedStartupGuard::new();

    let EmbeddedStartupConfig { config, temp_dir_guard } =
        prepare_embedded_startup_config(args.address, args.access_key, args.secret_key, args.volumes, args.region)
            .await
            .map_err(init_error)?;

    init_embedded_runtime_hooks(config.obs_endpoint.clone())
        .await
        .map_err(init_error)?;

    let listen_context = init_embedded_startup_listen_context(&config).await.map_err(init_error)?;

    startup_guard
        .mark_global_init_started()
        .map_err(|_| EmbeddedStartupError::AlreadyStarted)?;

    let endpoint_pools = init_embedded_startup_storage_foundation(&listen_context.server_address, &config.volumes)
        .await
        .map_err(init_error)?;

    let http_server = start_embedded_http_server(&config, listen_context.readiness.clone()).await?;
    let shutdown_handle = http_server.shutdown_handle;
    let bound_addr = http_server.bound_addr;
    let cancel_token = CancellationToken::new();

    let storage_runtime = match init_embedded_startup_storage_runtime(
        listen_context.server_addr,
        &endpoint_pools,
        listen_context.readiness.clone(),
        cancel_token.clone(),
    )
    .await
    {
        Ok(runtime) => runtime,
        Err(err) => {
            signal_embedded_startup_shutdown(&shutdown_handle, &cancel_token);
            return Err(init_error(err));
        }
    };

    let service_runtime = init_embedded_startup_runtime_services(
        &config,
        endpoint_pools,
        storage_runtime.store,
        cancel_token.clone(),
        listen_context.readiness.clone(),
    )
    .await
    .map_err(|err| {
        signal_embedded_startup_shutdown(&shutdown_handle, &cancel_token);
        init_error(err)
    })?;

    publish_embedded_startup_ready(service_runtime.iam_bootstrap, listen_context.readiness.as_ref())
        .await
        .map_err(|err| {
            signal_embedded_startup_shutdown(&shutdown_handle, &cancel_token);
            EmbeddedStartupError::Init(format!("runtime readiness: {err}"))
        })?;

    Ok(EmbeddedStartedServer {
        bound_addr,
        shutdown_handle,
        cancel_token,
        temp_dir: temp_dir_guard.map(|guard| guard.keep()),
    })
}

fn init_error(err: impl std::fmt::Display) -> EmbeddedStartupError {
    EmbeddedStartupError::Init(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::EmbeddedStartupArgs;

    #[test]
    fn embedded_startup_args_default_matches_public_builder_defaults() {
        let args = EmbeddedStartupArgs::new_default();

        assert_eq!(args.address, "127.0.0.1:9000");
        assert_eq!(args.access_key, rustfs_credentials::DEFAULT_ACCESS_KEY);
        assert_eq!(args.secret_key, rustfs_credentials::DEFAULT_SECRET_KEY);
        assert_eq!(args.region, rustfs_config::RUSTFS_REGION);
        assert!(args.volumes.is_empty());
    }
}
