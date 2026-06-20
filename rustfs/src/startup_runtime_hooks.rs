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

use crate::license::license_status;
use rustls::crypto::aws_lc_rs::default_provider;
use std::future::Future;
use tracing::{debug, info};

const LOG_COMPONENT_MAIN: &str = "main";
const LOG_SUBSYSTEM_LICENSE: &str = "license";
const LOG_SUBSYSTEM_STARTUP: &str = "startup";
const EVENT_CRYPTO_PROVIDER_STATE: &str = "crypto_provider_state";
const EVENT_DIAL9_RUNTIME_STATUS: &str = "dial9_runtime_status";
const EVENT_RUNTIME_LICENSE_STATUS: &str = "runtime_license_status";

pub fn log_startup_runtime_diagnostics() {
    log_dial9_runtime_status();
    log_runtime_license_status();
    debug!("{}", crate::server::LOGO);
}

fn log_dial9_runtime_status() {
    if rustfs_obs::dial9::is_enabled() {
        info!(
            target: "rustfs::main",
            event = EVENT_DIAL9_RUNTIME_STATUS,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            enabled = true,
            "Dial9 Tokio runtime telemetry is enabled"
        );
    } else {
        debug!(
            target: "rustfs::main",
            event = EVENT_DIAL9_RUNTIME_STATUS,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            enabled = false,
            "Dial9 Tokio runtime telemetry is disabled"
        );
    }
}

fn log_runtime_license_status() {
    info!(
        target: "rustfs::main",
        event = EVENT_RUNTIME_LICENSE_STATUS,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_LICENSE,
        license_status = %license_status(),
        "Initialized runtime license state"
    );
}

pub async fn init_profiling_runtime() {
    init_profiling_runtime_with(crate::profiling::init_from_env).await;
}

async fn init_profiling_runtime_with<InitFn, InitFuture>(init: InitFn)
where
    InitFn: FnOnce() -> InitFuture,
    InitFuture: Future<Output = ()>,
{
    init().await;
}

pub fn shutdown_profiling_runtime() {
    shutdown_profiling_runtime_with(crate::profiling::shutdown_profiling);
}

fn shutdown_profiling_runtime_with<ShutdownFn>(shutdown: ShutdownFn)
where
    ShutdownFn: FnOnce(),
{
    shutdown();
}

pub fn install_default_crypto_provider() {
    if default_provider().install_default().is_err() {
        debug!(
            target: "rustfs::main",
            event = EVENT_CRYPTO_PROVIDER_STATE,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            provider = "aws_lc_rs",
            state = "already_installed",
            "Rustls crypto provider state checked"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::{init_profiling_runtime_with, shutdown_profiling_runtime_with};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    #[tokio::test]
    async fn init_profiling_runtime_invokes_registered_hook() {
        let called = Arc::new(AtomicBool::new(false));
        let hook_called = called.clone();

        init_profiling_runtime_with(move || async move {
            hook_called.store(true, Ordering::SeqCst);
        })
        .await;

        assert!(called.load(Ordering::SeqCst));
    }

    #[test]
    fn shutdown_profiling_runtime_invokes_registered_hook() {
        let called = AtomicBool::new(false);

        shutdown_profiling_runtime_with(|| {
            called.store(true, Ordering::SeqCst);
        });

        assert!(called.load(Ordering::SeqCst));
    }
}
