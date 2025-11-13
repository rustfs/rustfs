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

use crate::{AppConfig, GlobalError, OtelGuard, SystemObserver, telemetry::init_telemetry};
use std::sync::{Arc, Mutex};
use tokio::sync::OnceCell;
use tracing::{error, info};

/// Global guard for OpenTelemetry tracing
static GLOBAL_GUARD: OnceCell<Arc<Mutex<OtelGuard>>> = OnceCell::const_new();

/// Flag indicating if observability metric is enabled
pub(crate) static OBSERVABILITY_METRIC_ENABLED: OnceCell<bool> = OnceCell::const_new();

/// Check whether Observability metric is enabled
pub fn observability_metric_enabled() -> bool {
    OBSERVABILITY_METRIC_ENABLED.get().copied().unwrap_or(false)
}

/// Initialize the observability module
///
/// # Parameters
/// - `config`: Configuration information
///
/// # Returns
/// A tuple containing the logger and the telemetry guard
///
/// # Example
/// ```no_run
/// # use rustfs_obs::init_obs;
///
/// # #[tokio::main]
/// # async fn main() {
/// #    match init_obs(None).await {
/// #         Ok(guard) => {}
/// #         Err(e) => { eprintln!("Failed to initialize observability: {}", e); }
/// #     }
/// # }
/// ```
pub async fn init_obs(endpoint: Option<String>) -> Result<OtelGuard, GlobalError> {
    // Load the configuration file
    let config = AppConfig::new_with_endpoint(endpoint);

    let otel_guard = init_telemetry(&config.observability)?;
    // Server will be created per connection - this ensures isolation
    tokio::spawn(async move {
        // Record the PID-related metrics of the current process
        let obs_result = SystemObserver::init_process_observer().await;
        match obs_result {
            Ok(_) => {
                info!(target: "rustfs::obs::system::metrics","Process observer initialized successfully");
            }
            Err(e) => {
                error!(target: "rustfs::obs::system::metrics","Failed to initialize process observer: {}", e);
            }
        }
    });

    Ok(otel_guard)
}

/// Set the global guard for OpenTelemetry
///
/// # Arguments
/// * `guard` - The OtelGuard instance to set globally
///
/// # Returns
/// * `Ok(())` if successful
/// * `Err(GuardError)` if setting fails
///
/// # Example
/// ```no_run
/// # use rustfs_obs::{ init_obs, set_global_guard};
///
/// # async fn init() -> Result<(), Box<dyn std::error::Error>> {
/// #    let guard = match init_obs(None).await{
/// #         Ok(g) => g,
/// #         Err(e) => { return Err(Box::new(e)); }
/// #    };
/// #    set_global_guard(guard)?;
/// #    Ok(())
/// # }
/// ```
pub fn set_global_guard(guard: OtelGuard) -> Result<(), GlobalError> {
    info!("Initializing global OpenTelemetry guard");
    GLOBAL_GUARD.set(Arc::new(Mutex::new(guard))).map_err(GlobalError::SetError)
}

/// Get the global guard for OpenTelemetry
///
/// # Returns
/// * `Ok(Arc<Mutex<OtelGuard>>)` if guard exists
/// * `Err(GuardError)` if guard not initialized
///
/// # Example
/// ```no_run
/// # use rustfs_obs::get_global_guard;
///
/// # async fn trace_operation() -> Result<(), Box<dyn std::error::Error>> {
/// #    let guard = get_global_guard()?;
/// #    let _lock = guard.lock().unwrap();
/// #    // Perform traced operation
/// #    Ok(())
/// # }
/// ```
pub fn get_global_guard() -> Result<Arc<Mutex<OtelGuard>>, GlobalError> {
    GLOBAL_GUARD.get().cloned().ok_or(GlobalError::NotInitialized)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn test_get_uninitialized_guard() {
        let result = get_global_guard();
        assert!(matches!(result, Err(GlobalError::NotInitialized)));
    }
}
