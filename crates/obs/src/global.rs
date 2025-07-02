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

use crate::logger::InitLogStatus;
use crate::telemetry::{OtelGuard, init_telemetry};
use crate::{AppConfig, Logger, get_global_logger, init_global_logger};
use std::sync::{Arc, Mutex};
use tokio::sync::{OnceCell, SetError};
use tracing::{error, info};

/// Global guard for OpenTelemetry tracing
static GLOBAL_GUARD: OnceCell<Arc<Mutex<OtelGuard>>> = OnceCell::const_new();

/// Error type for global guard operations
#[derive(Debug, thiserror::Error)]
pub enum GlobalError {
    #[error("Failed to set global guard: {0}")]
    SetError(#[from] SetError<Arc<Mutex<OtelGuard>>>),
    #[error("Global guard not initialized")]
    NotInitialized,
    #[error("Global system metrics err: {0}")]
    MetricsError(String),
    #[error("Failed to get current PID: {0}")]
    PidError(String),
    #[error("Process with PID {0} not found")]
    ProcessNotFound(u32),
    #[error("Failed to get physical core count")]
    CoreCountError,
    #[error("GPU initialization failed: {0}")]
    GpuInitError(String),
    #[error("GPU device not found: {0}")]
    GpuDeviceError(String),
    #[error("Failed to send log: {0}")]
    SendFailed(&'static str),
    #[error("Operation timed out: {0}")]
    Timeout(&'static str),
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
/// use rustfs_obs::init_obs;
///
/// # #[tokio::main]
/// # async fn main() {
/// let (logger, guard) = init_obs(None).await;
/// # }
/// ```
pub async fn init_obs(endpoint: Option<String>) -> (Arc<tokio::sync::Mutex<Logger>>, OtelGuard) {
    // Load the configuration file
    let config = AppConfig::new_with_endpoint(endpoint);

    let guard = init_telemetry(&config.observability);

    let logger = init_global_logger(&config).await;
    let obs_config = config.observability.clone();
    tokio::spawn(async move {
        let result = InitLogStatus::init_start_log(&obs_config).await;
        match result {
            Ok(_) => {
                info!("Logger initialized successfully");
            }
            Err(e) => {
                error!("Failed to initialize logger: {}", e);
            }
        }
    });

    (logger, guard)
}

/// Get the global logger instance
/// This function returns a reference to the global logger instance.
///
/// # Returns
/// A reference to the global logger instance
///
/// # Example
/// ```no_run
/// use rustfs_obs::get_logger;
///
/// let logger = get_logger();
/// ```
pub fn get_logger() -> &'static Arc<tokio::sync::Mutex<Logger>> {
    get_global_logger()
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
/// ```rust
/// use rustfs_obs::{ init_obs, set_global_guard};
///
/// async fn init() -> Result<(), Box<dyn std::error::Error>> {
///     let (_, guard) = init_obs(None).await;
///     set_global_guard(guard)?;
///     Ok(())
/// }
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
/// ```rust
/// use rustfs_obs::get_global_guard;
///
/// async fn trace_operation() -> Result<(), Box<dyn std::error::Error>> {
///     let guard = get_global_guard()?;
///     let _lock = guard.lock().unwrap();
///     // Perform traced operation
///     Ok(())
/// }
/// ```
pub fn get_global_guard() -> Result<Arc<Mutex<OtelGuard>>, GlobalError> {
    GLOBAL_GUARD.get().cloned().ok_or(GlobalError::NotInitialized)
}

/// Try to get the global guard for OpenTelemetry
///
/// # Returns
/// * `Some(Arc<Mutex<OtelGuard>>)` if guard exists
/// * `None` if guard not initialized
pub fn try_get_global_guard() -> Option<Arc<Mutex<OtelGuard>>> {
    GLOBAL_GUARD.get().cloned()
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
