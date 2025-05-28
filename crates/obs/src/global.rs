use crate::telemetry::OtelGuard;
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
/// use rustfs_obs::{init_telemetry, load_config, set_global_guard};
///
/// fn init() -> Result<(), Box<dyn std::error::Error>> {
///     let config = load_config(None);
///     let guard = init_telemetry(&config.observability);
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
