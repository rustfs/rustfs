use crate::telemetry::OtelGuard;
use std::sync::{Arc, Mutex};
use tokio::sync::{OnceCell, SetError};
use tracing::{error, info};

pub(crate) const USE_STDOUT: bool = true;
pub(crate) const SERVICE_NAME: &str = "RustFS";
pub(crate) const SAMPLE_RATIO: f64 = 1.0;
pub(crate) const METER_INTERVAL: u64 = 60;
pub(crate) const SERVICE_VERSION: &str = "0.1.0";
pub(crate) const ENVIRONMENT: &str = "development";
pub(crate) const LOGGER_LEVEL: &str = "info";

/// Global guard for OpenTelemetry tracing
static GLOBAL_GUARD: OnceCell<Arc<Mutex<OtelGuard>>> = OnceCell::const_new();

/// Error type for global guard operations
#[derive(Debug, thiserror::Error)]
pub enum GuardError {
    #[error("Failed to set global guard: {0}")]
    SetError(#[from] SetError<Arc<Mutex<OtelGuard>>>),
    #[error("Global guard not initialized")]
    NotInitialized,
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
/// async fn init() -> Result<(), Box<dyn std::error::Error>> {
///     let config = load_config(None);
///     let guard = init_telemetry(&config.observability).await?;
///     set_global_guard(guard)?;
///     Ok(())
/// }
/// ```
pub fn set_global_guard(guard: OtelGuard) -> Result<(), GuardError> {
    info!("Initializing global OpenTelemetry guard");
    GLOBAL_GUARD.set(Arc::new(Mutex::new(guard))).map_err(GuardError::SetError)
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
pub fn get_global_guard() -> Result<Arc<Mutex<OtelGuard>>, GuardError> {
    GLOBAL_GUARD.get().cloned().ok_or(GuardError::NotInitialized)
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
        assert!(matches!(result, Err(GuardError::NotInitialized)));
    }
}
