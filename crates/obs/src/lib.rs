//! # RustFS Observability
//!
//! provides tools for system and service monitoring
//!
//! ## feature mark
//!
//! - `file`: enable file logging enabled by default
//! - `gpu`: gpu monitoring function
//! - `kafka`: enable kafka metric output
//! - `webhook`: enable webhook notifications
//! - `full`: includes all functions
//!
//! to enable gpu monitoring add in cargo toml
//!
//! ```toml
//! # using gpu monitoring
//! rustfs-obs = { version = "0.1.0", features = ["gpu"] }
//!
//! # use all functions
//! rustfs-obs = { version = "0.1.0", features = ["full"] }
//! ```
///
/// ## Usage
///
/// ```rust
/// use rustfs_obs::{AppConfig, init_obs};
///
/// let config = AppConfig::default();
/// let (logger, guard) = init_obs(config);
/// ```
mod config;
mod entry;
mod global;
mod logger;
mod metrics;
mod sink;
mod telemetry;
mod utils;
mod worker;

#[cfg(feature = "gpu")]
pub use crate::metrics::init_gpu_metrics;

pub use config::load_config;
pub use config::{AppConfig, OtelConfig};
pub use entry::args::Args;
pub use entry::audit::{ApiDetails, AuditLogEntry};
pub use entry::base::BaseLogEntry;
pub use entry::unified::{ConsoleLogEntry, ServerLogEntry, UnifiedLogEntry};
pub use entry::{LogKind, LogRecord, ObjectVersion, SerializableLevel};
pub use global::{get_global_guard, set_global_guard, try_get_global_guard, GlobalError};
pub use logger::{ensure_logger_initialized, log_debug, log_error, log_info, log_trace, log_warn, log_with_context};
pub use logger::{get_global_logger, init_global_logger, locked_logger, start_logger};
pub use logger::{log_init_state, InitLogStatus};
pub use logger::{LogError, Logger};
pub use metrics::{init_system_metrics, init_system_metrics_for_pid};
pub use sink::Sink;
use std::sync::Arc;
pub use telemetry::init_telemetry;
use tokio::sync::Mutex;
pub use utils::{get_local_ip, get_local_ip_with_default};
pub use worker::start_worker;

/// Initialize the observability module
///
/// # Parameters
/// - `config`: Configuration information
///
/// # Returns
/// A tuple containing the logger and the telemetry guard
///
/// # Example
/// ```
/// use rustfs_obs::{AppConfig, init_obs};
///
/// let config = AppConfig::default();
/// let (logger, guard) = init_obs(config);
/// ```
pub async fn init_obs(config: AppConfig) -> (Arc<Mutex<Logger>>, telemetry::OtelGuard) {
    let guard = init_telemetry(&config.observability);
    let sinks = sink::create_sinks(&config).await;
    let logger = init_global_logger(&config, sinks).await;
    (logger, guard)
}

/// Get the global logger instance
/// This function returns a reference to the global logger instance.
///
/// # Returns
/// A reference to the global logger instance
///
/// # Example
/// ```
/// use rustfs_obs::get_logger;
///
/// let logger = get_logger();
/// ```
pub fn get_logger() -> &'static Arc<Mutex<Logger>> {
    get_global_logger()
}
