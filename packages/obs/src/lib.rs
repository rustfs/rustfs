/// # obs
///
/// `obs` is a logging and observability library for Rust.
/// It provides a simple and easy-to-use interface for logging and observability.
/// It is built on top of the `log` crate and `opentelemetry` crate.
///
/// ## Features
/// - Structured logging
/// - Distributed tracing
/// - Metrics collection
/// - Log processing worker
/// - Multiple sinks
/// - Configuration-based setup
/// - Telemetry guard
/// - Global logger
/// - Log levels
/// - Log entry types
/// - Log record
/// - Object version
/// - Local IP address
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
mod sink;
mod telemetry;
mod utils;
mod worker;
pub use config::load_config;
pub use config::{AppConfig, OtelConfig};
pub use entry::args::Args;
pub use entry::audit::{ApiDetails, AuditLogEntry};
pub use entry::base::BaseLogEntry;
pub use entry::unified::{ConsoleLogEntry, ServerLogEntry, UnifiedLogEntry};
pub use entry::{LogKind, LogRecord, ObjectVersion, SerializableLevel};
pub use global::{get_global_guard, set_global_guard, try_get_global_guard, GuardError};
pub use logger::{ensure_logger_initialized, log_debug, log_error, log_info, log_trace, log_warn, log_with_context};
pub use logger::{get_global_logger, init_global_logger, locked_logger, start_logger};
pub use logger::{log_init_state, InitLogStatus};
pub use logger::{LogError, Logger};
pub use sink::Sink;
use std::sync::Arc;
pub use telemetry::init_telemetry;
pub use telemetry::{get_global_registry, metrics};
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
