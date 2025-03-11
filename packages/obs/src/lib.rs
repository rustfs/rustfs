/// # obs
///
/// `obs` is a logging and observability library for Rust.
/// It provides a simple and easy-to-use interface for logging and observability.
/// It is built on top of the `log` crate and `opentelemetry` crate.
mod config;
mod entry;
mod logger;
mod sink;
mod telemetry;
mod utils;
mod worker;

pub use config::load_config;
pub use config::{AppConfig, OtelConfig};
pub use entry::{LogEntry, SerializableLevel};
pub use logger::start_logger;
pub use logger::{LogError, Logger};
pub use sink::Sink;
pub use telemetry::init_telemetry;
pub use utils::{get_local_ip, get_local_ip_with_default};
pub use worker::start_worker;

/// Log module initialization function
///
/// Return to Logger and Clean Guard
pub fn init_logging(config: AppConfig) -> (Logger, telemetry::OtelGuard) {
    let guard = init_telemetry(&config.observability);
    let sinks = sink::create_sinks(&config);
    let logger = start_logger(&config, sinks);
    (logger, guard)
}
