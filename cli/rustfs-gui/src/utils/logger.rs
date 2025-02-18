use dioxus::logger::tracing::debug;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::fmt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// Initialize the logger with a rolling file appender
/// that rotates log files daily
pub fn init_logger() -> WorkerGuard {
    // configuring rolling logs rolling by day
    let home_dir = dirs::home_dir().expect("无法获取用户目录");
    let rustfs_dir = home_dir.join("rustfs");
    let logs_dir = rustfs_dir.join("logs");
    let file_appender = RollingFileAppender::builder()
        .rotation(Rotation::DAILY) // rotate log files once every hour
        .filename_prefix("rustfs-cli") // log file names will be prefixed with `myapp.`
        .filename_suffix("log") // log file names will be suffixed with `.log`
        .build(logs_dir) // try to build an appender that stores log files in `/ var/ log`
        .expect("initializing rolling file appender failed");
    // non-blocking writer for improved performance
    let (non_blocking_file, worker_guard) = tracing_appender::non_blocking(file_appender);

    // console output layer
    let console_layer = fmt::layer()
        .with_writer(std::io::stdout)
        .with_ansi(true)
        .with_line_number(true); // enable colors in the console

    // file output layer
    let file_layer = fmt::layer()
        .with_writer(non_blocking_file)
        .with_ansi(false)
        .with_thread_names(true)
        .with_target(true)
        .with_thread_ids(true)
        .with_level(true)
        .with_line_number(true); // disable colors in the file

    // Combine all tiers and initialize global subscribers
    tracing_subscriber::registry()
        .with(console_layer)
        .with(file_layer)
        .with(tracing_subscriber::EnvFilter::new("info")) // filter the log level by environment variables
        .init();
    debug!("Logger initialized");
    worker_guard
}
