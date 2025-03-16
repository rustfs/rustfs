use crate::{AppConfig, LogEntry, SerializableLevel, Sink};
use std::sync::Arc;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::{Mutex, OnceCell};
use tracing_core::Level;

// Add the global instance at the module level
static GLOBAL_LOGGER: OnceCell<Arc<Mutex<Logger>>> = OnceCell::const_new();

/// Server log processor
#[derive(Debug)]
pub struct Logger {
    sender: Sender<LogEntry>, // Log sending channel
    queue_capacity: usize,
}

impl Logger {
    /// Create a new Logger instance
    /// Returns Logger and corresponding Receiver
    pub fn new(config: &AppConfig) -> (Self, Receiver<LogEntry>) {
        // Get queue capacity from configuration, or use default values 10000
        let queue_capacity = config.logger.queue_capacity.unwrap_or(10000);
        let (sender, receiver) = mpsc::channel(queue_capacity);
        (Logger { sender, queue_capacity }, receiver)
    }

    /// get the queue capacity
    /// This function returns the queue capacity.
    /// # Returns
    /// The queue capacity
    /// # Example
    /// ```
    /// use rustfs_obs::Logger;
    /// async fn example(logger: &Logger) {
    ///    let _ = logger.get_queue_capacity();
    /// }
    /// ```
    pub fn get_queue_capacity(&self) -> usize {
        self.queue_capacity
    }

    /// Asynchronous logging of server logs
    /// Attach the log to the current Span and generate a separate Tracing Event
    #[tracing::instrument(skip(self), fields(log_source = "logger"))]
    pub async fn log(&self, entry: LogEntry) -> Result<(), LogError> {
        // Log messages to the current Span
        tracing::Span::current()
            .record("log_message", &entry.message)
            .record("source", &entry.source);
        println!("target start is {:?}", &entry.target);
        let target = if let Some(target) = &entry.target {
            target.clone()
        } else {
            "server_logs".to_string()
        };

        println!("target end is {:?}", target);
        // Generate independent Tracing Events with full LogEntry information
        // Generate corresponding events according to level
        match entry.level {
            SerializableLevel(Level::ERROR) => {
                tracing::error!(
                    target = %target.clone(),
                    timestamp = %entry.timestamp,
                    message = %entry.message,
                    source = %entry.source,
                    request_id = ?entry.request_id,
                    user_id = ?entry.user_id,
                    fields = ?entry.fields
                );
            }
            SerializableLevel(Level::WARN) => {
                tracing::warn!(
                    target = %target.clone(),
                    timestamp = %entry.timestamp,
                    message = %entry.message,
                    source = %entry.source,
                    request_id = ?entry.request_id,
                    user_id = ?entry.user_id,
                    fields = ?entry.fields
                );
            }
            SerializableLevel(Level::INFO) => {
                tracing::info!(
                    target = %target.clone(),
                    timestamp = %entry.timestamp,
                    message = %entry.message,
                    source = %entry.source,
                    request_id = ?entry.request_id,
                    user_id = ?entry.user_id,
                    fields = ?entry.fields
                );
            }
            SerializableLevel(Level::DEBUG) => {
                tracing::debug!(
                    target = %target.clone(),
                    timestamp = %entry.timestamp,
                    message = %entry.message,
                    source = %entry.source,
                    request_id = ?entry.request_id,
                    user_id = ?entry.user_id,
                    fields = ?entry.fields
                );
            }
            SerializableLevel(Level::TRACE) => {
                tracing::trace!(
                    target = %target.clone(),
                    timestamp = %entry.timestamp,
                    message = %entry.message,
                    source = %entry.source,
                    request_id = ?entry.request_id,
                    user_id = ?entry.user_id,
                    fields = ?entry.fields
                );
            }
        }

        // Send logs to asynchronous queues to improve error handling
        match self.sender.try_send(entry) {
            Ok(_) => Ok(()),
            Err(mpsc::error::TrySendError::Full(entry)) => {
                // Processing strategy when queue is full
                tracing::warn!("Log queue full, applying backpressure");
                match tokio::time::timeout(std::time::Duration::from_millis(500), self.sender.send(entry)).await {
                    Ok(Ok(_)) => Ok(()),
                    Ok(Err(_)) => Err(LogError::SendFailed("Channel closed")),
                    Err(_) => Err(LogError::Timeout("Queue backpressure timeout")),
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => Err(LogError::SendFailed("Logger channel closed")),
        }
    }

    /// Write log with context information
    /// This function writes log messages with context information.
    ///
    /// # Parameters
    /// - `message`: Message to be logged
    /// - `source`: Source of the log
    /// - `request_id`: Request ID
    /// - `user_id`: User ID
    /// - `fields`: Additional fields
    ///
    /// # Returns
    /// Result indicating whether the operation was successful
    ///
    /// # Example
    /// ```
    /// use tracing_core::Level;
    /// use rustfs_obs::Logger;
    ///
    /// async fn example(logger: &Logger) {
    ///    let _ = logger.write_with_context("This is an information message", "example",Level::INFO, Some("target".to_string()),Some("req-12345".to_string()), Some("user-6789".to_string()), vec![("endpoint".to_string(), "/api/v1/data".to_string())]).await;
    /// }
    pub async fn write_with_context(
        &self,
        message: &str,
        source: &str,
        level: Level,
        target: Option<String>,
        request_id: Option<String>,
        user_id: Option<String>,
        fields: Vec<(String, String)>,
    ) -> Result<(), LogError> {
        self.log(LogEntry::new(
            level,
            message.to_string(),
            source.to_string(),
            target,
            request_id,
            user_id,
            fields,
        ))
        .await
    }

    /// Write log
    /// This function writes log messages.
    /// # Parameters
    /// - `message`: Message to be logged
    /// - `source`: Source of the log
    /// - `level`: Log level
    ///
    /// # Returns
    /// Result indicating whether the operation was successful
    ///
    /// # Example
    /// ```
    /// use rustfs_obs::Logger;
    /// use tracing_core::Level;
    ///
    /// async fn example(logger: &Logger) {
    ///   let _ = logger.write("This is an information message", "example", Level::INFO).await;
    /// }
    /// ```
    pub async fn write(&self, message: &str, source: &str, level: Level) -> Result<(), LogError> {
        self.log(LogEntry::new(
            level,
            message.to_string(),
            source.to_string(),
            None,
            None,
            None,
            Vec::new(),
        ))
        .await
    }

    /// Shutdown the logger
    /// This function shuts down the logger.
    ///
    /// # Returns
    /// Result indicating whether the operation was successful
    ///
    /// # Example
    /// ```
    /// use rustfs_obs::Logger;
    ///
    /// async fn example(logger: Logger) {
    ///  let _ = logger.shutdown().await;
    /// }
    /// ```
    pub async fn shutdown(self) -> Result<(), LogError> {
        drop(self.sender); //Close the sending end so that the receiver knows that there is no new message
        Ok(())
    }
}

/// Log error type
/// This enum defines the error types that can occur when logging.
/// It is used to provide more detailed error information.
/// # Example
/// ```
/// use rustfs_obs::LogError;
/// use thiserror::Error;
///
/// LogError::SendFailed("Failed to send log");
/// LogError::Timeout("Operation timed out");
/// ```
#[derive(Debug, thiserror::Error)]
pub enum LogError {
    #[error("Failed to send log: {0}")]
    SendFailed(&'static str),
    #[error("Operation timed out: {0}")]
    Timeout(&'static str),
}

/// Start the log module
/// This function starts the log module.
/// It initializes the logger and starts the worker to process logs.
/// # Parameters
/// - `config`: Configuration information
/// - `sinks`: A vector of Sink instances
/// # Returns
/// The global logger instance
/// # Example
/// ```
/// use rustfs_obs::{AppConfig, start_logger};
///
/// let config = AppConfig::default();
/// let sinks = vec![];
/// let logger = start_logger(&config, sinks);
/// ```
pub fn start_logger(config: &AppConfig, sinks: Vec<Arc<dyn Sink>>) -> Logger {
    let (logger, receiver) = Logger::new(config);
    tokio::spawn(crate::worker::start_worker(receiver, sinks));
    logger
}

/// Initialize the global logger instance
/// This function initializes the global logger instance and returns a reference to it.
/// If the logger has been initialized before, it will return the existing logger instance.
///
/// # Parameters
/// - `config`: Configuration information
/// - `sinks`: A vector of Sink instances
///
/// # Returns
/// A reference to the global logger instance
///
/// # Example
/// ```
/// use rustfs_obs::{AppConfig,init_global_logger};
///
/// let config = AppConfig::default();
/// let sinks = vec![];
/// let logger = init_global_logger(&config, sinks);
/// ```
pub async fn init_global_logger(config: &AppConfig, sinks: Vec<Arc<dyn Sink>>) -> Arc<Mutex<Logger>> {
    let logger = Arc::new(Mutex::new(start_logger(config, sinks)));
    GLOBAL_LOGGER.set(logger.clone()).expect("Logger already initialized");
    logger
}

/// Get the global logger instance
///
/// This function returns a reference to the global logger instance.
///
/// # Returns
/// A reference to the global logger instance
///
/// # Example
/// ```
/// use rustfs_obs::get_global_logger;
///
/// let logger = get_global_logger();
/// ```
pub fn get_global_logger() -> &'static Arc<Mutex<Logger>> {
    GLOBAL_LOGGER.get().expect("Logger not initialized")
}

/// Get the global logger instance with a lock
/// This function returns a reference to the global logger instance with a lock.
/// It is used to ensure that the logger is thread-safe.
///
/// # Returns
/// A reference to the global logger instance with a lock
///
/// # Example
/// ```
/// use rustfs_obs::locked_logger;
///
/// async fn example() {
///     let logger = locked_logger().await;
/// }
/// ```
pub async fn locked_logger() -> tokio::sync::MutexGuard<'static, Logger> {
    get_global_logger().lock().await
}

/// Initialize with default empty logger if needed (optional)
/// This function initializes the logger with a default empty logger if needed.
/// It is used to ensure that the logger is initialized before logging.
///
/// # Returns
/// A reference to the global logger instance
///
/// # Example
/// ```
/// use rustfs_obs::ensure_logger_initialized;
///
/// let logger = ensure_logger_initialized();
/// ```
pub fn ensure_logger_initialized() -> &'static Arc<Mutex<Logger>> {
    if GLOBAL_LOGGER.get().is_none() {
        let config = AppConfig::default();
        let sinks = vec![];
        let logger = Arc::new(Mutex::new(start_logger(&config, sinks)));
        let _ = GLOBAL_LOGGER.set(logger);
    }
    GLOBAL_LOGGER.get().unwrap()
}

/// Log information
/// This function logs information messages.
///
/// # Parameters
/// - `message`: Message to be logged
/// - `source`: Source of the log
///
/// # Returns
/// Result indicating whether the operation was successful
///
/// # Example
/// ```
/// use rustfs_obs::log_info;
///
/// async fn example() {
///    let _ = log_info("This is an information message", "example").await;
/// }
/// ```
pub async fn log_info(message: &str, source: &str) -> Result<(), LogError> {
    get_global_logger().lock().await.write(message, source, Level::INFO).await
}

/// Log error
/// This function logs error messages.
/// # Parameters
/// - `message`: Message to be logged
/// - `source`: Source of the log
/// # Returns
/// Result indicating whether the operation was successful
/// # Example
/// ```
/// use rustfs_obs::log_error;
///
/// async fn example() {
///     let _ = log_error("This is an error message", "example").await;
/// }
pub async fn log_error(message: &str, source: &str) -> Result<(), LogError> {
    get_global_logger().lock().await.write(message, source, Level::ERROR).await
}

/// Log warning
/// This function logs warning messages.
/// # Parameters
/// - `message`: Message to be logged
/// - `source`: Source of the log
/// # Returns
/// Result indicating whether the operation was successful
///
/// # Example
/// ```
/// use rustfs_obs::log_warn;
///
/// async fn example() {
///     let _ = log_warn("This is a warning message", "example").await;
/// }
/// ```
pub async fn log_warn(message: &str, source: &str) -> Result<(), LogError> {
    get_global_logger().lock().await.write(message, source, Level::WARN).await
}

/// Log debug
/// This function logs debug messages.
/// # Parameters
/// - `message`: Message to be logged
/// - `source`: Source of the log
/// # Returns
/// Result indicating whether the operation was successful
///
/// # Example
/// ```
/// use rustfs_obs::log_debug;
///
/// async fn example() {
///     let _ = log_debug("This is a debug message", "example").await;
/// }
/// ```
pub async fn log_debug(message: &str, source: &str) -> Result<(), LogError> {
    get_global_logger().lock().await.write(message, source, Level::DEBUG).await
}

/// Log trace
/// This function logs trace messages.
/// # Parameters
/// - `message`: Message to be logged
/// - `source`: Source of the log
///
/// # Returns
/// Result indicating whether the operation was successful
///
/// # Example
/// ```
/// use rustfs_obs::log_trace;
///
/// async fn example() {
///    let _ = log_trace("This is a trace message", "example").await;
/// }
/// ```
pub async fn log_trace(message: &str, source: &str) -> Result<(), LogError> {
    get_global_logger().lock().await.write(message, source, Level::TRACE).await
}

/// Log with context information
/// This function logs messages with context information.
/// # Parameters
/// - `message`: Message to be logged
/// - `source`: Source of the log
/// - `level`: Log level
/// - `target`: Log target
/// - `request_id`: Request ID
/// - `user_id`: User ID
/// - `fields`: Additional fields
/// # Returns
/// Result indicating whether the operation was successful
/// # Example
/// ```
/// use tracing_core::Level;
/// use rustfs_obs::log_with_context;
///
/// async fn example() {
///    let _ = log_with_context("This is an information message", "example", Level::INFO, Some("target".to_string()), Some("req-12345".to_string()), Some("user-6789".to_string()), vec![("endpoint".to_string(), "/api/v1/data".to_string())]).await;
/// }
/// ```
pub async fn log_with_context(
    message: &str,
    source: &str,
    level: Level,
    target: Option<String>,
    request_id: Option<String>,
    user_id: Option<String>,
    fields: Vec<(String, String)>,
) -> Result<(), LogError> {
    get_global_logger()
        .lock()
        .await
        .write_with_context(message, source, level, target, request_id, user_id, fields)
        .await
}
