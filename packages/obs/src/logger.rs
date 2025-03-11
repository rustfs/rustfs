use crate::{AppConfig, LogEntry, SerializableLevel, Sink};
use std::sync::Arc;
use tokio::sync::mpsc::{self, Receiver, Sender};

/// Server log processor
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
        (
            Logger {
                sender,
                queue_capacity,
            },
            receiver,
        )
    }

    // Add a method to get queue capacity
    pub fn queue_capacity(&self) -> usize {
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

        // Record queue utilization (if a certain threshold is exceeded)
        let queue_len = self.sender.capacity();
        let utilization = queue_len as f64 / self.queue_capacity as f64;
        if utilization > 0.8 {
            tracing::warn!("Log queue utilization high: {:.1}%", utilization * 100.0);
        }

        // Generate independent Tracing Events with full LogEntry information
        // Generate corresponding events according to level
        match entry.level {
            SerializableLevel(tracing::Level::ERROR) => {
                tracing::error!(
                    target: "server_logs",
                    timestamp = %entry.timestamp,
                    message = %entry.message,
                    source = %entry.source,
                    request_id = ?entry.request_id,
                    user_id = ?entry.user_id,
                    fields = ?entry.fields
                );
            }
            SerializableLevel(tracing::Level::WARN) => {
                tracing::warn!(
                    target: "server_logs",
                    timestamp = %entry.timestamp,
                    message = %entry.message,
                    source = %entry.source,
                    request_id = ?entry.request_id,
                    user_id = ?entry.user_id,
                    fields = ?entry.fields
                );
            }
            SerializableLevel(tracing::Level::INFO) => {
                tracing::info!(
                    target: "server_logs",
                    timestamp = %entry.timestamp,
                    message = %entry.message,
                    source = %entry.source,
                    request_id = ?entry.request_id,
                    user_id = ?entry.user_id,
                    fields = ?entry.fields
                );
            }
            SerializableLevel(tracing::Level::DEBUG) => {
                tracing::debug!(
                    target: "server_logs",
                    timestamp = %entry.timestamp,
                    message = %entry.message,
                    source = %entry.source,
                    request_id = ?entry.request_id,
                    user_id = ?entry.user_id,
                    fields = ?entry.fields
                );
            }
            SerializableLevel(tracing::Level::TRACE) => {
                tracing::trace!(
                    target: "server_logs",
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
                match tokio::time::timeout(
                    std::time::Duration::from_millis(500),
                    self.sender.send(entry),
                )
                .await
                {
                    Ok(Ok(_)) => Ok(()),
                    Ok(Err(_)) => Err(LogError::SendFailed("Channel closed")),
                    Err(_) => Err(LogError::Timeout("Queue backpressure timeout")),
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                Err(LogError::SendFailed("Logger channel closed"))
            }
        }
    }

    // Add convenient methods to simplify logging
    // Fix the info() method, replacing None with an empty vector instead of the Option type
    pub async fn info(&self, message: &str, source: &str) -> Result<(), LogError> {
        self.log(LogEntry::new(
            tracing::Level::INFO,
            message.to_string(),
            source.to_string(),
            None,
            None,
            Vec::new(), // 使用空向量代替 None
        ))
        .await
    }

    /// Add warn() method
    pub async fn error(&self, message: &str, source: &str) -> Result<(), LogError> {
        self.log(LogEntry::new(
            tracing::Level::ERROR,
            message.to_string(),
            source.to_string(),
            None,
            None,
            Vec::new(),
        ))
        .await
    }

    /// Add warn() method
    pub async fn warn(&self, message: &str, source: &str) -> Result<(), LogError> {
        self.log(LogEntry::new(
            tracing::Level::WARN,
            message.to_string(),
            source.to_string(),
            None,
            None,
            Vec::new(),
        ))
        .await
    }

    /// Add debug() method
    pub async fn debug(&self, message: &str, source: &str) -> Result<(), LogError> {
        self.log(LogEntry::new(
            tracing::Level::DEBUG,
            message.to_string(),
            source.to_string(),
            None,
            None,
            Vec::new(),
        ))
        .await
    }

    /// Add trace() method
    pub async fn trace(&self, message: &str, source: &str) -> Result<(), LogError> {
        self.log(LogEntry::new(
            tracing::Level::TRACE,
            message.to_string(),
            source.to_string(),
            None,
            None,
            Vec::new(),
        ))
        .await
    }

    // Add extension methods with context information for more flexibility
    pub async fn info_with_context(
        &self,
        message: &str,
        source: &str,
        request_id: Option<String>,
        user_id: Option<String>,
        fields: Vec<(String, String)>,
    ) -> Result<(), LogError> {
        self.log(LogEntry::new(
            tracing::Level::INFO,
            message.to_string(),
            source.to_string(),
            request_id,
            user_id,
            fields,
        ))
        .await
    }

    // Add elegant closing method
    pub async fn shutdown(self) -> Result<(), LogError> {
        drop(self.sender); //Close the sending end so that the receiver knows that there is no new message
        Ok(())
    }
}

// Define custom error type
#[derive(Debug, thiserror::Error)]
pub enum LogError {
    #[error("Failed to send log: {0}")]
    SendFailed(&'static str),
    #[error("Operation timed out: {0}")]
    Timeout(&'static str),
}

/// Start the log module
pub fn start_logger(config: &AppConfig, sinks: Vec<Arc<dyn Sink>>) -> Logger {
    let (logger, receiver) = Logger::new(config);
    tokio::spawn(crate::worker::start_worker(receiver, sinks));
    logger
}
