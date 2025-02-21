use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

/// AuditEntry is a struct that represents an audit entry
/// that can be logged
/// # Fields
/// * `version` - The version of the audit entry
/// * `event_type` - The type of event that occurred
/// * `bucket` - The bucket that was accessed
/// * `object` - The object that was accessed
/// * `user` - The user that accessed the object
/// * `time` - The time the event occurred
/// * `user_agent` - The user agent that accessed the object
/// * `span_id` - The span ID of the event
/// # Example
/// ```
/// use rustfs_logging::AuditEntry;
/// let entry = AuditEntry {
///     version: "1.0".to_string(),
///     event_type: "read".to_string(),
///     bucket: "bucket".to_string(),
///     object: "object".to_string(),
///     user: "user".to_string(),
///     time: "time".to_string(),
///     user_agent: "user_agent".to_string(),
///     span_id: "span_id".to_string(),
/// };
/// ```
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AuditEntry {
    pub version: String,
    pub event_type: String,
    pub bucket: String,
    pub object: String,
    pub user: String,
    pub time: String,
    pub user_agent: String,
    pub span_id: String,
}

/// AuditTarget is a trait that defines the interface for audit targets
/// that can receive audit entries
pub trait AuditTarget: Send + Sync {
    fn send(&self, entry: AuditEntry);
}

/// FileAuditTarget is an audit target that logs audit entries to a file
pub struct FileAuditTarget;

impl AuditTarget for FileAuditTarget {
    /// Send an audit entry to a file
    /// # Arguments
    /// *   `entry` - The audit entry to send
    /// # Example
    /// ```
    /// use rustfs_logging::{AuditEntry, AuditTarget, FileAuditTarget};
    /// let entry = AuditEntry {
    ///     version: "1.0".to_string(),
    ///     event_type: "read".to_string(),
    ///     bucket: "bucket".to_string(),
    ///     object: "object".to_string(),
    ///     user: "user".to_string(),
    ///     time: "time".to_string(),
    ///     user_agent: "user_agent".to_string(),
    ///     span_id: "span_id".to_string(),
    /// };
    /// FileAuditTarget.send(entry);
    /// ```
    ///
    fn send(&self, entry: AuditEntry) {
        println!("File audit: {:?}", entry);
    }
}

/// AuditLogger is a logger that logs audit entries
/// to multiple targets
///
/// # Example
/// ```
/// use rustfs_logging::{AuditEntry, AuditLogger, FileAuditTarget};
///
/// #[tokio::main]
/// async fn main() {
///     let logger = AuditLogger::new(vec![Box::new(FileAuditTarget)]);
///     let entry = AuditEntry {
///         version: "1.0".to_string(),
///         event_type: "read".to_string(),
///         bucket: "bucket".to_string(),
///         object: "object".to_string(),
///         user: "user".to_string(),
///         time: "time".to_string(),
///         user_agent: "user_agent".to_string(),
///         span_id: "span_id".to_string(),
///     };
///     logger.log(entry).await;
/// }
/// ```
#[derive(Debug)]
pub struct AuditLogger {
    tx: mpsc::Sender<AuditEntry>,
}

impl AuditLogger {
    /// Create a new AuditLogger with the given targets
    /// that will receive audit entries
    /// # Arguments
    /// * `targets` - A vector of audit targets
    /// # Returns
    /// * An AuditLogger
    /// # Example
    /// ```
    /// use rustfs_logging::{AuditLogger, AuditEntry, FileAuditTarget};
    /// let logger = AuditLogger::new(vec![Box::new(FileAuditTarget)]);
    /// ```
    pub fn new(targets: Vec<Box<dyn AuditTarget>>) -> Self {
        let (tx, mut rx) = mpsc::channel::<AuditEntry>(1000);
        tokio::spawn(async move {
            while let Some(entry) = rx.recv().await {
                for target in &targets {
                    target.send(entry.clone());
                }
            }
        });
        Self { tx }
    }

    /// Log an audit entry
    /// # Arguments
    /// * `entry` - The audit entry to log
    /// # Example
    /// ```
    /// use rustfs_logging::{AuditEntry, AuditLogger, FileAuditTarget};
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let logger = AuditLogger::new(vec![Box::new(FileAuditTarget)]);
    ///     let entry = AuditEntry {
    ///         version: "1.0".to_string(),
    ///         event_type: "read".to_string(),
    ///         bucket: "bucket".to_string(),
    ///         object: "object".to_string(),
    ///         user: "user".to_string(),
    ///         time: "time".to_string(),
    ///         user_agent: "user_agent".to_string(),
    ///         span_id: "span_id".to_string(),
    ///     };
    ///     logger.log(entry).await;
    /// }
    /// ```
    pub async fn log(&self, entry: AuditEntry) {
        let _ = self.tx.send(entry).await;
    }
}
