use chrono::{DateTime, Utc};
use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tracing_core::Level;

/// Wrapper for `tracing_core::Level` to implement `Serialize` and `Deserialize`
#[derive(Debug, Clone)]
pub struct SerializableLevel(pub Level);

impl From<Level> for SerializableLevel {
    fn from(level: Level) -> Self {
        SerializableLevel(level)
    }
}

impl From<SerializableLevel> for Level {
    fn from(serializable_level: SerializableLevel) -> Self {
        serializable_level.0
    }
}

impl Serialize for SerializableLevel {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.0.as_str())
    }
}

impl<'de> Deserialize<'de> for SerializableLevel {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "TRACE" => Ok(SerializableLevel(Level::TRACE)),
            "DEBUG" => Ok(SerializableLevel(Level::DEBUG)),
            "INFO" => Ok(SerializableLevel(Level::INFO)),
            "WARN" => Ok(SerializableLevel(Level::WARN)),
            "ERROR" => Ok(SerializableLevel(Level::ERROR)),
            _ => Err(D::Error::custom("unknown log level")),
        }
    }
}

/// Server log entry structure
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LogEntry {
    pub timestamp: DateTime<Utc>,      // Log timestamp
    pub level: SerializableLevel,      // Log Level
    pub message: String,               // Log messages
    pub source: String,                // Log source (such as module name)
    pub request_id: Option<String>,    // Request ID (Common Server Fields)
    pub user_id: Option<String>,       // User ID (Common Server Fields)
    pub fields: Vec<(String, String)>, // Attached fields (key value pairs)
}

impl LogEntry {
    /// Create a new LogEntry
    pub fn new(
        level: Level,
        message: String,
        source: String,
        request_id: Option<String>,
        user_id: Option<String>,
        fields: Vec<(String, String)>,
    ) -> Self {
        LogEntry {
            timestamp: Utc::now(),
            level: SerializableLevel::from(level),
            message,
            source,
            request_id,
            user_id,
            fields,
        }
    }
}
