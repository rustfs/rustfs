// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(dead_code)]
pub(crate) mod args;
pub(crate) mod audit;
pub(crate) mod base;
pub(crate) mod unified;

use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tracing_core::Level;

/// ObjectVersion is used across multiple modules
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct ObjectVersion {
    #[serde(rename = "name")]
    pub object_name: String,
    #[serde(rename = "versionId", skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
}

impl ObjectVersion {
    /// Create a new ObjectVersion object
    pub fn new() -> Self {
        ObjectVersion {
            object_name: String::new(),
            version_id: None,
        }
    }

    /// Create a new ObjectVersion with object name
    pub fn new_with_object_name(object_name: String) -> Self {
        ObjectVersion {
            object_name,
            version_id: None,
        }
    }

    /// Set the object name
    pub fn set_object_name(mut self, object_name: String) -> Self {
        self.object_name = object_name;
        self
    }

    /// Set the version ID
    pub fn set_version_id(mut self, version_id: Option<String>) -> Self {
        self.version_id = version_id;
        self
    }
}

impl Default for ObjectVersion {
    fn default() -> Self {
        Self::new()
    }
}

/// Log kind/level enum
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum LogKind {
    #[serde(rename = "INFO")]
    #[default]
    Info,
    #[serde(rename = "WARNING")]
    Warning,
    #[serde(rename = "ERROR")]
    Error,
    #[serde(rename = "FATAL")]
    Fatal,
}

/// Trait for types that can be serialized to JSON and have a timestamp
/// This trait is used by `ServerLogEntry` to convert the log entry to JSON
/// and get the timestamp of the log entry
/// This trait is implemented by `ServerLogEntry`
///
/// # Example
/// ```
/// use rustfs_audit_logger::LogRecord;
/// use chrono::{DateTime, Utc};
/// use rustfs_audit_logger::ServerLogEntry;
/// use tracing_core::Level;
///
/// let log_entry = ServerLogEntry::new(Level::INFO, "api_handler".to_string());
/// let json = log_entry.to_json();
/// let timestamp = log_entry.get_timestamp();
/// ```
pub trait LogRecord {
    fn to_json(&self) -> String;
    fn get_timestamp(&self) -> chrono::DateTime<chrono::Utc>;
}

/// Wrapper for `tracing_core::Level` to implement `Serialize` and `Deserialize`
/// for `ServerLogEntry`
/// This is necessary because `tracing_core::Level` does not implement `Serialize`
/// and `Deserialize`
/// This is a workaround to allow `ServerLogEntry` to be serialized and deserialized
/// using `serde`
///
/// # Example
/// ```
/// use rustfs_audit_logger::SerializableLevel;
/// use tracing_core::Level;
///
/// let level = Level::INFO;
/// let serializable_level = SerializableLevel::from(level);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
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
