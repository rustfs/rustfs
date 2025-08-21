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

use crate::{AuditLogEntry, BaseLogEntry, LogKind, LogRecord, SerializableLevel};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing_core::Level;

/// Server log entry with structured fields
/// ServerLogEntry is used to log structured log entries from the server
///
/// The `ServerLogEntry` structure contains the following fields:
/// - `base` - the base log entry
/// - `level` - the log level
/// - `source` - the source of the log entry
/// - `user_id` - the user ID
/// - `fields` - the structured fields of the log entry
///
/// The `ServerLogEntry` structure contains the following methods:
/// - `new` - create a new `ServerLogEntry` with specified level and source
/// - `with_base` - set the base log entry
/// - `user_id` - set the user ID
/// - `fields` - set the fields
/// - `add_field` - add a field
///
/// # Example
/// ```
/// use rustfs_audit_logger::ServerLogEntry;
/// use tracing_core::Level;
///
/// let entry = ServerLogEntry::new(Level::INFO, "test_module".to_string())
///    .user_id(Some("user-456".to_string()))
///   .add_field("operation".to_string(), "login".to_string());
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServerLogEntry {
    #[serde(flatten)]
    pub base: BaseLogEntry,

    pub level: SerializableLevel,
    pub source: String,

    #[serde(rename = "userId", skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,

    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub fields: Vec<(String, String)>,
}

impl ServerLogEntry {
    /// Create a new ServerLogEntry with specified level and source
    pub fn new(level: Level, source: String) -> Self {
        ServerLogEntry {
            base: BaseLogEntry::new(),
            level: SerializableLevel(level),
            source,
            user_id: None,
            fields: Vec::new(),
        }
    }

    /// Set the base log entry
    pub fn with_base(mut self, base: BaseLogEntry) -> Self {
        self.base = base;
        self
    }

    /// Set the user ID
    pub fn user_id(mut self, user_id: Option<String>) -> Self {
        self.user_id = user_id;
        self
    }

    /// Set fields
    pub fn fields(mut self, fields: Vec<(String, String)>) -> Self {
        self.fields = fields;
        self
    }

    /// Add a field
    pub fn add_field(mut self, key: String, value: String) -> Self {
        self.fields.push((key, value));
        self
    }
}

impl LogRecord for ServerLogEntry {
    fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| String::from("{}"))
    }

    fn get_timestamp(&self) -> DateTime<Utc> {
        self.base.timestamp
    }
}

/// Console log entry structure
/// ConsoleLogEntry is used to log console log entries
/// The `ConsoleLogEntry` structure contains the following fields:
/// - `base` - the base log entry
/// - `level` - the log level
/// - `console_msg` - the console message
/// - `node_name` - the node name
/// - `err` - the error message
///
/// The `ConsoleLogEntry` structure contains the following methods:
/// - `new` - create a new `ConsoleLogEntry`
/// - `new_with_console_msg` - create a new `ConsoleLogEntry` with console message and node name
/// - `with_base` - set the base log entry
/// - `set_level` - set the log level
/// - `set_node_name` - set the node name
/// - `set_console_msg` - set the console message
/// - `set_err` - set the error message
///
/// # Example
/// ```
/// use rustfs_audit_logger::ConsoleLogEntry;
///
/// let entry = ConsoleLogEntry::new_with_console_msg("Test message".to_string(), "node-123".to_string());
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsoleLogEntry {
    #[serde(flatten)]
    pub base: BaseLogEntry,

    pub level: LogKind,
    pub console_msg: String,
    pub node_name: String,

    #[serde(skip)]
    pub err: Option<String>,
}

impl ConsoleLogEntry {
    /// Create a new ConsoleLogEntry
    pub fn new() -> Self {
        ConsoleLogEntry {
            base: BaseLogEntry::new(),
            level: LogKind::Info,
            console_msg: String::new(),
            node_name: String::new(),
            err: None,
        }
    }

    /// Create a new ConsoleLogEntry with console message and node name
    pub fn new_with_console_msg(console_msg: String, node_name: String) -> Self {
        ConsoleLogEntry {
            base: BaseLogEntry::new(),
            level: LogKind::Info,
            console_msg,
            node_name,
            err: None,
        }
    }

    /// Set the base log entry
    pub fn with_base(mut self, base: BaseLogEntry) -> Self {
        self.base = base;
        self
    }

    /// Set the log level
    pub fn set_level(mut self, level: LogKind) -> Self {
        self.level = level;
        self
    }

    /// Set the node name
    pub fn set_node_name(mut self, node_name: String) -> Self {
        self.node_name = node_name;
        self
    }

    /// Set the console message
    pub fn set_console_msg(mut self, console_msg: String) -> Self {
        self.console_msg = console_msg;
        self
    }

    /// Set the error message
    pub fn set_err(mut self, err: Option<String>) -> Self {
        self.err = err;
        self
    }
}

impl Default for ConsoleLogEntry {
    fn default() -> Self {
        Self::new()
    }
}

impl LogRecord for ConsoleLogEntry {
    fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| String::from("{}"))
    }

    fn get_timestamp(&self) -> DateTime<Utc> {
        self.base.timestamp
    }
}

/// Unified log entry type
/// UnifiedLogEntry is used to log different types of log entries
///
/// The `UnifiedLogEntry` enum contains the following variants:
/// - `Server` - a server log entry
/// - `Audit` - an audit log entry
/// - `Console` - a console log entry
///
/// The `UnifiedLogEntry` enum contains the following methods:
/// - `to_json` - convert the log entry to JSON
/// - `get_timestamp` - get the timestamp of the log entry
///
/// # Example
/// ```
/// use rustfs_audit_logger::{UnifiedLogEntry, ServerLogEntry};
/// use tracing_core::Level;
///
/// let server_entry = ServerLogEntry::new(Level::INFO, "test_module".to_string());
/// let unified = UnifiedLogEntry::Server(server_entry);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum UnifiedLogEntry {
    #[serde(rename = "server")]
    Server(ServerLogEntry),

    #[serde(rename = "audit")]
    Audit(Box<AuditLogEntry>),

    #[serde(rename = "console")]
    Console(ConsoleLogEntry),
}

impl LogRecord for UnifiedLogEntry {
    fn to_json(&self) -> String {
        match self {
            UnifiedLogEntry::Server(entry) => entry.to_json(),
            UnifiedLogEntry::Audit(entry) => entry.to_json(),
            UnifiedLogEntry::Console(entry) => entry.to_json(),
        }
    }

    fn get_timestamp(&self) -> DateTime<Utc> {
        match self {
            UnifiedLogEntry::Server(entry) => entry.get_timestamp(),
            UnifiedLogEntry::Audit(entry) => entry.get_timestamp(),
            UnifiedLogEntry::Console(entry) => entry.get_timestamp(),
        }
    }
}
