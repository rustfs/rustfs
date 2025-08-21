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

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// Base log entry structure shared by all log types
/// This structure is used to serialize log entries to JSON
/// and send them to the log sinks
/// This structure is also used to deserialize log entries from JSON
/// This structure is also used to store log entries in the database
/// This structure is also used to query log entries from the database
///
/// The `BaseLogEntry` structure contains the following fields:
/// - `timestamp` - the timestamp of the log entry
/// - `request_id` - the request ID of the log entry
/// - `message` - the message of the log entry
/// - `tags` - the tags of the log entry
///
/// The `BaseLogEntry` structure contains the following methods:
/// - `new` - create a new `BaseLogEntry` with default values
/// - `message` - set the message
/// - `request_id` - set the request ID
/// - `tags` - set the tags
/// - `timestamp` - set the timestamp
///
/// # Example
/// ```
/// use rustfs_audit_logger::BaseLogEntry;
/// use chrono::{DateTime, Utc};
/// use std::collections::HashMap;
///
/// let timestamp = Utc::now();
/// let request = Some("req-123".to_string());
/// let message = Some("This is a log message".to_string());
/// let tags = Some(HashMap::new());
///
/// let entry = BaseLogEntry::new()
///     .timestamp(timestamp)
///     .request_id(request)
///     .message(message)
///     .tags(tags);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Default)]
pub struct BaseLogEntry {
    #[serde(rename = "time")]
    pub timestamp: DateTime<Utc>,

    #[serde(rename = "requestID", skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,

    #[serde(rename = "message", skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,

    #[serde(rename = "tags", skip_serializing_if = "Option::is_none")]
    pub tags: Option<HashMap<String, Value>>,
}

impl BaseLogEntry {
    /// Create a new BaseLogEntry with default values
    pub fn new() -> Self {
        BaseLogEntry {
            timestamp: Utc::now(),
            request_id: None,
            message: None,
            tags: None,
        }
    }

    /// Set the message
    pub fn message(mut self, message: Option<String>) -> Self {
        self.message = message;
        self
    }

    /// Set the request ID
    pub fn request_id(mut self, request_id: Option<String>) -> Self {
        self.request_id = request_id;
        self
    }

    /// Set the tags
    pub fn tags(mut self, tags: Option<HashMap<String, Value>>) -> Self {
        self.tags = tags;
        self
    }

    /// Set the timestamp
    pub fn timestamp(mut self, timestamp: DateTime<Utc>) -> Self {
        self.timestamp = timestamp;
        self
    }
}
