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

//! Channel 1: tracing-subscriber JSON lines.
//!
//! RustFS application logs are produced by `tracing_subscriber`'s JSON
//! formatter with `flatten_event(true)` (see `crates/obs`'s
//! `build_json_log_layer`): one JSON object per line, event fields flattened
//! at the top level, RFC-3339 `timestamp` with a local-timezone offset and
//! an uppercase `level`.

use crate::model::{EventKind, LogEvent, LogLevel, SourceRef};
use serde_json::{Map, Value};
use std::sync::Arc;

/// Parses one line as a tracing JSON log event.
///
/// Returns `None` when the line is not a JSON object or is a JSON object
/// that does not look like a log line (carries neither `message` nor
/// `level`) — business JSON payloads fall through to the text channel.
pub(crate) fn try_parse(line: &str, file: &Arc<str>, line_no: u64, node: &Option<Arc<str>>) -> Option<LogEvent> {
    let mut map: Map<String, Value> = serde_json::from_str(line).ok()?;
    if !map.contains_key("message") && !map.contains_key("level") {
        return None;
    }

    let timestamp = match map.remove("timestamp") {
        Some(Value::String(raw)) => match chrono::DateTime::parse_from_rfc3339(&raw) {
            Ok(ts) => Some(ts),
            Err(_) => {
                map.insert("timestamp_raw".to_string(), Value::String(raw));
                None
            }
        },
        Some(other) => {
            map.insert("timestamp_raw".to_string(), other);
            None
        }
        None => None,
    };

    let level = match map.remove("level") {
        Some(Value::String(raw)) => match LogLevel::parse(&raw) {
            Some(level) => Some(level),
            None => {
                map.insert("level_raw".to_string(), Value::String(raw));
                None
            }
        },
        Some(other) => {
            map.insert("level_raw".to_string(), other);
            None
        }
        None => None,
    };

    let target = match map.remove("target") {
        Some(Value::String(s)) => Some(s),
        Some(other) => {
            map.insert("target".to_string(), other);
            None
        }
        None => None,
    };

    let message = match map.remove("message") {
        Some(Value::String(s)) => s,
        Some(other) => other.to_string(),
        None => String::new(),
    };

    Some(LogEvent {
        timestamp,
        level,
        target,
        message,
        fields: map,
        source: SourceRef {
            file: Arc::clone(file),
            line: line_no,
        },
        node: node.clone(),
        kind: EventKind::Json,
    })
}
