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

//! Unified event model shared by every stage of the analyzer.

use chrono::{DateTime, FixedOffset};
use serde::{Deserialize, Serialize, Serializer};
use std::fmt;
use std::sync::Arc;

/// Log severity level, ordered: Trace < Debug < Info < Warn < Error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl LogLevel {
    /// Parses the tracing-subscriber JSON `level` value.
    ///
    /// Accepts any ASCII case ("ERROR", "error", "Error"); returns `None`
    /// for unknown values.
    pub fn parse(s: &str) -> Option<Self> {
        const TABLE: [(&str, LogLevel); 5] = [
            ("trace", LogLevel::Trace),
            ("debug", LogLevel::Debug),
            ("info", LogLevel::Info),
            ("warn", LogLevel::Warn),
            ("error", LogLevel::Error),
        ];
        TABLE
            .iter()
            .find(|(name, _)| s.eq_ignore_ascii_case(name))
            .map(|(_, level)| *level)
    }
}

impl fmt::Display for LogLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            LogLevel::Trace => "TRACE",
            LogLevel::Debug => "DEBUG",
            LogLevel::Info => "INFO",
            LogLevel::Warn => "WARN",
            LogLevel::Error => "ERROR",
        })
    }
}

fn serialize_arc_str<S: Serializer>(value: &Arc<str>, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(value)
}

fn serialize_opt_arc_str<S: Serializer>(value: &Option<Arc<str>>, serializer: S) -> Result<S::Ok, S::Error> {
    match value {
        Some(v) => serializer.serialize_some(&**v),
        None => serializer.serialize_none(),
    }
}

/// Where an event came from: logical file name + 1-based line number.
///
/// `file` is the human-readable provenance path, e.g.
/// `customer.zip!node1/rustfs.log` (archive nesting joined with '!').
/// It is an `Arc<str>` on purpose: one file yields many events that all
/// share the same provenance string.
#[derive(Debug, Clone, Serialize)]
pub struct SourceRef {
    #[serde(serialize_with = "serialize_arc_str")]
    pub file: Arc<str>,
    pub line: u64,
}

/// How the line was recognized by the parser.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    /// A tracing-subscriber JSON line (possibly after container-prefix stripping).
    Json,
    /// A folded multi-line Rust panic block from stderr.
    Panic,
    /// Any other non-empty text line.
    Text,
}

/// The unified event every downstream stage consumes.
#[derive(Debug, Clone, Serialize)]
pub struct LogEvent {
    /// RFC-3339 timestamp with offset. None for panic/text lines that carry none.
    pub timestamp: Option<DateTime<FixedOffset>>,
    /// None when the line carries no recognizable level (text lines).
    pub level: Option<LogLevel>,
    /// tracing `target`, e.g. "rustfs::scanner::io". None for panic/text.
    pub target: Option<String>,
    /// Human message: JSON `message` field, panic headline, or the raw text line.
    pub message: String,
    /// All remaining flattened structured fields (JSON lines), or synthetic
    /// fields for panic blocks (`panic_location`, `panic_thread`, `panic_full`).
    pub fields: serde_json::Map<String, serde_json::Value>,
    pub source: SourceRef,
    /// Node label assigned by ingest grouping. None = single/unknown node.
    #[serde(serialize_with = "serialize_opt_arc_str")]
    pub node: Option<Arc<str>>,
    pub kind: EventKind,
}

impl LogEvent {
    /// Field as `&str` if it is a JSON string; numbers/bools are NOT coerced here.
    pub fn field_str(&self, name: &str) -> Option<&str> {
        self.fields.get(name)?.as_str()
    }

    /// Field rendered to a display string: strings as-is (unquoted),
    /// numbers/bools via `to_string`, other JSON types as compact JSON.
    /// Used by rule evidence collection.
    pub fn field_display(&self, name: &str) -> Option<String> {
        self.fields.get(name).map(|value| match value {
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        })
    }
}

/// Per-file parse accounting. Merged across files by ingest.
#[derive(Debug, Clone, Default, Serialize)]
pub struct ParseStats {
    pub total_lines: u64,
    /// Lines parsed as JSON directly.
    pub json_ok: u64,
    /// Lines parsed as JSON after container-prefix stripping.
    pub prefixed_json: u64,
    pub panic_blocks: u64,
    pub text_lines: u64,
    pub empty_lines: u64,
}

impl ParseStats {
    pub fn merge(&mut self, other: &ParseStats) {
        self.total_lines += other.total_lines;
        self.json_ok += other.json_ok;
        self.prefixed_json += other.prefixed_json;
        self.panic_blocks += other.panic_blocks;
        self.text_lines += other.text_lines;
        self.empty_lines += other.empty_lines;
    }

    /// `(json_ok + prefixed_json) / max(1, total_lines - empty_lines)`, in `[0, 1]`.
    pub fn json_ratio(&self) -> f64 {
        let denominator = self.total_lines.saturating_sub(self.empty_lines).max(1);
        (self.json_ok + self.prefixed_json) as f64 / denominator as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn sample_event(fields: serde_json::Map<String, Value>) -> LogEvent {
        LogEvent {
            timestamp: None,
            level: Some(LogLevel::Error),
            target: Some("rustfs::scanner::io".to_string()),
            message: "Disk health check marked disk faulty".to_string(),
            fields,
            source: SourceRef {
                file: Arc::from("customer.zip!node1/rustfs.log"),
                line: 42,
            },
            node: Some(Arc::from("node1")),
            kind: EventKind::Json,
        }
    }

    fn fields(value: Value) -> serde_json::Map<String, Value> {
        match value {
            Value::Object(map) => map,
            other => panic!("expected object, got {other}"),
        }
    }

    #[test]
    fn log_level_parse_accepts_any_ascii_case() {
        assert_eq!(LogLevel::parse("ERROR"), Some(LogLevel::Error));
        assert_eq!(LogLevel::parse("error"), Some(LogLevel::Error));
        assert_eq!(LogLevel::parse("Error"), Some(LogLevel::Error));
        assert_eq!(LogLevel::parse("WARN"), Some(LogLevel::Warn));
        assert_eq!(LogLevel::parse("trace"), Some(LogLevel::Trace));
        assert_eq!(LogLevel::parse("DEBUG"), Some(LogLevel::Debug));
        assert_eq!(LogLevel::parse("info"), Some(LogLevel::Info));
        assert_eq!(LogLevel::parse("FATAL"), None);
        assert_eq!(LogLevel::parse(""), None);
    }

    #[test]
    fn log_level_ordering_matches_severity() {
        assert!(LogLevel::Error > LogLevel::Warn);
        assert!(LogLevel::Warn > LogLevel::Info);
        assert!(LogLevel::Info > LogLevel::Debug);
        assert!(LogLevel::Debug > LogLevel::Trace);
    }

    #[test]
    fn log_level_display_is_uppercase() {
        assert_eq!(LogLevel::Error.to_string(), "ERROR");
        assert_eq!(LogLevel::Trace.to_string(), "TRACE");
    }

    #[test]
    fn field_str_returns_only_json_strings() {
        let ev = sample_event(fields(json!({"reason": "faulty_disk", "n": 3})));
        assert_eq!(ev.field_str("reason"), Some("faulty_disk"));
        assert_eq!(ev.field_str("n"), None);
        assert_eq!(ev.field_str("missing"), None);
    }

    #[test]
    fn field_display_renders_scalars_and_compact_json() {
        let ev = sample_event(fields(json!({
            "reason": "faulty_disk",
            "n": 3,
            "ok": true,
            "a": [1]
        })));
        assert_eq!(ev.field_display("reason"), Some("faulty_disk".to_string()));
        assert_eq!(ev.field_display("n"), Some("3".to_string()));
        assert_eq!(ev.field_display("ok"), Some("true".to_string()));
        assert_eq!(ev.field_display("a"), Some("[1]".to_string()));
        assert_eq!(ev.field_display("missing"), None);
    }

    #[test]
    fn parse_stats_merge_adds_every_counter() {
        let mut a = ParseStats {
            total_lines: 10,
            json_ok: 6,
            prefixed_json: 1,
            panic_blocks: 1,
            text_lines: 1,
            empty_lines: 1,
        };
        let b = ParseStats {
            total_lines: 5,
            json_ok: 2,
            prefixed_json: 1,
            panic_blocks: 0,
            text_lines: 1,
            empty_lines: 1,
        };
        a.merge(&b);
        assert_eq!(a.total_lines, 15);
        assert_eq!(a.json_ok, 8);
        assert_eq!(a.prefixed_json, 2);
        assert_eq!(a.panic_blocks, 1);
        assert_eq!(a.text_lines, 2);
        assert_eq!(a.empty_lines, 2);
    }

    #[test]
    fn json_ratio_ignores_empty_lines_and_never_divides_by_zero() {
        let full = ParseStats {
            total_lines: 10,
            json_ok: 6,
            prefixed_json: 2,
            empty_lines: 2,
            ..Default::default()
        };
        assert!((full.json_ratio() - 1.0).abs() < f64::EPSILON);

        let all_empty = ParseStats {
            total_lines: 3,
            empty_lines: 3,
            ..Default::default()
        };
        assert_eq!(all_empty.json_ratio(), 0.0);

        assert_eq!(ParseStats::default().json_ratio(), 0.0);
    }

    #[test]
    fn serde_representation_is_stable() {
        let ev = sample_event(fields(json!({"reason": "faulty_disk"})));
        let value = serde_json::to_value(&ev).expect("serialize");
        assert_eq!(value["level"], "ERROR");
        assert_eq!(value["kind"], "json");
        assert_eq!(value["node"], "node1");
        assert_eq!(value["source"]["file"], "customer.zip!node1/rustfs.log");
        assert_eq!(value["source"]["line"], 42);
        assert_eq!(value["fields"]["reason"], "faulty_disk");

        let no_node = LogEvent { node: None, ..ev };
        let value = serde_json::to_value(&no_node).expect("serialize");
        assert_eq!(value["node"], Value::Null);
        assert_eq!(value["timestamp"], Value::Null);

        assert_eq!(serde_json::to_value(EventKind::Panic).expect("serialize"), "panic");
        assert_eq!(serde_json::to_value(EventKind::Text).expect("serialize"), "text");
    }
}
