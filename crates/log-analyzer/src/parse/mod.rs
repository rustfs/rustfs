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

//! Line parsing: raw customer log lines -> [`LogEvent`]s.
//!
//! Four channels, tried in order per line (see rustfs/backlog#1283):
//! 1. JSON (native RustFS application logs);
//! 2. container-prefix stripping, then JSON again;
//! 3. multi-line panic block folding;
//! 4. plain-text fallback (never fails).

mod json;
mod panic_block;
mod prefix;

use crate::model::{EventKind, LogEvent, ParseStats, SourceRef};
use std::sync::Arc;

/// Stateful per-file line parser. One instance per logical file: panic
/// blocks never span file boundaries.
pub struct LineParser {
    file: Arc<str>,
    node: Option<Arc<str>>,
    pending_panic: Option<panic_block::PanicAcc>,
    stats: ParseStats,
}

impl LineParser {
    /// `file` becomes `SourceRef.file` for every event this parser emits.
    pub fn new(file: Arc<str>, node: Option<Arc<str>>) -> Self {
        Self {
            file,
            node,
            pending_panic: None,
            stats: ParseStats::default(),
        }
    }

    /// Feeds one raw line (caller strips `\r\n`/`\n`).
    ///
    /// Returns 0..=2 events: an in-progress panic block being flushed may
    /// precede the event produced by the current line.
    pub fn feed(&mut self, line: &str, line_no: u64) -> Vec<LogEvent> {
        self.stats.total_lines += 1;

        if line.trim().is_empty() {
            self.stats.empty_lines += 1;
            // A blank line terminates an open panic block.
            return self.flush_panic().into_iter().collect();
        }

        let mut out = Vec::with_capacity(1);
        if let Some(pending) = self.pending_panic.as_mut() {
            if pending.absorbs(line) {
                pending.push(line);
                return out;
            }
            // Not absorbable: close the block, then treat this line normally.
            out.extend(self.flush_panic());
        }

        if let Some(ev) = json::try_parse(line, &self.file, line_no, &self.node) {
            self.stats.json_ok += 1;
            out.push(ev);
            return out;
        }

        // After stripping, non-JSON content still goes through the panic and
        // text channels (containers wrap stderr panics too).
        let mut line = line;
        if let Some(stripped) = prefix::strip(line) {
            if let Some(ev) = json::try_parse(stripped, &self.file, line_no, &self.node) {
                self.stats.prefixed_json += 1;
                out.push(ev);
                return out;
            }
            line = stripped;
        }

        if let Some(acc) = panic_block::starts(line, line_no) {
            self.pending_panic = Some(acc);
            return out;
        }

        self.stats.text_lines += 1;
        out.push(self.text_event(line, line_no));
        out
    }

    /// Flushes a trailing unterminated panic block at EOF.
    pub fn finish(&mut self) -> Option<LogEvent> {
        self.flush_panic()
    }

    pub fn stats(&self) -> &ParseStats {
        &self.stats
    }

    fn flush_panic(&mut self) -> Option<LogEvent> {
        let acc = self.pending_panic.take()?;
        self.stats.panic_blocks += 1;
        Some(acc.into_event(&self.file, &self.node))
    }

    fn text_event(&self, line: &str, line_no: u64) -> LogEvent {
        LogEvent {
            timestamp: None,
            level: None,
            target: None,
            message: line.trim_end().to_string(),
            fields: serde_json::Map::new(),
            source: SourceRef {
                file: Arc::clone(&self.file),
                line: line_no,
            },
            node: self.node.clone(),
            kind: EventKind::Text,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::LogLevel;

    const JSON_LINE: &str = r#"{"timestamp":"2026-07-15T14:23:01.123456+08:00","level":"ERROR","target":"rustfs::scanner::io","filename":"crates/ecstore/src/disk/disk_store.rs","line_number":780,"threadName":"tokio-runtime-worker","threadId":"ThreadId(7)","message":"Disk health check marked disk faulty","reason":"faulty_disk","disk":"/data/disk3"}"#;

    fn parser() -> LineParser {
        LineParser::new(Arc::from("rustfs.log"), Some(Arc::from("node1")))
    }

    fn feed_all(parser: &mut LineParser, lines: &[&str]) -> Vec<LogEvent> {
        let mut out = Vec::new();
        for (idx, line) in lines.iter().enumerate() {
            out.extend(parser.feed(line, idx as u64 + 1));
        }
        out.extend(parser.finish());
        out
    }

    fn assert_faulty_disk_json(ev: &LogEvent) {
        assert_eq!(ev.kind, EventKind::Json);
        assert_eq!(ev.level, Some(LogLevel::Error));
        assert_eq!(ev.target.as_deref(), Some("rustfs::scanner::io"));
        assert_eq!(ev.message, "Disk health check marked disk faulty");
        let ts = ev.timestamp.expect("timestamp");
        assert_eq!(ts.offset().local_minus_utc(), 8 * 3600);
        assert_eq!(ev.field_str("reason"), Some("faulty_disk"));
        assert_eq!(ev.field_str("disk"), Some("/data/disk3"));
        assert!(ev.fields.contains_key("filename"));
        assert!(!ev.fields.contains_key("message"));
        assert_eq!(ev.node.as_deref(), Some("node1"));
    }

    #[test]
    fn native_json_line() {
        let mut p = parser();
        let events = p.feed(JSON_LINE, 1);
        assert_eq!(events.len(), 1);
        assert_faulty_disk_json(&events[0]);
        assert_eq!(p.stats().json_ok, 1);
    }

    #[test]
    fn cri_prefixed_json_line() {
        let mut p = parser();
        let line = format!("2026-07-15T06:23:01.123456789Z stdout F {JSON_LINE}");
        let events = p.feed(&line, 1);
        assert_eq!(events.len(), 1);
        assert_faulty_disk_json(&events[0]);
        assert_eq!(p.stats().prefixed_json, 1);
        assert_eq!(p.stats().json_ok, 0);
    }

    #[test]
    fn compose_prefixed_json_line() {
        let mut p = parser();
        let line = format!("rustfs-node1-1  | {JSON_LINE}");
        let events = p.feed(&line, 1);
        assert_eq!(events.len(), 1);
        assert_faulty_disk_json(&events[0]);
        assert_eq!(p.stats().prefixed_json, 1);
    }

    #[test]
    fn journald_prefixed_json_line() {
        let mut p = parser();
        let line = format!("Jul 15 06:23:01 host1 rustfs[1234]: {JSON_LINE}");
        let events = p.feed(&line, 1);
        assert_eq!(events.len(), 1);
        assert_faulty_disk_json(&events[0]);
        assert_eq!(p.stats().prefixed_json, 1);
    }

    #[test]
    fn new_format_panic_then_json_flushes_two_events() {
        let mut p = parser();
        assert!(
            p.feed("thread 'tokio-runtime-worker' panicked at crates/ecstore/src/set_disk.rs:100:17:", 1)
                .is_empty()
        );
        assert!(p.feed("called `Option::unwrap()` on a `None` value", 2).is_empty());
        assert!(
            p.feed("note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace", 3)
                .is_empty()
        );
        let events = p.feed(JSON_LINE, 4);
        assert_eq!(events.len(), 2);
        let panic = &events[0];
        assert_eq!(panic.kind, EventKind::Panic);
        assert_eq!(panic.level, Some(LogLevel::Error));
        assert!(panic.timestamp.is_none());
        assert_eq!(
            panic.message,
            "thread 'tokio-runtime-worker' panicked at crates/ecstore/src/set_disk.rs:100:17: \
             called `Option::unwrap()` on a `None` value"
        );
        assert_eq!(panic.field_str("panic_location"), Some("crates/ecstore/src/set_disk.rs:100:17"));
        assert_eq!(panic.field_str("panic_thread"), Some("tokio-runtime-worker"));
        assert!(panic.field_str("panic_full").expect("panic_full").contains("note: run with"));
        assert_eq!(panic.source.line, 1);
        assert_faulty_disk_json(&events[1]);
        assert_eq!(p.stats().panic_blocks, 1);
    }

    #[test]
    fn old_format_panic_normalizes_to_same_message_shape() {
        let mut p = parser();
        assert!(p.feed("thread 'main' panicked at 'oh no', src/main.rs:3:5", 1).is_empty());
        let panic = p.finish().expect("flushed at EOF");
        assert_eq!(panic.message, "thread 'main' panicked at src/main.rs:3:5: oh no");
        assert_eq!(panic.field_str("panic_location"), Some("src/main.rs:3:5"));
        assert_eq!(p.stats().panic_blocks, 1);
    }

    #[test]
    fn panic_absorbs_backtrace_lines() {
        let mut p = parser();
        let events = feed_all(
            &mut p,
            &[
                "thread 'main' panicked at src/main.rs:3:5:",
                "boom",
                "stack backtrace:",
                "   0: std::panicking::begin_panic",
                "             at /rustc/abc/library/std/src/panicking.rs:600:12",
                "   1: rustfs::main",
            ],
        );
        assert_eq!(events.len(), 1);
        let full = events[0].field_str("panic_full").expect("panic_full");
        assert!(full.contains("stack backtrace:"));
        assert!(full.contains("0: std::panicking::begin_panic"));
        assert!(full.contains("at /rustc/abc"));
    }

    #[test]
    fn blank_line_terminates_panic_block() {
        let mut p = parser();
        assert!(p.feed("thread 'main' panicked at src/main.rs:3:5:", 1).is_empty());
        assert!(p.feed("boom", 2).is_empty());
        let events = p.feed("", 3);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, EventKind::Panic);
        assert_eq!(p.stats().empty_lines, 1);
        assert!(p.finish().is_none());
    }

    #[test]
    fn panic_full_is_capped() {
        let mut p = parser();
        assert!(p.feed("thread 'main' panicked at src/main.rs:3:5:", 1).is_empty());
        assert!(p.feed("boom", 2).is_empty());
        let long = format!("   {}", "x".repeat(600));
        for i in 0..20 {
            assert!(p.feed(&long, 3 + i).is_empty());
        }
        let panic = p.finish().expect("flushed");
        let full = panic.field_str("panic_full").expect("panic_full");
        assert!(full.len() <= 8 * 1024 + "…[truncated]".len() + 1);
        assert!(full.ends_with("…[truncated]"));
    }

    #[test]
    fn business_json_without_log_keys_is_text() {
        let mut p = parser();
        let events = p.feed(r#"{"foo":1}"#, 1);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, EventKind::Text);
        assert_eq!(p.stats().text_lines, 1);
        assert_eq!(p.stats().json_ok, 0);
    }

    #[test]
    fn bad_timestamp_is_preserved_raw() {
        let mut p = parser();
        let events = p.feed(r#"{"level":"WARN","message":"x","timestamp":"not-a-date"}"#, 1);
        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert!(ev.timestamp.is_none());
        assert_eq!(ev.level, Some(LogLevel::Warn));
        assert_eq!(ev.field_str("timestamp_raw"), Some("not-a-date"));
    }

    #[test]
    fn unknown_level_is_preserved_raw() {
        let mut p = parser();
        let events = p.feed(r#"{"level":"NOTICE","message":"x"}"#, 1);
        assert_eq!(events[0].level, None);
        assert_eq!(events[0].field_str("level_raw"), Some("NOTICE"));
    }

    #[test]
    fn fatal_stderr_line_is_text_with_verbatim_message() {
        let mut p = parser();
        let events = p.feed("[FATAL] Observability initialization failed: collector unavailable", 1);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, EventKind::Text);
        assert_eq!(events[0].message, "[FATAL] Observability initialization failed: collector unavailable");
    }

    #[test]
    fn empty_and_whitespace_lines_produce_no_events() {
        let mut p = parser();
        assert!(p.feed("", 1).is_empty());
        assert!(p.feed("   ", 2).is_empty());
        assert_eq!(p.stats().empty_lines, 2);
        assert_eq!(p.stats().total_lines, 2);
    }

    #[test]
    fn mixed_input_stats_are_consistent() {
        let mut p = parser();
        let events = feed_all(
            &mut p,
            &[
                JSON_LINE,
                "",
                "plain text noise",
                &format!("2026-07-15T06:23:01Z stdout F {JSON_LINE}"),
                "thread 'main' panicked at src/main.rs:3:5:",
                "boom",
            ],
        );
        // json + text + prefixed json + panic (flushed by finish)
        assert_eq!(events.len(), 4);
        let stats = p.stats();
        assert_eq!(stats.total_lines, 6);
        assert_eq!(stats.json_ok, 1);
        assert_eq!(stats.prefixed_json, 1);
        assert_eq!(stats.text_lines, 1);
        assert_eq!(stats.empty_lines, 1);
        assert_eq!(stats.panic_blocks, 1);
        // 6 non-empty-adjusted lines: 5 counted, 2 json → ratio 2/5
        assert!((stats.json_ratio() - 0.4).abs() < f64::EPSILON);
    }
}
