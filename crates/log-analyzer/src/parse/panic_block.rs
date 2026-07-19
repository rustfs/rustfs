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

//! Channel 3: folding multi-line Rust panic blocks.
//!
//! RustFS installs no panic hook, so panics never reach the JSON log stream:
//! they are printed by the default handler to stderr as multi-line plain
//! text, in one of two real formats:
//!
//! - new (Rust >= 1.65), payload on the NEXT line:
//!   `thread 'main' panicked at src/main.rs:3:5:`
//! - old, payload inline:
//!   `thread 'main' panicked at 'oh no', src/main.rs:3:5`
//!
//! optionally followed by `note: run with ...` and an indented backtrace.

use crate::model::{EventKind, LogEvent, LogLevel, SourceRef};
use regex::Regex;
use std::sync::{Arc, LazyLock};

/// Cap on the folded raw block stored in `fields["panic_full"]`.
const PANIC_FULL_CAP: usize = 8 * 1024;
const TRUNCATION_MARK: &str = "…[truncated]";

/// New format: location ends the line, payload follows on the next line.
static START_NEW: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^thread '([^']*)' panicked at (.+:\d+(?::\d+)?):$").expect("static regex"));

/// Old format: quoted payload inline, location last.
static START_OLD: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^thread '([^']*)' panicked at '(.*)', (.+:\d+(?::\d+)?)$").expect("static regex"));

/// Accumulator for one panic block being folded.
pub(crate) struct PanicAcc {
    thread: String,
    location: String,
    payload: Option<String>,
    full: String,
    truncated: bool,
    /// New-format start absorbs the very next line unconditionally (payload).
    expect_payload: bool,
    start_line: u64,
}

/// Returns an accumulator when `line` starts a panic block.
pub(crate) fn starts(line: &str, line_no: u64) -> Option<PanicAcc> {
    if let Some(caps) = START_NEW.captures(line) {
        return Some(PanicAcc::new(&caps[1], &caps[2], None, line, line_no));
    }
    if let Some(caps) = START_OLD.captures(line) {
        let payload = caps[2].to_string();
        return Some(PanicAcc::new(&caps[1], &caps[3], Some(payload), line, line_no));
    }
    None
}

/// Whether `line` begins a panic block (either format), without allocating.
pub(crate) fn is_start(line: &str) -> bool {
    START_NEW.is_match(line) || START_OLD.is_match(line)
}

impl PanicAcc {
    fn new(thread: &str, location: &str, payload: Option<String>, raw: &str, line_no: u64) -> Self {
        let expect_payload = payload.is_none();
        Self {
            thread: thread.to_string(),
            location: location.to_string(),
            payload,
            full: raw.to_string(),
            truncated: false,
            expect_payload,
            start_line: line_no,
        }
    }

    /// Whether the block absorbs this line (call only while the block is open).
    pub(crate) fn absorbs(&self, line: &str) -> bool {
        if self.expect_payload {
            // The next line is normally this panic's payload, but a line that
            // is itself a new event (JSON) or a fresh panic header is not —
            // swallowing it would drop a real log event (e.g. an interleaved
            // ERROR in merged stdout/stderr, or a panic-during-panic header).
            return !is_start(line) && !line.trim_start().starts_with('{');
        }
        // `note: ` covers both "run with `RUST_BACKTRACE`…" and the trailing
        // "Some details are omitted, run with …" line emitted at BACKTRACE=1.
        line.starts_with(char::is_whitespace) || line == "stack backtrace:" || line.starts_with("note: ")
    }

    pub(crate) fn push(&mut self, line: &str) {
        if self.expect_payload {
            self.payload = Some(line.to_string());
            self.expect_payload = false;
        }
        if self.truncated {
            return;
        }
        if self.full.len() + 1 + line.len() > PANIC_FULL_CAP {
            self.full.push('\n');
            self.full.push_str(TRUNCATION_MARK);
            self.truncated = true;
        } else {
            self.full.push('\n');
            self.full.push_str(line);
        }
    }

    /// Finalizes the block into a synthetic event. Panics carry no timestamp.
    pub(crate) fn into_event(self, file: &Arc<str>, node: &Option<Arc<str>>) -> LogEvent {
        let payload = self.payload.as_deref().unwrap_or("").trim();
        let message = format!("thread '{}' panicked at {}: {}", self.thread, self.location, payload);
        let mut fields = serde_json::Map::new();
        fields.insert("panic_thread".to_string(), self.thread.into());
        fields.insert("panic_location".to_string(), self.location.into());
        fields.insert("panic_full".to_string(), self.full.into());
        LogEvent {
            timestamp: None,
            level: Some(LogLevel::Error),
            target: None,
            message,
            fields,
            source: SourceRef {
                file: Arc::clone(file),
                line: self.start_line,
            },
            node: node.clone(),
            kind: EventKind::Panic,
        }
    }
}
