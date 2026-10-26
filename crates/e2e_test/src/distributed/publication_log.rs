// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Follow the same append-only log files without rescanning previous JSON rows.

use same_file::Handle;
use serde::{Deserialize, Deserializer};
use serde_json::Value;
use std::fs::File;
use std::io::{self, Read};
use std::path::PathBuf;

// Captured RPC spans already produce valid rows above 2 MiB. Keep complete
// rows bounded independently from the much smaller selected-event history.
const MAX_LINE_BYTES: usize = 4 << 20;
const MAX_EVENTS: usize = 128;
const MAX_EVENT_BYTES: usize = 1 << 20;

fn invalid(reason: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, reason)
}

#[derive(Default, Deserialize)]
struct Selector {
    #[serde(default)]
    event: Value,
    #[serde(default)]
    op: Value,
    #[serde(default)]
    bucket: Value,
    #[serde(default)]
    object: Value,
}

impl Selector {
    fn matches(&self, bucket: &str, object: &str) -> bool {
        self.event.as_str() == Some("object_namespace_lock")
            && self.op.as_str() == Some("data_movement_publication")
            && self.bucket.as_str() == Some(bucket)
            && self.object.as_str() == Some(object)
    }
}

#[derive(Deserialize)]
struct Probe {
    #[serde(default)]
    event: Value,
    #[serde(default)]
    op: Value,
    #[serde(default)]
    bucket: Value,
    #[serde(default)]
    object: Value,
    // A present null is not a missing fields object. The old reader uses the
    // root only when this key is absent, including for non-object JSON values.
    #[serde(default, deserialize_with = "present_fields")]
    fields: Option<Option<Selector>>,
}

fn present_fields<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Option<Selector>>, D::Error> {
    Option::<Selector>::deserialize(d).map(Some)
}

impl Probe {
    fn matches(&self, bucket: &str, object: &str) -> bool {
        match &self.fields {
            Some(Some(fields)) => fields.matches(bucket, object),
            Some(None) => false,
            None => {
                self.event.as_str() == Some("object_namespace_lock")
                    && self.op.as_str() == Some("data_movement_publication")
                    && self.bucket.as_str() == Some(bucket)
                    && self.object.as_str() == Some(object)
            }
        }
    }
}

fn select(line: &[u8], bucket: &str, object: &str, full_decodes: &mut u64) -> io::Result<Option<Value>> {
    let text = std::str::from_utf8(line).map_err(|_| invalid("invalid UTF-8 in a completed log row"))?;
    // Skip unknown fields without allocating a Value tree for every RPC trace.
    // A shape/duplicate-field error falls back to the original Value parser;
    // it must not discard valid escaped strings or last-key-wins JSON.
    if let Ok(probe) = serde_json::from_str::<Probe>(text)
        && !probe.matches(bucket, object)
    {
        return Ok(None);
    }
    *full_decodes += 1;
    let Ok(row) = serde_json::from_str::<Value>(text) else {
        return Ok(None);
    };
    let fields = row.get("fields").unwrap_or(&row);
    if fields.get("event").and_then(Value::as_str) != Some("object_namespace_lock")
        || fields.get("op").and_then(Value::as_str) != Some("data_movement_publication")
        || fields.get("bucket").and_then(Value::as_str) != Some(bucket)
        || fields.get("object").and_then(Value::as_str) != Some(object)
    {
        return Ok(None);
    }
    let mut event = fields.clone();
    event["timestamp"] = row["timestamp"].clone();
    Ok(Some(event))
}

struct Tail {
    path: PathBuf,
    handle: Handle,
    offset: u64,
    pending: Vec<u8>,
}

impl Tail {
    fn open(path: PathBuf) -> io::Result<Self> {
        let file = File::open(&path)?;
        if !file.metadata()?.is_file() {
            return Err(invalid("publication log is not a regular file"));
        }
        Ok(Self {
            path,
            handle: Handle::from_file(file)?,
            offset: 0,
            pending: Vec::new(),
        })
    }

    fn check_identity(&self) -> io::Result<()> {
        if Handle::from_path(&self.path)? != self.handle {
            return Err(invalid("publication log file was replaced"));
        }
        Ok(())
    }

    fn poll(&mut self, bytes_read: &mut u64, mut accept: impl FnMut(&[u8]) -> io::Result<()>) -> io::Result<()> {
        self.check_identity()?;
        let end = self.handle.as_file().metadata()?.len();
        if end < self.offset {
            return Err(invalid("publication log file was truncated"));
        }
        // A fixed high-water snapshot bounds each poll even if writers append
        // faster than the reader. Bytes after it remain for the next poll.
        let mut chunk = [0_u8; 64 * 1024];
        while self.offset < end {
            let size = usize::try_from((end - self.offset).min(64 * 1024)).expect("a log chunk fits usize");
            self.handle.as_file_mut().read_exact(&mut chunk[..size])?;
            self.offset += u64::try_from(size).expect("a bounded log read fits u64");
            *bytes_read += u64::try_from(size).expect("a bounded log read fits u64");
            let appended_at = self.pending.len();
            self.pending.extend_from_slice(&chunk[..size]);
            let mut start = 0;
            for end in self.pending[appended_at..]
                .iter()
                .enumerate()
                .filter_map(|(i, byte)| (*byte == b'\n').then_some(appended_at + i + 1))
            {
                if end - start > MAX_LINE_BYTES {
                    return Err(invalid("publication log row exceeds its fixed bound"));
                }
                accept(&self.pending[start..end])?;
                start = end;
            }
            self.pending.drain(..start);
            if self.pending.len() > MAX_LINE_BYTES {
                return Err(invalid("unfinished publication log row exceeds its fixed bound"));
            }
        }
        self.check_identity()
    }
}

pub(super) struct PublicationEvents {
    tails: Vec<Tail>,
    bucket: String,
    object: String,
    events: Vec<Value>,
    per_file: Vec<Vec<Value>>,
    event_count: usize,
    event_bytes: usize,
    bytes_read: u64,
    full_decodes: u64,
    failed: bool,
}

impl PublicationEvents {
    pub(super) fn new(paths: Vec<PathBuf>, bucket: &str, object: &str) -> io::Result<Self> {
        if paths.is_empty() || paths.len() > 4 {
            return Err(invalid("publication capture requires one to four log files"));
        }
        let tails: Vec<Tail> = paths.into_iter().map(Tail::open).collect::<io::Result<_>>()?;
        let mut reader = Self {
            per_file: vec![Vec::new(); tails.len()],
            tails,
            bucket: bucket.to_owned(),
            object: object.to_owned(),
            events: Vec::new(),
            event_bytes: 0,
            event_count: 0,
            bytes_read: 0,
            full_decodes: 0,
            failed: false,
        };
        // Prime before holding remote DELETE or starting a movement attempt.
        // Retain earlier matching events too: the caller still applies its
        // original timestamp boundary, and evidence keeps the complete history.
        reader.poll()?;
        Ok(reader)
    }

    pub(super) fn poll(&mut self) -> io::Result<&[Value]> {
        if self.failed {
            return Err(invalid("publication log reader was already invalidated"));
        }
        let result = (|| {
            for (tail, history) in self.tails.iter_mut().zip(&mut self.per_file) {
                tail.poll(&mut self.bytes_read, |line| {
                    if let Some(event) = select(line, &self.bucket, &self.object, &mut self.full_decodes)? {
                        let bytes = serde_json::to_vec(&event).map_err(io::Error::other)?.len();
                        if self.event_count >= MAX_EVENTS || bytes > MAX_EVENT_BYTES - self.event_bytes {
                            return Err(invalid("publication event history exceeds its fixed bound"));
                        }
                        self.event_bytes += bytes;
                        self.event_count += 1;
                        history.push(event);
                    }
                    Ok(())
                })?;
            }
            Ok(())
        })();
        // Preserve the old file-then-row order, even when a later poll appends
        // to an earlier file. Only bounded selected history is copied here.
        self.events = self.per_file.iter().flatten().cloned().collect();
        if let Err(error) = result {
            self.failed = true;
            return Err(error);
        }
        Ok(&self.events)
    }

    /// Diagnostics only: after a read error this history can be incomplete.
    pub(super) fn cached(&self) -> &[Value] {
        &self.events
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::io::Write;

    fn event(id: &str) -> Value {
        json!({"timestamp":"2026-10-26T00:00:00Z","event":"object_namespace_lock","op":"data_movement_publication","bucket":"bucket","object":"object","lock_attempt_id":id,"state":"acquiring","extra":{"kept":[1,2]}})
    }
    fn append(path: &std::path::Path, raw: &[u8]) {
        std::fs::OpenOptions::new()
            .append(true)
            .open(path)
            .expect("open append log")
            .write_all(raw)
            .expect("append log bytes");
    }
    fn legacy(raw: &[u8]) -> Vec<Value> {
        std::str::from_utf8(raw)
            .expect("fixture UTF-8")
            .lines()
            .filter_map(|line| {
                let row: Value = serde_json::from_str(line).ok()?;
                let fields = row.get("fields").unwrap_or(&row);
                if fields.get("event").and_then(Value::as_str) == Some("object_namespace_lock")
                    && fields.get("op").and_then(Value::as_str) == Some("data_movement_publication")
                    && fields.get("bucket").and_then(Value::as_str) == Some("bucket")
                    && fields.get("object").and_then(Value::as_str) == Some("object")
                {
                    let mut result = fields.clone();
                    result["timestamp"] = row["timestamp"].clone();
                    Some(result)
                } else {
                    None
                }
            })
            .collect()
    }

    #[test]
    fn appended_events_keep_full_fields_without_history_rescans() {
        let file = tempfile::NamedTempFile::new().expect("log file");
        let mut raw = (json!({"message":"unrelated trace","payload":"x".repeat(4096)}).to_string() + "\n")
            .repeat(64)
            .into_bytes();
        raw.extend_from_slice(
            format!("{}\n", json!({"event":"object_namespace_lock","op":"get_object","bucket":"bucket","object":"object","spans":"x".repeat(2401832)})).as_bytes(),
        );
        raw.extend_from_slice(format!("{}\n", event("first")).as_bytes());
        std::fs::write(file.path(), &raw).expect("seed log");
        let mut reader = PublicationEvents::new(vec![file.path().to_owned()], "bucket", "object").expect("prime log");
        assert_eq!(reader.cached(), legacy(&raw));
        assert_eq!(reader.full_decodes, 1);
        let bytes = reader.bytes_read;
        assert_eq!(reader.poll().expect("unchanged poll"), legacy(&raw));
        assert_eq!(reader.bytes_read, bytes);
        let next = format!("{}\n", event("second"));
        append(file.path(), next.as_bytes());
        raw.extend_from_slice(next.as_bytes());
        assert_eq!(reader.poll().expect("new rows"), legacy(&raw));
        assert_eq!(reader.bytes_read, u64::try_from(raw.len()).expect("fixture length"));
        assert_eq!(reader.full_decodes, 2);
    }

    #[test]
    fn later_appends_preserve_the_old_file_then_row_order() {
        let first = tempfile::NamedTempFile::new().expect("first log");
        let second = tempfile::NamedTempFile::new().expect("second log");
        std::fs::write(first.path(), format!("{}\n", event("a1"))).expect("seed first");
        std::fs::write(second.path(), format!("{}\n", event("b1"))).expect("seed second");
        let mut reader = PublicationEvents::new(vec![first.path().to_owned(), second.path().to_owned()], "bucket", "object")
            .expect("prime both");
        append(first.path(), format!("{}\n", event("a2")).as_bytes());
        let mut expected = legacy(&std::fs::read(first.path()).expect("first bytes"));
        expected.extend(legacy(&std::fs::read(second.path()).expect("second bytes")));
        assert_eq!(reader.poll().expect("append first after second was read"), expected);
    }

    #[test]
    fn root_nested_escaped_and_duplicate_fields_match_the_old_reader() {
        let file = tempfile::NamedTempFile::new().expect("log file");
        let root = event("root");
        let mut nested = json!({"timestamp":root["timestamp"],"fields":root.clone()});
        nested["fields"]["extra"] = json!({"unicode":"é"});
        let null = json!({"fields":null,"event":"object_namespace_lock","op":"data_movement_publication","bucket":"bucket","object":"object"});
        let escaped = root
            .to_string()
            .replace("object_namespace_lock", "\\u006fbject_namespace_lock");
        let duplicate = root.to_string().replacen("{", "{\"event\":\"earlier-value\",", 1);
        let raw = format!("{root}\n{nested}\n{null}\n{escaped}\n{duplicate}\nnot json\n{{\"fields\":[]}}\n");
        std::fs::write(file.path(), &raw).expect("mixed log");
        let reader = PublicationEvents::new(vec![file.path().to_owned()], "bucket", "object").expect("probe and fallback");
        assert_eq!(reader.cached(), legacy(raw.as_bytes()));
        assert_eq!(reader.cached().len(), 4);
    }

    #[test]
    fn incomplete_rows_and_split_utf8_wait_for_the_newline() {
        let file = tempfile::NamedTempFile::new().expect("log file");
        let mut row = event("partial");
        row["extra"] = json!("é");
        let raw = format!("{row}\n").into_bytes();
        let cut = raw.iter().position(|b| *b == 0xc3).expect("UTF-8 prefix") + 1;
        std::fs::write(file.path(), &raw[..cut]).expect("partial code point");
        let mut reader =
            PublicationEvents::new(vec![file.path().to_owned()], "bucket", "object").expect("incomplete row is pending");
        assert!(reader.cached().is_empty());
        append(file.path(), &raw[cut..raw.len() - 1]);
        assert!(reader.poll().expect("complete JSON without delimiter").is_empty());
        append(file.path(), b"\n");
        assert_eq!(reader.poll().expect("published row"), legacy(&raw));
    }

    #[test]
    fn truncation_is_sticky_even_after_the_file_grows_again() {
        let file = tempfile::NamedTempFile::new().expect("log file");
        std::fs::write(file.path(), format!("{}\n", event("first"))).expect("seed log");
        let mut reader = PublicationEvents::new(vec![file.path().to_owned()], "bucket", "object").expect("prime log");
        file.as_file().set_len(0).expect("truncate");
        assert!(reader.poll().is_err());
        append(file.path(), format!("{}\n", event("longer replacement")).as_bytes());
        assert!(reader.poll().is_err());
        assert_eq!(reader.cached().len(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn a_replaced_path_does_not_switch_to_the_new_log() {
        let dir = tempfile::tempdir().expect("log dir");
        let path = dir.path().join("node.log");
        std::fs::write(&path, b"").expect("log file");
        let mut reader = PublicationEvents::new(vec![path.clone()], "bucket", "object").expect("open log");
        std::fs::rename(&path, dir.path().join("retired.log")).expect("replace log");
        std::fs::write(&path, format!("{}\n", event("other-generation"))).expect("replacement");
        assert!(reader.poll().is_err());
        assert!(reader.cached().is_empty());
    }

    #[test]
    fn invalid_utf8_and_oversized_or_excessive_records_fail_closed() {
        let file = tempfile::NamedTempFile::new().expect("log file");
        let mut reader = PublicationEvents::new(vec![file.path().to_owned()], "bucket", "object").expect("empty log");
        append(file.path(), b"\xff\n");
        assert!(reader.poll().is_err());
        let file = tempfile::NamedTempFile::new().expect("log file");
        let mut reader = PublicationEvents::new(vec![file.path().to_owned()], "bucket", "object").expect("empty log");
        append(file.path(), &vec![b'x'; MAX_LINE_BYTES + 1]);
        assert!(reader.poll().is_err());
        let file = tempfile::NamedTempFile::new().expect("log file");
        let mut reader = PublicationEvents::new(vec![file.path().to_owned()], "bucket", "object").expect("empty log");
        for _ in 0..=MAX_EVENTS {
            append(file.path(), format!("{}\n", event("attempt")).as_bytes());
        }
        assert!(reader.poll().is_err());
        assert_eq!(reader.cached().len(), MAX_EVENTS);
        assert!(reader.poll().is_err());
        let file = tempfile::NamedTempFile::new().expect("log file");
        let mut reader = PublicationEvents::new(vec![file.path().to_owned()], "bucket", "object").expect("empty log");
        let mut large = event("large");
        large["extra"] = json!("x".repeat(600 * 1024));
        for _ in 0..2 {
            append(file.path(), format!("{large}\n").as_bytes());
        }
        assert!(reader.poll().is_err());
        assert_eq!(reader.cached().len(), 1);
        assert!(reader.poll().is_err());
    }
}
