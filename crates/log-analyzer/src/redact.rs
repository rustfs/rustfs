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

//! Report redaction (`--redact`): customer identifiers are replaced by
//! stable hashes so a report can be forwarded while staying correlatable
//! (same value -> same hash). Rule ids, categories, module targets and panic
//! locations (RustFS source paths, not customer data) and timestamps are left
//! intact.
//!
//! Coverage is best-effort, not a guarantee: known sensitive fields are hashed
//! whole, every other field value and every free-text surface (messages,
//! evidence, unmatched templates, provenance) is run through [`redact_text`],
//! which hashes IPv4/IPv6 literals and `key=value` / `key: value` identifiers.
//! Residual free-form tokens that are neither field-shaped nor an IP (e.g. a
//! bucket name mentioned in prose) may still remain — reviewers should treat
//! `--redact` as scrubbing identifiers, not as a proof of full anonymization.

use crate::model::LogEvent;
use regex::Regex;
use sha2::{Digest, Sha256};
use std::sync::LazyLock;

/// Field names whose values are customer identifiers and are hashed whole.
const SENSITIVE_FIELDS: &[&str] = &[
    "bucket",
    "object",
    "key",
    "prefix",
    "access_key",
    "accessKey",
    "host",
    "remote_addr",
    "remotehost",
    "endpoint",
    "peer",
    "ip",
    "path",
    "file_path",
    "disk",
    "drive",
    "volume",
    "volumes",
    "url",
    "node",
    "user",
    "username",
    "resource",
];

static IPV4: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}(?::\d+)?\b").expect("static regex"));

/// IPv6 literals, compressed (`fe80::1`, `::1`, `2001:db8::2:1`) or full
/// 8-group. Deliberately requires a `::` or eight groups so it never matches a
/// Rust module path (`rustfs::server::http`) or a `HH:MM:SS` clock.
static IPV6: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(?:(?:[0-9a-f]{1,4}:){7}[0-9a-f]{1,4}|(?:[0-9a-f]{1,4}:)+:[0-9a-f]{0,4}(?::[0-9a-f]{1,4})*|::(?:[0-9a-f]{1,4})(?::[0-9a-f]{1,4})*)\b")
        .expect("static regex")
});

/// The same identifiers when they are embedded in message text rather than
/// structured fields: `access_key=AK123`, `bucket: media, object: a/b.bin`,
/// `path="/rustfs/data"`. The value ends at whitespace or a list/close
/// delimiter unless quoted. Keep the alternation in sync with
/// [`SENSITIVE_FIELDS`].
static FIELD_IN_TEXT: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"(?i)\b(bucket|object|key|prefix|access[_-]?key|host|remote_addr|remotehost|endpoint|peer|ip|path|file_path|disk|drive|volume|volumes|url|node|user|username|resource)(\s*[:=]\s*)("[^"]*"|'[^']*'|[^\s,;)\]}"']+)"#,
    )
    .expect("static regex")
});

/// `h:` + first 16 hex chars (64 bits) of sha256 — long enough that
/// collisions across a report's identifiers stay negligible.
pub(crate) fn hash_value(value: &str) -> String {
    let digest = Sha256::digest(value.as_bytes());
    let hex: String = digest.iter().take(8).map(|b| format!("{b:02x}")).collect();
    format!("h:{hex}")
}

pub(crate) fn redact_text(text: &str) -> String {
    // Field-shaped identifiers first (so `endpoint=1.2.3.4:9000` hashes the
    // whole value exactly like the structured field would), then bare IPs.
    let text = FIELD_IN_TEXT.replace_all(text, |caps: &regex::Captures<'_>| {
        let value = caps[3].trim_matches(['"', '\'']);
        format!("{}{}{}", &caps[1], &caps[2], hash_value(value))
    });
    let text = IPV4.replace_all(&text, |caps: &regex::Captures<'_>| hash_value(&caps[0]));
    IPV6.replace_all(&text, |caps: &regex::Captures<'_>| hash_value(&caps[0]))
        .into_owned()
}

/// Redacts customer provenance (`/home/acme/bundle/node1/rustfs.log`,
/// `support.zip!node1/rustfs.log`): the leaf name is diagnostic and kept, the
/// directory/archive prefix (which carries hostnames / org names) is hashed.
pub(crate) fn redact_provenance(provenance: &str) -> String {
    match provenance.rfind(['/', '!']) {
        Some(i) => format!("{}{}", hash_value(&provenance[..i]), &provenance[i..]),
        None => provenance.to_string(),
    }
}

/// Redacts a captured event in place: message + every field + provenance. Node
/// labels are hashed at ingestion time (see `Analyzer::observe`) so the whole
/// report — summary, timelines, samples — stays correlatable under one hash.
pub(crate) fn redact_event(event: &mut LogEvent) {
    event.message = redact_text(&event.message);
    for (name, value) in event.fields.iter_mut() {
        let rendered = match &*value {
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        if is_sensitive_field(name) {
            *value = serde_json::Value::String(hash_value(&rendered));
        } else {
            let redacted = redact_text(&rendered);
            // Only rewrite (and re-type to string) when something changed, so
            // untouched structural fields keep their original JSON type.
            if redacted != rendered {
                *value = serde_json::Value::String(redacted);
            }
        }
    }
    event.source.file = std::sync::Arc::from(redact_provenance(&event.source.file).as_str());
}

pub(crate) fn is_sensitive_field(name: &str) -> bool {
    SENSITIVE_FIELDS.iter().any(|f| f.eq_ignore_ascii_case(name))
}

/// Hashes a single value if `field` is sensitive, otherwise scrubs identifiers
/// out of it with [`redact_text`]. Used for evidence rendering.
pub(crate) fn redact_evidence_value(field: &str, value: &str) -> String {
    if is_sensitive_field(field) {
        hash_value(value)
    } else {
        redact_text(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hashing_is_stable_and_prefixed() {
        let a = hash_value("media-bucket");
        let b = hash_value("media-bucket");
        assert_eq!(a, b);
        assert!(a.starts_with("h:"));
        assert_eq!(a.len(), 18);
        assert_ne!(a, hash_value("other-bucket"));
    }

    #[test]
    fn ipv4_addresses_are_hashed_in_text() {
        let out = redact_text("peer 10.0.0.12:9000 unreachable, retry 10.0.0.12:9000");
        assert!(!out.contains("10.0.0.12"));
        // Same address hashes identically both times.
        let h = hash_value("10.0.0.12:9000");
        assert_eq!(out.matches(&h).count(), 2);
    }

    #[test]
    fn ipv6_addresses_are_hashed_but_paths_and_clocks_are_not() {
        let out = redact_text("peer fe80::1 and 2001:db8::2:1 down");
        assert!(!out.contains("fe80::1"), "got {out}");
        assert!(!out.contains("2001:db8::2:1"), "got {out}");
        assert_eq!(redact_text("fe80::1").matches(&hash_value("fe80::1")).count(), 1);
        // Rust module paths and wall-clock times must survive untouched.
        assert_eq!(redact_text("target rustfs::server::http ok"), "target rustfs::server::http ok");
        assert_eq!(redact_text("finished at 12:34:56 today"), "finished at 12:34:56 today");
        // Bracketed IPv6 with port: the address is scrubbed.
        assert!(!redact_text("dial [2001:db8::1]:9000").contains("2001:db8"));
    }

    #[test]
    fn field_shaped_identifiers_in_message_text_are_hashed() {
        let out = redact_text("authenticate_request: signature mismatch access_key=AK123");
        assert!(!out.contains("AK123"), "got {out}");
        assert_eq!(
            out,
            format!("authenticate_request: signature mismatch access_key={}", hash_value("AK123"))
        );

        let out = redact_text("reduce_read_quorum_errs: not found, bucket: media, object: private/a.bin");
        assert!(!out.contains("media"), "got {out}");
        assert!(!out.contains("private/a.bin"), "got {out}");
        assert!(out.contains(&format!("bucket: {}", hash_value("media"))), "got {out}");
        assert!(out.contains(&format!("object: {}", hash_value("private/a.bin"))), "got {out}");

        // Quoted values hash their inner content, matching the structured-field hash.
        let out = redact_text(r#"lifecycle skip path="/rustfs/data/disk1""#);
        assert!(out.contains(&format!("path={}", hash_value("/rustfs/data/disk1"))), "got {out}");

        // A field-shaped IP hashes the whole value exactly like the structured field would.
        let out = redact_text("dial failed endpoint=10.0.0.12:9000");
        assert_eq!(out, format!("dial failed endpoint={}", hash_value("10.0.0.12:9000")));

        // peer / disk are now field-shaped too.
        assert!(!redact_text("marked offline peer=node-3.corp:9000").contains("node-3.corp"));
        assert!(!redact_text("io error disk=/data/rustfs/disk1").contains("/data/rustfs/disk1"));
    }

    #[test]
    fn provenance_keeps_the_leaf_and_hashes_the_prefix() {
        let out = redact_provenance("/home/acme-corp/bundle/prod-node-01/rustfs.log");
        assert!(out.ends_with("/rustfs.log"), "got {out}");
        assert!(!out.contains("acme-corp"), "got {out}");
        assert!(!out.contains("prod-node-01"), "got {out}");

        let out = redact_provenance("support.zip!node1/rustfs.log");
        assert!(out.ends_with("/rustfs.log"), "got {out}");
        assert!(!out.contains("node1"), "got {out}");

        // A bare filename has no sensitive prefix to hash.
        assert_eq!(redact_provenance("rustfs.log"), "rustfs.log");
    }

    #[test]
    fn redact_event_hashes_sensitive_fields_and_scrubs_the_rest() {
        use crate::model::{EventKind, SourceRef};
        use std::sync::Arc;
        let mut ev = LogEvent {
            timestamp: None,
            level: None,
            target: Some("rustfs::rpc::peer".to_string()),
            message: "dial fe80::9 failed".to_string(),
            fields: serde_json::Map::new(),
            source: SourceRef {
                file: Arc::from("acme-bundle/node1/rustfs.log"),
                line: 7,
            },
            node: Some(Arc::from("node1")),
            kind: EventKind::Json,
        };
        ev.fields.insert("bucket".into(), serde_json::Value::String("media".into()));
        ev.fields
            .insert("peer".into(), serde_json::Value::String("10.0.0.7:9000".into()));
        ev.fields
            .insert("client_ip".into(), serde_json::Value::String("203.0.113.9".into()));
        ev.fields.insert("line_number".into(), serde_json::Value::from(780));
        ev.fields
            .insert("reason".into(), serde_json::Value::String("faulty_disk".into()));

        redact_event(&mut ev);

        assert!(!ev.message.contains("fe80::9"));
        assert_eq!(ev.fields["bucket"], serde_json::Value::String(hash_value("media")));
        assert_eq!(ev.fields["peer"], serde_json::Value::String(hash_value("10.0.0.7:9000")));
        // Non-listed field with an embedded IP is still scrubbed via redact_text.
        assert!(!ev.fields["client_ip"].as_str().expect("str").contains("203.0.113.9"));
        // Structural fields keep their type/value.
        assert_eq!(ev.fields["line_number"], serde_json::Value::from(780));
        assert_eq!(ev.fields["reason"], serde_json::Value::String("faulty_disk".into()));
        // Provenance leaf kept, prefix hashed.
        assert!(ev.source.file.ends_with("/rustfs.log"));
        assert!(!ev.source.file.contains("node1"));
    }
}
