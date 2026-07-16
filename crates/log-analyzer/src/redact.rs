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
//! (same value -> same hash). Rule ids, targets, panic locations (source
//! paths, not customer data) and timestamps are left intact.

use crate::model::LogEvent;
use regex::Regex;
use sha2::{Digest, Sha256};
use std::sync::LazyLock;

/// Field names whose values are customer identifiers.
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
    "ip",
    "path",
    "file_path",
    "volumes",
    "resource",
];

static IPV4: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}(?::\d+)?\b").expect("static regex"));

/// The same identifiers when they are embedded in message text rather than
/// structured fields: `access_key=AK123`, `bucket: media, object: a/b.bin`,
/// `path="/rustfs/data"`. The value ends at whitespace or a list/close
/// delimiter unless quoted. Keep the alternation in sync with
/// [`SENSITIVE_FIELDS`].
static FIELD_IN_TEXT: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"(?i)\b(bucket|object|key|prefix|access[_-]?key|host|remote_addr|remotehost|endpoint|ip|path|file_path|volumes|resource)(\s*[:=]\s*)("[^"]*"|'[^']*'|[^\s,;)\]}"']+)"#,
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
    IPV4.replace_all(&text, |caps: &regex::Captures<'_>| hash_value(&caps[0]))
        .into_owned()
}

pub(crate) fn redact_event(event: &mut LogEvent) {
    event.message = redact_text(&event.message);
    for name in SENSITIVE_FIELDS {
        if let Some(value) = event.fields.get_mut(*name) {
            let rendered = match &*value {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            *value = serde_json::Value::String(hash_value(&rendered));
        }
    }
}

pub(crate) fn is_sensitive_field(name: &str) -> bool {
    SENSITIVE_FIELDS.contains(&name)
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
    }
}
