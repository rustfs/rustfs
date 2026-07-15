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

/// `h:` + first 8 hex chars of sha256.
pub(crate) fn hash_value(value: &str) -> String {
    let digest = Sha256::digest(value.as_bytes());
    let hex: String = digest.iter().take(4).map(|b| format!("{b:02x}")).collect();
    format!("h:{hex}")
}

pub(crate) fn redact_text(text: &str) -> String {
    IPV4.replace_all(text, |caps: &regex::Captures<'_>| hash_value(&caps[0]))
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
        assert_eq!(a.len(), 10);
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
}
