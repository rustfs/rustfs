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

//! Deterministic redaction shared with Connect's frozen D05 fixture contract.

use std::sync::LazyLock;

use regex::Regex;
use serde::Serialize;
use serde_json::{Map, Value};
use thiserror::Error;

pub const REDACTION_VERSION: &str = "rustfs.connect.redaction.v1";
pub const RULESET_HASH: &str = "b37436d8e72515394a122d633865b1dc028d4ece349352a0a3a23f52ca4285f3";
const PLACEHOLDER: &str = "[REDACTED]";
const MAX_INPUT_BYTES: usize = 262_144;
const MAX_DEPTH: usize = 8;
const MAX_NODES: usize = 4_096;
const MAX_VALUE_BYTES: usize = 4_096;

const HEARTBEAT_FIELDS: &[&str] = &[
    "agentVersion",
    "capabilities",
    "clientTime",
    "coarseNodeSummary",
    "protocolVersion",
    "sequence",
];
const INVENTORY_FIELDS: &[&str] = &[
    "capacityTotalBytes",
    "capacityUsedBytes",
    "coarseFlags",
    "driveCount",
    "nodeCount",
    "osVersion",
    "rustfsVersion",
];
const OFFLINE_FIELDS: &[&str] = &[
    "capacityTotalBytes",
    "capacityUsedBytes",
    "coarseHealthFlags",
    "cpuSummary",
    "driveCount",
    "filesystemSummary",
    "kernelSummary",
    "memorySummary",
    "networkSummary",
    "nodeCount",
    "osSummary",
    "rustfsVersion",
];

const KEY_RULES: &[&str] = &[
    "accesskey",
    "accesskeyid",
    "apikey",
    "apitoken",
    "authorization",
    "bearertoken",
    "cookie",
    "credential",
    "credentials",
    "csrftoken",
    "kmskey",
    "kmskeyid",
    "kmsmasterkey",
    "kmssecret",
    "passphrase",
    "passwd",
    "password",
    "privatekey",
    "pwd",
    "refreshtoken",
    "registrationtoken",
    "secret",
    "secretaccesskey",
    "secretkey",
    "sessioncookie",
    "sessionid",
    "sessiontoken",
    "signingkey",
    "token",
    "xsrftoken",
];

static VALUE_RULES: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    [
        r"(?-u:\b)(?:A3T[A-Z0-9]{2}|ABIA|ACCA|AKIA|ASIA)[A-Z0-9]{16}(?-u:\b)",
        r"(?i:(?-u:\b)bearer\s{1,8}[A-Za-z0-9\-._~+/]{8,4096}={0,2})",
        r#"(?i:(?-u:\b)[a-z0-9_.-]{0,24}(?:access[_.-]?key(?:[_.-]?id)?|api[_.-]?key|credentials?|passphrase|secret(?:[_.-]?key)?|token)(?-u:\b)\s{0,8}[:=]\s{0,8}["']?[A-Za-z0-9\-._~+/=]{8,4096})"#,
        r"(?-u:\b)eyJ[A-Za-z0-9_-]{4,4096}\.[A-Za-z0-9_-]{4,4096}\.[A-Za-z0-9_-]{4,4096}",
        r"(?i:(?-u:\b)(?:passwd|password|pwd)(?-u:\b)\s{0,8}[:=]\s{0,8}\S)",
        r"-----BEGIN [A-Z0-9 ]{0,32}PRIVATE KEY(?: BLOCK)?-----",
        r#"(?i:(?-u:\b)(?:csrf[_.-]?token|jsessionid|phpsessid|sess|session|sid|xsrf[_.-]?token)(?:[_.-]?id)?(?-u:\b)\s{0,8}[:=]\s{0,8}["']?[A-Za-z0-9%\-._~+/]{12,4096})"#,
        r"(?-u:\b)[a-zA-Z][a-zA-Z0-9+.\-]{0,31}://[^\s/@:]{1,256}(?::[^\s/@]{0,256})?@",
    ]
    .into_iter()
    .map(|pattern| Regex::new(pattern).expect("the frozen redaction patterns are valid"))
    .collect()
});

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RedactionSource {
    Heartbeat,
    Inventory,
    OfflineDiagnostic,
}

impl TryFrom<&str> for RedactionSource {
    type Error = RedactionError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "heartbeat" => Ok(Self::Heartbeat),
            "inventory" => Ok(Self::Inventory),
            "offline-diagnostic" => Ok(Self::OfflineDiagnostic),
            _ => Err(RedactionError::UnknownSurface),
        }
    }
}

impl RedactionSource {
    fn allows(self, field: &str) -> bool {
        match self {
            Self::Heartbeat => HEARTBEAT_FIELDS.contains(&field),
            Self::Inventory => INVENTORY_FIELDS.contains(&field),
            Self::OfflineDiagnostic => OFFLINE_FIELDS.contains(&field),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RedactionCounts {
    pub dropped_field: usize,
    pub redacted_value: usize,
    pub redacted_oversize_value: usize,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RedactionResult {
    pub document: Map<String, Value>,
    pub canonical_json: String,
    pub redaction_version: &'static str,
    pub ruleset_hash: &'static str,
    pub redacted_count: usize,
    pub counts: RedactionCounts,
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum RedactionError {
    #[error("Redaction refused the document: it names no registered collection surface.")]
    UnknownSurface,
    #[error("Redaction refused the document: its size in bytes exceeds the frozen budget of 262144.")]
    InputTooLarge,
    #[error("Redaction refused the document: its nesting depth exceeds the frozen budget of 8.")]
    TooDeep,
    #[error("Redaction refused the document: its node count exceeds the frozen budget of 4096.")]
    TooManyNodes,
    #[error("Redaction refused the document: it is not representable as JSON.")]
    NotRepresentable,
}

pub(super) fn redact(source: RedactionSource, document: &Map<String, Value>) -> Result<RedactionResult, RedactionError> {
    let encoded = serde_json::to_vec(document).map_err(|_| RedactionError::NotRepresentable)?;
    if encoded.len() > MAX_INPUT_BYTES {
        return Err(RedactionError::InputTooLarge);
    }

    let mut counts = RedactionCounts::default();
    let allowed = document
        .iter()
        .filter_map(|(key, value)| {
            if source.allows(key) {
                Some((key.clone(), value.clone()))
            } else {
                counts.dropped_field += 1;
                None
            }
        })
        .collect();
    let mut nodes = 0;
    let (_, redacted) = walk_map(allowed, 0, &mut nodes, &mut counts)?;
    let canonical_json = serde_json::to_string(&redacted).map_err(|_| RedactionError::NotRepresentable)?;
    let redacted_count = counts.dropped_field + counts.redacted_value + counts.redacted_oversize_value;

    Ok(RedactionResult {
        document: redacted,
        canonical_json,
        redaction_version: REDACTION_VERSION,
        ruleset_hash: RULESET_HASH,
        redacted_count,
        counts,
    })
}

/// Redact a JSON object received at a protocol boundary. Invalid JSON and
/// non-object JSON are refused without including any input bytes in the error.
pub fn redact_json(source: RedactionSource, encoded: &[u8]) -> Result<RedactionResult, RedactionError> {
    if encoded.len() > MAX_INPUT_BYTES {
        return Err(RedactionError::InputTooLarge);
    }
    let Value::Object(document) = serde_json::from_slice(encoded).map_err(|_| RedactionError::NotRepresentable)? else {
        return Err(RedactionError::NotRepresentable);
    };
    redact(source, &document)
}

fn walk_map(
    map: Map<String, Value>,
    depth: usize,
    nodes: &mut usize,
    counts: &mut RedactionCounts,
) -> Result<(bool, Map<String, Value>), RedactionError> {
    check_depth(depth)?;
    let mut out = Map::new();
    for (key, value) in map {
        count_node(nodes)?;
        if !is_ascii_key(&key) {
            counts.dropped_field += 1;
            continue;
        }
        match value {
            Value::Object(nested) => {
                let (keep, nested) = walk_map(nested, depth + 1, nodes, counts)?;
                if keep {
                    out.insert(key, Value::Object(nested));
                } else {
                    counts.dropped_field += 1;
                }
            }
            Value::Array(list) => {
                out.insert(key.clone(), Value::Array(walk_list(list, &key, depth + 1, nodes, counts)?));
            }
            scalar => {
                out.insert(key.clone(), scrub_value(&key, scalar, counts));
            }
        }
    }
    out.sort_keys();
    Ok((!out.is_empty(), out))
}

fn walk_list(
    list: Vec<Value>,
    key: &str,
    depth: usize,
    nodes: &mut usize,
    counts: &mut RedactionCounts,
) -> Result<Vec<Value>, RedactionError> {
    check_depth(depth)?;
    let mut out = Vec::with_capacity(list.len());
    for value in list {
        count_node(nodes)?;
        match value {
            Value::Object(nested) => {
                let (keep, nested) = walk_map(nested, depth + 1, nodes, counts)?;
                if keep {
                    out.push(Value::Object(nested));
                } else {
                    counts.dropped_field += 1;
                }
            }
            Value::Array(nested) => out.push(Value::Array(walk_list(nested, key, depth + 1, nodes, counts)?)),
            scalar => out.push(scrub_value(key, scalar, counts)),
        }
    }
    Ok(out)
}

fn check_depth(depth: usize) -> Result<(), RedactionError> {
    if depth > MAX_DEPTH {
        Err(RedactionError::TooDeep)
    } else {
        Ok(())
    }
}

fn count_node(nodes: &mut usize) -> Result<(), RedactionError> {
    *nodes += 1;
    if *nodes > MAX_NODES {
        Err(RedactionError::TooManyNodes)
    } else {
        Ok(())
    }
}

fn scrub_value(key: &str, value: Value, counts: &mut RedactionCounts) -> Value {
    let Value::String(text) = value else {
        return value;
    };
    if text.len() > MAX_VALUE_BYTES {
        counts.redacted_oversize_value += 1;
        return Value::String(PLACEHOLDER.to_owned());
    }
    if redacts_key(key) || matches_value(&text) {
        counts.redacted_value += 1;
        return Value::String(PLACEHOLDER.to_owned());
    }
    Value::String(text)
}

fn is_ascii_key(key: &str) -> bool {
    (1..=64).contains(&key.len())
        && key.is_ascii()
        && key.as_bytes()[0].is_ascii_alphanumeric()
        && key.as_bytes()[1..]
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

fn redacts_key(key: &str) -> bool {
    let normalized: String = key
        .bytes()
        .filter(|byte| !matches!(byte, b'_' | b'-' | b'.'))
        .map(|byte| byte.to_ascii_lowercase() as char)
        .collect();
    KEY_RULES.contains(&normalized.as_str())
}

fn matches_value(value: &str) -> bool {
    matches_aws_secret_access_key(value) || VALUE_RULES.iter().any(|rule| rule.is_match(value))
}

fn matches_aws_secret_access_key(value: &str) -> bool {
    let bytes = value.as_bytes();
    let is_secret_char = |byte: u8| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/');
    let mut start = 0;
    while start < bytes.len() {
        while start < bytes.len() && !is_secret_char(bytes[start]) {
            start += 1;
        }
        let mut end = start;
        while end < bytes.len() && is_secret_char(bytes[end]) {
            end += 1;
        }
        let candidate = &bytes[start..end];
        if candidate.len() == 40
            && bytes.get(end) != Some(&b'=')
            && candidate.iter().any(u8::is_ascii_lowercase)
            && candidate.iter().any(u8::is_ascii_uppercase)
            && candidate.iter().any(u8::is_ascii_digit)
        {
            return true;
        }
        start = end.saturating_add(1);
    }
    false
}
