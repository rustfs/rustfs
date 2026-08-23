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

//! Offline collector and frozen redaction conformance tests.

use std::fs;
use std::path::PathBuf;

use rustfs::connect::offline::{CollectorError, RedactionSource, collect_offline_diagnostics, redact, redact_json};
use rustfs_madmin::{Disk, ITEM_OFFLINE, ITEM_ONLINE, StorageInfo};
use serde_json::{Map, Value, json};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../protocol/agent/v1/fixtures/redaction")
}

fn fixture_json(name: &str) -> Value {
    let manifest = fs::read_to_string(fixture_dir().join("MANIFEST.sha256")).expect("read fixture manifest");
    let expected = manifest
        .lines()
        .find_map(|line| {
            let (digest, file) = line.split_once("  ")?;
            (file == name).then_some(digest)
        })
        .unwrap_or_else(|| panic!("{name} is listed in the fixture manifest"));
    let bytes = fs::read(fixture_dir().join(name)).unwrap_or_else(|error| panic!("read {name}: {error}"));
    let actual = Sha256::digest(&bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    assert_eq!(actual, expected, "{name} matches the frozen manifest");
    serde_json::from_slice(&bytes).unwrap_or_else(|error| panic!("parse {name}: {error}"))
}

fn vectors(name: &str) -> Vec<Value> {
    fixture_json(name)["vectors"].as_array().expect("fixture vectors").clone()
}

fn object(value: &Value) -> Map<String, Value> {
    value.as_object().expect("fixture document is an object").clone()
}

#[test]
fn connect_offline_collectors_match_every_allowed_and_secret_redaction_vector() {
    let ruleset = fixture_json("ruleset.json");
    for fixture in ["allowed-vectors.json", "secret-vectors.json"] {
        for vector in vectors(fixture) {
            let name = vector["name"].as_str().expect("vector name");
            let source = RedactionSource::try_from(vector["source"].as_str().expect("vector source"))
                .unwrap_or_else(|error| panic!("{name}: {error}"));
            let result =
                redact(source, &object(&vector["document"])).unwrap_or_else(|error| panic!("{name} must be accepted: {error}"));
            assert_eq!(result.redaction_version, ruleset["redactionVersion"], "{name}: redaction version");
            assert_eq!(result.ruleset_hash, ruleset["rulesetHash"], "{name}: ruleset hash");
            assert_eq!(result.canonical_json, vector["expectedCanonicalJson"], "{name}: canonical bytes");
            if let Some(expected) = vector["expectedRedactedCount"].as_u64() {
                assert_eq!(result.redacted_count as u64, expected, "{name}: redacted count");
                assert_eq!(result.counts.dropped_field as u64, vector["expectedCounts"]["droppedField"], "{name}");
                assert_eq!(result.counts.redacted_value as u64, vector["expectedCounts"]["redactedValue"], "{name}");
                assert_eq!(
                    result.counts.redacted_oversize_value as u64, vector["expectedCounts"]["redactedOversizeValue"],
                    "{name}"
                );
            } else {
                assert_eq!(result.redacted_count, 0, "{name}: allowed vectors must not be changed");
            }
            if let Some(secrets) = vector["secretLiterals"].as_array() {
                for secret in secrets {
                    assert!(
                        !result.canonical_json.contains(secret.as_str().expect("secret literal")),
                        "{name}: a secret survived redaction"
                    );
                }
            }
        }
    }
}

#[test]
fn connect_offline_collectors_enforce_the_frozen_rejection_budgets() {
    for vector in vectors("rejection-vectors.json") {
        let name = vector["name"].as_str().expect("vector name");
        let expected = vector["expected"]["message"].as_str();
        let source = match RedactionSource::try_from(vector["source"].as_str().expect("source")) {
            Ok(source) => source,
            Err(error) => {
                assert_eq!(error.to_string(), expected.expect("refusal message"), "{name}");
                continue;
            }
        };
        let build = &vector["build"];
        let field = build["field"].as_str().unwrap_or("rustfsVersion");
        let document = match build["kind"].as_str().expect("builder kind") {
            "literal" => object(&build["document"]),
            "bulkStrings" => {
                let entries = build["entries"].as_u64().expect("entries") as usize;
                let bytes = build["valueBytes"].as_u64().expect("value bytes") as usize;
                let nested = (0..entries)
                    .map(|index| (format!("f{index}"), json!("a".repeat(bytes))))
                    .collect();
                Map::from_iter([(field.to_owned(), Value::Object(nested))])
            }
            "nestedDepth" => {
                let mut nested = json!({ "leaf": 1 });
                for _ in 0..build["depth"].as_u64().expect("depth") {
                    nested = json!({ "nested": nested });
                }
                Map::from_iter([(field.to_owned(), nested)])
            }
            "listNodes" => {
                let count = build["count"].as_u64().expect("count") as usize;
                Map::from_iter([(field.to_owned(), Value::Array(vec![json!(1); count]))])
            }
            "unrepresentable" => {
                let encoded = format!(r#"{{"{field}":NaN}}"#);
                assert_eq!(
                    redact_json(source, encoded.as_bytes()).expect_err(name).to_string(),
                    expected.expect("refusal message"),
                    "{name}"
                );
                continue;
            }
            kind => panic!("unknown fixture builder {kind}"),
        };
        match expected {
            Some(message) => assert_eq!(redact(source, &document).expect_err(name).to_string(), message, "{name}"),
            None => assert_eq!(
                redact(source, &document)
                    .unwrap_or_else(|error| panic!("{name}: {error}"))
                    .redacted_count,
                vector["expected"]["redactedCount"],
                "{name}"
            ),
        }
    }
}

#[tokio::test]
async fn connect_offline_collectors_emit_only_fixed_redacted_entries_and_honor_cancellation() {
    let storage = StorageInfo {
        disks: vec![
            Disk {
                endpoint: "node-a.private.example".to_owned(),
                drive_path: "/secret/customer/path-a".to_owned(),
                uuid: "private-drive-a".to_owned(),
                state: ITEM_ONLINE.to_owned(),
                total_space: 1_000,
                used_space: 400,
                ..Disk::default()
            },
            Disk {
                endpoint: "node-b.private.example".to_owned(),
                drive_path: "/secret/customer/path-b".to_owned(),
                uuid: "private-drive-b".to_owned(),
                state: ITEM_OFFLINE.to_owned(),
                total_space: 2_000,
                used_space: 500,
                healing: true,
                ..Disk::default()
            },
        ],
        ..StorageInfo::default()
    };
    let cancel = CancellationToken::new();
    let entries = collect_offline_diagnostics(&storage, &cancel)
        .await
        .expect("collect fixed offline entries");
    assert_eq!(entries.len(), 12);
    let encoded = serde_json::to_string(&entries).expect("manifest entries serialize");
    for forbidden in [
        "node-a.private.example",
        "node-b.private.example",
        "/secret/customer/path-a",
        "/secret/customer/path-b",
        "private-drive-a",
        "private-drive-b",
    ] {
        assert!(!encoded.contains(forbidden), "private storage metadata must not leave the collector");
    }
    assert!(entries.iter().all(|entry| entry.field_id.starts_with("offline.")));
    assert!(entries.iter().all(|entry| entry.canonical_json.len() <= 16 * 1024));

    cancel.cancel();
    assert!(matches!(
        collect_offline_diagnostics(&storage, &cancel).await,
        Err(CollectorError::Cancelled)
    ));

    let oversized = StorageInfo {
        disks: vec![Disk::default(); 4_097],
        ..StorageInfo::default()
    };
    let active = CancellationToken::new();
    assert!(matches!(
        collect_offline_diagnostics(&oversized, &active).await,
        Err(CollectorError::StorageTopologyTooLarge)
    ));
}
