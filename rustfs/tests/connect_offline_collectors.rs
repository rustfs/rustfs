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
#[cfg(target_os = "linux")]
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
#[cfg(target_os = "linux")]
use std::time::Duration;

#[cfg(target_os = "linux")]
use rustfs::connect::offline::{CollectorError, collect_offline_diagnostics};
use rustfs::connect::offline::{RedactionSource, redact_json};
#[cfg(target_os = "linux")]
use rustfs::connect::{
    CredentialStore, HeartbeatConfig, IdentityStore, InventoryError, InventoryFlag, InventorySchedule, InventorySnapshot,
    InventoryStatus, spawn_inventory_runtime,
};
use serde_json::{Map, Value, json};
use sha2::{Digest as _, Sha256};
#[cfg(target_os = "linux")]
use tokio::sync::watch;
#[cfg(target_os = "linux")]
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

fn redact_document(source: RedactionSource, document: &Map<String, Value>) -> rustfs::connect::offline::RedactionResult {
    let encoded = serde_json::to_vec(document).expect("fixture document is representable");
    redact_json(source, &encoded).expect("fixture document must be accepted")
}

#[test]
fn connect_offline_collectors_match_every_allowed_and_secret_redaction_vector() {
    let ruleset = fixture_json("ruleset.json");
    for fixture in ["allowed-vectors.json", "secret-vectors.json"] {
        for vector in vectors(fixture) {
            let name = vector["name"].as_str().expect("vector name");
            let source = RedactionSource::try_from(vector["source"].as_str().expect("vector source"))
                .unwrap_or_else(|error| panic!("{name}: {error}"));
            let result = redact_document(source, &object(&vector["document"]));
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
            Some(message) => {
                let encoded = serde_json::to_vec(&document).expect("rejection fixture document is representable");
                assert_eq!(redact_json(source, &encoded).expect_err(name).to_string(), message, "{name}")
            }
            None => assert_eq!(
                redact_document(source, &document).redacted_count,
                vector["expected"]["redactedCount"],
                "{name}"
            ),
        }
    }
}

#[test]
fn connect_offline_collectors_reject_oversize_raw_input_before_parsing() {
    let invalid = vec![b'!'; 262_145];
    assert_eq!(
        redact_json(RedactionSource::OfflineDiagnostic, &invalid)
            .expect_err("oversize raw input")
            .to_string(),
        "Redaction refused the document: its size in bytes exceeds the frozen budget of 262144."
    );
}

#[cfg(target_os = "linux")]
async fn wait_for_inventory(status: &mut watch::Receiver<InventoryStatus>) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if matches!(status.borrow_and_update().clone(), InventoryStatus::Unchanged { .. }) {
                return;
            }
            status.changed().await.expect("inventory status channel");
        }
    })
    .await
    .expect("inventory persistence timeout");
}

#[cfg(target_os = "linux")]
async fn wait_for_inventory_failure(status: &mut watch::Receiver<InventoryStatus>) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if matches!(status.borrow_and_update().clone(), InventoryStatus::Failed { .. }) {
                return;
            }
            status.changed().await.expect("inventory status channel");
        }
    })
    .await
    .expect("inventory failure timeout");
}

#[cfg(target_os = "linux")]
fn private_directory(path: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt as _;

    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).expect("private directory permissions");
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn connect_offline_collectors_read_only_the_stopped_runtime_inventory() {
    let temp = tempfile::tempdir_in(env!("CARGO_MANIFEST_DIR")).expect("safe temporary directory");
    let state = temp.path().join("state");
    fs::create_dir(&state).expect("state root");
    private_directory(&state);
    let config = HeartbeatConfig::new(
        "",
        Vec::new(),
        IdentityStore::new(state.join("identity")),
        CredentialStore::new(state.join("credential")),
        state.join("heartbeat/state.json"),
    );
    let shutdown = CancellationToken::new();
    let runtime = spawn_inventory_runtime(
        Some(config),
        InventorySchedule {
            cadence: Duration::from_secs(60),
            jitter: Duration::ZERO,
        },
        &shutdown,
        || {
            std::future::ready(InventorySnapshot::new(
                "1.4.2",
                None,
                2,
                3,
                6_000,
                1_500,
                [InventoryFlag::ClusterDegraded, InventoryFlag::DriveOffline],
            ))
        },
    )
    .expect("state-only inventory")
    .expect("configured inventory");
    let mut status = runtime.status();
    wait_for_inventory(&mut status).await;

    assert!(matches!(
        collect_offline_diagnostics(&state, &CancellationToken::new()).await,
        Err(CollectorError::Inventory(InventoryError::AlreadyRunning))
    ));
    runtime.shutdown().await;

    let diagnostics = collect_offline_diagnostics(&state, &CancellationToken::new())
        .await
        .expect("collect fixed offline entries");
    assert_eq!(diagnostics.entries.len(), 12);
    assert!(diagnostics.inventory_captured_at.ends_with('Z'));
    assert!(diagnostics.inventory_age < Duration::from_secs(10));
    assert!(diagnostics.entries.iter().all(|entry| entry.field_id.starts_with("offline.")));
    assert!(
        diagnostics
            .entries
            .iter()
            .all(|entry| entry.canonical_json.len() <= 16 * 1024)
    );
    let canonical = |field_id| {
        diagnostics
            .entries
            .iter()
            .find(|entry| entry.field_id == field_id)
            .unwrap_or_else(|| panic!("missing {field_id}"))
            .canonical_json
            .as_str()
    };
    assert_eq!(canonical("offline.rustfsVersion"), r#"{"rustfsVersion":"1.4.2"}"#);
    assert_eq!(canonical("offline.nodeCount"), r#"{"nodeCount":2}"#);
    assert_eq!(canonical("offline.driveCount"), r#"{"driveCount":3}"#);
    assert_eq!(canonical("offline.capacityUsedBytes"), r#"{"capacityUsedBytes":1500}"#);
    assert_eq!(canonical("offline.capacityTotalBytes"), r#"{"capacityTotalBytes":6000}"#);
    assert_eq!(
        canonical("offline.coarseHealthFlags"),
        r#"{"coarseHealthFlags":["cluster.degraded","drive.offline"]}"#
    );

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert!(matches!(
        collect_offline_diagnostics(&state, &cancelled).await,
        Err(CollectorError::Cancelled)
    ));

    fs::write(state.join("inventory/latest.json"), b"{}").expect("corrupt latest inventory");
    assert!(matches!(
        collect_offline_diagnostics(&state, &CancellationToken::new()).await,
        Err(CollectorError::Inventory(InventoryError::EnvelopeInvalid))
    ));
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn connect_offline_collectors_reject_a_live_runtime_after_its_task_fails() {
    let temp = tempfile::tempdir_in(env!("CARGO_MANIFEST_DIR")).expect("safe temporary directory");
    let state = temp.path().join("state");
    fs::create_dir(&state).expect("state root");
    private_directory(&state);
    let config = HeartbeatConfig::new(
        "",
        Vec::new(),
        IdentityStore::new(state.join("identity")),
        CredentialStore::new(state.join("credential")),
        state.join("heartbeat/state.json"),
    );
    let attempts = Arc::new(AtomicUsize::new(0));
    let sample_attempts = attempts.clone();
    let runtime = spawn_inventory_runtime(
        Some(config),
        InventorySchedule {
            cadence: Duration::from_millis(1),
            jitter: Duration::ZERO,
        },
        &CancellationToken::new(),
        move || {
            let attempt = sample_attempts.fetch_add(1, Ordering::SeqCst);
            std::future::ready(if attempt == 0 {
                InventorySnapshot::new("1.4.2", None, 1, 1, 100, 50, [])
            } else {
                Err(InventoryError::Capacity)
            })
        },
    )
    .expect("state-only inventory")
    .expect("configured inventory");
    let mut status = runtime.status();
    wait_for_inventory_failure(&mut status).await;

    assert!(matches!(
        collect_offline_diagnostics(&state, &CancellationToken::new()).await,
        Err(CollectorError::Inventory(InventoryError::AlreadyRunning))
    ));
    runtime.shutdown().await;
    collect_offline_diagnostics(&state, &CancellationToken::new())
        .await
        .expect("collector after runtime shutdown");
}
