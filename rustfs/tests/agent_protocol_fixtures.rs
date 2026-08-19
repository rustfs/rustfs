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

//! Conformance of this repository's copy of the Connect agent protocol fixtures.
//!
//! `fixture-sets.json` requires a byte-identical copy of every populated set,
//! and Connect's `make protocol-compat` runs this test by name (the Makefile's
//! `RUSTFS_CONSUMER_TESTS` default) after comparing the two trees. The
//! comparison there proves the copies match; this proves the copy is internally
//! consistent, so a fixture edited on this side is caught here even when
//! Connect is not checked out.

use std::fs;
use std::path::PathBuf;

use sha2::{Digest as _, Sha256};

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../protocol/agent/v1/fixtures")
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes).iter().map(|byte| format!("{byte:02x}")).collect()
}

/// The registry is closed at eight sets; a ninth is a protocol change, not a
/// fixture change. Mirrors `EXPECTED_SETS` in Connect's checker.
const EXPECTED_SETS: [&str; 8] = [
    "auth",
    "version",
    "registration",
    "heartbeat",
    "inventory",
    "offline-enrollment",
    "bundle",
    "redaction",
];

#[test]
fn agent_protocol_fixtures_registry_is_the_frozen_eight_sets() {
    let registry: serde_json::Value =
        serde_json::from_slice(&fs::read(fixture_root().join("fixture-sets.json")).expect("read fixture-sets.json"))
            .expect("fixture-sets.json parses");

    let names: Vec<&str> = registry["sets"]
        .as_array()
        .expect("sets is an array")
        .iter()
        .map(|set| set["name"].as_str().expect("set has a name"))
        .collect();

    assert_eq!(names, EXPECTED_SETS, "the fixture registry must stay closed and ordered");
    assert_eq!(
        registry["consumerCopy"]["path"].as_str(),
        Some("protocol/agent/v1/fixtures"),
        "this copy lives at the path the registry declares"
    );
}

#[test]
fn agent_protocol_fixtures_match_their_manifests() {
    let root = fixture_root();
    let registry: serde_json::Value =
        serde_json::from_slice(&fs::read(root.join("fixture-sets.json")).expect("read fixture-sets.json"))
            .expect("fixture-sets.json parses");

    let mut checked = 0usize;

    for set in registry["sets"].as_array().expect("sets is an array") {
        let name = set["name"].as_str().expect("set has a name");
        let status = set["status"].as_str().expect("set has a status");

        let set_dir = root.join(name);
        if status == "reserved" {
            assert!(!set_dir.exists(), "reserved fixture set '{name}' must hold no files yet");
            continue;
        }

        let manifest = fs::read_to_string(set_dir.join("MANIFEST.sha256"))
            .unwrap_or_else(|error| panic!("populated set '{name}' must carry a manifest: {error}"));

        let mut listed = Vec::new();
        for line in manifest.lines().filter(|line| !line.trim().is_empty()) {
            let (digest, file) = line
                .split_once("  ")
                .unwrap_or_else(|| panic!("malformed manifest line in '{name}': {line}"));
            listed.push(file.to_string());

            let bytes = fs::read(set_dir.join(file))
                .unwrap_or_else(|error| panic!("set '{name}' lists {file} which is missing: {error}"));
            assert_eq!(sha256_hex(&bytes), digest, "set '{name}' file {file} does not match its manifest");
            checked += 1;
        }

        // A file present but unlisted would travel unchecked, so the manifest
        // has to be exhaustive rather than merely correct about what it names.
        let mut present: Vec<String> = fs::read_dir(&set_dir)
            .expect("read fixture set directory")
            .map(|entry| entry.expect("read dir entry").file_name().to_string_lossy().into_owned())
            .filter(|file| file != "MANIFEST.sha256")
            .collect();
        present.sort();
        listed.sort();
        assert_eq!(present, listed, "set '{name}' holds files its manifest does not list");
    }

    assert!(checked > 0, "no fixture files were verified");
}
