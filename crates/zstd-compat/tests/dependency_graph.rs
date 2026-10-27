// Copyright 2026 RustFS Team
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

use semver::Version;
use std::path::Path;

#[test]
fn legacy_constraints_resolve_only_to_the_fixed_registry_implementation() {
    let directory = Path::new(env!("CARGO_MANIFEST_DIR"));
    let manifest: toml::Value =
        toml::from_str(&std::fs::read_to_string(directory.join("Cargo.toml")).expect("read adapter manifest"))
            .expect("parse adapter manifest");
    let dependencies = manifest["dependencies"].as_table().expect("runtime dependency table");
    assert_eq!(dependencies.len(), 1, "the adapter must have one canonical codec implementation");
    let implementation = &dependencies["zstd_fixed"];
    assert_eq!(implementation["package"].as_str(), Some("zstd"));
    assert!(implementation.get("path").is_none());
    assert!(implementation.get("git").is_none());

    let root = directory
        .parent()
        .expect("crates directory")
        .parent()
        .expect("workspace directory");
    let root_manifest: toml::Value =
        toml::from_str(&std::fs::read_to_string(root.join("Cargo.toml")).expect("read workspace manifest"))
            .expect("parse workspace manifest");
    let patch = root_manifest["patch"]["crates-io"]["zstd"]["path"]
        .as_str()
        .expect("local zstd patch");
    assert_eq!(
        root.join(patch).canonicalize().expect("resolve patch"),
        directory.canonicalize().expect("resolve adapter")
    );

    let lock: toml::Value = toml::from_str(&std::fs::read_to_string(root.join("Cargo.lock")).expect("read workspace lockfile"))
        .expect("parse workspace lockfile");
    let mut adapters = 0;
    let mut implementations = 0;
    let mut safe_bindings = 0;
    for package in lock["package"].as_array().expect("locked packages") {
        let name = package["name"].as_str().expect("package name");
        if !matches!(name, "zstd" | "zstd-safe") {
            continue;
        }
        let version = Version::parse(package["version"].as_str().expect("package version")).expect("valid package semver");
        if name == "zstd" && package.get("source").is_none() {
            adapters += 1;
            assert_eq!(version, Version::new(0, 13, 3));
            let runtime = package["dependencies"].as_array().expect("adapter dependency edges");
            assert!(runtime.iter().any(|value| value.as_str() == Some("zstd 0.14.3")));
        } else {
            let source = package["source"].as_str().expect("registry codec source");
            assert!(source.starts_with("registry+"), "codec implementation must remain a registry package");
            if name == "zstd" {
                implementations += 1;
                assert!(version >= Version::new(0, 14, 3), "vulnerable zstd implementation: {version}");
            } else {
                safe_bindings += 1;
                assert!(version >= Version::new(8, 2, 0), "vulnerable zstd-safe implementation: {version}");
            }
        }
    }
    assert_eq!((adapters, implementations, safe_bindings), (1, 1, 1));
}
