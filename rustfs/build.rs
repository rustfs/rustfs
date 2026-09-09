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

use std::path::Path;
use std::process::Command;

fn git(root: &Path, args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).current_dir(root).output().ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn main() -> shadow_rs::SdResult<()> {
    let manifest = std::env::var_os("CARGO_MANIFEST_DIR").ok_or("missing CARGO_MANIFEST_DIR")?;
    let root = Path::new(&manifest).join("..");
    println!("cargo:rerun-if-changed=build.rs");
    // Match the E2E build identity's source and configuration inputs.
    for path in [
        "crates",
        "rustfs",
        "Cargo.toml",
        "Cargo.lock",
        "rust-toolchain.toml",
        ".cargo",
        ".config",
    ] {
        let path = root.join(path);
        if path.exists() {
            println!("cargo:rerun-if-changed={}", path.display());
        }
    }
    // Resolve worktree-local HEAD/index and common refs through Git. Watching
    // refs also covers tags and a new loose ref created from packed refs.
    for name in ["HEAD", "index", "refs", "packed-refs"] {
        if let Some(path) = git(&root, &["rev-parse", "--git-path", name]) {
            let path = Path::new(&path);
            let path = if path.is_absolute() {
                path.to_owned()
            } else {
                root.join(path)
            };
            if path.exists() {
                println!("cargo:rerun-if-changed={}", path.display());
            }
        }
    }

    println!("cargo:rerun-if-env-changed=RUSTFS_BUILD_VERSION");
    if let Ok(version) = std::env::var("RUSTFS_BUILD_VERSION")
        && !version.is_empty()
    {
        assert!(
            !version.contains(['\n', '\r']),
            "RUSTFS_BUILD_VERSION must be a single-line version string"
        );
        println!("cargo:rustc-env=RUSTFS_BUILD_VERSION={version}");
    }

    shadow_rs::ShadowBuilder::builder().build()?;
    Ok(())
}
