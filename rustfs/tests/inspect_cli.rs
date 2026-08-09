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

//! End-to-end coverage for the offline inspect command dispatcher.

use std::process::Command;

#[test]
fn inspect_subcommand_reaches_the_offline_executor() {
    let drive = tempfile::tempdir().expect("drive tempdir");
    let output = Command::new(env!("CARGO_BIN_EXE_rustfs-cli"))
        .args([
            "inspect",
            "bucket-meta",
            "--path",
            drive.path().to_string_lossy().as_ref(),
            "--bucket",
            "interop",
        ])
        .output()
        .expect("run rustfs-cli inspect");

    assert!(!output.status.success(), "missing metadata must reach the executor and fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("mount source drives read-only"), "executor warning missing: {stderr}");
    assert!(stderr.contains("no drive yielded a readable shard"), "executor error missing: {stderr}");
}
