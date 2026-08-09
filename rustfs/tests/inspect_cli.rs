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

fn decode_hex(source: &str) -> Vec<u8> {
    let digits = source
        .chars()
        .filter(|character| character.is_ascii_hexdigit())
        .collect::<String>();
    digits
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).expect("hex pair"), 16).expect("fixture hex byte"))
        .collect()
}

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

#[test]
fn inspect_success_reports_persisted_header_and_all_config_timestamps() {
    let drive = tempfile::tempdir().expect("drive tempdir");
    let object_dir = drive.path().join(".rustfs.sys/buckets/interop/.metadata.bin");
    std::fs::create_dir_all(&object_dir).expect("create metadata object directory");
    let fixture = decode_hex(include_str!("../../crates/ecstore/tests/fixtures/minio/bucket_metadata_full.xlmeta.hex"));
    std::fs::write(object_dir.join("xl.meta"), fixture).expect("write metadata fixture");

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

    assert!(output.status.success(), "valid metadata inspection failed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let expected_lines = [
        "bucket   : interop",
        "format   : 1",
        "version  : 1",
        "created  : 2026-07-07T15:58:57.210712Z",
        "notification.xml                    231  2026-07-07T15:58:57.614429Z",
        "lifecycle.xml                       344  2026-07-07T15:58:57.337595Z",
        "object-lock.xml                     184  2026-07-07T15:58:57.293303Z",
        "versioning.xml                      123  2026-07-07T15:58:57.254109Z",
        "bucket-encryption.xml               240  2026-07-07T15:58:57.554116Z",
        "tagging.xml                         128  2026-07-07T15:58:57.378177Z",
        "replication.xml                     639  2026-07-07T15:59:20.148526Z",
        "cors.xml                              0  -",
        "logging.xml                           0  -",
        "website.xml                           0  -",
        "accelerate.xml                        0  -",
        "request-payment.xml                   0  -",
        "public-access-block.xml               0  -",
    ];
    for expected in expected_lines {
        assert!(stdout.lines().any(|line| line == expected), "missing stdout line {expected:?}:\n{stdout}");
    }
}
