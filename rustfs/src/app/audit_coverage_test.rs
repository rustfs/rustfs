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

//! Operations without a notification event must emit one use-case audit entry.
use crate::guard_sources::SourceSet;
use rustfs_s3_ops::S3Operation;

fn is_read_only(op: S3Operation) -> bool {
    let name = op.as_str().trim_start_matches("s3:");
    ["Get", "Head", "List", "Select"]
        .iter()
        .any(|prefix| name.starts_with(prefix))
}

fn audit_sites(use_cases: &SourceSet, op: S3Operation) -> usize {
    let helper = format!("OperationHelper::audit_only_from_view(&envelope.audit_view(), S3Operation::{op:?}");
    use_cases.count_ignoring_whitespace(&format!("audit_only_operation(envelope, S3Operation::{op:?},"))
        + use_cases.count_ignoring_whitespace(&format!("{helper})"))
        + use_cases.count_ignoring_whitespace(&format!("{helper},)"))
}

#[test]
fn mutating_operations_without_a_notification_event_are_audited_once() {
    let mut expected: Vec<S3Operation> = S3Operation::ALL
        .into_iter()
        .filter(|op| !is_read_only(*op) && op.to_event_name().is_none())
        .collect();
    // AbortMultipartUpload has an internal event but no handler-built helper.
    expected.push(S3Operation::AbortMultipartUpload);
    assert_eq!(expected.len(), 26, "an operation was added or removed: {expected:?}");

    let use_cases = SourceSet::production("src/app/mod.rs", "src/app");
    for op in expected {
        assert_eq!(audit_sites(&use_cases, op), 1, "expected one audit site for {op:?}");
    }
}

const OWNED_HELPER: &str =
    "let helper = OperationHelper::audit_only_from_view(\n    &envelope.audit_view(),\n    S3Operation::UploadPart,\n);";
const OWNED_WRAPPER: &str =
    "RequestEnvelope::audit_only_operation(\n    envelope,\n    S3Operation::UploadPart,\n    StatusCode::OK,\n    run,\n)";

fn count_sites(source: &str) -> usize {
    let root = tempfile::tempdir().unwrap();
    let app = root.path().join("src/app");
    std::fs::create_dir_all(&app).unwrap();
    std::fs::write(app.join("mod.rs"), source).unwrap();
    let sources = SourceSet::production_under(root.path(), "src/app/mod.rs", "src/app");
    audit_sites(&sources, S3Operation::UploadPart)
}

#[test]
fn the_guard_accepts_exactly_one_audit_place() {
    assert_eq!(count_sites(OWNED_HELPER), 1);
    assert_eq!(count_sites(OWNED_WRAPPER), 1);
}

#[test]
fn the_guard_refuses_an_operation_audited_nowhere_or_twice() {
    assert_eq!(count_sites(""), 0);
    assert_eq!(count_sites(&format!("{OWNED_HELPER}\n{OWNED_WRAPPER}")), 2);
    assert_eq!(count_sites(&format!("#[cfg(test)]\nmod tests {{\n{OWNED_HELPER}\n}}")), 0);
    assert_eq!(count_sites(&OWNED_WRAPPER.replace("::UploadPart,", "::UploadPartCopy,")), 0);
    assert_eq!(count_sites(&OWNED_HELPER.replace("::UploadPart,", "::UploadPartCopy,")), 0);
}
