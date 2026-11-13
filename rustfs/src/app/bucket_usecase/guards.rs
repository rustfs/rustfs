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

//! Source-text guards over the bucket use-case sources.

/// Production text of this file plus `bucket_usecase/`, where a split moves its methods.
fn bucket_usecase_sources() -> crate::guard_sources::SourceSet {
    crate::guard_sources::SourceSet::production("src/app/bucket_usecase.rs", "src/app/bucket_usecase")
}

/// The one `async fn method` of the set, through its own closing line, so a neighbour of any
/// visibility or a test module can never satisfy a check about it.
fn usecase_method_source<'a>(source: &'a crate::guard_sources::SourceSet, method: &str) -> &'a str {
    source.method_body(method)
}

#[test]
fn bucket_create_and_delete_use_admitted_fresh_tasks() {
    let sources = bucket_usecase_sources();
    let source = &sources;
    for (method, operation, inner_call) in [
        (
            "execute_create_bucket",
            "bucket creation",
            "execute_create_bucket_inner(envelope, input, mutation_permit).await",
        ),
        ("execute_delete_bucket", "bucket deletion", "execute_delete_bucket_inner(req).await"),
    ] {
        let body = usecase_method_source(source, method);
        assert!(
            body.contains(&format!("await_bucket_usecase_on_fresh_task(\"{operation}\"")),
            "{method} must use bounded detached-task admission"
        );
        assert!(body.contains(inner_call), "{method} must run its mutation inside the admitted task");
    }
    let create_body = usecase_method_source(source, "execute_create_bucket_inner");
    assert!(
        create_body.contains("notify_bucket_metadata_reload_with_context("),
        "a successful bucket creation must refresh peer metadata caches"
    );
}

#[test]
fn bucket_metadata_config_changes_notify_peer_metadata_reload() {
    let sources = bucket_usecase_sources();
    let source = &sources;
    for (method, operation, scanner_maintenance_change) in [
        ("execute_delete_bucket_policy", "delete bucket policy", false),
        ("execute_put_bucket_policy", "put bucket policy", false),
        ("execute_delete_public_access_block", "delete public access block", false),
        ("execute_put_public_access_block", "put public access block", false),
        ("execute_delete_bucket_lifecycle", "delete bucket lifecycle", true),
        ("execute_put_bucket_lifecycle_configuration", "put bucket lifecycle", true),
        ("execute_put_bucket_versioning", "put bucket versioning", false),
        ("execute_delete_bucket_tagging", "delete bucket tagging", false),
        ("execute_put_bucket_tagging", "put bucket tagging", false),
        ("execute_delete_bucket_replication", "delete bucket replication", true),
        ("execute_put_bucket_replication", "put bucket replication", true),
        ("execute_delete_bucket_cors", "delete bucket cors", false),
        ("execute_put_bucket_cors", "put bucket cors", false),
        ("execute_delete_bucket_encryption", "delete bucket encryption", false),
        ("execute_put_bucket_encryption", "put bucket encryption", false),
        ("execute_put_bucket_notification_configuration", "put bucket notification", false),
    ] {
        let body = usecase_method_source(source, method);
        assert!(
            body.contains("notify_bucket_metadata_reload("),
            "{method} should notify peers to reload cached bucket metadata"
        );
        assert!(
            body.contains(operation),
            "{method} should identify the bucket metadata operation in reload logs"
        );
        let expected_reload = format!(
            "notify_bucket_metadata_reload(bucket.clone(), \"{operation}\", request_context, {scanner_maintenance_change}).await;"
        );
        assert!(
            body.contains(&expected_reload),
            "{method} should propagate scanner_maintenance_change={scanner_maintenance_change}"
        );
    }
}
