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

use super::*;
use rustfs_gateway_types::persistence::{PersistedReplicationDestination, PersistedReplicationRule};

pub(super) fn replication_check_target(arn: &str, status: &str, error: Option<&str>) -> ReplicationCheckTargetStatus {
    ReplicationCheckTargetStatus {
        arn: arn.to_string(),
        bucket: format!("bucket-{arn}"),
        status: status.to_string(),
        error: error.map(ToOwned::to_owned),
        phases: ReplicationCheckPhases::default(),
    }
}

#[test]
fn build_replication_check_response_documents_active_mutations() {
    let response = build_replication_check_response(vec![
        replication_check_target("arn:a", "OK", None),
        replication_check_target("arn:z", "OK", None),
    ])
    .expect("response should build");

    let bytes = futures::executor::block_on(http_body_util::BodyExt::collect(response.into_parts().2))
        .expect("body should read")
        .to_bytes();
    let payload: serde_json::Value = serde_json::from_slice(&bytes).expect("response should be JSON");
    assert_eq!(payload["Status"], "OK");
    assert_eq!(payload["ActiveMutation"], true);
    assert_eq!(payload["ProbeNamespace"], REPLICATION_CHECK_PROBE_PREFIX);
    assert!(payload["MutationDescription"].as_str().unwrap_or_default().contains("Writes"));
    assert_eq!(payload["Targets"][0]["Arn"], "arn:a");
}

#[test]
fn build_replication_check_response_preserves_partial_target_results() {
    let response = build_replication_check_response(vec![
        replication_check_target("arn:z", "FAILED", Some("boom")),
        replication_check_target("arn:a", "OK", None),
    ])
    .expect("partial result should remain structured");

    let bytes = futures::executor::block_on(http_body_util::BodyExt::collect(response.into_parts().2))
        .expect("body should read")
        .to_bytes();
    let payload: serde_json::Value = serde_json::from_slice(&bytes).expect("response should be JSON");
    assert_eq!(payload["Status"], "FAILED");
    assert_eq!(payload["Targets"][0]["Arn"], "arn:a");
    assert_eq!(payload["Targets"][0]["Status"], "OK");
    assert_eq!(payload["Targets"][1]["Arn"], "arn:z");
    assert_eq!(payload["Targets"][1]["Error"], "boom");
}

#[test]
fn build_replication_check_response_rejects_empty_target_list_at_runtime_boundary() {
    let config = PersistedReplicationConfiguration {
        role: String::new(),
        rules: vec![],
    };
    let replication_targets = filter_replication_check_targets(BucketTargets::default(), &config);

    assert!(replication_targets.is_empty());
}

#[test]
fn format_replication_check_client_error_prefers_structured_access_denied() {
    let err =
        S3ClientError::with_metadata("AccessDenied: denied", None, Some("AccessDenied".to_string()), Some("denied".to_string()));

    let formatted = format_replication_check_client_error(&err, ReplicationCheckFailureContext::BucketCheck);
    assert_eq!(formatted, "target bucket check failed: access denied");
}

#[test]
fn format_replication_check_client_error_excludes_remote_message() {
    let err = S3ClientError::with_metadata(
        "InvalidRequest: bucket versioning is suspended",
        None,
        Some("InvalidRequest".to_string()),
        Some("bucket versioning is suspended".to_string()),
    );

    let formatted = format_replication_check_client_error(&err, ReplicationCheckFailureContext::VersioningCheck);
    assert_eq!(formatted, "target bucket versioning check failed: InvalidRequest");
    assert!(!formatted.contains("suspended"));
}

#[test]
fn replication_check_error_is_single_line_and_bounded() {
    let error = format!("first\nsecond {}", "é".repeat(REPLICATION_CHECK_ERROR_MAX_BYTES));
    let bounded = bound_replication_check_error(error);

    assert!(bounded.len() <= REPLICATION_CHECK_ERROR_MAX_BYTES);
    assert!(!bounded.contains('\n'));
    assert!(bounded.ends_with("..."));
}

#[test]
fn replication_check_unknown_transport_error_does_not_expose_secrets() {
    let err = S3ClientError::with_metadata(
        "https://ACCESS:SECRET@example.test/?X-Amz-Signature=signature",
        None,
        None,
        Some("Authorization: Bearer token".to_string()),
    );

    let formatted = format_replication_check_client_error(&err, ReplicationCheckFailureContext::BucketCheck);
    assert_eq!(formatted, "target bucket check failed: remote request failed");
    assert!(!formatted.contains("SECRET"));
    assert!(!formatted.contains("token"));
}

#[test]
fn format_replication_check_client_error_maps_replicate_permission_failures() {
    let err =
        S3ClientError::with_metadata("AccessDenied: denied", None, Some("AccessDenied".to_string()), Some("denied".to_string()));

    let replicate_object = format_replication_check_client_error(&err, ReplicationCheckFailureContext::ReplicateObject);
    assert_eq!(replicate_object, "s3:ReplicateObject permissions missing for replication user");

    let replicate_delete = format_replication_check_client_error(&err, ReplicationCheckFailureContext::ReplicateDeleteMarker);
    assert_eq!(replicate_delete, "s3:ReplicateDelete permissions missing for replication user");

    let delete_object = format_replication_check_client_error(&err, ReplicationCheckFailureContext::DeleteObjectVersion);
    assert_eq!(
        delete_object,
        "s3:ReplicateDelete/s3:DeleteObject permissions missing for replication user"
    );
}

#[test]
fn is_object_lock_not_enabled_error_recognizes_missing_configuration() {
    let code_only = S3ClientError::with_metadata(
        "ObjectLockConfigurationNotFoundError: missing",
        None,
        Some("ObjectLockConfigurationNotFoundError".to_string()),
        Some("missing".to_string()),
    );
    assert!(is_object_lock_not_enabled_error(&code_only));

    let message_only = S3ClientError::with_metadata(
        "Object Lock is not enabled for this bucket",
        None,
        None,
        Some("Object Lock is not enabled for this bucket".to_string()),
    );
    assert!(is_object_lock_not_enabled_error(&message_only));

    let access_denied =
        S3ClientError::with_metadata("AccessDenied: denied", None, Some("AccessDenied".to_string()), Some("denied".to_string()));
    assert!(!is_object_lock_not_enabled_error(&access_denied));
}

#[test]
fn filter_replication_check_targets_only_keeps_configured_replication_targets() {
    let targets = BucketTargets {
        targets: vec![
            BucketTarget {
                arn: "arn:replication:a".to_string(),
                target_type: BucketTargetType::ReplicationService,
                ..Default::default()
            },
            BucketTarget {
                arn: "arn:replication:b".to_string(),
                target_type: BucketTargetType::ReplicationService,
                ..Default::default()
            },
            BucketTarget {
                arn: "arn:ilm:c".to_string(),
                target_type: BucketTargetType::IlmService,
                ..Default::default()
            },
        ],
    };
    let config = PersistedReplicationConfiguration {
        role: String::new(),
        rules: vec![PersistedReplicationRule {
            delete_marker_replication: None,
            delete_replication: None,
            destination: PersistedReplicationDestination {
                bucket: "arn:replication:b".to_string(),
                ..Default::default()
            },
            existing_object_replication: None,
            filter: None,
            id: None,
            prefix: Some(String::new()),
            priority: None,
            source_selection_criteria: None,
            status: Status::ENABLED.to_string(),
        }],
    };

    let filtered = filter_replication_check_targets(targets, &config);

    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].arn, "arn:replication:b");
}

#[test]
fn validate_replication_check_config_targets_rejects_stale_enabled_rule_target() {
    let targets = BucketTargets {
        targets: vec![BucketTarget {
            arn: "arn:replication:a".to_string(),
            target_type: BucketTargetType::ReplicationService,
            ..Default::default()
        }],
    };
    let config = PersistedReplicationConfiguration {
        role: String::new(),
        rules: vec![PersistedReplicationRule {
            delete_marker_replication: None,
            delete_replication: None,
            destination: PersistedReplicationDestination {
                bucket: "arn:replication:missing".to_string(),
                ..Default::default()
            },
            existing_object_replication: None,
            filter: None,
            id: Some("rule-stale".to_string()),
            prefix: Some(String::new()),
            priority: None,
            source_selection_criteria: None,
            status: Status::ENABLED.to_string(),
        }],
    };

    let err = validate_replication_check_config_targets(&targets, &config).expect_err("stale target should be rejected");

    assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
    assert!(err.message().unwrap_or_default().contains("rule-stale"));
}
