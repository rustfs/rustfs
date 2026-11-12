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
use rustfs_gateway_types::dto::Status;
use rustfs_gateway_types::persistence::{PersistedReplicationDestination, PersistedReplicationRule, PersistedReplicationStatus};
use time::macros::datetime;

#[test]
fn replication_extension_policy_action_uses_write_permission_for_active_check() {
    assert_eq!(
        replication_extension_policy_action(ReplicationExtRoute::MetricsV1),
        Action::S3Action(S3Action::GetReplicationConfigurationAction)
    );
    assert_eq!(
        replication_extension_policy_action(ReplicationExtRoute::Check),
        Action::S3Action(S3Action::PutReplicationConfigurationAction)
    );
    assert_eq!(
        replication_extension_policy_action(ReplicationExtRoute::ResetStart),
        Action::S3Action(S3Action::ResetBucketReplicationStateAction)
    );
}

#[test]
fn parse_reset_start_target_defaults_reset_before_and_supports_older_than() {
    let no_window: Uri = "/demo-bucket?replication-reset".parse().expect("uri should parse");
    let before_default = OffsetDateTime::now_utc();
    let parsed_default = parse_reset_start_target(&no_window).expect("default reset request should parse");
    let after_default = OffsetDateTime::now_utc();

    assert!(parsed_default.arn.is_empty());
    assert!(!parsed_default.reset_id.is_empty());
    let reset_before = parsed_default.reset_before.expect("default reset window should be set");
    assert!(reset_before >= before_default && reset_before <= after_default);

    let older_than: Uri = "/demo-bucket?replication-reset&arn=arn:target&reset-id=rid-1&older-than=1h"
        .parse()
        .expect("uri should parse");
    let before_window = OffsetDateTime::now_utc();
    let parsed_window = parse_reset_start_target(&older_than).expect("older-than reset request should parse");
    let after_window = OffsetDateTime::now_utc();

    assert_eq!(parsed_window.reset_id, "rid-1");
    let reset_before = parsed_window.reset_before.expect("older-than reset window should be set");
    assert!(reset_before <= after_window - time::Duration::minutes(59));
    assert!(reset_before >= before_window - time::Duration::hours(1) - time::Duration::seconds(1));
}

#[test]
fn resolve_replication_reset_target_arn_uses_single_existing_object_target_by_default() {
    let config = PersistedReplicationConfiguration {
        role: String::new(),
        rules: vec![PersistedReplicationRule {
            delete_marker_replication: None,
            delete_replication: None,
            destination: PersistedReplicationDestination {
                bucket: "arn:replication:a".to_string(),
                ..Default::default()
            },
            existing_object_replication: Some(PersistedReplicationStatus {
                status: Status::ENABLED.to_string(),
            }),
            filter: None,
            id: Some("rule-a".to_string()),
            prefix: Some(String::new()),
            priority: None,
            source_selection_criteria: None,
            status: Status::ENABLED.to_string(),
        }],
    };

    let resolved = resolve_replication_reset_target_arn(&config, "").expect("single target should resolve");
    assert_eq!(resolved, "arn:replication:a");
}

#[test]
fn resolve_replication_reset_target_arn_requires_arn_for_multiple_targets() {
    let config = PersistedReplicationConfiguration {
        role: String::new(),
        rules: vec![
            PersistedReplicationRule {
                delete_marker_replication: None,
                delete_replication: None,
                destination: PersistedReplicationDestination {
                    bucket: "arn:replication:a".to_string(),
                    ..Default::default()
                },
                existing_object_replication: Some(PersistedReplicationStatus {
                    status: Status::ENABLED.to_string(),
                }),
                filter: None,
                id: Some("rule-a".to_string()),
                prefix: Some(String::new()),
                priority: None,
                source_selection_criteria: None,
                status: Status::ENABLED.to_string(),
            },
            PersistedReplicationRule {
                delete_marker_replication: None,
                delete_replication: None,
                destination: PersistedReplicationDestination {
                    bucket: "arn:replication:b".to_string(),
                    ..Default::default()
                },
                existing_object_replication: Some(PersistedReplicationStatus {
                    status: Status::ENABLED.to_string(),
                }),
                filter: None,
                id: Some("rule-b".to_string()),
                prefix: Some(String::new()),
                priority: None,
                source_selection_criteria: None,
                status: Status::ENABLED.to_string(),
            },
        ],
    };

    let err = resolve_replication_reset_target_arn(&config, "").expect_err("multiple targets should require arn");
    assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
    assert!(err.message().unwrap_or_default().contains("arn query parameter is required"));
}

#[test]
fn resolve_replication_reset_target_arn_rejects_target_without_existing_object_replication() {
    let config = PersistedReplicationConfiguration {
        role: String::new(),
        rules: vec![PersistedReplicationRule {
            delete_marker_replication: None,
            delete_replication: None,
            destination: PersistedReplicationDestination {
                bucket: "arn:replication:a".to_string(),
                ..Default::default()
            },
            existing_object_replication: Some(PersistedReplicationStatus {
                status: Status::DISABLED.to_string(),
            }),
            filter: None,
            id: Some("rule-a".to_string()),
            prefix: Some(String::new()),
            priority: None,
            source_selection_criteria: None,
            status: Status::ENABLED.to_string(),
        }],
    };

    let err = resolve_replication_reset_target_arn(&config, "arn:replication:a")
        .expect_err("target without existing object replication should fail");
    assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
    assert!(err.message().unwrap_or_default().contains("existing object replication"));
}

#[test]
fn active_replication_resync_conflict_maps_to_http_conflict() {
    let error = replication_resync_active_conflict_error("run-active");

    assert_eq!(error.code(), &S3ErrorCode::OperationAborted);
    assert_eq!(error.status_code(), Some(StatusCode::CONFLICT));
    assert!(error.message().unwrap_or_default().contains("run-active"));
}

#[test]
fn replication_reset_response_matches_madmin_resync_targets_info_shape() {
    let payload = serde_json::to_value(ReplicationResetResponse {
        targets: vec![ReplicationResetTarget {
            arn: "arn:minio:replication::depl:bucket".to_string(),
            reset_id: "rid-1".to_string(),
        }],
    })
    .expect("reset response must serialize");

    // madmin-go `ResyncTargetsInfo` json tags: shell `target`, fields `arn`/`resetid`.
    assert_eq!(payload["target"][0]["arn"], "arn:minio:replication::depl:bucket");
    assert_eq!(payload["target"][0]["resetid"], "rid-1");
    assert!(payload.get("Targets").is_none());
}

#[test]
fn build_replication_reset_status_response_serializes_sorted_targets() {
    let mut status = BucketReplicationResyncStatus::new();
    status.targets_map.insert(
        "arn:z".to_string(),
        crate::admin::storage_api::bucket::replication::TargetReplicationResyncStatus {
            resync_id: "rid-z".to_string(),
            last_update: Some(datetime!(2025-01-03 00:00 UTC)),
            resync_status: crate::admin::storage_api::bucket::replication::ResyncStatusType::ResyncFailed,
            failed_count: 2,
            failed_size: 4,
            bucket: "bucket-z".to_string(),
            error: Some("boom".to_string()),
            ..Default::default()
        },
    );
    status.targets_map.insert(
        "arn:a".to_string(),
        crate::admin::storage_api::bucket::replication::TargetReplicationResyncStatus {
            resync_id: "rid-a".to_string(),
            start_time: Some(datetime!(2025-01-01 00:00 UTC)),
            last_update: Some(datetime!(2025-01-02 00:00 UTC)),
            resync_status: crate::admin::storage_api::bucket::replication::ResyncStatusType::ResyncCompleted,
            replicated_count: 3,
            replicated_size: 9,
            bucket: "bucket-a".to_string(),
            ..Default::default()
        },
    );

    let response = build_replication_reset_status_response(status, None).expect("status response should build");
    let bytes = futures::executor::block_on(http_body_util::BodyExt::collect(response.into_parts().2))
        .expect("body should read")
        .to_bytes();
    let payload: serde_json::Value = serde_json::from_slice(&bytes).expect("response must be json");

    assert_eq!(payload["target"][0]["arn"], "arn:a");
    assert_eq!(payload["target"][0]["resetid"], "rid-a");
    assert_eq!(payload["target"][0]["bucket"], "bucket-a");
    assert_eq!(payload["target"][0]["resyncStatus"], "Completed");
    assert_eq!(payload["target"][0]["startTime"], "2025-01-01T00:00:00Z");
    assert_eq!(payload["target"][0]["endTime"], "2025-01-02T00:00:00Z");
    assert_eq!(payload["target"][0]["replicationCount"], 3);
    assert_eq!(payload["target"][0]["completedReplicationSize"], 9);
    assert_eq!(payload["target"][1]["arn"], "arn:z");
    assert_eq!(payload["target"][1]["bucket"], "bucket-z");
    assert_eq!(payload["target"][1]["resyncStatus"], "Failed");
    assert_eq!(payload["target"][1]["endTime"], "2025-01-03T00:00:00Z");
    assert_eq!(payload["target"][1]["failedReplicationCount"], 2);
    assert_eq!(payload["target"][1]["failedReplicationSize"], 4);
    assert_eq!(payload["target"][1]["Error"], "boom");
}

#[test]
fn build_replication_reset_status_response_filters_targets_by_arn() {
    let mut status = BucketReplicationResyncStatus::new();
    status.targets_map.insert(
        "arn:z".to_string(),
        crate::admin::storage_api::bucket::replication::TargetReplicationResyncStatus {
            resync_id: "rid-z".to_string(),
            last_update: Some(datetime!(2025-02-03 00:00 UTC)),
            resync_status: crate::admin::storage_api::bucket::replication::ResyncStatusType::ResyncFailed,
            failed_count: 2,
            failed_size: 4,
            bucket: "bucket-z".to_string(),
            error: Some("boom".to_string()),
            ..Default::default()
        },
    );
    status.targets_map.insert(
        "arn:a".to_string(),
        crate::admin::storage_api::bucket::replication::TargetReplicationResyncStatus {
            resync_id: "rid-a".to_string(),
            last_update: Some(datetime!(2025-02-02 00:00 UTC)),
            resync_status: crate::admin::storage_api::bucket::replication::ResyncStatusType::ResyncCompleted,
            replicated_count: 3,
            replicated_size: 9,
            bucket: "bucket-a".to_string(),
            ..Default::default()
        },
    );

    let response = build_replication_reset_status_response(status, Some("arn:z")).expect("status response should build");
    let bytes = futures::executor::block_on(http_body_util::BodyExt::collect(response.into_parts().2))
        .expect("body should read")
        .to_bytes();
    let payload: serde_json::Value = serde_json::from_slice(&bytes).expect("response must be json");

    assert_eq!(payload["target"].as_array().map(Vec::len), Some(1));
    assert_eq!(payload["target"][0]["arn"], "arn:z");
    assert_eq!(payload["target"][0]["bucket"], "bucket-z");
    assert_eq!(payload["target"][0]["resyncStatus"], "Failed");
    assert_eq!(payload["target"][0]["endTime"], "2025-02-03T00:00:00Z");
    assert_eq!(payload["target"][0]["Error"], "boom");
}

/// The v1 body must decode into minio-go `replication.Metrics` (exact
/// json tags); Go's decoder matches case-insensitively but does not
/// ignore underscores, so the internal snake_case names read as all-zero.
#[test]
fn serialize_replication_metrics_body_v1_returns_minio_go_metrics_shape() {
    let mut stats = BucketStats {
        uptime: 99,
        ..Default::default()
    };
    stats.replication_stats.replica_count = 7;
    stats.replication_stats.replicated_size = 2048;
    stats
        .replication_stats
        .stats
        .entry("arn:minio:replication::t:b".to_string())
        .or_default()
        .replicated_count = 5;
    stats.proxy_stats.put_total = 3;

    let body = serialize_replication_metrics_body(&stats, ReplicationExtRoute::MetricsV1, "node-1:9000")
        .expect("metrics v1 body should serialize");
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("body should be json");

    assert_eq!(payload["replicaCount"], 7);
    assert_eq!(payload["completedReplicationSize"], 2048);
    assert_eq!(payload["Stats"]["arn:minio:replication::t:b"]["replicationCount"], 5);
    assert!(payload.get("uptime").is_none());
    assert!(payload.get("proxy_stats").is_none());
    // The internal snake_case names must not leak into the wire body.
    assert!(payload.get("replica_count").is_none());
    assert!(payload.get("q_stat").is_none());
}

#[test]
fn apply_replication_metrics_bandwidth_report_updates_existing_target_stats() {
    let mut stats = BucketStats::default();
    stats
        .replication_stats
        .stats
        .entry("arn:replication:a".to_string())
        .or_default()
        .replicated_count = 3;

    let bandwidth_report = HashMap::from([(
        "arn:replication:a".to_string(),
        BandwidthDetails {
            limit_bytes_per_sec: 2048,
            current_bandwidth_bytes_per_sec: 1536.5,
        },
    )]);

    let updated = apply_replication_metrics_bandwidth_report(stats, bandwidth_report);
    let stat = updated
        .replication_stats
        .stats
        .get("arn:replication:a")
        .expect("target stats should exist");

    assert_eq!(stat.replicated_count, 3);
    assert_eq!(stat.bandwidth_limit_bytes_per_sec, 2048);
    assert_eq!(stat.current_bandwidth_bytes_per_sec, 1536.5);
    assert_eq!(
        serde_json::to_value(stat).expect("target stats should serialize")["bandwidth_scope"],
        "node_local"
    );
}

#[test]
fn apply_replication_metrics_bandwidth_report_creates_missing_target_stats() {
    let bandwidth_report = HashMap::from([(
        "arn:replication:b".to_string(),
        BandwidthDetails {
            limit_bytes_per_sec: 4096,
            current_bandwidth_bytes_per_sec: 1024.25,
        },
    )]);

    let updated = apply_replication_metrics_bandwidth_report(BucketStats::default(), bandwidth_report);
    let stat = updated
        .replication_stats
        .stats
        .get("arn:replication:b")
        .expect("target stats should be created from bandwidth report");

    assert_eq!(stat.bandwidth_limit_bytes_per_sec, 4096);
    assert_eq!(stat.current_bandwidth_bytes_per_sec, 1024.25);
}

#[test]
fn apply_replication_metrics_bandwidth_report_does_not_replace_cluster_values() {
    let mut stats = BucketStats::default();
    stats.replication_stats.expected_node_count = 2;
    let target = stats
        .replication_stats
        .stats
        .entry("arn:replication:a".to_string())
        .or_default();
    target.bandwidth_limit_bytes_per_sec = 8192;
    target.current_bandwidth_bytes_per_sec = 3000.0;

    let updated = apply_replication_metrics_bandwidth_report(
        stats,
        HashMap::from([(
            "arn:replication:a".to_string(),
            BandwidthDetails {
                limit_bytes_per_sec: 2048,
                current_bandwidth_bytes_per_sec: 1000.0,
            },
        )]),
    );
    let target = &updated.replication_stats.stats["arn:replication:a"];

    assert_eq!(target.bandwidth_limit_bytes_per_sec, 8192);
    assert_eq!(target.current_bandwidth_bytes_per_sec, 3000.0);
}

/// The v2 body must decode into minio-go `replication.MetricsV2`
/// (`uptime`/`currStats`/`queueStats`); `mc replicate status` reads
/// `currStats` and `queueStats.nodes` and silently shows zeros when the
/// keys do not match.
#[test]
fn serialize_replication_metrics_body_v2_returns_minio_go_metrics_v2_shape() {
    let mut stats = BucketStats {
        uptime: 99,
        ..Default::default()
    };
    stats.replication_stats.replica_count = 7;
    stats
        .replication_stats
        .q_stat
        .curr
        .now_count
        .store(4, std::sync::atomic::Ordering::Relaxed);
    stats
        .replication_stats
        .q_stat
        .curr
        .now_bytes
        .store(1200, std::sync::atomic::Ordering::Relaxed);
    stats.replication_stats.q_stat = stats.replication_stats.q_stat.snapshot();
    stats.proxy_stats.put_total = 3;

    let body = serialize_replication_metrics_body(&stats, ReplicationExtRoute::MetricsV2, "node-1:9000")
        .expect("metrics v2 body should serialize");
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("body should be json");

    assert_eq!(payload["uptime"], 99);
    assert_eq!(payload["currStats"]["replicaCount"], 7);
    assert_eq!(payload["currStats"]["queued"]["curr"]["count"], 4.0);
    // The queue snapshot must surface at least one node: mc derives the
    // worker/queue panels from queueStats.nodes and treats an empty list
    // as "no data".
    assert_eq!(payload["queueStats"]["nodes"][0]["queueStats"]["curr"]["count"], 4.0);
    assert_eq!(payload["queueStats"]["nodes"][0]["uptime"], 99);
    // The internal snake_case names must not leak into the wire body.
    assert!(payload.get("replication_stats").is_none());
    assert!(payload.get("queue_stats").is_none());
    assert!(payload.get("proxy_stats").is_none());
}

#[test]
fn apply_replication_metrics_runtime_fields_only_overrides_v2_uptime() {
    let stats = BucketStats {
        uptime: 99,
        ..Default::default()
    };

    let v1 = apply_replication_metrics_runtime_fields(stats.clone(), ReplicationExtRoute::MetricsV1, 42);
    let v2 = apply_replication_metrics_runtime_fields(stats, ReplicationExtRoute::MetricsV2, 42);

    assert_eq!(v1.uptime, 99);
    assert_eq!(v2.uptime, 42);
}
