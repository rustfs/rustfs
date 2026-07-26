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

//! Seed-library tests: one realistic positive sample per rule, exact-set
//! smoke samples, and cross-rule negative cases.

use super::super::{RuleEngine, seed_rule_set};
use crate::model::{EventKind, LogEvent, LogLevel, SourceRef};
use std::sync::Arc;

fn engine() -> RuleEngine {
    RuleEngine::new(seed_rule_set())
}

struct Sample {
    message: &'static str,
    level: Option<LogLevel>,
    target: &'static str,
    kind: EventKind,
    fields: &'static [(&'static str, &'static str)],
}

impl Default for Sample {
    fn default() -> Self {
        Self {
            message: "",
            level: Some(LogLevel::Error),
            target: "rustfs::server::http",
            kind: EventKind::Json,
            fields: &[],
        }
    }
}

fn event(sample: &Sample) -> LogEvent {
    let mut fields = serde_json::Map::new();
    for (k, v) in sample.fields {
        fields.insert((*k).to_string(), serde_json::Value::String((*v).to_string()));
    }
    LogEvent {
        timestamp: None,
        level: sample.level,
        target: Some(sample.target.to_string()),
        message: sample.message.to_string(),
        fields,
        source: SourceRef {
            file: Arc::from("rustfs.log"),
            line: 1,
        },
        node: None,
        kind: sample.kind,
    }
}

fn msg(message: &'static str) -> Sample {
    Sample {
        message,
        ..Default::default()
    }
}

#[test]
fn seed_rule_set_is_valid_and_complete() {
    let set = seed_rule_set();
    assert_eq!(set.rules().len(), 69);
    // Every message-matching rule carries at least one anchor for the CI
    // guard; the only anchor-less rules are the two structural matchers
    // (panic kind, scanner target+level).
    let anchorless: Vec<_> = set
        .rules()
        .iter()
        .filter(|r| r.anchors.is_empty())
        .map(|r| r.id.as_str())
        .collect();
    assert_eq!(anchorless, vec!["scanner-error-burst", "process-panic"]);
}

#[test]
fn every_rule_has_a_positive_sample() {
    // (rule id, sample). Kept in seed-table order (rustfs/backlog#1286).
    let table: Vec<(&str, Sample)> = vec![
        // disk
        (
            "disk-marked-faulty",
            Sample {
                message: "health state changed",
                fields: &[("reason", "faulty_disk")],
                ..Default::default()
            },
        ),
        (
            "disk-faulty-rejected",
            Sample {
                message: "operation rejected",
                fields: &[("reason", "disk_marked_faulty")],
                ..Default::default()
            },
        ),
        ("remote-peer-faulty", msg("Remote peer health check failed for node2: marking as faulty")),
        (
            "peer-disks-offline",
            msg("reporting peer disks offline after consecutive storage_info failures"),
        ),
        ("drive-faulty-error", msg("remote drive is faulty")),
        (
            "metacache-listing-timeout",
            Sample {
                message: "Metacache listing quorum failed",
                fields: &[("state", "quorum_failed"), ("bucket", "photos"), ("path", "wide/")],
                ..Default::default()
            },
        ),
        ("unformatted-disk", msg("Unformatted disk found")),
        ("disk-access-denied", msg("disk access denied: /data/disk1")),
        ("inconsistent-drive", msg("inconsistent drive found")),
        ("fd-exhausted", msg("too many open files, please increase 'ulimit -n'")),
        ("odirect-unsupported", msg("drive does not support O_DIRECT")),
        (
            "rename-across-devices",
            msg("Rename across devices not allowed, please fix your backend configuration"),
        ),
        // erasure
        ("bitrot-detected", msg("bitrot hash mismatch on shard 3")),
        ("short-shard-read", msg("bitrot reader short shard read: got 100 of 200 bytes")),
        ("heal-cannot-reconstruct", msg("Heal object cannot reconstruct with available shards")),
        ("ec-write-quorum", msg("erasure write quorum (required=8, achieved=5, failed=3)")),
        ("ec-read-quorum", msg("reduce_read_quorum_errs: [..], bucket: media, object: a.bin")),
        ("file-corrupted", msg("part missing or corrupt")),
        // quorum
        (
            "nslock-quorum",
            msg("Namespace lock quorum unavailable for write lock on b/o: required 3, achieved 1"),
        ),
        (
            "below-write-quorum",
            msg("online disk snapshot 3 below write quorum 4 for b/o; returning error"),
        ),
        ("bucket-op-quorum", msg("heal_bucket reduce_write_quorum_errs: pool error")),
        ("quorum-meta-derive", msg("object_quorum_from_meta: parity_blocks < 0, errs=[..]")),
        // network
        ("peer-rpc-timeout", msg("Remote peer operation timeout after 30s")),
        ("peer-probe-timeout", msg("peer node2 server_info timed out after retry (10s)")),
        (
            "internode-signature-mismatch",
            msg("peer request failed with 403 Forbidden: SignatureDoesNotMatch"),
        ),
        (
            "rpc-secret-resolution",
            msg("RPC auth secret resolution failed: missing secret; source=env"),
        ),
        ("peer-connection-offline", msg("peer_connection_marked_offline")),
        ("topology-mismatch", msg("Expected number of all hosts (4) to be remote +1 (3)")),
        // lock
        ("lock-acquire-timeout", msg("Lock acquisition timeout for resource 'b/o' after 30s")),
        ("lock-quorum-nodes", msg("Quorum not reached: required 3, achieved 1")),
        ("lock-owner-mismatch", msg("Not the lock owner: lock_id abc, owner node1")),
        ("dist-unlock-failed", msg("distributed unlock failed on client: node2")),
        (
            "lock-state-inconsistent",
            msg("Atomic state inconsistency during exclusive lock release: owner=x, atomic_state=101"),
        ),
        ("rwlock-poisoned", msg("bucket monitor measurement rwlock read poisoned, recovering")),
        // heal
        (
            "heal-no-datadir",
            msg("heal: latest metadata for b/o has no data_dir, cannot heal object data"),
        ),
        ("heal-all-writes-failed", msg("all drives had write errors, unable to heal b/o")),
        ("heal-rename-failed", msg("all healed data rename attempts failed for b/o")),
        (
            "heal-xlmeta-regen-failed",
            msg("heal_object: failed to regenerate recoverable xl.meta on disk"),
        ),
        (
            "heal-writer-create-failed",
            msg("create_bitrot_writer  disk d1, err timeout, skipping operation"),
        ),
        ("heal-orphan-reclaim-failed", msg("heal_object: orphan data-dir reclaim failed")),
        ("heal-task-failure", msg("Heal task execution failed: worker died")),
        // scanner
        ("scanner-partial-cache", msg("Scanner stopped with partial data usage cache")),
        (
            "scanner-config-invalid",
            msg("invalid scanner config value for max_wait: -1 (must be positive)"),
        ),
        (
            "scanner-error-burst",
            Sample {
                message: "scan cycle failed",
                target: "rustfs::scanner::io",
                ..Default::default()
            },
        ),
        // iam
        ("client-signature-mismatch", msg("SignatureDoesNotMatch")),
        ("unknown-access-key", msg("The Access Key Id you provided does not exist in our records.")),
        (
            "admin-auth-failed",
            msg("authenticate_request: authentication failed - access_key=AK123, error=denied"),
        ),
        ("access-denied-burst", msg("action not allowed for user x")),
        ("credential-format-invalid", msg("invalid access key length")),
        ("keystone-auth-failed", msg("Invalid Keystone token: expired")),
        ("assume-role-failed", msg("AssumeRole get policy failed, err: NotFound, access_key: AK1")),
        // startup
        (
            "endpoint-resolve-failed",
            msg("Create pool endpoints host node5 not found, error: dns failure"),
        ),
        ("listen-config-invalid", msg("Console and endpoint should use different ports")),
        (
            "server-config-corrupt",
            msg("persisted server config cannot be decoded, object is corrupt"),
        ),
        (
            "tls-cert-load-failed",
            msg("unable to load the certificate for example.com domain name: bad path"),
        ),
        ("tls-config-invalid", msg("client_cert and client_key must be specified as a pair")),
        (
            "subsystem-init-order",
            msg("Audit system initialization failed: Global server configuration not loaded."),
        ),
        (
            "startup-fatal",
            Sample {
                message: "[FATAL] Command parse failed: bad flag",
                level: None,
                kind: EventKind::Text,
                ..Default::default()
            },
        ),
        (
            "runtime-failed",
            Sample {
                message: "Server runtime failed",
                fields: &[("event", "server_runtime_failed")],
                ..Default::default()
            },
        ),
        // capacity
        ("disk-full", msg("Disk full")),
        ("min-free-threshold", msg("Storage reached its minimum free drive threshold.")),
        (
            "insufficient-storage",
            msg("Storage resources are insufficient for the write operation: 2/4"),
        ),
        (
            "bucket-quota-exceeded",
            msg("Bucket quota exceeded: current=100, limit=50, operation=PutObject"),
        ),
        (
            "decom-capacity-insufficient",
            msg("failed to start decommission: insufficient target pool capacity: required 100 bytes available 50 bytes"),
        ),
        // ops
        ("decom-object-failed", msg("decommission_pool: decommission_object err timeout")),
        ("rebalance-worker-error", msg("Rebalance worker 3 error: disk gone")),
        (
            "datamove-same-pool",
            msg("invalid data movement operation, source and destination pool are the same for : b/o-1"),
        ),
        ("ops-state-conflict", msg("Decommission already running")),
        // process
        (
            "process-panic",
            Sample {
                message: "thread 'main' panicked at src/main.rs:3:5: boom",
                kind: EventKind::Panic,
                ..Default::default()
            },
        ),
    ];

    let engine = engine();
    let rule_ids: Vec<&str> = engine.rules().iter().map(|r| r.id.as_str()).collect();
    assert_eq!(table.len(), rule_ids.len(), "one sample per rule");

    for (id, sample) in &table {
        assert!(rule_ids.contains(id), "sample references unknown rule '{id}'");
        let hits = engine.matches(&event(sample));
        let hit_ids: Vec<&str> = hits.iter().map(|&i| engine.rules()[i].id.as_str()).collect();
        assert!(hit_ids.contains(id), "rule '{id}' did not match its sample; hits: {hit_ids:?}");
    }
}

#[test]
fn smoke_samples_hit_exact_rule_sets() {
    let engine = engine();
    let exact = |sample: &Sample, expected: &[&str]| {
        let hits = engine.matches(&event(sample));
        let mut hit_ids: Vec<&str> = hits.iter().map(|&i| engine.rules()[i].id.as_str()).collect();
        hit_ids.sort_unstable();
        let mut expected = expected.to_vec();
        expected.sort_unstable();
        assert_eq!(hit_ids, expected, "sample: {}", sample.message);
    };

    exact(
        &Sample {
            message: "Disk health check marked disk faulty",
            fields: &[("reason", "faulty_disk")],
            ..Default::default()
        },
        &["disk-marked-faulty"],
    );
    exact(&msg("erasure write quorum (required=8, achieved=5)"), &["ec-write-quorum"]);
    exact(
        &Sample {
            message: "Metacache listing quorum failed",
            fields: &[("state", "quorum_failed"), ("bucket", "photos"), ("path", "wide/")],
            ..Default::default()
        },
        &["metacache-listing-timeout"],
    );
    exact(
        &Sample {
            message: "S3Error InvalidAccessKeyId: Access Key Id you provided does not exist",
            fields: &[("code", "InvalidAccessKeyId")],
            ..Default::default()
        },
        &["unknown-access-key"],
    );
    exact(&msg("HTTP transport failed: BrokenPipe while streaming response body"), &[]);
    // Internode auth failures legitimately hit both the internode rule and
    // the generic client signature rule.
    exact(
        &msg("peer request failed with 403 Forbidden: SignatureDoesNotMatch"),
        &["internode-signature-mismatch", "client-signature-mismatch"],
    );
    exact(
        &Sample {
            message: "thread 'main' panicked at src/main.rs:3:5: boom",
            kind: EventKind::Panic,
            ..Default::default()
        },
        &["process-panic"],
    );
    // Healthy INFO lines and plain text hit nothing.
    exact(
        &Sample {
            message: "request completed in 12ms",
            level: Some(LogLevel::Info),
            ..Default::default()
        },
        &[],
    );
    exact(
        &Sample {
            message: "some ordinary text trailer",
            level: None,
            kind: EventKind::Text,
            ..Default::default()
        },
        &[],
    );
}
